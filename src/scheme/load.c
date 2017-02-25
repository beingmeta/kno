/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/stream.h"
#include "framerd/support.h"

#include <libu8/libu8io.h>
#include <libu8/u8exceptions.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>

#include <stdio.h>

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

u8_condition FileLoad=_("File Load"), FileDone=_("File Done");
u8_condition LoadEval=_("Load Eval");

static int trace_load=0, trace_load_eval=0;

static fdtype after_symbol, traceloadeval_symbol, postload_symbol;

static u8_condition UnconfiguredSource="Unconfigured source";

/* Getting sources */

static struct FD_SOURCEFN *sourcefns=NULL;

#if FD_THREADS_ENABLED
static u8_mutex sourcefns_lock;
#endif

FD_EXPORT u8_string fd_get_source
  (u8_string path,u8_string enc,u8_string *basepathp,time_t *timep)
{
  struct FD_SOURCEFN *scan=sourcefns;
  while (scan) {
    u8_string basepath=NULL;
    u8_string data=scan->fd_getsource
      (1,path,enc,&basepath,timep,scan->fd_getsource_data);
    if (data) {
      *basepathp=basepath;
      fd_clear_errors(0);
      return data;}
    else scan=scan->fd_next_sourcefn;}
  return NULL;
}
FD_EXPORT int fd_probe_source
  (u8_string path,u8_string *basepathp,time_t *timep)
{
  struct FD_SOURCEFN *scan=sourcefns;
  while (scan) {
    u8_string basepath=NULL;
    u8_string data=scan->fd_getsource
      (0,path,NULL,&basepath,timep,scan->fd_getsource_data);
    if (data) {
      *basepathp=basepath; 
      fd_clear_errors(0);
      return 1;}
    else scan=scan->fd_next_sourcefn;}
  return 0;
}
FD_EXPORT void fd_register_sourcefn
(u8_string (*fn)(int op,u8_string,u8_string,u8_string *,time_t *,void *),
 void *sourcefn_data)
{
  struct FD_SOURCEFN *new_entry=u8_alloc(struct FD_SOURCEFN);
  fd_lock_mutex(&sourcefns_lock);
  new_entry->fd_getsource=fn;
  new_entry->fd_next_sourcefn=sourcefns;
  new_entry->fd_getsource_data=sourcefn_data;
  sourcefns=new_entry;
  fd_unlock_mutex(&sourcefns_lock);
}

/* Tracking the current source base */

#if FD_USE_TLS
static u8_tld_key sourcebase_key;
FD_EXPORT u8_string fd_sourcebase()
{
  return u8_tld_get(sourcebase_key);
}
static u8_string bind_sourcebase(u8_string push)
{
  u8_string current=u8_tld_get(sourcebase_key);
  u8_tld_set(sourcebase_key,push);
  return current;
}
static void restore_sourcebase(u8_string old)
{
  u8_tld_set(sourcebase_key,old);
}
#else
static FD_THREADVAR u8_string sourcebase;
FD_EXPORT u8_string fd_sourcebase()
{
  return sourcebase;
}
static u8_string bind_sourcebase(u8_string push)
{
  u8_string current=sourcebase;
  sourcebase=push;
  return current;
}
static void restore_sourcebase(u8_string old)
{
  sourcebase=old;
}
#endif

static fdtype loading_symbol;
static void record_error_source(u8_string sourceid)
{
  fdtype entry=fd_make_list(2,loading_symbol,fd_make_string(NULL,-1,sourceid));
  fd_push_error_context("fd_load_source",entry);
}

FD_EXPORT fdtype fd_load_source_with_date
  (u8_string sourceid,fd_lispenv env,u8_string enc_name,time_t *modtime)
{
  struct U8_INPUT stream;
  fdtype postload=FD_VOID;
  u8_string sourcebase=NULL, outer_sourcebase;
  u8_string encoding=((enc_name)?(enc_name):((u8_string)("auto")));
  u8_string content=fd_get_source(sourceid,encoding,&sourcebase,modtime);
  const u8_byte *input=content;
  double start=u8_elapsed_time();
  if (content==NULL) return FD_ERROR_VALUE;
  else outer_sourcebase=bind_sourcebase(sourcebase);
  if (errno) {
    u8_log(LOG_WARN,u8_UnexpectedErrno,
           "Dangling errno value %d (%s) before loading %s",
           errno,u8_strerror(errno),sourceid);
    errno=0;}
  if ((trace_load) || (trace_load_eval))
    u8_log(LOG_NOTICE,FileLoad,
           "Loading %s (%d bytes)",sourcebase,u8_strlen(content));
  if ((input[0]=='#') && (input[1]=='!')) input=strchr(input,'\n');
  U8_INIT_STRING_INPUT((&stream),-1,input);
  {
    /* This does a read/eval loop. */
    fdtype result=FD_VOID;
    fdtype expr=fd_parse_expr(&stream), last_expr=FD_VOID;
    double start_time;
    while (!((FD_ABORTP(expr)) || (FD_EOFP(expr)))) {
      fd_decref(result);
      if ((trace_load_eval) ||
          (fd_test(env->env_bindings,traceloadeval_symbol,FD_TRUE))) {
        u8_log(LOG_NOTICE,LoadEval,"From %s, evaluating %q",sourcebase,expr);
        if (errno) {
          u8_log(LOG_WARN,"UnexpectedErrno",
                 "Dangling errno value %d (%s) before evaluating %q",
                 errno,u8_strerror(errno),expr);
          errno=0;}
        start_time=u8_elapsed_time();}
      else start_time=-1.0;
      result=fd_eval(expr,env);
      if (FD_ABORTP(result)) {
        if (FD_TROUBLEP(result)) {
          u8_exception ex=u8_current_exception;
          u8_log(LOG_ERR,ex->u8x_cond,
                 "Error (%s:%s) in %s while evaluating %q",
                 ((ex->u8x_context)?(ex->u8x_context):((u8_string)"")),
                 ((ex->u8x_details)?(ex->u8x_details):((u8_string)"")),
                 sourcebase,expr);
          record_error_source(sourceid);}
        restore_sourcebase(outer_sourcebase);
        u8_free(sourcebase);
        u8_free(content);
        fd_decref(last_expr); last_expr=FD_VOID;
        fd_decref(expr);
        return result;}
      else if ((trace_load_eval) ||
               (fd_test(env->env_bindings,traceloadeval_symbol,FD_TRUE))) {
        if (start_time>0)
          u8_log(LOG_NOTICE,LoadEval,"Took %fs to evaluate %q",
                 u8_elapsed_time()-start_time,expr);
        if (errno) {
          u8_log(LOG_WARN,"UnexpectedErrno",
                 "Dangling errno value %d (%s) after evaluating %q",
                 errno,u8_strerror(errno),expr);
          errno=0;}}
      else {}
      fd_decref(last_expr); last_expr=expr;
      expr=fd_parse_expr(&stream);}
    if (expr==FD_EOF) {
      fd_decref(last_expr); last_expr=FD_VOID;}
    else if (FD_TROUBLEP(expr)) {
      fd_seterr(NULL,"fd_parse_expr",u8_strdup("just after"),
                last_expr);
      record_error_source(sourceid);
      fd_decref(result); /* This is the previous result */
      last_expr=FD_VOID;
      /* This is now also the result */
      result=expr; fd_incref(expr);}
    else if (FD_ABORTP(expr)) {
      fd_decref(result);
      result=expr; 
      fd_incref(expr); 
      expr=FD_VOID;}
    if ((trace_load) || (trace_load_eval))
      u8_log(LOG_NOTICE,FileDone,"Loaded %s in %f seconds",
             sourcebase,u8_elapsed_time()-start);
    postload=fd_symeval(postload_symbol,env);
    if ((!(FD_ABORTP(result)))&&(!(FD_VOIDP(postload)))) {
      if ((FD_FALSEP(postload))||(FD_EMPTY_CHOICEP(postload))) {}
      else if (FD_APPLICABLEP(postload)) {
        fdtype post_result=fd_apply(postload,0,NULL);
        if (FD_ABORTP(post_result)) {
          fd_clear_errors(1);}
        fd_decref(post_result);}
      else u8_log(LOG_WARN,"fd_load_source",
                  "Postload method is not applicable: ",
                  postload);}
    fd_decref(postload);
    restore_sourcebase(outer_sourcebase);
    u8_free(sourcebase);
    u8_free(content);
    if (last_expr==expr) {
      fd_decref(last_expr); last_expr=FD_VOID;}
    else {
      fd_decref(expr); fd_decref(last_expr);
      expr=FD_VOID; last_expr=FD_VOID;}
    if (errno) {
      u8_log(LOG_WARN,"UnexpectedErrno",
             "Dangling errno value %d (%s) after loading %s",
             errno,u8_strerror(errno),sourceid);
      errno=0;}
    return result;}
}

FD_EXPORT fdtype fd_load_source
  (u8_string sourceid,fd_lispenv env,u8_string enc_name)
{
  return fd_load_source_with_date(sourceid,env,enc_name,NULL);
}

static u8_string get_component(u8_string spec)
{
  u8_string base=fd_sourcebase();
  if (base) return u8_realpath(spec,base);
  else return u8_strdup(spec);
}

FD_EXPORT
/* fd_get_component:
    Arguments: a utf8 string identifying a filename
    Returns: a utf8 string identifying a filename
  Interprets a relative pathname with respect to the directory
   of the current file being loaded.
*/
u8_string fd_get_component(u8_string spec)
{
  u8_string base=fd_sourcebase();
  if (base) return u8_realpath(spec,base);
  else return u8_strdup(spec);
}

FD_EXPORT
/* fd_bind_sourcebase:
      Arguments: a UTF-8 string
      Returns: a UTF-8 string
  This dynamically binds the sourcebase, which indicates
 the "current file" and is used by functions like load-component
 and get-component. */
u8_string fd_bind_sourcebase(u8_string sourcebase)
{
  return bind_sourcebase(sourcebase);
}

FD_EXPORT
/* fd_restore_sourcebase:
      Arguments: a UTF-8 string
      Returns: void
  Restores the previous sourcebase, passed as an argument. */
void fd_restore_sourcebase(u8_string sourcebase)
{
  restore_sourcebase(sourcebase);
}

/* Loading config files */

static int trace_config_load=0;

FD_EXPORT int fd_load_config(u8_string sourceid)
{
  struct U8_INPUT stream; int retval;
  u8_string sourcebase=NULL, outer_sourcebase;
  u8_string content=fd_get_source(sourceid,NULL,&sourcebase,NULL);
  if (content==NULL) return -1;
  else if (sourcebase) {
    outer_sourcebase=bind_sourcebase(sourcebase);}
  else outer_sourcebase=NULL;
  U8_INIT_STRING_INPUT((&stream),-1,content);
  if ((trace_load)||(trace_config_load))
    u8_log(LOG_NOTICE,FileLoad,
           "Loading config %s (%d bytes)",sourcebase,u8_strlen(content));
  retval=fd_read_config(&stream);
  if (trace_load)
    u8_log(LOG_NOTICE,FileLoad,"Loaded config %s",sourcebase);
  if (sourcebase) {
    restore_sourcebase(outer_sourcebase);
    u8_free(sourcebase);}
  u8_free(content);
  return retval;
}

FD_EXPORT int fd_load_default_config(u8_string sourceid)
{
  struct U8_INPUT stream; int retval;
  u8_string sourcebase=NULL, outer_sourcebase;
  u8_string content=fd_get_source(sourceid,NULL,&sourcebase,NULL);
  if (content==NULL) return -1;
  else if (sourcebase) {
    outer_sourcebase=bind_sourcebase(sourcebase);}
  else outer_sourcebase=NULL;
  U8_INIT_STRING_INPUT((&stream),-1,content);
  if ((trace_load)||(trace_config_load))
    u8_log(LOG_NOTICE,FileLoad,
           "Loading config %s (%d bytes)",sourcebase,u8_strlen(content));
  retval=fd_read_default_config(&stream);
  if (trace_load)
    u8_log(LOG_NOTICE,FileLoad,"Loaded config %s",sourcebase);
  if (sourcebase) {
    restore_sourcebase(outer_sourcebase);
    u8_free(sourcebase);}
  u8_free(content);
  return retval;
}

/* Scheme primitives */

static fdtype load_source(fdtype expr,fd_lispenv env)
{
  fdtype source_expr=fd_get_arg(expr,1), source, result;
  fdtype encname_expr=fd_get_arg(expr,2), encval=FD_VOID;
  u8_string encname;
  if (FD_VOIDP(source_expr))
    return fd_err(fd_TooFewExpressions,"LOAD",NULL,expr);
  else source=fd_eval(source_expr,env);
  if (FD_SYMBOLP(source)) {
    fdtype config_val=fd_config_get(FD_SYMBOL_NAME(source));
    if (FD_STRINGP(config_val)) {
      u8_log(LOG_NOTICE,"Config","Loading %s = %s",
             FD_SYMBOL_NAME(source),
             FD_STRDATA(config_val));
      source=config_val;}
    else if (FD_VOIDP(config_val)) {
      return fd_err(UnconfiguredSource,"load_source",
                    "this source is not configured",
                    source_expr);}
    else return fd_err(UnconfiguredSource,"load_source",
                       "this source is misconfigured",
                       config_val);}
  if (!(FD_STRINGP(source)))
    return fd_type_error("filename","LOAD",source);
  encval=fd_eval(encname_expr,env);
  if (FD_VOIDP(encval)) encname="auto";
  else if (FD_STRINGP(encval))
    encname=FD_STRDATA(encval);
  else if (FD_SYMBOLP(encval))
    encname=FD_SYMBOL_NAME(encval);
  else encname=NULL;
  while (!(FD_HASHTABLEP(env->env_bindings))) env=env->env_parent;
  result=fd_load_source(FD_STRDATA(source),env,encname);
  fd_decref(source); fd_decref(encval);
  return result;
}

static fdtype load_into_env_prim(fdtype source,fdtype envarg,fdtype resultfn)
{
  fdtype result=FD_VOID; fd_lispenv env;
  if (!((FD_VOIDP(resultfn))||(FD_APPLICABLEP(resultfn))))
    return fd_type_error("callback procedure","LOAD->ENV",envarg);
  if ((FD_VOIDP(envarg))||(FD_TRUEP(envarg)))
    env=fd_working_environment();
  else if (FD_FALSEP(envarg))
    env=fd_safe_working_environment();
  else if (FD_ENVIRONMENTP(envarg)) {
    env=(fd_lispenv)envarg; fd_incref(envarg);}
  else if (FD_TABLEP(envarg)) {
    env=fd_new_environment(envarg,0);
    fd_incref(envarg);}
  else return fd_type_error("environment","LOAD->ENV",envarg);
  result=fd_load_source(FD_STRDATA(source),env,NULL);
  if (FD_ABORTP(result)) {
    fd_decref((fdtype)env);
    return result;}
  if (FD_APPLICABLEP(resultfn)) {
    fdtype tmp=fd_apply(resultfn,1,&result);
    fd_decref(tmp);}
  fd_decref(result);
  return (fdtype) env;
}

static fdtype load_component(fdtype expr,fd_lispenv env)
{
  fdtype source_expr=fd_get_arg(expr,1), source, result;
  fdtype encname_expr=fd_get_arg(expr,2), encval=FD_VOID;
  u8_string encname;
  if (FD_VOIDP(source_expr))
    return fd_err(fd_TooFewExpressions,"LOAD-COMPONENT",NULL,expr);
  else source=fd_eval(source_expr,env);
  if (FD_ABORTP(source))
    return source;
  else if (!(FD_STRINGP(source)))
    return fd_type_error("filename","LOAD-COMPONENT",source);
  encval=fd_eval(encname_expr,env);
  if (FD_VOIDP(encval)) encname="auto";
  else if (FD_STRINGP(encval))
    encname=FD_STRDATA(encval);
  else if (FD_SYMBOLP(encval))
    encname=FD_SYMBOL_NAME(encval);
  else encname=NULL;
  while (!(FD_HASHTABLEP(env->env_bindings))) env=env->env_parent;
  {
    u8_string abspath=get_component(FD_STRDATA(source));
    result=fd_load_source(abspath,env,encname);
    u8_free(abspath);
  }
  fd_decref(source); fd_decref(encval);
  return result;
}

static fdtype lisp_get_source()
{
  u8_string path=fd_sourcebase();
  if (path) return fdtype_string(path);
  else return FD_FALSE;
}

static fdtype lisp_get_component(fdtype string,fdtype base)
{
  if (FD_VOIDP(base)) {
    u8_string fullpath=get_component(FD_STRDATA(string));
    return fd_lispstring(fullpath);}
  else {
    u8_string thepath=u8_realpath(FD_STRDATA(string),FD_STRDATA(base));
    return fd_lispstring(thepath);}
}

static fdtype lisp_load_config(fdtype arg)
{
  if (FD_STRINGP(arg)) {
    u8_string abspath=u8_abspath(FD_STRDATA(arg),NULL);
    int retval=fd_load_config(abspath);
    u8_free(abspath);
    if (retval<0)
      return FD_ERROR_VALUE;
    else return FD_INT(retval);}
  else if (FD_SYMBOLP(arg)) {
    fdtype config_val=fd_config_get(FD_SYMBOL_NAME(arg));
    if (FD_STRINGP(config_val)) {
      fdtype result=lisp_load_config(config_val);
      fd_decref(config_val);
      return result;}
    else if (FD_VOIDP(config_val))
      return fd_err(UnconfiguredSource,"lisp_load_config",
                    "this source is not configured",
                    arg);
    else return fd_err(UnconfiguredSource,"lisp_load_config",
                       "this source source is misconfigured",
                       config_val);}
  else return fd_type_error
         ("path or config symbol","lisp_load_config",arg);
}

static fdtype lisp_load_default_config(fdtype arg)
{
  if (FD_STRINGP(arg)) {
    u8_string abspath=u8_abspath(FD_STRDATA(arg),NULL);
    int retval=fd_load_default_config(abspath);
    u8_free(abspath);
    if (retval<0)
      return FD_ERROR_VALUE;
    else return FD_INT(retval);}
  else if (FD_SYMBOLP(arg)) {
    fdtype config_val=fd_config_get(FD_SYMBOL_NAME(arg));
    if (FD_STRINGP(config_val)) {
      fdtype result=lisp_load_default_config(config_val);
      fd_decref(config_val);
      return result;}
    else if (FD_VOIDP(config_val))
      return fd_err(UnconfiguredSource,"lisp_load_default_config",
                    "this source is not configured",
                    arg);
    else return fd_err(UnconfiguredSource,"lisp_load_default_config",
                       "this source is misconfigured",
                       config_val);}
  else return fd_type_error
         ("path or config symbol","lisp_load_default_config",arg);
}

static fdtype lisp_read_config(fdtype arg)
{
  struct U8_INPUT in; int retval;
  U8_INIT_STRING_INPUT(&in,FD_STRLEN(arg),FD_STRDATA(arg));
  retval=fd_read_config(&in);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_INT2DTYPE(retval);
}

/* Config config */

#if FD_THREADS_ENABLED
static u8_mutex config_file_lock;
#endif

static FD_CONFIG_RECORD *config_records=NULL, *config_stack=NULL;

static fdtype get_config_files(fdtype var,void U8_MAYBE_UNUSED *data)
{
  struct FD_CONFIG_RECORD *scan; fdtype result=FD_EMPTY_LIST;
  fd_lock_mutex(&config_file_lock);
  scan=config_records; while (scan) {
    result=fd_conspair(fdtype_string(scan->fd_config_source),result);
    scan=scan->fd_config_next;}
  fd_unlock_mutex(&config_file_lock);
  return result;
}

static int add_config_file_helper(fdtype var,fdtype val,
                                  void U8_MAYBE_UNUSED *data,
                                  int isopt,int isdflt)
{
  if (!(FD_STRINGP(val))) return -1;
  else if (FD_STRLEN(val)==0) return 0;
  else {
    int retval;
    struct FD_CONFIG_RECORD on_stack, *scan, *newrec;
    u8_string sourcebase=fd_sourcebase();
    u8_string pathname=u8_abspath(FD_STRDATA(val),sourcebase);
    fd_lock_mutex(&config_file_lock);
    scan=config_stack; while (scan) {
      if (strcmp(scan->fd_config_source,pathname)==0) {
        fd_unlock_mutex(&config_file_lock);
        u8_free(pathname);
        return 0;}
      else scan=scan->fd_config_next;}
    memset(&on_stack,0,sizeof(struct FD_CONFIG_RECORD));
    on_stack.fd_config_source=pathname;
    on_stack.fd_config_next=config_stack;
    config_stack=&on_stack;
    fd_unlock_mutex(&config_file_lock);
    if (isdflt)
      retval=fd_load_default_config(pathname);
    else retval=fd_load_config(pathname);
    fd_lock_mutex(&config_file_lock);
    if (retval<0) {
      if (isopt) {
        u8_free(pathname); config_stack=on_stack.fd_config_next;
        fd_unlock_mutex(&config_file_lock);
        u8_pop_exception();
        return 0;}
      u8_free(pathname); config_stack=on_stack.fd_config_next;
      fd_unlock_mutex(&config_file_lock);
      return retval;}
    newrec=u8_alloc(struct FD_CONFIG_RECORD);
    newrec->fd_config_source=pathname;
    newrec->fd_config_next=config_records;
    config_records=newrec;
    config_stack=on_stack.fd_config_next;
    fd_unlock_mutex(&config_file_lock);
    return retval;}
}

static int add_config_file(fdtype var,fdtype val,void U8_MAYBE_UNUSED *data)
{
  return add_config_file_helper(var,val,data,0,0);
}

static int add_opt_config_file(fdtype var,fdtype val,void U8_MAYBE_UNUSED *data)
{
  return add_config_file_helper(var,val,data,1,0);
}

static int add_default_config_file(fdtype var,fdtype val,void U8_MAYBE_UNUSED *data)
{
  return add_config_file_helper(var,val,data,1,1);
}

/* Initialization */

FD_EXPORT void fd_init_load_c()
{
  u8_register_source_file(_FILEINFO);

#if FD_THREADS_ENABLED
  fd_init_mutex(&sourcefns_lock);
  fd_init_mutex(&config_file_lock);
#endif
#if FD_USE_TLS
  u8_new_threadkey(&sourcebase_key,NULL);
#endif

 after_symbol=fd_intern("AFTEREXPR");
 loading_symbol=fd_intern("%LOADING");
 traceloadeval_symbol=fd_intern("%TRACELOADEVAL");
 postload_symbol=fd_intern("%POSTLOAD");


 fd_defspecial(fd_xscheme_module,"LOAD",load_source);
 fd_defspecial(fd_xscheme_module,"LOAD-COMPONENT",load_component);

 fd_defn(fd_xscheme_module,
         fd_make_cprim3x("LOAD->ENV",load_into_env_prim,1,
                         fd_string_type,FD_VOID,
                         fd_environment_type,FD_VOID,
                         -1,FD_VOID));

 fd_idefn(fd_scheme_module,
          fd_make_cprim1x("READ-CONFIG",lisp_read_config,1,
                          fd_string_type,FD_VOID));
 fd_idefn(fd_scheme_module,
          fd_make_cprim1x("LOAD-CONFIG",lisp_load_config,1,
                          -1,FD_VOID));
 fd_idefn(fd_scheme_module,
          fd_make_cprim1x("LOAD-DEFAULT-CONFIG",lisp_load_default_config,1,
                          -1,FD_VOID));

 fd_idefn(fd_scheme_module,
          fd_make_cprim2x("GET-COMPONENT",lisp_get_component,1,
                          fd_string_type,FD_VOID,
                          fd_string_type,FD_VOID));
 fd_idefn(fd_scheme_module,
          fd_make_cprim0("GET-SOURCE",lisp_get_source,0));

 fd_register_config("CONFIG","Add a CONFIG file/URI to process",
                    get_config_files,add_config_file,NULL);
 fd_register_config("DEFAULTS","Add a CONFIG file/URI to process as defaults",
                    get_config_files,add_default_config_file,NULL);
 fd_register_config("OPTCONFIG","Add an optional CONFIG file/URI to process",
                    get_config_files,add_opt_config_file,NULL);
 fd_register_config("TRACELOAD","Trace file load starts and ends",
                    fd_boolconfig_get,fd_boolconfig_set,&trace_load);
 fd_register_config("TRACELOADCONFIG","Trace config file loading",
                    fd_boolconfig_get,fd_boolconfig_set,&trace_config_load);
 fd_register_config("TRACELOADEVAL","Trace expressions while loading files",
                    fd_boolconfig_get,fd_boolconfig_set,&trace_load_eval);
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
