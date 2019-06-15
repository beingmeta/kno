/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/streams.h"
#include "kno/support.h"

#include <libu8/libu8io.h>
#include <libu8/u8exceptions.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

u8_condition FileLoad=_("File Load"), FileDone=_("File Done");
u8_condition LoadEval=_("Load Eval");
u8_condition LoadConfig=_("Loading config");

static int trace_load = 0, trace_load_eval = 0;

static lispval after_symbol, traceloadeval_symbol, postload_symbol;

static u8_condition UnconfiguredSource="Unconfigured source";

/* Getting sources */

static struct KNO_SOURCEFN *sourcefns = NULL;
static u8_mutex sourcefns_lock;

KNO_EXPORT u8_string kno_get_source
  (u8_string path,u8_string enc,u8_string *basepathp,time_t *timep)
{
  struct KNO_SOURCEFN *scan = sourcefns;
  while (scan) {
    u8_string basepath = NULL;
    u8_string data = scan->getsource
      (1,path,enc,&basepath,timep,scan->getsource_data);
    if (data) {
      *basepathp = basepath;
      kno_clear_errors(0);
      return data;}
    else scan = scan->getsource_next;}
  return NULL;
}
KNO_EXPORT int kno_probe_source
  (u8_string path,u8_string *basepathp,time_t *timep)
{
  struct KNO_SOURCEFN *scan = sourcefns;
  while (scan) {
    u8_string basepath = NULL;
    u8_string data = scan->getsource
      (0,path,NULL,&basepath,timep,scan->getsource_data);
    if (data) {
      *basepathp = basepath;
      kno_clear_errors(0);
      return 1;}
    else scan = scan->getsource_next;}
  return 0;
}
KNO_EXPORT void kno_register_sourcefn
(u8_string (*fn)(int op,u8_string,u8_string,u8_string *,time_t *,void *),
 void *sourcefn_data)
{
  struct KNO_SOURCEFN *new_entry = u8_alloc(struct KNO_SOURCEFN);
  u8_lock_mutex(&sourcefns_lock);
  new_entry->getsource = fn;
  new_entry->getsource_next = sourcefns;
  new_entry->getsource_data = sourcefn_data;
  sourcefns = new_entry;
  u8_unlock_mutex(&sourcefns_lock);
}

static lispval loading_symbol;

#define LOAD_CONTEXT_SIZE 40

KNO_EXPORT lispval kno_load_stream(u8_input loadstream,kno_lexenv env,
                                 u8_string sourcebase)
{
  u8_string outer_sourcebase = kno_bind_sourcebase(sourcebase);
  double start = u8_elapsed_time();
  struct KNO_STACK *_stack = kno_stackptr;
  lispval postload = VOID;
  if (errno) {
    u8_log(LOG_WARN,u8_UnexpectedErrno,
           "Dangling errno value %d (%s) before loading %s",
           errno,u8_strerror(errno),sourcebase);
    errno = 0;}
  KNO_PUSH_STACK(load_stack,"loadsource",u8_strdup(sourcebase),VOID);
  U8_SETBITS(load_stack->stack_flags,KNO_STACK_FREE_LABEL);
  {
    /* This does a read/eval loop. */
    u8_byte context_buf[LOAD_CONTEXT_SIZE+1];
    lispval result = VOID;
    lispval expr = VOID, last_expr = VOID;
    double start_time;
    kno_skip_whitespace(loadstream);
    load_stack->stack_status=context_buf; context_buf[0]='\0';
    while (!((KNO_ABORTP(expr)) || (KNO_EOFP(expr)))) {
      kno_decref(result);
      if ((trace_load_eval) ||
          (kno_test(env->env_bindings,traceloadeval_symbol,KNO_TRUE))) {
        u8_log(LOG_WARN,LoadEval,"From %s, evaluating %q",sourcebase,expr);
        if (errno) {
          u8_log(LOG_WARN,"UnexpectedErrno",
                 "Dangling errno value %d (%s) before evaluating %q",
                 errno,u8_strerror(errno),expr);
          errno = 0;}
        start_time = u8_elapsed_time();}
      else start_time = -1.0;
      result = kno_eval(expr,env);
      if (KNO_ABORTP(result)) {
        if (KNO_TROUBLEP(result)) {
          u8_exception ex = u8_current_exception;
          u8_log(LOG_ERR,ex->u8x_cond,
                 "Error (%s:%s) in %s while evaluating %q",
                 ((ex->u8x_context)?(ex->u8x_context):((u8_string)"")),
                 ((ex->u8x_details)?(ex->u8x_details):((u8_string)"")),
                 sourcebase,expr);}
        kno_restore_sourcebase(outer_sourcebase);
        kno_decref(last_expr); last_expr = VOID;
        kno_decref(expr);
        kno_pop_stack(load_stack);
        return result;}
      else if ((trace_load_eval) ||
               (kno_test(env->env_bindings,traceloadeval_symbol,KNO_TRUE))) {
        if (start_time>0)
          u8_log(LOG_WARN,LoadEval,"Took %fs to evaluate %q",
                 u8_elapsed_time()-start_time,expr);
        if (errno) {
          u8_log(LOG_WARN,"UnexpectedErrno",
                 "Dangling errno value %d (%s) after evaluating %q",
                 errno,u8_strerror(errno),expr);
          errno = 0;}}
      else {}
      kno_decref(last_expr); last_expr = expr;
      kno_skip_whitespace(loadstream);
      if (loadstream->u8_inlim == loadstream->u8_read)
        context_buf[0]='\0';
      else u8_string2buf(loadstream->u8_read,context_buf,LOAD_CONTEXT_SIZE);
      expr = kno_parse_expr(loadstream);}
    if (expr == KNO_EOF) {
      kno_decref(last_expr);
      last_expr = VOID;}
    else if (KNO_TROUBLEP(expr)) {
      kno_seterr(NULL,"kno_parse_expr",load_stack->stack_status,last_expr);
      kno_decref(result); /* This is the previous result */
      last_expr = VOID;
      /* This is now also the result */
      result = expr;
      kno_incref(expr);}
    else if (KNO_ABORTP(expr)) {
      kno_decref(result);
      result = expr;
      kno_incref(expr);
      expr = VOID;}
    else {}
    /* Clear the stack status */
    load_stack->stack_status=NULL;
    if ((trace_load) || (trace_load_eval))
      u8_log(LOG_WARN,FileDone,"Loaded %s in %f seconds",
             sourcebase,u8_elapsed_time()-start);
    postload = kno_symeval(postload_symbol,env);
    if ((!(KNO_ABORTP(result)))&&(!(VOIDP(postload)))) {
      if ((FALSEP(postload))||(EMPTYP(postload))) {}
      else if (KNO_APPLICABLEP(postload)) {
        lispval post_result = kno_apply(postload,0,NULL);
        if (KNO_ABORTP(post_result)) {
          kno_clear_errors(1);}
        kno_decref(post_result);}
      else u8_log(LOG_WARN,"kno_load_source",
                  "Postload method is not applicable: ",
                  postload);}
    kno_decref(postload);
    kno_restore_sourcebase(outer_sourcebase);
    if (last_expr == expr) {
      kno_decref(last_expr);
      last_expr = VOID;}
    else {
      kno_decref(expr);
      kno_decref(last_expr);
      expr = VOID;
      last_expr = VOID;}
    if (errno) {
      u8_log(LOG_WARN,"UnexpectedErrno",
             "Dangling errno value %d (%s) after loading %s",
             errno,u8_strerror(errno),sourcebase);
      errno = 0;}
    kno_pop_stack(load_stack);
    return result;}
}

KNO_EXPORT lispval kno_load_source_with_date
  (u8_string sourceid,kno_lexenv env,u8_string enc_name,time_t *modtime)
{
  struct U8_INPUT stream;
  u8_string sourcebase = NULL;
  u8_string encoding = ((enc_name)?(enc_name):((u8_string)("auto")));
  u8_string content = kno_get_source(sourceid,encoding,&sourcebase,modtime);
  if (content == NULL)
    return KNO_ERROR;
  const u8_byte *input = content;
  if ((trace_load) || (trace_load_eval))
    u8_log(LOG_WARN,FileLoad,
           "Loading %s (%d bytes)",sourcebase,u8_strlen(content));
  if ((input[0]=='#') && (input[1]=='!')) input = strchr(input,'\n');
  U8_INIT_STRING_INPUT((&stream),-1,input);
  lispval result = kno_load_stream(&stream,env,sourcebase);
  if (sourcebase) u8_free(sourcebase);
  u8_free(content);
  return result;
}

KNO_EXPORT lispval kno_load_source
  (u8_string sourceid,kno_lexenv env,u8_string enc_name)
{
  return kno_load_source_with_date(sourceid,env,enc_name,NULL);
}

static u8_string get_component(u8_string spec)
{
  u8_string base = kno_sourcebase();
  if (base) return u8_realpath(spec,base);
  else return u8_strdup(spec);
}

/* Loading config files */

static int trace_config_load = 0;

KNO_EXPORT int kno_load_config(u8_string sourceid)
{
  struct U8_INPUT stream; int retval;
  u8_string sourcebase = NULL, outer_sourcebase;
  u8_string content = kno_get_source(sourceid,NULL,&sourcebase,NULL);
  if (content == NULL) return -1;
  else if (sourcebase) {
    outer_sourcebase = kno_bind_sourcebase(sourcebase);}
  else outer_sourcebase = NULL;
  U8_INIT_STRING_INPUT((&stream),-1,content);
  if ( (trace_load) || (trace_config_load) || (kno_trace_config) )
    u8_log(LOG_WARN,FileLoad,
           "Loading config %s (%d bytes)",sourcebase,u8_strlen(content));
  retval = kno_read_config(&stream);
  if (trace_load)
    u8_log(LOG_WARN,FileLoad,"Loaded config %s",sourcebase);
  if (sourcebase) {
    kno_restore_sourcebase(outer_sourcebase);
    u8_free(sourcebase);}
  u8_free(content);
  return retval;
}

KNO_EXPORT int kno_load_default_config(u8_string sourceid)
{
  struct U8_INPUT stream; int retval;
  u8_string sourcebase = NULL, outer_sourcebase;
  u8_string content = kno_get_source(sourceid,NULL,&sourcebase,NULL);
  if (content == NULL) return -1;
  else if (sourcebase) {
    outer_sourcebase = kno_bind_sourcebase(sourcebase);}
  else outer_sourcebase = NULL;
  U8_INIT_STRING_INPUT((&stream),-1,content);
  if ( (trace_load) || (trace_config_load) || (kno_trace_config) )
    u8_log(LOG_WARN,FileLoad,
           "Loading config %s (%d bytes)",sourcebase,u8_strlen(content));
  retval = kno_read_default_config(&stream);
  if (trace_load)
    u8_log(LOG_WARN,FileLoad,"Loaded config %s",sourcebase);
  if (sourcebase) {
    kno_restore_sourcebase(outer_sourcebase);
    u8_free(sourcebase);}
  u8_free(content);
  return retval;
}

/* Scheme primitives */

static lispval load_source_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval source_expr = kno_get_arg(expr,1), source, result;
  lispval encname_expr = kno_get_arg(expr,2), encval = VOID;
  u8_string encname;
  if (VOIDP(source_expr))
    return kno_err(kno_TooFewExpressions,"LOAD",NULL,expr);
  else source = kno_eval(source_expr,env);
  if (SYMBOLP(source)) {
    lispval config_val = kno_config_get(SYM_NAME(source));
    if (STRINGP(config_val)) {
      u8_log(LOG_NOTICE,"Config","Loading %s = %s",
             SYM_NAME(source),
             CSTRING(config_val));
      source = config_val;}
    else if (VOIDP(config_val)) {
      return kno_err(UnconfiguredSource,"load_source",
                    "this source is not configured",
                    source_expr);}
    else return kno_err(UnconfiguredSource,"load_source",
                       "this source is misconfigured",
                       config_val);}
  if (!(STRINGP(source)))
    return kno_type_error("filename","LOAD",source);
  encval = kno_eval(encname_expr,env);
  if (VOIDP(encval)) encname="auto";
  else if (STRINGP(encval))
    encname = CSTRING(encval);
  else if (SYMBOLP(encval))
    encname = SYM_NAME(encval);
  else encname = NULL;
  while (!(HASHTABLEP(env->env_bindings))) env = env->env_parent;
  result = kno_load_source(CSTRING(source),env,encname);
  kno_decref(source); kno_decref(encval);
  return result;
}

static lispval load_into_env_prim(lispval source,lispval envarg,lispval resultfn)
{
  lispval result = VOID; kno_lexenv env;
  if (!((VOIDP(resultfn))||(KNO_APPLICABLEP(resultfn))))
    return kno_type_error("callback procedure","LOAD->ENV",envarg);
  if ((VOIDP(envarg))||(KNO_TRUEP(envarg)))
    env = kno_working_lexenv();
  else if (KNO_LEXENVP(envarg)) {
    env = (kno_lexenv)envarg; kno_incref(envarg);}
  else if (TABLEP(envarg)) {
    env = kno_new_lexenv(envarg);
    kno_incref(envarg);}
  else return kno_type_error("environment","LOAD->ENV",envarg);
  result = kno_load_source(CSTRING(source),env,NULL);
  if (KNO_ABORTP(result)) {
    kno_decref((lispval)env);
    return result;}
  if (KNO_APPLICABLEP(resultfn)) {
    lispval tmp = kno_apply(resultfn,1,&result);
    kno_decref(tmp);}
  kno_decref(result);
  return (lispval) env;
}

static lispval load_component_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval source_expr = kno_get_arg(expr,1), source, result;
  lispval encname_expr = kno_get_arg(expr,2), encval = VOID;
  u8_string encname;
  if (VOIDP(source_expr))
    return kno_err(kno_TooFewExpressions,"LOAD-COMPONENT",NULL,expr);
  else source = kno_eval(source_expr,env);
  if (KNO_ABORTP(source))
    return source;
  else if (!(STRINGP(source)))
    return kno_type_error("filename","LOAD-COMPONENT",source);
  encval = kno_eval(encname_expr,env);
  if (VOIDP(encval)) encname="auto";
  else if (STRINGP(encval))
    encname = CSTRING(encval);
  else if (SYMBOLP(encval))
    encname = SYM_NAME(encval);
  else encname = NULL;
  while (!(HASHTABLEP(env->env_bindings))) env = env->env_parent;
  {
    u8_string abspath = get_component(CSTRING(source));
    result = kno_load_source(abspath,env,encname);
    u8_free(abspath);
  }
  kno_decref(source); kno_decref(encval);
  return result;
}

static lispval lisp_get_component(lispval string,lispval base)
{
  if (VOIDP(base)) {
    u8_string fullpath = get_component(CSTRING(string));
    return kno_lispstring(fullpath);}
  else {
    u8_string thepath = u8_realpath(CSTRING(string),CSTRING(base));
    return kno_lispstring(thepath);}
}

static lispval path_macro(lispval expr,kno_lexenv env,kno_stack ptr)
{
  lispval arg = kno_get_arg(expr,1);
  if (KNO_STRINGP(arg)) {
    u8_string fullpath = (kno_sourcebase()) ?
      (kno_get_component(KNO_CSTRING(arg))) :
      (u8_abspath(KNO_CSTRING(arg),NULL));
    return kno_init_string(NULL,-1,fullpath);}
  else return kno_err(kno_TypeError,"path_macro","string",arg);
}

static lispval lisp_load_config(lispval arg)
{
  if (STRINGP(arg)) {
    u8_string abspath = u8_abspath(CSTRING(arg),NULL);
    int retval = kno_load_config(abspath);
    u8_free(abspath);
    if (retval<0)
      return KNO_ERROR;
    else return KNO_INT(retval);}
  else if (SYMBOLP(arg)) {
    lispval config_val = kno_config_get(SYM_NAME(arg));
    if (STRINGP(config_val)) {
      lispval result = lisp_load_config(config_val);
      kno_decref(config_val);
      return result;}
    else if (VOIDP(config_val))
      return kno_err(UnconfiguredSource,"lisp_load_config",
                    "this source is not configured",
                    arg);
    else return kno_err(UnconfiguredSource,"lisp_load_config",
                       "this source source is misconfigured",
                       config_val);}
  else return kno_type_error
         ("path or config symbol","lisp_load_config",arg);
}

static lispval lisp_load_default_config(lispval arg)
{
  if (STRINGP(arg)) {
    u8_string abspath = u8_abspath(CSTRING(arg),NULL);
    int retval = kno_load_default_config(abspath);
    u8_free(abspath);
    if (retval<0)
      return KNO_ERROR;
    else return KNO_INT(retval);}
  else if (SYMBOLP(arg)) {
    lispval config_val = kno_config_get(SYM_NAME(arg));
    if (STRINGP(config_val)) {
      lispval result = lisp_load_default_config(config_val);
      kno_decref(config_val);
      return result;}
    else if (VOIDP(config_val))
      return kno_err(UnconfiguredSource,"lisp_load_default_config",
                    "this source is not configured",
                    arg);
    else return kno_err(UnconfiguredSource,"lisp_load_default_config",
                       "this source is misconfigured",
                       config_val);}
  else return kno_type_error
         ("path or config symbol","lisp_load_default_config",arg);
}

static lispval lisp_read_config(lispval arg)
{
  struct U8_INPUT in; int retval;
  U8_INIT_STRING_INPUT(&in,STRLEN(arg),CSTRING(arg));
  retval = kno_read_config(&in);
  if (retval<0) return KNO_ERROR;
  else return KNO_INT2DTYPE(retval);
}

/* Lisp run */

static lispval kno_run(u8_string source_file,struct U8_OUTPUT *out,
                      int n,lispval *args)
{
  kno_lexenv env = kno_working_lexenv();
  lispval load_result = kno_load_source(source_file,env,NULL);
  if (KNO_ABORTP(load_result)) {
    kno_decref((lispval)env);
    return load_result;}
  else {
    lispval main_proc = kno_symeval(KNOSYM_MAIN,env);
    if (KNO_VOIDP(main_proc)) {
      u8_log(LOG_CRIT,"NoMain",
             "No (MAIN) was defined in '%s', returning last value",
             source_file);
      return load_result;}
    else if (!(KNO_APPLICABLEP(main_proc))) {
      kno_seterr("BadMainProc","lisp_run_file",u8_strdup(source_file),main_proc);
      kno_decref((lispval)env);
      return KNO_ERROR_VALUE;}
    else {
      u8_output prev = u8_current_output;
      if (out) u8_set_default_output(out);
      lispval result = kno_dapply(main_proc,n,args);
      kno_decref((lispval)env);
      kno_decref(load_result);
      if (out) u8_set_default_output(prev);
      if (KNO_ABORTP(result)) {
        u8_exception ex = u8_erreify();
        lispval exception_object = kno_wrap_exception(ex);
        u8_free_exception(ex,1);
        return exception_object;}
      else return result;}}
}

static lispval kno_run_file(int n,lispval *args)
{
  if ( (KNO_STRINGP(args[0])) &&
       (u8_file_existsp(KNO_CSTRING(args[0]))) )
    return kno_run(KNO_CSTRING(args[0]),NULL,n-1,args+1);
  else return kno_type_error("filename","kno_run_file",args[0]);
}

static lispval kno_run_file_2string(int n,lispval *args)
{
  if ( (KNO_STRINGP(args[0])) &&
       (u8_file_existsp(KNO_CSTRING(args[0]))) ) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,4096);
    lispval result = kno_run(KNO_CSTRING(args[0]),&out,n-1,args+1);
    if (KNO_ABORTP(result)) {
      if ( (out.u8_write-out.u8_outbuf) > 0) {
        lispval output = kno_stream_string(&out);
        u8_close_output(&out);
        kno_seterr("RunFailed","kno_run_file_2string",
                  u8_strdup(KNO_CSTRING(args[0])),
                  output);
        kno_decref(output);
        return result;}
      else return result;}
    else {
      lispval output = kno_stream_string(&out);
      kno_decref(result);
      u8_close_output(&out);
      return output;}}
  else return kno_type_error("filename","kno_run_file_2string",args[0]);
}

/* Config config */

static u8_mutex config_file_lock;

static KNO_CONFIG_RECORD *config_records = NULL, *config_stack = NULL;

static lispval get_config_files(lispval var,void U8_MAYBE_UNUSED *data)
{
  struct KNO_CONFIG_RECORD *scan; lispval result = NIL;
  u8_lock_mutex(&config_file_lock);
  scan = config_records; while (scan) {
    result = kno_conspair(lispval_string(scan->config_filename),result);
    scan = scan->loaded_after;}
  u8_unlock_mutex(&config_file_lock);
  return result;
}

static int add_config_file_helper(lispval var,lispval val,
                                  void U8_MAYBE_UNUSED *data,
                                  int isopt,int isdflt)
{
  if (!(STRINGP(val))) return -1;
  else if (STRLEN(val)==0) return 0;
  else {
    int retval;
    struct KNO_CONFIG_RECORD on_stack, *scan, *newrec;
    u8_string sourcebase = kno_sourcebase();
    u8_string pathname = u8_abspath(CSTRING(val),sourcebase);
    u8_lock_mutex(&config_file_lock);
    scan = config_stack; while (scan) {
      if (strcmp(scan->config_filename,pathname)==0) {
        u8_unlock_mutex(&config_file_lock);
        if ( (kno_trace_config) || (trace_load) || (trace_config_load) )
          u8_log(LOGINFO,LoadConfig,
                 "Skipping redundant reload of %s from the config %q",
                 KNO_CSTRING(val),var);
        u8_free(pathname);
        return 0;}
      else scan = scan->loaded_after;}
    if ( (kno_trace_config) || (trace_load) || (trace_config_load) )
      u8_log(LOGWARN,LoadConfig,
             "Loading the config file %s in response to a %q directive",
             KNO_CSTRING(val),var);
    memset(&on_stack,0,sizeof(struct KNO_CONFIG_RECORD));
    on_stack.config_filename = pathname;
    on_stack.loaded_after = config_stack;
    config_stack = &on_stack;
    u8_unlock_mutex(&config_file_lock);
    if (isdflt)
      retval = kno_load_default_config(pathname);
    else retval = kno_load_config(pathname);
    u8_lock_mutex(&config_file_lock);
    if (retval<0) {
      if (isopt) {
        u8_free(pathname); config_stack = on_stack.loaded_after;
        u8_unlock_mutex(&config_file_lock);
        u8_pop_exception();
        return 0;}
      u8_free(pathname); config_stack = on_stack.loaded_after;
      u8_unlock_mutex(&config_file_lock);
      return retval;}
    newrec = u8_alloc(struct KNO_CONFIG_RECORD);
    newrec->config_filename = pathname;
    newrec->loaded_after = config_records;
    config_records = newrec;
    config_stack = on_stack.loaded_after;
    u8_unlock_mutex(&config_file_lock);
    return retval;}
}

static int add_config_file(lispval var,lispval val,void U8_MAYBE_UNUSED *data)
{
  return add_config_file_helper(var,val,data,0,0);
}

static int add_opt_config_file(lispval var,lispval val,void U8_MAYBE_UNUSED *data)
{
  return add_config_file_helper(var,val,data,1,0);
}

static int add_default_config_file(lispval var,lispval val,void U8_MAYBE_UNUSED *data)
{
  return add_config_file_helper(var,val,data,1,1);
}

/* Initialization */

KNO_EXPORT void kno_init_load_c()
{
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&sourcefns_lock);
  u8_init_mutex(&config_file_lock);

 after_symbol = kno_intern("afterexpr");
 loading_symbol = kno_intern("%loading");
 traceloadeval_symbol = kno_intern("%traceloadeval");
 postload_symbol = kno_intern("%postload");


 kno_def_evalfn(kno_scheme_module,"LOAD","",load_source_evalfn);
 kno_def_evalfn(kno_scheme_module,"LOAD-COMPONENT","",load_component_evalfn);

 kno_defn(kno_scheme_module,
         kno_make_cprim3x("LOAD->ENV",load_into_env_prim,1,
                         kno_string_type,VOID,
                         kno_lexenv_type,VOID,
                         -1,VOID));

 kno_idefn(kno_scheme_module,
          kno_make_cprim1x("READ-CONFIG",lisp_read_config,1,
                          kno_string_type,VOID));
 kno_idefn(kno_scheme_module,
          kno_make_cprim1x("LOAD-CONFIG",lisp_load_config,1,
                          -1,VOID));
 kno_idefn(kno_scheme_module,
          kno_make_cprim1x("LOAD-DEFAULT-CONFIG",lisp_load_default_config,1,
                          -1,VOID));

 kno_idefnN(kno_scheme_module,"KNO/RUN-FILE",
           kno_run_file,KNO_NEEDS_1_ARG|KNO_NDCALL,
           "Loads a file and applies its (main) procedure to the arguments");
 kno_idefnN(kno_scheme_module,"KNO/RUN->STRING",
            kno_run_file_2string,KNO_NEEDS_1_ARG|KNO_NDCALL,
            "Loads a KNO file and applies its (main) procedure "
            "to the arguments, returns the output as a string");

 kno_idefn(kno_scheme_module,
          kno_make_cprim2x("GET-COMPONENT",lisp_get_component,1,
                          kno_string_type,VOID,
                          kno_string_type,VOID));

  kno_def_evalfn(kno_scheme_module,"#PATH",
                "#:PATH\"init/foo.scm\" or #:PATH:home.scm\n"
                "evaluates to an environment variable",
                path_macro);

 kno_register_config("CONFIG","Add a CONFIG file/URI to process",
                    get_config_files,add_config_file,NULL);
 kno_register_config("DEFAULTS","Add a CONFIG file/URI to process as defaults",
                    get_config_files,add_default_config_file,NULL);
 kno_register_config("OPTCONFIG","Add an optional CONFIG file/URI to process",
                    get_config_files,add_opt_config_file,NULL);
 kno_register_config("TRACELOAD","Trace file load starts and ends",
                    kno_boolconfig_get,kno_boolconfig_set,&trace_load);
 kno_register_config("TRACELOADCONFIG","Trace config file loading",
                    kno_boolconfig_get,kno_boolconfig_set,&trace_config_load);
 kno_register_config("TRACELOADEVAL","Trace expressions while loading files",
                    kno_boolconfig_get,kno_boolconfig_set,&trace_load_eval);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
