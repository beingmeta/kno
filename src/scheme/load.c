/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/dtypestream.h"
#include "fdb/support.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>

#include <stdio.h>

static MAYBE_UNUSED char versionid[] =
  "$Id$";

u8_condition FileLoad=_("File Load"), FileDone=_("File Done");

fd_exception fd_NotAFilename=_("Not a filename");
fd_exception fd_FileNotFound=_("File not found");

static int trace_load=0;

static fdtype after_symbol;

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
    u8_string data=scan->getsource(path,enc,&basepath,timep);
    if (data) {*basepathp=basepath; return data;}
    else scan=scan->next;}
  return NULL;
}
FD_EXPORT void fd_register_sourcefn(u8_string (*fn)(u8_string,u8_string,u8_string *,time_t *))
{
  struct FD_SOURCEFN *new_entry=u8_alloc(struct FD_SOURCEFN);
  fd_lock_mutex(&sourcefns_lock);
  new_entry->getsource=fn; new_entry->next=sourcefns; sourcefns=new_entry;
  fd_unlock_mutex(&sourcefns_lock);
}

/* Tracking the current source base */

#if FD_THREADS_ENABLED
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
static u8_string sourcebase;
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

FD_EXPORT fdtype fd_load_source
  (u8_string sourceid,fd_lispenv env,u8_string enc_name)
{
  struct U8_INPUT stream;
  u8_string sourcebase=NULL, outer_sourcebase;
  u8_string encoding=((enc_name)?(enc_name):((u8_string)("auto")));
  u8_string content=fd_get_source(sourceid,encoding,&sourcebase,NULL);
  u8_byte *input=content;
  double start=u8_elapsed_time();
  if (content==NULL) return FD_ERROR_VALUE;
  else outer_sourcebase=bind_sourcebase(sourcebase);
  if (trace_load) 
    u8_log(LOG_INFO,FileLoad,"Loading %s (%d bytes)",sourcebase,u8_strlen(content));
  if ((input[0]=='#') && (input[1]=='!')) input=strchr(input,'\n');
  U8_INIT_STRING_INPUT((&stream),-1,input);
  {
    /* This does a read/eval loop. */
    fdtype result=FD_VOID;
    fdtype expr=fd_parse_expr(&stream), last_expr=FD_VOID;
    while (!((FD_ABORTP(expr)) || (FD_EOFP(expr)))) {
      fd_decref(result);
      result=fd_eval(expr,env);
      if (FD_ABORTP(result)) {
	restore_sourcebase(outer_sourcebase);
	u8_free(sourcebase);
	u8_free(content);
	fd_decref(last_expr);
	return result;}
      fd_decref(last_expr);  last_expr=expr;
      expr=fd_parse_expr(&stream);}
    if (expr==FD_EOF) {
      fd_decref(last_expr); last_expr=FD_VOID;}
    else if (FD_TROUBLEP(expr)) {
      fd_decref(result);
      fd_seterr(NULL,"fd_parse_expr",u8_strdup("just after"),
		last_expr);
      result=fd_incref(expr);}
    else if (FD_ABORTP(expr)) {
      result=expr; expr=FD_VOID;}
    else fd_decref(last_expr);
    if (trace_load) 
      u8_log(LOG_INFO,FileDone,"Loaded %s in %f seconds",
	     sourcebase,u8_elapsed_time()-start);
    restore_sourcebase(outer_sourcebase);
    u8_free(sourcebase);
    u8_free(content);
    fd_decref(expr);
    return result;}
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

FD_EXPORT int fd_load_config(u8_string sourceid)
{
  struct U8_INPUT stream; int retval;
  u8_string sourcebase=NULL;
  u8_string content=fd_get_source(sourceid,NULL,&sourcebase,NULL);
  if (content==NULL) return FD_ERROR_VALUE;
  else if (sourcebase) u8_free(sourcebase);
  U8_INIT_STRING_INPUT((&stream),-1,content);
  retval=fd_read_config(&stream);
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
  if (!(FD_STRINGP(source)))
    return fd_err(fd_NotAFilename,"LOAD",NULL,source);
  encval=fd_eval(encname_expr,env);
  if (FD_VOIDP(encval)) encname="auto";
  else if (FD_STRINGP(encval)) 
    encname=FD_STRDATA(encval);
  else if (FD_SYMBOLP(encval)) 
    encname=FD_SYMBOL_NAME(encval);
  else encname=NULL;
  while (!(FD_HASHTABLEP(env->bindings))) env=env->parent;
  result=fd_load_source(FD_STRDATA(source),env,encname);
  fd_decref(source); fd_decref(encval);
  return result;
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
    return fd_err(fd_NotAFilename,"LOAD-COMPONENT",NULL,source);
  encval=fd_eval(encname_expr,env);
  if (FD_VOIDP(encval)) encname="auto";
  else if (FD_STRINGP(encval)) 
    encname=FD_STRDATA(encval);
  else if (FD_SYMBOLP(encval)) 
    encname=FD_SYMBOL_NAME(encval);
  else encname=NULL;
  while (!(FD_HASHTABLEP(env->bindings))) env=env->parent;
  {
    u8_string abspath=get_component(FD_STRDATA(source));
    result=fd_load_source(abspath,env,encname);
    u8_free(abspath);
  }
  fd_decref(source); fd_decref(encval);
  return result;
}

static fdtype lisp_get_component(fdtype string,fdtype base)
{
  if (FD_VOIDP(base)) {
    u8_string fullpath=get_component(FD_STRDATA(string));
    return fd_init_string(NULL,strlen(fullpath),fullpath);}
  else {
    u8_string thepath=u8_realpath(FD_STRDATA(string),FD_STRDATA(base));
    return fd_init_string(NULL,strlen(thepath),thepath);}
}

static fdtype lisp_load_config(fdtype string)
{
  u8_string abspath=u8_abspath(FD_STRDATA(string),NULL);
  int retval=fd_load_config(abspath);
  u8_free(abspath);
  if (retval<0) 
    return FD_ERROR_VALUE;
  else return FD_INT2DTYPE(retval);
}

/* Config config */

#if FD_THREADS_ENABLED
static u8_mutex config_file_lock;
#endif

static FD_CONFIG_RECORD *config_records=NULL, *config_stack=NULL;

static fdtype get_config_files(fdtype var,void MAYBE_UNUSED *data)
{
  struct FD_CONFIG_RECORD *scan; fdtype result=FD_EMPTY_LIST;
  fd_lock_mutex(&config_file_lock);
  scan=config_records; while (scan) {
    result=fd_init_pair(NULL,fdtype_string(scan->source),result);
    scan=scan->next;}
  fd_unlock_mutex(&config_file_lock);
  return result;
}

static int add_config_file(fdtype var,fdtype val,void MAYBE_UNUSED *data)
{
  if (FD_STRINGP(val)) {
    int retval;
    struct FD_CONFIG_RECORD on_stack, *scan, *newrec;
    u8_string pathname=u8_abspath(FD_STRDATA(val),NULL);
    fd_lock_mutex(&config_file_lock);
    scan=config_stack; while (scan)
      if (strcmp(scan->source,pathname)==0) {
	fd_unlock_mutex(&config_file_lock);
	u8_free(pathname);
	return 0;}
      else scan=scan->next;
    on_stack.source=pathname;
    on_stack.next=config_stack;
    config_stack=&on_stack;
    fd_config_lock(0);
    retval=fd_load_config(pathname);
    fd_config_lock(1);
    if (retval<0) {
      fd_lock_mutex(&config_file_lock);
      u8_free(pathname); config_stack=on_stack.next;
      fd_unlock_mutex(&config_file_lock);
      return retval;}
    newrec=u8_alloc(struct FD_CONFIG_RECORD);
    newrec->source=pathname;
    newrec->next=config_records;
    config_records=newrec;
    fd_unlock_mutex(&config_file_lock);
    return retval;}
  else return -1;
}
/* Initialization */

FD_EXPORT void fd_init_load_c()
{
  fd_register_source_file(versionid);

#if FD_THREADS_ENABLED
  fd_init_mutex(&sourcefns_lock);
  fd_init_mutex(&config_file_lock);
 u8_new_threadkey(&sourcebase_key,NULL);
#endif

 after_symbol=fd_intern("AFTEREXPR");
 
 fd_defspecial(fd_xscheme_module,"LOAD",load_source);
 fd_defspecial(fd_xscheme_module,"LOAD-COMPONENT",load_component);
 
 fd_idefn(fd_scheme_module,
	  fd_make_cprim1x("LOAD-CONFIG",lisp_load_config,1,
			  fd_string_type,FD_VOID));
 fd_idefn(fd_scheme_module,
	  fd_make_cprim2x("GET-COMPONENT",lisp_get_component,1,
			  fd_string_type,FD_VOID,
			  fd_string_type,FD_VOID));
 
 fd_register_config("CONFIG","Add a CONFIG file or URI to process",
		    get_config_files,add_config_file,NULL);
 fd_register_config("TRACELOAD","Trace file load starts and ends",
		    fd_boolconfig_get,fd_boolconfig_set,&trace_load);
}

