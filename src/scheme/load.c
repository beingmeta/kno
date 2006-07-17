/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/dtypestream.h"

#include <libu8/u8.h>
#include <libu8/u8io.h>
#include <libu8/stringfns.h>
#include <libu8/pathfns.h>
#include <libu8/filefns.h>

#include <stdio.h>

fd_exception fd_NotAFilename=_("Not a filename");
fd_exception fd_FileNotFound=_("File not found");

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
  struct FD_SOURCEFN *new_entry=u8_malloc_type(struct FD_SOURCEFN);
  u8_lock_mutex(&sourcefns_lock);
  new_entry->getsource=fn; new_entry->next=sourcefns; sourcefns=new_entry;
  u8_unlock_mutex(&sourcefns_lock);
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
  if (content==NULL) return fd_erreify();
  else outer_sourcebase=bind_sourcebase(sourcebase);
  if ((input[0]=='#') && (input[1]=='!')) input=strchr(input,'\n');
  U8_INIT_STRING_INPUT((&stream),-1,input);
  {
    fdtype result=FD_VOID;
    fdtype expr=fd_parser(&stream,NULL), last_expr=FD_VOID;
    while (!(FD_TROUBLEP(expr))) {
      fd_decref(result);
      result=fd_eval(expr,env);
      fd_decref(last_expr);  last_expr=expr;
      if (FD_EXCEPTIONP(result)) {
	restore_sourcebase(outer_sourcebase);
	u8_free(sourcebase);
	u8_free(content);
	fd_decref(last_expr);
	return result;}
      expr=fd_parser(&stream,NULL);}
    if (expr==FD_PARSE_ERROR) result=fd_erreify();
    fd_decref(last_expr);
    restore_sourcebase(outer_sourcebase); u8_free(sourcebase);
    u8_free(content);
    return result;}
}

static u8_string get_component(u8_string spec)
{
  u8_string base=fd_sourcebase(), path; int len=strlen(spec);
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
  u8_string base=fd_sourcebase(), path; int len=strlen(spec);
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
  u8_byte *input=content;
  if (content==NULL) return fd_erreify();
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
  if (FD_EXCEPTIONP(source))
    return fd_passerr(source,fd_incref(expr));
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

static fdtype lisp_get_component(fdtype string)
{
  u8_string fullpath=get_component(FD_STRDATA(string));
  return fd_init_string(NULL,strlen(fullpath),fullpath);
}

static fdtype lisp_load_config(fdtype string)
{
  u8_string abspath=u8_abspath(FD_STRDATA(string),NULL);
  int retval=fd_load_config(abspath);
  u8_free(abspath);
  if (retval<0) {
    fdtype immediate_error=fd_erreify();
    fdtype underlying_error=fd_erreify();
    return fd_passerr(immediate_error,underlying_error);}
  else return FD_INT2DTYPE(retval);
}

/* Config config */

#if FD_THREADS_ENABLED
static u8_mutex config_lock;
#endif

static FD_CONFIG_RECORD *config_records=NULL, *config_stack=NULL;

static fdtype get_config_files(fdtype var)
{
  struct FD_CONFIG_RECORD *scan; fdtype result=FD_EMPTY_LIST;
  u8_lock_mutex(&config_lock);
  scan=config_records; while (scan) {
    result=fd_init_pair(NULL,fdtype_string(scan->source),result);
    scan=scan->next;}
  u8_unlock_mutex(&config_lock);
  return result;
}

static int add_config_file(fdtype var,fdtype val)
{
  if (FD_STRINGP(val)) {
    int retval;
    struct FD_CONFIG_RECORD on_stack, *scan, *newrec;
    u8_string pathname=u8_abspath(FD_STRDATA(val),NULL);
    u8_lock_mutex(&config_lock);
    scan=config_stack; while (scan)
      if (strcmp(scan->source,pathname)==0) {
	u8_unlock_mutex(&config_lock);
	u8_free(pathname);
	return 0;}
      else scan=scan->next;
    on_stack.source=pathname;
    on_stack.next=config_stack;
    config_stack=&on_stack;
    u8_unlock_mutex(&config_lock);
    retval=fd_load_config(pathname);
    if (retval<0) {
      u8_lock_mutex(&config_lock);
      u8_free(pathname); config_stack=on_stack.next;
      u8_unlock_mutex(&config_lock);
      return retval;}
    u8_lock_mutex(&config_lock);
    newrec=u8_malloc(sizeof(struct FD_CONFIG_RECORD));
    newrec->source=pathname;
    newrec->next=config_records;
    config_records=newrec;
    u8_unlock_mutex(&config_lock);
    return retval;}
  else return -1;
}
/* Initialization */

FD_EXPORT void fd_init_load_c()
{
  fd_register_source_file(versionid);

#if FD_THREADS_ENABLED
  u8_init_mutex(&sourcefns_lock);
  u8_init_mutex(&config_lock);
 u8_new_threadkey(&sourcebase_key,NULL);
#endif

  fd_defspecial(fd_xscheme_module,"LOAD",load_source);
  fd_defspecial(fd_xscheme_module,"LOAD-COMPONENT",load_component);

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("LOAD-CONFIG",lisp_load_config,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("GET-COMPONENT",lisp_get_component,1,
			   fd_string_type,FD_VOID));

  fd_register_config("CONFIG",get_config_files,add_config_file,NULL);
}


/* The CVS log for this file
   $Log: load.c,v $
   Revision 1.38  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.37  2006/01/23 00:34:59  haase
   Added API for sourcebase binding

   Revision 1.36  2006/01/19 21:52:25  haase
   Added fd_get_component

   Revision 1.35  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.34  2005/12/19 00:43:25  haase
   Fixed minor leak in file loading

   Revision 1.33  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.32  2005/07/23 22:21:48  haase
   Made load find an environment it can modify (e.g. a hashtable)

   Revision 1.31  2005/05/21 17:51:23  haase
   Added DTPROCs (remote procedures)

   Revision 1.30  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.29  2005/05/17 20:31:00  haase
   Fixed missing return keyword in loading

   Revision 1.28  2005/04/24 02:09:05  haase
   Don't use realpath until inside the file to get get/load-component to work right

   Revision 1.27  2005/04/21 19:02:55  haase
   Track last expr on loading

   Revision 1.26  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.25  2005/04/11 00:39:54  haase
   Convert load-time parse errors into exception objects

   Revision 1.24  2005/04/04 22:18:12  haase
   Added LOAD-CONFIG and the CONFIG config variable

   Revision 1.23  2005/03/30 14:48:44  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.22  2005/03/26 00:16:13  haase
   Made loading facility be generic and moved the rest of file access into fileio.c

   Revision 1.21  2005/03/25 13:25:11  haase
   Seperated out file io functions from generic port functions

   Revision 1.20  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.19  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.18  2005/03/04 04:08:33  haase
   Fixes for minor libu8 changes

   Revision 1.17  2005/02/23 23:14:26  haase
   Fixes to get-component handling

   Revision 1.16  2005/02/15 13:34:32  haase
   Updated fd_parser to use input streams rather than just strings

   Revision 1.15  2005/02/12 03:39:51  haase
   Updated loading to use u8_filestring

   Revision 1.14  2005/02/12 02:35:58  haase
   Added init for current file threadkey

   Revision 1.13  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
