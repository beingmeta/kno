/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/eval.h"

#include <libu8/libu8.h>
#include <libu8/stringfns.h>
#include <libu8/filefns.h>

static fdtype loadstamp_symbol;

fd_exception fd_NotAModule=_("Argument is not a module (table)");
fd_exception fd_NoSuchModule=_("Can't find named module");
fd_exception MissingModule=_("Loading failed to resolve module");
fd_exception OpaqueModule=_("Can't switch to opaque module");

static struct MODULE_LOADER {
  int (*loader)(u8_string,int);
  struct MODULE_LOADER *next;} *module_loaders=NULL;

#if FD_THREADS_ENABLED
static u8_mutex module_loaders_lock;
static u8_mutex module_wait_lock;
static u8_condvar module_wait;
#endif

/* Getting modules */

FD_EXPORT
fdtype fd_find_module(fdtype spec,int safe)
{
  fdtype module=fd_get_module(spec,safe);
  if (!(FD_VOIDP(module))) {
    fdtype loadstamp=fd_get(module,loadstamp_symbol,FD_VOID);
    while (FD_VOIDP(loadstamp)) {
      u8_condvar_wait(&module_wait,&module_wait_lock);
      loadstamp=fd_get(module,loadstamp_symbol,FD_VOID);}
    if (FD_EXCEPTIONP(loadstamp)) return loadstamp;
    else return module;}
  else {
    struct MODULE_LOADER *scan=module_loaders;
    u8_string module_name; int retval;
    if (FD_SYMBOLP(spec))
      module_name=u8_downcase(FD_SYMBOL_NAME(spec));
    else if (FD_STRINGP(spec))
      module_name=u8_downcase(FD_STRDATA(spec));
    else return fd_type_error(_("module name"),"fd_find_module",spec);
    while (scan) {
      int retval=scan->loader(module_name,safe);
      if (retval>0) {
	u8_free(module_name);
	module=fd_get_module(spec,safe);
	if (FD_VOIDP(module)) return fd_err(MissingModule,NULL,NULL,spec);
	fd_finish_module(module);
	return module;}
      else if (retval<0) return fd_erreify();
      else scan=scan->next;}
    return fd_err(fd_NoSuchModule,NULL,module_name,spec);}
}

FD_EXPORT
int fd_finish_module(fdtype module)
{
  if (FD_TABLEP(module)) {
    struct U8_XTIME xtptr; fdtype timestamp;
    u8_localtime(&xtptr,time(NULL));
    timestamp=fd_make_timestamp(&xtptr,NULL);
    fd_store(module,loadstamp_symbol,timestamp);
    fd_decref(timestamp);
    return 1;}
  else {
    fd_seterr(fd_NotAModule,"fd_finish_module",NULL,module);
    return -1;}
}

FD_EXPORT
void fd_add_module_loader(int (*loader)(u8_string,int))
{
  struct MODULE_LOADER *consed=u8_malloc(sizeof(struct MODULE_LOADER));
  u8_lock_mutex(&module_loaders_lock);
  consed->loader=loader;
  consed->next=module_loaders;
  module_loaders=consed;
  u8_unlock_mutex(&module_loaders_lock);
}

/* Loading dynamic libraries */

static fdtype dloadpath=FD_EMPTY_LIST;

static int load_dynamic_module(u8_string name,int safe)
{
  FD_DOLIST(elt,dloadpath) {
    if (FD_STRINGP(elt)) {
      u8_string module_name=u8_find_file(name,FD_STRDATA(elt),NULL);
      if (module_name) {
	void *mod=u8_dynamic_load(module_name);
	u8_free(module_name);
	if (mod) return 1; else return -1;}}}
  return 0;
}

static fdtype dynamic_load_prim(fdtype arg)
{
  u8_string name=FD_STRING_DATA(arg);
  if (*name=='/') {
    void *mod=u8_dynamic_load(name);
    if (mod) return FD_TRUE;
    else return fd_erreify();}
  else {
    FD_DOLIST(elt,dloadpath) {
      if (FD_STRINGP(elt)) {
	u8_string module_name=u8_find_file(name,FD_STRDATA(elt),NULL);
	if (module_name) {
	  void *mod=u8_dynamic_load(module_name);
	  u8_free(module_name);
	  if (mod) return FD_TRUE;
	  else return fd_erreify();}}}
    return FD_FALSE;}
}

/* Switching modules */

static fd_lispenv switch_module
  (fd_lispenv env,fdtype module_name,int safe)
{
  fdtype module_spec, module;
  if (FD_STRINGP(module_name))
    module_spec=fd_intern(FD_STRDATA(module_name));
  else module_spec=fd_eval(module_name,env);
  module=fd_get_module(module_spec,safe);
  if (FD_HASHTABLEP(module)) {
    fd_seterr(OpaqueModule,"IN-MODULE",NULL,module_spec);
    return NULL;}
  else if (FD_PRIM_TYPEP(module,fd_environment_type)) {
    FD_ENVIRONMENT *menv=
      FD_GET_CONS(module,fd_environment_type,FD_ENVIRONMENT *);
    if (menv != env) {
      fd_decref((fdtype)env->parent);
      env->parent=(fd_lispenv)fd_incref((fdtype)menv->parent);
      fd_decref(env->bindings); env->bindings=fd_incref(menv->bindings);
      fd_decref(env->exports); env->exports=fd_incref(menv->exports);}}
  else if (FD_VOIDP(module)) {
    if (!(FD_HASHTABLEP(env->exports)))
      env->exports=fd_make_hashtable(NULL,0,NULL);
    fd_register_module(FD_SYMBOL_NAME(module_spec),(fdtype)env,
		       ((safe) ? (FD_MODULE_SAFE) : (0)));}
  fd_decref(module);
  return env;
}
static fdtype safe_in_module(fdtype expr,fd_lispenv env)
{
  fdtype module_name=fd_get_arg(expr,1);
  if (FD_VOIDP(module_name))
    return fd_err(fd_TooFewExpressions,"IN-MODULE",NULL,expr);
  else if (switch_module(env,module_name,1)) return FD_VOID;
  else return fd_erreify();
}

static fdtype in_module(fdtype expr,fd_lispenv env)
{
  fdtype module_name=fd_get_arg(expr,1);
  if (FD_VOIDP(module_name))
    return fd_err(fd_TooFewExpressions,"IN-MODULE",NULL,expr);
  else if (switch_module(env,module_name,0)) return FD_VOID;
  else return fd_erreify();
}

static fdtype safe_within_module(fdtype expr,fd_lispenv env)
{
  fd_lispenv consed_env=fd_working_environment();
  fdtype module_name=fd_get_arg(expr,1);
  if (FD_VOIDP(module_name)) {
    fd_decref((fdtype)consed_env);
    return fd_err(fd_TooFewExpressions,"WITHIN-MODULE",NULL,expr);}
  else if (switch_module(consed_env,module_name,1)) {
    fdtype result=FD_VOID, body=fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result); result=fd_eval(elt,consed_env);}
    fd_decref((fdtype)consed_env);
    return result;}
  else return fd_erreify();
}

static fdtype within_module(fdtype expr,fd_lispenv env)
{
  fd_lispenv consed_env=fd_working_environment();
  fdtype module_name=fd_get_arg(expr,1);
  if (FD_VOIDP(module_name)) {
    fd_decref((fdtype)consed_env);
    return fd_err(fd_TooFewExpressions,"WITHIN-MODULE",NULL,expr);}
  else if (switch_module(consed_env,module_name,0)) {
    fdtype result=FD_VOID, body=fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result); result=fd_eval(elt,consed_env);}
    fd_decref((fdtype)consed_env);
    return result;}
  else return fd_erreify();
}

static fd_lispenv make_hybrid_env(fd_lispenv base,fdtype module_spec,int safe)
{
  fdtype module=
    ((FD_PRIM_TYPEP(module,fd_environment_type)) ?
     (fd_incref(module_spec)) :
     (fd_get_module(module_spec,safe)));
  if (FD_ABORTP(module)) {
    fd_interr(module);
    return NULL;}
  else if (FD_HASHTABLEP(module)) {
    fd_seterr(OpaqueModule,"IN-MODULE",NULL,module_spec);
    return NULL;}
  else if (FD_PRIM_TYPEP(module,fd_environment_type)) {
    FD_ENVIRONMENT *menv=
      FD_GET_CONS(module,fd_environment_type,FD_ENVIRONMENT *);
    fd_lispenv result=fd_make_env(fd_incref(base->bindings),menv);
    fd_decref(module);
    return result;}
  else if (FD_VOIDP(module)) {
    fd_seterr(fd_NoSuchModule,"USING-MODULE",NULL,fd_incref(module_spec));
    return NULL;}
  else {
    fd_seterr(fd_TypeError,"USING-MODULE",NULL,fd_incref(module));
    return NULL;}
}

static fdtype accessing_module(fdtype expr,fd_lispenv env)
{
  fdtype module_name=fd_eval(fd_get_arg(expr,1),env);
  fd_lispenv hybrid;
  if (FD_VOIDP(module_name)) {
    return fd_err(fd_TooFewExpressions,"WITHIN-MODULE",NULL,expr);}
  hybrid=make_hybrid_env(env,module_name,0);
  if (hybrid) {
    fdtype result=FD_VOID, body=fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result); result=fd_eval(elt,hybrid);}
    fd_decref(module_name);
    fd_decref((fdtype)hybrid);
    return result;}
  else {
    fd_decref(module_name);
    return fd_erreify();}
}

static fdtype safe_accessing_module(fdtype expr,fd_lispenv env)
{
  fdtype module_name=fd_eval(fd_get_arg(expr,1),env);
  fd_lispenv hybrid;
  if (FD_VOIDP(module_name)) {
    return fd_err(fd_TooFewExpressions,"WITHIN-MODULE",NULL,expr);}
  hybrid=make_hybrid_env(env,module_name,1);
  if (hybrid) {
    fdtype result=FD_VOID, body=fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result); result=fd_eval(elt,hybrid);}
    fd_decref(module_name);
    fd_decref((fdtype)hybrid);
    return result;}
  else {
    fd_decref(module_name);
    return fd_erreify();}
}

/* Exporting from modules */

#if FD_THREADS_ENABLED
static u8_mutex exports_lock;
#endif

static fd_hashtable get_exports(fd_lispenv env)
{
  fd_hashtable exports;
  u8_lock_mutex(&exports_lock);
  if (FD_HASHTABLEP(env->exports)) {
    u8_unlock_mutex(&exports_lock);
    return (fd_hashtable) env->exports;}
  exports=(fd_hashtable)(env->exports=fd_make_hashtable(NULL,16,NULL));
  u8_unlock_mutex(&exports_lock);
  return exports;
}

static fdtype module_export(fdtype expr,fd_lispenv env)
{
  fd_hashtable exports;
  fdtype symbols_spec=fd_get_arg(expr,1), symbols;
  if (FD_VOIDP(symbols_spec))
    return fd_err(fd_TooFewExpressions,"MODULE-EXPORT!",NULL,expr);
  symbols=fd_eval(symbols_spec,env);
  if (FD_EXCEPTIONP(symbols)) return symbols;
  {FD_DO_CHOICES(symbol,symbols)
      if (!(FD_SYMBOLP(symbol))) {
	fd_decref(symbols);
	return fd_type_error(_("symbol"),"module_export",symbol);}}
  if (FD_HASHTABLEP(env->exports))
    exports=(fd_hashtable)env->exports;
  else exports=get_exports(env);
  {FD_DO_CHOICES(symbol,symbols) {
    fdtype val=fd_get(env->bindings,symbol,FD_VOID);
    fd_hashtable_store(exports,symbol,val);
    fd_decref(val);}}
  fd_decref(symbols);
  return FD_VOID;
}

/* Using modules */

static fdtype safe_use_module(fdtype expr,fd_lispenv env)
{
  fdtype module_names=fd_eval(fd_get_arg(expr,1),env);
  if (FD_VOIDP(module_names))
    return fd_err(fd_TooFewExpressions,"USE-MODULE",NULL,expr);
  else {
    FD_DO_CHOICES(module_name,module_names) {
      fdtype module=fd_find_module(module_name,1);
      fd_lispenv oldparent=env->parent;
      if (FD_EXCEPTIONP(module))
	return module;
      else if (FD_VOIDP(module))
	return fd_err(fd_NoSuchModule,"USE-MODULE",NULL,module_name);
      else if (FD_HASHTABLEP(module)) {
	env->parent=fd_make_export_env(module,env->parent);}
      else {
	fd_lispenv expenv=
	  FD_GET_CONS(module,fd_environment_type,fd_environment);
	fdtype expval=(fdtype)get_exports(expenv);
	env->parent=fd_make_export_env(expval,env->parent);}
      fd_decref((fdtype)oldparent);}
    fd_decref(module_names);
    return FD_VOID;}
}

static fdtype safe_get_module(fdtype modname)
{
  fdtype module=fd_find_module(modname,1);
  return module;
}

static fdtype use_module(fdtype expr,fd_lispenv env)
{
  fdtype module_names=fd_eval(fd_get_arg(expr,1),env);
  if (FD_VOIDP(module_names)) 
    return fd_err(fd_TooFewExpressions,"USE-MODULE",NULL,expr);
  else {
    FD_DO_CHOICES(module_name,module_names) {
      fdtype module;
      module=fd_find_module(module_name,0);
      if (FD_EXCEPTIONP(module))
	return module;
      else if (FD_VOIDP(module))
	return fd_err(fd_NoSuchModule,"USE-MODULE",NULL,module_name);
      else if (FD_HASHTABLEP(module)) {
	fd_lispenv oldparent=env->parent;
	env->parent=fd_make_export_env(module,env->parent);
	fd_decref((fdtype)(oldparent));}
      else {
	fd_lispenv oldparent=env->parent;
	fd_lispenv expenv=
	  FD_GET_CONS(module,fd_environment_type,fd_environment);
	fdtype expval=(fdtype)get_exports(expenv);
	env->parent=fd_make_export_env(expval,env->parent);
	fd_decref((fdtype)(oldparent));}
      fd_decref(module);}
    fd_decref(module_names);
    return FD_VOID;}
}

static fdtype get_module(fdtype modname)
{
  fdtype module=fd_find_module(modname,0);
  return module;
}

/* Initialization */

FD_EXPORT void fd_init_modules_c()
{
#if FD_THREADS_ENABLED
  u8_init_mutex(&module_loaders_lock);
  u8_init_mutex(&module_wait_lock);
  u8_init_condvar(&module_wait);
  u8_init_mutex(&exports_lock);
#endif

  fd_add_module_loader(load_dynamic_module);
  fd_register_config("DLLOADPATH",fd_lconfig_get,fd_lconfig_push,&dloadpath);

  {
    u8_string path=u8_getenv("FD_DLLOADPATH");
    fdtype v=((path) ? (fd_init_string(NULL,-1,path)) :
	      (fdtype_string(FD_DEFAULT_DLLOADPATH)));
    fd_config_set("DLLOADPATH",v);
    fd_decref(v);}

  loadstamp_symbol=fd_intern("%LOADSTAMP");
  /* loadfile_symbol=fd_intern("%LOADFILE"); */

  fd_idefn(fd_xscheme_module,
	   fd_make_cprim1x("DYNAMIC-LOAD",dynamic_load_prim,1,
			   fd_string_type,FD_VOID));

  fd_defspecial(fd_scheme_module,"IN-MODULE",safe_in_module);
  fd_defspecial(fd_scheme_module,"WITHIN-MODULE",safe_within_module);
  fd_defspecial(fd_scheme_module,"ACCESSING-MODULE",safe_accessing_module);
  fd_defspecial(fd_scheme_module,"USE-MODULE",safe_use_module);
  fd_defspecial(fd_scheme_module,"MODULE-EXPORT!",module_export);
  fd_idefn(fd_scheme_module,fd_make_cprim1("GET-MODULE",safe_get_module,1));

  fd_defspecial(fd_xscheme_module,"IN-MODULE",in_module);
  fd_defspecial(fd_xscheme_module,"WITHIN-MODULE",within_module);
  fd_defspecial(fd_xscheme_module,"ACCESSING-MODULE",accessing_module);
  fd_defspecial(fd_xscheme_module,"USE-MODULE",use_module);
  fd_idefn(fd_xscheme_module,fd_make_cprim1("GET-MODULE",get_module,1));
}


/* The CVS log for this file
   $Log: modules.c,v $
   Revision 1.32  2006/01/27 23:20:02  haase
   Fix to safe USE-MODULE

   Revision 1.31  2006/01/27 22:06:14  haase
   Fixed leak in USE-MODULE

   Revision 1.30  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.29  2006/01/18 04:33:06  haase
   Fixed various iteration binding bugs

   Revision 1.28  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.27  2005/12/19 00:43:05  haase
   Fixed typo in ACCESSING-MODULE declaration

   Revision 1.26  2005/11/11 05:37:50  haase
   Added DYNAMIC-LOAD primitive

   Revision 1.25  2005/09/13 03:34:30  haase
   Fixed WITHIN-MODULE and added ACCESSING-MODULE

   Revision 1.24  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.23  2005/08/04 23:24:13  haase
   Added (optional) automatic module updating

   Revision 1.22  2005/07/21 00:19:44  haase
   Fixed some error passing bugs

   Revision 1.21  2005/06/23 15:51:19  haase
   Fixed some module GC bugs

   Revision 1.20  2005/06/23 02:08:25  haase
   Added cast for change to switch_module

   Revision 1.19  2005/06/23 02:07:17  haase
   Fixed GC bug in switch_module and added GET-MODULE primitive

   Revision 1.18  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.17  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.16  2005/05/04 09:42:42  haase
   Added module loading locking stuff

   Revision 1.15  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.14  2005/04/14 16:22:44  haase
   Add distinction of export environments for environments created by use-module etc.

   Revision 1.13  2005/04/13 15:00:25  haase
   Fixed USE-MODULE to handle non-determnistic arguments

   Revision 1.12  2005/04/06 19:24:51  haase
   Fixes to the module system

   Revision 1.11  2005/04/03 16:35:22  haase
   Added XML output functionality

   Revision 1.10  2005/04/01 02:42:40  haase
   Reimplemented module exports to be faster and less kludgy

   Revision 1.9  2005/03/30 14:48:44  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.8  2005/03/26 00:16:13  haase
   Made loading facility be generic and moved the rest of file access into fileio.c

   Revision 1.7  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.6  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.5  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
