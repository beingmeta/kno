/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8printf.h>
#include <libu8/u8filefns.h>

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static fdtype loadstamp_symbol, moduleid_symbol;

fd_exception fd_NotAModule=_("Argument is not a module (table)");
fd_exception fd_NoSuchModule=_("Can't find named module");
fd_exception MissingModule=_("Loading failed to resolve module");
fd_exception OpaqueModule=_("Can't switch to opaque module");

static struct MODULE_LOADER {
  int (*loader)(fdtype,int,void *); void *data;
  struct MODULE_LOADER *next;} *module_loaders=NULL;

static u8_mutex module_loaders_lock;
static u8_mutex module_wait_lock;
static u8_condvar module_wait;

static int trace_dload=0;
static int auto_fix_modules=0;
static int auto_fix_exports=0;
static int auto_lock_modules=0;
static int auto_lock_exports=0;
static int auto_static_modules=0;
static int auto_static_exports=0;

static int readonly_tablep(fdtype arg)
{
  fd_ptr_type type=FD_PTR_TYPE(arg);
  switch (type) {
  case fd_hashtable_type:
    return FD_HASHTABLE_READONLYP(arg);
  case fd_slotmap_type:
    return FD_SLOTMAP_READONLYP(arg);
  case fd_schemap_type:
    return FD_SCHEMAP_READONLYP(arg);
  default:
    return 0;}
}

/* Design for avoiding the module loading race condition */

/* Have a list of modules being sought; a function fd_need_module
    locks a mutex and does a fd_get_module.  If it gets non-void,
    it unlikes its mutext and returns it.  Otherwise, if the module is
    on the list, it waits on the condvar and tries again (get and
    then list) it wakes up.  If the module isn't on the list,
    it puts it on the list, unlocks its mutex and returns
    FD_VOID, indicating that its caller can try to load it.
    Finishing a module pops it off of the seeking list.
    If loading the module fails, it's also popped off of
    the seeking list.
  And while we're at it, it looks like finish_module should
   wake up the loadstamp condvar!
*/

/* Getting the loadlock for a module */

static fdtype loading_modules=FD_EMPTY_CHOICE;

static fdtype getloadlock(fdtype spec,int safe)
{
  fdtype module;
  u8_lock_mutex(&module_wait_lock);
  module=fd_get_module(spec,safe);
  if (!(FD_VOIDP(module))) {
    u8_unlock_mutex(&module_wait_lock);
    return module;}
  if (fd_choice_containsp(spec,loading_modules)) {
    while (FD_VOIDP(module)) {
      u8_condvar_wait(&module_wait,&module_wait_lock);
      module=fd_get_module(spec,safe);}
    loading_modules=fd_difference(loading_modules,spec);
    u8_unlock_mutex(&module_wait_lock);
    return module;}
  else {
    fd_incref(spec);
    FD_ADD_TO_CHOICE(loading_modules,spec);
    u8_unlock_mutex(&module_wait_lock);
    return FD_VOID;}
}

static void clearloadlock(fdtype spec)
{
  u8_lock_mutex(&module_wait_lock);
  if (fd_choice_containsp(spec,loading_modules)) {
    fdtype prev_loading=loading_modules;
    loading_modules=fd_difference(loading_modules,spec);
    fd_decref(prev_loading);
    u8_condvar_broadcast(&module_wait);}
  u8_unlock_mutex(&module_wait_lock);
}

/* Getting modules */

FD_EXPORT
fdtype fd_find_module(fdtype spec,int safe,int err)
{
  fdtype module=fd_get_module(spec,safe);
  if (FD_VOIDP(module)) module=getloadlock(spec,safe);
  if (!(FD_VOIDP(module))) {
    fdtype loadstamp=fd_get(module,loadstamp_symbol,FD_VOID);
    while (FD_VOIDP(loadstamp)) {
      u8_lock_mutex(&module_wait_lock);
      u8_condvar_wait(&module_wait,&module_wait_lock);
      loadstamp=fd_get(module,loadstamp_symbol,FD_VOID);}
    clearloadlock(spec);
    if (FD_ABORTP(loadstamp)) return loadstamp;
    else {
      fd_decref(loadstamp);
      return module;}}
  else {
    struct MODULE_LOADER *scan=module_loaders;
    if ((FD_SYMBOLP(spec)) || (FD_STRINGP(spec))) {}
    else {
      clearloadlock(spec);
      return fd_type_error(_("module name"),"fd_find_module",spec);}
    while (scan) {
      int retval=scan->loader(spec,safe,scan->data);
      if (retval>0) {
        clearloadlock(spec);
        module=fd_get_module(spec,safe);
        if (FD_VOIDP(module)) {
          u8_string modname=(FD_SYMBOLP(spec)) ? (FD_SYMBOL_NAME(spec)) :
            (FD_STRDATA(spec));
          return fd_err(MissingModule,"fd_find_module",modname,spec);}
        fd_finish_module(module);
        return module;}
      else if (retval<0) {
        fd_discard_module(spec,safe);
        clearloadlock(spec);
        return FD_ERROR_VALUE;}
      else scan=scan->next;}
    clearloadlock(spec);
    if (err)
      return fd_err(fd_NoSuchModule,"fd_find_module",NULL,spec);
    else return FD_FALSE;}
}

FD_EXPORT
int fd_finish_module(fdtype module)
{
  if (FD_TABLEP(module))
    if (readonly_tablep(module))
      return 0;
    else {
      int flags= ((auto_fix_modules)?(FD_FIX_MODULES):(0)) |
        ((auto_fix_exports)?(FD_FIX_EXPORTS):(0)) |
        ((auto_lock_modules)?(FD_LOCK_MODULES):(0)) |
        ((auto_lock_exports)?(FD_LOCK_EXPORTS):(0)) |
        ((auto_static_modules)?(FD_STATIC_MODULES):(0)) |
        ((auto_static_exports)?(FD_STATIC_EXPORTS):(0));
      return fd_module_finished(module,flags);}
  else {
    fd_seterr(fd_NotAModule,"fd_finish_module",NULL,module);
    return -1;}
}

FD_EXPORT
int fd_module_finished(fdtype module,int flags)
{
  if (!(FD_TABLEP(module))) {
    fd_seterr(fd_NotAModule,"fd_finish_module",NULL,module);
    return -1;}
  else {
    struct U8_XTIME xtptr; fdtype timestamp;
    fdtype cur_timestamp=fd_get(module,loadstamp_symbol,FD_VOID);
    fdtype moduleid=fd_get(module,moduleid_symbol,FD_VOID);
    u8_init_xtime(&xtptr,-1,u8_second,0,0,0);
    timestamp=fd_make_timestamp(&xtptr);
    fd_store(module,loadstamp_symbol,timestamp);
    fd_decref(timestamp);
    if (FD_VOIDP(moduleid))
      u8_log(LOG_WARN,_("Anonymous module"),
             "The module %q doesn't have an id",module);
    /* In this case, the module was already finished,
       so the loadlock would have been cleared. */
    else if (cur_timestamp) {}
    else clearloadlock(moduleid);
    fd_decref(moduleid);
    fd_decref(cur_timestamp);
    if (FD_ENVIRONMENTP(module)) {
      fd_lispenv env=(fd_lispenv) module;
      fdtype exports=env->env_exports;
      if (FD_HASHTABLEP(env->env_bindings)) {
        if (U8_BITP(flags,FD_STATIC_MODULES))
          fd_static_module(env->env_bindings);
        else {}
        if (U8_BITP(flags,FD_LOCK_MODULES))
          fd_hashtable_set_readonly((fd_hashtable)(env->env_bindings),1);}
      if (FD_HASHTABLEP(exports)) {
        if (U8_BITP(flags,FD_STATIC_EXPORTS)) fd_static_module(exports);
        else {}
        if (U8_BITP(flags,FD_LOCK_EXPORTS))
          fd_hashtable_set_readonly((fd_hashtable)exports,1);}}
    else if (FD_HASHTABLEP(module)) {
      if (U8_BITP( flags, FD_STATIC_EXPORTS | FD_STATIC_MODULES))
        fd_static_module(module);
      else {}
      if (flags&(FD_LOCK_EXPORTS|FD_LOCK_MODULES)) fd_lock_exports(module);}
    else {}
    return 1;}
}

FD_EXPORT
int fd_static_module(fdtype module)
{
  if (FD_HASHTABLEP(module)) {
    int conversions=0;
    struct FD_HASHTABLE *ht=(fd_hashtable)module;
    conversions=conversions+fd_static_hashtable(ht,fd_sproc_type);
    conversions=conversions+fd_static_hashtable(ht,fd_primfcn_type);
    conversions=conversions+fd_static_hashtable(ht,fd_specform_type);
    return conversions;}
  else if (FD_TABLEP(module)) return 0;
  else {
    fd_seterr(fd_NotAModule,"fd_static_module",NULL,module);
    return -1;}
}

FD_EXPORT
int fd_lock_exports(fdtype module)
{
  if (FD_HASHTABLEP(module)) 
    return fd_hashtable_set_readonly((struct FD_HASHTABLE *) module, 1);
  else if (FD_ENVIRONMENTP(module)) {
    fd_lispenv env=(fd_lispenv) module;
    fdtype exports=env->env_exports;
    if (FD_HASHTABLEP(exports)) 
      return fd_hashtable_set_readonly((struct FD_HASHTABLE *) exports, 1);
    else return 0;}
  else {
    fd_incref(module);
    return fd_reterr(_("not a module"),"fd_lock_exports",NULL,module);}
}

FD_EXPORT
void fd_add_module_loader(int (*loader)(fdtype,int,void *),void *data)
{
  struct MODULE_LOADER *consed=u8_alloc(struct MODULE_LOADER);
  u8_lock_mutex(&module_loaders_lock);
  consed->loader=loader;
  consed->data=data;
  consed->next=module_loaders;
  module_loaders=consed;
  u8_unlock_mutex(&module_loaders_lock);
}

/* Loading dynamic libraries */

static fdtype dloadpath=FD_EMPTY_LIST;

static void init_dloadpath()
{
  u8_string tmp=u8_getenv("FD_INIT_DLOADPATH"); fdtype strval;
  if (tmp==NULL) 
    strval=fdtype_string(FD_DEFAULT_DLOADPATH);
  else strval=fd_lispstring(tmp);
  dloadpath=fd_init_pair(NULL,strval,dloadpath);
  if ((tmp)||(trace_dload)||(getenv("FD_DLOAD:TRACE")))
    u8_log(-LOGNOTICE,"DynamicLoadPath","Initialized to %q",
           dloadpath);
}

static int load_dynamic_module(fdtype spec,int safe,void *data)
{
  if (FD_SYMBOLP(spec)) {
    u8_string pname=FD_SYMBOL_NAME(spec);
    u8_string name=u8_downcase(FD_SYMBOL_NAME(spec)), alt_name=NULL;
    /* The alt name has a suffix, which lets the path elements be either
       % patterns (which may provide a suffix) or just directory names
       (which may not) */
    if (strchr(pname,'.')==NULL)
      alt_name=u8_mkstring("%ls.%s",pname,FD_DLOAD_SUFFIX);
    FD_DOLIST(elt,dloadpath) {
      if (FD_STRINGP(elt)) {
        u8_string module_filename=u8_find_file(name,FD_STRDATA(elt),NULL);
        if ((!(module_filename))&&(alt_name))
          module_filename=u8_find_file(alt_name,FD_STRDATA(elt),NULL);
        if (module_filename) {
          void *mod=u8_dynamic_load(module_filename);
          if (mod==NULL) {
            u8_log(LOGWARN,_("FailedModule"),
                   "Failed to load module file %s for %q",
                   module_filename,spec);}
          else if (trace_dload) {
            u8_log(LOGNOTICE,_("DynamicLoad"),
                   "Loaded module %q from %s",spec,module_filename);}
          else {}
          if (errno) {
            u8_log(LOGWARN,u8_UnexpectedErrno,
                   "Leftover errno %d (%s) from loading %s",
                   errno,u8_strerror(errno),pname);
            errno=0;}
          u8_threadcheck();
          u8_free(module_filename); u8_free(name);
          if (alt_name) u8_free(alt_name);
          if (mod) return 1; else return -1;}}}
    if (alt_name) u8_free(alt_name);
    u8_free(name);
    u8_threadcheck();
    return 0;}
  else if  ((safe==0) &&
            (FD_STRINGP(spec)) &&
            (u8_file_existsp(FD_STRDATA(spec)))) {
    void *mod=u8_dynamic_load(FD_STRDATA(spec));
    u8_threadcheck();
    if (mod) return 1; else return -1;}
  else return 0;
}

static fdtype dynamic_load_prim(fdtype arg)
{
  u8_string name=FD_STRING_DATA(arg);
  if (*name=='/') {
    void *mod=u8_dynamic_load(name);
    if (mod) return FD_TRUE;
    else return FD_ERROR_VALUE;}
  else {
    FD_DOLIST(elt,dloadpath) {
      if (FD_STRINGP(elt)) {
        u8_string module_name=u8_find_file(name,FD_STRDATA(elt),NULL);
        if (module_name) {
          void *mod=u8_dynamic_load(module_name);
          u8_free(module_name);
          if (mod) return FD_TRUE;
          else return FD_ERROR_VALUE;}}}
    return FD_FALSE;}
}

/* Switching modules */

static fd_lispenv become_module
   (fd_lispenv env,fdtype module_name,int safe,int create)
{
  fdtype module_spec, module;
  if (FD_STRINGP(module_name))
    module_spec=fd_intern(FD_STRDATA(module_name));
  else module_spec=fd_eval(module_name,env);
  if (FD_SYMBOLP(module_spec))
    module=fd_get_module(module_spec,safe);
  else {
    module=module_spec;
    fd_incref(module);}
  if (FD_ABORTP(module)) return NULL;
  else if ((!(create))&&(FD_VOIDP(module))) {
    fd_seterr(fd_NoSuchModule,"become_module",NULL,module_spec);
    fd_decref(module);
    return NULL;}
  else if (FD_HASHTABLEP(module)) {
    fd_seterr(OpaqueModule,"become_module",NULL,module_spec);
    fd_decref(module);
    return NULL;}
  else if (FD_ENVIRONMENTP(module)) {
    FD_ENVIRONMENT *menv=
      fd_consptr(FD_ENVIRONMENT *,module,fd_environment_type);
    if (menv != env) {
      fd_decref(((fdtype)(env->env_parent)));
      env->env_parent=(fd_lispenv)fd_incref((fdtype)menv->env_parent);
      fd_decref(env->env_bindings); env->env_bindings=fd_incref(menv->env_bindings);
      fd_decref(env->env_exports); env->env_exports=fd_incref(menv->env_exports);}}
  else if (FD_VOIDP(module)) {
    if (!(FD_HASHTABLEP(env->env_exports)))
      env->env_exports=fd_make_hashtable(NULL,0);
    else if (FD_HASHTABLE_READONLYP(env->env_exports)) {
      fd_seterr(_("Can't reload a read-only module"),
                "become_module",NULL,module_spec);
      return NULL;}
    else {}
    fd_store(env->env_exports,moduleid_symbol,module_spec);
    fd_register_module(FD_SYMBOL_NAME(module_spec),(fdtype)env,
                       ((safe) ? (FD_MODULE_SAFE) : (0)));}
  else {
    fd_seterr(fd_NotAModule,"use_module",NULL,module_spec);
    return NULL;}
  fd_decref(module); fd_decref(module_spec);
  return env;
}
static fdtype safe_in_module(fdtype expr,fd_lispenv env)
{
  fdtype module_name=fd_get_arg(expr,1);
  if (FD_VOIDP(module_name))
    return fd_err(fd_TooFewExpressions,"IN-MODULE",NULL,expr);
  else if (become_module(env,module_name,1,1)) return FD_VOID;
  else return FD_ERROR_VALUE;
}

static fdtype in_module(fdtype expr,fd_lispenv env)
{
  fdtype module_name=fd_get_arg(expr,1);
  if (FD_VOIDP(module_name))
    return fd_err(fd_TooFewExpressions,"IN-MODULE",NULL,expr);
  else if (become_module(env,module_name,0,1)) return FD_VOID;
  else return FD_ERROR_VALUE;
}

static fdtype safe_within_module(fdtype expr,fd_lispenv env)
{
  fd_lispenv consed_env=fd_working_environment();
  fdtype module_name=fd_get_arg(expr,1);
  if (FD_VOIDP(module_name)) {
    fd_decref((fdtype)consed_env);
    return fd_err(fd_TooFewExpressions,"WITHIN-MODULE",NULL,expr);}
  else if (become_module(consed_env,module_name,1,0)) {
    fdtype result=FD_VOID, body=fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result); result=fd_eval(elt,consed_env);}
    fd_decref((fdtype)consed_env);
    return result;}
  else return FD_ERROR_VALUE;
}

static fdtype within_module(fdtype expr,fd_lispenv env)
{
  fd_lispenv consed_env=fd_working_environment();
  fdtype module_name=fd_get_arg(expr,1);
  if (FD_VOIDP(module_name)) {
    fd_decref((fdtype)consed_env);
    return fd_err(fd_TooFewExpressions,"WITHIN-MODULE",NULL,expr);}
  else if (become_module(consed_env,module_name,0,0)) {
    fdtype result=FD_VOID, body=fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result); result=fd_eval(elt,consed_env);}
    fd_decref((fdtype)consed_env);
    return result;}
  else return FD_ERROR_VALUE;
}

static fd_lispenv make_hybrid_env(fd_lispenv base,fdtype module_spec,int safe)
{
  fdtype module=
    ((FD_ENVIRONMENTP(module_spec)) ?
     (fd_incref(module_spec)) :
     (fd_get_module(module_spec,safe)));
  if (FD_ABORTP(module)) {
    fd_interr(module);
    return NULL;}
  else if (FD_HASHTABLEP(module)) {
    fd_seterr(OpaqueModule,"IN-MODULE",NULL,module_spec);
    return NULL;}
  else if (FD_ENVIRONMENTP(module)) {
    FD_ENVIRONMENT *menv=
      fd_consptr(FD_ENVIRONMENT *,module,fd_environment_type);
    fd_lispenv result=fd_make_env(fd_incref(base->env_bindings),menv);
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
    return FD_ERROR_VALUE;}
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
    return FD_ERROR_VALUE;}
}

/* Exporting from modules */

static u8_mutex exports_lock;

static fd_hashtable get_exports(fd_lispenv env)
{
  fd_hashtable exports;
  fdtype moduleid=fd_get(env->env_bindings,moduleid_symbol,FD_VOID);
  u8_lock_mutex(&exports_lock);
  if (FD_HASHTABLEP(env->env_exports)) {
    u8_unlock_mutex(&exports_lock);
    fd_decref(moduleid);
    return (fd_hashtable) env->env_exports;}
  exports=(fd_hashtable)(env->env_exports=fd_make_hashtable(NULL,16));
  if (!(FD_VOIDP(moduleid)))
    fd_hashtable_store(exports,moduleid_symbol,moduleid);
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
  if (FD_ABORTP(symbols)) return symbols;
  {FD_DO_CHOICES(symbol,symbols)
      if (!(FD_SYMBOLP(symbol))) {
        fd_decref(symbols);
        return fd_type_error(_("symbol"),"module_export",symbol);}}
  if (FD_HASHTABLEP(env->env_exports))
    exports=(fd_hashtable)env->env_exports;
  else exports=get_exports(env);
  {FD_DO_CHOICES(symbol,symbols) {
    fdtype val=fd_get(env->env_bindings,symbol,FD_VOID);
    fd_hashtable_store(exports,symbol,val);
    fd_decref(val);}}
  fd_decref(symbols);
  return FD_VOID;
}

/* Using modules */

static int uses_bindings(fd_lispenv env,fdtype bindings)
{
  fd_lispenv scan=env;
  while (scan)
    if (scan->env_bindings==bindings) return 1;
    else scan=scan->env_parent;
  return 0;
}

static fdtype use_module_handler(fdtype expr,fd_lispenv env,int safe)
{
  fdtype module_names=fd_eval(fd_get_arg(expr,1),env);
  fd_lispenv modify_env=env;
  if (FD_VOIDP(module_names))
    return fd_err(fd_TooFewExpressions,"USE-MODULE",NULL,expr);
  else {
    FD_DO_CHOICES(module_name,module_names) {
      fdtype module;
      module=fd_find_module(module_name,safe,1);
      if (FD_ABORTP(module))
        return module;
      else if (FD_VOIDP(module))
        return fd_err(fd_NoSuchModule,"USE-MODULE",NULL,module_name);
      else if (FD_HASHTABLEP(module)) {
        if (!(uses_bindings(env,module))) {
          fd_lispenv old_parent;
          /* Use a dynamic copy if the enviroment is static, so it
             gets freed. */
          if (env->env_copy != env) modify_env=fd_copy_env(env);
          old_parent=modify_env->env_parent;
          modify_env->env_parent=fd_make_export_env(module,old_parent);
          /* We decref this because 'env' is no longer pointing to it
             and fd_make_export_env incref'd it again. */
          if (old_parent) fd_decref((fdtype)(old_parent));}}
      else {
        fd_lispenv expenv=
          fd_consptr(fd_environment,module,fd_environment_type);
        fdtype expval=(fdtype)get_exports(expenv);
        if (!(uses_bindings(env,expval))) {
          if (env->env_copy != env) modify_env=fd_copy_env(env);
          fd_lispenv old_parent=modify_env->env_parent;
          modify_env->env_parent=fd_make_export_env(expval,old_parent);
          /* We decref this because 'env' is no longer pointing to it
             and fd_make_export_env incref'd it again. */
          if (old_parent) fd_decref((fdtype)(old_parent));}}
      fd_decref(module);}
    fd_decref(module_names);
    if (modify_env!=env) {fd_decref((fdtype)modify_env);}
    return FD_VOID;}
}

static fdtype safe_use_module(fdtype expr,fd_lispenv env)
{
  return use_module_handler(expr,env,1);
}

static fdtype safe_get_module(fdtype modname)
{
  fdtype module=fd_find_module(modname,1,0);
  return module;
}

static fdtype use_module(fdtype expr,fd_lispenv env)
{
  return use_module_handler(expr,env,0);
}

static fdtype get_module(fdtype modname)
{
  fdtype module=fd_find_module(modname,0,0);
  return module;
}

static fdtype get_loaded_module(fdtype modname)
{
  fdtype module=fd_get_module(modname,0);
  if (FD_VOIDP(module))
    return FD_FALSE;
  else return module;
}

static fdtype safe_get_loaded_module(fdtype modname)
{
  fdtype module=fd_get_module(modname,1);
  if (FD_VOIDP(module))
    return FD_FALSE;
  else return module;
}

FD_EXPORT
fdtype fd_use_module(fd_lispenv env,fdtype module)
{
  int free_module=0;
  if (FD_SYMBOLP(module)) {
    module=get_module(module); free_module=1;}
  if (FD_HASHTABLEP(module)) {
    if (!(uses_bindings(env,module))) {
      fd_lispenv oldp=env->env_parent;
      env->env_parent=fd_make_export_env(module,oldp);
      fd_decref((fdtype)(oldp));}}
  else if (FD_SLOTMAPP(module)) {
    if (!(uses_bindings(env,module))) {
      fd_lispenv oldp=env->env_parent;
      env->env_parent=fd_make_env(module,oldp);
      fd_incref(module);
      fd_decref((fdtype)(oldp));}}
  else if (FD_ENVIRONMENTP(module)) {
    fd_lispenv expenv=
      fd_consptr(fd_environment,module,fd_environment_type);
    fdtype expval=(fdtype)get_exports(expenv);
    if (!(uses_bindings(env,expval))) {
      fd_lispenv oldp=env->env_parent;
      env->env_parent=fd_make_export_env(expval,oldp);
      if (oldp) fd_decref((fdtype)(oldp));}}
  else return fd_type_error("module","fd_use_module",module);
  if (free_module) fd_decref(module);
  return FD_VOID;
}

static fdtype static_module(fdtype module)
{
  if (FD_HASHTABLEP(module)) {
    int conversions=fd_static_module(module);
    if (conversions<0) return FD_ERROR_VALUE;
    else return FD_INT(conversions);}
  else if (FD_ENVIRONMENTP(module)) {
    fd_lispenv env=(fd_lispenv) module;
    int conversions=0, delta=0;
    if (FD_HASHTABLEP(env->env_bindings))
      delta=fd_static_module(env->env_bindings);
    if (conversions<0) return FD_ERROR_VALUE;
    else conversions=conversions+delta;
    if ((env->env_exports) && (FD_HASHTABLEP(env->env_exports)))
      delta=fd_static_module(env->env_exports);
    if (conversions<0) return FD_ERROR_VALUE;
    else return FD_INT(conversions+delta);}
  else {
    fdtype module_val=fd_find_module(module,0,0);
    if (FD_ABORTP(module_val)) return module_val;
    else {
      fdtype result=static_module(module_val);
      fd_decref(module_val);
      return result;}}
}

static fdtype get_exports_prim(fdtype arg)
{
  fdtype module=arg;
  if ((FD_STRINGP(arg))||(FD_SYMBOLP(arg)))
    module=fd_find_module(arg,0,0);
  else fd_incref(module);
  if (FD_ABORTP(module)) return module;
  else if (FD_VOIDP(module))
    return fd_err(fd_NoSuchModule,"USE-MODULE",NULL,arg);
  else if (FD_HASHTABLEP(module)) {
    fdtype keys=fd_getkeys(module);
    fd_decref(module);
    return keys;}
  else if (FD_TYPEP(module,fd_environment_type)) {
    fd_lispenv expenv=
      fd_consptr(fd_environment,module,fd_environment_type);
    fdtype expval=(fdtype)get_exports(expenv);
    if (FD_ABORTP(expval)) return expval;
    fdtype keys=fd_getkeys(expval);
    fd_decref(module);
    return keys;}
  else return FD_EMPTY_CHOICE;
}

static fdtype safe_get_exports_prim(fdtype arg)
{
  fdtype module=arg;
  if ((FD_STRINGP(arg))||(FD_SYMBOLP(arg)))
    module=fd_find_module(arg,1,0);
  else fd_incref(module);
  if (FD_ABORTP(module)) return module;
  else if (FD_VOIDP(module))
    return fd_err(fd_NoSuchModule,"USE-MODULE",NULL,arg);
  else if (FD_HASHTABLEP(module)) {
    fdtype keys=fd_getkeys(module);
    fd_decref(module);
    return keys;}
  else if (FD_TYPEP(module,fd_environment_type)) {
    fd_lispenv expenv=
      fd_consptr(fd_environment,module,fd_environment_type);
    fdtype expval=(fdtype)get_exports(expenv);
    if (FD_ABORTP(expval)) return expval;
    fdtype keys=fd_getkeys(expval);
    fd_decref(module);
    return keys;}
  else return FD_EMPTY_CHOICE;
}

/* LOADMODULE config */

static int loadmodule_sandbox=0;

static int loadmodule_config_set(fdtype var,fdtype val,void *ignored)
{
  fdtype module=fd_find_module(val,loadmodule_sandbox,0);
  if (FD_FALSEP(module)) {
    u8_log(LOG_WARN,"NoModuleLoaded","Couldn't load the module %q",val);
    return -1;}
  else return 1;
}

static fdtype loadmodule_config_get(fdtype var,void *ignored)
{
  return FD_FALSE;
}

static int loadmodule_sandbox_config_set(fdtype var,fdtype val,void *ignored)
{
  if (FD_FALSEP(val)) {
    if (!(loadmodule_sandbox)) return 0;
    else {
      fd_seterr("Can't reset LOADMODULE:SANDBOX",
                "loadmodule_sandbox_config_set",NULL,
                val);
      return -1;}}
  else {
    if (loadmodule_sandbox) return 0;
    else {
      loadmodule_sandbox=1;
      return 1;}}
}

static fdtype loadmodule_sandbox_config_get(fdtype var,void *ignored)
{
  if (loadmodule_sandbox) return FD_TRUE; else return FD_FALSE;
}

/* Initialization */

FD_EXPORT void fd_init_modules_c()
{
  u8_init_mutex(&module_loaders_lock);
  u8_init_mutex(&module_wait_lock);
  u8_init_condvar(&module_wait);
  u8_init_mutex(&exports_lock);

  fd_add_module_loader(load_dynamic_module,NULL);
  init_dloadpath();
  fd_register_config("DLOADPATH",
                     "Add directories for dynamic compiled modules",
                     fd_lconfig_get,fd_lconfig_push,&dloadpath);
  fd_register_config("DLOAD:PATH",
                     "Add directories for dynamic compiled modules",
                     fd_lconfig_get,fd_lconfig_push,&dloadpath);
  fd_register_config("DLOAD:TRACE",
                     "Whether to announce the loading of dynamic modules",
                     fd_boolconfig_get,fd_boolconfig_set,&trace_dload);

  loadstamp_symbol=fd_intern("%LOADSTAMP");
  moduleid_symbol=fd_intern("%MODULEID");

  fd_idefn(fd_xscheme_module,
           fd_make_cprim1x("DYNAMIC-LOAD",dynamic_load_prim,1,
                           fd_string_type,FD_VOID));
  fd_defalias(fd_xscheme_module,"DLOAD","DYNAMIC-LOAD");
  fd_defalias(fd_xscheme_module,"LOAD-DLL","DYNAMIC-LOAD");

  fd_defspecial(fd_scheme_module,"IN-MODULE",safe_in_module);
  fd_defspecial(fd_scheme_module,"WITHIN-MODULE",safe_within_module);
  fd_defalias(fd_scheme_module,"W/M","WITHIN-MODULE");
  fd_defalias(fd_scheme_module,"%WM","WITHIN-MODULE");
  fd_defspecial(fd_scheme_module,"ACCESSING-MODULE",safe_accessing_module);
  fd_defspecial(fd_scheme_module,"USE-MODULE",safe_use_module);
  fd_defspecial(fd_scheme_module,"MODULE-EXPORT!",module_export);
  fd_idefn(fd_scheme_module,fd_make_cprim1("GET-MODULE",safe_get_module,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("GET-LOADED-MODULE",safe_get_loaded_module,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("GET-EXPORTS",safe_get_exports_prim,1));
  fd_defalias(fd_scheme_module,"%LS","GET-EXPORTS");

  fd_defspecial(fd_xscheme_module,"IN-MODULE",in_module);
  fd_defspecial(fd_xscheme_module,"WITHIN-MODULE",within_module);
  fd_defalias(fd_xscheme_module,"W/M","WITHIN-MODULE");
  fd_defalias(fd_xscheme_module,"%WM","WITHIN-MODULE");
  fd_defspecial(fd_xscheme_module,"ACCESSING-MODULE",accessing_module);
  fd_defspecial(fd_xscheme_module,"USE-MODULE",use_module);
  fd_idefn(fd_xscheme_module,fd_make_cprim1("GET-MODULE",get_module,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("GET-LOADED-MODULE",get_loaded_module,1));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim1("GET-EXPORTS",get_exports_prim,1));
  fd_defalias(fd_xscheme_module,"%LS","GET-EXPORTS");

  fd_idefn(fd_scheme_module,fd_make_cprim1("STATIC-MODULE!",static_module,1));

  fd_register_config("LOCKEXPORTS",
                     "Lock the exports of modules when loaded",
                     fd_boolconfig_get,fd_boolconfig_set,
                     &auto_lock_exports);
  fd_register_config("LOCKMODULES",
                     "Lock bindings of source modules after they are loaded",
                     fd_boolconfig_get,fd_boolconfig_set,
                     &auto_lock_modules);
  fd_register_config("STATICMODULES",
                     "Convert function values in a module into static values",
                     fd_boolconfig_get,fd_boolconfig_set,
                     &auto_static_modules);
  fd_register_config("LOADMODULE",
                     "Specify modules to be loaded",
                     loadmodule_config_get,loadmodule_config_set,NULL);
  fd_register_config("LOADMODULE:SANDBOX",
                     "Whether LOADMODULE loads from the sandbox",
                     loadmodule_sandbox_config_get,
                     loadmodule_sandbox_config_set,
                     NULL);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
