/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
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

#if HAVE_DLFCN_H
#include <dlfcn.h>
#endif

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static lispval loadstamp_symbol, moduleid_symbol, source_symbol, safemod_symbol;

u8_condition fd_NotAModule=_("Argument is not a module (table)");
u8_condition fd_NoSuchModule=_("Can't find named module");
u8_condition MissingModule=_("Loading failed to resolve module");
u8_condition OpaqueModule=_("Can't switch to opaque module");

static struct MODULE_LOADER {
  int (*loader)(lispval,int,void *); void *data;
  struct MODULE_LOADER *next;} *module_loaders = NULL;

static struct FD_HASHTABLE module_map, safe_module_map;
static fd_lexenv default_env = NULL, safe_default_env = NULL;
lispval fd_scheme_module = FD_VOID, fd_xscheme_module = FD_VOID;

static u8_mutex module_loaders_lock;
static u8_mutex module_wait_lock;
static u8_condvar module_wait;

static int trace_dload = 0;
static int auto_fix_modules = 0;
static int auto_fix_exports = 0;
static int auto_lock_modules = 0;
static int auto_lock_exports = 0;
static int auto_static_modules = 0;
static int auto_static_exports = 0;

static int readonly_tablep(lispval arg)
{
  fd_ptr_type type = FD_PTR_TYPE(arg);
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

/* Module system */

FD_EXPORT fd_lexenv fd_make_env(lispval bindings,fd_lexenv parent)
{
  if (PRED_FALSE(!(TABLEP(bindings)) )) {
    u8_byte buf[100];
    fd_seterr(fd_TypeError,"fd_make_env",
              u8_sprintf(buf,100,_("object is not a %m"),"table"),
              bindings);
    return NULL;}
  else {
    struct FD_LEXENV *e = u8_alloc(struct FD_LEXENV);
    FD_INIT_FRESH_CONS(e,fd_lexenv_type);
    e->env_bindings = bindings; e->env_exports = VOID;
    e->env_parent = fd_copy_env(parent);
    e->env_copy = e;
    return e;}
}


FD_EXPORT
/* fd_make_export_env:
    Arguments: a hashtable and an environment
    Returns: a consed environment whose bindings and exports
  are the exports table.  This indicates that the environment
  is "for export only" and cannot be modified. */
fd_lexenv fd_make_export_env(lispval exports,fd_lexenv parent)
{
  if (PRED_FALSE(!(HASHTABLEP(exports)) )) {
    u8_byte buf[100];
    fd_seterr(fd_TypeError,"fd_make_env",
              u8_sprintf(buf,100,_("object is not a %m"),"hashtable"),
              exports);
    return NULL;}
  else {
    struct FD_LEXENV *e = u8_alloc(struct FD_LEXENV);
    FD_INIT_FRESH_CONS(e,fd_lexenv_type);
    e->env_bindings = fd_incref(exports);
    e->env_exports = fd_incref(e->env_bindings);
    e->env_parent = fd_copy_env(parent);
    e->env_copy = e;
    return e;}
}

FD_EXPORT fd_lexenv fd_new_lexenv(lispval bindings,int safe)
{
  if (fd_scheme_initialized==0) fd_init_scheme();
  if (VOIDP(bindings))
    bindings = fd_make_hashtable(NULL,17);
  else fd_incref(bindings);
  return fd_make_env(bindings,((safe)?(safe_default_env):(default_env)));
}
FD_EXPORT fd_lexenv fd_working_lexenv()
{
  if (fd_scheme_initialized==0) fd_init_scheme();
  return fd_make_env(fd_make_hashtable(NULL,17),default_env);
}
FD_EXPORT fd_lexenv fd_safe_working_lexenv()
{
  if (fd_scheme_initialized==0) fd_init_scheme();
  return fd_make_env(fd_make_hashtable(NULL,17),safe_default_env);
}

FD_EXPORT lispval fd_register_module_x(lispval name,lispval module,int flags)
{
  if (flags&FD_MODULE_SAFE)
    fd_hashtable_store(&safe_module_map,name,module);
  else fd_hashtable_store(&module_map,name,module);

  /* Set the module ID*/
  if (FD_LEXENVP(module)) {
    fd_lexenv env = (fd_lexenv)module;
    fd_add(env->env_bindings,moduleid_symbol,name);}
  else if (HASHTABLEP(module))
    fd_add(module,moduleid_symbol,name);
  else {}

  /* Add to the appropriate default environment */
  if (flags&FD_MODULE_DEFAULT) {
    fd_lexenv scan;
    if (flags&FD_MODULE_SAFE) {
      scan = safe_default_env;
      while (scan)
        if (FD_EQ(scan->env_bindings,module))
          /* It's okay to return now, because if it's in the safe module
             defaults it's also in the risky module defaults. */
          return module;
        else scan = scan->env_parent;
      safe_default_env->env_parent=
        fd_make_env(fd_incref(module),safe_default_env->env_parent);}
    scan = default_env;
    while (scan)
      if (FD_EQ(scan->env_bindings,module)) return module;
      else scan = scan->env_parent;
    default_env->env_parent=
      fd_make_env(fd_incref(module),default_env->env_parent);}
  return module;
}

FD_EXPORT lispval fd_register_module(u8_string name,lispval module,int flags)
{
  return fd_register_module_x(fd_intern(name),module,flags);
}

FD_EXPORT lispval fd_new_module(char *name,int flags)
{
  lispval module_name, module, as_stored;
  if (fd_scheme_initialized==0) fd_init_scheme();
  module_name = fd_intern(name);
  module = fd_make_hashtable(NULL,0);
  fd_add(module,moduleid_symbol,module_name);
  if (flags&FD_MODULE_SAFE)
    fd_store(module,safemod_symbol,FD_TRUE);
  struct FD_HASHTABLE *modmap =
    (flags&FD_MODULE_SAFE) ? (&safe_module_map) : (&module_map);
  fd_hashtable_op(modmap,fd_table_default,module_name,module);
  as_stored = fd_get((lispval)modmap,module_name,VOID);
  if (!(FD_EQ(module,as_stored))) {
    fd_decref(module);
    return as_stored;}
  else fd_decref(as_stored);
  if (flags&FD_MODULE_DEFAULT) {
    if (flags&FD_MODULE_SAFE)
      safe_default_env->env_parent = fd_make_env(module,safe_default_env->env_parent);
    default_env->env_parent = fd_make_env(module,default_env->env_parent);}
  return module;}

FD_EXPORT lispval fd_new_cmodule(char *name,int flags,void *addr)
{
  lispval mod = fd_new_module(name,flags);
#if HAVE_DLADDR
  Dl_info  dlinfo;
  if (dladdr(addr,&dlinfo)) {
    const char *cfilename = dlinfo.dli_fname;
    if (cfilename) {
      u8_string filename = u8_fromlibc((char *)cfilename);
      lispval fname = fd_make_string(NULL,-1,filename);
      u8_free(filename);
      fd_add(mod,source_symbol,fname);
      fd_decref(fname);}}
#endif
  return mod;
}

FD_EXPORT lispval fd_get_module(lispval name,int safe)
{
  if (safe)
    return fd_hashtable_get(&safe_module_map,name,VOID);
  else {
    lispval module = fd_hashtable_get(&module_map,name,VOID);
    if (VOIDP(module))
      return fd_hashtable_get(&safe_module_map,name,VOID);
    else return module;}
}

FD_EXPORT lispval fd_all_modules()
{
  return fd_hashtable_assocs(&module_map);
}

FD_EXPORT lispval fd_safe_modules()
{
  return fd_hashtable_assocs(&safe_module_map);
}

FD_EXPORT int fd_discard_module(lispval name,int safe)
{
  if (safe)
    return fd_hashtable_store(&safe_module_map,name,VOID);
  else {
    lispval module = fd_hashtable_get(&module_map,name,VOID);
    if (VOIDP(module))
      return fd_hashtable_store(&safe_module_map,name,VOID);
    else {
      fd_decref(module);
      return fd_hashtable_store(&module_map,name,VOID);}}
}

/* Design for avoiding the module loading race condition */

/* Have a list of modules being sought; a function fd_need_module
    locks a mutex and does a fd_get_module.  If it gets non-void,
    it unlikes its mutext and returns it.  Otherwise, if the module is
    on the list, it waits on the condvar and tries again (get and
    then list) it wakes up.  If the module isn't on the list,
    it puts it on the list, unlocks its mutex and returns
    VOID, indicating that its caller can try to load it.
    Finishing a module pops it off of the seeking list.
    If loading the module fails, it's also popped off of
    the seeking list.
  And while we're at it, it looks like finish_module should
   wake up the loadstamp condvar!
*/

/* Getting the loadlock for a module */

static lispval loading_modules = EMPTY;

static lispval getloadlock(lispval spec,int safe)
{
  lispval module;
  u8_lock_mutex(&module_wait_lock);
  module = fd_get_module(spec,safe);
  if (!(VOIDP(module))) {
    u8_unlock_mutex(&module_wait_lock);
    return module;}
  if (fd_choice_containsp(spec,loading_modules)) {
    while (VOIDP(module)) {
      u8_condvar_wait(&module_wait,&module_wait_lock);
      module = fd_get_module(spec,safe);}
    loading_modules = fd_difference(loading_modules,spec);
    u8_unlock_mutex(&module_wait_lock);
    return module;}
  else {
    fd_incref(spec);
    CHOICE_ADD(loading_modules,spec);
    u8_unlock_mutex(&module_wait_lock);
    return VOID;}
}

static void clearloadlock(lispval spec)
{
  u8_lock_mutex(&module_wait_lock);
  if (fd_choice_containsp(spec,loading_modules)) {
    lispval prev_loading = loading_modules;
    loading_modules = fd_difference(loading_modules,spec);
    fd_decref(prev_loading);
    u8_condvar_broadcast(&module_wait);}
  u8_unlock_mutex(&module_wait_lock);
}

/* Getting modules */

FD_EXPORT
lispval fd_find_module(lispval spec,int safe,int err)
{
  u8_string modname = (SYMBOLP(spec)) ? (SYM_NAME(spec)) :
    (STRINGP(spec)) ? (CSTRING(spec)) : (NULL);

  lispval module = fd_get_module(spec,safe);
  if (VOIDP(module)) module = getloadlock(spec,safe);
  if (!(VOIDP(module))) {
    lispval loadstamp = fd_get(module,loadstamp_symbol,VOID);
    while (VOIDP(loadstamp)) {
      u8_lock_mutex(&module_wait_lock);
      u8_condvar_wait(&module_wait,&module_wait_lock);
      loadstamp = fd_get(module,loadstamp_symbol,VOID);}
    clearloadlock(spec);
    if (FD_ABORTP(loadstamp)) return loadstamp;
    else {
      fd_decref(loadstamp);
      return module;}}
  else {
    struct MODULE_LOADER *scan = module_loaders;
    if ((SYMBOLP(spec)) || (STRINGP(spec))) {}
    else {
      clearloadlock(spec);
      return fd_type_error(_("module name"),"fd_find_module",spec);}
    while (scan) {
      int retval = scan->loader(spec,safe,scan->data);
      if (retval>0) {
        clearloadlock(spec);
        module = fd_get_module(spec,safe);
        if (VOIDP(module)) {
          return fd_err(MissingModule,"fd_find_module",modname,spec);}
        fd_finish_module(module);
        return module;}
      else if (retval<0) {
        fd_discard_module(spec,safe);
        clearloadlock(spec);
        return FD_ERROR;}
      else scan = scan->next;}
    clearloadlock(spec);
    if (err)
      return fd_err(fd_NoSuchModule,"fd_find_module",modname,spec);
    else return FD_FALSE;}
}

FD_EXPORT
int fd_finish_module(lispval module)
{
  if (TABLEP(module))
    if (readonly_tablep(module))
      return 0;
    else {
      int flags = ((auto_fix_modules)?(FD_FIX_MODULES):(0)) |
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
int fd_module_finished(lispval module,int flags)
{
  if (!(TABLEP(module))) {
    fd_seterr(fd_NotAModule,"fd_finish_module",NULL,module);
    return -1;}
  else {
    struct U8_XTIME xtptr; lispval timestamp;
    lispval cur_timestamp = fd_get(module,loadstamp_symbol,VOID);
    lispval moduleid = fd_get(module,moduleid_symbol,VOID);
    u8_init_xtime(&xtptr,-1,u8_second,0,0,0);
    timestamp = fd_make_timestamp(&xtptr);
    fd_store(module,loadstamp_symbol,timestamp);
    fd_decref(timestamp);
    if (VOIDP(moduleid))
      u8_log(LOG_WARN,_("Anonymous module"),
             "The module %q doesn't have an id",module);
    /* In this case, the module was already finished,
       so the loadlock would have been cleared. */
    else if (cur_timestamp) {}
    else clearloadlock(moduleid);
    fd_decref(moduleid);
    fd_decref(cur_timestamp);
    if (FD_LEXENVP(module)) {
      fd_lexenv env = (fd_lexenv) module;
      lispval exports = env->env_exports;
      if (HASHTABLEP(env->env_bindings)) {
        if (U8_BITP(flags,FD_STATIC_MODULES))
          fd_static_module(env->env_bindings);
        else {}
        if (U8_BITP(flags,FD_LOCK_MODULES))
          fd_hashtable_set_readonly((fd_hashtable)(env->env_bindings),1);}
      if (HASHTABLEP(exports)) {
        if (U8_BITP(flags,FD_STATIC_EXPORTS)) fd_static_module(exports);
        else {}
        if (U8_BITP(flags,FD_LOCK_EXPORTS))
          fd_hashtable_set_readonly((fd_hashtable)exports,1);}}
    else if (HASHTABLEP(module)) {
      if (U8_BITP( flags, FD_STATIC_EXPORTS | FD_STATIC_MODULES))
        fd_static_module(module);
      else {}
      if (flags&(FD_LOCK_EXPORTS|FD_LOCK_MODULES)) fd_lock_exports(module);}
    else {}
    return 1;}
}

FD_EXPORT
int fd_static_module(lispval module)
{
  if (HASHTABLEP(module)) {
    int conversions = 0;
    struct FD_HASHTABLE *ht = (fd_hashtable)module;
    conversions = conversions+fd_static_hashtable(ht,fd_lambda_type);
    conversions = conversions+fd_static_hashtable(ht,fd_cprim_type);
    conversions = conversions+fd_static_hashtable(ht,fd_evalfn_type);
    return conversions;}
  else if (TABLEP(module)) return 0;
  else {
    fd_seterr(fd_NotAModule,"fd_static_module",NULL,module);
    return -1;}
}

FD_EXPORT
int fd_lock_exports(lispval module)
{
  if (HASHTABLEP(module))
    return fd_hashtable_set_readonly((struct FD_HASHTABLE *) module, 1);
  else if (FD_LEXENVP(module)) {
    fd_lexenv env = (fd_lexenv) module;
    lispval exports = env->env_exports;
    if (HASHTABLEP(exports))
      return fd_hashtable_set_readonly((struct FD_HASHTABLE *) exports, 1);
    else return 0;}
  else {
    fd_incref(module);
    return fd_reterr(_("not a module"),"fd_lock_exports",NULL,module);}
}

FD_EXPORT
void fd_add_module_loader(int (*loader)(lispval,int,void *),void *data)
{
  struct MODULE_LOADER *consed = u8_alloc(struct MODULE_LOADER);
  u8_lock_mutex(&module_loaders_lock);
  consed->loader = loader;
  consed->data = data;
  consed->next = module_loaders;
  module_loaders = consed;
  u8_unlock_mutex(&module_loaders_lock);
}

/* Loading dynamic libraries */

static lispval dloadpath = NIL;

static void init_dloadpath()
{
  u8_string tmp = u8_getenv("FD_INIT_DLOADPATH"); lispval strval;
  if (tmp == NULL)
    strval = lispval_string(FD_DEFAULT_DLOADPATH);
  else strval = fd_lispstring(tmp);
  dloadpath = fd_init_pair(NULL,strval,dloadpath);
  if ((tmp)||(trace_dload)||(getenv("FD_DLOAD:TRACE")))
    u8_log(LOG_INFO,"DynamicLoadPath","Initialized to %q",
           dloadpath);
}

static int load_dynamic_module(lispval spec,int safe,void *data)
{
  if (SYMBOLP(spec)) {
    u8_string pname = SYM_NAME(spec);
    u8_string name = u8_downcase(SYM_NAME(spec)), alt_name = NULL;
    /* The alt name has a suffix, which lets the path elements be either
       % patterns (which may provide a suffix) or just directory names
       (which may not) */
    if (strchr(pname,'.') == NULL)
      alt_name = u8_mkstring("%ls.%s",pname,FD_DLOAD_SUFFIX);
    FD_DOLIST(elt,dloadpath) {
      if (STRINGP(elt)) {
        u8_string module_filename = u8_find_file(name,CSTRING(elt),NULL);
        if ((!(module_filename))&&(alt_name))
          module_filename = u8_find_file(alt_name,CSTRING(elt),NULL);
        if (module_filename) {
          void *mod = u8_dynamic_load(module_filename);
          if (mod == NULL) {
            u8_log(LOG_WARN,_("FailedModule"),
                   "Failed to load module file %s for %q",
                   module_filename,spec);}
          else if (trace_dload) {
            u8_log(LOGF_NOTICE,_("DynamicLoad"),
                   "Loaded module %q from %s",spec,module_filename);}
          else {}
          if (errno) {
            u8_log(LOG_WARN,u8_UnexpectedErrno,
                   "Leftover errno %d (%s) from loading %s",
                   errno,u8_strerror(errno),pname);
            errno = 0;}
          u8_threadcheck();
          u8_free(module_filename);
          u8_free(name);
          if (alt_name) u8_free(alt_name);
          if (mod) return 1; else return -1;}}}
    if (alt_name) u8_free(alt_name);
    u8_free(name);
    u8_threadcheck();
    return 0;}
  else if  ((safe==0) &&
            (STRINGP(spec)) &&
            (u8_file_existsp(CSTRING(spec)))) {
    void *mod = u8_dynamic_load(CSTRING(spec));
    u8_threadcheck();
    if (mod) return 1; else return -1;}
  else return 0;
}

static lispval dynamic_load_prim(lispval arg)
{
  u8_string name = FD_STRING_DATA(arg);
  if (*name=='/') {
    void *mod = u8_dynamic_load(name);
    if (mod) return FD_TRUE;
    else return FD_ERROR;}
  else {
    FD_DOLIST(elt,dloadpath) {
      if (STRINGP(elt)) {
        u8_string module_name = u8_find_file(name,CSTRING(elt),NULL);
        if (module_name) {
          void *mod = u8_dynamic_load(module_name);
          u8_free(module_name);
          if (mod) return FD_TRUE;
          else return FD_ERROR;}}}
    return FD_FALSE;}
}

/* Switching modules */

static fd_lexenv become_module
   (fd_lexenv env,lispval module_name,int safe,int create)
{
  lispval module_spec, module;
  if (STRINGP(module_name))
    module_spec = fd_intern(CSTRING(module_name));
  else module_spec = fd_eval(module_name,env);
  if (SYMBOLP(module_spec))
    module = fd_get_module(module_spec,safe);
  else {
    module = module_spec;
    fd_incref(module);}
  if (FD_ABORTP(module)) return NULL;
  else if ((!(create))&&(VOIDP(module))) {
    fd_seterr(fd_NoSuchModule,"become_module",NULL,module_spec);
    fd_decref(module);
    return NULL;}
  else if (HASHTABLEP(module)) {
    fd_seterr(OpaqueModule,"become_module",NULL,module_spec);
    fd_decref(module);
    return NULL;}
  else if (FD_LEXENVP(module)) {
    FD_LEXENV *menv=
      fd_consptr(FD_LEXENV *,module,fd_lexenv_type);
    if (menv != env) {
      fd_decref(((lispval)(env->env_parent)));
      env->env_parent = (fd_lexenv)fd_incref((lispval)menv->env_parent);
      fd_decref(env->env_bindings);
      env->env_bindings = fd_incref(menv->env_bindings);
      fd_decref(env->env_exports);
      env->env_exports = fd_incref(menv->env_exports);}}
  else if (VOIDP(module)) {
    if (!(HASHTABLEP(env->env_exports)))
      env->env_exports = fd_make_hashtable(NULL,0);
    else if (FD_HASHTABLE_READONLYP(env->env_exports)) {
      fd_seterr(_("Can't reload a read-only module"),
                "become_module",NULL,module_spec);
      return NULL;}
    else {}
    fd_store(env->env_exports,moduleid_symbol,module_spec);
    fd_register_module(SYM_NAME(module_spec),(lispval)env,
                       ((safe) ? (FD_MODULE_SAFE) : (0)));}
  else {
    fd_seterr(fd_NotAModule,"use_module",NULL,module_spec);
    return NULL;}
  fd_decref(module);
  fd_decref(module_spec);
  return env;
}

static lispval safe_in_module_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval module_name = fd_get_arg(expr,1);
  if (VOIDP(module_name))
    return fd_err(fd_TooFewExpressions,"IN-MODULE",NULL,expr);
  else if (become_module(env,module_name,1,1)) return VOID;
  else return FD_ERROR;
}

static lispval in_module_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval module_name = fd_get_arg(expr,1);
  if (VOIDP(module_name))
    return fd_err(fd_TooFewExpressions,"IN-MODULE",NULL,expr);
  else if (become_module(env,module_name,0,1)) return VOID;
  else return FD_ERROR;
}

static lispval safe_within_module_evalfn
(lispval expr,fd_lexenv env,fd_stack _stack)
{
  fd_lexenv consed_env = fd_working_lexenv();
  lispval module_name = fd_get_arg(expr,1);
  if (VOIDP(module_name)) {
    fd_decref((lispval)consed_env);
    return fd_err(fd_TooFewExpressions,"WITHIN-MODULE",NULL,expr);}
  else if (become_module(consed_env,module_name,1,0)) {
    lispval result = VOID, body = fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result); result = fd_eval(elt,consed_env);}
    fd_decref((lispval)consed_env);
    return result;}
  else return FD_ERROR;
}

static lispval within_module_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  fd_lexenv consed_env = fd_working_lexenv();
  lispval module_name = fd_get_arg(expr,1);
  if (VOIDP(module_name)) {
    fd_decref((lispval)consed_env);
    return fd_err(fd_TooFewExpressions,"WITHIN-MODULE",NULL,expr);}
  else if (become_module(consed_env,module_name,0,0)) {
    lispval result = VOID, body = fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result); result = fd_eval(elt,consed_env);}
    fd_decref((lispval)consed_env);
    return result;}
  else return FD_ERROR;
}

static fd_lexenv make_hybrid_env(fd_lexenv base,lispval module_spec,int safe)
{
  lispval module=
    ((FD_LEXENVP(module_spec)) ?
     (fd_incref(module_spec)) :
     (fd_get_module(module_spec,safe)));
  if (FD_ABORTP(module)) {
    fd_interr(module);
    return NULL;}
  else if (HASHTABLEP(module)) {
    fd_seterr(OpaqueModule,"IN-MODULE",NULL,module_spec);
    return NULL;}
  else if (FD_LEXENVP(module)) {
    FD_LEXENV *menv=
      fd_consptr(FD_LEXENV *,module,fd_lexenv_type);
    fd_lexenv result = fd_make_env(fd_incref(base->env_bindings),menv);
    fd_decref(module);
    return result;}
  else if (VOIDP(module)) {
    fd_seterr(fd_NoSuchModule,"USING-MODULE",NULL,fd_incref(module_spec));
    return NULL;}
  else {
    fd_seterr(fd_TypeError,"USING-MODULE",NULL,fd_incref(module));
    return NULL;}
}

static lispval accessing_module_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval module_name = fd_eval(fd_get_arg(expr,1),env);
  fd_lexenv hybrid;
  if (VOIDP(module_name)) {
    return fd_err(fd_TooFewExpressions,"WITHIN-MODULE",NULL,expr);}
  hybrid = make_hybrid_env(env,module_name,0);
  if (hybrid) {
    lispval result = VOID, body = fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result); result = fd_eval(elt,hybrid);}
    fd_decref(module_name);
    fd_decref((lispval)hybrid);
    return result;}
  else {
    fd_decref(module_name);
    return FD_ERROR;}
}

static lispval safe_accessing_module_evalfn
(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval module_name = fd_eval(fd_get_arg(expr,1),env);
  fd_lexenv hybrid;
  if (VOIDP(module_name)) {
    return fd_err(fd_TooFewExpressions,"WITHIN-MODULE",NULL,expr);}
  hybrid = make_hybrid_env(env,module_name,1);
  if (hybrid) {
    lispval result = VOID, body = fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result); result = fd_eval(elt,hybrid);}
    fd_decref(module_name);
    fd_decref((lispval)hybrid);
    return result;}
  else {
    fd_decref(module_name);
    return FD_ERROR;}
}

/* Exporting from modules */

static u8_mutex exports_lock;

static fd_hashtable get_exports(fd_lexenv env)
{
  fd_hashtable exports;
  lispval moduleid = fd_get(env->env_bindings,moduleid_symbol,VOID);
  u8_lock_mutex(&exports_lock);
  if (HASHTABLEP(env->env_exports)) {
    u8_unlock_mutex(&exports_lock);
    fd_decref(moduleid);
    return (fd_hashtable) env->env_exports;}
  exports = (fd_hashtable)(env->env_exports = fd_make_hashtable(NULL,16));
  if (!(VOIDP(moduleid)))
    fd_hashtable_store(exports,moduleid_symbol,moduleid);
  u8_unlock_mutex(&exports_lock);
  return exports;
}

static lispval module_export_evalfn(lispval expr,fd_lexenv env,fd_stack stack)
{
  fd_hashtable exports;
  lispval symbols_spec = fd_get_arg(expr,1), symbols;
  if (VOIDP(symbols_spec))
    return fd_err(fd_TooFewExpressions,"MODULE-EXPORT!",NULL,expr);
  symbols = fd_eval(symbols_spec,env);
  if (FD_ABORTP(symbols)) return symbols;
  {DO_CHOICES(symbol,symbols)
      if (!(SYMBOLP(symbol))) {
        fd_decref(symbols);
        return fd_type_error(_("symbol"),"module_export",symbol);}}
  if (HASHTABLEP(env->env_exports))
    exports = (fd_hashtable)env->env_exports;
  else exports = get_exports(env);
  {DO_CHOICES(symbol,symbols) {
    lispval val = fd_get(env->env_bindings,symbol,VOID);
    fd_hashtable_store(exports,symbol,val);
    fd_decref(val);}}
  fd_decref(symbols);
  return VOID;
}

/* Using modules */

static int uses_bindings(fd_lexenv env,lispval bindings)
{
  fd_lexenv scan = env;
  while (scan)
    if (scan->env_bindings == bindings) return 1;
    else scan = scan->env_parent;
  return 0;
}

static lispval use_module_helper(lispval expr,fd_lexenv env,int safe)
{
  lispval module_names = fd_eval(fd_get_arg(expr,1),env);
  fd_lexenv modify_env = env;
  if (VOIDP(module_names))
    return fd_err(fd_TooFewExpressions,"USE-MODULE",NULL,expr);
  else {
    DO_CHOICES(module_name,module_names) {
      lispval module =
        (FD_HASHTABLEP(module_name)) ? (fd_incref(module_name)) :
        (FD_LEXENVP(module_name)) ? (fd_incref(module_name)) :
        (fd_find_module(module_name,safe,1));
      if (FD_ABORTP(module))
        return module;
      else if (VOIDP(module))
        return fd_err(fd_NoSuchModule,"USE-MODULE",NULL,module_name);
      else if (HASHTABLEP(module)) {
        if (!(uses_bindings(env,module))) {
          fd_lexenv old_parent;
          /* Use a dynamic copy if the enviroment is static, so it
             gets freed. */
          if (env->env_copy != env) modify_env = fd_copy_env(env);
          old_parent = modify_env->env_parent;
          modify_env->env_parent = fd_make_export_env(module,old_parent);
          /* We decref this because 'env' is no longer pointing to it
             and fd_make_export_env incref'd it again. */
          if (old_parent) fd_decref((lispval)(old_parent));}}
      else {
        fd_lexenv expenv=
          fd_consptr(fd_lexenv,module,fd_lexenv_type);
        lispval expval = (lispval)get_exports(expenv);
        if (!(uses_bindings(env,expval))) {
          if (env->env_copy != env) modify_env = fd_copy_env(env);
          fd_lexenv old_parent = modify_env->env_parent;
          modify_env->env_parent = fd_make_export_env(expval,old_parent);
          /* We decref this because 'env' is no longer pointing to it
             and fd_make_export_env incref'd it again. */
          if (old_parent) fd_decref((lispval)(old_parent));}}
      fd_decref(module);}
    fd_decref(module_names);
    if (modify_env!=env) {fd_decref((lispval)modify_env);}
    return VOID;}
}

static lispval use_module_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return use_module_helper(expr,env,0);
}

static lispval safe_use_module_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return use_module_helper(expr,env,1);
}

static lispval safe_get_module(lispval modname)
{
  lispval module = fd_find_module(modname,1,0);
  return module;
}

static lispval get_module(lispval modname)
{
  lispval module = fd_find_module(modname,0,0);
  return module;
}

static lispval get_loaded_module(lispval modname)
{
  lispval module = fd_get_module(modname,0);
  if (VOIDP(module))
    return FD_FALSE;
  else return module;
}

static lispval safe_get_loaded_module(lispval modname)
{
  lispval module = fd_get_module(modname,1);
  if (VOIDP(module))
    return FD_FALSE;
  else return module;
}

FD_EXPORT
lispval fd_use_module(fd_lexenv env,lispval module)
{
  int free_module = 0;
  if (SYMBOLP(module)) {
    module = get_module(module);
    free_module = 1;}
  if (HASHTABLEP(module)) {
    if (!(uses_bindings(env,module))) {
      fd_lexenv oldp = env->env_parent;
      env->env_parent = fd_make_export_env(module,oldp);
      fd_decref((lispval)(oldp));}}
  else if (SLOTMAPP(module)) {
    if (!(uses_bindings(env,module))) {
      fd_lexenv oldp = env->env_parent;
      env->env_parent = fd_make_env(module,oldp);
      fd_incref(module);
      fd_decref((lispval)(oldp));}}
  else if (FD_LEXENVP(module)) {
    fd_lexenv expenv=
      fd_consptr(fd_lexenv,module,fd_lexenv_type);
    lispval expval = (lispval)get_exports(expenv);
    if (!(uses_bindings(env,expval))) {
      fd_lexenv oldp = env->env_parent;
      env->env_parent = fd_make_export_env(expval,oldp);
      if (oldp) fd_decref((lispval)(oldp));}}
  else return fd_type_error("module","fd_use_module",module);
  if (free_module) fd_decref(module);
  return VOID;
}

static lispval static_module(lispval module)
{
  if (HASHTABLEP(module)) {
    int conversions = fd_static_module(module);
    if (conversions<0) return FD_ERROR;
    else return FD_INT(conversions);}
  else if (FD_LEXENVP(module)) {
    fd_lexenv env = (fd_lexenv) module;
    int conversions = 0, delta = 0;
    if (HASHTABLEP(env->env_bindings))
      delta = fd_static_module(env->env_bindings);
    if (conversions<0) return FD_ERROR;
    else conversions = conversions+delta;
    if ((env->env_exports) && (HASHTABLEP(env->env_exports)))
      delta = fd_static_module(env->env_exports);
    if (conversions<0) return FD_ERROR;
    else return FD_INT(conversions+delta);}
  else {
    lispval module_val = fd_find_module(module,0,0);
    if (FD_ABORTP(module_val)) return module_val;
    else {
      lispval result = static_module(module_val);
      fd_decref(module_val);
      return result;}}
}

static lispval get_exports_prim(lispval arg)
{
  lispval module = arg;
  if ((STRINGP(arg))||(SYMBOLP(arg)))
    module = fd_find_module(arg,0,0);
  else fd_incref(module);
  if (FD_ABORTP(module)) return module;
  else if (VOIDP(module))
    return fd_err(fd_NoSuchModule,"USE-MODULE",NULL,arg);
  else if (HASHTABLEP(module)) {
    lispval keys = fd_getkeys(module);
    fd_decref(module);
    return keys;}
  else if (TYPEP(module,fd_lexenv_type)) {
    fd_lexenv expenv=
      fd_consptr(fd_lexenv,module,fd_lexenv_type);
    lispval expval = (lispval)get_exports(expenv);
    if (FD_ABORTP(expval)) return expval;
    lispval keys = fd_getkeys(expval);
    fd_decref(module);
    return keys;}
  else return EMPTY;
}

static lispval safe_get_exports_prim(lispval arg)
{
  lispval module = arg;
  if ((STRINGP(arg))||(SYMBOLP(arg)))
    module = fd_find_module(arg,1,0);
  else fd_incref(module);
  if (FD_ABORTP(module)) return module;
  else if (VOIDP(module))
    return fd_err(fd_NoSuchModule,"USE-MODULE",NULL,arg);
  else if (HASHTABLEP(module)) {
    lispval keys = fd_getkeys(module);
    fd_decref(module);
    return keys;}
  else if (TYPEP(module,fd_lexenv_type)) {
    fd_lexenv expenv=
      fd_consptr(fd_lexenv,module,fd_lexenv_type);
    lispval expval = (lispval)get_exports(expenv);
    if (FD_ABORTP(expval)) return expval;
    lispval keys = fd_getkeys(expval);
    fd_decref(module);
    return keys;}
  else return EMPTY;
}

static lispval get_source(lispval arg,int safe)
{
  lispval ids = FD_EMPTY;
  if (FD_VOIDP(arg)) {
    u8_string path = fd_sourcebase();
    if (path) return lispval_string(path);
    else return FD_FALSE;}
  else if (FD_LEXENVP(arg)) {
    fd_lexenv envptr = fd_consptr(fd_lexenv,arg,fd_lexenv_type);
    ids = fd_get(envptr->env_bindings,source_symbol,FD_VOID);
    if (FD_VOIDP(ids))
      ids = fd_get(envptr->env_bindings,moduleid_symbol,FD_VOID);}
  else if (TABLEP(arg)) {
    ids = fd_get(arg,source_symbol,FD_VOID);
    if (FD_VOIDP(ids))
      ids = fd_get(arg,moduleid_symbol,FD_VOID);}
  else if (FD_SYMBOLP(arg)) {
    lispval mod = fd_find_module(arg,safe,1);
    if (FD_ABORTP(mod))
      return mod;
    lispval mod_source = get_source(mod,safe);
    fd_decref(mod);
    return mod_source;}
  /* These aren't strictly modules, but they're nice to have here */
  else if (FD_LAMBDAP(arg)) {
    struct FD_LAMBDA *f = (fd_lambda) fd_fcnid_ref(arg);
    if (f->fcn_filename)
      return fd_make_string(NULL,-1,f->fcn_filename);
    else {
      lispval sourceinfo = fd_symeval(source_symbol,f->lambda_env);
      if (FD_STRINGP(sourceinfo))
        return sourceinfo;
      else return FD_FALSE;}}
  else if (FD_FUNCTIONP(arg)) {
    struct FD_FUNCTION *f = (fd_function) fd_fcnid_ref(arg);
    if (f->fcn_filename)
      return fd_make_string(NULL,-1,f->fcn_filename);
    else return FD_FALSE;}
  else if (TYPEP(arg,fd_evalfn_type)) {
    struct FD_EVALFN *sf = (fd_evalfn) fd_fcnid_ref(arg);
    if (sf->evalfn_filename)
      return lispval_string(sf->evalfn_filename);
    else return FD_FALSE;}
  else if (FD_TYPEP(arg,fd_macro_type)) {
    struct FD_FUNCTION *f = (fd_function) fd_fcnid_ref(arg);
    if (f->fcn_filename)
      return fd_make_string(NULL,-1,f->fcn_filename);
    else return FD_FALSE;}
  else return fd_type_error(_("module"),"module_bindings",arg);
  if (FD_VOIDP(ids))
    return FD_FALSE;
  else {
    FD_DO_CHOICES(id,ids) {
      if (FD_STRINGP(id)) {
        fd_incref(id);
        fd_decref(ids);
        FD_STOP_DO_CHOICES;
        return id;}}
    return FD_FALSE;}
}

static lispval get_source_prim(lispval arg)
{
  return get_source(arg,0);
}

static lispval safe_get_source_prim(lispval arg)
{
  return get_source(arg,1);
}

/* LOADMODULE config */

static int loadmodule_sandbox = 0;

static int loadmodule_config_set(lispval var,lispval val,void *ignored)
{
  lispval module = fd_find_module(val,loadmodule_sandbox,0);
  if (FALSEP(module)) {
    u8_log(LOG_WARN,"NoModuleLoaded","Couldn't load the module %q",val);
    return -1;}
  else return 1;
}

static lispval loadmodule_config_get(lispval var,void *ignored)
{
  return FD_FALSE;
}

static int loadmodule_sandbox_config_set(lispval var,lispval val,void *ignored)
{
  if (FALSEP(val)) {
    if (!(loadmodule_sandbox)) return 0;
    else {
      fd_seterr("Can't reset LOADMODULE:SANDBOX",
                "loadmodule_sandbox_config_set",NULL,
                val);
      return -1;}}
  else {
    if (loadmodule_sandbox) return 0;
    else {
      loadmodule_sandbox = 1;
      return 1;}}
}

static lispval loadmodule_sandbox_config_get(lispval var,void *ignored)
{
  if (loadmodule_sandbox) return FD_TRUE; else return FD_FALSE;
}

/* Modules to be used by fd_app_env */

static lispval config_used_modules(lispval var,void *data)
{
  lispval modlist=NIL;
  fd_lexenv scan=fd_app_env;
  while (scan) {
    if (scan->env_copy) scan=scan->env_copy;
    if (FD_HASHTABLEP(scan->env_bindings)) {
      lispval bindings=fd_incref(scan->env_bindings);
      modlist=fd_init_pair(NULL,bindings,modlist);}
    scan=scan->env_parent;}
  return modlist;
}
static int config_use_module(lispval var,lispval val,void *data)
{
  lispval rv=fd_use_module(fd_app_env,val);
  if (FD_ABORTP(rv))
    return -1;
  else {
    fd_decref(rv);
    return 1;}
}

/* Accessing module bindings */

static lispval
get_binding_helper(lispval modarg,lispval symbol,lispval dflt,
                   int safe,int export_only,int symeval,
                   u8_context caller)
{
  lispval module = (FD_HASHTABLEP(modarg)) ? (modarg) :
    (FD_LEXENVP(modarg)) ? (modarg) :
    (FD_SYMBOLP(modarg)) ? (fd_find_module(modarg,safe,0)) :
    (FD_STRINGP(modarg)) ? (fd_find_module(modarg,safe,0)) :
    (VOID);
  if (VOIDP(module)) {
    if (VOIDP(dflt))
      return fd_err(fd_NoSuchModule,caller,NULL,modarg);
    else return fd_incref(dflt);}
  else if (FD_HASHTABLEP(module)) {
    lispval value = fd_hashtable_get((fd_hashtable)module,symbol,VOID);
    if (VOIDP(value)) {
      lispval retval=
        fd_err(fd_UnboundIdentifier,caller,SYMBOL_NAME(symbol),module);
      if (module == modarg) fd_decref(module);
      return retval;}
    else return fd_incref(dflt);}
  else if (FD_LEXENVP(module)) {
    fd_lexenv env = (fd_lexenv) module;
    if (symeval) {
      lispval val = fd_symeval(symbol,env);
      if ( (FD_VOIDP(val)) || (val == FD_UNBOUND) ) {
        if (FD_VOIDP(dflt))
          return fd_err(fd_UnboundIdentifier,caller,SYMBOL_NAME(symbol),module);
        else return fd_incref(dflt);}
      else return val;}
    else if (TABLEP(env->env_exports)) {
      lispval expval = fd_get(env->env_exports,symbol,VOID);
      if (!(VOIDP(expval))) {
        if (module != modarg) fd_decref(module);
        return expval;}}
    if (!export_only) {
      lispval inval= fd_get(env->env_bindings,symbol,VOID);
      if (!(VOIDP(inval))) {
        if (module != modarg) fd_decref(module);
        return inval;}}
    if (VOIDP(dflt)) {
      lispval retval=
        fd_err(fd_UnboundIdentifier,caller,SYMBOL_NAME(symbol),module);
      if (module == modarg) fd_decref(module);
      return retval;}
    else return fd_incref(dflt);}
  else if (VOIDP(dflt)) {
    lispval retval=
      fd_err(fd_NotAModule,caller,SYMBOL_NAME(symbol),module);
    if (module == modarg) fd_decref(module);
    return retval;}
  else {
    u8_log(LOG_WARN,"BadModule","From %q for %q: %q",
           modarg,symbol,module);
    if (module == modarg) fd_decref(module);
    return fd_incref(dflt);}
}

static lispval get_binding_prim
(lispval mod_arg,lispval symbol,lispval dflt)
{
  return get_binding_helper(mod_arg,symbol,dflt,0,1,0,
                            "get_binding_prim");
}

static lispval safe_get_binding_prim
(lispval mod_arg,lispval symbol,lispval dflt)
{
  return get_binding_helper(mod_arg,symbol,dflt,1,1,0,
                            "safe_get_binding_prim");
}

static lispval get_internal_binding_prim
(lispval mod_arg,lispval symbol,lispval dflt)
{
  return get_binding_helper(mod_arg,symbol,dflt,0,0,0,
                            "get_internal_binding_prim");
}

static lispval import_var_prim(lispval mod_arg,lispval symbol,lispval dflt)
{
  return get_binding_helper(mod_arg,symbol,dflt,0,0,1,"import_var_prim");
}

static volatile int module_tables_initialized = 0;

void fd_init_module_tables()
{
  if (module_tables_initialized) return;
  else module_tables_initialized=1;

  moduleid_symbol = fd_intern("%MODULEID");
  safemod_symbol = fd_intern("%SAFEMOD");
  loadstamp_symbol = fd_intern("%LOADSTAMP");
  source_symbol = fd_intern("%SOURCE");

  FD_INIT_STATIC_CONS(&module_map,fd_hashtable_type);
  fd_make_hashtable(&module_map,67);

  FD_INIT_STATIC_CONS(&safe_module_map,fd_hashtable_type);
  fd_make_hashtable(&safe_module_map,67);

  if (FD_VOIDP(fd_scheme_module)) {
    fd_xscheme_module = fd_make_hashtable(NULL,71);
    fd_scheme_module = fd_make_hashtable(NULL,71);}

  default_env = fd_make_env(fd_make_hashtable(NULL,0),fd_app_env);
  safe_default_env = fd_make_env(fd_make_hashtable(NULL,0),NULL);

  fd_register_module("SCHEME",fd_scheme_module,
                     (FD_MODULE_DEFAULT|FD_MODULE_SAFE));
  fd_register_module("XSCHEME",fd_xscheme_module,(FD_MODULE_DEFAULT));
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

  fd_idefn(fd_xscheme_module,
           fd_make_cprim1x("DYNAMIC-LOAD",dynamic_load_prim,1,
                           fd_string_type,VOID));
  fd_defalias(fd_xscheme_module,"DLOAD","DYNAMIC-LOAD");
  fd_defalias(fd_xscheme_module,"LOAD-DLL","DYNAMIC-LOAD");

  fd_def_evalfn(fd_scheme_module,"IN-MODULE","",safe_in_module_evalfn);
  fd_def_evalfn(fd_scheme_module,"WITHIN-MODULE","",safe_within_module_evalfn);
  fd_defalias(fd_scheme_module,"W/M","WITHIN-MODULE");
  fd_defalias(fd_scheme_module,"%WM","WITHIN-MODULE");
  fd_def_evalfn(fd_scheme_module,"ACCESSING-MODULE","",safe_accessing_module_evalfn);
  fd_def_evalfn(fd_scheme_module,"USE-MODULE","",safe_use_module_evalfn);
  fd_def_evalfn(fd_scheme_module,"MODULE-EXPORT!","",module_export_evalfn);
  fd_idefn(fd_scheme_module,fd_make_cprim1("GET-MODULE",safe_get_module,1));
  fd_idefn1(fd_scheme_module,"GET-LOADED-MODULE",safe_get_loaded_module,1,
            "Gets a loaded module, fails for non-loaded or non-existent modules",
            -1,VOID);
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("GET-EXPORTS",safe_get_exports_prim,1));
  fd_defalias(fd_scheme_module,"%LS","GET-EXPORTS");

  fd_idefn1(fd_scheme_module,"GET-SOURCE",safe_get_source_prim,0,
            "(get-source [*obj*])\nGets the source file implementing *obj*, "
            "which can be a function (or macro or evalfn), module, or module "
            "name. With no arguments, returns the current SOURCEBASE",
            -1,VOID);
  fd_idefn1(fd_xscheme_module,"GET-SOURCE",get_source_prim,0,
            "(get-source [*obj*])\nGets the source file implementing *obj*, "
            "which can be a function (or macro or evalfn), module, or module "
            "name. With no arguments, returns the current SOURCEBASE",
            -1,VOID);

  fd_idefn3(fd_scheme_module,"GET-BINDING",safe_get_binding_prim,2,
            "(get-binding *module* *symbol* [*default*])\n"
            "Gets *module*'s exported binding of *symbol* "
            "without using the whole module. On failure, "
            "returns *default* if provided or errs otherwise.",
            -1,VOID,fd_symbol_type,VOID,-1,VOID);
  fd_idefn3(fd_xscheme_module,"GET-BINDING",get_binding_prim,2,
            "(get-binding *module* *symbol* [*default*])\n"
            "Gets *module*'s exported binding of *symbol*. "
            "On failure, returns *default* if provided "
            "or errs if none is provided.",
            -1,VOID,fd_symbol_type,VOID,-1,VOID);

  fd_idefn3(fd_xscheme_module,"%GET-BINDING",get_internal_binding_prim,2,
            "(get-binding *module* *symbol* [*default*])\n"
            "Gets *module*'s binding of *symbol* "
            "(internal or external). On failure returns "
            "*default* (if provided) or errs if none is provided.",
            -1,VOID,fd_symbol_type,VOID,-1,VOID);
  fd_idefn3(fd_xscheme_module,"IMPORTVAR",import_var_prim,2,
            "(importvar *module* *symbol* [*default*])\n"
            "Gets *module*'s binding of *symbol* "
            "(internal, external, or inherhited). On failure returns "
            "*default*, if provided, or errors (if not).",
            -1,VOID,fd_symbol_type,VOID,-1,FD_VOID);

  fd_def_evalfn(fd_xscheme_module,"IN-MODULE","",in_module_evalfn);
  fd_def_evalfn(fd_xscheme_module,"WITHIN-MODULE","",within_module_evalfn);
  fd_defalias(fd_xscheme_module,"W/M","WITHIN-MODULE");
  fd_defalias(fd_xscheme_module,"%WM","WITHIN-MODULE");
  fd_def_evalfn(fd_xscheme_module,"ACCESSING-MODULE","",accessing_module_evalfn);
  fd_def_evalfn(fd_xscheme_module,"USE-MODULE","",use_module_evalfn);
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
  fd_register_config("MODULE",
                     "Specify modules to be used by the default live environment",
                     config_used_modules,config_use_module,NULL);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
