/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_FCNIDS 1
#define FD_INLINE_STACKS 1
#define FD_INLINE_LEXENV 1

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/support.h"
#include "framerd/storage.h"
#include "framerd/eval.h"
#include "framerd/dtproc.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/ports.h"
#include "framerd/dtcall.h"
#include "framerd/ffi.h"

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

fd_lexenv fd_app_env=NULL;
static lispval init_list = NIL;
static lispval module_list = NIL;
static lispval loadfile_list = NIL;

/* The "application" environment */

static int app_cleanup_started=0;
static u8_mutex app_cleanup_lock;

static lispval cleanup_app_env()
{
  if (fd_app_env==NULL) return FD_VOID;
  if (app_cleanup_started) return FD_VOID;
  if ( (fd_exiting) && (fd_fast_exit) ) return FD_VOID;
  u8_lock_mutex(&app_cleanup_lock);
  if (app_cleanup_started) {
    u8_unlock_mutex(&app_cleanup_lock);
    return FD_VOID;}
  else app_cleanup_started=1;
  fd_lexenv env=fd_app_env; fd_app_env=NULL;
  /* Hollow out the environment, which should let it be reclaimed.
     This patches around some of the circular references that might
     exist because working_lexenv may contain procedures which
     are closed in the working environment, so the working environment
     itself won't be GC'd because of those circular pointers. */
  unsigned int refcount = FD_CONS_REFCOUNT(env);
  fd_decref((lispval)env);
  if (refcount>1) {
    if (HASHTABLEP(env->env_bindings))
      fd_reset_hashtable((fd_hashtable)(env->env_bindings),0,1);}
  u8_unlock_mutex(&app_cleanup_lock);
  return FD_VOID;
}

FD_EXPORT void fd_setup_app_env()
{
  lispval exit_handler = fd_make_cprim0("APPENV/ATEXIT",cleanup_app_env);
  fd_config_set("ATEXIT",exit_handler);
  fd_decref(exit_handler);
}

static int run_init(lispval init,fd_lexenv env)
{
  lispval v = FD_VOID;
  if (FD_APPLICABLEP(init)) {
    if (FD_FUNCTIONP(init)) {
      struct FD_FUNCTION *f = (fd_function) init;
      v = (f->fcn_arity == 0) ?
        (fd_apply(init,0,NULL)) :
        (fd_apply(init,1,(lispval *)(&env)));}
    else v = fd_apply(init,0,NULL);}
  else if (FD_PAIRP(init))
    v = fd_eval(init,env);
  else NO_ELSE;
  if (FD_ABORTP(v)) {
    u8_exception ex = u8_erreify();
    u8_log(LOGCRIT,"InitFailed",
           "Failed to apply init %q: %m <%s> %s%s%s",
           init,ex->u8x_cond,ex->u8x_context,
           U8OPTSTR(" (",ex->u8x_details,")"));
    u8_free_exception(ex,1);
    return -1;}
  else {
    fd_decref(v);
    return 1;}
}

FD_EXPORT void fd_set_app_env(fd_lexenv env)
{
  if (env==fd_app_env)
    return;
  else if (fd_app_env) {
    fd_lexenv old_env=fd_app_env;
    fd_app_env=NULL;
    fd_decref((lispval)old_env);}
  fd_app_env=env;
  if (env) {
    int modules_loaded = 0, files_loaded = 0;
    int modules_failed = 0, files_failed = 0;
    int inits_run = 0, inits_failed = 0;
    lispval modules = fd_reverse(module_list);
    {FD_DOLIST(modname,modules) {
        lispval module = fd_find_module(modname,0,0);
        if (FD_ABORTP(module)) {
          u8_log(LOG_WARN,"LoadModuleError","Error loading module %q",modname);
          fd_clear_errors(1);
          modules_failed++;}
        else {
          lispval used = fd_use_module(fd_app_env,module);
          if (FD_ABORTP(used)) {
            u8_log(LOG_WARN,"UseModuleError","Error using module %q",module);
            fd_clear_errors(1);
            modules_failed++;}
          else modules_loaded++;
          fd_decref(module);
          fd_decref(used);}}}
    fd_decref(modules); modules=FD_VOID;
    lispval files = fd_reverse(loadfile_list);
    {FD_DOLIST(file,files) {
        lispval loadval = fd_load_source(FD_CSTRING(file),fd_app_env,NULL);
        if (FD_ABORTP(loadval)) {
          u8_log(LOG_WARN,"LoadError","fd_set_app_end",
                 "Error loading %s into the application environment",
                 FD_CSTRING(file));
          fd_clear_errors(1);
          files_failed++;}
        else files_loaded++;
        fd_decref(loadval);}}
    fd_decref(files); files=FD_VOID;
    lispval inits = init_list;
    {FD_DOLIST(init,inits) {
        int rv = run_init(init,env);
        if (rv > 0) inits_run++;
        else if (rv<0) inits_failed++;
        else NO_ELSE;}}
    fd_decref(inits); inits=FD_VOID;
    u8_log(LOG_INFO,"AppEnvLoad",
           "%d:%d:%d modules:files:inits loaded/run, "
           "%d:%d:%d modules:files:init failed",
           modules_loaded,files_loaded,inits_run,
           modules_failed,files_failed,inits_failed);}
}

static lispval appenv_prim()
{
  if (fd_app_env)
    return (lispval)fd_copy_env(fd_app_env);
  else return FD_FALSE;
}

/* Module and loading config */

static u8_string get_next(u8_string pt,u8_string seps);

static lispval parse_module_spec(u8_string s)
{
  if (*s) {
    u8_string brk = get_next(s," ,;");
    if (brk) {
      u8_string elt = u8_slice(s,brk);
      lispval parsed = fd_parse(elt);
      if (FD_ABORTP(parsed)) {
        u8_free(elt);
        return parsed;}
      else return fd_init_pair(NULL,parsed,
                               parse_module_spec(brk+1));}
    else {
      lispval parsed = fd_parse(s);
      if (FD_ABORTP(parsed)) return parsed;
      else return fd_init_pair(NULL,parsed,NIL);}}
  else return NIL;
}

static u8_string get_next(u8_string pt,u8_string seps)
{
  u8_string closest = NULL;
  while (*seps) {
    u8_string brk = strchr(pt,*seps);
    if ((brk) && ((brk<closest) || (closest == NULL)))
      closest = brk;
    seps++;}
  return closest;
}

static int add_modname(lispval modname)
{
  if (fd_app_env) {
    lispval module = fd_find_module(modname,0,0);
    if (FD_ABORTP(module))
      return -1;
    else if (FD_VOIDP(module)) {
      u8_log(LOG_WARN,fd_NoSuchModule,"module_config_set",
             "No module found for %q",modname);
      return -1;}
    lispval used = fd_use_module(fd_app_env,module);
    if (FD_ABORTP(used)) {
      u8_log(LOG_WARN,"LoadModuleError",
             "Error using module %q",module);
      fd_clear_errors(1);
      fd_decref(module);
      fd_decref(used);
      return -1;}
    module_list = fd_conspair(modname,module_list);
    fd_incref(modname);
    fd_decref(module);
    fd_decref(used);
    return 1;}
  else {
    module_list = fd_conspair(modname,module_list);
    fd_incref(modname);
    return 0;}
}

static int module_config_set(lispval var,lispval vals,void *d)
{
  int loads = 0; DO_CHOICES(val,vals) {
    lispval modname = ((SYMBOLP(val))?(val):
                       (STRINGP(val))?
                       (parse_module_spec(CSTRING(val))):
                       (VOID));
    if (VOIDP(modname)) {
      fd_seterr(fd_TypeError,"module_config_set","module",val);
      return -1;}
    else if (PAIRP(modname)) {
      FD_DOLIST(elt,modname) {
        if (!(SYMBOLP(elt))) {
          u8_log(LOG_WARN,fd_TypeError,"module_config_set",
                 "Not a valid module name: %q",elt);}
        else {
          int added = add_modname(elt);
          if (added>0) loads++;}}
      fd_decref(modname);}
    else if (!(SYMBOLP(modname))) {
      fd_seterr(fd_TypeError,"module_config_set","module name",val);
      fd_decref(modname);
      return -1;}
    else {
      int added = add_modname(modname);
      if (added > 0) loads++;}}
  return loads;
}

static lispval module_config_get(lispval var,void *d)
{
  return fd_incref(module_list);
}

static int loadfile_config_set(lispval var,lispval vals,void *d)
{
  int loads = 0; DO_CHOICES(val,vals) {
    if (!(STRINGP(val))) {
      fd_seterr(fd_TypeError,"loadfile_config_set","filename",val);
      return -1;}}
  if (fd_app_env == NULL) {
    FD_DO_CHOICES(val,vals) {
      u8_string loadpath = (!(strchr(CSTRING(val),':'))) ?
        (u8_abspath(CSTRING(val),NULL)) :
        (u8_strdup(CSTRING(val)));
      loadfile_list = fd_conspair(fd_lispstring(loadpath),loadfile_list);}}
  else {
    FD_DO_CHOICES(val,vals) {
      u8_string loadpath = (!(strchr(CSTRING(val),':'))) ?
        (u8_abspath(CSTRING(val),NULL)) :
        (u8_strdup(CSTRING(val)));
      lispval loadval = fd_load_source(loadpath,fd_app_env,NULL);
      if (FD_ABORTP(loadval)) {
        fd_seterr(_("load error"),"loadfile_config_set",loadpath,val);
        return -1;}
      else {
        loadfile_list = fd_conspair(fdstring(loadpath),loadfile_list);
        u8_free(loadpath);
        loads++;}}}
  return loads;
}

static lispval loadfile_config_get(lispval var,void *d)
{
  return fd_incref(loadfile_list);
}

static lispval inits_config_get(lispval var,void *d)
{
  return fd_incref(init_list);
}

static int inits_config_set(lispval var,lispval inits,void *d)
{
  int run_count = 0;
  if (fd_app_env == NULL) {
    FD_DO_CHOICES(init,inits) {
      fd_incref(init);
      init_list = fd_conspair(init,init_list);}}
  else {
    FD_DO_CHOICES(init,inits) {
      int rv = run_init(init,fd_app_env);
      if (rv > 0) {
        fd_incref(init);
        init_list = fd_conspair(init,init_list);
        run_count++;}}}
  return run_count;
}

FD_EXPORT
void fd_autoload_config(u8_string module_inits,
                        u8_string file_inits,
                        u8_string run_inits)
{
  fd_register_config
    (module_inits,_("Which modules to load into the application environment"),
     module_config_get,module_config_set,&module_list);
  fd_register_config
    (file_inits,_("Which files to load into the application environment"),
     loadfile_config_get,loadfile_config_set,&loadfile_list);
  fd_register_config
    (run_inits,_("Which functions/forms to execute to set up "
                 "the application environment"),
     inits_config_get,inits_config_set,&init_list);
}

/* Initialization */

FD_EXPORT void fd_init_eval_appenv_c()
{
  u8_register_source_file(_FILEINFO);

  fd_idefn0(fd_scheme_module,"%APPENV",appenv_prim,
            "Returns the base 'application environment' for the "
            "current instance");
  u8_init_mutex(&app_cleanup_lock);
}

