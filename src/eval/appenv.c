/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES 1
#define KNO_INLINE_TABLES 1
#define KNO_INLINE_FCNIDS 1
#define KNO_INLINE_STACKS 1
#define KNO_INLINE_LEXENV 1

#define KNO_PROVIDE_FASTEVAL 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/support.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/dtproc.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/ports.h"
#include "kno/dtcall.h"
#include "kno/ffi.h"

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

kno_lexenv kno_app_env=NULL;
static lispval init_list = NIL;
static lispval module_list = NIL;
static lispval loadfile_list = NIL;

/* The "application" environment */

static int app_cleanup_started=0;
static u8_mutex app_cleanup_lock;

static lispval cleanup_app_env()
{
  if (kno_app_env==NULL) return KNO_VOID;
  if (app_cleanup_started) return KNO_VOID;
  if ( (kno_exiting) && (kno_fast_exit) ) return KNO_VOID;
  u8_lock_mutex(&app_cleanup_lock);
  if (app_cleanup_started) {
    u8_unlock_mutex(&app_cleanup_lock);
    return KNO_VOID;}
  else app_cleanup_started=1;
  kno_lexenv env=kno_app_env; kno_app_env=NULL;
  /* Hollow out the environment, which should let it be reclaimed.
     This patches around some of the circular references that might
     exist because working_lexenv may contain procedures which
     are closed in the working environment, so the working environment
     itself won't be GC'd because of those circular pointers. */
  unsigned int refcount = KNO_CONS_REFCOUNT(env);
  kno_decref((lispval)env);
  if (refcount>1) {
    if (HASHTABLEP(env->env_bindings))
      kno_reset_hashtable((kno_hashtable)(env->env_bindings),0,1);}
  u8_unlock_mutex(&app_cleanup_lock);
  return KNO_VOID;
}

KNO_EXPORT void kno_setup_app_env()
{
  lispval exit_handler = kno_make_cprim0("APPENV/ATEXIT",cleanup_app_env);
  kno_config_set("ATEXIT",exit_handler);
  kno_decref(exit_handler);
}

static int run_init(lispval init,kno_lexenv env)
{
  lispval v = KNO_VOID;
  if (KNO_APPLICABLEP(init)) {
    if (KNO_FUNCTIONP(init)) {
      struct KNO_FUNCTION *f = (kno_function) init;
      v = (f->fcn_arity == 0) ?
	(kno_apply(init,0,NULL)) :
	(kno_apply(init,1,(lispval *)(&env)));}
    else v = kno_apply(init,0,NULL);}
  else if (KNO_PAIRP(init))
    v = kno_eval(init,env);
  else NO_ELSE;
  if (KNO_ABORTP(v)) {
    u8_exception ex = u8_erreify();
    u8_log(LOGCRIT,"InitFailed",
	   "Failed to apply init %q: %m <%s> %s%s%s",
	   init,ex->u8x_cond,ex->u8x_context,
	   U8OPTSTR(" (",ex->u8x_details,")"));
    u8_free_exception(ex,1);
    return -1;}
  else {
    kno_decref(v);
    return 1;}
}

KNO_EXPORT void kno_set_app_env(kno_lexenv env)
{
  if (env==kno_app_env)
    return;
  else if (kno_app_env) {
    kno_lexenv old_env=kno_app_env;
    kno_app_env=NULL;
    kno_decref((lispval)old_env);}
  kno_app_env=env;
  if (env) {
    int modules_loaded = 0, files_loaded = 0;
    int modules_failed = 0, files_failed = 0;
    int inits_run = 0, inits_failed = 0;
    lispval modules = kno_reverse(module_list);
    {KNO_DOLIST(modname,modules) {
	lispval module = kno_find_module(modname,0);
	if (KNO_ABORTP(module)) {
	  u8_log(LOG_WARN,"LoadModuleError","Error loading module %q",modname);
	  kno_clear_errors(1);
	  modules_failed++;}
	else {
	  lispval used = kno_use_module(kno_app_env,module);
	  if (KNO_ABORTP(used)) {
	    u8_log(LOG_WARN,"UseModuleError","Error using module %q",module);
	    kno_clear_errors(1);
	    modules_failed++;}
	  else modules_loaded++;
	  kno_decref(module);
	  kno_decref(used);}}}
    kno_decref(modules); modules=KNO_VOID;
    lispval files = kno_reverse(loadfile_list);
    {KNO_DOLIST(file,files) {
	lispval loadval = kno_load_source(KNO_CSTRING(file),kno_app_env,NULL);
	if (KNO_ABORTP(loadval)) {
	  u8_log(LOG_WARN,"LoadError","kno_set_app_end",
		 "Error loading %s into the application environment",
		 KNO_CSTRING(file));
	  kno_clear_errors(1);
	  files_failed++;}
	else files_loaded++;
	kno_decref(loadval);}}
    kno_decref(files); files=KNO_VOID;
    lispval inits = init_list;
    {KNO_DOLIST(init,inits) {
	int rv = run_init(init,env);
	if (rv > 0) inits_run++;
	else if (rv<0) inits_failed++;
	else NO_ELSE;}}
    kno_decref(inits); inits=KNO_VOID;
    u8_log(LOG_INFO,"AppEnvLoad",
	   "%d:%d:%d modules:files:inits loaded/run, "
	   "%d:%d:%d modules:files:init failed",
	   modules_loaded,files_loaded,inits_run,
	   modules_failed,files_failed,inits_failed);}
}

static lispval appenv_prim()
{
  if (kno_app_env)
    return (lispval)kno_copy_env(kno_app_env);
  else return KNO_FALSE;
}

/* Module and loading config */

static u8_string get_next(u8_string pt,u8_string seps);

static lispval parse_module_spec(u8_string s)
{
  if (*s) {
    u8_string brk = get_next(s," ,;");
    if (brk) {
      u8_string elt = u8_slice(s,brk);
      lispval parsed = kno_parse(elt);
      if (KNO_ABORTP(parsed)) {
	u8_free(elt);
	return parsed;}
      else return kno_init_pair(NULL,parsed,
			       parse_module_spec(brk+1));}
    else {
      lispval parsed = kno_parse(s);
      if (KNO_ABORTP(parsed)) return parsed;
      else return kno_init_pair(NULL,parsed,NIL);}}
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
  if (kno_app_env) {
    lispval module = kno_find_module(modname,0);
    if (KNO_ABORTP(module))
      return -1;
    else if (KNO_VOIDP(module)) {
      u8_log(LOG_WARN,kno_NoSuchModule,"module_config_set",
	     "No module found for %q",modname);
      return -1;}
    lispval used = kno_use_module(kno_app_env,module);
    if (KNO_ABORTP(used)) {
      u8_log(LOG_WARN,"LoadModuleError",
	     "Error using module %q",module);
      kno_clear_errors(1);
      kno_decref(module);
      kno_decref(used);
      return -1;}
    module_list = kno_conspair(modname,module_list);
    kno_incref(modname);
    kno_decref(module);
    kno_decref(used);
    return 1;}
  else {
    module_list = kno_conspair(modname,module_list);
    kno_incref(modname);
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
      kno_seterr(kno_TypeError,"module_config_set","module",val);
      return -1;}
    else if (PAIRP(modname)) {
      KNO_DOLIST(elt,modname) {
	if (!(SYMBOLP(elt))) {
	  u8_log(LOG_WARN,kno_TypeError,"module_config_set",
		 "Not a valid module name: %q",elt);}
	else {
	  int added = add_modname(elt);
	  if (added>0) loads++;}}
      kno_decref(modname);}
    else if (!(SYMBOLP(modname))) {
      kno_seterr(kno_TypeError,"module_config_set","module name",val);
      kno_decref(modname);
      return -1;}
    else {
      int added = add_modname(modname);
      if (added > 0) loads++;}}
  return loads;
}

static lispval module_config_get(lispval var,void *d)
{
  return kno_incref(module_list);
}

static int loadfile_config_set(lispval var,lispval vals,void *d)
{
  int loads = 0; DO_CHOICES(val,vals) {
    if (!(STRINGP(val))) {
      kno_seterr(kno_TypeError,"loadfile_config_set","filename",val);
      return -1;}}
  if (kno_app_env == NULL) {
    KNO_DO_CHOICES(val,vals) {
      u8_string loadpath = (!(strchr(CSTRING(val),':'))) ?
	(u8_abspath(CSTRING(val),NULL)) :
	(u8_strdup(CSTRING(val)));
      loadfile_list = kno_conspair(kno_wrapstring(loadpath),loadfile_list);}}
  else {
    KNO_DO_CHOICES(val,vals) {
      u8_string loadpath = (!(strchr(CSTRING(val),':'))) ?
	(u8_abspath(CSTRING(val),NULL)) :
	(u8_strdup(CSTRING(val)));
      lispval loadval = kno_load_source(loadpath,kno_app_env,NULL);
      if (KNO_ABORTP(loadval)) {
	kno_seterr(_("load error"),"loadfile_config_set",loadpath,val);
	return -1;}
      else {
	loadfile_list = kno_conspair(knostring(loadpath),loadfile_list);
	u8_free(loadpath);
	loads++;}}}
  return loads;
}

static lispval loadfile_config_get(lispval var,void *d)
{
  return kno_incref(loadfile_list);
}

static lispval inits_config_get(lispval var,void *d)
{
  return kno_incref(init_list);
}

static int inits_config_set(lispval var,lispval inits,void *d)
{
  int run_count = 0;
  if (kno_app_env == NULL) {
    KNO_DO_CHOICES(init,inits) {
      kno_incref(init);
      init_list = kno_conspair(init,init_list);}}
  else {
    KNO_DO_CHOICES(init,inits) {
      int rv = run_init(init,kno_app_env);
      if (rv > 0) {
	kno_incref(init);
	init_list = kno_conspair(init,init_list);
	run_count++;}}}
  return run_count;
}

KNO_EXPORT
void kno_autoload_config(u8_string module_inits,
			u8_string file_inits,
			u8_string run_inits)
{
  kno_register_config
    (module_inits,_("Which modules to load into the application environment"),
     module_config_get,module_config_set,&module_list);
  kno_register_config
    (file_inits,_("Which files to load into the application environment"),
     loadfile_config_get,loadfile_config_set,&loadfile_list);
  kno_register_config
    (run_inits,_("Which functions/forms to execute to set up "
		 "the application environment"),
     inits_config_get,inits_config_set,&init_list);
}

/* Initialization */

KNO_EXPORT void kno_init_eval_appenv_c()
{
  u8_register_source_file(_FILEINFO);

  /* These are the default appenv configs. Particularly applications
     or executables may define their own. */
  kno_autoload_config("APPMODS","APPLOAD","APPEVAL");

  kno_idefn0(kno_scheme_module,"%APPENV",appenv_prim,
	    "Returns the base 'application environment' for the "
	    "current instance");
  u8_init_mutex(&app_cleanup_lock);
}

