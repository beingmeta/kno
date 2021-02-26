/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES   (!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES    (!(KNO_AVOID_INLINE))
#define KNO_INLINE_FCNIDS    (!(KNO_AVOID_INLINE))
#define KNO_INLINE_STACKS    (!(KNO_AVOID_INLINE))
#define KNO_INLINE_LEXENV    (!(KNO_AVOID_INLINE))

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
#include "kno/cprims.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>
#include <libu8/u8contour.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>


kno_lexenv kno_app_env=NULL;
static lispval init_list = NIL;
static lispval module_list = NIL;
static lispval loadfile_list = NIL;
static lispval inits_done = NIL;
static lispval inits_failed = NIL;

static u8_mutex init_lock;

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
  struct KNO_CPRIM *exit_handler =
    kno_init_cprim("APPENV/ATEXIT","cleanup_app_env",0,_FILEINFO,
		   "Cleans up the application environment",
		   KNO_MAX_ARGS(0),NULL,NULL);
  exit_handler->fcn_handler.call0 = cleanup_app_env;
  kno_config_set("ATEXIT",(lispval)exit_handler);
  kno_decref((lispval)exit_handler);
}

static int run_init(lispval init,kno_lexenv env,kno_stack stack)
{
  lispval v = KNO_VOID;
  {U8_UNWIND_PROTECT("run_init",0) {
      u8_lock_mutex(&init_lock);
      if (KNO_APPLICABLEP(init)) {
	if (KNO_FUNCTIONP(init)) {
	  struct KNO_FUNCTION *f = (kno_function) init;
	  v = (f->fcn_arity==0) ?
	    (kno_dcall(stack,init,0,NULL)) :
	    (kno_dcall(stack,init,1,(lispval *)(&env)));}
	else v = kno_apply(init,0,NULL);}
      else if (KNO_PAIRP(init))
	v = kno_eval(init,env,stack);
      else NO_ELSE;
      if (KNO_ABORTP(v))
	inits_failed = kno_conspair(kno_incref(init),inits_failed);
      else inits_done = kno_conspair(kno_incref(init),inits_done);
      U8_ON_UNWIND
	u8_unlock_mutex(&init_lock);
      U8_END_UNWIND;}}
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
    KNO_START_EVALX(appinit,"appenv_setup",KNO_VOID,env,
		    kno_stackptr,17);
    int modules_loaded = 0, files_loaded = 0;
    int modules_failed = 0, files_failed = 0;
    int inits_run = 0, inits_failed = 0;
    lispval modules = kno_reverse(module_list);
    {KNO_DOLIST(modname,modules) {
	lispval module = kno_find_module(modname,0);
	if (KNO_ABORTP(module)) {
	  u8_log(LOG_WARN,"AppEnv/LoadModuleError",
		 "Error loading module %q",modname);
	  kno_clear_errors(1);
	  modules_failed++;}
	else {
	  lispval used = kno_use_module(kno_app_env,module);
	  if (KNO_ABORTP(used)) {
	    u8_log(LOG_WARN,"AppEnv/UseModuleError",
		   "Error using module %q",modname);
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
	  u8_log(LOG_WARN,"AppEnv/LoadFileError",
		 "Error loading %s into the application environment",
		 KNO_CSTRING(file));
	  kno_clear_errors(1);
	  files_failed++;}
	else files_loaded++;
	kno_decref(loadval);}}
    kno_decref(files);
    files=KNO_VOID;
    lispval inits = init_list;
    {KNO_DOLIST(init,inits) {
	int rv = run_init(init,env,appinit);
	if (rv > 0) inits_run++;
	else if (rv<0) inits_failed++;
	else NO_ELSE;}}
    kno_decref(inits); inits=KNO_VOID;
    kno_pop_stack(appinit);
    u8_log(LOG_INFO,"AppEnv",
	   "%d:%d:%d modules:files:inits loaded/run, "
	   "%d:%d:%d modules:files:init failed",
	   modules_loaded,files_loaded,inits_run,
	   modules_failed,files_failed,inits_failed);}
}

DEFC_PRIM("%appenv",appenv_prim,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "Returns the base 'application environment' for "
	  "the current instance")
static lispval appenv_prim()
{
  if (kno_app_env)
    return (lispval)kno_copy_env(kno_app_env);
  else return KNO_FALSE;
}

/* Module and loading config */

static u8_string get_next(u8_string pt,u8_string seps);

static lispval parse_module_spec(u8_string s);

static lispval path_module(u8_string elt)
{
  if (elt[0] == '/')
    return knostring(elt);
  else {
    u8_string path = u8_abspath(elt,NULL);
    if (path == NULL) return KNO_ERROR;
    else return kno_wrapstring(path);}
}

static lispval opt_module(u8_string elt)
{
  lispval core_spec = parse_module_spec(elt);
  return kno_make_list(2,KNOSYM_OPTIONAL,core_spec);
}

static lispval parse_module_spec(u8_string s)
{
  if (strchr(":({",*s))
    return kno_parse_arg(s);
  else if (*s) {
    u8_string brk = get_next(s," ,;");
    if (brk) {
      u8_string elt = u8_slice(s,brk);
      lispval parsed = (elt[0]==':') ? (kno_parse(elt+1)) :
	( (elt[0]=='/') || (elt[0]=='.') ) ? (path_module(elt)) :
	( (elt[0]=='~') || (elt[0]=='?') ) ? (opt_module(elt+1)) :
	(kno_parse(elt));
      u8_free(elt);
      if (KNO_ABORTP(parsed))
	return parsed;
      else return kno_init_pair(NULL,parsed,parse_module_spec(brk+1));}
    else {
      u8_string elt = s;
      lispval parsed = (elt[0]==':') ? (kno_parse(elt+1)) :
	( (elt[0]=='/') || (elt[0]=='.') ) ? (path_module(elt)) :
	( (elt[0]=='~') || (elt[0]=='?') ) ? (opt_module(elt+1)) :
	(kno_parse(elt));
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
    else if ( (KNO_VOIDP(module)) || (KNO_FALSEP(module)) ) {
      kno_seterr(kno_NoSuchModule,"add_modname",
		 (KNO_SYMBOLP(modname)) ? (KNO_SYMBOL_NAME(modname)) :
		 (KNO_STRINGP(modname)) ? (KNO_CSTRING(modname)) :
		 (NULL),
		 modname);
      return -1;}
    kno_use_module(kno_app_env,module);
    module_list = kno_conspair(modname,module_list);
    kno_incref(modname);
    kno_decref(module);
    return 1;}
  else {
    u8_log(LOG_INFO,"AppConfig","Will load module %q",modname);
    module_list = kno_conspair(modname,module_list);
    kno_incref(modname);
    return 0;}
}

static int module_config_set(lispval var,lispval vals,void *d)
{
  int loads = 0; DO_CHOICES(val,vals) {
    lispval modname = ((SYMBOLP(val))?(val):
		       (STRINGP(val))?(parse_module_spec(CSTRING(val))):
		       ( (PAIRP(val)) || (CHOICEP(val)) ) ? (kno_incref(val)) :
		       (VOID));
    if (VOIDP(modname)) {
      kno_seterr(kno_TypeError,"module_config_set","module",val);
      return -1;}
    else if ( (PAIRP(modname)) && (KNO_EQ(KNO_CAR(modname),KNOSYM_OPTIONAL)) ) {
      KNO_DOLIST(elt,KNO_CDR(modname)) {
	if ( (SYMBOLP(elt)) || (STRINGP(elt)) ) {
	  int added = add_modname(elt);
	  if (added>0) loads++;
	  else if (added<0) {
	    u8_pop_exception();}}}
      kno_decref(modname);}
    else if (PAIRP(modname)) {
      KNO_DOLIST(elt,modname) {
	if ( (SYMBOLP(elt)) || (STRINGP(elt)) ) {
	  int added = add_modname(elt);
	  if (added>0) loads++;
	  else if (added<0) {
	    kno_decref(modname);
	    return -1;}}
	else if ( (PAIRP(elt)) || (KNO_CAR(elt)==KNOSYM_OPTIONAL) ) {
	  KNO_DOLIST(e,KNO_CDR(elt)) {
	    if ( (SYMBOLP(e)) || (STRINGP(e)) ) {
	      int added = add_modname(elt);
	      if (added>0) loads++;
	      else if (added<0) {
		u8_pop_exception();}}}}
	else {
	  kno_seterr(kno_TypeError,"module_config_set","module",elt);
	  kno_decref(modname);
	  return -1;}}
      kno_decref(modname);}
    else if (CHOICEP(modname)) {
      KNO_DO_CHOICES(elt,modname) {
	if ( (SYMBOLP(elt)) || (STRINGP(elt)) ) {
	  int added = add_modname(elt);
	  if (added>0) loads++;
	  else if (added<0) {
	    kno_decref(modname);
	    return -1;}
	  else NO_ELSE;}
	else {
	  kno_seterr(kno_TypeError,"module_config_set","module",elt);
	  return -1;}}
      kno_decref(modname);}
    else if ( (SYMBOLP(modname)) || (STRINGP(modname)) ) {
      int added = add_modname(modname);
      if (added > 0) loads++;
      else if (added < 0) {
	kno_decref(modname);
	return -1;}
      else NO_ELSE;}
    else {
      kno_seterr(kno_TypeError,"module_config_set","module name",val);
      kno_decref(modname);
      return -1;}}
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
      u8_log(LOG_INFO,"AppConfig/",
	     "Will load %s (%q)",loadpath,val);
      loadfile_list = kno_conspair(kno_wrapstring(loadpath),loadfile_list);}}
  else {
    KNO_DO_CHOICES(val,vals) {
      double started = u8_elapsed_time();
      u8_string loadpath = (!(strchr(CSTRING(val),':'))) ?
	(u8_abspath(CSTRING(val),NULL)) :
	(u8_strdup(CSTRING(val)));
      u8_log(LOG_DEBUG,"AppConfig","Loading %s (%q)",loadpath,val);
      lispval loadval = kno_load_source(loadpath,kno_app_env,NULL);
      if (KNO_ABORTP(loadval)) {
	kno_seterr(_("load error"),"loadfile_config_set",loadpath,val);
	return -1;}
      else {
	u8_log(LOG_DEBUG,"AppConfig","Loaded %s (%q) in %fs",
	       loadpath,val,u8_elapsed_time()-started);
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
  if (kno_app_env)
    return kno_copy(inits_done);
  else return kno_copy(init_list);
}

static int inits_config_set(lispval var,lispval inits,void *d)
{
  int run_count = 0;
  if (kno_app_env == NULL) {
    KNO_DO_CHOICES(init,inits) {
      kno_incref(init);
      init_list = kno_conspair(init,init_list);}}
  else {
    KNO_START_EVALX(runinit,"appinit",inits,kno_app_env,kno_stackptr,17);
    KNO_DO_CHOICES(init,inits) {
      int rv = run_init(init,kno_app_env,runinit);
      if (rv > 0) {
	run_count++;}}
    kno_pop_stack(runinit);}
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

  link_local_cprims();

  kno_register_config("INITS:FAILED",
		      _("Init expressions or functions which failed"),
		      kno_lconfig_get,NULL,&inits_failed);
  kno_register_config("INITS:DONE",
		      _("Init expressions or functions which failed"),
		      kno_lconfig_get,NULL,&inits_done);
  kno_register_config("INITS:QUEUED",
		      _("Init expressions or functions which failed"),
		      kno_lconfig_get,NULL,&init_list);

  u8_init_mutex(&app_cleanup_lock);
  u8_init_mutex(&init_lock);
}



static void link_local_cprims()
{
  KNO_LINK_CPRIM("%appenv",appenv_prim,0,kno_scheme_module);
}
