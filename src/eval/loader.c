/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/ports.h"
#include "framerd/numbers.h"
#include "framerd/fileprims.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8netfns.h>
#include <libu8/u8xfiles.h>

#include <stdlib.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#if HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#include <ctype.h>

#if HAVE_SIGNAL_H
#include <signal.h>
#endif

#if ((HAVE_SYS_VFS_H)&&(HAVE_STATFS))
#include <sys/vfs.h>
#elif ((HAVE_SYS_FSTAT_H)&&(HAVE_STATFS))
#include <sys/statfs.h>
#endif

static fd_exception fd_ReloadError=_("Module reload error");

static u8_string libscm_path;

static lispval safe_loadpath = NIL;
static lispval loadpath = NIL;
static int log_reloads = 1;

static void add_load_record
  (lispval spec,u8_string filename,fd_lexenv env,time_t mtime);
static lispval load_source_for_module
  (lispval spec,u8_string module_source,int safe);
static u8_string get_module_source(lispval spec,int safe);

static lispval moduleid_symbol;

/* Module finding */

static int load_source_module(lispval spec,int safe,void *ignored)
{
  u8_string module_source = get_module_source(spec,safe);
  if (module_source) {
    lispval load_result = load_source_for_module(spec,module_source,safe);
    if (FD_ABORTP(load_result)) {
      u8_free(module_source); fd_decref(load_result);
      return -1;}
    else {
      lispval module_key = lispval_string(module_source);
      fd_register_module_x(module_key,load_result,safe);
      /* Store non symbolic specifiers as module identifiers */
      if (STRINGP(spec)) 
	fd_add(load_result,moduleid_symbol,spec);
      /* Register the module under its filename too. */
      if (strchr(module_source,':') == NULL) {
	lispval abspath_key = fd_lispstring(u8_abspath(module_source,NULL));
	fd_register_module_x(abspath_key,load_result,safe);
	fd_decref(abspath_key);}
      fd_decref(module_key);
      u8_free(module_source);
      fd_decref(load_result);
      return 1;}}
  else return 0;
}

static u8_string get_module_source(lispval spec,int safe)
{
  if (SYMBOLP(spec)) {
    u8_string name = u8_downcase(SYM_NAME(spec));
    u8_string module_source = u8_find_file(name,libscm_path,NULL);
    if (module_source) {
      u8_free(name);
      return module_source;}
    else if (safe==0) {
      FD_DOLIST(elt,loadpath) {
        if (STRINGP(elt)) {
          module_source = u8_find_file(name,CSTRING(elt),NULL);
          if (module_source) {
            u8_free(name);
            return module_source;}}}}
    FD_DOLIST(elt,safe_loadpath) {
      if (STRINGP(elt)) {
        module_source = u8_find_file(name,CSTRING(elt),NULL);
        if (module_source) {
          u8_free(name);
          return module_source;}}}
    u8_free(name);
    return NULL;}
  else if ((safe==0) && (STRINGP(spec))) {
    u8_string spec_data = CSTRING(spec);
    if (strchr(spec_data,':') == NULL) {
      u8_string abspath = u8_abspath(CSTRING(spec),NULL);
      if (u8_file_existsp(abspath))
        return abspath;
      else {
        u8_free(abspath);
        return NULL;}}
    else {
      u8_string use_path = NULL;
      if (fd_probe_source(spec_data,&use_path,NULL)) {
        if (use_path) return use_path;
        else return u8_strdup(spec_data);}
      else return NULL;}}
  else return NULL;
}

static lispval load_source_for_module
  (lispval spec,u8_string module_source,int safe)
{
  time_t mtime;
  fd_lexenv env=
    ((safe) ?
     (fd_safe_working_lexenv()) :
     (fd_working_lexenv()));
  lispval load_result = fd_load_source_with_date(module_source,env,"auto",&mtime);
  if (FD_ABORTP(load_result)) {
    if (HASHTABLEP(env->env_bindings))
      fd_reset_hashtable((fd_hashtable)(env->env_bindings),0,1);
    fd_decref((lispval)env);
    return load_result;}
  if (STRINGP(spec))
    fd_register_module_x(spec,(lispval) env,
                         ((safe) ? (FD_MODULE_SAFE) : (0)));
  add_load_record(spec,module_source,env,mtime);
  fd_decref(load_result);
  return (lispval)env;
}

static lispval reload_module(lispval module)
{
  if (STRINGP(module)) {
    int retval = load_source_module(module,0,NULL);
    if (retval) return FD_TRUE; else return FD_FALSE;}
  else if (SYMBOLP(module)) {
    lispval resolved = fd_get_module(module,0);
    if (TABLEP(resolved)) {
      lispval result = reload_module(resolved);
      fd_decref(resolved);
      return result;}
    else return fd_err(fd_TypeError,"reload_module",
		       "module name or path",module);}
  else if (TABLEP(module)) {
    lispval ids = fd_get(module,moduleid_symbol,EMPTY), source = VOID;
    DO_CHOICES(id,ids) {
      if (STRINGP(id)) {source = id; FD_STOP_DO_CHOICES; break;}}
    if (STRINGP(source)) {
      lispval result = reload_module(source);
      fd_decref(ids);
      return result;}
    else {
      fd_decref(ids);
      return fd_err(fd_ReloadError,"reload_module",
		    "Couldn't find source",module);}}
  else return fd_err(fd_TypeError,"reload_module",
		     "module name or path",module);
}

static lispval safe_reload_module(lispval module)
{
  if (STRINGP(module)) {
    int retval = load_source_module(module,1,NULL);
    if (retval) return FD_TRUE; else return FD_FALSE;}
  else if (SYMBOLP(module)) {
    lispval resolved = fd_get_module(module,1);
    if (TABLEP(resolved)) {
      lispval result = safe_reload_module(resolved);
      fd_decref(resolved);
      return result;}
    else return fd_err(fd_TypeError,"safe_reload_module",
		       "module name or path",module);}
  else if (TABLEP(module)) {
    lispval ids = fd_get(module,moduleid_symbol,EMPTY), source = VOID;
    DO_CHOICES(id,ids) {
      if (STRINGP(id)) {source = id; FD_STOP_DO_CHOICES; break;}}
    if (STRINGP(source)) {
      lispval result = safe_reload_module(source);
      fd_decref(ids);
      return result;}
    else {
      fd_decref(ids);
      return fd_err(fd_ReloadError,"safe_reload_module",
		    "Couldn't find source",module);}}
  else return fd_err(fd_TypeError,"safe_reload_module",
		     "module name or path",module);
}

/* Automatic module reloading */

static double reload_interval = -1.0, last_reload = -1.0;

static u8_mutex load_record_lock;
static u8_mutex update_modules_lock;

struct FD_LOAD_RECORD {
  lispval fd_loadspec;
  u8_string fd_loadfile;
  fd_lexenv fd_loadenv;
  time_t fd_modtime;
  int fd_reloading:1;
  struct FD_LOAD_RECORD *fd_next_reload;} *load_records = NULL;

static void add_load_record
  (lispval spec,u8_string filename,fd_lexenv env,time_t mtime)
{
  struct FD_LOAD_RECORD *scan;
  u8_lock_mutex(&load_record_lock);
  scan = load_records; while (scan)
    if ((strcmp(filename,scan->fd_loadfile))==0) {
      if (env!=scan->fd_loadenv) {
        fd_incref((lispval)env);
        fd_decref((lispval)(scan->fd_loadenv));
        scan->fd_loadenv = env;}
      scan->fd_modtime = mtime;
      u8_unlock_mutex(&load_record_lock);
      return;}
    else scan = scan->fd_next_reload;
  scan = u8_alloc(struct FD_LOAD_RECORD);
  scan->fd_loadfile = u8_strdup(filename);
  scan->fd_modtime = mtime; scan->fd_reloading = 0;
  scan->fd_loadenv = (fd_lexenv)fd_incref((lispval)env);
  scan->fd_loadspec = fd_incref(spec);
  scan->fd_next_reload = load_records;
  load_records = scan;
  u8_unlock_mutex(&load_record_lock);
}

typedef struct FD_MODULE_RELOAD {
  u8_string fd_loadfile;
  fd_lexenv fd_loadenv;
  lispval fd_loadspec;
  struct FD_LOAD_RECORD *fd_load_record;
  time_t fd_modtime;
  struct FD_MODULE_RELOAD *fd_next_reload;} RELOAD_MODULE;
typedef struct FD_MODULE_RELOAD *module_reload;

FD_EXPORT int fd_update_file_modules(int force)
{
  module_reload reloads = NULL, rscan;
  int n_reloads = 0;
  if ((force) ||
      ((reload_interval>=0) &&
       ((u8_elapsed_time()-last_reload)>reload_interval))) {
    struct FD_LOAD_RECORD *scan; double reload_time;
    u8_lock_mutex(&update_modules_lock);
    u8_lock_mutex(&load_record_lock);
    reload_time = u8_elapsed_time();
    scan = load_records; while (scan) {
      time_t mtime;
      /* Don't automatically update non-local files */
      if (strchr(scan->fd_loadfile,':')!=NULL) {
        scan = scan->fd_next_reload; continue;}
      else mtime = u8_file_mtime(scan->fd_loadfile);
      if (mtime>scan->fd_modtime) {
        struct FD_MODULE_RELOAD *toreload = u8_alloc(struct FD_MODULE_RELOAD);
        toreload->fd_loadfile = scan->fd_loadfile;
        toreload->fd_loadenv = scan->fd_loadenv;
        toreload->fd_loadspec = scan->fd_loadspec;
        toreload->fd_load_record = scan;
        toreload->fd_modtime = -1;
        toreload->fd_next_reload = reloads;
        rscan = reloads = toreload;
        scan->fd_reloading = 1;}
      scan = scan->fd_next_reload;}
    u8_unlock_mutex(&load_record_lock);
    rscan = reloads; while (rscan) {
      module_reload this = rscan; lispval load_result;
      u8_string filename = this->fd_loadfile;
      fd_lexenv env = this->fd_loadenv;
      time_t mtime = u8_file_mtime(this->fd_loadfile);
      rscan = this->fd_next_reload;
      if (log_reloads)
        u8_log(LOG_WARN,"fd_update_file_modules","Reloading %q from %s",
               this->fd_loadspec,filename);
      load_result = fd_load_source(filename,env,"auto");
      if (FD_ABORTP(load_result)) {
        u8_log(LOG_CRIT,"update_file_modules","Error reloading %q from %s",
               this->fd_loadspec,filename);
        fd_clear_errors(1);}
      else {
        if (log_reloads)
          u8_log(LOG_WARN,"fd_update_file_modules","Reloaded %q from %s",
                 this->fd_loadspec,filename);
        fd_decref(load_result); n_reloads++;
        this->fd_modtime = mtime;}}
    u8_lock_mutex(&load_record_lock);
    rscan = reloads; while (rscan) {
      module_reload this = rscan;
      if (this->fd_modtime>0) {
        this->fd_load_record->fd_modtime = this->fd_modtime;}
      this->fd_load_record->fd_reloading = 0;
      rscan = this->fd_next_reload;
      u8_free(this);}
    u8_unlock_mutex(&load_record_lock);
    last_reload = reload_time;
    u8_unlock_mutex(&update_modules_lock);}
  return n_reloads;
}

FD_EXPORT int fd_update_file_module(u8_string module_source,int force)
{
  struct FD_LOAD_RECORD *scan;
  time_t mtime = (time_t) -1;
  if (strchr(module_source,':') == NULL)
    mtime = u8_file_mtime(module_source);
  else if (strncasecmp(module_source,"file:",5)==0)
    mtime = u8_file_mtime(module_source+5);
  else {
    int rv = fd_probe_source(module_source,NULL,&mtime);
    if (rv<0) {
      u8_log(LOG_WARN,fd_ReloadError,"Couldn't find %s to reload it",
             module_source);
      return -1;}}
  if (mtime<0) return 0;
  u8_lock_mutex(&update_modules_lock);
  u8_lock_mutex(&load_record_lock);
  scan = load_records;
  while (scan)
    if (strcmp(scan->fd_loadfile,module_source)==0) break;
    else scan = scan->fd_next_reload;
  if ((scan)&&(scan->fd_reloading)) {
    u8_unlock_mutex(&load_record_lock);
    u8_unlock_mutex(&update_modules_lock);
    return 0;}
  else if (!(scan)) {
    u8_unlock_mutex(&load_record_lock);
    u8_unlock_mutex(&update_modules_lock);
    /* Maybe, this should load it. */
    fd_seterr(fd_ReloadError,"fd_update_file_module",
              u8_mkstring(_("The file %s has never been loaded"),
                          module_source),
              VOID);
    return -1;}
  else if ((!(force))&&(mtime<=scan->fd_modtime)) {
    u8_unlock_mutex(&load_record_lock);
    u8_unlock_mutex(&update_modules_lock);
    return 0;}
  scan->fd_reloading = 1;
  u8_unlock_mutex(&load_record_lock);
  if ((force) || (mtime>scan->fd_modtime)) {
    lispval load_result;
    if (log_reloads)
      u8_log(LOG_WARN,"fd_update_file_module","Reloading %s",
             scan->fd_loadfile);
    load_result = fd_load_source
      (scan->fd_loadfile,scan->fd_loadenv,"auto");
    if (FD_ABORTP(load_result)) {
      fd_seterr(fd_ReloadError,"fd_reload_modules",
                u8_strdup(scan->fd_loadfile),load_result);
      u8_lock_mutex(&load_record_lock);
      scan->fd_reloading = 0;
      u8_unlock_mutex(&load_record_lock);
      u8_unlock_mutex(&update_modules_lock);
      return -1;}
    else {
      fd_decref(load_result);
      u8_lock_mutex(&load_record_lock);
      scan->fd_modtime = mtime; scan->fd_reloading = 0;
      u8_unlock_mutex(&load_record_lock);
      u8_unlock_mutex(&update_modules_lock);
      return 1;}}
  else return 0;
}

static lispval update_modules_prim(lispval flag)
{
  if (fd_update_file_modules((!FALSEP(flag)))<0)
    return FD_ERROR;
  else return VOID;
}

static lispval update_module_prim(lispval spec,lispval force)
{
  if (FALSEP(force)) {
    u8_string module_source = get_module_source(spec,0);
    if (module_source) {
      int retval = fd_update_file_module(module_source,0);
      if (retval) return FD_TRUE;
      else return FD_FALSE;}
    else {
      fd_seterr(fd_ReloadError,"update_module_prim",
                u8_strdup(_("Module does not exist")),
                spec);
      return FD_ERROR;}}
  else return load_source_module(spec,0,NULL);
}

static lispval updatemodules_config_get(lispval var,void *ignored)
{
  if (reload_interval<0.0) return FD_FALSE;
  else return fd_init_double(NULL,reload_interval);
}
static int updatemodules_config_set(lispval var,lispval val,void *ignored)
{
  if (FD_FLONUMP(val)) {
    reload_interval = FD_FLONUM(val);
    return 1;}
  else if (FIXNUMP(val)) {
    reload_interval = fd_getint(val);
    return 1;}
  else if (FALSEP(val)) {
    reload_interval = -1.0;
    return 1;}
  else if (FD_TRUEP(val)) {
    reload_interval = 0.25;
    return 1;}
  else {
    fd_seterr(fd_TypeError,"updatemodules_config_set",NULL,val);
    return -1;}
}

/* Getting file sources */

static u8_string file_source_fn(int fetch,u8_string filename,u8_string encname,
                                u8_string *abspath,time_t *timep,
                                void *ignored)
{
  if (strncmp(filename,"file:",5)==0)
    filename = filename+5;
  else if (strchr(filename,':')!=NULL)
    return NULL;
  else {}
  if (fetch) {
    u8_string data = u8_filestring(filename,encname);
    if (data) {
      if (abspath) *abspath = u8_abspath(filename,NULL);
      if (timep) *timep = u8_file_mtime(filename);
      return data;}
    else return NULL;}
  else {
    time_t mtime = u8_file_mtime(filename);
    if (mtime<0) return NULL;
    if (abspath) *abspath = u8_abspath(filename,NULL);
    if (timep) *timep = mtime;
    return "exists";}
}

/* Latest source functions */

static lispval source_symbol;

static lispval get_entry(lispval key,lispval entries)
{
  lispval entry = EMPTY;
  DO_CHOICES(each,entries)
    if (!(PAIRP(each))) {}
    else if (LISP_EQUAL(key,FD_CAR(each))) {
      entry = each; FD_STOP_DO_CHOICES; break;}
    else {}
  return entry;
}

FD_EXPORT
int fd_load_latest
(u8_string filename,fd_lexenv env,u8_string base)
{
  if (filename == NULL) {
    int loads = 0;
    fd_lexenv scan = env;
    lispval result = VOID;
    while (scan) {
      lispval sources =
        fd_get(scan->env_bindings,source_symbol,EMPTY);
      DO_CHOICES(entry,sources) {
        struct FD_TIMESTAMP *loadstamp=
          fd_consptr(fd_timestamp,FD_CDR(entry),fd_timestamp_type);
        time_t mod_time = u8_file_mtime(CSTRING(FD_CAR(entry)));
        if (mod_time>loadstamp->ts_u8xtime.u8_tick) {
          struct FD_PAIR *pair = (struct FD_PAIR *)entry;
          struct FD_TIMESTAMP *tstamp = u8_alloc(struct FD_TIMESTAMP);
          FD_INIT_CONS(tstamp,fd_timestamp_type);
          u8_init_xtime(&(tstamp->ts_u8xtime),mod_time,u8_second,0,0,0);
          fd_decref(pair->cdr);
          pair->cdr = LISP_CONS(tstamp);
          if (log_reloads)
            u8_log(LOG_WARN,"fd_load_latest","Reloading %s",
                   CSTRING(FD_CAR(entry)));
          result = fd_load_source(CSTRING(FD_CAR(entry)),scan,"auto");
          if (FD_ABORTP(result)) {
            fd_decref(sources);
            return fd_interr(result);}
          else fd_decref(result);
          loads++;}}
      scan = scan->env_parent;}
    return loads;}
  else {
    u8_string abspath = u8_abspath(filename,base);
    lispval abspath_dtype = lispval_string(abspath);
    lispval sources =
      fd_get(env->env_bindings,source_symbol,EMPTY);
    lispval entry = get_entry(abspath_dtype,sources);
    lispval result = VOID;
    if (PAIRP(entry))
      if (FD_TYPEP(FD_CDR(entry),fd_timestamp_type)) {
        struct FD_TIMESTAMP *curstamp=
          fd_consptr(fd_timestamp,FD_CDR(entry),fd_timestamp_type);
        time_t last_loaded = curstamp->ts_u8xtime.u8_tick;
        time_t mod_time = u8_file_mtime(CSTRING(abspath_dtype));
        if (mod_time<=last_loaded)
          return 0;
        else {
          struct FD_PAIR *pair = (struct FD_PAIR *)entry;
          struct FD_TIMESTAMP *tstamp = u8_alloc(struct FD_TIMESTAMP);
          FD_INIT_CONS(tstamp,fd_timestamp_type);
          u8_init_xtime(&(tstamp->ts_u8xtime),mod_time,u8_second,0,0,0);
          fd_decref(pair->cdr);
          pair->cdr = LISP_CONS(tstamp);}}
      else {
        fd_seterr("Invalid load_latest record","load_latest",
                  abspath,entry);
        fd_decref(sources); fd_decref(abspath_dtype);
        return -1;}
    else {
      time_t mod_time = u8_file_mtime(abspath);
      struct FD_TIMESTAMP *tstamp = u8_alloc(struct FD_TIMESTAMP);
      FD_INIT_CONS(tstamp,fd_timestamp_type);
      u8_init_xtime(&(tstamp->ts_u8xtime),mod_time,u8_second,0,0,0);
      entry = fd_conspair(fd_incref(abspath_dtype),LISP_CONS(tstamp));
      if (EMPTYP(sources))
        fd_bind_value(source_symbol,entry,env);
      else fd_add_value(source_symbol,entry,env);}
    if (log_reloads)
      u8_log(LOG_WARN,"fd_load_latest","Reloading %s",abspath);
    result = fd_load_source(abspath,env,"auto");
    u8_free(abspath);
    fd_decref(abspath_dtype);
    fd_decref(sources);
    if (FD_ABORTP(result))
      return fd_interr(result);
    else fd_decref(result);
    return 1;}
}

static lispval load_latest_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  if (NILP(FD_CDR(expr))) {
    int loads = fd_load_latest(NULL,env,NULL);
    return FD_INT(loads);}
  else {
    int retval = -1;
    lispval path_expr = fd_get_arg(expr,1);
    lispval path = fd_eval(path_expr,env);
    if (!(STRINGP(path)))
      return fd_type_error("pathname","load_latest",path);
    else retval = fd_load_latest(CSTRING(path),env,NULL);
    if (retval<0) return FD_ERROR;
    else if (retval) {
      fd_decref(path); return FD_TRUE;}
    else {
      fd_decref(path); return FD_FALSE;}}
}

/* The init function */

static int scheme_loader_initialized = 0;

FD_EXPORT void fd_init_loader_c()
{
  lispval loader_module;
  if (scheme_loader_initialized) return;
  scheme_loader_initialized = 1;
  fd_init_scheme();
  loader_module = fd_new_module("LOADER",(FD_MODULE_DEFAULT));
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&load_record_lock);
  u8_init_mutex(&update_modules_lock);

  moduleid_symbol = fd_intern("%MODULEID");
  source_symbol = fd_intern("%SOURCE");

  /* Setup load paths */
  {u8_string path = u8_getenv("FD_INIT_LOADPATH");
    lispval v = ((path) ? (fd_lispstring(path)) :
                (lispval_string(FD_DEFAULT_LOADPATH)));
    loadpath = fd_init_pair(NULL,v,loadpath);}
    
  {u8_string path = u8_getenv("FD_INIT_SAFELOADPATH");
    lispval v = ((path) ? (fd_lispstring(path)) :
                (lispval_string(FD_DEFAULT_SAFE_LOADPATH)));
    safe_loadpath = fd_init_pair(NULL,v,safe_loadpath);}
    
  {u8_string dir=u8_getenv("FD_LIBSCM_DIR");
    if (dir==NULL) dir = FD_LIBSCM_DIR;
    if (u8_has_suffix(dir,"/",0))
      libscm_path=u8_string_append(dir,"%/module.scm:",dir,"%.scm",NULL);
    else libscm_path=u8_string_append(dir,"/%/module.scm:",dir,"/%.scm",NULL);}

  fd_register_config
    ("UPDATEMODULES","Modules to update automatically on UPDATEMODULES",
                     updatemodules_config_get,updatemodules_config_set,NULL);
  fd_register_config
    ("LOADPATH","Directories/URIs to search for modules (not sandbox)",
                     fd_lconfig_get,fd_lconfig_push,&loadpath);
  fd_register_config
    ("SAFELOADPATH","Directories/URIs to search for sandbox modules",
     fd_lconfig_get,fd_lconfig_push,&safe_loadpath);
  fd_register_config
    ("LIBSCM","The location for bundled modules (prioritized before loadpath)",
     fd_sconfig_get,fd_sconfig_set,&libscm_path);

  fd_idefn(fd_scheme_module,
           fd_make_cprim1("RELOAD-MODULE",safe_reload_module,1));
  fd_idefn(loader_module,
           fd_make_cprim1("RELOAD-MODULE",reload_module,1));
  fd_idefn(loader_module,
           fd_make_cprim1("UPDATE-MODULES",update_modules_prim,0));
  fd_idefn(loader_module,
           fd_make_cprim2x("UPDATE-MODULE",update_module_prim,1,
                           -1,VOID,-1,FD_FALSE));

  fd_defspecial(loader_module,"LOAD-LATEST",load_latest_evalfn);

  fd_add_module_loader(load_source_module,NULL);
  fd_register_sourcefn(file_source_fn,NULL);

  fd_finish_module(loader_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
