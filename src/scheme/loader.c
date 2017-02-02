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
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
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

static fdtype safe_loadpath=FD_EMPTY_LIST;
static fdtype loadpath=FD_EMPTY_LIST;
static int log_reloads=1;

static void add_load_record
  (fdtype spec,u8_string filename,fd_lispenv env,time_t mtime);
static fdtype load_source_for_module
  (fdtype spec,u8_string module_source,int safe);
static u8_string get_module_source(fdtype spec,int safe);

static fdtype moduleid_symbol;

/* Module finding */

static int load_source_module(fdtype spec,int safe,void *ignored)
{
  u8_string module_source=get_module_source(spec,safe);
  if (module_source) {
    fdtype load_result=load_source_for_module(spec,module_source,safe);
    if (FD_ABORTP(load_result)) {
      u8_free(module_source); fd_decref(load_result);
      return -1;}
    else {
      fdtype module_key=fdtype_string(module_source);
      fd_register_module_x(module_key,load_result,safe);
      /* Store non symbolic specifiers as module identifiers */
      if (FD_STRINGP(spec)) 
	fd_add(load_result,moduleid_symbol,spec);
      /* Register the module under its filename too. */
      if (strchr(module_source,':')==NULL) {
	fdtype abspath_key=fd_lispstring(u8_abspath(module_source,NULL));
	fd_register_module_x(abspath_key,load_result,safe);
	fd_decref(abspath_key);}
      fd_decref(module_key);
      u8_free(module_source);
      fd_decref(load_result);
      return 1;}}
  else return 0;
}

static u8_string get_module_source(fdtype spec,int safe)
{
  if (FD_SYMBOLP(spec)) {
    u8_string name=u8_downcase(FD_SYMBOL_NAME(spec));
    u8_string module_source=NULL;
    if (safe==0) {
      FD_DOLIST(elt,loadpath) {
        if (FD_STRINGP(elt)) {
          module_source=u8_find_file(name,FD_STRDATA(elt),NULL);
          if (module_source) {
            u8_free(name);
            return module_source;}}}}
    if (module_source==NULL)  {
      FD_DOLIST(elt,safe_loadpath) {
        if (FD_STRINGP(elt)) {
          module_source=u8_find_file(name,FD_STRDATA(elt),NULL);
          if (module_source) {
            u8_free(name);
            return module_source;}}}}
    u8_free(name);
    return module_source;}
  else if ((safe==0) && (FD_STRINGP(spec))) {
    u8_string spec_data=FD_STRDATA(spec);
    if (strchr(spec_data,':')==NULL) {
      u8_string abspath=u8_abspath(FD_STRDATA(spec),NULL);
      if (u8_file_existsp(abspath))
        return abspath;
      else {
        u8_free(abspath);
        return NULL;}}
    else {
      u8_string use_path=NULL;
      if (fd_probe_source(spec_data,&use_path,NULL)) {
        if (use_path) return use_path;
        else return u8_strdup(spec_data);}
      else return NULL;}}
  else return NULL;
}

static fdtype load_source_for_module
  (fdtype spec,u8_string module_source,int safe)
{
  time_t mtime;
  fd_lispenv env=
    ((safe) ?
     (fd_safe_working_environment()) :
     (fd_working_environment()));
  fdtype load_result=fd_load_source_with_date(module_source,env,"auto",&mtime);
  if (FD_ABORTP(load_result)) {
    if (FD_HASHTABLEP(env->bindings))
      fd_reset_hashtable((fd_hashtable)(env->bindings),0,1);
    fd_decref((fdtype)env);
    return load_result;}
  if (FD_STRINGP(spec))
    fd_register_module_x(spec,(fdtype) env,
                         ((safe) ? (FD_MODULE_SAFE) : (0)));
  add_load_record(spec,module_source,env,mtime);
  fd_decref(load_result);
  return (fdtype)env;
}

static fdtype reload_module(fdtype module)
{
  if (FD_STRINGP(module)) {
    int retval=load_source_module(module,0,NULL);
    if (retval) return FD_TRUE; else return FD_FALSE;}
  else if (FD_SYMBOLP(module)) {
    fdtype resolved=fd_get_module(module,0);
    if (FD_TABLEP(resolved)) {
      fdtype result=reload_module(resolved);
      fd_decref(resolved);
      return result;}
    else return fd_err(fd_TypeError,"reload_module",
		       "module name or path",module);}
  else if (FD_TABLEP(module)) {
    fdtype ids=fd_get(module,moduleid_symbol,FD_EMPTY_CHOICE), source=FD_VOID;
    FD_DO_CHOICES(id,ids) {
      if (FD_STRINGP(id)) {source=id; FD_STOP_DO_CHOICES; break;}}
    if (FD_STRINGP(source)) {
      fdtype result=reload_module(source);
      fd_decref(ids);
      return result;}
    else {
      fd_decref(ids);
      return fd_err(fd_ReloadError,"reload_module",
		    "Couldn't find source",module);}}
  else return fd_err(fd_TypeError,"reload_module",
		     "module name or path",module);
}

static fdtype safe_reload_module(fdtype module)
{
  if (FD_STRINGP(module)) {
    int retval=load_source_module(module,1,NULL);
    if (retval) return FD_TRUE; else return FD_FALSE;}
  else if (FD_SYMBOLP(module)) {
    fdtype resolved=fd_get_module(module,1);
    if (FD_TABLEP(resolved)) {
      fdtype result=safe_reload_module(resolved);
      fd_decref(resolved);
      return result;}
    else return fd_err(fd_TypeError,"safe_reload_module",
		       "module name or path",module);}
  else if (FD_TABLEP(module)) {
    fdtype ids=fd_get(module,moduleid_symbol,FD_EMPTY_CHOICE), source=FD_VOID;
    FD_DO_CHOICES(id,ids) {
      if (FD_STRINGP(id)) {source=id; FD_STOP_DO_CHOICES; break;}}
    if (FD_STRINGP(source)) {
      fdtype result=safe_reload_module(source);
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

static double reload_interval=-1.0, last_reload=-1.0;

#if FD_THREADS_ENABLED
static u8_mutex load_record_lock;
static u8_mutex update_modules_lock;
#endif

struct FD_LOAD_RECORD {
  u8_string filename; fd_lispenv env; fdtype spec;
  time_t mtime; int reloading:1;
  struct FD_LOAD_RECORD *next;} *load_records=NULL;

static void add_load_record
  (fdtype spec,u8_string filename,fd_lispenv env,time_t mtime)
{
  struct FD_LOAD_RECORD *scan;
  fd_lock_mutex(&load_record_lock);
  scan=load_records; while (scan)
    if ((strcmp(filename,scan->filename))==0) {
      if (env!=scan->env) {
        fd_incref((fdtype)env);
        fd_decref((fdtype)(scan->env));
        scan->env=env;}
      scan->mtime=mtime;
      fd_unlock_mutex(&load_record_lock);
      return;}
    else scan=scan->next;
  scan=u8_alloc(struct FD_LOAD_RECORD);
  scan->filename=u8_strdup(filename);
  scan->mtime=mtime; scan->reloading=0;
  scan->env=(fd_lispenv)fd_incref((fdtype)env);
  scan->spec=fd_incref(spec);
  scan->next=load_records; load_records=scan;
  fd_unlock_mutex(&load_record_lock);
}

typedef struct MODULE_RELOAD {
  u8_string filename; fd_lispenv env; fdtype spec;
  struct FD_LOAD_RECORD *record; time_t mtime;
  struct MODULE_RELOAD *next;} RELOAD_MODULE;
typedef struct MODULE_RELOAD *module_reload;

FD_EXPORT int fd_update_file_modules(int force)
{
  module_reload reloads=NULL, rscan;
  int n_reloads=0;
  if ((force) ||
      ((reload_interval>=0) &&
       ((u8_elapsed_time()-last_reload)>reload_interval))) {
    struct FD_LOAD_RECORD *scan; double reload_time;
    fd_lock_mutex(&update_modules_lock);
    fd_lock_mutex(&load_record_lock);
    reload_time=u8_elapsed_time();
    scan=load_records; while (scan) {
      time_t mtime;
      /* Don't automatically update non-local files */
      if (strchr(scan->filename,':')!=NULL) {
	scan=scan->next; continue;}
      else mtime=u8_file_mtime(scan->filename);
      if (mtime>scan->mtime) {
        struct MODULE_RELOAD *toreload=u8_alloc(struct MODULE_RELOAD);
        toreload->filename=scan->filename; toreload->env=scan->env;
        toreload->spec=scan->spec;
        toreload->record=scan; toreload->mtime=-1;
        toreload->next=reloads;
        rscan=reloads=toreload;
        scan->reloading=1;}
      scan=scan->next;}
    fd_unlock_mutex(&load_record_lock);
    rscan=reloads; while (rscan) {
      module_reload this=rscan; fdtype load_result;
      u8_string filename=this->filename;
      fd_lispenv env=this->env; rscan=this->next;
      time_t mtime=u8_file_mtime(this->filename);
      if (log_reloads)
        u8_log(LOG_WARN,"fd_update_file_modules","Reloading %q from %s",
               this->spec,filename);
      load_result=fd_load_source(filename,env,"auto");
      if (FD_ABORTP(load_result)) {
        u8_log(LOG_CRIT,"update_file_modules","Error reloading %q from %s",
               this->spec,filename);
        fd_clear_errors(1);}
      else {
        if (log_reloads)
          u8_log(LOG_WARN,"fd_update_file_modules","Reloaded %q from %s",
                 this->spec,filename);
        fd_decref(load_result); n_reloads++;
        this->mtime=mtime;}}
    fd_lock_mutex(&load_record_lock);
    rscan=reloads; while (rscan) {
      module_reload this=rscan;
      if (this->mtime>0) {
        this->record->mtime=this->mtime;}
      this->record->reloading=0;
      rscan=this->next;
      u8_free(this);}
    fd_unlock_mutex(&load_record_lock);
    last_reload=reload_time;
    fd_unlock_mutex(&update_modules_lock);}
  return n_reloads;
}

FD_EXPORT int fd_update_file_module(u8_string module_source,int force)
{
  struct FD_LOAD_RECORD *scan;
  time_t mtime=(time_t) -1;
  if (strchr(module_source,':')==NULL)
    mtime=u8_file_mtime(module_source);
  else if (strncasecmp(module_source,"file:",5)==0) 
    mtime=u8_file_mtime(module_source+5);
  else {
    int rv=fd_probe_source(module_source,NULL,&mtime);
    if (rv<0) {
      u8_log(LOG_WARN,fd_ReloadError,
	     "Couldn't find %s to reload it",module_source);
      return -1;}}
  if (mtime<0) return 0;
  fd_lock_mutex(&update_modules_lock);
  fd_lock_mutex(&load_record_lock);
  scan=load_records;
  while (scan)
    if (strcmp(scan->filename,module_source)==0) break;
    else scan=scan->next;
  if ((scan)&&(scan->reloading)) {
    fd_unlock_mutex(&load_record_lock);
    fd_unlock_mutex(&update_modules_lock);
    return 0;}
  else if (!(scan)) {
    fd_unlock_mutex(&load_record_lock);
    fd_unlock_mutex(&update_modules_lock);
    /* Maybe, this should load it. */
    fd_seterr(fd_ReloadError,"fd_update_file_module",
              u8_mkstring(_("The file %s has never been loaded"),
                          module_source),
              FD_VOID);
    return -1;}
  else if ((!(force))&&(mtime<=scan->mtime)) {
    fd_unlock_mutex(&load_record_lock);
    fd_unlock_mutex(&update_modules_lock);
    return 0;}
  scan->reloading=1;
  fd_unlock_mutex(&load_record_lock);
  if ((force) || (mtime>scan->mtime)) {
    fdtype load_result;
    if (log_reloads)
      u8_log(LOG_WARN,"fd_update_file_module","Reloading %s",scan->filename);
    load_result=fd_load_source(scan->filename,scan->env,"auto");
    if (FD_ABORTP(load_result)) {
      fd_seterr(fd_ReloadError,"fd_reload_modules",
                u8_strdup(scan->filename),load_result);
      fd_lock_mutex(&load_record_lock);
      scan->reloading=0;
      fd_unlock_mutex(&load_record_lock);
      fd_unlock_mutex(&update_modules_lock);
      return -1;}
    else {
      fd_decref(load_result);
      fd_lock_mutex(&load_record_lock);
      scan->mtime=mtime; scan->reloading=0;
      fd_unlock_mutex(&load_record_lock);
      fd_unlock_mutex(&update_modules_lock);
      return 1;}}
  else return 0;
}

static fdtype update_modules_prim(fdtype flag)
{
  if (fd_update_file_modules((!FD_FALSEP(flag)))<0)
    return FD_ERROR_VALUE;
  else return FD_VOID;
}

static fdtype update_module_prim(fdtype spec,fdtype force)
{
  if (FD_FALSEP(force)) {
    u8_string module_source=get_module_source(spec,0);
    if (module_source) {
      int retval=fd_update_file_module(module_source,0);
      if (retval) return FD_TRUE;
      else return FD_FALSE;}
    else {
      fd_seterr(fd_ReloadError,"update_module_prim",
                u8_strdup(_("Module does not exist")),
                spec);
      return FD_ERROR_VALUE;}}
  else return load_source_module(spec,0,NULL);
}

static fdtype updatemodules_config_get(fdtype var,void *ignored)
{
  if (reload_interval<0.0) return FD_FALSE;
  else return fd_init_double(NULL,reload_interval);
}
static int updatemodules_config_set(fdtype var,fdtype val,void *ignored)
{
  if (FD_FLONUMP(val)) {
    reload_interval=FD_FLONUM(val);
    return 1;}
  else if (FD_FIXNUMP(val)) {
    reload_interval=fd_getint(val);
    return 1;}
  else if (FD_FALSEP(val)) {
    reload_interval=-1.0;
    return 1;}
  else if (FD_TRUEP(val)) {
    reload_interval=0.25;
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
    filename=filename+5;
  else if (strchr(filename,':')!=NULL)
    return NULL;
  else {}
  if (fetch) {
    u8_string data=u8_filestring(filename,encname);
    if (data) {
      if (abspath) *abspath=u8_abspath(filename,NULL);
      if (timep) *timep=u8_file_mtime(filename);
      return data;}
    else return NULL;}
  else {
    time_t mtime=u8_file_mtime(filename);
    if (mtime<0) return NULL;
    if (abspath) *abspath=u8_abspath(filename,NULL);
    if (timep) *timep=mtime;
    return "exists";}
}

/* Latest source functions */

static fdtype source_symbol;

static fdtype get_entry(fdtype key,fdtype entries)
{
  fdtype entry=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(each,entries)
    if (!(FD_PAIRP(each))) {}
    else if (FDTYPE_EQUAL(key,FD_CAR(each))) {
      entry=each; FD_STOP_DO_CHOICES; break;}
    else {}
  return entry;
}

FD_EXPORT int fd_load_latest(u8_string filename,fd_lispenv env,u8_string base)
{
  if (filename==NULL) {
    int loads=0; fd_lispenv scan=env; fdtype result=FD_VOID;
    while (scan) {
      fdtype sources=fd_get(scan->bindings,source_symbol,FD_EMPTY_CHOICE);
      FD_DO_CHOICES(entry,sources) {
        struct FD_TIMESTAMP *loadstamp=
          FD_GET_CONS(FD_CDR(entry),fd_timestamp_type,struct FD_TIMESTAMP *);
        time_t mod_time=u8_file_mtime(FD_STRDATA(FD_CAR(entry)));
        if (mod_time>loadstamp->fd_u8xtime.u8_tick) {
          struct FD_PAIR *pair=(struct FD_PAIR *)entry;
          struct FD_TIMESTAMP *tstamp=u8_alloc(struct FD_TIMESTAMP);
          FD_INIT_CONS(tstamp,fd_timestamp_type);
          u8_init_xtime(&(tstamp->fd_u8xtime),mod_time,u8_second,0,0,0);
          fd_decref(pair->fd_cdr);
          pair->fd_cdr=FDTYPE_CONS(tstamp);
          if (log_reloads)
            u8_log(LOG_WARN,"fd_load_latest","Reloading %s",
                   FD_STRDATA(FD_CAR(entry)));
          result=fd_load_source(FD_STRDATA(FD_CAR(entry)),scan,"auto");
          if (FD_ABORTP(result)) {
            fd_decref(sources);
            return fd_interr(result);}
          else fd_decref(result);
          loads++;}}
      scan=scan->parent;}
    return loads;}
  else {
    u8_string abspath=u8_abspath(filename,base);
    fdtype abspath_dtype=fdtype_string(abspath);
    fdtype sources=fd_get(env->bindings,source_symbol,FD_EMPTY_CHOICE);
    fdtype entry=get_entry(abspath_dtype,sources);
    fdtype result=FD_VOID;
    if (FD_PAIRP(entry))
      if (FD_PTR_TYPEP(FD_CDR(entry),fd_timestamp_type)) {
        struct FD_TIMESTAMP *curstamp=
          FD_GET_CONS(FD_CDR(entry),fd_timestamp_type,struct FD_TIMESTAMP *);
        time_t last_loaded=curstamp->fd_u8xtime.u8_tick;
        time_t mod_time=u8_file_mtime(FD_STRDATA(abspath_dtype));
        if (mod_time<=last_loaded) return 0;
        else {
          struct FD_PAIR *pair=(struct FD_PAIR *)entry;
          struct FD_TIMESTAMP *tstamp=u8_alloc(struct FD_TIMESTAMP);
          FD_INIT_CONS(tstamp,fd_timestamp_type);
          u8_init_xtime(&(tstamp->fd_u8xtime),mod_time,u8_second,0,0,0);
          fd_decref(pair->fd_cdr);
          pair->fd_cdr=FDTYPE_CONS(tstamp);}}
      else {
        fd_seterr("Invalid load_latest record","load_latest",abspath,entry);
        fd_decref(sources); fd_decref(abspath_dtype);
        return -1;}
    else {
      time_t mod_time=u8_file_mtime(abspath);
      struct FD_TIMESTAMP *tstamp=u8_alloc(struct FD_TIMESTAMP);
      FD_INIT_CONS(tstamp,fd_timestamp_type);
      u8_init_xtime(&(tstamp->fd_u8xtime),mod_time,u8_second,0,0,0);
      entry=fd_conspair(fd_incref(abspath_dtype),FDTYPE_CONS(tstamp));
      if (FD_EMPTY_CHOICEP(sources)) fd_bind_value(source_symbol,entry,env);
      else fd_add_value(source_symbol,entry,env);}
    if (log_reloads)
      u8_log(LOG_WARN,"fd_load_latest","Reloading %s",abspath);
    result=fd_load_source(abspath,env,"auto");
    u8_free(abspath);
    fd_decref(abspath_dtype);
    fd_decref(sources);
    if (FD_ABORTP(result))
      return fd_interr(result);
    else fd_decref(result);
    return 1;}
}

static fdtype load_latest(fdtype expr,fd_lispenv env)
{
  if (FD_EMPTY_LISTP(FD_CDR(expr))) {
    int loads=fd_load_latest(NULL,env,NULL);
    return FD_INT(loads);}
  else {
    int retval=-1;
    fdtype path_expr=fd_get_arg(expr,1);
    fdtype path=fd_eval(path_expr,env);
    if (!(FD_STRINGP(path)))
      return fd_type_error("pathname","load_latest",path);
    else retval=fd_load_latest(FD_STRDATA(path),env,NULL);
    if (retval<0) return FD_ERROR_VALUE;
    else if (retval) {
      fd_decref(path); return FD_TRUE;}
    else {
      fd_decref(path); return FD_FALSE;}}
}

/* The init function */

static int scheme_loader_initialized=0;

FD_EXPORT void fd_init_loader_c()
{
  fdtype loader_module;
  if (scheme_loader_initialized) return;
  scheme_loader_initialized=1;
  fd_init_fdscheme();
  loader_module=fd_new_module("LOADER",(FD_MODULE_DEFAULT));
  u8_register_source_file(_FILEINFO);

#if FD_THREADS_ENABLED
  fd_init_mutex(&load_record_lock);
  fd_init_mutex(&update_modules_lock);
#endif

  moduleid_symbol=fd_intern("%MODULEID");
  source_symbol=fd_intern("%SOURCE");

  /* Setup load paths */
  {
    u8_string path=u8_getenv("FD_INIT_LOADPATH");
    fdtype v=((path) ? (fd_lispstring(path)) :
              (fdtype_string(FD_DEFAULT_LOADPATH)));
    loadpath=fd_init_pair(NULL,v,loadpath);}
  {
    u8_string path=u8_getenv("FD_INIT_SAFELOADPATH");
    fdtype v=((path) ? (fd_lispstring(path)) :
              (fdtype_string(FD_DEFAULT_SAFE_LOADPATH)));
    safe_loadpath=fd_init_pair(NULL,v,safe_loadpath);}

  fd_register_config
    ("UPDATEMODULES","Modules to update automatically on UPDATEMODULES",
                     updatemodules_config_get,updatemodules_config_set,NULL);
  fd_register_config
    ("LOADPATH","Directories/URIs to search for modules (not sandbox)",
                     fd_lconfig_get,fd_lconfig_push,&loadpath);
  fd_register_config
    ("SAFELOADPATH","Directories/URIs to search for sandbox modules",
     fd_lconfig_get,fd_lconfig_push,&safe_loadpath);

  fd_idefn(fd_scheme_module,
           fd_make_cprim1("RELOAD-MODULE",safe_reload_module,1));
  fd_idefn(loader_module,
           fd_make_cprim1("RELOAD-MODULE",reload_module,1));
  fd_idefn(loader_module,
           fd_make_cprim1("UPDATE-MODULES",update_modules_prim,0));
  fd_idefn(loader_module,
           fd_make_cprim2x("UPDATE-MODULE",update_module_prim,1,
                           -1,FD_VOID,-1,FD_FALSE));

  fd_defspecial(loader_module,"LOAD-LATEST",load_latest);

  fd_add_module_loader(load_source_module,NULL);
  fd_register_sourcefn(file_source_fn,NULL);

  fd_finish_module(loader_module);
  fd_persist_module(loader_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
