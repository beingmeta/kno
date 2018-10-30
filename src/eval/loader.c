/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
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

#if HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

#if ((HAVE_SYS_VFS_H)&&(HAVE_STATFS))
#include <sys/vfs.h>
#elif ((HAVE_SYS_FSTAT_H)&&(HAVE_STATFS))
#include <sys/statfs.h>
#endif

static u8_condition fd_ReloadError=_("Module reload error");

static u8_string libscm_path;

static lispval safe_loadpath = NIL;
static lispval loadpath = NIL;
static int log_reloads = 1;

static void add_load_record
  (lispval spec,u8_string filename,fd_lexenv env,time_t mtime);
static lispval load_source_for_module
  (lispval spec,u8_string module_source,int safe);
static u8_string get_module_source(lispval spec,int safe);

static lispval moduleid_symbol, source_symbol, loadstamps_symbol;

/* Module finding */

static int loadablep(u8_string path)
{
  if (! (u8_file_readablep(path)) )
    return 0;
  else {
    char *lpath = u8_localpath(path);
    struct stat info;
    int rv = stat(lpath,&info);
    u8_free(lpath);
    if (rv != 0)
      return 0;
    else if ( (info.st_mode&S_IFMT) == S_IFREG )
      return 1;
    else return 0;}
}

static u8_string find_path_sep(u8_string path)
{
  u8_string loc[4];
  loc[0] = strchr(path,':');
  loc[1] = strchr(path,';');
  loc[2] = strchr(path,' ');
  loc[3] = strchr(path,',');
  u8_string result = NULL;
  int i = 0;
  while (i<4) {
    if (loc[i] == NULL) i++;
    else if ( (result == NULL) || (loc[i] < result) )
      result = loc[i++];
    else i++;}
  return result;
}

static u8_string check_module_source(u8_string name,u8_string search_path)
{
  if (strchr(search_path,'%'))
    return u8_find_file(name,search_path,loadablep);
  else if (find_path_sep(search_path)) {
    u8_string start = search_path;
    while (start) {
      u8_string end = find_path_sep(start);
      size_t len = (end) ? (end-start) : (strlen(start));
      unsigned char buf[len+1];
      strncpy(buf,start,len); buf[len]='\0';
      u8_string probe = check_module_source(name,buf);
      if (probe) return probe;
      if (end) start = end+1; else start =NULL;}
    return NULL;}
  else {
    unsigned char buf[1000];
    u8_string init_sep = (u8_has_suffix(search_path,"/",0)) 
      ? (U8S(""))
      : (U8S("/"));
    if (u8_file_existsp
        (u8_bprintf(buf,"%s%s%s/module.scm",search_path,init_sep,name)))
      return u8_mkstring("%s%s%s/module.scm",search_path,init_sep,name);
    else if (u8_file_existsp(u8_bprintf(buf,"%s%s%s.scm",
                                        search_path,init_sep,name)))
      return u8_mkstring("%s%s%s.scm",search_path,init_sep,name);
    else return NULL;}
}

static int load_source_module(lispval spec,int safe,void *ignored)
{
  u8_string module_source = get_module_source(spec,safe);
  if (module_source) {
    lispval load_result = load_source_for_module(spec,module_source,safe);
    if (FD_ABORTP(load_result)) {
      u8_free(module_source);
      fd_decref(load_result);
      return -1;}
    else {
      lispval module_filename = lispval_string(module_source);
      fd_register_module_x(module_filename,load_result,safe);
      /* Store non symbolic specifiers as module identifiers */
      if (STRINGP(spec))
        fd_add(load_result,source_symbol,spec);
      if (FD_LEXENVP(load_result)) {
        fd_lexenv lexenv = (fd_lexenv) load_result;
        fd_add(load_result,source_symbol,module_filename);
        if (FD_TABLEP(lexenv->env_exports))
          fd_add(lexenv->env_exports,source_symbol,module_filename);}
      /* Register the module under its filename too. */
      if (strchr(module_source,':') == NULL) {
        lispval abspath_key = fd_lispstring(u8_abspath(module_source,NULL));
        fd_register_module_x(abspath_key,load_result,safe);
        fd_decref(abspath_key);}
      fd_decref(module_filename);
      u8_free(module_source);
      fd_decref(load_result);
      return 1;}}
  else return 0;
}

static u8_string get_module_source(lispval spec,int safe)
{
  if (SYMBOLP(spec)) {
    u8_string name = u8_downcase(SYM_NAME(spec));
    u8_string module_source = check_module_source(name,libscm_path);
    if (module_source) {
      u8_free(name);
      return module_source;}
    else if (safe==0) {
      FD_DOLIST(elt,loadpath) {
        if (STRINGP(elt)) {
          module_source = check_module_source(name,CSTRING(elt));
          if (module_source) {
            u8_free(name);
            return module_source;}}}}
    FD_DOLIST(elt,safe_loadpath) {
      if (STRINGP(elt)) {
        module_source = check_module_source(name,CSTRING(elt));
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
  lispval load_result =
    fd_load_source_with_date(module_source,env,"auto",&mtime);
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
    lispval ids = fd_get(module,source_symbol,EMPTY), source = VOID;
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
    lispval ids = fd_get(module,source_symbol,EMPTY), source = VOID;
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
  lispval loadarg;
  u8_string loadfile;
  fd_lexenv loadenv;
  time_t file_modtime;
  int reloading:1;
  struct FD_LOAD_RECORD *prev_loaded;} *load_records = NULL;

static void add_load_record
  (lispval spec,u8_string filename,fd_lexenv env,time_t mtime)
{
  struct FD_LOAD_RECORD *scan;
  u8_lock_mutex(&load_record_lock);
  scan = load_records; while (scan)
    if ((strcmp(filename,scan->loadfile))==0) {
      if (env!=scan->loadenv) {
        fd_incref((lispval)env);
        fd_decref((lispval)(scan->loadenv));
        scan->loadenv = env;}
      scan->file_modtime = mtime;
      u8_unlock_mutex(&load_record_lock);
      return;}
    else scan = scan->prev_loaded;
  scan = u8_alloc(struct FD_LOAD_RECORD);
  scan->loadfile = u8_strdup(filename);
  scan->file_modtime = mtime;
  scan->loadenv = (fd_lexenv)fd_incref((lispval)env);
  scan->loadarg = fd_incref(spec);
  scan->prev_loaded = load_records;
  scan->reloading = 0;
  load_records = scan;
  u8_unlock_mutex(&load_record_lock);
}

typedef struct FD_MODULE_RELOAD {
  u8_string loadfile;
  fd_lexenv loadenv;
  lispval loadarg;
  struct FD_LOAD_RECORD *load_record;
  time_t file_modtime;
  struct FD_MODULE_RELOAD *prev_loaded;} RELOAD_MODULE;
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
    /* Queue up all the modules needing reloading */
    scan = load_records; while (scan) {
      time_t mtime;
      /* Don't automatically update non-local loads */
      if (strchr(scan->loadfile,':')!=NULL) {
        scan = scan->prev_loaded; continue;}
      else mtime = u8_file_mtime(scan->loadfile);
      if (mtime>scan->file_modtime) {
        struct FD_MODULE_RELOAD *toreload = u8_alloc(struct FD_MODULE_RELOAD);
        toreload->loadfile = scan->loadfile;
        toreload->loadenv = scan->loadenv;
        toreload->loadarg = scan->loadarg;
        toreload->load_record = scan;
        toreload->file_modtime = -1;
        toreload->prev_loaded = reloads;
        rscan = reloads = toreload;
        scan->reloading = 1;}
      scan = scan->prev_loaded;}
    u8_unlock_mutex(&load_record_lock);
    /* Now iterate over the modules you're reloading, actually calling
       fd_load_source */
    rscan = reloads; while (rscan) {
      module_reload this = rscan; lispval load_result;
      u8_string filename = this->loadfile;
      fd_lexenv env = this->loadenv;
      time_t mtime = u8_file_mtime(this->loadfile);
      rscan = this->prev_loaded;
      if (log_reloads)
        u8_log(LOG_WARN,"fd_update_file_modules","Reloading %q from %s",
               this->loadarg,filename);

      load_result = fd_load_source(filename,env,"auto");

      if (FD_ABORTP(load_result)) {
        u8_log(LOG_CRIT,"update_file_modules","Error reloading %q from %s",
               this->loadarg,filename);
        fd_clear_errors(1);}
      else {
        if (log_reloads)
          u8_log(LOG_WARN,"fd_update_file_modules","Reloaded %q from %s",
                 this->loadarg,filename);
        fd_decref(load_result); n_reloads++;
        this->file_modtime = mtime;}}
    u8_lock_mutex(&load_record_lock);
    /* Update the load records with the new modtimes, freeing along
       the way */
    rscan = reloads; while (rscan) {
      module_reload this = rscan;
      if (this->file_modtime>0) {
        this->load_record->file_modtime = this->file_modtime;}
      this->load_record->reloading = 0;
      rscan = this->prev_loaded;
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
    if (strcmp(scan->loadfile,module_source)==0) break;
    else scan = scan->prev_loaded;
  if ((scan)&&(scan->reloading)) {
    u8_unlock_mutex(&load_record_lock);
    u8_unlock_mutex(&update_modules_lock);
    return 0;}
  else if (!(scan)) {
    u8_byte buf[200];
    u8_unlock_mutex(&load_record_lock);
    u8_unlock_mutex(&update_modules_lock);
    /* Maybe, this should load it. */
    fd_seterr(fd_ReloadError,"fd_update_file_module",
              u8_sprintf(buf,200,_("The file %s has never been loaded"),
                         module_source),
              VOID);
    return -1;}
  else if ((!(force))&&(mtime<=scan->file_modtime)) {
    u8_unlock_mutex(&load_record_lock);
    u8_unlock_mutex(&update_modules_lock);
    return 0;}
  scan->reloading = 1;
  u8_unlock_mutex(&load_record_lock);
  if ((force) || (mtime>scan->file_modtime)) {
    lispval load_result;
    if (log_reloads)
      u8_log(LOG_WARN,"fd_update_file_module","Reloading %s",
             scan->loadfile);
    load_result = fd_load_source
      (scan->loadfile,scan->loadenv,"auto");
    if (FD_ABORTP(load_result)) {
      fd_seterr(fd_ReloadError,"fd_reload_modules",
                scan->loadfile,load_result);
      u8_lock_mutex(&load_record_lock);
      scan->reloading = 0;
      u8_unlock_mutex(&load_record_lock);
      u8_unlock_mutex(&update_modules_lock);
      return -1;}
    else {
      fd_decref(load_result);
      u8_lock_mutex(&load_record_lock);
      scan->file_modtime = mtime; scan->reloading = 0;
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
                _("Module does not exist"),
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
int fd_load_latest(u8_string filename,fd_lexenv env,u8_string base)
{
  if (filename == NULL) {
    int loads = 0;
    fd_lexenv scan = env;
    lispval result = VOID;
    while (scan) {
      lispval loadstamps =
        fd_get(scan->env_bindings,loadstamps_symbol,EMPTY);
      DO_CHOICES(entry,loadstamps) {
        struct FD_TIMESTAMP *loadstamp=
          fd_consptr(fd_timestamp,FD_CDR(entry),fd_timestamp_type);
        time_t mod_time = u8_file_mtime(CSTRING(FD_CAR(entry)));
        if (mod_time>loadstamp->u8xtimeval.u8_tick) {
          struct FD_PAIR *pair = (struct FD_PAIR *)entry;
          struct FD_TIMESTAMP *tstamp = u8_alloc(struct FD_TIMESTAMP);
          FD_INIT_CONS(tstamp,fd_timestamp_type);
          u8_init_xtime(&(tstamp->u8xtimeval),mod_time,u8_second,0,0,0);
          fd_decref(pair->cdr);
          pair->cdr = LISP_CONS(tstamp);
          if (log_reloads)
            u8_log(LOG_WARN,"fd_load_latest","Reloading %s",
                   CSTRING(FD_CAR(entry)));
          result = fd_load_source(CSTRING(FD_CAR(entry)),scan,"auto");
          if (FD_ABORTP(result)) {
            fd_decref(loadstamps);
            return fd_interr(result);}
          else fd_decref(result);
          loads++;}}
      scan = scan->env_parent;}
    return loads;}
  else {
    u8_string abspath = u8_abspath(filename,base);
    lispval abspath_dtype = lispval_string(abspath);
    lispval loadstamps =
      fd_get(env->env_bindings,loadstamps_symbol,EMPTY);
    lispval entry = get_entry(abspath_dtype,loadstamps);
    lispval result = VOID;
    if (PAIRP(entry))
      if (TYPEP(FD_CDR(entry),fd_timestamp_type)) {
        struct FD_TIMESTAMP *curstamp=
          fd_consptr(fd_timestamp,FD_CDR(entry),fd_timestamp_type);
        time_t last_loaded = curstamp->u8xtimeval.u8_tick;
        time_t mod_time = u8_file_mtime(CSTRING(abspath_dtype));
        if (mod_time<=last_loaded)
          return 0;
        else {
          struct FD_PAIR *pair = (struct FD_PAIR *)entry;
          struct FD_TIMESTAMP *tstamp = u8_alloc(struct FD_TIMESTAMP);
          FD_INIT_CONS(tstamp,fd_timestamp_type);
          u8_init_xtime(&(tstamp->u8xtimeval),mod_time,u8_second,0,0,0);
          fd_decref(pair->cdr);
          pair->cdr = LISP_CONS(tstamp);}}
      else {
        fd_seterr("Invalid load_latest record","load_latest",
                  abspath,entry);
        fd_decref(loadstamps);
        fd_decref(abspath_dtype);
        return -1;}
    else {
      time_t mod_time = u8_file_mtime(abspath);
      struct FD_TIMESTAMP *tstamp = u8_alloc(struct FD_TIMESTAMP);
      FD_INIT_CONS(tstamp,fd_timestamp_type);
      u8_init_xtime(&(tstamp->u8xtimeval),mod_time,u8_second,0,0,0);
      entry = fd_conspair(fd_incref(abspath_dtype),LISP_CONS(tstamp));
      if (EMPTYP(loadstamps))
        fd_bind_value(loadstamps_symbol,entry,env);
      else fd_add_value(loadstamps_symbol,entry,env);}
    if (log_reloads)
      u8_log(LOG_WARN,"fd_load_latest","Reloading %s",abspath);
    result = fd_load_source(abspath,env,"auto");
    u8_free(abspath);
    fd_decref(abspath_dtype);
    fd_decref(loadstamps);
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

/* The LIVELOAD config */

static lispval liveload_get(lispval var,void *ignored)
{
  lispval result=EMPTY;
  struct FD_LOAD_RECORD *scan=load_records;
  while (scan) {
    lispval loadarg=fd_incref(scan->loadarg);
    CHOICE_ADD(result,loadarg);
    scan=scan->prev_loaded;}
  return result;
}

static int liveload_add(lispval var,lispval val,void *ignored)
{
  if (!(FD_STRINGP(val)))
    return fd_reterr
      (fd_TypeError,"preload_config_set",u8_strdup("string"),val);
  else if (FD_STRLEN(val)==0)
    return 0;
  else return fd_load_latest(FD_CSTRING(val),fd_app_env,NULL);
}

/* loadpath config set */


static int loadpath_config_set(lispval var,lispval vals,void *d)
{
  lispval add_paths=FD_EMPTY_LIST;
  DO_CHOICES(val,vals) {
    if (!(STRINGP(val))) {
      fd_seterr(fd_TypeError,"loadpath_config_set","filename",val);
      fd_decref(add_paths);
      return -1;}
    else {
      u8_string pathstring = CSTRING(val);
      if (strchr(pathstring,'%')) {
        add_paths = fd_init_pair(NULL,fd_incref(val),add_paths);}
      else {
        size_t len = 0, n_elts = 0;
        u8_string start = pathstring;
        while (start) {
          u8_byte buf[1000];
          u8_string next = strchr(start,':');
          if (next) {
            strncpy(buf,start,next-start);
            len = next-start;}
          else {
            strcpy(buf,start);
            len = strlen(start);}
          if (len == 0) {
            if (next)
              start=next+1;
            else start=NULL;
            continue;}
          else if (buf[len-1] == '/')
            buf[len]='\0';
          else {
            buf[len]='/'; len++;
            buf[len]='\0';}
          add_paths = fd_init_pair(NULL,fdstring(buf),add_paths);
          if (next)
            start=next+1;
          else start=NULL;}}}}
  lispval *pathref = (lispval *) d;
  lispval cur_path = *pathref, path = fd_incref(cur_path);
  {FD_DOLIST(path_elt,add_paths) {
      if (! ( (FD_PAIRP(path)) && (FD_EQUALP(path_elt,FD_CAR(path))) ) ) {
        path = fd_init_pair(NULL,fd_incref(path_elt),path);}}}
  *pathref = path;
  fd_decref(add_paths);
  fd_decref(cur_path);
  return 1;
}

static lispval loadpath_config_get(lispval var,void *d)
{
  lispval *pathref = (lispval *) d;
  lispval path = *pathref;
  return fd_incref(path);
}

/* Load file support */

#if 0

static lispval loadfile_list = NIL;

static int loadfile_config_set(lispval var,lispval vals,void *d)
{
  int loads = 0; DO_CHOICES(val,vals) {
    u8_string loadpath; lispval loadval;
    if (!(STRINGP(val))) {
      fd_seterr(fd_TypeError,"loadfile_config_set","filename",val);
      return -1;}
    else if (!(strchr(CSTRING(val),':')))
      loadpath = u8_abspath(CSTRING(val),NULL);
    else loadpath = u8_strdup(CSTRING(val));
    loadval = fd_load_source(loadpath,console_env,NULL);
    if (FD_ABORTP(loadval)) {
      fd_seterr(_("load error"),"loadfile_config_set",loadpath,val);
      return -1;}
    else {
      loadfile_list = fd_conspair(fdstring(loadpath),loadfile_list);
      u8_free(loadpath);
      loads++;}}
  return loads;
}

static lispval loadfile_config_get(lispval var,void *d)
{
  return fd_incref(loadfile_list);
}

#endif

/* The init function */

static int scheme_loader_initialized = 0;

FD_EXPORT void fd_init_loader_c()
{
  lispval loader_module;
  if (scheme_loader_initialized) return;
  scheme_loader_initialized = 1;
  fd_init_scheme();
  loader_module = fd_new_cmodule("LOADER",(FD_MODULE_DEFAULT),fd_init_loader_c);
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&load_record_lock);
  u8_init_mutex(&update_modules_lock);

  moduleid_symbol = fd_intern("%MODULEID");
  source_symbol = fd_intern("%SOURCE");
  loadstamps_symbol = fd_intern("%LOADSTAMPS");

  /* Setup load paths */
  {u8_string path = u8_getenv("FD_INIT_LOADPATH");
    lispval v = ((path) ? (fd_lispstring(path)) :
                (lispval_string(FD_DEFAULT_LOADPATH)));
    loadpath_config_set(fd_intern("LOADPATH"),v,&loadpath);
    fd_decref(v);}

  {u8_string path = u8_getenv("FD_INIT_SAFELOADPATH");
    lispval v = ((path) ? (fd_lispstring(path)) :
                (lispval_string(FD_DEFAULT_SAFE_LOADPATH)));
    loadpath_config_set(fd_intern("SAFELOADPATH"),v,&safe_loadpath);
    fd_decref(v);}

  {u8_string dir=u8_getenv("FD_LIBSCM_DIR");
    if (dir==NULL) dir = u8_strdup(FD_LIBSCM_DIR);
    if (u8_has_suffix(dir,"/",0))
      libscm_path=u8_string_append(dir,"%/module.scm:",dir,"%.scm",NULL);
    else libscm_path=u8_string_append(dir,"/%/module.scm:",dir,"/%.scm",NULL);
    u8_free(dir);}

  fd_register_config
    ("UPDATEMODULES","Modules to update automatically on UPDATEMODULES",
                     updatemodules_config_get,updatemodules_config_set,NULL);
  fd_register_config
    ("LOADPATH","Directories/URIs to search for modules (not sandbox)",
     loadpath_config_get,loadpath_config_set,&loadpath);
  fd_register_config
    ("SAFELOADPATH","Directories/URIs to search for sandbox modules",
     loadpath_config_get,loadpath_config_set,&safe_loadpath);
  fd_register_config
    ("LIBSCM","The location for bundled modules (prioritized before loadpath)",
     fd_sconfig_get,fd_sconfig_set,&libscm_path);

  fd_register_config
    ("LIVELOAD","Files to be reloaded as they change",
     liveload_get,liveload_add,NULL);
#if 0
  fd_register_config
    ("LOADFILE",_("Which files to load"),
     loadfile_config_get,loadfile_config_set,&loadfile_list);
#endif

  fd_idefn(fd_scheme_module,
           fd_make_cprim1("RELOAD-MODULE",safe_reload_module,1));
  fd_idefn(loader_module,
           fd_make_cprim1("RELOAD-MODULE",reload_module,1));
  fd_idefn(loader_module,
           fd_make_cprim1("UPDATE-MODULES",update_modules_prim,0));
  fd_idefn(loader_module,
           fd_make_cprim2x("UPDATE-MODULE",update_module_prim,1,
                           -1,VOID,-1,FD_FALSE));

  fd_def_evalfn(loader_module,"LOAD-LATEST","",load_latest_evalfn);

  fd_add_module_loader(load_source_module,NULL);
  fd_register_sourcefn(file_source_fn,NULL);

  fd_finish_module(loader_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
