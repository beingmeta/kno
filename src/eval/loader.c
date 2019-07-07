/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/ports.h"
#include "kno/numbers.h"
#include "kno/fileprims.h"

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

static u8_condition kno_ReloadError=_("Module reload error");

static u8_string libscm_path;

static lispval loadpath = NIL;
static int log_reloads = 1;

static void add_load_record
(lispval spec,u8_string filename,kno_lexenv env,time_t mtime);
static lispval load_source_for_module
(lispval spec,u8_string module_source);
static u8_string get_module_source(lispval spec);

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

static int load_source_module(lispval spec,void *ignored)
{
  u8_string module_source = get_module_source(spec);
  if (module_source) {
    lispval load_result = load_source_for_module(spec,module_source);
    if (KNO_ABORTP(load_result)) {
      u8_free(module_source);
      kno_decref(load_result);
      return -1;}
    else {
      lispval module_filename = kno_mkstring(module_source);
      kno_register_module_x(module_filename,load_result,0);
      /* Store non symbolic specifiers as module identifiers */
      if (STRINGP(spec))
        kno_add(load_result,source_symbol,spec);
      if (KNO_LEXENVP(load_result)) {
        kno_lexenv lexenv = (kno_lexenv) load_result;
        kno_add(load_result,source_symbol,module_filename);
        if (KNO_TABLEP(lexenv->env_exports))
          kno_add(lexenv->env_exports,source_symbol,module_filename);}
      /* Register the module under its filename too. */
      if (strchr(module_source,':') == NULL) {
        lispval abspath_key = kno_wrapstring(u8_abspath(module_source,NULL));
        kno_register_module_x(abspath_key,load_result,0);
        kno_decref(abspath_key);}
      kno_decref(module_filename);
      u8_free(module_source);
      kno_decref(load_result);
      return 1;}}
  else return 0;
}

static u8_string get_module_source(lispval spec)
{
  if (SYMBOLP(spec)) {
    u8_string name = u8_downcase(SYM_NAME(spec));
    u8_string module_source = check_module_source(name,libscm_path);
    if (module_source) {
      u8_free(name);
      return module_source;}
    KNO_DOLIST(elt,loadpath) {
      if (STRINGP(elt)) {
        module_source = check_module_source(name,CSTRING(elt));
        if (module_source) {
          u8_free(name);
          return module_source;}}}
    u8_free(name);
    return NULL;}
  else if (STRINGP(spec)) {
    u8_string spec_data = CSTRING(spec);
    if (strchr(spec_data,':') == NULL) {
      u8_string abspath = kno_get_component(CSTRING(spec));
      if (u8_file_existsp(abspath))
        return abspath;
      else {
        u8_free(abspath);
        return NULL;}}
    else {
      u8_string use_path = NULL;
      if (kno_probe_source(spec_data,&use_path,NULL)) {
        if (use_path) return use_path;
	else return u8_strdup(spec_data);}
      else return NULL;}}
  else return NULL;
}

static lispval load_source_for_module(lispval spec,u8_string module_source)
{
  time_t mtime;
  kno_lexenv env = kno_working_lexenv();
  lispval load_result =
    kno_load_source_with_date(module_source,env,"auto",&mtime);
  if (KNO_ABORTP(load_result)) {
    if (HASHTABLEP(env->env_bindings))
      kno_reset_hashtable((kno_hashtable)(env->env_bindings),0,1);
    kno_decref((lispval)env);
    return load_result;}
  if (STRINGP(spec))
    kno_register_module_x(spec,(lispval) env,0);
  add_load_record(spec,module_source,env,mtime);
  kno_decref(load_result);
  return (lispval)env;
}

static lispval reload_module(lispval module)
{
  if (STRINGP(module)) {
    int retval = load_source_module(module,NULL);
    if (retval) return KNO_TRUE; else return KNO_FALSE;}
  else if (SYMBOLP(module)) {
    lispval resolved = kno_get_module(module);
    if (TABLEP(resolved)) {
      lispval result = reload_module(resolved);
      kno_decref(resolved);
      return result;}
    else return kno_err(kno_TypeError,"reload_module",
                        "module name or path",module);}
  else if (TABLEP(module)) {
    lispval ids = kno_get(module,source_symbol,EMPTY), source = VOID;
    DO_CHOICES(id,ids) {
      if (STRINGP(id)) {source = id; KNO_STOP_DO_CHOICES; break;}}
    if (STRINGP(source)) {
      lispval result = reload_module(source);
      kno_decref(ids);
      return result;}
    else {
      kno_decref(ids);
      return kno_err(kno_ReloadError,"reload_module",
                     "Couldn't find source",module);}}
  else return kno_err(kno_TypeError,"reload_module",
                      "module name or path",module);
}

/* Automatic module reloading */

static double reload_interval = -1.0, last_reload = -1.0;

static u8_mutex load_record_lock;
static u8_mutex update_modules_lock;

struct KNO_LOAD_RECORD {
  lispval loadarg;
  u8_string loadfile;
  kno_lexenv loadenv;
  time_t file_modtime;
  int reloading:1;
  struct KNO_LOAD_RECORD *prev_loaded;} *load_records = NULL;

static void add_load_record
(lispval spec,u8_string filename,kno_lexenv env,time_t mtime)
{
  struct KNO_LOAD_RECORD *scan;
  u8_lock_mutex(&load_record_lock);
  scan = load_records; while (scan)
                         if ((strcmp(filename,scan->loadfile))==0) {
                           if (env!=scan->loadenv) {
                             kno_incref((lispval)env);
                             kno_decref((lispval)(scan->loadenv));
                             scan->loadenv = env;}
                           scan->file_modtime = mtime;
                           u8_unlock_mutex(&load_record_lock);
                           return;}
                         else scan = scan->prev_loaded;
  scan = u8_alloc(struct KNO_LOAD_RECORD);
  scan->loadfile = u8_strdup(filename);
  scan->file_modtime = mtime;
  scan->loadenv = (kno_lexenv)kno_incref((lispval)env);
  scan->loadarg = kno_incref(spec);
  scan->prev_loaded = load_records;
  scan->reloading = 0;
  load_records = scan;
  u8_unlock_mutex(&load_record_lock);
}

typedef struct KNO_MODULE_RELOAD {
  u8_string loadfile;
  kno_lexenv loadenv;
  lispval loadarg;
  struct KNO_LOAD_RECORD *load_record;
  time_t file_modtime;
  struct KNO_MODULE_RELOAD *prev_loaded;} RELOAD_MODULE;
typedef struct KNO_MODULE_RELOAD *module_reload;

KNO_EXPORT int kno_update_file_modules(int force)
{
  module_reload reloads = NULL, rscan;
  int n_reloads = 0;
  if ((force) ||
      ((reload_interval>=0) &&
       ((u8_elapsed_time()-last_reload)>reload_interval))) {
    struct KNO_LOAD_RECORD *scan; double reload_time;
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
        struct KNO_MODULE_RELOAD *toreload = u8_alloc(struct KNO_MODULE_RELOAD);
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
       kno_load_source */
    rscan = reloads; while (rscan) {
      module_reload this = rscan; lispval load_result;
      u8_string filename = this->loadfile;
      kno_lexenv env = this->loadenv;
      time_t mtime = u8_file_mtime(this->loadfile);
      rscan = this->prev_loaded;
      if (log_reloads)
        u8_log(LOG_WARN,"kno_update_file_modules","Reloading %q from %s",
               this->loadarg,filename);

      load_result = kno_load_source(filename,env,"auto");

      if (KNO_ABORTP(load_result)) {
        u8_log(LOG_CRIT,"update_file_modules","Error reloading %q from %s",
               this->loadarg,filename);
        kno_clear_errors(1);}
      else {
        if (log_reloads)
          u8_log(LOG_WARN,"kno_update_file_modules","Reloaded %q from %s",
                 this->loadarg,filename);
        kno_decref(load_result); n_reloads++;
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

KNO_EXPORT int kno_update_file_module(u8_string module_source,int force)
{
  struct KNO_LOAD_RECORD *scan;
  time_t mtime = (time_t) -1;
  if (strchr(module_source,':') == NULL)
    mtime = u8_file_mtime(module_source);
  else if (strncasecmp(module_source,"file:",5)==0)
    mtime = u8_file_mtime(module_source+5);
  else {
    int rv = kno_probe_source(module_source,NULL,&mtime);
    if (rv<0) {
      u8_log(LOG_WARN,kno_ReloadError,"Couldn't find %s to reload it",
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
    kno_seterr(kno_ReloadError,"kno_update_file_module",
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
      u8_log(LOG_WARN,"kno_update_file_module","Reloading %s",
             scan->loadfile);
    load_result = kno_load_source
      (scan->loadfile,scan->loadenv,"auto");
    if (KNO_ABORTP(load_result)) {
      kno_seterr(kno_ReloadError,"kno_reload_modules",
                 scan->loadfile,load_result);
      u8_lock_mutex(&load_record_lock);
      scan->reloading = 0;
      u8_unlock_mutex(&load_record_lock);
      u8_unlock_mutex(&update_modules_lock);
      return -1;}
    else {
      kno_decref(load_result);
      u8_lock_mutex(&load_record_lock);
      scan->file_modtime = mtime; scan->reloading = 0;
      u8_unlock_mutex(&load_record_lock);
      u8_unlock_mutex(&update_modules_lock);
      return 1;}}
  else return 0;
}

static lispval update_modules_prim(lispval flag)
{
  if (kno_update_file_modules((!FALSEP(flag)))<0)
    return KNO_ERROR;
  else return VOID;
}

static lispval update_module_prim(lispval spec,lispval force)
{
  if (FALSEP(force)) {
    u8_string module_source = get_module_source(spec);
    if (module_source) {
      int retval = kno_update_file_module(module_source,0);
      u8_free(module_source);
      if (retval)
        return KNO_TRUE;
      else return KNO_FALSE;}
    else return kno_err(kno_ReloadError,"update_module_prim",
                        _("Module does not exist"),
                        spec);}
  else return load_source_module(spec,NULL);
}

static lispval updatemodules_config_get(lispval var,void *ignored)
{
  if (reload_interval<0.0) return KNO_FALSE;
  else return kno_init_double(NULL,reload_interval);
}
static int updatemodules_config_set(lispval var,lispval val,void *ignored)
{
  if (KNO_FLONUMP(val)) {
    reload_interval = KNO_FLONUM(val);
    return 1;}
  else if (FIXNUMP(val)) {
    reload_interval = kno_getint(val);
    return 1;}
  else if (FALSEP(val)) {
    reload_interval = -1.0;
    return 1;}
  else if (KNO_TRUEP(val)) {
    reload_interval = 0.25;
    return 1;}
  else {
    kno_seterr(kno_TypeError,"updatemodules_config_set",NULL,val);
    return -1;}
}

/* Getting file sources */

static u8_string file_source_fn(int fetch,lispval pathspec,u8_string encname,
                                u8_string *abspath,time_t *timep,
				void *ignored)
{
  u8_string filename = NULL;
  if (KNO_STRINGP(pathspec)) {
    u8_string path = KNO_CSTRING(pathspec);
    if (strncmp(path,"file:",5)==0)
      filename = path+5;
    else if (strchr(path,':')!=NULL)
      return NULL;
    else filename = path;}
  else return NULL;
  if (fetch) {
    u8_string data = u8_filestring(filename,encname);
    if (data) {
      if (abspath) *abspath = u8_realpath(filename,NULL); /* u8_abspath? */
      if (timep) *timep = u8_file_mtime(filename);
      return data;}
    else return NULL;}
  else {
    time_t mtime = u8_file_mtime(filename);
    if (mtime<0) return NULL;
    if (abspath) *abspath = u8_realpath(filename,NULL); /* u8_abspath? */
    if (timep) *timep = mtime;
    return "exists";}
}

/* Latest source functions */

static lispval get_entry(lispval key,lispval entries)
{
  lispval entry = EMPTY;
  DO_CHOICES(each,entries)
    if (!(PAIRP(each))) {}
    else if (LISP_EQUAL(key,KNO_CAR(each))) {
      entry = each;
      KNO_STOP_DO_CHOICES;
      break;}
    else {}
  return entry;
}

KNO_EXPORT
int kno_load_latest(u8_string filename,kno_lexenv env,u8_string base)
{
  if (filename == NULL) {
    int loads = 0;
    kno_lexenv scan = env;
    lispval result = VOID;
    while (scan) {
      lispval loadstamps =
        kno_get(scan->env_bindings,loadstamps_symbol,EMPTY);
      DO_CHOICES(entry,loadstamps) {
        struct KNO_TIMESTAMP *loadstamp=
          kno_consptr(kno_timestamp,KNO_CDR(entry),kno_timestamp_type);
        time_t mod_time = u8_file_mtime(CSTRING(KNO_CAR(entry)));
        if (mod_time>loadstamp->u8xtimeval.u8_tick) {
          struct KNO_PAIR *pair = (struct KNO_PAIR *)entry;
          struct KNO_TIMESTAMP *tstamp = u8_alloc(struct KNO_TIMESTAMP);
          KNO_INIT_CONS(tstamp,kno_timestamp_type);
          u8_init_xtime(&(tstamp->u8xtimeval),mod_time,u8_second,0,0,0);
          kno_decref(pair->cdr);
          pair->cdr = LISP_CONS(tstamp);
          if (log_reloads)
            u8_log(LOG_WARN,"kno_load_latest","Reloading %s",
                   CSTRING(KNO_CAR(entry)));
          result = kno_load_source(CSTRING(KNO_CAR(entry)),scan,"auto");
          if (KNO_ABORTP(result)) {
            KNO_STOP_DO_CHOICES;
            break;}
          else kno_decref(result);
          loads++;}}
      kno_decref(loadstamps);
      if (KNO_ABORTP(result))
        return kno_interr(result);
      else scan = scan->env_parent;}
    return loads;}
  else {
    u8_string abspath = u8_abspath(filename,base);
    lispval lisp_abspath = kno_mkstring(abspath);
    lispval loadstamps =
      kno_get(env->env_bindings,loadstamps_symbol,EMPTY);
    lispval entry = get_entry(lisp_abspath,loadstamps);
    lispval result = VOID;
    if (PAIRP(entry))
      if (TYPEP(KNO_CDR(entry),kno_timestamp_type)) {
        struct KNO_TIMESTAMP *curstamp=
          kno_consptr(kno_timestamp,KNO_CDR(entry),kno_timestamp_type);
        time_t last_loaded = curstamp->u8xtimeval.u8_tick;
        time_t mod_time = u8_file_mtime(CSTRING(lisp_abspath));
        if (mod_time<=last_loaded) {
          kno_decref(lisp_abspath);
          kno_decref(loadstamps);
          u8_free(abspath);
          return 0;}
        else {
          struct KNO_PAIR *pair = (struct KNO_PAIR *)entry;
          struct KNO_TIMESTAMP *tstamp = u8_alloc(struct KNO_TIMESTAMP);
          KNO_INIT_CONS(tstamp,kno_timestamp_type);
          u8_init_xtime(&(tstamp->u8xtimeval),mod_time,u8_second,0,0,0);
          kno_decref(pair->cdr);
          pair->cdr = LISP_CONS(tstamp);}}
      else {
        kno_seterr("Invalid load_latest record","load_latest",
                   abspath,entry);
        kno_decref(loadstamps);
        kno_decref(lisp_abspath);
        u8_free(abspath);
        return -1;}
    else {
      time_t mod_time = u8_file_mtime(abspath);
      struct KNO_TIMESTAMP *tstamp = u8_alloc(struct KNO_TIMESTAMP);
      KNO_INIT_CONS(tstamp,kno_timestamp_type);
      u8_init_xtime(&(tstamp->u8xtimeval),mod_time,u8_second,0,0,0);
      entry = kno_conspair(kno_incref(lisp_abspath),LISP_CONS(tstamp));
      if (EMPTYP(loadstamps))
        kno_bind_value(loadstamps_symbol,entry,env);
      else kno_add_value(loadstamps_symbol,entry,env);}
    if (log_reloads)
      u8_log(LOG_WARN,"kno_load_latest","Reloading %s",abspath);
    result = kno_load_source(abspath,env,"auto");
    u8_free(abspath);
    kno_decref(lisp_abspath);
    kno_decref(loadstamps);
    if (KNO_ABORTP(result))
      return kno_interr(result);
    else kno_decref(result);
    return 1;}
}

static lispval load_latest_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  if (NILP(KNO_CDR(expr))) {
    int loads = kno_load_latest(NULL,env,NULL);
    return KNO_INT(loads);}
  else {
    int retval = -1;
    lispval path_expr = kno_get_arg(expr,1);
    lispval path = kno_eval(path_expr,env);
    if (!(STRINGP(path)))
      return kno_type_error("pathname","load_latest",path);
    else retval = kno_load_latest(CSTRING(path),env,NULL);
    kno_decref(path);
    if (retval<0)
      return KNO_ERROR;
    else if (retval)
      return KNO_TRUE;
    else return KNO_FALSE;}
}

/* The LIVELOAD config */

#if 0
static lispval liveload_get(lispval var,void *ignored)
{
  lispval result=EMPTY;
  struct KNO_LOAD_RECORD *scan=load_records;
  while (scan) {
    lispval loadarg=kno_incref(scan->loadarg);
    CHOICE_ADD(result,loadarg);
    scan=scan->prev_loaded;}
  return result;
}

static int liveload_add(lispval var,lispval val,void *ignored)
{
  if (!(KNO_STRINGP(val)))
    return kno_reterr
      (kno_TypeError,"preload_config_set",u8_strdup("string"),val);
  else if (KNO_STRLEN(val)==0)
    return 0;
  else return kno_load_latest(KNO_CSTRING(val),kno_app_env,NULL);
}

#endif

/* loadpath config set */


static int loadpath_config_set(lispval var,lispval vals,void *d)
{
  lispval add_paths=KNO_EMPTY_LIST;
  DO_CHOICES(val,vals) {
    if (!(STRINGP(val))) {
      kno_seterr(kno_TypeError,"loadpath_config_set","filename",val);
      kno_decref(add_paths);
      return -1;}
    else {
      u8_string pathstring = CSTRING(val);
      if (strchr(pathstring,'%')) {
        add_paths = kno_init_pair(NULL,kno_incref(val),add_paths);}
      else {
        size_t len = 0;
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
          add_paths = kno_init_pair(NULL,knostring(buf),add_paths);
          if (next)
            start=next+1;
          else start=NULL;}}}}
  lispval *pathref = (lispval *) d;
  lispval cur_path = *pathref, path = kno_incref(cur_path);
  {KNO_DOLIST(path_elt,add_paths) {
      if (! ( (KNO_PAIRP(path)) && (KNO_EQUALP(path_elt,KNO_CAR(path))) ) ) {
        path = kno_init_pair(NULL,kno_incref(path_elt),path);}}}
  *pathref = path;
  kno_decref(add_paths);
  kno_decref(cur_path);
  return 1;
}

static lispval loadpath_config_get(lispval var,void *d)
{
  lispval *pathref = (lispval *) d;
  lispval path = *pathref;
  return kno_incref(path);
}

/* The init function */

static int scheme_loader_initialized = 0;

KNO_EXPORT void kno_init_loader_c()
{
  lispval loader_module;
  if (scheme_loader_initialized) return;
  scheme_loader_initialized = 1;
  kno_init_scheme();
  loader_module = kno_new_cmodule("loader",(KNO_MODULE_DEFAULT),kno_init_loader_c);
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&load_record_lock);
  u8_init_mutex(&update_modules_lock);

  moduleid_symbol = kno_intern("%moduleid");
  source_symbol = kno_intern("%source");
  loadstamps_symbol = kno_intern("%loadstamps");

  /* Setup load paths */
  {u8_string path = u8_getenv("KNO_INIT_LOADPATH");
    lispval v = ((path) ? (kno_wrapstring(path)) :
                 (kno_mkstring(KNO_DEFAULT_LOADPATH)));
    loadpath_config_set(kno_intern("loadpath"),v,&loadpath);
    kno_decref(v);}

  {u8_string dir=u8_getenv("KNO_LIBSCM_DIR");
    if (dir==NULL) dir = u8_strdup(KNO_LIBSCM_DIR);
    if (u8_has_suffix(dir,"/",0))
      libscm_path=dir;
    else {
      libscm_path=u8_string_append(dir,"/",NULL);
      u8_free(dir);}}

  kno_register_config
    ("UPDATEMODULES","Modules to update automatically on UPDATEMODULES",
     updatemodules_config_get,updatemodules_config_set,NULL);
  kno_register_config
    ("LOADPATH","Directories/URIs to search for modules (not sandbox)",
     loadpath_config_get,loadpath_config_set,&loadpath);
  kno_register_config
    ("LIBSCM","The location for bundled modules (prioritized before loadpath)",
     kno_sconfig_get,kno_sconfig_set,&libscm_path);

#if 0
  kno_register_config
    ("LIVELOAD","Files to be reloaded as they change",
     liveload_get,liveload_add,NULL);
#endif

  kno_idefn(loader_module,
            kno_make_cprim1("RELOAD-MODULE",reload_module,1));
  kno_idefn(loader_module,
            kno_make_cprim1("UPDATE-MODULES",update_modules_prim,0));
  kno_idefn(loader_module,
            kno_make_cprim2x("UPDATE-MODULE",update_module_prim,1,
                             -1,VOID,-1,KNO_FALSE));

  kno_def_evalfn(loader_module,"LOAD-LATEST","",load_latest_evalfn);

  kno_add_module_loader(load_source_module,NULL);
  kno_register_sourcefn(file_source_fn,NULL);

  kno_finish_module(loader_module);
}
