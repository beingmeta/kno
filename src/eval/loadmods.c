/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

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
#include "kno/getsource.h"
#include "kno/zipsource.h"
#include "kno/cprims.h"

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

#define STR_EXTRACT(into,start,end)					\
  u8_string into ## _end = end;						\
  size_t into ## _strlen = (into ## _end) ? ((into ## _end)-(start)) : (strlen(start)); \
  u8_byte into[into ## _strlen +1];					\
  memcpy(into,start,into ## _strlen);					\
  into[into ## _strlen]='\0'

u8_string kno_default_libscm = KNO_LIBSCM_DIR;

u8_condition MissingModule=_("Loading failed to resolve module");
u8_condition kno_ReloadError=_("Module reload error");

static lispval libscm_path = KNO_FALSE;

static lispval sys_loadpath = NIL;
static lispval loadpath = NIL;
static int check_loadpaths = 1;

static void add_load_record
(lispval spec,u8_string filename,kno_lexenv env,time_t mtime);
static lispval load_source_for_module
(lispval spec,u8_string module_source);
static u8_string get_module_source(lispval spec);

static lispval source_symbol, moduleinfo_symbol;

static struct MODULE_LOADER {
  int (*loader)(lispval,void *); void *data;
  struct MODULE_LOADER *next;} *module_loaders = NULL;

static u8_mutex module_loaders_lock;
static u8_mutex module_wait_lock;
static u8_condvar module_wait;

static int trace_dload = 0;

KNO_EXPORT
void kno_add_module_loader(int (*loader)(lispval,void *),void *data)
{
  struct MODULE_LOADER *consed = u8_alloc(struct MODULE_LOADER);
  u8_lock_mutex(&module_loaders_lock);
  consed->loader = loader;
  consed->data = data;
  consed->next = module_loaders;
  module_loaders = consed;
  u8_unlock_mutex(&module_loaders_lock);
}

/* Module finding */

static u8_string check_module_source(u8_string name,lispval path)
{
  unsigned char buf[1000];
  if (STRINGP(path)) {
    u8_string search_path = KNO_CSTRING(path);
    u8_string eqpos = strchr(search_path,'=');
    if (eqpos) {
      int name_len = strlen(name);
      int prefix_len = eqpos - search_path;
      u8_byte prefix[prefix_len+1];
      strncpy(prefix,search_path,prefix_len);
      prefix[prefix_len] = '\0';
      if ( (name_len >= prefix_len) &&
	   (u8_has_prefix(name,prefix,0)) )  {
	if (name_len == prefix_len)
	  name = name+prefix_len;
	else name = name+prefix_len+1;
	search_path = search_path+prefix_len+1;}}
    if (u8_has_prefix(search_path,"zip:",1)) {
      u8_string zip_end = strstr(search_path,".zip");
      if (zip_end==NULL) return NULL;
      else if ( (zip_end[4]=='\0') || (zip_end[4]=='/') ) {
	STR_EXTRACT(zip_path,search_path+4,zip_end+4);
	lispval zs = kno_get_zipsource(zip_path);
	u8_byte prefix_buf[100];
	u8_string prefix = (zip_end[4]=='\0') ? (U8S("")) :
	  (zip_end[5]=='\0') ? (U8S("")) :
	  (u8_has_suffix(zip_end+5,"/",0)) ? (zip_end+5) :
	  (u8_bprintf(prefix_buf,"%s/",zip_end+5));
	if (KNO_ZIPSOURCEP(zs)) {
	  u8_string probe = u8_bprintf(buf,"%s%s/module.scm",prefix,name);
	  if (kno_zipsource_existsp(zs,probe))
	    return u8_mkstring("zip:%s/%s%s/module.scm",
			       zip_path,prefix,name);
	  probe = u8_bprintf(buf,"%s%s.scm",prefix,name);
	  if (kno_zipsource_existsp(zs,probe))
	    return u8_mkstring("zip:%s/%s%s.scm",zip_path,prefix,name);
	  return NULL;}
	else return NULL;}
      else return NULL;}
    else {
      u8_string init_sep = (u8_has_suffix(search_path,"/",0))
	? (U8S(""))
	: (U8S("/"));
      u8_string probe_file =
	u8_bprintf(buf,"%s%s%s%smodule.scm",
		   search_path,init_sep,
		   name,((*name)?("/"):("")));
      if (u8_file_existsp(probe_file))
	return u8_abspath(probe_file,NULL);
      else if (*name == '\0')
	return NULL;
      else probe_file=u8_bprintf(buf,"%s%s%s.scm",search_path,init_sep,name);
      if (u8_file_existsp(probe_file))
	return u8_abspath(probe_file,NULL);
      else return NULL;}}
  else return NULL;
}

static int load_module_source(lispval spec,void *ignored)
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
      lispval module_info = kno_get(load_result,moduleinfo_symbol,KNO_VOID);
      if (TABLEP(module_info))
	kno_store(module_info,source_symbol,module_filename);
      /* Store string module IDs as %source */
      if (STRINGP(spec)) kno_add(load_result,source_symbol,spec);
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
      kno_decref(module_info);
      u8_free(module_source);
      kno_run_loadmod_hooks(load_result);
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
    {KNO_DOLIST(elt,sys_loadpath) {
	if (STRINGP(elt)) {
	  module_source = check_module_source(name,elt);
	  if (module_source) {
	    u8_free(name);
	    return module_source;}}}}
    {KNO_DOLIST(elt,loadpath) {
	if (STRINGP(elt)) {
	  module_source = check_module_source(name,elt);
	  if (module_source) {
	    u8_free(name);
	    return module_source;}}}}
    u8_free(name);
    return NULL;}
  else if (STRINGP(spec)) {
    u8_string spec_data = CSTRING(spec);
    if (strchr(spec_data,':') == NULL) {
      u8_string abspath = kno_get_component(CSTRING(spec));
      if (u8_file_existsp(abspath))
	return abspath;
      u8_free(abspath);
      u8_string use_path = NULL;
      if (kno_probe_source(spec_data,&use_path,NULL,NULL)) {
	if (use_path)
	  return use_path;
	else return u8_strdup(spec_data);}
      else return NULL;}
    else {
      u8_string use_path = NULL;
      if (kno_probe_source(spec_data,&use_path,NULL,NULL)) {
	if (use_path)
	  return use_path;
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

DEFC_PRIM("reload-module",reload_module,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"module",kno_any_type,KNO_VOID})
static lispval reload_module(lispval module)
{
  if (STRINGP(module)) {
    int retval = load_module_source(module,NULL);
    if (retval<0)
      return KNO_ERROR;
    else if (retval)
      return KNO_TRUE;
    else return KNO_FALSE;}
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
      if (kno_log_reloads)
	u8_log(LOG_WARN,"kno_update_file_modules","Reloading %q from %s",
	       this->loadarg,filename);

      load_result = kno_load_source(filename,env,"auto");

      if (KNO_ABORTP(load_result)) {
	u8_log(LOG_CRIT,"update_file_modules","Error reloading %q from %s",
	       this->loadarg,filename);
	kno_clear_errors(1);}
      else {
	if (kno_log_reloads)
	  u8_log(LOG_WARN,"kno_update_file_modules","Reloaded %q from %s",
		 this->loadarg,filename);
	kno_decref(load_result); n_reloads++;
	this->file_modtime = mtime;
	kno_run_loadmod_hooks((lispval)env);}}
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
  int rv = kno_probe_source(module_source,NULL,&mtime,NULL);
  if (rv<0) {
    u8_log(LOG_WARN,kno_ReloadError,"Couldn't find %s to reload it",
	   module_source);
    return -1;}
  if ( (mtime<0) && (!(force)) ) {
    u8_log(LOG_WARN,kno_ReloadError,
	   "Couldn't determine load time of %s to reload it",
	   module_source);
    return 0;}
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
    if (kno_log_reloads)
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
      kno_run_loadmod_hooks((lispval)(scan->loadenv));
      return 1;}}
  else return 0;
}

DEFC_PRIM("update-modules",update_modules_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "**undocumented**",
	  {"flag",kno_any_type,KNO_VOID})
static lispval update_modules_prim(lispval flag)
{
  if (kno_update_file_modules((!FALSEP(flag)))<0)
    return KNO_ERROR;
  else return VOID;
}

DEFC_PRIM("update-module",update_module_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"spec",kno_any_type,KNO_VOID},
	  {"force",kno_any_type,KNO_FALSE})
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
  else return load_module_source(spec,NULL);
}

static lispval updatemodules_config_get(lispval var,void *ignored)
{
  if ( reload_interval < 0)
    return KNO_FALSE;
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

/* libscm config set */

static lispval getsrcpath(u8_string path)
{
  lispval result = KNO_VOID;
  if (strncasecmp(path,"zip:",4)==0) {
    u8_string zip_end = strstr(path,".zip");
    if ( (zip_end == NULL) ||
	 (! ((zip_end[4]=='/') || (zip_end[4]=='\0') ) ) ) {
      if (check_loadpaths) {
	u8_seterr("BadZipSourceReference","getsrcpath",
		  u8_strdup(path));
	return KNO_ERROR;}
      else {
	u8_log(LOGERR,"BadZipReference",
	       "Path %s doesn't contain a zip file reference",
	       path);
	return KNO_FALSE;}}
    else {
      STR_EXTRACT(zipsrc,path+4,zip_end+4);
      if (!(u8_file_existsp(zipsrc))) {
	u8_log(LOGERR,"BadZipReference",
	       "The zip file '%s' in the loadpath %s doesn't exist",
	       zipsrc,path);
	return KNO_FALSE;}}
    if (path[4]=='/') return kno_mkstring(path);
    u8_string abspath = u8_abspath(path+4,NULL);
    u8_string zip_path = u8_string_append("zip:",abspath,NULL);
    result = kno_mkstring(zip_path);
    u8_free(zip_path);
    u8_free(abspath);}
  else if (strchr(path,':'))
    return kno_mkstring(path);
  else if ((*path)=='\0')
    return KNO_FALSE;
  else if (!(u8_directoryp(path))) {
    u8_log(LOGERR,"BadSourcePath",
	   "The source directory '%s' doesn't exist",path);
    return KNO_FALSE;}
  else if (*path == '/')
    return kno_mkstring(path);
  else {
    u8_string abspath = u8_abspath(path,NULL);
    result = kno_mkstring(abspath);
    u8_free(abspath);}
  return result;
}

static int libscm_config_set(lispval var,lispval vals,void *d)
{
  if (KNO_STRINGP(vals)) {
    lispval entry = getsrcpath(KNO_CSTRING(vals));
    lispval *lptr = (lispval *) d, cur = *lptr;
    if (KNO_STRINGP(entry)) {
      *lptr = entry;
      kno_decref(cur);
      return 1;}
    else if (KNO_ABORTED(entry))
      return -1;
    else return 0;}
  kno_seterr("BadLIBSCM","libscm_config_set",NULL,vals);
  return -1;
}

static char getsepchar(u8_string string)
{
  if ( (*string == ':') || (*string == ';') || (*string == ',') )
    return *string;
  else if (strchr(string,',')) return ',';
  else if (strchr(string,';')) return ';';
  else if (strchr(string,' ')) return ' ';
  else return ':';
}

/* loadpath config set */

static int loadpath_config_set(lispval var,lispval vals,void *d)
{
  lispval add_paths=KNO_EMPTY_LIST;
  DO_CHOICES(val,vals) {
    if (STRINGP(val)) {
      u8_string pathstring = CSTRING(val);
      if (strchr(pathstring,'%')) {
	/* This is especially interpreted by u8_find_file */
	add_paths = kno_init_pair(NULL,kno_incref(val),add_paths);}
      else {
	char sepchar = getsepchar(pathstring);
	u8_string scan = pathstring;
	while (scan) {
	  u8_string sep = strchr(scan,sepchar);
	  STR_EXTRACT(path,scan,sep);
	  if (*path) {
	    u8_string mountsep = strchr(path,'=');
	    if (mountsep) {
	      lispval path_elt = getsrcpath(mountsep+1);
	      if (KNO_STRINGP(path_elt)) {
		STR_EXTRACT(mountpoint,path,mountsep);
		u8_string combined =
		  u8_mkstring("%s=%s",mountpoint,KNO_CSTRING(path_elt));
		lispval push_elt = kno_mkstring(combined);
		add_paths = kno_init_pair(NULL,push_elt,add_paths);
		u8_free(combined);}
	      else if (KNO_ABORTED(path_elt)) {
		kno_decref(add_paths);
		return path_elt;}
	      else NO_ELSE;
	      kno_decref(path_elt);}
	    else {
	      lispval path_elt = getsrcpath(path);
	      if (KNO_STRINGP(path_elt))
		add_paths = kno_init_pair(NULL,path_elt,add_paths);
	      else if (KNO_ABORTED(path_elt)) {
		kno_decref(add_paths);
		return path_elt;}}}
	  if (sep) scan=sep+1;
	  else scan=NULL;}}}
    else if (check_loadpaths) {
      kno_seterr("BadLoadPathValue","loadpath_config_set",NULL,val);
      kno_decref(add_paths);
      return -1;}
    else u8_log(LOGERR,"BadLoadPath","Invalid loadpath value %q",val);}
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

/* Design for avoiding the module loading race condition */

/* Have a list of modules being sought; a function kno_need_module locks a mutex and does a
   kno_get_module.  If it gets non-void, it unlocks its mutex and returns that result.  Otherwise,
   if the module is on the list, it waits on the condvar and tries again (get and then check the
   list) when it wakes up.  If the module isn't on the list, it puts it on the list, unlocks its
   mutex and returns VOID, indicating that its caller can try to load it.  Finishing a module pops
   it off of the seeking list.  If loading the module fails, it's also popped off of the seeking
   list.  And one of the results of finish_module is that it wakes up the condvar.
*/

/* Getting the loadlock for a module */

static lispval loading_modules = EMPTY, loadstamp_symbol;

static lispval get_module_load_lock(lispval spec)
{
  lispval module;
  u8_lock_mutex(&module_wait_lock);
  module = kno_get_module(spec);
  if (!(VOIDP(module))) {
    u8_unlock_mutex(&module_wait_lock);
    return module;}
  if (kno_choice_containsp(spec,loading_modules)) {
    while (VOIDP(module)) {
      u8_condvar_wait(&module_wait,&module_wait_lock);
      module = kno_get_module(spec);}
    loading_modules = kno_difference(loading_modules,spec);
    u8_unlock_mutex(&module_wait_lock);
    return module;}
  else {
    kno_incref(spec);
    CHOICE_ADD(loading_modules,spec);
    u8_unlock_mutex(&module_wait_lock);
    return VOID;}
}

void clear_module_load_lock(lispval spec)
{
  u8_lock_mutex(&module_wait_lock);
  if (kno_choice_containsp(spec,loading_modules)) {
    lispval prev_loading = loading_modules;
    loading_modules = kno_difference(loading_modules,spec);
    kno_decref(prev_loading);
    u8_condvar_broadcast(&module_wait);}
  u8_unlock_mutex(&module_wait_lock);
}

/* Loading dynamic libraries */

static lispval dloadpath = NIL;

static void init_dloadpath()
{
  u8_string tmp = u8_getenv("KNO_INIT_DLOADPATH"); lispval strval;
  if (tmp == NULL)
    strval = kno_mkstring(KNO_DEFAULT_DLOADPATH);
  else strval = kno_wrapstring(tmp);
  dloadpath = kno_init_pair(NULL,strval,dloadpath);
  if ((tmp)||(trace_dload)||(getenv("KNO_DLOAD:TRACE")))
    u8_log(LOG_INFO,"DynamicLoadPath","Initialized to %q",
	   dloadpath);
}

static int load_dynamic_module(lispval spec,void *data)
{
  if (SYMBOLP(spec)) {
    u8_string pname = SYM_NAME(spec);
    u8_string name = u8_downcase(SYM_NAME(spec)), alt_name = NULL;
    /* The alt name has a suffix, which lets the path elements be either
       % patterns (which may provide a suffix) or just directory names
       (which may not) */
    if (strchr(pname,'.') == NULL)
      alt_name = u8_mkstring("%ls.%s",pname,KNO_DLOAD_SUFFIX);
    KNO_DOLIST(elt,dloadpath) {
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
  else return 0;
}

DEFC_PRIM("dynamic-load",dynamic_load_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "loads a dynamic module into KNO. If *modname* (a "
	  "string) is a path (includes a '/'), it is loaded "
	  "directly. Otherwise, it looks for an dynamic "
	  "module file in the default search path.",
	  {"arg",kno_string_type,KNO_VOID},
	  {"err",kno_any_type,KNO_FALSE})
static lispval dynamic_load_prim(lispval arg,lispval err)
{
  u8_string name = KNO_STRING_DATA(arg);
  if (*name=='/') {
    void *mod = u8_dynamic_load(name);
    if (mod) return KNO_TRUE;
    else return KNO_ERROR;}
  else {
    KNO_DOLIST(elt,dloadpath) {
      if (STRINGP(elt)) {
	u8_string module_name = u8_find_file(name,CSTRING(elt),NULL);
	if (module_name) {
	  void *mod = u8_dynamic_load(module_name);
	  u8_free(module_name);
	  if (mod) return KNO_TRUE;
	  else return KNO_ERROR;}}}
    if ( (KNO_FALSEP(err)) || (KNO_VOIDP(err)) )
      return KNO_FALSE;
    else return kno_err("ModuleNotFound","dynamic_load_prim",NULL,arg);}
}

/* Getting modules */

KNO_EXPORT
lispval kno_find_module(lispval spec,int err)
{
  u8_string modname = (SYMBOLP(spec)) ? (SYM_NAME(spec)) :
    (STRINGP(spec)) ? (CSTRING(spec)) : (NULL);

  lispval module = kno_get_module(spec);
  if (VOIDP(module)) module = get_module_load_lock(spec);
  if (!(VOIDP(module))) {
    lispval loadstamp = kno_get(module,loadstamp_symbol,VOID);
    while (VOIDP(loadstamp)) {
      u8_lock_mutex(&module_wait_lock);
      u8_condvar_wait(&module_wait,&module_wait_lock);
      loadstamp = kno_get(module,loadstamp_symbol,VOID);}
    clear_module_load_lock(spec);
    if (KNO_ABORTP(loadstamp))
      return loadstamp;
    else {
      kno_decref(loadstamp);
      return module;}}
  else {
    if (! ((SYMBOLP(spec)) || (STRINGP(spec))) ) {
      clear_module_load_lock(spec);
      return kno_type_error(_("module name"),"kno_find_module",spec);}
    struct MODULE_LOADER *scan = module_loaders;
    while (scan) {
      int retval = scan->loader(spec,scan->data);
      if (retval>0) {
	clear_module_load_lock(spec);
	module = kno_get_module(spec);
	if (VOIDP(module))
	  return kno_err(MissingModule,"kno_find_module",modname,spec);
	kno_finish_module(module);
	return module;}
      else if (retval<0) {
	kno_discard_module(spec);
	clear_module_load_lock(spec);
	return KNO_ERROR;}
      else scan = scan->next;}
    clear_module_load_lock(spec);
    if (err)
      return kno_err(kno_NoSuchModule,"kno_find_module",modname,spec);
    else return KNO_FALSE;}
}

KNO_EXPORT int kno_load_module(u8_string modname)
{
  lispval namesym = kno_intern(modname);
  lispval module = kno_find_module(namesym,0);
  if (KNO_FALSEP(module))
    return 0;
  else {
    kno_decref(module);
    return 1;}
}

/* Checking a module path */

static u8_string check_module_dir(u8_string dir)
{
  u8_string use_path = NULL;
  if (dir == NULL)
    return NULL;
  else if (u8_has_suffix(dir,"/",0))
    use_path=u8_strdup(dir);
  else if (u8_has_prefix(dir,"zip:",0))
    use_path=u8_strdup(dir);
  else if (u8_has_suffix(dir,".zip",0))
    use_path=u8_strdup(dir);
   else  use_path=u8_string_append(dir,"/",NULL);
  if ( (u8_has_prefix(use_path,"zip:",0)) ?
       (u8_file_existsp(use_path+4)) :
       (u8_has_suffix(use_path,".zip",0) ) ?
       (u8_file_existsp(use_path)) :
       (u8_directoryp(use_path)) )
    return use_path;
  else {
    u8_free(use_path);
    return NULL;}
}

/* The init function */

static int scheme_loadmods_initialized = 0;

lispval loadmods_module;

KNO_EXPORT void kno_init_loadmods_c()
{
  if (scheme_loadmods_initialized) return;
  scheme_loadmods_initialized = 1;
  kno_init_scheme();
  loadmods_module = kno_new_cmodule("loadmods",(KNO_MODULE_DEFAULT),kno_init_loadmods_c);
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&load_record_lock);
  u8_init_mutex(&update_modules_lock);
  u8_init_mutex(&module_loaders_lock);
  u8_init_mutex(&module_wait_lock);
  u8_init_condvar(&module_wait);

  source_symbol = kno_intern("%source");
  moduleinfo_symbol = kno_intern("%modinfo");
  loadstamp_symbol = kno_intern("%loadstamp");

  /* Setup load paths */
  {u8_string path = u8_getenv("KNO_INIT_LOADPATH");
    lispval v = ((path) ? (kno_wrapstring(path)) :
		 (kno_mkstring(KNO_DEFAULT_LOADPATH)));
    loadpath_config_set(kno_intern("loadpath"),v,&loadpath);
    kno_decref(v);}

  {u8_string dir=u8_getenv("KNO_LIBSCM_DIR");
    u8_string use_path = check_module_dir(dir);
    if (use_path == NULL) use_path=check_module_dir(kno_default_libscm);
    if (use_path == NULL) use_path=check_module_dir(KNO_LIBSCM_DIR);
    lispval new_path = (use_path) ? (knostring(use_path)) : (KNO_ERROR);
    if (!(KNO_ABORTED(new_path))) {
      lispval old_path = libscm_path;
      libscm_path = new_path;
      kno_decref(old_path);}
    else u8_raise("Bad LIBSCM path","init_libscm",use_path);
    if (dir) u8_free(dir);
    u8_free(use_path);}

  kno_register_config
    ("UPDATEMODULES","Modules to update automatically on UPDATEMODULES",
     updatemodules_config_get,updatemodules_config_set,NULL);
  kno_register_config
    ("LOADPATH","Directories/URIs to search for modules",
     loadpath_config_get,loadpath_config_set,&loadpath);
  kno_register_config
    ("SYS:LOADPATH",
     "Directories/URIs to search for modules, prioritized before LOADPATH but "
     "after LIBSCM",
     loadpath_config_get,loadpath_config_set,&loadpath);
  kno_register_config
    ("LIBSCM","The location for bundled modules (prioritized before loadpath)",
     kno_lconfig_get,libscm_config_set,&libscm_path);

  link_local_cprims();

  kno_add_module_loader(load_module_source,NULL);
  kno_add_module_loader(load_dynamic_module,NULL);
  init_dloadpath();
  kno_register_config("DLOADPATH",
		      "Add directories for dynamic compiled modules",
		      kno_lconfig_get,kno_lconfig_push,&dloadpath);
  kno_register_config("DLOAD:PATH",
		      "Add directories for dynamic compiled modules",
		      kno_lconfig_get,kno_lconfig_push,&dloadpath);
  kno_register_config("DLOAD:TRACE",
		      "Whether to announce the loading of dynamic modules",
		      kno_boolconfig_get,kno_boolconfig_set,&trace_dload);

  kno_finish_cmodule(loadmods_module);
}


static void link_local_cprims()
{
  KNO_LINK_CPRIM("dynamic-load",dynamic_load_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("update-module",update_module_prim,2,loadmods_module);
  KNO_LINK_CPRIM("update-modules",update_modules_prim,1,loadmods_module);
  KNO_LINK_CPRIM("reload-module",reload_module,1,loadmods_module);
}
