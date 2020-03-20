/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* #define KNO_INLINE_EVAL 1 */

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include "kno/support.h"
#include "kno/getsource.h"
#include "kno/cprims.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>

#include <errno.h>
#include <math.h>
#include "kno/cprims.h"

u8_condition LoadConfig=_("Loading config");
u8_condition UnconfiguredSource;

static lispval simple_symbol = KNO_NULL;

static u8_string get_config_path(u8_string spec)
{
  if (*spec == '/')
    return u8_strdup(spec);
  else if (((u8_string)(strstr(spec,"file:"))) == spec)
    return u8_strdup(spec+7);
  else if (strchr(spec,':')) /* It's got a schema, assume it's absolute */
    return u8_strdup(spec);
  else {
    u8_string sourcebase = kno_sourcebase();
    if (sourcebase) {
      u8_string full = u8_mkpath(sourcebase,spec);
      if (kno_probe_source(full,NULL,NULL,NULL))
	return full;
      else u8_free(full);}
    u8_string abspath = u8_abspath(spec,NULL);
    if (u8_file_existsp(abspath))
      return abspath;
    else {
      u8_free(abspath);
      return NULL;}}
}

/* Core functions */

DEFPRIM3("config",config_get,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1)|KNO_NDOP,
	 "`(CONFIG *name* [*default*=#f] [*valfn*])`\n"
	 "Gets the configuration setting named *name*, "
	 "returning *default* if it isn't defined. *valfn*, "
	 "if provided is either a function to call on the "
	 "retrieved value or #t to indicate that string "
	 "values should be parsed as lisp",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval config_get(lispval vars,lispval dflt,lispval valfn)
{
  lispval result = EMPTY;
  DO_CHOICES(var,vars) {
    lispval value;
    if (STRINGP(var))
      value = kno_config_get(CSTRING(var));
    else if (SYMBOLP(var))
      value = kno_config_get(SYM_NAME(var));
    else {
      kno_decref(result);
      return kno_type_error(_("string or symbol"),"config_get",var);}
    if (VOIDP(value)) {}
    else if (value==KNO_DEFAULT_VALUE) {}
    else if (KNO_APPLICABLEP(valfn)) {
      lispval converted = kno_apply(valfn,1,&value);
      if (KNO_ABORTP(converted)) {
	u8_log(LOG_WARN,"ConfigConversionError",
	       "Error converting config value of %q=%q using %q",
	       var,value,valfn);
	kno_clear_errors(1);
	kno_decref(value);}
      else {
	CHOICE_ADD(result,converted);
	kno_decref(value);}}
    else if ((KNO_STRINGP(value)) && (KNO_TRUEP(valfn))) {
      u8_string valstring = KNO_CSTRING(value);
      lispval parsed = kno_parse(valstring);
      if (KNO_ABORTP(parsed)) {
	u8_log(LOG_WARN,"ConfigParseError",
	       "Error parsing config value of %q=%q",
	       var,value);
	kno_clear_errors(1);
	kno_decref(value);}
      else {
	CHOICE_ADD(result,parsed);
	kno_decref(value);}}
    else {
      CHOICE_ADD(result,value);}}
  if ( (VOIDP(result)) || (EMPTYP(result)) )
    if (VOIDP(dflt))
      return KNO_FALSE;
    else if (TYPEP(dflt,kno_promise_type))
      return kno_force_promise(dflt);
    else return kno_incref(dflt);
  else return result;
}

static lispval config_macro(lispval expr,kno_lexenv env,kno_eval_stack ptr)
{
  lispval var = kno_get_arg(expr,1);
  if (KNO_SYMBOLP(var)) {
    lispval config_val = (kno_config_get(KNO_SYMBOL_NAME(var)));
    if (KNO_VOIDP(config_val))
      return KNO_FALSE;
    else return config_val;}
  else return KNO_FALSE;
}

DEFPRIM("config!",set_config,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(CONFIG! *name1* *value1* *name2* *value2* ...)`\n"
	"Sets each configuration setting *name_i* to "
	"*value_i*. This invokes the config *handler* "
	"defined for *name_i* if there is one.");
static lispval set_config(int n,kno_argvec args)
{
  int retval, i = 0;
  if (n%2) return kno_err(kno_SyntaxError,"set_config",NULL,VOID);
  while (i<n) {
    lispval var = args[i++], val = args[i++], use_val = val;
    if (TYPEP(val,kno_promise_type))
      use_val = kno_force_promise(val);
    if (STRINGP(var))
      retval = kno_set_config(CSTRING(var),use_val);
    else if (SYMBOLP(var))
      retval = kno_set_config(SYM_NAME(var),use_val);
    else return kno_type_error(_("string or symbol"),"set_config",var);
    if (use_val != val) kno_decref(use_val);
    if (retval<0) return KNO_ERROR;}
  return VOID;
}

DEFPRIM2("config-default!",set_default_config,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(CONFIG-DEFAULT! *name* *value*)`\n"
	 "Sets the configuration value named *name* to "
	 "*value* if it has not yet been specified for the "
	 "current process. This invokes the config "
	 "*handler* defined for *name* if it has been "
	 "defined. ",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval set_default_config(lispval var,lispval val)
{
  int retval; lispval use_val = val;
  if (TYPEP(val,kno_promise_type))
    use_val = kno_force_promise(val);
  if (STRINGP(var))
    retval = kno_default_config(CSTRING(var),use_val);
  else if (SYMBOLP(var))
    retval = kno_default_config(SYM_NAME(var),use_val);
  else {
    if (use_val != val) kno_decref(use_val);
    return kno_type_error(_("string or symbol"),"config_default",var);}
  if (use_val != val) kno_decref(use_val);
  if (retval<0)
    return KNO_ERROR;
  else if (retval)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM2("find-configs",find_configs,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(FIND-CONFIGS *pattern* *withdocs*)`\n"
	 "Finds all config settings matching *pattern* and "
	 "returns their names (symbols). *pattern* is a "
	 "string or regex and a setting matches *pattern* "
	 "if a match can be found in the name of the "
	 "setting. The second argument, if specified and "
	 "true, returns the matches as pairs of names and "
	 "docstrings.",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval find_configs(lispval pat,lispval raw)
{
  int with_docs = ((VOIDP(raw))||(FALSEP(raw))||(KNO_DEFAULTP(raw)));
  lispval configs = ((with_docs)?(kno_all_configs(0)):(kno_all_configs(1)));
  lispval results = EMPTY;
  DO_CHOICES(config,configs) {
    lispval key = ((PAIRP(config))?(KNO_CAR(config)):(config));
    u8_string keystring=
      ((STRINGP(key))?(CSTRING(key)):(SYM_NAME(key)));
    if ((STRINGP(pat))?(strcasestr(keystring,CSTRING(pat))!=NULL):
	(TYPEP(pat,kno_regex_type))?
	(kno_regex_op(rx_search,pat,keystring,-1,REG_ICASE)>=0):
	(0)) {
      CHOICE_ADD(results,config);
      kno_incref(config);}}
  kno_decref(configs);
  return results;
}

static lispval lconfig_get(lispval var,void *data)
{
  lispval proc = (lispval)data;
  lispval result = kno_apply(proc,1,&var);
  return result;
}

static int lconfig_set(lispval var,lispval val,void *data)
{
  lispval proc = (lispval)data, args[2], result;
  args[0]=var; args[1]=val;
  result = kno_apply(proc,2,args);
  if (KNO_ABORTP(result))
    return kno_interr(result);
  else if (KNO_TRUEP(result)) {
    kno_decref(result); return 1;}
  else return 0;
}

static int reuse_lconfig(struct KNO_CONFIG_HANDLER *e);

DEFPRIM4("config-def!",config_def,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(CONFIG/DEF! *config_name* *handler* [*doc*] [*opts*])` "
	 "Defines the procedure *handler* as the config "
	 "handler for the *config_name* configuration "
	 "setting, with *doc* if it's provided. *handler* "
	 "should be a function of 1 required and one "
	 "optional argument. If the second argument is "
	 "provided, the configuration setting is being set; "
	 "otherwise, it is just being requested.",
	 kno_symbol_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval config_def(lispval var,lispval handler,lispval docstring,
			  lispval opts)
{
  int retval, flags = 0;
  kno_incref(handler);
  if ( (KNO_TRUEP(opts)) ||
       ( (KNO_TABLEP(opts)) &&
	 (kno_testopt(opts,KNOSYM(simple),KNO_VOID)) ) )
    flags = flags | KNO_CONFIG_SINGLE_VALUE;
  retval = kno_register_config_x
    (SYM_NAME(var),
     ((STRINGP(docstring)) ? (CSTRING(docstring)) : (NULL)),
     lconfig_get,lconfig_set,(void *) handler,0,
     reuse_lconfig);
  if (retval<0)
    return KNO_ERROR;
  return VOID;
}
static int reuse_lconfig(struct KNO_CONFIG_HANDLER *e){
  if (e->configdata) {
    kno_decref((lispval)(e->configdata));
    return 1;}
  else return 0;}

/* Loading config files */

static int trace_config_load = 0;

KNO_EXPORT int kno_load_config(u8_string sourceid)
{
  struct U8_INPUT stream; int retval;
  u8_string sourcebase = NULL, outer_sourcebase = NULL;
  u8_string fullpath = get_config_path(sourceid);
  if (fullpath == NULL)
    return KNO_ERR(-1,"MissingConfig","kno_load_config",sourceid,VOID);
  u8_string content = kno_get_source(fullpath,NULL,&sourcebase,NULL,NULL);
  u8_free(fullpath);
  if (content == NULL)
    return -1;
  else if (sourcebase) {
    outer_sourcebase = kno_bind_sourcebase(sourcebase);}
  else outer_sourcebase = NULL;
  U8_INIT_STRING_INPUT((&stream),-1,content);
  if ( (trace_config_load) || (kno_trace_config) )
    u8_log(LOG_WARN,LoadConfig,
	   "Loading config %s (%d bytes)",sourcebase,u8_strlen(content));
  retval = kno_read_config(&stream);
  if ( (trace_config_load) || (kno_trace_config) )
    u8_log(LOG_WARN,LoadConfig,"Loaded config %s",sourcebase);
  if ( (sourcebase) || (outer_sourcebase) ) {
    kno_restore_sourcebase(outer_sourcebase);
    if (sourcebase) u8_free(sourcebase);}
  u8_free(content);
  return retval;
}

KNO_EXPORT int kno_load_default_config(u8_string sourceid)
{
  struct U8_INPUT stream; int retval;
  u8_string sourcebase = NULL, outer_sourcebase = NULL;
  u8_string fullpath = get_config_path(sourceid);
  if (fullpath == NULL)
    return KNO_ERR(-1,"MissingConfig","kno_load_default_config",sourceid,VOID);
  u8_string content = kno_get_source(fullpath,NULL,&sourcebase,NULL,NULL);
  u8_free(fullpath);
  if (content == NULL)
    return -1;
  else if (sourcebase) {
    outer_sourcebase = kno_bind_sourcebase(sourcebase);}
  else outer_sourcebase = NULL;
  U8_INIT_STRING_INPUT((&stream),-1,content);
  if ( (trace_config_load) || (kno_trace_config) )
    u8_log(LOG_WARN,LoadConfig,
	   "Loading default config %s (%d bytes)",sourcebase,u8_strlen(content));
  retval = kno_read_default_config(&stream);
  if ( (trace_config_load) || (kno_trace_config) )
    u8_log(LOG_WARN,LoadConfig,"Loaded default config %s",sourcebase);
  if ( (sourcebase) || (outer_sourcebase) ) {
    kno_restore_sourcebase(outer_sourcebase);
    if (sourcebase) u8_free(sourcebase);}
  u8_free(content);
  return retval;
}

/* From Scheme */

DEFPRIM1("load-config",lisp_load_config,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(LOAD-CONFIG *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lisp_load_config(lispval arg)
{
  if (STRINGP(arg)) {
    int retval = kno_load_config(KNO_CSTRING(arg));
    if (retval<0)
      return KNO_ERROR;
    else return KNO_INT(retval);}
  else if (SYMBOLP(arg)) {
    lispval config_val = kno_config_get(SYM_NAME(arg));
    if (STRINGP(config_val)) {
      lispval result = lisp_load_config(config_val);
      kno_decref(config_val);
      return result;}
    else if (VOIDP(config_val))
      return kno_err(UnconfiguredSource,"lisp_load_config",
		     "this source is not configured",
		     arg);
    else return kno_err(UnconfiguredSource,"lisp_load_config",
			"this source source is misconfigured",
			config_val);}
  else return kno_type_error
	 ("path or config symbol","lisp_load_config",arg);
}

DEFPRIM1("load-default-config",lisp_load_default_config,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(LOAD-DEFAULT-CONFIG *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lisp_load_default_config(lispval arg)
{
  if (STRINGP(arg)) {
    int retval = kno_load_default_config(KNO_CSTRING(arg));
    if (retval<0)
      return KNO_ERROR;
    else return KNO_INT(retval);}
  else if (SYMBOLP(arg)) {
    lispval config_val = kno_config_get(SYM_NAME(arg));
    if (STRINGP(config_val)) {
      lispval result = lisp_load_default_config(config_val);
      kno_decref(config_val);
      return result;}
    else if (VOIDP(config_val))
      return kno_err(UnconfiguredSource,"lisp_load_default_config",
		     "this source is not configured",
		     arg);
    else return kno_err(UnconfiguredSource,"lisp_load_default_config",
			"this source is misconfigured",
			config_val);}
  else return kno_type_error
	 ("path or config symbol","lisp_load_default_config",arg);
}

DEFPRIM1("read-config",lisp_read_config,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(READ-CONFIG *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval lisp_read_config(lispval arg)
{
  struct U8_INPUT in; int retval;
  U8_INIT_STRING_INPUT(&in,STRLEN(arg),CSTRING(arg));
  retval = kno_read_config(&in);
  if (retval<0) return KNO_ERROR;
  else return KNO_INT2LISP(retval);
}

/* Config config */

static u8_mutex config_file_lock;

static KNO_CONFIG_RECORD *config_records = NULL, *config_stack = NULL;

static lispval get_config_files(lispval var,void U8_MAYBE_UNUSED *data)
{
  struct KNO_CONFIG_RECORD *scan; lispval result = NIL;
  u8_lock_mutex(&config_file_lock);
  scan = config_records; while (scan) {
    result = kno_conspair(kno_mkstring(scan->config_filename),result);
    scan = scan->loaded_after;}
  u8_unlock_mutex(&config_file_lock);
  return result;
}

static int add_config_file_helper(lispval var,lispval val,
				  void U8_MAYBE_UNUSED *data,
				  int isopt,int isdflt)
{
  if (!(STRINGP(val))) return -1;
  else if (STRLEN(val)==0) return 0;
  else {
    int retval;
    struct KNO_CONFIG_RECORD on_stack, *scan, *newrec;
    u8_string root = kno_sourcebase();
    u8_string pathname = u8_abspath(CSTRING(val),root);
    u8_lock_mutex(&config_file_lock);
    scan = config_stack; while (scan) {
      if (strcmp(scan->config_filename,pathname)==0) {
	u8_unlock_mutex(&config_file_lock);
	if ( (kno_trace_config) || (trace_config_load) )
	  u8_log(LOGINFO,LoadConfig,
		 "Skipping redundant reload of %s from the config %q",
		 KNO_CSTRING(val),var);
	u8_free(pathname);
	return 0;}
      else scan = scan->loaded_after;}
    if ( (kno_trace_config) || (trace_config_load) )
      u8_log(LOGWARN,LoadConfig,
	     "Loading the config file %s in response to a %q directive",
	     KNO_CSTRING(val),var);
    memset(&on_stack,0,sizeof(struct KNO_CONFIG_RECORD));
    on_stack.config_filename = pathname;
    on_stack.loaded_after = config_stack;
    config_stack = &on_stack;
    u8_unlock_mutex(&config_file_lock);
    if (isdflt)
      retval = kno_load_default_config(pathname);
    else retval = kno_load_config(pathname);
    u8_lock_mutex(&config_file_lock);
    if (retval<0) {
      if (isopt) {
	u8_free(pathname); config_stack = on_stack.loaded_after;
	u8_unlock_mutex(&config_file_lock);
	u8_pop_exception();
	return 0;}
      u8_free(pathname); config_stack = on_stack.loaded_after;
      u8_unlock_mutex(&config_file_lock);
      return retval;}
    newrec = u8_alloc(struct KNO_CONFIG_RECORD);
    newrec->config_filename = pathname;
    newrec->loaded_after = config_records;
    config_records = newrec;
    config_stack = on_stack.loaded_after;
    u8_unlock_mutex(&config_file_lock);
    return retval;}
}

static int add_config_file(lispval var,lispval val,void U8_MAYBE_UNUSED *data)
{
  return add_config_file_helper(var,val,data,0,0);
}

static int add_opt_config_file(lispval var,lispval val,void U8_MAYBE_UNUSED *data)
{
  return add_config_file_helper(var,val,data,1,0);
}

static int add_default_config_file(lispval var,lispval val,void U8_MAYBE_UNUSED *data)
{
  return add_config_file_helper(var,val,data,1,1);
}

/* Initialization */

KNO_EXPORT void kno_init_configops_c()
{
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&config_file_lock);

  link_local_cprims();

  kno_def_evalfn(kno_scheme_module,"#CONFIG",config_macro,
		 "#:CONFIG\"KNOVERSION\" or #:CONFIG:LOADPATH\n"
		 "evaluates to a value from the current configuration "
		 "environment");

  kno_register_config("CONFIG","Add a CONFIG file/URI to process",
		      get_config_files,add_config_file,NULL);
  kno_register_config("DEFAULTS","Add a CONFIG file/URI to process as defaults",
		      get_config_files,add_default_config_file,NULL);
  kno_register_config("OPTCONFIG","Add an optional CONFIG file/URI to process",
		      get_config_files,add_opt_config_file,NULL);
  kno_register_config("TRACELOADCONFIG","Trace config file loading",
		      kno_boolconfig_get,kno_boolconfig_set,&trace_config_load);
}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_PRIM("read-config",lisp_read_config,1,scheme_module);
  KNO_LINK_PRIM("load-default-config",lisp_load_default_config,1,scheme_module);
  KNO_LINK_PRIM("load-config",lisp_load_config,1,scheme_module);
  KNO_LINK_PRIM("config-def!",config_def,4,scheme_module);
  KNO_LINK_PRIM("find-configs",find_configs,2,scheme_module);
  KNO_LINK_PRIM("config-default!",set_default_config,2,scheme_module);
  KNO_LINK_VARARGS("config!",set_config,scheme_module);
  KNO_LINK_PRIM("config",config_get,3,scheme_module);

  KNO_LINK_ALIAS("config?",find_configs,scheme_module);
}
