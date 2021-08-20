/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* #define KNO_EVAL_INTERNALS 1 */

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

extern u8_condition UnconfiguredSource;

static lispval simple_symbol = KNO_NULL;

/* Core functions */

DEFC_PRIM("config",config_get,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "Gets the configuration setting named *name*, "
	  "returning *default* if it isn't defined. *valfn*, "
	  "if provided is either a function to call on the "
	  "retrieved value or #t to indicate that string "
	  "values should be parsed as lisp",
	  {"vars",kno_any_type,KNO_VOID},
	  {"dflt",kno_any_type,KNO_VOID},
	  {"valfn",kno_any_type,KNO_VOID})
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

static lispval config_macro(lispval expr,kno_lexenv env,kno_stack ptr)
{
  lispval var = kno_get_arg(expr,1);
  if ( (KNO_SYMBOLP(var)) || (KNO_STRINGP(var)) ) {
    u8_string name = (KNO_SYMBOLP(var)) ? (KNO_SYMBOL_NAME(var)) :
      (KNO_CSTRING(var));
    lispval config_val = kno_config_get(name);
    if (KNO_VOIDP(config_val))
      return KNO_FALSE;
    else return config_val;}
  else if ( (KNO_PAIRP(var)) &&
	    ( (KNO_SYMBOLP(KNO_CAR(var))) || (KNO_STRINGP(KNO_CDR(var))) ) ) {
    lispval spec = KNO_CAR(var);
    u8_string name = (KNO_SYMBOLP(spec)) ? (KNO_SYMBOL_NAME(spec)) :
      (KNO_CSTRING(spec));
    lispval config_val = kno_config_get(name);
    if (KNO_VOIDP(config_val)) {
      if (KNO_PAIRP(KNO_CDR(var)))
	return kno_incref(KNO_CADR(var));
      else return KNO_FALSE;}
    else return config_val;}
  else return KNO_FALSE;
}

static lispval string_macro(lispval expr,kno_lexenv env,kno_stack ptr)
{
  lispval arg = kno_get_arg(expr,1);
  if (KNO_STRINGP(arg))
    return kno_incref(arg);
  else if (KNO_SYMBOLP(arg))
    return knostring(KNO_SYMBOL_NAME(arg));
  else if (KNO_PAIRP(arg)) {
    U8_STATIC_OUTPUT(string,128);
    lispval scan = arg; while (KNO_PAIRP(scan)) {
      lispval car = KNO_CAR(scan); scan = KNO_CDR(scan);
      lispval elt = (KNO_PAIRP(car)) ? (kno_interpret_config(car)) :
	(kno_incref(car));
      if (KNO_STRINGP(elt))
	u8_putn(stringout,KNO_CSTRING(elt),KNO_STRLEN(elt));
      else kno_unparse(stringout,elt);
      kno_decref(elt);}
    if (scan != KNO_EMPTY_LIST) {
      u8_log(LOGWARN,"BadConfigValue","Tail=%q in %q",
	     scan,arg);}
    lispval result = kno_stream2string(stringout);
    u8_close_output(stringout);
    return result;}
  else return kno_incref(arg);
}

static lispval now_macro(lispval expr,kno_lexenv env,kno_stack ptr)
{
  lispval field = kno_get_arg(expr,1);
  lispval now = kno_make_timestamp(NULL);
  lispval v = (FALSEP(field)) ? (kno_incref(now)) :
    (kno_get(now,field,KNO_VOID));
  kno_decref(now);
  if ( (KNO_VOIDP(v)) || (KNO_EMPTYP(v)) )
    return KNO_FALSE;
  else return v;
}

static lispval getenv_macro(lispval expr,kno_lexenv env,kno_stack ptr)
{
  lispval var = kno_get_arg(expr,1);
  if ( (KNO_STRINGP(var)) || (KNO_SYMBOLP(var)) ) {
    u8_string enval = (KNO_SYMBOLP(var)) ?
      (u8_getenv(KNO_SYMBOL_NAME(var))) :
      (u8_getenv(CSTRING(var)));
    if (enval == NULL)
      return KNO_FALSE;
    else return kno_wrapstring(enval);}
  else if ( (KNO_PAIRP(var)) &&
	    ( (KNO_STRINGP(KNO_CAR(var))) ||
	      (KNO_SYMBOLP(KNO_CAR(var))) ) )  {
    lispval name = KNO_CAR(var);
    u8_string enval = (KNO_SYMBOLP(name)) ?
      (u8_getenv(KNO_SYMBOL_NAME(name))) :
      (u8_getenv(CSTRING(name)));
    if (enval == NULL) {
      if (KNO_PAIRP(KNO_CDR(var))) {
	lispval dflt = KNO_CADR(var);
	return kno_incref(dflt);}
      else return KNO_FALSE;}
    else return kno_wrapstring(enval);}
  else return kno_err(kno_TypeError,"getenv_macro","string or symbol",var);
}

DEFC_PRIMN("config!",set_config,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "\n"
	   "Sets each configuration setting *name_i* to "
	   "*value_i*. This invokes the config *handler* "
	   "defined for *name_i* if there is one.")
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

DEFC_PRIM("config-default!",set_default_config,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "\n"
	  "Sets the configuration value named *name* to "
	  "*value* if it has not yet been specified for the "
	  "current process. This invokes the config "
	  "*handler* defined for *name* if it has been "
	  "defined. ",
	  {"var",kno_any_type,KNO_VOID},
	  {"val",kno_any_type,KNO_VOID})
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

DEFC_PRIM("find-configs",find_configs,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "\n"
	  "Finds all config settings matching *pattern* and "
	  "returns their names (symbols). *pattern* is a "
	  "string or regex and a setting matches *pattern* "
	  "if a match can be found in the name of the "
	  "setting. The second argument, if specified and "
	  "true, returns the matches as pairs of names and "
	  "docstrings.",
	  {"pat",kno_any_type,KNO_VOID},
	  {"raw",kno_any_type,KNO_VOID})
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

DEFC_PRIM("config-def!",config_def,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	  "Defines the procedure *handler* as the config "
	  "handler for the *config_name* configuration "
	  "setting, with *doc* if it's provided. *handler* "
	  "should be a function of 1 required and one "
	  "optional argument. If the second argument is "
	  "provided, the configuration setting is being set; "
	  "otherwise, it is just being requested.",
	  {"var",kno_symbol_type,KNO_VOID},
	  {"handler",kno_any_type,KNO_VOID},
	  {"docstring",kno_string_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
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

/* From Scheme */

DEFC_PRIM("load-config",lisp_load_config,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("load-default-config",lisp_load_default_config,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("read-config",lisp_read_config,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_string_type,KNO_VOID})
static lispval lisp_read_config(lispval arg)
{
  struct U8_INPUT in; int retval;
  U8_INIT_STRING_INPUT(&in,STRLEN(arg),CSTRING(arg));
  retval = kno_read_config(&in);
  if (retval<0) return KNO_ERROR;
  else return KNO_INT2LISP(retval);
}

/* Initialization */

KNO_EXPORT void kno_init_configops_c()
{
  u8_register_source_file(_FILEINFO);

  link_local_cprims();

  kno_def_evalfn(kno_scheme_module,"#CONFIG",config_macro,
		 "#:CONFIG\"KNOVERSION\" or #:CONFIG:LOADPATH\n"
		 "evaluates to a value from the current configuration "
		 "environment");
  kno_def_evalfn(kno_scheme_module,"#NOW",now_macro,
		 "#:NOW:YEAR\n evaluates to a field of the current time");
  kno_def_evalfn(kno_scheme_module,"#ENV",getenv_macro,
		 "#:ENV\"HOME\" or #:ENV:HOME\n"
		 "evaluates to an environment variable");
  kno_def_evalfn(kno_scheme_module,"#STRING",string_macro,
		 "#:STRING(FOO 3 #:envHOME)\n"
		 "evaluates to string made up of components");
  kno_def_evalfn(kno_scheme_module,"#GLOM",string_macro,
		 "#:GLOM(FOO 3 #:envHOME)\n"
		 "evaluates to string made up of components");
}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_CPRIM("read-config",lisp_read_config,1,scheme_module);
  KNO_LINK_CPRIM("load-default-config",lisp_load_default_config,1,scheme_module);
  KNO_LINK_CPRIM("load-config",lisp_load_config,1,scheme_module);
  KNO_LINK_CPRIM("config-def!",config_def,4,scheme_module);
  KNO_LINK_CPRIM("find-configs",find_configs,2,scheme_module);
  KNO_LINK_CPRIM("config-default!",set_default_config,2,scheme_module);
  KNO_LINK_CPRIMN("config!",set_config,scheme_module);
  KNO_LINK_CPRIM("config",config_get,3,scheme_module);

  KNO_LINK_ALIAS("config?",find_configs,scheme_module);
}
