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
#include "kno/numbers.h"
#include "kno/support.h"
#include "kno/cprims.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>


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
    else return kno_incref(dflt);
  else return result;
}

static lispval config_macro(lispval expr,kno_lexenv env,kno_stack ptr)
{
  lispval var = kno_get_arg(expr,1);
  if (KNO_SYMBOLP(var)) {
    lispval config_val = (kno_config_get(KNO_SYMBOL_NAME(var)));
    if (KNO_VOIDP(config_val))
      return KNO_FALSE;
    else return config_val;}
  else return KNO_FALSE;
}

static lispval set_config(int n,lispval *args)
{
  int retval, i = 0;
  if (n%2) return kno_err(kno_SyntaxError,"set_config",NULL,VOID);
  while (i<n) {
    lispval var = args[i++], val = args[i++];
    if (STRINGP(var))
      retval = kno_set_config(CSTRING(var),val);
    else if (SYMBOLP(var))
      retval = kno_set_config(SYM_NAME(var),val);
    else return kno_type_error(_("string or symbol"),"set_config",var);
    if (retval<0) return KNO_ERROR;}
  return VOID;
}

static lispval config_default(lispval var,lispval val)
{
  int retval;
  if (STRINGP(var))
    retval = kno_default_config(CSTRING(var),val);
  else if (SYMBOLP(var))
    retval = kno_default_config(SYM_NAME(var),val);
  else return kno_type_error(_("string or symbol"),"config_default",var);
  if (retval<0) return KNO_ERROR;
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

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
        (TYPEP(pat,kno_regex_type))?(kno_regex_test(pat,keystring,-1)):
        (0)) {
      CHOICE_ADD(results,config);
      kno_incref(config);}}
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
DCLPRIM3("CONFIG-DEF!",config_def,MIN_ARGS(2),
         "`(CONFIG/DEF! *config_name* *handler* [*doc*])` Defines "
         "the procedure *handler* as the config handler for the "
         "*config_name* configuration setting, with *doc* if it's "
         "provided. *handler* should be a function of 1 required and "
         "one optional argument. If the second argument is provided, "
         "the configuration setting is being set; otherwise, it is just "
         "being requested.",
         kno_symbol_type,KNO_VOID,-1,KNO_VOID,kno_string_type,KNO_VOID)
static lispval config_def(lispval var,lispval handler,lispval docstring)
{
  int retval;
  kno_incref(handler);
  retval = kno_register_config_x
    (SYM_NAME(var),
     ((STRINGP(docstring)) ? (CSTRING(docstring)) : (NULL)),
     lconfig_get,lconfig_set,(void *) handler,
     reuse_lconfig);
  if (retval<0) {
    kno_decref(handler);
    return KNO_ERROR;}
  return VOID;
}
static int reuse_lconfig(struct KNO_CONFIG_HANDLER *e){
  if (e->configdata) {
    kno_decref((lispval)(e->configdata));
    return 1;}
  else return 0;}

/* Initialization */

KNO_EXPORT void kno_init_configops_c()
{
  u8_register_source_file(_FILEINFO);

  kno_idefn3(kno_scheme_module,"CONFIG",config_get,KNO_NEEDS_1_ARG,
            "CONFIG *name* *default* *valfn*)\n"
            "Gets the configuration value named *name*, returning *default* "
            "if it isn't defined. *valfn*, if provided is either a function "
            "to call on the retrieved value or #t to indicate that string "
            "values should be parsed as lisp",
            -1,KNO_VOID,-1,KNO_VOID,-1,KNO_VOID);

  kno_def_evalfn(kno_scheme_module,"#CONFIG",
                "#:CONFIG\"KNOVERSION\" or #:CONFIG:LOADPATH\n"
                "evaluates to a value from the current configuration "
                "environment",
                config_macro);

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("SET-CONFIG!",set_config,2)));
  kno_defalias(kno_scheme_module,"CONFIG!","SET-CONFIG!");
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim2("CONFIG-DEFAULT!",config_default,2)));
  kno_idefn(kno_scheme_module,kno_make_cprim2("FIND-CONFIGS",find_configs,1));
  kno_defalias(kno_scheme_module,"CONFIG?","FIND-CONFIGS");

  DECL_PRIM(config_def,3,kno_scheme_module);

}

