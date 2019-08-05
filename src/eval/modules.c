/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/cprims.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8printf.h>
#include <libu8/u8filefns.h>

#if HAVE_DLFCN_H
#include <dlfcn.h>

#endif

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static lispval moduleid_symbol, source_symbol, loadstamp_symbol, dlsource_symbol;

u8_condition kno_NotAModule=_("Argument is not a module (table)");
u8_condition kno_NoSuchModule=_("Can't find named module");
u8_condition OpaqueModule=_("Can't switch to opaque module");

static struct KNO_HASHTABLE module_map;
static kno_lexenv default_env = NULL;
lispval kno_scheme_module = KNO_VOID;
lispval kno_io_module = KNO_VOID;
lispval kno_db_module = KNO_VOID;
lispval kno_sys_module = KNO_VOID;

void clear_module_load_lock(lispval spec);

/* Module system */

KNO_EXPORT kno_lexenv kno_make_env(lispval bindings,kno_lexenv parent)
{
  if (PRED_FALSE(!(TABLEP(bindings)) )) {
    u8_byte buf[100];
    kno_seterr(kno_TypeError,"kno_make_env",
               u8_sprintf(buf,100,_("object is not a %m"),"table"),
               bindings);
    return NULL;}
  else {
    struct KNO_LEXENV *e = u8_alloc(struct KNO_LEXENV);
    KNO_INIT_FRESH_CONS(e,kno_lexenv_type);
    e->env_bindings = bindings; e->env_exports = VOID;
    e->env_parent = kno_copy_env(parent);
    e->env_copy = e;
    return e;}
}


KNO_EXPORT
/* kno_make_export_env:
   Arguments: a hashtable and an environment
   Returns: a consed environment whose bindings and exports
   are the exports table.  This indicates that the environment
   is "for export only" and cannot be modified. */
kno_lexenv kno_make_export_env(lispval exports,kno_lexenv parent)
{
  if (PRED_FALSE(!(HASHTABLEP(exports)) )) {
    u8_byte buf[100];
    kno_seterr(kno_TypeError,"kno_make_env",
               u8_sprintf(buf,100,_("object is not a %m"),"hashtable"),
               exports);
    return NULL;}
  else {
    struct KNO_LEXENV *e = u8_alloc(struct KNO_LEXENV);
    KNO_INIT_FRESH_CONS(e,kno_lexenv_type);
    e->env_bindings = kno_incref(exports);
    e->env_exports = kno_incref(e->env_bindings);
    e->env_parent = kno_copy_env(parent);
    e->env_copy = e;
    return e;}
}

KNO_EXPORT kno_lexenv kno_new_lexenv(lispval bindings)
{
  if (kno_scheme_initialized==0) kno_init_scheme();
  if (VOIDP(bindings))
    bindings = kno_make_hashtable(NULL,17);
  else kno_incref(bindings);
  kno_lexenv env = kno_make_env(bindings,default_env);
  if (env==NULL) kno_decref(bindings);
  return env;
}
KNO_EXPORT kno_lexenv kno_working_lexenv()
{
  if (kno_scheme_initialized==0) kno_init_scheme();
  lispval table = kno_make_hashtable(NULL,17);
  kno_lexenv env = kno_make_env(table,default_env);
  if (env == NULL) kno_decref(table);
  return env;
}

KNO_EXPORT lispval kno_register_module_x(lispval name,lispval module,int flags)
{
  kno_hashtable_store(&module_map,name,module);

  /* Set the module ID*/
  if (KNO_LEXENVP(module)) {
    kno_lexenv env = (kno_lexenv)module;
    kno_add(env->env_bindings,moduleid_symbol,name);}
  else if (HASHTABLEP(module))
    kno_add(module,moduleid_symbol,name);
  else {}

  /* Add to the appropriate default environment */
  if (flags&KNO_MODULE_DEFAULT) {
    kno_lexenv scan;
    scan = default_env;
    while (scan)
      if (KNO_EQ(scan->env_bindings,module)) return module;
      else scan = scan->env_parent;
    default_env->env_parent=
      kno_make_env(kno_incref(module),default_env->env_parent);}
  return module;
}

KNO_EXPORT lispval kno_register_module(u8_string name,lispval module,int flags)
{
  return kno_register_module_x(kno_intern(name),module,flags);
}

KNO_EXPORT lispval kno_new_module(char *name,int flags)
{
  lispval module_name, module, as_stored;
  if (kno_scheme_initialized==0) kno_init_scheme();
  module_name = kno_getsym(name);
  module = kno_make_hashtable(NULL,0);
  kno_add(module,moduleid_symbol,module_name);
  struct KNO_HASHTABLE *modmap = &module_map;
  kno_hashtable_op(modmap,kno_table_default,module_name,module);
  as_stored = kno_get((lispval)modmap,module_name,VOID);
  if (!(KNO_EQ(module,as_stored))) {
    kno_decref(module);
    return as_stored;}
  else kno_decref(as_stored);
  if (flags&KNO_MODULE_DEFAULT) {
    default_env->env_parent = kno_make_env(module,default_env->env_parent);}
  return module;}

KNO_EXPORT lispval kno_new_cmodule_x(char *name,int flags,void *addr,
                                     u8_string filename)
{
  lispval mod = kno_new_module(name,flags);
  int free_filename = 0;
  if (filename) {
    lispval fname = knostring(filename);
    if (free_filename) u8_free(filename);
    kno_add(mod,source_symbol,fname);
    kno_decref(fname);}
  return mod;
}

KNO_EXPORT lispval kno_get_module(lispval name)
{
  return kno_hashtable_get(&module_map,name,KNO_VOID);
}

KNO_EXPORT lispval kno_all_modules()
{
  return kno_hashtable_assocs(&module_map);
}

KNO_EXPORT int kno_discard_module(lispval name)
{
  int rv = kno_hashtable_drop(&module_map,name,VOID);
  return rv;
}

KNO_EXPORT
int kno_finish_module(lispval module)
{
  if (TABLEP(module))
    if (kno_test(module,loadstamp_symbol,KNO_VOID))
      return 0;
    else return kno_module_finished(module,0);
  else {
    kno_seterr(kno_NotAModule,"kno_finish_module",NULL,module);
    return -1;}
}

KNO_EXPORT
int kno_module_finished(lispval module,int flags)
{
  if (!(TABLEP(module))) {
    kno_seterr(kno_NotAModule,"kno_finish_module",NULL,module);
    return -1;}
  else {
    struct U8_XTIME xtptr; lispval timestamp;
    lispval cur_timestamp = kno_get(module,loadstamp_symbol,VOID);
    lispval moduleid = kno_get(module,moduleid_symbol,VOID);
    u8_init_xtime(&xtptr,-1,u8_second,0,0,0);
    timestamp = kno_make_timestamp(&xtptr);
    kno_store(module,loadstamp_symbol,timestamp);
    kno_decref(timestamp);
    if (VOIDP(moduleid))
      u8_log(LOG_WARN,_("Anonymous module"),
             "The module %q doesn't have an id",module);
    /* In this case, the module was already finished,
       so the loadlock would have been cleared. */
    else if (cur_timestamp) {}
    else clear_module_load_lock(moduleid);
    kno_decref(moduleid);
    kno_decref(cur_timestamp);
    return 1;}
}

/* Switching modules */

static kno_lexenv become_module(kno_lexenv env,lispval module_name,int create)
{
  lispval module_spec = kno_eval(module_name,env), module;
  if (ABORTP(module_spec))
    return NULL;
  else if (SYMBOLP(module_spec))
    module = kno_get_module(module_spec);
  else if ( (KNO_LEXENVP(module_spec)) || (KNO_HASHTABLEP(module_spec)) )
    module = kno_incref(module_spec);
  else {
    kno_decref(module_spec);
    return KNO_ERR(NULL,"InvalidModuleSpec","become_module",NULL,module_spec);}
  if (KNO_ABORTP(module)) return NULL;
  else if ( (!(create)) && (VOIDP(module)) ) {
    kno_seterr(kno_NoSuchModule,"become_module",
	       KNO_GETSTRING(module_spec),
	       module_spec);
    kno_decref(module);
    return NULL;}
  else if (HASHTABLEP(module)) {
    kno_decref(module);
    return KNO_ERR(NULL,OpaqueModule,"become_module",
		   KNO_GETSTRING(module_spec),module_spec);}
  else if (KNO_LEXENVP(module)) {
    KNO_LEXENV *menv=
      kno_consptr(KNO_LEXENV *,module,kno_lexenv_type);
    if (menv != env) {
      kno_decref(((lispval)(env->env_parent)));
      env->env_parent = (kno_lexenv)kno_incref((lispval)menv->env_parent);
      kno_decref(env->env_bindings);
      env->env_bindings = kno_incref(menv->env_bindings);
      kno_decref(env->env_exports);
      env->env_exports = kno_incref(menv->env_exports);}}
  else if (VOIDP(module)) {
    if (!(HASHTABLEP(env->env_exports)))
      env->env_exports = kno_make_hashtable(NULL,0);
    else if (KNO_HASHTABLE_READONLYP(env->env_exports)) {
      kno_seterr(_("Can't reload a read-only module"),
		 "become_module",NULL,module_spec);
      return NULL;}
    else {}
    kno_store(env->env_exports,moduleid_symbol,module_spec);
    kno_register_module(SYM_NAME(module_spec),(lispval)env,0);}
  else {
    kno_seterr(kno_NotAModule,"use_module",
	       KNO_GETSTRING(module_spec),module_spec);
    env = NULL;}
  kno_decref(module);
  kno_decref(module_spec);
  return env;
}

static lispval in_module_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval module_name = kno_get_arg(expr,1);
  if (VOIDP(module_name))
    return kno_err(kno_TooFewExpressions,"IN-MODULE",NULL,expr);
  else if (become_module(env,module_name,1)) return VOID;
  else return KNO_ERROR;
}

static lispval within_module_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval module_name = kno_get_arg(expr,1);
  if (VOIDP(module_name))
    return kno_err(kno_TooFewExpressions,"WITHIN-MODULE",NULL,expr);
  kno_lexenv consed_env = kno_working_lexenv();
  if (become_module(consed_env,module_name,0)) {
    lispval result = VOID, body = kno_get_body(expr,2);
    KNO_DOLIST(elt,body) {
      kno_decref(result); result = kno_eval(elt,consed_env);}
    kno_decref((lispval)consed_env);
    return result;}
  else {
    kno_decref((lispval)consed_env);
    return KNO_ERROR;}
}

static kno_lexenv make_hybrid_env(kno_lexenv base,lispval module_spec)
{
  lispval module=
    ((KNO_LEXENVP(module_spec)) ?
     (kno_incref(module_spec)) :
     (kno_get_module(module_spec)));
  if (KNO_ABORTP(module)) {
    kno_interr(module);
    return NULL;}
  else if (HASHTABLEP(module)) {
    kno_seterr(OpaqueModule,"IN-MODULE",NULL,module_spec);
    kno_decref(module);
    return NULL;}
  else if (KNO_LEXENVP(module)) {
    KNO_LEXENV *menv=
      kno_consptr(KNO_LEXENV *,module,kno_lexenv_type);
    kno_lexenv result = kno_make_env(kno_incref(base->env_bindings),menv);
    kno_decref(module);
    return result;}
  else if (VOIDP(module)) {
    kno_seterr(kno_NoSuchModule,"USING-MODULE",NULL,module_spec);
    return NULL;}
  else {
    kno_seterr(kno_TypeError,"USING-MODULE",NULL,module);
    return NULL;}
}

static lispval accessing_module_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval module_name = kno_eval(kno_get_arg(expr,1),env);
  kno_lexenv hybrid;
  if (VOIDP(module_name))
    return kno_err(kno_TooFewExpressions,"WITHIN-MODULE",NULL,expr);
  hybrid = make_hybrid_env(env,module_name);
  if (hybrid) {
    lispval result = VOID, body = kno_get_body(expr,2);
    KNO_DOLIST(elt,body) {
      if (KNO_ABORTP(result)) break;
      kno_decref(result);
      result = kno_eval(elt,hybrid);}
    kno_decref(module_name);
    kno_decref((lispval)hybrid);
    return result;}
  else {
    kno_decref(module_name);
    return KNO_ERROR;}
}

/* Exporting from modules */

static u8_mutex exports_lock;

static kno_hashtable get_exports(kno_lexenv env)
{
  kno_hashtable exports;
  lispval moduleid = kno_get(env->env_bindings,moduleid_symbol,VOID);
  u8_lock_mutex(&exports_lock);
  if (HASHTABLEP(env->env_exports)) {
    u8_unlock_mutex(&exports_lock);
    kno_decref(moduleid);
    return (kno_hashtable) env->env_exports;}
  exports = (kno_hashtable)(env->env_exports = kno_make_hashtable(NULL,16));
  if (!(VOIDP(moduleid)))
    kno_hashtable_store(exports,moduleid_symbol,moduleid);
  u8_unlock_mutex(&exports_lock);
  return exports;
}

static lispval module_export_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  kno_hashtable exports;
  lispval symbols_spec = kno_get_arg(expr,1), symbols;
  if (VOIDP(symbols_spec))
    return kno_err(kno_TooFewExpressions,"MODULE-EXPORT!",NULL,expr);
  symbols = kno_eval(symbols_spec,env);
  if (KNO_ABORTP(symbols)) return symbols;
  {DO_CHOICES(symbol,symbols)
      if (!(SYMBOLP(symbol))) {
        kno_decref(symbols);
        return kno_type_error(_("symbol"),"module_export",symbol);}}
  if (HASHTABLEP(env->env_exports))
    exports = (kno_hashtable)env->env_exports;
  else exports = get_exports(env);
  {DO_CHOICES(symbol,symbols) {
      lispval val = kno_get(env->env_bindings,symbol,VOID);
      kno_hashtable_store(exports,symbol,val);
      kno_decref(val);}}
  kno_decref(symbols);
  return VOID;
}

/* Using modules */

static int uses_bindings(kno_lexenv env,lispval bindings)
{
  kno_lexenv scan = env;
  while (scan)
    if (scan->env_bindings == bindings) return 1;
    else scan = scan->env_parent;
  return 0;
}

static lispval use_module_helper(lispval expr,kno_lexenv env)
{
  lispval module_names = kno_eval(kno_get_arg(expr,1),env);
  kno_lexenv modify_env = env;
  if (VOIDP(module_names))
    return kno_err(kno_TooFewExpressions,"USE-MODULE",NULL,expr);
  else {
    DO_CHOICES(module_name,module_names) {
      lispval module =
        (KNO_HASHTABLEP(module_name)) ? (kno_incref(module_name)) :
        (KNO_LEXENVP(module_name)) ? (kno_incref(module_name)) :
        (kno_find_module(module_name,1));
      if ( (KNO_ABORTP(module)) || (VOIDP(module)) ) {
        KNO_STOP_DO_CHOICES;
        kno_decref(module_names);
        if (VOIDP(module))
          return kno_err(kno_NoSuchModule,"USE-MODULE",NULL,module_name);
        else return module;}
      else if (HASHTABLEP(module)) {
        if (!(uses_bindings(env,module))) {
          kno_lexenv old_parent;
          /* Use a dynamic copy if the enviroment is static, so it
             gets freed. */
          if (env->env_copy != env) modify_env = kno_copy_env(env);
          old_parent = modify_env->env_parent;
          modify_env->env_parent = kno_make_export_env(module,old_parent);
          /* We decref this because 'env' is no longer pointing to it
             and kno_make_export_env incref'd it again. */
          if (old_parent) kno_decref((lispval)(old_parent));}}
      else {
        kno_lexenv expenv=
          kno_consptr(kno_lexenv,module,kno_lexenv_type);
        lispval expval = (lispval)get_exports(expenv);
        if (!(uses_bindings(env,expval))) {
          if (env->env_copy != env) modify_env = kno_copy_env(env);
          kno_lexenv old_parent = modify_env->env_parent;
          modify_env->env_parent = kno_make_export_env(expval,old_parent);
          /* We decref this because 'env' is no longer pointing to it
             and kno_make_export_env incref'd it again. */
          if (old_parent) kno_decref((lispval)(old_parent));}}
      kno_decref(module);}
    kno_decref(module_names);
    if (modify_env!=env) {kno_decref((lispval)modify_env);}
    return VOID;}
}

static lispval use_module_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return use_module_helper(expr,env);
}

static lispval export_alias_helper(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval modexpr = kno_get_arg(expr,1);
  if (KNO_VOIDP(modexpr))
    return kno_err(kno_SyntaxError,"export_alias_helper",NULL,expr);
  lispval old_exports = env->env_exports;
  lispval modname = kno_stack_eval(modexpr,env,_stack,0);
  if (KNO_ABORTP(modname)) return modname;
  lispval module = kno_find_module(modname,1);
  if (KNO_ABORTP(module)) return module;
  kno_hashtable exports =
    (KNO_HASHTABLEP(module)) ? ((kno_hashtable)module) :
    (KNO_LEXENVP(module)) ? (get_exports((kno_lexenv)module)) :
    (NULL);
  kno_incref((lispval)exports);
  env->env_exports=kno_incref((lispval)exports);
  kno_decref(modname);
  kno_decref(module);
  kno_decref(old_exports);
  return KNO_VOID;
}

static lispval export_alias_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return export_alias_helper(expr,env,_stack);
}

DEFPRIM1("get-module",get_module,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(GET-MODULE *arg0*)` **undocumented**",
         kno_any_type,KNO_VOID);
static lispval get_module(lispval modname)
{
  lispval module = kno_find_module(modname,0);
  return module;
}

DEFPRIM1("get-loaded-module",get_loaded_module,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(GET-LOADED-MODULE *arg0*)` **undocumented**",
         kno_any_type,KNO_VOID);
static lispval get_loaded_module(lispval modname)
{
  lispval module = kno_get_module(modname);
  if (VOIDP(module))
    return KNO_FALSE;
  else return module;
}

KNO_EXPORT
lispval kno_use_module(kno_lexenv env,lispval module)
{
  int free_module = 0;
  if (SYMBOLP(module)) {
    module = get_module(module);
    free_module = 1;}
  if (HASHTABLEP(module)) {
    if (!(uses_bindings(env,module))) {
      kno_lexenv oldp = env->env_parent;
      env->env_parent = kno_make_export_env(module,oldp);
      kno_decref((lispval)(oldp));}}
  else if (SLOTMAPP(module)) {
    if (!(uses_bindings(env,module))) {
      kno_lexenv oldp = env->env_parent;
      env->env_parent = kno_make_env(module,oldp);
      kno_incref(module);
      kno_decref((lispval)(oldp));}}
  else if (KNO_LEXENVP(module)) {
    kno_lexenv expenv=
      kno_consptr(kno_lexenv,module,kno_lexenv_type);
    lispval expval = (lispval)get_exports(expenv);
    if (!(uses_bindings(env,expval))) {
      kno_lexenv oldp = env->env_parent;
      env->env_parent = kno_make_export_env(expval,oldp);
      if (oldp) kno_decref((lispval)(oldp));}}
  else return kno_type_error("module","kno_use_module",module);
  if (free_module) kno_decref(module);
  return VOID;
}

DEFPRIM1("get-exports",get_exports_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(GET-EXPORTS *arg0*)` **undocumented**",
         kno_any_type,KNO_VOID);
static lispval get_exports_prim(lispval arg)
{
  lispval module = arg;
  if ((STRINGP(arg))||(SYMBOLP(arg)))
    module = kno_find_module(arg,0);
  else kno_incref(module);
  if (KNO_ABORTP(module)) return module;
  else if (VOIDP(module))
    return kno_err(kno_NoSuchModule,"USE-MODULE",NULL,arg);
  else if (HASHTABLEP(module)) {
    lispval keys = kno_getkeys(module);
    kno_decref(module);
    return keys;}
  else if (TYPEP(module,kno_lexenv_type)) {
    kno_lexenv expenv=
      kno_consptr(kno_lexenv,module,kno_lexenv_type);
    lispval expval = (lispval)get_exports(expenv);
    if (KNO_ABORTP(expval)) return expval;
    lispval keys = kno_getkeys(expval);
    kno_decref(module);
    return keys;}
  else return EMPTY;
}

static lispval get_source(lispval arg)
{
  lispval ids = KNO_EMPTY;
  if (KNO_VOIDP(arg)) {
    u8_string path = kno_sourcebase();
    if (path) return kno_mkstring(path);
    else return KNO_FALSE;}
  else if (KNO_LEXENVP(arg)) {
    kno_lexenv envptr = kno_consptr(kno_lexenv,arg,kno_lexenv_type);
    ids = kno_get(envptr->env_bindings,source_symbol,KNO_VOID);
    if (KNO_VOIDP(ids))
      ids = kno_get(envptr->env_bindings,moduleid_symbol,KNO_VOID);}
  else if (TABLEP(arg)) {
    ids = kno_get(arg,source_symbol,KNO_VOID);
    if (KNO_VOIDP(ids))
      ids = kno_get(arg,moduleid_symbol,KNO_VOID);}
  else if (KNO_SYMBOLP(arg)) {
    lispval mod = kno_find_module(arg,1);
    if (KNO_ABORTP(mod))
      return mod;
    lispval mod_source = get_source(mod);
    kno_decref(mod);
    return mod_source;}
  /* These aren't strictly modules, but they're nice to have here */
  else if (KNO_LAMBDAP(arg)) {
    struct KNO_LAMBDA *f = (kno_lambda) kno_fcnid_ref(arg);
    if (f->fcn_filename)
      return kno_make_string(NULL,-1,f->fcn_filename);
    else {
      lispval sourceinfo =
	kno_get(f->lambda_env->env_bindings,source_symbol,VOID);
      if (KNO_STRINGP(sourceinfo))
	return sourceinfo;
      else {
	kno_decref(sourceinfo);
	return KNO_FALSE;}}}
  else if (KNO_FUNCTIONP(arg)) {
    struct KNO_FUNCTION *f = (kno_function) kno_fcnid_ref(arg);
    if (f->fcn_filename)
      return kno_make_string(NULL,-1,f->fcn_filename);
    else return KNO_FALSE;}
  else if (TYPEP(arg,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = (kno_evalfn) kno_fcnid_ref(arg);
    if (sf->evalfn_filename)
      return kno_mkstring(sf->evalfn_filename);
    else return KNO_FALSE;}
  else if (KNO_TYPEP(arg,kno_macro_type)) {
    struct KNO_FUNCTION *f = (kno_function) kno_fcnid_ref(arg);
    if (f->fcn_filename)
      return kno_make_string(NULL,-1,f->fcn_filename);
    else return KNO_FALSE;}
  else return kno_type_error(_("module"),"module_bindings",arg);
  if (KNO_VOIDP(ids))
    return KNO_FALSE;
  else {
    KNO_DO_CHOICES(id,ids) {
      if (KNO_STRINGP(id)) {
        kno_incref(id);
        kno_decref(ids);
        KNO_STOP_DO_CHOICES;
        return id;}}
    return KNO_FALSE;}
}

DEFPRIM1("get-source",get_source_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
         "(get-source [*obj*])\n"
         "Gets the source file implementing *obj*, which "
         "can be a function (or macro or evalfn), module, "
         "or module name. With no arguments, returns the "
         "current SOURCEBASE",
         kno_any_type,KNO_VOID);
static lispval get_source_prim(lispval arg)
{
  return get_source(arg);
}

/* Modules to be used by kno_app_env */

static lispval config_used_modules(lispval var,void *data)
{
  lispval modlist=NIL;
  kno_lexenv scan=kno_app_env;
  while (scan) {
    if (scan->env_copy) scan=scan->env_copy;
    if (KNO_HASHTABLEP(scan->env_bindings)) {
      lispval bindings=kno_incref(scan->env_bindings);
      modlist=kno_init_pair(NULL,bindings,modlist);}
    scan=scan->env_parent;}
  return modlist;
}
static int config_use_module(lispval var,lispval val,void *data)
{
  lispval rv=kno_use_module(kno_app_env,val);
  if (KNO_ABORTP(rv))
    return -1;
  else {
    kno_decref(rv);
    return 1;}
}

/* Accessing module bindings */

static lispval
get_binding_helper(lispval modarg,lispval symbol,lispval dflt,
                   int export_only,int symeval,
                   u8_context caller)
{
  lispval module = (KNO_HASHTABLEP(modarg)) ? (modarg) :
    (KNO_LEXENVP(modarg)) ? (modarg) :
    (KNO_SYMBOLP(modarg)) ? (kno_find_module(modarg,0)) :
    (KNO_STRINGP(modarg)) ? (kno_find_module(modarg,0)) :
    (VOID);
  if (VOIDP(module)) {
    if (VOIDP(dflt))
      return kno_err(kno_NoSuchModule,caller,NULL,modarg);
    else return kno_incref(dflt);}
  else if (KNO_HASHTABLEP(module)) {
    lispval value = kno_hashtable_get((kno_hashtable)module,symbol,VOID);
    if (VOIDP(value)) {
      lispval retval=
        kno_err(kno_UnboundIdentifier,caller,SYMBOL_NAME(symbol),module);
      if (module == modarg) kno_decref(module);
      return retval;}
    else return kno_incref(dflt);}
  else if (KNO_LEXENVP(module)) {
    kno_lexenv env = (kno_lexenv) module;
    if (symeval) {
      lispval val = kno_symeval(symbol,env);
      if ( (KNO_VOIDP(val)) || (val == KNO_UNBOUND) ) {
        if (KNO_VOIDP(dflt))
          return kno_err(kno_UnboundIdentifier,caller,SYMBOL_NAME(symbol),module);
        else return kno_incref(dflt);}
      else return val;}
    else if (TABLEP(env->env_exports)) {
      lispval expval = kno_get(env->env_exports,symbol,VOID);
      if (!(VOIDP(expval))) {
        if (module != modarg) kno_decref(module);
        return expval;}}
    if (!export_only) {
      lispval inval= kno_get(env->env_bindings,symbol,VOID);
      if (!(VOIDP(inval))) {
        if (module != modarg) kno_decref(module);
        return inval;}}
    if (VOIDP(dflt)) {
      lispval retval=
        kno_err(kno_UnboundIdentifier,caller,SYMBOL_NAME(symbol),module);
      if (module == modarg) kno_decref(module);
      return retval;}
    else return kno_incref(dflt);}
  else if (VOIDP(dflt)) {
    lispval retval=
      kno_err(kno_NotAModule,caller,SYMBOL_NAME(symbol),module);
    if (module == modarg) kno_decref(module);
    return retval;}
  else {
    u8_log(LOG_WARN,"BadModule","From %q for %q: %q",
           modarg,symbol,module);
    if (module == modarg) kno_decref(module);
    return kno_incref(dflt);}
}

DEFPRIM3("get-binding",get_binding_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
         "(get-binding *module* *symbol* [*default*])\n"
         "Gets *module*'s exported binding of *symbol*. On "
         "failure, returns *default* if provided or errs if "
         "none is provided.",
         kno_any_type,KNO_VOID,kno_symbol_type,KNO_VOID,
         kno_any_type,KNO_VOID);
static lispval get_binding_prim
(lispval mod_arg,lispval symbol,lispval dflt)
{
  return get_binding_helper(mod_arg,symbol,dflt,1,0,"get_binding_prim");
}

DEFPRIM3("%get-binding",get_internal_binding_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
         "(get-binding *module* *symbol* [*default*])\n"
         "Gets *module*'s binding of *symbol* (internal or "
         "external). On failure returns *default* (if "
         "provided) or errs if none is provided.",
         kno_any_type,KNO_VOID,kno_symbol_type,KNO_VOID,
         kno_any_type,KNO_VOID);
static lispval get_internal_binding_prim
(lispval mod_arg,lispval symbol,lispval dflt)
{
  return get_binding_helper(mod_arg,symbol,dflt,
                            0,
                            0,
                            "get_internal_binding_prim");
}

DEFPRIM3("importvar",import_var_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
         "(importvar *module* *symbol* [*default*])\n"
         "Gets *module*'s binding of *symbol* (internal, "
         "external, or inherhited). On failure returns "
         "*default*, if provided, or errors (if not).",
         kno_any_type,KNO_VOID,kno_symbol_type,KNO_VOID,
         kno_any_type,KNO_VOID);
static lispval import_var_prim(lispval mod_arg,lispval symbol,lispval dflt)
{
  return get_binding_helper(mod_arg,symbol,dflt,0,1,"import_var_prim");
}

static volatile int module_tables_initialized = 0;

void kno_init_module_tables()
{
  if (module_tables_initialized) return;
  else module_tables_initialized=1;

  loadstamp_symbol = kno_intern("%loadstamp");
  moduleid_symbol = kno_intern("%moduleid");
  source_symbol = kno_intern("%source");
  dlsource_symbol = kno_intern("%dlsource");

  KNO_INIT_STATIC_CONS(&module_map,kno_hashtable_type);
  kno_make_hashtable(&module_map,67);

  lispval default_bindings = kno_make_hashtable(NULL,0);
  default_env = kno_make_env(default_bindings,kno_app_env);
  kno_store(default_bindings,kno_intern("%source"),KNO_FALSE);

  if (KNO_VOIDP(kno_scheme_module)) {
    kno_scheme_module = kno_make_hashtable(NULL,71);}
  kno_register_module("scheme",kno_scheme_module,(KNO_MODULE_DEFAULT));

  if (KNO_VOIDP(kno_io_module)) {
    kno_io_module = kno_make_hashtable(NULL,71);}
  kno_register_module("io",kno_io_module,(KNO_MODULE_DEFAULT));

  if (KNO_VOIDP(kno_sys_module)) {
    kno_sys_module = kno_make_hashtable(NULL,71);}
  kno_register_module("sys",kno_sys_module,(KNO_MODULE_DEFAULT));

  if (KNO_VOIDP(kno_db_module)) {
    kno_db_module = kno_make_hashtable(NULL,71);}
  kno_register_module("db",kno_db_module,(KNO_MODULE_DEFAULT));
}

/* Initialization */

KNO_EXPORT void kno_init_modules_c()
{
  u8_init_mutex(&exports_lock);

  link_local_cprims();

  kno_def_evalfn(kno_sys_module,"EXPORT-ALIAS!",export_alias_evalfn,
                 "Combine the exports of this module with another");

  kno_def_evalfn(kno_scheme_module,"IN-MODULE",in_module_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_sys_module,"WITHIN-MODULE",within_module_evalfn,
		 "*undocumented*");
  kno_defalias(kno_sys_module,"W/M","WITHIN-MODULE");
  kno_defalias(kno_sys_module,"%WM","WITHIN-MODULE");
  kno_def_evalfn(kno_sys_module,"ACCESSING-MODULE",accessing_module_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"USE-MODULE",use_module_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"MODULE-EXPORT!",module_export_evalfn,
		 "*undocumented*");

  kno_register_config("MODULE",
		      "Specify modules to be used by the default live environment",
		      config_used_modules,config_use_module,NULL);
}

static void link_local_cprims()
{
  KNO_LINK_PRIM("importvar",import_var_prim,3,kno_sys_module);
  KNO_LINK_PRIM("%get-binding",get_internal_binding_prim,3,kno_sys_module);
  KNO_LINK_PRIM("get-binding",get_binding_prim,3,kno_sys_module);
  KNO_LINK_PRIM("get-source",get_source_prim,1,kno_sys_module);
  KNO_LINK_PRIM("get-exports",get_exports_prim,1,kno_sys_module);
  KNO_LINK_PRIM("get-loaded-module",get_loaded_module,1,kno_sys_module);
  KNO_LINK_PRIM("get-module",get_module,1,kno_sys_module);

  KNO_LINK_ALIAS("%ls",get_exports_prim,kno_sys_module);
}
