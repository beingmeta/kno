/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_EVAL_INTERNALS 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "eval_internals.h"
#include "kno/storage.h"

#include <libu8/u8printf.h>

u8_condition kno_BindError=_("Can't bind variable");
u8_condition kno_BindSyntaxError=_("Bad binding expression");

/* Set operations */

DEFC_EVALFN("set!",assign_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(set! *var* *value*)` assigns the bound variable "
	    "*var* to be the result of evaluating *value*.")
static lispval assign_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  int retval;
  lispval var = kno_get_arg(expr,1), val_expr = kno_get_arg(expr,2), value;
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"SET!",NULL,expr);
  else if (!(SYMBOLP(var)))
    return kno_err(kno_NotAnIdentifier,"SET!",NULL,expr);
  else if (VOIDP(val_expr))
    return kno_err(kno_TooFewExpressions,"SET!",SYM_NAME(var),expr);
  value = kno_eval(val_expr,env,_stack);
  if (KNO_ABORTED(value)) return value;
  else if (KNO_BAD_ARGP(value))
    return kno_bad_arg(value,"assign_evalfn",val_expr);
  else if ((retval = (kno_assign_value(var,value,env)))) {
    kno_decref(value);
    if (RARELY(retval<0)) {
      /* TODO: Convert table errors to env errors */
      return KNO_ERROR;}
    else return VOID;}
  else if ((retval = (kno_bind_value(var,value,env)))) {
    kno_decref(value);
    if (RARELY(retval<0)) {
      /* TODO: Convert table errors to env errors */
      return KNO_ERROR;}
    else return VOID;}
  else return kno_err(kno_BindError,"SET!",SYM_NAME(var),var);
}

DEFC_EVALFN("set+!",assign_plus_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(set+! *var* *values*)` adds the result of evaluting "
	    "*values* to the set/choice stored in *var*.")
static lispval assign_plus_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1), val_expr = kno_get_arg(expr,2), value;
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"SET+!",NULL,expr);
  else if (!(SYMBOLP(var)))
    return kno_err(kno_NotAnIdentifier,"SET+!",NULL,expr);
  else if (VOIDP(val_expr))
    return kno_err(kno_TooFewExpressions,"SET+!",NULL,expr);
  value = kno_eval(val_expr,env,_stack);
  if (KNO_ABORTED(value)) return value;
  else if (KNO_BAD_ARGP(value))
    return kno_bad_arg(value,"assign_plus_evalfn",val_expr);
  else if (kno_add_value(var,value,env)>0) {}
  else if (kno_bind_value(var,value,env)>=0) {}
  else return KNO_ERROR;
  kno_decref(value);
  return VOID;
}

DEFC_EVALFN("local",local_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(local *var* *value* *type*)` assigns the variable *var* "
	    "in the current lexical environment to the result of evaluating "
	    "*value*. If *type* is provided, the value is checked against "
	    "*type*")
static lispval local_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  int retval;
  lispval args = KNO_CDR(expr);
  lispval var = pop_arg(args), val_expr = pop_arg(args), type = pop_arg(args);
  lispval value = KNO_VOID;
  lispval bindings = env->env_bindings;
  struct KNO_SCHEMAP *schemap =
    (USUALLY(KNO_TYPEP(bindings,kno_schemap_type))) ?
    ((kno_schemap)env) : (NULL);
  if (schemap == NULL)
    return kno_err(kno_SyntaxError,"local","not in lexical context",expr);
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"LOCAL",NULL,expr);
  else if (!(SYMBOLP(var)))
    return kno_err(kno_NotAnIdentifier,"LOCAL",NULL,expr);
  else if (VOIDP(val_expr))
    return kno_err(kno_TooFewExpressions,"LOCAL",SYM_NAME(var),expr);
  else NO_ELSE;
  int off = -1, len = schemap->schema_length;
  lispval *schema = schemap->table_schema;
  lispval *scan = schema, *limit=schema+len;
  while (scan<limit) {
    if (*scan==var) {
      off=scan-schema;
      break;}
    else scan++;}
  if (RARELY(off<0)) return kno_err("NotLocal","local",NULL,var);

  lispval *vals = schemap->table_values;
  if (RARELY(( (env->env_copy) && (env->env_copy!=env) ))) {
    kno_lexenv copy = env->env_copy;
    if (USUALLY (KNO_SCHEMAPP(copy->env_bindings)) ) {
      struct KNO_SCHEMAP *alt = (kno_schemap)copy->env_bindings;
      if (alt->table_schema == schemap->table_schema)
	vals = alt->table_values;
      else vals = NULL;}
    else vals = NULL;}
  if (vals==NULL) return KNO_ERROR;
  value = kno_eval(val_expr,env,_stack);
  if (KNO_ABORTED(value)) return value;
  else if (KNO_BAD_ARGP(value))
    return kno_bad_arg(value,"assign_evalfn",val_expr);
  else if ( (KNO_VOIDP(type)) || (KNO_FALSEP(type)) ) {}
  else if (KNO_CHECKTYPE(value,type)) {}
  else {
    kno_seterr(kno_TypeError,"local",KNO_SYMBOL_NAME(var),value);
    kno_decref(value);
    return KNO_ERROR;}
  lispval oldval = vals[off];
  vals[off] = value;
  kno_decref(oldval);
  return KNO_VOID;
}

static int check_defaultp(lispval val,lispval replace_values)
{
  if (VOIDP(replace_values)) return 0;
  else if (val == replace_values) return 1;
  else if (CHOICEP(replace_values)) {
    DO_CHOICES(rv,replace_values) {
      if (val == rv) return 1;}
    return 0;}
  else return 0;
}

DEFC_EVALFN("DEFAULT!",assign_default_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(default! *var* *value* [*replace*])` sets the "
	    "bound value of *var* to *value* if *var* does not "
	    "currently have a value or if the value is in any of "
	    "*replace*. *replace* is not evaluated and comparision "
	    "is based on strict object equality. "
	    "Note that *value* will not be evaluated when *var* "
	    "doesn't need to be set.")
static lispval assign_default_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval symbol = kno_get_arg(expr,1);
  lispval value_expr = kno_get_arg(expr,2);
  lispval replace_vals = kno_get_arg(expr,3);
  if (!(SYMBOLP(symbol)))
    return kno_err(kno_SyntaxError,"assign_default_evalfn",NULL,expr);
  else if (VOIDP(value_expr))
    return kno_err(kno_SyntaxError,"assign_default_evalfn",NULL,expr);
  else {
    lispval val = kno_symeval(symbol,env);
    if ( (VOIDP(val)) || (val == KNO_UNBOUND) || (val == KNO_DEFAULT_VALUE) ||
	 (check_defaultp(val,replace_vals) ) ) {
      lispval value = kno_eval(value_expr,env,_stack);
      if (KNO_ABORTED(value)) return value;
      else if (KNO_BAD_ARGP(value))
	return kno_bad_arg(value,"assign_default_evalfn",value_expr);
      else NO_ELSE;
      /* Try to assign/bind it, checking for error return values */
      int rv = kno_assign_value(symbol,value,env);
      if (rv==0)
	rv=kno_bind_value(symbol,value,env);
      kno_decref(value);
      if (rv<0)
	return KNO_ERROR;
      else return KNO_VOID;}
    else {
      kno_decref(val);
      return VOID;}}
}

/* Simple binders */

DEFC_EVALFN("let",let_evalfn,KNO_EVALFN_DEFAULTS,
	    "*undocumented*")
static lispval let_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval bindexprs = kno_get_arg(expr,1), result = VOID;
  int n, tail = KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS);
  if (RARELY(! ( (bindexprs == KNO_NIL) || (PAIRP(bindexprs)) ) ))
    return kno_err(kno_BindSyntaxError,"LET",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    PUSH_STACK_ENV(letenv,env,n,_stack);
    int i = 0;
    {KNO_DOBINDINGS(var,val_expr,bindexprs) {
	lispval value = kno_eval(val_expr,env,letenv_stack);
	if (KNO_ABORTED(value)) {
	  result = value; goto pop_stack;}
	else if (KNO_BAD_ARGP(value)) {
	  result = kno_bad_arg(value,"let_evalfn",val_expr);
	  goto pop_stack;}
	else {
	  letenv_vars[i]=var;
	  letenv_vals[i]=value;
	  i++;}}}
    result = eval_body(kno_get_body(expr,2),letenv,letenv_stack,
		       "LET",SYM_NAME(letenv_vars[0]),tail);
  pop_stack:
    kno_pop_stack(letenv_stack);
    return result;}
}

DEFC_EVALFN("let*",letstar_evalfn,KNO_EVALFN_DEFAULTS,
	    "*undocumented*")
static lispval letstar_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval bindexprs = kno_get_arg(expr,1), result = VOID;
  int n, tail = KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS);
  if (RARELY(! ( (bindexprs == KNO_NIL) || (PAIRP(bindexprs)) ) ))
    return kno_err(kno_BindSyntaxError,"LET*",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    PUSH_STACK_ENV(letstar,env,n,_stack);
    int i = 0, j = 0;
    {KNO_DOBINDINGS(var,val_expr,bindexprs) {
	letstar_vars[j]=var;
	letstar_vals[j]=KNO_UNBOUND;
	j++;}}
    {KNO_DOBINDINGS(var,val_expr,bindexprs) {
	lispval value = kno_eval(val_expr,letstar,letstar_stack);
	if (KNO_ABORTED(value)) {
	  result = value; goto pop_stack;}
	else if (KNO_BAD_ARGP(value)) {
	  result = kno_bad_arg(value,"letstar_evalfn",val_expr);
	  goto pop_stack;}
	else if (letstar->env_copy) {
	  kno_bind_value(var,value,letstar->env_copy);
	  kno_decref(value);}
	else {
	  letstar_vars[i]=var;
	  letstar_vals[i]=value;}
	i++;}}
    result = eval_body(kno_get_body(expr,2),letstar,letstar_stack,
		       "LET*",SYM_NAME(letstar_vars[0]),tail);
  pop_stack:
    kno_pop_stack(letstar_stack);
    return result;}
}

/* LETREC */

DEFC_EVALFN("letrec",letrec_evalfn,KNO_EVALFN_DEFAULTS,
	    "*undocumented*")
static lispval letrec_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval bindexprs = kno_get_arg(expr,1), result = VOID;
  int n, tail = KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS);
  if (RARELY(! ( (bindexprs == KNO_NIL) || (PAIRP(bindexprs)) ) ))
    return kno_err(kno_BindSyntaxError,"LETREC",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    PUSH_STACK_ENV(letrec,env,n,_stack);
    int i = 0, j = 0;
    {KNO_DOBINDINGS(var,val_expr,bindexprs) {
	letrec_vars[j]=var;
	letrec_vals[j]=KNO_UNBOUND;
	j++;}}
    {KNO_DOBINDINGS(var,val_expr,bindexprs) {
	lispval value = kno_eval(val_expr,letrec,letrec_stack);
	if (KNO_ABORTED(value)) {
	  result = value; goto pop_stack;}
	else if (KNO_BAD_ARGP(value)) {
	  result = kno_bad_arg(value,"letrec_evalfn",val_expr);
	  goto pop_stack;}
	else if (letrec->env_copy) {
	  kno_bind_value(var,value,letrec->env_copy);
	  kno_decref(value);}
	else {
	  letrec_vars[i]=var;
	  letrec_vals[i]=value;}
	i++;}}
    result = eval_body(kno_get_body(expr,2),letrec,letrec_stack,
		       "LETREC",SYM_NAME(letrec_vars[0]),tail);
  pop_stack:
    kno_pop_stack(letrec_stack);
    return result;}
}

/* DO */

DEFC_EVALFN("do",do_evalfn,KNO_EVALFN_DEFAULTS,
	    "*undocumented*")
static lispval do_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  int n = -1, tail = KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS);
  lispval doloop_result = VOID;
  lispval bindexprs = kno_get_arg(expr,1);
  lispval exitexprs = kno_get_arg(expr,2);
  lispval testexpr = kno_get_arg(exitexprs,0), testval = VOID;
  lispval body = kno_get_body(expr,3);
  if (RARELY(! ( (bindexprs == KNO_NIL) || (PAIRP(bindexprs)) ) ))
    return kno_err(kno_BindSyntaxError,"DO",NULL,expr);
  if (RARELY(! ( (body == KNO_NIL) || (PAIRP(body)) ) ))
    return kno_err(kno_BindSyntaxError,"DO",NULL,expr);
  else if (!(PAIRP(exitexprs)))
    return kno_err(kno_BindSyntaxError,"DO",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&doloop_result))<0)
    return doloop_result;
  else {
    PUSH_STACK_ENV(doloop,env,n,_stack);
    kno_lexenv use_env = _stack->eval_env;
    _stack->eval_env = NULL;
    lispval updaters[n], tmp[n];
    int i = 0;
    /* Do the initial bindings */
    {KNO_DOLIST(bindexpr,bindexprs) {
      lispval var = kno_get_arg(bindexpr,0);
      lispval value_expr = kno_get_arg(bindexpr,1);
      if ( (!(SYMBOLP(var))) || (KNO_VOIDP(value_expr)) ) {
	doloop_result = kno_err(kno_SyntaxError,"do_evalfn",NULL,expr);
	goto pop_stack;}
      lispval update_expr = kno_get_arg(bindexpr,2);
      lispval value = kno_eval(value_expr,env,doloop_stack);
      if (KNO_ABORTED(value)) {
	doloop_result = value; goto pop_stack;}
      else if (KNO_BAD_ARGP(value)) {
	doloop_result = kno_bad_arg(value,"do_evalfn",value_expr);
	goto pop_stack;}
      else {
	doloop_vars[i]=var;
	doloop_vals[i]=value;
	updaters[i]=update_expr;
	tmp[i]=KNO_VOID;
	i++;}}}
    _stack->eval_env = use_env;
    /* First test */
    testval = kno_eval(testexpr,doloop,doloop_stack);
    if (KNO_ABORTED(testval)) {
      doloop_result = testval; goto pop_stack;}
    /* The iteration itself */
    while (FALSEP(testval)) {
      int i = 0;
      /* Execute the body */
      KNO_DOLIST(bodyexpr,body) {
	lispval result = kno_eval(bodyexpr,doloop,doloop_stack);
	if (KNO_ABORTED(result)) {
	  doloop_result = result; goto pop_stack;}
	else kno_decref(result);}
      /* Do an update, storing new values in tmp[] to be consistent. */
      while (i < n) {
	lispval new_val = (VOIDP(updaters[i])) ? (kno_incref(doloop_vals[i])) :
	  (kno_eval(updaters[i],doloop,doloop_stack));
	if (KNO_ABORTED(new_val)) {
	  /* GC the updated values you've generated so far.
	     Note that tmp[i] (the exception) is not freed. */
	  kno_decref_vec(tmp,i);
	  doloop_result = new_val; goto pop_stack;}
	else tmp[i++] = new_val;}
      /* Now, free the current values and replace them with the values
	 from tmp[]. */
      i = 0; while (i < n) {
	lispval val = doloop_vals[i];
	if ((CONSP(val))&&(KNO_MALLOCD_CONSP((kno_cons)val))) {
	  kno_decref(val);}
	doloop_vals[i] = tmp[i];
	i++;}
      /* Decref/recycle any dynamic copis of the environment */
      reset_env(doloop);
      /* Free the old testval and evaluate it again. */
      kno_decref(testval);
      testval = kno_eval(testexpr,doloop,doloop_stack);
      if (KNO_ABORTED(testval)) {
	doloop_result = testval; goto pop_stack;}}
    /* Now we're done, so we set result to testval. */
    doloop_result = testval;
    if (KNO_EMPTYP(testval)) {}
    else if (!(KNO_EMPTY_LISTP(KNO_CDR(exitexprs)))) {
      kno_decref(doloop_result);
      doloop_result = eval_body(kno_get_body(exitexprs,1),doloop,doloop_stack,
				"DO",SYM_NAME(doloop_vars[0]),tail);}
    else NO_ELSE;
  pop_stack:
    kno_pop_stack(doloop_stack);
    /* Free the environment. */
    return doloop_result;}
}

/* DEFINE-LOCAL */

/* This defines an identifier in the local environment to
   the value it would have anyway by environment inheritance.
   This is helpful if it was to rexport it, for example. */
DEFC_EVALFN("define-local",define_local_evalfn,KNO_EVALFN_DEFAULTS,
	    "*undocumented*")
static lispval define_local_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1);
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"DEFINE-LOCAL",NULL,expr);
  else if (SYMBOLP(var)) {
    lispval inherited = kno_symeval(var,env->env_parent);
    if (KNO_ABORTED(inherited)) return inherited;
    else if (VOIDP(inherited))
      return kno_err(kno_UnboundIdentifier,"DEFINE-LOCAL",
		     SYM_NAME(var),var);
    else if (kno_bind_value(var,inherited,env)<0)  {
      kno_decref(inherited);
      return KNO_ERROR;}
    else {
      kno_decref(inherited);
      return VOID;}}
  else return kno_err(kno_NotAnIdentifier,"DEFINE-LOCAL",NULL,var);
}

/* DEFINE-INIT */

/* This defines an identifier in the local environment only if
   it is not currently defined. */
DEFC_EVALFN("define-init",define_init_evalfn,KNO_EVALFN_DEFAULTS,
	    "*undocumented*")
static lispval define_init_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1);
  lispval init_expr = kno_get_arg(expr,2);
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"DEFINE-LOCAL",NULL,expr);
  else if (VOIDP(init_expr))
    return kno_err(kno_TooFewExpressions,"DEFINE-LOCAL",NULL,expr);
  else if (SYMBOLP(var)) {
    lispval current = kno_get(env->env_bindings,var,VOID);
    if (KNO_ABORTED(current)) return current;
    else if (!(VOIDP(current))) {
      kno_decref(current);
      return VOID;}
    else {
      int bound = 0;
      lispval init_value = kno_eval(init_expr,env,_stack);
      if (KNO_ABORTED(init_value)) return init_value;
      else bound = kno_bind_value(var,init_value,env);
      if (bound>0) {
	kno_decref(init_value);
	return VOID;}
      else return KNO_ERROR;}}
  else return kno_err(kno_NotAnIdentifier,"DEFINE_INIT",NULL,var);
}

/* This defines an identifier in the local environment to
   the value it would have anyway by environment inheritance.
   This is helpful if it was to rexport it, for example. */
DEFC_EVALFN("def+",define_return_evalfn,KNO_EVALFN_DEFAULTS,
	    "*undocumented*")
static lispval define_return_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1), val_expr = kno_get_arg(expr,2);
  if ( (VOIDP(var)) || (VOIDP(val_expr)) )
    return kno_err(kno_TooFewExpressions,"DEF+",NULL,expr);
  else if (SYMBOLP(var)) {
    lispval value = kno_eval(val_expr,env,_stack);
    if (KNO_ABORTED(value))
      return value;
    else if (kno_bind_value(var,value,env)<0) {
      kno_decref(value);
      return KNO_ERROR;}
    else return value;}
  else return kno_err(kno_NotAnIdentifier,"DEF+",NULL,var);
}

/* DEFINE-INIT */

/* This defines an identifier in the local environment only if
   it is not currently defined. */

DEFC_EVALFN("DEFIMPORT",defimport_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(DEFIMPORT *name* '*module* [*origin*]) defines "
	    "a local binding *name* for the value of *origin* "
	    "in the module name *module*. If *origin* is not provided, "
	    "*name* is used as *origin*.")
static lispval defimport_evalfn(lispval expr,kno_lexenv env,
				kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1);
  lispval module_expr = kno_get_arg(expr,2);
  lispval import_name = kno_get_arg(expr,3);
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"DEFIMPORT",NULL,expr);
  else if (VOIDP(module_expr))
    return kno_err(kno_TooFewExpressions,"DEFIMPORT",NULL,expr);
  else if (!(KNO_SYMBOLP(var)))
    return kno_err(kno_SyntaxError,"DEFIMPORT/VAR",NULL,expr);

  if (KNO_VOIDP(import_name)) import_name = var;

  int decref_module = 0;
  lispval module_spec = kno_eval(module_expr,env,_stack);
  if (KNO_ABORTP(module_spec)) return module_spec;

  lispval module = module_spec;
  if (! ( (KNO_HASHTABLEP(module_spec)) || (KNO_LEXENVP(module_spec)) ) ) {
    module = kno_find_module(module_spec,0);
    decref_module=1;}

  lispval result = KNO_VOID;
  if (KNO_ABORTP(module)) return module;
  else if (KNO_TABLEP(module)) {
    lispval import_value = kno_get(module,import_name,KNO_VOID);
    if (KNO_ABORTP(import_value))
      result = import_value;
    else if (KNO_VOIDP(import_value)) {
      result = kno_err("UndefinedImport","DEFIMPORT",
		       KNO_SYMBOL_NAME(import_name),
		       module);}
    else if (kno_bind_value(var,import_value,env) < 0)
      result = KNO_ERROR;
    else NO_ELSE;
    if (decref_module) kno_decref(module);
    kno_decref(module_spec);
    kno_decref(import_value);
    return result;}
  else {
    kno_decref(module_spec);
    return kno_err("NotAModule","DEFIMPORT",NULL,module);}
}

/* Initialization */

KNO_EXPORT void kno_init_binders_c()
{
  u8_register_source_file(_FILEINFO);

  KNO_LINK_EVALFN(kno_scheme_module,assign_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,assign_plus_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,assign_default_evalfn);

  KNO_LINK_EVALFN(kno_scheme_module,local_evalfn);

  KNO_LINK_EVALFN(kno_scheme_module,let_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,letstar_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,letrec_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,do_evalfn);

  KNO_LINK_EVALFN(kno_scheme_module,define_init_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,define_return_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,define_local_evalfn);

  KNO_LINK_EVALFN(kno_scheme_module,defimport_evalfn);
}

