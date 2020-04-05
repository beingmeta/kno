/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
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

#define _env_return \
  return release_stack_env(_stack),

/* Set operations */

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
  value = kno_eval(val_expr,env,_stack,0);
  if (KNO_ABORTED(value)) return value;
  else if ((retval = (kno_assign_value(var,value,env)))) {
    kno_decref(value);
    if (PRED_FALSE(retval<0)) {
      /* TODO: Convert table errors to env errors */
      return KNO_ERROR;}
    else return VOID;}
  else if ((retval = (kno_bind_value(var,value,env)))) {
    kno_decref(value);
    if (PRED_FALSE(retval<0)) {
      /* TODO: Convert table errors to env errors */
      return KNO_ERROR;}
    else return VOID;}
  else return kno_err(kno_BindError,"SET!",SYM_NAME(var),var);
}

static lispval assign_plus_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1), val_expr = kno_get_arg(expr,2), value;
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"SET+!",NULL,expr);
  else if (!(SYMBOLP(var)))
    return kno_err(kno_NotAnIdentifier,"SET+!",NULL,expr);
  else if (VOIDP(val_expr))
    return kno_err(kno_TooFewExpressions,"SET+!",NULL,expr);
  value = kno_eval(val_expr,env,_stack,0);
  if (KNO_ABORTED(value)) return value;
  else if (kno_add_value(var,value,env)>0) {}
  else if (kno_bind_value(var,value,env)>=0) {}
  else return KNO_ERROR;
  kno_decref(value);
  return VOID;
}

static lispval assign_default_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval symbol = kno_get_arg(expr,1);
  lispval value_expr = kno_get_arg(expr,2);
  if (!(SYMBOLP(symbol)))
    return kno_err(kno_SyntaxError,"assign_default_evalfn",NULL,kno_incref(expr));
  else if (VOIDP(value_expr))
    return kno_err(kno_SyntaxError,"assign_default_evalfn",NULL,kno_incref(expr));
  else {
    lispval val = kno_symeval(symbol,env);
    if ((VOIDP(val))||(val == KNO_UNBOUND)||(val == KNO_DEFAULT_VALUE)) {
      lispval value = kno_eval(value_expr,env,_stack,0);
      if (KNO_ABORTED(value))
	return value;
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

static lispval assign_false_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval symbol = kno_get_arg(expr,1);
  lispval value_expr = kno_get_arg(expr,2);
  if (!(SYMBOLP(symbol)))
    return kno_err(kno_SyntaxError,"assign_false_evalfn",NULL,kno_incref(expr));
  else if (VOIDP(value_expr))
    return kno_err(kno_SyntaxError,"assign_false_evalfn",NULL,kno_incref(expr));
  else {
    lispval val = kno_symeval(symbol,env);
    if ((VOIDP(val))||(FALSEP(val))||
	(val == KNO_UNBOUND)||(val == KNO_DEFAULT_VALUE)) {
      lispval value = kno_eval(value_expr,env,_stack,0);
      if (KNO_ABORTED(value))
	return value;
      /* Try to assign/bind it, checking for error return values */
      int rv = kno_assign_value(symbol,value,env);
      if (rv == 0)
	rv = kno_bind_value(symbol,value,env);
      kno_decref(value);
      if (rv<0)
	return KNO_ERROR;
      else return VOID;}
    else {
      kno_decref(val);
      return VOID;}}
}

static lispval bind_default_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval symbol = kno_get_arg(expr,1);
  lispval value_expr = kno_get_arg(expr,2);
  if (!(SYMBOLP(symbol)))
    return kno_err(kno_SyntaxError,"bind_default_evalfn",NULL,expr);
  else if (VOIDP(value_expr))
    return kno_err(kno_SyntaxError,"bind_default_evalfn",NULL,expr);
  else if (env == NULL)
    return kno_err(kno_SyntaxError,"bind_default_evalfn",NULL,expr);
  else {
    lispval val = kno_get(env->env_bindings,symbol,VOID);
    if ((VOIDP(val))||(val == KNO_UNBOUND)||(val == KNO_DEFAULT_VALUE)) {
      lispval value = kno_eval(value_expr,env,_stack,0);
      if (KNO_ABORTED(value))
	return value;
      int rv = kno_bind_value(symbol,value,env);
      kno_decref(value);
      if (rv<0)
	return KNO_ERROR;
      else return VOID;}
    else {
      kno_decref(val); return VOID;}}
}

/* Simple binders */

static lispval let_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval bindexprs = kno_get_arg(expr,1), result = VOID;
  int n, tail = KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS);
  if (PRED_FALSE(! ( (bindexprs == KNO_NIL) || (PAIRP(bindexprs)) ) ))
    return kno_err(kno_BindSyntaxError,"LET",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    PUSH_STACK_ENV(letenv,env,n,_stack);
    int i = 0;
    {KNO_DOBINDINGS(var,val_expr,bindexprs) {
	lispval value = kno_eval(val_expr,env,letenv_stack,0);
	if (KNO_ABORTED(value)) {
	  result = value; goto pop_stack;}
	else if (VOIDP(value)) {
	  result = kno_err(kno_VoidBinding,"let_evalfn",
			   KNO_SYMBOL_NAME(var),val_expr);
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

static lispval letstar_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval bindexprs = kno_get_arg(expr,1), result = VOID;
  int n, tail = KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS);
  if (PRED_FALSE(! ( (bindexprs == KNO_NIL) || (PAIRP(bindexprs)) ) ))
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
	lispval value = kno_eval(val_expr,letstar,letstar_stack,0);
	if (KNO_ABORTED(value)) {
	  result = value; goto pop_stack;}
	else if (VOIDP(value)) {
	  result = kno_err(kno_VoidBinding,"letstar_evalfn",
			   KNO_SYMBOL_NAME(var),val_expr);
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

static lispval letrec_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval bindexprs = kno_get_arg(expr,1), result = VOID;
  int n, tail = KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS);
  if (PRED_FALSE(! ( (bindexprs == KNO_NIL) || (PAIRP(bindexprs)) ) ))
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
	lispval value = kno_eval(val_expr,letrec,letrec_stack,0);
	if (KNO_ABORTED(value)) {
	  result = value; goto pop_stack;}
	else if (VOIDP(value)) {
	  result = kno_err(kno_VoidBinding,"letstar_evalfn",
			   KNO_SYMBOL_NAME(var),val_expr);
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

static lispval do_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  int n = -1, tail = KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS);
  lispval doloop_result = VOID;
  lispval bindexprs = kno_get_arg(expr,1);
  lispval exitexprs = kno_get_arg(expr,2);
  lispval testexpr = kno_get_arg(exitexprs,0), testval = VOID;
  lispval body = kno_get_body(expr,3);
  if (PRED_FALSE(! ( (bindexprs == KNO_NIL) || (PAIRP(bindexprs)) ) ))
    return kno_err(kno_BindSyntaxError,"DO",NULL,expr);
  if (PRED_FALSE(! ( (body == KNO_NIL) || (PAIRP(body)) ) ))
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
      lispval value = kno_eval(value_expr,env,doloop_stack,0);
      if (KNO_ABORTED(value)) {
	doloop_result = value; goto pop_stack;}
      else {
	doloop_vars[i]=var;
	doloop_vals[i]=value;
	updaters[i]=update_expr;
	tmp[i]=KNO_VOID;
	i++;}}}
    _stack->eval_env = use_env;
    /* First test */
    testval = kno_eval(testexpr,doloop,doloop_stack,0);
    if (KNO_ABORTED(testval)) {
      doloop_result = testval; goto pop_stack;}
    /* The iteration itself */
    while (FALSEP(testval)) {
      int i = 0;
      /* Execute the body */
      KNO_DOLIST(bodyexpr,body) {
	lispval result = kno_eval(bodyexpr,doloop,doloop_stack,0);
	if (KNO_ABORTED(result)) {
	  doloop_result = result; goto pop_stack;}
	else kno_decref(result);}
      /* Do an update, storing new values in tmp[] to be consistent. */
      while (i < n) {
	lispval new_val = (VOIDP(updaters[i])) ? (kno_incref(doloop_vals[i])) :
	  (kno_eval(updaters[i],doloop,doloop_stack,0));
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
      testval = kno_eval(testexpr,doloop,doloop_stack,0);
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
      lispval init_value = kno_eval(init_expr,env,_stack,0);
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
static lispval define_return_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1), val_expr = kno_get_arg(expr,2);
  if ( (VOIDP(var)) || (VOIDP(val_expr)) )
    return kno_err(kno_TooFewExpressions,"DEF+",NULL,expr);
  else if (SYMBOLP(var)) {
    lispval value = kno_eval(val_expr,env,_stack,0);
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
static lispval define_import_evalfn(lispval expr,kno_lexenv env,
				    kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1);
  lispval module_expr = kno_get_arg(expr,2);
  lispval import_name = kno_get_arg(expr,3);
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"DEFINE-IMPORT",NULL,expr);
  else if (VOIDP(module_expr))
    return kno_err(kno_TooFewExpressions,"DEFINE-IMPORT",NULL,expr);
  else if (!(KNO_SYMBOLP(var)))
    return kno_err(kno_SyntaxError,"DEFINE-IMPORT/VAR",NULL,expr);

  if (KNO_VOIDP(import_name)) import_name = var;

  int decref_module = 0;
  lispval module_spec = kno_eval(module_expr,env,_stack,0);
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
      result = kno_err("UndefinedImport","define_import",
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
    return kno_err("NotAModule","define_import_evalfn",NULL,module);}
}

static lispval define_import(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return define_import_evalfn(expr,env,_stack);
}

/* Initialization */

KNO_EXPORT void kno_init_binders_c()
{
  u8_register_source_file(_FILEINFO);

  kno_def_evalfn(kno_scheme_module,"SET!",assign_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"SET+!",assign_plus_evalfn,
		 "*undocumented*");

  kno_def_evalfn(kno_scheme_module,"LET",let_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"LET*",letstar_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"LETREC",letrec_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"DEFINE-INIT",define_init_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"DEFINE-LOCAL",define_local_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"DEF+",define_return_evalfn,
		 "*undocumented*");

  kno_def_evalfn(kno_scheme_module,"DO",do_evalfn,
		 "*undocumented*");

  kno_def_evalfn(kno_scheme_module,"DEFAULT!",assign_default_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"SETFALSE!",assign_false_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"BIND-DEFAULT!",bind_default_evalfn,
		 "*undocumented*");

  kno_def_evalfn(kno_scheme_module,"DEFINE-IMPORT",define_import,
		 "`(DEFINE-IMPORT *name* *module* [*defname*])` defines "
		 "a local binding *name* for the value of *defname * "
		 "in *module*. If not provided, *name* is used as *defname*.");
  kno_defalias(kno_scheme_module,"DEFIMPORT","DEFINE-IMPORT");

}

