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
#include "eval_internals.h"
#include "kno/storage.h"

#include <libu8/u8printf.h>

u8_condition kno_BindError=_("Can't bind variable");
u8_condition kno_BindSyntaxError=_("Bad binding expression");

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
  value = fast_eval(val_expr,env);
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
  value = fast_eval(val_expr,env);
  if (KNO_ABORTED(value)) return value;
  else if (kno_add_value(var,value,env)>0) {}
  else if (kno_bind_value(var,value,env)>=0) {}
  else return kno_err(kno_BindError,"SET+!",SYM_NAME(var),var);
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
      lispval value = kno_eval(value_expr,env);
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
      lispval value = kno_eval(value_expr,env);
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
    return kno_err(kno_SyntaxError,"bind_default_evalfn",NULL,kno_incref(expr));
  else if (VOIDP(value_expr))
    return kno_err(kno_SyntaxError,"bind_default_evalfn",NULL,kno_incref(expr));
  else if (env == NULL)
    return kno_err(kno_SyntaxError,"bind_default_evalfn",NULL,kno_incref(expr));
  else {
    lispval val = kno_get(env->env_bindings,symbol,VOID);
    if ((VOIDP(val))||(val == KNO_UNBOUND)||(val == KNO_DEFAULT_VALUE)) {
      lispval value = kno_eval(value_expr,env);
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
  int n;
  if (PRED_FALSE(! ( (bindexprs == KNO_NIL) || (PAIRP(bindexprs)) ) ))
    return kno_err(kno_BindSyntaxError,"LET",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    INIT_STACK_ENV(_stack,letenv,env,n);
    int i = 0;
    {KNO_DOBINDINGS(var,val_expr,bindexprs) {
        lispval value = fast_eval(val_expr,env);
        if (KNO_ABORTED(value)) {
          _return value;}
        else if (VOIDP(value)) {
          kno_seterr(kno_VoidBinding,"let_evalfn",
                    KNO_SYMBOL_NAME(var),val_expr);
          return KNO_ERROR_VALUE;}
        else {
          letenv_vars[i]=var;
          letenv_vals[i]=value;
          i++;}}}
    result = eval_inner_body
      (":LET",SYM_NAME(letenv_vars[0]),expr,2,letenv,_stack);
    _return result;}
}

static lispval letstar_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval bindexprs = kno_get_arg(expr,1), result = VOID;
  int n;
  if (PRED_FALSE(! ( (bindexprs == KNO_NIL) || (PAIRP(bindexprs)) ) ))
    return kno_err(kno_BindSyntaxError,"LET*",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    INIT_STACK_ENV(_stack,letseq,env,n);
    int i = 0, j = 0;
    {KNO_DOBINDINGS(var,val_expr,bindexprs) {
        letseq_vars[j]=var;
        letseq_vals[j]=KNO_UNBOUND;
        j++;}}
    {KNO_DOBINDINGS(var,val_expr,bindexprs) {
        lispval value = fast_eval(val_expr,letseq);
        if (KNO_ABORTED(value))
          _return value;
        else if (VOIDP(value)) {
          kno_seterr(kno_VoidBinding,"letstar_evalfn",
                    KNO_SYMBOL_NAME(var),val_expr);
          return KNO_ERROR_VALUE;}
        else if (letseq->env_copy) {
          kno_bind_value(var,value,letseq->env_copy);
          kno_decref(value);}
        else {
          letseq_vars[i]=var;
          letseq_vals[i]=value;}
        i++;}}
    result = eval_inner_body(":LET*",
                       SYM_NAME(letseq_vars[0]),
                       expr,2,letseq,_stack);
    _return result;}
}

/* LETREC */

static lispval letrec_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval bindexprs = kno_get_arg(expr,1), result = VOID;
  int n;
  if (PRED_FALSE(! ( (bindexprs == KNO_NIL) || (PAIRP(bindexprs)) ) ))
    return kno_err(kno_BindSyntaxError,"LETREC",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    INIT_STACK_ENV(_stack,letrec,env,n);
    int i = 0, j = 0;
    {KNO_DOBINDINGS(var,val_expr,bindexprs) {
        letrec_vars[j]=var;
        letrec_vals[j]=KNO_UNBOUND;
        j++;}}
    {KNO_DOBINDINGS(var,val_expr,bindexprs) {
        lispval value = fast_eval(val_expr,letrec);
        if (KNO_ABORTED(value))
          _return value;
        else if (VOIDP(value)) {
          kno_seterr(kno_VoidBinding,"letstar_evalfn",
                    KNO_SYMBOL_NAME(var),val_expr);
          kno_decref_vec(letrec_vals,i);
          return KNO_ERROR_VALUE;}
        else if (letrec->env_copy) {
          kno_bind_value(var,value,letrec->env_copy);
          kno_decref(value);}
        else {
          letrec_vars[i]=var;
          letrec_vals[i]=value;}
        i++;}}
    result = eval_inner_body(":LETREC",
                             SYM_NAME(letrec_vars[0]),
                             expr,2,letrec,_stack);
    _return result;}
}

/* DO */

static lispval do_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval bindexprs = kno_get_arg(expr,1);
  lispval exitexprs = kno_get_arg(expr,2);
  lispval testexpr = kno_get_arg(exitexprs,0), testval = VOID;
  if (PRED_FALSE(! ( (bindexprs == KNO_NIL) || (PAIRP(bindexprs)) ) ))
    return kno_err(kno_BindSyntaxError,"DO",NULL,expr);
  else if (VOIDP(exitexprs))
    return kno_err(kno_BindSyntaxError,"DO",NULL,expr);
  else {
    lispval _vars[16], _vals[16], _updaters[16], _tmp[16];
    lispval *updaters, *vars, *vals, *tmp, result = VOID;
    int i = 0, n = 0;
    struct KNO_SCHEMAP bindings;
    struct KNO_LEXENV envstruct, *inner_env;
    if ((n = check_bindexprs(bindexprs,&result))<0) return result;
    else if (n>16) {
      lispval bindings; struct KNO_SCHEMAP *sm;
      inner_env = make_dynamic_env(n,env);
      bindings = inner_env->env_bindings; sm = (struct KNO_SCHEMAP *)bindings;
      vars = sm->table_schema; vals = sm->schema_values;
      updaters = u8_alloc_n(n,lispval);
      tmp = u8_alloc_n(n,lispval);}
    else {
      inner_env = init_static_env(n,env,&bindings,&envstruct,_vars,_vals);
      vars=_vars; vals=_vals; updaters=_updaters; tmp=_tmp;}
    /* Do the initial bindings */
    {KNO_DOLIST(bindexpr,bindexprs) {
      lispval var = kno_get_arg(bindexpr,0);
      lispval value_expr = kno_get_arg(bindexpr,1);
      lispval update_expr = kno_get_arg(bindexpr,2);
      lispval value = kno_eval(value_expr,env);
      if (KNO_ABORTED(value)) {
        /* When there's an error here, there's no need to bind. */
        kno_free_lexenv(inner_env);
        if (n>16) {u8_free(tmp); u8_free(updaters);}
        return value;}
      else {
        vars[i]=var; vals[i]=value; updaters[i]=update_expr;
        i++;}}}
    /* First test */
    testval = kno_eval(testexpr,inner_env);
    if (KNO_ABORTED(testval)) {
      if (n>16) {u8_free(tmp); u8_free(updaters);}
      return testval;}
    /* The iteration itself */
    while (FALSEP(testval)) {
      int i = 0; lispval body = kno_get_body(expr,3);
      /* Execute the body */
      KNO_DOLIST(bodyexpr,body) {
        lispval result = fast_eval(bodyexpr,inner_env);
        if (KNO_ABORTED(result)) {
          if (n>16) {u8_free(tmp); u8_free(updaters);}
          return result;}
        else kno_decref(result);}
      /* Do an update, storing new values in tmp[] to be consistent. */
      while (i < n) {
        if (!(VOIDP(updaters[i])))
          tmp[i]=kno_eval(updaters[i],inner_env);
        else tmp[i]=kno_incref(vals[i]);
        if (KNO_ABORTED(tmp[i])) {
          /* GC the updated values you've generated so far.
             Note that tmp[i] (the exception) is not freed. */
          kno_decref_vec(tmp,i);
          /* Free the temporary arrays if neccessary */
          if (n>16) {u8_free(tmp); u8_free(updaters);}
          /* Return the error result, adding the expr and environment. */
          return tmp[i];}
        else i++;}
      /* Now, free the current values and replace them with the values
         from tmp[]. */
      i = 0; while (i < n) {
        lispval val = vals[i];
        if ((CONSP(val))&&(KNO_MALLOCD_CONSP((kno_cons)val))) {
          kno_decref(val);}
        vals[i]=tmp[i];
        i++;}
      /* Free the testval and evaluate it again. */
      kno_decref(testval);
      if (envstruct.env_copy) {
        kno_recycle_lexenv(envstruct.env_copy);
        envstruct.env_copy = NULL;}
      testval = kno_eval(testexpr,inner_env);
      if (KNO_ABORTED(testval)) {
        /* If necessary, free the temporary arrays. */
        if (n>16) {u8_free(tmp); u8_free(updaters);}
        return testval;}}
    /* Now we're done, so we set result to testval. */
    result = testval;
    if (PAIRP(KNO_CDR(exitexprs))) {
      kno_decref(result);
      result = eval_inner_body(":DO",SYM_NAME(vars[0]),exitexprs,1,
                         inner_env,_stack);}
    /* Free the environment. */
    kno_free_lexenv(&envstruct);
    if (n>16) {u8_free(tmp); u8_free(updaters);}
    return result;}
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
    else if (kno_bind_value(var,inherited,env)) {
      kno_decref(inherited);
      return VOID;}
    else {
      kno_decref(inherited);
      return kno_err(kno_BindError,"DEFINE-LOCAL",SYM_NAME(var),var);}}
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
      lispval init_value = kno_eval(init_expr,env); int bound = 0;
      if (KNO_ABORTED(init_value)) return init_value;
      else bound = kno_bind_value(var,init_value,env);
      if (bound>0) {
        kno_decref(init_value);
        return VOID;}
      else if (bound<0) return KNO_ERROR;
      else return kno_err(kno_BindError,"DEFINE-INIT",SYM_NAME(var),var);}}
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
    lispval value = fast_stack_eval(val_expr,env,_stack);
    if (KNO_ABORTED(value))
      return value;
    else if (kno_bind_value(var,value,env))
      return value;
    else {
      kno_decref(value);
      return kno_err(kno_BindError,"DEF+",SYM_NAME(var),var);}}
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

  lispval module_spec = kno_stack_eval(module_expr,env,_stack,0);
  if (KNO_ABORTP(module_spec)) return module_spec;

  lispval module = ( (KNO_HASHTABLEP(module_spec)) || (KNO_LEXENVP(module_spec)) ) ?
    (kno_incref(module_spec)) : (kno_find_module(module_spec,0));
  if (KNO_ABORTP(module)) {
    kno_decref(module_spec);
    return module;}
  else if (KNO_TABLEP(module)) {
    lispval import_value = kno_get(module,import_name,KNO_VOID);
    if (KNO_ABORTP(import_value)) {
      kno_decref(module); kno_decref(module_spec);
      return import_value;}
    else if (KNO_VOIDP(import_value)) {
      kno_decref(module); kno_decref(module_spec);
      return kno_err("UndefinedImport","define_import",
                    KNO_SYMBOL_NAME(import_name),
                    module);}
    kno_bind_value(var,import_value,env);
    kno_decref(module);
    kno_decref(import_value);
    return KNO_VOID;}
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

  moduleid_symbol = kno_intern("%moduleid");

  kno_def_evalfn(kno_scheme_module,"SET!","",assign_evalfn);
  kno_def_evalfn(kno_scheme_module,"SET+!","",assign_plus_evalfn);

  kno_def_evalfn(kno_scheme_module,"LET","",let_evalfn);
  kno_def_evalfn(kno_scheme_module,"LET*","",letstar_evalfn);
  kno_def_evalfn(kno_scheme_module,"LETREC","",letrec_evalfn);
  kno_def_evalfn(kno_scheme_module,"DEFINE-INIT","",define_init_evalfn);
  kno_def_evalfn(kno_scheme_module,"DEFINE-LOCAL","",define_local_evalfn);
  kno_def_evalfn(kno_scheme_module,"DEF+","",define_return_evalfn);

  kno_def_evalfn(kno_scheme_module,"DO","",do_evalfn);

  kno_def_evalfn(kno_scheme_module,"DEFAULT!","",assign_default_evalfn);
  kno_def_evalfn(kno_scheme_module,"SETFALSE!","",assign_false_evalfn);
  kno_def_evalfn(kno_scheme_module,"BIND-DEFAULT!","",bind_default_evalfn);

  kno_def_evalfn(kno_scheme_module,"DEFINE-IMPORT",
                "Defines a local binding for a value in another module",
                define_import);
  kno_defalias(kno_scheme_module,"DEFIMPORT","DEFINE-IMPORT");

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
