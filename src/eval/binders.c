/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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
#include "eval_internals.h"
#include "framerd/storage.h"

#include <libu8/u8printf.h>

u8_condition fd_BindError=_("Can't bind variable");
u8_condition fd_BindSyntaxError=_("Bad binding expression");

/* Set operations */

static lispval assign_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  int retval;
  lispval var = fd_get_arg(expr,1), val_expr = fd_get_arg(expr,2), value;
  if (VOIDP(var))
    return fd_err(fd_TooFewExpressions,"SET!",NULL,expr);
  else if (!(SYMBOLP(var)))
    return fd_err(fd_NotAnIdentifier,"SET!",NULL,expr);
  else if (VOIDP(val_expr))
    return fd_err(fd_TooFewExpressions,"SET!",SYM_NAME(var),expr);
  value = fast_eval(val_expr,env);
  if (FD_ABORTED(value)) return value;
  else if ((retval = (fd_assign_value(var,value,env)))) {
    fd_decref(value);
    if (PRED_FALSE(retval<0)) {
      /* TODO: Convert table errors to env errors */
      return FD_ERROR;}
    else return VOID;}
  else if ((retval = (fd_bind_value(var,value,env)))) {
    fd_decref(value);
    if (PRED_FALSE(retval<0)) {
      /* TODO: Convert table errors to env errors */
      return FD_ERROR;}
    else return VOID;}
  else return fd_err(fd_BindError,"SET!",SYM_NAME(var),var);
}

static lispval assign_plus_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval var = fd_get_arg(expr,1), val_expr = fd_get_arg(expr,2), value;
  if (VOIDP(var))
    return fd_err(fd_TooFewExpressions,"SET+!",NULL,expr);
  else if (!(SYMBOLP(var)))
    return fd_err(fd_NotAnIdentifier,"SET+!",NULL,expr);
  else if (VOIDP(val_expr))
    return fd_err(fd_TooFewExpressions,"SET+!",NULL,expr);
  value = fast_eval(val_expr,env);
  if (FD_ABORTED(value)) return value;
  else if (fd_add_value(var,value,env)>0) {}
  else if (fd_bind_value(var,value,env)>=0) {}
  else return fd_err(fd_BindError,"SET+!",SYM_NAME(var),var);
  fd_decref(value);
  return VOID;
}

static lispval assign_default_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval symbol = fd_get_arg(expr,1);
  lispval value_expr = fd_get_arg(expr,2);
  if (!(SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"assign_default_evalfn",NULL,fd_incref(expr));
  else if (VOIDP(value_expr))
    return fd_err(fd_SyntaxError,"assign_default_evalfn",NULL,fd_incref(expr));
  else {
    lispval val = fd_symeval(symbol,env);
    if ((VOIDP(val))||(val == FD_UNBOUND)||(val == FD_DEFAULT_VALUE)) {
      lispval value = fd_eval(value_expr,env);
      if (FD_ABORTED(value)) return value;
      /* TODO: Error checking */
      if (fd_assign_value(symbol,value,env)==0)
        fd_bind_value(symbol,value,env);
      fd_decref(value);
      return VOID;}
    else {
      fd_decref(val);
      return VOID;}}
}

static lispval assign_false_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval symbol = fd_get_arg(expr,1);
  lispval value_expr = fd_get_arg(expr,2);
  if (!(SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"assign_false_evalfn",NULL,fd_incref(expr));
  else if (VOIDP(value_expr))
    return fd_err(fd_SyntaxError,"assign_false_evalfn",NULL,fd_incref(expr));
  else {
    lispval val = fd_symeval(symbol,env);
    if ((VOIDP(val))||(FALSEP(val))||
        (val == FD_UNBOUND)||(val == FD_DEFAULT_VALUE)) {
      lispval value = fd_eval(value_expr,env);
      if (FD_ABORTED(value)) return value;
      /* TODO: Error checking */
      if (fd_assign_value(symbol,value,env)==0)
        fd_bind_value(symbol,value,env);
      fd_decref(value);
      return VOID;}
    else {
      fd_decref(val); return VOID;}}
}

static lispval bind_default_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval symbol = fd_get_arg(expr,1);
  lispval value_expr = fd_get_arg(expr,2);
  if (!(SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"bind_default_evalfn",NULL,fd_incref(expr));
  else if (VOIDP(value_expr))
    return fd_err(fd_SyntaxError,"bind_default_evalfn",NULL,fd_incref(expr));
  else if (env == NULL)
    return fd_err(fd_SyntaxError,"bind_default_evalfn",NULL,fd_incref(expr));
  else {
    lispval val = fd_get(env->env_bindings,symbol,VOID);
    if ((VOIDP(val))||(val == FD_UNBOUND)||(val == FD_DEFAULT_VALUE)) {
      lispval value = fd_eval(value_expr,env);
      if (FD_ABORTED(value)) return value;
      /* TODO: Error checking */
      fd_bind_value(symbol,value,env);
      fd_decref(value);
      return VOID;}
    else {
      fd_decref(val); return VOID;}}
}

static u8_mutex sassign_lock;

/* This implements a simple version of globally synchronized set, which
   wraps a mutex around a regular set call, including evaluation of the
   value expression.  This can be used, for instance, to safely increment
   a variable. */
static lispval sassign_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  int retval;
  lispval var = fd_get_arg(expr,1), val_expr = fd_get_arg(expr,2), value;
  if (VOIDP(var))
    return fd_err(fd_TooFewExpressions,"SSET!",NULL,expr);
  else if (!(SYMBOLP(var)))
    return fd_err(fd_NotAnIdentifier,"SSET!",NULL,expr);
  else if (VOIDP(val_expr))
    return fd_err(fd_TooFewExpressions,"SSET!",NULL,expr);
  u8_lock_mutex(&sassign_lock);
  value = fast_eval(val_expr,env);
  if (FD_ABORTED(value)) {
    u8_unlock_mutex(&sassign_lock);
    return value;}
  else if ((retval = (fd_assign_value(var,value,env)))) {
    fd_decref(value); u8_unlock_mutex(&sassign_lock);
    if (retval<0) return FD_ERROR;
    else return VOID;}
  else if ((retval = (fd_bind_value(var,value,env)))) {
    fd_decref(value); u8_unlock_mutex(&sassign_lock);
    if (retval<0) return FD_ERROR;
    else return VOID;}
  else {
    u8_unlock_mutex(&sassign_lock);
    return fd_err(fd_BindError,"SSET!",SYM_NAME(var),var);}
}

/* Environment utilities */

FD_FASTOP int check_bindexprs(lispval bindexprs,lispval *why_not)
{
  if (PAIRP(bindexprs)) {
    int n = 0; FD_DOLIST(bindexpr,bindexprs) {
      lispval var = fd_get_arg(bindexpr,0);
      if (VOIDP(var)) {
        *why_not = fd_err(fd_BindSyntaxError,NULL,NULL,bindexpr);
        return -1;}
      else n++;}
    return n;}
  else if (FD_CODEP(bindexprs)) {
    int len = FD_CODE_LENGTH(bindexprs);
    if ((len%2)==1) {
      *why_not = fd_err(fd_BindSyntaxError,"check_bindexprs",NULL,bindexprs);
      return -1;}
    else return len/2;}
  else return -1;
}

FD_FASTOP fd_lexenv make_dynamic_env(int n,fd_lexenv parent)
{
  int i = 0;
  struct FD_LEXENV *e = u8_alloc(struct FD_LEXENV);
  lispval *vars = u8_alloc_n(n,lispval);
  lispval *vals = u8_alloc_n(n,lispval);
  lispval schemap = fd_make_schemap(NULL,n,FD_SCHEMAP_PRIVATE,vars,vals);
  while (i<n) {vars[i]=VOID; vals[i]=VOID; i++;}
  FD_INIT_FRESH_CONS(e,fd_lexenv_type);
  e->env_copy = e;
  e->env_bindings = schemap;
  e->env_exports = VOID;
  e->env_parent = fd_copy_env(parent);
  return e;
}

/* Simple binders */

static lispval let_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval bindexprs = fd_get_arg(expr,1), result = VOID;
  int n;
  if (VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"LET",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    INIT_STACK_ENV(_stack,letenv,env,n);
    int i = 0;
    {FD_DOBINDINGS(var,val_expr,bindexprs) {
        lispval value = fast_eval(val_expr,env);
        if (FD_ABORTED(value)) {
          _return value;}
        else if (VOIDP(value)) {
          fd_seterr(fd_VoidBinding,"let_evalfn",
                    FD_SYMBOL_NAME(var),val_expr);
          return FD_ERROR_VALUE;}
        else {
          letenv_vars[i]=var;
          letenv_vals[i]=value;
          i++;}}}
    result = eval_inner_body(":LET",SYM_NAME(letenv_vars[0]),
                       expr,2,letenv,_stack);
    _return result;}
}

static lispval letstar_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval bindexprs = fd_get_arg(expr,1), result = VOID;
  int n;
  if (VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"LET*",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    INIT_STACK_ENV(_stack,letseq,env,n);
    int i = 0, j = 0;
    {FD_DOBINDINGS(var,val_expr,bindexprs) {
        letseq_vars[j]=var;
        letseq_vals[j]=FD_UNBOUND;
        j++;}}
    {FD_DOBINDINGS(var,val_expr,bindexprs) {
        lispval value = fast_eval(val_expr,letseq);
        if (FD_ABORTED(value))
          _return value;
        else if (VOIDP(value)) {
          fd_seterr(fd_VoidBinding,"letstar_evalfn",
                    FD_SYMBOL_NAME(var),val_expr);
          return FD_ERROR_VALUE;}
        else if (letseq->env_copy) {
          fd_bind_value(var,value,letseq->env_copy);
          fd_decref(value);}
        else {
          letseq_vars[i]=var;
          letseq_vals[i]=value;}
        i++;}}
    result = eval_inner_body(":LET*",
                       SYM_NAME(letseq_vars[0]),
                       expr,2,letseq,_stack);
    _return result;}
}

/* DO */

static lispval do_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval bindexprs = fd_get_arg(expr,1);
  lispval exitexprs = fd_get_arg(expr,2);
  lispval testexpr = fd_get_arg(exitexprs,0), testval = VOID;
  if (VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"DO",NULL,expr);
  else if (VOIDP(exitexprs))
    return fd_err(fd_BindSyntaxError,"DO",NULL,expr);
  else {
    lispval _vars[16], _vals[16], _updaters[16], _tmp[16];
    lispval *updaters, *vars, *vals, *tmp, result = VOID;
    int i = 0, n = 0;
    struct FD_SCHEMAP bindings;
    struct FD_LEXENV envstruct, *inner_env;
    if ((n = check_bindexprs(bindexprs,&result))<0) return result;
    else if (n>16) {
      lispval bindings; struct FD_SCHEMAP *sm;
      inner_env = make_dynamic_env(n,env);
      bindings = inner_env->env_bindings; sm = (struct FD_SCHEMAP *)bindings;
      vars = sm->table_schema; vals = sm->schema_values;
      updaters = u8_alloc_n(n,lispval);
      tmp = u8_alloc_n(n,lispval);}
    else {
      inner_env = init_static_env(n,env,&bindings,&envstruct,_vars,_vals);
      vars=_vars; vals=_vals; updaters=_updaters; tmp=_tmp;}
    /* Do the initial bindings */
    {FD_DOLIST(bindexpr,bindexprs) {
      lispval var = fd_get_arg(bindexpr,0);
      lispval value_expr = fd_get_arg(bindexpr,1);
      lispval update_expr = fd_get_arg(bindexpr,2);
      lispval value = fd_eval(value_expr,env);
      if (FD_ABORTED(value)) {
        /* When there's an error here, there's no need to bind. */
        fd_free_lexenv(inner_env);
        if (n>16) {u8_free(tmp); u8_free(updaters);}
        return value;}
      else {
        vars[i]=var; vals[i]=value; updaters[i]=update_expr;
        i++;}}}
    /* First test */
    testval = fd_eval(testexpr,inner_env);
    if (FD_ABORTED(testval)) {
      if (n>16) {u8_free(tmp); u8_free(updaters);}
      return testval;}
    /* The iteration itself */
    while (FALSEP(testval)) {
      int i = 0; lispval body = fd_get_body(expr,3);
      /* Execute the body */
      FD_DOLIST(bodyexpr,body) {
        lispval result = fast_eval(bodyexpr,inner_env);
        if (FD_ABORTED(result)) {
          if (n>16) {u8_free(tmp); u8_free(updaters);}
          return result;}
        else fd_decref(result);}
      /* Do an update, storing new values in tmp[] to be consistent. */
      while (i < n) {
        if (!(VOIDP(updaters[i])))
          tmp[i]=fd_eval(updaters[i],inner_env);
        else tmp[i]=fd_incref(vals[i]);
        if (FD_ABORTED(tmp[i])) {
          /* GC the updated values you've generated so far.
             Note that tmp[i] (the exception) is not freed. */
          fd_decref_vec(tmp,i);
          /* Free the temporary arrays if neccessary */
          if (n>16) {u8_free(tmp); u8_free(updaters);}
          /* Return the error result, adding the expr and environment. */
          return tmp[i];}
        else i++;}
      /* Now, free the current values and replace them with the values
         from tmp[]. */
      i = 0; while (i < n) {
        lispval val = vals[i];
        if ((CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
          fd_decref(val);}
        vals[i]=tmp[i];
        i++;}
      /* Free the testval and evaluate it again. */
      fd_decref(testval);
      if (envstruct.env_copy) {
        fd_recycle_lexenv(envstruct.env_copy);
        envstruct.env_copy = NULL;}
      testval = fd_eval(testexpr,inner_env);
      if (FD_ABORTED(testval)) {
        /* If necessary, free the temporary arrays. */
        if (n>16) {u8_free(tmp); u8_free(updaters);}
        return testval;}}
    /* Now we're done, so we set result to testval. */
    result = testval;
    if (PAIRP(FD_CDR(exitexprs))) {
      fd_decref(result);
      result = eval_inner_body(":DO",SYM_NAME(vars[0]),exitexprs,1,
                         inner_env,_stack);}
    /* Free the environment. */
    fd_free_lexenv(&envstruct);
    if (n>16) {u8_free(tmp); u8_free(updaters);}
    return result;}
}

/* DEFINE-LOCAL */

/* This defines an identifier in the local environment to
   the value it would have anyway by environment inheritance.
   This is helpful if it was to rexport it, for example. */
static lispval define_local_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval var = fd_get_arg(expr,1);
  if (VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-LOCAL",NULL,expr);
  else if (SYMBOLP(var)) {
    lispval inherited = fd_symeval(var,env->env_parent);
    if (FD_ABORTED(inherited)) return inherited;
    else if (VOIDP(inherited))
      return fd_err(fd_UnboundIdentifier,"DEFINE-LOCAL",
                    SYM_NAME(var),var);
    else if (fd_bind_value(var,inherited,env)) {
      fd_decref(inherited);
      return VOID;}
    else {
      fd_decref(inherited);
      return fd_err(fd_BindError,"DEFINE-LOCAL",SYM_NAME(var),var);}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE-LOCAL",NULL,var);
}

/* DEFINE-INIT */

/* This defines an identifier in the local environment only if
   it is not currently defined. */
static lispval define_init_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval var = fd_get_arg(expr,1);
  lispval init_expr = fd_get_arg(expr,2);
  if (VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-LOCAL",NULL,expr);
  else if (VOIDP(init_expr))
    return fd_err(fd_TooFewExpressions,"DEFINE-LOCAL",NULL,expr);
  else if (SYMBOLP(var)) {
    lispval current = fd_get(env->env_bindings,var,VOID);
    if (FD_ABORTED(current)) return current;
    else if (!(VOIDP(current))) {
      fd_decref(current);
      return VOID;}
    else {
      lispval init_value = fd_eval(init_expr,env); int bound = 0;
      if (FD_ABORTED(init_value)) return init_value;
      else bound = fd_bind_value(var,init_value,env);
      if (bound>0) {
        fd_decref(init_value);
        return VOID;}
      else if (bound<0) return FD_ERROR;
      else return fd_err(fd_BindError,"DEFINE-INIT",SYM_NAME(var),var);}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE_INIT",NULL,var);
}

/* This defines an identifier in the local environment to
   the value it would have anyway by environment inheritance.
   This is helpful if it was to rexport it, for example. */
static lispval define_return_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval var = fd_get_arg(expr,1), val_expr = fd_get_arg(expr,2);
  if ( (VOIDP(var)) || (VOIDP(val_expr)) )
    return fd_err(fd_TooFewExpressions,"DEF+",NULL,expr);
  else if (SYMBOLP(var)) {
    lispval value = fast_stack_eval(val_expr,env,_stack);
    if (FD_ABORTED(value))
      return value;
    else if (fd_bind_value(var,value,env))
      return value;
    else {
      fd_decref(value);
      return fd_err(fd_BindError,"DEF+",SYM_NAME(var),var);}}
  else return fd_err(fd_NotAnIdentifier,"DEF+",NULL,var);
}

/* DEFINE-INIT */

/* This defines an identifier in the local environment only if
   it is not currently defined. */
static lispval define_import_evalfn(lispval expr,fd_lexenv env,fd_stack _stack,int safe)
{
  lispval var = fd_get_arg(expr,1);
  lispval module_expr = fd_get_arg(expr,2);
  lispval import_name = fd_get_arg(expr,3);
  if (VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-IMPORT",NULL,expr);
  else if (VOIDP(module_expr))
    return fd_err(fd_TooFewExpressions,"DEFINE-IMPORT",NULL,expr);
  else if (!(FD_SYMBOLP(var)))
    return fd_err(fd_SyntaxError,"DEFINE-IMPORT/VAR",NULL,expr);

  if (FD_VOIDP(import_name)) import_name = var;

  lispval module_spec = fd_stack_eval(module_expr,env,_stack,0);
  if (FD_ABORTP(module_spec)) return module_spec;

  lispval module = ( (FD_HASHTABLEP(module_spec)) || (FD_LEXENVP(module_spec)) ) ?
    (fd_incref(module_spec)) : (fd_find_module(module_spec,safe,0));
  if (FD_ABORTP(module)) {
    fd_decref(module_spec);
    return module;}
  else if (FD_TABLEP(module)) {
    lispval import_value = fd_get(module,import_name,FD_VOID);
    if (FD_ABORTP(import_value)) {
      fd_decref(module); fd_decref(module_spec);
      return import_value;}
    else if (FD_VOIDP(import_value)) {
      fd_decref(module); fd_decref(module_spec);
      return fd_err("UndefinedImport","define_import",
                    FD_SYMBOL_NAME(import_name),
                    module);}
    fd_bind_value(var,import_value,env);
    fd_decref(module);
    fd_decref(import_value);
    return FD_VOID;}
  else {
    fd_decref(module_spec);
    return fd_err("NotAModule","define_import_evalfn",NULL,module);}
}

static lispval define_import(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return define_import_evalfn(expr,env,_stack,0);
}

static lispval safe_define_import(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return define_import_evalfn(expr,env,_stack,1);
}

/* Initialization */

FD_EXPORT void fd_init_binders_c()
{
  u8_register_source_file(_FILEINFO);

  moduleid_symbol = fd_intern("%MODULEID");

  u8_init_mutex(&sassign_lock);

  fd_def_evalfn(fd_scheme_module,"SET!","",assign_evalfn);
  fd_def_evalfn(fd_scheme_module,"SET+!","",assign_plus_evalfn);
  fd_def_evalfn(fd_scheme_module,"SSET!","",sassign_evalfn);

  fd_def_evalfn(fd_scheme_module,"LET","",let_evalfn);
  fd_def_evalfn(fd_scheme_module,"LET*","",letstar_evalfn);
  fd_def_evalfn(fd_scheme_module,"DEFINE-INIT","",define_init_evalfn);
  fd_def_evalfn(fd_scheme_module,"DEFINE-LOCAL","",define_local_evalfn);
  fd_def_evalfn(fd_scheme_module,"DEF+","",define_return_evalfn);

  fd_def_evalfn(fd_scheme_module,"DO","",do_evalfn);

  fd_def_evalfn(fd_scheme_module,"DEFAULT!","",assign_default_evalfn);
  fd_def_evalfn(fd_scheme_module,"SETFALSE!","",assign_false_evalfn);
  fd_def_evalfn(fd_scheme_module,"BIND-DEFAULT!","",bind_default_evalfn);

  fd_def_evalfn(fd_xscheme_module,"DEFINE-IMPORT",
                "Defines a local binding for a value in another module",
                define_import);
  fd_def_evalfn(fd_scheme_module,"DEFINE-IMPORT",
                "Defines a local binding for a value in another module",
                safe_define_import);
  fd_defalias(fd_xscheme_module,"DEFIMPORT","DEFINE-IMPORT");
  fd_defalias(fd_scheme_module,"DEFIMPORT","DEFINE-IMPORT");

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
