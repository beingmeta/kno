/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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

fd_exception fd_BindError=_("Can't bind variable");
fd_exception fd_BindSyntaxError=_("Bad binding expression");

/* Set operations */

static fdtype set_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  int retval;
  fdtype var = fd_get_arg(expr,1), val_expr = fd_get_arg(expr,2), value;
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"SET!",NULL,expr);
  else if (!(FD_SYMBOLP(var)))
    return fd_err(fd_NotAnIdentifier,"SET!",NULL,expr);
  else if (FD_VOIDP(val_expr))
    return fd_err(fd_TooFewExpressions,"SET!",FD_SYMBOL_NAME(var),expr);
  value = fast_eval(val_expr,env);
  if (FD_ABORTED(value)) return value;
  else if ((retval = (fd_set_value(var,value,env)))) {
    fd_decref(value);
    if (retval<0) return FD_ERROR_VALUE;
    else return FD_VOID;}
  else if ((retval = (fd_bind_value(var,value,env)))) {
    fd_decref(value);
    if (retval<0) return FD_ERROR_VALUE;
    else return FD_VOID;}
  else return fd_err(fd_BindError,"SET!",FD_SYMBOL_NAME(var),var);
}

static fdtype set_plus_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype var = fd_get_arg(expr,1), val_expr = fd_get_arg(expr,2), value;
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"SET+!",NULL,expr);
  else if (!(FD_SYMBOLP(var)))
    return fd_err(fd_NotAnIdentifier,"SET+!",NULL,expr);
  else if (FD_VOIDP(val_expr))
    return fd_err(fd_TooFewExpressions,"SET+!",NULL,expr);
  value = fast_eval(val_expr,env);
  if (FD_ABORTED(value))
    return value;
  else if (fd_add_value(var,value,env)>0) {}
  else if (fd_bind_value(var,value,env)>=0) {}
  else return fd_err(fd_BindError,"SET+!",FD_SYMBOL_NAME(var),var);
  fd_decref(value);
  return FD_VOID;
}

static fdtype set_default_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype symbol = fd_get_arg(expr,1);
  fdtype value_expr = fd_get_arg(expr,2);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"set_default_evalfn",NULL,fd_incref(expr));
  else if (FD_VOIDP(value_expr))
    return fd_err(fd_SyntaxError,"set_default_evalfn",NULL,fd_incref(expr));
  else {
    fdtype val = fd_symeval(symbol,env);
    if ((FD_VOIDP(val))||(val == FD_UNBOUND)||(val == FD_DEFAULT_VALUE)) {
      fdtype value = fd_eval(value_expr,env);
      if (FD_ABORTED(value)) return value;
      if (fd_set_value(symbol,value,env)==0)
        fd_bind_value(symbol,value,env);
      fd_decref(value);
      return FD_VOID;}
    else {
      fd_decref(val); return FD_VOID;}}
}

static fdtype set_false_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype symbol = fd_get_arg(expr,1);
  fdtype value_expr = fd_get_arg(expr,2);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"set_false_evalfn",NULL,fd_incref(expr));
  else if (FD_VOIDP(value_expr))
    return fd_err(fd_SyntaxError,"set_false_evalfn",NULL,fd_incref(expr));
  else {
    fdtype val = fd_symeval(symbol,env);
    if ((FD_VOIDP(val))||(FD_FALSEP(val))||
        (val == FD_UNBOUND)||(val == FD_DEFAULT_VALUE)) {
      fdtype value = fd_eval(value_expr,env);
      if (FD_ABORTED(value)) return value;
      if (fd_set_value(symbol,value,env)==0)
        fd_bind_value(symbol,value,env);
      fd_decref(value);
      return FD_VOID;}
    else {
      fd_decref(val); return FD_VOID;}}
}

static fdtype bind_default_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype symbol = fd_get_arg(expr,1);
  fdtype value_expr = fd_get_arg(expr,2);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"bind_default_evalfn",NULL,fd_incref(expr));
  else if (FD_VOIDP(value_expr))
    return fd_err(fd_SyntaxError,"bind_default_evalfn",NULL,fd_incref(expr));
  else if (env == NULL)
    return fd_err(fd_SyntaxError,"bind_default_evalfn",NULL,fd_incref(expr));
  else {
    fdtype val = fd_get(env->env_bindings,symbol,FD_VOID);
    if ((FD_VOIDP(val))||(val == FD_UNBOUND)||(val == FD_DEFAULT_VALUE)) {
      fdtype value = fd_eval(value_expr,env);
      if (FD_ABORTED(value)) return value;
      fd_bind_value(symbol,value,env);
      fd_decref(value);
      return FD_VOID;}
    else {
      fd_decref(val); return FD_VOID;}}
}

static u8_mutex sset_lock;

/* This implements a simple version of globally synchronized set, which
   wraps a mutex around a regular set call, including evaluation of the
   value expression.  This can be used, for instance, to safely increment
   a variable. */
static fdtype sset_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  int retval;
  fdtype var = fd_get_arg(expr,1), val_expr = fd_get_arg(expr,2), value;
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"SSET!",NULL,expr);
  else if (!(FD_SYMBOLP(var)))
    return fd_err(fd_NotAnIdentifier,"SSET!",NULL,expr);
  else if (FD_VOIDP(val_expr))
    return fd_err(fd_TooFewExpressions,"SSET!",NULL,expr);
  u8_lock_mutex(&sset_lock);
  value = fast_eval(val_expr,env);
  if (FD_ABORTED(value)) {
    u8_unlock_mutex(&sset_lock);
    return value;}
  else if ((retval = (fd_set_value(var,value,env)))) {
    fd_decref(value); u8_unlock_mutex(&sset_lock);
    if (retval<0) return FD_ERROR_VALUE;
    else return FD_VOID;}
  else if ((retval = (fd_bind_value(var,value,env)))) {
    fd_decref(value); u8_unlock_mutex(&sset_lock);
    if (retval<0) return FD_ERROR_VALUE;
    else return FD_VOID;}
  else {
    u8_unlock_mutex(&sset_lock);
    return fd_err(fd_BindError,"SSET!",FD_SYMBOL_NAME(var),var);}
}

/* Environment utilities */

FD_FASTOP int check_bindexprs(fdtype bindexprs,fdtype *why_not)
{
  if (FD_PAIRP(bindexprs)) {
    int n = 0; FD_DOLIST(bindexpr,bindexprs) {
      fdtype var = fd_get_arg(bindexpr,0);
      if (FD_VOIDP(var)) {
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

FD_FASTOP fd_lispenv make_dynamic_env(int n,fd_lispenv parent)
{
  int i = 0;
  struct FD_ENVIRONMENT *e = u8_alloc(struct FD_ENVIRONMENT);
  fdtype *vars = u8_alloc_n(n,fdtype);
  fdtype *vals = u8_alloc_n(n,fdtype);
  fdtype schemap = fd_make_schemap(NULL,n,FD_SCHEMAP_PRIVATE,vars,vals);
  while (i<n) {vars[i]=FD_VOID; vals[i]=FD_VOID; i++;}
  FD_INIT_FRESH_CONS(e,fd_environment_type);
  e->env_copy = e; e->env_bindings = schemap; e->env_exports = FD_VOID;
  e->env_parent = fd_copy_env(parent);
  return e;
}

/* Simple binders */

static fdtype let_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype bindexprs = fd_get_arg(expr,1), result = FD_VOID;
  int n;
  if (FD_VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"LET",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    INIT_STATIC_ENV(letenv,env,n);
    int i = 0;
    {FD_DOBINDINGS(var,val_expr,bindexprs) {
        fdtype value = fast_eval(val_expr,env);
        if (FD_ABORTED(value)) {
          _return value;}
        else {
          letenv_vars[i]=var;
          letenv_vals[i]=value;
          i++;}}}
    result = eval_body(":LET",FD_SYMBOL_NAME(letenv_vars[0]),
                       expr,2,letenv,_stack);
    _return result;}
}

static fdtype letstar_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype bindexprs = fd_get_arg(expr,1), result = FD_VOID;
  int n;
  if (FD_VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"LET*",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    INIT_STATIC_ENV(letseq,env,n);
    int i = 0, j = 0;
    {FD_DOBINDINGS(var,val_expr,bindexprs) {
        letseq_vars[j]=var;
        letseq_vals[j]=FD_UNBOUND;
        j++;}}
    {FD_DOBINDINGS(var,val_expr,bindexprs) {
        fdtype value = fast_eval(val_expr,letseq);
        if (FD_ABORTED(value))
          _return value;
        else if (letseq->env_copy) {
          fd_bind_value(var,value,letseq->env_copy);
          fd_decref(value);}
        else {
          letseq_vars[i]=var;
          letseq_vals[i]=value;}
        i++;}}
    result = eval_body(":LET*",
                       FD_SYMBOL_NAME(letseq_vars[0]),
                       expr,2,letseq,_stack);
    _return result;}
}

/* DO */

static fdtype do_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype bindexprs = fd_get_arg(expr,1);
  fdtype exitexprs = fd_get_arg(expr,2);
  fdtype testexpr = fd_get_arg(exitexprs,0), testval = FD_VOID;
  if (FD_VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"DO",NULL,expr);
  else if (FD_VOIDP(exitexprs))
    return fd_err(fd_BindSyntaxError,"DO",NULL,expr);
  else {
    fdtype _vars[16], _vals[16], _updaters[16], _tmp[16];
    fdtype *updaters, *vars, *vals, *tmp, result = FD_VOID;
    int i = 0, n = 0;
    struct FD_SCHEMAP bindings;
    struct FD_ENVIRONMENT envstruct, *inner_env;
    if ((n = check_bindexprs(bindexprs,&result))<0) return result;
    else if (n>16) {
      fdtype bindings; struct FD_SCHEMAP *sm;
      inner_env = make_dynamic_env(n,env);
      bindings = inner_env->env_bindings; sm = (struct FD_SCHEMAP *)bindings;
      vars = sm->table_schema; vals = sm->schema_values;
      updaters = u8_alloc_n(n,fdtype);
      tmp = u8_alloc_n(n,fdtype);}
    else {
      inner_env = init_static_env(n,env,&bindings,&envstruct,_vars,_vals);
      vars=_vars; vals=_vals; updaters=_updaters; tmp=_tmp;}
    /* Do the initial bindings */
    {FD_DOLIST(bindexpr,bindexprs) {
      fdtype var = fd_get_arg(bindexpr,0);
      fdtype value_expr = fd_get_arg(bindexpr,1);
      fdtype update_expr = fd_get_arg(bindexpr,2);
      fdtype value = fd_eval(value_expr,env);
      if (FD_ABORTED(value)) {
        /* When there's an error here, there's no need to bind. */
        fd_free_environment(inner_env);
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
    while (FD_FALSEP(testval)) {
      int i = 0; fdtype body = fd_get_body(expr,3);
      /* Execute the body */
      FD_DOLIST(bodyexpr,body) {
        fdtype result = fast_eval(bodyexpr,inner_env);
        if (FD_ABORTED(result)) {
          if (n>16) {u8_free(tmp); u8_free(updaters);}
          return result;}
        else fd_decref(result);}
      /* Do an update, storing new values in tmp[] to be consistent. */
      while (i < n) {
        if (!(FD_VOIDP(updaters[i])))
          tmp[i]=fd_eval(updaters[i],inner_env);
        else tmp[i]=fd_incref(vals[i]);
        if (FD_ABORTED(tmp[i])) {
          /* GC the updated values you've generated so far.
             Note that tmp[i] (the exception) is not freed. */
          fd_decref_vec(tmp,i,0);
          /* Free the temporary arrays if neccessary */
          if (n>16) {u8_free(tmp); u8_free(updaters);}
          /* Return the error result, adding the expr and environment. */
          return tmp[i];}
        else i++;}
      /* Now, free the current values and replace them with the values
         from tmp[]. */
      i = 0; while (i < n) {
        fdtype val = vals[i];
        if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
          fd_decref(val);}
        vals[i]=tmp[i];
        i++;}
      /* Free the testval and evaluate it again. */
      fd_decref(testval);
      if (envstruct.env_copy) {
        fd_recycle_environment(envstruct.env_copy);
        envstruct.env_copy = NULL;}
      testval = fd_eval(testexpr,inner_env);
      if (FD_ABORTED(testval)) {
        /* If necessary, free the temporary arrays. */
        if (n>16) {u8_free(tmp); u8_free(updaters);}
        return testval;}}
    /* Now we're done, so we set result to testval. */
    result = testval;
    if (FD_PAIRP(FD_CDR(exitexprs))) {
      fd_decref(result);
      result = eval_body(":DO",FD_SYMBOL_NAME(vars[0]),exitexprs,1,
                         inner_env,_stack);}
    /* Free the environment. */
    fd_free_environment(&envstruct);
    if (n>16) {u8_free(tmp); u8_free(updaters);}
    return result;}
}

/* DEFINE-LOCAL */

/* This defines an identifier in the local environment to
   the value it would have anyway by environment inheritance.
   This is helpful if it was to rexport it, for example. */
static fdtype define_local_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype var = fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-LOCAL",NULL,expr);
  else if (FD_SYMBOLP(var)) {
    fdtype inherited = fd_symeval(var,env->env_parent);
    if (FD_ABORTED(inherited)) return inherited;
    else if (FD_VOIDP(inherited))
      return fd_err(fd_UnboundIdentifier,"DEFINE-LOCAL",
                    FD_SYMBOL_NAME(var),var);
    else if (fd_bind_value(var,inherited,env)) {
      fd_decref(inherited);
      return FD_VOID;}
    else {
      fd_decref(inherited);
      return fd_err(fd_BindError,"DEFINE-LOCAL",FD_SYMBOL_NAME(var),var);}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE-LOCAL",NULL,var);
}

/* DEFINE-INIT */

/* This defines an identifier in the local environment only if
   it is not currently defined. */
static fdtype define_init_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype var = fd_get_arg(expr,1);
  fdtype init_expr = fd_get_arg(expr,2);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-LOCAL",NULL,expr);
  else if (FD_VOIDP(init_expr))
    return fd_err(fd_TooFewExpressions,"DEFINE-LOCAL",NULL,expr);
  else if (FD_SYMBOLP(var)) {
    fdtype current = fd_get(env->env_bindings,var,FD_VOID);
    if (FD_ABORTED(current)) return current;
    else if (!(FD_VOIDP(current))) {
      fd_decref(current);
      return FD_VOID;}
    else {
      fdtype init_value = fd_eval(init_expr,env); int bound = 0;
      if (FD_ABORTED(init_value)) return init_value;
      else bound = fd_bind_value(var,init_value,env);
      if (bound>0) {
        fd_decref(init_value);
        return FD_VOID;}
      else if (bound<0) return FD_ERROR_VALUE;
      else return fd_err(fd_BindError,"DEFINE-INIT",FD_SYMBOL_NAME(var),var);}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE_INIT",NULL,var);
}

/* IPEVAL binding */

#if FD_IPEVAL_ENABLED
struct IPEVAL_BINDSTRUCT {
  int n_bindings; fdtype *vals;
  fdtype valexprs; fd_lispenv env;};

static int ipeval_let_step(struct IPEVAL_BINDSTRUCT *bs)
{
  int i = 0, n = bs->n_bindings;
  fdtype *bindings = bs->vals, scan = bs->valexprs;
  fd_lispenv env = bs->sproc_env;
  while (i<n) {
    fd_decref(bindings[i]); bindings[i++]=FD_VOID;}
  i = 0; while (FD_PAIRP(scan)) {
    fdtype binding = FD_CAR(scan), val_expr = FD_CADR(binding);
    fdtype val = fd_eval(val_expr,env);
    if (FD_ABORTED(val)) fd_interr(val);
    else bindings[i++]=val;
    scan = FD_CDR(scan);}
  return 1;
}

static int ipeval_letstar_step(struct IPEVAL_BINDSTRUCT *bs)
{
  int i = 0, n = bs->n_bindings;
  fdtype *bindings = bs->vals, scan = bs->valexprs;
  fd_lispenv env = bs->sproc_env;
  while (i<n) {
    fd_decref(bindings[i]); bindings[i++]=FD_UNBOUND;}
  i = 0; while (FD_PAIRP(scan)) {
    fdtype binding = FD_CAR(scan), val_expr = FD_CADR(binding);
    fdtype val = fd_eval(val_expr,env);
    if (FD_ABORTED(val)) fd_interr(val);
    else bindings[i++]=val;
    scan = FD_CDR(scan);}
  return 1;
}

static int ipeval_let_binding
  (int n,fdtype *vals,fdtype bindexprs,fd_lispenv env)
{
  struct IPEVAL_BINDSTRUCT bindstruct;
  bindstruct.n_bindings = n; bindstruct.vals = vals;
  bindstruct.valexprs = bindexprs; bindstruct.env = env;
  return fd_ipeval_call((fd_ipevalfn)ipeval_let_step,&bindstruct);
}

static int ipeval_letstar_binding
  (int n,fdtype *vals,fdtype bindexprs,fd_lispenv bind_env,fd_lispenv env)
{
  struct IPEVAL_BINDSTRUCT bindstruct;
  bindstruct.n_bindings = n; bindstruct.vals = vals;
  bindstruct.valexprs = bindexprs; bindstruct.env = env;
  return fd_ipeval_call((fd_ipevalfn)ipeval_letstar_step,&bindstruct);
}

static fdtype letq_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype bindexprs = fd_get_arg(expr,1), result = FD_VOID;
  int n;
  if (FD_VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"LET",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    struct FD_ENVIRONMENT *inner_env = make_dynamic_env(n,env);
    fdtype bindings = inner_env->env_bindings;
    struct FD_SCHEMAP *sm = (struct FD_SCHEMAP *)bindings;
    fdtype *vars = sm->table_schema, *vals = sm->schema_values;
    int i = 0; fdtype scan = bindexprs; while (i<n) {
      fdtype bind_expr = FD_CAR(scan), var = FD_CAR(bind_expr);
      vars[i]=var; vals[i]=FD_VOID; scan = FD_CDR(scan); i++;}
    if (ipeval_let_binding(n,vals,bindexprs,env)<0) 
      return ERROR_VALUE;
    {fdtype body = fd_get_body(expr,2);
     FD_DOLIST(bodyexpr,body) {
      fd_decref(result);
      result = fast_eval(bodyexpr,inner_env);
      if (FD_ABORTED(result))
        return result}}
    fd_free_environment(inner_env);
    return result;}
}

static fdtype letqstar_evalfn
(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype bindexprs = fd_get_arg(expr,1), result = FD_VOID;
  int n;
  if (FD_VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"LET*",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    struct FD_ENVIRONMENT *inner_env = make_dynamic_env(n,env);
    fdtype bindings = inner_env->env_bindings;
    struct FD_SCHEMAP *sm = (struct FD_SCHEMAP *)bindings;
    fdtype *vars = sm->table_schema, *vals = sm->schema_values;
    int i = 0; fdtype scan = bindexprs; while (i<n) {
      fdtype bind_expr = FD_CAR(scan), var = FD_CAR(bind_expr);
      vars[i]=var; vals[i]=FD_UNBOUND; scan = FD_CDR(scan); i++;}
    if (ipeval_letstar_binding(n,vals,bindexprs,inner_env,inner_env)<0) 
      return FD_ERROR_VALUE;
    {fdtype body = fd_get_body(expr,2);
     FD_DOLIST(bodyexpr,body) {
      fd_decref(result);
      result = fast_eval(bodyexpr,inner_env);
      if (FD_ABORTED(result)) 
        return result;}}
    if (inner_env->env_copy) fd_free_environment(inner_env->env_copy);
    return result;}
}

#endif

/* Extend fcnid parsing to incluce functional compounds */

/* Initialization */

FD_EXPORT void fd_init_binders_c()
{
  u8_register_source_file(_FILEINFO);

  moduleid_symbol = fd_intern("%MODULEID");

  u8_init_mutex(&sset_lock);

  fd_defspecial(fd_scheme_module,"SET!",set_evalfn);
  fd_defspecial(fd_scheme_module,"SET+!",set_plus_evalfn);
  fd_defspecial(fd_scheme_module,"SSET!",sset_evalfn);

  fd_defspecial(fd_scheme_module,"LET",let_evalfn);
  fd_defspecial(fd_scheme_module,"LET*",letstar_evalfn);
  fd_defspecial(fd_scheme_module,"DEFINE-INIT",define_init_evalfn);
  fd_defspecial(fd_scheme_module,"DEFINE-LOCAL",define_local_evalfn);

  fd_defspecial(fd_scheme_module,"DO",do_evalfn);

  fd_defspecial(fd_scheme_module,"DEFAULT!",set_default_evalfn);
  fd_defspecial(fd_scheme_module,"SETFALSE!",set_false_evalfn);
  fd_defspecial(fd_scheme_module,"BIND-DEFAULT!",bind_default_evalfn);

#if FD_IPEVAL_ENABLED
  fd_defspecial(fd_scheme_module,"LETQ",letq_evalfn);
  fd_defspecial(fd_scheme_module,"LETQ*",letqstar_evalfn);
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
