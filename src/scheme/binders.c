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
#include "framerd/fddb.h"

#include <libu8/u8printf.h>

fd_exception fd_BadArglist=_("Malformed argument list");
fd_exception fd_BindError=_("Can't bind variable");
fd_exception fd_BindSyntaxError=_("Bad binding expression");
fd_exception fd_BadDefineForm=_("Bad procedure defining form");

fd_ptr_type fd_macro_type;

static fdtype lambda_symbol;

static u8_string sproc_id(struct FD_SPROC *fn)
{
  if ((fn->fcn_name)&&(fn->fcn_filename))
    return u8_mkstring("%s:%s",fn->fcn_name,fn->fcn_filename);
  else if (fn->fcn_name)
    return u8_strdup(fn->fcn_name);
  else if (fn->fcn_filename)
    return u8_mkstring("λ%lx:%s",
                       ((unsigned long)
                        (((unsigned long long)fn)&0xFFFFFFFF)),
                       fn->fcn_filename);
  else return u8_mkstring("λ%lx",
                          ((unsigned long)
                           (((unsigned long long)fn)&0xFFFFFFFF)));
}

/* Set operations */

static fdtype set_handler(fdtype expr,fd_lispenv env)
{
  int retval;
  fdtype var=fd_get_arg(expr,1), val_expr=fd_get_arg(expr,2), value;
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"SET!",NULL,expr);
  else if (!(FD_SYMBOLP(var)))
    return fd_err(fd_NotAnIdentifier,"SET!",NULL,expr);
  else if (FD_VOIDP(val_expr))
    return fd_err(fd_TooFewExpressions,"SET!",FD_SYMBOL_NAME(var),expr);
  value=fasteval(val_expr,env);
  if (FD_ABORTED(value)) return value;
  else if ((retval=(fd_set_value(var,value,env)))) {
    fd_decref(value);
    if (retval<0) return FD_ERROR_VALUE;
    else return FD_VOID;}
  else if ((retval=(fd_bind_value(var,value,env)))) {
    fd_decref(value);
    if (retval<0) return FD_ERROR_VALUE;
    else return FD_VOID;}
  else return fd_err(fd_BindError,"SET!",FD_SYMBOL_NAME(var),var);
}

static fdtype set_plus_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1), val_expr=fd_get_arg(expr,2), value;
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"SET+!",NULL,expr);
  else if (!(FD_SYMBOLP(var)))
    return fd_err(fd_NotAnIdentifier,"SET+!",NULL,expr);
  else if (FD_VOIDP(val_expr))
    return fd_err(fd_TooFewExpressions,"SET+!",NULL,expr);
  value=fasteval(val_expr,env);
  if (FD_ABORTED(value))
    return value;
  else if (fd_add_value(var,value,env)) {}
  else if (fd_bind_value(var,value,env)) {}
  else return fd_err(fd_BindError,"SET+!",FD_SYMBOL_NAME(var),var);
  fd_decref(value);
  return FD_VOID;
}

static fdtype set_default_handler(fdtype expr,fd_lispenv env)
{
  fdtype symbol=fd_get_arg(expr,1);
  fdtype value_expr=fd_get_arg(expr,2);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"set_default_handler",NULL,fd_incref(expr));
  else if (FD_VOIDP(value_expr))
    return fd_err(fd_SyntaxError,"set_default_handler",NULL,fd_incref(expr));
  else {
    fdtype val=fd_symeval(symbol,env);
    if ((FD_VOIDP(val))||(val==FD_UNBOUND)||(val==FD_DEFAULT_VALUE)) {
      fdtype value=fd_eval(value_expr,env);
      if (FD_ABORTED(value)) return value;
      if (fd_set_value(symbol,value,env)==0)
        fd_bind_value(symbol,value,env);
      fd_decref(value);
      return FD_VOID;}
    else {
      fd_decref(val); return FD_VOID;}}
}

static fdtype set_false_handler(fdtype expr,fd_lispenv env)
{
  fdtype symbol=fd_get_arg(expr,1);
  fdtype value_expr=fd_get_arg(expr,2);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"set_false_handler",NULL,fd_incref(expr));
  else if (FD_VOIDP(value_expr))
    return fd_err(fd_SyntaxError,"set_false_handler",NULL,fd_incref(expr));
  else {
    fdtype val=fd_symeval(symbol,env);
    if ((FD_VOIDP(val))||(FD_FALSEP(val))||
        (val==FD_UNBOUND)||(val==FD_DEFAULT_VALUE)) {
      fdtype value=fd_eval(value_expr,env);
      if (FD_ABORTED(value)) return value;
      if (fd_set_value(symbol,value,env)==0)
        fd_bind_value(symbol,value,env);
      fd_decref(value);
      return FD_VOID;}
    else {
      fd_decref(val); return FD_VOID;}}
}

static fdtype bind_default_handler(fdtype expr,fd_lispenv env)
{
  fdtype symbol=fd_get_arg(expr,1);
  fdtype value_expr=fd_get_arg(expr,2);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"bind_default_handler",NULL,fd_incref(expr));
  else if (FD_VOIDP(value_expr))
    return fd_err(fd_SyntaxError,"bind_default_handler",NULL,fd_incref(expr));
  else if (env==NULL)
    return fd_err(fd_SyntaxError,"bind_default_handler",NULL,fd_incref(expr));
  else {
    fdtype val=fd_get(env->env_bindings,symbol,FD_VOID);
    if ((FD_VOIDP(val))||(val==FD_UNBOUND)||(val==FD_DEFAULT_VALUE)) {
      fdtype value=fd_eval(value_expr,env);
      if (FD_ABORTED(value)) return value;
      fd_bind_value(symbol,value,env);
      fd_decref(value);
      return FD_VOID;}
    else {
      fd_decref(val); return FD_VOID;}}
}

#if FD_THREADS_ENABLED
static u8_mutex sset_lock;
#endif

/* This implements a simple version of globally synchronized set, which
   wraps a mutex around a regular set call, including evaluation of the
   value expression.  This can be used, for instance, to safely increment
   a variable. */
static fdtype sset_handler(fdtype expr,fd_lispenv env)
{
  int retval;
  fdtype var=fd_get_arg(expr,1), val_expr=fd_get_arg(expr,2), value;
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"SSET!",NULL,expr);
  else if (!(FD_SYMBOLP(var)))
    return fd_err(fd_NotAnIdentifier,"SSET!",NULL,expr);
  else if (FD_VOIDP(val_expr))
    return fd_err(fd_TooFewExpressions,"SSET!",NULL,expr);
  fd_lock_mutex(&sset_lock);
  value=fasteval(val_expr,env);
  if (FD_ABORTED(value)) {
    fd_unlock_mutex(&sset_lock);
    return value;}
  else if ((retval=(fd_set_value(var,value,env)))) {
    fd_decref(value); fd_unlock_mutex(&sset_lock);
    if (retval<0) return FD_ERROR_VALUE;
    else return FD_VOID;}
  else if ((retval=(fd_bind_value(var,value,env)))) {
    fd_decref(value); fd_unlock_mutex(&sset_lock);
    if (retval<0) return FD_ERROR_VALUE;
    else return FD_VOID;}
  else {
    fd_unlock_mutex(&sset_lock);
    return fd_err(fd_BindError,"SSET!",FD_SYMBOL_NAME(var),var);}
}

/* Environment utilities */

FD_FASTOP int check_bindexprs(fdtype bindexprs,fdtype *why_not)
{
  if (FD_PAIRP(bindexprs)) {
    int n=0; FD_DOLIST(bindexpr,bindexprs) {
      fdtype var=fd_get_arg(bindexpr,0);
      if (FD_VOIDP(var)) {
        *why_not=fd_err(fd_BindSyntaxError,NULL,NULL,bindexpr);
        return -1;}
      else n++;}
    return n;}
  else if (FD_RAILP(bindexprs)) {
    int len=FD_RAIL_LENGTH(bindexprs);
    if ((len%2)==1) {
      *why_not=fd_err(fd_BindSyntaxError,"check_bindexprs",NULL,bindexprs);
      return -1;}
    else return len/2;}
  else return -1;
}

FD_FASTOP fd_lispenv init_static_env
  (int n,fd_lispenv parent,
   struct FD_SCHEMAP *bindings,struct FD_ENVIRONMENT *envstruct,
   fdtype *vars,fdtype *vals)
{
  int i=0; while (i < n) {
    vars[i]=FD_VOID; vals[i]=FD_VOID; i++;}
  FD_INIT_STATIC_CONS(envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(bindings,fd_schemap_type);
  bindings->schemap_onstack=1;
  bindings->table_schema=vars;
  bindings->schema_values=vals;
  bindings->table_size=n;
  fd_init_rwlock(&(bindings->table_rwlock));
  envstruct->env_bindings=FDTYPE_CONS((bindings));
  envstruct->env_exports=FD_VOID;
  envstruct->env_parent=parent;
  envstruct->env_copy=NULL;
  return envstruct;
}

FD_FASTOP fd_lispenv make_dynamic_env(int n,fd_lispenv parent)
{
  int i=0;
  struct FD_ENVIRONMENT *e=u8_alloc(struct FD_ENVIRONMENT);
  fdtype *vars=u8_alloc_n(n,fdtype);
  fdtype *vals=u8_alloc_n(n,fdtype);
  fdtype schemap=fd_make_schemap(NULL,n,FD_SCHEMAP_PRIVATE,vars,vals);
  while (i<n) {vars[i]=FD_VOID; vals[i]=FD_VOID; i++;}
  FD_INIT_FRESH_CONS(e,fd_environment_type);
  e->env_copy=e; e->env_bindings=schemap; e->env_exports=FD_VOID;
  e->env_parent=fd_copy_env(parent);
  return e;
}

/* Simple binders */

static fdtype let_handler(fdtype expr,fd_lispenv env)
{
  fdtype bindexprs=fd_get_arg(expr,1), result=FD_VOID;
  int n;
  if (FD_VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"LET",NULL,expr);
  else if ((n=check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    struct FD_SCHEMAP bindings;
    struct FD_ENVIRONMENT envstruct, *inner_env;
    fdtype _vars[16], _vals[16], *vars, *vals;
    int i=0;
    if (n>16) {
      fdtype bindings; struct FD_SCHEMAP *sm;
      inner_env=make_dynamic_env(n,env);
      bindings=inner_env->env_bindings; sm=(struct FD_SCHEMAP *)bindings;
      vars=sm->table_schema; vals=sm->schema_values;}
    else {
      inner_env=init_static_env(n,env,&bindings,&envstruct,_vars,_vals);
      vars=_vars; vals=_vals;}
    {FD_DOBINDINGS(var,val_expr,bindexprs) {
        fdtype value=fasteval(val_expr,env);
        if (FD_ABORTED(value))
          return return_error_env(value,":LET",inner_env);
        else {
          vars[i]=var; vals[i]=value; i++;}}}
    result=eval_body(":LET",expr,2,inner_env);
    free_environment(inner_env);
    return result;}
}

static fdtype letstar_handler(fdtype expr,fd_lispenv env)
{
  fdtype bindexprs=fd_get_arg(expr,1), result=FD_VOID;
  int n;
  if (FD_VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"LET*",NULL,expr);
  else if ((n=check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    struct FD_SCHEMAP bindings;
    struct FD_ENVIRONMENT envstruct, *inner_env;
    fdtype _vars[16], _vals[16], *vars, *vals;
    int i=0, j=0;
    if (n>16) {
      fdtype bindings; struct FD_SCHEMAP *sm;
      inner_env=make_dynamic_env(n,env);
      bindings=inner_env->env_bindings; sm=(struct FD_SCHEMAP *)bindings;
      vars=sm->table_schema; vals=sm->schema_values;}
    else {
      inner_env=init_static_env(n,env,&bindings,&envstruct,_vars,_vals);
      vars=_vars; vals=_vals;}
    {FD_DOBINDINGS(var,val_expr,bindexprs) {
      vars[j]=var; vals[j]=FD_UNBOUND; j++;}}
    {FD_DOBINDINGS(var,val_expr,bindexprs) {
      fdtype value=fasteval(val_expr,inner_env);
      if (FD_ABORTED(value))
        return return_error_env(value,":LET*",inner_env);
      else if (inner_env->env_copy) {
        fd_bind_value(var,value,inner_env->env_copy);
        fd_decref(value);}
      else {
        vars[i]=var; vals[i]=value;}
      i++;}}
    result=eval_body(":LET*",expr,2,inner_env);
    free_environment(inner_env);
    return result;}
}

/* DO */

static fdtype do_handler(fdtype expr,fd_lispenv env)
{
  fdtype bindexprs=fd_get_arg(expr,1);
  fdtype exitexprs=fd_get_arg(expr,2);
  fdtype testexpr=fd_get_arg(exitexprs,0), testval=FD_VOID;
  if (FD_VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"DO",NULL,expr);
  else if (FD_VOIDP(exitexprs))
    return fd_err(fd_BindSyntaxError,"DO",NULL,expr);
  else {
    fdtype _vars[16], _vals[16], _updaters[16], _tmp[16];
    fdtype *updaters, *vars, *vals, *tmp, result=FD_VOID;
    int i=0, n=0;
    struct FD_SCHEMAP bindings;
    struct FD_ENVIRONMENT envstruct, *inner_env;
    if ((n=check_bindexprs(bindexprs,&result))<0) return result;
    else if (n>16) {
      fdtype bindings; struct FD_SCHEMAP *sm;
      inner_env=make_dynamic_env(n,env);
      bindings=inner_env->env_bindings; sm=(struct FD_SCHEMAP *)bindings;
      vars=sm->table_schema; vals=sm->schema_values;
      updaters=u8_alloc_n(n,fdtype);
      tmp=u8_alloc_n(n,fdtype);}
    else {
      inner_env=init_static_env(n,env,&bindings,&envstruct,_vars,_vals);
      vars=_vars; vals=_vals; updaters=_updaters; tmp=_tmp;}
    /* Do the initial bindings */
    {FD_DOBODY(bindexpr,bindexprs,0) {
      fdtype var=fd_get_arg(bindexpr,0);
      fdtype value_expr=fd_get_arg(bindexpr,1);
      fdtype update_expr=fd_get_arg(bindexpr,2);
      fdtype value=fd_eval(value_expr,env);
      if (FD_ABORTED(value)) {
        /* When there's an error here, there's no need to bind. */
        free_environment(inner_env);
        if (n>16) {u8_free(tmp); u8_free(updaters);}
        return value;}
      else {
        vars[i]=var; vals[i]=value; updaters[i]=update_expr;
        i++;}}}
    /* First test */
    testval=fd_eval(testexpr,inner_env);
    if (FD_ABORTED(testval)) {
      if (n>16) {u8_free(tmp); u8_free(updaters);}
      return return_error_env(testval,":DO",inner_env);}
    /* The iteration itself */
    while (FD_FALSEP(testval)) {
      int i=0;
      /* Execute the body */
      FD_DOBODY(bodyexpr,expr,3) {
        fdtype result=fasteval(bodyexpr,inner_env);
        if (FD_ABORTED(result)) {
          if (n>16) {u8_free(tmp); u8_free(updaters);}
          return return_error_env(result,":DO",inner_env);}
        else fd_decref(result);}
      /* Do an update, storing new values in tmp[] to be consistent. */
      while (i < n) {
        if (!(FD_VOIDP(updaters[i])))
          tmp[i]=fd_eval(updaters[i],inner_env);
        else tmp[i]=fd_incref(vals[i]);
        if (FD_ABORTED(tmp[i])) {
          /* GC the updated values you've generated so far.
             Note that tmp[i] (the exception) is not freed. */
          int j=0; while (j<i) {fd_decref(tmp[j]); j++;}
          /* Free the temporary arrays if neccessary */
          if (n>16) {u8_free(tmp); u8_free(updaters);}
          /* Return the error result, adding the expr and environment. */
          return return_error_env(tmp[i],":DO",inner_env);}
        else i++;}
      /* Now, free the current values and replace them with the values
         from tmp[]. */
      i=0; while (i < n) {
        fdtype val=vals[i];
        if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
          fd_decref(val);}
        vals[i]=tmp[i];
        i++;}
      /* Free the testval and evaluate it again. */
      fd_decref(testval);
      if (envstruct.env_copy) {
        fd_recycle_environment(envstruct.env_copy);
        envstruct.env_copy=NULL;}
      testval=fd_eval(testexpr,inner_env);
      if (FD_ABORTED(testval)) {
        /* If necessary, free the temporary arrays. */
        if (n>16) {u8_free(tmp); u8_free(updaters);}
        return return_error_env(testval,":DO",inner_env);}}
    /* Now we're done, so we set result to testval. */
    result=testval;
    if (FD_PAIRP(FD_CDR(exitexprs))) {
      fd_decref(result);
      result=eval_body(":DO",exitexprs,1,inner_env);}
    /* Free the environment. */
    free_environment(&envstruct);
    if (n>16) {u8_free(tmp); u8_free(updaters);}
    return result;}
}

/* SPROCs */

FD_EXPORT fdtype fd_apply_sproc(struct FD_SPROC *fn,int n,fdtype *args)
{
  fdtype _vals[6], *vals=_vals, lexpr_arg=FD_EMPTY_LIST, result=FD_VOID;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  int n_vars=fn->sproc_n_vars;
  /* We're optimizing to avoid GC (and thread contention) for the
     simple case where the arguments exactly match the argument list.
     Essentially, we use the args vector as the values vector of
     the SCHEMAP used for binding.  The problem is when the arguments
     don't match the number of arguments (lexprs or optionals).  In this
     case we set free_env=1 and just use a regular environment where
     all the values are incref'd.  */
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  bindings.table_schema=fn->sproc_vars;
  bindings.table_size=n_vars;
  bindings.schemap_onstack=1;
  fd_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_bindings=FDTYPE_CONS(&bindings);
  envstruct.env_exports=FD_VOID;
  envstruct.env_parent=fn->sproc_env; envstruct.env_copy=NULL;
  if (n_vars>6) vals=u8_alloc_n(fn->sproc_n_vars,fdtype);
  bindings.schema_values=vals;
  if (fn->fcn_arity>0) {
    if (n<fn->fcn_min_arity) {
      fd_destroy_rwlock(&(bindings.table_rwlock));
      return fd_err(fd_TooFewArgs,fn->fcn_name,NULL,FD_VOID);}
    else if (n>fn->fcn_arity) {
      fd_destroy_rwlock(&(bindings.table_rwlock));
      return fd_err(fd_TooManyArgs,fn->fcn_name,NULL,FD_VOID);}
    else {
      /* This code handles argument defaults for sprocs */
      int i=0;
      if (FD_PAIRP(fn->sproc_arglist)) {
        FD_DOLIST(arg,fn->sproc_arglist)
          if (i<n) {
            fdtype val=args[i];
            if ((val==FD_DEFAULT_VALUE)&&(FD_PAIRP(arg))&&
                (FD_PAIRP(FD_CDR(arg)))) {
              fdtype default_expr=FD_CADR(arg);
              fdtype default_value=fd_eval(default_expr,fn->sproc_env);
              vals[i]=default_value;}
            else if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val)))
              vals[i]=fd_incref(val);
            else vals[i]=val;
            i++;}
          else if ((FD_PAIRP(arg)) && (FD_PAIRP(FD_CDR(arg)))) {
            /* This code handles argument defaults for sprocs */
            fdtype default_expr=FD_CADR(arg);
            fdtype default_value=fd_eval(default_expr,fn->sproc_env);
            vals[i]=default_value;
            i++;}
          else vals[i++]=FD_VOID;}
      else if (FD_RAILP(fn->sproc_arglist)) {
        struct FD_VECTOR *v=FD_GET_CONS(fn->sproc_arglist,fd_rail_type,fd_vector);
        int len=v->fd_veclen; fdtype *dflts=v->fd_vecelts;
        while (i<len) {
          fdtype val=args[i];
          if ((val==FD_DEFAULT_VALUE)&&(dflts))  {
            fdtype default_expr=dflts[i];
            fdtype default_value=fd_eval(default_expr,fn->sproc_env);
            if (FD_VOIDP(default_value)) vals[i]=val;
            else vals[i]=default_value;}
          else if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
            vals[i]=fd_incref(val);}
          else vals[i]=val;
          i++;}
        while (i<n_vars) vals[i++]=FD_VOID;
        assert(i==fn->sproc_n_vars);}}}
  else if (fn->fcn_arity==0) {}
  else { /* We have a lexpr */
    int i=0, j=n-1;
    {FD_DOLIST(arg,fn->sproc_arglist)
       if (i<n) {
         fdtype val=args[i];
         if ((val==FD_DEFAULT_VALUE)&&(FD_PAIRP(arg))&&
             (FD_PAIRP(FD_CDR(arg)))) {
           /* This code handles argument defaults for sprocs */
           fdtype default_expr=FD_CADR(arg);
           fdtype default_value=fd_eval(default_expr,fn->sproc_env);
           vals[i]=default_value; i++;}
         else if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val)))
           vals[i]=fd_incref(val);
         else vals[i]=val;
         i++;}
       else if ((FD_PAIRP(arg)) && (FD_PAIRP(FD_CDR(arg)))) {
         /* This code handles argument defaults for sprocs */
         fdtype default_expr=FD_CADR(arg);
         fdtype default_value=fd_eval(default_expr,fn->sproc_env);
         vals[i]=default_value; i++;}
       else {vals[i]=FD_VOID; i++;}}
    while (j >= i) {
      lexpr_arg=fd_conspair(fd_incref(args[j]),lexpr_arg);
      j--;}
    fd_incref(lexpr_arg);
    vals[i]=lexpr_arg;}
  /* If we're synchronized, lock the mutex. */
  if (fn->sproc_synchronized) u8_lock_mutex(&(fn->sproc_lock));
  result=eval_body(":SPROC",fn->sproc_body,0,&envstruct);
  if (fn->sproc_synchronized) result=fd_finish_call(result);
  if (FD_THROWP(result)) {}
  else if (FD_ABORTED(result))
    u8_current_exception->u8x_details=sproc_id(fn);
  else {}
  /* If we're synchronized, unlock the mutex. */
  if (fn->sproc_synchronized) u8_unlock_mutex(&(fn->sproc_lock));
  fd_decref(lexpr_arg);
  if (envstruct.env_copy) {
    fd_recycle_environment(envstruct.env_copy);
    envstruct.env_copy=NULL;}
  free_environment(&envstruct);
  if (vals!=_vals) u8_free(vals);
  return result;
}

static fdtype sproc_applier(fdtype f,int n,fdtype *args)
{
  struct FD_SPROC *s=FD_GET_CONS(f,fd_sproc_type,struct FD_SPROC *);
  return fd_apply_sproc(s,n,args);
}

static fdtype _make_sproc(u8_string name,
                          fdtype arglist,fdtype body,fd_lispenv env,
                          int nd,int sync,
                          int incref,int copy_env)
{
  int i=0, n_vars=0, min_args=0;
  fdtype scan=arglist, *schema=NULL;
  struct FD_SPROC *s=u8_alloc(struct FD_SPROC);
  FD_INIT_CONS(s,fd_sproc_type);
  s->fcn_name=((name) ? (u8_strdup(name)) : (NULL));
  while (FD_PAIRP(scan)) {
    fdtype argspec=FD_CAR(scan);
    n_vars++; scan=FD_CDR(scan);
    if (FD_SYMBOLP(argspec)) min_args=n_vars;}
  if (FD_EMPTY_LISTP(scan)) {
    s->sproc_n_vars=s->fcn_arity=n_vars;}
  else {
    n_vars++; s->sproc_n_vars=n_vars; s->fcn_arity=-1;}
  s->fcn_min_arity=min_args; s->fcn_xcall=1; s->fcn_ndcall=nd;
  s->fcn_handler.fnptr=NULL;
  s->fcn_typeinfo=NULL;
  if (n_vars)
    s->sproc_vars=schema=u8_alloc_n((n_vars+1),fdtype);
  else s->sproc_vars=NULL;
  s->fcn_defaults=NULL; s->fcn_filename=NULL;
  if (incref) {
    s->sproc_body=fd_incref(body); s->sproc_arglist=fd_incref(arglist);}
  else {
    s->sproc_body=body; s->sproc_arglist=arglist;}
  if (env==NULL)
    s->sproc_env=env;
  else if ( (copy_env) || (FD_MALLOCD_CONSP(env)) )
    s->sproc_env=fd_copy_env(env);
  else s->sproc_env=fd_copy_env(env); /* s->sproc_env=env; */
  if (sync) {
    s->sproc_synchronized=1;
    fd_init_mutex(&(s->sproc_lock));}
  else s->sproc_synchronized=0;
  scan=arglist; i=0; while (FD_PAIRP(scan)) {
    fdtype argspec=FD_CAR(scan);
    if (FD_PAIRP(argspec)) {
      schema[i]=FD_CAR(argspec);}
    else {
      schema[i]=argspec;}
    i++; scan=FD_CDR(scan);}
  if (i<s->sproc_n_vars) schema[i]=scan;
  return FDTYPE_CONS(s);
}

static fdtype make_sproc(u8_string name,
                         fdtype arglist,fdtype body,fd_lispenv env,
                         int nd,int sync)
{
  return _make_sproc(name,arglist,body,env,nd,sync,1,1);
}

FD_EXPORT fdtype fd_make_sproc(u8_string name,
                               fdtype arglist,fdtype body,fd_lispenv env,
                               int nd,int sync)
{
  return make_sproc(name,arglist,body,env,nd,sync);
}


FD_EXPORT void recycle_sproc(struct FD_CONS *c)
{
  struct FD_SPROC *sproc=(struct FD_SPROC *)c;
  int mallocd=FD_MALLOCD_CONSP(c);
  if (sproc->fcn_name) u8_free(sproc->fcn_name);
  if (sproc->fcn_typeinfo) u8_free(sproc->fcn_typeinfo);
  if (sproc->fcn_defaults) u8_free(sproc->fcn_defaults);
  fd_decref(sproc->sproc_arglist); fd_decref(sproc->sproc_body);
  u8_free(sproc->sproc_vars);
  if (sproc->sproc_env->env_copy) {
    fd_decref((fdtype)(sproc->sproc_env->env_copy));
    /* fd_recycle_environment(sproc->sproc_env->env_copy); */
  }
  if (sproc->sproc_synchronized)
    fd_destroy_mutex(&(sproc->sproc_lock));
  if (sproc->fcn_filename) u8_free(sproc->fcn_filename);
  if (mallocd) {
    memset(sproc,0,sizeof(struct FD_SPROC));
    u8_free(sproc);}
}

static int unparse_sproc(u8_output out,fdtype x)
{
  struct FD_SPROC *sproc=FD_GET_CONS(x,fd_sproc_type,struct FD_SPROC *);
  fdtype arglist=sproc->sproc_arglist;
  unsigned long long addr=(unsigned long long) sproc;
  u8_string codes=
    (((sproc->sproc_synchronized)&&(sproc->fcn_ndcall))?("∀∥"):
     (sproc->sproc_synchronized)?("∥"):
     (sproc->fcn_ndcall)?("∀"):(""));
  if (sproc->fcn_name)
    u8_printf(out,"#<λ%s%s",codes,sproc->fcn_name);
  else u8_printf(out,"#<λ%s0x%04x",codes,((addr>>2)%0x10000));
  if (FD_PAIRP(arglist)) {
    int first=1; fdtype scan=sproc->sproc_arglist;
    fdtype spec=FD_VOID, arg=FD_VOID;
    u8_putc(out,'(');
    while (FD_PAIRP(scan)) {
      if (first) first=0; else u8_putc(out,' ');
      spec=FD_CAR(scan);
      arg=FD_SYMBOLP(spec)?(spec):(FD_PAIRP(spec))?(FD_CAR(spec)):(FD_VOID);
      if (FD_SYMBOLP(arg))
        u8_puts(out,FD_SYMBOL_NAME(arg));
      else u8_puts(out,"??");
      if (FD_PAIRP(spec)) u8_putc(out,'?');
      scan=FD_CDR(scan);}
    if (FD_EMPTY_LISTP(scan))
      u8_putc(out,')');
    else if (FD_SYMBOLP(scan))
      u8_printf(out,"%s…)",FD_SYMBOL_NAME(scan));
    else u8_printf(out,"…%q…)",scan);}
  else if (FD_EMPTY_LISTP(arglist))
    u8_puts(out,"()");
  else if (FD_SYMBOLP(arglist))
    u8_printf(out,"(%s…)",FD_SYMBOL_NAME(arglist));
  else u8_printf(out,"(…%q…)",arglist);
  if (!(sproc->fcn_name))
    u8_printf(out," #!0x%llx",(unsigned long long)sproc);
  if (sproc->fcn_filename)
    u8_printf(out," '%s'>",sproc->fcn_filename);
  else u8_puts(out,">");
  return 1;
}

static int *copy_intvec(int *vec,int n,int *into)
{
  int *dest=(into)?(into):(u8_alloc_n(n,int));
  int i=0; while (i<n) {
    dest[i]=vec[i]; i++;}
  return dest;
}

FD_EXPORT fdtype copy_sproc(struct FD_CONS *c,int flags)
{
  struct FD_SPROC *sproc=(struct FD_SPROC *)c;
  if (sproc->sproc_synchronized) {
    fdtype sp=(fdtype)sproc;
    fd_incref(sp);
    return sp;}
  else {
    struct FD_SPROC *fresh=u8_alloc(struct FD_SPROC);
    int n_args=sproc->sproc_n_vars+1, arity=sproc->fcn_arity;
    memcpy(fresh,sproc,sizeof(struct FD_SPROC));

    /* This sets a new reference count or declares it static */
    FD_INIT_CONS(fresh,fd_sproc_type);

    if (sproc->fcn_name) fresh->fcn_name=u8_strdup(sproc->fcn_name);
    if (sproc->fcn_filename)
      fresh->fcn_filename=u8_strdup(sproc->fcn_filename);
    if (sproc->fcn_typeinfo)
      fresh->fcn_typeinfo=copy_intvec(sproc->fcn_typeinfo,arity,NULL);

    fresh->sproc_arglist=fd_copier(sproc->sproc_arglist,flags);
    fresh->sproc_body=fd_copier(sproc->sproc_body,flags);
    if (sproc->sproc_vars)
      fresh->sproc_vars=fd_copy_vec(sproc->sproc_vars,n_args,NULL,flags);
    if (sproc->fcn_defaults)
      fresh->fcn_defaults=fd_copy_vec(sproc->fcn_defaults,arity,NULL,flags);

    if (fresh->sproc_synchronized)
      fd_init_mutex(&(fresh->sproc_lock));

    if (U8_BITP(flags,FD_STATIC_COPY)) {
      FD_MAKE_CONS_STATIC(fresh);}

    return (fdtype) fresh;}
}

/* Macros */

FD_EXPORT fdtype fd_make_macro(u8_string name,fdtype xformer)
{
  int xftype=FD_PRIM_TYPE(xformer);
  if ((xftype<FD_TYPE_MAX) && (fd_applyfns[xftype])) {
    struct FD_MACRO *s=u8_alloc(struct FD_MACRO);
    FD_INIT_CONS(s,fd_macro_type);
    s->fd_macro_name=((name) ? (u8_strdup(name)) : (NULL));
    s->fd_macro_transformer=fd_incref(xformer);
    return FDTYPE_CONS(s);}
  else return fd_err(fd_InvalidMacro,NULL,name,xformer);
}

static fdtype macro_handler(fdtype expr,fd_lispenv env)
{
  if ((FD_PAIRP(expr)) && (FD_PAIRP(FD_CDR(expr))) &&
      (FD_SYMBOLP(FD_CADR(expr))) &&
      (FD_PAIRP(FD_CDR(FD_CDR(expr))))) {
    fdtype name=FD_CADR(expr), body=FD_CDR(FD_CDR(expr));
    fdtype lambda_form=
      fd_conspair(lambda_symbol,
                  fd_conspair(fd_make_list(1,name),fd_incref(body)));
    fdtype transformer=fd_eval(lambda_form,env);
    fdtype macro=fd_make_macro(FD_SYMBOL_NAME(name),transformer);
    fd_decref(lambda_form); fd_decref(transformer);
    return macro;}
  else return fd_err(fd_SyntaxError,"MACRO",NULL,expr);
}

FD_EXPORT void recycle_macro(struct FD_CONS *c)
{
  struct FD_MACRO *mproc=(struct FD_MACRO *)c;
  if (mproc->fd_macro_name) u8_free(mproc->fd_macro_name);
  fd_decref(mproc->fd_macro_transformer);
  if (FD_MALLOCD_CONSP(c)) u8_free(mproc);
}

static int unparse_macro(u8_output out,fdtype x)
{
  struct FD_MACRO *mproc=FD_GET_CONS(x,fd_macro_type,struct FD_MACRO *);
  if (mproc->fd_macro_name)
    u8_printf(out,"#<MACRO %s #!%x>",
              mproc->fd_macro_name,(unsigned long)mproc);
  else u8_printf(out,"#<MACRO #!%x>",(unsigned long)mproc);
  return 1;
}

/* SPROC generators */

static fdtype lambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype arglist=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"LAMBDA",NULL,expr);
  if (FD_RAILP(body)) {
    fd_incref(arglist);
    return _make_sproc(NULL,arglist,body,env,0,0,0,0);}
  else return make_sproc(NULL,arglist,body,env,0,0);
}

static fdtype ambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype arglist=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"AMBDA",NULL,expr);
  if (FD_RAILP(body)) {
    fd_incref(arglist);
    return _make_sproc(NULL,arglist,body,env,1,0,0,0);}
  else return make_sproc(NULL,arglist,body,env,1,0);
}

static fdtype nambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype name_expr=fd_get_arg(expr,1), name;
  fdtype arglist=fd_get_arg(expr,2);
  fdtype body=fd_get_body(expr,3);
  u8_string namestring=NULL;
  if ((FD_VOIDP(name_expr))||(FD_VOIDP(arglist)))
    return fd_err(fd_TooFewExpressions,"NAMBDA",NULL,expr);
  else name=fd_eval(name_expr,env);
  if (FD_SYMBOLP(name)) namestring=FD_SYMBOL_NAME(name);
  else if (FD_STRINGP(name)) namestring=FD_STRDATA(name);
  else return fd_type_error("procedure name (string or symbol)",
                            "nambda_handler",name);
  if (FD_RAILP(body)) {
    fd_incref(arglist);
    return _make_sproc(namestring,arglist,body,env,1,0,0,0);}
  else return make_sproc(namestring,arglist,body,env,1,0);
}

static fdtype slambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype arglist=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"SLAMBDA",NULL,expr);
 if (FD_RAILP(body)) {
    fd_incref(arglist);
    return _make_sproc(NULL,arglist,body,env,0,1,0,0);}
  else return make_sproc(NULL,arglist,body,env,0,1);
}

static fdtype sambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype arglist=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"SLAMBDA",NULL,expr);
 if (FD_RAILP(body)) {
    fd_incref(arglist);
    return _make_sproc(NULL,arglist,body,env,1,1,0,0);}
  else return make_sproc(NULL,arglist,body,env,1,1);
}

static fdtype thunk_handler(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1);
  if (FD_RAILP(body))
    return _make_sproc(NULL,FD_EMPTY_LIST,body,env,0,0,0,0);
  else return make_sproc(NULL,FD_EMPTY_LIST,body,env,0,0);
}

/* DEFINE */

static fdtype define_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE",NULL,expr);
  else if (FD_SYMBOLP(var)) {
    fdtype val_expr=fd_get_arg(expr,2);
    if (FD_VOIDP(val_expr))
      return fd_err(fd_TooFewExpressions,"DEFINE",NULL,expr);
    else {
      fdtype value=fd_eval(val_expr,env);
      if (FD_ABORTED(value)) return value;
      else if (fd_bind_value(var,value,env)) {
        fdtype fvalue=(FD_FCNIDP(value))?(fd_fcnid_ref(value)):(value);
        if (FD_SPROCP(fvalue)) {
          struct FD_SPROC *s=(fd_sproc) fvalue;
          if (s->fcn_filename==NULL) {
            u8_string sourcebase=fd_sourcebase();
            if (sourcebase) s->fcn_filename=u8_strdup(sourcebase);}}
        fd_decref(value);
        return FD_VOID;}
      else {
        fd_decref(value);
        return fd_err(fd_BindError,"DEFINE",FD_SYMBOL_NAME(var),var);}}}
  else if (FD_PAIRP(var)) {
    fdtype fn_name=FD_CAR(var), args=FD_CDR(var);
    fdtype body=fd_get_body(expr,2);
    if (!(FD_SYMBOLP(fn_name)))
      return fd_err(fd_NotAnIdentifier,"DEFINE",NULL,fn_name);
    else {
      fdtype value=make_sproc(FD_SYMBOL_NAME(fn_name),args,body,env,0,0);
      if (FD_ABORTED(value)) return value;
      else if (fd_bind_value(fn_name,value,env)) {
        fdtype fvalue=(FD_FCNIDP(value))?(fd_fcnid_ref(value)):(value);
        if (FD_SPROCP(fvalue)) {
          struct FD_SPROC *s=(fd_sproc)fvalue;
          if (s->fcn_filename==NULL) {
            u8_string sourcebase=fd_sourcebase();
            if (sourcebase) s->fcn_filename=u8_strdup(sourcebase);}}
        fd_decref(value);
        return FD_VOID;}
      else {
        fd_decref(value);
        return fd_err(fd_BindError,"DEFINE",FD_SYMBOL_NAME(fn_name),var);}}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE",NULL,var);
}

static fdtype defslambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-SYNCHRONIZED",NULL,expr);
  else if (FD_SYMBOLP(var))
    return fd_err(fd_BadDefineForm,"DEFINE-SYNCHRONIZED",NULL,expr);
  else if (FD_PAIRP(var)) {
    fdtype fn_name=FD_CAR(var), args=FD_CDR(var);
    if (!(FD_SYMBOLP(fn_name)))
      return fd_err(fd_NotAnIdentifier,"DEFINE-SYNCHRONIZED",NULL,fn_name);
    else {
      fdtype body=fd_get_body(expr,2);
      fdtype value=make_sproc(FD_SYMBOL_NAME(fn_name),args,body,env,0,1);
      if (FD_ABORTED(value))
        return value;
      else if (fd_bind_value(fn_name,value,env)) {
        fdtype opvalue=(FD_FCNIDP(value))?(fd_fcnid_ref(value)):(value);
        if (FD_SPROCP(opvalue)) {
          struct FD_SPROC *s=(fd_sproc)opvalue;
          if (s->fcn_filename==NULL) {
            u8_string sourcebase=fd_sourcebase();
            if (sourcebase) s->fcn_filename=u8_strdup(sourcebase);}}
        fd_decref(value);
        return FD_VOID;}
      else {
        fd_decref(value);
        return fd_err(fd_BindError,"DEFINE-SYNCHRONIZED",
                      FD_SYMBOL_NAME(var),var);}}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE-SYNCHRONIZED",NULL,var);
}

static fdtype defambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-AMB",NULL,expr);
  else if (FD_SYMBOLP(var))
    return fd_err(fd_BadDefineForm,"DEFINE-AMB",FD_SYMBOL_NAME(var),expr);
  else if (FD_PAIRP(var)) {
    fdtype fn_name=FD_CAR(var), args=FD_CDR(var);
    fdtype body=fd_get_body(expr,2);
    if (!(FD_SYMBOLP(fn_name)))
      return fd_err(fd_NotAnIdentifier,"DEFINE-AMB",NULL,fn_name);
    else {
      fdtype value=make_sproc(FD_SYMBOL_NAME(fn_name),args,body,env,1,0);
      if (FD_ABORTED(value)) return value;
      else if (fd_bind_value(fn_name,value,env)) {
        fdtype opvalue=fd_fcnid_ref(value);
        if (FD_SPROCP(opvalue)) {
          struct FD_SPROC *s=(fd_sproc)opvalue;
          if (s->fcn_filename==NULL) {
            u8_string sourcebase=fd_sourcebase();
            if (sourcebase) s->fcn_filename=u8_strdup(sourcebase);}}
        fd_decref(value);
        return FD_VOID;}
      else {
        fd_decref(value);
        return fd_err(fd_BindError,"DEFINE-AMB",FD_SYMBOL_NAME(fn_name),var);}}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE-AMB",NULL,var);
}

/* DEFINE-LOCAL */

/* This defines an identifier in the local environment to
   the value it would have anyway by environment inheritance.
   This is helpful if it was to rexport it, for example. */
static fdtype define_local_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-LOCAL",NULL,expr);
  else if (FD_SYMBOLP(var)) {
    fdtype inherited=fd_symeval(var,env->env_parent);
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
static fdtype define_init_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1);
  fdtype init_expr=fd_get_arg(expr,2);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-LOCAL",NULL,expr);
  else if (FD_VOIDP(init_expr))
    return fd_err(fd_TooFewExpressions,"DEFINE-LOCAL",NULL,expr);
  else if (FD_SYMBOLP(var)) {
    fdtype current=fd_get(env->env_bindings,var,FD_VOID);
    if (FD_ABORTED(current)) return current;
    else if (!(FD_VOIDP(current))) {
      fd_decref(current);
      return FD_VOID;}
    else {
      fdtype init_value=fd_eval(init_expr,env); int bound=0;
      if (FD_ABORTED(init_value)) return init_value;
      else bound=fd_bind_value(var,init_value,env);
      if (bound>0) {
        fd_decref(init_value);
        return FD_VOID;}
      else if (bound<0) return FD_ERROR_VALUE;
      else return fd_err(fd_BindError,"DEFINE-INIT",FD_SYMBOL_NAME(var),var);}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE_INIT",NULL,var);
}

/* Extended apply */

static fdtype tail_symbol;

FD_EXPORT
/* fd_xapply_sproc:
     Arguments: a pointer to an sproc, a void* data pointer, and a function
      of a void* pointer and a dtype pointer
     Returns: the application result
  This uses an external function to get parameter values from some
  other data structure (cast as a void* pointer).  This is used, for instance,
  to expose CGI data fields as arguments to a main function, or to
  apply XML attributes and elements similarly. */
fdtype fd_xapply_sproc
  (struct FD_SPROC *fn,void *data,fdtype (*getval)(void *,fdtype))
{
  int i=0;
  fdtype _vals[12], *vals=_vals, arglist=fn->sproc_arglist, result=FD_VOID;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.table_schema=fn->sproc_vars;
  bindings.table_size=fn->sproc_n_vars;
  fd_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_bindings=FDTYPE_CONS(&bindings);
  envstruct.env_exports=FD_VOID;
  envstruct.env_parent=fn->sproc_env; envstruct.env_copy=NULL;
  if (fn->sproc_n_vars>=12)
    bindings.schema_values=vals=u8_alloc_n(fn->sproc_n_vars,fdtype);
  else bindings.schema_values=vals=_vals;
  while (FD_PAIRP(arglist)) {
    fdtype argspec=FD_CAR(arglist), argname=FD_VOID, argval;
    if (FD_SYMBOLP(argspec)) argname=argspec;
    else if (FD_PAIRP(argspec)) argname=FD_CAR(argspec);
    if (!(FD_SYMBOLP(argname)))
      return fd_err(fd_BadArglist,fn->fcn_name,NULL,fn->sproc_arglist);
    argval=getval(data,argname);
    if (FD_ABORTED(argval)) {
      int j=0; while (j<i) {
        fdtype val=vals[j++];
        if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val)))
          fd_decref(val);}
      if (vals!=_vals) u8_free(vals);
      return argval;}
    else if (((FD_VOIDP(argval))||(argval==FD_DEFAULT_VALUE)) &&
             (FD_PAIRP(argspec)) && (FD_PAIRP(FD_CDR(argspec)))) {
      fdtype default_expr=FD_CADR(argspec);
      fdtype default_value=fd_eval(default_expr,fn->sproc_env);
      vals[i++]=default_value;}
    else vals[i++]=argval;
    arglist=FD_CDR(arglist);}
  /* This means we have a lexpr arg. */
  if (i<fn->sproc_n_vars) {
    /* We look for the arg directly and then we use the special
       tail_symbol (%TAIL) to get something. */
    fdtype argval=getval(data,arglist);
    if (FD_VOIDP(argval)) argval=getval(data,tail_symbol);
    if (FD_ABORTED(argval)) {
      int j=0; while (j<i) {
        fdtype val=vals[j++];
        if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
          fd_decref(val);}}
      if (vals!=_vals) u8_free(vals);
      return argval;}
    else vals[i++]=argval;}
  assert(i==fn->sproc_n_vars);
  /* If we're synchronized, lock the mutex. */
  if (fn->sproc_synchronized) u8_lock_mutex(&(fn->sproc_lock));
  result=eval_body(":XPROC",fn->sproc_body,0,&envstruct);
  /* if (fn->sproc_synchronized) result=fd_finish_call(result); */
  /* We always finish tail calls here */
  result=fd_finish_call(result);
  if (FD_THROWP(result)) {}
  else if ((FD_ABORTED(result)) && (fn->fcn_filename))
    u8_current_exception->u8x_details=sproc_id(fn);
  else {}
  /* If we're synchronized, unlock the mutex. */
  if (fn->sproc_synchronized)
    u8_unlock_mutex(&(fn->sproc_lock));
  fd_destroy_rwlock(&(bindings.table_rwlock));
  if (envstruct.env_copy) {
    fd_recycle_environment(envstruct.env_copy);
    envstruct.env_copy=NULL;}
  free_environment(&envstruct);
  if (vals!=_vals) u8_free(vals);
  return result;
}

static fdtype tablegetval(void *obj,fdtype var)
{
  fdtype tbl=(fdtype)obj;
  return fd_get(tbl,var,FD_VOID);
}

static fdtype xapply_prim(fdtype proc,fdtype obj)
{
  struct FD_SPROC *sproc=FD_GET_CONS(proc,fd_sproc_type,struct FD_SPROC *);
  if (!(FD_TABLEP(obj)))
    return fd_type_error("table","xapply_prim",obj);
  return fd_xapply_sproc(sproc,(void *)obj,tablegetval);
}

/* IPEVAL binding */

#if FD_IPEVAL_ENABLED
struct IPEVAL_BINDSTRUCT {
  int n_bindings; fdtype *vals;
  fdtype valexprs; fd_lispenv env;};

static int ipeval_let_step(struct IPEVAL_BINDSTRUCT *bs)
{
  int i=0, n=bs->n_bindings;
  fdtype *bindings=bs->vals, scan=bs->valexprs;
  fd_lispenv env=bs->sproc_env;
  while (i<n) {
    fd_decref(bindings[i]); bindings[i++]=FD_VOID;}
  i=0; while (FD_PAIRP(scan)) {
    fdtype binding=FD_CAR(scan), val_expr=FD_CADR(binding);
    fdtype val=fd_eval(val_expr,env);
    if (FD_ABORTED(val)) fd_interr(val);
    else bindings[i++]=val;
    scan=FD_CDR(scan);}
  return 1;
}

static int ipeval_letstar_step(struct IPEVAL_BINDSTRUCT *bs)
{
  int i=0, n=bs->n_bindings;
  fdtype *bindings=bs->vals, scan=bs->valexprs;
  fd_lispenv env=bs->sproc_env;
  while (i<n) {
    fd_decref(bindings[i]); bindings[i++]=FD_UNBOUND;}
  i=0; while (FD_PAIRP(scan)) {
    fdtype binding=FD_CAR(scan), val_expr=FD_CADR(binding);
    fdtype val=fd_eval(val_expr,env);
    if (FD_ABORTED(val)) fd_interr(val);
    else bindings[i++]=val;
    scan=FD_CDR(scan);}
  return 1;
}

static int ipeval_let_binding
  (int n,fdtype *vals,fdtype bindexprs,fd_lispenv env)
{
  struct IPEVAL_BINDSTRUCT bindstruct;
  bindstruct.n_bindings=n; bindstruct.vals=vals;
  bindstruct.valexprs=bindexprs; bindstruct.env=env;
  return fd_ipeval_call((fd_ipevalfn)ipeval_let_step,&bindstruct);
}

static int ipeval_letstar_binding
  (int n,fdtype *vals,fdtype bindexprs,fd_lispenv bind_env,fd_lispenv env)
{
  struct IPEVAL_BINDSTRUCT bindstruct;
  bindstruct.n_bindings=n; bindstruct.vals=vals;
  bindstruct.valexprs=bindexprs; bindstruct.env=env;
  return fd_ipeval_call((fd_ipevalfn)ipeval_letstar_step,&bindstruct);
}

static fdtype letq_handler(fdtype expr,fd_lispenv env)
{
  fdtype bindexprs=fd_get_arg(expr,1), result=FD_VOID;
  int n;
  if (FD_VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"LET",NULL,expr);
  else if ((n=check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    struct FD_ENVIRONMENT *inner_env=make_dynamic_env(n,env);
    fdtype bindings=inner_env->env_bindings;
    struct FD_SCHEMAP *sm=(struct FD_SCHEMAP *)bindings;
    fdtype *vars=sm->table_schema, *vals=sm->schema_values;
    int i=0; fdtype scan=bindexprs; while (i<n) {
      fdtype bind_expr=FD_CAR(scan), var=FD_CAR(bind_expr);
      vars[i]=var; vals[i]=FD_VOID; scan=FD_CDR(scan); i++;}
    if (ipeval_let_binding(n,vals,bindexprs,env)<0) {
      fdtype errobj=FD_ERROR_VALUE;
      return return_error_env(errobj,":LETQ",env);}
    {FD_DOBODY(bodyexpr,expr,2) {
      fd_decref(result);
      result=fasteval(bodyexpr,inner_env);
      if (FD_ABORTED(result))
        return return_error_env(result,":LETQ",inner_env);}}
    free_environment(inner_env);
    return result;}
}

static fdtype letqstar_handler(fdtype expr,fd_lispenv env)
{
  fdtype bindexprs=fd_get_arg(expr,1), result=FD_VOID;
  int n;
  if (FD_VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"LET*",NULL,expr);
  else if ((n=check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    struct FD_ENVIRONMENT *inner_env=make_dynamic_env(n,env);
    fdtype bindings=inner_env->env_bindings;
    struct FD_SCHEMAP *sm=(struct FD_SCHEMAP *)bindings;
    fdtype *vars=sm->table_schema, *vals=sm->schema_values;
    int i=0; fdtype scan=bindexprs; while (i<n) {
      fdtype bind_expr=FD_CAR(scan), var=FD_CAR(bind_expr);
      vars[i]=var; vals[i]=FD_UNBOUND; scan=FD_CDR(scan); i++;}
    if (ipeval_letstar_binding(n,vals,bindexprs,inner_env,inner_env)<0) {
      fdtype errobj=FD_ERROR_VALUE;
      return return_error_env(errobj,":LETQ*",inner_env);}
    {FD_DOBODY(bodyexpr,expr,2) {
      fd_decref(result);
      result=fasteval(bodyexpr,inner_env);
      if (FD_ABORTED(result)) {
        return return_error_env(result,":LETQ*",inner_env);}}}
    if (inner_env->env_copy) free_environment(inner_env->env_copy);
    return result;}
}

#endif

/* Extend fcnid parsing to incluce functional compounds */


static int unparse_extended_fcnid(u8_output out,fdtype x)
{
  fdtype lp=fd_fcnid_ref(x);
  if (FD_PRIM_TYPEP(lp,fd_sproc_type)) {
    struct FD_SPROC *sproc=FD_GET_CONS(lp,fd_sproc_type,struct FD_SPROC *);
    unsigned long long addr=(unsigned long long) sproc;
    fdtype arglist=sproc->sproc_arglist;
    u8_string codes=
      (((sproc->sproc_synchronized)&&(sproc->fcn_ndcall))?("∀∥"):
       (sproc->sproc_synchronized)?("∥"):
       (sproc->fcn_ndcall)?("∀"):(""));
    if (sproc->fcn_name)
      u8_printf(out,"#<~%d<λ%s%s",
                FD_GET_IMMEDIATE(x,fd_fcnid_type),
                codes,sproc->fcn_name);
    else u8_printf(out,"#<~%d<λ%s0x%04x",
                   FD_GET_IMMEDIATE(x,fd_fcnid_type),
                   codes,((addr>>2)%0x10000));
    if (FD_PAIRP(arglist)) {
      int first=1; fdtype scan=sproc->sproc_arglist;
      fdtype spec=FD_VOID, arg=FD_VOID;
      u8_putc(out,'(');
      while (FD_PAIRP(scan)) {
        if (first) first=0; else u8_putc(out,' ');
        spec=FD_CAR(scan);
        arg=FD_SYMBOLP(spec)?(spec):(FD_PAIRP(spec))?(FD_CAR(spec)):(FD_VOID);
        if (FD_SYMBOLP(arg))
          u8_puts(out,FD_SYMBOL_NAME(arg));
        else u8_puts(out,"??");
        if (FD_PAIRP(spec)) u8_putc(out,'?');
        scan=FD_CDR(scan);}
      if (FD_EMPTY_LISTP(scan))
        u8_putc(out,')');
      else if (FD_SYMBOLP(scan))
        u8_printf(out,"%s…)",FD_SYMBOL_NAME(scan));
      else u8_printf(out,"…%q…)",scan);}
    else if (FD_EMPTY_LISTP(arglist))
      u8_puts(out,"()");
    else if (FD_SYMBOLP(arglist))
      u8_printf(out,"(%s…)",FD_SYMBOL_NAME(arglist));
    else u8_printf(out,"(…%q…)",arglist);
    if (!(sproc->fcn_name))
      u8_printf(out," #!0x%llx",(unsigned long long)sproc);
    if (sproc->fcn_filename)
      u8_printf(out," '%s'>>",sproc->fcn_filename);
    else u8_puts(out,">>");
    return 1;}
  else if (FD_PRIM_TYPEP(lp,fd_primfcn_type)) {
      struct FD_FUNCTION *fcn=(fd_function)lp;
      unsigned long long addr=(unsigned long long) fcn;
      u8_string name=fcn->fcn_name;
      u8_string filename=fcn->fcn_filename;
      u8_byte arity[16]=""; u8_byte codes[16]="";
      if ((filename)&&(filename[0]=='\0')) filename=NULL;
      if (name==NULL) name=fcn->fcn_name;
      if (fcn->fcn_ndcall) strcat(codes,"∀");
      if ((fcn->fcn_arity<0)&&(fcn->fcn_min_arity<0))
        strcat(arity,"…");
      else if (fcn->fcn_arity==fcn->fcn_min_arity)
        sprintf(arity,"[%d]",fcn->fcn_min_arity);
      else if (fcn->fcn_arity<0)
        sprintf(arity,"[%d,…]",fcn->fcn_min_arity);
      else sprintf(arity,"[%d,%d]",fcn->fcn_min_arity,fcn->fcn_arity);
      if (name)
        u8_printf(out,"#<~%d<%s%s%s%s%s%s>>",
                  FD_GET_IMMEDIATE(x,fd_fcnid_type),
                  codes,name,arity,U8OPTSTR("'",filename,"'"));
      else u8_printf(out,"#<~%d<Φ%s0x%04x%s #!0x%llx%s%s%s>>",
                     FD_GET_IMMEDIATE(x,fd_fcnid_type),
                     codes,((addr>>2)%0x10000),arity,
                     (unsigned long long) fcn,
                     arity,U8OPTSTR("'",filename,"'"));
      return 1;}
  else u8_printf(out,"#<~%ld %q>",FD_GET_IMMEDIATE(x,fd_fcnid_type),lp);
  return 1;
}

/* Initialization */

FD_EXPORT void fd_init_binders_c()
{
  u8_register_source_file(_FILEINFO);

  moduleid_symbol=fd_intern("%MODULEID");
  lambda_symbol=fd_intern("LAMBDA");
  tail_symbol=fd_intern("%TAIL");

#if FD_THREADS_ENABLED
  fd_init_mutex(&sset_lock);
#endif

  fd_macro_type=fd_register_cons_type(_("scheme syntactic macro"));

  fd_applyfns[fd_sproc_type]=sproc_applier;
  fd_functionp[fd_sproc_type]=1;

  fd_unparsers[fd_sproc_type]=unparse_sproc;
  fd_recyclers[fd_sproc_type]=recycle_sproc;

  fd_unparsers[fd_macro_type]=unparse_macro;
  fd_recyclers[fd_macro_type]=recycle_macro;

  fd_unparsers[fd_fcnid_type]=unparse_extended_fcnid;

  fd_defspecial(fd_scheme_module,"SET!",set_handler);
  fd_defspecial(fd_scheme_module,"SET+!",set_plus_handler);
  fd_defspecial(fd_scheme_module,"SSET!",sset_handler);

  fd_defspecial(fd_scheme_module,"LET",let_handler);
  fd_defspecial(fd_scheme_module,"LET*",letstar_handler);
  fd_defspecial(fd_scheme_module,"LAMBDA",lambda_handler);
  fd_defspecial(fd_scheme_module,"AMBDA",ambda_handler);
  fd_defspecial(fd_scheme_module,"NAMBDA",nambda_handler);
  fd_defspecial(fd_scheme_module,"SLAMBDA",slambda_handler);
  fd_defspecial(fd_scheme_module,"SAMBDA",sambda_handler);
  fd_defspecial(fd_scheme_module,"THUNK",thunk_handler);
  fd_defspecial(fd_scheme_module,"DEFINE",define_handler);
  fd_defspecial(fd_scheme_module,"DEFSLAMBDA",defslambda_handler);
  fd_defspecial(fd_scheme_module,"DEFAMBDA",defambda_handler);
  fd_defspecial(fd_scheme_module,"DEFINE-INIT",define_init_handler);
  fd_defspecial(fd_scheme_module,"DEFINE-LOCAL",define_local_handler);

  fd_defspecial(fd_scheme_module,"MACRO",macro_handler);

  fd_defspecial(fd_scheme_module,"DO",do_handler);

  fd_defspecial(fd_scheme_module,"DEFAULT!",set_default_handler);
  fd_defspecial(fd_scheme_module,"SETFALSE!",set_default_handler);
  fd_defspecial(fd_scheme_module,"BIND-DEFAULT!",bind_default_handler);

#if FD_IPEVAL_ENABLED
  fd_defspecial(fd_scheme_module,"LETQ",letq_handler);
  fd_defspecial(fd_scheme_module,"LETQ*",letqstar_handler);
#endif

  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("XAPPLY",xapply_prim,2,fd_sproc_type,FD_VOID,-1,FD_VOID));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
