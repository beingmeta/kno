/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] = 
  "$Id: binders.c,v 1.65 2006/01/31 13:47:24 haase Exp $";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"

fd_exception fd_BadArglist=_("Malformed argument list");
fd_exception fd_BindError=_("Can't bind variable");
fd_exception fd_BindSyntaxError=_("Bad binding expression");

fd_ptr_type fd_sproc_type, fd_macro_type;

static fdtype lambda_symbol;

#define copy_bindings(env) \
  ((env->copy) ? (fd_copy(env->copy->bindings)) : (fd_copy(env->bindings)))

static fdtype sproc_id(struct FD_SPROC *fn)
{
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  if (fn->name)
    u8_printf(&out,"%s:%s",fn->name,fn->filename);
  else u8_puts(&out,fn->filename);
  return fd_init_string(NULL,out.point-out.bytes,out.bytes);
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
    return fd_err(fd_TooFewExpressions,"SET!",NULL,expr);
  value=fasteval(val_expr,env);
  if (FD_EXCEPTIONP(value)) return value;
  else if (retval=fd_set_value(var,value,env)) {
    fd_decref(value);
    if (retval<0) return fd_erreify();
    else return FD_VOID;}
  else if (retval=fd_bind_value(var,value,env)) {
    fd_decref(value);
    if (retval<0) return fd_erreify();
    else return FD_VOID;}
  else return fd_err(fd_BindError,"SET!",NULL,var);
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
  if (FD_ABORTP(value))
    return value;
  else if (fd_add_value(var,value,env)) {}
  else if (fd_bind_value(var,value,env)) {}
  else return fd_err(fd_BindError,"SET+!",NULL,var);
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
    if (FD_VOIDP(val)) {
      fdtype value=fd_eval(value_expr,env);
      if (FD_ABORTP(value)) return value;
      if (fd_set_value(symbol,value,env)==0)
	fd_bind_value(symbol,value,env);
      fd_decref(value);
      return FD_VOID;}
    else {
      fd_decref(val); return FD_VOID;}}
}

/* Environment utilities */

static void free_environment(struct FD_ENVIRONMENT *env)
{
  if (env->copy) 
    fd_recycle_environment(env->copy);
  else {
    struct FD_SCHEMAP *sm=FD_XSCHEMAP(env->bindings);
    int i=0, n=FD_XSCHEMAP_SIZE(sm); fdtype *vals=sm->values;
    while (i < n) {fd_decref(vals[i]); i++;}}
}

static passerr_env(fdtype error,fd_lispenv env)
{
  fdtype bindings=copy_bindings(env);
  free_environment(env);
  return fd_passerr(error,bindings);
}

static int check_bindexprs(fdtype bindexprs,fdtype *why_not)
{
  int n=0;
  FD_DOLIST(bindexpr,bindexprs) {
    fdtype var=fd_get_arg(bindexpr,0);
    fdtype val_expr=fd_get_arg(bindexpr,1);
    if (FD_VOIDP(var)) {
      *why_not=fd_err(fd_BindSyntaxError,NULL,NULL,bindexpr);
      return -1;}
    else n++;}
  return n;
}

static fd_lispenv init_static_env
  (int n,fd_lispenv parent,
   struct FD_SCHEMAP *bindings,struct FD_ENVIRONMENT *envstruct,
   fdtype *vars,fdtype *vals)
{
  int i=0; while (i < n) {
    vars[i]=FD_VOID; vals[i]=FD_VOID; i++;}
  FD_INIT_STACK_CONS(bindings,fd_schemap_type);
  bindings->flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings->schema=vars;
  bindings->values=vals;
  bindings->size=n;
  u8_init_mutex(&(bindings->lock));
  envstruct->bindings=FDTYPE_CONS((bindings));
  envstruct->exports=FD_VOID;
  envstruct->parent=parent;
  FD_INIT_STACK_CONS(envstruct,fd_environment_type);
  envstruct->copy=NULL;
  return envstruct;
}

static fd_lispenv make_dynamic_env(int n,fd_lispenv parent)
{
  int i=0;
  struct FD_ENVIRONMENT *e=u8_malloc(sizeof(struct FD_ENVIRONMENT));
  fdtype *vars=u8_malloc(sizeof(fdtype)*n);
  fdtype *vals=u8_malloc(sizeof(fdtype)*n);
  fdtype schemap=fd_make_schemap(NULL,n,FD_SCHEMAP_PRIVATE,vars,vals,NULL);
  while (i<n) {vars[i]=FD_VOID; vals[i]=FD_VOID; i++;}
  FD_INIT_CONS(e,fd_environment_type);
  e->copy=e; e->bindings=schemap; e->exports=FD_VOID;
  e->parent=fd_copy_env(parent);
  return e;
}

static fd_lispenv make_dynamic_envx
  (fdtype bindexprs,fd_lispenv parent,fdtype *why_not)
{
  struct FD_ENVIRONMENT *e;
  fdtype *vars, *vals, schemap;
  int i=0, n=0;
  FD_DOLIST(expr,bindexprs) {
    fdtype var=fd_get_arg(expr,0);
    if (FD_VOIDP(var)) {
      *why_not=fd_err(fd_BindSyntaxError,NULL,NULL,bindexprs);
      return NULL;}
    else n++;}
  vars=u8_malloc(sizeof(fdtype)*n);
  {FD_DOLIST(expr,bindexprs) {vars[i++]=fd_get_arg(expr,0);}}
  schemap=fd_make_schemap(NULL,n,0,vars,NULL,NULL);
  e=u8_malloc(sizeof(struct FD_ENVIRONMENT));
  FD_INIT_CONS(e,fd_environment_type);
  e->copy=e; e->bindings=schemap; e->exports=FD_VOID;
  e->parent=fd_copy_env(parent);
  return e;
}
/* Simple binders */

static fdtype let_handler(fdtype expr,fd_lispenv env)
{
  fdtype bindexprs=fd_get_arg(expr,1), result=FD_VOID;
  fdtype body=fd_get_body(expr,2);
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
      bindings=inner_env->bindings; sm=(struct FD_SCHEMAP *)bindings;
      vars=sm->schema; vals=sm->values;}
    else {
      inner_env=init_static_env(n,env,&bindings,&envstruct,_vars,_vals);
      vars=_vars; vals=_vals;}
    {FD_DOLIST(bindexpr,bindexprs) {
      fdtype var=fd_get_arg(bindexpr,0);
      fdtype val_expr=fd_get_arg(bindexpr,1);
      fdtype value=fasteval(val_expr,env);
      if (FD_ABORTP(value)) {
	free_environment(inner_env);
	return value;}
      else {
	vars[i]=var; vals[i]=value; i++;}}}
    {FD_DOLIST(bodyexpr,body) {
      fd_decref(result);
      result=fasteval(bodyexpr,inner_env);
      if (FD_ABORTP(result))
	return passerr_env(result,inner_env);}}
    free_environment(inner_env);
    return result;}
}

static fdtype letstar_handler(fdtype expr,fd_lispenv env)
{
  fdtype bindexprs=fd_get_arg(expr,1), result=FD_VOID;
  fdtype body=fd_get_body(expr,2);
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
      bindings=inner_env->bindings; sm=(struct FD_SCHEMAP *)bindings;
      vars=sm->schema; vals=sm->values;}
    else {
      inner_env=init_static_env(n,env,&bindings,&envstruct,_vars,_vals);
      vars=_vars; vals=_vals;}
    {FD_DOLIST(bindexpr,bindexprs) {
      fdtype var=fd_get_arg(bindexpr,0);
      vars[j]=var; vals[j]=FD_UNBOUND; j++;}}
    {FD_DOLIST(bindexpr,bindexprs) {
      fdtype var=fd_get_arg(bindexpr,0);
      fdtype val_expr=fd_get_arg(bindexpr,1);
      fdtype value=fasteval(val_expr,inner_env);
      if (FD_EXCEPTIONP(value))
	return passerr_env(value,inner_env);
      else if (inner_env->copy) {
	fd_bind_value(var,value,inner_env->copy);
	fd_decref(value);}
      else {
	vars[i]=var; vals[i]=value; i++;}}}
    {FD_DOLIST(bodyexpr,body) {
      fd_decref(result);
      result=fasteval(bodyexpr,inner_env);
      if (FD_EXCEPTIONP(result)) {
	int i=0;
	return passerr_env(result,inner_env);}}}
    free_environment(inner_env);
    return result;}
}

/* DO */

static fdtype do_handler(fdtype expr,fd_lispenv env)
{
  fdtype bindexprs=fd_get_arg(expr,1);
  fdtype exitexprs=fd_get_arg(expr,2);
  fdtype testexpr=fd_get_arg(exitexprs,0), testval=FD_VOID;
  fdtype body=fd_get_body(expr,3);
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
      bindings=inner_env->bindings; sm=(struct FD_SCHEMAP *)bindings;
      vars=sm->schema; vals=sm->values;
      updaters=u8_malloc(sizeof(fdtype)*n);
      tmp=u8_malloc(sizeof(fdtype)*n);}
    else {
      inner_env=init_static_env(n,env,&bindings,&envstruct,_vars,_vals);
      vars=_vars; vals=_vals; updaters=_updaters; tmp=_tmp;}
    /* Do the initial bindings */
    {FD_DOLIST(bindexpr,bindexprs) {
      fdtype var=fd_get_arg(bindexpr,0);
      fdtype value_expr=fd_get_arg(bindexpr,1);
      fdtype update_expr=fd_get_arg(bindexpr,2);
      fdtype value=fd_eval(value_expr,env);
      if (FD_EXCEPTIONP(value)) {
	/* When there's an error here, there's no need to bind. */
	free_environment(inner_env);
	if (n>16) {u8_free(tmp); u8_free(updaters);}
	return value;}
      else {
	vars[i]=var; vals[i]=value; updaters[i]=update_expr;
	i++;}}}
    /* First test */
    testval=fd_eval(testexpr,inner_env);
    if (FD_EXCEPTIONP(testval)) {
      if (n>16) {u8_free(tmp); u8_free(updaters);}
      return passerr_env(testval,inner_env);}
    /* The iteration itself */
    while (FD_FALSEP(testval)) {
      int i=0;
      /* Execute the body */
      FD_DOLIST(bodyexpr,body) {
	fdtype result=fasteval(bodyexpr,inner_env);
	if (FD_EXCEPTIONP(result)) {
	  if (n>16) {u8_free(tmp); u8_free(updaters);}
	  return passerr_env(result,inner_env);}
	else fd_decref(result);}
      /* Do an update, storing new values in tmp[] to be consistent. */
      while (i < n) {
	if (!(FD_VOIDP(updaters[i])))
	  tmp[i]=fd_eval(updaters[i],inner_env);
	else tmp[i]=fd_incref(vals[i]);
	if (FD_EXCEPTIONP(tmp[i])) {
	  /* GC the updated values you've generated so far.
	     Note that tmp[i] (the exception) is not freed. */
	  int j=0; while (j<i) {fd_decref(tmp[j]); j++;}
	  /* Free the temporary arrays if neccessary */
	  if (n>16) {u8_free(tmp); u8_free(updaters);}
	  /* Return the error result, adding the expr and environment. */
	  return passerr_env(tmp[i],inner_env);}
	else i++;}
      /* Now, free the current values and replace them with the values
	 from tmp[]. */
      i=0; while (i < n) {
	fd_decref(vals[i]); vals[i]=tmp[i]; i++;}
      /* Free the testval and evaluate it again. */
      fd_decref(testval);
      if (envstruct.copy) {
	fd_recycle_environment(envstruct.copy);
	envstruct.copy=NULL;}
      testval=fd_eval(testexpr,inner_env);
      if (FD_EXCEPTIONP(testval)) {
	/* If necessary, free the temporary arrays. */
	if (n>16) {u8_free(tmp); u8_free(updaters);}
	return passerr_env(testval,inner_env);}}
    /* Now we're done, so we set result to testval. */
    result=testval;
    {FD_DOLIST(exitexpr,FD_CDR(exitexprs)) {
      fd_decref(result);
      result=fd_eval(exitexpr,inner_env);
      if (FD_EXCEPTIONP(result)) {
	if (n>16) {u8_free(tmp); u8_free(updaters);}
	return passerr_env(result,inner_env);} }}
    /* Free the environment. */
    free_environment(&envstruct);
    if (n>16) {u8_free(tmp); u8_free(updaters);}
    return result;}
}

/* SPROCs */

FD_EXPORT fdtype fd_apply_sproc(struct FD_SPROC *fn,int n,fdtype *args)
{
  int free_vals=0;
  fdtype _vals[6], *vals=_vals, lexpr_arg=FD_EMPTY_LIST, result=FD_VOID;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type);
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type);
  bindings.schema=fn->schema;
  bindings.size=fn->n_vars;
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  u8_init_mutex(&(bindings.lock));
  envstruct.bindings=FDTYPE_CONS(&bindings);
  envstruct.exports=FD_VOID;
  envstruct.parent=fn->env; envstruct.copy=NULL;
  if (fn->arity>0)
    if (n==fn->n_vars) bindings.values=args;
    else if (n<fn->min_arity) 
      return fd_err(fd_TooFewArgs,fn->name,NULL,FD_VOID);
    else if (n>fn->arity) 
      return fd_err(fd_TooManyArgs,fn->name,NULL,FD_VOID);
    else {
      /* This code handles argument defaults for sprocs */
      int i=0;
      free_vals=1; bindings.values=vals=
		     u8_malloc(sizeof(fdtype)*fn->n_vars); ;
      {FD_DOLIST(arg,fn->arglist)
	 if (i<n) {vals[i]=args[i]; i++;}
	 else if ((FD_PAIRP(arg)) && (FD_PAIRP(FD_CDR(arg)))) {
	   vals[i]=FD_CADR(arg); i++;}
	 else vals[i]=FD_VOID;}
      assert(i==fn->n_vars);}
  else { /* We have a lexpr */
    int i=0, j=n-1, lim=fn->n_vars-1;
    if (fn->n_vars>6) {
      vals=u8_malloc(sizeof(fdtype)*fn->n_vars); free_vals=1;}
    bindings.values=vals;
    {FD_DOLIST(arg,fn->arglist)
       if (i<n) {vals[i]=args[i]; i++;}
       else if ((FD_PAIRP(arg)) && (FD_PAIRP(FD_CDR(arg)))) {
	 /* This code handles argument defaults for sprocs */
	 vals[i]=FD_CADR(arg); i++;}
       else {vals[i]=FD_VOID; i++;}}
    while (j >= i) {
      lexpr_arg=fd_init_pair(NULL,fd_incref(args[j]),lexpr_arg);
      j--;}
    vals[i]=lexpr_arg;}
  /* If we're synchronized, lock the mutex. */
  if (fn->synchronized) u8_lock_mutex(&(fn->lock));
  {FD_DOLIST(expr,fn->body) {
    fd_decref(result); result=fasteval(expr,&envstruct);
    if (FD_ABORTP(result)) {
      /* Note that we don't use passerr_env because
	 adding the current expr should be redundant. */
      fdtype bindings=copy_bindings((&envstruct));
      result=fd_passerr(result,bindings);
      if (fn->filename) result=fd_passerr(result,sproc_id(fn));
      break;}}}
  /* If we're synchronized, unlock the mutex. */
  if (fn->synchronized) u8_unlock_mutex(&(fn->lock));
  u8_destroy_mutex(&(bindings.lock));
  fd_decref(lexpr_arg);
  if (free_vals) u8_free(vals);
  if (envstruct.copy)
    fd_recycle_environment(envstruct.copy);
  return result;
}

static fdtype sproc_applier(fdtype f,int n,fdtype *args)
{
  struct FD_SPROC *s=FD_GET_CONS(f,fd_sproc_type,struct FD_SPROC *);
  return fd_apply_sproc(s,n,args);
}

static fdtype make_sproc(u8_string name,
			  fdtype arglist,fdtype body,fd_lispenv env,
			  int nd,int sync)
{
  int i=0, n_vars=0, min_args=0, max_args=0, defaults_start=0;
  fdtype scan=arglist;
  struct FD_SPROC *s=u8_malloc_type(struct FD_SPROC);
  FD_INIT_CONS(s,fd_sproc_type);
  s->name=((name) ? (u8_strdup(name)) : (NULL));
  while (FD_PAIRP(scan)) {
    fdtype argspec=FD_CAR(scan);
    n_vars++; scan=FD_CDR(scan);
    if (FD_SYMBOLP(argspec)) min_args=n_vars;}
  if (FD_EMPTY_LISTP(scan)) {
    s->n_vars=s->arity=n_vars;}
  else {
    n_vars++; s->n_vars=n_vars; s->arity=-1;}
  s->min_arity=min_args; s->xprim=1; s->ndprim=nd;
  s->typeinfo=NULL; 
  if (n_vars)
    s->schema=u8_malloc(sizeof(fdtype)*(n_vars+1));
  else s->schema=NULL;
  if (min_args<n_vars)
    s->defaults=u8_malloc(sizeof(fdtype)*n_vars);
  else s->defaults=NULL;
  s->body=fd_incref(body); s->arglist=fd_incref(arglist);
  s->env=fd_copy_env(env); s->filename=NULL;
  if (sync) {
    s->synchronized=1; u8_init_mutex(&(s->lock));}
  else s->synchronized=0;
  scan=arglist; i=0; while (FD_PAIRP(scan)) {
    fdtype argspec=FD_CAR(scan);
    if (FD_PAIRP(argspec)) {
      s->schema[i]=FD_CAR(argspec);
      if (s->defaults) s->defaults[i]=fd_get_arg(argspec,1);}
    else {
      s->schema[i]=argspec;
      if (s->defaults) s->defaults[i]=FD_VOID;}
    i++; scan=FD_CDR(scan);}
  if (i<s->n_vars) s->schema[i]=scan;
  return FDTYPE_CONS(s);
}

FD_EXPORT void recycle_sproc(struct FD_CONS *c)
{
  int i=0;
  struct FD_SPROC *sproc=(struct FD_SPROC *)c;
  if (sproc->name) u8_free(sproc->name);
  if (sproc->typeinfo) u8_free(sproc->typeinfo);
  if (sproc->defaults) u8_free(sproc->defaults);
  fd_decref(sproc->arglist); fd_decref(sproc->body);
  u8_free(sproc->schema);
  if (sproc->env->copy) {
    fd_decref((fdtype)sproc->env->copy);}
  if (sproc->synchronized) u8_destroy_mutex(&(sproc->lock));
  if (sproc->filename) u8_free(sproc->filename);
  if (FD_MALLOCD_CONSP(c))
    u8_free_x(sproc,sizeof(struct FD_SPROC));
}

static int unparse_sproc(u8_output out,fdtype x)
{
  struct FD_SPROC *sproc=FD_GET_CONS(x,fd_sproc_type,struct FD_SPROC *);
  if (sproc->name)
    if (sproc->filename)
      u8_printf(out,"#<PROC %s %q \"%s\" #!%x>",
		sproc->name,sproc->arglist,sproc->filename,(unsigned long)sproc);
    else u8_printf(out,"#<PROC %s %q #!%x>",
		   sproc->name,sproc->arglist,(unsigned long)sproc);
  else if (sproc->filename)
    u8_printf(out,"#<LAMBDA %q \"%s\" #!%x>",
	      sproc->arglist,sproc->filename,(unsigned long)sproc);
  else u8_printf(out,"#<LAMBDA %q #!%x>",
		 sproc->arglist,(unsigned long)sproc);
  return 1;
}

/* Macros */

FD_EXPORT fdtype fd_make_macro(u8_string name,fdtype xformer)
{
  int xftype=FD_PTR_TYPE(xformer);
  if ((xftype<FD_TYPE_MAX) && (fd_applyfns[xftype])) {
    struct FD_MACRO *s=u8_malloc_type(struct FD_MACRO);
    FD_INIT_CONS(s,fd_macro_type);
    s->name=((name) ? (u8_strdup(name)) : (NULL));
    s->transformer=fd_incref(xformer);
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
      fd_init_pair(NULL,lambda_symbol,
		   fd_init_pair(NULL,fd_make_list(1,name),fd_incref(body)));
    fdtype transformer=fd_eval(lambda_form,env);
    fdtype macro=fd_make_macro(FD_SYMBOL_NAME(name),transformer);
    fd_decref(lambda_form); fd_decref(transformer);
    return macro;}
  else return fd_err(fd_SyntaxError,"MACRO",NULL,expr);
}

FD_EXPORT void recycle_macro(struct FD_CONS *c)
{
  int i=0;
  struct FD_MACRO *mproc=(struct FD_MACRO *)c;
  if (mproc->name) u8_free(mproc->name);
  fd_decref(mproc->transformer);
  if (FD_MALLOCD_CONSP(c))
    u8_free_x(mproc,sizeof(struct FD_MACRO));
}

static int unparse_macro(u8_output out,fdtype x)
{
  struct FD_MACRO *mproc=FD_GET_CONS(x,fd_macro_type,struct FD_MACRO *);
  if (mproc->name) 
    u8_printf(out,"#<MACRO %s #!%x>",
	      mproc->name,(unsigned long)mproc);
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
  return make_sproc(NULL,arglist,body,env,0,0);
}

static fdtype ambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype arglist=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"AMBDA",NULL,expr);
  return make_sproc(NULL,arglist,body,env,1,0);
}

static fdtype slambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype arglist=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"SLAMBDA",NULL,expr);
  return make_sproc(NULL,arglist,body,env,0,1);
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
      if (FD_EXCEPTIONP(value)) return value;
      else if (fd_bind_value(var,value,env)) {
	if (FD_PRIM_TYPEP(value,fd_sproc_type)) {
	  struct FD_SPROC *s=(fd_sproc)value;
	  if (s->filename==NULL) {
	    u8_string sourcebase=fd_sourcebase();
	    if (sourcebase) s->filename=u8_strdup(sourcebase);}}
	fd_decref(value);
	return FD_VOID;}
      else {
	fd_decref(value);
	return fd_err(fd_BindError,"DEFINE",NULL,var);}}}
  else if (FD_PAIRP(var)) {
    fdtype fn_name=FD_CAR(var), args=FD_CDR(var);
    fdtype body=fd_get_body(expr,2);
    if (!(FD_SYMBOLP(fn_name)))
      fd_err(fd_NotAnIdentifier,"DEFINE",NULL,fn_name);
    else {
      fdtype value=make_sproc(FD_SYMBOL_NAME(fn_name),args,body,env,0,0);
      if (FD_EXCEPTIONP(value)) return value;
      else if (fd_bind_value(fn_name,value,env)) {
	if (FD_PRIM_TYPEP(value,fd_sproc_type)) {
	  struct FD_SPROC *s=(fd_sproc)value;
	  if (s->filename==NULL) {
	    u8_string sourcebase=fd_sourcebase();
	    if (sourcebase) s->filename=u8_strdup(sourcebase);}}
	fd_decref(value);
	return FD_VOID;}
      else {
	fd_decref(value);
	return fd_err(fd_BindError,"DEFINE",NULL,var);}}}
  else fd_err(fd_NotAnIdentifier,"DEFINE",NULL,var);
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
  fdtype _vals[12], *vals=_vals, arglist=fn->arglist, result=FD_VOID;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type);
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type);
  bindings.schema=fn->schema;
  bindings.size=fn->n_vars;
  bindings.flags=0;
  u8_init_mutex(&(bindings.lock));
  envstruct.bindings=FDTYPE_CONS(&bindings);
  envstruct.exports=FD_VOID;
  envstruct.parent=fn->env; envstruct.copy=NULL;
  if (fn->n_vars>=12)
    bindings.values=vals=u8_malloc(sizeof(fdtype)*fn->n_vars);
  else bindings.values=vals=_vals;
  while (FD_PAIRP(arglist)) {
    fdtype argspec=FD_CAR(arglist), argname=FD_VOID, argval;
    if (FD_SYMBOLP(argspec)) argname=argspec;
    else if (FD_PAIRP(argspec)) argname=FD_CAR(argspec);
    if (!(FD_SYMBOLP(argname)))
      return fd_err(fd_BadArglist,fn->name,NULL,fn->arglist);
    argval=getval(data,argname);
    if (FD_EXCEPTIONP(argval)) {
      int j=0; while (j<i) {fd_decref(vals[j]); j++;}
      if (vals!=_vals) u8_free(vals);
      return argval;}
    else if ((FD_VOIDP(argval)) && (FD_PAIRP(argspec)))
      vals[i++]=fd_incref(FD_CADR(argspec));
    else vals[i++]=argval;
    arglist=FD_CDR(arglist);}
  /* This means we have a lexpr arg. */
  if (i<fn->n_vars) {
    /* We look for the arg directly and then we use the special
       tail_symbol (%TAIL) to get something. */
    fdtype argval=getval(data,arglist);
    if (FD_VOIDP(argval)) argval=getval(data,tail_symbol);
    if (FD_EXCEPTIONP(argval)) {
      int j=0; while (j<i) {fd_decref(vals[j]); j++;}
      if (vals!=_vals) u8_free(vals);
      return argval;}
    else vals[i++]=argval;}
  assert(i==fn->n_vars);
  /* If we're synchronized, lock the mutex. */
  if (fn->synchronized) u8_lock_mutex(&(fn->lock));
  {FD_DOLIST(expr,fn->body) {
    fd_decref(result); result=fasteval(expr,&envstruct);
    if (FD_EXCEPTIONP(result)) {
      /* Note that we don't use passerr_env because
	 adding the current expr should be redundant. */
      fdtype bindings=copy_bindings((&envstruct));
      result=fd_passerr(result,bindings);
      if (fn->filename) result=fd_passerr(result,sproc_id(fn));
      break;}}}
  /* If we're synchronized, unlock the mutex. */
  if (fn->synchronized) u8_unlock_mutex(&(fn->lock));
  {
    int j=0; while (j<i) {fd_decref(vals[j]); j++;}
    if (vals!=_vals) u8_free(vals);}
  u8_destroy_mutex(&(bindings.lock));
  return result;
}

/* IPEVAL binding */

struct IPEVAL_BINDSTRUCT {
  int n_bindings; fdtype *vals;
  fdtype valexprs; fd_lispenv env;};
  
static int ipeval_let_step(struct IPEVAL_BINDSTRUCT *bs)
{
  int i=0, n=bs->n_bindings; 
  fdtype *bindings=bs->vals, scan=bs->valexprs;
  fd_lispenv env=bs->env;
  while (i<n) {
    fd_decref(bindings[i]); bindings[i++]=FD_VOID;}
  i=0; while (FD_PAIRP(scan)) {
    fdtype binding=FD_CAR(scan), val_expr=FD_CADR(binding);
    fdtype val=fd_eval(val_expr,env);
    if (FD_ABORTP(val)) fd_interr(val);
    else bindings[i++]=val;
    scan=FD_CDR(scan);}
  return 1;
}

static int ipeval_letstar_step(struct IPEVAL_BINDSTRUCT *bs)
{
  int i=0, n=bs->n_bindings;
  fdtype *bindings=bs->vals, scan=bs->valexprs;
  fd_lispenv env=bs->env;
  while (i<n) {
    fd_decref(bindings[i]); bindings[i++]=FD_UNBOUND;}
  i=0; while (FD_PAIRP(scan)) {
    fdtype binding=FD_CAR(scan), val_expr=FD_CADR(binding);
    fdtype val=fd_eval(val_expr,env);
    if (FD_ABORTP(val)) fd_interr(val);
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
  fdtype body=fd_get_body(expr,2);
  int n;
  if (FD_VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"LET",NULL,expr);
  else if ((n=check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    struct FD_ENVIRONMENT *inner_env=make_dynamic_env(n,env);
    fdtype bindings=inner_env->bindings;
    struct FD_SCHEMAP *sm=(struct FD_SCHEMAP *)bindings;
    fdtype *vars=sm->schema, *vals=sm->values;
    int i=0; fdtype scan=bindexprs; while (i<n) {
      fdtype bind_expr=FD_CAR(scan), var=FD_CAR(bind_expr);
      vars[i]=var; vals[i]=FD_VOID; scan=FD_CDR(scan); i++;}
    if (ipeval_let_binding(n,vals,bindexprs,env)<0) {
      fdtype errobj=fd_erreify();
      return passerr_env(errobj,env);}
    {FD_DOLIST(bodyexpr,body) {
      fd_decref(result);
      result=fasteval(bodyexpr,inner_env);
      if (FD_ABORTP(result))
	return passerr_env(result,inner_env);}}
    free_environment(inner_env);
    return result;}
}

static fdtype letqstar_handler(fdtype expr,fd_lispenv env)
{
  fdtype bindexprs=fd_get_arg(expr,1), result=FD_VOID;
  fdtype body=fd_get_body(expr,2);
  int n;
  if (FD_VOIDP(bindexprs))
    return fd_err(fd_BindSyntaxError,"LET*",NULL,expr);
  else if ((n=check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    struct FD_ENVIRONMENT *inner_env=make_dynamic_env(n,env);
    fdtype bindings=inner_env->bindings;
    struct FD_SCHEMAP *sm=(struct FD_SCHEMAP *)bindings;
    fdtype *vars=sm->schema, *vals=sm->values;
    int i=0; fdtype scan=bindexprs; while (i<n) {
      fdtype bind_expr=FD_CAR(scan), var=FD_CAR(bind_expr);
      vars[i]=var; vals[i]=FD_UNBOUND; scan=FD_CDR(scan); i++;}
    if (ipeval_letstar_binding(n,vals,bindexprs,inner_env,inner_env)<0) {
      fdtype errobj=fd_erreify();
      return passerr_env(errobj,inner_env);}
    {FD_DOLIST(bodyexpr,body) {
      fd_decref(result);
      result=fasteval(bodyexpr,inner_env);
      if (FD_EXCEPTIONP(result)) {
	return passerr_env(result,inner_env);}}}
    if (inner_env->copy) free_environment(inner_env->copy);
    return result;}
}

/* Initialization */

FD_EXPORT void fd_init_binders_c()
{
  fd_register_source_file(versionid);

  lambda_symbol=fd_intern("LAMBDA");
  tail_symbol=fd_intern("%TAIL");

  fd_sproc_type=fd_register_cons_type("scheme procedure");
  fd_macro_type=fd_register_cons_type(_("scheme syntactic macro"));

  fd_applyfns[fd_sproc_type]=sproc_applier;

  fd_unparsers[fd_sproc_type]=unparse_sproc;
  fd_recyclers[fd_sproc_type]=recycle_sproc;

  fd_unparsers[fd_macro_type]=unparse_macro;
  fd_recyclers[fd_macro_type]=recycle_macro;

  fd_defspecial(fd_scheme_module,"SET!",set_handler);
  fd_defspecial(fd_scheme_module,"SET+!",set_plus_handler);

  fd_defspecial(fd_scheme_module,"LET",let_handler);
  fd_defspecial(fd_scheme_module,"LET*",letstar_handler);
  fd_defspecial(fd_scheme_module,"LAMBDA",lambda_handler);
  fd_defspecial(fd_scheme_module,"AMBDA",ambda_handler);
  fd_defspecial(fd_scheme_module,"SLAMBDA",slambda_handler);
  fd_defspecial(fd_scheme_module,"DEFINE",define_handler);

  fd_defspecial(fd_scheme_module,"MACRO",macro_handler);

  fd_defspecial(fd_scheme_module,"DO",do_handler);

  fd_defspecial(fd_scheme_module,"DEFAULT!",set_default_handler);

  fd_defspecial(fd_scheme_module,"LETQ",letq_handler);
  fd_defspecial(fd_scheme_module,"LETQ*",letqstar_handler);
}


/* The CVS log for this file
   $Log: binders.c,v $
   Revision 1.65  2006/01/31 13:47:24  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.64  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.63  2006/01/20 04:08:42  haase
   Fixed leak in the MACRO special form

   Revision 1.62  2006/01/19 02:53:02  haase
   Fixed missing incref for defaults in xapply

   Revision 1.61  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.60  2006/01/02 19:59:30  haase
   Fixed erroneous identity of iterative environments

   Revision 1.59  2005/12/28 22:16:43  haase
   Fix schemap allocation leak

   Revision 1.58  2005/12/23 16:58:29  haase
   Added IPEVAL LET/LET* binding forms

   Revision 1.57  2005/12/22 19:50:45  haase
   More sproc/environment leak fixing

   Revision 1.56  2005/12/22 19:28:08  haase
   Minor syntactic change

   Revision 1.55  2005/12/22 19:15:50  haase
   Added more comprehensive environment recycling

   Revision 1.54  2005/12/22 14:38:29  haase
   Fix typo in environment freeing

   Revision 1.53  2005/12/20 19:09:32  haase
   Made LET* work with internal variable refs

   Revision 1.52  2005/08/25 20:35:45  haase
   Fixed environment copying bug

   Revision 1.51  2005/08/19 22:50:10  haase
   More filename field fixes

   Revision 1.50  2005/08/19 22:45:40  haase
   Initialize filename field of SPROCs

   Revision 1.49  2005/08/15 03:28:56  haase
   Added file information to functions and display it in regular and HTML backtraces

   Revision 1.48  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.47  2005/08/06 20:32:20  haase
   Fixed bug/typo in large LET case

   Revision 1.46  2005/07/13 21:39:31  haase
   XSLOTMAP/XSCHEMAP renaming

   Revision 1.45  2005/06/25 16:25:47  haase
   Added threading primitives

   Revision 1.44  2005/06/20 13:56:59  haase
   Fixes to regularize CONS header initialization

   Revision 1.43  2005/06/19 19:40:04  haase
   Regularized use of FD_CONS_HEADER

   Revision 1.42  2005/06/02 17:55:59  haase
   Added SIDE-EFFECTS module

   Revision 1.41  2005/05/22 20:30:03  haase
   Pass initialization errors out of config-def! and other error improvements

   Revision 1.40  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.39  2005/04/26 16:16:50  haase
   Changed changelog

   Revision 1.38  2005/04/26 01:22:49  haase
   Fixed bulk fetching bug in zindices and fileindices

   Revision 1.37  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.36  2005/04/12 16:06:00  haase
   Made binders use FD_ABORTP

   Revision 1.35  2005/04/11 21:27:28  haase
   Fix bug in error propogation in DO

   Revision 1.34  2005/04/09 22:45:03  haase
   Fixed binding bug which failed to declare stack schemas correctly

   Revision 1.33  2005/04/08 15:34:32  haase
   Fix erroneous error report in xapply_sproc

   Revision 1.32  2005/04/08 04:46:30  haase
   Improvements to backtrace accumulation

   Revision 1.31  2005/04/07 19:11:30  haase
   Made backtraces include environments, fixed some minor GC issues

   Revision 1.30  2005/04/07 15:27:32  haase
   Added fd_xapply_sproc

   Revision 1.29  2005/04/01 02:42:40  haase
   Reimplemented module exports to be faster and less kludgy

   Revision 1.28  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.27  2005/03/26 20:06:52  haase
   Exposed APPLY to scheme and made optional arguments generally available

   Revision 1.26  2005/03/22 20:25:39  haase
   Added macros, fixed DO syntax checking error, and made minor fixes to quasiquoting

   Revision 1.25  2005/03/08 05:18:29  haase
   Fixed log entry to not include comment end sequence

   Revision 1.24  2005/03/08 05:16:29  haase
   Fixed let/lambda interaction

   Revision 1.23  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.22  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.21  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.20  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
