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

/* IPEVAL */

#if FD_IPEVAL_ENABLED
struct IPEVAL_STRUCT { fdtype expr, fd_value; fd_lispenv env;};

static int ipeval_step(struct IPEVAL_STRUCT *s)
{
  fdtype fd_value = fd_eval(s->expr,s->env);
  fd_decref(s->kv_val); s->kv_val = fd_value;
  if (FD_ABORTED(fd_value))
    return -1;
  else return 1;
}

static fdtype ipeval_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  struct IPEVAL_STRUCT tmp;
  tmp.expr = fd_refcar(fd_refcdr(expr)); tmp.env = env; tmp.kv_val = FD_VOID;
  fd_ipeval_call((fd_ipevalfn)ipeval_step,&tmp);
  return tmp.kv_val;
}

static fdtype trace_ipeval_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  struct IPEVAL_STRUCT tmp; int old_trace = fd_trace_ipeval;
  tmp.expr = fd_refcar(fd_refcdr(expr)); tmp.env = env; tmp.kv_val = FD_VOID;
  fd_trace_ipeval = 1;
  fd_ipeval_call((fd_ipevalfn)ipeval_step,&tmp);
  fd_trace_ipeval = old_trace;
  return tmp.kv_val;
}

static fdtype track_ipeval_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  struct IPEVAL_STRUCT tmp;
  struct FD_IPEVAL_RECORD *records; int n_cycles; double total_time;
  fdtype *vec; int i = 0;
  tmp.expr = fd_refcar(fd_refcdr(expr)); tmp.env = env; tmp.kv_val = FD_VOID;
  fd_tracked_ipeval_call((fd_ipevalfn)ipeval_step,&tmp,&records,&n_cycles,&total_time);
  vec = u8_alloc_n(n_cycles,fdtype);
  i = 0; while (i<n_cycles) {
    struct FD_IPEVAL_RECORD *record = &(records[i]);
    vec[i++]=
      fd_make_nvector(3,FD_INT(record->delays),
                      fd_init_double(NULL,record->exec_time),
                      fd_init_double(NULL,record->fetch_time));}
  return fd_make_nvector(3,tmp.kv_val,
                         fd_init_double(NULL,total_time),
                         fd_init_vector(NULL,n_cycles,vec));
}
#endif

/* Extend fcnid parsing to incluce functional compounds */

/* Initialization */

FD_EXPORT void fd_init_ipevalprims_c()
{
  u8_register_source_file(_FILEINFO);

  moduleid_symbol = fd_intern("%MODULEID");

  fd_defspecial(fd_scheme_module,"LETQ",letq_evalfn);
  fd_defspecial(fd_scheme_module,"LETQ*",letqstar_evalfn);

#if FD_IPEVAL_ENABLED
  fd_defspecial(fd_scheme_module,"IPEVAL",ipeval_evalfn);
  fd_defspecial(fd_scheme_module,"TIPEVAL",trace_ipeval_evalfn);
  fd_defspecial(fd_scheme_module,"TRACK-IPEVAL",track_ipeval_evalfn);
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
