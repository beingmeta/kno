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
#include "kno/dtype.h"
#include "kno/eval.h"
#include "eval_internals.h"
#include "kno/storage.h"

#include <libu8/u8printf.h>

/* IPEVAL binding */

#if KNO_IPEVAL_ENABLED
struct IPEVAL_BINDSTRUCT {
  int n_bindings; lispval *vals;
  lispval valexprs; kno_lexenv env;};

static int ipeval_let_step(struct IPEVAL_BINDSTRUCT *bs)
{
  int i = 0, n = bs->n_bindings;
  lispval *bindings = bs->vals, scan = bs->valexprs;
  kno_lexenv env = bs->lambda_env;
  while (i<n) {
    kno_decref(bindings[i]); bindings[i++]=VOID;}
  i = 0; while (PAIRP(scan)) {
    lispval binding = KNO_CAR(scan), val_expr = KNO_CADR(binding);
    lispval val = kno_eval(val_expr,env);
    if (KNO_ABORTED(val)) kno_interr(val);
    else bindings[i++]=val;
    scan = KNO_CDR(scan);}
  return 1;
}

static int ipeval_letstar_step(struct IPEVAL_BINDSTRUCT *bs)
{
  int i = 0, n = bs->n_bindings;
  lispval *bindings = bs->vals, scan = bs->valexprs;
  kno_lexenv env = bs->lambda_env;
  while (i<n) {
    kno_decref(bindings[i]); bindings[i++]=KNO_UNBOUND;}
  i = 0; while (PAIRP(scan)) {
    lispval binding = KNO_CAR(scan), val_expr = KNO_CADR(binding);
    lispval val = kno_eval(val_expr,env);
    if (KNO_ABORTED(val)) kno_interr(val);
    else bindings[i++]=val;
    scan = KNO_CDR(scan);}
  return 1;
}

static int ipeval_let_binding
  (int n,lispval *vals,lispval bindexprs,kno_lexenv env)
{
  struct IPEVAL_BINDSTRUCT bindstruct;
  bindstruct.n_bindings = n; bindstruct.vals = vals;
  bindstruct.valexprs = bindexprs; bindstruct.env = env;
  return kno_ipeval_call((kno_ipevalfn)ipeval_let_step,&bindstruct);
}

static int ipeval_letstar_binding
  (int n,lispval *vals,lispval bindexprs,kno_lexenv bind_env,kno_lexenv env)
{
  struct IPEVAL_BINDSTRUCT bindstruct;
  bindstruct.n_bindings = n; bindstruct.vals = vals;
  bindstruct.valexprs = bindexprs; bindstruct.env = env;
  return kno_ipeval_call((kno_ipevalfn)ipeval_letstar_step,&bindstruct);
}

static lispval letq_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval bindexprs = kno_get_arg(expr,1), result = VOID;
  int n;
  if (VOIDP(bindexprs))
    return kno_err(kno_BindSyntaxError,"LET",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    struct KNO_LEXENV *inner_env = make_dynamic_env(n,env);
    lispval bindings = inner_env->env_bindings;
    struct KNO_SCHEMAP *sm = (struct KNO_SCHEMAP *)bindings;
    lispval *vars = sm->table_schema, *vals = sm->schema_values;
    int i = 0; lispval scan = bindexprs; while (i<n) {
      lispval bind_expr = KNO_CAR(scan), var = KNO_CAR(bind_expr);
      vars[i]=var; vals[i]=VOID; scan = KNO_CDR(scan); i++;}
    if (ipeval_let_binding(n,vals,bindexprs,env)<0)
      return ERROR_VALUE;
    {lispval body = kno_get_body(expr,2);
     KNO_DOLIST(bodyexpr,body) {
      kno_decref(result);
      result = fast_eval(bodyexpr,inner_env);
      if (KNO_ABORTED(result))
        return result}}
    kno_free_lexenv(inner_env);
    return result;}
}

static lispval letqstar_evalfn
(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval bindexprs = kno_get_arg(expr,1), result = VOID;
  int n;
  if (VOIDP(bindexprs))
    return kno_err(kno_BindSyntaxError,"LET*",NULL,expr);
  else if ((n = check_bindexprs(bindexprs,&result))<0)
    return result;
  else {
    struct KNO_LEXENV *inner_env = make_dynamic_env(n,env);
    lispval bindings = inner_env->env_bindings;
    struct KNO_SCHEMAP *sm = (struct KNO_SCHEMAP *)bindings;
    lispval *vars = sm->table_schema, *vals = sm->schema_values;
    int i = 0; lispval scan = bindexprs; while (i<n) {
      lispval bind_expr = KNO_CAR(scan), var = KNO_CAR(bind_expr);
      vars[i]=var; vals[i]=KNO_UNBOUND; scan = KNO_CDR(scan); i++;}
    if (ipeval_letstar_binding(n,vals,bindexprs,inner_env,inner_env)<0)
      return KNO_ERROR;
    {lispval body = kno_get_body(expr,2);
     KNO_DOLIST(bodyexpr,body) {
      kno_decref(result);
      result = fast_eval(bodyexpr,inner_env);
      if (KNO_ABORTED(result))
        return result;}}
    if (inner_env->env_copy) kno_free_lexenv(inner_env->env_copy);
    return result;}
}

#endif

/* IPEVAL */

#if KNO_IPEVAL_ENABLED
struct IPEVAL_STRUCT { lispval expr, kno_value; kno_lexenv env;};

static int ipeval_step(struct IPEVAL_STRUCT *s)
{
  lispval kno_value = kno_eval(s->expr,s->env);
  kno_decref(s->kv_val); s->kv_val = kno_value;
  if (KNO_ABORTED(kno_value))
    return -1;
  else return 1;
}

static lispval ipeval_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct IPEVAL_STRUCT tmp;
  tmp.expr = kno_refcar(kno_refcdr(expr)); tmp.env = env; tmp.kv_val = VOID;
  kno_ipeval_call((kno_ipevalfn)ipeval_step,&tmp);
  return tmp.kv_val;
}

static lispval trace_ipeval_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct IPEVAL_STRUCT tmp; int old_trace = kno_trace_ipeval;
  tmp.expr = kno_refcar(kno_refcdr(expr)); tmp.env = env; tmp.kv_val = VOID;
  kno_trace_ipeval = 1;
  kno_ipeval_call((kno_ipevalfn)ipeval_step,&tmp);
  kno_trace_ipeval = old_trace;
  return tmp.kv_val;
}

static lispval track_ipeval_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct IPEVAL_STRUCT tmp;
  struct KNO_IPEVAL_RECORD *records; int n_cycles; double total_time;
  lispval *vec; int i = 0;
  tmp.expr = kno_refcar(kno_refcdr(expr)); tmp.env = env; tmp.kv_val = VOID;
  kno_tracked_ipeval_call((kno_ipevalfn)ipeval_step,&tmp,&records,&n_cycles,&total_time);
  vec = u8_alloc_n(n_cycles,lispval);
  i = 0; while (i<n_cycles) {
    struct KNO_IPEVAL_RECORD *record = &(records[i]);
    vec[i++]=
      kno_make_nvector(3,KNO_INT(record->delays),
                      kno_init_double(NULL,record->exec_time),
                      kno_init_double(NULL,record->fetch_time));}
  return kno_make_nvector(3,tmp.kv_val,
                         kno_init_double(NULL,total_time),
                         kno_wrap_vector(n_cycles,vec));
}
#endif

/* Extend fcnid parsing to incluce functional compounds */

/* Initialization */

KNO_EXPORT void kno_init_ipevalprims_c()
{
  u8_register_source_file(_FILEINFO);

  moduleid_symbol = kno_intern("%moduleid");

  kno_def_evalfn(kno_scheme_module,"LETQ","",letq_evalfn);
  kno_def_evalfn(kno_scheme_module,"LETQ*","",letqstar_evalfn);

#if KNO_IPEVAL_ENABLED
  kno_def_evalfn(kno_scheme_module,"IPEVAL","",ipeval_evalfn);
  kno_def_evalfn(kno_scheme_module,"TIPEVAL","",trace_ipeval_evalfn);
  kno_def_evalfn(kno_scheme_module,"TRACK-IPEVAL","",track_ipeval_evalfn);
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
