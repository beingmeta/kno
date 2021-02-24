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

#if KNO_IPEVAL_ENABLED
struct IPEVAL_BINDSTRUCT {
  int bs_len;
  lispval *bs_values;
  lispval bs_val_exprs;
  kno_lexenv bs_env;};

struct IPEVAL_STRUCT {
  lispval ipv_expr, ipv_value;
  kno_lexenv ipv_env;};
#endif

KNO_FASTOP kno_lexenv make_dynamic_env(int n,kno_lexenv parent)
{
  int i = 0;
  struct KNO_LEXENV *e = u8_alloc(struct KNO_LEXENV);
  lispval *vars = u8_alloc_n(n,lispval);
  lispval *vals = u8_alloc_n(n,lispval);
  lispval schemap = kno_make_schemap(NULL,n,KNO_SCHEMAP_PRIVATE,vars,vals);
  while (i<n) {vars[i]=VOID; vals[i]=VOID; i++;}
  KNO_INIT_FRESH_CONS(e,kno_lexenv_type);
  e->env_copy     = e;
  e->env_bindings = schemap;
  e->env_exports  = VOID;
  e->env_parent   = kno_copy_env(parent);
  if (KNO_SCHEMAPP(bindings)) {
    struct KNO_SCHEMAP *smap = (kno_schemap) bindings;
    e->env_vals = smap->table_values;}
  else e->env_vals     = NULL;
  e->env_bits    = 0;
  return e;
}

/* IPEVAL binding */

#if KNO_IPEVAL_ENABLED
static int ipeval_let_step(struct IPEVAL_BINDSTRUCT *bs)
{
  int i = 0, n = bs->bs_len;
  lispval *bindings = bs->bs_values, scan = bs->bs_val_exprs;
  kno_lexenv env = bs->bs_env;
  while (i<n) {
    kno_decref(bindings[i]); bindings[i++]=VOID;}
  i = 0; while (PAIRP(scan)) {
    lispval binding = KNO_CAR(scan), val_expr = KNO_CADR(binding);
    lispval val = kno_eval(val_expr,env,NULL);
    if (KNO_ABORTED(val)) kno_interr(val);
    else bindings[i++]=val;
    scan = KNO_CDR(scan);}
  return 1;
}

static int ipeval_letstar_step(struct IPEVAL_BINDSTRUCT *bs)
{
  int i = 0, n = bs->bs_len;
  lispval *bindings = bs->bs_values, scan = bs->bs_val_exprs;
  kno_lexenv env = bs->bs_env;
  while (i<n) {
    kno_decref(bindings[i]); bindings[i++]=KNO_UNBOUND;}
  i = 0; while (PAIRP(scan)) {
    lispval binding = KNO_CAR(scan), val_expr = KNO_CADR(binding);
    lispval val = kno_eval(val_expr,env,NULL);
    if (KNO_ABORTED(val)) kno_interr(val);
    else bindings[i++]=val;
    scan = KNO_CDR(scan);}
  return 1;
}

static int ipeval_let_binding
  (int n,lispval *vals,lispval bindexprs,kno_lexenv env)
{
  struct IPEVAL_BINDSTRUCT bindstruct;
  bindstruct.bs_len = n; bindstruct.bs_values = vals;
  bindstruct.bs_val_exprs = bindexprs; bindstruct.bs_env = env;
  return kno_ipeval_call((kno_ipevalfn)ipeval_let_step,&bindstruct);
}

static int ipeval_letstar_binding
  (int n,lispval *vals,lispval bindexprs,kno_lexenv bind_env,kno_lexenv env)
{
  struct IPEVAL_BINDSTRUCT bindstruct;
  bindstruct.bs_len = n; bindstruct.bs_values = vals;
  bindstruct.bs_val_exprs = bindexprs; bindstruct.bs_env = env;
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
    lispval *vars = sm->table_schema, *vals = sm->table_values;
    int i = 0; lispval scan = bindexprs; while (i<n) {
      lispval bind_expr = KNO_CAR(scan), var = KNO_CAR(bind_expr);
      vars[i]=var; vals[i]=VOID; scan = KNO_CDR(scan); i++;}
    if (ipeval_let_binding(n,vals,bindexprs,env)<0)
      return KNO_ERROR_VALUE;
    {lispval body = kno_get_body(expr,2);
     KNO_DOLIST(bodyexpr,body) {
      kno_decref(result);
      result = kno_eval(bodyexpr,inner_env,_stack);
      if (KNO_ABORTED(result))
        return result;}}
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
    lispval *vars = sm->table_schema, *vals = sm->table_values;
    int i = 0; lispval scan = bindexprs; while (i<n) {
      lispval bind_expr = KNO_CAR(scan), var = KNO_CAR(bind_expr);
      vars[i]=var; vals[i]=KNO_UNBOUND; scan = KNO_CDR(scan); i++;}
    if (ipeval_letstar_binding(n,vals,bindexprs,inner_env,inner_env)<0)
      return KNO_ERROR;
    {lispval body = kno_get_body(expr,2);
     KNO_DOLIST(bodyexpr,body) {
      kno_decref(result);
      result = kno_eval(bodyexpr,inner_env,_stack);
      if (KNO_ABORTED(result))
        return result;}}
    if (inner_env->env_copy) kno_free_lexenv(inner_env->env_copy);
    return result;}
}

#endif

/* IPEVAL */

#if KNO_IPEVAL_ENABLED
static int ipeval_step(struct IPEVAL_STRUCT *s)
{
  lispval kno_value = kno_eval(s->ipv_expr,s->ipv_env,NULL);
  kno_decref(s->ipv_value); s->ipv_value = kno_value;
  if (KNO_ABORTED(kno_value))
    return -1;
  else return 1;
}

static lispval ipeval_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct IPEVAL_STRUCT tmp;
  tmp.ipv_expr = kno_refcar(kno_refcdr(expr));
  tmp.ipv_env = env; tmp.ipv_value = VOID;
  kno_ipeval_call((kno_ipevalfn)ipeval_step,&tmp);
  return tmp.ipv_value;
}

static lispval trace_ipeval_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct IPEVAL_STRUCT tmp; int old_trace = kno_trace_ipeval;
  tmp.ipv_expr = kno_refcar(kno_refcdr(expr));
  tmp.ipv_env = env;
  tmp.ipv_value = VOID;
  kno_trace_ipeval = 1;
  kno_ipeval_call((kno_ipevalfn)ipeval_step,&tmp);
  kno_trace_ipeval = old_trace;
  return tmp.ipv_value;
}

static lispval track_ipeval_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct IPEVAL_STRUCT tmp;
  int n_cycles; double total_time;
  struct KNO_IPEVAL_RECORD *records;
  lispval *vec; int i = 0;
  tmp.ipv_expr = kno_refcar(kno_refcdr(expr));
  tmp.ipv_value = VOID;
  tmp.ipv_env = env;
  kno_tracked_ipeval_call((kno_ipevalfn)ipeval_step,
                          &tmp,&records,&n_cycles,&total_time);
  vec = u8_alloc_n(n_cycles,lispval);
  i = 0; while (i<n_cycles) {
    struct KNO_IPEVAL_RECORD *record = &(records[i]);
    vec[i++]=
      kno_make_nvector(3,KNO_INT(record->ipv_delays),
                      kno_init_double(NULL,record->ipv_exec_time),
                      kno_init_double(NULL,record->ipv_fetch_time));}
  return kno_make_nvector(3,tmp.ipv_value,
                         kno_init_double(NULL,total_time),
                         kno_wrap_vector(n_cycles,vec));
}
#endif

/* Extend fcnid parsing to incluce functional compounds */

/* Initialization */

KNO_EXPORT void kno_init_ipevalprims_c()
{
  u8_register_source_file(_FILEINFO);

  kno_def_evalfn(kno_scheme_module,"LETQ",letq_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"LETQ*",letqstar_evalfn,
		 "*undocumented*");

#if KNO_IPEVAL_ENABLED
  kno_def_evalfn(kno_scheme_module,"IPEVAL",ipeval_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"TIPEVAL",trace_ipeval_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"TRACK-IPEVAL",track_ipeval_evalfn,
		 "*undocumented*");
#endif

}
