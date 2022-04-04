/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include "kno/cprims.h"
#include "kno/futures.h"

#include <libu8/libu8io.h>

#include <sys/types.h>

/* Promises, Delays, etc */

static int eval_futurefn(kno_future,lispval,int);

static lispval delay_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval delay_expr = kno_get_arg(expr,1);
  if (KNO_VOIDP(delay_expr))
    return kno_err(kno_SyntaxError,"delay_evalfn",NULL,expr);
  else if (KNO_EVALP(delay_expr)) {
    return (lispval) kno_init_future
      (NULL,KNO_NULL,kno_incref(delay_expr),kno_env2lisp(env),
       delay_futurefn,
       0);}
  else return kno_init_future
	 (NULL,kno_incref(delay_expr),kno_incref(delay_expr),kno_env2lisp(env),
	  NULL,KNO_FUTURE_FINAL);
}

static int eval_futurefn(kno_future f,lispval arg,int flags)
{
  
}

KNO_EXPORT lispval kno_force_promise(lispval promise)
{
  if (KNO_TYPEP(promise,kno_promise_type)) {
    struct KNO_PROMISE *p = (kno_promise) promise;
    if (p->promise_value)
      return kno_incref(p->promise_value);
    else {
      u8_lock_mutex(&p->promise_lock);
      /* Handle race conditions */
      if (p->promise_value) {
        u8_unlock_mutex(&p->promise_lock);
        return kno_incref(p->promise_value);}
      lispval result = KNO_VOID;
      int resolve_flags = KNO_PROMISE_FINAL;
      switch (KNO_PROMISE_TYPEOF(p)) {
      case kno_eval_promise: {
	lispval expr = p->promise_body.eval.expr;
	kno_lexenv env = p->promise_body.eval.env;
	result = kno_eval(expr,env,NULL);
	break;}
      case kno_apply_promise: {
	lispval fn = p->promise_body.apply.fcn;
	lispval *args = p->promise_body.apply.args;
	int n_args = p->promise_body.apply.n_args;
	result = kno_apply(fn,n_args,args);
	break;}
      case kno_async_promise: {
	lispval fallback = p->promise_body.async.fallback;
	result = kno_incref(fallback);
	resolve_flags = KNO_PROMISE_PARTIAL;
	break;}
      default:
	result = kno_err("CorruptPromise","kno_force_promise",NULL,KNO_VOID);
	break;}
      if (KNO_ABORTED(result)) {
	u8_exception ex = u8_current_exception;
	lispval exo = kno_simple_exception(ex);
	kno_promise_resolve(p,exo,KNO_PROMISE_BROKEN|KNO_PROMISE_FINAL);}
      else kno_promise_resolve(p,result,resolve_flags);
      return result;}}
  else return kno_incref(promise);
}

DEFC_PRIM("force",force_promise_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the value promised by *promise*. If "
	  "*promise* is not a promise object, it is simply "
	  "returned. Promises only compute their values "
	  "once, so the result of the first call is memoized",
	  {"promise",kno_any_type,KNO_VOID})
static lispval force_promise_prim(lispval promise)
{
  if (KNO_TYPEP(promise,kno_promise_type)) {
    struct KNO_PROMISE *p = (kno_promise) promise;
    return kno_force_promise(promise);}
  else return kno_incref(promise);
}

DEFC_PRIM("promise/probe",probe_promise_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "returns the value promised by *promise* if it has "
	  "been resolved. If *promise* has not been "
	  "resolved, *marker* is returned and if *promise* "
	  "is not a promise it is returned.",
	  {"promise",kno_any_type,KNO_VOID},
	  {"marker",kno_any_type,KNO_VOID})
static lispval probe_promise_prim(lispval promise,lispval marker)
{
  if (KNO_TYPEP(promise,kno_promise_type)) {
    struct KNO_PROMISE *p = (kno_promise) promise;
    if (p->promise_value)
      return kno_incref(p->promise_value);
    else if ( (KNO_PROMISE_TYPEP(p,kno_async_promise)) && (KNO_VOIDP(marker)) ) {
      return kno_incref(p->promise_body.async.fallback);}
    else if (KNO_VOIDP(marker))
      return KNO_FALSE;
    else return kno_incref(marker);}
  else return kno_incref(promise);
}

DEFC_PRIM("make-promise",make_promise_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "returns a promise which returns *value* when "
	  "FORCEd.",
	  {"value",kno_any_type,KNO_VOID})
static lispval make_promise_prim(lispval value)
{
  struct KNO_PROMISE *promise = kno_init_promise(NULL,kno_eval_promise,0);
  promise->promise_body.eval.expr = kno_incref(value);
  promise->promise_body.eval.env = NULL;
  if (KNO_VOIDP(value))
    promise->promise_value = KNO_NULL;
  else promise->promise_value = kno_incref(value);
  return (lispval) promise;
}

/* Initialization */

static int promises_init = 0;

KNO_EXPORT int kno_init_promises_c()
{
  if (promises_init)
    return 0;
  else promises_init = 1;

  link_local_cprims();
  kno_def_evalfn(kno_scheme_module,"DELAY",delay_evalfn,
                 "`(DELAY *expr*)` creates a *promise* to evalute *expr* in "
                 "the current environment, which is delivered when "
                 "the promise is *forced*.");

  u8_register_source_file(_FILEINFO);

  return 1;
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("make-promise",make_promise_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("promise/probe",probe_promise_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("force",force_promise_prim,1,kno_scheme_module);
}
