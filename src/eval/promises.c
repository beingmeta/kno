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

static int force_eval_promise(kno_future,lispval,int);

static lispval delay_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval delay_expr = kno_get_arg(expr,1);
  if (KNO_VOIDP(delay_expr))
    return kno_err(kno_SyntaxError,"delay_evalfn",NULL,expr);
  else if (KNO_EVALP(delay_expr)) {
    lispval source = kno_init_pair(NULL,kno_incref(delay_expr),kno_env2lisp(env));
    return (lispval) kno_init_future
      (NULL,KNO_NULL,source,force_eval_promise,
       KNO_FUTURE_COMPUTABLE|KNO_FUTURE_MONOTONIC);}
  else return (lispval) kno_init_future
	 (NULL,kno_incref(delay_expr),kno_incref(delay_expr),NULL,
	  KNO_FUTURE_COMPUTABLE|KNO_FUTURE_FINALIZED|KNO_FUTURE_MONOTONIC);
}

static int force_eval_promise(kno_future f,lispval curval,int flags)
{
  lispval result = KNO_VOID;
  if (curval) return 0;
  lispval source = f->future_source;
  if ( (KNO_PAIRP(source)) &&
       (KNO_LEXENVP(KNO_CDR(source))) ) {
    lispval expr = KNO_CAR(source);
    kno_lexenv env = (kno_lexenv) KNO_CDR(source);
    result = kno_eval(expr,env,NULL);}
  else result = kno_err("BadEvalPromise","kno_force_promise",NULL,source);
  if (KNO_ABORTED(result)) {
    u8_exception ex = u8_current_exception;
    lispval exo = kno_simple_exception(ex);
    f->future_value=exo;
    KNO_FUTURE_SET(f,KNO_FUTURE_EXCEPTION);
    KNO_FUTURE_SET(f,KNO_FUTURE_FINALIZED);
    return -1;}
  else {
    f->future_value=result;
    KNO_FUTURE_SET(f,KNO_FUTURE_FINALIZED);
    return 1;}
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
  if ( (KNO_TYPEP(promise,kno_future_type)) &&
       ((((kno_future)promise)->future_bits)&(KNO_FUTURE_COMPUTABLE)) )
    return kno_force_future((kno_future)promise,KNO_VOID);
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
  if (KNO_TYPEP(promise,kno_future_type)) {
    struct KNO_FUTURE *f = (kno_future) promise;
    if ( (f->future_value) && (KNO_FUTURE_FINALP(f)) )
      return kno_incref(f->future_value);
    else if ( (f->future_value == KNO_NULL) && (!(KNO_VOIDP(marker))) )
      return kno_incref(marker);
    else {
      u8_lock_mutex(&f->future_lock);
      lispval retval = (f->future_value == KNO_NULL) ? (kno_incref(marker)) :
	(KNO_PRECHOICEP(f->future_value)) ?
	(kno_make_simple_choice(f->future_value)) :
	(kno_incref(f->future_value));
      u8_unlock_mutex(&f->future_lock);
      return retval;}}
  else return kno_incref(promise);
}

DEFC_PRIM("make-promise",make_promise_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "returns a promise which returns *value* when "
	  "FORCEd.",
	  {"value",kno_any_type,KNO_VOID})
static lispval make_promise_prim(lispval value)
{
  return (lispval) kno_init_future
    (NULL,value,KNO_VOID,NULL,
     KNO_FUTURE_COMPUTABLE|KNO_FUTURE_MONOTONIC|KNO_FUTURE_FINALIZED);
}

DEFC_PRIM("promise?",promisep_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if *value* is a promise (a computable future).",
	  {"value",kno_any_type,KNO_VOID})
static lispval promisep_prim(lispval value)
{
  if ( (KNO_TYPEP(value,kno_future_type)) &&
       (KNO_FUTURE_BITP(value,KNO_FUTURE_COMPUTABLE)) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("promise/broken?",promise_brokenp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if *value* is a promise which has yielded an error.",
	  {"value",kno_any_type,KNO_VOID})
static lispval promise_brokenp_prim(lispval value)
{
  if ( (KNO_TYPEP(value,kno_future_type)) &&
       (KNO_FUTURE_BITP(value,KNO_FUTURE_EXCEPTION)) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("promise/resolved?",promise_resolvedp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if *value* is a promise which has yielded a result or an error.",
	  {"value",kno_any_type,KNO_VOID})
static lispval promise_resolvedp_prim(lispval value)
{
  if ( (KNO_TYPEP(value,kno_future_type)) &&
       (KNO_FUTURE_RESOLVEDP((kno_future)value)) &&
       (KNO_FUTURE_FINALIZEDP((kno_future)value)) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("promise/satisfied?",promise_satisfiedp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if *value* is a promise which has yielded a result or an error.",
	  {"value",kno_any_type,KNO_VOID})
static lispval promise_satisfiedp_prim(lispval value)
{
  if ( (KNO_TYPEP(value,kno_future_type)) &&
       (KNO_FUTURE_RESOLVEDP((kno_future)value)) &&
       (KNO_FUTURE_FINALIZEDP((kno_future)value)) &&
       (!(KNO_FUTURE_EXCEPTIONP((kno_future)value))) )
    return KNO_TRUE;
  else return KNO_FALSE;
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
  KNO_LINK_CPRIM("promise?",promisep_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("make-promise",make_promise_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("promise/probe",probe_promise_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("promise/broken?",promise_brokenp_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("promise/resolved?",promise_resolvedp_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("promise/satisfied?",promise_satisfiedp_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("force",force_promise_prim,1,kno_scheme_module);
}
