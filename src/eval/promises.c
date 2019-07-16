/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
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

#include <libu8/libu8io.h>

#include <sys/types.h>
#include <kno/cprims.h>


/* Promises, Delays, etc */

static lispval delay_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval delay_expr = kno_get_arg(expr,1);
  if (KNO_VOIDP(delay_expr))
    return kno_err(kno_SyntaxError,"delay_evalfn",NULL,expr);
  else {
    struct KNO_PROMISE *promise = u8_alloc(struct KNO_PROMISE);
    KNO_INIT_FRESH_CONS(promise,kno_promise_type);
    u8_init_mutex(&promise->promise_lock);
    promise->promise_expr = kno_incref(delay_expr);
    promise->promise_env = kno_copy_env(env);
    promise->promise_consumers = KNO_EMPTY;
    if (KNO_EVALP(delay_expr))
      promise->promise_value = KNO_NULL;
    else promise->promise_value = kno_incref(delay_expr);
    return (lispval) promise;}
}

DEFPRIM1("force",force_promise_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(FORCE *promise*)` "
 "returns the value promised by *promise*. If "
 "*promise* is not a promise object, it is simply "
 "returned. Promises only compute their values "
 "once, so the result of the first call is memoized",
 kno_any_type,KNO_VOID);
static lispval force_promise_prim(lispval promise)
{
  if (KNO_TYPEP(promise,kno_promise_type)) {
    struct KNO_PROMISE *p = (kno_promise) promise;
    if (p->promise_value)
      return kno_incref(p->promise_value);
    else {
      u8_lock_mutex(&p->promise_lock);
      if (p->promise_value) {
	u8_unlock_mutex(&p->promise_lock);
	return kno_incref(p->promise_value);}
      lispval result = kno_stack_eval
	(p->promise_expr,p->promise_env,kno_stackptr,0);
      if (KNO_ABORTED(result)) {
	u8_exception ex = u8_current_exception;
	/* p->promise_value = kno_simple_exception(ex); */
	p->promise_value = kno_simple_exception(ex);
	p->promise_broken = 1;}
      else p->promise_value = kno_incref(result);
      lispval consumers = p->promise_consumers;
      p->promise_consumers = KNO_EMPTY;
      u8_unlock_mutex(&p->promise_lock);
      /* This should handle consumers somehow, TBD */
      kno_decref(consumers);
      return result;}}
  else return kno_incref(promise);
}

KNO_EXPORT lispval kno_force_promise(lispval promise)
{
  if (KNO_TYPEP(promise,kno_promise_type)) {
    struct KNO_PROMISE *p = (kno_promise) promise;
    if (p->promise_value)
      return kno_incref(p->promise_value);
    else {
      u8_lock_mutex(&p->promise_lock);
      if (p->promise_value) {
	u8_unlock_mutex(&p->promise_lock);
	return kno_incref(p->promise_value);}
      lispval result = kno_stack_eval
	(p->promise_expr,p->promise_env,kno_stackptr,0);
      if (KNO_ABORTED(result)) {
	u8_exception ex = u8_current_exception;
	/* p->promise_value = kno_simple_exception(ex); */
	p->promise_value = kno_simple_exception(ex);
	p->promise_broken = 1;}
      else p->promise_value = kno_incref(result);
      lispval consumers = p->promise_consumers;
      p->promise_consumers = KNO_EMPTY;
      u8_unlock_mutex(&p->promise_lock);
      /* This should handle consumers somehow, TBD */
      kno_decref(consumers);
      return result;}}
  else return kno_incref(promise);
}

DEFPRIM2("promise/probe",probe_promise_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(PROMISE/PROBE *promise* *marker*)` "
 "returns the value promised by *promise* if it has "
 "been resolved. If *promise* has not been "
 "resolved, *marker* is returned and if *promise* "
 "is not a promise it is returned.",
 kno_any_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval probe_promise_prim(lispval promise,lispval marker)
{
  if (KNO_TYPEP(promise,kno_promise_type)) {
    struct KNO_PROMISE *p = (kno_promise) promise;
    if (p->promise_value)
      return kno_incref(p->promise_value);
    else return kno_incref(marker);}
  else return kno_incref(promise);
}

DEFPRIM1("make-promise",make_promise_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(MAKE-PROMISE *value*)` "
 "returns a promise which returns *value* when "
 "FORCEd.",
 kno_any_type,KNO_VOID);
static lispval make_promise_prim(lispval value)
{
  struct KNO_PROMISE *promise = u8_alloc(struct KNO_PROMISE);
  KNO_INIT_FRESH_CONS(promise,kno_promise_type);
  u8_init_mutex(&promise->promise_lock);
  promise->promise_expr = kno_incref(value);
  promise->promise_env = NULL;
  promise->promise_consumers = KNO_EMPTY;
  promise->promise_value = kno_incref(value);
  return (lispval) promise;
}

DEFPRIM1("promise/resolved?",promise_resolvedp_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(PROMISE/RESOLVED? *promise*)` "
 "returns #t if *promise* has had its value "
 "computed and cached (or generated an error).",
 kno_promise_type,KNO_VOID);
static lispval promise_resolvedp_prim(lispval value)
{
  struct KNO_PROMISE *promise = (kno_promise) value;
  if (promise->promise_value)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("promise/broken?",promise_brokenp_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(PROMISE/BROKEN? *promise*)` "
 "returns #t if *promise* generated an error when "
 "evaluated.",
 kno_promise_type,KNO_VOID);
static lispval promise_brokenp_prim(lispval value)
{
  struct KNO_PROMISE *promise = (kno_promise) value;
  if ( (promise->promise_value) &&
       (promise->promise_broken) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("promise/satisfied?",promise_satisfiedp_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(PROMISE/SATISFIED? *promise*)` "
 "returns #t if *promise* has been computed and "
 "cached without error.",
 kno_promise_type,KNO_VOID);
static lispval promise_satisfiedp_prim(lispval value)
{
  struct KNO_PROMISE *promise = (kno_promise) value;
  if ( (promise->promise_value) &&
       (! (KNO_EXCEPTIONP(promise->promise_value)) ) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("promise?",promisep_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(PROMISE? *value*)` "
 "returns true if *value* is a promise.",
 kno_any_type,KNO_VOID);
static lispval promisep_prim(lispval value)
{
  if (KNO_TYPEP(value,kno_promise_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_EXPORT void recycle_promise(struct KNO_RAW_CONS *c)
{
  struct KNO_PROMISE *p = (struct KNO_PROMISE *)c;
  kno_decref(p->promise_expr);
  p->promise_expr = KNO_VOID;
  if (p->promise_value) {
    lispval v = p->promise_value;
    p->promise_value = KNO_NULL;
    kno_decref(v);}
  if (p->promise_env) {
    lispval envptr = (lispval)p->promise_env;
    if (!(KNO_STATICP(envptr)))
      kno_decref(envptr);
    p->promise_env = NULL;}
  if (!(EMPTYP(p->promise_consumers))) {
    kno_decref(p->promise_consumers);
    p->promise_consumers = KNO_EMPTY;}
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

static int unparse_promise(u8_output out,lispval x)
{
  struct KNO_PROMISE *p = (kno_promise) x;
  u8_printf(out,"#<PROMISE %s %q>",
	    ((p->promise_value == KNO_NULL) ? ("pending") :
	     (p->promise_broken) ? ("broken") :
             ("resolved")),
	    p->promise_expr);
  return 1;
}

/* Initialization */

static int promises_init = 0;

KNO_EXPORT int kno_init_promises_c()
{
  if (promises_init)
    return 0;
  else promises_init = 1;

  kno_type_names[kno_promise_type]=_("promise");
  kno_unparsers[kno_promise_type]=unparse_promise;
  kno_recyclers[kno_promise_type]=recycle_promise;

  init_local_cprims();
  kno_def_evalfn(kno_scheme_module,"DELAY",
		 "`(DELAY *expr*)` creates a *promise* to evalute *expr* in "
		 "the current environment, which is delivered when "
		 "the promise is *forced*.",
		 delay_evalfn);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/



static void init_local_cprims()
{
  KNO_LINK_PRIM("promise?",promisep_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("promise/satisfied?",promise_satisfiedp_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("promise/broken?",promise_brokenp_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("promise/resolved?",promise_resolvedp_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("make-promise",make_promise_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("promise/probe",probe_promise_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("force",force_promise_prim,1,kno_scheme_module);
}
