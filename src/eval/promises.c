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

#include <libu8/libu8io.h>

#include <sys/types.h>

/* Basic stuff */

KNO_EXPORT struct KNO_PROMISE *kno_init_promise
(struct KNO_PROMISE *promise,enum KNO_PROMISE_TYPEVAL type,int flags)
{
  int do_alloc = (promise == NULL);
  if (do_alloc)
    promise = u8_alloc(struct KNO_PROMISE);
  KNO_INIT_FRESH_CONS(promise,kno_promise_type);
  promise->promise_bits = (flags&(~KNO_PROMISE_TYPEMASK)) | ((int)type);
  promise->promise_callbacks = KNO_EMPTY;
  switch (type) {
  case kno_eval_promise:
    promise->promise_body.eval.expr = KNO_VOID;
    promise->promise_body.eval.env = NULL;
    break;
  case kno_apply_promise:
    promise->promise_body.apply.fcn = KNO_VOID;
    promise->promise_body.apply.args = NULL;
    promise->promise_body.apply.n_args = 0;
    break;
  case kno_async_promise:
    promise->promise_body.async.source = KNO_VOID;
    promise->promise_body.async.task = KNO_VOID;
    break;
  default:
    if (do_alloc) u8_free(promise);
    u8_seterr("BadPromiseType","kno_init_promise",NULL);
    return NULL;
  }
  u8_init_mutex(&promise->promise_lock);
  u8_init_condvar(&promise->promise_condition);
  return promise;
}

static void run_promise_callbacks(kno_promise p,lispval callbacks,lispval vals);

KNO_EXPORT int kno_promise_resolve(kno_promise p,lispval value,int flags)
{
  lispval cur = p->promise_value;
  u8_lock_mutex(&(p->promise_lock));
  lispval callbacks = p->promise_callbacks;
  p->promise_value = value;
  if (flags&KNO_PROMISE_BROKEN) {
    KNO_PROMISE_SET(p,KNO_PROMISE_BROKEN);}
  else {
    KNO_PROMISE_CLEAR(p,KNO_PROMISE_BROKEN);}
  KNO_PROMISE_SET(p,KNO_PROMISE_FINAL);
  KNO_PROMISE_CLEAR(p,KNO_PROMISE_PARTIAL);
  p->promise_updated = time(NULL);
  p->promise_callbacks=KNO_EMPTY;
  u8_condvar_broadcast(&(p->promise_condition));
  u8_unlock_mutex(&(p->promise_lock));
  if (!(KNO_EMPTYP(callbacks))) {
    run_promise_callbacks(p,callbacks,value);
    kno_decref(callbacks);}
  return 1;
}

#if 0
KNO_EXPORT int kno_promise_add(kno_promise p,lispval values,int flags)
{
  u8_lock_mutex(&(p->promise_lock));
  p->promise_updated = time(NULL);
  if (KNO_ABORTED(values)) {}
  else {
    lispval cur = p->promise_value, next = cur;
    lispval callbacks = p->promise_callbacks;
    int free_callbacks = 0;
    kno_incref(values);
    if (next == KNO_NULL) next=values;
    else {KNO_ADD_TO_CHOICE(next,values);}
    p->promise_value = next;
    if (flags&KNO_PROMISE_BROKEN) {
      KNO_PROMISE_SET(p,KNO_PROMISE_BROKEN);}
    else {
      KNO_PROMISE_CLEAR(p,KNO_PROMISE_BROKEN);}
    if (!(flags&KNO_PROMISE_FINAL)) {
      KNO_PROMISE_SET(p,KNO_PROMISE_PARTIAL);}
    else {
      KNO_PROMISE_SET(p,KNO_PROMISE_FINAL);
      KNO_PROMISE_CLEAR(p,KNO_PROMISE_PARTIAL);}
    if (free_callbacks) {
      p->promise_callbacks=KNO_EMPTY;
      u8_condvar_broadcast(&(p->promise_condition));
      u8_unlock_mutex(&(p->promise_lock));
      if (!(KNO_EMPTYP(callbacks))) {
	run_promise_callbacks(p,callbacks,value);
	kno_decref(callbacks);}
      return 1;
    }
  }}
#endif

/* Running callbacks with smart argument pruning */

static int get_argcount(lispval handler,int n_args)
{
  struct KNO_FUNCTION *f = KNO_FUNCTION_INFO(handler);
  if (f == NULL) return n_args;
  int arity = f->fcn_arity, min_arity = f->fcn_min_arity;
  if (arity<0) {
    if (n_args>=min_arity) return n_args;}
  else if (n_args>arity)
    return arity;
  else if (n_args>=min_arity)
    return n_args;
  else NO_ELSE;
  u8_byte buf[24];
  kno_seterr(kno_TooFewArgs,"run_promise_callbacks",
	     u8_bprintf(buf,"%d",n_args),handler);
  return -1;
}

static void run_promise_callbacks(kno_promise p,lispval callbacks,lispval vals)
{
  KNO_DO_CHOICES(callback,callbacks) {
    lispval result = KNO_VOID;
    if ( (KNO_PAIRP(callback)) &&
	 (KNO_APPLICABLEP(KNO_CAR(callback))) &&
	 (KNO_VECTORP(KNO_CDR(callback))) ) {
      lispval handler = KNO_CAR(callback);
      lispval args = KNO_CDR(callback);
      int n_args = 2+KNO_VECTOR_LENGTH(args);
      lispval argvec[n_args];
      argvec[0]=vals; argvec[1]=(lispval)p;
      kno_lspcpy(argvec+2,KNO_VECTOR_ELTS(args),KNO_VECTOR_LENGTH(args));
      int min_args = -1, max_args = -1;
      int use_args = get_argcount(handler,n_args);
      if (use_args<0)
	result = KNO_ERROR;
      else result = kno_apply(handler,use_args,argvec);}
    else if (KNO_APPLICABLEP(callback))
      result = kno_apply(callback,1,&vals);
    else u8_log(LOGERR,"BadPromiseCallback","Can't apply %q to %q for %q",
		callback,vals,(lispval)p);
    if (KNO_ABORTED(result)) {
      u8_log(LOGERR,"PromiseCallBackError","For %q, applying %q to %q: %s",
	     (lispval)p,callback,vals,NULL);
      kno_clear_errors(1);}
    else {kno_decref(result);}}
}

/* Promises, Delays, etc */

static lispval delay_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval delay_expr = kno_get_arg(expr,1);
  if (KNO_VOIDP(delay_expr))
    return kno_err(kno_SyntaxError,"delay_evalfn",NULL,expr);
  else {
    struct KNO_PROMISE *promise = kno_init_promise(NULL,kno_eval_promise,0);
    promise->promise_body.eval.expr = kno_incref(delay_expr);
    promise->promise_body.eval.env  = kno_copy_env(env);
    if (!(KNO_EVALP(delay_expr))) {
      promise->promise_value = kno_incref(delay_expr);
      KNO_PROMISE_SET(promise,KNO_PROMISE_FINAL);}
    return (lispval) promise;}
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

DEFC_PRIM("promise/resolved?",promise_resolvedp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *promise* has had its value "
	  "computed and cached (or generated an error).",
	  {"value",kno_promise_type,KNO_VOID})
static lispval promise_resolvedp_prim(lispval value)
{
  struct KNO_PROMISE *promise = (kno_promise) value;
  if (promise->promise_value)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("promise/broken?",promise_brokenp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *promise* generated an error when "
	  "evaluated.",
	  {"value",kno_promise_type,KNO_VOID})
static lispval promise_brokenp_prim(lispval value)
{
  struct KNO_PROMISE *promise = (kno_promise) value;
  if ( (promise->promise_value) &&
       ((promise->promise_bits)&KNO_PROMISE_BROKEN) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("promise/satisfied?",promise_satisfiedp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *promise* has been computed and "
	  "cached without error.",
	  {"value",kno_promise_type,KNO_VOID})
static lispval promise_satisfiedp_prim(lispval value)
{
  struct KNO_PROMISE *promise = (kno_promise) value;
  if ( (promise->promise_value) &&
       (! (KNO_PROMISE_BROKENP(promise)) ) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("promise?",promisep_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if *value* is a promise.",
	  {"value",kno_any_type,KNO_VOID})
static lispval promisep_prim(lispval value)
{
  if (KNO_TYPEP(value,kno_promise_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_EXPORT void recycle_promise(struct KNO_RAW_CONS *c)
{
  struct KNO_PROMISE *p = (struct KNO_PROMISE *)c;
  enum KNO_PROMISE_TYPEVAL pt = KNO_PROMISE_TYPEOF(p);
  switch (pt) {
  case kno_eval_promise: {
    lispval expr = p->promise_body.eval.expr;
    kno_lexenv env = p->promise_body.eval.env;
    p->promise_body.eval.expr = KNO_VOID;
    p->promise_body.eval.env = NULL;
    kno_decref(p->promise_body.eval.expr);
    if (env) {
      lispval envptr = (lispval)env;
      if (!(KNO_STATICP(envptr)))
	kno_decref(envptr);}
    break;}
  case kno_apply_promise: {
    lispval fn = p->promise_body.apply.fcn;
    lispval *args = p->promise_body.apply.args;
    int n = p->promise_body.apply.n_args;
    p->promise_body.apply.fcn  = KNO_VOID;
    p->promise_body.apply.args = NULL;
    p->promise_body.apply.n_args = -1;
    kno_decref(fn);
    kno_decref_elts(args,n);
    u8_free(args);
    break;}
  case kno_async_promise: {
    lispval source=p->promise_body.async.source;
    lispval task=p->promise_body.async.task;
    lispval fallback=p->promise_body.async.fallback;
    p->promise_body.async.source=KNO_VOID;
    p->promise_body.async.task=KNO_VOID;
    p->promise_body.async.fallback=KNO_VOID;
    kno_decref(source);
    kno_decref(task);
    kno_decref(fallback);
    break;}
  default: {}
  }
  if (p->promise_value) {
    lispval v = p->promise_value;
    p->promise_value = KNO_NULL;
    kno_decref(v);}
  lispval callbacks = p->promise_callbacks;
  p->promise_callbacks = KNO_EMPTY;
  kno_decref(callbacks);
  u8_destroy_mutex(&(p->promise_lock));
  u8_destroy_condvar(&(p->promise_condition));
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

static int unparse_promise(u8_output out,lispval x)
{
  struct KNO_PROMISE *p = (kno_promise) x;
  u8_string typename = NULL, status = NULL;
  lispval source = KNO_VOID;
  switch (KNO_PROMISE_TYPEOF(p)) {
  case kno_eval_promise:
    typename="eval";
    source=p->promise_body.eval.expr;
    break;
  case kno_apply_promise:
    typename="apply";
    source=p->promise_body.apply.fcn;
    break;
  case kno_async_promise:
    typename="async";
    source=p->promise_body.async.task;
      if (KNO_VOIDP(source))
	source=p->promise_body.async.source;
    break;
  default:
    typename="corrupt"; break;
  }
  if (p->promise_value == KNO_NULL)
    status = "pending";
  else if (KNO_PROMISE_BROKENP(p))
    status = "broken";
  else if (KNO_PROMISE_PARTIALP(p))
    status = "partial";
  else if (KNO_PROMISE_FINALP(p))
    status = "final";
  else status = "resolved";
  u8_printf(out,"#<PROMISE:%s %s %q>",typename,status,source);
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
  KNO_LINK_CPRIM("promise/satisfied?",promise_satisfiedp_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("promise/broken?",promise_brokenp_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("promise/resolved?",promise_resolvedp_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("make-promise",make_promise_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("promise/probe",probe_promise_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("force",force_promise_prim,1,kno_scheme_module);
}
