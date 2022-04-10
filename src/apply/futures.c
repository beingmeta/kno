/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include "kno/cprims.h"

#include <libu8/libu8io.h>

#include <sys/types.h>

/* Basic stuff */

KNO_EXPORT struct KNO_FUTURE *kno_init_future
(struct KNO_FUTURE *future,
 lispval init,lispval source,
 future_updatefn updatefn,
 unsigned int bits)
{
  int do_alloc = (future == NULL);
  if (do_alloc) future = u8_alloc(struct KNO_FUTURE);
  KNO_INIT_FRESH_CONS(future,kno_future_type);
  future->future_bits = bits;
  future->future_value = init;
  future->future_callbacks = KNO_EMPTY;
  future->future_source=source;
  future->future_updatefn=updatefn;
  future->future_updated=-1;
  u8_init_mutex(&future->future_lock);
  u8_init_condvar(&future->future_condition);
  return future;
}

static void run_future_callbacks(kno_future f,lispval callbacks,lispval vals);

KNO_EXPORT lispval kno_future_update(kno_future f,lispval value,unsigned int flags)
{
  u8_lock_mutex(&(p->future_lock));
  lispval cur = f->future_value;
  unsigned int cflags = f->future_bits;
  if (KNO_ABORTED(value)) {
    u8_exception ex = u8_pop_exception();
    if (KNO_FUTURE_ROBUSTP(f)) {
      u8_condition c = (ex) ? (ex->u8x_cond) ? ("MysteriousError");
      u8_log(LOGERR,"Ignoring error %s for %q: %s",c,(lispval)f);
      value=kno_exception_object(ex);
      if (ex) u8_free_exception(ex);}
    else {
      KNO_FUTURE_SET(p,KNO_FUTURE_EXCEPTION);
      f->future_value=kno_exception_object(ex);}}
  else {
    if (f->future_updatefn)
      f->future_updatefn(f,value,flags);
    else {
      p->future_value = kno_incref(value);
      if (cur) kno_decref(cur);}
    if (flags&KNO_FUTURE_EXCEPTION) {
      KNO_FUTURE_SET(f,KNO_FUTURE_EXCEPTION);}
    else {
      KNO_FUTURE_CLEAR(f,KNO_FUTURE_EXCEPTION);}}
  f->future_updated = time(NULL);
  int monotonic = ( (cflags) && (KNO_FUTURE_MONOTONIC) );
  lispval callbacks = (monotonic) ? (f->future_callbacks) : (kno_incref(f->future_callbacks));
  if (monotonic) f->future_callbacks=KNO_EMPTY;
  u8_condvar_broadcast(&(f->future_condition));
  u8_unlock_mutex(&(f->future_lock));
  if (!(KNO_EMPTYP(callbacks))) {
    run_future_callbacks(f,callbacks,value);
    kno_decref(callbacks);}
  return 1;
}

KNO_EXPORT lispval kno_future_value(kno_future f,unsigned int flags,lispval dflt)
{
  if (KNO_FUTURE_BITP(f,KNO_FUTURE_FINALIZED)) {
    if (f->future_value)
      return kno_incref(f->future_value);
    else return kno_incref(dflt);}
  else if (f->future_value == KNO_NULL) {
    int rv = f->future_update(f,KNO_NULL,flags);
    if (f->future_value)
      return kno_incref(f->future_value);}
  lispval cur = p->future_value;
  u8_lock_mutex(&(p->future_lock));
  lispval callbacks = p->future_callbacks;
  p->future_value = value;
  if (flags&KNO_FUTURE_BROKEN) {
    KNO_FUTURE_SET(p,KNO_FUTURE_BROKEN);}
  else {
    KNO_FUTURE_CLEAR(p,KNO_FUTURE_BROKEN);}
  KNO_FUTURE_SET(p,KNO_FUTURE_FINAL);
  KNO_FUTURE_CLEAR(p,KNO_FUTURE_PARTIAL);
  p->future_updated = time(NULL);
  p->future_callbacks=KNO_EMPTY;
  u8_condvar_broadcast(&(p->future_condition));
  u8_unlock_mutex(&(p->future_lock));
  if (!(KNO_EMPTYP(callbacks))) {
    run_future_callbacks(p,callbacks,value);
    kno_decref(callbacks);}
  return 1;
}

#if 0
KNO_EXPORT int kno_future_add(kno_future p,lispval values,int flags)
{
  u8_lock_mutex(&(p->future_lock));
  p->future_updated = time(NULL);
  if (KNO_ABORTED(values)) {}
  else {
    lispval cur = p->future_value, next = cur;
    lispval callbacks = p->future_callbacks;
    int free_callbacks = 0;
    kno_incref(values);
    if (next == KNO_NULL) next=values;
    else {KNO_ADD_TO_CHOICE(next,values);}
    p->future_value = next;
    if (flags&KNO_FUTURE_BROKEN) {
      KNO_FUTURE_SET(p,KNO_FUTURE_BROKEN);}
    else {
      KNO_FUTURE_CLEAR(p,KNO_FUTURE_BROKEN);}
    if (!(flags&KNO_FUTURE_FINAL)) {
      KNO_FUTURE_SET(p,KNO_FUTURE_PARTIAL);}
    else {
      KNO_FUTURE_SET(p,KNO_FUTURE_FINAL);
      KNO_FUTURE_CLEAR(p,KNO_FUTURE_PARTIAL);}
    if (free_callbacks) {
      p->future_callbacks=KNO_EMPTY;
      u8_condvar_broadcast(&(p->future_condition));
      u8_unlock_mutex(&(p->future_lock));
      if (!(KNO_EMPTYP(callbacks))) {
	run_future_callbacks(p,callbacks,value);
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
  kno_seterr(kno_TooFewArgs,"run_future_callbacks",
	     u8_bprintf(buf,"%d",n_args),handler);
  return -1;
}

static void run_future_callbacks(kno_future p,lispval callbacks,lispval vals)
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
    else u8_log(LOGERR,"BadFutureCallback","Can't apply %q to %q for %q",
		callback,vals,(lispval)p);
    if (KNO_ABORTED(result)) {
      u8_log(LOGERR,"FutureCallBackError","For %q, applying %q to %q: %s",
	     (lispval)p,callback,vals,NULL);
      kno_clear_errors(1);}
    else {kno_decref(result);}}
}

KNO_EXPORT void recycle_future(struct KNO_RAW_CONS *c)
{
  struct KNO_FUTURE *f = (struct KNO_FUTURE *)c;
  if (f->future_value) {
    lispval v = f->future_value;
    f->future_value = KNO_NULL;
    kno_decref(v);}
  lispval callbacks = f->future_callbacks;
  f->future_callbacks = KNO_EMPTY;
  kno_decref(callbacks);
  kno_decref(f->future_source); f->future_source=KNO_VOID;
  kno_decref(f->future_context); f->future_context=KNO_VOID;
  u8_destroy_mutex(&(f->future_lock));
  u8_destroy_condvar(&(f->future_condition));
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

static int unparse_future(u8_output out,lispval x)
{
  struct KNO_FUTURE *f = (kno_future) x;
  u8_string typename = NULL, status = NULL;
  lispval source = f->future_source;
  if (f->future_value == KNO_NULL)
    status = "pending";
  else if (KNO_FUTURE_BROKENP(p))
    status = "broken";
  else if (KNO_FUTURE_PARTIALP(p))
    status = "partial";
  else if (KNO_FUTURE_FINALP(p))
    status = "final";
  else status = "resolved";
  u8_printf(out,"#<FUTURE:%s %s %q>",typename,status,source);
  return 1;
}

/* Initialization */

static int futures_init = 0;

KNO_EXPORT int kno_init_futures_c()
{
  if (futures_init)
    return 0;
  else futures_init = 1;

  kno_type_names[kno_future_type]=_("future");
  kno_unparsers[kno_future_type]=unparse_future;
  kno_recyclers[kno_future_type]=recycle_future;

  u8_register_source_file(_FILEINFO);

  return 1;
}

