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
#include "kno/futures.h"

#include <libu8/libu8io.h>

#include <sys/types.h>

/* Basic stuff */

KNO_EXPORT struct KNO_FUTURE *kno_init_future
(struct KNO_FUTURE *future,
 lispval init,lispval source,
 future_handler updatefn,
 unsigned int bits)
{
  int do_alloc = (future == NULL);
  if (do_alloc) future = u8_alloc(struct KNO_FUTURE);
  KNO_INIT_FRESH_CONS(future,kno_future_type);
  future->future_bits = bits;
  future->future_value = init;
  future->future_callbacks = KNO_EMPTY;
  future->future_errfns = KNO_EMPTY;
  future->future_source=source;
  future->future_handler=updatefn;
  future->future_updated=-1;
  u8_init_mutex(&future->future_lock);
  u8_init_condvar(&future->future_condition);
  return future;
}

static void run_future_callbacks(kno_future f,lispval callbacks,lispval vals);

KNO_EXPORT int kno_change_future(kno_future f,lispval value,unsigned int flags)
{
  if (KNO_ABORTED(value)) return value;
  if (KNO_FUTURE_FINALIZEDP(f))
    return kno_reterr("FutureFinalized","kno_change_future",NULL,(lispval)f);
  u8_lock_mutex(&(f->future_lock));
  if (KNO_FUTURE_FINALIZEDP(f)) {
    lispval err=kno_err("FutureFinalized","kno_change_future",NULL,(lispval)f);
    u8_unlock_mutex(&(f->future_lock));
    return -1;}
  if (f->future_handler) {
    int rv = f->future_handler(f,value,flags);
    if (rv<0) {
      u8_unlock_mutex(&(f->future_lock));
      return rv;}
    else if (rv) {
      f->future_updated=time(NULL);
      u8_condvar_broadcast(&(f->future_condition));}
    else {
      /* Didn't do anything */
      u8_unlock_mutex(&(f->future_lock));
      return 0;}}
  if (flags&KNO_FUTURE_FINALIZED) {
    KNO_FUTURE_SET(f,KNO_FUTURE_FINALIZED);}
  if (flags&KNO_FUTURE_EXCEPTION) {
    KNO_FUTURE_SET(f,KNO_FUTURE_EXCEPTION);}
  int final = KNO_FUTURE_FINALIZEDP(f);
  lispval callbacks = (final) ? (f->future_callbacks) :
    (kno_incref(f->future_callbacks));
  if (final) f->future_callbacks=KNO_EMPTY;
  u8_unlock_mutex(&(f->future_lock));
  if (!(KNO_EMPTYP(callbacks))) {
    run_future_callbacks(f,callbacks,value);
    kno_decref(callbacks);}
  return 1;
}

KNO_EXPORT lispval kno_force_future(kno_future f,lispval dflt)
{
  if ( (f->future_value) && (KNO_FUTURE_FINALP(f)) )
    return kno_incref(f->future_value);
  else if (f->future_value) {
    u8_lock_mutex(&(f->future_lock));
    lispval result = (KNO_PRECHOICEP(f->future_value)) ?
      (kno_make_simple_choice(f->future_value)) :
      (kno_incref(f->future_value));
    u8_unlock_mutex(&(f->future_lock));
    return result;}
  else if ( (f->future_bits) & (KNO_FUTURE_COMPUTABLE) ) {
    int rv = kno_change_future(f,KNO_NULL,0);
    if (rv<0)
      return KNO_ERROR;
    else if ( (f->future_value) && (KNO_FUTURE_FINALP(f)) )
      return kno_incref(f->future_value);
    else if (f->future_value) {
      u8_lock_mutex(&(f->future_lock));
      lispval result = (KNO_PRECHOICEP(f->future_value)) ?
	(kno_make_simple_choice(f->future_value)) :
	(kno_incref(f->future_value));
      u8_unlock_mutex(&(f->future_lock));
      return result;}
    else return kno_incref(dflt);}
  else return kno_incref(dflt);
}

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
    /* Should there be a way to pass the future object itself into the call? */
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
  lispval errfns = f->future_callbacks;
  f->future_errfns = KNO_EMPTY;
  kno_decref(errfns);
  kno_decref(f->future_source); f->future_source=KNO_VOID;
  u8_destroy_mutex(&(f->future_lock));
  u8_destroy_condvar(&(f->future_condition));
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

static int unparse_future(u8_output out,lispval x)
{
  struct KNO_FUTURE *f = (kno_future) x;
  u8_string status = NULL;
  lispval source = f->future_source;
  if (f->future_value == KNO_NULL)
    status = "pending";
  else if (KNO_FUTURE_FINALIZEDP(f))
    status = "final";
  else if (KNO_FUTURE_EXCEPTIONP(f))
    status = "exception";
  else status = "partial";
  if (KNO_PAIRP(source))
    u8_printf(out,"#<FUTURE:%s #!0x%llx %q>",status,
	      (unsigned long long)f,KNO_CAR(source));
  else u8_printf(out,"#<FUTURE:%s #!0x%llx %q>",status,
		 (unsigned long long)f,source);
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

