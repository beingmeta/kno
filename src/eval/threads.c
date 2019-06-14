/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL 1
#define KNO_INLINE_TABLES 1
#define KNO_INLINE_FCNIDS 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/support.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/dtproc.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/ports.h"
#include "kno/dtcall.h"
#include "kno/cprims.h"

#include "eval_internals.h"

#include <libu8/u8elapsed.h>
#include <libu8/u8timefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8logging.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>

static u8_condition ThreadReturnError=_("ThreadError");
static u8_condition ThreadExit=_("ThreadExit");
static u8_condition ThreadBacktrace=_("ThreadBacktrace");

static int thread_loglevel = LOG_NOTICE;
static int thread_log_exit = 1;
static lispval logexit_symbol = VOID, timeout_symbol = VOID;
static lispval void_symbol = VOID;

#ifndef U8_STRING_ARG
#define U8_STRING_ARG(s) (((s) == NULL)?((u8_string)""):((u8_string)(s)))
#endif

#define notexited(tstruct) (! ( ((tstruct)->flags) & (KNO_THREAD_DONE) ) )

#define SYNC_TYPEP(x,tp)                                        \
  ( (((struct KNO_SYNCHRONIZER *)x)->synctype) == (tp) )

/* Thread structures */

static struct KNO_THREAD_STRUCT *thread_ring=NULL;
u8_mutex thread_ring_lock;

kno_ptr_type kno_thread_type;
kno_ptr_type kno_synchronizer_type;

static void add_thread(struct KNO_THREAD_STRUCT *thread)
{
  if (thread == NULL)
    u8_log(LOG_CRIT,"AddThreadError","NULL thread added to thread ring!");
  else if ( (thread->ring_left) || (thread->ring_right) )
    u8_log(LOG_WARN,"RedundantAddThread",
           "'New' thread object %q is being added again",
           (lispval)thread);
  else {
    u8_lock_mutex(&thread_ring_lock);
    thread->ring_right=thread_ring;
    if (thread_ring) thread_ring->ring_left=thread;
    thread->ring_left=NULL;
    thread_ring=thread;
    u8_unlock_mutex(&thread_ring_lock);}
}

static void remove_thread(struct KNO_THREAD_STRUCT *thread)
{
  if (thread == NULL)
    u8_log(LOG_CRIT,"RemoveThreadError",
           "Attempt to remove NULL thread description!");
  else {
    u8_lock_mutex(&thread_ring_lock);
    struct KNO_THREAD_STRUCT *left = thread->ring_left;
    struct KNO_THREAD_STRUCT *right = thread->ring_right;
    if (left) {
      assert( left->ring_right == thread );
      left->ring_right = right;}
    if (right) {
      assert( right->ring_left == thread );
      right->ring_left = left;}
    if ( thread_ring == thread )
      thread_ring = right;
    thread->ring_left = NULL;
    thread->ring_right = NULL;
    u8_unlock_mutex(&thread_ring_lock);}
}

DCLPRIM1("THREAD?",threadp_prim,MIN_ARGS(1),
         "`(THREAD? *object*)` returns #t if *object is a thread.",
         kno_any_type,KNO_VOID)
static lispval threadp_prim(lispval arg)
{
  if (KNO_TYPEP(arg,kno_thread_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Finding threads by numeric ID */

DCLPRIM2("FIND-THREAD",findthread_prim,MIN_ARGS(1),
         "(FIND-THREAD [*id*] [*err*]) returns the thread object for "
         "the the thread numbered *id* (which is the value returned "
         "by (threadid)). If *id* is not provided or #default, returns "
         "the thread object for the current thread. If a thread object "
         "doesn't exist, either reports an error if *err* is not-false or"
         "returns #f otherwise.",
         kno_fixnum_type,KNO_VOID,kno_any_type,KNO_VOID)
static lispval findthread_prim(lispval threadid_arg,lispval err)
{
  long long threadid =
    ( (KNO_VOIDP(threadid_arg)) || (KNO_DEFAULTP(threadid_arg)) ) ?
    ( u8_threadid() ) :
    (KNO_INTEGERP(threadid_arg)) ? (kno_getint(threadid_arg)) :
    (-1);
  if (threadid>0) {
    u8_lock_mutex(&thread_ring_lock);
    struct KNO_THREAD_STRUCT *scan = thread_ring;
    while (scan) {
      if (scan->threadid == threadid) {
        lispval found = (lispval) scan;
        kno_incref(found);
        u8_unlock_mutex(&thread_ring_lock);
        return found;}
      else scan = scan->ring_right;}
    u8_unlock_mutex(&thread_ring_lock);
    return KNO_FALSE;}
  if (KNO_VOIDP(err))
    return KNO_EMPTY;
  else if (KNO_FALSEP(err))
    return KNO_FALSE;
  else return kno_err("NoThreadID","findthread_prim",NULL,threadid_arg);
}

DCLPRIM("THREAD-ID",threadid_prim,MIN_ARGS(1)|MAX_ARGS(1),
        "`(THREAD-ID *thread*)` returns the integer identifier for *thread*")
static lispval threadid_prim(lispval thread)
{
  struct KNO_THREAD_STRUCT *th = (kno_thread_struct) thread;
  return KNO_INT(th->threadid);
}

static lispval allthreads_config_get(lispval var,U8_MAYBE_UNUSED void *data)
{
  lispval results = EMPTY;
  u8_lock_mutex(&thread_ring_lock);
  struct KNO_THREAD_STRUCT *scan = thread_ring;
  while (scan) {
    lispval thread = (lispval) scan;
    kno_incref(thread);
    CHOICE_ADD(results,thread);
    scan=scan->ring_right;}
  u8_unlock_mutex(&thread_ring_lock);
  return results;
}

/* Thread functions */

int kno_thread_backtrace = 0;

static int unparse_thread_struct(u8_output out,lispval x)
{
  struct KNO_THREAD_STRUCT *th=
    kno_consptr(struct KNO_THREAD_STRUCT *,x,kno_thread_type);
  if (th->flags&KNO_EVAL_THREAD)
    u8_printf(out,"#<THREAD (%lld)0x%x%s eval %q>",
              th->threadid,(unsigned long)(th->tid),
              ((th->flags&KNO_THREAD_ERROR) ? (" error") :
               (th->flags&KNO_THREAD_DONE) ? (" done") :
               ("live")),
              th->evaldata.expr);
  else u8_printf(out,"#<THREAD (%lld)0x%x%s apply %q>",
                 th->threadid,(unsigned long)(th->tid),
                 ((th->flags&KNO_THREAD_ERROR) ? (" error") :
                  (th->flags&KNO_THREAD_DONE) ? (" done") :
                  ("live")),
                 th->applydata.fn);
  return 1;
}

KNO_EXPORT void recycle_thread_struct(struct KNO_RAW_CONS *c)
{
  struct KNO_THREAD_STRUCT *th = (struct KNO_THREAD_STRUCT *)c;
  remove_thread(th);
  if (th->flags&KNO_EVAL_THREAD) {
    if (th->evaldata.env) {
      lispval free_env = (lispval) th->evaldata.env;
      th->evaldata.env = NULL;
      kno_decref(free_env);}
    kno_decref(th->evaldata.expr);}
  else {
    int i = 0, n = th->applydata.n_args;
    lispval *args = th->applydata.args;
    while (i<n) {kno_decref(args[i]); i++;}
    kno_decref(th->applydata.fn);
    u8_free(args);}
  if (th->result!=KNO_NULL) kno_decref(th->result);
  th->resultptr = NULL;
  pthread_attr_destroy(&(th->attr));
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

/* Synchronizer methods */

static int unparse_synchronizer(u8_output out,lispval obj)
{
  struct KNO_SYNCHRONIZER *sync =
    kno_consptr(struct KNO_SYNCHRONIZER *,obj,kno_synchronizer_type);
  u8_string synctype = "synchronizer";
  switch (sync->synctype) {
  case sync_condvar:
    synctype = "CONDVAR"; break;
  case sync_mutex:
    synctype = "MUTEX"; break;
  case sync_rwlock:
    synctype = "RWLOCK"; break;}
  u8_printf(out,"#<%s %lx>",synctype,obj);
  return 1;
}

KNO_EXPORT void recycle_synchronizer(struct KNO_RAW_CONS *c)
{
  struct KNO_SYNCHRONIZER *sync = (struct KNO_SYNCHRONIZER *)c;
  switch (sync->synctype) {
  case sync_condvar: {
    u8_destroy_mutex(&(sync->obj.condvar.lock));
    u8_destroy_condvar(&(sync->obj.condvar.cvar));
    break;}
  case sync_mutex: {
    u8_destroy_mutex(&(sync->obj.mutex));
    break;}
  case sync_rwlock: {
    u8_destroy_rwlock(&(sync->obj.rwlock));
    break;}}
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

DCLPRIM1("SYNCHRONIZER?",synchronizerp_prim,MIN_ARGS(1),
         "`(SYNCHRONIZER? *obj*)` returns #t if *obj* is a synchronizer. "
         "Synchronizers currently include condvars and synchronized lambdas.",
         kno_any_type,KNO_VOID)
static lispval synchronizerp_prim(lispval arg)
{
  if (KNO_TYPEP(arg,kno_synchronizer_type))
    return KNO_TRUE;
  else if (KNO_LAMBDAP(arg)) {
    struct KNO_LAMBDA *sp = kno_consptr(kno_lambda,arg,kno_lambda_type);
    if (sp->lambda_synchronized)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DCLPRIM1("CONDVAR?",condvarp_prim,MIN_ARGS(1),
         "`(CONDVAR? *object*)` returns #t if *object* is a condition variable",
         kno_any_type,KNO_VOID)
static lispval condvarp_prim(lispval arg)
{
  if ( (KNO_TYPEP(arg,kno_synchronizer_type)) &&
       (SYNC_TYPEP(arg,sync_condvar)) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DCLPRIM1("MUTEX?",mutexp_prim,MIN_ARGS(1),
         "`(MUTEX? *object*)` returns #t if *object* is a mutex",
         kno_any_type,KNO_VOID)
static lispval mutexp_prim(lispval arg)
{
  if ( (KNO_TYPEP(arg,kno_synchronizer_type)) &&
       (SYNC_TYPEP(arg,sync_mutex)) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DCLPRIM1("RWLOCK?",rwlockp_prim,MIN_ARGS(1),
         "`(RWLOCK? *object*)` returns #t if *object* is a mutex",
         kno_any_type,KNO_VOID)
static lispval rwlockp_prim(lispval arg)
{
  if ( (KNO_TYPEP(arg,kno_synchronizer_type)) &&
       (SYNC_TYPEP(arg,sync_rwlock)) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* MUTEXes */

DCLPRIM("MAKE-MUTEX",make_mutex,0,
        "(MAKE-MUTEX) allocates and returns a new mutex.")
static lispval make_mutex()
{
  int rv = 0;
  struct KNO_SYNCHRONIZER *cv = u8_alloc(struct KNO_SYNCHRONIZER);
  KNO_INIT_FRESH_CONS(cv,kno_synchronizer_type);
  cv->synctype = sync_mutex;
  rv = u8_init_mutex(&(cv->obj.mutex));
  if (rv) {
    u8_graberrno("make_mutex",NULL);
    u8_free(cv);
    return KNO_ERROR;}
  else return LISP_CONS(cv);
}

DCLPRIM("MAKE-RWLOCK",make_rwlock,0,
        "(MAKE-RWLOCK) allocates and returns a new read/write lock.")
static lispval make_rwlock()
{
  int rv = 0;
  struct KNO_SYNCHRONIZER *cv = u8_alloc(struct KNO_SYNCHRONIZER);
  KNO_INIT_FRESH_CONS(cv,kno_synchronizer_type);
  cv->synctype = sync_rwlock;
  rv = u8_init_rwlock(&(cv->obj.rwlock));
  if (rv) {
    u8_graberrno("make_rwlock",NULL);
    u8_free(cv);
    return KNO_ERROR;}
  else return LISP_CONS(cv);
}

DCLPRIM("MAKE-CONDVAR",make_condvar,0,
        "(MAKE-CONDVAR) allocates and returns a new condvar.")
static lispval make_condvar()
{
  int rv = 0;
  struct KNO_SYNCHRONIZER *cv = u8_alloc(struct KNO_SYNCHRONIZER);
  KNO_INIT_FRESH_CONS(cv,kno_synchronizer_type);
  cv->synctype = sync_condvar;
  rv = u8_init_mutex(&(cv->obj.condvar.lock));
  if (rv) {
    u8_graberrno("make_condvar",NULL);
    u8_free(cv);
    return KNO_ERROR;}
  else rv = u8_init_condvar(&(cv->obj.condvar.cvar));
  if (rv) {
    u8_graberrno("make_condvar",NULL);
    return KNO_ERROR;}
  return LISP_CONS(cv);
}

/* Generic locking/unlocking/withlocking */

static void lock_synchronizer(struct KNO_SYNCHRONIZER *sync)
{
  switch (sync->synctype) {
  case sync_mutex:
    u8_lock_mutex(&(sync->obj.mutex)); return;
  case sync_rwlock:
    u8_write_lock(&(sync->obj.rwlock)); return;
  case sync_condvar:
    u8_lock_mutex(&(sync->obj.condvar.lock));
    return;}
}

static void unlock_synchronizer(struct KNO_SYNCHRONIZER *sync)
{
  switch (sync->synctype) {
  case sync_mutex:
    u8_unlock_mutex(&(sync->obj.mutex)); return;
  case sync_rwlock:
    u8_rw_unlock(&(sync->obj.rwlock)); return;
  case sync_condvar:
    u8_unlock_mutex(&(sync->obj.condvar.lock));
    return;}
}

/* These functions generically access the locks on CONDVARs
   and LAMBDAs */

DCLPRIM("SYNC/LOCK!",sync_lock,MIN_ARGS(1)|MAX_ARGS(1),
        "`(SYNC/LOCK! *synchornizer*)` Locks *synchronizer*, "
        "which can be a mutex, a read/write lock, a condition variable "
        "or a synchronized lambda.")
static lispval sync_lock(lispval lck)
{
  if (TYPEP(lck,kno_synchronizer_type)) {
    struct KNO_SYNCHRONIZER *sync =
      kno_consptr(struct KNO_SYNCHRONIZER *,lck,kno_synchronizer_type);
    lock_synchronizer(sync);
    return KNO_TRUE;}
  else if (KNO_LAMBDAP(lck)) {
    struct KNO_LAMBDA *sp = kno_consptr(kno_lambda,lck,kno_lambda_type);
    if (sp->lambda_synchronized) {
      u8_lock_mutex(&(sp->lambda_lock));}
    else return kno_type_error("lockable","synchro_lock",lck);
    return KNO_TRUE;}
  else return kno_type_error("lockable","synchro_lock",lck);
}

DCLPRIM("SYNC/RELEASE!",sync_unlock,MIN_ARGS(1)|MAX_ARGS(1),
        "`(SYNC/RELEASE! *synchronizer*)` Releases the lock on *synchronizer*, "
        "which can be a mutex, a read/write lock, a condition variable "
        "or a synchronized lambda.")
static lispval sync_unlock(lispval lck)
{
  if (TYPEP(lck,kno_synchronizer_type)) {
    struct KNO_SYNCHRONIZER *sync =
      kno_consptr(struct KNO_SYNCHRONIZER *,lck,kno_synchronizer_type);
    unlock_synchronizer(sync);
    return KNO_TRUE;}
  else if (KNO_LAMBDAP(lck)) {
    struct KNO_LAMBDA *sp = kno_consptr(kno_lambda,lck,kno_lambda_type);
    if (sp->lambda_synchronized) {
      u8_unlock_mutex(&(sp->lambda_lock));}
    else return kno_type_error("lockable","synchro_lock",lck);
    return KNO_TRUE;}
  else return kno_type_error("lockable","synchro_unlock",lck);
}

DCLPRIM("SYNC/READ/LOCK!",sync_read_lock,MIN_ARGS(1)|MAX_ARGS(1),
        "`(SYNC/READ/LOCK! *readwritelock*)` Locks *readwritelock* for reading. "
        "This lock can be released with `SYNC/RELEASE!`")
static lispval sync_read_lock(lispval lck)
{
  if (TYPEP(lck,kno_synchronizer_type)) {
    struct KNO_SYNCHRONIZER *sync =
      kno_consptr(struct KNO_SYNCHRONIZER *,lck,kno_synchronizer_type);
    if (sync->synctype == sync_rwlock)
      u8_read_lock(&(sync->obj.rwlock));
    else lock_synchronizer(sync);
    return KNO_TRUE;}
  else return kno_type_error("lockable","synchro_read_lock",lck);
}

static lispval with_lock_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval lock_expr = kno_get_arg(expr,1), lck, value = VOID;
  u8_mutex *mutex = NULL; u8_rwlock *rwlock = NULL;
  if (VOIDP(lock_expr))
    return kno_err(kno_SyntaxError,"with_lock_evalfn",NULL,expr);
  else lck = kno_eval(lock_expr,env);
  if (KNO_ABORTED(lck))
    return lck;
  else if (TYPEP(lck,kno_synchronizer_type)) {
    struct KNO_SYNCHRONIZER *sync =
      kno_consptr(struct KNO_SYNCHRONIZER *,lck,kno_synchronizer_type);
    switch (sync->synctype) {
    case sync_mutex:
      mutex = &(sync->obj.mutex); break;
    case sync_rwlock:
      rwlock = &(sync->obj.rwlock); break;
    case sync_condvar:
      mutex = &(sync->obj.condvar.lock); break;}}
  else if (KNO_LAMBDAP(lck)) {
    struct KNO_LAMBDA *sp = kno_consptr(kno_lambda,lck,kno_lambda_type);
    if (sp->lambda_synchronized) {
      mutex = &(sp->lambda_lock);}
    else {
      kno_decref(lck);
      return kno_type_error("lockable","with_lock_evalfn",lck);}}
  else {
    kno_decref(lck);
    return kno_type_error("lockable","with_lock_evalfn",lck);}
  {U8_WITH_CONTOUR("WITH-LOCK",0) {
      if (mutex)
        u8_lock_mutex(mutex);
      else u8_write_lock(rwlock);
      KNO_DOLIST(elt_expr,KNO_CDDR(expr)) {
        kno_decref(value);
        value = kno_eval(elt_expr,env);
        if (KNO_ABORTED(value)) {
          if (mutex)
            u8_unlock_mutex(mutex);
          else u8_rw_unlock(rwlock);
          kno_decref(lck);
          break;}}}
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      kno_decref(value);
      value = KNO_ERROR;}
    U8_END_EXCEPTION;}
  if (mutex)
    u8_unlock_mutex(mutex);
  else u8_rw_unlock(rwlock);
  kno_decref(lck);
  return value;
}

/* CONDVAR functions */

/* This primitive combine cond_wait and cond_timedwait
   through a second (optional) argument, which is an
   interval in seconds. */
DCLPRIM("CONDVAR/WAIT",condvar_wait,MIN_ARGS(1)|MAX_ARGS(2),
        "(CONDVAR/WAIT *condvar*) waits for the value associated with "
        "*condvar* to change.")
static lispval condvar_wait(lispval cvar,lispval timeout)
{
  int rv = 0;
  if (!(SYNC_TYPEP(cvar,sync_condvar)))
    return kno_type_error("condvar","condvar_wait",cvar);
  struct KNO_SYNCHRONIZER *cv =
    kno_consptr(struct KNO_SYNCHRONIZER *,cvar,kno_synchronizer_type);
  if (VOIDP(timeout))
    if ((rv = u8_condvar_wait(&(cv->obj.condvar.cvar),&(cv->obj.condvar.lock)))==0)
      return KNO_TRUE;
    else {
      return kno_type_error(_("valid condvar"),"condvar_wait",cvar);}
  else {
    struct timespec tm;
    if ((FIXNUMP(timeout)) && (FIX2INT(timeout)>=0)) {
      long long ival = FIX2INT(timeout);
      tm.tv_sec = time(NULL)+ival; tm.tv_nsec = 0;}
#if 0 /* Define this later.  This allows sub-second waits but
         is a little bit tricky. */
    else if (KNO_FLONUMP(timeout)) {
      double flo = KNO_FLONUM(cvar);
      if (flo>=0) {
        int secs = floor(flo)/1000000;
        int nsecs = (flo-(secs*1000000))*1000.0;
        tm.tv_sec = secs; tm.tv_nsec = nsecs;}
      else return kno_type_error(_("time interval"),"condvar_wait",timeout);}
#endif
    else return kno_type_error(_("time interval"),"condvar_wait",timeout);
    rv = u8_condvar_timedwait(&(cv->obj.condvar.cvar),&(cv->obj.condvar.lock),&tm);
    if (rv==0)
      return KNO_TRUE;
    else if (rv == ETIMEDOUT)
      return KNO_FALSE;
    return kno_type_error(_("valid condvar"),"condvar_wait",cvar);}
}

DCLPRIM("CONDVAR/SIGNAL",condvar_signal,MIN_ARGS(1)|MAX_ARGS(2),
        "(CONDVAR/SIGNAL *condvar* [*broadcast*]) signals that *condvar* "
        "has changed, signalling threads waiting on the value and causing their "
        "`thread-wait` calls to return. If *broadcast* is #t, all waiting threads "
        "are signalled. Otherwise only one is signalled.")
static lispval condvar_signal(lispval cvar,lispval broadcast)
{
  if (!(SYNC_TYPEP(cvar,sync_condvar)))
    return kno_type_error("condvar","condvar_wait",cvar);
  struct KNO_SYNCHRONIZER *cv=
    kno_consptr(struct KNO_SYNCHRONIZER *,cvar,kno_synchronizer_type);
  if (KNO_TRUEP(broadcast))
    if (u8_condvar_broadcast(&(cv->obj.condvar.cvar))==0)
      return KNO_TRUE;
    else return kno_type_error(_("valid condvar"),"condvar_signal",cvar);
  else if (u8_condvar_signal(&(cv->obj.condvar.cvar))==0)
    return KNO_TRUE;
  else return kno_type_error(_("valid condvar"),"condvar_signal",cvar);
}

DCLPRIM("CONDVAR/LOCK!",condvar_lock,MIN_ARGS(1)|MAX_ARGS(1),
        "`(CONDVAR/LOCK! *condvar*)` locks *condvar* (or precisely, "
        "its mutex)..")
static lispval condvar_lock(lispval cvar)
{
  if (!(SYNC_TYPEP(cvar,sync_condvar)))
    return kno_type_error("condvar","condvar_wait",cvar);
  struct KNO_SYNCHRONIZER *cv=
    kno_consptr(struct KNO_SYNCHRONIZER *,cvar,kno_synchronizer_type);
  u8_lock_mutex(&(cv->obj.condvar.lock));
  return KNO_TRUE;
}

DCLPRIM("CONDVAR/UNLOCK!",condvar_unlock,MIN_ARGS(1)|MAX_ARGS(1),
        "`(CONDVAR/UNLOCK! *condvar*)` unlocks *condvar* (or precisely, "
        "its mutex)..")
static lispval condvar_unlock(lispval cvar)
{
  if (!(SYNC_TYPEP(cvar,sync_condvar)))
    return kno_type_error("condvar","condvar_wait",cvar);
  struct KNO_SYNCHRONIZER *cv=
    kno_consptr(struct KNO_SYNCHRONIZER *,cvar,kno_synchronizer_type);
  u8_unlock_mutex(&(cv->obj.condvar.lock));
  return KNO_TRUE;
}

/* Functions */

static void *thread_main(void *data)
{
  lispval result;
  struct KNO_THREAD_STRUCT *tstruct = (struct KNO_THREAD_STRUCT *)data;
  int flags = tstruct->flags, log_exit=
    ((flags)&(KNO_THREAD_TRACE_EXIT)) ||
    (((flags)&(KNO_THREAD_QUIET_EXIT)) ? (0) :
     (thread_log_exit));

  tstruct->errnop = &(errno);
  tstruct->threadid = u8_threadid();

  KNO_INIT_STACK();

  KNO_NEW_STACK(((struct KNO_STACK *)NULL),"thread",NULL,VOID);
  _stack->stack_label=u8_mkstring("thread%lld",u8_threadid());
  U8_SETBITS(_stack->stack_flags,KNO_STACK_FREE_LABEL);
  tstruct->thread_stackptr=_stack;

  /* Set (block) most signals */
  pthread_sigmask(SIG_SETMASK,kno_default_sigmask,NULL);

  /* Run any thread init functions */
  u8_threadcheck();

  tstruct->started = u8_elapsed_time();
  tstruct->finished = -1;

  if (tstruct->flags&KNO_EVAL_THREAD)
    result = kno_eval(tstruct->evaldata.expr,tstruct->evaldata.env);
  else
    result = kno_dapply(tstruct->applydata.fn,
                        tstruct->applydata.n_args,
                        tstruct->applydata.args);
  result = kno_finish_call(result);

  tstruct->finished = u8_elapsed_time();
  tstruct->flags = tstruct->flags|KNO_THREAD_DONE;

  if ( (KNO_ABORTP(result)) && (errno) ) {
    u8_exception ex = u8_current_exception;
    u8_log(thread_loglevel,ThreadExit,
           "Thread #%lld error (errno=%s:%d) %s (%s) %s",
           u8_threadid(),u8_strerror(errno),errno,
           ex->u8x_cond,U8_STRING_ARG(ex->u8x_context),
           U8_STRING_ARG(ex->u8x_details));}
  else if (KNO_ABORTP(result)) {
    u8_exception ex = u8_current_exception;
    u8_log(thread_loglevel,ThreadExit,
           "Thread #%lld error %s (%s) %s",
           u8_threadid(),ex->u8x_cond,
           U8_STRING_ARG(ex->u8x_context),
           U8_STRING_ARG(ex->u8x_details));}
  else if (errno) {
    u8_log(thread_loglevel,ThreadExit,
           "Thread #%lld exited (errno=%s:%d) returning %s%q",
           u8_threadid(),u8_strerror(errno),errno,
           ((CONSP(result))?("\n    "):("")),
           result);}
  else if (log_exit) {
    tstruct->result = result;
    u8_log(thread_loglevel,ThreadExit,
           "Thread #%lld exited returning %s%q",
           u8_threadid(),((CONSP(result))?("\n    "):("")),result);}
  else tstruct->result = result;

  u8_threadexit();

  if (KNO_ABORTP(result)) {
    u8_exception ex = u8_erreify();
    tstruct->flags = tstruct->flags|KNO_THREAD_ERROR;
    if (ex->u8x_details)
      u8_log(LOG_WARN,ThreadReturnError,
             "Thread #%d %s @%s (%s)",u8_threadid(),
             ex->u8x_cond,ex->u8x_context,ex->u8x_details);
    else u8_log(LOG_WARN,ThreadReturnError,
                "Thread #%d %s @%s",u8_threadid(),
                ex->u8x_cond,ex->u8x_context);
    if (tstruct->flags&KNO_EVAL_THREAD)
      u8_log(LOG_WARN,ThreadReturnError,"Thread #%d was evaluating %q",
             u8_threadid(),tstruct->evaldata.expr);
    else u8_log(LOG_WARN,ThreadReturnError,"Thread #%d was applying %q",
                u8_threadid(),tstruct->applydata.fn);
    kno_log_errstack(ex,LOG_WARN,1);
    if (kno_thread_backtrace) {
      U8_STATIC_OUTPUT(tmp,8000);
      kno_sum_exception(tmpout,ex);
      u8_log(LOG_WARN,ThreadBacktrace,"%s",tmp.u8_outbuf);
      if (kno_dump_exception) {
        lispval exo = kno_get_exception(ex);
        kno_dump_exception(exo);}
      u8_close_output(tmpout);}
    lispval exception = kno_get_exception(ex);
    if (KNO_VOIDP(exception))
      exception = kno_wrap_exception(ex);
    else kno_incref(exception);
    if (kno_dump_exception) kno_dump_exception(exception);
    tstruct->result = exception;
    if (tstruct->resultptr) {
      kno_incref(exception);
      *(tstruct->resultptr) = exception;}
    else NO_ELSE;
    u8_free_exception(ex,1);}
  else {
    if (tstruct->resultptr) {
      kno_incref(result);
      *(tstruct->resultptr) = result;}
    else NO_ELSE;}
  if (tstruct->flags&KNO_EVAL_THREAD) {
    lispval free_env = (lispval) tstruct->evaldata.env;
    tstruct->evaldata.env = NULL;
    kno_decref(free_env);}
  tstruct->thread_stackptr = NULL;
  kno_pop_stack(_stack);
  kno_decref((lispval)tstruct);
  return NULL;
}

KNO_EXPORT
kno_thread_struct kno_thread_call(lispval *resultptr,
                                  lispval fn,int n,lispval *args,
                                  int flags)
{
  struct KNO_THREAD_STRUCT *tstruct = u8_alloc(struct KNO_THREAD_STRUCT);
  if (tstruct == NULL) {
    u8_seterr(u8_MallocFailed,"kno_thread_call",NULL);
    return NULL;}
  KNO_INIT_FRESH_CONS(tstruct,kno_thread_type);
  lispval *rail = u8_alloc_n(n,lispval);
  int i=0; while (i<n) {rail[i]=args[i]; i++;}
  if (resultptr) {
    tstruct->resultptr = resultptr;
    *resultptr = KNO_NULL;
    tstruct->result = KNO_NULL;}
  else {
    tstruct->result = KNO_NULL;
    tstruct->resultptr = NULL;}
  tstruct->finished = -1;
  tstruct->flags = flags;
  pthread_attr_init(&(tstruct->attr));
  tstruct->applydata.fn = kno_incref(fn);
  tstruct->applydata.n_args = n;
  tstruct->applydata.args = rail;
  /* We need to do this first, before the thread exits and recycles itself! */
  kno_incref((lispval)tstruct);
  add_thread(tstruct);
  pthread_create(&(tstruct->tid),&(tstruct->attr),
                 thread_main,(void *)tstruct);
  return tstruct;
}

KNO_EXPORT
kno_thread_struct kno_thread_eval(lispval *resultptr,
                                  lispval expr,kno_lexenv env,
                                  int flags)
{
  struct KNO_THREAD_STRUCT *tstruct = u8_alloc(struct KNO_THREAD_STRUCT);
  if (tstruct == NULL) {
    u8_seterr(u8_MallocFailed,"kno_thread_eval",NULL);
    return NULL;}
  KNO_INIT_FRESH_CONS(tstruct,kno_thread_type);
  if (resultptr) {
    tstruct->resultptr = resultptr;
    *resultptr = KNO_NULL;
    tstruct->result = KNO_NULL;}
  else {
    tstruct->result = KNO_NULL;
    tstruct->resultptr = NULL;}
  tstruct->finished = -1;
  tstruct->flags = flags|KNO_EVAL_THREAD;
  tstruct->evaldata.expr = kno_incref(expr);
  tstruct->evaldata.env = kno_copy_env(env);
  /* We need to do this first, before the thread exits and recycles itself! */
  kno_incref((lispval)tstruct);
  add_thread(tstruct);
  pthread_create(&(tstruct->tid),pthread_attr_default,
                 thread_main,(void *)tstruct);
  return tstruct;
}

/* Scheme primitives */

DCLPRIM("THREAD/CALL",threadcall_prim,
        MIN_ARGS(1)|KNO_VAR_ARGS,
        "(THREAD/CALL *fcn* *args*...) applies *fcn* "
        "in parallel to all of the combinations of *args* "
        "and returns one thread for each combination.")
static lispval threadcall_prim(int n,lispval *args)
{
  lispval fn = args[0];
  if (KNO_APPLICABLEP(fn)) {
    kno_thread_struct thread = NULL;
    lispval call_args[n];
    int i = 1; while (i<n) {
      lispval call_arg = args[i];
      kno_incref(call_arg);
      call_args[i-1]=call_arg;
      i++;}
    thread = kno_thread_call(NULL,fn,n-1,call_args,0);
    if (thread == NULL) {
      u8_log(LOG_WARN,"ThreadLaunchFailed",
             "Failed to launch thread calling %q",args[0]);
      return KNO_EMPTY;}
    else return (lispval) thread;}
  else if (VOIDP(fn))
    return kno_err(kno_TooFewArgs,"threadcall_prim",NULL,VOID);
  else {
    kno_incref(fn);
    return kno_type_error(_("applicable"),"threadcall_prim",fn);}
}

DCLPRIM("THREAD/APPLY",threadapply_prim,
        MIN_ARGS(1)|KNO_VAR_ARGS,
        "(THREAD/APPLY *fcn* *args*...) applies *fcn* "
        "in parallel to all of the combinations of *args* "
        "and returns one thread for each combination. Choice "
        "arguments can be passed as one by using `QCHOICE`.")
static lispval threadapply_prim(int n,lispval *args)
{
  if (n == 1)
    return threadcall_prim(1,args);
  else {
    lispval last = args[n-1];
    if (KNO_NILP(last))
      return threadcall_prim(n-1,args);
    else if (!(KNO_SEQUENCEP(last)))
      return kno_err("NotASequence","threadapply_prim",
                     "The last argument to THREAD/APPLY was not a sequence",
                     last);
    else {
      int n_last = kno_seq_length(last);
      lispval extended_args[n+n_last-1];
      int i = 0, lim = n-1; while (i <lim) {
        lispval arg = args[i]; kno_incref(arg);
        extended_args[i] = args[i];
        i++;}
      int j = 0; while (j < n_last) {
        lispval arg = kno_seq_elt(last,j);
        extended_args[i++] = arg;
        j++;}
      return threadcall_prim(n+n_last-1,extended_args);}}
}

static int threadopts(lispval opts)
{
  lispval logexit = kno_getopt(opts,logexit_symbol,VOID);
  if (VOIDP(logexit)) {
    if (thread_log_exit>0)
      return KNO_THREAD_TRACE_EXIT;
    else return KNO_THREAD_QUIET_EXIT;}
  else if ((FALSEP(logexit))||(KNO_ZEROP(logexit)))
    return KNO_THREAD_QUIET_EXIT;
  else {
    kno_decref(logexit);
    return KNO_THREAD_TRACE_EXIT;}
}

DCLPRIM("THREAD/CALL+",
        threadcallx_prim,MIN_ARGS(2)|KNO_VAR_ARGS,
        "(THREAD/CALL+ *opts* *fcn* *args*...) applies *fcn* "
        "in parallel to all of the combinations of *args* "
        "and returns one thread for each combination. *opts* "
        "specifies options for creating each thread.")
static lispval threadcallx_prim(int n,lispval *args)
{
  lispval opts = args[0];
  lispval fn   = args[1];
  if (KNO_APPLICABLEP(fn)) {
    kno_thread_struct thread = NULL;
    lispval call_args[n];
    int flags = threadopts(opts);
    int i = 2; while (i<n) {
      lispval call_arg = args[i];
      kno_incref(call_arg);
      call_args[i-2]=call_arg;
      i++;}
    thread = kno_thread_call(NULL,fn,n-2,call_args,flags);
    if (thread == NULL) {
      u8_log(LOG_WARN,"ThreadLaunchFailed",
             "Failed to launch thread calling %q",args[0]);
      return KNO_EMPTY;}
    else return (lispval) thread;}
  else if (VOIDP(fn))
    return kno_err(kno_TooFewArgs,"threadcallx_prim",NULL,VOID);
  else {
    kno_incref(fn);
    return kno_type_error(_("applicable"),"threadcallx_prim",fn);}
}

static lispval spawn_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval to_eval = kno_get_arg(expr,1);
  lispval opts = kno_stack_eval(kno_get_arg(expr,2),env,_stack,0);
  if (KNO_ABORTED(opts)) {
    return opts;}
  int flags = threadopts(opts)|KNO_EVAL_THREAD;
  lispval results = KNO_EMPTY;
  DO_CHOICES(thread_expr,to_eval) {
    kno_thread_struct thread = kno_thread_eval(NULL,thread_expr,env,flags);
    if ( thread == NULL ) {
      u8_log(LOG_WARN,"ThreadLaunchFailed",
             "Error evaluating %q in its own thread, ignoring",thread_expr);}
    else {
      lispval thread_val = (lispval) thread;
      CHOICE_ADD(results,thread_val);}}
  kno_decref(opts);
  return results;
}

static lispval threadeval_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval to_eval = kno_eval(kno_get_arg(expr,1),env);
  if (KNO_ABORTED(to_eval))
    return to_eval;
  lispval env_arg = kno_eval(kno_get_arg(expr,2),env);
  if (KNO_ABORTED(env_arg)) {
    kno_decref(to_eval);
    return env_arg;}
  lispval opts_arg = kno_eval(kno_get_arg(expr,3),env);
  if (KNO_ABORTED(opts_arg)) {
    kno_decref(to_eval);
    kno_decref(env_arg);
    return opts_arg;}
  lispval opts=
    ((VOIDP(opts_arg)) &&
     (!(KNO_LEXENVP(env_arg))) &&
     (TABLEP(env_arg))) ?
    (env_arg):
    (opts_arg);
  kno_lexenv use_env= ( (VOIDP(env_arg)) || (FALSEP(env_arg))) ? (env) :
    (KNO_LEXENVP(env_arg)) ? ((kno_lexenv)env_arg) :
    (NULL);

  if (VOIDP(to_eval)) {
    kno_decref(opts_arg);
    kno_decref(env_arg);
    return kno_err(kno_SyntaxError,"spawn_evalfn","VOID expression",expr);}
  else if (use_env == NULL) {
    kno_decref(to_eval);
    kno_decref(opts_arg);
    kno_decref(env_arg);
    return kno_type_error(_("lispenv"),"spawn_evalfn",env_arg);}
  else {
    int flags = threadopts(opts)|KNO_EVAL_THREAD;
    lispval results = EMPTY;
    DO_CHOICES(thread_expr,to_eval) {
      kno_thread_struct thread = kno_thread_eval(NULL,thread_expr,use_env,flags);
      if ( thread == NULL ) {
        u8_log(LOG_WARN,"ThreadLaunchFailed",
               "Error evaluating %q in its own thread, ignoring",thread_expr);}
      else {
        lispval thread_val = (lispval) thread;
        CHOICE_ADD(results,thread_val);}}
    kno_decref(to_eval);
    kno_decref(env_arg);
    kno_decref(opts_arg);
    return results;}
}

DCLPRIM("THREAD/EXITED?",thread_exitedp,MIN_ARGS(1)|MAX_ARGS(1),
        "(THREAD/EXITED? *thread*) returns true if *thread* has exited")
static lispval thread_exitedp(lispval thread_arg)
{
  struct KNO_THREAD_STRUCT *thread = (struct KNO_THREAD_STRUCT *)thread_arg;
  if ((thread->flags)&(KNO_THREAD_DONE))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DCLPRIM("THREAD/FINISHED?",thread_finishedp,MIN_ARGS(1)|MAX_ARGS(1),
        "(THREAD/EXITED? *thread*) returns true "
        "if *thread* exited normally, #F otherwise")
static lispval thread_finishedp(lispval thread_arg)
{
  struct KNO_THREAD_STRUCT *thread = (struct KNO_THREAD_STRUCT *)thread_arg;
  if ( ( (thread->flags)&(KNO_THREAD_DONE) )  &&
       (! ( (thread->flags) & (KNO_THREAD_ERROR) ) ) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DCLPRIM("THREAD/ERROR?",thread_errorp,MIN_ARGS(1)|MAX_ARGS(1),
        "(THREAD/ERROR? *thread*) returns #t if *thread* "
        "exited with an error, #f otherwise")
static lispval thread_errorp(lispval thread_arg)
{
  struct KNO_THREAD_STRUCT *thread = (struct KNO_THREAD_STRUCT *)thread_arg;
  if ( ( (thread->flags) & (KNO_THREAD_DONE) ) &&
       ( (thread->flags) & (KNO_THREAD_ERROR) ) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DCLPRIM("THREAD/RESULT",thread_result,MIN_ARGS(1)|MAX_ARGS(1),
        "(THREAD/RESULT *thread*) returns the final result of the thread "
        "or {} if it hasn't finished. If the thread returned an error "
        "this returns the exception object for the error. If you want to "
        "wait for the result, use THREAD/FINISH.")
static lispval thread_result(lispval thread_arg)
{
  struct KNO_THREAD_STRUCT *thread = (struct KNO_THREAD_STRUCT *)thread_arg;
  if ((thread->flags)&(KNO_THREAD_DONE)) {
    if (thread->result!=KNO_NULL) {
      lispval result = thread->result;
      kno_incref(result);
      return result;}
    else return EMPTY;}
  else return EMPTY;
}

/* The results of this function indicate whether and how a call
   should wait for a join to be completed.
   0  = wait forever
   1  = wait for an interval (stored in wait)
   -1 = don't wait
*/
static int get_join_wait(lispval opts,struct timespec *wait)
{
  if ( (KNO_VOIDP(opts)) || (KNO_DEFAULTP(opts)) )
    return 0;
  else if (KNO_FALSEP(opts))
    return -1;
  else if (wait == NULL)
    return 0;
  else if ( (KNO_FIXNUMP(opts)) || (KNO_BIGINTP(opts))) {
    long long ival = kno_getint(opts);
    int rv = clock_gettime(CLOCK_REALTIME,wait);
    if (rv < 0) {
      int e = errno; errno=0;
      u8_log(LOG_CRIT,"clock_gettime","Failed with errno=%d:%s",
             e,u8_strerror(e));
      return -1;}
    else if (ival < 0)
      return -1;
    else if (ival > wait->tv_sec)
      wait->tv_sec = ival;
    else wait->tv_sec += ival;
    return 1;}
  else if (KNO_FLONUMP(opts)) {
    double delta = KNO_FLONUM(opts);
    long long secs = (long long) floor(delta);
    long long nsecs = (long long) floor(1000000000*(delta-secs));
    int rv = clock_gettime(CLOCK_REALTIME,wait);
    if (rv < 0) {
      int e = errno; errno=0;
      u8_log(LOG_CRIT,"clock_gettime","Failed with errno=%d:%s",
             e,u8_strerror(e));
      return -1;}
    wait->tv_sec += secs;
    wait->tv_nsec += nsecs;
    return 1;}
  else if (KNO_TYPEP(opts,kno_timestamp_type)) {
    struct KNO_TIMESTAMP *t = (kno_timestamp) opts;
    wait->tv_sec = t->u8xtimeval.u8_tick;
    wait->tv_nsec = t->u8xtimeval.u8_nsecs;
    return 1;}
  else if ( (KNO_TABLEP(opts)) && (kno_testopt(opts,timeout_symbol,KNO_VOID)) ) {
    lispval v = kno_getopt(opts,timeout_symbol,KNO_VOID);
    int rv = get_join_wait(v,wait);
    kno_decref(v);
    return rv;}
  else return 0;
}

static int join_thread(struct KNO_THREAD_STRUCT *tstruct,
                       int waiting,struct timespec *ts)
{
  int rv = 0;
  if (tstruct->finished > 0)
    return 0;
  else if (waiting == 0)
    rv = pthread_join(tstruct->tid,NULL);
  else if (waiting < 0) {
#if HAVE_PTHREAD_TRYJOIN_NP
    rv = pthread_tryjoin_np(tstruct->tid,NULL);
#else
    u8_log(LOG_WARN,"NotImplemented",
           "No pthread_tryjoin_np support");
    return 0;
#endif
  } else {
#if HAVE_PTHREAD_TIMEDJOIN_NP
    rv = pthread_timedjoin_np(tstruct->tid,NULL,ts);
#else
    u8_log(LOG_WARN,"NotImplemented",
           "No pthread_timedjoin_np support");
#endif
  }
  if ( (rv == 0) && (tstruct->result == KNO_NULL) ) {
    u8_log(LOG_ERROR,"NoThreadResult",
           "Thread which joined but doesn't have a result.");}
  return rv;
}

DCLPRIM2("THREAD/JOIN",threadjoin_prim,
         MIN_ARGS(1)|MAX_ARGS(2)|KNO_NDCALL,
         "(THREAD/JOIN *threads* [*opts*]) waits for *threads* "
         "to finish. If *opts* is provided, it specifies a timeout. "
         "This returns all of the threads which are finished.",
         -1,KNO_VOID,-1,KNO_VOID)
static lispval threadjoin_prim(lispval threads,lispval U8_MAYBE_UNUSED opts)
{
  {DO_CHOICES(thread,threads)
      if (!(TYPEP(thread,kno_thread_type)))
        return kno_type_error(_("thread"),"threadjoin_prim",thread);}

  lispval finished = EMPTY;
  struct timespec until = { 0 };
  int waiting = get_join_wait(opts,&until);

  {DO_CHOICES(thread,threads) {
      struct KNO_THREAD_STRUCT *tstruct = (kno_thread_struct)thread;
      int retval = join_thread(tstruct,waiting,&until);
      if (retval == 0) {
        kno_incref(thread);
        KNO_ADD_TO_CHOICE(finished,thread);}
      else if ( (retval == EBUSY) || (retval == ETIMEDOUT) ) {
        u8_log(LOG_INFO,ThreadReturnError,
               "Thread wait returned %d (%s) for %q",
               retval,strerror(retval),thread);}
      else u8_log(LOG_WARN,ThreadReturnError,
                  "Thread wait returned %d (%s) for %q",
                  retval,strerror(retval),thread);}}

  return finished;
}

DCLPRIM2("THREAD/WAIT",threadwait_prim,
         MIN_ARGS(1)|MAX_ARGS(2)|KNO_NDCALL,
         "(THREAD/WAIT *threads* [*opts*]) waits for all of *threads* "
         "to return. If provided *opts* specifies a timeout, and unfinished "
         "threads are returned.",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID)
static lispval threadwait_prim(lispval threads,lispval U8_MAYBE_UNUSED opts)
{
  struct timespec until;
  int waiting = get_join_wait(opts,&until);
  lispval unfinished = KNO_EMPTY;

  {DO_CHOICES(thread,threads)
      if (!(TYPEP(thread,kno_thread_type)))
        return kno_type_error(_("thread"),"threadjoin_prim",thread);}
  {DO_CHOICES(thread,threads) {
      struct KNO_THREAD_STRUCT *tstruct = (kno_thread_struct)thread;
      int retval = join_thread(tstruct,waiting,&until);
      if (retval != 0) {
        kno_incref(thread);
        KNO_ADD_TO_CHOICE(unfinished,thread);
        if ( (retval == EBUSY) || (retval == ETIMEDOUT) )
          u8_log(LOG_INFO,ThreadReturnError,
                 "Thread wait returned %d (%s) for %q",
                 retval,strerror(retval),thread);
        else u8_log(LOG_WARN,ThreadReturnError,
                    "Thread wait returned %d (%s) for %q",
                    retval,strerror(retval),thread);;}}}

  return unfinished;
}

DCLPRIM2("THREAD/FINISH",threadfinish_prim,
         MIN_ARGS(1)|MAX_ARGS(2)|KNO_NDCALL,
         "(THREAD/FINISH *args* [*opts*]) waits for all of threads in *args* "
         "to return, returning the non-VOID thread results together "
         "with any non-thread *args*. If provided, *opts* specifies a timeout, "
         "with unfinished threads being returned as thread objects.",
         -1,KNO_VOID,-1,KNO_VOID)
static lispval threadfinish_prim(lispval args,lispval opts)
{
  lispval results = EMPTY;

  {DO_CHOICES(arg,args)
      if (TYPEP(arg,kno_thread_type)) {
        struct timespec until = { 0 };
        struct KNO_THREAD_STRUCT *tstruct = (kno_thread_struct)arg;
        int waiting = get_join_wait(opts,&until);
        int retval = join_thread(tstruct,waiting,&until);
        if (retval == 0) {
          lispval result = tstruct->result;
          if (result == KNO_NULL) {}
          else if (KNO_VOIDP(result)) {
            lispval void_val = kno_getopt(opts,void_symbol,KNO_VOID);
            if (KNO_VOIDP(void_val)) {
              void_val = kno_init_exception
                (NULL,ThreadReturnError,"threadfinish_prim",
                 u8_mkstring("%d:%s",retval,strerror(retval)),
                 VOID,VOID,VOID,
                 u8_sessionid(),u8_elapsed_time(),
                 u8_elapsed_base(),
                 tstruct->threadid);}
            tstruct->result = void_val;}
          else {
            kno_incref(result);
            CHOICE_ADD(results,result);}}
        else {
          kno_incref(arg);
          KNO_ADD_TO_CHOICE(results,arg);
          if ( (retval == EBUSY) || (retval == ETIMEDOUT) )
            u8_log(LOG_INFO,ThreadReturnError,
                   "Thread wait returned %d (%s) for %q",
                   retval,strerror(retval),arg);
          else u8_log(LOG_WARN,ThreadReturnError,
                      "Thread wait returned %d (%s) for %q",
                      retval,strerror(retval),arg);}}
      else {
        kno_incref(arg);
        KNO_ADD_TO_CHOICE(results,arg);}}

  return results;
}

DCLPRIM2("THREAD/WAIT!",threadwaitbang_prim,
         MIN_ARGS(1)|MAX_ARGS(2)|KNO_NDCALL,
         "(THREAD/WAIT! *threads*) waits for all of *threads* to return, "
         "and returns VOID. *opts is currently ignored.",
         -1,KNO_VOID,-1,KNO_VOID)
static lispval threadwaitbang_prim(lispval threads,lispval U8_MAYBE_UNUSED opts)
{
  struct timespec until;
  int waiting = get_join_wait(opts,&until);

  {DO_CHOICES(thread,threads)
      if (!(TYPEP(thread,kno_thread_type)))
        return kno_type_error(_("thread"),"threadjoin_prim",thread);}
  {DO_CHOICES(thread,threads) {
      struct KNO_THREAD_STRUCT *tstruct = (kno_thread_struct)thread;
      int retval = join_thread(tstruct,waiting,&until);
      if (retval==0) {}
      else if ( (retval == EBUSY) || (retval == ETIMEDOUT) )
        u8_log(LOG_INFO,ThreadReturnError,
               "Thread wait returned %d (%s) for %q",
               retval,strerror(retval),thread);
      else u8_log(LOG_WARN,ThreadReturnError,
                  "Thread wait returned %d (%s) for %q",
                  retval,strerror(retval),thread);}}

  return KNO_VOID;
}

static lispval parallel_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  kno_thread_struct _threads[6], *threads;
  lispval _results[6], *results, scan = KNO_CDR(expr), result = EMPTY;
  int i = 0, n_exprs = 0;
  /* Compute number of expressions */
  while (PAIRP(scan)) {n_exprs++; scan = KNO_CDR(scan);}
  /* Malloc two vectors if neccessary. */
  if (n_exprs>6) {
    results = u8_alloc_n(n_exprs,lispval);
    threads = u8_alloc_n(n_exprs,kno_thread_struct);}
  else {results=_results; threads=_threads;}
  /* Start up the threads and store the pointers. */
  scan = KNO_CDR(expr); while (PAIRP(scan)) {
    lispval thread_expr = KNO_CAR(scan);
    kno_thread_struct thread =
      kno_thread_eval(&results[i],thread_expr,env,KNO_EVAL_THREAD|KNO_THREAD_QUIET_EXIT);
    if (thread == NULL) {
      u8_log(LOG_WARN,"ThreadLaunchFailed",
             "Unable to launch a thread evaluating %q",thread_expr);
      scan = KNO_CDR(scan);}
    else {
      threads[i]=thread;
      scan = KNO_CDR(scan);
      i++;}}
  /* Now wait for them to finish, accumulating values. */
  i = 0; while (i<n_exprs) {
    pthread_join(threads[i]->tid,NULL);
    /* If any threads return errors, return the errors, combining
       them as contexts if multiple. */
    CHOICE_ADD(result,results[i]);
    kno_decref((lispval)(threads[i]));
    i++;}
  if (n_exprs>6) {
    u8_free(threads); u8_free(results);}
  return result;
}

DCLPRIM("THREAD/YIELD",threadyield_prim,0,
        "(THREAD/YIELD) allows other threads to run.")
static lispval threadyield_prim()
{
#if _POSIX_PRIORITY_SCHEDULING
  int retval = sched_yield();
#elif HAVE_NANOSLEEP
  struct timespec req={0,1000}, rem={0,0};
  int retval = nanosleep(&req,&rem);
  if (retval>=0)
    retval = ((rem.tv_sec==0)&&(rem.tv_nsec<1000));
#elif HAVE_SLEEP
  int retval = sleep(1);
  if (retval>=0) {
    if (retval==0) retval = 1; else retval = 0;}
#endif
  if (retval<0) return KNO_ERROR;
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

/* Synchronized SET */

static u8_mutex sassign_lock;

/* This implements a simple version of globally synchronized set, which
   wraps a mutex around a regular set call, including evaluation of the
   value expression.  This can be used, for instance, to safely increment
   a variable. */
static lispval sassign_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  int retval;
  lispval var = kno_get_arg(expr,1), val_expr = kno_get_arg(expr,2), value;
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"SSET!",NULL,expr);
  else if (!(SYMBOLP(var)))
    return kno_err(kno_NotAnIdentifier,"SSET!",NULL,expr);
  else if (VOIDP(val_expr))
    return kno_err(kno_TooFewExpressions,"SSET!",NULL,expr);
  u8_lock_mutex(&sassign_lock);
  value = fast_eval(val_expr,env);
  if (KNO_ABORTED(value)) {
    u8_unlock_mutex(&sassign_lock);
    return value;}
  else if ((retval = (kno_assign_value(var,value,env)))) {
    kno_decref(value); u8_unlock_mutex(&sassign_lock);
    if (retval<0) return KNO_ERROR;
    else return VOID;}
  else if ((retval = (kno_bind_value(var,value,env)))) {
    kno_decref(value); u8_unlock_mutex(&sassign_lock);
    if (retval<0) return KNO_ERROR;
    else return VOID;}
  else {
    u8_unlock_mutex(&sassign_lock);
    return kno_err(kno_BindError,"SSET!",SYM_NAME(var),var);}
}

/* Thread variables */

DCLPRIM1("THREAD/GET",thread_get,MIN_ARGS(1),
         "`(THREAD/GET *sym*)` gets the fluid (thread-local) value for *sym* "
         "or the empty choice if there isn't one",
         kno_symbol_type,KNO_VOID)
static lispval thread_get(lispval var)
{
  lispval value = kno_thread_get(var);
  if (VOIDP(value))
    return EMPTY;
  else return value;
}

DCLPRIM("THREAD/RESET-VARS!",thread_reset_vars,MIN_ARGS(0)|MAX_ARGS(0),
        "`(THREAD/RESET-VARS!)` resets all the fluid (thread-local) "
        "variables for the current thread.")
static lispval thread_reset_vars()
{
  kno_reset_threadvars();
  return VOID;
}

DCLPRIM1("THREAD/BOUND?",thread_boundp,MIN_ARGS(1),
         "`(THREAD/BOUND? *sym*)` returns true if *sym* is "
         "fluidly bound in the current thread.",
         kno_symbol_type,KNO_VOID)
static lispval thread_boundp(lispval var)
{
  if (kno_thread_probe(var))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DCLPRIM2("THREAD/SET!",thread_set,MIN_ARGS(2),
         "`(THREAD/SET! *sym* *value*)` sets the fluid (thread-local) value "
         "for *sym* to *value and returns VOID.",
         kno_symbol_type,KNO_VOID,kno_any_type,KNO_VOID)
static lispval thread_set(lispval var,lispval val)
{
  if (kno_thread_set(var,val)<0)
    return KNO_ERROR;
  else return VOID;
}

DCLPRIM2("THREAD/ADD!",thread_add,MIN_ARGS(2),
         "`(THREAD/ADD! *sym* *values*)` inserts *values* into "
         "the fluid (thread-local) binding for *sym*.",
         kno_symbol_type,KNO_VOID,kno_any_type,KNO_VOID)
static lispval thread_add(lispval var,lispval val)
{
  if (kno_thread_add(var,val)<0)
    return KNO_ERROR;
  else return VOID;
}

static lispval thread_ref_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval sym_arg = kno_get_arg(expr,1), sym, val;
  lispval dflt_expr = kno_get_arg(expr,2);
  if (VOIDP(sym_arg))
    return kno_err(kno_SyntaxError,"thread_ref",
                   "No thread symbol",expr);
  else if (VOIDP(dflt_expr))
    return kno_err(kno_SyntaxError,"thread_ref",
                   "No generating expression",expr);
  else sym = kno_eval(sym_arg,env);
  if (KNO_ABORTP(sym))
    return sym;
  else if (!(SYMBOLP(sym)))
    return kno_err(kno_TypeError,"thread_ref","symbol",sym);
  else val = kno_thread_get(sym);
  if (KNO_ABORTP(val))
    return val;
  else if (VOIDP(val)) {
    lispval useval = kno_eval(dflt_expr,env);
    if (KNO_ABORTP(useval))
      return useval;
    else if (KNO_VOIDP(useval))
      return KNO_VOID;
    else NO_ELSE;
    int rv = kno_thread_set(sym,useval);
    if (rv<0) {
      kno_decref(useval);
      return KNO_ERROR;}
    return useval;}
  else return val;
}

/* Walking thread structs */

static int walk_thread_struct(kno_walker walker,lispval x,
                              void *walkdata,
                              kno_walk_flags flags,
                              int depth)
{
  struct KNO_THREAD_STRUCT *tstruct =
    kno_consptr(struct KNO_THREAD_STRUCT *,x,kno_thread_type);
  if ( (tstruct->flags) & (KNO_EVAL_THREAD) ) {
    if (kno_walk(walker,tstruct->evaldata.expr,walkdata,flags,depth-1)<0)
      return -1;
    else if ( (tstruct->evaldata.env) &&
              (kno_walk(walker,((lispval)(tstruct->evaldata.env)),walkdata,
                        flags,depth-1)<0) )
      return -1;}
  else if (kno_walk(walker,tstruct->applydata.fn,walkdata,flags,depth-1)<0)
    return -1;
  else {
    lispval *args = tstruct->applydata.args;
    int i=0, n = tstruct->applydata.n_args;
    while (i<n) {
      if (kno_walk(walker,args[i],walkdata,flags,depth-1)<0)
        return -1;
      else i++;}}
  if (tstruct->thread_stackptr) {
    struct KNO_STACK *stackptr = tstruct->thread_stackptr;
    if (kno_walk(walker,stackptr->stack_op,walkdata,flags,depth-1)<0) {
      return -1;}
    if ((stackptr->stack_env) &&
        (kno_walk(walker,((lispval)stackptr->stack_env),walkdata,flags,depth-1)<0))
      return -1;
    if (!(KNO_EMPTYP(stackptr->stack_vals))) {
      KNO_DO_CHOICES(stack_val,stackptr->stack_vals) {
        if (kno_walk(walker,stack_val,walkdata,flags,depth-1)<0) {
          KNO_STOP_DO_CHOICES;
          return -1;}}}
    if (stackptr->n_args) {
      lispval *args = stackptr->stack_args;
      int i=0, n=stackptr->n_args; while (i<n) {
        if (kno_walk(walker,args[i],walkdata,flags,depth-1)<0) {
          return -1;}
        i++;}}}
  return 1;
}

/* Thread information */

DCLPRIM("CSTACK-DEPTH",cstack_depth_prim,MAX_ARGS(0),
        "Returns the current depth of the C stack")
static lispval cstack_depth_prim()
{
  ssize_t depth = u8_stack_depth();
  return KNO_INT2DTYPE(depth);
}
DCLPRIM("CSTACK-LIMIT",cstack_limit_prim,MAX_ARGS(0),
        "Returns the allocated limit of the C stack")
static lispval cstack_limit_prim()
{
  ssize_t limit = kno_stack_limit;
  return KNO_INT2DTYPE(limit);
}
DCLPRIM("CSTACK-LIMIT!",set_cstack_limit_prim,MIN_ARGS(1)|MAX_ARGS(1),
        "Returns the allocated limit of the C stack")
static lispval set_cstack_limit_prim(lispval arg)
{
  if (KNO_FLONUMP(arg)) {
    ssize_t result = kno_stack_resize(KNO_FLONUM(arg));
    if (result<0)
      return KNO_ERROR;
    else return KNO_INT2DTYPE(result);}
  else if ( (FIXNUMP(arg)) || (KNO_BIGINTP(arg)) ) {
    ssize_t limit = (ssize_t) kno_getint(arg);
    ssize_t result = (limit)?(kno_stack_setsize(limit)):(-1);
    if (limit) {
      if (result<0)
        return KNO_ERROR;
      else return KNO_INT2DTYPE(result);}
    else return kno_type_error("stacksize","set_stack_limit_prim",arg);}
  else return kno_type_error("stacksize","set_stack_limit_prim",arg);
}

KNO_EXPORT void kno_init_threads_c()
{
  u8_init_mutex(&thread_ring_lock);

  kno_thread_type = kno_register_cons_type(_("thread"));
  kno_recyclers[kno_thread_type]=recycle_thread_struct;
  kno_unparsers[kno_thread_type]=unparse_thread_struct;

  kno_synchronizer_type = kno_register_cons_type(_("synchronizer"));
  kno_recyclers[kno_synchronizer_type]=recycle_synchronizer;
  kno_unparsers[kno_synchronizer_type]=unparse_synchronizer;

  kno_def_evalfn(kno_scheme_module,"PARALLEL",
                 "`(PARALLEL *exprs...*)` is just like `CHOICE`, "
                 "but each of *exprs* is evaluated in its own thread.",
                 parallel_evalfn);
  kno_def_evalfn(kno_scheme_module,"SPAWN",
                 "`(SPAWN *expr* *opts**)` starts and returns parallel threads"
                 "evaluating *expr* (which may be a choice of exprs). "
                 "*opts*, if provided, specifies thread creation options.",
                 spawn_evalfn);

  kno_def_evalfn(kno_scheme_module,"THREAD/EVAL",
                 "`(THREAD/EVAL *expr* [*env*] [*opts*])` starts a parallel "
                 "thread evaluating *expr*. If *env* is provided, it is used, "
                 "otherwise, *expr* is evaluated in a *copy* of the current "
                 "environment. *opts*, if provided, specifies thread creation "
                 "options.",
                 threadeval_evalfn);

  DECL_PRIM_N(threadapply_prim,kno_scheme_module);
  DECL_PRIM_N(threadcall_prim,kno_scheme_module);

  DECL_PRIM_N(threadcallx_prim,kno_scheme_module);
  DECL_PRIM(threadyield_prim,0,kno_scheme_module);
  DECL_PRIM(threadjoin_prim,2,kno_scheme_module);

  DECL_PRIM(threadwait_prim,2,kno_scheme_module);
  DECL_PRIM(threadwaitbang_prim,2,kno_scheme_module);
  DECL_PRIM(threadfinish_prim,2,kno_scheme_module);

  DECL_PRIM(threadp_prim,1,kno_scheme_module);
  DECL_PRIM(synchronizerp_prim,1,kno_scheme_module);
  DECL_PRIM(mutexp_prim,1,kno_scheme_module);
  DECL_PRIM(rwlockp_prim,1,kno_scheme_module);
  DECL_PRIM(condvarp_prim,1,kno_scheme_module);

  u8_init_mutex(&sassign_lock);
  kno_def_evalfn(kno_scheme_module,"SSET!",
		 "`(SSET! *var* *value*)` thread-safely sets *var* "
		 "to *value*, just like `SET!` would do.",
		 sassign_evalfn);

  int one_thread_arg[1] = { kno_thread_type };
  int find_thread_types[2] = { kno_fixnum_type, kno_any_type };

  DECL_PRIM_ARGS(threadid_prim,1,kno_scheme_module,one_thread_arg,NULL);
  DECL_PRIM_ARGS(findthread_prim,2,kno_scheme_module,find_thread_types,NULL);

  DECL_PRIM_ARGS(thread_exitedp,1,kno_scheme_module,one_thread_arg,NULL);
  DECL_PRIM_ARGS(thread_finishedp,1,kno_scheme_module,one_thread_arg,NULL);
  DECL_PRIM_ARGS(thread_errorp,1,kno_scheme_module,one_thread_arg,NULL);
  DECL_PRIM_ARGS(thread_result,1,kno_scheme_module,one_thread_arg,NULL);

  timeout_symbol = kno_intern("timeout");
  logexit_symbol = kno_intern("logexit");
  void_symbol = kno_intern("void");

  int one_synchronizer_arg[1] = { kno_synchronizer_type };
  int condvar_signal_args[2] = { kno_synchronizer_type, kno_any_type };

  DECL_PRIM(make_mutex,0,kno_scheme_module);
  DECL_PRIM(make_rwlock,0,kno_scheme_module);
  DECL_PRIM(make_condvar,0,kno_scheme_module);

  DECL_PRIM_ARGS(condvar_signal,2,kno_scheme_module,
                 condvar_signal_args,NULL);
  DECL_PRIM_ARGS(condvar_wait,2,kno_scheme_module,
                 condvar_signal_args,NULL);
  DECL_PRIM_ARGS(condvar_lock,1,kno_scheme_module,
                 one_synchronizer_arg,NULL);
  DECL_PRIM_ARGS(condvar_unlock,1,kno_scheme_module,
                 one_synchronizer_arg,NULL);

  DECL_PRIM(sync_lock,1,kno_scheme_module);
  DECL_PRIM(sync_unlock,1,kno_scheme_module);
  DECL_PRIM(sync_read_lock,1,kno_scheme_module);

  kno_def_evalfn(kno_scheme_module,"WITH-LOCK",
                 "`(WITH-LOCK *synchronizer* *body...*)` executes the "
                 "expressions in *body* with *synchronizer* locked.",
                 with_lock_evalfn);

  DECL_PRIM(thread_get,1,kno_scheme_module);
  DECL_PRIM(thread_boundp,1,kno_scheme_module);
  DECL_PRIM(thread_set,2,kno_scheme_module);
  DECL_PRIM(thread_add,2,kno_scheme_module);
  DECL_PRIM(thread_reset_vars,0,kno_scheme_module);

  kno_def_evalfn(kno_scheme_module,"THREAD/REF",
                 "`(THREAD/REF *sym* *expr*)` returns the fluid "
                 "(thread-local) value of *sym* (which is evaluated) "
                 "if it exists. If it doesn't exist, *expr* is evaluted "
                 "and fluidly assigned to *sym*.",
                 thread_ref_evalfn);

  DECL_PRIM(cstack_depth_prim,0,kno_scheme_module);
  DECL_PRIM(cstack_limit_prim,0,kno_scheme_module);
  DECL_PRIM(set_cstack_limit_prim,1,kno_scheme_module);

  kno_register_config("ALLTHREADS",
                      "All active LISP threads",
                      allthreads_config_get,kno_readonly_config_set,
                      NULL);

  kno_register_config("THREAD:BACKTRACE",
                      "Whether errors in threads print out full backtraces",
                      kno_boolconfig_get,kno_boolconfig_set,
                      &kno_thread_backtrace);
  kno_register_config("THREAD:LOGLEVEL",
                      "The log level to use for thread-related events",
                      kno_intconfig_get,kno_loglevelconfig_set,
                      &thread_loglevel);
  kno_register_config("THREAD:LOGEXIT",
                      "Whether to log the normal exit values of threads",
                      kno_boolconfig_get,kno_boolconfig_set,
                      &thread_log_exit);

  kno_walkers[kno_thread_type]=walk_thread_struct;

  u8_register_source_file(_FILEINFO);
}
