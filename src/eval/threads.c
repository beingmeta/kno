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
static u8_condition ThreadVOID=_("ThreadVoidResult");

static int thread_loglevel = LOG_NOTICE;
static int thread_log_exit = 1;
static lispval logexit_symbol = VOID, timeout_symbol = VOID;
static lispval void_symbol = VOID;

#ifndef U8_STRING_ARG
#define U8_STRING_ARG(s) (((s) == NULL)?((u8_string)""):((u8_string)(s)))
#endif

/* Thread structures */

static struct KNO_THREAD_STRUCT *thread_ring=NULL;
u8_mutex thread_ring_lock;

kno_ptr_type kno_thread_type;
kno_ptr_type kno_condvar_type;

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

static lispval threadp_prim(lispval arg)
{
  if (KNO_TYPEP(arg,kno_thread_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval synchronizerp_prim(lispval arg)
{
  if (KNO_TYPEP(arg,kno_condvar_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval findthread_prim(lispval threadid_arg)
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
  return kno_err("NoThreadID","find_thread",NULL,threadid_arg);
}

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
              ((th->flags&KNO_THREAD_DONE) ? (" done") : ("")),
              th->evaldata.expr);
  else u8_printf(out,"#<THREAD (%lld)0x%x%s apply %q>",
                 th->threadid,(unsigned long)(th->tid),
                 ((th->flags&KNO_THREAD_DONE) ? (" done") : ("")),
                 th->applydata.fn);
  return 1;
}

KNO_EXPORT void recycle_thread_struct(struct KNO_RAW_CONS *c)
{
  struct KNO_THREAD_STRUCT *th = (struct KNO_THREAD_STRUCT *)c;
  remove_thread(th);
  if (th->flags&KNO_EVAL_THREAD) {
    kno_decref(th->evaldata.expr);
    if (th->evaldata.env) {
      kno_decref((lispval)(th->evaldata.env));}}
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

/* CONDVAR support */

static lispval make_condvar()
{
  int rv = 0;
  struct KNO_CONDVAR *cv = u8_alloc(struct KNO_CONDVAR);
  KNO_INIT_FRESH_CONS(cv,kno_condvar_type);
  rv = u8_init_mutex(&(cv->kno_cvlock));
  if (rv) {
    u8_graberrno("make_condvar",NULL);
    return KNO_ERROR;}
  else rv = u8_init_condvar(&(cv->kno_cvar));
  if (rv) {
    u8_graberrno("make_condvar",NULL);
    return KNO_ERROR;}
  return LISP_CONS(cv);
}

/* This primitive combine cond_wait and cond_timedwait
   through a second (optional) argument, which is an
   interval in seconds. */
static lispval condvar_wait(lispval cvar,lispval timeout)
{
  int rv = 0;
  struct KNO_CONDVAR *cv=
    kno_consptr(struct KNO_CONDVAR *,cvar,kno_condvar_type);
  if (VOIDP(timeout))
    if ((rv = u8_condvar_wait(&(cv->kno_cvar),&(cv->kno_cvlock)))==0)
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
    rv = u8_condvar_timedwait(&(cv->kno_cvar),&(cv->kno_cvlock),&tm);
    if (rv==0)
      return KNO_TRUE;
    else if (rv == ETIMEDOUT)
      return KNO_FALSE;
    return kno_type_error(_("valid condvar"),"condvar_wait",cvar);}
}

/* This primitive combines signals and broadcasts through
   a second (optional) argument which, when true, implies
   a broadcast. */
static lispval condvar_signal(lispval cvar,lispval broadcast)
{
  struct KNO_CONDVAR *cv=
    kno_consptr(struct KNO_CONDVAR *,cvar,kno_condvar_type);
  if (KNO_TRUEP(broadcast))
    if (u8_condvar_broadcast(&(cv->kno_cvar))==0)
      return KNO_TRUE;
    else return kno_type_error(_("valid condvar"),"condvar_signal",cvar);
  else if (u8_condvar_signal(&(cv->kno_cvar))==0)
    return KNO_TRUE;
  else return kno_type_error(_("valid condvar"),"condvar_signal",cvar);
}

static lispval condvar_lock(lispval cvar)
{
  struct KNO_CONDVAR *cv=
    kno_consptr(struct KNO_CONDVAR *,cvar,kno_condvar_type);
  u8_lock_mutex(&(cv->kno_cvlock));
  return KNO_TRUE;
}

static lispval condvar_unlock(lispval cvar)
{
  struct KNO_CONDVAR *cv=
    kno_consptr(struct KNO_CONDVAR *,cvar,kno_condvar_type);
  u8_unlock_mutex(&(cv->kno_cvlock));
  return KNO_TRUE;
}

static int unparse_condvar(u8_output out,lispval cvar)
{
  struct KNO_CONDVAR *cv=
    kno_consptr(struct KNO_CONDVAR *,cvar,kno_condvar_type);
  u8_printf(out,"#<CONDVAR %lx>",cv);
  return 1;
}

KNO_EXPORT void recycle_condvar(struct KNO_RAW_CONS *c)
{
  struct KNO_CONDVAR *cv=
    (struct KNO_CONDVAR *)c;
  u8_destroy_mutex(&(cv->kno_cvlock));  u8_destroy_condvar(&(cv->kno_cvar));
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

/* These functions generically access the locks on CONDVARs
   and LAMBDAs */

static lispval synchro_lock(lispval lck)
{
  if (TYPEP(lck,kno_condvar_type)) {
    struct KNO_CONDVAR *cv=
      kno_consptr(struct KNO_CONDVAR *,lck,kno_condvar_type);
    u8_lock_mutex(&(cv->kno_cvlock));
    return KNO_TRUE;}
  else if (KNO_LAMBDAP(lck)) {
    struct KNO_LAMBDA *sp = kno_consptr(kno_lambda,lck,kno_lambda_type);
    if (sp->lambda_synchronized) {
      u8_lock_mutex(&(sp->lambda_lock));}
    else return kno_type_error("lockable","synchro_lock",lck);
    return KNO_TRUE;}
  else return kno_type_error("lockable","synchro_lock",lck);
}

static lispval synchro_unlock(lispval lck)
{
  if (TYPEP(lck,kno_condvar_type)) {
    struct KNO_CONDVAR *cv=
      kno_consptr(struct KNO_CONDVAR *,lck,kno_condvar_type);
    u8_unlock_mutex(&(cv->kno_cvlock));
    return KNO_TRUE;}
  else if (KNO_LAMBDAP(lck)) {
    struct KNO_LAMBDA *sp = kno_consptr(kno_lambda,lck,kno_lambda_type);
    if (sp->lambda_synchronized) {
      u8_lock_mutex(&(sp->lambda_lock));}
    else return kno_type_error("lockable","synchro_lock",lck);
    return KNO_TRUE;}
  else return kno_type_error("lockable","synchro_unlock",lck);
}

static lispval with_lock_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval lock_expr = kno_get_arg(expr,1), lck, value = VOID;
  u8_mutex *uselock = NULL;
  if (VOIDP(lock_expr))
    return kno_err(kno_SyntaxError,"with_lock_evalfn",NULL,expr);
  else lck = kno_eval(lock_expr,env);
  if (KNO_ABORTED(lck)) return lck;
  else if (TYPEP(lck,kno_condvar_type)) {
    struct KNO_CONDVAR *cv=
      kno_consptr(struct KNO_CONDVAR *,lck,kno_condvar_type);
    uselock = &(cv->kno_cvlock);}
  else if (KNO_LAMBDAP(lck)) {
    struct KNO_LAMBDA *sp = kno_consptr(kno_lambda,lck,kno_lambda_type);
    if (sp->lambda_synchronized) {
      uselock = &(sp->lambda_lock);}
    else {
      kno_decref(lck);
      return kno_type_error("lockable","synchro_lock",lck);}}
  else {
    kno_decref(lck);
    return kno_type_error("lockable","synchro_unlock",lck);}
  {U8_WITH_CONTOUR("WITH-LOCK",0) {
      u8_lock_mutex(uselock);
      KNO_DOLIST(elt_expr,KNO_CDDR(expr)) {
        kno_decref(value);
        value = kno_eval(elt_expr,env);
        if (KNO_ABORTED(value)) {
          u8_unlock_mutex(uselock);
          kno_decref(lck);
          break;}}}
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      kno_decref(value);
      value = KNO_ERROR;}
    U8_END_EXCEPTION;}
  u8_unlock_mutex(uselock);
  kno_decref(lck);
  return value;
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
    u8_log(thread_loglevel,ThreadExit,
           "Thread #%lld exited returning %s%q",
           u8_threadid(),((CONSP(result))?("\n    "):("")),result);}
  else {}

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
    else {}
    u8_free_exception(ex,1);}
  else {
    tstruct->result = result;
    if (tstruct->resultptr) {
      *(tstruct->resultptr) = result;
      kno_incref(result);}}
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

static lispval threadeval_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval to_eval = kno_get_arg(expr,1);
  lispval env_arg = kno_eval(kno_get_arg(expr,2),env);
  if (KNO_ABORTED(env_arg)) return env_arg;
  lispval opts_arg = kno_eval(kno_get_arg(expr,3),env);
  if (KNO_ABORTED(opts_arg)) {
    kno_decref(env_arg);
    return opts_arg;}
  lispval opts=
    ((VOIDP(opts_arg))&&
     (!(KNO_LEXENVP(env_arg)))&&
     (TABLEP(env_arg)))?
    (env_arg):
    (opts_arg);
  kno_lexenv use_env=
    ((VOIDP(env_arg))||(FALSEP(env_arg)))?(env):
    (KNO_LEXENVP(env_arg))?((kno_lexenv)env_arg):
    (NULL);
  if (VOIDP(to_eval)) {
    kno_decref(opts_arg); kno_decref(env_arg);
    return kno_err(kno_SyntaxError,"threadeval_evalfn",NULL,expr);}
  else if (use_env == NULL) {
    kno_decref(opts_arg);
    return kno_type_error(_("lispenv"),"threadeval_evalfn",env_arg);}
  else {
    int flags = threadopts(opts)|KNO_EVAL_THREAD;
    kno_lexenv env_copy = kno_copy_env(use_env);
    lispval results = EMPTY, envptr = (lispval)env_copy;
    DO_CHOICES(thread_expr,to_eval) {
      kno_thread_struct thread = kno_thread_eval(NULL,thread_expr,env_copy,flags);
      if ( thread == NULL ) {
        u8_log(LOG_WARN,"ThreadLaunchFailed",
               "Error evaluating %q in its own thread, ignoring",thread_expr);}
      else {
        lispval thread_val = (lispval) thread;
        CHOICE_ADD(results,thread_val);}}
    kno_decref(envptr);
    kno_decref(env_arg);
    kno_decref(opts_arg);
    return results;}
}

static lispval thread_exitedp(lispval thread_arg)
{
  struct KNO_THREAD_STRUCT *thread = (struct KNO_THREAD_STRUCT *)thread_arg;
  if ((thread->flags)&(KNO_THREAD_DONE))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval thread_finishedp(lispval thread_arg)
{
  struct KNO_THREAD_STRUCT *thread = (struct KNO_THREAD_STRUCT *)thread_arg;
  if ( ( (thread->flags)&(KNO_THREAD_DONE) )  &&
       (! ( (thread->flags) & (KNO_THREAD_ERROR) ) ) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval thread_errorp(lispval thread_arg)
{
  struct KNO_THREAD_STRUCT *thread = (struct KNO_THREAD_STRUCT *)thread_arg;
  if ( ( (thread->flags) & (KNO_THREAD_DONE) ) &&
       ( (thread->flags) & (KNO_THREAD_ERROR) ) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

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

static int get_thread_wait(lispval opts,struct timespec *wait)
{
  if (KNO_VOIDP(opts))
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
    int rv = get_thread_wait(v,wait);
    kno_decref(v);
    return rv;}
  else return 0;
}

static int join_thread(struct KNO_THREAD_STRUCT *tstruct,int waiting,
                       struct timespec *ts,
                       void **vptr)
{
  if (tstruct->finished > 0)
    return 0;
  else if (waiting == 0) 
    return pthread_join(tstruct->tid,vptr);
  else if (waiting < 0) {
#if HAVE_PTHREAD_TRYJOIN_NP
    return pthread_tryjoin_np(tstruct->tid,vptr);
#else
  u8_log(LOG_WARN,"NotImplemented",
         "No pthread_tryjoin_np support");
  return 0;
#endif
  } else {
#if HAVE_PTHREAD_TIMEDJOIN_NP
    return pthread_timedjoin_np(tstruct->tid,vptr,ts);
#else
    u8_log(LOG_WARN,"NotImplemented",
           "No pthread_timedjoin_np support");
  return 0;
#endif    
  }
}

static lispval threadjoin_prim(lispval threads,lispval U8_MAYBE_UNUSED opts)
{
  {DO_CHOICES(thread,threads)
     if (!(TYPEP(thread,kno_thread_type)))
       return kno_type_error(_("thread"),"threadjoin_prim",thread);}

  lispval results = EMPTY;
  struct timespec until;
  int waiting = get_thread_wait(opts,&until);

  {DO_CHOICES(thread,threads) {
      struct KNO_THREAD_STRUCT *tstruct = (kno_thread_struct)thread;
      int retval = join_thread(tstruct,waiting,&until,NULL);
      if (retval == EINVAL)
        u8_log(LOG_WARN,ThreadReturnError,"Bad return code %d (%s) from %q",
               retval,strerror(retval),thread);
      else if (retval == 0) {
        if ( (tstruct->resultptr == NULL) ||
             ((tstruct->resultptr) == &(tstruct->result)) ) {
          /* If the result wasn't put somewhere else, add it to the results */
          if (VOIDP(tstruct->result))
            u8_log(LOG_INFO,ThreadVOID,
                   "The thread %q unexpectedly returned VOID but without error",
                   thread);
          else  {
            kno_incref(tstruct->result);
            CHOICE_ADD(results,tstruct->result);}}}}}

  return results;
}

static lispval threadwait_prim(lispval threads,lispval U8_MAYBE_UNUSED opts)
{
  struct timespec until;
  int waiting = get_thread_wait(opts,&until);

  {DO_CHOICES(thread,threads)
     if (!(TYPEP(thread,kno_thread_type)))
       return kno_type_error(_("thread"),"threadjoin_prim",thread);}
  {DO_CHOICES(thread,threads) {
    struct KNO_THREAD_STRUCT *tstruct = (kno_thread_struct)thread;
    int retval = join_thread(tstruct,waiting,&until,NULL);
    if (retval == EINVAL)
      u8_log(LOG_WARN,ThreadReturnError,"Bad return code %d (%s) from %q",
             retval,strerror(retval),thread);}}

  return kno_incref(threads);
}

static lispval threadfinish_prim(lispval args,lispval U8_MAYBE_UNUSED opts)
{
  lispval results = EMPTY;
  struct timespec until;
  int waiting = get_thread_wait(opts,&until);

  {DO_CHOICES(arg,args)
      if (TYPEP(arg,kno_thread_type)) {
        struct KNO_THREAD_STRUCT *tstruct = (kno_thread_struct)arg;
        int retval = join_thread(tstruct,waiting,&until,NULL);
        if (retval == EINVAL) {
          u8_log(LOG_WARN,ThreadReturnError,
                 "Bad return code %d (%s) from %q",
                 retval,strerror(retval),arg);}
        else if (retval == 0) {
          lispval result = tstruct->result;
          if (KNO_VOIDP(result)) {
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
          kno_incref(result);
          CHOICE_ADD(results,result);}
        else {
          kno_incref((lispval)tstruct);
          KNO_ADD_TO_CHOICE(results,arg);}}
      else {
        kno_incref(arg);
        KNO_ADD_TO_CHOICE(results,arg);}}

  return results;
}

static lispval threadwaitbang_prim(lispval threads,lispval U8_MAYBE_UNUSED opts)
{
  struct timespec until;
  int waiting = get_thread_wait(opts,&until);

  {DO_CHOICES(thread,threads)
     if (!(TYPEP(thread,kno_thread_type)))
       return kno_type_error(_("thread"),"threadjoin_prim",thread);}
  {DO_CHOICES(thread,threads) {
    struct KNO_THREAD_STRUCT *tstruct = (kno_thread_struct)thread;
    int retval = join_thread(tstruct,waiting,&until,NULL);
    if (retval)
      u8_log(LOG_WARN,ThreadReturnError,"Bad return code %d (%s) from %q",
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
    else if (kno_walk(walker,((lispval)(tstruct->evaldata.env)),walkdata,
                       flags,depth-1)<0)
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

static lispval stack_depth_prim()
{
  ssize_t depth = u8_stack_depth();
  return KNO_INT2DTYPE(depth);
}
static lispval stack_limit_prim()
{
  ssize_t limit = kno_stack_limit;
  return KNO_INT2DTYPE(limit);
}
static lispval set_stack_limit_prim(lispval arg)
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

  kno_condvar_type = kno_register_cons_type(_("condvar"));
  kno_recyclers[kno_condvar_type]=recycle_condvar;
  kno_unparsers[kno_condvar_type]=unparse_condvar;

  kno_def_evalfn(kno_scheme_module,"PARALLEL","",parallel_evalfn);
  kno_def_evalfn(kno_scheme_module,"SPAWN","",threadeval_evalfn);

  kno_idefn1(kno_scheme_module,"THREAD?",threadp_prim,1,
            "(THREAD? *arg*) returns #t if *arg* is a thread object",
            -1,KNO_VOID);
  kno_idefn1(kno_scheme_module,"SYNCHRONIZER?",synchronizerp_prim,1,
            "(SYNCHRONIZER? *arg*) returns #t if *arg* is a "
            "synchronizer (condvar)",
            -1,KNO_VOID);
  kno_idefn1(kno_scheme_module,"THREAD-ID",threadid_prim,1,
             "`(THREAD-ID *thread*)` returns the integer identifier for *thread*",
             kno_thread_type,KNO_VOID);


  kno_idefnN(kno_scheme_module,"THREAD/CALL",threadcall_prim,
            KNO_NEEDS_1_ARG,
            "(THREAD/CALL *fcn* *args*...) applies *fcn* "
            "in parallel to all of the combinations of *args* "
            "and returns one thread for each combination.");
  kno_defalias(kno_scheme_module,"THREADCALL","THREAD/CALL");

  kno_idefnN(kno_scheme_module,"THREAD/CALL+",
            threadcallx_prim,KNO_NEEDS_2_ARGS,
            "(THREAD/CALL+ *opts* *fcn* *args*...) applies *fcn* "
            "in parallel to all of the combinations of *args* "
            "and returns one thread for each combination. *opts* "
            "specifies options for creating each thread.");

  kno_idefn0(kno_scheme_module,"THREAD/YIELD",threadyield_prim,
            "(THREAD/YIELD) allows other threads to run");
  kno_defalias(kno_scheme_module,"THREADYIELD","THREAD/YIELD");

  kno_idefn2(kno_scheme_module,"THREAD/JOIN",threadjoin_prim,
            KNO_NEEDS_1_ARG|KNO_NDCALL,
            "(THREAD/JOIN *threads* [*opts*]) waits for all of *threads* "
            "to finish and returns all of their non VOID results "
            "(as a choice), logging when a VOID result is returned. "
            "*opts is currently ignored.",
            -1,KNO_VOID,-1,KNO_VOID);
  kno_defalias(kno_scheme_module,"THREADJOIN","THREAD/JOIN");

  kno_idefn2(kno_scheme_module,"THREAD/WAIT",threadwait_prim,
            KNO_NEEDS_1_ARG|KNO_NDCALL,
            "(THREAD/WAIT *threads* [*opts*]) waits for all of *threads* "
            "to return, returning the thread objects. "
            "*opts is currently ignored.",
            -1,KNO_VOID,-1,KNO_VOID);

  kno_idefn2(kno_scheme_module,"THREAD/FINISH",threadfinish_prim,
            KNO_NEEDS_1_ARG|KNO_NDCALL,
            "(THREAD/FINISH *args* [*opts*]) waits for all of threads in *args* "
            "to return, returning the non-VOID thread results together "
            "with any non-thread *args*. *opts is currently ignored.",
            -1,KNO_VOID,-1,KNO_VOID);

  kno_idefn2(kno_scheme_module,"THREAD/WAIT!",threadwaitbang_prim,
            KNO_NEEDS_1_ARG|KNO_NDCALL,
            "(THREAD/WAIT! *threads*) waits for all of *threads* to return, "
            "and returns VOID. *opts is currently ignored.",
            -1,KNO_VOID,-1,KNO_VOID);

  kno_idefn1(kno_scheme_module,"FIND-THREAD",findthread_prim,0,
            "(FIND-THREAD [*id*]) returns the thread object for "
            "the the thread numbered *id* (which is the value returned "
            "by (threadid)). If *id* is not provided or #default, returns "
            "the thread object for the current thread. If a thread object "
            "doesn't exist, returns #f",
            -1,KNO_VOID);

  kno_idefn1(kno_scheme_module,"THREAD/EXITED?",thread_exitedp,1,
            "(THREAD/EXITED? *thread*) returns true if *thread* has exited",
            kno_thread_type,VOID);
  kno_idefn1(kno_scheme_module,"THREAD/FINISHED?",thread_finishedp,1,
            "(THREAD/EXITED? *thread*) returns true "
            "if *thread* exited normally, #F otherwise",
            kno_thread_type,VOID);
  kno_idefn1(kno_scheme_module,"THREAD/ERROR?",thread_errorp,1,
            "(THREAD/ERROR? *thread*) returns true if *thread* "
            "exited with an error, #F otherwise",
            kno_thread_type,VOID);
  kno_idefn1(kno_scheme_module,"THREAD/RESULT",thread_result,1,
            "(THREAD/RESULT *thread*) returns the final result of the thread "
            "or {} if it hasn't finished. If the thread returned an error "
            "this returns the exception object for the error. If you want to "
            "wait for the result, use THREAD/JOIN.",
            kno_thread_type,VOID);

  timeout_symbol = kno_intern("timeout");
  logexit_symbol = kno_intern("logexit");
  void_symbol = kno_intern("void");

  kno_idefn(kno_scheme_module,kno_make_cprim0("MAKE-CONDVAR",make_condvar));
  kno_idefn(kno_scheme_module,kno_make_cprim2("CONDVAR-WAIT",condvar_wait,1));
  kno_idefn(kno_scheme_module,kno_make_cprim2("CONDVAR-SIGNAL",condvar_signal,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("CONDVAR-LOCK",condvar_lock,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("CONDVAR-UNLOCK",condvar_unlock,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("SYNCHRO-LOCK",synchro_lock,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("SYNCHRO-UNLOCK",synchro_unlock,1));
  kno_def_evalfn(kno_scheme_module,"WITH-LOCK","",with_lock_evalfn);


  kno_idefn(kno_scheme_module,kno_make_cprim0("STACK-DEPTH",stack_depth_prim));
  kno_idefn(kno_scheme_module,kno_make_cprim0("STACK-LIMIT",stack_limit_prim));
  kno_idefn(kno_scheme_module,kno_make_cprim1x("STACK-LIMIT!",set_stack_limit_prim,1,
                                            kno_fixnum_type,VOID));

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

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
