/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_FCNIDS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/support.h"
#include "framerd/fddb.h"
#include "framerd/eval.h"
#include "framerd/dtproc.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/ports.h"
#include "framerd/dtcall.h"

#include <libu8/u8timefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8logging.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

static u8_condition ThreadReturnError=_("ThreadError");
static u8_condition ThreadExit=_("ThreadExit");
static u8_condition ThreadBacktrace=_("ThreadBacktrace");

static int thread_loglevel=LOGNOTICE;
static int thread_log_exit=1;
static fdtype logexit_symbol=FD_VOID;

#ifndef U8_STRING_ARG
#define U8_STRING_ARG(s) (((s)==NULL)?((u8_string)""):((u8_string)(s)))
#endif

/* Thread functions */

#if FD_THREADS_ENABLED

int fd_thread_backtrace=0;

fd_ptr_type fd_thread_type;
fd_ptr_type fd_condvar_type;

static int unparse_thread_struct(u8_output out,fdtype x)
{
  struct FD_THREAD_STRUCT *th=
    fd_consptr(struct FD_THREAD_STRUCT *,x,fd_thread_type);
  if (th->flags&FD_EVAL_THREAD)
    u8_printf(out,"#<THREAD 0x%x%s eval %q>",
              (unsigned long)(th->tid),
              ((th->flags&FD_THREAD_DONE) ? (" done") : ("")),
              th->evaldata.expr);
  else u8_printf(out,"#<THREAD 0x%x%s apply %q>",
                 (unsigned long)(th->tid),
                 ((th->flags&FD_THREAD_DONE) ? (" done") : ("")),
                 th->applydata.fn);
  return 1;
}

FD_EXPORT void recycle_thread_struct(struct FD_RAW_CONS *c)
{
  struct FD_THREAD_STRUCT *th=(struct FD_THREAD_STRUCT *)c;
  if (th->flags&FD_EVAL_THREAD) {
    fd_decref(th->evaldata.expr);
    fd_decref((fdtype)(th->evaldata.env));}
  else {
    int i=0, n=th->applydata.n_args; fdtype *args=th->applydata.args;
    while (i<n) {fd_decref(args[i]); i++;}
    u8_free(args); fd_decref(th->applydata.fn);}
  if (th->result!=FD_NULL) fd_decref(th->result);
  th->resultptr=NULL;
  pthread_attr_destroy(&(th->attr));
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

/* CONDVAR support */

static fdtype make_condvar()
{
  int rv=0;
  struct FD_CONSED_CONDVAR *cv=u8_alloc(struct FD_CONSED_CONDVAR);
  FD_INIT_FRESH_CONS(cv,fd_condvar_type);
  rv=fd_init_mutex(&(cv->fd_cvlock));
  if (rv) {
    u8_graberr(-1,"make_condvar",NULL);
    return FD_ERROR_VALUE;}
  else rv=u8_init_condvar(&(cv->fd_cvar));
  if (rv) {
    u8_graberr(-1,"make_condvar",NULL);
    return FD_ERROR_VALUE;}
  return FDTYPE_CONS(cv);
}

/* This primitive combine cond_wait and cond_timedwait
   through a second (optional) argument, which is an
   interval in seconds. */
static fdtype condvar_wait(fdtype cvar,fdtype timeout)
{
  int rv=0;
  struct FD_CONSED_CONDVAR *cv=
    fd_consptr(struct FD_CONSED_CONDVAR *,cvar,fd_condvar_type);
  if (FD_VOIDP(timeout))
    if ((rv=fd_condvar_wait(&(cv->fd_cvar),&(cv->fd_cvlock)))==0)
      return FD_TRUE;
    else {
      return fd_type_error(_("valid condvar"),"condvar_wait",cvar);}
  else {
    struct timespec tm;
    if ((FD_FIXNUMP(timeout)) && (FD_FIX2INT(timeout)>=0)) {
      int ival=FD_FIX2INT(timeout);
      tm.tv_sec=time(NULL)+ival; tm.tv_nsec=0;}
#if 0 /* Define this later.  This allows sub-second waits but
         is a little bit tricky. */
    else if (FD_FLONUMP(timeout)) {
      double flo=FD_FLONUM(cvar);
      if (flo>=0) {
        int secs=floor(flo)/1000000;
        int nsecs=(flo-(secs*1000000))*1000.0;
        tm.tv_sec=secs; tm.tv_nsec=nsecs;}
      else return fd_type_error(_("time interval"),"condvar_wait",timeout);}
#endif
    else return fd_type_error(_("time interval"),"condvar_wait",timeout);
    rv=u8_condvar_timedwait(&(cv->fd_cvar),&(cv->fd_cvlock),&tm);
    if (rv==0)
      return FD_TRUE;
    else if (rv==ETIMEDOUT)
      return FD_FALSE;
    return fd_type_error(_("valid condvar"),"condvar_wait",cvar);}
}

/* This primitive combines signals and broadcasts through
   a second (optional) argument which, when true, implies
   a broadcast. */
static fdtype condvar_signal(fdtype cvar,fdtype broadcast)
{
  struct FD_CONSED_CONDVAR *cv=
    fd_consptr(struct FD_CONSED_CONDVAR *,cvar,fd_condvar_type);
  if (FD_TRUEP(broadcast))
    if (u8_condvar_broadcast(&(cv->fd_cvar))==0)
      return FD_TRUE;
    else return fd_type_error(_("valid condvar"),"condvar_signal",cvar);
  else if (u8_condvar_signal(&(cv->fd_cvar))==0)
    return FD_TRUE;
  else return fd_type_error(_("valid condvar"),"condvar_signal",cvar);
}

static fdtype condvar_lock(fdtype cvar)
{
  struct FD_CONSED_CONDVAR *cv=
    fd_consptr(struct FD_CONSED_CONDVAR *,cvar,fd_condvar_type);
  fd_lock_mutex(&(cv->fd_cvlock));
  return FD_TRUE;
}

static fdtype condvar_unlock(fdtype cvar)
{
  struct FD_CONSED_CONDVAR *cv=
    fd_consptr(struct FD_CONSED_CONDVAR *,cvar,fd_condvar_type);
  u8_unlock_mutex(&(cv->fd_cvlock));
  return FD_TRUE;
}

static int unparse_condvar(u8_output out,fdtype cvar)
{
  struct FD_CONSED_CONDVAR *cv=
    fd_consptr(struct FD_CONSED_CONDVAR *,cvar,fd_condvar_type);
  u8_printf(out,"#<CONDVAR %lx>",cv);
  return 1;
}

FD_EXPORT void recycle_condvar(struct FD_RAW_CONS *c)
{
  struct FD_CONSED_CONDVAR *cv=
    (struct FD_CONSED_CONDVAR *)c;
  u8_destroy_mutex(&(cv->fd_cvlock));  u8_destroy_condvar(&(cv->fd_cvar));
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

/* These functions generically access the locks on CONDVARs
   and SPROCs */

static fdtype synchro_lock(fdtype lck)
{
  if (FD_TYPEP(lck,fd_condvar_type)) {
    struct FD_CONSED_CONDVAR *cv=
      fd_consptr(struct FD_CONSED_CONDVAR *,lck,fd_condvar_type);
    u8_lock_mutex(&(cv->fd_cvlock));
    return FD_TRUE;}
  else if (FD_SPROCP(lck)) {
    struct FD_SPROC *sp=fd_consptr(fd_sproc,lck,fd_sproc_type);
    if (sp->sproc_synchronized) {
      u8_lock_mutex(&(sp->sproc_lock));}
    else return fd_type_error("lockable","synchro_lock",lck);
    return FD_TRUE;}
  else return fd_type_error("lockable","synchro_lock",lck);
}

static fdtype synchro_unlock(fdtype lck)
{
  if (FD_TYPEP(lck,fd_condvar_type)) {
    struct FD_CONSED_CONDVAR *cv=
      fd_consptr(struct FD_CONSED_CONDVAR *,lck,fd_condvar_type);
    u8_unlock_mutex(&(cv->fd_cvlock));
    return FD_TRUE;}
  else if (FD_SPROCP(lck)) {
    struct FD_SPROC *sp=fd_consptr(fd_sproc,lck,fd_sproc_type);
    if (sp->sproc_synchronized) {
      u8_lock_mutex(&(sp->sproc_lock));}
    else return fd_type_error("lockable","synchro_lock",lck);
    return FD_TRUE;}
  else return fd_type_error("lockable","synchro_unlock",lck);
}

static fdtype with_lock_handler(fdtype expr,fd_lispenv env)
{
  fdtype lock_expr=fd_get_arg(expr,1), lck, value=FD_VOID;
  if (FD_VOIDP(lock_expr))
    return fd_err(fd_SyntaxError,"with_lock_handler",NULL,expr);
  else lck=fd_eval(lock_expr,env);
  if (FD_TYPEP(lck,fd_condvar_type)) {
    struct FD_CONSED_CONDVAR *cv=
      fd_consptr(struct FD_CONSED_CONDVAR *,lck,fd_condvar_type);
    u8_lock_mutex(&(cv->fd_cvlock));}
  else if (FD_SPROCP(lck)) {
    struct FD_SPROC *sp=fd_consptr(fd_sproc,lck,fd_sproc_type);
    if (sp->sproc_synchronized) {
      u8_lock_mutex(&(sp->sproc_lock));}
    else return fd_type_error("lockable","synchro_lock",lck);}
  else return fd_type_error("lockable","synchro_unlock",lck);
  {U8_WITH_CONTOUR("WITH-LOCK",0) {
      FD_DOLIST(elt_expr,FD_CDR(FD_CDR(expr))) {
        fd_decref(value); value=fd_eval(elt_expr,env);}}
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      fd_decref(value);
      value=FD_ERROR_VALUE;}
    U8_END_EXCEPTION;}
  if (FD_TYPEP(lck,fd_condvar_type)) {
    struct FD_CONSED_CONDVAR *cv=
      fd_consptr(struct FD_CONSED_CONDVAR *,lck,fd_condvar_type);
    u8_unlock_mutex(&(cv->fd_cvlock));}
  else if (FD_SPROCP(lck)) {
    struct FD_SPROC *sp=fd_consptr(fd_sproc,lck,fd_sproc_type);
    if (sp->sproc_synchronized) {
      u8_unlock_mutex(&(sp->sproc_lock));}
    else return fd_type_error("lockable","synchro_lock",lck);}
  else return fd_type_error("lockable","synchro_unlock",lck);
  return value;
}

/* Functions */

static void *thread_call(void *data)
{
  fdtype result;
  struct FD_THREAD_STRUCT *tstruct=(struct FD_THREAD_STRUCT *)data;
  int flags=tstruct->flags, log_exit=
    ((flags)&(FD_THREAD_TRACE_EXIT)) ||
    (((flags)&(FD_THREAD_QUIET_EXIT)) ? (0) :
     (thread_log_exit));
  
  tstruct->errnop=&(errno);

  FD_INIT_STACK();

  /* Set (block) most signals */
  pthread_sigmask(SIG_SETMASK,fd_default_sigmask,NULL);

  /* Run any thread init functions */
  u8_threadcheck();

  tstruct->started=u8_elapsed_time();
  tstruct->finished=-1;

  if (tstruct->flags&FD_EVAL_THREAD)
    result=fd_eval(tstruct->evaldata.expr,tstruct->evaldata.env);
  else
    result=fd_dapply(tstruct->applydata.fn,
                     tstruct->applydata.n_args,
                     tstruct->applydata.args);
  result=fd_finish_call(result);
  
  tstruct->finished=u8_elapsed_time();

  if ((FD_ABORTP(result))&&(errno)) {
    u8_exception ex=u8_current_exception;
    u8_log(thread_loglevel,ThreadExit,
           "Thread #%lld error (errno=%s:%d) %s (%s) %s",
           u8_threadid(),u8_strerror(errno),errno,
           ex->u8x_cond,U8_STRING_ARG(ex->u8x_context),
           U8_STRING_ARG(ex->u8x_details));}
  else if (FD_ABORTP(result)) {
    u8_exception ex=u8_current_exception;
    u8_log(thread_loglevel,ThreadExit,
           "Thread #%lld error %s (%s) %s",
           u8_threadid(),ex->u8x_cond,
           U8_STRING_ARG(ex->u8x_context),
           U8_STRING_ARG(ex->u8x_details));}
  else if (errno) {
    u8_log(thread_loglevel,ThreadExit,
           "Thread #%lld exited (errno=%s:%d) returning %s%q",
           u8_threadid(),u8_strerror(errno),errno,
           ((FD_CONSP(result))?("\n    "):("")),
           result);}
  else if (log_exit) {
    u8_log(thread_loglevel,ThreadExit,
           "Thread #%lld exited returning %s%q",
           u8_threadid(),((FD_CONSP(result))?("\n    "):("")),result);}
  else {}

  u8_threadexit();
  if (FD_ABORTP(result)) {
    u8_exception ex=u8_erreify();
    fdtype exobj=fd_init_exception(NULL,ex);
    u8_string errstring=fd_errstring(ex);
    u8_log(LOG_WARN,ThreadReturnError,"Thread #%d %s",u8_threadid(),errstring);
    if (tstruct->flags&FD_EVAL_THREAD)
      u8_log(LOG_WARN,ThreadReturnError,"Thread #%d wasevaluating %q",
             u8_threadid(),tstruct->evaldata.expr);
      else u8_log(LOG_WARN,ThreadReturnError,"Thread #%d was applying %q",
                  u8_threadid(),tstruct->applydata.fn);
    u8_free(errstring);
    if (fd_thread_backtrace) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,16384);
      fd_summarize_backtrace(&out,ex);
      u8_log(LOG_WARN,ThreadBacktrace,"%s",out.u8_outbuf);
      if (fd_dump_backtrace) {
        out.u8_write=out.u8_outbuf;
        fd_print_backtrace(&out,ex,120);
        fd_dump_backtrace(out.u8_outbuf);}
      else fd_log_backtrace(ex,LOG_NOTICE,ThreadBacktrace,120);
      u8_free(out.u8_outbuf);}
    tstruct->result=exobj;
    if (tstruct->resultptr) {
      fd_incref(exobj);
      *(tstruct->resultptr)=exobj;}
    else {}}
  else {
    tstruct->result=result;
    if (tstruct->resultptr) {
      *(tstruct->resultptr)=result;
      fd_incref(result);}}
  tstruct->flags=tstruct->flags|FD_THREAD_DONE;
  fd_decref((fdtype)tstruct);
  return NULL;
}

FD_EXPORT
fd_thread_struct fd_thread_call(fdtype *resultptr,
                                fdtype fn,int n,fdtype *rail,
                                int flags)
{
  struct FD_THREAD_STRUCT *tstruct=u8_alloc(struct FD_THREAD_STRUCT);
  if (tstruct==NULL) {
    u8_seterr(u8_MallocFailed,"fd_thread_call",NULL);
    return NULL;}
  FD_INIT_FRESH_CONS(tstruct,fd_thread_type);
  if (resultptr) {
    tstruct->resultptr=resultptr;
    *resultptr=FD_NULL;
    tstruct->result=FD_NULL;}
  else {
    tstruct->result=FD_NULL;
    tstruct->resultptr=NULL;}
  tstruct->flags=flags;
  pthread_attr_init(&(tstruct->attr));
  tstruct->applydata.fn=fd_incref(fn);
  tstruct->applydata.n_args=n; 
  tstruct->applydata.args=rail;
  /* We need to do this first, before the thread exits and recycles itself! */
  fd_incref((fdtype)tstruct);
  pthread_create(&(tstruct->tid),&(tstruct->attr),
                 thread_call,(void *)tstruct);
  return tstruct;
}

FD_EXPORT
fd_thread_struct fd_thread_eval(fdtype *resultptr,
                                fdtype expr,fd_lispenv env,
                                int flags)
{
  struct FD_THREAD_STRUCT *tstruct=u8_alloc(struct FD_THREAD_STRUCT);
  if (tstruct==NULL) {
    u8_seterr(u8_MallocFailed,"fd_thread_eval",NULL);
    return NULL;}
  FD_INIT_FRESH_CONS(tstruct,fd_thread_type);
  if (resultptr) {
    tstruct->resultptr=resultptr; 
    *resultptr=FD_NULL;
    tstruct->result=FD_NULL;}
  else {
    tstruct->result=FD_NULL;
    tstruct->resultptr=NULL;}
  tstruct->flags=flags|FD_EVAL_THREAD;
  tstruct->evaldata.expr=fd_incref(expr);
  tstruct->evaldata.env=fd_copy_env(env);
  /* We need to do this first, before the thread exits and recycles itself! */
  fd_incref((fdtype)tstruct);
  pthread_create(&(tstruct->tid),pthread_attr_default,
                 thread_call,(void *)tstruct);
  return tstruct;
}

/* Scheme primitives */

static fdtype threadcall_prim(int n,fdtype *args)
{
  fdtype fn = args[0];
  if (FD_APPLICABLEP(fn)) {
    fdtype *call_args=u8_alloc_n(n,fdtype), thread;
    int i=1; while (i<n) {
      fdtype call_arg = args[i]; fd_incref(call_arg);
      call_args[i-1]=call_arg; i++;}
    thread=(fdtype)fd_thread_call(NULL,args[0],n-1,call_args,0);
    return thread;}
  else if (FD_VOIDP(fn))
    return fd_err(fd_TooFewArgs,"threadcall_prim",NULL,FD_VOID);
  else {
    fd_incref(fn);
    return fd_type_error(_("applicable"),"threadcall_prim",fn);}
}

static int threadopts(fdtype opts)
{
  fdtype logexit=fd_getopt(opts,logexit_symbol,FD_VOID);
  if (FD_VOIDP(logexit)) {
    if (thread_log_exit>0)
      return FD_THREAD_TRACE_EXIT;
    else return FD_THREAD_QUIET_EXIT;}
  else if ((FD_FALSEP(logexit))||(FD_ZEROP(logexit)))
    return FD_THREAD_QUIET_EXIT;
  else {
    fd_decref(logexit);
    return FD_THREAD_TRACE_EXIT;}
}

static fdtype threadcallx_prim(int n,fdtype *args)
{
  fdtype opts = args[0];
  fdtype fn   = args[1];
  if (FD_APPLICABLEP(fn)) {
    fdtype *call_args=u8_alloc_n(n-2,fdtype), thread;
    int flags=threadopts(opts);
    int i=2; while (i<n) {
      fdtype call_arg = args[i]; fd_incref(call_arg);
      call_args[i-2]=call_arg; i++;}
    thread=(fdtype)fd_thread_call(NULL,fn,n-2,call_args,flags);
    return thread;}
  else if (FD_VOIDP(fn))
    return fd_err(fd_TooFewArgs,"threadcallx_prim",NULL,FD_VOID);
  else {
    fd_incref(fn);
    return fd_type_error(_("applicable"),"threadcallx_prim",fn);}
}

static fdtype threadeval_handler(fdtype expr,fd_lispenv env)
{
  fdtype to_eval=fd_get_arg(expr,1);
  fdtype env_arg=fd_eval(fd_get_arg(expr,2),env);
  fdtype opts_arg=fd_eval(fd_get_arg(expr,3),env);
  fdtype opts=((FD_VOIDP(opts_arg))&&
               (!(FD_ENVIRONMENTP(env_arg)))&&
               (FD_TABLEP(env_arg)))?
    (env_arg):
    (opts_arg);
  fd_lispenv use_env=
    ((FD_VOIDP(env_arg))||(FD_FALSEP(env_arg)))?(env):
    (FD_ENVIRONMENTP(env_arg))?((fd_lispenv)env_arg):
    (NULL);
  if (FD_VOIDP(to_eval)) {
    fd_decref(opts_arg); fd_decref(env_arg);
    return fd_err(fd_SyntaxError,"threadeval_handler",NULL,expr);}
  else if (use_env==NULL) {
    fd_decref(opts_arg);
    return fd_type_error(_("lispenv"),"threadeval_handler",env_arg);}
  else {
    int flags=threadopts(opts)|FD_EVAL_THREAD;
    fd_lispenv env_copy=fd_copy_env(use_env);
    fdtype results=FD_EMPTY_CHOICE, envptr=(fdtype)env_copy;
    FD_DO_CHOICES(thread_expr,to_eval) {
      fdtype thread=(fdtype)fd_thread_eval(NULL,thread_expr,env_copy,flags);
      FD_ADD_TO_CHOICE(results,thread);}
    fd_decref(envptr); fd_decref(env_arg); fd_decref(opts_arg);
    return results;}
}

static fdtype thread_exitedp(fdtype thread_arg)
{
  struct FD_THREAD_STRUCT *thread=(struct FD_THREAD_STRUCT *)thread_arg;
  if ((thread->flags)&(FD_THREAD_DONE))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype thread_errorp(fdtype thread_arg)
{
  struct FD_THREAD_STRUCT *thread=(struct FD_THREAD_STRUCT *)thread_arg;
  if ((thread->flags)&(FD_THREAD_DONE)) {
    if (FD_ABORTP(thread->result))
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

static fdtype thread_result(fdtype thread_arg)
{
  struct FD_THREAD_STRUCT *thread=(struct FD_THREAD_STRUCT *)thread_arg;
  if ((thread->flags)&(FD_THREAD_DONE)) {
    if (thread->result!=FD_NULL) {
      fdtype result=thread->result;
      fd_incref(result);
      return result;}
    else return FD_EMPTY_CHOICE;}
  else return FD_EMPTY_CHOICE;
}

static fdtype threadjoin_prim(fdtype threads)
{
  fdtype results=FD_EMPTY_CHOICE;
  {FD_DO_CHOICES(thread,threads)
     if (!(FD_TYPEP(thread,fd_thread_type)))
       return fd_type_error(_("thread"),"threadjoin_prim",thread);}
  {FD_DO_CHOICES(thread,threads) {
    struct FD_THREAD_STRUCT *tstruct=(fd_thread_struct)thread;
    int retval=pthread_join(tstruct->tid,NULL);
    if (retval==0) {
      if ((tstruct->resultptr)==&(tstruct->result))
        if (!(FD_VOIDP(tstruct->result))) {
          fd_incref(tstruct->result);
          FD_ADD_TO_CHOICE(results,tstruct->result);}}
    else u8_log(LOG_WARN,ThreadReturnError,"Bad return code %d (%s) from %q",
                 retval,strerror(retval),thread);}}
  return results;
}

static fdtype parallel_handler(fdtype expr,fd_lispenv env)
{
  fd_thread_struct _threads[6], *threads;
  fdtype _results[6], *results, scan=FD_CDR(expr), result=FD_EMPTY_CHOICE;
  int i=0, n_exprs=0;
  /* Compute number of expressions */
  while (FD_PAIRP(scan)) {n_exprs++; scan=FD_CDR(scan);}
  /* Malloc two vectors if neccessary. */
  if (n_exprs>6) {
    results=u8_alloc_n(n_exprs,fdtype);
    threads=u8_alloc_n(n_exprs,fd_thread_struct);}
  else {results=_results; threads=_threads;}
  /* Start up the threads and store the pointers. */
  scan=FD_CDR(expr); while (FD_PAIRP(scan)) {
    threads[i]=fd_thread_eval(&results[i],FD_CAR(scan),env,
                              FD_EVAL_THREAD|FD_THREAD_QUIET_EXIT);
    scan=FD_CDR(scan); i++;}
  /* Now wait for them to finish, accumulating values. */
  i=0; while (i<n_exprs) {
    pthread_join(threads[i]->tid,NULL);
    /* If any threads return errors, return the errors, combining
       them as contexts if multiple. */
    FD_ADD_TO_CHOICE(result,results[i]);
    fd_decref((fdtype)(threads[i]));
    i++;}
  if (n_exprs>6) {
    u8_free(threads); u8_free(results);}
  return result;
}

static fdtype threadyield_prim()
{
#if _POSIX_PRIORITY_SCHEDULING
  int retval=sched_yield();
#elif HAVE_NANOSLEEP
  struct timespec req={0,1000}, rem={0,0};
  int retval=nanosleep(&req,&rem);
  if (retval>=0)
    retval=((rem.tv_sec==0)&&(rem.tv_nsec<1000));
#elif HAVE_SLEEP
  int retval=sleep(1);
  if (retval>=0) {
    if (retval==0) retval=1; else retval=0;}
#endif
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

/* Thread information */

static fdtype stack_depth_prim()
{
  ssize_t depth=u8_stack_depth();
  return FD_INT2DTYPE(depth);
}
static fdtype stack_limit_prim()
{
  ssize_t limit=fd_stack_limit();
  return FD_INT2DTYPE(limit);
}
static fdtype set_stack_limit_prim(fdtype arg)
{
  ssize_t limit=FD_FIX2INT(arg);
  limit=fd_stack_limit_set(limit);
  return FD_INT2DTYPE(limit);
}

#endif

#if FD_THREADS_ENABLED
FD_EXPORT void fd_init_threadprims_c()
{
  fd_thread_type=fd_register_cons_type(_("thread"));
  fd_recyclers[fd_thread_type]=recycle_thread_struct;
  fd_unparsers[fd_thread_type]=unparse_thread_struct;

  fd_condvar_type=fd_register_cons_type(_("condvar"));
  fd_recyclers[fd_condvar_type]=recycle_condvar;
  fd_unparsers[fd_condvar_type]=unparse_condvar;

  fd_defspecial(fd_scheme_module,"PARALLEL",parallel_handler);
  fd_defspecial(fd_scheme_module,"SPAWN",threadeval_handler);
  fd_idefn(fd_scheme_module,fd_make_cprimn("THREAD/CALL",threadcall_prim,1));
  fd_defalias(fd_scheme_module,"THREADCALL","THREAD/CALL");
  fd_idefn(fd_scheme_module,fd_make_cprimn("THREAD/CALL+",threadcallx_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim0("THREAD/YIELD",threadyield_prim,0));
  fd_defalias(fd_scheme_module,"THREADYIELD","THREAD/YIELD");
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("THREAD/JOIN",threadjoin_prim,1)));
  fd_defalias(fd_scheme_module,"THREADJOIN","THREAD/JOIN");

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("THREAD/EXITED?",thread_exitedp,1,
                           fd_thread_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("THREAD/ERROR?",thread_errorp,1,
                           fd_thread_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("THREAD/RESULT",thread_result,1,
                           fd_thread_type,FD_VOID));

  logexit_symbol=fd_intern("LOGEXIT");

  fd_idefn(fd_scheme_module,fd_make_cprim0("MAKE-CONDVAR",make_condvar,0));
  fd_idefn(fd_scheme_module,fd_make_cprim2("CONDVAR-WAIT",condvar_wait,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("CONDVAR-SIGNAL",condvar_signal,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CONDVAR-LOCK",condvar_lock,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CONDVAR-UNLOCK",condvar_unlock,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SYNCHRO-LOCK",synchro_lock,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SYNCHRO-UNLOCK",synchro_unlock,1));
  fd_defspecial(fd_scheme_module,"WITH-LOCK",with_lock_handler);


  fd_idefn(fd_scheme_module,fd_make_cprim0("STACK-DEPTH",stack_depth_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("STACK-LIMIT",stack_limit_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("STACK-LIMIT!",set_stack_limit_prim,1,
                                            fd_fixnum_type,FD_VOID));

  fd_register_config("THREAD:BACKTRACE",
                     "Whether errors in threads print out full backtraces",
                     fd_boolconfig_get,fd_boolconfig_set,
                     &fd_thread_backtrace);
  fd_register_config("THREAD:LOGLEVEL",
                     "The log level to use for thread-related events",
                     fd_intconfig_get,fd_loglevelconfig_set,
                     &thread_loglevel);
  fd_register_config("THREAD:LOGEXIT",
                     "Whether to log the normal exit values of threads",
                     fd_boolconfig_get,fd_boolconfig_set,
                     &thread_log_exit);

  u8_register_source_file(_FILEINFO);
}
#else
FD_EXPORT void fd_init_threadprims_c()
{
  u8_register_source_file(_FILEINFO);
}
#endif


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
