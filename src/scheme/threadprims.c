/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2009 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_PPTRS 1

#include "fdb/dtype.h"
#include "fdb/support.h"
#include "fdb/fddb.h"
#include "fdb/eval.h"
#include "fdb/dtproc.h"
#include "fdb/numbers.h"
#include "fdb/sequences.h"
#include "fdb/ports.h"
#include "fdb/dtcall.h"

#include <libu8/u8timefns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

static u8_condition ThreadReturnError=_("Thread returned with error");
static u8_condition ThreadBacktrace=_("ThreadBacktrace");

/* Thread functions */

#if FD_THREADS_ENABLED

int fd_threaderror_backtrace=1;

fd_ptr_type fd_thread_type;
fd_ptr_type fd_condvar_type;

static int unparse_thread_struct(u8_output out,fdtype x)
{
  struct FD_THREAD_STRUCT *th=
    FD_GET_CONS(x,fd_thread_type,struct FD_THREAD_STRUCT *);
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

FD_EXPORT void recycle_thread_struct(struct FD_CONS *c)
{
  struct FD_THREAD_STRUCT *th=(struct FD_THREAD_STRUCT *)c;
  if (th->flags&FD_EVAL_THREAD) {
    fd_decref(th->evaldata.expr);
    fd_decref((fdtype)(th->evaldata.env));}
  else {
    int i=0, n=th->applydata.n_args; fdtype *args=th->applydata.args;
    while (i<n) {fd_decref(args[i]); i++;}
    u8_free(args); fd_decref(th->applydata.fn);}
  u8_free(th);
}

/* CONDVAR support */

static fdtype make_condvar()
{
  struct FD_CONSED_CONDVAR *cv=u8_alloc(struct FD_CONSED_CONDVAR);
  FD_INIT_CONS(cv,fd_condvar_type);
  fd_init_mutex(&(cv->lock)); u8_init_condvar(&(cv->cvar));
  return FDTYPE_CONS(cv);
}

/* This primitive combine cond_wait and cond_timedwait
   through a second (optional) argument, which is an
   interval in seconds. */
static fdtype condvar_wait(fdtype cvar,fdtype timeout)
{
  struct FD_CONSED_CONDVAR *cv=
    FD_GET_CONS(cvar,fd_condvar_type,struct FD_CONSED_CONDVAR *);
  if (FD_VOIDP(timeout))
    if (fd_condvar_wait(&(cv->cvar),&(cv->lock))==0)
      return FD_TRUE;
    else {
      return fd_type_error(_("valid condvar"),"condvar_wait",cvar);}
  else {
    struct timespec tm; int retval;
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
    retval=u8_condvar_timedwait(&(cv->cvar),&(cv->lock),&tm);
    if (retval==0)
      return FD_TRUE;
    else if (retval==ETIMEDOUT)
      return FD_FALSE;
    return fd_type_error(_("valid condvar"),"condvar_wait",cvar);}
}

/* This primitive combines signals and broadcasts through
   a second (optional) argument which, when true, implies
   a broadcast. */
static fdtype condvar_signal(fdtype cvar,fdtype broadcast)
{
  struct FD_CONSED_CONDVAR *cv=
    FD_GET_CONS(cvar,fd_condvar_type,struct FD_CONSED_CONDVAR *);
  if (FD_TRUEP(broadcast)) 
    if (u8_condvar_broadcast(&(cv->cvar))==0)
      return FD_TRUE;
    else return fd_type_error(_("valid condvar"),"condvar_signal",cvar);
  else if (u8_condvar_signal(&(cv->cvar))==0)
    return FD_TRUE;
  else return fd_type_error(_("valid condvar"),"condvar_signal",cvar);
}

static fdtype condvar_lock(fdtype cvar)
{
  struct FD_CONSED_CONDVAR *cv=
    FD_GET_CONS(cvar,fd_condvar_type,struct FD_CONSED_CONDVAR *);
  fd_lock_struct(cv);
  return FD_TRUE;
}

static fdtype condvar_unlock(fdtype cvar)
{
  struct FD_CONSED_CONDVAR *cv=
    FD_GET_CONS(cvar,fd_condvar_type,struct FD_CONSED_CONDVAR *);
  fd_unlock_struct(cv);
  return FD_TRUE;
}

static int unparse_condvar(u8_output out,fdtype cvar)
{
  struct FD_CONSED_CONDVAR *cv=
    FD_GET_CONS(cvar,fd_condvar_type,struct FD_CONSED_CONDVAR *);
  u8_printf(out,"#<CONDVAR %lx>",cv);
  return 1;
}

FD_EXPORT void recycle_condvar(struct FD_CONS *c)
{
  struct FD_CONSED_CONDVAR *cv=
    (struct FD_CONSED_CONDVAR *)c;
  fd_destroy_mutex(&(cv->lock));  u8_destroy_condvar(&(cv->cvar));
  u8_free(cv);
}

/* These functions generically access the locks on CONDVARs
   and SPROCs */

static fdtype synchro_lock(fdtype lck)
{
  if (FD_PTR_TYPEP(lck,fd_condvar_type)) {
    struct FD_CONSED_CONDVAR *cv=
      FD_GET_CONS(lck,fd_condvar_type,struct FD_CONSED_CONDVAR *);
    fd_lock_struct(cv);
    return FD_TRUE;}
  else if (FD_PTR_TYPEP(lck,fd_sproc_type)) {
    struct FD_SPROC *sp=FD_GET_CONS(lck,fd_sproc_type,struct FD_SPROC *);
    if (sp->synchronized) {
      fd_lock_struct(sp);}
    else return fd_type_error("lockable","synchro_lock",lck);
    return FD_TRUE;}
  else return fd_type_error("lockable","synchro_lock",lck);
}

static fdtype synchro_unlock(fdtype lck)
{
  if (FD_PTR_TYPEP(lck,fd_condvar_type)) {
    struct FD_CONSED_CONDVAR *cv=
      FD_GET_CONS(lck,fd_condvar_type,struct FD_CONSED_CONDVAR *);
    fd_unlock_struct(cv);
    return FD_TRUE;}
  else if (FD_PTR_TYPEP(lck,fd_sproc_type)) {
    struct FD_SPROC *sp=FD_GET_CONS(lck,fd_sproc_type,struct FD_SPROC *);
    if (sp->synchronized) {
      fd_unlock_struct(sp);}
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
  if (FD_PTR_TYPEP(lck,fd_condvar_type)) {
    struct FD_CONSED_CONDVAR *cv=
      FD_GET_CONS(lck,fd_condvar_type,struct FD_CONSED_CONDVAR *);
    fd_lock_struct(cv);}
  else if (FD_PTR_TYPEP(lck,fd_sproc_type)) {
    struct FD_SPROC *sp=FD_GET_CONS(lck,fd_sproc_type,struct FD_SPROC *);
    if (sp->synchronized) {
      fd_lock_struct(sp);}
    else return fd_type_error("lockable","synchro_lock",lck);}
  else return fd_type_error("lockable","synchro_unlock",lck);
  FD_DOLIST(elt_expr,FD_CDR(FD_CDR(expr))) {
    fd_decref(value); value=fd_eval(elt_expr,env);}
  if (FD_PTR_TYPEP(lck,fd_condvar_type)) {
    struct FD_CONSED_CONDVAR *cv=
      FD_GET_CONS(lck,fd_condvar_type,struct FD_CONSED_CONDVAR *);
    fd_unlock_struct(cv);}
  else if (FD_PTR_TYPEP(lck,fd_sproc_type)) {
    struct FD_SPROC *sp=FD_GET_CONS(lck,fd_sproc_type,struct FD_SPROC *);
    if (sp->synchronized) {
      fd_unlock_struct(sp);}
    else return fd_type_error("lockable","synchro_lock",lck);}
  else return fd_type_error("lockable","synchro_unlock",lck);
  return value;
}

/* Functions */

static void *thread_call(void *data)
{
  fdtype result;
  struct FD_THREAD_STRUCT *tstruct=(struct FD_THREAD_STRUCT *)data;
  /* Run any thread init functions */
  u8_threadcheck();

  if (tstruct->flags&FD_EVAL_THREAD) 
    result=fd_eval(tstruct->evaldata.expr,tstruct->evaldata.env);
  else 
    result=fd_dapply(tstruct->applydata.fn,
		     tstruct->applydata.n_args,
		     tstruct->applydata.args);
  result=fd_finish_call(result);
  u8_threadexit();
  if (FD_ABORTP(result)) {
    u8_exception ex=u8_erreify();
    u8_string errstring=fd_errstring(ex);
    if (tstruct->flags&FD_EVAL_THREAD)
      u8_log(LOG_WARN,ThreadReturnError,
	     "%q ==> %s",tstruct->evaldata.expr,errstring);
    else u8_log(LOG_WARN,ThreadReturnError,"%q ==> %s",
		tstruct->applydata.fn,errstring);
    u8_free(errstring);
    if (fd_threaderror_backtrace) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,16384);
      fd_summarize_backtrace(&out,ex);
      u8_log(LOG_WARN,ThreadBacktrace,"%s",out.u8_outbuf);
      if (fd_dump_backtrace) {
	out.u8_outptr=out.u8_outbuf;
	fd_print_backtrace(&out,ex,120);
	fd_dump_backtrace(out.u8_outbuf);}
      u8_free(out.u8_outbuf);}
    if (tstruct->resultptr)
      *(tstruct->resultptr)=fd_init_exception(NULL,ex);
    else u8_free_exception(ex,1);}
  else if (tstruct->resultptr) *(tstruct->resultptr)=result;
  else fd_decref(result);
  tstruct->flags=tstruct->flags|FD_THREAD_DONE;
  fd_decref((fdtype)tstruct);
  return NULL;
}

FD_EXPORT
fd_thread_struct fd_thread_call
  (fdtype *resultptr,fdtype fn,int n,fdtype *rail)
{
  struct FD_THREAD_STRUCT *tstruct=u8_alloc(struct FD_THREAD_STRUCT);
  FD_INIT_CONS(tstruct,fd_thread_type);
  if (resultptr) {
    tstruct->resultptr=resultptr; tstruct->result=FD_NULL;}
  else {
    tstruct->result=FD_NULL; tstruct->resultptr=&(tstruct->result);}
  tstruct->flags=0;
  tstruct->applydata.fn=fd_incref(fn);
  tstruct->applydata.n_args=n; tstruct->applydata.args=rail; 
  /* We need to do this first, before the thread exits and recycles itself! */
  fd_incref((fdtype)tstruct);
  pthread_create(&(tstruct->tid),pthread_attr_default,
		 thread_call,(void *)tstruct);
  return tstruct;
}

FD_EXPORT
fd_thread_struct fd_thread_eval(fdtype *resultptr,fdtype expr,fd_lispenv env)
{
  struct FD_THREAD_STRUCT *tstruct=u8_alloc(struct FD_THREAD_STRUCT);
  FD_INIT_CONS(tstruct,fd_thread_type);
  if (resultptr) {
    tstruct->resultptr=resultptr; tstruct->result=FD_NULL;}
  else {
    tstruct->result=FD_NULL; tstruct->resultptr=&(tstruct->result);}
  tstruct->flags=FD_EVAL_THREAD;
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
  fdtype *call_args=u8_alloc_n((n-1),fdtype), thread;
  int i=1; while (i<n) {
    call_args[i-1]=fd_incref(args[i]); i++;}
  thread=(fdtype)fd_thread_call(NULL,args[0],n-1,call_args);
  return thread;
}

static fdtype threadeval_handler(fdtype expr,fd_lispenv env)
{
  fdtype to_eval=fd_get_arg(expr,1);
  if (FD_VOIDP(to_eval))
    return fd_err(fd_SyntaxError,"threadeval_handler",NULL,expr);
  else if (FD_CHOICEP(to_eval)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(thread_expr,to_eval) {
      fdtype thread=(fdtype)fd_thread_eval(NULL,thread_expr,env);
      FD_ADD_TO_CHOICE(results,thread);}
    return results;}
  else return (fdtype)fd_thread_eval(NULL,to_eval,env);
}

static fdtype threadjoin_prim(fdtype threads)
{
  fdtype results=FD_EMPTY_CHOICE;
  {FD_DO_CHOICES(thread,threads)
     if (!(FD_PTR_TYPEP(thread,fd_thread_type)))
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
    threads[i]=fd_thread_eval(&results[i],FD_CAR(scan),env);
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
  fd_idefn(fd_scheme_module,fd_make_cprimn("THREADCALL",threadcall_prim,1));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("THREADJOIN",threadjoin_prim,1)));

  fd_idefn(fd_scheme_module,fd_make_cprim0("MAKE-CONDVAR",make_condvar,0));
  fd_idefn(fd_scheme_module,fd_make_cprim2("CONDVAR-WAIT",condvar_wait,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("CONDVAR-SIGNAL",condvar_signal,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CONDVAR-LOCK",condvar_lock,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CONDVAR-UNLOCK",condvar_unlock,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SYNCHRO-LOCK",synchro_lock,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SYNCHRO-UNLOCK",synchro_unlock,1));
  fd_defspecial(fd_scheme_module,"WITH-LOCK",with_lock_handler);


  fd_register_config("THREADTRACE","Whether errors in threads print out full backtraces",
		     fd_boolconfig_get,fd_boolconfig_set,&fd_threaderror_backtrace);

  fd_register_source_file(versionid);
}
#else
FD_EXPORT void fd_init_threadprims_c()
{
  fd_register_source_file(versionid);
}
#endif

