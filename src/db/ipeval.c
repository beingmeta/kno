/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_IPEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/apply.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"

#include <libu8/libu8.h>
#include <libu8/u8filefns.h>
#include <libu8/u8timefns.h>

int fd_trace_ipeval=0;

#if (FD_GLOBAL_IPEVAL)
static u8_mutex global_ipeval_lock;
int fd_ipeval_state;
#elif (FD_USE__THREAD)
__thread int fd_ipeval_state;
#elif (FD_USE_TLS)
u8_tld_key fd_ipeval_state_key;
#else
int fd_ipeval_state;
#endif

static u8_condition ipeval_fetch="FETCH",
  ipeval_exec="EXEC", ipeval_done="IPEDONE";

FD_EXPORT fd_wideint _fd_ipeval_delay(int n)
{
  return fd_ipeval_delay(n);
}
FD_EXPORT fd_wideint _fd_ipeval_status()
{
  return fd_ipeval_status();
}
FD_EXPORT int _fd_ipeval_failp()
{
  return (fd_ipeval_status()>1);
}
FD_EXPORT void _fd_set_ipeval_state(fd_wideint s)
{
  fd_set_ipeval_state(s);
}

static void ipeval_start_msg(u8_condition c,int iteration)
{
  u8_log(LOG_DEBUG,c,"Starting %s#%d",c,iteration);
}

static void ipeval_done_msg(u8_condition c,int iteration,double interval)
{
  u8_log(LOG_DEBUG,c,"Completed %s#%d in %lf seconds",c,iteration,interval);
}

#define time_since(x) (u8_elapsed_time()-(x))

/* Putting it all together */

FD_EXPORT int fd_ipeval_call(int (*fcn)(void *),void *data)
{
  int state, saved_state, ipeval_count=1, retval=0, gave_up=0;
  /* This will be NULL if a thread cache is already in force. */
  struct FD_THREAD_CACHE *tc=fd_use_threadcache();
  double start, point;
#if FD_GLOBAL_IPEVAL
  if (fd_ipeval_status()>0) {
    retval=fcn(data);
    if (tc) fd_pop_threadcache(tc);
    return retval;}
  fd_lock_mutex(&global_ipeval_lock);
#endif
  saved_state=state=fd_ipeval_status();
  start=u8_elapsed_time(); point=start;
  if (state<1) {
    fd_init_pool_delays();
    fd_init_index_delays();}
  fd_set_ipeval_state(1);
  if (fd_trace_ipeval)
    ipeval_start_msg(ipeval_exec,ipeval_count);
  retval=fcn(data);
  state=fd_ipeval_status();
  while ((retval>=0) && (state>1)) {
    fd_set_ipeval_state(0);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval) {
      ipeval_done_msg(ipeval_exec,ipeval_count,time_since(point));
      ipeval_start_msg(ipeval_fetch,ipeval_count);}
#endif
    point=u8_elapsed_time();
    fd_for_pools(fd_execute_pool_delays,NULL);
    fd_for_indices(fd_execute_index_delays,NULL);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval)
      ipeval_done_msg(ipeval_fetch,ipeval_count,time_since(point));
#endif
    ipeval_count++;
    fd_set_ipeval_state(1);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval) ipeval_start_msg(ipeval_exec,ipeval_count);
#endif
    point=u8_elapsed_time();
    retval=fcn(data);
    state=fd_ipeval_status();}
  if (gave_up) {
    fd_set_ipeval_state(0);
    retval=fcn(data);}
  fd_set_ipeval_state(saved_state);
#if FD_TRACE_IPEVAL
  if (fd_trace_ipeval) {
    u8_log(LOG_DEBUG,ipeval_exec,"IPEVAL iteration #%d completed in %lf",
	   ipeval_count,time_since(point));
    u8_log(LOG_DEBUG,ipeval_done,
	   "IPEVAL finished with %d iterations in %lf seconds",
	   ipeval_count,time_since(start));}
#endif
#if FD_GLOBAL_IPEVAL
  fd_unlock_mutex(&global_ipeval_lock);
#endif
  if (tc) fd_pop_threadcache(tc);
  return retval;
}

FD_EXPORT int fd_tracked_ipeval_call(int (*fcn)(void *),void *data,
				     struct FD_IPEVAL_RECORD **history,
				     int *n_cycles,double *total_time)
{
  int state, saved_state, ipeval_count=1, n_records=16, delays, retval=0, gave_up=0;
  /* This will be NULL if a thread cache is already in force. */
  struct FD_THREAD_CACHE *tc=fd_use_threadcache();
  /* This keeps track of execution. */
  struct FD_IPEVAL_RECORD *records=u8_alloc_n(16,struct FD_IPEVAL_RECORD);
  double start, point, exec_time, fetch_time;
#if FD_GLOBAL_IPEVAL
  if (fd_ipeval_status()>0) {
    if (tc) fd_pop_threadcache(tc);
    return fcn(data);}
  fd_lock_mutex(&global_ipeval_lock);
#endif
  saved_state=state=fd_ipeval_status();
  start=u8_elapsed_time(); point=start;
  if (state<1) {
    fd_init_pool_delays();
    fd_init_index_delays();}
  fd_set_ipeval_state(1);
  retval=fcn(data);
  exec_time=time_since(point);
  delays=fd_ipeval_status()-1;
  state=fd_ipeval_status();
  while ((retval>=0) && (state>1)) {
    fd_set_ipeval_state(0);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval) {
      ipeval_done_msg(ipeval_exec,ipeval_count,exec_time);
      ipeval_start_msg(ipeval_fetch,ipeval_count);}
#endif
    point=u8_elapsed_time();
    fd_for_pools(fd_execute_pool_delays,NULL);
    fd_for_indices(fd_execute_index_delays,NULL);
    fetch_time=time_since(point);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval)
      ipeval_done_msg(ipeval_fetch,ipeval_count,fetch_time);
#endif
    if (ipeval_count>=n_records) {
      records=u8_realloc_n(records,n_records+16,struct FD_IPEVAL_RECORD);
      n_records=n_records+16;}
    records[ipeval_count-1].cycle=ipeval_count;
    records[ipeval_count-1].delays=delays;
    records[ipeval_count-1].exec_time=exec_time;
    records[ipeval_count-1].fetch_time=fetch_time;
    ipeval_count++;
    fd_set_ipeval_state(1);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval) ipeval_start_msg(ipeval_exec,ipeval_count);
#endif
    point=u8_elapsed_time();
    retval=fcn(data);
    exec_time=time_since(point); delays=fd_ipeval_status()-1;
    state=fd_ipeval_status();}
  if (gave_up) {
    fd_set_ipeval_state(0);
    retval=fcn(data);
    exec_time=time_since(point); delays=0;}
  records[ipeval_count-1].cycle=ipeval_count;
  records[ipeval_count-1].delays=delays;
  records[ipeval_count-1].exec_time=exec_time;
  records[ipeval_count-1].fetch_time=0.0;
  fd_set_ipeval_state(saved_state);
  *history=records; *n_cycles=ipeval_count; *total_time=time_since(start);
#if FD_TRACE_IPEVAL
  if (fd_trace_ipeval) {
    u8_log(LOG_INFO,ipeval_exec,"IPEVAL iteration #%d completed in %lf",
	   ipeval_count,time_since(point));
    u8_log(LOG_INFO,ipeval_done,
	   "IPEVAL finished with %d iterations in %lf seconds",
	   ipeval_count,time_since(start));}
#endif
#if FD_GLOBAL_IPEVAL
  fd_unlock_mutex(&global_ipeval_lock);
#endif
  if (tc) fd_pop_threadcache(tc);
  return retval;
}

/* Cache calls */

static struct FD_HASHTABLE fcn_caches;

static struct FD_HASHTABLE *get_fcn_cache(fdtype fcn,int create)
{
  fdtype cache=fd_hashtable_get(&fcn_caches,fcn,FD_VOID);
  if (FD_VOIDP(cache)) {
    cache=fd_make_hashtable(NULL,512);
    fd_hashtable_store(&fcn_caches,fcn,cache);
    return FD_GET_CONS(cache,fd_hashtable_type,struct FD_HASHTABLE *);}
  else return FD_GET_CONS(cache,fd_hashtable_type,struct FD_HASHTABLE *);
}

FD_EXPORT fdtype fd_cachecall(fdtype fcn,int n,fdtype *args)
{
  fdtype vec, cached;
  struct FD_HASHTABLE *cache=get_fcn_cache(fcn,1);
  struct FD_VECTOR vecstruct;
  vecstruct.consbits=0;
  vecstruct.length=n;
  vecstruct.freedata=0;
  vecstruct.data=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  cached=fd_hashtable_get(cache,vec,FD_VOID);
  if (FD_VOIDP(cached)) {
    int state=fd_ipeval_status();
    fdtype result=fd_finish_call(fd_dapply(fcn,n,args));
    if (FD_ABORTP(result)) {
      fd_decref((fdtype)cache);
      return result;}
    else if (fd_ipeval_status()==state) {
      fdtype *datavec=((n) ? (u8_alloc_n(n,fdtype)) : (NULL));
      fdtype key=fd_init_vector(NULL,n,datavec);
      int i=0; while (i<n) {
	datavec[i]=fd_incref(args[i]); i++;}
      fd_hashtable_store(cache,key,result);
      fd_decref(key);}
    fd_decref((fdtype)cache);
    return result;}
  else {
    fd_decref((fdtype)cache);
    return cached;}
}

FD_EXPORT fdtype fd_xcachecall
  (struct FD_HASHTABLE *cache,fdtype fcn,int n,fdtype *args)
{
  fdtype vec, cached;
  struct FD_VECTOR vecstruct;
  vecstruct.consbits=0;
  vecstruct.length=n;
  vecstruct.freedata=0;
  vecstruct.data=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  cached=fd_hashtable_get(cache,vec,FD_VOID);
  if (FD_VOIDP(cached)) {
    int state=fd_ipeval_status();
    fdtype result=fd_finish_call(fd_dapply(fcn,n,args));
    if (FD_ABORTP(result)) {
      fd_decref((fdtype)cache);
      return result;}
    else if (fd_ipeval_status()==state) {
      fdtype *datavec=((n) ? (u8_alloc_n(n,fdtype)) : (NULL));
      fdtype key=fd_init_vector(NULL,n,datavec);
      int i=0; while (i<n) {
	datavec[i]=fd_incref(args[i]); i++;}
      fd_hashtable_store(cache,key,result);
      fd_decref(key);}
    return result;}
  else return cached;
}

FD_EXPORT void fd_clear_callcache(fdtype arg)
{
  if (fcn_caches.n_keys==0) return;
  if (FD_VOIDP(arg)) fd_reset_hashtable(&fcn_caches,128,1);
  else if ((FD_VECTORP(arg)) && (FD_VECTOR_LENGTH(arg)>0)) {
    fdtype fcn=FD_VECTOR_REF(arg,0);
    fdtype table=fd_hashtable_get(&fcn_caches,fcn,FD_EMPTY_CHOICE);
    if (FD_EMPTY_CHOICEP(table)) return;
    /* This should probably reall signal an error. */
    else if (!(FD_HASHTABLEP(table))) return;
    else {
      int i=0, n_args=FD_VECTOR_LENGTH(arg)-1;
      fdtype *datavec=u8_alloc_n(n_args,fdtype);
      fdtype key=fd_init_vector(NULL,n_args,datavec);
      while (i<n_args) {
	datavec[i]=fd_incref(FD_VECTOR_REF(arg,i+1)); i++;}
      fd_hashtable_store((fd_hashtable)table,key,FD_VOID);
      fd_decref(key);}}
  else if (fd_hashtable_probe(&fcn_caches,arg))
    fd_hashtable_store(&fcn_caches,arg,FD_VOID);
  else return;
}

static int hashtable_cachecount(fdtype key,fdtype v,void *ptr)
{
  if (FD_HASHTABLEP(v)) {
    fd_hashtable h=(fd_hashtable)v;
    int *count=(int *)ptr;
    *count=*count+h->n_keys;}
  return 0;
}

FD_EXPORT long fd_callcache_load()
{
  int count=0;
  fd_for_hashtable(&fcn_caches,hashtable_cachecount,&count,1);
  return count;
}

FD_EXPORT int fd_cachecall_probe(fdtype fcn,int n,fdtype *args)
{
  fdtype vec; int iscached=0;
  struct FD_HASHTABLE *cache=get_fcn_cache(fcn,1);
  struct FD_VECTOR vecstruct;
  vecstruct.consbits=0;
  vecstruct.length=n;
  vecstruct.freedata=0;
  vecstruct.data=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  iscached=fd_hashtable_probe(cache,vec);
  fd_decref((fdtype)cache);
  return iscached;
}

FD_EXPORT int fd_xcachecall_probe(struct FD_HASHTABLE *cache,fdtype fcn,int n,fdtype *args)
{
  fdtype vec; int iscached=0;
  struct FD_VECTOR vecstruct;
  vecstruct.consbits=0;
  vecstruct.length=n;
  vecstruct.freedata=0;
  vecstruct.data=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  iscached=fd_hashtable_probe(cache,vec);
  return iscached;
}

FD_EXPORT fdtype fd_cachecall_try(fdtype fcn,int n,fdtype *args)
{
  fdtype vec; fdtype value;
  struct FD_HASHTABLE *cache=get_fcn_cache(fcn,1);
  struct FD_VECTOR vecstruct;
  vecstruct.consbits=0;
  vecstruct.length=n;
  vecstruct.freedata=0;
  vecstruct.data=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  value=fd_hashtable_get(cache,vec,FD_VOID);
  fd_decref((fdtype)cache);
  if (FD_VOIDP(value)) return FD_EMPTY_CHOICE;
  else return value;
}

FD_EXPORT fdtype fd_xcachecall_try(struct FD_HASHTABLE *cache,fdtype fcn,int n,fdtype *args)
{
  fdtype vec; fdtype value;
  struct FD_VECTOR vecstruct;
  vecstruct.consbits=0;
  vecstruct.length=n;
  vecstruct.freedata=0;
  vecstruct.data=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  value=fd_hashtable_get(cache,vec,FD_VOID);
  if (FD_VOIDP(value)) return FD_EMPTY_CHOICE;
  else return value;
}

/* Thread cache calls */

#define TCACHECALL_STACK_ELTS 8

FD_EXPORT fdtype fd_tcachecall(fdtype fcn,int n,fdtype *args)
{
  if (fd_threadcache) {
    fd_thread_cache tc=fd_threadcache;
    struct FD_VECTOR vecstruct;
    fdtype _elts[TCACHECALL_STACK_ELTS], *elts=NULL, vec, cached;
    /* Initialize the stack vector */
    vecstruct.consbits=0;
    vecstruct.freedata=0;
    vecstruct.length=n+1;
    FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
    /* Allocate an elements vector if neccessary */
    if ((n+1)>(TCACHECALL_STACK_ELTS)) 
      elts=u8_alloc_n(n+1,fdtype);
    else elts=_elts;
    /* Initialize the elements */
    elts[0]=fcn; memcpy(elts+1,args,sizeof(fdtype)*n);
    vecstruct.data=elts;
    vec=FDTYPE_CONS(&vecstruct);
    /* Look it up in the cache. */
    cached=fd_hashtable_get_nolock(&(tc->calls),vec,FD_VOID);
    if (!(FD_VOIDP(cached))) {
      if (elts!=_elts) u8_free(elts);
      return cached;}
    else {
      int state=fd_ipeval_status();
      fdtype result=fd_finish_call(fd_dapply(fcn,n,args));
      if (FD_ABORTP(result)) {
	if (elts!=_elts) u8_free(elts);
	return result;}
      else if (fd_ipeval_status()==state) {
	if (elts==_elts) {
	  int i=0, nelts=n+1;
	  elts=u8_alloc_n(n+1,fdtype);
	  while (i<nelts) {elts[i]=fd_incref(_elts[i]); i++;}
	  vec=fd_init_vector(NULL,nelts,elts);}
	else {
	  int i=0, nelts=n+1;
	  while (i<nelts) {fd_incref(elts[i]); i++;}
	  vec=fd_init_vector(NULL,nelts,elts);}
	fd_hashtable_store(&(tc->calls),vec,result);
	fd_decref(vec);
	return result;}
      else return result;}}
  else if (fd_cachecall_probe(fcn,n,args))
    return fd_cachecall(fcn,n,args);
  else return fd_finish_call(fd_dapply(fcn,n,args));
}

/* Initialization stuff */

FD_EXPORT void fd_init_ipeval_c()
{
  u8_register_source_file(_FILEINFO);
  fd_register_config("TRACEIPEVAL",_("Trace ipeval execution"),
		     fd_boolconfig_get,fd_boolconfig_set,&fd_trace_ipeval);
#if FD_GLOBAL_IPEVAL
  fd_init_mutex(&global_ipeval_lock);
#endif
#if (FD_USE_TLS)
  u8_new_threadkey(&fd_ipeval_state_key,NULL);
#endif

  FD_INIT_STATIC_CONS(&fcn_caches,fd_hashtable_type);
  fd_make_hashtable(&fcn_caches,128);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
