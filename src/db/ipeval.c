/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_IPEVAL 1

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/apply.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/indices.h"

#include <libu8/libu8.h>
#include <libu8/u8filefns.h>
#include <libu8/u8timefns.h>

int fd_trace_ipeval=0;

#if (FD_GLOBAL_IPEVAL)
static u8_mutex global_ipeval_lock;
int fd_ipeval_state;
#elif (FD_USE_TLS)
u8_tld_key fd_ipeval_state_key;
#elif (FD_USE__THREAD)
__thread int fd_ipeval_state;
#else
int fd_ipeval_state;
#endif

static u8_condition ipeval_fetch="FETCH",
  ipeval_exec="EXEC", ipeval_done="IPEDONE";

FD_EXPORT int _fd_ipeval_delay(int n)
{
  return fd_ipeval_delay(n);
}
FD_EXPORT int _fd_ipeval_status()
{
  return fd_ipeval_status();
}
FD_EXPORT int _fd_ipeval_failp()
{
  return (fd_ipeval_status()>1);
}
FD_EXPORT void _fd_set_ipeval_state(int s)
{
  fd_set_ipeval_state(s);
}

static void ipeval_start_msg(u8_condition c,int iteration)
{
  u8_notify(c,"Starting %s#%d",c,iteration);
}

static void ipeval_done_msg(u8_condition c,int iteration,double interval)
{
  u8_notify(c,"Completed %s#%d in %lf seconds",c,iteration,interval);
}

#define time_since(x) (u8_elapsed_time()-(x))

/* Putting it all together */

FD_EXPORT int fd_ipeval_call(int (*fcn)(void *),void *data)
{
  int state, saved_state, ipeval_count=1, retval=0;
  double start, point;
#if FD_GLOBAL_IPEVAL
  if (fd_ipeval_status()>0) {
    retval=fcn(data); return retval;}
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
  fd_set_ipeval_state(saved_state);
#if FD_TRACE_IPEVAL
  if (fd_trace_ipeval) {
    u8_notify(ipeval_exec,"IPEVAL iteration #%d completed in %lf",
	      ipeval_count,time_since(point));
    u8_notify(ipeval_done,
	      "IPEVAL finished with %d iterations in %lf seconds",
	      ipeval_count,time_since(start));}
#endif
#if FD_GLOBAL_IPEVAL
  fd_unlock_mutex(&global_ipeval_lock);
#endif
  return retval;
}

FD_EXPORT int fd_tracked_ipeval_call(int (*fcn)(void *),void *data,
				     struct FD_IPEVAL_RECORD **history,
				     int *n_cycles,double *total_time)
{
  int state, saved_state, ipeval_count=1, n_records=16, delays, retval=0;
  struct FD_IPEVAL_RECORD *records=u8_alloc_n(16,struct FD_IPEVAL_RECORD);
  double start, point, exec_time, fetch_time;
#if FD_GLOBAL_IPEVAL
  if (fd_ipeval_status()>0) 
    return fcn(data);
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
  records[ipeval_count-1].cycle=ipeval_count;
  records[ipeval_count-1].delays=delays;
  records[ipeval_count-1].exec_time=exec_time;
  records[ipeval_count-1].fetch_time=0.0;
  fd_set_ipeval_state(saved_state);
  *history=records; *n_cycles=ipeval_count; *total_time=time_since(start);
#if FD_TRACE_IPEVAL
  if (fd_trace_ipeval) {
    u8_notify(ipeval_exec,"IPEVAL iteration #%d completed in %lf",
	      ipeval_count,time_since(point));
    u8_notify(ipeval_done,
	      "IPEVAL finished with %d iterations in %lf seconds",
	      ipeval_count,time_since(start));}
#endif
#if FD_GLOBAL_IPEVAL
  fd_unlock_mutex(&global_ipeval_lock);
#endif
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
  vecstruct.data=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  cached=fd_hashtable_get(cache,vec,FD_VOID);
  if (FD_VOIDP(cached)) {
    int state=fd_ipeval_status();
    fdtype result=fd_dapply(fcn,n,args);
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

FD_EXPORT void fd_clear_callcache(fdtype arg)
{
  if (fcn_caches.n_keys==0) return;
  if (FD_VOIDP(arg)) fd_reset_hashtable(&fcn_caches,128,1);
  else if (fd_hashtable_probe(&fcn_caches,arg))
    fd_hashtable_store(&fcn_caches,arg,FD_VOID);
}

static int hashtable_cachecount(fdtype key,fdtype v,void *ptr)
{
  if (FD_HASHTABLEP(v)) {
    fd_hashtable h=(fd_hashtable)v;
    int *count=(int *)ptr;
    *count=*count+h->n_keys;}
  return 0;
}

FD_EXPORT int fd_callcache_load()
{
  int count=0;
  fd_for_hashtable(&fcn_caches,hashtable_cachecount,&count,1);
  return count;
}

/* Initialization stuff */

FD_EXPORT void fd_init_ipeval_c()
{
  fd_register_source_file(versionid);
  fd_register_config("TRACEIPEVAL",
		     fd_boolconfig_get,fd_boolconfig_set,&fd_trace_ipeval);
#if FD_GLOBAL_IPEVAL
  fd_init_mutex(&global_ipeval_lock);
#endif
#if ((FD_USE_TLS) && (!(FD_GLOBAL_IPEVAL)))
  u8_new_threadkey(&fd_ipeval_state_key,NULL);
#endif

  fd_make_hashtable(&fcn_caches,128);

}


/* The CVS log for this file
   $Log: ipeval.c,v $
   Revision 1.27  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.26  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.25  2005/12/23 16:58:07  haase
   Added config var for ipeval tracing

   Revision 1.24  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.23  2005/07/12 21:10:28  haase
   Cleaned up some ipeval trace statements

   Revision 1.22  2005/05/23 00:53:24  haase
   Fixes to header ordering to get off_t consistently defined

   Revision 1.21  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.20  2005/04/30 02:46:17  haase
   Made recursive ipeval possible

   Revision 1.19  2005/04/24 18:30:27  haase
   Made ipeval step function return -1 on error and abort

   Revision 1.18  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.17  2005/03/03 17:00:54  haase
   Remove stdio dependency in ipeval

   Revision 1.16  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
