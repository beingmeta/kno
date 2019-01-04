/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_IPEVAL 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/apply.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"

#include <libu8/libu8.h>
#include <libu8/u8filefns.h>
#include <libu8/u8timefns.h>

int fd_trace_ipeval = 0;

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
  u8_logf(LOG_DEBUG,c,"Starting %s#%d",c,iteration);
}

static void ipeval_done_msg(u8_condition c,int iteration,double interval)
{
  u8_logf(LOG_DEBUG,c,"Completed %s#%d in %lf seconds",c,iteration,interval);
}

#define time_since(x) (u8_elapsed_time()-(x))

/* Putting it all together */

FD_EXPORT int fd_ipeval_call(int (*fcn)(void *),void *data)
{
  int state, saved_state, ipeval_count = 1, retval = 0, gave_up = 0;
  /* This will be NULL if a thread cache is already in force. */
  struct FD_THREAD_CACHE *tc = fd_use_threadcache();
  double start, point;
#if FD_GLOBAL_IPEVAL
  if (fd_ipeval_status()>0) {
    retval = fcn(data);
    if (tc) fd_pop_threadcache(tc);
    return retval;}
  u8_lock_mutex(&global_ipeval_lock);
#endif
  saved_state = state = fd_ipeval_status();
  start = u8_elapsed_time(); point = start;
  if (state<1) {
    fd_init_pool_delays();
    fd_init_index_delays();}
  fd_set_ipeval_state(1);
  if (fd_trace_ipeval)
    ipeval_start_msg(ipeval_exec,ipeval_count);
  retval = fcn(data);
  state = fd_ipeval_status();
  while ((retval>=0) && (state>1)) {
    fd_set_ipeval_state(0);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval) {
      ipeval_done_msg(ipeval_exec,ipeval_count,time_since(point));
      ipeval_start_msg(ipeval_fetch,ipeval_count);}
#endif
    point = u8_elapsed_time();
    fd_for_pools(fd_execute_pool_delays,NULL);
    fd_for_indexes(fd_execute_index_delays,NULL);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval)
      ipeval_done_msg(ipeval_fetch,ipeval_count,time_since(point));
#endif
    ipeval_count++;
    fd_set_ipeval_state(1);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval) ipeval_start_msg(ipeval_exec,ipeval_count);
#endif
    point = u8_elapsed_time();
    retval = fcn(data);
    state = fd_ipeval_status();}
  if (gave_up) {
    fd_set_ipeval_state(0);
    retval = fcn(data);}
  fd_set_ipeval_state(saved_state);
#if FD_TRACE_IPEVAL
  if (fd_trace_ipeval) {
    u8_logf(LOG_DEBUG,ipeval_exec,"IPEVAL iteration #%d completed in %lf",
            ipeval_count,time_since(point));
    u8_logf(LOG_DEBUG,ipeval_done,
            "IPEVAL finished with %d iterations in %lf seconds",
            ipeval_count,time_since(start));}
#endif
#if FD_GLOBAL_IPEVAL
  u8_unlock_mutex(&global_ipeval_lock);
#endif
  if (tc) fd_pop_threadcache(tc);
  return retval;
}

FD_EXPORT int fd_tracked_ipeval_call(int (*fcn)(void *),void *data,
                                     struct FD_IPEVAL_RECORD **history,
                                     int *n_cycles,double *total_time)
{
  int state, saved_state, ipeval_count = 1, n_records = 16, delays, retval = 0, gave_up = 0;
  /* This will be NULL if a thread cache is already in force. */
  struct FD_THREAD_CACHE *tc = fd_use_threadcache();
  /* This keeps track of execution. */
  struct FD_IPEVAL_RECORD *records = u8_alloc_n(16,struct FD_IPEVAL_RECORD);
  double start, point, exec_time, fetch_time;
#if FD_GLOBAL_IPEVAL
  if (fd_ipeval_status()>0) {
    if (tc) fd_pop_threadcache(tc);
    return fcn(data);}
  u8_lock_mutex(&global_ipeval_lock);
#endif
  saved_state = state = fd_ipeval_status();
  start = u8_elapsed_time(); point = start;
  if (state<1) {
    fd_init_pool_delays();
    fd_init_index_delays();}
  fd_set_ipeval_state(1);
  retval = fcn(data);
  exec_time = time_since(point);
  delays = fd_ipeval_status()-1;
  state = fd_ipeval_status();
  while ((retval>=0) && (state>1)) {
    fd_set_ipeval_state(0);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval) {
      ipeval_done_msg(ipeval_exec,ipeval_count,exec_time);
      ipeval_start_msg(ipeval_fetch,ipeval_count);}
#endif
    point = u8_elapsed_time();
    fd_for_pools(fd_execute_pool_delays,NULL);
    fd_for_indexes(fd_execute_index_delays,NULL);
    fetch_time = time_since(point);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval)
      ipeval_done_msg(ipeval_fetch,ipeval_count,fetch_time);
#endif
    if (ipeval_count>=n_records) {
      records = u8_realloc_n(records,n_records+16,struct FD_IPEVAL_RECORD);
      n_records = n_records+16;}
    records[ipeval_count-1].cycle = ipeval_count;
    records[ipeval_count-1].delays = delays;
    records[ipeval_count-1].exec_time = exec_time;
    records[ipeval_count-1].fetch_time = fetch_time;
    ipeval_count++;
    fd_set_ipeval_state(1);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval) ipeval_start_msg(ipeval_exec,ipeval_count);
#endif
    point = u8_elapsed_time();
    retval = fcn(data);
    exec_time = time_since(point); delays = fd_ipeval_status()-1;
    state = fd_ipeval_status();}
  if (gave_up) {
    fd_set_ipeval_state(0);
    retval = fcn(data);
    exec_time = time_since(point); delays = 0;}
  records[ipeval_count-1].cycle = ipeval_count;
  records[ipeval_count-1].delays = delays;
  records[ipeval_count-1].exec_time = exec_time;
  records[ipeval_count-1].fetch_time = 0.0;
  fd_set_ipeval_state(saved_state);
  *history = records; *n_cycles = ipeval_count; *total_time = time_since(start);
#if FD_TRACE_IPEVAL
  if (fd_trace_ipeval) {
    u8_logf(LOG_INFO,ipeval_exec,"IPEVAL iteration #%d completed in %lf",
            ipeval_count,time_since(point));
    u8_logf(LOG_INFO,ipeval_done,
            "IPEVAL finished with %d iterations in %lf seconds",
            ipeval_count,time_since(start));}
#endif
#if FD_GLOBAL_IPEVAL
  u8_unlock_mutex(&global_ipeval_lock);
#endif
  if (tc) fd_pop_threadcache(tc);
  return retval;
}

/* Initialization stuff */

FD_EXPORT void fd_init_ipeval_c()
{
  u8_register_source_file(_FILEINFO);
  fd_register_config("TRACEIPEVAL",_("Trace ipeval execution"),
                     fd_boolconfig_get,fd_boolconfig_set,&fd_trace_ipeval);
#if FD_GLOBAL_IPEVAL
  u8_init_mutex(&global_ipeval_lock);
#endif
#if (FD_USE_TLS)
  u8_new_threadkey(&fd_ipeval_state_key,NULL);
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
