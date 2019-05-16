/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_IPEVAL 1
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/tables.h"
#include "kno/apply.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"

#include <libu8/libu8.h>
#include <libu8/u8filefns.h>
#include <libu8/u8timefns.h>

int kno_trace_ipeval = 0;

#if (KNO_GLOBAL_IPEVAL)
static u8_mutex global_ipeval_lock;
int kno_ipeval_state;
#elif (KNO_USE__THREAD)
__thread int kno_ipeval_state;
#elif (KNO_USE_TLS)
u8_tld_key kno_ipeval_state_key;
#else
int kno_ipeval_state;
#endif

static u8_condition ipeval_fetch="FETCH",
  ipeval_exec="EXEC", ipeval_done="IPEDONE";

KNO_EXPORT kno_wideint _kno_ipeval_delay(int n)
{
  return kno_ipeval_delay(n);
}
KNO_EXPORT kno_wideint _kno_ipeval_status()
{
  return kno_ipeval_status();
}
KNO_EXPORT int _kno_ipeval_failp()
{
  return (kno_ipeval_status()>1);
}
KNO_EXPORT void _kno_set_ipeval_state(kno_wideint s)
{
  kno_set_ipeval_state(s);
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

KNO_EXPORT int kno_ipeval_call(int (*fcn)(void *),void *data)
{
  int state, saved_state, ipeval_count = 1, retval = 0, gave_up = 0;
  /* This will be NULL if a thread cache is already in force. */
  struct KNO_THREAD_CACHE *tc = kno_use_threadcache();
  double start, point;
#if KNO_GLOBAL_IPEVAL
  if (kno_ipeval_status()>0) {
    retval = fcn(data);
    if (tc) kno_pop_threadcache(tc);
    return retval;}
  u8_lock_mutex(&global_ipeval_lock);
#endif
  saved_state = state = kno_ipeval_status();
  start = u8_elapsed_time(); point = start;
  if (state<1) {
    kno_init_pool_delays();
    kno_init_index_delays();}
  kno_set_ipeval_state(1);
  if (kno_trace_ipeval)
    ipeval_start_msg(ipeval_exec,ipeval_count);
  retval = fcn(data);
  state = kno_ipeval_status();
  while ((retval>=0) && (state>1)) {
    kno_set_ipeval_state(0);
#if KNO_TRACE_IPEVAL
    if (kno_trace_ipeval) {
      ipeval_done_msg(ipeval_exec,ipeval_count,time_since(point));
      ipeval_start_msg(ipeval_fetch,ipeval_count);}
#endif
    point = u8_elapsed_time();
    kno_for_pools(kno_execute_pool_delays,NULL);
    kno_for_indexes(kno_execute_index_delays,NULL);
#if KNO_TRACE_IPEVAL
    if (kno_trace_ipeval)
      ipeval_done_msg(ipeval_fetch,ipeval_count,time_since(point));
#endif
    ipeval_count++;
    kno_set_ipeval_state(1);
#if KNO_TRACE_IPEVAL
    if (kno_trace_ipeval) ipeval_start_msg(ipeval_exec,ipeval_count);
#endif
    point = u8_elapsed_time();
    retval = fcn(data);
    state = kno_ipeval_status();}
  if (gave_up) {
    kno_set_ipeval_state(0);
    retval = fcn(data);}
  kno_set_ipeval_state(saved_state);
#if KNO_TRACE_IPEVAL
  if (kno_trace_ipeval) {
    u8_logf(LOG_DEBUG,ipeval_exec,"IPEVAL iteration #%d completed in %lf",
            ipeval_count,time_since(point));
    u8_logf(LOG_DEBUG,ipeval_done,
            "IPEVAL finished with %d iterations in %lf seconds",
            ipeval_count,time_since(start));}
#endif
#if KNO_GLOBAL_IPEVAL
  u8_unlock_mutex(&global_ipeval_lock);
#endif
  if (tc) kno_pop_threadcache(tc);
  return retval;
}

KNO_EXPORT int kno_tracked_ipeval_call(int (*fcn)(void *),void *data,
                                     struct KNO_IPEVAL_RECORD **history,
                                     int *n_cycles,double *total_time)
{
  int state, saved_state, ipeval_count = 1, n_records = 16, delays, retval = 0, gave_up = 0;
  /* This will be NULL if a thread cache is already in force. */
  struct KNO_THREAD_CACHE *tc = kno_use_threadcache();
  /* This keeps track of execution. */
  struct KNO_IPEVAL_RECORD *records = u8_alloc_n(16,struct KNO_IPEVAL_RECORD);
  double start, point, exec_time, fetch_time;
#if KNO_GLOBAL_IPEVAL
  if (kno_ipeval_status()>0) {
    if (tc) kno_pop_threadcache(tc);
    return fcn(data);}
  u8_lock_mutex(&global_ipeval_lock);
#endif
  saved_state = state = kno_ipeval_status();
  start = u8_elapsed_time(); point = start;
  if (state<1) {
    kno_init_pool_delays();
    kno_init_index_delays();}
  kno_set_ipeval_state(1);
  retval = fcn(data);
  exec_time = time_since(point);
  delays = kno_ipeval_status()-1;
  state = kno_ipeval_status();
  while ((retval>=0) && (state>1)) {
    kno_set_ipeval_state(0);
#if KNO_TRACE_IPEVAL
    if (kno_trace_ipeval) {
      ipeval_done_msg(ipeval_exec,ipeval_count,exec_time);
      ipeval_start_msg(ipeval_fetch,ipeval_count);}
#endif
    point = u8_elapsed_time();
    kno_for_pools(kno_execute_pool_delays,NULL);
    kno_for_indexes(kno_execute_index_delays,NULL);
    fetch_time = time_since(point);
#if KNO_TRACE_IPEVAL
    if (kno_trace_ipeval)
      ipeval_done_msg(ipeval_fetch,ipeval_count,fetch_time);
#endif
    if (ipeval_count>=n_records) {
      records = u8_realloc_n(records,n_records+16,struct KNO_IPEVAL_RECORD);
      n_records = n_records+16;}
    records[ipeval_count-1].cycle = ipeval_count;
    records[ipeval_count-1].delays = delays;
    records[ipeval_count-1].exec_time = exec_time;
    records[ipeval_count-1].fetch_time = fetch_time;
    ipeval_count++;
    kno_set_ipeval_state(1);
#if KNO_TRACE_IPEVAL
    if (kno_trace_ipeval) ipeval_start_msg(ipeval_exec,ipeval_count);
#endif
    point = u8_elapsed_time();
    retval = fcn(data);
    exec_time = time_since(point); delays = kno_ipeval_status()-1;
    state = kno_ipeval_status();}
  if (gave_up) {
    kno_set_ipeval_state(0);
    retval = fcn(data);
    exec_time = time_since(point); delays = 0;}
  records[ipeval_count-1].cycle = ipeval_count;
  records[ipeval_count-1].delays = delays;
  records[ipeval_count-1].exec_time = exec_time;
  records[ipeval_count-1].fetch_time = 0.0;
  kno_set_ipeval_state(saved_state);
  *history = records; *n_cycles = ipeval_count; *total_time = time_since(start);
#if KNO_TRACE_IPEVAL
  if (kno_trace_ipeval) {
    u8_logf(LOG_INFO,ipeval_exec,"IPEVAL iteration #%d completed in %lf",
            ipeval_count,time_since(point));
    u8_logf(LOG_INFO,ipeval_done,
            "IPEVAL finished with %d iterations in %lf seconds",
            ipeval_count,time_since(start));}
#endif
#if KNO_GLOBAL_IPEVAL
  u8_unlock_mutex(&global_ipeval_lock);
#endif
  if (tc) kno_pop_threadcache(tc);
  return retval;
}

/* Initialization stuff */

KNO_EXPORT void kno_init_ipeval_c()
{
  u8_register_source_file(_FILEINFO);
  kno_register_config("TRACEIPEVAL",_("Trace ipeval execution"),
                     kno_boolconfig_get,kno_boolconfig_set,&kno_trace_ipeval);
#if KNO_GLOBAL_IPEVAL
  u8_init_mutex(&global_ipeval_lock);
#endif
#if (KNO_USE_TLS)
  u8_new_threadkey(&kno_ipeval_state_key,NULL);
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
