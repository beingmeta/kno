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
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/indices.h"

#include <libu8/libu8.h>
#include <libu8/filefns.h>
#include <libu8/timefns.h>

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
  u8_lock_mutex(&global_ipeval_lock);
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
  u8_unlock_mutex(&global_ipeval_lock);
#endif
  return retval;
}

FD_EXPORT int fd_tracked_ipeval_call(int (*fcn)(void *),void *data,
				     struct FD_IPEVAL_RECORD **history,
				     int *n_cycles,double *total_time)
{
  int state, saved_state, ipeval_count=1, n_records=16, delays, retval=0;
  struct FD_IPEVAL_RECORD *records=u8_malloc(16*sizeof(struct FD_IPEVAL_RECORD));
  double start, point, exec_time, fetch_time;
#if FD_GLOBAL_IPEVAL
  if (fd_ipeval_status()>0) 
    return fcn(data);
  u8_lock_mutex(&global_ipeval_lock);
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
      records=u8_realloc(records,sizeof(struct FD_IPEVAL_RECORD)*(n_records+16));
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
  u8_unlock_mutex(&global_ipeval_lock);
#endif
  return retval;
}

FD_EXPORT fd_init_ipeval_c()
{
  fd_register_source_file(versionid);
  fd_register_config("TRACEIPEVAL",
		     fd_boolconfig_get,fd_boolconfig_set,&fd_trace_ipeval);
#if FD_GLOBAL_IPEVAL
  u8_init_mutex(&global_ipeval_lock);
#endif
#if ((FD_USE_TLS) && (!(FD_GLOBAL_IPEVAL)))
  u8_new_threadkey(&fd_ipeval_state_key,NULL);
#endif
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
