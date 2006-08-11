/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_FDDB_H
#define FDB_FDDB_H 1
#define FDB_FDDB_H_VERSION "$Id$"

#include "dtypestream.h"

FD_EXPORT fd_exception fd_InternalError, fd_AmbiguousObjectName,
  fd_UnknownObjectName, fd_BadServerResponse, fd_NoBackground;
FD_EXPORT u8_condition fd_Commitment;
FD_EXPORT fd_exception fd_BadMetaData;

FD_EXPORT int fd_init_db(void) FD_LIBINIT_FN;

FD_EXPORT fdtype (*fd_get_oid_name)(fdtype);
FD_EXPORT int fd_default_cache_level;
FD_EXPORT int fd_oid_display_level;
FD_EXPORT int fd_prefetch;
FD_EXPORT fd_exception fd_InternalError;
FD_EXPORT fd_exception fd_BadServerResponse;

fdtype fd_fdbserv_module;

#define FD_EXPLICIT_SETCACHE 2
#define FD_STICKY_CACHESIZE 4

#ifndef FDBSERV_MAX_POOLS
#define FDBSERV_MAX_POOLS 128
#endif

#ifndef FD_NET_BUFSIZE
#define FD_NET_BUFSIZE 64*1024
#endif

FD_EXPORT fd_ptr_type fd_index_type, fd_pool_type;

#define FD_INDEXP(x) (FD_PRIM_TYPEP(x,fd_index_type))
#define FD_POOLP(x) (FD_PRIM_TYPEP(x,fd_pool_type))

FD_EXPORT int fd_commit_all(void);
FD_EXPORT void fd_swapout_all(void);
FD_EXPORT void fd_fast_swapout_all(void);

/* IPEVAL stuff */

#if (FD_GLOBAL_IPEVAL)
FD_EXPORT int fd_ipeval_state;
#elif (FD_USE_TLS)
FD_EXPORT u8_tld_key fd_ipeval_state_key;
#elif (FD_USE__THREAD)
FD_EXPORT __thread int fd_ipeval_state;
#else
FD_EXPORT int fd_ipeval_state;
#endif

FD_EXPORT int _fd_ipeval_delay(int n);
FD_EXPORT int _fd_ipeval_status(void);
FD_EXPORT int _fd_ipeval_failp(void);
FD_EXPORT void _fd_set_ipeval_state(int s);

#ifndef FD_INLINE_IPEVAL
#define FD_INLINE_IPEVAL 1
#endif

#if (FD_INLINE_IPEVAL)
#if ((FD_USE_TLS) && (!(FD_GLOBAL_IPEVAL)))
static int fd_ipeval_delay(int n)
{
  int current= (int) u8_tld_get(fd_ipeval_state_key);
  if (current<1) return 0;
  else {
    u8_tld_set(fd_ipeval_state_key,(void *)(current+n));
    return 1;}
}
static int fd_ipeval_status()
{
  return (int) u8_tld_get(fd_ipeval_state_key);
}
static int fd_ipeval_failp()
{
  return (((int)(u8_tld_get(fd_ipeval_state_key)))>1);
}
static void fd_set_ipeval_state(int s)
{
  u8_tld_set(fd_ipeval_state_key,(void *)s);
}
#else
static int fd_ipeval_delay(int n)
{
  if (fd_ipeval_state<1) return 0;
  else {
    fd_ipeval_state=fd_ipeval_state+n;
    return 1;}
}
static int fd_ipeval_status()
{
  return fd_ipeval_state;
}
static int fd_ipeval_failp()
{
  return (fd_ipeval_state>1);
}
static void fd_set_ipeval_state(int s)
{
  fd_ipeval_state=s;
}
#endif
#else
#define fd_ipeval_delay(n) _fd_ipeval_delay(n)
#define fd_ipeval_status() _fd_ipeval_status()
#define fd_ipeval_failp() _fd_ipeval_failp()
#define fd_set_ipeval_state(s) _fd_set_ipeval_state(s)
#endif

typedef struct FD_IPEVAL_RECORD {
  int cycle, delays;
  double exec_time, fetch_time;} FD_IPEVAL_RECORD;
typedef struct FD_IPEVAL_RECORD *fd_ipeval_record;

FD_EXPORT int fd_tracked_ipeval_call
   (int (*fcn)(void *),void *data,
   struct FD_IPEVAL_RECORD **history,
   int *n_cycles,double *total_time);
FD_EXPORT int fd_ipeval_call(int (*fcn)(void *),void *data);

typedef int (*fd_ipevalfn)(void *);

FD_EXPORT int fd_trace_ipeval;
#if FD_THREADS_ENABLED
FD_EXPORT u8_mutex fd_ipeval_lock;
#endif

#endif

