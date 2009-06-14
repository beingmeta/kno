/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_FDDB_H
#define FDB_FDDB_H 1
#define FDB_FDDB_H_VERSION "$Id$"

#include "dtypestream.h"

FD_EXPORT fd_exception fd_InternalError, fd_AmbiguousObjectName,
  fd_UnknownObjectName, fd_BadServerResponse, fd_NoBackground;
FD_EXPORT u8_condition fd_ServerReconnect;
FD_EXPORT u8_condition fd_Commitment;
FD_EXPORT fd_exception fd_BadMetaData;
FD_EXPORT fd_exception fd_ConnectionFailed;

FD_EXPORT int fd_init_db(void) FD_LIBINIT_FN;
FD_EXPORT void fd_init_fddbserv(void) FD_LIBINIT_FN;

FD_EXPORT fdtype (*fd_get_oid_name)(fdtype);
FD_EXPORT int fd_default_cache_level;
FD_EXPORT int fd_oid_display_level;
FD_EXPORT int fddb_loglevel;
FD_EXPORT int fd_prefetch;
FD_EXPORT fd_exception fd_InternalError;
FD_EXPORT fd_exception fd_BadServerResponse;

FD_EXPORT fdtype fd_fdbserv_module;

FD_EXPORT int fd_swapcheck(void);

#if FD_THREADS_ENABLED
FD_EXPORT u8_mutex fd_swapcheck_lock;
#endif

#define FD_EXPLICIT_SETCACHE 2
#define FD_STICKY_CACHESIZE 4

#ifndef FDBSERV_MAX_POOLS
#define FDBSERV_MAX_POOLS 128
#endif

#ifndef FD_NET_BUFSIZE
#define FD_NET_BUFSIZE 64*1024
#endif

#ifndef FD_DBCONN_RESERVE_DEFAULT
#define FD_DBCONN_RESERVE_DEFAULT 2
#endif

#ifndef FD_DBCONN_CAP_DEFAULT
#define FD_DBCONN_CAP_DEFAULT 6
#endif

#ifndef FD_DBCONN_INIT_DEFAULT
#define FD_DBCONN_INIT_DEFAULT 1
#endif

FD_EXPORT int fd_dbconn_reserve_default, fd_dbconn_cap_default, fd_dbconn_init_default;

FD_EXPORT fd_ptr_type fd_index_type, fd_pool_type, fd_raw_pool_type;

#define FD_INDEXP(x) (FD_PTR_TYPEP(x,fd_index_type))
#define FD_POOLP(x) (FD_PTR_TYPEP(x,fd_pool_type))

FD_EXPORT int fd_commit_all(void);
FD_EXPORT void fd_swapout_all(void);
FD_EXPORT void fd_fast_swapout_all(void);

/* IPEVAL stuff */

#if (FD_GLOBAL_IPEVAL)
FD_EXPORT fd_wideint fd_ipeval_state;
#elif (FD_USE_TLS)
FD_EXPORT u8_tld_key fd_ipeval_state_key;
#elif (FD_USE__THREAD)
FD_EXPORT __thread int fd_ipeval_state;
#else
FD_EXPORT int fd_ipeval_state;
#endif

FD_EXPORT fd_wideint _fd_ipeval_delay(int n);
FD_EXPORT fd_wideint _fd_ipeval_status(void);
FD_EXPORT int _fd_ipeval_failp(void);
FD_EXPORT void _fd_set_ipeval_state(fd_wideint s);

#ifndef FD_INLINE_IPEVAL
#define FD_INLINE_IPEVAL 1
#endif

#if (FD_INLINE_IPEVAL)
#if ((FD_USE_TLS) && (!(FD_GLOBAL_IPEVAL)))
FD_INLINE_FCN fd_wideint fd_ipeval_delay(int n)
{
  fd_wideint current= (fd_wideint) u8_tld_get(fd_ipeval_state_key);
  if (current<1) return 0;
  else {
    u8_tld_set(fd_ipeval_state_key,(void *)(current+n));
    return 1;}
}
FD_INLINE_FCN fd_wideint fd_ipeval_status()
{
  return (fd_wideint) u8_tld_get(fd_ipeval_state_key);
}
FD_INLINE_FCN int fd_ipeval_failp()
{
  return (((fd_wideint)(u8_tld_get(fd_ipeval_state_key)))>1);
}
FD_INLINE_FCN void fd_set_ipeval_state(fd_wideint s)
{
  u8_tld_set(fd_ipeval_state_key,(void *)s);
}
#else
FD_INLINE_FCN fd_wideint fd_ipeval_delay(int n) 
{
  if (fd_ipeval_state<1) return 0;
  else {
    fd_ipeval_state=fd_ipeval_state+n;
    return 1;}
}
FD_INLINE_FCN fd_wideint fd_ipeval_status()
{
  return fd_ipeval_state;
}
FD_INLINE_FCN fd_wideint fd_ipeval_failp()
{
  return (fd_ipeval_state>1);
}
FD_INLINE_FCN void fd_set_ipeval_state(fd_wideint s)
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

/* Cache calls */

FD_EXPORT long fd_callcache_load(void);
FD_EXPORT void fd_clear_callcache(fdtype arg);

FD_EXPORT fdtype fd_cachecall(fdtype fcn,int n,fdtype *args);
FD_EXPORT fdtype fd_xcachecall
  (struct FD_HASHTABLE *cache,fdtype fcn,int n,fdtype *args);

FD_EXPORT fdtype fd_cachecall_try(fdtype fcn,int n,fdtype *args);
FD_EXPORT fdtype fd_xcachecall_try
  (struct FD_HASHTABLE *cache,fdtype fcn,int n,fdtype *args);

FD_EXPORT int fd_cachecall_probe(fdtype fcn,int n,fdtype *args);
FD_EXPORT int fd_xcachecall_probe(fd_hashtable,fdtype fcn,int n,fdtype *args);

FD_EXPORT fdtype fd_tcachecall(fdtype fcn,int n,fdtype *args);

/* Thread caches */

#ifndef FD_THREAD_CALLCACHE_SIZE
#define FD_THREAD_CALLCACHE_SIZE 128
#endif
#ifndef FD_THREAD_OIDCACHE_SIZE
#define FD_THREAD_OIDCACHE_SIZE 0
#endif
#ifndef FD_THREAD_BGCACHE_SIZE
#define FD_THREAD_BGCACHE_SIZE 0
#endif
#ifndef FD_THREAD_KEYCACHE_SIZE
#define FD_THREAD_KEYCACHE_SIZE 0
#endif

typedef struct FD_THREAD_CACHE {
  struct FD_HASHTABLE fdtc_calls;
  struct FD_HASHTABLE fdtc_oids;
  struct FD_HASHTABLE fdtc_bground;
  struct FD_HASHTABLE fdtc_keys;
  struct FD_THREAD_CACHE *fdtc_prev;} FD_THREAD_CACHE;
typedef struct FD_THREAD_CACHE *fd_thread_cache;

#if (FD_USE_TLS)
FD_EXPORT u8_tld_key fd_threadcache_key;
#define fd_threadcache ((struct FD_THREAD_CACHE *)u8_tld_get(fd_threadcache_key))
#elif (FD_USE__THREAD)
FD_EXPORT __thread struct FD_THREAD_CACHE *fd_threadcache;
#else
FD_EXPORT struct FD_THREAD_CACHE *fd_threadcache;
#endif

FD_EXPORT int fd_free_thread_cache(struct FD_THREAD_CACHE *tc);
FD_EXPORT int fd_pop_threadcache(struct FD_THREAD_CACHE *tc);

FD_EXPORT fd_thread_cache fd_new_thread_cache(void);
FD_EXPORT fd_thread_cache
  fd_cons_thread_cache(int ccsize,int ocsize,int bcsize,int kcsize);

FD_EXPORT fd_thread_cache fd_push_threadcache(fd_thread_cache);
FD_EXPORT fd_thread_cache fd_use_threadcache(void);

/* Include other stuff */

#include "pools.h"
#include "indices.h"
#include "frames.h"


#endif

