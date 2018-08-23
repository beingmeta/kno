/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_STORAGE_H
#define FRAMERD_STORAGE_H 1
#ifndef FRAMERD_STORAGE_H_INFO
#define FRAMERD_STORAGE_H_INFO "include/framerd/storage.h"
#endif

#ifndef FD_STORAGE_DRIVER_BUFSIZE
#define FD_STORAGE_DRIVER_BUFSIZE 262144
#endif

#include "streams.h"

FD_EXPORT u8_condition fd_AmbiguousObjectName,
  fd_UnknownObjectName, fd_BadServerResponse, fd_NoBackground;
FD_EXPORT u8_condition fd_NoStorageMetadata;
FD_EXPORT u8_condition fd_ServerReconnect;
FD_EXPORT u8_condition fd_Commitment;
FD_EXPORT u8_condition fd_BadMetaData;
FD_EXPORT u8_condition fd_ConnectionFailed;

FD_EXPORT int fd_init_drivers(void) FD_LIBINIT_FN;
FD_EXPORT int fd_init_storage(void) FD_LIBINIT_FN;
FD_EXPORT int fd_init_dbserv(void) FD_LIBINIT_FN;

FD_EXPORT int fd_default_cache_level;
FD_EXPORT int fd_oid_display_level;
FD_EXPORT int fd_storage_loglevel;
FD_EXPORT int *fd_storage_loglevel_ptr;
FD_EXPORT int fd_prefetch;

FD_EXPORT lispval fd_dbserv_module;

FD_EXPORT int fd_swapcheck(void);

FD_EXPORT u8_mutex fd_swapcheck_lock;

/*
   FramerD database objects have a 32-bit wide index_flags field.  The
   first 12 bits are reserved for generic flags (any database), the
   next 8 bits are for flags applicable to any pool or index and the
   remaining 12 bits are for flags for particular implementations.
*/

typedef int fd_storage_flags;

#define FD_STORAGE_ISPOOL                  0x001
#define FD_STORAGE_ISINDEX                 0x002
#define FD_STORAGE_READ_ONLY               0x004

#define FD_STORAGE_UNREGISTERED            0x010
#define FD_STORAGE_KEEP_CACHESIZE          0x020
#define FD_STORAGE_NOSWAP                  0x040
#define FD_STORAGE_NOERR                   0x080
#define FD_STORAGE_PHASED                  0x100
#define FD_STORAGE_REPAIR                  0x200
#define FD_STORAGE_VIRTUAL                 0x400

#define FD_STORAGE_MAX_INIT_BITS           0x800
#define FD_STORAGE_MAX_STATE_BITS          0x1000

typedef char fdb_cache_level;

#define FD_POOL_FLAG(n)            ((0x10000)<<n)
#define FD_INDEX_FLAG(n)           ((0x10000)<<n)

#ifndef FD_DBSERV_MAX_POOLS
#define FD_DBSERV_MAX_POOLS 128
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

FD_EXPORT int fd_dbconn_reserve_default;
FD_EXPORT int fd_dbconn_cap_default;
FD_EXPORT int fd_dbconn_init_default;

FD_EXPORT fd_ptr_type fd_consed_index_type;
FD_EXPORT fd_ptr_type fd_consed_pool_type;

#define FD_ETERNAL_INDEXP(x) (FD_TYPEP(x,fd_index_type))
#define FD_CONSED_INDEXP(x) (FD_TYPEP(x,fd_consed_index_type))
#define FD_INDEXP(x) ( (FD_ETERNAL_INDEXP(x)) || (FD_CONSED_INDEXP(x)) )
#define FD_INDEX_CONSEDP(ix) ( (ix) && ( ((ix)->index_serialno) < 0) )

#define FD_ETERNAL_POOLP(x) (FD_TYPEP(x,fd_pool_type))
#define FD_CONSED_POOLP(x) (FD_TYPEP(x,fd_consed_pool_type))
#define FD_POOLP(x) ( (FD_ETERNAL_POOLP(x)) || (FD_CONSED_POOLP(x)) )
#define FD_POOL_CONSEDP(p) ( (p) && ( ((p)->pool_serialno) < 0 ) )

FD_EXPORT int fd_commit_all(void);
FD_EXPORT void fd_swapout_all(void);
FD_EXPORT void fd_fast_swapout_all(void);

/* Committing changes */

typedef enum FD_COMMIT_PHASE {
  fd_no_commit = 0,
  fd_commit_start = 1,
  fd_commit_write = 2,
  fd_commit_sync = 3,
  fd_commit_rollback = 4,
  fd_commit_flush = 5,
  fd_commit_cleanup = 6,
  fd_commit_done = 7 } fd_commit_phase;

struct FD_COMMIT_TIMES {
  double base, start, setup, write, sync, flush, cleanup; };

FD_EXPORT lispval fd_commit_phases[8];

FD_EXPORT fd_storage_flags fd_get_dbflags(lispval,fd_storage_flags);

/* IPEVAL stuff */

#if FD_IPEVAL_ENABLED
#if (FD_GLOBAL_IPEVAL)
FD_EXPORT fd_wideint fd_ipeval_state;
#elif (FD_USE__THREAD)
FD_EXPORT __thread int fd_ipeval_state;
#elif (FD_USE_TLS)
FD_EXPORT u8_tld_key fd_ipeval_state_key;
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
  fd_wideint current = (fd_wideint) u8_tld_get(fd_ipeval_state_key);
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
    fd_ipeval_state = fd_ipeval_state+n;
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
  fd_ipeval_state = s;
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
FD_EXPORT u8_mutex fd_ipeval_lock;

#else /* FD_IPEVAL_ENABLED */
#define fd_trace_ipeval (0)
#define fd_ipeval_delay(n) (0)
#define fd_ipeval_status() (0)
#define fd_ipeval_failp() (0)
#define fd_set_ipeval_state(s)
#endif

/* Cache calls */

FD_EXPORT long fd_callcache_load(void);
FD_EXPORT void fd_clear_callcache(lispval arg);

FD_EXPORT lispval fd_cachecall(lispval fcn,int n,lispval *args);
FD_EXPORT lispval fd_xcachecall
  (struct FD_HASHTABLE *cache,lispval fcn,int n,lispval *args);

FD_EXPORT lispval fd_cachecall_try(lispval fcn,int n,lispval *args);
FD_EXPORT lispval fd_xcachecall_try
  (struct FD_HASHTABLE *cache,lispval fcn,int n,lispval *args);

FD_EXPORT int fd_cachecall_probe(lispval fcn,int n,lispval *args);
FD_EXPORT int fd_xcachecall_probe(fd_hashtable,lispval fcn,int n,lispval *args);

FD_EXPORT lispval fd_tcachecall(lispval fcn,int n,lispval *args);

/* Thread caches */

#ifndef FD_THREAD_CALLCACHE_SIZE
#define FD_THREAD_CALLCACHE_SIZE 128
#endif
#ifndef FD_THREAD_OIDCACHE_SIZE
#define FD_THREAD_OIDCACHE_SIZE 128
#endif
#ifndef FD_THREAD_BGCACHE_SIZE
#define FD_THREAD_BGCACHE_SIZE 128
#endif
#ifndef FD_THREAD_KEYCACHE_SIZE
#define FD_THREAD_KEYCACHE_SIZE 128
#endif

typedef struct FD_THREAD_CACHE {
  int threadcache_inuse; u8_string threadcache_id;
  struct FD_HASHTABLE oids;
  struct FD_HASHTABLE indexes;
  struct FD_HASHTABLE bground;
  struct FD_HASHTABLE calls;
  struct FD_THREAD_CACHE *threadcache_prev;} FD_THREAD_CACHE;
typedef struct FD_THREAD_CACHE *fd_thread_cache;
typedef struct FD_THREAD_CACHE FDTC;

#if (FD_USE__THREAD)
FD_EXPORT __thread struct FD_THREAD_CACHE *fd_threadcache;
#elif (FD_USE_TLS)
FD_EXPORT u8_tld_key fd_threadcache_key;
#define fd_threadcache ((struct FD_THREAD_CACHE *)u8_tld_get(fd_threadcache_key))
#else
FD_EXPORT struct FD_THREAD_CACHE *fd_threadcache;
#endif

FD_EXPORT int fd_free_thread_cache(struct FD_THREAD_CACHE *tc);
FD_EXPORT int fd_pop_threadcache(struct FD_THREAD_CACHE *tc);

FD_EXPORT fd_thread_cache fd_new_thread_cache(void);
FD_EXPORT fd_thread_cache
  fd_cons_thread_cache(int ccsize,int ocsize,int bcsize,int kcsize);

FD_EXPORT fd_thread_cache fd_push_threadcache(fd_thread_cache);
FD_EXPORT fd_thread_cache fd_set_threadcache(fd_thread_cache);
FD_EXPORT fd_thread_cache fd_use_threadcache(void);

/* Include other stuff */

#include "pools.h"
#include "indexes.h"
#include "frames.h"

FD_EXPORT lispval (*fd_get_oid_name)(fd_pool,lispval);

FD_EXPORT lispval fd_getpath(lispval start,int n,lispval *path,int infer,int accumulate);

FD_EXPORT ssize_t fd_save_head(u8_string source,u8_string dest,size_t head_len);
FD_EXPORT ssize_t fd_apply_head(u8_string head,u8_string file);
FD_EXPORT ssize_t fd_restore_head(u8_string file,u8_string head);

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
