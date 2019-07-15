/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_STORAGE_H
#define KNO_STORAGE_H 1
#ifndef KNO_STORAGE_H_INFO
#define KNO_STORAGE_H_INFO "include/kno/storage.h"
#endif

#ifndef KNO_STORAGE_DRIVER_BUFSIZE
#define KNO_STORAGE_DRIVER_BUFSIZE 262144
#endif

#include "streams.h"

KNO_EXPORT u8_condition kno_AmbiguousObjectName,
  kno_UnknownObjectName, kno_BadServerResponse, kno_NoBackground;
KNO_EXPORT u8_condition kno_NoStorageMetadata;
KNO_EXPORT u8_condition kno_ServerReconnect;
KNO_EXPORT u8_condition kno_Commitment;
KNO_EXPORT u8_condition kno_BadMetaData;
KNO_EXPORT u8_condition kno_ConnectionFailed;

KNO_EXPORT int kno_init_drivers(void) KNO_LIBINIT_FN;
KNO_EXPORT int kno_init_storage(void) KNO_LIBINIT_FN;
KNO_EXPORT int kno_init_dbserv(void) KNO_LIBINIT_FN;

KNO_EXPORT int kno_default_cache_level;
KNO_EXPORT int kno_oid_display_level;
KNO_EXPORT int kno_storage_loglevel;
KNO_EXPORT int *kno_storage_loglevel_ptr;
KNO_EXPORT int kno_prefetch;

KNO_EXPORT lispval kno_dbserv_module;

KNO_EXPORT int kno_swapcheck(void);

KNO_EXPORT u8_mutex kno_swapcheck_lock;

/*
   Kno database objects have a 32-bit wide index_flags field.  The
   first 12 bits are reserved for generic flags (any database), the
   next 8 bits are for flags applicable to any pool or index and the
   remaining 12 bits are for flags for particular implementations.
*/

typedef int kno_storage_flags;

#define KNO_STORAGE_ISPOOL                  0x001
#define KNO_STORAGE_ISINDEX                 0x002
#define KNO_STORAGE_READ_ONLY               0x004

#define KNO_STORAGE_UNREGISTERED            0x010
#define KNO_STORAGE_KEEP_CACHESIZE          0x020
#define KNO_STORAGE_NOSWAP                  0x040
#define KNO_STORAGE_NOERR                   0x080
#define KNO_STORAGE_PHASED                  0x100
#define KNO_STORAGE_REPAIR                  0x200
#define KNO_STORAGE_VIRTUAL                 0x400
#define KNO_STORAGE_LOUDSYMS                 0x800

#define KNO_STORAGE_MAX_INIT_BITS           0x1000
#define KNO_STORAGE_MAX_STATE_BITS          0x10000

typedef char fdb_cache_level;

#define KNO_POOL_FLAG(n)            ((0x10000)<<n)
#define KNO_INDEX_FLAG(n)           ((0x10000)<<n)

#ifndef KNO_DBSERV_MAX_POOLS
#define KNO_DBSERV_MAX_POOLS 128
#endif

#ifndef KNO_DBCONN_RESERVE_DEFAULT
#define KNO_DBCONN_RESERVE_DEFAULT 2
#endif

#ifndef KNO_DBCONN_CAP_DEFAULT
#define KNO_DBCONN_CAP_DEFAULT 6
#endif

#ifndef KNO_DBCONN_INIT_DEFAULT
#define KNO_DBCONN_INIT_DEFAULT 1
#endif

KNO_EXPORT int kno_dbconn_reserve_default;
KNO_EXPORT int kno_dbconn_cap_default;
KNO_EXPORT int kno_dbconn_init_default;

KNO_EXPORT kno_lisp_type kno_consed_index_type;
KNO_EXPORT kno_lisp_type kno_consed_pool_type;

#define KNO_ETERNAL_INDEXP(x) (KNO_TYPEP(x,kno_index_type))
#define KNO_CONSED_INDEXP(x) (KNO_TYPEP(x,kno_consed_index_type))
#define KNO_INDEXP(x) ( (KNO_ETERNAL_INDEXP(x)) || (KNO_CONSED_INDEXP(x)) )
#define KNO_INDEX_CONSEDP(ix) ( (ix) && ( ((ix)->index_serialno) < 0) )

#define KNO_ETERNAL_POOLP(x) (KNO_TYPEP(x,kno_pool_type))
#define KNO_CONSED_POOLP(x) (KNO_TYPEP(x,kno_consed_pool_type))
#define KNO_POOLP(x) ( (KNO_ETERNAL_POOLP(x)) || (KNO_CONSED_POOLP(x)) )
#define KNO_POOL_CONSEDP(p) ( (p) && ( ((p)->pool_serialno) < 0 ) )

KNO_EXPORT int kno_commit_all(void);
KNO_EXPORT void kno_swapout_all(void);
KNO_EXPORT void kno_fast_swapout_all(void);

/* Committing changes */

typedef enum KNO_COMMIT_PHASE {
  kno_no_commit = 0,
  kno_commit_start = 1,
  kno_commit_write = 2,
  kno_commit_sync = 3,
  kno_commit_rollback = 4,
  kno_commit_flush = 5,
  kno_commit_cleanup = 6,
  kno_commit_done = 7 } kno_commit_phase;

struct KNO_COMMIT_TIMES {
  double base, start, setup, write, sync, flush, cleanup; };

KNO_EXPORT lispval kno_commit_phases[8];

KNO_EXPORT kno_storage_flags kno_get_dbflags(lispval,kno_storage_flags);

/* IPEVAL stuff */

#if KNO_IPEVAL_ENABLED
#if (KNO_GLOBAL_IPEVAL)
KNO_EXPORT kno_wideint kno_ipeval_state;
#elif (KNO_USE__THREAD)
KNO_EXPORT __thread int kno_ipeval_state;
#elif (KNO_USE_TLS)
KNO_EXPORT u8_tld_key kno_ipeval_state_key;
#else
KNO_EXPORT int kno_ipeval_state;
#endif

KNO_EXPORT kno_wideint _kno_ipeval_delay(int n);
KNO_EXPORT kno_wideint _kno_ipeval_status(void);
KNO_EXPORT int _kno_ipeval_failp(void);
KNO_EXPORT void _kno_set_ipeval_state(kno_wideint s);

#ifndef KNO_INLINE_IPEVAL
#define KNO_INLINE_IPEVAL 1
#endif

#if (KNO_INLINE_IPEVAL)
#if ((KNO_USE_TLS) && (!(KNO_GLOBAL_IPEVAL)))
KNO_INLINE_FCN kno_wideint kno_ipeval_delay(int n)
{
  kno_wideint current = (kno_wideint) u8_tld_get(kno_ipeval_state_key);
  if (current<1) return 0;
  else {
    u8_tld_set(kno_ipeval_state_key,(void *)(current+n));
    return 1;}
}
KNO_INLINE_FCN kno_wideint kno_ipeval_status()
{
  return (kno_wideint) u8_tld_get(kno_ipeval_state_key);
}
KNO_INLINE_FCN int kno_ipeval_failp()
{
  return (((kno_wideint)(u8_tld_get(kno_ipeval_state_key)))>1);
}
KNO_INLINE_FCN void kno_set_ipeval_state(kno_wideint s)
{
  u8_tld_set(kno_ipeval_state_key,(void *)s);
}
#else
KNO_INLINE_FCN kno_wideint kno_ipeval_delay(int n)
{
  if (kno_ipeval_state<1) return 0;
  else {
    kno_ipeval_state = kno_ipeval_state+n;
    return 1;}
}
KNO_INLINE_FCN kno_wideint kno_ipeval_status()
{
  return kno_ipeval_state;
}
KNO_INLINE_FCN kno_wideint kno_ipeval_failp()
{
  return (kno_ipeval_state>1);
}
KNO_INLINE_FCN void kno_set_ipeval_state(kno_wideint s)
{
  kno_ipeval_state = s;
}
#endif
#else
#define kno_ipeval_delay(n) _kno_ipeval_delay(n)
#define kno_ipeval_status() _kno_ipeval_status()
#define kno_ipeval_failp() _kno_ipeval_failp()
#define kno_set_ipeval_state(s) _kno_set_ipeval_state(s)
#endif

typedef struct KNO_IPEVAL_RECORD {
  int ipv_cycle, ipv_delays;
  double ipv_exec_time, ipv_fetch_time;} KNO_IPEVAL_RECORD;
typedef struct KNO_IPEVAL_RECORD *kno_ipeval_record;

KNO_EXPORT int kno_tracked_ipeval_call
   (int (*fcn)(void *),void *data,
   struct KNO_IPEVAL_RECORD **history,
   int *n_cycles,double *total_time);
KNO_EXPORT int kno_ipeval_call(int (*fcn)(void *),void *data);

typedef int (*kno_ipevalfn)(void *);

KNO_EXPORT int kno_trace_ipeval;
KNO_EXPORT u8_mutex kno_ipeval_lock;

#else /* KNO_IPEVAL_ENABLED */
#define kno_trace_ipeval (0)
#define kno_ipeval_delay(n) (0)
#define kno_ipeval_status() (0)
#define kno_ipeval_failp() (0)
#define kno_set_ipeval_state(s)
#endif

/* Cache calls */

KNO_EXPORT long kno_callcache_load(void);
KNO_EXPORT void kno_clear_callcache(lispval arg);

KNO_EXPORT lispval kno_cachecall(lispval fcn,int n,lispval *args);
KNO_EXPORT lispval kno_xcachecall
  (struct KNO_HASHTABLE *cache,lispval fcn,int n,lispval *args);

KNO_EXPORT lispval kno_cachecall_try(lispval fcn,int n,lispval *args);
KNO_EXPORT lispval kno_xcachecall_try
  (struct KNO_HASHTABLE *cache,lispval fcn,int n,lispval *args);

KNO_EXPORT int kno_cachecall_probe(lispval fcn,int n,lispval *args);
KNO_EXPORT int kno_xcachecall_probe(kno_hashtable,lispval fcn,int n,lispval *args);

KNO_EXPORT lispval kno_tcachecall(lispval fcn,int n,lispval *args);

/* Thread caches */

#ifndef KNO_THREAD_CALLCACHE_SIZE
#define KNO_THREAD_CALLCACHE_SIZE 128
#endif
#ifndef KNO_THREAD_OIDCACHE_SIZE
#define KNO_THREAD_OIDCACHE_SIZE 128
#endif
#ifndef KNO_THREAD_BGCACHE_SIZE
#define KNO_THREAD_BGCACHE_SIZE 128
#endif
#ifndef KNO_THREAD_KEYCACHE_SIZE
#define KNO_THREAD_KEYCACHE_SIZE 128
#endif

typedef struct KNO_THREAD_CACHE {
  int threadcache_inuse; u8_string threadcache_id;
  struct KNO_HASHTABLE oids;
  struct KNO_HASHTABLE indexes;
  struct KNO_HASHTABLE bground;
  struct KNO_HASHTABLE calls;
  struct KNO_THREAD_CACHE *threadcache_prev;} KNO_THREAD_CACHE;
typedef struct KNO_THREAD_CACHE *kno_thread_cache;
typedef struct KNO_THREAD_CACHE KNOTC;

#if (KNO_USE__THREAD)
KNO_EXPORT __thread struct KNO_THREAD_CACHE *kno_threadcache;
#elif (KNO_USE_TLS)
KNO_EXPORT u8_tld_key kno_threadcache_key;
#define kno_threadcache ((struct KNO_THREAD_CACHE *)u8_tld_get(kno_threadcache_key))
#else
KNO_EXPORT struct KNO_THREAD_CACHE *kno_threadcache;
#endif

KNO_EXPORT int kno_free_thread_cache(struct KNO_THREAD_CACHE *tc);
KNO_EXPORT int kno_pop_threadcache(struct KNO_THREAD_CACHE *tc);

KNO_EXPORT kno_thread_cache kno_new_thread_cache(void);
KNO_EXPORT kno_thread_cache
  kno_cons_thread_cache(int ccsize,int ocsize,int bcsize,int kcsize);

KNO_EXPORT kno_thread_cache kno_push_threadcache(kno_thread_cache);
KNO_EXPORT kno_thread_cache kno_set_threadcache(kno_thread_cache);
KNO_EXPORT kno_thread_cache kno_use_threadcache(void);

/* Include other stuff */

#include "pools.h"
#include "indexes.h"
#include "frames.h"

KNO_EXPORT lispval (*kno_get_oid_name)(kno_pool,lispval);

KNO_EXPORT lispval kno_getpath(lispval start,int n,lispval *path,int infer,int accumulate);

KNO_EXPORT ssize_t kno_save_head(u8_string source,u8_string dest,size_t head_len);
KNO_EXPORT ssize_t kno_apply_head(u8_string head,u8_string file);
KNO_EXPORT ssize_t kno_restore_head(u8_string file,u8_string head);

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
