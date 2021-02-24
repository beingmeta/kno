/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_POOLS_H
#define KNO_POOLS_H 1
#ifndef KNO_POOLS_H_INFO
#define KNO_POOLS_H_INFO "include/kno/pools.h"
#endif

/* HOW POOLS WORK

   Pools provide a way to organize the allocation and storage of object
   references (OIDs).  A pool is a continguous range of OIDs.  The range,
   referred to as the *capacity* of the pool, is typically a power of 2,
   which simplifies certain kinds of processing.  Pools are generally
   allocated out of a particular *superpool*, which refers to a pool
   consisting of 4,294,967,296 (2^32) OIDs and aligned on 32 bit OID
   boundaries.  For simplicity, individual pools are defined to never
   cross superpool boundaries.

   Many implementations of OIDs make an additional distinction of
   distinguished *buckets*.  Buckets are pools of a set size which
   simplify OID lookup and are generally aligned on binary boundaries.
   In all the current implementations which use buckets, a bucket is a
   range of 1,048,576 (2^20) OIDs which may contain or be contained in
   other pools.

   The most common usage of the term pool is to describe a "source pool":
   a range of OIDs whose values is provided by a particular resource,
   such as a network server or disk file.  A source pool has a *base*
   (the first OID in the pool), a *load* (the number of OIDs currently
   allocated from the pool), and a *capacity* (as above, the total number
   of possible OIDs in the pool).

   As mentioned above, many implementations work by identifying *buckets*
   within the OID address space.  In Kno, for instance, there are 1024
   buckets, each with a capacity of 1,048,576 (2^20) OIDs.  A fixed
   vector of data structures maps each bucket into either a source pool
   or a "glue pool" which contains pointers to smaller source pools
   sorted to be searchable by a binary tree algorithm.  However, when
   pools are sized to fit bucket boundaries (e.g. multiples of 2^20
   aligned at 20-bit boundaries), going from OID to pool takes linear
   time.

   Generically, a pool is characterized by its base and capacity, a
   consecutively allocated serial number, a cache level, and a set of
   flags.  It is also characterized by four strings:
    * label: an intentially unique external label assigned to the pool,
       such as brico.framerd.org or xbrico.beingmeta.com
    * source: a string identifying the name of the resource providing
       the pool's data; e.g. a filename such as "brico.pool" or a server
       identification such as "bground@gemini"
    * cid: a canonical version of the source used to find identical
       sources; e.g. /disk1/data/kno/bg/brico.pool"
    * prefix: the string used to identify OIDs in the pool using
       the string prefix syntax, e.g. @/brico/3b94 indicating
       the OID @1/3b94 when the 'brico' pool's based is @1/0.

   In Kno, all pools have three additional tables associated with them:
    * cache: stores values fetched for particular OIDs.
    * locks: stores values being modified for particular OIDs and locked in
       the pool's source.
    * adjuncts: stores map particular frame slotids into external
       tables; this allows some slots (in the frame database) to be stored
       independent of their OID values.  This can improve performance
       in some cases and also allows description to be *politically*
       distributed.

   Finally, each pool has a methods pointer (the ->handler field), which
   provides methods for operating on the pool.  All of these methods take
   a pool (the pool) as their first arguments.  If a method is NULL, the
   pool does not support the method.  The core methods are:
    * close(kno_pool p) returns void
       Closes the pool, freeing as much memory as it can.
    * setcache(kno_pool,int level) returns void
       Sets a caching level for the pool.  0 means no caching (every
       request goes to network or disk), 1 means use the local cache, and
       higher numbers mean different things for different kinds of pools.
    * setbuf(kno_pool,int size) returns void
       Sets the buffer size used into file or network transactions (if any).
    * alloc(kno_pool p,int n) returns lispval
       Returns a lisp pointer to N OIDs allocated from the pool
    * fetch(kno_pool p,lispval oid) returns lispval
       Returns the value of the OID oid (a lisp pointer).
       This returns an error if the OID isn't in the pool.
    * fetchn(kno_pool p,int n,lispval *oids) return (lispval *)
       Returns the values of the OIDs in the n-element array oids.  OIDs
       not in the pool are given values of KNO_VOID.
    * getload(kno_pool p) returns int
       Returns the number of OIDs currently allocated in pool.
    * lock(kno_pool p,lispval oids)
       Locks OIDs in pool, where OIDs can be an individual OID,
       a choice of OIDs, or a vector of OIDs.
       Returns 1 on success, 0 on failure
    * unlock(kno_pool p,lispval oids) returns int
       Unlocks OIDs in pool, where OIDs can be an individual OID,
       a choice of OIDs, or a vector of OIDs.  Note that modified
       OIDs lose their modifications when unlocked.
    * storen(kno_pool p,int n,lispval *oids,lispval *values) int
       Assigns a set of values, in the pool's source, for the given
       OIDs and values.
    * metadata(kno_pool p,lispval arg) returns lispval
       if arg is KNO_VOID, this gets the metadata associated with the pool,
       which will be a lisp pointer to a table; otherwise, it sets the
       metadata, which should be a lisp pointer to a table
    * sync(kno_pool p) returns int
       synchronizes the pool cache with the resource it gets its values from.
*/

#ifndef KNO_INLINE_POOLS
#define KNO_INLINE_POOLS 0
#endif

#ifndef KNO_MAX_POOLS
#define KNO_MAX_POOLS (KNO_N_OID_BUCKETS*4)
#endif

KNO_EXPORT u8_condition kno_AnonymousOID, kno_UnallocatedOID, kno_HomelessOID;
KNO_EXPORT u8_condition kno_InvalidPoolPtr, kno_PoolRangeError, kno_CantLockOID,
  kno_PoolConflict, kno_PoolOverflow, kno_InvalidIndexPtr,
  kno_NotAPool, kno_UnknownPoolType, kno_CorrputedPool,
  kno_ReadOnlyPool, kno_ExhaustedPool, kno_PoolCommitError, kno_NoSuchPool,
  kno_NotAFilePool, kno_NoFilePools, kno_BadFilePoolLabel, kno_DataFileOverflow;

KNO_EXPORT u8_condition kno_PoolCommit;

#define KNO_POOL_CACHE_INIT 123
#define KNO_POOL_CHANGES_INIT 73
KNO_EXPORT int kno_pool_cache_init;
KNO_EXPORT int kno_pool_lock_init;

#define KNO_POOL_SPARSE    (KNO_POOL_FLAG(0))
#define KNO_POOL_ADJUNCT   (KNO_POOL_FLAG(1))
#define KNO_POOL_VIRTUAL   (KNO_POOL_FLAG(2))
#define KNO_POOL_NOLOCKS   (KNO_POOL_FLAG(3))
#define KNO_POOL_PREALLOC  (KNO_POOL_FLAG(4))

typedef enum kno_storage_unlock_flags {
  commit_modified = 1,
  leave_modified = 0,
  discard_modified = -1 } kno_storage_unlock_flag;

typedef struct KNO_ADJUNCT {
  struct KNO_POOL *pool; lispval slotid; lispval table;} KNO_ADJUNCT;
typedef struct KNO_ADJUNCT *kno_adjunct;

#define KNO_POOL_FIELDS					 \
  KNO_CONS_HEADER;                                       \
  int pool_serialno;					 \
  u8_string poolid, pool_label;				 \
  u8_string pool_source, canonical_source;		 \
  u8_string pool_typeid;				 \
  KNO_OID pool_base;                                     \
  unsigned int pool_capacity;				 \
  kno_storage_flags pool_flags, modified_flags;          \
  struct KNO_POOL_HANDLER *pool_handler;                 \
  lispval pool_adjunct;					 \
  short pool_cache_level;				 \
  short pool_loglevel;					 \
  U8_RWLOCK_DECL(pool_struct_lock);			 \
  U8_MUTEX_DECL(pool_commit_lock);			 \
  struct KNO_HASHTABLE pool_cache, pool_changes;         \
  struct KNO_SLOTMAP pool_metadata, pool_props;          \
  int pool_n_adjuncts, pool_adjuncts_len;		 \
  struct KNO_ADJUNCT *pool_adjuncts;                     \
  lispval pool_indexes;					 \
  u8_string pool_prefix;				 \
  lispval pool_namefn;					 \
  lispval pool_opts

typedef struct KNO_POOL {KNO_POOL_FIELDS;} KNO_POOL;
typedef struct KNO_POOL *kno_pool;

/* This is an index of all pools by the base IDs they contain. */
KNO_EXPORT kno_pool kno_top_pools[KNO_N_OID_BUCKETS];
/* This is an index of all pools by their serial numbers (if registered) */
KNO_EXPORT kno_pool kno_pools_by_serialno[KNO_MAX_POOLS];
KNO_EXPORT int kno_n_pools, kno_max_pools;

KNO_EXPORT kno_pool kno_default_pool;
KNO_EXPORT struct KNO_POOL _kno_zero_pool;
#define kno_zero_pool (&(_kno_zero_pool))

KNO_EXPORT lispval kno_zero_pool_value(lispval oid);
KNO_EXPORT lispval kno_zero_pool_store(lispval oid,lispval);
KNO_EXPORT lispval kno_zero_pool_init(lispval oid,lispval);

KNO_EXPORT int kno_register_pool(kno_pool p);
KNO_EXPORT lispval kno_all_pools(void);

/* Locking functions */

KNO_EXPORT void _kno_lock_pool_struct(kno_pool p,int for_write);
KNO_EXPORT void _kno_unlock_pool_struct(kno_pool p);

#if KNO_INLINE_POOLS
KNO_FASTOP void kno_lock_pool_struct(kno_pool p,int for_write)
{
  if (for_write)
    u8_write_lock(&((p)->pool_struct_lock));
  else u8_read_lock(&((p)->pool_struct_lock));
}
KNO_FASTOP void kno_unlock_pool_struct(kno_pool p)
{
  u8_rw_unlock(&((p)->pool_struct_lock));
}
#else
#define kno_lock_pool_struct(p,for_write) _kno_lock_pool_struct(p,for_write)
#define kno_unlock_pool_struct(p) _kno_unlock_pool_struct(p)
#endif

/* Pool commit objects */

typedef struct KNO_POOL_COMMITS {
  kno_pool commit_pool;
  kno_commit_phase commit_phase;
  unsigned int commit_2phase:1;
  ssize_t commit_count;
  lispval *commit_oids;
  lispval *commit_vals;
  lispval commit_metadata;
  struct KNO_COMMIT_TIMES commit_times;
  struct KNO_STREAM *commit_stream;} *kno_pool_commits;

/* Pool handlers */

typedef struct KNO_POOL_HANDLER {
  u8_string name; int version, length, n_handlers;
  void (*close)(kno_pool p);
  lispval (*alloc)(kno_pool p,int n);
  lispval (*fetch)(kno_pool p,lispval oid);
  lispval *(*fetchn)(kno_pool p,int n,lispval *oids);
  int (*getload)(kno_pool p);
  int (*lock)(kno_pool p,lispval oids);
  int (*unlock)(kno_pool p,lispval oids);
  int (*commit)(kno_pool p,kno_commit_phase phase,struct KNO_POOL_COMMITS *);
  int (*swapout)(kno_pool p,lispval oids);
  kno_pool (*create)(u8_string spec,void *typedata,
                    kno_storage_flags flags,lispval opts);
  int (*walker)(kno_pool,kno_walker,void *,kno_walk_flags,int);
  void (*recycle)(kno_pool p);
  lispval (*poolctl)(kno_pool p,lispval op,int n,kno_argvec args);}
  KNO_POOL_HANDLER;
typedef struct KNO_POOL_HANDLER *kno_pool_handler;

#if 0
struct KNO_POOL_HANDLER some_handler={
   "any", 1, sizeof(poolstruct), 9,
   NULL, /* close */
   NULL, /* alloc */
   NULL, /* fetch */
   NULL, /* fetchn */
   NULL, /* lock */
   NULL, /* release */
   NULL, /* storen */
   NULL, /* create */
   NULL, /* walk */
   NULL, /* recycle */
   NULL  /* pool op */
};
#endif

KNO_EXPORT lispval kno_pool_ctl(kno_pool p,lispval op,int n,kno_argvec args);

KNO_EXPORT lispval kno_default_poolctl(kno_pool p,lispval op,int n,kno_argvec args);
KNO_EXPORT lispval kno_pool_base_metadata(kno_pool p);

KNO_EXPORT void kno_init_pool(kno_pool p,KNO_OID base,unsigned int capacity,
                            struct KNO_POOL_HANDLER *h,
                            u8_string id,u8_string source,u8_string csource,
                            kno_storage_flags flags,
                            lispval metadata,
                            lispval opts);
KNO_EXPORT int kno_pool_set_metadata(kno_pool p,lispval metadata);
KNO_EXPORT void kno_set_pool_namefn(kno_pool p,lispval namefn);

KNO_EXPORT int kno_for_pools(int (*fcn)(kno_pool,void *),void *data);
KNO_EXPORT lispval kno_find_pools_by_source(u8_string id);

KNO_EXPORT kno_pool kno_find_pool_by_id(u8_string id);
KNO_EXPORT kno_pool kno_find_pool_by_source(u8_string source);
KNO_EXPORT kno_pool kno_find_pool_by_prefix(u8_string prefix);

KNO_EXPORT kno_pool kno_find_pool(u8_string);
KNO_EXPORT u8_string kno_locate_pool(u8_string);

/* Pools and lisp pointers */

KNO_EXPORT lispval kno_pool2lisp(kno_pool p);
KNO_EXPORT kno_pool kno_lisp2pool(lispval lp);
KNO_EXPORT kno_pool kno_open_pool(u8_string spec,kno_storage_flags flags,lispval opts);
KNO_EXPORT kno_pool kno_get_pool(u8_string spec,kno_storage_flags flags,lispval opts);
KNO_EXPORT kno_pool kno_use_pool(u8_string spec,kno_storage_flags flags,lispval opts);
KNO_EXPORT kno_pool kno_name2pool(u8_string spec);

KNO_EXPORT lispval kno_poolconfig_get(lispval var,void *vptr);
KNO_EXPORT int kno_poolconfig_set(lispval ignored,lispval v,void *vptr);

/* GLUEPOOLS */

typedef struct KNO_GLUEPOOL {
  KNO_POOL_FIELDS;
  int n_subpools; struct KNO_POOL **subpools;} KNO_GLUEPOOL;
typedef struct KNO_GLUEPOOL *kno_gluepool;

KNO_EXPORT kno_pool kno_find_subpool(struct KNO_GLUEPOOL *gp,lispval oid);

KNO_EXPORT kno_pool _kno_oid2pool(lispval oid);
KNO_EXPORT lispval kno_oid_value(lispval oid);

/* Using pools like tables */

KNO_EXPORT lispval kno_pool_getkey(kno_pool p,lispval key);
KNO_EXPORT int kno_pool_store(kno_pool p,lispval key,lispval value);
KNO_EXPORT lispval kno_pool_keys(lispval arg);

/* IPEVAL delays */

#ifndef KNO_N_POOL_DELAYS
#define KNO_N_POOL_DELAYS 1024
#endif

KNO_EXPORT lispval *kno_get_pool_delays(void);

#if (KNO_GLOBAL_IPEVAL)
KNO_EXPORT lispval *kno_pool_delays;
#elif (KNO_USE__THREAD)
KNO_EXPORT __thread lispval *kno_pool_delays;
#elif (KNO_USE_TLS)
KNO_EXPORT u8_tld_key kno_pool_delays_key;
#define kno_pool_delays ((lispval *)u8_tld_get(kno_pool_delays_key))
#else
KNO_EXPORT lispval *kno_pool_delays;
#endif

KNO_EXPORT void kno_init_pool_delays(void);
KNO_EXPORT lispval *kno_get_pool_delays(void);
KNO_EXPORT int kno_execute_pool_delays(kno_pool p,void *data);

/* OID Access */

KNO_EXPORT lispval kno_pool_get(kno_pool p,lispval oid);
KNO_EXPORT lispval kno_pool_fetch(kno_pool p,lispval oid);
KNO_EXPORT lispval kno_pool_alloc(kno_pool p,int n);
KNO_EXPORT int kno_pool_prefetch(kno_pool p,lispval oids);
KNO_EXPORT int kno_prefetch_oids(lispval oids);
KNO_EXPORT int kno_finish_oids(lispval oids);
KNO_EXPORT int kno_lock_oids(lispval oids);
KNO_EXPORT int kno_lock_oid(lispval oid);
KNO_EXPORT int kno_unlock_oids(lispval oids,int commit);
KNO_EXPORT int kno_swapout_oid(lispval oid);
KNO_EXPORT int kno_swapout_oids(lispval oids);
KNO_EXPORT int kno_pool_lock(kno_pool p,lispval oids);
KNO_EXPORT int kno_pool_unlock(kno_pool p,lispval oids,int commit);
KNO_EXPORT int kno_commit_pool(kno_pool p,lispval oids);
KNO_EXPORT int kno_pool_finish(kno_pool p,lispval oids);
KNO_EXPORT void kno_pool_setcache(kno_pool p,int level);
KNO_EXPORT int kno_pool_storen(kno_pool p,int n,lispval *oids,lispval *values);
KNO_EXPORT lispval kno_pool_fetchn(kno_pool p,lispval oids);
KNO_EXPORT void kno_pool_close(kno_pool p);
KNO_EXPORT int kno_pool_swapout(kno_pool p,lispval oids);
KNO_EXPORT u8_string kno_pool_label(kno_pool p);
KNO_EXPORT u8_string kno_pool_id(kno_pool p);

KNO_EXPORT lispval kno_pool_value(kno_pool p,lispval oid);

KNO_EXPORT kno_pool _kno_poolptr(lispval x);

KNO_EXPORT int kno_pool_unlock_all(kno_pool p,kno_storage_unlock_flag flags);
KNO_EXPORT int kno_commit_all_oids(kno_pool p);

KNO_EXPORT int kno_pool_load(kno_pool p);

KNO_EXPORT void kno_reset_pool_tables(kno_pool p,ssize_t cacheval,ssize_t locksval);

KNO_EXPORT int kno_set_oid_value(lispval oid,lispval value);
KNO_EXPORT lispval kno_init_oid_value(lispval oid,lispval value);
KNO_EXPORT lispval kno_locked_oid_value(kno_pool p,lispval oid);

KNO_EXPORT int kno_swapout_pools(void);
KNO_EXPORT int kno_close_pools(void);
KNO_EXPORT int kno_commit_pools(void);
KNO_EXPORT int kno_commit_pools_noerr(void);
KNO_EXPORT int kno_unlock_pools(int);
KNO_EXPORT long kno_object_cache_load(void);
KNO_EXPORT lispval kno_cached_oids(kno_pool p);
KNO_EXPORT lispval kno_changed_oids(kno_pool p);

KNO_EXPORT int kno_commit_oids(lispval oids);
KNO_EXPORT int kno_init_pool_commits
(kno_pool p,lispval oids,struct KNO_POOL_COMMITS *commits);

#if KNO_INLINE_POOLS
KNO_FASTOP kno_pool kno_oid2pool(lispval oid)
{
  int baseid = KNO_OID_BASE_ID(oid);
  int baseoff = KNO_OID_BASE_OFFSET(oid);
  struct KNO_POOL *top = kno_top_pools[baseid];
  if (top == NULL) return top;
  else if (baseoff<top->pool_capacity) return top;
  else if (top->pool_capacity) {
    u8_raise(_("Corrupted pool table"),"kno_oid2pool",NULL);
    return NULL;}
  else return kno_find_subpool((struct KNO_GLUEPOOL *)top,oid);
}
KNO_FASTOP U8_MAYBE_UNUSED kno_pool kno_poolptr(lispval x)
{
  if (KNO_TYPEP(x,kno_poolref_type)) {
    int serial = KNO_GET_IMMEDIATE(x,kno_poolref_type);
    if (serial<kno_n_pools)
      return kno_pools_by_serialno[serial];
    else return NULL;}
  else if (KNO_TYPEP(x,kno_consed_pool_type))
    return (kno_pool)x;
  else return NULL;
}
#else
#define kno_oid2pool _kno_oid2pool
#define kno_poolptr _kno_poolptr
#endif

/* Adjuncts */

/* Adjuncts are lisp tables (including indices or even other pools)
   which are used for the values of particular slotids, either on a
   pool or globally. */

KNO_EXPORT u8_condition kno_BadAdjunct, kno_AdjunctError;

KNO_EXPORT int kno_set_adjuncts(kno_pool p,lispval adjuncts);
KNO_EXPORT int kno_set_adjunct(kno_pool p,lispval slotid,lispval table);
KNO_EXPORT kno_adjunct kno_get_adjunct(kno_pool p,lispval slotid);
KNO_EXPORT kno_adjunct kno_oid_adjunct(lispval oid,lispval slotid);
KNO_EXPORT int kno_adjunctp(kno_pool p,lispval slotid);

KNO_EXPORT lispval kno_get_adjuncts(kno_pool p);

KNO_EXPORT lispval kno_adjunct_slotids;

/* Generic Pools */

typedef struct KNO_GPOOL {
  KNO_POOL_FIELDS;
  lispval fetchfn, newfn, loadfn, savefn;} KNO_GPOOL;
typedef struct KNO_GPOOL *kno_gpool;

/* Proc pools */

struct KNO_PROCPOOL_METHODS {
  lispval openfn, allocfn, getloadfn,
    fetchfn, fetchnfn, swapoutfn,
    lockfn, releasefn,
    commitfn, metadatafn,
    createfn, closefn, ctlfn;};

typedef struct KNO_PROCPOOL {
  KNO_POOL_FIELDS;
  struct KNO_PROCPOOL_METHODS *pool_methods;
  lispval pool_state;}
  KNO_PROCPOOL;
typedef struct KNO_PROCPOOL *kno_procpool;

KNO_EXPORT
kno_pool kno_make_procpool(KNO_OID base,
                         unsigned int cap,unsigned int load,
                         lispval opts,lispval state,
                         u8_string label,
                         u8_string source);
KNO_EXPORT void kno_register_procpool(u8_string typename,lispval handler);

KNO_EXPORT struct KNO_POOL_HANDLER kno_procpool_handler;

/* External Pools */

typedef struct KNO_EXTPOOL {
  KNO_POOL_FIELDS;
  lispval fetchfn, savefn;
  lispval lockfn, allocfn;
  lispval state;} KNO_EXTPOOL;
typedef struct KNO_EXTPOOL *kno_extpool;

KNO_EXPORT
kno_pool kno_make_extpool
  (u8_string label,KNO_OID base,unsigned int cap,
   lispval fetchfn,lispval savefn,
   lispval lockfn,lispval allocfn,
   lispval state,
   lispval opts);
KNO_EXPORT int kno_extpool_cache_value(kno_pool p,lispval oid,lispval value);

KNO_EXPORT struct KNO_POOL_HANDLER kno_extpool_handler;

/* Memory Pools (only in memory, no fetch/commit) */

typedef struct KNO_MEMPOOL {
  KNO_POOL_FIELDS;
  unsigned int pool_load;
  unsigned int noswap;} KNO_MEMPOOL;
typedef struct KNO_MEMPOOL *kno_mempool;

KNO_EXPORT kno_pool kno_make_mempool
  (u8_string label,KNO_OID base,unsigned int cap,
   unsigned int load,unsigned int noswap,
   lispval opts);
KNO_EXPORT kno_pool kno_get_mempool(u8_string label);

/* Removes deadwood */
KNO_EXPORT int kno_clean_mempool(kno_pool p);

/* Clears all the locks, caches, and load for a mempool. */
KNO_EXPORT int kno_reset_mempool(kno_pool p);

#endif /* KNO_POOLS_H */

