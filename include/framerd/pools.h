/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_POOLS_H
#define FRAMERD_POOLS_H 1
#ifndef FRAMERD_POOLS_H_INFO
#define FRAMERD_POOLS_H_INFO "include/framerd/pools.h"
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
   within the OID address space.  In FramerD, for instance, there are 1024
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
       sources; e.g. /disk1/data/framerd/bg/brico.pool"
    * prefix: the string used to identify OIDs in the pool using
       the string prefix syntax, e.g. @/brico/3b94 indicating
       the OID @1/3b94 when the 'brico' pool's based is @1/0.

   In FramerD, all pools have three additional tables associated with them:
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
    * close(fd_pool p) returns void
       Closes the pool, freeing as much memory as it can.
    * setcache(fd_pool,int level) returns void
       Sets a caching level for the pool.  0 means no caching (every
       request goes to network or disk), 1 means use the local cache, and
       higher numbers mean different things for different kinds of pools.
    * setbuf(fd_pool,int size) returns void
       Sets the buffer size used into file or network transactions (if any).
    * alloc(fd_pool p,int n) returns fdtype
       Returns a dtype pointer to N OIDs allocated from the pool
    * fetch(fd_pool p,fdtype oid) returns fdtype
       Returns the value of the OID oid (a dtype pointer).
       This returns an error if the OID isn't in the pool.
    * fetchn(fd_pool p,int n,fdtype *oids) return (fdtype *)
       Returns the values of the OIDs in the n-element array oids.  OIDs
       not in the pool are given values of FD_VOID.
    * getload(fd_pool p) returns int
       Returns the number of OIDs currently allocated in pool.
    * lock(fd_pool p,fdtype oids)
       Locks OIDs in pool, where OIDs can be an individual OID,
       a choice of OIDs, or a vector of OIDs.
       Returns 1 on success, 0 on failure
    * unlock(fd_pool p,fdtype oids) returns int
       Unlocks OIDs in pool, where OIDs can be an individual OID,
       a choice of OIDs, or a vector of OIDs.  Note that modified
       OIDs lose their modifications when unlocked.
    * storen(fd_pool p,int n,fdtype *oids,fdtype *values) int
       Assigns a set of values, in the pool's source, for the given
       OIDs and values.
    * metadata(fd_pool p,fdtype arg) returns fdtype
       if arg is FD_VOID, this gets the metadata associated with the pool,
       which will be a dtype pointer to a table; otherwise, it sets the
       metadata, which should be a dtype pointer to a table
    * sync(fd_pool p) returns int
       synchronizes the pool cache with the resource it gets its values from.
*/

#include "defines.h"
#include "fddb.h"

FD_EXPORT fd_exception
fd_CantLockOID, fd_InvalidPoolPtr, fd_PoolRangeError,
  fd_NotAFilePool, fd_AnonymousOID, fd_UnallocatedOID,
  fd_NoFilePools, fd_NotAPool, fd_UnknownPool, fd_CorrputedPool,
  fd_BadFilePoolLabel, fd_ReadOnlyPool, fd_ExhaustedPool,
  fd_PoolCommitError, fd_UnresolvedPool, fd_DataFileOverflow;

FD_EXPORT u8_condition fd_PoolCommit;

#define FD_POOL_CACHE_INIT 123
#define FD_POOL_CHANGES_INIT 73
FD_EXPORT int fd_pool_cache_init;
FD_EXPORT int fd_pool_lock_init;

#define FD_OIDHOLES_OKAY  (FDB_POOL_FLAG(1))

FD_EXPORT int fd_ignore_anonymous_oids;

typedef unsigned int fd_pool_commit_flags;
#define FD_POOL_COMMIT_UNLOCK 1
#define FD_POOL_COMMIT_FINISHED 2
#define FD_POOL_COMMIT_UNMODIFIED 4

#define FD_POOL_COMMIT_FINAL (FD_POOL_COMMIT_UNLOCK)

typedef struct FD_ADJUNCT {
  struct FD_POOL *pool; fdtype slotid; fdtype table;} FD_ADJUNCT;
typedef struct FD_ADJUNCT *fd_adjunct;

#define FD_POOL_FIELDS \
  FD_CONS_HEADER;					\
  FD_OID pool_base;					\
  unsigned int pool_capacity, pool_read_only;		\
  int pool_serialno, pool_cache_level, pool_flags;	\
  u8_string pool_label, pool_prefix;			\
  u8_string pool_source, pool_cid, pool_xid;		\
  int pool_n_adjuncts, pool_max_adjuncts;		\
  struct FD_ADJUNCT *pool_adjuncts;			\
  struct FD_HASHTABLE *oid_handlers;			\
  struct FD_POOL_HANDLER *pool_handler;			\
  fdtype pool_namefn;					\
  struct FD_HASHTABLE pool_cache, pool_changes;		\
  int pool_n_locked

typedef struct FD_POOL {FD_POOL_FIELDS;} FD_POOL;
typedef struct FD_POOL *fd_pool;
FD_EXPORT struct FD_POOL *fd_top_pools[];

FD_EXPORT int fd_n_pools;
FD_EXPORT fd_pool fd_default_pool;
FD_EXPORT int fd_register_pool(fd_pool p);

FD_EXPORT fdtype fd_all_pools(void);

FD_EXPORT fd_pool *fd_pool_serial_table;
FD_EXPORT int fd_pool_serial_count;

/* Pool handlers */

struct FD_POOL_HANDLER {
  u8_string name; int version, length, n_handlers;
  void (*close)(fd_pool p);
  void (*setcache)(fd_pool p,int level);
  void (*setbuf)(fd_pool p,int size);
  fdtype (*alloc)(fd_pool p,int n);
  fdtype (*fetch)(fd_pool p,fdtype oid);
  fdtype *(*fetchn)(fd_pool p,int n,fdtype *oids);
  int (*getload)(fd_pool p);
  int (*lock)(fd_pool p,fdtype oids);
  int (*unlock)(fd_pool p,fdtype oids);
  int (*storen)(fd_pool p,int n,fdtype *oids,fdtype *vals);
  int (*swapout)(fd_pool p,fdtype oids);
  fdtype (*metadata)(fd_pool p,fdtype);
  int (*sync)(fd_pool p);};

#if 0
struct FD_POOL_HANDLER some_handler={
   "any", 1, sizeof(poolstruct), 9,
   NULL, /* close */
   NULL, /* setcache */
   NULL, /* setbuf */
   NULL, /* alloc */
   NULL, /* fetch */
   NULL, /* fetchn */
   NULL, /* lock */
   NULL, /* release */
   NULL, /* storen */
   NULL, /* metadata */
   NULL}; /* sync */
#endif

FD_EXPORT void fd_init_pool(fd_pool p,FD_OID base,unsigned int capacity,
                            struct FD_POOL_HANDLER *h,
                            u8_string source,u8_string cid);
FD_EXPORT void fd_set_pool_namefn(fd_pool p,fdtype namefn);

FD_EXPORT int fd_for_pools(int (*fcn)(fd_pool,void *),void *data);
FD_EXPORT fdtype fd_find_pools_by_cid(u8_string cid);
FD_EXPORT fd_pool fd_find_pool_by_cid(u8_string cid);
FD_EXPORT fd_pool fd_find_pool_by_prefix(u8_string prefix);


/* Pools and dtype pointers */

FD_EXPORT fdtype fd_pool2lisp(fd_pool p);
FD_EXPORT fd_pool fd_lisp2pool(fdtype lp);
FD_EXPORT fd_pool fd_open_pool(u8_string spec);
FD_EXPORT fd_pool fd_use_pool(u8_string spec);
FD_EXPORT fd_pool fd_name2pool(u8_string spec);

FD_EXPORT fdtype fd_poolconfig_get(fdtype var,void *vptr);
FD_EXPORT int fd_poolconfig_set(fdtype ignored,fdtype v,void *vptr);

/* GLUEPOOLS */

typedef struct FD_GLUEPOOL {
  FD_POOL_FIELDS;
  int n_subpools; struct FD_POOL **subpools;} FD_GLUEPOOL;
typedef struct FD_GLUEPOOL *fd_gluepool;

FD_EXPORT fd_pool fd_find_subpool(struct FD_GLUEPOOL *gp,fdtype oid);

FD_EXPORT fd_pool _fd_oid2pool(fdtype oid);
FD_EXPORT fdtype _fd_oid_value(fdtype oid);
FD_EXPORT fdtype _fd_fetch_oid(fd_pool p,fdtype oid);

/* IPEVAL delays */

#ifndef FD_N_POOL_DELAYS
#define FD_N_POOL_DELAYS 1024
#endif

FD_EXPORT fdtype *fd_get_pool_delays(void);

#if (FD_GLOBAL_IPEVAL)
FD_EXPORT fdtype *fd_pool_delays;
#elif (FD_USE__THREAD)
FD_EXPORT __thread fdtype *fd_pool_delays;
#elif (FD_USE_TLS)
FD_EXPORT u8_tld_key fd_pool_delays_key;
#define fd_pool_delays ((fdtype *)u8_tld_get(fd_pool_delays_key))
#else
FD_EXPORT fdtype *fd_pool_delays;
#endif

FD_EXPORT void fd_init_pool_delays(void);
FD_EXPORT fdtype *fd_get_pool_delays(void);
FD_EXPORT int fd_execute_pool_delays(fd_pool p,void *data);

/* OID Access */

FD_EXPORT fdtype fd_pool_fetch(fd_pool p,fdtype oid);
FD_EXPORT fdtype fd_pool_alloc(fd_pool p,int n);
FD_EXPORT int fd_pool_prefetch(fd_pool p,fdtype oids);
FD_EXPORT int fd_prefetch_oids(fdtype oids);
FD_EXPORT int fd_lock_oids(fdtype oids);
FD_EXPORT int fd_lock_oid(fdtype oid);
FD_EXPORT int fd_unlock_oids(fdtype oids,int commit);
FD_EXPORT int fd_unlock_oid(fdtype oid,int commit);
FD_EXPORT int fd_swapout_oid(fdtype oid);
FD_EXPORT int fd_swapout_oids(fdtype oids);
FD_EXPORT int fd_finish_oids(fdtype oids,fd_pool poolarg);
FD_EXPORT int fd_pool_lock(fd_pool p,fdtype oids);
FD_EXPORT int fd_pool_unlock(fd_pool p,fdtype oids,int commit);
FD_EXPORT int fd_pool_commit(fd_pool p,fdtype oids,fd_pool_commit_flags flags);
FD_EXPORT void fd_pool_setcache(fd_pool p,int level);
FD_EXPORT void fd_pool_close(fd_pool p);
FD_EXPORT void fd_pool_swapout(fd_pool p);
FD_EXPORT u8_string fd_pool_label(fd_pool p);
FD_EXPORT u8_string fd_pool_id(fd_pool p);

FD_EXPORT fd_pool _fd_get_poolptr(fdtype x);

FD_EXPORT int fd_pool_unlock_all(fd_pool p,int commit);
FD_EXPORT int fd_pool_commit_all(fd_pool p,int unlock);

FD_EXPORT int fd_pool_load(fd_pool p);

FD_EXPORT void fd_reset_pool_tables(fd_pool p,ssize_t cacheval,ssize_t locksval);

FD_EXPORT int fd_set_oid_value(fdtype oid,fdtype value);
FD_EXPORT fdtype fd_locked_oid_value(fd_pool p,fdtype oid);

FD_EXPORT int fd_swapout_pools(void);
FD_EXPORT int fd_close_pools(void);
FD_EXPORT int fd_commit_pools(void);
FD_EXPORT int fd_commit_pools_noerr(void);
FD_EXPORT int fd_unlock_pools(int);
FD_EXPORT long fd_object_cache_load(void);
FD_EXPORT fdtype fd_cached_oids(fd_pool p);

FD_EXPORT int fd_commit_oids(fdtype oids,fd_pool_commit_flags flags);

#if FD_INLINE_POOLS
FD_FASTOP fd_pool fd_oid2pool(fdtype oid)
{
  int baseid=FD_OID_BASE_ID(oid);
  int baseoff=FD_OID_BASE_OFFSET(oid);
  struct FD_POOL *top=fd_top_pools[baseid];
  if (top==NULL) return top;
  else if (baseoff<top->pool_capacity) return top;
  else if (top->pool_capacity) {
    u8_raise(_("Corrupted pool table"),"fd_oid2pool",NULL);
    return NULL;}
  else return fd_find_subpool((struct FD_GLUEPOOL *)top,oid);
}
FD_FASTOP fdtype fd_fetch_oid(fd_pool p,fdtype oid)
{
  FDTC *fdtc=((FD_USE_THREADCACHE)?(fd_threadcache):(NULL));
  fdtype value;
  if (p==NULL) p=fd_oid2pool(oid);
  if (p==NULL) {
    if (fd_ignore_anonymous_oids) return FD_EMPTY_CHOICE;
    else return fd_err(fd_AnonymousOID,NULL,NULL,oid);}
  if (fdtc) {
    fdtype value=((fdtc->oids.fd_n_keys)?
                  (fd_hashtable_get(&(fdtc->oids),oid,FD_VOID)):
                  (FD_VOID));
    if (!(FD_VOIDP(value))) return value;}
  if (p->pool_n_locked)
    if (fd_hashtable_probe_novoid(&(p->pool_changes),oid)) {
      value=fd_hashtable_get(&(p->pool_changes),oid,FD_VOID);
      if (value == FD_LOCKHOLDER) {
        value=fd_pool_fetch(p,oid);
        fd_hashtable_store(&(p->pool_changes),oid,value);
        return value;}
      else return value;}
  if (p->pool_cache_level)
    value=fd_hashtable_get(&(p->pool_cache),oid,FD_VOID);
  else value=FD_VOID;
  if (FD_VOIDP(value)) {
    if (fd_ipeval_delay(1)) {
      FD_ADD_TO_CHOICE(fd_pool_delays[p->pool_serialno],oid);
      return FD_EMPTY_CHOICE;}
    else value=fd_pool_fetch(p,oid);}
  if (FD_ABORTP(value)) return value;
  if (fdtc) fd_hashtable_store(&(fdtc->oids),oid,value);
  return value;
}
FD_FASTOP fdtype fd_oid_value(fdtype oid)
{
  if (FD_EMPTY_CHOICEP(oid)) return oid;
  else if (FD_OIDP(oid)) return fd_fetch_oid(NULL,oid);
  else return fd_type_error(_("OID"),"fd_oid_value",oid);
}
FD_FASTOP U8_MAYBE_UNUSED fd_pool fd_get_poolptr(fdtype x)
{
  int serial=FD_GET_IMMEDIATE(x,fd_pool_type);
  if (serial<fd_pool_serial_count)
    return fd_pool_serial_table[serial];
  else return NULL;
}
#else
#define fd_fetch_oid _fd_fetch_oid
#define fd_oid_value _fd_oid_value
#define fd_oid2pool _fd_oid2pool
#define fd_get_poolptr _fd_get_poolptr
#endif

FD_EXPORT fdtype fd_anonymous_oid(const u8_string cxt,fdtype oid);

/* Generic Pools */

typedef struct FD_GPOOL {
  FD_POOL_FIELDS;
  fdtype fetchfn, newfn, loadfn, savefn;} FD_GPOOL;
typedef struct FD_GPOOL *fd_gpool;

/* Network Pools */

typedef struct FD_NETWORK_POOL {
  FD_POOL_FIELDS;
  struct U8_CONNPOOL *pool_connpool;
  int bulk_commitp;} FD_NETWORK_POOL;
typedef struct FD_NETWORK_POOL *fd_network_pool;

/* External Pools */

typedef struct FD_EXTPOOL {
  FD_POOL_FIELDS;
  fdtype fetchfn, savefn;
  fdtype lockfn, allocfn;
  fdtype state;} FD_EXTPOOL;
typedef struct FD_EXTPOOL *fd_extpool;

FD_EXPORT
fd_pool fd_make_extpool
  (u8_string label,
   FD_OID base,int cap,
   fdtype fetchfn,fdtype savefn,
   fdtype lockfn,fdtype allocfn,
   fdtype state);
FD_EXPORT int fd_extpool_cache_value(fd_pool p,fdtype oid,fdtype value);

FD_EXPORT struct FD_POOL_HANDLER fd_extpool_handler;

/* Memory Pools (only in memory, no fetch/commit) */

typedef struct FD_MEMPOOL {
  FD_POOL_FIELDS;
  unsigned int pool_load;
  unsigned int noswap;
  u8_mutex pool_lock;} FD_MEMPOOL;
typedef struct FD_MEMPOOL *fd_mempool;

FD_EXPORT fd_pool fd_make_mempool
  (u8_string label,FD_OID base,unsigned int cap,unsigned int load,
   unsigned int noswap);
/* Removes deadwood */
FD_EXPORT int fd_clean_mempool(fd_pool p);
/* Clears all the locks, caches, and load for a mempool. */
FD_EXPORT int fd_reset_mempool(fd_pool p);

/* File pool opener */

FD_EXPORT fd_pool (*fd_file_pool_opener)(u8_string);

FD_EXPORT void fd_register_pool_opener
  (unsigned int id,
   fd_pool (*opener)(u8_string filename,int read_only),
   fdtype (*mdreader)(FD_DTYPE_STREAM *),
   fdtype (*mdwriter)(FD_DTYPE_STREAM *,fdtype));

#endif /* FRAMERD_POOLS_H */
