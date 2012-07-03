/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2012 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_POOLS_H
#define FDB_POOLS_H 1
#ifndef FDB_POOLS_H_INFO
#define FDB_POOLS_H_INFO "include/framerd/pools.h"
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
   within the OID address space.  In FDB, for instance, there are 1024
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
   
   In FDB, all pools have three additional tables associated with them:
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

#define FD_POOL_BATCHABLE 2

FD_EXPORT fd_exception
  fd_CantLockOID, fd_InvalidPoolPtr, 
  fd_NotAFilePool, fd_AnonymousOID, fd_UnallocatedOID,
  fd_NoFilePools, fd_NotAPool, fd_UnknownPool, fd_CorrputedPool,
  fd_BadFilePoolLabel, fd_ReadOnlyPool, fd_ExhaustedPool,
  fd_PoolCommitError, fd_UnresolvedPool;

FD_EXPORT int fd_ignore_anonymous_oids;

typedef struct FD_ADJUNCT {
  struct FD_POOL *pool; fdtype slotid; fdtype table;} FD_ADJUNCT;
typedef struct FD_ADJUNCT *fd_adjunct;

#define FD_POOL_FIELDS \
  FD_CONS_HEADER;                                          \
  FD_OID base;                                             \
  unsigned int capacity, read_only;                        \
  int serialno; int cache_level, flags;                    \
  u8_string label, source, cid, xid, prefix;		   \
  int n_adjuncts, max_adjuncts;                            \
  struct FD_ADJUNCT *adjuncts;				   \
  struct FD_POOL_HANDLER *handler;                         \
  fdtype oidnamefn;                                        \
  struct FD_HASHTABLE cache, locks; int n_locks

typedef struct FD_POOL {FD_POOL_FIELDS;} FD_POOL;
typedef struct FD_POOL *fd_pool;
FD_EXPORT struct FD_POOL *fd_top_pools[];

FD_EXPORT int fd_n_pools;
FD_EXPORT fd_pool fd_default_pool;
FD_EXPORT int fd_register_pool(fd_pool p);

FD_EXPORT fdtype fd_all_pools(void);

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
#elif (FD_USE_TLS)
FD_EXPORT u8_tld_key fd_pool_delays_key;
#define fd_pool_delays ((fdtype *)u8_tld_get(fd_pool_delays_key))
#elif (FD_USE__THREAD)
FD_EXPORT __thread fdtype *fd_pool_delays;
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
FD_EXPORT int fd_pool_lock(fd_pool p,fdtype oids);
FD_EXPORT int fd_pool_unlock(fd_pool p,fdtype oids,int commit);
FD_EXPORT int fd_pool_commit(fd_pool p,fdtype oids,int unlock);
FD_EXPORT void fd_pool_setcache(fd_pool p,int level);
FD_EXPORT void fd_pool_close(fd_pool p);
FD_EXPORT void fd_pool_swapout(fd_pool p);

FD_EXPORT int fd_pool_unlock_all(fd_pool p,int commit);
FD_EXPORT int fd_pool_commit_all(fd_pool p,int unlock);

FD_EXPORT int fd_pool_load(fd_pool p);

FD_EXPORT int fd_set_oid_value(fdtype oid,fdtype value);
FD_EXPORT fdtype fd_locked_oid_value(fd_pool p,fdtype oid);

FD_EXPORT int fd_swapout_pools(void);
FD_EXPORT int fd_close_pools(void);
FD_EXPORT int fd_commit_pools(void);
FD_EXPORT int fd_commit_pools_noerr(void);
FD_EXPORT int fd_unlock_pools(int);
FD_EXPORT long fd_object_cache_load(void);
FD_EXPORT fdtype fd_cached_oids(fd_pool p);

FD_EXPORT int fd_commit_oids(fdtype oids,int unlock);

#if FD_INLINE_POOLS
FD_FASTOP fd_pool fd_oid2pool(fdtype oid)
{
  int baseid=FD_OID_BASE_ID(oid);
  int baseoff=FD_OID_BASE_OFFSET(oid);
  struct FD_POOL *top=fd_top_pools[baseid];
  if (top==NULL) return top;
  else if (baseoff<top->capacity) return top;
  else if (top->capacity) {
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
    fdtype value=((fdtc->oids.n_keys)?
		  (fd_hashtable_get(&(fdtc->oids),oid,FD_VOID)):
		  (FD_VOID));
    if (!(FD_VOIDP(value))) return value;}
  if (p->n_locks)
    if (fd_hashtable_probe_novoid(&(p->locks),oid)) {
      value=fd_hashtable_get(&(p->locks),oid,FD_VOID);
      if (value == FD_LOCKHOLDER) {
	value=fd_pool_fetch(p,oid);
	fd_hashtable_store(&(p->locks),oid,value);
	return value;}
      else return value;}
  if (p->cache_level)
    value=fd_hashtable_get(&(p->cache),oid,FD_VOID);
  else value=FD_VOID;
  if (FD_VOIDP(value)) {
    if (fd_ipeval_delay(1)) {
      FD_ADD_TO_CHOICE(fd_pool_delays[p->serialno],oid);
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
#else
#define fd_fetch_oid _fd_fetch_oid
#define fd_oid_value _fd_oid_value
#define fd_oid2pool _fd_oid2pool
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
  struct U8_CONNPOOL *connpool;
  int bulk_commitp;} FD_NETWORK_POOL;
typedef struct FD_NETWORK_POOL *fd_network_pool;

/* External Pools */

typedef struct FD_EXTPOOL {
  FD_POOL_FIELDS;
  fdtype fetchfn, savefn, lockfn, state;} FD_EXTPOOL;
typedef struct FD_EXTPOOL *fd_extpool;

FD_EXPORT
fd_pool fd_make_extpool
  (u8_string label,
   FD_OID base,int cap,
   fdtype fetchfn,fdtype savefn,
   fdtype lockfn,fdtype state);
FD_EXPORT int fd_extpool_cache_value(fd_pool p,fdtype oid,fdtype value);

FD_EXPORT struct FD_POOL_HANDLER fd_extpool_handler;

/* Memory Pools (only in memory, no fetch/commit) */

typedef struct FD_MEMPOOL {
  FD_POOL_FIELDS;
  unsigned int load; u8_mutex lock;} FD_MEMPOOL;
typedef struct FD_MEMPOOL *fd_mempool;

FD_EXPORT fd_pool fd_make_mempool
  (u8_string label,FD_OID base,unsigned int cap,unsigned int load);
/* Removes deadwood */
FD_EXPORT int fd_clean_mempool(fd_pool p);

/* File pool opener */

FD_EXPORT fd_pool (*fd_file_pool_opener)(u8_string);

FD_EXPORT void fd_register_pool_opener
  (unsigned int id,
   fd_pool (*opener)(u8_string filename,int read_only),
   fdtype (*mdreader)(FD_DTYPE_STREAM *),
   fdtype (*mdwriter)(FD_DTYPE_STREAM *,fdtype));

#endif /* FDB_POOLS_H */

