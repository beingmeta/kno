/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_POOLS 1
#define FD_INLINE_IPEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/storage.h"
#include "framerd/apply.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8netfns.h>
#include <libu8/u8printf.h>

fd_exception fd_UnknownPoolType=_("Unknown pool type");
fd_exception fd_CantLockOID=_("Can't lock OID");
fd_exception fd_CantUnlockOID=_("Can't unlock OID");
fd_exception fd_PoolRangeError=_("the OID is out of the range of the pool");
fd_exception fd_NoLocking=_("No locking available");
fd_exception fd_ReadOnlyPool=_("pool is read-only");
fd_exception fd_InvalidPoolPtr=_("Invalid pool PTR");
fd_exception fd_NotAFilePool=_("not a file pool");
fd_exception fd_NoFilePools=_("file pools are not supported");
fd_exception fd_AnonymousOID=_("no pool covers this OID");
fd_exception fd_NotAPool=_("Not a pool");
fd_exception fd_NoSuchPool=_("No such pool");
fd_exception fd_BadFilePoolLabel=_("file pool label is not a string");
fd_exception fd_ExhaustedPool=_("pool has no more OIDs");
fd_exception fd_InvalidPoolRange=_("pool overlaps 0x100000 boundary");
fd_exception fd_PoolCommitError=_("can't save changes to pool");
fd_exception fd_UnregisteredPool=_("internal error with unregistered pool");
fd_exception fd_UnhandledOperation=_("This pool can't handle this operation");
fd_exception fd_DataFileOverflow=_("Data file is over implementation limit");

u8_condition fd_PoolCommit=_("Pool/Commit");

int fd_pool_cache_init = FD_POOL_CACHE_INIT;
int fd_pool_lock_init  = FD_POOL_CHANGES_INIT;

int fd_n_pools = 0;
struct FD_POOL *fd_top_pools[FD_N_OID_BUCKETS];

static struct FD_HASHTABLE poolid_table;

static u8_condition ipeval_objfetch="OBJFETCH";

static fdtype lock_symbol, unlock_symbol;

fd_pool fd_pools_by_serialno[FD_MAX_POOLS];

static int savep(fdtype v,int only_finished);
static int modifiedp(fdtype v);

/* This is used in committing pools */

struct FD_POOL_WRITES {
  int len; fdtype *oids, *values;};

/* Locking functions for external libraries */

FD_EXPORT void _fd_lock_pool(fd_pool p)
{
  fd_lock_pool(p);
}

FD_EXPORT void _fd_unlock_pool(fd_pool p)
{
  if (p->pool_islocked) {
    p->pool_islocked = 0;
    u8_unlock_mutex(&((p)->pool_lock));}
  else u8_log(LOGCRIT,"PoolUnLockError",                       \
              "Pool '%s' already unlocked!",p->poolid);
}

/* Pool delays for IPEVAL */

#if FD_GLOBAL_IPEVAL
fdtype *fd_pool_delays = NULL;
#elif FD_USE__THREAD
__thread fdtype *fd_pool_delays = NULL;
#elif FD_USE_TLS
u8_tld_key fd_pool_delays_key;
#else
fdtype *fd_pool_delays = NULL;
#endif

#if ((FD_USE_TLS) && (!(FD_GLOBAL_IPEVAL)))
FD_EXPORT fdtype *fd_get_pool_delays()
{
  return (fdtype *) u8_tld_get(fd_pool_delays_key);
}
FD_EXPORT void fd_init_pool_delays()
{
  int i = 0;
  fdtype *delays = (fdtype *) u8_tld_get(fd_pool_delays_key);
  if (delays) return;
  delays = u8_alloc_n(FD_N_POOL_DELAYS,fdtype);
  while (i<FD_N_POOL_DELAYS) delays[i++]=FD_EMPTY_CHOICE;
  u8_tld_set(fd_pool_delays_key,delays);
}
#else
FD_EXPORT fdtype *fd_get_pool_delays()
{
  return fd_pool_delays;
}
FD_EXPORT void fd_init_pool_delays()
{
  int i = 0;
  if (fd_pool_delays) return;
  fd_pool_delays = u8_alloc_n(FD_N_POOL_DELAYS,fdtype);
  while (i<FD_N_POOL_DELAYS) fd_pool_delays[i++]=FD_EMPTY_CHOICE;
}
#endif

static struct FD_POOL_HANDLER gluepool_handler;
static void (*pool_conflict_handler)(fd_pool upstart,fd_pool holder) = NULL;

static void pool_conflict(fd_pool upstart,fd_pool holder);
static struct FD_GLUEPOOL *make_gluepool(FD_OID base);
static int add_to_gluepool(struct FD_GLUEPOOL *gp,fd_pool p);

FD_EXPORT fd_pool fd_open_network_pool(u8_string spec,fd_storage_flags flags);

int fd_ignore_anonymous_oids = 0;

static u8_mutex pool_registry_lock;

FD_EXPORT fdtype fd_anonymous_oid(const u8_string cxt,fdtype oid)
{
  if (fd_ignore_anonymous_oids) return FD_EMPTY_CHOICE;
  else return fd_err(fd_AnonymousOID,cxt,NULL,oid);
}

/* Pool ops */

FD_EXPORT fdtype fd_pool_ctl(fd_pool p,int poolop,int n,fdtype *args)
{
  struct FD_POOL_HANDLER *h = p->pool_handler;
  if (h->poolctl)
    return h->poolctl(p,poolop,n,args);
  else return FD_FALSE;
}

/* Pool caching */

FD_EXPORT void fd_pool_setcache(fd_pool p,int level)
{
  fdtype intarg = FD_INT(level);
  fdtype result = fd_pool_ctl(p,FD_POOLOP_CACHELEVEL,1,&intarg);
  if (FD_ABORTP(result)) {fd_clear_errors(1);}
  fd_decref(result);
  p->pool_cache_level = level;
}

static void init_cache_level(fd_pool p)
{
  if (FD_EXPECT_FALSE(p->pool_cache_level<0)) {
    fd_pool_setcache(p,fd_default_cache_level);}
}

FD_EXPORT
void fd_reset_pool_tables(fd_pool p,
                          ssize_t cacheval,
                          ssize_t locksval)
{
  int read_only = U8_BITP(p->pool_flags,FD_STORAGE_READ_ONLY);
  fd_hashtable cache = &(p->pool_cache), locks = &(p->pool_changes);
  fd_reset_hashtable(cache,((cacheval==0)?(fd_pool_cache_init):(cacheval)),1);
  if (locks->table_n_keys==0) {
    ssize_t level = (read_only)?(0):(locksval==0)?(fd_pool_lock_init):(locksval);
    fd_reset_hashtable(locks,level,1);}
}

/* Registering pools */

FD_EXPORT int fd_register_pool(fd_pool p)
{
  unsigned int capacity = p->pool_capacity, serial_no;
  int bix = fd_get_oid_base_index(p->pool_base,1);
  if (p->pool_serialno>=0)
    return 0;
  else if (bix<0)
    return bix;
  else if (p->pool_flags&FD_STORAGE_UNREGISTERED)
    return 0;
  else u8_lock_mutex(&pool_registry_lock);
  /* Set up the serial number */
  serial_no = p->pool_serialno = fd_n_pools++;
  fd_pools_by_serialno[serial_no]=p;
  if ((capacity>=FD_OID_BUCKET_SIZE) &&
      ((p->pool_base)%FD_OID_BUCKET_SIZE)) {
    fd_seterr(fd_InvalidPoolRange,"fd_register_pool",
              u8_strdup(p->poolid),FD_VOID);
    u8_unlock_mutex(&pool_registry_lock);
    return -1;}
  if (p->pool_flags&FD_POOL_ADJUNCT) {
    u8_unlock_mutex(&pool_registry_lock);
    return 1;}
  if (capacity>=FD_OID_BUCKET_SIZE) {
    int i = 0, lim = capacity/FD_OID_BUCKET_SIZE;
    /* Now get baseids for the pool and save them in fd_top_pools */
    while (i<lim) {
      FD_OID base = FD_OID_PLUS(p->pool_base,(FD_OID_BUCKET_SIZE*i));
      int baseid = fd_get_oid_base_index(base,1);
      if (baseid<0) {
        u8_unlock_mutex(&pool_registry_lock);
        return -1;}
      else if (fd_top_pools[baseid]) {
        pool_conflict(p,fd_top_pools[baseid]);
        u8_unlock_mutex(&pool_registry_lock);
        return -1;}
      else fd_top_pools[baseid]=p;
      i++;}}
  else if (fd_top_pools[bix] == NULL) {
    /* If the pool is smaller than an OID bucket, and there isn't a
       pool in fd_top_pools, create a gluepool and place it there */
    struct FD_GLUEPOOL *gluepool = make_gluepool(fd_base_oids[bix]);
    fd_top_pools[bix]=(struct FD_POOL *)gluepool;
    if (add_to_gluepool(gluepool,p)<0) {
      u8_unlock_mutex(&pool_registry_lock);
      return -1;}}
  else if (fd_top_pools[bix]->pool_capacity) {
    /* If the top pool has a capacity (i.e. it's not a gluepool), we
       have a pool conflict. Complain and error. */
    pool_conflict(p,fd_top_pools[bix]);
    u8_unlock_mutex(&pool_registry_lock);
    return -1;}
  else if (add_to_gluepool((struct FD_GLUEPOOL *)fd_top_pools[bix],p)<0) {
    /* Otherwise, it is a gluepool, so try to add the pool to it. This
       will error if there is a pool conflict within the glue pool, so
       we check. */
    u8_unlock_mutex(&pool_registry_lock);
    return -1;}
  u8_unlock_mutex(&pool_registry_lock);
  if (p->pool_label) {
    u8_string base = u8_string_subst(p->pool_label,"/","_");
    u8_byte *dot = strchr(base,'.');
    fdtype pkey = FD_VOID, probe = FD_VOID;
    if (dot) {
      pkey = fd_substring(base,dot);
      probe = fd_hashtable_get(&poolid_table,pkey,FD_EMPTY_CHOICE);
      if (FD_EMPTY_CHOICEP(probe)) {
        fd_hashtable_store(&poolid_table,pkey,fd_pool2lisp(p));
        p->pool_prefix = FD_STRDATA(pkey);}
      else fd_decref(pkey);}
    else pkey = fd_substring(base,NULL);
    probe = fd_hashtable_get(&poolid_table,pkey,FD_EMPTY_CHOICE);
    if (FD_EMPTY_CHOICEP(probe)) {
      fd_hashtable_store(&poolid_table,pkey,fd_pool2lisp(p));
      if (p->pool_prefix == NULL) p->pool_prefix = FD_STRDATA(pkey);}
    u8_free(base);}
  return 1;
}

static struct FD_GLUEPOOL *make_gluepool(FD_OID base)
{
  struct FD_GLUEPOOL *pool = u8_alloc(struct FD_GLUEPOOL);
  pool->pool_base = base; pool->pool_capacity = 0;
  pool->pool_flags = FD_STORAGE_READ_ONLY;
  pool->pool_serialno = fd_get_oid_base_index(base,1);
  pool->pool_label="gluepool";
  pool->pool_source = NULL;
  pool->n_subpools = 0; pool->subpools = NULL;
  pool->pool_handler = &gluepool_handler;
  FD_INIT_STATIC_CONS(&(pool->pool_cache),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(pool->pool_changes),fd_hashtable_type);
  fd_make_hashtable(&(pool->pool_cache),64);
  fd_make_hashtable(&(pool->pool_changes),64);
  /* There was a redundant serial number call here */
  return pool;
}

static int add_to_gluepool(struct FD_GLUEPOOL *gp,fd_pool p)
{
  if (gp->n_subpools == 0) {
    struct FD_POOL **pools = u8_alloc_n(1,fd_pool);
    pools[0]=p; gp->n_subpools = 1; gp->subpools = pools;
    return 1;}
  else {
    int comparison = 0;
    struct FD_POOL **bottom = gp->subpools;
    struct FD_POOL **top = gp->subpools+(gp->n_subpools)-1;
    struct FD_POOL **read, **new, **write, **ipoint;
    struct FD_POOL **middle = bottom+(top-bottom)/2;
    /* First find where it goes (or a conflict if there is one) */
    while (top>=bottom) {
      middle = bottom+(top-bottom)/2;
      comparison = FD_OID_COMPARE(p->pool_base,(*middle)->pool_base);
      if (comparison == 0) break;
      else if (bottom == top) break;
      else if (comparison<0) top = middle-1; else bottom = middle+1;}
    /* If there is a conflict, we report it and quit */
    if (comparison==0) {pool_conflict(p,*middle); return -1;}
    /* Now we make a new vector to copy into */
    write = new = u8_alloc_n((gp->n_subpools+1),struct FD_POOL *);
    /* Figure out where we should put the new pool */
    if (comparison>0)
      ipoint = write+(middle-gp->subpools)+1;
    else ipoint = write+(middle-gp->subpools);
    /* Now, copy the subpools, doing the insertions */
    read = gp->subpools; top = gp->subpools+gp->n_subpools;
    while (read<top)
      if (write == ipoint) {*write++=p; *write++= *read++;}
      else *write++= *read++;
    if (write == ipoint) *write = p;
    /* Finally, update the structures.  Note that we are explicitly
       leaking the old subpools because we're avoiding locking on lookup. */
    p->pool_serialno = ((gp->pool_serialno)<<10)+(gp->n_subpools+1);
    gp->subpools = new; gp->n_subpools++;}
  return 1;
}

FD_EXPORT u8_string fd_pool_id(fd_pool p)
{
  if (p->pool_label!=NULL) return p->pool_label;
  else if (p->poolid!=NULL) return p->poolid;
  else if (p->pool_source!=NULL) return p->pool_source;
  else return NULL;
}

FD_EXPORT u8_string fd_pool_label(fd_pool p)
{
  if (p->pool_label!=NULL) return p->pool_label;
  else return NULL;
}

/* Finding the subpool */

FD_EXPORT fd_pool fd_find_subpool(struct FD_GLUEPOOL *gp,fdtype oid)
{
  FD_OID addr = FD_OID_ADDR(oid);
  struct FD_POOL **subpools = gp->subpools; int n = gp->n_subpools;
  struct FD_POOL **bottom = subpools, **top = subpools+(n-1);
  while (top>=bottom) {
    struct FD_POOL **middle = bottom+(top-bottom)/2;
    int comparison = FD_OID_COMPARE(addr,(*middle)->pool_base);
    unsigned int difference = FD_OID_DIFFERENCE(addr,(*middle)->pool_base);
    if (comparison < 0) top = middle-1;
    else if ((difference < ((*middle)->pool_capacity)))
      return *middle;
    else if (bottom == top) break;
    else bottom = middle+1;}
  return NULL;
}

/* Handling pool conflicts */

static void pool_conflict(fd_pool upstart,fd_pool holder)
{
  if (pool_conflict_handler) pool_conflict_handler(upstart,holder);
  else {
    u8_log(LOG_WARN,_("Pool conflict"),
           "%s (from %s) and existing pool %s (from %s)\n",
           upstart->pool_label,upstart->pool_source,
           holder->pool_label,holder->pool_source);
    u8_seterr(_("Pool confict"),NULL,NULL);}
}


/* Basic functions on single OID values */

FD_EXPORT fdtype fd_oid_value(fdtype oid)
{
  if (FD_EMPTY_CHOICEP(oid)) return oid;
  else if (FD_OIDP(oid)) return fd_fetch_oid(NULL,oid);
  else return fd_type_error(_("OID"),"fd_oid_value",oid);
}

FD_EXPORT fdtype fd_locked_oid_value(fd_pool p,fdtype oid)
{
  if (p->pool_handler->lock == NULL) {
    return fd_fetch_oid(p,oid);}
  else if (p == fd_zero_pool)
    return fd_zero_pool_value(oid);
  else {
    fdtype smap = fd_hashtable_get(&(p->pool_changes),oid,FD_VOID);
    if (FD_VOIDP(smap)) {
      int retval = fd_pool_lock(p,oid);
      if (retval<0) return FD_ERROR_VALUE;
      else if (retval) {
        fdtype v = fd_fetch_oid(p,oid);
        if (FD_ABORTP(v)) return v;
        fd_hashtable_store(&(p->pool_changes),oid,v);
        return v;}
      else return fd_err(fd_CantLockOID,"fd_locked_oid_value",
                         u8_strdup(p->pool_source),oid);}
    else if (smap == FD_LOCKHOLDER) {
      fdtype v = fd_fetch_oid(p,oid);
      if (FD_ABORTP(v)) return v;
      fd_hashtable_store(&(p->pool_changes),oid,v);
      return v;}
    else return smap;}
}

FD_EXPORT int fd_set_oid_value(fdtype oid,fdtype value)
{
  fd_pool p = fd_oid2pool(oid);
  if (p == NULL)
    return fd_reterr(fd_AnonymousOID,"SET-OID_VALUE!",NULL,oid);
  else if (p == fd_zero_pool)
    return fd_zero_pool_store(oid,value);
  else {
    if ((FD_SLOTMAPP(value))||(FD_SCHEMAPP(value))||(FD_HASHTABLEP(value)))
      fd_set_modified(value,1);
    if (p->pool_handler->lock == NULL) {
      fd_hashtable_store(&(p->pool_cache),oid,value);
      return 1;}
    else if (fd_lock_oid(oid)) {
      fd_hashtable_store(&(p->pool_changes),oid,value);
      return 1;}
    else return fd_reterr(fd_CantLockOID,"SET-OID_VALUE!",NULL,oid);}
}

/* Fetching OID values */

FD_EXPORT fdtype fd_pool_fetch(fd_pool p,fdtype oid)
{
  fdtype v = FD_VOID;
  init_cache_level(p);
  if (p == fd_zero_pool)
    return fd_zero_pool_value(oid);
  else if (p->pool_handler->fetch)
    v = p->pool_handler->fetch(p,oid);
  else if (p->pool_cache_level)
    v = fd_hashtable_get(&(p->pool_cache),oid,FD_EMPTY_CHOICE);
  else {}
  if (FD_ABORTP(v)) return v;
  else if (p->pool_cache_level == 0)
    return v;
  /* If it's locked, store it in the locks table */
  else if ( (p->pool_changes.table_n_keys) &&
            (fd_hashtable_op(&(p->pool_changes),fd_table_replace_novoid,oid,v)) )
    return v;
  else if (FD_SLOTMAPP(v)) {FD_SLOTMAP_SET_READONLY(v);}
  else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_SET_READONLY(v);}
  else {}
  if (p->pool_cache_level>0)
    fd_hashtable_store(&(p->pool_cache),oid,v);
  return v;
}

FD_EXPORT int fd_pool_prefetch(fd_pool p,fdtype oids)
{
  FDTC *fdtc = ((FD_USE_THREADCACHE)?(fd_threadcache):(NULL));
  struct FD_HASHTABLE *oidcache = ((fdtc!=NULL)?(&(fdtc->oids)):(NULL));
  int decref_oids = 0, cachelevel;
  if (p == NULL) {
    fd_seterr(fd_NotAPool,"fd_pool_prefetch",
              u8_strdup("NULL pool ptr"),FD_VOID);
    return -1;}
  else if (FD_EMPTY_CHOICEP(oids))
    return 0;
  else init_cache_level(p);
  cachelevel = p->pool_cache_level;
  /* if (p->pool_cache_level<1) return 0; */
  if (p->pool_handler->fetchn == NULL) {
    if (fd_ipeval_delay(FD_CHOICE_SIZE(oids))) {
      FD_ADD_TO_CHOICE(fd_pool_delays[p->pool_serialno],oids);
      return 0;}
    else {
      int n_fetches = 0;
      FD_DO_CHOICES(oid,oids) {
        fdtype v = fd_pool_fetch(p,oid);
        if (FD_ABORTP(v)) {
          FD_STOP_DO_CHOICES; return v;}
        n_fetches++; fd_decref(v);}
      return n_fetches;}}
  if (FD_PRECHOICEP(oids)) {
    oids = fd_make_simple_choice(oids); 
    decref_oids = 1;}
  if (fd_ipeval_status()) {
    FD_HASHTABLE *cache = &(p->pool_cache); int n_to_fetch = 0;
    /* fdtype oidschoice = fd_make_simple_choice(oids); */
    fdtype *delays = &(fd_pool_delays[p->pool_serialno]);
    FD_DO_CHOICES(oid,oids)
      if (fd_hashtable_probe_novoid(cache,oid)) {}
      else {
        FD_ADD_TO_CHOICE(*delays,oid);
        n_to_fetch++;}
    (void)fd_ipeval_delay(n_to_fetch);
    /* fd_decref(oidschoice); */
    return 0;}
  else if (FD_CHOICEP(oids)) {
    struct FD_HASHTABLE *cache = &(p->pool_cache), *locks = &(p->pool_changes);
    int n = FD_CHOICE_SIZE(oids);
    /* We use n_locked to track how many of the OIDs are stored in
       pool_changes rather than pool_cache */
    int n_locked = (locks->table_n_keys)?(0):(-1);
    fdtype *values, *oidv = u8_alloc_n(n,fdtype), *write = oidv;
    FD_DO_CHOICES(o,oids)
      if (((oidcache == NULL)||(fd_hashtable_probe_novoid(oidcache,o)==0))&&
          (fd_hashtable_probe_novoid(cache,o)==0) &&
          (fd_hashtable_probe(locks,o)==0))
        /* If it's not in the oid cache, the pool cache, or the locks, get it */
        *write++=o;
      else if ((n_locked>=0)&&
               (fd_hashtable_op(locks,fd_table_test,o,FD_LOCKHOLDER))) {
        /* If it's in the locks but not loaded, save it for loading and not
           that some of the results should be put in the locks rather than
           the cache */
        *write++=o; n_locked++;}
      else {}
    if (write == oidv) {
      /* Nothing to prefetch, free and return */
      u8_free(oidv);
      if (decref_oids) fd_decref(oids);
      return 0;}
    else n = write-oidv;
    /* Call the pool handler */
    values = p->pool_handler->fetchn(p,n,oidv);
    /* If you got results, store them in the cache */
    if (values)
      if (n_locked) {
        /* If some values are locked, we consider each value and
           store it in the appropriate tables (locks or cache). */
        int j = 0; while (j<n) {
          fdtype v = values[j], oid = oidv[j];
          if (fd_hashtable_op(&(p->pool_changes),fd_table_replace_novoid,oid,v)==0) {
            /* This is when the OID we're storing isn't locked */
            if (FD_SLOTMAPP(v)) {FD_SLOTMAP_SET_READONLY(v);}
            else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_SET_READONLY(v);}
            if (fdtc) fd_hashtable_op(&(fdtc->oids),fd_table_store,oid,v);
            if (cachelevel>0)
              fd_hashtable_op(&(p->pool_cache),fd_table_store,oid,v);}
          /* We decref it since it would have been incref'd when
             processed above. */
          fd_decref(values[j]);
          j++;}}
      else {
        int j = 0; while (j<n) {
          fdtype v = values[j++];
          if (FD_SLOTMAPP(v)) {FD_SLOTMAP_SET_READONLY(v);}
          else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_SET_READONLY(v);}}
        if (fdtc)
          fd_hashtable_iter(oidcache,fd_table_store,n,oidv,values);
        if (cachelevel>0)
          fd_hashtable_iter(&(p->pool_cache),fd_table_store_noref,n,oidv,values);}
    else {
      u8_free(oidv);
      if (decref_oids) fd_decref(oids);
      return -1;}
    /* We don't have to do this now that we have fd_table_store_noref */
    /* i = 0; while (i < n) {fd_decref(values[i]); i++;} */
    u8_free(oidv); u8_free(values);
    if (decref_oids) fd_decref(oids);
    return n;}
  else {
    fdtype v = p->pool_handler->fetch(p,oids);
    fd_hashtable changes = &(p->pool_changes);
    if ( (changes->table_n_keys==0) ||
         /* This will store it in changes if it's already there (i.e. 'locked') */
         (fd_hashtable_op(changes,fd_table_replace_novoid,oids,v)==0) ) {
      if (fdtc) fd_hashtable_op(&(fdtc->oids),fd_table_store,oids,v);}
    if (cachelevel>0) fd_hashtable_store(&(p->pool_cache),oids,v);
    fd_decref(v);
    return 1;}
}

/* Swapping out OIDs */

FD_EXPORT int fd_pool_swapout(fd_pool p,fdtype oids)
{
  fd_hashtable cache = &(p->pool_cache);
  if (FD_PRECHOICEP(oids)) {
    fdtype simple = fd_make_simple_choice(oids);
    int rv = fd_pool_swapout(p,simple);
    fd_decref(simple);
    return rv;}
  else if (FD_VOIDP(oids)) {
    int rv = cache->table_n_keys;
    if ((p->pool_flags)&(FD_STORAGE_KEEP_CACHESIZE))
      fd_reset_hashtable(cache,-1,1);
    else fd_reset_hashtable(cache,fd_pool_cache_init,1);
    return rv;}
  else if ((FD_OIDP(oids))||(FD_CHOICEP(oids)))
    u8_log(fd_storage_loglevel+2,"SwapPool",
           "Swapping out %d oids in pool %s",
           FD_CHOICE_SIZE(oids),p->poolid);
  else if (FD_PRECHOICEP(oids))
    u8_log(fd_storage_loglevel+2,"SwapPool",
           "Swapping out ~%d oids in pool %s",
           FD_PRECHOICE_SIZE(oids),p->poolid);
  else u8_log(fd_storage_loglevel+2,"SwapPool",
              "Swapping out oids in pool %s",p->poolid);
  int rv = -1;
  double started = u8_elapsed_time();
  if (p->pool_handler->swapout) {
    p->pool_handler->swapout(p,oids);
    u8_log(fd_storage_loglevel+1,"SwapPool",
           "Finished custom swapout for pool %s, clearing caches...",
           p->poolid);}
  else u8_log(fd_storage_loglevel+2,"SwapPool",
              "No custom swapout clearing caches for %s",p->poolid);
  if (p->pool_flags&FD_STORAGE_NOSWAP)
    return 0;
  else if (FD_OIDP(oids)) {
    fd_hashtable_store(cache,oids,FD_VOID);
    rv = 1;}
  else if (FD_CHOICEP(oids)) {
    rv = FD_CHOICE_SIZE(oids);
    fd_hashtable_iterkeys(cache,fd_table_replace,
                          FD_CHOICE_SIZE(oids),FD_CHOICE_DATA(oids),
                          FD_VOID);
    fd_devoid_hashtable(cache,0);}
  else {
    rv = cache->table_n_keys;
    if ((p->pool_flags)&(FD_STORAGE_KEEP_CACHESIZE))
      fd_reset_hashtable(cache,-1,1);
    else fd_reset_hashtable(cache,fd_pool_cache_init,1);}
  u8_log(fd_storage_loglevel+1,"SwapPool",
         "Swapped out %d oids from pool '%s' in %f",
         rv,p->poolid,u8_elapsed_time()-started);
  return rv;
}


/* Allocating OIDs */

FD_EXPORT fdtype fd_pool_alloc(fd_pool p,int n)
{
  fdtype result = p->pool_handler->alloc(p,n);
  if (p == fd_zero_pool)
    return result;
  else if (FD_OIDP(result)) {
    if (p->pool_handler->lock)
      fd_hashtable_store(&(p->pool_changes),result,FD_EMPTY_CHOICE);
    else fd_hashtable_store(&(p->pool_cache),result,FD_EMPTY_CHOICE);
    return result;}
  else if (FD_CHOICEP(result)) {
    if (p->pool_handler->lock)
      fd_hashtable_iterkeys(&(p->pool_changes),fd_table_store,
                            FD_CHOICE_SIZE(result),FD_CHOICE_DATA(result),
                            FD_EMPTY_CHOICE);
    else fd_hashtable_iterkeys(&(p->pool_cache),fd_table_store,
                               FD_CHOICE_SIZE(result),FD_CHOICE_DATA(result),
                               FD_EMPTY_CHOICE);
    return result;}
  else if (FD_ABORTP(result)) return result;
  else if (FD_EXCEPTIONP(result)) return result;
  else return fd_err("BadDriverResult","fd_pool_alloc",
                     u8_strdup(p->pool_source),FD_VOID);
}

/* Locking and unlocking OIDs within pools */

FD_EXPORT int fd_pool_lock(fd_pool p,fdtype oids)
{
  int decref_oids = 0;
  struct FD_HASHTABLE *locks = &(p->pool_changes);
  if (p->pool_handler->lock == NULL)
    return 0;
  if (FD_PRECHOICEP(oids)) {
    oids = fd_make_simple_choice(oids); decref_oids = 1;}
  if (FD_CHOICEP(oids)) {
    fdtype needy; int retval, n;
    struct FD_CHOICE *oidc = fd_alloc_choice(FD_CHOICE_SIZE(oids));
    fdtype *oidv = (fdtype *)FD_XCHOICE_DATA(oidc), *write = oidv;
    FD_DO_CHOICES(o,oids)
      if (fd_hashtable_probe(locks,o)==0) *write++=o;
    if (decref_oids) fd_decref(oids);
    if (write == oidv) {
      /* Nothing to lock, free and return */
      u8_free(oidc);
      return 1;}
    else n = write-oidv;
    if (p->pool_handler->lock == NULL) {
      fd_seterr(fd_CantLockOID,"fd_pool_lock",
                u8_strdup(p->poolid),oids);
      return -1;}
    needy = fd_init_choice(oidc,n,NULL,FD_CHOICE_ISATOMIC);
    retval = p->pool_handler->lock(p,needy);
    if (retval<0) {
      fd_decref(needy);
      return retval;}
    else if (retval) {
      fd_hashtable_iterkeys(&(p->pool_cache),fd_table_replace,n,oidv,FD_VOID);
      fd_hashtable_iterkeys(locks,fd_table_store,n,oidv,FD_LOCKHOLDER);}
    fd_decref(needy);
    return retval;}
  else if (fd_hashtable_probe(locks,oids)==0) {
    int retval = p->pool_handler->lock(p,oids);
    if (decref_oids) fd_decref(oids);
    if (retval<0) return retval;
    else if (retval) {
      fd_hashtable_op(&(p->pool_cache),fd_table_replace,oids,FD_VOID);
      fd_hashtable_op(locks,fd_table_store,oids,FD_LOCKHOLDER);
      return 1;}
    else return 0;}
  else {
    if (decref_oids) fd_decref(oids);
    return 1;}
}

FD_EXPORT int fd_pool_unlock(fd_pool p,fdtype oids,
                             fd_storage_unlock_flag flags)
{
  struct FD_HASHTABLE *changes = &(p->pool_changes);
  if (changes->table_n_keys==0)
    return 0;
  else if (flags<0) {
    int n = changes->table_n_keys;
    if (p->pool_handler->unlock) {
      fdtype to_unlock = fd_hashtable_keys(changes);
      p->pool_handler->unlock(p,to_unlock);
      fd_decref(to_unlock);}
    fd_reset_hashtable(changes,-1,1);
    return -n;}
  else {
    int n_unlocked = 0, n_committed = (flags>0) ? (fd_pool_commit(p,oids)) : (0);
    fdtype to_unlock = FD_EMPTY_CHOICE;
    fdtype locked_oids = fd_hashtable_keys(changes);
    if (n_committed<0) {}
    {FD_DO_CHOICES(oid,locked_oids) {
        fd_pool pool = fd_oid2pool(oid);
        if (p == pool) {
          fdtype v = fd_hashtable_get(changes,oid,FD_VOID);
          if ( (!(FD_VOIDP(v))) && (!(modifiedp(v)))) {
            FD_ADD_TO_CHOICE(to_unlock,v);}}}}
    fd_decref(locked_oids);
    to_unlock = fd_simplify_choice(to_unlock);
    n_unlocked = FD_CHOICE_SIZE(to_unlock);
    if (p->pool_handler->unlock)
      p->pool_handler->unlock(p,to_unlock);
    fd_hashtable_iterkeys(changes,fd_table_replace,
                          FD_CHOICE_SIZE(to_unlock),
                          FD_CHOICE_DATA(to_unlock),
                          FD_VOID);
    fd_devoid_hashtable(changes,0);
    fd_decref(to_unlock);
    return n_unlocked+n_committed;}
}

FD_EXPORT int fd_pool_unlock_all(fd_pool p,fd_storage_unlock_flag flags)
{
  return fd_pool_unlock(p,FD_FALSE,flags);
}

/* Declare locked OIDs 'finished' (read-only) */

FD_EXPORT int fd_pool_finish(fd_pool p,fdtype oids)
{
  int finished = 0;
  fd_hashtable changes = &(p->pool_changes);
  FD_DO_CHOICES(oid,oids) {
    fd_pool pool = fd_oid2pool(oid);
    if (p == pool) {
      fdtype v = fd_hashtable_get(changes,oid,FD_VOID);
      if (FD_CONSP(v)) {
        if (FD_SLOTMAPP(v)) {FD_SLOTMAP_SET_READONLY(v); finished++;}
        else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_SET_READONLY(v); finished++;}
        else if (FD_HASHTABLEP(v)) {FD_HASHTABLE_SET_READONLY(v); finished++;}
        else {}
        fd_decref(v);}}}
  return finished;
}

/* Committing OIDs to external sources */

struct FD_POOL_WRITES pick_writes(fd_pool p,fdtype oids);
struct FD_POOL_WRITES pick_modified(fd_pool p,int finished);
static void abort_commit(fd_pool p,struct FD_POOL_WRITES writes);
static void finish_commit(fd_pool p,struct FD_POOL_WRITES writes);

FD_EXPORT int fd_pool_commit(fd_pool p,fdtype oids)
{
  struct FD_HASHTABLE *locks = &(p->pool_changes);

  if (locks->table_n_keys==0) {
    u8_log(fd_storage_loglevel+1,fd_PoolCommit,
           "####### No locked oids in %s",p->poolid);
    return 0;}
  else if (p->pool_handler->storen == NULL) {
    u8_log(fd_storage_loglevel+1,fd_PoolCommit,
           "####### Unlocking OIDs in %s",p->poolid);
    int rv = fd_pool_unlock(p,oids,leave_modified);
    return rv;}
  else {
    int retval = 0; double start_time = u8_elapsed_time();
    struct FD_POOL_WRITES writes=
      ((FD_FALSEP(oids))||(FD_VOIDP(oids))) ? (pick_modified(p,0)):
      ((FD_OIDP(oids))||(FD_CHOICEP(oids))||(FD_PRECHOICEP(oids))) ?
      (pick_writes(p,oids)) :
      (FD_TRUEP(oids)) ? (pick_modified(p,1)):
      (pick_writes(p,FD_EMPTY_CHOICE));
    if (writes.len) {
      u8_log(fd_storage_loglevel+1,"PoolCommit",
             "####### Saving %d/%d OIDs in %s",
             writes.len,p->pool_changes.table_n_keys,
             p->poolid);
      init_cache_level(p);
      retval = p->pool_handler->storen(p,writes.len,writes.oids,writes.values);}
    else retval = 0;

    if (retval<0) {
      u8_log(LOGCRIT,fd_PoolCommitError,
             "Error %d (%s) saving oids to %s",
             errno,u8_strerror(errno),p->poolid);
      u8_seterr(fd_PoolCommitError,"fd_pool_commit",u8_strdup(p->poolid));
      abort_commit(p,writes);
      return retval;}
    else if (writes.len) {
      u8_log(fd_storage_loglevel,fd_PoolCommit,
             "####### Saved %d OIDs to %s in %f secs",
             writes.len,p->poolid,u8_elapsed_time()-start_time);
      finish_commit(p,writes);}
    else {}
    return retval;}
}

FD_EXPORT int fd_pool_commit_all(fd_pool p)
{
  return fd_pool_commit(p,FD_FALSE);
}

FD_EXPORT int fd_pool_commit_finished(fd_pool p)
{
  return fd_pool_commit(p,FD_TRUE);
}

/* Commitment commitment and recovery */

static void abort_commit(fd_pool p,struct FD_POOL_WRITES writes)
{
  fdtype *scan = writes.values, *limit = writes.values+writes.len;
  while (scan<limit) {
    fdtype v = *scan++;
    if (!(FD_CONSP(v))) continue;
    else if (FD_SLOTMAPP(v)) {FD_SLOTMAP_MARK_MODIFIED(v);}
    else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_MARK_MODIFIED(v);}
    else if (FD_HASHTABLEP(v)) {FD_HASHTABLE_MARK_MODIFIED(v);}
    else {}
    fd_decref(v);}
  u8_free(writes.values); writes.values = NULL;
  u8_free(writes.oids); writes.oids = NULL;
}

static void finish_commit(fd_pool p,struct FD_POOL_WRITES writes)
{
  fd_hashtable changes = &(p->pool_changes);
  int i = 0, n = writes.len, unlock_count;
  fdtype *oids = writes.oids;
  fdtype *values = writes.values;
  fdtype *unlock = oids;
  while (i<n) {
    fdtype v = values[i];
    if (!(FD_CONSP(v))) {
      *unlock++=oids[i]; i++; continue;}
    else if (FD_SLOTMAPP(v)) {
      if (!(FD_SLOTMAP_MODIFIEDP(v))) {
        *unlock++=oids[i];}}
    else if (FD_SCHEMAPP(v)) {
      if (!(FD_SCHEMAP_MODIFIEDP(v))) {
        *unlock++=oids[i];}}
    else if (FD_HASHTABLEP(v)) {
      if (!(FD_HASHTABLE_MODIFIEDP(v))) {
        *unlock++=oids[i];}}
    else *unlock++=oids[i];
    fd_decref(v);
    i++;}
  u8_free(writes.values); writes.values = NULL;
  unlock_count = unlock-oids;
  if ((p->pool_handler->unlock)&&(unlock_count)) {
    fdtype to_unlock = fd_init_choice(NULL,unlock_count,oids,FD_CHOICE_ISATOMIC);
    int retval = p->pool_handler->unlock(p,to_unlock);
    if (retval<0) {
      u8_log(LOGCRIT,"UnlockFailed","Error unlocking pool %s",p->poolid);
      fd_clear_errors(1);}
    fd_decref(to_unlock);}
  fd_hashtable_iterkeys(changes,fd_table_replace,unlock_count,oids,FD_VOID);
  u8_free(writes.oids); writes.oids = NULL;
  fd_devoid_hashtable(changes,0);
}

/* Support for committing OIDs */

static int savep(fdtype v,int only_finished)
{
  if (!(FD_CONSP(v))) return 1;
  else if (FD_SLOTMAPP(v)) {
    if (!(FD_SLOTMAP_MODIFIEDP(v))) return 0;
    else if ((!(only_finished))||(FD_SLOTMAP_READONLYP(v))) {
      FD_SLOTMAP_CLEAR_MODIFIED(v);
      return 1;}
    else return 0;}
  else if (FD_SCHEMAPP(v)) {
    if (!(FD_SCHEMAP_MODIFIEDP(v))) return 0;
    else if ((!(only_finished))||(FD_SCHEMAP_READONLYP(v))) {
      FD_SCHEMAP_CLEAR_MODIFIED(v);
      return 1;}
    else return 0;}
  else if (FD_HASHTABLEP(v)) {
    if (!(FD_HASHTABLE_MODIFIEDP(v))) return 0;
    else if ((!(only_finished))||(FD_HASHTABLE_READONLYP(v))) {
      FD_HASHTABLE_CLEAR_MODIFIED(v);
      return 1;}
    else return 0;}
  else if (only_finished) return 0;
  else return 1;
}

static int modifiedp(fdtype v)
{
  if (!(FD_CONSP(v))) return 1;
  else if (FD_SLOTMAPP(v))
    return FD_SLOTMAP_MODIFIEDP(v);
  else if (FD_SCHEMAPP(v))
    return FD_SCHEMAP_MODIFIEDP(v);
  else if (FD_HASHTABLEP(v))
    return FD_HASHTABLE_MODIFIEDP(v);
  else return 1;
}

struct FD_POOL_WRITES pick_writes(fd_pool p,fdtype oids)
{
  struct FD_POOL_WRITES writes;
  if (FD_EMPTY_CHOICEP(oids)) {
    u8_zero_struct(writes);
    writes.len = 0;
    writes.oids = NULL;
    writes.values = NULL;
    return writes;}
  else {
    fd_hashtable changes = &(p->pool_changes);
    int n = FD_CHOICE_SIZE(oids);
    fdtype *oidv, *values;
    u8_zero_struct(writes);
    writes.len = n;
    writes.oids = oidv = u8_zalloc_n(n,fdtype);
    writes.values = values = u8_zalloc_n(n,fdtype);
    {FD_DO_CHOICES(oid,oids) {
        fd_pool pool = fd_oid2pool(oid);
        if (pool == p) {
          fdtype v = fd_hashtable_get(changes,oid,FD_VOID);
          if (v == FD_LOCKHOLDER) {}
          else if (savep(v,0)) {
            *oidv++=oid; *values++=v;}
          else fd_decref(v);}}}
    writes.len = oidv-writes.oids;
    return writes;}
}

struct FD_POOL_WRITES pick_modified(fd_pool p,int finished)
{
  struct FD_POOL_WRITES writes;
  fd_hashtable changes = &(p->pool_changes); int unlock = 0;
  if (changes->table_uselock) {
    u8_read_lock(&(changes->table_rwlock)); 
    unlock = 1;}
  int n = changes->table_n_keys;
  fdtype *oidv, *values;
  u8_zero_struct(writes);
  writes.len = n;
  writes.oids = oidv = u8_zalloc_n(n,fdtype);
  writes.values = values = u8_zalloc_n(n,fdtype);
  {
    struct FD_HASH_BUCKET **scan = changes->ht_buckets;
    struct FD_HASH_BUCKET **lim = scan+changes->ht_n_buckets;
    while (scan < lim) {
      if (*scan) {
        struct FD_HASH_BUCKET *e = *scan; int fd_n_entries = e->fd_n_entries;
        struct FD_KEYVAL *kvscan = &(e->kv_val0), *kvlimit = kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          fdtype key = kvscan->kv_key, val = kvscan->kv_val;
          if (val == FD_LOCKHOLDER) {kvscan++; continue;}
          else if (savep(val,finished)) {
            *oidv++=key; *values++=val; fd_incref(val);}
          else {}
          kvscan++;}
        scan++;}
      else scan++;}
  }
  if (unlock) u8_rw_unlock(&(changes->table_rwlock));
  writes.len = oidv-writes.oids;
  if (writes.len==0) {
    u8_free(writes.oids); writes.oids = NULL;
    u8_free(writes.values); writes.values = NULL;}
  return writes;
}

/* Operations on a single OID which operate on its pool */

FD_EXPORT int fd_lock_oid(fdtype oid)
{
  fd_pool p = fd_oid2pool(oid);
  return fd_pool_lock(p,oid);
}

FD_EXPORT int fd_unlock_oid(fdtype oid,int commit)
{
  fd_pool p = fd_oid2pool(oid);
  return fd_pool_unlock(p,oid,commit);
}


FD_EXPORT int fd_swapout_oid(fdtype oid)
{
  fd_pool p = fd_oid2pool(oid);
  if (p == NULL)
    return fd_reterr(fd_AnonymousOID,"SET-OID_VALUE!",NULL,oid);
  else if (p->pool_handler->swapout)
    return p->pool_handler->swapout(p,oid);
  else if (!(fd_hashtable_probe_novoid(&(p->pool_cache),oid)))
    return 0;
  else {
    fd_hashtable_store(&(p->pool_cache),oid,FD_VOID);
    return 1;}
}

/* Operations over choices of OIDs from different pools */

typedef int (*fd_pool_op)(fd_pool p,fdtype oids);

static int apply_poolop(fd_pool_op fn,fdtype oids_arg)
{
  fdtype oids = fd_make_simple_choice(oids_arg);
  if (FD_OIDP(oids)) {
    fd_pool p = fd_oid2pool(oids);
    int rv = (p)?(fn(p,oids)):(0);
    fd_decref(oids);
    return rv;}
  else {
    int n = FD_CHOICE_SIZE(oids), sum = 0;
    fdtype *allocd = u8_alloc_n(n,fdtype), *oidv = allocd, *write = oidv;
    {FD_DO_CHOICES(oid,oids) {
        if (FD_OIDP(oid)) *write++=oid;}}
    n = write-oidv;
    while (n>0) {
      fd_pool p = fd_oid2pool(oidv[0]);
      if (p == NULL) {oidv++; n--; continue;}
      else {
        fdtype oids_in_pool = oidv[0];
        fdtype *keep = oidv;
        int rv = 0, i = 1; while (i<n) {
          fdtype oid = oidv[i++];
          fd_pool pool = fd_oid2pool(oid);
          if (p == NULL) {}
          else if (pool == p) {
            FD_ADD_TO_CHOICE(oids_in_pool,oid);}
          else *keep++=oid;}
        oids_in_pool = fd_simplify_choice(oids_in_pool);
        if (FD_EMPTY_CHOICEP(oids_in_pool)) rv = 0;
        else rv = fn(p,oids_in_pool);
        if (rv<0) {}
        else sum = sum+rv;
        fd_decref(oids_in_pool);
        n = keep-oidv;}}
    fd_decref(oids);
    u8_free(allocd);
    return sum;}
}

FD_EXPORT int fd_lock_oids(fdtype oids_arg)
{
  return apply_poolop(fd_pool_lock,oids_arg);
}

static int commit_and_unlock(fd_pool p,fdtype oids)
{
  return fd_pool_unlock(p,oids,commit_modified);
}

static int unlock_unmodified(fd_pool p,fdtype oids)
{
  return fd_pool_unlock(p,oids,leave_modified);
}

static int unlock_and_discard(fd_pool p,fdtype oids)
{
  return fd_pool_unlock(p,oids,discard_modified);
}

FD_EXPORT int fd_unlock_oids(fdtype oids_arg,fd_storage_unlock_flag flags)
{
  switch (flags) {
  case commit_modified:
    return apply_poolop(commit_and_unlock,oids_arg);
  case leave_modified:
    return apply_poolop(unlock_unmodified,oids_arg);
  case discard_modified:
    return apply_poolop(unlock_and_discard,oids_arg);
  default:
    u8_seterr("UnknownUnlockFlag","fd_unlock_oids",NULL);
    return -1;}
}

FD_EXPORT int fd_swapout_oids(fdtype oids)
{
  return apply_poolop(fd_pool_swapout,oids);
}

FD_EXPORT
/* fd_prefetch_oids:
   Arguments: a dtype pointer to an oid or OID choice
   Returns:
   Sorts the OIDs by pool (ignoring non-oids) and executes
   a prefetch from each pool.
*/
int fd_prefetch_oids(fdtype oids)
{
  return apply_poolop(fd_pool_prefetch,oids);
}

FD_EXPORT int fd_commit_oids(fdtype oids)
{
  return apply_poolop(fd_pool_commit,oids);
}

FD_EXPORT int fd_finish_oids(fdtype oids)
{
  return apply_poolop(fd_pool_finish,oids);
}

/* Other pool operations */

FD_EXPORT int fd_pool_load(fd_pool p)
{
  if (p->pool_handler->getload)
    return (p->pool_handler->getload)(p);
  else {
    fd_seterr(fd_UnhandledOperation,"fd_pool_load",
              u8_strdup(p->poolid),FD_VOID);
    return -1;}
}

FD_EXPORT void fd_pool_close(fd_pool p)
{
  if ((p) && (p->pool_handler) && (p->pool_handler->close))
    p->pool_handler->close(p);
}

/* Callable versions of simple functions */

FD_EXPORT fd_pool _fd_oid2pool(fdtype oid)
{
  int baseid = FD_OID_BASE_ID(oid);
  int baseoff = FD_OID_BASE_OFFSET(oid);
  struct FD_POOL *top = fd_top_pools[baseid];
  if (top == NULL) return NULL;
  else if (baseoff<top->pool_capacity) return top;
  else if (top->pool_capacity) {
    u8_raise(_("Corrupted pool table"),"fd_oid2pool",NULL);
    return NULL;}
  else return fd_find_subpool((struct FD_GLUEPOOL *)top,oid);
}
FD_EXPORT fdtype fd_fetch_oid(fd_pool p,fdtype oid)
{
  FDTC *fdtc = ((FD_USE_THREADCACHE)?(fd_threadcache):(NULL));
  fdtype value;
  if (fdtc) {
    fdtype value = ((fdtc->oids.table_n_keys)?
                  (fd_hashtable_get(&(fdtc->oids),oid,FD_VOID)):
                  (FD_VOID));
    if (!(FD_VOIDP(value))) return value;}
  if (p == NULL) p = fd_oid2pool(oid);
  if (p == NULL) return fd_anonymous_oid("fd_fetch_oid",oid);
  else if (p == fd_zero_pool)
    return fd_zero_pool_value(oid);
  else if ( (p->pool_cache_level) &&
            (p->pool_changes.table_n_keys) &&
            (fd_hashtable_probe_novoid(&(p->pool_changes),oid)) ) {
    /* This is where the OID is 'locked' (has an entry in the changes
       table */
    value = fd_hashtable_get(&(p->pool_changes),oid,FD_VOID);
    if (value == FD_LOCKHOLDER) {
      value = fd_pool_fetch(p,oid);
      fd_hashtable_store(&(p->pool_changes),oid,value);
      return value;}
    else return value;}
  if (p->pool_cache_level)
    value = fd_hashtable_get(&(p->pool_cache),oid,FD_VOID);
  else value = FD_VOID;
  if (FD_VOIDP(value)) {
    if (fd_ipeval_delay(1)) {
      FD_ADD_TO_CHOICE(fd_pool_delays[p->pool_serialno],oid);
      return FD_EMPTY_CHOICE;}
    else value = fd_pool_fetch(p,oid);}
  if (FD_ABORTP(value)) return value;
  if (fdtc) fd_hashtable_store(&(fdtc->oids),oid,value);
  return value;
}

/* Pools to lisp and vice versa */

FD_EXPORT fdtype fd_pool2lisp(fd_pool p)
{
  if (p == NULL)
    return FD_ERROR_VALUE;
  else if (p->pool_serialno<0)
    return fd_incref((fdtype) p);
  else return FDTYPE_IMMEDIATE(fd_pool_type,p->pool_serialno);
}
FD_EXPORT fd_pool fd_lisp2pool(fdtype lp)
{
  if (FD_TYPEP(lp,fd_pool_type)) {
    int serial = FD_GET_IMMEDIATE(lp,fd_pool_type);
    if (serial<fd_n_pools)
      return fd_pools_by_serialno[serial];
    else {
      char buf[32];
      sprintf(buf,"serial = 0x%x",serial);
      fd_seterr3(fd_InvalidPoolPtr,"fd_lisp2pool",buf);
      return NULL;}}
  else if (FD_STRINGP(lp))
    return fd_use_pool(FD_STRDATA(lp),0,FD_VOID);
  else {
    fd_seterr(fd_TypeError,_("not a pool"),NULL,lp);
    return NULL;}
}

/* Iterating over pools */

FD_EXPORT int fd_for_pools(int (*fcn)(fd_pool,void *),void *data)
{
  int i = 0, pool_count = 0; fd_pool last_pool = NULL;
  while (i < 1024)
    if (fd_top_pools[i]==NULL) i++;
    else if (fd_top_pools[i]->pool_capacity) {
      fd_pool p = fd_top_pools[i++];
      if (p == last_pool) {}
      else if (fcn(p,data)) return pool_count+1;
      else {last_pool = p; pool_count++;}}
    else {
      struct FD_GLUEPOOL *gp = (struct FD_GLUEPOOL *)fd_top_pools[i++];
      fd_pool *subpools; int j = 0;
      subpools = gp->subpools;
      while (j<gp->n_subpools) {
        fd_pool p = subpools[j++];
        int retval = ((p == last_pool) ? (0) : (fcn(p,data)));
        last_pool = p;
        if (retval<0) return retval;
        else if (retval) return pool_count+1;
        else pool_count++;}}
  return pool_count;
}

FD_EXPORT fdtype fd_all_pools()
{
  fdtype results = FD_EMPTY_CHOICE; int i = 0;
  while (i < 1024)
    if (fd_top_pools[i]==NULL) i++;
    else if (fd_top_pools[i]->pool_capacity) {
      fdtype lp = fd_pool2lisp(fd_top_pools[i]);
      FD_ADD_TO_CHOICE(results,lp); i++;}
    else {
      struct FD_GLUEPOOL *gp = (struct FD_GLUEPOOL *)fd_top_pools[i++];
      fd_pool *subpools; int j = 0;
      subpools = gp->subpools;
      while (j<gp->n_subpools) {
        fdtype lp = fd_pool2lisp(subpools[j++]);
        FD_ADD_TO_CHOICE(results,lp);}}
  return results;
}

static int match_pool_source(fd_pool p,u8_string source)
{
  return ((source)&&(p->pool_source)&&
          (strcmp(p->pool_source,source)==0));
}

FD_EXPORT fd_pool fd_find_pool_by_source(u8_string source)
{
  int i = 0;
  if (source == NULL) return NULL;
  while (i < 1024)
    if (fd_top_pools[i] == NULL) i++;
    else if (fd_top_pools[i]->pool_capacity) {
      if (match_pool_source(fd_top_pools[i],source)) {
        return fd_top_pools[i];}
      else i++;}
    else {
      struct FD_GLUEPOOL *gp = (struct FD_GLUEPOOL *)fd_top_pools[i++];
      fd_pool *subpools = gp->subpools; int j = 0;
      while (j<gp->n_subpools)
        if ((subpools[j]) && (match_pool_source(subpools[j],source))) {
          return subpools[j];}
        else j++;}
  return NULL;
}

FD_EXPORT fdtype fd_find_pools_by_source(u8_string source)
{
  fdtype results = FD_EMPTY_CHOICE;
  int i = 0;
  if (source == NULL) return results;
  while (i < 1024)
    if (fd_top_pools[i] == NULL) i++;
    else if (fd_top_pools[i]->pool_capacity)
      if (match_pool_source(fd_top_pools[i],source)) {
        fdtype poolv = fd_pool2lisp(fd_top_pools[i]);
        fd_incref(poolv);
        FD_ADD_TO_CHOICE(results,poolv);
        i++;}
      else i++;
    else {
      struct FD_GLUEPOOL *gp = (struct FD_GLUEPOOL *)fd_top_pools[i++];
      fd_pool *subpools = gp->subpools; int j = 0;
      while (j<gp->n_subpools)
        if ((subpools[j]) && (match_pool_source(subpools[j],source))) {
          fdtype poolv = fd_pool2lisp(subpools[j]);
          fd_incref(poolv);
          FD_ADD_TO_CHOICE(results,poolv);
          j++;}
        else j++;}
  return results;
}

FD_EXPORT fd_pool fd_find_pool_by_prefix(u8_string prefix)
{
  int i = 0;
  while (i < 1024)
    if (fd_top_pools[i] == NULL) i++;
    else if (fd_top_pools[i]->pool_capacity)
      if (((fd_top_pools[i]->pool_prefix) &&
           ((strcasecmp(prefix,fd_top_pools[i]->pool_prefix)) == 0)) ||
          ((fd_top_pools[i]->pool_label) &&
           ((strcasecmp(prefix,fd_top_pools[i]->pool_label)) == 0)))
        return fd_top_pools[i];
      else i++;
    else {
      struct FD_GLUEPOOL *gp = (struct FD_GLUEPOOL *)fd_top_pools[i++];
      fd_pool *subpools = gp->subpools; int j = 0;
      while (j<gp->n_subpools)
        if (((subpools[j]->pool_prefix) &&
             ((strcasecmp(prefix,subpools[j]->pool_prefix)) == 0)) ||
            ((subpools[j]->pool_label) &&
             ((strcasecmp(prefix,subpools[j]->pool_label)) == 0))) {
          return subpools[j];}
        else j++;}
  return NULL;
}

/* Operations over all pools */

static int do_swapout(fd_pool p,void *data)
{
  fd_pool_swapout(p,FD_VOID);
  return 0;
}

FD_EXPORT int fd_swapout_pools()
{
  return fd_for_pools(do_swapout,NULL);
}

static int do_close(fd_pool p,void *data)
{
  fd_pool_close(p);
  return 0;
}

FD_EXPORT int fd_close_pools()
{
  return fd_for_pools(do_close,NULL);
}

static int do_commit(fd_pool p,void *data)
{
  int retval = fd_pool_unlock_all(p,1);
  if (retval<0)
    if (data) {
      u8_log(LOG_CRIT,"POOL_COMMIT_FAIL",
             "Error when committing pool %s",p->poolid);
      return 0;}
    else return -1;
  else return 0;
}

FD_EXPORT int fd_commit_pools()
{
  return fd_for_pools(do_commit,(void *)NULL);
}

FD_EXPORT int fd_commit_pools_noerr()
{
  return fd_for_pools(do_commit,(void *)"NOERR");
}

static int do_unlock(fd_pool p,void *data)
{
  int *commitp = (int *)data;
  fd_pool_unlock_all(p,*commitp);
  return 0;
}

FD_EXPORT int fd_unlock_pools(int commitp)
{
  return fd_for_pools(do_unlock,&commitp);
}

static int accumulate_cachecount(fd_pool p,void *ptr)
{
  int *count = (int *)ptr;
  *count = *count+p->pool_cache.table_n_keys;
  return 0;
}

FD_EXPORT
long fd_object_cache_load()
{
  int result = 0, retval;
  retval = fd_for_pools(accumulate_cachecount,(void *)&result);
  if (retval<0) return retval;
  else return result;
}

static int accumulate_cached(fd_pool p,void *ptr)
{
  fdtype *vals = (fdtype *)ptr;
  fdtype keys = fd_hashtable_keys(&(p->pool_cache));
  FD_ADD_TO_CHOICE(*vals,keys);
  return 0;
}

FD_EXPORT
fdtype fd_cached_oids(fd_pool p)
{
  if (p == NULL) {
    int retval; fdtype result = FD_EMPTY_CHOICE;
    retval = fd_for_pools(accumulate_cached,(void *)&result);
    if (retval<0) {
      fd_decref(result);
      return FD_ERROR_VALUE;}
    else return result;}
  else return fd_hashtable_keys(&(p->pool_cache));
}

/* Common pool initialization stuff */

FD_EXPORT void fd_init_pool(fd_pool p,FD_OID base,
                            unsigned int capacity,
                            struct FD_POOL_HANDLER *h,
                            u8_string source,u8_string cid)
{
  FD_INIT_CONS(p,fd_consed_pool_type);
  p->pool_base = base; p->pool_capacity = capacity;
  p->pool_serialno = -1; p->pool_cache_level = -1;
  p->pool_flags = 0;
  FD_INIT_STATIC_CONS(&(p->pool_cache),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(p->pool_changes),fd_hashtable_type);
  fd_make_hashtable(&(p->pool_cache),fd_pool_cache_init);
  fd_make_hashtable(&(p->pool_changes),fd_pool_lock_init);
  u8_init_mutex(&(p->pool_lock));
  p->pool_adjuncts = NULL;
  p->pool_adjuncts_len = 0;
  p->pool_n_adjuncts = 0;
  p->oid_handlers = NULL;
  p->pool_handler = h;
  p->pool_source = u8_strdup(source);
  p->poolid = u8_strdup(cid);
  p->pool_label = NULL;
  p->pool_prefix = NULL;
  p->pool_namefn = FD_VOID;
}

FD_EXPORT void fd_set_pool_namefn(fd_pool p,fdtype namefn)
{
  fdtype oldnamefn = p->pool_namefn;
  fd_incref(namefn);
  p->pool_namefn = namefn;
  fd_decref(oldnamefn);
}

/* GLUEPOOL handler (empty) */

static struct FD_POOL_HANDLER gluepool_handler={
  "gluepool", 1, sizeof(struct FD_GLUEPOOL), 11,
  NULL, /* close */
  NULL, /* alloc */
  NULL, /* fetch */
  NULL, /* fetchn */
  NULL, /* getload */
  NULL, /* lock */
  NULL, /* release */
  NULL, /* storen */
  NULL, /* swapout */
  NULL, /* metadata */
  NULL, /* create */
  NULL, /* walker */
  NULL, /* recycle */
  NULL  /* poolctl */
};


FD_EXPORT fd_pool fd_get_pool
(u8_string spec,fd_storage_flags flags,fdtype opts)
{
  if (strchr(spec,';')) {
    fd_pool p = NULL;
    u8_byte *copy = u8_strdup(spec), *start = copy;
    u8_byte *brk = strchr(start,';');
    while (brk) {
      if (p == NULL) {
        *brk='\0'; p = fd_open_pool(start,flags,opts);
        if (p) {brk = NULL; start = NULL;}
        else {
          start = brk+1;
          brk = strchr(start,';');}}}
    if (p) return p;
    else if ((start)&&(*start)) {
      int start_off = start-copy;
      u8_free(copy);
      return fd_open_pool(spec+start_off,flags,opts);}
    else return NULL;}
  else return fd_open_pool(spec,flags,opts);
}

FD_EXPORT fd_pool fd_use_pool
(u8_string spec,fd_storage_flags flags,fdtype opts)
{
  return fd_get_pool(spec,flags&(~FD_STORAGE_UNREGISTERED),opts);
}

FD_EXPORT fd_pool fd_name2pool(u8_string spec)
{
  fdtype label_string = fd_make_string(NULL,-1,spec);
  fdtype poolv = fd_hashtable_get(&poolid_table,label_string,FD_VOID);
  fd_decref(label_string);
  if (FD_VOIDP(poolv)) return NULL;
  else return fd_lisp2pool(poolv);
}

static void display_pool(u8_output out,fd_pool p,fdtype lp)
{
  char addrbuf[128], numbuf[32];
  u8_string type = ((p->pool_handler) && (p->pool_handler->name)) ?
    (p->pool_handler->name) : ((u8_string)"notype");
  u8_string tag = (FD_CONSP(lp)) ? ("CONSPOOL") : ("POOL");
  int n_cached=p->pool_cache.table_n_keys;
  int n_changed=p->pool_changes.table_n_keys;
  strcpy(addrbuf,"@");
  strcat(addrbuf,u8_uitoa16(FD_OID_HI(p->pool_base),numbuf));
  strcat(addrbuf,"/");
  strcat(addrbuf,u8_uitoa16(FD_OID_LO(p->pool_base),numbuf));
  strcat(addrbuf,"+0x0");
  strcat(addrbuf,u8_uitoa16(p->pool_capacity,numbuf));
  if ((p->pool_source)&&(p->pool_label))
    u8_printf(out,"#<%s %s (%s) %s oids=%d/%d #!%lx '%s' \"%s\">",
              tag,p->poolid,type,addrbuf,n_cached,n_changed,
              lp,p->pool_label,p->pool_source);
  else if (p->pool_label)
    u8_printf(out,"#<%s %s (%s) oids=%d/%d #!%lx '%s'>",
              tag,type,addrbuf,n_cached,n_changed,
              lp,p->pool_label);
  else if (p->pool_source)
    u8_printf(out,"#<%s %s (%s) %s oids=%d/%d #!%lx \"%s\">",
              tag,p->poolid,type,addrbuf,n_cached,n_changed,
              lp,p->pool_source);
  else u8_printf(out,"#<%s %s (%s) %s oids=%d/%d #!%lx \"%s\">",
                 tag,p->poolid,type,addrbuf,n_cached,n_changed,
                 lp);
}

static int unparse_pool(u8_output out,fdtype x)
{
  fd_pool p = fd_lisp2pool(x);
  if (p == NULL) return 0;
  display_pool(out,p,x);
  return 1;
}

static int unparse_consed_pool(u8_output out,fdtype x)
{
  fd_pool p = (fd_pool)x;
  if (p == NULL) return 0;
  display_pool(out,p,x);
  return 1;
}

static fdtype pool_parsefn(int n,fdtype *args,fd_compound_typeinfo e)
{
  fd_pool p = NULL;
  if (n<3) return FD_VOID;
  else if (n==3)
    p = fd_use_pool(FD_STRING_DATA(args[2]),0,FD_VOID);
  else if ((FD_STRINGP(args[2])) &&
           ((p = fd_find_pool_by_prefix(FD_STRING_DATA(args[2]))) == NULL))
    p = fd_use_pool(FD_STRING_DATA(args[3]),0,FD_VOID);
  if (p) return fd_pool2lisp(p);
  else return fd_err(fd_CantParseRecord,"pool_parsefn",NULL,FD_VOID);
}

/* Config methods */

static fdtype config_get_anonymousok(fdtype var,void *data)
{
  if (fd_ignore_anonymous_oids) return FD_TRUE;
  else return FD_TRUE;
}

static int config_set_anonymousok(fdtype var,fdtype val,void *data)
{
  if (FD_TRUEP(val)) fd_ignore_anonymous_oids = 1;
  else fd_ignore_anonymous_oids = 0;
  return 1;
}

/* Executing pool delays */

FD_EXPORT int fd_execute_pool_delays(fd_pool p,void *data)
{
  fdtype todo = fd_pool_delays[p->pool_serialno];
  if (FD_EMPTY_CHOICEP(todo)) return 0;
  else {
    /* u8_lock_mutex(&(fd_ipeval_lock)); */
    todo = fd_pool_delays[p->pool_serialno];
    fd_pool_delays[p->pool_serialno]=FD_EMPTY_CHOICE;
    todo = fd_simplify_choice(todo);
    /* u8_unlock_mutex(&(fd_ipeval_lock)); */
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval>1)
      u8_log(LOG_NOTICE,ipeval_objfetch,"Fetching %d oids from %s: %q",
             FD_CHOICE_SIZE(todo),p->poolid,todo);
    else if (fd_trace_ipeval)
      u8_log(LOG_NOTICE,ipeval_objfetch,"Fetching %d oids from %s",
             FD_CHOICE_SIZE(todo),p->poolid);
#endif
    fd_pool_prefetch(p,todo);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval)
      u8_log(LOG_NOTICE,ipeval_objfetch,"Fetched %d oids from %s",
             FD_CHOICE_SIZE(todo),p->poolid);
#endif
    return 0;}
}

/* Raw pool table operations */

static fdtype pool_get(fd_pool p,fdtype key,fdtype dflt)
{
  if (FD_OIDP(key)) {
    FD_OID addr = FD_OID_ADDR(key);
    FD_OID base = p->pool_base;
    if (FD_OID_COMPARE(addr,base)<0)
      return fd_incref(dflt);
    else {
      unsigned int offset = FD_OID_DIFFERENCE(addr,base);
      unsigned int capacity = p->pool_capacity;
      int load = fd_pool_load(p);
      if (load<0)
        return FD_ERROR_VALUE;
      else if ((p->pool_flags&FD_POOL_ADJUNCT) ?
               (offset<capacity) :
               (offset<load))
        return fd_fetch_oid(p,key);
      else return fd_incref(dflt);}}
  else return fd_incref(dflt);
}

static fdtype pool_tableget(fdtype arg,fdtype key,fdtype dflt)
{
  fd_pool p=fd_lisp2pool(arg);
  if (p)
    return pool_get(p,key,dflt);
  else return fd_incref(dflt);
}

FD_EXPORT fdtype fd_pool_get(fd_pool p,fdtype key)
{
  return pool_get(p,key,FD_EMPTY_CHOICE);
}

static int pool_store(fd_pool p,fdtype key,fdtype value)
{
  if (FD_OIDP(key)) {
    FD_OID addr = FD_OID_ADDR(key);
    FD_OID base = p->pool_base;
    if (FD_OID_COMPARE(addr,base)<0) {
      fd_seterr(fd_PoolRangeError,"pool_store",
                u8_strdup(fd_pool_id(p)),
                key);
      return -1;}
    else {
      unsigned int offset = FD_OID_DIFFERENCE(addr,base);
      int cap = p->pool_capacity, rv = -1;
      if (offset>cap) {
        fd_seterr(fd_PoolRangeError,"pool_store",
                  u8_strdup(fd_pool_id(p)),
                  key);
        return -1;}
      else if (p->pool_handler->lock == NULL) {
        rv = fd_hashtable_store(&(p->pool_cache),key,value);}
      else if (fd_pool_lock(p,key)) {
        rv = fd_hashtable_store(&(p->pool_changes),key,value);}
      else {
        fd_seterr(fd_CantLockOID,"pool_store",
                  u8_strdup(fd_pool_id(p)),
                  key);
        return -1;}
      return rv;}}
  else {
    fd_seterr(fd_NotAnOID,"pool_store",
              u8_strdup(fd_pool_id(p)),
              fd_incref(key));
    return -1;}
}

static fdtype pool_tablestore(fdtype arg,fdtype key,fdtype val)
{
  fd_pool p=fd_lisp2pool(arg);
  if (p) {
    int rv=pool_store(p,key,val);
    if (rv<0)
      return FD_ERROR_VALUE;
    else return FD_TRUE;}
  else return fd_type_error("pool","pool_tablestore",arg);
}

FD_EXPORT int fd_pool_store(fd_pool p,fdtype key,fdtype value)
{
  return pool_store(p,key,value);
}

FD_EXPORT fdtype fd_pool_keys(fdtype arg)
{
  fdtype results = FD_EMPTY_CHOICE;
  fd_pool p = fd_lisp2pool(arg);
  if (p==NULL)
    return fd_type_error("pool","pool_tablekeys",arg);
  else {
    FD_OID base = p->pool_base;
    unsigned int i = 0; int load = fd_pool_load(p);
    if (load<0) return FD_ERROR_VALUE;
    while (i<load) {
      fdtype each = fd_make_oid(FD_OID_PLUS(base,i));
      FD_ADD_TO_CHOICE(results,each);
      i++;}
    return results;}
}

static void recycle_consed_pool(struct FD_RAW_CONS *c)
{
  struct FD_POOL *p = (struct FD_POOL *)c;
  struct FD_POOL_HANDLER *handler = p->pool_handler;
  if (handler->recycle) handler->recycle(p);
  fd_recycle_hashtable(&(p->pool_cache));
  fd_recycle_hashtable(&(p->pool_changes));
  u8_free(p->poolid);
  u8_free(p->pool_source);
  if (p->pool_label) u8_free(p->pool_label);
  if (p->pool_prefix) u8_free(p->pool_prefix);
  fd_decref(p->pool_namefn); fd_decref(p->pool_namefn);
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

static fdtype copy_consed_pool(fdtype x,int deep)
{
  return x;
}

/* Initialization */

fd_ptr_type fd_consed_pool_type;

static int check_pool(fdtype x)
{
  int serial = FD_GET_IMMEDIATE(x,fd_pool_type);
  if (serial<0) return 0;
  else if (serial<fd_n_pools) return 1;
  else return 0;
}

/* This is just for use from the debugger, so we can allocate it
   statically. */
static char oid_info_buf[512];

static u8_string _more_oid_info(fdtype oid)
{
  if (FD_OIDP(oid)) {
    FD_OID addr = FD_OID_ADDR(oid);
    fd_pool p = fd_oid2pool(oid);
    unsigned int hi = FD_OID_HI(addr), lo = FD_OID_LO(addr);
    if (p == NULL)
      sprintf(oid_info_buf,"@%x/%x in no pool",hi,lo);
    else if ((p->pool_label)&&(p->pool_source))
      sprintf(oid_info_buf,"@%x/%x in %s from %s = %s",
              hi,lo,p->pool_label,p->pool_source,p->poolid);
    else if (p->pool_label)
      sprintf(oid_info_buf,"@%x/%x in %s",hi,lo,p->pool_label);
    else if (p->pool_source)
      sprintf(oid_info_buf,"@%x/%x from %s = %s",
              hi,lo,p->pool_source,p->poolid);
    else sprintf(oid_info_buf,"@%x/%x from %s",hi,lo,p->poolid);
    return oid_info_buf;}
  else return "not an oid!";
}


/* Config functions */

FD_EXPORT fdtype fd_poolconfig_get(fdtype var,void *vptr)
{
  fd_pool *pptr = (fd_pool *)vptr;
  if (*pptr == NULL)
    return fd_err(_("Config mis-config"),"pool_config_get",NULL,var);
  else {
    if (*pptr)
      return fd_pool2lisp(*pptr);
    else return FD_EMPTY_CHOICE;}
}
FD_EXPORT int fd_poolconfig_set(fdtype ignored,fdtype v,void *vptr)
{
  fd_pool *ppid = (fd_pool *)vptr;
  if (FD_POOLP(v)) *ppid = fd_lisp2pool(v);
  else if (FD_STRINGP(v)) {
    fd_pool p = fd_use_pool(FD_STRDATA(v),0,FD_VOID);
    if (p) *ppid = p; else return -1;}
  else return fd_type_error(_("pool spec"),"pool_config_set",v);
  return 1;
}

/* The zero pool */

struct FD_POOL _fd_zero_pool;
static u8_mutex zero_pool_alloc_lock;
fd_pool fd_default_pool = &_fd_zero_pool;

static fdtype zero_pool_alloc(fd_pool p,int n)
{
  int start = -1;
  u8_lock_mutex(&zero_pool_alloc_lock);
  start = fd_zero_pool_load;
  fd_zero_pool_load = start+n;
  u8_unlock_mutex(&zero_pool_alloc_lock);
  if (n==1) 
    return fd_make_oid(FD_MAKE_OID(0,start));
  else {
    struct FD_CHOICE *ch = fd_alloc_choice(n);
    FD_INIT_CONS(ch,fd_cons_type);
    ch->choice_size = n;
    fdtype *results = &(ch->choice_0);
    int i = 0; while (i<n) {
      FD_OID addr = FD_MAKE_OID(0,start+i);
      fdtype oid = fd_make_oid(addr);
      results[i]=oid;
      i++;}
    return (fdtype)fd_cleanup_choice(ch,FD_CHOICE_DOSORT);}
}

static int zero_pool_getload(fd_pool p)
{
  return fd_zero_pool_load;
}

static fdtype zero_pool_fetch(fd_pool p,fdtype oid)
{
  return fd_zero_pool_value(oid);
}

static fdtype *zero_pool_fetchn(fd_pool p,int n,fdtype *oids)
{
  fdtype *results = u8_alloc_n(n,fdtype);
  int i = 0; while (i<n) {
    results[i]=fd_zero_pool_value(oids[i]);
    i++;}
  return results;
}

static int zero_pool_lock(fd_pool p,fdtype oids)
{
  return 1;
}
static int zero_pool_unlock(fd_pool p,fdtype oids)
{
  return 0;
}

static struct FD_POOL_HANDLER zero_pool_handler={
  "zero_pool", 1, sizeof(struct FD_POOL), 12,
  NULL, /* close */
  zero_pool_alloc, /* alloc */
  zero_pool_fetch, /* fetch */
  zero_pool_fetchn, /* fetchn */
  zero_pool_getload, /* getload */
  zero_pool_lock, /* lock */
  zero_pool_unlock, /* release */
  NULL, /* storen */
  NULL, /* swapout */
  NULL, /* metadata */
  NULL, /* create */
  NULL,  /* walk */
  NULL,  /* recycle */
  NULL  /* poolctl */
};

static void init_zero_pool()
{
  FD_INIT_STATIC_CONS(&_fd_zero_pool,fd_pool_type);
  _fd_zero_pool.pool_serialno = -1;
  _fd_zero_pool.poolid = u8_strdup("_fd_zero_pool");
  _fd_zero_pool.pool_source = u8_strdup("init_fd_zero_pool");
  _fd_zero_pool.pool_label = u8_strdup("_fd_zero_pool");
  _fd_zero_pool.pool_base = 0;
  _fd_zero_pool.pool_capacity = 0x100000; /* About a million */
  _fd_zero_pool.pool_flags = 0;
  _fd_zero_pool.modified_flags = 0;
  _fd_zero_pool.pool_handler = &zero_pool_handler;
  _fd_zero_pool.pool_islocked = 0;
  u8_init_mutex(&(_fd_zero_pool.pool_lock));
  fd_init_hashtable(&(_fd_zero_pool.pool_cache),0,0);
  fd_init_hashtable(&(_fd_zero_pool.pool_changes),0,0);
  _fd_zero_pool.pool_n_adjuncts = 0;
  _fd_zero_pool.pool_adjuncts_len = 0;
  _fd_zero_pool.pool_adjuncts = NULL;
  _fd_zero_pool.oid_handlers = NULL;
  _fd_zero_pool.pool_prefix="";
  _fd_zero_pool.pool_namefn = FD_VOID;
  fd_register_pool(&_fd_zero_pool);
}

/* Lisp pointers to Pool pointers */

FD_EXPORT fd_pool _fd_get_poolptr(fdtype x)
{
  return fd_get_poolptr(x);
}

/* Initialization */

FD_EXPORT void fd_init_pools_c()
{
  int i = 0; while (i < 1024) fd_top_pools[i++]=NULL;

  u8_register_source_file(_FILEINFO);

  fd_type_names[fd_pool_type]=_("pool");
  fd_immediate_checkfns[fd_pool_type]=check_pool;

  fd_consed_pool_type = fd_register_cons_type("raw pool");

  fd_type_names[fd_consed_pool_type]=_("raw pool");

  _fd_oid_info=_more_oid_info;

  lock_symbol = fd_intern("LOCK");
  unlock_symbol = fd_intern("UNLOCK");

  memset(&fd_top_pools,0,sizeof(fd_top_pools));
  memset(&fd_pools_by_serialno,0,sizeof(fd_top_pools));

  {
    struct FD_COMPOUND_TYPEINFO *e =
      fd_register_compound(fd_intern("POOL"),NULL,NULL);
    e->fd_compound_parser = pool_parsefn;}

  FD_INIT_STATIC_CONS(&poolid_table,fd_hashtable_type);
  fd_make_hashtable(&poolid_table,-1);

#if (FD_USE_TLS)
  u8_new_threadkey(&fd_pool_delays_key,NULL);
#endif
  fd_unparsers[fd_pool_type]=unparse_pool;
  fd_unparsers[fd_consed_pool_type]=unparse_consed_pool;
  fd_recyclers[fd_consed_pool_type]=recycle_consed_pool;
  fd_copiers[fd_consed_pool_type]=copy_consed_pool;
  fd_register_config
    ("ANONYMOUSOK",_("whether value of anonymous OIDs are {} or signal an error"),
     config_get_anonymousok,
     config_set_anonymousok,NULL);
  fd_register_config("DEFAULTPOOL",_("Default location for new OID allocation"),
                     fd_poolconfig_get,fd_poolconfig_set,
                     &fd_default_pool);

  fd_tablefns[fd_pool_type]=u8_zalloc(struct FD_TABLEFNS);
  fd_tablefns[fd_pool_type]->get = (fd_table_get_fn)pool_tableget;
  fd_tablefns[fd_pool_type]->store = (fd_table_store_fn)pool_tablestore;
  fd_tablefns[fd_pool_type]->keys = (fd_table_keys_fn)fd_pool_keys;

  fd_tablefns[fd_consed_pool_type]=u8_zalloc(struct FD_TABLEFNS);
  fd_tablefns[fd_consed_pool_type]->get = (fd_table_get_fn)pool_tableget;
  fd_tablefns[fd_consed_pool_type]->store = (fd_table_store_fn)pool_tablestore;
  fd_tablefns[fd_consed_pool_type]->keys = (fd_table_keys_fn)fd_pool_keys;

#if FD_CALLTRACK_ENABLED
  {
    fd_calltrack_sensor cts = fd_get_calltrack_sensor("OIDS",1);
    cts->enabled = 1; cts->intfcn = fd_object_cache_load;}
#endif

  init_zero_pool();

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
