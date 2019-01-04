/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_POOLS 1
#define FD_INLINE_IPEVAL 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/storage.h"
#include "framerd/drivers.h"
#include "framerd/apply.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8netfns.h>
#include <libu8/u8printf.h>

u8_condition fd_PoolConflict=("Pool conflict");
u8_condition fd_CantLockOID=_("Can't lock OID");
u8_condition fd_CantUnlockOID=_("Can't unlock OID");
u8_condition fd_PoolRangeError=_("the OID is out of the range of the pool");
u8_condition fd_NoLocking=_("No locking available");
u8_condition fd_ReadOnlyPool=_("pool is read-only");
u8_condition fd_InvalidPoolPtr=_("Invalid pool PTR");
u8_condition fd_NotAFilePool=_("not a file pool");
u8_condition fd_NoFilePools=_("file pools are not supported");
u8_condition fd_AnonymousOID=_("no pool covers this OID");
u8_condition fd_NotAPool=_("Not a pool");
u8_condition fd_NoSuchPool=_("No such pool");
u8_condition fd_BadFilePoolLabel=_("file pool label is not a string");
u8_condition fd_PoolOverflow=_("pool load > capacity");
u8_condition fd_ExhaustedPool=_("pool has no more OIDs");
u8_condition fd_InvalidPoolRange=_("pool overlaps 0x100000 boundary");
u8_condition fd_PoolCommitError=_("can't save changes to pool");
u8_condition fd_UnregisteredPool=_("internal error with unregistered pool");
u8_condition fd_UnhandledOperation=_("This pool can't handle this operation");
u8_condition fd_DataFileOverflow=_("Data file is over implementation limit");

u8_condition fd_PoolCommit=_("Pool/Commit");

int fd_pool_cache_init = FD_POOL_CACHE_INIT;
int fd_pool_lock_init  = FD_POOL_CHANGES_INIT;

int fd_n_pools   = 0;
int fd_max_pools = FD_MAX_POOLS;
fd_pool fd_pools_by_serialno[FD_MAX_POOLS];

struct FD_POOL *fd_top_pools[FD_N_OID_BUCKETS];

static struct FD_HASHTABLE poolid_table;

static u8_condition ipeval_objfetch="OBJFETCH";

static lispval lock_symbol, unlock_symbol;

static int savep(lispval v,int only_finished);
static int modifiedp(lispval v);

static fd_pool *consed_pools = NULL;
ssize_t n_consed_pools = 0, consed_pools_len=0;
u8_mutex consed_pools_lock;

static int add_consed_pool(fd_pool p)
{
  u8_lock_mutex(&consed_pools_lock);
  fd_pool *scan=consed_pools, *limit=scan+n_consed_pools;
  while (scan<limit) {
    if ( *scan == p ) {
      u8_unlock_mutex(&consed_pools_lock);
      return 0;}
    else scan++;}
  if (n_consed_pools>=consed_pools_len) {
    ssize_t new_len=consed_pools_len+64;
    fd_pool *newvec=u8_realloc(consed_pools,new_len*sizeof(fd_pool));
    if (newvec) {
      consed_pools=newvec;
      consed_pools_len=new_len;}
    else {
      u8_seterr(fd_MallocFailed,"add_consed_pool",u8dup(p->poolid));
      u8_unlock_mutex(&consed_pools_lock);
      return -1;}}
  consed_pools[n_consed_pools++]=p;
  u8_unlock_mutex(&consed_pools_lock);
  return 1;
}

static int drop_consed_pool(fd_pool p)
{
  u8_lock_mutex(&consed_pools_lock);
  fd_pool *scan=consed_pools, *limit=scan+n_consed_pools;
  while (scan<limit) {
    if ( *scan == p ) {
      size_t to_shift=(limit-scan)-1;
      memmove(scan,scan+1,to_shift);
      n_consed_pools--;
      u8_unlock_mutex(&consed_pools_lock);
      return 1;}
    else scan++;}
  u8_unlock_mutex(&consed_pools_lock);
  return 0;
}

/* This is used in committing pools */

struct FD_POOL_WRITES {
  int len; lispval *oids, *values;};

/* Locking functions for external libraries */

FD_EXPORT void _fd_lock_pool_struct(fd_pool p,int for_write)
{
  fd_lock_pool_struct(p,for_write);
}

FD_EXPORT void _fd_unlock_pool_struct(fd_pool p)
{
  fd_unlock_pool_struct(p);
}

FD_FASTOP int modify_readonly(lispval table,int val)
{
  if (CONSP(table)) {
    fd_ptr_type table_type = FD_PTR_TYPE(table);
    switch (table_type) {
    case fd_slotmap_type: {
      struct FD_SLOTMAP *tbl=(fd_slotmap)table;
      tbl->table_readonly=val;
      return 1;}
    case fd_schemap_type: {
      struct FD_SCHEMAP *tbl=(fd_schemap)table;
      tbl->table_readonly=val;
      return 1;}
    case fd_hashtable_type: {
      struct FD_HASHTABLE *tbl=(fd_hashtable)table;
      tbl->table_readonly=val;
      return 1;}
    default: {
      fd_ptr_type typecode = FD_PTR_TYPE(table);
      struct FD_TABLEFNS *methods = fd_tablefns[typecode];
      if ( (methods) && (methods->readonly) )
        return (methods->readonly)(table,1);
      else return 0;}}}
  else return 0;
}

FD_FASTOP int modify_modified(lispval table,int val)
{
  if (CONSP(table)) {
    fd_ptr_type table_type = FD_PTR_TYPE(table);
    switch (table_type) {
    case fd_slotmap_type: {
      struct FD_SLOTMAP *tbl=(fd_slotmap)table;
      tbl->table_modified=val;
      return 1;}
    case fd_schemap_type: {
      struct FD_SCHEMAP *tbl=(fd_schemap)table;
      tbl->table_modified=val;
      return 1;}
    case fd_hashtable_type: {
      struct FD_HASHTABLE *tbl=(fd_hashtable)table;
      tbl->table_modified=val;
      return 1;}
    default: {
      fd_ptr_type typecode = FD_PTR_TYPE(table);
      struct FD_TABLEFNS *methods = fd_tablefns[typecode];
      if ( (methods) && (methods->modified) )
        return (methods->modified)(table,1);
      else return 0;}}}
  else return 0;
}

static int metadata_changed(fd_pool p)
{
  return (fd_modifiedp((lispval)(&(p->pool_metadata))));
}

/* Pool delays for IPEVAL */

#if FD_GLOBAL_IPEVAL
lispval *fd_pool_delays = NULL;
#elif FD_USE__THREAD
__thread lispval *fd_pool_delays = NULL;
#elif FD_USE_TLS
u8_tld_key fd_pool_delays_key;
#else
lispval *fd_pool_delays = NULL;
#endif

#if ((FD_USE_TLS) && (!(FD_GLOBAL_IPEVAL)))
FD_EXPORT lispval *fd_get_pool_delays()
{
  return (lispval *) u8_tld_get(fd_pool_delays_key);
}
FD_EXPORT void fd_init_pool_delays()
{
  int i = 0;
  lispval *delays = (lispval *) u8_tld_get(fd_pool_delays_key);
  if (delays) return;
  delays = u8_alloc_n(FD_N_POOL_DELAYS,lispval);
  while (i<FD_N_POOL_DELAYS) delays[i++]=EMPTY;
  u8_tld_set(fd_pool_delays_key,delays);
}
#else
FD_EXPORT lispval *fd_get_pool_delays()
{
  return fd_pool_delays;
}
FD_EXPORT void fd_init_pool_delays()
{
  int i = 0;
  if (fd_pool_delays) return;
  fd_pool_delays = u8_alloc_n(FD_N_POOL_DELAYS,lispval);
  while (i<FD_N_POOL_DELAYS) fd_pool_delays[i++]=EMPTY;
}
#endif

static struct FD_POOL_HANDLER gluepool_handler;
static void (*pool_conflict_handler)(fd_pool upstart,fd_pool holder) = NULL;

static void pool_conflict(fd_pool upstart,fd_pool holder);
static struct FD_GLUEPOOL *make_gluepool(FD_OID base);
static int add_to_gluepool(struct FD_GLUEPOOL *gp,fd_pool p);

FD_EXPORT fd_pool fd_open_network_pool(u8_string spec,fd_storage_flags flags);

static u8_mutex pool_registry_lock;

/* Pool ops */

FD_EXPORT lispval fd_pool_ctl(fd_pool p,lispval poolop,int n,lispval *args)
{
  struct FD_POOL_HANDLER *h = p->pool_handler;
  if (h->poolctl)
    return h->poolctl(p,poolop,n,args);
  else return FD_FALSE;
}

/* Pool caching */

FD_EXPORT void fd_pool_setcache(fd_pool p,int level)
{
  lispval intarg = FD_INT(level);
  lispval result = fd_pool_ctl(p,fd_cachelevel_op,1,&intarg);
  if (FD_ABORTP(result)) {fd_clear_errors(1);}
  fd_decref(result);
  p->pool_cache_level = level;
}

static void init_cache_level(fd_pool p)
{
  if (PRED_FALSE(p->pool_cache_level<0)) {
    lispval opts = p->pool_opts;
    long long level=fd_getfixopt(opts,"CACHELEVEL",fd_default_cache_level);
    fd_pool_setcache(p,level);}
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

static void register_pool_label(fd_pool p);

FD_EXPORT int fd_register_pool(fd_pool p)
{
  unsigned int capacity = p->pool_capacity, serial_no;
  int bix = fd_get_oid_base_index(p->pool_base,1);
  if (p->pool_serialno>=0)
    return 0;
  else if (bix<0)
    return bix;
  else if (p->pool_flags&FD_STORAGE_UNREGISTERED) {
    add_consed_pool(p);
    return 0;}
  else u8_lock_mutex(&pool_registry_lock);
  if (fd_n_pools >= fd_max_pools) {
    fd_seterr(_("n_pools > MAX_POOLS"),"fd_register_pool",NULL,VOID);
    u8_unlock_mutex(&pool_registry_lock);
    return -1;}
  if ((capacity>=FD_OID_BUCKET_SIZE) &&
      ((p->pool_base)%FD_OID_BUCKET_SIZE)) {
    fd_seterr(fd_InvalidPoolRange,"fd_register_pool",p->poolid,VOID);
    u8_unlock_mutex(&pool_registry_lock);
    return -1;}
  /* Set up the serial number */
  serial_no = p->pool_serialno = fd_n_pools++;
  fd_pools_by_serialno[serial_no]=p;
  /* Make it static (as a cons) */
  FD_SET_REFCOUNT(p,0);

  if (p->pool_flags&FD_POOL_ADJUNCT) {
    /* Adjunct pools don't get stored in the pool lookup table */
    u8_unlock_mutex(&pool_registry_lock);
    return 1;}
  else if (capacity>=FD_OID_BUCKET_SIZE) {
    /* This is the case where the pool spans several OID buckets */
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
    if (p->pool_capacity == FD_OID_BUCKET_SIZE)
      fd_top_pools[bix]=p;
    else {
      /* If the pool is smaller than an OID bucket, and there isn't a
         pool in fd_top_pools, create a gluepool and place it there */
      struct FD_GLUEPOOL *gluepool = make_gluepool(fd_base_oids[bix]);
      fd_top_pools[bix]=(struct FD_POOL *)gluepool;
      if (add_to_gluepool(gluepool,p)<0) {
        u8_unlock_mutex(&pool_registry_lock);
        return -1;}}}
  else if (fd_top_pools[bix]->pool_capacity) {
    /* If the top pool has a capacity (i.e. it's not a gluepool), we
       have a pool conflict. Complain and error. */
    pool_conflict(p,fd_top_pools[bix]);
    u8_unlock_mutex(&pool_registry_lock);
    return -1;}
  /* Otherwise, it is a gluepool, so try to add the pool to it. */
  else if (add_to_gluepool((struct FD_GLUEPOOL *)fd_top_pools[bix],p)<0) {
    /* If this fails, the registration fails. Note that we've still left
       the pool registered, so we can't free it. */
    u8_unlock_mutex(&pool_registry_lock);
    return -1;}
  u8_unlock_mutex(&pool_registry_lock);
  if (p->pool_label) register_pool_label(p);
  return 1;
}

static void register_pool_label(fd_pool p)
{
  u8_string base = u8_string_subst(p->pool_label,"/","_"), dot;
  lispval full = lispval_string(base);
  lispval lisp_arg = fd_pool2lisp(p);
  lispval probe = fd_hashtable_get(&poolid_table,full,EMPTY);
  if (EMPTYP(probe))
    fd_hashtable_store(&poolid_table,full,fd_pool2lisp(p));
  else {
    fd_pool conflict = fd_lisp2pool(probe);
    if (conflict != p) {
      u8_log(LOG_WARN,"PoolLabelConflict",
             "The label '%s' is already associated "
             "with the pool\n    %q\n rather than %q",
             base,conflict,lisp_arg);}
    fd_decref(full); fd_decref(probe); u8_free(base);
    return;}
  if ((dot=strchr(base,'.'))) {
    lispval prefix = fd_substring(base,dot);
    lispval prefix_probe = fd_hashtable_get(&poolid_table,prefix,EMPTY);
    if (EMPTYP(prefix_probe)) {
      fd_hashtable_store(&poolid_table,prefix,fd_pool2lisp(p));}
    fd_decref(prefix);
    fd_decref(prefix_probe);}
  fd_decref(full);
  fd_decref(probe);
  u8_free(base);
}

static struct FD_GLUEPOOL *make_gluepool(FD_OID base)
{
  struct FD_GLUEPOOL *pool = u8_alloc(struct FD_GLUEPOOL);
  pool->pool_base = base; pool->pool_capacity = 0;
  pool->pool_flags = FD_STORAGE_READ_ONLY;
  pool->pool_serialno = -1;
  pool->pool_label =
    u8_mkstring("gluepool(@%lx/%lx)",FD_OID_HI(base),FD_OID_LO(base));
  pool->pool_source = NULL;
  pool->pool_typeid = u8_strdup("gluepool");
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
    pools[0]=p;
    gp->n_subpools = 1;
    gp->subpools = pools;
    return 1;}
  else {
    int comparison = 0;
    struct FD_POOL **prev = gp->subpools;
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
    read = gp->subpools;
    top = gp->subpools+gp->n_subpools;
    while (read<top)
      if (write == ipoint) {
        *write++=p;
        *write++= *read++;}
      else *write++ = *read++;
    if (write == ipoint)
      *write = p;
    gp->subpools = new;
    gp->n_subpools++;
    u8_free(prev);}
  return 1;
}

FD_EXPORT u8_string fd_pool_id(fd_pool p)
{
  if (p->pool_label!=NULL)
    return p->pool_label;
  else if (p->poolid!=NULL)
    return p->poolid;
  else if (p->pool_source!=NULL)
    return p->pool_source;
  else return NULL;
}

FD_EXPORT u8_string fd_pool_label(fd_pool p)
{
  if (p->pool_label!=NULL)
    return p->pool_label;
  else return NULL;
}

/* Finding the subpool */

FD_EXPORT fd_pool fd_find_subpool(struct FD_GLUEPOOL *gp,lispval oid)
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
    u8_logf(LOG_WARN,fd_PoolConflict,
            "%s (from %s) and existing pool %s (from %s)\n",
            upstart->pool_label,upstart->pool_source,
            holder->pool_label,holder->pool_source);
    u8_seterr(_("Pool confict"),NULL,NULL);}
}


/* Basic functions on single OID values */

FD_EXPORT lispval fd_oid_value(lispval oid)
{
  if (EMPTYP(oid))
    return oid;
  else if (OIDP(oid)) {
    lispval v = fd_fetch_oid(NULL,oid);
    if (FD_ABORTP(v))
      return v;
    else if (v==FD_UNALLOCATED_OID) {
      fd_pool p = fd_oid2pool(oid);
      if (p)
        fd_seterr(fd_UnallocatedOID,"fd_oid_value",(p->poolid),oid);
      else fd_seterr(fd_UnallocatedOID,"fd_oid_value",(p->poolid),oid);
      return FD_ERROR_VALUE;}
    else return v;}
  else return fd_type_error(_("OID"),"fd_oid_value",oid);
}

FD_EXPORT lispval fd_locked_oid_value(fd_pool p,lispval oid)
{
  if (PRED_FALSE(!(OIDP(oid))))
    return fd_type_error(_("OID"),"fd_locked_oid_value",oid);
  else if ( (p->pool_handler->lock == NULL) ||
            (U8_BITP(p->pool_flags,FD_POOL_VIRTUAL)) ||
            (U8_BITP(p->pool_flags,FD_POOL_NOLOCKS)) ) {
    return fd_fetch_oid(p,oid);}
  else if (p == fd_zero_pool)
    return fd_zero_pool_value(oid);
  else {
    lispval smap = fd_hashtable_get(&(p->pool_changes),oid,VOID);
    if (VOIDP(smap)) {
      int retval = fd_pool_lock(p,oid);
      if (retval<0)
        return FD_ERROR;
      else if (retval) {
        lispval v = fd_fetch_oid(p,oid);
        if (FD_ABORTP(v)) {
          fd_seterr("FetchFailed","fd_locked_oid_value",p->poolid,oid);
          return v;}
        else if (v==FD_UNALLOCATED_OID) {
          fd_pool p = fd_oid2pool(oid);
          fd_seterr(fd_UnallocatedOID,"fd_locked_oid_value",
                    (p)?(p->poolid):((u8_string)"no pool"),oid);
          return FD_ERROR_VALUE;}
        modify_readonly(v,0);
        fd_hashtable_store(&(p->pool_changes),oid,v);
        return v;}
      else return fd_err(fd_CantLockOID,"fd_locked_oid_value",
                         p->pool_source,oid);}
    else if (smap == FD_LOCKHOLDER) {
      lispval v = fd_fetch_oid(p,oid);
      if (FD_ABORTP(v)) {
        fd_seterr("FetchFailed","fd_locked_oid_value",p->poolid,oid);
        return v;}
      modify_readonly(v,0);
      fd_hashtable_store(&(p->pool_changes),oid,v);
      return v;}
    else return smap;}
}

FD_EXPORT int fd_set_oid_value(lispval oid,lispval value)
{
  fd_pool p = fd_oid2pool(oid);
  if (p == NULL)
    return fd_reterr(fd_AnonymousOID,"SET-OID-VALUE!",NULL,oid);
  else if (p == fd_zero_pool)
    return fd_zero_pool_store(oid,value);
  else {
    modify_readonly(value,0);
    modify_modified(value,1);
    if ( (p->pool_handler->lock == NULL) ||
         (U8_BITP(p->pool_flags,FD_POOL_VIRTUAL)) ||
         (U8_BITP(p->pool_flags,FD_POOL_NOLOCKS)) ) {
      fd_hashtable_store(&(p->pool_cache),oid,value);
      return 1;}
    else if (fd_lock_oid(oid)) {
      fd_hashtable_store(&(p->pool_changes),oid,value);
      return 1;}
    else return fd_reterr(fd_CantLockOID,"SET-OID-VALUE!",NULL,oid);}
}

FD_EXPORT int fd_replace_oid_value(lispval oid,lispval value)
{
  fd_pool p = fd_oid2pool(oid);
  if (p == NULL)
    return fd_reterr(fd_AnonymousOID,"REPLACE-OID-VALUE!",NULL,oid);
  else if (p == fd_zero_pool)
    return fd_zero_pool_store(oid,value);
  else {
    if ( (p->pool_handler->lock == NULL) ||
         (U8_BITP(p->pool_flags,FD_POOL_VIRTUAL)) ||
         (U8_BITP(p->pool_flags,FD_POOL_NOLOCKS)) ) {
      fd_hashtable_store(&(p->pool_cache),oid,value);
      return 1;}
    else if (fd_hashtable_probe(&(p->pool_changes),oid)) {
      fd_hashtable_store(&(p->pool_changes),oid,value);
      return 1;}
    else {
      fd_hashtable_store(&(p->pool_cache),oid,value);
      return 1;}}
}

/* Fetching OID values */

FD_EXPORT lispval fd_pool_fetch(fd_pool p,lispval oid)
{
  lispval v = VOID;
  init_cache_level(p);
  if (p == fd_zero_pool)
    return fd_zero_pool_value(oid);
  else if (p->pool_handler->fetch)
    v = p->pool_handler->fetch(p,oid);
  else if (p->pool_cache_level)
    v = fd_hashtable_get(&(p->pool_cache),oid,EMPTY);
  else {}
  if (FD_ABORTP(v)) return v;
  else if (p->pool_cache_level == 0)
    return v;
  /* If it's locked, store it in the locks table */
  else if ( (p->pool_changes.table_n_keys) &&
            (fd_hashtable_op(&(p->pool_changes),
                             fd_table_replace_novoid,oid,v)) )
    return v;
  else if ( (p->pool_handler->lock == NULL) ||
            (U8_BITP(p->pool_flags,FD_POOL_VIRTUAL)) ||
            (U8_BITP(p->pool_flags,FD_POOL_NOLOCKS)) )
    modify_readonly(v,0);
  else modify_readonly(v,1);
  if ( ( (p->pool_cache_level) > 0) &&
       ( ! ( (p->pool_flags) & (FD_STORAGE_VIRTUAL) ) ) )
    fd_hashtable_store(&(p->pool_cache),oid,v);
  return v;
}

FD_EXPORT int fd_pool_prefetch(fd_pool p,lispval oids)
{
  FDTC *fdtc = ((FD_USE_THREADCACHE)?(fd_threadcache):(NULL));
  struct FD_HASHTABLE *oidcache = ((fdtc!=NULL)?(&(fdtc->oids)):(NULL));
  int decref_oids = 0, cachelevel;
  if (p == NULL) {
    fd_seterr(fd_NotAPool,"fd_pool_prefetch","NULL pool ptr",VOID);
    return -1;}
  else if (EMPTYP(oids))
    return 0;
  else init_cache_level(p);
  int nolock=
    ( (p->pool_handler->lock == NULL) ||
      (U8_BITP(p->pool_flags,FD_POOL_VIRTUAL)) ||
      (U8_BITP(p->pool_flags,FD_POOL_NOLOCKS)) );
  cachelevel = p->pool_cache_level;
  /* It doesn't make sense to prefetch if you're not caching. */
  if (cachelevel<1) return 0;
  if (p->pool_handler->fetchn == NULL) {
    if (fd_ipeval_delay(FD_CHOICE_SIZE(oids))) {
      CHOICE_ADD(fd_pool_delays[p->pool_serialno],oids);
      return 0;}
    else {
      int n_fetches = 0;
      DO_CHOICES(oid,oids) {
        lispval v = fd_pool_fetch(p,oid);
        if (FD_ABORTP(v)) {
          FD_STOP_DO_CHOICES; return v;}
        n_fetches++; fd_decref(v);}
      return n_fetches;}}
  if (PRECHOICEP(oids)) {
    oids = fd_make_simple_choice(oids);
    decref_oids = 1;}
  if (fd_ipeval_status()) {
    FD_HASHTABLE *cache = &(p->pool_cache); int n_to_fetch = 0;
    /* lispval oidschoice = fd_make_simple_choice(oids); */
    lispval *delays = &(fd_pool_delays[p->pool_serialno]);
    DO_CHOICES(oid,oids)
      if (fd_hashtable_probe_novoid(cache,oid)) {}
      else {
        CHOICE_ADD(*delays,oid);
        n_to_fetch++;}
    (void)fd_ipeval_delay(n_to_fetch);
    /* fd_decref(oidschoice); */
    return 0;}
  else if (CHOICEP(oids)) {
    struct FD_HASHTABLE *cache = &(p->pool_cache);
    struct FD_HASHTABLE *changes = &(p->pool_changes);
    int n = FD_CHOICE_SIZE(oids);
    /* We use n_locked to track how many of the OIDs are in
       pool_changes (locked). */
    int n_locked = (changes->table_n_keys)?(0):(-1);
    lispval *values, *oidv = u8_big_alloc_n(n,lispval), *write = oidv;
    DO_CHOICES(o,oids)
      if (((oidcache == NULL)||(fd_hashtable_probe_novoid(oidcache,o)==0))&&
          (fd_hashtable_probe_novoid(cache,o)==0) &&
          (fd_hashtable_probe(changes,o)==0))
        /* If it's not in the oid cache, the pool cache, or the changes, get it */
        *write++=o;
      else if ((n_locked>=0)&&
               (fd_hashtable_op(changes,fd_table_test,o,FD_LOCKHOLDER))) {
        /* If it's in the changes but not loaded, save it for loading and not
           that some of the results should be put in the changes rather than
           the cache */
        *write++=o; n_locked++;}
      else {}
    if (write == oidv) {
      /* Nothing to prefetch, free and return */
      u8_big_free(oidv);
      if (decref_oids) fd_decref(oids);
      return 0;}
    else n = write-oidv;
    /* Call the pool handler */
    values = p->pool_handler->fetchn(p,n,oidv);
    /* If you got results, store them in the cache */
    if (values) {
      if (nolock) {
        /* If the pool doesn't have to be locked, don't bother locking
           any values */}
      else if (n_locked>0) {
        /* If some values are locked, we consider each value and
           store it in the appropriate tables (changes or cache). */
        int j = 0; while (j<n) {
          lispval v = values[j], oid = oidv[j];
          /* Try to replace it in the changes table, and only store it
             in the cache if it's not there. Also update the readonly
             bit accordingly. */
          if (fd_hashtable_op(changes,fd_table_replace_novoid,oid,v)==0) {
            /* This is when the OID we're storing isn't locked */
            modify_readonly(v,1);
            fd_hashtable_op(cache,fd_table_store,oid,v);}
          else modify_readonly(v,0);
          if (fdtc) fd_hashtable_op(&(fdtc->oids),fd_table_store,oid,v);
          /* We decref it since it would have been incref'd when stored. */
          fd_decref(values[j]);
          j++;}}
      else {
        /* If no values are locked, make them all readonly */
        int j = 0; while (j<n) {
          lispval v=values[j];
          modify_readonly(v,1);
          j++;}
        if (fdtc) fd_hashtable_iter(oidcache,fd_table_store,n,oidv,values);
        /* Store them all in the cache */
        fd_hashtable_iter(cache,fd_table_store_noref,n,oidv,values);}}
    else {
      u8_big_free(oidv);
      if (decref_oids) fd_decref(oids);
      return -1;}
    u8_big_free(oidv);
    u8_big_free(values);
    if (decref_oids) fd_decref(oids);
    return n;}
  else {
    lispval v = p->pool_handler->fetch(p,oids);
    if (FD_ABORTP(v)) return v;
    fd_hashtable changes = &(p->pool_changes);
    if ( (changes->table_n_keys==0) ||
         /* This will store it in changes if it's already there */
         (fd_hashtable_op(changes,fd_table_replace_novoid,oids,v)==0) ) {}
    else if ( ( (p->pool_cache_level) > 0) &&
              ( ! ( (p->pool_flags) & (FD_STORAGE_VIRTUAL) ) ) )
      fd_hashtable_store(&(p->pool_cache),oids,v);
    else {}
    if (fdtc) fd_hashtable_op(&(fdtc->oids),fd_table_store,oids,v);
    fd_decref(v);
    return 1;}
}

/* Swapping out OIDs */

FD_EXPORT int fd_pool_swapout(fd_pool p,lispval oids)
{
  fd_hashtable cache = &(p->pool_cache);
  if (PRECHOICEP(oids)) {
    lispval simple = fd_make_simple_choice(oids);
    int rv = fd_pool_swapout(p,simple);
    fd_decref(simple);
    return rv;}
  else if (VOIDP(oids)) {
    int rv = cache->table_n_keys;
    if ((p->pool_flags)&(FD_STORAGE_KEEP_CACHESIZE))
      fd_reset_hashtable(cache,-1,1);
    else fd_reset_hashtable(cache,fd_pool_cache_init,1);
    return rv;}
  else if ((OIDP(oids))||(CHOICEP(oids)))
    u8_logf(LOG_GLUT,"SwapPool",_("Swapping out %d oids in pool %s"),
            FD_CHOICE_SIZE(oids),p->poolid);
  else if (PRECHOICEP(oids))
    u8_logf(LOG_GLUT,"SwapPool",_("Swapping out ~%d oids in pool %s"),
            FD_PRECHOICE_SIZE(oids),p->poolid);
  else u8_logf(LOG_GLUT,"SwapPool",_("Swapping out oids in pool %s"),p->poolid);
  int rv = -1;
  double started = u8_elapsed_time();
  if (p->pool_handler->swapout) {
    p->pool_handler->swapout(p,oids);
    u8_logf(LOG_DETAIL,"SwapPool",
            "Finished custom swapout for pool %s, clearing caches...",
            p->poolid);}
  else u8_logf(LOG_GLUT,"SwapPool",
               "No custom swapout clearing caches for %s",p->poolid);
  if (p->pool_flags&FD_STORAGE_NOSWAP)
    return 0;
  else if (OIDP(oids)) {
    fd_hashtable_op(cache,fd_table_replace,oids,VOID);
    rv = 1;}
  else if (CHOICEP(oids)) {
    rv = FD_CHOICE_SIZE(oids);
    fd_hashtable_iterkeys(cache,fd_table_replace,
                          FD_CHOICE_SIZE(oids),FD_CHOICE_DATA(oids),
                          VOID);
    fd_devoid_hashtable(cache,0);}
  else {
    rv = cache->table_n_keys;
    if ((p->pool_flags)&(FD_STORAGE_KEEP_CACHESIZE))
      fd_reset_hashtable(cache,-1,1);
    else fd_reset_hashtable(cache,fd_pool_cache_init,1);}
  u8_logf(LOG_DETAIL,"SwapPool",
          "Swapped out %d oids from pool '%s' in %f",
          rv,p->poolid,u8_elapsed_time()-started);
  return rv;
}


/* Allocating OIDs */

FD_EXPORT lispval fd_pool_alloc(fd_pool p,int n)
{
  lispval result = p->pool_handler->alloc(p,n);
  if (p == fd_zero_pool)
    return result;
  else if ( (p->pool_flags) & (FD_STORAGE_VIRTUAL) )
    return result;
  else if (OIDP(result)) {
    if (p->pool_handler->lock)
      fd_hashtable_store(&(p->pool_changes),result,EMPTY);
    else fd_hashtable_store(&(p->pool_cache),result,EMPTY);
    return result;}
  else if (CHOICEP(result)) {
    if (p->pool_handler->lock)
      fd_hashtable_iterkeys(&(p->pool_changes),fd_table_store,
                            FD_CHOICE_SIZE(result),FD_CHOICE_DATA(result),
                            EMPTY);
    else fd_hashtable_iterkeys(&(p->pool_cache),fd_table_store,
                               FD_CHOICE_SIZE(result),FD_CHOICE_DATA(result),
                               EMPTY);
    return result;}
  else if (FD_ABORTP(result)) return result;
  else if (FD_EXCEPTIONP(result)) return result;
  else return fd_err("BadDriverResult","fd_pool_alloc",
                     u8_strdup(p->pool_source),VOID);
}

/* Locking and unlocking OIDs within pools */

FD_EXPORT int fd_pool_lock(fd_pool p,lispval oids)
{
  int decref_oids = 0;
  struct FD_HASHTABLE *locks = &(p->pool_changes);
  if ( (p->pool_handler->lock == NULL) ||
       (U8_BITP(p->pool_flags,FD_POOL_VIRTUAL)) ||
       (U8_BITP(p->pool_flags,FD_POOL_NOLOCKS)) )
    return 0;
  if (PRECHOICEP(oids)) {
    oids = fd_make_simple_choice(oids);
    decref_oids = 1;}
  if (CHOICEP(oids)) {
    lispval needy; int retval, n; lispval temp_oidv[1];
    struct FD_CHOICE *oidc = fd_alloc_choice(FD_CHOICE_SIZE(oids));
    lispval *oidv = (lispval *)FD_XCHOICE_DATA(oidc), *write = oidv;
    DO_CHOICES(o,oids)
      if (fd_hashtable_probe(locks,o)==0) *write++=o;
    if (decref_oids) fd_decref(oids);
    if (write == oidv) {
      /* Nothing to lock, free and return */
      fd_free_choice(oidc);
      return 1;}
    else n = write-oidv;
    if (p->pool_handler->lock == NULL) {
      fd_seterr(fd_CantLockOID,"fd_pool_lock",p->poolid,oids);
      return -1;}
    if (n==1) {
      needy=temp_oidv[0]=oidv[0];
      fd_free_choice(oidc);
      oidv=temp_oidv;}
    else needy = fd_init_choice(oidc,n,NULL,FD_CHOICE_ISATOMIC);
    retval = p->pool_handler->lock(p,needy);
    if (retval<0) {
      fd_decref(needy);
      return retval;}
    else if (retval) {
      fd_hashtable_iterkeys(&(p->pool_cache),fd_table_replace,n,oidv,VOID);
      fd_hashtable_iterkeys(locks,fd_table_store,n,oidv,FD_LOCKHOLDER);}
    fd_decref(needy);
    return retval;}
  else if (fd_hashtable_probe(locks,oids)==0) {
    int retval = p->pool_handler->lock(p,oids);
    if (decref_oids) fd_decref(oids);
    if (retval<0) return retval;
    else if (retval) {
      fd_hashtable_op(&(p->pool_cache),fd_table_replace,oids,VOID);
      fd_hashtable_op(locks,fd_table_store,oids,FD_LOCKHOLDER);
      return 1;}
    else return 0;}
  else {
    if (decref_oids) fd_decref(oids);
    return 1;}
}

FD_EXPORT int fd_pool_unlock(fd_pool p,lispval oids,
                             fd_storage_unlock_flag flags)
{
  struct FD_HASHTABLE *changes = &(p->pool_changes);
  if (changes->table_n_keys==0)
    return 0;
  else if (flags<0) {
    int n = changes->table_n_keys;
    if (p->pool_handler->unlock) {
      lispval to_unlock = fd_hashtable_keys(changes);
      p->pool_handler->unlock(p,to_unlock);
      fd_decref(to_unlock);}
    fd_reset_hashtable(changes,-1,1);
    return -n;}
  else {
    int n_unlocked = 0, n_committed = (flags>0) ? (fd_commit_pool(p,oids)) : (0);
    lispval to_unlock = EMPTY;
    lispval locked_oids = fd_hashtable_keys(changes);
    if (n_committed<0) {}
    {DO_CHOICES(oid,locked_oids) {
        fd_pool pool = fd_oid2pool(oid);
        if (p == pool) {
          lispval v = fd_hashtable_get(changes,oid,VOID);
          if ( (!(VOIDP(v))) && (!(modifiedp(v)))) {
            CHOICE_ADD(to_unlock,oid);}}}}
    fd_decref(locked_oids);
    to_unlock = fd_simplify_choice(to_unlock);
    n_unlocked = FD_CHOICE_SIZE(to_unlock);
    if (p->pool_handler->unlock)
      p->pool_handler->unlock(p,to_unlock);
    if (OIDP(to_unlock))
      fd_hashtable_op(changes,fd_table_replace,to_unlock,VOID);
    else {
      fd_hashtable_iterkeys(changes,fd_table_replace,
                            FD_CHOICE_SIZE(to_unlock),
                            FD_CHOICE_DATA(to_unlock),
                            VOID);}
    fd_devoid_hashtable(changes,0);
    fd_decref(to_unlock);
    return n_unlocked+n_committed;}
}

FD_EXPORT int fd_pool_unlock_all(fd_pool p,fd_storage_unlock_flag flags)
{
  return fd_pool_unlock(p,FD_FALSE,flags);
}

/* Declare locked OIDs 'finished' (read-only) */

FD_EXPORT int fd_pool_finish(fd_pool p,lispval oids)
{
  int finished = 0;
  fd_hashtable changes = &(p->pool_changes);
  DO_CHOICES(oid,oids) {
    fd_pool pool = fd_oid2pool(oid);
    if (p == pool) {
      lispval v = fd_hashtable_get(changes,oid,VOID);
      if (CONSP(v)) {
        if (SLOTMAPP(v)) {
          if (FD_SLOTMAP_MODIFIEDP(v)) {
            FD_SLOTMAP_MARK_FINISHED(v);
            finished++;}}
        else if (SCHEMAPP(v)) {
          if (FD_SCHEMAP_MODIFIEDP(v)) {
            FD_SCHEMAP_MARK_FINISHED(v);
            finished++;}}
        else if (HASHTABLEP(v)) {
          if (FD_HASHTABLE_MODIFIEDP(v)) {
            FD_HASHTABLE_MARK_FINISHED(v);
            finished++;}}
        else {}
        fd_decref(v);}}}
  return finished;
}

static int rollback_commits(fd_pool p,struct FD_POOL_COMMITS *commits)
{
  u8_logf(LOG_ERR,"Rollback","commit of %d OIDs%s to %s",
          commits->commit_count,
          ((FD_VOIDP(commits->commit_metadata)) ? ("") : (" and metadata") ),
          p->poolid);
  int rv = p->pool_handler->commit(p,fd_commit_rollback,commits);
  if (rv<0)
    u8_logf(LOG_CRIT,"Rollback/Failed","commit of %d OIDs%s to %s",
            commits->commit_count,
            ((FD_VOIDP(commits->commit_metadata)) ? ("") : (" and metadata") ),
            p->poolid);
  commits->commit_phase = fd_commit_flush;
  return rv;
}

/* Timing */

#define record_elapsed(loc) \
  ((loc=(u8_elapsed_time()-(mark))),(mark=u8_elapsed_time()))

/* Committing OIDs to external sources */

static int pick_writes(fd_pool p,lispval oids,struct FD_POOL_COMMITS *commits);
static int pick_modified(fd_pool p,int finished,struct FD_POOL_COMMITS *commits);
static void abort_commit(fd_pool p,struct FD_POOL_COMMITS *commits);
static int finish_commit(fd_pool p,struct FD_POOL_COMMITS *commits);

static int pool_docommit(fd_pool p,lispval oids,
                         struct FD_POOL_COMMITS *use_commits)
{
  struct FD_HASHTABLE *locks = &(p->pool_changes);
  int fd_storage_loglevel = (p->pool_loglevel>=0) ? (p->pool_loglevel) :
    (*fd_storage_loglevel_ptr);

  if ( (use_commits == NULL) &&
       (locks->table_n_keys==0) &&
       (! (metadata_changed(p)) )) {
    u8_logf(LOG_DEBUG,fd_PoolCommit,"No locked oids in %s",p->poolid);
    return 0;}
  else if (p->pool_handler->commit == NULL) {
    u8_logf(LOG_DEBUG,fd_PoolCommit,
            "No commit handler, just unlocking OIDs in %s",p->poolid);
    int rv = fd_pool_unlock(p,oids,leave_modified);
    return rv;}
  else {
    int free_commits = 1;
    struct FD_POOL_COMMITS commits = { 0 };

    u8_lock_mutex(&(p->pool_commit_lock));
    if (use_commits) {
      memcpy(&commits,use_commits,sizeof(struct FD_POOL_COMMITS));
      if (commits.commit_vals)
        fd_incref_vec(commits.commit_vals,commits.commit_count);
      free_commits=0;}
    else commits.commit_pool = p;

    if (commits.commit_metadata == FD_NULL)
      commits.commit_metadata = FD_VOID;

    double start_time = u8_elapsed_time(), mark = start_time;
    commits.commit_times.base = start_time;

    u8_logf(LOG_DEBUG,"PoolCommit/Start","Starting to commit %s",p->poolid);
    int started = p->pool_handler->commit(p,fd_commit_start,&commits);
    record_elapsed(commits.commit_times.start);

    if ( (started < 0) || (commits.commit_count < 0) ) {
      u8_graberrno("pool_commit",u8_strdup(p->poolid));
      u8_logf(LOG_WARN,"PoolCommit/Start",
              "Failed starting to commit %s",p->poolid);
      u8_unlock_mutex(&(p->pool_commit_lock));
      return started;}

    if (commits.commit_count) {
      /* Either use_commits or fd_commit_start initialized the
         commit_count, so we won't touch it. */}
    else if (! (locks->table_n_keys) ) {
      /* No locked OIDs to commit */
      u8_logf(LOG_DEBUG,"PoolCommit/Nada",
              "No locked OIDs to commit to %s",p->poolid);}
    else if ((FALSEP(oids))||(VOIDP(oids))) {
      /* Commit all the modified OIDS */
      pick_modified(p,0,&commits);
      if (commits.commit_count)
        u8_logf(LOG_DEBUG,"PoolCommit/modified",
                "%d modified OIDs to commit to %s",
                commits.commit_count,p->poolid);}
    else if ((OIDP(oids))||(CHOICEP(oids))||(PRECHOICEP(oids))) {
      /* Commit the designated OIDs (if modified) */
      pick_writes(p,oids,&commits);
      if (commits.commit_count)
        u8_logf(LOG_DEBUG,"PoolCommit/specified",
                "%d/%d modified OIDs to commit to %s",
                commits.commit_count,FD_CHOICE_SIZE(oids),
                p->poolid);}
    else if (FD_TRUEP(oids)) {
      /* Commit all the modified OIDs which are also 'finished,' which
         means they've been marked readonly. */
      pick_modified(p,1,&commits);
      if (commits.commit_count)
        u8_logf(LOG_DEBUG,"PoolCommit/finalized",
                "%d modified+finished OIDs to commit to %s",
                commits.commit_count,p->poolid);}
    else pick_writes(p,EMPTY,&commits);

    if (use_commits) {
      /* Fix a NULL metadata slot if you've been handled it. */
      if (commits.commit_metadata == FD_NULL)
        commits.commit_metadata = FD_VOID;}
    /* Otherwise, copy the current metadata if it's been modified. */
    else if (metadata_changed(p))
      commits.commit_metadata = fd_deep_copy((lispval)&(p->pool_metadata));
    else commits.commit_metadata = FD_VOID;

    /* We've now figured out everything we're going to save */
    record_elapsed(commits.commit_times.setup);

    int w_metadata = FD_SLOTMAPP(commits.commit_metadata);
    int commit_count = commits.commit_count;
    int written, synced, rollback = 0;

    if ( (commits.commit_count == 0) &&
         (FD_VOIDP(commits.commit_metadata)) ) {
      /* There's nothing to do */
      commits.commit_times.write     = 0;
      commits.commit_times.sync = 0;
      commits.commit_times.flush    = 0;
      written = 0;}
    else {
      written = p->pool_handler->commit(p,fd_commit_write,&commits);}

    if (written >= 0)
      u8_logf(LOG_DEBUG,"PoolCommit/Written","%d OIDs%s to %s",
              commit_count,((w_metadata) ? (" and metadata") : ("") ),
              p->poolid);

    if (written < 0) {
      u8_logf(LOG_ERR,"PoolCommit/WriteFailed",
              "Couldn't write %d OIDs%s to %s, rolling back any changes",
              commit_count,((w_metadata) ? (" and metadata") : ("") ),
              p->poolid);
      rollback = rollback_commits(p,&commits);
      synced = -1;}
    else if (commits.commit_phase == fd_commit_sync) {
      synced = p->pool_handler->commit(p,fd_commit_sync,&commits);
      if (synced < 0) {
        u8_logf(LOG_ERR,"PoolCommit/SyncFailed",
                "Couldn't sync %d commits%s to %s",
                commit_count,((w_metadata) ? (" and metadata") : ("") ),
                p->poolid);
        rollback = rollback_commits(p,&commits);}
      else u8_logf(LOG_DEBUG,"PoolCommit/Synced","%d OIDs%s to %s",
                   commit_count,((w_metadata) ? (" and metadata") : ("") ),
                   p->poolid);}
    else synced = 0;
    record_elapsed(commits.commit_times.sync);

    if (rollback<0)
      u8_logf(LOG_CRIT,"Rollback/Failed",
              _("Couldn't rollback %d changes%s to %s"),
              commit_count,((w_metadata) ? (" and metadata") : ("") ),
              p->poolid);

    if (use_commits == NULL)
      u8_logf(LOG_DEBUG,"PoolCommit/Flushing",
              "cached changes for %d OIDs%s written to %s",
              commit_count,((w_metadata) ? (" and metadata") : ("") ),
              p->poolid);
    int flushed = p->pool_handler->commit(p,fd_commit_flush,&commits);
    if (flushed<0)
      u8_logf(LOG_WARN,"PoolCommit/Flush/Failed",
              "Couldn't flush DB state for %d OIDs%s written to %s",
              commit_count,((w_metadata) ? (" and metadata") : ("") ),
              p->poolid);
    else u8_logf(LOG_DEBUG,"PoolCommit/Flushed",
                 "DB state for %d OIDs%s written to %s",
                 commit_count,((w_metadata) ? (" and metadata") : ("") ),
                 p->poolid);

    if (use_commits == NULL) {
      int finished=0;
      if (synced < 0)
        abort_commit(p,&commits);
      else {
        u8_logf(LOG_DETAIL,"PoolCommit/Unlocking",
                "Unlocking and flushing changes for %d OIDs%s written to %s",
                commit_count,((w_metadata) ? (" and metadata") : ("") ),
                p->poolid);
        finished = finish_commit(p,&commits);}
      if (finished < 0)
        u8_logf(LOG_WARN,"PoolCommit/Unlock/Failure",
                "couldn't unlock changes for %d OIDs%s written to %s",
                commit_count,((w_metadata) ? (" and metadata") : ("") ),
                p->poolid);
      else u8_logf(LOG_DETAIL,"PoolCommit/Unlocked",
                   "Unlocked and flushed changes for %d OIDs%s written to %s",
                   commit_count,((w_metadata) ? (" and metadata") : ("") ),
                   p->poolid);}
    record_elapsed(commits.commit_times.flush);

    u8_logf(LOG_DEBUG,"PoolCommit/Cleanup",
            "Cleaning up after saving %d OIDs%s to %s",
            commit_count,((w_metadata) ? (" and metadata") : ("") ),
            p->poolid);
    int cleanup = p->pool_handler->commit(p,fd_commit_cleanup,&commits);
    record_elapsed(commits.commit_times.cleanup);

    if (cleanup < 0) {
      u8_seterr("CleanupFailed","pool_commit",u8_strdup(p->poolid));
      u8_logf(LOG_WARN,"CleanupFailed","Cleanup for %s failed",p->poolid);}

    if (synced < 0)
      u8_logf(LOG_WARN,"Pool/Commit/Failed",
              "Couldn't commit %d OIDs%s to %s after %f secs",
              commit_count,((w_metadata) ? (" and metadata") : ("") ),
              p->poolid,u8_elapsed_time()-start_time);

    u8_logf(LOG_INFO,
            ((sync<0) ? ("Pool/Commit/Timing") : ("Pool/Commit/Complete")),
            "%s %d OIDs%s to '%s' in %fs\n"
            "total=%f, start=%f, setup=%f, save=%f, "
            "finalize=%f, apply=%f, cleanup=%f",
            ((sync<0) ? ("for") : ("Committed")),
            commits.commit_count,
            ((w_metadata) ? (" and metadata") : ("") ),
            p->poolid,
            u8_elapsed_time()-commits.commit_times.base,
            u8_elapsed_time()-commits.commit_times.base,
            commits.commit_times.start,
            commits.commit_times.setup,
            commits.commit_times.write,
            commits.commit_times.sync,
            commits.commit_times.flush,
            commits.commit_times.cleanup);

    if (free_commits) {
      if (commits.commit_oids) {
        u8_big_free(commits.commit_oids);
        commits.commit_oids=NULL;}
      else {
        u8_big_free(commits.commit_oids);
        commits.commit_oids=NULL;}
      if (commits.commit_vals) {
        u8_big_free(commits.commit_vals);
        commits.commit_vals=NULL;}
      else {
        u8_big_free(commits.commit_vals);
        commits.commit_vals=NULL;}
      fd_decref(commits.commit_metadata);}

    commits.commit_times.cleanup = u8_elapsed_time();

    u8_unlock_mutex(&(p->pool_commit_lock));

    return written;}
}

FD_EXPORT int fd_commit_pool(fd_pool p,lispval oids)
{
  return pool_docommit(p,oids,NULL);
}

FD_EXPORT int fd_commit_all_oids(fd_pool p)
{
  return fd_commit_pool(p,FD_FALSE);
}

FD_EXPORT int fd_commit_finished_oids(fd_pool p)
{
  return fd_commit_pool(p,FD_TRUE);
}

/* Commitment commitment and recovery */

static void abort_commit(fd_pool p,struct FD_POOL_COMMITS *commits)
{
  lispval *scan = commits->commit_vals, *limit = scan+commits->commit_count;
  while (scan<limit) {
    lispval v = *scan++;
    if (CONSP(v)) {
      modify_modified(v,1);
      fd_decref(v);}}
  u8_big_free(commits->commit_vals);
  commits->commit_vals = NULL;
}

static int finish_commit(fd_pool p,struct FD_POOL_COMMITS *commits)
{
  fd_hashtable changes = &(p->pool_changes);
  int i = 0, n = commits->commit_count, unlock_count=0;
  lispval *oids = commits->commit_oids;
  lispval *values = commits->commit_vals;
  lispval *unlock = oids;
  u8_write_lock(&(changes->table_rwlock));
  FD_HASH_BUCKET **buckets = changes->ht_buckets;
  int n_buckets = changes->ht_n_buckets;
  while (i<n) {
    lispval oid = oids[i];
    lispval v   = values[i];
    struct FD_KEYVAL *kv=fd_hashvec_get(oid,buckets,n_buckets);
    if (kv==NULL) {i++; continue;}
    else if (kv->kv_val != v) {
      i++;
      fd_decref(v);
      continue;}
    else if (!(CONSP(v))) {
      kv->kv_val=VOID;
      *unlock++=oids[i++];
      continue;}
    else {
      int finished=1;
      lispval cur = kv->kv_val;
      if (SLOTMAPP(v)) {
        if (FD_SLOTMAP_MODIFIEDP(v)) finished=0;}
      else if (SCHEMAPP(v)) {
        if (FD_SCHEMAP_MODIFIEDP(v)) finished=0;}
      else if (HASHTABLEP(v)) {
        if (FD_HASHTABLE_MODIFIEDP(v)) finished=0;}
      else {}
      /* We "swap out" the value (free, dereference, and unlock it) if:
         1. it hasn't been modified since we picked it (finished) and
         2. the only pointers to it are in *values and the hashtable
         (refcount<=2) */
      if ((finished) && (FD_CONS_REFCOUNT(v)<=2)) {
        *unlock++=oids[i];
        values[i]=VOID;
        kv->kv_val=VOID;
        fd_decref(cur);}
      fd_decref(v);
      i++;}}
  u8_rw_unlock(&(changes->table_rwlock));
  unlock_count = unlock-oids;
  if ((p->pool_handler->unlock)&&(unlock_count)) {
    lispval to_unlock = fd_init_choice
      (NULL,unlock_count,oids,FD_CHOICE_ISATOMIC);
    int retval = p->pool_handler->unlock(p,to_unlock);
    if (retval<0) {
      u8_logf(LOG_CRIT,"UnlockFailed",
              "Error unlocking pool %s, all %d values saved "
              "but up to %d OIDs may still be locked",
              p->poolid,n,unlock_count);
      fd_decref(to_unlock);
      fd_clear_errors(1);
      return retval;}
    fd_decref(to_unlock);}
  fd_devoid_hashtable(changes,0);
  return unlock_count;
}

/* Support for committing OIDs */

static int savep(lispval v,int only_finished)
{
  if (!(CONSP(v))) return 1;
  else if (SLOTMAPP(v)) {
    if (FD_SLOTMAP_MODIFIEDP(v)) {
      if ((!(only_finished))||(FD_SLOTMAP_FINISHEDP(v))) {
        FD_SLOTMAP_CLEAR_MODIFIED(v);
        return 1;}
      else return 0;}
    else return 0;}
  else if (SCHEMAPP(v)) {
    if (FD_SCHEMAP_MODIFIEDP(v)) {
      if ((!(only_finished))||(FD_SCHEMAP_FINISHEDP(v))) {
        FD_SCHEMAP_CLEAR_MODIFIED(v);
        return 1;}
      else return 0;}
    else return 0;}
  else if (HASHTABLEP(v)) {
    if (FD_HASHTABLE_MODIFIEDP(v)) {
      if ((!(only_finished))||(FD_HASHTABLE_READONLYP(v))) {
        FD_HASHTABLE_CLEAR_MODIFIED(v);
        return 1;}
      else return 0;}
    else return 0;}
  else if (only_finished) return 0;
  else return 1;
}

static int modifiedp(lispval v)
{
  if (!(CONSP(v))) return 1;
  else if (SLOTMAPP(v))
    return FD_SLOTMAP_MODIFIEDP(v);
  else if (SCHEMAPP(v))
    return FD_SCHEMAP_MODIFIEDP(v);
  else if (HASHTABLEP(v))
    return FD_HASHTABLE_MODIFIEDP(v);
  else return 1;
}

static int pick_writes(fd_pool p,lispval oids,struct FD_POOL_COMMITS *commits)
{
  if (EMPTYP(oids)) {
    commits->commit_count = 0;
    commits->commit_oids = NULL;
    commits->commit_vals = NULL;
    return 0;}
  else {
    fd_hashtable changes = &(p->pool_changes);
    int n = FD_CHOICE_SIZE(oids);
    lispval *oidv, *values;
    commits->commit_count = n;
    commits->commit_oids = oidv = u8_big_alloc_n(n,lispval);
    commits->commit_vals = values = u8_big_alloc_n(n,lispval);
    {DO_CHOICES(oid,oids) {
        fd_pool pool = fd_oid2pool(oid);
        if (pool == p) {
          lispval v = fd_hashtable_get(changes,oid,VOID);
          if (v == FD_LOCKHOLDER) {}
          else if (savep(v,0)) {
            *oidv++=oid;
            *values++=v;}
          else fd_decref(v);}}}
    commits->commit_count = oidv-commits->commit_oids;
    return commits->commit_count;}
}

static int pick_modified(fd_pool p,int finished,
                         struct FD_POOL_COMMITS *commits)
{
  fd_hashtable changes = &(p->pool_changes); int unlock = 0;
  if (changes->table_uselock) {
    u8_read_lock(&(changes->table_rwlock));
    unlock = 1;}
  int n = changes->table_n_keys;
  lispval *oidv, *values;
  commits->commit_count = n;
  commits->commit_oids = oidv = u8_big_alloc_n(n,lispval);
  commits->commit_vals = values = u8_big_alloc_n(n,lispval);
  {
    struct FD_HASH_BUCKET **scan = changes->ht_buckets;
    struct FD_HASH_BUCKET **lim = scan+changes->ht_n_buckets;
    while (scan < lim) {
      if (*scan) {
        struct FD_HASH_BUCKET *e = *scan;
        int bucket_len = e->bucket_len;
        struct FD_KEYVAL *kvscan = &(e->kv_val0);
        struct FD_KEYVAL *kvlimit = kvscan+bucket_len;
        while (kvscan<kvlimit) {
          lispval key = kvscan->kv_key, val = kvscan->kv_val;
          if (val == FD_LOCKHOLDER) {
            kvscan++;
            continue;}
          else if (savep(val,finished)) {
            *oidv++=key;
            *values++=val;
            fd_incref(val);}
          else {}
          kvscan++;}
        scan++;}
      else scan++;}
  }
  if (unlock) u8_rw_unlock(&(changes->table_rwlock));
  commits->commit_count = oidv-commits->commit_oids;
  return commits->commit_count;
}

/* Operations on a single OID which operate on its pool */

FD_EXPORT int fd_lock_oid(lispval oid)
{
  fd_pool p = fd_oid2pool(oid);
  return fd_pool_lock(p,oid);
}

FD_EXPORT int fd_unlock_oid(lispval oid,int commit)
{
  fd_pool p = fd_oid2pool(oid);
  return fd_pool_unlock(p,oid,commit);
}


FD_EXPORT int fd_swapout_oid(lispval oid)
{
  fd_pool p = fd_oid2pool(oid);
  if (p == NULL)
    return fd_reterr(fd_AnonymousOID,"SET-OID_VALUE!",NULL,oid);
  else if (p->pool_handler->swapout)
    return p->pool_handler->swapout(p,oid);
  else if (!(fd_hashtable_probe_novoid(&(p->pool_cache),oid)))
    return 0;
  else {
    fd_hashtable_store(&(p->pool_cache),oid,VOID);
    return 1;}
}

/* Operations over choices of OIDs from different pools */

typedef int (*fd_pool_op)(fd_pool p,lispval oids);

static int apply_poolop(fd_pool_op fn,lispval oids_arg)
{
  lispval oids = fd_make_simple_choice(oids_arg);
  if (OIDP(oids)) {
    fd_pool p = fd_oid2pool(oids);
    int rv = (p)?(fn(p,oids)):(0);
    fd_decref(oids);
    return rv;}
  else {
    int n = FD_CHOICE_SIZE(oids), sum = 0;
    lispval *allocd = u8_alloc_n(n,lispval), *oidv = allocd, *write = oidv;
    {DO_CHOICES(oid,oids) {
        if (OIDP(oid)) *write++=oid;}}
    n = write-oidv;
    while (n>0) {
      fd_pool p = fd_oid2pool(oidv[0]);
      if (p == NULL) {oidv++; n--; continue;}
      else {
        lispval oids_in_pool = oidv[0];
        lispval *keep = oidv;
        int rv = 0, i = 1; while (i<n) {
          lispval oid = oidv[i++];
          fd_pool pool = fd_oid2pool(oid);
          if (p == NULL) {}
          else if (pool == p) {
            CHOICE_ADD(oids_in_pool,oid);}
          else *keep++=oid;}
        oids_in_pool = fd_simplify_choice(oids_in_pool);
        if (EMPTYP(oids_in_pool)) rv = 0;
        else rv = fn(p,oids_in_pool);
        if (rv>0)
          sum = sum+rv;
        fd_decref(oids_in_pool);
        n = keep-oidv;}}
    fd_decref(oids);
    u8_free(allocd);
    return sum;}
}

FD_EXPORT int fd_lock_oids(lispval oids_arg)
{
  return apply_poolop(fd_pool_lock,oids_arg);
}

static int commit_and_unlock(fd_pool p,lispval oids)
{
  return fd_pool_unlock(p,oids,commit_modified);
}

static int unlock_unmodified(fd_pool p,lispval oids)
{
  return fd_pool_unlock(p,oids,leave_modified);
}

static int unlock_and_discard(fd_pool p,lispval oids)
{
  return fd_pool_unlock(p,oids,discard_modified);
}

FD_EXPORT int fd_unlock_oids(lispval oids_arg,fd_storage_unlock_flag flags)
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

FD_EXPORT int fd_swapout_oids(lispval oids)
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
int fd_prefetch_oids(lispval oids)
{
  return apply_poolop(fd_pool_prefetch,oids);
}

FD_EXPORT int fd_commit_oids(lispval oids)
{
  return apply_poolop(fd_commit_pool,oids);
}

FD_EXPORT int fd_finish_oids(lispval oids)
{
  return apply_poolop(fd_pool_finish,oids);
}

/* Other pool operations */

FD_EXPORT int fd_pool_load(fd_pool p)
{
  if (p->pool_handler->getload)
    return (p->pool_handler->getload)(p);
  else {
    fd_seterr(fd_UnhandledOperation,"fd_pool_load",p->poolid,VOID);
    return -1;}
}

FD_EXPORT void fd_pool_close(fd_pool p)
{
  if ((p) && (p->pool_handler) && (p->pool_handler->close))
    p->pool_handler->close(p);
}

/* Callable versions of simple functions */

FD_EXPORT fd_pool _fd_oid2pool(lispval oid)
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
FD_EXPORT lispval fd_fetch_oid(fd_pool p,lispval oid)
{
  FDTC *fdtc = ((FD_USE_THREADCACHE)?(fd_threadcache):(NULL));
  lispval value;
  if (fdtc) {
    lispval value = ((fdtc->oids.table_n_keys)?
                     (fd_hashtable_get(&(fdtc->oids),oid,VOID)):
                     (VOID));
    if (!(VOIDP(value))) return value;}
  if (p == NULL) p = fd_oid2pool(oid);
  if (p == NULL)
    return EMPTY;
  else if (p == fd_zero_pool)
    return fd_zero_pool_value(oid);
  else if ( (p->pool_cache_level) &&
            (p->pool_changes.table_n_keys) &&
            (fd_hashtable_probe_novoid(&(p->pool_changes),oid)) ) {
    /* This is where the OID is 'locked' (has an entry in the changes
       table */
    value = fd_hashtable_get(&(p->pool_changes),oid,VOID);
    if (value == FD_LOCKHOLDER) {
      value = fd_pool_fetch(p,oid);
      if (FD_ABORTP(value)) {
        fd_seterr("FetchFailed","fd_fetch_oid",p->poolid,oid);
        return value;}
      if (FD_CONSP(value)) modify_readonly(value,0);
      if ( ! ( (p->pool_flags) & (FD_STORAGE_VIRTUAL) ) )
        fd_hashtable_store(&(p->pool_changes),oid,value);
      return value;}
    else return value;}
  if (p->pool_cache_level)
    value = fd_hashtable_get(&(p->pool_cache),oid,VOID);
  else value = VOID;
  if (VOIDP(value)) {
    if (fd_ipeval_delay(1)) {
      CHOICE_ADD(fd_pool_delays[p->pool_serialno],oid);
      return EMPTY;}
    else value = fd_pool_fetch(p,oid);}
  if (FD_ABORTP(value)) {
    fd_seterr("FetchFailed","fd_fetch_oid",p->poolid,oid);
    return value;}
  if (fdtc) fd_hashtable_store(&(fdtc->oids),oid,value);
  return value;
}

FD_EXPORT int fd_pool_storen(fd_pool p,int n,lispval *oids,lispval *values)
{
  if (n==0)
    return 0;
  else if (p==NULL)
    return 0;
  else if ((p->pool_handler) && (p->pool_handler->commit)) {
    struct FD_POOL_COMMITS commits = {0};
    commits.commit_pool = p;
    commits.commit_count = n;
    commits.commit_oids = oids;
    commits.commit_vals = values;
    commits.commit_metadata = FD_VOID;
    int rv = pool_docommit(p,FD_VOID,&commits);
    return rv;}
  else if (p->pool_handler) {
    u8_seterr("NoHandler","fd_pool_storen",u8_strdup(p->poolid));
    return -1;}
  else return 0;
}

FD_EXPORT lispval fd_pool_fetchn(fd_pool p,lispval oids_arg)
{
  if (p==NULL) return FD_EMPTY;
  else if ((p->pool_handler) && (p->pool_handler->fetchn)) {
    lispval oids = fd_make_simple_choice(oids_arg);
    if (FD_VECTORP(oids)) {
      int n = FD_VECTOR_LENGTH(oids);
      lispval *oidv = FD_VECTOR_ELTS(oids);
      lispval *values = p->pool_handler->fetchn(p,n,oidv);
      fd_decref(oids);
      return fd_cons_vector(NULL,n,1,values);}
    else {
      int n = FD_CHOICE_SIZE(oids);
      if (n==0)
        return fd_make_hashtable(NULL,8);
      else if (n==1) {
        lispval table=fd_make_hashtable(NULL,16);
        lispval value = fd_fetch_oid(p,oids);
        fd_store(table,oids,value);
        fd_decref(value);
        fd_decref(oids);
        return table;}
      else {
        init_cache_level(p);
        const lispval *oidv=FD_CHOICE_DATA(oids);
        const lispval *values=p->pool_handler->fetchn(p,n,(lispval *)oidv);
        lispval table=fd_make_hashtable(NULL,n*3);
        fd_hashtable_iter((fd_hashtable)table,fd_table_store_noref,n,
                          oidv,values);
        u8_big_free((lispval *)values);
        fd_decref(oids);
        return table;}}}
  else if (p->pool_handler) {
    u8_seterr("NoHandler","fd_pool_fetchn",u8_strdup(p->poolid));
    return FD_ERROR;}
  else return FD_EMPTY;
}

/* Pools to lisp and vice versa */

FD_EXPORT lispval fd_pool2lisp(fd_pool p)
{
  if (p == NULL)
    return FD_ERROR;
  else if (p->pool_serialno<0)
    return fd_incref((lispval) p);
  else return LISPVAL_IMMEDIATE(fd_pool_type,p->pool_serialno);
}
FD_EXPORT fd_pool fd_lisp2pool(lispval lp)
{
  if (TYPEP(lp,fd_pool_type)) {
    int serial = FD_GET_IMMEDIATE(lp,fd_pool_type);
    if (serial<fd_n_pools)
      return fd_pools_by_serialno[serial];
    else {
      char buf[64];
      fd_seterr3(fd_InvalidPoolPtr,"fd_lisp2pool",
                 u8_sprintf(buf,64,"serial = 0x%x",serial));
      return NULL;}}
  else if (TYPEP(lp,fd_consed_pool_type))
    return (fd_pool) lp;
  else {
    fd_seterr(fd_TypeError,_("not a pool"),NULL,lp);
    return NULL;}
}

/* Iterating over pools */

FD_EXPORT int fd_for_pools(int (*fcn)(fd_pool,void *),void *data)
{
  int i=0, n=fd_n_pools, count=0;

  while (i<n) {
    fd_pool p = fd_pools_by_serialno[i++];
    if (fcn(p,data))
      return count+1;
    count++;}

  if (n_consed_pools) {
    u8_lock_mutex(&consed_pools_lock);
    int j = 0; while (j<n_consed_pools) {
      fd_pool p = consed_pools[j++];
      int retval = fcn(p,data);
      count++;
      if (retval) u8_unlock_mutex(&consed_pools_lock);
      if (retval<0) return retval;
      else if (retval) break;
      else j++;}}
  return count;
}

FD_EXPORT lispval fd_all_pools()
{
  lispval results = EMPTY;
  int i=0, n=fd_n_pools;
  while (i<n) {
    fd_pool p = fd_pools_by_serialno[i++];
    lispval lp = fd_pool2lisp(p);
    CHOICE_ADD(results,lp);}
  return results;
}

/* Finding pools by various names */

/* TODO: Add generic method which uses the pool matcher
   methods to find pools or pool sources.
*/

FD_EXPORT fd_pool fd_find_pool(u8_string spec)
{
  fd_pool p=fd_find_pool_by_source(spec);
  if (p) return p;
  else if ((p=fd_find_pool_by_id(spec)))
    return p;
  /* TODO: Add generic method which uses the pool matcher
     methods to find pools */
  else return NULL;
}
FD_EXPORT u8_string fd_locate_pool(u8_string spec)
{
  fd_pool p=fd_find_pool_by_source(spec);
  if (p) return u8_strdup(p->pool_source);
  else if ((p=fd_find_pool_by_id(spec)))
    return u8_strdup(p->pool_source);
  /* TODO: Add generic method which uses the pool matcher
     methods to find pools */
  else return NULL;
}

static int match_pool_id(fd_pool p,u8_string id)
{
  return ((id)&&(p->poolid)&&
          (strcmp(p->poolid,id)==0));
}

static int match_pool_source(fd_pool p,u8_string source)
{
  return (fd_same_sourcep(source,p->canonical_source)) ||
    (fd_same_sourcep(source,p->pool_source));
}

FD_EXPORT fd_pool fd_find_pool_by_id(u8_string id)
{
  int i=0, n=fd_n_pools;
  while (i<n) {
    fd_pool p = fd_pools_by_serialno[i++];
    if (match_pool_id(p,id))
      return p;}
  return NULL;
}

FD_EXPORT fd_pool fd_find_pool_by_source(u8_string source)
{
  int i=0, n=fd_n_pools;
  while (i<n) {
    fd_pool p = fd_pools_by_serialno[i++];
    if (match_pool_source(p,source))
      return p;}
  return NULL;
}

FD_EXPORT lispval fd_find_pools_by_source(u8_string source)
{
  lispval results=EMPTY;
  int i=0, n=fd_n_pools;
  while (i<n) {
    fd_pool p = fd_pools_by_serialno[i++];
    if (match_pool_source(p,source)) {
      lispval lp=fd_pool2lisp(p);
      CHOICE_ADD(results,lp);}}
  return results;
}

FD_EXPORT fd_pool fd_find_pool_by_prefix(u8_string prefix)
{
  int i=0, n=fd_n_pools;
  while (i<n) {
    fd_pool p = fd_pools_by_serialno[i++];
    if (( (p->pool_prefix) && ((strcasecmp(prefix,p->pool_prefix)) == 0) ) ||
        ( (p->pool_label) && ((strcasecmp(prefix,p->pool_label)) == 0) ))
      return p;}
  return NULL;
}

/* Operations over all pools */

static int do_swapout(fd_pool p,void *data)
{
  fd_pool_swapout(p,VOID);
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

static int commit_each_pool(fd_pool p,void *data)
{
  /* Phased pools shouldn't be committed by themselves */
  if ( (p->pool_flags) & (FD_STORAGE_PHASED) ) return 0;
  int retval = fd_pool_unlock_all(p,1);
  if (retval<0)
    if (data) {
      u8_logf(LOG_CRIT,"POOL_COMMIT_FAIL",
              "Error when committing pool %s",p->poolid);
      return 0;}
    else return -1;
  else return 0;
}

FD_EXPORT int fd_commit_pools()
{
  return fd_for_pools(commit_each_pool,(void *)NULL);
}

FD_EXPORT int fd_commit_pools_noerr()
{
  return fd_for_pools(commit_each_pool,(void *)"NOERR");
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
  lispval *vals = (lispval *)ptr;
  lispval keys = fd_hashtable_keys(&(p->pool_cache));
  CHOICE_ADD(*vals,keys);
  return 0;
}

FD_EXPORT
lispval fd_cached_oids(fd_pool p)
{
  if (p == NULL) {
    int retval; lispval result = EMPTY;
    retval = fd_for_pools(accumulate_cached,(void *)&result);
    if (retval<0) {
      fd_decref(result);
      return FD_ERROR;}
    else return result;}
  else return fd_hashtable_keys(&(p->pool_cache));
}

static int accumulate_changes(fd_pool p,void *ptr)
{
  if (p->pool_changes.table_n_keys) {
    lispval *vals = (lispval *)ptr;
    lispval keys = fd_hashtable_keys(&(p->pool_changes));
    CHOICE_ADD(*vals,keys);}
  return 0;
}

FD_EXPORT
lispval fd_changed_oids(fd_pool p)
{
  if (p == NULL) {
    int retval; lispval result = EMPTY;
    retval = fd_for_pools(accumulate_changes,(void *)&result);
    if (retval<0) {
      fd_decref(result);
      return FD_ERROR;}
    else return result;}
  else return fd_hashtable_keys(&(p->pool_changes));
}

/* Common pool initialization stuff */

FD_EXPORT void fd_init_pool(fd_pool p,
                            FD_OID base,unsigned int capacity,
                            struct FD_POOL_HANDLER *h,
                            u8_string id,u8_string source,u8_string csource,
                            fd_storage_flags flags,
                            lispval metadata,
                            lispval opts)
{
  FD_INIT_CONS(p,fd_consed_pool_type);
  p->pool_base = base;
  p->pool_capacity = capacity;
  p->pool_source = u8_strdup(source);
  p->canonical_source = u8_strdup(csource);
  p->poolid = u8_strdup(id);
  p->pool_typeid = NULL;
  p->pool_handler = h;
  p->pool_flags = fd_get_dbflags(opts,flags);
  p->pool_serialno = -1; p->pool_cache_level = -1;
  p->pool_adjuncts = NULL;
  p->pool_adjuncts_len = 0;
  p->pool_n_adjuncts = 0;
  p->pool_label = NULL;
  p->pool_prefix = NULL;
  p->pool_namefn = VOID;

  lispval ll = fd_getopt(opts,FDSYM_LOGLEVEL,FD_VOID);
  if (FD_VOIDP(ll))
    p->pool_loglevel = -1;
  else if ( (FD_FIXNUMP(ll)) && ( (FD_FIX2INT(ll)) >= 0 ) &&
       ( (FD_FIX2INT(ll)) < U8_MAX_LOGLEVEL ) )
    p->pool_loglevel = FD_FIX2INT(ll);
  else {
    u8_log(LOG_WARN,"BadLogLevel",
           "Invalid loglevel %q for pool %s",ll,id);
    p->pool_loglevel = -1;}
  fd_decref(ll);

  if ( (FD_VOIDP(opts)) || (FD_FALSEP(opts)) )
    p->pool_opts = FD_FALSE;
  else p->pool_opts = fd_incref(opts);

  /* Data tables */
  FD_INIT_STATIC_CONS(&(p->pool_cache),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(p->pool_changes),fd_hashtable_type);
  fd_make_hashtable(&(p->pool_cache),fd_pool_cache_init);
  fd_make_hashtable(&(p->pool_changes),fd_pool_lock_init);

  /* Metadata tables */
  FD_INIT_STATIC_CONS(&(p->pool_props),fd_slotmap_type);
  fd_init_slotmap(&(p->pool_props),17,NULL);

  FD_INIT_STATIC_CONS(&(p->pool_metadata),fd_slotmap_type);
  if (FD_SLOTMAPP(metadata)) {
    lispval adj = fd_get(metadata,FDSYM_ADJUNCT,FD_VOID);
    if ( (FD_OIDP(adj)) || (FD_TRUEP(adj)) || (FD_SYMBOLP(adj)) )
      p->pool_flags |= FD_POOL_ADJUNCT;
    else fd_decref(adj);
    fd_copy_slotmap((fd_slotmap)metadata,&(p->pool_metadata));}
  else {
    fd_init_slotmap(&(p->pool_metadata),17,NULL);}
  p->pool_metadata.table_modified = 0;

  u8_init_rwlock(&(p->pool_struct_lock));
  u8_init_mutex(&(p->pool_commit_lock));
}

FD_EXPORT int fd_pool_set_metadata(fd_pool p,lispval metadata)
{
  if (FD_SLOTMAPP(metadata)) {
    if (p->pool_metadata.n_allocd) {
      struct FD_SLOTMAP *sm = & p->pool_metadata;
      if ( (sm->sm_free_keyvals) && (sm->sm_keyvals) )
        u8_free(sm->sm_keyvals);
      u8_destroy_rwlock(&(sm->table_rwlock));}
    lispval adj = fd_get(metadata,FDSYM_ADJUNCT,FD_VOID);
    if ( (FD_OIDP(adj)) || (FD_TRUEP(adj)) || (FD_SYMBOLP(adj)) )
      p->pool_flags |= FD_POOL_ADJUNCT;
    else fd_decref(adj);
    fd_copy_slotmap((fd_slotmap)metadata,&(p->pool_metadata));}
  else {
    FD_INIT_STATIC_CONS(&(p->pool_metadata),fd_slotmap_type);
    fd_init_slotmap(&(p->pool_metadata),17,NULL);}
  p->pool_metadata.table_modified = 0;
  return 0;
}

FD_EXPORT void fd_set_pool_namefn(fd_pool p,lispval namefn)
{
  lispval oldnamefn = p->pool_namefn;
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
  NULL, /* create */
  NULL, /* walker */
  NULL, /* recycle */
  NULL  /* poolctl */
};


FD_EXPORT fd_pool fd_get_pool
(u8_string spec,fd_storage_flags flags,lispval opts)
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
(u8_string spec,fd_storage_flags flags,lispval opts)
{
  return fd_get_pool(spec,flags&(~FD_STORAGE_UNREGISTERED),opts);
}

FD_EXPORT fd_pool fd_name2pool(u8_string spec)
{
  lispval label_string = fd_make_string(NULL,-1,spec);
  lispval poolv = fd_hashtable_get(&poolid_table,label_string,VOID);
  fd_decref(label_string);
  if (VOIDP(poolv)) return NULL;
  else return fd_lisp2pool(poolv);
}

static void display_pool(u8_output out,fd_pool p,lispval lp)
{
  char addrbuf[128], numbuf[32];
  u8_string typeid = p->pool_typeid;
  u8_string type = (typeid) ? (typeid) :
    ((p->pool_handler) && (p->pool_handler->name)) ?
    (p->pool_handler->name) : ((u8_string)"notype");
  u8_string tag = (CONSP(lp)) ? ("CONSPOOL") : ("POOL");
  u8_string id = p->poolid;
  u8_string source = (p->pool_source) ? (p->pool_source) : (id);
  u8_string useid = ( (strchr(id,':')) || (strchr(id,'@')) ) ? (id) :
    (u8_strchr(id,'/',1)) ? ((u8_strchr(id,'/',-1))+1) : (id);
  int n_cached=p->pool_cache.table_n_keys;
  int n_changed=p->pool_changes.table_n_keys;
  int is_adjunct = ( (p->pool_flags) & (FD_POOL_ADJUNCT) );
  strcpy(addrbuf,"@");
  strcat(addrbuf,u8_uitoa16(FD_OID_HI(p->pool_base),numbuf));
  strcat(addrbuf,"/");
  strcat(addrbuf,u8_uitoa16(FD_OID_LO(p->pool_base),numbuf));
  strcat(addrbuf,"+0x0");
  strcat(addrbuf,u8_uitoa16(p->pool_capacity,numbuf));
  if (p->pool_label)
    u8_printf(out,"#<%s %s (%s%s) %s cx=%d/%d #!%lx \"%s\">",
              tag,p->pool_label,type,
              ((is_adjunct)?("/adj"):("")),
              addrbuf,n_cached,n_changed,
              lp,source);
  else if (strcmp(useid,source))
    u8_printf(out,"#<%s %s (%s%s) %s oids=%d/%d #!%lx \"%s\">",
              tag,useid,type,
              ((is_adjunct)?("/adj"):("")),
              addrbuf,n_cached,n_changed,lp,source);
  else u8_printf(out,"#<%s %s (%s%s) %s oids=%d/%d #!%lx>",
                 tag,useid,type,
                 ((is_adjunct)?("/adj"):("")),
                 addrbuf,n_cached,n_changed,lp);

}

static int unparse_pool(u8_output out,lispval x)
{
  fd_pool p = fd_lisp2pool(x);
  if (p == NULL) return 0;
  display_pool(out,p,x);
  return 1;
}

static int unparse_consed_pool(u8_output out,lispval x)
{
  fd_pool p = (fd_pool)x;
  if (p == NULL) return 0;
  display_pool(out,p,x);
  return 1;
}

static lispval pool_parsefn(int n,lispval *args,fd_compound_typeinfo e)
{
  fd_pool p = NULL;
  if (n<3) return VOID;
  else if (n==3)
    p = fd_use_pool(FD_STRING_DATA(args[2]),0,VOID);
  else if ((STRINGP(args[2])) &&
           ((p = fd_find_pool_by_prefix(FD_STRING_DATA(args[2]))) == NULL))
    p = fd_use_pool(FD_STRING_DATA(args[3]),0,VOID);
  if (p) return fd_pool2lisp(p);
  else return fd_err(fd_CantParseRecord,"pool_parsefn",NULL,VOID);
}

/* Executing pool delays */

FD_EXPORT int fd_execute_pool_delays(fd_pool p,void *data)
{
  lispval todo = fd_pool_delays[p->pool_serialno];
  if (EMPTYP(todo)) return 0;
  else {
    /* u8_lock_mutex(&(fd_ipeval_lock)); */
    todo = fd_pool_delays[p->pool_serialno];
    fd_pool_delays[p->pool_serialno]=EMPTY;
    todo = fd_simplify_choice(todo);
    /* u8_unlock_mutex(&(fd_ipeval_lock)); */
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval>1)
      u8_logf(LOG_DEBUG,ipeval_objfetch,"Fetching %d oids from %s: %q",
              FD_CHOICE_SIZE(todo),p->poolid,todo);
    else if (fd_trace_ipeval)
      u8_logf(LOG_DEBUG,ipeval_objfetch,"Fetching %d oids from %s",
              FD_CHOICE_SIZE(todo),p->poolid);
#endif
    fd_pool_prefetch(p,todo);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval)
      u8_logf(LOG_DEBUG,ipeval_objfetch,"Fetched %d oids from %s",
              FD_CHOICE_SIZE(todo),p->poolid);
#endif
    return 0;}
}

/* Raw pool table operations */

static lispval pool_get(fd_pool p,lispval key,lispval dflt)
{
  if (OIDP(key)) {
    FD_OID addr = FD_OID_ADDR(key);
    FD_OID base = p->pool_base;
    if (FD_OID_COMPARE(addr,base)<0)
      return fd_incref(dflt);
    else {
      unsigned int offset = FD_OID_DIFFERENCE(addr,base);
      unsigned int capacity = p->pool_capacity;
      int load = fd_pool_load(p);
      if (load<0)
        return FD_ERROR;
      else if ((p->pool_flags&FD_POOL_ADJUNCT) ?
               (offset<capacity) :
               (offset<load))
        return fd_fetch_oid(p,key);
      else return fd_incref(dflt);}}
  else return fd_incref(dflt);
}

static lispval pool_tableget(lispval arg,lispval key,lispval dflt)
{
  fd_pool p=fd_lisp2pool(arg);
  if (p)
    return pool_get(p,key,dflt);
  else return fd_incref(dflt);
}

FD_EXPORT lispval fd_pool_get(fd_pool p,lispval key)
{
  return pool_get(p,key,EMPTY);
}

static int pool_store(fd_pool p,lispval key,lispval value)
{
  if (OIDP(key)) {
    FD_OID addr = FD_OID_ADDR(key);
    FD_OID base = p->pool_base;
    if (FD_OID_COMPARE(addr,base)<0) {
      fd_seterr(fd_PoolRangeError,"pool_store",fd_pool_id(p),key);
      return -1;}
    else {
      unsigned int offset = FD_OID_DIFFERENCE(addr,base);
      modify_modified(value,1);
      int cap = p->pool_capacity, rv = -1;
      if (offset>cap) {
        fd_seterr(fd_PoolRangeError,"pool_store",fd_pool_id(p),key);
        return -1;}
      else if ( (p->pool_flags & FD_STORAGE_VIRTUAL) &&
                (p->pool_handler->commit) )
        rv=fd_pool_storen(p,1,&key,&value);
      else if (p->pool_handler->lock == NULL) {
        rv = fd_hashtable_store(&(p->pool_cache),key,value);}
      else if (fd_pool_lock(p,key)) {
        rv = fd_hashtable_store(&(p->pool_changes),key,value);}
      else {
        fd_seterr(fd_CantLockOID,"pool_store",fd_pool_id(p),key);
        return -1;}
      return rv;}}
  else {
    fd_seterr(fd_NotAnOID,"pool_store",fd_pool_id(p),fd_incref(key));
    return -1;}
}

static lispval pool_tablestore(lispval arg,lispval key,lispval val)
{
  fd_pool p=fd_lisp2pool(arg);
  if (p) {
    int rv=pool_store(p,key,val);
    if (rv<0)
      return FD_ERROR;
    else return FD_TRUE;}
  else return fd_type_error("pool","pool_tablestore",arg);
}

FD_EXPORT int fd_pool_store(fd_pool p,lispval key,lispval value)
{
  return pool_store(p,key,value);
}

FD_EXPORT lispval fd_pool_keys(lispval arg)
{
  lispval results = EMPTY;
  fd_pool p = fd_lisp2pool(arg);
  if (p==NULL)
    return fd_type_error("pool","pool_tablekeys",arg);
  else {
    FD_OID base = p->pool_base;
    unsigned int i = 0; int load = fd_pool_load(p);
    if (load<0) return FD_ERROR;
    while (i<load) {
      lispval each = fd_make_oid(FD_OID_PLUS(base,i));
      CHOICE_ADD(results,each);
      i++;}
    return results;}
}

static void recycle_consed_pool(struct FD_RAW_CONS *c)
{
  struct FD_POOL *p = (struct FD_POOL *)c;
  struct FD_POOL_HANDLER *handler = p->pool_handler;
  if (p->pool_serialno>=0)
    return;
  else if (p->pool_handler == &gluepool_handler)
    return;
  else {}
  drop_consed_pool(p);
  if (handler->recycle) handler->recycle(p);
  fd_recycle_hashtable(&(p->pool_cache));
  fd_recycle_hashtable(&(p->pool_changes));
  u8_free(p->poolid);
  u8_free(p->pool_source);
  if (p->pool_label) u8_free(p->pool_label);
  if (p->pool_prefix) u8_free(p->pool_prefix);
  if (p->pool_typeid) u8_free(p->pool_typeid);

  struct FD_ADJUNCT *adjuncts = p->pool_adjuncts;
  int n_adjuncts = p->pool_n_adjuncts;
  if ( (adjuncts) && (n_adjuncts) ) {
    int i = 0; while (i<n_adjuncts) {
      fd_decref(adjuncts[i].table);
      adjuncts[i].table = VOID;
      i++;}}
  if (adjuncts) u8_free(adjuncts);
  p->pool_adjuncts = NULL;

  fd_free_slotmap(&(p->pool_metadata));
  fd_free_slotmap(&(p->pool_props));

  fd_decref(p->pool_namefn);
  p->pool_namefn = VOID;

  fd_decref(p->pool_opts);
  p->pool_opts = VOID;

  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

static lispval base_slot, capacity_slot, cachelevel_slot,
  label_slot, poolid_slot, source_slot, adjuncts_slot, _adjuncts_slot,
  cached_slot, locked_slot, flags_slot, registered_slot, opts_slot,
  core_slot;

static lispval read_only_flag, unregistered_flag, registered_flag,
  noswap_flag, noerr_flag, phased_flag, sparse_flag, background_flag,
  virtual_flag, nolocks_flag;

static void mdstore(lispval md,lispval slot,lispval v)
{
  if (FD_VOIDP(v)) return;
  fd_store(md,slot,v);
  fd_decref(v);
}
static void mdstring(lispval md,lispval slot,u8_string s)
{
  if (s==NULL) return;
  lispval v=fdstring(s);
  fd_store(md,slot,v);
  fd_decref(v);
}

static lispval metadata_readonly_props = FD_VOID;

FD_EXPORT lispval fd_pool_base_metadata(fd_pool p)
{
  int flags=p->pool_flags;
  lispval metadata=fd_deep_copy((lispval) &(p->pool_metadata));
  mdstore(metadata,core_slot,fd_deep_copy((lispval) &(p->pool_metadata)));
  mdstore(metadata,base_slot,fd_make_oid(p->pool_base));
  mdstore(metadata,capacity_slot,FD_INT(p->pool_capacity));
  mdstore(metadata,cachelevel_slot,FD_INT(p->pool_cache_level));
  mdstring(metadata,poolid_slot,p->poolid);
  mdstring(metadata,source_slot,p->pool_source);
  mdstring(metadata,label_slot,p->pool_label);
  if ((p->pool_handler) && (p->pool_handler->name))
    mdstring(metadata,FDSYM_TYPE,(p->pool_handler->name));
  mdstore(metadata,cached_slot,FD_INT(p->pool_cache.table_n_keys));
  mdstore(metadata,locked_slot,FD_INT(p->pool_changes.table_n_keys));

  if (U8_BITP(flags,FD_STORAGE_READ_ONLY))
    fd_add(metadata,flags_slot,read_only_flag);
  if (U8_BITP(flags,FD_STORAGE_UNREGISTERED))
    fd_add(metadata,flags_slot,unregistered_flag);
  else {
    fd_add(metadata,flags_slot,registered_flag);
    fd_store(metadata,registered_slot,FD_INT(p->pool_serialno));}
  if (U8_BITP(flags,FD_STORAGE_NOSWAP))
    fd_add(metadata,flags_slot,noswap_flag);
  if (U8_BITP(flags,FD_STORAGE_NOERR))
    fd_add(metadata,flags_slot,noerr_flag);
  if (U8_BITP(flags,FD_STORAGE_PHASED))
    fd_add(metadata,flags_slot,phased_flag);
  if (U8_BITP(flags,FD_POOL_SPARSE))
    fd_add(metadata,flags_slot,sparse_flag);
  if (U8_BITP(flags,FD_POOL_VIRTUAL))
    fd_add(metadata,flags_slot,virtual_flag);
  if (U8_BITP(flags,FD_POOL_NOLOCKS))
    fd_add(metadata,flags_slot,nolocks_flag);

  if (fd_testopt(metadata,FDSYM_ADJUNCT,FD_VOID))
    fd_add(metadata,flags_slot,FDSYM_ISADJUNCT);
  else fd_add(metadata,flags_slot,background_flag);

  if (U8_BITP(flags,FD_POOL_ADJUNCT))
    fd_add(metadata,flags_slot,FDSYM_ADJUNCT);

  lispval props_copy = fd_copier(((lispval)&(p->pool_props)),0);
  fd_store(metadata,FDSYM_PROPS,props_copy);
  fd_decref(props_copy);

  if (p->pool_n_adjuncts) {
    int i=0, n=p->pool_n_adjuncts;
    struct FD_ADJUNCT *adjuncts=p->pool_adjuncts;
    lispval adjuncts_table=fd_make_slotmap(n,0,NULL);
    while (i<n) {
      fd_store(adjuncts_table,
               adjuncts[i].slotid,
               adjuncts[i].table);
      i++;}
    fd_store(metadata,_adjuncts_slot,adjuncts_table);
    fd_decref(adjuncts_table);}

  if (FD_TABLEP(p->pool_opts))
    fd_store(metadata,opts_slot,p->pool_opts);

  fd_add(metadata,metadata_readonly_props,FDSYM_TYPE);
  fd_add(metadata,metadata_readonly_props,base_slot);
  fd_add(metadata,metadata_readonly_props,capacity_slot);
  fd_add(metadata,metadata_readonly_props,cachelevel_slot);
  fd_add(metadata,metadata_readonly_props,poolid_slot);
  fd_add(metadata,metadata_readonly_props,label_slot);
  fd_add(metadata,metadata_readonly_props,source_slot);
  fd_add(metadata,metadata_readonly_props,locked_slot);
  fd_add(metadata,metadata_readonly_props,cached_slot);
  fd_add(metadata,metadata_readonly_props,flags_slot);

  fd_add(metadata,metadata_readonly_props,FDSYM_PROPS);
  fd_add(metadata,metadata_readonly_props,opts_slot);
  fd_add(metadata,metadata_readonly_props,core_slot);

  return metadata;
}

FD_EXPORT lispval fd_default_poolctl(fd_pool p,lispval op,int n,lispval *args)
{
  if ((n>0)&&(args == NULL))
    return fd_err("BadPoolOpCall","fd_default_poolctl",p->poolid,VOID);
  else if (n<0)
    return fd_err("BadPoolOpCall","fd_default_poolctl",p->poolid,VOID);
  else if (op == fd_label_op) {
    if (n==0) {
      if (!(p->pool_label))
        return FD_FALSE;
      else return lispval_string(p->pool_label);}
    else return FD_FALSE;}
  else if (op == FDSYM_OPTS)  {
    lispval opts = p->pool_opts;
    if (n > 1)
      return fd_err(fd_TooManyArgs,"fd_default_indexctl",p->poolid,VOID);
    else if ( (opts == FD_NULL) || (VOIDP(opts) ) )
      return FD_FALSE;
    else if ( n == 1 )
      return fd_getopt(opts,args[0],FD_FALSE);
    else return fd_incref(opts);}
  else if (op == fd_metadata_op) {
    lispval metadata = (lispval) &(p->pool_metadata);
    lispval slotid = (n>0) ? (args[0]) : (FD_VOID);
    /* TODO: check that slotid isn't any of the slots returned by default */
    if (n == 0)
      return fd_pool_base_metadata(p);
    else if (n == 1) {
      lispval extended=fd_pool_ctl(p,fd_metadata_op,0,NULL);
      lispval v = fd_get(extended,slotid,FD_EMPTY);
      fd_decref(extended);
      return v;}
    else if (n == 2) {
      lispval extended=fd_pool_ctl(p,fd_metadata_op,0,NULL);
      if (fd_test(extended,FDSYM_READONLY,slotid)) {
        fd_decref(extended);
        return fd_err("ReadOnlyMetadataProperty","fd_default_poolctl",
                      p->poolid,slotid);}
      else fd_decref(extended);
      int rv=fd_store(metadata,slotid,args[1]);
      if (rv<0)
        return FD_ERROR_VALUE;
      else return fd_incref(args[1]);}
    else return fd_err(fd_TooManyArgs,"fd_pool_ctl/metadata",
                       FD_SYMBOL_NAME(op),fd_pool2lisp(p));}
  else if (op == adjuncts_slot) {
    if (n == 0) {
      if (p->pool_n_adjuncts) {
        int i=0, n=p->pool_n_adjuncts;
        struct FD_ADJUNCT *adjuncts=p->pool_adjuncts;
        lispval adjuncts_table=fd_make_slotmap(n,0,NULL);
        while (i<n) {
          fd_store(adjuncts_table,
                   adjuncts[i].slotid,
                   adjuncts[i].table);
          i++;}
        return adjuncts_table;}
      else return fd_empty_slotmap();}
    else if (n == 1) {
      if (p->pool_n_adjuncts) {
        lispval slotid = args[0];
        struct FD_ADJUNCT *adjuncts=p->pool_adjuncts;
        int i=0, n = p->pool_n_adjuncts;
        while (i<n) {
          if (adjuncts[i].slotid == slotid)
            return fd_incref(adjuncts[i].table);
          i++;}
        return FD_FALSE;}
      else return FD_FALSE;}
    else if (n == 2) {
      lispval slotid = args[0];
      lispval adjunct = args[1];
      int rv = fd_set_adjunct(p,slotid,adjunct);
      if (rv<0)
        return FD_ERROR_VALUE;
      else return adjunct;}
    else return fd_err(fd_TooManyArgs,"fd_pool_ctl/adjuncts",
                       FD_SYMBOL_NAME(op),fd_pool2lisp(p));}
  else if (op == FDSYM_PROPS) {
    lispval props = (lispval) &(p->pool_props);
    lispval slotid = (n>0) ? (args[0]) : (FD_VOID);
    if (n == 0)
      return fd_copier(props,0);
    else if (n == 1)
      return fd_get(props,slotid,FD_EMPTY);
    else if (n == 2) {
      int rv=fd_store(props,slotid,args[1]);
      if (rv<0)
        return FD_ERROR_VALUE;
      else return fd_incref(args[1]);}
    else return fd_err(fd_TooManyArgs,"fd_pool_ctl/props",
                       FD_SYMBOL_NAME(op),fd_pool2lisp(p));}
  else if (op == fd_capacity_op)
    return FD_INT(p->pool_capacity);
  else if (op == FDSYM_LOGLEVEL) {
    if (n == 0) {
      if (p->pool_loglevel < 0)
        return FD_FALSE;
      else return FD_INT(p->pool_loglevel);}
    else if (n == 1) {
      if (FIXNUMP(args[0])) {
        long long level = FD_FIX2INT(args[0]);
        if ((level<0) || (level > 128))
          return fd_err(fd_RangeError,"fd_default_poolctl",p->poolid,args[0]);
        else {
          int old_loglevel = p->pool_loglevel;
          p->pool_loglevel = level;
          if (old_loglevel<0)
            return FD_FALSE;
          else return FD_INT(old_loglevel);}}
      else return fd_type_error("loglevel","fd_default_poolctl",args[0]);}
    else return fd_err(fd_TooManyArgs,"fd_default_poolctl",p->poolid,VOID);}
  else if (op == fd_partitions_op)
    return FD_EMPTY;
  else if (op == FDSYM_CACHELEVEL)
    return FD_INT2FIX(1);
  else if (op == fd_raw_metadata_op)
    return fd_deep_copy((lispval) &(p->pool_metadata));
  else if (op == FDSYM_READONLY) {
    if (U8_BITP((p->pool_flags),FD_STORAGE_READ_ONLY))
      return FD_TRUE;
    else return FD_FALSE;}
  else {
    u8_log(LOG_WARN,"Unhandled POOLCTL op",
           "Couldn't handle %q for %s",op,p->poolid);
    return FD_FALSE;}
}

static lispval copy_consed_pool(lispval x,int deep)
{
  return fd_incref(x);
}

/* Initialization */

fd_ptr_type fd_consed_pool_type;

static int check_pool(lispval x)
{
  int serial = FD_GET_IMMEDIATE(x,fd_pool_type);
  if (serial<0) return 0;
  else if (serial<fd_n_pools) return 1;
  else return 0;
}

/* This is just for use from the debugger, so we can allocate it
   statically. */
static char oid_info_buf[512];

static u8_string _more_oid_info(lispval oid)
{
  if (OIDP(oid)) {
    FD_OID addr = FD_OID_ADDR(oid);
    fd_pool p = fd_oid2pool(oid);
    unsigned int hi = FD_OID_HI(addr), lo = FD_OID_LO(addr);
    if (p == NULL)
      u8_sprintf(oid_info_buf,sizeof(oid_info_buf),
                 "@%x/%x in no pool",hi,lo);
    else if ((p->pool_label)&&(p->pool_source))
      u8_sprintf(oid_info_buf,sizeof(oid_info_buf),
                 "@%x/%x in %s from %s = %s",
                 hi,lo,p->pool_label,p->pool_source,p->poolid);
    else if (p->pool_label)
      u8_sprintf(oid_info_buf,sizeof(oid_info_buf),
                 "@%x/%x in %s",hi,lo,p->pool_label);
    else if (p->pool_source)
      u8_sprintf(oid_info_buf,sizeof(oid_info_buf),
                 "@%x/%x from %s = %s",
                 hi,lo,p->pool_source,p->poolid);
    else u8_sprintf(oid_info_buf,sizeof(oid_info_buf),
                    "@%x/%x from %s",hi,lo,p->poolid);
    return oid_info_buf;}
  else return "not an oid!";
}


/* Config functions */

FD_EXPORT lispval fd_poolconfig_get(lispval var,void *vptr)
{
  fd_pool *pptr = (fd_pool *)vptr;
  if (*pptr == NULL)
    return fd_err(_("Config mis-config"),"pool_config_get",NULL,var);
  else {
    if (*pptr)
      return fd_pool2lisp(*pptr);
    else return EMPTY;}
}
FD_EXPORT int fd_poolconfig_set(lispval ignored,lispval v,void *vptr)
{
  fd_pool *ppid = (fd_pool *)vptr;
  if (FD_POOLP(v)) *ppid = fd_lisp2pool(v);
  else if (STRINGP(v)) {
    fd_pool p = fd_use_pool(CSTRING(v),0,VOID);
    if (p) *ppid = p; else return -1;}
  else return fd_type_error(_("pool spec"),"pool_config_set",v);
  return 1;
}

/* The zero pool */

struct FD_POOL _fd_zero_pool;
static u8_mutex zero_pool_alloc_lock;
fd_pool fd_default_pool = &_fd_zero_pool;

static lispval zero_pool_alloc(fd_pool p,int n)
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
    lispval *results = &(ch->choice_0);
    int i = 0; while (i<n) {
      FD_OID addr = FD_MAKE_OID(0,start+i);
      lispval oid = fd_make_oid(addr);
      results[i]=oid;
      i++;}
    return (lispval)fd_cleanup_choice(ch,FD_CHOICE_DOSORT);}
}

static int zero_pool_getload(fd_pool p)
{
  return fd_zero_pool_load;
}

static lispval zero_pool_fetch(fd_pool p,lispval oid)
{
  return fd_zero_pool_value(oid);
}

static lispval *zero_pool_fetchn(fd_pool p,int n,lispval *oids)
{
  lispval *results = u8_alloc_n(n,lispval);
  int i = 0; while (i<n) {
    results[i]=fd_zero_pool_value(oids[i]);
    i++;}
  return results;
}

static int zero_pool_lock(fd_pool p,lispval oids)
{
  return 1;
}
static int zero_pool_unlock(fd_pool p,lispval oids)
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
  _fd_zero_pool.pool_typeid = u8_strdup("_mempool");
  _fd_zero_pool.pool_base = 0;
  _fd_zero_pool.pool_capacity = 0x100000; /* About a million */
  _fd_zero_pool.pool_flags = 0;
  _fd_zero_pool.modified_flags = 0;
  _fd_zero_pool.pool_handler = &zero_pool_handler;
  u8_init_rwlock(&(_fd_zero_pool.pool_struct_lock));
  u8_init_mutex(&(_fd_zero_pool.pool_commit_lock));
  fd_init_hashtable(&(_fd_zero_pool.pool_cache),0,0);
  fd_init_hashtable(&(_fd_zero_pool.pool_changes),0,0);
  _fd_zero_pool.pool_n_adjuncts = 0;
  _fd_zero_pool.pool_adjuncts_len = 0;
  _fd_zero_pool.pool_adjuncts = NULL;
  _fd_zero_pool.pool_prefix="";
  _fd_zero_pool.pool_namefn = VOID;
  _fd_zero_pool.pool_opts = FD_FALSE;
  fd_register_pool(&_fd_zero_pool);
}

/* Lisp pointers to Pool pointers */

FD_EXPORT fd_pool _fd_get_poolptr(lispval x)
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

  base_slot=fd_intern("BASE");
  capacity_slot=fd_intern("CAPACITY");
  cachelevel_slot=fd_intern("CACHELEVEL");
  label_slot=fd_intern("LABEL");
  poolid_slot=fd_intern("POOLID");
  source_slot=fd_intern("SOURCE");
  adjuncts_slot=fd_intern("ADJUNCTS");
  _adjuncts_slot=fd_intern("_ADJUNCTS");
  cached_slot=fd_intern("CACHED");
  locked_slot=fd_intern("LOCKED");
  flags_slot=fd_intern("FLAGS");
  registered_slot=fd_intern("REGISTERED");
  opts_slot=fd_intern("OPTS");
  core_slot=fd_intern("CORE");

  read_only_flag=FDSYM_READONLY;
  unregistered_flag=fd_intern("UNREGISTERED");
  registered_flag=fd_intern("REGISTERED");
  noswap_flag=fd_intern("NOSWAP");
  noerr_flag=fd_intern("NOERR");
  phased_flag=fd_intern("PHASED");
  sparse_flag=fd_intern("SPARSE");
  background_flag=fd_intern("BACKGROUND");
  virtual_flag=fd_intern("VIRTUAL");
  nolocks_flag=fd_intern("NOLOCKS");

  metadata_readonly_props = fd_intern("_READONLY_PROPS");

  memset(&fd_top_pools,0,sizeof(fd_top_pools));
  memset(&fd_pools_by_serialno,0,sizeof(fd_top_pools));

  u8_init_mutex(&consed_pools_lock);
  consed_pools = u8_malloc(64*sizeof(fd_pool));
  consed_pools_len=64;

  {
    struct FD_COMPOUND_TYPEINFO *e =
      fd_register_compound(fd_intern("POOL"),NULL,NULL);
    e->compound_parser = pool_parsefn;}

  FD_INIT_STATIC_CONS(&poolid_table,fd_hashtable_type);
  fd_make_hashtable(&poolid_table,-1);

#if (FD_USE_TLS)
  u8_new_threadkey(&fd_pool_delays_key,NULL);
#endif
  fd_unparsers[fd_pool_type]=unparse_pool;
  fd_unparsers[fd_consed_pool_type]=unparse_consed_pool;
  fd_recyclers[fd_consed_pool_type]=recycle_consed_pool;
  fd_copiers[fd_consed_pool_type]=copy_consed_pool;
  fd_register_config("DEFAULTPOOL",_("Default location for new OID allocation"),
                     fd_poolconfig_get,fd_poolconfig_set,
                     &fd_default_pool);

  fd_tablefns[fd_pool_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_pool_type]->get = (fd_table_get_fn)pool_tableget;
  fd_tablefns[fd_pool_type]->store = (fd_table_store_fn)pool_tablestore;
  fd_tablefns[fd_pool_type]->keys = (fd_table_keys_fn)fd_pool_keys;

  fd_tablefns[fd_consed_pool_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_consed_pool_type]->get = (fd_table_get_fn)pool_tableget;
  fd_tablefns[fd_consed_pool_type]->store = (fd_table_store_fn)pool_tablestore;
  fd_tablefns[fd_consed_pool_type]->keys = (fd_table_keys_fn)fd_pool_keys;

  init_zero_pool();

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
