/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_POOLS 1
#define KNO_INLINE_IPEVAL 1
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/tables.h"
#include "kno/storage.h"
#include "kno/drivers.h"
#include "kno/apply.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8netfns.h>
#include <libu8/u8printf.h>

u8_condition kno_PoolConflict=("Pool conflict");
u8_condition kno_CantLockOID=_("Can't lock OID");
u8_condition kno_CantUnlockOID=_("Can't unlock OID");
u8_condition kno_PoolRangeError=_("the OID is out of the range of the pool");
u8_condition kno_NoLocking=_("No locking available");
u8_condition kno_ReadOnlyPool=_("pool is read-only");
u8_condition kno_InvalidPoolPtr=_("Invalid pool PTR");
u8_condition kno_NotAFilePool=_("not a file pool");
u8_condition kno_NoFilePools=_("file pools are not supported");
u8_condition kno_AnonymousOID=_("no pool covers this OID");
u8_condition kno_NotAPool=_("Not a pool");
u8_condition kno_NoSuchPool=_("No such pool");
u8_condition kno_BadFilePoolLabel=_("file pool label is not a string");
u8_condition kno_PoolOverflow=_("pool load > capacity");
u8_condition kno_ExhaustedPool=_("pool has no more OIDs");
u8_condition kno_InvalidPoolRange=_("pool overlaps 0x100000 boundary");
u8_condition kno_PoolCommitError=_("can't save changes to pool");
u8_condition kno_UnregisteredPool=_("internal error with unregistered pool");
u8_condition kno_UnhandledOperation=_("This pool can't handle this operation");
u8_condition kno_DataFileOverflow=_("Data file is over implementation limit");

u8_condition kno_PoolCommit=_("Pool/Commit");

int kno_pool_cache_init = KNO_POOL_CACHE_INIT;
int kno_pool_lock_init  = KNO_POOL_CHANGES_INIT;

int kno_n_pools   = 0;
int kno_max_pools = KNO_MAX_POOLS;
kno_pool kno_pools_by_serialno[KNO_MAX_POOLS];

struct KNO_POOL *kno_top_pools[KNO_N_OID_BUCKETS];

static struct KNO_HASHTABLE poolid_table;

static u8_condition ipeval_objfetch="OBJFETCH";

static lispval lock_symbol, unlock_symbol;

static int savep(lispval v,int only_finished);
static int modifiedp(lispval v);

static kno_pool *consed_pools = NULL;
ssize_t n_consed_pools = 0, consed_pools_len=0;
u8_mutex consed_pools_lock;

static int add_consed_pool(kno_pool p)
{
  u8_lock_mutex(&consed_pools_lock);
  kno_pool *scan=consed_pools, *limit=scan+n_consed_pools;
  while (scan<limit) {
    if ( *scan == p ) {
      u8_unlock_mutex(&consed_pools_lock);
      return 0;}
    else scan++;}
  if (n_consed_pools>=consed_pools_len) {
    ssize_t new_len=consed_pools_len+64;
    kno_pool *newvec=u8_realloc(consed_pools,new_len*sizeof(kno_pool));
    if (newvec) {
      consed_pools=newvec;
      consed_pools_len=new_len;}
    else {
      u8_seterr(kno_MallocFailed,"add_consed_pool",u8dup(p->poolid));
      u8_unlock_mutex(&consed_pools_lock);
      return -1;}}
  consed_pools[n_consed_pools++]=p;
  u8_unlock_mutex(&consed_pools_lock);
  return 1;
}

static int drop_consed_pool(kno_pool p)
{
  u8_lock_mutex(&consed_pools_lock);
  kno_pool *scan=consed_pools, *limit=scan+n_consed_pools;
  while (scan<limit) {
    if ( *scan == p ) {
      size_t to_shift=(limit-scan)-1;
      *scan = NULL;
      memmove(scan,scan+1,to_shift);
      n_consed_pools--;
      u8_unlock_mutex(&consed_pools_lock);
      return 1;}
    else scan++;}
  u8_unlock_mutex(&consed_pools_lock);
  return 0;
}

/* This is used in committing pools */

struct KNO_POOL_WRITES {
  int len; lispval *oids, *values;};

/* Locking functions for external libraries */

KNO_EXPORT void _kno_lock_pool_struct(kno_pool p,int for_write)
{
  kno_lock_pool_struct(p,for_write);
}

KNO_EXPORT void _kno_unlock_pool_struct(kno_pool p)
{
  kno_unlock_pool_struct(p);
}

KNO_FASTOP int modify_readonly(lispval table,int val)
{
  if (TABLEP(table)) {
    kno_lisp_type table_type = KNO_TYPEOF(table);
    switch (table_type) {
    case kno_slotmap_type: {
      struct KNO_SLOTMAP *tbl=(kno_slotmap)table;
      KNO_XTABLE_SET_READONLY(tbl,val);
      return 1;}
    case kno_schemap_type: {
      struct KNO_SCHEMAP *tbl=(kno_schemap)table;
      KNO_XTABLE_SET_READONLY(tbl,val);
      return 1;}
    case kno_hashtable_type: {
      struct KNO_HASHTABLE *tbl=(kno_hashtable)table;
      KNO_XTABLE_SET_READONLY(tbl,val);
      return 1;}
    default: {
      kno_lisp_type typecode = KNO_TYPEOF(table);
      struct KNO_TABLEFNS *methods = kno_tablefns[typecode];
      if ( (methods) && (methods->readonly) )
	return (methods->readonly)(table,1);
      else return 0;}}}
  else return 0;
}

KNO_FASTOP int modify_modified(lispval table,int val)
{
  if (TABLEP(table)) {
    kno_lisp_type table_type = KNO_TYPEOF(table);
    switch (table_type) {
    case kno_slotmap_type: {
      struct KNO_SLOTMAP *tbl=(kno_slotmap)table;
      KNO_XTABLE_SET_MODIFIED(tbl,val);
      return 1;}
    case kno_schemap_type: {
      struct KNO_SCHEMAP *tbl=(kno_schemap)table;
      KNO_XTABLE_SET_MODIFIED(tbl,val);
      return 1;}
    case kno_hashtable_type: {
      struct KNO_HASHTABLE *tbl=(kno_hashtable)table;
      KNO_XTABLE_SET_MODIFIED(tbl,val);
      return 1;}
    default: {
      kno_lisp_type typecode = KNO_TYPEOF(table);
      struct KNO_TABLEFNS *methods = kno_tablefns[typecode];
      if ( (methods) && (methods->modified) )
	return (methods->modified)(table,1);
      else return 0;}}}
  else return 0;
}

static int metadata_changed(kno_pool p)
{
  return (kno_modifiedp((lispval)(&(p->pool_metadata))));
}

/* Pool delays for IPEVAL */

#if KNO_GLOBAL_IPEVAL
lispval *kno_pool_delays = NULL;
#elif KNO_USE__THREAD
__thread lispval *kno_pool_delays = NULL;
#elif KNO_USE_TLS
u8_tld_key kno_pool_delays_key;
#else
lispval *kno_pool_delays = NULL;
#endif

#if ((KNO_USE_TLS) && (!(KNO_GLOBAL_IPEVAL)))
KNO_EXPORT lispval *kno_get_pool_delays()
{
  return (lispval *) u8_tld_get(kno_pool_delays_key);
}
KNO_EXPORT void kno_init_pool_delays()
{
  int i = 0;
  lispval *delays = (lispval *) u8_tld_get(kno_pool_delays_key);
  if (delays) return;
  delays = u8_alloc_n(KNO_N_POOL_DELAYS,lispval);
  while (i<KNO_N_POOL_DELAYS) delays[i++]=EMPTY;
  u8_tld_set(kno_pool_delays_key,delays);
}
#else
KNO_EXPORT lispval *kno_get_pool_delays()
{
  return kno_pool_delays;
}
KNO_EXPORT void kno_init_pool_delays()
{
  int i = 0;
  if (kno_pool_delays) return;
  kno_pool_delays = u8_alloc_n(KNO_N_POOL_DELAYS,lispval);
  while (i<KNO_N_POOL_DELAYS) kno_pool_delays[i++]=EMPTY;
}
#endif

static struct KNO_POOL_HANDLER gluepool_handler;
static void (*pool_conflict_handler)(kno_pool upstart,kno_pool holder) = NULL;

static void pool_conflict(kno_pool upstart,kno_pool holder);
static struct KNO_GLUEPOOL *make_gluepool(KNO_OID base);
static int add_to_gluepool(struct KNO_GLUEPOOL *gp,kno_pool p);

KNO_EXPORT kno_pool kno_open_network_pool(u8_string spec,kno_storage_flags flags);

static u8_mutex pool_registry_lock;

/* Pool ops */

KNO_EXPORT lispval kno_pool_ctl(kno_pool p,lispval poolop,int n,kno_argvec args)
{
  struct KNO_POOL_HANDLER *h = p->pool_handler;
  if (h->poolctl)
    return h->poolctl(p,poolop,n,args);
  else return KNO_FALSE;
}

/* Pool caching */

KNO_EXPORT void kno_pool_setcache(kno_pool p,int level)
{
  lispval intarg = KNO_INT(level);
  lispval result = kno_pool_ctl(p,kno_cachelevel_op,1,&intarg);
  if (KNO_ABORTP(result)) {kno_clear_errors(1);}
  kno_decref(result);
  p->pool_cache_level = level;
}

static void init_cache_level(kno_pool p)
{
  if (PRED_FALSE(p->pool_cache_level<0)) {
    lispval opts = p->pool_opts;
    long long level=kno_getfixopt(opts,"CACHELEVEL",kno_default_cache_level);
    kno_pool_setcache(p,level);}
}

KNO_EXPORT
void kno_reset_pool_tables(kno_pool p,
			   ssize_t cacheval,
			   ssize_t locksval)
{
  int read_only = U8_BITP(p->pool_flags,KNO_STORAGE_READ_ONLY);
  kno_hashtable cache = &(p->pool_cache), locks = &(p->pool_changes);
  kno_reset_hashtable(cache,((cacheval==0)?(kno_pool_cache_init):(cacheval)),1);
  if (locks->table_n_keys==0) {
    ssize_t level = (read_only)?(0):(locksval==0)?(kno_pool_lock_init):(locksval);
    kno_reset_hashtable(locks,level,1);}
}

/* Registering pools */

static void register_pool_label(kno_pool p);

KNO_EXPORT int kno_register_pool(kno_pool p)
{
  unsigned int capacity = p->pool_capacity, serial_no;
  int bix = kno_get_oid_base_index(p->pool_base,1);
  if (p->pool_serialno>=0)
    return 0;
  else if (bix<0)
    return bix;
  else if (p->pool_flags&KNO_STORAGE_UNREGISTERED) {
    add_consed_pool(p);
    return 0;}
  else u8_lock_mutex(&pool_registry_lock);
  if (kno_n_pools >= kno_max_pools) {
    kno_seterr(_("n_pools > MAX_POOLS"),"kno_register_pool",NULL,VOID);
    u8_unlock_mutex(&pool_registry_lock);
    return -1;}
  if ((capacity>=KNO_OID_BUCKET_SIZE) &&
      ((p->pool_base)%KNO_OID_BUCKET_SIZE)) {
    kno_seterr(kno_InvalidPoolRange,"kno_register_pool",p->poolid,VOID);
    u8_unlock_mutex(&pool_registry_lock);
    return -1;}
  /* Set up the serial number */
  serial_no = p->pool_serialno = kno_n_pools++;
  kno_pools_by_serialno[serial_no]=p;
  /* Make it static (as a cons) */
  KNO_SET_REFCOUNT(p,0);

  if (p->pool_flags&KNO_POOL_ADJUNCT) {
    /* Adjunct pools don't get stored in the pool lookup table */
    u8_unlock_mutex(&pool_registry_lock);
    return 1;}
  else if (capacity>=KNO_OID_BUCKET_SIZE) {
    /* This is the case where the pool spans several OID buckets */
    int i = 0, lim = capacity/KNO_OID_BUCKET_SIZE;
    /* Now get baseids for the pool and save them in kno_top_pools */
    while (i<lim) {
      KNO_OID base = KNO_OID_PLUS(p->pool_base,(KNO_OID_BUCKET_SIZE*i));
      int baseid = kno_get_oid_base_index(base,1);
      if (baseid<0) {
	u8_unlock_mutex(&pool_registry_lock);
	return -1;}
      else if (kno_top_pools[baseid]) {
	pool_conflict(p,kno_top_pools[baseid]);
	u8_unlock_mutex(&pool_registry_lock);
	return -1;}
      else kno_top_pools[baseid]=p;
      i++;}}
  else if (kno_top_pools[bix] == NULL) {
    if (p->pool_capacity == KNO_OID_BUCKET_SIZE)
      kno_top_pools[bix]=p;
    else {
      /* If the pool is smaller than an OID bucket, and there isn't a
	 pool in kno_top_pools, create a gluepool and place it there */
      struct KNO_GLUEPOOL *gluepool = make_gluepool(kno_base_oids[bix]);
      kno_top_pools[bix]=(struct KNO_POOL *)gluepool;
      if (add_to_gluepool(gluepool,p)<0) {
	u8_unlock_mutex(&pool_registry_lock);
	return -1;}}}
  else if (kno_top_pools[bix]->pool_capacity) {
    /* If the top pool has a capacity (i.e. it's not a gluepool), we
       have a pool conflict. Complain and error. */
    pool_conflict(p,kno_top_pools[bix]);
    u8_unlock_mutex(&pool_registry_lock);
    return -1;}
  /* Otherwise, it is a gluepool, so try to add the pool to it. */
  else if (add_to_gluepool((struct KNO_GLUEPOOL *)kno_top_pools[bix],p)<0) {
    /* If this fails, the registration fails. Note that we've still left
       the pool registered, so we can't free it. */
    u8_unlock_mutex(&pool_registry_lock);
    return -1;}
  u8_unlock_mutex(&pool_registry_lock);
  if (p->pool_label) register_pool_label(p);
  return 1;
}

static void register_pool_label(kno_pool p)
{
  u8_string base = u8_string_subst(p->pool_label,"/","_"), dot;
  lispval full = kno_mkstring(base);
  lispval lisp_arg = kno_pool2lisp(p);
  lispval probe = kno_hashtable_get(&poolid_table,full,EMPTY);
  if (EMPTYP(probe))
    kno_hashtable_store(&poolid_table,full,kno_pool2lisp(p));
  else {
    kno_pool conflict = kno_lisp2pool(probe);
    if (conflict != p) {
      u8_log(LOG_WARN,"PoolLabelConflict",
	     "The label '%s' is already associated "
	     "with the pool\n    %q\n rather than %q",
	     base,conflict,lisp_arg);}
    kno_decref(full);
    kno_decref(probe);
    u8_free(base);
    return;}
  if ((dot=strchr(base,'.'))) {
    lispval prefix = kno_substring(base,dot);
    lispval prefix_probe = kno_hashtable_get(&poolid_table,prefix,EMPTY);
    if (EMPTYP(prefix_probe)) {
      kno_hashtable_store(&poolid_table,prefix,kno_pool2lisp(p));}
    kno_decref(prefix);
    kno_decref(prefix_probe);}
  kno_decref(full);
  kno_decref(probe);
  u8_free(base);
}

static struct KNO_GLUEPOOL *make_gluepool(KNO_OID base)
{
  struct KNO_GLUEPOOL *pool = u8_alloc(struct KNO_GLUEPOOL);
  pool->pool_base = base; pool->pool_capacity = 0;
  pool->pool_flags = KNO_STORAGE_READ_ONLY;
  pool->pool_serialno = -1;
  pool->pool_label =
    u8_mkstring("gluepool(@%lx/%lx)",KNO_OID_HI(base),KNO_OID_LO(base));
  pool->pool_source = NULL;
  pool->pool_typeid = u8_strdup("gluepool");
  pool->n_subpools = 0; pool->subpools = NULL;
  pool->pool_handler = &gluepool_handler;
  KNO_INIT_STATIC_CONS(&(pool->pool_cache),kno_hashtable_type);
  KNO_INIT_STATIC_CONS(&(pool->pool_changes),kno_hashtable_type);
  kno_make_hashtable(&(pool->pool_cache),64);
  kno_make_hashtable(&(pool->pool_changes),64);
  /* There was a redundant serial number call here */
  return pool;
}

static int add_to_gluepool(struct KNO_GLUEPOOL *gp,kno_pool p)
{
  if (gp->n_subpools == 0) {
    struct KNO_POOL **pools = u8_alloc_n(1,kno_pool);
    pools[0]=p;
    gp->n_subpools = 1;
    gp->subpools = pools;
    return 1;}
  else {
    int comparison = 0;
    struct KNO_POOL **prev = gp->subpools;
    struct KNO_POOL **bottom = gp->subpools;
    struct KNO_POOL **top = gp->subpools+(gp->n_subpools)-1;
    struct KNO_POOL **read, **new, **write, **ipoint;
    struct KNO_POOL **middle = bottom+(top-bottom)/2;
    /* First find where it goes (or a conflict if there is one) */
    while (top>=bottom) {
      middle = bottom+(top-bottom)/2;
      comparison = KNO_OID_COMPARE(p->pool_base,(*middle)->pool_base);
      if (comparison == 0) break;
      else if (bottom == top) break;
      else if (comparison<0) top = middle-1; else bottom = middle+1;}
    /* If there is a conflict, we report it and quit */
    if (comparison==0) {pool_conflict(p,*middle); return -1;}
    /* Now we make a new vector to copy into */
    write = new = u8_alloc_n((gp->n_subpools+1),struct KNO_POOL *);
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

KNO_EXPORT u8_string kno_pool_id(kno_pool p)
{
  if (p->pool_label!=NULL)
    return p->pool_label;
  else if (p->poolid!=NULL)
    return p->poolid;
  else if (p->pool_source!=NULL)
    return p->pool_source;
  else return NULL;
}

KNO_EXPORT u8_string kno_pool_label(kno_pool p)
{
  if (p->pool_label!=NULL)
    return p->pool_label;
  else return NULL;
}

/* Finding the subpool */

KNO_EXPORT kno_pool kno_find_subpool(struct KNO_GLUEPOOL *gp,lispval oid)
{
  KNO_OID addr = KNO_OID_ADDR(oid);
  struct KNO_POOL **subpools = gp->subpools; int n = gp->n_subpools;
  struct KNO_POOL **bottom = subpools, **top = subpools+(n-1);
  while (top>=bottom) {
    struct KNO_POOL **middle = bottom+(top-bottom)/2;
    int comparison = KNO_OID_COMPARE(addr,(*middle)->pool_base);
    unsigned int difference = KNO_OID_DIFFERENCE(addr,(*middle)->pool_base);
    if (comparison < 0) top = middle-1;
    else if ((difference < ((*middle)->pool_capacity)))
      return *middle;
    else if (bottom == top) break;
    else bottom = middle+1;}
  return NULL;
}

/* Handling pool conflicts */

static void pool_conflict(kno_pool upstart,kno_pool holder)
{
  if (pool_conflict_handler) pool_conflict_handler(upstart,holder);
  else {
    u8_string upstart_id = upstart->pool_source;
    u8_string holder_id = holder->pool_source;
    if (!(upstart_id)) upstart_id = upstart->poolid;
    if (!(holder_id)) holder_id = holder->poolid;
    u8_logf(LOG_WARN,kno_PoolConflict,
	    "%s (from %s) and existing pool %s (from %s)\n",
	    upstart->pool_label,upstart_id,holder->pool_label,holder_id);
    u8_seterr(_("Pool confict"),"kno_register_pool",
	      u8_mkstring("%s w/ %s",upstart_id,holder_id));}
}


/* Basic functions on single OID values */

KNO_EXPORT lispval kno_oid_value(lispval oid)
{
  if (EMPTYP(oid))
    return oid;
  else if (OIDP(oid)) {
    lispval v = kno_fetch_oid(NULL,oid);
    if (KNO_ABORTP(v))
      return v;
    else if (v==KNO_UNALLOCATED_OID) {
      kno_pool p = kno_oid2pool(oid);
      if (p)
	kno_seterr(kno_UnallocatedOID,"kno_oid_value",(p->poolid),oid);
      else kno_seterr(kno_UnallocatedOID,"kno_oid_value",(p->poolid),oid);
      return KNO_ERROR_VALUE;}
    else return v;}
  else return kno_type_error(_("OID"),"kno_oid_value",oid);
}

KNO_EXPORT lispval kno_locked_oid_value(kno_pool p,lispval oid)
{
  if (PRED_FALSE(!(OIDP(oid))))
    return kno_type_error(_("OID"),"kno_locked_oid_value",oid);
  else if ( (p->pool_handler->lock == NULL) ||
	    (U8_BITP(p->pool_flags,KNO_POOL_VIRTUAL)) ||
	    (U8_BITP(p->pool_flags,KNO_POOL_NOLOCKS)) ) {
    return kno_fetch_oid(p,oid);}
  else if (p == kno_zero_pool)
    return kno_zero_pool_value(oid);
  else {
    lispval smap = kno_hashtable_get(&(p->pool_changes),oid,VOID);
    if (VOIDP(smap)) {
      int retval = kno_pool_lock(p,oid);
      if (retval<0)
	return KNO_ERROR;
      else if (retval) {
	lispval v = kno_fetch_oid(p,oid);
	if (KNO_ABORTP(v)) {
	  kno_seterr("FetchFailed","kno_locked_oid_value",p->poolid,oid);
	  return v;}
	else if (v==KNO_UNALLOCATED_OID) {
	  kno_pool p = kno_oid2pool(oid);
	  kno_seterr(kno_UnallocatedOID,"kno_locked_oid_value",
		     (p)?(p->poolid):((u8_string)"no pool"),oid);
	  return KNO_ERROR_VALUE;}
	modify_readonly(v,0);
	kno_hashtable_store(&(p->pool_changes),oid,v);
	return v;}
      else return kno_err(kno_CantLockOID,"kno_locked_oid_value",
			  p->pool_source,oid);}
    else if (smap == KNO_LOCKHOLDER) {
      lispval v = kno_fetch_oid(p,oid);
      if (KNO_ABORTP(v)) {
	kno_seterr("FetchFailed","kno_locked_oid_value",p->poolid,oid);
	return v;}
      modify_readonly(v,0);
      kno_hashtable_store(&(p->pool_changes),oid,v);
      return v;}
    else return smap;}
}

KNO_EXPORT int kno_set_oid_value(lispval oid,lispval value)
{
  kno_pool p = kno_oid2pool(oid);
  if (p == NULL)
    return kno_reterr(kno_AnonymousOID,"SET-OID-VALUE!",NULL,oid);
  else if (p == kno_zero_pool)
    return kno_zero_pool_store(oid,value);
  else {
    modify_readonly(value,0);
    modify_modified(value,1);
    if ( (p->pool_handler->lock == NULL) ||
	 (U8_BITP(p->pool_flags,KNO_POOL_VIRTUAL)) ||
	 (U8_BITP(p->pool_flags,KNO_POOL_NOLOCKS)) ) {
      kno_hashtable_store(&(p->pool_cache),oid,value);
      return 1;}
    else if (kno_lock_oid(oid)) {
      kno_hashtable_store(&(p->pool_changes),oid,value);
      return 1;}
    else return kno_reterr(kno_CantLockOID,"SET-OID-VALUE!",NULL,oid);}
}

KNO_EXPORT int kno_replace_oid_value(lispval oid,lispval value)
{
  kno_pool p = kno_oid2pool(oid);
  if (p == NULL)
    return kno_reterr(kno_AnonymousOID,"REPLACE-OID-VALUE!",NULL,oid);
  else if (p == kno_zero_pool)
    return kno_zero_pool_store(oid,value);
  else {
    if ( (p->pool_handler->lock == NULL) ||
	 (U8_BITP(p->pool_flags,KNO_POOL_VIRTUAL)) ||
	 (U8_BITP(p->pool_flags,KNO_POOL_NOLOCKS)) ) {
      kno_hashtable_store(&(p->pool_cache),oid,value);
      return 1;}
    else if (kno_hashtable_probe(&(p->pool_changes),oid)) {
      kno_hashtable_store(&(p->pool_changes),oid,value);
      return 1;}
    else {
      kno_hashtable_store(&(p->pool_cache),oid,value);
      return 1;}}
}

/* Fetching OID values */

KNO_EXPORT lispval kno_pool_fetch(kno_pool p,lispval oid)
{
  lispval v = VOID;
  init_cache_level(p);
  if (p == kno_zero_pool)
    return kno_zero_pool_value(oid);
  else if (p->pool_handler->fetch)
    v = p->pool_handler->fetch(p,oid);
  else if (p->pool_cache_level)
    v = kno_hashtable_get(&(p->pool_cache),oid,EMPTY);
  else {}
  if (KNO_ABORTP(v)) return v;
  else if (p->pool_cache_level == 0)
    return v;
  /* If it's locked, store it in the locks table */
  else if ( (p->pool_changes.table_n_keys) &&
	    (kno_hashtable_op(&(p->pool_changes),
			      kno_table_replace_novoid,oid,v)) )
    return v;
  else if ( (p->pool_handler->lock == NULL) ||
	    (U8_BITP(p->pool_flags,KNO_POOL_VIRTUAL)) ||
	    (U8_BITP(p->pool_flags,KNO_POOL_NOLOCKS)) )
    modify_readonly(v,0);
  else modify_readonly(v,1);
  if ( ( (p->pool_cache_level) > 0) &&
       ( ! ( (p->pool_flags) & (KNO_STORAGE_VIRTUAL) ) ) )
    kno_hashtable_store(&(p->pool_cache),oid,v);
  return v;
}

KNO_EXPORT int kno_pool_prefetch(kno_pool p,lispval oids)
{
  KNOTC *knotc = ((KNO_USE_THREADCACHE)?(kno_threadcache):(NULL));
  struct KNO_HASHTABLE *oidcache = ((knotc!=NULL)?(&(knotc->oids)):(NULL));
  int decref_oids = 0, cachelevel;
  if (p == NULL) {
    kno_seterr(kno_NotAPool,"kno_pool_prefetch","NULL pool ptr",VOID);
    return -1;}
  else if (EMPTYP(oids))
    return 0;
  else init_cache_level(p);
  int nolock=
    ( (p->pool_handler->lock == NULL) ||
      (U8_BITP(p->pool_flags,KNO_POOL_VIRTUAL)) ||
      (U8_BITP(p->pool_flags,KNO_POOL_NOLOCKS)) );
  cachelevel = p->pool_cache_level;
  /* It doesn't make sense to prefetch if you're not caching. */
  if (cachelevel<1) return 0;
  if (p->pool_handler->fetchn == NULL) {
    if (kno_ipeval_delay(KNO_CHOICE_SIZE(oids))) {
      CHOICE_ADD(kno_pool_delays[p->pool_serialno],oids);
      return 0;}
    else {
      int n_fetches = 0;
      DO_CHOICES(oid,oids) {
	lispval v = kno_pool_fetch(p,oid);
	if (KNO_ABORTP(v)) {
	  KNO_STOP_DO_CHOICES; return v;}
	n_fetches++; kno_decref(v);}
      return n_fetches;}}
  if (PRECHOICEP(oids)) {
    oids = kno_make_simple_choice(oids);
    decref_oids = 1;}
  if (kno_ipeval_status()) {
    KNO_HASHTABLE *cache = &(p->pool_cache); int n_to_fetch = 0;
    /* lispval oidschoice = kno_make_simple_choice(oids); */
    lispval *delays = &(kno_pool_delays[p->pool_serialno]);
    DO_CHOICES(oid,oids)
      if (kno_hashtable_probe_novoid(cache,oid)) {}
      else {
	CHOICE_ADD(*delays,oid);
	n_to_fetch++;}
    (void)kno_ipeval_delay(n_to_fetch);
    /* kno_decref(oidschoice); */
    return 0;}
  else if (CHOICEP(oids)) {
    struct KNO_HASHTABLE *cache = &(p->pool_cache);
    struct KNO_HASHTABLE *changes = &(p->pool_changes);
    int n = KNO_CHOICE_SIZE(oids);
    /* We use n_locked to track how many of the OIDs are in
       pool_changes (locked). */
    int n_locked = (changes->table_n_keys)?(0):(-1);
    lispval *values, *oidv = u8_big_alloc_n(n,lispval), *write = oidv;
    DO_CHOICES(o,oids)
      if (((oidcache == NULL)||(kno_hashtable_probe_novoid(oidcache,o)==0))&&
	  (kno_hashtable_probe_novoid(cache,o)==0) &&
	  (kno_hashtable_probe(changes,o)==0))
	/* If it's not in the oid cache, the pool cache, or the changes, get it */
	*write++=o;
      else if ((n_locked>=0)&&
	       (kno_hashtable_op(changes,kno_table_test,o,KNO_LOCKHOLDER))) {
	/* If it's in the changes but not loaded, save it for loading and not
	   that some of the results should be put in the changes rather than
	   the cache */
	*write++=o; n_locked++;}
      else {}
    if (write == oidv) {
      /* Nothing to prefetch, free and return */
      u8_big_free(oidv);
      if (decref_oids) kno_decref(oids);
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
	  if (kno_hashtable_op(changes,kno_table_replace_novoid,oid,v)==0) {
	    /* This is when the OID we're storing isn't locked */
	    modify_readonly(v,1);
	    kno_hashtable_op(cache,kno_table_store,oid,v);}
	  else modify_readonly(v,0);
	  if (knotc) kno_hashtable_op(&(knotc->oids),kno_table_store,oid,v);
	  /* We decref it since it would have been incref'd when stored. */
	  kno_decref(values[j]);
	  j++;}}
      else {
	/* If no values are locked, make them all readonly */
	int j = 0; while (j<n) {
	  lispval v=values[j];
	  modify_readonly(v,1);
	  j++;}
	if (knotc) kno_hashtable_iter(oidcache,kno_table_store,n,oidv,values);
	/* Store them all in the cache */
	kno_hashtable_iter(cache,kno_table_store_noref,n,oidv,values);}}
    else {
      u8_big_free(oidv);
      if (decref_oids) kno_decref(oids);
      return -1;}
    u8_big_free(oidv);
    u8_big_free(values);
    if (decref_oids) kno_decref(oids);
    return n;}
  else {
    lispval v = p->pool_handler->fetch(p,oids);
    if (KNO_ABORTP(v)) return v;
    kno_hashtable changes = &(p->pool_changes);
    if ( (changes->table_n_keys==0) ||
	 /* This will store it in changes if it's already there */
	 (kno_hashtable_op(changes,kno_table_replace_novoid,oids,v)==0) ) {}
    else if ( ( (p->pool_cache_level) > 0) &&
	      ( ! ( (p->pool_flags) & (KNO_STORAGE_VIRTUAL) ) ) )
      kno_hashtable_store(&(p->pool_cache),oids,v);
    else {}
    if (knotc) kno_hashtable_op(&(knotc->oids),kno_table_store,oids,v);
    kno_decref(v);
    return 1;}
}

/* Swapping out OIDs */

KNO_EXPORT int kno_pool_swapout(kno_pool p,lispval oids)
{
  kno_hashtable cache = &(p->pool_cache);
  if (PRECHOICEP(oids)) {
    lispval simple = kno_make_simple_choice(oids);
    int rv = kno_pool_swapout(p,simple);
    kno_decref(simple);
    return rv;}
  else if (VOIDP(oids)) {
    int rv = cache->table_n_keys;
    if ((p->pool_flags)&(KNO_STORAGE_KEEP_CACHESIZE))
      kno_reset_hashtable(cache,-1,1);
    else kno_reset_hashtable(cache,kno_pool_cache_init,1);
    return rv;}
  else if ((OIDP(oids))||(CHOICEP(oids)))
    u8_logf(LOG_GLUT,"SwapPool",_("Swapping out %d oids in pool %s"),
	    KNO_CHOICE_SIZE(oids),p->poolid);
  else if (PRECHOICEP(oids))
    u8_logf(LOG_GLUT,"SwapPool",_("Swapping out ~%d oids in pool %s"),
	    KNO_PRECHOICE_SIZE(oids),p->poolid);
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
  if (p->pool_flags&KNO_STORAGE_NOSWAP)
    return 0;
  else if (OIDP(oids)) {
    kno_hashtable_op(cache,kno_table_replace,oids,VOID);
    rv = 1;}
  else if (CHOICEP(oids)) {
    rv = KNO_CHOICE_SIZE(oids);
    kno_hashtable_iterkeys(cache,kno_table_replace,
			   KNO_CHOICE_SIZE(oids),KNO_CHOICE_DATA(oids),
			   VOID);
    kno_devoid_hashtable(cache,0);}
  else {
    rv = cache->table_n_keys;
    if ((p->pool_flags)&(KNO_STORAGE_KEEP_CACHESIZE))
      kno_reset_hashtable(cache,-1,1);
    else kno_reset_hashtable(cache,kno_pool_cache_init,1);}
  u8_logf(LOG_DETAIL,"SwapPool",
	  "Swapped out %d oids from pool '%s' in %f",
	  rv,p->poolid,u8_elapsed_time()-started);
  return rv;
}


/* Allocating OIDs */

KNO_EXPORT lispval kno_pool_alloc(kno_pool p,int n)
{
  lispval result = p->pool_handler->alloc(p,n);
  kno_hashtable init_cache = (p->pool_handler->lock) ?
    (&(p->pool_changes)) : (&(p->pool_cache));
  if (p == kno_zero_pool)
    return result;
  else if ( (p->pool_flags) & (KNO_STORAGE_VIRTUAL) )
    return result;
  else if (OIDP(result)) {
    kno_hashtable_op(init_cache,kno_table_default,result,EMPTY);
    return result;}
  else if (CHOICEP(result)) {
    kno_hashtable_iterkeys(init_cache,kno_table_default,
			   KNO_CHOICE_SIZE(result),KNO_CHOICE_DATA(result),
			   EMPTY);
    return result;}
  else if (KNO_ABORTP(result)) return result;
  else if (KNO_EXCEPTIONP(result)) return result;
  else return kno_err("BadDriverResult","kno_pool_alloc",
		      u8_strdup(p->pool_source),VOID);
}

/* Locking and unlocking OIDs within pools */

KNO_EXPORT int kno_pool_lock(kno_pool p,lispval oids)
{
  int decref_oids = 0;
  struct KNO_HASHTABLE *locks = &(p->pool_changes);
  if ( (p->pool_handler->lock == NULL) ||
       (U8_BITP(p->pool_flags,KNO_POOL_VIRTUAL)) ||
       (U8_BITP(p->pool_flags,KNO_POOL_NOLOCKS)) )
    return 0;
  if (PRECHOICEP(oids)) {
    oids = kno_make_simple_choice(oids);
    decref_oids = 1;}
  if (CHOICEP(oids)) {
    lispval needy; int retval, n; lispval temp_oidv[1];
    struct KNO_CHOICE *oidc = kno_alloc_choice(KNO_CHOICE_SIZE(oids));
    lispval *oidv = (lispval *)KNO_XCHOICE_DATA(oidc), *write = oidv;
    DO_CHOICES(o,oids)
      if (kno_hashtable_probe(locks,o)==0) *write++=o;
    if (decref_oids) kno_decref(oids);
    if (write == oidv) {
      /* Nothing to lock, free and return */
      kno_free_choice(oidc);
      return 1;}
    else n = write-oidv;
    if (p->pool_handler->lock == NULL) {
      kno_seterr(kno_CantLockOID,"kno_pool_lock",p->poolid,oids);
      return -1;}
    if (n==1) {
      needy=temp_oidv[0]=oidv[0];
      kno_free_choice(oidc);
      oidv=temp_oidv;}
    else needy = kno_init_choice(oidc,n,NULL,KNO_CHOICE_ISATOMIC);
    retval = p->pool_handler->lock(p,needy);
    if (retval<0) {
      kno_decref(needy);
      return retval;}
    else if (retval) {
      kno_hashtable_iterkeys(&(p->pool_cache),kno_table_replace,n,oidv,VOID);
      kno_hashtable_iterkeys(locks,kno_table_store,n,oidv,KNO_LOCKHOLDER);}
    kno_decref(needy);
    return retval;}
  else if (kno_hashtable_probe(locks,oids)==0) {
    int retval = p->pool_handler->lock(p,oids);
    if (decref_oids) kno_decref(oids);
    if (retval<0) return retval;
    else if (retval) {
      kno_hashtable_op(&(p->pool_cache),kno_table_replace,oids,VOID);
      kno_hashtable_op(locks,kno_table_store,oids,KNO_LOCKHOLDER);
      return 1;}
    else return 0;}
  else {
    if (decref_oids) kno_decref(oids);
    return 1;}
}

/* TODO: Cleanup kno_pool_unlock call return values */
KNO_EXPORT int kno_pool_unlock(kno_pool p,lispval oids,
			       kno_storage_unlock_flag flags)
{
  struct KNO_HASHTABLE *changes = &(p->pool_changes);
  if (changes->table_n_keys==0)
    return 0;
  else if (flags<0) {
    int n = changes->table_n_keys;
    if (p->pool_handler->unlock) {
      lispval to_unlock = kno_hashtable_keys(changes);
      p->pool_handler->unlock(p,to_unlock);
      kno_decref(to_unlock);}
    kno_reset_hashtable(changes,-1,1);
    return n;}
  else {
    int n_unlocked = 0;
    int n_committed = (flags>0) ? (kno_commit_pool(p,oids)) : (0);
    lispval to_unlock = EMPTY;
    lispval locked_oids = kno_hashtable_keys(changes);
    if (n_committed<0) {}
    {DO_CHOICES(oid,locked_oids) {
	kno_pool pool = kno_oid2pool(oid);
	if (p == pool) {
	  lispval v = kno_hashtable_get(changes,oid,VOID);
	  if ( (!(VOIDP(v))) && (!(modifiedp(v)))) {
	    CHOICE_ADD(to_unlock,oid);}}}}
    kno_decref(locked_oids);
    to_unlock = kno_simplify_choice(to_unlock);
    n_unlocked = KNO_CHOICE_SIZE(to_unlock);
    if (p->pool_handler->unlock)
      p->pool_handler->unlock(p,to_unlock);
    if (OIDP(to_unlock))
      kno_hashtable_op(changes,kno_table_replace,to_unlock,VOID);
    else {
      kno_hashtable_iterkeys(changes,kno_table_replace,
			     KNO_CHOICE_SIZE(to_unlock),
			     KNO_CHOICE_DATA(to_unlock),
			     VOID);}
    kno_devoid_hashtable(changes,0);
    kno_decref(to_unlock);
    return n_unlocked+n_committed;}
}

KNO_EXPORT int kno_pool_unlock_all(kno_pool p,kno_storage_unlock_flag flags)
{
  return kno_pool_unlock(p,KNO_FALSE,flags);
}

/* Declare locked OIDs 'finished' (read-only) */

KNO_EXPORT int kno_pool_finish(kno_pool p,lispval oids)
{
  int finished = 0;
  kno_hashtable changes = &(p->pool_changes);
  DO_CHOICES(oid,oids) {
    kno_pool pool = kno_oid2pool(oid);
    if (p == pool) {
      lispval v = kno_hashtable_get(changes,oid,VOID);
      if (CONSP(v)) {
	if (SLOTMAPP(v)) {
	  if (KNO_TABLE_MODIFIEDP(v)) {
	    KNO_TABLE_SET_FINISHED(v,1);
	    finished++;}}
	else if (SCHEMAPP(v)) {
	  if (KNO_TABLE_MODIFIEDP(v)) {
	    KNO_TABLE_SET_FINISHED(v,1);
	    finished++;}}
	else if (HASHTABLEP(v)) {
	  if (KNO_TABLE_MODIFIEDP(v)) {
	    KNO_TABLE_SET_FINISHED(v,1);
	    finished++;}}
	else {}
	kno_decref(v);}}}
  return finished;
}

static int rollback_commits(kno_pool p,struct KNO_POOL_COMMITS *commits)
{
  u8_logf(LOG_ERR,"Rollback","commit of %d OIDs%s to %s",
	  commits->commit_count,
	  ((KNO_VOIDP(commits->commit_metadata)) ? ("") : (" and metadata") ),
	  p->poolid);
  int rv = p->pool_handler->commit(p,kno_commit_rollback,commits);
  if (rv<0)
    u8_logf(LOG_CRIT,"Rollback/Failed","commit of %d OIDs%s to %s",
	    commits->commit_count,
	    ((KNO_VOIDP(commits->commit_metadata)) ? ("") : (" and metadata") ),
	    p->poolid);
  commits->commit_phase = kno_commit_flush;
  return rv;
}

/* Timing */

#define record_elapsed(loc)					\
  ((loc=(u8_elapsed_time()-(mark))),(mark=u8_elapsed_time()))

/* Committing OIDs to external sources */

static int pick_writes(kno_pool p,lispval oids,struct KNO_POOL_COMMITS *commits);
static int pick_modified(kno_pool p,int finished,struct KNO_POOL_COMMITS *commits);
static void abort_commit(kno_pool p,struct KNO_POOL_COMMITS *commits);
static int finish_commit(kno_pool p,struct KNO_POOL_COMMITS *commits);

static int pool_docommit(kno_pool p,lispval oids,
			 struct KNO_POOL_COMMITS *use_commits)
{
  struct KNO_HASHTABLE *locks = &(p->pool_changes);
  int kno_storage_loglevel = (p->pool_loglevel>=0) ? (p->pool_loglevel) :
    (*kno_storage_loglevel_ptr);

  if ( (use_commits == NULL) &&
       (locks->table_n_keys==0) &&
       (! (metadata_changed(p)) )) {
    u8_logf(LOG_DEBUG,kno_PoolCommit,"No locked oids in %s",p->poolid);
    return 0;}
  else if (p->pool_handler->commit == NULL) {
    u8_logf(LOG_DEBUG,kno_PoolCommit,
	    "No commit handler, just unlocking OIDs in %s",p->poolid);
    int rv = kno_pool_unlock(p,oids,leave_modified);
    return rv;}
  else {
    int free_commits = 1;
    struct KNO_POOL_COMMITS commits = { 0 };

    u8_lock_mutex(&(p->pool_commit_lock));
    if (use_commits) {
      memcpy(&commits,use_commits,sizeof(struct KNO_POOL_COMMITS));
      if (commits.commit_vals)
	kno_incref_vec(commits.commit_vals,commits.commit_count);
      free_commits=0;}
    else commits.commit_pool = p;

    if (commits.commit_metadata == KNO_NULL)
      commits.commit_metadata = KNO_VOID;

    double start_time = u8_elapsed_time(), mark = start_time;
    commits.commit_times.base = start_time;

    u8_logf(LOG_DEBUG,"PoolCommit/Start","Starting to commit %s",p->poolid);
    int started = p->pool_handler->commit(p,kno_commit_start,&commits);
    record_elapsed(commits.commit_times.start);

    if ( (started < 0) || (commits.commit_count < 0) ) {
      u8_graberrno("pool_commit",u8_strdup(p->poolid));
      u8_logf(LOG_WARN,"PoolCommit/Start",
	      "Failed starting to commit %s",p->poolid);
      u8_unlock_mutex(&(p->pool_commit_lock));
      return started;}

    if (commits.commit_count) {
      /* Either use_commits or kno_commit_start initialized the
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
		commits.commit_count,KNO_CHOICE_SIZE(oids),
		p->poolid);}
    else if (KNO_TRUEP(oids)) {
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
      if (commits.commit_metadata == KNO_NULL)
	commits.commit_metadata = KNO_VOID;}
    /* Otherwise, copy the current metadata if it's been modified. */
    else if (metadata_changed(p))
      commits.commit_metadata = kno_deep_copy((lispval)&(p->pool_metadata));
    else commits.commit_metadata = KNO_VOID;

    /* We've now figured out everything we're going to save */
    record_elapsed(commits.commit_times.setup);

    int w_metadata = KNO_SLOTMAPP(commits.commit_metadata);
    int commit_count = commits.commit_count;
    int written, synced, rollback = 0;

    if ( (commits.commit_count == 0) &&
	 (KNO_VOIDP(commits.commit_metadata)) ) {
      /* There's nothing to do */
      commits.commit_times.write     = 0;
      commits.commit_times.sync = 0;
      commits.commit_times.flush    = 0;
      written = 0;}
    else {
      written = p->pool_handler->commit(p,kno_commit_write,&commits);}

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
    else if (commits.commit_phase == kno_commit_sync) {
      synced = p->pool_handler->commit(p,kno_commit_sync,&commits);
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
    int flushed = p->pool_handler->commit(p,kno_commit_flush,&commits);
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
    int cleanup = p->pool_handler->commit(p,kno_commit_cleanup,&commits);
    record_elapsed(commits.commit_times.cleanup);

    if (cleanup < 0) {
      if (errno) { u8_graberrno("pool_docommit",p->poolid); }
      kno_seterr("PoolCleanupFailed","pool_commit",p->poolid,KNO_VOID);
      u8_logf(LOG_WARN,"PoolCleanupFailed","Cleanup for %s failed",p->poolid);}

    if (synced < 0)
      u8_logf(LOG_WARN,"Pool/Commit/Failed",
	      "Couldn't commit %d OIDs%s to %s after %f secs",
	      commit_count,((w_metadata) ? (" and metadata") : ("") ),
	      p->poolid,u8_elapsed_time()-start_time);

    u8_logf(LOG_NOTICE,
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
      kno_decref(commits.commit_metadata);}

    commits.commit_times.cleanup = u8_elapsed_time();

    u8_unlock_mutex(&(p->pool_commit_lock));

    return written;}
}

KNO_EXPORT int kno_commit_pool(kno_pool p,lispval oids)
{
  return pool_docommit(p,oids,NULL);
}

KNO_EXPORT int kno_commit_all_oids(kno_pool p)
{
  return kno_commit_pool(p,KNO_FALSE);
}

KNO_EXPORT int kno_commit_finished_oids(kno_pool p)
{
  return kno_commit_pool(p,KNO_TRUE);
}

/* Commitment commitment and recovery */

static void abort_commit(kno_pool p,struct KNO_POOL_COMMITS *commits)
{
  lispval *scan = commits->commit_vals, *limit = scan+commits->commit_count;
  while (scan<limit) {
    lispval v = *scan++;
    if (TABLEP(v)) {
      modify_modified(v,1);
      kno_decref(v);}}
  u8_big_free(commits->commit_vals);
  commits->commit_vals = NULL;
}

static int finish_commit(kno_pool p,struct KNO_POOL_COMMITS *commits)
{
  kno_hashtable changes = &(p->pool_changes);
  int i = 0, n = commits->commit_count, unlock_count=0;
  lispval *oids = commits->commit_oids;
  lispval *values = commits->commit_vals;
  lispval *unlock = oids;
  u8_write_lock(&(changes->table_rwlock));
  KNO_HASH_BUCKET **buckets = changes->ht_buckets;
  int n_buckets = changes->ht_n_buckets;
  while (i<n) {
    lispval oid = oids[i];
    lispval v   = values[i];
    struct KNO_KEYVAL *kv=kno_hashvec_get(oid,buckets,n_buckets);
    if (kv==NULL) {i++; continue;}
    else if (kv->kv_val != v) {
      i++;
      kno_decref(v);
      continue;}
    else if (!(CONSP(v))) {
      kv->kv_val=VOID;
      *unlock++=oids[i++];
      continue;}
    else {
      int finished=1;
      lispval cur = kv->kv_val;
      if (SLOTMAPP(v)) {
	if (KNO_TABLE_MODIFIEDP(v)) finished=0;}
      else if (SCHEMAPP(v)) {
	if (KNO_TABLE_MODIFIEDP(v)) finished=0;}
      else if (HASHTABLEP(v)) {
	if (KNO_TABLE_MODIFIEDP(v)) finished=0;}
      else {}
      /* We "swap out" the value (free, dereference, and unlock it) if:
	 1. it hasn't been modified since we picked it (finished) and
	 2. the only pointers to it are in *values and the hashtable
	 (refcount<=2) */
      if ((finished) && (KNO_REFCOUNT(v)<=2)) {
	*unlock++=oids[i];
	values[i]=VOID;
	kv->kv_val=VOID;
	kno_decref(cur);}
      kno_decref(v);
      i++;}}
  u8_rw_unlock(&(changes->table_rwlock));
  unlock_count = unlock-oids;
  if ((p->pool_handler->unlock)&&(unlock_count)) {
    lispval to_unlock = kno_init_choice
      (NULL,unlock_count,oids,KNO_CHOICE_ISATOMIC);
    int retval = p->pool_handler->unlock(p,to_unlock);
    if (retval<0) {
      u8_logf(LOG_CRIT,"UnlockFailed",
	      "Error unlocking pool %s, all %d values saved "
	      "but up to %d OIDs may still be locked",
	      p->poolid,n,unlock_count);
      kno_decref(to_unlock);
      kno_clear_errors(1);
      return retval;}
    kno_decref(to_unlock);}
  kno_devoid_hashtable(changes,0);
  return unlock_count;
}

/* Support for committing OIDs */

static int savep(lispval v,int only_finished)
{
  if (!(TABLEP(v))) return 1;
  else if (SLOTMAPP(v)) {
    if (KNO_TABLE_MODIFIEDP(v)) {
      if ((!(only_finished))||(KNO_TABLE_FINISHEDP(v))) {
	KNO_TABLE_SET_MODIFIED(v,0);
	return 1;}
      else return 0;}
    else return 0;}
  else if (SCHEMAPP(v)) {
    if (KNO_TABLE_MODIFIEDP(v)) {
      if ((!(only_finished))||(KNO_TABLE_FINISHEDP(v))) {
	KNO_TABLE_SET_MODIFIED(v,0);
	return 1;}
      else return 0;}
    else return 0;}
  else if (HASHTABLEP(v)) {
    if (KNO_TABLE_MODIFIEDP(v)) {
      if ((!(only_finished))||(KNO_TABLE_READONLYP(v))) {
	KNO_TABLE_SET_MODIFIED(v,0);
	return 1;}
      else return 0;}
    else return 0;}
  else if (only_finished) return 0;
  else return 1;
}

static int modifiedp(lispval v)
{
  if (!(TABLEP(v))) return 1;
  else if (SLOTMAPP(v))
    return KNO_TABLE_MODIFIEDP(v);
  else if (SCHEMAPP(v))
    return KNO_TABLE_MODIFIEDP(v);
  else if (HASHTABLEP(v))
    return KNO_TABLE_MODIFIEDP(v);
  else return 1;
}

static int pick_writes(kno_pool p,lispval oids,struct KNO_POOL_COMMITS *commits)
{
  if (EMPTYP(oids)) {
    commits->commit_count = 0;
    commits->commit_oids = NULL;
    commits->commit_vals = NULL;
    return 0;}
  else {
    kno_hashtable changes = &(p->pool_changes);
    int n = KNO_CHOICE_SIZE(oids);
    lispval *oidv, *values;
    commits->commit_count = n;
    commits->commit_oids = oidv = u8_big_alloc_n(n,lispval);
    commits->commit_vals = values = u8_big_alloc_n(n,lispval);
    {DO_CHOICES(oid,oids) {
	kno_pool pool = kno_oid2pool(oid);
	if (pool == p) {
	  lispval v = kno_hashtable_get(changes,oid,VOID);
	  if (v == KNO_LOCKHOLDER) {}
	  else if (savep(v,0)) {
	    *oidv++=oid;
	    *values++=v;}
	  else kno_decref(v);}}}
    commits->commit_count = oidv-commits->commit_oids;
    return commits->commit_count;}
}

static int pick_modified(kno_pool p,int finished,
			 struct KNO_POOL_COMMITS *commits)
{
  kno_hashtable changes = &(p->pool_changes); int unlock = 0;
  if (KNO_XTABLE_USELOCKP(changes)) {
    u8_read_lock(&(changes->table_rwlock));
    unlock = 1;}
  int n = changes->table_n_keys;
  lispval *oidv, *values;
  commits->commit_count = n;
  commits->commit_oids = oidv = u8_big_alloc_n(n,lispval);
  commits->commit_vals = values = u8_big_alloc_n(n,lispval);
  {
    struct KNO_HASH_BUCKET **scan = changes->ht_buckets;
    struct KNO_HASH_BUCKET **lim = scan+changes->ht_n_buckets;
    while (scan < lim) {
      if (*scan) {
	struct KNO_HASH_BUCKET *e = *scan;
	int bucket_len = e->bucket_len;
	struct KNO_KEYVAL *kvscan = &(e->kv_val0);
	struct KNO_KEYVAL *kvlimit = kvscan+bucket_len;
	while (kvscan<kvlimit) {
	  lispval key = kvscan->kv_key, val = kvscan->kv_val;
	  if (val == KNO_LOCKHOLDER) {
	    kvscan++;
	    continue;}
	  else if (savep(val,finished)) {
	    *oidv++=key;
	    *values++=val;
	    kno_incref(val);}
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

KNO_EXPORT int kno_lock_oid(lispval oid)
{
  kno_pool p = kno_oid2pool(oid);
  return kno_pool_lock(p,oid);
}

KNO_EXPORT int kno_unlock_oid(lispval oid,int commit)
{
  kno_pool p = kno_oid2pool(oid);
  return kno_pool_unlock(p,oid,commit);
}


KNO_EXPORT int kno_swapout_oid(lispval oid)
{
  kno_pool p = kno_oid2pool(oid);
  if (p == NULL)
    return kno_reterr(kno_AnonymousOID,"SET-OID_VALUE!",NULL,oid);
  else if (p->pool_handler->swapout) {
    int rv = p->pool_handler->swapout(p,oid);
    if (rv<=0) return rv;}
  else NO_ELSE;
  if (!(kno_hashtable_probe_novoid(&(p->pool_cache),oid)))
    return 0;
  else {
    kno_hashtable_store(&(p->pool_cache),oid,VOID);
    return 1;}
}

/* Operations over choices of OIDs from different pools */

typedef int (*kno_pool_op)(kno_pool p,lispval oids);

static int apply_poolop(kno_pool_op fn,lispval oids_arg)
{
  lispval oids = kno_make_simple_choice(oids_arg);
  if (OIDP(oids)) {
    kno_pool p = kno_oid2pool(oids);
    int rv = (p)?(fn(p,oids)):(0);
    kno_decref(oids);
    return rv;}
  else {
    int n = KNO_CHOICE_SIZE(oids), sum = 0;
    lispval *allocd = u8_alloc_n(n,lispval), *oidv = allocd, *write = oidv;
    {DO_CHOICES(oid,oids) {
	if (OIDP(oid)) *write++=oid;}}
    n = write-oidv;
    while (n>0) {
      kno_pool p = kno_oid2pool(oidv[0]);
      if (p == NULL) {oidv++; n--; continue;}
      else {
	lispval oids_in_pool = oidv[0];
	lispval *keep = oidv;
	int rv = 0, i = 1; while (i<n) {
	  lispval oid = oidv[i++];
	  kno_pool pool = kno_oid2pool(oid);
	  if (p == NULL) {}
	  else if (pool == p) {
	    CHOICE_ADD(oids_in_pool,oid);}
	  else *keep++=oid;}
	oids_in_pool = kno_simplify_choice(oids_in_pool);
	if (EMPTYP(oids_in_pool)) rv = 0;
	else rv = fn(p,oids_in_pool);
	if (rv>0)
	  sum = sum+rv;
	kno_decref(oids_in_pool);
	n = keep-oidv;}}
    kno_decref(oids);
    u8_free(allocd);
    return sum;}
}

KNO_EXPORT int kno_lock_oids(lispval oids_arg)
{
  return apply_poolop(kno_pool_lock,oids_arg);
}

static int commit_and_unlock(kno_pool p,lispval oids)
{
  return kno_pool_unlock(p,oids,commit_modified);
}

static int unlock_unmodified(kno_pool p,lispval oids)
{
  return kno_pool_unlock(p,oids,leave_modified);
}

static int unlock_and_discard(kno_pool p,lispval oids)
{
  return kno_pool_unlock(p,oids,discard_modified);
}

KNO_EXPORT int kno_unlock_oids(lispval oids_arg,kno_storage_unlock_flag flags)
{
  switch (flags) {
  case commit_modified:
    return apply_poolop(commit_and_unlock,oids_arg);
  case leave_modified:
    return apply_poolop(unlock_unmodified,oids_arg);
  case discard_modified:
    return apply_poolop(unlock_and_discard,oids_arg);
  default:
    kno_seterr("UnknownUnlockFlag","kno_unlock_oids",NULL,KNO_VOID);
    return -1;}
}

KNO_EXPORT int kno_swapout_oids(lispval oids)
{
  return apply_poolop(kno_pool_swapout,oids);
}

KNO_EXPORT
/* kno_prefetch_oids:
   Arguments: a dtype pointer to an oid or OID choice
   Returns:
   Sorts the OIDs by pool (ignoring non-oids) and executes
   a prefetch from each pool.
*/
int kno_prefetch_oids(lispval oids)
{
  return apply_poolop(kno_pool_prefetch,oids);
}

KNO_EXPORT int kno_commit_oids(lispval oids)
{
  return apply_poolop(kno_commit_pool,oids);
}

KNO_EXPORT int kno_finish_oids(lispval oids)
{
  return apply_poolop(kno_pool_finish,oids);
}

/* Other pool operations */

KNO_EXPORT int kno_pool_load(kno_pool p)
{
  if (p->pool_handler->getload)
    return (p->pool_handler->getload)(p);
  else {
    kno_seterr(kno_UnhandledOperation,"kno_pool_load",p->poolid,VOID);
    return -1;}
}

KNO_EXPORT void kno_pool_close(kno_pool p)
{
  if ((p) && (p->pool_handler) && (p->pool_handler->close))
    p->pool_handler->close(p);
}

/* Callable versions of simple functions */

KNO_EXPORT kno_pool _kno_oid2pool(lispval oid)
{
  int baseid = KNO_OID_BASE_ID(oid);
  int baseoff = KNO_OID_BASE_OFFSET(oid);
  struct KNO_POOL *top = kno_top_pools[baseid];
  if (top == NULL) return NULL;
  else if (baseoff<top->pool_capacity) return top;
  else if (top->pool_capacity) {
    u8_raise(_("Corrupted pool table"),"kno_oid2pool",NULL);
    return NULL;}
  else return kno_find_subpool((struct KNO_GLUEPOOL *)top,oid);
}
KNO_EXPORT lispval kno_fetch_oid(kno_pool p,lispval oid)
{
  KNOTC *knotc = ((KNO_USE_THREADCACHE)?(kno_threadcache):(NULL));
  lispval value;
  if (knotc) {
    lispval value = ((knotc->oids.table_n_keys)?
		     (kno_hashtable_get(&(knotc->oids),oid,VOID)):
		     (VOID));
    if (!(VOIDP(value))) return value;}
  if (p == NULL) p = kno_oid2pool(oid);
  if (p == NULL)
    return EMPTY;
  else if (p == kno_zero_pool)
    return kno_zero_pool_value(oid);
  else if ( (p->pool_cache_level) &&
	    (p->pool_changes.table_n_keys) &&
	    (kno_hashtable_probe_novoid(&(p->pool_changes),oid)) ) {
    /* This is where the OID is 'locked' (has an entry in the changes
       table */
    value = kno_hashtable_get(&(p->pool_changes),oid,VOID);
    if (value == KNO_LOCKHOLDER) {
      value = kno_pool_fetch(p,oid);
      if (KNO_ABORTP(value)) {
	kno_seterr("FetchFailed","kno_fetch_oid",p->poolid,oid);
	return value;}
      if (KNO_TABLEP(value)) modify_readonly(value,0);
      if ( ! ( (p->pool_flags) & (KNO_STORAGE_VIRTUAL) ) )
	kno_hashtable_store(&(p->pool_changes),oid,value);
      return value;}
    else return value;}
  if (p->pool_cache_level)
    value = kno_hashtable_get(&(p->pool_cache),oid,VOID);
  else value = VOID;
  if (VOIDP(value)) {
    if (kno_ipeval_delay(1)) {
      CHOICE_ADD(kno_pool_delays[p->pool_serialno],oid);
      return EMPTY;}
    else value = kno_pool_fetch(p,oid);}
  if (KNO_ABORTP(value)) {
    kno_seterr("FetchFailed","kno_fetch_oid",p->poolid,oid);
    return value;}
  if (knotc) kno_hashtable_store(&(knotc->oids),oid,value);
  return value;
}

KNO_EXPORT int kno_pool_storen(kno_pool p,int n,lispval *oids,lispval *values)
{
  if (n==0)
    return 0;
  else if (p==NULL)
    return 0;
  else if ((p->pool_handler) && (p->pool_handler->commit)) {
    struct KNO_POOL_COMMITS commits = {0};
    commits.commit_pool = p;
    commits.commit_count = n;
    commits.commit_oids = oids;
    commits.commit_vals = values;
    commits.commit_metadata = KNO_VOID;
    int rv = pool_docommit(p,KNO_VOID,&commits);
    return rv;}
  else if (p->pool_handler) {
    u8_seterr("NoHandler","kno_pool_storen",u8_strdup(p->poolid));
    return -1;}
  else return 0;
}

KNO_EXPORT lispval kno_pool_fetchn(kno_pool p,lispval oids_arg)
{
  if (p==NULL) return KNO_EMPTY;
  else if ((p->pool_handler) && (p->pool_handler->fetchn)) {
    lispval oids = kno_make_simple_choice(oids_arg);
    if (KNO_VECTORP(oids)) {
      int n = KNO_VECTOR_LENGTH(oids);
      if (n == 0) {
	kno_decref(oids);
	return kno_init_vector(NULL,0,NULL);}
      lispval *oidv = KNO_VECTOR_ELTS(oids);
      lispval *values = p->pool_handler->fetchn(p,n,oidv);
      kno_decref(oids);
      if (values==NULL)
	return KNO_ERROR_VALUE;
      else return kno_cons_vector(NULL,n,1,values);}
    else {
      int n = KNO_CHOICE_SIZE(oids);
      if (n==0)
	return kno_make_hashtable(NULL,8);
      else if (n==1) {
	lispval table=kno_make_hashtable(NULL,16);
	lispval value = kno_fetch_oid(p,oids);
	kno_store(table,oids,value);
	kno_decref(value);
	kno_decref(oids);
	return table;}
      else {
	init_cache_level(p);
	const lispval *oidv=KNO_CHOICE_DATA(oids);
	const lispval *values=p->pool_handler->fetchn(p,n,(lispval *)oidv);
	lispval table=kno_make_hashtable(NULL,n*3);
	kno_hashtable_iter((kno_hashtable)table,kno_table_store_noref,n,
			   oidv,values);
	u8_big_free((lispval *)values);
	kno_decref(oids);
	return table;}}}
  else if (p->pool_handler) {
    u8_seterr("NoHandler","kno_pool_fetchn",u8_strdup(p->poolid));
    return KNO_ERROR;}
  else return KNO_EMPTY;
}

/* Pools to lisp and vice versa */

KNO_EXPORT lispval kno_pool2lisp(kno_pool p)
{
  if (p == NULL)
    return KNO_ERROR;
  else if (p->pool_serialno<0)
    return kno_incref((lispval) p);
  else return LISPVAL_IMMEDIATE(kno_pool_type,p->pool_serialno);
}
KNO_EXPORT kno_pool kno_lisp2pool(lispval lp)
{
  if (TYPEP(lp,kno_pool_type)) {
    int serial = KNO_GET_IMMEDIATE(lp,kno_pool_type);
    if (serial<kno_n_pools)
      return kno_pools_by_serialno[serial];
    else {
      char buf[64];
      kno_seterr3(kno_InvalidPoolPtr,"kno_lisp2pool",
		  u8_sprintf(buf,64,"serial = 0x%x",serial));
      return NULL;}}
  else if (TYPEP(lp,kno_consed_pool_type))
    return (kno_pool) lp;
  else {
    kno_seterr(kno_TypeError,_("not a pool"),NULL,lp);
    return NULL;}
}

/* Iterating over pools */

KNO_EXPORT int kno_for_pools(int (*fcn)(kno_pool,void *),void *data)
{
  int i=0, n=kno_n_pools, count=0;

  while (i<n) {
    kno_pool p = kno_pools_by_serialno[i++];
    if (fcn(p,data))
      return count+1;
    count++;}

  if (n_consed_pools) {
    u8_lock_mutex(&consed_pools_lock);
    int j = 0; while (j<n_consed_pools) {
      kno_pool p = consed_pools[j++];
      int retval = fcn(p,data);
      count++;
      if (retval) u8_unlock_mutex(&consed_pools_lock);
      if (retval<0) return retval;
      else if (retval) break;
      else j++;}}
  return count;
}

KNO_EXPORT lispval kno_all_pools()
{
  lispval results = EMPTY;
  int i=0, n=kno_n_pools;
  while (i<n) {
    kno_pool p = kno_pools_by_serialno[i++];
    lispval lp = kno_pool2lisp(p);
    CHOICE_ADD(results,lp);}
  return results;
}

/* Finding pools by various names */

/* TODO: Add generic method which uses the pool matcher
   methods to find pools or pool sources.
*/

KNO_EXPORT kno_pool kno_find_pool(u8_string spec)
{
  kno_pool p=kno_find_pool_by_source(spec);
  if (p) return p;
  else if ((p=kno_find_pool_by_id(spec)))
    return p;
  /* TODO: Add generic method which uses the pool matcher
     methods to find pools */
  else return NULL;
}
KNO_EXPORT u8_string kno_locate_pool(u8_string spec)
{
  kno_pool p=kno_find_pool_by_source(spec);
  if (p) return u8_strdup(p->pool_source);
  else if ((p=kno_find_pool_by_id(spec)))
    return u8_strdup(p->pool_source);
  /* TODO: Add generic method which uses the pool matcher
     methods to find pools */
  else return NULL;
}

static int match_pool_id(kno_pool p,u8_string id)
{
  return ((id)&&(p->poolid)&&
	  (strcmp(p->poolid,id)==0));
}

static int match_pool_source(kno_pool p,u8_string source)
{
  return (kno_same_sourcep(source,p->canonical_source)) ||
    (kno_same_sourcep(source,p->pool_source));
}

KNO_EXPORT kno_pool kno_find_pool_by_id(u8_string id)
{
  int i=0, n=kno_n_pools;
  while (i<n) {
    kno_pool p = kno_pools_by_serialno[i++];
    if (match_pool_id(p,id))
      return p;}
  return NULL;
}

KNO_EXPORT kno_pool kno_find_pool_by_source(u8_string source)
{
  int i=0, n=kno_n_pools;
  while (i<n) {
    kno_pool p = kno_pools_by_serialno[i++];
    if (match_pool_source(p,source))
      return p;}
  return NULL;
}

KNO_EXPORT lispval kno_find_pools_by_source(u8_string source)
{
  lispval results=EMPTY;
  int i=0, n=kno_n_pools;
  while (i<n) {
    kno_pool p = kno_pools_by_serialno[i++];
    if (match_pool_source(p,source)) {
      lispval lp=kno_pool2lisp(p);
      CHOICE_ADD(results,lp);}}
  return results;
}

KNO_EXPORT kno_pool kno_find_pool_by_prefix(u8_string prefix)
{
  int i=0, n=kno_n_pools;
  while (i<n) {
    kno_pool p = kno_pools_by_serialno[i++];
    if (( (p->pool_prefix) && ((strcasecmp(prefix,p->pool_prefix)) == 0) ) ||
	( (p->pool_label) && ((strcasecmp(prefix,p->pool_label)) == 0) ))
      return p;}
  return NULL;
}

/* Operations over all pools */

static int do_swapout(kno_pool p,void *data)
{
  kno_pool_swapout(p,VOID);
  return 0;
}

KNO_EXPORT int kno_swapout_pools()
{
  return kno_for_pools(do_swapout,NULL);
}

static int do_close(kno_pool p,void *data)
{
  kno_pool_close(p);
  return 0;
}

KNO_EXPORT int kno_close_pools()
{
  return kno_for_pools(do_close,NULL);
}

static int commit_each_pool(kno_pool p,void *data)
{
  /* Phased pools shouldn't be committed by themselves */
  if ( (p->pool_flags) & (KNO_STORAGE_PHASED) ) return 0;
  int retval = kno_pool_unlock_all(p,1);
  if (retval<0)
    if (data) {
      u8_logf(LOG_CRIT,"POOL_COMMIT_FAIL",
	      "Error when committing pool %s",p->poolid);
      return 0;}
    else return -1;
  else if  (retval) {}
  else if (KNO_TABLE_MODIFIEDP(&(p->pool_metadata)))
    kno_commit_pool(p,KNO_EMPTY_CHOICE);
  else NO_ELSE;
  return 0;
}

KNO_EXPORT int kno_commit_pools()
{
  return kno_for_pools(commit_each_pool,(void *)NULL);
}

KNO_EXPORT int kno_commit_pools_noerr()
{
  return kno_for_pools(commit_each_pool,(void *)"NOERR");
}

static int do_unlock(kno_pool p,void *data)
{
  int *commitp = (int *)data;
  kno_pool_unlock_all(p,*commitp);
  return 0;
}

KNO_EXPORT int kno_unlock_pools(int commitp)
{
  return kno_for_pools(do_unlock,&commitp);
}

static int accumulate_cachecount(kno_pool p,void *ptr)
{
  int *count = (int *)ptr;
  *count = *count+p->pool_cache.table_n_keys;
  return 0;
}

KNO_EXPORT
long kno_object_cache_load()
{
  int result = 0, retval;
  retval = kno_for_pools(accumulate_cachecount,(void *)&result);
  if (retval<0) return retval;
  else return result;
}

static int accumulate_cached(kno_pool p,void *ptr)
{
  lispval *vals = (lispval *)ptr;
  lispval keys = kno_hashtable_keys(&(p->pool_cache));
  CHOICE_ADD(*vals,keys);
  return 0;
}

KNO_EXPORT
lispval kno_cached_oids(kno_pool p)
{
  if (p == NULL) {
    int retval; lispval result = EMPTY;
    retval = kno_for_pools(accumulate_cached,(void *)&result);
    if (retval<0) {
      kno_decref(result);
      return KNO_ERROR;}
    else return result;}
  else return kno_hashtable_keys(&(p->pool_cache));
}

static int accumulate_changes(kno_pool p,void *ptr)
{
  if (p->pool_changes.table_n_keys) {
    lispval *vals = (lispval *)ptr;
    lispval keys = kno_hashtable_keys(&(p->pool_changes));
    CHOICE_ADD(*vals,keys);}
  return 0;
}

KNO_EXPORT
lispval kno_changed_oids(kno_pool p)
{
  if (p == NULL) {
    int retval; lispval result = EMPTY;
    retval = kno_for_pools(accumulate_changes,(void *)&result);
    if (retval<0) {
      kno_decref(result);
      return KNO_ERROR;}
    else return result;}
  else return kno_hashtable_keys(&(p->pool_changes));
}

/* Common pool initialization stuff */

KNO_EXPORT void kno_init_pool(kno_pool p,
			      KNO_OID base,unsigned int capacity,
			      struct KNO_POOL_HANDLER *h,
			      u8_string id,u8_string source,u8_string csource,
			      kno_storage_flags flags,
			      lispval metadata,
			      lispval opts)
{
  KNO_INIT_CONS(p,kno_consed_pool_type);
  p->pool_base = base;
  p->pool_capacity = capacity;
  p->pool_source = u8_strdup(source);
  p->canonical_source = u8_strdup(csource);
  p->poolid = u8_strdup(id);
  p->pool_typeid = u8_strdup(h->name);
  p->pool_handler = h;
  p->pool_flags = kno_get_dbflags(opts,flags);
  p->pool_serialno = -1; p->pool_cache_level = -1;
  p->pool_adjuncts = NULL;
  p->pool_adjuncts_len = 0;
  p->pool_n_adjuncts = 0;
  p->pool_label = NULL;
  p->pool_prefix = NULL;
  p->pool_namefn = VOID;

  lispval ll = kno_getopt(opts,KNOSYM_LOGLEVEL,KNO_VOID);
  if (KNO_VOIDP(ll))
    p->pool_loglevel = -1;
  else if ( (KNO_FIXNUMP(ll)) && ( (KNO_FIX2INT(ll)) >= 0 ) &&
	    ( (KNO_FIX2INT(ll)) < U8_MAX_LOGLEVEL ) )
    p->pool_loglevel = KNO_FIX2INT(ll);
  else {
    u8_log(LOG_WARN,"BadLogLevel",
	   "Invalid loglevel %q for pool %s",ll,id);
    p->pool_loglevel = -1;}
  kno_decref(ll);

  if ( (KNO_VOIDP(opts)) || (KNO_FALSEP(opts)) )
    p->pool_opts = KNO_FALSE;
  else p->pool_opts = kno_incref(opts);

  /* Data tables */
  KNO_INIT_STATIC_CONS(&(p->pool_cache),kno_hashtable_type);
  KNO_INIT_STATIC_CONS(&(p->pool_changes),kno_hashtable_type);
  kno_make_hashtable(&(p->pool_cache),kno_pool_cache_init);
  kno_make_hashtable(&(p->pool_changes),kno_pool_lock_init);

  /* Metadata tables */
  KNO_INIT_STATIC_CONS(&(p->pool_props),kno_slotmap_type);
  kno_init_slotmap(&(p->pool_props),17,NULL);

  KNO_INIT_STATIC_CONS(&(p->pool_metadata),kno_slotmap_type);
  if (KNO_SLOTMAPP(metadata)) {
    lispval adj = kno_get(metadata,KNOSYM_ADJUNCT,KNO_VOID);
    if ( (KNO_OIDP(adj)) || (KNO_TRUEP(adj)) || (KNO_SYMBOLP(adj)) )
      p->pool_flags |= KNO_POOL_ADJUNCT;
    else kno_decref(adj);
    kno_copy_slotmap((kno_slotmap)metadata,&(p->pool_metadata));}
  else {
    kno_init_slotmap(&(p->pool_metadata),17,NULL);}
  KNO_XTABLE_SET_MODIFIED(&(p->pool_metadata),0);

  u8_init_rwlock(&(p->pool_struct_lock));
  u8_init_mutex(&(p->pool_commit_lock));
}

KNO_EXPORT int kno_pool_set_metadata(kno_pool p,lispval metadata)
{
  if (KNO_SLOTMAPP(metadata)) {
    if (p->pool_metadata.n_allocd) {
      struct KNO_SLOTMAP *sm = & p->pool_metadata;
      if ( (KNO_XTABLE_BITP(sm,KNO_SLOTMAP_FREE_KEYVALS)) && (sm->sm_keyvals) )
	u8_free(sm->sm_keyvals);
      u8_destroy_rwlock(&(sm->table_rwlock));}
    lispval adj = kno_get(metadata,KNOSYM_ADJUNCT,KNO_VOID);
    if ( (KNO_OIDP(adj)) || (KNO_TRUEP(adj)) || (KNO_SYMBOLP(adj)) )
      p->pool_flags |= KNO_POOL_ADJUNCT;
    else kno_decref(adj);
    kno_copy_slotmap((kno_slotmap)metadata,&(p->pool_metadata));}
  else {
    KNO_INIT_STATIC_CONS(&(p->pool_metadata),kno_slotmap_type);
    kno_init_slotmap(&(p->pool_metadata),17,NULL);}
  KNO_XTABLE_SET_MODIFIED(&(p->pool_metadata),0);
  return 0;
}

KNO_EXPORT void kno_set_pool_namefn(kno_pool p,lispval namefn)
{
  lispval oldnamefn = p->pool_namefn;
  kno_incref(namefn);
  p->pool_namefn = namefn;
  kno_decref(oldnamefn);
}

/* GLUEPOOL handler (empty) */

static struct KNO_POOL_HANDLER gluepool_handler={
  "gluepool", 1, sizeof(struct KNO_GLUEPOOL), 11,
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


KNO_EXPORT kno_pool kno_get_pool
(u8_string spec,kno_storage_flags flags,lispval opts)
{
  if (strchr(spec,';')) {
    kno_pool p = NULL;
    u8_byte *copy = u8_strdup(spec), *start = copy;
    u8_byte *brk = strchr(start,';');
    while (brk) {
      if (p == NULL) {
	*brk='\0'; p = kno_open_pool(start,flags,opts);
	if (p) {brk = NULL; start = NULL;}
	else {
	  start = brk+1;
	  brk = strchr(start,';');}}}
    if (p) return p;
    else if ((start)&&(*start)) {
      int start_off = start-copy;
      u8_free(copy);
      return kno_open_pool(spec+start_off,flags,opts);}
    else return NULL;}
  else return kno_open_pool(spec,flags,opts);
}

KNO_EXPORT kno_pool kno_use_pool
(u8_string spec,kno_storage_flags flags,lispval opts)
{
  return kno_get_pool(spec,flags&(~KNO_STORAGE_UNREGISTERED),opts);
}

KNO_EXPORT kno_pool kno_name2pool(u8_string spec)
{
  lispval label_string = kno_make_string(NULL,-1,spec);
  lispval poolv = kno_hashtable_get(&poolid_table,label_string,VOID);
  kno_decref(label_string);
  if (VOIDP(poolv)) return NULL;
  else return kno_lisp2pool(poolv);
}

static void display_pool(u8_output out,kno_pool p,lispval lp)
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
  int is_adjunct = ( (p->pool_flags) & (KNO_POOL_ADJUNCT) );
  strcpy(addrbuf,"@");
  strcat(addrbuf,u8_uitoa16(KNO_OID_HI(p->pool_base),numbuf));
  strcat(addrbuf,"/");
  strcat(addrbuf,u8_uitoa16(KNO_OID_LO(p->pool_base),numbuf));
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
  kno_pool p = kno_lisp2pool(x);
  if (p == NULL) return 0;
  display_pool(out,p,x);
  return 1;
}

static int unparse_consed_pool(u8_output out,lispval x)
{
  kno_pool p = (kno_pool)x;
  if (p == NULL) return 0;
  display_pool(out,p,x);
  return 1;
}

static lispval pool_parsefn(int n,lispval *args,kno_typeinfo e)
{
  kno_pool p = NULL;
  if (n<3) return VOID;
  else if (n==3)
    p = kno_use_pool(KNO_STRING_DATA(args[2]),0,VOID);
  else if ((STRINGP(args[2])) &&
	   ((p = kno_find_pool_by_prefix(KNO_STRING_DATA(args[2]))) == NULL))
    p = kno_use_pool(KNO_STRING_DATA(args[3]),0,VOID);
  if (p) return kno_pool2lisp(p);
  else return kno_err(kno_CantParseRecord,"pool_parsefn",NULL,VOID);
}

/* Executing pool delays */

KNO_EXPORT int kno_execute_pool_delays(kno_pool p,void *data)
{
  lispval todo = kno_pool_delays[p->pool_serialno];
  if (EMPTYP(todo)) return 0;
  else {
    /* u8_lock_mutex(&(kno_ipeval_lock)); */
    todo = kno_pool_delays[p->pool_serialno];
    kno_pool_delays[p->pool_serialno]=EMPTY;
    todo = kno_simplify_choice(todo);
    /* u8_unlock_mutex(&(kno_ipeval_lock)); */
#if KNO_TRACE_IPEVAL
    if (kno_trace_ipeval>1)
      u8_logf(LOG_DEBUG,ipeval_objfetch,"Fetching %d oids from %s: %q",
	      KNO_CHOICE_SIZE(todo),p->poolid,todo);
    else if (kno_trace_ipeval)
      u8_logf(LOG_DEBUG,ipeval_objfetch,"Fetching %d oids from %s",
	      KNO_CHOICE_SIZE(todo),p->poolid);
#endif
    kno_pool_prefetch(p,todo);
#if KNO_TRACE_IPEVAL
    if (kno_trace_ipeval)
      u8_logf(LOG_DEBUG,ipeval_objfetch,"Fetched %d oids from %s",
	      KNO_CHOICE_SIZE(todo),p->poolid);
#endif
    return 0;}
}

/* Raw pool table operations */

static lispval pool_get(kno_pool p,lispval key,lispval dflt)
{
  if (OIDP(key)) {
    KNO_OID addr = KNO_OID_ADDR(key);
    KNO_OID base = p->pool_base;
    if (KNO_OID_COMPARE(addr,base)<0)
      return kno_incref(dflt);
    else {
      unsigned int offset = KNO_OID_DIFFERENCE(addr,base);
      unsigned int capacity = p->pool_capacity;
      int load = kno_pool_load(p);
      if (load<0)
	return KNO_ERROR;
      else if ((p->pool_flags&KNO_POOL_ADJUNCT) ?
	       (offset<capacity) :
	       (offset<load))
	return kno_fetch_oid(p,key);
      else return kno_incref(dflt);}}
  else return kno_incref(dflt);
}

static lispval pool_tableget(lispval arg,lispval key,lispval dflt)
{
  kno_pool p=kno_lisp2pool(arg);
  if (p)
    return pool_get(p,key,dflt);
  else return kno_incref(dflt);
}

KNO_EXPORT lispval kno_pool_get(kno_pool p,lispval key)
{
  return pool_get(p,key,EMPTY);
}

static int pool_store(kno_pool p,lispval key,lispval value)
{
  if (OIDP(key)) {
    KNO_OID addr = KNO_OID_ADDR(key);
    KNO_OID base = p->pool_base;
    if (KNO_OID_COMPARE(addr,base)<0) {
      kno_seterr(kno_PoolRangeError,"pool_store",kno_pool_id(p),key);
      return -1;}
    else {
      unsigned int offset = KNO_OID_DIFFERENCE(addr,base);
      modify_modified(value,1);
      int cap = p->pool_capacity, rv = -1;
      if (offset>cap) {
	kno_seterr(kno_PoolRangeError,"pool_store",kno_pool_id(p),key);
	return -1;}
      else if ( (p->pool_flags & KNO_STORAGE_VIRTUAL) &&
		(p->pool_handler->commit) )
	rv=kno_pool_storen(p,1,&key,&value);
      else if (p->pool_handler->lock == NULL) {
	rv = kno_hashtable_store(&(p->pool_cache),key,value);}
      else if (kno_pool_lock(p,key)) {
	rv = kno_hashtable_store(&(p->pool_changes),key,value);}
      else {
	kno_seterr(kno_CantLockOID,"pool_store",kno_pool_id(p),key);
	return -1;}
      return rv;}}
  else {
    kno_seterr(kno_NotAnOID,"pool_store",kno_pool_id(p),kno_incref(key));
    return -1;}
}

static lispval pool_tablestore(lispval arg,lispval key,lispval val)
{
  kno_pool p=kno_lisp2pool(arg);
  if (p) {
    int rv=pool_store(p,key,val);
    if (rv<0)
      return KNO_ERROR;
    else return KNO_TRUE;}
  else return kno_type_error("pool","pool_tablestore",arg);
}

KNO_EXPORT int kno_pool_store(kno_pool p,lispval key,lispval value)
{
  return pool_store(p,key,value);
}

KNO_EXPORT lispval kno_pool_keys(lispval arg)
{
  lispval results = EMPTY;
  kno_pool p = kno_lisp2pool(arg);
  if (p==NULL)
    return kno_type_error("pool","pool_tablekeys",arg);
  else {
    KNO_OID base = p->pool_base;
    unsigned int i = 0; int load = kno_pool_load(p);
    if (load<0) return KNO_ERROR;
    while (i<load) {
      lispval each = kno_make_oid(KNO_OID_PLUS(base,i));
      CHOICE_ADD(results,each);
      i++;}
    return results;}
}

static void recycle_consed_pool(struct KNO_RAW_CONS *c)
{
  struct KNO_POOL *p = (struct KNO_POOL *)c;
  struct KNO_POOL_HANDLER *handler = p->pool_handler;
  if (p->pool_serialno>=0)
    return;
  else if (p->pool_handler == &gluepool_handler)
    return;
  else {}
  drop_consed_pool(p);
  if (handler->recycle) handler->recycle(p);
  kno_recycle_hashtable(&(p->pool_cache));
  kno_recycle_hashtable(&(p->pool_changes));
  u8_free(p->poolid);
  u8_free(p->pool_source);
  if (p->pool_label) u8_free(p->pool_label);
  if (p->pool_prefix) u8_free(p->pool_prefix);
  if (p->pool_typeid) u8_free(p->pool_typeid);

  struct KNO_ADJUNCT *adjuncts = p->pool_adjuncts;
  int n_adjuncts = p->pool_n_adjuncts;
  if ( (adjuncts) && (n_adjuncts) ) {
    int i = 0; while (i<n_adjuncts) {
      kno_decref(adjuncts[i].table);
      adjuncts[i].table = VOID;
      i++;}}
  if (adjuncts) u8_free(adjuncts);
  p->pool_adjuncts = NULL;

  kno_free_slotmap(&(p->pool_metadata));
  kno_free_slotmap(&(p->pool_props));

  kno_decref(p->pool_namefn);
  p->pool_namefn = VOID;

  kno_decref(p->pool_opts);
  p->pool_opts = VOID;

  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
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
  if (KNO_VOIDP(v)) return;
  kno_store(md,slot,v);
  kno_decref(v);
}
static void mdstring(lispval md,lispval slot,u8_string s)
{
  if (s==NULL) return;
  lispval v=knostring(s);
  kno_store(md,slot,v);
  kno_decref(v);
}

static lispval metadata_readonly_props = KNO_VOID;

KNO_EXPORT lispval kno_pool_base_metadata(kno_pool p)
{
  int flags=p->pool_flags;
  lispval metadata=kno_deep_copy((lispval) &(p->pool_metadata));
  mdstore(metadata,core_slot,kno_deep_copy((lispval) &(p->pool_metadata)));
  mdstore(metadata,base_slot,kno_make_oid(p->pool_base));
  mdstore(metadata,capacity_slot,KNO_INT(p->pool_capacity));
  mdstore(metadata,cachelevel_slot,KNO_INT(p->pool_cache_level));
  mdstring(metadata,poolid_slot,p->poolid);
  mdstring(metadata,source_slot,p->pool_source);
  mdstring(metadata,label_slot,p->pool_label);
  if ((p->pool_handler) && (p->pool_handler->name))
    mdstring(metadata,KNOSYM_TYPE,(p->pool_handler->name));
  mdstore(metadata,cached_slot,KNO_INT(p->pool_cache.table_n_keys));
  mdstore(metadata,locked_slot,KNO_INT(p->pool_changes.table_n_keys));

  if (U8_BITP(flags,KNO_STORAGE_READ_ONLY))
    kno_add(metadata,flags_slot,read_only_flag);
  if (U8_BITP(flags,KNO_STORAGE_UNREGISTERED))
    kno_add(metadata,flags_slot,unregistered_flag);
  else {
    kno_add(metadata,flags_slot,registered_flag);
    kno_store(metadata,registered_slot,KNO_INT(p->pool_serialno));}
  if (U8_BITP(flags,KNO_STORAGE_NOSWAP))
    kno_add(metadata,flags_slot,noswap_flag);
  if (U8_BITP(flags,KNO_STORAGE_NOERR))
    kno_add(metadata,flags_slot,noerr_flag);
  if (U8_BITP(flags,KNO_STORAGE_PHASED))
    kno_add(metadata,flags_slot,phased_flag);
  if (U8_BITP(flags,KNO_POOL_SPARSE))
    kno_add(metadata,flags_slot,sparse_flag);
  if (U8_BITP(flags,KNO_POOL_VIRTUAL))
    kno_add(metadata,flags_slot,virtual_flag);
  if (U8_BITP(flags,KNO_POOL_NOLOCKS))
    kno_add(metadata,flags_slot,nolocks_flag);

  if (kno_testopt(metadata,KNOSYM_ADJUNCT,KNO_VOID))
    kno_add(metadata,flags_slot,KNOSYM_ISADJUNCT);
  else kno_add(metadata,flags_slot,background_flag);

  if (U8_BITP(flags,KNO_POOL_ADJUNCT))
    kno_add(metadata,flags_slot,KNOSYM_ADJUNCT);

  lispval props_copy = kno_copier(((lispval)&(p->pool_props)),0);
  kno_store(metadata,KNOSYM_PROPS,props_copy);
  kno_decref(props_copy);

  if (p->pool_n_adjuncts) {
    int i=0, n=p->pool_n_adjuncts;
    struct KNO_ADJUNCT *adjuncts=p->pool_adjuncts;
    lispval adjuncts_table=kno_make_slotmap(n,0,NULL);
    while (i<n) {
      kno_store(adjuncts_table,
		adjuncts[i].slotid,
		adjuncts[i].table);
      i++;}
    kno_store(metadata,_adjuncts_slot,adjuncts_table);
    kno_decref(adjuncts_table);}

  if (KNO_TABLEP(p->pool_opts))
    kno_store(metadata,opts_slot,p->pool_opts);

  kno_add(metadata,metadata_readonly_props,KNOSYM_TYPE);
  kno_add(metadata,metadata_readonly_props,base_slot);
  kno_add(metadata,metadata_readonly_props,capacity_slot);
  kno_add(metadata,metadata_readonly_props,cachelevel_slot);
  kno_add(metadata,metadata_readonly_props,poolid_slot);
  kno_add(metadata,metadata_readonly_props,label_slot);
  kno_add(metadata,metadata_readonly_props,source_slot);
  kno_add(metadata,metadata_readonly_props,locked_slot);
  kno_add(metadata,metadata_readonly_props,cached_slot);
  kno_add(metadata,metadata_readonly_props,flags_slot);

  kno_add(metadata,metadata_readonly_props,KNOSYM_PROPS);
  kno_add(metadata,metadata_readonly_props,opts_slot);
  kno_add(metadata,metadata_readonly_props,core_slot);

  return metadata;
}

KNO_EXPORT lispval kno_default_poolctl(kno_pool p,lispval op,int n,kno_argvec args)
{
  if ((n>0)&&(args == NULL))
    return kno_err("BadPoolOpCall","kno_default_poolctl",p->poolid,VOID);
  else if (n<0)
    return kno_err("BadPoolOpCall","kno_default_poolctl",p->poolid,VOID);
  else if (op == kno_label_op) {
    if (n==0) {
      if (!(p->pool_label))
	return KNO_FALSE;
      else return kno_mkstring(p->pool_label);}
    else return KNO_FALSE;}
  else if (op == KNOSYM_OPTS)  {
    lispval opts = p->pool_opts;
    if (n > 1)
      return kno_err(kno_TooManyArgs,"kno_default_indexctl",p->poolid,VOID);
    else if ( (opts == KNO_NULL) || (VOIDP(opts) ) )
      return KNO_FALSE;
    else if ( n == 1 )
      return kno_getopt(opts,args[0],KNO_FALSE);
    else return kno_incref(opts);}
  else if (op == kno_metadata_op) {
    lispval metadata = (lispval) &(p->pool_metadata);
    lispval slotid = (n>0) ? (args[0]) : (KNO_VOID);
    /* TODO: check that slotid isn't any of the slots returned by default */
    if (n == 0)
      return kno_pool_base_metadata(p);
    else if (n == 1) {
      lispval extended=kno_pool_ctl(p,kno_metadata_op,0,NULL);
      lispval v = kno_get(extended,slotid,KNO_EMPTY);
      kno_decref(extended);
      return v;}
    else if (n == 2) {
      lispval extended=kno_pool_ctl(p,kno_metadata_op,0,NULL);
      if (kno_test(extended,KNOSYM_READONLY,slotid)) {
	kno_decref(extended);
	return kno_err("ReadOnlyMetadataProperty","kno_default_poolctl",
		       p->poolid,slotid);}
      else kno_decref(extended);
      int rv=kno_store(metadata,slotid,args[1]);
      if (rv<0)
	return KNO_ERROR_VALUE;
      else return kno_incref(args[1]);}
    else return kno_err(kno_TooManyArgs,"kno_pool_ctl/metadata",
			KNO_SYMBOL_NAME(op),kno_pool2lisp(p));}
  else if (op == adjuncts_slot) {
    if (n == 0) {
      if (p->pool_n_adjuncts) {
	int i=0, n=p->pool_n_adjuncts;
	struct KNO_ADJUNCT *adjuncts=p->pool_adjuncts;
	lispval adjuncts_table=kno_make_slotmap(n,0,NULL);
	while (i<n) {
	  kno_store(adjuncts_table,
		    adjuncts[i].slotid,
		    adjuncts[i].table);
	  i++;}
	return adjuncts_table;}
      else return kno_empty_slotmap();}
    else if (n == 1) {
      if (p->pool_n_adjuncts) {
	lispval slotid = args[0];
	struct KNO_ADJUNCT *adjuncts=p->pool_adjuncts;
	int i=0, n = p->pool_n_adjuncts;
	while (i<n) {
	  if (adjuncts[i].slotid == slotid)
	    return kno_incref(adjuncts[i].table);
	  i++;}
	return KNO_FALSE;}
      else return KNO_FALSE;}
    else if (n == 2) {
      lispval slotid = args[0];
      lispval adjunct = args[1];
      int rv = kno_set_adjunct(p,slotid,adjunct);
      if (rv<0)
	return KNO_ERROR_VALUE;
      else return adjunct;}
    else return kno_err(kno_TooManyArgs,"kno_pool_ctl/adjuncts",
			KNO_SYMBOL_NAME(op),kno_pool2lisp(p));}
  else if (op == KNOSYM_PROPS) {
    lispval props = (lispval) &(p->pool_props);
    lispval slotid = (n>0) ? (args[0]) : (KNO_VOID);
    if (n == 0)
      return kno_copier(props,0);
    else if (n == 1)
      return kno_get(props,slotid,KNO_EMPTY);
    else if (n == 2) {
      int rv=kno_store(props,slotid,args[1]);
      if (rv<0)
	return KNO_ERROR_VALUE;
      else return kno_incref(args[1]);}
    else return kno_err(kno_TooManyArgs,"kno_pool_ctl/props",
			KNO_SYMBOL_NAME(op),kno_pool2lisp(p));}
  else if (op == kno_capacity_op)
    return KNO_INT(p->pool_capacity);
  else if (op == KNOSYM_LOGLEVEL) {
    if (n == 0) {
      if (p->pool_loglevel < 0)
	return KNO_FALSE;
      else return KNO_INT(p->pool_loglevel);}
    else if (n == 1) {
      if (FIXNUMP(args[0])) {
	long long level = KNO_FIX2INT(args[0]);
	if ((level<0) || (level > 128))
	  return kno_err(kno_RangeError,"kno_default_poolctl",p->poolid,args[0]);
	else {
	  int old_loglevel = p->pool_loglevel;
	  p->pool_loglevel = level;
	  if (old_loglevel<0)
	    return KNO_FALSE;
	  else return KNO_INT(old_loglevel);}}
      else return kno_type_error("loglevel","kno_default_poolctl",args[0]);}
    else return kno_err(kno_TooManyArgs,"kno_default_poolctl",p->poolid,VOID);}
  else if (op == kno_partitions_op)
    return KNO_EMPTY;
  else if (op == KNOSYM_CACHELEVEL)
    return KNO_INT2FIX(1);
  else if (op == kno_raw_metadata_op)
    return kno_deep_copy((lispval) &(p->pool_metadata));
  else if (op == KNOSYM_READONLY) {
    if (U8_BITP((p->pool_flags),KNO_STORAGE_READ_ONLY))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (op == KNOSYM_ADJUNCT) {
    if (U8_BITP((p->pool_flags),KNO_POOL_ADJUNCT)) {
      lispval slotid = kno_slotmap_get
	(&(p->pool_metadata),KNOSYM_ADJUNCT,KNO_VOID);
      if ( (KNO_OIDP(slotid)) || (KNO_SYMBOLP(slotid)) )
	return slotid;
      else {
	kno_decref(slotid);
	return KNO_TRUE;}}
    else return KNO_FALSE;}
  else if (op == KNOSYM_TYPE)
    return kno_intern(p->pool_typeid);
  else {
    u8_log(LOG_WARN,"Unhandled POOLCTL op",
	   "Couldn't handle %q for %s",op,p->poolid);
    return KNO_FALSE;}
}

static lispval copy_consed_pool(lispval x,int deep)
{
  return kno_incref(x);
}

/* Initialization */

kno_lisp_type kno_consed_pool_type;

static int check_pool(lispval x)
{
  int serial = KNO_GET_IMMEDIATE(x,kno_pool_type);
  if (serial<0) return 0;
  else if (serial<kno_n_pools) return 1;
  else return 0;
}

/* This is just for use from the debugger, so we can allocate it
   statically. */
static char oid_info_buf[512];

static u8_string _more_oid_info(lispval oid)
{
  if (OIDP(oid)) {
    KNO_OID addr = KNO_OID_ADDR(oid);
    kno_pool p = kno_oid2pool(oid);
    unsigned int hi = KNO_OID_HI(addr), lo = KNO_OID_LO(addr);
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

KNO_EXPORT lispval kno_poolconfig_get(lispval var,void *vptr)
{
  kno_pool *pptr = (kno_pool *)vptr;
  if (*pptr == NULL)
    return kno_err(_("Config mis-config"),"pool_config_get",NULL,var);
  else {
    if (*pptr)
      return kno_pool2lisp(*pptr);
    else return EMPTY;}
}
KNO_EXPORT int kno_poolconfig_set(lispval ignored,lispval v,void *vptr)
{
  kno_pool *ppid = (kno_pool *)vptr;
  if (KNO_POOLP(v)) *ppid = kno_lisp2pool(v);
  else if (STRINGP(v)) {
    kno_pool p = kno_use_pool(CSTRING(v),0,VOID);
    if (p) *ppid = p; else return -1;}
  else return kno_type_error(_("pool spec"),"pool_config_set",v);
  return 1;
}

/* The zero pool */

struct KNO_POOL _kno_zero_pool;
static u8_mutex zero_pool_alloc_lock;
kno_pool kno_default_pool = &_kno_zero_pool;

static lispval zero_pool_alloc(kno_pool p,int n)
{
  int start = -1;
  u8_lock_mutex(&zero_pool_alloc_lock);
  start = kno_zero_pool_load;
  kno_zero_pool_load = start+n;
  u8_unlock_mutex(&zero_pool_alloc_lock);
  if (n==1)
    return kno_make_oid(KNO_MAKE_OID(0,start));
  else {
    struct KNO_CHOICE *ch = kno_alloc_choice(n);
    KNO_INIT_CONS(ch,kno_cons_type);
    ch->choice_size = n;
    lispval *results = &(ch->choice_0);
    int i = 0; while (i<n) {
      KNO_OID addr = KNO_MAKE_OID(0,start+i);
      lispval oid = kno_make_oid(addr);
      results[i]=oid;
      i++;}
    return (lispval)kno_cleanup_choice(ch,KNO_CHOICE_DOSORT);}
}

static int zero_pool_getload(kno_pool p)
{
  return kno_zero_pool_load;
}

static lispval zero_pool_fetch(kno_pool p,lispval oid)
{
  return kno_zero_pool_value(oid);
}

static lispval *zero_pool_fetchn(kno_pool p,int n,lispval *oids)
{
  lispval *results = u8_alloc_n(n,lispval);
  int i = 0; while (i<n) {
    results[i]=kno_zero_pool_value(oids[i]);
    i++;}
  return results;
}

static int zero_pool_lock(kno_pool p,lispval oids)
{
  return 1;
}
static int zero_pool_unlock(kno_pool p,lispval oids)
{
  return 0;
}

static struct KNO_POOL_HANDLER zero_pool_handler={
  "zero_pool", 1, sizeof(struct KNO_POOL), 12,
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
  KNO_INIT_STATIC_CONS(&_kno_zero_pool,kno_pool_type);
  _kno_zero_pool.pool_serialno = -1;
  _kno_zero_pool.poolid = u8_strdup("_kno_zero_pool");
  _kno_zero_pool.pool_source = u8_strdup("init_kno_zero_pool");
  _kno_zero_pool.pool_label = u8_strdup("_kno_zero_pool");
  _kno_zero_pool.pool_typeid = u8_strdup("_mempool");
  _kno_zero_pool.pool_base = 0;
  _kno_zero_pool.pool_capacity = 0x100000; /* About a million */
  _kno_zero_pool.pool_flags = 0;
  _kno_zero_pool.modified_flags = 0;
  _kno_zero_pool.pool_handler = &zero_pool_handler;
  u8_init_rwlock(&(_kno_zero_pool.pool_struct_lock));
  u8_init_mutex(&(_kno_zero_pool.pool_commit_lock));
  kno_init_hashtable(&(_kno_zero_pool.pool_cache),0,0);
  kno_init_hashtable(&(_kno_zero_pool.pool_changes),0,0);
  _kno_zero_pool.pool_n_adjuncts = 0;
  _kno_zero_pool.pool_adjuncts_len = 0;
  _kno_zero_pool.pool_adjuncts = NULL;
  _kno_zero_pool.pool_prefix="";
  _kno_zero_pool.pool_namefn = VOID;
  _kno_zero_pool.pool_opts = KNO_FALSE;
  kno_register_pool(&_kno_zero_pool);
}

/* Lisp pointers to Pool pointers */

KNO_EXPORT kno_pool _kno_poolptr(lispval x)
{
  return kno_poolptr(x);
}

/* Initialization */

KNO_EXPORT void kno_init_pools_c()
{
  int i = 0; while (i < 1024) kno_top_pools[i++]=NULL;

  u8_register_source_file(_FILEINFO);

  kno_type_names[kno_pool_type]=_("pool");
  kno_immediate_checkfns[kno_pool_type]=check_pool;

  kno_consed_pool_type = kno_register_cons_type("raw pool");

  kno_type_names[kno_consed_pool_type]=_("raw pool");

  _kno_oid_info=_more_oid_info;

  lock_symbol = kno_intern("lock");
  unlock_symbol = kno_intern("unlock");

  base_slot=kno_intern("base");
  capacity_slot=kno_intern("capacity");
  cachelevel_slot=kno_intern("cachelevel");
  label_slot=kno_intern("label");
  poolid_slot=kno_intern("poolid");
  source_slot=kno_intern("source");
  adjuncts_slot=kno_intern("adjuncts");
  _adjuncts_slot=kno_intern("_adjuncts");
  cached_slot=kno_intern("cached");
  locked_slot=kno_intern("locked");
  flags_slot=kno_intern("flags");
  registered_slot=kno_intern("registered");
  opts_slot=kno_intern("opts");
  core_slot=kno_intern("core");

  read_only_flag=KNOSYM_READONLY;
  unregistered_flag=kno_intern("unregistered");
  registered_flag=kno_intern("registered");
  noswap_flag=kno_intern("noswap");
  noerr_flag=kno_intern("noerr");
  phased_flag=kno_intern("phased");
  sparse_flag=kno_intern("sparse");
  background_flag=kno_intern("background");
  virtual_flag=kno_intern("virtual");
  nolocks_flag=kno_intern("nolocks");

  metadata_readonly_props = kno_intern("_readonly_props");

  memset(&kno_top_pools,0,sizeof(kno_top_pools));
  memset(&kno_pools_by_serialno,0,sizeof(kno_top_pools));

  u8_init_mutex(&consed_pools_lock);
  consed_pools = u8_malloc(64*sizeof(kno_pool));
  consed_pools_len=64;

  {
    struct KNO_TYPEINFO *e = kno_use_typeinfo(kno_intern("pool"));
    e->type_parsefn = pool_parsefn;}

  KNO_INIT_STATIC_CONS(&poolid_table,kno_hashtable_type);
  kno_make_hashtable(&poolid_table,-1);

#if (KNO_USE_TLS)
  u8_new_threadkey(&kno_pool_delays_key,NULL);
#endif
  kno_unparsers[kno_pool_type]=unparse_pool;
  kno_unparsers[kno_consed_pool_type]=unparse_consed_pool;
  kno_recyclers[kno_consed_pool_type]=recycle_consed_pool;
  kno_copiers[kno_consed_pool_type]=copy_consed_pool;
  kno_register_config("DEFAULTPOOL",_("Default location for new OID allocation"),
		      kno_poolconfig_get,kno_poolconfig_set,
		      &kno_default_pool);

  kno_tablefns[kno_pool_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_pool_type]->get = (kno_table_get_fn)pool_tableget;
  kno_tablefns[kno_pool_type]->store = (kno_table_store_fn)pool_tablestore;
  kno_tablefns[kno_pool_type]->keys = (kno_table_keys_fn)kno_pool_keys;

  kno_tablefns[kno_consed_pool_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_consed_pool_type]->get = (kno_table_get_fn)pool_tableget;
  kno_tablefns[kno_consed_pool_type]->store = (kno_table_store_fn)pool_tablestore;
  kno_tablefns[kno_consed_pool_type]->keys = (kno_table_keys_fn)kno_pool_keys;

  init_zero_pool();

}

