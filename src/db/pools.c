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
#include "framerd/fddb.h"
#include "framerd/apply.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8netfns.h>
#include <libu8/u8printf.h>

#define COMMIT_FINISHED (FD_POOL_COMMIT_UNLOCK|FD_POOL_COMMIT_FINISHED)

fd_exception fd_UnknownPool=_("Unknown pool");
fd_exception fd_CantLockOID=_("Can't lock OID");
fd_exception fd_CantUnlockOID=_("Can't unlock OID");
fd_exception fd_PoolRangeError=_("the OID is out of the range of the pool");
fd_exception fd_NoLocking=_("No locking available");
fd_exception fd_ReadOnlyPool=_("pool is read-only");
fd_exception fd_InvalidPoolPtr=_("Invalid pool PTR");
fd_exception fd_NotAFilePool=_("not a file pool");
fd_exception fd_NoFilePools=_("file pools are not supported");
fd_exception fd_AnonymousOID=_("no pool covers this OID");
fd_exception fd_NotAPool=_("pool");
fd_exception fd_BadFilePoolLabel=_("file pool label is not a string");
fd_exception fd_ExhaustedPool=_("pool has no more OIDs");
fd_exception fd_InvalidPoolRange=_("pool overlaps 0x100000 boundary");
fd_exception fd_PoolCommitError=_("can't save changes to pool");
fd_exception fd_UnregisteredPool=_("internal error with unregistered pool");
fd_exception fd_UnresolvedPool=_("Cannot resolve pool specification");
fd_exception fd_UnhandledOperation=_("This pool can't handle this operation");
fd_exception fd_DataFileOverflow=_("Data file is over implementation limit");

u8_condition fd_PoolCommit=_("Pool/Commit");

int fd_pool_cache_init = FD_POOL_CACHE_INIT;
int fd_pool_lock_init  = FD_POOL_LOCKS_INIT;

int fd_n_pools=0;
fd_pool fd_default_pool=NULL;
struct FD_POOL *fd_top_pools[1024];

static struct FD_HASHTABLE poolid_table;

static u8_condition ipeval_objfetch="OBJFETCH";

static fdtype lock_symbol, unlock_symbol;

fd_pool init_pool_serial_table[1024],
  *fd_pool_serial_table=init_pool_serial_table;
int fd_pool_serial_count=0;

static int _goodfunarg(fdtype fn)
{
  if ((fn==FD_VOID)||(fn==FD_FALSE)||(FD_APPLICABLEP(fn)))
    return 1;
  else return 0;
}

#define goodfunarg(fn) (FD_EXPECT_TRUE(_goodfunarg(fn)))

/* This is used in committing pools */

struct FD_POOL_WRITES {
  int len, unlocked;
  fdtype choice, *oids, *values;};

static int pool_block_commit(fd_pool p,fd_hashtable locks,fdtype oids,
                             fd_pool_commit_flags flags);
static int commit_one_oid(fd_pool p,fdtype oid,fd_pool_commit_flags flags);
static int just_unlock(fd_pool p,fdtype oids,fd_pool_commit_flags flags);
static void restore_locks(fd_hashtable locks,
                          struct FD_POOL_WRITES *writes);
struct FD_POOL_WRITES locks2writes(fd_pool p,fd_pool_commit_flags flags);
struct FD_POOL_WRITES choice2writes(fd_pool p,fdtype oids,
                                    fd_pool_commit_flags flags);
static void cleanup_values(int n,fdtype *vec);
static int savep(fdtype v,fd_pool_commit_flags flags);

/* Pool delays for IPEVAL */

#if FD_GLOBAL_IPEVAL
fdtype *fd_pool_delays=NULL;
#elif FD_USE__THREAD
__thread fdtype *fd_pool_delays=NULL;
#elif FD_USE_TLS
u8_tld_key fd_pool_delays_key;
#else
fdtype *fd_pool_delays=NULL;
#endif

#if ((FD_USE_TLS) && (!(FD_GLOBAL_IPEVAL)))
FD_EXPORT fdtype *fd_get_pool_delays()
{
  return (fdtype *) u8_tld_get(fd_pool_delays_key);
}
FD_EXPORT void fd_init_pool_delays()
{
  int i=0;
  fdtype *delays= (fdtype *) u8_tld_get(fd_pool_delays_key);
  if (delays) return;
  delays=u8_alloc_n(FD_N_POOL_DELAYS,fdtype);
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
  int i=0;
  if (fd_pool_delays) return;
  fd_pool_delays=u8_alloc_n(FD_N_POOL_DELAYS,fdtype);
  while (i<FD_N_POOL_DELAYS) fd_pool_delays[i++]=FD_EMPTY_CHOICE;
}
#endif

static struct FD_POOL_HANDLER gluepool_handler;
static void (*pool_conflict_handler)(fd_pool upstart,fd_pool holder)=NULL;

static void pool_conflict(fd_pool upstart,fd_pool holder);
static struct FD_GLUEPOOL *make_gluepool(FD_OID base);
static int add_to_gluepool(struct FD_GLUEPOOL *gp,fd_pool p);

FD_EXPORT fd_pool fd_open_network_pool(u8_string spec,int read_only);

int fd_ignore_anonymous_oids=0;

#if FD_THREADS_ENABLED
static u8_mutex pool_registry_lock;
#endif

FD_EXPORT fdtype fd_anonymous_oid(const u8_string cxt,fdtype oid)
{
  if (fd_ignore_anonymous_oids) return FD_EMPTY_CHOICE;
  else return fd_err(fd_AnonymousOID,cxt,NULL,oid);
}

/* Pool caching */

FD_EXPORT void fd_pool_setcache(fd_pool p,int level)
{
  if (p->handler->setcache) p->handler->setcache(p,level);
  p->fd_cache_level=level;
  p->fdb_flags=p->fdb_flags|FDB_CACHELEVEL_SET;
}

static void init_cache_level(fd_pool p)
{
  if (FD_EXPECT_FALSE(p->fd_cache_level<0)) {
    p->fd_cache_level=fd_default_cache_level;
    if (p->handler->setcache)
      p->handler->setcache(p,fd_default_cache_level);}
}

FD_EXPORT
void fd_reset_pool_tables(fd_pool p,
                          ssize_t cacheval,
                          ssize_t locksval)
{
  int read_only=p->fd_read_only;
  fd_hashtable cache=&(p->fd_cache), locks=&(p->fdp_locks);
  fd_reset_hashtable(cache,((cacheval==0)?(fd_pool_cache_init):(cacheval)),1);
  if (locks->fd_n_keys==0) {
    ssize_t level=(read_only)?(0):(locksval==0)?(fd_pool_lock_init):(locksval);
    fd_reset_hashtable(locks,level,1);}
}

/* Registering pools */

#define FD_TOP_POOL_SIZE 0x100000

FD_EXPORT int fd_register_pool(fd_pool p)
{
  unsigned int capacity=p->fdp_capacity, serial_no;
  int baseindex=fd_get_oid_base_index(p->fdp_base,1);
  if (p->fdp_serialno>=0) return 0;
  else if (baseindex<0) return baseindex;
  fd_lock_mutex(&pool_registry_lock);
  /* Set up the serial number */
  serial_no=p->fdp_serialno=fd_pool_serial_count++; fd_n_pools++;
  fd_pool_serial_table[serial_no]=p;
  if ((capacity>=FD_TOP_POOL_SIZE) && ((p->fdp_base)%FD_TOP_POOL_SIZE)) {
    fd_seterr(fd_InvalidPoolRange,"fd_register_pool",u8_strdup(p->fd_cid),FD_VOID);
    fd_unlock_mutex(&pool_registry_lock);
    return -1;}
  if (capacity>=FD_TOP_POOL_SIZE) {
    int i=0, lim=capacity/FD_TOP_POOL_SIZE;
    /* Now get a baseid and register the pool in top_pools */
    while (i<lim) {
      FD_OID base=FD_OID_PLUS(p->fdp_base,(FD_TOP_POOL_SIZE*i));
      int baseid=fd_get_oid_base_index(base,1);
      if (baseid<0) {
        fd_unlock_mutex(&pool_registry_lock);
        return -1;}
      else if (fd_top_pools[baseid]) {
        pool_conflict(p,fd_top_pools[baseid]);
        fd_unlock_mutex(&pool_registry_lock);
        return -1;}
      else fd_top_pools[baseid]=p;
      i++;}}
  else if (fd_top_pools[baseindex] == NULL) {
    struct FD_GLUEPOOL *gluepool=make_gluepool(fd_base_oids[baseindex]);
    fd_top_pools[baseindex]=(struct FD_POOL *)gluepool;
    if (add_to_gluepool(gluepool,p)<0) {
      fd_unlock_mutex(&pool_registry_lock);
      return -1;}}
  else if (fd_top_pools[baseindex]->fdp_capacity) {
    pool_conflict(p,fd_top_pools[baseindex]);
    fd_unlock_mutex(&pool_registry_lock);
    return -1;}
  else if (add_to_gluepool((struct FD_GLUEPOOL *)fd_top_pools[baseindex],p)<0) {
    fd_unlock_mutex(&pool_registry_lock);
    return -1;}
  fd_unlock_mutex(&pool_registry_lock);
  if (p->label) {
    u8_byte *dot=strchr(p->label,'.');
    fdtype pkey=FD_VOID, probe=FD_VOID;
    if (dot) {
      pkey=fd_substring(p->label,dot);
      probe=fd_hashtable_get(&poolid_table,pkey,FD_EMPTY_CHOICE);
      if (FD_EMPTY_CHOICEP(probe)) {
        fd_hashtable_store(&poolid_table,pkey,fd_pool2lisp(p));
        p->prefix=FD_STRDATA(pkey);}
      else fd_decref(pkey);}
    pkey=fd_substring(p->label,NULL);
    probe=fd_hashtable_get(&poolid_table,pkey,FD_EMPTY_CHOICE);
    if (FD_EMPTY_CHOICEP(probe)) {
      fd_hashtable_store(&poolid_table,pkey,fd_pool2lisp(p));
      if (p->prefix == NULL) p->prefix=FD_STRDATA(pkey);}}
  return 1;
}

static struct FD_GLUEPOOL *make_gluepool(FD_OID base)
{
  struct FD_GLUEPOOL *pool=u8_alloc(struct FD_GLUEPOOL);
  pool->fdp_base=base; pool->fdp_capacity=0; pool->fd_read_only=1;
  pool->fdp_serialno=fd_get_oid_base_index(base,1);
  pool->label="gluepool"; pool->fd_source=NULL;
  pool->n_subpools=0; pool->subpools=NULL;
  pool->handler=&gluepool_handler;
  FD_INIT_STATIC_CONS(&(pool->fd_cache),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(pool->fdp_locks),fd_hashtable_type);
  fd_make_hashtable(&(pool->fd_cache),64);
  fd_make_hashtable(&(pool->fdp_locks),64);
  return pool;
}

static int add_to_gluepool(struct FD_GLUEPOOL *gp,fd_pool p)
{
  if (gp->n_subpools == 0) {
    struct FD_POOL **pools=u8_alloc_n(1,fd_pool);
    pools[0]=p; gp->n_subpools=1; gp->subpools=pools;
    return 1;}
  else {
    int comparison=0;
    struct FD_POOL **bottom=gp->subpools;
    struct FD_POOL **top=gp->subpools+(gp->n_subpools)-1;
    struct FD_POOL **read, **new, **write, **ipoint;
    struct FD_POOL **middle=bottom+(top-bottom)/2;
    /* First find where it goes (or a conflict if there is one) */
    while (top>=bottom) {
      middle=bottom+(top-bottom)/2;
      comparison=FD_OID_COMPARE(p->fdp_base,(*middle)->fdp_base);
      if (comparison == 0) break;
      else if (bottom==top) break;
      else if (comparison<0) top=middle-1; else bottom=middle+1;}
    /* If there is a conflict, we report it and quit */
    if (comparison==0) {pool_conflict(p,*middle); return -1;}
    /* Now we make a new vector to copy into */
    write=new=u8_alloc_n((gp->n_subpools+1),struct FD_POOL *);
    /* Figure out where we should put the new pool */
    if (comparison>0)
      ipoint=write+(middle-gp->subpools)+1;
    else ipoint=write+(middle-gp->subpools);
    /* Now, copy the subpools, doing the insertions */
    read=gp->subpools; top=gp->subpools+gp->n_subpools;
    while (read<top)
      if (write == ipoint) {*write++=p; *write++=*read++;}
      else *write++=*read++;
    if (write == ipoint) *write=p;
    /* Finally, update the structures.  Note that we are explicitly
       leaking the old subpools because we're avoiding locking on lookup. */
    p->fdp_serialno=((gp->fdp_serialno)<<10)+(gp->n_subpools+1);
    gp->subpools=new; gp->n_subpools++;}
  p->fdp_serialno=fd_pool_serial_count++;
  fd_pool_serial_table[p->fdp_serialno]=p;
  return 1;
}

FD_EXPORT u8_string fd_pool_id(fd_pool p)
{
  if (p->label!=NULL) return p->label;
  else if (p->fd_xid!=NULL) return p->fd_xid;
  else if (p->fd_cid!=NULL) return p->fd_cid;
  else if (p->fd_source!=NULL) return p->fd_source;
  else return NULL;
}

FD_EXPORT u8_string fd_pool_label(fd_pool p)
{
  if (p->label!=NULL) return p->label;
  else return NULL;
}

/* Finding the subpool */

FD_EXPORT fd_pool fd_find_subpool(struct FD_GLUEPOOL *gp,fdtype oid)
{
  FD_OID addr=FD_OID_ADDR(oid);
  struct FD_POOL **subpools=gp->subpools; int n=gp->n_subpools;
  struct FD_POOL **bottom=subpools, **top=subpools+(n-1);
  while (top>=bottom) {
    struct FD_POOL **middle=bottom+(top-bottom)/2;
    int comparison=FD_OID_COMPARE(addr,(*middle)->fdp_base);
    unsigned int difference=FD_OID_DIFFERENCE(addr,(*middle)->fdp_base);
    if (comparison < 0) top=middle-1;
    else if ((difference < ((*middle)->fdp_capacity)))
      return *middle;
    else if (bottom==top) break;
    else bottom=middle+1;}
  return NULL;
}

/* Handling pool conflicts */

static void pool_conflict(fd_pool upstart,fd_pool holder)
{
  if (pool_conflict_handler) pool_conflict_handler(upstart,holder);
  else {
    u8_log(LOG_WARN,_("Pool conflict"),
           "%s (from %s) and existing pool %s (from %s)\n",
           upstart->label,upstart->fd_source,holder->label,holder->fd_source);
    u8_seterr(_("Pool confict"),NULL,NULL);}
}

/* Fetching OID values */

FD_EXPORT fdtype fd_pool_fetch(fd_pool p,fdtype oid)
{
  fdtype v;
  init_cache_level(p);
  v=p->handler->fetch(p,oid);
  if (FD_ABORTP(v)) return v;
  if (p->fdp_n_locks)
    if (fd_hashtable_op(&(p->fdp_locks),fd_table_replace_novoid,oid,v))
      return v;
  if (FD_SLOTMAPP(v)) {FD_SLOTMAP_SET_READONLY(v);}
  else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_SET_READONLY(v);}
  if (p->fd_cache_level>0) fd_hashtable_store(&(p->fd_cache),oid,v);
  return v;
}

FD_EXPORT fdtype fd_pool_alloc(fd_pool p,int n)
{
  if (p->fd_read_only)
    return fd_err(fd_ReadOnlyPool,"fd_pool_alloc",
                  u8_strdup(p->fd_source),FD_VOID);
  else {
    fdtype result=p->handler->alloc(p,n);
    if (FD_CHOICEP(result)) {
      if ((p->fdb_flags)&(FDB_LOCKFREE)) {
        fd_hashtable_iterkeys(&(p->fd_cache),fd_table_store,
                              FD_CHOICE_SIZE(result),FD_CHOICE_DATA(result),
                              FD_EMPTY_CHOICE);}
      else {
        fd_hashtable_iterkeys(&(p->fdp_locks),fd_table_store,
                              FD_CHOICE_SIZE(result),FD_CHOICE_DATA(result),
                              FD_EMPTY_CHOICE);
        p->fdp_n_locks=p->fdp_n_locks+n;}}
    else if (FD_ABORTP(result)) return result;
    else if (FD_EXCEPTIONP(result)) return result;
    else if ((p->fdb_flags)&(FDB_LOCKFREE)) {
      fd_hashtable_store(&(p->fd_cache),result,FD_EMPTY_CHOICE);}
    else {
      fd_hashtable_store(&(p->fdp_locks),result,FD_EMPTY_CHOICE);
      p->fdp_n_locks=p->fdp_n_locks+n;}
    return result;}
}

FD_EXPORT int fd_pool_prefetch(fd_pool p,fdtype oids)
{
  FDTC *fdtc=((FD_USE_THREADCACHE)?(fd_threadcache):(NULL));
  struct FD_HASHTABLE *oidcache=((fdtc!=NULL)?(&(fdtc->oids)):(NULL));
  int decref_oids=0, cachelevel;
  if (p==NULL) {
    fd_seterr(fd_NotAPool,"fd_pool_prefetch",
              u8_strdup("NULL pool ptr"),FD_VOID);
    return -1;}
  else init_cache_level(p);
  cachelevel=p->fd_cache_level;
  /* if (p->fd_cache_level<1) return 0; */
  if ( (p->handler->fetchn==NULL) || (!(p->fdb_flags&(FDB_BATCHABLE))) ) {
    if (fd_ipeval_delay(FD_CHOICE_SIZE(oids))) {
      FD_ADD_TO_CHOICE(fd_pool_delays[p->fdp_serialno],oids);
      return 0;}
    else {
      int n_fetches=0;
      FD_DO_CHOICES(oid,oids) {
        fdtype v=fd_pool_fetch(p,oid);
        if (FD_ABORTP(v)) {
          FD_STOP_DO_CHOICES; return v;}
        n_fetches++; fd_decref(v);}
      return n_fetches;}}
  if (FD_ACHOICEP(oids)) {
    oids=fd_make_simple_choice(oids); decref_oids=1;}
  if (fd_ipeval_status()) {
    FD_HASHTABLE *cache=&(p->fd_cache); int n_to_fetch=0;
    /* fdtype oidschoice=fd_make_simple_choice(oids); */
    fdtype *delays=&(fd_pool_delays[p->fdp_serialno]);
    FD_DO_CHOICES(oid,oids)
      if (fd_hashtable_probe_novoid(cache,oid)) {}
      else {
        FD_ADD_TO_CHOICE(*delays,oid);
        n_to_fetch++;}
    fd_ipeval_delay(n_to_fetch);
    /* fd_decref(oidschoice); */
    return 0;}
  else if (FD_CHOICEP(oids)) {
    int n=FD_CHOICE_SIZE(oids), some_locked=0;
    struct FD_HASHTABLE *cache=&(p->fd_cache), *locks=&(p->fdp_locks);
    fdtype *values, *oidv=u8_alloc_n(n,fdtype), *write=oidv;
    int nolocks=((p->fdb_flags)&(FDB_LOCKFREE))||(locks->fd_n_keys==0);
    FD_DO_CHOICES(o,oids)
      if (((oidcache==NULL)||(fd_hashtable_probe_novoid(oidcache,o)==0))&&
          (fd_hashtable_probe_novoid(cache,o)==0) &&
          ((nolocks)||(fd_hashtable_probe_novoid(locks,o)==0)))
        *write++=o;
      else if ((!nolocks)&&
               (fd_hashtable_op(locks,fd_table_test,o,FD_LOCKHOLDER))) {
        *write++=o; some_locked=1;}
      else {}
    if (write==oidv) {
      /* Nothing to prefetch, free and return */
      u8_free(oidv);
      if (decref_oids) fd_decref(oids);
      return 0;}
    else n=write-oidv;
    /* Call the pool handler */
    values=p->handler->fetchn(p,n,oidv);
    /* If you got results, store them in the cache */
    if (values)
      if (some_locked) {
        /* If some values are locked, we consider each value and
           store it in the appropriate tables (locks or cache). */
        int j=0; while (j<n) {
          fdtype v=values[j], oid=oidv[j];
          if (fd_hashtable_op(&(p->fdp_locks),fd_table_replace_novoid,oid,v)==0) {
            /* This is when the OID we're storing isn't locked */
            if (FD_SLOTMAPP(v)) {FD_SLOTMAP_SET_READONLY(v);}
            else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_SET_READONLY(v);}
            if (fdtc) fd_hashtable_op(&(fdtc->oids),fd_table_store,oid,v);
            if (cachelevel>0)
              fd_hashtable_op(&(p->fd_cache),fd_table_store,oid,v);}
          /* We decref it since it would have been incref'd when
             processed above. */
          fd_decref(values[j]);
          j++;}}
      else {
        int j=0; while (j<n) {
          fdtype v=values[j++];
          if (FD_SLOTMAPP(v)) {FD_SLOTMAP_SET_READONLY(v);}
          else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_SET_READONLY(v);}}
        if (fdtc)
          fd_hashtable_iter(oidcache,fd_table_store,n,oidv,values);
        if (cachelevel>0)
          fd_hashtable_iter(&(p->fd_cache),fd_table_store_noref,n,oidv,values);}
    else {
      u8_free(oidv);
      if (decref_oids) fd_decref(oids);
      return -1;}
    /* We don't have to do this now that we have fd_table_store_noref */
    /* i=0; while (i < n) {fd_decref(values[i]); i++;} */
    u8_free(oidv); u8_free(values);
    if (decref_oids) fd_decref(oids);
    return n;}
  else {
    fdtype v=p->handler->fetch(p,oids);
    if ((p->fdp_n_locks==0) || ((p->fdb_flags)&(FDB_LOCKFREE)) ||
        (fd_hashtable_op(&(p->fdp_locks),fd_table_replace_novoid,oids,v)==0)) {
      if (fdtc) fd_hashtable_op(&(fdtc->oids),fd_table_store,oids,v);
      if (cachelevel>0) fd_hashtable_store(&(p->fd_cache),oids,v);}
    if (decref_oids) fd_decref(oids);
    fd_decref(v);
    return 1;}
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
  FDTC *fdtc=((FD_USE_THREADCACHE)?(fd_threadcache):(NULL));
  fd_pool _pools[32], *pools=_pools;
  fdtype _toget[32], *toget=_toget;
  int i=0, n_pools=0, max_pools=32, total=0;
  FD_DO_CHOICES(oid,oids) {
    if ((fdtc)&&(fd_hashtable_probe(&(fdtc->oids),oid))) {}
    else if (FD_OIDP(oid)) {
      fd_pool p=fd_oid2pool(oid);
      if (p) {
        fdtype v=((p->fdb_flags)&(FDB_LOCKFREE))?
          (fd_hashtable_get_noref(&(p->fdp_locks),oid,FD_VOID)):
          (FD_VOID);
        if (FD_VOIDP(v)) v=fd_hashtable_get_noref(&(p->fd_cache),oid,FD_VOID);
        if ((v==FD_VOID)||(v==FD_LOCKHOLDER)) {
          /* Scan current pools to see if you've already seen it. */
          i=0; while (i<n_pools) if (pools[i]==p) break; else i++;
          if (i>=n_pools) { /* This means you need to add an entry */
            if (i<max_pools) {} /* Enough space */
            else if (max_pools==32) {
              /* Running out of space for the first time. */
              int j=0;
              pools=u8_alloc_n(64,fd_pool);
              toget=u8_alloc_n(64,fdtype);
              while (j<n_pools) {
                pools[j]=_pools[j]; toget[j]=_toget[j]; j++;}
              max_pools=64;}
            else {
              /* Running out of space again. */
              pools=u8_realloc_n(pools,max_pools+32,fd_pool);
              toget=u8_realloc_n(toget,max_pools+32,fdtype);
              max_pools=max_pools+32;}
            pools[i]=p; toget[i]=FD_EMPTY_CHOICE; n_pools++;}
          /* Now, i is bound to the index for the pools and to gets */
          FD_ADD_TO_CHOICE(toget[i],oid);}
        else if (fdtc)
          fd_hashtable_store(&(fdtc->oids),oid,v);
        else {}}}
    else {}}
  i=0; while (i < n_pools) {
    int retval=fd_pool_prefetch(pools[i],toget[i]);
    if (retval<0) {
      while (i<n_pools) {fd_decref(toget[i]); i++;}
      return -1;}
    total=total+FD_CHOICE_SIZE(toget[i]);
    fd_decref(toget[i]); i++;}
  return total;
}

FD_EXPORT int fd_lock_oid(fdtype oid)
{
  fd_pool p=fd_oid2pool(oid);
  return fd_pool_lock(p,oid);
}

FD_EXPORT int fd_lock_oids(fdtype oids)
{
  fd_pool _pools[32], *pools=_pools;
  fdtype _tolock[32], *tolock=_tolock;
  int i=0, n_pools=0, max_pools=32, total=0;
  FD_DO_CHOICES(oid,oids) {
    if (FD_OIDP(oid)) {
      fd_pool p=fd_oid2pool(oid);
      if (!(p)) {}
      else if ((p->fdb_flags)&(FDB_LOCKFREE)) {}
      else if (fd_hashtable_probe_novoid(&(p->fdp_locks),oid)==0) {
        i=0; while (i<n_pools) if (pools[i]==p) break; else i++;
        if (i>=n_pools) {
          /* Create a pool entry if neccessary */
          if (i<max_pools) {
            pools[i]=p; tolock[i]=FD_EMPTY_CHOICE; n_pools++;}
          /* Grow the tables if neccessary */
          else if (max_pools==32) {
            int j=0;
            pools=u8_alloc_n(64,fd_pool);
            tolock=u8_alloc_n(64,fdtype);
            while (j<n_pools) {
              pools[j]=_pools[j]; tolock[j]=_tolock[j]; j++;}
            max_pools=64;}
          else {
            pools=u8_realloc_n(pools,max_pools+32,fd_pool);
            tolock=u8_realloc_n(tolock,max_pools+32,fdtype);
            max_pools=max_pools+32;}}
        /* Now, i is bound to the index for the pools and to gets */
        FD_ADD_TO_CHOICE(tolock[i],oid);}}}
  i=0; while (i < n_pools) {
    fd_pool_lock(pools[i],tolock[i]);
    total=total+FD_CHOICE_SIZE(tolock[i]);
    fd_decref(tolock[i]); i++;}
  return total;
}

FD_EXPORT int fd_unlock_oid(fdtype oid,int commit)
{
  fd_pool p=fd_oid2pool(oid);
  return fd_pool_unlock(p,oid,commit);
}

FD_EXPORT int fd_unlock_oids(fdtype oids,int commit)
{
  fd_pool _pools[32], *pools=_pools;
  fdtype _tounlock[32], *tounlock=_tounlock;
  int i=0, n_pools=0, max_pools=32, total=0;
  FD_DO_CHOICES(oid,oids) {
    if (FD_OIDP(oid)) {
      fd_pool p=fd_oid2pool(oid);
      if (!(p)) {}
      else if ((p->fdb_flags)&(FDB_LOCKFREE)) {}
      else if (fd_hashtable_probe_novoid(&(p->fdp_locks),oid)) {
        i=0; while (i<n_pools) if (pools[i]==p) break; else i++;
        if (i>=n_pools) {
          /* Create a pool entry if neccessary */
          if (i<max_pools) {
            pools[i]=p; tounlock[i]=FD_EMPTY_CHOICE; n_pools++;}
          /* Grow the tables if neccessary */
          else if (max_pools==32) {
            int j=0;
            pools=u8_alloc_n(64,fd_pool);
            tounlock=u8_alloc_n(64,fdtype);
            while (j<n_pools) {
              pools[j]=_pools[j]; tounlock[j]=_tounlock[j]; j++;}
            max_pools=64;}
          else {
            pools=u8_realloc_n(pools,max_pools+32,fd_pool);
            tounlock=u8_realloc_n(tounlock,max_pools+32,fdtype);
            max_pools=max_pools+32;}}
        /* Now, i is bound to the index for the pools and to gets */
        FD_ADD_TO_CHOICE(tounlock[i],oid);}
      else {}}}
  i=0; while (i < n_pools) {
    fd_pool_unlock(pools[i],tounlock[i],commit);
    total=total+FD_CHOICE_SIZE(tounlock[i]);
    fd_decref(tounlock[i]); i++;}
  if (pools!=_pools) u8_free(pools);
  if (tounlock!=_tounlock) u8_free(tounlock);
  return total;
}

FD_EXPORT int fd_set_oid_value(fdtype oid,fdtype value)
{
  fd_pool p=fd_oid2pool(oid);
  if (p==NULL)
    return fd_reterr(fd_AnonymousOID,"SET-OID_VALUE!",NULL,oid);
  else {
    if ((FD_SLOTMAPP(value))||(FD_SCHEMAPP(value))||(FD_HASHTABLEP(value)))
      fd_set_modified(value,1);
    if ((p->fdb_flags)&(FDB_LOCKFREE)) {
      fd_hashtable_store(&(p->fd_cache),oid,value);
      return 1;}
    else if (fd_lock_oid(oid)) {
      fd_hashtable_store(&(p->fdp_locks),oid,value);
      return 1;}
    else return fd_reterr(fd_CantLockOID,"SET-OID_VALUE!",NULL,oid);}
}

FD_EXPORT int fd_swapout_oid(fdtype oid)
{
  fd_pool p=fd_oid2pool(oid);
  if (p==NULL)
    return fd_reterr(fd_AnonymousOID,"SET-OID_VALUE!",NULL,oid);
  else if (p->handler->swapout) return p->handler->swapout(p,oid);
  else if ((!((p->fdb_flags)&(FDB_LOCKFREE)))&&
           (fd_hashtable_probe_novoid(&(p->fdp_locks),oid)))
    /* Don't swap out locked OIDs */
   return 0;
  else if (!(fd_hashtable_probe_novoid(&(p->fd_cache),oid))) return 0;
  else {
    fd_hashtable_store(&(p->fd_cache),oid,FD_VOID);
    return 1;}
}

FD_EXPORT int fd_swapout_oids(fdtype oids)
{
  int n_pools=0, max_pools=32; fd_pool one=NULL;
  fd_pool *pools=u8_malloc(max_pools*sizeof(fd_pool));
  fdtype *toswap=u8_malloc(max_pools*sizeof(fdtype));
  FD_DO_CHOICES(oid,oids) {
    if (one==NULL) one=fd_oid2pool(oid);
    else if (one!=fd_oid2pool(oid)) {
      one=NULL; FD_STOP_DO_CHOICES; break;}
    else {}}
  if (one) {
    n_pools=1; pools[0]=one; toswap[0]=oids;}
  else {
    FD_DO_CHOICES(oid,oids) {
      fd_pool p=fd_oid2pool(oid);
      int i=0; while (i<n_pools)
                 if (pools[i]==p) break; else i++;
      if (i<n_pools) {FD_ADD_TO_CHOICE(toswap[i],oid);}
      else if (n_pools<max_pools) {
        pools[i]=p; toswap[i]=oid; n_pools++;}
      else {
        pools=u8_realloc(pools,((max_pools*2)*sizeof(fd_pool)));
        toswap=u8_realloc(toswap,((max_pools*2)*sizeof(fdtype)));
        max_pools=max_pools*2;
        pools[i]=p; toswap[i]=oid; n_pools++;}}}
  {
      int i=0; while (i<n_pools) {
      fd_pool p=pools[i];
      if (p->handler->swapout)
        p->handler->swapout(p,toswap[i]);
      else {
        fdtype oids=fd_make_simple_choice(toswap[i]);
        if (FD_EMPTY_CHOICEP(oids)) {}
        else if (FD_OIDP(oids)) {
          fd_hashtable_op(&(p->fd_cache),fd_table_replace,oids,FD_VOID);}
        else {
          fd_hashtable_iterkeys
            (&(p->fd_cache),fd_table_replace,
             FD_CHOICE_SIZE(oids),FD_CHOICE_DATA(oids),FD_VOID);
          /* These are values we can unlock */
          fd_pool_commit(p,oids,COMMIT_FINISHED);
          fd_devoid_hashtable(&(p->fdp_locks));}
        fd_decref(oids);}
      i++;}}
  return n_pools;
}

static fd_pool guesspool(fdtype oids)
{
  if (FD_OIDP(oids)) return fd_oid2pool(oids);
  else if ((FD_CHOICEP(oids))||(FD_ACHOICEP(oids))) {
    fd_pool p=NULL;
    FD_DO_CHOICES(oid,oids) {
      if (FD_OIDP(oid)) p=fd_oid2pool(oid);
      if (p) {
        FD_STOP_DO_CHOICES;
        return p;}}
    return p;}
  else return NULL;
}

FD_EXPORT int fd_finish_oids(fdtype oids,fd_pool poolarg)
{
  if (FD_EMPTY_CHOICEP(oids)) return 0;
  else if ((FD_OIDP(oids))||
           ((FD_CONSP(oids))&&((FD_CHOICEP(oids))||(FD_ACHOICEP(oids))))) {
    fd_pool p=(poolarg)?(poolarg):(guesspool(oids));
    if (!(p))
      return fd_reterr(fd_UnknownPool,"fd_finish_oids",NULL,FD_VOID);
    else {
      fd_hashtable locks=&(p->fdp_locks);
      int n_oids=FD_CHOICE_SIZE(oids), finished=0;
      FD_DO_CHOICES(oid,oids) {
        if (!(FD_OIDP(oid))) {
          u8_log(LOGWARN,"fd_finish_oids",
                 "The argument %q is not an OID",oid);}
        else {
          fdtype v=fd_hashtable_get(locks,oid,FD_VOID);
          if ((FD_VOIDP(v))||(v==FD_LOCKHOLDER)) {}
          else if (FD_SLOTMAPP(v)) {FD_SLOTMAP_SET_READONLY(v); finished++;}
          else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_SET_READONLY(v); finished++;}
          else if (FD_HASHTABLEP(v)) {FD_HASHTABLE_SET_READONLY(v); finished++;}
          else {}
          fd_decref(v);}}
      return finished;}}
    else return 0;
}

FD_EXPORT int fd_pool_lock(fd_pool p,fdtype oids)
{
  struct FD_HASHTABLE *locks=&(p->fdp_locks); int decref_oids=0;
  if (p->handler->lock==NULL) {
    if ((p->fdb_flags)&(FDB_LOCKFREE)) {
      return 0;}
    else {
      fd_seterr(fd_NoLocking,"fd_pool_lock",u8_strdup(fd_pool_id(p)),FD_VOID);
      return -1;}}
  if (FD_ACHOICEP(oids)) {
    oids=fd_make_simple_choice(oids); decref_oids=1;}
  if (FD_CHOICEP(oids)) {
    fdtype needy; int retval, n;
    struct FD_CHOICE *oidc=fd_alloc_choice(FD_CHOICE_SIZE(oids));
    fdtype *oidv=(fdtype *)FD_XCHOICE_DATA(oidc), *write=oidv;
    FD_DO_CHOICES(o,oids)
      if (fd_hashtable_probe(locks,o)==0) *write++=o;
    if (decref_oids) fd_decref(oids);
    if (write==oidv) {
      /* Nothing to lock, free and return */
      u8_free(oidc);
      return 1;}
    else n=write-oidv;
    if (p->handler->lock==NULL) {
      fd_seterr(fd_CantLockOID,"fd_pool_lock",
                u8_strdup(p->fd_cid),oids);
      return -1;}
    needy=fd_init_choice(oidc,n,NULL,FD_CHOICE_ISATOMIC);
    retval=p->handler->lock(p,needy);
    if (retval<0) {
      fd_decref(needy);
      return retval;}
    else if (retval) {
      fd_hashtable_iterkeys(&(p->fd_cache),fd_table_replace,n,oidv,FD_VOID);
      fd_hashtable_iterkeys(locks,fd_table_store,n,oidv,FD_LOCKHOLDER);}
    fd_decref(needy);
    return retval;}
  else if (fd_hashtable_probe(locks,oids)==0) {
    int retval=p->handler->lock(p,oids);
    if (decref_oids) fd_decref(oids);
    if (retval<0) return retval;
    else if (retval) {
      fd_hashtable_op(&(p->fd_cache),fd_table_replace,oids,FD_VOID);
      fd_hashtable_op(locks,fd_table_store,oids,FD_LOCKHOLDER);
      return 1;}
    else return 0;}
  else {
    if (decref_oids) fd_decref(oids);
    return 1;}
}

FD_EXPORT int fd_pool_unlock(fd_pool p,fdtype oids,int commit)
{
  struct FD_HASHTABLE *locks=&(p->fdp_locks);
  u8_log(fddb_loglevel,"Unlock","Unlocking %d oids in pool %s",
         FD_CHOICE_SIZE(oids),p->fd_cid);
  if (p->handler->unlock==NULL) {
    if ((p->fdb_flags)&(FDB_LOCKFREE)) {
      return 0;}
    else {
      fd_seterr(fd_NoLocking,"fd_pool_lock",u8_strdup(fd_pool_id(p)),FD_VOID);
      return -1;}}
  if (commit) {
    if (fd_pool_commit(p,oids,FD_POOL_COMMIT_UNLOCK)<0)
      return -1;
    else {}}
  if (FD_CHOICEP(oids)) {
    fdtype needy; int retval, n;
    struct FD_CHOICE *oidc=fd_alloc_choice(FD_CHOICE_SIZE(oids));
    fdtype *oidv=(fdtype *)FD_XCHOICE_DATA(oidc), *write=oidv;
    u8_log(fddb_loglevel+1,"Unlock","Cleaning up locks table for %s",p->fd_cid);
    FD_DO_CHOICES(o,oids)
      if (fd_hashtable_probe_novoid(locks,o)) *write++=o;
    if (write==oidv) {
      u8_free(oidc);
      return 1;}
    else n=write-oidv;
    if (p->handler->unlock==NULL) {
      fd_seterr(fd_CantLockOID,"fd_pool_unlock",
                u8_strdup(p->fd_cid),oids);
      return -1;}
    u8_log(fddb_loglevel+1,"Unlock","Running custom unlock for %s",p->fd_cid);
    needy=fd_init_choice(oidc,n,NULL,FD_CHOICE_ISATOMIC);
    retval=p->handler->unlock(p,needy);
    if (retval<0) {
      fd_decref(needy);
      return -1;}
    else if (retval) {
      u8_log(fddb_loglevel,"Unlock",
             "Voiding lock table entries for %s",p->fd_cid);
      fd_hashtable_iterkeys(locks,fd_table_replace,n,oidv,FD_VOID);
      u8_log(fddb_loglevel+1,"Unlock","Devoiding lock table for %s",p->fd_cid);
      if (fd_devoid_hashtable(locks)<0) {
        fd_decref(needy);
        return -1;}
      fd_decref(needy);
      return retval;}
    else return 0;}
  else if (fd_hashtable_probe(locks,oids)) {
    int retval=p->handler->unlock(p,oids);
    if (retval<0) return -1;
    else if (retval) {
      fd_hashtable_op(locks,fd_table_replace,oids,FD_VOID);
      if (fd_devoid_hashtable(locks)<0) return -1;
      return 1;}
    else return 0;}
  else return 1;
}

FD_EXPORT int fd_pool_unlock_all(fd_pool p,int commit)
{
  int result;
  struct FD_HASHTABLE *locks=&(p->fdp_locks);
  fdtype oids=fd_hashtable_keys(locks);
  if (FD_EMPTY_CHOICEP(oids)) return 0;
  else result=fd_pool_unlock(p,oids,commit);
  fd_decref(oids);
  return result;
}

FD_EXPORT int fd_pool_commit(fd_pool p,fdtype oids,
                             fd_pool_commit_flags flags)
{
  struct FD_HASHTABLE *locks=&(p->fdp_locks);
  double start=u8_elapsed_time();

  fd_write_lock_struct(locks);

  if (locks->fd_n_keys==0) {
    u8_log(fddb_loglevel+1,fd_PoolCommit,"####### No locked oids in %s",p->fd_cid);
    return 0;}
  else if (p->handler->storen==NULL) {
    u8_log(fddb_loglevel+1,fd_PoolCommit,"####### Unlocking OIDs in %s",p->fd_cid);
    int rv=just_unlock(p,oids,flags);
    return rv;}
  else if (FD_OIDP(oids)) {
    int rv=commit_one_oid(p,oids,flags);
    if (rv>0)
      u8_log(fddb_loglevel+1,fd_PoolCommit,
             "####### Committed one OIDs in %s",p->fd_cid);
    else if (rv==0)
      u8_log(fddb_loglevel+1,fd_PoolCommit,
             "####### No modified OIDs in %s",p->fd_cid);
    else {}
    return rv;}
  else if (FD_EMPTY_CHOICEP(oids)) {
    return 0;}
  else {
    fd_write_lock_struct(locks);
    return pool_block_commit(p,locks,oids,flags);}
}

FD_EXPORT int fd_pool_commit_all(fd_pool p,int unlock)
{
  fd_pool_commit_flags flags=
    ((unlock)?(FD_POOL_COMMIT_UNLOCK):(0));
  return fd_pool_commit(p,FD_VOID,flags);
}

/* Support for commitment */

#define FD_COMMIT_OID 1
#define FD_UNLOCK_OID 2
#define FD_COMMIT_AND_UNLOCK 3

static int savep(fdtype v,fd_pool_commit_flags flags)
{
  int saveall=(!(U8_BITP(flags,FD_POOL_COMMIT_FINISHED)));
  int unlock=(!(U8_BITP(flags,FD_POOL_COMMIT_UNLOCK)));
  if ((FD_VOIDP(v))||(v==FD_LOCKHOLDER)) {
    if (unlock)
      return FD_UNLOCK_OID;
    else return 0;}
  else if (FD_SLOTMAPP(v)) {
    if (FD_SLOTMAP_MODIFIEDP(v)) {
      if ((saveall)||(FD_SLOTMAP_READONLYP(v))) {
        if (unlock)
          return FD_COMMIT_AND_UNLOCK;
        else return FD_COMMIT_OID;}
      else return 0;}
    else if (unlock)
      return FD_UNLOCK_OID;
    else return 0;}
  else if (FD_SCHEMAPP(v)) {
    if (FD_SCHEMAP_MODIFIEDP(v)) {
      if ((saveall)||(FD_SCHEMAP_READONLYP(v))) {
        if (unlock)
          return FD_COMMIT_AND_UNLOCK;
        else return FD_COMMIT_OID;}
      else return 0;}
    else if (unlock)
      return FD_UNLOCK_OID;
    else return 0;}
  else if (FD_HASHTABLEP(v)) {
    if (FD_HASHTABLE_MODIFIEDP(v)) {
      if ((saveall)||(FD_HASHTABLE_READONLYP(v))) {
        if (unlock)
          return FD_COMMIT_AND_UNLOCK;
        else return FD_COMMIT_OID;}
      else return 0;}
    else if (unlock)
      return FD_UNLOCK_OID;
    else return 0;}
  else if (unlock)
    return FD_COMMIT_AND_UNLOCK;
  else return saveall;
}

static int pool_block_commit(fd_pool p,fd_hashtable locks,fdtype oids,
                             fd_pool_commit_flags flags)
{
  double start_time=u8_elapsed_time(), last_report=start_time;
  int explicit_oids=
    (FD_CONSP(oids))&&((FD_CHOICEP(oids))||(FD_ACHOICEP(oids)));

  if (explicit_oids)
    u8_log(fddb_loglevel,fd_PoolCommit,
           "####### Committing%s %d %sOIDs from %s",
           ((U8_BITP(flags,FD_POOL_COMMIT_FINISHED))?(" up to"):("")),
           FD_CHOICE_SIZE(oids),
           ((U8_BITP(flags,FD_POOL_COMMIT_FINISHED))?("updated "):("")),
           p->fd_cid);
  else u8_log(fddb_loglevel,fd_PoolCommit,
              "####### Committing %sOIDs from %s",
              ((U8_BITP(flags,FD_POOL_COMMIT_FINISHED))?("updated "):("")),
              p->fd_cid);

  struct FD_POOL_WRITES writes=(explicit_oids)?
    (choice2writes(p,oids,flags)):
    (locks2writes(p,flags));
  int unlock=U8_BITP(flags,FD_POOL_COMMIT_UNLOCK), retval=-1;

  init_cache_level(p);

  if (explicit_oids)
    u8_log(fddb_loglevel+1,"PoolCommit/Sorted",
           "####### Saving%s %d/%d OIDs in %s",
           ((unlock)?("/unlocking"):("")),
           writes.len,FD_CHOICE_SIZE(oids),p->fd_cid);
  else u8_log(fddb_loglevel+1,"PoolCommit/Sorted",
              "####### Saving%s %d OIDs in %s",
              ((unlock)?("/unlocking"):("")),writes.len,p->fd_cid);

  if (writes.len)
    retval=p->handler->storen(p,writes.len,writes.oids,writes.values);
  else retval=0;

  if (retval<0) {
    u8_log(LOGCRIT,fd_PoolCommitError,
           "Error %d (%s) saving oids to %s",
           errno,u8_strerror(errno),p->fd_cid);
    u8_seterr(fd_PoolCommitError,"fd_pool_commit",u8_strdup(p->fd_cid));
    if (unlock) restore_locks(locks,&writes);
    return retval;}
  else {
    if (writes.len)
      u8_log(fddb_loglevel,fd_PoolCommit,
             "####### Saved %d OIDs to %s in %f secs",
             writes.len,p->fd_cid,u8_elapsed_time()-start_time);
    last_report=u8_elapsed_time();
    if (unlock) {
      int unlocked=p->handler->unlock(p,writes.choice);
      if (unlocked<0) {
        u8_log(LOG_CRIT,fd_PoolCommit,
               "Error unlocking %d OIDs from %s after %f secs",
               writes.len,p->fd_cid,
               u8_elapsed_time()-last_report);
        restore_locks(locks,&writes);
        return unlocked;}
      if ((u8_elapsed_time()-last_report)>1) {
        u8_log(fddb_loglevel+1,fd_PoolCommit,
               "Unlocked %d OIDs from %s in %f secs",
               writes.len,p->fd_cid,u8_elapsed_time()-start_time);}}
    cleanup_values(writes.len,writes.values);
    u8_free(writes.values);
    fd_decref(writes.choice);
    u8_log(fddb_loglevel,fd_PoolCommit,
           "####### Committed %d OIDs to %s (leaving %d locked) in %f secs",
           writes.len,p->fd_cid,locks->fd_n_keys,
           u8_elapsed_time()-start_time);
    return retval;}
}

struct FD_POOL_WRITES choice2writes(fd_pool p,fdtype oids,
                                    fd_pool_commit_flags flags)
{
  int unlock=U8_BITP(flags,FD_POOL_COMMIT_UNLOCK);
  int max_writes=FD_CHOICE_SIZE(oids), n_writes;
  struct FD_POOL_WRITES writes; u8_zero_struct(writes);
  struct FD_CHOICE *xchoice=fd_alloc_choice(max_writes);
  fd_hashtable locks=&(p->fdp_locks);
  fdtype choice, *oidv, *values;
  /* We're going to go through the locks, so we're going to release 
     this one. */
  fd_rw_unlock_struct(locks);
  FD_INIT_FRESH_CONS(xchoice,fd_choice_type);
  writes.len=max_writes;
  writes.choice=choice=(fdtype)xchoice;
  writes.oids=oidv=(fdtype *)(FD_XCHOICE_DATA(xchoice));
  writes.values=values=u8_zalloc_n(max_writes,fdtype);
  {FD_DO_CHOICES(o,oids) {
      fd_pool pool=fd_oid2pool(o);
      if (p==pool) {
        fdtype v=fd_hashtable_get(locks,o,FD_VOID);
        int save=savep(v,flags);
        if (U8_BITP(save,FD_COMMIT_OID)) {
          *oidv++=o;
          *values++=v;}
        else fd_decref(v);
        if (U8_BITP(save,FD_UNLOCK_OID)) {
          fd_hashtable_store(locks,o,FD_VOID);}}}}
  n_writes=oidv-writes.oids;
  writes.len=n_writes;
  xchoice->fd_choicesize=n_writes;
  if ((unlock)&&(writes.len))
    fd_devoid_hashtable_x(locks,1);
  return writes;
}

struct FD_POOL_WRITES locks2writes(fd_pool p,fd_pool_commit_flags flags)
{
  struct FD_POOL_WRITES writes; u8_zero_struct(writes);
  int unlock = U8_BITP(flags,FD_POOL_COMMIT_UNLOCK);
  fd_hashtable locks=&(p->fdp_locks);
  fdtype choice, *oidv, *values;
  size_t max_writes=locks->fd_n_keys;
  struct FD_CHOICE *xchoice=fd_alloc_choice(max_writes);
  FD_INIT_FRESH_CONS(xchoice,fd_choice_type);
  writes.len=max_writes;
  writes.choice=choice=(fdtype)xchoice;
  writes.oids=oidv=(fdtype *)FD_XCHOICE_DATA(xchoice);
  writes.values=values=u8_zalloc_n(max_writes,fdtype);
  if (locks->fd_n_buckets) {
    struct FD_HASH_BUCKET **scan=locks->fd_buckets;
    struct FD_HASH_BUCKET **lim=scan+locks->fd_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int n_keyvals=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+n_keyvals;
        while (kvscan<kvlimit) {
          fdtype o=kvscan->fd_kvkey, v=kvscan->fd_keyval;
          if (U8_EXPECT_TRUE(FD_OIDP(o))) {
            if (savep(v,flags)) {
              *oidv++=o;
              *values++=v;
              if (unlock) {
                kvscan->fd_keyval=FD_VOID;}
              else fd_incref(v);}
            kvscan++;}
          else kvscan++;
          scan++;}}
      else scan++;
    writes.len=oidv-writes.oids;}
  else writes.len=0;
  if ((unlock)&&(writes.len))
    fd_devoid_hashtable_x(locks,1);
  fd_rw_unlock_struct(locks);
  return writes;
}

static int just_unlock(fd_pool p,fdtype oids,fd_pool_commit_flags flags)
{
  int unlock = (U8_BITP(flags,FD_POOL_COMMIT_UNLOCK));
  /* If we don't have a store function ,we just unlock them
     and discard changes. */
  if (FD_EMPTY_CHOICEP(oids))
    return 0;
  else if ((U8_BITP(flags,FD_POOL_COMMIT_UNLOCK)) &&
           (p->handler->unlock)) {
    fd_hashtable locks=&(p->fdp_locks);
    fdtype choice=((FD_CHOICEP(oids))||(FD_ACHOICEP(oids)))?
      (fd_make_simple_choice(oids)):
      (fd_hashtable_keys(locks));
    int choice_size=FD_CHOICE_SIZE(choice);
    int unlocked=p->handler->unlock(p,choice);
    if (unlocked<0) return unlocked;
    /* Now drop them from the locks table */
    if (choice_size>1) {
      const fdtype *oids=FD_CHOICE_DATA(oids);
      fd_hashtable_iterkeys(&(p->fdp_locks),fd_table_replace,
                            choice_size,oids,FD_VOID);
      fd_devoid_hashtable(&(p->fdp_locks));}
    else if (choice_size==1) {
      fd_hashtable_store(&(p->fdp_locks),choice,FD_VOID);}
    else {}
    return choice_size;}
  else if ((unlock)&&(!((p->fdb_flags)&(FDB_LOCKFREE)))) {
    /* If we can't unlock them and it's not declared lock free (like mempools),
       return an error. */
    fd_seterr(fd_NoLocking,"fd_pool_commit",
              u8_strdup(fd_pool_id(p)),FD_VOID);
    return -1;}
  /* 0 means we didn't do anything */
  else return 0;
}

static int commit_one_oid(fd_pool p,fdtype oid,fd_pool_commit_flags flags)
{
  fd_hashtable locks=&(p->fdp_locks);
  int unlock=(U8_BITP(flags,FD_POOL_COMMIT_UNLOCK));
  fdtype value=fd_hashtable_get(locks,oid,FD_VOID);
  double start_time=u8_elapsed_time();
  if (!(savep(value,flags))) {
    fd_decref(value);
    return 0;}
  else {
    int retcode;
    fdtype oidv[1], values[1];
    oidv[0]=oid; values[0]=value;
    retcode=p->handler->storen(p,1,oidv,values);
    if (retcode<0) {
      u8_seterr(fd_PoolCommitError,"fd_pool_commit",u8_strdup(p->fd_cid));
      fd_decref(value);
      return retcode;}
    else fd_decref(value);
    if (unlock) {
      int unlocked=p->handler->unlock(p,oid);
      if (unlocked>=0) {
        fd_hashtable_op(locks,fd_table_store,oid,FD_VOID);
        u8_log(fddb_loglevel,fd_PoolCommit,
               "[%*t] Saved one OID from %s in %f secs",
               p->fd_cid,u8_elapsed_time()-start_time);
        return 1;}
      else return unlocked;}
    else return 1;}
}

/* Commitment commitment and recovery */

static void cleanup_values(int n,fdtype *vec)
{
  fdtype *scan=vec, *limit=vec+n; while (scan<limit) {
    fdtype v=*scan++;
    if (!(FD_CONSP(v))) {}
    else if (FD_SLOTMAPP(v)) {FD_SLOTMAP_CLEAR_MODIFIED(v);}
    else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_CLEAR_MODIFIED(v);}
    else if (FD_HASHTABLEP(v)) {FD_HASHTABLE_CLEAR_MODIFIED(v);}
    else {}
    fd_decref(v);}
}

static void restore_locks(fd_hashtable locks,
                          struct FD_POOL_WRITES *writes)
{
  int i=0, len=writes->len;
  fdtype *oids=writes->oids, *values=writes->values;
  while (i<len) {
    fdtype val=values[i];
    fd_hashtable_store(locks,oids[i],val);
    fd_decref(val);
    i++;}
  u8_free(oids); u8_free(values);
}

/* Other pool operations */

FD_EXPORT int fd_pool_load(fd_pool p)
{
  if (p->handler->getload)
    return (p->handler->getload)(p);
  else {
    fd_seterr(fd_UnhandledOperation,"fd_pool_load",
              u8_strdup(p->fd_cid),FD_VOID);
    return -1;}
}

FD_EXPORT void fd_pool_close(fd_pool p)
{
  if ((p) && (p->handler) && (p->handler->close))
    p->handler->close(p);
}

FD_EXPORT void fd_pool_swapout(fd_pool p)
{
  u8_log(fddb_loglevel,"PoolDB","Swapping out pool %s",p->fd_cid);
  if (p->handler->swapout) {
    p->handler->swapout(p,FD_VOID);
    u8_log(fddb_loglevel+1,"PoolDB",
           "Finished custom swapout for pool %s",p->fd_cid);}
  if (p) {
    if ((p->fdb_flags)&(FD_STICKY_CACHESIZE))
      fd_reset_hashtable(&(p->fd_cache),-1,1);
    else fd_reset_hashtable(&(p->fd_cache),fd_pool_cache_init,1);}
  if (p) {
    /* Commit and unlock all of the finished OIDs */
    fd_pool_commit(p,FD_VOID,COMMIT_FINISHED);
    fd_devoid_hashtable(&(p->fdp_locks));}
}

/* Callable versions of simple functions */

FD_EXPORT fd_pool _fd_oid2pool(fdtype oid)
{
  int baseid=FD_OID_BASE_ID(oid);
  int baseoff=FD_OID_BASE_OFFSET(oid);
  struct FD_POOL *top=fd_top_pools[baseid];
  if (top==NULL) return NULL;
  else if (baseoff<top->fdp_capacity) return top;
  else if (top->fdp_capacity) {
    u8_raise(_("Corrupted pool table"),"fd_oid2pool",NULL);
    return NULL;}
  else return fd_find_subpool((struct FD_GLUEPOOL *)top,oid);
}
FD_EXPORT fdtype _fd_fetch_oid(fd_pool p,fdtype oid)
{
  FDTC *fdtc=((FD_USE_THREADCACHE)?(fd_threadcache):(NULL));
  fdtype value;
  if (fdtc) {
    fdtype value=((fdtc->oids.fd_n_keys)?
                  (fd_hashtable_get(&(fdtc->oids),oid,FD_VOID)):
                  (FD_VOID));
    if (!(FD_VOIDP(value))) return value;}
  if (p==NULL) p=fd_oid2pool(oid);
  if (p==NULL) return fd_anonymous_oid("fd_fetch_oid",oid);
  else if (p->fdp_n_locks) {
    if (fd_hashtable_probe_novoid(&(p->fdp_locks),oid)) {
      value=fd_hashtable_get(&(p->fdp_locks),oid,FD_VOID);
      if (value == FD_LOCKHOLDER) {
        value=fd_pool_fetch(p,oid);
        fd_hashtable_store(&(p->fdp_locks),oid,value);
        return value;}
      else return value;}
    else {}}
  if (p->fd_cache_level)
    value=fd_hashtable_get(&(p->fd_cache),oid,FD_VOID);
  else value=FD_VOID;
  if (FD_VOIDP(value)) {
    if (fd_ipeval_delay(1)) {
      FD_ADD_TO_CHOICE(fd_pool_delays[p->fdp_serialno],oid);
      return FD_EMPTY_CHOICE;}
    else value=fd_pool_fetch(p,oid);}
  if (FD_ABORTP(value)) return value;
  if (fdtc) fd_hashtable_store(&(fdtc->oids),oid,value);
  return value;
}

FD_EXPORT fdtype _fd_oid_value(fdtype oid)
{
  if (FD_EMPTY_CHOICEP(oid)) return oid;
  else if (FD_OIDP(oid)) return fd_fetch_oid(NULL,oid);
  else return fd_type_error(_("OID"),"fd_oid_value",oid);
}

FD_EXPORT fdtype fd_locked_oid_value(fd_pool p,fdtype oid)
{
  if ((p->fdb_flags)&(FDB_LOCKFREE)) {
    return fd_fetch_oid(p,oid);}
  else {
    fdtype smap=fd_hashtable_get(&(p->fdp_locks),oid,FD_VOID);
    if (FD_VOIDP(smap)) {
      int retval=fd_pool_lock(p,oid);
      if (retval<0) return FD_ERROR_VALUE;
      else if (retval) {
        fdtype v=fd_fetch_oid(p,oid);
        if (FD_ABORTP(v)) return v;
        fd_hashtable_store(&(p->fdp_locks),oid,v);
        return v;}
      else return fd_err(fd_CantLockOID,"fd_locked_oid_value",
                         u8_strdup(p->fd_source),oid);}
    else if (smap==FD_LOCKHOLDER) {
      fdtype v=fd_fetch_oid(p,oid);
      if (FD_ABORTP(v)) return v;
      fd_hashtable_store(&(p->fdp_locks),oid,v);
      return v;}
    else return smap;}
}

/* Pools to lisp and vice versa */

FD_EXPORT fdtype fd_pool2lisp(fd_pool p)
{
  if (p==NULL)
    return FD_ERROR_VALUE;
  else if (p->fdp_serialno<0)
    return fd_err(fd_UnregisteredPool,"fd_pool2lisp",p->fd_cid,FD_VOID);
  else return FDTYPE_IMMEDIATE(fd_pool_type,p->fdp_serialno);
}
FD_EXPORT fd_pool fd_lisp2pool(fdtype lp)
{
  if (FD_PTR_TYPEP(lp,fd_pool_type)) {
    int serial=FD_GET_IMMEDIATE(lp,fd_pool_type);
    if (serial<fd_pool_serial_count)
      return fd_pool_serial_table[serial];
    else {
      char buf[32];
      sprintf(buf,"serial=0x%x",serial);
      fd_seterr3(fd_InvalidPoolPtr,"fd_lisp2pool",buf);
      return NULL;}}
  else if (FD_STRINGP(lp))
    return fd_use_pool(FD_STRDATA(lp));
  else {
    fd_seterr(fd_TypeError,_("not a pool"),NULL,lp);
    return NULL;}
}

/* Iterating over pools */

FD_EXPORT int fd_for_pools(int (*fcn)(fd_pool,void *),void *data)
{
  int i=0, pool_count=0; fd_pool last_pool=NULL;
  while (i < 1024)
    if (fd_top_pools[i]==NULL) i++;
    else if (fd_top_pools[i]->fdp_capacity) {
      fd_pool p=fd_top_pools[i++];
      if (p==last_pool) {}
      else if (fcn(p,data)) return pool_count+1;
      else {last_pool=p; pool_count++;}}
    else {
      struct FD_GLUEPOOL *gp=(struct FD_GLUEPOOL *)fd_top_pools[i++];
      fd_pool *subpools; int j=0;
      subpools=gp->subpools;
      while (j<gp->n_subpools) {
        fd_pool p=subpools[j++];
        int retval=((p==last_pool) ? (0) : (fcn(p,data)));
        last_pool=p;
        if (retval<0) return retval;
        else if (retval) return pool_count+1;
        else pool_count++;}}
  return pool_count;
}

FD_EXPORT fdtype fd_all_pools()
{
  fdtype results=FD_EMPTY_CHOICE; int i=0;
  while (i < 1024)
    if (fd_top_pools[i]==NULL) i++;
    else if (fd_top_pools[i]->fdp_capacity) {
      fdtype lp=fd_pool2lisp(fd_top_pools[i]);
      FD_ADD_TO_CHOICE(results,lp); i++;}
    else {
      struct FD_GLUEPOOL *gp=(struct FD_GLUEPOOL *)fd_top_pools[i++];
      fd_pool *subpools; int j=0;
      subpools=gp->subpools;
      while (j<gp->n_subpools) {
        fdtype lp=fd_pool2lisp(subpools[j++]);
        FD_ADD_TO_CHOICE(results,lp);}}
  return results;
}

FD_EXPORT fd_pool fd_find_pool_by_cid(u8_string cid)
{
  int i=0; u8_string canonical;
  if (cid==NULL) return NULL;
  if (strchr(cid,'@'))
    canonical=u8_canonical_addr(cid);
  else canonical=u8_realpath(cid,NULL);
  while (i < 1024)
    if (fd_top_pools[i] == NULL) i++;
    else if (fd_top_pools[i]->fdp_capacity)
      if ((fd_top_pools[i]->fd_cid) &&
          ((strcmp(canonical,fd_top_pools[i]->fd_cid)) == 0)) {
        u8_free(canonical);
        return fd_top_pools[i];}
      else i++;
    else {
      struct FD_GLUEPOOL *gp=(struct FD_GLUEPOOL *)fd_top_pools[i++];
      fd_pool *subpools=gp->subpools; int j=0;
      while (j<gp->n_subpools)
        if ((subpools[j]) &&
            (subpools[j]->fd_cid) &&
            (strcmp(cid,subpools[j]->fd_cid)==0)) {
          u8_free(canonical);
          return subpools[j];}
        else j++;}
  u8_free(canonical);
  return NULL;
}

FD_EXPORT fdtype fd_find_pools_by_cid(u8_string cid)
{
  fdtype results=FD_EMPTY_CHOICE; u8_string canonical;
  int i=0;
  if (cid==NULL) return results;
  if (strchr(cid,'@'))
    canonical=u8_canonical_addr(cid);
  else canonical=u8_realpath(cid,NULL);
  while (i < 1024)
    if (fd_top_pools[i] == NULL) i++;
    else if (fd_top_pools[i]->fdp_capacity)
      if ((fd_top_pools[i]->fd_cid) &&
          ((strcmp(canonical,fd_top_pools[i]->fd_cid)) == 0)) {
        fdtype poolv=fd_pool2lisp(fd_top_pools[i]); i++;
        fd_incref(poolv);
        FD_ADD_TO_CHOICE(results,poolv);}
      else i++;
    else {
      struct FD_GLUEPOOL *gp=(struct FD_GLUEPOOL *)fd_top_pools[i++];
      fd_pool *subpools=gp->subpools; int j=0;
      while (j<gp->n_subpools)
        if ((subpools[j]->fd_cid) && (strcmp(canonical,subpools[j]->fd_cid)==0)) {
          fdtype poolv=fd_pool2lisp(subpools[j]); j++;
          fd_incref(poolv);
          FD_ADD_TO_CHOICE(results,poolv);}
        else j++;}
  u8_free(canonical);
  return results;
}

FD_EXPORT fd_pool fd_find_pool_by_prefix(u8_string prefix)
{
  int i=0;
  while (i < 1024)
    if (fd_top_pools[i] == NULL) i++;
    else if (fd_top_pools[i]->fdp_capacity)
      if (((fd_top_pools[i]->prefix) &&
           ((strcasecmp(prefix,fd_top_pools[i]->prefix)) == 0)) ||
          ((fd_top_pools[i]->label) &&
           ((strcasecmp(prefix,fd_top_pools[i]->label)) == 0)))
        return fd_top_pools[i];
      else i++;
    else {
      struct FD_GLUEPOOL *gp=(struct FD_GLUEPOOL *)fd_top_pools[i++];
      fd_pool *subpools=gp->subpools; int j=0;
      while (j<gp->n_subpools)
        if (((subpools[j]->prefix) &&
             ((strcasecmp(prefix,subpools[j]->prefix)) == 0)) ||
            ((subpools[j]->label) &&
             ((strcasecmp(prefix,subpools[j]->label)) == 0))) {
          return subpools[j];}
        else j++;}
  return NULL;
}

/* Operations over all pools */

static int do_swapout(fd_pool p,void *data)
{
  fd_pool_swapout(p);
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
  int retval=fd_pool_unlock_all(p,1);
  if (retval<0)
    if (data) {
      u8_log(LOG_CRIT,"POOL_COMMIT_FAIL",
             "Error when committing pool %s",p->fd_cid);
      return 0;}
    else return -1;
  else return 0;
}

FD_EXPORT int fd_commit_oids(fdtype oids,fd_pool_commit_flags flags)
{
  struct POOL2OID { fd_pool pool; fdtype oids;} pool2oids[32];
  int i=0, n_pool2oids=0;
  FD_DO_CHOICES(oid,oids) {
    fd_pool p=fd_oid2pool(oid);
    if (p) {
      int j=0; while (j<n_pool2oids)
                 if (pool2oids[j].pool==p) {
                   FD_ADD_TO_CHOICE(pool2oids[j].oids,oid); i++;}
                 else j++;
      if (n_pool2oids==32)
        return fd_reterr(_("Too many pools for commit"),"fd_commit_oids",
                         NULL,oids);
      pool2oids[n_pool2oids].pool=p;
      pool2oids[n_pool2oids].oids=oid; n_pool2oids++;}
    else return fd_reterr(_("No pool for OID"),"fd_commit_oids",NULL,oid);}
  while (i<n_pool2oids) {
    fdtype simple=fd_simplify_choice(pool2oids[i].oids);
    fd_pool_commit(pool2oids[i].pool,simple,flags);
    fd_decref(simple); i++;}
  return n_pool2oids;
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
  int *commitp=(int *)data;
  fd_pool_unlock_all(p,*commitp);
  return 0;
}

FD_EXPORT int fd_unlock_pools(int commitp)
{
  return fd_for_pools(do_unlock,&commitp);
}

static int accumulate_cachecount(fd_pool p,void *ptr)
{
  int *count=(int *)ptr;
  *count=*count+p->fd_cache.fd_n_keys;
  return 0;
}

FD_EXPORT
long fd_object_cache_load()
{
  int result=0, retval;
  retval=fd_for_pools(accumulate_cachecount,(void *)&result);
  if (retval<0) return retval;
  else return result;
}

static int accumulate_cached(fd_pool p,void *ptr)
{
  fdtype *vals=(fdtype *)ptr;
  fdtype keys=fd_hashtable_keys(&(p->fd_cache));
  FD_ADD_TO_CHOICE(*vals,keys);
  return 0;
}

FD_EXPORT
fdtype fd_cached_oids(fd_pool p)
{
  if (p==NULL) {
    int retval; fdtype result=FD_EMPTY_CHOICE;
    retval=fd_for_pools(accumulate_cached,(void *)&result);
    if (retval<0) {
      fd_decref(result);
      return FD_ERROR_VALUE;}
    else return result;}
  else return fd_hashtable_keys(&(p->fd_cache));
}

/* Common pool initialization stuff */

FD_EXPORT void fd_init_pool(fd_pool p,FD_OID base,unsigned int capacity,
                            struct FD_POOL_HANDLER *h,
                            u8_string source,u8_string cid)
{
  FD_INIT_CONS(p,fd_raw_pool_type);
  p->fdp_base=base; p->fdp_capacity=capacity;
  p->fdp_serialno=-1; p->fd_cache_level=-1; p->fd_read_only=1;
  p->fdb_flags=((h->fetchn)?(FDB_BATCHABLE):(0));
  FD_INIT_STATIC_CONS(&(p->fd_cache),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(p->fdp_locks),fd_hashtable_type);
  fd_make_hashtable(&(p->fd_cache),fd_pool_cache_init);
  fd_make_hashtable(&(p->fdp_locks),fd_pool_lock_init);
  p->fdp_max_adjuncts=0; p->fdp_n_adjuncts=0; p->fdp_adjuncts=NULL;
  p->op_handlers=NULL;
  p->fdp_n_locks=0;
  p->handler=h;
  p->fd_source=u8_strdup(source); p->fd_cid=u8_strdup(cid); p->fd_xid=NULL;
  p->label=NULL; p->prefix=NULL; p->oidnamefn=FD_VOID;
}

FD_EXPORT void fd_set_pool_namefn(fd_pool p,fdtype namefn)
{
  fdtype oldnamefn=p->oidnamefn;
  fd_incref(namefn);
  p->oidnamefn=namefn;
  fd_decref(oldnamefn);
}

/* GLUEPOOL handler (empty) */

static struct FD_POOL_HANDLER gluepool_handler={
  "gluepool", 1, sizeof(struct FD_GLUEPOOL), 11,
  NULL, /* close */
  NULL, /* setcache */
  NULL, /* setbuf */
  NULL, /* alloc */
  NULL, /* fetch */
  NULL, /* fetchn */
  NULL, /* getload */
  NULL, /* lock */
  NULL, /* release */
  NULL, /* storen */
  NULL, /* swapout */
  NULL, /* metadata */
  NULL /* sync */
};

fd_pool (*fd_file_pool_opener)(u8_string spec);

FD_EXPORT fd_pool fd_use_pool(u8_string spec)
{
  if (strchr(spec,';')) {
    fd_pool p=NULL;
    u8_byte *copy=u8_strdup(spec);
    u8_byte *start=copy, *end=strchr(start,';');
    *end='\0'; while (start) {
      p=fd_use_pool(start);
      if (p==NULL) {
        u8_free(copy); return NULL;}
      if ((end) && (end[1])) {
        start=end+1; end=strchr(start,';');
        if (end) *end='\0';}
      else start=NULL;}
    u8_free(copy);
    return p;}
  else if (strchr(spec,'@')) {
    fd_pool p=fd_find_pool_by_cid(spec);
    if (p==NULL) p=fd_open_network_pool(spec,1);
    if (p) fd_register_pool(p);
    return p;}
  else if (fd_file_pool_opener) {
    fd_pool p=fd_file_pool_opener(spec);
    if (p) fd_register_pool(p);
    return p;}
  else {
    fd_seterr3(fd_NoFilePools,"fd_use_pool",u8_strdup(spec));
    return NULL;}
}

FD_EXPORT fd_pool fd_open_pool(u8_string spec)
{
  if (strchr(spec,';')) {
    fd_pool p=NULL;
    u8_byte *copy=u8_strdup(spec);
    u8_byte *start=copy, *end=strchr(start,';');
    *end='\0'; while (start) {
      p=fd_use_pool(start);
      if (p==NULL) {
        u8_free(copy); return NULL;}
      if ((end) && (end[1])) {
        start=end+1; end=strchr(start,';');
        if (end) *end='\0';}
      else start=NULL;}
    u8_free(copy);
    return p;}
  else if (strchr(spec,'@')) {
    fd_pool p=fd_find_pool_by_cid(spec);
    if (p) return p;
    else return fd_open_network_pool(spec,1);}
  else if (fd_file_pool_opener)
    return fd_file_pool_opener(spec);
  else {
    fd_seterr3(fd_NoFilePools,"fd_use_pool",u8_strdup(spec));
    return NULL;}
}

FD_EXPORT fd_pool fd_name2pool(u8_string spec)
{
  fdtype label_string=fd_make_string(NULL,-1,spec);
  fdtype poolv=fd_hashtable_get(&poolid_table,label_string,FD_VOID);
  fd_decref(label_string);
  if (FD_VOIDP(poolv)) return NULL;
  else return fd_lisp2pool(poolv);
}

static int unparse_pool(u8_output out,fdtype x)
{
  fd_pool p=fd_lisp2pool(x); u8_string type; char addrbuf[128];
  if (p==NULL) return 0;
  if ((p->handler) && (p->handler->name)) type=p->handler->name;
  else type="unrecognized";
  sprintf(addrbuf,"@%x/%x+0x%x",FD_OID_HI(p->fdp_base),FD_OID_LO(p->fdp_base),p->fdp_capacity);
  if (p->label)
    if ((p->fd_xid) && (strcmp(p->fd_source,p->fd_xid)))
      u8_printf(out,"#<POOL %s %s #!%lx \"%s\" \"%s|%s\">",
                type,addrbuf,x,p->label,p->fd_source,p->fd_xid);
    else u8_printf(out,"#<POOL %s %s #!%lx \"%s\" \"%s\">",
                   type,addrbuf,x,p->label,p->fd_source);
  else if (p->fd_source)
    if ((p->fd_xid) && (strcmp(p->fd_source,p->fd_xid)))
      u8_printf(out,"#<POOL %s %s #!%lx \"%s|%s\">",
                type,addrbuf,x,p->fd_source,p->fd_xid);
    else u8_printf(out,"#<POOL %s %s #!%lx \"%s\">",
                   type,addrbuf,x,p->fd_source);
  else u8_printf(out,"#<POOL %s,0x%lx>",type,addrbuf,x);
  return 1;
}

static int unparse_raw_pool(u8_output out,fdtype x)
{
  fd_pool p=(fd_pool)x; u8_string type;
  if (p==NULL) return 0;
  if ((p->handler) && (p->handler->name)) type=p->handler->name;
  else type="unrecognized";
  if (p->label)
    if ((p->fd_xid) && (strcmp(p->fd_source,p->fd_xid)))
      u8_printf(out,"#<POOL %s 0x%lx \"%s\" \"%s|%s\">",
                type,x,p->label,p->fd_source,p->fd_xid);
    else u8_printf(out,"#<POOL %s 0x%lx \"%s\" \"%s\">",type,x,p->label,p->fd_source);
  else if (p->fd_source)
    if ((p->fd_xid) && (strcmp(p->fd_source,p->fd_xid)))
      u8_printf(out,"#<POOL %s 0x%lx \"%s|%s\">",type,x,p->fd_source,p->fd_xid);
    else u8_printf(out,"#<POOL %s 0x%lx \"%s\">",type,x,p->fd_source);
  else u8_printf(out,"#<POOL %s,0x%lx>",type,x);
  return 1;
}

static fdtype pool_parsefn(int n,fdtype *args,fd_compound_typeinfo e)
{
  fd_pool p=NULL;
  if (n<3) return FD_VOID;
  else if (n==3)
    p=fd_use_pool(FD_STRING_DATA(args[2]));
  else if ((FD_STRINGP(args[2])) &&
           ((p=fd_find_pool_by_prefix(FD_STRING_DATA(args[2])))==NULL))
    p=fd_use_pool(FD_STRING_DATA(args[3]));
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
  if (FD_TRUEP(val)) fd_ignore_anonymous_oids=1;
  else fd_ignore_anonymous_oids=0;
  return 1;
}

/* Executing pool delays */

FD_EXPORT int fd_execute_pool_delays(fd_pool p,void *data)
{
  fdtype todo=fd_pool_delays[p->fdp_serialno];
  if (FD_EMPTY_CHOICEP(todo)) return 0;
  else {
    /* fd_lock_mutex(&(fd_ipeval_lock)); */
    todo=fd_pool_delays[p->fdp_serialno];
    fd_pool_delays[p->fdp_serialno]=FD_EMPTY_CHOICE;
    todo=fd_simplify_choice(todo);
    /* fd_unlock_mutex(&(fd_ipeval_lock)); */
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval>1)
      u8_log(LOG_NOTICE,ipeval_objfetch,"Fetching %d oids from %s: %q",
             FD_CHOICE_SIZE(todo),p->fd_cid,todo);
    else if (fd_trace_ipeval)
      u8_log(LOG_NOTICE,ipeval_objfetch,"Fetching %d oids from %s",
             FD_CHOICE_SIZE(todo),p->fd_cid);
#endif
    fd_pool_prefetch(p,todo);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval)
      u8_log(LOG_NOTICE,ipeval_objfetch,"Fetched %d oids from %s",
             FD_CHOICE_SIZE(todo),p->fd_cid);
#endif
    return 0;}
}

/* Raw pool table operations */

static fdtype raw_pool_get(fdtype arg,fdtype key,fdtype dflt)
{
  if (FD_OIDP(key)) {
    fd_pool p=(fd_pool)arg;
    FD_OID addr=FD_OID_ADDR(key);
    FD_OID base=p->fdp_base;
    if (FD_OID_COMPARE(addr,base)<0)
      return fd_incref(dflt);
    else {
      unsigned int offset=FD_OID_DIFFERENCE(addr,base);
      int load=fd_pool_load(p);
      if (load<0) return FD_ERROR_VALUE;
      else if (offset<load)
        return fd_fetch_oid(p,key);
      else return fd_incref(dflt);}}
  else return fd_incref(dflt);
}

static fdtype raw_pool_store(fdtype arg,fdtype key,fdtype value)
{
  fd_pool p=(fd_pool)arg;
  if (FD_OIDP(key)) {
    FD_OID addr=FD_OID_ADDR(key);
    FD_OID base=p->fdp_base;
    if (FD_OID_COMPARE(addr,base)<0)
      return fd_err(fd_PoolRangeError,"raw_pool_store",
                    u8_strdup(fd_pool_id(p)),
                    key);
    else {
      unsigned int offset=FD_OID_DIFFERENCE(addr,base);
      int cap=p->fdp_capacity, rv=-1;
      if (offset>cap)
        return fd_err(fd_PoolRangeError,"raw_pool_store",
                      u8_strdup(fd_pool_id(p)),
                      key);
      else if ((p->fdb_flags)&(FDB_LOCKFREE)) {
        rv=fd_hashtable_store(&(p->fd_cache),key,value);}
      else if (fd_pool_lock(p,key)) {
        rv=fd_hashtable_store(&(p->fdp_locks),key,value);}
      else {
        fd_seterr(fd_CantLockOID,"raw_pool_store",
                  u8_strdup(fd_pool_id(p)),
                  key);
        return FD_ERROR_VALUE;}
      if (rv<0) 
        return FD_ERROR_VALUE;
      return FD_VOID;}}
  else return fd_err(fd_NotAnOID,"raw_pool_store",
                     u8_strdup(fd_pool_id(p)),
                     key);
}

static fdtype raw_pool_keys(fdtype arg)
{
  fdtype results=FD_EMPTY_CHOICE;
  fd_pool p=(fd_pool)arg;
  FD_OID base=p->fdp_base;
  unsigned int i=0; int load=fd_pool_load(p);
  if (load<0) return FD_ERROR_VALUE;
  while (i<load) {
    fdtype each=fd_make_oid(FD_OID_PLUS(base,i));
    FD_ADD_TO_CHOICE(results,each);
    i++;}
  return results;
}

/* Mem pools */

static struct FD_POOL_HANDLER mempool_handler;

FD_EXPORT fd_pool fd_make_mempool(u8_string label,FD_OID base,
                                  unsigned int cap,unsigned int load,
                                  unsigned int noswap)
{
  struct FD_MEMPOOL *mp=u8_alloc(struct FD_MEMPOOL);
  fd_init_pool((fd_pool)mp,base,cap,&mempool_handler,label,label);
  mp->label=u8_strdup(label);
  mp->fdp_load=load; mp->fd_read_only=0; mp->noswap=noswap;
  u8_init_mutex(&(mp->fd_lock));
  mp->fdb_flags=mp->fdb_flags|(FDB_LOCKFREE);
  if (fd_register_pool((fd_pool)mp)<0) {
    u8_destroy_mutex(&(mp->fd_lock));
    u8_free(mp->fd_source); u8_free(mp->fd_cid);
    fd_recycle_hashtable(&(mp->fd_cache));
    fd_recycle_hashtable(&(mp->fdp_locks));
    u8_free(mp);
    return NULL;}
  else return (fd_pool)mp;
}

static fdtype mempool_alloc(fd_pool p,int n)
{
  struct FD_MEMPOOL *mp=(fd_mempool)p;
  if ((mp->fdp_load+n)>=mp->fdp_capacity)
    return fd_err(fd_ExhaustedPool,"mempool_alloc",mp->fd_cid,FD_VOID);
  else {
    fdtype results=FD_EMPTY_CHOICE;
    int i=0;
    u8_lock_mutex(&(mp->fd_lock));
    if ((mp->fdp_load+n)>=mp->fdp_capacity) {
      u8_unlock_mutex(&(mp->fd_lock));
      return fd_err(fd_ExhaustedPool,"mempool_alloc",mp->fd_cid,FD_VOID);}
    else {
      FD_OID base=FD_OID_PLUS(mp->fdp_base,mp->fdp_load);
      while (i<n) {
        FD_OID each=FD_OID_PLUS(base,i);
        FD_ADD_TO_CHOICE(results,fd_make_oid(each));
        i++;}
      mp->fdp_load=mp->fdp_load+n; mp->fdp_n_locks=mp->fdp_n_locks+n;
      u8_unlock_mutex(&(mp->fd_lock));
      return fd_simplify_choice(results);}}
}

static fdtype mempool_fetch(fd_pool p,fdtype oid)
{
  struct FD_MEMPOOL *mp=(fd_mempool)p;
  FD_OID addr=FD_OID_ADDR(oid);
  int off=FD_OID_DIFFERENCE(addr,mp->fdp_base);
  if ((off>mp->fdp_load) && (!((p->fdb_flags)&FD_OIDHOLES_OKAY)))
    return fd_err(fd_UnallocatedOID,"mpool_fetch",mp->fd_cid,oid);
  else return FD_EMPTY_CHOICE;
}

static fdtype *mempool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_MEMPOOL *mp=(fd_mempool)p; fdtype *results;
  int i=0; while (i<n) {
    FD_OID addr=FD_OID_ADDR(oids[i]);
    int off=FD_OID_DIFFERENCE(addr,mp->fdp_base);
    if ((off>mp->fdp_load) && (!((p->fdb_flags)&FD_OIDHOLES_OKAY))) {
      fd_seterr(fd_UnallocatedOID,"mpool_fetch",u8_strdup(mp->fd_cid),fd_make_oid(addr));
      return NULL;}
    else i++;}
  results=u8_alloc_n(n,fdtype);
  i=0; while (i<n) results[i++]=FD_EMPTY_CHOICE;
  return results;
}

static int mempool_load(fd_pool p)
{
  struct FD_MEMPOOL *mp=(fd_mempool)p;
  return mp->fdp_load;
}

static int mempool_lock(fd_pool p,fdtype oids)
{
  struct FD_MEMPOOL *mp=(fd_mempool)p;
  u8_lock_mutex(&(mp->fd_lock));
  mp->fdp_n_locks=mp->fdp_n_locks+FD_CHOICE_SIZE(oids);
  return 1;
}

static int mempool_unlock(fd_pool p,fdtype oids)
{
  struct FD_MEMPOOL *mp=(fd_mempool)p;
  u8_lock_mutex(&(mp->fd_lock));
  mp->fdp_n_locks=mp->fdp_n_locks-FD_CHOICE_SIZE(oids);
  return 1;
}

static int mempool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  return 1;
}

static int mempool_swapout(fd_pool p,fdtype oidvals)
{
  struct FD_MEMPOOL *mp=(fd_mempool)p;
  if (mp->noswap) return 0;
  else if (FD_VOIDP(oidvals)) {
    fd_reset_hashtable(&(p->fdp_locks),fd_pool_lock_init,1);
    return 1;}
  else {
    fdtype oids=fd_make_simple_choice(oidvals);
    if (FD_EMPTY_CHOICEP(oids)) {}
    else if (FD_OIDP(oids)) {
      fd_hashtable_op(&(p->fdp_locks),fd_table_replace,oids,FD_VOID);}
    else {
      fd_hashtable_iterkeys
        (&(p->fdp_locks),fd_table_replace,
         FD_CHOICE_SIZE(oids),FD_CHOICE_DATA(oids),FD_VOID);
      fd_devoid_hashtable(&(p->fdp_locks));}
    fd_decref(oids);
    return 1;}
}

static struct FD_POOL_HANDLER mempool_handler={
  "mempool", 1, sizeof(struct FD_MEMPOOL), 12,
  NULL, /* close */
  NULL, /* setcache */
  NULL, /* setbuf */
  mempool_alloc, /* alloc */
  mempool_fetch, /* fetch */
  mempool_fetchn, /* fetchn */
  mempool_load, /* getload */
  NULL, /* lock (mempool_lock) */
  NULL, /* release (mempool_unlock) */
  mempool_storen, /* storen */
  mempool_swapout, /* swapout */
  NULL, /* metadata */
  NULL}; /* sync */

FD_EXPORT int fd_clean_mempool(fd_pool p)
{
  if (p->handler!=&mempool_handler)
    return fd_reterr
      (fd_TypeError,"fd_clean_mempool",
       _("mempool"),fd_pool2lisp(p));
  else {
    fd_remove_deadwood(&(p->fdp_locks));
    fd_devoid_hashtable(&(p->fdp_locks));
    fd_remove_deadwood(&(p->fd_cache));
    fd_devoid_hashtable(&(p->fd_cache));
    return p->fd_cache.fd_n_keys+p->fdp_locks.fd_n_keys;}
}

FD_EXPORT int fd_reset_mempool(fd_pool p)
{
  if (p->handler!=&mempool_handler)
    return fd_reterr
      (fd_TypeError,"fd_clean_mempool",
       _("mempool"),fd_pool2lisp(p));
  else {
    struct FD_MEMPOOL *mp=(struct FD_MEMPOOL *)p;
    fd_lock_mutex(&(mp->fd_lock));
    fd_reset_hashtable(&(p->fdp_locks),-1,1);
    fd_reset_hashtable(&(p->fd_cache),-1,1);
    mp->fdp_load=0;
    return 0;}
}

/* Fetch pools */

/* Fetch pools are pools which keep their data externally and take care
   of their own data management.  They basically have a fetch function and
   a load function but may be extended to be clever about saving in some way. */

FD_EXPORT
fd_pool fd_make_extpool(u8_string label,
                        FD_OID base,int cap,
                        fdtype fetchfn,fdtype savefn,
                        fdtype lockfn,fdtype allocfn,
                        fdtype state)
{
  if (!(FD_EXPECT_TRUE(FD_APPLICABLEP(fetchfn)))) {
    fd_seterr(fd_TypeError,"fd_make_extpool","fetch function",
              fd_incref(fetchfn));
    return NULL;}
  else if (!(goodfunarg(savefn))) {
    fd_seterr(fd_TypeError,"fd_make_extpool","save function",
              fd_incref(savefn));
    return NULL;}
  else if (!(goodfunarg(lockfn))) {
    fd_seterr(fd_TypeError,"fd_make_extpool","lock function",
              fd_incref(lockfn));
    return NULL;}
  else if (!(goodfunarg(allocfn))) {
    fd_seterr(fd_TypeError,"fd_make_extpool","alloc function",
              fd_incref(allocfn));
    return NULL;}
  else {
    struct FD_EXTPOOL *xp=u8_alloc(struct FD_EXTPOOL);
    memset(xp,0,sizeof(struct FD_EXTPOOL));
    fd_init_pool((fd_pool)xp,base,cap,&fd_extpool_handler,label,label);
    if (!(FD_VOIDP(savefn))) xp->fd_read_only=0;
    fd_register_pool((fd_pool)xp);
    fd_incref(fetchfn); fd_incref(savefn);
    fd_incref(lockfn); fd_incref(allocfn); 
    fd_incref(state);
    xp->fetchfn=fetchfn; xp->savefn=savefn;
    xp->lockfn=lockfn; xp->allocfn=allocfn;
    xp->state=state; xp->label=label;
    xp->fdb_flags=xp->fdb_flags|FD_OIDHOLES_OKAY;
    if ((FD_VOIDP(lockfn))||(FD_FALSEP(lockfn)))
      xp->fdb_flags=xp->fdb_flags|FDB_LOCKFREE;
    return (fd_pool)xp;}
}

static fdtype extpool_fetch(fd_pool p,fdtype oid)
{
  struct FD_EXTPOOL *xp=(fd_extpool)p;
  fdtype state=xp->state, value, fetchfn=xp->fetchfn;
  struct FD_FUNCTION *fptr=((FD_FUNCTIONP(fetchfn))?
                            ((struct FD_FUNCTION *)fetchfn):
                            (NULL));
  if ((FD_VOIDP(state))||(FD_FALSEP(state))||
      ((fptr)&&(fptr->fdfn_arity==1)))
    value=fd_apply(fetchfn,1,&oid);
  else {
    fdtype args[2]; args[0]=oid; args[1]=state;
    value=fd_apply(fetchfn,2,args);}
  if (FD_ABORTP(value)) return value;
  else if ((FD_EMPTY_CHOICEP(value))||(FD_VOIDP(value)))
    if ((p->fdb_flags)&FD_OIDHOLES_OKAY)
      return FD_EMPTY_CHOICE;
    else return fd_err(fd_UnallocatedOID,"extpool_fetch",xp->fd_cid,oid);
  else return value;
}

/* This assumes that the FETCH function handles a vector intelligently,
   and just calls it on the vector of OIDs and extracts the data from
   the returned vector.  */
static fdtype *extpool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_EXTPOOL *xp=(fd_extpool)p;
  struct FD_VECTOR vstruct; fdtype vecarg;
  fdtype state=xp->state, fetchfn=xp->fetchfn, value=FD_VOID;
  struct FD_FUNCTION *fptr=((FD_FUNCTIONP(fetchfn))?
                            ((struct FD_FUNCTION *)fetchfn):
                            (NULL));
  if (!(xp->fdb_flags&(FDB_BATCHABLE))) return NULL;
  FD_INIT_STATIC_CONS(&vstruct,fd_vector_type);
  vstruct.fd_veclen=n; vstruct.fd_vecelts=oids;
  vstruct.fd_freedata=0;
  vecarg=FDTYPE_CONS(&vstruct);
  if ((FD_VOIDP(state))||(FD_FALSEP(state))||
      ((fptr)&&(fptr->fdfn_arity==1)))
    value=fd_apply(fetchfn,1,&vecarg);
  else {
    fdtype args[2]; args[0]=vecarg; args[1]=state;
    value=fd_apply(fetchfn,2,args);}
  if (FD_ABORTP(value)) return NULL;
  else if (FD_VECTORP(value)) {
    struct FD_VECTOR *vstruct=(struct FD_VECTOR *)value;
    fdtype *results=u8_alloc_n(n,fdtype);
    memcpy(results,vstruct->fd_vecelts,sizeof(fdtype)*n);
    /* Free the CONS itself (and maybe data), to avoid DECREF/INCREF
       of values. */
    if (vstruct->fd_freedata) u8_free(vstruct->fd_vecelts);
    u8_free((struct FD_CONS *)value);
    return results;}
  else {
    fdtype *values=u8_alloc_n(n,fdtype);
    if ((FD_VOIDP(state))||(FD_FALSEP(state))||
        ((fptr)&&(fptr->fdfn_arity==1))) {
      int i=0; while (i<n) {
        fdtype oid=oids[i];
        fdtype value=fd_apply(fetchfn,1,&oid);
        if (FD_ABORTP(value)) {
          int j=0; while (j<i) {fd_decref(values[j]); j++;}
          u8_free(values);
          return NULL;}
        else values[i++]=value;}
      return values;}
    else {
      fdtype args[2]; int i=0; args[1]=state;
      i=0; while (i<n) {
        fdtype oid=oids[i], value;
        args[0]=oid; value=fd_apply(fetchfn,2,args);
        if (FD_ABORTP(value)) {
          int j=0; while (j<i) {fd_decref(values[j]); j++;}
          u8_free(values);
          return NULL;}
        else values[i++]=value;}
      return values;}}
}

static int extpool_lock(fd_pool p,fdtype oids)
{
  struct FD_EXTPOOL *xp=(struct FD_EXTPOOL *) p;
  if (FD_APPLICABLEP(xp->lockfn)) {
    fdtype lockfn=xp->lockfn;
    fd_hashtable locks=&(xp->fdp_locks);
    fd_hashtable cache=&(xp->fd_cache);
    FD_DO_CHOICES(oid,oids) {
      fdtype cur=fd_hashtable_get(cache,oid,FD_VOID);
      fdtype args[3]={lock_symbol,oid,
                      ((cur==FD_LOCKHOLDER)?(FD_VOID):(cur))};
      fdtype value=fd_apply(lockfn,3,args);
      if (FD_ABORTP(value)) {
        fd_decref(cur);
        return -1;}
      else if (FD_FALSEP(value)) {}
      else if (FD_VOIDP(value)) {
        fd_hashtable_store(locks,oid,FD_LOCKHOLDER);}
      else fd_hashtable_store(locks,oid,value);
      fd_decref(cur); fd_decref(value);}}
  else if (FD_FALSEP(xp->lockfn)) {
    fd_seterr(fd_CantLockOID,"fd_pool_lock",
              u8_strdup(p->fd_cid),fd_incref(oids));
    return -1;}
  else if ((xp->fdb_flags)&(FDB_LOCKFREE)) {
    return 0;}
  else {
    FD_DO_CHOICES(oid,oids) {
      fd_hashtable_store(&(p->fdp_locks),oids,FD_LOCKHOLDER);}}
  return 1;
}

static fdtype extpool_alloc(fd_pool p,int n)
{
  struct FD_EXTPOOL *ep=(struct FD_EXTPOOL *)p;
  if (FD_VOIDP(ep->allocfn))
    return fd_err(_("No OID alloc method"),"extpool_alloc",NULL,
                  fd_pool2lisp(p));
  else {
    fdtype args[2]; args[0]=FD_INT(n); args[1]=ep->state;
    if (FD_FIXNUMP(args[0]))
      return fd_apply(ep->allocfn,2,args);
    else {
      fdtype result=fd_apply(ep->allocfn,2,args);
      fd_decref(args[0]);
      return result;}}
}

static int extpool_unlock(fd_pool p,fdtype oids)
{
  struct FD_EXTPOOL *xp=(struct FD_EXTPOOL *) p;
  if (FD_APPLICABLEP(xp->lockfn)) {
    fdtype lockfn=xp->lockfn; 
    fd_hashtable locks=&(xp->fdp_locks);
    fd_hashtable cache=&(xp->fd_cache);
    FD_DO_CHOICES(oid,oids) {
      fdtype cur=fd_hashtable_get(locks,oid,FD_VOID);
      fdtype args[3]={unlock_symbol,oid,
                      ((cur==FD_LOCKHOLDER)?(FD_VOID):(cur))};
      fdtype value=fd_apply(lockfn,3,args);
      fd_hashtable_store(locks,oid,FD_VOID);
      if (FD_ABORTP(value)) {
        fd_decref(cur);
        return -1;}
      else if (FD_VOIDP(value)) {}
      else {
        fd_hashtable_store(cache,oid,value);}
      fd_decref(cur); fd_decref(value);}}
  else if ((xp->fdb_flags)&(FDB_LOCKFREE)) {
    return 0;}
  else {
    FD_DO_CHOICES(oid,oids) {
      fd_hashtable_store(&(p->fdp_locks),oids,FD_VOID);}}
  return 1;
}

FD_EXPORT int fd_extpool_cache_value(fd_pool p,fdtype oid,fdtype value)
{
  if (p->handler==&fd_extpool_handler)
    return fd_hashtable_store(&(p->fd_cache),oid,value);
  else return fd_reterr
         (fd_TypeError,"fd_extpool_cache_value",
          u8_strdup("extpool"),fd_pool2lisp(p));
}

struct FD_POOL_HANDLER fd_extpool_handler={
  "extpool", 1, sizeof(struct FD_EXTPOOL), 12,
  NULL, /* close */
  NULL, /* setcache */
  NULL, /* setbuf */
  extpool_alloc, /* alloc */
  extpool_fetch, /* fetch */
  extpool_fetchn, /* fetchn */
  NULL, /* getload */
  extpool_lock, /* lock */
  extpool_unlock, /* release */
  NULL, /* storen */
  NULL, /* swapout */
  NULL, /* metadata */
  NULL}; /* sync */

static void recycle_raw_pool(struct FD_CONS *c)
{
  struct FD_POOL *p=(struct FD_POOL *)c;
  if (p->handler==&fd_extpool_handler) {
    struct FD_EXTPOOL *xp=(struct FD_EXTPOOL *)c;
    fd_decref(xp->fetchfn); fd_decref(xp->savefn);
    fd_decref(xp->lockfn); fd_decref(xp->allocfn); 
    fd_decref(xp->state);}
  fd_recycle_hashtable(&(p->fd_cache));
  fd_recycle_hashtable(&(p->fdp_locks));
  u8_free(p->fd_cid);
  u8_free(p->fd_source);
  if (p->fd_xid) u8_free(p->fd_xid);
  if (p->label) u8_free(p->label);
  if (p->prefix) u8_free(p->prefix);
  fd_decref(p->oidnamefn); fd_decref(p->oidnamefn);
  u8_free(p);
}

static fdtype copy_raw_pool(fdtype x,int deep)
{
  return x;
}

/* Initialization */

fd_ptr_type fd_pool_type, fd_raw_pool_type;

static int check_pool(fdtype x)
{
  int serial=FD_GET_IMMEDIATE(x,fd_pool_type);
  if (serial<0) return 0;
  else if (serial<fd_pool_serial_count) return 1;
  else return 0;
}

/* This is just for use from the debugger, so we can allocate it
   statically. */
static char oid_info_buf[512];

static u8_string _more_oid_info(fdtype oid)
{
  if (FD_OIDP(oid)) {
    FD_OID addr=FD_OID_ADDR(oid);
    fd_pool p=fd_oid2pool(oid);
    unsigned int hi=FD_OID_HI(addr), lo=FD_OID_LO(addr);
    if (p==NULL)
      sprintf(oid_info_buf,"@%x/%x in no pool",hi,lo);
    else if (p->label)
      if (p->fd_source)
        if (p->fd_cid)
          if (p->fd_xid)
            sprintf(oid_info_buf,"@%x/%x in %s from %s = %s = %s",
                    hi,lo,p->label,p->fd_source,p->fd_cid,p->fd_xid);
          else sprintf(oid_info_buf,"@%x/%x in %s from %s = %s",hi,lo,p->label,p->fd_source,p->fd_cid);
        else sprintf(oid_info_buf,"@%x/%x in %s from %s",hi,lo,p->label,p->fd_source);
      else sprintf(oid_info_buf,"@%x/%x in %s",hi,lo,p->label);
    else if (p->fd_source)
      if (p->fd_cid)
        if (p->fd_xid)
          sprintf(oid_info_buf,"@%x/%x from %s = %s = %s",hi,lo,p->fd_source,p->fd_cid,p->fd_xid);
        else sprintf(oid_info_buf,"@%x/%x from %s = %s",hi,lo,p->fd_source,p->fd_cid);
      else sprintf(oid_info_buf,"@%x/%x from %s",hi,lo,p->fd_source);
    else if (p->fd_cid)
      if (p->fd_xid)
        sprintf(oid_info_buf,"@%x/%x from %s = %s",hi,lo,p->fd_cid,p->fd_xid);
      else sprintf(oid_info_buf,"@%x/%x from %s",hi,lo,p->fd_cid);
    else sprintf(oid_info_buf,"@%x/%x in stub pool",hi,lo);
    return oid_info_buf;}
  else return "not an oid!";
}


/* Config functions */

FD_EXPORT fdtype fd_poolconfig_get(fdtype var,void *vptr)
{
  fd_pool *pptr=(fd_pool *)vptr;
  if (*pptr==NULL)
    return fd_err(_("Config mis-config"),"pool_config_get",NULL,var);
  else {
    if (*pptr)
      return fd_pool2lisp(*pptr);
    else return FD_EMPTY_CHOICE;}
}
FD_EXPORT int fd_poolconfig_set(fdtype ignored,fdtype v,void *vptr)
{
  fd_pool *pptr=(fd_pool *)vptr;
  if (FD_POOLP(v)) *pptr=fd_lisp2pool(v);
  else if (FD_STRINGP(v)) {
    fd_pool p=fd_use_pool(FD_STRDATA(v));
    if (p) *pptr=p; else return -1;}
  else return fd_type_error(_("pool spec"),"pool_config_set",v);
  return 1;
}

/* Lisp pointers to Pool pointers */

FD_EXPORT fd_pool _fd_get_poolptr(fdtype x)
{
  return fd_get_poolptr(x);
}

/* Initialization */

FD_EXPORT void fd_init_pools_c()
{
  int i=0; while (i < 1024) fd_top_pools[i++]=NULL;

  u8_register_source_file(_FILEINFO);

  fd_pool_type=fd_register_immediate_type("pool",check_pool);
  fd_raw_pool_type=fd_register_cons_type("raw pool");

  fd_type_names[fd_pool_type]=_("pool");
  fd_type_names[fd_raw_pool_type]=_("raw pool");

  _fd_oid_info=_more_oid_info;

  lock_symbol=fd_intern("LOCK");  
  unlock_symbol=fd_intern("UNLOCK");

  {
    struct FD_COMPOUND_TYPEINFO *e=fd_register_compound(fd_intern("POOL"),NULL,NULL);
    e->fd_compound_parser=pool_parsefn;}

  FD_INIT_STATIC_CONS(&poolid_table,fd_hashtable_type);
  fd_make_hashtable(&poolid_table,-1);

#if (FD_USE_TLS)
  u8_new_threadkey(&fd_pool_delays_key,NULL);
#endif
  fd_unparsers[fd_pool_type]=unparse_pool;
  fd_unparsers[fd_raw_pool_type]=unparse_raw_pool;
  fd_recyclers[fd_raw_pool_type]=recycle_raw_pool;
  fd_copiers[fd_raw_pool_type]=copy_raw_pool;
  fd_register_config
    ("ANONYMOUSOK",_("whether value of anonymous OIDs are {} or signal an error"),
     config_get_anonymousok,
     config_set_anonymousok,NULL);
  fd_register_config("DEFAULTPOOL",_("Default location for new OID allocation"),
                     fd_poolconfig_get,fd_poolconfig_set,
                     &fd_default_pool);

  fd_tablefns[fd_raw_pool_type]=u8_zalloc(struct FD_TABLEFNS);
  fd_tablefns[fd_raw_pool_type]->get=(fd_table_get_fn)raw_pool_get;
  fd_tablefns[fd_raw_pool_type]->store=(fd_table_store_fn)raw_pool_store;
  fd_tablefns[fd_raw_pool_type]->keys=(fd_table_keys_fn)raw_pool_keys;


#if FD_CALLTRACK_ENABLED
  {
    fd_calltrack_sensor cts=fd_get_calltrack_sensor("OIDS",1);
    cts->enabled=1; cts->intfcn=fd_object_cache_load;}
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
