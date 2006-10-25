/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_POOLS 1
#define FD_INLINE_IPEVAL 1

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/pools.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8netfns.h>

fd_exception fd_UnknownPool=_("Unknown pool");
fd_exception fd_CantLockOID=_("Can't lock OID");
fd_exception fd_ReadOnlyPool=_("pool is read-only");
fd_exception fd_InvalidPoolPtr=_("Invalid pool PTR");
fd_exception fd_NotAFilePool=_("not a file pool");
fd_exception fd_NoFilePools=_("file pools are not supported");
fd_exception fd_AnonymousOID=_("no pool covers this OID");
fd_exception fd_NotAPool=_("pool");
fd_exception fd_BadFilePoolLabel=_("file pool label is not a string");
fd_exception fd_ExhaustedPool=_("pool has no more OIDs");

int fd_n_pools=0;

struct FD_POOL *fd_top_pools[1024];
static struct FD_HASHTABLE poolid_table;

static u8_condition ipeval_objfetch="OBJFETCH";

static fd_pool pool_serial_table[1024], *extra_pools=NULL;
static int pool_serial_count=0;

#if FD_GLOBAL_IPEVAL
fdtype *fd_pool_delays=NULL;
#elif FD_USE_TLS
u8_tld_key fd_pool_delays_key;
#elif FD_USE__THREAD
__thread fdtype *fd_pool_delays=NULL;
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
  delays=u8_malloc(sizeof(fdtype)*FD_N_POOL_DELAYS);
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
  fd_pool_delays=u8_malloc(sizeof(fdtype)*FD_N_POOL_DELAYS);
  while (i<FD_N_POOL_DELAYS) fd_pool_delays[i++]=FD_EMPTY_CHOICE;
}
#endif

static struct FD_POOL_HANDLER gluepool_handler;
static void (*pool_conflict_handler)(fd_pool upstart,fd_pool holder)=NULL;

static void pool_conflict(fd_pool upstart,fd_pool holder);
static struct FD_GLUEPOOL *make_gluepool(FD_OID base);
static void add_to_gluepool(struct FD_GLUEPOOL *gp,fd_pool p);

FD_EXPORT fd_pool fd_open_network_pool(u8_string spec,int read_only);

int fd_ignore_anonymous_oids=0;

#if FD_THREADS_ENABLED
static u8_mutex pool_registry_lock;
#endif

FD_EXPORT fdtype fd_anonymous_oid(fdtype oid)
{
  if (fd_ignore_anonymous_oids) return FD_EMPTY_CHOICE;
  else return fd_err(fd_AnonymousOID,NULL,NULL,oid);
}

/* Pool caching */

FD_EXPORT void fd_pool_setcache(fd_pool p,int level)
{
  if (p->handler->setcache) {
    p->handler->setcache(p,level);
    p->cache_level=level;}
  p->flags=p->flags|FD_EXPLICIT_SETCACHE;
}

static void init_cache_level(fd_pool p)
{
  if (FD_EXPECT_FALSE(p->cache_level<0)) {
    p->cache_level=fd_default_cache_level;
    if (p->handler->setcache)
      p->handler->setcache(p,fd_default_cache_level);}
}

/* Registering pools */

#define FD_TOP_POOL_SIZE 0x100000

FD_EXPORT int fd_register_pool(fd_pool p)
{
  unsigned int capacity=p->capacity, serial_no;
  int baseindex=fd_get_oid_base_index(p->base,1);
  if (p->serialno>=0) return 0;
  else if (baseindex<0) return baseindex;
  fd_lock_mutex(&(pool_registry_lock));
  /* Set up the serial number */
  serial_no=p->serialno=pool_serial_count++; fd_n_pools++;
  pool_serial_table[serial_no]=p;
  if (capacity>=FD_TOP_POOL_SIZE) {
    int i=0, lim=capacity/FD_TOP_POOL_SIZE;
    /* Now get a baseid and register the pool in top_pools */
    while (i<lim) {
      FD_OID base=FD_OID_PLUS(p->base,(FD_TOP_POOL_SIZE*i));
      int baseid=fd_get_oid_base_index(base,1);
      if (baseid<0) {
	fd_unlock_mutex(&(pool_registry_lock));
	return -1;}
      else if (fd_top_pools[baseid]) {
	pool_conflict(p,fd_top_pools[baseid]);
	fd_unlock_mutex(&(pool_registry_lock));
	return -1;}
      else fd_top_pools[baseid]=p;
      i++;}}
  else if (fd_top_pools[baseindex] == NULL) {
    struct FD_GLUEPOOL *gluepool=make_gluepool(fd_base_oids[baseindex]);
    fd_top_pools[baseindex]=(struct FD_POOL *)gluepool;
    add_to_gluepool(gluepool,p);}
  else if (fd_top_pools[baseindex]->capacity) {
    pool_conflict(p,fd_top_pools[baseindex]);
    fd_unlock_mutex(&(pool_registry_lock));
    return -1;}
  else add_to_gluepool((struct FD_GLUEPOOL *)fd_top_pools[baseindex],p);
  fd_unlock_mutex(&(pool_registry_lock));
  if (p->label) {
    u8_byte *dot=strchr(p->label,'.');
    fdtype pkey, probe;
    if (dot) {
      pkey=fd_extract_string(NULL,p->label,dot);
      probe=fd_hashtable_get(&poolid_table,pkey,FD_EMPTY_CHOICE);
      if (FD_EMPTY_CHOICEP(probe)) {
	fd_hashtable_store(&poolid_table,pkey,fd_pool2lisp(p));
	p->prefix=FD_STRDATA(pkey);}
      else fd_decref(pkey);}
    pkey=fd_extract_string(NULL,p->label,NULL);
    probe=fd_hashtable_get(&poolid_table,pkey,FD_EMPTY_CHOICE);
    if (FD_EMPTY_CHOICEP(probe)) {
      fd_hashtable_store(&poolid_table,pkey,fd_pool2lisp(p));
      if (p->prefix == NULL) p->prefix=FD_STRDATA(pkey);}}
  return 1;
}

static struct FD_GLUEPOOL *make_gluepool(FD_OID base)
{
  struct FD_GLUEPOOL *pool=u8_malloc(sizeof(struct FD_GLUEPOOL));
  pool->base=base; pool->capacity=0; pool->read_only=1;
  pool->serialno=fd_get_oid_base_index(base,1);
  pool->label="gluepool"; pool->source=NULL;
  pool->n_subpools=0; pool->subpools=NULL;
  pool->handler=&gluepool_handler;
  fd_make_hashtable(&(pool->cache),64,NULL);
  fd_make_hashtable(&(pool->locks),64,NULL);
  return pool;
}

static void add_to_gluepool(struct FD_GLUEPOOL *gp,fd_pool p)
{
  if (gp->n_subpools == 0) {
    struct FD_POOL **pools=u8_malloc(sizeof(struct FD_POOL *));
    pools[0]=p; gp->n_subpools=1; gp->subpools=pools;}
  else {
    int comparison;
    struct FD_POOL **bottom=gp->subpools;
    struct FD_POOL **top=gp->subpools+(gp->n_subpools)-1;
    struct FD_POOL **read, **new, **write, **ipoint;
    struct FD_POOL **middle;
    /* First find where it goes (or a conflict if there is one) */
    while (top>=bottom) {
      middle=bottom+(top-bottom)/2;
      comparison=FD_OID_COMPARE(p->base,(*middle)->base);
      if (comparison == 0) break;
      else if (bottom==top) break;
      else if (comparison<0) top=middle-1; else bottom=middle+1;}
    /* If there is a conflict, we report it and quit */
    if (comparison==0) {pool_conflict(p,*middle); return;}
    /* Now we make a new vector to copy into */
    write=new=u8_malloc(sizeof(struct FD_POOL *)*(gp->n_subpools+1));
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
    p->serialno=(gp->serialno)<<10+gp->n_subpools;
    gp->subpools=new; gp->n_subpools++;}
  p->serialno=pool_serial_count++;
  pool_serial_table[p->serialno]=p;
}

/* Finding the subpool */

FD_EXPORT fd_pool fd_find_subpool(struct FD_GLUEPOOL *gp,fdtype oid)
{
  FD_OID addr=FD_OID_ADDR(oid);
  struct FD_POOL **subpools=gp->subpools; int n=gp->n_subpools;
  struct FD_POOL **bottom=subpools, **top=subpools+(n-1);
  while (top>=bottom) {
    struct FD_POOL **middle=bottom+(top-bottom)/2;
    int comparison=FD_OID_COMPARE(addr,(*middle)->base);
    unsigned int difference=FD_OID_DIFFERENCE(addr,(*middle)->base);
    if (comparison < 0) top=middle-1;
    else if ((difference < ((*middle)->capacity)))
      return *middle;
    else if (bottom==top) break;
    else bottom=middle+1;}
  return NULL;
}

/* Handling pool conflicts */

static void pool_conflict(fd_pool upstart,fd_pool holder)
{
  if (pool_conflict_handler) pool_conflict_handler(upstart,holder);
  else 
    u8_warn(_("Pool conflict"),"%s (from %s) and existing pool %s (from %s)\n",
	    upstart->label,upstart->source,holder->label,holder->source);
}

/* Fetching OID values */

FD_EXPORT fdtype fd_pool_fetch(fd_pool p,fdtype oid)
{
  fdtype v;
  init_cache_level(p);
  v=p->handler->fetch(p,oid);
  if (FD_ABORTP(v)) return v;
  if (p->n_locks)
    if (fd_hashtable_op(&(p->locks),fd_table_replace_novoid,oid,v)) 
      return v;
  if (FD_SLOTMAPP(v)) {FD_SLOTMAP_SET_READONLY(v);}
  else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_SET_READONLY(v);}
  if (p->cache_level>0) fd_hashtable_store(&(p->cache),oid,v);
  return v;
}

FD_EXPORT fdtype fd_pool_alloc(fd_pool p,int n)
{
  if (p->read_only)
    return fd_err(fd_ReadOnlyPool,"fd_pool_alloc",
		  u8_strdup(p->source),FD_VOID);
  else {
    fdtype result=p->handler->alloc(p,n);
    if (FD_CHOICEP(result))
      fd_hashtable_iterkeys(&(p->locks),fd_table_store,
			    FD_CHOICE_SIZE(result),FD_CHOICE_DATA(result),
			    FD_EMPTY_CHOICE);
    else fd_hashtable_store(&(p->locks),result,FD_EMPTY_CHOICE);
    return result;}
}

FD_EXPORT int fd_pool_prefetch(fd_pool p,fdtype oids)
{
  int decref_oids=0;
  if (p==NULL) {
    fd_seterr(fd_NotAPool,"fd_pool_prefetch",
	      u8_strdup("NULL pool ptr"),FD_VOID);
    return -1;}
  else init_cache_level(p);
  if (p->cache_level<1) return 0;
  if (p->handler->fetchn==NULL)
    if (fd_ipeval_delay(FD_CHOICE_SIZE(oids))) {
      FD_ADD_TO_CHOICE(fd_pool_delays[p->serialno],oids);
      return 0;}
    else {
      int n_fetches=0;
      FD_DO_CHOICES(oid,oids) {
	fdtype v=fd_pool_fetch(p,oid); n_fetches++; fd_decref(v);}
      return n_fetches;}
  if (FD_ACHOICEP(oids)) {
    oids=fd_make_simple_choice(oids); decref_oids=1;}
  if (fd_ipeval_status()) {
    FD_HASHTABLE *cache=&(p->cache); int n_to_fetch=0;
    /* fdtype oidschoice=fd_make_simple_choice(oids); */
    fdtype *delays=&(fd_pool_delays[p->serialno]);
    FD_DO_CHOICES(oid,oids)
      if (fd_hashtable_probe_novoid(cache,oid)) {}
      else {
	FD_ADD_TO_CHOICE(*delays,oid);
	n_to_fetch++;}
    fd_ipeval_delay(n_to_fetch);
    /* fd_decref(oidschoice); */
    return 0;}
  else if (FD_CHOICEP(oids)) {
    int i=0, n=FD_CHOICE_SIZE(oids), some_locked=0;
    struct FD_HASHTABLE *cache=&(p->cache), *locks=&(p->locks);
    fdtype *values, *oidv=u8_malloc(sizeof(fdtype)*n), *write=oidv;
    FD_DO_CHOICES(o,oids)
      if ((fd_hashtable_probe_novoid(cache,o)==0) &&
	  (fd_hashtable_probe_novoid(locks,o)==0))
	*write++=o;
      else if (fd_hashtable_op(locks,fd_table_test,o,FD_LOCKHOLDER)) {
	*write++=o; some_locked=1;}
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
	  if (fd_hashtable_op(&(p->locks),fd_table_replace_novoid,
			      oidv[j],values[j])==0) {
	    fdtype v=values[j];
	    if (FD_SLOTMAPP(v)) {FD_SLOTMAP_SET_READONLY(v);}
	    else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_SET_READONLY(v);}
	    fd_hashtable_op(&(p->cache),fd_table_store,oidv[j],v);}
	  j++;}}
      else {
	int j=0; while (j<n) {
	  fdtype v=values[j++];
	  if (FD_SLOTMAPP(v)) {FD_SLOTMAP_SET_READONLY(v);}
	  else if (FD_SCHEMAPP(v)) {FD_SCHEMAP_SET_READONLY(v);}}
	fd_hashtable_iter(&(p->cache),fd_table_store_noref,n,oidv,values);}
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
    if ((p->n_locks==0) ||
	(fd_hashtable_op(&(p->locks),fd_table_replace_novoid,oids,v)==0)) 
      fd_hashtable_store(&(p->cache),oids,v);
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
  fd_pool _pools[32], *pools=_pools;
  fdtype _toget[32], *toget=_toget;
  int i=0, n_pools=0, max_pools=32, total=0;
  FD_DO_CHOICES(oid,oids) {
    if (FD_OIDP(oid)) {
      fd_pool p=fd_oid2pool(oid);
      if ((p) &&
	  ((fd_hashtable_probe_novoid(&(p->locks),oid)) 
	   ? (fd_hashtable_op(&(p->locks),fd_table_test,oid,FD_LOCKHOLDER))
	   : (fd_hashtable_probe_novoid(&(p->cache),oid)==0))) {
	i=0; while (i<n_pools) if (pools[i]==p) break; else i++;
	if (i>=n_pools)
	  /* Create a pool entry if neccessary */
	  if (i<max_pools) {
	    pools[i]=p; toget[i]=FD_EMPTY_CHOICE; n_pools++;}
	/* Grow the tables if neccessary */
	  else if (max_pools==32) {
	    int j=0;
	    pools=u8_malloc(sizeof(fd_pool)*64);
	    toget=u8_malloc(sizeof(fdtype)*64);
	    while (j<n_pools) {
	      pools[j]=_pools[j]; toget[j]=_toget[j]; j++;}
	    max_pools=64;}
	  else {
	    pools=u8_realloc(pools,sizeof(fd_pool)*(max_pools+32));
	    toget=u8_realloc(toget,sizeof(fdtype)*(max_pools+32));
	    max_pools=max_pools+32;}
	/* Now, i is bound to the index for the pools and to gets */
	FD_ADD_TO_CHOICE(toget[i],oid);}}
    else {}}
  i=0; while (i < n_pools) {
    fd_pool_prefetch(pools[i],toget[i]);
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
  fdtype _toget[32], *toget=_toget;
  int i=0, n_pools=0, max_pools=32, total=0;
  FD_DO_CHOICES(oid,oids) {
    if (FD_OIDP(oid)) {
      fd_pool p=fd_oid2pool(oid);
      if ((p) && (fd_hashtable_probe_novoid(&(p->locks),oid)==0)) {
	i=0; while (i<n_pools) if (pools[i]==p) break; else i++;
	if (i>=n_pools)
	  /* Create a pool entry if neccessary */
	  if (i<max_pools) {
	    pools[i]=p; toget[i]=FD_EMPTY_CHOICE; n_pools++;}
	/* Grow the tables if neccessary */
	  else if (max_pools==32) {
	    int j=0;
	    pools=u8_malloc(sizeof(fd_pool)*64);
	    toget=u8_malloc(sizeof(fdtype)*64);
	    while (j<n_pools) {
	      pools[j]=_pools[j]; toget[j]=_toget[j]; j++;}
	    max_pools=64;}
	  else {
	    pools=u8_realloc(pools,sizeof(fd_pool)*(max_pools+32));
	    toget=u8_realloc(toget,sizeof(fdtype)*(max_pools+32));
	    max_pools=max_pools+32;}
	/* Now, i is bound to the index for the pools and to gets */
	FD_ADD_TO_CHOICE(toget[i],oid);}}
    else {}}
  i=0; while (i < n_pools) {
    fd_pool_lock(pools[i],toget[i]);
    total=total+FD_CHOICE_SIZE(toget[i]);
    fd_decref(toget[i]); i++;}
  return total;
}

FD_EXPORT int fd_set_oid_value(fdtype oid,fdtype value)
{
  fd_pool p=fd_oid2pool(oid);
  if (p==NULL)
    return fd_reterr(fd_AnonymousOID,"SET-OID_VALUE!",NULL,oid);
  else {
    if (fd_lock_oid(oid)) {
      fd_hashtable_store(&(p->locks),oid,value);
      return 1;}
    else return fd_reterr(fd_CantLockOID,"SET-OID_VALUE!",NULL,oid);}
}

FD_EXPORT int fd_swapout_oid(fdtype oid)
{
  fd_pool p=fd_oid2pool(oid);
  if (p==NULL)
    return fd_reterr(fd_AnonymousOID,"SET-OID_VALUE!",NULL,oid);
  else if (fd_hashtable_probe_novoid(&(p->locks),oid)) return 0;
  else if (!(fd_hashtable_probe_novoid(&(p->cache),oid))) return 0;
  else {
    fd_hashtable_store(&(p->cache),oid,FD_VOID);
    return 1;}
}

FD_EXPORT int fd_pool_lock(fd_pool p,fdtype oids)
{
  struct FD_HASHTABLE *locks=&(p->locks); int decref_oids=0;
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
    needy=fd_init_choice(oidc,n,NULL,FD_CHOICE_ISATOMIC);
    retval=p->handler->lock(p,needy);
    if (retval<0) {
      fd_decref(needy);
      return retval;}
    else if (retval) {
      fd_hashtable_iterkeys(&(p->cache),fd_table_replace,n,oidv,FD_VOID);
      fd_hashtable_iterkeys(locks,fd_table_store,n,oidv,FD_LOCKHOLDER);}
    fd_decref(needy);
    return retval;}
  else if (fd_hashtable_probe(locks,oids)==0) {
    int retval=p->handler->lock(p,oids);
    if (decref_oids) fd_decref(oids);
    if (retval<0) return retval;
    else if (retval) {
      fd_hashtable_op(&(p->cache),fd_table_replace,oids,FD_VOID);
      fd_hashtable_op(locks,fd_table_store,oids,FD_LOCKHOLDER);
      return 1;}
    else return 0;}
  else {
    if (decref_oids) fd_decref(oids);
    return 1;}
}

FD_EXPORT int fd_pool_unlock(fd_pool p,fdtype oids,int commit)
{
  struct FD_HASHTABLE *locks=&(p->locks);
  if (commit)
    if (fd_pool_commit(p,oids,0)<0) return -1;
    else {}
  if (FD_CHOICEP(oids)) {
    fdtype needy; int retval, n;
    struct FD_CHOICE *oidc=fd_alloc_choice(FD_CHOICE_SIZE(oids));
    fdtype *oidv=(fdtype *)FD_XCHOICE_DATA(oidc), *write=oidv;
    FD_DO_CHOICES(o,oids)
      if (fd_hashtable_probe_novoid(locks,o)) *write++=o;
    if (write==oidv) {
      u8_free(oidc);
      return 1;}
    else n=write-oidv;
    needy=fd_init_choice(oidc,n,NULL,FD_CHOICE_ISATOMIC);
    retval=p->handler->unlock(p,needy); 
    if (retval<0) {
      fd_decref(needy);
      return -1;}
    else if (retval) {
      fd_hashtable_iterkeys(locks,fd_table_replace,n,oidv,FD_VOID);
      if (fd_devoid_hashtable(locks)<0) {
	fd_decref(needy);
	return -1;}
    fd_decref(needy);
    return retval;}}
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
  struct FD_HASHTABLE *locks=&(p->locks);
  fdtype oids=fd_hashtable_keys(locks);
  if (FD_EMPTY_CHOICEP(oids)) return 0;
  else result=fd_pool_unlock(p,oids,commit);
  fd_decref(oids);
  return result;
}

FD_EXPORT int fd_pool_commit(fd_pool p,fdtype oids,int unlock)
{
  struct FD_HASHTABLE *cache=&(p->cache); 
  struct FD_HASHTABLE *locks=&(p->locks); 
  double start_time=u8_elapsed_time();
  init_cache_level(p);
  if (FD_CHOICEP(oids)) {
    int n_oids=FD_CHOICE_SIZE(oids), retval, n;
    struct FD_CHOICE *oidc=fd_alloc_choice(n_oids);
    fdtype *oidv=(fdtype *)FD_XCHOICE_DATA(oidc), *owrite=oidv;
    fdtype *values=u8_malloc(sizeof(fdtype)*n_oids), *vwrite=values;
    FD_DO_CHOICES(o,oids) {
      fdtype v=fd_hashtable_get(locks,o,FD_VOID);
      if (FD_VOIDP(v)) {}
      else if (FD_SLOTMAPP(v))
	if (FD_SLOTMAP_MODIFIEDP(v)) {
	  *owrite++=o; *vwrite++=v;}
	else fd_decref(v);
      else if (FD_SCHEMAPP(v))
	if (FD_SCHEMAP_MODIFIEDP(v)) {
	  *owrite++=o; *vwrite++=v;}
	else fd_decref(v);
      else if (FD_HASHTABLEP(v))
	if (FD_HASHTABLE_MODIFIEDP(v)) {
	  *owrite++=o; *vwrite++=v;}
	else fd_decref(v);
      else {*owrite++=o; *vwrite++=v;}}
    if (owrite==oidv) {
      u8_free(oidc); u8_free(values);
      return 1;}
    else n=owrite-oidv;
    retval=p->handler->storen(p,n,oidv,values);
    /* Free the values pointers, and clear the modified flags */
    {
      fdtype *scan=values;
      while (scan<vwrite){
	if (FD_SLOTMAPP(*scan)) {
	  fdtype v=*scan; FD_SLOTMAP_CLEAR_MODIFIED(v);}
	fd_decref(*scan);
	scan++;}
      u8_free(values);
    }
    if ((retval>0) && (unlock)) {
      fdtype needy=fd_init_choice(oidc,n,oidv,FD_CHOICE_ISATOMIC);
      if (p->handler->unlock(p,needy))
	fd_hashtable_iterkeys(locks,fd_table_replace,n,oidv,FD_VOID);
      fd_decref(needy);}
    else u8_free(oidc);
    if (retval<0)
      u8_warn(fd_Commitment,
		"Error saving %d OIDs from %s in %f secs",n,p->cid,
		u8_elapsed_time()-start_time);
    else u8_notify(fd_Commitment,
		   "Saved %d OIDs from %s in %f secs",n,p->cid,
		   u8_elapsed_time()-start_time);
    return retval;}
  else {
    int retcode;
    fdtype oidv[1], values[1];
    fdtype value=fd_hashtable_get(locks,oids,FD_VOID);
    if (FD_VOIDP(value)) return 1;
    oidv[0]=oids; values[0]=value;
    retcode=p->handler->storen(p,1,oidv,values);
    fd_decref(value);
    if (retcode<0) {
      u8_warn(fd_Commitment,
	      "[%*t] Error saving one OID from %s in %f secs",
	      p->cid,u8_elapsed_time()-start_time);
      return retcode;}
    else if (retcode) {
      if (unlock) {
	if (p->handler->unlock(p,oids)) 
	  fd_hashtable_op(locks,fd_table_store,oids,FD_VOID);}
      u8_notify(fd_Commitment,
		"[%*t] Saved one OID from %s in %f secs",
		 p->cid,u8_elapsed_time()-start_time);
      return 1;}
    else return 0;}
}
FD_EXPORT int fd_pool_commit_all(fd_pool p,int unlock)
{
  int result;
  struct FD_HASHTABLE *locks=&(p->locks);
  fdtype oids=fd_hashtable_keys(locks);
  result=fd_pool_commit(p,oids,unlock);
  fd_devoid_hashtable(&(p->locks));
  fd_decref(oids);
  return result;
}

FD_EXPORT int fd_pool_load(fd_pool p)
{
  if (p->handler->getload)
    return (p->handler->getload)(p);
  else return -1;
}

FD_EXPORT void fd_pool_close(fd_pool p)
{
  if ((p) && (p->handler) && (p->handler->close))
    p->handler->close(p);
}

FD_EXPORT void fd_pool_swapout(fd_pool p)
{
  if (p) fd_reset_hashtable(&(p->cache),67,1);
  if (p) fd_devoid_hashtable(&(p->locks));
}

/* Callable versions of simple functions */

FD_EXPORT fd_pool _fd_oid2pool(fdtype oid)
{
  int baseid=FD_OID_BASE_ID(oid);
  int baseoff=FD_OID_BASE_OFFSET(oid);
  struct FD_POOL *top=fd_top_pools[baseid];
  if (top==NULL) return NULL;
  else if (baseoff<top->capacity) return top;
  else if (top->capacity) {
    u8_raise(_("Corrupted pool table"),"fd_oid2pool",NULL);
    return NULL;}
  else return fd_find_subpool((struct FD_GLUEPOOL *)top,oid);
}
FD_EXPORT fdtype _fd_fetch_oid(fd_pool p,fdtype oid)
{
  fdtype value;
  if (p==NULL) return fd_anonymous_oid(oid);
  else if (p->n_locks)
    if (fd_hashtable_probe_novoid(&(p->locks),oid)) {
      value=fd_hashtable_get(&(p->locks),oid,FD_VOID);
      if (value == FD_LOCKHOLDER) {
	value=fd_pool_fetch(p,oid);
	fd_hashtable_store(&(p->locks),oid,value);
	return value;}
      else return value;}
  value=fd_hashtable_get(&(p->cache),oid,FD_VOID);
  if (FD_VOIDP(value))
    if (fd_ipeval_delay(1)) {
      FD_ADD_TO_CHOICE(fd_pool_delays[p->serialno],oid);
      return FD_EMPTY_CHOICE;}
    else return fd_pool_fetch(p,oid);
  else return value;
}
FD_EXPORT fdtype _fd_oid_value(fdtype oid)
{
  if (FD_EMPTY_CHOICEP(oid)) return oid;
  else if (FD_OIDP(oid)) {
    fd_pool p=fd_oid2pool(oid);
    return _fd_fetch_oid(p,oid);}
  else return fd_type_error(_("OID"),"_fd_oid_value",oid);
}

FD_EXPORT fdtype fd_locked_oid_value(fd_pool p,fdtype oid)
{
  fdtype smap=fd_hashtable_get(&(p->locks),oid,FD_VOID);
  if (FD_VOIDP(smap)) {
    int retval=fd_pool_lock(p,oid);
    if (retval<0) return fd_erreify();
    else if (retval) {
      fdtype v=fd_pool_fetch(p,oid);
      fd_hashtable_store(&(p->locks),oid,v);
      return v;}
    else return fd_err(fd_CantLockOID,"fd_locked_oid_value",
		       u8_strdup(p->source),oid);}
  else if (smap==FD_LOCKHOLDER) {
    fdtype v=fd_pool_fetch(p,oid);
    fd_hashtable_store(&(p->locks),oid,v);
    return v;}
  else return smap;
}

/* Pools to lisp and vice versa */

FD_EXPORT fdtype fd_pool2lisp(fd_pool p)
{
  return FDTYPE_IMMEDIATE(fd_pool_type,p->serialno);
}
FD_EXPORT fd_pool fd_lisp2pool(fdtype lp)
{
  if (FD_PTR_TYPEP(lp,fd_pool_type)) {
    int serial=FD_GET_IMMEDIATE(lp,fd_pool_type); 
    if (serial<pool_serial_count)
      return pool_serial_table[serial];
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
    else if (fd_top_pools[i]->capacity) {
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
    else if (fd_top_pools[i]->capacity) {
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
    else if (fd_top_pools[i]->capacity)
      if ((fd_top_pools[i]->cid) &&
	  ((strcmp(canonical,fd_top_pools[i]->cid)) == 0)) {
	u8_free(canonical);
	return fd_top_pools[i];}
      else i++;
    else {
      struct FD_GLUEPOOL *gp=(struct FD_GLUEPOOL *)fd_top_pools[i++];
      fd_pool *subpools=gp->subpools; int j=0;
      while (j<gp->n_subpools)
	if ((subpools[j]) &&
	    (subpools[j]->cid) &&
	    (strcmp(cid,subpools[j]->cid)==0)) {
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
    else if (fd_top_pools[i]->capacity)
      if ((fd_top_pools[i]->cid) &&
	  ((strcmp(canonical,fd_top_pools[i]->cid)) == 0)) {
	fdtype poolv=fd_pool2lisp(fd_top_pools[i]); i++;
	FD_ADD_TO_CHOICE(results,fd_incref(poolv));}
      else i++;
    else {
      struct FD_GLUEPOOL *gp=(struct FD_GLUEPOOL *)fd_top_pools[i++];
      fd_pool *subpools=gp->subpools; int j=0;
      while (j<gp->n_subpools)
	if ((subpools[j]->cid) && (strcmp(canonical,subpools[j]->cid)==0)) {
	  fdtype poolv=fd_pool2lisp(subpools[j]); j++;
	  FD_ADD_TO_CHOICE(results,fd_incref(poolv));}
	else j++;}
  u8_free(canonical);
  return results;
}

FD_EXPORT fd_pool fd_find_pool_by_prefix(u8_string prefix)
{
  int i=0;
  while (i < 1024)
    if (fd_top_pools[i] == NULL) i++;
    else if (fd_top_pools[i]->capacity)
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
      u8_warn("POOL_COMMIT_FAIL","Error when commiting pool %s",p->cid);
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

static int accumulate_cachecount(fd_pool p,void *ptr)
{
  int *count=(int *)ptr;
  *count=*count+p->cache.n_keys;
  return 0;
}

FD_EXPORT
int fd_cachecount_pools()
{
  int result=0, retval;
  retval=fd_for_pools(accumulate_cachecount,(void *)&result);
  if (retval<0) return retval;
  else return result;
}

static int accumulate_cached(fd_pool p,void *ptr)
{
  fdtype *vals=(fdtype *)ptr;
  FD_ADD_TO_CHOICE(*vals,fd_hashtable_keys(&(p->cache)));
  return 0;
}

FD_EXPORT
fdtype fd_cached_oids(fd_pool p)
{
  if (p==NULL) {
    int retval; fdtype result=FD_EMPTY_CHOICE;
    fd_for_pools(accumulate_cached,(void *)&result);
    if (retval<0) {
      fd_decref(result);
      return fd_erreify();}
    else return result;}
  else return fd_hashtable_keys(&(p->cache));
}

/* Common pool initialization stuff */

FD_EXPORT void fd_init_pool(fd_pool p,FD_OID base,unsigned int capacity,
			    struct FD_POOL_HANDLER *h,
			    u8_string source,u8_string cid)
{
  p->base=base; p->capacity=capacity;
  p->serialno=-1; p->cache_level=-1; p->read_only=1; p->flags=0;
  fd_make_hashtable(&(p->cache),64,NULL);
  fd_make_hashtable(&(p->locks),0,NULL); 
  p->n_adjuncts=0; p->adjuncts=NULL;
  p->n_locks=0;
  p->handler=h;
  p->source=u8_strdup(source); p->cid=u8_strdup(cid); p->xid=NULL;
  p->label=NULL; p->prefix=NULL;
}

/* GLUEPOOL handler (empty) */

static struct FD_POOL_HANDLER gluepool_handler={
  "gluepool", 1, sizeof(struct FD_GLUEPOOL), 11,
  NULL, /* getmetadata */
  NULL, /* fetch */
  NULL, /* store */
  NULL, /* alloc */
  NULL, /* fetchn */
  NULL, /* storen */
  NULL, /* lock */
  NULL /* release */
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
  fdtype label_string=fd_init_string(NULL,-1,u8_strdup(spec));
  fdtype poolv=fd_hashtable_get(&poolid_table,label_string,FD_VOID);
  fd_decref(label_string);
  if (FD_VOIDP(poolv)) return NULL;
  else return fd_lisp2pool(poolv);
}

static int unparse_pool(u8_output out,fdtype x)
{
  fd_pool p=fd_lisp2pool(x);
  if (p==NULL) return 0;
  else if (p->label)
    if (p->xid)
      u8_printf(out,"#<POOL 0x%lx \"%s\" \"%s|%s\">",
		x,p->label,p->source,p->xid);
    else u8_printf(out,"#<POOL 0x%lx \"%s\" \"%s\">",x,p->label,p->source);
  else if (p->source)
    if (p->xid)
      u8_printf(out,"#<POOL 0x%lx \"%s|%s\">",x,p->source,p->xid);
    else u8_printf(out,"#<POOL 0x%lx \"%s\">",x,p->source);
  else u8_printf(out,"#<POOL 0x%lx>",x);  
  return 1;
}

static fdtype pool_parsefn(FD_MEMORY_POOL_TYPE *pool,int n,fdtype *args)
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

static fdtype config_set_anonymousok(fdtype var,fdtype val,void *data)
{
  if (FD_TRUEP(val)) fd_ignore_anonymous_oids=1;
  else fd_ignore_anonymous_oids=0;
  return 1;
}

/* Executing pool delays */

FD_EXPORT int fd_execute_pool_delays(fd_pool p,void *data)
{
  fdtype todo=fd_pool_delays[p->serialno];
  if (FD_EMPTY_CHOICEP(todo)) return 0;
  else {
    /* fd_lock_mutex(&(fd_ipeval_lock)); */
    todo=fd_pool_delays[p->serialno];
    fd_pool_delays[p->serialno]=FD_EMPTY_CHOICE;
    todo=fd_simplify_choice(todo);
    /* fd_unlock_mutex(&(fd_ipeval_lock)); */
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval>1)
      u8_notify(ipeval_objfetch,"Fetching %d oids from %s: %q",
		FD_CHOICE_SIZE(todo),p->cid,todo);
    else if (fd_trace_ipeval)
      u8_notify(ipeval_objfetch,"Fetching %d oids from %s",
		FD_CHOICE_SIZE(todo),p->cid);
#endif
    fd_pool_prefetch(p,todo);
    return 0;}
}

/* Initialize */

fd_ptr_type fd_pool_type;

static int check_pool(fdtype x)
{
  int serial=FD_GET_IMMEDIATE(x,fd_pool_type); 
  if (serial<0) return 0;
  else if (serial<pool_serial_count) return 1;
  else return 0;
}

FD_EXPORT fd_init_pools_c()
{
  int i=0; while (i < 1024) fd_top_pools[i++]=NULL;

  fd_register_source_file(versionid);

  fd_pool_type=fd_register_immediate_type("pool",check_pool);

  {
    struct FD_COMPOUND_ENTRY *e=fd_register_compound(fd_intern("POOL"));
    e->parser=pool_parsefn;}

  fd_make_hashtable(&poolid_table,32,NULL);
#if ((FD_USE_TLS) && (!(FD_GLOBAL_IPEVAL)))
  u8_new_threadkey(&fd_pool_delays_key,NULL);
#endif
  fd_unparsers[fd_pool_type]=unparse_pool;
  fd_register_config("ANONYMOUSOK",
		     config_get_anonymousok,
		     config_set_anonymousok,NULL);
}


/* The CVS log for this file
   $Log: pools.c,v $
   Revision 1.113  2006/03/14 04:30:19  haase
   Signal error when a pool is exhausted

   Revision 1.112  2006/02/09 02:28:11  haase
   Fixed LOCKHOLDER bug

   Revision 1.111  2006/01/31 13:47:23  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.110  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.109  2006/01/20 04:08:26  haase
   Fixed leak in looking up pools by canonical ID

   Revision 1.108  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.107  2006/01/07 14:01:13  haase
   Fixed some leaks

   Revision 1.106  2006/01/05 18:20:33  haase
   Added missing return keyword

   Revision 1.105  2005/12/28 23:03:29  haase
   Made choices be direct blocks of elements, including various fixes, simplifications, and more detailed documentation.

   Revision 1.104  2005/12/26 20:14:26  haase
   Further fixes to type reorganization

   Revision 1.103  2005/11/08 15:22:29  haase
   Made (COMMIT) ignore (but warn) on individual errors for now

   Revision 1.102  2005/10/13 16:02:40  haase
   Fixed bad typo in pool registration

   Revision 1.101  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.100  2005/07/25 19:39:22  haase
   Made pool commits use the modified flag on hashtable values

   Revision 1.99  2005/07/16 15:15:17  haase
   Fixed another binary search bug for pools

   Revision 1.98  2005/07/13 21:24:05  haase
   Added readonly schemaps and slotmaps and corresponding optimizations

   Revision 1.97  2005/07/12 01:51:38  haase
   Fixed buggy interaction between locking (modification) and prefetching

   Revision 1.96  2005/07/11 20:30:15  haase
   Fixed binary sort bugs

   Revision 1.95  2005/07/09 23:33:13  haase
   Fixed subpool registration bug

   Revision 1.94  2005/06/22 23:19:45  haase
   Fixed repeated time statements in commitment reports

   Revision 1.93  2005/06/15 14:39:45  haase
   Fixed return value problem in fd_for_pools (leading to bad results from fd_cachecount_pools)

   Revision 1.92  2005/06/04 12:44:09  haase
   Fixed error catching for prefetches

   Revision 1.91  2005/06/01 13:07:55  haase
   Fixes for less forgiving compilers

   Revision 1.90  2005/05/31 14:30:59  haase
   Fix bug in subpool lookup

   Revision 1.89  2005/05/30 00:03:54  haase
   Fixes to pool declaration, allowing the USE-POOL primitive to return multiple pools correctly when given a ; spearated list or a pool server which provides multiple pools

   Revision 1.88  2005/05/29 22:58:48  haase
   Simplified pool lookup and added choice returning fd_find_pools_by_cid

   Revision 1.87  2005/05/29 22:38:47  haase
   Simplified db layer fd_use_pool and fd_use_index

   Revision 1.86  2005/05/27 20:39:53  haase
   Added OID swapout

   Revision 1.85  2005/05/26 20:48:42  haase
   Made USE-POOL return a pool, rather than void, again

   Revision 1.84  2005/05/26 12:41:11  haase
   More return value error handling fixes

   Revision 1.83  2005/05/26 11:03:21  haase
   Fixed some bugs with read-only pools and indices

   Revision 1.82  2005/05/23 00:53:24  haase
   Fixes to header ordering to get off_t consistently defined

   Revision 1.81  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.80  2005/05/17 22:09:24  haase
   Added commit reports

   Revision 1.79  2005/05/17 18:41:27  haase
   Fixed bug in oid commitment and unlocking

   Revision 1.78  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.77  2005/04/25 13:11:34  haase
   Added cache size counting

   Revision 1.76  2005/04/24 01:48:34  haase
   Check oids against cache when prefetching

   Revision 1.75  2005/04/16 20:07:49  haase
   Fixed ANONYMOUSOK set config to return a success value

   Revision 1.74  2005/04/16 19:24:01  haase
   Added CONFIG variable ANONYMOUSOK and changed  to anonymous in flags and functions

   Revision 1.73  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.72  2005/04/09 22:35:37  haase
   Make pool and index unparsing decline for invalid values

   Revision 1.71  2005/03/30 15:30:00  haase
   Made calls to new seterr do appropriate strdups

   Revision 1.70  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.69  2005/03/28 19:19:36  haase
   Added metadata reading and writing and file pool/index creation

   Revision 1.68  2005/03/26 19:09:13  haase
   Made index and pool commits set the cache level

   Revision 1.67  2005/03/26 18:31:41  haase
   Various configuration fixes

   Revision 1.66  2005/03/25 19:49:47  haase
   Removed base library for eframerd, deferring to libu8

   Revision 1.65  2005/03/23 21:47:29  haase
   Added return values for use pool/index primitives and added CONFIG interface to pool and index usage

   Revision 1.64  2005/03/22 18:25:01  haase
   Added quasiquote

   Revision 1.63  2005/03/14 05:49:31  haase
   Updated comments and internal documentation

   Revision 1.62  2005/03/07 14:18:19  haase
   Moved lock counting into pool handlers and made swapout devoid the locks table

   Revision 1.61  2005/03/06 18:28:21  haase
   Added timeprims

   Revision 1.60  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.59  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.58  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.57  2005/03/03 17:58:15  haase
   Moved stdio dependencies out of fddb and reorganized make structure

   Revision 1.56  2005/02/28 02:41:44  haase
   Addded config procedures

   Revision 1.55  2005/02/26 22:31:04  haase
   Fixed pool commit bug

   Revision 1.54  2005/02/26 21:41:31  haase
   Fixed n_locks decrement for single oid unlock

   Revision 1.53  2005/02/26 21:38:32  haase
   Cleaned up handling of locks and oid values

   Revision 1.52  2005/02/25 20:13:22  haase
   Fixed incref and decref references for double evaluation

   Revision 1.51  2005/02/25 19:23:32  haase
   Fixes to pool locking and commitment

   Revision 1.50  2005/02/24 19:12:46  haase
   Fixes to handling index arguments which are strings specifiying index sources

   Revision 1.49  2005/02/23 22:49:27  haase
   Created generalized compound registry and made compound dtypes and #< reading use it

   Revision 1.48  2005/02/22 21:31:48  haase
   Add immediate checkers for pools and indices

   Revision 1.47  2005/02/21 22:36:11  haase
   Fixes to record reading implementation

   Revision 1.46  2005/02/21 22:13:08  haase
   Added readable record printing

   Revision 1.45  2005/02/19 19:30:17  haase
   Fix OID prefixes to be case-insensitive

   Revision 1.44  2005/02/19 16:24:33  haase
   Added fd_set_oid_value

   Revision 1.43  2005/02/15 15:17:40  haase
   Added automatic handling of .pool and .index files

   Revision 1.42  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.41  2005/02/11 04:46:01  haase
   Made prefetching use _noref hashtable ops

   Revision 1.40  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
