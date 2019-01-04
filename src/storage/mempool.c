/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_POOLS 1
#define FD_INLINE_BUFIO 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include <libu8/u8printf.h>

/* Mem pools */

static struct FD_POOL_HANDLER mempool_handler;

FD_EXPORT fd_pool fd_make_mempool(u8_string label,FD_OID base,
                                  unsigned int cap,unsigned int load,
                                  unsigned int noswap,
                                  lispval opts)
{
  fd_pool p=fd_get_mempool(label);
  if (p) {
    if ( base != (p->pool_base) ) {
      u8_byte buf[128];
      fd_seterr(fd_PoolConflict,"fd_make_mempool",
                u8_sprintf(buf,128,
                           "Existing '%s' mempool doesn't have a base of @%x/%x",
                           label,FD_OID_HI(base),FD_OID_LO(base)),
                fd_pool2lisp(p));
      return NULL;}
    else if ( cap != (p->pool_capacity) ) {
      u8_byte buf[128];
      fd_seterr(fd_PoolConflict,"fd_make_mempool",
                u8_sprintf(buf,128,
                           "Existing '%s' mempool doesn't have a capacity of @%d OIDs",
                           label,cap),
                fd_pool2lisp(p));
      return NULL;}
    else return p;}
  else if (load>cap) {
    u8_seterr(fd_PoolOverflow,"make_mempool",
              u8_sprintf(NULL,256,
                         "Specified load (%u) > capacity (%u) for '%s'",
                         load,cap,label));
    return NULL;}
  else {
    struct FD_MEMPOOL *mp = u8_alloc(struct FD_MEMPOOL);
    fd_init_pool((fd_pool)mp,base,cap,
                 &mempool_handler,
                 label,label,label,
                 FD_STORAGE_ISPOOL,FD_VOID,opts);
    mp->pool_label = u8_strdup(label);
    mp->pool_load = load; mp->noswap = noswap;
    u8_init_rwlock(&(mp->pool_struct_lock));
    mp->pool_flags = FD_STORAGE_ISPOOL;
    if (fd_register_pool((fd_pool)mp)<0) {
      u8_destroy_rwlock(&(mp->pool_struct_lock));
      u8_free(mp->pool_source); u8_free(mp->poolid);
      fd_recycle_hashtable(&(mp->pool_cache));
      fd_recycle_hashtable(&(mp->pool_changes));
      u8_free(mp);
      return NULL;}
    else return (fd_pool)mp;}
}

struct GET_MEMPOOL_STATE {
  fd_pool pool; u8_string label;};

static int get_mempool_helper(fd_pool p,void *data)
{
  struct GET_MEMPOOL_STATE *state=(struct GET_MEMPOOL_STATE *) data;
  if ((p->pool_handler == &mempool_handler) &&
      (strcmp(p->pool_label,state->label)==0)) {
    state->pool=p;
    return 1;}
  else return 0;
}

FD_EXPORT fd_pool fd_get_mempool(u8_string label)
{
  struct GET_MEMPOOL_STATE state;
  state.pool=NULL; state.label=label;
  fd_for_pools(get_mempool_helper,&state);
  return state.pool;
}

static lispval mempool_alloc(fd_pool p,int n)
{
  struct FD_MEMPOOL *mp = (fd_mempool)p;
  if ((mp->pool_load+n)>=mp->pool_capacity)
    return fd_err(fd_ExhaustedPool,"mempool_alloc",mp->poolid,VOID);
  else {
    lispval results = EMPTY;
    int i = 0;
    fd_lock_pool_struct(p,1);
    if ((mp->pool_load+n)>=mp->pool_capacity) {
      fd_unlock_pool_struct(p);
      return fd_err(fd_ExhaustedPool,"mempool_alloc",mp->poolid,VOID);}
    else {
      FD_OID base = FD_OID_PLUS(mp->pool_base,mp->pool_load);
      while (i<n) {
        FD_OID each = FD_OID_PLUS(base,i);
        CHOICE_ADD(results,fd_make_oid(each));
        i++;}
      mp->pool_load = mp->pool_load+n;
      fd_unlock_pool_struct(p);
      return fd_simplify_choice(results);}}
}

static lispval mempool_fetch(fd_pool p,lispval oid)
{
  struct FD_MEMPOOL *mp = (fd_mempool)p;
  struct FD_HASHTABLE *cache = &(p->pool_cache);
  FD_OID addr = FD_OID_ADDR(oid);
  int off = FD_OID_DIFFERENCE(addr,mp->pool_base);
  if (off>mp->pool_load)
    return FD_UNALLOCATED_OID;
  else return fd_hashtable_get(cache,oid,EMPTY);
}

static lispval *mempool_fetchn(fd_pool p,int n,lispval *oids)
{
  struct FD_HASHTABLE *cache = &(p->pool_cache);
  struct FD_MEMPOOL *mp = (fd_mempool)p;
  unsigned int load = mp->pool_load;
  lispval *results=u8_alloc_n(n,lispval);
  int i = 0; while (i<n) {
    FD_OID addr = FD_OID_ADDR(oids[i]);
    int off = FD_OID_DIFFERENCE(addr,mp->pool_base);
    if (off>load)
      results[i]=FD_UNALLOCATED_OID;
    else results[i]=fd_hashtable_get(cache,oids[i],EMPTY);
    i++;}
  return results;
}

static int mempool_load(fd_pool p)
{
  struct FD_MEMPOOL *mp = (fd_mempool)p;
  return mp->pool_load;
}

static int mempool_commit(fd_pool p,fd_commit_phase phase,
                          struct FD_POOL_COMMITS *commits)
{
  return 1;
}

static int mempool_swapout(fd_pool p,lispval oidvals)
{
  struct FD_MEMPOOL *mp = (fd_mempool)p;
  if (mp->noswap) return 0;
  else if (VOIDP(oidvals)) {
    fd_reset_hashtable(&(p->pool_changes),fd_pool_lock_init,1);
    return 1;}
  else {
    lispval oids = fd_make_simple_choice(oidvals);
    if (EMPTYP(oids)) {}
    else if (OIDP(oids)) {
      fd_hashtable_op(&(p->pool_changes),fd_table_replace,oids,VOID);}
    else {
      fd_hashtable_iterkeys
        (&(p->pool_changes),fd_table_replace,
         FD_CHOICE_SIZE(oids),FD_CHOICE_DATA(oids),VOID);
      fd_devoid_hashtable(&(p->pool_changes),0);}
    fd_decref(oids);
    return 1;}
}

static struct FD_POOL_HANDLER mempool_handler={
  "mempool", 1, sizeof(struct FD_MEMPOOL), 12,
  NULL, /* close */
  mempool_alloc, /* alloc */
  mempool_fetch, /* fetch */
  mempool_fetchn, /* fetchn */
  mempool_load, /* getload */
  NULL, /* lock (mempool_lock) */
  NULL, /* release (mempool_unlock) */
  mempool_commit, /* commit */
  mempool_swapout, /* swapout */
  NULL, /* create */
  NULL,  /* walk */
  NULL, /* recycle */
  NULL  /* poolctl */
};

FD_EXPORT int fd_clean_mempool(fd_pool p)
{
  if (p->pool_handler!= &mempool_handler)
    return fd_reterr
      (fd_TypeError,"fd_clean_mempool",
       _("mempool"),fd_pool2lisp(p));
  else {
    fd_remove_deadwood(&(p->pool_changes),NULL,NULL);
    fd_devoid_hashtable(&(p->pool_changes),0);
    fd_remove_deadwood(&(p->pool_cache),NULL,NULL);
    fd_devoid_hashtable(&(p->pool_cache),0);
    return p->pool_cache.table_n_keys+p->pool_changes.table_n_keys;}
}

FD_EXPORT int fd_reset_mempool(fd_pool p)
{
  if (p->pool_handler!= &mempool_handler)
    return fd_reterr
      (fd_TypeError,"fd_clean_mempool",
       _("mempool"),fd_pool2lisp(p));
  else {
    struct FD_MEMPOOL *mp = (struct FD_MEMPOOL *)p;
    fd_lock_pool_struct(p,1);
    fd_reset_hashtable(&(p->pool_changes),-1,1);
    fd_reset_hashtable(&(p->pool_cache),-1,1);
    fd_unlock_pool_struct(p);
    mp->pool_load = 0;
    return 0;}
}

FD_EXPORT void fd_init_mempool_c()
{
  u8_register_source_file(_FILEINFO);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
