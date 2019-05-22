/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_POOLS 1
#define KNO_INLINE_BUFIO 1
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

#include <libu8/u8printf.h>

/* Mem pools */

static struct KNO_POOL_HANDLER mempool_handler;

KNO_EXPORT kno_pool kno_make_mempool(u8_string label,KNO_OID base,
                                  unsigned int cap,unsigned int load,
                                  unsigned int noswap,
                                  lispval opts)
{
  kno_pool p=kno_get_mempool(label);
  if (p) {
    if ( base != (p->pool_base) ) {
      u8_byte buf[128];
      kno_seterr(kno_PoolConflict,"kno_make_mempool",
                u8_sprintf(buf,128,
                           "Existing '%s' mempool doesn't have a base of @%x/%x",
                           label,KNO_OID_HI(base),KNO_OID_LO(base)),
                kno_pool2lisp(p));
      return NULL;}
    else if ( cap != (p->pool_capacity) ) {
      u8_byte buf[128];
      kno_seterr(kno_PoolConflict,"kno_make_mempool",
                u8_sprintf(buf,128,
                           "Existing '%s' mempool doesn't have a capacity of @%d OIDs",
                           label,cap),
                kno_pool2lisp(p));
      return NULL;}
    else return p;}
  else if (load>cap) {
    u8_seterr(kno_PoolOverflow,"make_mempool",
              u8_sprintf(NULL,256,
                         "Specified load (%u) > capacity (%u) for '%s'",
                         load,cap,label));
    return NULL;}
  else {
    struct KNO_MEMPOOL *mp = u8_alloc(struct KNO_MEMPOOL);
    kno_init_pool((kno_pool)mp,base,cap,
                 &mempool_handler,
                 label,label,label,
                 KNO_STORAGE_ISPOOL,KNO_VOID,opts);
    mp->pool_label = u8_strdup(label);
    mp->pool_load = load; mp->noswap = noswap;
    u8_init_rwlock(&(mp->pool_struct_lock));
    mp->pool_flags = KNO_STORAGE_ISPOOL;
    if (kno_register_pool((kno_pool)mp)<0) {
      u8_destroy_rwlock(&(mp->pool_struct_lock));
      u8_free(mp->pool_source); u8_free(mp->poolid);
      kno_recycle_hashtable(&(mp->pool_cache));
      kno_recycle_hashtable(&(mp->pool_changes));
      u8_free(mp);
      return NULL;}
    else return (kno_pool)mp;}
}

struct GET_MEMPOOL_STATE {
  kno_pool pool; u8_string label;};

static int get_mempool_helper(kno_pool p,void *data)
{
  struct GET_MEMPOOL_STATE *state=(struct GET_MEMPOOL_STATE *) data;
  if ((p->pool_handler == &mempool_handler) &&
      (strcmp(p->pool_label,state->label)==0)) {
    state->pool=p;
    return 1;}
  else return 0;
}

KNO_EXPORT kno_pool kno_get_mempool(u8_string label)
{
  struct GET_MEMPOOL_STATE state;
  state.pool=NULL; state.label=label;
  kno_for_pools(get_mempool_helper,&state);
  return state.pool;
}

static lispval mempool_alloc(kno_pool p,int n)
{
  struct KNO_MEMPOOL *mp = (kno_mempool)p;
  if ((mp->pool_load+n)>=mp->pool_capacity)
    return kno_err(kno_ExhaustedPool,"mempool_alloc",mp->poolid,VOID);
  else {
    lispval results = EMPTY;
    int i = 0;
    kno_lock_pool_struct(p,1);
    if ((mp->pool_load+n)>=mp->pool_capacity) {
      kno_unlock_pool_struct(p);
      return kno_err(kno_ExhaustedPool,"mempool_alloc",mp->poolid,VOID);}
    else {
      KNO_OID base = KNO_OID_PLUS(mp->pool_base,mp->pool_load);
      while (i<n) {
        KNO_OID each = KNO_OID_PLUS(base,i);
        CHOICE_ADD(results,kno_make_oid(each));
        i++;}
      mp->pool_load = mp->pool_load+n;
      kno_unlock_pool_struct(p);
      return kno_simplify_choice(results);}}
}

static lispval mempool_fetch(kno_pool p,lispval oid)
{
  struct KNO_MEMPOOL *mp = (kno_mempool)p;
  struct KNO_HASHTABLE *cache = &(p->pool_cache);
  KNO_OID addr = KNO_OID_ADDR(oid);
  int off = KNO_OID_DIFFERENCE(addr,mp->pool_base);
  if (off>mp->pool_load)
    return KNO_UNALLOCATED_OID;
  else return kno_hashtable_get(cache,oid,EMPTY);
}

static lispval *mempool_fetchn(kno_pool p,int n,lispval *oids)
{
  struct KNO_HASHTABLE *cache = &(p->pool_cache);
  struct KNO_MEMPOOL *mp = (kno_mempool)p;
  unsigned int load = mp->pool_load;
  lispval *results=u8_alloc_n(n,lispval);
  int i = 0; while (i<n) {
    KNO_OID addr = KNO_OID_ADDR(oids[i]);
    int off = KNO_OID_DIFFERENCE(addr,mp->pool_base);
    if (off>load)
      results[i]=KNO_UNALLOCATED_OID;
    else results[i]=kno_hashtable_get(cache,oids[i],EMPTY);
    i++;}
  return results;
}

static int mempool_load(kno_pool p)
{
  struct KNO_MEMPOOL *mp = (kno_mempool)p;
  return mp->pool_load;
}

static int mempool_commit(kno_pool p,kno_commit_phase phase,
                          struct KNO_POOL_COMMITS *commits)
{
  return 1;
}

static int mempool_swapout(kno_pool p,lispval oidvals)
{
  struct KNO_MEMPOOL *mp = (kno_mempool)p;
  if (mp->noswap) return 0;
  else if (VOIDP(oidvals)) {
    kno_reset_hashtable(&(p->pool_changes),kno_pool_lock_init,1);
    return 1;}
  else {
    lispval oids = kno_make_simple_choice(oidvals);
    if (EMPTYP(oids)) {}
    else if (OIDP(oids)) {
      kno_hashtable_op(&(p->pool_changes),kno_table_replace,oids,VOID);}
    else {
      kno_hashtable_iterkeys
        (&(p->pool_changes),kno_table_replace,
         KNO_CHOICE_SIZE(oids),KNO_CHOICE_DATA(oids),VOID);
      kno_devoid_hashtable(&(p->pool_changes),0);}
    kno_decref(oids);
    return 1;}
}

static struct KNO_POOL_HANDLER mempool_handler={
  "mempool", 1, sizeof(struct KNO_MEMPOOL), 12,
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

KNO_EXPORT int kno_clean_mempool(kno_pool p)
{
  if (p->pool_handler!= &mempool_handler)
    return kno_reterr
      (kno_TypeError,"kno_clean_mempool",
       _("mempool"),kno_pool2lisp(p));
  else {
    kno_remove_deadwood(&(p->pool_changes),NULL,NULL);
    kno_devoid_hashtable(&(p->pool_changes),0);
    kno_remove_deadwood(&(p->pool_cache),NULL,NULL);
    kno_devoid_hashtable(&(p->pool_cache),0);
    return p->pool_cache.table_n_keys+p->pool_changes.table_n_keys;}
}

KNO_EXPORT int kno_reset_mempool(kno_pool p)
{
  if (p->pool_handler!= &mempool_handler)
    return kno_reterr
      (kno_TypeError,"kno_clean_mempool",
       _("mempool"),kno_pool2lisp(p));
  else {
    struct KNO_MEMPOOL *mp = (struct KNO_MEMPOOL *)p;
    kno_lock_pool_struct(p,1);
    kno_reset_hashtable(&(p->pool_changes),-1,1);
    kno_reset_hashtable(&(p->pool_cache),-1,1);
    kno_unlock_pool_struct(p);
    mp->pool_load = 0;
    return 0;}
}

KNO_EXPORT void kno_init_mempool_c()
{
  u8_register_source_file(_FILEINFO);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
