!/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_BUFIO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>

#include <stdarg.h>

/*
  Thoughts on odd pools.
  The most general kind of pool has applicable lisp object
  for all the pool handler functions;
  Another interesting kind of pool is able to be fetched
  but manages storage in other ways.  This is good for a case
  where the OIDs represent objects in an external SQL database
  (like WebEchoes pings).  Note that this kind of pool doesn't
  really have an alloc function of its own, since creation happens
  on the server side.
  Another interesting variant is a memory-only pool, where
  you can create and modify frames but they are limited to
  the current memory image.  This is what mempools are.

*/

static struct KNO_POOL_HANDLER gpool_handler;

KNO_EXPORT
kno_pool kno_make_gpool(KNO_OID base,int cap,u8_string id,
                      lispval fetchfn,lispval loadfn,
                      lispval allocfn,lispval savefn,
                      lispval lockfn,lispval state)
{
  struct KNO_GPOOL *gp = u8_alloc(struct KNO_GPOOL);
  lispval loadval = kno_apply(loadfn,0,NULL); unsigned int load;
  if (!(FIXNUMP(loadval)))
    return kno_type_error("kno_make_gpool","pool load (fixnum)",loadval);
  else load = FIX2INT(loadval);
  kno_init_pool((kno_pool)gp,base,cap,
               &gpool_handler,
               id,id,id,
               KNO_STORAGE_ISPOOL,KNO_FALSE,KNO_FALSE);
  gp->pool_load = load;
  kno_incref(fetchfn); kno_incref(loadfn);
  kno_incref(allocfn); kno_incref(savefn);
  kno_incref(lockfn); kno_incref(state);
  gp->fetchfn = fetchfn; gp->pool_loadfn = loadfn;
  gp->allocfn = allocfn;  gp->savefn = savefn;
  gp->lockfn = lockfn; gp->state = state;
  return gp;
}

static int gpool_load(kno_pool p)
{
  struct KNO_GPOOL *np = (struct KNO_GPOOL *)p;
  lispval value;
  value = kno_dtcall(np->pool_connpool,2,get_load_symbol,kno_make_oid(p->pool_base));
  if (FIXNUMP(value)) return FIX2INT(value);
  else if (KNO_ABORTP(value))
    return kno_interr(value);
  else return KNO_ERR(-1,kno_BadServerResponse,"POOL-LOAD",NULL,value);
}

static lispval gpool_fetch(kno_pool p,lispval oid)
{
  struct KNO_GPOOL *np = (struct KNO_GPOOL *)p;
  lispval value;
  value = kno_dtcall(np->pool_connpool,2,oid_value_symbol,oid);
  return value;
}

static lispval *gpool_fetchn(kno_pool p,int n,lispval *oids)
{
  struct KNO_GPOOL *np = (struct KNO_GPOOL *)p;
  lispval vector = kno_wrap_vector(n,oids);
  lispval value = kno_dtcall(np->pool_connpool,2,fetch_oids_symbol,vector);
  kno_decref(vector);
  if (VECTORP(value)) {
    struct KNO_VECTOR *vstruct = (struct KNO_VECTOR)value;
    lispval *results = u8_alloc_n(n,lispval);
    memcpy(results,vstruct->vec_elts,LISPVEC_BYTELEN(n));
    /* Free the CONS itself (and maybe data), to avoid DECREF/INCREF
       of values. */
    if (vstruct->str_freebytes) u8_free(vstruct->vec_elts);
    u8_free((struct KNO_CONS *)value);
    return results;}
  else return KNO_ERR(NULL,kno_BadServerResponse,"netpool_fetchn",
                      np->poolid,kno_incref(value));
}

static int gpool_lock(kno_pool p,lispval oid)
{
  struct KNO_GPOOL *np = (struct KNO_GPOOL *)p;
  lispval value;
  value = kno_dtcall(np->pool_connpool,3,lock_oid_symbol,oid,client_id);
  if (VOIDP(value)) return 0;
  else if (KNO_ABORTP(value))
    return kno_interr(value);
  else {
    kno_hashtable_store(&(p->index_cache),oid,value);
    kno_decref(value);
    return 1;}
}

static int gpool_unlock(kno_pool p,lispval oids)
{
  struct KNO_GPOOL *np = (struct KNO_GPOOL *)p;
  lispval result;
  result = kno_dtcall(np->pool_connpool,3,clear_oid_lock_symbol,oids,client_id);
  if (KNO_ABORTP(result)) {
    kno_decref(result); return 0;}
  else {kno_decref(result); return 1;}
}

static int gpool_storen(kno_pool p,int n,lispval *oids,lispval *values)
{
  struct KNO_GPOOL *np = (struct KNO_GPOOL *)p;
  if (np->bulk_commitp) {
    int i = 0;
    lispval *storevec = u8_alloc_n(n*2,lispval), vec, result;
    while (i < n) {
      storevec[i*2]=oids[i];
      storevec[i*2+1]=values[i];
      i++;}
    vec = kno_wrap_vector(n*2,storevec);
    result = kno_dtcall(np->pool_connpool,3,bulk_commit_symbol,client_id,vec);
    /* Don't decref the individual elements because you didn't incref them. */
    u8_free((struct KNO_CONS *)vec); u8_free(storevec);
    return 1;}
  else {
    int i = 0;
    while (i < n) {
      lispval result = kno_dtcall(np->pool_connpool,4,unlock_oid_symbol,oids[i],client_id,values[i]);
      kno_decref(result); i++;}
    return 1;}
}

static lispval gpool_alloc(kno_pool p,int n)
{
  lispval results = EMPTY, request; int i = 0;
  struct KNO_GPOOL *np = (struct KNO_GPOOL *)p;
  request = kno_conspair(new_oid_symbol,NIL);
  while (i < n) {
    lispval result = kno_dteval(np->pool_connpool,request);
    CHOICE_ADD(results,result);
    i++;}
  return results;
}

static struct KNO_POOL_HANDLER gpool_handler={
  "gpool", 1, sizeof(struct KNO_GPOOL), 12,
  NULL, /* close */
  gpool_alloc, /* alloc */
  gpool_fetch, /* fetch */
  gpool_fetchn, /* fetchn */
  gpool_load, /* getload */
  gpool_lock, /* lock */
  gpool_unlock, /* release */
  gpool_storen, /* storen */
  NULL, /* swapout */
  NULL}; /* sync */

KNO_EXPORT void kno_init_gpools_c()
{
  u8_register_source_file(_FILEINFO);

}

