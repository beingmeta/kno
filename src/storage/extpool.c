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
#include "kno/apply.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

lispval lock_symbol, unlock_symbol;

/* Fetch pools */

/* Fetch pools are pools which keep their data externally and take care
   of their own data management.  They basically have a fetch function and
   a load function but may be extended to be clever about saving in some way. */

KNO_EXPORT
kno_pool kno_make_extpool(u8_string label,
                        KNO_OID base,unsigned int cap,
                        lispval fetchfn,lispval savefn,
                        lispval lockfn,lispval allocfn,
                        lispval state,
                        lispval opts)
{
  if (!(PRED_TRUE(KNO_APPLICABLEP(fetchfn))))
    return KNO_ERR(NULL,kno_TypeError,"kno_make_extpool","fetch function",fetchfn);
  else if (!(KNO_ISFUNARG(savefn)))
    return KNO_ERR(NULL,kno_TypeError,"kno_make_extpool","save function",savefn);
  else if (!(KNO_ISFUNARG(lockfn)))
    return KNO_ERR(NULL,kno_TypeError,"kno_make_extpool","lock function",lockfn);
  else if (!(KNO_ISFUNARG(allocfn)))
    return KNO_ERR(NULL,kno_TypeError,"kno_make_extpool","alloc function",allocfn);
  else {
    struct KNO_EXTPOOL *xp = u8_alloc(struct KNO_EXTPOOL);
    kno_storage_flags flags = KNO_STORAGE_ISPOOL | KNO_POOL_VIRTUAL;
    if (VOIDP(savefn)) flags |= KNO_STORAGE_READ_ONLY;
    memset(xp,0,sizeof(struct KNO_EXTPOOL));
    kno_init_pool((kno_pool)xp,base,cap,&kno_extpool_handler,
                 label,label,label,flags,KNO_VOID,opts);
    /* We currently use the OID value as a cache for stored values, so
       we don't make it read-only. But there's probably a better
       solution. */
    kno_register_pool((kno_pool)xp);
    kno_incref(fetchfn);
    kno_incref(savefn);
    kno_incref(lockfn);
    kno_incref(allocfn);
    kno_incref(state);
    xp->fetchfn = fetchfn;
    xp->savefn = savefn;
    xp->lockfn = lockfn;
    xp->allocfn = allocfn;
    xp->state = state;
    xp->pool_label = u8_strdup(label);
    xp->pool_flags = xp->pool_flags|KNO_POOL_SPARSE;

    lispval metadata = kno_getopt(opts,KNOSYM_METADATA,KNO_VOID);
    if (KNO_SLOTMAPP(metadata))
      kno_copy_slotmap((kno_slotmap)metadata,&(xp->pool_metadata));
    kno_decref(metadata);

    return (kno_pool)xp;}
}

static lispval extpool_fetch(kno_pool p,lispval oid)
{
  struct KNO_EXTPOOL *xp = (kno_extpool)p;
  lispval state = xp->state, value, fetchfn = xp->fetchfn;
  int arity = KNO_FUNCTION_ARITY(fetchfn);
  if ( (VOIDP(state)) || (FALSEP(state)) || (arity == 1) )
    value = kno_apply(fetchfn,1,&oid);
  else {
    lispval args[2]; args[0]=oid; args[1]=state;
    value = kno_apply(fetchfn,2,args);}
  if (KNO_ABORTP(value))
    return value;
  else if ((EMPTYP(value))||(VOIDP(value)))
    return KNO_UNALLOCATED_OID;
  else return value;
}

/* This assumes that the FETCH function handles a vector intelligently,
   and just calls it on the vector of OIDs and extracts the data from
   the returned vector.  */
static lispval *extpool_fetchn(kno_pool p,int n,lispval *oids)
{
  struct KNO_EXTPOOL *xp = (kno_extpool)p;
  lispval state = xp->state, fetchfn = xp->fetchfn, value = VOID;
  int arity = KNO_FUNCTION_ARITY(fetchfn);
  struct KNO_VECTOR vstruct;
  lispval vecarg;
  KNO_INIT_STATIC_CONS(&vstruct,kno_vector_type);
  vstruct.vec_length = n; vstruct.vec_elts = oids;
  vstruct.vec_free_elts = 0;
  vecarg = LISP_CONS(&vstruct);
  if ( (VOIDP(state)) || (FALSEP(state)) || (arity == 1) )
    value = kno_apply(fetchfn,1,&vecarg);
  else {
    lispval args[2]; args[0]=vecarg; args[1]=state;
    value = kno_apply(fetchfn,2,args);}
  if (KNO_ABORTP(value))
    return NULL;
  else if (VECTORP(value)) {
    struct KNO_VECTOR *vstruct = (struct KNO_VECTOR *)value;
    lispval *results = u8_big_alloc_n(n,lispval);
    memcpy(results,vstruct->vec_elts,LISPVEC_BYTELEN(n));
    /* Free the CONS itself (and maybe data), to avoid DECREF/INCREF
       of values. */
    if (vstruct->vec_free_elts)
      u8_free(vstruct->vec_elts);
    u8_free((struct KNO_CONS *)value);
    return results;}
  else {
    u8_log(LOGWARN,"BadExtPoolFetch","Invalid value from %q",fetchfn);
    kno_decref(value); value = KNO_VOID;
    lispval *values = u8_big_alloc_n(n,lispval);
    if ( (VOIDP(state)) || (FALSEP(state)) || (arity == 1) ) {
      int i = 0; while (i<n) {
        lispval oid = oids[i];
        lispval value = kno_apply(fetchfn,1,&oid);
        if (KNO_ABORTP(value)) {
          int j = 0; while (j<i) {kno_decref(values[j]); j++;}
          u8_free(values);
          return NULL;}
        else values[i++]=value;}
      return values;}
    else {
      lispval args[2]; int i = 0; args[1]=state;
      i = 0; while (i<n) {
        lispval oid = oids[i], value;
        args[0]=oid; value = kno_apply(fetchfn,2,args);
        if (KNO_ABORTP(value)) {
          int j = 0; while (j<i) {kno_decref(values[j]); j++;}
          u8_free(values);
          return NULL;}
        else values[i++]=value;}
      return values;}}
}

static int extpool_lock(kno_pool p,lispval oids)
{
  struct KNO_EXTPOOL *xp = (struct KNO_EXTPOOL *) p;
  if (KNO_APPLICABLEP(xp->lockfn)) {
    lispval lockfn = xp->lockfn;
    kno_hashtable locks = &(xp->pool_changes);
    kno_hashtable cache = &(xp->pool_cache);
    DO_CHOICES(oid,oids) {
      lispval cur = kno_hashtable_get(cache,oid,VOID);
      lispval args[3]={lock_symbol,oid,
                       ((cur == KNO_LOCKHOLDER)?(VOID):(cur))};
      lispval value = kno_apply(lockfn,3,args);
      if (KNO_ABORTP(value)) {
        kno_decref(cur);
        return -1;}
      else if (FALSEP(value)) {}
      else if (VOIDP(value)) {
        kno_hashtable_store(locks,oid,KNO_LOCKHOLDER);}
      else kno_hashtable_store(locks,oid,value);
      kno_decref(cur); kno_decref(value);}}
  else if (FALSEP(xp->lockfn))
    return KNO_ERR(-1,kno_CantLockOID,"kno_pool_lock",p->poolid,oids);
  else {
    DO_CHOICES(oid,oids) {
      kno_hashtable_store(&(p->pool_changes),oids,KNO_LOCKHOLDER);}}
  return 1;
}

static lispval extpool_alloc(kno_pool p,int n)
{
  struct KNO_EXTPOOL *ep = (struct KNO_EXTPOOL *)p;
  if (VOIDP(ep->allocfn))
    return kno_err(_("No OID alloc method"),"extpool_alloc",NULL,
                  kno_pool2lisp(p));
  else {
    lispval args[2]; args[0]=KNO_INT(n); args[1]=ep->state;
    if (FIXNUMP(args[0]))
      return kno_apply(ep->allocfn,2,args);
    else {
      lispval result = kno_apply(ep->allocfn,2,args);
      kno_decref(args[0]);
      return result;}}
}

static int extpool_unlock(kno_pool p,lispval oids)
{
  struct KNO_EXTPOOL *xp = (struct KNO_EXTPOOL *) p;
  if (KNO_APPLICABLEP(xp->lockfn)) {
    lispval lockfn = xp->lockfn;
    kno_hashtable locks = &(xp->pool_changes);
    kno_hashtable cache = &(xp->pool_cache);
    DO_CHOICES(oid,oids) {
      lispval cur = kno_hashtable_get(locks,oid,VOID);
      lispval args[3]={unlock_symbol,oid,
                       ((cur == KNO_LOCKHOLDER)?(VOID):(cur))};
      lispval value = kno_apply(lockfn,3,args);
      kno_hashtable_store(locks,oid,VOID);
      if (KNO_ABORTP(value)) {
        kno_decref(cur);
        return -1;}
      else if (VOIDP(value)) {}
      else {
        kno_hashtable_store(cache,oid,value);}
      kno_decref(cur); kno_decref(value);}}
  else {
    DO_CHOICES(oid,oids) {
      kno_hashtable_store(&(p->pool_changes),oids,VOID);}}
  return 1;
}

KNO_EXPORT int kno_extpool_cache_value(kno_pool p,lispval oid,lispval value)
{
  if (p->pool_handler== &kno_extpool_handler)
    return kno_hashtable_store(&(p->pool_cache),oid,value);
  else return kno_reterr
         (kno_TypeError,"kno_extpool_cache_value",
          u8_strdup("extpool"),kno_pool2lisp(p));
}

static void recycle_extpool(kno_pool p)
{
  if (p->pool_handler== &kno_extpool_handler) {
    struct KNO_EXTPOOL *xp = (struct KNO_EXTPOOL *)p;
    kno_decref(xp->fetchfn); kno_decref(xp->savefn);
    kno_decref(xp->lockfn); kno_decref(xp->allocfn);
    kno_decref(xp->state);}
}

struct KNO_POOL_HANDLER kno_extpool_handler={
  "extpool", 1, sizeof(struct KNO_EXTPOOL), 12,
  NULL, /* close */
  extpool_alloc, /* alloc */
  extpool_fetch, /* fetch */
  extpool_fetchn, /* fetchn */
  NULL, /* getload */
  extpool_lock, /* lock */
  extpool_unlock, /* release */
  NULL, /* commit */
  NULL, /* swapout */
  NULL, /* create */
  NULL,  /* walk */
  recycle_extpool, /* recycle */
  NULL  /* poolop */
};

KNO_EXPORT void kno_init_extpool_c()
{
  lock_symbol = kno_intern("lock");
  unlock_symbol = kno_intern("unlock");

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
