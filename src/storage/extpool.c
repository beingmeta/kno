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
#include "framerd/apply.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

lispval lock_symbol, unlock_symbol;

/* Fetch pools */

/* Fetch pools are pools which keep their data externally and take care
   of their own data management.  They basically have a fetch function and
   a load function but may be extended to be clever about saving in some way. */

FD_EXPORT
fd_pool fd_make_extpool(u8_string label,
                        FD_OID base,unsigned int cap,
                        lispval fetchfn,lispval savefn,
                        lispval lockfn,lispval allocfn,
                        lispval state,
                        lispval opts)
{
  if (!(PRED_TRUE(FD_APPLICABLEP(fetchfn)))) {
    fd_seterr(fd_TypeError,"fd_make_extpool","fetch function",
              fd_incref(fetchfn));
    return NULL;}
  else if (!(FD_ISFUNARG(savefn))) {
    fd_seterr(fd_TypeError,"fd_make_extpool","save function",
              fd_incref(savefn));
    return NULL;}
  else if (!(FD_ISFUNARG(lockfn))) {
    fd_seterr(fd_TypeError,"fd_make_extpool","lock function",
              fd_incref(lockfn));
    return NULL;}
  else if (!(FD_ISFUNARG(allocfn))) {
    fd_seterr(fd_TypeError,"fd_make_extpool","alloc function",
              fd_incref(allocfn));
    return NULL;}
  else {
    struct FD_EXTPOOL *xp = u8_alloc(struct FD_EXTPOOL);
    fd_storage_flags flags = FD_STORAGE_ISPOOL | FD_POOL_VIRTUAL;
    if (VOIDP(savefn)) flags |= FD_STORAGE_READ_ONLY;
    memset(xp,0,sizeof(struct FD_EXTPOOL));
    fd_init_pool((fd_pool)xp,base,cap,&fd_extpool_handler,
                 label,label,label,flags,FD_VOID,opts);
    /* We currently use the OID value as a cache for stored values, so
       we don't make it read-only. But there's probably a better
       solution. */
    fd_register_pool((fd_pool)xp);
    fd_incref(fetchfn);
    fd_incref(savefn);
    fd_incref(lockfn);
    fd_incref(allocfn);
    fd_incref(state);
    xp->fetchfn = fetchfn;
    xp->savefn = savefn;
    xp->lockfn = lockfn;
    xp->allocfn = allocfn;
    xp->state = state;
    xp->pool_label = u8_strdup(label);
    xp->pool_flags = xp->pool_flags|FD_POOL_SPARSE;

    lispval metadata = fd_getopt(opts,FDSYM_METADATA,FD_VOID);
    if (FD_SLOTMAPP(metadata))
      fd_copy_slotmap((fd_slotmap)metadata,&(xp->pool_metadata));
    fd_decref(metadata);

    return (fd_pool)xp;}
}

static lispval extpool_fetch(fd_pool p,lispval oid)
{
  struct FD_EXTPOOL *xp = (fd_extpool)p;
  lispval state = xp->state, value, fetchfn = xp->fetchfn;
  int arity = FD_FUNCTION_ARITY(fetchfn);
  if ( (VOIDP(state)) || (FALSEP(state)) || (arity == 1) )
    value = fd_apply(fetchfn,1,&oid);
  else {
    lispval args[2]; args[0]=oid; args[1]=state;
    value = fd_apply(fetchfn,2,args);}
  if (FD_ABORTP(value))
    return value;
  else if ((EMPTYP(value))||(VOIDP(value)))
    return FD_UNALLOCATED_OID;
  else return value;
}

/* This assumes that the FETCH function handles a vector intelligently,
   and just calls it on the vector of OIDs and extracts the data from
   the returned vector.  */
static lispval *extpool_fetchn(fd_pool p,int n,lispval *oids)
{
  struct FD_EXTPOOL *xp = (fd_extpool)p;
  lispval state = xp->state, fetchfn = xp->fetchfn, value = VOID;
  int arity = FD_FUNCTION_ARITY(fetchfn);
  struct FD_VECTOR vstruct;
  lispval vecarg;
  FD_INIT_STATIC_CONS(&vstruct,fd_vector_type);
  vstruct.vec_length = n; vstruct.vec_elts = oids;
  vstruct.vec_free_elts = 0;
  vecarg = LISP_CONS(&vstruct);
  if ( (VOIDP(state)) || (FALSEP(state)) || (arity == 1) )
    value = fd_apply(fetchfn,1,&vecarg);
  else {
    lispval args[2]; args[0]=vecarg; args[1]=state;
    value = fd_apply(fetchfn,2,args);}
  if (FD_ABORTP(value))
    return NULL;
  else if (VECTORP(value)) {
    struct FD_VECTOR *vstruct = (struct FD_VECTOR *)value;
    lispval *results = u8_big_alloc_n(n,lispval);
    memcpy(results,vstruct->vec_elts,LISPVEC_BYTELEN(n));
    /* Free the CONS itself (and maybe data), to avoid DECREF/INCREF
       of values. */
    if (vstruct->vec_free_elts)
      u8_free(vstruct->vec_elts);
    u8_free((struct FD_CONS *)value);
    return results;}
  else {
    u8_log(LOGWARN,"BadExtPoolFetch","Invalid value from %q",fetchfn);
    fd_decref(value); value = FD_VOID;
    lispval *values = u8_big_alloc_n(n,lispval);
    if ( (VOIDP(state)) || (FALSEP(state)) || (arity == 1) ) {
      int i = 0; while (i<n) {
        lispval oid = oids[i];
        lispval value = fd_apply(fetchfn,1,&oid);
        if (FD_ABORTP(value)) {
          int j = 0; while (j<i) {fd_decref(values[j]); j++;}
          u8_free(values);
          return NULL;}
        else values[i++]=value;}
      return values;}
    else {
      lispval args[2]; int i = 0; args[1]=state;
      i = 0; while (i<n) {
        lispval oid = oids[i], value;
        args[0]=oid; value = fd_apply(fetchfn,2,args);
        if (FD_ABORTP(value)) {
          int j = 0; while (j<i) {fd_decref(values[j]); j++;}
          u8_free(values);
          return NULL;}
        else values[i++]=value;}
      return values;}}
}

static int extpool_lock(fd_pool p,lispval oids)
{
  struct FD_EXTPOOL *xp = (struct FD_EXTPOOL *) p;
  if (FD_APPLICABLEP(xp->lockfn)) {
    lispval lockfn = xp->lockfn;
    fd_hashtable locks = &(xp->pool_changes);
    fd_hashtable cache = &(xp->pool_cache);
    DO_CHOICES(oid,oids) {
      lispval cur = fd_hashtable_get(cache,oid,VOID);
      lispval args[3]={lock_symbol,oid,
                       ((cur == FD_LOCKHOLDER)?(VOID):(cur))};
      lispval value = fd_apply(lockfn,3,args);
      if (FD_ABORTP(value)) {
        fd_decref(cur);
        return -1;}
      else if (FALSEP(value)) {}
      else if (VOIDP(value)) {
        fd_hashtable_store(locks,oid,FD_LOCKHOLDER);}
      else fd_hashtable_store(locks,oid,value);
      fd_decref(cur); fd_decref(value);}}
  else if (FALSEP(xp->lockfn)) {
    fd_seterr(fd_CantLockOID,"fd_pool_lock",p->poolid,fd_incref(oids));
    return -1;}
  else {
    DO_CHOICES(oid,oids) {
      fd_hashtable_store(&(p->pool_changes),oids,FD_LOCKHOLDER);}}
  return 1;
}

static lispval extpool_alloc(fd_pool p,int n)
{
  struct FD_EXTPOOL *ep = (struct FD_EXTPOOL *)p;
  if (VOIDP(ep->allocfn))
    return fd_err(_("No OID alloc method"),"extpool_alloc",NULL,
                  fd_pool2lisp(p));
  else {
    lispval args[2]; args[0]=FD_INT(n); args[1]=ep->state;
    if (FIXNUMP(args[0]))
      return fd_apply(ep->allocfn,2,args);
    else {
      lispval result = fd_apply(ep->allocfn,2,args);
      fd_decref(args[0]);
      return result;}}
}

static int extpool_unlock(fd_pool p,lispval oids)
{
  struct FD_EXTPOOL *xp = (struct FD_EXTPOOL *) p;
  if (FD_APPLICABLEP(xp->lockfn)) {
    lispval lockfn = xp->lockfn;
    fd_hashtable locks = &(xp->pool_changes);
    fd_hashtable cache = &(xp->pool_cache);
    DO_CHOICES(oid,oids) {
      lispval cur = fd_hashtable_get(locks,oid,VOID);
      lispval args[3]={unlock_symbol,oid,
                       ((cur == FD_LOCKHOLDER)?(VOID):(cur))};
      lispval value = fd_apply(lockfn,3,args);
      fd_hashtable_store(locks,oid,VOID);
      if (FD_ABORTP(value)) {
        fd_decref(cur);
        return -1;}
      else if (VOIDP(value)) {}
      else {
        fd_hashtable_store(cache,oid,value);}
      fd_decref(cur); fd_decref(value);}}
  else {
    DO_CHOICES(oid,oids) {
      fd_hashtable_store(&(p->pool_changes),oids,VOID);}}
  return 1;
}

FD_EXPORT int fd_extpool_cache_value(fd_pool p,lispval oid,lispval value)
{
  if (p->pool_handler== &fd_extpool_handler)
    return fd_hashtable_store(&(p->pool_cache),oid,value);
  else return fd_reterr
         (fd_TypeError,"fd_extpool_cache_value",
          u8_strdup("extpool"),fd_pool2lisp(p));
}

static void recycle_extpool(fd_pool p)
{
  if (p->pool_handler== &fd_extpool_handler) {
    struct FD_EXTPOOL *xp = (struct FD_EXTPOOL *)p;
    fd_decref(xp->fetchfn); fd_decref(xp->savefn);
    fd_decref(xp->lockfn); fd_decref(xp->allocfn);
    fd_decref(xp->state);}
}

struct FD_POOL_HANDLER fd_extpool_handler={
  "extpool", 1, sizeof(struct FD_EXTPOOL), 12,
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

FD_EXPORT void fd_init_extpool_c()
{
  lock_symbol = fd_intern("LOCK");
  unlock_symbol = fd_intern("UNLOCK");

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
