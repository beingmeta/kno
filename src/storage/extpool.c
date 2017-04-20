/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_POOLS 1
#define FD_INLINE_BUFIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/apply.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

fdtype lock_symbol, unlock_symbol;

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
    memset(xp,0,sizeof(struct FD_EXTPOOL));
    fd_init_pool((fd_pool)xp,base,cap,&fd_extpool_handler,label,label);
    if (FD_VOIDP(savefn)) xp->pool_flags |= FDKB_READ_ONLY;
    fd_register_pool((fd_pool)xp);
    fd_incref(fetchfn); fd_incref(savefn);
    fd_incref(lockfn); fd_incref(allocfn);
    fd_incref(state);
    xp->fetchfn = fetchfn; xp->savefn = savefn;
    xp->lockfn = lockfn; xp->allocfn = allocfn;
    xp->state = state; xp->pool_label = label;
    xp->pool_flags = xp->pool_flags|FD_OIDHOLES_OKAY;
    return (fd_pool)xp;}
}

static fdtype extpool_fetch(fd_pool p,fdtype oid)
{
  struct FD_EXTPOOL *xp = (fd_extpool)p;
  fdtype state = xp->state, value, fetchfn = xp->fetchfn;
  struct FD_FUNCTION *fptr = ((FD_FUNCTIONP(fetchfn))?
                            ((struct FD_FUNCTION *)fetchfn):
                            (NULL));
  if ((FD_VOIDP(state))||(FD_FALSEP(state))||
      ((fptr)&&(fptr->fcn_arity==1)))
    value = fd_apply(fetchfn,1,&oid);
  else {
    fdtype args[2]; args[0]=oid; args[1]=state;
    value = fd_apply(fetchfn,2,args);}
  if (FD_ABORTP(value)) return value;
  else if ((FD_EMPTY_CHOICEP(value))||(FD_VOIDP(value)))
    if ((p->pool_flags)&FD_OIDHOLES_OKAY)
      return FD_EMPTY_CHOICE;
    else return fd_err(fd_UnallocatedOID,"extpool_fetch",xp->poolid,oid);
  else return value;
}

/* This assumes that the FETCH function handles a vector intelligently,
   and just calls it on the vector of OIDs and extracts the data from
   the returned vector.  */
static fdtype *extpool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_EXTPOOL *xp = (fd_extpool)p;
  struct FD_VECTOR vstruct; fdtype vecarg;
  fdtype state = xp->state, fetchfn = xp->fetchfn, value = FD_VOID;
  struct FD_FUNCTION *fptr = ((FD_FUNCTIONP(fetchfn))?
                            ((struct FD_FUNCTION *)fetchfn):
                            (NULL));
  FD_INIT_STATIC_CONS(&vstruct,fd_vector_type);
  vstruct.fdvec_length = n; vstruct.fdvec_elts = oids;
  vstruct.fdvec_free_elts = 0;
  vecarg = FDTYPE_CONS(&vstruct);
  if ((FD_VOIDP(state))||(FD_FALSEP(state))||
      ((fptr)&&(fptr->fcn_arity==1)))
    value = fd_apply(fetchfn,1,&vecarg);
  else {
    fdtype args[2]; args[0]=vecarg; args[1]=state;
    value = fd_apply(fetchfn,2,args);}
  if (FD_ABORTP(value)) return NULL;
  else if (FD_VECTORP(value)) {
    struct FD_VECTOR *vstruct = (struct FD_VECTOR *)value;
    fdtype *results = u8_alloc_n(n,fdtype);
    memcpy(results,vstruct->fdvec_elts,sizeof(fdtype)*n);
    /* Free the CONS itself (and maybe data), to avoid DECREF/INCREF
       of values. */
    if (vstruct->fdvec_free_elts) 
      u8_free(vstruct->fdvec_elts);
    u8_free((struct FD_CONS *)value);
    return results;}
  else {
    fdtype *values = u8_alloc_n(n,fdtype);
    if ((FD_VOIDP(state))||(FD_FALSEP(state))||
        ((fptr)&&(fptr->fcn_arity==1))) {
      int i = 0; while (i<n) {
        fdtype oid = oids[i];
        fdtype value = fd_apply(fetchfn,1,&oid);
        if (FD_ABORTP(value)) {
          int j = 0; while (j<i) {fd_decref(values[j]); j++;}
          u8_free(values);
          return NULL;}
        else values[i++]=value;}
      return values;}
    else {
      fdtype args[2]; int i = 0; args[1]=state;
      i = 0; while (i<n) {
        fdtype oid = oids[i], value;
        args[0]=oid; value = fd_apply(fetchfn,2,args);
        if (FD_ABORTP(value)) {
          int j = 0; while (j<i) {fd_decref(values[j]); j++;}
          u8_free(values);
          return NULL;}
        else values[i++]=value;}
      return values;}}
}

static int extpool_lock(fd_pool p,fdtype oids)
{
  struct FD_EXTPOOL *xp = (struct FD_EXTPOOL *) p;
  if (FD_APPLICABLEP(xp->lockfn)) {
    fdtype lockfn = xp->lockfn;
    fd_hashtable locks = &(xp->pool_changes);
    fd_hashtable cache = &(xp->pool_cache);
    FD_DO_CHOICES(oid,oids) {
      fdtype cur = fd_hashtable_get(cache,oid,FD_VOID);
      fdtype args[3]={lock_symbol,oid,
                      ((cur == FD_LOCKHOLDER)?(FD_VOID):(cur))};
      fdtype value = fd_apply(lockfn,3,args);
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
              u8_strdup(p->poolid),fd_incref(oids));
    return -1;}
  else {
    FD_DO_CHOICES(oid,oids) {
      fd_hashtable_store(&(p->pool_changes),oids,FD_LOCKHOLDER);}}
  return 1;
}

static fdtype extpool_alloc(fd_pool p,int n)
{
  struct FD_EXTPOOL *ep = (struct FD_EXTPOOL *)p;
  if (FD_VOIDP(ep->allocfn))
    return fd_err(_("No OID alloc method"),"extpool_alloc",NULL,
                  fd_pool2lisp(p));
  else {
    fdtype args[2]; args[0]=FD_INT(n); args[1]=ep->state;
    if (FD_FIXNUMP(args[0]))
      return fd_apply(ep->allocfn,2,args);
    else {
      fdtype result = fd_apply(ep->allocfn,2,args);
      fd_decref(args[0]);
      return result;}}
}

static int extpool_unlock(fd_pool p,fdtype oids)
{
  struct FD_EXTPOOL *xp = (struct FD_EXTPOOL *) p;
  if (FD_APPLICABLEP(xp->lockfn)) {
    fdtype lockfn = xp->lockfn; 
    fd_hashtable locks = &(xp->pool_changes);
    fd_hashtable cache = &(xp->pool_cache);
    FD_DO_CHOICES(oid,oids) {
      fdtype cur = fd_hashtable_get(locks,oid,FD_VOID);
      fdtype args[3]={unlock_symbol,oid,
                      ((cur == FD_LOCKHOLDER)?(FD_VOID):(cur))};
      fdtype value = fd_apply(lockfn,3,args);
      fd_hashtable_store(locks,oid,FD_VOID);
      if (FD_ABORTP(value)) {
        fd_decref(cur);
        return -1;}
      else if (FD_VOIDP(value)) {}
      else {
        fd_hashtable_store(cache,oid,value);}
      fd_decref(cur); fd_decref(value);}}
  else {
    FD_DO_CHOICES(oid,oids) {
      fd_hashtable_store(&(p->pool_changes),oids,FD_VOID);}}
  return 1;
}

FD_EXPORT int fd_extpool_cache_value(fd_pool p,fdtype oid,fdtype value)
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
  NULL, /* storen */
  NULL, /* swapout */
  NULL, /* metadata */
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
