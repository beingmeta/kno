/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/apply.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

lispval set_symbol, drop_symbol;

/* FETCH indexes */

FD_EXPORT
fd_index fd_make_extindex
(u8_string name,lispval fetchfn,lispval commitfn,lispval state,
 fd_storage_flags flags,lispval opts)
{
  if (!(PRED_TRUE(FD_APPLICABLEP(fetchfn)))) {
    fd_seterr(fd_TypeError,"fd_make_extindex","fetch function",
              fd_incref(fetchfn));
    return NULL;}
  else if (!(FD_ISFUNARG(commitfn))) {
    fd_seterr(fd_TypeError,"fd_make_extindex","commit function",
              fd_incref(commitfn));
    return NULL;}
  else {
    struct FD_EXTINDEX *fetchix = u8_alloc(struct FD_EXTINDEX);
    FD_INIT_STRUCT(fetchix,struct FD_EXTINDEX);
    fd_init_index((fd_index)fetchix,&fd_extindex_handler,
                  name,NULL,NULL,flags,FD_VOID,opts);
    fetchix->index_cache_level = 1;
    if (VOIDP(commitfn))
      U8_SETBITS(fetchix->index_flags,FD_STORAGE_READ_ONLY);
    fetchix->fetchfn = fd_incref(fetchfn);
    fetchix->commitfn = fd_incref(commitfn);
    fetchix->state = fd_incref(state);
    fd_register_index((fd_index)fetchix);
    return (fd_index)fetchix;}
}

static lispval extindex_fetch(fd_index p,lispval key)
{
  struct FD_EXTINDEX *xp = (fd_extindex)p;
  lispval state = xp->state, fetchfn = xp->fetchfn, value;
  struct FD_FUNCTION *fptr = ((FD_FUNCTIONP(fetchfn))?
                              ((struct FD_FUNCTION *)fetchfn):
                              (NULL));
  /* TODO: If key is a vector, we need to wrap it in another vector
     for the method, and then unwrap the value. */
  if (FD_VECTORP(key)) {
    struct FD_VECTOR vstruct;
    FD_INIT_STATIC_CONS(&vstruct,fd_vector_type);
    vstruct.vec_length = 1;
    vstruct.vec_elts = &key;
    vstruct.vec_free_elts = 0;
    lispval value = VOID;
    if ((VOIDP(state))||(FALSEP(state))||
        ((fptr)&&(fptr->fcn_arity==1)))
      value = fd_apply(fetchfn,1,&key);
    else {
      lispval args[2]; args[0]=key; args[1]=state;
      value = fd_apply(fetchfn,2,args);}
    if (FD_VECTORP(value)) {
      lispval result = FD_VECTOR_REF(value,0);
      fd_incref(result);
      fd_decref(value);
      return result;}
    else return fd_err("BadExtindexResult","extindex_fetch",
                       xp->indexid,value);}
  else if ((VOIDP(state))||(FALSEP(state))||
      ((fptr)&&(fptr->fcn_arity==1)))
    value = fd_apply(fetchfn,1,&key);
  else {
    lispval args[2]; args[0]=key; args[1]=state;
    value = fd_apply(fetchfn,2,args);}
  return value;
}

/* This assumes that the FETCH function handles a vector intelligently,
   and just calls it on the vector of OIDs and extracts the data from
   the returned vector.  */
static lispval *extindex_fetchn(fd_index p,int n,const lispval *keys)
{
  struct FD_EXTINDEX *xp = (fd_extindex)p;
  lispval state = xp->state, fetchfn = xp->fetchfn, value = VOID;
  struct FD_FUNCTION *fptr = ((FD_FUNCTIONP(fetchfn))?
                              ((struct FD_FUNCTION *)fetchfn):
                              (NULL));
  struct FD_VECTOR vstruct;
  lispval vecarg;
  FD_INIT_STATIC_CONS(&vstruct,fd_vector_type);
  vstruct.vec_length = n;
  vstruct.vec_elts = (lispval *) keys;
  vstruct.vec_free_elts = 0;
  vecarg = LISP_CONS(&vstruct);
  if ((VOIDP(state))||(FALSEP(state))||
      ((fptr)&&(fptr->fcn_arity==1)))
    value = fd_apply(xp->fetchfn,1,&vecarg);
  else {
    lispval args[2]; args[0]=vecarg; args[1]=state;
    value = fd_apply(xp->fetchfn,2,args);}
  if (FD_ABORTP(value))
    return NULL;
  else if (VECTORP(value)) {
    struct FD_VECTOR *vstruct = (struct FD_VECTOR *)value;
    lispval *results = u8_big_alloc_n(n,lispval);
    memcpy(results,vstruct->vec_elts,LISPVEC_BYTELEN(n));
    /* Free the CONS itself (and maybe data), to avoid DECREF/INCREF
       of the elements of the vector, which are being returned in
       *results*. */
    if (vstruct->vec_free_elts)
      u8_free(vstruct->vec_elts);
    u8_free((struct FD_CONS *)value);
    return results;}
  /* It didnt' return a vector, which is its way of telling us that it
     needs to be fed a key at a time. */
  fd_decref(value); value = FD_VOID;
  lispval *values = u8_big_alloc_n(n,lispval);
  if ((VOIDP(state))||(FALSEP(state))||
      ((fptr)&&(fptr->fcn_arity==1))) {
    int i = 0; while (i<n) {
      lispval key = keys[i];
      lispval value = fd_apply(fetchfn,1,&key);
      if (FD_ABORTP(value)) {
        int j = 0; while (j<i) {fd_decref(values[j]); j++;}
        u8_free(values);
        return NULL;}
      else values[i++]=value;}
    return values;}
  else {
    lispval args[2]; int i = 0; args[1]=state;
    i = 0; while (i<n) {
      lispval key = keys[i], value;
      args[0]=key; value = fd_apply(fetchfn,2,args);
      if (FD_ABORTP(value)) {
        int j = 0; while (j<i) {fd_decref(values[j]); j++;}
        u8_free(values);
        return NULL;}
      else values[i++]=value;}
    return values;}
}

static int extindex_save(struct FD_INDEX *ix,
                         struct FD_CONST_KEYVAL *adds,int n_adds,
                         struct FD_CONST_KEYVAL *drops,int n_drops,
                         struct FD_CONST_KEYVAL *stores,int n_stores,
                         lispval changed_metadata)
{
  struct FD_EXTINDEX *exi = (fd_extindex)ix;
  lispval avec = fd_make_vector(n_adds,NULL);
  lispval dvec = fd_make_vector(n_drops,NULL);
  lispval svec = fd_make_vector(n_stores,NULL);
  int i = 0; while (i<n_adds) {
    lispval key = adds[i].kv_key, val = adds[i].kv_val;
    lispval pair = fd_make_pair(key,val);
    FD_VECTOR_SET(avec,i,pair);}
  i=0; while (i<n_drops) {
    lispval key = drops[i].kv_key, val = drops[i].kv_val;
    lispval pair = fd_make_pair(key,val);
    FD_VECTOR_SET(dvec,i,pair);}
  i=0; while (i<n_stores) {
    lispval key = stores[i].kv_key, val = stores[i].kv_val;
    lispval pair = fd_make_pair(key,val);
    FD_VECTOR_SET(svec,i,pair);}

  lispval argv[4], result = VOID;
  argv[0]=avec;
  argv[1]=dvec;
  argv[2]=svec;
  argv[3]=exi->state;
  result = fd_apply(exi->commitfn,((VOIDP(exi->state))?(3):(4)),argv);
  fd_decref(argv[0]); fd_decref(argv[1]); fd_decref(argv[2]);
  if (FD_ABORTP(result))
    return -1;
  else {
    fd_decref(result);
    return 1;}
}

static int extindex_commit(fd_index ix,fd_commit_phase phase,
                           struct FD_INDEX_COMMITS *commit)
{
  switch (phase) {
  case fd_commit_write: {
    return extindex_save(ix,
                         (struct FD_CONST_KEYVAL *)commit->commit_adds,
                         commit->commit_n_adds,
                         (struct FD_CONST_KEYVAL *)commit->commit_drops,
                         commit->commit_n_drops,
                         (struct FD_CONST_KEYVAL *)commit->commit_stores,
                         commit->commit_n_stores,
                         commit->commit_metadata);}
  default: {
    u8_logf(LOG_INFO,"NoPhasedCommit",
            "The index %s doesn't support phased commits",
            ix->indexid);
    return 0;}
  }
}

static void recycle_extindex(fd_index ix)
{
  if (ix->index_handler== &fd_extindex_handler) {
    struct FD_EXTINDEX *ei = (struct FD_EXTINDEX *)ix;
    fd_decref(ei->fetchfn);
    fd_decref(ei->commitfn);
    fd_decref(ei->state);}
  u8_free(ix);
}

struct FD_INDEX_HANDLER fd_extindex_handler={
  "extindexhandler", 1, sizeof(struct FD_EXTINDEX), 14,
  NULL, /* close */
  extindex_commit, /* commit */
  extindex_fetch, /* fetch */
  NULL, /* fetchsize */
  NULL, /* prefetch */
  extindex_fetchn, /* fetchn */
  NULL, /* fetchkeys */
  NULL, /* fetchinfo */
  NULL, /* batchadd */
  NULL, /* create */
  NULL,  /* walk */
  recycle_extindex,  /* recycle */
  fd_default_indexctl  /* indexctl */
};

FD_EXPORT int fd_extindexp(fd_index ix)
{
  return (ix->index_handler == &fd_extindex_handler);
}

FD_EXPORT void fd_init_extindex_c()
{
  set_symbol = fd_intern("set");
  drop_symbol = fd_intern("drop");

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
