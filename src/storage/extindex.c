/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_BUFIO 1
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/apply.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

lispval set_symbol, drop_symbol;

/* FETCH indexes */

KNO_EXPORT
kno_index kno_make_extindex
(u8_string name,lispval fetchfn,lispval commitfn,lispval state,
 kno_storage_flags flags,lispval opts)
{
  if (!(USUALLY(KNO_APPLICABLEP(fetchfn))))
    return KNO_ERR(NULL,kno_TypeError,"kno_make_extindex","fetch function",
                   kno_incref(fetchfn));
  else if (!(KNO_ISFUNARG(commitfn)))
    return KNO_ERR(NULL,kno_TypeError,"kno_make_extindex","commit function",
                   kno_incref(commitfn));
  else {
    struct KNO_EXTINDEX *fetchix = u8_alloc(struct KNO_EXTINDEX);
    KNO_INIT_STRUCT(fetchix,struct KNO_EXTINDEX);
    kno_init_index((kno_index)fetchix,&kno_extindex_handler,
                  name,NULL,NULL,flags,KNO_VOID,opts);
    fetchix->index_cache_level = 1;
    if (VOIDP(commitfn))
      U8_SETBITS(fetchix->index_flags,KNO_STORAGE_READ_ONLY);
    fetchix->fetchfn = kno_incref(fetchfn);
    fetchix->commitfn = kno_incref(commitfn);
    fetchix->state = kno_incref(state);
    kno_register_index((kno_index)fetchix);
    return (kno_index)fetchix;}
}

static lispval extindex_fetch(kno_index p,lispval key)
{
  struct KNO_EXTINDEX *xp = (kno_extindex)p;
  lispval state = xp->state, fetchfn = xp->fetchfn, value;
  int arity = KNO_FUNCTION_ARITY(fetchfn);
  /* TODO: If key is a vector, we need to wrap it in another vector
     for the method, and then unwrap the value. */
  if (KNO_VECTORP(key)) {
    struct KNO_VECTOR vstruct;
    KNO_INIT_STATIC_CONS(&vstruct,kno_vector_type);
    vstruct.vec_length = 1;
    vstruct.vec_elts = &key;
    vstruct.vec_free_elts = 0;
    lispval value = VOID;
    if ( (VOIDP(state)) || (FALSEP(state)) || (arity == 1) )
      value = kno_apply(fetchfn,1,&key);
    else {
      lispval args[2]; args[0]=key; args[1]=state;
      value = kno_apply(fetchfn,2,args);}
    if (KNO_VECTORP(value)) {
      lispval result = KNO_VECTOR_REF(value,0);
      kno_incref(result);
      kno_decref(value);
      return result;}
    else if (KNO_ABORTED(value))
      return value;
    else return kno_err("BadExtindexResult","extindex_fetch",
                       xp->indexid,value);}
  else if ( (VOIDP(state)) || (FALSEP(state)) || (arity == 1) )
    value = kno_apply(fetchfn,1,&key);
  else {
    lispval args[2]; args[0]=key; args[1]=state;
    value = kno_apply(fetchfn,2,args);}
  return value;
}

/* This assumes that the FETCH function handles a vector intelligently,
   and just calls it on the vector of OIDs and extracts the data from
   the returned vector.  */
static lispval *extindex_fetchn(kno_index p,int n,const lispval *keys)
{
  struct KNO_EXTINDEX *xp = (kno_extindex)p;
  lispval state = xp->state, fetchfn = xp->fetchfn, value = VOID;
  int arity = KNO_FUNCTION_ARITY(fetchfn);
  struct KNO_VECTOR vstruct;
  lispval vecarg;
  KNO_INIT_STATIC_CONS(&vstruct,kno_vector_type);
  vstruct.vec_length = n;
  vstruct.vec_elts = (lispval *) keys;
  vstruct.vec_free_elts = 0;
  vecarg = LISP_CONS(&vstruct);
  if ( (VOIDP(state)) || (FALSEP(state)) || (arity == 1) )
    value = kno_apply(xp->fetchfn,1,&vecarg);
  else {
    lispval args[2]; args[0]=vecarg; args[1]=state;
    value = kno_apply(xp->fetchfn,2,args);}
  if (VECTORP(value)) {
    struct KNO_VECTOR *vstruct = (struct KNO_VECTOR *)value;
    lispval *results = u8_big_alloc_n(n,lispval);
    memcpy(results,vstruct->vec_elts,LISPVEC_BYTELEN(n));
    /* Free the CONS itself (and maybe data), to avoid DECREF/INCREF
       of the elements of the vector, which are being returned in
       *results*. */
    if (vstruct->vec_free_elts)
      u8_free(vstruct->vec_elts);
    u8_free((struct KNO_CONS *)value);
    return results;}
  else if (KNO_ABORTED(value))
    return NULL;
  else NO_ELSE;
  /* It didnt' return a vector, which is its way of telling us that it
     needs to be fed a key at a time. */
  kno_decref(value); value = KNO_VOID;
  lispval *values = u8_big_alloc_n(n,lispval);
  if ( (VOIDP(state)) || (FALSEP(state)) || (arity == 1) ) {
    int i = 0; while (i<n) {
      lispval key = keys[i];
      lispval value = kno_apply(fetchfn,1,&key);
      if (KNO_ABORTP(value)) {
        int j = 0; while (j<i) {kno_decref(values[j]); j++;}
        u8_free(values);
        return NULL;}
      else values[i++]=value;}
    return values;}
  else {
    lispval args[2]; int i = 0; args[1]=state;
    i = 0; while (i<n) {
      lispval key = keys[i], value;
      args[0]=key; value = kno_apply(fetchfn,2,args);
      if (KNO_ABORTP(value)) {
        int j = 0; while (j<i) {kno_decref(values[j]); j++;}
        u8_free(values);
        return NULL;}
      else values[i++]=value;}
    return values;}
}

static int extindex_save(struct KNO_INDEX *ix,
                         struct KNO_CONST_KEYVAL *adds,int n_adds,
                         struct KNO_CONST_KEYVAL *drops,int n_drops,
                         struct KNO_CONST_KEYVAL *stores,int n_stores,
                         lispval changed_metadata)
{
  struct KNO_EXTINDEX *exi = (kno_extindex)ix;
  lispval avec = kno_make_vector(n_adds,NULL);
  lispval dvec = kno_make_vector(n_drops,NULL);
  lispval svec = kno_make_vector(n_stores,NULL);
  int i = 0; while (i<n_adds) {
    lispval key = adds[i].kv_key, val = adds[i].kv_val;
    lispval pair = kno_make_pair(key,val);
    KNO_VECTOR_SET(avec,i,pair);}
  i=0; while (i<n_drops) {
    lispval key = drops[i].kv_key, val = drops[i].kv_val;
    lispval pair = kno_make_pair(key,val);
    KNO_VECTOR_SET(dvec,i,pair);}
  i=0; while (i<n_stores) {
    lispval key = stores[i].kv_key, val = stores[i].kv_val;
    lispval pair = kno_make_pair(key,val);
    KNO_VECTOR_SET(svec,i,pair);}

  lispval argv[4], result = VOID;
  argv[0]=avec;
  argv[1]=dvec;
  argv[2]=svec;
  argv[3]=exi->state;
  result = kno_apply(exi->commitfn,((VOIDP(exi->state))?(3):(4)),argv);
  kno_decref(argv[0]); kno_decref(argv[1]); kno_decref(argv[2]);
  if (KNO_ABORTED(result))
    return -1;
  else {
    kno_decref(result);
    return 1;}
}

static int extindex_commit(kno_index ix,kno_commit_phase phase,
                           struct KNO_INDEX_COMMITS *commit)
{
  switch (phase) {
  case kno_commit_write: {
    return extindex_save(ix,
                         (struct KNO_CONST_KEYVAL *)commit->commit_adds,
                         commit->commit_n_adds,
                         (struct KNO_CONST_KEYVAL *)commit->commit_drops,
                         commit->commit_n_drops,
                         (struct KNO_CONST_KEYVAL *)commit->commit_stores,
                         commit->commit_n_stores,
                         commit->commit_metadata);}
  default: {
    u8_logf(LOG_INFO,"NoPhasedCommit",
            "The index %s doesn't support phased commits",
            ix->indexid);
    return 0;}
  }
}

static void recycle_extindex(kno_index ix)
{
  if (ix->index_handler== &kno_extindex_handler) {
    struct KNO_EXTINDEX *ei = (struct KNO_EXTINDEX *)ix;
    kno_decref(ei->fetchfn);
    kno_decref(ei->commitfn);
    kno_decref(ei->state);}
  u8_free(ix);
}

struct KNO_INDEX_HANDLER kno_extindex_handler={
  "extindexhandler", 1, sizeof(struct KNO_EXTINDEX), 14,
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
  kno_default_indexctl  /* indexctl */
};

KNO_EXPORT int kno_extindexp(kno_index ix)
{
  return (ix->index_handler == &kno_extindex_handler);
}

KNO_EXPORT void kno_init_extindex_c()
{
  set_symbol = kno_intern("set");
  drop_symbol = kno_intern("drop");

  u8_register_source_file(_FILEINFO);
}

