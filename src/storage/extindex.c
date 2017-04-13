/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/apply.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

fdtype set_symbol, drop_symbol;

/* FETCH indexes */

FD_EXPORT
fd_index fd_make_extindex
  (u8_string name,fdtype fetchfn,fdtype commitfn,fdtype state,int reg)
{
  if (!(FD_EXPECT_TRUE(FD_APPLICABLEP(fetchfn)))) {
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
		  name,NULL,(!(reg)));
    fetchix->index_cache_level = 1;
    if (FD_VOIDP(commitfn))
      U8_SETBITS(fetchix->index_flags,FDKB_READ_ONLY);
    fetchix->fetchfn = fd_incref(fetchfn);
    fetchix->commitfn = fd_incref(commitfn);
    fetchix->state = fd_incref(state);
    if (reg) fd_register_index((fd_index)fetchix);
    else fetchix->index_serialno = -1;
    return (fd_index)fetchix;}
}

static fdtype extindex_fetch(fd_index p,fdtype oid)
{
  struct FD_EXTINDEX *xp = (fd_extindex)p;
  fdtype state = xp->state, fetchfn = xp->fetchfn, value;
  struct FD_FUNCTION *fptr = ((FD_FUNCTIONP(fetchfn))?
                            ((struct FD_FUNCTION *)fetchfn):
                            (NULL));
  if ((FD_VOIDP(state))||(FD_FALSEP(state))||
      ((fptr)&&(fptr->fcn_arity==1)))
    value = fd_apply(fetchfn,1,&oid);
  else {
    fdtype args[2]; args[0]=oid; args[1]=state;
    value = fd_apply(fetchfn,2,args);}
  return value;
}

/* This assumes that the FETCH function handles a vector intelligently,
   and just calls it on the vector of OIDs and extracts the data from
   the returned vector.  */
static fdtype *extindex_fetchn(fd_index p,int n,fdtype *keys)
{
  struct FD_EXTINDEX *xp = (fd_extindex)p;
  struct FD_VECTOR vstruct; fdtype vecarg;
  fdtype state = xp->state, fetchfn = xp->fetchfn, value = FD_VOID;
  struct FD_FUNCTION *fptr = ((FD_FUNCTIONP(fetchfn))?
                            ((struct FD_FUNCTION *)fetchfn):
                            (NULL));
  FD_INIT_STATIC_CONS(&vstruct,fd_vector_type);
  vstruct.fdvec_length = n; 
  vstruct.fdvec_elts = keys; 
  vstruct.fdvec_free_elts = 0;
  vecarg = FDTYPE_CONS(&vstruct);
  if ((FD_VOIDP(state))||(FD_FALSEP(state))||
      ((fptr)&&(fptr->fcn_arity==1)))
    value = fd_apply(xp->fetchfn,1,&vecarg);
  else {
    fdtype args[2]; args[0]=vecarg; args[1]=state;
    value = fd_apply(xp->fetchfn,2,args);}
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
        fdtype key = keys[i];
        fdtype value = fd_apply(fetchfn,1,&key);
        if (FD_ABORTP(value)) {
          int j = 0; while (j<i) {fd_decref(values[j]); j++;}
          u8_free(values);
          return NULL;}
        else values[i++]=value;}
      return values;}
    else {
      fdtype args[2]; int i = 0; args[1]=state;
      i = 0; while (i<n) {
        fdtype key = keys[i], value;
        args[0]=key; value = fd_apply(fetchfn,2,args);
        if (FD_ABORTP(value)) {
          int j = 0; while (j<i) {fd_decref(values[j]); j++;}
          u8_free(values);
          return NULL;}
        else values[i++]=value;}
      return values;}}
}

static int extindex_commit(fd_index ix)
{
  struct FD_EXTINDEX *exi = (fd_extindex)ix;
  fdtype *adds, *drops, *stores;
  int n_adds = 0, n_drops = 0, n_stores = 0;
  fd_write_lock_table(&(exi->index_adds));
  fd_write_lock_table(&(exi->index_edits));
  if (exi->index_edits.table_n_keys) {
    drops = u8_alloc_n(exi->index_edits.table_n_keys,fdtype);
    stores = u8_alloc_n(exi->index_edits.table_n_keys,fdtype);}
  else {drops = NULL; stores = NULL;}
  if (exi->index_adds.table_n_keys)
    adds = u8_alloc_n(exi->index_adds.table_n_keys,fdtype);
  else adds = NULL;
  if (exi->index_edits.table_n_keys) {
    int n_edits;
    struct FD_KEYVAL *kvals = fd_hashtable_keyvals(&(exi->index_edits),&n_edits,0);
    struct FD_KEYVAL *scan = kvals, *limit = kvals+n_edits;
    while (scan<limit) {
      fdtype key = scan->kv_key;
      if (FD_PAIRP(key)) {
        fdtype kind = FD_CAR(key), realkey = FD_CDR(key), value = scan->kv_val;
        fdtype assoc = fd_conspair(realkey,value);
        if (FD_EQ(kind,set_symbol)) {
          stores[n_stores++]=assoc; fd_incref(realkey);}
        else if (FD_EQ(kind,drop_symbol)) {
          drops[n_drops++]=assoc; fd_incref(realkey);}
        else u8_raise(_("Bad edit key in index"),"fd_extindex_commit",NULL);}
      else u8_raise(_("Bad edit key in index"),"fd_extindex_commit",NULL);
      scan++;}
    scan = kvals; while (scan<kvals) {
      fd_decref(scan->kv_key);
      /* Not neccessary because we used the pointer above. */
      /* fd_decref(scan->kv_val); */
      scan++;}}
  if (exi->index_adds.table_n_keys) {
    int add_len;
    struct FD_KEYVAL *kvals = fd_hashtable_keyvals(&(exi->index_adds),&add_len,0);
    /* Note that we don't have to decref these kvals because
       their pointers will become in the assocs in the adds vector. */
    struct FD_KEYVAL *scan = kvals, *limit = kvals+add_len;
    while (scan<limit) {
      fdtype key = scan->kv_key, value = scan->kv_val;
      fdtype assoc = fd_conspair(key,value);
      adds[n_adds++]=assoc;
      scan++;}}
  {
    fdtype avec = fd_init_vector(NULL,n_adds,adds);
    fdtype dvec = fd_init_vector(NULL,n_drops,drops);
    fdtype svec = fd_init_vector(NULL,n_stores,stores);
    fdtype argv[4], result = FD_VOID;
    argv[0]=avec;
    argv[1]=dvec;
    argv[2]=svec;
    argv[3]=exi->state;
    result = fd_apply(exi->commitfn,((FD_VOIDP(exi->state))?(3):(4)),argv);
    fd_decref(argv[0]); fd_decref(argv[1]); fd_decref(argv[2]);
    fd_reset_hashtable(&(exi->index_adds),fd_index_adds_init,0);
    fd_reset_hashtable(&(exi->index_edits),fd_index_edits_init,0);
    fd_unlock_table(&(exi->index_adds));
    fd_unlock_table(&(exi->index_edits));
    if (FD_ABORTP(result)) return -1;
    else {fd_decref(result); return 1;}
  }
}

static void recycle_extindex(fd_index ix)
{
  if (ix->index_handler== &fd_extindex_handler) {
    struct FD_EXTINDEX *ei = (struct FD_EXTINDEX *)ix;
    fd_decref(ei->fetchfn);
    fd_decref(ei->commitfn);
    fd_decref(ei->state);}
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
  NULL, /* fetchsizes */
  NULL, /* batchadd */
  NULL, /* metadata */
  NULL, /* create */
  NULL,  /* walk */
  recycle_extindex,  /* recycle */
  NULL  /* indexctl */
};

FD_EXPORT void fd_init_extindex_c()
{
  set_symbol = fd_intern("set");
  drop_symbol = fd_intern("drop");

  u8_register_source_file(_FILEINFO);
}
