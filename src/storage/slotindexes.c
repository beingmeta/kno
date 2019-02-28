/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_POOLS 1
#define FD_INLINE_INDEXES 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_IPEVAL 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/apply.h"

#include <stdarg.h>

static u8_condition OddFindFramesArgs=_("Odd number of args to find frames");

static lispval index2lisp(fd_index ix)
{
  if (ix->index_serialno>=0)
    return LISPVAL_IMMEDIATE(fd_index_type,ix->index_serialno);
  else {
    lispval v = (lispval) ix;
    fd_incref(v);
    return v;}
}

/* Searching */

static lispval make_features(lispval slotids,lispval values)
{
  if (EMPTYP(values))
    return values;
  else {
    lispval results = EMPTY;
    DO_CHOICES(slotid,slotids) {
      DO_CHOICES(value,values) {
	lispval feature = fd_make_pair(slotid,value);
	CHOICE_ADD(results,feature);}}
    return results;}
}

static lispval aggregate_prim_find
(fd_aggregate_index ax,lispval slotids,lispval values);

static lispval index_prim_find(fd_index ix,lispval slotids,lispval values)
{
  if (fd_aggregate_indexp(ix))
    return aggregate_prim_find((fd_aggregate_index)ix,slotids,values);
  lispval combined = FD_EMPTY_CHOICE;
  lispval keyslot = ix->index_keyslot;
  DO_CHOICES(slotid,slotids) {
    if (slotid == keyslot) {
      if (FD_AMBIGP(values))
	fd_index_prefetch(ix,values);
      DO_CHOICES(value,values) {
	lispval result = fd_index_get(ix,value);
	if (FD_ABORTP(result)) {
	  FD_STOP_DO_CHOICES;
	  fd_decref(combined);
	  return result;}
	CHOICE_ADD(combined,result);}}
    else if ( (OIDP(keyslot)) || (SYMBOLP(keyslot)) ||
	      ( (FD_AMBIGP(keyslot)) &&
		(! (fd_choice_containsp(slotid,keyslot))) ) ) {
      /* Don't bother fetching */}
    else if (FD_AMBIGP(values)) {
      lispval keys = FD_EMPTY_CHOICE;
      {DO_CHOICES(value,values) {
	  lispval key = fd_make_pair(slotid,value);
	  FD_ADD_TO_CHOICE(keys,key);}}
      {DO_CHOICES(key,keys) {
	  lispval result = fd_index_get(ix,key);
	  if (FD_ABORTP(result)) {
	    FD_STOP_DO_CHOICES;
	    fd_decref(keys);
	    fd_decref(combined);
	    return result;}
	  else {CHOICE_ADD(combined,result);}}}
      fd_decref(keys);}
    else {
      lispval key = fd_make_pair(slotid,values);
      lispval result = fd_index_get(ix,key);
      if (FD_ABORTP(result)) {
	FD_STOP_DO_CHOICES;
	fd_decref(combined);
	return result;}
      CHOICE_ADD(combined,result);
      fd_decref(key);}}
  return combined;
}

static lispval aggregate_prim_find
(fd_aggregate_index ax,lispval slotids,lispval values)
{
  lispval combined = FD_EMPTY;
  lispval keyslot = ax->index_keyslot;
  int i = 0, n =ax->ax_n_indexes;
  fd_index *indexes = ax->ax_indexes;
  if ( (FD_SYMBOLP(keyslot)) || (FD_OIDP(keyslot)) ) {
    if (fd_choice_containsp(keyslot,slotids)) {
      fd_index_prefetch((fd_index)ax,values);
      DO_CHOICES(value,values) {
	lispval v = fd_index_get((fd_index)ax,value);
	if (FD_ABORTED(v)) {
	  fd_decref(combined);
	  return v;}
	else {CHOICE_ADD(combined,v);}}}
    else {/* No matching slotids */}}
  else if ( (FD_AMBIGP(keyslot)) &&
	    (! (fd_overlapp(keyslot,slotids)) ) ) {
    /* No matching slotids */}
  else {
    /* If we get here, the cache is store keypairs as conses because
       either there isn't a keyslot or it's a choice. */
    fd_hashtable cache = &(ax->index_cache);
    lispval features = make_features(slotids,values);
    lispval fetch_features = FD_EMPTY;
    /* Get the features we need to fetch */
    {DO_CHOICES(feature,features) {
	lispval cached = fd_hashtable_get(cache,feature,FD_VOID);
	if (!(FD_VOIDP(cached))) {
	  CHOICE_ADD(combined,cached);
	  fd_decref(feature);}
	else {
	  CHOICE_ADD(fetch_features,feature);}}}
    if (!(FD_EMPTYP(fetch_features))) {
      i=0; while (i < n) {
	fd_index ex = indexes[i++];
	if (fd_aggregate_indexp(ex)) {
	  /* If it's an embedded aggregate, recur, passing slotids and
	     values again. We might end up recomputing slotvalue pairs,
	     but we won't worry about that for now. */
	  lispval ex_results =
	    aggregate_prim_find((fd_aggregate_index)ex,slotids,values);
	  if (FD_ABORTED(ex_results)) {
	    fd_decref(fetch_features);
	    fd_decref(combined);
	    return ex_results;}
	  else {CHOICE_ADD(combined,ex_results);}
	  continue;}
	lispval ekeyslot = ex->index_keyslot;
	if ( (SYMBOLP(ekeyslot)) || (OIDP(ekeyslot)) ) {
	  if (fd_choice_containsp(ekeyslot,slotids)) {
	    lispval ex_results = index_prim_find(ex,ekeyslot,values);
	    if (FD_ABORTED(ex_results)) {
	      fd_decref(fetch_features);
	      fd_decref(combined);
	      return ex_results;}
	    else {CHOICE_ADD(combined,ex_results);}}
	  else {/* No requests for ekeyslot */}
	  continue;}
	else {
	  fd_index_prefetch((fd_index)ex,fetch_features);
	  ekeyslot = FD_VOID;}
	int j=0, n_features = FD_CHOICE_SIZE(fetch_features);
	/* We will use keyvec and valvec to update the aggregate index
	   cache. */
	lispval *keyvec = u8_alloc_n(n_features,lispval);
	lispval *valvec = u8_alloc_n(n_features,lispval);
	DO_CHOICES(feature,fetch_features) {
	  lispval v = fd_index_get((fd_index)ex,feature);
	  if (FD_ABORTED(v)) {
	    fd_decref(fetch_features);
	    fd_decref(combined);
	    fd_decref_vec(valvec,j);
	    u8_free(keyvec);
	    u8_free(valvec);
	    FD_STOP_DO_CHOICES;
	    return v;}
	  keyvec[j] = feature;
	  valvec[j] = v;
	  fd_incref(v);
	  CHOICE_ADD(combined,v);
	  j++;}
	int rv = fd_hashtable_iter(cache,fd_table_add,j,keyvec,valvec);
	fd_decref_vec(valvec,j);
	u8_free(keyvec);
	u8_free(valvec);
	if (rv<0) {
	  fd_decref(fetch_features);
	  return FD_ERROR_VALUE;}}
      fd_decref(fetch_features);}}
  return combined;
}

FD_EXPORT lispval fd_prim_find(lispval indexes,lispval slotids,lispval values)
{
  if (CHOICEP(indexes)) {
    lispval combined = EMPTY;
    DO_CHOICES(index,indexes) {
      if (FD_INDEXP(index)) {
	fd_index ix = fd_indexptr(index);
	lispval indexed = index_prim_find(ix,slotids,values);
	if (FD_ABORTP(indexed)) {
	  fd_decref(combined);
	  return indexed;}
	CHOICE_ADD(combined,indexed);}
      else if (TABLEP(index)) {
	DO_CHOICES(slotid,slotids) {
	  DO_CHOICES(value,values) {
	    lispval key = fd_make_pair(slotid,value);
	    lispval result = fd_get(index,key,EMPTY);
	    if (FD_ABORTP(result)) {
	      fd_decref(combined);
	      return result;}
	    CHOICE_ADD(combined,result);
	    fd_decref(key);}}}
      else {
	fd_decref(combined);
	return fd_type_error(_("index"),"fd_prim_find",index);}}
    return combined;}
  else if (FD_INDEXP(indexes))
    return index_prim_find(fd_indexptr(indexes),slotids,values);
  else if (TABLEP(indexes)) {
    lispval combined = EMPTY;
    DO_CHOICES(slotid,slotids) {
      DO_CHOICES(value,values) {
	lispval key = fd_make_pair(slotid,value);
	lispval result = fd_get(indexes,key,EMPTY);
	if (FD_ABORTP(result)) {
	  fd_decref(combined);
	  return result;}
	CHOICE_ADD(combined,result);
	fd_decref(key);}}
    return combined;}
  else return fd_type_error("index/table","fd_prim_find",indexes);
}

FD_EXPORT lispval fd_finder(lispval indexes,int n,lispval *slotvals)
{
  if (EMPTYP(indexes)) return EMPTY;
  int i = 0, n_conjuncts = n/2;
  lispval conjuncts[n_conjuncts], result;
  while (i < n_conjuncts) {
    conjuncts[i]=fd_prim_find(indexes,slotvals[i*2],slotvals[i*2+1]);
    if (FD_ABORTP(conjuncts[i])) {
      lispval error = conjuncts[i];
      fd_decref_vec(conjuncts,i);
      return error;}
    if (EMPTYP(conjuncts[i])) {
      fd_decref_vec(conjuncts,i);
      return EMPTY;}
    i++;}
  if (n_conjuncts == 1)
    return conjuncts[0];
  result = fd_intersection(conjuncts,n_conjuncts);
  fd_decref_vec(conjuncts,n_conjuncts);
  return result;
}

FD_EXPORT lispval fd_find_frames(lispval indexes,...)
{
  lispval _slotvals[64], *slotvals=_slotvals, val;
  int n_slotvals = 0, max_slotvals = 64; va_list args;
  va_start(args,indexes); val = va_arg(args,lispval);
  while (!(VOIDP(val))) {
    if (n_slotvals>=max_slotvals) {
      if (max_slotvals == 64) {
	lispval *newsv = u8_alloc_n(128,lispval); int i = 0;
	while (i<64) {newsv[i]=slotvals[i]; i++;}
	slotvals = newsv; max_slotvals = 128;}
      else {
	slotvals = u8_realloc(slotvals,LISPVEC_BYTELEN(max_slotvals)*2);
	max_slotvals = max_slotvals*2;}}
    slotvals[n_slotvals++]=val; val = va_arg(args,lispval);}
  if (n_slotvals%2) {
    if (slotvals != _slotvals) u8_free(slotvals);
    return fd_err(OddFindFramesArgs,"fd_find_frames",NULL,VOID);}
  if (slotvals != _slotvals) {
    lispval result = fd_finder(indexes,n_slotvals,slotvals);
    u8_free(slotvals);
    return result;}
  else return fd_finder(indexes,n_slotvals,slotvals);
}


/* Find prefetching */

/* This prefetches a set of slotvalue keys from an index.
   The main advantage of this over assembling a set of keys
   externally is that this doesn't bother sorting the vector
   of generated keys.  This can save time, especially when the
   numbers of values is large and values may be strings. */

FD_EXPORT
int fd_find_prefetch(fd_index ix,lispval slotids,lispval values)
{
  lispval keyslot = ix->index_keyslot;
  if ((ix->index_handler->fetchn) == NULL) {
    lispval keys = EMPTY;
    DO_CHOICES(slotid,slotids) {
      if (slotid == keyslot) {
	fd_incref(values);
	CHOICE_ADD(keys,values);}
      else if ( (OIDP(keyslot)) || (SYMBOLP(keyslot)) ||
		( (AMBIGP(keyslot)) && (! (fd_overlapp(keyslot,slotids)) ) ) ) {
	/* These aren't the features you're looking for */}
      else {
	DO_CHOICES(value,values) {
	  lispval key = fd_conspair(slotid,value);
	  CHOICE_ADD(keys,key);}}}
    fd_index_prefetch(ix,keys);
    fd_decref(keys);
    return 1;}
  else {
    int max_keys = FD_CHOICE_SIZE(slotids)*FD_CHOICE_SIZE(values);
    lispval *keyv = u8_alloc_n(max_keys,lispval);
    lispval *valuev = NULL;
    int n_keys = 0;
    DO_CHOICES(slotid,slotids) {
      if (keyslot == slotid) {
	DO_CHOICES(value,values) {
	  keyv[n_keys++]=value;
	  fd_incref(value);}}
      else if ( (OIDP(keyslot)) || (SYMBOLP(keyslot)) ||
		( (AMBIGP(keyslot)) && (! (fd_overlapp(keyslot,slotids)) ) ) ) {
	/* These aren't the features you're looking for */}
      else {
	DO_CHOICES(value,values) {
	  lispval key = fd_conspair(slotid,value);
	  keyv[n_keys++]=key;}}}
    valuev = (ix->index_handler->fetchn)(ix,n_keys,keyv);
    fd_hashtable_iter(&(ix->index_cache),fd_table_add_empty_noref,
		      n_keys,keyv,valuev);
    u8_free(keyv); u8_free(valuev);
    return 1;}
}


/* Indexing frames */

#define FD_LOOP_BREAK() FD_STOP_DO_CHOICES; break

#define FALSISH(x) ( (FD_VOIDP(x)) || (FD_FALSEP(x)) || (FD_EMPTYP(x)) )

static fd_index get_writable_slotindex(fd_index ix,lispval slotid)
{
  if (fd_aggregate_indexp(ix)) {
    lispval keyslot = ix->index_keyslot;
    fd_index generic =
      ( (FALSISH(keyslot)) ||
	( (FD_AMBIGP(keyslot)) &&
	  (fd_choice_containsp(slotid,keyslot)) ) ) ?
      (fd_get_writable_index(ix)) :
      (NULL);
    struct FD_AGGREGATE_INDEX *aix = (fd_aggregate_index) ix;
    fd_index *indexes = aix->ax_indexes;
    int i = 0, n = aix->ax_n_indexes; while (i<n) {
      fd_index possible = indexes[i++], use_front = NULL;
      lispval pkeyslot = possible->index_keyslot;
      if (pkeyslot == slotid) {
	if ( (use_front = fd_get_writable_index(possible)) ){
	  if (generic) fd_decref_index(generic);
	  return use_front;}}
      else if ( (generic == NULL) &&
		( (FD_VOIDP(pkeyslot)) ||
		  (FD_FALSEP(pkeyslot)) ||
		  (FD_EMPTYP(pkeyslot)) ) ) {
	fd_incref_index(possible);
	generic = possible;}
      else {}}
    i = 0; while (i<n) {
      fd_index possible = indexes[i++], use_front = NULL;
      lispval pkeyslot = possible->index_keyslot;
      if (FD_AMBIGP(pkeyslot) && (fd_choice_containsp(slotid,pkeyslot)) ) {
	if ( (use_front = fd_get_writable_index(possible)) ) {
	  if (generic) fd_decref_index(generic);
	  return use_front;}}
      else {}}
    return generic;}
  else return fd_get_writable_index(ix);
}

FD_EXPORT
int fd_index_frame(fd_index ix,lispval frames,lispval slotids,lispval values)
{
  int rv = 0;
  if (FD_EMPTYP(values)) return 0;
  DO_CHOICES(slotid,slotids) {
    fd_index write_index = get_writable_slotindex(ix,slotid);
    if (write_index == NULL) {
      lispval irritant = fd_index2lisp(ix);
      fd_seterr("Read-only index","fd_index_frame",
		ix->indexid,irritant);
      fd_decref(irritant);
      return -1;}
    lispval keyslot = write_index->index_keyslot;
    DO_CHOICES(frame,frames) {
      int add_rv = 0;
      lispval use_values = (FD_VOIDP(values)) ?
	(fd_frame_get(frame,slotid)) : (values);
      if (FD_ABORTP(use_values)) add_rv = -1;
      else if (FD_EMPTYP(use_values)) {}
      else if (slotid == keyslot)
	add_rv = fd_index_add(write_index,use_values,frame);
      else {
	lispval features = FD_EMPTY;
	FD_DO_CHOICES(value,use_values) {
	  lispval feature = fd_init_pair(NULL,slotid,value);
	  fd_incref(slotid); fd_incref(value);
	  CHOICE_ADD(features,feature);}
	add_rv = fd_index_add(write_index,features,frame);
	fd_decref(features);}
      if (use_values != values) fd_decref(use_values);
      if (add_rv < 0) { rv = -1; FD_LOOP_BREAK();}
      else rv += add_rv;}
    if (FD_INDEX_CONSEDP(write_index)) fd_decref(LISPVAL(write_index));
    if (rv<0) { FD_LOOP_BREAK(); }}
  return rv;
}

struct KEYSLOT_STATE { fd_hashtable adds; lispval keyslot; };

static int merge_keys_with_slotid(struct FD_KEYVAL *kv,void *data)
{
  struct KEYSLOT_STATE *state = data;
  struct FD_PAIR pair;
  FD_INIT_STATIC_CONS(&pair,fd_pair_type);
  pair.car = state->keyslot;
  pair.cdr = kv->kv_key;
  fd_hashtable_op_nolock(state->adds,fd_table_add,(lispval)&pair,kv->kv_val);
  return 0;
}

static int merge_keys_for_slotids(struct FD_KEYVAL *kv,void *data)
{
  struct KEYSLOT_STATE *state = data;
  lispval key = kv->kv_key;
  lispval keyslot = state->keyslot;
  if ( (FD_PAIRP(key)) &&
       (fd_choice_containsp(FD_CAR(key),keyslot)) )
    fd_hashtable_op_nolock(state->adds,fd_table_add,key,kv->kv_val);
  return 0;
}

static int merge_keys_without_slotid(struct FD_KEYVAL *kv,void *data)
{
  struct KEYSLOT_STATE *state = data;
  lispval key = kv->kv_key;
  if (FD_PAIRP(key)) {
    if ((FD_CAR(key)) == state->keyslot )
      fd_hashtable_op_nolock(state->adds,fd_table_add,FD_CDR(key),kv->kv_val);
    return 0;}
  else return 0;
}

#define SINGLE_KEYSLOTP(x) ( (FD_OIDP(x)) || (FD_SYMBOLP(x)) )

FD_EXPORT int fd_slotindex_merge(fd_index into,lispval from)
{
  lispval keyslot = into->index_keyslot;
  if (FD_HASHTABLEP(from)) {
    if (FD_VOIDP(keyslot))
      return fd_index_merge(into,(fd_hashtable)from);
    else if (SINGLE_KEYSLOTP(keyslot)) {
      /* If we merge a hashtable into a single-keyslot index, we just
	 add the values directly. */
      struct KEYSLOT_STATE state = { (fd_hashtable)from, keyslot };
      int rv = fd_for_hashtable_kv
	((fd_hashtable)from,merge_keys_without_slotid,&state,1);
      return rv;}
    else  {
      /* If we merge a hashtable into an index with keyslots, we merge
	 only the pairs whose cars (the slotkeys) are in the target's
	 slotkeys. */
      struct KEYSLOT_STATE state = { (fd_hashtable)from, keyslot };
      int rv = fd_for_hashtable_kv
	((fd_hashtable)from,merge_keys_for_slotids,&state,1);
      return rv;}}
  else if ( (SINGLE_KEYSLOTP(keyslot)) && (FD_INDEXP(from)) ) {
    fd_index ix = fd_indexptr(from);
    if (fd_tempindexp(ix)) {
      if ( (FD_VOIDP(ix->index_keyslot)) ||
	   ( (FD_AMBIGP(ix->index_keyslot)) &&
	     (fd_choice_containsp(keyslot,ix->index_keyslot)) ) ) {
	struct KEYSLOT_STATE state = { &ix->index_adds, keyslot };
	/* If the input index doesn't have any keyslots, then it
	   contains (slot . value) keys and we merge just the values
	   for entries whose slot matches the target's keyslot). */
	int rv = fd_for_hashtable_kv
	  (&(into->index_adds),merge_keys_without_slotid,&state,1);
	return rv;}
      else if ( ix->index_keyslot == keyslot ) {
	/* If the target index and the subject index have exactly the
	   same keyslots, do a straight merge. */
	return fd_index_merge(into,&(ix->index_adds));}
      else {
	/* In this case, the subject index doesn't have any keys in
	   which we're interested. */
	return 0;}}
    else {
      fd_seterr("Not a hashtable or tempindex","fd_slotindex_merge",
		into->indexid,from);
      return -1;}}
  else if ( (FALSISH(keyslot)) && (FD_INDEXP(from)) ) {
    /* This is where we just copy any key value pairs we find. */
    fd_index ix = fd_indexptr(from);
    if (fd_tempindexp(ix)) {
      if (SINGLE_KEYSLOTP(ix->index_keyslot)) {
	struct KEYSLOT_STATE state = { &ix->index_adds, ix->index_keyslot };
	int rv = fd_for_hashtable_kv
	  (&(into->index_adds),merge_keys_with_slotid,&state,1);
	return rv;}
      else return fd_index_merge(into,&(ix->index_adds));}
    else if (fd_aggregate_indexp(ix)) {
      int merged = 0;
      struct FD_AGGREGATE_INDEX *agg = (fd_aggregate_index) from;
      fd_index *indexes = agg->ax_indexes;
      int i=0, n=agg->ax_n_indexes; while (i<n) {
	fd_index partition = indexes[i++];
	if (partition->index_adds.table_n_keys == 0)
	  continue;
	lispval partition_ptr = index2lisp(partition);
	merged=fd_slotindex_merge(into,partition_ptr);
	if (merged<0) {
	  fd_seterr("IndexMergeError","fd_slotindex_merge",
		    into->indexid,partition_ptr);
	  fd_decref(partition_ptr);
	  break;}
	else fd_decref(partition_ptr);}
      return merged;}
    else {
      fd_seterr("IndexMergeError","fd_slotindex_merge",
		into->indexid,from);
      return -1;}}
  else if (FD_INDEXP(from)) {
    /* Here, the keyslot is a choice of slotids */
    fd_index ix = fd_indexptr(from);
    if (fd_tempindexp(ix)) {
      if ( (SINGLE_KEYSLOTP(ix->index_keyslot)) &&
	   (fd_choice_containsp(ix->index_keyslot,keyslot)) ) {
	struct KEYSLOT_STATE state = { &ix->index_adds, ix->index_keyslot };
	int rv = fd_for_hashtable_kv
	  (&(into->index_adds),merge_keys_with_slotid,&state,1);
	return rv;}
      else if ( (FD_AMBIGP(ix->index_keyslot)) ) {
	lispval v[2] = { ix->index_keyslot, keyslot };
	lispval common = fd_intersection(v,2);
	if (!(FD_EMPTYP(common))) {
	  struct KEYSLOT_STATE state = { &ix->index_adds, common };
	  int rv = fd_for_hashtable_kv
	    (&(into->index_adds),merge_keys_for_slotids,&state,1);
	  fd_decref(common);
	  return rv;}
	return 0;}
      else return fd_index_merge(into,&(ix->index_adds));}
    else if (fd_aggregate_indexp(ix)) {
      int merged = 0;
      struct FD_AGGREGATE_INDEX *agg = (fd_aggregate_index) from;
      fd_index *indexes = agg->ax_indexes;
      int i=0, n=agg->ax_n_indexes; while (i<n) {
	fd_index partition = indexes[i++];
	lispval pkeyslot = partition->index_keyslot;
	if (partition->index_adds.table_n_keys == 0)
	  continue;
	else if ( (SINGLE_KEYSLOTP(pkeyslot)) &&
		  (! (fd_choice_containsp(pkeyslot,keyslot)) ) )
	  continue;
	else if ( (FD_AMBIGP(pkeyslot)) &&
		  (! (fd_overlapp(pkeyslot,keyslot)) ) )
	  continue;
	else NO_ELSE;
	/* Call merge on into and the partition */
	lispval partition_ptr = index2lisp(partition);
	merged=fd_slotindex_merge(into,partition_ptr);
	if (merged<0) {
	  fd_seterr("IndexMergeError","fd_slotindex_merge",
		    into->indexid,partition_ptr);
	  fd_decref(partition_ptr);
	  break;}
	else fd_decref(partition_ptr);}
      return merged;}
    else {
      fd_seterr("IndexMergeError","fd_slotindex_merge",
		into->indexid,from);
      return -1;}}
  else {
    fd_seterr("Not a hashtable or tempindex","fd_slotindex_merge",
	      into->indexid,from);
    return -1;}
}

/* Background searching */

FD_EXPORT lispval fd_bg_get(lispval slotid,lispval value)
{
  return aggregate_prim_find(fd_background,slotid,value);
}

FD_EXPORT lispval fd_bgfinder(int n,lispval *slotvals)
{
  return fd_finder(index2lisp((fd_index)fd_background),n,slotvals);
}

FD_EXPORT lispval fd_bgfind(lispval slotid,lispval values,...)
{
    int n_args = 2;
    va_list args, scan; va_start(args,values);
    lispval arg = va_arg(args,lispval);
    while (!(VOIDP(arg))) {
      n_args++;
      arg = va_arg(args,lispval);}
    if (n_args == 0) {
      lispval slotvals[2] = {slotid,values};
      return fd_bgfinder(2,slotvals);}
    else if (n_args%2)
      return fd_err(OddFindFramesArgs,"fd_bgfind",NULL,VOID);
    else {
      lispval slotvals[n_args], val;
      int i = 2;
      slotvals[0] = slotid; slotvals[1] = values;
      /* Start over */
      va_start(scan,values);
      val = va_arg(scan,lispval);
      while (!(VOIDP(val))) {
	val = va_arg(scan,lispval);
	slotvals[i++]=val;
	val = va_arg(scan,lispval);
	slotvals[i++]=val;}
      return fd_bgfinder(n_args,slotvals);}
}

FD_EXPORT int fd_bg_prefetch(lispval keys)
{
  return fd_index_prefetch((fd_index)fd_background,keys);
}

FD_EXPORT void fd_init_slotindexes_c()
{
  u8_register_source_file(_FILEINFO);
}
