/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_POOLS (!(KNO_AVOID_INLINE))
#define KNO_INLINE_INDEXES (!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES (!(KNO_AVOID_INLINE))
#define KNO_INLINE_CHOICES (!(KNO_AVOID_INLINE))
#define KNO_INLINE_IPEVAL (!(KNO_AVOID_INLINE))
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/apply.h"

#include <libu8/u8printf.h>

#include <stdarg.h>

static u8_condition OddFindFramesArgs=_("Odd number of args to find frames");
static u8_condition NoWritableIndexForSlot=_("NoIndexForSlot");

static lispval index2lisp(kno_index ix)
{
  if (ix->index_serialno>=0)
    return LISPVAL_IMMEDIATE(kno_indexref_type,ix->index_serialno);
  else {
    lispval v = (lispval) ix;
    kno_incref(v);
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
	lispval feature = kno_make_pair(slotid,value);
	CHOICE_ADD(results,feature);}}
    return results;}
}

static lispval aggregate_prim_find
(kno_aggregate_index ax,lispval slotids,lispval values);

static lispval index_prim_find(kno_index ix,lispval slotids,lispval values)
{
  if (kno_aggregate_indexp(ix))
    return aggregate_prim_find((kno_aggregate_index)ix,slotids,values);
  lispval combined = KNO_EMPTY_CHOICE;
  lispval keyslot = ix->index_keyslot;
  DO_CHOICES(slotid,slotids) {
    if (slotid == keyslot) {
      if (KNO_AMBIGP(values))
	kno_index_prefetch(ix,values);
      DO_CHOICES(value,values) {
	lispval result = kno_index_get(ix,value);
	if (KNO_ABORTP(result)) {
	  KNO_STOP_DO_CHOICES;
	  kno_decref(combined);
	  return result;}
	CHOICE_ADD(combined,result);}}
    else if ( (OIDP(keyslot)) || (SYMBOLP(keyslot)) ||
	      ( (KNO_AMBIGP(keyslot)) &&
		(! (kno_choice_containsp(slotid,keyslot))) ) ) {
      /* This index doesn't cover keyslot, so don't bother fetching
       anything */
      continue;}
    else if (KNO_AMBIGP(values)) {
      lispval keys = KNO_EMPTY_CHOICE;
      {DO_CHOICES(value,values) {
	  lispval key = kno_make_pair(slotid,value);
	  KNO_ADD_TO_CHOICE(keys,key);}}
      {DO_CHOICES(key,keys) {
	  lispval result = kno_index_get(ix,key);
	  if (KNO_ABORTP(result)) {
	    KNO_STOP_DO_CHOICES;
	    kno_decref(keys);
	    kno_decref(combined);
	    return result;}
	  else {CHOICE_ADD(combined,result);}}}
      kno_decref(keys);}
    else {
      lispval key = kno_make_pair(slotid,values);
      lispval result = kno_index_get(ix,key);
      if (KNO_ABORTP(result)) {
	KNO_STOP_DO_CHOICES;
	kno_decref(combined);
	return result;}
      CHOICE_ADD(combined,result);
      kno_decref(key);}}
  return combined;
}

static lispval aggregate_prim_find
(kno_aggregate_index ax,lispval slotids,lispval values)
{
  lispval combined = KNO_EMPTY;
  lispval keyslot = ax->index_keyslot;
  int i = 0, n =ax->ax_n_indexes;
  kno_index *indexes = ax->ax_indexes;
  if ( (KNO_SYMBOLP(keyslot)) || (KNO_OIDP(keyslot)) ) {
    if (kno_choice_containsp(keyslot,slotids)) {
      kno_index_prefetch((kno_index)ax,values);
      DO_CHOICES(value,values) {
	lispval v = kno_index_get((kno_index)ax,value);
	if (KNO_ABORTED(v)) {
	  kno_decref(combined);
	  return v;}
	else {CHOICE_ADD(combined,v);}}}
    else {/* No matching slotids */}}
  else if ( (KNO_AMBIGP(keyslot)) &&
	    (! (kno_overlapp(keyslot,slotids)) ) ) {
    /* No matching slotids */}
  else {
    /* If we get here, the cache is store keypairs as conses because
       either there isn't a keyslot or it's a choice. */
    kno_hashtable cache = &(ax->index_cache);
    lispval features = make_features(slotids,values);
    lispval fetch_features = KNO_EMPTY;
    /* Get the features we need to fetch */
    if (ax->index_cache_level > 0) {
      DO_CHOICES(feature,features) {
	lispval cached = kno_hashtable_get(cache,feature,KNO_VOID);
	if (!(KNO_VOIDP(cached))) {
	  CHOICE_ADD(combined,cached);}
	else {
	  CHOICE_ADD(fetch_features,feature);
	  kno_incref(feature);}}
      fetch_features = kno_simplify_choice(fetch_features);
      kno_decref(features);}
    else fetch_features=features;
    if (!(KNO_EMPTYP(fetch_features))) {
      i=0; while (i < n) {
	kno_index ex = indexes[i++];
	if (kno_aggregate_indexp(ex)) {
	  /* If it's an embedded aggregate, recur, passing slotids and
	     values again. We might end up recomputing slotvalue pairs,
	     but we won't worry about that for now. */
	  lispval ex_results =
	    aggregate_prim_find((kno_aggregate_index)ex,slotids,values);
	  if (KNO_ABORTED(ex_results)) {
	    kno_decref(fetch_features);
	    kno_decref(combined);
	    return ex_results;}
	  else {CHOICE_ADD(combined,ex_results);}
	  continue;}
	lispval ekeyslot = ex->index_keyslot;
	if ( (SYMBOLP(ekeyslot)) || (OIDP(ekeyslot)) ) {
	  if (kno_choice_containsp(ekeyslot,slotids)) {
	    lispval ex_results = index_prim_find(ex,ekeyslot,values);
	    if (KNO_ABORTED(ex_results)) {
	      kno_decref(fetch_features);
	      kno_decref(combined);
	      return ex_results;}
	    else {CHOICE_ADD(combined,ex_results);}}
	  else {/* No requests for ekeyslot */}
	  continue;}
	else if ( (KNO_CHOICEP(ekeyslot)) &&
		  (! (kno_overlapp(ekeyslot,slotids)) ) ) {
	  /* No overlapping slotids between index and query */}
	else {
	  kno_index_prefetch((kno_index)ex,fetch_features);
	  ekeyslot = KNO_VOID;}
	int j=0, n_features = KNO_CHOICE_SIZE(fetch_features);
	/* We will use keyvec and valvec to update the aggregate index
	   cache. */
	lispval *keyvec = u8_alloc_n(n_features,lispval);
	lispval *valvec = u8_alloc_n(n_features,lispval);
	DO_CHOICES(feature,fetch_features) {
	  lispval v = kno_index_get((kno_index)ex,feature);
	  if (KNO_ABORTED(v)) {
	    kno_decref(fetch_features);
	    kno_decref(combined);
	    kno_decref_vec(valvec,j);
	    u8_free(keyvec);
	    u8_free(valvec);
	    KNO_STOP_DO_CHOICES;
	    return v;}
	  keyvec[j] = feature;
	  valvec[j] = v;
	  kno_incref(v);
	  CHOICE_ADD(combined,v);
	  j++;}
	int rv = (ax->index_cache_level<1) ? (0) :
	  (kno_hashtable_iter(cache,kno_table_add,j,keyvec,valvec));
	kno_decref_vec(valvec,j);
	u8_free(keyvec);
	u8_free(valvec);
	if (rv<0) {
	  kno_decref(fetch_features);
	  return KNO_ERROR_VALUE;}}
      kno_decref(fetch_features);}}
  return combined;
}

KNO_EXPORT lispval kno_prim_find(lispval indexes,lispval slotids,lispval values)
{
  if (CHOICEP(indexes)) {
    lispval combined = EMPTY;
    DO_CHOICES(index,indexes) {
      if (KNO_INDEXP(index)) {
	kno_index ix = kno_indexptr(index);
	lispval indexed = index_prim_find(ix,slotids,values);
	if (KNO_ABORTP(indexed)) {
	  kno_decref(combined);
	  return indexed;}
	CHOICE_ADD(combined,indexed);}
      else if (TABLEP(index)) {
	DO_CHOICES(slotid,slotids) {
	  DO_CHOICES(value,values) {
	    lispval key = kno_make_pair(slotid,value);
	    lispval result = kno_get(index,key,EMPTY);
	    if (KNO_ABORTP(result)) {
	      kno_decref(combined);
	      return result;}
	    CHOICE_ADD(combined,result);
	    kno_decref(key);}}}
      else {
	kno_decref(combined);
	return kno_type_error(_("index"),"kno_prim_find",index);}}
    return combined;}
  else if (KNO_INDEXP(indexes))
    return index_prim_find(kno_indexptr(indexes),slotids,values);
  else if (TABLEP(indexes)) {
    lispval combined = EMPTY;
    DO_CHOICES(slotid,slotids) {
      DO_CHOICES(value,values) {
	lispval key = kno_make_pair(slotid,value);
	lispval result = kno_get(indexes,key,EMPTY);
	if (KNO_ABORTP(result)) {
	  kno_decref(combined);
	  return result;}
	CHOICE_ADD(combined,result);
	kno_decref(key);}}
    return combined;}
  else return kno_type_error("index/table","kno_prim_find",indexes);
}

KNO_EXPORT lispval kno_finder(lispval indexes,int n,kno_argvec slotvals)
{
  if (EMPTYP(indexes)) return EMPTY;
  int i = 0, n_conjuncts = n/2;
  lispval conjuncts[n_conjuncts], result;
  while (i < n_conjuncts) {
    conjuncts[i]=kno_prim_find(indexes,slotvals[i*2],slotvals[i*2+1]);
    if (KNO_ABORTP(conjuncts[i])) {
      lispval error = conjuncts[i];
      kno_decref_vec(conjuncts,i);
      return error;}
    if (EMPTYP(conjuncts[i])) {
      kno_decref_vec(conjuncts,i);
      return EMPTY;}
    i++;}
  if (n_conjuncts == 1)
    return conjuncts[0];
  result = kno_intersection(conjuncts,n_conjuncts);
  kno_decref_vec(conjuncts,n_conjuncts);
  return result;
}

KNO_EXPORT lispval kno_find_frames(lispval indexes,...)
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
    return kno_err(OddFindFramesArgs,"kno_find_frames",NULL,VOID);}
  if (slotvals != _slotvals) {
    lispval result = kno_finder(indexes,n_slotvals,slotvals);
    u8_free(slotvals);
    return result;}
  else return kno_finder(indexes,n_slotvals,slotvals);
}

/* Find prefetching */

static int aggregate_prefetch
(kno_aggregate_index ax,lispval slotids,lispval values);

/* This prefetches a set of slotvalue keys from an index.
   The main advantage of this over assembling a set of keys
   externally is that this doesn't bother sorting the vector
   of generated keys.  This can save time, especially when the
   numbers of values is large and values may be strings. */

KNO_EXPORT
int kno_find_prefetch(kno_index ix,lispval slotids,lispval values)
{
  lispval keyslot = ix->index_keyslot;
  if (kno_aggregate_indexp(ix))
    return aggregate_prefetch((kno_aggregate_index)ix,slotids,values);
  else if ((ix->index_handler->fetchn) == NULL) {
    lispval keys = EMPTY;
    DO_CHOICES(slotid,slotids) {
      if (slotid == keyslot) {
	kno_incref(values);
	CHOICE_ADD(keys,values);}
      else if ( (OIDP(keyslot)) || (SYMBOLP(keyslot)) ||
		( (AMBIGP(keyslot)) && (! (kno_overlapp(keyslot,slotids)) ) ) ) {
	/* These aren't the features you're looking for */}
      else {
	DO_CHOICES(value,values) {
	  lispval key = kno_conspair(slotid,value);
	  CHOICE_ADD(keys,key);}}}
    if (KNO_EMPTYP(keys)) return 0;
    kno_index_prefetch(ix,keys);
    kno_decref(keys);
    return 1;}
  else {
    int max_keys = KNO_CHOICE_SIZE(slotids)*KNO_CHOICE_SIZE(values);
    lispval *keyv = u8_alloc_n(max_keys,lispval);
    lispval *valuev = NULL;
    int n_keys = 0;
    DO_CHOICES(slotid,slotids) {
      if (keyslot == slotid) {
	DO_CHOICES(value,values) {
	  keyv[n_keys++]=value;
	  kno_incref(value);}}
      else if ( (OIDP(keyslot)) || (SYMBOLP(keyslot)) ||
		( (AMBIGP(keyslot)) && (! (kno_overlapp(keyslot,slotids)) ) ) ) {
	/* These aren't the features you're looking for */}
      else {
	DO_CHOICES(value,values) {
	  lispval key = kno_conspair(slotid,value);
	  keyv[n_keys++]=key;}}}
    if (n_keys) {
      valuev = (ix->index_handler->fetchn)(ix,n_keys,keyv);
      if (ix->index_cache_level>0)
	kno_hashtable_iter(&(ix->index_cache),kno_table_add_empty_noref,
			   n_keys,keyv,valuev);}
    u8_free(keyv); if (valuev) u8_free(valuev);
    return 1;}
}

static int aggregate_prefetch
(kno_aggregate_index ax,lispval slotids,lispval values)
{
  kno_hashtable cache = (ax->index_cache_level<1) ? (NULL) :
    (&(ax->index_cache));
  lispval keyslot = ax->index_keyslot;
  int one_slot = (!(KNO_AMBIGP(keyslot))) &&
    ( (KNO_OIDP(keyslot)) || (KNO_SYMBOLP(keyslot)) );
  if (one_slot) {
    if (! ( ( keyslot == slotids) || (kno_choice_containsp(keyslot,slotids)) ) )
      return 0;}
  else if ( (KNO_AMBIGP(keyslot)) && (! (kno_overlapp(keyslot,slotids)) ) )
    return 0;
  else if (KNO_AMBIGP(keyslot)) { /* Will need to free slotids */
    lispval combine[2] = { keyslot, slotids };
    slotids = kno_intersection(combine,2);}
  else NO_ELSE;
  int i = 0, n = ax->ax_n_indexes;
  kno_index *indexes = ax->ax_indexes;
  if (one_slot) {
    ssize_t n_fetched = 0;
    lispval fetch_values = KNO_EMPTY;
    if (cache) {
      KNO_DO_CHOICES(value,values) {
	if (kno_hashtable_probe(cache,value) == 0) {
	  kno_incref(value);
	  KNO_ADD_TO_CHOICE(fetch_values,value);}}
      fetch_values = kno_simplify_choice(fetch_values);}
    else fetch_values=kno_incref(values);
    if (KNO_EMPTYP(fetch_values)) return 0;
    while (i < n) {
      kno_index ex = indexes[i++];
      kno_hashtable ecache = &(ex->index_cache);
      int rv = kno_index_prefetch(ex,fetch_values);
      DO_CHOICES(value,fetch_values) {
	lispval v = kno_hashtable_get(ecache,value,KNO_VOID);
	if (!(KNO_VOIDP(v))) {
	  if (KNO_AMBIGP(v)) v = kno_simplify_choice(v);
	  rv = kno_hashtable_add(cache,value,v);
	  n_fetched += KNO_CHOICE_SIZE(v);
	  kno_decref(v);
	  if (rv<0) {
	    n_fetched = -1;
	    KNO_STOP_DO_CHOICES;
	    break;}}
	else NO_ELSE;}}
    kno_decref(fetch_values);
    return n_fetched;}
  else {
    ssize_t n_fetched = 0;
    lispval features = make_features(slotids,values);
    lispval fetch_features = KNO_EMPTY;
    if (cache) {
      KNO_DO_CHOICES(feature,features) {
	if (! (kno_hashtable_probe(cache,feature)) ) {
	  kno_incref(feature);
	  KNO_ADD_TO_CHOICE(fetch_features,feature);}}}
    while (i < n) {
      kno_index ex = indexes[i++];
      lispval ekeyslot = ex->index_keyslot;
      kno_hashtable ecache = &(ex->index_cache);
      if ( (KNO_OIDP(ekeyslot)) || (KNO_OIDP(keyslot)) ) {
	lispval fetch_values = KNO_EMPTY;
	{DO_CHOICES(feature,fetch_features) {
	    if ( (KNO_PAIRP(feature)) && (KNO_EQ(KNO_CAR(feature),ekeyslot)) ) {
	      lispval v = KNO_CDR(feature); kno_incref(v);
	      KNO_ADD_TO_CHOICE(fetch_values,v);}}}
	int rv = kno_index_prefetch(ex,fetch_values);
	if (rv<0) { n_fetched = -1; break; }
	{DO_CHOICES(fetch_val,fetch_values) {
	    lispval keyval = kno_conspair(ekeyslot,fetch_val);
	    lispval refs = kno_hashtable_get(ecache,keyval,KNO_VOID);
	    if ( (cache) && (! ( (KNO_VOIDP(refs)) || (KNO_EMPTYP(refs)) ) ) )
	      rv = kno_hashtable_add(cache,keyval,refs);
	    kno_decref(refs);
	    kno_decref(keyval);
	    if (rv<0) {
	      n_fetched = -1;
	      KNO_STOP_DO_CHOICES;
	      break;}}}
	kno_decref(fetch_values);
	if (n_fetched < 0) break;}
      else {
	int rv = kno_index_prefetch(ex,fetch_features);
	if (rv<0) { n_fetched = -1; break; }
	DO_CHOICES(feature,fetch_features) {
	  lispval v = kno_hashtable_get(ecache,feature,KNO_VOID);
	  if (!(KNO_VOIDP(v))) {
	    if (KNO_AMBIGP(v)) v = kno_simplify_choice(v);
	    if (cache)
	      rv = kno_hashtable_add(cache,feature,v);
	    n_fetched += KNO_CHOICE_SIZE(v);
	    kno_decref(v);
	    if (rv<0) {KNO_STOP_DO_CHOICES; break;}}}}}
    kno_decref(fetch_features);
    kno_decref(features);
    return n_fetched;}
}

/* Indexing frames */

#define KNO_LOOP_BREAK() KNO_STOP_DO_CHOICES; break

#define FALSISH(x) ( (KNO_VOIDP(x)) || (KNO_FALSEP(x)) || (KNO_EMPTYP(x)) )

static kno_index get_writable_slotindex(kno_index ix,lispval slotid)
{
  if (kno_aggregate_indexp(ix)) {
    lispval keyslot = ix->index_keyslot;
    kno_index generic =
      ( (FALSISH(keyslot)) ||
	( (KNO_AMBIGP(keyslot)) &&
	  (kno_choice_containsp(slotid,keyslot)) ) ) ?
      (kno_get_writable_index(ix)) :
      (NULL);
    struct KNO_AGGREGATE_INDEX *aix = (kno_aggregate_index) ix;
    kno_index *indexes = aix->ax_indexes;
    int i = 0, n = aix->ax_n_indexes; while (i<n) {
      kno_index possible = indexes[i++], use_front = NULL;
      lispval pkeyslot = possible->index_keyslot;
      if (pkeyslot == slotid) {
	if ( (use_front = kno_get_writable_index(possible)) ){
	  if (generic) kno_decref_index(generic);
	  return use_front;}}
      else if ( (generic == NULL) &&
		( (KNO_VOIDP(pkeyslot)) ||
		  (KNO_FALSEP(pkeyslot)) ||
		  (KNO_EMPTYP(pkeyslot)) ) ) {
	kno_incref_index(possible);
	generic = possible;}
      else {}}
    i = 0; while (i<n) {
      kno_index possible = indexes[i++], use_front = NULL;
      lispval pkeyslot = possible->index_keyslot;
      if (KNO_AMBIGP(pkeyslot) && (kno_choice_containsp(slotid,pkeyslot)) ) {
	if ( (use_front = kno_get_writable_index(possible)) ) {
	  if (generic) kno_decref_index(generic);
	  return use_front;}}
      else {}}
    return generic;}
  else return kno_get_writable_index(ix);
}

KNO_EXPORT
int kno_index_frame(kno_index ix,lispval frames,lispval slotids,lispval values)
{
  int rv = 0;
  if (KNO_EMPTYP(values)) return 0;
  DO_CHOICES(slotid,slotids) {
    kno_index write_index = get_writable_slotindex(ix,slotid);
    if (write_index == NULL) {
      lispval irritant = kno_index2lisp(ix);
      u8_byte errbuf[256];
      kno_seterr(NoWritableIndexForSlot,"kno_index_frame",
		 (KNO_SYMBOLP(slotid)) ? 
		 (KNO_SYMBOL_NAME(slotid)) :
		 (u8_bprintf(errbuf,"%q",slotid)),
		 irritant);
      kno_decref(irritant);
      return -1;}
    lispval keyslot = write_index->index_keyslot;
    DO_CHOICES(frame,frames) {
      int add_rv = 0;
      lispval use_values = (KNO_VOIDP(values)) ?
	(kno_frame_get(frame,slotid)) : (values);
      if (KNO_ABORTP(use_values)) add_rv = -1;
      else if (KNO_EMPTYP(use_values)) {}
      else if (slotid == keyslot)
	add_rv = kno_index_add(write_index,use_values,frame);
      else {
	lispval features = KNO_EMPTY;
	KNO_DO_CHOICES(value,use_values) {
	  lispval feature = kno_init_pair(NULL,slotid,value);
	  kno_incref(slotid); kno_incref(value);
	  CHOICE_ADD(features,feature);}
	add_rv = kno_index_add(write_index,features,frame);
	kno_decref(features);}
      if (use_values != values) kno_decref(use_values);
      if (add_rv < 0) { rv = -1; KNO_LOOP_BREAK();}
      else rv += add_rv;}
    if (KNO_INDEX_CONSEDP(write_index)) kno_decref(LISPVAL(write_index));
    if (rv<0) { KNO_LOOP_BREAK(); }}
  return rv;
}

struct KEYSLOT_STATE { kno_hashtable adds; lispval keyslot; };

static int merge_keys_with_slotid(struct KNO_KEYVAL *kv,void *data)
{
  struct KEYSLOT_STATE *state = data;
  struct KNO_PAIR pair;
  KNO_INIT_STATIC_CONS(&pair,kno_pair_type);
  pair.car = state->keyslot;
  pair.cdr = kv->kv_key;
  kno_hashtable_op_nolock(state->adds,kno_table_add,(lispval)&pair,kv->kv_val);
  return 0;
}

static int merge_keys_for_slotids(struct KNO_KEYVAL *kv,void *data)
{
  struct KEYSLOT_STATE *state = data;
  lispval key = kv->kv_key;
  lispval keyslot = state->keyslot;
  if ( (KNO_PAIRP(key)) &&
       (kno_choice_containsp(KNO_CAR(key),keyslot)) )
    kno_hashtable_op_nolock(state->adds,kno_table_add,key,kv->kv_val);
  return 0;
}

static int merge_keys_without_slotid(struct KNO_KEYVAL *kv,void *data)
{
  struct KEYSLOT_STATE *state = data;
  lispval key = kv->kv_key;
  if (KNO_PAIRP(key)) {
    if ((KNO_CAR(key)) == state->keyslot )
      kno_hashtable_op_nolock(state->adds,kno_table_add,KNO_CDR(key),kv->kv_val);
    return 0;}
  else return 0;
}

#define SINGLE_KEYSLOTP(x) ( (KNO_OIDP(x)) || (KNO_SYMBOLP(x)) )

KNO_EXPORT int kno_slotindex_merge(kno_index into,lispval from)
{
  lispval keyslot = into->index_keyslot;
  if (KNO_HASHTABLEP(from)) {
    if (KNO_VOIDP(keyslot))
      return kno_index_merge(into,(kno_hashtable)from);
    else if (SINGLE_KEYSLOTP(keyslot)) {
      /* If we merge a hashtable into a single-keyslot index, we just
	 add the values directly. */
      struct KEYSLOT_STATE state = { (kno_hashtable)from, keyslot };
      int rv = kno_for_hashtable_kv
	((kno_hashtable)from,merge_keys_without_slotid,&state,1);
      return rv;}
    else  {
      /* If we merge a hashtable into an index with keyslots, we merge
	 only the pairs whose cars (the slotkeys) are in the target's
	 slotkeys. */
      struct KEYSLOT_STATE state = { (kno_hashtable)from, keyslot };
      int rv = kno_for_hashtable_kv
	((kno_hashtable)from,merge_keys_for_slotids,&state,1);
      return rv;}}
  else if ( (SINGLE_KEYSLOTP(keyslot)) && (KNO_INDEXP(from)) ) {
    kno_index ix = kno_indexptr(from);
    if (kno_tempindexp(ix)) {
      if ( (KNO_VOIDP(ix->index_keyslot)) ||
	   ( (KNO_AMBIGP(ix->index_keyslot)) &&
	     (kno_choice_containsp(keyslot,ix->index_keyslot)) ) ) {
	struct KEYSLOT_STATE state = { &ix->index_adds, keyslot };
	/* If the input index doesn't have any keyslots, then it
	   contains (slot . value) keys and we merge just the values
	   for entries whose slot matches the target's keyslot). */
	int rv = kno_for_hashtable_kv
	  (&(into->index_adds),merge_keys_without_slotid,&state,1);
	return rv;}
      else if ( ix->index_keyslot == keyslot ) {
	/* If the target index and the subject index have exactly the
	   same keyslots, do a straight merge. */
	return kno_index_merge(into,&(ix->index_adds));}
      else {
	/* In this case, the subject index doesn't have any keys in
	   which we're interested. */
	return 0;}}
    else {
      kno_seterr("Not a hashtable or tempindex","kno_slotindex_merge",
		into->indexid,from);
      return -1;}}
  else if ( (FALSISH(keyslot)) && (KNO_INDEXP(from)) ) {
    /* This is where we just copy any key value pairs we find. */
    kno_index ix = kno_indexptr(from);
    if (kno_tempindexp(ix)) {
      if (SINGLE_KEYSLOTP(ix->index_keyslot)) {
	struct KEYSLOT_STATE state = { &ix->index_adds, ix->index_keyslot };
	int rv = kno_for_hashtable_kv
	  (&(into->index_adds),merge_keys_with_slotid,&state,1);
	return rv;}
      else return kno_index_merge(into,&(ix->index_adds));}
    else if (kno_aggregate_indexp(ix)) {
      int merged = 0;
      struct KNO_AGGREGATE_INDEX *agg = (kno_aggregate_index) from;
      kno_index *indexes = agg->ax_indexes;
      int i=0, n=agg->ax_n_indexes; while (i<n) {
	kno_index partition = indexes[i++];
	if (partition->index_adds.table_n_keys == 0)
	  continue;
	lispval partition_ptr = index2lisp(partition);
	merged=kno_slotindex_merge(into,partition_ptr);
	if (merged<0) {
	  kno_seterr("IndexMergeError","kno_slotindex_merge",
		    into->indexid,partition_ptr);
	  kno_decref(partition_ptr);
	  break;}
	else kno_decref(partition_ptr);}
      return merged;}
    else {
      kno_seterr("IndexMergeError","kno_slotindex_merge",
		into->indexid,from);
      return -1;}}
  else if (KNO_INDEXP(from)) {
    /* Here, the keyslot is a choice of slotids */
    kno_index ix = kno_indexptr(from);
    if (kno_tempindexp(ix)) {
      if ( (SINGLE_KEYSLOTP(ix->index_keyslot)) &&
	   (kno_choice_containsp(ix->index_keyslot,keyslot)) ) {
	struct KEYSLOT_STATE state = { &ix->index_adds, ix->index_keyslot };
	int rv = kno_for_hashtable_kv
	  (&(into->index_adds),merge_keys_with_slotid,&state,1);
	return rv;}
      else if ( (KNO_AMBIGP(ix->index_keyslot)) ) {
	lispval v[2] = { ix->index_keyslot, keyslot };
	lispval common = kno_intersection(v,2);
	if (!(KNO_EMPTYP(common))) {
	  struct KEYSLOT_STATE state = { &ix->index_adds, common };
	  int rv = kno_for_hashtable_kv
	    (&(into->index_adds),merge_keys_for_slotids,&state,1);
	  kno_decref(common);
	  return rv;}
	return 0;}
      else return kno_index_merge(into,&(ix->index_adds));}
    else if (kno_aggregate_indexp(ix)) {
      int merged = 0;
      struct KNO_AGGREGATE_INDEX *agg = (kno_aggregate_index) from;
      kno_index *indexes = agg->ax_indexes;
      int i=0, n=agg->ax_n_indexes; while (i<n) {
	kno_index partition = indexes[i++];
	lispval pkeyslot = partition->index_keyslot;
	if (partition->index_adds.table_n_keys == 0)
	  continue;
	else if ( (SINGLE_KEYSLOTP(pkeyslot)) &&
		  (! (kno_choice_containsp(pkeyslot,keyslot)) ) )
	  continue;
	else if ( (KNO_AMBIGP(pkeyslot)) &&
		  (! (kno_overlapp(pkeyslot,keyslot)) ) )
	  continue;
	else NO_ELSE;
	/* Call merge on into and the partition */
	lispval partition_ptr = index2lisp(partition);
	merged=kno_slotindex_merge(into,partition_ptr);
	if (merged<0) {
	  kno_seterr("IndexMergeError","kno_slotindex_merge",
		    into->indexid,partition_ptr);
	  kno_decref(partition_ptr);
	  break;}
	else kno_decref(partition_ptr);}
      return merged;}
    else {
      kno_seterr("IndexMergeError","kno_slotindex_merge",
		into->indexid,from);
      return -1;}}
  else {
    kno_seterr("Not a hashtable or tempindex","kno_slotindex_merge",
	      into->indexid,from);
    return -1;}
}

/* Background searching */

KNO_EXPORT lispval kno_bg_get(lispval slotid,lispval value)
{
  return aggregate_prim_find(kno_default_background,slotid,value);
}

KNO_EXPORT lispval kno_bgfinder(int n,kno_argvec slotvals)
{
  return kno_finder(index2lisp((kno_index)kno_default_background),n,slotvals);
}

KNO_EXPORT lispval kno_bgfind(lispval slotid,lispval values,...)
{
    int n_args = 2;
    va_list args, scan; va_start(args,values);
    lispval arg = va_arg(args,lispval);
    while (!(VOIDP(arg))) {
      n_args++;
      arg = va_arg(args,lispval);}
    if (n_args == 0) {
      lispval slotvals[2] = {slotid,values};
      return kno_bgfinder(2,slotvals);}
    else if (n_args%2)
      return kno_err(OddFindFramesArgs,"kno_bgfind",NULL,VOID);
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
      return kno_bgfinder(n_args,slotvals);}
}

KNO_EXPORT int kno_bg_prefetch(lispval keys)
{
  return kno_index_prefetch((kno_index)kno_default_background,keys);
}

KNO_EXPORT void kno_init_slotindexes_c()
{
  u8_register_source_file(_FILEINFO);
}
