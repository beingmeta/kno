/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
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
      if (FD_CHOICEP(values))
	fd_index_prefetch(ix,values);
      DO_CHOICES(value,values) {
	lispval result = fd_index_get(ix,value);
	if (FD_ABORTP(result)) {
	  FD_STOP_DO_CHOICES;
	  fd_decref(combined);
	  return result;}
	CHOICE_ADD(combined,result);}}
    else {
      DO_CHOICES(value,values) {
	lispval key = fd_make_pair(slotid,value);
	lispval result = fd_index_get(ix,key);
	if (FD_ABORTP(result)) {
	  FD_STOP_DO_CHOICES;
	  fd_decref(combined);
	  return result;}
	CHOICE_ADD(combined,result);
	fd_decref(key);}}}
  return combined;
}

static lispval aggregate_prim_find
(fd_aggregate_index ax,lispval slotids,lispval values)
{
  lispval combined = FD_EMPTY;
  int i = 0, n =ax->ax_n_indexes;
  fd_index *indexes = ax->ax_indexes;
  while (i<n) {
    fd_index ex = indexes[i++];
    lispval v = index_prim_find(ex,slotids,values);
    if (FD_ABORTP(v)) {
      fd_decref(combined);
      return v;}
    CHOICE_ADD(combined,v);}
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
  int i = 0, n_conjuncts = n/2;
  lispval _conjuncts[6], *conjuncts=_conjuncts, result;
  if (EMPTYP(indexes)) return EMPTY;
  if (n_conjuncts>6) conjuncts = u8_alloc_n(n_conjuncts,lispval);
  while (i < n_conjuncts) {
    conjuncts[i]=fd_prim_find(indexes,slotvals[i*2],slotvals[i*2+1]);
    if (FD_ABORTP(conjuncts[i])) {
      lispval error = conjuncts[i];
      int j = 0; while (j<i) {fd_decref(conjuncts[j]); j++;}
      if (conjuncts != _conjuncts) u8_free(conjuncts);
      return error;}
    if (EMPTYP(conjuncts[i])) {
      int j = 0; while (j<i) {fd_decref(conjuncts[j]); j++;}
      return EMPTY;}
    i++;}
  result = fd_intersection(conjuncts,n_conjuncts);
  i = 0; while (i < n_conjuncts) {
    lispval cj=_conjuncts[i++]; fd_decref(cj);}
  if (_conjuncts != conjuncts) u8_free(conjuncts);
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

static fd_index get_writable_slotindex(fd_index ix,lispval slotid)
{
  if (fd_aggregate_indexp(ix)) {
    fd_index writable = NULL, generic = NULL;
    struct FD_AGGREGATE_INDEX *aix = (fd_aggregate_index) ix;
    fd_index *indexes = aix->ax_indexes;
    int i = 0, n = aix->ax_n_indexes; while (i<n) {
      fd_index possible = indexes[i++];
      fd_index use_front = fd_get_writable_index(possible);
      if (use_front) {
	if (possible->index_keyslot == slotid) {
	  lispval ptr = fd_index2lisp(use_front);
	  if (FD_ABORTP(ptr))
	    return NULL;
	  else {
	    if (FD_CONSP(ptr)) {fd_incref(ptr);}
	    return (fd_index) possible;}}
	else if ( (FD_VOIDP(use_front->index_keyslot)) ||
		  (FD_FALSEP(use_front->index_keyslot)) ||
		  (FD_EMPTYP(use_front->index_keyslot)) ) {
	  if (generic == NULL) generic = use_front;
	  if (writable) writable = use_front;}
	else if (writable == NULL)
	  writable = use_front;
	else NO_ELSE;}}
    if (generic) {
      lispval ptr = fd_index2lisp(generic);
      if (FD_CONSP(ptr)) fd_incref(ptr);
      return generic;}
    else if (writable) {
      lispval ptr = fd_index2lisp(writable);
      if (FD_CONSP(ptr)) fd_incref(ptr);
      return writable;}
    else return NULL;}
  else return fd_get_writable_index(ix);
}


FD_EXPORT
int fd_index_frame(fd_index ix,lispval frames,lispval slotids,lispval values)
{
  int rv = 0;
  DO_CHOICES(slotid,slotids) {
    fd_index write_index = get_writable_slotindex(ix,slotid);
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
    lispval ptr = fd_index2lisp(write_index);
    fd_decref(ptr);
    if (rv<0) { FD_LOOP_BREAK(); }}
  return rv;
}

/* Background searching */

FD_EXPORT lispval fd_bg_get(lispval slotid,lispval value)
{
  if (fd_background) {
    lispval results = EMPTY;
    lispval features = make_features(slotid,value);
    if ((fd_prefetch) && (fd_ipeval_status()==0) &&
	(CHOICEP(features)) &&
	(fd_index_prefetch((fd_index)fd_background,features)<0)) {
      fd_decref(features); return FD_ERROR;}
    else {
      DO_CHOICES(feature,features) {
	lispval result = fd_index_get((fd_index)fd_background,feature);
	if (FD_ABORTP(result)) {
	  fd_decref(results); fd_decref(features);
	  return result;}
	else {CHOICE_ADD(results,result);}}
      fd_decref(features);}
    return fd_simplify_choice(results);}
  else return EMPTY;
}

FD_EXPORT lispval fd_bgfinder(int n,lispval *slotvals)
{
  int i = 0, n_conjuncts = n/2;
  lispval _conjuncts[6], *conjuncts=_conjuncts, result;
  if (fd_background == NULL) return EMPTY;
  if (n_conjuncts>6) conjuncts = u8_alloc_n(n_conjuncts,lispval);
  while (i < n_conjuncts) {
    _conjuncts[i]=fd_bg_get(slotvals[i*2],slotvals[i*2+1]); i++;}
  result = fd_intersection(conjuncts,n_conjuncts);
  i = 0; while (i < n_conjuncts) {
    lispval cj=_conjuncts[i++]; fd_decref(cj);}
  if (_conjuncts != conjuncts) u8_free(conjuncts);
  return result;
}

FD_EXPORT lispval fd_bgfind(lispval slotid,lispval values,...)
{
  lispval _slotvals[64], *slotvals=_slotvals, val;
  int n_slotvals = 0, max_slotvals = 64; va_list args;
  va_start(args,values); val = va_arg(args,lispval);
  slotvals[n_slotvals++]=slotid; slotvals[n_slotvals++]=values;
  while (!(VOIDP(val))) {
    if (n_slotvals>=max_slotvals) {
      if (max_slotvals == 64) {
	lispval *newsv = u8_alloc_n(128,lispval); int i = 0;
	while (i<64) {newsv[i]=slotvals[i]; i++;}
	slotvals = newsv; max_slotvals = 128;}
      else {
	slotvals = u8_realloc_n(slotvals,max_slotvals*2,lispval);
	max_slotvals = max_slotvals*2;}}
    slotvals[n_slotvals++]=val; val = va_arg(args,lispval);}
  if (n_slotvals%2) {
    if (slotvals != _slotvals) u8_free(slotvals);
    return fd_err(OddFindFramesArgs,"fd_bgfind",NULL,VOID);}
  if (slotvals != _slotvals) {
    lispval result = fd_bgfinder(n_slotvals,slotvals);
    u8_free(slotvals);
    return result;}
  else return fd_bgfinder(n_slotvals,slotvals);
}

FD_EXPORT int fd_bg_prefetch(lispval keys)
{
  if (fd_background)
    return fd_index_prefetch((fd_index)fd_background,keys);
  else return 0;
}

FD_EXPORT void fd_init_slotindexes_c()
{
  u8_register_source_file(_FILEINFO);
}
