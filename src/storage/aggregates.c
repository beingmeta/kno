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
#include "framerd/storage.h"
#include "framerd/apply.h"

#include <libu8/libu8.h>
#include <libu8/u8memlist.h>
#include <libu8/u8printf.h>

static struct FD_INDEX_HANDLER aggregate_index_handler;

static lispval aggregate_fetch(fd_index ix,lispval key)
{
  struct FD_AGGREGATE_INDEX *aix = (struct FD_AGGREGATE_INDEX *)ix;
  lispval combined = EMPTY;
  int i = 0, lim;
  fd_lock_index(aix);
  lim = aix->ax_n_indexes;
  while (i < lim) {
    fd_index each = aix->ax_indexes[i++];
    lispval value;
    if (each->index_cache_level<0) {
      each->index_cache_level = fd_default_cache_level;
      fd_index_setcache(each,fd_default_cache_level);}
    if (fd_hashtable_probe(&(each->index_cache),key))
      value = fd_hashtable_get(&(each->index_cache),key,EMPTY);
    else if ((each->index_adds.table_n_keys) ||
             (each->index_drops.table_n_keys) ||
             (each->index_stores.table_n_keys))
      value = fd_index_get(each,key);
    else value = each->index_handler->fetch(each,key);
    if (FD_ABORTP(value)) {
      fd_decref(combined); fd_unlock_index(aix);
      return value;}
    else {CHOICE_ADD(combined,value);}}
  fd_unlock_index(aix);
  return combined;
}

static int aggregate_prefetch(fd_index ix,lispval keys)
{
  int n_fetches = 0, i = 0, lim, n = FD_CHOICE_SIZE(keys);
  struct FD_AGGREGATE_INDEX *aix = (struct FD_AGGREGATE_INDEX *)ix;
  lispval *keyv = u8_big_alloc_n(n,lispval);
  lispval *valuev = u8_big_alloc_n(n,lispval);
  DO_CHOICES(key,keys)
    if (!(fd_hashtable_probe(&(aix->index_cache),key))) {
      keyv[n_fetches]=key;
      valuev[n_fetches]=EMPTY;
      n_fetches++;}
  if (n_fetches==0) {
    u8_big_free(keyv);
    u8_big_free(valuev);
    return 0;}
  fd_lock_index(aix);
  lim = aix->ax_n_indexes;
  while (i < lim) {
    int j = 0; fd_index each = aix->ax_indexes[i];
    lispval *values=each->index_handler->fetchn(each,n_fetches,keyv);
    if (values == NULL) {
      u8_big_free(keyv);
      u8_big_free(valuev);
      fd_unlock_index(aix);
      return -1;}
    while (j<n_fetches) {
      CHOICE_ADD(valuev[j],values[j]); j++;}
    u8_big_free(values);
    i++;}
  fd_unlock_index(aix);
  i = 0; while (i<n_fetches) {
           if (PRECHOICEP(valuev[i])) {
             valuev[i]=fd_simplify_choice(valuev[i]);}
           i++;}
  /* The operation fd_table_add_empty_noref will create an entry even
     if the value is the empty choice. */
  fd_hashtable_iter(&(aix->index_cache),fd_table_add_empty_noref,
                    n_fetches,keyv,valuev);
  u8_big_free(keyv);
  u8_big_free(valuev);
  return n_fetches;
}

static lispval *aggregate_fetchn(fd_index ix,int n,const lispval *keys)
{
  int n_fetches = 0, i = 0, lim;
  struct FD_AGGREGATE_INDEX *aix = (struct FD_AGGREGATE_INDEX *)ix;
  lispval *keyv = u8_big_alloc_n(n,lispval);
  lispval *valuev = u8_big_alloc_n(n,lispval);
  unsigned int *posmap = u8_big_alloc_n(n,unsigned int);
  const lispval *scan = keys, *limit = keys+n;
  while (scan<limit) {
    int off = scan-keys; lispval key = *scan++;
    if (!(fd_hashtable_probe(&(aix->index_cache),key))) {
      keyv[n_fetches]=key;
      valuev[n_fetches]=EMPTY;
      posmap[n_fetches]=off;
      n_fetches++;}
    else valuev[scan-keys]=fd_hashtable_get(&(aix->index_cache),key,EMPTY);}
  if (n_fetches==0) {
    u8_big_free(keyv);
    u8_big_free(posmap);
    return valuev;}
  fd_lock_index(aix);
  lim = aix->ax_n_indexes;
  while (i < lim) {
    int j = 0; fd_index each = aix->ax_indexes[i];
    lispval *values=each->index_handler->fetchn(each,n_fetches,keyv);
    if (values == NULL) {
      u8_big_free(keyv);
      u8_big_free(posmap);
      u8_big_free(valuev);
      fd_unlock_index(aix);
      return NULL;}
    while (j<n_fetches) {
      CHOICE_ADD(valuev[posmap[j]],values[j]);
      j++;}
    u8_big_free(values);
    i++;}
  fd_unlock_index(aix);
  i = 0; while (i<n_fetches) {
           if (PRECHOICEP(valuev[i])) {
             valuev[i]=fd_simplify_choice(valuev[i]);}
           i++;}
  /* The operation fd_table_add_empty_noref will create an entry even
     if the value is the empty choice. */
  u8_big_free(keyv);
  u8_big_free(posmap);
  return valuev;
}

static lispval *aggregate_fetchkeys(fd_index ix,int *n)
{
  struct FD_AGGREGATE_INDEX *aix = (struct FD_AGGREGATE_INDEX *)ix;
  lispval combined = EMPTY;
  int i = 0, lim;
  fd_lock_index(aix);
  lim = aix->ax_n_indexes;
  while (i < lim) {
    fd_index each = aix->ax_indexes[i++];
    lispval keys = fd_index_keys(each);
    if (FD_ABORTP(keys)) {
      fd_decref(combined);
      fd_unlock_index(aix);
      fd_interr(keys);
      return NULL;}
    else {CHOICE_ADD(combined,keys);}}
  fd_unlock_index(aix);
  {
    lispval simple = fd_simplify_choice(combined);
    int n_elts = FD_CHOICE_SIZE(simple);
    lispval *results = ((n>0) ? (u8_big_alloc_n(n_elts,lispval)) : (NULL));
    if (n_elts==0) {
      *n = 0;
      return results;}
    else if (n_elts==1) {
      results[0]=simple;
      *n = 1;
      return results;}
    else if (FD_CONS_REFCOUNT((fd_cons)simple)==1) {
      int j=0;
      DO_CHOICES(key,simple) {
        results[j]=key;
        j++;}
      fd_free_choice((fd_choice)simple);
      *n = j;
      return results;}
    else {
      int j=0;
      DO_CHOICES(key,simple) {
        results[j]=fd_incref(key);
        j++;}
      fd_decref(simple);
      *n = j;
      return results;}
  }
}

FD_EXPORT fd_aggregate_index fd_make_aggregate_index
(lispval opts,int n_allocd,int n,fd_index *indexes)
{
  struct FD_AGGREGATE_INDEX *aix = u8_alloc(struct FD_AGGREGATE_INDEX);
  lispval metadata = fd_getopt(opts,FDSYM_METADATA,FD_VOID);
  fd_storage_flags flags =
    fd_get_dbflags(opts,FD_STORAGE_ISINDEX|FD_STORAGE_READ_ONLY);
  fd_init_index((fd_index)aix,&aggregate_index_handler,
                "new-aggregate",NULL,NULL,
                flags,opts,metadata);
  if (n_allocd < n) n_allocd = n;
  u8_init_mutex(&(aix->index_lock));
  aix->ax_n_allocd = n_allocd;
  aix->ax_n_indexes = 0;
  aix->ax_indexes = u8_alloc_n(n_allocd,fd_index);
  aix->ax_oldvecs = NULL;
  int i = 0; while (i < n) {
    fd_index add = indexes[i++];
    if (add) fd_add_to_aggregate_index(aix,add);}
  fd_register_index((fd_index)aix);
  return aix;
}

FD_EXPORT int fd_add_to_aggregate_index(fd_aggregate_index aix,fd_index add)
{
  if (aix->index_handler == &aggregate_index_handler) {
    int i = 0, n = aix->ax_n_indexes;
    while (i < n)
      if (aix->ax_indexes[i] == add)
        return 0;
      else i++;
    fd_lock_index(aix);
    if ( (aix->ax_n_allocd == 0) || (aix->ax_indexes == NULL) ) {
      int size = 4;
      fd_index *new = u8_realloc_n(aix->ax_indexes,size,fd_index);
      if (new) {
        aix->ax_indexes = new;
        aix->ax_n_allocd = size;
        aix->ax_n_indexes = 0;}
      else {
        fd_unlock_index(aix);
        u8_seterr("CouldntGrowIndex","fd_add_to_aggregate_index",
                  u8_strdup(aix->indexid));
        return -1;}}
    else if (aix->ax_n_indexes >= aix->ax_n_allocd) {
      int allocd = aix->ax_n_allocd;
      int reallocd = allocd * 2;
      fd_index *new = u8_realloc_n(aix->ax_indexes,reallocd,fd_index);
      if (new) {
        aix->ax_oldvecs = u8_cons_list(aix->ax_indexes,aix->ax_oldvecs,0);
        aix->ax_indexes = new;
        aix->ax_n_allocd = reallocd;}
      else {
        fd_unlock_index(aix);
        u8_seterr("CouldntGrowIndex","fd_add_to_aggregate_index",
                  u8_strdup(aix->indexid));
        return -1;}}
    else NO_ELSE;

    aix->ax_indexes[aix->ax_n_indexes++]=add;

    if (add->index_serialno<0) {
      lispval alix = (lispval)add;
      fd_incref(alix);}

    u8_string old_source = aix->index_source;

    u8_string old_id = aix->indexid;
    aix->indexid = u8_mkstring("%s+%d",
                               aix->ax_indexes[0]->indexid,
                               (aix->ax_n_indexes-1));
    if (old_id) u8_free(old_id);

    struct U8_OUTPUT sourceout;
    U8_INIT_OUTPUT(&sourceout,128);
    int j = 0; while (j < aix->ax_n_indexes) {
      fd_index each = aix->ax_indexes[j];
      if (j>0) u8_putc(&sourceout,'\n');
      if (each->index_source)
        u8_puts(&sourceout,each->index_source);
      else if (each->indexid)
        u8_puts(&sourceout,each->indexid);
      else NO_ELSE;
      j++;}
    aix->index_source=sourceout.u8_outbuf;
    if (old_source) u8_free(old_source);

    /* Invalidate the cache now that there's a new source */
    fd_reset_hashtable(&(aix->index_cache),-1,1);
    fd_unlock_index(aix);
    return 1;}
  else return fd_reterr(fd_TypeError,("aggregate_index"),NULL,
                        fd_index2lisp((fd_index)aix));
}

/* CTL */

static lispval partitions_symbol;

static lispval aggregate_ctl(fd_index ix,lispval op,int n,lispval *args)
{
  struct FD_AGGREGATE_INDEX *cx = (struct FD_AGGREGATE_INDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return fd_err("BadIndexOpCall","hashindex_ctl",
                  cx->indexid,VOID);
  else if (op == partitions_symbol) {
    if (n == 0) {
      lispval result = EMPTY;
      fd_index *indexes = cx->ax_indexes;
      int i = 0; while (i<cx->ax_n_indexes) {
        fd_index ix = indexes[i++];
        lispval lix = fd_index2lisp(ix);
        fd_incref(lix);
        CHOICE_ADD(result,lix);}
      return result;}
    else if (n == 1) {
      fd_index front = fd_lisp2index(args[0]);
      int rv = fd_add_to_aggregate_index(cx,front);
      if (rv<0) return FD_ERROR;
      else return FD_INT(cx->ax_n_indexes);}
    else return fd_err(fd_TooManyArgs,"aggregate_index_ctl/paritions",
                       cx->indexid,VOID);}
  else return fd_default_indexctl(ix,op,n,args);
}

static void recycle_aggregate_index(fd_index ix)
{
  if (ix->index_serialno>=0) return;
  struct FD_AGGREGATE_INDEX *agg = (struct FD_AGGREGATE_INDEX *) ix;
  fd_index *indexes = agg->ax_indexes;
  int i = 0, n = agg->ax_n_indexes;
  while (i < n) {
    fd_index each = indexes[i++];
    if (each->index_serialno<0) {
      lispval lix = (lispval) each;
      fd_decref(lix);}}
  u8_free(agg->ax_indexes);
  agg->ax_indexes = NULL;
  agg->ax_n_indexes = agg->ax_n_allocd = 0;
  if (agg->ax_oldvecs) u8_free_list(agg->ax_oldvecs);
}

static struct FD_INDEX_HANDLER aggregate_index_handler={
  "aggregateindex", 1, sizeof(struct FD_AGGREGATE_INDEX), 14,
  NULL, /* close */
  NULL, /* commit */
  aggregate_fetch, /* fetch */
  NULL, /* fetchsize */
  aggregate_prefetch, /* prefetch */
  aggregate_fetchn, /* fetchn */
  aggregate_fetchkeys, /* fetchkeys */
  NULL, /* fetchinfo */
  NULL, /* batchadd */
  NULL, /* create */
  NULL, /* walk */
  recycle_aggregate_index, /* recycle */
  aggregate_ctl /* indexctl */
};
struct FD_INDEX_HANDLER *fd_aggregate_index_handler=&aggregate_index_handler;

FD_EXPORT int fd_aggregate_indexp(fd_index ix)
{
  return ( (ix) && (ix->index_handler == &aggregate_index_handler) );
}

FD_EXPORT void fd_init_aggregates_c()
{
  u8_register_source_file(_FILEINFO);
  partitions_symbol = fd_intern("PARTITIONS");

  fd_init_index(((fd_index)fd_background),
                &aggregate_index_handler,
                "background",NULL,NULL,
                FD_STORAGE_ISINDEX|FD_STORAGE_READ_ONLY,
                FD_FALSE,FD_FALSE);
  fd_background->ax_n_allocd = 64;
  fd_background->ax_n_indexes = 0;
  fd_background->ax_indexes = u8_alloc_n(64,fd_index);
  fd_background->ax_oldvecs = NULL;
  fd_register_index(((fd_index)fd_background));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
