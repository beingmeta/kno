/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
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
    u8_free(keyv); u8_free(valuev);
    return 0;}
  fd_lock_index(aix);
  lim = aix->ax_n_indexes;
  while (i < lim) {
    int j = 0; fd_index each = aix->ax_indexes[i];
    lispval *values=each->index_handler->fetchn(each,n_fetches,keyv);
    if (values == NULL) {
      u8_big_free(keyv); u8_big_free(valuev);
      fd_unlock_index(aix);
      return -1;}
    while (j<n_fetches) {
      CHOICE_ADD(valuev[j],values[j]); j++;}
    u8_big_free(values);
    i++;}
  fd_unlock_index(aix);
  i = 0; while (i<n_fetches)
           if (PRECHOICEP(valuev[i])) {
             valuev[i]=fd_simplify_choice(valuev[i]); i++;}
           else i++;
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
    u8_big_free(keyv); u8_big_free(posmap);
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
      CHOICE_ADD(valuev[posmap[j]],values[j]); j++;}
    u8_big_free(values);
    i++;}
  fd_unlock_index(aix);
  i = 0; while (i<n_fetches)
           if (PRECHOICEP(valuev[i])) {
             valuev[i]=fd_simplify_choice(valuev[i]); i++;}
           else i++;
  /* The operation fd_table_add_empty_noref will create an entry even
     if the value is the empty choice. */
  u8_big_free(keyv); u8_big_free(posmap);
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
    int j = 0, n_elts = FD_CHOICE_SIZE(simple);
    lispval *results = ((n>0) ? (u8_alloc_n(n_elts,lispval)) : (NULL));
    if (n_elts==0) {
      *n = 0; return results;}
    else if (n_elts==1) {
      results[0]=simple; *n = 1;
      return results;}
    else if (FD_CONS_REFCOUNT((fd_cons)simple)==1) {
      DO_CHOICES(key,simple) {results[j]=key;}
      fd_free_choice((fd_choice)simple);
      *n = n_elts; return results;}
    else {
      DO_CHOICES(key,simple) {results[j]=fd_incref(key);}
      fd_decref(simple);
      *n = n_elts; return results;}
  }
}

static u8_string get_aggregate_id(int n,fd_index *indexes)
{
  if (n) {
    struct U8_OUTPUT out; int i = 0;
    U8_INIT_OUTPUT(&out,80);
    while (i < n) {
      if (i) u8_puts(&out,"|"); else u8_puts(&out,"{");
      u8_puts(&out,indexes[i]->indexid); i++;}
    u8_puts(&out,"}");
    return out.u8_outbuf;}
  else return u8_strdup("aggregate");
}

FD_EXPORT fd_index fd_make_aggregate_index
(int n_allocd,int cx_n_indexes,fd_index *indexes)
{
  struct FD_AGGREGATE_INDEX *aix = u8_alloc(struct FD_AGGREGATE_INDEX);
  u8_string cid = get_aggregate_id(cx_n_indexes,indexes);
  fd_init_index((fd_index)aix,&aggregate_index_handler,cid,NULL,
                FD_STORAGE_ISINDEX|FD_STORAGE_READ_ONLY,
                FD_VOID,FD_VOID);
  u8_init_mutex(&(aix->index_lock));
  u8_free(cid);
  aix->ax_n_allocd = n_allocd;
  aix->ax_n_indexes = cx_n_indexes;
  aix->ax_indexes = indexes;
  aix->ax_oldvecs = NULL;
  fd_register_index((fd_index)aix);
  return (fd_index) aix;
}

FD_EXPORT int fd_add_to_aggregate_index(fd_aggregate_index aix,fd_index add)
{
  if (aix->index_handler == &aggregate_index_handler) {
    int i = 0, n = aix->ax_n_indexes;
    while (i < n)
      if (aix->ax_indexes[i] == add)
        return 0;
      else i++;
    int read_only = FD_INDEX_READONLYP(add);
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
    aix->index_source = get_aggregate_id(aix->ax_n_indexes,aix->ax_indexes);
    if ( ( old_source ) && ( old_source != old_id) )
      u8_free(old_source);
    if (old_id) {
      u8_byte *brace=strchr(old_id,'{');
      if (brace) {
        size_t prefix_len=brace-old_id;
        u8_byte buf[prefix_len+1];
        strncpy(buf,old_id,prefix_len); buf[prefix_len]='\0';
        aix->indexid = u8_mkstring("%s%s",buf,aix->index_source);}
      else aix->indexid = u8_mkstring("%s%s",old_id,aix->index_source);
      u8_free(old_id);}
    else aix->indexid = u8_mkstring("aggregate%s",aix->index_source);

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
  struct FD_AGGREGATE_INDEX *cx = (struct FD_AGGREGATE_INDEX *) ix;
  fd_index *indexes = cx->ax_indexes;
  int i = 0, n = cx->ax_n_indexes;
  while (i < n) {
    fd_index each = indexes[i++];
    if (each->index_serialno<0) {
      lispval lix = (lispval) each;
      fd_decref(lix);}}
  cx->ax_indexes = NULL;
  cx->ax_n_indexes = cx->ax_n_allocd = 0;
  if (cx->ax_oldvecs) u8_free_list(cx->ax_oldvecs);
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

FD_EXPORT int fd_aggregate_indexp(fd_index ix)
{
  if ( (ix) && (ix->index_handler == &aggregate_index_handler) )
    return 1;
  else return 0;
}

FD_EXPORT void fd_init_aggregates_c()
{
  u8_register_source_file(_FILEINFO);
  partitions_symbol = fd_intern("PARTITIONS");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
