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

static struct FD_INDEX_HANDLER compoundindex_handler;

static lispval compound_fetch(fd_index ix,lispval key)
{
  struct FD_COMPOUND_INDEX *cix = (struct FD_COMPOUND_INDEX *)ix;
  lispval combined = EMPTY;
  int i = 0, lim;
  fd_lock_index(cix);
  lim = cix->cx_n_indexes;
  while (i < lim) {
    fd_index each = cix->cx_indexes[i++];
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
      fd_decref(combined); fd_unlock_index(cix);
      return value;}
    else {CHOICE_ADD(combined,value);}}
  fd_unlock_index(cix);
  return combined;
}

static int compound_prefetch(fd_index ix,lispval keys)
{
  int n_fetches = 0, i = 0, lim, n = FD_CHOICE_SIZE(keys);
  struct FD_COMPOUND_INDEX *cix = (struct FD_COMPOUND_INDEX *)ix;
  lispval *keyv = u8_big_alloc_n(n,lispval);
  lispval *valuev = u8_big_alloc_n(n,lispval);
  DO_CHOICES(key,keys)
    if (!(fd_hashtable_probe(&(cix->index_cache),key))) {
      keyv[n_fetches]=key;
      valuev[n_fetches]=EMPTY;
      n_fetches++;}
  if (n_fetches==0) {
    u8_free(keyv); u8_free(valuev);
    return 0;}
  fd_lock_index(cix);
  lim = cix->cx_n_indexes;
  while (i < lim) {
    int j = 0; fd_index each = cix->cx_indexes[i];
    lispval *values=each->index_handler->fetchn(each,n_fetches,keyv);
    if (values == NULL) {
      u8_big_free(keyv); u8_big_free(valuev);
      fd_unlock_index(cix);
      return -1;}
    while (j<n_fetches) {
      CHOICE_ADD(valuev[j],values[j]); j++;}
    u8_big_free(values);
    i++;}
  fd_unlock_index(cix);
  i = 0; while (i<n_fetches)
           if (PRECHOICEP(valuev[i])) {
             valuev[i]=fd_simplify_choice(valuev[i]); i++;}
           else i++;
  /* The operation fd_table_add_empty_noref will create an entry even
     if the value is the empty choice. */
  fd_hashtable_iter(&(cix->index_cache),fd_table_add_empty_noref,
                    n_fetches,keyv,valuev);
  u8_big_free(keyv);
  u8_big_free(valuev);
  return n_fetches;
}

static lispval *compound_fetchn(fd_index ix,int n,const lispval *keys)
{
  int n_fetches = 0, i = 0, lim;
  struct FD_COMPOUND_INDEX *cix = (struct FD_COMPOUND_INDEX *)ix;
  lispval *keyv = u8_big_alloc_n(n,lispval);
  lispval *valuev = u8_big_alloc_n(n,lispval);
  unsigned int *posmap = u8_big_alloc_n(n,unsigned int);
  const lispval *scan = keys, *limit = keys+n;
  while (scan<limit) {
    int off = scan-keys; lispval key = *scan++;
    if (!(fd_hashtable_probe(&(cix->index_cache),key))) {
      keyv[n_fetches]=key;
      valuev[n_fetches]=EMPTY;
      posmap[n_fetches]=off;
      n_fetches++;}
    else valuev[scan-keys]=fd_hashtable_get(&(cix->index_cache),key,EMPTY);}
  if (n_fetches==0) {
    u8_big_free(keyv); u8_big_free(posmap);
    return valuev;}
  fd_lock_index(cix);
  lim = cix->cx_n_indexes;
  while (i < lim) {
    int j = 0; fd_index each = cix->cx_indexes[i];
    lispval *values=each->index_handler->fetchn(each,n_fetches,keyv);
    if (values == NULL) {
      u8_big_free(keyv);
      u8_big_free(posmap);
      u8_big_free(valuev);
      fd_unlock_index(cix);
      return NULL;}
    while (j<n_fetches) {
      CHOICE_ADD(valuev[posmap[j]],values[j]); j++;}
    u8_big_free(values);
    i++;}
  fd_unlock_index(cix);
  i = 0; while (i<n_fetches)
           if (PRECHOICEP(valuev[i])) {
             valuev[i]=fd_simplify_choice(valuev[i]); i++;}
           else i++;
  /* The operation fd_table_add_empty_noref will create an entry even
     if the value is the empty choice. */
  u8_big_free(keyv); u8_big_free(posmap);
  return valuev;
}

static lispval *compound_fetchkeys(fd_index ix,int *n)
{
  struct FD_COMPOUND_INDEX *cix = (struct FD_COMPOUND_INDEX *)ix;
  lispval combined = EMPTY;
  int i = 0, lim;
  fd_lock_index(cix);
  lim = cix->cx_n_indexes;
  while (i < lim) {
    fd_index each = cix->cx_indexes[i++];
    lispval keys = fd_index_keys(each);
    if (FD_ABORTP(keys)) {
      fd_decref(combined);
      fd_unlock_index(cix);
      fd_interr(keys);
      return NULL;}
    else {CHOICE_ADD(combined,keys);}}
  fd_unlock_index(cix);
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

static u8_string get_compound_id(int n,fd_index *indexes)
{
  if (n) {
    struct U8_OUTPUT out; int i = 0;
    U8_INIT_OUTPUT(&out,80);
    while (i < n) {
      if (i) u8_puts(&out,"|"); else u8_puts(&out,"{");
      u8_puts(&out,indexes[i]->indexid); i++;}
    u8_puts(&out,"}");
    return out.u8_outbuf;}
  else return u8_strdup("compound");
}

FD_EXPORT fd_index fd_make_compound_index(int n_allocd,int cx_n_indexes,fd_index *indexes)
{
  struct FD_COMPOUND_INDEX *cix = u8_alloc(struct FD_COMPOUND_INDEX);
  u8_string cid = get_compound_id(cx_n_indexes,indexes);
  fd_init_index((fd_index)cix,&compoundindex_handler,cid,NULL,0,
                FD_VOID,FD_VOID);
  u8_init_mutex(&(cix->index_lock));
  u8_free(cid);
  cix->cx_n_allocd = n_allocd;
  cix->cx_n_indexes = cx_n_indexes;
  cix->cx_indexes = indexes;
  cix->cx_oldvecs = NULL;
  fd_register_index((fd_index)cix);
  return (fd_index) cix;
}

FD_EXPORT int fd_add_to_compound_index(fd_compound_index cix,fd_index add)
{
  if (cix->index_handler == &compoundindex_handler) {
    int i = 0, n = cix->cx_n_indexes;
    while (i < n)
      if (cix->cx_indexes[i] == add)
        return 0;
      else i++;
    fd_lock_index(cix);
    if ( (cix->cx_n_allocd == 0) || (cix->cx_indexes == NULL) ) {
      int size = 4;
      fd_index *new = u8_realloc_n(cix->cx_indexes,size,fd_index);
      if (new) {
        cix->cx_indexes = new;
        cix->cx_n_allocd = size;
        cix->cx_n_indexes = 0;}
      else {
        fd_unlock_index(cix);
        u8_seterr("CouldntGrowIndex","fd_add_to_compound_index",
                  u8_strdup(cix->indexid));
        return -1;}}
    else if (cix->cx_n_indexes >= cix->cx_n_allocd) {
      int allocd = cix->cx_n_allocd;
      int reallocd = allocd * 2;
      fd_index *new = u8_realloc_n(cix->cx_indexes,reallocd,fd_index);
      if (new) {
        cix->cx_oldvecs = u8_cons_list(cix->cx_indexes,cix->cx_oldvecs,0);
        cix->cx_indexes = new;
        cix->cx_n_allocd = reallocd;}
      else {
        fd_unlock_index(cix);
        u8_seterr("CouldntGrowIndex","fd_add_to_compound_index",
                  u8_strdup(cix->indexid));
        return -1;}}
    else NO_ELSE;

    cix->cx_indexes[cix->cx_n_indexes++]=add;
    if ( (cix->cx_front == NULL) && (!(FD_INDEX_READONLYP(add))) )
      cix->cx_front=add;

    if (add->index_serialno<0) {
      lispval alix = (lispval)add;
      fd_incref(alix);}

    u8_string old_source = cix->index_source;
    u8_string old_id = cix->indexid;
    cix->index_source = get_compound_id(cix->cx_n_indexes,cix->cx_indexes);
    if ( ( old_source ) && ( old_source != old_id) )
      u8_free(old_source);
    if (old_id) {
      u8_byte *brace=strchr(old_id,'{');
      if (brace) {
        size_t prefix_len=brace-old_id;
        u8_byte buf[prefix_len+1];
        strncpy(buf,old_id,prefix_len); buf[prefix_len]='\0';
        cix->indexid = u8_mkstring("%s%s",buf,cix->index_source);}
      else cix->indexid = u8_mkstring("%s%s",old_id,cix->index_source);
      u8_free(old_id);}
    else cix->indexid = u8_mkstring("compound%s",cix->index_source);

    /* Invalidate the cache now that there's a new source */
    fd_reset_hashtable(&(cix->index_cache),-1,1);
    fd_unlock_index(cix);
    return 1;}
  else return fd_reterr(fd_TypeError,("compound_index"),NULL,
                        fd_index2lisp((fd_index)cix));
}

/* CTL */

static lispval front_symbol, partitions_symbol;

static lispval compound_ctl(fd_index ix,lispval op,int n,lispval *args)
{
  struct FD_COMPOUND_INDEX *cx = (struct FD_COMPOUND_INDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return fd_err("BadIndexOpCall","hashindex_ctl",
                  cx->indexid,VOID);
  else if ( op == front_symbol) {
    if (n == 0) {
      if (cx->cx_front)
        return fd_index2lisp(cx->cx_front);
      else return FD_FALSE;}
    else if (n == 1) {
      fd_index front = fd_lisp2index(args[0]);
      if (front == NULL) return FD_ERROR;
      if  (FD_INDEX_READONLYP(front)) {
        fd_seterr("ReadOnlyIndex","compound_index_ctl/front",cx->indexid,
                  fd_index2lisp(front));
        return FD_ERROR;}
      int added = fd_add_to_compound_index(cx,front);
      if (added<0) return FD_ERROR;
      cx->cx_front = front;
      return fd_index2lisp(front);}
    else return fd_err(fd_TooManyArgs,
                       "compound_index_ctl/front",cx->indexid,
                       VOID);}
  else if (op == partitions_symbol) {
    if (n == 0) {
      lispval result = EMPTY;
      fd_index *indexes = cx->cx_indexes;
      int i = 0; while (i<cx->cx_n_indexes) {
        fd_index ix = indexes[i++];
        lispval lix = fd_index2lisp(ix);
        fd_incref(lix);
        CHOICE_ADD(result,lix);}
      return result;}
    else if (n == 1) {
      fd_index front = fd_lisp2index(args[0]);
      int rv = fd_add_to_compound_index(cx,front);
      if (rv<0) return FD_ERROR;
      else return FD_INT(cx->cx_n_indexes);}
    else return fd_err(fd_TooManyArgs,"compound_index_ctl/paritions",
                       cx->indexid,VOID);}
  else return fd_default_indexctl(ix,op,n,args);
}

static void recycle_compound_index(fd_index ix)
{
  if (ix->index_serialno>=0) return;
  struct FD_COMPOUND_INDEX *cx = (struct FD_COMPOUND_INDEX *) ix;
  fd_index *indexes = cx->cx_indexes;
  int i = 0, n = cx->cx_n_indexes;
  while (i < n) {
    fd_index each = indexes[i++];
    if (each->index_serialno<0) {
      lispval lix = (lispval) each;
      fd_decref(lix);}}
  cx->cx_indexes = NULL;
  cx->cx_n_indexes = cx->cx_n_allocd = 0;
  if (cx->cx_oldvecs) u8_free_list(cx->cx_oldvecs);
}

static struct FD_INDEX_HANDLER compoundindex_handler={
  "compoundindex", 1, sizeof(struct FD_COMPOUND_INDEX), 14,
  NULL, /* close */
  NULL, /* commit */
  compound_fetch, /* fetch */
  NULL, /* fetchsize */
  compound_prefetch, /* prefetch */
  compound_fetchn, /* fetchn */
  compound_fetchkeys, /* fetchkeys */
  NULL, /* fetchinfo */
  NULL, /* batchadd */
  NULL, /* create */
  NULL, /* walk */
  recycle_compound_index, /* recycle */
  compound_ctl /* indexctl */
};

FD_EXPORT void fd_init_compoundindexes_c()
{
  u8_register_source_file(_FILEINFO);
  front_symbol = fd_intern("FRONT");
  partitions_symbol = fd_intern("PARTITIONS");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
