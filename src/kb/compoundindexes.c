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
#include "framerd/fdkbase.h"

#include <libu8/libu8.h>

static struct FD_INDEX_HANDLER compoundindex_handler;

static fdtype compound_fetch(fd_index ix,fdtype key)
{
  struct FD_COMPOUND_INDEX *cix=(struct FD_COMPOUND_INDEX *)ix;
  fdtype combined=FD_EMPTY_CHOICE;
  int i=0, lim;
  fd_lock_index(cix);
  lim=cix->n_indexes;
  while (i < lim) {
    fd_index eix=cix->indexes[i++];
    fdtype value;
    if (eix->index_cache_level<0) {
      eix->index_cache_level=fd_default_cache_level;
      fd_index_setcache(eix,fd_default_cache_level);}
    if (fd_hashtable_probe(&(eix->index_cache),key))
      value=fd_hashtable_get(&(eix->index_cache),key,FD_EMPTY_CHOICE);
    else if ((eix->index_adds.table_n_keys) || (eix->index_edits.table_n_keys))
      value=fd_index_get(eix,key);
    else value=eix->index_handler->fetch(eix,key);
    if (FD_ABORTP(value)) {
      fd_decref(combined); fd_unlock_index(cix);
      return value;}
    else {FD_ADD_TO_CHOICE(combined,value);}}
  fd_unlock_index(cix);
  return combined;
}

static int compound_prefetch(fd_index ix,fdtype keys)
{
  int n_fetches=0, i=0, lim, n=FD_CHOICE_SIZE(keys);
  struct FD_COMPOUND_INDEX *cix=(struct FD_COMPOUND_INDEX *)ix;
  fdtype *keyv=u8_alloc_n(n,fdtype);
  fdtype *valuev=u8_alloc_n(n,fdtype);
  FD_DO_CHOICES(key,keys)
    if (!(fd_hashtable_probe(&(cix->index_cache),key))) {
      keyv[n_fetches]=key; valuev[n_fetches]=FD_EMPTY_CHOICE; n_fetches++;}
  if (n_fetches==0) {
    u8_free(keyv); u8_free(valuev);
    return 0;}
  fd_lock_index(cix);
  lim=cix->n_indexes;
  while (i < lim) {
    int j=0; fd_index eix=cix->indexes[i];
    fdtype *values=
      eix->index_handler->fetchn(eix,n_fetches,keyv);
    if (values==NULL) {
      u8_free(keyv); u8_free(valuev);
      fd_unlock_index(cix);
      return -1;}
    while (j<n_fetches) {
      FD_ADD_TO_CHOICE(valuev[j],values[j]); j++;}
    u8_free(values);
    i++;}
  fd_unlock_index(cix);
  i=0; while (i<n_fetches)
    if (FD_ACHOICEP(valuev[i])) {
      valuev[i]=fd_simplify_choice(valuev[i]); i++;}
    else i++;
  /* The operation fd_table_add_empty_noref will create an entry even if the value
     is the empty choice. */
  fd_hashtable_iter(&(cix->index_cache),fd_table_add_empty_noref,n_fetches,keyv,valuev);
  u8_free(keyv); u8_free(valuev);
  return n_fetches;
}

static fdtype *compound_fetchn(fd_index ix,int n,fdtype *keys)
{
  int n_fetches=0, i=0, lim;
  struct FD_COMPOUND_INDEX *cix=(struct FD_COMPOUND_INDEX *)ix;
  fdtype *keyv=u8_alloc_n(n,fdtype);
  fdtype *valuev=u8_alloc_n(n,fdtype);
  unsigned int *posmap=u8_alloc_n(n,unsigned int);
  fdtype *scan=keys, *limit=keys+n;
  while (scan<limit) {
    int off=scan-keys; fdtype key=*scan++;
    if (!(fd_hashtable_probe(&(cix->index_cache),key))) {
      keyv[n_fetches]=key;
      valuev[n_fetches]=FD_EMPTY_CHOICE;
      posmap[n_fetches]=off;
      n_fetches++;}
    else valuev[scan-keys]=
           fd_hashtable_get(&(cix->index_cache),key,FD_EMPTY_CHOICE);}
  if (n_fetches==0) {
    u8_free(keyv); u8_free(posmap);
    return valuev;}
  fd_lock_index(cix);
  lim=cix->n_indexes;
  while (i < lim) {
    int j=0; fd_index eix=cix->indexes[i];
    fdtype *values=
      eix->index_handler->fetchn(eix,n_fetches,keyv);
    if (values==NULL) {
      u8_free(keyv); u8_free(posmap); u8_free(valuev);
      fd_unlock_index(cix);
      return NULL;}
    while (j<n_fetches) {
      FD_ADD_TO_CHOICE(valuev[posmap[j]],values[j]); j++;}
    u8_free(values);
    i++;}
  fd_unlock_index(cix);
  i=0; while (i<n_fetches)
    if (FD_ACHOICEP(valuev[i])) {
      valuev[i]=fd_simplify_choice(valuev[i]); i++;}
    else i++;
  /* The operation fd_table_add_empty_noref will create an entry even
     if the value is the empty choice. */
  u8_free(keyv); u8_free(posmap);
  return valuev;
}

static fdtype *compound_fetchkeys(fd_index ix,int *n)
{
  struct FD_COMPOUND_INDEX *cix=(struct FD_COMPOUND_INDEX *)ix;
  fdtype combined=FD_EMPTY_CHOICE;
  int i=0, lim;
  fd_lock_index(cix);
  lim=cix->n_indexes;
  while (i < lim) {
    fd_index eix=cix->indexes[i++];
    fdtype keys=fd_index_keys(eix);
    if (FD_ABORTP(keys)) {
      fd_decref(combined);
      fd_unlock_index(cix);
      fd_interr(keys);
      return NULL;}
    else {FD_ADD_TO_CHOICE(combined,keys);}}
  fd_unlock_index(cix);
  {
    fdtype simple=fd_simplify_choice(combined);
    int j=0, n_elts=FD_CHOICE_SIZE(simple);
    fdtype *results=((n>0) ? (u8_alloc_n(n_elts,fdtype)) : (NULL));
    if (n_elts==0) {
      *n=0; return results;}
    else if (n_elts==1) {
      results[0]=simple; *n=1;
      return results;}
    else if (FD_CONS_REFCOUNT((fd_cons)simple)==1) {
      FD_DO_CHOICES(key,simple) {results[j]=key;}
      u8_free((fd_cons)simple);
      *n=n_elts; return results;}
    else {
      FD_DO_CHOICES(key,simple) {results[j]=fd_incref(key);}
      fd_decref(simple);
      *n=n_elts; return results;}
  }
}

static u8_string get_compound_id(int n,fd_index *indexes)
{
  if (n) {
    struct U8_OUTPUT out; int i=0;
    U8_INIT_OUTPUT(&out,80);
    while (i < n) {
      if (i) u8_puts(&out,"|"); else u8_puts(&out,"{");
      u8_puts(&out,indexes[i]->indexid); i++;}
    u8_puts(&out,"}");
    return out.u8_outbuf;}
  else return u8_strdup("compound");
}

FD_EXPORT fd_index fd_make_compound_index(int n_indexes,fd_index *indexes)
{
  struct FD_COMPOUND_INDEX *cix=u8_alloc(struct FD_COMPOUND_INDEX);
  u8_string cid=get_compound_id(n_indexes,indexes);
  fd_init_index((fd_index)cix,&compoundindex_handler,cid,NULL,0);
  u8_init_mutex(&(cix->index_lock)); u8_free(cid);
  cix->n_indexes=n_indexes; cix->indexes=indexes;
  fd_register_index((fd_index)cix);
  return (fd_index) cix;
}

FD_EXPORT int fd_add_to_compound_index(fd_compound_index cix,fd_index add)
{
  if (cix->index_handler == &compoundindex_handler) {
    int i=0, n=cix->n_indexes;
    while (i < n)
      if (cix->indexes[i] == add) {
        fd_unlock_index(cix); return 0;}
      else i++;
    if (cix->indexes)
      cix->indexes=u8_realloc_n(cix->indexes,cix->n_indexes+1,fd_index);
    else cix->indexes=u8_alloc_n(1,fd_index);
    cix->indexes[cix->n_indexes++]=add;
    if (add->index_serialno<0) {
      fdtype alix=(fdtype)add; fd_incref(alix);}
    if ((cix->indexid) || (cix->index_source)) {
      if ((cix->indexid)==(cix->index_source)) {
        u8_free(cix->indexid); cix->indexid=cix->index_source=NULL;}
      else {
        if (cix->indexid) {u8_free(cix->indexid); cix->indexid=NULL;}
        if (cix->index_source) {u8_free(cix->index_source); cix->index_source=NULL;}}}
    cix->indexid=cix->index_source=get_compound_id(cix->n_indexes,cix->indexes);
    fd_reset_hashtable(&(cix->index_cache),-1,1);
    return 1;}
  else return fd_reterr(fd_TypeError,("compound_index"),NULL,FD_VOID);
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
  NULL, /* fetchsizes */
  NULL, /* batchadd */
  NULL, /* metadata */
  NULL, /* create */
  NULL, /* walk */
  NULL, /* recycle */
  NULL /* indexctl */
};

FD_EXPORT void fd_init_compoundindexes_c()
{
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
