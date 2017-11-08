/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1
#define FD_INLINE_CHOICES 1
#define FD_FAST_CHOICE_CONTAINSP 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include "headers/htindex.h"

#include <libu8/u8pathfns.h>

/* The in-memory index */

static lispval *htindex_fetchn(fd_index ix,int n,const lispval *keys)
{
  lispval *results = u8_big_alloc_n(n,lispval);
  int i = 0; while (i<n) {
    results[i]=fd_hashtable_get(&(ix->index_cache),keys[i],EMPTY);
    i++;}
  return results;
}

static lispval *htindex_fetchkeys(fd_index ix,int *n)
{
  lispval keys = fd_hashtable_keys(&(ix->index_cache));
  int n_elts = FD_CHOICE_SIZE(keys);
  lispval *result = u8_big_alloc_n(n_elts,lispval);
  int j = 0;
  DO_CHOICES(key,keys) {result[j++]=key;}
  *n = n_elts;
  return result;
}

struct FETCHINFO_STATE {
  struct FD_KEY_SIZE *result, *write;
  fd_choice filter;};

static int htindex_fetchinfo_helper(lispval key,lispval value,void *ptr)
{
  struct FETCHINFO_STATE *state=(struct FETCHINFO_STATE *)ptr;
  if ( (state->filter == NULL) || (fast_choice_containsp(key,state->filter)) ) {
    state->write->keysize_key = key; fd_incref(key);
    state->write->keysize_count = FD_CHOICE_SIZE(value);
    state->write++;}
  return 0;
}

static struct FD_KEY_SIZE *htindex_fetchinfo(fd_index ix,fd_choice filter,int *n)
{
  if (ix->index_cache.table_n_keys == 0) {
    *n=0; return NULL;}
  int n_keys = (filter == NULL) ? (ix->index_cache.table_n_keys) :
    (FD_XCHOICE_SIZE(filter));
  struct FD_KEY_SIZE *sizes= u8_big_alloc_n(n_keys,FD_KEY_SIZE);
  struct FETCHINFO_STATE state={sizes,sizes,filter};
  fd_for_hashtable(&(ix->index_cache),htindex_fetchinfo_helper,&state,1);
  *n = n_keys;
  return sizes;
}

static int htindex_save(struct FD_INDEX *ix,
                          struct FD_CONST_KEYVAL *adds,int n_adds,
                          struct FD_CONST_KEYVAL *drops,int n_drops,
                          struct FD_CONST_KEYVAL *stores,int n_stores,
                          lispval changed_metadata)
{
  struct FD_HTINDEX *mix = (struct FD_HTINDEX *)ix;
  if ((mix->index_source) && (mix->commitfn))
    return (mix->commitfn)(mix,mix->index_source);
  else {
    fd_seterr(fd_EphemeralIndex,"htindex_save",ix->indexid,VOID);
    return -1;}
}

static int htindex_commit(fd_index ix,fd_commit_phase phase,
                          struct FD_INDEX_COMMITS *commit)
{
  switch (phase) {
  case fd_commit_save: {
    return htindex_save(ix,
                        (struct FD_CONST_KEYVAL *)commit->commit_adds,
                        commit->commit_n_adds,
                        (struct FD_CONST_KEYVAL *)commit->commit_drops,
                        commit->commit_n_drops,
                        (struct FD_CONST_KEYVAL *)commit->commit_stores,
                        commit->commit_n_stores,
                        commit->commit_metadata);}
  default: {
    u8_log(LOG_INFO,"NoPhasedCommit",
           "The index %s doesn't support phased commits",
           ix->indexid);
    return 0;}
  }
}

static int htindex_savefn(struct FD_HTINDEX *ix,u8_string file)
{
  struct FD_STREAM stream, *rstream;
  if ((ix->index_adds.table_n_keys>0) ||
      (ix->index_drops.table_n_keys>0) ||
      (ix->index_stores.table_n_keys>0)) {
    rstream = fd_init_file_stream
      (&stream,file,FD_FILE_CREATE,-1,fd_driver_bufsize);
    if (rstream == NULL) return -1;
    stream.stream_flags &= ~FD_STREAM_IS_CONSED;
    fd_write_dtype(fd_writebuf(&stream),(lispval)&(ix->index_cache));
    fd_free_stream(&stream);
    return 1;}
  else return 0;
}

static fd_index open_htindex(u8_string file,fd_storage_flags flags,lispval opts)
{
  struct FD_HTINDEX *mix = (fd_htindex)fd_make_htindex(flags);
  lispval lval; struct FD_HASHTABLE *h;
  struct FD_STREAM stream;
  fd_init_file_stream
    (&stream,file,FD_FILE_READ,-1,fd_driver_bufsize);
  stream.stream_flags &= ~FD_STREAM_IS_CONSED;
  lval = fd_read_dtype(fd_readbuf(&stream));
  fd_free_stream(&stream);
  if (HASHTABLEP(lval)) h = (fd_hashtable)lval;
  else {
    fd_decref(lval);
    return NULL;}
  if (mix->indexid) u8_free(mix->indexid);
  mix->indexid = u8_strdup(file);
  mix->index_source = u8_realpath(file,NULL);
  mix->commitfn = htindex_savefn;
  mix->index_cache.ht_n_buckets = h->ht_n_buckets;
  mix->index_cache.table_n_keys = h->table_n_keys;
  mix->index_cache.table_load_factor = h->table_load_factor;
  mix->index_cache.ht_buckets = h->ht_buckets;
  u8_free(h);
  return (fd_index)mix;
}

static struct FD_INDEX_HANDLER htindex_handler={
  "htindex", 1, sizeof(struct FD_HTINDEX), 14,
  NULL, /* close */
  htindex_commit, /* commit */
  NULL, /* fetch */
  NULL, /* fetchsize */
  NULL, /* prefetch */
  htindex_fetchn, /* fetchn */
  htindex_fetchkeys, /* fetchkeys */
  htindex_fetchinfo, /* fetchinfo */
  NULL, /* batchadd */
  NULL, /* recycle */
  NULL  /* indexctl */
};

FD_EXPORT
fd_index fd_make_htindex(fd_storage_flags flags)
{
  struct FD_HTINDEX *mix = u8_alloc(struct FD_HTINDEX);
  FD_INIT_STRUCT(mix,struct FD_HTINDEX);
  fd_init_index((fd_index)mix,&htindex_handler,"ephemeral",NULL,flags);
  mix->index_cache_level = 1;
  U8_SETBITS(mix->index_flags,(FD_STORAGE_NOSWAP|FD_STORAGE_READ_ONLY));
  fd_register_index((fd_index)mix);
  return (fd_index)mix;
}


FD_EXPORT void fd_init_htindex_c()
{
  u8_register_source_file(_FILEINFO);

  fd_register_index_type("htindex1",
                         &htindex_handler,open_htindex,
                           fd_match4bytes,(void *)0x42c20000);
  fd_register_index_type("htindex2",
                         &htindex_handler,open_htindex,
                           fd_match4bytes,(void *)0x42c20100);
  fd_register_index_type("htindex3",
                         &htindex_handler,open_htindex,
                           fd_match4bytes,(void *)0x42820200);
  fd_register_index_type("htindex4",
                         &htindex_handler,open_htindex,
                           fd_match4bytes,(void *)0x42820300);
  fd_register_index_type("htindex5",
                         &htindex_handler,open_htindex,
                           fd_match4bytes,(void *)0x42820400);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
