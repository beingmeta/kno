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
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include "headers/logindex.h"

/* The in-memory index */

static fdtype *log_index_fetchkeys(fd_index ix,int *n)
{
  fdtype keys=fd_getkeys(&(ix->index_cache));
  if (FD_EMPTY_CHOICEP(keys)) {
    *n=0; return NULL;}
  else {
    if (FD_ACHOICEP(keys)) keys=fd_simplify_achoice(keys);
    int n_elts=FD_CHOICE_SIZE(keys);
    if (n_elts==1) {
      fdtype *results=u8_alloc_n(1,fdtype);
      results[0]=keys;
      *n=1;
      u8_free(keys);
      return results;}
    else {
      fdtype *results=u8_alloc_n(n_elts,fdtype);
      fdtype *values=FD_XCHOICE_DATA(keys);
      memcpy(results,values,sizeof(fdtype)*n_elts);
      u8_free(keys);
      *n=n_elts;
      return results;}}
}

static int log_index_fetchsizes_helper(fdtype key,fdtype value,void *ptr)
{
  struct FD_KEY_SIZE **key_size_ptr=(struct FD_KEY_SIZE **)ptr;
  (*key_size_ptr)->keysizekey=fd_incref(key);
  (*key_size_ptr)->keysizenvals=FD_CHOICE_SIZE(value);
  *key_size_ptr=(*key_size_ptr)+1;
  return 0;
}

static struct FD_KEY_SIZE *log_index_fetchsizes(fd_index ix,int *n)
{
  if (ix->index_cache.table_n_keys) {
    struct FD_KEY_SIZE *sizes, *write; int n_keys;
    n_keys=ix->index_cache.table_n_keys;
    sizes=u8_alloc_n(n_keys,FD_KEY_SIZE); write=&(sizes[0]);
    fd_for_hashtable(&(ix->index_cache),log_index_fetchsizes_helper,
                     (void *)write,1);
    *n=n_keys;
    return sizes;}
  else {
    *n=0; return NULL;}
}

static int log_index_commit(fd_index ix)
{
  struct FD_LOG_INDEX *logix=(struct FD_LOG_INDEX *)ix;
  fd_stream s=&(logix->index_stream);
  fd_lock_stream(s);
  fd_unlock_stream(s);
}

static fd_index open_log_index(u8_string file,fddb_flags flags)
{
  struct FD_LOG_INDEX *mix=(fd_mem_index)fd_make_mem_index(flags);
  fdtype lispval; struct FD_HASHTABLE *h;
  struct FD_STREAM stream;
  fd_init_file_stream
    (&stream,file,FD_STREAM_READ,fd_driver_bufsize);
  stream.stream_flags&=~FD_STREAM_IS_MALLOCD;
  lispval=fd_read_dtype(fd_readbuf(&stream));
  fd_free_stream(&stream);
  if (FD_HASHTABLEP(lispval)) h=(fd_hashtable)lispval;
  else {
    fd_decref(lispval);
    return NULL;}
  if (mix->index_idstring) u8_free(mix->index_idstring);
  mix->index_source=mix->index_idstring=u8_strdup(file);
  mix->commitfn=log_index_commitfn;
  mix->index_cache.ht_n_buckets=h->ht_n_buckets;
  mix->index_cache.table_n_keys=h->table_n_keys;
  mix->index_cache.table_load_factor=h->table_load_factor;
  mix->index_cache.ht_buckets=h->ht_buckets;
  u8_free(h);
  return (fd_index)mix;
}

static struct FD_INDEX_HANDLER log_index_handler={
  "log_index", 1, sizeof(struct FD_MEM_INDEX), 12,
  NULL, /* close */
  log_index_commit, /* commit */
  NULL, /* setcache */
  NULL, /* setbuf */
  NULL, /* fetch */
  NULL, /* fetchsize */
  NULL, /* prefetch */
  NULL, /* fetchn */
  log_index_fetchkeys, /* fetchkeys */
  log_index_fetchsizes, /* fetchsizes */
  NULL, /* metadata */
  NULL, /* sync */
  NULL  /* indexop */
};

FD_EXPORT
fd_index fd_make_log_index(fddb_flags flags)
{
  struct FD_MEM_INDEX *mix=u8_alloc(struct FD_MEM_INDEX);
  FD_INIT_STRUCT(mix,struct FD_MEM_INDEX);
  fd_init_index((fd_index)mix,&log_index_handler,"ephemeral",flags);

  u8_log(LOG_INFO,"CreateLogIndex","Creating a logindex '%s'",filename);

  mix->index_cache_level=1;
  U8_SETBITS(mix->index_flags,(FD_INDEX_NOSWAP|FDB_READ_ONLY));
  fd_register_index((fd_index)mix);
  return (fd_index)mix;
}


FD_EXPORT void fd_init_logindex_c()
{
  u8_register_source_file(_FILEINFO);
}
