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

#include "headers/htindex.h"

/* The in-memory index */

static fdtype *htindex_fetchn(fd_index ix,int n,fdtype *keys)
{
  fdtype *results=u8_alloc_n(n,fdtype);
  int i=0; while (i<n) {
    results[i]=fd_hashtable_get(&(ix->index_cache),keys[i],FD_EMPTY_CHOICE);
    i++;}
  return results;
}

static fdtype *htindex_fetchkeys(fd_index ix,int *n)
{
  fdtype keys=fd_hashtable_keys(&(ix->index_cache));
  int n_elts=FD_CHOICE_SIZE(keys);
  fdtype *result=u8_alloc_n(n_elts,fdtype);
  int j=0;
  FD_DO_CHOICES(key,keys) {result[j++]=key;}
  *n=n_elts;
  return result;
}

static int htindex_fetchsizes_helper(fdtype key,fdtype value,void *ptr)
{
  struct FD_KEY_SIZE **key_size_ptr=(struct FD_KEY_SIZE **)ptr;
  (*key_size_ptr)->keysizekey=fd_incref(key);
  (*key_size_ptr)->keysizenvals=FD_CHOICE_SIZE(value);
  *key_size_ptr=(*key_size_ptr)+1;
  return 0;
}

static struct FD_KEY_SIZE *htindex_fetchsizes(fd_index ix,int *n)
{
  if (ix->index_cache.table_n_keys) {
    struct FD_KEY_SIZE *sizes, *write; int n_keys;
    n_keys=ix->index_cache.table_n_keys;
    sizes=u8_alloc_n(n_keys,FD_KEY_SIZE); write=&(sizes[0]);
    fd_for_hashtable(&(ix->index_cache),htindex_fetchsizes_helper,
                     (void *)write,1);
    *n=n_keys;
    return sizes;}
  else {
    *n=0; return NULL;}
}

static int htindex_commit(fd_index ix)
{
  struct FD_HT_INDEX *mix=(struct FD_HT_INDEX *)ix;
  if ((mix->index_source) && (mix->commitfn))
    return (mix->commitfn)(mix,mix->index_source);
  else {
    fd_seterr(fd_EphemeralIndex,"htindex_commit",
	      u8_strdup(ix->index_idstring),FD_VOID);
    return -1;}
}

static int htindex_commitfn(struct FD_HT_INDEX *ix,u8_string file)
{
  struct FD_STREAM stream, *rstream;
  if ((ix->index_adds.table_n_keys>0) || (ix->index_edits.table_n_keys>0)) {
    rstream=fd_init_file_stream
      (&stream,file,FD_STREAM_CREATE,fd_driver_bufsize);
    if (rstream==NULL) return -1;
    stream.stream_flags&=~FD_STREAM_IS_MALLOCD;
    fd_write_dtype(fd_writebuf(&stream),(fdtype)&(ix->index_cache));
    fd_free_stream(&stream);
    return 1;}
  else return 0;
}

static fd_index open_htindex(u8_string file,fddb_flags flags)
{
  struct FD_HT_INDEX *mix=(fd_mem_index)fd_make_ht_index(flags);
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
  mix->commitfn=htindex_commitfn;
  mix->index_cache.ht_n_buckets=h->ht_n_buckets;
  mix->index_cache.table_n_keys=h->table_n_keys;
  mix->index_cache.table_load_factor=h->table_load_factor;
  mix->index_cache.ht_buckets=h->ht_buckets;
  u8_free(h);
  return (fd_index)mix;
}

static struct FD_INDEX_HANDLER htindex_handler={
  "htindex", 1, sizeof(struct FD_HT_INDEX), 12,
  NULL, /* close */
  htindex_commit, /* commit */
  NULL, /* setcache */
  NULL, /* setbuf */
  NULL, /* fetch */
  NULL, /* fetchsize */
  NULL, /* prefetch */
  htindex_fetchn, /* fetchn */
  htindex_fetchkeys, /* fetchkeys */
  htindex_fetchsizes, /* fetchsizes */
  NULL, /* metadata */
  NULL, /* sync */
  NULL  /* indexop */
};

FD_EXPORT
fd_index fd_make_ht_index(fddb_flags flags)
{
  struct FD_HT_INDEX *mix=u8_alloc(struct FD_HT_INDEX);
  FD_INIT_STRUCT(mix,struct FD_HT_INDEX);
  fd_init_index((fd_index)mix,&htindex_handler,"ephemeral",flags);
  mix->index_cache_level=1;
  U8_SETBITS(mix->index_flags,(FD_INDEX_NOSWAP|FDB_READ_ONLY));
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
