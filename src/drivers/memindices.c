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
#include "framerd/indices.h"
#include "framerd/drivers.h"

/* The in-memory index */

static fdtype *memindex_fetchn(fd_index ix,int n,fdtype *keys)
{
  fdtype *results=u8_alloc_n(n,fdtype);
  int i=0; while (i<n) {
    results[i]=fd_hashtable_get(&(ix->index_cache),keys[i],FD_EMPTY_CHOICE);
    i++;}
  return results;
}

static fdtype *memindex_fetchkeys(fd_index ix,int *n)
{
  fdtype keys=fd_hashtable_keys(&(ix->index_cache));
  int n_elts=FD_CHOICE_SIZE(keys);
  fdtype *result=u8_alloc_n(n_elts,fdtype);
  int j=0;
  FD_DO_CHOICES(key,keys) {result[j++]=key;}
  *n=n_elts;
  return result;
}

static int memindex_fetchsizes_helper(fdtype key,fdtype value,void *ptr)
{
  struct FD_KEY_SIZE **key_size_ptr=(struct FD_KEY_SIZE **)ptr;
  (*key_size_ptr)->keysizekey=fd_incref(key);
  (*key_size_ptr)->keysizenvals=FD_CHOICE_SIZE(value);
  *key_size_ptr=(*key_size_ptr)+1;
  return 0;
}

static struct FD_KEY_SIZE *memindex_fetchsizes(fd_index ix,int *n)
{
  if (ix->index_cache.table_n_keys) {
    struct FD_KEY_SIZE *sizes, *write; int n_keys;
    n_keys=ix->index_cache.table_n_keys;
    sizes=u8_alloc_n(n_keys,FD_KEY_SIZE); write=&(sizes[0]);
    fd_for_hashtable(&(ix->index_cache),memindex_fetchsizes_helper,
                     (void *)write,1);
    *n=n_keys;
    return sizes;}
  else {
    *n=0; return NULL;}
}

static int memindex_commit(fd_index ix)
{
  struct FD_MEM_INDEX *mix=(struct FD_MEM_INDEX *)ix;
  if ((mix->index_source) && (mix->commitfn))
    return (mix->commitfn)(mix,mix->index_source);
  else {
    fd_seterr(fd_EphemeralIndex,"memindex_commit",
	      u8_strdup(ix->index_idstring),FD_VOID);
    return -1;}
}

static int memindex_commitfn(struct FD_MEM_INDEX *ix,u8_string file)
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

static fd_index open_memindex(u8_string file,fddb_flags flags)
{
  struct FD_MEM_INDEX *mix=(fd_mem_index)fd_make_mem_index(flags);
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
  mix->commitfn=memindex_commitfn;
  mix->index_cache.ht_n_buckets=h->ht_n_buckets;
  mix->index_cache.table_n_keys=h->table_n_keys;
  mix->index_cache.table_load_factor=h->table_load_factor;
  mix->index_cache.ht_buckets=h->ht_buckets;
  u8_free(h);
  return (fd_index)mix;
}

static struct FD_INDEX_HANDLER memindex_handler={
  "memindex", 1, sizeof(struct FD_MEM_INDEX), 12,
  NULL, /* close */
  memindex_commit, /* commit */
  NULL, /* setcache */
  NULL, /* setbuf */
  NULL, /* fetch */
  NULL, /* fetchsize */
  NULL, /* prefetch */
  memindex_fetchn, /* fetchn */
  memindex_fetchkeys, /* fetchkeys */
  memindex_fetchsizes, /* fetchsizes */
  NULL, /* metadata */
  NULL, /* sync */
  NULL  /* indexop */
};

FD_EXPORT
fd_index fd_make_mem_index(fddb_flags flags)
{
  struct FD_MEM_INDEX *mix=u8_alloc(struct FD_MEM_INDEX);
  FD_INIT_STRUCT(mix,struct FD_MEM_INDEX);
  fd_init_index((fd_index)mix,&memindex_handler,"ephemeral",flags);
  mix->index_cache_level=1;
  U8_SETBITS(mix->index_flags,(FD_INDEX_NOSWAP|FDB_READ_ONLY));
  fd_register_index((fd_index)mix);
  return (fd_index)mix;
}


FD_EXPORT void fd_init_memindices_c()
{
  u8_register_source_file(_FILEINFO);

  fd_register_index_type("memindex1",
			 &memindex_handler,open_memindex,
                           fd_match4bytes,(void *)0x42c20000);
  fd_register_index_type("memindex2",
			 &memindex_handler,open_memindex,
                           fd_match4bytes,(void *)0x42c20100);
  fd_register_index_type("memindex3",
			 &memindex_handler,open_memindex,
                           fd_match4bytes,(void *)0x42820200);
  fd_register_index_type("memindex4",
			 &memindex_handler,open_memindex,
                           fd_match4bytes,(void *)0x42820300);
  fd_register_index_type("memindex5",
			 &memindex_handler,open_memindex,
                           fd_match4bytes,(void *)0x42820400);
}
