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
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include "headers/memindex.h"

#include "libu8/u8filefns.h"
#include "libu8/u8printf.h"

static int memindex_cache_init=10000;
static int memindex_adds_init=5000;
static int memindex_edits_init=1000;

static struct FD_INDEX_HANDLER mem_index_handler;

/* The in-memory index */

static fdtype *mem_index_fetchn(fd_index ix,int n,fdtype *keys)
{
  fdtype *results=u8_alloc_n(n,fdtype);
  fd_hashtable cache=&(ix->index_cache);
  int i=0;
  u8_read_lock(&(cache->table_rwlock));
  while (i<n) {
    results[i]=fd_hashtable_get_nolock
      (&(ix->index_cache),keys[i],FD_EMPTY_CHOICE);
    i++;}
  u8_rw_unlock(&(cache->table_rwlock));
  return results;
}

static fdtype *mem_index_fetchkeys(fd_index ix,int *n)
{
  fdtype keys=fd_hashtable_keys(&(ix->index_cache));
  fdtype added=fd_hashtable_keys(&(ix->index_adds));
  fdtype edits=fd_hashtable_keys(&(ix->index_adds));
  FD_ADD_TO_CHOICE(keys,added);
  FD_DO_CHOICES(key,edits) {
    if (FD_PAIRP(key)) {
      fdtype real_key=FD_CDR(key);
      fd_incref(real_key);
      FD_ADD_TO_CHOICE(keys,real_key);}}
  fd_decref(edits);
  if (FD_EMPTY_CHOICEP(keys)) {
    *n=0; return NULL;}
  else {
    if (FD_ACHOICEP(keys)) keys=fd_simplify_choice(keys);
    int n_elts=FD_CHOICE_SIZE(keys);
    if (n_elts==1) {
      fdtype *results=u8_alloc_n(1,fdtype);
      results[0]=keys;
      *n=1;
      u8_free(keys);
      return results;}
    else {
      fdtype *results=u8_alloc_n(n_elts,fdtype);
      const fdtype *values=FD_CHOICE_DATA(keys);
      memcpy(results,values,sizeof(fdtype)*n_elts);
      u8_free(keys);
      *n=n_elts;
      return results;}}
}

static fdtype drop_symbol, set_symbol;

static int write_add(struct FD_KEYVAL *kv,void *mixptr)
{
  struct FD_MEM_INDEX *memidx=(struct FD_MEM_INDEX *)mixptr;
  struct FD_STREAM *stream=&(memidx->index_stream);
  struct FD_OUTBUF *out=fd_writebuf(stream);
  fd_write_byte(out,1);
  fd_write_dtype(out,kv->kv_key);
  fd_write_dtype(out,kv->kv_val);
  return 0;
}

static int merge_adds(struct FD_KEYVAL *kv,void *cacheptr)
{
  fd_hashtable cache=(fd_hashtable)cacheptr;
  fd_hashtable_op_nolock(cache,fd_table_add,kv->kv_key,kv->kv_val);
  return 0;
}

static int write_edit(struct FD_KEYVAL *kv,void *mixptr)
{
  struct FD_MEM_INDEX *memidx=(struct FD_MEM_INDEX *)mixptr;
  struct FD_STREAM *stream=&(memidx->index_stream);
  struct FD_OUTBUF *out=fd_writebuf(stream);
  fdtype key=kv->kv_key;
  if ((FD_PAIRP(key))&&(FD_SYMBOLP(FD_CAR(key)))) {
    if ((FD_CAR(key))==drop_symbol) {
      fd_write_byte(out,(unsigned char)-1);
      fd_write_dtype(out,FD_CDR(key));
      fd_write_dtype(out,kv->kv_val);}
    else if ((FD_CAR(key))==set_symbol) {
      fd_write_byte(out,(unsigned char)0);
      fd_write_dtype(out,FD_CDR(key));
      fd_write_dtype(out,kv->kv_val);}
    else {}}
  else {}
  return 0;
}

static int merge_edits(struct FD_KEYVAL *kv,void *cacheptr)
{
  fd_hashtable cache=(fd_hashtable)cacheptr;
  fdtype key=kv->kv_key;
  if ((FD_PAIRP(key))&&(FD_CAR(key)==drop_symbol)) {
    fdtype real_key=FD_CDR(key);
    fd_hashtable_op_nolock(cache,fd_table_drop,real_key,kv->kv_val);}
  return 0;
}

static int mem_index_commit(fd_index ix)
{
  struct FD_MEM_INDEX *memidx=(struct FD_MEM_INDEX *)ix;
  struct FD_HASHTABLE *adds=&(ix->index_adds), *edits=&(ix->index_edits);
  struct FD_HASHTABLE *cache=&(ix->index_cache);
  struct FD_HASHTABLE _adds, _edits;
  unsigned long long n_updates=0;

  if ((adds->table_n_keys==0)&&(edits->table_n_keys==0))
    return 0;

  /* Update the cache from the adds and edits */
  u8_write_lock(&(cache->table_rwlock));
  u8_read_lock(&(adds->table_rwlock));
  u8_read_lock(&(edits->table_rwlock));

  n_updates=adds->table_n_keys+edits->table_n_keys;

  if (n_updates>60000)
    u8_log(fdkb_loglevel,"MemIndex/Commit",
	   "Saving %d updates to %s",n_updates,ix->index_idstring);
  else u8_log(fdkb_loglevel+1,"MemIndex/Commit",
	      "Saving %d updates to %s",n_updates,ix->index_idstring);

  fd_for_hashtable_kv(adds,merge_adds,(void *)cache,0);
  fd_for_hashtable_kv(edits,merge_edits,(void *)cache,0);

  /* Now copy the adds and edits into the hashtables on the stack */
  fd_swap_hashtable(adds,&_adds,256,1);
  fd_swap_hashtable(edits,&_edits,256,1);
  u8_rw_unlock(&(cache->table_rwlock));
  u8_rw_unlock(&(adds->table_rwlock));
  u8_rw_unlock(&(edits->table_rwlock));


  u8_log(fdkb_loglevel+1,"MemIndex/Commit",
	 "Updated in-memory cache with %d updates to %s, writing to disk",
	 n_updates,ix->index_idstring);

  /* At this point, the index tables are unlocked and can start being
     used by other threads. We'll now write the changes to the
     disk. */

  /* We don't currently have recovery provisions here. One possibility
     would be to write all of the values to a separate file before
     returning. Another would be to wait with unlocking the adds and
     edits until they've been written. */

  /* Now write the adds and edits to disk */
  fd_stream stream=&(memidx->index_stream);
  fd_inbuf in=( fd_lock_stream(stream), fd_start_read(stream,0x08) );
  unsigned long long n_entries=fd_read_8bytes(in);
  size_t end=fd_read_8bytes(in);
  fd_outbuf out=fd_start_write(stream,end);

  n_entries=n_entries+_adds.table_n_keys;
  fd_for_hashtable_kv(&_adds,write_add,(void *)memidx,0);
  fd_recycle_hashtable(&_adds);

  n_entries=n_entries+_edits.table_n_keys;
  fd_for_hashtable_kv(&_edits,write_edit,(void *)memidx,0);
  fd_recycle_hashtable(&_edits);

  end=fd_getpos(stream);

  fd_setpos(stream,0x08); out=fd_writebuf(stream);

  fd_write_8bytes(out,n_entries);
  fd_write_8bytes(out,end);

  fd_flush_stream(stream);

  fd_unlock_stream(stream);

  u8_log(fdkb_loglevel,"MemIndex/Finished",
	 "Finished writing %lld/%lld changes to disk for %s, endpos=%lld",
	 n_updates,n_entries,ix->index_idstring,end);

  return 1;
}

static fd_index open_mem_index(u8_string file,fdkb_flags flags)
{
  struct FD_MEM_INDEX *memidx=u8_alloc(struct FD_MEM_INDEX);
  fd_init_index((fd_index)memidx,&mem_index_handler,file,flags|FD_INDEX_NOSWAP);
  struct FD_STREAM *stream=
    fd_init_file_stream(&(memidx->index_stream),file,
			FD_FILE_MODIFY,-1,
			fd_driver_bufsize);
  if (!(stream)) return NULL;
  stream->stream_flags&=~FD_STREAM_IS_CONSED;
  unsigned int magic_no=fd_read_4bytes(fd_readbuf(stream));
  if (magic_no!=FD_MEM_INDEX_MAGIC_NUMBER) {
    fd_seterr(_("NotMemindex"),"open_mem_index",u8_strdup(file),FD_VOID);
    fd_close_stream(stream,0);
    u8_free(memidx);
    return NULL;}
  else {
    fd_inbuf in=fd_readbuf(stream);
    unsigned U8_MAYBE_UNUSED int not_used=fd_read_4bytes(in);
    long long i=0, n_entries=fd_read_8bytes(in);
    fd_hashtable cache=&(memidx->index_cache);
    memidx->mix_valid_data=fd_read_8bytes(in);
    ftruncate(stream->stream_fileno,memidx->mix_valid_data);
    fd_setpos(stream,256);
    if (n_entries<memindex_cache_init)
      fd_resize_hashtable(&(memidx->index_cache),memindex_cache_init);
    else fd_resize_hashtable(&(memidx->index_cache),1.5*n_entries);
    fd_resize_hashtable(&(memidx->index_adds),memindex_adds_init);
    fd_resize_hashtable(&(memidx->index_edits),memindex_edits_init);
    in=fd_readbuf(stream);
    while (i<n_entries) {
      char op=fd_read_byte(in);
      fdtype key=fd_read_dtype(in);
      fdtype value=fd_read_dtype(in);
      fd_hashtable_op_nolock
	(cache,
	 ((op<0)?(fd_table_drop):
	  (op==0)?(fd_table_store_noref):
	  (fd_table_add_noref)),
	 key,value);
      fd_decref(key);
      if (op<0) fd_decref(value);
      i++;}
    if (!(U8_BITP(flags,FDKB_ISCONSED)))
      fd_register_index((fd_index)memidx);
    return (fd_index)memidx;}
}

FD_EXPORT int fd_make_mem_index(u8_string spec)
{
  struct FD_STREAM _stream;
  struct FD_STREAM *stream=
    fd_init_file_stream(&_stream,spec,FD_FILE_CREATE,-1,fd_driver_bufsize);
  fd_outbuf out=fd_writebuf(stream);
  int i=24;
  fd_write_4bytes(out,FD_MEM_INDEX_MAGIC_NUMBER);
  fd_write_4bytes(out,0); /* n_keys */
  fd_write_8bytes(out,0); /* n_entries */
  fd_write_8bytes(out,256); /* valid_data */
  while (i<256) {fd_write_4bytes(out,0); i=i+4;}
  fd_close_stream(stream,FD_STREAM_FREEDATA);
  return 1;
}

static fd_index mem_index_create(u8_string spec,void *type_data,
				 fdkb_flags flags,fdtype opts)
{
  if (fd_make_mem_index(spec)>=0)
    return fd_open_index(spec,flags);
  else return NULL;
}

static u8_string match_index_name(u8_string spec,void *data)
{
  if ((u8_file_existsp(spec)) &&
      (fd_match4bytes(spec,data)))
    return spec;
  else if (u8_has_suffix(spec,".index",1))
    return NULL;
  else {
    u8_string variation=u8_mkstring("%s.index",spec);
    if ((u8_file_existsp(variation))&&
        (fd_match4bytes(variation,data)))
      return variation;
    else {
      u8_free(variation);
      return NULL;}}
}

static struct FD_INDEX_HANDLER mem_index_handler={
  "memindex", 1, sizeof(struct FD_MEM_INDEX), 12,
  NULL, /* close */
  mem_index_commit, /* commit */
  NULL, /* setcache */
  NULL, /* setbuf */
  NULL, /* fetch */
  NULL, /* fetchsize */
  NULL, /* prefetch */
  mem_index_fetchn, /* fetchn */
  mem_index_fetchkeys, /* fetchkeys */
  NULL, /* fetchsizes */
  NULL, /* metadata */
  mem_index_create, /* create */
  NULL, /* recycle */
  NULL  /* indexop */
};

FD_EXPORT void fd_init_memindex_c()
{
  drop_symbol=fd_intern("DROP");
  set_symbol=fd_intern("SET");

  fd_register_index_type("memindex",
                         &mem_index_handler,
                         open_mem_index,
                         match_index_name,
                         (void *) U8_INT2PTR(FD_MEM_INDEX_MAGIC_NUMBER));

  u8_register_source_file(_FILEINFO);
}
