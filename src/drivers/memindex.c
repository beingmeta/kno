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

#include "headers/memindex.h"

#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

static int memindex_cache_init = 10000;
static int memindex_adds_init = 5000;
static int memindex_edits_init = 1000;

static struct FD_INDEX_HANDLER mem_index_handler;

static ssize_t load_mem_index(struct FD_MEM_INDEX *memidx,int lock_cache);

/* The in-memory index */

static lispval mem_index_fetch(fd_index ix,lispval key)
{
  struct FD_MEM_INDEX *mix = (struct FD_MEM_INDEX *)ix;
  if (mix->mix_loaded==0) load_mem_index(mix,1);
  return fd_hashtable_get(&(ix->index_cache),key,EMPTY);
}

static int mem_index_fetchsize(fd_index ix,lispval key)
{
  struct FD_MEM_INDEX *mix = (struct FD_MEM_INDEX *)ix;
  if (mix->mix_loaded==0) load_mem_index(mix,1);
  lispval v = fd_hashtable_get(&(ix->index_cache),key,EMPTY);
  int size = FD_CHOICE_SIZE(v);
  fd_decref(v);
  return size;
}

static lispval *mem_index_fetchn(fd_index ix,int n,lispval *keys)
{
  struct FD_MEM_INDEX *mix = (struct FD_MEM_INDEX *)ix;
  if (mix->mix_loaded==0) load_mem_index(mix,1);
  lispval *results = u8_alloc_n(n,lispval);
  fd_hashtable cache = &(ix->index_cache);
  int i = 0;
  u8_read_lock(&(cache->table_rwlock));
  while (i<n) {
    results[i]=fd_hashtable_get_nolock
      (&(ix->index_cache),keys[i],EMPTY);
    i++;}
  u8_rw_unlock(&(cache->table_rwlock));
  return results;
}

static lispval *mem_index_fetchkeys(fd_index ix,int *n)
{
  struct FD_MEM_INDEX *mix = (struct FD_MEM_INDEX *)ix;
  if (mix->mix_loaded==0) load_mem_index(mix,1);
  lispval keys = fd_hashtable_keys(&(ix->index_cache));
  lispval added = fd_hashtable_keys(&(ix->index_adds));
  lispval edits = fd_hashtable_keys(&(ix->index_adds));
  CHOICE_ADD(keys,added);
  DO_CHOICES(key,edits) {
    if (PAIRP(key)) {
      lispval real_key = FD_CDR(key);
      fd_incref(real_key);
      CHOICE_ADD(keys,real_key);}}
  fd_decref(edits);
  if (EMPTYP(keys)) {
    *n = 0; return NULL;}
  else {
    if (PRECHOICEP(keys)) keys = fd_simplify_choice(keys);
    int n_elts = FD_CHOICE_SIZE(keys);
    if (n_elts==1) {
      lispval *results = u8_alloc_n(1,lispval);
      results[0]=keys;
      *n = 1;
      u8_free(keys);
      return results;}
    else {
      lispval *results = u8_alloc_n(n_elts,lispval);
      const lispval *values = FD_CHOICE_DATA(keys);
      memcpy(results,values,sizeof(lispval)*n_elts);
      u8_free(keys);
      *n = n_elts;
      return results;}}
}

struct FETCHINFO_STATE {
  struct FD_KEY_SIZE *sizes;
  struct FD_CHOICE *filter;
  int i, n;};

static int gather_keysizes(struct FD_KEYVAL *kv,void *data)
{
  struct FETCHINFO_STATE *state = (struct FETCHINFO_STATE *)data;
  fd_choice filter=state->filter;
  int i = state->i;
  if (i<state->n) {
    lispval key = kv->kv_key;
    if ( (filter==NULL) || (fast_choice_containsp(key,filter)) ) {
      lispval value = kv->kv_val;
      int size = FD_CHOICE_SIZE(value);
      state->sizes[i].keysizekey = key;
      fd_incref(key);
      state->sizes[i].keysizenvals = FD_INT(size);
      state->i++;}}
  return 0;
}

static struct FD_KEY_SIZE *mem_index_fetchinfo(fd_index ix,fd_choice filter,int *n)
{
  struct FD_MEM_INDEX *mix = (struct FD_MEM_INDEX *)ix;
  if (mix->mix_loaded==0) load_mem_index(mix,1);
  int n_keys = (filter == NULL) ? (ix->index_cache.table_n_keys) :
    (FD_XCHOICE_SIZE(filter));
  struct FD_KEY_SIZE *keysizes = u8_alloc_n(n_keys,struct FD_KEY_SIZE);
  struct FETCHINFO_STATE state={keysizes,filter,0,n_keys};
  fd_for_hashtable_kv(&(ix->index_cache),gather_keysizes,(void *)&state,1);
  *n = state.i;
  return keysizes;
}

static lispval drop_symbol, set_symbol;

static int write_add(struct FD_KEYVAL *kv,void *mixptr)
{
  struct FD_MEM_INDEX *memidx = (struct FD_MEM_INDEX *)mixptr;
  struct FD_STREAM *stream = &(memidx->index_stream);
  struct FD_OUTBUF *out = fd_writebuf(stream);
  fd_write_byte(out,1);
  fd_write_dtype(out,kv->kv_key);
  fd_write_dtype(out,kv->kv_val);
  return 0;
}

static int merge_adds(struct FD_KEYVAL *kv,void *cacheptr)
{
  fd_hashtable cache = (fd_hashtable)cacheptr;
  fd_hashtable_op_nolock(cache,fd_table_add,kv->kv_key,kv->kv_val);
  return 0;
}

static int write_edit(struct FD_KEYVAL *kv,void *mixptr)
{
  struct FD_MEM_INDEX *memidx = (struct FD_MEM_INDEX *)mixptr;
  struct FD_STREAM *stream = &(memidx->index_stream);
  struct FD_OUTBUF *out = fd_writebuf(stream);
  lispval key = kv->kv_key;
  if ((PAIRP(key))&&(SYMBOLP(FD_CAR(key)))) {
    if ((FD_CAR(key)) == drop_symbol) {
      fd_write_byte(out,(unsigned char)-1);
      fd_write_dtype(out,FD_CDR(key));
      fd_write_dtype(out,kv->kv_val);}
    else if ((FD_CAR(key)) == set_symbol) {
      fd_write_byte(out,(unsigned char)0);
      fd_write_dtype(out,FD_CDR(key));
      fd_write_dtype(out,kv->kv_val);}
    else {}}
  else {}
  return 0;
}

static int merge_edits(struct FD_KEYVAL *kv,void *cacheptr)
{
  fd_hashtable cache = (fd_hashtable)cacheptr;
  lispval key = kv->kv_key;
  if ((PAIRP(key))&&(FD_CAR(key) == drop_symbol)) {
    lispval real_key = FD_CDR(key);
    fd_hashtable_op_nolock(cache,fd_table_drop,real_key,kv->kv_val);}
  return 0;
}

static int mem_index_commit(struct FD_INDEX *ix,
                            struct FD_KEYVAL *adds,int n_adds,
                            struct FD_KEYVAL *edits,int n_edits,
                            lispval changed_metadata)
{
  return 1;
}
#if 0
{
  struct FD_MEM_INDEX *memidx = (struct FD_MEM_INDEX *)ix;
  struct FD_HASHTABLE *adds = &(ix->index_adds), *edits = &(ix->index_edits);
  struct FD_HASHTABLE *cache = &(ix->index_cache);
  struct FD_HASHTABLE _adds, _edits;
  unsigned long long n_updates = 0;

  if ((adds->table_n_keys==0)&&(edits->table_n_keys==0))
    return 0;

  /* Update the cache from the adds and edits */
  u8_write_lock(&(cache->table_rwlock));
  u8_read_lock(&(adds->table_rwlock));
  u8_read_lock(&(edits->table_rwlock));

  n_updates = adds->table_n_keys+edits->table_n_keys;

  if (n_updates>60000)
    u8_log(fd_storage_loglevel,"MemIndex/Commit",
	   "Saving %d updates to %s",n_updates,ix->indexid);
  else u8_log(fd_storage_loglevel+1,"MemIndex/Commit",
	      "Saving %d updates to %s",n_updates,ix->indexid);

  fd_for_hashtable_kv(adds,merge_adds,(void *)cache,0);
  fd_for_hashtable_kv(edits,merge_edits,(void *)cache,0);

  /* Now copy the adds and edits into the hashtables on the stack */
  fd_swap_hashtable(adds,&_adds,256,1);
  fd_swap_hashtable(edits,&_edits,256,1);
  u8_rw_unlock(&(cache->table_rwlock));
  u8_rw_unlock(&(adds->table_rwlock));
  u8_rw_unlock(&(edits->table_rwlock));


  u8_log(fd_storage_loglevel+1,"MemIndex/Commit",
	 "Updated in-memory cache with %d updates to %s, writing to disk",
	 n_updates,ix->indexid);

  /* At this point, the index tables are unlocked and can start being
     used by other threads. We'll now write the changes to the
     disk. */

  /* We don't currently have recovery provisions here. One possibility
     would be to write all of the values to a separate file before
     returning. Another would be to wait with unlocking the adds and
     edits until they've been written. */

  /* Now write the adds and edits to disk */
  fd_stream stream = &(memidx->index_stream);
  fd_inbuf in = ( fd_lock_stream(stream), fd_start_read(stream,0x08) );
  unsigned long long n_entries = fd_read_8bytes(in);
  size_t end = fd_read_8bytes(in);
  fd_outbuf out = fd_start_write(stream,end);

  n_entries = n_entries+_adds.table_n_keys;
  fd_for_hashtable_kv(&_adds,write_add,(void *)memidx,0);
  fd_recycle_hashtable(&_adds);

  n_entries = n_entries+_edits.table_n_keys;
  fd_for_hashtable_kv(&_edits,write_edit,(void *)memidx,0);
  fd_recycle_hashtable(&_edits);

  end = fd_getpos(stream);

  fd_setpos(stream,0x08); out = fd_writebuf(stream);

  fd_write_8bytes(out,n_entries);
  fd_write_8bytes(out,end);

  fd_flush_stream(stream);

  fd_unlock_stream(stream);

  u8_log(fd_storage_loglevel,"MemIndex/Finished",
	 "Finished writing %lld/%lld changes to disk for %s, endpos=%lld",
	 n_updates,n_entries,ix->indexid,end);

  return n_updates;
}
#endif

static int simplify_choice(struct FD_KEYVAL *kv,void *data)
{
  if (PRECHOICEP(kv->kv_val))
    kv->kv_val = fd_simplify_choice(kv->kv_val);
  return 0;
}

static ssize_t load_mem_index(struct FD_MEM_INDEX *memidx,int lock_cache)
{
  struct FD_STREAM *stream = &(memidx->index_stream);
  if (memidx->mix_loaded) return 0;
  else if (fd_lock_stream(stream)<0) return -1;
  else if (memidx->mix_loaded) {
    fd_unlock_stream(stream);
    return 0;}
  fd_inbuf in = fd_start_read(stream,8);
  long long i = 0, n_entries = fd_read_8bytes(in);
  fd_hashtable cache = &(memidx->index_cache);
  double started = u8_elapsed_time();
  u8_log(fd_storage_loglevel+1,"MemIndexLoad",
	 "Loading %lld entries for '%s'",n_entries,memidx->indexid);
  memidx->mix_valid_data = fd_read_8bytes(in);
  ftruncate(stream->stream_fileno,memidx->mix_valid_data);
  fd_setpos(stream,256);
  if (n_entries<memindex_cache_init)
    fd_resize_hashtable(&(memidx->index_cache),memindex_cache_init);
  else fd_resize_hashtable(&(memidx->index_cache),1.5*n_entries);
  if (lock_cache) u8_write_lock(&(cache->table_rwlock));
  in = fd_readbuf(stream);
  while (i<n_entries) {
    char op = fd_read_byte(in);
    lispval key = fd_read_dtype(in);
    lispval value = fd_read_dtype(in);
    fd_hashtable_op_nolock
      (cache,
       ((op<0)?(fd_table_drop):
	(op==0)?(fd_table_store_noref):
	(fd_table_add_noref)),
       key,value);
    fd_decref(key);
    if (op<0) fd_decref(value);
    i++;}
  fd_for_hashtable_kv(cache,simplify_choice,NULL,0);
  if (lock_cache) u8_rw_unlock(&(cache->table_rwlock));
  memidx->mix_loaded = 1;
  fd_unlock_stream(stream);
  u8_log(fd_storage_loglevel,"MemIndexLoad",
	 "Loaded %lld entries for '%s' in %fs",
	 n_entries,memidx->indexid,u8_elapsed_time()-started);
  return 1;
}

static lispval preload_opt;

static fd_index open_mem_index(u8_string file,fd_storage_flags flags,lispval opts)
{
  struct FD_MEM_INDEX *memidx = u8_alloc(struct FD_MEM_INDEX);
  fd_init_index((fd_index)memidx,&mem_index_handler,
		file,u8_realpath(file,NULL),
		flags|FD_STORAGE_NOSWAP);
  struct FD_STREAM *stream=
    fd_init_file_stream(&(memidx->index_stream),file,
			FD_FILE_MODIFY,-1,
			fd_driver_bufsize);
  lispval preload = fd_getopt(opts,preload_opt,FD_TRUE);
  if (!(stream)) return NULL;
  stream->stream_flags &= ~FD_STREAM_IS_CONSED;
  unsigned int magic_no = fd_read_4bytes(fd_readbuf(stream));
  if (magic_no!=FD_MEM_INDEX_MAGIC_NUMBER) {
    fd_seterr(_("NotMemindex"),"open_mem_index",file,VOID);
    fd_close_stream(stream,0);
    u8_free(memidx);
    return NULL;}
  else {
    fd_inbuf in = fd_readbuf(stream);
    unsigned U8_MAYBE_UNUSED int not_used = fd_read_4bytes(in);
    long long n_entries = fd_read_8bytes(in);
    memidx->mix_valid_data = fd_read_8bytes(in);
    ftruncate(stream->stream_fileno,memidx->mix_valid_data);
    fd_setpos(stream,256);
    if (n_entries<memindex_cache_init)
      fd_resize_hashtable(&(memidx->index_cache),memindex_cache_init);
    else fd_resize_hashtable(&(memidx->index_cache),1.5*n_entries);
    fd_resize_hashtable(&(memidx->index_adds),memindex_adds_init);
    fd_resize_hashtable(&(memidx->index_edits),memindex_edits_init);
    if (!(FALSEP(preload)))
      load_mem_index(memidx,0);
    if (!(U8_BITP(flags,FD_STORAGE_UNREGISTERED)))
      fd_register_index((fd_index)memidx);
    return (fd_index)memidx;}
}

static lispval mem_index_ctl(fd_index ix,lispval op,int n,lispval *args)
{
  struct FD_MEM_INDEX *mix = (struct FD_MEM_INDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return fd_err("BadIndexOpCall","hashindex_ctl",
		  mix->indexid,VOID);
  else if (op == fd_cachelevel_op) {
    if (mix->mix_loaded)
      return FD_INT(3);
    else return FD_INT(0);}
  else if (op == fd_getmap_op) {
    if (mix->mix_loaded==0) load_mem_index(mix,1);
    return (lispval) fd_copy_hashtable(NULL,&(ix->index_cache),1);}
  else if (op == fd_preload_op) {
    if (mix->mix_loaded==0) load_mem_index(mix,1);
    return FD_TRUE;}
  else if (op == fd_capacity_op)
    return EMPTY;
  else if (op == fd_load_op) {
    if (mix->mix_loaded == 0) load_mem_index(mix,1);
    return FD_INT(mix->index_cache.table_n_keys);}
  else return fd_default_indexctl(ix,op,n,args);
}

FD_EXPORT int fd_make_mem_index(u8_string spec)
{
  struct FD_STREAM _stream;
  struct FD_STREAM *stream=
    fd_init_file_stream(&_stream,spec,FD_FILE_CREATE,-1,fd_driver_bufsize);
  fd_outbuf out = fd_writebuf(stream);
  int i = 24;
  fd_write_4bytes(out,FD_MEM_INDEX_MAGIC_NUMBER);
  fd_write_4bytes(out,0); /* n_keys */
  fd_write_8bytes(out,0); /* n_entries */
  fd_write_8bytes(out,256); /* valid_data */
  while (i<256) {fd_write_4bytes(out,0); i = i+4;}
  fd_close_stream(stream,FD_STREAM_FREEDATA);
  return 1;
}

static fd_index mem_index_create(u8_string spec,void *type_data,
				 fd_storage_flags flags,lispval opts)
{
  if (fd_make_mem_index(spec)>=0)
    return fd_open_index(spec,flags,VOID);
  else return NULL;
}

/* Initializing the driver module */

static struct FD_INDEX_HANDLER mem_index_handler={
  "memindex", 1, sizeof(struct FD_MEM_INDEX), 14,
  NULL, /* close */
  mem_index_commit, /* commit */
  mem_index_fetch, /* fetch */
  mem_index_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  mem_index_fetchn, /* fetchn */
  mem_index_fetchkeys, /* fetchkeys */
  mem_index_fetchinfo, /* fetchinfo */
  NULL, /* batchadd */
  mem_index_create, /* create */
  NULL, /* walk */
  NULL, /* recycle */
  mem_index_ctl  /* indexctl */
};

FD_EXPORT void fd_init_memindex_c()
{
  drop_symbol = fd_intern("DROP");
  set_symbol = fd_intern("SET");

  preload_opt = fd_intern("PRELOAD");

  fd_register_index_type("memindex",
                         &mem_index_handler,
                         open_mem_index,
                         fd_match_index_file,
                         (void *) U8_INT2PTR(FD_MEM_INDEX_MAGIC_NUMBER));

  u8_register_source_file(_FILEINFO);
}
