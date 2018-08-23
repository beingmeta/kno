/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/components/storage_layer.h"
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

static int memindex_map_init = 10000;

static struct FD_INDEX_HANDLER memindex_handler;

static ssize_t load_memindex(struct FD_MEMINDEX *memidx);

static void truncate_failed(int fileno,u8_string file)
{
  int got_err = errno; errno=0;
  if (got_err)
    u8_logf(LOG_WARN,"TruncateFailed",
            "Couldn't truncate memindex file %s (fd=%d) (errno=%d:%s)",
            file,fileno,got_err,u8_strerror(got_err));
  else u8_logf(LOG_WARN,"TruncateFailed",
               "Couldn't truncate memindex file %s (fd=%d)",file,fileno);
}


/* The in-memory index */

static lispval memindex_fetch(fd_index ix,lispval key)
{
  struct FD_MEMINDEX *mix = (struct FD_MEMINDEX *)ix;
  if (! (mix->mix_loaded) ) load_memindex(mix);
  return fd_hashtable_get(&(mix->mix_map),key,EMPTY);
}

static int memindex_fetchsize(fd_index ix,lispval key)
{
  struct FD_MEMINDEX *mix = (struct FD_MEMINDEX *)ix;
  if (! (mix->mix_loaded) ) load_memindex(mix);
  lispval v = fd_hashtable_get(&(mix->mix_map),key,EMPTY);
  int size = FD_CHOICE_SIZE(v);
  fd_decref(v);
  return size;
}

static lispval *memindex_fetchn(fd_index ix,int n,const lispval *keys)
{
  struct FD_MEMINDEX *mix = (struct FD_MEMINDEX *)ix;
  if (! (mix->mix_loaded) ) load_memindex(mix);
  struct FD_HASHTABLE *map = &(mix->mix_map);
  lispval *results = u8_big_alloc_n(n,lispval);
  fd_write_lock_table(map);
  int i = 0; while (i<n) {
    results[i]=fd_hashtable_get_nolock(map,keys[i],EMPTY);
    i++;}
  fd_unlock_table(map);
  return results;
}

static lispval *memindex_fetchkeys(fd_index ix,int *n)
{
  struct FD_MEMINDEX *mix = (struct FD_MEMINDEX *)ix;
  if (! (mix->mix_loaded) ) load_memindex(mix);
  lispval keys = fd_hashtable_keys(&(mix->mix_map));
  if (FD_PRECHOICEP(keys)) keys = fd_simplify_choice(keys);
  if (FD_CHOICEP(keys)) {
    int count = FD_CHOICE_SIZE(keys);
    lispval *keyv = u8_big_alloc_n(count,lispval);
    if (FD_CONS_REFCOUNT(keys) == 1) {
      struct FD_CHOICE *ch = (fd_choice) keys;
      memmove(keyv,FD_CHOICE_ELTS(keys),count*LISPVAL_LEN);
      fd_free_choice(ch);}
    else {
      const lispval *elts = FD_CHOICE_ELTS(keys);
      int i=0; while (i<count) {
        keyv[i] = fd_incref(elts[i]);
        i++;}
      fd_decref(keys);}
    *n=count;
    return keyv;}
  else {
    lispval *one = u8_big_alloc_n(1,lispval);
    *one=keys; *n=1;
    return one;}
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
      state->sizes[i].keysize_key = key;
      fd_incref(key);
      state->sizes[i].keysize_count = FD_INT(size);
      state->i++;}}
  return 0;
}

static struct FD_KEY_SIZE *memindex_fetchinfo(fd_index ix,fd_choice filter,int *n)
{
  struct FD_MEMINDEX *mix = (struct FD_MEMINDEX *)ix;
  if (! (mix->mix_loaded) ) load_memindex(mix);
  int n_keys = (filter == NULL) ? (mix->mix_map.table_n_keys) :
    (FD_XCHOICE_SIZE(filter));
  struct FD_KEY_SIZE *keysizes = u8_big_alloc_n(n_keys,struct FD_KEY_SIZE);
  struct FETCHINFO_STATE state={keysizes,filter,0,n_keys};
  fd_for_hashtable_kv(&(mix->mix_map),gather_keysizes,(void *)&state,1);
  *n = state.i;
  return keysizes;
}

static int memindex_save(struct FD_INDEX *ix,
                         struct FD_CONST_KEYVAL *adds,int n_adds,
                         struct FD_CONST_KEYVAL *drops,int n_drops,
                         struct FD_CONST_KEYVAL *stores,int n_stores,
                         lispval changed_metadata)
{
  struct FD_MEMINDEX *memidx = (struct FD_MEMINDEX *)ix;
  unsigned long long n_changes = n_adds + n_drops + n_stores;

  if (n_changes == 0) return 0;

  /* Now write the adds and edits to disk */
  fd_stream stream = &(memidx->index_stream);
  fd_inbuf in = ( fd_lock_stream(stream), fd_start_read(stream,0x08) );
  unsigned long long n_entries = fd_read_8bytes(in);
  size_t end = fd_read_8bytes(in);
  fd_outbuf out = fd_start_write(stream,end);

  int i=0; while (i<n_adds) {
    fd_write_byte(out,1);
    fd_write_dtype(out,adds[i].kv_key);
    fd_write_dtype(out,adds[i].kv_val);
    i++;}

  i=0; while (i<n_drops) {
    fd_write_byte(out,(unsigned char)-1);
    fd_write_dtype(out,drops[i].kv_key);
    fd_write_dtype(out,drops[i].kv_val);
    i++;}

  i=0; while (i<n_stores) {
    fd_write_byte(out,(unsigned char)0);
    fd_write_dtype(out,stores[i].kv_key);
    fd_write_dtype(out,stores[i].kv_val);
    i++;}

  end = fd_getpos(stream);

  fd_setpos(stream,0x08); out = fd_writebuf(stream);

  fd_write_8bytes(out,n_entries+n_changes);
  fd_write_8bytes(out,end);

  fd_flush_stream(stream);

  fd_unlock_stream(stream);

  if (memidx->mix_loaded) {
    fd_reset_hashtable(&memidx->mix_map,17,1);
    memidx->mix_loaded=0;}

  u8_logf(LOG_NOTICE,"MemIndex/Finished",
          "Finished writing %lld/%lld changes to disk for %s, endpos=%lld",
          n_changes,n_entries,ix->indexid,end);

  return n_changes;
}

static int memindex_commit(fd_index ix,fd_commit_phase phase,
                           struct FD_INDEX_COMMITS *commit)
{
  switch (phase) {
  case fd_commit_write: {
    int rv = memindex_save(ix,
                           (struct FD_CONST_KEYVAL *)commit->commit_adds,
                           commit->commit_n_adds,
                           (struct FD_CONST_KEYVAL *)commit->commit_drops,
                           commit->commit_n_drops,
                           (struct FD_CONST_KEYVAL *)commit->commit_stores,
                           commit->commit_n_stores,
                           commit->commit_metadata);
    if (rv<0) commit->commit_phase = fd_commit_rollback;
    else commit->commit_phase = fd_commit_flush;
    return rv;}
  default: {
    u8_logf(LOG_INFO,"NoPhasedCommit",
            "The index %s doesn't support phased commits",
            ix->indexid);
    return 0;}
  }
}

static int simplify_choice(struct FD_KEYVAL *kv,void *data)
{
  if (PRECHOICEP(kv->kv_val))
    kv->kv_val = fd_simplify_choice(kv->kv_val);
  return 0;
}

static ssize_t load_memindex(struct FD_MEMINDEX *memidx)
{
  struct FD_STREAM *stream = &(memidx->index_stream);
  if (memidx->mix_loaded) return 0;
  else if (fd_lock_stream(stream)<0) return -1;
  else if (memidx->mix_loaded) {
    fd_unlock_stream(stream);
    return 0;}
  fd_inbuf in = fd_start_read(stream,8);
  long long i = 0, n_entries = fd_read_8bytes(in);
  double started = u8_elapsed_time();
  fd_hashtable mix_map = &(memidx->mix_map);
  u8_logf(LOG_INFO,"MemIndexLoad",
          "Loading %lld entries for '%s'",n_entries,memidx->indexid);
  memidx->mix_valid_data = fd_read_8bytes(in);
  int rv = ftruncate(stream->stream_fileno,memidx->mix_valid_data);
  if (rv<0) truncate_failed(stream->stream_fileno,stream->streamid);
  fd_setpos(stream,256);
  if (n_entries<memindex_map_init)
    fd_resize_hashtable(mix_map,memindex_map_init);
  else fd_resize_hashtable(mix_map,1.5*n_entries);
  fd_write_lock_table(mix_map);
  in = fd_readbuf(stream);
  while (i<n_entries) {
    char op = fd_read_byte(in);
    lispval key = fd_read_dtype(in);
    lispval value = fd_read_dtype(in);
    fd_hashtable_op_nolock
      (mix_map,( (op<0) ? (fd_table_drop) :
                 (op==0) ? (fd_table_store_noref):
                 (fd_table_add_noref)),
       key,value);
    fd_decref(key);
    if (op<0) fd_decref(value);
    i++;}
  fd_for_hashtable_kv(mix_map,simplify_choice,NULL,0);
  fd_unlock_table(mix_map);
  memidx->mix_loaded = 1;
  fd_unlock_stream(stream);
  u8_logf(LOG_NOTICE,"MemIndexLoad",
          "Loaded %lld entries for '%s' in %fs",
          n_entries,memidx->indexid,u8_elapsed_time()-started);
  return 1;
}

static lispval preload_opt;

static fd_index open_memindex(u8_string file,fd_storage_flags flags,
                              lispval opts)
{
  struct FD_MEMINDEX *memidx = u8_alloc(struct FD_MEMINDEX);
  u8_string abspath = u8_abspath(file,NULL);
  u8_string realpath = u8_realpath(file,NULL);
  fd_init_index((fd_index)memidx,&memindex_handler,
                file,abspath,realpath,
                flags|FD_STORAGE_NOSWAP,
                VOID,
                opts);
  struct FD_STREAM *stream=
    fd_init_file_stream(&(memidx->index_stream),abspath,
                        FD_FILE_MODIFY,-1,
                        fd_driver_bufsize);
  lispval preload = fd_getopt(opts,preload_opt,FD_TRUE);
  u8_free(abspath); u8_free(realpath);
  if (!(stream)) return NULL;
  stream->stream_flags &= ~FD_STREAM_IS_CONSED;
  unsigned int magic_no = fd_read_4bytes(fd_readbuf(stream));
  if (magic_no!=FD_MEMINDEX_MAGIC_NUMBER) {
    fd_seterr(_("NotMemindex"),"open_memindex",file,VOID);
    fd_close_stream(stream,0);
    u8_free(memidx);
    return NULL;}
  else {
    fd_inbuf in = fd_readbuf(stream);
    unsigned U8_MAYBE_UNUSED int n_keys = fd_read_4bytes(in);
    long long n_entries = fd_read_8bytes(in);
    memidx->mix_valid_data = fd_read_8bytes(in);
    int rv = ftruncate(stream->stream_fileno,memidx->mix_valid_data);
    if (rv<0) truncate_failed(stream->stream_fileno,stream->streamid);
    fd_setpos(stream,256);
    if (n_entries<memindex_map_init)
      fd_init_hashtable(&(memidx->mix_map),memindex_map_init,NULL);
    else fd_init_hashtable(&(memidx->mix_map),1.5*n_entries,NULL);
    if (!(FALSEP(preload)))
      load_memindex(memidx);
    if (!(U8_BITP(flags,FD_STORAGE_UNREGISTERED)))
      fd_register_index((fd_index)memidx);
    return (fd_index)memidx;}
}

static lispval memindex_ctl(fd_index ix,lispval op,int n,lispval *args)
{
  struct FD_MEMINDEX *mix = (struct FD_MEMINDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return fd_err("BadIndexOpCall","hashindex_ctl",
                  mix->indexid,VOID);
  else if (op == fd_cachelevel_op) {
    if (mix->mix_loaded)
      return FD_INT(3);
    else return FD_INT(0);}
  else if (op == fd_getmap_op) {
    if (mix->mix_loaded==0) load_memindex(mix);
    return (lispval) fd_copy_hashtable(NULL,&(mix->mix_map),1);}
  else if (op == fd_preload_op) {
    if (mix->mix_loaded==0) load_memindex(mix);
    return FD_TRUE;}
  else if (op == fd_capacity_op)
    return EMPTY;
  else if (op == fd_swapout_op) {
    if (mix->mix_loaded) {
      mix->mix_loaded=0;
      fd_reset_hashtable(&(mix->mix_map),17,1);
      return FD_TRUE;}
    return FD_INT(mix->mix_map.table_n_keys);}
  else if (op == fd_load_op) {
    if (mix->mix_loaded == 0) load_memindex(mix);
    return FD_INT(mix->mix_map.table_n_keys);}
  else if (op == fd_keycount_op) {
    if (mix->mix_loaded == 0) load_memindex(mix);
    return FD_INT(mix->mix_map.table_n_keys);}
  else return fd_default_indexctl(ix,op,n,args);
}

FD_EXPORT int fd_make_memindex(u8_string spec)
{
  struct FD_STREAM _stream;
  struct FD_STREAM *stream=
    fd_init_file_stream(&_stream,spec,FD_FILE_CREATE,-1,fd_driver_bufsize);
  fd_outbuf out = fd_writebuf(stream);
  int i = 24;
  fd_write_4bytes(out,FD_MEMINDEX_MAGIC_NUMBER);
  fd_write_4bytes(out,0); /* n_keys */
  fd_write_8bytes(out,0); /* n_entries */
  fd_write_8bytes(out,256); /* valid_data */
  while (i<256) {fd_write_4bytes(out,0); i = i+4;}
  fd_close_stream(stream,FD_STREAM_FREEDATA);
  return 1;
}

static fd_index memindex_create(u8_string spec,void *type_data,
                                fd_storage_flags flags,
                                lispval opts)
{
  if (fd_make_memindex(spec)>=0) {
    fd_set_file_opts(spec,opts);
    return fd_open_index(spec,flags,VOID);}
  else return NULL;
}

/* Initializing the driver module */

static struct FD_INDEX_HANDLER memindex_handler={
  "memindex", 1, sizeof(struct FD_MEMINDEX), 14,
  NULL, /* close */
  memindex_commit, /* commit */
  memindex_fetch, /* fetch */
  memindex_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  memindex_fetchn, /* fetchn */
  memindex_fetchkeys, /* fetchkeys */
  memindex_fetchinfo, /* fetchinfo */
  NULL, /* batchadd */
  memindex_create, /* create */
  NULL, /* walk */
  NULL, /* recycle */
  memindex_ctl  /* indexctl */
};

FD_EXPORT void fd_init_memindex_c()
{
  preload_opt = fd_intern("PRELOAD");

  fd_register_index_type("memindex",
                         &memindex_handler,
                         open_memindex,
                         fd_match_index_file,
                         (void *) U8_INT2PTR(FD_MEMINDEX_MAGIC_NUMBER));

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
