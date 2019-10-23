/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"
#define KNO_INLINE_BUFIO (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_CHOICES (!(KNO_AVOID_CHOICES))
#define KNO_FAST_CHOICE_CONTAINSP (!(KNO_AVOID_CHOICES))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

#include "headers/memindex.h"

#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

static int memindex_map_init = 10000;

static struct KNO_INDEX_HANDLER memindex_handler;

static ssize_t load_memindex(struct KNO_MEMINDEX *memidx);

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

static lispval memindex_fetch(kno_index ix,lispval key)
{
  struct KNO_MEMINDEX *mix = (struct KNO_MEMINDEX *)ix;
  if (! (mix->mix_loaded) ) load_memindex(mix);
  return kno_hashtable_get(&(mix->mix_map),key,EMPTY);
}

static int memindex_fetchsize(kno_index ix,lispval key)
{
  struct KNO_MEMINDEX *mix = (struct KNO_MEMINDEX *)ix;
  if (! (mix->mix_loaded) ) load_memindex(mix);
  lispval v = kno_hashtable_get(&(mix->mix_map),key,EMPTY);
  int size = KNO_CHOICE_SIZE(v);
  kno_decref(v);
  return size;
}

static lispval *memindex_fetchn(kno_index ix,int n,const lispval *keys)
{
  struct KNO_MEMINDEX *mix = (struct KNO_MEMINDEX *)ix;
  if (! (mix->mix_loaded) ) load_memindex(mix);
  struct KNO_HASHTABLE *map = &(mix->mix_map);
  lispval *results = u8_big_alloc_n(n,lispval);
  kno_write_lock_table(map);
  int i = 0; while (i<n) {
    results[i]=kno_hashtable_get_nolock(map,keys[i],EMPTY);
    i++;}
  kno_unlock_table(map);
  return results;
}

static lispval *memindex_fetchkeys(kno_index ix,int *n)
{
  struct KNO_MEMINDEX *mix = (struct KNO_MEMINDEX *)ix;
  if (! (mix->mix_loaded) ) load_memindex(mix);
  lispval keys = kno_hashtable_keys(&(mix->mix_map));
  if (KNO_PRECHOICEP(keys)) keys = kno_simplify_choice(keys);
  if (KNO_CHOICEP(keys)) {
    int count = KNO_CHOICE_SIZE(keys);
    lispval *keyv = u8_big_alloc_n(count,lispval);
    if (KNO_CONS_REFCOUNT(keys) == 1) {
      struct KNO_CHOICE *ch = (kno_choice) keys;
      memmove(keyv,KNO_CHOICE_ELTS(keys),count*LISPVAL_LEN);
      kno_free_choice(ch);}
    else {
      const lispval *elts = KNO_CHOICE_ELTS(keys);
      int i=0; while (i<count) {
        keyv[i] = kno_incref(elts[i]);
        i++;}
      kno_decref(keys);}
    *n=count;
    return keyv;}
  else {
    lispval *one = u8_big_alloc_n(1,lispval);
    *one=keys; *n=1;
    return one;}
}

struct FETCHINFO_STATE {
  struct KNO_KEY_SIZE *sizes;
  struct KNO_CHOICE *filter;
  int i, n;};

static int gather_keysizes(struct KNO_KEYVAL *kv,void *data)
{
  struct FETCHINFO_STATE *state = (struct FETCHINFO_STATE *)data;
  kno_choice filter=state->filter;
  int i = state->i;
  if (i<state->n) {
    lispval key = kv->kv_key;
    if ( (filter==NULL) || (fast_choice_containsp(key,filter)) ) {
      lispval value = kv->kv_val;
      int size = KNO_CHOICE_SIZE(value);
      state->sizes[i].keysize_key = key;
      kno_incref(key);
      state->sizes[i].keysize_count = KNO_INT(size);
      state->i++;}}
  return 0;
}

static struct KNO_KEY_SIZE *memindex_fetchinfo(kno_index ix,kno_choice filter,int *n)
{
  struct KNO_MEMINDEX *mix = (struct KNO_MEMINDEX *)ix;
  if (! (mix->mix_loaded) ) load_memindex(mix);
  int n_keys = (filter == NULL) ? (mix->mix_map.table_n_keys) :
    (KNO_XCHOICE_SIZE(filter));
  struct KNO_KEY_SIZE *keysizes = u8_big_alloc_n(n_keys,struct KNO_KEY_SIZE);
  struct FETCHINFO_STATE state={keysizes,filter,0,n_keys};
  kno_for_hashtable_kv(&(mix->mix_map),gather_keysizes,(void *)&state,1);
  *n = state.i;
  return keysizes;
}

static int memindex_save(struct KNO_INDEX *ix,
                         struct KNO_CONST_KEYVAL *adds,int n_adds,
                         struct KNO_CONST_KEYVAL *drops,int n_drops,
                         struct KNO_CONST_KEYVAL *stores,int n_stores,
                         lispval changed_metadata)
{
  struct KNO_MEMINDEX *memidx = (struct KNO_MEMINDEX *)ix;
  unsigned long long n_changes = n_adds + n_drops + n_stores;

  if (n_changes == 0) return 0;

  /* Now write the adds and edits to disk */
  kno_stream stream = &(memidx->index_stream);
  kno_inbuf in = ( kno_lock_stream(stream), kno_start_read(stream,0x08) );
  unsigned long long n_entries = kno_read_8bytes(in);
  size_t end = kno_read_8bytes(in);
  kno_outbuf out = kno_start_write(stream,end);

  int i=0; while (i<n_adds) {
    kno_write_byte(out,1);
    kno_write_dtype(out,adds[i].kv_key);
    kno_write_dtype(out,adds[i].kv_val);
    i++;}

  i=0; while (i<n_drops) {
    kno_write_byte(out,(unsigned char)-1);
    kno_write_dtype(out,drops[i].kv_key);
    kno_write_dtype(out,drops[i].kv_val);
    i++;}

  i=0; while (i<n_stores) {
    kno_write_byte(out,(unsigned char)0);
    kno_write_dtype(out,stores[i].kv_key);
    kno_write_dtype(out,stores[i].kv_val);
    i++;}

  end = kno_getpos(stream);

  kno_setpos(stream,0x08); out = kno_writebuf(stream);

  kno_write_8bytes(out,n_entries+n_changes);
  kno_write_8bytes(out,end);

  kno_flush_stream(stream);

  kno_unlock_stream(stream);

  if (memidx->mix_loaded) {
    kno_reset_hashtable(&memidx->mix_map,17,1);
    memidx->mix_loaded=0;}

  u8_logf(LOG_NOTICE,"MemIndex/Finished",
          "Finished writing %lld/%lld changes to disk for %s, endpos=%lld",
          n_changes,n_entries,ix->indexid,end);

  return n_changes;
}

static int memindex_commit(kno_index ix,kno_commit_phase phase,
                           struct KNO_INDEX_COMMITS *commit)
{
  switch (phase) {
  case kno_commit_write: {
    int rv = memindex_save(ix,
                           (struct KNO_CONST_KEYVAL *)commit->commit_adds,
                           commit->commit_n_adds,
                           (struct KNO_CONST_KEYVAL *)commit->commit_drops,
                           commit->commit_n_drops,
                           (struct KNO_CONST_KEYVAL *)commit->commit_stores,
                           commit->commit_n_stores,
                           commit->commit_metadata);
    if (rv<0) commit->commit_phase = kno_commit_rollback;
    else commit->commit_phase = kno_commit_flush;
    return rv;}
  default: {
    u8_logf(LOG_INFO,"NoPhasedCommit",
            "The index %s doesn't support phased commits",
            ix->indexid);
    return 0;}
  }
}

static int simplify_choice(struct KNO_KEYVAL *kv,void *data)
{
  if (PRECHOICEP(kv->kv_val))
    kv->kv_val = kno_simplify_choice(kv->kv_val);
  return 0;
}

static ssize_t load_memindex(struct KNO_MEMINDEX *memidx)
{
  struct KNO_STREAM *stream = &(memidx->index_stream);
  if (memidx->mix_loaded) return 0;
  else if (kno_lock_stream(stream)<0) return -1;
  else if (memidx->mix_loaded) {
    kno_unlock_stream(stream);
    return 0;}
  kno_inbuf in = kno_start_read(stream,8);
  long long i = 0, n_entries = kno_read_8bytes(in);
  double started = u8_elapsed_time();
  kno_hashtable mix_map = &(memidx->mix_map);
  u8_logf(LOG_INFO,"MemIndexLoad",
          "Loading %lld entries for '%s'",n_entries,memidx->indexid);
  memidx->mix_valid_data = kno_read_8bytes(in);
  int rv = ftruncate(stream->stream_fileno,memidx->mix_valid_data);
  if (rv<0) truncate_failed(stream->stream_fileno,stream->streamid);
  kno_setpos(stream,256);
  if (n_entries<memindex_map_init)
    kno_resize_hashtable(mix_map,memindex_map_init);
  else kno_resize_hashtable(mix_map,1.5*n_entries);
  kno_write_lock_table(mix_map);
  in = kno_readbuf(stream);
  while (i<n_entries) {
    char op = kno_read_byte(in);
    lispval key = kno_read_dtype(in);
    lispval value = kno_read_dtype(in);
    kno_hashtable_op_nolock
      (mix_map,( (op<0) ? (kno_table_drop) :
                 (op==0) ? (kno_table_store_noref):
                 (kno_table_add_noref)),
       key,value);
    kno_decref(key);
    if (op<0) kno_decref(value);
    i++;}
  kno_for_hashtable_kv(mix_map,simplify_choice,NULL,0);
  kno_unlock_table(mix_map);
  memidx->mix_loaded = 1;
  kno_unlock_stream(stream);
  u8_logf(LOG_NOTICE,"MemIndexLoad",
          "Loaded %lld entries for '%s' in %fs",
          n_entries,memidx->indexid,u8_elapsed_time()-started);
  return 1;
}

static lispval preload_opt;

static kno_index open_memindex(u8_string file,kno_storage_flags flags,
                              lispval opts)
{
  struct KNO_MEMINDEX *memidx = u8_alloc(struct KNO_MEMINDEX);
  u8_string abspath = u8_abspath(file,NULL);
  u8_string realpath = u8_realpath(file,NULL);
  kno_init_index((kno_index)memidx,&memindex_handler,
                file,abspath,realpath,
                flags|KNO_STORAGE_NOSWAP,
                VOID,
                opts);
  struct KNO_STREAM *stream=
    kno_init_file_stream(&(memidx->index_stream),abspath,
                        KNO_FILE_MODIFY,-1,
                        kno_driver_bufsize);
  lispval preload = kno_getopt(opts,preload_opt,KNO_TRUE);
  u8_free(abspath); u8_free(realpath);
  if (!(stream)) return NULL;
  stream->stream_flags &= ~KNO_STREAM_IS_CONSED;
  unsigned int magic_no = kno_read_4bytes(kno_readbuf(stream));
  if (magic_no!=KNO_MEMINDEX_MAGIC_NUMBER) {
    kno_seterr(_("NotMemindex"),"open_memindex",file,VOID);
    kno_close_stream(stream,0);
    u8_free(memidx);
    return NULL;}
  else {
    kno_inbuf in = kno_readbuf(stream);
    unsigned U8_MAYBE_UNUSED int n_keys = kno_read_4bytes(in);
    long long n_entries = kno_read_8bytes(in);
    memidx->mix_valid_data = kno_read_8bytes(in);
    int rv = ftruncate(stream->stream_fileno,memidx->mix_valid_data);
    if (rv<0) truncate_failed(stream->stream_fileno,stream->streamid);
    kno_setpos(stream,256);
    if (n_entries<memindex_map_init)
      kno_init_hashtable(&(memidx->mix_map),memindex_map_init,NULL);
    else kno_init_hashtable(&(memidx->mix_map),1.5*n_entries,NULL);
    if (!(FALSEP(preload)))
      load_memindex(memidx);
    if (!(U8_BITP(flags,KNO_STORAGE_UNREGISTERED)))
      kno_register_index((kno_index)memidx);
    return (kno_index)memidx;}
}

static lispval memindex_ctl(kno_index ix,lispval op,int n,kno_argvec args)
{
  struct KNO_MEMINDEX *mix = (struct KNO_MEMINDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return kno_err("BadIndexOpCall","hashindex_ctl",
                  mix->indexid,VOID);
  else if (op == kno_cachelevel_op) {
    if (mix->mix_loaded)
      return KNO_INT(3);
    else return KNO_INT(0);}
  else if (op == kno_getmap_op) {
    if (mix->mix_loaded==0) load_memindex(mix);
    return (lispval) kno_copy_hashtable(NULL,&(mix->mix_map),1);}
  else if (op == kno_preload_op) {
    if (mix->mix_loaded==0) load_memindex(mix);
    return KNO_TRUE;}
  else if (op == kno_capacity_op)
    return EMPTY;
  else if (op == kno_swapout_op) {
    if (mix->mix_loaded) {
      mix->mix_loaded=0;
      kno_reset_hashtable(&(mix->mix_map),17,1);
      return KNO_TRUE;}
    return KNO_INT(mix->mix_map.table_n_keys);}
  else if (op == kno_load_op) {
    if (mix->mix_loaded == 0) load_memindex(mix);
    return KNO_INT(mix->mix_map.table_n_keys);}
  else if (op == kno_keycount_op) {
    if (mix->mix_loaded == 0) load_memindex(mix);
    return KNO_INT(mix->mix_map.table_n_keys);}
  else return kno_default_indexctl(ix,op,n,args);
}

KNO_EXPORT int kno_make_memindex(u8_string spec)
{
  struct KNO_STREAM _stream;
  struct KNO_STREAM *stream=
    kno_init_file_stream(&_stream,spec,KNO_FILE_CREATE,-1,kno_driver_bufsize);
  kno_outbuf out = kno_writebuf(stream);
  int i = 24;
  kno_write_4bytes(out,KNO_MEMINDEX_MAGIC_NUMBER);
  kno_write_4bytes(out,0); /* n_keys */
  kno_write_8bytes(out,0); /* n_entries */
  kno_write_8bytes(out,256); /* valid_data */
  while (i<256) {kno_write_4bytes(out,0); i = i+4;}
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  return 1;
}

static kno_index memindex_create(u8_string spec,void *type_data,
                                kno_storage_flags flags,
                                lispval opts)
{
  if (kno_make_memindex(spec)>=0) {
    kno_set_file_opts(spec,opts);
    return kno_open_index(spec,flags,VOID);}
  else return NULL;
}

/* Initializing the driver module */

static struct KNO_INDEX_HANDLER memindex_handler={
  "memindex", 1, sizeof(struct KNO_MEMINDEX), 14,
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

KNO_EXPORT void kno_init_memindex_c()
{
  preload_opt = kno_intern("preload");

  kno_register_index_type("memindex",
                         &memindex_handler,
                         open_memindex,
                         kno_match_index_file,
                         (void *) U8_INT2PTR(KNO_MEMINDEX_MAGIC_NUMBER));

  u8_register_source_file(_FILEINFO);
}

