/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* A logindex is a file-based index primarily for writing key/value pairs. It just accumulates keys
   and values in the designated file. Queries on the logindex load all of those keys and values,
   which may take a while. */
   

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"
#define KNO_INLINE_BUFIO (!(KNO_AVOID_INLINE))
#define KNO_INLINE_CHOICES (!(KNO_AVOID_INLINE))
#define KNO_FAST_CHOICE_CONTAINSP (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

#include "headers/logindex.h"

#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

static int logindex_map_init = 10000;

static struct KNO_INDEX_HANDLER logindex_handler;

static ssize_t load_logindex(struct KNO_LOGINDEX *logidx);

static void truncate_failed(int fileno,u8_string file)
{
  int got_err = errno; errno=0;
  if (got_err)
    u8_logf(LOG_WARN,"TruncateFailed",
            "Couldn't truncate logindex file %s (fd=%d) (errno=%d:%s)",
            file,fileno,got_err,u8_strerror(got_err));
  else u8_logf(LOG_WARN,"TruncateFailed",
               "Couldn't truncate logindex file %s (fd=%d)",file,fileno);
}


/* The in-memory index */

static lispval logindex_fetch(kno_index ix,lispval key)
{
  struct KNO_LOGINDEX *logx = (struct KNO_LOGINDEX *)ix;
  if (! (logx->logx_loaded) ) load_logindex(logx);
  return kno_hashtable_get(&(logx->logx_map),key,EMPTY);
}

static int logindex_fetchsize(kno_index ix,lispval key)
{
  struct KNO_LOGINDEX *logx = (struct KNO_LOGINDEX *)ix;
  if (! (logx->logx_loaded) ) load_logindex(logx);
  lispval v = kno_hashtable_get(&(logx->logx_map),key,EMPTY);
  int size = KNO_CHOICE_SIZE(v);
  kno_decref(v);
  return size;
}

static lispval *logindex_fetchn(kno_index ix,int n,const lispval *keys)
{
  struct KNO_LOGINDEX *logx = (struct KNO_LOGINDEX *)ix;
  if (! (logx->logx_loaded) ) load_logindex(logx);
  struct KNO_HASHTABLE *map = &(logx->logx_map);
  lispval *results = u8_big_alloc_n(n,lispval);
  kno_write_lock_table(map);
  int i = 0; while (i<n) {
    results[i]=kno_hashtable_get_nolock(map,keys[i],EMPTY);
    i++;}
  kno_unlock_table(map);
  return results;
}

static lispval *logindex_fetchkeys(kno_index ix,int *n)
{
  struct KNO_LOGINDEX *logx = (struct KNO_LOGINDEX *)ix;
  if (! (logx->logx_loaded) ) load_logindex(logx);
  lispval keys = kno_hashtable_keys(&(logx->logx_map));
  if (KNO_PRECHOICEP(keys)) keys = kno_simplify_choice(keys);
  if (KNO_CHOICEP(keys)) {
    int count = KNO_CHOICE_SIZE(keys);
    lispval *keyv = u8_big_alloc_n(count,lispval);
    if (KNO_REFCOUNT(keys) == 1) {
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

static struct KNO_KEY_SIZE *logindex_fetchinfo(kno_index ix,kno_choice filter,int *n)
{
  struct KNO_LOGINDEX *logx = (struct KNO_LOGINDEX *)ix;
  if (! (logx->logx_loaded) ) load_logindex(logx);
  int n_keys = (filter == NULL) ? (logx->logx_map.table_n_keys) :
    (KNO_XCHOICE_SIZE(filter));
  struct KNO_KEY_SIZE *keysizes = u8_big_alloc_n(n_keys,struct KNO_KEY_SIZE);
  struct FETCHINFO_STATE state={keysizes,filter,0,n_keys};
  kno_for_hashtable_kv(&(logx->logx_map),gather_keysizes,(void *)&state,1);
  *n = state.i;
  return keysizes;
}

static int logindex_save(struct KNO_INDEX *ix,
                         struct KNO_CONST_KEYVAL *adds,int n_adds,
                         struct KNO_CONST_KEYVAL *drops,int n_drops,
                         struct KNO_CONST_KEYVAL *stores,int n_stores,
                         lispval changed_metadata)
{
  struct KNO_LOGINDEX *logidx = (struct KNO_LOGINDEX *)ix;
  unsigned long long n_changes = n_adds + n_drops + n_stores;

  if (n_changes == 0) return 0;

  /* Now write the adds and edits to disk */
  kno_stream stream = &(logidx->index_stream);
  kno_inbuf in = ( kno_lock_stream(stream), kno_start_read(stream,0x08) );
  unsigned long long n_entries = kno_read_8bytes(in);
  size_t end = kno_read_8bytes(in);
  kno_outbuf out = kno_start_write(stream,end);
  xtype_refs xrefs = &(logidx->index_xrefs);

  u8_logf(LOG_NOTICE,"Logindex/Started",
	  "Writing %lld/%lld changes to disk for %s, endpos=%lld",
	  n_changes,n_entries,ix->indexid,end);

  if (xrefs);

  int i=0; while (i<n_adds) {
    kno_write_byte(out,1);
    kno_write_xtype(out,adds[i].kv_key,xrefs);
    kno_write_xtype(out,adds[i].kv_val,xrefs);
    i++;}

  i=0; while (i<n_drops) {
    kno_write_byte(out,(unsigned char)-1);
    kno_write_xtype(out,drops[i].kv_key,xrefs);
    kno_write_xtype(out,drops[i].kv_val,xrefs);
    i++;}

  i=0; while (i<n_stores) {
    kno_write_byte(out,(unsigned char)0);
    kno_write_xtype(out,stores[i].kv_key,xrefs);
    kno_write_xtype(out,stores[i].kv_val,xrefs);
    i++;}

  if ( (xrefs->xt_refs_flags) & (XTYPE_REFS_CHANGED) ) {
     
}



  end = kno_getpos(stream);

  kno_setpos(stream,0x08); out = kno_writebuf(stream);
  kno_write_8bytes(out,n_entries+n_changes);
  kno_write_8bytes(out,end);

  kno_flush_stream(stream);

  kno_unlock_stream(stream);

  if (logidx->logx_loaded) {
    kno_reset_hashtable(&logidx->logx_map,17,1);
    logidx->logx_loaded=0;}

  u8_logf(LOG_NOTICE,"Logindex/Finished",
          "Finished writing %lld/%lld changes to disk for %s, endpos=%lld",
          n_changes,n_entries,ix->indexid,end);

  return n_changes;
}

static int logindex_commit(kno_index ix,kno_commit_phase phase,
                           struct KNO_INDEX_COMMITS *commit)
{
  switch (phase) {
  case kno_commit_write: {
    int rv = logindex_save(ix,
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

static ssize_t load_logindex(struct KNO_LOGINDEX *logidx)
{
  struct KNO_STREAM *stream = &(logidx->index_stream);
  struct KNO_XREFS *xrefs = &(logidx->index_xrefs);
  if (logidx->logx_loaded) return 0;
  else if (kno_lock_stream(stream)<0) return -1;
  else if (logidx->logx_loaded) {
    kno_unlock_stream(stream);
    return 0;}
  kno_inbuf in = kno_start_read(stream,8);
  long long i = 0, n_entries = kno_read_8bytes(in);
  double started = u8_elapsed_time();
  kno_hashtable logx_map = &(logidx->logx_map);
  u8_logf((n_entries<1000)?(LOG_INFO):(n_entries<10000)?(LOG_NOTICE):(LOG_WARN),
	  "LogindexLoad","Loading %lld entries for '%s'",
	  n_entries,logidx->indexid);
  logidx->logx_valid_data = kno_read_8bytes(in);
  int rv = ftruncate(stream->stream_fileno,logidx->logx_valid_data);
  if (rv<0) truncate_failed(stream->stream_fileno,stream->streamid);
  kno_setpos(stream,256);
  if (n_entries<logindex_map_init)
    kno_resize_hashtable(logx_map,logindex_map_init);
  else kno_resize_hashtable(logx_map,1.5*n_entries);
  kno_write_lock_table(logx_map);
  in = kno_readbuf(stream);
  while (i<n_entries) {
    char op = kno_read_byte(in);
    lispval key = kno_read_xtype(in,xrefs);
    lispval value = kno_read_xtype(in,xrefs);
    kno_hashtable_op_nolock
      (logx_map,( (op<0) ? (kno_table_drop) :
		  (op==0) ? (kno_table_store_noref):
		  (kno_table_add_noref)),
       key,value);
    kno_decref(key);
    if (op<0) kno_decref(value);
    i++;}
  kno_for_hashtable_kv(logx_map,simplify_choice,NULL,0);
  kno_unlock_table(logx_map);
  logidx->logx_loaded = 1;
  kno_unlock_stream(stream);
  u8_logf(LOG_NOTICE,"LogindexLoad",
          "Loaded %lld entries for '%s' in %fs",
          n_entries,logidx->indexid,u8_elapsed_time()-started);
  return 1;
}

static lispval preload_opt;

static lispval read_xtype_file(u8_string file)
{
  struct KNO_STREAM _stream, *stream=
    kno_init_file_stream(&_stream,xrefs_file,
			 KNO_FILE_READ,-1,
			 kno_driver_bufsize);
  u8_inbuf in = u8_readbuf(stream);
  lispval result = kno_read_xtype(in,NULL);
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  return result;
}

static kno_index open_logindex
(u8_string file,kno_storage_flags flags,lispval opts)
{
  if (!(u8_directoryp(file))) {
    kno_seterr(_("NotLogindex"),"open_logindex",file,VOID);
    return NULL;}
  u8_string abspath = u8_abspath(file,NULL);
  u8_string realpath = u8_realpath(file,NULL);
  u8_string data_file = u8_mkpath(abspath,"data");
  u8_string xrefs_file = u8_mkpath(abspath,"xrefs");
  u8_string metadata_file = u8_mkpath(abspath,"metadata");
  lispval metadata =  (u8_file_existsp(metadata_file)) ?
    (read_xtype_file(metadata_file)) : (KNO_VOID);
  lispval xrefs_init = (u8_file_existsp(metadata_file)) ?
    (read_xtype_file(metadata_file,NULL)) : (KNO_VOID);
  if (KNO_VOIDP(metadata)) {}
  else if (KNO_TABLEP(metadata)) {}
  else {
    kno_seterr("BadMetadata","open_logindex",file,metadata);
    goto err_exit;}
  if (KNO_VOIDP(xrefs_init)) {}
  else if (KNO_VECTORP(xrefs_init)) {}
  else {
    kno_seterr("BadXRefs","open_logindex",file,metadata);
    goto err_exit;}
  struct KNO_LOGINDEX *logidx = u8_alloc(struct KNO_LOGINDEX);
  kno_init_index((kno_index)logidx,&logindex_handler,
		 file,abspath,realpath,
		 flags|KNO_STORAGE_NOSWAP,
		 metadata,
		 opts);
  if (KNO_VECTORP(xrefs_init)) {
    int n_refs = KNO_VECTOR_LENGTH(refvec);
    int read_only = (logidx->index_flags &  KNO_STORAGE_READONLY) ||
      (!(u8_file_writablep(xrefs_file)));
    int flags = XTYPE_REFS_EXT_ELTS |
      ( (read_only) ? (XTYPE_REFS_READ_ONLY) : (0) );
    kno_init_xrefs(&(logidx->index_xrefs),n_refs,n_refs,n_refs,flags,
		   KNO_VECTOR_ELTS(refvec),
		   NULL);}
  else kno_init_xrefs(xrefs,0,0,0,flags,NULL,NULL);}

  struct KNO_STREAM *stream=
  lispval preload = kno_getopt(opts,preload_opt,KNO_TRUE);
  u8_free(abspath); u8_free(realpath);
  if (!(stream)) return NULL;
  stream->stream_flags &= ~KNO_STREAM_IS_CONSED;
  unsigned int magic_no = kno_read_4bytes(kno_readbuf(stream));
  if (magic_no!=KNO_LOGINDEX_MAGIC_NUMBER) {
    kno_seterr(_("NotLogindex"),"open_logindex",file,VOID);
    kno_close_stream(stream,0);
    u8_free(logidx);
    return NULL;}
  else {
    kno_inbuf in = kno_readbuf(stream);
    unsigned U8_MAYBE_UNUSED int n_keys = kno_read_4bytes(in);
    long long n_entries = kno_read_8bytes(in);
    ssize_t good_endpos = kno_read_8bytes(in);
    ssize_t xrefs_loc = kno_read_8bytes(in);
    ssize_t n_xrefs   = kno_read_4bytes(in);
    ssize_t xrefs_max = kno_read_4bytes(in);
    if ( (n_keys < 0) || (n_entries < 0) || (good_endpos < 0) ||
	 (xrefs_loc < 0) || (n_xrefs < 0) || (xrefs_max < 0) ) {
      kno_seterr(_("BadLogindex"),"open_logindex",file,VOID);
      kno_close_stream(stream,0);
      u8_free(logidx);
      return NULL;}

    /* Truncate the file if needed */
    logidx->logx_valid_data = good_endpos;
    int rv = ftruncate(stream->stream_fileno,logidx->logx_valid_data);
    if (rv<0) truncate_failed(stream->stream_fileno,stream->streamid);

    /* Init xrefs */
    int flags = XTYPE_REFS_ADD_OIDS | XTYPE_REFS_ADD_SYMS;
    int use_max = (xrefs_max>0) ? (xrefs_max) : (-1);
    ssize_t alloc_len = ((n_xrefs/256)+1)*256;
    lispval *refs = u8_alloc_n(alloc_len,sizeof(lispval));
    if (xrefs_loc) {
      kno_setpos(stream,xrefs_loc);
      int i = 0; while (i<n_xrefs) {
	lispval ref = kno_read_xtype(stream,NULL);
	refs[i]=ref;
	i++;}
      while (i<alloc_len) refs[i++]=KNO_VOID;
      kno_init_xrefs(xrefs,n_xrefs,alloc_len,use_max,flags,elts,NULL);}
    else  {
      int i = 0; while (i < alloc_len) refs[i++]=KNO_VOID;
      kno_init_xrefs(xrefs,0,256,use_max,flags,refs,NULL);}

    kno_setpos(stream,256);
    if (n_entries<logindex_map_init)
      kno_init_hashtable(&(logidx->logx_map),logindex_map_init,NULL);
    else kno_init_hashtable(&(logidx->logx_map),1.5*n_entries,NULL);
    if (!(FALSEP(preload)))
      load_logindex(logidx);
    if (!(U8_BITP(flags,KNO_STORAGE_UNREGISTERED)))
      kno_register_index((kno_index)logidx);
    return (kno_index)logidx;}
}

static lispval logindex_ctl(kno_index ix,lispval op,int n,kno_argvec args)
{
  struct KNO_LOGINDEX *logx = (struct KNO_LOGINDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return kno_err("BadIndexOpCall","hashindex_ctl",
                  logx->indexid,VOID);
  else if (op == kno_cachelevel_op) {
    if (logx->logx_loaded)
      return KNO_INT(3);
    else return KNO_INT(0);}
  else if (op == kno_getmap_op) {
    if (logx->logx_loaded==0) load_logindex(logx);
    return (lispval) kno_copy_hashtable(NULL,&(logx->logx_map),1);}
  else if (op == kno_preload_op) {
    if (logx->logx_loaded==0) load_logindex(logx);
    return KNO_TRUE;}
  else if (op == kno_capacity_op)
    return EMPTY;
  else if (op == kno_swapout_op) {
    if (logx->logx_loaded) {
      logx->logx_loaded=0;
      kno_reset_hashtable(&(logx->logx_map),17,1);
      return KNO_TRUE;}
    return KNO_INT(logx->logx_map.table_n_keys);}
  else if (op == kno_load_op) {
    if (logx->logx_loaded == 0) load_logindex(logx);
    return KNO_INT(logx->logx_map.table_n_keys);}
  else if (op == kno_keycount_op) {
    if (logx->logx_loaded == 0) load_logindex(logx);
    return KNO_INT(logx->logx_map.table_n_keys);}
  else if (op == KNOSYM_FILENAME)
    return knostring(ix->index_source);
  else return kno_default_indexctl(ix,op,n,args);
}

KNO_EXPORT int kno_make_logindex(u8_string spec,lispval xrefs,lispval metadata)
{
  int rv = (u8_directoryp(spec)) ? (0) : (u8_mkdir(spec,-1));
  if (rv<0) return rv;
  u8_string data_file = u8_mkpath(spec,"data");
  u8_string xrefs_file = u8_mkpath(spec,"xrefs");
  u8_string metadata_file = u8_mkpath(spec,"metadata");
  lispval rootdir = kno_lispstring(spec);
  lispval created = kno_make_timestamp(NULL);
  lispval origin = kno_lispstring(u8_sessionid());
  struct KNO_STREAM _stream, *stream;

  /* Init data */
  stream=kno_init_file_stream(&_stream,data_file,KNO_FILE_CREATE,-1,kno_driver_bufsize);
  kno_outbuf out = kno_writebuf(stream);
  int i = 24;
  kno_write_4bytes(out,KNO_LOGINDEX_MAGIC_NUMBER);
  kno_write_4bytes(out,0); /* n_keys */
  kno_write_8bytes(out,0); /* n_entries */
  kno_write_8bytes(out,256); /* valid_data */
  while (i<256) {kno_write_4bytes(out,0); i = i+4;}
  kno_close_stream(stream,KNO_STREAM_FREEDATA);

  /* Init xrefs */
  if (KNO_VOIDP(xrefs))
    xrefs = kno_init_vector(NULL,0,NULL);
  else kno_incref(xrefs);
  stream=kno_init_file_stream
    (&_stream,xrefs_file,KNO_FILE_CREATE,-1,kno_driver_bufsize);
  kno_outbuf out = kno_writebuf(stream);
  kno_write_xtype(out,xrefs,NULL);
  kno_close_stream(stream,KNO_STREAM_FREEDATA);

  /* Init metadata */
  if (KNO_VOIDP(metadata))
    metadata = kno_make_slotmap(8,0,NULL);
  else metadata = kno_deep_copy(metadata);
  kno_store(metadata,kno_intern("rootdir"),rootdir);
  kno_store(metadata,kno_intern("created"),created);
  kno_store(metadata,kno_intern("origin"),origin);
  stream=kno_init_file_stream
    (&_stream,metadata_file,KNO_FILE_CREATE,-1,kno_driver_bufsize);
  kno_outbuf out = kno_writebuf(stream);
  kno_write_xtype(out,metadata,NULL);
  kno_close_stream(stream,KNO_STREAM_FREEDATA);

  kno_decref(xrefs); kno_decref(metadata);
  kno_decref(rootdir); kno_decref(created); kno_decref(origin);
  u8_free(data_file); u8_free(xrefs_file); u8_free(metadata_file);

  return 1;
}

static kno_index logindex_create(u8_string spec,void *type_data,
				 kno_storage_flags flags,
				 lispval opts)
{
  lispval xrefs = kno_getopt(opts,KNOSYM_XREFS,KNO_VOID);
  lispval metadata = kno_getopt(opts,KNOSYM_METADATA,KNO_VOID);
  kno_index ix = result;
  if (kno_make_logindex(spec,xrefs,metadata)>=0) {
    int rv = kno_set_file_opts(spec,opts);
    if (rv<0) {
      u8_log(LOG_ERROR,"FailedFileOpts",
	     "Couldn't set specified options for file %s",spec);
      ix=kno_open_index(spec,flags,VOID);}}
  kno_decref(xrefs); kno_decref(metadata);
  return ix;
}

static void logindex_recycle(kno_index ix)
{
  struct KNO_LOGINDEX *lx = (struct KNO_LOGINDEX *)ix;
  kno_recycle_hashtable(&(lx->logx_map));
  if (lx->logx_slotids) u8_free(lx->logx_slotids);
  if (lx->logx_baseoids) u8_free(lx->logx_slotids);
  kno_close_stream(&(lx->index_stream),KNO_STREAM_FREEDATA);
}

/* Initializing the driver module */

static struct KNO_INDEX_HANDLER logindex_handler={
  "logindex", 1, sizeof(struct KNO_LOGINDEX), 14,
  NULL, /* close */
  logindex_commit, /* commit */
  logindex_fetch, /* fetch */
  logindex_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  logindex_fetchn, /* fetchn */
  logindex_fetchkeys, /* fetchkeys */
  logindex_fetchinfo, /* fetchinfo */
  NULL, /* batchadd */
  logindex_create, /* create */
  NULL, /* walk */
  logindex_recycle, /* recycle */
  logindex_ctl  /* indexctl */
};

KNO_EXPORT void kno_init_logindex_c()
{
  preload_opt = kno_intern("preload");

  kno_register_index_type("logindex",
			  &logindex_handler,
			  open_logindex,
			  kno_match_index_file,
			  (void *) U8_INT2PTR(KNO_LOGINDEX_MAGIC_NUMBER));

  u8_register_source_file(_FILEINFO);
}

