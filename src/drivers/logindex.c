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

#include "libu8/u8filefns.h"
#include "libu8/u8printf.h"

static int logindex_cache_init=10000;
static int logindex_adds_init=5000;
static int logindex_edits_init=1000;

static struct FD_INDEX_HANDLER log_index_handler;

/* The in-memory index */

static fdtype *log_index_fetchkeys(fd_index ix,int *n)
{
  fdtype keys=fd_hashtable_keys(&(ix->index_cache));
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

static fdtype drop_symbol, set_symbol;

static int write_add(struct FD_KEYVAL *kv,void *lixptr)
{
  struct FD_LOG_INDEX *logix=(struct FD_LOG_INDEX *)lixptr;
  struct FD_STREAM *stream=&(logix->index_stream);
  struct FD_OUTBUF *out=fd_writebuf(stream);
  fd_write_byte(out,1);
  fd_write_dtype(out,kv->kv_key);
  fd_write_dtype(out,kv->kv_val);
  return 1;
}

static int write_edit(struct FD_KEYVAL *kv,void *lixptr)
{
  struct FD_LOG_INDEX *logix=(struct FD_LOG_INDEX *)lixptr;
  struct FD_STREAM *stream=&(logix->index_stream);
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
  return 1;
}

static int log_index_commit(fd_index ix)
{
  struct FD_LOG_INDEX *logix=(struct FD_LOG_INDEX *)ix;
  struct FD_HASHTABLE *adds=&(ix->index_cache), *edits=&(ix->index_cache);
  struct FD_HASHTABLE _adds, _edits;
  fd_stream stream=&(logix->index_stream);
  fd_inbuf in=( fd_lock_stream(stream), fd_start_read(stream,0x08) );
  unsigned long long n_entries=fd_read_8bytes(in);
  size_t end=fd_read_8bytes(in);
  fd_outbuf out=fd_start_write(stream,end);

  fd_swap_hashtable(adds,&_adds,256,0);
  n_entries=n_entries+_adds.table_n_keys;
  fd_for_hashtable_kv(&_adds,write_add,(void *)logix,0);
  fd_recycle_hashtable(&_adds);

  fd_swap_hashtable(edits,&_edits,256,0);
  n_entries=n_entries+_edits.table_n_keys;
  fd_for_hashtable_kv(&_edits,write_edit,(void *)logix,0);
  fd_recycle_hashtable(&_edits);

  end=fd_getpos(stream);

  fd_setpos(stream,0x08); out=fd_writebuf(stream);

  fd_write_8bytes(out,n_entries);
  fd_write_8bytes(out,end);

  fd_flush_stream(stream);

  fd_unlock_stream(stream);
}

static fd_index open_log_index(u8_string file,fddb_flags flags)
{
  fdtype lispval; struct FD_HASHTABLE *h;
  struct FD_LOG_INDEX *logix=u8_alloc(struct FD_LOG_INDEX);
  struct FD_STREAM *stream=&(logix->index_stream);
  fd_init_index((fd_index)logix,&log_index_handler,file,flags);
  fd_init_file_stream(stream,file,FD_STREAM_READ,fd_driver_bufsize);
  stream->stream_flags&=~FD_STREAM_IS_MALLOCD;
  unsigned int magic_no=fd_read_4bytes(fd_readbuf(stream));
  if (magic_no!=FD_LOG_INDEX_MAGIC_NUMBER) {
    fd_seterr(_("NotLogIndex"),"open_log_index",u8_strdup(file),FD_VOID);
    fd_close_stream(stream,0);
    u8_free(logix);
    return NULL;}
  else {
    fd_inbuf in=fd_readbuf(stream);
    unsigned int not_used=fd_read_4bytes(in);
    long long i=0, n_entries=fd_read_8bytes(in);
    fd_hashtable cache=&(logix->index_cache);
    logix->lix_valid_data=fd_read_8bytes(in);
    ftruncate(stream->stream_fileno,logix->lix_valid_data);
    fd_setpos(stream,256);
    if (n_entries<logindex_cache_init)
      fd_resize_hashtable(&(logix->index_cache),logindex_cache_init);
    else fd_resize_hashtable(&(logix->index_cache),1.5*n_entries);
    fd_resize_hashtable(&(logix->index_adds),logindex_adds_init);
    fd_resize_hashtable(&(logix->index_edits),logindex_edits_init);
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
    return (fd_index)logix;}
}

FD_EXPORT int fd_make_log_index(u8_string spec)
{
  struct FD_STREAM _stream;
  struct FD_STREAM *stream=
    fd_init_file_stream(&_stream,spec,FD_STREAM_CREATE,fd_driver_bufsize);
  fd_outbuf out=fd_writebuf(stream);
  int i=24;
  fd_write_4bytes(out,FD_LOG_INDEX_MAGIC_NUMBER);
  fd_write_4bytes(out,0); /* n_keys */
  fd_write_8bytes(out,0); /* n_entries */
  fd_write_8bytes(out,256); /* valid_data */
  while (i<256) {fd_write_4bytes(out,0); i=i+4;}
  fd_close_stream(stream,FD_STREAM_FREEDATA);
  return 1;
}

static fd_index log_index_create(u8_string spec,void *type_data,
				 fddb_flags flags,fdtype opts)
{
  if (fd_make_log_index(spec)>=0)
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

static struct FD_INDEX_HANDLER log_index_handler={
  "logindex", 1, sizeof(struct FD_LOG_INDEX), 12,
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
  log_index_create, /* create */
  NULL  /* indexop */
};

FD_EXPORT void fd_init_logindex_c()
{
  drop_symbol=fd_intern("DROP");
  set_symbol=fd_intern("SET");

  fd_register_index_type("logindex",
                         &log_index_handler,
                         open_log_index,
                         match_index_name,
                         (void *) U8_INT2PTR(FD_LOG_INDEX_MAGIC_NUMBER));

  u8_register_source_file(_FILEINFO);
}
