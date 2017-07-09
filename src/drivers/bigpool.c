/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_POOLS 1
#define FD_INLINE_BUFIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/storage.h"
#include "framerd/streams.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include "headers/bigpool.h"

#include <libu8/libu8.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>

#include <zlib.h>

#if (HAVE_MMAP)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#define MMAP_FLAGS MAP_SHARED
#endif

#define POOLFILE_LOCKEDP(op) \
  (U8_BITP((op->pool_stream.stream_flags),FD_STREAM_FILE_LOCKED))

FD_FASTOP int LOCK_POOLSTREAM(fd_bigpool bp,u8_string caller)
{
  fd_lock_stream(&(bp->pool_stream));
  if (bp->pool_stream.stream_fileno < 0) {
    u8_seterr("PoolStreamClosed",caller,u8_strdup(bp->poolid));
    fd_unlock_stream(&(bp->pool_stream));
    return -1;}
  else return 1;
}

#define UNLOCK_POOLSTREAM(op) fd_unlock_stream(&(op->pool_stream))

static void reload_offdata(struct FD_BIGPOOL *p);
static void update_modtime(struct FD_BIGPOOL *fp);

/*static int recover_bigpool(struct FD_BIGPOOL *); */

static struct FD_POOL_HANDLER bigpool_handler;

static fd_exception InvalidOffset=_("Invalid offset in BIGPOOL");

static unsigned int *get_offdata(struct FD_BIGPOOL *bp)
{
  if (bp->bigpool_offdata) {
    u8_read_lock(&(bp->bigpool_offdata_lock));
    if (bp->bigpool_offdata)
      return bp->bigpool_offdata;
    else {
      u8_rw_unlock(&(bp->bigpool_offdata_lock));
      return NULL;}}
  else return NULL;
}

static void release_offdata(struct FD_BIGPOOL *bp)
{
  if (bp->bigpool_offdata)
    u8_rw_unlock(&(bp->bigpool_offdata_lock));
}

/* BIGPOOLs are the next generation of object pool data file.  While
    previous formats have all stored OIDs for years and years, BIGPOOLs
    are supposed to be the best to date and somewhat paradigmatic.
   The design focuses on performance and extensibility.  With FD_HASHINDEX,
    it became clear that storing offset sizes helped with performance and
    with the use of compression, it becomes even more relevant.
   BIGPOOLs are also the first native files to support files >4GB and
    use three different offset models, described below.
*/

/* Layout of new BIGPOOL files
   [256 bytes of header]
   [offset table]
   ...data items...

   Header consists of

   0x00 XXXX     Magic number
   0x04 XXXX     Base OID of pool (8 bytes)
        XXXX      (64 bits)
   0x0c XXXX     Capacity of pool
   0x10 XXXX     Load of pool
   0x14 XXXX     Pool information bits/flags
   0x18 XXXX     file offset of the pool label (a dtype) (8 bytes)
        XXXX      (64 bits)
   0x20 XXXX     byte length of label dtype (4 bytes)
   0x24 XXXX     file offset of pool metadata (a dtype) (8 bytes)
        XXXX      (64 bits)
   0x2c XXXX     byte length of pool metadata representation (4 bytes)
   0x30 XXXX     pool creation time_t (8 bytes)
        XXXX      (64 bits)
   0x38 XXXX     pool repack time_t (8 bytes)
        XXXX      (64 bits)
   0x40 XXXX     pool modification time_t (8 bytes)
        XXXX      (64 bits)
   0x48 XXXX     repack generation (8 bytes)
        XXXX      (64 bits)
   0x50 XXXX     number of registered slotids
   0x54 XXXX     file offset of the slotids block
        XXXX      (64 bits)
   0x5c XXXX     size of slotids dtype representation

   0x60 XXXX     number of value blocks written  (8 bytes)
   0x64 XXXX      (64 bits)
   0xa0 XXXX     end of valid data (8 bytes)
        XXXX

   The flags are divided as follows:
     MASK        INTERPRETATION
     0x0003      Structure of offsets table:
                    0= 4 bytes of position followed by 4 bytes of length (32B form)
                    1= 5 bytes of position followed by 3 bytes of length (40B form)
                    2= 8 bytes of position followed by 4 bytes of length (64B form)
                    3= reserved for future use
     0x001c      Compression function for blocks:
                    0= no compression
                    1= libz compression (level 6)
                    2= libz compression (level 9)
                    3= snappy compression
                    4-7 reserved for future use
     0x0020      Read Only: set if this pool is intended to be read-only
     0x0040      Phased: set if this pool should implement phased commits

   The offsets block starts at 0x100 and goes for either capacity*8 or
    capacity*12 bytes.  The offset values are stored as pairs of
    big-endian binary representations.  For the 32B and 64B forms,
    these are just straightforward integers of the same size.  For the
    40B form, which is designed to better use memory and cache, the
    high 8 bits of the second word are taken as the high eight bits of
    a forty-byte offset.

*/

/* Getting chunk refs */

typedef long long int ll;
typedef unsigned long long ull;

static int get_chunk_ref_size(fd_bigpool p)
{
  switch (p->bigpool_offtype) {
  case FD_B32: case FD_B40: return 8;
  case FD_B64: return 12;}
  return -1;
}

static size_t get_maxpos(fd_bigpool p)
{
  switch (p->bigpool_offtype) {
  case FD_B32:
    return ((size_t)(((size_t)1)<<32));
  case FD_B40:
    return ((size_t)(((size_t)1)<<40));
  case FD_B64:
    return ((size_t)(((size_t)1)<<63));
  default:
    return -1;}
}

/* Making and opening bigpools */

static fd_pool open_bigpool(u8_string fname,fd_storage_flags open_flags,
                            lispval opts)
{
  FD_OID base = FD_NULL_OID_INIT;
  unsigned int hi, lo, magicno, capacity, load, n_slotids, bigpool_format = 0;
  fd_off_t label_loc, slotids_loc;
  lispval label;
  struct FD_BIGPOOL *pool = u8_alloc(struct FD_BIGPOOL);
  int read_only = U8_BITP(open_flags,FD_STORAGE_READ_ONLY) ||
    (!(u8_file_writablep(fname)));
  fd_stream_mode mode=
    ((read_only) ? (FD_FILE_READ) : (FD_FILE_MODIFY));
  u8_string rname = u8_realpath(fname,NULL);
  int cache_level = fd_fixopt(opts,"CACHELEVEL",fd_default_cache_level);
  int stream_flags = FD_STREAM_CAN_SEEK | FD_STREAM_NEEDS_LOCK |
    ( (read_only) ? (FD_STREAM_READ_ONLY) : (0) ) |
    ( (cache_level>=3) ? (FD_STREAM_USEMMAP) : (0) );
  struct FD_STREAM *stream=
    fd_init_file_stream(&(pool->pool_stream),fname,
                        mode,stream_flags,fd_driver_bufsize);
  struct FD_INBUF *instream = (stream) ? (fd_readbuf(stream)) : (NULL);
  if (instream == NULL) {
    u8_raise(fd_FileNotFound,"open_bigpool",u8_strdup(fname));
    return NULL;}

  stream->stream_flags &= ~FD_STREAM_IS_CONSED;
  magicno = fd_read_4bytes(instream);
  if (magicno!=FD_BIGPOOL_MAGIC_NUMBER) {
    fd_seterr(_("Not a bigpool"),"open_bigpool",fname,VOID);
    return NULL;}
  /* Read POOL base etc. */
  hi = fd_read_4bytes(instream); lo = fd_read_4bytes(instream);
  FD_SET_OID_HI(base,hi); FD_SET_OID_LO(base,lo);
  pool->pool_capacity = capacity = fd_read_4bytes(instream);
  pool->pool_load = load = fd_read_4bytes(instream);
  bigpool_format = fd_read_4bytes(instream);
  pool->bigpool_format = bigpool_format;

  if ((U8_BITP(bigpool_format,FD_BIGPOOL_READ_ONLY))&&
      (!(fd_testopt(opts,FDSYM_READONLY,FD_FALSE)))) {
    /* If the pool is intrinsically read-only make it so. */
    fd_unlock_stream(stream);
    fd_close_stream(stream,0);
    fd_init_file_stream(stream,fname,
                        FD_FILE_READ,
                        FD_STREAM_READ_ONLY,
                        fd_driver_bufsize);
    fd_lock_stream(stream);
    fd_setpos(stream,FD_BIGPOOL_LABEL_POS);
    open_flags |= FD_STORAGE_READ_ONLY;}

  pool->bigpool_offtype =
    (fd_offset_type)((bigpool_format)&(FD_BIGPOOL_OFFMODE));
  pool->bigpool_compression=
    fd_compression_type(opts,
                        (fd_compress_type)
                        (((bigpool_format)&(FD_BIGPOOL_COMPRESSION))>>3));
  fd_init_pool((fd_pool)pool,base,capacity,&bigpool_handler,fname,rname);

  if ((U8_BITP(bigpool_format,FD_BIGPOOL_ADJUNCT))&&
      (!(fd_testopt(opts,FDSYM_ISADJUNCT,FD_FALSE))))
    open_flags |= FD_POOL_ADJUNCT;
  if (U8_BITP(bigpool_format,FD_BIGPOOL_SPARSE))
    open_flags |= FD_POOL_SPARSE;

  pool->pool_flags=open_flags;

  u8_free(rname); /* Done with this */

  /* Get the label location */
  label_loc = fd_read_8bytes(instream);
  /* label_size = */ fd_read_4bytes(instream);
  /* Skip the metadata field and size*/
  fd_read_8bytes(instream); fd_read_4bytes(instream);
  /* Creation time */
  fd_read_8bytes(instream);
  /* Repack time */
  fd_read_8bytes(instream);
  /* Mod time */
  fd_read_8bytes(instream);
  /* Repack generation */
  fd_read_8bytes(instream);
  /* Now at 0x50 */
  n_slotids = fd_read_4bytes(instream);
  /* Read and initialize the slotids_loc */
  slotids_loc = fd_read_8bytes(instream);
  fd_read_4bytes(instream); /* Ignore size */
  if (label_loc) {
    if (fd_setpos(stream,label_loc)>0) {
      label = fd_read_dtype(instream);
      if (STRINGP(label)) pool->pool_label = u8_strdup(CSTRING(label));
      else u8_log(LOG_WARN,fd_BadFilePoolLabel,fd_lisp2string(label));
      fd_decref(label);}
    else {
      fd_seterr(fd_BadFilePoolLabel,"open_bigpool","bad label loc",
                FD_INT(label_loc));
      fd_close_stream(stream,0);
      u8_free(rname); u8_free(pool);
      return NULL;}}
  if ((n_slotids)&&(slotids_loc)) {
    int slotids_length = (n_slotids>256)?(n_slotids*2):(256);
    lispval *slotids = u8_alloc_n(slotids_length,lispval);
    struct FD_HASHTABLE *slotcodes = &(pool->slotcodes);
    int i = 0;
    fd_init_hashtable(slotcodes,n_slotids,NULL);
    FD_SET_CONS_TYPE(&(pool->slotcodes),fd_hashtable_type);
    fd_setpos(stream,slotids_loc);
    while (i<n_slotids) {
      lispval slotid = fd_read_dtype(instream);
      slotids[i]=slotid;
      fd_hashtable_store(slotcodes,slotid,FD_INT(i));
      i++;}
    pool->bigpool_slotids = slotids;
    pool->bigpool_n_slotids = n_slotids;
    pool->bigpool_slotids_length = slotids_length;}
  else {
    pool->bigpool_slotids = u8_alloc_n(256,lispval);
    pool->bigpool_n_slotids = 0; pool->bigpool_slotids_length = 256;
    fd_init_hashtable(&(pool->slotcodes),256,NULL);}
  /* Offsets size is the malloc'd size (in unsigned ints) of the
     offsets.  We don't fill this in until we actually need it. */
  pool->bigpool_offdata = NULL;
  pool->bigpool_offdata_length = 0;
  u8_init_rwlock(&pool->bigpool_offdata_lock);
  if (read_only)
    U8_SETBITS(pool->pool_flags,FD_STORAGE_READ_ONLY);
  else U8_CLEARBITS(pool->pool_flags,FD_STORAGE_READ_ONLY);
  fd_register_pool((fd_pool)pool);
  update_modtime(pool);
  return (fd_pool)pool;
}

static void update_modtime(struct FD_BIGPOOL *fp)
{
  struct stat fileinfo;
  if ((fstat(fp->pool_stream.stream_fileno,&fileinfo))<0)
    fp->pool_modtime = (time_t)-1;
  else fp->pool_modtime = fileinfo.st_mtime;
}

/* Getting slotids */

static int grow_slotcodes(struct FD_BIGPOOL *bp)
{
  lispval *slotids = bp->bigpool_slotids;
  size_t cur_length = bp->bigpool_slotids_length;
  size_t new_length = cur_length*2;
  lispval *newslotids = u8_alloc_n(new_length,lispval);
  if (newslotids == NULL) return -1;
  else {
    memcpy(newslotids,slotids,sizeof(lispval)*cur_length);
    if (bp->bigpool_old_slotids) u8_free(bp->bigpool_old_slotids);
    bp->bigpool_slotids_length = new_length;
    bp->bigpool_old_slotids = slotids;
    bp->bigpool_slotids = newslotids;
    return 1;}
}

static int add_slotcode(struct FD_BIGPOOL *bp,lispval slotid)
{
  struct FD_HASHTABLE *slotcodes = &(bp->slotcodes);
  u8_write_lock(&(slotcodes->table_rwlock)); {
    lispval *slotids = bp->bigpool_slotids;
    lispval v = fd_hashtable_get_nolock(slotcodes,slotid,VOID);
    if (FD_UINTP(v)) {
      /* Another thread got here first */
      u8_rw_unlock(&(slotcodes->table_rwlock));
      return FIX2INT(v);}
    else {
      if (bp->bigpool_n_slotids>=bp->bigpool_slotids_length) {
        if (grow_slotcodes(bp)<0) {
          u8_rw_unlock(&(slotcodes->table_rwlock));
          /* This keeps lookup from trying again */
          fd_hashtable_store(slotcodes,slotid,FD_INT(-1));
          return -1;}
        else slotids = bp->bigpool_slotids;}
      int use_code = bp->bigpool_n_slotids++;
      slotids[use_code]=slotid;
      bp->bigpool_added_slotids++;
      fd_hashtable_op_nolock(slotcodes,fd_table_store,slotid,FD_INT(use_code));
      u8_rw_unlock(&(slotcodes->table_rwlock));
      return use_code;}}
}

FD_FASTOP int get_slotcode(struct FD_BIGPOOL *bp,lispval slotid)
{
  struct FD_HASHTABLE *slotcodes = &(bp->slotcodes);
  lispval v = fd_hashtable_get(slotcodes,slotid,VOID);
  if (FIXNUMP(v)) return FIX2INT(v);
  else if (CONSP(slotid)) return -1;
  else return add_slotcode(bp,slotid);
}

/* Load reading and updates */

/* These assume that the pool itself is locked */
static int write_bigpool_load(fd_bigpool bp)
{
  if (FD_POOLFILE_LOCKEDP(bp)) {
    /* Update the load */
    long long load;
    fd_stream stream = &(bp->pool_stream);
    load = fd_read_4bytes_at(stream,16);
    if (load<0) {
      return -1;}
    else if (bp->pool_load>load) {
      int rv = fd_write_4bytes_at(stream,bp->pool_load,16);
      if (rv<0) return rv;
      else return rv;}
    else {
      return 0;}}
  else return 0;
}

static int read_bigpool_load(fd_bigpool bp)
{
  long long load;
  fd_stream stream = &(bp->pool_stream);
  if (POOLFILE_LOCKEDP(bp)) {
    return bp->pool_load;}
   if (fd_lockfile(stream)<0) return -1;
  load = fd_read_4bytes_at(stream,16);
  if (load<0) {
    fd_unlockfile(stream);
     return -1;}
  fd_unlockfile(stream);
  bp->pool_load = load;
  return load;
}

/* These assume that the pool itself is locked */
static int write_bigpool_slotids(fd_bigpool bp)
{
  if (bp->bigpool_added_slotids) {
    if (FD_POOLFILE_LOCKEDP(bp)) {
      lispval *slotids = bp->bigpool_slotids;
      unsigned int n_slotids = bp->bigpool_n_slotids;
      fd_stream stream = &(bp->pool_stream);
      off_t start_pos = fd_endpos(stream), end_pos = start_pos;
      fd_outbuf out = fd_writebuf(stream);
      int i = 0, lim = bp->bigpool_n_slotids; while (i<lim) {
        lispval slotid = slotids[i++];
        ssize_t size = fd_write_dtype(out,slotid);
        if (size<0) return -1;
        else end_pos+=size;}
      fd_write_4bytes_at(stream,n_slotids,0x50);
      fd_write_8bytes_at(stream,start_pos,0x54);
      fd_write_4bytes_at(stream,end_pos-start_pos,0x5c);
      bp->bigpool_added_slotids = 0;
      return 1;}
    else return 0;}
  else return 0;
}

/* Lock the underlying Bigpool */

static int lock_bigpool_file(struct FD_BIGPOOL *bp,int use_mutex)
{
  if (FD_POOLFILE_LOCKEDP(bp)) return 1;
  else if ((bp->pool_flags)&(FD_STORAGE_READ_ONLY))
    return 0;
  else {
    struct FD_STREAM *s = &(bp->pool_stream);
    struct stat fileinfo;
    if (use_mutex) fd_lock_pool((fd_pool)bp);
    if (FD_POOLFILE_LOCKEDP(bp)) {
      /* Another thread got here first */
      if (use_mutex) fd_unlock_pool((fd_pool)bp);
      return 1;}
    LOCK_POOLSTREAM(bp,"lock_bigpool_file");
    if (fd_lockfile(s)==0) {
      fd_unlock_stream(s);
      if (use_mutex) fd_unlock_pool((fd_pool)bp);
      UNLOCK_POOLSTREAM(bp);
      return 0;}
    fstat( s->stream_fileno, &fileinfo);
    if ( fileinfo.st_mtime > bp->pool_modtime ) {
      /* Make sure we're up to date. */
      read_bigpool_load(bp);
      if (bp->bigpool_offdata)
        reload_offdata(bp);
      else {
        fd_reset_hashtable(&(bp->pool_cache),-1,1);
        fd_reset_hashtable(&(bp->pool_changes),32,1);}}
    UNLOCK_POOLSTREAM(bp);
    if (use_mutex) fd_unlock_pool((fd_pool)bp);
    return 1;}
}

/* BIGPOOL operations */

static int make_bigpool
  (u8_string fname,u8_string label,
   FD_OID base,unsigned int capacity,unsigned int load,
   unsigned int flags,lispval slotids_init,
   time_t ctime,time_t mtime,int cycles)
{
  time_t now = time(NULL);
  fd_off_t slotids_pos = 0, metadata_pos = 0, label_pos = 0;
  size_t slotids_size = 0, metadata_size = 0, label_size = 0;
  struct FD_STREAM _stream, *stream=
    fd_init_file_stream(&_stream,fname,
                        FD_FILE_CREATE,-1,
                        fd_driver_bufsize);
  fd_outbuf outstream = (stream) ? (fd_writebuf(stream)) : (NULL);
  fd_offset_type offtype = (fd_offset_type)((flags)&(FD_BIGPOOL_OFFMODE));

  if (outstream == NULL) return -1;
  else if ((stream->stream_flags)&FD_STREAM_READ_ONLY) {
    fd_seterr3(fd_CantWrite,"fd_make_bigpool",fname);
    fd_free_stream(stream);
    return -1;}

  u8_log(LOG_INFO,"CreateBigPool",
         "Creating a bigpool '%s' for %u OIDs based at %x/%x",
         fname,capacity,FD_OID_HI(base),FD_OID_LO(base));

  stream->stream_flags &= ~FD_STREAM_IS_CONSED;
  fd_lock_stream(stream);
  fd_setpos(stream,0);
  fd_write_4bytes(outstream,FD_BIGPOOL_MAGIC_NUMBER);
  fd_write_4bytes(outstream,FD_OID_HI(base));
  fd_write_4bytes(outstream,FD_OID_LO(base));
  fd_write_4bytes(outstream,capacity);
  fd_write_4bytes(outstream,load);
  fd_write_4bytes(outstream,flags);

  fd_write_8bytes(outstream,0); /* Pool label */
  fd_write_4bytes(outstream,0); /* Pool label */

  fd_write_8bytes(outstream,0); /* metadata */
  fd_write_4bytes(outstream,0); /* metadata */

  /* Write the index creation time */
  if (ctime<0) ctime = now;
  fd_write_4bytes(outstream,0);
  fd_write_4bytes(outstream,((unsigned int)ctime));

  /* Write the index repack time */
  fd_write_4bytes(outstream,0);
  fd_write_4bytes(outstream,((unsigned int)now));

  /* Write the index modification time */
  if (mtime<0) mtime = now;
  fd_write_4bytes(outstream,0);
  fd_write_4bytes(outstream,((unsigned int)mtime));

  /* Write the number of repack cycles */
  if (mtime<0) mtime = now;
  fd_write_4bytes(outstream,0);
  fd_write_4bytes(outstream,cycles);

  /* Write number of slotids (if any) */
  if (VECTORP(slotids_init))
    fd_write_4bytes(outstream,VEC_LEN(slotids_init));
  else fd_write_4bytes(outstream,0);
  fd_write_8bytes(outstream,0); /* slotids offset (may be changed) */
  fd_write_4bytes(outstream,0); /* slotids dtype length (may be changed) */

  /* Fill the rest of the space. */
  {
    int i = 0, bytes_to_write = 256-fd_getpos(stream);
    while (i<bytes_to_write) {
      fd_write_byte(outstream,0); i++;}}

  /* Write the top level bucket table */
  {
    int i = 0;
    if ((offtype == FD_B32) || (offtype == FD_B40))
      while (i<capacity) {
        fd_write_4bytes(outstream,0);
        fd_write_4bytes(outstream,0);
        i++;}
    else {
      while (i<capacity) {
        fd_write_8bytes(outstream,0);
        fd_write_4bytes(outstream,0);
        i++;}}}

  if (label) {
    int len = strlen(label);
    label_pos = fd_getpos(stream);
    fd_write_byte(outstream,dt_string);
    fd_write_4bytes(outstream,len);
    fd_write_bytes(outstream,label,len);
    label_size = fd_getpos(stream)-label_pos;}

  /* Write the schemas */
  if (VECTORP(slotids_init)) {
    int i = 0, len = VEC_LEN(slotids_init);
    slotids_pos = fd_getpos(stream);
    while (i<len) {
      lispval slotid = VEC_REF(slotids_init,i);
      fd_write_dtype(outstream,slotid);
      i++;}
    slotids_size = fd_getpos(stream)-slotids_pos;}

  if (label_pos) {
    fd_setpos(stream,FD_BIGPOOL_LABEL_POS);
    fd_write_8bytes(outstream,label_pos);
    fd_write_4bytes(outstream,label_size);}
  if (metadata_pos) {
    fd_setpos(stream,FD_BIGPOOL_METADATA_POS);
    fd_write_8bytes(outstream,metadata_pos);
    fd_write_4bytes(outstream,metadata_size);}
  if (slotids_pos) {
    fd_setpos(stream,FD_BIGPOOL_SLOTIDS_POS);
    fd_write_8bytes(outstream,slotids_pos);
    fd_write_4bytes(outstream,slotids_size);}

  fd_flush_stream(stream);
  fd_unlock_stream(stream);
  fd_close_stream(stream,FD_STREAM_FREEDATA);
  return 0;
}

/* Methods */

static int bigpool_load(fd_pool p)
{
  fd_bigpool bp = (fd_bigpool)p;
  if (FD_POOLFILE_LOCKEDP(bp))
    /* If we have the file locked, the stored load is good. */
    return bp->pool_load;
  else {
    /* Otherwise, we need to read the load from the file */
    int load;
    fd_lock_pool(p);
    load = read_bigpool_load(bp);
    fd_unlock_pool(p);
    return load;}
}

static lispval read_oid_value(fd_bigpool bp,
                              fd_inbuf in,
                              const u8_context cxt)
{
  int byte0 = fd_probe_byte(in);
  if (byte0==0xFF) {
    /* Compressed data */
    fd_compress_type zmethod=
      (fd_compress_type)(fd_read_byte(in), fd_read_zint(in));
    size_t data_len = fd_read_zint(in);
    if (fd_request_bytes(in,data_len)<0) {
      return FD_EOD;}
    switch (zmethod) {
    case FD_NOCOMPRESS:
      return read_oid_value(bp,in,cxt);
#if HAVE_SNAPPYC_H
    case FD_SNAPPY: {
      unsigned char _ubuf[FD_INIT_ZBUF_SIZE*3];
      ssize_t ubuf_size = -1;
      snappy_status size_rv=
        snappy_uncompressed_length(in->bufread,data_len,&ubuf_size);
      struct FD_INBUF inflated;
      unsigned char *ubuf = (size_rv == SNAPPY_OK)?
        ((ubuf_size>FD_INIT_ZBUF_SIZE*3) ?
         (u8_malloc(ubuf_size)) : (&(_ubuf)) ) :
        (NULL);
      snappy_status inflate_rv=
        snappy_uncompress(in->bufread,data_len,ubuf,&ubuf_size);
      if (inflate_rv == SNAPPY_OK) {
        FD_INIT_BYTE_INPUT(&inflated,ubuf,ubuf_size);
        if (ubuf==_ubuf)
          return read_oid_value(bp,&inflated,cxt);
        else {
          lispval result = read_oid_value(bp,&inflated,cxt);
          u8_free(ubuf);
          return result;}}
      else return fd_err("SnappyUncompressFailed",cxt,
                         bp->poolid,VOID);}
#endif
    case FD_ZLIB: {
      unsigned char _ubuf[FD_INIT_ZBUF_SIZE*3], *ubuf=_ubuf;
      size_t ubuf_size = FD_INIT_ZBUF_SIZE*3;
      struct FD_INBUF inflated;
      if (data_len>FD_INIT_ZBUF_SIZE)
        ubuf = do_zuncompress(in->bufread,data_len,&ubuf_size,NULL);
      else ubuf = do_zuncompress(in->bufread,data_len,&ubuf_size,_ubuf);
      FD_INIT_BYTE_INPUT(&inflated,ubuf,ubuf_size);
      if (ubuf==_ubuf)
        return read_oid_value(bp,&inflated,cxt);
      else {
        lispval result = read_oid_value(bp,&inflated,cxt);
        u8_free(ubuf);
        return result;}
    default:
      fd_seterr("Invalid compression code",cxt,bp->poolid,FD_INT(zmethod));
      return FD_ERROR;}}}
  else if (byte0==0xF0) {
    /* Encoded slotmap/schemap */
    unsigned int n_slots = (fd_read_byte(in), fd_read_zint(in));
    lispval sm = fd_make_slotmap(n_slots+1,n_slots,NULL);
    struct FD_KEYVAL *kvals = FD_SLOTMAP_KEYVALS(sm);
    int i = 0; while (i<n_slots) {
      lispval key = VOID, val = VOID;
      int slot_byte0 = fd_probe_byte(in);
      if (slot_byte0==0xE0) {
        long long slotcode = (fd_read_byte(in), fd_read_zint(in));
        if ((slotcode>=0)&&(slotcode<bp->bigpool_n_slotids))
          kvals[i].kv_key = bp->bigpool_slotids[slotcode];
        else {
          fd_seterr(_("BadSlotCode"),cxt,bp->poolid,VOID);
          fd_decref((lispval)sm);
          return FD_ERROR;}}
      else kvals[i].kv_key = key = fd_read_dtype(in);
      if (FD_ABORTP(key)) {
        FD_SLOTMAP_NSLOTS(sm) = i;
        fd_decref(sm);
        return key;}
      else kvals[i].kv_val = val = fd_read_dtype(in);
      if (FD_ABORTP(val)) {
        fd_decref(key);
        FD_SLOTMAP_NSLOTS(sm) = i;
        fd_decref(sm);
        return val;}
      else i++;}
    return sm;} /* close (byte0==0xF0) */
  /* Just a regular dtype */
  else return fd_read_dtype(in);
}

static lispval read_oid_value_at(fd_bigpool bp,
                                 fd_inbuf fetchbuf,
                                 FD_CHUNK_REF ref,
                                 u8_context cxt)
{
  if (ref.off==0)
    return VOID;
  else {
    fd_stream stream=&(bp->pool_stream);
    struct FD_INBUF _in={0}, *in=
      (fetchbuf == NULL) ?
      (fd_open_block(stream,&_in,ref.off,ref.size,0)) :
      (fd_open_block(stream,fetchbuf,ref.off,ref.size,0));
    if ( (in) && (fetchbuf) )
      return read_oid_value(bp,in,cxt);
    else if (in) {
      lispval result=read_oid_value(bp,in,cxt);
      fd_close_inbuf(in);
      return result;}
    else return FD_ERROR_VALUE;}
}

static lispval bigpool_fetch(fd_pool p,lispval oid)
{
  fd_bigpool bp = (fd_bigpool)p;
  FD_OID addr = FD_OID_ADDR(oid);
  int offset = FD_OID_DIFFERENCE(addr,bp->pool_base);
  unsigned int *offdata=NULL;
  if (PRED_FALSE(offset>=bp->pool_load)) {
    /* Double check by going to disk */
    if (bp->pool_flags&FD_POOL_ADJUNCT) {}
    else if (offset>=(bigpool_load(p)))
      return FD_UNALLOCATED_OID;}
  if (offdata=get_offdata(bp)) {
    FD_CHUNK_REF ref =
      fd_get_chunk_ref(offdata,bp->bigpool_offtype,offset);
    release_offdata(bp);
    if (ref.off<0) return FD_ERROR;
    else if (ref.off==0)
      return EMPTY;
    else return read_oid_value_at(bp,NULL,ref,"bigpool_fetch");}
  else {
    FD_CHUNK_REF ref=
      fd_fetch_chunk_ref(&(bp->pool_stream),
                         256,bp->bigpool_offtype,
                         offset,0);
    if (ref.off<0) return FD_ERROR;
    else if ((ref.off<=0)||(ref.size<=0))
      return EMPTY;
    else return read_oid_value_at(bp,NULL,ref,"bigpool_fetch");}
}

static int compare_offsets(const void *x1,const void *x2)
{
  const struct BIGPOOL_FETCH_SCHEDULE *s1 = x1, *s2 = x2;
  if (s1->location.off<s2->location.off) return -1;
  else if (s1->location.off>s2->location.off) return 1;
  else return 0;
}

static lispval *bigpool_fetchn(fd_pool p,int n,lispval *oids)
{
  fd_bigpool bp = (fd_bigpool)p;
  FD_OID base = p->pool_base;
  lispval *values = u8_alloc_n(n,lispval);
  unsigned int *offdata=get_offdata(bp);
  if (offdata == NULL) {
    /* Don't bother being clever if you don't even have an offsets
       table.  This could be fixed later for small memory implementations. */
    int i = 0; while (i<n) {
      values[i]=bigpool_fetch(p,oids[i]); i++;}
    return values;}
  else {
    unsigned int unlock_stream = 0;
    struct BIGPOOL_FETCH_SCHEDULE *schedule=
      u8_alloc_n(n,struct BIGPOOL_FETCH_SCHEDULE);
#if (!(HAVE_PREAD))
    fd_lock_stream(&(bp->pool_stream));
    unlock_stream = 1;
#endif
    int i = 0;
    struct FD_INBUF _in={0}, *in=&_in;
    /* Populate a fetch schedule with where to get OID values */
    while (i<n) {
      lispval oid = oids[i]; FD_OID addr = FD_OID_ADDR(oid);
      unsigned int off = FD_OID_DIFFERENCE(addr,base);
      schedule[i].value_at = i;
      schedule[i].location = fd_get_chunk_ref(offdata,bp->bigpool_offtype,off);
      if (schedule[i].location.off<0) {
        fd_seterr(InvalidOffset,"bigpool_fetchn",p->poolid,oid);
        u8_free(schedule); u8_free(values);
        if (unlock_stream) fd_unlock_stream(&(bp->pool_stream));
        release_offdata(bp);
        return NULL;}
      else i++;}
    /* Note that we sort the fetch schedule even if we're mmapped in
       order to try to take advantage of page locality. */
    qsort(schedule,n,sizeof(struct BIGPOOL_FETCH_SCHEDULE),compare_offsets);
    i = 0; while (i<n) {
      lispval value =
        read_oid_value_at(bp,in,schedule[i].location,"bigpool_fetchn");
      if (FD_ABORTP(value)) {
        int j = 0; while (j<i) {
          lispval value = values[schedule[j].value_at];
          fd_decref(value);
          j++;}
        u8_free(schedule); u8_free(values);
        u8_condition condition;
        u8_exception ex = u8_current_exception;
        if (ex==NULL)
          condition="Unknown bigpool error";
        else condition=ex->u8x_cond;
        u8_seterr(condition,"bigpool_fetchn/read",u8dup(bp->poolid));
        if (unlock_stream) fd_unlock_stream(&(bp->pool_stream));
        release_offdata(bp);
        return NULL;}
      else values[schedule[i].value_at]=value;
      i++;}
    if (unlock_stream) fd_unlock_stream(&(bp->pool_stream));
    fd_close_inbuf(in);
    release_offdata(bp);
    u8_free(schedule);
    return values;}
}

static int bigpool_write_value(lispval value,fd_stream stream,
                               fd_bigpool p,struct FD_OUTBUF *tmpout,
                               unsigned char **zbuf,int *zbuf_size)
{
  fd_outbuf outstream = fd_writebuf(stream);
  /* Reset the tmpout stream */
  tmpout->bufwrite = tmpout->buffer;
  if (SCHEMAPP(value)) {
    struct FD_SCHEMAP *sm = (fd_schemap)value;
    lispval *schema = sm->table_schema;
    lispval *values = sm->schema_values;
    int i = 0, size = sm->schema_length;
    fd_write_byte(tmpout,0xF0);
    fd_write_zint(tmpout,size);
    while (i<size) {
      lispval slotid = schema[i], value = values[i];
      int slotcode = get_slotcode(p,slotid);
      if (slotcode<0)
        fd_write_dtype(tmpout,slotid);
      else {
        fd_write_byte(tmpout,0xE0);
        fd_write_zint(tmpout,slotcode);}
      fd_write_dtype(tmpout,value);
      i++;}}
  else if (SLOTMAPP(value)) {
    struct FD_SLOTMAP *sm = (fd_slotmap)value;
    struct FD_KEYVAL *keyvals = sm->sm_keyvals;
    int i = 0, size = sm->n_slots;
    fd_write_byte(tmpout,0xF0);
    fd_write_zint(tmpout,size);
    while (i<size) {
      lispval slotid = keyvals[i].kv_key;
      lispval value = keyvals[i].kv_val;
      int slotcode = get_slotcode(p,slotid);
      if (slotcode<0)
        fd_write_dtype(tmpout,slotid);
      else {
        fd_write_byte(tmpout,0xE0);
        fd_write_zint(tmpout,slotcode);}
      fd_write_dtype(tmpout,value);
      i++;}}
  else fd_write_dtype(tmpout,value);
  if (p->bigpool_compression) {
    unsigned char _zbuf[FD_INIT_ZBUF_SIZE];
    unsigned char *zbuf=_zbuf, *zbufout = NULL;
    size_t raw_length = tmpout->bufwrite-tmpout->buffer;
    size_t compressed_length = FD_INIT_ZBUF_SIZE;
    switch (p->bigpool_compression) {
    case FD_SNAPPY: {
      size_t max_compressed_length = snappy_max_compressed_length(raw_length);
      if (max_compressed_length>FD_INIT_ZBUF_SIZE)
        zbuf = u8_malloc(max_compressed_length);
      snappy_status compress_rv=
        snappy_compress(tmpout->buffer,raw_length,zbuf,&compressed_length);
      if (compress_rv == SNAPPY_OK)
        zbufout = zbuf;
      else {
        if (zbuf!=_zbuf) u8_free(zbuf);
        zbuf = NULL;}}
      break;
    case FD_ZLIB:
      zbufout = do_zcompress
        (tmpout->buffer,raw_length,&compressed_length,zbuf,6);
      break;
    case FD_ZLIB9:
      zbufout = do_zcompress
        (tmpout->buffer,raw_length,&compressed_length,zbuf,9);
      break;
    default:
      u8_log(LOGCRIT,"BadCompressionType",
             "The compression type code, %d, was invalid",
             (int)(p->bigpool_compression));
    }
    if (zbufout) {
      int header = 1;
      fd_write_byte(outstream,0xFF);
      header+=fd_write_zint(outstream,(int)p->bigpool_compression);
      header+=fd_write_zint(outstream,compressed_length);
      fd_write_bytes(outstream,zbufout,compressed_length);
      if (zbuf!=_zbuf) u8_free(zbuf);
      return header+compressed_length;}
    if (zbuf!=_zbuf) u8_free(zbuf);}
  fd_write_bytes(outstream,tmpout->buffer,
                 tmpout->bufwrite-tmpout->buffer);
  return tmpout->bufwrite-tmpout->buffer;
}

static ssize_t write_offdata
(struct FD_BIGPOOL *bp, fd_stream stream, int n,
 struct BIGPOOL_SAVEINFO *saveinfo);

/*
static int write_recovery_info
  (struct FD_BIGPOOL *fp,fd_stream stream,
   int n,struct BIGPOOL_SAVEINFO *saveinfo,
   unsigned int load);
*/

static int bigpool_storen(fd_pool p,int n,lispval *oids,lispval *values)
{
  fd_bigpool bp = (fd_bigpool)p;
  struct FD_STREAM *stream = &(bp->pool_stream);
  struct FD_OUTBUF *outstream = fd_writebuf(stream);
  if ((LOCK_POOLSTREAM(bp,"bigpool_storen"))<0) return -1;
  int new_blocks = 0;
  double started = u8_elapsed_time();
  u8_log(fd_storage_loglevel+1,"BigpoolStore",
         "Storing %d oid values in bigpool %s",n,p->poolid);
  struct BIGPOOL_SAVEINFO *saveinfo=
    u8_alloc_n(n,struct BIGPOOL_SAVEINFO);
  struct FD_OUTBUF tmpout;
  unsigned char *zbuf = u8_malloc(FD_INIT_ZBUF_SIZE);
  unsigned int i = 0, zbuf_size = FD_INIT_ZBUF_SIZE;
  unsigned int init_buflen = 2048*n;
  FD_OID base = bp->pool_base;
  size_t maxpos = get_maxpos(bp);
  fd_off_t endpos;
  if (init_buflen>262144) init_buflen = 262144;
  FD_INIT_BYTE_OUTPUT(&tmpout,init_buflen);
  endpos = fd_endpos(stream);
  if ((bp->bigpool_format)&(FD_BIGPOOL_DTYPEV2))
    tmpout.buf_flags = tmpout.buf_flags|FD_USE_DTYPEV2|FD_IS_WRITING;
  while (i<n) {
    FD_OID addr = FD_OID_ADDR(oids[i]);
    lispval value = values[i];
    int n_bytes = bigpool_write_value(value,stream,bp,&tmpout,&zbuf,&zbuf_size);
    if (n_bytes<0) {
      u8_free(zbuf);
      u8_free(saveinfo);
      u8_free(tmpout.buffer);
      UNLOCK_POOLSTREAM(bp);
      return n_bytes;}
    else new_blocks++;
    if ((endpos+n_bytes)>=maxpos) {
      u8_free(zbuf); u8_free(saveinfo); u8_free(tmpout.buffer);
      u8_seterr(fd_DataFileOverflow,"bigpool_storen",
                u8_strdup(p->poolid));
      UNLOCK_POOLSTREAM(bp);
      return -1;}

    saveinfo[i].chunk.off = endpos; saveinfo[i].chunk.size = n_bytes;
    saveinfo[i].oidoff = FD_OID_DIFFERENCE(addr,base);

    endpos = endpos+n_bytes;
    i++;}
  u8_free(tmpout.buffer);
  u8_free(zbuf);

  fd_lock_pool(p);

  write_bigpool_slotids(bp);
  write_offdata(bp,stream,n,saveinfo);
  write_bigpool_load(bp);

  u8_free(saveinfo);
  fd_start_write(stream,0);
  fd_write_4bytes(outstream,FD_BIGPOOL_MAGIC_NUMBER);
  fd_flush_stream(stream);
  fsync(stream->stream_fileno);
  u8_log(fd_storage_loglevel,"BigpoolStore",
         "Stored %d oid values in bigpool %s in %f seconds",
         n,p->poolid,u8_elapsed_time()-started);
  UNLOCK_POOLSTREAM(bp);
  fd_unlock_pool(p);
  return n;
}

/* Three different ways to write offdata */

static ssize_t mmap_write_offdata
(struct FD_BIGPOOL *bp,fd_stream stream,int n,
 struct BIGPOOL_SAVEINFO *saveinfo,
 unsigned int min_off,
 unsigned int max_off);
static ssize_t cache_write_offdata
(struct FD_BIGPOOL *bp,fd_stream stream, int n,
 struct BIGPOOL_SAVEINFO *saveinfo,
 unsigned int *cur_offdata,
 unsigned int min_off,
 unsigned int max_off);
static ssize_t direct_write_offdata
(struct FD_BIGPOOL *bp,fd_stream stream,int n,
 struct BIGPOOL_SAVEINFO *saveinfo);

static ssize_t write_offdata
(struct FD_BIGPOOL *bp,
 fd_stream stream,
 int n,
 struct BIGPOOL_SAVEINFO *saveinfo)
{
  unsigned int min_off=bp->pool_load,  max_off=0, i=0;
  fd_offset_type offtype = bp->bigpool_offtype;
  unsigned int *offdata=NULL;
  if (!((offtype == FD_B32)||(offtype = FD_B40)||(offtype = FD_B64))) {
    u8_log(LOG_WARN,"Corrupted bigpool struct",
           "Bad offset type code (%d) for %s",(int)offtype,bp->poolid);
    u8_seterr("CorruptedBigpoolStruct","bigpool:write_offdata",
              u8_strdup(bp->poolid));
    u8_free(saveinfo);
    return -1;}
  else if ((offdata=get_offdata(bp)))
    while (i<n) {
      unsigned int oidoff = saveinfo[i++].oidoff;
      if (oidoff>max_off) max_off = oidoff;
      if (oidoff<min_off) min_off = oidoff;}

  if (offdata) {
#if HAVE_MMAP
    ssize_t result=
      mmap_write_offdata(bp,stream,n,saveinfo,min_off,max_off);
    if (result>=0) return result;
#endif
    result=cache_write_offdata(bp,stream,n,saveinfo,offdata,min_off,max_off);
    release_offdata(bp);
    if (result>=0) {
      fd_clear_errors(0);
      return result;}}
  ssize_t result=direct_write_offdata(bp,stream,n,saveinfo);
  release_offdata(bp);
  return result;
}

static ssize_t mmap_write_offdata
(struct FD_BIGPOOL *bp,fd_stream stream,
 int n, struct BIGPOOL_SAVEINFO *saveinfo,
 unsigned int min_off,unsigned int max_off)
{
  int chunk_ref_size = get_chunk_ref_size(bp);
  int retval = -1;
  u8_log(fd_storage_loglevel+1,"bigpool:write_offdata",
         "Finalizing %d oid values for %s",n,bp->poolid);

  unsigned int *offdata = NULL;
  size_t byte_length =
    (bp->pool_flags&FD_POOL_ADJUNCT) ?
    (chunk_ref_size*(bp->pool_capacity)) :
    (chunk_ref_size*(bp->pool_load));
  /* Map a second version of offdata to modify */
  unsigned int *memblock=
    mmap(NULL,256+(byte_length),(PROT_READ|PROT_WRITE),MAP_SHARED,
         stream->stream_fileno,0);
  if ( (memblock==NULL) || (memblock == MAP_FAILED) ) {
    u8_log(LOGCRIT,u8_strerror(errno),
             "Failed MMAP of %lld bytes of offdata for bigpool %s",
             256+(byte_length),bp->poolid);
    U8_CLEAR_ERRNO();
    u8_graberrno("bigpool_write_offdata",u8_strdup(bp->poolid));
    return -1;}
    else offdata = memblock+64;
  switch (bp->bigpool_offtype) {
  case FD_B64: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      offdata[oidoff*3]=fd_net_order((saveinfo[k].chunk.off)>>32);
      offdata[oidoff*3+1]=fd_net_order((saveinfo[k].chunk.off)&(0xFFFFFFFF));
      offdata[oidoff*3+2]=fd_net_order(saveinfo[k].chunk.size);
      k++;}
    break;}
  case FD_B32: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      offdata[oidoff*2]=fd_net_order(saveinfo[k].chunk.off);
      offdata[oidoff*2+1]=fd_net_order(saveinfo[k].chunk.size);
      k++;}
    break;}
  case FD_B40: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff, w1 = 0, w2 = 0;
      fd_convert_FD_B40_ref(saveinfo[k].chunk,&w1,&w2);
      offdata[oidoff*2]=fd_net_order(w1);
      offdata[oidoff*2+1]=fd_net_order(w2);
      k++;}
    break;}
  default:
    u8_log(LOG_WARN,"Bad offset type for %s",bp->poolid);
    u8_free(saveinfo);
    exit(-1);}
  retval = msync(offdata-64,256+byte_length,MS_SYNC|MS_INVALIDATE);
  if (retval<0) {
    u8_log(LOG_WARN,u8_strerror(errno),
           "bigpool:write_offdata:msync %s",bp->poolid);
    u8_graberrno("bigpool_write_offdata:msync",u8_strdup(bp->poolid));}
  retval = munmap(offdata-64,256+byte_length);
  if (retval<0) {
    u8_log(LOG_WARN,u8_strerror(errno),
           "bigpool/bigpool_write_offdata:munmap %s",bp->poolid);
    u8_graberrno("bigpool_write_offdata:munmap",u8_strdup(bp->poolid));
    return -1;}
  return n;
}

static ssize_t cache_write_offdata
(struct FD_BIGPOOL *bp,fd_stream stream,
 int n, struct BIGPOOL_SAVEINFO *saveinfo,
 unsigned int *cur_offdata,
 unsigned int min_off,unsigned int max_off)
{
  int chunk_ref_size = get_chunk_ref_size(bp);
  size_t offdata_modified_length = chunk_ref_size*(max_off-min_off);
  size_t offdata_modified_start = chunk_ref_size*min_off;
  unsigned int *offdata =u8_zmalloc(offdata_modified_length);
  if (offdata == NULL) {
    u8_graberrno("bigpool:write_offdata:malloc",u8_strdup(bp->poolid));
    return -1;}
  memcpy(offdata,cur_offdata+offdata_modified_start,offdata_modified_length);
  switch (bp->bigpool_offtype) {
    case FD_B64: {
      int k = 0; while (k<n) {
        unsigned int oidoff = saveinfo[k].oidoff;
        offdata[oidoff*3]=(saveinfo[k].chunk.off)>>32;
        offdata[oidoff*3+1]=(saveinfo[k].chunk.off)&(0xFFFFFFFF);
        offdata[oidoff*3+2]=saveinfo[k].chunk.size;
        k++;}
      break;}
    case FD_B32: {
      int k = 0; while (k<n) {
        unsigned int oidoff = saveinfo[k].oidoff;
        offdata[oidoff*2]=saveinfo[k].chunk.off;
        offdata[oidoff*2+1]=saveinfo[k].chunk.size;
        k++;}
      break;}
    case FD_B40: {
      int k = 0; while (k<n) {
        unsigned int oidoff = saveinfo[k].oidoff, w1 = 0, w2 = 0;
        fd_convert_FD_B40_ref(saveinfo[k].chunk,&w1,&w2);
        offdata[oidoff*2]=w1;
        offdata[oidoff*2+1]=w2;
        k++;}
      break;}
    default:
      u8_log(LOG_WARN,"Bad offset type for %s",bp->poolid);
      u8_free(saveinfo);
      exit(-1);}
  fd_setpos(stream,256+offdata_modified_start);
  fd_write_ints(stream,offdata_modified_length,offdata);
  u8_free(offdata);
  return n;
}

static ssize_t direct_write_offdata(struct FD_BIGPOOL *bp,fd_stream stream,
                                    int n, struct BIGPOOL_SAVEINFO *saveinfo)
{
  fd_outbuf outstream = fd_writebuf(stream);
  switch (bp->bigpool_offtype) {
  case FD_B32: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      fd_setpos(stream,256+oidoff*8);
      fd_write_4bytes(outstream,saveinfo[k].chunk.off);
      fd_write_4bytes(outstream,saveinfo[k].chunk.size);
      k++;}
    break;}
  case FD_B40: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff, w1 = 0, w2 = 0;
      fd_setpos(stream,256+oidoff*8);
      fd_convert_FD_B40_ref(saveinfo[k].chunk,&w1,&w2);
      fd_write_4bytes(outstream,w1);
      fd_write_4bytes(outstream,w2);
      k++;}
    break;}
  case FD_B64: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      fd_setpos(stream,256+oidoff*12);
      fd_write_8bytes(outstream,saveinfo[k].chunk.off);
      fd_write_4bytes(outstream,saveinfo[k].chunk.size);
      k++;}
    break;}
  default:
    u8_log(LOG_WARN,"Bad offset type for %s",bp->poolid);
    u8_free(saveinfo);
    exit(-1);}
  return n;
}

/* Allocating OIDs */

static lispval bigpool_alloc(fd_pool p,int n)
{
  fd_bigpool bp = (fd_bigpool)p;
  lispval results = EMPTY; int i = 0;
  FD_OID base=bp->pool_base;
  unsigned int start;
  fd_lock_pool(p);
  if (!(POOLFILE_LOCKEDP(bp))) lock_bigpool_file(bp,0);
  if (bp->pool_load+n>=bp->pool_capacity) {
    fd_unlock_pool(p);
    return fd_err(fd_ExhaustedPool,"bigpool_alloc",
                  p->poolid,VOID);}
  start=bp->pool_load; bp->pool_load+=n;
  fd_unlock_pool(p);
  while (i < n) {
    FD_OID new_addr = FD_OID_PLUS(base,start+i);
    lispval new_oid = fd_make_oid(new_addr);
    CHOICE_ADD(results,new_oid);
    i++;}
  return fd_simplify_choice(results);
}

/* Locking and unlocking */

static int bigpool_lock(fd_pool p,lispval oids)
{
  struct FD_BIGPOOL *fp = (struct FD_BIGPOOL *)p;
  int retval = lock_bigpool_file(fp,1);
  return retval;
}

static int bigpool_unlock(fd_pool p,lispval oids)
{
  struct FD_BIGPOOL *fp = (struct FD_BIGPOOL *)p;
  if (fp->pool_changes.table_n_keys == 0)
    /* This unlocks the underlying file, not the stream itself */
    fd_unlockfile(&(fp->pool_stream));
  return 1;
}

/* Setting the cache level */

#if HAVE_MMAP
static void setcache(fd_bigpool bp,int level,int chunk_ref_size)
{
  int stream_flags=bp->pool_stream.stream_flags;

  if ( (level < 2) && (bp->bigpool_offdata) ) {
    /* Unmap the offsets cache */
    int retval;
    u8_write_lock(&bp->bigpool_offdata_lock);
    size_t offsets_size = bp->bigpool_offdata_length;
    size_t header_size = 256+offsets_size;
    /* The address we pass to munmap is 64 (not the 256 we passed to
       mmap originally) because bp->bigpool_offdata is an (unsigned
       int *). */
    retval = munmap((bp->bigpool_offdata)-64,header_size);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "bigpool_setcache:munmap %s",bp->poolid);
      bp->bigpool_offdata = NULL;
      U8_CLEAR_ERRNO();}
    bp->bigpool_offdata = NULL;
    bp->bigpool_offdata_length = 0;
    if (U8_BITP(stream_flags,FD_STREAM_MMAPPED)) {
      fd_setbufsize(&(bp->pool_stream),fd_stream_bufsize);}
    u8_rw_unlock(&bp->bigpool_offdata_lock);
    return;}

  if ( (LOCK_POOLSTREAM(bp,"bigpool_setcache")) < 0) {
    u8_log(LOGWARN,"PoolStreamClosed",
           "During bigpool_setcache for %s",bp->poolid);
    UNLOCK_POOLSTREAM(bp);
    return;}

  /* Setting the bufsize will unmap the stream */
  if ( (level < 3) && (U8_BITP(stream_flags,FD_STREAM_MMAPPED)) )
    fd_setbufsize(&(bp->pool_stream),fd_stream_bufsize);

  if (level < 2 ) return;

  /* Everything below here requires a file descriptor */

#if HAVE_MMAP
  if ( (level >= 3) && (!(U8_BITP(stream_flags,FD_STREAM_MMAPPED)) ) )
    /* Setting the bufsize to -1 will mmap the stream if it's
       readonly */
    fd_setbufsize(&(bp->pool_stream),-1);
#endif

  if ( (level >= 2) && (bp->bigpool_offdata == NULL) ) {
    unsigned int *offsets, *newmmap;
    u8_write_lock(&bp->bigpool_offdata_lock);
    /* Sizes here are in bytes */
    size_t offsets_size = (bp->pool_capacity)*chunk_ref_size;
    size_t header_size = 256+offsets_size;
    /* Map the offsets */
    newmmap=
      mmap(NULL,header_size,PROT_READ,MAP_SHARED|MAP_NORESERVE,
           bp->pool_stream.stream_fileno,
           0);
    if ((newmmap == NULL) || (newmmap == MAP_FAILED)) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "bigpool_setcache:mmap %s",bp->poolid);
      bp->bigpool_offdata = NULL;
      bp->bigpool_offdata_length = 0;
      U8_CLEAR_ERRNO();}
    else {
      bp->bigpool_offdata = offsets = newmmap+64;
      bp->bigpool_offdata_length = offsets_size;} 
    u8_rw_unlock(&(bp->bigpool_offdata_lock));}

  UNLOCK_POOLSTREAM(bp);
}
#else
static void setcache(fd_bigpool bp,int level,int chunk_ref_size)
{
  unsigned int *offdata=NULL;
  if (level < 2) {
    if (bp->bigpool_offdata) {
      u8_write_lock(bp->bigpool_offdata_lock);
      if (bp->bigpool_offdata) {
        u8_free(bp->bigpool_offdata);
        bp->bigpool_offdata = NULL;}
      u8_rw_unlock(bp->bigpool_offdata_lock);
      return;}}
  else {
    unsigned int *offsets;
    fd_stream s = &(bp->pool_stream);
    fd_inbuf ins = fd_readbuf(s);
    if (LOCK_POOLSTREAM(bp)<0) {
      fd_clear_errors(1);}
    else {
      size_t offsets_size = chunk_ref_size*(bp->pool_load);
      fd_stream_start_read(s);
      fd_setpos(s,12);
      bp->pool_load = load = fd_read_4bytes(ins);
      offsets = u8_malloc(offsets_size);
      fd_setpos(s,24);
      fd_read_ints(ins,load,offsets);
      u8_write_lock(&(bp->bigpool_offdata_lock));
      bp->bigpool_offdata = offsets;
      bp->bigpool_offdata_length = offsets_size;
      u8_rw_unlock(&(bp->bigpool_offdata_lock));
      UNLOCK_POOLSTREAM(bp);}
    return;}
}
#endif

static void bigpool_setcache(fd_pool p,int level)
{
  fd_bigpool bp = (fd_bigpool)p;
  int chunk_ref_size = get_chunk_ref_size(bp);
  if (chunk_ref_size<0) {
    u8_log(LOG_WARN,fd_CorruptedPool,"Pool structure invalid: %s",p->poolid);
    return;}
  if ( ( (level<2) && (bp->bigpool_offdata == NULL) ) ||
       ( (level==2) && ( bp->bigpool_offdata != NULL ) ) )
    return;
  fd_lock_pool(p);
  /* Check again, race condition */
  if ( ( (level<2) && (bp->bigpool_offdata == NULL) ) ||
       ( (level==2) && ( bp->bigpool_offdata != NULL ) ) ) {
    fd_unlock_pool(p);
    return;}
  setcache(bp,level,chunk_ref_size);
  fd_unlock_pool(p);
}

/* Write values:
  * 0: just for reading, open up to the *load* of the pool
  * 1: for writing, open up to the capcity of the pool
  * -1: for reading, but sync before remapping
*/
#if HAVE_MMAP
static void reload_offdata(fd_bigpool bp) {}
#else
static void reload_offdata(fd_bigpool bp)
{
  double start = u8_elapsed_time();
  fd_stream s = &(bp->pool_stream);
  fd_inbuf ins = fd_readbuf(s);
  /* Read new offsets table, compare it with the current, and
     only void those OIDs */
  unsigned int new_load, *offsets, *nscan, *oscan, *olim;
  struct FD_STREAM *s = &(bp->pool_stream);
  if ( (LOCK_POOLSTREAM(bp,"bigpool/reload_offdata")) < 0) {
    u8_log(LOGWARN,"PoolStreamClosed",
           "During bigpool_reload_offdata for %s",bp->poolid);
    UNLOCK_POOLSTREAM(bp);
    return;}
  oscan = bp->bigpool_offdata;
  olim = oscan+(bp->bigpool_offdata_length/4);
  fd_setpos(s,0x10); new_load = fd_read_4bytes(ins);
  nscan = offsets = u8_alloc_n(new_load,unsigned int);
  fd_setpos(s,0x100);
  fd_read_ints(ins,new_load,offsets);
  /* Clear cached values whose offsets have changed */
  while (oscan < olim)
    if (*oscan == *nscan) {oscan++; nscan++;}
    else {
      FD_OID addr = FD_OID_PLUS(bp->pool_base,(nscan-offsets));
      lispval changed_oid = fd_make_oid(addr);
      fd_hashtable_bp(&(bp->pool_cache),fd_table_replace,changed_oid,VOID);
      oscan++; nscan++;}
  u8_write_lock(&(bp->bigpool_offdata_lock));
  u8_free(bp->bigpool_offdata);
  bp->bigpool_offdata = offsets;
  bp->pool_load = new_load;
  bp->bigpool_offdata_length = new_load*get_chunk_ref_size(bp);
  u8_rw_unlock(&(bp->bigpool_offdata_lock));
  update_modtime(bp);
  UNLOCK_POOLSTREAM(bp)
  u8_log(fd_storage_loglevel+1,"ReloadOffsets",
         "Offsets for %s reloaded in %f secs",
         bp->poolid,u8_elapsed_time()-start);
}
#endif

static void bigpool_close(fd_pool p)
{
  fd_bigpool bp = (fd_bigpool)p;
  fd_lock_pool(p);
  if (bp->bigpool_offdata) {
    u8_write_lock(&(bp->bigpool_offdata_lock));
#if HAVE_MMAP
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval = munmap((bp->bigpool_offdata)-64,bp->bigpool_offdata_length+256);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "bigpool_close:munmap offsets %s",bp->poolid);
      errno = 0;}
#else
    u8_free(bp->bigpool_offdata);
#endif
    bp->bigpool_offdata = NULL;
    bp->bigpool_offdata_length = 0;
    u8_rw_unlock(&(bp->bigpool_offdata_lock));
    bp->pool_cache_level = -1;}
  if (POOLFILE_LOCKEDP(bp))
    write_bigpool_load(bp);
  fd_close_stream(&(bp->pool_stream),0);
  fd_unlock_pool(p);
}

static void bigpool_setbuf(fd_pool p,ssize_t bufsize)
{
  fd_bigpool bp = (fd_bigpool)p;
  if ( (bp->pool_stream.stream_flags) & (FD_STREAM_MMAPPED) )
    return;
  fd_lock_pool(p);
  fd_setbufsize(&(bp->pool_stream),(size_t)bufsize);
  fd_unlock_pool(p);
}

/* Bigpool ops */

static lispval bigpool_ctl(fd_pool p,lispval op,int n,lispval *args)
{
  struct FD_BIGPOOL *fp = (struct FD_BIGPOOL *)p;
  if ((n>0)&&(args == NULL))
    return fd_err("BadPoolOpCall","bigpool_op",fp->poolid,VOID);
  else if (n<0)
    return fd_err("BadPoolOpCall","bigpool_op",fp->poolid,VOID);
  else if (op == fd_cachelevel_op) {
    if (n==0)
      return FD_INT(fp->pool_cache_level);
    else {
      lispval arg = (args)?(args[0]):(VOID);
      if ((FIXNUMP(arg))&&(FIX2INT(arg)>=0)&&
          (FIX2INT(arg)<0x100)) {
        bigpool_setcache(p,FIX2INT(arg));
        return FD_INT(fp->pool_cache_level);}
      else return fd_type_error
             (_("cachelevel"),"bigpool_op/cachelevel",arg);}}
  else if (op == fd_reload_op) {
    reload_offdata(fp);
    return FD_TRUE;}
  else if (op == fd_bufsize_op) {
    if (n==0)
      return FD_INT(fp->pool_stream.buf.raw.buflen);
    else if (FIXNUMP(args[0])) {
      bigpool_setbuf(p,FIX2INT(args[0]));
      return FD_INT(fp->pool_stream.buf.raw.buflen);}
    else return fd_type_error("buffer size","bigpool_op/bufsize",args[0]);}
  else if (op == fd_slotids_op) {
    int n_slotids=fp->bigpool_n_slotids;
    lispval result=fd_make_vector(n_slotids,NULL);
    lispval *slotids=fp->bigpool_slotids;
    int i=0; while (i<n) {
      lispval slotid=slotids[i];
      FD_VECTOR_SET(result,i,slotid);
      fd_incref(slotid);
      i++;}
    return result;}
  else if (op == fd_capacity_op)
    return FD_INT(fp->pool_capacity);
  else if (op == fd_load_op)
    return FD_INT(fp->pool_load);
  else return FD_FALSE;
}

/* Creating bigpool */

static unsigned int get_bigpool_format(fd_storage_flags sflags,lispval opts)
{
  unsigned int flags = 0;
  lispval offtype = fd_intern("OFFTYPE");
  if ( fd_testopt(opts,offtype,fd_intern("B64"))  ||
       fd_testopt(opts,offtype,FD_INT(64)))
    flags |= FD_B64;
  else if ( fd_testopt(opts,offtype,fd_intern("B40"))  ||
            fd_testopt(opts,offtype,FD_INT(40)))
    flags |= FD_B40;
  else if ( fd_testopt(opts,offtype,fd_intern("B32"))  ||
            fd_testopt(opts,offtype,FD_INT(32)))
    flags |= FD_B32;
  else flags |= FD_B40;

  flags |= ((fd_compression_type(opts,FD_NOCOMPRESS))<<3);

  if (fd_testopt(opts,fd_intern("DTYPEV2"),VOID))
    flags |= FD_BIGPOOL_DTYPEV2;

  if ( (sflags) & (FD_STORAGE_READ_ONLY) ||
       (fd_testopt(opts,FDSYM_READONLY,VOID)) )
    flags |= FD_BIGPOOL_READ_ONLY;

  if ( (sflags) & (FD_POOL_ADJUNCT) ||
       (fd_testopt(opts,FDSYM_ISADJUNCT,VOID)) )
    flags |= FD_BIGPOOL_ADJUNCT;

  if ( (sflags) & (FD_POOL_ADJUNCT) ||
       (fd_testopt(opts,fd_intern("SPARSE"),VOID)) )
    flags |= FD_BIGPOOL_SPARSE;

  return flags;
}

static fd_pool bigpool_create(u8_string spec,void *type_data,
                              fd_storage_flags storage_flags,
                              lispval opts)
{
  lispval base_oid = fd_getopt(opts,fd_intern("BASE"),VOID);
  lispval capacity_arg = fd_getopt(opts,fd_intern("CAPACITY"),VOID);
  lispval load_arg = fd_getopt(opts,fd_intern("LOAD"),FD_FIXZERO);
  lispval label = fd_getopt(opts,FDSYM_LABEL,VOID);
  lispval slotids = fd_getopt(opts,fd_intern("SLOTIDS"),VOID);
  unsigned int flags = get_bigpool_format(storage_flags,opts);
  unsigned int capacity, load;
  int rv = 0;
  if (u8_file_existsp(spec)) {
    fd_seterr(_("FileAlreadyExists"),"bigpool_create",spec,VOID);
    return NULL;}
  else if (!(OIDP(base_oid))) {
    fd_seterr("Not a base oid","bigpool_create",spec,base_oid);
    rv = -1;}
  else if (FD_ISINT(capacity_arg)) {
    int capval = fd_getint(capacity_arg);
    if (capval<=0) {
      fd_seterr("Not a valid capacity","bigpool_create",
                spec,capacity_arg);
      rv = -1;}
    else capacity = capval;}
  else {
    fd_seterr("Not a valid capacity","bigpool_create",
              spec,capacity_arg);
      rv = -1;}
  if (rv<0) {}
  else if (FD_ISINT(load_arg)) {
    int loadval = fd_getint(load_arg);
    if (loadval<0) {
      fd_seterr("Not a valid load","bigpool_create",
                spec,load_arg);
      rv = -1;}
    else load = loadval;}
  else {
    fd_seterr("Not a valid load","bigpool_create",
              spec,load_arg);
    rv = -1;}

  if (storage_flags&FD_POOL_ADJUNCT) load=capacity;

  if (rv<0) return NULL;
  else rv = make_bigpool(spec,
                         ((STRINGP(label)) ? (CSTRING(label)) : (spec)),
                         FD_OID_ADDR(base_oid),capacity,load,flags,slotids,
                         time(NULL),
                         time(NULL),1);
  if (rv>=0)
    return fd_open_pool(spec,storage_flags,opts);
  else return NULL;
}

/* Initializing the driver module */

static struct FD_POOL_HANDLER bigpool_handler={
  "bigpool", 1, sizeof(struct FD_BIGPOOL), 12,
  bigpool_close, /* close */
  bigpool_alloc, /* alloc */
  bigpool_fetch, /* fetch */
  bigpool_fetchn, /* fetchn */
  bigpool_load, /* getload */
  bigpool_lock, /* lock */
  bigpool_unlock, /* release */
  bigpool_storen, /* storen */
  NULL, /* swapout */
  NULL, /* metadata */
  bigpool_create, /* create */
  NULL,  /* walk */
  NULL,  /* recycle */
  bigpool_ctl  /* poolctl */
};


FD_EXPORT void fd_init_bigpool_c()
{
  u8_register_source_file(_FILEINFO);

  fd_register_pool_type
    ("bigpool",
     &bigpool_handler,
     open_bigpool,
     fd_match_pool_file,
     (void*)U8_INT2PTR(FD_BIGPOOL_MAGIC_NUMBER));

  fd_set_default_pool_type("bigpool");

}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
