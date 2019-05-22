/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

extern int kno_storage_loglevel;
static int bigpool_loglevel = -1;
#define U8_LOGLEVEL (kno_int_default(bigpool_loglevel,(kno_storage_loglevel-1)))

#define KNO_INLINE_POOLS 1
#define KNO_INLINE_BUFIO 1

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/numbers.h"
#include "kno/storage.h"
#include "kno/streams.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

#include "headers/bigpool.h"

#include <libu8/libu8.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8netfns.h>

#include <zlib.h>

#if (KNO_USE_MMAP)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#define MMAP_FLAGS MAP_SHARED
#endif

#ifndef FETCHBUF_SIZE
#define FETCHBUF_SIZE 16000
#endif

static lispval load_symbol, slotids_symbol, compression_symbol, offmode_symbol;

static void bigpool_setcache(kno_bigpool p,int level);
static int update_offdata_cache(kno_bigpool bp,int level,int chunk_ref_size);

static void use_bigpool(kno_bigpool bp)
{
  kno_lock_pool_struct((kno_pool)bp,0);
  if ( (bp->pool_cache_level < 0) ||
       (bp->pool_stream.stream_fileno < 0) ) {
    kno_unlock_pool_struct((kno_pool)bp);
    kno_lock_pool_struct((kno_pool)bp,1);
    if (! ( (bp->pool_cache_level < 0) ||
            (bp->pool_stream.stream_fileno < 0) ) ) {
      kno_unlock_pool_struct((kno_pool)bp);
      kno_lock_pool_struct((kno_pool)bp,0);}
    else {
      int level = bp->pool_cache_level;
      if (bp->pool_stream.stream_fileno < 0)
        kno_reopen_file_stream
          (&(bp->pool_stream),KNO_FILE_READ,
           kno_getfixopt(bp->pool_opts,"BUFSIZE",kno_driver_bufsize));
      if (level<0) {
        level=kno_default_cache_level;
        bigpool_setcache(bp,level);}
      kno_unlock_pool_struct((kno_pool)bp);
      kno_lock_pool_struct((kno_pool)bp,0);}}
}
static void bigpool_finished(kno_bigpool bp)
{
  kno_unlock_pool_struct((kno_pool)bp);
}

#define POOLFILE_LOCKEDP(op)                                            \
  (U8_BITP((op->pool_stream.stream_flags),KNO_STREAM_FILE_LOCKED))

KNO_FASTOP int LOCK_POOLSTREAM(kno_bigpool bp,u8_string caller)
{
  kno_lock_stream(&(bp->pool_stream));
  if (bp->pool_stream.stream_fileno < 0) {
    u8_seterr("PoolStreamClosed",caller,u8_strdup(bp->poolid));
    kno_unlock_stream(&(bp->pool_stream));
    return -1;}
  else return 1;
}

#define UNLOCK_POOLSTREAM(op) kno_unlock_stream(&(op->pool_stream))

static void reload_bigpool(struct KNO_BIGPOOL *p,int is_locked);
static void update_filetime(struct KNO_BIGPOOL *fp);

static struct KNO_POOL_HANDLER bigpool_handler;

static u8_condition CorruptBigpool=_("Corrupt bigpool");
static u8_condition InvalidOffset=_("Invalid offset in BIGPOOL");

/* BIGPOOLs are the next generation of object pool data file.  While
   previous formats have all stored OIDs for years and years, BIGPOOLs
   are supposed to be the best to date and somewhat paradigmatic.
   The design focuses on performance and extensibility.  With KNO_HASHINDEX,
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

   0x68 XXXX     location of compression dictionary (8 bytes)
   0x70 XXXX      size of compression data(64 bits)

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
   4= ZSTD compression
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

static ssize_t get_chunk_ref_size(kno_bigpool p)
{
  switch (p->pool_offtype) {
  case KNO_B32: case KNO_B40:
    return 8;
  case KNO_B64: return 12;}
  return -1;
}

static kno_size_t get_maxpos(kno_bigpool p)
{
  switch (p->pool_offtype) {
  case KNO_B32:
    return ((kno_size_t)(((kno_size_t)1)<<32));
  case KNO_B40:
    return ((kno_size_t)(((kno_size_t)1)<<40));
  case KNO_B64:
    return ((kno_size_t)(((kno_size_t)1)<<63));
  default:
    return -1;}
}

/* Making and opening bigpools */

static kno_pool open_bigpool(u8_string fname,kno_storage_flags open_flags,
                            lispval opts)
{
  KNO_OID base = KNO_NULL_OID_INIT;
  unsigned int hi, lo, magicno, capacity, load, n_slotids, bigpool_format = 0;
  kno_off_t label_loc, metadata_loc, slotids_loc, slotids_size;
  lispval label;
  struct KNO_BIGPOOL *pool = u8_alloc(struct KNO_BIGPOOL);
  int read_only = U8_BITP(open_flags,KNO_STORAGE_READ_ONLY);
  if ( (read_only == 0) && (u8_file_writablep(fname)) ) {
    if (kno_check_rollback("open_hashindex",fname)<0) {
      /* If we can't apply the rollback, open the file read-only */
      u8_log(LOG_WARN,"RollbackFailed",
             "Opening bigpool %s as read-only due to failed rollback",
             fname);
      kno_clear_errors(1);
      read_only=1;}}
  else read_only=1;
  u8_string realpath = u8_realpath(fname,NULL);
  u8_string abspath = u8_abspath(fname,NULL);
  int stream_flags =
    KNO_STREAM_CAN_SEEK | KNO_STREAM_NEEDS_LOCK | KNO_STREAM_READ_ONLY;
  struct KNO_STREAM *stream=
    kno_init_file_stream(&(pool->pool_stream),fname,KNO_FILE_READ,stream_flags,-1);
  struct KNO_INBUF *instream = (stream) ? (kno_readbuf(stream)) : (NULL);
  if (instream == NULL) {
    u8_raise(kno_FileNotFound,"open_bigpool",u8_strdup(fname));
    u8_free(realpath);
    u8_free(abspath);
    return NULL;}

  stream->stream_flags &= ~KNO_STREAM_IS_CONSED;
  magicno = kno_read_4bytes(instream);
  if (magicno!=KNO_BIGPOOL_MAGIC_NUMBER) {
    kno_seterr(_("Not a bigpool"),"open_bigpool",fname,VOID);
    u8_free(realpath);
    u8_free(abspath);
    return NULL;}
  /* Read POOL base etc. */
  hi = kno_read_4bytes(instream); lo = kno_read_4bytes(instream);
  KNO_SET_OID_HI(base,hi); KNO_SET_OID_LO(base,lo);
  pool->pool_capacity = capacity = kno_read_4bytes(instream);
  pool->pool_load = load = kno_read_4bytes(instream);
  bigpool_format = kno_read_4bytes(instream);
  pool->bigpool_format = bigpool_format;

  if (load > capacity) {
    u8_logf(LOG_CRIT,kno_PoolOverflow,
            "The bigpool %s specifies a load (%lld) > its capacity (%lld)",
            fname,load,capacity);
    pool->pool_load=load=capacity;}

  if ((U8_BITP(bigpool_format,KNO_BIGPOOL_READ_ONLY))&&
      (!(kno_testopt(opts,FDSYM_READONLY,KNO_FALSE)))) {
    /* If the pool is intrinsically read-only make it so. */
    open_flags |= KNO_STORAGE_READ_ONLY;}

  pool->pool_offtype =
    (kno_offset_type)((bigpool_format)&(KNO_BIGPOOL_OFFMODE));
  kno_compress_type cmptype = (((bigpool_format)&(KNO_BIGPOOL_COMPRESSION))>>3);
  pool->pool_compression = kno_compression_type(opts,cmptype);

  if (U8_BITP(bigpool_format,KNO_BIGPOOL_ADJUNCT))
    open_flags |= KNO_POOL_ADJUNCT;
  if (U8_BITP(bigpool_format,KNO_BIGPOOL_SPARSE))
    open_flags |= KNO_POOL_SPARSE;

  kno_setpos(stream,KNO_BIGPOOL_LABEL_POS);
  /* Get the label location */
  label_loc = kno_read_8bytes(instream);
  /* label_size = */ kno_read_4bytes(instream);
  /* Get the metadata loc and size */
  metadata_loc = kno_read_8bytes(instream);
  /* metadata_size = */ kno_read_4bytes(instream);
  /* Creation time */
  pool->pool_ctime=(time_t)kno_read_8bytes(instream);
  /* Repack time */
  pool->pool_rptime=(time_t)kno_read_8bytes(instream);
  /* Mod time */
  pool->pool_mtime=(time_t)kno_read_8bytes(instream);
  /* Repack generation */
  pool->pool_repack_count=kno_read_8bytes(instream);
  /* Now at 0x50 */
  n_slotids = kno_read_4bytes(instream);
  /* Read and initialize the slotids_loc */
  slotids_loc = kno_read_8bytes(instream);
  slotids_size = kno_read_4bytes(instream);

  /* Read the number of blocks stored in the pool */
  pool->pool_nblocks = kno_read_8bytes(instream);

  int metadata_modified = 0;
  lispval metadata=KNO_VOID;
  if (metadata_loc) {
    if (kno_setpos(stream,metadata_loc)>0) {
      kno_inbuf in = kno_readbuf(stream);
      metadata = kno_read_dtype(in);}
    else {
      metadata=KNO_ERROR_VALUE;}

    if ( (KNO_FALSEP(metadata)) || (KNO_VOIDP(metadata)) )
      metadata = KNO_VOID;
    else if (KNO_SLOTMAPP(metadata)) {}
    else if ( open_flags & KNO_STORAGE_REPAIR ) {
      u8_logf(LOG_WARN,"BadMetaData",
              "Ignoring bad metadata stored for %s",fname);
      metadata = kno_empty_slotmap();
      metadata_modified = 1;}
    else {
      if (KNO_ABORTP(metadata))
        kno_seterr("BadPoolMetadata","open_bigpool",fname,KNO_INT(metadata_loc));
      else kno_seterr("BadPoolMetadata","open_bigpool",fname,metadata);
      kno_close_stream(stream,0);
      u8_free(realpath);
      u8_free(abspath);
      u8_free(pool);
      return NULL;}}

  kno_init_pool((kno_pool)pool,base,capacity,
               &bigpool_handler,
               fname,abspath,realpath,
               open_flags,metadata,opts);
  u8_free(realpath);
  u8_free(abspath);

  open_flags = pool->pool_flags;
  kno_decref(metadata);
  if (metadata_modified)
    pool->pool_metadata.table_modified = 1;

  if (label_loc) {
    if (kno_setpos(stream,label_loc)>0) {
      label = kno_read_dtype(instream);
      if (STRINGP(label))
        pool->pool_label = u8_strdup(CSTRING(label));
      else if (SYMBOLP(label))
        pool->pool_label = u8_strdup(KNO_SYMBOL_NAME(label));
      else if (open_flags & KNO_STORAGE_REPAIR) {
        u8_logf(LOG_WARN,kno_BadFilePoolLabel,
                "Invalid pool label for %s: %q",fname,label);
        label = KNO_VOID;}
      else {
        kno_seterr(kno_BadFilePoolLabel,"open_bigpool","bad label value",label);
        kno_decref(label);
        kno_close_stream(stream,0);
        u8_free(pool);
        return NULL;}
      kno_decref(label);}
    else {
      kno_seterr(kno_BadFilePoolLabel,"open_bigpool","bad label loc",
                KNO_INT(label_loc));
      kno_close_stream(stream,0);
      u8_free(pool);
      return NULL;}}
 
  if (slotids_loc) {
    int slotids_length = (n_slotids>256)?((1+(n_slotids/2))*2):(256);
    lispval *slotids = u8_alloc_n(slotids_length,lispval);
    struct KNO_INBUF _in = {0}, *in =
      kno_open_block(stream,&_in,slotids_loc,slotids_size,1);
    int i = 0;
    kno_setpos(stream,slotids_loc);
    while (_in.bufread < _in.buflim ) {
      lispval slotid = kno_read_dtype(in);
      if (!( (KNO_SYMBOLP(slotid)) || (KNO_OIDP(slotid)) ||
             (KNO_EMPTYP(slotid)) || (KNO_VOIDP(slotid)) ||
             (KNO_FALSEP(slotid)) )) {
        kno_seterr(CorruptBigpool,"open_bigpool/slotid_init",fname,slotid);
        kno_close_stream(stream,0);
        u8_free(pool);
        return NULL;}
      slotids[i++]=slotid;}
    kno_close_inbuf(in);
    kno_init_slotcoder(& pool->pool_slotcodes, i, slotids);
    u8_free(slotids);}
  else kno_init_slotcoder(& pool->pool_slotcodes, -1, NULL);
  pool->pool_offdata = NULL;
  pool->pool_offlen = 0;
  if (read_only)
    U8_SETBITS(pool->pool_flags,KNO_STORAGE_READ_ONLY);
  else U8_CLEARBITS(pool->pool_flags,KNO_STORAGE_READ_ONLY);
  kno_register_pool((kno_pool)pool);
  update_filetime(pool);
  return (kno_pool)pool;
}

static void update_filetime(struct KNO_BIGPOOL *fp)
{
  struct stat fileinfo;
  if ((fstat(fp->pool_stream.stream_fileno,&fileinfo))<0)
    fp->file_mtime = (time_t)-1;
  else fp->file_mtime = fileinfo.st_mtime;
}

static kno_pool recover_bigpool(u8_string fname,kno_storage_flags open_flags,
                               lispval opts)
{
  u8_string rollback_file=u8_string_append(fname,".rollback",NULL);
  if (u8_file_existsp(rollback_file)) {
    u8_logf(LOG_WARN,"Rollback",
            "Applying rollback file %s to %s",rollback_file,fname);
    ssize_t rv=kno_restore_head(rollback_file,fname);
    if (rv<0) {
      u8_graberrno("recover_bigpool",rollback_file);
      return NULL;}
    kno_pool opened = open_bigpool(fname,open_flags,opts);
    if (opened) {
      u8_string rollback_applied = u8_string_append(rollback_file,".applied",NULL);
      u8_movefile(rollback_file,rollback_applied);
      u8_free(rollback_applied);
      u8_free(rollback_file);
      return opened;}
    else if (! (kno_testopt(opts,kno_intern("fixup"),KNO_VOID))) {
      kno_seterr("RecoveryFailed","recover_bigpool",fname,KNO_VOID);
      u8_free(rollback_file);
      return NULL;}
    else {
      u8_string rollback_failed = u8_string_append(rollback_file,".failed",NULL);
      u8_byte details[256];
      u8_logf(LOG_ERR,"RecoveryFailed",
              "Failed to recover %s using %s",fname,rollback_file);
      if (u8_movefile(rollback_file,rollback_failed) < 0) {
        kno_seterr("RecoveryFailed","recover_bigpool",
                  u8_sprintf(details,256,"Couldn't (re)move rollback file %s",
                             rollback_file),
                  KNO_VOID);
        u8_free(rollback_file);
        u8_free(rollback_failed);
        return NULL;}
      else {
        u8_free(rollback_file);
        u8_free(rollback_failed);}}}
  else if (! (kno_testopt(opts,kno_intern("fixup"),KNO_VOID)) ) {
    u8_seterr("NoRollbackFile",
              "The bigpool file %s doesn't have a rollback file %s",
              rollback_file);
    return NULL;}
  else {
    u8_logf(LOG_CRIT,CorruptBigpool,
            "The bigpool file %s doesn't have a rollback file %s",
            fname,rollback_file);
    u8_free(rollback_file);
    rollback_file=NULL;}
  /* Try to 'force' recovery by updating the header */
  char *src = u8_tolibc(fname);
  KNO_DECL_OUTBUF(headbuf,256);
  unsigned int magicno = KNO_BIGPOOL_MAGIC_NUMBER;
  int out=open(src,O_RDWR);
  kno_write_4bytes(&headbuf,magicno);
#if HAVE_PREAD
  int rv = pwrite(out,headbuf.buffer,4,0);
#else
  lseek(out,SEEK_SET,0);
  int rv = write(out,headbuf.buffer,4);
#endif
  fsync(out);
  close(out);
  u8_free(src);
  if (rv>0) {
    kno_pool opened = open_bigpool(fname,open_flags,opts);
    if (opened)
      return opened;
    u8_seterr("FailedFixup","recover_bigpool",u8_strdup(fname));
    return NULL;}
  else return NULL;
}

/* Getting slotids */

/* Lock the underlying Bigpool */

static int lock_bigpool_file(struct KNO_BIGPOOL *bp,int struct_locked)
{
  if (KNO_POOLSTREAM_LOCKEDP(bp))
    return 1;
  else if ((bp->pool_flags)&(KNO_STORAGE_READ_ONLY))
    return 0;
  else {
    struct KNO_STREAM *s = &(bp->pool_stream);
    struct stat fileinfo;
    int reload=0;
    if (!(struct_locked)) kno_lock_pool_struct((kno_pool)bp,1);
    if (KNO_POOLSTREAM_LOCKEDP(bp)) {
      /* Another thread got here first */
      if (!(struct_locked)) kno_unlock_pool_struct((kno_pool)bp);
      return 1;}
    LOCK_POOLSTREAM(bp,"lock_bigpool_file");
    if (kno_streamctl(s,kno_stream_lockfile,NULL)==0) {
      kno_unlock_stream(s);
      if (!(struct_locked)) kno_unlock_pool_struct((kno_pool)bp);
      UNLOCK_POOLSTREAM(bp);
      return 0;}
    fstat( s->stream_fileno, &fileinfo);
    if ( fileinfo.st_mtime > bp->pool_mtime ) reload=1;
    UNLOCK_POOLSTREAM(bp);
    if (reload) reload_bigpool(bp,KNO_ISLOCKED);
    if (!(struct_locked)) kno_unlock_pool_struct((kno_pool)bp);
    return 1;}
}

/* BIGPOOL operations */

static int make_bigpool
(u8_string fname,u8_string label,
 KNO_OID base,unsigned int capacity,unsigned int load,
 unsigned int flags,
 lispval metadata_init,lispval slotcodes_init,
 time_t ctime,time_t mtime,
 int n_repacks)
{
  time_t now = time(NULL);
  kno_off_t slotcodes_pos = 0, metadata_pos = 0, label_pos = 0;
  size_t slotcodes_size = 0, metadata_size = 0, label_size = 0;
  if (load>capacity) {
    u8_seterr(kno_PoolOverflow,"make_bigpool",
              u8_sprintf(NULL,256,
                         "Specified load (%u) > capacity (%u) for '%s'",
                         load,capacity,fname));
    return -1;}
  if (ctime<0) ctime = now;
  if (mtime<0) mtime = now;

  struct KNO_STREAM _stream, *stream=
    kno_init_file_stream(&_stream,fname,
                        KNO_FILE_CREATE,-1,
                        kno_driver_bufsize);
  kno_outbuf outstream = (stream) ? (kno_writebuf(stream)) : (NULL);
  kno_offset_type offtype = (kno_offset_type)((flags)&(KNO_BIGPOOL_OFFMODE));

  if (outstream == NULL) return -1;
  else if ((stream->stream_flags)&KNO_STREAM_READ_ONLY) {
    kno_seterr3(kno_CantWrite,"kno_make_bigpool",fname);
    kno_free_stream(stream);
    return -1;}

  outstream->buf_flags |= KNO_WRITE_OPAQUE;

  /* Remove leftover files */
  kno_remove_suffix(fname,".commit");
  kno_remove_suffix(fname,".rollback");

  u8_logf(LOG_INFO,"CreateBigPool",
          "Creating a bigpool '%s' for %u OIDs based at %x/%x",
          fname,capacity,KNO_OID_HI(base),KNO_OID_LO(base));

  stream->stream_flags &= ~KNO_STREAM_IS_CONSED;
  kno_lock_stream(stream);
  kno_setpos(stream,0);
  kno_write_4bytes(outstream,KNO_BIGPOOL_MAGIC_NUMBER);
  kno_write_4bytes(outstream,KNO_OID_HI(base));
  kno_write_4bytes(outstream,KNO_OID_LO(base));
  kno_write_4bytes(outstream,capacity);
  kno_write_4bytes(outstream,load);
  kno_write_4bytes(outstream,flags);

  kno_write_8bytes(outstream,0); /* Pool label */
  kno_write_4bytes(outstream,0); /* Pool label */

  kno_write_8bytes(outstream,0); /* metadata */
  kno_write_4bytes(outstream,0); /* metadata */

  /* Write the index creation time */
  kno_write_4bytes(outstream,0);
  kno_write_4bytes(outstream,((unsigned int)ctime));

  /* Write the index repack time */
  kno_write_4bytes(outstream,0);
  kno_write_4bytes(outstream,((unsigned int)now));

  /* Write the index modification time */
  kno_write_4bytes(outstream,0);
  kno_write_4bytes(outstream,((unsigned int)mtime));

  /* Write the number of repack n_repacks */
  if (mtime<0) mtime = now;
  kno_write_4bytes(outstream,0);
  kno_write_4bytes(outstream,n_repacks);

  /* Write number of slotcodes (if any) */
  if (VECTORP(slotcodes_init)) {
    int real_len = 0; int i =0, lim = VEC_LEN(slotcodes_init);
    while (i<lim) {
      lispval slotid = VEC_REF(slotcodes_init,i);
      if ( (KNO_OIDP(slotid)) || (KNO_SYMBOLP(slotid)) )
        real_len++;
      else break;}
    kno_write_4bytes(outstream,real_len);}
  else kno_write_4bytes(outstream,0);
  kno_write_8bytes(outstream,0); /* slotcodes offset (may be changed) */
  kno_write_4bytes(outstream,0); /* slotcodes dtype length (may be changed) */

  /* Fill the rest of the space. */
  {
    int i = 0, bytes_to_write = 256-kno_getpos(stream);
    while (i<bytes_to_write) {
      kno_write_byte(outstream,0); i++;}}

  /* Write the top level bucket table */
  {
    int i = 0;
    if ((offtype == KNO_B32) || (offtype == KNO_B40))
      while (i<capacity) {
        kno_write_4bytes(outstream,0);
        kno_write_4bytes(outstream,0);
        i++;}
    else {
      while (i<capacity) {
        kno_write_8bytes(outstream,0);
        kno_write_4bytes(outstream,0);
        i++;}}}

  if (label) {
    int len = strlen(label);
    label_pos = kno_getpos(stream);
    kno_write_byte(outstream,dt_string);
    kno_write_4bytes(outstream,len);
    kno_write_bytes(outstream,label,len);
    label_size = kno_getpos(stream)-label_pos;}

  /* Write the actual slotcodes */
  if (VECTORP(slotcodes_init)) {
    int i = 0, len = VEC_LEN(slotcodes_init);
    slotcodes_pos = kno_getpos(stream);
    while (i<len) {
      lispval slotid = VEC_REF(slotcodes_init,i);
      kno_write_dtype(outstream,slotid);
      i++;}
    slotcodes_size = kno_getpos(stream)-slotcodes_pos;}
  else if (KNO_FIXNUMP(slotcodes_init)) {
    long long i=0, len = KNO_FIX2INT(slotcodes_init);
    slotcodes_pos = kno_getpos(stream);
    while (i<len) {
      kno_write_dtype(outstream,KNO_FALSE);
      i++;}
    slotcodes_size = kno_getpos(stream)-slotcodes_pos;}
  else NO_ELSE;

  if (KNO_TABLEP(metadata_init)) {
    metadata_pos = kno_getpos(stream);
    kno_write_dtype(outstream,metadata_init);
    metadata_size = kno_getpos(stream)-metadata_pos;}

  if (label_pos) {
    kno_setpos(stream,KNO_BIGPOOL_LABEL_POS);
    kno_write_8bytes(outstream,label_pos);
    kno_write_4bytes(outstream,label_size);}

  if (slotcodes_pos) {
    kno_setpos(stream,KNO_BIGPOOL_SLOTCODES_POS);
    kno_write_8bytes(outstream,slotcodes_pos);
    kno_write_4bytes(outstream,slotcodes_size);}

  if (metadata_pos) {
    kno_setpos(stream,KNO_BIGPOOL_METADATA_POS);
    kno_write_8bytes(outstream,metadata_pos);
    kno_write_4bytes(outstream,metadata_size);}

  kno_flush_stream(stream);
  kno_unlock_stream(stream);
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  return 0;
}

/* Methods */

static int bigpool_load(kno_pool p)
{
  kno_bigpool bp = (kno_bigpool)p;
  if (KNO_POOLSTREAM_LOCKEDP(bp))
    /* If we have the file locked, the stored load is good. */
    return bp->pool_load;
  else if (bp->pool_load == bp->pool_capacity)
    return bp->pool_load;
  else if (bp->pool_stream.stream_fileno >= 0) {
    struct stat info;
    int rv = fstat(bp->pool_stream.stream_fileno,&info);
    if ( (rv>=0) && ( bp->file_mtime >= info.st_mtime ) )
      return bp->pool_load;
    reload_bigpool(bp,KNO_UNLOCKED);
    return bp->pool_load;}
  else return bp->pool_load;
}

static lispval read_oid_value(kno_bigpool bp,
                              kno_inbuf in,
                              const u8_context cxt)
{
  int byte0 = kno_probe_byte(in);
  if (byte0==0xFF) {
    /* Compressed data */
    kno_compress_type zmethod=
      (kno_compress_type)(kno_read_byte(in), kno_read_varint(in));
    ssize_t data_len = kno_read_varint(in);
    if (data_len<0) {
      u8_seterr("ReadVarintFailed",cxt,u8_strdup(bp->poolid));
      return KNO_ERROR_VALUE;}
    else if (kno_request_bytes(in,data_len)<0) {
      u8_seterr("UnableToGetBytes",cxt,u8_strdup(bp->poolid));
      return KNO_EOD;}
    if (zmethod == KNO_NOCOMPRESS)
      return read_oid_value(bp,in,cxt);
    else {
      ssize_t uncompressed_len = 0;
      unsigned char *uncompressed =
        kno_uncompress(zmethod,&uncompressed_len,in->bufread,data_len,NULL);
      if (uncompressed) {
        struct KNO_INBUF inflated = { 0 };
        KNO_INIT_BYTE_INPUT(&inflated,uncompressed,uncompressed_len);
        lispval oid_value = read_oid_value(bp,&inflated,cxt);
        u8_big_free(uncompressed);
        return oid_value;}
      else return kno_err("UncompressFailed",cxt,bp->poolid,VOID);}}
  else if (byte0==0xF0) {
    /* Encoded slotmap/schemap */
    unsigned int n_slots = (kno_read_byte(in), kno_read_varint(in));
    lispval sm = kno_make_slotmap(n_slots+1,n_slots,NULL);
    struct KNO_KEYVAL *kvals = KNO_SLOTMAP_KEYVALS(sm);
    int i = 0; while (i<n_slots) {
      lispval key = VOID, val = VOID;
      int slot_byte0 = kno_probe_byte(in);
      if (slot_byte0==0xE0) {
        long long slotcode = (kno_read_byte(in), kno_read_varint(in));
        lispval slotid = (slotcode < 0) ? (KNO_ERROR) :
          kno_code2slotid( & bp->pool_slotcodes, slotcode );
        if (KNO_TROUBLEP(slotid)) {
          kno_seterr(_("BadSlotCode"),cxt,bp->poolid,VOID);
          kno_decref((lispval)sm);
          return KNO_ERROR;}
        else kvals[i].kv_key = slotid;}
      else kvals[i].kv_key = key = kno_read_dtype(in);
      if (KNO_ABORTP(key)) {
        KNO_SLOTMAP_NSLOTS(sm) = i;
        kno_decref(sm);
        return key;}
      else kvals[i].kv_val = val = kno_read_dtype(in);
      if (KNO_ABORTP(val)) {
        kno_decref(key);
        KNO_SLOTMAP_NSLOTS(sm) = i;
        kno_decref(sm);
        return val;}
      else i++;}
    return sm;} /* close (byte0==0xF0) */
  /* Just a regular dtype */
  else return kno_read_dtype(in);
}

static lispval read_oid_value_at
(kno_bigpool bp,lispval oid,kno_inbuf fetchbuf,KNO_CHUNK_REF ref,u8_context cxt)
{
  kno_stream stream=&(bp->pool_stream);
  if (ref.off == 0)
    return VOID;
  else if (ref.size == 0)
    return KNO_EMPTY;
  else if ( (fetchbuf == NULL) || (fetchbuf->buffer == NULL) ||
            (ref.size > fetchbuf->buflen) ) {
    struct KNO_INBUF _in={0}, *in =
      kno_open_block(stream,&_in,ref.off,ref.size,0);
    if (in) {
      lispval value = read_oid_value(bp,in,cxt);
      kno_close_inbuf(in);
      return value;}
    else if (bp->pool_flags & KNO_STORAGE_REPAIR) {
      KNO_OID addr = KNO_OID_ADDR(oid);
      u8_log(LOG_WARN,"BadBlockRef",
             "Couldn't read OID %llx/%llx from %lld+%lld in %s",
             KNO_OID_HI(addr),KNO_OID_LO(addr),
             ref.off,ref.size,
             bp->pool_source);
      kno_clear_errors(1);
      return KNO_EMPTY_CHOICE;}
    else return KNO_ERROR_VALUE;}
  else {
    struct KNO_INBUF *in = kno_open_block(stream,fetchbuf,ref.off,ref.size,0);
    if (in)
      return read_oid_value(bp,in,cxt);
    else if (bp->pool_flags & KNO_STORAGE_REPAIR) {
      KNO_OID addr = KNO_OID_ADDR(oid);
      u8_log(LOG_WARN,"BadBlockRef",
             "Couldn't read OID %llx/%llx from %lld+%lld in %s",
             KNO_OID_HI(addr),KNO_OID_LO(addr),
             ref.off,ref.size,
             bp->pool_source);
      kno_clear_errors(1);
      return KNO_EMPTY_CHOICE;}
    else return KNO_ERROR_VALUE;}
}

static int bigpool_locked_load(kno_pool p)
{
  kno_bigpool bp = (kno_bigpool)p;
  if (KNO_POOLSTREAM_LOCKEDP(bp))
    /* If we have the file locked, the stored load is good. */
    return bp->pool_load;
  else if (bp->pool_stream.stream_fileno >= 0) {
    struct stat info;
    int rv = fstat(bp->pool_stream.stream_fileno,&info);
    if ( (rv>=0) && ( bp->file_mtime >= info.st_mtime ) )
      return bp->pool_load;
    reload_bigpool(bp,KNO_ISLOCKED);
    return bp->pool_load;}
  else return bp->pool_load;
}

static lispval bigpool_fetch(kno_pool p,lispval oid)
{
  kno_bigpool bp = (kno_bigpool)p;
  KNO_OID addr   = KNO_OID_ADDR(oid);
  int offset    = KNO_OID_DIFFERENCE(addr,bp->pool_base);
  use_bigpool(bp);
  if (PRED_FALSE(offset>=bp->pool_load)) {
    /* It looks out of range, so double check by going to disk */
    if (offset>=(bigpool_locked_load(p))) {
      bigpool_finished(bp);
      if (bp->pool_flags&KNO_POOL_ADJUNCT)
        return KNO_EMPTY_CHOICE;
      else return KNO_UNALLOCATED_OID;}}

  if ((bp->pool_offdata) && (offset>=bp->pool_offlen))
    update_offdata_cache(bp,bp->pool_cache_level,get_chunk_ref_size(bp));

  unsigned int *offdata = bp->pool_offdata;
  unsigned int off_len = bp->pool_offlen;
  KNO_CHUNK_REF ref = (offdata) ?
    (kno_get_chunk_ref(offdata,bp->pool_offtype,offset,off_len)) :
    (kno_fetch_chunk_ref(&(bp->pool_stream),256,bp->pool_offtype,
                        offset,0));
  lispval result;
  if (ref.off < 0) result=KNO_ERROR;
  else if (ref.off == 0) result=EMPTY;
  else {
    struct KNO_INBUF in = { 0 };
    unsigned char buf[FETCHBUF_SIZE];
    if (ref.size < FETCHBUF_SIZE) {
      KNO_INIT_INBUF(&in,buf,FETCHBUF_SIZE,0);}
    result=read_oid_value_at(bp,oid,&in,ref,"bigpool_fetch");
    kno_close_inbuf(&in);}
  bigpool_finished(bp);
  return result;
}

static int compare_offsets(const void *x1,const void *x2)
{
  const struct BIGPOOL_FETCH_SCHEDULE *s1 = x1, *s2 = x2;
  if (s1->location.off<s2->location.off) return -1;
  else if (s1->location.off>s2->location.off) return 1;
  else return 0;
}

static lispval *bigpool_fetchn(kno_pool p,int n,lispval *oids)
{
  KNO_OID base = p->pool_base;
  kno_bigpool bp = (kno_bigpool)p;
  struct KNO_STREAM *stream = &(bp->pool_stream);
  lispval *values = u8_big_alloc_n(n,lispval);

  use_bigpool(bp); /* Ensure that the file stream is opened */

  unsigned int *offdata = bp->pool_offdata;
  unsigned int offdata_offlen = bp->pool_offlen;
  unsigned int load = bp->pool_load;
  if (offdata == NULL) {
    /* Don't bother being clever if you don't even have an offsets
       table. */
    int i = 0; while (i<n) {
      values[i]=bigpool_fetch(p,oids[i]);
      i++;}
    bigpool_finished(bp);
    return values;}
  else {
    unsigned int unlock_stream = 0;
    struct BIGPOOL_FETCH_SCHEDULE *schedule=
      u8_big_alloc_n(n,struct BIGPOOL_FETCH_SCHEDULE);
    if (bp->pool_load>bp->pool_offlen)
      update_offdata_cache(bp,bp->pool_cache_level,get_chunk_ref_size(bp));
    int i = 0;
    /* Populate a fetch schedule with where to get OID values */
    while (i<n) {
      lispval oid = oids[i]; KNO_OID addr = KNO_OID_ADDR(oid);
      unsigned int off = KNO_OID_DIFFERENCE(addr,base);
      schedule[i].value_at = i;
      if (off<load)
        schedule[i].location =
          kno_get_chunk_ref(offdata,bp->pool_offtype,off,offdata_offlen);
      else {
        kno_seterr(kno_UnallocatedOID,"bigpool_fetchn",p->poolid,oids[i]);
        break;}
      if (schedule[i].location.off<0) {
        kno_seterr(InvalidOffset,"bigpool_fetchn",p->poolid,oids[i]);
        break;}
      else i++;}
    if (i<n) {
      u8_big_free(schedule);
      u8_big_free(values);
      bigpool_finished(bp);
      return NULL;}
    /* Note that we sort the fetch schedule even if we're mmapped in
       order to try to take advantage of page locality. */
    qsort(schedule,n,sizeof(struct BIGPOOL_FETCH_SCHEDULE),compare_offsets);

    unsigned char bytes[FETCHBUF_SIZE];
    struct KNO_INBUF sbuf={0}, mbuf={0};
    KNO_INIT_INBUF(&sbuf,bytes,FETCHBUF_SIZE,0);

    i = 0; while (i<n) {
      if (schedule[i].location.size==0)
        values[schedule[i].value_at]=KNO_EMPTY;
      else {
        kno_inbuf usebuf =
          ( schedule[i].location.size < FETCHBUF_SIZE ) ? (&sbuf) :
          (KNO_USE_MMAP) ? (&mbuf) : (&sbuf);
        kno_inbuf in = kno_open_block(stream,usebuf,
                                    schedule[i].location.off,
                                    schedule[i].location.size,
                                    0);
        if (in == NULL) {
          if (bp->pool_flags & KNO_STORAGE_REPAIR) {
            int value_at = schedule[i].value_at;
            lispval oid = oids[value_at];
            KNO_OID addr = KNO_OID_ADDR(oid);
            u8_log(LOG_WARN,"FatchFailed",
                   "Couldn't read block for %llx/%llx at %lld+%lld from %s",
                   KNO_OID_HI(addr),KNO_OID_LO(addr),
                   schedule[i].location.off,
                   schedule[i].location.size,
                   bp->poolid);
            kno_clear_errors(1);
            values[schedule[i].value_at]=KNO_VOID;
            i++;
            continue;}
          else break;}
        lispval value = read_oid_value(bp,in,"bigpool_fetchn");
        if (KNO_ABORTP(value)) {
          if (bp->pool_flags & KNO_STORAGE_REPAIR) {
            int value_at = schedule[i].value_at;
            lispval oid = oids[value_at];
            KNO_OID addr = KNO_OID_ADDR(oid);
            u8_log(LOG_WARN,"FatchFailed",
                   "Couldn't read value for %llx/%llx at %lld+%lld from %s",
                   KNO_OID_HI(addr),KNO_OID_LO(addr),
                   schedule[i].location.off,
                   schedule[i].location.size,
                   bp->poolid);
            values[schedule[i].value_at]=KNO_VOID;
            i++;
            continue;}
          else break;}
        else values[schedule[i].value_at]=value;}
      i++;}

    if ( (i != n) && ( (bp->pool_flags & KNO_STORAGE_REPAIR) == 0) ) { /* Error */
      int j = 0; while (j<i) {
        lispval value = values[schedule[j].value_at];
        kno_decref(value);
        j++;}
      u8_seterr("FetchNError","bigpool_fetchn/read",u8dup(bp->poolid));
      if (unlock_stream)
        kno_unlock_stream(&(bp->pool_stream));
      u8_big_free(values);
      values=NULL;}
    kno_close_inbuf(&mbuf);
    kno_close_inbuf(&sbuf);
    u8_big_free(schedule);
    bigpool_finished(bp);
    return values;}
}

/* Saving changed OIDs */

static ssize_t write_offdata
(struct KNO_BIGPOOL *bp, kno_stream stream,int n,unsigned int load,
 struct BIGPOOL_SAVEINFO *saveinfo);
static ssize_t bigpool_write_value(kno_bigpool p,lispval value,
                                   kno_stream stream,
                                   struct KNO_OUTBUF *tmpout,
                                   unsigned char **zbuf,int *zbuf_size);
static int update_bigpool(kno_bigpool bp,kno_stream stream,kno_stream head_stream,
                          int new_load,int n_saved,
                          struct BIGPOOL_SAVEINFO *saveinfo,
                          lispval metadata);

static int file_format_overflow(kno_pool p,kno_stream stream)
{
  u8_seterr(kno_DataFileOverflow,"bigpool_storen",u8_strdup(p->poolid));
  if (kno_streamctl(stream,kno_stream_unlockfile,NULL)<0)
    u8_logf(LOG_CRIT,"UnlockFailed",
            "Couldn't unlock output stream (%d:%s) for %s",
            stream->stream_fileno,
            p->pool_source,
            p->poolid);
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  return -1;
}

static kno_stream get_commit_stream(kno_bigpool bp,struct KNO_POOL_COMMITS *commit)
{
  if (commit->commit_stream)
    return commit->commit_stream;
  else if (u8_file_writablep(bp->pool_source)) {
    ssize_t buf_size =
      kno_getfixopt(bp->pool_opts,"WRITESIZE",
                   kno_getfixopt(bp->pool_opts,"BUFSIZE",kno_driver_bufsize));
    struct KNO_STREAM *new_stream =
      kno_init_file_stream(NULL,bp->pool_source,KNO_FILE_MODIFY,-1,buf_size);
    /* Lock the file descriptor */
    if (kno_streamctl(new_stream,kno_stream_lockfile,NULL)<0) {
      kno_close_stream(new_stream,KNO_STREAM_FREEDATA);
      u8_free(new_stream);
      kno_seterr("CantLockFile","get_commit_stream/bigpool",
                bp->pool_source,KNO_VOID);
      return NULL;}
    commit->commit_stream = new_stream;
    return new_stream;}
  else {
    kno_seterr("CantWriteFile","get_commit_stream/bigpool",
              bp->pool_source,KNO_VOID);
    return NULL;}
}

static void release_commit_stream(kno_pool p,struct KNO_POOL_COMMITS *commit)
{
  kno_stream stream = commit->commit_stream;
  if (stream == NULL) return;
  else commit->commit_stream=NULL;
  if (kno_streamctl(stream,kno_stream_unlockfile,NULL)<0)
    u8_logf(LOG_WARN,"CantUnLockFile",
            "For commit stream of bigpool %s",p->poolid);
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  u8_free(stream);
}

static int bigpool_storen(kno_pool p,struct KNO_POOL_COMMITS *commits,
                          kno_stream stream,kno_stream head_stream)
{
  kno_bigpool bp = (kno_bigpool)p;
  int n = commits->commit_count;
  lispval *oids = commits->commit_oids;
  lispval *values = commits->commit_vals;
  lispval metadata = commits->commit_metadata;
  double started = u8_elapsed_time();

  u8_string fname=bp->pool_source;
  if (!(u8_file_writablep(fname))) {
    kno_seterr("CantWriteFile","bigpool_storen",fname,KNO_VOID);
    return -1;}

  unsigned int load = bp->pool_load;

  struct BIGPOOL_SAVEINFO *saveinfo= (n>0) ?
    (u8_big_alloc_n(n,struct BIGPOOL_SAVEINFO)) :
    (NULL);

  int i=0;
  if (n>0) { /* There are values to save */
    int new_blocks = 0;
    u8_logf(LOG_DEBUG,"BigpoolStore",
            "Storing %d oid values in bigpool %s",n,p->poolid);

    /* These are used repeatedly for rendering objects to dtypes or to
       compressed dtypes. */
    struct KNO_OUTBUF tmpout = { 0 };
    unsigned char *zbuf = u8_malloc(KNO_INIT_ZBUF_SIZE);
    unsigned int zbuf_size = KNO_INIT_ZBUF_SIZE;
    unsigned int init_buflen = 2048*n;
    if (init_buflen>262144) init_buflen = 262144;
    KNO_INIT_BYTE_OUTPUT(&tmpout,init_buflen);
    if ((bp->bigpool_format)&(KNO_BIGPOOL_DTYPEV2))
      tmpout.buf_flags |= KNO_USE_DTYPEV2;
    tmpout.buf_flags |= KNO_WRITE_OPAQUE;

    KNO_OID base = bp->pool_base;
    int isadjunct = (bp->pool_flags) & (KNO_POOL_ADJUNCT);
    size_t maxpos = get_maxpos(bp);
    kno_off_t endpos = kno_endpos(stream);

    while (i<n) {
      /* We walk over all of the oids, Writing their values to disk and
         recording where we wrote them. */
      KNO_OID addr = KNO_OID_ADDR(oids[i]);
      unsigned int offset = KNO_OID_LO(addr)-KNO_OID_LO(base);
      lispval value = values[i];
      ssize_t n_bytes = 0;
      if ( (KNO_CONSTANTP(value)) &&
           ( ( value >= KNO_EOF) && (value <= KNO_UNALLOCATED_OID) ) ) {
        u8_logf(LOG_CRIT,"BadOIDValue",
                "The value for @%x/%x (%q) couldn't be written to %s",
                KNO_OID_HI(addr),KNO_OID_LO(addr),value,p->poolid);}
      else n_bytes = bigpool_write_value(bp,value,stream,
                                         &tmpout,&zbuf,&zbuf_size);
      if (n_bytes<0) {
        /* Should there be a way to force an error to be signalled here? */
        u8_logf(LOG_CRIT,"BadOIDValue",
                "The value for %x/%x couldn't be written to save to %s",
                KNO_OID_HI(addr),KNO_OID_LO(addr),p->poolid);
        n_bytes=0;}

      /* We keep track of value blocks in the file so we can determine
         how much space is being wasted. */
      if (n_bytes) {
        if ( (isadjunct) && (offset >= load) ) load=offset+1;
        new_blocks++;}

      /* Check for file format overflow */
      if ((endpos+n_bytes)>=maxpos) {
        u8_free(zbuf);
        u8_big_free(saveinfo);
        kno_close_outbuf(&tmpout);
        return file_format_overflow(p,stream);}

      if (n_bytes) {
        saveinfo[i].chunk.off  = endpos;
        saveinfo[i].chunk.size = n_bytes;
        saveinfo[i].oidoff     = KNO_OID_DIFFERENCE(addr,base);}
      else {
        saveinfo[i].chunk.off  = 0;
        saveinfo[i].chunk.size = 0;
        saveinfo[i].oidoff     = KNO_OID_DIFFERENCE(addr,base);}

      endpos = endpos+n_bytes;

      i++;}
    kno_close_outbuf(&tmpout);
    u8_free(zbuf);}

  double apply_started = u8_elapsed_time();
  if (head_stream == stream)
    commits->commit_times.write = apply_started-started;

  kno_lock_pool_struct(p,1);

  if (update_bigpool(bp,stream,head_stream,load,n,saveinfo,metadata)<0) {
    u8_logf(LOG_CRIT,"BigpoolUpdateFailed",
            "Couldn't update bigpool %s",bp->poolid);

    /* Unlock the pool */
    kno_unlock_pool_struct(p);

    return -1;}

  u8_big_free(saveinfo);

  fsync(stream->stream_fileno);

  kno_write_4bytes_at(head_stream,KNO_BIGPOOL_MAGIC_NUMBER,0);
  kno_flush_stream(head_stream);

  u8_logf(LOG_INFO,"BigpoolStore",
          "Stored %d oid values in bigpool %s in %f seconds",
          n,p->poolid,u8_elapsed_time()-started);

  /* Unlock the pool */
  kno_unlock_pool_struct(p);

  if (head_stream != stream)
    commits->commit_times.write = u8_elapsed_time()-started;
  else commits->commit_times.sync = u8_elapsed_time()-apply_started;

  return n;
}

static int bigpool_commit(kno_pool p,kno_commit_phase phase,
                          struct KNO_POOL_COMMITS *commits)
{
  kno_bigpool bp = (kno_bigpool) p;
  kno_stream stream = get_commit_stream(bp,commits);
  bigpool_setcache(bp,-1);
  if (stream == NULL)
    return -1;
  u8_string fname = p->pool_source;
  int chunk_ref_size = get_chunk_ref_size(bp);
  switch (phase) {
  case kno_no_commit:
    u8_seterr("BadCommitPhase(commit_none)","bigpool_commit",
              u8_strdup(p->poolid));
    return -1;
  case kno_commit_start: {
    size_t cap = p->pool_capacity;
    size_t recovery_size = 256+(chunk_ref_size*cap);
    int rv = kno_write_rollback("bigpool_commit",p->poolid,fname,recovery_size);
    if (rv>=0) commits->commit_phase = kno_commit_write;
    return rv;}
  case kno_commit_write: {
    struct KNO_STREAM *head_stream = NULL;
    if (commits->commit_2phase) {
      size_t cap = p->pool_capacity;
      size_t commit_size = 256+(chunk_ref_size*cap);
      u8_string commit_file = u8_mkstring("%s.commit",fname);
      ssize_t head_saved = kno_save_head(fname,commit_file,commit_size);
      head_stream = (head_saved>=0) ?
        (kno_init_file_stream(NULL,commit_file,KNO_FILE_MODIFY,-1,-1)) :
        (NULL);
      if (head_stream == NULL) {
        u8_seterr("CantOpenCommitFile","bigpool_commit",commit_file);
        return -1;}
      else u8_free(commit_file);}
    else head_stream=stream;
    int rv = bigpool_storen(p,commits,stream,head_stream);
    kno_flush_stream(stream);
    if (head_stream != stream) {
      size_t endpos = kno_endpos(stream);
      size_t head_endpos = kno_endpos(head_stream);
      kno_write_8bytes_at(head_stream,endpos,head_endpos);
      kno_flush_stream(head_stream);
      kno_close_stream(head_stream,KNO_STREAM_FREEDATA);
      u8_free(head_stream);}
    if (rv<0)
      commits->commit_phase = kno_commit_rollback;
    else if (commits->commit_2phase)
      commits->commit_phase = kno_commit_sync;
    else commits->commit_phase = kno_commit_flush;
    return rv;}
  case kno_commit_sync: {
    u8_string commit = u8_mkstring("%s.commit",fname);
    ssize_t rv = kno_apply_head(commit,fname);
    u8_free(commit);
    if (rv >= 0) {
      update_offdata_cache(bp,p->pool_cache_level,chunk_ref_size);
      return 1;}
    else return -1;}
  case kno_commit_rollback: {
    u8_string rollback = u8_mkstring("%s.rollback",fname);
    ssize_t rv = kno_apply_head(rollback,fname);
    u8_free(rollback);
    if (rv<0) return -1; else return 1;}
  case kno_commit_flush: {
    return 1;}
  case kno_commit_cleanup: {
    if (commits->commit_stream) release_commit_stream(p,commits);
    u8_string rollback = u8_mkstring("%s.rollback",fname);
    if (u8_file_existsp(rollback)) {
      if ( (u8_removefile(rollback)) < 0) {
        u8_logf(LOG_WARN,"PoolCleanupFailed",
                "Couldn't remove file %s for %s",rollback,fname);}}
    u8_free(rollback);
    if (commits->commit_2phase) {
      u8_string commit = u8_mkstring("%s.commit",fname);
      if (u8_file_existsp(commit)) {
        if ( (u8_removefile(commit)) < 0) {
          u8_logf(LOG_WARN,"PoolCleanupFailed",
                  "Couldn't remove file %s for %s",commit,fname);}}
      u8_free(commit);}
    return 0;}
  default:
    return 0;
  }
}

/* Writing OID values */

static ssize_t bigpool_write_value(kno_bigpool p,lispval value,
                                   kno_stream stream,
                                   struct KNO_OUTBUF *tmpout,
                                   unsigned char **zbuf,int *zbuf_size)
{
  kno_outbuf outstream = kno_writebuf(stream);
  /* Reset the tmpout stream */
  tmpout->bufwrite = tmpout->buffer;
  if (SCHEMAPP(value)) {
    struct KNO_SCHEMAP *sm = (kno_schemap)value;
    lispval *schema = sm->table_schema;
    lispval *values = sm->schema_values;
    int i = 0, size = sm->schema_length;
    kno_write_byte(tmpout,0xF0);
    kno_write_varint(tmpout,size);
    while (i<size) {
      lispval slotid = schema[i], value = values[i];
      if ( ( (KNO_SYMBOLP(slotid)) || (KNO_OIDP(slotid)) ) &&
           (p->pool_slotcodes.slotids) ) {
        int slotcode = kno_slotid2code( & p->pool_slotcodes , slotid );
        if (slotcode < 0)
          slotcode = kno_add_slotcode( & p->pool_slotcodes , slotid );
        if (slotcode>=0) {
          kno_write_byte(tmpout,0xE0);
          kno_write_varint(tmpout,slotcode);}
        else if (kno_write_dtype(tmpout,slotid)<0)
          return -1;
        else NO_ELSE;}
      else if (kno_write_dtype(tmpout,slotid)<0)
        return -1;
      else NO_ELSE;
      if (kno_write_dtype(tmpout,value)<0)
        return -1;
      else i++;}}
  else if (SLOTMAPP(value)) {
    struct KNO_SLOTMAP *sm = (kno_slotmap)value;
    struct KNO_KEYVAL *keyvals = sm->sm_keyvals;
    int i = 0, size = sm->n_slots;
    kno_write_byte(tmpout,0xF0);
    kno_write_varint(tmpout,size);
    while (i<size) {
      lispval slotid = keyvals[i].kv_key;
      lispval value  = keyvals[i].kv_val;
      if ( ( (KNO_SYMBOLP(slotid)) || (KNO_OIDP(slotid)) ) &&
           (p->pool_slotcodes.slotids) ) {
        int slotcode = kno_slotid2code( & p->pool_slotcodes , slotid );
        if (slotcode < 0)
          slotcode = kno_add_slotcode( & p->pool_slotcodes , slotid );
        if (slotcode>=0) {
          kno_write_byte(tmpout,0xE0);
          kno_write_varint(tmpout,slotcode);}
        else if (kno_write_dtype(tmpout,slotid)<0)
          return -1;
        else NO_ELSE;}
      else if (kno_write_dtype(tmpout,slotid)<0)
        return -1;
      else NO_ELSE;
      if (kno_write_dtype(tmpout,value)<0)
        return -1;
      else i++;}}
  else if (kno_write_dtype(tmpout,value)<0)
    return -1;
  if (p->pool_compression) {
    size_t source_length = tmpout->bufwrite-tmpout->buffer;
    size_t compressed_length = 0;
    unsigned char *compressed =
      kno_compress(p->pool_compression,&compressed_length,
                  tmpout->buffer,source_length,NULL);
    if (compressed) {
      size_t header = 1;
      kno_write_byte(outstream,0xFF);
      header+=kno_write_varint(outstream,(int)p->pool_compression);
      header+=kno_write_varint(outstream,compressed_length);
      kno_write_bytes(outstream,compressed,compressed_length);
      u8_big_free(compressed);
      return header+compressed_length;}
    else {
      u8_log(LOG_CRIT,"CompressionFailed",
             "Couldn't write compressed value to %s",
             p->poolid);}}
  /* If you can't compress (for whatever reason), fall through to here
     and just output directly */
  kno_write_bytes(outstream,tmpout->buffer,tmpout->bufwrite-tmpout->buffer);
  return tmpout->bufwrite-tmpout->buffer;
}

/* Updating information in pool */

static int write_bigpool_load(kno_bigpool bp,
                              unsigned int new_load,
                              kno_stream stream)
{
  /* Update the load */
  long long load;
  load = kno_read_4bytes_at(stream,16,KNO_ISLOCKED);
  if (load<0) {
    return -1;}
  else if (new_load>load) {
    int rv = kno_write_4bytes_at(stream,new_load,16);
    if (rv<0) return rv;
    bp->pool_load = new_load;
    return rv;}
  else {
    return 0;}
}

static int bump_bigpool_nblocks(kno_bigpool bp,
                                unsigned int new_blocks,
                                kno_stream stream)
{
  /* Update the load */
  int err=0;
  unsigned long long n_blocks =
    kno_read_8bytes_at(stream,0x60,KNO_ISLOCKED,&err);
  if (err>=0) {
    n_blocks = n_blocks + new_blocks;
    bp->pool_nblocks=n_blocks;
    return kno_write_8bytes_at(stream,n_blocks,0x60);}
  else return -1;
}

static int set_bigpool_label(kno_bigpool bp,u8_string label)
{
  int rv = -1;
  u8_string source = bp->pool_source;
  kno_off_t label_pos = 0; size_t label_size = 0;
  /* We use this so we don't intrude on any active commits */
  u8_lock_mutex(&(bp->pool_commit_lock));
  /* But we open our own copy of the file */
  kno_stream stream = kno_init_file_stream
    (NULL,source,KNO_FILE_MODIFY,-1,256);
  if (stream == NULL) {
    u8_unlock_mutex(&(bp->pool_commit_lock));
    return -1;}
  if (kno_streamctl(stream,kno_stream_lockfile,NULL)<0) {
    kno_close_stream(stream,KNO_STREAM_FREEDATA);
    return -1;}
  if (label) {
    size_t len = strlen(label);
    label_pos = kno_endpos(stream);
    kno_outbuf out = kno_writebuf(stream);
    label_size = strlen(label)+5;
    if (out) {
      rv = kno_write_byte(out,dt_string);
      if (rv>0) rv=kno_write_4bytes(out,len);
      if (rv>0) rv=kno_write_bytes(out,label,len);
      kno_flush_stream(stream);}}
  else {/* Resetting label */}
  if (rv > 0) {
    rv = kno_setpos(stream,KNO_BIGPOOL_LABEL_POS);
    if (rv>=0) {
      kno_outbuf out = kno_writebuf(stream);
      if (out) rv=kno_write_8bytes(out,label_pos);
      if (rv>0) rv=kno_write_4bytes(out,label_size);
      if (rv>0) kno_flush_stream(stream);}}
  if (kno_streamctl(stream,kno_stream_unlockfile,NULL)<0) {
    int saved_errno = errno; errno=0;
    u8_logf(LOG_ERR,"LabelUnlockFailed",
            "Couldn't unlock file %s errno=%d:%s",
            source,saved_errno,u8_strerror(saved_errno));}
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  u8_free(stream);
  return rv;
}

/* These assume that the pool itself is locked */
static int write_bigpool_slotids(kno_bigpool bp,kno_stream stream,kno_stream head_stream)
{
  if (bp->pool_slotcodes.n_slotcodes > bp->pool_slotcodes.init_n_slotcodes ) {
    lispval *slotids = bp->pool_slotcodes.slotids->vec_elts;
    unsigned int n_slotids = bp->pool_slotcodes.n_slotcodes;
    off_t start_pos = kno_endpos(stream), end_pos = start_pos;
    kno_outbuf out = kno_writebuf(stream);
    int i = 0, lim = bp->pool_slotcodes.n_slotcodes; while (i<lim) {
      lispval slotid = slotids[i++];
      ssize_t size = kno_write_dtype(out,slotid);
      if (size<0) return -1;
      else end_pos+=size;}
    kno_write_4bytes_at(head_stream,n_slotids,0x50);
    kno_write_8bytes_at(head_stream,start_pos,0x54);
    kno_write_4bytes_at(head_stream,end_pos-start_pos,0x5c);
    return 1;}
  else return 0;
}

/* These assume that the pool itself is locked */

static int write_bigpool_metadata(kno_bigpool bp,lispval metadata,
                                  kno_stream stream,kno_stream head_stream)
{
  if (KNO_TABLEP(metadata)) {
    u8_logf(LOG_WARN,"WriteMetadata",
            "Writing modified metadata for %s",bp->poolid);
    off_t start_pos = kno_endpos(stream), end_pos = start_pos;
    kno_outbuf out = kno_writebuf(stream);
    int rv=kno_write_dtype(out,metadata);
    if (rv<0) {
      u8_exception ex = u8_current_exception;
      u8_condition cond = (ex) ? (ex->u8x_cond) :
        ((u8_condition)"Unknown DType error");
      u8_logf(LOG_CRIT,cond,"Couldnt'save metadata for bigpool %s: %q",
              bp->poolid,metadata);
      return rv;}
    else end_pos=kno_endpos(stream);
    kno_flush_stream(stream);
    kno_write_8bytes_at(head_stream,start_pos,KNO_BIGPOOL_METADATA_POS);
    kno_write_4bytes_at(head_stream,end_pos-start_pos,KNO_BIGPOOL_METADATA_POS+8);
    kno_set_modified(metadata,0);
    return 1;}
  else return 0;
}

static int update_bigpool(kno_bigpool bp,kno_stream stream,kno_stream head_stream,
                          int new_load,int n_saved,
                          struct BIGPOOL_SAVEINFO *saveinfo,
                          lispval metadata)
{
  int rv=write_bigpool_slotids(bp,stream,head_stream);
  if (saveinfo) {
    if (rv>=0) rv=write_offdata(bp,head_stream,n_saved,new_load,saveinfo);
    if (rv>=0) rv=bump_bigpool_nblocks(bp,n_saved,head_stream);}
  if ( (rv>=0) && (KNO_SLOTMAPP(metadata)) )
    rv=write_bigpool_metadata(bp,metadata,stream,head_stream);
  kno_flush_stream(stream);
  fsync(stream->stream_fileno);
  size_t end_pos = kno_endpos(stream);
  kno_write_8bytes_at(head_stream,end_pos,256-8);
  if (rv>=0) rv=write_bigpool_load(bp,new_load,head_stream);
  return rv;
}

/* Three different ways to write offdata */

static ssize_t mmap_write_offdata
(struct KNO_BIGPOOL *bp,kno_stream stream,int n,
 struct BIGPOOL_SAVEINFO *saveinfo,
 unsigned int min_off,
 unsigned int max_off);
static ssize_t cache_write_offdata
(struct KNO_BIGPOOL *bp,kno_stream stream, int n,
 struct BIGPOOL_SAVEINFO *saveinfo,
 unsigned int *cur_offdata,
 unsigned int min_off,
 unsigned int max_off);
static ssize_t direct_write_offdata
(struct KNO_BIGPOOL *bp,kno_stream stream,int n,
 struct BIGPOOL_SAVEINFO *saveinfo);

static ssize_t write_offdata
(struct KNO_BIGPOOL *bp,kno_stream stream,int n,unsigned int load,
 struct BIGPOOL_SAVEINFO *saveinfo)
{
  unsigned int min_off=load,  max_off=0, i=0;
  kno_offset_type offtype = bp->pool_offtype;
  if (!((offtype == KNO_B32)||(offtype = KNO_B40)||(offtype = KNO_B64))) {
    u8_logf(LOG_WARN,CorruptBigpool,
            "Bad offset type code (%d) for %s",
            (int)offtype,bp->poolid);
    u8_seterr(CorruptBigpool,"bigpool:write_offdata:bad_offtype",
              u8_strdup(bp->poolid));
    u8_big_free(saveinfo);
    return -1;}
  else while (i<n) {
      unsigned int oidoff = saveinfo[i++].oidoff;
      if (oidoff>max_off) max_off = oidoff;
      if (oidoff<min_off) min_off = oidoff;}

  if (bp->pool_cache_level >= 2) {
#if KNO_USE_MMAP
    ssize_t result=
      mmap_write_offdata(bp,stream,n,saveinfo,min_off,max_off);
    if (result>=0) {
      return result;}
#endif
    result=cache_write_offdata(bp,stream,n,saveinfo,bp->pool_offdata,
                               min_off,max_off);
    if (result>=0) {
      kno_clear_errors(0);
      return result;}}
  ssize_t result=direct_write_offdata(bp,stream,n,saveinfo);
  return result;
}

static ssize_t mmap_write_offdata
(struct KNO_BIGPOOL *bp,kno_stream stream,
 int n, struct BIGPOOL_SAVEINFO *saveinfo,
 unsigned int min_off,unsigned int max_off)
{
  int chunk_ref_size = get_chunk_ref_size(bp);
  int retval = -1;
  u8_logf(LOG_DEBUG,"bigpool:write_offdata",
          "Finalizing %d oid values for %s",n,bp->poolid);

  unsigned int *offdata = NULL;
  size_t byte_length = chunk_ref_size*(max_off+1);
  /* Map a second version of offdata to modify */
  unsigned int *memblock=
    mmap(NULL,256+(byte_length),(PROT_READ|PROT_WRITE),MAP_SHARED,
         stream->stream_fileno,0);
  if ( (memblock==NULL) || (memblock == MAP_FAILED) ) {
    u8_logf(LOG_CRIT,u8_strerror(errno),
            "Failed MMAP of %lld bytes of offdata for bigpool %s",
            256+(byte_length),bp->poolid);
    u8_graberrno("bigpool_write_offdata",u8_strdup(bp->poolid));
    return -1;}
  else offdata = memblock+64;
  switch (bp->pool_offtype) {
  case KNO_B64: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      offdata[oidoff*3]=kno_net_order((saveinfo[k].chunk.off)>>32);
      offdata[oidoff*3+1]=kno_net_order((saveinfo[k].chunk.off)&(0xFFFFFFFF));
      offdata[oidoff*3+2]=kno_net_order(saveinfo[k].chunk.size);
      k++;}
    break;}
  case KNO_B32: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      offdata[oidoff*2]=kno_net_order(saveinfo[k].chunk.off);
      offdata[oidoff*2+1]=kno_net_order(saveinfo[k].chunk.size);
      k++;}
    break;}
  case KNO_B40: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff, w1 = 0, w2 = 0;
      kno_convert_KNO_B40_ref(saveinfo[k].chunk,&w1,&w2);
      offdata[oidoff*2]=kno_net_order(w1);
      offdata[oidoff*2+1]=kno_net_order(w2);
      k++;}
    break;}
  default:
    u8_logf(LOG_WARN,"Bad offset type for %s",bp->poolid);
    u8_big_free(saveinfo);
    exit(-1);}
  retval = msync(offdata-64,256+byte_length,MS_SYNC|MS_INVALIDATE);
  if (retval<0) {
    u8_logf(LOG_WARN,u8_strerror(errno),
            "bigpool:write_offdata:msync %s",bp->poolid);
    u8_graberrno("bigpool_write_offdata:msync",u8_strdup(bp->poolid));}
  retval = munmap(offdata-64,256+byte_length);
  if (retval<0) {
    u8_logf(LOG_WARN,u8_strerror(errno),
            "bigpool/bigpool_write_offdata:munmap %s",bp->poolid);
    u8_graberrno("bigpool_write_offdata:munmap",u8_strdup(bp->poolid));
    return -1;}
  return n;
}

static ssize_t cache_write_offdata
(struct KNO_BIGPOOL *bp,kno_stream stream,
 int n, struct BIGPOOL_SAVEINFO *saveinfo,
 unsigned int *cur_offdata,
 unsigned int min_off,unsigned int max_off)
{
  int chunk_ref_size = get_chunk_ref_size(bp);
  size_t offdata_modified_length = chunk_ref_size*(1+(max_off-min_off));
  size_t offdata_modified_start = chunk_ref_size*min_off;
  unsigned int *offdata =u8_zmalloc(offdata_modified_length);
  if (offdata == NULL) {
    u8_graberrno("bigpool:write_offdata:malloc",u8_strdup(bp->poolid));
    return -1;}
  memcpy(offdata,cur_offdata+offdata_modified_start,offdata_modified_length);
  switch (bp->pool_offtype) {
  case KNO_B64: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      offdata[oidoff*3]=(saveinfo[k].chunk.off)>>32;
      offdata[oidoff*3+1]=(saveinfo[k].chunk.off)&(0xFFFFFFFF);
      offdata[oidoff*3+2]=saveinfo[k].chunk.size;
      k++;}
    break;}
  case KNO_B32: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      offdata[oidoff*2]=saveinfo[k].chunk.off;
      offdata[oidoff*2+1]=saveinfo[k].chunk.size;
      k++;}
    break;}
  case KNO_B40: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff, w1 = 0, w2 = 0;
      kno_convert_KNO_B40_ref(saveinfo[k].chunk,&w1,&w2);
      offdata[oidoff*2]=w1;
      offdata[oidoff*2+1]=w2;
      k++;}
    break;}
  default:
    u8_logf(LOG_WARN,"Bad offset type for %s",bp->poolid);
    u8_big_free(saveinfo);
    exit(-1);}
  kno_setpos(stream,256+offdata_modified_start);
  kno_write_ints(stream,offdata_modified_length,offdata);
  u8_free(offdata);
  return n;
}

static ssize_t direct_write_offdata(struct KNO_BIGPOOL *bp,kno_stream stream,
                                    int n, struct BIGPOOL_SAVEINFO *saveinfo)
{
  kno_outbuf outstream = kno_writebuf(stream);
  switch (bp->pool_offtype) {
  case KNO_B32: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      kno_setpos(stream,256+oidoff*8);
      kno_write_4bytes(outstream,saveinfo[k].chunk.off);
      kno_write_4bytes(outstream,saveinfo[k].chunk.size);
      k++;}
    break;}
  case KNO_B40: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff, w1 = 0, w2 = 0;
      kno_setpos(stream,256+oidoff*8);
      kno_convert_KNO_B40_ref(saveinfo[k].chunk,&w1,&w2);
      kno_write_4bytes(outstream,w1);
      kno_write_4bytes(outstream,w2);
      k++;}
    break;}
  case KNO_B64: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      kno_setpos(stream,256+oidoff*12);
      kno_write_8bytes(outstream,saveinfo[k].chunk.off);
      kno_write_4bytes(outstream,saveinfo[k].chunk.size);
      k++;}
    break;}
  default:
    u8_logf(LOG_WARN,"Bad offset type for %s",bp->poolid);
    u8_big_free(saveinfo);
    exit(-1);}
  return n;
}

/* Allocating OIDs */

static lispval bigpool_alloc(kno_pool p,int n)
{
  kno_bigpool bp = (kno_bigpool)p;
  lispval results = EMPTY; int i = 0;
  KNO_OID base=bp->pool_base;
  unsigned int start;
  kno_lock_pool_struct(p,1);
  if (!(POOLFILE_LOCKEDP(bp)))
    lock_bigpool_file(bp,KNO_ISLOCKED);
  if ( (bp->pool_load+n) > (bp->pool_capacity) ) {
    kno_unlock_pool_struct(p);
    return kno_err(kno_ExhaustedPool,"bigpool_alloc",
                  p->poolid,VOID);}
  start=bp->pool_load; bp->pool_load+=n;
  kno_unlock_pool_struct(p);
  while (i < n) {
    KNO_OID new_addr = KNO_OID_PLUS(base,start+i);
    lispval new_oid = kno_make_oid(new_addr);
    CHOICE_ADD(results,new_oid);
    i++;}
  return kno_simplify_choice(results);
}

/* Locking and unlocking */

static int bigpool_lock(kno_pool p,lispval oids)
{
  struct KNO_BIGPOOL *fp = (struct KNO_BIGPOOL *)p;
  int retval = lock_bigpool_file(fp,1);
  return retval;
}

static int bigpool_unlock(kno_pool p,lispval oids)
{
  struct KNO_BIGPOOL *fp = (struct KNO_BIGPOOL *)p;
  if (fp->pool_changes.table_n_keys == 0)
    /* This unlocks the underlying file, not the stream itself */
    kno_streamctl(&(fp->pool_stream),kno_stream_unlockfile,NULL);
  return 1;
}

/* Setting the cache level */

#if KNO_USE_MMAP
static int update_offdata_cache(kno_bigpool bp,int level,int chunk_ref_size)
{
  unsigned int *offdata = bp->pool_offdata;

  if ( (level < 2) && (offdata) ) {
    /* Unmap the offsets cache */
    int retval;
    size_t offsets_size = bp->pool_offlen*chunk_ref_size;
    size_t header_size = 256+offsets_size;
    bp->pool_offdata = NULL;
    bp->pool_offlen  = 0;
    /* The address we pass to munmap is offdata-64 (not the 256 we
       passed to mmap originally) because bp->pool_offdata is an
       (unsigned int *) but the size is in bytes. */
    retval = munmap(offdata-64,header_size);
    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),
              "bigpool_setcache:munmap %s",bp->poolid);
      U8_CLEAR_ERRNO();
      return 0;}
    else return 1;}

  if (level < 2 ) return 1;

  if ( (LOCK_POOLSTREAM(bp,"bigpool_setcache")) < 0) {
    u8_logf(LOG_WARN,"PoolStreamClosed",
            "During bigpool_setcache for %s",bp->poolid);
    UNLOCK_POOLSTREAM(bp);
    return -1;}

  /* Everything below here requires a file descriptor */

  if ( (level >= 2) && (bp->pool_offdata == NULL) ) {
    unsigned int *newmmap;
    /* Sizes here are in bytes */
    size_t offsets_size = (bp->pool_capacity)*chunk_ref_size;
    size_t header_size = 256+offsets_size;
    /* Map the offsets */
    newmmap=
      mmap(NULL,header_size,PROT_READ,MAP_SHARED|MAP_NORESERVE,
           bp->pool_stream.stream_fileno,0);
    if ((newmmap == NULL) || (newmmap == MAP_FAILED)) {
      u8_logf(LOG_WARN,u8_strerror(errno),
              "bigpool_setcache:mmap %s",bp->poolid);
      U8_CLEAR_ERRNO();
      UNLOCK_POOLSTREAM(bp);
      return 0;}
    else {
      bp->pool_offdata = newmmap+64;
      bp->pool_offlen = bp->pool_capacity;}}
  UNLOCK_POOLSTREAM(bp);
  return 1;
}
#else
static int update_offdata_cache(kno_bigpool bp,int level,int chunk_ref_size)
{
  unsigned int *offdata=bp->pool_offdata;
  if ( (level < 2) && (offdata) ) {
    bp->pool_offdata = NULL;
    u8_free(offdata);
    return 1;}
  else if ( (level >= 2) && (offdata == NULL) ) {
    kno_stream stream = &(bp->pool_stream);
    kno_inbuf  readbuf = kno_readbuf(s);
    if (LOCK_POOLSTREAM(bp)<0) {
      kno_clear_errors(1);
      return 0;}
    else {
      unsigned int load = bp->pool_load;
      kno_stream_start_read(s);
      kno_setpos(s,0x10);
      bp->pool_load = load = kno_read_4bytes(ins);
      size_t offdata_length = chunk_ref_size*load;
      offdata=u8_malloc(offdata_length);
      kno_setpos(s,0x100);
      kno_read_ints(readbuf,load,offdata);
      bp->pool_offdata = offsets;
      bp->pool_offlen = load;
      UNLOCK_POOLSTREAM(bp);
      return 1;}}
  else return 1;
}
#endif

static void bigpool_setcache(kno_bigpool p,int level)
{
  kno_bigpool bp = (kno_bigpool)p;
  int chunk_ref_size = get_chunk_ref_size(bp);
  if (chunk_ref_size<0) {
    u8_logf(LOG_WARN,CorruptBigpool,
            "Pool structure invalid: %s",p->poolid);
    return;}
  kno_stream stream = &(bp->pool_stream);
  size_t bufsize  = kno_stream_bufsize(stream);
  size_t use_bufsize = kno_getfixopt(bp->pool_opts,"BUFSIZE",kno_driver_bufsize);

  if (level < 0) level = kno_default_cache_level;

  /* Update the bufsize */
  if (bufsize < use_bufsize)
    kno_setbufsize(stream,use_bufsize);

  if ( ( (level<2) && (bp->pool_offdata == NULL) ) ||
       ( (level==2) && ( bp->pool_offdata != NULL ) ) ) {
    bp->pool_cache_level=level;
    return;}

  /* Check again, race condition */
  if ( ( (level<2) && (bp->pool_offdata == NULL) ) ||
       ( (level==2) && ( bp->pool_offdata != NULL ) ) ) {
    bp->pool_cache_level=level;
    return;}

  if (update_offdata_cache(bp,level,chunk_ref_size))
    bp->pool_cache_level=level;

}

/* Write values:
 * 0: just for reading, open up to the *load* of the pool
 * 1: for writing, open up to the capcity of the pool
 * -1: for reading, but sync before remapping
 */
static void reload_bigpool(kno_bigpool bp,int is_locked)
{
  if (!(is_locked)) use_bigpool(bp);
  int err=0;
  kno_stream stream = &(bp->pool_stream);
  time_t mtime = (time_t) kno_read_8bytes_at(stream,0x12,KNO_ISLOCKED,&err);
  if (!(is_locked)) bigpool_finished(bp);
  if ((err==0) && (mtime == bp->pool_mtime))
    return;
  else if (!(is_locked))
    kno_lock_pool_struct((kno_pool)bp,1);
  else {}
  long long new_load = kno_read_4bytes_at(stream,0x10,KNO_ISLOCKED);
  unsigned int *offdata = bp->pool_offdata;
  if (offdata==NULL) {
    bp->pool_load=new_load;
    kno_reset_hashtable(&(bp->pool_cache),-1,1);
    kno_reset_hashtable(&(bp->pool_changes),32,1);
    if (!(is_locked)) bigpool_finished(bp);
    return;}
  /* Make it NULL while we're messing with it */
  else bp->pool_offdata=NULL;
  double start = u8_elapsed_time();
#if KNO_USE_MMAP
  /* When we have MMAP, the offlen is always the whole cache */
#else
  kno_stream stream = &(bp->pool_stream);
  kno_inbuf  readbuf = kno_readbuf(stream);
  size_t new_size = chunkref_size*new_load;
  if (new_load != cur_load)
    offdata = u8_realloc(offdata,new_size);
  kno_setpos(s,0x100);
  kno_read_ints(ins,new_load,new_offdata);
  bp->pool_offdata = offdata;
  bp->pool_offlen = new_load;
#endif
  update_filetime(bp);
  bp->pool_load = new_load;
  kno_reset_hashtable(&(bp->pool_cache),-1,1);
  kno_reset_hashtable(&(bp->pool_changes),32,1);
  if (!(is_locked)) kno_unlock_pool_struct((kno_pool)bp);
  u8_logf(LOG_DEBUG,"ReloadOffsets",
          "Offsets for %s reloaded in %f secs",
          bp->poolid,u8_elapsed_time()-start);
}

static void bigpool_close(kno_pool p)
{
  kno_bigpool bp = (kno_bigpool)p;
  kno_lock_pool_struct(p,1);
  /* Close the stream */
  kno_close_stream(&(bp->pool_stream),0);
  if (bp->pool_offdata) {
    unsigned int *offdata=bp->pool_offdata;
    bp->pool_offdata = NULL;
    /* TODO: Be more careful about freeing/unmapping the
       offdata. Users might get a seg fault rather than a "file not
       open error". */
#if KNO_USE_MMAP
    size_t offdata_length = 256+((bp->pool_capacity)*get_chunk_ref_size(bp));
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval = munmap(offdata-64,offdata_length);
    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),
              "bigpool_close:munmap offsets %s",bp->poolid);
      errno = 0;}
#else
    u8_free(bp->pool_offdata);
#endif
  }
  kno_unlock_pool_struct(p);
}

static void bigpool_setbuf(kno_pool p,ssize_t bufsize)
{
  kno_bigpool bp = (kno_bigpool)p;
  if ( (bp->pool_stream.stream_flags) & (KNO_STREAM_MMAPPED) )
    return;
  kno_lock_pool_struct(p,1);
  kno_setbufsize(&(bp->pool_stream),(size_t)bufsize);
  kno_unlock_pool_struct(p);
}

static int bigpool_set_compression(kno_bigpool bp,kno_compress_type cmptype)
{
  struct KNO_STREAM _stream, *stream = kno_init_file_stream
    (&_stream,bp->pool_source,KNO_FILE_MODIFY,-1,-1);
  if (stream == NULL) return -1;
  unsigned int format = kno_read_4bytes_at(stream,KNO_BIGPOOL_FORMAT_POS,KNO_STREAM_ISLOCKED);
  format = format & (~(KNO_BIGPOOL_COMPRESSION));
  format = format | (cmptype<<3);
  ssize_t v = kno_write_4bytes_at(stream,format,KNO_BIGPOOL_FORMAT_POS);
  if (v>=0) {
    kno_lock_pool_struct((kno_pool)bp,1);
    bp->pool_compression = cmptype;
    kno_unlock_pool_struct((kno_pool)bp);}
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  if (v<0) return v;
  else return KNO_INT(cmptype);
}

static int bigpool_set_read_only(kno_bigpool bp,int read_only)
{
  struct KNO_STREAM _stream, *stream = kno_init_file_stream
    (&_stream,bp->pool_source,KNO_FILE_MODIFY,-1,-1);
  if (stream == NULL) return -1;
  unsigned int format = kno_read_4bytes_at(stream,KNO_BIGPOOL_FORMAT_POS,KNO_STREAM_ISLOCKED);
  if (read_only)
    format = format | (KNO_BIGPOOL_READ_ONLY);
  else format = format & (~(KNO_BIGPOOL_READ_ONLY));
  ssize_t v = kno_write_4bytes_at(stream,format,KNO_BIGPOOL_FORMAT_POS);
  if (v>=0) {
    kno_lock_pool_struct((kno_pool)bp,1);
    if (read_only)
      bp->pool_flags |=  KNO_STORAGE_READ_ONLY;
    else bp->pool_flags &=  (~(KNO_STORAGE_READ_ONLY));
    kno_unlock_pool_struct((kno_pool)bp);}
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  return v;
}

static lispval bigpool_getoids(kno_bigpool bp)
{
  if (bp->pool_cache_level<0) {
    kno_pool_setcache((kno_pool)bp,kno_default_cache_level);}
  lispval results = EMPTY;
  KNO_OID base = bp->pool_base;
  unsigned int i=0, load=bp->pool_load;
  kno_stream stream = &(bp->pool_stream);
  if (bp->pool_offdata) {
    unsigned int *offdata = bp->pool_offdata;
    unsigned int offlen = bp->pool_offlen;
    while (i<load) {
      struct KNO_CHUNK_REF ref=
        kno_get_chunk_ref(offdata,bp->pool_offtype,i,offlen);
      if (ref.off>0) {
        KNO_OID addr = KNO_OID_PLUS(base,i);
        KNO_ADD_TO_CHOICE(results,kno_make_oid(addr));}
      i++;}}
  else while (i<load) {
      struct KNO_CHUNK_REF ref=
        kno_fetch_chunk_ref(stream,256,bp->pool_offtype,i,0);
      if (ref.off>0) {
        KNO_OID addr = KNO_OID_PLUS(base,i);
        KNO_ADD_TO_CHOICE(results,kno_make_oid(addr));}
      i++;}
  return results;
}

/* Bigpool ops */

static lispval metadata_readonly_props = KNO_VOID;

static lispval bigpool_ctl(kno_pool p,lispval op,int n,lispval *args)
{
  struct KNO_BIGPOOL *bp = (struct KNO_BIGPOOL *)p;
  if ((n>0)&&(args == NULL))
    return kno_err("BadPoolOpCall","bigpool_op",bp->poolid,VOID);
  else if (n<0)
    return kno_err("BadPoolOpCall","bigpool_op",bp->poolid,VOID);
  else if (op == kno_cachelevel_op) {
    if (n==0)
      return KNO_INT(bp->pool_cache_level);
    else {
      lispval arg = (args)?(args[0]):(VOID);
      if ((FIXNUMP(arg))&&(FIX2INT(arg)>=0)&&
          (FIX2INT(arg)<0x100)) {
        kno_lock_pool_struct(p,1);
        bigpool_setcache(bp,FIX2INT(arg));
        kno_unlock_pool_struct(p);
        return KNO_INT(bp->pool_cache_level);}
      else return kno_type_error
             (_("cachelevel"),"bigpool_op/cachelevel",arg);}}
  else if (op == kno_reload_op) {
    reload_bigpool(bp,KNO_UNLOCKED);
    return KNO_TRUE;}
  else if (op == kno_bufsize_op) {
    if (n==0)
      return KNO_INT(bp->pool_stream.buf.raw.buflen);
    else if (FIXNUMP(args[0])) {
      bigpool_setbuf(p,FIX2INT(args[0]));
      return KNO_INT(bp->pool_stream.buf.raw.buflen);}
    else return kno_type_error("buffer size","bigpool_op/bufsize",args[0]);}
  else if (op == kno_slotids_op) {
    if (bp->pool_slotcodes.slotids)
      return kno_deep_copy((lispval)(bp->pool_slotcodes.slotids));
    else return kno_empty_vector(0);}
  else if (op == kno_label_op) {
    if (n == 0) {
      if (p->pool_label)
        return KNO_FALSE;
      else return lispval_string(p->pool_label);}
    else if ( (n==1) && ( (KNO_STRINGP(args[0])) || (KNO_FALSEP(args[0]))) ) {
      if ( (p->pool_flags)&(KNO_STORAGE_READ_ONLY) )
        return kno_err(kno_ReadOnlyPool,"bigpool_ctl/label",p->poolid,args[0]);
      else {
        int rv = (KNO_STRINGP(args[0])) ? (set_bigpool_label(bp,KNO_CSTRING(args[0]))) :
          (set_bigpool_label(bp,NULL));
        if (rv<0)
          return KNO_ERROR_VALUE;
        else return KNO_INT(rv);}}
    else return kno_err("BadArg","bigpool_ctl/label",p->poolid,args[0]);}
  else if (op == kno_capacity_op)
    return KNO_INT(bp->pool_capacity);
  else if ( (op == kno_metadata_op) && (n == 0) ) {
    lispval base=kno_pool_base_metadata(p);
    lispval slotids_vec =
      (bp->pool_slotcodes.slotids) ?
      kno_deep_copy((lispval)(bp->pool_slotcodes.slotids)) :
      kno_empty_vector(0);
    kno_store(base,load_symbol,KNO_INT(bp->pool_load));
    kno_store(base,slotids_symbol,KNO_INT(bp->pool_slotcodes.n_slotcodes));
    if ( bp->bigpool_format & KNO_BIGPOOL_READ_ONLY )
      kno_store(base,FDSYM_READONLY,KNO_TRUE);
    if ( bp->bigpool_format & KNO_BIGPOOL_READ_ONLY )
      kno_add(base,FDSYM_FORMAT,FDSYM_READONLY);
    if ( bp->bigpool_format & KNO_BIGPOOL_ADJUNCT )
      kno_add(base,FDSYM_FORMAT,FDSYM_ADJUNCT);
    if ( bp->bigpool_format & KNO_BIGPOOL_DTYPEV2 )
      kno_add(base,FDSYM_FORMAT,kno_intern("dtypev2"));
    if ( bp->bigpool_format & KNO_BIGPOOL_SPARSE )
      kno_add(base,FDSYM_FORMAT,kno_intern("sparse"));
    if ( bp->pool_offtype == KNO_B32) {
      kno_store(base,offmode_symbol,kno_intern("b32"));
      kno_add(base,FDSYM_FORMAT,kno_intern("b32"));}
    else if ( bp->pool_offtype == KNO_B40) {
      kno_store(base,offmode_symbol,kno_intern("b40"));
      kno_add(base,FDSYM_FORMAT,kno_intern("b40"));}
    else if ( bp->pool_offtype == KNO_B64) {
      kno_store(base,offmode_symbol,kno_intern("b64"));
      kno_add(base,FDSYM_FORMAT,kno_intern("b64"));}
    else kno_store(base,offmode_symbol,kno_intern("!!invalid!!"));
    if ( bp->pool_compression == KNO_NOCOMPRESS ) {
      kno_store(base,compression_symbol,KNO_FALSE);
      kno_add(base,FDSYM_FORMAT,kno_intern("nocompress"));}
    else if ( bp->pool_compression == KNO_ZLIB ) {
      kno_store(base,compression_symbol,kno_intern("zlib"));
      kno_add(base,FDSYM_FORMAT,kno_intern("zlib"));}
    else if ( bp->pool_compression == KNO_ZLIB9 ) {
      kno_store(base,compression_symbol,kno_intern("zlib9"));
      kno_add(base,FDSYM_FORMAT,kno_intern("zlib9"));}
    else if ( bp->pool_compression == KNO_SNAPPY ) {
      kno_store(base,compression_symbol,kno_intern("snappy"));
      kno_add(base,FDSYM_FORMAT,kno_intern("snappy"));}
    else if ( bp->pool_compression == KNO_ZSTD ) {
      kno_store(base,compression_symbol,kno_intern("zstd"));
      kno_add(base,FDSYM_FORMAT,kno_intern("zstd"));}
    else kno_store(base,compression_symbol,kno_intern("!!invalid!!"));
    kno_add(base,metadata_readonly_props,load_symbol);
    kno_add(base,metadata_readonly_props,slotids_symbol);
    kno_add(base,metadata_readonly_props,compression_symbol);
    kno_add(base,metadata_readonly_props,offmode_symbol);
    kno_decref(slotids_vec);
    return base;}
  else if ( (op == kno_load_op) && (n == 0) )
    return KNO_INT(bp->pool_load);
  else if ( ( ( op == compression_symbol ) && (n == 0) ) ||
            ( ( op == kno_metadata_op ) && (n == 1) &&
              ( args[0] == compression_symbol ) ) ) {
    if ( bp->pool_compression == KNO_NOCOMPRESS )
      return KNO_FALSE;
    else if ( bp->pool_compression == KNO_ZLIB )
      return kno_intern("zlib");
    else if ( bp->pool_compression == KNO_ZLIB9 )
      return kno_intern("zlib9");
    else if ( bp->pool_compression == KNO_SNAPPY )
      return kno_intern("snappy");
    else if ( bp->pool_compression == KNO_ZSTD )
      return kno_intern("zstd");
    else {
      kno_seterr("BadCompressionType","bigpool_ctl",bp->poolid,KNO_VOID);
      return KNO_ERROR;}}
  else if ( ( ( op == FDSYM_READONLY ) && (n == 0) ) ||
            ( ( op == kno_metadata_op ) && (n == 1) &&
              ( args[0] == FDSYM_READONLY ) ) ) {
    if ( (bp->pool_flags) & (KNO_STORAGE_READ_ONLY) )
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if ( ( ( op == FDSYM_READONLY ) && (n == 1) ) ||
            ( ( op == kno_metadata_op ) && (n == 2) &&
              ( args[0] == FDSYM_READONLY ) ) ) {
    lispval arg = ( op == FDSYM_READONLY ) ? (args[0]) : (args[1]);
    int rv = (KNO_FALSEP(arg)) ? (bigpool_set_read_only(bp,0)) :
      (bigpool_set_read_only(bp,1));
    if (rv<0)
      return KNO_ERROR;
    else return kno_incref(arg);}
  else if ( ( ( op == compression_symbol ) && (n == 1) ) ||
            ( ( op == kno_metadata_op ) && (n == 2) &&
              ( args[0] == compression_symbol ) ) ) {
    lispval arg = (op == compression_symbol) ? (args[0]) : (args[1]);
    int rv = bigpool_set_compression(bp,kno_compression_type(arg,KNO_NOCOMPRESS));
    if (rv<0) return KNO_ERROR;
    else return KNO_TRUE;}
  else if (op == kno_load_op) {
    lispval loadval = args[0];
    if (KNO_UINTP(loadval)) {
      lispval sessionid = lispval_string(u8_sessionid());
      lispval timestamp = kno_make_timestamp(NULL);
      lispval record = kno_make_nvector(3,kno_incref(args[0]),timestamp,sessionid);
      kno_store(((lispval)(&(p->pool_metadata))),
               kno_intern("load_changed"),record);
      bp->pool_load = KNO_FIX2INT(loadval);
      return record;}
    else return kno_type_error("pool load","bigpool_ctl",args[0]);}
  else if (op == kno_keys_op) {
    lispval keys = bigpool_getoids(bp);
    return kno_simplify_choice(keys);}
  else return kno_default_poolctl(p,op,n,args);
}

/* Creating bigpool */

static unsigned int get_bigpool_format(kno_storage_flags sflags,lispval opts)
{
  unsigned int flags = 0;
  lispval offtype = kno_intern("offtype");
  if ( kno_testopt(opts,offtype,kno_intern("b64"))  ||
       kno_testopt(opts,offtype,KNO_INT(64))        ||
       kno_testopt(opts,FDSYM_FLAGS,kno_intern("b64")))
    flags |= KNO_B64;
  else if ( kno_testopt(opts,offtype,kno_intern("b40"))  ||
            kno_testopt(opts,offtype,KNO_INT(40))        ||
            kno_testopt(opts,FDSYM_FLAGS,kno_intern("b40")) )
    flags |= KNO_B40;
  else if ( kno_testopt(opts,offtype,kno_intern("b32"))  ||
            kno_testopt(opts,offtype,KNO_INT(32))        ||
            kno_testopt(opts,FDSYM_FLAGS,kno_intern("b40")) )
    flags |= KNO_B32;
  else flags |= KNO_B40;

  flags |= ((kno_compression_type(opts,KNO_NOCOMPRESS))<<3);

  if ( kno_testopt(opts,kno_intern("dtypev2"),VOID) ||
       kno_testopt(opts,FDSYM_FLAGS,kno_intern("dtypev2")) ||
       kno_testopt(opts,FDSYM_FORMAT,kno_intern("dtypev2")) )
    flags |= KNO_BIGPOOL_DTYPEV2;

  if ( (kno_testopt(opts,FDSYM_READONLY,VOID)) )
    flags |= KNO_BIGPOOL_READ_ONLY;

  if ( (kno_testopt(opts,FDSYM_ISADJUNCT,VOID)) ||
       (kno_testopt(opts,FDSYM_FLAGS,FDSYM_ISADJUNCT)) ||
       (kno_testopt(opts,FDSYM_FORMAT,FDSYM_ISADJUNCT)) ||
       (kno_testopt(opts,FDSYM_FORMAT,FDSYM_ADJUNCT)) )
    flags |= KNO_BIGPOOL_ADJUNCT;

  if ( ( (sflags) & (KNO_POOL_SPARSE) ) ||
       (kno_testopt(opts,kno_intern("sparse"),VOID)) )
    flags |= KNO_BIGPOOL_SPARSE;

  return flags;
}

static kno_pool bigpool_create(u8_string spec,void *type_data,
                              kno_storage_flags storage_flags,
                              lispval opts)
{
  lispval base_oid = kno_getopt(opts,kno_intern("base"),VOID);
  lispval capacity_arg = kno_getopt(opts,kno_intern("capacity"),VOID);
  lispval load_arg = kno_getopt(opts,kno_intern("load"),KNO_FIXZERO);
  lispval label = kno_getopt(opts,FDSYM_LABEL,VOID);
  lispval slotcodes = kno_getopt(opts,kno_intern("slotids"),VOID);
  lispval metadata_init = kno_getopt(opts,kno_intern("metadata"),VOID);
  lispval ctime_opt = kno_getopt(opts,kno_intern("ctime"),KNO_VOID);
  lispval mtime_opt = kno_getopt(opts,kno_intern("mtime"),KNO_VOID);
  lispval generation_opt = kno_getopt(opts,kno_intern("generation"),KNO_VOID);
  time_t now=time(NULL), ctime, mtime;
  long long generation=1;
  unsigned int flags = get_bigpool_format(storage_flags,opts);
  unsigned int capacity, load;
  int rv = 0;
  if (u8_file_existsp(spec)) {
    kno_seterr(_("FileAlreadyExists"),"bigpool_create",spec,VOID);
    return NULL;}
  else if (!(OIDP(base_oid))) {
    kno_seterr("Not a base oid","bigpool_create",spec,base_oid);
    rv = -1;}
  else if (KNO_ISINT(capacity_arg)) {
    int capval = kno_getint(capacity_arg);
    if (capval<=0) {
      kno_seterr("Not a valid capacity","bigpool_create",
                spec,capacity_arg);
      rv = -1;}
    else capacity = capval;}
  else {
    kno_seterr("Not a valid capacity","bigpool_create",
              spec,capacity_arg);
    rv = -1;}
  if (rv<0) {}
  else if (KNO_ISINT(load_arg)) {
    int loadval = kno_getint(load_arg);
    if (loadval<0) {
      kno_seterr("Not a valid load","bigpool_create",spec,load_arg);
      rv = -1;}
    else if (loadval > capacity) {
      kno_seterr(kno_PoolOverflow,"bigpool_create",spec,load_arg);
      rv = -1;}
    else load = loadval;}
  else if ( (FALSEP(load_arg)) || (EMPTYP(load_arg)) ||
            (VOIDP(load_arg)) || (load_arg == KNO_DEFAULT_VALUE))
    load=0;
  else {
    kno_seterr("Not a valid load","bigpool_create",spec,load_arg);
    rv = -1;}

  if (KNO_FIXNUMP(ctime_opt))
    ctime = (time_t) KNO_FIX2INT(ctime_opt);
  else if (KNO_PRIM_TYPEP(ctime_opt,kno_timestamp_type)) {
    struct KNO_TIMESTAMP *moment = (kno_timestamp) ctime_opt;
    ctime = moment->u8xtimeval.u8_tick;}
  else ctime=now;

  if (KNO_FIXNUMP(mtime_opt))
    mtime = (time_t) KNO_FIX2INT(mtime_opt);
  else if (KNO_PRIM_TYPEP(ctime_opt,kno_timestamp_type)) {
    struct KNO_TIMESTAMP *moment = (kno_timestamp) mtime_opt;
    mtime = moment->u8xtimeval.u8_tick;}
  else mtime=now;

  if (KNO_FIXNUMP(generation_opt))
    generation=KNO_FIX2INT(generation_opt)+1;
  else generation=1;

  lispval metadata = VOID;
  lispval created_symbol = kno_intern("created");
  lispval packed_symbol = kno_intern("packed");
  lispval init_opts = kno_intern("initopts");
  lispval make_opts = kno_intern("makeopts");

  if (KNO_TABLEP(metadata_init))
    metadata = kno_deep_copy(metadata_init);
  else metadata = kno_make_slotmap(8,0,NULL);

  lispval ltime = kno_time2timestamp(now);
  if (!(kno_test(metadata,created_symbol,KNO_VOID)))
    kno_store(metadata,created_symbol,ltime);
  kno_store(metadata,packed_symbol,ltime);
  kno_decref(ltime); ltime = KNO_VOID;

  if (!(kno_test(metadata,init_opts,KNO_VOID)))
    kno_store(metadata,init_opts,opts);
  kno_store(metadata,make_opts,opts);

  if (rv<0) return NULL;
  else rv = make_bigpool(spec,
                         ((STRINGP(label)) ? (CSTRING(label)) : (spec)),
                         KNO_OID_ADDR(base_oid),capacity,load,flags,
                         metadata,slotcodes,
                         ctime,mtime,generation);
  kno_decref(base_oid);
  kno_decref(capacity_arg);
  kno_decref(load_arg);
  kno_decref(label);
  kno_decref(slotcodes);
  kno_decref(metadata);
  kno_decref(ctime_opt);
  kno_decref(mtime_opt);
  kno_decref(generation_opt);

  if (rv>=0) {
    kno_set_file_opts(spec,opts);
    return kno_open_pool(spec,storage_flags,opts);}
  else return NULL;
}

/* Initializing the driver module */

static struct KNO_POOL_HANDLER bigpool_handler={
  "bigpool", 1, sizeof(struct KNO_BIGPOOL), 12,
  bigpool_close, /* close */
  bigpool_alloc, /* alloc */
  bigpool_fetch, /* fetch */
  bigpool_fetchn, /* fetchn */
  bigpool_load, /* getload */
  bigpool_lock, /* lock */
  bigpool_unlock, /* release */
  bigpool_commit, /* commit */
  NULL, /* swapout */
  bigpool_create, /* create */
  NULL,  /* walk */
  NULL,  /* recycle */
  bigpool_ctl  /* poolctl */
};


KNO_EXPORT void kno_init_bigpool_c()
{
  u8_register_source_file(_FILEINFO);

  kno_register_pool_type
    ("bigpool",
     &bigpool_handler,
     open_bigpool,
     kno_match_pool_file,
     (void*)U8_INT2PTR(KNO_BIGPOOL_MAGIC_NUMBER));

  kno_register_pool_type
    ("corrupted bigpool",
     &bigpool_handler,
     recover_bigpool,
     kno_match_pool_file,
     (void*)U8_INT2PTR(KNO_BIGPOOL_TO_RECOVER));

  load_symbol=kno_intern("load");
  slotids_symbol=kno_intern("slotids");
  compression_symbol=kno_intern("compression");
  offmode_symbol=kno_intern("offmode");
  metadata_readonly_props = kno_intern("_readonly_props");

  kno_register_config("BIGPOOL:LOGLEVEL",
                     "The default loglevel for bigpools",
                     kno_intconfig_get,kno_loglevelconfig_set,
                     &bigpool_loglevel);

  kno_set_default_pool_type("bigpool");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
