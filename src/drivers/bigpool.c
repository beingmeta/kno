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
#include "framerd/numbers.h"
#include "framerd/fddb.h"
#include "framerd/stream.h"
#include "framerd/fddb.h"
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
    u8_seterr("PoolStreamClosed",caller,u8_strdup(bp->pool_idstring));
    fd_unlock_stream(&(bp->pool_stream));
    return -1;}
  else return 1;
}

#define UNLOCK_POOLSTREAM(op) fd_unlock_stream(&(op->pool_stream))

static void update_modtime(struct FD_BIGPOOL *fp);
static void reload_offdata(struct FD_BIGPOOL *fp,int lock,int write);
/*static int recover_bigpool(struct FD_BIGPOOL *); */

static struct FD_POOL_HANDLER bigpool_handler;

static fd_exception InvalidOffset=_("Invalid offset in BIGPOOL");

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
        XXXX
   0x0c XXXX     Capacity of pool
   0x10 XXXX     Load of pool
   0x14 XXXX     Pool information bits/flags
   0x18 XXXX     file offset of the pool label (a dtype) (8 bytes)
        XXXX
   0x20 XXXX     byte length of label dtype (4 bytes)
   0x24 XXXX     file offset of pool metadata (a dtype) (8 bytes)
        XXXX
   0x2c XXXX     byte length of pool metadata representation (4 bytes)
   0x30 XXXX     pool creation time_t (8 bytes)
        XXXX
   0x38 XXXX     pool repack time_t (8 bytes)
        XXXX
   0x40 XXXX     pool modification time_t (8 bytes)
        XXXX
   0x48 XXXX     repack generation (8 bytes)
        XXXX
   0x50 XXXX     number of registered slotids
   0x54 XXXX     file offset of the slotids block
        XXXX
   0x5c XXXX     size of slotids dtype representation

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
                    1= libz compression
                    2= libbz2 compression -- Not yet implemented
                    3-7 reserved for future use
     0x0020      Set if this pool is intended to be read-only

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

static FD_CHUNK_REF read_chunk_ref(fd_stream stream,
                                   fd_off_t base,fd_offset_type offtype,
                                   unsigned int offset);

static int get_chunk_ref_size(fd_bigpool p)
{
  switch (p->pool_offtype) {
  case FD_B32: case FD_B40: return 8;
  case FD_B64: return 12;}
  return -1;
}

static size_t get_maxpos(fd_bigpool p)
{
  switch (p->pool_offtype) {
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

static fd_pool open_bigpool(u8_string fname,fddb_flags flags)
{
  FD_OID base=FD_NULL_OID_INIT;
  unsigned int hi, lo, magicno, capacity, load, n_slotids;
  fd_off_t label_loc, slotids_loc; fdtype label;
  struct FD_BIGPOOL *pool=u8_alloc(struct FD_BIGPOOL);
  int read_only=U8_BITP(flags,FDB_READ_ONLY);
  fd_stream_mode mode=
    ((read_only) ? (FD_STREAM_READ) : (FD_STREAM_MODIFY));
  u8_string rname=u8_realpath(fname,NULL);
  struct FD_STREAM *stream=
    fd_init_file_stream(&(pool->pool_stream),fname,mode,fd_driver_bufsize);
  struct FD_INBUF *instream=fd_readbuf(stream);

  /* See if it ended up read only */
  if ((stream->stream_flags)&(FD_STREAM_READ_ONLY)) read_only=1;
  stream->stream_flags&=~FD_STREAM_IS_MALLOCD;
  magicno=fd_read_4bytes(instream);
  /* Read POOL base etc. */
  hi=fd_read_4bytes(instream); lo=fd_read_4bytes(instream);
  FD_SET_OID_HI(base,hi); FD_SET_OID_LO(base,lo);
  pool->pool_capacity=capacity=fd_read_4bytes(instream);
  pool->pool_load=load=fd_read_4bytes(instream);
  flags=fd_read_4bytes(instream);
  pool->pool_xformat=flags;
  if (U8_BITP(flags,FDB_READ_ONLY)) {
    /* If the pool is intrinsically read-only make it so. */
    fd_unlock_stream(stream);
    fd_close_stream(stream,0);
    fd_init_file_stream(stream,fname,FD_STREAM_READ,fd_driver_bufsize);
    fd_lock_stream(stream);
    fd_setpos(stream,FD_BIGPOOL_LABEL_POS);}
  pool->pool_offtype=(fd_offset_type)((flags)&(FD_BIGPOOL_OFFMODE));
  pool->pool_compression=
    (fd_compression_type)(((flags)&(FD_BIGPOOL_COMPRESSION))>>3);
  fd_init_pool((fd_pool)pool,base,capacity,&bigpool_handler,fname,rname);
  u8_free(rname); /* Done with this */
  /*
  if (magicno==FD_BIGPOOL_TO_RECOVER) {
    u8_log(LOG_WARN,fd_RecoveryRequired,"Recovering the file pool %s",fname);
    if (recover_bigpool(pool)<0) {
      fd_seterr(fd_MallocFailed,"open_bigpool",NULL,FD_VOID);
      return NULL;}}
  */
  /* Get the label */
  label_loc=fd_read_8bytes(instream);
  /* label_size=*/ fd_read_4bytes(instream);
  /* Skip the metadata field */
  fd_read_8bytes(instream);
  /* metadata size */ fd_read_4bytes(instream);
  n_slotids=fd_read_4bytes(instream);
  /* Read and initialize the slotids_loc */
  slotids_loc=fd_read_8bytes(instream);
  fd_read_4bytes(instream); /* Ignore size */
  if (label_loc) {
    if (fd_setpos(stream,label_loc)>0) {
      label=fd_read_dtype(instream);
      if (FD_STRINGP(label)) pool->pool_label=u8_strdup(FD_STRDATA(label));
      else u8_log(LOG_WARN,fd_BadFilePoolLabel,fd_dtype2string(label));
      fd_decref(label);}
    else {
      fd_seterr(fd_BadFilePoolLabel,"open_bigpool",
                u8_strdup("bad label loc"),
                FD_INT(label_loc));
      fd_close_stream(stream,0);
      u8_free(rname); u8_free(pool);
      return NULL;}}
  if ((n_slotids)&&(slotids_loc)) {
    int slotids_length=(n_slotids>256)?(n_slotids*2):(256);
    fdtype *slotids=u8_zalloc_n(slotids_length,fdtype);
    struct FD_HASHTABLE *slotcodes=&(pool->slotcodes);
    int i=0;
    fd_init_hashtable(slotcodes,n_slotids,NULL);
    fd_setpos(stream,slotids_loc);
    while (i<n_slotids) {
      fdtype slotid=fd_read_dtype(instream);
      slotids[i]=slotid;
      fd_hashtable_store(slotcodes,slotid,FD_INT(i));
      i++;}
    pool->slotids=slotids;
    pool->n_slotids=n_slotids;
    pool->slotids_length=slotids_length;}
  else {
    pool->slotids=u8_alloc_n(256,fdtype);
    pool->n_slotids=0; pool->slotids_length=256;
    fd_init_hashtable(&(pool->slotcodes),256,NULL);}
  /* Offsets size is the malloc'd size (in unsigned ints) of the
     offsets.  We don't fill this in until we actually need it. */
  pool->pool_offdata=NULL; pool->pool_offdata_length=0;
  if (read_only)
    U8_SETBITS(pool->pool_flags,FDB_READ_ONLY);
  else U8_CLEARBITS(pool->pool_flags,FDB_READ_ONLY);
  pool->pool_mmap=NULL; pool->pool_mmap_size=0;
  fd_init_mutex(&(pool->file_lock));
  if (!(U8_BITP(pool->pool_flags,FDB_UNREGISTERED)))
    fd_register_pool((fd_pool)pool);
  update_modtime(pool);
  return (fd_pool)pool;
}

static void update_modtime(struct FD_BIGPOOL *fp)
{
  struct stat fileinfo;
  if ((fstat(fp->pool_stream.stream_fileno,&fileinfo))<0)
    fp->pool_modtime=(time_t)-1;
  else fp->pool_modtime=fileinfo.st_mtime;
}

/* Getting slotids */

FD_FASTOP int probe_slotcode(struct FD_BIGPOOL *bp,fdtype slotid)
{
  fdtype codeval=fd_hashtable_get(&(bp->slotcodes),slotid,FD_VOID);
  if (FD_VOIDP(codeval)) return -1;
  else if (FD_FIXNUMP(codeval)) return FD_INT(codeval);
  else {
    u8_log(LOGWARN,_("CorruptedBigPool"),"Bad slotid code map for %q",slotid);
    return -1;}
}

static int add_slotcode(struct FD_BIGPOOL *bp,fdtype slotid)
{
  struct FD_HASHTABLE *slotcodes=&(bp->slotcodes);
  u8_write_lock(&(slotcodes->table_rwlock)); {
    fdtype *slotids=bp->slotids;
    fdtype v=fd_hashtable_get_nolock(slotcodes,slotid,FD_VOID);
    int use_code=bp->n_slotids++;
    if (FD_FIXNUMP(v)) {
      u8_rw_unlock(&(slotcodes->table_rwlock));
      return FD_FIX2INT(v);}
    if (use_code>=bp->slotids_length) {
      fdtype *newslotids=u8_alloc_n(bp->slotids_length*2,fdtype);
      if (newslotids==NULL) {
        u8_rw_unlock(&(slotcodes->table_rwlock));
        /* This keeps lookup for trying again */
        fd_hashtable_store(slotcodes,slotid,FD_INT(-1));
        return -1;}
      memcpy(newslotids,slotids,sizeof(fdtype)*bp->slotids_length);
      if (bp->old_slotids) u8_free(bp->old_slotids);
      bp->old_slotids=slotids;
      bp->slotids=slotids=newslotids;
      bp->added_slotids++;}
    slotids[use_code]=slotid;
    fd_hashtable_op_nolock(slotcodes,fd_table_store,slotid,FD_INT(use_code));
    u8_rw_unlock(&(slotcodes->table_rwlock));
    return use_code;}
}

FD_FASTOP int get_slotcode(struct FD_BIGPOOL *bp,fdtype slotid)
{
  struct FD_HASHTABLE *slotcodes=&(bp->slotcodes);
  fdtype v=fd_hashtable_get(slotcodes,slotid,FD_VOID);
  if (FD_FIXNUMP(v)) return FD_FIX2INT(v);
  else if (FD_CONSP(slotid)) return -1;
  else return add_slotcode(bp,slotid);
}

/* Load reading and updates */

/* These assume that the pool itself is locked */
static int write_bigpool_load(fd_bigpool bp)
{
  if (FD_POOLFILE_LOCKEDP(bp)) {
    /* Update the load */
    long long load;
    fd_stream stream=&(bp->pool_stream);
    LOCK_POOLSTREAM(bp,"write_bigpool_load");
    load=fd_read_4bytes_at(stream,16);
    if (load<0) {
      UNLOCK_POOLSTREAM(bp);
      return -1;}
    else if (bp->pool_load>load) {
      int rv=fd_write_4bytes_at(stream,bp->pool_load,16);
      UNLOCK_POOLSTREAM(bp);
      if (rv<0) return rv;
      else return rv;}
    else {
      UNLOCK_POOLSTREAM(bp);
      return 0;}}
  else return 0;
}

static int read_bigpool_load(fd_bigpool bp)
{
  long long load;
  fd_stream stream=&(bp->pool_stream);
  if (POOLFILE_LOCKEDP(bp)) {
    return bp->pool_load;}
  else {LOCK_POOLSTREAM(bp,"read_bigpool_load");}
  if (fd_lockfile(stream)<0) return -1;
  load=fd_read_4bytes_at(stream,16);
  if (load<0) {
    fd_unlockfile(stream);
    UNLOCK_POOLSTREAM(bp);
    return -1;}
  fd_unlockfile(stream);
  UNLOCK_POOLSTREAM(bp);
  bp->pool_load=load;
  fd_unlock_pool(bp);
  return load;
}

/* These assume that the pool itself is locked */
static int write_bigpool_slotids(fd_bigpool bp)
{
  if (bp->added_slotids) {
    if (FD_POOLFILE_LOCKEDP(bp)) {
      fdtype *slotids=bp->slotids;
      unsigned int n_slotids=bp->n_slotids;
      fd_stream stream=&(bp->pool_stream);
      off_t start_pos=fd_endpos(stream), end_pos=start_pos;
      fd_outbuf out=fd_writebuf(stream);
      int i=0, lim=bp->n_slotids; while (i<lim) {
        fdtype slotid=slotids[i++];
        ssize_t size=fd_write_dtype(out,slotid);
        if (size<0) return -1;
        else end_pos+=size;}
      fd_write_4bytes_at(stream,n_slotids,0x50);
      fd_write_8bytes_at(stream,start_pos,0x54);
      fd_write_4bytes_at(stream,end_pos-start_pos,0x5c);
      return 1;}
    else return 0;}
  else return 0;
}

/* Lock the underlying Bigpool */

static int lock_bigpool_file(struct FD_BIGPOOL *bp,int use_mutex)
{
  if (FD_POOLFILE_LOCKEDP(bp)) return 1;
  else if ((bp->pool_stream.stream_flags)&(FD_STREAM_READ_ONLY))
    return 0;
  else {
    struct FD_STREAM *s=&(bp->pool_stream);
    struct stat fileinfo;
    if (use_mutex) fd_lock_pool(bp);
    if (FD_POOLFILE_LOCKEDP(bp)) {
      /* Another thread got here first */
      if (use_mutex) fd_unlock_pool(bp);
      return 1;}
    LOCK_POOLSTREAM(bp,"lock_bigpool_file");
    if (fd_lockfile(s)==0) {
      fd_unlock_stream(s);
      if (use_mutex) fd_unlock_pool(bp);
      UNLOCK_POOLSTREAM(bp);
      return 0;}
    fstat( s->stream_fileno, &fileinfo);
    if ( fileinfo.st_mtime > bp->pool_modtime ) {
      /* Make sure we're up to date. */
      read_bigpool_load(bp);
      if (bp->pool_offdata) reload_offdata(bp,0,0);
      else {
        fd_reset_hashtable(&(bp->pool_cache),-1,1);
        fd_reset_hashtable(&(bp->pool_changes),32,1);}}
    UNLOCK_POOLSTREAM(bp);
    if (use_mutex) fd_unlock_pool(bp);
    return 1;}
}

/* BIGPOOL operations */

FD_EXPORT int fd_make_bigpool
  (u8_string fname,u8_string label,
   FD_OID base,unsigned int capacity,unsigned int load,
   unsigned int flags,fdtype slotids_init,
   time_t ctime,time_t mtime,int cycles)
{
  time_t now=time(NULL);
  fd_off_t slotids_pos=0, metadata_pos=0, label_pos=0;
  size_t slotids_size=0, metadata_size=0, label_size=0;
  struct FD_STREAM _stream, *stream=
    fd_init_file_stream(&_stream,fname,FD_STREAM_CREATE,
                        fd_driver_bufsize);
  fd_outbuf outstream=fd_writebuf(stream);
  fd_offset_type offtype=(fd_offset_type)((flags)&(FD_BIGPOOL_OFFMODE));
  if (stream==NULL) return -1;
  else if ((stream->stream_flags)&FD_STREAM_READ_ONLY) {
    fd_seterr3(fd_CantWrite,"fd_make_bigpool",u8_strdup(fname));
    fd_free_stream(stream);
    return -1;}

  u8_log(LOG_INFO,"CreateBigPool",
         "Creating a bigpool '%s' for %u OIDs based at %x/%x",
         fname,capacity,FD_OID_HI(base),FD_OID_LO(base));

  stream->stream_flags&=~FD_STREAM_IS_MALLOCD;
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
  if (ctime<0) ctime=now;
  fd_write_4bytes(outstream,0);
  fd_write_4bytes(outstream,((unsigned int)ctime));

  /* Write the index repack time */
  fd_write_4bytes(outstream,0);
  fd_write_4bytes(outstream,((unsigned int)now));

  /* Write the index modification time */
  if (mtime<0) mtime=now;
  fd_write_4bytes(outstream,0);
  fd_write_4bytes(outstream,((unsigned int)mtime));

  /* Write the number of repack cycles */
  if (mtime<0) mtime=now;
  fd_write_4bytes(outstream,0);
  fd_write_4bytes(outstream,cycles);

  /* Write number of slotids (if any) */
  if (FD_VECTORP(slotids_init))
    fd_write_4bytes(outstream,FD_VECTOR_LENGTH(slotids_init));
  else fd_write_4bytes(outstream,0);
  fd_write_8bytes(outstream,0); /* slotids offset (may be changed) */
  fd_write_4bytes(outstream,0); /* slotids dtype length (may be changed) */

  /* Fill the rest of the space. */
  {
    int i=0, bytes_to_write=256-fd_getpos(stream);
    while (i<bytes_to_write) {
      fd_write_byte(outstream,0); i++;}}

  /* Write the top level bucket table */
  {
    int i=0;
    if ((offtype==FD_B32) || (offtype==FD_B40))
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
    int len=strlen(label);
    label_pos=fd_getpos(stream);
    fd_write_byte(outstream,dt_string);
    fd_write_4bytes(outstream,len);
    fd_write_bytes(outstream,label,len);
    label_size=fd_getpos(stream)-label_pos;}

  /* Write the schemas */
  if (FD_VECTORP(slotids_init)) {
    int i=0, len=FD_VECTOR_LENGTH(slotids_init);
    slotids_pos=fd_getpos(stream);
    while (i<len) {
      fdtype slotid=FD_VECTOR_REF(slotids_init,i);
      fd_write_dtype(outstream,slotid);
      i++;}
    slotids_size=fd_getpos(stream)-slotids_pos;}

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
  fd_bigpool bp=(fd_bigpool)p;
  if (FD_POOLFILE_LOCKEDP(bp))
    /* If we have the file locked, the stored load is good. */
    return bp->pool_load;
  else {
    /* Otherwise, we need to read the load from the file */
    int load;
    fd_lock_pool(bp);
    load=read_bigpool_load(bp);
    fd_unlock_pool(bp);
    return load;}
}

static fdtype read_oid_value(fd_bigpool bp,
                             fd_inbuf in,
                             const u8_string cxt)
{
  int byte0=fd_probe_byte(in);
  if (byte0==0xF0) {
    /* Encoded slotmap/schemap */
    unsigned int n_slots= (fd_read_byte(in), fd_read_zint(in));
    fdtype sm=fd_init_slotmap(NULL,n_slots+1,NULL);
    struct FD_KEYVAL *kvals=FD_SLOTMAP_KEYVALS(sm);
    int i=0; while (i<n_slots) {
      int slot_byte0=fd_probe_byte(in);
      if (slot_byte0==0xE0) {
        long long slotcode= (fd_read_byte(in), fd_read_zint(in));
        if ((slotcode>=0)&&(slotcode<bp->n_slotids))
          kvals[i].kv_key=bp->slotids[slotcode];
        else {
          fd_seterr(_("BadSlotCode"),cxt,bp->pool_idstring,FD_VOID);
          fd_decref((fdtype)sm);
          return FD_ERROR_VALUE;}}
      else kvals[i].kv_key=fd_read_dtype(in);
      kvals[i].kv_val=fd_read_dtype(in);
      i++;}
    return sm;} /* close (byte0==0xF0) */
  /* Just a regular dtype */
  else return fd_read_dtype(in);
}

static fdtype read_oid_value_at(fd_bigpool bp,
                                FD_CHUNK_REF ref,
                                const u8_string cxt,
                                u8_mutex *unlock)
{
  if (ref.off==0) return FD_VOID;
  else if ( (bp->pool_compression==FD_NOCOMPRESS) && (bp->pool_mmap) ) {
      FD_INBUF in;
      if (unlock) u8_unlock_mutex(unlock);
      FD_INIT_BYTE_INPUT(&in,bp->pool_mmap+ref.off,ref.size);
      return read_oid_value(bp,&in,cxt);}
  else {
    unsigned char _buf[FD_BIGPOOL_FETCHBUF_SIZE], *buf;
    int free_buf=0;
    if (bp->pool_mmap) buf=bp->pool_mmap+ref.off;
    else if (ref.size>FD_BIGPOOL_FETCHBUF_SIZE) {
      buf=read_chunk(&(bp->pool_stream),bp->pool_mmap,
                     ref.off,ref.size,NULL,unlock);
      free_buf=1;}
    else buf=read_chunk(&(bp->pool_stream),bp->pool_mmap,
                        ref.off,ref.size,_buf,unlock);
    if (buf==NULL) return FD_ERROR_VALUE;
    else if (free_buf) {
      FD_INBUF in;
      FD_INIT_BYTE_INPUT(&in,buf,ref.size);
      fdtype result=read_oid_value(bp,&in,cxt);
      u8_free(buf);
      return result;}
    else {
      FD_INBUF in;
      FD_INIT_BYTE_INPUT(&in,buf,ref.size);
      return read_oid_value(bp,&in,cxt);}}
}

static fdtype bigpool_fetch(fd_pool p,fdtype oid)
{
  fd_bigpool bp=(fd_bigpool)p;
  FD_OID addr=FD_OID_ADDR(oid);
  int offset=FD_OID_DIFFERENCE(addr,bp->pool_base);
  if (FD_EXPECT_FALSE(offset>=bp->pool_load)) {
    /* Double check by going to disk */
    if (offset>=(bigpool_load(p)))
      return fd_err(fd_UnallocatedOID,"file_pool_fetch",bp->pool_idstring,oid);}
  if (bp->pool_offdata) {
    FD_CHUNK_REF ref=get_chunk_ref(&(bp->pool_stream),
                                   bp->pool_offdata,
                                   256,bp->pool_offtype,
                                   offset,STREAM_UNLOCKED);
    if (ref.off<0) return FD_ERROR_VALUE;
    else if (ref.off==0)
      return FD_EMPTY_CHOICE;
    else {
      fdtype value;
      fd_lock_stream(&(bp->pool_stream));
      value=read_oid_value_at(bp,ref,"bigpool_fetch",
                              &(bp->pool_stream.stream_lock));
      return value;}}
  else {
    fd_lock_stream(&(bp->pool_stream)); {
      FD_CHUNK_REF ref=read_chunk_ref(&(bp->pool_stream),
                                      256,bp->pool_offtype,
                                      offset);
      if (ref.off<=0) fd_unlock_stream(&(bp->pool_stream));
      if (ref.off<0)
        return FD_ERROR_VALUE;
      else if (ref.off==0)
        return FD_EMPTY_CHOICE;
      else {
        fdtype value;
        value=read_oid_value_at(bp,ref,"bigpool_fetch",
                                &(bp->pool_stream.stream_lock));
        return value;}}}
}
struct BIGPOOL_FETCH_SCHEDULE {
  unsigned int value_at; FD_CHUNK_REF location;};

static int compare_offsets(const void *x1,const void *x2)
{
  const struct BIGPOOL_FETCH_SCHEDULE *s1=x1, *s2=x2;
  if (s1->location.off<s2->location.off) return -1;
  else if (s1->location.off>s2->location.off) return 1;
  else return 0;
}

static fdtype *bigpool_fetchn(fd_pool p,int n,fdtype *oids)
{
  fd_bigpool bp=(fd_bigpool)p; FD_OID base=p->pool_base;
  fdtype *values=u8_alloc_n(n,fdtype);
  if (bp->pool_offdata==NULL) {
    /* Don't bother being clever if you don't even have an offsets
       table.  This could be fixed later for small memory implementations. */
    int i=0; while (i<n) {
      values[i]=bigpool_fetch(p,oids[i]); i++;}
    return values;}
  else {
    struct BIGPOOL_FETCH_SCHEDULE *schedule=
      u8_alloc_n(n,struct BIGPOOL_FETCH_SCHEDULE);
    fd_lock_stream(&(bp->pool_stream));
    int i=0;
    while (i<n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base);
      schedule[i].value_at=i;
      schedule[i].location=get_chunk_ref(NULL,bp->pool_offdata,
                                         256,bp->pool_offtype,
                                         off,STREAM_LOCKED);
      if (schedule[i].location.off<0) {
        fd_seterr(InvalidOffset,"bigpool_fetchn",p->pool_idstring,oid);
        u8_free(schedule); u8_free(values);
        fd_unlock_stream(&(bp->pool_stream));
        return NULL;}
      else i++;}
    /* Note that we sort even if we're mmaped in order to take
       advantage of page locality. */
    qsort(schedule,n,sizeof(struct BIGPOOL_FETCH_SCHEDULE),
          compare_offsets);
    i=0; while (i<n) {
      fdtype value;
#if ((BIGPOOL_PREFETCH_WINDOW)>0)
      if (bp->pool_mmap) {
        unsigned char *fd_vecelts=bp->pool_mmap;
        int j=i, lim=(((i+BIGPOOL_PREFETCH_WINDOW)>n) ? (n) : (i+4));
        while (j<lim) {
          FD_PREFETCH(&(fd_vecelts[schedule[j].location.off]));
          j++;}}
#endif
      value=read_oid_value_at(bp,schedule[i].location,"bigpool_fetchn",NULL);
      if (FD_ABORTP(value)) {
        int j=0; while (j<i) { fd_decref(values[j]); j++;}
        u8_free(schedule); u8_free(values);
        fd_push_error_context("bigpool_fetchn/read",oids[schedule[i].value_at]);
        fd_unlock_stream(&(bp->pool_stream));
        return NULL;}
      else values[schedule[i].value_at]=value;
      i++;}
    fd_unlock_stream(&(bp->pool_stream));
    u8_free(schedule);
    return values;}
}

struct BIGPOOL_SAVEINFO {
  FD_CHUNK_REF chunk; unsigned int oidoff;} BIGPOOL_SAVEINFO;

static int compare_oidoffs(const void *p1,const void *p2)
{
  struct BIGPOOL_SAVEINFO *si1=(struct BIGPOOL_SAVEINFO *)p1;
  struct BIGPOOL_SAVEINFO *si2=(struct BIGPOOL_SAVEINFO *)p2;
  if (si1->oidoff<si2->oidoff) return -1;
  else if (si1->oidoff>si2->oidoff) return 1;
  else return 0;
}

static int bigpool_write_value(fdtype value,fd_stream stream,
                               fd_bigpool p,struct FD_OUTBUF *tmpout,
                               unsigned char **zbuf,int *zbuf_size)
{
  fd_outbuf outstream=fd_writebuf(stream);
  /* Reset the tmpout stream */
  tmpout->bufwrite=tmpout->buffer;
  if (FD_SCHEMAPP(value)) {
    struct FD_SCHEMAP *sm=(fd_schemap)value;
    fdtype *schema=sm->table_schema;
    fdtype *values=sm->schema_values;
    int i=0, size=sm->schema_length;
    fd_write_byte(tmpout,0xF0);
    fd_write_zint(tmpout,size);
    while (i<size) {
      fdtype slotid=schema[i], value=values[i];
      int slotcode=get_slotcode(p,slotid);
      if (slotcode<0)
        fd_write_dtype(tmpout,slotid);
      else {
        fd_write_byte(tmpout,0xE0);
        fd_write_zint(tmpout,slotcode);}
      fd_write_dtype(tmpout,value);
      i++;}}
  else if (FD_SLOTMAPP(value)) {
    struct FD_SLOTMAP *sm=(fd_slotmap)value;
    struct FD_KEYVAL *keyvals=sm->sm_keyvals;
    int i=0, size=sm->n_slots;
    fd_write_byte(tmpout,0xF0);
    fd_write_zint(tmpout,size);
    while (i<size) {
      fdtype slotid=keyvals[i].kv_key;
      fdtype value=keyvals[i].kv_val;
      int slotcode=get_slotcode(p,slotid);
      if (slotcode<0)
        fd_write_dtype(tmpout,slotid);
      else {
        fd_write_byte(tmpout,0xE0);
        fd_write_zint(tmpout,slotcode);}
      fd_write_dtype(tmpout,value);
      i++;}}
  else fd_write_dtype(tmpout,value);
  fd_write_bytes(outstream,tmpout->buffer,tmpout->bufwrite-tmpout->buffer);
  return tmpout->bufwrite-tmpout->buffer;
}

static int update_offdata(struct FD_BIGPOOL *bp, fd_stream stream,
                          int n, struct BIGPOOL_SAVEINFO *saveinfo);

/*
static int write_recovery_info
  (struct FD_BIGPOOL *fp,fd_stream stream,
   int n,struct BIGPOOL_SAVEINFO *saveinfo,
   unsigned int load);
*/

static int bigpool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  fd_bigpool bp=(fd_bigpool)p;
  fdtype *slotids=bp->slotids;
  struct FD_STREAM *stream=&(bp->pool_stream);
  struct FD_OUTBUF *outstream=fd_writebuf(stream);
  if ((LOCK_POOLSTREAM(bp,"bigpool_storen"))<0) return -1;
  double started=u8_elapsed_time();
  u8_log(fddb_loglevel+1,"BigpoolStore",
         "Storing %d oid values in bigpool %s",n,p->pool_idstring);
  struct BIGPOOL_SAVEINFO *saveinfo=
    u8_alloc_n(n,struct BIGPOOL_SAVEINFO);
  struct FD_OUTBUF tmpout;
  unsigned char *zbuf=u8_malloc(FD_INIT_ZBUF_SIZE);
  unsigned int i=0, zbuf_size=FD_INIT_ZBUF_SIZE, n_slotids=bp->n_slotids;
  unsigned int init_buflen=2048*n;
  fd_off_t endpos, recovery_pos, slotids_pos=0;
  size_t maxpos=get_maxpos(bp), slotids_size;
  FD_OID base=bp->pool_base;
  if (init_buflen>262144) init_buflen=262144;
  FD_INIT_BYTE_OUTBUF(&tmpout,init_buflen);
  endpos=fd_endpos(stream);
  if ((bp->pool_xformat)&(FD_BIGPOOL_DTYPEV2))
    tmpout.buf_flags=tmpout.buf_flags|FD_USE_DTYPEV2;
  while (i<n) {
    FD_OID addr=FD_OID_ADDR(oids[i]);
    fdtype value=values[i];
    int n_bytes=bigpool_write_value(value,stream,bp,&tmpout,&zbuf,&zbuf_size);
    if (n_bytes<0) {
      u8_free(zbuf);
      u8_free(saveinfo);
      u8_free(tmpout.buffer);
      UNLOCK_POOLSTREAM(bp);
      return n_bytes;}
    if ((endpos+n_bytes)>=maxpos) {
      u8_free(zbuf); u8_free(saveinfo); u8_free(tmpout.buffer);
      u8_seterr(fd_DataFileOverflow,"bigpool_storen",
                u8_strdup(p->pool_idstring));
      UNLOCK_POOLSTREAM(bp);
      return -1;}

    saveinfo[i].chunk.off=endpos; saveinfo[i].chunk.size=n_bytes;
    saveinfo[i].oidoff=FD_OID_DIFFERENCE(addr,base);

    endpos=endpos+n_bytes;
    i++;}
  u8_free(tmpout.buffer);
  u8_free(zbuf);

  update_offdata(bp,stream,n,saveinfo);
  write_bigpool_load(bp);
  write_bigpool_slotids(bp);

  u8_free(saveinfo);
  fd_setpos(stream,0);
  fd_write_4bytes(outstream,FD_BIGPOOL_MAGIC_NUMBER);
  fd_flush_stream(stream);
  fsync(stream->stream_fileno);
  u8_log(fddb_loglevel,"BigpoolStore",
         "Stored %d oid values in bigpool %s in %f seconds",
         n,p->pool_idstring,u8_elapsed_time()-started);
  UNLOCK_POOLSTREAM(bp);
  fd_unlock_pool(bp);
  return n;
}

/*
static fd_off_t write_recovery_info
  (struct FD_BIGPOOL *fp,fd_stream stream,
   int n,struct BIGPOOL_SAVEINFO *saveinfo,
   unsigned int load,unsigned int n_slotids,
   fd_off_t slotids_loc,size_t slotids_size)
{
  fd_off_t recovery_pos=endpos;
  qsort(saveinfo,n,sizeof(struct BIGPOOL_SAVEINFO),compare_oidoffs);
  fd_write_4bytes(outstream,load);
  fd_write_4bytes(outstream,n);
  i=0; while (i<n) {
    fd_write_4bytes(outstream,saveinfo[i].oidoff);
    fd_write_8bytes(outstream,saveinfo[i].chunk.off);
    fd_write_4bytes(outstream,saveinfo[i].chunk.size);
    endpos=endpos+16; i++;}
  fd_write_8bytes(outstream,recovery_pos);
  fd_setpos(stream,0);
  fd_write_4bytes(outstream,FD_BIGPOOL_TO_RECOVER);
  fd_flush_stream(stream);
  fsync(stream->stream_fileno);
  return recovery_pos;
}
*/

static int update_offdata(struct FD_BIGPOOL *bp, fd_stream stream,
                          int n, struct BIGPOOL_SAVEINFO *saveinfo)
{
  unsigned int min_off=bp->pool_capacity, max_off=0;
  fd_outbuf outstream=fd_writebuf(stream);
  int i=0;
  u8_log(fddb_loglevel+1,"BigpoolFinalize",
         "Finalizing %d oid values %s in %f seconds",
         n,bp->pool_idstring);
  while (i<n) {
    unsigned int oidoff=saveinfo[i++].oidoff;
    if (oidoff>max_off) max_off=oidoff;
    if (oidoff<min_off) min_off=oidoff;}

  if (bp->pool_offdata) {
#if HAVE_MMAP
    unsigned int *offdata;
    if (bp->pool_offdata) reload_offdata(bp,0,1);
    offdata=bp->pool_offdata;
    switch (bp->pool_offtype) {
    case FD_B64: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff;
        offdata[oidoff*3]=fd_net_order((saveinfo[k].chunk.off)>>32);
        offdata[oidoff*3+1]=fd_net_order((saveinfo[k].chunk.off)&(0xFFFFFFFF));
        offdata[oidoff*3+2]=fd_net_order(saveinfo[k].chunk.size);
        k++;}
      break;}
    case FD_B32: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff;
        offdata[oidoff*2]=fd_net_order(saveinfo[k].chunk.off);
        offdata[oidoff*2+1]=fd_net_order(saveinfo[k].chunk.size);
        k++;}
      break;}
    case FD_B40: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff, w1=0, w2=0;
        convert_FD_B40_ref(saveinfo[k].chunk,&w1,&w2);
        offdata[oidoff*2]=fd_net_order(w1);
        offdata[oidoff*2+1]=fd_net_order(w2);
        k++;}
      break;}
    default:
      u8_log(LOG_WARN,"Bad offset type for %s",bp->pool_idstring);
      u8_free(saveinfo);
      exit(-1);}
    if (bp->pool_offdata) reload_offdata(bp,0,-1);
#else
    int i=0, refsize=get_chunk_ref_size(bp), offsize=bp->pool_offdata_length;
    unsigned int *offdata=
      u8_realloc(bp->pool_offdata,refsize*(bp->pool_load));
    if (offdata) {
      bp->pool_offdata=offdata;
      bp->pool_offdata_length=refsize*bp->pool_load;}
    else {
      u8_log(LOG_WARN,"Realloc failed","When writing offdata");
      return -1;}
    switch (bp->pool_offtype) {
    case FD_B64: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff;
        offdata[oidoff*3]=(saveinfo[k].chunk.off)>>32;
        offdata[oidoff*3+1]=((saveinfo[k].chunk.off)&(0xFFFFFFFF));
        offdata[oidoff*3+2]=(saveinfo[k].chunk.achoice_size);
        k++;}
      break;}
    case FD_B32: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff;
        offdata[oidoff*2]=(saveinfo[k].chunk.off);
        offdata[oidoff*2+1]=(saveinfo[k].chunk.achoice_size);
        k++;}
      break;}
    case FD_B40: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff, w1, w2;
        convert_FD_B40_ref(saveinfo[k].chunk,&w1,&w2);
        offdata[oidoff*2]=(w1);
        offdata[oidoff*2+1]=(w2);
        k++;}
      break;}
    default:
      u8_log(LOG_WARN,"Bad offset type for %s",bp->pool_idstring);
      u8_free(saveinfo);
      exit(-1);}
    {
      unsigned int range=max_off-min_off;
      fd_setpos(stream,256+(min_off*refsize));
      fd_write_ints(outstream,(range*(refsize/4)),offdata+min_off);}
#endif
  } else switch (bp->pool_offtype) {
    case FD_B32: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff;
        fd_setpos(stream,256+oidoff*8);
        fd_write_4bytes(outstream,saveinfo[k].chunk.off);
        fd_write_4bytes(outstream,saveinfo[k].chunk.size);
        k++;}
      break;}
    case FD_B40: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff, w1=0, w2=0;
        fd_setpos(stream,256+oidoff*8);
        convert_FD_B40_ref(saveinfo[k].chunk,&w1,&w2);
        fd_write_4bytes(outstream,w1);
        fd_write_4bytes(outstream,w2);
        k++;}
      break;}
    case FD_B64: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff;
        fd_setpos(stream,256+oidoff*12);
        fd_write_8bytes(outstream,saveinfo[k].chunk.off);
        fd_write_4bytes(outstream,saveinfo[k].chunk.size);
        k++;}
      break;}
    default:
      u8_log(LOG_WARN,"Bad offset type for %s",bp->pool_idstring);
      u8_free(saveinfo);
      exit(-1);}
  write_bigpool_load(bp);
  write_bigpool_slotids(bp);
  if (bp->pool_mmap) {
    int retval=munmap(bp->pool_mmap,bp->pool_mmap_size);
    if (retval<0) {
      u8_log(LOG_WARN,"MUNMAP",
             "bigpool MUNMAP failed with %s",
             u8_strerror(errno));
      errno=0;}
    bp->pool_mmap_size=u8_file_size(bp->pool_idstring);
    bp->pool_mmap=
      mmap(NULL,bp->pool_mmap_size,PROT_READ,MMAP_FLAGS,
           bp->pool_stream.stream_fileno,0);}
  return 0;
}

static fdtype bigpool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE; int i=0;
  fd_bigpool bp=(fd_bigpool)p;
  fd_lock_pool(bp);
  if (!(POOLFILE_LOCKEDP(bp))) lock_bigpool_file(bp,0);
  if (bp->pool_load+n>=bp->pool_capacity) {
    fd_unlock_pool(bp);
    return fd_err(fd_ExhaustedPool,"file_pool_alloc",
                  p->pool_idstring,FD_VOID);}
  while (i < n) {
    FD_OID new_addr=FD_OID_PLUS(bp->pool_base,bp->pool_load);
    fdtype new_oid=fd_make_oid(new_addr);
    FD_ADD_TO_CHOICE(results,new_oid);
    i++;}
  bp->pool_load+=n;
  fd_unlock_pool(bp);
  return fd_simplify_choice(results);
}

static int bigpool_lock(fd_pool p,fdtype oids)
{
  struct FD_BIGPOOL *fp=(struct FD_BIGPOOL *)p;
  int retval=lock_bigpool_file(fp,1);
  return retval;
}

static int bigpool_unlock(fd_pool p,fdtype oids)
{
  struct FD_BIGPOOL *fp=(struct FD_BIGPOOL *)p;
  if (fp->pool_changes.table_n_keys == 0)
    /* This unlocks the underlying file, not the stream itself */
    fd_unlockfile(&(fp->pool_stream));
  return 1;
}

static void bigpool_setcache(fd_pool p,int level)
{
  fd_bigpool bp=(fd_bigpool)p;
  int chunk_ref_size=get_chunk_ref_size(bp);
  if (chunk_ref_size<0) {
    u8_log(LOG_WARN,fd_CorruptedPool,"Pool structure invalid: %s",p->pool_idstring);
    return;}
  if ( ( (level<2) && (bp->pool_offdata == NULL) ) ||
       ( (level==2) && ( bp->pool_offdata != NULL ) ) )
    return;
  fd_lock_pool(bp);
  if ( ( (level<2) && (bp->pool_offdata == NULL) ) ||
       ( (level==2) && ( bp->pool_offdata != NULL ) ) ) {
    fd_unlock_pool(bp);
    return;}
#if (!(HAVE_MMAP))
  if (level < 2) {
    if (bp->pool_offdata) {
      u8_free(bp->pool_offdata);
      bp->pool_offdata=NULL;}
    fd_unlock_pool(bp);
    return;}
  else {
    unsigned int *offsets;
    fd_stream s=&(bp->pool_stream);
    fd_inbuf ins=fd_readbuf(s);
    if (LOCK_POOLSTREAM(bp)<0) {
      fd_clear_errors(1);}
    else {
      size_t offsets_size=chunk_ref_size*(bp->pool_load);
      fd_stream_start_read(s);
      fd_setpos(s,12);
      bp->pool_load=load=fd_read_4bytes(ins);
      offsets=u8_malloc(offsets_size);
      fd_setpos(s,24);
      fd_read_ints(ins,load,offsets);
      bp->pool_offdata=offsets;
      bp->pool_offdata_length=offsets_size;
      UNLOCK_POOLSTREAM(bp);}
    fd_unlock_pool(bp);
    return;}
#else /* HAVE_MMAP */
  if ( (level < 2) && (bp->pool_offdata) ) {
    /* Unmap the offsets cache */
    int retval;
    size_t offsets_size=bp->pool_offdata_length;
    size_t header_size=256+offsets_size;
    /* The address to munmap is 64 (not 256) because bp->pool_offdata is an
       (unsigned int *) */
    retval=munmap((bp->pool_offdata)-64,header_size);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "bigpool_setcache:munmap %s",bp->pool_idstring);
      bp->pool_offdata=NULL;
      U8_CLEAR_ERRNO();}
    bp->pool_offdata=NULL;
    bp->pool_offdata_length=0;}

  if ( (level < 3) && (bp->pool_mmap) ) {
    /* Unmap the file */
    int retval=munmap(bp->pool_mmap,bp->pool_mmap_size);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "hash_index_setcache:munmap %s",bp->pool_idstring);
      U8_CLEAR_ERRNO();}
    bp->pool_mmap=NULL;
    bp->pool_mmap_size=-1;}

  if ( (LOCK_POOLSTREAM(bp,"bigpool_setcache")) < 0) {
    u8_log(LOGWARN,"PoolStreamClosed",
           "During bigpool_setcache for %s",bp->pool_idstring);
    UNLOCK_POOLSTREAM(bp);
    fd_unlock_pool(bp);
    return;}

  if ( (level >= 2) && (bp->pool_offdata == NULL) ) {
    unsigned int *offsets, *newmmap;
    /* Sizes here are in bytes */
    size_t offsets_size=(bp->pool_load)*chunk_ref_size;
    size_t header_size=256+offsets_size;
    /* Map the offsets */
    newmmap=
      mmap(NULL,header_size,PROT_READ,MAP_SHARED|MAP_NORESERVE,
           bp->pool_stream.stream_fileno,
           0);
    if ((newmmap==NULL) || (newmmap==((void *)-1))) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "bigpool_setcache:mmap %s",bp->pool_idstring);
      bp->pool_offdata=NULL;
      bp->pool_offdata_length=0;
      U8_CLEAR_ERRNO();}
    else {
      bp->pool_offdata=offsets=newmmap+64;
      bp->pool_offdata_length=offsets_size;} }

  if ( (level >= 3) && (bp->pool_mmap == NULL) ) {
      unsigned char *mmapped=NULL;
      ssize_t mmap_size=u8_file_size(bp->pool_idstring);
      if (mmap_size<0) {
        u8_log(LOG_WARN,u8_strerror(errno),
               "bigpool u8_file_size for mmap %s",bp->pool_source);
        errno=0;
        UNLOCK_POOLSTREAM(bp);
        fd_unlock_pool(bp);
        return;}
      mmapped=
        mmap(NULL,mmap_size,PROT_READ,MMAP_FLAGS,
             bp->pool_stream.stream_fileno,0);
      if (mmapped) {
        bp->pool_mmap=mmapped;
        bp->pool_mmap_size=mmap_size;}
      else {
        u8_log(LOG_WARN,u8_strerror(errno),
               "bigpool_setcache:mmap %s",bp->pool_source);
        bp->pool_mmap=mmapped;
        bp->pool_mmap_size=mmap_size;
        errno=0;}}

  UNLOCK_POOLSTREAM(bp);
  fd_unlock_pool(bp);
#endif /* HAVE_MMAP */
}

/* Write values:
  * 0: just for reading, open up to the *load* of the pool
  * 1: for writing, open up to the capcity of the pool
  * -1: for reading, but sync before remapping
*/
static void reload_offdata(fd_bigpool bp,int lock,int write)
{
  double start=u8_elapsed_time();
#if HAVE_MMAP
  /* With MMAP, this consists of unmapping the current buffer
     and mapping a new one with the new load. */
  int retval=0, chunk_ref_size=get_chunk_ref_size(bp);
  if (lock) fd_lock_pool(bp);

  if (write<0)
    retval=msync(bp->pool_offdata-64,bp->pool_offdata_length+256,MS_SYNC|MS_INVALIDATE);
  if (retval<0) {
    u8_log(LOG_WARN,u8_strerror(errno),"bigpool/reload_offsets:msync %s",bp->pool_idstring);
    retval=0;}
  /* Unmap the current buffer */
  retval=munmap((bp->pool_offdata)-64,(bp->pool_offdata_length)+256);
  if (retval<0) {
    u8_log(LOG_WARN,u8_strerror(errno),"bigpool/reload_offsets:munmap %s",bp->pool_idstring);
    bp->pool_offdata=NULL; errno=0;}
  else {
    fd_stream s=&(bp->pool_stream);
    size_t mmap_size=(write>0)?
      (chunk_ref_size*(bp->pool_capacity)):
      (chunk_ref_size*(bp->pool_load));
    unsigned int *newmmap;
    /* Map with the new load */
    newmmap=
      /* When allocating an offset buffer to read, we only have to make it as
         big as the file pools load. */
      mmap(NULL,(mmap_size)+256,
           ((write>0) ? (PROT_READ|PROT_WRITE) : (PROT_READ)),
           ((write>0) ? (MAP_SHARED) : (MAP_SHARED|MAP_NORESERVE)),
           s->stream_fileno,0);
    if ((newmmap==NULL) || (newmmap==((void *)-1))) {
      u8_log(LOG_WARN,u8_strerror(errno),"bigpool/reload_offsets:mmap %s",bp->pool_idstring);
      bp->pool_offdata=NULL; bp->pool_offdata_length=0; errno=0;}
    bp->pool_offdata=newmmap+64;
    bp->pool_offdata_length=mmap_size;}
  if (lock) fd_unlock_pool(bp);
#else
  fd_stream s=&(bp->pool_stream);
  fd_inbuf ins=fd_readbuf(s);
  /* Read new offsets table, compare it with the current, and
     only void those OIDs */
  unsigned int new_load, *offsets, *nscan, *oscan, *olim;
  struct FD_STREAM *s=&(bp->pool_stream);
  if (lock) fd_lock_pool(bp);
  if ( (LOCK_POOLSTREAM(bp,"bigpool/reload_offsets")) < 0) {
    u8_log(LOGWARN,"PoolStreamClosed",
           "During bigpool_reload_offsets for %s",bp->pool_idstring);
    UNLOCK_POOLSTREAM(bp)
    fd_unlock_pool(bp);
    return;}
  oscan=bp->pool_offdata; olim=oscan+(bp->pool_offdata_length/4);
  fd_setpos(s,0x10); new_load=fd_read_4bytes(ins);
  nscan=offsets=u8_alloc_n(new_load,unsigned int);
  fd_setpos(s,0x100);
  fd_read_ints(ins,new_load,offsets);
  while (oscan < olim)
    if (*oscan == *nscan) {oscan++; nscan++;}
    else {
      FD_OID addr=FD_OID_PLUS(bp->pool_base,(nscan-offsets));
      fdtype changed_oid=fd_make_oid(addr);
      fd_hashtable_bp(&(bp->pool_cache),fd_table_replace,changed_oid,FD_VOID);
      oscan++; nscan++;}
  u8_free(bp->pool_offdata);
  bp->pool_offdata=offsets;
  bp->pool_load=new_load;
  bp->pool_offdata_length=new_load*get_chunk_ref_size(bp);
  update_modtime(bp);
  UNLOCK_POOLSTREAM(bp)
  if (lock) fd_unlock_pool(bp);
#endif
  u8_log(LOGWARN,"ReloadOffsets","Offsets for %s reloaded in %f secs",
         bp->pool_idstring,u8_elapsed_time()-start);
}

static void bigpool_close(fd_pool p)
{
  fd_bigpool bp=(fd_bigpool)p;
  fd_lock_pool(bp);
  if (bp->pool_offdata) {
#if HAVE_MMAP
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval=munmap((bp->pool_offdata)-64,bp->pool_offdata_length+256);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "bigpool_close:munmap offsets %s",bp->pool_idstring);
      errno=0;}
#else
    u8_free(bp->pool_offdata);
#endif
    bp->pool_offdata=NULL; bp->pool_offdata_length=0;
    bp->pool_cache_level=-1;}
#if HAVE_MMAP
  if (bp->pool_mmap) {
    int retval=munmap(bp->pool_mmap,bp->pool_mmap_size);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "bigpool_close:munmap %s",bp->pool_idstring);
      errno=0;}}
#endif
  if (POOLFILE_LOCKEDP(bp))
    write_bigpool_load(bp);
  fd_close_stream(&(bp->pool_stream),0);
  fd_unlock_pool(bp);
}

static void bigpool_setbuf(fd_pool p,int bufsiz)
{
  fd_bigpool bp=(fd_bigpool)p;
  fd_lock_pool(bp);
  fd_stream_setbuf(&(bp->pool_stream),bufsiz);
  fd_unlock_pool(bp);
}

/* Creating bigpool */

static int interpret_pool_flags(fdtype opts)
{
  int flags=0;
  fdtype offtype=fd_intern("OFFTYPE");
  fdtype compression=fd_intern("COMPRESSION");
  if ( fd_testopt(opts,offtype,fd_intern("B64"))  ||
       fd_testopt(opts,offtype,FD_INT(64)))
    flags|=FD_B64;
  else if ( fd_testopt(opts,offtype,fd_intern("B40"))  ||
            fd_testopt(opts,offtype,FD_INT(40)))
    flags|=FD_B40;
  else if ( fd_testopt(opts,offtype,fd_intern("B32"))  ||
            fd_testopt(opts,offtype,FD_INT(32)))
    flags|=FD_B32;
  else flags|=FD_B40;

  if (fd_testopt(opts,compression,fd_intern("ZLIB")))
    flags|=((FD_ZLIB)<<3);
  else if (fd_testopt(opts,compression,fd_intern("BZ2")))
    flags|=((FD_BZ2)<<3);
  else if (fd_testopt(opts,compression,FD_VOID))
    flags|=((FD_ZLIB)<<3);
  else flags=flags;

  if (fd_testopt(opts,fd_intern("DTYPEV2"),FD_VOID))
    flags|=FD_BIGPOOL_DTYPEV2;

  if (fd_testopt(opts,fd_intern("READONLY"),FD_VOID))
    flags|=FD_BIGPOOL_READ_ONLY;

  return flags;
}

static fd_pool bigpool_create(u8_string spec,void *type_data,
                              fddb_flags flags,fdtype opts)
{
  fdtype base_oid=fd_getopt(opts,fd_intern("BASE"),FD_VOID);
  fdtype capacity_arg=fd_getopt(opts,fd_intern("CAPACITY"),FD_VOID);
  fdtype load_arg=fd_getopt(opts,fd_intern("LOAD"),FD_FIXZERO);
  fdtype label=fd_getopt(opts,fd_intern("LABEL"),FD_VOID);
  fdtype schemas=fd_getopt(opts,fd_intern("SCHEMAS"),FD_VOID);
  unsigned int capacity, load;
  int rv=0;
  if (u8_file_existsp(spec)) {
    fd_seterr(_("FileAlreadyExists"),"bigpool_create",spec,FD_VOID);
    return NULL;}
  else if (!(FD_OIDP(base_oid))) {
    fd_seterr("Not a base oid","bigpool_create",spec,base_oid);
    rv=-1;}
  else if (FD_ISINT(capacity_arg)) {
    int capval=fd_getint(capacity_arg);
    if (capval<=0) {
      fd_seterr("Not a valid capacity","bigpool_create",
                spec,capacity_arg);
      rv=-1;}
    else capacity=capval;}
  else {
    fd_seterr("Not a valid capacity","bigpool_create",
              spec,capacity_arg);
      rv=-1;}
  if (rv<0) {}
  else if (FD_ISINT(load_arg)) {
    int loadval=fd_getint(load_arg);
    if (loadval<0) {
      fd_seterr("Not a valid load","bigpool_create",
                spec,load_arg);
      rv=-1;}
    else load=loadval;}
  else {
    fd_seterr("Not a valid load","bigpool_create",
              spec,load_arg);
    rv=-1;}
  if (rv<0) return NULL;
  else rv=fd_make_bigpool(spec,
                          ((FD_STRINGP(label)) ? (FD_STRDATA(label)) : (spec)),
                          FD_OID_ADDR(base_oid),capacity,load,
                          interpret_pool_flags(opts),
                          schemas,
                          time(NULL),
                          time(NULL),1);
  if (rv>=0)
    return fd_open_pool(spec,flags);
  else return NULL;
}

/* Module (file) Initialization */

static struct FD_POOL_HANDLER bigpool_handler={
  "bigpool", 1, sizeof(struct FD_BIGPOOL), 12,
  bigpool_close, /* close */
  bigpool_setcache, /* setcache */
  bigpool_setbuf, /* setbuf */
  bigpool_alloc, /* alloc */
  bigpool_fetch, /* fetch */
  bigpool_fetchn, /* fetchn */
  bigpool_load, /* getload */
  bigpool_lock, /* lock */
  bigpool_unlock, /* release */
  bigpool_storen, /* storen */
  NULL, /* swapout */
  NULL, /* metadata */
  NULL, /* sync */
  bigpool_create, /* create */
  NULL  /* poolop */
};


/* Matching pool names */

static u8_string match_pool_name(u8_string spec,void *data)
{
  if ((u8_file_existsp(spec)) &&
      (fd_match4bytes(spec,data)))
    return spec;
  else if (u8_has_suffix(spec,".pool",1))
    return NULL;
  else {
    u8_string variation=u8_mkstring("%s.pool",spec);
    if ((u8_file_existsp(variation))&&
        (fd_match4bytes(variation,data)))
      return variation;
    else {
      u8_free(variation);
      return NULL;}}
}

FD_EXPORT void fd_init_bigpools_c()
{
  u8_register_source_file(_FILEINFO);

  fd_register_pool_type
    ("bigpool",
     &bigpool_handler,
     open_bigpool,
     match_pool_name,
     (void*)U8_INT2PTR(FD_BIGPOOL_MAGIC_NUMBER));
  /*
  fd_register_pool_type
    ("damaged_bigpool",
     &bigpool_handler,
     open_bigpool,
     match_pool_name,
     (void*)U8_INT2PTR(FD_BIGPOOL_TO_RECOVER));
  */
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
