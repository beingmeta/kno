/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* Notes:
   A normal 32-bit hash index with N buckets consists of 256 bytes of
   header, followed by N*8 bytes of offset table, followed by an arbitrary
   number of "data blocks", each starting with a zint-encoded byte,
   count and a zint-encoded element count;

   The header consists of: a 4-byte magic number (identifying the file
   type), a 4-byte number indicating the number of buckets, 1 byte
   indicating the "hash function" for the bucket and 3 bytes indicating
   other flags and followed by a 4 byte number reserved for customizing
   the hash function. This is followed by a 4 byte offset and a 4 byte
   size pointing to a DTYPE representation for the index's metadata.
   Following this are

   0x00     XXXX    4-byte magic number
   0x04     XXXX    number of buckets
   0x08     XXXX    flags, including hash function identifier
   0x0C     XXXX    hash function constant
   0x10     XXXX    number of keys
   0x14     XXXX    file offset of slotids vector
   0x18     XXXX     (64 bits)
   0x1C     XXXX    size of slotids DTYPE representation
   0x20     XXXX    file offset of baseoids vector
   0x28     XXXX     (64 bits)
   0x2C     XXXX    size of baseoids DTYPE representation
   0x30     XXXX    file offset of index metadata
   0x34     XXXX     (64 bits)
   0x38     XXXX    size of metadata DTYPE representation
   0x3C     XXXX    number of used buckets
   0x40     XXXX    number of keyblocks
   0x44     XXXX     (64 bits)
   0x48     XXXX    number of value blocks
   0x4C     XXXX     (64 bits)
   0x80     XXXX    reserved for future use
   ....
   0xEA     XXXX    end of valid data
   0xEC     XXXX     (64 bits)
   0x100    XXXX    beginning of offsets table

   There are two basic kinds of data blocks: key blocks and value
   blocks.  Continuation offsets are not currently supported for key
   blocks.  A value block consists of a byte count, an element count
   (N), and a (possibly zero) continuation pointer, followed by N
   "zvalue" dtype representations.  A key block consists of a
   byte_count, an element_count, and a number of key entries.  A key
   entry consists of a zint-coded size (in bytes) followed by a zkey
   dtype representation, followed by a zint value_count.  If the
   value_count is zero, the entry ends, if it is one, it is followed
   by a single zvalue dtype.  Otherwise, it is followed by a 4 byte
   file offset to a value block and a zint coded block size.

   A zvalue dtype representation consists of a zint encoded OID serial
   number.  If this is zero, it is followed by a regular DTYPE representation.
   Otherwise, it is followed by three bytes of OID offset which are added
   to the base OID associated with the serial number.

   A zkey dtype representation consists of a zint encoded SLOTID
   serial number followed by a regular DTYPE representation.  If the
   serial number is zero, the key is the following DTYPE; otherwise,
   the key is a pair of the corresponding SLOTID and the following
   DTYPE.
*/

extern int fd_storage_loglevel;
static int hashindex_loglevel = -1;
#define U8_LOGLEVEL (fd_int_default(hashindex_loglevel,(fd_storage_loglevel-1)))

#define FD_INLINE_BUFIO 1
#define FD_INLINE_CHOICES 1
#define FD_FAST_CHOICE_CONTAINSP 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dtypeio.h"
#include "framerd/numbers.h"
#include "framerd/storage.h"
#include "framerd/streams.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include "headers/hashindex.h"

#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8pathfns.h>
#include <libu8/libu8io.h>

#include <errno.h>
#include <math.h>
#include <sys/stat.h>

#if (FD_USE_MMAP)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#define MMAP_FLAGS MAP_SHARED
#endif

#ifndef FD_DEBUG_HASHINDEXES
#define FD_DEBUG_HASHINDEXES 0
#endif

#ifndef FD_DEBUG_DTYPEIO
#define FD_DEBUG_DTYPEIO 0
#endif

#ifndef HX_KEYBUF_SIZE
#define HX_KEYBUF_SIZE 8000
#endif

#ifndef HX_VALBUF_SIZE
#define HX_VALBUF_SIZE 16000
#endif

#define LOCK_STREAM 1
#define DONT_LOCK_STREAM 0

#if FD_DEBUG_HASHINDEXES
#define CHECK_POS(pos,stream)                                       \
  if ((pos)!=(fd_getpos(stream)))                                   \
    u8_logf(LOG_CRIT,"FILEPOS error","position mismatch %ld/%ld",   \
            pos,fd_getpos(stream));                                 \
  else {}
#define CHECK_ENDPOS(pos,stream)                                        \
  { ssize_t curpos = fd_getpos(stream), endpos = fd_endpos(stream);     \
    if (((pos)!=(curpos)) && ((pos)!=(endpos)))                         \
      u8_logf(LOG_CRIT,"ENDPOS error","position mismatch %ld/%ld/%ld",  \
              pos,curpos,endpos);                                       \
    else {}                                                             \
  }
#else
#define CHECK_POS(pos,stream)
#define CHECK_ENDPOS(pos,stream)
#endif

FD_EXPORT u8_condition fd_TooManyArgs;

static ssize_t hashindex_default_size=32000;

static ssize_t get_maxpos(fd_hashindex p)
{
  switch (p->index_offtype) {
  case FD_B32:
    return ((size_t)(((size_t)1)<<32));
  case FD_B40:
    return ((size_t)(((size_t)1)<<40));
  case FD_B64:
    return ((size_t)(((size_t)1)<<62));
  default:
    return -1;}
}

static void init_cache_level(fd_index ix)
{
  if (PRED_FALSE(ix->index_cache_level<0)) {
    lispval opts = ix->index_opts;
    long long level=fd_getfixopt(opts,"CACHELEVEL",fd_default_cache_level);
    fd_index_setcache(ix,level);}
}

static lispval read_values(fd_hashindex,lispval,int,fd_off_t,size_t);

static struct FD_INDEX_HANDLER hashindex_handler;

static u8_condition CorruptedHashIndex=_("Corrupted hashindex file");
static u8_condition BadHashFn=_("hashindex has unknown hash function");

static lispval set_symbol, drop_symbol, keycounts_symbol;
static lispval slotids_symbol, baseoids_symbol, buckets_symbol, nkeys_symbol;

/* Utilities for DTYPE I/O */

#define nobytes(in,nbytes) (PRED_FALSE(!(fd_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (PRED_TRUE(fd_request_bytes(in,nbytes)))

#define output_byte(out,b)                              \
  if (fd_write_byte(out,b)<0) return -1; else {}
#define output_4bytes(out,w)                            \
  if (fd_write_4bytes(out,w)<0) return -1; else {}
#define output_bytes(out,bytes,n)                       \
  if (fd_write_bytes(out,bytes,n)<0) return -1; else {}

/* Getting chunk refs */

static ssize_t get_chunk_ref_size(fd_hashindex ix)
{
  switch (ix->index_offtype) {
  case FD_B32: case FD_B40: return 8;
  case FD_B64: return 12;}
  return -1;
}

/* Opening database blocks */

FD_FASTOP lispval read_dtype_at_pos(fd_stream s,fd_off_t off)
{
  fd_off_t retval = fd_setpos(s,off);
  fd_inbuf ins = fd_readbuf(s);
  if (retval<0) return FD_ERROR;
  else return fd_read_dtype(ins);
}

/* Opening a hash index */

static int load_header(struct FD_HASHINDEX *index,struct FD_STREAM *stream);

static fd_index open_hashindex(u8_string fname,fd_storage_flags flags,
                               lispval opts)
{
  struct FD_HASHINDEX *index = u8_alloc(struct FD_HASHINDEX);
  int read_only = U8_BITP(flags,FD_STORAGE_READ_ONLY);

  if ( (read_only == 0) && (u8_file_writablep(fname)) ) {
    if (fd_check_rollback("open_hashindex",fname)<0) {
      /* If we can't apply the rollback, open the file read-only */
      u8_log(LOG_WARN,"RollbackFailed",
             "Opening hashindex %s as read-only due to failed rollback",
             fname);
      fd_clear_errors(1);
      read_only=1;}}
  else read_only=1;

  u8_string abspath = u8_abspath(fname,NULL);
  u8_string realpath = u8_realpath(fname,NULL);
  unsigned int magicno;
  fd_stream_mode mode=
    ((read_only) ? (FD_FILE_READ) : (FD_FILE_MODIFY));
  int stream_flags =
    FD_STREAM_CAN_SEEK | FD_STREAM_NEEDS_LOCK | FD_STREAM_READ_ONLY;

  fd_init_index((fd_index)index,&hashindex_handler,
                fname,abspath,realpath,
                flags,FD_VOID,opts);

  fd_stream stream=
    fd_init_file_stream(&(index->index_stream),abspath,mode,stream_flags,-1);
  u8_free(abspath); u8_free(realpath);

  if (stream == NULL) {
    u8_free(index);
    fd_seterr3(u8_CantOpenFile,"open_hashindex",fname);
    return NULL;}
  /* See if it ended up read only */
  if (!(u8_file_writablep(fname)))
    read_only = 1;
  stream->stream_flags &= ~FD_STREAM_IS_CONSED;
  magicno = fd_read_4bytes_at(stream,0,FD_ISLOCKED);
  if ( magicno != 0x8011308) {
    fd_seterr3(fd_NotAFileIndex,"open_hashindex",fname);
    u8_free(index);
    fd_close_stream(stream,FD_STREAM_FREEDATA|FD_STREAM_NOFLUSH);
    return NULL;}

  index->index_n_buckets =
    fd_read_4bytes_at(stream,FD_HASHINDEX_NBUCKETS_POS,FD_ISLOCKED);
  index->index_offdata = NULL;
  index->hashindex_format =
    fd_read_4bytes_at(stream,FD_HASHINDEX_FORMAT_POS,FD_ISLOCKED);
  if (read_only) {
    U8_SETBITS(index->index_flags,FD_STORAGE_READ_ONLY);}
  else if ((index->hashindex_format) & (FD_HASHINDEX_READ_ONLY) ) {
    U8_SETBITS(index->index_flags,FD_STORAGE_READ_ONLY);}
  else NO_ELSE;

  if ((flags) & (FD_INDEX_ONESLOT) ) {}
  else if ((index->hashindex_format) & (FD_HASHINDEX_ONESLOT) ) {
    U8_SETBITS(index->index_flags,FD_INDEX_ONESLOT);}
  else NO_ELSE;

  if (((index->hashindex_format)&(FD_HASHINDEX_FN_MASK))!=0)  {
    u8_free(index);
    fd_seterr3(BadHashFn,"open_hashindex",NULL);
    return NULL;}

  index->index_offtype = (fd_offset_type)
    (((index->hashindex_format)&(FD_HASHINDEX_OFFTYPE_MASK))>>4);

  index->index_custom = fd_read_4bytes_at(stream,12,FD_ISLOCKED);

  index->table_n_keys = fd_read_4bytes_at(stream,FD_HASHINDEX_NKEYS_POS,FD_ISLOCKED);

  struct FD_SLOTCODER *sc = &(index->index_slotcodes);
  struct FD_OIDCODER *oc = &(index->index_oidcodes);
  memset(sc,0,sizeof(struct FD_SLOTCODER));
  memset(oc,0,sizeof(struct FD_OIDCODER));
  u8_init_rwlock(&(sc->rwlock));

  int rv = load_header(index,stream);

  if (rv < 0) {
    fd_free_stream(stream);
    u8_free(index);
    return NULL;}

  u8_init_mutex(&(index->index_lock));

  fd_register_index((fd_index)index);

  return (fd_index)index;
}

static int load_header(struct FD_HASHINDEX *index,struct FD_STREAM *stream)
{
  u8_string fname = index->index_source;

  fd_off_t slotids_pos =
    fd_read_8bytes_at(stream,FD_HASHINDEX_SLOTIDS_POS,FD_ISLOCKED,NULL);
  ssize_t slotids_size =
    fd_read_4bytes_at(stream,FD_HASHINDEX_SLOTIDS_POS+8,FD_ISLOCKED);

  fd_off_t baseoids_pos =
    fd_read_8bytes_at(stream,FD_HASHINDEX_BASEOIDS_POS,FD_ISLOCKED,NULL);
  ssize_t baseoids_size =
    fd_read_4bytes_at(stream,FD_HASHINDEX_BASEOIDS_POS+8,FD_ISLOCKED);

  fd_off_t metadata_loc  = fd_read_8bytes_at
    (stream,FD_HASHINDEX_METADATA_POS,FD_ISLOCKED,NULL);
  ssize_t metadata_size = fd_read_4bytes_at
    (stream,FD_HASHINDEX_METADATA_POS+8,FD_ISLOCKED);

  /* Initialize the slotids field used for storing feature keys */
  if (slotids_size) {
    lispval slotids_vector = read_dtype_at_pos(stream,slotids_pos);
    if ( (VOIDP(slotids_vector)) || (FD_FALSEP(slotids_vector)) ) {
      fd_init_slotcoder(&(index->index_slotcodes),-1,NULL);}
    else if (VECTORP(slotids_vector)) {
      fd_init_slotcoder(&(index->index_slotcodes),
                        VEC_LEN(slotids_vector),
                        VEC_DATA(slotids_vector));
      fd_decref(slotids_vector);}
    else {
      fd_seterr("Bad SLOTIDS data","open_hashindex",fname,VOID);
      return -1;}
    index->hx_slotcodes_pos = slotids_pos;}
  else fd_init_slotcoder(&(index->index_slotcodes),-1,NULL);

  /* Initialize the baseoids field used for compressed OID values */
  if (baseoids_size) {
    lispval baseoids_vector = read_dtype_at_pos(stream,baseoids_pos);
    if ( (VOIDP(baseoids_vector)) || (FD_FALSEP(baseoids_vector)) )
      fd_init_oidcoder(&(index->index_oidcodes),-1,NULL);
    else if (VECTORP(baseoids_vector)) {
      fd_init_oidcoder(&(index->index_oidcodes),
                       VEC_LEN(baseoids_vector),
                       VEC_DATA(baseoids_vector));
      fd_decref(baseoids_vector);}
    else {
      fd_seterr("Bad BASEOIDS data","open_hashindex",fname,VOID);
      return -1;}
    index->hx_oidcodes_pos = baseoids_pos;}
  else fd_init_oidcoder(&(index->index_oidcodes),-1,NULL);

  int modified_metadata = 0;
  lispval metadata=FD_VOID;
  if ( (metadata_size) && (metadata_loc) ) {
    if (fd_setpos(stream,metadata_loc)>0) {
      fd_inbuf in = fd_readbuf(stream);
      metadata = fd_read_dtype(in);}
    else {
      fd_seterr("BadMetaData","open_hashindex",
                "BadMetadataLocation",FD_INT(metadata_loc));
      metadata=FD_ERROR_VALUE;}}
  else metadata=FD_FALSE;

  if (FD_FALSEP(metadata))
    metadata = FD_VOID;
  else if (FD_VOIDP(metadata)) {}
  else if (FD_SLOTMAPP(metadata)) {}
  else if ( index->index_flags & FD_STORAGE_REPAIR ) {
    u8_log(LOG_WARN,"BadMetadata",
               "Repairing bad metadata for %s @%lld+%lld = %q",
               fname,metadata_loc,metadata_size,metadata);
    if (FD_ABORTP(metadata)) fd_clear_errors(1);
    fd_decref(metadata);
    metadata=fd_empty_slotmap();
    modified_metadata=1;}
  else return -1;

  if (FD_VOIDP(metadata)) {}
  else if (FD_SLOTMAPP(metadata)) {
    fd_index_set_metadata((fd_index)index,metadata);
    if (modified_metadata)
      index->index_metadata.table_modified = 1;
    index->hx_metadata_pos = metadata_loc;}
  else {
    u8_log(LOG_WARN,"BadMetadata",
           "Bad metadata for %s @%lld+%lld: %q",
           fname,metadata_loc,metadata_size,metadata);}
  fd_decref(metadata);
  metadata = FD_VOID;

  index->table_n_keys = fd_read_4bytes_at(stream,16,FD_ISLOCKED);

  return 1;
}

static fd_index recover_hashindex(u8_string fname,fd_storage_flags open_flags,
                                  lispval opts)
{
  u8_string recovery_file=u8_string_append(fname,".rollback",NULL);
  if (u8_file_existsp(recovery_file)) {
    ssize_t rv=fd_restore_head(recovery_file,fname);
    if (rv<0) {
      u8_graberrno("recover_hashindex",recovery_file);
      return NULL;}
    else if (rv == 0)
      u8_logf(LOG_CRIT,CorruptedHashIndex,
              "The hashindex file %s has a corrupted recovery file %s",
              fname,recovery_file);
    else {
      fd_index opened = open_hashindex(fname,open_flags,opts);
      if (opened) {
        u8_removefile(recovery_file);
        u8_free(recovery_file);
        return opened;}
      if (! (fd_testopt(opts,fd_intern("FIXUP"),FD_VOID))) {
        fd_seterr("RecoveryFailed","recover_hashindex",fname,FD_VOID);
        return NULL;}
      else {
        u8_logf(LOG_ERR,"RecoveryFailed",
                "Recovering %s using %s failed",fname,recovery_file);
        u8_removefile(recovery_file);}}}
  else u8_logf(LOG_CRIT,CorruptedHashIndex,
               "The hashindex file %s doesn't have a recovery file %s",
               fname,recovery_file);
  if (fd_testopt(opts,fd_intern("FIXUP"),FD_VOID)) {
    char *src = u8_tolibc(fname);
    FD_DECL_OUTBUF(headbuf,256);
    unsigned int magicno = FD_HASHINDEX_MAGIC_NUMBER;
    int out=open(src,O_RDWR);
    fd_write_4bytes(&headbuf,magicno);
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
      u8_free(recovery_file);
      return open_hashindex(fname,open_flags,opts);}
    else u8_seterr("FailedRecovery","recover_hashindex",recovery_file);}
  return NULL;
}

/* Making a hash index */

FD_EXPORT int make_hashindex
(u8_string fname,
 int n_buckets_arg,
 unsigned int flags,
 unsigned int hashconst,
 lispval metadata_init,
 lispval slotids_init,
 lispval baseoids_init,
 time_t ctime,
 time_t mtime)
{
  int n_buckets;
  time_t now = time(NULL);
  fd_off_t slotids_pos = 0, baseoids_pos = 0, metadata_pos = 0;
  size_t slotids_size = 0, baseoids_size = 0, metadata_size = 0;
  int offtype = (fd_offset_type)(((flags)&(FD_HASHINDEX_OFFTYPE_MASK))>>4);
  struct FD_STREAM _stream, *stream=
    fd_init_file_stream(&_stream,fname,FD_FILE_CREATE,-1,fd_driver_bufsize);
  struct FD_OUTBUF *outstream = (stream) ? (fd_writebuf(stream)) : (NULL);

  if (outstream == NULL)
    return -1;
  else if ((stream->stream_flags)&FD_STREAM_READ_ONLY) {
    fd_seterr3(fd_CantWrite,"make_hashindex",fname);
    fd_free_stream(stream);
    return -1;}
  stream->stream_flags &= ~FD_STREAM_IS_CONSED;
  if (n_buckets_arg<0) n_buckets = -n_buckets_arg;
  else n_buckets = fd_get_hashtable_size(n_buckets_arg);

  u8_logf(LOG_INFO,"CreateHashIndex",
          "Creating a hashindex '%s' with %ld buckets",
          fname,n_buckets);

  /* Remove leftover files */
  fd_remove_suffix(fname,".commit");
  fd_remove_suffix(fname,".rollback");

  fd_setpos(stream,0);
  fd_write_4bytes(outstream,FD_HASHINDEX_MAGIC_NUMBER);
  fd_write_4bytes(outstream,n_buckets);
  fd_write_4bytes(outstream,flags);
  fd_write_4bytes(outstream,hashconst); /* No custom hash constant */
  fd_write_4bytes(outstream,0); /* No keys to start */

  /* This is where we store the offset and size for the slotid init */
  fd_write_8bytes(outstream,0);
  fd_write_4bytes(outstream,0);

  /* This is where we store the offset and size for the baseoids init */
  fd_write_8bytes(outstream,0);
  fd_write_4bytes(outstream,0);

  /* This is where we store the offset and size for the metadata */
  fd_write_8bytes(outstream,0);
  fd_write_4bytes(outstream,0);

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

  /* Fill the rest of the space. */
  {
    int i = 0, bytes_to_write = 256-fd_getpos(stream);
    while (i<bytes_to_write) {
      fd_write_byte(outstream,0); i++;}}

  /* Write the top level bucket table */
  {
    int i = 0; while (i<n_buckets) {
      fd_write_4bytes(outstream,0);
      fd_write_4bytes(outstream,0);
      if (offtype == FD_B64)
        fd_write_4bytes(outstream,0);
      else {}
      i++;}}

  /* Write the slotids */
  if (VECTORP(slotids_init)) {
    slotids_pos = fd_getpos(stream);
    fd_write_dtype(outstream,slotids_init);
    slotids_size = fd_getpos(stream)-slotids_pos;}

  /* Write the baseoids */
  if (VECTORP(baseoids_init)) {
    baseoids_pos = fd_getpos(stream);
    fd_write_dtype(outstream,baseoids_init);
    baseoids_size = fd_getpos(stream)-baseoids_pos;}

  if (!(FD_VOIDP(metadata_init))) {
    metadata_pos = fd_getpos(stream);
    fd_write_dtype(outstream,metadata_init);
    metadata_size = fd_getpos(stream)-metadata_pos;}

  if (slotids_pos) {
    fd_setpos(stream,FD_HASHINDEX_SLOTIDS_POS);
    fd_write_8bytes(outstream,slotids_pos);
    fd_write_4bytes(outstream,slotids_size);}

  if (baseoids_pos) {
    fd_setpos(stream,FD_HASHINDEX_BASEOIDS_POS);
    fd_write_8bytes(outstream,baseoids_pos);
    fd_write_4bytes(outstream,baseoids_size);}

  if (metadata_pos) {
    fd_setpos(stream,FD_HASHINDEX_METADATA_POS);
    fd_write_8bytes(outstream,metadata_pos);
    fd_write_4bytes(outstream,metadata_size);}

  fd_flush_stream(stream);
  fd_close_stream(stream,FD_STREAM_FREEDATA);

  return 0;
}

/* Hash functions */

typedef unsigned long long ull;

FD_FASTOP unsigned int hash_mult(unsigned int x,unsigned int y)
{
  unsigned long long prod = ((ull)x)*((ull)y);
  return (prod*2100000523)%(MYSTERIOUS_MODULUS);
}

FD_FASTOP unsigned int hash_combine(unsigned int x,unsigned int y)
{
  if ((x == 0) && (y == 0)) return MAGIC_MODULUS+2;
  else if ((x == 0) || (y == 0))
    return x+y;
  else return hash_mult(x,y);
}

FD_FASTOP unsigned int hash_bytes(const unsigned char *start,int len)
{
  unsigned int prod = 1, asint = 0;
  const unsigned char *ptr = start, *limit = ptr+len;
  /* Compute a starting place */
  while (ptr < limit) prod = prod+*ptr++;
  /* Now do a multiplication */
  ptr = start; limit = ptr+((len%4) ? (4*(len/4)) : (len));
  while (ptr < limit) {
    asint = (ptr[0]<<24)|(ptr[1]<<16)|(ptr[2]<<8)|(ptr[3]);
    prod = hash_combine(prod,asint); ptr = ptr+4;}
  switch (len%4) {
  case 0: asint = 1; break;
  case 1: asint = ptr[0]; break;
  case 2: asint = ptr[0]|(ptr[1]<<8); break;
  case 3: asint = ptr[0]|(ptr[1]<<8)|(ptr[2]<<16); break;}
  return hash_combine(prod,asint);
}

/* ZKEYs */

static int fast_write_dtype(fd_outbuf out,lispval key)
{
  int v2 = ((out->buf_flags)&FD_USE_DTYPEV2);
  if (OIDP(key)) {
    FD_OID addr = FD_OID_ADDR(key);
    fd_write_byte(out,dt_oid);
    fd_write_4bytes(out,FD_OID_HI(addr));
    fd_write_4bytes(out,FD_OID_LO(addr));
    return 9;}
  else if (SYMBOLP(key)) {
    int data = FD_GET_IMMEDIATE(key,itype);
    lispval name = fd_symbol_names[data];
    struct FD_STRING *s = fd_consptr(struct FD_STRING *,name,fd_string_type);
    int len = s->str_bytelen;
    if ((v2) && (len<256)) {
      {output_byte(out,dt_tiny_symbol);}
      {output_byte(out,len);}
      {output_bytes(out,s->str_bytes,len);}
      return len+2;}
    else {
      {output_byte(out,dt_symbol);}
      {output_4bytes(out,len);}
      {output_bytes(out,s->str_bytes,len);}
      return len+5;}}
  else if (STRINGP(key)) {
    struct FD_STRING *s = fd_consptr(struct FD_STRING *,key,fd_string_type);
    int len = s->str_bytelen;
    if ((v2) && (len<256)) {
      {output_byte(out,dt_tiny_string);}
      {output_byte(out,len);}
      {output_bytes(out,s->str_bytes,len);}
      return len+2;}
    else {
      {output_byte(out,dt_string);}
      {output_4bytes(out,len);}
      {output_bytes(out,s->str_bytes,len);}
      return len+5;}}
  else return fd_write_dtype(out,key);
}

FD_FASTOP ssize_t write_zkey(fd_hashindex hx,fd_outbuf out,lispval key)
{
  int slotid_index = -1; size_t retval = -1;
  int natsort = out->buf_flags&FD_NATSORT_VALUES;
  out->buf_flags |= FD_NATSORT_VALUES;
  if ( (PAIRP(key)) && (hx->index_slotcodes.slotids) ) {
    lispval car = FD_CAR(key);
    if ((OIDP(car)) || (SYMBOLP(car))) {
      fd_use_slotcodes(& hx->index_slotcodes );
      slotid_index = fd_slotid2code(&(hx->index_slotcodes),car);
      fd_release_slotcodes(& hx->index_slotcodes );
      if (slotid_index<0)
        retval = fd_write_byte(out,0)+fd_write_dtype(out,key);
      else retval = fd_write_zint(out,slotid_index+1)+
             fast_write_dtype(out,FD_CDR(key));}
    else retval = fd_write_byte(out,0)+fd_write_dtype(out,key);}
  else retval = fd_write_byte(out,0)+fd_write_dtype(out,key);
  if (!(natsort)) out->buf_flags &= ~FD_NATSORT_VALUES;
  return retval;
}

FD_FASTOP ssize_t write_zkey_wsc(fd_slotcoder sc,fd_outbuf out,lispval key)
{
  int slotid_index = -1; size_t retval = -1;
  int natsort = out->buf_flags&FD_NATSORT_VALUES;
  out->buf_flags |= FD_NATSORT_VALUES;
  if ( (PAIRP(key)) && (sc->slotids) ) {
    lispval car = FD_CAR(key);
    if ((OIDP(car)) || (SYMBOLP(car))) {
      slotid_index = fd_slotid2code(sc,car);
      if (slotid_index < 0 )
        slotid_index = fd_add_slotcode(sc,car);
      if (slotid_index < 0)
        retval = fd_write_byte(out,0)+fd_write_dtype(out,key);
      else retval = fd_write_zint(out,slotid_index+1)+
             fast_write_dtype(out,FD_CDR(key));}
    else retval = fd_write_byte(out,0)+fd_write_dtype(out,key);}
  else retval = fd_write_byte(out,0)+fd_write_dtype(out,key);
  if (!(natsort)) out->buf_flags &= ~FD_NATSORT_VALUES;
  return retval;
}

static lispval fast_read_dtype(fd_inbuf in)
{
  if (nobytes(in,1)) return fd_return_errcode(FD_EOD);
  else {
    int code = *(in->bufread);
    switch (code) {
    case dt_oid:
      if (nobytes(in,9)) return fd_return_errcode(FD_EOD);
      else {
        FD_OID addr; in->bufread++;
#if FD_STRUCT_OIDS
        memset(&addr,0,sizeof(addr));
#else
        addr = 0;
#endif
        FD_SET_OID_HI(addr,fd_read_4bytes(in));
        FD_SET_OID_LO(addr,fd_read_4bytes(in));
        return fd_make_oid(addr);}
    case dt_string:
      if (nobytes(in,5)) return fd_return_errcode(FD_EOD);
      else {
        int len = fd_get_4bytes(in->bufread+1); in->bufread = in->bufread+5;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          lispval result = fd_make_string(NULL,len,in->bufread);
          in->bufread = in->bufread+len;
          return result;}}
    case dt_tiny_string:
      if (nobytes(in,2)) return fd_return_errcode(FD_EOD);
      else {
        int len = fd_get_byte(in->bufread+1); in->bufread = in->bufread+2;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          lispval result = fd_make_string(NULL,len,in->bufread);
          in->bufread = in->bufread+len;
          return result;}}
    case dt_symbol:
      if (nobytes(in,5)) return fd_return_errcode(FD_EOD);
      else {
        int len = fd_get_4bytes(in->bufread+1); in->bufread = in->bufread+5;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          lispval symbol;
          unsigned char buf[len+1];
          memcpy(buf,in->bufread,len); buf[len]='\0';
          in->bufread = in->bufread+len;
          symbol = fd_make_symbol(buf,len);
          return symbol;}}
    case dt_tiny_symbol:
      if (nobytes(in,2)) return fd_return_errcode(FD_EOD);
      else {
        int len = fd_get_byte(in->bufread+1); in->bufread = in->bufread+2;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          unsigned char buf[257];
          memcpy(buf,in->bufread,len); buf[len]='\0';
          in->bufread = in->bufread+len;
          return fd_make_symbol(buf,len);}}
    default:
      return fd_read_dtype(in);} /* switch */
  } /* else */
}

FD_FASTOP lispval read_key(fd_hashindex hx,fd_inbuf in)
{
  int code = fd_read_zint(in);
  if (code==0)
    return fast_read_dtype(in);
  else if (hx->index_slotcodes.slotids) {
    lispval slotid = fd_code2slotid(&(hx->index_slotcodes),code-1);
    if ( (FD_SYMBOLP(slotid)) || (FD_OIDP(slotid)) ) {
      lispval cdr = fast_read_dtype(in);
      if (FD_ABORTP(cdr))
        return fd_err(CorruptedHashIndex,"read_key/dtype",hx->indexid,VOID);
      else return fd_conspair(slotid,cdr);}
    else return fd_err(CorruptedHashIndex,"read_key/slotcode",hx->indexid,VOID);}
  else return fd_err(CorruptedHashIndex,"read_key/slotcode",hx->indexid,VOID);
}

FD_EXPORT ssize_t hashindex_bucket(struct FD_HASHINDEX *hx,lispval key,
                                   ssize_t modulate)
{
  struct FD_OUTBUF out = { 0 }; unsigned char buf[1024];
  unsigned int hashval; int dtype_len;
  FD_INIT_BYTE_OUTBUF(&out,buf,1024);
  if ((hx->hashindex_format)&(FD_HASHINDEX_DTYPEV2))
    out.buf_flags = out.buf_flags|FD_USE_DTYPEV2;
  dtype_len = write_zkey(hx,&out,key);
  hashval = hash_bytes(out.buffer,dtype_len);
  fd_close_outbuf(&out);
  if (modulate<0)
    return hashval%(hx->index_n_buckets);
  else if (modulate==0)
    return hashval;
  else return hashval%modulate;
}

/* ZVALUEs */

FD_FASTOP int write_zvalue(fd_hashindex hx,fd_outbuf out,
                           fd_oidcoder oc,lispval value)
{
  if (OIDP(value)) { /* (&&(hx->index_oidcodes.n_oids)) */
    int base = FD_OID_BASE_ID(value);
    int oidcode = fd_get_oidcode(oc,base);
    if (oidcode < 0) {
      if (oc->oidcodes)
        oidcode = fd_add_oidcode(oc,value);
      else {}}
    if (oidcode<0) {
      int bytes_written; fd_write_byte(out,0);
      bytes_written = fd_write_dtype(out,value);
      if (bytes_written<0)
        return FD_ERROR;
      else return bytes_written+1;}
    else {
      int offset = FD_OID_BASE_OFFSET(value), bytes_written;
      bytes_written = fd_write_zint(out,oidcode+1);
      if (bytes_written<0)
        return FD_ERROR;
      int more = fd_write_zint(out,offset);
      if (more<0)
        return FD_ERROR;
      else bytes_written = bytes_written+more;
      return bytes_written;}}
  else {
    int bytes_written; fd_write_byte(out,0);
    bytes_written = fd_write_dtype(out,value);
    return bytes_written+1;}
}

FD_FASTOP lispval read_zvalue(fd_hashindex hx,fd_inbuf in)
{
  int prefix = fd_read_zint(in);
  if (prefix==0)
    return fd_read_dtype(in);
  else {
    lispval baseoid = fd_get_baseoid(&(hx->index_oidcodes),(prefix-1));
    unsigned int base = FD_OID_BASE_ID(baseoid);
    unsigned int offset = fd_read_zint(in);
    return FD_CONSTRUCT_OID(base,offset);}
}

/* Fetching */

static lispval hashindex_fetch(fd_index ix,lispval key)
{
  struct FD_HASHINDEX *hx = (fd_hashindex)ix;
  struct FD_STREAM *stream=&(hx->index_stream);
  unsigned int *offdata=hx->index_offdata;
  unsigned char buf[HX_KEYBUF_SIZE];
  struct FD_OUTBUF out = { 0 };
  unsigned int hashval, bucket, n_keys, i, dtype_len, n_values;
  fd_off_t vblock_off; size_t vblock_size;
  FD_CHUNK_REF keyblock;
  FD_INIT_BYTE_OUTBUF(&out,buf,HX_KEYBUF_SIZE);
#if FD_DEBUG_HASHINDEXES
  /* u8_message("Fetching the key %q from %s",key,hx->indexid); */
#endif
  /* If the index doesn't have oddkeys and you're looking up some feature (pair)
     whose slotid isn't in the slotids, the key isn't in the table. */
  if ((!((hx->hashindex_format)&(FD_HASHINDEX_ODDKEYS))) && (PAIRP(key))) {
    lispval slotid = FD_CAR(key);
    if ((SYMBOLP(slotid)) || (OIDP(slotid))) {
      int code = fd_slotid2code(&(hx->index_slotcodes),slotid);
      if (code < 0) {
#if FD_DEBUG_HASHINDEXES
        u8_message("The slotid %q isn't indexed in %s, returning {}",
                   slotid,hx->indexid);
#endif
        fd_close_outbuf(&out);
        return EMPTY;}}}
  if ((hx->hashindex_format)&(FD_HASHINDEX_DTYPEV2))
    out.buf_flags |= FD_USE_DTYPEV2;
  dtype_len = write_zkey(hx,&out,key); {}
  hashval = hash_bytes(out.buffer,dtype_len);
  bucket = hashval%(hx->index_n_buckets);
  if (offdata)
    keyblock = fd_get_chunk_ref(offdata,hx->index_offtype,bucket,
                                hx->index_n_buckets);
  else keyblock=fd_fetch_chunk_ref(stream,256,hx->index_offtype,bucket,0);
  if (fd_bad_chunk(&keyblock)) {
    fd_close_outbuf(&out);
    return FD_ERROR_VALUE;}
  else if (keyblock.size==0) {
    fd_close_outbuf(&out);
    return EMPTY;}
  else {
    unsigned char keybuf[HX_KEYBUF_SIZE];
    struct FD_INBUF keystream={0};
    if (keyblock.size<HX_KEYBUF_SIZE) {
      FD_INIT_INBUF(&keystream,keybuf,HX_KEYBUF_SIZE,FD_STATIC_BUFFER);}
    struct FD_INBUF *opened=
      fd_open_block(stream,&keystream,keyblock.off,keyblock.size,1);
    if (opened==NULL) {
      fd_close_outbuf(&out);
      return FD_ERROR_VALUE;}
    n_keys = fd_read_zint(&keystream);
    i = 0; while (i<n_keys) {
      int key_len = fd_read_zint(&keystream);
      if ((key_len == dtype_len) &&
          (memcmp(keystream.bufread,out.buffer,dtype_len)==0)) {
        keystream.bufread = keystream.bufread+key_len;
        n_values = fd_read_zint(&keystream);
        if (n_values==0) {
          fd_close_inbuf(&keystream);
          fd_close_outbuf(&out);
          return EMPTY;}
        else if (n_values==1) {
          lispval value = read_zvalue(hx,&keystream);
          fd_close_inbuf(&keystream);
          fd_close_outbuf(&out);
          return value;}
        else {
          vblock_off = (fd_off_t)fd_read_zint(&keystream);
          vblock_size = (size_t)fd_read_zint(&keystream);
          fd_close_inbuf(&keystream);
          fd_close_outbuf(&out);
          return read_values(hx,key,n_values,vblock_off,vblock_size);}}
      else {
        keystream.bufread = keystream.bufread+key_len;
        n_values = fd_read_zint(&keystream);
        if (n_values==0) {}
        else if (n_values==1) {
          int code = fd_read_zint(&keystream);
          if (code==0) {
            lispval val = fd_read_dtype(&keystream);
            fd_decref(val);}
          else fd_read_zint(&keystream);}
        else {
          fd_read_zint(&keystream);
          fd_read_zint(&keystream);}}
      i++;}
    fd_close_inbuf(&keystream);}
  fd_close_outbuf(&out);
  return EMPTY;
}

static FD_CHUNK_REF read_value_block
(fd_hashindex hx,lispval key,
 int n_values,FD_CHUNK_REF chunk,
 int *n_readp,lispval *values,int *consp)
{
  fd_stream stream = &(hx->index_stream);
  int n_read=*n_readp;
  FD_CHUNK_REF result = {-1,-1};
  fd_off_t vblock_off = chunk.off;
  size_t  vblock_size = chunk.size;
  struct FD_INBUF instream={0};
  unsigned char stackbuf[HX_VALBUF_SIZE];
  if ( vblock_size < HX_VALBUF_SIZE ) {
    FD_INIT_INBUF(&instream,stackbuf,HX_VALBUF_SIZE,0);}
  else if (! FD_USE_MMAP) {
    unsigned char *usebuf = u8_malloc(vblock_size);
    FD_INIT_INBUF(&instream,usebuf,HX_VALBUF_SIZE,FD_HEAP_BUFFER);}
  else {}
  fd_inbuf vblock = fd_open_block(stream,&instream,vblock_off,vblock_size,1);
  if (vblock == NULL) {
    if (hx->index_flags & FD_STORAGE_REPAIR) {
      u8_log(LOG_WARN,"BadBlockRef",
             "Couldn't open value block (%d/%d) at %lld+%lld in %s for %q",
             n_read,n_values,vblock_off,vblock_size,hx->index_source,key);
      result.off=0; result.size=0;
      return result;}
    else {
      u8_seterr("BadBlockRef","read_value_block/hashindex",
                u8_mkstring("Couldn't open value block (%d/%d) at %lld+%lld in %s for %q",
                            n_read,n_values,vblock_off,vblock_size,hx->index_source,key));
      return result;}}
  fd_off_t next_off;
  ssize_t next_size=-1;
  int i=0, atomicp = 1;
  ssize_t n_elts = fd_read_zint(vblock);
  if (n_elts<0) {
    fd_close_inbuf(vblock);
    return result;}
  i = 0; while ( (i<n_elts) && (n_read < n_values) ) {
    lispval val = read_zvalue(hx,vblock);
    if (FD_ABORTP(val)) {
      if (!(atomicp)) *consp=1;
      *n_readp = n_read;
      fd_close_inbuf(vblock);
      return result;}
    else if (CONSP(val)) atomicp = 0;
    else {}
    values[n_read]=val;
    n_read++;
    i++;}
  /* For vblock continuation pointers, we make the size be first,
     so that we don't need to store an offset if it's zero. */
  next_size = fd_read_zint(vblock);
  if (next_size<0) {}
  else if (next_size)
    next_off = fd_read_zint(vblock);
  else next_off = 0;
  if ( (next_size<0) || (next_off < 0)) {
    *n_readp = n_read;
    if (!(atomicp)) *consp=0;
    fd_close_inbuf(vblock);
    return result;}
  result.off=next_off; result.size=next_size;
  *n_readp = n_read;
  if (!(atomicp)) *consp=1;
  fd_close_inbuf(vblock);
  return result;
}

static lispval read_values
(fd_hashindex hx,lispval key,int n_values,
 fd_off_t vblock_off,size_t vblock_size)

{
  struct FD_CHOICE *result = fd_alloc_choice(n_values);
  FD_CHUNK_REF chunk_ref = { vblock_off, vblock_size };
  lispval *values = (lispval *)FD_XCHOICE_DATA(result);
  int consp = 0, n_read=0;
  while ( (chunk_ref.off>0) && (n_read < n_values) )
    chunk_ref=read_value_block(hx,key,n_values,chunk_ref,
                               &n_read,values,&consp);
  if (chunk_ref.off<0) {
    u8_byte buf[64];
    fd_seterr("HashIndexError","read_values",
              u8_sprintf(buf,64,"reading %d values from %s",
                         n_values,hx->indexid),
              key);
    result->choice_size=n_read;
    fd_decref_ptr(result);
    return FD_ERROR;}
  else if (n_read != n_values) {
    u8_logf(LOG_WARN,"InconsistentValueSize",
            "In '%s', the number of stored values "
            "for %q, %lld != %lld (expected)",
            hx->indexid,key,n_read,n_values);
    /* This makes freeing the pointer work */
    result->choice_size=n_read;
    fd_seterr("InconsistentValueSize","read_values",NULL,key);
    fd_decref_ptr(result);
    return FD_ERROR;}
  else if (n_values == 1) {
    lispval v = values[0];
    fd_incref(v);
    u8_big_free(result);
    return v;}
  else {
    return fd_init_choice
      (result,n_values,NULL,
       FD_CHOICE_DOSORT|FD_CHOICE_REALLOC|
       ((consp)?(FD_CHOICE_ISCONSES):
        (FD_CHOICE_ISATOMIC)));}
}


static int hashindex_fetchsize(fd_index ix,lispval key)
{
  fd_hashindex hx = (fd_hashindex)ix;
  fd_stream stream = &(hx->index_stream);
  unsigned int *offdata=hx->index_offdata;
  struct FD_OUTBUF out = { 0 }; unsigned char buf[64];
  unsigned int hashval, bucket, n_keys, i, dtype_len, n_values;
  FD_CHUNK_REF keyblock;
  FD_INIT_BYTE_OUTBUF(&out,buf,64);
  if ((hx->hashindex_format)&(FD_HASHINDEX_DTYPEV2))
    out.buf_flags = out.buf_flags|FD_USE_DTYPEV2;
  dtype_len = write_zkey(hx,&out,key);
  hashval = hash_bytes(out.buffer,dtype_len);
  bucket = hashval%(hx->index_n_buckets);
  if (offdata)
    keyblock = fd_get_chunk_ref(offdata,hx->index_offtype,bucket,
                                hx->index_n_buckets);
  else keyblock = fd_fetch_chunk_ref
         (&(hx->index_stream),256,hx->index_offtype,bucket,0);
  if ( (keyblock.off<0) || (keyblock.size<0) )
    return -1;
  else if (keyblock.size == 0)
    return keyblock.size;
  else {
    struct FD_INBUF keystream={0};
    struct FD_INBUF *opened=
      fd_open_block(stream,&keystream,keyblock.off,keyblock.size,1);
    if (opened==NULL)
      return -1;
    n_keys = fd_read_zint(&keystream);
    i = 0; while (i<n_keys) {
      int key_len = fd_read_zint(&keystream);
      n_values = fd_read_zint(&keystream);
      /* vblock_off = */ (void)(fd_off_t)fd_read_zint(&keystream);
      /* vblock_size = */ (void)(size_t)fd_read_zint(&keystream);
      if (key_len!=dtype_len)
        keystream.bufread = keystream.bufread+key_len;
      else if (memcmp(keystream.bufread,out.buffer,dtype_len)==0) {
        fd_close_inbuf(&keystream);
        fd_close_outbuf(&out);
        return n_values;}
      else keystream.bufread = keystream.bufread+key_len;
      i++;}
    fd_close_inbuf(&keystream);}
  fd_close_outbuf(&out);
  return 0;
}


/* Fetching multiple keys */

static int sort_ks_by_bucket(const void *k1,const void *k2)
{
  struct KEY_SCHEDULE *ks1 = (struct KEY_SCHEDULE *)k1;
  struct KEY_SCHEDULE *ks2 = (struct KEY_SCHEDULE *)k2;
  if (ks1->ksched_bucket<ks2->ksched_bucket) return -1;
  else if (ks1->ksched_bucket>ks2->ksched_bucket) return 1;
  else return 0;
}

static int sort_ks_by_refoff(const void *k1,const void *k2)
{
  struct KEY_SCHEDULE *ks1 = (struct KEY_SCHEDULE *)k1;
  struct KEY_SCHEDULE *ks2 = (struct KEY_SCHEDULE *)k2;
  if (ks1->ksched_chunk.off<ks2->ksched_chunk.off) return -1;
  else if (ks1->ksched_chunk.off>ks2->ksched_chunk.off) return 1;
  else return 0;
}

static int sort_vs_by_refoff(const void *v1,const void *v2)
{
  struct VALUE_SCHEDULE *vs1 = (struct VALUE_SCHEDULE *)v1;
  struct VALUE_SCHEDULE *vs2 = (struct VALUE_SCHEDULE *)v2;
  if (vs1->vsched_chunk.off<vs2->vsched_chunk.off) return -1;
  else if (vs1->vsched_chunk.off>vs2->vsched_chunk.off) return 1;
  else return 0;
}

static int match_keybuf(u8_string buf,int size,
                        struct KEY_SCHEDULE *ksched,
                        u8_string keyreps)
{
  return ((size == ksched->ksched_dtsize) &&
          (memcmp(keyreps+ksched->ksched_keyoff,buf,size)==0));
}

static lispval *fetchn(struct FD_HASHINDEX *hx,int n,const lispval *keys)
{
  if (n == 0) return NULL;

  lispval *values = u8_big_alloc_n(n,lispval);
  /* This is a buffer where we write keybuf representations of all of the
     keys, which let's use do memcmp to match them to on-disk data */
  struct FD_OUTBUF keysbuf = { 0 };
  /* This is used to fetch information about keys, sorted to be linear */
  struct KEY_SCHEDULE *ksched = u8_big_alloc_n(n,struct KEY_SCHEDULE);
  /* This is used to fetch the chained values in the table, also
     sorted to be linear. */
  struct VALUE_SCHEDULE *vsched = u8_big_alloc_n(n,struct VALUE_SCHEDULE);
  if ( (values==NULL) || (ksched==NULL) || (vsched==NULL) ) {
    u8_seterr(fd_MallocFailed,"hashindex_fetchn",NULL);
    if (values) u8_big_free(values);
    if (ksched) u8_big_free(ksched);
    if (vsched) u8_big_free(vsched);
    return NULL;}

  unsigned int *offdata = hx->index_offdata;
  unsigned char *keyreps;
  int i = 0, n_entries = 0, vsched_size = 0;
  size_t vbuf_size=0;
  int oddkeys = ((hx->hashindex_format)&(FD_HASHINDEX_ODDKEYS));
  fd_stream stream = &(hx->index_stream);
  fd_use_slotcodes(& hx->index_slotcodes);
#if FD_DEBUG_HASHINDEXES
  u8_message("Reading %d keys from %s",n,hx->indexid);
#endif
  /* Initialize sized based on assuming 32 bytes per key */
  FD_INIT_BYTE_OUTPUT(&keysbuf,n*32);
  if ((hx->hashindex_format)&(FD_HASHINDEX_DTYPEV2))
    keysbuf.buf_flags = keysbuf.buf_flags|FD_USE_DTYPEV2;
  /* Fill out a fetch schedule, computing hashes and buckets for each
     key.  If we have an offsets table, we compute the offsets during
     this phase, otherwise we defer to an additional loop.

     This also writes out DTYPE representations for all of the keys
     and we use a direct memcmp to match requested keys to byte ranges
     in fetched buckets. */
  while (i<n) {
    lispval key = keys[i];
    int dt_start = keysbuf.bufwrite-keysbuf.buffer;
    int dt_size, bucket;
    /* If the index doesn't have oddkeys and you're looking up some feature (pair)
       whose slotid isn't in the slotids, the key isn't in the table. */
    if ((!oddkeys) && (PAIRP(key))) {
      lispval slotid = FD_CAR(key);
      if ((SYMBOLP(slotid)) || (OIDP(slotid))) {
        int code = fd_slotid2code(&(hx->index_slotcodes),slotid);
        if (code < 0) {
          values[i++]=EMPTY;
          continue;}}}
    ksched[n_entries].ksched_i = i;
    ksched[n_entries].ksched_key = key;
    ksched[n_entries].ksched_keyoff = dt_start;
    write_zkey(hx,&keysbuf,key);
    dt_size = (keysbuf.bufwrite-keysbuf.buffer)-dt_start;
    ksched[n_entries].ksched_dtsize = dt_size;
    ksched[n_entries].ksched_bucket = bucket=
      hash_bytes(keysbuf.buffer+dt_start,dt_size)%(hx->index_n_buckets);
    if (offdata) {
      /* Because we have an offsets table, we can use fd_get_chunk_ref,
         which doesn't touch the stream, and use it immediately. */
      ksched[n_entries].ksched_chunk =
        fd_get_chunk_ref(offdata,hx->index_offtype,bucket,
                         hx->index_n_buckets);
      size_t keyblock_size = ksched[n_entries].ksched_chunk.size;
      if (keyblock_size==0) {
        /* It is empty, so we don't even need to handle this entry. */
        values[i]=EMPTY;
        /* We don't need to keep its dtype representation around either,
           so we reset the key stream. */
        keysbuf.bufwrite = keysbuf.buffer+dt_start;}
      else n_entries++;}
    else n_entries++;
    i++;}
  keyreps=keysbuf.buffer;
  fd_release_slotcodes(& hx->index_slotcodes );
  if (offdata == NULL) {
    int write_at = 0;
    /* When fetching bucket references, we sort the schedule first, so that
       we're accessing them in order in the file. */
    qsort(ksched,n_entries,sizeof(struct KEY_SCHEDULE),
          sort_ks_by_bucket);
    i = 0; while (i<n_entries) {
      ksched[i].ksched_chunk = fd_fetch_chunk_ref
        (stream,256,hx->index_offtype,ksched[i].ksched_bucket,0);
      size_t keyblock_size=ksched[i].ksched_chunk.size;
      if (keyblock_size==0) {
        values[ksched[i].ksched_i]=EMPTY;
        i++;}
      else if (write_at == i) {
        write_at++;
        i++;}
      else {
        ksched[write_at++]=ksched[i++];}}
    n_entries = write_at;}
  /* We now have the entries of all the keyblocks we're touching, so
     we sort them for serial access. */
  qsort(ksched,n_entries,sizeof(struct KEY_SCHEDULE),
        sort_ks_by_refoff);
  {
    unsigned char keyblock_buf[HX_KEYBUF_SIZE];
    struct FD_INBUF keyblock={0};
    FD_INIT_INBUF(&keyblock,keyblock_buf,HX_KEYBUF_SIZE,FD_STATIC_BUFFER);
    int bucket = -1, j = 0, k = 0, n_keys=0;
    const unsigned char *keyblock_start=NULL;
    while (j<n_entries) {
      int found = 0;
      fd_off_t blockpos = ksched[j].ksched_chunk.off;
      fd_size_t blocksize = ksched[j].ksched_chunk.size;
      if (ksched[j].ksched_bucket != bucket) {
        /* If we're in a new bucket, open it as input */
        if (fd_open_block(stream,&keyblock,blockpos,blocksize,1) == NULL) {
          if (hx->index_flags & FD_STORAGE_REPAIR) {
            u8_log(LOG_WARN,"BadBlockRef",
                   "Couldn't open bucket %d at %lld+%lld in %s for %q",
                   bucket,blockpos,blocksize,hx->index_source,
                   ksched[j].ksched_key);
            bucket=ksched[j].ksched_bucket;
            while (ksched[j].ksched_bucket == bucket) j++;
            continue;}
          else {
            u8_seterr("BadBlockRef","hashindex_fetchn",u8_mkstring
                      ("Couldn't open bucket %d at %lld+%lld in %s for %q",
                       bucket,blockpos,blocksize,hx->index_source,
                       ksched[j].ksched_key));
            fd_close_inbuf(&keyblock);
            u8_big_free(ksched);
            u8_big_free(vsched);
            fd_close_outbuf(&keysbuf);
            fd_release_slotcodes(& hx->index_slotcodes );
            return NULL;}}
        keyblock_start = keyblock.bufread;
        /* And initialize bucket position and limit */
        k=0; n_keys = fd_read_zint(&keyblock);
        keyblock_start = keyblock.bufread;}
      else {
        /* We could be smarter here, but it's probably not worth it */
        keyblock.bufread=keyblock_start;
        k=0;}
      while (k<n_keys) {
        int n_vals;
        fd_size_t dtsize = fd_read_zint(&keyblock);
        if (match_keybuf(keyblock.bufread,dtsize,&ksched[j],keyreps)) {
          found = 1;
          keyblock.bufread = keyblock.bufread+dtsize;
          n_vals = fd_read_zint(&keyblock);
          if (n_vals==0)
            values[ksched[j].ksched_i]=EMPTY;
          else if (n_vals==1)
            /* Single values are stored inline in the keyblocks */
            values[ksched[j].ksched_i]=read_zvalue(hx,&keyblock);
          else {
            /* Populate the values schedule for that key */
            fd_off_t block_off = fd_read_zint(&keyblock);
            fd_size_t block_size = fd_read_zint(&keyblock);
            struct FD_CHOICE *result = fd_alloc_choice(n_vals);
            /* Track the max vblock size for later buffer allocation */
            if (block_size>vbuf_size) vbuf_size=block_size;
            FD_SET_CONS_TYPE(result,fd_choice_type);
            result->choice_size = n_vals;
            values[ksched[j].ksched_i]=(lispval)result;
            vsched[vsched_size].vsched_i = ksched[j].ksched_i;
            vsched[vsched_size].vsched_chunk.off = block_off;
            vsched[vsched_size].vsched_chunk.size = block_size;
            vsched[vsched_size].vsched_write = (lispval *)FD_XCHOICE_DATA(result);
            vsched[vsched_size].vsched_atomicp = 1;
            vsched_size++;}
          /* Advance the key index in case we have other keys to read
             from this bucket */
          k++;
          /* This breaks out the loop iterating over the keys in this
             bucket. */
          break;}
        else {
          /* Skip this key */
          keyblock.bufread = keyblock.bufread+dtsize;
          n_vals = fd_read_zint(&keyblock);
          if (n_vals==0) {}
          else if (n_vals==1) {
            /* Read the one inline value */
            /* TODO: replace with skip_zvalue */
            lispval v = read_zvalue(hx,&keyblock);
            fd_decref(v);}
          else {
            /* Skip offset information */
            fd_read_zint(&keyblock);
            fd_read_zint(&keyblock);}}
        k++;}
      if (!(found))
        values[ksched[j].ksched_i]=EMPTY;
      j++;}
    fd_close_inbuf(&keyblock);}
  /* Now we're done with the ksched */
  u8_big_free(ksched);
  {
    struct FD_INBUF vblock={0}, bigvblock={};
    unsigned char valbuf[HX_VALBUF_SIZE];
    FD_INIT_INBUF(&vblock,valbuf,HX_VALBUF_SIZE,0);
    while (vsched_size) {
      qsort(vsched,vsched_size,sizeof(struct VALUE_SCHEDULE),
            sort_vs_by_refoff);
      i = 0; while (i<vsched_size) {
        int j = 0, n_vals;
        fd_size_t next_size;
        off_t off = vsched[i].vsched_chunk.off;
        size_t size = vsched[i].vsched_chunk.size;
        fd_inbuf valstream =
          ( (FD_USE_MMAP) && (size > vblock.buflen) &&
            (size > fd_bigbuf_threshold) ) ?
          ( fd_open_block(stream,&bigvblock,off,size,1) ) :
          ( fd_open_block(stream,&vblock,off,size,1) );
        n_vals = fd_read_zint(valstream);
        while (j<n_vals) {
          lispval v = read_zvalue(hx,valstream);
          if (CONSP(v)) vsched[i].vsched_atomicp = 0;
          *((vsched[i].vsched_write)++) = v;
          j++;}
        next_size = fd_read_zint(valstream);
        if (next_size) {
          vsched[i].vsched_chunk.size = next_size;
          vsched[i].vsched_chunk.off = fd_read_zint(valstream);}
        else {
          vsched[i].vsched_chunk.size = 0;
          vsched[i].vsched_chunk.off = 0;}
        i++;}
      {
        struct VALUE_SCHEDULE *read = &(vsched[0]), *write = &(vsched[0]);
        struct VALUE_SCHEDULE *limit = read+vsched_size;
        vsched_size = 0; while (read<limit) {
          if (read->vsched_chunk.size) {
            *write++= *read++;
            vsched_size++;}
          else {
            /* We're now done with this value, so we finalize it,
               using fd_init_choice to sort it.  Note that (rarely),
               fd_init_choice will free what was passed to it (if it
               had zero or one element), so the real result is what is
               returned and not what was passed in. */
            int index = read->vsched_i, atomicp = read->vsched_atomicp;
            struct FD_CHOICE *result = (struct FD_CHOICE *)values[index];
            int n_values = result->choice_size;
            lispval realv = fd_init_choice(result,n_values,NULL,
                                           FD_CHOICE_DOSORT|
                                           ((atomicp)?(FD_CHOICE_ISATOMIC):
                                            (FD_CHOICE_ISCONSES))|
                                           FD_CHOICE_REALLOC);
            values[index]=realv;
            read++;}}}}
    fd_close_inbuf(&vblock);
    fd_close_inbuf(&bigvblock);}
  u8_big_free(vsched);
#if FD_DEBUG_HASHINDEXES
  u8_message("Finished reading %d keys from %s",n,hx->indexid);
#endif
  fd_close_outbuf(&keysbuf);
  return values;
}

/* This is the handler exposed by the index handler struct */
static lispval *hashindex_fetchn(fd_index ix,int n,const lispval *keys)
{
  return fetchn((fd_hashindex)ix,n,keys);
}


/* Getting all keys */

static int sort_blockrefs_by_off(const void *v1,const void *v2)
{
  struct FD_CHUNK_REF *br1 = (struct FD_CHUNK_REF *)v1;
  struct FD_CHUNK_REF *br2 = (struct FD_CHUNK_REF *)v2;
  if (br1->off<br2->off) return -1;
  else if (br1->off>br2->off) return 1;
  else return 0;
}

static lispval *hashindex_fetchkeys(fd_index ix,int *n)
{
  lispval *results = NULL;
  struct FD_HASHINDEX *hx = (struct FD_HASHINDEX *)ix;
  fd_stream s = &(hx->index_stream);
  unsigned int *offdata = hx->index_offdata;
  fd_offset_type offtype = hx->index_offtype;
  int i = 0, n_buckets = (hx->index_n_buckets), n_to_fetch = 0;
  int total_keys = 0, key_count = 0, buckets_len, results_len;
  FD_CHUNK_REF *buckets;
  fd_lock_stream(s);
  total_keys = fd_read_4bytes(fd_start_read(s,16));
  if (total_keys==0) {
    fd_unlock_stream(s);
    *n = 0;
    return NULL;}
  buckets = u8_big_alloc_n(total_keys,FD_CHUNK_REF);
  buckets_len=total_keys;
  if (buckets == NULL)
    return NULL;
  else {
    results = u8_big_alloc_n(total_keys,lispval);
    results_len=total_keys;}
  if (results == NULL) {
    fd_unlock_stream(s);
    u8_big_free(buckets);
    u8_seterr(fd_MallocFailed,"hashindex_fetchkeys",NULL);
    return NULL;}
  /* If we have chunk offsets in memory, we don't need to keep the
     stream locked while we get them. */
  if (offdata) {
    fd_unlock_stream(s);
    while (i<n_buckets) {
      FD_CHUNK_REF ref =
        fd_get_chunk_ref(offdata,offtype,i,hx->index_n_buckets);
      if (ref.size>0) {
        if (n_to_fetch >= buckets_len) {
          u8_logf(LOG_WARN,"BadKeyCount",
                  "Bad key count in %s: %d",ix->indexid,total_keys);
          buckets=u8_big_realloc_n(buckets,n_buckets,FD_CHUNK_REF);
          buckets_len=n_buckets;}
        buckets[n_to_fetch++]=ref;}
      i++;}}
  else {
    while (i<n_buckets) {
      FD_CHUNK_REF ref = fd_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) {
        if (n_to_fetch >= buckets_len) {
          u8_logf(LOG_WARN,"BadKeyCount",
                  "Bad key count in %s: %d",ix->indexid,total_keys);
          buckets=u8_big_realloc_n(buckets,n_buckets,FD_CHUNK_REF);
          buckets_len=n_buckets;}
        buckets[n_to_fetch++]=ref;}
      i++;}
    fd_unlock_stream(s);}
  if (n_to_fetch > total_keys) {
    /* If total_keys is wrong, resize results to n_to_fetch */
    results = u8_big_realloc_n(results,n_to_fetch,lispval);
    results_len = n_to_fetch;}
  qsort(buckets,n_to_fetch,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  unsigned char keyblock_buf[HX_KEYBUF_SIZE];
  struct FD_INBUF keyblock={0};
  FD_INIT_INBUF(&keyblock,keyblock_buf,HX_KEYBUF_SIZE,0);
  i = 0; while (i<n_to_fetch) {
    int j = 0, n_keys;
    if (!fd_open_block(s,&keyblock,buckets[i].off,buckets[i].size,1)) {
      fd_unlock_stream(s);
      fd_decref_elts(results,key_count);
      u8_big_free(buckets);
      u8_big_free(results);
      fd_close_inbuf(&keyblock);
      *n=-1;
      return NULL;}
    n_keys = fd_read_zint(&keyblock);
    while (j<n_keys) {
      lispval key; int n_vals;
      ssize_t dtype_len = fd_read_zint(&keyblock); /* IGNORE size */
      if (dtype_len<0) {
        fd_seterr(fd_UnexpectedEOD,"",hx->indexid,FD_VOID);
        key=FD_ERROR_VALUE;}
      else {
        const unsigned char *key_end = keyblock.bufread+dtype_len;
        key = read_key(hx,&keyblock);
        if (FD_ABORTP(key)) keyblock.bufread=key_end;}
      if ( (n_vals = fd_read_zint(&keyblock)) < 0) {
        fd_seterr(fd_UnexpectedEOD,"",hx->indexid,FD_VOID);
        key=FD_ERROR_VALUE;}
      if (!(FD_TROUBLEP(key))) {
        if (key_count >= results_len) {
          results=u8_big_realloc(results,LISPVEC_BYTELEN(results_len*2));
          results_len = results_len * 2;
          if (results == NULL) {
            fd_unlock_stream(s);
            u8_seterr(u8_MallocFailed,"hashindex_fetchkeys",hx->indexid);
            fd_decref_elts(results,key_count);
            u8_big_free(buckets);
            u8_big_free(results);
            fd_close_inbuf(&keyblock);
            *n=-1;
            return NULL;}}
        results[key_count++]=key;}
      if (n_vals<0) {}
      else if (n_vals==0) {}
      else if (n_vals==1) {
        int code = fd_read_zint(&keyblock);
        if (code==0) {
          lispval val = fd_read_dtype(&keyblock);
          fd_decref(val);}
        else fd_read_zint(&keyblock);}
      else {
        fd_read_zint(&keyblock);
        fd_read_zint(&keyblock);}
      if (FD_ABORTP(key)) {
        if ( hx->index_flags & FD_STORAGE_REPAIR ) {
          fd_clear_errors(0);
          u8_log(LOG_CRIT,"CorruptedHashIndex",
                 "Error reading %d keys @%lld+%lld in %s",
                 n_keys,buckets[i].off,buckets[i].size,hx->index_source);
          j=n_keys;}
        else {
          lispval off_pair = fd_init_pair(NULL,FD_INT(buckets[i].off),
                                          FD_INT(buckets[i].size));
          fd_seterr("CorruptedHashIndex","hashindex_fetchkeys",
                    hx->indexid,off_pair);
          fd_decref(off_pair);
          fd_close_inbuf(&keyblock);
          u8_big_free(buckets);
          fd_decref_elts(results,key_count);
          u8_big_free(results);
          *n = -1;
          return NULL;}}
      else j++;}
    i++;}
  fd_close_inbuf(&keyblock);
  u8_big_free(buckets);
  *n = key_count;
  return results;
}

static void free_keysizes(struct FD_KEY_SIZE *sizes,int n)
{
  int i=0; while (i<n) {
    lispval key = sizes[i++].keysize_key;
    fd_decref(key);}
}

static struct FD_KEY_SIZE *hashindex_fetchinfo(fd_index ix,fd_choice filter,int *n)
{
  struct FD_KEY_SIZE *sizes = NULL;
  struct FD_HASHINDEX *hx = (struct FD_HASHINDEX *)ix;
  fd_stream s = &(hx->index_stream);
  unsigned int *offdata = hx->index_offdata;
  fd_offset_type offtype = hx->index_offtype;
  int i = 0, n_buckets = (hx->index_n_buckets), n_to_fetch = 0;
  int total_keys = 0, key_count = 0, buckets_len, sizes_len;
  FD_CHUNK_REF *buckets;
  fd_lock_stream(s);
  total_keys = fd_read_4bytes(fd_start_read(s,16));
  if (total_keys==0) {
    fd_unlock_stream(s);
    *n = 0;
    return NULL;}
  buckets = u8_big_alloc_n(total_keys,FD_CHUNK_REF);
  buckets_len=total_keys;
  if (buckets == NULL)
    return NULL;
  else {
    sizes = u8_big_alloc_n(total_keys,FD_KEY_SIZE);
    sizes_len=total_keys;}
  if (sizes == NULL) {
    fd_unlock_stream(s);
    u8_big_free(buckets);
    u8_seterr(fd_MallocFailed,"hashindex_fetchinfo",NULL);
    return NULL;}
  /* If we have chunk offsets in memory, we don't need to keep the
     stream locked while we get them. */
  if (offdata) {
    fd_unlock_stream(s);
    while (i<n_buckets) {
      FD_CHUNK_REF ref =
        fd_get_chunk_ref(offdata,offtype,i,hx->index_n_buckets);
      if (ref.size>0) {
        if (n_to_fetch >= buckets_len) {
          u8_logf(LOG_WARN,"BadKeyCount",
                  "Bad key count in %s: %d",ix->indexid,total_keys);
          buckets=u8_big_realloc_n(buckets,n_buckets,FD_CHUNK_REF);
          buckets_len=n_buckets;}
        buckets[n_to_fetch++]=ref;}
      i++;}}
  else {
    while (i<n_buckets) {
      FD_CHUNK_REF ref = fd_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) {
        if (n_to_fetch >= buckets_len) {
          u8_logf(LOG_WARN,"BadKeyCount",
                  "Bad key count in %s: %d",ix->indexid,total_keys);
          buckets=u8_big_realloc_n(buckets,n_buckets,FD_CHUNK_REF);
          buckets_len=n_buckets;}
        buckets[n_to_fetch++]=ref;}
      i++;}
    fd_unlock_stream(s);}
  if (n_to_fetch > total_keys) {
    /* If total_keys is wrong, resize results to n_to_fetch */
    sizes = u8_big_realloc_n(sizes,n_to_fetch,struct FD_KEY_SIZE);
    sizes_len = n_to_fetch;}
  qsort(buckets,n_to_fetch,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  unsigned char keyblock_buf[HX_KEYBUF_SIZE];
  struct FD_INBUF keyblock={0};
  FD_INIT_INBUF(&keyblock,keyblock_buf,HX_KEYBUF_SIZE,0);
  i = 0; while (i<n_to_fetch) {
    int j = 0, n_keys;
    if (!fd_open_block(s,&keyblock,buckets[i].off,buckets[i].size,1)) {
      fd_unlock_stream(s);
      u8_big_free(buckets);
      free_keysizes(sizes,key_count);
      u8_big_free(sizes);
      fd_close_inbuf(&keyblock);
      *n=-1;
      return NULL;}
    n_keys = fd_read_zint(&keyblock);
    while (j<n_keys) {
      lispval key; int n_vals;
      ssize_t dtype_len = fd_read_zint(&keyblock); /* IGNORE size */
      if (dtype_len<0) {
        fd_seterr(fd_UnexpectedEOD,"",hx->indexid,FD_VOID);
        key=FD_ERROR_VALUE;}
      else {
        const unsigned char *key_end = keyblock.bufread+dtype_len;
        key = read_key(hx,&keyblock);
        if (FD_ABORTP(key)) keyblock.bufread=key_end;}
      if ( (n_vals = fd_read_zint(&keyblock)) < 0) {
        fd_seterr(fd_UnexpectedEOD,"",hx->indexid,FD_VOID);
        key=FD_ERROR_VALUE;}
      if ( (!(FD_TROUBLEP(key))) &&
           ( (filter == NULL) || (fast_choice_containsp(key,filter)) ) ) {
        if (key_count >= sizes_len) {
          sizes=u8_big_realloc(sizes,2*sizes_len*sizeof(struct FD_KEY_SIZE));
          sizes_len = sizes_len * 2;}
        if (sizes == NULL) {
          fd_unlock_stream(s);
          u8_big_free(buckets);
          free_keysizes(sizes,key_count);
          u8_big_free(sizes);
          fd_close_inbuf(&keyblock);
          *n=-1;
          u8_seterr(u8_MallocFailed,"hashindex_fetchinfo",hx->indexid);
          return NULL;}
        sizes[key_count].keysize_key = key;
        sizes[key_count].keysize_count = n_vals;
        key_count++;}
      if (n_vals<0) {}
      else if (n_vals==0) {}
      else if (n_vals==1) {
        int code = fd_read_zint(&keyblock);
        if (code==0) {
          lispval val = fd_read_dtype(&keyblock);
          fd_decref(val);}
        else fd_read_zint(&keyblock);}
      else {
        fd_read_zint(&keyblock);
        fd_read_zint(&keyblock);}
      if (FD_ABORTP(key)) {
        if ( hx->index_flags & FD_STORAGE_REPAIR ) {
          fd_clear_errors(0);
          u8_log(LOG_CRIT,"CorruptedHashIndex",
                 "Error reading key @%lld+%lld in %s",
                 buckets[i].off,buckets[i].size,hx->index_source);
          j=n_keys;}
        else {
          lispval off_pair = fd_init_pair(NULL,FD_INT(buckets[i].off),
                                          FD_INT(buckets[i].size));
          fd_seterr("CorruptedHashIndex","hashindex_fetchinfo",
                    hx->indexid,off_pair);
          fd_decref(off_pair);
          fd_close_inbuf(&keyblock);
          u8_big_free(buckets);
          free_keysizes(sizes,key_count);
          u8_big_free(sizes);
          *n = -1;
          return NULL;}}
      else j++;}
    i++;}
  fd_close_inbuf(&keyblock);
  u8_big_free(buckets);
  *n = key_count;
  return sizes;
}

#if 0
static
struct FD_KEY_SIZE *hashindex_fetchinfo(fd_index ix,fd_choice filter,int *n)
{
  struct FD_HASHINDEX *hx = (struct FD_HASHINDEX *)ix;
  fd_stream s = &(hx->index_stream);
  unsigned int *offdata = hx->index_offdata;
  fd_offset_type offtype = hx->index_offtype;
  fd_inbuf ins = fd_readbuf(s);
  int i = 0, n_buckets = (hx->index_n_buckets), total_keys, buckets_len;
  int n_to_fetch = 0, key_count = 0;
  fd_lock_stream(s);
  fd_setpos(s,16); total_keys = fd_read_4bytes(ins);
  if (total_keys==0) {
    fd_unlock_stream(s);
    *n = 0;
    return NULL;}
  FD_CHUNK_REF *buckets = u8_big_alloc_n(total_keys,FD_CHUNK_REF);
  if (buckets)
    buckets_len=total_keys;
  else {
    fd_unlock_stream(s);
    u8_seterr(fd_MallocFailed,"hashindex_fetchinfo/buckets",NULL);
    *n = -1;
    return NULL;}
  struct FD_KEY_SIZE *sizes = u8_big_alloc_n(total_keys,FD_KEY_SIZE);
  if (sizes == NULL) {
    fd_unlock_stream(s);
    if (buckets)  u8_big_free(buckets);
    u8_seterr(fd_MallocFailed,"hashindex_fetchinfo/sizes",NULL);
    *n = -1;
    return NULL;}
  /* If we don't have chunk offsets in memory, we keep the stream
     locked while we get them. */
  if (offdata == NULL) {
    while (i<n_buckets) {
      FD_CHUNK_REF ref = fd_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) {
        if (n_to_fetch >= buckets_len) {
          u8_logf(LOG_WARN,"BadKeyCount",
                  "Bad key count in %s: %d",ix->indexid,total_keys);
          buckets=u8_realloc_n(buckets,n_buckets,FD_CHUNK_REF);
          buckets_len=n_buckets;}
        buckets[n_to_fetch++]=ref;}
      i++;}
    fd_unlock_stream(s);}
  else {
    fd_unlock_stream(s);
    int ref_i=0; while (ref_i<n_buckets) {
      FD_CHUNK_REF ref = fd_get_chunk_ref
        (offdata,offtype,ref_i,hx->index_n_buckets);
      if (ref.size>0) {
        if (n_to_fetch >= buckets_len) {
          u8_logf(LOG_WARN,"BadKeyCount",
                  "Bad key count in %s: %d",ix->indexid,total_keys);
          /* Allocate the whole n_buckets if something goes wrong */
          buckets = u8_big_realloc_n(buckets,n_buckets,FD_CHUNK_REF);
          sizes   = u8_big_realloc_n(sizes,n_buckets,FD_KEY_SIZE);
          buckets_len=n_buckets;}
        buckets[n_to_fetch++]=ref;}
      ref_i++;}}
  qsort(buckets,n_to_fetch,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  struct FD_INBUF keyblkstrm={0};
  i = 0; while (i<n_to_fetch) {
    int j = 0, n_keys;
    if (!fd_open_block(s,&keyblkstrm,buckets[i].off,buckets[i].size,1)) {
      fd_close_inbuf(&keyblkstrm);
      u8_big_free(buckets);
      *n=-1;
      return NULL;}
    n_keys = fd_read_zint(&keyblkstrm);
    while (j<n_keys) {
      /* size = */ fd_read_zint(&keyblkstrm);
      lipsval key = read_key(hx,&keyblkstrm);
      int n_vals = fd_read_zint(&keyblkstrm);
      if ( (filter == NULL) || (fast_choice_containsp(key,filter)) ) {
        sizes[key_count].keysize_key = key;
        sizes[key_count].keysize_count = n_vals;
        key_count++;}
      else fd_decref(key);
      if (n_vals==0) {}
      else if (n_vals==1) {
        int code = fd_read_zint(&keyblkstrm);
        if (code==0) {
          lispval val = fd_read_dtype(&keyblkstrm);
          fd_decref(val);}
        else fd_read_zint(&keyblkstrm);}
      else {
        fd_read_zint(&keyblkstrm);
        fd_read_zint(&keyblkstrm);}
      j++;}
    i++;}
  fd_close_inbuf(&keyblkstrm);
  u8_big_free(buckets);
  *n=key_count;
  return sizes;
}
#endif

static void hashindex_getstats(struct FD_HASHINDEX *hx,
                               int *nf,int *max,int *singles,int *n2sum)
{
  fd_stream s = &(hx->index_stream);
  fd_inbuf ins = fd_readbuf(s);
  int i = 0, n_buckets = (hx->index_n_buckets);
  int n_to_fetch = 0, total_keys = 0, buckets_len;
  unsigned int *offdata = hx->index_offdata;
  fd_offset_type offtype = hx->index_offtype;
  int max_keyblock_size=0;
  FD_CHUNK_REF *buckets;
  fd_lock_index(hx);
  fd_lock_stream(s);
  fd_setpos(s,16); total_keys = fd_read_4bytes(ins);
  if (total_keys==0) {
    fd_unlock_stream(s);
    *nf = 0; *max = 0; *singles = 0; *n2sum = 0;
    return;}
  buckets = u8_big_alloc_n(total_keys,FD_CHUNK_REF);
  /* If we don't have chunk offsets in memory, we keep the stream
     locked while we get them. */
  if (offdata) {
    fd_unlock_stream(s);
    while (i<n_buckets) {
      FD_CHUNK_REF ref =
        fd_get_chunk_ref(offdata,offtype,i,hx->index_n_buckets);
      if (ref.size>0) {
        if (n_to_fetch >= buckets_len) {
          u8_logf(LOG_WARN,"BadKeyCount",
                  "Bad key count in %s: %d",hx->indexid,total_keys);
          buckets=u8_realloc_n(buckets,n_buckets,FD_CHUNK_REF);
          buckets_len=n_buckets;}
        buckets[n_to_fetch++]=ref;}
      if (ref.size>max_keyblock_size)
        max_keyblock_size=ref.size;
      i++;}}
  else {
    while (i<n_buckets) {
      FD_CHUNK_REF ref = fd_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) buckets[n_to_fetch++]=ref;
      if (ref.size>max_keyblock_size) max_keyblock_size=ref.size;
      i++;}
    fd_unlock_stream(s);}
  *nf = n_to_fetch;
  /* Now we actually unlock it if we kept it locked. */
  fd_unlock_index(hx);
  qsort(buckets,n_to_fetch,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  struct FD_INBUF keyblkstrm={0};
  int n_keys;
  i = 0; while (i<n_to_fetch) {
    fd_open_block(s,&keyblkstrm,buckets[i].off,buckets[i].size,1);
    n_keys = fd_read_zint(&keyblkstrm);
    if (n_keys==1) (*singles)++;
    if (n_keys>(*max)) *max = n_keys;
    *n2sum = *n2sum+(n_keys*n_keys);
    i++;}
  fd_close_inbuf(&keyblkstrm);
  if (buckets) u8_big_free(buckets);
}


/* Cache setting */

static void hashindex_setcache(struct FD_HASHINDEX *hx,int level)
{
  int chunk_ref_size = get_chunk_ref_size(hx);
  if (chunk_ref_size<0) {
    u8_logf(LOG_WARN,fd_CorruptedIndex,
            "Index structure invalid: %s",hx->indexid);
    return;}
  fd_stream stream = &(hx->index_stream);
  size_t bufsize  = fd_stream_bufsize(stream);
  size_t use_bufsize = fd_getfixopt(hx->index_opts,"BUFSIZE",fd_driver_bufsize);

  /* Update the bufsize */
  if (bufsize < use_bufsize)
    fd_setbufsize(stream,use_bufsize);

  if (level >= 2) {
    if (hx->index_offdata)
      return;
    else {
      fd_stream s = &(hx->index_stream);
      size_t n_buckets = hx->index_n_buckets;
      unsigned int *buckets, *newmmap;
#if FD_USE_MMAP
      newmmap=
        mmap(NULL,(n_buckets*chunk_ref_size)+256,
             PROT_READ,MMAP_FLAGS,s->stream_fileno,0);
      if ((newmmap == NULL) || (newmmap == MAP_FAILED)) {
        u8_logf(LOG_WARN,u8_strerror(errno),
                "hashindex_setcache:mmap %s",hx->index_source);
        hx->index_offdata = NULL;
        errno = 0;}
      else hx->index_offdata = buckets = newmmap+64;
#else
      ht_buckets = u8_big_alloc_n
        (chunk_ref_size*(hx->index_n_buckets),unsigned int);
      fd_lock_stream(s);
      stream_start_read(s);
      fd_setpos(s,256);
      retval = fd_read_ints
        (s,(chunk_ref_size/4)*(hx->index_n_buckets),ht_buckets);
      if (retval<0) {
        u8_logf(LOG_WARN,u8_strerror(errno),
                "hashindex_setcache:read offsets %s",hx->index_source);
        errno = 0;}
      else hx->index_offdata = ht_buckets;
      fd_unlock_stream(s);
#endif
    }}

  else if (level < 2) {

    int retval=0;
    unsigned int *offdata = hx->index_offdata;
    if (offdata == NULL)
      return;
    else hx->index_offdata=NULL;
    /* TODO: We should be more careful before unmapping or freeing
       this, since somebody could still have a pointer to it. */
#if FD_USE_MMAP
    retval = munmap(offdata-64,((hx->index_n_buckets)*chunk_ref_size)+256);
    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),
              "hashindex_setcache:munmap %s",hx->index_source);
      errno = 0;}
#else
    u8_big_free(offdata);
#endif
  }
  else {}
}


/* Populating a hash index
   This writes data into the hashtable but ignores what is already there.
   It is commonly used when initializing a hash index. */

static int sort_br_by_bucket(const void *p1,const void *p2)
{
  struct BUCKET_REF *ps1 = (struct BUCKET_REF *)p1;
  struct BUCKET_REF *ps2 = (struct BUCKET_REF *)p2;
  if (ps1->bucketno<ps2->bucketno) return -1;
  else if (ps1->bucketno>ps2->bucketno) return 1;
  else return 0;
}

static int sort_br_by_off(const void *p1,const void *p2)
{
  struct BUCKET_REF *ps1 = (struct BUCKET_REF *)p1;
  struct BUCKET_REF *ps2 = (struct BUCKET_REF *)p2;
  if (ps1->bck_ref.off<ps2->bck_ref.off) return -1;
  else if (ps1->bck_ref.off>ps2->bck_ref.off) return 1;
  else return 0;
}


/* COMMIT */

/* General design:

   We start by swapping the current adds and edits into into separate
   hashtables to allow other threads to write new adds and edits to
   the index. We then process these tables in order to populate a
   COMMIT_SCHEDULE for each key we're going to change.

   Handle edits first with 'process_edits', converting drops into
   stores by fetching the current values and adding them as stores to
   the commit schedule. (Drops are supposed to be rare).

   Pointers from the edits table are incref'd before being saved to
   the commit schedule. Also, adds are integrated into the commits
   from the edits table.

   Then handle adds with 'process_adds', pushing them onto the commit
   schedule, adding them to the current values in the schedule if they
   weren't integrated into results from the edits table.

   Pointers from the adds table **are not** incref'd but the keyvalues
   in the adds table are VOIDed to avoid double pointers.

   After this, the adds and edits can be reset, to zero because we're
   not going to use them again.

   We then figure out which buckets each of the keys goes in and sort
   the commit schedule by bucket. For each bucket, we then get the
   location of the bucket data on disk (either from offdata or by
   going to disk). This is stored in an BUCKET_REF array, which
   we sort by file position and read the buckets we're changing in
   order. Each bucket is read from disk and stored into an array of
   KEYBUCKET structs.

   A keybucket structure consists of an array of KEYENTRY structures
   which contain information about each key in the bucket. It also
   contains a buffer with the DTYPE representations of all of the
   keys, to avoid regenerating them.

   There will often be fewer buckets than keys and so the
   COMMIT_SCHEDULE array may be longer than the KEYBUCKET
   array. However, the number of KEYENTRY structures in a bucket will
   often be greater than the number of corresponding keys in the
   commit schedule because it includes entries for keys in the bucket
   which aren't being changed.

   We then sort both the commit schedule and the keybuckets by bucket
   number. This allows us to march along them in parallel and operate
   on a keybucket together with all its changing keys.

   We extend each keybucket (using 'update_keybucket') with the
   additional keys or new values. This is where the actual values are
   written and the corresponding chunk locations stored in the
   KEYENTRY structs for each key. When a key has only a single value,
   it is stored in the keyblock itself. Otherwise, a separate value
   block is written which includes a reference to the value block from
   the keyblock read from disk.

   After the values are all written, we write the keybucket itself to
   disk. The new location of the keybucket is saved in the same
   BUCKET_REF array used to fetch the keybuckets in the first
   place.

   After the keybuckets (and their values) have been written to disk,
   we may (if fd_acid_files is not zero) write recovery data in the
   form of the changed buckets from the BUCKET_REF array. Once
   this is written to disk, we write the position where the recovery
   information started to the end of the file and set the initial word
   of the file to the code FD_HASHINDEX_TO_RECOVER.

   After the recovery data is written, we write the new offset data
   and change the initial word. Then we truncate the file to remove
   the recovery data. And that's it.
*/

static int sort_cs_by_bucket(const void *k1,const void *k2)
{
  struct COMMIT_SCHEDULE *ks1 = (struct COMMIT_SCHEDULE *)k1;
  struct COMMIT_SCHEDULE *ks2 = (struct COMMIT_SCHEDULE *)k2;
  if (ks1->commit_bucket<ks2->commit_bucket) return -1;
  else if (ks1->commit_bucket>ks2->commit_bucket) return 1;
  else return 0;
}

static int sort_kb_by_bucket(const void *k1,const void *k2)
{
  struct KEYBUCKET **kb1 = (struct KEYBUCKET **)k1;
  struct KEYBUCKET **kb2 = (struct KEYBUCKET **)k2;
  if ((*kb1)->kb_bucketno<(*kb2)->kb_bucketno) return -1;
  else if ((*kb1)->kb_bucketno>(*kb2)->kb_bucketno) return 1;
  else return 0;
}

static int process_stores(struct FD_HASHINDEX *hx,
                          struct FD_CONST_KEYVAL *stores,int n_stores,
                          struct COMMIT_SCHEDULE *s,
                          int i)
{
  int oddkeys = ((hx->hashindex_format)&(FD_HASHINDEX_ODDKEYS));
  int store_i = 0; while ( store_i < n_stores) {
    lispval key = stores[store_i].kv_key, val = stores[store_i].kv_val;
    s[i].commit_key     = key;
    s[i].commit_values  = val;
    s[i].free_values    = 0;
    s[i].commit_replace = 1;
    store_i++;
    i++;}

  /* Record if there were any odd keys */
  if (oddkeys) hx->hashindex_format |= (FD_HASHINDEX_ODDKEYS);

  return i;
}

static int process_drops(struct FD_HASHINDEX *hx,
                         struct FD_CONST_KEYVAL *drops,int n_drops,
                         struct COMMIT_SCHEDULE *s,
                         int sched_i)
{
  lispval *to_fetch = u8_big_alloc_n(n_drops,lispval); int n_fetches = 0;
  int *fetch_scheds = u8_big_alloc_n(n_drops,unsigned int);
  int oddkeys = ((hx->hashindex_format)&(FD_HASHINDEX_ODDKEYS));

  /* For all of the drops, we need to fetch their values to do the
     drop, so we accumulate them in to_fetch[]. We're not checking the
     cache for those values (which could be there), because the cache
     might have additional changes made before the commit. (Got
     that?) */
  int drop_i = 0; while ( drop_i < n_drops) {
    lispval key = drops[drop_i].kv_key, val = drops[drop_i].kv_val;

    s[sched_i].commit_key = key;
    /* We temporarily store the dropped values in the
       schedule. We'll replace them with the call to fd_difference
       below. */
    s[sched_i].commit_values = val;
    s[sched_i].free_values   = 0;
    to_fetch[n_fetches]      = key;
    fetch_scheds[n_fetches++]  = sched_i;
    s[sched_i].commit_replace = 1;
    drop_i++;
    sched_i++;}

  /* Record if there were any odd (non (slotid . val)) keys */
  if (oddkeys) hx->hashindex_format |= (FD_HASHINDEX_ODDKEYS);

  /* Get the current values of all the keys you're dropping, to turn
     the drops into stores. */
  lispval *drop_vals = fetchn(hx,n_fetches,to_fetch);

  int j = 0; while (j<n_fetches) {
    int sched_ref   = fetch_scheds[j];
    lispval ondisk  = drop_vals[j];
    lispval todrop  = s[sched_ref].commit_values;
    lispval reduced = fd_difference(ondisk,todrop);
    s[sched_ref].commit_values = reduced;
    s[sched_ref].free_values   = 1;
    j++;}

  fd_decref_vec(drop_vals,n_fetches);
  u8_big_free(drop_vals);
  u8_big_free(fetch_scheds);
  u8_big_free(to_fetch);

  return sched_i;
}

static int process_adds(struct FD_HASHINDEX *hx,
                        struct FD_CONST_KEYVAL *adds,int n_adds,
                        struct COMMIT_SCHEDULE *s,
                        int i)
{
  int add_i = 0;
  int oddkeys = ((hx->hashindex_format)&(FD_HASHINDEX_ODDKEYS));
  while ( add_i < n_adds ) {
    lispval key = adds[add_i].kv_key;
    lispval val = adds[add_i].kv_val;
    if (FD_EMPTYP(val)) {
      add_i++;
      continue;}
    else if ((oddkeys==0) && (PAIRP(key)) &&
        ((OIDP(FD_CAR(key))) || (SYMBOLP(FD_CAR(key))))) {
      lispval slotid = FD_CAR(key);
      int code = fd_slotid2code(&(hx->index_slotcodes),slotid);
      if (code < 0) oddkeys = 1;}
    s[i].commit_key     = key;
    if (PRECHOICEP(val)) {
      s[i].commit_values  = fd_make_simple_choice(val);
      s[i].free_values    = 1;}
    else {
      s[i].commit_values  = val;
      s[i].free_values    = 0;}
    s[i].commit_replace = 0;
    add_i++;
    i++;}
  if (oddkeys)
    hx->hashindex_format |= (FD_HASHINDEX_ODDKEYS);
  return i;
}

FD_FASTOP void parse_keybucket(fd_hashindex hx,struct KEYBUCKET *kb,
                               fd_inbuf in,int n_keys)
{
  int i = 0; struct KEYENTRY *base_entry = &(kb->kb_elt0);
  kb->kb_n_keys = n_keys;
  while (i<n_keys) {
    int dt_size = fd_read_zint(in), n_values;
    struct KEYENTRY *entry = base_entry+i;
    entry->ke_dtrep_size = dt_size;
    entry->ke_dtstart    = in->bufread;
    in->bufread          = in->bufread+dt_size;
    entry->ke_nvals      = n_values = fd_read_zint(in);
    if (n_values==0)
      entry->ke_values = EMPTY;
    else if (n_values==1) {
      lispval v = read_zvalue(hx,in);
      entry->ke_values = v;
      if (FD_EMPTYP(v)) entry->ke_nvals = 0;}
    else {
      entry->ke_values    = VOID;
      entry->ke_vref.off  = fd_read_zint(in);
      entry->ke_vref.size = fd_read_zint(in);}
    i++;}
}

FD_FASTOP FD_CHUNK_REF write_value_block
(struct FD_HASHINDEX *hx,fd_stream stream,fd_oidcoder oc,
 lispval values,lispval extra,
 fd_off_t cont_off,fd_off_t cont_size,
 fd_off_t startpos)
{
  struct FD_OUTBUF *outstream = fd_writebuf(stream);
  FD_CHUNK_REF retval; fd_off_t endpos = startpos;
  if (CHOICEP(values)) {
    int full_size = FD_CHOICE_SIZE(values)+((VOIDP(extra))?0:1);
    endpos = endpos+fd_write_zint(outstream,full_size);
    if (!(VOIDP(extra)))
      endpos = endpos+write_zvalue(hx,outstream,oc,extra);
    {DO_CHOICES(value,values)
        endpos = endpos+write_zvalue(hx,outstream,oc,value);}}
  else if (VOIDP(extra)) {
    endpos = endpos+fd_write_zint(outstream,1);
    endpos = endpos+write_zvalue(hx,outstream,oc,values);}
  else {
    endpos = endpos+fd_write_zint(outstream,2);
    endpos = endpos+write_zvalue(hx,outstream,oc,extra);
    endpos = endpos+write_zvalue(hx,outstream,oc,values);}
  endpos = endpos+fd_write_zint(outstream,cont_size);
  if (cont_size)
    endpos = endpos+fd_write_zint(outstream,cont_off);
  CHECK_POS(endpos,stream);
  retval.off  = startpos;
  retval.size = endpos-startpos;
  return retval;
}

/* This adds new entries to a keybucket, writing value blocks to the
   file where neccessary (more than one value). */
FD_FASTOP fd_off_t update_keybucket
(fd_hashindex hx,fd_stream stream,
 fd_slotcoder sc,fd_oidcoder oc,
 struct KEYBUCKET *kb,
 struct COMMIT_SCHEDULE *schedule,int i,int j,
 fd_outbuf newkeys,int *new_valueblocksp,
 fd_off_t endpos,ssize_t maxpos)
{
  int k = i, free_keyvecs = 0;
  int _keyoffs[16], _keysizes[16], *keyoffs, *keysizes;
  if (PRED_FALSE((j-i)>16) )  {
    keyoffs = u8_alloc_n((j-i),int);
    keysizes = u8_alloc_n((j-i),int);
    free_keyvecs = 1;}
  else {keyoffs=_keyoffs; keysizes=_keysizes;}
  while (k<j) {
    int off;
    keyoffs[k-i]=off = newkeys->bufwrite-newkeys->buffer;
    write_zkey_wsc(sc,newkeys,schedule[k].commit_key);
    keysizes[k-i]=(newkeys->bufwrite-newkeys->buffer)-off;
    k++;}
  k = i; while (k<j) {
    unsigned char *keydata = newkeys->buffer+keyoffs[k-i];
    struct KEYENTRY *ke = &(kb->kb_elt0);
    int keysize = keysizes[k-i];
    int key_i = 0, n_keys = kb->kb_n_keys, n_values;
    while (key_i<n_keys) {
      if ((ke[key_i].ke_dtrep_size)!= keysize) key_i++;
      else if (memcmp(keydata,ke[key_i].ke_dtstart,keysize)) key_i++;
      else if (schedule[k].commit_replace) {
        /* The key is already in there, but we are ignoring
           it's current value.  If key has more than one associated
           values, we write a value block, otherwise we store the value
           in the key entry. */
        int n_values = FD_CHOICE_SIZE(schedule[k].commit_values);
        ke[key_i].ke_nvals = n_values;
        if (n_values==0) {
          ke[key_i].ke_values = EMPTY;
          ke[key_i].ke_vref.off = 0;
          ke[key_i].ke_vref.size = 0;}
        else if (n_values==1) {
          /* If there is only one value we're saving, so we'll write
             it as part of the keyblock (that's what being in
             .ke_values means) */
          lispval current = ke[key_i].ke_values;
          ke[key_i].ke_values = fd_incref(schedule[k].commit_values);
          ke[key_i].ke_vref.off = 0;
          ke[key_i].ke_vref.size = 0;
          fd_decref(current);}
        else {
          ke[key_i].ke_values = VOID;
          ke[key_i].ke_vref=
            write_value_block(hx,stream,oc,
                              schedule[k].commit_values,VOID,
                              0,0,endpos);
          (*new_valueblocksp)++;
          endpos = ke[key_i].ke_vref.off+ke[key_i].ke_vref.size;}
        if (endpos>=maxpos) {
          if (free_keyvecs) {
            u8_free(keyoffs);
            u8_free(keysizes);
            u8_seterr(fd_DataFileOverflow,"update_keybucket",
                      u8_mkstring("%s: %lld >= %lld",
                                  hx->indexid,endpos,maxpos));
            return -1;}}
        break;}
      else {
        /* The key is already in there and has values, so we write
           a value block with a continuation pointer to the current
           value block and update the key entry.  */
        int n_values = ke[key_i].ke_nvals;
        int n_values_added = FD_CHOICE_SIZE(schedule[k].commit_values);
        ke[key_i].ke_nvals = n_values+n_values_added;
        if ( (n_values==0) && (n_values_added == 1) ) {
          /* This is the case where there was an entry with no values
             and we're adding one value */
          lispval one_val = schedule[k].commit_values;
          ke[key_i].ke_values=one_val;
          fd_incref(one_val);}
        else if (n_values==1) {
          /* This is the special case is where there is one current
             value and we are adding to that.  The value block we
             write must contain both the values we are adding and the
             current singleton value.  The current singleton value is
             passed as the fourth (extra) argument to
             write_value_block.  */
          lispval current = ke[key_i].ke_values;
          ke[key_i].ke_vref=
            write_value_block(hx,stream,oc,
                              schedule[k].commit_values,
                              current,0,0,endpos);
          endpos = ke[key_i].ke_vref.off+ke[key_i].ke_vref.size;
          /* We void this because it's now on disk */
          fd_decref(current);
          ke[key_i].ke_values = VOID;}
        else {
          ke[key_i].ke_vref=
            write_value_block(hx,stream,oc,
                              schedule[k].commit_values,VOID,
                              ke[key_i].ke_vref.off,ke[key_i].ke_vref.size,
                              endpos);
          if ( (ke[key_i].ke_values != VOID) &&
               (ke[key_i].ke_values != EMPTY) )
            u8_logf(LOG_WARN,"NotVoid",
                    "This value for key %d is %q, not VOID/EMPTY as expected",
                    key_i,ke[key_i].ke_values);
          endpos = ke[key_i].ke_vref.off+ke[key_i].ke_vref.size;}
        if (endpos>=maxpos) {
          if (free_keyvecs) {
            u8_free(keyoffs);
            u8_free(keysizes);}
          u8_seterr(fd_DataFileOverflow,"update_keybucket",
                    u8_mkstring("%s: %lld >= %lld",
                                hx->indexid,endpos,maxpos));
          return -1;}
        break;}}
    /* This is the case where we are adding a new key to the bucket. */
    if (key_i == n_keys) { /* This should always be true */
      ke[n_keys].ke_dtrep_size = keysize;
      ke[n_keys].ke_nvals = n_values =
        FD_CHOICE_SIZE(schedule[k].commit_values);
      ke[n_keys].ke_dtstart = newkeys->buffer+keyoffs[k-i];
      if (n_values==0) ke[n_keys].ke_values = EMPTY;
      else if (n_values==1)
        /* As above, we don't need to incref this because any value in
           it comes from the key schedule, so we won't decref it when
           we reclaim the keybuckets. */
        ke[n_keys].ke_values = fd_incref(schedule[k].commit_values);
      else {
        ke[n_keys].ke_values = VOID;
        ke[n_keys].ke_vref=
          write_value_block(hx,stream,oc,
                            schedule[k].commit_values,VOID,
                            0,0,endpos);
        endpos = ke[key_i].ke_vref.off+ke[key_i].ke_vref.size;
        if (endpos>=maxpos) {
          if (free_keyvecs) {
            u8_free(keyoffs);
            u8_free(keysizes);}
          u8_seterr(fd_DataFileOverflow,"update_keybucket",
                    u8_mkstring("%s: %lld >= %lld",
                                hx->indexid,endpos,maxpos));
          return -1;}}
      kb->kb_n_keys++;}
    k++;}
  if (free_keyvecs) {
    u8_free(keyoffs);
    u8_free(keysizes);}
  return endpos;
}

FD_FASTOP fd_off_t write_keybucket
(fd_hashindex hx,fd_stream stream,fd_oidcoder oc,
 struct KEYBUCKET *kb,
 fd_off_t endpos,fd_off_t maxpos)
{
  int i = 0, n_keys = kb->kb_n_keys;
  struct KEYENTRY *ke = &(kb->kb_elt0);
  struct FD_OUTBUF *outstream = fd_writebuf(stream);
  endpos = endpos+fd_write_zint(outstream,n_keys);
  while (i<n_keys) {
    int dtype_size = ke[i].ke_dtrep_size, n_values = ke[i].ke_nvals;
    endpos = endpos+fd_write_zint(outstream,dtype_size);
    endpos = endpos+fd_write_bytes(outstream,ke[i].ke_dtstart,dtype_size);
    endpos = endpos+fd_write_zint(outstream,n_values);
    if (n_values==0) {}
    else if (n_values==1) {
      endpos = endpos+write_zvalue(hx,outstream,oc,ke[i].ke_values);
      fd_decref(ke[i].ke_values);
      ke[i].ke_values = VOID;}
    else {
      endpos = endpos+fd_write_zint(outstream,ke[i].ke_vref.off);
      endpos = endpos+fd_write_zint(outstream,ke[i].ke_vref.size);}
    i++;}
  if (endpos>=maxpos) {
    u8_seterr(fd_DataFileOverflow,"write_keybucket",
              u8_mkstring("%s: %lld >= %lld",hx->indexid,endpos,maxpos));
    return -1;}
  return endpos;
}

FD_FASTOP struct KEYBUCKET *read_keybucket
(fd_hashindex hx,fd_stream stream,
 int bucket,FD_CHUNK_REF ref,int extra)
{
  int n_keys;
  struct KEYBUCKET *kb;
  /* We allocate this dynamically because we're going to store it on
     the keybucket. */
  if (ref.size>0) {
    unsigned char *keybuf=u8_malloc(ref.size);
    ssize_t read_result=fd_read_block(stream,keybuf,ref.size,ref.off,1);
    if (read_result<0) {
      fd_seterr("FailedBucketRead","read_keybucket/hashindex",
                hx->indexid,FD_INT(bucket));
      return NULL;}
    else {
      struct FD_INBUF keystream = { 0 };
      FD_INIT_INBUF(&keystream,keybuf,ref.size,0);
      n_keys = fd_read_zint(&keystream);
      kb = (struct KEYBUCKET *)
        u8_malloc(sizeof(struct KEYBUCKET)+
                  sizeof(struct KEYENTRY)*((extra+n_keys)-1));
      kb->kb_bucketno = bucket;
      kb->kb_n_keys   = n_keys;
      kb->kb_keybuf   = keybuf;
      parse_keybucket(hx,kb,&keystream,n_keys);}}
  else {
    kb = (struct KEYBUCKET *)
      u8_malloc(sizeof(struct KEYBUCKET)+
                sizeof(struct KEYENTRY)*(extra-1));
    kb->kb_bucketno = bucket;
    kb->kb_n_keys   = 0;
    kb->kb_keybuf   = NULL;}
  return kb;
}

static int update_hashindex_ondisk
(fd_hashindex hx,lispval metadata,
 unsigned int flags,unsigned int new_keys,
 unsigned int changed_buckets,
 struct BUCKET_REF *bucket_locs,
 struct FD_SLOTCODER *sc,
 struct FD_OIDCODER *oc,
 struct FD_STREAM *stream,
 struct FD_STREAM *head);
static int update_hashindex_metadata(fd_hashindex,lispval,fd_stream,fd_stream);

static void free_keybuckets(int n,struct KEYBUCKET **keybuckets);

static int hashindex_save(struct FD_HASHINDEX *hx,
                          struct FD_INDEX_COMMITS *commits,
                          struct FD_STREAM *stream,
                          struct FD_STREAM *head)
{
  struct FD_CONST_KEYVAL *adds = commits->commit_adds;
  int n_adds = commits->commit_n_adds;
  struct FD_CONST_KEYVAL *drops = commits->commit_drops;
  int n_drops = commits->commit_n_drops;
  struct FD_CONST_KEYVAL *stores = commits->commit_stores;
  int n_stores = commits->commit_n_stores;
  lispval changed_metadata = commits->commit_metadata;
  u8_string fname=hx->index_source;
  struct FD_SLOTCODER sc = fd_copy_slotcodes(&(hx->index_slotcodes));
  struct FD_OIDCODER oc = fd_copy_oidcodes(&(hx->index_oidcodes));
  if (!(u8_file_writablep(fname))) {
    fd_seterr("CantWriteFile","hashindex_save",fname,FD_VOID);
    return -1;}
  fd_lock_index(hx);
  struct BUCKET_REF *bucket_locs;
  fd_offset_type offtype = hx->index_offtype;
  if (!((offtype == FD_B32)||(offtype = FD_B40)||(offtype = FD_B64))) {
    u8_logf(LOG_WARN,CorruptedHashIndex,
            "Bad offset type code=%d for %s",(int)offtype,hx->indexid);
    u8_seterr(CorruptedHashIndex,"hashindex_save/offtype",
              u8_strdup(hx->indexid));
    fd_unlock_index(hx);
    return -1;}

  if (  (n_adds==0) && (n_drops==0) && (n_stores==0) ) {
    if (FD_SLOTMAPP(changed_metadata))
      update_hashindex_metadata(hx,changed_metadata,stream,head);
    fd_unlock_index(hx);
    return 0;}

  int new_keys = 0, n_keys, new_buckets = 0;
  int schedule_max, changed_buckets = 0, total_keys = hx->table_n_keys;
  int new_keyblocks = 0, new_valueblocks = 0;
  ssize_t endpos, maxpos = get_maxpos(hx);
  double started = u8_elapsed_time();
  size_t n_buckets = hx->index_n_buckets;
  unsigned int *offdata = hx->index_offdata;
  schedule_max = n_adds + n_drops + n_stores;
  bucket_locs = u8_big_alloc_n(schedule_max,struct BUCKET_REF);

  /* This is where we write everything to disk */
  int sched_i = 0, bucket_i = 0;
  int schedule_size = 0;
  struct COMMIT_SCHEDULE *schedule=
    u8_big_alloc_n(schedule_max,struct COMMIT_SCHEDULE);

  /* We're going to write DTYPE representations of keys and values,
     so we create streams where they'll all be written to one big buffer
     (which we free at the end). */
  struct FD_OUTBUF out = { 0 }, newkeys = { 0 };
  FD_INIT_BYTE_OUTPUT(&out,1024);
  FD_INIT_BYTE_OUTPUT(&newkeys,schedule_max*16);
  if ((hx->hashindex_format)&(FD_HASHINDEX_DTYPEV2)) {
    out.buf_flags |= FD_USE_DTYPEV2;
    newkeys.buf_flags |= FD_USE_DTYPEV2;}

  /* Get all the keys we need to write and put then in the commit
     schedule */
  schedule_size = process_stores(hx,stores,n_stores,schedule,schedule_size);
  schedule_size = process_drops(hx,drops,n_drops,schedule,schedule_size);
  schedule_size = process_adds(hx,adds,n_adds,schedule,schedule_size);

  /* The commit schedule is now filled and we determine the bucket for
     each key. */
  sched_i = 0; while (sched_i<schedule_size) {
    lispval key = schedule[sched_i].commit_key; int bucket;
    out.bufwrite = out.buffer;
    write_zkey_wsc(&sc,&out,key);
    schedule[sched_i].commit_bucket = bucket =
      hash_bytes(out.buffer,out.bufwrite-out.buffer)%n_buckets;
    sched_i++;}

  /* Get all the bucket locations, sorting the schedule to avoid
     multiple lookups */
  qsort(schedule,schedule_size,sizeof(struct COMMIT_SCHEDULE),
        sort_cs_by_bucket);
  sched_i = 0; bucket_i = 0; while (sched_i<schedule_size) {
    int bucket = schedule[sched_i].commit_bucket;
    int bucket_last_key = sched_i;
    bucket_locs[changed_buckets].bucketno = bucket;
    bucket_locs[changed_buckets].bck_ref = (offdata) ? /* location on disk */
      (fd_get_chunk_ref(offdata,offtype,bucket,hx->index_n_buckets)):
      (fd_fetch_chunk_ref(stream,256,offtype,bucket,1));
    int keys_in_bucket = 1;
    /* Scan over all committed keys in the same bucket */
    while ( (bucket_last_key<schedule_size) &&
            (schedule[bucket_last_key].commit_bucket == bucket) ) {
      bucket_last_key++; keys_in_bucket++;}
    bucket_locs[changed_buckets].max_new = keys_in_bucket;
    sched_i = bucket_last_key;
    changed_buckets++;}

  /* Now we have all the bucket locations, which we'll read in
     order. */
  struct KEYBUCKET **keybuckets=
    u8_big_alloc_n(changed_buckets,struct KEYBUCKET *);
  qsort(bucket_locs,changed_buckets,sizeof(struct BUCKET_REF),
        sort_br_by_off);
  bucket_i = 0; while (bucket_i<changed_buckets) {
    keybuckets[bucket_i]=
      read_keybucket(hx,stream,bucket_locs[bucket_i].bucketno,
                     bucket_locs[bucket_i].bck_ref,
                     bucket_locs[bucket_i].max_new);
    if ((keybuckets[bucket_i]->kb_n_keys)==0) new_buckets++;
    bucket_i++;}

  /* Now all the keybuckets have been read and buckets have been
     created for keys that didn't have buckets before. */
  /* schedule is already sorted by bucket ? */
#if 0
  qsort(schedule,schedule_size,sizeof(struct COMMIT_SCHEDULE),
        sort_cs_by_bucket);
#endif
  qsort(keybuckets,changed_buckets,sizeof(struct KEYBUCKET *),
        sort_kb_by_bucket);
  /* bucket_locs is currently sorted by offset, so we resort it by
     bucket because we're going to iterate by bucket and write the new
     bucket locations into it. */
  qsort(bucket_locs,changed_buckets,sizeof(struct BUCKET_REF),
        sort_br_by_bucket);

  /* March along the commit schedule (keys) and keybuckets (buckets)
     in parallel, updating each bucket.  This is where values are
     written out and their new offsets stored in the schedule. */
  sched_i = 0; bucket_i = 0; endpos = fd_endpos(stream);
  while (sched_i<schedule_size) {
    struct KEYBUCKET *kb = keybuckets[bucket_i];
    int bucket = schedule[sched_i].commit_bucket;
    int j = sched_i, cur_keys = kb->kb_n_keys;
    if (FD_EXPECT_FALSE(bucket != kb->kb_bucketno)) {
      u8_log(LOG_CRIT,"HashIndexError",
             "Bucket at sched_i=%d/%d was %d != %d (expected) in %s",
             sched_i,schedule_size,bucket,kb->kb_bucketno,hx->indexid);}
    while ((j<schedule_size) && (schedule[j].commit_bucket == bucket)) j++;
    /* This may write values to disk, so we use the returned endpos */
    endpos = update_keybucket(hx,stream,&sc,&oc,kb,schedule,sched_i,j,
                              &newkeys,&new_valueblocks,
                              endpos,maxpos);
    CHECK_POS(endpos,stream);
    new_keys = new_keys+(kb->kb_n_keys-cur_keys);
    {
      fd_off_t startpos = endpos;
      /* This writes the keybucket itself. */
      endpos = write_keybucket(hx,stream,&oc,kb,endpos,maxpos);
      new_keyblocks++;
      if (endpos<0) {
        u8_big_free(bucket_locs);
        u8_big_free(schedule);
        u8_big_free(keybuckets);
        fd_unlock_index(hx);
        fd_close_stream(stream,FD_STREAM_FREEDATA);
        u8_seterr("WriteKeyBucketFailed","hashindex_commit",
                  u8_strdup(hx->indexid));
        return -1;}
      CHECK_POS(endpos,stream);
      bucket_locs[bucket_i].bck_ref.off = startpos;
      bucket_locs[bucket_i].bck_ref.size = endpos-startpos;}
    sched_i = j;
    bucket_i++;}
  fd_flush_stream(stream);

  /* Free all the keybuckets */
  free_keybuckets(changed_buckets,keybuckets);

  /* Now we free the keys and values in the schedule. */
  sched_i = 0; while (sched_i<schedule_size) {
    if (schedule[sched_i].free_values) {
      lispval v = schedule[sched_i].commit_values;
      schedule[sched_i].commit_values = VOID;
      fd_decref(v);}
    sched_i++;}
  u8_big_free(schedule);
  fd_close_outbuf(&out);
  fd_close_outbuf(&newkeys);
  n_keys = schedule_size;

  total_keys += new_keys;
  hx->table_n_keys = total_keys;

  int final_rv = update_hashindex_ondisk
    (hx,changed_metadata,
     hx->hashindex_format,total_keys,
     changed_buckets,bucket_locs,
     &sc,&oc,stream,head);

  fd_update_slotcodes(&(hx->index_slotcodes),&sc);
  fd_update_oidcodes(&(hx->index_oidcodes),&oc);

  /* Free the bucket locations */
  u8_big_free(bucket_locs);
  /* And unlock the index */
  fd_unlock_index(hx);

  if (final_rv < 0)
    u8_logf(LOG_ERR,"HashIndexCommit",
            "Saving header information failed");
  else u8_logf(LOG_INFO,"HashIndexCommit",
               "Saved mappings for %d keys (%d/%d new/total) to %s in %f secs",
               n_keys,new_keys,total_keys,
               hx->indexid,u8_elapsed_time()-started);

  if (final_rv<0)
    return final_rv;
  else return n_keys;
}

static fd_stream get_commit_stream(fd_index ix,struct FD_INDEX_COMMITS *commit)
{
  if (commit->commit_stream)
    return commit->commit_stream;
  else if (u8_file_writablep(ix->index_source)) {
    struct FD_STREAM *new_stream =
      fd_init_file_stream(NULL,ix->index_source,FD_FILE_MODIFY,-1,-1);
    /* Lock the file descriptor */
    if (fd_streamctl(new_stream,fd_stream_lockfile,NULL)<0) {
      fd_close_stream(new_stream,FD_STREAM_FREEDATA);
      u8_free(new_stream);
      fd_seterr("CantLockFile","get_commit_stream/hashindex",
                ix->index_source,FD_VOID);
      return NULL;}
    commit->commit_stream = new_stream;
    return new_stream;}
  else {
    fd_seterr("CantWriteFile","get_commit_stream/hashindex",
              ix->index_source,FD_VOID);
    return NULL;}
}

static void release_commit_stream(fd_index ix,struct FD_INDEX_COMMITS *commit)
{
  fd_stream stream = commit->commit_stream;
  if (stream == NULL) return;
  else commit->commit_stream=NULL;
  if (fd_streamctl(stream,fd_stream_unlockfile,NULL)<0)
    u8_logf(LOG_WARN,"CantUnLockFile",
            "For commit stream of hashindex %s",ix->indexid);
  fd_close_stream(stream,FD_STREAM_FREEDATA);
  u8_free(stream);
}

static int hashindex_commit(fd_index ix,fd_commit_phase phase,
                            struct FD_INDEX_COMMITS *commits)
{
  struct FD_HASHINDEX *hx = (fd_hashindex) ix;
  int ref_size = get_chunk_ref_size(hx);
  size_t n_buckets = hx->index_n_buckets;
  fd_stream stream = get_commit_stream(ix,commits);
  u8_string source = ix->index_source;
  if (stream == NULL)
    return -1;
  switch (phase) {
  case fd_no_commit:
    u8_seterr("BadCommitPhase(commit_none)","hashindex_commit",
              u8_strdup(ix->indexid));
    return -1;
  case fd_commit_start:
    return fd_write_rollback("hashindex_commit",ix->indexid,source,
                             256+(ref_size*n_buckets));
  case fd_commit_write: {
    struct FD_STREAM *head_stream = NULL;
    if (commits->commit_2phase) {
      size_t recovery_size = 256+(ref_size*n_buckets);
      u8_string commit_file = u8_mkstring("%s.commit",source);
      ssize_t head_saved = fd_save_head(source,commit_file,recovery_size);
      head_stream = (head_saved>=0) ?
        (fd_init_file_stream(NULL,commit_file,FD_FILE_MODIFY,-1,-1)) :
        (NULL);
      if (head_stream == NULL) {
        u8_seterr("CantOpenCommitFile","bigpool_commit",commit_file);
        return -1;}
      else u8_free(commit_file);}
    else head_stream=stream;
    int rv = hashindex_save((struct FD_HASHINDEX *)ix,commits,stream,head_stream);
    if (rv<0) {
      u8_string commit_file = u8_mkstring("%s.commit",source);
      u8_seterr("HashIndexSaveFailed","hashindex_commit",u8_strdup(ix->indexid));
      if (u8_removefile(commit_file)<0)
        u8_log(LOG_CRIT,"RemoveFileFailed","Couldn't remove file %s for %s",
               commit_file,ix->indexid);
      u8_free(commit_file);}
    else if (head_stream != stream) {
      size_t endpos = fd_endpos(stream);
      size_t head_endpos = fd_endpos(head_stream);
      fd_write_8bytes_at(head_stream,endpos,head_endpos);
      fd_flush_stream(head_stream);
      fd_close_stream(head_stream,FD_STREAM_FREEDATA);
      u8_free(head_stream);
      head_stream=NULL;
      commits->commit_phase = fd_commit_sync;}
    else commits->commit_phase = fd_commit_flush;
    return rv;}
  case fd_commit_rollback: {
    u8_string rollback_file = u8_string_append(source,".rollback",NULL);
    if (u8_file_existsp(rollback_file)) {
      ssize_t rv = fd_apply_head(rollback_file,source);
      if (rv<0) {
        u8_log(LOG_CRIT,"RollbackFailed",
               "Couldn't apply rollback %s to %s for %s",
               rollback_file,source,ix->indexid);
        u8_free(rollback_file);
        return -1; }
      u8_free(rollback_file);
      return 1;}
    else {
      u8_seterr("RollbackFailed","hashindex_commit",
                u8_mkstring("The rollback file %s for %s doesn't exist",
                            rollback_file,ix->indexid));
      u8_free(rollback_file);
      return -1;}}
  case fd_commit_sync: {
    u8_string commit_file = u8_mkstring("%s.commit",source);
    ssize_t rv = fd_apply_head(commit_file,source);
    if (rv<0) {
      u8_seterr("FinishCommitFailed","hashindex_commit",
                u8_mkstring("Couldn't apply commit file %s to %s for %s",
                            commit_file,source,ix->indexid));
      u8_free(commit_file);
      return -1;}
    else {
      u8_free(commit_file);
      int rv = load_header(hx,stream);
      if (rv<0) {
        u8_seterr("FinishCommitFailed","hashindex_commit",
                  u8_mkstring("Couldn't reload header for restored %s (%s)",
                              source,ix->indexid));
        return -1;}
      else return 1;}}
  case fd_commit_flush: {
    return 1;}
  case fd_commit_cleanup: {
    if (commits->commit_stream) release_commit_stream(ix,commits);
    u8_string rollback_file = u8_string_append(source,".rollback",NULL);
    int rollback_cleanup = 0, commit_cleanup = 0;
    if (u8_file_existsp(rollback_file))
      rollback_cleanup = u8_removefile(rollback_file);
    else u8_log(LOG_WARN,"MissingRollbackFile",
                "Rollback file %s went missing",rollback_file);
    if (commits->commit_2phase) {
      u8_string commit_file = u8_string_append(source,".commit",NULL);
      if (u8_file_existsp(commit_file))
        commit_cleanup = u8_removefile(commit_file);
      else u8_logf(LOG_WARN,"MissingRollbackFile",
                   "Commit file %s went missing",commit_file);
      u8_free(commit_file);}
    u8_free(rollback_file);
    if ( ( rollback_cleanup < 0) || ( commit_cleanup < 0) )
      return -1;
    else return 1;}
  default: {
    u8_logf(LOG_WARN,"NoPhasedCommit",
            "The index %s doesn't support phased commits",
            ix->indexid);
    return -1;}
  }
}

static void free_keybuckets(int n,struct KEYBUCKET **keybuckets)
{
  int i = 0; while (i<n) {
    struct KEYBUCKET *kb = keybuckets[i++];
    /* We don't need to decref the ke_values of the individual key
       entries, because they're simply copied (without incref) from
       the commit schedule from where they'll be freed. */
    if (kb->kb_keybuf) u8_free(kb->kb_keybuf);
    u8_free(kb);}
  u8_big_free(keybuckets);
}

static int update_hashindex_metadata(fd_hashindex hx,
                                     lispval metadata,
                                     struct FD_STREAM *stream,
                                     struct FD_STREAM *head)
{
  int error=0;
  u8_logf(LOG_WARN,"WriteMetadata",
          "Writing modified metadata for %s",hx->indexid);
  ssize_t metadata_pos = fd_endpos(stream);
  if (metadata_pos>0) {
    fd_outbuf outbuf = fd_writebuf(stream);
    ssize_t new_metadata_size = fd_write_dtype(outbuf,metadata);
    ssize_t metadata_end = fd_getpos(stream);
    if (new_metadata_size<0)
      error=1;
    else {
      if ((metadata_end-metadata_pos) != new_metadata_size) {
        u8_logf(LOG_CRIT,"MetadataSizeIconsistency",
                "There was an inconsistency writing the metadata for %s",
                hx->indexid);}
      fd_write_8bytes_at(head,metadata_pos,FD_HASHINDEX_METADATA_POS);
      fd_write_4bytes_at(head,metadata_end-metadata_pos,FD_HASHINDEX_METADATA_POS+8);}}
  else error=1;
  if (error) {
    u8_seterr("MetaDataWriteError","update_hashindex_metadata/endpos",
              u8_strdup(hx->index_source));
    return -1;}
  else return 1;
}

static int update_hashindex_oidcodes(fd_hashindex hx,fd_oidcoder oc,
                                     struct FD_STREAM *stream,
                                     struct FD_STREAM *head)
{
  int error=0;
  u8_logf(LOG_NOTICE,"WriteOIDMap",
          "Writing modified hashindex oidcodes for %s",hx->indexid);
  ssize_t baseoids_pos = fd_endpos(stream);
  if (baseoids_pos>0) {
    fd_outbuf outbuf = fd_writebuf(stream);
    int len = oc->oids_len;
    struct FD_VECTOR vec;
    FD_INIT_STATIC_CONS(&vec,fd_vector_type);
    vec.vec_length = len;
    vec.vec_free_elts = vec.vec_bigalloc = vec.vec_bigalloc_elts =0;
    vec.vec_elts = oc->baseoids;
    ssize_t new_baseoids_size = fd_write_dtype(outbuf,(lispval)&vec);
    ssize_t baseoids_end = fd_getpos(stream);
    if (new_baseoids_size<0)
      error=1;
    else {
      if ((baseoids_end-baseoids_pos) != new_baseoids_size) {
        u8_logf(LOG_CRIT,"BaseOidsSizeIconsistency",
                "There was an inconsistency writing the base oids for %s",
                hx->indexid);}
      fd_write_8bytes_at(head,baseoids_pos,FD_HASHINDEX_BASEOIDS_POS);
      fd_write_4bytes_at(head,baseoids_end-baseoids_pos,
                         (FD_HASHINDEX_BASEOIDS_POS+8));}}
  else error=1;
  if (error) {
    u8_seterr("MetaDataWriteError","update_hashindex_baseoids/endpos",
              u8_strdup(hx->index_source));
    return -1;}
  else return 1;
}

static int update_hashindex_slotcodes(fd_hashindex hx,
                                      struct FD_SLOTCODER *sc,
                                      struct FD_STREAM *stream,
                                      struct FD_STREAM *head)
{
  int error=0;
  u8_logf(LOG_NOTICE,"WriteOIDMap",
          "Writing modified hashindex slotcodes for %s",hx->indexid);
  ssize_t slotids_pos = fd_endpos(stream);
  if (slotids_pos>0) {
    fd_outbuf outbuf = fd_writebuf(stream);
    ssize_t new_slotids_size = fd_write_dtype(outbuf,(lispval)(sc->slotids));
    ssize_t slotids_end = fd_getpos(stream);
    if (new_slotids_size<0)
      error=1;
    else {
      if ((slotids_end-slotids_pos) != new_slotids_size) {
        u8_logf(LOG_CRIT,"SlotIDsSizeIconsistency",
                "There was an inconsistency saving the slotcodes for %s",
                hx->indexid);}
      fd_write_8bytes_at(head,slotids_pos,FD_HASHINDEX_SLOTIDS_POS);
      fd_write_4bytes_at(head,slotids_end-slotids_pos,
                         (FD_HASHINDEX_SLOTIDS_POS+8));}}
  else error=1;
  if (error) {
    u8_seterr("MetaDataWriteError","update_hashindex_slotocdes/endpos",
              u8_strdup(hx->index_source));
    return -1;}
  else return 1;
}

static int update_hashindex_ondisk
(fd_hashindex hx,lispval metadata,
 unsigned int flags,unsigned int cur_keys,
 unsigned int changed_buckets,
 struct BUCKET_REF *bucket_locs,
 struct FD_SLOTCODER *sc,
 struct FD_OIDCODER *oc,
 struct FD_STREAM *stream,
 struct FD_STREAM *head)
{
  if (FD_SLOTMAPP(metadata))
    update_hashindex_metadata(hx,metadata,stream,head);

  if (sc->n_slotcodes > sc->init_n_slotcodes)
    update_hashindex_slotcodes(hx,sc,stream,head);

  if (oc->n_oids > oc->init_n_oids)
    update_hashindex_oidcodes(hx,oc,stream,head);

  struct FD_OUTBUF *outstream = fd_writebuf(head);

  int i = 0;
  unsigned int *offdata = NULL;
  size_t n_buckets = hx->index_n_buckets;
  unsigned int chunk_ref_size = get_chunk_ref_size(hx);
  ssize_t offdata_byte_length = n_buckets*chunk_ref_size;
#if FD_USE_MMAP
  unsigned int *memblock=
    mmap(NULL,256+offdata_byte_length,
         PROT_READ|PROT_WRITE,
         MMAP_FLAGS,
         head->stream_fileno,
         0);
  if ((memblock==NULL) || (memblock == MAP_FAILED)) {
    u8_graberrno("update_hashindex_ondisk:mmap",u8_strdup(hx->indexid));
    return -1;}
  else offdata = memblock+64;
#else
  size_t offdata_length = n_buckets*chunk_ref_size;
  offdata = u8_big_alloc(offdata_length);
  fd_setpos(head,256);
  int rv = fd_read_ints(head,offdata_length/4,offdata);
  if (rv<0) {
    u8_graberrno("update_hashindex_ondisk:fd_read_ints",u8_strdup(hx->indexid));
    u8_big_free(offdata);
    return -1;}
#endif
  /* Update the buckets if you have them */
  if ((offdata) && (hx->index_offtype == FD_B64)) {
    while (i<changed_buckets) {
      unsigned int word1, word2, word3, bucket = bucket_locs[i].bucketno;
      word1 = (((bucket_locs[i].bck_ref.off)>>32)&(0xFFFFFFFF));
      word2 = (((bucket_locs[i].bck_ref.off))&(0xFFFFFFFF));
      word3 = (bucket_locs[i].bck_ref.size);
#if ((FD_USE_MMAP) && (!(WORDS_BIGENDIAN)))
      offdata[bucket*3]=fd_flip_word(word1);
      offdata[bucket*3+1]=fd_flip_word(word2);
      offdata[bucket*3+2]=fd_flip_word(word3);
#else
      offdata[bucket*3]=word1;
      offdata[bucket*3+1]=word2;
      offdata[bucket*3+2]=word3;
#endif
      i++;}}
  else if ((offdata) && (hx->index_offtype == FD_B32)) {
    while (i<changed_buckets) {
      unsigned int word1, word2, bucket = bucket_locs[i].bucketno;
      word1 = ((bucket_locs[i].bck_ref.off)&(0xFFFFFFFF));
      word2 = (bucket_locs[i].bck_ref.size);
#if ((FD_USE_MMAP) && (!(WORDS_BIGENDIAN)))
      offdata[bucket*2]=fd_flip_word(word1);
      offdata[bucket*2+1]=fd_flip_word(word2);
#else
      offdata[bucket*2]=word1;
      offdata[bucket*2+1]=word2;
#endif
      i++;}}
  else if ((offdata) && (hx->index_offtype == FD_B40)) {
    while (i<changed_buckets) {
      unsigned int word1 = 0, word2 = 0, bucket = bucket_locs[i].bucketno;
      fd_convert_FD_B40_ref(bucket_locs[i].bck_ref,&word1,&word2);
#if ((FD_USE_MMAP) && (!(WORDS_BIGENDIAN)))
      offdata[bucket*2]=fd_flip_word(word1);
      offdata[bucket*2+1]=fd_flip_word(word2);
#else
      offdata[bucket*2]=word1;
      offdata[bucket*2+1]=word2;
#endif
      i++;}}
  /* If you don't have offsets in memory, write them by hand, stepping
     through the file. */
  else if (hx->index_offtype == FD_B64) {
    size_t bucket_start = 256;
    size_t bytes_in_bucket = 3*SIZEOF_INT;
    while (i<changed_buckets) {
      unsigned int bucket_no = bucket_locs[i].bucketno;
      unsigned int bucket_pos = bucket_start+bytes_in_bucket*bucket_no;
      fd_start_write(head,bucket_pos);
      fd_write_8bytes(outstream,bucket_locs[i].bck_ref.off);
      fd_write_4bytes(outstream,bucket_locs[i].bck_ref.size);
      i++;}}
  else if (hx->index_offtype == FD_B32) {
    size_t bucket_start = 256;
    size_t bytes_in_bucket = 2*SIZEOF_INT;
    while (i<changed_buckets) {
      unsigned int bucket_no = bucket_locs[i].bucketno;
      unsigned int bucket_pos = bucket_start+bytes_in_bucket*bucket_no;
      fd_start_write(head,bucket_pos);
      fd_write_4bytes(outstream,bucket_locs[i].bck_ref.off);
      fd_write_4bytes(outstream,bucket_locs[i].bck_ref.size);
      i++;}}
  else {
    /* This is FD_B40 encoding, 2 words */
    size_t bucket_start = 256;
    size_t bytes_in_bucket = 2*SIZEOF_INT;
    while (i<changed_buckets) {
      unsigned int bucket_no = bucket_locs[i].bucketno;
      unsigned int bucket_pos = bucket_start+bytes_in_bucket*bucket_no;
      unsigned int word1 = 0, word2 = 0;
      fd_convert_FD_B40_ref(bucket_locs[i].bck_ref,&word1,&word2);
      fd_start_write(head,bucket_pos);
      fd_write_4bytes(outstream,word1);
      fd_write_4bytes(outstream,word2);
      i++;}}
  /* The offsets have now been updated in memory */
  if (offdata) {
#if (FD_USE_MMAP)
    /* If you have MMAP, make them unwritable which swaps them out to
       the file. */
    int retval = msync(offdata-64,
                       256+offdata_byte_length,
                       MS_SYNC|MS_INVALIDATE);
    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),
              "update_hashindex_ondisk:msync %s",hx->indexid);
      u8_graberrno("update_hashindex_ondisk:msync",u8_strdup(hx->indexid));}
    retval = munmap(offdata-64,256+offdata_byte_length);
    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),
              "update_hashindex_ondisk:munmap %s",hx->indexid);
      u8_graberrno("update_hashindex_ondisk:msync",u8_strdup(hx->indexid));}
#else
    struct FD_OUTBUF *out = fd_start_write(head,256);
    if (hx->index_offtype == FD_B64)
      fd_write_ints(outstream,3*SIZEOF_INT*n_buckets,offdata);
    else fd_write_ints(outstream,2*SIZEOF_INT*n_buckets,offdata);
    u8_big_free(offdata);
#endif
  }

  fd_flush_stream(stream);
  fsync(stream->stream_fileno);
  size_t end_pos = fd_endpos(stream);
  fd_write_8bytes_at(head,end_pos,256-8);

  /* Write any changed flags */
  fd_write_4bytes_at(head,flags,8);
  fd_write_4bytes_at(head,cur_keys,16);
  fd_write_4bytes_at(head,FD_HASHINDEX_MAGIC_NUMBER,0);
  fd_flush_stream(stream);
  fd_flush_stream(head);

  return 0;
}

static void reload_offdata(struct FD_INDEX *ix)
#if FD_USE_MMAP
{
}
#else
{
  struct FD_HASHINDEX *hx=(fd_hashindex)ix;
  unsigned int *offdata=NULL;
  if (hx->index_offdata==NULL)
    return;
  else offdata=hx->index_offdata;
  fd_stream stream = &(hx->index_stream);
  size_t n_buckets = hx->index_n_buckets;
  unsigned int chunk_ref_size = get_chunk_ref_size(hx);
  fd_setpos(s,256);
  int retval = fd_read_ints(stream,int_len,offdata);
  if (retval<0)
    u8_logf(LOG_CRIT,"reload_offdata",
            "Couldn't reload offdata for %s",hx->indexid);
}
#endif


/* Miscellaneous methods */

static void hashindex_close(fd_index ix)
{
  struct FD_HASHINDEX *hx = (struct FD_HASHINDEX *)ix;
  unsigned int chunk_ref_size = get_chunk_ref_size(hx);
  unsigned int *offdata = hx->index_offdata;
  size_t n_buckets = hx->index_n_buckets;
  u8_logf(LOG_DEBUG,"HASHINDEX","Closing hash index %s",ix->indexid);
  fd_lock_index(hx);
  fd_close_stream(&(hx->index_stream),0);
  if (offdata) {
#if FD_USE_MMAP
    int retval=
      munmap(offdata-64,(chunk_ref_size*n_buckets)+256);
    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),
              "hashindex_close:munmap %s",hx->index_source);
      errno = 0;}
#else
    u8_big_free(offdata);
#endif
    hx->index_offdata = NULL;
    hx->index_cache_level = -1;}
  u8_logf(LOG_DEBUG,"HASHINDEX","Closed hash index %s",ix->indexid);
  fd_unlock_index(hx);
}

static void hashindex_recycle(fd_index ix)
{
  struct FD_HASHINDEX *hx = (struct FD_HASHINDEX *)ix;
  if ( (hx->index_offdata) || (hx->index_stream.stream_fileno > 0) )
    hashindex_close(ix);
  fd_recycle_slotcoder(&(hx->index_slotcodes));
  fd_recycle_oidcoder(&(hx->index_oidcodes));
}

/* Creating a hash ksched_i handler */

static int interpret_hashindex_flags(lispval opts)
{
  int flags = 0;
  lispval offtype = fd_intern("OFFTYPE");
  if ( fd_testopt(opts,offtype,fd_intern("B64"))  ||
       fd_testopt(opts,offtype,FD_INT(64))        ||
       fd_testopt(opts,FDSYM_FLAGS,fd_intern("B64")))
    flags |= (FD_B64<<4);
  else if ( fd_testopt(opts,offtype,fd_intern("B40"))  ||
            fd_testopt(opts,offtype,FD_INT(40))        ||
            fd_testopt(opts,FDSYM_FLAGS,fd_intern("B40")) )
    flags |= (FD_B40<<4);
  else if ( fd_testopt(opts,offtype,fd_intern("B32"))  ||
            fd_testopt(opts,offtype,FD_INT(32))        ||
            fd_testopt(opts,FDSYM_FLAGS,fd_intern("B32")))
    flags |= (FD_B32<<4);
  else flags |= (FD_B40<<4);

  if ( fd_testopt(opts,fd_intern("DTYPEV2"),VOID) ||
       fd_testopt(opts,FDSYM_FLAGS,fd_intern("DTYPEV2")) )
    flags |= FD_HASHINDEX_DTYPEV2;

  return flags;
}

static fd_index hashindex_create(u8_string spec,void *typedata,
                                 fd_storage_flags flags,
                                 lispval opts)
{
  int rv = 0;
  lispval metadata_init = fd_getopt(opts,fd_intern("METADATA"),FD_VOID);
  lispval slotids_arg=FD_VOID, slotids_init =
    fd_getopt(opts,fd_intern("SLOTIDS"),VOID);
  lispval baseoids_arg=FD_VOID, baseoids_init =
    fd_getopt(opts,fd_intern("BASEOIDS"),VOID);
  lispval nbuckets_arg = fd_getopt(opts,fd_intern("SLOTS"),
                                   fd_getopt(opts,FDSYM_SIZE,
                                             FD_INT(hashindex_default_size)));
  lispval hashconst = fd_getopt(opts,fd_intern("HASHCONST"),FD_FIXZERO);
  if (!(FD_UINTP(nbuckets_arg))) {
    fd_seterr("InvalidBucketCount","hashindex_create",spec,nbuckets_arg);
    rv = -1;}
  else if (!(FD_INTEGERP(hashconst))) {
    fd_seterr("InvalidHashConst","hashindex_create",spec,hashconst);
    rv = -1;}
  else {
    baseoids_arg = fd_baseoids_arg(baseoids_init);
    if (FD_ABORTP(baseoids_arg)) {
      fd_seterr("BadBaseOIDs","hashindex_create",spec,baseoids_init);
      rv = -1;}
    else {
      slotids_arg = fd_slotids_arg(slotids_init);
      if (FD_ABORTP(slotids_arg)) {
        fd_seterr("BadSlotids","hashindex_create",spec,slotids_init);
        fd_decref(baseoids_arg);
        rv=-1;}}}

  fd_decref(slotids_init);
  fd_decref(baseoids_init);

  lispval metadata = VOID;
  lispval created_symbol = fd_intern("CREATED");
  lispval assembled_symbol = fd_intern("ASSEMBLED");
  lispval init_opts = fd_intern("INITOPTS");
  lispval make_opts = fd_intern("MAKEOPTS");

  if (FD_TABLEP(metadata_init)) {
    metadata = fd_deep_copy(metadata_init);}
  else metadata = fd_make_slotmap(8,0,NULL);

  lispval ltime = fd_make_timestamp(NULL);
  if (!(fd_test(metadata,created_symbol,FD_VOID)))
    fd_store(metadata,created_symbol,ltime);
  fd_store(metadata,assembled_symbol,ltime);
  fd_decref(ltime); ltime = FD_VOID;

  if (!(fd_test(metadata,init_opts,FD_VOID)))
    fd_store(metadata,init_opts,opts);
  fd_store(metadata,make_opts,opts);

  lispval keyslot = fd_getopt(opts,FDSYM_KEYSLOT,FD_VOID);

  if ( (FD_VOIDP(keyslot)) || (FD_FALSEP(keyslot)) ) {}
  else if ( (FD_SYMBOLP(keyslot)) || (FD_OIDP(keyslot)) )
    fd_store(metadata,FDSYM_KEYSLOT,keyslot);
  else u8_log(LOG_WARN,"InvalidKeySlot",
              "Not initializing keyslot of %s to %q",spec,keyslot);

  if (rv<0)
    return NULL;
  else rv = make_hashindex
         (spec,FIX2INT(nbuckets_arg),
          interpret_hashindex_flags(opts),
          FD_INT(hashconst),
          metadata,
          slotids_arg,
          baseoids_arg,-1,-1);

  fd_decref(metadata);
  fd_decref(metadata_init);
  fd_decref(baseoids_arg);
  fd_decref(slotids_arg);
  fd_decref(nbuckets_arg);
  fd_decref(hashconst);

  if (rv<0)
    return NULL;
  else {
    fd_set_file_opts(spec,opts);
    return fd_open_index(spec,flags,VOID);}
}


/* Useful functions */

FD_EXPORT int fd_hashindexp(struct FD_INDEX *ix)
{
  return (ix->index_handler== &hashindex_handler);
}

static lispval hashindex_stats(struct FD_HASHINDEX *hx)
{
  lispval result = fd_empty_slotmap();
  int n_filled = 0, maxk = 0, n_singles = 0, n2sum = 0;
  fd_add(result,fd_intern("NBUCKETS"),FD_INT(hx->index_n_buckets));
  fd_add(result,fd_intern("NKEYS"),FD_INT(hx->table_n_keys));
  fd_add(result,fd_intern("NBASEOIDS"),FD_INT((hx->index_oidcodes.n_oids)));
  fd_add(result,fd_intern("NSLOTIDS"),FD_INT(hx->index_slotcodes.n_slotcodes));
  hashindex_getstats(hx,&n_filled,&maxk,&n_singles,&n2sum);
  fd_add(result,fd_intern("NFILLED"),FD_INT(n_filled));
  fd_add(result,fd_intern("NSINGLES"),FD_INT(n_singles));
  fd_add(result,fd_intern("MAXKEYS"),FD_INT(maxk));
  fd_add(result,fd_intern("N2SUM"),FD_INT(n2sum));
  {
    double avg = (hx->table_n_keys*1.0)/(n_filled*1.0);
    double sd2 = (n2sum*1.0)/(n_filled*n_filled*1.0);
    fd_add(result,fd_intern("MEAN"),fd_make_flonum(avg));
    fd_add(result,fd_intern("SD2"),fd_make_flonum(sd2));
  }
  return result;
}

FD_EXPORT ssize_t fd_hashindex_bucket(lispval ixarg,lispval key,
                                      lispval mod_arg)
{
  struct FD_INDEX *ix=fd_lisp2index(ixarg);
  ssize_t modulate = (FD_FIXNUMP(mod_arg)) ? (FD_FIX2INT(mod_arg)) : (-1);
  if (ix==NULL) {
    fd_type_error("hashindex","fd_hashindex_bucket",ixarg);
    return -1;}
  else if (ix->index_handler != &hashindex_handler) {
    fd_type_error("hashindex","fd_hashindex_bucket",ixarg);
    return -1;}
  else return hashindex_bucket(((struct FD_HASHINDEX *)ix),key,modulate);
}

/* Getting more key/bucket info */

static lispval keyinfo_schema[5];

FD_EXPORT lispval fd_hashindex_keyinfo(lispval lix,
                                       lispval min_count,
                                       lispval max_count)
{
  fd_index ix = fd_lisp2index(lix);
  if (ix == NULL)
    return FD_ERROR_VALUE;
  else if (ix->index_handler != &hashindex_handler)
    return fd_err(_("NotAHashIndex"),"fd_hashindex_keyinfo",NULL,lix);
  else NO_ELSE;
  long long min_thresh =
    (FD_FIXNUMP(min_count)) ? (FD_FIX2INT(min_count)) : (-1);
  long long max_thresh =
    (FD_FIXNUMP(min_count)) ? (FD_FIX2INT(max_count)) : (-1);
  struct FD_HASHINDEX *hx = (struct FD_HASHINDEX *)ix;
  fd_stream s = &(hx->index_stream);
  unsigned int *offdata = hx->index_offdata;
  fd_offset_type offtype = hx->index_offtype;
  fd_inbuf ins = fd_readbuf(s);
  int i = 0, n_buckets = (hx->index_n_buckets), total_keys;
  int n_to_fetch = 0, key_count = 0;
  fd_lock_stream(s);
  fd_setpos(s,16); total_keys = fd_read_4bytes(ins);
  if (total_keys==0) {
    fd_unlock_stream(s);
    return FD_EMPTY_CHOICE;}
  struct FD_CHOICE *choice = fd_alloc_choice(total_keys);
  lispval *elts  = (lispval *) FD_CHOICE_DATA(choice);
  FD_CHUNK_REF *buckets = u8_big_alloc_n(total_keys,FD_CHUNK_REF);
  int *bucket_no = u8_big_alloc_n(total_keys,unsigned int);
  if (offdata) {
    /* If we have chunk offsets in memory, we just read them off. */
    fd_unlock_stream(s);
    while (i<n_buckets) {
      FD_CHUNK_REF ref =
        fd_get_chunk_ref(offdata,offtype,i,hx->index_n_buckets);
      if (ref.size>0) {
        bucket_no[n_to_fetch]=i;
        buckets[n_to_fetch]=ref;
        n_to_fetch++;}
      i++;}}
  else {
    /* If we don't have chunk offsets in memory, we keep the stream
       locked while we get them. */
    while (i<n_buckets) {
      FD_CHUNK_REF ref = fd_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) {
        bucket_no[n_to_fetch]=i;
        buckets[n_to_fetch]=ref;
        n_to_fetch++;}
      i++;}
    fd_unlock_stream(s);}
  qsort(buckets,n_to_fetch,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  struct FD_INBUF keyblkstrm={0};
  i = 0; while (i<n_to_fetch) {
    int j = 0, n_keys;
    if (!fd_open_block(s,&keyblkstrm,buckets[i].off,buckets[i].size,1)) {
      fd_close_inbuf(&keyblkstrm);
      u8_big_free(buckets);
      u8_big_free(bucket_no);
      fd_decref_elts(elts,key_count);
      return FD_EMPTY_CHOICE;}
    n_keys = fd_read_zint(&keyblkstrm);
    while (j<n_keys) {
      size_t key_rep_len = fd_read_zint(&keyblkstrm);
      const unsigned char *start = keyblkstrm.bufread;
      lispval key = read_key(hx,&keyblkstrm);
      int n_vals = fd_read_zint(&keyblkstrm);
      assert(key!=0);
      if ( ( (min_thresh < 0) || (n_vals > min_thresh) ) &&
           ( (max_thresh < 0) || (n_vals < max_thresh) ) ) {
        lispval *keyinfo_values=u8_alloc_n(4,lispval);
        int hashval = hash_bytes(start,key_rep_len);
        keyinfo_values[0] = key;
        keyinfo_values[1] = FD_INT(n_vals);
        keyinfo_values[2] = FD_INT(bucket_no[i]);
        keyinfo_values[3] = FD_INT(hashval);
        lispval sm =
          fd_make_schemap(NULL,4,
                          FD_SCHEMAP_FIXED|FD_SCHEMAP_READONLY,
                          keyinfo_schema,keyinfo_values);
        elts[key_count++]=(lispval)sm;}
      else fd_decref(key);
      if (n_vals==0) {}
      else if (n_vals==1) {
        int code = fd_read_zint(&keyblkstrm);
        if (code==0) {
          lispval val=fd_read_dtype(&keyblkstrm);
          fd_decref(val);}
        else fd_read_zint(&keyblkstrm);}
      else {
        fd_read_zint(&keyblkstrm);
        fd_read_zint(&keyblkstrm);}
      j++;}
    i++;}
  fd_close_inbuf(&keyblkstrm);
  u8_big_free(buckets);
  u8_big_free(bucket_no);
  return fd_init_choice(choice,key_count,NULL,
                        FD_CHOICE_REALLOC|
                        FD_CHOICE_ISCONSES|
                        FD_CHOICE_DOSORT);
}

static lispval get_hashbuckets(struct FD_HASHINDEX *hx)
{
  lispval buckets = FD_EMPTY;
  fd_stream s = &(hx->index_stream);
  unsigned int *offdata = hx->index_offdata;
  fd_offset_type offtype = hx->index_offtype;
  int i = 0, n_buckets = (hx->index_n_buckets);
  init_cache_level((fd_index)hx);
  fd_lock_stream(s);
  if (offdata) {
    /* If we have chunk offsets in memory, we just read them off. */
    fd_unlock_stream(s);
    while (i<n_buckets) {
      FD_CHUNK_REF ref =
        fd_get_chunk_ref(offdata,offtype,i,hx->index_n_buckets);
      if (ref.size>0) {
        lispval fixnum = FD_INT2DTYPE(i);
        FD_ADD_TO_CHOICE(buckets,fixnum);}
      i++;}}
  else {
    /* If we don't have chunk offsets in memory, we keep the stream
       locked while we get them. */
    while (i<n_buckets) {
      FD_CHUNK_REF ref = fd_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) {
        lispval fixnum = FD_INT2DTYPE(i);
        FD_ADD_TO_CHOICE(buckets,fixnum);}
      i++;}
    fd_unlock_stream(s);}
  return fd_simplify_choice(buckets);
}

static lispval hashbucket_info(struct FD_HASHINDEX *hx,lispval bucket_nums)
{
  fd_stream s = &(hx->index_stream);
  unsigned int *offdata = hx->index_offdata;
  fd_offset_type offtype = hx->index_offtype;
  int i = 0, n_buckets = (hx->index_n_buckets);
  int n_to_fetch = FD_CHOICE_SIZE(bucket_nums), n_refs=0;
  init_cache_level((fd_index)hx);
  fd_lock_stream(s);
  lispval keyinfo = FD_EMPTY;
  FD_CHUNK_REF *buckets = u8_big_alloc_n(n_to_fetch,FD_CHUNK_REF);
  int *bucket_no = u8_big_alloc_n(n_to_fetch,unsigned int);
  if (offdata) {
    /* If we have chunk offsets in memory, we just read them off. */
    fd_unlock_stream(s);
    FD_DO_CHOICES(bucket,bucket_nums) {
      if (FD_FIXNUMP(bucket)) {
        long long bucket_val = FD_FIX2INT(bucket);
        if ( (bucket_val >= 0) && (bucket_val < n_buckets) ) {
          FD_CHUNK_REF ref =
            fd_get_chunk_ref(offdata,offtype,bucket_val,n_buckets);
          if (ref.size>0) {
            bucket_no[n_refs]=i;
            buckets[n_refs]=ref;
            n_refs++;}}}}}
  else {
    FD_DO_CHOICES(bucket,bucket_nums) {
      if (FD_FIXNUMP(bucket)) {
        long long bucket_val = FD_FIX2INT(bucket);
        if ( (bucket_val >= 0) && (bucket_val < n_buckets) ) {
          FD_CHUNK_REF ref =
            fd_fetch_chunk_ref(s,256,offtype,bucket_val,1);
          if (ref.size>0) {
            bucket_no[n_refs]=bucket_val;
            buckets[n_refs]=ref;
            n_refs++;}}}}}
  qsort(buckets,n_refs,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  struct FD_INBUF keyblkstrm={0};
  i = 0; while (i<n_refs) {
    int j = 0, n_keys;
    if (!fd_open_block(s,&keyblkstrm,buckets[i].off,buckets[i].size,1)) {
      fd_close_inbuf(&keyblkstrm);
      u8_big_free(buckets);
      u8_big_free(bucket_no);
      fd_decref(keyinfo);
      return FD_EMPTY_CHOICE;}
    n_keys = fd_read_zint(&keyblkstrm);
    while (j<n_keys) {
      size_t key_rep_len = fd_read_zint(&keyblkstrm);
      const unsigned char *start = keyblkstrm.bufread;
      lispval key = read_key(hx,&keyblkstrm);
      int n_vals = fd_read_zint(&keyblkstrm);
      assert(key!=0);
      int n_slots = (n_vals>1) ? (4) : (5);
      lispval *keyinfo_values = u8_alloc_n(n_slots,lispval);
      int hashval = hash_bytes(start,key_rep_len);
      keyinfo_values[0] = key;
      keyinfo_values[1] = FD_INT(n_vals);
      keyinfo_values[2] = FD_INT(bucket_no[i]);
      keyinfo_values[3] = FD_INT(hashval);
      if (n_vals==0)
        keyinfo_values[4] = FD_EMPTY_CHOICE;
      else if (n_vals==1) {
        lispval value = read_zvalue(hx,&keyblkstrm);
        keyinfo_values[4]=value;}
      else {
        fd_read_zint(&keyblkstrm);
        fd_read_zint(&keyblkstrm);}
      lispval sm =
        fd_make_schemap(NULL,n_slots,
                        FD_SCHEMAP_FIXED|FD_SCHEMAP_READONLY,
                        keyinfo_schema,keyinfo_values);
      FD_ADD_TO_CHOICE(keyinfo,sm);
      j++;}
    i++;}
  fd_close_inbuf(&keyblkstrm);
  u8_big_free(buckets);
  u8_big_free(bucket_no);
  return keyinfo;
}

static lispval hashrange_info(struct FD_HASHINDEX *hx,
                              unsigned int start,unsigned int end)
{
  fd_stream s = &(hx->index_stream);
  unsigned int *offdata = hx->index_offdata;
  fd_offset_type offtype = hx->index_offtype;
  int n_to_fetch = end-start, n_refs=0;
  init_cache_level((fd_index)hx);
  fd_lock_stream(s);
  lispval keyinfo = FD_EMPTY;
  FD_CHUNK_REF *buckets = u8_big_alloc_n(n_to_fetch,FD_CHUNK_REF);
  int *bucket_no = u8_big_alloc_n(n_to_fetch,unsigned int);
  if (offdata) {
    /* If we have chunk offsets in memory, we just read them off. */
    fd_unlock_stream(s);
    int i=start; while (i<end) {
      FD_CHUNK_REF ref =
        fd_get_chunk_ref(offdata,offtype,i,hx->index_n_buckets);
      if (ref.size>0) {
        bucket_no[n_refs]=i;
        buckets[n_refs]=ref;
        n_refs++;}
      i++;}}
  else {
    /* If we have chunk offsets in memory, we just read them off. */
    fd_unlock_stream(s);
    int i=start; while (i<end) {
      FD_CHUNK_REF ref =fd_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) {
        bucket_no[n_refs]=i;
        buckets[n_refs]=ref;
        n_refs++;}
      i++;}}
  qsort(buckets,n_refs,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  struct FD_INBUF keyblkstrm={0};
  int i = 0; while (i<n_refs) {
    int j = 0, n_keys;
    if (!fd_open_block(s,&keyblkstrm,buckets[i].off,buckets[i].size,1)) {
      fd_close_inbuf(&keyblkstrm);
      u8_big_free(buckets);
      u8_big_free(bucket_no);
      fd_decref(keyinfo);
      return FD_EMPTY_CHOICE;}
    n_keys = fd_read_zint(&keyblkstrm);
    while (j<n_keys) {
      size_t key_rep_len = fd_read_zint(&keyblkstrm);
      const unsigned char *start = keyblkstrm.bufread;
      lispval key = read_key(hx,&keyblkstrm);
      int n_vals = fd_read_zint(&keyblkstrm);
      assert(key!=0);
      int n_slots = (n_vals>1) ? (4) : (5);
      lispval *keyinfo_values = u8_alloc_n(n_slots,lispval);
      int hashval = hash_bytes(start,key_rep_len);
      keyinfo_values[0] = key;
      keyinfo_values[1] = FD_INT(n_vals);
      keyinfo_values[2] = FD_INT(bucket_no[i]);
      keyinfo_values[3] = FD_INT(hashval);
      if (n_vals==0)
        keyinfo_values[4] = FD_EMPTY_CHOICE;
      else if (n_vals==1) {
        lispval value = read_zvalue(hx,&keyblkstrm);
        keyinfo_values[4]=value;}
      else {
        fd_read_zint(&keyblkstrm);
        fd_read_zint(&keyblkstrm);}
      lispval sm = fd_make_schemap
        (NULL,n_slots,
         FD_SCHEMAP_FIXED|FD_SCHEMAP_READONLY,
         keyinfo_schema,keyinfo_values);
      FD_ADD_TO_CHOICE(keyinfo,sm);
      j++;}
    i++;}
  fd_close_inbuf(&keyblkstrm);
  u8_big_free(buckets);
  u8_big_free(bucket_no);
  return keyinfo;
}

static lispval set_slotids(struct FD_HASHINDEX *hx,lispval arg)
{
  if (hx->index_slotcodes.n_slotcodes > 0) {
    u8_log(LOG_WARN,"ExistingSlotcodes",
           "The index %s already has slotcodes: %q",
           hx->indexid,(lispval)(hx->index_slotcodes.slotids));
    return FD_FALSE;}
  else {
    int rv = 0;
    struct FD_SLOTCODER *slotcodes = & hx->index_slotcodes;
    if ( (FD_FIXNUMP(arg)) && ( (FD_FIX2INT(arg)) >= 0) )
      rv = fd_init_slotcoder(slotcodes,FD_FIX2INT(arg),NULL);
    else if ( FD_VECTORP(arg) )
      rv = fd_init_slotcoder(slotcodes,
                             FD_VECTOR_LENGTH(arg),
                             FD_VECTOR_ELTS(arg));
    else {
      fd_seterr("BadSlotIDs","hashindex_ctl/slotids",hx->indexid,arg);
      return FD_ERROR_VALUE;}
    if (rv>=0) {
      u8_lock_mutex(& hx->index_commit_lock );
      struct FD_STREAM _stream = {0}, *stream = fd_init_file_stream
        (&_stream,hx->index_source,FD_FILE_MODIFY,0,8192);
      rv=update_hashindex_slotcodes(hx,slotcodes,stream,stream);
      fd_close_stream(stream,FD_STREAM_FREEDATA);
      u8_unlock_mutex(& hx->index_commit_lock );}
    if (rv<0)
      return FD_ERROR;
    else return FD_TRUE;}
}

static lispval set_baseoids(struct FD_HASHINDEX *hx,lispval arg)
{
  int rv = 0;
  if (hx->index_oidcodes.n_oids > 0) {
    u8_log(LOG_WARN,"ExistingBaseOIDs",
           "The index %s already has baseoids",
           hx->indexid);
    return FD_FALSE;}
  else {
    struct FD_OIDCODER *oidcodes = & hx->index_oidcodes;
    if ( (FD_FIXNUMP(arg)) && ( (FD_FIX2INT(arg)) >= 0) )
      fd_init_oidcoder(oidcodes,FD_FIX2INT(arg),NULL);
    else if ( FD_VECTORP(arg) )
      fd_init_oidcoder(oidcodes,
                       FD_VECTOR_LENGTH(arg),
                       FD_VECTOR_ELTS(arg));
    else {
      fd_seterr("BadSlotIDs","hashindex_ctl/slotids",hx->indexid,arg);
      return FD_ERROR_VALUE;}
    u8_lock_mutex(& hx->index_commit_lock );
    struct FD_STREAM _stream = {0}, *stream = fd_init_file_stream
      (&_stream,hx->index_source,FD_FILE_MODIFY,0,8192);
    rv = update_hashindex_oidcodes(hx,oidcodes,stream,stream);
    fd_close_stream(stream,FD_STREAM_FREEDATA);
    u8_unlock_mutex(& hx->index_commit_lock );
    if (rv<0)
      return FD_ERROR_VALUE;
    else return FD_TRUE;}
}

/* Modifying readonly status */

static int hashindex_set_read_only(fd_hashindex hx,int read_only)
{
  struct FD_STREAM _stream, *stream = fd_init_file_stream
    (&_stream,hx->index_source,FD_FILE_MODIFY,-1,-1);
  if (stream == NULL) return -1;
  fd_lock_index(hx);
  unsigned int format =
    fd_read_4bytes_at(stream,FD_HASHINDEX_FORMAT_POS,FD_STREAM_ISLOCKED);
  if (read_only)
    format = format | (FD_HASHINDEX_READ_ONLY);
  else format = format & (~(FD_HASHINDEX_READ_ONLY));
  ssize_t v = fd_write_4bytes_at(stream,format,FD_HASHINDEX_FORMAT_POS);
  if (v>=0) {
    if (read_only)
      hx->index_flags |=  FD_STORAGE_READ_ONLY;
    else hx->index_flags &=  (~(FD_STORAGE_READ_ONLY));}
  fd_close_stream(stream,FD_STREAM_FREEDATA);
  fd_unlock_index(hx);
  return v;
}

/* The control function */

static lispval metadata_readonly_props = FD_VOID;

static lispval hashindex_ctl(fd_index ix,lispval op,int n,lispval *args)
{
  struct FD_HASHINDEX *hx = (struct FD_HASHINDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return fd_err("BadIndexOpCall","hashindex_ctl",
                  hx->indexid,VOID);
  else if (op == fd_cachelevel_op) {
    if (n==0)
      return FD_INT(hx->index_cache_level);
    else {
      lispval arg = (args)?(args[0]):(VOID);
      if ((FIXNUMP(arg))&&(FIX2INT(arg)>=0)&&
          (FIX2INT(arg)<0x100)) {
        hashindex_setcache(hx,FIX2INT(arg));
        return FD_INT(hx->index_cache_level);}
      else return fd_type_error
             (_("cachelevel"),"hashindex_ctl/cachelevel",arg);}}
  else if (op == fd_bufsize_op) {
    if (n==0)
      return FD_INT(hx->index_stream.buf.raw.buflen);
    else if (FIXNUMP(args[0])) {
      fd_lock_index(hx);
      fd_setbufsize(&(hx->index_stream),FIX2INT(args[0]));
      fd_unlock_index(hx);
      return FD_INT(hx->index_stream.buf.raw.buflen);}
    else return fd_type_error("buffer size","hashindex_ctl/bufsize",args[0]);}
  else if (op == fd_index_hashop) {
    if (n==0)
      return FD_INT(hx->index_n_buckets);
    else {
      lispval mod_arg = (n>1) ? (args[1]) : (VOID);
      ssize_t bucket = hashindex_bucket(hx,args[0],0);
      if (FIXNUMP(mod_arg))
        return FD_INT((bucket%FIX2INT(mod_arg)));
      else if ((FALSEP(mod_arg))||(VOIDP(mod_arg)))
        return FD_INT(bucket);
      else return FD_INT((bucket%(hx->index_n_buckets)));}}
  else if (op == fd_index_bucketsop) {
    if (n==0)
      return get_hashbuckets(hx);
    else if (n==1)
      return hashbucket_info(hx,args[0]);
    else if (n==2) {
      lispval arg0 = args[0], arg1 = args[1];
      if (!(FD_UINTP(arg0)))
        return fd_type_error("lowerhashbucket","hashindex_ctl/buckets",arg0);
      else if (!(FD_UINTP(arg0)))
        return fd_type_error("lowerhashbucket","hashindex_ctl/buckets",arg1);
      else {
        long long lower = fd_getint(arg0);
        long long upper = fd_getint(arg1);
        if (upper==lower) return hashbucket_info(hx,args[0]);
        else if (upper < lower)
          return fd_err(fd_DisorderedRange,"hashindex_ctl",hx->indexid,
                        fd_init_pair(NULL,arg0,arg1));
        else if (upper > hx->index_n_buckets)
          return fd_err(fd_RangeError,"hashindex_ctl",hx->indexid,arg1);
        else return hashrange_info(hx,lower,upper);}}
    else return fd_err(fd_TooManyArgs,"hashindex_ctl",hx->indexid,op);}
  else if ( (op == fd_metadata_op) && (n == 0) ) {
    lispval base = fd_index_base_metadata(ix);
    int n_slotids = hx->index_slotcodes.n_slotcodes;
    int n_baseoids = hx->index_oidcodes.n_oids;
    if ( hx->hashindex_format & FD_HASHINDEX_READ_ONLY )
      fd_store(base,FDSYM_READONLY,FD_TRUE);
    fd_store(base,slotids_symbol,FD_INT(n_slotids));
    fd_store(base,baseoids_symbol,FD_INT(n_baseoids));
    fd_store(base,buckets_symbol,FD_INT(hx->index_n_buckets));
    fd_store(base,nkeys_symbol,FD_INT(hx->table_n_keys));
    fd_add(base,metadata_readonly_props,slotids_symbol);
    fd_add(base,metadata_readonly_props,baseoids_symbol);
    fd_add(base,metadata_readonly_props,buckets_symbol);
    fd_add(base,metadata_readonly_props,nkeys_symbol);
    return base;}
  else if ( ( ( op == FDSYM_READONLY ) && (n == 0) ) ||
            ( ( op == fd_metadata_op ) && (n == 2) &&
              ( args[1] == FDSYM_READONLY ) ) ) {
    if ( (ix->index_flags) & (FD_STORAGE_READ_ONLY) )
      return FD_TRUE;
    else return FD_FALSE;}
  else if ( ( ( op == FDSYM_READONLY ) && (n == 1) ) ||
            ( ( op == fd_metadata_op ) && (n == 2) &&
              ( args[1] == FDSYM_READONLY ) ) ) {
    lispval arg = ( op == FDSYM_READONLY ) ? (args[0]) : (args[1]);
    int rv = (FD_FALSEP(arg)) ? (hashindex_set_read_only(hx,0)) :
      (hashindex_set_read_only(hx,1));
    if (rv<0)
      return FD_ERROR;
    else return fd_incref(arg);}
  else if (op == fd_stats_op)
    return hashindex_stats(hx);
  else if (op == fd_reload_op) {
    reload_offdata(ix);
    fd_index_swapout(ix,((n==0)?(FD_VOID):(args[0])));
    return FD_TRUE;}
  else if (op == fd_slotids_op) {
    if (n == 0) {
      int n_slotcodes = hx->index_slotcodes.n_slotcodes;
      lispval *elts = u8_alloc_n(n_slotcodes,lispval);
      lispval *slotids = hx->index_slotcodes.slotids->vec_elts;
      int i = 0;
      while (i< n_slotcodes) {
        lispval slotid = slotids[i];
        elts[i]=slotid; fd_incref(slotid);
        i++;}
      return fd_wrap_vector(n_slotcodes,elts);}
    else if (n == 1)
      return set_slotids(hx,args[0]);
    else return fd_err(fd_TooManyArgs,"hashindex_ctl/slotids",hx->indexid,FD_VOID);}
  else if (op == fd_baseoids_op) {
    if (n == 0) {
      int n_baseoids=hx->index_oidcodes.n_oids;
      lispval *baseoids=hx->index_oidcodes.baseoids;
      lispval result=fd_make_vector(n_baseoids,NULL);
      int i=0; while (i<n_baseoids) {
        lispval baseoid = baseoids[i];
        FD_VECTOR_SET(result,i,baseoid);
        i++;}
      return result;}
    else if (n == 1)
      return set_baseoids(hx,args[0]);
    else return fd_err(fd_TooManyArgs,"hashindex_ctl/slotids",hx->indexid,FD_VOID);}
  else if (op == fd_capacity_op)
    return FD_INT(hx->index_n_buckets);
  else if (op == fd_load_op)
    return FD_INT(hx->table_n_keys);
  else if (op == fd_keycount_op)
    return FD_INT(hx->table_n_keys);
  else if (op == keycounts_symbol) {
    int n_keys=0;
    fd_choice filter;
    struct FD_CHOICE static_choice;
    init_cache_level(ix);
    if (n==0) filter=NULL;
    else {
      lispval arg0 = args[0];
      if (EMPTYP(arg0))
        return arg0;
      else if (CHOICEP(arg0))
        filter = (fd_choice) arg0;
      else {
        static_choice.choice_size=1;
        static_choice.choice_isatomic=(!(FD_CONSP(arg0)));
        static_choice.choice_0=arg0;
        filter=&static_choice;}}
    struct FD_KEY_SIZE *info = hashindex_fetchinfo(ix,filter,&n_keys);
    struct FD_HASHTABLE *table= (fd_hashtable) fd_make_hashtable(NULL,n_keys);
    int i=0; while (i<n_keys) {
      fd_hashtable_op_nolock(table,fd_table_store,
                             info[i].keysize_key,
                             FD_INT(info[i].keysize_count));
      fd_decref(info[i].keysize_key);
      i++;}
    u8_big_free(info);
    return (lispval)table;}
  else return fd_default_indexctl(ix,op,n,args);
}


/* Initializing the driver module */

static struct FD_INDEX_HANDLER hashindex_handler={
  "hashindex", 1, sizeof(struct FD_HASHINDEX), 14,
  hashindex_close, /* close */
  hashindex_commit, /* commit */
  hashindex_fetch, /* fetch */
  hashindex_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  hashindex_fetchn, /* fetchn */
  hashindex_fetchkeys, /* fetchkeys */
  hashindex_fetchinfo, /* fetchinfo */
  NULL, /* batchadd */
  hashindex_create, /* create */
  NULL, /* walk */
  hashindex_recycle, /* recycle */
  hashindex_ctl /* indexctl */
};

/* Module (file) Initialization */

FD_EXPORT void fd_init_hashindex_c()
{
  set_symbol = fd_intern("SET");
  drop_symbol = fd_intern("DROP");
  keycounts_symbol = fd_intern("KEYCOUNTS");
  slotids_symbol = fd_intern("SLOTIDS");
  baseoids_symbol = fd_intern("BASEOIDS");
  buckets_symbol = fd_intern("BUCKETS");
  nkeys_symbol = fd_intern("KEYS");

  metadata_readonly_props = fd_intern("_READONLY_PROPS");

  keyinfo_schema[0] = fd_intern("KEY");
  keyinfo_schema[1] = fd_intern("COUNT");
  keyinfo_schema[2] = fd_intern("BUCKET");
  keyinfo_schema[3] = fd_intern("HASH");
  keyinfo_schema[4] = fd_intern("VALUE");

  u8_register_source_file(_FILEINFO);

  fd_register_index_type
    ("hashindex",
     &hashindex_handler,
     open_hashindex,
     fd_match_index_file,
     (void *)(U8_INT2PTR(FD_HASHINDEX_MAGIC_NUMBER)));
  fd_register_index_type
    ("damaged_hashindex",
     &hashindex_handler,
     recover_hashindex,
     fd_match_index_file,
     (void *)(U8_INT2PTR(FD_HASHINDEX_TO_RECOVER)));

  fd_register_config("HASHINDEX:SIZE","The default size for hash indexes",
                     fd_sizeconfig_get,fd_sizeconfig_set,
                     &hashindex_default_size);
  fd_register_config("HASHINDEX:LOGLEVEL",
                     "The default loglevel for hashindexs",
                     fd_intconfig_get,fd_loglevelconfig_set,
                     &hashindex_loglevel);


  fd_set_default_index_type("hashindex");
}

/* TODO:
 * make baseoids be megapools, use intrinsic tables
 * add memory prefetch recommendations
 * implement dynamic slotid/baseoid addition
 * implement commit
 * implement ACID
 */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
