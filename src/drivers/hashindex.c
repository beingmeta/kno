/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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

#define FD_INLINE_BUFIO 1
#define FD_INLINE_CHOICES 1
#define FD_FAST_CHOICE_CONTAINSP 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dtypeio.h"
#include "framerd/streams.h"
#include "framerd/numbers.h"
#include "framerd/storage.h"
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

#if (HAVE_MMAP)
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
#define HX_KEYBUF_SIZE 2000
#endif

#ifdef HX_VALBUF_SIZE
#define HX_VALBUF_SIZE 1000
#endif

#define LOCK_STREAM 1
#define DONT_LOCK_STREAM 0

#if FD_DEBUG_HASHINDEXES
#define CHECK_POS(pos,stream)                                      \
  if ((pos)!=(fd_getpos(stream)))                                  \
    u8_log(LOG_CRIT,"FILEPOS error","position mismatch %ld/%ld",   \
           pos,fd_getpos(stream));                                 \
  else {}
#define CHECK_ENDPOS(pos,stream)                                      \
  { ssize_t curpos = fd_getpos(stream), endpos = fd_endpos(stream);       \
    if (((pos)!=(curpos)) && ((pos)!=(endpos)))                       \
      u8_log(LOG_CRIT,"ENDPOS error","position mismatch %ld/%ld/%ld", \
             pos,curpos,endpos);                                      \
    else {}                                                           \
  }
#else
#define CHECK_POS(pos,stream)
#define CHECK_ENDPOS(pos,stream)
#endif

static ssize_t hash_index_default_size=32000;

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

static fdtype read_values(fd_hashindex,fdtype,int,fd_off_t,size_t);

static struct FD_INDEX_HANDLER hashindex_handler;

static fd_exception CorruptedHashIndex=_("Corrupted hashindex file");
static fd_exception BadHashFn=_("hashindex has unknown hash function");

static fdtype set_symbol, drop_symbol;

/* Utilities for DTYPE I/O */

#define nobytes(in,nbytes) (FD_EXPECT_FALSE(!(fd_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (FD_EXPECT_TRUE(fd_request_bytes(in,nbytes)))

#define output_byte(out,b) \
  if (fd_write_byte(out,b)<0) return -1; else {}
#define output_4bytes(out,w) \
  if (fd_write_4bytes(out,w)<0) return -1; else {}
#define output_bytes(out,bytes,n) \
  if (fd_write_bytes(out,bytes,n)<0) return -1; else {}

/* Getting chunk refs */

static int get_chunk_ref_size(fd_hashindex ix)
{
  switch (ix->index_offtype) {
  case FD_B32: case FD_B40: return 8;
  case FD_B64: return 12;}
  return -1;
}

/* Opening database blocks */

FD_FASTOP fd_inbuf open_block
  (struct FD_INBUF *tmpbuf,struct FD_HASHINDEX *hx,
   fd_off_t off,fd_size_t size,unsigned char *buf)
{
  fd_stream stream = &(hx->index_stream);
  if (read_chunk(stream,size,off,buf)) {
    FD_INIT_BYTE_INPUT(tmpbuf,buf,size);
    return tmpbuf;}
  else return NULL;
}

FD_FASTOP fdtype read_dtype_at_pos(fd_stream s,fd_off_t off)
{
  fd_off_t retval = fd_setpos(s,off);
  fd_inbuf ins = fd_readbuf(s);
  if (retval<0) return FD_ERROR_VALUE;
  else return fd_read_dtype(ins);
}

/* Opening a hash index */

static int init_slotids
  (struct FD_HASHINDEX *hx,int n_slotids,fdtype *slotids_init);
static int init_baseoids
  (struct FD_HASHINDEX *hx,int n_baseoids,fdtype *baseoids_init);
static int recover_hashindex(struct FD_HASHINDEX *hx);

static fd_index open_hashindex(u8_string fname,fd_storage_flags flags,fdtype opts)
{
  struct FD_HASHINDEX *index = u8_alloc(struct FD_HASHINDEX);
  int read_only = U8_BITP(flags,FD_STORAGE_READ_ONLY);
  int consed = U8_BITP(flags,FD_STORAGE_UNREGISTERED);

  unsigned int magicno, n_keys;
  fd_off_t slotids_pos, baseoids_pos;
  fd_size_t slotids_size, baseoids_size;
  fd_stream_mode mode=
    ((read_only) ? (FD_FILE_READ) : (FD_FILE_MODIFY));
  long long cache_level = fd_fixopt(opts,"CACHELEVEL",fd_default_cache_level);
  int stream_flags = FD_STREAM_CAN_SEEK | FD_STREAM_NEEDS_LOCK |
    ( (read_only) ? (FD_STREAM_READ_ONLY) : (0) ) |
    ( (cache_level>=3) ? (FD_STREAM_USEMMAP) : (0) );

  fd_init_index((fd_index)index,&hashindex_handler,
                fname,u8_realpath(fname,NULL),
                flags);
  fd_stream stream=
    fd_init_file_stream(&(index->index_stream),fname,mode,
                        stream_flags,
                        fd_driver_bufsize);

  if (stream == NULL) {
    u8_free(index);
    fd_seterr3(u8_CantOpenFile,"open_hashindex",u8_strdup(fname));
    return NULL;}
  /* See if it ended up read only */
  if (stream->stream_flags&FD_STREAM_READ_ONLY)
    read_only = 1;
  stream->stream_flags &= ~FD_STREAM_IS_CONSED;
  magicno = fd_read_4bytes_at(stream,0);
  index->index_n_buckets = fd_read_4bytes_at(stream,4);
  if (magicno == FD_HASHINDEX_TO_RECOVER) {
    u8_log(LOG_WARN,fd_RecoveryRequired,"Recovering the hash index %s",fname);
    recover_hashindex(index);
    magicno = magicno&(~0x20);}
  index->index_offdata = NULL;
  index->fd_storage_xformat = fd_read_4bytes_at(stream,8);
  if (read_only)
    U8_SETBITS(index->index_flags,FD_STORAGE_READ_ONLY);
  if (((index->fd_storage_xformat)&(FD_HASHINDEX_FN_MASK))!=0) {
    u8_free(index);
    fd_seterr3(BadHashFn,"open_hashindex",NULL);
    return NULL;}

  index->index_offtype = (fd_offset_type)
    (((index->fd_storage_xformat)&(FD_HASHINDEX_OFFTYPE_MASK))>>4);

  index->index_custom = fd_read_4bytes_at(stream,12);

  /* Currently ignored */
  index->table_n_keys = n_keys = fd_read_4bytes_at(stream,16);

  slotids_pos = fd_read_8bytes_at(stream,20,NULL);
  slotids_size = fd_read_4bytes_at(stream,28);

  baseoids_pos = fd_read_8bytes_at(stream,32,NULL);
  baseoids_size = fd_read_4bytes_at(stream,40);

  /* metadata_pos = */ fd_read_8bytes_at(stream,44,NULL);
  /* metadata_size = */ fd_read_4bytes_at(stream,52);

  /* Initialize the slotids field used for storing feature keys */
  if (slotids_size) {
    fdtype slotids_vector = read_dtype_at_pos(stream,slotids_pos);
    if (FD_VOIDP(slotids_vector)) {
      index->index_n_slotids = 0; index->index_new_slotids = 0;
      index->index_slotids = NULL;
      index->slotid_lookup = NULL;}
    else if (FD_VECTORP(slotids_vector)) {
      init_slotids(index,
                   FD_VECTOR_LENGTH(slotids_vector),
                   FD_VECTOR_DATA(slotids_vector));
      fd_decref(slotids_vector);}
    else {
      fd_seterr("Bad SLOTIDS data","open_hashindex",
                u8_strdup(fname),FD_VOID);
      fd_free_stream(stream);
      u8_free(index);
      return NULL;}}
  else {
    index->index_n_slotids = 0; index->index_new_slotids = 0;
    index->index_slotids = NULL;
    index->slotid_lookup = NULL;}

  /* Initialize the baseoids field used for compressed OID values */
  if (baseoids_size) {
    fdtype baseoids_vector = read_dtype_at_pos(stream,baseoids_pos);
    if (FD_VOIDP(baseoids_vector)) {
      index->index_n_baseoids = 0; index->index_new_baseoids = 0;
      index->index_baseoid_ids = NULL;
      index->index_ids2baseoids = NULL;}
    else if (FD_VECTORP(baseoids_vector)) {
      init_baseoids(index,
                    FD_VECTOR_LENGTH(baseoids_vector),
                    FD_VECTOR_DATA(baseoids_vector));
      fd_decref(baseoids_vector);}
    else {
      fd_seterr("Bad BASEOIDS data","open_hashindex",
                u8_strdup(fname),FD_VOID);
      fd_free_stream(stream);
      u8_free(index);
      return NULL;}}
  else {
    index->index_n_baseoids = 0;
    index->index_new_baseoids = 0;
    index->index_baseoid_ids = NULL;
    index->index_ids2baseoids = NULL;}

  u8_init_mutex(&(index->index_lock));

  if (!(consed)) fd_register_index((fd_index)index);

  return (fd_index)index;
}

static int sort_by_slotid(const void *p1,const void *p2)
{
  const fd_slotid_lookup l1 = (fd_slotid_lookup)p1, l2 = (fd_slotid_lookup)p2;
  if (l1->slotid<l2->slotid) return -1;
  else if (l1->slotid>l2->slotid) return 1;
  else return 0;
}

static int init_slotids(fd_hashindex hx,int n_slotids,fdtype *slotids_init)
{
  struct FD_SLOTID_LOOKUP *lookup; int i = 0;
  fdtype *slotids, slotids_choice = FD_EMPTY_CHOICE;
  hx->index_slotids = slotids = u8_alloc_n(n_slotids,fdtype);
  hx->slotid_lookup = lookup = u8_alloc_n(n_slotids,FD_SLOTID_LOOKUP);
  hx->index_n_slotids = n_slotids; hx->index_new_slotids = 0;
  if ((hx->fd_storage_xformat)&(FD_HASHINDEX_ODDKEYS))
    slotids_choice = FD_VOID;
  while (i<n_slotids) {
    fdtype slotid = slotids_init[i];
    if (FD_VOIDP(slotids_choice)) {}
    else if (FD_ATOMICP(slotid)) {
      FD_ADD_TO_CHOICE(slotids_choice,slotid);}
    else {
      fd_decref(slotids_choice); slotids_choice = FD_VOID;}
    slotids[i]=slotid;
    lookup[i].zindex = i;
    lookup[i].slotid = fd_incref(slotid);
    i++;}
  qsort(lookup,n_slotids,sizeof(FD_SLOTID_LOOKUP),sort_by_slotid);
  if (!(FD_VOIDP(slotids_choice)))
    hx->index_covers_slotids = fd_simplify_choice(slotids_choice);
  return 0;
}

static int init_baseoids(fd_hashindex hx,int n_baseoids,fdtype *baseoids_init)
{
  int i = 0;
  unsigned int *index_baseoid_ids = u8_alloc_n(n_baseoids,unsigned int);
  short *index_ids2baseoids = u8_alloc_n(1024,short);
  memset(index_baseoid_ids,0,SIZEOF_INT*n_baseoids);
  i = 0; while (i<1024) index_ids2baseoids[i++]= -1;
  hx->index_n_baseoids = n_baseoids;
  hx->index_new_baseoids = 0;
  hx->index_baseoid_ids = index_baseoid_ids;
  hx->index_ids2baseoids = index_ids2baseoids;
  i = 0; while (i<n_baseoids) {
    fdtype baseoid = baseoids_init[i];
    index_baseoid_ids[i]=FD_OID_BASE_ID(baseoid);
    index_ids2baseoids[FD_OID_BASE_ID(baseoid)]=i;
    i++;}
  return 0;
}

/* Making a hash index */

FD_EXPORT int make_hashindex
  (u8_string fname,
   int n_buckets_arg,
   unsigned int flags,
   unsigned int hashconst,
   fdtype slotids_init,
   fdtype baseoids_init,
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
    fd_seterr3(fd_CantWrite,"make_hashindex",u8_strdup(fname));
    fd_free_stream(stream);
    return -1;}
  stream->stream_flags &= ~FD_STREAM_IS_CONSED;
  if (n_buckets_arg<0) n_buckets = -n_buckets_arg;
  else n_buckets = fd_get_hashtable_size(n_buckets_arg);

  u8_log(LOG_INFO,"CreateHashIndex",
         "Creating a hashindex '%s' with %ld buckets",
         fname,n_buckets);

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
  if (FD_VECTORP(slotids_init)) {
    slotids_pos = fd_getpos(stream);
    fd_write_dtype(outstream,slotids_init);
    slotids_size = fd_getpos(stream)-slotids_pos;}

  /* Write the baseoids */
  if (FD_VECTORP(baseoids_init)) {
    baseoids_pos = fd_getpos(stream);
    fd_write_dtype(outstream,baseoids_init);
    baseoids_size = fd_getpos(stream)-baseoids_pos;}

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

FD_FASTOP unsigned int hash_bytes(unsigned char *start,int len)
{
  unsigned int prod = 1, asint = 0;
  unsigned char *ptr = start, *limit = ptr+len;
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

FD_FASTOP int get_slotid_index(fd_hashindex hx,fdtype slotid)
{
  const int size = hx->index_n_slotids;
  fd_slotid_lookup bottom = hx->slotid_lookup, middle = bottom+size/2;
  fd_slotid_lookup hard_top = bottom+size, top = hard_top;
  while (top>bottom) {
    if (slotid == middle->slotid) return middle->zindex;
    else if (slotid<middle->slotid) {
      top = middle-1; middle = bottom+(top-bottom)/2;}
    else {
        bottom = middle+1; middle = bottom+(top-bottom)/2;}
    if ((middle) && (middle<hard_top) && (slotid == middle->slotid))
      return middle->zindex;}
  return -1;
}

static int fast_write_dtype(fd_outbuf out,fdtype key)
{
  int v2 = ((out->buf_flags)&FD_USE_DTYPEV2);
  if (FD_OIDP(key)) {
    FD_OID addr = FD_OID_ADDR(key);
    fd_write_byte(out,dt_oid);
    fd_write_4bytes(out,FD_OID_HI(addr));
    fd_write_4bytes(out,FD_OID_LO(addr));
    return 9;}
  else if (FD_SYMBOLP(key)) {
    int data = FD_GET_IMMEDIATE(key,itype);
    fdtype name = fd_symbol_names[data];
    struct FD_STRING *s = fd_consptr(struct FD_STRING *,name,fd_string_type);
    int len = s->fd_bytelen;
    if ((v2) && (len<256)) {
      {output_byte(out,dt_tiny_symbol);}
      {output_byte(out,len);}
      {output_bytes(out,s->fd_bytes,len);}
      return len+2;}
    else {
      {output_byte(out,dt_symbol);}
      {output_4bytes(out,len);}
      {output_bytes(out,s->fd_bytes,len);}
      return len+5;}}
  else if (FD_STRINGP(key)) {
    struct FD_STRING *s = fd_consptr(struct FD_STRING *,key,fd_string_type);
    int len = s->fd_bytelen;
    if ((v2) && (len<256)) {
      {output_byte(out,dt_tiny_string);}
      {output_byte(out,len);}
      {output_bytes(out,s->fd_bytes,len);}
      return len+2;}
    else {
      {output_byte(out,dt_string);}
      {output_4bytes(out,len);}
      {output_bytes(out,s->fd_bytes,len);}
      return len+5;}}
  else return fd_write_dtype(out,key);
}

FD_FASTOP ssize_t write_zkey(fd_hashindex hx,fd_outbuf out,fdtype key)
{
  int slotid_index = -1; size_t retval = -1;
  int natsort = out->buf_flags&FD_NATSORT_VALUES;
  out->buf_flags |= FD_NATSORT_VALUES;
  if (FD_PAIRP(key)) {
    fdtype car = FD_CAR(key);
    if ((FD_OIDP(car)) || (FD_SYMBOLP(car))) {
      slotid_index = get_slotid_index(hx,car);
      if (slotid_index<0)
        retval = fd_write_byte(out,0)+fd_write_dtype(out,key);
      else retval = fd_write_zint(out,slotid_index+1)+
             fast_write_dtype(out,FD_CDR(key));}
    else retval = fd_write_byte(out,0)+fd_write_dtype(out,key);}
  else retval = fd_write_byte(out,0)+fd_write_dtype(out,key);
  if (!(natsort)) out->buf_flags &= ~FD_NATSORT_VALUES;
  return retval;
}

static fdtype fast_read_dtype(fd_inbuf in)
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
          fdtype result = fd_make_string(NULL,len,in->bufread);
          in->bufread = in->bufread+len;
          return result;}}
    case dt_tiny_string:
      if (nobytes(in,2)) return fd_return_errcode(FD_EOD);
      else {
        int len = fd_get_byte(in->bufread+1); in->bufread = in->bufread+2;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          fdtype result = fd_make_string(NULL,len,in->bufread);
          in->bufread = in->bufread+len;
          return result;}}
    case dt_symbol:
      if (nobytes(in,5)) return fd_return_errcode(FD_EOD);
      else {
        int len = fd_get_4bytes(in->bufread+1); in->bufread = in->bufread+5;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          fdtype symbol;
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

FD_FASTOP fdtype read_key(fd_hashindex hx,fd_inbuf in)
{
  int code = fd_read_zint(in);
  if (code==0) return fast_read_dtype(in);
  else if ((code-1)<hx->index_n_slotids) {
    fdtype cdr = fast_read_dtype(in);
    if (FD_ABORTP(cdr)) return cdr;
    else return fd_conspair(hx->index_slotids[code-1],cdr);}
  else return fd_err(CorruptedHashIndex,"read_key",NULL,FD_VOID);
}

FD_EXPORT ssize_t hashindex_bucket(struct FD_HASHINDEX *hx,fdtype key,
                                   ssize_t modulate)
{
  struct FD_OUTBUF out; unsigned char buf[1024];
  unsigned int hashval; int dtype_len;
  FD_INIT_BYTE_OUTBUF(&out,buf,1024);
  if ((hx->fd_storage_xformat)&(FD_HASHINDEX_DTYPEV2))
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

FD_FASTOP fdtype write_zvalue(fd_hashindex hx,fd_outbuf out,fdtype value)
{
  if ((FD_OIDP(value))&&(hx->index_ids2baseoids)) {
    int base = FD_OID_BASE_ID(value);
    short baseoid_index = hx->index_ids2baseoids[base];
    if (baseoid_index<0) {
      int bytes_written; fd_write_byte(out,0);
      bytes_written = fd_write_dtype(out,value);
      if (bytes_written<0) return FD_ERROR_VALUE;
      else return bytes_written+1;}
    else {
      int offset = FD_OID_BASE_OFFSET(value), bytes_written;
      bytes_written = fd_write_zint(out,baseoid_index+1);
      if (bytes_written<0) return FD_ERROR_VALUE;
      bytes_written = bytes_written+fd_write_zint(out,offset);
      return bytes_written;}}
  else {
    int bytes_written; fd_write_byte(out,0);
    bytes_written = fd_write_dtype(out,value);
    return bytes_written+1;}
}

FD_FASTOP fdtype read_zvalue(fd_hashindex hx,fd_inbuf in)
{
  int prefix = fd_read_zint(in);
  if (prefix==0) return fd_read_dtype(in);
  else {
    unsigned int base = hx->index_baseoid_ids[prefix-1];
    unsigned int offset = fd_read_zint(in);
    return FD_CONSTRUCT_OID(base,offset);}
}

/* Fetching */

static fdtype hashindex_fetch(fd_index ix,fdtype key)
{
  struct FD_HASHINDEX *hx = (fd_hashindex)ix;
  size_t blockbuf_size=8192;
  unsigned char buf[HX_KEYBUF_SIZE];
  unsigned char *blockbuf=u8_malloc(blockbuf_size);
  struct FD_OUTBUF out;
  unsigned int hashval, bucket, n_keys, i, dtype_len, n_values;
  fd_off_t vblock_off; size_t vblock_size;
  FD_CHUNK_REF keyblock;
  FD_INIT_BYTE_OUTBUF(&out,buf,HX_KEYBUF_SIZE);
#if FD_DEBUG_HASHINDEXES
  /* u8_message("Fetching the key %q from %s",key,hx->indexid); */
#endif
  /* If the index doesn't have oddkeys and you're looking up some feature (pair)
     whose slotid isn't in the slotids, the key isn't in the table. */
  if ((!((hx->fd_storage_xformat)&(FD_HASHINDEX_ODDKEYS))) && (FD_PAIRP(key))) {
    fdtype slotid = FD_CAR(key);
    if (((FD_SYMBOLP(slotid)) || (FD_OIDP(slotid))) &&
        (get_slotid_index(hx,slotid)<0)) {
#if FD_DEBUG_HASHINDEXES
      u8_message("The slotid %q isn't indexed in %s, returning {}",
                 slotid,hx->indexid);
#endif
      fd_close_outbuf(&out);
      u8_free(blockbuf);
      return FD_EMPTY_CHOICE;}}
  if ((hx->fd_storage_xformat)&(FD_HASHINDEX_DTYPEV2))
    out.buf_flags |= FD_USE_DTYPEV2;
  dtype_len = write_zkey(hx,&out,key);
  hashval = hash_bytes(out.buffer,dtype_len);
  bucket = hashval%(hx->index_n_buckets);
#if (!(HAVE_PREAD))
  fd_lock_stream(stream);
#endif
  if (hx->index_offdata)
    keyblock = get_chunk_ref(hx->index_offdata,hx->index_offtype,bucket);
  else keyblock=
         fetch_chunk_ref(&(hx->index_stream),256,hx->index_offtype,bucket);
  if (keyblock.size==0) {
#if (!(HAVE_PREAD))
    fd_unlock_stream(stream);
#endif
    fd_close_outbuf(&out);
    u8_free(blockbuf);
    return FD_EMPTY_CHOICE;}
  else {
    struct FD_INBUF keystream;
    unsigned char *inbuf=blockbuf;
    if (blockbuf_size<keyblock.size) {
      size_t new_size=blockbuf_size;
      while (new_size<keyblock.size) new_size=new_size*2;
      unsigned char *newbuf=u8_malloc(new_size);
      if (newbuf==NULL) {
        u8_seterr(fd_MallocFailed,"hashindex_fetch",u8dup(hx->indexid));
        fd_close_outbuf(&out);
        u8_free(blockbuf);
        return FD_ERROR_VALUE;}
      else {
        u8_free(blockbuf);
        inbuf=blockbuf=newbuf;
        blockbuf_size=new_size;}}
    open_block(&keystream,hx,keyblock.off,keyblock.size,inbuf);
    n_keys = fd_read_zint(&keystream);
    i = 0; while (i<n_keys) {
      int key_len = fd_read_zint(&keystream);
      if ((key_len == dtype_len) &&
          (memcmp(keystream.bufread,out.buffer,dtype_len)==0)) {
        keystream.bufread = keystream.bufread+key_len;
        n_values = fd_read_zint(&keystream);
        if (n_values==0) {
          fd_close_outbuf(&out);
          u8_free(blockbuf);
          return FD_EMPTY_CHOICE;}
        else if (n_values==1) {
          fdtype value = read_zvalue(hx,&keystream);
          fd_close_outbuf(&out);
          u8_free(blockbuf);
          return value;}
        else {
          vblock_off = (fd_off_t)fd_read_zint(&keystream);
          vblock_size = (size_t)fd_read_zint(&keystream);
          fd_close_outbuf(&out);
          u8_free(blockbuf);
          return read_values(hx,key,n_values,vblock_off,vblock_size);}}
      else {
        keystream.bufread = keystream.bufread+key_len;
        n_values = fd_read_zint(&keystream);
        if (n_values==0) {}
        else if (n_values==1) {
          int code = fd_read_zint(&keystream);
          if (code==0) {
            fdtype val = fd_read_dtype(&keystream);
            fd_decref(val);}
          else fd_read_zint(&keystream);}
        else {
          fd_read_zint(&keystream);
          fd_read_zint(&keystream);}}
      i++;}}
  u8_free(blockbuf);
  fd_close_outbuf(&out);
  return FD_EMPTY_CHOICE;
}

static FD_CHUNK_REF read_value_block
(fd_hashindex hx,fdtype key,
 int n_values,FD_CHUNK_REF chunk,
 int *n_readp,fdtype *values,int *consp,
 unsigned char **vbufp,size_t *vbuf_len)
{
  int n_read=*n_readp;
  FD_CHUNK_REF result = {-1,-1};
  fd_off_t vblock_off = chunk.off;
  size_t  vblock_size = chunk.size;
  unsigned char *vbuf=*vbufp;
  if (vblock_size > *vbuf_len) {
    size_t cur_size = *vbuf_len;
    size_t new_size = cur_size;
    while (new_size < vblock_size)
      new_size=((new_size<0x20000)? (new_size*2) :
                (((new_size/0x20000)+4)*0x20000));
    unsigned char *newbuf=u8_realloc(vbuf,new_size);
    if (newbuf == NULL) {
      u8_seterr("ReallocFailed","read_value_block",u8_strdup(hx->indexid));
      return result;}
    else {
      vbuf=*vbufp=newbuf;
      *vbuf_len=new_size;}}
  struct FD_INBUF instream;
  fd_off_t next_off;
  ssize_t next_size=-1;
  int i=0, atomicp = 1;
  open_block(&instream,hx,vblock_off,vblock_size,vbuf);
  ssize_t n_elts = fd_read_zint(&instream);
  if (n_elts<0)
    return result;
  i = 0; while ( (i<n_elts) && (n_read < n_values) ) {
    fdtype val = read_zvalue(hx,&instream);
    if (FD_ABORTP(val)) {
      if (!(atomicp)) *consp=1;
      *n_readp = n_read;
      return result;}
    else if (FD_CONSP(val)) atomicp = 0;
    else {}
    values[n_read]=val;
    n_read++;
    i++;}
  /* For vblock continuation pointers, we make the size be first,
     so that we don't need to store an offset if it's zero. */
  next_size = fd_read_zint(&instream);
  if (next_size<0) {}
  else if (next_size)
    next_off = fd_read_zint(&instream);
  else next_off = 0;
  if ( (next_size<0) || (next_off < 0)) {
    *n_readp = n_read;
    if (!(atomicp)) *consp=0;
    return result;}
  result.off=next_off; result.size=next_size;
  *n_readp = n_read;
  if (!(atomicp)) *consp=1;
  return result;
}

static fdtype read_values
(fd_hashindex hx,fdtype key,int n_values,
 fd_off_t vblock_off,size_t vblock_size)

{
  struct FD_CHOICE *result = fd_alloc_choice(n_values);
  FD_CHUNK_REF chunk_ref = { vblock_off, vblock_size };
  fdtype *values = (fdtype *)FD_XCHOICE_DATA(result);
  size_t vbuf_len = ( vblock_size < 0x20000) ? (0x20000) :
    (((vblock_size/0x20000)+4)*0x20000);
  unsigned char *vbuf = u8_malloc(vbuf_len);
  int consp = 0, n_read=0;
  while ( (chunk_ref.off>0) && (n_read < n_values) )
    chunk_ref=read_value_block(hx,key,n_values,chunk_ref,
                               &n_read,values,&consp,
                               &vbuf,&vbuf_len);
  if (chunk_ref.off<0) {
    fd_seterr("HashIndexError","read_values",
              u8_mkstring("reading %d values from %s",
                          n_values,hx->indexid),
              key);
    result->choice_size=n_read;
    fd_decref_ptr(result);
    u8_free(vbuf);
    return FD_ERROR_VALUE;}
  else if (n_read != n_values) {
    u8_log(LOGWARN,"InconsistentValueSize",
           "In '%s', the number of stored values "
           "for %q, %lld != %lld (expected)",
           hx->indexid,key,n_read,n_values);
    fd_seterr("InconsistentValueSize","read_values",NULL,key);
    result->choice_size=n_read;
    fd_decref_ptr(result);
    u8_free(vbuf);
    return FD_ERROR_VALUE;}
  else if (n_values == 1) {
    fdtype v = values[0];
    fd_incref(v);
    u8_free(result);
    u8_free(vbuf);
    return v;}
  else {
    u8_free(vbuf);
    return fd_init_choice
      (result,n_values,NULL,
       FD_CHOICE_DOSORT|FD_CHOICE_REALLOC|
       ((consp)?(FD_CHOICE_ISCONSES):
        (FD_CHOICE_ISATOMIC)));}
}


static int hashindex_fetchsize(fd_index ix,fdtype key)
{
  struct FD_HASHINDEX *hx = (fd_hashindex)ix;
  struct FD_OUTBUF out; unsigned char buf[64];
  unsigned int hashval, bucket, n_keys, i, dtype_len, n_values;
  FD_CHUNK_REF keyblock;
  FD_INIT_BYTE_OUTBUF(&out,buf,64);
  if ((hx->fd_storage_xformat)&(FD_HASHINDEX_DTYPEV2))
    out.buf_flags = out.buf_flags|FD_USE_DTYPEV2;
  dtype_len = write_zkey(hx,&out,key);
  hashval = hash_bytes(out.buffer,dtype_len);
  bucket = hashval%(hx->index_n_buckets);
  if (hx->index_offdata)
    keyblock = get_chunk_ref(hx->index_offdata,hx->index_offtype,bucket);
  else keyblock = fetch_chunk_ref
         (&(hx->index_stream),256,hx->index_offtype,bucket);
  if (keyblock.size<=0) return keyblock.size;
  else {
    struct FD_INBUF keystream;
    unsigned char inbuf[keyblock.size];
    open_block(&keystream,hx,keyblock.off,keyblock.size,inbuf);
    n_keys = fd_read_zint(&keystream);
    i = 0; while (i<n_keys) {
      int key_len = fd_read_zint(&keystream);
      n_values = fd_read_zint(&keystream);
      /* vblock_off = */ (void)(fd_off_t)fd_read_zint(&keystream);
      /* vblock_size = */ (void)(size_t)fd_read_zint(&keystream);
      if (key_len!=dtype_len)
        keystream.bufread = keystream.bufread+key_len;
      else if (memcmp(keystream.bufread,out.buffer,dtype_len)==0) {
        fd_close_outbuf(&out);
        return n_values;}
      else keystream.bufread = keystream.bufread+key_len;
      i++;}}
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

static fdtype *fetchn(struct FD_HASHINDEX *hx,int n,fdtype *keys)
{
  fdtype *values = u8_alloc_n(n,fdtype);
  /* This is a buffer where we write keybuf representations of all of the
     keys, which let's use do memcmp to match them to on-disk data */
  struct FD_OUTBUF keysbuf;
  /* This is used to fetch information about keys, sorted to be linear */
  struct KEY_SCHEDULE *ksched = u8_alloc_n(n,struct KEY_SCHEDULE);
  /* This is used to fetch the chained values in the table, also
     sorted to be linear. */
  struct VALUE_SCHEDULE *vsched = u8_alloc_n(n,struct VALUE_SCHEDULE);
  if ( (values==NULL) || (ksched==NULL) || (vsched==NULL) ) {
    u8_seterr(fd_MallocFailed,"hashindex_fetchn",NULL);
    if (values) u8_free(values);
    if (ksched) u8_free(ksched);
    if (vsched) u8_free(vsched);
    return NULL;}

  unsigned int *offdata = hx->index_offdata;
  unsigned char *keyreps;
  int i = 0, n_entries = 0, vsched_size = 0;
  size_t max_keyblock_size = 0, vbuf_size=0;
  int oddkeys = ((hx->fd_storage_xformat)&(FD_HASHINDEX_ODDKEYS));
  fd_stream stream = &(hx->index_stream);
#if FD_DEBUG_HASHINDEXES
  u8_message("Reading %d keys from %s",n,hx->indexid);
#endif
  /* Assuming 32 bytes per key representation */
  FD_INIT_BYTE_OUTPUT(&keysbuf,n*32);
  if ((hx->fd_storage_xformat)&(FD_HASHINDEX_DTYPEV2))
    keysbuf.buf_flags = keysbuf.buf_flags|FD_USE_DTYPEV2;
  /* Fill out a fetch schedule, computing hashes and buckets for each key.
     If we have an offsets table, we compute the offsets during this phase,
     otherwise we defer to an additional loop. */
  while (i<n) {
    fdtype key = keys[i];
    int dt_start = keysbuf.bufwrite-keysbuf.buffer;
    int dt_size, bucket;
   /* If the index doesn't have oddkeys and you're looking up some feature (pair)
     whose slotid isn't in the slotids, the key isn't in the table. */
    if ((!oddkeys) && (FD_PAIRP(key))) {
      fdtype slotid = FD_CAR(key);
      if (((FD_SYMBOLP(slotid)) || (FD_OIDP(slotid))) &&
          (get_slotid_index(hx,slotid)<0)) {
        values[i++]=FD_EMPTY_CHOICE;
        continue;}}
    ksched[n_entries].ksched_i = i;
    ksched[n_entries].ksched_key = key;
    ksched[n_entries].ksched_keyoff = dt_start;
    write_zkey(hx,&keysbuf,key);
    dt_size = (keysbuf.bufwrite-keysbuf.buffer)-dt_start;
    ksched[n_entries].ksched_dtsize = dt_size;
    ksched[n_entries].ksched_bucket = bucket=
      hash_bytes(keysbuf.buffer+dt_start,dt_size)%(hx->index_n_buckets);
    if (offdata) {
      /* Because we have an offsets table, we can use get_chunk_ref,
         which doesn't touch the stream, and use it immediately. */
      ksched[n_entries].ksched_chunk =
        get_chunk_ref(offdata,hx->index_offtype,bucket);
      size_t keyblock_size = ksched[n_entries].ksched_chunk.size;
      /* Track the max_keyblock_size, for a VLA below*/
      if (keyblock_size>max_keyblock_size) max_keyblock_size=keyblock_size;
      if (keyblock_size==0) {
        /* It is empty, so we don't even need to handle this entry. */
        values[i]=FD_EMPTY_CHOICE;
        /* We don't need to keep its dtype representation around either,
           so we reset the key stream. */
        keysbuf.bufwrite = keysbuf.buffer+dt_start;}
      else n_entries++;}
    else n_entries++;
    i++;}
  keyreps=keysbuf.buffer;
  if (hx->index_offdata == NULL) {
    int write_at = 0;
    /* When fetching bucket references, we sort the schedule first, so that
       we're accessing them in order in the file. */
    qsort(ksched,n_entries,sizeof(struct KEY_SCHEDULE),
          sort_ks_by_bucket);
    i = 0; while (i<n_entries) {
      ksched[i].ksched_chunk = fetch_chunk_ref
        (stream,256,hx->index_offtype,ksched[i].ksched_bucket);
      size_t keyblock_size=ksched[i].ksched_chunk.size;
      /* Track the max_keyblock_size, for a VLA below*/
      if (keyblock_size>max_keyblock_size)max_keyblock_size=keyblock_size;
      if (keyblock_size==0) {
        values[ksched[i].ksched_i]=FD_EMPTY_CHOICE;
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
    struct FD_INBUF keyblock;
    unsigned char buf[max_keyblock_size];
    int bucket = -1, j = 0, k = 0, n_keys=0;
    while (j<n_entries) {
      int found = 0;
      fd_off_t blockpos = ksched[j].ksched_chunk.off;
      fd_size_t blocksize = ksched[j].ksched_chunk.size;
      if (ksched[j].ksched_bucket!=bucket) {
        /* If we're in a new bucket, open it as input */
        open_block(&keyblock,hx,blockpos,blocksize,buf);
        bucket=ksched[j].ksched_bucket;
        /* And initialize bucket position and limit */
        k=0; n_keys = fd_read_zint(&keyblock);}
      while (k<n_keys) {
        int n_vals;
        fd_size_t dtsize = fd_read_zint(&keyblock);
        if (match_keybuf(keyblock.bufread,dtsize,&ksched[j],keyreps)) {
          found = 1;
          keyblock.bufread = keyblock.bufread+dtsize;
          n_vals = fd_read_zint(&keyblock);
          if (n_vals==0)
            values[ksched[j].ksched_i]=FD_EMPTY_CHOICE;
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
            values[ksched[j].ksched_i]=(fdtype)result;
            vsched[vsched_size].vsched_i = ksched[j].ksched_i;
            vsched[vsched_size].vsched_chunk.off = block_off;
            vsched[vsched_size].vsched_chunk.size = block_size;
            vsched[vsched_size].vsched_write = (fdtype *)FD_XCHOICE_DATA(result);
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
            fdtype v = read_zvalue(hx,&keyblock);
            fd_decref(v);}
          else {
            /* Skip offset information */
            fd_read_zint(&keyblock);
            fd_read_zint(&keyblock);}}
        k++;}
      if (!(found))
        values[ksched[j].ksched_i]=FD_EMPTY_CHOICE;
      j++;}}
  /* Now we're done with the ksched */
  u8_free(ksched);
  {
    struct FD_INBUF vblock;
    unsigned char *vbuf = u8_malloc(vbuf_size);
    if (vbuf==NULL) {
      u8_free(values); u8_free(vsched);
      u8_seterr(fd_MallocFailed,"hashindex_fetchn",NULL);
      return NULL;}
    while (vsched_size) {
      qsort(vsched,vsched_size,sizeof(struct VALUE_SCHEDULE),
            sort_vs_by_refoff);
      i = 0; while (i<vsched_size) {
        int j = 0, n_vals;
        fd_size_t next_size, block_size=vsched[i].vsched_chunk.size;
        if (block_size>vbuf_size) {
          unsigned char *newbuf = u8_realloc(vbuf,block_size);
          if (newbuf) {vbuf=newbuf; vbuf_size=block_size;}
          else {
            u8_free(values); u8_free(vsched); u8_free(vbuf);
            u8_seterr(fd_MallocFailed,"hashindex_fetchn",NULL);
            return NULL;}}
        open_block(&vblock,hx,vsched[i].vsched_chunk.off,
                   vsched[i].vsched_chunk.size,vbuf);
        n_vals = fd_read_zint(&vblock);
        while (j<n_vals) {
          fdtype v = read_zvalue(hx,&vblock);
          if (FD_CONSP(v)) vsched[i].vsched_atomicp = 0;
          *((vsched[i].vsched_write)++) = v; j++;}
        next_size = fd_read_zint(&vblock);
        if (next_size) {
          vsched[i].vsched_chunk.size = next_size;
          vsched[i].vsched_chunk.off = fd_read_zint(&vblock);}
        else {
          vsched[i].vsched_chunk.size = 0;
          vsched[i].vsched_chunk.off = 0;}
        i++;}
      {
        struct VALUE_SCHEDULE *read = &(vsched[0]), *write = &(vsched[0]);
        struct VALUE_SCHEDULE *limit = read+vsched_size;
        vsched_size = 0; while (read<limit) {
          if (read->vsched_chunk.size) {
            *write++= *read++; vsched_size++;}
          else {
            /* We're now done with this value, so we finalize it,
               using fd_init_choice to sort it.  Note that (rarely),
               fd_init_choice will free what was passed to it (if it
               had zero or one element), so the real result is what is
               returned and not what was passed in. */
            int index = read->vsched_i, atomicp = read->vsched_atomicp;
            struct FD_CHOICE *result = (struct FD_CHOICE *)values[index];
            int n_values = result->choice_size;
            fdtype realv = fd_init_choice(result,n_values,NULL,
                                        FD_CHOICE_DOSORT|
                                        ((atomicp)?(FD_CHOICE_ISATOMIC):
                                         (FD_CHOICE_ISCONSES))|
                                        FD_CHOICE_REALLOC);
            values[index]=realv;
            read++;}}}}
    u8_free(vbuf);}
  u8_free(vsched);
#if FD_DEBUG_HASHINDEXES
  u8_message("Finished reading %d keys from %s",n,hx->indexid);
#endif
  fd_close_outbuf(&keysbuf);
  return values;
}

static fdtype *hashindex_fetchn_inner(fd_index ix,
                                       int n,fdtype *keys,
                                       int stream_locked,
                                       int adds_locked)
{
  struct FD_HASHINDEX *hx = (struct FD_HASHINDEX *)ix;
  fdtype *results;
  results = fetchn(hx,n,keys);
  if (results) {
    int i = 0;
    if (adds_locked==0) fd_read_lock_table(&(hx->index_adds));
    while (i<n) {
      fdtype v = fd_hashtable_get_nolock(&(hx->index_adds),keys[i],
                                       FD_EMPTY_CHOICE);
      if (FD_ABORTP(v)) {
        int j = 0; while (j<n) { fd_decref(results[j]); j++;}
        u8_free(results);
        if (adds_locked) fd_unlock_table(&(hx->index_adds));
        fd_interr(v);
        return NULL;}
      else if (FD_EMPTY_CHOICEP(v)) i++;
      else {
        FD_ADD_TO_CHOICE(results[i],v); i++;}}
    if (adds_locked==0) fd_unlock_table(&(hx->index_adds));}
  return results;
}

/* This is the handler exposed by the index handler struct */
static fdtype *hashindex_fetchn(fd_index ix,int n,fdtype *keys)
{
  return hashindex_fetchn_inner(ix,n,keys,0,0);
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

static fdtype *hashindex_fetchkeys(fd_index ix,int *n)
{
  fdtype *results = NULL;
  struct FD_HASHINDEX *hx = (struct FD_HASHINDEX *)ix;
  fd_stream s = &(hx->index_stream);
  unsigned int *offdata = hx->index_offdata;
  fd_offset_type offtype = hx->index_offtype;
  int i = 0, n_buckets = (hx->index_n_buckets), n_to_fetch = 0;
  int total_keys = 0, key_count = 0;
  FD_CHUNK_REF *buckets;
  fd_lock_stream(s);
  total_keys = fd_read_4bytes(fd_start_read(s,16));
  if (total_keys==0) {
    fd_unlock_stream(s);
    *n = 0;
    return NULL;}
  buckets = u8_alloc_n(total_keys,FD_CHUNK_REF);
  if (buckets == NULL) return NULL;
  else results = u8_alloc_n(total_keys,fdtype);
  if (results == NULL) {
    fd_unlock_stream(s);
    u8_free(buckets);
    u8_seterr(fd_MallocFailed,"hashindex_fetchkeys",NULL);
    return NULL;}
  unsigned char *keybuf = u8_malloc(HX_KEYBUF_SIZE);
  ssize_t keybuf_size = HX_KEYBUF_SIZE;
  /* If we have chunk offsets in memory, we don't need to keep the
     stream locked while we get them. */
  if (hx->index_offdata) {
    fd_unlock_stream(s);
    while (i<n_buckets) {
      FD_CHUNK_REF ref = get_chunk_ref(offdata,offtype,i);
      if (ref.size>0) buckets[n_to_fetch++]=ref;
      i++;}}
  else {
    while (i<n_buckets) {
      FD_CHUNK_REF ref = fetch_chunk_ref(s,256,offtype,i);
      if (ref.size>0) buckets[n_to_fetch++]=ref;
      i++;}
    fd_unlock_stream(s);}
  qsort(buckets,n_to_fetch,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  i = 0; while (i<n_to_fetch) {
    struct FD_INBUF keyblock; int j = 0, n_keys;
    if (buckets[i].size<keybuf_size) {}
    else {
      keybuf_size = buckets[i].size;
      keybuf = u8_realloc(keybuf,keybuf_size);}
    open_block(&keyblock,hx,buckets[i].off,buckets[i].size,keybuf);
    n_keys = fd_read_zint(&keyblock);
    while (j<n_keys) {
      fdtype key; int n_vals;
      fd_read_zint(&keyblock); /* IGNORE size */
      key = read_key(hx,&keyblock);
      n_vals = fd_read_zint(&keyblock);
      results[key_count++]=key;
      if (n_vals==0) {}
      else if (n_vals==1) {
        int code = fd_read_zint(&keyblock);
        if (code==0) {
          fdtype val = fd_read_dtype(&keyblock);
          fd_decref(val);}
        else fd_read_zint(&keyblock);}
      else {
        fd_read_zint(&keyblock);
        fd_read_zint(&keyblock);}
      j++;}
    i++;}
  u8_free(keybuf);
  u8_free(buckets);
  *n = total_keys;
  return results;
}

static struct FD_KEY_SIZE *hashindex_fetchinfo(fd_index ix,fd_choice filter,int *n)
{
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
    *n = 0;
    return NULL;}
  FD_CHUNK_REF *buckets = u8_alloc_n(total_keys,FD_CHUNK_REF);
  struct FD_KEY_SIZE *sizes = u8_alloc_n(total_keys,FD_KEY_SIZE);
  if (sizes == NULL) {
    fd_unlock_stream(s);
    u8_seterr(fd_MallocFailed,"hashindex_fetchinfo",NULL);
    *n = -1;
    return NULL;}
  unsigned char *keybuf = u8_malloc(HX_KEYBUF_SIZE);
  ssize_t keybuf_size   = HX_KEYBUF_SIZE;
  /* If we don't have chunk offsets in memory, we keep the stream
     locked while we get them. */
  if (hx->index_offdata) {
    fd_unlock_stream(s);
    while (i<n_buckets) {
      FD_CHUNK_REF ref = get_chunk_ref(offdata,offtype,i);
      if (ref.size>0) buckets[n_to_fetch++]=ref;
      i++;}}
  else {
    while (i<n_buckets) {
      FD_CHUNK_REF ref = fetch_chunk_ref(s,256,offtype,i);
      if (ref.size>0) buckets[n_to_fetch++]=ref;
      i++;}
    fd_unlock_stream(s);}
  qsort(buckets,n_to_fetch,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  i = 0; while (i<n_to_fetch) {
    struct FD_INBUF keyblock; int j = 0, n_keys;
    if (buckets[i].size<keybuf_size) {}
    else {
      keybuf_size = buckets[i].size;
      keybuf      = u8_realloc(keybuf,keybuf_size);}
    open_block(&keyblock,hx,buckets[i].off,buckets[i].size,keybuf);
    n_keys = fd_read_zint(&keyblock);
    while (j<n_keys) {
      fdtype key; int n_vals;
      /* size = */ fd_read_zint(&keyblock);
      key = read_key(hx,&keyblock);
      n_vals = fd_read_zint(&keyblock);
      assert(key!=0);
      if ( (filter == NULL) || (fast_choice_containsp(key,filter)) ) {
        sizes[key_count].keysizekey = key;
        sizes[key_count].keysizenvals = n_vals;
        key_count++;}
      else fd_decref(key);
      if (n_vals==0) {}
      else if (n_vals==1) {
        int code = fd_read_zint(&keyblock);
        if (code==0) {
          fdtype val = fd_read_dtype(&keyblock);
          fd_decref(val);}
        else fd_read_zint(&keyblock);}
      else {
        fd_read_zint(&keyblock);
        fd_read_zint(&keyblock);}
      j++;}
    i++;}
  u8_free(keybuf);
  u8_free(buckets);
  *n=key_count;
  return sizes;
}

static void hashindex_getstats(struct FD_HASHINDEX *hx,
                                int *nf,int *max,int *singles,int *n2sum)
{
  fd_stream s = &(hx->index_stream);
  fd_inbuf ins = fd_readbuf(s);
  int i = 0, n_buckets = (hx->index_n_buckets), n_to_fetch = 0, total_keys = 0;
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
  buckets = u8_alloc_n(total_keys,FD_CHUNK_REF);
  /* If we don't have chunk offsets in memory, we keep the stream
     locked while we get them. */
  if (hx->index_offdata) {
    fd_unlock_stream(s);
    while (i<n_buckets) {
      FD_CHUNK_REF ref = get_chunk_ref(offdata,offtype,i);
      if (ref.size>0) buckets[n_to_fetch++]=ref;
      if (ref.size>max_keyblock_size) max_keyblock_size=ref.size;
      i++;}}
  else {
    while (i<n_buckets) {
      FD_CHUNK_REF ref = fetch_chunk_ref(s,256,offtype,i);
      if (ref.size>0) buckets[n_to_fetch++]=ref;
      if (ref.size>max_keyblock_size) max_keyblock_size=ref.size;
      i++;}
    fd_unlock_stream(s);}
  *nf = n_to_fetch;
  /* Now we actually unlock it if we kept it locked. */
  fd_unlock_index(hx);
  qsort(buckets,n_to_fetch,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  unsigned char keybuf[max_keyblock_size];
  struct FD_INBUF keyblock; int n_keys;
  i = 0; while (i<n_to_fetch) {
    open_block(&keyblock,hx,buckets[i].off,buckets[i].size,keybuf);
    n_keys = fd_read_zint(&keyblock);
    if (n_keys==1) (*singles)++;
    if (n_keys>(*max)) *max = n_keys;
    *n2sum = *n2sum+(n_keys*n_keys);
    i++;}
  if (buckets) u8_free(buckets);
}


/* Cache setting */

static void hashindex_setcache(struct FD_HASHINDEX *hx,int level)
{
  unsigned int chunk_ref_size = get_chunk_ref_size(hx);
  int stream_flags=hx->index_stream.stream_flags;

  if (level >= 2) {
    if ( (level >= 3) && (!(U8_BITP(stream_flags,FD_STREAM_MMAPPED)) ) )
      fd_setbufsize(&(hx->index_stream),-1);
    else if (U8_BITP(stream_flags,FD_STREAM_MMAPPED))
      fd_setbufsize(&(hx->index_stream),fd_filestream_bufsize);
    else {}
    if (hx->index_offdata) return;
    else {
      fd_stream s = &(hx->index_stream);
      unsigned int n_buckets = hx->index_n_buckets;
      unsigned int *buckets, *newmmap;
      fd_lock_index(hx);
      if (hx->index_offdata) {
        fd_unlock_index(hx);
        return;}
#if HAVE_MMAP
      newmmap=
        mmap(NULL,(n_buckets*chunk_ref_size)+256,
             PROT_READ,MMAP_FLAGS,s->stream_fileno,0);
      if ((newmmap == NULL) || (newmmap == MAP_FAILED)) {
        u8_log(LOG_WARN,u8_strerror(errno),
               "hashindex_setcache:mmap %s",hx->index_source);
        hx->index_offdata = NULL;
        errno = 0;}
      else hx->index_offdata = buckets = newmmap+64;
#else
      fd_lock_stream(s)
      stream_start_read(s);
      ht_buckets = u8_alloc_n(chunk_ref_size*(hx->index_n_buckets),unsigned int);
      fd_setpos(s,256);
      retval = fd_read_ints
        (s,(chunk_ref_size/4)*(hx->index_n_buckets),ht_buckets);
      if (retval<0) {
        u8_log(LOG_WARN,u8_strerror(errno),
               "hashindex_setcache:read offsets %s",hx->index_source);
        hx->index_offdata = NULL; errno = 0;}
      else hx->index_offdata = ht_buckets;
      fd_unlock_stream(s)
#endif
      fd_unlock_index(hx);}}

  else if (level < 2) {

    if (U8_BITP(stream_flags,FD_STREAM_MMAPPED))
      fd_setbufsize(&(hx->index_stream),fd_filestream_bufsize);

    if (hx->index_offdata == NULL)
      return;
    else {
      int retval;
      unsigned int *offdata = NULL;
      fd_lock_index(hx);
      if (hx->index_offdata == NULL) {
        fd_unlock_index(hx);
        return;}
      offdata = hx->index_offdata;
      hx->index_offdata = NULL;
#if HAVE_MMAP
      retval = munmap(offdata-64,((hx->index_n_buckets)*chunk_ref_size)+256);
      if (retval<0) {
        u8_log(LOG_WARN,u8_strerror(errno),
               "hashindex_setcache:munmap %s",hx->index_source);
        errno = 0;}
#else
      u8_free(offdata);
#endif
      fd_unlock_index(hx);}}
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

   We extend each keybucket (using 'extend_keybucket') with the
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

static int process_edits(struct FD_HASHINDEX *hx,
                        fd_hashtable adds,fd_hashtable edits,
                         fd_hashset replaced_keys,
                         struct COMMIT_SCHEDULE *s,
                         int i)
{
  fd_hashtable cache = &(hx->index_cache);
  fdtype *drops = u8_alloc_n((edits->table_n_keys),fdtype), *drop_values;
  int j = 0, n_drops = 0, oddkeys = 0;
  struct FD_HASH_BUCKET **scan = edits->ht_buckets;
  struct FD_HASH_BUCKET **lim = scan+edits->ht_n_buckets;
  while (scan < lim)
    if (*scan) {
      struct FD_HASH_BUCKET *e = *scan;
      int n_keyvals = e->fd_n_entries;
      struct FD_KEYVAL *kvscan = &(e->kv_val0);
      struct FD_KEYVAL *kvlimit = kvscan+n_keyvals;
      while (kvscan<kvlimit) {
        fdtype key = kvscan->kv_key;
        if (FD_PAIRP(key)) {
          fdtype real_key = FD_CDR(key); fd_incref(real_key);
          if ((FD_CAR(key)) == set_symbol) {
            fd_hashset_add(replaced_keys,real_key);
            if ((oddkeys==0) && (FD_PAIRP(real_key)) &&
                ((FD_OIDP(FD_CAR(real_key))) ||
                 (FD_SYMBOLP(FD_CAR(real_key))))) {
              if (get_slotid_index(hx,key)<0) oddkeys = 1;}
            fdtype save_value = fd_incref(kvscan->kv_val);
            fdtype added = fd_hashtable_get_nolock(adds,real_key,FD_EMPTY_CHOICE);
            FD_ADD_TO_CHOICE(save_value,added);
            s[i].commit_key = real_key;
            s[i].commit_values = fd_simplify_choice(save_value);
            s[i].commit_replace = 1;
            i++;}
          else if ((FD_CAR(key)) == drop_symbol) {
            fdtype cached = fd_hashtable_get(cache,real_key,FD_VOID);
            fd_hashset_add(replaced_keys,real_key);
            if (FD_VOIDP(cached))
              drops[n_drops++]=FD_CDR(key);
            else {
              fdtype added=
                fd_hashtable_get_nolock(adds,real_key,FD_EMPTY_CHOICE);
              /* This uses up the reference to added */
              FD_ADD_TO_CHOICE(cached,added);
              s[i].commit_key = real_key;
              s[i].commit_values = fd_difference(cached,kvscan->kv_val);
              s[i].commit_replace = 1;
              fd_decref(cached);
              i++;}}
          else {
            u8_log(LOGWARN,"CorruptedIndex",
                   "The edits table for %s contained an invalid key %q",
                   hx->indexid,real_key);
            fd_decref(real_key);}}
        else {}
        kvscan++;}
      scan++;}
    else scan++;
  
  /* Record if there are odd keys */
  if (oddkeys)
    hx->fd_storage_xformat |= (FD_HASHINDEX_ODDKEYS);
  
  /* Get the current values of all the keys you're dropping, to turn
     the drops into stores. */
  drop_values = hashindex_fetchn_inner((fd_index)hx,n_drops,drops,1,1);
  
  /* Now, scan the edits again, in the same order as before, with 'j'
     tracking which dropped key you're processing. */
  scan = edits->ht_buckets;
  lim = scan+edits->ht_n_buckets;
  j = 0; while (scan < lim)
    if (*scan) {
      struct FD_HASH_BUCKET *e = *scan; int n_keyvals = e->fd_n_entries;
      struct FD_KEYVAL *kvscan = &(e->kv_val0), *kvlimit = kvscan+n_keyvals;
      while (kvscan<kvlimit) {
        fdtype key = kvscan->kv_key;
        if ((FD_PAIRP(key)) && ((FD_CAR(key)) == drop_symbol)) {
          fdtype real_key = FD_CDR(key); fd_incref(real_key);
          if ((j<n_drops) && (real_key == drops[j])) {
            fdtype cached = drop_values[j];
            fdtype added = fd_hashtable_get_nolock(adds,real_key,FD_EMPTY_CHOICE);
            /* This consumes the reference to 'added' */
            FD_ADD_TO_CHOICE(cached,added);
            /* Add a new commit to the schedule */
            s[i].commit_key = real_key;
            /* Now remove the dropped values from the current value
               and save the drop as a store. */
            s[i].commit_values = fd_difference(cached,kvscan->kv_val);
            s[i].commit_replace = 1;
            fd_decref(cached);
            i++; j++;}}
        kvscan++;}
      scan++;}
    else scan++;
  u8_free(drops); u8_free(drop_values);
  return i;
}

static int process_adds(struct FD_HASHINDEX *hx,
                        fd_hashtable adds,fd_hashtable edits,
                        fd_hashset replaced_keys,
                        struct COMMIT_SCHEDULE *s,int i)
{
  int oddkeys = ((hx->fd_storage_xformat)&(FD_HASHINDEX_ODDKEYS));
  struct FD_HASH_BUCKET **scan = adds->ht_buckets;
  struct FD_HASH_BUCKET **lim = scan+adds->ht_n_buckets;
  while (scan < lim)
    if (*scan) {
      struct FD_HASH_BUCKET *e = *scan; int n_keyvals = e->fd_n_entries;
      struct FD_KEYVAL *kvscan = &(e->kv_val0), *kvlimit = kvscan+n_keyvals;
      /* We clear the adds as we go */
      while (kvscan<kvlimit) {
        fdtype key = kvscan->kv_key, val = kvscan->kv_val;
        if (!(fd_hashset_get(replaced_keys,key))) {
          if ((oddkeys==0) && (FD_PAIRP(key)) &&
              ((FD_OIDP(FD_CAR(key))) || (FD_SYMBOLP(FD_CAR(key))))) {
            if (get_slotid_index(hx,key)<0) oddkeys = 1;}
          s[i].commit_key = key;
          s[i].commit_values = fd_simplify_choice(val);
          s[i].commit_replace = 0;
          i++;}
        else {fd_decref(val); fd_decref(key);}
        kvscan->kv_key = FD_VOID; kvscan->kv_val = FD_VOID;
        kvscan++;}
      e->fd_n_entries = 0;
      scan++;}
    else scan++;
  if (oddkeys)
    hx->fd_storage_xformat |= (FD_HASHINDEX_ODDKEYS);
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
    entry->ke_dtrep_size = dt_size; entry->ke_dtstart = in->bufread;
    in->bufread = in->bufread+dt_size;
    entry->ke_nvals = n_values = fd_read_zint(in);
    if (n_values==0) entry->ke_values = FD_EMPTY_CHOICE;
    else if (n_values==1) 
      entry->ke_values = read_zvalue(hx,in);
    else {
      entry->ke_values = FD_VOID;
      entry->ke_vref.off = fd_read_zint(in);
      entry->ke_vref.size = fd_read_zint(in);}
    i++;}
}

FD_FASTOP FD_CHUNK_REF write_value_block
(struct FD_HASHINDEX *hx,fd_stream stream,
 fdtype values,fdtype extra,
 fd_off_t cont_off,fd_off_t cont_size,fd_off_t startpos)
{
  struct FD_OUTBUF *outstream = fd_writebuf(stream);
  FD_CHUNK_REF retval; fd_off_t endpos = startpos;
  if (FD_CHOICEP(values)) {
    int full_size = FD_CHOICE_SIZE(values)+((FD_VOIDP(extra))?0:1);
    endpos = endpos+fd_write_zint(outstream,full_size);
    if (!(FD_VOIDP(extra)))
      endpos = endpos+write_zvalue(hx,outstream,extra);
    {FD_DO_CHOICES(value,values)
        endpos = endpos+write_zvalue(hx,outstream,value);}}
  else if (FD_VOIDP(extra)) {
    endpos = endpos+fd_write_zint(outstream,1);
    endpos = endpos+write_zvalue(hx,outstream,values);}
  else {
    endpos = endpos+fd_write_zint(outstream,2);
    endpos = endpos+write_zvalue(hx,outstream,extra);
    endpos = endpos+write_zvalue(hx,outstream,values);}
  endpos = endpos+fd_write_zint(outstream,cont_size);
  if (cont_size)
    endpos = endpos+fd_write_zint(outstream,cont_off);
  retval.off = startpos; retval.size = endpos-startpos;
  return retval;
}

/* This adds new entries to a keybucket, writing value blocks to the
   file where neccessary (more than one value). */
FD_FASTOP fd_off_t extend_keybucket
  (fd_hashindex hx,struct KEYBUCKET *kb,
   struct COMMIT_SCHEDULE *schedule,int i,int j,
   fd_outbuf newkeys,int *new_valueblocksp,
   fd_off_t endpos,ssize_t maxpos)
{
  int k = i, free_keyvecs = 0;
  int _keyoffs[16], _keysizes[16], *keyoffs, *keysizes;
  if (FD_EXPECT_FALSE((j-i)>16) )  {
    keyoffs = u8_alloc_n((j-i),int);
    keysizes = u8_alloc_n((j-i),int);
    free_keyvecs = 1;}
  else {keyoffs=_keyoffs; keysizes=_keysizes;}
  while (k<j) {
    int off;
    keyoffs[k-i]=off = newkeys->bufwrite-newkeys->buffer;
    write_zkey(hx,newkeys,schedule[k].commit_key);
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
          ke[key_i].ke_values = FD_EMPTY_CHOICE;
          ke[key_i].ke_vref.off = 0;
          ke[key_i].ke_vref.size = 0;}
        else if (n_values==1) {
          /* If there is only one value, we write it as part of the
             keyblock (that's what being in .ke_values means) */
          fdtype current = ke[key_i].ke_values;
          ke[key_i].ke_values = fd_incref(schedule[k].commit_values);
          fd_decref(current);
          ke[key_i].ke_vref.off = 0;
          ke[key_i].ke_vref.size = 0;}
        else {
          ke[key_i].ke_values = FD_VOID;
          ke[key_i].ke_vref=
            write_value_block(hx,&(hx->index_stream),
                              schedule[k].commit_values,FD_VOID,
                              0,0,endpos);
          (*new_valueblocksp)++;
          endpos = ke[key_i].ke_vref.off+ke[key_i].ke_vref.size;}
        if (endpos>=maxpos) {
          if (free_keyvecs) {
            u8_free(keyoffs); u8_free(keysizes);
            u8_seterr(fd_DataFileOverflow,"extend_keybucket",
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
        if (n_values==0) {}
        else if (n_values==1) {
          /* This is the special case is where there is one current value
             and we are adding to that.  The value block we write must
             contain both the current singleton value and whatever values
             we are adding.  We pass this as the fourth (extra) argument
             to write_value_block.  */
          fdtype current = ke[key_i].ke_values;
          ke[key_i].ke_vref=
            write_value_block(hx,&(hx->index_stream),schedule[k].commit_values,
                              current,0,0,endpos);
          endpos = ke[key_i].ke_vref.off+ke[key_i].ke_vref.size;
          fd_decref(current);
          ke[key_i].ke_values = FD_VOID;}
        else {
          ke[key_i].ke_vref=
            write_value_block(hx,&(hx->index_stream),
                              schedule[k].commit_values,FD_VOID,
                              ke[key_i].ke_vref.off,ke[key_i].ke_vref.size,
                              endpos);
          /* We void the values field because there's a values block now. */
          if (ke[key_i].ke_values!=FD_VOID)
            u8_log(LOGWARN,"NotVoid",
                   "This value for key %d is %q, not VOID as expected",
                   key_i,ke[key_i].ke_values);
          endpos = ke[key_i].ke_vref.off+ke[key_i].ke_vref.size;}
        if (endpos>=maxpos) {
          if (free_keyvecs) {
            u8_free(keyoffs); u8_free(keysizes);
            u8_seterr(fd_DataFileOverflow,"extend_keybucket",
                      u8_mkstring("%s: %lld >= %lld",
                                  hx->indexid,endpos,maxpos));
            return -1;}}
        break;}}
    /* This is the case where we are adding a new key to the bucket. */
    if (key_i == n_keys) { /* This should always be true */
      ke[n_keys].ke_dtrep_size = keysize;
      ke[n_keys].ke_nvals = n_values =
        FD_CHOICE_SIZE(schedule[k].commit_values);
      ke[n_keys].ke_dtstart = newkeys->buffer+keyoffs[k-i];
      if (n_values==0) ke[n_keys].ke_values = FD_EMPTY_CHOICE;
      else if (n_values==1)
        /* As above, we don't need to incref this because any value in
           it comes from the key schedule, so we won't decref it when
           we reclaim the keybuckets. */
        ke[n_keys].ke_values = fd_incref(schedule[k].commit_values);
      else {
        ke[n_keys].ke_values = FD_VOID;
        ke[n_keys].ke_vref=
          write_value_block(hx,&(hx->index_stream),
                            schedule[k].commit_values,FD_VOID,
                            0,0,endpos);
        endpos = ke[key_i].ke_vref.off+ke[key_i].ke_vref.size;
        if (endpos>=maxpos) {
          if (free_keyvecs) {
            u8_free(keyoffs); u8_free(keysizes);
            u8_seterr(fd_DataFileOverflow,"extend_keybucket",
                      u8_mkstring("%s: %lld >= %lld",
                                  hx->indexid,endpos,maxpos));
            return -1;}}}
      kb->kb_n_keys++;}
    k++;}
  return endpos;
}

FD_FASTOP fd_off_t write_keybucket
(fd_hashindex hx,
 fd_stream stream,
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
      endpos = endpos+write_zvalue(hx,outstream,ke[i].ke_values);
      fd_decref(ke[i].ke_values); 
      ke[i].ke_values = FD_VOID;}
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
  (fd_hashindex hx,int bucket,FD_CHUNK_REF ref,int extra)
{
  int n_keys;
  struct FD_INBUF keyblock;
  struct KEYBUCKET *kb;
  /* We allocate this dynamically because we're going to store it on
     the keybucket. */
  if (ref.size>0) {
    unsigned char *keybuf = u8_zmalloc(ref.size);
    open_block(&keyblock,hx,ref.off,ref.size,keybuf);
    n_keys = fd_read_zint(&keyblock);
    kb = (struct KEYBUCKET *)
      u8_malloc(sizeof(struct KEYBUCKET)+
                sizeof(struct KEYENTRY)*((extra+n_keys)-1));
    kb->kb_bucketno = bucket;
    kb->kb_n_keys = n_keys; kb->kb_keybuf = keybuf;
    parse_keybucket(hx,kb,&keyblock,n_keys);}
  else {
    kb = (struct KEYBUCKET *)
      u8_malloc(sizeof(struct KEYBUCKET)+
                sizeof(struct KEYENTRY)*(extra-1));
    kb->kb_bucketno = bucket;
    kb->kb_n_keys = 0; kb->kb_keybuf = NULL;}
  return kb;
}

static int update_hashindex_ondisk
  (fd_hashindex hx,unsigned int flags,unsigned int new_keys,
   unsigned int changed_buckets,struct BUCKET_REF *bucket_locs);

static void free_keybuckets(int n,struct KEYBUCKET **keybuckets);

static int hashindex_commit(struct FD_INDEX *ix)
{
  int new_keys = 0, n_keys, new_buckets = 0;
  int schedule_max, changed_buckets = 0, total_keys = 0;
  int new_keyblocks = 0, new_valueblocks = 0;
  struct FD_HASHINDEX *hx = (struct FD_HASHINDEX *)ix;
  struct FD_STREAM *stream = &(hx->index_stream);
  struct FD_OUTBUF *outstream = fd_writebuf(stream);
  struct BUCKET_REF *bucket_locs;
  struct FD_HASHTABLE adds, edits;
  fd_offset_type offtype = hx->index_offtype;
  if (!((offtype == FD_B32)||(offtype = FD_B40)||(offtype = FD_B64))) {
    u8_log(LOG_WARN,"Corrupted hashindex (in memory)",
           "Bad offset type code=%d for %s",(int)offtype,hx->indexid);
    u8_seterr("CorruptedHashIndex","hashindex_commit",u8_strdup(ix->indexid));
    return -1;}
  fd_off_t recovery_start, recovery_pos;
  ssize_t endpos, maxpos = get_maxpos(hx);
  double started = u8_elapsed_time();
  unsigned int n_buckets = hx->index_n_buckets;
  unsigned int *offdata = hx->index_offdata;
  fd_lock_index(hx);
  fd_lock_stream(stream);
  fd_write_lock_table(&(hx->index_adds));
  fd_write_lock_table(&(hx->index_edits));
  fd_swap_hashtable(&(hx->index_adds),&adds,
                    hx->index_adds.table_n_keys,
                    1);
  fd_swap_hashtable(&(hx->index_edits),&edits,
                    hx->index_edits.table_n_keys,
                    1);
  fd_unlock_table(&(hx->index_adds));
  fd_unlock_table(&(hx->index_edits));
  schedule_max = adds.table_n_keys+edits.table_n_keys;
  bucket_locs = u8_alloc_n(schedule_max,struct BUCKET_REF);
  /* This is where we write everything to disk */
  {
    int sched_i = 0, bucket_i = 0;
    int schedule_size = 0;
    struct COMMIT_SCHEDULE *schedule=
      u8_alloc_n(schedule_max,struct COMMIT_SCHEDULE);
    struct KEYBUCKET **keybuckets=
      u8_alloc_n(schedule_max,struct KEYBUCKET *);
    struct FD_HASHSET replaced_keys;
    struct FD_OUTBUF out, newkeys;
    /* First, we populate the commit schedule.
       The 'replaced_keys' hashset contains keys that are edited.
       We process all of the edits, getting values if neccessary.
       Then we process all the adds. */
    fd_init_hashset(&replaced_keys,3*(hx->index_edits.table_n_keys),
                    FD_STACK_CONS);
#if FD_DEBUG_HASHINDEXES
    u8_message("Adding %d edits to the schedule",hx->index_edits.table_n_keys);
#endif
    /* Get all the keys we need to write.  */
    schedule_size = process_edits(hx,&adds,&edits,&replaced_keys,
                                  schedule,schedule_size);
#if FD_DEBUG_HASHINDEXES
    u8_message("Adding %d adds to the schedule",hx->index_adds.table_n_keys);
#endif
    schedule_size = process_adds(hx,&adds,&edits,&replaced_keys,
                               schedule,schedule_size);
    fd_recycle_hashset(&replaced_keys);

    /* We're done with these tables */
    fd_reset_hashtable(&adds,0,0);
    fd_reset_hashtable(&edits,0,0);

    /* The commit schedule is now filled and we start generating a
       bucket schedule. */
    /* We're going to write DTYPE representations of keys and values,
       so we create streams to buffer them */
    FD_INIT_BYTE_OUTPUT(&out,1024);
    FD_INIT_BYTE_OUTPUT(&newkeys,schedule_max*16);
    if ((hx->fd_storage_xformat)&(FD_HASHINDEX_DTYPEV2)) {
      out.buf_flags = out.buf_flags|FD_USE_DTYPEV2;
      newkeys.buf_flags = newkeys.buf_flags|FD_USE_DTYPEV2;}

    /* Compute all the buckets for all the keys */
#if FD_DEBUG_HASHINDEXES
    u8_message("Computing the buckets for %d scheduled keys",schedule_size);
#endif

    /* Compute the hashes and the buckets for all of the keys
       in the commit schedule. */
    sched_i = 0; while (sched_i<schedule_size) {
      fdtype key = schedule[sched_i].commit_key; int bucket;
      out.bufwrite = out.buffer;
      write_zkey(hx,&out,key);
      schedule[sched_i].commit_bucket = bucket =
        hash_bytes(out.buffer,out.bufwrite-out.buffer)%n_buckets;
      sched_i++;}
    /* Get all the bucket locations.  It may be that we can fold this
       into the phase above when we have the offsets table in
       memory. */

#if FD_DEBUG_HASHINDEXES
    u8_message("Fetching bucket locations");
#endif
    qsort(schedule,schedule_size,sizeof(struct COMMIT_SCHEDULE),
          sort_cs_by_bucket);
    sched_i = 0; bucket_i = 0; while (sched_i<schedule_size) {
      int bucket = schedule[sched_i].commit_bucket;
      int bucket_first_key = sched_i;
      int bucket_last_key = sched_i;
      bucket_locs[changed_buckets].bucketno = bucket;
      bucket_locs[changed_buckets].bck_ref = (offdata)?
        (get_chunk_ref(offdata,offtype,bucket)):
        (fetch_chunk_ref(stream,256,offtype,bucket));
      while ( (bucket_last_key<schedule_size) &&
              (schedule[bucket_last_key].commit_bucket == bucket) )
        bucket_last_key++;
      bucket_locs[changed_buckets].max_new=
        bucket_last_key-bucket_first_key;
      sched_i = bucket_last_key;
      changed_buckets++;}

    /* Now we have all the bucket locations, which we'll read in
       order. */

    /* Process all of the buckets in order, reading each keyblock.  We
       may be able to combine this with extending the bucket below,
       but that would entail moving the writing of values out of the
       bucket extension (since both want to get at the file) Could we
       have two pointers into the file?  */

#if FD_DEBUG_HASHINDEXES
    u8_message("Reading all the %d changed buckets in order",
               changed_buckets);
#endif
    qsort(bucket_locs,changed_buckets,sizeof(struct BUCKET_REF),
          sort_br_by_off);
    bucket_i = 0; while (bucket_i<changed_buckets) {
      keybuckets[bucket_i]=
        read_keybucket(hx,bucket_locs[bucket_i].bucketno,
                       bucket_locs[bucket_i].bck_ref,
                       bucket_locs[bucket_i].max_new);
      if ((keybuckets[bucket_i]->kb_n_keys)==0) new_buckets++;
      bucket_i++;}

    /* Now all the keybuckets have been read and buckets have been
       created for keys that didn't have buckets before. */
#if FD_DEBUG_HASHINDEXES
    u8_message("Created %d new buckets",new_buckets);
#endif
    qsort(schedule,schedule_size,sizeof(struct COMMIT_SCHEDULE),
          sort_cs_by_bucket);
    qsort(keybuckets,changed_buckets,sizeof(struct KEYBUCKET *),
          sort_kb_by_bucket);
    /* bucket_locs is currently sorted by offset */
    qsort(bucket_locs,changed_buckets,sizeof(struct BUCKET_REF),
          sort_br_by_bucket);

#if FD_DEBUG_HASHINDEXES
    u8_message("Extending for %d keys over %d buckets",
               schedule_size,changed_buckets);
#endif

    /* March along the commit schedule (keys) and keybuckets (buckets)
       in parallel, extending each bucket.  This is where values are
       written out and their offsets stored in the loaded bucket
       structure. */
    sched_i = 0; bucket_i = 0; endpos = fd_endpos(stream);
    while (sched_i<schedule_size) {
      struct KEYBUCKET *kb = keybuckets[bucket_i];
      int bucket = schedule[sched_i].commit_bucket;
      int j = sched_i, cur_keys = kb->kb_n_keys;
      assert(bucket == kb->kb_bucketno);
      while ((j<schedule_size) && (schedule[j].commit_bucket == bucket)) j++;
      /* This may write values to disk, so we use the returned endpos */
      endpos = extend_keybucket(hx,kb,schedule,sched_i,j,
                                &newkeys,&new_valueblocks,
                                endpos,maxpos);
      CHECK_POS(endpos,&(hx->index_stream));
      new_keys = new_keys+(kb->kb_n_keys-cur_keys);
      {
        fd_off_t startpos = endpos;
        /* This writes the keybucket itself. */
        endpos = write_keybucket(hx,stream,kb,endpos,maxpos);
        new_keyblocks++;
        if (endpos<0) {
          u8_free(bucket_locs);
          u8_free(schedule);
          u8_free(keybuckets);
          fd_unlock_index(hx);
          fd_unlock_stream(stream);
          return -1;}
        CHECK_POS(endpos,&(hx->index_stream));
        bucket_locs[bucket_i].bck_ref.off = startpos;
        bucket_locs[bucket_i].bck_ref.size = endpos-startpos;}
      sched_i = j; bucket_i++;}
    fd_flush_stream(&(hx->index_stream));

#if FD_DEBUG_HASHINDEXES
    u8_message("Cleaning up");
#endif

    /* Free all the keybuckets */
    free_keybuckets(changed_buckets,keybuckets);

    /* Now we free the keys and values in the schedule. */
    sched_i = 0; while (sched_i<schedule_size) {
      fdtype key = schedule[sched_i].commit_key;
      fdtype v = schedule[sched_i].commit_values;
      fd_decref(key);
      fd_decref(v);
      sched_i++;}
    u8_free(schedule);
    u8_free(out.buffer);
    u8_free(newkeys.buffer);
    n_keys = schedule_size;}

  /* This writes the new offset information */
  if (fd_acid_files) {
    int i = 0;
#if FD_DEBUG_HASHINDEXES
    u8_message("Writing recovery data");
#endif
    /* Write the new offsets information to the end of the file
       for recovery if we die while doing the actual write. */
    /* Start by getting the total number of keys in the new file. */
    total_keys = fd_read_4bytes(fd_start_read(stream,FD_HASHINDEX_KEYCOUNT_POS))
      +new_keys;
    recovery_start = fd_endpos(stream);
    outstream = fd_writebuf(stream);
    fd_write_4bytes(outstream,hx->fd_storage_xformat);
    fd_write_4bytes(outstream,total_keys);
    fd_write_4bytes(outstream,changed_buckets);
    while (i<changed_buckets) {
      fd_write_8bytes(outstream,bucket_locs[i].bck_ref.off);
      fd_write_4bytes(outstream,bucket_locs[i].bck_ref.size);
      fd_write_4bytes(outstream,bucket_locs[i].bucketno);
      i++;}
    fd_write_8bytes(outstream,recovery_start);
    fd_setpos(stream,0);
    fd_write_4bytes(outstream,FD_HASHINDEX_TO_RECOVER);
    fd_flush_stream(stream);
    fsync(stream->stream_fileno);
  }

#if FD_DEBUG_HASHINDEXES
  u8_message("Writing offset data changes");
#endif
  update_hashindex_ondisk
    (hx,hx->fd_storage_xformat,total_keys,changed_buckets,bucket_locs);
  if (fd_acid_files) {
    int retval = 0;
#if FD_DEBUG_HASHINDEXES
    u8_message("Erasing old recovery information");
#endif
    fd_flush_stream(stream);
    fsync(stream->stream_fileno);
    /* Now erase the recovery information, since we don't need it
       anymore. */
    recovery_pos = fd_read_8bytes(fd_start_read(stream,-8));
    if (recovery_pos!=recovery_start) {
      u8_log(LOG_ERR,"hashindex_commit",
             "Trouble truncating recovery information for %s",
             hx->indexid);}
    retval = ftruncate(stream->stream_fileno,recovery_pos);
    if (retval<0)
      u8_log(LOG_ERR,"hashindex_commit",
             "Trouble truncating recovery information for %s",
             hx->indexid);}

#if FD_DEBUG_HASHINDEXES
  u8_message("Resetting tables");
#endif

  /* Free the bucket locations */
  u8_free(bucket_locs);

  /* And unlock all the locks. */
  fd_unlock_stream(stream);
  fd_unlock_index(hx);

  u8_log(fd_storage_loglevel,"HashIndexCommit",
         "Saved mappings for %d keys (%d/%d new/total) to %s in %f secs",
         n_keys,new_keys,total_keys,
         ix->indexid,u8_elapsed_time()-started);

#if FD_DEBUG_HASHINDEXES
  u8_message("Returning from hashindex_commit()");
#endif

  return n_keys;
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
  u8_free(keybuckets);
}

static int update_hashindex_ondisk
  (fd_hashindex hx,unsigned int flags,unsigned int cur_keys,
   unsigned int changed_buckets,struct BUCKET_REF *bucket_locs)
{
  struct FD_STREAM *stream = &(hx->index_stream);
  struct FD_OUTBUF *outstream = fd_writebuf(stream);
  int i = 0; 
  unsigned int *current = hx->index_offdata, *offdata = NULL;
  unsigned int n_buckets = hx->index_n_buckets;
  unsigned int chunk_ref_size = get_chunk_ref_size(hx);
  ssize_t offdata_byte_length = n_buckets*chunk_ref_size;
  if (current) {
#if HAVE_MMAP
    unsigned int *memblock=
      mmap(NULL,256+offdata_byte_length,
           PROT_READ|PROT_WRITE,
           MMAP_FLAGS,
           hx->index_stream.stream_fileno,
           0);
    if ((memblock==NULL) || (memblock == MAP_FAILED)) {
      u8_graberrno("update_hashindex_ondisk:mmap",u8_strdup(hx->indexid));
      return -1;}
    else offdata = memblock+64;
#else
    size_t offdata_length = n_buckets*chunk_ref_size;
    offdata = u8_malloc(offdata_length);
    int rv = fd_read_ints(stream,offdata_length/4,offdata);
    if (rv<0) {
      u8_graberrno("update_hashindex_ondisk:fd_read_ints",u8_strdup(hx->indexid));
      u8_free(offdata);
      return -1;}
#endif
  }
  /* Update the buckets if you have them */
  if ((offdata) && (hx->index_offtype == FD_B64)) {
    while (i<changed_buckets) {
      unsigned int word1, word2, word3, bucket = bucket_locs[i].bucketno;
      word1 = (((bucket_locs[i].bck_ref.off)>>32)&(0xFFFFFFFF));
      word2 = (((bucket_locs[i].bck_ref.off))&(0xFFFFFFFF));
      word3 = (bucket_locs[i].bck_ref.size);
#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
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
#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
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
      convert_FD_B40_ref(bucket_locs[i].bck_ref,&word1,&word2);
#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
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
      fd_start_write(stream,bucket_pos);
      fd_write_8bytes(outstream,bucket_locs[i].bck_ref.off);
      fd_write_4bytes(outstream,bucket_locs[i].bck_ref.size);
      i++;}}
  else if (hx->index_offtype == FD_B32) {
    size_t bucket_start = 256;
    size_t bytes_in_bucket = 2*SIZEOF_INT;
    while (i<changed_buckets) {
      unsigned int bucket_no = bucket_locs[i].bucketno;
      unsigned int bucket_pos = bucket_start+bytes_in_bucket*bucket_no;
      fd_start_write(stream,bucket_pos);
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
      convert_FD_B40_ref(bucket_locs[i].bck_ref,&word1,&word2);
      fd_start_write(stream,bucket_pos);
      fd_write_4bytes(outstream,word1);
      fd_write_4bytes(outstream,word2);
      i++;}}
  /* The offsets have now been updated in memory */
  if (offdata) {
#if (HAVE_MMAP)
    /* If you have MMAP, make them unwritable which swaps them out to
       the file. */
    int retval = msync(offdata-64,
                     256+offdata_byte_length,
                     MS_SYNC|MS_INVALIDATE);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "update_hashindex_ondisk:msync %s",hx->indexid);
      u8_graberrno("update_hashindex_ondisk:msync",u8_strdup(hx->indexid));}
    retval = munmap(offdata-64,256+offdata_byte_length);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "update_hashindex_ondisk:munmap %s",hx->indexid);
      u8_graberrno("update_hashindex_ondisk:msync",u8_strdup(hx->indexid));}
#else
    struct FD_OUTBUF *out = fd_start_write(stream,256);
    if (hx->index_offtype == FD_B64)
      fd_write_ints(outstream,3*SIZEOF_INT*n_buckets,offdata);
    else fd_write_ints(outstream,2*SIZEOF_INT*n_buckets,offdata);
#endif
  }
  /* Write any changed flags */
  fd_write_4bytes_at(stream,flags,8);
  fd_write_4bytes_at(stream,cur_keys,16);
  fd_write_4bytes_at(stream,FD_HASHINDEX_MAGIC_NUMBER,0);
  fd_flush_stream(stream);
  return 0;
}

static int recover_hashindex(struct FD_HASHINDEX *hx)
{
  fd_off_t recovery_pos;
  struct FD_STREAM *s = &(hx->index_stream);
  fd_inbuf in = fd_readbuf(s);
  struct BUCKET_REF *bucket_locs;
  unsigned int i = 0, flags, cur_keys, n_buckets, retval;
  fd_endpos(s); fd_movepos(s,-8);
  recovery_pos = fd_read_8bytes(in);
  fd_setpos(s,recovery_pos);
  flags = fd_read_4bytes(in);
  cur_keys = fd_read_4bytes(in);
  n_buckets = fd_read_4bytes(in);
  bucket_locs = u8_alloc_n(n_buckets,struct BUCKET_REF);
  while (i<n_buckets) {
    bucket_locs[i].bucketno = fd_read_4bytes(in);
    bucket_locs[i].bck_ref.off = fd_read_8bytes(in);
    bucket_locs[i].bck_ref.size = fd_read_4bytes(in);
    i++;}
  retval = update_hashindex_ondisk(hx,flags,cur_keys,n_buckets,bucket_locs);
  u8_free(bucket_locs);
  return retval;
}


/* Miscellaneous methods */

static void hashindex_close(fd_index ix)
{
  struct FD_HASHINDEX *hx = (struct FD_HASHINDEX *)ix;
  unsigned int chunk_ref_size = get_chunk_ref_size(hx);
  u8_log(LOG_DEBUG,"HASHINDEX","Closing hash index %s",ix->indexid);
  fd_lock_index(hx);
  if (hx->index_offdata) {
#if HAVE_MMAP
    int retval=
      munmap(hx->index_offdata-64,(chunk_ref_size*hx->index_n_buckets)+256);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "hashindex_close:munmap %s",hx->index_source);
      errno = 0;}
#else
    u8_free(hx->index_offdata);
#endif
    hx->index_offdata = NULL;
    hx->index_cache_level = -1;}
  fd_close_stream(&(hx->index_stream),0);
  u8_log(LOG_DEBUG,"HASHINDEX","Closed hash index %s",ix->indexid);
  fd_unlock_index(hx);
}

/* Creating a hash ksched_i handler */

static int interpret_hashindex_flags(fdtype opts)
{
  int flags = 0;
  fdtype offtype = fd_intern("OFFTYPE");
  if ( fd_testopt(opts,offtype,fd_intern("B64"))  ||
       fd_testopt(opts,offtype,FD_INT(64)))
    flags |= (FD_B64<<4);
  else if ( fd_testopt(opts,offtype,fd_intern("B40"))  ||
            fd_testopt(opts,offtype,FD_INT(40)))
    flags |= (FD_B40<<4);
  else if ( fd_testopt(opts,offtype,fd_intern("B32"))  ||
            fd_testopt(opts,offtype,FD_INT(32)))
    flags |= (FD_B32<<4);
  else flags |= (FD_B40<<4);

  if (fd_testopt(opts,fd_intern("DTYPEV2"),FD_VOID))
    flags |= FD_HASHINDEX_DTYPEV2;

  return flags;
}

static int good_initval(fdtype val)
{
  return ((FD_VOIDP(val))||(FD_FALSEP(val))||(FD_DEFAULTP(val))||
          (FD_VECTORP(val)));
}

static fd_index hashindex_create(u8_string spec,void *typedata,
                                  fd_storage_flags flags,fdtype opts)
{
  int rv = 0;
  fdtype slotids_init = fd_getopt(opts,fd_intern("SLOTIDS"),FD_VOID);
  fdtype baseoids_init = fd_getopt(opts,fd_intern("BASEOIDS"),FD_VOID);
  fdtype nbuckets_arg = fd_getopt(opts,fd_intern("SLOTS"),
                                  fd_getopt(opts,FDSYM_SIZE,
                                            FD_INT(hash_index_default_size)));
  fdtype hashconst = fd_getopt(opts,fd_intern("HASHCONST"),FD_FIXZERO);
  if (!(FD_UINTP(nbuckets_arg))) {
    fd_seterr("InvalidBucketCount","hashindex_create",spec,nbuckets_arg);
    rv = -1;}
  else if (!(good_initval(baseoids_init))) {
    fd_seterr("InvalidBaseOIDs","hashindex_create",spec,baseoids_init);
    rv = -1;}
  else if (!(good_initval(slotids_init))) {
    fd_seterr("InvalidSlotIDs","hashindex_create",spec,slotids_init);
    rv = -1;}
  else if (!(FD_INTEGERP(hashconst))) {
    fd_seterr("InvalidHashConst","hashindex_create",spec,hashconst);
    rv = -1;}
  else {}
  if (rv<0)
    return NULL;
  else rv = make_hashindex
    (spec,FD_FIX2INT(nbuckets_arg),
     interpret_hashindex_flags(opts),
     FD_INT(hashconst),
     slotids_init,baseoids_init,-1,-1);
  if (rv<0)
    return NULL;
  else return fd_open_index(spec,flags,FD_VOID);
}


/* Deprecated primitives */


/* Useful functions */

FD_EXPORT int fd_hashindexp(struct FD_INDEX *ix)
{
  return (ix->index_handler== &hashindex_handler);
}

static fdtype hashindex_stats(struct FD_HASHINDEX *hx)
{
  fdtype result = fd_empty_slotmap();
  int n_filled = 0, maxk = 0, n_singles = 0, n2sum = 0;
  fd_add(result,fd_intern("NBUCKETS"),FD_INT(hx->index_n_buckets));
  fd_add(result,fd_intern("NKEYS"),FD_INT(hx->table_n_keys));
  fd_add(result,fd_intern("NBASEOIDS"),FD_INT(hx->index_n_baseoids));
  fd_add(result,fd_intern("NSLOTIDS"),FD_INT(hx->index_n_slotids));
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

FD_EXPORT ssize_t fd_hashindex_bucket(fdtype ixarg,fdtype key,ssize_t modulate)
{
  struct FD_INDEX *ix=fd_lisp2index(ixarg);
  if (ix==NULL) {
    fd_type_error("hashindex","fd_hashindex_bucket",ixarg);
    return -1;}
  else if (ix->index_handler != &hashindex_handler) {
    fd_type_error("hashindex","fd_hashindex_bucket",ixarg);
    return -1;}
  else return hashindex_bucket(((struct FD_HASHINDEX *)ix),key,modulate);
}

/* The control function */

static fdtype hashindex_ctl(fd_index ix,fdtype op,int n,fdtype *args)
{
  struct FD_HASHINDEX *hx = (struct FD_HASHINDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return fd_err("BadIndexOpCall","hashindex_ctl",
                  hx->indexid,FD_VOID);
  else if (op == fd_cachelevel_op) {
    if (n==0)
      return FD_INT(hx->index_cache_level);
    else {
      fdtype arg = (args)?(args[0]):(FD_VOID);
      if ((FD_FIXNUMP(arg))&&(FD_FIX2INT(arg)>=0)&&
          (FD_FIX2INT(arg)<0x100)) {
        hashindex_setcache(hx,FD_FIX2INT(arg));
        return FD_INT(hx->index_cache_level);}
      else return fd_type_error
             (_("cachelevel"),"hashindex_ctl/cachelevel",arg);}}
  else if (op == fd_bufsize_op) {
    if (n==0)
      return FD_INT(hx->index_stream.buf.raw.buflen);
    else if (FD_FIXNUMP(args[0])) {
      fd_lock_index(hx);
      fd_setbufsize(&(hx->index_stream),FD_FIX2INT(args[0]));
      fd_unlock_index(hx);
      return FD_INT(hx->index_stream.buf.raw.buflen);}
    else return fd_type_error("buffer size","hashindex_ctl/bufsize",args[0]);}
  else if (op == fd_index_hashop) {
    if (n==0)
      return FD_INT(hx->index_n_buckets);
    else {
      fdtype mod_arg = (n>1) ? (args[1]) : (FD_VOID);
      ssize_t bucket = hashindex_bucket(hx,args[0],0);
      if (FD_FIXNUMP(mod_arg))
        return FD_INT((bucket%FD_FIX2INT(mod_arg)));
      else if ((FD_FALSEP(mod_arg))||(FD_VOIDP(mod_arg)))
        return FD_INT(bucket);
      else return FD_INT((bucket%(hx->index_n_buckets)));}}
  else if (op == fd_stats_op)
    return hashindex_stats(hx);
  else if (op == fd_slotids_op) {
    fdtype *elts = u8_alloc_n(hx->index_n_slotids,fdtype);
    fdtype *slotids = hx->index_slotids;
    int i = 0, n = hx->index_n_slotids;
    while (i< n) {elts[i]=slotids[i]; i++;}
    return fd_init_vector(NULL,n,elts);}
  else if (op == fd_baseoids_op) {
    int n_baseoids=hx->index_n_baseoids+hx->index_new_baseoids;
    unsigned int *baseids=hx->index_baseoid_ids;
    fdtype result=fd_make_vector(n_baseoids,NULL);
    int i=0; while (i<n_baseoids) {
      int baseid=baseids[i];
      FD_VECTOR_SET(result,i,fd_make_oid(fd_base_oids[baseid]));
      i++;}
    return result;}
  else if (op == fd_capacity_op)
    return FD_INT(hx->index_n_buckets);
  else if (op == fd_load_op)
    return FD_INT(hx->table_n_keys);
  else return FD_FALSE;
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
  NULL, /* metadata */
  hashindex_create, /* create */ 
  NULL, /* walk */
  NULL, /* recycle */
  hashindex_ctl /* indexctl */
};

/* Module (file) Initialization */

FD_EXPORT void fd_init_hashindex_c()
{
  set_symbol = fd_intern("SET");
  drop_symbol = fd_intern("DROP");

  u8_register_source_file(_FILEINFO);

  fd_register_index_type
    ("hashindex",
     &hashindex_handler,
     open_hashindex,
     match_index_file,
     (void *)(U8_INT2PTR(FD_HASHINDEX_MAGIC_NUMBER)));
  fd_register_index_type
    ("damaged_hashindex",
     &hashindex_handler,
     open_hashindex,
     match_index_file,
     (void *)(U8_INT2PTR(FD_HASHINDEX_TO_RECOVER)));

  fd_register_config("HASHINDEX:SIZE","The default size for hash indexes",
                     fd_sizeconfig_get,fd_sizeconfig_set,
                     &hash_index_default_size);


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
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
