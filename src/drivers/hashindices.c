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

 0     XXXX    4-byte magic number
 4     XXXX    number of buckets
 8     XXXX    flags, including hash function identifier
12     XXXX    hash function constant
16     XXXX    number of keys
20     XXXX    file offset of slotids vector
24     XXXX     (64 bits)
28     XXXX    size of slotids DTYPE representation
32     XXXX    file offset of baseoids vector
36     XXXX     (64 bits)
40     XXXX    size of baseoids DTYPE representation
44     XXXX    file offset of index metadata
48     XXXX     (64 bits)
52     XXXX    size of metadata DTYPE representation

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

#define FD_INLINE_DTYPEIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dtypeio.h"
#include "framerd/stream.h"
#include "framerd/numbers.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/drivers.h"

#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>

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

#ifndef FD_DEBUG_HASHINDICES
#define FD_DEBUG_HASHINDICES 0
#endif

#ifndef FD_DEBUG_DTYPEIO
#define FD_DEBUG_DTYPEIO 0
#endif

#define LOCK_STREAM 1
#define DONT_LOCK_STREAM 0

#include "dbdriver_internals.h"

#if FD_DEBUG_HASHINDICES
#define CHECK_POS(pos,stream)                                      \
  if ((pos)!=(fd_getpos(stream)))                                  \
    u8_log(LOG_CRIT,"FILEPOS error","position mismatch %ld/%ld",   \
           pos,fd_getpos(stream));                                 \
  else {}
#define CHECK_ENDPOS(pos,stream)                                      \
  { ssize_t curpos=fd_getpos(stream), endpos=fd_endpos(stream);       \
    if (((pos)!=(curpos)) && ((pos)!=(endpos)))                       \
      u8_log(LOG_CRIT,"ENDPOS error","position mismatch %ld/%ld/%ld", \
             pos,curpos,endpos);                                      \
    else {}                                                           \
  }
#else
#define CHECK_POS(pos,stream)
#define CHECK_ENDPOS(pos,stream)
#endif

static ssize_t get_maxpos(fd_hash_index p)
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

#ifndef HASHINDEX_PREFETCH_WINDOW
#ifdef FD_MMAP_PREFETCH_WINDOW
#define HASHINDEX_PREFETCH_WINDOW FD_MMAP_PREFETCH_WINDOW
#else
#define HASHINDEX_PREFETCH_WINDOW 0
#endif
#endif

/* Used to generate hash codes */
#define MAGIC_MODULUS 16777213 /* 256000001 */
#define MIDDLIN_MODULUS 573786077 /* 256000001 */
#define MYSTERIOUS_MODULUS 2000239099 /* 256000001 */

#define FD_HASH_INDEX_KEYCOUNT_POS 16
#define FD_HASH_INDEX_SLOTIDS_POS 20
#define FD_HASH_INDEX_BASEOIDS_POS 32
#define FD_HASH_INDEX_METADATA_POS 44

static fdtype read_zvalues(fd_hash_index,int,fd_off_t,size_t);

static struct FD_INDEX_HANDLER hash_index_handler;

static fd_exception CorruptedHashIndex=_("Corrupted hash_index file");
static fd_exception BadHashFn=_("hash_index has unknown hash function");

static fdtype set_symbol, drop_symbol;

/* Utilities for DTYPE I/O */

#define nobytes(in,nbytes) (FD_EXPECT_FALSE(!(fd_needs_bytes(in,nbytes))))
#define havebytes(in,nbytes) (FD_EXPECT_TRUE(fd_needs_bytes(in,nbytes)))

#define output_byte(out,b) \
  if (fd_write_byte(out,b)<0) return -1; else {}
#define output_4bytes(out,w) \
  if (fd_write_4bytes(out,w)<0) return -1; else {}
#define output_bytes(out,bytes,n) \
  if (fd_write_bytes(out,bytes,n)<0) return -1; else {}

/* Getting chunk refs */

static FD_CHUNK_REF index_chunk_ref(struct FD_HASH_INDEX *ix,
                                    unsigned int bucket,
                                    int stream_locked)
{
  return get_chunk_ref(&(ix->index_stream),ix->index_offdata,
                       256,ix->index_offtype,bucket,
                       stream_locked);
}


static int get_chunk_ref_size(fd_hash_index ix)
{
  switch (ix->index_offtype) {
  case FD_B32: case FD_B40: return 8;
  case FD_B64: return 12;}
  return -1;
}

/* Opening database blocks */

FD_FASTOP fd_inbuf open_block
  (struct FD_INBUF *tmpbuf,FD_HASH_INDEX *hx,
   fd_off_t off,fd_size_t size,unsigned char *buf,
   int dolock)
{
  u8_mutex *streamlock=&(hx->index_stream.stream_lock);
  int use_lock=(dolock)&&(hx->index_mmap==NULL);
  if (use_lock) u8_lock_mutex(streamlock); {
    if (read_chunk(&(hx->index_stream),hx->index_mmap,off,size,buf,
                   ((use_lock) ? (streamlock) : (NULL)))) {
      FD_INIT_BYTE_INPUT(tmpbuf,buf,size);
      return tmpbuf;}
    if (use_lock) u8_unlock_mutex(&(hx->index_lock));}
  return NULL;
}

FD_FASTOP fdtype read_dtype_at_pos(fd_stream s,fd_off_t off)
{
  fd_off_t retval=fd_setpos(s,off);
  fd_inbuf ins=fd_readbuf(s);
  if (retval<0) return FD_ERROR_VALUE;
  else return fd_read_dtype(ins);
}

/* Opening a hash index */

static int init_slotids
  (struct FD_HASH_INDEX *hx,int n_slotids,fdtype *slotids_init);
static int init_baseoids
  (struct FD_HASH_INDEX *hx,int n_baseoids,fdtype *baseoids_init);
static int recover_hash_index(struct FD_HASH_INDEX *hx);

static fd_index open_hash_index(u8_string fname,fddb_flags flags)
{
  struct FD_HASH_INDEX *index=u8_alloc(struct FD_HASH_INDEX);
  struct FD_STREAM *stream=&(index->index_stream);
  int read_only=U8_BITP(flags,FDB_READ_ONLY|FDB_INIT_READ_ONLY);
  int consed=U8_BITP(flags,FDB_ISCONSED);
  unsigned int magicno, n_keys;
  fd_off_t slotids_pos, baseoids_pos;
  fd_size_t slotids_size, baseoids_size;
  fd_stream_mode mode=
    ((read_only) ? (FD_STREAM_READ) : (FD_STREAM_MODIFY));
  fd_init_index((fd_index)index,&hash_index_handler,fname,flags);
  if (fd_init_file_stream(stream,fname,mode,fd_driver_bufsize)
      == NULL) {
    u8_free(index);
    fd_seterr3(fd_CantOpenFile,"open_hash_index",u8_strdup(fname));
    return NULL;}
  /* See if it ended up read only */
  if (index->index_stream.stream_flags&FD_STREAM_READ_ONLY) read_only=1;
  stream->stream_flags&=~FD_STREAM_IS_MALLOCD;
  index->index_mmap=NULL;
  magicno=fd_read_4bytes_at(stream,0);
  index->index_n_buckets=fd_read_4bytes_at(stream,4);
  if (magicno==FD_HASH_INDEX_TO_RECOVER) {
    u8_log(LOG_WARN,fd_RecoveryRequired,"Recovering the hash index %s",fname);
    recover_hash_index(index);
    magicno=magicno&(~0x20);}
  index->index_offdata=NULL;
  index->fdb_xformat=fd_read_4bytes_at(stream,8);
  if (read_only)
    U8_SETBITS(index->index_flags,FDB_READ_ONLY|FDB_INIT_READ_ONLY);
  if (((index->fdb_xformat)&(FD_HASH_INDEX_FN_MASK))!=0) {
    u8_free(index);
    fd_seterr3(BadHashFn,"open_hash_index",NULL);
    return NULL;}

  index->index_offtype=(fd_offset_type)
    (((index->fdb_xformat)&(FD_HASH_INDEX_OFFTYPE_MASK))>>4);

  index->index_custom=fd_read_4bytes_at(stream,12);

  /* Currently ignored */
  index->table_n_keys=n_keys=fd_read_4bytes_at(stream,16);

  slotids_pos=fd_read_8bytes_at(stream,20,NULL);
  slotids_size=fd_read_4bytes_at(stream,28);

  baseoids_pos=fd_read_8bytes_at(stream,32,NULL);
  baseoids_size=fd_read_4bytes_at(stream,40);

  /* metadata_pos=*/ fd_read_8bytes_at(stream,44,NULL);
  /* metadata_size=*/ fd_read_4bytes_at(stream,52);

  /* Initialize the slotids field used for storing feature keys */
  if (slotids_size) {
    fdtype slotids_vector=read_dtype_at_pos(stream,slotids_pos);
    if (FD_VOIDP(slotids_vector)) {
      index->index_n_slotids=0; index->index_new_slotids=0;
      index->index_slotids=NULL;
      index->slotid_lookup=NULL;}
    else if (FD_VECTORP(slotids_vector)) {
      init_slotids(index,
                   FD_VECTOR_LENGTH(slotids_vector),
                   FD_VECTOR_DATA(slotids_vector));
      fd_decref(slotids_vector);}
    else {
      fd_seterr("Bad SLOTIDS data","open_hash_index",
                u8_strdup(fname),FD_VOID);
      fd_close_stream(stream,1);
      u8_free(index);
      return NULL;}}
  else {
    index->index_n_slotids=0; index->index_new_slotids=0;
    index->index_slotids=NULL;
    index->slotid_lookup=NULL;}

  /* Initialize the baseoids field used for compressed OID values */
  if (baseoids_size) {
    fdtype baseoids_vector=read_dtype_at_pos(stream,baseoids_pos);
    if (FD_VOIDP(baseoids_vector)) {
      index->index_n_baseoids=0; index->index_new_baseoids=0;
      index->index_baseoid_ids=NULL;
      index->index_ids2baseoids=NULL;}
    else if (FD_VECTORP(baseoids_vector)) {
      init_baseoids(index,
                    FD_VECTOR_LENGTH(baseoids_vector),
                    FD_VECTOR_DATA(baseoids_vector));
      fd_decref(baseoids_vector);}
    else {
      fd_seterr("Bad BASEOIDS data","open_hash_index",
                u8_strdup(fname),FD_VOID);
      fd_close_stream(stream,1);
      u8_free(index);
      return NULL;}}
  else {
    index->index_n_baseoids=0; index->index_new_baseoids=0;
    index->index_baseoid_ids=NULL;
    index->index_ids2baseoids=NULL;}

  fd_init_mutex(&(index->index_lock));

  if (!(consed)) fd_register_index((fd_index)index);

  return (fd_index)index;
}

static int sort_by_slotid(const void *p1,const void *p2)
{
  const fd_slotid_lookup l1=(fd_slotid_lookup)p1, l2=(fd_slotid_lookup)p2;
  if (l1->slotid<l2->slotid) return -1;
  else if (l1->slotid>l2->slotid) return 1;
  else return 0;
}

static int init_slotids(fd_hash_index hx,int n_slotids,fdtype *slotids_init)
{
  struct FD_SLOTID_LOOKUP *lookup; int i=0;
  fdtype *slotids, slotids_choice=FD_EMPTY_CHOICE;
  hx->index_slotids=slotids=u8_alloc_n(n_slotids,fdtype);
  hx->slotid_lookup=lookup=u8_alloc_n(n_slotids,FD_SLOTID_LOOKUP);
  hx->index_n_slotids=n_slotids; hx->index_new_slotids=0;
  if ((hx->fdb_xformat)&(FD_HASH_INDEX_ODDKEYS))
    slotids_choice=FD_VOID;
  while (i<n_slotids) {
    fdtype slotid=slotids_init[i];
    if (FD_VOIDP(slotids_choice)) {}
    else if (FD_ATOMICP(slotid)) {
      FD_ADD_TO_CHOICE(slotids_choice,slotid);}
    else {
      fd_decref(slotids_choice); slotids_choice=FD_VOID;}
    slotids[i]=slotid;
    lookup[i].zindex=i;
    lookup[i].slotid=fd_incref(slotid);
    i++;}
  qsort(lookup,n_slotids,sizeof(FD_SLOTID_LOOKUP),sort_by_slotid);
  if (!(FD_VOIDP(slotids_choice)))
    hx->index_has_slotids=fd_simplify_choice(slotids_choice);
  return 0;
}

static int init_baseoids(fd_hash_index hx,int n_baseoids,fdtype *baseoids_init)
{
  int i=0;
  unsigned int *index_baseoid_ids=u8_alloc_n(n_baseoids,unsigned int);
  short *index_ids2baseoids=u8_alloc_n(1024,short);
  memset(index_baseoid_ids,0,SIZEOF_INT*n_baseoids);
  i=0; while (i<1024) index_ids2baseoids[i++]=-1;
  hx->index_n_baseoids=n_baseoids; hx->index_new_baseoids=0;
  hx->index_baseoid_ids=index_baseoid_ids;
  hx->index_ids2baseoids=index_ids2baseoids;
  i=0; while (i<n_baseoids) {
    fdtype baseoid=baseoids_init[i];
    index_baseoid_ids[i]=FD_OID_BASE_ID(baseoid);
    index_ids2baseoids[FD_OID_BASE_ID(baseoid)]=i;
    i++;}
  return 0;
}

/* Making a hash index */

FD_EXPORT int fd_make_hash_index
  (u8_string fname,int n_buckets_arg,
   unsigned int flags,unsigned int hashconst,
   fdtype slotids_init,fdtype baseoids_init,
   time_t ctime,time_t mtime)
{
  int n_buckets;
  time_t now=time(NULL);
  fd_off_t slotids_pos=0, baseoids_pos=0, metadata_pos=0;
  size_t slotids_size=0, baseoids_size=0, metadata_size=0;
  int offtype=(fd_offset_type)(((flags)&(FD_HASH_INDEX_OFFTYPE_MASK))>>4);
  struct FD_STREAM _stream, *stream=
    fd_init_file_stream(&_stream,fname,FD_STREAM_CREATE,8192);
  struct FD_OUTBUF *outstream=fd_writebuf(stream);
  if (stream==NULL) return -1;
  else if ((stream->stream_flags)&FD_STREAM_READ_ONLY) {
    fd_seterr3(fd_CantWrite,"fd_make_hash_index",u8_strdup(fname));
    fd_close_stream(stream,1);
    return -1;}
  stream->stream_flags&=~FD_STREAM_IS_MALLOCD;
  if (n_buckets_arg<0) n_buckets=-n_buckets_arg;
  else n_buckets=fd_get_hashtable_size(n_buckets_arg);
  fd_setpos(stream,0);
  fd_write_4bytes(outstream,FD_HASH_INDEX_MAGIC_NUMBER);
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

  /* Fill the rest of the space. */
  {
    int i=0, bytes_to_write=256-fd_getpos(stream);
    while (i<bytes_to_write) {
      fd_write_byte(outstream,0); i++;}}

  /* Write the top level bucket table */
  {
    int i=0; while (i<n_buckets) {
      fd_write_4bytes(outstream,0);
      fd_write_4bytes(outstream,0);
      if (offtype==FD_B64)
        fd_write_4bytes(outstream,0);
      else {}
      i++;}}

  /* Write the slotids */
  if (FD_VECTORP(slotids_init)) {
    slotids_pos=fd_getpos(stream);
    fd_write_dtype(outstream,slotids_init);
    slotids_size=fd_getpos(stream)-slotids_pos;}

  /* Write the baseoids */
  if (FD_VECTORP(baseoids_init)) {
    baseoids_pos=fd_getpos(stream);
    fd_write_dtype(outstream,baseoids_init);
    baseoids_size=fd_getpos(stream)-baseoids_pos;}

  if (slotids_pos) {
    fd_setpos(stream,FD_HASH_INDEX_SLOTIDS_POS);
    fd_write_8bytes(outstream,slotids_pos);
    fd_write_4bytes(outstream,slotids_size);}
  if (baseoids_pos) {
    fd_setpos(stream,FD_HASH_INDEX_BASEOIDS_POS);
    fd_write_8bytes(outstream,baseoids_pos);
    fd_write_4bytes(outstream,baseoids_size);}
  if (metadata_pos) {
    fd_setpos(stream,FD_HASH_INDEX_METADATA_POS);
    fd_write_8bytes(outstream,metadata_pos);
    fd_write_4bytes(outstream,metadata_size);}

  fd_flush_stream(stream);
  fd_close_stream(stream,1);
  
  return 0;
}

/* Hash functions */

typedef unsigned long long ull;

FD_FASTOP unsigned int hash_mult(unsigned int x,unsigned int y)
{
  unsigned long long prod=((ull)x)*((ull)y);
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
  unsigned int prod=1, asint=0;
  unsigned char *ptr=start, *limit=ptr+len;
  /* Compute a starting place */
  while (ptr < limit) prod=prod+*ptr++;
  /* Now do a multiplication */
  ptr=start; limit=ptr+((len%4) ? (4*(len/4)) : (len));
  while (ptr < limit) {
    asint=(ptr[0]<<24)|(ptr[1]<<16)|(ptr[2]<<8)|(ptr[3]);
    prod=hash_combine(prod,asint); ptr=ptr+4;}
  switch (len%4) {
  case 0: asint=1; break;
  case 1: asint=ptr[0]; break;
  case 2: asint=ptr[0]|(ptr[1]<<8); break;
  case 3: asint=ptr[0]|(ptr[1]<<8)|(ptr[2]<<16); break;}
  return hash_combine(prod,asint);
}

/* ZKEYs */

FD_FASTOP int get_slotid_index(fd_hash_index hx,fdtype slotid)
{
  const int size=hx->index_n_slotids;
  fd_slotid_lookup bottom=hx->slotid_lookup, middle=bottom+size/2;
  fd_slotid_lookup hard_top=bottom+size, top=hard_top;
  while (top>bottom) {
    if (slotid==middle->slotid) return middle->zindex;
    else if (slotid<middle->slotid) {
      top=middle-1; middle=bottom+(top-bottom)/2;}
    else {
        bottom=middle+1; middle=bottom+(top-bottom)/2;}
    if ((middle) && (middle<hard_top) && (slotid==middle->slotid))
      return middle->zindex;}
  return -1;
}

static int fast_write_dtype(fd_outbuf out,fdtype key)
{
  int v2=((out->buf_flags)&FD_USE_DTYPEV2);
  if (FD_OIDP(key)) {
    FD_OID addr=FD_OID_ADDR(key);
    fd_write_byte(out,dt_oid);
    fd_write_4bytes(out,FD_OID_HI(addr));
    fd_write_4bytes(out,FD_OID_LO(addr));
    return 9;}
  else if (FD_SYMBOLP(key)) {
    int data=FD_GET_IMMEDIATE(key,itype);
    fdtype name=fd_symbol_names[data];
    struct FD_STRING *s=fd_consptr(struct FD_STRING *,name,fd_string_type);
    int len=s->fd_bytelen;
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
    struct FD_STRING *s=fd_consptr(struct FD_STRING *,key,fd_string_type);
    int len=s->fd_bytelen;
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

FD_FASTOP ssize_t write_zkey(fd_hash_index hx,fd_outbuf out,fdtype key)
{
  int slotid_index=-1; size_t retval=-1;
  int natsort=out->buf_flags&FD_NATSORT_VALUES;
  out->buf_flags|=FD_NATSORT_VALUES;
  if (FD_PAIRP(key)) {
    fdtype car=FD_CAR(key);
    if ((FD_OIDP(car)) || (FD_SYMBOLP(car))) {
      slotid_index=get_slotid_index(hx,car);
      if (slotid_index<0)
        retval=fd_write_byte(out,0)+fd_write_dtype(out,key);
      else retval=fd_write_zint(out,slotid_index+1)+
             fast_write_dtype(out,FD_CDR(key));}
    else retval=fd_write_byte(out,0)+fd_write_dtype(out,key);}
  else retval=fd_write_byte(out,0)+fd_write_dtype(out,key);
  if (!(natsort)) out->buf_flags&=~FD_NATSORT_VALUES;
  return retval;
}

static fdtype fast_read_dtype(fd_inbuf in)
{
  if (nobytes(in,1)) return fd_return_errcode(FD_EOD);
  else {
    int code=*(in->bufpoint);
    switch (code) {
    case dt_oid:
      if (nobytes(in,9)) return fd_return_errcode(FD_EOD);
      else {
        FD_OID addr; in->bufpoint++;
#if FD_STRUCT_OIDS
        memset(&addr,0,sizeof(addr));
#else
        addr=0;
#endif
        FD_SET_OID_HI(addr,fd_read_4bytes(in));
        FD_SET_OID_LO(addr,fd_read_4bytes(in));
        return fd_make_oid(addr);}
    case dt_string:
      if (nobytes(in,5)) return fd_return_errcode(FD_EOD);
      else {
        int len=fd_get_4bytes(in->bufpoint+1); in->bufpoint=in->bufpoint+5;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          fdtype result=fd_make_string(NULL,len,in->bufpoint);
          in->bufpoint=in->bufpoint+len;
          return result;}}
    case dt_tiny_string:
      if (nobytes(in,2)) return fd_return_errcode(FD_EOD);
      else {
        int len=fd_get_byte(in->bufpoint+1); in->bufpoint=in->bufpoint+2;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          fdtype result=fd_make_string(NULL,len,in->bufpoint);
          in->bufpoint=in->bufpoint+len;
          return result;}}
    case dt_symbol:
      if (nobytes(in,5)) return fd_return_errcode(FD_EOD);
      else {
        int len=fd_get_4bytes(in->bufpoint+1); in->bufpoint=in->bufpoint+5;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          unsigned char _buf[256], *buf=NULL; fdtype symbol;
          if (len<256) buf=_buf; else buf=u8_malloc(len+1);
          memcpy(buf,in->bufpoint,len); buf[len]='\0'; in->bufpoint=in->bufpoint+len;
          symbol=fd_make_symbol(buf,len);
          if (buf!=_buf) u8_free(buf);
          return symbol;}}
    case dt_tiny_symbol:
      if (nobytes(in,2)) return fd_return_errcode(FD_EOD);
      else {
        int len=fd_get_byte(in->bufpoint+1); in->bufpoint=in->bufpoint+2;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          unsigned char _buf[256];
          memcpy(_buf,in->bufpoint,len); _buf[len]='\0'; in->bufpoint=in->bufpoint+len;
          return fd_make_symbol(_buf,len);}}
    default:
      return fd_read_dtype(in);
#if 0
      {
        fdtype result=fd_read_dtype(in);
        if (FD_CHECK_PTR(result)) return result;
        else {
          u8_log(LOG_WARN,"Bad Pointer","from fd_read_dtype");
          return FD_VOID;}}
#endif
    }}
}

FD_FASTOP fdtype read_zkey(fd_hash_index hx,fd_inbuf in)
{
  int code=fd_read_zint(in);
  if (code==0) return fast_read_dtype(in);
  else if ((code-1)<hx->index_n_slotids) {
    fdtype cdr=fast_read_dtype(in);
    if (FD_ABORTP(cdr)) return cdr;
    else return fd_conspair(hx->index_slotids[code-1],cdr);}
  else return fd_err(CorruptedHashIndex,"read_zkey",NULL,FD_VOID);
}

FD_EXPORT int fd_hash_index_bucket(struct FD_HASH_INDEX *hx,fdtype key,int modulate)
{
  struct FD_OUTBUF out; unsigned char buf[1024];
  unsigned int hashval; int dtype_len;
  FD_INIT_FIXED_BYTE_OUTBUF(&out,buf,1024);
  if ((hx->fdb_xformat)&(FD_HASH_INDEX_DTYPEV2))
    out.buf_flags=out.buf_flags|FD_USE_DTYPEV2;
  dtype_len=write_zkey(hx,&out,key);
  hashval=hash_bytes(out.bufbase,dtype_len);
  fd_close_outbuf(&out);
  if (modulate) return hashval%(hx->index_n_buckets);
  else return hashval;
}

/* ZVALUEs */

FD_FASTOP fdtype write_zvalue(fd_hash_index hx,fd_outbuf out,fdtype value)
{
  if ((FD_OIDP(value))&&(hx->index_ids2baseoids)) {
    int base=FD_OID_BASE_ID(value);
    short baseoid_index=hx->index_ids2baseoids[base];
    if (baseoid_index<0) {
      int bytes_written; fd_write_byte(out,0);
      bytes_written=fd_write_dtype(out,value);
      if (bytes_written<0) return FD_ERROR_VALUE;
      else return bytes_written+1;}
    else {
      int offset=FD_OID_BASE_OFFSET(value), bytes_written;
      bytes_written=fd_write_zint(out,baseoid_index+1);
      if (bytes_written<0) return FD_ERROR_VALUE;
      bytes_written=bytes_written+fd_write_zint(out,offset);
      return bytes_written;}}
  else {
    int bytes_written; fd_write_byte(out,0);
    bytes_written=fd_write_dtype(out,value);
    return bytes_written+1;}
}

FD_FASTOP fdtype read_zvalue(fd_hash_index hx,fd_inbuf in)
{
  int prefix=fd_read_zint(in);
  if (prefix==0) return fd_read_dtype(in);
  else {
    unsigned int base=hx->index_baseoid_ids[prefix-1];
    unsigned int offset=fd_read_zint(in);
    return FD_CONSTRUCT_OID(base,offset);}
}

/* Fetching */

static fdtype hash_index_fetch(fd_index ix,fdtype key)
{
  struct FD_HASH_INDEX *hx=(fd_hash_index)ix;
  struct FD_STREAM *stream=&(hx->index_stream);
  struct FD_OUTBUF out; unsigned char buf[64];
  struct FD_INBUF keystream;
  unsigned char _inbuf[512], *inbuf=NULL;
  unsigned int hashval, bucket, n_keys, i, dtype_len, n_values;
  fd_off_t vblock_off; size_t vblock_size;
  FD_CHUNK_REF keyblock;
  FD_INIT_FIXED_BYTE_OUTBUF(&out,buf,64);
#if FD_DEBUG_HASHINDICES
  /* u8_message("Fetching the key %q from %s",key,hx->index_cid); */
#endif
  /* If the index doesn't have oddkeys and you're looking up some feature (pair)
     whose slotid isn't in the slotids, the key isn't in the table. */
  if ((!((hx->fdb_xformat)&(FD_HASH_INDEX_ODDKEYS))) && (FD_PAIRP(key))) {
    fdtype slotid=FD_CAR(key);
    if (((FD_SYMBOLP(slotid)) || (FD_OIDP(slotid))) &&
        (get_slotid_index(hx,slotid)<0)) {
#if FD_DEBUG_HASHINDICES
      u8_message("The slotid %q isn't indexed in %s, returning {}",
                 slotid,hx->index_cid);
#endif
      return FD_EMPTY_CHOICE;}}
  if ((hx->fdb_xformat)&(FD_HASH_INDEX_DTYPEV2))
    out.buf_flags|=FD_USE_DTYPEV2;
  dtype_len=write_zkey(hx,&out,key);
  hashval=hash_bytes(out.bufbase,dtype_len);
  bucket=hashval%(hx->index_n_buckets);
  fd_lock_stream(stream);
  keyblock=index_chunk_ref(hx,bucket,1);
  if (keyblock.size==0) {
    fd_unlock_stream(stream);
    fd_close_outbuf(&out);
    return FD_EMPTY_CHOICE;}
  if (keyblock.size<512)
    open_block(&keystream,hx,keyblock.off,keyblock.size,_inbuf,LOCK_STREAM);
  else {
    inbuf=u8_malloc(keyblock.size);
    open_block(&keystream,hx,keyblock.off,keyblock.size,inbuf,LOCK_STREAM);}
  fd_unlock_stream(stream);
  n_keys=fd_read_zint(&keystream);
  i=0; while (i<n_keys) {
    int key_len=fd_read_zint(&keystream);
    if ((key_len==dtype_len) &&
        (memcmp(keystream.bufpoint,out.bufbase,dtype_len)==0)) {
      keystream.bufpoint=keystream.bufpoint+key_len;
      n_values=fd_read_zint(&keystream);
      if (n_values==0) {
        if (inbuf) u8_free(inbuf);
        fd_close_outbuf(&out);
        return FD_EMPTY_CHOICE;}
      else if (n_values==1) {
        fdtype value=read_zvalue(hx,&keystream);
        if (inbuf) u8_free(inbuf);
        fd_close_outbuf(&out);
        return value;}
      else {
        vblock_off=(fd_off_t)fd_read_zint(&keystream);
        vblock_size=(size_t)fd_read_zint(&keystream);
        if (inbuf) u8_free(inbuf);
        fd_close_outbuf(&out);
        return read_zvalues(hx,n_values,vblock_off,vblock_size);}}
    else {
      keystream.bufpoint=keystream.bufpoint+key_len;
      n_values=fd_read_zint(&keystream);
      if (n_values==0) {}
      else if (n_values==1) {
        int code=fd_read_zint(&keystream);
        if (code==0) {
          fdtype val=fd_read_dtype(&keystream);
          fd_decref(val);}
        else fd_read_zint(&keystream);}
      else {
        fd_read_zint(&keystream);
        fd_read_zint(&keystream);}}
    i++;}
  if (inbuf) u8_free(inbuf);
  fd_close_outbuf(&out);
  return FD_EMPTY_CHOICE;
}

static fdtype read_zvalues
  (fd_hash_index hx,int n_values,fd_off_t vblock_off,size_t vblock_size)
{
  struct FD_CHOICE *result=fd_alloc_choice(n_values);
  fdtype *values=(fdtype *)FD_XCHOICE_DATA(result), *scan=values;
  unsigned char _vbuf[1024], *vbuf=NULL, vbuf_size=-1;
  struct FD_INBUF instream;
  int atomicp=1;
  while (vblock_off != 0) {
    int n_elts, i;
    if (vblock_size<1024)
      open_block(&instream,hx,vblock_off,vblock_size,_vbuf,LOCK_STREAM);
    else if (vbuf_size>vblock_size)
      open_block(&instream,hx,vblock_off,vblock_size,vbuf,LOCK_STREAM);
    else {
      if (vbuf) vbuf=u8_realloc(vbuf,vblock_size);
      else vbuf=u8_malloc(vblock_size);
      open_block(&instream,hx,vblock_off,vblock_size,vbuf,LOCK_STREAM);}
    n_elts=fd_read_zint(&instream);
    i=0; while (i<n_elts) {
      fdtype val=read_zvalue(hx,&instream);
      if (FD_CONSP(val)) atomicp=0;
      *scan++=val; i++;}
    /* For vblock continuation pointers, we make the size be first,
       so that we don't need to store an offset if it's zero. */
    vblock_size=fd_read_zint(&instream);
    if (vblock_size) vblock_off=fd_read_zint(&instream);
    else vblock_off=0;}
  if (vbuf) u8_free(vbuf);
  return fd_init_choice(result,n_values,NULL,
                        FD_CHOICE_DOSORT|
                        ((atomicp)?(FD_CHOICE_ISATOMIC):
                         (FD_CHOICE_ISCONSES))|
                        FD_CHOICE_REALLOC);
}

static int hash_index_fetchsize(fd_index ix,fdtype key)
{
  struct FD_HASH_INDEX *hx=(fd_hash_index)ix;
  struct FD_OUTBUF out; unsigned char buf[64];
  struct FD_INBUF keystream; unsigned char _inbuf[256], *inbuf;
  unsigned int hashval, bucket, n_keys, i, dtype_len, n_values;
  FD_CHUNK_REF keyblock;
  FD_INIT_FIXED_BYTE_OUTBUF(&out,buf,64);
  if ((hx->fdb_xformat)&(FD_HASH_INDEX_DTYPEV2))
    out.buf_flags=out.buf_flags|FD_USE_DTYPEV2;
  dtype_len=write_zkey(hx,&out,key);
  hashval=hash_bytes(out.bufbase,dtype_len);
  bucket=hashval%(hx->index_n_buckets);
  keyblock=index_chunk_ref(hx,bucket,LOCK_STREAM);
  if (keyblock.size==0) return 0;
  if (keyblock.size==0) return FD_EMPTY_CHOICE;
  if (keyblock.size<512)
    open_block(&keystream,hx,keyblock.off,keyblock.size,_inbuf,LOCK_STREAM);
  else {
    inbuf=u8_malloc(keyblock.size);
    open_block(&keystream,hx,keyblock.off,keyblock.size,inbuf,LOCK_STREAM);}
  n_keys=fd_read_zint(&keystream);
  i=0; while (i<n_keys) {
    int key_len=fd_read_zint(&keystream);
    n_values=fd_read_zint(&keystream);
    /* vblock_off= */ (void)(fd_off_t)fd_read_zint(&keystream);
    /* vblock_size=*/ (void)(size_t)fd_read_zint(&keystream);
    if (key_len!=dtype_len)
      keystream.bufpoint=keystream.bufpoint+key_len;
    else if (memcmp(keystream.bufpoint,out.bufbase,dtype_len)==0) {
      fd_close_outbuf(&out);
      return n_values;}
    else keystream.bufpoint=keystream.bufpoint+key_len;
    i++;}
  fd_close_outbuf(&out);
  return 0;
}


/* Fetching multiple keys */

struct KEY_SCHEDULE {
  int index; fdtype key; unsigned int key_start, size;
  int fd_bucketno; FD_CHUNK_REF ref;};
struct VALUE_SCHEDULE {
  int index; fdtype *write; int atomicp; FD_CHUNK_REF ref;};

static int sort_ks_by_bucket(const void *k1,const void *k2)
{
  struct KEY_SCHEDULE *ks1=(struct KEY_SCHEDULE *)k1;
  struct KEY_SCHEDULE *ks2=(struct KEY_SCHEDULE *)k2;
  if (ks1->fd_bucketno<ks2->fd_bucketno) return -1;
  else if (ks1->fd_bucketno>ks2->fd_bucketno) return 1;
  else return 0;
}

static int sort_ks_by_refoff(const void *k1,const void *k2)
{
  struct KEY_SCHEDULE *ks1=(struct KEY_SCHEDULE *)k1;
  struct KEY_SCHEDULE *ks2=(struct KEY_SCHEDULE *)k2;
  if (ks1->ref.off<ks2->ref.off) return -1;
  else if (ks1->ref.off>ks2->ref.off) return 1;
  else return 0;
}

static int sort_vs_by_refoff(const void *v1,const void *v2)
{
  struct VALUE_SCHEDULE *vs1=(struct VALUE_SCHEDULE *)v1;
  struct VALUE_SCHEDULE *vs2=(struct VALUE_SCHEDULE *)v2;
  if (vs1->ref.off<vs2->ref.off) return -1;
  else if (vs1->ref.off>vs2->ref.off) return 1;
  else return 0;
}

static fdtype *fetchn(struct FD_HASH_INDEX *hx,int n,fdtype *keys,int stream_locked)
{
  fdtype *values=u8_alloc_n(n,fdtype);
  int dolock=((stream_locked) ? (DONT_LOCK_STREAM) : (LOCK_STREAM));
  struct FD_OUTBUF out;
  struct KEY_SCHEDULE *schedule=u8_alloc_n(n,struct KEY_SCHEDULE);
  struct VALUE_SCHEDULE *vsched=u8_alloc_n(n,struct VALUE_SCHEDULE);
  int i=0, n_entries=0, vsched_size=0;
  int oddkeys=((hx->fdb_xformat)&(FD_HASH_INDEX_ODDKEYS));
#if FD_DEBUG_HASHINDICES
  u8_message("Reading %d keys from %s",n,hx->index_cid);
#endif
  FD_INIT_BYTE_OUTBUF(&out,n*16);
  if ((hx->fdb_xformat)&(FD_HASH_INDEX_DTYPEV2))
    out.buf_flags=out.buf_flags|FD_USE_DTYPEV2;
  /* Fill out a fetch schedule, computing hashes and buckets for each key.
     If we have an offsets table, we compute the offsets during this phase,
     otherwise we defer to an additional loop. */
  while (i<n) {
    fdtype key=keys[i];
    int dt_start=out.bufpoint-out.bufbase, dt_size, bucket;
   /* If the index doesn't have oddkeys and you're looking up some feature (pair)
     whose slotid isn't in the slotids, the key isn't in the table. */
    if ((!oddkeys) && (FD_PAIRP(key))) {
      fdtype slotid=FD_CAR(key);
      if (((FD_SYMBOLP(slotid)) || (FD_OIDP(slotid))) &&
          (get_slotid_index(hx,slotid)<0)) {
        values[i++]=FD_EMPTY_CHOICE;
        continue;}}
    schedule[n_entries].index=i; schedule[n_entries].key=key;
    schedule[n_entries].key_start=dt_start;
    write_zkey(hx,&out,key);
    dt_size=(out.bufpoint-out.bufbase)-dt_start;
    schedule[n_entries].size=dt_size;
    schedule[n_entries].fd_bucketno=bucket=
      hash_bytes(out.bufbase+dt_start,dt_size)%(hx->index_n_buckets);
    if (hx->index_offdata) {
      /* Because we have an offsets table, get_chunk_ref won't ever
         actually need to lock the stream. */
      schedule[n_entries].ref=index_chunk_ref(hx,bucket,dolock);
      if (schedule[n_entries].ref.size==0) {
        /* It is empty, so we don't even need to handle this entry. */
        values[i]=FD_EMPTY_CHOICE;
        /* We don't need to keep its dtype representation around either,
           so we reset the key stream. */
        out.bufpoint=out.bufbase+dt_start;}
      else n_entries++;}
    else n_entries++;
    i++;}
  if (hx->index_offdata==NULL) {
    int write_at=0;
    /* When fetching bucket references, we sort the schedule first, so that
       we're accessing them in order in the file. */
    qsort(schedule,n_entries,sizeof(struct KEY_SCHEDULE),
          sort_ks_by_bucket);
    i=0; while (i<n_entries) {
      schedule[i].ref=(index_chunk_ref(hx,schedule[i].fd_bucketno,dolock));
      if (schedule[i].ref.size==0) {
        values[schedule[i].index]=FD_EMPTY_CHOICE; i++;}
      else if (write_at==i) {write_at++; i++;}
      else {schedule[write_at++]=schedule[i++];}}
    n_entries=write_at;}
  qsort(schedule,n_entries,sizeof(struct KEY_SCHEDULE),
        sort_ks_by_refoff);
  {
    struct FD_INBUF keyblock;
    unsigned char _buf[1024], *buf=NULL;
    int bucket=-1, j=0, bufsiz=0;
    while (j<n_entries) {
      int k=0, n_keys, found=0;
      fd_off_t blockpos=schedule[j].ref.off;
      fd_size_t blocksize=schedule[j].ref.size;
      if (schedule[j].fd_bucketno!=bucket) {
#if HASHINDEX_PREFETCH_WINDOW
        if (hx->index_mmap) {
          unsigned char *fd_vecelts=hx->index_mmap;
          int k=j+1, newbuck=schedule[j].fd_bucketno, n_prefetched=0;
          while ((k<n_entries) && (n_prefetched<4))
            if (schedule[k].fd_bucketno!=newbuck) {
              newbuck=schedule[k].fd_bucketno;
              FD_PREFETCH(&fd_vecelts[schedule[k].ref.off]);
              n_prefetched++;}
            else k++;}
#endif
        if (blocksize<1024)
          open_block(&keyblock,hx,blockpos,blocksize,_buf,dolock);
        else if (buf)
          if (blocksize<bufsiz)
            open_block(&keyblock,hx,blockpos,blocksize,buf,dolock);
          else {
            buf=u8_realloc(buf,blocksize); bufsiz=blocksize;
            open_block(&keyblock,hx,blockpos,blocksize,buf,dolock);}
        else {
          buf=u8_malloc(blocksize); bufsiz=blocksize;
          open_block(&keyblock,hx,blockpos,blocksize,buf,dolock);}}
      else keyblock.bufpoint=keyblock.bufbase;
      n_keys=fd_read_zint(&keyblock);
      while (k<n_keys) {
        int n_vals;
        fd_size_t dtsize=fd_read_zint(&keyblock);
        if ((dtsize==schedule[j].size) &&
            (memcmp(out.bufbase+schedule[j].key_start,
                    keyblock.bufpoint,dtsize)==0)) {
          keyblock.bufpoint=keyblock.bufpoint+dtsize; found=1;
          n_vals=fd_read_zint(&keyblock);
          if (n_vals==0)
            values[schedule[j].index]=FD_EMPTY_CHOICE;
          else if (n_vals==1)
            values[schedule[j].index]=read_zvalue(hx,&keyblock);
          else {
            fd_off_t block_off=fd_read_zint(&keyblock);
            fd_size_t block_size=fd_read_zint(&keyblock);
            struct FD_CHOICE *result=fd_alloc_choice(n_vals);
            FD_SET_CONS_TYPE(result,fd_choice_type);
            result->choice_size=n_vals;
            values[schedule[j].index]=(fdtype)result;
            vsched[vsched_size].index=schedule[j].index;
            vsched[vsched_size].ref.off=block_off;
            vsched[vsched_size].ref.size=block_size;
            vsched[vsched_size].write=(fdtype *)FD_XCHOICE_DATA(result);
            vsched[vsched_size].atomicp=1;
            vsched_size++;}
          /* This breaks out the loop iterating over the keys in this bucket. */
          break;}
        else {
          keyblock.bufpoint=keyblock.bufpoint+dtsize;
          n_vals=fd_read_zint(&keyblock);
          if (n_vals==0) {}
          else if (n_vals==1) {
            fdtype v=read_zvalue(hx,&keyblock);
            fd_decref(v);}
          else {
            /* Skip offset information */
            fd_read_zint(&keyblock);
            fd_read_zint(&keyblock);}}
        k++;}
      if (!(found))
        values[schedule[j].index]=FD_EMPTY_CHOICE;
      j++;}
    if (buf) u8_free(buf);}
  u8_free(schedule);
  {
    struct FD_INBUF vblock;
    unsigned char _vbuf[8192], *vbuf=NULL;
    unsigned int vbuf_size=0;
    while (vsched_size) {
      qsort(vsched,vsched_size,sizeof(struct VALUE_SCHEDULE),
            sort_vs_by_refoff);
      i=0; while (i<vsched_size) {
        int j=0, n_vals;
        fd_size_t next_size;
        if (vsched[i].ref.size>8192) {
          if (vbuf==NULL) {
            vbuf=u8_realloc(vbuf,vsched[i].ref.size);
            vbuf_size=vsched[i].ref.size;}
          else if (vsched[i].ref.size>vbuf_size) {
            vbuf=u8_realloc(vbuf,vsched[i].ref.size);
            vbuf_size=vsched[i].ref.size;}
          open_block(&vblock,hx,vsched[i].ref.off,vsched[i].ref.size,
                     vbuf,dolock);}
        else open_block(&vblock,hx,vsched[i].ref.off,vsched[i].ref.size,
                        _vbuf,dolock);
        n_vals=fd_read_zint(&vblock);
        while (j<n_vals) {
          fdtype v=read_zvalue(hx,&vblock);
          if (FD_CONSP(v)) vsched[i].atomicp=0;
          *(vsched[i].write++)=v; j++;}
        next_size=fd_read_zint(&vblock);
        if (next_size) {
          vsched[i].ref.size=next_size;
          vsched[i].ref.off=fd_read_zint(&vblock);}
        else {
          vsched[i].ref.size=0;
          vsched[i].ref.off=0;}
        i++;}
      {
        struct VALUE_SCHEDULE *read=&(vsched[0]), *write=&(vsched[0]);
        struct VALUE_SCHEDULE *limit=read+vsched_size;
        vsched_size=0; while (read<limit) {
          if (read->ref.size) {
            *write++=*read++; vsched_size++;}
          else {
            /* We're now done with this value, so we finalize it,
               using fd_init_choice to sort it.  Note that (rarely),
               fd_init_choice will free what was passed to it (if it
               had zero or one element), so the real result is what is
               returned and not what was passed in. */
            int index=read->index, atomicp=read->atomicp;
            struct FD_CHOICE *result=(struct FD_CHOICE *)values[index];
            int n_values=result->choice_size;
            fdtype realv=fd_init_choice(result,n_values,NULL,
                                        FD_CHOICE_DOSORT|
                                        ((atomicp)?(FD_CHOICE_ISATOMIC):
                                         (FD_CHOICE_ISCONSES))|
                                        FD_CHOICE_REALLOC);
            values[index]=realv;
            read++;}}}}
    if (vbuf) u8_free(vbuf);}
  u8_free(vsched);
#if FD_DEBUG_HASHINDICES
  u8_message("Finished reading %d keys from %s",n,hx->index_cid);
#endif
  fd_close_outbuf(&out);
  return values;
}

static fdtype *hash_index_fetchn_inner(fd_index ix,
                                       int n,fdtype *keys,
                                       int stream_locked,int adds_locked)
{
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  fdtype *results;
  results=fetchn(hx,n,keys,stream_locked);
  if (results) {
    int i=0;
    if (adds_locked==0) fd_read_lock_table(&(hx->index_adds));
    while (i<n) {
      fdtype v=fd_hashtable_get_nolock(&(hx->index_adds),keys[i],
                                       FD_EMPTY_CHOICE);
      if (FD_ABORTP(v)) {
        int j=0; while (j<n) { fd_decref(results[j]); j++;}
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

static fdtype *hash_index_fetchn(fd_index ix,int n,fdtype *keys)
{
  return hash_index_fetchn_inner(ix,n,keys,0,0);
}


/* Getting all keys */

static int sort_blockrefs_by_off(const void *v1,const void *v2)
{
  struct FD_CHUNK_REF *br1=(struct FD_CHUNK_REF *)v1;
  struct FD_CHUNK_REF *br2=(struct FD_CHUNK_REF *)v2;
  if (br1->off<br2->off) return -1;
  else if (br1->off>br2->off) return 1;
  else return 0;
}

static fdtype *hash_index_fetchkeys(fd_index ix,int *n)
{
  fdtype *results=NULL;
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  fd_stream s=&(hx->index_stream);
  int i=0, n_buckets=(hx->index_n_buckets), n_to_fetch=0;
  int total_keys=0, key_count=0;
  unsigned char _keybuf[512], *keybuf=NULL; int keybuf_size=-1;
  FD_CHUNK_REF *buckets;
  fd_lock_stream(s);
  total_keys=fd_read_4bytes(fd_start_read(s,16));
  if (total_keys==0) {
    fd_unlock_stream(s);
    *n=0;
    return NULL;}
  buckets=u8_alloc_n(total_keys,FD_CHUNK_REF);
  results=u8_alloc_n(total_keys,fdtype);
  /* If we have chunk offsets in memory, we don't need to keep the
     stream locked while we get them. */
  if (hx->index_offdata) {
    fd_unlock_stream(s);
    while (i<n_buckets) {
      FD_CHUNK_REF ref=index_chunk_ref(hx,i,0);
      if (ref.size) buckets[n_to_fetch++]=ref;
      i++;}}
  else {
    while (i<n_buckets) {
      FD_CHUNK_REF ref=index_chunk_ref(hx,i,STREAM_LOCKED);
      if (ref.size) buckets[n_to_fetch++]=ref;
      i++;}
    fd_unlock_stream(s);}
  qsort(buckets,n_to_fetch,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  i=0; while (i<n_to_fetch) {
    struct FD_INBUF keyblock; int j=0, n_keys;
    if (buckets[i].size<512)
      open_block(&keyblock,hx,buckets[i].off,buckets[i].size,
                 _keybuf,LOCK_STREAM);
    else {
      if (keybuf==NULL) {
        keybuf_size=buckets[i].size;
        keybuf=u8_malloc(keybuf_size);}
      else if (buckets[i].size<keybuf_size) {}
      else {
        keybuf_size=buckets[i].size;
        keybuf=u8_realloc(keybuf,keybuf_size);}
      open_block(&keyblock,hx,buckets[i].off,buckets[i].size,
                 keybuf,LOCK_STREAM);}
    n_keys=fd_read_zint(&keyblock);
    while (j<n_keys) {
      fdtype key; int n_vals;
      /* size=*/ fd_read_zint(&keyblock);
      key=read_zkey(hx,&keyblock);
      n_vals=fd_read_zint(&keyblock);
      results[key_count++]=key;
      if (n_vals==0) {}
      else if (n_vals==1) {
        int code=fd_read_zint(&keyblock);
        if (code==0) {
          fdtype val=fd_read_dtype(&keyblock);
          fd_decref(val);}
        else fd_read_zint(&keyblock);}
      else {
        fd_read_zint(&keyblock);
        fd_read_zint(&keyblock);}
      j++;}
    i++;}
  if (keybuf) u8_free(keybuf);
  if (buckets) u8_free(buckets);
  *n=total_keys;
  return results;
}

static struct FD_KEY_SIZE *hash_index_fetchsizes(fd_index ix,int *n)
{
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  fd_stream s=&(hx->index_stream);
  fd_inbuf ins=fd_readbuf(s);
  int i=0, n_buckets=(hx->index_n_buckets), total_keys;
  int n_to_fetch=0, key_count=0, keybuf_size=-1;
  struct FD_KEY_SIZE *sizes; FD_CHUNK_REF *buckets;
  unsigned char _keybuf[512], *keybuf=NULL;
  u8_lock_mutex(&(s->stream_lock));
  fd_setpos(s,16); total_keys=fd_read_4bytes(ins);
  if (total_keys==0) {
    u8_unlock_mutex(&(s->stream_lock));
    *n=0;
    return NULL;}
  buckets=u8_alloc_n(total_keys,FD_CHUNK_REF);
  sizes=u8_alloc_n(total_keys,FD_KEY_SIZE);
  /* If we don't have chunk offsets in memory, we keep the stream
     locked while we get them. */
  if (hx->index_offdata) {
    fd_unlock_stream(s);
    while (i<n_buckets) {
      FD_CHUNK_REF ref=index_chunk_ref(hx,i,0);
      if (ref.size) buckets[n_to_fetch++]=ref;
      i++;}}
  else {
    while (i<n_buckets) {
      FD_CHUNK_REF ref=index_chunk_ref(hx,i,1);
      if (ref.size) buckets[n_to_fetch++]=ref;
      i++;}
    fd_unlock_stream(s);}
  qsort(buckets,n_to_fetch,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  i=0; while (i<n_to_fetch) {
    struct FD_INBUF keyblock; int j=0, n_keys;
    if (buckets[i].size<512)
      open_block(&keyblock,hx,buckets[i].off,buckets[i].size,
                 _keybuf,LOCK_STREAM);
    else {
      if (keybuf==NULL) {
        keybuf_size=buckets[i].size;
        keybuf=u8_malloc(keybuf_size);}
      else if (buckets[i].size<keybuf_size) {}
      else {
        keybuf_size=buckets[i].size;
        keybuf=u8_realloc(keybuf,keybuf_size);}
      open_block(&keyblock,hx,buckets[i].off,buckets[i].size,
                 keybuf,LOCK_STREAM);}
    n_keys=fd_read_zint(&keyblock);
    while (j<n_keys) {
      fdtype key; int n_vals;
      /* size=*/ fd_read_zint(&keyblock);
      key=read_zkey(hx,&keyblock);
      n_vals=fd_read_zint(&keyblock);
      assert(key!=0);
      sizes[key_count].keysizekey=key; sizes[key_count].keysizenvals=n_vals;
      key_count++;
      if (n_vals==0) {}
      else if (n_vals==1) {
        int code=fd_read_zint(&keyblock);
        if (code==0) {
          fdtype val=fd_read_dtype(&keyblock);
          fd_decref(val);}
        else fd_read_zint(&keyblock);}
      else {
        fd_read_zint(&keyblock);
        fd_read_zint(&keyblock);}
      j++;}
    i++;}
  assert(key_count==hx->table_n_keys);
  *n=hx->table_n_keys;
  if (keybuf) u8_free(keybuf);
  if (buckets) u8_free(buckets);
  return sizes;
}

static void hash_index_getstats(struct FD_HASH_INDEX *hx,
                                int *nf,int *max,int *singles,int *n2sum)
{
  fd_stream s=&(hx->index_stream);
  fd_inbuf ins=fd_readbuf(s);
  int i=0, n_buckets=(hx->index_n_buckets), n_to_fetch=0, total_keys=0;
  unsigned char _keybuf[512], *keybuf=NULL; int keybuf_size=-1;
  FD_CHUNK_REF *buckets;
  u8_lock_mutex(&(s->stream_lock));
  fd_setpos(s,16); total_keys=fd_read_4bytes(ins);
  if (total_keys==0) {
    u8_unlock_mutex(&(s->stream_lock));
    *nf=0; *max=0; *singles=0; *n2sum=0;
    return;}
  buckets=u8_alloc_n(total_keys,FD_CHUNK_REF);
  /* If we don't have chunk offsets in memory, we keep the stream locked while we get them. */
  if (hx->index_offdata) {
    u8_unlock_mutex(&(s->stream_lock));
    while (i<n_buckets) {
      FD_CHUNK_REF ref=index_chunk_ref(hx,i,0);
      if (ref.size) buckets[n_to_fetch++]=ref;
      i++;}}
  else {
    while (i<n_buckets) {
      FD_CHUNK_REF ref=index_chunk_ref(hx,i,1);
      if (ref.size) buckets[n_to_fetch++]=ref;
      i++;}
    u8_unlock_mutex(&(s->stream_lock));}
  *nf=n_to_fetch;
  /* Now we actually unlock it if we kept it locked. */
  if (hx->index_offdata==NULL) fd_unlock_index(hx);
  qsort(buckets,n_to_fetch,sizeof(FD_CHUNK_REF),sort_blockrefs_by_off);
  i=0; while (i<n_to_fetch) {
    struct FD_INBUF keyblock; int n_keys;
    if (buckets[i].size<512)
      open_block(&keyblock,hx,buckets[i].off,buckets[i].size,
                 _keybuf,LOCK_STREAM);
    else {
      if (keybuf==NULL) {
        keybuf_size=buckets[i].size;
        keybuf=u8_malloc(keybuf_size);}
      else if (buckets[i].size<keybuf_size) {}
      else {
        keybuf_size=buckets[i].size;
        keybuf=u8_realloc(keybuf,keybuf_size);}
      open_block(&keyblock,hx,buckets[i].off,buckets[i].size,
                 keybuf,LOCK_STREAM);}
    n_keys=fd_read_zint(&keyblock);
    if (n_keys==1) (*singles)++;
    if (n_keys>(*max)) *max=n_keys;
    *n2sum=*n2sum+(n_keys*n_keys);
    i++;}
  if (keybuf) u8_free(keybuf);
  if (buckets) u8_free(buckets);
}


/* Cache setting */

static void hash_index_setcache(fd_index ix,int level)
{
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  unsigned int chunk_ref_size=get_chunk_ref_size(hx);
  ssize_t mmap_size;
#if (HAVE_MMAP)
  if (level > 2) {
    if (hx->index_mmap) return;
    fd_lock_index(hx);
    if (hx->index_mmap) {
      fd_unlock_index(hx);
      return;}
    mmap_size=u8_file_size(hx->index_cid);
    if (mmap_size>=0) {
      hx->index_mmap_size=(size_t)mmap_size;
      hx->index_mmap=
        mmap(NULL,hx->index_mmap_size,PROT_READ,MMAP_FLAGS,
             hx->index_stream.stream_fileno,0);}
    else {
      u8_log(LOG_WARN,"FailedMMAPSize",
             "Couldn't get mmap size for hash index %s",hx->index_cid);
      hx->index_mmap_size=0; hx->index_mmap=NULL;}
    if (hx->index_mmap==NULL) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "hash_index_setcache:mmap %s",hx->index_source);
      hx->index_mmap=NULL; errno=0;}
    fd_unlock_index(hx);}
  if ((level<3) && (hx->index_mmap)) {
    int retval;
    fd_lock_index(hx);
    retval=munmap(hx->index_mmap,hx->index_mmap_size);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "hash_index_setcache:munmap %s",hx->index_source);
      hx->index_mmap=NULL; errno=0;}
    fd_unlock_index(hx);}
#endif
  if (level >= 2) {
    if (hx->index_offdata) return;
    else {
      fd_stream s=&(hx->index_stream);
      unsigned int n_buckets=hx->index_n_buckets;
      unsigned int *buckets, *newmmap;
      fd_lock_index(hx);
      if (hx->index_offdata) {
        fd_unlock_index(hx);
        return;}
#if HAVE_MMAP
      newmmap=
        mmap(NULL,(n_buckets*chunk_ref_size)+256,
             PROT_READ,MMAP_FLAGS,s->stream_fileno,0);
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
        u8_log(LOG_WARN,u8_strerror(errno),
               "hash_index_setcache:mmap %s",hx->index_source);
        hx->index_offdata=NULL; errno=0;}
      else hx->index_offdata=buckets=newmmap+64;
#else
      u8_lock_mutex(&(s->stream_lock));
      stream_start_read(s);
      ht_buckets=u8_alloc_n(chunk_ref_size*(hx->index_n_buckets),unsigned int);
      fd_setpos(s,256);
      retval=fd_read_ints
        (s,(chunk_ref_size/4)*(hx->index_n_buckets),ht_buckets);
      if (retval<0) {
        u8_log(LOG_WARN,u8_strerror(errno),
               "hash_index_setcache:read offsets %s",hx->index_source);
        hx->index_offdata=NULL; errno=0;}
      else hx->index_offdata=ht_buckets;
      u8_unlock_mutex(&(s->stream_lock));
#endif
      fd_unlock_index(hx);}}
  else if (level < 2) {
    if (hx->index_offdata == NULL) return;
    else {
      int retval;
      fd_lock_index(hx);
      if (hx->index_offdata==NULL) {
        fd_unlock_index(hx);
        return;}
#if HAVE_MMAP
      retval=munmap((hx->index_offdata)-64,((hx->index_n_buckets)*chunk_ref_size)+256);
      if (retval<0) {
        u8_log(LOG_WARN,u8_strerror(errno),
               "hash_index_setcache:munmap %s",hx->index_source);
        hx->index_offdata=NULL; errno=0;}
#else
      u8_free(hx->index_offdata);
#endif
      hx->index_offdata=NULL;
      fd_unlock_index(hx);}}
}


/* Populating a hash index
   This writes data into the hashtable but ignores what is already there.
   It is commonly used when initializing a hash index. */

struct INDEX_POP_SCHEDULE {
  fdtype key; unsigned int fd_bucketno;
  unsigned int size;};
struct INDEX_BUCKET_REF {
  /* max_new is only used when committing. */
  unsigned int fd_bucketno, max_new;
  FD_CHUNK_REF ref;};

static int sort_ps_by_bucket(const void *p1,const void *p2)
{
  struct INDEX_POP_SCHEDULE *ps1=(struct INDEX_POP_SCHEDULE *)p1;
  struct INDEX_POP_SCHEDULE *ps2=(struct INDEX_POP_SCHEDULE *)p2;
  if (ps1->fd_bucketno<ps2->fd_bucketno) return -1;
  else if (ps1->fd_bucketno>ps2->fd_bucketno) return 1;
  else return 0;
}

static int sort_br_by_bucket(const void *p1,const void *p2)
{
  struct INDEX_BUCKET_REF *ps1=(struct INDEX_BUCKET_REF *)p1;
  struct INDEX_BUCKET_REF *ps2=(struct INDEX_BUCKET_REF *)p2;
  if (ps1->fd_bucketno<ps2->fd_bucketno) return -1;
  else if (ps1->fd_bucketno>ps2->fd_bucketno) return 1;
  else return 0;
}

static int sort_br_by_off(const void *p1,const void *p2)
{
  struct INDEX_BUCKET_REF *ps1=(struct INDEX_BUCKET_REF *)p1;
  struct INDEX_BUCKET_REF *ps2=(struct INDEX_BUCKET_REF *)p2;
  if (ps1->ref.off<ps2->ref.off) return -1;
  else if (ps1->ref.off>ps2->ref.off) return 1;
  else return 0;
}

static int populate_prefetch
   (struct INDEX_POP_SCHEDULE *psched,fd_index ix,int i,int blocksize,int n_keys)
{
  fdtype prefetch=FD_EMPTY_CHOICE;
  int k=i, lim=k+blocksize, final_bckt;
  fd_index_swapout(ix);
  if (lim>n_keys) lim=n_keys;
  while (k<lim) {
    fdtype key=psched[k++].key; fd_incref(key);
    FD_ADD_TO_CHOICE(prefetch,key);}
  final_bckt=psched[k-1].fd_bucketno; while (k<n_keys)
    if (psched[k].fd_bucketno != final_bckt) break;
    else {
      fdtype key=psched[k++].key; fd_incref(key);
      FD_ADD_TO_CHOICE(prefetch,key);}
  fd_index_prefetch(ix,prefetch);
  fd_decref(prefetch);
  return k;
}

FD_EXPORT int fd_populate_hash_index
  (struct FD_HASH_INDEX *hx,fdtype from,
   const fdtype *keys,int n_keys, int blocksize)
{
  /* This overwrites all the data in *hx* with the key/value mappings
     in the table *from* for the keys *keys*. */
  int i=0, n_buckets=hx->index_n_buckets;
  int filled_buckets=0, bucket_count=0, fetch_max=-1;
  int cycle_buckets=0, cycle_keys=0, cycle_max=0;
  int overall_buckets=0, overall_keys=0, overall_max=0;
  struct INDEX_POP_SCHEDULE *psched=u8_alloc_n(n_keys,struct INDEX_POP_SCHEDULE);
  struct FD_OUTBUF out; FD_INIT_BYTE_OUTBUF(&out,8192);
  struct INDEX_BUCKET_REF *bucket_refs; fd_index ix=NULL;
  fd_stream stream=&(hx->index_stream);
  struct FD_OUTBUF *outstream=fd_writebuf(stream);
  fd_off_t endpos=fd_endpos(stream);
  double start_time=u8_elapsed_time();

  if (psched==NULL) {
    u8_free(out.bufbase);
    fd_seterr(fd_MallocFailed,"populuate_hash_index",NULL,FD_VOID);
    return -1;}

  if ((FD_INDEXP(from))||(FD_TYPEP(from,fd_raw_index_type)))
    ix=fd_indexptr(from);

  /* Population doesn't leave any odd keys */
  if ((hx->fdb_xformat)&(FD_HASH_INDEX_ODDKEYS))
    hx->fdb_xformat=hx->fdb_xformat&(~(FD_HASH_INDEX_ODDKEYS));

  if ((hx->fdb_xformat)&(FD_HASH_INDEX_DTYPEV2))
    out.buf_flags=out.buf_flags|FD_USE_DTYPEV2;

  /* Fill the key schedule and count the number of buckets used */
  memset(psched,0,n_keys*sizeof(struct INDEX_POP_SCHEDULE));
  {
    int cur_bucket=-1;
    const fdtype *keyscan=keys, *keylim=keys+n_keys;
    while (keyscan<keylim) {
      fdtype key=*keyscan++;
      int bucket;
      out.bufpoint=out.bufbase; /* Reset stream */
      psched[i].key=key;
      write_zkey(hx,&out,key);
      psched[i].size=(out.bufpoint-out.bufbase);
      bucket=hash_bytes(out.bufbase,psched[i].size)%n_buckets;
      psched[i].fd_bucketno=bucket;
      if (bucket!=cur_bucket) {cur_bucket=bucket; filled_buckets++;}
      i++;}
    /* Allocate the bucket_refs */
    bucket_refs=u8_alloc_n(filled_buckets,struct INDEX_BUCKET_REF);}

  /* Sort the key schedule by bucket */
  qsort(psched,n_keys,sizeof(struct INDEX_POP_SCHEDULE),sort_ps_by_bucket);

  i=0; while (i<n_keys) {
    struct FD_OUTBUF keyblock; int retval;
    unsigned char buf[4096];
    unsigned int bucket=psched[i].fd_bucketno, load=0, j=i;
    while ((j<n_keys) && (psched[j].fd_bucketno==bucket)) j++;
    cycle_buckets++; cycle_keys=cycle_keys+(j-i);
    if ((j-i)>cycle_max) cycle_max=j-i;
    bucket_refs[bucket_count].fd_bucketno=bucket;
    load=j-i; 
    FD_INIT_FIXED_BYTE_OUTBUF(&keyblock,buf,4096);

    if ((hx->fdb_xformat)&(FD_HASH_INDEX_DTYPEV2))
      keyblock.buf_flags=keyblock.buf_flags|FD_USE_DTYPEV2;

    fd_write_zint(&keyblock,load);
    if ((ix) && (i>=fetch_max)) {
      double fetch_start=u8_elapsed_time();
      if (i>0) {
        double elapsed=fetch_start-start_time;
        double togo=elapsed*((1.0*(n_keys-i))/(1.0*i));
        double total=elapsed+togo;
        double percent=(100.0*i)/(1.0*n_keys);
        u8_message("Distributed %d keys over %d buckets, averaging %.2f keys per bucket (%d keys max)",
                   cycle_keys,cycle_buckets,
                   ((1.0*cycle_keys)/(1.0*cycle_buckets)),
                   cycle_max);
        overall_keys=overall_keys+cycle_keys;
        overall_buckets=overall_buckets+cycle_buckets;
        if (cycle_max>overall_max) overall_max=cycle_max;
        cycle_keys=cycle_buckets=cycle_max=0;
        u8_message("Processed %d of %d keys (%.2f%%) from %s in %.2f secs, ~%.2f secs to go (~%.2f secs total)",
                   i,n_keys,percent,ix->index_cid,elapsed,togo,total);}
      if (i>0)
        u8_message("Overall, distributed %d keys over %d buckets, averaging %.2f keys per bucket (%d keys max)",
                   overall_keys,overall_buckets,
                   ((1.0*overall_keys)/(1.0*overall_buckets)),
                   overall_max);
      fetch_max=populate_prefetch(psched,ix,i,blocksize,n_keys);
      u8_message("Prefetched %d keys from %s in %.3f seconds",
                 fetch_max-i,ix->index_cid,u8_elapsed_time()-fetch_start);}

    while (i<j) {
      fdtype key=psched[i].key, values=fd_get(from,key,FD_EMPTY_CHOICE);
      fd_write_zint(&keyblock,psched[i].size);
      write_zkey(hx,&keyblock,key);
      fd_write_zint(&keyblock,FD_CHOICE_SIZE(values));
      if (FD_EMPTY_CHOICEP(values)) {}
      else if (FD_CHOICEP(values)) {
        int bytes_written=0;
        CHECK_POS(endpos,stream);
        fd_write_zint(&keyblock,endpos);
        retval=fd_write_zint(outstream,FD_CHOICE_SIZE(values));
        if (retval<0) {
          if ((keyblock.buf_flags)&(FD_BUFFER_IS_MALLOCD))
            u8_free(keyblock.bufbase);
          fd_close_outbuf(&out);
          u8_free(psched);
          u8_free(bucket_refs);
          return -1;}
        else bytes_written=bytes_written+retval;
        {FD_DO_CHOICES(value,values) {
          int retval=write_zvalue(hx,outstream,value);
          if (retval<0) {
            if ((keyblock.buf_flags)&(FD_BUFFER_IS_MALLOCD))
              u8_free(keyblock.bufbase);
            fd_close_outbuf(&out);
            u8_free(psched);
            u8_free(bucket_refs);
            return -1;}
          else bytes_written=bytes_written+retval;}}
        /* Write a NULL to indicate no continuation. */
        fd_write_byte(outstream,0); bytes_written++;
        fd_write_zint(&keyblock,bytes_written);
        endpos=endpos+bytes_written;
        CHECK_POS(endpos,stream);}
      else write_zvalue(hx,&keyblock,values);
      fd_decref(values);
      i++;}
    /* Write the bucket information */
    bucket_refs[bucket_count].ref.off=endpos;
    retval=fd_write_bytes
      (outstream,
       keyblock.bufbase,
       keyblock.bufpoint-keyblock.bufbase);
    if (retval<0) {
      if ((keyblock.buf_flags)&(FD_BUFFER_IS_MALLOCD))
        u8_free(keyblock.bufbase);
      fd_close_outbuf(&out);
      u8_free(psched);
      u8_free(bucket_refs);
      return -1;}
    else bucket_refs[bucket_count].ref.size=retval;
    endpos=endpos+retval;
    CHECK_POS(endpos,stream);
    bucket_count++;}
  qsort(bucket_refs,bucket_count,sizeof(struct INDEX_BUCKET_REF),sort_br_by_bucket);
  /* This would probably be faster if we put it all in a huge vector and wrote it
     out all at once.  */
  i=0; while (i<bucket_count) {
    fd_setpos(stream,256+bucket_refs[i].fd_bucketno*8);
    fd_write_4bytes(outstream,bucket_refs[i].ref.off);
    fd_write_4bytes(outstream,bucket_refs[i].ref.size);
    i++;}
  fd_setpos(stream,16);
  fd_write_4bytes(outstream,n_keys);
  fd_flush_stream(stream);
  overall_keys=overall_keys+cycle_keys;
  overall_buckets=overall_buckets+cycle_buckets;
  if (cycle_max>overall_max) overall_max=cycle_max;
  u8_message
    ("Finished in %f seconds, placing %d keys over %d buckets, averaging %.2f keys/bucket (%d keys max)",
     u8_elapsed_time()-start_time,
     overall_keys,overall_buckets,
     ((1.0*overall_keys)/(1.0*overall_buckets)),
     overall_max);
  fd_close_outbuf(&out);
  u8_free(psched);
  u8_free(bucket_refs);
  return bucket_count;
}


/* COMMIT */

/* General design:

   Handle edits first, converting drops into sets and adding them to
   the commit schedule.

   Then handle adds, pushing them onto the commit schedule.

   Fill in the block refs for all the entries in the commit schedule.

   Sort the commit schedule by ref and read data for some number of
   buckets, parsing each bucket data into a FD_KEYBUCKET structure made
   up of FD_KEYENTRY structs.

   Add buckref values to each commit entry for the retrieved keybucket
   it is in.

   Iterate over the commit schedule, updating the keybucket structure
   for each key, potentially writing value blocks to disk.

   Then iterate over the keybucket structure, outputting keyblocks and
   updating a global bucket/blockrefs mapping.

   On a first pass implementation, don't process it in chunks.  The
   disadvantage is that you'll have lots of keyblocks in memory, but
   we can deal with that for now.

*/

struct INDEX_COMMIT_SCHEDULE {
  fdtype key, values; short replace;
  int fd_bucketno;};

struct FD_KEYENTRY {
  int fd_dtrep_size, fd_nvals;
  /* Make this into an int relative to the parent keybucket's keybuf */
  const unsigned char *dtype_start;
  fdtype values; FD_CHUNK_REF vref;};

struct FD_KEYBUCKET {
  int fd_bucketno, table_n_keys;
  unsigned char *fd_keybuf;
  struct FD_KEYENTRY fdk_elt0;};

static int sort_cs_by_bucket(const void *k1,const void *k2)
{
  struct INDEX_COMMIT_SCHEDULE *ks1=(struct INDEX_COMMIT_SCHEDULE *)k1;
  struct INDEX_COMMIT_SCHEDULE *ks2=(struct INDEX_COMMIT_SCHEDULE *)k2;
  if (ks1->fd_bucketno<ks2->fd_bucketno) return -1;
  else if (ks1->fd_bucketno>ks2->fd_bucketno) return 1;
  else return 0;
}

static int sort_kb_by_bucket(const void *k1,const void *k2)
{
  struct FD_KEYBUCKET **kb1=(struct FD_KEYBUCKET **)k1;
  struct FD_KEYBUCKET **kb2=(struct FD_KEYBUCKET **)k2;
  if ((*kb1)->fd_bucketno<(*kb2)->fd_bucketno) return -1;
  else if ((*kb1)->fd_bucketno>(*kb2)->fd_bucketno) return 1;
  else return 0;
}

static int process_edits(struct FD_HASH_INDEX *hx,fd_hashset taken,
                         struct INDEX_COMMIT_SCHEDULE *s,int i)
{
  fd_hashtable adds=&(hx->index_adds), edits=&(hx->index_edits);
  fd_hashtable cache=&(hx->index_cache);
  fdtype *drops=u8_alloc_n((edits->table_n_keys),fdtype), *drop_values;
  int j=0, n_drops=0, oddkeys=0;
  struct FD_HASH_BUCKET **scan=edits->ht_buckets, **lim=scan+edits->ht_n_buckets;
  while (scan < lim)
    if (*scan) {
      struct FD_HASH_BUCKET *e=*scan; int n_keyvals=e->fd_n_entries;
      struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+n_keyvals;
      while (kvscan<kvlimit) {
        fdtype key=kvscan->fd_kvkey;
        if (FD_PAIRP(key))
          if ((FD_CAR(key))==set_symbol) {
            fdtype real_key=FD_CDR(key);
            fd_hashset_add(taken,real_key);
            if ((oddkeys==0) && (FD_PAIRP(real_key)) &&
                ((FD_OIDP(FD_CAR(real_key))) ||
                 (FD_SYMBOLP(FD_CAR(real_key))))) {
              if (get_slotid_index(hx,key)<0) oddkeys=1;}
            s[i].key=real_key;
            s[i].values=fd_make_simple_choice(kvscan->fd_keyval);
            s[i].replace=1; i++;}
          else if ((FD_CAR(key))==drop_symbol) {
            fdtype key_to_drop=FD_CDR(key);
            fdtype cached=fd_hashtable_get(cache,key_to_drop,FD_VOID);
            if (FD_VOIDP(cached))
              drops[n_drops++]=FD_CDR(key);
            else {
              fdtype added=fd_hashtable_get_nolock(adds,key_to_drop,FD_EMPTY_CHOICE);
              FD_ADD_TO_CHOICE(cached,added);
              s[i].key=key_to_drop;
              s[i].values=fd_difference(cached,kvscan->fd_keyval);
              s[i].replace=1;
              fd_incref(key_to_drop);
              fd_decref(cached);
              i++;}}
          else {}
        else {}
        kvscan++;}
      scan++;}
    else scan++;
  if (oddkeys) hx->fdb_xformat=((hx->fdb_xformat)|(FD_HASH_INDEX_ODDKEYS));
  drop_values=hash_index_fetchn_inner((fd_index)hx,n_drops,drops,1,1);
  scan=edits->ht_buckets; lim=scan+edits->ht_n_buckets;
  j=0; while (scan < lim)
    if (*scan) {
      struct FD_HASH_BUCKET *e=*scan; int n_keyvals=e->fd_n_entries;
      struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+n_keyvals;
      while (kvscan<kvlimit) {
        fdtype key=kvscan->fd_kvkey;
        if ((FD_PAIRP(key)) && ((FD_CAR(key))==drop_symbol)) {
          fdtype key_to_drop=FD_CDR(key);
          if ((j<n_drops) && (FD_CDR(key)==drops[j])) {
            fdtype cached=drop_values[j];
            fdtype added=
              fd_hashtable_get_nolock(adds,key_to_drop,FD_EMPTY_CHOICE);
            FD_ADD_TO_CHOICE(cached,added);
            s[i].key=key_to_drop;
            s[i].values=fd_difference(cached,kvscan->fd_keyval);
            s[i].replace=1;
            fd_incref(key_to_drop);
            fd_decref(cached);
            i++; j++;}}
        kvscan++;}
      scan++;}
    else scan++;
  u8_free(drops); u8_free(drop_values);
  return i;
}

static int process_adds(struct FD_HASH_INDEX *hx,fd_hashset taken,
                        struct INDEX_COMMIT_SCHEDULE *s,int i)
{
  int oddkeys=((hx->fdb_xformat)&(FD_HASH_INDEX_ODDKEYS));
  fd_hashtable adds=&(hx->index_adds);
  struct FD_HASH_BUCKET **scan=adds->ht_buckets, **lim=scan+adds->ht_n_buckets;
  while (scan < lim)
    if (*scan) {
      struct FD_HASH_BUCKET *e=*scan; int n_keyvals=e->fd_n_entries;
      struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+n_keyvals;
      while (kvscan<kvlimit) {
        fdtype key=kvscan->fd_kvkey;
        if (!(fd_hashset_get(taken,key))) {
          if ((oddkeys==0) && (FD_PAIRP(key)) &&
              ((FD_OIDP(FD_CAR(key))) || (FD_SYMBOLP(FD_CAR(key))))) {
            if (get_slotid_index(hx,key)<0) oddkeys=1;}
          s[i].key=key;
          s[i].values=fd_make_simple_choice(kvscan->fd_keyval);
          s[i].replace=0; 
          fd_incref(key);
          i++;}
        kvscan++;}
      scan++;}
    else scan++;
  if (oddkeys) hx->fdb_xformat=((hx->fdb_xformat)|(FD_HASH_INDEX_ODDKEYS));
  return i;
}

FD_FASTOP void parse_keybucket(fd_hash_index hx,struct FD_KEYBUCKET *kb,
                               fd_inbuf in,int n_keys)
{
  int i=0; struct FD_KEYENTRY *base_entry=&(kb->fdk_elt0);
  kb->table_n_keys=n_keys;
  while (i<n_keys) {
    int dt_size=fd_read_zint(in), n_values;
    struct FD_KEYENTRY *entry=base_entry+i;
    entry->fd_dtrep_size=dt_size; entry->dtype_start=in->bufpoint;
    in->bufpoint=in->bufpoint+dt_size;
    entry->fd_nvals=n_values=fd_read_zint(in);
    if (n_values==0) entry->values=FD_EMPTY_CHOICE;
    else if (n_values==1) entry->values=read_zvalue(hx,in);
    else {
      entry->values=FD_VOID;
      entry->vref.off=fd_read_zint(in);
      entry->vref.size=fd_read_zint(in);}
    i++;}
}

FD_FASTOP FD_CHUNK_REF write_value_block
(struct FD_HASH_INDEX *hx,fd_stream stream,
 fdtype values,fdtype extra,
 fd_off_t cont_off,fd_off_t cont_size,fd_off_t startpos)
{
  struct FD_OUTBUF *outstream=fd_writebuf(stream);
  FD_CHUNK_REF retval; fd_off_t endpos=startpos;
  if (FD_CHOICEP(values)) {
    int full_size=FD_CHOICE_SIZE(values)+((FD_VOIDP(extra))?0:1);
    endpos=endpos+fd_write_zint(outstream,full_size);
    if (!(FD_VOIDP(extra)))
      endpos=endpos+write_zvalue(hx,outstream,extra);
    {FD_DO_CHOICES(value,values)
        endpos=endpos+write_zvalue(hx,outstream,value);}}
  else if (FD_VOIDP(extra)) {
    endpos=endpos+fd_write_zint(outstream,1);
    endpos=endpos+write_zvalue(hx,outstream,values);}
  else {
    endpos=endpos+fd_write_zint(outstream,2);
    endpos=endpos+write_zvalue(hx,outstream,extra);
    endpos=endpos+write_zvalue(hx,outstream,values);}
  endpos=endpos+fd_write_zint(outstream,cont_size);
  if (cont_size)
    endpos=endpos+fd_write_zint(outstream,cont_off);
  retval.off=startpos; retval.size=endpos-startpos;
  return retval;
}

/* This adds new entries to a keybucket, writing value blocks to the
   file where neccessary (more than one value). */
FD_FASTOP fd_off_t extend_keybucket
  (fd_hash_index hx,struct FD_KEYBUCKET *kb,
   struct INDEX_COMMIT_SCHEDULE *schedule,int i,int j,
   fd_outbuf newkeys,
   fd_off_t endpos,ssize_t maxpos)
{
  int k=i, free_keyvecs=0;
  int _keyoffs[16], _keysizes[16], *keyoffs, *keysizes;
  if (FD_EXPECT_FALSE((j-i)>16) )  {
    keyoffs=u8_alloc_n((j-i),int);
    keysizes=u8_alloc_n((j-i),int);
    free_keyvecs=1;}
  else {keyoffs=_keyoffs; keysizes=_keysizes;}
  while (k<j) {
    int off;
    keyoffs[k-i]=off=newkeys->bufpoint-newkeys->bufbase;
    write_zkey(hx,newkeys,schedule[k].key);
    keysizes[k-i]=(newkeys->bufpoint-newkeys->bufbase)-off;
    k++;}
  k=i; while (k<j) {
    unsigned char *keydata=newkeys->bufbase+keyoffs[k-i];
    struct FD_KEYENTRY *ke=&(kb->fdk_elt0);
    int keysize=keysizes[k-i];
    int scan=0, n_keys=kb->table_n_keys, n_values;
    while (scan<n_keys) {
      if ((ke[scan].fd_dtrep_size)!= keysize) scan++;
      else if (memcmp(keydata,ke[scan].dtype_start,keysize)) scan++;
      else if (schedule[k].replace) {
        /* The key is already in there, but we are ignoring
           it's current value.  If key has more than one associated
           values, we write a value block, otherwise we store the value
           in the key entry. */
        int n_values=FD_CHOICE_SIZE(schedule[k].values);
        ke[scan].fd_nvals=n_values;
        if (n_values==0) {
          ke[scan].values=FD_EMPTY_CHOICE;
          ke[scan].vref.off=0; ke[scan].vref.size=0;}
        else if (n_values==1) {
          ke[scan].values=schedule[k].values;
          ke[scan].vref.off=0; ke[scan].vref.size=0;}
        else {
          ke[scan].values=FD_VOID;
          ke[scan].vref=
            write_value_block(hx,&(hx->index_stream),schedule[k].values,FD_VOID,
                              0,0,endpos);
          endpos=ke[scan].vref.off+ke[scan].vref.size;}
        if (endpos>=maxpos) {
          if (free_keyvecs) {
            u8_free(keyoffs); u8_free(keysizes);
            u8_seterr(fd_DataFileOverflow,"extend_keybucket",
                      u8_mkstring("%s: %lld >= %lld",hx->index_cid,endpos,maxpos));
            return -1;}}
        scan++;
        break;}
      else {
        /* The key is already in there and has values, so we write
           a value block with a continuation pointer to the current
           value block and update the key entry.  */
        int n_values=ke[scan].fd_nvals;
        int n_values_added=FD_CHOICE_SIZE(schedule[k].values);
        ke[scan].fd_nvals=n_values+n_values_added;
        if (n_values==0) {}
        else if (n_values==1) {
          /* This is the special case is where there is one current value
             and we are adding to that.  The value block we write must
             contain both the current singleton value and whatever values
             we are adding.  We pass this as the fourth (extra) argument
             to write_value_block.  */
          fdtype current=ke[scan].values;
          ke[scan].vref=
            write_value_block(hx,&(hx->index_stream),schedule[k].values,current,
                              0,0,endpos);
          endpos=ke[scan].vref.off+ke[scan].vref.size;
          ke[scan].values=FD_VOID;
          fd_decref(current);}
        else {
          ke[scan].vref=
            write_value_block(hx,&(hx->index_stream),schedule[k].values,FD_VOID,
                              ke[scan].vref.off,ke[scan].vref.size,
                              endpos);
          /* We void the values field because there's a values block now. */
          ke[scan].values=FD_VOID;
          endpos=ke[scan].vref.off+ke[scan].vref.size;}
        if (endpos>=maxpos) {
          if (free_keyvecs) {
            u8_free(keyoffs); u8_free(keysizes);
            u8_seterr(fd_DataFileOverflow,"extend_keybucket",
                      u8_mkstring("%s: %lld >= %lld",hx->index_cid,endpos,maxpos));
            return -1;}}
        scan++;
        break;}}
    /* This is the case where we are adding a new key to the bucket. */
    if (scan==n_keys) {
      ke[n_keys].fd_dtrep_size=keysize;
      ke[n_keys].fd_nvals=n_values=FD_CHOICE_SIZE(schedule[k].values);
      ke[n_keys].dtype_start=newkeys->bufbase+keyoffs[k-i];
      if (n_values==0) ke[n_keys].values=FD_EMPTY_CHOICE;
      else if (n_values==1) ke[n_keys].values=schedule[k].values;
      else {
        ke[n_keys].values=FD_VOID;
        ke[n_keys].vref=
          write_value_block(hx,&(hx->index_stream),schedule[k].values,FD_VOID,
                            0,0,endpos);
        endpos=ke[scan].vref.off+ke[scan].vref.size;
        if (endpos>=maxpos) {
          if (free_keyvecs) {
            u8_free(keyoffs); u8_free(keysizes);
            u8_seterr(fd_DataFileOverflow,"extend_keybucket",
                      u8_mkstring("%s: %lld >= %lld",hx->index_cid,endpos,maxpos));
            return -1;}}}
      kb->table_n_keys++;}
    k++;}
  return endpos;
}

FD_FASTOP fd_off_t write_keybucket
(fd_hash_index hx,
 fd_stream stream,
 struct FD_KEYBUCKET *kb,
 fd_off_t endpos,fd_off_t maxpos)
{
  int i=0, n_keys=kb->table_n_keys;
  struct FD_KEYENTRY *ke=&(kb->fdk_elt0);
  struct FD_OUTBUF *outstream=fd_writebuf(stream);
  endpos=endpos+fd_write_zint(outstream,n_keys);
  while (i<n_keys) {
    int dtype_size=ke[i].fd_dtrep_size, n_values=ke[i].fd_nvals;
    endpos=endpos+fd_write_zint(outstream,dtype_size);
    endpos=endpos+fd_write_bytes(outstream,ke[i].dtype_start,dtype_size);
    endpos=endpos+fd_write_zint(outstream,n_values);
    if (n_values==0) {}
    else if (n_values==1)
      endpos=endpos+write_zvalue(hx,outstream,ke[i].values);
    else {
      endpos=endpos+fd_write_zint(outstream,ke[i].vref.off);
      endpos=endpos+fd_write_zint(outstream,ke[i].vref.size);}
    i++;}
  if (endpos>=maxpos) {
    u8_seterr(fd_DataFileOverflow,"write_keybucket",
              u8_mkstring("%s: %lld >= %lld",hx->index_cid,endpos,maxpos));
    return -1;}
  return endpos;
}

FD_FASTOP struct FD_KEYBUCKET *read_keybucket
  (fd_hash_index hx,int bucket,FD_CHUNK_REF ref,int extra)
{
  int n_keys;
  struct FD_INBUF keyblock;
  struct FD_KEYBUCKET *kb; unsigned char *keybuf;
  if (ref.size) {
    keybuf=u8_malloc(ref.size);
    open_block(&keyblock,hx,ref.off,ref.size,keybuf,DONT_LOCK_STREAM);
    n_keys=fd_read_zint(&keyblock);
    kb=(struct FD_KEYBUCKET *)
      u8_malloc(sizeof(struct FD_KEYBUCKET)+
                sizeof(struct FD_KEYENTRY)*((extra+n_keys)-1));
    kb->fd_bucketno=bucket;
    kb->table_n_keys=n_keys; kb->fd_keybuf=keybuf;
    parse_keybucket(hx,kb,&keyblock,n_keys);}
  else {
    kb=(struct FD_KEYBUCKET *)
      u8_malloc(sizeof(struct FD_KEYBUCKET)+
                sizeof(struct FD_KEYENTRY)*(extra-1));
    kb->fd_bucketno=bucket;
    kb->table_n_keys=0; kb->fd_keybuf=NULL;}
  return kb;
}

static int update_hash_index_ondisk
  (fd_hash_index hx,unsigned int flags,unsigned int new_keys,
   unsigned int changed_buckets,struct INDEX_BUCKET_REF *bucket_locs);


static int hash_index_commit(struct FD_INDEX *ix)
{
  int new_keys=0, n_keys, new_buckets=0;
  int schedule_max, changed_buckets=0, total_keys=0;
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  struct FD_STREAM *stream=&(hx->index_stream);
  struct FD_OUTBUF *outstream=fd_writebuf(stream);
  struct INDEX_BUCKET_REF *bucket_locs;
  fd_off_t recovery_start, recovery_pos;
  ssize_t endpos, maxpos=get_maxpos(hx);
  fd_lock_index(hx);
  fd_lock_stream(stream);
  fd_write_lock_table(&(hx->index_adds));
  fd_write_lock_table(&(hx->index_edits));
  schedule_max=hx->index_adds.table_n_keys+hx->index_edits.table_n_keys;
  bucket_locs=u8_alloc_n(schedule_max,struct INDEX_BUCKET_REF);
  {
    int i=0, bscan=0;
    int schedule_size=0;
    struct INDEX_COMMIT_SCHEDULE *schedule=
      u8_alloc_n(schedule_max,struct INDEX_COMMIT_SCHEDULE);
    struct FD_KEYBUCKET **keybuckets=
      u8_alloc_n(schedule_max,struct FD_KEYBUCKET *);
    struct FD_HASHSET taken;
    struct FD_OUTBUF out, newkeys;
    /* First, we populate the commit schedule.
       The 'taken' hashset contains keys that are edited.
       We process all of the edits, getting values if neccessary.
       Then we process all the adds. */
    fd_init_hashset(&taken,3*(hx->index_edits.table_n_keys),FD_STACK_CONS);
#if FD_DEBUG_HASHINDICES
    u8_message("Adding %d edits to the schedule",hx->index_edits.table_n_keys);
#endif
    /* Get all the keys we need to write.  */
    schedule_size=process_edits(hx,&taken,schedule,schedule_size);
#if FD_DEBUG_HASHINDICES
    u8_message("Adding %d adds to the schedule",hx->index_adds.table_n_keys);
#endif
    schedule_size=process_adds(hx,&taken,schedule,schedule_size);
    fd_recycle_hashset(&taken);

    /* Release the modification hashtables, which let's other threads
       start writing to the index again. */
    fd_reset_hashtable(&(ix->index_adds),67,0);
    fd_unlock_table(&(ix->index_adds));
    fd_reset_hashtable(&(ix->index_edits),67,0);
    fd_unlock_table(&(ix->index_edits));

    /* The commit schedule is now filled and we start generating a bucket schedule. */
    /* We're going to write keys and values, so we create streams to do so. */
    FD_INIT_BYTE_OUTBUF(&out,1024);
    FD_INIT_BYTE_OUTBUF(&newkeys,schedule_max*16);
    if ((hx->fdb_xformat)&(FD_HASH_INDEX_DTYPEV2)) {
      out.buf_flags=out.buf_flags|FD_USE_DTYPEV2;
      newkeys.buf_flags=newkeys.buf_flags|FD_USE_DTYPEV2;}
    /* Compute all the buckets for all the keys */
#if FD_DEBUG_HASHINDICES
    u8_message("Computing the buckets for %d scheduled keys",schedule_size);
#endif
    /* Compute the hashes and the buckets for all of the keys
       in the commit schedule. */
    i=0; while (i<schedule_size) {
      fdtype key=schedule[i].key; int bucket;
      out.bufpoint=out.bufbase;
      write_zkey(hx,&out,key);
      schedule[i].fd_bucketno=bucket=
        hash_bytes(out.bufbase,out.bufpoint-out.bufbase)%
        (hx->index_n_buckets);
      i++;}
    /* Get all the bucket locations.  It may be that we can fold this
       into the phase above when we have the offsets table in
       memory. */
#if FD_DEBUG_HASHINDICES
    u8_message("Fetching bucket locations");
#endif
    qsort(schedule,schedule_size,sizeof(struct INDEX_COMMIT_SCHEDULE),
          sort_cs_by_bucket);
    i=0; bscan=0; while (i<schedule_size) {
      int bucket=schedule[i].fd_bucketno, j=i;
      bucket_locs[changed_buckets].fd_bucketno=bucket;
      bucket_locs[changed_buckets].ref=index_chunk_ref(hx,bucket,1);
      while ((j<schedule_size) && 
             (schedule[j].fd_bucketno==bucket)) 
        j++;
      bucket_locs[changed_buckets].max_new=j-i;
      changed_buckets++; i=j;}
    /* Now we have all the bucket locations, which we'll read in
       order. */
    /* Read all the buckets in order, reading each keyblock.  We may
       be able to combine this with extending the bucket below, but
       that would entail moving the writing of values out of the
       bucket extension (since both want to get at the file) Could we
       have two pointers into the file?  */
#if FD_DEBUG_HASHINDICES
    u8_message("Reading all the %d changed buckets in order",
               changed_buckets);
#endif
    qsort(bucket_locs,changed_buckets,sizeof(struct INDEX_BUCKET_REF),
          sort_br_by_off);
    i=0; while (i<changed_buckets) {
      keybuckets[i]=
        read_keybucket(hx,bucket_locs[i].fd_bucketno,
                       bucket_locs[i].ref,bucket_locs[i].max_new);
      if ((keybuckets[i]->table_n_keys)==0) new_buckets++;
      i++;}
    /* Now all the keybuckets have been read and buckets have been
       created for keys that didn't have buckets before. */
#if FD_DEBUG_HASHINDICES
    u8_message("Created %d new buckets",new_buckets);
#endif
    /* bucket_locs should still be sorted by bucket. */
    qsort(schedule,schedule_size,sizeof(struct INDEX_COMMIT_SCHEDULE),
          sort_cs_by_bucket);
    qsort(keybuckets,changed_buckets,sizeof(struct FD_KEYBUCKET *),
          sort_kb_by_bucket);
    qsort(bucket_locs,changed_buckets,sizeof(struct INDEX_BUCKET_REF),
          sort_br_by_bucket);
#if FD_DEBUG_HASHINDICES
    u8_message("Extending for %d keys over %d buckets",
               schedule_size,changed_buckets);
#endif
    /* March along the commit schedule (keys) and keybuckets (buckets)
       in parallel, extending each bucket.  This is where values are
       written out and their offsets stored in the loaded bucket
       structure. */
    i=0; bscan=0; endpos=fd_endpos(stream);
    while (i<schedule_size) {
      struct FD_KEYBUCKET *kb=keybuckets[bscan];
      int bucket=schedule[i].fd_bucketno, j=i, cur_keys;
      assert(bucket==kb->fd_bucketno);
      while ((j<schedule_size) && (schedule[j].fd_bucketno==bucket)) j++;
      cur_keys=kb->table_n_keys;
      /* This may write values to disk. */
      endpos=extend_keybucket(hx,kb,schedule,i,j,&newkeys,endpos,maxpos);
      CHECK_POS(endpos,&(hx->index_stream));
      new_keys=new_keys+(kb->table_n_keys-cur_keys);
      {
        fd_off_t startpos=endpos;
        /* This writes the keybucket itself. */
        endpos=write_keybucket(hx,stream,kb,endpos,maxpos);
        if (endpos<0) {
          u8_free(bucket_locs);
          u8_free(schedule);
          u8_free(keybuckets);
          fd_unlock_index(hx);
          fd_unlock_stream(stream);
          return -1;}
        CHECK_POS(endpos,&(hx->index_stream));
        bucket_locs[bscan].ref.off=startpos;
        bucket_locs[bscan].ref.size=endpos-startpos;}
      i=j; bscan++;}
    fd_flush_stream(&(hx->index_stream));
#if FD_DEBUG_HASHINDICES
    u8_message("Cleaning up");
#endif
    /* Free all the buckets */
    bscan=0; while (bscan<changed_buckets) {
      struct FD_KEYBUCKET *kb=keybuckets[bscan++];
      /* struct FD_KEYENTRY *scan=&(kb->fdk_elt0), *limit=scan+kb->table_n_keys; */
      if (kb->fd_keybuf) u8_free(kb->fd_keybuf);
      u8_free(kb);}
    u8_free(keybuckets);
    /* Now we free the keys and values in the schedule. */
    { int i=0; while (i<schedule_size) {
        fdtype key=schedule[i].key;
        fdtype v=schedule[i].values;
        fd_decref(key);
        fd_decref(v);
        i++;}}
    u8_free(schedule);
    u8_free(out.bufbase);
    u8_free(newkeys.bufbase);
    n_keys=schedule_size;}
  if (fd_acid_files) {
    int i=0;
#if FD_DEBUG_HASHINDICES
    u8_message("Writing recovery data");
#endif
    /* Write the new offsets information to the end of the file
       for recovery if we die while doing the actual write. */
    /* Start by getting the total number of keys in the new file. */
    total_keys=fd_read_4bytes(fd_start_read(stream,FD_HASH_INDEX_KEYCOUNT_POS))
      +new_keys;
    recovery_start=fd_endpos(stream);
    outstream=fd_writebuf(stream);
    fd_write_4bytes(outstream,hx->fdb_xformat);
    fd_write_4bytes(outstream,total_keys);
    fd_write_4bytes(outstream,changed_buckets);
    while (i<changed_buckets) {
      fd_write_8bytes(outstream,bucket_locs[i].ref.off);
      fd_write_4bytes(outstream,bucket_locs[i].ref.size);
      fd_write_4bytes(outstream,bucket_locs[i].fd_bucketno);
      i++;}
    fd_write_8bytes(outstream,recovery_start);
    fd_setpos(stream,0);
    fd_write_4bytes(outstream,FD_HASH_INDEX_TO_RECOVER);
    fd_flush_stream(stream);
    fsync(stream->stream_fileno);
  }
#if FD_DEBUG_HASHINDICES
  u8_message("Writing offset data changes");
#endif
  update_hash_index_ondisk
    (hx,hx->fdb_xformat,total_keys,changed_buckets,bucket_locs);
  if (fd_acid_files) {
    int retval=0;
#if FD_DEBUG_HASHINDICES
    u8_message("Erasing old recovery information");
#endif
    fd_flush_stream(stream);
    fsync(stream->stream_fileno);
    /* Now erase the recovery information, since we don't need it
       anymore. */
    recovery_pos=fd_read_8bytes(fd_start_read(stream,-8));
    if (recovery_pos!=recovery_start) {
      u8_log(LOG_ERR,"hash_index_commit",
             "Trouble truncating recovery information for %s",
             hx->index_cid);}
    retval=ftruncate(stream->stream_fileno,recovery_pos);
    if (retval<0)
      u8_log(LOG_ERR,"hash_index_commit",
             "Trouble truncating recovery information for %s",
             hx->index_cid);}

  /* Remap the file */
  if (hx->index_mmap) {
    int retval=munmap(hx->index_mmap,hx->index_mmap_size);
    if (retval<0) {
      u8_log(LOG_WARN,"MUNMAP","hash_index MUNMAP failed with %s",
             u8_strerror(errno));
      errno=0;}
    hx->index_mmap_size=u8_file_size(hx->index_cid);
    hx->index_mmap=
      mmap(NULL,hx->index_mmap_size,PROT_READ,MMAP_FLAGS,
           hx->index_stream.stream_fileno,0);}
#if FD_DEBUG_HASHINDICES
  u8_message("Resetting tables");
#endif

  u8_free(bucket_locs);

  /* And unlock all the locks. */
  fd_unlock_stream(stream);
  fd_unlock_index(hx);

#if FD_DEBUG_HASHINDICES
  u8_message("Returning from hash_index_commit()");
#endif

  return n_keys;
}

#if HAVE_MMAP
static int make_offsets_writable(fd_hash_index hx)
{
  unsigned int *newmmap, n_buckets=hx->index_n_buckets;
  size_t chunk_ref_size=get_chunk_ref_size(hx);
  int retval=munmap(hx->index_offdata-64,(n_buckets*chunk_ref_size)+256);
  if (retval<0) {
    u8_log(LOG_WARN,u8_strerror(errno),
           "hash_index/make_offsets_writable:munmap %s",hx->index_source);
    return retval;}
  newmmap=mmap(NULL,(n_buckets*chunk_ref_size)+256,
               PROT_READ|PROT_WRITE,MMAP_FLAGS,hx->index_stream.stream_fileno,0);
  if ((newmmap==NULL) || (newmmap==((void *)-1))) {
    u8_log(LOG_WARN,u8_strerror(errno),
           "hash_index/make_offsets_writable:mmap %s",hx->index_source);
    hx->index_offdata=NULL; errno=0;}
  else hx->index_offdata=newmmap+64;
  return retval;
}

static int make_offsets_unwritable(fd_hash_index hx)
{
  unsigned int *newmmap, n_buckets=hx->index_n_buckets;
  size_t chunk_ref_size=get_chunk_ref_size(hx);
  int retval=msync(hx->index_offdata-64,(n_buckets*chunk_ref_size)+256,
                   MS_SYNC|MS_INVALIDATE);
  if (retval<0) {
    u8_log(LOG_WARN,u8_strerror(errno),
           "hash_index/make_offsets_unwritable:msync %s",hx->index_source);
    return retval;}
  retval=munmap(hx->index_offdata-64,
                (n_buckets*chunk_ref_size)+256);
  if (retval<0) {
    u8_log(LOG_WARN,u8_strerror(errno),
           "hash_index/make_offsets_unwritable:munmap %s",hx->index_source);
    return retval;}
  newmmap=mmap(NULL,(n_buckets*chunk_ref_size)+256,
               PROT_READ,MMAP_FLAGS,hx->index_stream.stream_fileno,0);
  if ((newmmap==NULL) || (newmmap==((void *)-1))) {
    u8_log(LOG_WARN,u8_strerror(errno),
           "hash_index/make_offsets_unwritable:mmap %s",hx->index_source);
    hx->index_offdata=NULL; errno=0;}
  else hx->index_offdata=newmmap+64;
  return retval;
}
#endif

static int update_hash_index_ondisk
  (fd_hash_index hx,unsigned int flags,unsigned int cur_keys,
   unsigned int changed_buckets,struct INDEX_BUCKET_REF *bucket_locs)
{
  struct FD_STREAM *stream=&(hx->index_stream);
  struct FD_OUTBUF *outstream=fd_writebuf(stream);
  int i=0; unsigned int *buckets=hx->index_offdata;
#if (HAVE_MMAP)
  if (buckets) {
    make_offsets_writable(hx);
    buckets=hx->index_offdata;}
#endif
  /* Update the buckets if you have them */
  if ((buckets) && (hx->index_offtype==FD_B64)) {
    while (i<changed_buckets) {
      unsigned int word1, word2, word3, bucket=bucket_locs[i].fd_bucketno;
      word1=(((bucket_locs[i].ref.off)>>32)&(0xFFFFFFFF));
      word2=(((bucket_locs[i].ref.off))&(0xFFFFFFFF));
      word3=(bucket_locs[i].ref.size);
#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
      buckets[bucket*3]=fd_flip_word(word1);
      buckets[bucket*3+1]=fd_flip_word(word2);
      buckets[bucket*3+2]=fd_flip_word(word3);
#else
      buckets[bucket*3]=word1;
      buckets[bucket*3+1]=word2;
      buckets[bucket*3+2]=word3;
#endif
      i++;}}
  else if ((buckets) && (hx->index_offtype==FD_B32)) {
    while (i<changed_buckets) {
      unsigned int word1, word2, bucket=bucket_locs[i].fd_bucketno;
      word1=((bucket_locs[i].ref.off)&(0xFFFFFFFF));
      word2=(bucket_locs[i].ref.size);
#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
      buckets[bucket*2]=fd_flip_word(word1);
      buckets[bucket*2+1]=fd_flip_word(word2);
#else
      buckets[bucket*2]=word1;
      buckets[bucket*2+1]=word2;
#endif
      i++;}}
  else if ((buckets) && (hx->index_offtype==FD_B40)) {
    while (i<changed_buckets) {
      unsigned int word1=0, word2=0, bucket=bucket_locs[i].fd_bucketno;
      convert_FD_B40_ref(bucket_locs[i].ref,&word1,&word2);
#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
      buckets[bucket*2]=fd_flip_word(word1);
      buckets[bucket*2+1]=fd_flip_word(word2);
#else
      buckets[bucket*2]=word1;
      buckets[bucket*2+1]=word2;
#endif
      i++;}}
  /* If you don't have offsets in memory, write them by hand, stepping
     through the file. */
  else if (hx->index_offtype==FD_B64) {
    size_t bucket_start=256;
    size_t bytes_in_bucket=3*SIZEOF_INT;
    while (i<changed_buckets) {
      unsigned int bucket_no=bucket_locs[i].fd_bucketno;
      unsigned int bucket_pos=bucket_start+bytes_in_bucket*bucket_no;
      fd_start_write(stream,bucket_pos);
      fd_write_8bytes(outstream,bucket_locs[i].ref.off);
      fd_write_4bytes(outstream,bucket_locs[i].ref.size);
      i++;}}
  else if (hx->index_offtype==FD_B32) {
    size_t bucket_start=256;
    size_t bytes_in_bucket=2*SIZEOF_INT;
    while (i<changed_buckets) {
      unsigned int bucket_no=bucket_locs[i].fd_bucketno;
      unsigned int bucket_pos=bucket_start+bytes_in_bucket*bucket_no;
      fd_start_write(stream,bucket_pos);
      fd_write_4bytes(outstream,bucket_locs[i].ref.off);
      fd_write_4bytes(outstream,bucket_locs[i].ref.size);
      i++;}}
  else {
    /* This is FD_B40 encoding, 2 words */
    size_t bucket_start=256;
    size_t bytes_in_bucket=2*SIZEOF_INT;
    while (i<changed_buckets) {
      unsigned int bucket_no=bucket_locs[i].fd_bucketno;
      unsigned int bucket_pos=bucket_start+bytes_in_bucket*bucket_no;
      unsigned int word1=0, word2=0;
      convert_FD_B40_ref(bucket_locs[i].ref,&word1,&word2);
      fd_start_write(stream,bucket_pos);
      fd_write_4bytes(outstream,word1);
      fd_write_4bytes(outstream,word2);
      i++;}}
  /* The offsets have now been updated in memory */
  if (hx->index_offdata) {
#if (HAVE_MMAP)
    /* If you have MMAP, make them unwritable which swaps them out to
       the file. */
    make_offsets_unwritable(hx);
#else
    struct FD_OUTBUF *out=fd_start_write(stream,256);
    if (hx->index_offtype==FD_B64)
      fd_write_ints(outstream,3*SIZEOF_INT*(hx->index_n_buckets),
                    hx->index_offdata);
    else fd_write_ints(outstream,2*SIZEOF_INT*(hx->index_n_buckets),
                       hx->index_offdata);
#endif
  }
  /* Write any changed flags */
  fd_write_4bytes_at(stream,flags,8);
  fd_write_4bytes_at(stream,cur_keys,16);
  fd_write_4bytes_at(stream,FD_HASH_INDEX_MAGIC_NUMBER,0);
  fd_flush_stream(stream);
  return 0;
}

static int recover_hash_index(struct FD_HASH_INDEX *hx)
{
  fd_off_t recovery_pos;
  struct FD_STREAM *s=&(hx->index_stream);
  fd_inbuf in=fd_readbuf(s);
  struct INDEX_BUCKET_REF *bucket_locs;
  unsigned int i=0, flags, cur_keys, n_buckets, retval;
  fd_endpos(s); fd_movepos(s,-8);
  recovery_pos=fd_read_8bytes(in);
  fd_setpos(s,recovery_pos);
  flags=fd_read_4bytes(in);
  cur_keys=fd_read_4bytes(in);
  n_buckets=fd_read_4bytes(in);
  bucket_locs=u8_alloc_n(n_buckets,struct INDEX_BUCKET_REF);
  while (i<n_buckets) {
    bucket_locs[i].fd_bucketno=fd_read_4bytes(in);
    bucket_locs[i].ref.off=fd_read_8bytes(in);
    bucket_locs[i].ref.size=fd_read_4bytes(in);
    i++;}
  retval=update_hash_index_ondisk(hx,flags,cur_keys,n_buckets,bucket_locs);
  u8_free(bucket_locs);
  return retval;
}


/* Miscellaneous methods */

static void hash_index_close(fd_index ix)
{
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  unsigned int chunk_ref_size=get_chunk_ref_size(hx);
  u8_log(LOG_DEBUG,"HASHINDEX","Closing hash index %s",ix->index_cid);
  fd_lock_index(hx);
  if (hx->index_offdata) {
#if HAVE_MMAP
    int retval=
      munmap(hx->index_offdata-64,(chunk_ref_size*hx->index_n_buckets)+256);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "hash_index_close:munmap %s",hx->index_source);
      errno=0;}
#else
    u8_free(hx->index_offdata);
#endif
    hx->index_offdata=NULL;
    hx->index_cache_level=-1;}
  fd_free_stream(&(hx->index_stream),1);
  u8_log(LOG_DEBUG,"HASHINDEX","Closed hash index %s",ix->index_cid);
  fd_unlock_index(hx);
}

static void hash_index_setbuf(fd_index ix,int bufsiz)
{
  struct FD_HASH_INDEX *fx=(struct FD_HASH_INDEX *)ix;
  fd_lock_index(fx);
  fd_stream_setbuf(&(fx->index_stream),bufsiz);
  fd_unlock_index(fx);
}


/* The handler struct */

static struct FD_INDEX_HANDLER hash_index_handler={
  "hash_index", 1, sizeof(struct FD_HASH_INDEX), 12,
  hash_index_close, /* close */
  hash_index_commit, /* commit */
  hash_index_setcache, /* setcache */
  hash_index_setbuf, /* setbuf */
  hash_index_fetch, /* fetch */
  hash_index_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  hash_index_fetchn, /* fetchn */
  hash_index_fetchkeys, /* fetchkeys */
  hash_index_fetchsizes, /* fetchsizes */
  NULL, /* metadata */
  NULL /* sync */
};

FD_EXPORT int fd_hash_indexp(struct FD_INDEX *ix)
{
  return (ix->index_handler==&hash_index_handler);
}

FD_EXPORT fdtype fd_hash_index_stats(struct FD_HASH_INDEX *hx)
{
  fdtype result=fd_empty_slotmap();
  int n_filled=0, maxk=0, n_singles=0, n2sum=0;
  fd_add(result,fd_intern("NBUCKETS"),FD_INT(hx->index_n_buckets));
  fd_add(result,fd_intern("NKEYS"),FD_INT(hx->table_n_keys));
  fd_add(result,fd_intern("NBASEOIDS"),FD_INT(hx->index_n_baseoids));
  fd_add(result,fd_intern("NSLOTIDS"),FD_INT(hx->index_n_slotids));
  hash_index_getstats(hx,&n_filled,&maxk,&n_singles,&n2sum);
  fd_add(result,fd_intern("NFILLED"),FD_INT(n_filled));
  fd_add(result,fd_intern("NSINGLES"),FD_INT(n_singles));
  fd_add(result,fd_intern("MAXKEYS"),FD_INT(maxk));
  fd_add(result,fd_intern("N2SUM"),FD_INT(n2sum));
  {
    double avg=(hx->table_n_keys*1.0)/(n_filled*1.0);
    double sd2=(n2sum*1.0)/(n_filled*n_filled*1.0);
    fd_add(result,fd_intern("MEAN"),fd_make_flonum(avg));
    fd_add(result,fd_intern("SD2"),fd_make_flonum(sd2));
  }
  return result;
}

FD_EXPORT void fd_init_hashindices_c()
{
  set_symbol=fd_intern("SET");
  drop_symbol=fd_intern("DROP");

  u8_register_source_file(_FILEINFO);

  fd_register_index_opener(&hash_index_handler,
                           open_hash_index,
                           fd_match4bytes,
                           (void *)FD_HASH_INDEX_MAGIC_NUMBER);
  fd_register_index_opener(&hash_index_handler,
                           open_hash_index,
                           fd_match4bytes,
                           (void *)FD_HASH_INDEX_TO_RECOVER);
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
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
