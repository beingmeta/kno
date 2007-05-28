/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: fileindices.c 1023 2007-05-09 23:09:38Z haase $";

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
24     XXXX    size of slotids DTYPE representation
28     XXXX    file offset of baseoids vector
32     XXXX    size of baseoids DTYPE representation
36     XXXX    file offset of index metadata
40     XXXX    size of metadata DTYPE representation

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

#include "fdb/dtype.h"
#include "fdb/dtypeio.h"
#include "fdb/dtypestream.h"
#include "fdb/dbfile.h"
#include <errno.h>
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

#if FD_DEBUG_HASHINDICES
#define CHECK_ENDPOS(pos,stream) \
   if (endpos!=fd_getpos(stream)) u8_warn("Mismatch","endpos/getpos mismatch");
#else
#define CHECK_ENDPOS(pos,stream)
#endif

/* Used to generate hash codes */
#define MAGIC_MODULUS 16777213 /* 256000001 */
#define MIDDLIN_MODULUS 573786077 /* 256000001 */
#define MYSTERIOUS_MODULUS 2000239099 /* 256000001 */

static fdtype read_zvalues(fd_hash_index,int,off_t,size_t);

static struct FD_INDEX_HANDLER hashindex_handler;

static fd_exception CorruptedHashIndex=_("Corrupted hashindex file");

/* Utilities for DTYPE I/O */

#define nobytes(in,nbytes) (FD_EXPECT_FALSE(!(fd_needs_bytes(in,nbytes))))
#define havebytes(in,nbytes) (FD_EXPECT_TRUE(fd_needs_bytes(in,nbytes)))

#define output_byte(out,b) \
  if (fd_write_byte(out,b)<0) return -1; else {}
#define output_4bytes(out,w) \
  if (fd_write_4bytes(out,w)<0) return -1; else {}
#define output_bytes(out,bytes,n)				\
  if (fd_write_bytes(out,bytes,n)<0) return -1; else {}

#if FD_DEBUG_DTYPEIO
#define return_errcode(x) (_return_errcode(x))
#else
#define return_errcode(x) (x)
#endif


/* Opening database blocks */

FD_FASTOP fd_byte_input open_block
  (struct FD_BYTE_INPUT *bi,FD_HASH_INDEX *hx,
   fd_off_t off,fd_size_t size,unsigned char *buf)
{
  if (hx->mmap) {
    FD_INIT_BYTE_INPUT(bi,hx->mmap+off,size);
    return bi;}
  else {
    fd_setpos(&(hx->stream),off);
    fd_dtsread_bytes(&(hx->stream),buf,size);
    FD_INIT_BYTE_INPUT(bi,buf,size);
    return bi;}
}

FD_FASTOP fdtype read_dtype_at_pos(fd_dtype_stream s,fd_off_t off)
{
  fd_setpos(s,off);
  return fd_dtsread_dtype(s);
}

FD_FASTOP FD_BLOCK_REF get_block_ref(fd_hash_index hx,unsigned int bucket)
{
  if (FD_EXPECT_FALSE((hx->buckets)==NULL)) {
    struct FD_BLOCK_REF ref;
    fd_setpos(&(hx->stream),256+bucket*8);
    ref.off=fd_dtsread_4bytes(&(hx->stream));
    ref.size=fd_dtsread_4bytes(&(hx->stream));
    return ref;}
#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
  else {
    struct FD_BLOCK_REF ref;
    ref.off=fd_flip_word((((hx->buckets)[bucket]).off));
    ref.size=fd_flip_word((((hx->buckets)[bucket]).size));
    return ref;}
#else
  else return hx->buckets[bucket];
#endif
  }

/* Opening a hash index */

static int init_slotids
  (struct FD_HASH_INDEX *hx,int n_slotids,fdtype *slotids_init);
static int init_baseoids
  (struct FD_HASH_INDEX *hx,int n_baseoids,fdtype *baseoids_init);

static fd_index open_hashindex(u8_string fname,int read_only)
{
  struct FD_HASH_INDEX *index=u8_malloc(sizeof(struct FD_HASH_INDEX));
  struct FD_DTYPE_STREAM *s=&(index->stream);
  unsigned int magicno, n_buckets, hash_const, n_keys;
  fd_off_t slotids_pos, baseoids_pos, metadata_pos;
  fd_size_t slotids_size, baseoids_size, metadata_size;  
  fd_dtstream_mode mode=
    ((read_only) ? (FD_DTSTREAM_READ) : (FD_DTSTREAM_MODIFY));
  fd_init_index(index,&hashindex_handler,fname);
  if (fd_init_dtype_file_stream(s,fname,mode,FD_FILEDB_BUFSIZE,NULL,NULL)
      == NULL) {
    u8_free(index);
    fd_seterr3(fd_CantOpenFile,"open_fileindex",u8_strdup(fname));
    return NULL;}
  /* See if it ended up read only */
  if (index->stream.flags&FD_DTSTREAM_READ_ONLY) read_only=1;
  index->stream.mallocd=0;
  index->mmap=NULL;
  magicno=fd_dtsread_4bytes(s);
  index->n_buckets=fd_dtsread_4bytes(s);
#if 0
  if (magicno==FD_HASH_INDEX_TO_RECOVER) {
    u8_warn(fd_RecoveryRequired,"Recovering the file index %s",fname);
    recover_file_index(index);
    magicno=magicno&(~0x20);}
#endif
  index->buckets=NULL; index->read_only=read_only;
  index->hxflags=fd_dtsread_4bytes(s);
  index->hxcustom=fd_dtsread_4bytes(s);

  n_keys=fd_dtsread_4bytes(s); /* Currently ignored */

  slotids_pos=fd_dtsread_4bytes(s); slotids_size=fd_dtsread_4bytes(s);
  baseoids_pos=fd_dtsread_4bytes(s); baseoids_size=fd_dtsread_4bytes(s);
  metadata_pos=fd_dtsread_4bytes(s); metadata_size=fd_dtsread_4bytes(s);

  /* Initialize the slotids field used for storing feature keys */
  if (slotids_size) {
    fdtype slotids_vector=read_dtype_at_pos(s,slotids_pos);
    if (FD_VOIDP(slotids_vector)) {
      index->n_slotids=0;
      index->slotids=NULL;
      index->slotid_lookup=NULL;}
    else if (FD_VECTORP(slotids_vector)) {
      init_slotids(index,
		   FD_VECTOR_LENGTH(slotids_vector),
		   FD_VECTOR_DATA(slotids_vector));
      fd_decref(slotids_vector);}
    else {
      fd_seterr("Bad SLOTIDS data","open_hash_index",
		u8_strdup(fname),FD_VOID);
      fd_dtsclose(s,1);
      u8_free(index);
      return NULL;}}

  /* Initialize the baseoids field used for compressed OID values */
  if (baseoids_size) {
    fdtype baseoids_vector=read_dtype_at_pos(s,baseoids_pos);
    if (FD_VOIDP(baseoids_vector)) {
      index->n_baseoids=0;
      index->baseoid_ids=NULL;
      index->ids2baseoids=NULL;}
    else if (FD_VECTORP(baseoids_vector)) {
      init_baseoids(index,
		    FD_VECTOR_LENGTH(baseoids_vector),
		    FD_VECTOR_DATA(baseoids_vector));
      fd_decref(baseoids_vector);}
    else {
      fd_seterr("Bad BASEOIDS data","open_hash_index",
		u8_strdup(fname),FD_VOID);
      fd_dtsclose(s,1);
      u8_free(index);
      return NULL;}}

  fd_init_mutex(&(index->lock));

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
  fdtype *slotids; struct FD_SLOTID_LOOKUP *lookup; int i=0;
  hx->slotids=slotids=u8_malloc(sizeof(fdtype)*n_slotids);
  hx->slotid_lookup=lookup=
    u8_malloc(sizeof(FD_SLOTID_LOOKUP)*n_slotids);
  hx->n_slotids=n_slotids;
  while (i<n_slotids) {
    fdtype slotid=slotids_init[i];
    slotids[i]=slotid;
    lookup[i].zindex=i;
    lookup[i].slotid=fd_incref(slotid);
    i++;}
  qsort(lookup,n_slotids,sizeof(FD_SLOTID_LOOKUP),sort_by_slotid);
}

static int sort_by_baseoid(const void *p1,const void *p2)
{
  const fd_baseoid_lookup l1=(fd_baseoid_lookup)p1, l2=(fd_baseoid_lookup)p2;
  return (FD_OID_COMPARE((l1->baseoid),(l2->baseoid)));
}

static int init_baseoids(fd_hash_index hx,int n_baseoids,fdtype *baseoids_init)
{
  int i=0;
  unsigned int *baseoid_ids=u8_malloc(sizeof(unsigned int)*n_baseoids);
  short *ids2baseoids=u8_malloc(sizeof(short)*1024);
  memset(baseoid_ids,0,sizeof(unsigned int)*n_baseoids);
  i=0; while (i<1024) ids2baseoids[i++]=-1;
  hx->n_baseoids=n_baseoids;
  hx->baseoid_ids=baseoid_ids;
  hx->ids2baseoids=ids2baseoids;
  i=0; while (i<n_baseoids) {
    fdtype baseoid=baseoids_init[i];
    baseoid_ids[i]=FD_OID_BASE_ID(baseoid);
    ids2baseoids[FD_OID_BASE_ID(baseoid)]=i;
    i++;}
}

/* Making a hash index */

FD_EXPORT int fd_make_hashindex(u8_string fname,int n_buckets_arg,
				fdtype slotids_init,fdtype baseoids_init,fdtype metadata_init,
				time_t ctime,time_t mtime)
{
  int n_buckets;
  time_t now=time(NULL);
  off_t slotids_pos=0, baseoids_pos=0, metadata_pos=0;
  size_t slotids_size=0, baseoids_size=0, metadata_size=0;
  struct FD_DTYPE_STREAM _stream, *stream=
    fd_init_dtype_file_stream(&_stream,fname,FD_DTSTREAM_CREATE,8192,NULL,NULL);
  if (stream==NULL) return -1;
  else if ((stream->flags)&FD_DTSTREAM_READ_ONLY) {
    fd_seterr3(fd_CantWrite,"fd_make_file_index",u8_strdup(fname));
    fd_dtsclose(stream,1);
    return -1;}
  stream->mallocd=0;
  if (n_buckets_arg<0) n_buckets=-n_buckets_arg;
  else n_buckets=fd_get_hashtable_size(n_buckets_arg);
  fd_setpos(stream,0);
  fd_dtswrite_4bytes(stream,FD_HASH_INDEX_MAGIC_NUMBER);
  fd_dtswrite_4bytes(stream,n_buckets);
  fd_dtswrite_4bytes(stream,0); /* No hxflags now. */
  fd_dtswrite_4bytes(stream,0); /* No custom hash constant */
  fd_dtswrite_4bytes(stream,0); /* No keys to start */
  
  /* This is where we store the offset and size for the slotid init */
  fd_dtswrite_4bytes(stream,0); fd_dtswrite_4bytes(stream,0);
  
  /* This is where we store the offset and size for the baseoids init */
  fd_dtswrite_4bytes(stream,0); fd_dtswrite_4bytes(stream,0);
  
  /* This is where we store the offset and size for the metadata */
  fd_dtswrite_4bytes(stream,0); fd_dtswrite_4bytes(stream,0);

  /* Write the index creation time */
  if (ctime<0) ctime=now;
  fd_dtswrite_4bytes(stream,0);
  fd_dtswrite_4bytes(stream,((unsigned int)ctime));
  
  /* Write the index repack time */
  fd_dtswrite_4bytes(stream,0);
  fd_dtswrite_4bytes(stream,((unsigned int)now));
  
  /* Write the index modification time */
  if (mtime<0) mtime=now;
  fd_dtswrite_4bytes(stream,0);
  fd_dtswrite_4bytes(stream,((unsigned int)mtime));
  
  /* Fill the rest of the space. */
  {
    int i=0, bytes_to_write=256-fd_getpos(stream);
    while (i<bytes_to_write) {
      fd_dtswrite_byte(stream,0); i++;}}
  
  /* Write the top level bucket table */
  {
    int i=0; while (i<n_buckets) {
      fd_dtswrite_4bytes(stream,0); fd_dtswrite_4bytes(stream,0); i++;}}
  
  /* Write the slotids */
  if (FD_VECTORP(slotids_init)) {
    slotids_pos=fd_getpos(stream);
    fd_dtswrite_dtype(stream,slotids_init);
    slotids_size=fd_getpos(stream)-slotids_pos;}
  
  /* Write the baseoids */
  if (FD_VECTORP(baseoids_init)) {
    baseoids_pos=fd_getpos(stream);
    fd_dtswrite_dtype(stream,baseoids_init);
    baseoids_size=fd_getpos(stream)-baseoids_pos;}

  /* Write the metdata */
  if (FD_SLOTMAPP(metadata_init)) {
    metadata_pos=fd_getpos(stream);
    fd_dtswrite_dtype(stream,metadata_init);
    metadata_size=fd_getpos(stream)-metadata_pos;}

  if (slotids_pos) {
    fd_setpos(stream,20);
    fd_dtswrite_4bytes(stream,slotids_pos);
    fd_dtswrite_4bytes(stream,slotids_size);}
  if (baseoids_pos) {
    fd_setpos(stream,28);
    fd_dtswrite_4bytes(stream,baseoids_pos);
    fd_dtswrite_4bytes(stream,baseoids_size);}
  if (metadata_pos) {
    fd_setpos(stream,36);
    fd_dtswrite_4bytes(stream,metadata_pos);
    fd_dtswrite_4bytes(stream,metadata_size);}

  fd_dtsclose(stream,1);
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
  unsigned int prod=1, asint;
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
  const int size=hx->n_slotids;
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

FD_FASTOP int fast_write_dtype(fd_byte_output out,fdtype key,int v2)
{
  if (FD_OIDP(key)) {
    FD_OID addr=FD_OID_ADDR(key);
    fd_write_byte(out,dt_oid);
    fd_write_4bytes(out,FD_OID_HI(addr));
    fd_write_4bytes(out,FD_OID_LO(addr));
    return 9;}
  else if (FD_SYMBOLP(key)) {
    int data=FD_GET_IMMEDIATE(key,itype);
    fdtype name=fd_symbol_names[data];
    struct FD_STRING *s=FD_GET_CONS(name,fd_string_type,struct FD_STRING *);
    int len=s->length;
    if ((v2) && (len<256)) {
      {output_byte(out,dt_tiny_symbol);}
      {output_byte(out,len);}
      {output_bytes(out,s->bytes,len);}
      return len+2;}
    else {
      {output_byte(out,dt_symbol);}
      {output_4bytes(out,len);}
      {output_bytes(out,s->bytes,len);}
      return len+5;}}
  else if (FD_STRINGP(key)) {
    struct FD_STRING *s=FD_GET_CONS(key,fd_string_type,struct FD_STRING *);
    int len=s->length;
    if ((v2) && (len<256)) {
      {output_byte(out,dt_tiny_string);}
      {output_byte(out,len);}
      {output_bytes(out,s->bytes,len);}
      return len+2;}
    else {
      {output_byte(out,dt_string);}
      {output_4bytes(out,len);}
      {output_bytes(out,s->bytes,len);}
      return len+5;}}
  else return fd_write_dtype(out,key);
}

static int write_zkey(fd_hash_index hx,fd_byte_output out,fdtype key)
{
  int start_len=out->ptr-out->start, slotid_index=-1;
  if (FD_PAIRP(key)) {
    fdtype car=FD_CAR(key);
    if ((FD_OIDP(car)) || (FD_SYMBOLP(car))) {
      slotid_index=get_slotid_index(hx,car);
      if (slotid_index<0) 
	return fd_write_byte(out,0)+fd_write_dtype(out,key);
      else return fd_write_zint(out,slotid_index+1)+
	     fast_write_dtype(out,FD_CDR(key),((out->flags)&(FD_DTYPEV2)));}
    else return fd_write_byte(out,0)+fd_write_dtype(out,key);}
  else return fd_write_byte(out,0)+fd_write_dtype(out,key);
}

FD_FASTOP int fast_read_dtype(fd_byte_input in)
{
  if (nobytes(in,1)) return return_errcode(FD_EOD);
  else {
    int code=*(in->ptr);
    switch (code) {
    case dt_oid:
      if (nobytes(in,9)) return return_errcode(FD_EOD);
      else {
	FD_OID addr; in->ptr++;
	FD_SET_OID_HI(addr,fd_read_4bytes(in));
	FD_SET_OID_LO(addr,fd_read_4bytes(in));
	return fd_make_oid(addr);}
    case dt_string:
      if (nobytes(in,5)) return return_errcode(FD_EOD);
      else {
	int len=fd_get_4bytes(in->ptr+1); in->ptr=in->ptr+5;
	if (nobytes(in,len)) return return_errcode(FD_EOD);
	else {
	  unsigned char *data=u8_pmalloc(p,len+1);
	  memcpy(data,in->ptr,len); data[len]='\0'; in->ptr=in->ptr+len;
	  return fd_init_string(u8_pmalloc(p,sizeof(struct FD_STRING)),
				len,data);}}
  case dt_tiny_string:
    if (nobytes(in,2)) return return_errcode(FD_EOD);
    else {
      int len=fd_get_byte(in->ptr+1); in->ptr=in->ptr+2;
      if (nobytes(in,len)) return return_errcode(FD_EOD);
      else {
	unsigned char *data=u8_pmalloc(p,len+1);
	memcpy(data,in->ptr,len); data[len]='\0'; in->ptr=in->ptr+len;
	return fd_init_string(u8_pmalloc(p,sizeof(struct FD_STRING)),
			      len,data);}}
    case dt_symbol:
      if (nobytes(in,5)) return return_errcode(FD_EOD);
      else {
	int len=fd_get_4bytes(in->ptr+1); in->ptr=in->ptr+5;
	if (nobytes(in,len)) return return_errcode(FD_EOD);
	else {
	  unsigned char _buf[256], *buf=NULL; fdtype symbol;
	  if (len<256) buf=_buf; else buf=u8_malloc(len+1);
	  memcpy(buf,in->ptr,len); buf[len]='\0'; in->ptr=in->ptr+len;
	  symbol=fd_make_symbol(buf,len);
	  if (buf!=_buf) u8_free(buf);
	  return symbol;}}
    case dt_tiny_symbol:
      if (nobytes(in,2)) return return_errcode(FD_EOD);
      else {
	int len=fd_get_byte(in->ptr+1); in->ptr=in->ptr+2;
	if (nobytes(in,len)) return return_errcode(FD_EOD);
	else {
	  unsigned char _buf[256];
	  memcpy(_buf,in->ptr,len); _buf[len]='\0'; in->ptr=in->ptr+len;
	  return fd_make_symbol(_buf,len);}}
    default:
      return fd_read_dtype(in,NULL);
    }}
}

static fdtype read_zkey(fd_hash_index hx,fd_byte_input in)
{
  int code=fd_read_zint(in);
  if (code==0) return fast_read_dtype(in);
  else if ((code-1)<hx->n_slotids) {
    fdtype cdr=fast_read_dtype(in);
    if (FD_ABORTP(cdr)) return cdr;
    else return fd_init_pair(NULL,hx->slotids[code-1],cdr);}
  else return fd_err(CorruptedHashIndex,"read_zkey",NULL,FD_VOID);
}

FD_EXPORT int fd_hashindex_bucket(struct FD_HASH_INDEX *hx,fdtype key,int modulate)
{
  struct FD_BYTE_OUTPUT out; unsigned char buf[1024];
  unsigned int hashval; int dtype_len;
  FD_INIT_FIXED_BYTE_OUTPUT(&out,buf,1024);
  dtype_len=write_zkey(hx,&out,key);
  hashval=hash_bytes(out.start,dtype_len);
  if (modulate) return hashval%(hx->n_buckets);
  else return hashval;
}

/* ZVALUEs */

static fdtype write_zvalue(fd_hash_index hx,fd_byte_output out,fdtype value)
{
  if (FD_OIDP(value)) {
    int base=FD_OID_BASE_ID(value);
    short baseoid_index=hx->ids2baseoids[base];
    if (baseoid_index<0) {
      int bytes_written; fd_write_byte(out,0);
      bytes_written=fd_write_dtype(out,value);
      if (bytes_written<0) return fd_erreify();
      else return bytes_written+1;}
    else {
      int offset=FD_OID_BASE_OFFSET(value), bytes_written;
      bytes_written=fd_write_zint(out,baseoid_index+1);
      if (bytes_written<0) return fd_erreify();
      bytes_written=bytes_written+fd_write_zint(out,offset);
      return bytes_written;}}
  else {
    int bytes_written; fd_write_byte(out,0);
    bytes_written=fd_write_dtype(out,value);
    return bytes_written+1;}
}

static fdtype dtswrite_zvalue(fd_hash_index hx,fd_dtype_stream out,fdtype value)
{
  if (FD_OIDP(value)) {
    int base=FD_OID_BASE_ID(value);
    short baseoid_index=hx->ids2baseoids[base];
    if (baseoid_index<0) {
      int bytes_written; fd_dtswrite_byte(out,0);
      bytes_written=fd_dtswrite_dtype(out,value);
      if (bytes_written<0) return fd_erreify();
      else return bytes_written+1;}
    else {
      int offset=FD_OID_BASE_OFFSET(value), bytes_written;
      bytes_written=fd_dtswrite_zint(out,baseoid_index+1);
      if (bytes_written<0) return fd_erreify();
      bytes_written=bytes_written+fd_dtswrite_zint(out,offset);
      return bytes_written;}}
  else {
    int bytes_written; fd_dtswrite_byte(out,0);
    bytes_written=fd_dtswrite_dtype(out,value);
    return bytes_written+1;}
}

static fdtype read_zvalue(fd_hash_index hx,fd_byte_input in)
{
  int prefix=fd_read_zint(in);
  if (prefix==0) return fd_read_dtype(in,NULL);
  else {
    unsigned int base=hx->baseoid_ids[prefix-1];
    unsigned int offset=fd_read_zint(in);
    return FD_CONSTRUCT_OID(base,offset);}
}

/* Fetching */

static fdtype hashindex_fetch(fd_index ix,fdtype key)
{
  struct FD_HASH_INDEX *hx=(fd_hash_index)ix;
  struct FD_BYTE_OUTPUT out; unsigned char buf[64];
  struct FD_BYTE_INPUT keystream;
  unsigned char _inbuf[512], *inbuf=NULL;
  unsigned int hashval, bucket, n_keys, i, dtype_len, n_values;
  off_t vblock_off; size_t vblock_size;
  FD_BLOCK_REF keyblock;
  FD_INIT_FIXED_BYTE_OUTPUT(&out,buf,64);
  dtype_len=write_zkey(hx,&out,key);
  hashval=hash_bytes(out.start,dtype_len);
  bucket=hashval%(hx->n_buckets);
  keyblock=get_block_ref(hx,bucket);
  if (keyblock.size==0) return FD_EMPTY_CHOICE;
  if (keyblock.size<512)
    open_block(&keystream,hx,keyblock.off,keyblock.size,_inbuf);
  else {
    inbuf=u8_malloc(keyblock.size);
    open_block(&keystream,hx,keyblock.off,keyblock.size,inbuf);}
  n_keys=fd_read_zint(&keystream);
  i=0; while (i<n_keys) {
    int key_len=fd_read_zint(&keystream);
    if ((key_len==dtype_len) &&
	(memcmp(keystream.ptr,out.start,dtype_len)==0)) {
      keystream.ptr=keystream.ptr+key_len;
      n_values=fd_read_zint(&keystream);
      if (n_values==0) {
	if (inbuf) u8_free(inbuf);
	return FD_EMPTY_CHOICE;}
      else if (n_values==1) {
	if (inbuf) u8_free(inbuf);
	return read_zvalue(hx,&keystream);}
      else {
	vblock_off=(off_t)fd_read_4bytes(&keystream);
	vblock_size=(size_t)fd_read_zint(&keystream);
	if (inbuf) u8_free(inbuf);
	return read_zvalues(hx,n_values,vblock_off,vblock_size);}}
    else {
      keystream.ptr=keystream.ptr+key_len;
      n_values=fd_read_zint(&keystream);
      if (n_values==0) {}
      else if (n_values==1) {
	int code=fd_read_zint(&keystream);
	if (code==0) {
	  fdtype val=fd_read_dtype(&keystream,NULL);
	  fd_decref(val);}
	else keystream.ptr=keystream.ptr+3;}
      else {
	fd_read_4bytes(&keystream);
	fd_read_zint(&keystream);}}
    i++;}
  if (inbuf) u8_free(inbuf);
  return FD_EMPTY_CHOICE;
}

static fdtype read_zvalues
  (fd_hash_index hx,int n_values,off_t vblock_off,size_t vblock_size)
{
  struct FD_CHOICE *result=fd_alloc_choice(n_values);
  fdtype *values=(fdtype *)FD_XCHOICE_DATA(result), *scan=values;
  unsigned char _vbuf[1024], *vbuf=NULL, vbuf_size=-1;
  struct FD_BYTE_INPUT instream;
  int atomicp=1;
  while (vblock_off != 0) {
    int n_elts, i;
    if (vblock_size<1024)
      open_block(&instream,hx,vblock_off,vblock_size,_vbuf);
    else if (vbuf_size>vblock_size)
      open_block(&instream,hx,vblock_off,vblock_size,vbuf);
    else {
      if (vbuf) vbuf=u8_realloc(vbuf,vblock_size);
      else vbuf=u8_malloc(vblock_size);
      open_block(&instream,hx,vblock_off,vblock_size,vbuf);}
    n_elts=fd_read_zint(&instream);
    i=0; while (i<n_elts) {
      fdtype val=read_zvalue(hx,&instream);
      if (FD_CONSP(val)) atomicp=0;
      *scan++=val; i++;}
    /* For vblock continuation pointers, we make the size be first,
       so that we don't need to store an offset if it's zero. */
    vblock_size=fd_read_zint(&instream);
    if (vblock_size) vblock_off=fd_read_4bytes(&instream);
    else vblock_off=0;}
  if (vbuf) u8_free(vbuf);
  return fd_init_choice(result,n_values,NULL,
			FD_CHOICE_DOSORT|
			((atomicp)?(FD_CHOICE_ISATOMIC):
			 (FD_CHOICE_ISCONSES))|
			FD_CHOICE_REALLOC);
}

static int hashindex_fetchsize(fd_index ix,fdtype key)
{
  struct FD_HASH_INDEX *hx=(fd_hash_index)ix;
  struct FD_BYTE_OUTPUT out; unsigned char buf[64];
  struct FD_BYTE_INPUT keystream; unsigned char _inbuf[256], *inbuf;
  unsigned int hashval, bucket, n_keys, i, dtype_len, n_values;
  off_t vblock_off; size_t vblock_size;
  FD_BLOCK_REF keyblock;
  FD_INIT_FIXED_BYTE_OUTPUT(&out,buf,64);
  dtype_len=write_zkey(hx,&out,key);
  hashval=hash_bytes(out.start,dtype_len);
  bucket=hashval%(hx->n_buckets);
  keyblock=get_block_ref(hx,bucket);
  if (keyblock.size==0) return 0;
  if (keyblock.size==0) return FD_EMPTY_CHOICE;
  if (keyblock.size<512)
    open_block(&keystream,hx,keyblock.off,keyblock.size,_inbuf);
  else {
    inbuf=u8_malloc(keyblock.size);
    open_block(&keystream,hx,keyblock.off,keyblock.size,inbuf);}
  n_keys=fd_read_zint(&keystream);
  i=0; while (i<n_keys) {
    int key_len=fd_read_zint(&keystream);
    n_values=fd_read_zint(&keystream);
    vblock_off=(off_t)fd_read_zint(&keystream);
    vblock_size=(size_t)fd_read_zint(&keystream);
    if (key_len!=dtype_len) 
      keystream.ptr=keystream.ptr+key_len;
    else if (memcmp(keystream.ptr,out.start,dtype_len)==0)
      return n_values;
    else keystream.ptr=keystream.ptr+key_len;
    i++;}
  return 0;
}


/* Fetching multiple keys */

struct KEY_SCHEDULE {
  int index; fdtype key; unsigned int key_start, size;
  int bucket; FD_BLOCK_REF ref;};
struct VALUE_SCHEDULE {
  int index; fdtype *write; int atomicp; FD_BLOCK_REF ref;};

static int sort_ks_by_bucket(const void *k1,const void *k2)
{
  struct KEY_SCHEDULE *ks1=(struct KEY_SCHEDULE *)k1;
  struct KEY_SCHEDULE *ks2=(struct KEY_SCHEDULE *)k2;
  if (ks1->bucket<ks2->bucket) return -1;
  else if (ks1->bucket>ks2->bucket) return 1;
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
  struct KEY_SCHEDULE *vs1=(struct KEY_SCHEDULE *)v1;
  struct KEY_SCHEDULE *vs2=(struct KEY_SCHEDULE *)v2;
  if (vs1->ref.off<vs2->ref.off) return -1;
  else if (vs1->ref.off>vs2->ref.off) return 1;
  else return 0;
}

static fdtype *fetchn(struct FD_HASH_INDEX *hx,int n,fdtype *keys)
{
  fdtype *values=u8_malloc(sizeof(fdtype)*n);
  struct FD_BYTE_OUTPUT out;
  struct KEY_SCHEDULE *schedule=u8_malloc(sizeof(struct KEY_SCHEDULE)*n);
  struct VALUE_SCHEDULE *vsched=u8_malloc(sizeof(struct VALUE_SCHEDULE)*n);
  int i=0, n_entries=0, vsched_size=0;
  FD_INIT_BYTE_OUTPUT(&out,n*16,NULL);
  while (i<n) {
    fdtype key=keys[i];
    int dt_start=out.ptr-out.start, dt_size, bucket;
    schedule[n_entries].index=i; schedule[n_entries].key=key;
    schedule[n_entries].key_start=dt_start;
    write_zkey(hx,&out,key);
    dt_size=(out.ptr-out.start)-dt_start;
    schedule[n_entries].size=dt_size;
    schedule[n_entries].bucket=bucket=
      hash_bytes(out.start+dt_start,dt_size)%(hx->n_buckets);
    if (hx->buckets) {
      schedule[i].ref=get_block_ref(hx,bucket);
      if (schedule[n_entries].ref.size==0) {
	/* It is empty, so we don't even need to handle this entry. */
	values[i]=FD_EMPTY_CHOICE;
	/* We don't need to keep its dtype representation around either,
	   so we reset the key stream. */
	out.ptr=out.start+dt_start;}
      else n_entries++;}
    else n_entries++;
    i++;}
  if (hx->buckets==NULL) {
    int write_at=0;
    qsort(schedule,n_entries,sizeof(struct KEY_SCHEDULE),
	  sort_ks_by_bucket);
    i=0; while (i<n_entries) {
      schedule[i].ref=get_block_ref(hx,schedule[i].bucket);
      if (schedule[i].ref.size==0) {
	values[schedule[i].index]=FD_EMPTY_CHOICE; i++;}
      else if (write_at==i) {write_at++; i++;}
      else {schedule[write_at++]=schedule[i++];}}
    n_entries=write_at;}
  qsort(schedule,n_entries,sizeof(struct KEY_SCHEDULE),
	sort_ks_by_refoff);
  {
    struct FD_BYTE_INPUT keyblock;
    unsigned char _buf[1024], *buf=NULL;
    int bucket=-1, j=0, bufsiz=0, key_found=0;
    while (j<n_entries) {
      int k=0, n_keys, found=0;
      fd_off_t blockpos=schedule[j].ref.off;
      fd_size_t blocksize=schedule[j].ref.size;
      if (schedule[j].bucket!=bucket) {
	if (blocksize<1024)
	  open_block(&keyblock,hx,blockpos,blocksize,_buf);
	else if (buf)
	  if (blocksize<bufsiz)
	    open_block(&keyblock,hx,blockpos,blocksize,buf);
	  else {
	    buf=u8_realloc(buf,blocksize); bufsiz=blocksize;
	    open_block(&keyblock,hx,blockpos,blocksize,buf);}
	else {
	  buf=u8_malloc(blocksize); bufsiz=blocksize;
	  open_block(&keyblock,hx,blockpos,blocksize,buf);}}
      else keyblock.ptr=keyblock.start;
      n_keys=fd_read_zint(&keyblock);
      while (k<n_keys) {
	int n_vals, vsize;
	fd_size_t dtsize=fd_read_zint(&keyblock);
	if ((dtsize==schedule[j].size) &&
	    (memcmp(out.start+schedule[j].key_start,keyblock.ptr,dtsize)==0)) {
	  keyblock.ptr=keyblock.ptr+dtsize; key_found=1;
	  n_vals=fd_read_zint(&keyblock);
	  if (n_vals==0) 
	    values[schedule[j].index]=FD_EMPTY_CHOICE;
	  else if (n_vals==1)
	    values[schedule[j].index]=read_zvalue(hx,&keyblock);
	  else {
	    fd_off_t block_off=fd_read_4bytes(&keyblock);
	    fd_size_t block_size=fd_read_zint(&keyblock);
	    struct FD_CHOICE *result=fd_alloc_choice(n_vals);
	    FD_SET_CONS_TYPE(result,fd_choice_type);
	    result->size=n_vals;
	    values[schedule[j].index]=(fdtype)result;
	    vsched[vsched_size].index=schedule[j].index;
	    vsched[vsched_size].ref.off=block_off;
	    vsched[vsched_size].ref.size=block_size;
	    vsched[vsched_size].write=(fdtype *)FD_XCHOICE_DATA(result);
	    vsched[vsched_size].atomicp=1;
	    vsched_size++;
	    break;}}
	else {
	  keyblock.ptr=keyblock.ptr+dtsize;
	  n_vals=fd_read_zint(&keyblock);
	  if (n_vals==0) {}
	  else if (n_vals==1) {
	    fdtype v=read_zvalue(hx,&keyblock);
	    fd_decref(v);}
	  else {
	    /* Skip offset information */
	    fd_read_4bytes(&keyblock);
	    fd_read_zint(&keyblock);}}
	k++;}
      if (!(key_found)) 
	values[schedule[j].index]=FD_EMPTY_CHOICE;
      j++;}
    if (buf) u8_free(buf);}
  u8_free(schedule);
  {
    struct FD_BYTE_INPUT vblock;
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
	  open_block(&vblock,hx,vsched[i].ref.off,vsched[i].ref.size,vbuf);}
	else open_block(&vblock,hx,vsched[i].ref.off,vsched[i].ref.size,_vbuf);
	n_vals=fd_read_zint(&vblock);
	while (j<n_vals) {
	  fdtype v=read_zvalue(hx,&vblock);
	  if (FD_CONSP(v)) vsched[i].atomicp=0;
	  *(vsched[i].write++)=v; j++;}
	next_size=fd_read_zint(&vblock);
	if (next_size) {
	  vsched[i].ref.size=next_size;
	  vsched[i].ref.off=fd_read_4bytes(&vblock);}
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
	    int n_values=result->size;
	    fdtype realv=fd_init_choice(result,n_values,NULL,
					FD_CHOICE_DOSORT|
					((atomicp)?(FD_CHOICE_ISATOMIC):
					 (FD_CHOICE_ISCONSES))|
					FD_CHOICE_REALLOC);
	    values[index]=realv;
	    read++;}}}}
    if (vbuf) u8_free(vbuf);}
  u8_free(vsched); 
  return values;
}

static fdtype *hashindex_fetchn(fd_index ix,int n,fdtype *keys)
{
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  fdtype *results;
  fd_lock_mutex(&(hx->lock));
  results=fetchn(hx,n,keys);
  if (results) {
    int i=0;
    fd_lock_mutex(&(hx->adds.lock));
    while (i<n) {
      fdtype v=fd_hashtable_get_nolock(&(hx->adds),keys[i],FD_EMPTY_CHOICE);
      if (FD_ABORTP(v)) {
	int j=0; while (j<n) { fd_decref(results[j]); j++;}
	u8_free(results);
	fd_unlock_mutex(&(hx->lock));
	fd_unlock_mutex(&(hx->adds.lock));
	fd_interr(v);
	return NULL;}
      else if (FD_EMPTY_CHOICEP(v)) i++;
      else {
	FD_ADD_TO_CHOICE(results[i],v); i++;}}
    fd_unlock_mutex(&(hx->adds.lock));}
  fd_unlock_mutex(&(hx->lock));
  return results;
}


/* Getting all keys */

static int sort_blockrefs_by_off(const void *v1,const void *v2)
{
  struct FD_BLOCK_REF *br1=(struct FD_BLOCK_REF *)v1;
  struct FD_BLOCK_REF *br2=(struct FD_BLOCK_REF *)v2;
  if (br1->off<br2->off) return -1;
  else if (br1->off>br2->off) return 1;
  else return 0;
}

static fdtype hashindex_fetchkeys(fd_index ix)
{
  fdtype results=FD_EMPTY_CHOICE;
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  fd_dtype_stream s=&(hx->stream);
  int i=0, n_buckets=(hx->n_buckets), n_to_fetch=0;
  FD_BLOCK_REF *buckets=u8_malloc(sizeof(FD_BLOCK_REF)*n_buckets);
  unsigned char _keybuf[512], *keybuf=NULL; int keybuf_size=-1;
  if (hx->buckets==NULL) {
    fd_setpos(s,256);
    while (i<n_buckets) {
      fd_off_t off; fd_size_t size;
      off=fd_dtsread_4bytes(s);
      size=fd_dtsread_4bytes(s);
      if (size) {
	buckets[n_to_fetch].off=off;
	buckets[n_to_fetch].size=size;
	n_to_fetch++;}
      i++;}}
  else while (i<n_buckets) {
      FD_BLOCK_REF ref=get_block_ref(hx,i);
      if (ref.size) {
	buckets[n_to_fetch].off=ref.off;
	buckets[n_to_fetch].size=ref.size;
	n_to_fetch++;}
      i++;}
  qsort(buckets,n_to_fetch,sizeof(FD_BLOCK_REF),sort_blockrefs_by_off);
  i=0; while (i<n_to_fetch) {
    struct FD_BYTE_INPUT keyblock; int j=0, n_keys;
    if (buckets[i].size<512) 
      open_block(&keyblock,hx,buckets[i].off,buckets[i].size,_keybuf);
    else {
      if (keybuf==NULL) {
	keybuf_size=buckets[i].size;
	keybuf=u8_malloc(keybuf_size);}
      else if (buckets[i].size<keybuf_size) {}
      else {
	keybuf_size=buckets[i].size;
	keybuf=u8_realloc(keybuf,keybuf_size);}
      open_block(&keyblock,hx,buckets[i].off,buckets[i].size,keybuf);}
    n_keys=fd_read_zint(&keyblock);
    while (j<n_keys) {
      fdtype key; int n_vals, size;
      /* Ignore size */
      size=fd_read_zint(&keyblock);
      key=read_zkey(hx,&keyblock);
      n_vals=fd_read_zint(&keyblock);
      FD_ADD_TO_CHOICE(results,key);
      if (n_vals==0) {}
      else if (n_vals==1) {
	int code=fd_read_zint(&keyblock);
	if (code==0) {
	  fdtype val=fd_read_dtype(&keyblock,NULL);
	  fd_decref(val);}
	else fd_read_zint(&keyblock);}
      else {
	fd_read_4bytes(&keyblock);
	fd_read_zint(&keyblock);}
      j++;}
    i++;}
  if (keybuf) u8_free(keybuf);
  if (buckets) u8_free(buckets);
  return fd_simplify_choice(results);
}

static fdtype hashindex_fetchsizes(fd_index ix)
{
  fdtype results=FD_EMPTY_CHOICE;
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  fd_dtype_stream s=&(hx->stream);
  int i=0, n_buckets=(hx->n_buckets), n_to_fetch=0;
  FD_BLOCK_REF *buckets=u8_malloc(sizeof(FD_BLOCK_REF)*n_buckets);
  unsigned char _keybuf[512], *keybuf=NULL; int keybuf_size=-1;
  if (hx->buckets==NULL) {
    fd_setpos(s,256);
    while (i<n_buckets) {
      fd_off_t off; fd_size_t size;
      off=fd_dtsread_4bytes(s);
      size=fd_dtsread_4bytes(s);
      if (size) {
	buckets[n_to_fetch].off=off;
	buckets[n_to_fetch].size=size;
	n_to_fetch++;}
      i++;}}
  else while (i<n_buckets) {
      FD_BLOCK_REF ref=get_block_ref(hx,i);
      if (ref.size) {
	buckets[n_to_fetch].off=ref.off;
	buckets[n_to_fetch].size=ref.size;
	n_to_fetch++;}
      i++;}
  qsort(buckets,n_to_fetch,sizeof(FD_BLOCK_REF),sort_blockrefs_by_off);
  i=0; while (i<n_to_fetch) {
    struct FD_BYTE_INPUT keyblock; int j=0, n_keys;
    if (buckets[i].size<512) 
      open_block(&keyblock,hx,buckets[i].off,buckets[i].size,_keybuf);
    else {
      if (keybuf==NULL) {
	keybuf_size=buckets[i].size;
	keybuf=u8_malloc(keybuf_size);}
      else if (buckets[i].size<keybuf_size) {}
      else {
	keybuf_size=buckets[i].size;
	keybuf=u8_realloc(keybuf,keybuf_size);}
      open_block(&keyblock,hx,buckets[i].off,buckets[i].size,keybuf);}
    n_keys=fd_read_zint(&keyblock);
    while (j<n_keys) {
      fdtype key, key_and_size; int n_vals, size;
      /* Ignore size */
      size=fd_read_zint(&keyblock);
      key=read_zkey(hx,&keyblock);
      n_vals=fd_read_zint(&keyblock);
      key_and_size=fd_init_pair(NULL,key,FD_INT2DTYPE(n_vals));
      FD_ADD_TO_CHOICE(results,key_and_size);
      if (n_vals==0) {}
      else if (n_vals==1) {
	int code=fd_read_zint(&keyblock);
	if (code==0) {
	  fdtype val=fd_read_dtype(&keyblock,NULL);
	  fd_decref(val);}
	else fd_read_zint(&keyblock);}
      else {
	fd_read_4bytes(&keyblock);
	fd_read_zint(&keyblock);}
      j++;}
    i++;}
  if (keybuf) u8_free(keybuf);
  if (buckets) u8_free(buckets);
  return fd_simplify_choice(results);
}


/* Cache setting */

static void hashindex_setcache(fd_index ix,int level)
{
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
#if (HAVE_MMAP)
  if (level == 3) {
    if (hx->mmap) return;
    fd_lock_mutex(&(hx->lock));
    if (hx->mmap) {
      fd_unlock_mutex(&(hx->lock));
      return;}
    fd_unlock_mutex(&(hx->lock));}
#endif
  if (level >= 2)
    if (hx->buckets) return;
    else {
      fd_dtype_stream s=&(hx->stream);
      unsigned int i, n_buckets=hx->n_buckets;
      struct FD_BLOCK_REF *buckets, *newmmap;
      fd_lock_mutex(&(hx->lock));
      if (hx->buckets) {
	fd_unlock_mutex(&(hx->lock));
	return;}
#if HAVE_MMAP
      newmmap=
	mmap(NULL,(n_buckets*sizeof(struct FD_BLOCK_REF))+256,
	     PROT_READ,MMAP_FLAGS,s->fd,0)+256;
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
	u8_warn(u8_strerror(errno),"fileindex_setcache:mmap %s",hx->source);
	hx->buckets=NULL; errno=0;}
      else hx->buckets=buckets=newmmap;
#else
      fd_dts_start_read(s);
      buckets=u8_malloc(sizeof(struct FD_BLOCK_REF)*(hx->n_buckets));
      fd_setpos(s,256);
      i=0; while (i<n_buckets) {
	buckets[i].off=fd_dtsread_4bytes(s);
	buckets[i].size=fd_dtsread_4bytes(s);
	i++;}
      hx->buckets=buckets; 
#endif
      fd_unlock_mutex(&(hx->lock));}
  else if (level < 2)
    if (hx->buckets == NULL) return;
    else {
      int retval;
      fd_lock_mutex(&(hx->lock));
#if HAVE_MMAP
      retval=munmap(hx->buckets-256,(hx->n_buckets*sizeof(FD_BLOCK_REF))+256);
      if (retval<0) {
	u8_warn(u8_strerror(errno),"fileindex_setcache:munmap %s",hx->source);
	hx->buckets=NULL; errno=0;}
#else
      u8_free(hx->buckets);
#endif
      hx->buckets=NULL;
      fd_unlock_mutex(&(hx->lock));}
}


/* Populating a hash index
   This writes data into the hashtable but ignores what is already there.
   It is commonly used when initializing a hash index. */

struct POP_SCHEDULE {
  fdtype key; unsigned int size, bucket;};
struct BUCKET_REF {
  unsigned int bucket; fd_off_t off; fd_size_t size;};

static int sort_ps_by_bucket(const void *p1,const void *p2)
{
  struct POP_SCHEDULE *ps1=(struct POP_SCHEDULE *)p1;
  struct POP_SCHEDULE *ps2=(struct POP_SCHEDULE *)p2;
  if (ps1->bucket<ps2->bucket) return -1;
  else if (ps1->bucket>ps2->bucket) return 1;
  else return 0;
}

static int sort_br_by_bucket(const void *p1,const void *p2)
{
  struct BUCKET_REF *ps1=(struct BUCKET_REF *)p1;
  struct BUCKET_REF *ps2=(struct BUCKET_REF *)p2;
  if (ps1->bucket<ps2->bucket) return -1;
  else if (ps1->bucket>ps2->bucket) return 1;
  else return 0;
}

static int populate_prefetch
   (struct POP_SCHEDULE *psched,fd_index ix,int i,int blocksize,int n_keys)
{
  fdtype prefetch=FD_EMPTY_CHOICE;
  int k=i, lim=k+blocksize, final_bckt;
  u8_message("Swapping out index %s",ix->cid);
  fd_index_swapout(ix);
  if (lim>n_keys) lim=n_keys;
  while (k<lim) {
    fdtype key=psched[k++].key; fd_incref(key);
    FD_ADD_TO_CHOICE(prefetch,key);}
  final_bckt=psched[k-1].bucket; while (k<n_keys)
    if (psched[k].bucket != final_bckt) break;
    else {
      fdtype key=psched[k++].key; fd_incref(key);
      FD_ADD_TO_CHOICE(prefetch,key);}
  u8_message("Prefetching %d keys from %s",lim-i,ix->cid);
  fd_index_prefetch(ix,prefetch);
  fd_decref(prefetch);
  return k;
}

#if FD_DEBUG_HASHINDICES
static int watch_for_bucket=-1;
#endif

FD_EXPORT int fd_populate_hashindex(struct FD_HASH_INDEX *hx,fdtype from,fdtype keys,int blocksize)
{
  int i=0, n_buckets=hx->n_buckets, n_keys=FD_CHOICE_SIZE(keys);
  int filled_buckets=0, bucket_count=0, fetch_max=-1;
  struct POP_SCHEDULE *psched=u8_malloc(n_keys*sizeof(struct POP_SCHEDULE));
  struct FD_BYTE_OUTPUT out; FD_INIT_BYTE_OUTPUT(&out,8192,NULL);
  struct BUCKET_REF *bucket_refs; fd_index ix=NULL;
  fd_dtype_stream stream=&(hx->stream);
  off_t endpos=fd_endpos(stream);
  
  if (FD_INDEXP(from)) ix=fd_lisp2index(from);

  /* Fill the key schedule and count the number of buckets used */
  memset(psched,0,n_keys*sizeof(struct POP_SCHEDULE));
  {
    int cur_bucket=-1;
    FD_DO_CHOICES(key,keys) {
      int bucket, size;
      out.ptr=out.start; /* Reset stream */
      psched[i].key=key;
      write_zkey(hx,&out,key);
      psched[i].size=(out.ptr-out.start);
      bucket=hash_bytes(out.start,psched[i].size)%n_buckets;
      psched[i].bucket=bucket;
      if (bucket!=cur_bucket) {cur_bucket=bucket; filled_buckets++;}
      i++;}
    /* Allocate the bucket_refs */
    bucket_refs=u8_malloc(sizeof(struct BUCKET_REF)*filled_buckets);}

  /* Sort the key schedule by bucket */
  qsort(psched,n_keys,sizeof(struct POP_SCHEDULE),sort_ps_by_bucket);
  
  i=0; while (i<n_keys) {
    struct FD_BYTE_OUTPUT keyblock; int retval;
    unsigned char buf[4096];
    unsigned int bucket=psched[i].bucket, load=0, j=i;
#if FD_DEBUG_HASHINDICES
    if (bucket==watch_for_bucket)
      u8_warn("Event","Hit the bucket %d",watch_for_bucket);
#endif
    while ((j<n_keys) && (psched[j].bucket==bucket)) j++;
    bucket_refs[bucket_count].bucket=bucket;
    load=j-i; FD_INIT_FIXED_BYTE_OUTPUT(&keyblock,buf,4096);
    fd_write_zint(&keyblock,load);
    if ((ix) && (i>=fetch_max))
      fetch_max=populate_prefetch(psched,ix,i,blocksize,n_keys);
    while (i<j) {
      fdtype key=psched[i].key, values=fd_get(from,key,FD_EMPTY_CHOICE);
      fd_write_zint(&keyblock,psched[i].size);
      write_zkey(hx,&keyblock,key);
      fd_write_zint(&keyblock,FD_CHOICE_SIZE(values));
      if (FD_EMPTY_CHOICEP(values)) {}
      else if (FD_CHOICEP(values)) {
	int bytes_written=0;
	CHECK_ENDPOS(endpos,stream);
	fd_write_4bytes(&keyblock,endpos);
	retval=fd_dtswrite_zint(stream,FD_CHOICE_SIZE(values));
	if (retval<0) {
	  if ((keyblock.flags)&(FD_BYTEBUF_MALLOCD))
	    u8_free(keyblock.start);
	  u8_free(out.start);
	  u8_free(psched);
	  u8_free(bucket_refs);
	  return -1;}
	else bytes_written=bytes_written+retval;
	{FD_DO_CHOICES(value,values) {
	  int retval=dtswrite_zvalue(hx,stream,value);
	  if (retval<0) {
	    if ((keyblock.flags)&(FD_BYTEBUF_MALLOCD))
	      u8_free(keyblock.start);
	    u8_free(out.start);
	    u8_free(psched);
	    u8_free(bucket_refs);
	    return -1;}
	  else bytes_written=bytes_written+retval;}}
	/* Write a NULL to indicate no continuation. */
	fd_dtswrite_byte(stream,0); bytes_written++;
	fd_write_zint(&keyblock,bytes_written);
	endpos=endpos+bytes_written;
	CHECK_ENDPOS(endpos,stream);}
      else write_zvalue(hx,&keyblock,values);
      fd_decref(values);
      i++;}
    /* Write the bucket information */
    bucket_refs[bucket_count].off=endpos;
    retval=fd_dtswrite_bytes(stream,keyblock.start,keyblock.ptr-keyblock.start);
    if (retval<0) {
      if ((keyblock.flags)&(FD_BYTEBUF_MALLOCD))
	u8_free(keyblock.start);
      u8_free(out.start);
      u8_free(psched);
      u8_free(bucket_refs);
      return -1;}
    else bucket_refs[bucket_count].size=retval;
    endpos=endpos+retval;
    CHECK_ENDPOS(endpos,stream);
    bucket_count++;}
  qsort(bucket_refs,bucket_count,sizeof(struct BUCKET_REF),sort_br_by_bucket);
  /* This would probably be faster if we put it all in a huge vector and wrote it
     out all at once.  */
  i=0; while (i<bucket_count) {
    fd_setpos(stream,256+bucket_refs[i].bucket*8);
    fd_dtswrite_4bytes(stream,bucket_refs[i].off);
    fd_dtswrite_4bytes(stream,bucket_refs[i].size);
    i++;}
  fd_setpos(stream,16);
  fd_dtswrite_4bytes(stream,n_keys);
  fd_dtsflush(stream);
  u8_free(out.start);
  u8_free(psched);
  u8_free(bucket_refs);
  return bucket_count;
}


/* Miscellaneous methods */

static void hashindex_close(fd_index ix)
{
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  fd_lock_mutex(&(hx->lock));
  fd_dtsclose(&(hx->stream),1);
  if (hx->buckets) {
#if HAVE_MMAP
    int retval=munmap(hx->buckets,(sizeof(FD_BLOCK_REF)*hx->n_buckets));
    if (retval<0) {
      u8_warn(u8_strerror(errno),"fileindex_close:munmap %s",hx->source);
      errno=0;}
#else
    u8_free(hx->buckets); 
#endif
    hx->buckets=NULL;
    hx->cache_level=-1;}
  fd_unlock_mutex(&(hx->lock));
}

static void hashindex_setbuf(fd_index ix,int bufsiz)
{
  struct FD_HASH_INDEX *fx=(struct FD_HASH_INDEX *)ix;
  fd_lock_mutex(&(fx->lock));
  fd_dtsbufsize(&(fx->stream),bufsiz);
  fd_unlock_mutex(&(fx->lock));
}

static fdtype hashindex_metadata(fd_index ix,fdtype md)
{
  struct FD_HASH_INDEX *fx=(struct FD_HASH_INDEX *)ix;
  if (FD_VOIDP(md))
    return fd_read_index_metadata(&(fx->stream));
  else return fd_write_index_metadata((&(fx->stream)),md);
}


/* The handler struct */

static struct FD_INDEX_HANDLER hashindex_handler={
  "hashindex", 1, sizeof(struct FD_HASH_INDEX), 12,
  hashindex_close, /* close */
  NULL, /* commit */
  hashindex_setcache, /* setcache */
  hashindex_setbuf, /* setbuf */
  hashindex_fetch, /* fetch */
  hashindex_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  hashindex_fetchn, /* fetchn */
  hashindex_fetchkeys, /* fetchkeys */
  hashindex_fetchsizes, /* fetchsizes */
  hashindex_metadata, /* metadata */
  NULL /* sync */
};

FD_EXPORT int fd_hashindexp(struct FD_INDEX *ix)
{
  return (ix->handler==&hashindex_handler);
}

FD_EXPORT fd_init_hashindices_c()
{
  fd_register_source_file(versionid);

  fd_register_index_opener(FD_HASH_INDEX_MAGIC_NUMBER,open_hashindex);
}

/* TODO:
 * make baseoids be megapools, use intrinsic tables
 * implement fetchn
 * implement getkeys
 * add memory prefetch recommendations
 * test mmap cache level 3
 * implement dynamic slotid/baseoid addition
 * implement commit
 * implement ACID
 */
 
