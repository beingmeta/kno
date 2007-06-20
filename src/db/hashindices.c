/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

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
static int recover_hash_index(struct FD_HASH_INDEX *hx);

static fd_index open_hash_index(u8_string fname,int read_only)
{
  struct FD_HASH_INDEX *index=u8_malloc(sizeof(struct FD_HASH_INDEX));
  struct FD_DTYPE_STREAM *s=&(index->stream);
  unsigned int magicno, n_buckets, hash_const, n_keys;
  fd_off_t slotids_pos, baseoids_pos, metadata_pos;
  fd_size_t slotids_size, baseoids_size, metadata_size;  
  fd_dtstream_mode mode=
    ((read_only) ? (FD_DTSTREAM_READ) : (FD_DTSTREAM_MODIFY));
  fd_init_index(index,&hash_index_handler,fname);
  if (fd_init_dtype_file_stream(s,fname,mode,FD_FILEDB_BUFSIZE,NULL,NULL)
      == NULL) {
    u8_free(index);
    fd_seterr3(fd_CantOpenFile,"open_file_index",u8_strdup(fname));
    return NULL;}
  /* See if it ended up read only */
  if (index->stream.flags&FD_DTSTREAM_READ_ONLY) read_only=1;
  index->stream.mallocd=0;
  index->mmap=NULL;
  magicno=fd_dtsread_4bytes(s);
  index->n_buckets=fd_dtsread_4bytes(s);
  if (magicno==FD_HASH_INDEX_TO_RECOVER) {
    u8_warn(fd_RecoveryRequired,"Recovering the hash index %s",fname);
    recover_hash_index(index);
    magicno=magicno&(~0x20);}
  index->buckets=NULL; index->read_only=read_only;
  index->hxflags=fd_dtsread_4bytes(s);

  if (((index->hxflags)&(FD_HASH_INDEX_FN_MASK))!=0) {
    u8_free(index);
    fd_seterr3(BadHashFn,"open_hash_index",NULL);
    return NULL;}
  
  index->hxcustom=fd_dtsread_4bytes(s);

  index->n_keys=n_keys=fd_dtsread_4bytes(s); /* Currently ignored */

  slotids_pos=fd_dtsread_4bytes(s); slotids_size=fd_dtsread_4bytes(s);
  baseoids_pos=fd_dtsread_4bytes(s); baseoids_size=fd_dtsread_4bytes(s);
  metadata_pos=fd_dtsread_4bytes(s); metadata_size=fd_dtsread_4bytes(s);

  /* Initialize the slotids field used for storing feature keys */
  if (slotids_size) {
    fdtype slotids_vector=read_dtype_at_pos(s,slotids_pos);
    if (FD_VOIDP(slotids_vector)) {
      index->n_slotids=0; index->new_slotids=0;
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
  else {
    index->n_slotids=0; index->new_slotids=0;
    index->slotids=NULL;
    index->slotid_lookup=NULL;}

  /* Initialize the baseoids field used for compressed OID values */
  if (baseoids_size) {
    fdtype baseoids_vector=read_dtype_at_pos(s,baseoids_pos);
    if (FD_VOIDP(baseoids_vector)) {
      index->n_baseoids=0; index->new_baseoids=0;
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
  else {
    index->n_baseoids=0; index->new_baseoids=0;
    index->baseoid_ids=NULL;
    index->ids2baseoids=NULL;}

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
  struct FD_SLOTID_LOOKUP *lookup; int i=0;
  fdtype *slotids, slotids_choice=FD_EMPTY_CHOICE;
  hx->slotids=slotids=u8_malloc(sizeof(fdtype)*n_slotids);
  hx->slotid_lookup=lookup=
    u8_malloc(sizeof(FD_SLOTID_LOOKUP)*n_slotids);
  hx->n_slotids=n_slotids; hx->new_slotids=0;
  if ((hx->hxflags)&(FD_HASH_INDEX_ODDKEYS)) slotids_choice=FD_VOID;
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
    hx->has_slotids=fd_simplify_choice(slotids_choice);
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
  hx->n_baseoids=n_baseoids; hx->new_baseoids;
  hx->baseoid_ids=baseoid_ids;
  hx->ids2baseoids=ids2baseoids;
  i=0; while (i<n_baseoids) {
    fdtype baseoid=baseoids_init[i];
    baseoid_ids[i]=FD_OID_BASE_ID(baseoid);
    ids2baseoids[FD_OID_BASE_ID(baseoid)]=i;
    i++;}
}

/* Making a hash index */

FD_EXPORT int fd_make_hash_index(u8_string fname,int n_buckets_arg,
				 unsigned int flags,unsigned int hashconst,
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
  fd_dtswrite_4bytes(stream,flags); /* No hxflags now. */
  fd_dtswrite_4bytes(stream,hashconst); /* No custom hash constant */
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

static int fast_write_dtype(fd_byte_output out,fdtype key,int v2)
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

FD_FASTOP int write_zkey(fd_hash_index hx,fd_byte_output out,fdtype key)
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

static int fast_read_dtype(fd_byte_input in)
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

FD_FASTOP fdtype read_zkey(fd_hash_index hx,fd_byte_input in)
{
  int code=fd_read_zint(in);
  if (code==0) return fast_read_dtype(in);
  else if ((code-1)<hx->n_slotids) {
    fdtype cdr=fast_read_dtype(in);
    if (FD_ABORTP(cdr)) return cdr;
    else return fd_init_pair(NULL,hx->slotids[code-1],cdr);}
  else return fd_err(CorruptedHashIndex,"read_zkey",NULL,FD_VOID);
}

FD_EXPORT int fd_hash_index_bucket(struct FD_HASH_INDEX *hx,fdtype key,int modulate)
{
  struct FD_BYTE_OUTPUT out; unsigned char buf[1024];
  unsigned int hashval; int dtype_len;
  FD_INIT_FIXED_BYTE_OUTPUT(&out,buf,1024);
  if ((hx->hxflags)&(FD_HASH_INDEX_DTYPEV2))
    out.flags=out.flags|FD_DTYPEV2;
  dtype_len=write_zkey(hx,&out,key);
  hashval=hash_bytes(out.start,dtype_len);
  if (modulate) return hashval%(hx->n_buckets);
  else return hashval;
}

/* ZVALUEs */

FD_FASTOP fdtype write_zvalue(fd_hash_index hx,fd_byte_output out,fdtype value)
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

FD_FASTOP fdtype dtswrite_zvalue(fd_hash_index hx,fd_dtype_stream out,fdtype value)
{
  if (FD_OIDP(value)) {
    int base=FD_OID_BASE_ID(value);
    short baseoid_index=((hx->ids2baseoids) ? (hx->ids2baseoids[base]) : (-1));
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

FD_FASTOP fdtype read_zvalue(fd_hash_index hx,fd_byte_input in)
{
  int prefix=fd_read_zint(in);
  if (prefix==0) return fd_read_dtype(in,NULL);
  else {
    unsigned int base=hx->baseoid_ids[prefix-1];
    unsigned int offset=fd_read_zint(in);
    return FD_CONSTRUCT_OID(base,offset);}
}

/* Fetching */

static fdtype hash_index_fetch(fd_index ix,fdtype key)
{
  struct FD_HASH_INDEX *hx=(fd_hash_index)ix;
  struct FD_BYTE_OUTPUT out; unsigned char buf[64];
  struct FD_BYTE_INPUT keystream;
  unsigned char _inbuf[512], *inbuf=NULL;
  unsigned int hashval, bucket, n_keys, i, dtype_len, n_values;
  off_t vblock_off; size_t vblock_size;
  FD_BLOCK_REF keyblock;
  FD_INIT_FIXED_BYTE_OUTPUT(&out,buf,64);
#if FD_DEBUG_HASHINDICES
  /* u8_message("Fetching the key %q from %s",key,hx->cid); */
#endif
  /* If the index doesn't have oddkeys and you're looking up some feature (pair)
     whose slotid isn't in the slotids, the key isn't in the table. */
  if ((!((hx->hxflags)&(FD_HASH_INDEX_ODDKEYS))) && (FD_PAIRP(key))) {
    fdtype slotid=FD_CAR(key);
    if (((FD_SYMBOLP(slotid)) || (FD_SYMBOLP(slotid))) &&
	(get_slotid_index(hx,slotid)<0)) {
#if FD_DEBUG_HASHINDICES
      u8_message("The slotid %q isn't indexed in %s, returning {}",slotid,hx->cid);
#endif
      return FD_EMPTY_CHOICE;}}
  if ((hx->hxflags)&(FD_HASH_INDEX_DTYPEV2))
    out.flags=out.flags|FD_DTYPEV2;
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
	else fd_read_zint(&keystream);}
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

static int hash_index_fetchsize(fd_index ix,fdtype key)
{
  struct FD_HASH_INDEX *hx=(fd_hash_index)ix;
  struct FD_BYTE_OUTPUT out; unsigned char buf[64];
  struct FD_BYTE_INPUT keystream; unsigned char _inbuf[256], *inbuf;
  unsigned int hashval, bucket, n_keys, i, dtype_len, n_values;
  off_t vblock_off; size_t vblock_size;
  FD_BLOCK_REF keyblock;
  FD_INIT_FIXED_BYTE_OUTPUT(&out,buf,64);
  if ((hx->hxflags)&(FD_HASH_INDEX_DTYPEV2))
    out.flags=out.flags|FD_DTYPEV2;
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
  int i=0, n_entries=0, vsched_size=0, oddkeys=((hx->hxflags)&(FD_HASH_INDEX_ODDKEYS));
#if FD_DEBUG_HASHINDICES
  u8_message("Reading %d keys from %s",n,hx->cid);
#endif
  FD_INIT_BYTE_OUTPUT(&out,n*16,NULL);
  if ((hx->hxflags)&(FD_HASH_INDEX_DTYPEV2))
    out.flags=out.flags|FD_DTYPEV2;
  while (i<n) {
    fdtype key=keys[i];
    int dt_start=out.ptr-out.start, dt_size, bucket;
   /* If the index doesn't have oddkeys and you're looking up some feature (pair)
     whose slotid isn't in the slotids, the key isn't in the table. */
    if ((!oddkeys) && (FD_PAIRP(key))) {
      fdtype slotid=FD_CAR(key);
      if (((FD_SYMBOLP(slotid)) || (FD_SYMBOLP(slotid))) &&
	  (get_slotid_index(hx,slotid)<0)) {
	values[i++]=FD_EMPTY_CHOICE;
	continue;}}
    schedule[n_entries].index=i; schedule[n_entries].key=key;
    schedule[n_entries].key_start=dt_start;
    write_zkey(hx,&out,key);
    dt_size=(out.ptr-out.start)-dt_start;
    schedule[n_entries].size=dt_size;
    schedule[n_entries].bucket=bucket=
      hash_bytes(out.start+dt_start,dt_size)%(hx->n_buckets);
    if (hx->buckets) {
      schedule[n_entries].ref=get_block_ref(hx,bucket);
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
    int bucket=-1, j=0, bufsiz=0;
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
	  keyblock.ptr=keyblock.ptr+dtsize; found=1;
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
	    vsched_size++;}
	  /* This breaks out the loop iterating over the keys in this bucket. */
	  break;}
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
      if (!(found)) 
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
#if FD_DEBUG_HASHINDICES
  u8_message("Finished reading %d keys from %s",n,hx->cid);
#endif
  return values;
}

static fdtype *hash_index_fetchn_inner(fd_index ix,int n,fdtype *keys,int lock)
{
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  fdtype *results;
  if (lock) fd_lock_mutex(&(hx->lock));
  results=fetchn(hx,n,keys);
  if (results) {
    int i=0;
    if (lock) fd_lock_mutex(&(hx->adds.lock));
    while (i<n) {
      fdtype v=fd_hashtable_get_nolock(&(hx->adds),keys[i],FD_EMPTY_CHOICE);
      if (FD_ABORTP(v)) {
	int j=0; while (j<n) { fd_decref(results[j]); j++;}
	u8_free(results);
	if (lock) fd_unlock_mutex(&(hx->lock));
	if (lock) fd_unlock_mutex(&(hx->adds.lock));
	fd_interr(v);
	return NULL;}
      else if (FD_EMPTY_CHOICEP(v)) i++;
      else {
	FD_ADD_TO_CHOICE(results[i],v); i++;}}
    if (lock) fd_unlock_mutex(&(hx->adds.lock));}
  if (lock) fd_unlock_mutex(&(hx->lock));
  return results;
}

static fdtype *hash_index_fetchn(fd_index ix,int n,fdtype *keys)
{
  return hash_index_fetchn_inner(ix,n,keys,1);
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

static fdtype *hash_index_fetchkeys(fd_index ix,int *n)
{
  fdtype *results=NULL;
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  fd_dtype_stream s=&(hx->stream);
  int i=0, n_buckets=(hx->n_buckets), n_to_fetch=0, total_keys=0, key_count=0;
  FD_BLOCK_REF *buckets=u8_malloc(sizeof(FD_BLOCK_REF)*n_buckets);
  unsigned char _keybuf[512], *keybuf=NULL; int keybuf_size=-1;
  fd_setpos(s,16); total_keys=fd_dtsread_4bytes(s);
  results=u8_malloc(sizeof(fdtype)*total_keys);
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
      results[key_count++]=key;
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
  *n=total_keys;
  return results;
}

static struct FD_KEY_SIZE *hash_index_fetchsizes(fd_index ix,int *n)
{
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  fd_dtype_stream s=(fd_lock_mutex(&(hx->lock)),&(hx->stream));
  int i=0, j=0, n_buckets=(hx->n_buckets), n_to_fetch=0, key_count=0, keybuf_size=-1;
  FD_BLOCK_REF *buckets=u8_malloc(sizeof(FD_BLOCK_REF)*n_buckets);
  unsigned char _keybuf[512], *keybuf=NULL; 
  struct FD_KEY_SIZE *sizes=u8_malloc(sizeof(FD_KEY_SIZE)*(hx->n_keys));
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
      assert(key!=0);
      sizes[key_count].key=key; sizes[key_count].n_values=n_vals;
      key_count++;
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
  assert(key_count==hx->n_keys);
  *n=hx->n_keys;
  if (keybuf) u8_free(keybuf);
  if (buckets) u8_free(buckets);
  fd_unlock_mutex(&(hx->lock));
  return sizes;
}


/* Cache setting */

static void hash_index_setcache(fd_index ix,int level)
{
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
#if (HAVE_MMAP)
  if (level == 3) {
    if (hx->mmap) return;
    fd_lock_mutex(&(hx->lock));
    if (hx->mmap) {
      fd_unlock_mutex(&(hx->lock));
      return;}
    hx->mmap_size=u8_file_size(hx->cid);
    hx->mmap=
      mmap(NULL,hx->mmap_size,PROT_READ,MMAP_FLAGS,hx->stream.fd,0);
    if (hx->mmap==NULL) {
      u8_warn(u8_strerror(errno),"hash_index_setcache:mmap %s",hx->source);
      hx->mmap=NULL; errno=0;}
    fd_unlock_mutex(&(hx->lock));}
  if ((level<3) && (hx->mmap)) {
    int retval;
    fd_lock_mutex(&(hx->lock));
    retval=munmap(hx->mmap,hx->mmap_size);
    if (retval<0) {
      u8_warn(u8_strerror(errno),"hash_index_setcache:munmap %s",hx->source);
      hx->mmap=NULL; errno=0;}
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
	u8_warn(u8_strerror(errno),"file_index_setcache:mmap %s",hx->source);
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
      retval=munmap((hx->buckets)-256,((hx->n_buckets)*sizeof(FD_BLOCK_REF))+256);
      if (retval<0) {
	u8_warn(u8_strerror(errno),"file_index_setcache:munmap %s",hx->source);
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
  /* max_new is only used when commiting. */
  unsigned int bucket, max_new; FD_BLOCK_REF ref;};

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

static int sort_br_by_off(const void *p1,const void *p2)
{
  struct BUCKET_REF *ps1=(struct BUCKET_REF *)p1;
  struct BUCKET_REF *ps2=(struct BUCKET_REF *)p2;
  if (ps1->ref.off<ps2->ref.off) return -1;
  else if (ps1->ref.off>ps2->ref.off) return 1;
  else return 0;
}

static int populate_prefetch
   (struct POP_SCHEDULE *psched,fd_index ix,int i,int blocksize,int n_keys)
{
  fdtype prefetch=FD_EMPTY_CHOICE;
  int k=i, lim=k+blocksize, final_bckt;
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
  fd_index_prefetch(ix,prefetch);
  fd_decref(prefetch);
  return k;
}

#if FD_DEBUG_HASHINDICES
static int watch_for_bucket=-1;
#endif

FD_EXPORT int fd_populate_hash_index
  (struct FD_HASH_INDEX *hx,fdtype from,
   const fdtype *keys,int n_keys, int blocksize)
{
  /* This overwrites all the data in *hx* with the key/value mappings in the table *from*
     for the keys *keys*. */
  int i=0, n_buckets=hx->n_buckets;
  int filled_buckets=0, bucket_count=0, fetch_max=-1;
  int cycle_buckets=0, cycle_keys=0, cycle_max=0;
  int overall_buckets=0, overall_keys=0, overall_max=0;
  struct POP_SCHEDULE *psched=u8_malloc(n_keys*sizeof(struct POP_SCHEDULE));
  struct FD_BYTE_OUTPUT out; FD_INIT_BYTE_OUTPUT(&out,8192,NULL);
  struct BUCKET_REF *bucket_refs; fd_index ix=NULL;
  fd_dtype_stream stream=&(hx->stream);
  off_t endpos=fd_endpos(stream);
  double start_time=u8_elapsed_time();
  
  if (psched==NULL) {
    u8_free(out.start);
    fd_seterr(fd_MallocFailed,"populuate_hash_index",NULL,FD_VOID);
    return -1;}

  if (FD_INDEXP(from)) ix=fd_lisp2index(from);

  /* Population doesn't leave any odd keys */
  if ((hx->hxflags)&(FD_HASH_INDEX_ODDKEYS)) 
    hx->hxflags=hx->hxflags&(~(FD_HASH_INDEX_ODDKEYS));
  
  if ((hx->hxflags)&(FD_HASH_INDEX_DTYPEV2))
    out.flags=out.flags|FD_DTYPEV2;

  /* Fill the key schedule and count the number of buckets used */
  memset(psched,0,n_keys*sizeof(struct POP_SCHEDULE));
  {
    int cur_bucket=-1;
    const fdtype *keyscan=keys, *keylim=keys+n_keys;
    while (keyscan<keylim) {
      fdtype key=*keyscan++;
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
    cycle_buckets++; cycle_keys=cycle_keys+(j-i);
    if ((j-i)>cycle_max) cycle_max=j-i;
    bucket_refs[bucket_count].bucket=bucket;
    load=j-i; FD_INIT_FIXED_BYTE_OUTPUT(&keyblock,buf,4096);

    if ((hx->hxflags)&(FD_HASH_INDEX_DTYPEV2))
      keyblock.flags=keyblock.flags|FD_DTYPEV2;
    
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
		   i,n_keys,percent,ix->cid,elapsed,togo,total);}
      if (i>0)
	u8_message("Overall, distributed %d keys over %d buckets, averaging %.2f keys per bucket (%d keys max)",
		   overall_keys,overall_buckets,
		   ((1.0*overall_keys)/(1.0*overall_buckets)),
		   overall_max);
      fetch_max=populate_prefetch(psched,ix,i,blocksize,n_keys);
      u8_message("Prefetched %d keys from %s in %.3f seconds",
		 fetch_max-i,ix->cid,u8_elapsed_time()-fetch_start);}

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
    bucket_refs[bucket_count].ref.off=endpos;
    retval=fd_dtswrite_bytes(stream,keyblock.start,keyblock.ptr-keyblock.start);
    if (retval<0) {
      if ((keyblock.flags)&(FD_BYTEBUF_MALLOCD))
	u8_free(keyblock.start);
      u8_free(out.start);
      u8_free(psched);
      u8_free(bucket_refs);
      return -1;}
    else bucket_refs[bucket_count].ref.size=retval;
    endpos=endpos+retval;
    CHECK_ENDPOS(endpos,stream);
    bucket_count++;}
  qsort(bucket_refs,bucket_count,sizeof(struct BUCKET_REF),sort_br_by_bucket);
  /* This would probably be faster if we put it all in a huge vector and wrote it
     out all at once.  */
  i=0; while (i<bucket_count) {
    fd_setpos(stream,256+bucket_refs[i].bucket*8);
    fd_dtswrite_4bytes(stream,bucket_refs[i].ref.off);
    fd_dtswrite_4bytes(stream,bucket_refs[i].ref.size);
    i++;}
  fd_setpos(stream,16);
  fd_dtswrite_4bytes(stream,n_keys);
  fd_dtsflush(stream);
  u8_free(out.start);
  u8_free(psched);
  u8_free(bucket_refs);
  return bucket_count;
}


/* COMMIT */

/* General design:
   Handle edits first, converting drops into sets and adding them to the commit schedule.
   Then handle adds, pushing them onto the commit schedule.
   Fill in the block refs for all the entries in the commit schedule.
   Sort the commit schedule by ref and read data for some number of buckets,
   parsing each bucket data into a KEYBUCKET structure made up of KEYENTRY structs.
   Add buckref values to each commit entry for the retrieved keybucket it is in.
   Iterate over the commit schedule, updating the keybucket structure for each key,
   potentially writing value blocks to disk.
   Then iterate over the keybucket structure, outputting keyblocks and updating
   a global bucket/blockrefs mapping.

   On a first pass implementation, don't process it in chunks.  The disadvantage
   is that you'll have lots of keyblocks in memory, but we can deal with that for now.

*/

struct COMMIT_SCHEDULE {
  fdtype key, values; short replace; int bucket;};

struct KEYENTRY {
  int dtype_size, n_values; 
  /* Make this into an int relative to the parent keybucket's keybuf */
  unsigned char *dtype_start;
  fdtype values; FD_BLOCK_REF vref;};

struct KEYBUCKET {
  int bucket, n_keys; unsigned char *keybuf; struct KEYENTRY elt0;};

static int sort_cs_by_bucket(const void *k1,const void *k2)
{
  struct COMMIT_SCHEDULE *ks1=(struct COMMIT_SCHEDULE *)k1;
  struct COMMIT_SCHEDULE *ks2=(struct COMMIT_SCHEDULE *)k2;
  if (ks1->bucket<ks2->bucket) return -1;
  else if (ks1->bucket>ks2->bucket) return 1;
  else return 0;
}

static int sort_kb_by_bucket(const void *k1,const void *k2)
{
  struct KEYBUCKET **kb1=(struct KEYBUCKET **)k1;
  struct KEYBUCKET **kb2=(struct KEYBUCKET **)k2;
  if ((*kb1)->bucket<(*kb2)->bucket) return -1;
  else if ((*kb1)->bucket>(*kb2)->bucket) return 1;
  else return 0;
}

static int process_edits(struct FD_HASH_INDEX *hx,fd_hashset taken,
			 struct COMMIT_SCHEDULE *s,int i)
{
  fd_hashtable adds=&(hx->adds), edits=&(hx->edits), cache=&(hx->cache);
  fdtype *drops=u8_malloc(sizeof(fdtype)*(edits->n_keys)), *drop_values;
  int j=0, n_drops=0, oddkeys=0;
  struct FD_HASHENTRY **scan=edits->slots, **lim=scan+edits->n_slots;
  while (scan < lim)
    if (*scan) {
      struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
      struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
      while (kvscan<kvlimit) {
	fdtype key=kvscan->key;
	if (FD_PAIRP(key))
	  if ((FD_CAR(key))==set_symbol) {
	    fdtype real_key=FD_CDR(key);
	    fd_hashset_add(taken,real_key);
	    if ((oddkeys==0) && (FD_PAIRP(real_key)) &&
		((FD_OIDP(FD_CAR(real_key))) || (FD_SYMBOLP(FD_CAR(real_key))))) {
	      if (get_slotid_index(hx,key)<0) oddkeys=1;}
	    s[i].key=real_key;
	    s[i].values=fd_make_simple_choice(kvscan->value);
	    s[i].replace=1; i++;}
	  else if ((FD_CAR(key))==drop_symbol) {
	    fdtype key_to_drop=FD_CDR(key);
	    fdtype cached=fd_hashtable_get(cache,key_to_drop,FD_VOID);
	    if (FD_VOIDP(cached))
	      drops[n_drops++]=FD_CDR(key);
	    else {
	      fdtype added=fd_hashtable_get(adds,key_to_drop,FD_EMPTY_CHOICE);
	      FD_ADD_TO_CHOICE(cached,added);
	      s[i].key=key_to_drop;
	      s[i].values=fd_difference(cached,kvscan->value);
	      s[i].replace=1;
	      fd_decref(cached);
	      i++;}}
	  else {}
	else {}
	kvscan++;}
      scan++;}
    else scan++;
  if (oddkeys) hx->hxflags=((hx->hxflags)|(FD_HASH_INDEX_ODDKEYS));
  drop_values=hash_index_fetchn_inner((fd_index)hx,n_drops,drops,0);
  scan=edits->slots; lim=scan+edits->n_slots;
  j=0; while (scan < lim)
	 if (*scan) {
	   struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
	   struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
	   while (kvscan<kvlimit) {
	     fdtype key=kvscan->key, key_to_drop=FD_CDR(key);
	     if ((FD_PAIRP(key)) && ((FD_CAR(key))==drop_symbol)) {
	       if (FD_CDR(key)==drops[j]) {
		 fdtype cached=drop_values[j];
		 fdtype added=
		   fd_hashtable_get(adds,key_to_drop,FD_EMPTY_CHOICE);
		 FD_ADD_TO_CHOICE(cached,added);
		 s[i].key=key_to_drop;
		 s[i].values=fd_difference(cached,kvscan->value);
		 s[i].replace=1;
		 fd_decref(cached);
		 i++; j++;}}
	     kvscan++;}
	   scan++;}
	 else scan++;
  u8_free(drops); u8_free(drop_values);
  return i;
}

static int process_adds(struct FD_HASH_INDEX *hx,fd_hashset taken,
			struct COMMIT_SCHEDULE *s,int i)
{
  int oddkeys=((hx->hxflags)&(FD_HASH_INDEX_ODDKEYS));
  fd_hashtable adds=&(hx->adds), edits=&(hx->edits), cache=&(hx->cache);
  struct FD_HASHENTRY **scan=adds->slots, **lim=scan+adds->n_slots;
  while (scan < lim)
    if (*scan) {
      struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
      struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
      while (kvscan<kvlimit) {
	fdtype key=kvscan->key;
	if (!(fd_hashset_get(taken,key))) {
	  if ((oddkeys==0) && (FD_PAIRP(key)) &&
	      ((FD_OIDP(FD_CAR(key))) || (FD_SYMBOLP(FD_CAR(key))))) {
	    if (get_slotid_index(hx,key)<0) oddkeys=1;}
	  s[i].key=key;
	  s[i].values=fd_make_simple_choice(kvscan->value);
	  s[i].replace=0; i++;}
	kvscan++;}
      scan++;}
    else scan++;
  if (oddkeys) hx->hxflags=((hx->hxflags)|(FD_HASH_INDEX_ODDKEYS));
  return i;
}

FD_FASTOP void parse_keybucket(fd_hash_index hx,struct KEYBUCKET *kb,
			    fd_byte_input in,int n_keys)
{
  int i=0; struct KEYENTRY *base_entry=&(kb->elt0);
  kb->n_keys=n_keys;
  while (i<n_keys) {
    int dt_size=fd_read_zint(in), n_values;
    struct KEYENTRY *entry=base_entry+i;
    entry->dtype_size=dt_size; entry->dtype_start=in->ptr;
    in->ptr=in->ptr+dt_size;
    entry->n_values=n_values=fd_read_zint(in);
    if (n_values==0) entry->values=FD_EMPTY_CHOICE;
    else if (n_values==1) entry->values=read_zvalue(hx,in);
    else {
      entry->values=FD_VOID;
      entry->vref.off=fd_read_4bytes(in);
      entry->vref.size=fd_read_zint(in);}
    i++;}
}

FD_FASTOP FD_BLOCK_REF write_value_block
(struct FD_HASH_INDEX *hx,fd_dtype_stream stream,
 fdtype values,fdtype extra,
 int cont_off,int cont_size,fd_off_t startpos)
{
  fd_off_t endpos=startpos; FD_BLOCK_REF retval;
  if (FD_CHOICEP(values)) {
    int full_size=FD_CHOICE_SIZE(values)+((FD_VOIDP(extra))?0:1);
    endpos=endpos+fd_dtswrite_zint(stream,full_size);
    if (!(FD_VOIDP(extra)))
      endpos=endpos+dtswrite_zvalue(hx,stream,extra);
    {FD_DO_CHOICES(value,values)
	endpos=endpos+dtswrite_zvalue(hx,stream,value);}}
  else if (FD_VOIDP(extra)) {
    endpos=endpos+fd_dtswrite_zint(stream,1);
    endpos=endpos+dtswrite_zvalue(hx,stream,values);}
  else {
    endpos=endpos+fd_dtswrite_zint(stream,2);
    endpos=endpos+dtswrite_zvalue(hx,stream,extra);
    endpos=endpos+dtswrite_zvalue(hx,stream,values);}
  endpos=endpos+fd_dtswrite_zint(stream,cont_size);
  if (cont_size)
    endpos=endpos+fd_dtswrite_4bytes(stream,cont_off);
  retval.off=startpos; retval.size=endpos-startpos;
  return retval;
}

FD_FASTOP fd_off_t extend_keybucket
  (fd_hash_index hx,struct KEYBUCKET *kb,
   struct COMMIT_SCHEDULE *schedule,int i,int j,
   fd_byte_output newkeys,fd_off_t endpos)
{
  int k=i; int _keyoffs[16], _keysizes[16], *keyoffs, *keysizes;
  if (FD_EXPECT_FALSE((j-i)>16) )  {
    keyoffs=u8_malloc(sizeof(int)*(j-i));
    keysizes=u8_malloc(sizeof(int)*(j-i));}
  else {keyoffs=_keyoffs; keysizes=_keysizes;}
  while (k<j) {
    int off;
    keyoffs[k-i]=off=newkeys->ptr-newkeys->start;
    write_zkey(hx,newkeys,schedule[k].key);
    keysizes[k-i]=(newkeys->ptr-newkeys->start)-off;
    k++;}
  k=i; while (k<j) {
    unsigned char *keydata=newkeys->start+keyoffs[k-i];
    struct KEYENTRY *ke=&(kb->elt0);
    int keysize=keysizes[k-i];
    int scan=0, n_keys=kb->n_keys, n_values;
    while (scan<n_keys)
      if ((ke[scan].dtype_size)!= keysize) scan++;
      else if (memcmp(keydata,ke[scan].dtype_start,keysize)) scan++;
      else {
	/* The key is already in there, so we write a value
	   block and update the key entry.  Note that if
	   the current value is a singleton, and stored in the keyblock,
	   we need to add that the value block we emit. */
	int n_values=ke[scan].n_values;
	int n_values_added=FD_CHOICE_SIZE(schedule[k].values);
	ke[scan].n_values=n_values+n_values_added;
	if (n_values==0) {}
	else if (n_values==1) {
	  ke[scan].vref=
	    write_value_block(hx,&(hx->stream),schedule[k].values,
			      ke[scan].values,0,0,endpos);
	  endpos=ke[scan].vref.off+ke[scan].vref.size;}
	else {
	  ke[scan].vref=
	    write_value_block(hx,&(hx->stream),schedule[k].values,FD_VOID,
			      ke[scan].vref.off,ke[scan].vref.size,
			      endpos);
	  endpos=ke[scan].vref.off+ke[scan].vref.size;}
	scan++;
	continue;}
    ke[n_keys].dtype_size=keysize;
    ke[n_keys].n_values=n_values=FD_CHOICE_SIZE(schedule[k].values);
    ke[n_keys].dtype_start=newkeys->start+keyoffs[k-i];
    if (n_values==0) ke[n_keys].values=FD_EMPTY_CHOICE;
    if (n_values==1) ke[n_keys].values=schedule[k].values;
    else {
      ke[n_keys].values=FD_VOID;
      ke[n_keys].vref=
	write_value_block(hx,&(hx->stream),schedule[k].values,FD_VOID,
			  0,0,endpos);
      endpos=ke[scan].vref.off+ke[scan].vref.size;}
    kb->n_keys++; k++;}
  return endpos;
}

FD_FASTOP fd_off_t write_keybucket
  (fd_hash_index hx,fd_dtype_stream stream,struct KEYBUCKET *kb,fd_off_t endpos)
{
  int i=0, n_keys=kb->n_keys;
  struct KEYENTRY *ke=&(kb->elt0);
  endpos=endpos+fd_dtswrite_zint(stream,n_keys);
  while (i<n_keys) {
    int dtype_size=ke[i].dtype_size, n_values=ke[i].n_values;
    endpos=endpos+fd_dtswrite_zint(stream,dtype_size);
    endpos=endpos+fd_dtswrite_bytes(stream,ke[i].dtype_start,dtype_size);
    endpos=endpos+fd_dtswrite_zint(stream,n_values);
    if (n_values==1)
      endpos=endpos+dtswrite_zvalue(hx,stream,ke[i].values);
    else {
      endpos=endpos+fd_dtswrite_4bytes(stream,ke[i].vref.off);
      endpos=endpos+fd_dtswrite_zint(stream,ke[i].vref.size);}
    i++;}
  return endpos;
}

FD_FASTOP struct KEYBUCKET *read_keybucket
  (fd_hash_index hx,int bucket,FD_BLOCK_REF ref,int extra)
{
  int n_keys;
  struct FD_BYTE_INPUT keyblock;
  struct KEYBUCKET *kb; unsigned char *keybuf;
  if (ref.size) {
    keybuf=u8_malloc(ref.size);
    open_block(&keyblock,hx,ref.off,ref.size,keybuf);
    n_keys=fd_read_zint(&keyblock);
    kb=u8_malloc(sizeof(struct KEYBUCKET)+
		 sizeof(struct KEYENTRY)*((extra+n_keys)-1));
    kb->bucket=bucket;
    kb->n_keys=n_keys; kb->keybuf=keybuf;
    parse_keybucket(hx,kb,&keyblock,n_keys);}
  else {
    kb=u8_malloc(sizeof(struct KEYBUCKET)+
		 sizeof(struct KEYENTRY)*(extra-1));
    kb->bucket=bucket;
    kb->n_keys=0; kb->keybuf=NULL;}
  return kb;
}

static int hash_index_commit(struct FD_INDEX *ix)
{
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  struct FD_DTYPE_STREAM *stream=&(hx->stream);
  struct FD_BLOCK_REF *buckets=u8_malloc(sizeof(FD_BLOCK_REF)*(hx->n_buckets));
  fd_off_t endpos;
  int n_keyblocks=0, new_keys=0, n_keys, new_buckets=0;
  fd_lock_mutex(&(hx->lock));
  fd_lock_mutex(&(hx->adds.lock));
  fd_lock_mutex(&(hx->edits.lock));
#if FD_DEBUG_HASHINDICES
  u8_message("Reading buckets for %s",hx->cid);
#endif
  fd_setpos(stream,256);
  fd_dtsread_ints(stream,(hx->n_buckets)*2,(unsigned int *)buckets);
#if FD_DEBUG_HASHINDICES
  u8_message("Read buckets for %s",hx->cid);
#endif
  {
    int i=0, n_buckets=0, cur_bucket=-1, bscan=0;
    int schedule_max=hx->adds.n_keys+hx->edits.n_keys, schedule_size=0;
    struct COMMIT_SCHEDULE *schedule=
      u8_malloc(sizeof(struct COMMIT_SCHEDULE)*schedule_max);
    struct BUCKET_REF *bucket_locs=
      u8_malloc(sizeof(struct BUCKET_REF)*schedule_max);
    struct KEYBUCKET **keybuckets=
      u8_malloc(sizeof(struct KEYBUCKET *)*schedule_max);
    struct FD_HASHSET taken;
    struct FD_BYTE_OUTPUT out, newkeys;
    fd_init_hashset(&taken,3*(hx->edits.n_keys));
#if FD_DEBUG_HASHINDICES
    u8_message("Adding %d edits to the schedule",hx->edits.n_keys);
#endif
    /* Get all the keys we need to write.  */
    schedule_size=process_edits(hx,&taken,schedule,schedule_size);
#if FD_DEBUG_HASHINDICES
    u8_message("Adding %d adds to the schedule",hx->adds.n_keys);
#endif
    schedule_size=process_adds(hx,&taken,schedule,schedule_size);
    fd_recycle_hashset(&taken);
    FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
    FD_INIT_BYTE_OUTPUT(&newkeys,schedule_max*16,NULL);
    if ((hx->hxflags)&(FD_HASH_INDEX_DTYPEV2)) {
      out.flags=out.flags|FD_DTYPEV2;
      newkeys.flags=newkeys.flags|FD_DTYPEV2;}
    /* Compute all the buckets for all the keys */
#if FD_DEBUG_HASHINDICES
    u8_message("Computing the buckets for %d scheduled keys",schedule_size);
#endif
    i=0; while (i<schedule_size) {
      fdtype key=schedule[i].key; int bucket;
      out.ptr=out.start;
      write_zkey(hx,&out,key);
      schedule[i].bucket=bucket=
	hash_bytes(out.start,out.ptr-out.start)%(hx->n_buckets);
      i++;}
#if FD_DEBUG_HASHINDICES
    u8_message("Fetching bucket locations");
#endif
    qsort(schedule,schedule_size,sizeof(struct COMMIT_SCHEDULE),
	  sort_cs_by_bucket);
    i=0; bscan=0; while (i<schedule_size) {
      int bucket=schedule[i].bucket, j=i;
      bucket_locs[n_buckets].bucket=bucket;
      bucket_locs[n_buckets].ref=get_block_ref(hx,bucket);
      while ((j<schedule_size) && (schedule[j].bucket==bucket)) j++;
      bucket_locs[n_buckets].max_new=j-i;
      n_buckets++; i=j;}
    /* Read all the buckets in order, reading each keyblock */
#if FD_DEBUG_HASHINDICES
    u8_message("Reading all the %d buckets in order",n_buckets);
#endif
    qsort(bucket_locs,n_buckets,sizeof(struct BUCKET_REF),
	  sort_br_by_off);
    i=0; while (i<n_buckets) {
      keybuckets[i]=
	read_keybucket(hx,bucket_locs[i].bucket,
		       bucket_locs[i].ref,bucket_locs[i].max_new);
      if ((keybuckets[i]->n_keys)==0) new_buckets++;
      i++;}
#if FD_DEBUG_HASHINDICES
    u8_message("Created %d new buckets",new_buckets);
#endif
    /* We could free bucket_locs here, but we don't. */
    /* Sort both the schedule and keybuckets by bucket. */
    qsort(schedule,schedule_size,sizeof(struct COMMIT_SCHEDULE),
	  sort_cs_by_bucket);
    qsort(keybuckets,n_buckets,sizeof(struct KEYBUCKET *),sort_kb_by_bucket);
#if FD_DEBUG_HASHINDICES
    u8_message("Extending for %d keys over %d buckets",
	       schedule_size,n_buckets);
#endif
    /* March along the commit schedule (keys) and keybuckets (buckets)
       in parallel. */
    i=0; bscan=0; endpos=fd_endpos(stream);
    while (i<schedule_size) {
      struct KEYBUCKET *kb=keybuckets[bscan];
      int bucket=schedule[i].bucket, j=i, n_keys, k, cur_keys;
      assert(bucket==kb->bucket);
      while ((j<schedule_size) && (schedule[j].bucket==bucket)) j++;
      cur_keys=kb->n_keys;
      endpos=extend_keybucket(hx,kb,schedule,i,j,&newkeys,endpos);
      new_keys=new_keys+(kb->n_keys-cur_keys);
      {
	fd_off_t startpos=endpos;
	endpos=write_keybucket(hx,stream,kb,endpos);
	buckets[schedule[i].bucket].off=startpos;
	buckets[schedule[i].bucket].size=endpos-startpos;}
      i=j; bscan++;}
#if FD_DEBUG_HASHINDICES
    u8_message("Cleaning up");
#endif
    /* Free the various things we've allocated or incref'd */
    u8_free(bucket_locs);
    /* Free all the buckets */
    bscan=0; while (bscan<n_buckets) {
      struct KEYBUCKET *kb=keybuckets[bscan++];
      struct KEYENTRY *scan=&(kb->elt0), *limit=scan+kb->n_keys;
      while (scan<limit) { fd_decref(scan->values); scan++; }
      if (kb->keybuf) u8_free(kb->keybuf);
      u8_free(kb);}
    u8_free(keybuckets);
    /* Note that the values stored in the schedule were copied
       into keyentries without being incref'd, so they were freed
       when we freed the keybuckets. */
    u8_free(schedule);
    u8_free(out.start);
    u8_free(newkeys.start);
    n_keys=schedule_size;
  }
  if (fd_acid_files) {
#if FD_DEBUG_HASHINDICES
    u8_message("Writing recovery data");
#endif
    /* Write the new offsets information to the end of the file
       for recovery if we die while doing the actual write. */
    fd_endpos(stream);
    fd_dtswrite_4bytes(stream,new_keys);
    fd_dtswrite_ints(stream,2*(hx->n_buckets),(unsigned int *)buckets);
    fd_setpos(stream,0); fd_dtswrite_4bytes(stream,FD_HASH_INDEX_TO_RECOVER);
    fd_dtsflush(stream); /* fsync(stream->fd); */
  }
#if FD_DEBUG_HASHINDICES
  u8_message("Writing offset data changes");
#endif
  /* Now, write the data it where it is supposed to be. */
  if (new_keys) {
    int cur_keys;
    /* Write any changed flags */
    fd_setpos(stream,8); fd_dtswrite_4bytes(stream,hx->hxflags);
    /* Write the new key count */
    fd_setpos(stream,16); cur_keys=fd_dtsread_4bytes(stream);
    fd_setpos(stream,16); fd_dtswrite_4bytes(stream,cur_keys+new_keys);}
  fd_setpos(stream,256);
  fd_dtswrite_ints(stream,2*(hx->n_buckets),(unsigned int *)buckets);
  if (fd_acid_files) {
    off_t end; int recovery_size=(8*(hx->n_buckets))+4;
#if FD_DEBUG_HASHINDICES
    u8_message("Erasing old recovery information");
#endif
    fd_setpos(stream,0); fd_dtswrite_4bytes(stream,FD_HASH_INDEX_MAGIC_NUMBER);
    fd_dtsflush(stream); fsync(stream->fd);
    /* Now erase the recovery information, since we don't need it anymore. */
    end=fd_endpos(stream);
    ftruncate(stream->fd,end-recovery_size);}

  if (hx->mmap) {
    int retval=munmap(hx->mmap,hx->mmap_size);
    if (retval<0) {
      u8_warn("MUNMAP","hashindex MUNMAP failed with %s",u8_strerror(errno));
      errno=0;}
    hx->mmap_size=u8_file_size(hx->cid);
    hx->mmap=
      mmap(NULL,hx->mmap_size,PROT_READ,MMAP_FLAGS,hx->stream.fd,0);}

#if FD_DEBUG_HASHINDICES
  u8_message("Resetting tables");
#endif

  /* And reset the modifications */
  fd_reset_hashtable(&(ix->adds),67,0);
  fd_unlock_mutex(&(ix->adds.lock));
  fd_reset_hashtable(&(ix->edits),67,0);
  fd_unlock_mutex(&(ix->edits.lock));
  u8_free(buckets);

  /* And unlock all the locks. */
  fd_unlock_mutex(&(hx->lock));
  
#if FD_DEBUG_HASHINDICES
  u8_message("Returning from hash_index_commit()");
#endif

  return n_keys;
}

static int recover_hash_index(struct FD_HASH_INDEX *hx)
{
  /* This reads the offsets vector written at the end of the file
     during commitment. */
  int i=0, len=hx->n_buckets, cur_keys, new_keys; off_t new_end;
  unsigned int *offsets=u8_malloc(8*len), magic_no;
  struct FD_DTYPE_STREAM *s=&(hx->stream);
  fd_endpos(s); new_end=fd_movepos(s,-(8*(len+1)));
  new_keys=fd_dtsread_4bytes(s);
  fd_dtsread_ints(s,len*2,offsets);
  fd_setpos(s,16); cur_keys=fd_dtsread_4bytes(s);
  fd_setpos(s,16); fd_dtswrite_4bytes(s,cur_keys+new_keys);
  fd_setpos(s,256);
  fd_dtswrite_ints(s,len*2,offsets);  
  fd_setpos(s,0); fd_dtswrite_4bytes(s,FD_HASH_INDEX_MAGIC_NUMBER);
  fd_dtsflush(s);
  ftruncate(s->fd,new_end);
  fsync(s->fd);
  return 0;
}


/* Miscellaneous methods */

static void hash_index_close(fd_index ix)
{
  struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
  fd_lock_mutex(&(hx->lock));
  fd_dtsclose(&(hx->stream),1);
  if (hx->buckets) {
#if HAVE_MMAP
    int retval=munmap(hx->buckets,(sizeof(FD_BLOCK_REF)*hx->n_buckets));
    if (retval<0) {
      u8_warn(u8_strerror(errno),"file_index_close:munmap %s",hx->source);
      errno=0;}
#else
    u8_free(hx->buckets); 
#endif
    hx->buckets=NULL;
    hx->cache_level=-1;}
  fd_unlock_mutex(&(hx->lock));
}

static void hash_index_setbuf(fd_index ix,int bufsiz)
{
  struct FD_HASH_INDEX *fx=(struct FD_HASH_INDEX *)ix;
  fd_lock_mutex(&(fx->lock));
  fd_dtsbufsize(&(fx->stream),bufsiz);
  fd_unlock_mutex(&(fx->lock));
}

static fdtype hash_index_metadata(fd_index ix,fdtype md)
{
  struct FD_HASH_INDEX *fx=(struct FD_HASH_INDEX *)ix;
  if (FD_VOIDP(md))
    return fd_read_index_metadata(&(fx->stream));
  else return fd_write_index_metadata((&(fx->stream)),md);
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
  hash_index_metadata, /* metadata */
  NULL /* sync */
};

FD_EXPORT int fd_hash_indexp(struct FD_INDEX *ix)
{
  return (ix->handler==&hash_index_handler);
}

FD_EXPORT fd_init_hashindices_c()
{
  set_symbol=fd_intern("SET");
  drop_symbol=fd_intern("DROP");

  fd_register_source_file(versionid);

  fd_register_index_opener(FD_HASH_INDEX_MAGIC_NUMBER,open_hash_index);
  fd_register_index_opener(FD_HASH_INDEX_TO_RECOVER,open_hash_index);
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
 
