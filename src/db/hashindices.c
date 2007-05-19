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
 8     X       hash function identifer
 9      XXX    flags
12     XXXX    hash function constant
16     XXXX    file offset of index metadata
20     XXXX    size of metadata DTYPE representation
24     XXXX    file offset of slotids vector
28     XXXX    size of slotids DTYPE representation
32     XXXX    file offset of baseoids vector
36     XXXX    size of baseoids DTYPE representation

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

static fdtype read_zvalues(fd_hash_index,int,off_t,size_t);

static struct FD_INDEX_HANDLER hashindex_handler;

/* Used to generate hash codes */
#define MAGIC_MODULUS 16777213 /* 256000001 */
#define MIDDLIN_MODULUS 573786077 /* 256000001 */
#define MYSTERIOUS_MODULUS 2000239099 /* 256000001 */

FD_FASTOP fd_byte_input open_block
  (struct FD_BYTE_INPUT *bi,FD_HASH_INDEX *hx,
   fd_off_t off,fd_size_t size)
{
  if (bi==NULL) {
    unsigned char *buf=u8_malloc(size);
    bi=u8_malloc(sizeof(FD_BYTE_INPUT));
    FD_INIT_BYTE_INPUT(bi,buf,size);}
  if (hx->mmap==NULL) {
    fd_setpos(&(hx->stream),off);
    fd_dtsread_bytes(&(hx->stream),bi->start,size);
    bi->end=bi->start+size; bi->ptr=bi->start;}
  else {
    bi->ptr=bi->start=hx->mmap+off;
    bi->end=bi->start+size;}
  return bi;
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
  unsigned int magicno, n_buckets, hash_const;
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

  /* Skip the metadata block reference */
  fd_dtsread_4bytes(s); fd_dtsread_4bytes(s);

  /* Initialize the slotids field used for storing feature keys */
  {
    off_t off=fd_dtsread_4bytes(s);
    size_t n_bytes=fd_dtsread_4bytes(s);
    fdtype slotids_vector=((n_bytes) ? (read_dtype_at_pos(s,off)) : (FD_VOID));
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
  {
    off_t off=fd_dtsread_4bytes(s);
    size_t n_bytes=fd_dtsread_4bytes(s);
    fdtype baseoids_vector=((n_bytes) ? (read_dtype_at_pos(s,off)) : (FD_VOID));
    if (FD_VOIDP(baseoids_vector)) {
      index->n_baseoids=0;
      index->baseoids=NULL;
      index->baseoid_lookup=NULL;}
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
  FD_OID *baseoids=u8_malloc(sizeof(FD_OID)*n_baseoids);
  struct FD_BASEOID_LOOKUP *lookup=
    u8_malloc(sizeof(FD_BASEOID_LOOKUP)*n_baseoids);
  hx->n_baseoids=n_baseoids;
  hx->baseoids=baseoids;
  hx->baseoid_lookup=lookup;
  while (i<n_baseoids) {
    fdtype baseoid=baseoids_init[i];
    lookup[i].zindex=i;
    lookup[i].baseoid=FD_OID_ADDR(baseoid);
    baseoids[i]=FD_OID_ADDR(baseoid);
    i++;}
  qsort(lookup,n_baseoids,sizeof(FD_BASEOID_LOOKUP),sort_by_baseoid);
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
  
  /* This is where we store the offset and size for the metadata */
  fd_dtswrite_4bytes(stream,0); fd_dtswrite_4bytes(stream,0);

  /* This is where we store the offset and size for the slotid init */
  fd_dtswrite_4bytes(stream,0); fd_dtswrite_4bytes(stream,0);
  
  /* This is where we store the offset and size for the baseoids init */
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
  
  /* Write the metdata */
  if (FD_SLOTMAPP(metadata_init)) {
    metadata_pos=fd_getpos(stream);
    fd_dtswrite_dtype(stream,metadata_init);
    metadata_size=fd_getpos(stream)-metadata_pos;}

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

  if (metadata_pos) {
    fd_setpos(stream,16);
    fd_dtswrite_4bytes(stream,metadata_pos);
    fd_dtswrite_4bytes(stream,metadata_size);}
  if (slotids_pos) {
    fd_setpos(stream,24);
    fd_dtswrite_4bytes(stream,slotids_pos);
    fd_dtswrite_4bytes(stream,slotids_size);}
  if (baseoids_pos) {
    fd_setpos(stream,32);
    fd_dtswrite_4bytes(stream,baseoids_pos);
    fd_dtswrite_4bytes(stream,baseoids_size);}

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

static int get_slotid_index(fd_hash_index hx,fdtype slotid)
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

static int write_zkey(fd_hash_index hx,fd_byte_output out,fdtype key)
{
  int start_len=out->ptr-out->start, slotid_index=-1;
  if (FD_PAIRP(key)) {
    fdtype car=FD_CAR(key);
    if ((FD_OIDP(car)) || (FD_SYMBOLP(car))) {
      slotid_index=get_slotid_index(hx,car);
      if (slotid_index<0) 
	return fd_write_byte(out,0)+fd_write_dtype(out,key);
      else return fd_write_zint(out,slotid_index)+
	     fd_write_dtype(out,FD_CDR(key));}
    else return fd_write_dtype(out,key);}
  else return fd_write_dtype(out,key);
}

/* ZVALUEs */

static int get_baseoid_index(fd_hash_index hx,FD_OID oid)
{
  const int size=hx->n_baseoids; int cmp; unsigned int lo;
  fd_baseoid_lookup bottom=hx->baseoid_lookup, middle=bottom+size/2;
  fd_baseoid_lookup hard_top=bottom+size, top=hard_top;
  /* Clear the lower three bits.  */
  lo=FD_OID_LO(oid); lo=lo&(0xFF000000);
  FD_SET_OID_LO(oid,lo);
  while (top>bottom) {
    cmp=FD_OID_COMPARE(oid,middle->baseoid);
    if (cmp==0) return middle->zindex;
    else if (cmp<0) {
      top=middle-1; middle=bottom+(top-bottom)/2;}
    else {
      bottom=middle+1; middle=bottom+(top-bottom)/2;}}
  if ((middle) && (middle<hard_top) &&
      (FD_OID_COMPARE(middle->baseoid,oid)==0))
    return middle->zindex;
  return -1;
}

static fdtype write_zvalue(fd_hash_index hx,fd_byte_output out,fdtype value)
{
  if (FD_OIDP(value)) {
    int baseoid_index=get_baseoid_index(hx,FD_OID_ADDR(value));
    if (baseoid_index<0) {
      int bytes_written; fd_write_byte(out,0);
      bytes_written=fd_write_dtype(out,value);
      return bytes_written+1;}
    else {
      FD_OID addr=FD_OID_ADDR(value);
      int offset=FD_OID_LO(addr);
      offset=offset&0xFFFFFF;
      fd_write_zint(out,baseoid_index+1);
      fd_write_byte(out,((offset>>16)&0xFF));
      fd_write_byte(out,((offset>>8)&0xFF));
      fd_write_byte(out,((offset)&0xFF));
      return 4;}}
  else {
    int bytes_written; fd_write_byte(out,0);
    bytes_written=fd_write_dtype(out,value);
    return bytes_written+1;}
}

static fdtype dtswrite_zvalue(fd_hash_index hx,fd_dtype_stream out,fdtype value)
{
  if (FD_OIDP(value)) {
    int baseoid_index=get_baseoid_index(hx,FD_OID_ADDR(value));
    if (baseoid_index<0) {
      int bytes_written; fd_dtswrite_byte(out,0);
      bytes_written=fd_dtswrite_dtype(out,value);
      return bytes_written+1;}
    else {
      FD_OID addr=FD_OID_ADDR(value);
      int offset=FD_OID_LO(addr);
      offset=offset&0xFFFFFF;
      fd_dtswrite_zint(out,baseoid_index+1);
      fd_dtswrite_byte(out,((offset>>16)&0xFF));
      fd_dtswrite_byte(out,((offset>>8)&0xFF));
      fd_dtswrite_byte(out,((offset)&0xFF));
      return 4;}}
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
    FD_OID addr=hx->baseoids[prefix-1];
    int offset=((*(in->ptr))<<16)|((*(in->ptr+1))<<8)|(*(in->ptr+2));
    in->ptr=in->ptr+3;
    FD_OID_PLUS(addr,offset);
    return fd_make_oid(addr);}
}

/* Fetching */

static fdtype hashindex_fetch(fd_index ix,fdtype key)
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
  if (keyblock.size==0) return FD_EMPTY_CHOICE;
  if (keyblock.size<512) {
    inbuf=NULL;
    FD_INIT_BYTE_INPUT(&keystream,_inbuf,keyblock.size);}
  else {
    inbuf=u8_malloc(keyblock.size);
    FD_INIT_BYTE_INPUT(&keystream,inbuf,keyblock.size);}
  open_block(&keystream,hx,keyblock.off,keyblock.size);
  n_keys=fd_read_zint(&keystream);
  i=0; while (i<n_keys) {
    int key_len=fd_read_zint(&keystream);
    if (key_len!=dtype_len) 
      keystream.ptr=keystream.ptr+key_len;
    else if (memcmp(keystream.ptr,out.start,dtype_len)==0) {
      keystream.ptr=keystream.ptr+key_len;
      n_values=fd_read_zint(&keystream);
      if (n_values==0) {
	if (inbuf) u8_free(inbuf);
	return FD_EMPTY_CHOICE;}
      else if (n_values==1)
	return read_zvalue(hx,&keystream);
      else {
	vblock_off=(off_t)fd_read_zint(&keystream);
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
  struct FD_BYTE_INPUT instream;
  int atomicp=1;
  while (vblock_off != 0) {
    int n_elts, i;
    open_block(&instream,hx,vblock_off,vblock_size);
    n_elts=fd_read_zint(&instream);
    i=0; while (i<n_elts) {
      fdtype val=read_zvalue(hx,&instream);
      if (FD_CONSP(val)) atomicp=0;
      *scan++=val; i++;}
    vblock_size=fd_read_zint(&instream);
    if (vblock_size) vblock_off=fd_read_4bytes(&instream);
    else vblock_off=0;}
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
  if (keyblock.size<512) {
    inbuf=NULL;
    FD_INIT_BYTE_INPUT(&keystream,_inbuf,keyblock.size);}
  else {
    inbuf=u8_malloc(keyblock.size);
    FD_INIT_BYTE_INPUT(&keystream,inbuf,keyblock.size);}
  open_block(&keystream,hx,keyblock.off,keyblock.size);
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
	mmap(NULL,(n_buckets*sizeof(struct FD_BLOCK_REF)),
	     PROT_READ,MMAP_FLAGS,s->fd,256);
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
      retval=munmap(hx->buckets-2,(hx->n_buckets*sizeof(FD_BLOCK_REF)));
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

struct KEY_SCHEDULE {
  fdtype key; unsigned char *dtype; unsigned int size;
  unsigned int bucket;};
struct BUCKET_REF {
  unsigned int bucket; fd_off_t off; fd_size_t size;};

static int sort_ks_by_bucket(const void *p1,const void *p2)
{
  struct KEY_SCHEDULE *ps1=(struct KEY_SCHEDULE *)p1;
  struct KEY_SCHEDULE *ps2=(struct KEY_SCHEDULE *)p2;
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

static int doprefetch(struct KEY_SCHEDULE *psched,fd_index ix,int i,int blocksize,int n_keys)
{
  fdtype prefetch=FD_EMPTY_CHOICE;
  int k=i, lim=k+blocksize, final_bckt;
  if (lim>n_keys) lim=n_keys;
  while (k<lim) {
    FD_ADD_TO_CHOICE(prefetch,psched[k].key); k++;}
  final_bckt=psched[k-1].bucket; while (k<n_keys)
    if (psched[k].bucket != final_bckt) break;
    else {FD_ADD_TO_CHOICE(prefetch,psched[k].key); k++;}
  fd_index_prefetch(ix,prefetch);
  fd_decref(prefetch);
  return k;
}

FD_EXPORT int fd_populate_hashindex(struct FD_HASH_INDEX *hx,fdtype from,fdtype keys,int blocksize)
{
  int i=0, n_buckets=hx->n_buckets, n_keys=FD_CHOICE_SIZE(keys);
  int filled_buckets=0, bucket_count=0;
  struct KEY_SCHEDULE *psched=u8_malloc(n_keys*sizeof(struct KEY_SCHEDULE));
  struct FD_BYTE_OUTPUT out; FD_INIT_BYTE_OUTPUT(&out,n_keys*32,NULL);
  struct BUCKET_REF *bucket_refs; fd_index ix=NULL;
  fd_dtype_stream stream=&(hx->stream);
  off_t endpos=fd_endpos(stream);
  
  if (FD_INDEXP(from)) ix=fd_lisp2index(from);

  /* Fill the key schedule and count the number of buckets used */
  memset(psched,0,n_keys*sizeof(struct KEY_SCHEDULE));
  {
    int cur_bucket=-1;
    FD_DO_CHOICES(key,keys) {
      int buf_off=out.ptr-out.start; int bucket;
      psched[i].key=key; psched[i].dtype=out.ptr;
      write_zkey(hx,&out,key);
      psched[i].size=(out.ptr-out.start)-buf_off;
      bucket=hash_bytes(psched[i].dtype,psched[i].size)%n_buckets;
      psched[i].bucket=bucket;
      if (bucket!=cur_bucket) {cur_bucket=bucket; filled_buckets++;}
      i++;}
    /* Allocate the bucket_refs */
    bucket_refs=u8_malloc(sizeof(struct BUCKET_REF)*filled_buckets);}

  /* Sort the key schedule by bucket */
  qsort(psched,n_keys,sizeof(struct KEY_SCHEDULE),sort_ks_by_bucket);

  i=0; while (i<n_keys) {
    struct FD_BYTE_OUTPUT keyblock;
    int retval, fetch_max=-1;
    unsigned int bucket=psched[i].bucket, load=0, j=i;
    if ((blocksize>0) && (FD_INDEXP(from)) && ((i%blocksize)==0)) 
    while ((j<n_keys) && (psched[j].bucket==bucket)) j++;
    bucket_refs[bucket_count].bucket=bucket;
    load=j-i; FD_INIT_BYTE_OUTPUT(&keyblock,load*64,NULL);
    fd_write_zint(&keyblock,load);
    if ((ix) && (i>fetch_max))
      fetch_max=doprefetch(psched,ix,i,blocksize,n_keys);
    while (i<j) {
      fdtype key=psched[i].key, values=fd_get(from,key,FD_EMPTY_CHOICE);
      fd_write_zint(&keyblock,psched[i].size);
      fd_write_bytes(&keyblock,psched[i].dtype,psched[i].size);
      fd_write_zint(&keyblock,FD_CHOICE_SIZE(values));
      if (FD_EMPTY_CHOICEP(values)) {}
      else if (FD_CHOICEP(values)) {
	int bytes_written=0;
	fd_write_4bytes(&keyblock,endpos);
	retval=fd_dtswrite_zint(stream,FD_CHOICE_SIZE(values));
	if (retval<0) {
	  u8_free(keyblock.start);
	  u8_free(out.start);
	  u8_free(psched);
	  u8_free(bucket_refs);
	  return -1;}
	else bytes_written=bytes_written+retval;
	{FD_DO_CHOICES(value,values) {
	  int retval=dtswrite_zvalue(hx,stream,value);
	  if (retval<0) return -1;
	  else bytes_written=bytes_written+retval;}}
	fd_write_zint(&keyblock,bytes_written);
	endpos=endpos+bytes_written;}
      else write_zvalue(hx,&keyblock,values);
      fd_decref(values);
      i++;}
    /* Write the bucket information */
    bucket_refs[bucket_count].off=endpos;
    retval=fd_dtswrite_bytes(stream,keyblock.start,keyblock.ptr-keyblock.start);
    if (retval<0) return -1;
    else bucket_refs[bucket_count].size=retval;
    endpos=endpos+retval;
    bucket_count++;}
  qsort(psched,n_keys,sizeof(struct KEY_SCHEDULE),sort_ks_by_bucket);
  i=0; while (i<bucket_count) {
    fd_setpos(stream,256+bucket_refs[i].bucket*8);
    fd_dtswrite_4bytes(stream,bucket_refs[i].off);
    fd_dtswrite_4bytes(stream,bucket_refs[i].size);
    i++;}
  fd_dtsflush(stream);
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
  NULL, /* fetchn */
  NULL, /* fetchkeys */
  NULL, /* fetchsizes */
  hashindex_metadata, /* fetchsizes */
  NULL /* sync */
};

FD_EXPORT fd_init_hashindices_c()
{
  fd_register_source_file(versionid);

  fd_register_index_opener(FD_HASH_INDEX_MAGIC_NUMBER,open_hashindex);
}




