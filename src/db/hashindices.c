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
16     XXXX    file offset of slotids vector
20     XXXX    size of slotids DTYPE representation
24     XXXX    file offset of baseoids vector
28     XXXX    size of baseoids DTYPE representation
32     XXXX    file offset of index metadata
36     XXXX    size of metadata DTYPE representation

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
  unsigned int magicno, n_buckets, hash_const;
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
    fd_setpos(stream,16);
    fd_dtswrite_4bytes(stream,slotids_pos);
    fd_dtswrite_4bytes(stream,slotids_size);}
  if (baseoids_pos) {
    fd_setpos(stream,24);
    fd_dtswrite_4bytes(stream,baseoids_pos);
    fd_dtswrite_4bytes(stream,baseoids_size);}
  if (metadata_pos) {
    fd_setpos(stream,32);
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
    if (key_len!=dtype_len) 
      keystream.ptr=keystream.ptr+key_len;
    else if (memcmp(keystream.ptr,out.start,dtype_len)==0) {
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

struct FETCH_SCHEDULE {
  int index; fdtype key; unsigned int key_start, size;
  unsigned int bucket; FD_BLOCK_REF ref;};

#if 0
static fdtype *fetchn(struct FD_FILE_INDEX *fx,int n,fdtype *keys,int lock_adds)
{
  fdtype *values=u8_malloc(sizeof(fdtype)*n);
  struct FD_BYTE_OUTPUT out;
  struct FETCH_SCHEDULE *schedule=u8_malloc(sizeof(struct FETCH_SCHEDULE)*n);
  int i=0, n_entries=0;
  FD_INIT_BYTE_OUTPUT(&out,n*64,NULL);
  while (i<n) {
    int dt_start=out.ptr-out.start, dt_size, bucket;
    schedule[n_entries].index=i; schedule[n_entries].key=key;
    schedule[n_entries].key_start=dt_start;
    write_zkey(hx,out,key);
    dt_size=(out.ptr-out.start)-start;
    schedule[n_entries].key_size=dt_size;
    schedule[n_entries].bucket=bucket=
      hash_bytes(out.start+dt_start,dt_size)%(hx->n_entries);
    if (hx->buckets) {
      schedule[i].ref=get_block_ref(hx,bucket);
      if (schedule[n_entries].ref.size==0) {
	/* It is empty, so we don't even need to handle this entry. */
	values[i]=FD_EMPTY_CHOICE;
	out.ptr=out.start+start;}
      else n_entries++;}
    else n_entries++;
    i++;}
  if (hx->buckets==NULL) {
    int write_at=0;
    qsort(schedule,sizeof(struct KEY_SCHEDULE),n_entries,
	  sort_ks_by_bucket);
    i=0; while (i<n_entries) {
      schedule[i].ref=get_block_ref(hx,bucket);
      if (schedule[i].ref.size==0) {
	values[schedule[i].index]=FD_EMPTY_CHOICE; i++;}
      else if (write_at==i) {write_at++; i++;}
      else {schedule[write_at++]=schedule[i++];}}
    n_entries=write-schedule;}
  qsort(schedule,sizeof(struct KEY_SCHEDULE),n_entries,
	sort_ks_by_refpos);
  {
    struct FD_BYTE_INPUT keyblock;
    unsigned char _buf[1024], *buf=NULL;
    int bucket=-1, j=0, bufsiz=0;
    while (j<n_entries) {
      int k=0, n_keys, found=0;
      fd_off_t blockpos=schedule[j].ref.pos;
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
	if ((dtsize==schedule[i].dtsize) &&
	    (memcmp(out.start+schedule[i].start,keyblock.ptr,dtsize)==0)) {
	  keyblock.ptr=keyblock.ptr+dtsize;
	  n_vals=fd_read_zint(&keyblock);
	  if (nvals==0) 
	    values[schedule[j].index]=FD_EMPTY_CHOICE;
	  else if (nvals==1)
	    values[schedule[j].index]=read_zvalue(hx,&keyblock);
	  else {}
	  break;}
	else keyblock.ptr=keyblock.ptr+dtsize;
	n_vals=fd_read_zint(&keyblock);
	if (n_vals==0) {}
	else if (n_vals==1) {
	  fdtype val=fd_read_dtype(&keyblock);
	  fd_decref(val);}
	else {
	  int vsize=fd_read_zint(&keyblock);
	  if (vsize) fd_read_4bytes(&keyblock);}
	k++;}
      j++;}
	    
	

  i=0; while (i<n_entries) {
    int n_keys;
    if (schedule[i].ref.size<1024)
      open_block(&keyblock,hx,schedule[i]ref.pos,schedule[i].ref.size,
		 _buf);
    else {
      buf=u8_malloc(schedule[i].ref.size);
      open_block(&keyblock,hx,schedule[i]ref.pos,schedule[i].ref.size,
		 buf);}
    n_keys=fd_read_zint(&keyblock);
    
  
    
}

#endif


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
  fdtype key; unsigned int pos, size;
  unsigned int bucket;};
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

static int doprefetch(struct POP_SCHEDULE *psched,fd_index ix,int i,int blocksize,int n_keys)
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

FD_EXPORT int fd_populate_hashindex(struct FD_HASH_INDEX *hx,fdtype from,fdtype keys,int blocksize)
{
  int i=0, n_buckets=hx->n_buckets, n_keys=FD_CHOICE_SIZE(keys);
  int filled_buckets=0, bucket_count=0, fetch_max=-1;
  struct POP_SCHEDULE *psched=u8_malloc(n_keys*sizeof(struct POP_SCHEDULE));
  struct FD_BYTE_OUTPUT out; FD_INIT_BYTE_OUTPUT(&out,n_keys*32,NULL);
  struct BUCKET_REF *bucket_refs; fd_index ix=NULL;
  fd_dtype_stream stream=&(hx->stream);
  off_t endpos=fd_endpos(stream);
  
  if (FD_INDEXP(from)) ix=fd_lisp2index(from);

  /* Fill the key schedule and count the number of buckets used */
  memset(psched,0,n_keys*sizeof(struct POP_SCHEDULE));
  {
    int cur_bucket=-1;
    FD_DO_CHOICES(key,keys) {
      int buf_off=out.ptr-out.start; int bucket;
      psched[i].key=key; psched[i].pos=buf_off;
      write_zkey(hx,&out,key);
      psched[i].size=(out.ptr-out.start)-buf_off;
      bucket=hash_bytes(out.start+psched[i].pos,psched[i].size)%n_buckets;
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
    while ((j<n_keys) && (psched[j].bucket==bucket)) j++;
    bucket_refs[bucket_count].bucket=bucket;
    load=j-i; FD_INIT_FIXED_BYTE_OUTPUT(&keyblock,buf,4096);
    fd_write_zint(&keyblock,load);
    if ((ix) && (i>=fetch_max))
      fetch_max=doprefetch(psched,ix,i,blocksize,n_keys);
    while (i<j) {
      fdtype key=psched[i].key, values=fd_get(from,key,FD_EMPTY_CHOICE);
      fd_write_zint(&keyblock,psched[i].size);
      fd_write_bytes(&keyblock,out.start+psched[i].pos,psched[i].size);
      fd_write_zint(&keyblock,FD_CHOICE_SIZE(values));
      if (FD_EMPTY_CHOICEP(values)) {}
      else if (FD_CHOICEP(values)) {
	int bytes_written=0;
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
	endpos=endpos+bytes_written;}
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
    bucket_count++;}
  qsort(bucket_refs,bucket_count,sizeof(struct BUCKET_REF),sort_br_by_bucket);
  i=0; while (i<bucket_count) {
    fd_setpos(stream,256+bucket_refs[i].bucket*8);
    fd_dtswrite_4bytes(stream,bucket_refs[i].off);
    fd_dtswrite_4bytes(stream,bucket_refs[i].size);
    i++;}
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
  NULL, /* fetchn */
  NULL, /* fetchkeys */
  NULL, /* fetchsizes */
  hashindex_metadata, /* fetchsizes */
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
 
