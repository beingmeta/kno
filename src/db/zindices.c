/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_DTYPEIO 1

#include "fdb/dtype.h"
#include "fdb/dbfile.h"

#include <sys/stat.h>
#include <errno.h>
#include <zlib.h>

static fd_exception BadZKEY=_("Bad ZKEY reference");

#if (HAVE_MMAP)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#define MMAP_FLAGS MAP_SHARED
#endif

#define SLOTSIZE (sizeof(unsigned int))

#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
#define offget(offvec,offset) (fd_flip_word((offvec)[offset]))
#define set_offset(offvec,offset,v) (offvec)[offset]=(fd_flip_word(v))
#else
#define offget(offvec,offset) ((offvec)[offset])
#define set_offset(offvec,offset,v) (offvec)[offset]=(v)
#endif

#define FD_DEFAULT_ZLEVEL 9

/* Reading compressed oid values */

static unsigned char *do_uncompress
  (unsigned char *bytes,int n_bytes,int *dbytes)
{
  int error;
  uLongf x_lim=4*n_bytes, x_bytes=x_lim;
  Bytef *fdata=(Bytef *)bytes, *xdata=u8_malloc(x_bytes);
  while ((error=uncompress(xdata,&x_bytes,fdata,n_bytes)) < Z_OK)
    if (error == Z_MEM_ERROR) {
      u8_free(xdata); u8_free(bytes);
      fd_seterr1("ZLIB Out of Memory");
      return NULL;}
    else if (error == Z_BUF_ERROR) {
      xdata=u8_realloc(xdata,x_lim*2); x_bytes=x_lim=x_lim*2;}
    else if (error == Z_DATA_ERROR) {
      u8_free(xdata); u8_free(bytes);
      fd_seterr1("ZLIB Data error");
      return NULL;}
    else {
      u8_free(xdata);
      fd_seterr1("Bad ZLIB return code");
      return NULL;}
  *dbytes=x_bytes;
  return xdata;
}

static unsigned char *do_compress(unsigned char *bytes,int n_bytes,int *zbytes)
{
  int error; Bytef *zdata;
  uLongf zlen, zlim, wlen=0;
  zlen=zlim=2*n_bytes; zdata=u8_malloc(zlen);
  while ((error=compress2(zdata,&zlen,bytes,n_bytes,FD_DEFAULT_ZLEVEL)) < Z_OK)
    if (error == Z_MEM_ERROR) {
      u8_free(zdata);
      fd_seterr1("ZLIB Out of Memory");
      return NULL;}
    else if (error == Z_BUF_ERROR) {
      zdata=u8_realloc(zdata,zlim*2); zlen=zlim=zlim*2;}
    else if (error == Z_DATA_ERROR) {
      u8_free(zdata); 
      fd_seterr1("ZLIB Data error");
      return NULL;}
    else {
      u8_free(zdata);
      fd_seterr1("Bad ZLIB return code");
      return NULL;}
  *zbytes=zlen;
  return zdata;
}

/* This reads a non frame value with compression. */
static fdtype zread_dtype(struct FD_DTYPE_STREAM *s)
{
  fdtype result;
  struct FD_BYTE_INPUT in;
  int n_bytes=fd_dtsread_zint(s), dbytes;
  unsigned char *bytes;
  bytes=u8_malloc(n_bytes); fd_dtsread_bytes(s,bytes,n_bytes);
  in.ptr=in.start=do_uncompress(bytes,n_bytes,&dbytes);
  in.end=in.start+dbytes; in.fillfn=NULL; in.mpool=NULL;
  result=fd_read_dtype(&in,NULL);
  u8_free(bytes); u8_free(in.start);
  return result;
}

static int read_baseoid_offset(struct FD_DTYPE_STREAM *s)
{
  int first_byte=fd_dtsread_byte(s);
  if (first_byte < 0xC0) return first_byte&0x3F;
  else {
    int result=(first_byte&0x3F)<<7, probe;
    while (probe=fd_dtsread_byte(s))
      if (probe&0x80) result=(result|probe)<<7;
      else break;
    return result|probe;}
}

static fdtype zread_value
  (struct FD_DTYPE_STREAM *s,FD_OID *baseoids,int n_baseoids)
{
  if (fd_needs_bytes((fd_byte_input)s,1))
    if ((*(s->ptr))<0x80) return fd_dtsread_dtype(s);
    else {
      int baseoff=read_baseoid_offset(s);
      FD_OID addr=baseoids[baseoff];
      unsigned int low=FD_OID_LO(addr);
      low=low|(fd_dtsread_byte(s)<<8);
      low=low|fd_dtsread_byte(s);
      FD_SET_OID_LO(addr,low);
      return fd_make_oid(addr);}
  else fd_err(fd_UnexpectedEOD,"zread_value",NULL,FD_VOID);  
}

static fdtype zread_key
  (struct FD_DTYPE_STREAM *s,fdtype slotids,FD_OID *baseoids,int n_baseoids)
{
  int code=fd_dtsread_zint(s), len=FD_VECTOR_LENGTH(slotids);
  if (code)
    if (code<=len) 
      return fd_init_pair(NULL,FD_VECTOR_REF(slotids,code-1),
			  zread_value(s,baseoids,n_baseoids));
    else return fd_err(BadZKEY,"zread_key",NULL,FD_INT2DTYPE(code));
  else return zread_value(s,baseoids,n_baseoids);
}

/* This writes a non frame value with compression. */
static int zwrite_dtype(struct FD_DTYPE_STREAM *s,fdtype x)
{
  fdtype result;
  unsigned char *zbytes; int zlen, size;
  struct FD_BYTE_OUTPUT out;
  out.ptr=out.start=u8_malloc(1024); out.end=out.start+1024;
  fd_write_dtype(&out,x);
  zbytes=do_compress(out.start,out.ptr-out.start,&zlen);
  size=fd_dtswrite_zint(s,zlen); size=size+zlen;
  fd_dtswrite_bytes(s,zbytes,zlen);
  u8_free(zbytes); u8_free(out.start);
  return size;
}

#define in_baseoid(addr,baseoid) \
  (((FD_OID_HI(addr)) == (FD_OID_HI(baseoid))) && \
   ((FD_OID_LO(addr)&0xFFFF0000) == (FD_OID_LO(baseoid))))

static int get_baseoid_offset(fdtype oid,FD_OID *baseoids,int n_baseoids)
{
  int i=0; FD_OID addr=FD_OID_ADDR(oid);
  while (i < n_baseoids)
    if (in_baseoid(addr,baseoids[i])) return i;
    else i++;
  return -1;
}

static int write_baseoid_offset(struct FD_DTYPE_STREAM *f,int i)
{
  if (i<0x40) {
    fd_dtswrite_byte(f,i|0x80); return 1;}
  else if (i < (0x40<<7)) {
    fd_dtswrite_byte(f,(i>>7)|0xC0);
    fd_dtswrite_byte(f,(i&0x7F));
    return 2;}
  else if (i < (0x40<<14)) {
    fd_dtswrite_byte(f,((i>>14)&0x7F)|0xC0);
    fd_dtswrite_byte(f,((i>>7)&0x7F)|0x80);
    fd_dtswrite_byte(f,(i&0x7F));
    return 3;}
  else if (i < (0x40<<21)) {
    fd_dtswrite_byte(f,((i>>21)&0x7F)|0xC0);
    fd_dtswrite_byte(f,((i>>14)&0x7F)|0x80);
    fd_dtswrite_byte(f,((i>>7)&0x7F)|0x80);
    fd_dtswrite_byte(f,(i&0x7F));
    return 4;}
  else {
    fd_dtswrite_byte(f,((i>>28)&0x7F)|0xC0);
    fd_dtswrite_byte(f,((i>>21)&0x7F)|0x80);
    fd_dtswrite_byte(f,((i>>14)&0x7F)|0x80);
    fd_dtswrite_byte(f,((i>>7)&0x7F)|0x80);
    fd_dtswrite_byte(f,(i&0x7F));
    return 5;}
}

static int zwrite_value
  (struct FD_DTYPE_STREAM *stream,fdtype value,
   FD_OID *baseoids,int n_baseoids)
{
  int size=0;
  if (FD_OIDP(value)) {
    int baseoid=get_baseoid_offset(value,baseoids,n_baseoids);
    if (baseoid < 0)
      return fd_dtswrite_dtype(stream,value);
    else {
      FD_OID addr=FD_OID_ADDR(value);
      int size=write_baseoid_offset(stream,baseoid);
      fd_dtswrite_byte(stream,((FD_OID_LO(addr)>>8)&0xFF));
      fd_dtswrite_byte(stream,(FD_OID_LO(addr)));
      return size+2;}}
  else return fd_dtswrite_dtype(stream,value);
}

static int zwrite_values
  (struct FD_DTYPE_STREAM *stream,
   fdtype value,
   FD_OID *baseoids,int n_baseoids,
   off_t nextpos,int *n_valuesp)
{
  fdtype realval=((FD_ACHOICEP(value)) ? (fd_make_simple_choice(value)) :
		   (value));
  int n_elts=FD_CHOICE_SIZE(realval);
  int size=fd_dtswrite_zint(stream,n_elts);
  FD_DO_CHOICES(elt,realval) {
    size=size+zwrite_value(stream,elt,baseoids,n_baseoids);}
  size=size+fd_dtswrite_zint(stream,nextpos);
  *n_valuesp=n_elts;
  if (FD_ACHOICEP(value)) fd_decref(realval);
  return size;
}

/* A key entry has either the form:
    slotid_offset+1 value_dtype
    0 value_dtype
   The value dtype follows the convention above, using baseoids
    for compression.
*/
static int zwrite_key
  (struct FD_DTYPE_STREAM *stream,fdtype key,
   fdtype slotids,FD_OID *baseoids,int n_baseoids)
{
  if (FD_PAIRP(key))
    if (FD_VECTORP(slotids)) {
      fdtype slotid=FD_CAR(key);
      int i=0, lim=FD_VECTOR_LENGTH(slotids);
      while (i < lim)
	if (FD_EQ(slotid,FD_VECTOR_REF(slotids,i))) break;
	else i++;
      if (i >= lim) {
	fd_dtswrite_byte(stream,0);
	return 1+zwrite_value(stream,key,baseoids,n_baseoids);}
      return fd_dtswrite_zint(stream,i+1)+
	zwrite_value(stream,FD_CDR(key),baseoids,n_baseoids);}
    else {
      fd_dtswrite_byte(stream,0);
      return 1+zwrite_value(stream,key,baseoids,n_baseoids);}
  else {
    fd_dtswrite_byte(stream,0);
    return 1+zwrite_value(stream,key,baseoids,n_baseoids);}
}

/* Opening zindices */

static fdtype set_symbol, drop_symbol;
static struct FD_INDEX_HANDLER zindex_handler;

static fd_index open_zindex(u8_string fname,int read_only)
{
  struct FD_ZINDEX *index=u8_malloc(sizeof(struct FD_ZINDEX));
  struct FD_DTYPE_STREAM *s=&(index->stream);
  unsigned int magicno, n_slots;
  fd_dtstream_mode mode=
    ((read_only) ? (FD_DTSTREAM_READ) : (FD_DTSTREAM_MODIFY));
  fd_init_index(index,&zindex_handler,fname);
  if (fd_init_dtype_file_stream(s,fname,mode,FD_FILEDB_BUFSIZE,NULL,NULL)==NULL) {
    u8_free(index);
    fd_seterr3(fd_CantOpenFile,"open_zindex",u8_strdup(fname));
    return NULL;}
  /* See if it ended up read only */
  if (index->stream.bits&FD_DTSTREAM_READ_ONLY) read_only=1;
  index->stream.mallocd=0;
  magicno=fd_dtsread_4bytes(s);
  if (magicno == FD_ZINDEX_MAGIC_NUMBER) index->hashv=1;
  else {
    fd_seterr3(fd_NotAFileIndex,"open_zincdex",u8_strdup(fname));
    u8_free(index);
    return NULL;}
  index->n_slots=fd_dtsread_4bytes(s);
  index->offsets=NULL; index->read_only=read_only;
  {
    fdtype metadata, slotids, baseoidsv; int i=0, probe; off_t md_loc;
    fd_setpos(s,8+index->n_slots*4); probe=fd_dtsread_4bytes(s);
    if (probe != 0xFFFFFFFE) {
      fd_seterr3(fd_BadMetaData,"open_zindex",u8_strdup(fname));
      u8_free(index);
      return NULL;}
    while (i<8) {fd_dtsread_4bytes(s); i++;}
    md_loc=fd_dtsread_4bytes(s);
    if (md_loc) fd_setpos(s,md_loc);
    else {
      fd_seterr3(fd_NotAFileIndex,"open_zindex",u8_strdup(fname));
      u8_free(index);
      return NULL;}
    metadata=fd_dtsread_dtype(s);
    index->slotids=slotids=
      fd_get(metadata,fd_intern("SLOTIDS"),FD_EMPTY_CHOICE);
    baseoidsv=fd_get(metadata,fd_intern("BASEOIDS"),FD_EMPTY_CHOICE);
    if (FD_VECTORP(baseoidsv)) {
      int n_baseoids=FD_VECTOR_LENGTH(baseoidsv);
      if (n_baseoids==0) {
	index->baseoids=NULL; index->n_baseoids=0;}
      else {
	FD_OID *baseoids=u8_malloc(sizeof(FD_OID)*n_baseoids);
	int i=0; while (i < n_baseoids) {
	  fdtype elt=FD_VECTOR_REF(baseoidsv,i);
	  if (FD_OIDP(elt)) baseoids[i]=FD_OID_ADDR(elt);
	  else {
	    fd_seterr3(fd_NotAFileIndex,"open_zindex",u8_strdup(fname));
	    u8_free(index);
	    return NULL;}
	  i++;}
	index->baseoids=baseoids;
	index->n_baseoids=n_baseoids;}}
    else {index->baseoids=NULL; index->n_baseoids=0;}
    fd_decref(baseoidsv);
    fd_decref(metadata);}
  u8_init_mutex(&(index->lock));
  return (fd_index)index;
}

static unsigned int get_offset(fd_zindex ix,int slotno)
{
  fd_setpos(&(ix->stream),slotno*4+8);
  return fd_dtsread_4bytes(&(ix->stream));
}

static void zindex_setcache(fd_index ix,int level)
{
  struct FD_ZINDEX *fx=(struct FD_ZINDEX *)ix;
  if (level == 2)
    if (fx->offsets) return;
    else {
      fd_dtype_stream s=&(fx->stream);
      unsigned int *offsets, *newmmap;
      u8_lock_mutex(&(fx->lock));
      if (fx->offsets) {
	u8_unlock_mutex(&(fx->lock));
	return;}
#if HAVE_MMAP
      newmmap=
	mmap(NULL,(fx->n_slots*SLOTSIZE)+8,
	     PROT_READ,MMAP_FLAGS,s->fd,0);
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
	u8_warn(u8_strerror(errno),"zindex_setcache:mmap %s",fx->source);
	fx->offsets=NULL; errno=0;}
      else fx->offsets=offsets=newmmap+2;
#else
      fd_dts_start_read(s);
      offsets=u8_malloc(SLOTSIZE*(fx->n_slots));
      fd_setpos(s,8);
      fd_dtsread_ints(s,fx->n_slots,offsets);
      fx->offsets=offsets; 
#endif
      u8_unlock_mutex(&(fx->lock));}
  else if (level < 2)
    if (fx->offsets == NULL) return;
    else {
      int retval;
      u8_lock_mutex(&(fx->lock));
#if HAVE_MMAP
      retval=munmap(fx->offsets-2,(fx->n_slots*SLOTSIZE)+8);
      if (retval<0) {
	u8_warn(u8_strerror(errno),"zindex_setcache:munnmap %s",fx->source);
	fx->offsets=NULL; errno=0;}
#else
      u8_free(fx->offsets);
#endif
      fx->offsets=NULL;
      u8_unlock_mutex(&(fx->lock));}
}

static fdtype zindex_fetch(fd_index ix,fdtype key)
{
  struct FD_ZINDEX *fx=(struct FD_ZINDEX *)ix;
  u8_lock_mutex(&(fx->lock));
  {
    fd_dtype_stream stream=&(fx->stream);
    unsigned int hashval=fd_hash_dtype2(key);
    unsigned int n_probes=0;
    unsigned int probe=hashval%(fx->n_slots);
    unsigned int chain_width=(hashval%(fx->n_slots-2))+1;
    unsigned int *offsets=fx->offsets;
    unsigned int keypos=
      ((offsets) ? (offget(offsets,probe)) : (get_offset(fx,probe)));
    while (keypos) {
      fdtype thiskey; unsigned int n_vals; off_t val_start;
      unsigned int pos_offset=fx->n_slots*4;
      fd_setpos(stream,keypos+pos_offset);
      n_vals=fd_dtsread_zint(stream);
      val_start=fd_dtsread_zint(stream);
      if (FD_EXPECT_FALSE((n_vals==0) && (val_start)))
	u8_raise(_("inconsistent file index"),"zindex_fetch",
		 u8_strdup(ix->cid));
      thiskey=zread_key(stream,fx->slotids,fx->baseoids,fx->n_baseoids);
      if (FD_ABORTP(thiskey)) return thiskey;
      else if (FDTYPE_EQUAL(key,thiskey))
	if (n_vals==0) {
	  u8_unlock_mutex(&fx->lock); fd_decref(thiskey);
	  return FD_EMPTY_CHOICE;}
	else {
	  int i=0, n_values, atomicp=1;
	  struct FD_CHOICE *result=fd_alloc_choice(n_vals);
	  fdtype *values=(fdtype *)FD_XCHOICE_DATA(result);
	  off_t next_pos=val_start;
	  while (next_pos) {
	    int n_values;
	    if (next_pos>1) fd_setpos(stream,next_pos+pos_offset);
	    n_values=fd_dtsread_zint(stream);
	    if (FD_EXPECT_FALSE((i+n_values)>n_vals))
	      u8_raise(_("inconsistent file index"),"zindex_fetch",
		       u8_strdup(ix->cid));
	    while (i<n_values) {
	      fdtype v=zread_value(stream,fx->baseoids,fx->n_baseoids);
	      if ((atomicp) && (FD_CONSP(v))) atomicp=0;
	      values[i++]=v;}
	    next_pos=fd_dtsread_zint(stream);}
	  u8_unlock_mutex(&fx->lock); fd_decref(thiskey);
	  return fd_init_choice(result,n_vals,NULL,
				(FD_CHOICE_DOSORT|
				 ((atomicp)?(FD_CHOICE_ISATOMIC):
				  (FD_CHOICE_ISCONSES))|
				 FD_CHOICE_REALLOC));}
      else if (n_probes>256) {
	u8_unlock_mutex(&fx->lock);
	return fd_err(fd_FileIndexOverflow,"zindex_fetch",
		      fx->source,FD_VOID);}
      else {
	n_probes++;
	fd_decref(thiskey);
	probe=(probe+chain_width)%(fx->n_slots);
	keypos=
	  ((offsets) ? (offget(offsets,probe)) : (get_offset(fx,probe)));}}}
  u8_unlock_mutex(&fx->lock);
  return FD_EMPTY_CHOICE;
}

/* Fetching sizes */

static int zindex_fetchsize(fd_index ix,fdtype key)
{
  struct FD_ZINDEX *fx=(struct FD_ZINDEX *)ix;
  u8_lock_mutex(&(fx->lock));
  {
    fd_dtype_stream stream=&(fx->stream);
    unsigned int hashval=
      ((fx->hashv) ? (fd_hash_dtype2(key)) : (fd_hash_dtype1(key)));
    unsigned int n_probes=0;
    unsigned int probe=hashval%(fx->n_slots);
    unsigned int chain_width=(hashval%(fx->n_slots-2))+1;
    unsigned int *offsets=fx->offsets;
    unsigned int keypos=
      ((offsets) ? (offget(offsets,probe)) : (get_offset(fx,probe)));
    while (keypos) {
      fdtype thiskey; unsigned int n_vals; off_t val_start;
      fd_setpos(stream,keypos+(fx->n_slots)*4);
      n_vals=fd_dtsread_4bytes(stream); val_start=fd_dtsread_4bytes(stream);
      thiskey=zread_key(stream,fx->slotids,fx->baseoids,fx->n_baseoids);
      if (FDTYPE_EQUAL(key,thiskey)) {
	u8_unlock_mutex(&(fx->lock));
	return n_vals;}
      else if (n_probes>256) {
	u8_unlock_mutex(&fx->lock);
	return fd_err(fd_FileIndexOverflow,"zindex_fetchsize",
		      fx->source,FD_VOID);}
      else {n_probes++; probe=(probe+chain_width)%(fx->n_slots);}}
    u8_unlock_mutex(&fx->lock);
    return FD_EMPTY_CHOICE;}
}

/* Fetching sizes */

static int sort_offsets(const void *vox,const void *voy)
{
  const unsigned int *ox=vox, *oy=voy;
  if (*ox<*oy) return -1;
  else if (*ox>*oy) return 1;
  else return 0;
}

static fdtype zindex_fetchkeys(fd_index ix)
{
  fdtype result=FD_EMPTY_CHOICE;
  struct FD_ZINDEX *fx=(struct FD_ZINDEX *)ix;
  struct FD_DTYPE_STREAM *stream=&(fx->stream);
  unsigned int n_slots, i=0, *offsets, pos_offset;
  u8_lock_mutex(&(fx->lock));
  n_slots=fx->n_slots; offsets=u8_malloc(SLOTSIZE*n_slots);
  pos_offset=4*n_slots;
  fd_setpos(&(fx->stream),8);
  fd_dtsread_ints(&(fx->stream),fx->n_slots,offsets);
  qsort(offsets,fx->n_slots,SLOTSIZE,sort_offsets);
  while (i < n_slots)
    if (offsets[i]) {
      fdtype key;
      fd_setpos(stream,4*n_slots+offsets[i]);
      fd_dtsread_zint(&(fx->stream)); fd_dtsread_zint(&(fx->stream));
      key=zread_key(&(fx->stream),fx->slotids,fx->baseoids,fx->n_baseoids);
      FD_ADD_TO_CHOICE(result,key);
      i++;}
    else i++;
  u8_unlock_mutex(&(fx->lock));
  u8_free(offsets);
  return fd_simplify_choice(result);
}

static fdtype zindex_fetchsizes(fd_index ix)
{
  fdtype result=FD_EMPTY_CHOICE;
  struct FD_ZINDEX *fx=(struct FD_ZINDEX *)ix;
  struct FD_DTYPE_STREAM *stream=&(fx->stream);
  unsigned int n_slots, i=0, *offsets, pos_offset;
  u8_lock_mutex(&(fx->lock));
  n_slots=fx->n_slots; offsets=u8_malloc(SLOTSIZE*n_slots);
  pos_offset=4*n_slots;
  fd_setpos(&(fx->stream),8);
  fd_dtsread_ints(&(fx->stream),fx->n_slots,offsets);
  qsort(offsets,fx->n_slots,SLOTSIZE,sort_offsets);
  while (i < n_slots)
    if (offsets[i]) {
      fdtype key, pair; int size, vpos;
      fd_setpos(stream,4*n_slots+offsets[i]);
      size=fd_dtsread_zint(&(fx->stream)); vpos=fd_dtsread_zint(&(fx->stream));
      key=zread_key(&(fx->stream),fx->slotids,fx->baseoids,fx->n_baseoids);
      pair=fd_init_pair(NULL,key,FD_INT2DTYPE(size));
      FD_ADD_TO_CHOICE(result,pair);
      i++;}
    else i++;
  u8_unlock_mutex(&(fx->lock));
  u8_free(offsets);
  return fd_simplify_choice(result);
}

/* Fetch N */

/* In the following structures,
     key: is the key being sought;
     index: is the position in the values vector where the value will be 
      written; this is negative until the key has been found, and zero
      after the value is complete.  The negative value is actually -(index+1)
      to distinguish the zeroth entry.
     filepos: is the next file position to look at; this is negative when
      the lookup is done.
     probe: is the slot in the hashtable being considered; when negative,
      the offset of the key entry is being sought (this only makes sense
      when the index offsets are NULL).
     chain_width: how big a step to take when trying new buckets.
*/

struct FETCH_SCHEDULE {
  fdtype key; int index; off_t filepos;
  int probe, chain_width;};
struct KEY_FETCH_SCHEDULE {
  fdtype key; int index; off_t filepos;
  int probe, chain_width;};
struct VALUE_FETCH_SCHEDULE {
  fdtype key; int index;  off_t filepos;
  int probe, n_values;};
union SCHEDULE {
  struct FETCH_SCHEDULE fs;
  struct KEY_FETCH_SCHEDULE ks;
  struct VALUE_FETCH_SCHEDULE vs;};

static int sort_by_filepos(const void *xp,const void *yp)
{
  struct FETCH_SCHEDULE *xs=(struct FETCH_SCHEDULE *)xp;
  struct FETCH_SCHEDULE *ys=(struct FETCH_SCHEDULE *)yp;
  if (xs->filepos<=0)
    if (ys->filepos<=0) return 0; else return 1;
  else if (ys->filepos<=0) return -1;
  else if (xs->filepos<ys->filepos) return -1;
  else if (xs->filepos>ys->filepos) return 1;
  else return 0;
}

static int run_schedule(struct FD_ZINDEX *fx,int n,
			union SCHEDULE *schedule,
			unsigned int *offsets,
			fdtype *values)
{
  unsigned i=0, pos_offset=fx->n_slots*4;
  while (i < n) {
    if (schedule[i].fs.filepos<=0) return i;
    if ((offsets == NULL) && (schedule[i].fs.probe<0)) {
      /* When the probe offset is negative, it means we are reading
	 the offset itself. */
      fd_setpos(&(fx->stream),schedule[i].fs.filepos);
      schedule[i].fs.filepos=fd_dtsread_4bytes(&(fx->stream));
      if (schedule[i].fs.filepos) 
	schedule[i].fs.probe=-(schedule[i].fs.probe)-1;
      else {
	/* The key has no key entry, thus no values.  Morph it into
	   a completed value entry. */
	struct VALUE_FETCH_SCHEDULE *vs=&(schedule[i].vs);
	int index=-(vs->index)-1;
	vs->probe=-(vs->probe)-1; 
	if (FD_VOIDP(values[index]))
	  values[index]=FD_EMPTY_CHOICE;
	vs->filepos=-1; vs->index=index;
	vs->n_values=0;}}
    else if (schedule[i].fs.index<0) { /* Still looking for the key */
      unsigned int n_values; off_t vpos; fdtype key;
      struct KEY_FETCH_SCHEDULE *ks=
	(struct KEY_FETCH_SCHEDULE *)(&(schedule[i]));
      /* Go to the key location and read the keydata */
      fd_setpos(&(fx->stream),schedule[i].fs.filepos+pos_offset);
      n_values=fd_dtsread_zint(&(fx->stream));
      vpos=(off_t)fd_dtsread_zint(&(fx->stream));
      key=zread_key(&(fx->stream),fx->slotids,fx->baseoids,fx->n_baseoids);
      if (FD_ABORTP(key)) return fd_interr(key);
      else if (FDTYPE_EQUAL(key,schedule[i].fs.key)) {
	/* If you found the key, morph the entry into a value
	   fetching entry. */
	struct VALUE_FETCH_SCHEDULE *vs=(struct VALUE_FETCH_SCHEDULE *)ks;
	/* Make the index positive, indicating that you are collecting
	   the value now. */
	unsigned int index=vs->index=-(vs->index)-1;
	vs->filepos=vpos+pos_offset; vs->n_values=n_values;
	fd_decref(key); /* No longer needed */
	if (n_values>1)
	  /* If there are multiple vlues, initialize an achoice. */
	  if (FD_VOIDP(values[index]))
	    values[index]=fd_init_achoice(NULL,n_values,0);
	  else {
	    fdtype val=values[index];
	    values[index]=fd_init_achoice(NULL,n_values,0);
	    FD_ADD_TO_CHOICE(values[index],val);}
	else if (n_values==0) {
	  /* If there are no values, store the empty choice and
	     declare the entry done by setting its filepos to -1. */
	  vs->filepos=-1;
	  values[index]=FD_EMPTY_CHOICE;}
	else if (FD_VOIDP(values[index]))
	  /* If there's one value, just initialize the entry. */
	  values[index]=FD_EMPTY_CHOICE;}
      else {
	/* Keep looking for the key */
	struct KEY_FETCH_SCHEDULE *ks=
	  (struct KEY_FETCH_SCHEDULE *)(&(schedule[i]));
	ks->probe=(ks->probe+ks->chain_width)%(fx->n_slots);
	fd_decref(key); /* No longer needed */
	if (offsets==NULL) {
	  ks->filepos=(ks->probe*4)+8; ks->probe=-(ks->probe+1);}
	else if (offsets[ks->probe]) 
	  ks->filepos=offget(offsets,ks->probe);
	else {
	  int index=-(ks->index)-1;
	  if (FD_VOIDP(values[index]))
	    values[index]=FD_EMPTY_CHOICE;
	  ks->index=0;
	  ks->filepos=0;}}}
    else {
      struct VALUE_FETCH_SCHEDULE *vs=
	(struct VALUE_FETCH_SCHEDULE *)(&(schedule[i]));
      off_t vpos=vs->filepos; fdtype val; int next=1;
      int index=vs->index;
      fd_setpos(&(fx->stream),vpos);
      while (next==1) {
	int i=0, n_values=fd_dtsread_zint(&(fx->stream));
	while (i < n_values) {
	  fdtype val=zread_value(&(fx->stream),fx->baseoids,fx->n_baseoids);
	  if (FD_ABORTP(val)) return fd_interr(val);
	  FD_ADD_TO_CHOICE(values[index],val); i++;}
	next=fd_dtsread_zint(&(fx->stream));}
      vs->filepos=next;
      if (next==0) vs->index=0;}
    i++;}
  return n;
}

static fdtype *fetchn(struct FD_ZINDEX *fx,int n,fdtype *keys,int lock_adds)
{
  unsigned int *offsets=fx->offsets, pos_offset=4*fx->n_slots;
  union SCHEDULE *schedule=u8_malloc(sizeof(union SCHEDULE)*n);
  fdtype *values=u8_malloc(sizeof(fdtype)*n);
  int i=0, schedule_size=0, init_schedule_size; while (i < n) {
    fdtype key=keys[i], cached=fd_hashtable_get(&(fx->cache),key,FD_VOID);
    if (FD_VOIDP(cached)) {
      struct KEY_FETCH_SCHEDULE *ksched=
	(struct KEY_FETCH_SCHEDULE *)&(schedule[schedule_size]);
      int hashcode=
	((fx->hashv) ? (fd_hash_dtype2(key)) : (fd_hash_dtype1(key)));
      int probe=hashcode%(fx->n_slots);
      ksched->key=key; ksched->index=-(i+1);
      if (offsets) {
	ksched->filepos=offget(offsets,probe);
	ksched->probe=probe;}
      else {
	ksched->filepos=8+probe*4;
	ksched->probe=(-probe);}
      ksched->chain_width=hashcode%(fx->n_slots-2)+1;
      if (ksched->filepos)
	if (lock_adds)
	  values[i]=fd_hashtable_get(&(fx->adds),key,FD_VOID);
	else values[i]=fd_hashtable_get_nolock(&(fx->adds),key,FD_VOID);
      else if (lock_adds)
	values[i]=fd_hashtable_get(&(fx->adds),key,FD_EMPTY_CHOICE);
      else values[i]=fd_hashtable_get_nolock(&(fx->adds),key,FD_EMPTY_CHOICE);
      i++; schedule_size++;}
    else  values[i++]=cached;}
  init_schedule_size=schedule_size;
  qsort(schedule,schedule_size,sizeof(struct FETCH_SCHEDULE),sort_by_filepos);
  while (schedule_size>0) {
    schedule_size=run_schedule(fx,schedule_size,schedule,offsets,values);
    if (schedule_size<0) break;
    qsort(schedule,schedule_size,sizeof(struct FETCH_SCHEDULE),sort_by_filepos);}
  if (schedule_size<0) {
    int k=0; while (k<init_schedule_size) {
      if (schedule[k].fs.filepos<0)
	fd_decref(values[schedule[k].fs.index]);
      k++;}
    u8_free(schedule);
    u8_free(values);
    return NULL;}
  u8_free(schedule);
  /* Note that we should now look at fx->edits and integrate any changes,
     but we're not doing that now. */
  return values;
}

static fdtype *zindex_fetchn(fd_index ix,int n,fdtype *keys)
{
  struct FD_ZINDEX *fx=(struct FD_ZINDEX *)ix;
  fdtype *results;
  u8_lock_mutex(&(fx->lock));
  results=fetchn(fx,n,keys,1);
  u8_unlock_mutex(&(fx->lock));
  return results;
}

/* Committing indices */

/* This is more complicated than you might think because the structure
   is optimized for adding new values to a key.  So when we commit, we have
   to go to the file to find what current values are currently there
   (or at least where they are stored in the file).

   This is made even hairier because we're trying to organize file access
   optimally by ordering seeks and doing multiple passes.  The most common use
   case is that we are storing lots of keys, so we don't want to just iterate
   over all of the keys and do a bunch of seeks for each one.  

   One good side effect of all this hair is that we write all the data
    to the file before we write the offsets, meaning that if it fails before
   that point, the file remains consistent.
*/

/* Utility functions and structures */

/* This is used to organize fetching and writing a particular key.
   A negative value for chain_width indicates that the entry is completed.
   A negative value for slotno indicates that we are fetching the
     offset for that slot (in the case where we don't have cached offsets).
*/
struct KEYDATA {
  fdtype key; int serial, slotno, chain_width, n_values;
  off_t pos;};

/* This is used to track which slotnos are newly filled when we are writing
    a file index without a vector of cached offsets. */
struct RESERVATIONS {
  unsigned int *slotnos;
  int n_reservations, max_reservations;};

static int sort_keydata(const void *kvx,const void *kvy)
{
  const struct KEYDATA *x=kvx, *y=kvy;
  if (x->chain_width<0)
    if (y->chain_width>0) return 1; else return 0;
  else if (y->chain_width<0) return -1;
  else if (x->pos < y->pos) return -1;
  else if (x->pos > y->pos) return 1;
  else return 0;
}

static int sort_keydata_pos(const void *kvx,const void *kvy)
{
  const struct KEYDATA *x=kvx, *y=kvy;
  if (x->pos < y->pos) return -1;
  else if (x->pos > y->pos) return 1;
  else return 0;
}

static int sort_keydata_serial(const void *kvx,const void *kvy)
{
  const struct KEYDATA *x=kvx, *y=kvy;
  if (x->serial < y->serial) return -1;
  else if (x->serial > y->serial) return 1;
  else return 0;
}

static int reserve_slotno(struct RESERVATIONS *r,unsigned int slotno)
{
  unsigned int *slotnos=r->slotnos, *lim=r->slotnos+r->n_reservations;
  unsigned int *bottom=slotnos, *top=lim;
  unsigned int *middle, insertoff, *insertpos;
  if (r->n_reservations==0) {
    bottom[0]=slotno; r->n_reservations++;
    return 1;}
  else while (bottom<=top) {
    middle=bottom+(top-bottom)/2;
    if ((middle<slotnos) || (middle>=lim)) break;
    else if (*middle==slotno) return 0;
    else if (slotno>*middle) bottom=middle+1;
    else top=middle-1;}
  insertoff=(middle-slotnos);
  if ((middle<lim) && (slotno>*middle)) insertoff++;
  if (r->n_reservations == r->max_reservations) {
    int new_max=r->max_reservations*2;
    slotnos=r->slotnos=u8_realloc(r->slotnos,SLOTSIZE*new_max);
    r->max_reservations=new_max;}
  insertpos=slotnos+insertoff;
  if (!(((insertpos<=slotnos) || (slotno>insertpos[-1])) &&
	((insertpos>=(slotnos+r->n_reservations)) || (slotno<insertpos[0]))))
    u8_warn(fd_FileIndexError,"Corrupt reservations table when saving index");
  if (insertoff<r->n_reservations)
    memmove(insertpos+1,insertpos,(SLOTSIZE*(r->n_reservations-insertoff)));
  *insertpos=slotno;
  r->n_reservations++;
  return 1;
}

/* Fetching keydata:
    This finds where all the keys are, reserving slots for them if neccessary.
    It also fetches the current number of values and the valuepos. */
/* Fetching keydata:
    This finds where all the keys are, reserving slots for them if neccessary.
    It also fetches the current number of values and the valuepos. */
static int fetch_keydata(struct FD_ZINDEX *fx,struct KEYDATA *kdata,int n)
{
  struct RESERVATIONS reserved;
  struct FD_DTYPE_STREAM *stream=&(fx->stream);
  unsigned int *offsets=fx->offsets, pos_offset=fx->n_slots*4, chain_length=0;
  int i=0, max=n, new_keys=0;
  if (offsets == NULL) {
    reserved.slotnos=u8_malloc(SLOTSIZE*64);
    reserved.n_reservations=0; reserved.max_reservations=64;}
  /* Setup the key data */
  while (i < n) {
    fdtype key=kdata[i].key;
    int hash=((fx->hashv) ? (fd_hash_dtype2(key)) : (fd_hash_dtype1(key)));
    int probe=hash%(fx->n_slots), chain_width=hash%(fx->n_slots-2)+1;
    if (offsets) {
      int koff=offget(offsets,probe);
      /* Skip over all the reserved slots */
      while (koff==1) {
	probe=(probe+chain_width)%(fx->n_slots);
	koff=offget(offsets,probe);}
      if (koff) {
	/* We found a full slot, queue it for examination. */
	kdata[i].slotno=probe; kdata[i].chain_width=chain_width;
	kdata[i].pos=offget(offsets,probe)+pos_offset;}
      else {
	/* We have an empty slot we can fill */
	set_offset(offsets,probe,1); new_keys++; /* Fill it */
	kdata[i].slotno=probe;
	kdata[i].chain_width=-1; /* Declare it found */
	/* We initialize .n_values to zero unless it has already been 
	   initialized.  This would be the case if the key's value was edited
	   (e.g. had the value set or values dropped), which means that the
	   values were already written. */
	if (kdata[i].n_values<0) kdata[i].n_values=0;
	kdata[i].pos=0;}}
    else {
      /* A negative probe value means that we are getting an offset
	 from the offset table. */
      kdata[i].slotno=-(probe+1); kdata[i].chain_width=chain_width;
      kdata[i].pos=8+SLOTSIZE*probe;}
    i++;}
  /* Now, collect keydata */
  while (max>0) {
    /* Sort by filepos for more coherent and hopefully faster disk access.
       We use a negative chain width to indicate that we're done with the entry. */
    qsort(kdata,max,sizeof(struct KEYDATA),sort_keydata);
    i=0; while (i<max) {
      if (kdata[i].chain_width<0) break;
      fd_setpos(stream,kdata[i].pos);
      if (kdata[i].slotno<0) { /* fetching offset */
	unsigned int off=fd_dtsread_4bytes(stream), slotno=((-kdata[i].slotno)-1);
	if (off) {
	  kdata[i].slotno=slotno;
	  kdata[i].pos=off+SLOTSIZE*(fx->n_slots);}
	else if (reserve_slotno(&reserved,slotno)) {
	  kdata[i].slotno=slotno; new_keys++;
	  kdata[i].chain_width=-1; kdata[i].pos=0;
	  /* We initialize .n_values to zero unless it has already been 
	     initialized.  This would be the case if the key's value was edited
	     (e.g. had the value set or values dropped), which means that the
	     values were already written. */
	  if (kdata[i].n_values<0) kdata[i].n_values=0;}
	else {
	  int next_probe=(slotno+kdata[i].chain_width)%(fx->n_slots);
	  kdata[i].slotno=-(next_probe+1);
	  kdata[i].pos=8+SLOTSIZE*next_probe;}
	i++;}
      else {
	unsigned int n_vals, vpos; fdtype key;
	n_vals=fd_dtsread_4bytes(stream);
	vpos=fd_dtsread_4bytes(stream);
	key=zread_key(stream,fx->slotids,fx->baseoids,fx->n_baseoids);
	if (FDTYPE_EQUAL(key,kdata[i].key)) {
	  kdata[i].pos=vpos; kdata[i].chain_width=-1;
	  if (kdata[i].n_values<0) kdata[i].n_values=n_vals;}
	else if (offsets) {
	  int next_probe=(kdata[i].slotno+kdata[i].chain_width)%(fx->n_slots);
	  /* Compute the next probe location, skipping slots
	     already taken by keys being dumped for the first time,
	     which is indicated by an offset value of 1. */
	  while ((offsets[next_probe]) && (offget(offsets,next_probe)==1))
	    next_probe=(next_probe+kdata[i].chain_width)%(fx->n_slots);
	  if (offsets[next_probe]) {
	    /* If we have an offset, it is a key on disk that we need
	       to look at. */
	    kdata[i].slotno=next_probe;
	    kdata[i].pos=offget(offsets,next_probe)+pos_offset;}
	  else {
	    /* Otherwise, we have an empty slot we can put this value in. */
	    new_keys++;
	    kdata[i].slotno=next_probe;
	    kdata[i].pos=0;
	    kdata[i].n_values=0;
	    set_offset(offsets,next_probe,1);
	    kdata[i].chain_width=-1;}}
	else {
	  int next_probe=(kdata[i].slotno+kdata[i].chain_width)%(fx->n_slots);
	  kdata[i].slotno=-(next_probe+1);
	  kdata[i].pos=8+(SLOTSIZE*next_probe);}
	fd_decref(key);
	i++;}}
    if (max==i)
      if (chain_length>256) {
	if (offsets == NULL) u8_free(reserved.slotnos);
	return fd_reterr(fd_FileIndexOverflow,"fetch_keydata",u8_strdup(fx->cid),FD_VOID);}
      else chain_length++;
    else chain_length=0;
    max=i;}
  if (offsets == NULL) u8_free(reserved.slotnos);
  return new_keys;
}

/* Committing edits */

/* Committing drops and sets are different from simple adds.
   For drops, we actually need to get the current value from disk
    in order to compute a new value to write altogether.
   For both sets and drops, because we are writing a whole value,
    we don't actually need to get the current value position
    (though we ended up doing so for the drops). */

/* This extends the KEYDATA vector with entries for the keys
   which are being set or having values dropped.  In either case,
   we end up storing the complete new value in the KEYDATA struct. */
static int commit_edits(struct FD_ZINDEX *f,struct KEYDATA *kdata)
{
  struct FD_DTYPE_STREAM *stream=&(f->stream);
  int i=0, n_edits=0, n_drops=0; off_t filepos;
  fdtype *dropkeys, *dropvals;
  struct FD_HASHENTRY **scan, **limit;
  if (f->edits.n_keys==0) return 0;
  dropkeys=u8_malloc(sizeof(fdtype)*f->edits.n_keys);
  scan=f->edits.slots; limit=scan+f->edits.n_slots;
  while (scan < limit)
    if (*scan) {
      /* Now we go through the edits table, finding all the drops.
	 We need to retrieve their values on disk in order to write
	 out a new value. */
      struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
      struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
      while (kvscan<kvlimit) {
	fdtype key=kvscan->key;
	if ((FD_PAIRP(key)) &&
	    (FD_EQ(FD_CAR(key),drop_symbol)) &&
	    (!(FD_VOIDP(kvscan->value)))) {
	  fdtype cached=fd_hashtable_get(&(f->cache),FD_CDR(key),FD_VOID);
	  if (!(FD_VOIDP(cached))) {
	    /* If the value of the key is cached, it will be up to date with
	       these drops, so we just convert the key to a "set" key
	       and store the cached value there.  Note that this breaks the
	       hashtable, but it doesn't matter because we're going to reset
	       it anyway. */
	    struct FD_PAIR *pair=
	      FD_GET_CONS(key,fd_pair_type,struct FD_PAIR *);
	    fd_decref(kvscan->value); kvscan->value=cached;
	    pair->car=set_symbol;}
	  else dropkeys[n_drops++]=FD_CDR(key);}
	kvscan++;}
      scan++;}
    else scan++;
  if (n_drops) dropvals=fetchn(f,n_drops,dropkeys,0);
  filepos=fd_endpos(stream);
  scan=f->edits.slots; limit=scan+f->edits.n_slots;
  while (scan < limit)
    if (*scan) {
      struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
      struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
      while (kvscan<kvlimit) {
	fdtype key=kvscan->key;
	if (FD_VOIDP(kvscan->value)) kvscan++;
	else if (FD_PAIRP(key)) {
	  kdata[n_edits].key=FD_CDR(key); kdata[n_edits].pos=filepos;
	  if (FD_EQ(FD_CAR(key),set_symbol)) {
	    /* If it's a set edit, just write out the whole thing */
	    filepos=filepos+zwrite_values(stream,kvscan->value,
					  f->baseoids,f->n_baseoids,
					  0,&(kdata[n_edits].n_values));}
	  else if (FD_EQ(FD_CAR(key),drop_symbol)) {
	    /* If it's a drop edit, you got the value, so compute
	       the difference and write that out.*/
	    fdtype new_value=fd_difference(dropvals[i],kvscan->value);
	    filepos=filepos+zwrite_values(stream,new_value,
					  f->baseoids,f->n_baseoids,
					  0,&(kdata[n_edits].n_values));
	    fd_decref(new_value);}
	  n_edits++; kvscan++;}
	else kvscan++;}
      scan++;}
    else scan++;
  if (n_drops) {
    i=0; while (i<n_drops) {fd_decref(dropvals[i]); i++;}
    u8_free(dropkeys); u8_free(dropvals);}
  return n_edits;
}

static void write_keys(struct FD_ZINDEX *fx,int n,struct KEYDATA *kdata)
{
  unsigned int *offsets=fx->offsets, pos_offset=fx->n_slots*4;
  FD_OID *baseoids=fx->baseoids; int n_baseoids=fx->n_baseoids;
  fdtype slotids=fx->slotids;
  struct FD_DTYPE_STREAM *stream=&(fx->stream);
  off_t pos=fd_endpos(stream);
  int i=0; while (i<n) {
    off_t kpos=pos;
    pos=pos+fd_dtswrite_zint(stream,kdata[i].n_values);
    pos=pos+fd_dtswrite_zint(stream,(unsigned int)kdata[i].pos);
    pos=pos+zwrite_key(stream,kdata[i].key,slotids,baseoids,n_baseoids)+8;
    if (offsets)
      set_offset(offsets,kdata[i].slotno,(kpos-pos_offset));
    else kdata[i].pos=(kpos-pos_offset);
    i++;}
}     

static void write_offsets(struct FD_ZINDEX *fx,int n,struct KEYDATA *kdata)
{
  struct FD_DTYPE_STREAM *stream=&(fx->stream);
  unsigned int *offsets=fx->offsets;
  if (offsets) {
    fd_setpos(stream,8);
    fd_dtswrite_ints(stream,fx->n_slots,offsets);
    fd_dtsflush(stream);}
  else {
    int i=0;
    qsort(kdata,n,sizeof(struct KEYDATA),sort_keydata_pos);
    while (i < n) {
      fd_setpos(stream,8+kdata[i].slotno*4);
      fd_dtswrite_4bytes(stream,(unsigned int)kdata[i].pos);
      i++;}
    fd_dtsflush(stream);}
}

/* Putting it all together */

static int zindex_commit(struct FD_INDEX *ix)
{
  struct FD_ZINDEX *fx=(struct FD_ZINDEX *)ix;
  struct FD_DTYPE_STREAM *stream=&(fx->stream);
  int pos_offset=fx->n_slots*4, newcount;
  u8_lock_mutex(&(ix->adds.lock));
  u8_lock_mutex(&(ix->edits.lock));
  u8_lock_mutex(&(fx->lock));
  fd_dts_start_write(stream);
#if HAVE_MMAP
  if (fx->offsets) {
    int retval=munmap(fx->offsets-2,(SLOTSIZE*fx->n_slots)+8);
    unsigned int *newmmap;
    if (retval<0) {
      u8_warn(u8_strerror(errno),"zindex_commit:munnmap %s",fx->source);
      fx->offsets=NULL; errno=0;}
    newmmap=
      mmap(NULL,(fx->n_slots*SLOTSIZE)+8,
	   PROT_READ|PROT_WRITE,MMAP_FLAGS,
	   stream->fd,0);
    if ((newmmap==NULL) || (newmmap==((void *)-1))) {
      u8_warn(u8_strerror(errno),"zindex_commit:mmap %s",fx->source);
      fx->offsets=NULL; errno=0;}
    else fx->offsets=newmmap+2;}
#endif
  {
    off_t filepos;
    int n_adds=ix->adds.n_keys, n_edits=ix->edits.n_keys;
    int i=0, n=0, n_changes=n_adds+n_edits, add_index;
    struct KEYDATA *kdata=u8_malloc(sizeof(struct KEYDATA)*n_changes);
    unsigned int *value_locs=
      ((n_edits) ? (u8_malloc(sizeof(unsigned int)*n_edits)) : (NULL));
    struct FD_HASHENTRY **scan=ix->adds.slots, **limit=scan+ix->adds.n_slots;
    while (scan < limit)
      if (*scan) {
	struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
	struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
	while (kvscan<kvlimit) {
	  kdata[n].key=kvscan->key;
	  /* We'll use this to sort back into the order of the adds table */
	  kdata[n].serial=n; 
	  kdata[n].n_values=-1;
	  kdata[n].slotno=-1;
	  kdata[n].pos=-1;
	  n++; kvscan++;}
	scan++;}
      else scan++;
    /* add_index is the point were key entries for simple additions and and
       key entries for edits begin. */
    add_index=n;
    n=n+commit_edits(fx,kdata+n);
    i=add_index; while (i<n) {kdata[i].serial=i; i++;}
    /* Copy the value locations recorded by commit_edits into
       value_locs.  (The .pos field will be used by fetch_keydata). */
    i=add_index; while (i<n) {
      kdata[i].serial=i; value_locs[i-add_index]=kdata[i].pos;
      i++;}
    newcount=fetch_keydata(fx,kdata,n);
    if (newcount<0) {
      u8_free(kdata);
      if (value_locs) u8_free(value_locs);
      u8_unlock_mutex(&(ix->adds.lock));
      u8_unlock_mutex(&(ix->edits.lock));
      u8_unlock_mutex(&(fx->lock));
      return newcount;}
    filepos=fd_endpos(stream);
    qsort(kdata,n,sizeof(struct KEYDATA),sort_keydata_serial);
    /* Copy back the value locations written by commit_edits. */
    i=add_index; while (i<n) {
      if (value_locs[i-add_index])
	kdata[i].pos=((off_t)(value_locs[i-add_index]-pos_offset));
      else kdata[i].pos=((off_t)0);
      i++;}
    /* Now, scan the adds again and write the added values. */
    scan=ix->adds.slots; limit=scan+ix->adds.n_slots;
    i=0; while (scan < limit)
      if (*scan) {
	struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
	struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
	while (kvscan<kvlimit) {
	  off_t writepos=filepos; int new_values;
	  filepos=filepos+zwrite_values(&(fx->stream),kvscan->value,
					fx->baseoids,fx->n_baseoids,
					kdata[i].pos,&new_values);
	  kdata[i].pos=writepos-pos_offset;
	  kdata[i].n_values=kdata[i].n_values+new_values;
	  i++; kvscan++;}
	scan++;}
      else scan++;
    write_keys(fx,n,kdata);
#if HAVE_MMAP
    if (fx->offsets) {
      int retval=munmap(fx->offsets-2,(SLOTSIZE*fx->n_slots)+8);
      unsigned int *newmmap;
      if (retval<0) {
	u8_warn(u8_strerror(errno),"zindex_commit:munmap %s",fx->source);
	fx->offsets=NULL; errno=0;}
      newmmap=
	mmap(NULL,(fx->n_slots*SLOTSIZE)+8,
	     PROT_READ,MMAP_FLAGS,stream->fd,0);
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
	u8_warn(u8_strerror(errno),"zindex_commit:mmap %s",fx->source);
	fx->offsets=NULL; errno=0;}
      else fx->offsets=newmmap+2;}
    else write_offsets(fx,n,kdata);
#else
    write_offsets(fx,n,kdata);
#endif
    fd_dtsflush(stream);
    fsync(stream->fd);
    u8_unlock_mutex(&(fx->lock));
    fd_reset_hashtable(&(ix->adds),67,0);
    u8_unlock_mutex(&(ix->adds.lock));
    fd_reset_hashtable(&(ix->edits),67,0);
    u8_unlock_mutex(&(ix->edits.lock));
    return n;}
}

static void zindex_close(fd_index ix)
{
  struct FD_ZINDEX *fx=(struct FD_ZINDEX *)ix;
  u8_lock_mutex(&(fx->lock));
  fd_dtsclose(&(fx->stream),1);
  if (fx->offsets) {
#if HAVE_MMAP
    int retval=munmap(fx->offsets-2,(SLOTSIZE*fx->n_slots)+8);
    if (retval<0) {
      u8_warn(u8_strerror(errno),"zindex_close:munnmap %s",fx->source);
      fx->offsets=NULL; errno=0;}
    else fx->offsets=NULL;
#else
    u8_free(fx->offsets); fx->offsets=NULL;
#endif
    fx->cache_level=-1;}
  u8_unlock_mutex(&(fx->lock));
}

static void zindex_setbuf(fd_index ix,int bufsiz)
{
  struct FD_ZINDEX *fx=(struct FD_ZINDEX *)ix;
  u8_lock_mutex(&(fx->lock));
  fd_dtsbufsize(&(fx->stream),bufsiz);
  u8_unlock_mutex(&(fx->lock));
}

static fdtype zindex_metadata(fd_index ix,fdtype md)
{
  struct FD_ZINDEX *fx=(struct FD_ZINDEX *)ix;
  if (FD_VOIDP(md))
    return fd_read_index_metadata(&(fx->stream));
  else return fd_write_index_metadata((&(fx->stream)),md);
}


/* The handler struct */

static struct FD_INDEX_HANDLER zindex_handler={
  "zindex", 1, sizeof(struct FD_ZINDEX), 12,
  zindex_close, /* close */
  zindex_commit, /* commit */
  zindex_setcache, /* setcache */
  zindex_setbuf, /* setbuf */
  zindex_fetch, /* fetch */
  zindex_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  zindex_fetchn, /* fetchn */
  zindex_fetchkeys, /* fetchkeys */
  zindex_fetchsizes, /* fetchsizes */
  zindex_metadata,
  NULL /* sync */
};

FD_EXPORT fd_init_zindices_c()
{
  fd_register_source_file(versionid);

  set_symbol=fd_intern("SET");
  drop_symbol=fd_intern("DROP");
  fd_register_index_opener(FD_ZINDEX_MAGIC_NUMBER,open_zindex);
}


/* The CVS log for this file
   $Log: zindices.c,v $
   Revision 1.63  2006/03/15 16:33:23  haase
   Fixed bug with negative zero pun in file/zindex fetch and commit

   Revision 1.62  2006/02/07 16:02:59  haase
   Fixed bug in fetchn with caching

   Revision 1.61  2006/01/31 13:47:23  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.60  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.59  2006/01/16 17:58:07  haase
   Fixes to empty choice cases for indices and better error handling

   Revision 1.58  2006/01/16 16:14:19  haase
   Fix to handling of existing but empty keys in indices

   Revision 1.57  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.56  2005/12/30 18:35:53  haase
   Minor whitespace changes

   Revision 1.55  2005/12/28 23:03:29  haase
   Made choices be direct blocks of elements, including various fixes, simplifications, and more detailed documentation.

   Revision 1.54  2005/12/20 00:54:23  haase
   Fixed bug in slotno reservation for file indices

   Revision 1.53  2005/12/19 00:47:22  haase
   Cleaned up leak in fetchkeys

   Revision 1.52  2005/12/13 19:22:07  haase
   Fixed recursive lock error on file indices

   Revision 1.51  2005/12/12 16:59:00  haase
   Moved lock point for file index commits

   Revision 1.50  2005/11/29 17:53:25  haase
   Catch file index overflows and 0/1 cases of index getkeys and getsizes

   Revision 1.49  2005/11/11 04:31:17  haase
   Fixed bug with setting keys to no values by storing or dropping

   Revision 1.48  2005/10/30 04:13:19  haase
   Fixed bug in index dropping

   Revision 1.47  2005/08/18 17:03:09  haase
   Fixed some incremental change bugs in file indices

   Revision 1.46  2005/08/11 12:41:48  haase
   Fixed bug with zkeys in zindices

   Revision 1.45  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.44  2005/08/07 20:50:34  haase
   Fixed bug where fetchn ignored local adds

   Revision 1.43  2005/06/18 23:22:38  haase
   Fixed some subtle index bugs

   Revision 1.42  2005/06/05 04:21:02  haase
   Further fixes to index commitment

   Revision 1.41  2005/06/05 03:42:28  haase
   Index simplifications and bug fixes for store/add combinations

   Revision 1.40  2005/06/04 12:43:25  haase
   Fix error in saving indices with mixed adds and stores

   Revision 1.39  2005/05/26 12:41:11  haase
   More return value error handling fixes

   Revision 1.38  2005/05/25 18:05:19  haase
   Check for -1 return value from mmap as well as NULL

   Revision 1.37  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.36  2005/04/26 01:22:49  haase
   Fixed bulk fetching bug in zindices and fileindices

   Revision 1.35  2005/04/16 19:23:32  haase
   Fixed some locking errors

   Revision 1.34  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.33  2005/04/12 20:42:45  haase
   Used #define FD_FILEDB_BUFSIZE to set default buffer size (initially 256K)

   Revision 1.32  2005/04/06 18:31:51  haase
   Fixed mmap error calls to produce warnings rather than raising errors and to use u8_strerror to get the condition name

   Revision 1.31  2005/03/30 15:30:00  haase
   Made calls to new seterr do appropriate strdups

   Revision 1.30  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.29  2005/03/29 04:12:36  haase
   Added pool/index making primitives

   Revision 1.28  2005/03/28 19:19:36  haase
   Added metadata reading and writing and file pool/index creation

   Revision 1.27  2005/03/26 04:46:58  haase
   Added fd_index_sizes

   Revision 1.26  2005/03/25 19:49:47  haase
   Removed base library for eframerd, deferring to libu8

   Revision 1.25  2005/03/18 02:27:33  haase
   Various file pool and index fixes

   Revision 1.24  2005/03/07 19:19:54  haase
   Fixes to fileindices and zindices

   Revision 1.23  2005/03/07 14:17:34  haase
   Patched leak in key reading and added file index consistency checks

   Revision 1.22  2005/03/06 18:28:21  haase
   Added timeprims

   Revision 1.21  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.20  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.19  2005/03/03 17:58:15  haase
   Moved stdio dependencies out of fddb and reorganized make structure

   Revision 1.18  2005/03/01 23:13:54  haase
   Fixes to index commitment implementation

   Revision 1.17  2005/03/01 19:40:23  haase
   Fixed commitment issues in file indices

   Revision 1.16  2005/02/27 03:01:35  haase
   Fixes to make index commitment work

   Revision 1.15  2005/02/26 22:31:41  haase
   Remodularized choice and oid add into xtables.c

   Revision 1.14  2005/02/25 19:47:46  haase
   Fixed commitment of cached file indices

   Revision 1.13  2005/02/19 16:25:02  haase
   Replaced fd_parse with fd_intern

   Revision 1.12  2005/02/11 04:44:06  haase
   Fixed some null vectors and miscellaneous leaks

   Revision 1.11  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
