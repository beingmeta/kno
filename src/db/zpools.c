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

#include <libu8/pathfns.h>
#include <libu8/filefns.h>


#include <zlib.h>
#include <errno.h>
#include <sys/stat.h>

#define FD_DEFAULT_ZLEVEL 9

#if (HAVE_MMAP)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#define MMAP_FLAGS (MAP_SHARED|MAP_NORESERVE)
#endif

#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
#define offget(offvec,offset) (fd_flip_word((offvec)[offset]))
#define set_offset(offvec,offset,v) (offvec)[offset]=(fd_flip_word(v))
#else
#define offget(offvec,offset) ((offvec)[offset])
#define set_offset(offvec,offset,v) (offvec)[offset]=(v)
#endif

static void update_modtime(struct FD_ZPOOL *fp);
static void reload_filepool_cache(struct FD_ZPOOL *fp,int lock);

static struct FD_POOL_HANDLER zpool_handler;

/* Handling Schemas */

static fdtype schemas_slotid;

static int compare_slotids(const void *s1,const void *s2)
{
  fdtype *slotid1=(fdtype *)s1, *slotid2=(fdtype *)s2;
  if ((*slotid1)>(*slotid2)) return 1;
  else if ((*slotid1) == (*slotid2)) return 0;
  else return -1;
}

static int compare_schema_ptrs(const void *v1,const void *v2)
{
  struct FD_SCHEMA_TABLE *s1=(struct FD_SCHEMA_TABLE *)v1, *s2=(struct FD_SCHEMA_TABLE *)v2;
  if (s1->schema == s2->schema) return 0;
  else if (s1->schema < s2->schema) return -1;
  else return 1;
}

static int compare_schema_vals(const void *v1,const void *v2)
{
  struct FD_SCHEMA_TABLE *s1=(struct FD_SCHEMA_TABLE *)v1, *s2=(struct FD_SCHEMA_TABLE *)v2;
  if (s1->size > s2->size) return 1;
  else if (s1->size < s2->size) return -1;
  else {
    fdtype *slotids1=s1->schema, *slotids2=s2->schema;
    int i=0, n=s1->size; while (i < n)
      if ((slotids1[i]) > (slotids2[i])) return 1;
      else if (slotids1[i] < slotids2[i]) return -1;
      else i++;
    return 0;}
}

static int find_schema_index_by_ptr(fdtype *schema,struct FD_SCHEMA_TABLE *table,int n_schemas)
{
  if (n_schemas) {
    struct FD_SCHEMA_TABLE *bot=table, *top=bot+n_schemas, *mid=bot+(n_schemas/2);
    while (top >= bot)
      if (mid->schema == schema) return mid->schema_index;
      else if (schema > mid->schema) {
	bot=mid+1; mid=bot+(top-bot)/2;}
      else {
	top=mid-1; mid=bot+(top-bot)/2;}
    if (mid->schema == schema) return mid->schema_index;
    else return -1;}
  else return -1;
}

int bm_find_schema_by_vals(fdtype *schema,int n,struct FD_SCHEMA_TABLE *table,int n_schemas)
{
  if (n_schemas) {
    struct FD_SCHEMA_TABLE *bot=table, *top=bot+n_schemas, *mid=bot+(n_schemas/2);
    struct FD_SCHEMA_TABLE probe; fdtype *copy=u8_malloc(sizeof(fdtype)*n);
    int i=0, comparison=0; while (i < n) {copy[i]=schema[i]; i++;}
    probe.size=n; probe.schema=copy;
    if (n) qsort(copy,n,sizeof(fdtype),compare_slotids);
    while (top >= bot) {
      comparison=compare_schema_vals((const void *)&probe,(const void *)mid);
      if (comparison == 0) {
	u8_free(copy);
	return mid->schema_index;}
      else if (comparison > 0) {
	bot=mid+1; mid=bot+(top-bot)/2;}
      else {
	top=mid-1; mid=bot+(top-bot)/2;}}
    u8_free(copy);
    if (comparison == 0) return mid->schema_index;
    else return -1;}
  else return -1;
}

/* Reading metadata */

static fdtype read_metadata(struct FD_DTYPE_STREAM *s)
{
  int probe; off_t md_loc;
  probe=fd_dtsread_4bytes(s);
  if (probe == 0xFFFFFFFF) { /* Version 1 */
    off_t md_loc; time_t notime=(time_t) -1;
    fdtype metadata=FD_EMPTY_CHOICE;
    /* Write the time information (actually, its absence in this version). */
    md_loc=fd_dtsread_off_t(s);
    if (md_loc) {
      if (fd_setpos(s,md_loc)<0)
	return fd_err(fd_BadMetaData,"read_metadata","seek failed",
		      fd_erreify());
      else return fd_dtsread_dtype(s);}}
  else if (probe == 0xFFFFFFFE) { /* Version 2 */
    fdtype metadata=FD_EMPTY_CHOICE;
    int i=0; off_t md_loc;
    while (i<8) {fd_dtsread_4bytes(s); i++;}
    md_loc=fd_dtsread_off_t(s);
    if (md_loc) {
      if (fd_setpos(s,md_loc)<0)
	return fd_err(fd_BadMetaData,"read_metadata","seek failed",
		      fd_erreify());
      return fd_dtsread_dtype(s);}
    else return FD_EMPTY_CHOICE;}
  else return FD_EMPTY_CHOICE;
}

void init_zpool_schemas(struct FD_ZPOOL *zp,fdtype vector)
{
  int i=0, n_schemas;
  if (FD_VECTORP(vector)) n_schemas=FD_VECTOR_LENGTH(vector);
  else {
    zp->n_schemas=0; zp->schemas=NULL;
    zp->schemas_byptr=NULL; zp->schemas_byval=NULL;
    return;}
  zp->n_schemas=n_schemas;
  if (n_schemas) {
    zp->schemas=u8_malloc(sizeof(struct FD_SCHEMA_TABLE)*n_schemas);
    zp->schemas_byptr=u8_malloc(sizeof(struct FD_SCHEMA_TABLE)*n_schemas);
    zp->schemas_byval=u8_malloc(sizeof(struct FD_SCHEMA_TABLE)*n_schemas);}
  else {
    zp->schemas=NULL;
    zp->schemas_byptr=NULL;
    zp->schemas_byval=NULL;}
  while (i < n_schemas) {
    fdtype schema_vec=FD_VECTOR_REF(vector,i);
    int j=0, schema_len=FD_VECTOR_LENGTH(schema_vec);
    fdtype *schema, *sorted;
    if (schema_len) {
      schema=u8_malloc(sizeof(fdtype)*schema_len);
      sorted=u8_malloc(sizeof(fdtype)*schema_len);}
    else {
      schema=NULL; sorted=NULL;}
    while (j < schema_len) {
      schema[j]=FD_VECTOR_REF(schema_vec,j);
      sorted[j]=FD_VECTOR_REF(schema_vec,j);
      j++;}
    if (schema_len) qsort(sorted,schema_len,sizeof(fdtype),compare_slotids);
    zp->schemas[i].schema_index=i;
    zp->schemas[i].size=schema_len;
    zp->schemas[i].schema=schema;
    zp->schemas_byptr[i].schema_index=i;
    zp->schemas_byptr[i].size=schema_len;
    zp->schemas_byptr[i].schema=schema;
    zp->schemas_byval[i].schema_index=i;
    zp->schemas_byval[i].size=schema_len;
    zp->schemas_byval[i].schema=sorted;
    i++;}
  if (n_schemas) {
    qsort(zp->schemas_byptr,n_schemas,
	  sizeof(struct FD_SCHEMA_TABLE),compare_schema_ptrs);
    qsort(zp->schemas_byval,n_schemas,
	  sizeof(struct FD_SCHEMA_TABLE),compare_schema_vals);}
}

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

/* This reads a non frame value with compression. */
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

/* This reads an OID value.
   A compressed OID value has one of the forms:
    0 n_bytes <data>
    schema_id n_values n_bytes <data>
   where schema_id>0 indicates the schema for the frame value,
    n_values indicates the number of values, and n_bytes indicates
    the number of bytes of data in the file which, when uncompressed,
    will yield the n_values for the frame.
*/
fdtype read_oid_value
  (struct FD_DTYPE_STREAM *f,struct FD_SCHEMA_TABLE *schemas,int n_schemas)
{
  unsigned int schema_code=fd_dtsread_zint(f);
  if (schema_code) {
    struct FD_BYTE_INPUT in;
    int schema_index=schema_code-1, i=0;
    int n_values=fd_dtsread_zint(f), n_bytes=fd_dtsread_zint(f), dbytes;
    unsigned char *bytes;
    fdtype *values, result;
    if (n_values == schemas[schema_index].size) 
      values=u8_malloc(sizeof(fdtype)*n_values);
    else return fd_err(_("Schema inconsistency"),"read_oid_value",
		       NULL,FD_VOID);
    /* Read the data bytes */
    bytes=u8_malloc(n_bytes);
    if (fd_dtsread_bytes(f,bytes,n_bytes)<n_bytes) {
      u8_free(bytes);
      return fd_erreify();}
    in.ptr=in.start=do_uncompress(bytes,n_bytes,&dbytes);
    in.end=in.start+dbytes; in.fillfn=NULL; in.mpool=NULL;
    /* Read the values for the slotmap */
    while ((in.ptr < in.end) && (i < n_values)) {
      values[i]=fd_read_dtype(&in,NULL); i++;}
    u8_free(bytes); u8_free(in.start);
    return fd_make_schemap
      (NULL,n_values,0,
       schemas[schema_index].schema,values,
       NULL);}
  else return zread_dtype(f);
}

int write_oid_value
  (struct FD_DTYPE_STREAM *s,fdtype v,
   struct FD_SCHEMA_TABLE *schemas,int n_schemas)
{
  if (FD_XSCHEMAP(v)) {
    struct FD_BYTE_OUTPUT out;
    struct FD_SCHEMAP *sm=FD_XSCHEMAP(v);
    fdtype *schema=sm->schema, *values=sm->values;
    int schema_index=find_schema_index_by_ptr(schema,schemas,n_schemas);
    int i=0, size=sm->size, zlen, wlen;
    unsigned char *zbytes; 
    if (schema_index < 0) {
      int size=fd_dtswrite_zint(s,0);
      size=size+zwrite_dtype(s,v);
      return size;}
    out.ptr=out.start=u8_malloc(4096); out.end=out.start+4096;
    wlen=fd_dtswrite_zint(s,schema_index+1);
    wlen=wlen+fd_dtswrite_zint(s,size);
    while (i < size) {
      fd_write_dtype(&out,values[i]); i++;}
    zbytes=do_compress(out.start,out.end-out.start,&zlen);
    fd_dtswrite_bytes(s,zbytes,zlen);
    wlen=wlen+zlen;
    u8_free(out.start); u8_free(zbytes);
    return wlen;}
  else {
    int size=fd_dtswrite_zint(s,0);
    size=size+zwrite_dtype(s,v);
    return size;}
}

/* Opening zpools */

static fd_pool open_zpool(u8_string fname,int read_only)
{
  struct FD_ZPOOL *pool=u8_malloc(sizeof(struct FD_ZPOOL));
  struct FD_DTYPE_STREAM *s=&(pool->stream);
  FD_OID base; unsigned int hi, lo, magicno, capacity, load;
  off_t label_loc; fdtype label;
  u8_string rname=u8_realpath(fname,NULL);
  fd_dtstream_mode mode=
    ((read_only) ? (FD_DTSTREAM_READ) : (FD_DTSTREAM_MODIFY));
  fd_init_dtype_file_stream(&(pool->stream),fname,mode,FD_FILEDB_BUFSIZE,NULL,NULL);
  /* See if it ended up read only */
  if (pool->stream.bits&FD_DTSTREAM_READ_ONLY) read_only=1;
  pool->stream.mallocd=0;
  magicno=fd_dtsread_4bytes(s);
  hi=fd_dtsread_4bytes(s); lo=fd_dtsread_4bytes(s);
  FD_SET_OID_HI(base,hi); FD_SET_OID_LO(base,lo);
  capacity=fd_dtsread_4bytes(s);
  fd_init_pool((fd_pool)pool,base,capacity,&zpool_handler,fname,rname);
  u8_free(rname);
  load=fd_dtsread_4bytes(s);
  label_loc=(off_t)fd_dtsread_4bytes(s);
  if (label_loc) {
    if (fd_setpos(s,label_loc)>0) {
      label=fd_dtsread_dtype(s);
      if (FD_STRINGP(label)) pool->label=u8_strdup(FD_STRDATA(label));
      else u8_warn(fd_BadFilePoolLabel,fd_dtype2string(label));
      fd_decref(label);}
    else {
      fd_seterr(fd_BadFilePoolLabel,"open_std_file_pool",
		u8_strdup("bad label loc"),
		fd_erreify());
      fd_dtsclose(&(pool->stream),1);
      u8_free(rname); u8_free(pool);
      return NULL;}}
  if (fd_setpos(s,24+capacity*4)<0) {
    fd_seterr(fd_BadFilePoolLabel,"open_std_file_pool",
	      u8_strdup("bad label loc"),
	      fd_erreify());
    fd_dtsclose(&(pool->stream),1);
    u8_free(rname); u8_free(pool);
    return NULL;}
  else {
    fdtype metadata=read_metadata(s);
    fdtype schemas=fd_get(metadata,schemas_slotid,FD_EMPTY_CHOICE);
    if (FD_VECTORP(schemas)) init_zpool_schemas(pool,schemas);
    fd_decref(metadata); fd_decref(schemas);}
  pool->load=load; pool->offsets=NULL;
  pool->read_only=read_only;
  u8_init_mutex(&(pool->lock));
  update_modtime(pool);
  return (fd_pool)pool;
}

static int lock_zpool(struct FD_ZPOOL *fp,int use_mutex)
{
  if (FD_FILEPOOL_LOCKED(fp)) return 1;
  else if ((fp->stream.bits)&(FD_DTSTREAM_READ_ONLY)) return 0;
  else {
    struct FD_DTYPE_STREAM *s=&(fp->stream);
    struct stat fileinfo;
    if (use_mutex) u8_lock_mutex(&(fp->lock));
    /* Handle race condition by checking when locked */
    if (FD_FILEPOOL_LOCKED(fp)) {
      if (use_mutex) u8_unlock_mutex(&(fp->lock));
      return 1;}
    if (fd_dtslock(s)==0) {
      u8_unlock_mutex(&(fp->lock));
      return 0;}
    fstat(s->fd,&fileinfo);
    if (fileinfo.st_mtime>fp->modtime) {
      /* Make sure we're up to date. */
      if (fp->offsets) reload_filepool_cache(fp,0);
      else {
	fd_reset_hashtable(&(fp->cache),-1,1);
	fd_reset_hashtable(&(fp->locks),32,1);}}
    if (use_mutex) u8_unlock_mutex(&(fp->lock));
    return 1;}
}

static void update_modtime(struct FD_ZPOOL *fp)
{
  struct stat fileinfo;
  if ((fstat(fp->stream.fd,&fileinfo))<0)
    fp->modtime=(time_t)-1;
  else fp->modtime=fileinfo.st_mtime;
}

static int zpool_load(fd_pool p)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  if (FD_FILEPOOL_LOCKED(fp)) return fp->load;
  else {
    int load;
    u8_lock_mutex(&(fp->lock));
    if (fd_setpos(&(fp->stream),16)<0) {
      u8_unlock_mutex(&(fp->lock));
      return -1;}
    load=fd_dtsread_4bytes(&(fp->stream));
    fp->load=load;
    u8_unlock_mutex(&(fp->lock));
    return load;}
}

static fdtype zpool_fetch(fd_pool p,fdtype oid)
{
  fdtype value;
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  FD_OID addr=FD_OID_ADDR(oid);
  int offset=FD_OID_DIFFERENCE(addr,fp->base);
  off_t data_pos;
  u8_lock_mutex(&(fp->lock));
  if (fp->offsets) data_pos=offget(fp->offsets,offset);
  else {
    if (fd_setpos(&(fp->stream),24+4*offset)<0) {
      u8_unlock_mutex(&(fp->lock));
      return fd_erreify();}
    data_pos=fd_dtsread_4bytes(&(fp->stream));}
  if (data_pos == 0) value=FD_EMPTY_CHOICE;
  else {
    if (fd_setpos(&(fp->stream),data_pos)<0) {
      u8_unlock_mutex(&(fp->lock));
      return fd_erreify();}
    value=read_oid_value(&(fp->stream),fp->schemas,fp->n_schemas);}
  u8_unlock_mutex(&(fp->lock));
  return value;
}

struct POOL_FETCH_SCHEDULE {
  unsigned int vpos; off_t filepos;};

static int compare_filepos(const void *x1,const void *x2)
{
  const struct POOL_FETCH_SCHEDULE *s1=x1, *s2=x2;
  if (s1->filepos<s2->filepos) return -1;
  else if (s1->filepos>s2->filepos) return 1;
  else return 0;
}

static fdtype *zpool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p; FD_OID base=p->base;
  struct FD_DTYPE_STREAM *stream=&(fp->stream);
  struct POOL_FETCH_SCHEDULE *schedule=u8_malloc(sizeof(struct POOL_FETCH_SCHEDULE)*n);
  fdtype *result=u8_malloc(sizeof(fdtype)*n);
  int i=0;
  u8_lock_mutex(&(fp->lock));
  if (fp->offsets) {
    unsigned int *offsets=fp->offsets;
    int i=0; while (i < n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base);
      schedule[i].vpos=i;
      schedule[i].filepos=offget(offsets,off);
      i++;}}
  else {
    int i=0; while (i < n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base);
      schedule[i].vpos=i;
      if (fd_setpos(stream,24+4*off)<0) {
	u8_free(schedule);
	u8_free(result);
	u8_unlock_mutex(&(fp->lock));
	return NULL;}
      schedule[i].filepos=fd_dtsread_4bytes(stream);
      i++;}}
  qsort(schedule,n,sizeof(struct POOL_FETCH_SCHEDULE),
	compare_filepos);
  i=0; while (i < n) 
    if (schedule[i].filepos) {
      if (fd_setpos(stream,schedule[i].filepos)<0) {
	int j=0; while (j<i) {
	  fd_decref(result[schedule[j].vpos]); j++;}
	u8_free(schedule);
	u8_free(result);
	u8_unlock_mutex(&(fp->lock));
	return NULL;}
      result[schedule[i].vpos]=
	read_oid_value(stream,fp->schemas,fp->n_schemas);
      i++;}
    else result[schedule[i++].vpos]=FD_EMPTY_CHOICE;
  u8_free(schedule);
  u8_unlock_mutex(&(fp->lock));  
  return result;
}

static int zpool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p; FD_OID base=p->base;
  unsigned int *offsets=u8_malloc(sizeof(unsigned int)*n);
  struct FD_SCHEMA_TABLE *schemas=fp->schemas_byptr;
  int n_schemas=fp->n_schemas;
  struct FD_DTYPE_STREAM *stream=&(fp->stream);
  off_t endpos; int i=0,retcode=n;;
  u8_lock_mutex(&(fp->lock));
  endpos=fd_endpos(stream);
#if HAVE_MMAP
  if (fp->offsets) {
    int retval=munmap((fp->offsets)-6,4*fp->capacity+24);
    unsigned int *newmmap;
    if (retval<0) {
      u8_warn(u8_strerror(errno),"zpool_storen:munmap %s",fp->source);
      fp->offsets=NULL; errno=0;}
    else fp->offsets=NULL;
    newmmap=
      mmap(NULL,(4*fp->capacity)+24,
	   PROT_READ|PROT_WRITE,
	   MAP_SHARED,stream->fd,0);
    if ((newmmap==NULL) || (newmmap==((void *)-1))) {
      u8_warn(u8_strerror(errno),"zpool_storen:mmap %s",fp->source);
      fp->offsets=NULL; errno=0;}
    else fp->offsets=newmmap+6;}
#endif
  while (i<n) {
    int delta=write_oid_value(stream,values[i],schemas,n_schemas);
    if (delta<0) {retcode=-1; break;}
    offsets[i]=endpos; endpos=endpos+delta;
    i++;}
  if (retcode<0) {}
  else if (fp->offsets) {
     int i=0; while (i<n) {
      FD_OID addr=FD_OID_ADDR(oids[i]);
      unsigned int reloff=FD_OID_DIFFERENCE(addr,base);
      set_offset(fp->offsets,reloff,offsets[i]);
      i++;}}
  else {
    int i=0; while (i<n) {
      FD_OID addr=FD_OID_ADDR(oids[i]);
      unsigned int reloff=FD_OID_DIFFERENCE(addr,base);
      if (fd_setpos(stream,24+4*reloff)<0) {
	retcode=-1; break;}
      fd_dtswrite_4bytes(stream,offsets[i]);
      i++;}}
  u8_free(offsets);
#if HAVE_MMAP
  if (fp->offsets) {
    int retval=munmap((fp->offsets)-6,4*fp->capacity+24);
    unsigned int *newmmap;
    if (retval<0)  {
      u8_warn(u8_strerror(errno),"zpool_storen:munmap %s",fp->source);
      fp->offsets=NULL; errno=0;}
    else fp->offsets=NULL;
    newmmap=
      mmap(NULL,(4*fp->capacity)+24,
	   PROT_READ,MAP_SHARED|MAP_NORESERVE,stream->fd,0);
    if ((newmmap==NULL) || (newmmap==((void *)-1))) {
      u8_warn(u8_strerror(errno),"zpool_storen:mmap %s",fp->source);
      fp->offsets=NULL; errno=0;}
    else fp->offsets=newmmap+6;}
#else
  if ((retcode>=0) && (fp->offsets)) {
    if (fd_setpos(stream,24)<0) retcode=-1;;
    else fd_dtswrite_ints(stream,fp->offsets_size,offsets);}
#endif
  if (retcode>=0) {
    if (fd_setpos(stream,16)<0) retcode=1;
    else {
      fd_dtswrite_4bytes(stream,fp->load);
      fd_dtsflush(stream);
      update_modtime(fp);
      fsync(stream->fd);}}
  u8_unlock_mutex(&(fp->lock));
  return retcode;
}

static fdtype zpool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE; int i=0;
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  u8_lock_mutex(&(fp->lock));
  if (!(FD_FILEPOOL_LOCKED(fp))) lock_zpool(fp,0);
  if (fp->load+n>=fp->capacity) {
    u8_unlock_mutex(&(fp->lock));
    return fd_err(fd_ExhaustedPool,"zpool_alloc",p->cid,FD_VOID);}
  while (i < n) {
    FD_OID new_addr=FD_OID_PLUS(fp->base,fp->load);
    fdtype new_oid=fd_make_oid(new_addr);
    FD_ADD_TO_CHOICE(results,new_oid);
    fp->load++; i++; fp->n_locks++;}
  u8_unlock_mutex(&(fp->lock));
  return results;
}

static int zpool_lock(fd_pool p,fdtype oids)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  int retval=lock_zpool(fp,1);
  if (retval)
    fp->n_locks=fp->n_locks+FD_CHOICE_SIZE(oids);
  return retval;
}

static int zpool_unlock(fd_pool p,fdtype oids)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  if (fp->n_locks == 0) return 0;
  else if (!(FD_FILEPOOL_LOCKED(fp))) return 0;
  else if (FD_CHOICEP(oids))
    fp->n_locks=fp->n_locks-FD_CHOICE_SIZE(oids);
  else fp->n_locks--;
  if (fp->n_locks == 0) {
    fd_dtsunlock(&(fp->stream));
    fd_reset_hashtable(&(fp->locks),0,1);}
  return 1;
}

static void zpool_setcache(fd_pool p,int level)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  if (level == 2)
    if (fp->offsets) return;
    else {
      fd_dtype_stream s=&(fp->stream);
      unsigned int load, *offsets, *newmmap;
      u8_lock_mutex(&(fp->lock));
      if (fp->offsets) {
	u8_unlock_mutex(&(fp->lock));
	return;}
#if HAVE_MMAP
      newmmap=
	mmap(NULL,(4*fp->capacity)+24,PROT_READ,
	     MAP_SHARED|MAP_NORESERVE,s->fd,0);
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
	u8_warn(u8_strerror(errno),"zpool_setcache:mmap %s",fp->source);
	fp->offsets=NULL; errno=0;}
      else {
	fp->offsets=offsets=newmmap+6;
	fp->offsets_size=fp->capacity;}
#else
      fd_dts_start_read(s);
      if (fd_setpos(s,12)>0) {
	fp->load=load=fd_dtsread_4bytes(s);
	offsets=u8_malloc(sizeof(unsigned int)*load);
	fd_setpos(s,24);
	fd_dtsread_ints(s,load,offsets);
	fp->offsets=offsets; fp->offsets_size=load;}
#endif
      u8_unlock_mutex(&(fp->lock));}
  else if (level < 2)
    if (fp->offsets == NULL) return;
    else {
      int retval;
      u8_lock_mutex(&(fp->lock));
#if HAVE_MMAP
      retval=munmap((fp->offsets)-6,4*fp->capacity+24);
      if (retval<0) {
	u8_warn(u8_strerror(errno),"zpool_setcache:munmap %s",fp->source);
	fp->offsets=NULL; errno=0;}
#else
      u8_free(fp->offsets);
#endif
      fp->offsets=NULL; fp->offsets_size=0;}
}

static void reload_filepool_cache(struct FD_ZPOOL *fp,int lock)
{
#if HAVE_MMAP
#else
  fd_dtype_stream s=&(fp->stream);
  /* Read new offsets table, compare it with the current, and
     only void those OIDs */
  unsigned int new_load, *offsets, *nscan, *oscan, *olim;
  if (lock) u8_lock_mutex(&(fp->lock));
  oscan=fp->offsets; olim=oscan+fp->offsets_size;
  fd_setpos(s,16); new_load=fd_dtsread_4bytes(s);
  nscan=offsets=u8_malloc(sizeof(unsigned int)*new_load);
  fd_setpos(s,24);
  fd_dtsread_ints(s,new_load,offsets);
  while (oscan < olim)
    if (*oscan == *nscan) {oscan++; nscan++;}
    else {
      FD_OID addr=FD_OID_PLUS(fp->base,(nscan-offsets));
      fdtype changed_oid=fd_make_oid(addr);
      fd_hashtable_op(&(fp->cache),fd_table_replace,changed_oid,FD_VOID);
      oscan++; nscan++;}
  u8_free(fp->offsets);
  fp->offsets=offsets; fp->load=fp->offsets_size=new_load;
  update_modtime(fp);
  if (lock) u8_unlock_mutex(&(fp->lock));
#endif
}

static void zpool_close(fd_pool p)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  u8_lock_mutex(&(fp->lock));
  fd_dtsclose(&(fp->stream),1);
  if (fp->offsets) {
#if HAVE_MMAP
    int retval=munmap((fp->offsets)-6,4*fp->capacity+24);
    unsigned int *newmmap;
    if (retval<0) {
      u8_warn(u8_strerror(errno),"file_pool_storen:munmap %s",fp->source);
      errno=0;}
#else
    u8_free(fp->offsets);
#endif
    fp->offsets=NULL;
    fp->cache_level=-1;}
  u8_unlock_mutex(&(fp->lock));
}

static void zpool_setbuf(fd_pool p,int bufsiz)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  u8_lock_mutex(&(fp->lock));
  fd_dtsbufsize(&(fp->stream),bufsiz);
  u8_unlock_mutex(&(fp->lock));
}

static fdtype zpool_metadata(fd_pool p,fdtype md)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  if (FD_VOIDP(md))
    return fd_read_pool_metadata(&(fp->stream));
  else return fd_write_pool_metadata((&(fp->stream)),md);
}

/* Initialization */

static struct FD_POOL_HANDLER zpool_handler={
  "filepool", 1, sizeof(struct FD_ZPOOL), 12,
   zpool_close, /* close */
   zpool_setcache, /* setcache */
   zpool_setbuf, /* setbuf */
   zpool_alloc, /* alloc */
   zpool_fetch, /* fetch */
   zpool_fetchn, /* fetchn */
   zpool_load, /* getload */
   zpool_lock, /* lock */
   zpool_unlock, /* release */
   zpool_storen, /* storen */
   zpool_metadata, /* metdata */
   NULL}; /* sync */

FD_EXPORT fd_init_zpools_c()
{
  fd_register_source_file(versionid);
  fd_register_pool_opener
    (FD_ZPOOL_MAGIC_NUMBER,
     open_zpool,fd_read_pool_metadata,fd_write_pool_metadata);

  fd_register_pool_opener
    (FD_ZPOOL_MAGIC_NUMBER,open_zpool,
     fd_read_pool_metadata,fd_write_pool_metadata);
  schemas_slotid=fd_intern("SCHEMAS");
}


/* The CVS log for this file
   $Log: zpools.c,v $
   Revision 1.39  2006/03/14 04:30:19  haase
   Signal error when a pool is exhausted

   Revision 1.38  2006/02/05 13:53:26  haase
   Added locking around file pool stores

   Revision 1.37  2006/01/31 13:47:23  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.36  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.35  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.34  2006/01/05 19:16:44  haase
   Fix file pool lock/unlock inconsistency

   Revision 1.33  2006/01/05 18:04:44  haase
   Made pool access check return values from fd_setpos

   Revision 1.32  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.31  2005/08/02 23:17:22  haase
   Incomplete fixes to zpool writing

   Revision 1.30  2005/07/13 21:39:31  haase
   XSLOTMAP/XSCHEMAP renaming

   Revision 1.29  2005/05/30 00:03:54  haase
   Fixes to pool declaration, allowing the USE-POOL primitive to return multiple pools correctly when given a ; spearated list or a pool server which provides multiple pools

   Revision 1.28  2005/05/26 11:03:21  haase
   Fixed some bugs with read-only pools and indices

   Revision 1.27  2005/05/25 18:05:19  haase
   Check for -1 return value from mmap as well as NULL

   Revision 1.26  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.25  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.24  2005/04/12 20:42:45  haase
   Used #define FD_FILEDB_BUFSIZE to set default buffer size (initially 256K)

   Revision 1.23  2005/04/06 18:31:51  haase
   Fixed mmap error calls to produce warnings rather than raising errors and to use u8_strerror to get the condition name

   Revision 1.22  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.21  2005/03/28 19:19:36  haase
   Added metadata reading and writing and file pool/index creation

   Revision 1.20  2005/03/25 19:49:47  haase
   Removed base library for eframerd, deferring to libu8

   Revision 1.19  2005/03/07 19:37:33  haase
   Added fsyncs and mmap/munmamp return value checking

   Revision 1.18  2005/03/06 19:26:44  haase
   Plug some leaks and some failures to return values

   Revision 1.17  2005/03/06 18:28:21  haase
   Added timeprims

   Revision 1.16  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.15  2005/03/03 17:58:15  haase
   Moved stdio dependencies out of fddb and reorganized make structure

   Revision 1.14  2005/02/26 22:31:41  haase
   Remodularized choice and oid add into xtables.c

   Revision 1.13  2005/02/25 19:45:24  haase
   Fixed commitment of cached file pools

   Revision 1.12  2005/02/24 00:11:17  haase
   Fixed bug in dtype stream overreading

   Revision 1.11  2005/02/11 04:44:06  haase
   Fixed some null vectors and miscellaneous leaks

   Revision 1.10  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
