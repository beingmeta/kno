/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_DTYPEIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dbfile.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>


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
static void reload_file_pool_cache(struct FD_ZPOOL *fp,int lock);

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
  struct FD_SCHEMA_TABLE *s1=(struct FD_SCHEMA_TABLE *)v1;
  struct FD_SCHEMA_TABLE *s2=(struct FD_SCHEMA_TABLE *)v2;
  if (s1->fdst_schema == s2->fdst_schema) return 0;
  else if (s1->fdst_schema < s2->fdst_schema) return -1;
  else return 1;
}

static int compare_schema_vals(const void *v1,const void *v2)
{
  struct FD_SCHEMA_TABLE *s1=(struct FD_SCHEMA_TABLE *)v1;
  struct FD_SCHEMA_TABLE *s2=(struct FD_SCHEMA_TABLE *)v2;
  if (s1->fdst_nslots > s2->fdst_nslots) return 1;
  else if (s1->fdst_nslots < s2->fdst_nslots) return -1;
  else {
    fdtype *slotids1=s1->fdst_schema, *slotids2=s2->fdst_schema;
    int i=0, n=s1->fdst_nslots; while (i < n)
      if ((slotids1[i]) > (slotids2[i])) return 1;
      else if (slotids1[i] < slotids2[i]) return -1;
      else i++;
    return 0;}
}

static int find_schema_index_by_ptr(fdtype *schema,
                                    struct FD_SCHEMA_TABLE *table,
                                    int n_schemas)
{
  if (n_schemas) {
    struct FD_SCHEMA_TABLE *bot=table, *top=bot+n_schemas, *mid=bot+(n_schemas/2);
    while (top >= bot)
      if (mid->fdst_schema == schema) return mid->fdst_index;
      else if (schema > mid->fdst_schema) {
        bot=mid+1; mid=bot+(top-bot)/2;}
      else {
        top=mid-1; mid=bot+(top-bot)/2;}
    if (mid->fdst_schema == schema) return mid->fdst_index;
    else return -1;}
  else return -1;
}

int bm_find_schema_by_vals(fdtype *schema,int n,
                           struct FD_SCHEMA_TABLE *table,
                           int n_schemas)
{
  if (n_schemas) {
    struct FD_SCHEMA_TABLE *bot=table, *top=bot+n_schemas, *mid=bot+(n_schemas/2);
    struct FD_SCHEMA_TABLE probe; fdtype *copy=u8_alloc_n(n,fdtype);
    int i=0, comparison=0; while (i < n) {copy[i]=schema[i]; i++;}
    probe.fdst_nslots=n; probe.fdst_schema=copy;
    if (n) qsort(copy,n,sizeof(fdtype),compare_slotids);
    while (top >= bot) {
      comparison=compare_schema_vals((const void *)&probe,(const void *)mid);
      if (comparison == 0) {
        u8_free(copy);
        return mid->fdst_index;}
      else if (comparison > 0) {
        bot=mid+1; mid=bot+(top-bot)/2;}
      else {
        top=mid-1; mid=bot+(top-bot)/2;}}
    u8_free(copy);
    if (comparison == 0) return mid->fdst_index;
    else return -1;}
  else return -1;
}

/* Reading metadata */

static fdtype read_metadata(struct FD_DTYPE_STREAM *s)
{
  int probe;
  probe=fd_dtsread_4bytes(s);
  if (probe == 0xFFFFFFFF) { /* Version 1 */
    fd_off_t md_loc;
    /* Write the time information (actually, its absence in this version). */
    md_loc=fd_dtsread_off_t(s);
    if (md_loc) {
      if (fd_setpos(s,md_loc)<0)
        return fd_err(fd_BadMetaData,"read_metadata","seek failed",
                      FD_ERROR_VALUE);
      else return fd_dtsread_dtype(s);}
    else return FD_EMPTY_CHOICE;}
  else if (probe == 0xFFFFFFFE) { /* Version 2 */
    int i=0; fd_off_t md_loc;
    while (i<8) {fd_dtsread_4bytes(s); i++;}
    md_loc=fd_dtsread_off_t(s);
    if (md_loc) {
      if (fd_setpos(s,md_loc)<0)
        return fd_err(fd_BadMetaData,"read_metadata","seek failed",
                      FD_ERROR_VALUE);
      return fd_dtsread_dtype(s);}
    else return FD_EMPTY_CHOICE;}
  else return FD_EMPTY_CHOICE;
}

void init_zpool_schemas(struct FD_ZPOOL *zp,fdtype vector)
{
  int i=0, n_schemas;
  if (FD_VECTORP(vector)) n_schemas=FD_VECTOR_LENGTH(vector);
  else {
    zp->fdp_n_schemas=0; zp->fdp_schemas=NULL;
    zp->fdp_fdp_schemas_byptr=NULL; zp->fdp_schemas_byval=NULL;
    return;}
  zp->fdp_n_schemas=n_schemas;
  if (n_schemas) {
    zp->fdp_schemas=u8_alloc_n(n_schemas,struct FD_SCHEMA_TABLE);
    zp->fdp_fdp_schemas_byptr=u8_alloc_n(n_schemas,struct FD_SCHEMA_TABLE);
    zp->fdp_schemas_byval=u8_alloc_n(n_schemas,struct FD_SCHEMA_TABLE);}
  else {
    zp->fdp_schemas=NULL;
    zp->fdp_fdp_schemas_byptr=NULL;
    zp->fdp_schemas_byval=NULL;}
  while (i < n_schemas) {
    fdtype schema_vec=FD_VECTOR_REF(vector,i);
    int j=0, schema_len=FD_VECTOR_LENGTH(schema_vec);
    fdtype *schema, *sorted;
    if (schema_len) {
      schema=u8_alloc_n(schema_len,fdtype);
      sorted=u8_alloc_n(schema_len,fdtype);}
    else {
      schema=NULL; sorted=NULL;}
    while (j < schema_len) {
      schema[j]=FD_VECTOR_REF(schema_vec,j);
      sorted[j]=FD_VECTOR_REF(schema_vec,j);
      j++;}
    if (schema_len) qsort(sorted,schema_len,sizeof(fdtype),compare_slotids);
    zp->fdp_schemas[i].fdst_index=i;
    zp->fdp_schemas[i].fdst_nslots=schema_len;
    zp->fdp_schemas[i].fdst_schema=schema;
    zp->fdp_fdp_schemas_byptr[i].fdst_index=i;
    zp->fdp_fdp_schemas_byptr[i].fdst_nslots=schema_len;
    zp->fdp_fdp_schemas_byptr[i].fdst_schema=schema;
    zp->fdp_schemas_byval[i].fdst_index=i;
    zp->fdp_schemas_byval[i].fdst_nslots=schema_len;
    zp->fdp_schemas_byval[i].fdst_schema=sorted;
    i++;}
  if (n_schemas) {
    qsort(zp->fdp_fdp_schemas_byptr,n_schemas,
          sizeof(struct FD_SCHEMA_TABLE),compare_schema_ptrs);
    qsort(zp->fdp_schemas_byval,n_schemas,
          sizeof(struct FD_SCHEMA_TABLE),compare_schema_vals);}
}

/* Reading compressed oid values */

static unsigned char *do_uncompress
  (unsigned char *bytes,size_t n_bytes,ssize_t *dbytes)
{
  int error;
  uLongf x_lim=4*n_bytes, x_bytes=x_lim;
  Bytef *fdata=(Bytef *)bytes, *xdata=u8_malloc(x_bytes);
  while ((error=uncompress(xdata,&x_bytes,fdata,n_bytes)) < Z_OK)
    if (error == Z_MEM_ERROR) {
      u8_free(xdata);
      fd_seterr1("ZLIB Out of Memory");
      return NULL;}
    else if (error == Z_BUF_ERROR) {
      xdata=u8_realloc(xdata,x_lim*2); x_bytes=x_lim=x_lim*2;}
    else if (error == Z_DATA_ERROR) {
      u8_free(xdata);
      fd_seterr1("ZLIB Data error");
      return NULL;}
    else {
      u8_free(xdata);
      fd_seterr1("Bad ZLIB return code");
      return NULL;}
  *dbytes=x_bytes;
  return xdata;
}

static unsigned char *do_compress(unsigned char *bytes,size_t n_bytes,
                                  ssize_t *zbytes)
{
  int error; Bytef *zdata;
  uLongf zlen, zlim;
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
  ssize_t n_bytes=fd_dtsread_zint(s), dbytes;
  unsigned char *bytes;
  int retval=-1;
  bytes=u8_malloc(n_bytes);
  retval=fd_dtsread_bytes(s,bytes,n_bytes);
  if (retval<n_bytes) {
    u8_free(bytes);
    return FD_ERROR_VALUE;}
  in.fd_bufptr=in.fd_bufstart=do_uncompress(bytes,n_bytes,&dbytes);
  if (in.fd_bufstart==NULL) {
    u8_free(bytes);
    return FD_ERROR_VALUE;}
  in.fd_buflim=in.fd_bufstart+dbytes; in.fd_dts_fillfn=NULL;
  result=fd_read_dtype(&in);
  u8_free(bytes); u8_free(in.fd_bufstart);
  return result;
}

/* This reads a non frame value with compression. */
static int zwrite_dtype(struct FD_DTYPE_STREAM *s,fdtype x)
{
  unsigned char *zbytes; ssize_t zlen=-1, size;
  struct FD_BYTE_OUTPUT out;
  out.fd_bufptr=out.fd_bufstart=u8_malloc(1024); out.fd_buflim=out.fd_bufstart+1024;
  if (fd_write_dtype(&out,x)<0) {
    u8_free(out.fd_bufstart);
    return FD_ERROR_VALUE;}
  zbytes=do_compress(out.fd_bufstart,out.fd_bufptr-out.fd_bufstart,&zlen);
  if (zlen<0) {
    u8_free(out.fd_bufstart);
    return FD_ERROR_VALUE;}
  size=fd_dtswrite_zint(s,zlen); size=size+zlen;
  if (fd_dtswrite_bytes(s,zbytes,zlen)<0) size=-1;
  u8_free(zbytes); u8_free(out.fd_bufstart);
  return size;
}

/* This reads a non frame value with compression. */
static int U8_MAYBE_UNUSED zwrite_dtypes(struct FD_DTYPE_STREAM *s,fdtype x)
{
  unsigned char *zbytes=NULL; ssize_t zlen=-1, size; int retval=0;
  struct FD_BYTE_OUTPUT out;
  out.fd_bufptr=out.fd_bufstart=u8_malloc(1024); out.fd_buflim=out.fd_bufstart+1024;
  if (FD_CHOICEP(x)) {
    FD_DO_CHOICES(v,x) {
      retval=fd_write_dtype(&out,v);
      if (retval<0) {FD_STOP_DO_CHOICES; break;}}}
  else if (FD_VECTORP(x)) {
    int i=0, len=FD_VECTOR_LENGTH(x); fdtype *data=FD_VECTOR_DATA(x);
    while (i<len) {
      retval=fd_write_dtype(&out,data[i]); i++;
      if (retval<0) break;}}
  else retval=fd_write_dtype(&out,x);
  if (retval>=0)
    zbytes=do_compress(out.fd_bufstart,out.fd_bufptr-out.fd_bufstart,&zlen);
  if ((retval<0)||(zlen<0)) {
    if (zbytes) u8_free(zbytes); u8_free(out.fd_bufstart);
    return -1;}
  size=fd_dtswrite_zint(s,zlen); size=size+zlen;
  retval=fd_dtswrite_bytes(s,zbytes,zlen);
  u8_free(zbytes); u8_free(out.fd_bufstart);
  if (retval<0) return retval;
  else return size;
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
    int n_values=fd_dtsread_zint(f);
    ssize_t n_bytes=fd_dtsread_zint(f), dbytes=-1;
    unsigned char *bytes;
    fdtype *values;
    if (n_values == schemas[schema_index].fdst_nslots)
      values=u8_alloc_n(n_values,fdtype);
    else return fd_err(_("Schema inconsistency"),"read_oid_value",
                       NULL,FD_VOID);
    /* Read the data bytes */
    bytes=u8_malloc(n_bytes);
    if (fd_dtsread_bytes(f,bytes,n_bytes)<n_bytes) {
      u8_free(bytes);
      return FD_ERROR_VALUE;}
    in.fd_bufptr=in.fd_bufstart=do_uncompress(bytes,n_bytes,&dbytes);
    if ((dbytes<0)||(in.fd_bufptr==NULL)) {
      u8_free(bytes); u8_free(in.fd_bufstart);
      return FD_ERROR_VALUE;}
    in.fd_buflim=in.fd_bufstart+dbytes; in.fd_dts_fillfn=NULL;
    /* Read the values for the slotmap */
    while ((in.fd_bufptr < in.fd_buflim) && (i < n_values)) {
      values[i]=fd_read_dtype(&in); i++;}
    u8_free(bytes); u8_free(in.fd_bufstart);
    return fd_make_schemap
      (NULL,n_values,0,schemas[schema_index].fdst_schema,values);}
  else return zread_dtype(f);
}

int write_oid_value
  (struct FD_DTYPE_STREAM *s,fdtype v,
   struct FD_SCHEMA_TABLE *schemas,int n_schemas)
{
  if (FD_XSCHEMAP(v)) {
    struct FD_BYTE_OUTPUT out;
    struct FD_SCHEMAP *sm=FD_XSCHEMAP(v);
    fdtype *schema=sm->fd_schema, *values=sm->fd_values;
    int schema_index=find_schema_index_by_ptr(schema,schemas,n_schemas);
    int i=0, size=sm->fd_table_size, retval=-1; ssize_t zlen=-1, wlen;
    unsigned char *zbytes;
    if (schema_index < 0) {
      int size=fd_dtswrite_zint(s,0);
      int dsize=((size>0)?(zwrite_dtype(s,v)):(-1));
      if (dsize<0) return -1;
      else return size+dsize;}
    out.fd_bufptr=out.fd_bufstart=u8_malloc(4096); out.fd_buflim=out.fd_bufstart+4096;
    wlen=fd_dtswrite_zint(s,schema_index+1);
    wlen=wlen+fd_dtswrite_zint(s,size);
    while (i < size) {
      retval=fd_write_dtype(&out,values[i]);
      if (retval<0) {
        u8_free(out.fd_bufstart);
        return -1;}
      i++;}
    zbytes=do_compress(out.fd_bufstart,out.fd_buflim-out.fd_bufstart,&zlen);
    if (zlen<0) {
      u8_free(out.fd_bufstart);
      return FD_ERROR_VALUE;}
    retval=fd_dtswrite_bytes(s,zbytes,zlen);
    if (retval<0) {
      u8_free(out.fd_bufstart); u8_free(zbytes);
      return FD_ERROR_VALUE;}
    wlen=wlen+zlen;
    u8_free(out.fd_bufstart); u8_free(zbytes);
    return wlen;}
  else {
    int size=fd_dtswrite_zint(s,0);
    int dsize=zwrite_dtype(s,v);
    if (dsize<0) return -1;
    else return size+dsize;}
}

/* Opening zpools */

static fd_pool open_zpool(u8_string fname,int read_only)
{
  struct FD_ZPOOL *pool=u8_alloc(struct FD_ZPOOL);
  struct FD_DTYPE_STREAM *s=&(pool->fd_stream);
  unsigned int hi, lo, magicno, capacity, load;
  fd_off_t label_loc; fdtype label;
  u8_string rname=u8_realpath(fname,NULL);
  fd_dtstream_mode mode=
    ((read_only) ? (FD_DTSTREAM_READ) : (FD_DTSTREAM_MODIFY));
  FD_OID base=FD_NULL_OID_INIT;
  fd_init_dtype_file_stream(&(pool->fd_stream),fname,mode,fd_filedb_bufsize);
  /* See if it ended up read only */
  if ((pool->fd_stream.fd_dts_flags)&FD_DTSTREAM_READ_ONLY) read_only=1;
  pool->fd_stream.fd_mallocd=0;
  magicno=fd_dtsread_4bytes(s);
  if (magicno!=FD_ZPOOL_MAGIC_NUMBER) {
    fd_seterr("Bad magic number","open_zpool",fname,FD_VOID);
    fd_dtsclose(&(pool->fd_stream),1); u8_free(pool);
    return NULL;}
  hi=fd_dtsread_4bytes(s); lo=fd_dtsread_4bytes(s);
  FD_SET_OID_HI(base,hi); FD_SET_OID_LO(base,lo);
  capacity=fd_dtsread_4bytes(s);
  fd_init_pool((fd_pool)pool,base,capacity,&zpool_handler,fname,rname);
  u8_free(rname);
  load=fd_dtsread_4bytes(s);
  label_loc=(fd_off_t)fd_dtsread_4bytes(s);
  if (label_loc) {
    if (fd_setpos(s,label_loc)>0) {
      label=fd_dtsread_dtype(s);
      if (FD_STRINGP(label)) pool->label=u8_strdup(FD_STRDATA(label));
      else u8_log(LOG_WARN,fd_BadFilePoolLabel,"label: %s",fd_dtype2string(label));
      fd_decref(label);}
    else {
      fd_seterr(fd_BadFilePoolLabel,"open_std_file_pool",
                u8_strdup("bad label loc"),
                FD_ERROR_VALUE);
      fd_dtsclose(&(pool->fd_stream),1);
      u8_free(rname); u8_free(pool);
      return NULL;}}
  if (fd_setpos(s,24+capacity*4)<0) {
    fd_seterr(fd_BadFilePoolLabel,"open_std_file_pool",
              u8_strdup("bad label loc"),
              FD_ERROR_VALUE);
    fd_dtsclose(&(pool->fd_stream),1);
    u8_free(rname); u8_free(pool);
    return NULL;}
  else {
    fdtype metadata=read_metadata(s);
    fdtype schemas=fd_get(metadata,schemas_slotid,FD_EMPTY_CHOICE);
    if (FD_VECTORP(schemas)) init_zpool_schemas(pool,schemas);
    fd_decref(metadata); fd_decref(schemas);}
  pool->fdp_load=load; pool->fd_offsets=NULL;
  pool->fd_read_only=read_only;
  fd_init_mutex(&(pool->fd_lock));
  update_modtime(pool);
  return (fd_pool)pool;
}

static int lock_zpool(struct FD_ZPOOL *fp,int use_mutex)
{
  if (FD_FILE_POOL_LOCKED(fp)) return 1;
  else if ((fp->fd_stream.fd_dts_flags)&(FD_DTSTREAM_READ_ONLY)) return 0;
  else {
    struct FD_DTYPE_STREAM *s=&(fp->fd_stream);
    struct stat fileinfo;
    if (use_mutex) fd_lock_struct(fp);
    /* Handle race condition by checking when locked */
    if (FD_FILE_POOL_LOCKED(fp)) {
      if (use_mutex) fd_unlock_struct(fp);
      return 1;}
    if (fd_dts_lockfile(s)==0) {
      fd_unlock_struct(fp);
      return 0;}
    fstat(s->fd_fileno,&fileinfo);
    if (fileinfo.st_mtime>fp->fd_modtime) {
      /* Make sure we're up to date. */
      if (fp->fd_offsets) reload_file_pool_cache(fp,0);
      else {
        fd_reset_hashtable(&(fp->fd_cache),-1,1);
        fd_reset_hashtable(&(fp->fdp_locks),32,1);}}
    if (use_mutex) fd_unlock_struct(fp);
    return 1;}
}

static void update_modtime(struct FD_ZPOOL *fp)
{
  struct stat fileinfo;
  if ((fstat(fp->fd_stream.fd_fileno,&fileinfo))<0)
    fp->fd_modtime=(time_t)-1;
  else fp->fd_modtime=fileinfo.st_mtime;
}

static int zpool_load(fd_pool p)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  if (FD_FILE_POOL_LOCKED(fp)) return fp->fdp_load;
  else {
    int load;
    fd_lock_struct(fp);
    if (fd_setpos(&(fp->fd_stream),16)<0) {
      fd_unlock_struct(fp);
      return -1;}
    load=fd_dtsread_4bytes(&(fp->fd_stream));
    fp->fdp_load=load;
    fd_unlock_struct(fp);
    return load;}
}

static fdtype zpool_fetch(fd_pool p,fdtype oid)
{
  fdtype value;
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  FD_OID addr=FD_OID_ADDR(oid);
  int offset=FD_OID_DIFFERENCE(addr,fp->fdp_base);
  fd_off_t data_pos;
  fd_lock_struct(fp);
  if (FD_EXPECT_FALSE(offset>=fp->fdp_load)) {
    fd_unlock_struct(fp);
    return fd_err(fd_UnallocatedOID,"file_pool_fetch",fp->fd_cid,oid);}
  else if (fp->fd_offsets) data_pos=offget(fp->fd_offsets,offset);
  else {
    if (fd_setpos(&(fp->fd_stream),24+4*offset)<0) {
      fd_unlock_struct(fp);
      return FD_ERROR_VALUE;}
    data_pos=fd_dtsread_4bytes(&(fp->fd_stream));}
  if (data_pos == 0) value=FD_EMPTY_CHOICE;
  else if (FD_EXPECT_FALSE(data_pos<24+fp->fdp_load*4)) {
    /* We got a data pointer into the file header.  This will
       happen in the (hopefully now non-existent) case where
       we've stored a >32 bit offset into a 32-bit sized location
       and it got truncated down. */
    fd_unlock_struct(fp);
    return fd_err(fd_CorruptedPool,"file_pool_fetch",fp->fd_cid,FD_VOID);}
  else {
    if (fd_setpos(&(fp->fd_stream),data_pos)<0) {
      fd_unlock_struct(fp);
      return FD_ERROR_VALUE;}
    value=read_oid_value(&(fp->fd_stream),fp->fdp_schemas,fp->fdp_n_schemas);}
  fd_unlock_struct(fp);
  return value;
}

struct POOL_FETCH_SCHEDULE {
  unsigned int vpos; fd_off_t filepos;};

static int compare_filepos(const void *x1,const void *x2)
{
  const struct POOL_FETCH_SCHEDULE *s1=x1, *s2=x2;
  if (s1->filepos<s2->filepos) return -1;
  else if (s1->filepos>s2->filepos) return 1;
  else return 0;
}

static fdtype *zpool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p; FD_OID base=p->fdp_base;
  struct FD_DTYPE_STREAM *stream=&(fp->fd_stream);
  struct POOL_FETCH_SCHEDULE *schedule=u8_alloc_n(n,struct POOL_FETCH_SCHEDULE);
  fdtype *result=u8_alloc_n(n,fdtype);
  int i=0, min_file_pos=24+fp->fdp_capacity*4, load;
  fd_lock_struct(fp); load=fp->fdp_load;
  if (fp->fd_offsets) {
    unsigned int *offsets=fp->fd_offsets;
    int i=0; while (i < n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base), file_off;
      if (FD_EXPECT_FALSE(off>=load)) {
        u8_free(result); u8_free(schedule);
        fd_unlock_struct(fp);
        fd_seterr(fd_UnallocatedOID,"file_pool_fetchn",u8_strdup(fp->fd_cid),oid);
        return NULL;}
      file_off=offget(offsets,off);
      schedule[i].vpos=i;
      if (FD_EXPECT_FALSE(file_off==0))
        /* This is okay, just an allocated but unassigned OID. */
        schedule[i].filepos=file_off;
      else if (FD_EXPECT_FALSE(file_off<min_file_pos)) {
        /* As above, we have a data pointer into the header.
           This should never happen unless a file is corrupted. */
        u8_free(result); u8_free(schedule);
        fd_unlock_struct(fp);
        fd_seterr(fd_CorruptedPool,"file_pool_fetchn",u8_strdup(fp->fd_cid),oid);
        return NULL;}
      else schedule[i].filepos=file_off;
      i++;}}
  else {
    int i=0; while (i < n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base);
      schedule[i].vpos=i;
      if (fd_setpos(stream,24+4*off)<0) {
        u8_free(schedule);
        u8_free(result);
        fd_unlock_struct(fp);
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
        fd_unlock_struct(fp);
        return NULL;}
      result[schedule[i].vpos]=
        read_oid_value(stream,fp->fdp_schemas,fp->fdp_n_schemas);
      i++;}
    else result[schedule[i++].vpos]=FD_EMPTY_CHOICE;
  u8_free(schedule);
  fd_unlock_struct(fp);
  return result;
}

static int zpool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p; FD_OID base=p->fdp_base;
  unsigned int *offsets=u8_alloc_n(n,unsigned int);
  struct FD_SCHEMA_TABLE *schemas=fp->fdp_fdp_schemas_byptr;
  int n_schemas=fp->fdp_n_schemas;
  struct FD_DTYPE_STREAM *stream=&(fp->fd_stream);
  fd_off_t endpos, pos_limit=0xFFFFFFFF;
  int i=0, retcode=n, load;
  fd_lock_struct(fp); load=fp->fdp_load;
  endpos=fd_endpos(stream);
#if HAVE_MMAP
  if (fp->fd_offsets) {
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval=munmap((fp->fd_offsets)-6,4*fp->fd_offsets_size+24);
    unsigned int *newmmap;
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),"zpool_storen:munmap %s",fp->fd_cid);
      fp->fd_offsets=NULL; errno=0;}
    else {fp->fd_offsets=NULL; fp->fd_offsets_size=0;}
    newmmap=
      mmap(NULL,(4*fp->fdp_load)+24,
           PROT_READ|PROT_WRITE,
           MAP_SHARED,stream->fd_fileno,0);
    if ((newmmap==NULL) || (newmmap==((void *)-1))) {
      u8_log(LOG_WARN,u8_strerror(errno),"zpool_storen:mmap %s",fp->fd_cid);
      fp->fd_offsets=NULL; errno=0;}
    else {fp->fd_offsets=newmmap+6; fp->fd_offsets_size=fp->fdp_load;}}
#endif
  while (i<n) {
    FD_OID oid=FD_OID_ADDR(oids[i]);
    unsigned int oid_off=FD_OID_DIFFERENCE(oid,base);
    int delta=write_oid_value(stream,values[i],schemas,n_schemas);
    if (FD_EXPECT_FALSE(oid_off>=load)) {
      fd_seterr(fd_UnallocatedOID,
                "zpool_storen",u8_strdup(fp->fd_cid),
                oids[i]);
      retcode=-1; break;}
    else if (FD_EXPECT_FALSE(delta<0)) {retcode=-1; break;}
    else if (FD_EXPECT_FALSE(((fd_off_t)(endpos+delta))>pos_limit)) {
      fd_seterr(fd_FileSizeOverflow,
                "file_pool_storen",u8_strdup(fp->fd_cid),
                oids[i]);
      retcode=-1; break;}
    offsets[i]=endpos; endpos=endpos+delta;
    i++;}
  if (retcode<0) {}
  else if (fp->fd_offsets) {
     int i=0; while (i<n) {
      FD_OID addr=FD_OID_ADDR(oids[i]);
      unsigned int oid_off=FD_OID_DIFFERENCE(addr,base);
      set_offset(fp->fd_offsets,oid_off,offsets[i]);
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
  if (fp->fd_offsets) {
    int retval=munmap((fp->fd_offsets)-6,4*fp->fd_offsets_size+24);
    unsigned int *newmmap;
    if (retval<0)  {
      u8_log(LOG_WARN,u8_strerror(errno),"zpool_storen:munmap %s",fp->fd_cid);
      fp->fd_offsets=NULL; errno=0;}
    else {fp->fd_offsets=NULL; fp->fd_offsets_size=0;}
    newmmap=
      /* When allocating an offset buffer to read, we only have to make it as
         big as the file pools load. */
      mmap(NULL,(4*fp->fdp_load)+24,
           PROT_READ,MAP_SHARED|MAP_NORESERVE,stream->fd_fileno,0);
    if ((newmmap==NULL) || (newmmap==((void *)-1))) {
      u8_log(LOG_WARN,u8_strerror(errno),"zpool_storen:mmap %s",fp->fd_cid);
      fp->fd_offsets=NULL; errno=0;}
    else {fp->fd_offsets=newmmap+6; fp->fd_offsets_size=0;}}
#else
  if ((retcode>=0) && (fp->fd_offsets)) {
    if (fd_setpos(stream,24)<0) retcode=-1;;
    else fd_dtswrite_ints(stream,fp->fd_offsets_size,offsets);}
#endif
  if (retcode>=0) {
    if (fd_setpos(stream,16)<0) retcode=1;
    else {
      fd_dtswrite_4bytes(stream,fp->fdp_load);
      fd_dtsflush(stream);
      update_modtime(fp);
      fsync(stream->fd_fileno);}}
  fd_unlock_struct(fp);
  return retcode;
}

static fdtype zpool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE; int i=0;
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  fd_lock_struct(fp);
  if (!(FD_FILE_POOL_LOCKED(fp))) lock_zpool(fp,0);
  if (fp->fdp_load+n>=fp->fdp_capacity) {
    fd_unlock_struct(fp);
    return fd_err(fd_ExhaustedPool,"zpool_alloc",p->fd_cid,FD_VOID);}
  while (i < n) {
    FD_OID new_addr=FD_OID_PLUS(fp->fdp_base,fp->fdp_load);
    fdtype new_oid=fd_make_oid(new_addr);
    FD_ADD_TO_CHOICE(results,new_oid);
    fp->fdp_load++; i++; fp->fdp_n_locks++;}
  fd_unlock_struct(fp);
  return results;
}

static int zpool_lock(fd_pool p,fdtype oids)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  int retval=lock_zpool(fp,1);
  if (retval)
    fp->fdp_n_locks=fp->fdp_n_locks+FD_CHOICE_SIZE(oids);
  return retval;
}

static int zpool_unlock(fd_pool p,fdtype oids)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  if (fp->fdp_n_locks == 0) return 0;
  else if (!(FD_FILE_POOL_LOCKED(fp))) return 0;
  else if (FD_CHOICEP(oids))
    fp->fdp_n_locks=fp->fdp_n_locks-FD_CHOICE_SIZE(oids);
  else if (FD_EMPTY_CHOICEP(oids)) {}
  else fp->fdp_n_locks--;
  if (fp->fdp_n_locks == 0) {
    fd_dts_unlockfile(&(fp->fd_stream));
    fd_reset_hashtable(&(fp->fdp_locks),0,1);}
  return 1;
}

static void zpool_setcache(fd_pool p,int level)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  if (level == 2)
    if (fp->fd_offsets) return;
    else {
      fd_dtype_stream s=&(fp->fd_stream);
      unsigned int *offsets, *newmmap;
      fd_lock_struct(fp);
      if (fp->fd_offsets) {
        fd_unlock_struct(fp);
        return;}
#if HAVE_MMAP
      newmmap=
        /* When allocating an offset buffer to read, we only have to make it as
           big as the file pools load. */
        mmap(NULL,(4*fp->fdp_load)+24,PROT_READ,
             MAP_SHARED|MAP_NORESERVE,s->fd_fileno,0);
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
        u8_log(LOG_WARN,u8_strerror(errno),"zpool_setcache:mmap %s",fp->fd_cid);
        fp->fd_offsets=NULL; errno=0;}
      else {
        fp->fd_offsets=offsets=newmmap+6;
        fp->fd_offsets_size=fp->fdp_capacity;}
#else
      fd_dts_start_read(s);
      if (fd_setpos(s,12)>0) {
        fp->fdp_load=load=fd_dtsread_4bytes(s);
        offsets=u8_alloc_n(load,unsigned int);
        fd_setpos(s,24);
        fd_dtsread_ints(s,load,offsets);
        fp->fd_offsets=offsets; fp->fd_offsets_size=load;}
#endif
      fd_unlock_struct(fp);}
  else if (level < 2) {
    if (fp->fd_offsets == NULL) return;
    else {
      int retval;
      fd_lock_struct(fp);
#if HAVE_MMAP
      /* Since we were just reading, the buffer was only as big
         as the load, not the capacity. */
      retval=munmap((fp->fd_offsets)-6,4*fp->fd_offsets_size+24);
      if (retval<0) {
        u8_log(LOG_WARN,u8_strerror(errno),"zpool_setcache:munmap %s",fp->fd_cid);
        fp->fd_offsets=NULL; errno=0;}
#else
      u8_free(fp->fd_offsets);
#endif
      fp->fd_offsets=NULL; fp->fd_offsets_size=0;}}
}

static void reload_file_pool_cache(struct FD_ZPOOL *fp,int lock)
{
#if HAVE_MMAP
#else
  fd_dtype_stream s=&(fp->fd_stream);
  /* Read new offsets table, compare it with the current, and
     only void those OIDs */
  unsigned int new_load, *offsets, *nscan, *oscan, *olim;
  if (lock) fd_lock_struct(fp);
  oscan=fp->fd_offsets; olim=oscan+fp->fd_offsets_size;
  fd_setpos(s,16); new_load=fd_dtsread_4bytes(s);
  nscan=offsets=u8_alloc_n(new_load,unsigned int);
  fd_setpos(s,24);
  fd_dtsread_ints(s,new_load,offsets);
  while (oscan < olim)
    if (*oscan == *nscan) {oscan++; nscan++;}
    else {
      FD_OID addr=FD_OID_PLUS(fp->fdp_base,(nscan-offsets));
      fdtype changed_oid=fd_make_oid(addr);
      fd_hashtable_op(&(fp->fd_cache),fd_table_replace,changed_oid,FD_VOID);
      oscan++; nscan++;}
  u8_free(fp->fd_offsets);
  fp->fd_offsets=offsets; fp->fdp_load=fp->fd_offsets_size=new_load;
  update_modtime(fp);
  if (lock) fd_unlock_struct(fp);
#endif
}

static void zpool_close(fd_pool p)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  fd_lock_struct(fp);
  fd_dtsclose(&(fp->fd_stream),1);
  if (fp->fd_offsets) {
#if HAVE_MMAP
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval=munmap((fp->fd_offsets)-6,4*fp->fd_offsets_size+24);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),"zpool_close:munmap %s",fp->fd_cid);
      errno=0;}
#else
    u8_free(fp->fd_offsets);
#endif
    fp->fd_offsets_size=0;
    fp->fd_offsets=NULL;
    fp->fd_cache_level=-1;}
  fd_unlock_struct(fp);
}

static void zpool_setbuf(fd_pool p,int bufsiz)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  fd_lock_struct(fp);
  fd_dtsbufsize(&(fp->fd_stream),bufsiz);
  fd_unlock_struct(fp);
}

static fdtype zpool_metadata(fd_pool p,fdtype md)
{
  struct FD_ZPOOL *fp=(struct FD_ZPOOL *)p;
  if (FD_VOIDP(md))
    return fd_read_pool_metadata(&(fp->fd_stream));
  else return fd_write_pool_metadata((&(fp->fd_stream)),md);
}

/* Initialization */

static struct FD_POOL_HANDLER zpool_handler={
  "file_pool", 1, sizeof(struct FD_ZPOOL), 12,
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
   NULL, /* swapout */
   zpool_metadata, /* metdata */
   NULL}; /* sync */

FD_EXPORT void fd_init_zpools_c()
{
  u8_register_source_file(_FILEINFO);
  fd_register_pool_opener
    (FD_ZPOOL_MAGIC_NUMBER,
     open_zpool,fd_read_pool_metadata,fd_write_pool_metadata);

  fd_register_pool_opener
    (FD_ZPOOL_MAGIC_NUMBER,open_zpool,
     fd_read_pool_metadata,fd_write_pool_metadata);
  schemas_slotid=fd_intern("SCHEMAS");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
