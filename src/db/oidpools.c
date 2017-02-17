/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_DTYPEIO 1

#ifndef OIDPOOL_PREFETCH_WINDOW
#ifdef FD_MMAP_PREFETCH_WINDOW
#define OIDPOOL_PREFETCH_WINDOW FD_MMAP_PREFETCH_WINDOW
#else
#define OIDPOOL_PREFETCH_WINDOW 0
#endif
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/dtypestream.h"
#include "framerd/dbfile.h"

#include <libu8/libu8.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>

#include <zlib.h>

#if (HAVE_MMAP)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#define MMAP_FLAGS MAP_SHARED
#endif

#include "dbfile_internals.h"

/* Locking oid pool streams */

#define POOLFILE_LOCKEDP(op) \
  (U8_BITP((op->pool_stream.bs_flags),FD_DTSTREAM_LOCKED))

FD_FASTOP int LOCK_POOLSTREAM(fd_oidpool op,u8_string caller)
{
  dts_lock(&(op->pool_stream));
  if (op->pool_stream.fd_fileno < 0) {
    u8_seterr("PoolStreamClosed",caller,u8_strdup(op->pool_cid));
    dts_unlock(&(op->pool_stream));
    return -1;}
  else return 1;
}

#define UNLOCK_POOLSTREAM(op) dts_unlock(&(op->pool_stream))

static void update_modtime(struct FD_OIDPOOL *fp);
static void reload_offsets(struct FD_OIDPOOL *fp,int lock,int write);
static int recover_oidpool(struct FD_OIDPOOL *);

static struct FD_POOL_HANDLER oidpool_handler;

fd_exception fd_InvalidSchemaDef=_("Invalid schema definition data");
fd_exception fd_InvalidSchemaRef=_("Invalid encoded schema reference");
fd_exception fd_SchemaInconsistency=_("Inconsistent schema reference and value data");

static fd_exception InvalidOffset=_("Invalid offset in OIDPOOL");

#ifndef FD_INIT_ZBUF_SIZE
#define FD_INIT_ZBUF_SIZE 24000
#endif

#define FD_OIDPOOL_LOAD_POS      0x10

#define FD_OIDPOOL_LABEL_POS             0x18
#define FD_OIDPOOL_METADATA_POS          0x24
#define FD_OIDPOOL_SCHEMAS_POS           0x30

#define FD_OIDPOOL_FETCHBUF_SIZE 8000

/* OIDPOOLs are the next generation of object pool data file.  While
    previous formats have all stored OIDs for years and years, OIDPOOLs
    are supposed to be the best to date and somewhat paradigmatic.
   The design focuses on performance and extensibility.  With FD_HASHINDEX,
    it became clear that storing offset sizes helped with performance and
    with the use of compression, it becomes even more relevant.
   OIDPOOLs are also the first native files to support files >4GB and
    use three different offset models, described below.
*/

/* Layout of new OIDPOOL files
   [256 bytes of header]
   [offset table]
   ...data items...

   Header consists of

   0x00 XXXX     Magic number
   0x04 XXXX     Base OID of pool (8 bytes)
        XXXX
   0x0c XXXX     Capacity of pool
   0x10 XXXX     Load of pool
   0x14 XXXX     Pool information bits/flags
   0x18 XXXX     file offset of the pool label (8 bytes)
        XXXX
   0x20 XXXX     size of label dtype representation (in bytes)
   0x24 XXXX     file offset of pool metadata (8 bytes)
        XXXX
   0x2c XXXX     size of pool metadata dtype representation
   0x30 XXXX     file offset of the schemas record (8 bytes)
        XXXX
   0x38 XXXX     size of schemas record dtype representation
   0x3c XXXX     pool creation time_t (8 bytes)
        XXXX
   0x44 XXXX     pool repack time_t (8 bytes)
        XXXX
   0x4c XXXX     pool modification time_t (8 bytes)
        XXXX
   0x54 XXXX     repack generation (8 bytes)
        XXXX

   The flags are divided as follows:
     MASK        INTERPRETATION
     0x0003      Structure of offsets table:
                    0= 4 bytes of position followed by 4 bytes of length (32B form)
                    1= 5 bytes of position followed by 3 bytes of length (40B form)
                    2= 8 bytes of position followed by 4 bytes of length (64B form)
                    3= reserved for future use
     0x001c      Compression function for blocks:
                    0= no compression
                    1= libz compression
                    2= libbz2 compression -- Not yet implemented
                    3-7 reserved for future use
     0x0020      Set if this pool is intended to be read-only

   The offsets block starts at 0x100 and goes for either capacity*8 or capacity*12
    bytes.  The offset values are stored as pairs of big-endian binary representations.
    For the 32B and 64B forms, these are just straightforward integers of the same size.
    For the 40B form, which is designed to better use memory and cache, the high 8
    bits of the second word are taken as the high eight bits of a forty-byte offset.

*/

/* Getting chunk refs */

typedef long long int ll;
typedef unsigned long long ull;

static FD_CHUNK_REF read_chunk_ref(struct FD_DTYPE_STREAM *stream,
                                   fd_off_t base,fd_offset_type offtype,
                                   unsigned int offset);

static int get_chunk_ref_size(fd_oidpool p)
{
  switch (p->pool_offtype) {
  case FD_B32: case FD_B40: return 8;
  case FD_B64: return 12;}
  return -1;
}

static size_t get_maxpos(fd_oidpool p)
{
  switch (p->pool_offtype) {
  case FD_B32:
    return ((size_t)(((size_t)1)<<32));
  case FD_B40:
    return ((size_t)(((size_t)1)<<40));
  case FD_B64: 
    return ((size_t)(((size_t)1)<<63));
  default:
    return -1;}
}

/* Schema functions */

/* Making and opening oidpools */

static int init_schemas(fd_oidpool,fdtype);

static fd_pool open_oidpool(u8_string fname,int read_only)
{
  FD_OID base=FD_NULL_OID_INIT;
  unsigned int hi, lo, magicno, capacity, load, flags;
  fd_off_t label_loc, schemas_loc; fdtype label;
  struct FD_OIDPOOL *pool=u8_alloc(struct FD_OIDPOOL);
  struct FD_DTYPE_STREAM *stream=&(pool->pool_stream);
  fd_dtstream_mode mode=
    ((read_only) ? (FD_DTSTREAM_READ) : (FD_DTSTREAM_MODIFY));
  u8_string rname=u8_realpath(fname,NULL);
  fd_init_dtype_file_stream(stream,fname,mode,fd_filedb_bufsize);
  /* See if it ended up read only */
  if ((stream->bs_flags)&(FD_DTSTREAM_READ_ONLY)) read_only=1;
  pool->pool_stream.dts_mallocd=0;
  magicno=fd_dtsread_4bytes(stream);
  /* Read POOL base etc. */
  hi=fd_dtsread_4bytes(stream); lo=fd_dtsread_4bytes(stream);
  FD_SET_OID_HI(base,hi); FD_SET_OID_LO(base,lo);
  pool->pool_capacity=capacity=fd_dtsread_4bytes(stream);
  pool->pool_load=load=fd_dtsread_4bytes(stream);
  flags=fd_dtsread_4bytes(stream);
  pool->pool_xformat=flags;
  if ((read_only==0) && ((flags)&(FD_OIDPOOL_READ_ONLY))) {
    /* If the pool is intrinsically read-only make it so. */
    read_only=1; fd_dtsclose(stream,1);
    fd_init_dtype_file_stream(stream,fname,FD_DTSTREAM_READ,fd_filedb_bufsize);
    fd_setpos(stream,FD_OIDPOOL_LABEL_POS);}
  pool->pool_offtype=(fd_offset_type)((flags)&(FD_OIDPOOL_OFFMODE));
  pool->pool_compression=
    (fd_compression_type)(((flags)&(FD_OIDPOOL_COMPRESSION))>>3);
  fd_init_pool((fd_pool)pool,base,capacity,&oidpool_handler,fname,rname);
  u8_free(rname); /* Done with this */
  if (magicno==FD_FILE_POOL_TO_RECOVER) {
    u8_log(LOG_WARN,fd_RecoveryRequired,"Recovering the file pool %s",fname);
    if (recover_oidpool(pool)<0) {
      fd_seterr(fd_MallocFailed,"open_oidpool",NULL,FD_VOID);
      return NULL;}}
  /* Get the label */
  label_loc=fd_dtsread_8bytes(stream);
  /* label_size=*/ fd_dtsread_4bytes(stream);
  /* Skip the metadata field */
  fd_dtsread_8bytes(stream);
  fd_dtsread_4bytes(stream); /* Ignore size */
  /* Read and initialize the schemas_loc */
  schemas_loc=fd_dtsread_8bytes(stream);
  fd_dtsread_4bytes(stream); /* Ignore size */
  if (label_loc) {
    if (fd_setpos(stream,label_loc)>0) {
      label=fd_dtsread_dtype(stream);
      if (FD_STRINGP(label)) pool->pool_label=u8_strdup(FD_STRDATA(label));
      else u8_log(LOG_WARN,fd_BadFilePoolLabel,fd_dtype2string(label));
      fd_decref(label);}
    else {
      fd_seterr(fd_BadFilePoolLabel,"open_oidpool",
                u8_strdup("bad label loc"),
                FD_INT(label_loc));
      fd_dtsclose(stream,1);
      u8_free(rname); u8_free(pool);
      return NULL;}}
  if (schemas_loc) {
    fdtype schemas;
    fd_setpos(stream,schemas_loc);
    schemas=fd_dtsread_dtype(stream);
    init_schemas(pool,schemas);
    fd_decref(schemas);}
  else init_schemas(pool,FD_VOID);
  /* Offsets size is the malloc'd size (in unsigned ints) of the offsets.
     We don't fill this in until we actually need it. */
  pool->pool_offsets=NULL; pool->pool_offsets_size=0;
  pool->pool_read_only=read_only;
  pool->pool_mmap=NULL; pool->pool_mmap_size=0;
  fd_init_mutex(&(pool->pool_lock));
  update_modtime(pool);
  return (fd_pool)pool;
}

static void update_modtime(struct FD_OIDPOOL *fp)
{
  struct stat fileinfo;
  if ((fstat(fp->pool_stream.fd_fileno,&fileinfo))<0)
    fp->pool_modtime=(time_t)-1;
  else fp->pool_modtime=fileinfo.st_mtime;
}

/* Maintaing the schema table */

static int init_schema_entry(struct FD_SCHEMA_ENTRY *e,int pos,fdtype vec);
static int compare_schema_vals(const void *p1,const void *p2);
static void sort_schema(fdtype *v,int n);

static int init_schemas(fd_oidpool op,fdtype schema_vec)
{
  if (!(FD_VECTORP(schema_vec))) {
    op->pool_n_schemas=0; op->pool_schemas=NULL; op->pool_schbyval=NULL;
    op->pool_max_slotids=0;
    return 0;}
  else {
    int i=0, n=FD_VECTOR_LENGTH(schema_vec), max_slotids=0;
    struct FD_SCHEMA_ENTRY *schemas=u8_alloc_n(n,FD_SCHEMA_ENTRY);
    struct FD_SCHEMA_LOOKUP *schbyval=u8_alloc_n(n,FD_SCHEMA_LOOKUP);
    while (i<n) {
      fdtype slotids=FD_VECTOR_REF(schema_vec,i);
      int n_slotids;
      if (FD_VECTORP(slotids)) n_slotids=FD_VECTOR_LENGTH(slotids);
      else {
        u8_free(schemas); u8_free(schbyval);
        return fd_reterr
          (fd_InvalidSchemaDef,"oidpool/init_schemas",NULL,schema_vec);}
      if (n_slotids>max_slotids) max_slotids=n_slotids;
      init_schema_entry(&(schemas[i]),i,slotids);
      schbyval[i].fd_schema_id=i; schbyval[i].fd_nslots=n_slotids;
      schbyval[i].fd_slotids=schemas[i].fd_slotids;
      i++;}
    op->pool_schemas=schemas; op->pool_schbyval=schbyval;
    op->pool_max_slotids=max_slotids; op->pool_n_schemas=n;
    qsort(schbyval,n,sizeof(struct FD_SCHEMA_LOOKUP),
          compare_schema_vals);
    return n;}
}

static int init_schema_entry(struct FD_SCHEMA_ENTRY *e,int pos,fdtype vec)
{
  int i=0, len=FD_VECTOR_LENGTH(vec);
  fdtype *slotids=u8_alloc_n((len+1),fdtype);
  unsigned int *mapin=u8_alloc_n(len,unsigned int);
  unsigned int *mapout=u8_alloc_n(len,unsigned int);
  e->fd_schema_id=pos; e->fd_nslots=len; e->normal=fd_incref(vec);
  e->fd_slotids=slotids; e->fd_slotmapin=mapin; e->fd_slotmapout=mapout;
  while (i<len) {
    fdtype val=FD_VECTOR_REF(vec,i);
    slotids[i]=fd_incref(val); i++;}
  sort_schema(slotids,len);
  /* This will make it fast to get the pos from the schema pointer */
  slotids[len]=FD_INT(pos);
  i=0; while (i<len) {
    fdtype val=FD_VECTOR_REF(vec,i);
    int j=0; while (j<len)
      if (slotids[j]==val) break;
      else j++;
    /* assert(i<len); assert(j<len); */
    mapin[i]=j; mapout[j]=i;
    i++;}
  return 0;
}

static int compare_schema_vals(const void *p1,const void *p2)
{
  struct FD_SCHEMA_LOOKUP *se1=(struct FD_SCHEMA_LOOKUP *)p1;
  struct FD_SCHEMA_LOOKUP *se2=(struct FD_SCHEMA_LOOKUP *)p2;
  if (se1->fd_nslots<se2->fd_nslots) return -1;
  else if (se1->fd_nslots>se2->fd_nslots) return 1;
  else {
    fdtype *slotids1=se1->fd_slotids, *slotids2=se2->fd_slotids;
    int i=0, n=se1->fd_nslots; while (i<n) {
      if (slotids1[i]<slotids2[i]) return -1;
      else if (slotids1[i]<slotids2[i]) return 1;
      else i++;}
    return 0;}
}

static int compare_schemas(struct FD_SCHEMA_LOOKUP *e,fdtype *slotids,int n)
{
  if (n<e->fd_nslots) return -1;
  else if (n>e->fd_nslots) return 1;
  else {
    fdtype *oslotids=e->fd_slotids;
    int i=0; while (i<n)
      if (slotids[i]<oslotids[i]) return -1;
      else if (slotids[i]>oslotids[i]) return 1;
      else i++;
    return 0;}
}

static int find_schema_byval(fd_oidpool op,fdtype *slotids,int n)
{
  int size=op->pool_n_schemas, cmp;
  struct FD_SCHEMA_LOOKUP *table=op->pool_schbyval, *max=table+size;
  struct FD_SCHEMA_LOOKUP *bot=table, *top=max, *middle=bot+(size)/2;
  while (top>bot) {
    cmp=compare_schemas(middle,slotids,n);
    if (cmp==0) return middle->fd_schema_id;
    else if (cmp<0) {
      top=middle-1; middle=bot+(top-bot)/2;}
    else {
      bot=middle+1; middle=bot+(top-bot)/2;}}
  if ((middle<max) && (middle>=table) &&
      (compare_schemas(middle,slotids,n)==0))
    return middle->fd_schema_id;
  else return -1;
}

FD_FASTOP void lispv_swap(fdtype *a,fdtype *b)
{
  fdtype t;
  t = *a;
  *a = *b;
  *b = t;
}

static void sort_schema(fdtype *v,int n)
{
  unsigned i, j, ln, rn;
  while (n > 1) {
    lispv_swap(&v[0], &v[n/2]);
    for (i = 0, j = n; ; ) {
      do --j; while (v[j] > v[0]);
      do ++i; while (i < j && v[i] < v[0]);
      if (i >= j) break; else {}
      lispv_swap(&v[i], &v[j]);}
    lispv_swap(&v[j], &v[0]);
    ln = j;
    rn = n - ++j;
    if (ln < rn) {
      fd_sort_schema(ln, v); v += j; n = rn;}
    else {fd_sort_schema(rn,v + j); n = ln;}}
}

U8_MAYBE_UNUSED static int schema_sortedp(fdtype *v,int n)
{
  int i=0; while (i<n-1)
    if (v[i]<v[i+1]) i++;
    else return 0;
  return 1;
}

/* Lock the underlying OIDpool */

static int lock_oidpool_file(struct FD_OIDPOOL *op,int use_mutex)
{
  if (FD_OIDPOOL_LOCKED(op)) return 1;
  else if ((op->pool_stream.bs_flags)&(FD_DTSTREAM_READ_ONLY))
    return 0;
  /* Locked here means that the underlying file is locked */
  if (POOLFILE_LOCKEDP(op)) return 1;
  else if (U8_BITP((op->pool_stream.bs_flags),FD_DTSTREAM_READ_ONLY))
    return 0;
  else {
    struct FD_DTYPE_STREAM *s=&(op->pool_stream);
    struct stat fileinfo;
    if (use_mutex) fd_lock_pool(op);
    if (POOLFILE_LOCKEDP(op)) {
      /* Another thread got here first */
      if (use_mutex) fd_unlock_pool(op);
      return 1;}
    LOCK_POOLSTREAM(op,"lock_oidpool_file");
    if (fd_dts_lockfile(s)==0) {
      if (use_mutex) fd_unlock_pool(op);
      UNLOCK_POOLSTREAM(op);
      return 0;}
    fstat( s->fd_fileno, &fileinfo);
    if ( fileinfo.st_mtime > op->pool_modtime ) {
      /* Make sure we're up to date. */
      fd_setpos(&(op->pool_stream),FD_OIDPOOL_LOAD_POS);
      op->pool_load=fd_dtsread_4bytes(&(op->pool_stream));
      if (op->pool_offsets) reload_offsets(op,0,0);
      else {
        fd_reset_hashtable(&(op->pool_cache),-1,1);
        fd_reset_hashtable(&(op->pool_changes),32,1);}}
    if (use_mutex) fd_unlock_pool(op);
    UNLOCK_POOLSTREAM(op);
    return 1;}
}

/* OIDPOOL operations */

FD_EXPORT int fd_make_oidpool
  (u8_string fname,u8_string label,
   FD_OID base,unsigned int capacity,unsigned int load,
   unsigned int flags,fdtype schemas_init,fdtype metadata_init,
   time_t ctime,time_t mtime,int cycles)
{
  time_t now=time(NULL);
  fd_off_t schemas_pos=0, metadata_pos=0, label_pos=0;
  size_t schemas_size=0, metadata_size=0, label_size=0;
  struct FD_DTYPE_STREAM _stream, *stream=
    fd_init_dtype_file_stream(&_stream,fname,FD_DTSTREAM_CREATE,8192);
  fd_offset_type offtype=(fd_offset_type)((flags)&(FD_OIDPOOL_OFFMODE));
  if (stream==NULL) return -1;
  else if ((stream->bs_flags)&FD_DTSTREAM_READ_ONLY) {
    fd_seterr3(fd_CantWrite,"fd_make_oidpool",u8_strdup(fname));
    fd_dtsclose(stream,1);
    return -1;}
  stream->dts_mallocd=0;
  fd_setpos(stream,0);
  fd_dtswrite_4bytes(stream,FD_OIDPOOL_MAGIC_NUMBER);
  fd_dtswrite_4bytes(stream,FD_OID_HI(base));
  fd_dtswrite_4bytes(stream,FD_OID_LO(base));
  fd_dtswrite_4bytes(stream,capacity);
  fd_dtswrite_4bytes(stream,load);
  fd_dtswrite_4bytes(stream,flags);

  fd_dtswrite_8bytes(stream,0); /* Pool label */
  fd_dtswrite_4bytes(stream,0); /* Pool label */

  fd_dtswrite_8bytes(stream,0); /* metdata */
  fd_dtswrite_4bytes(stream,0); /* metdata */

  fd_dtswrite_8bytes(stream,0); /* schema data */
  fd_dtswrite_4bytes(stream,0); /* schema data */

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

  /* Write the number of repack cycles */
  if (mtime<0) mtime=now;
  fd_dtswrite_4bytes(stream,0);
  fd_dtswrite_4bytes(stream,cycles);

  /* Fill the rest of the space. */
  {
    int i=0, bytes_to_write=256-fd_getpos(stream);
    while (i<bytes_to_write) {
      fd_dtswrite_byte(stream,0); i++;}}

  /* Write the top level bucket table */
  {
    int i=0;
    if ((offtype==FD_B32) || (offtype==FD_B40))
      while (i<capacity) {
        fd_dtswrite_4bytes(stream,0);
        fd_dtswrite_4bytes(stream,0);
        i++;}
    else {
      while (i<capacity) {
        fd_dtswrite_8bytes(stream,0);
        fd_dtswrite_4bytes(stream,0);
        i++;}}}

  if (label) {
    int len=strlen(label);
    label_pos=fd_getpos(stream);
    fd_dtswrite_byte(stream,dt_string);
    fd_dtswrite_4bytes(stream,len);
    dts_write_bytes(stream,label,len);
    label_size=fd_getpos(stream)-label_pos;}

  /* Write the metdata */
  if (FD_SLOTMAPP(metadata_init)) {
    metadata_pos=fd_getpos(stream);
    fd_dtswrite_dtype(stream,metadata_init);
    metadata_size=fd_getpos(stream)-metadata_pos;}

  /* Write the schemas */
  if (FD_VECTORP(schemas_init)) {
    schemas_pos=fd_getpos(stream);
    fd_dtswrite_dtype(stream,schemas_init);
    schemas_size=fd_getpos(stream)-schemas_pos;}

  if (label_pos) {
    fd_setpos(stream,FD_OIDPOOL_LABEL_POS);
    fd_dtswrite_8bytes(stream,label_pos);
    fd_dtswrite_4bytes(stream,label_size);}
  if (metadata_pos) {
    fd_setpos(stream,FD_OIDPOOL_METADATA_POS);
    fd_dtswrite_8bytes(stream,metadata_pos);
    fd_dtswrite_4bytes(stream,metadata_size);}
  if (schemas_pos) {
    fd_setpos(stream,FD_OIDPOOL_SCHEMAS_POS);
    fd_dtswrite_8bytes(stream,schemas_pos);
    fd_dtswrite_4bytes(stream,schemas_size);}

  fd_dtsflush(stream);
  fd_dtsclose(stream,1);
  return 0;
}

/* Methods */

static int oidpool_load(fd_pool p)
{
  fd_oidpool fp=(fd_oidpool)p;
  if (FD_OIDPOOL_LOCKED(fp))
    /* If we have the file locked, the stored load is good. */
    return fp->pool_load;
  else {
    /* Otherwise, we need to read the load from the file */
    int load;
    if (LOCK_POOLSTREAM(fp,"oidpool_load")<0)
      return -1;
    else if (fd_setpos(&(fp->pool_stream),16)<0) {
      UNLOCK_POOLSTREAM(fp);
      return -1;}
    else {
      load=fd_dtsread_4bytes(&(fp->pool_stream));
      fp->pool_load=load;
      UNLOCK_POOLSTREAM(fp);
      return load;}}
}

static fdtype read_oid_value(fd_oidpool op,
                             fd_byte_input in,
                             const u8_string cxt)
{
  int zip_code;
  zip_code=fd_read_zint(in);
  if (FD_EXPECT_FALSE(zip_code>(op->pool_n_schemas)))
    return fd_err(fd_InvalidSchemaRef,"oidpool_fetch",op->pool_cid,FD_VOID);
  else if (zip_code==0)
    return fd_read_dtype(in);
  else {
    struct FD_SCHEMA_ENTRY *se=&(op->pool_schemas[zip_code-1]);
    int n_vals=fd_read_zint(in), n_slotids=se->fd_nslots;
    if (FD_EXPECT_TRUE(n_vals==n_slotids)) {
      fdtype *values=u8_alloc_n(n_vals,fdtype);
      unsigned int i=0, *mapin=se->fd_slotmapin;
      /* We reorder the values coming in to agree with the
         schema sorting done in memory for fast lookup. That
         translation is stored in the mapin field. */
      while (i<n_vals) {
        values[mapin[i]]=fd_read_dtype(in); i++;}
      return fd_make_schemap(NULL,n_vals,FD_SCHEMAP_SORTED|FD_SCHEMAP_TAGGED,
                             se->fd_slotids,values);}
    else return fd_err(fd_SchemaInconsistency,cxt,op->pool_cid,FD_VOID);}
}

static fdtype read_oid_value_at(fd_oidpool op,
                                FD_CHUNK_REF ref,
                                const u8_string cxt,
                                u8_mutex *unlock)
{
  if (ref.off==0) return FD_VOID;
  else if ( (op->pool_compression==FD_NOCOMPRESS) && (op->pool_mmap) ) {
      FD_BYTE_INPUT in;
      if (unlock) u8_unlock_mutex(unlock);
      FD_INIT_BYTE_INPUT(&in,op->pool_mmap+ref.off,ref.size);
      return read_oid_value(op,&in,cxt);}
  else {
    unsigned char _buf[FD_OIDPOOL_FETCHBUF_SIZE], *buf; int free_buf=0;
    if (op->pool_mmap) buf=op->pool_mmap+ref.off;
    else if (ref.size>FD_OIDPOOL_FETCHBUF_SIZE) {
      buf=read_chunk(&(op->pool_stream),op->pool_mmap,
                     ref.off,ref.size,NULL,unlock);
      free_buf=1;}
    else buf=read_chunk(&(op->pool_stream),op->pool_mmap,
                        ref.off,ref.size,_buf,unlock);
    if (buf==NULL) return FD_ERROR_VALUE;
    else if (op->pool_compression==FD_NOCOMPRESS)
      if (free_buf) {
        FD_BYTE_INPUT in;
        FD_INIT_BYTE_INPUT(&in,buf,ref.size);
        fdtype result=read_oid_value(op,&in,cxt);
        u8_free(buf);
        return result;}
      else {
        FD_BYTE_INPUT in;
        FD_INIT_BYTE_INPUT(&in,buf,ref.size);
        return read_oid_value(op,&in,cxt);}
    else {
      unsigned char _ubuf[FD_OIDPOOL_FETCHBUF_SIZE*3], *ubuf=_ubuf;
      unsigned int ubuf_size=FD_OIDPOOL_FETCHBUF_SIZE*3;
      switch (op->pool_compression) {
      case FD_ZLIB:
        if (ref.size>FD_OIDPOOL_FETCHBUF_SIZE)
          ubuf=do_zuncompress(buf,ref.size,&ubuf_size,NULL);
        else ubuf=do_zuncompress(buf,ref.size,&ubuf_size,_ubuf);
        break;
      default:
        if (free_buf) u8_free(buf);
        if (ubuf!=_ubuf) u8_free(ubuf);
        return fd_err(_("Bad compress level"),"oidpool_fetch",op->pool_cid,
                      FD_VOID);}
      if (ubuf==NULL) {
        if (free_buf) u8_free(buf);
        if (ubuf!=_ubuf) u8_free(ubuf);
        return FD_ERROR_VALUE;}
      else if ((free_buf) || (ubuf!=_ubuf)) {
        FD_BYTE_INPUT in; fdtype result;
        FD_INIT_BYTE_INPUT(&in,ubuf,ubuf_size);
        result=read_oid_value(op,&in,cxt);
        if (free_buf) u8_free(buf);
        if (ubuf!=_ubuf) u8_free(ubuf);
        return result;}
      else {
        FD_BYTE_INPUT in;
        FD_INIT_BYTE_INPUT(&in,ubuf,ubuf_size);
        return read_oid_value(op,&in,cxt);}}}
}

static fdtype oidpool_fetch(fd_pool p,fdtype oid)
{
  fd_oidpool op=(fd_oidpool)p;
  FD_OID addr=FD_OID_ADDR(oid);
  int offset=FD_OID_DIFFERENCE(addr,op->pool_base);
  if (FD_EXPECT_FALSE(offset>=op->pool_load)) {
    /* Double check by going to disk */
    if (offset>=(oidpool_load(p)))
      return fd_err(fd_UnallocatedOID,"file_pool_fetch",op->pool_cid,oid);}
  if (op->pool_offsets) {
    FD_CHUNK_REF ref=get_chunk_ref(&(op->pool_stream),op->pool_offsets,
                                   256,op->pool_offtype,
                                   offset,STREAM_UNLOCKED);
    if (ref.off<0) return FD_ERROR_VALUE;
    else if (ref.off==0)
      return FD_EMPTY_CHOICE;
    else {
      fdtype value;
      dts_lock(&(op->pool_stream));
      value=read_oid_value_at(op,ref,"oidpool_fetch",
                              &(op->pool_stream.stream_lock));
      return value;}}
  else {
    dts_lock(&(op->pool_stream)); {
      FD_CHUNK_REF ref=read_chunk_ref(&(op->pool_stream),
                                      256,op->pool_offtype,
                                      offset);
      if (ref.off<=0) dts_unlock(&(op->pool_stream));
      if (ref.off<0)
        return FD_ERROR_VALUE;
      else if (ref.off==0)
        return FD_EMPTY_CHOICE;
      else {
        fdtype value;
        value=read_oid_value_at(op,ref,"oidpool_fetch",
                                &(op->pool_stream.stream_lock));
        return value;}}}
}
struct OIDPOOL_FETCH_SCHEDULE {
  unsigned int value_at; FD_CHUNK_REF location;};

static int compare_offsets(const void *x1,const void *x2)
{
  const struct OIDPOOL_FETCH_SCHEDULE *s1=x1, *s2=x2;
  if (s1->location.off<s2->location.off) return -1;
  else if (s1->location.off>s2->location.off) return 1;
  else return 0;
}

static fdtype *oidpool_fetchn(fd_pool p,int n,fdtype *oids)
{
  fd_oidpool op=(fd_oidpool)p; FD_OID base=p->pool_base;
  fdtype *values=u8_alloc_n(n,fdtype);
  if (op->pool_offsets==NULL) {
    /* Don't bother being clever if you don't even have an offsets
       table.  This could be fixed later for small memory implementations. */
    int i=0; while (i<n) {
      values[i]=oidpool_fetch(p,oids[i]); i++;}
    return values;}
  else {
    struct OIDPOOL_FETCH_SCHEDULE *schedule=
      u8_alloc_n(n,struct OIDPOOL_FETCH_SCHEDULE);
    dts_lock(&(op->pool_stream));
    int i=0;
    while (i<n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base);
      schedule[i].value_at=i;
      schedule[i].location=get_chunk_ref(NULL,op->pool_offsets,
                                         256,op->pool_offtype,
                                         off,STREAM_LOCKED);
      if (schedule[i].location.off<0) {
        fd_seterr(InvalidOffset,"oidpool_fetchn",p->pool_cid,oid);
        u8_free(schedule); u8_free(values);
        dts_unlock(&(op->pool_stream));
        return NULL;}
      else i++;}
    /* Note that we sort even if we're mmaped in order to take
       advantage of page locality. */
    qsort(schedule,n,sizeof(struct OIDPOOL_FETCH_SCHEDULE),
          compare_offsets);
    i=0; while (i<n) {
      fdtype value;
#if ((OIDPOOL_PREFETCH_WINDOW)>0)
      if (op->pool_mmap) {
        unsigned char *fd_vecelts=op->pool_mmap;
        int j=i, lim=(((i+OIDPOOL_PREFETCH_WINDOW)>n) ? (n) : (i+4));
        while (j<lim) {
          FD_PREFETCH(&(fd_vecelts[schedule[j].location.off]));
          j++;}}
#endif
      value=read_oid_value_at(op,schedule[i].location,"oidpool_fetchn",NULL);
      if (FD_ABORTP(value)) {
        int j=0; while (j<i) { fd_decref(values[j]); j++;}
        u8_free(schedule); u8_free(values);
        fd_push_error_context("oidpool_fetchn/read",oids[schedule[i].value_at]);
        dts_unlock(&(op->pool_stream));
        return NULL;}
      else values[schedule[i].value_at]=value;
      i++;}
    dts_unlock(&(op->pool_stream));
    u8_free(schedule);
    return values;}
}

struct OIDPOOL_SAVEINFO {
  FD_CHUNK_REF chunk; unsigned int oidoff;} OIDPOOL_SAVEINFO;

static int compare_oidoffs(const void *p1,const void *p2)
{
  struct OIDPOOL_SAVEINFO *si1=(struct OIDPOOL_SAVEINFO *)p1;
  struct OIDPOOL_SAVEINFO *si2=(struct OIDPOOL_SAVEINFO *)p2;
  if (si1->oidoff<si2->oidoff) return -1;
  else if (si1->oidoff>si2->oidoff) return 1;
  else return 0;
}

static int get_schema_id(fd_oidpool op,fdtype value)
{
  if ( (FD_SCHEMAPP(value)) && (FD_SCHEMAP_SORTEDP(value)) ) {
    struct FD_SCHEMAP *sm=(fd_schemap)value;
    fdtype *slotids=sm->table_schema, size=sm->table_size;
    if (sm->schemap_tagged) {
      fdtype pos=slotids[size];
      int intpos=fd_getint(pos);
      if ((intpos<op->pool_n_schemas) &&
          (op->pool_schemas[intpos].fd_slotids==slotids))
        return intpos;}
    return find_schema_byval(op,slotids,size);}
  else if (FD_SLOTMAPP(value)) {
    fdtype _tmp_slotids[32], *tmp_slotids;
    struct FD_SLOTMAP *sm=(fd_slotmap)value;
    int i=0, size=FD_XSLOTMAP_SIZE(sm);
    if (size<32)
      tmp_slotids=_tmp_slotids;
    else tmp_slotids=u8_alloc_n(size,fdtype);
    while (i<size) {
      tmp_slotids[i]=sm->sm_keyvals[i].fd_kvkey; i++;}
    /* assert(schema_sortedp(tmp_slotids,size)); */
    if (tmp_slotids==_tmp_slotids)
      return find_schema_byval(op,tmp_slotids,size);
    else {
      int retval=find_schema_byval(op,tmp_slotids,size);
      u8_free(tmp_slotids);
      return retval;}}
  else return -1;
}

static int oidpool_write_value(fdtype value,fd_dtype_stream stream,
                               fd_oidpool p,struct FD_BYTE_OUTPUT *tmpout,
                               unsigned char **zbuf,int *zbuf_size)
{
  if ((p->pool_compression==FD_NOCOMPRESS) && (p->pool_n_schemas==0)) {
    fd_dtswrite_byte(stream,0);
    return 1+fd_dtswrite_dtype(stream,value);}
  tmpout->bs_bufptr=tmpout->bs_bufstart;
  if (p->pool_n_schemas==0) {
    fd_write_byte(tmpout,0);
    fd_write_dtype(tmpout,value);}
  else if ((FD_SCHEMAPP(value)) || (FD_SLOTMAPP(value))) {
    int schema_id=get_schema_id(p,value);
    if (schema_id<0) {
      fd_write_byte(tmpout,0);
      fd_write_dtype(tmpout,value);}
    else {
      struct FD_SCHEMA_ENTRY *se=&(p->pool_schemas[schema_id]);
      fd_write_zint(tmpout,schema_id+1);
      if (FD_SCHEMAPP(value)) {
        struct FD_SCHEMAP *sm=(fd_schemap)value;
        fdtype *values=sm->schema_values;
        int i=0, size=sm->table_size;
        fd_write_zint(tmpout,size);
        while (i<size) {
          fd_write_dtype(tmpout,values[se->fd_slotmapout[i]]);
          i++;}}
      else {
        struct FD_SLOTMAP *sm=(fd_slotmap)value;
        struct FD_KEYVAL *data=sm->sm_keyvals;
        int i=0, size=FD_XSLOTMAP_SIZE(sm);
        fd_write_zint(tmpout,size);
        while (i<size) {
          fd_write_dtype(tmpout,data[se->fd_slotmapin[i]].fd_keyval);
          i++;}}}}
  else {
    fd_write_byte(tmpout,0);
    fd_write_dtype(tmpout,value);}
  if (p->pool_compression==FD_NOCOMPRESS) {
    dts_write_bytes(stream,tmpout->bs_bufstart,tmpout->bs_bufptr-tmpout->bs_bufstart);
    return tmpout->bs_bufptr-tmpout->bs_bufstart;}
  else if (p->pool_compression==FD_ZLIB) {
    unsigned char _cbuf[FD_OIDPOOL_FETCHBUF_SIZE], *cbuf;
    int cbuf_size=FD_OIDPOOL_FETCHBUF_SIZE;
    cbuf=do_zcompress(tmpout->bs_bufstart,tmpout->bs_bufptr-tmpout->bs_bufstart,
                      &cbuf_size,_cbuf,9);
    dts_write_bytes(stream,cbuf,cbuf_size);
    if (cbuf!=_cbuf) u8_free(cbuf);
    return cbuf_size;}
  else {
    u8_log(LOG_WARN,_("Out of luck"),
           "Compressed oidpools of this type are not yet yet supported");
    exit(-1);}
}

static int oidpool_finalize
  (struct FD_OIDPOOL *fp,fd_dtype_stream stream,
   int n,struct OIDPOOL_SAVEINFO *saveinfo,
   unsigned int load);

static int oidpool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  fd_oidpool op=(fd_oidpool)p;
  struct FD_DTYPE_STREAM *stream=&(op->pool_stream);
  if ((LOCK_POOLSTREAM(op,"oidpool_storen"))<0) return -1;
  double started=u8_elapsed_time();
  u8_log(fddb_loglevel+1,"OIDPoolStore",
         "Storing %d oid values in oidpool %s",n,p->pool_cid);
  struct OIDPOOL_SAVEINFO *saveinfo=
    u8_alloc_n(n,struct OIDPOOL_SAVEINFO);
  struct FD_BYTE_OUTPUT tmpout;
  unsigned char *zbuf=u8_malloc(FD_INIT_ZBUF_SIZE);
  unsigned int i=0, zbuf_size=FD_INIT_ZBUF_SIZE;
  unsigned int init_buflen=2048*n;
  fd_off_t endpos, recovery_pos;
  size_t maxpos=get_maxpos(op);
  FD_OID base=op->pool_base;
  if (init_buflen>262144) init_buflen=262144;
  FD_INIT_BYTE_OUTPUT(&tmpout,init_buflen);
  endpos=dts_endpos(stream);
  if ((op->pool_xformat)&(FD_OIDPOOL_DTYPEV2))
    tmpout.bs_flags=tmpout.bs_flags|FD_DTYPEV2;
  while (i<n) {
    FD_OID addr=FD_OID_ADDR(oids[i]);
    fdtype value=values[i];
    int n_bytes=oidpool_write_value(value,stream,op,&tmpout,&zbuf,&zbuf_size);
    if (n_bytes<0) {
      u8_free(zbuf);
      u8_free(saveinfo);
      u8_free(tmpout.bs_bufstart);
      UNLOCK_POOLSTREAM(op);
      return n_bytes;}
    if ((endpos+n_bytes)>=maxpos) {
      u8_free(zbuf); u8_free(saveinfo); u8_free(tmpout.bs_bufstart);
      u8_seterr(fd_DataFileOverflow,"oidpool_storen",
                u8_strdup(p->pool_cid));
      UNLOCK_POOLSTREAM(op);
      return -1;}

    saveinfo[i].chunk.off=endpos; saveinfo[i].chunk.size=n_bytes;
    saveinfo[i].oidoff=FD_OID_DIFFERENCE(addr,base);

    endpos=endpos+n_bytes;
    i++;}
  u8_free(tmpout.bs_bufstart);
  u8_free(zbuf);

  /* Now, write recovery information, which lets the state of the pool
     be reconstructed if something goes wrong while storing the
     offsets table. */

  /* The recovery information is a block with the following format:
      #### load (4 bytes)
      #### number of changed OIDs (4 bytes)
      #### changed oid offset from base (4 bytes)
      #### location of new value block (8 bytes)
      ####
      #### length of new value block (8 bytes)
      ####
      #### file offset for the beginning of the recovery block
      ####

      When the recovery block is active, the file id (first four
      bytes) is changed to FD_OIDPOOL_TO_RECOVER.
  */
  recovery_pos=endpos;
  qsort(saveinfo,n,sizeof(struct OIDPOOL_SAVEINFO),compare_oidoffs);
  fd_dtswrite_4bytes(stream,op->pool_load);
  fd_dtswrite_4bytes(stream,n);
  i=0; while (i<n) {
    fd_dtswrite_4bytes(stream,saveinfo[i].oidoff);
    fd_dtswrite_8bytes(stream,saveinfo[i].chunk.off);
    fd_dtswrite_4bytes(stream,saveinfo[i].chunk.size);
    endpos=endpos+16; i++;}
  fd_dtswrite_8bytes(stream,recovery_pos);
  fd_setpos(stream,0);
  fd_dtswrite_4bytes(stream,FD_OIDPOOL_TO_RECOVER);
  fd_dtsflush(stream);
  fsync(stream->fd_fileno);
  oidpool_finalize(op,stream,n,saveinfo,op->pool_load);
  u8_free(saveinfo);
  fd_setpos(stream,0);
  fd_dtswrite_4bytes(stream,FD_OIDPOOL_MAGIC_NUMBER);
  fd_dtsflush(stream);
  fsync(stream->fd_fileno);
  u8_log(fddb_loglevel,"OIDPoolStore",
         "Stored %d oid values in oidpool %s in %f seconds",
         n,p->pool_cid,u8_elapsed_time()-started);
  UNLOCK_POOLSTREAM(op);
  fd_unlock_pool(op);
  return n;
}

static int oidpool_finalize(struct FD_OIDPOOL *op,fd_dtype_stream stream,
                            int n,struct OIDPOOL_SAVEINFO *saveinfo,
                            unsigned int load)
{
  u8_log(fddb_loglevel+1,"OIDPoolFinalize",
         "Finalizing %d oid values %s in %f seconds",
         n,op->pool_cid);

  if (op->pool_offsets) {
#if HAVE_MMAP
    unsigned int *offsets;
    if (op->pool_offsets) reload_offsets(op,0,1);
    offsets=op->pool_offsets;
    switch (op->pool_offtype) {
    case FD_B64: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff;
        offsets[oidoff*3]=fd_net_order((saveinfo[k].chunk.off)>>32);
        offsets[oidoff*3+1]=fd_net_order((saveinfo[k].chunk.off)&(0xFFFFFFFF));
        offsets[oidoff*3+2]=fd_net_order(saveinfo[k].chunk.size);
        k++;}
      break;}
    case FD_B32: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff;
        offsets[oidoff*2]=fd_net_order(saveinfo[k].chunk.off);
        offsets[oidoff*2+1]=fd_net_order(saveinfo[k].chunk.size);
        k++;}
      break;}
    case FD_B40: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff, w1=0, w2=0;
        convert_FD_B40_ref(saveinfo[k].chunk,&w1,&w2);
        offsets[oidoff*2]=fd_net_order(w1);
        offsets[oidoff*2+1]=fd_net_order(w2);
        k++;}
      break;}
    default:
      u8_log(LOG_WARN,"Bad offset type for %s",op->pool_cid);
      u8_free(saveinfo);
      exit(-1);}
    if (op->pool_offsets) reload_offsets(op,0,-1);
#else
    int i=0, refsize=get_chunk_ref_size(op), offsize=op->pool_offsets_size;
    unsigned int *offsets=
      u8_realloc(op->pool_offsets,refsize*(op->pool_load));
    if (offsets) {
      op->pool_offsets=offsets;
      op->pool_offsets_size=refsize*op->pool_load;}
    else {
      u8_log(LOG_WARN,"Realloc failed","When writing offsets");
      return -1;}
    switch (op->pool_offtype) {
    case FD_B64: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff;
        offsets[oidoff*3]=(saveinfo[k].chunk.off)>>32;
        offsets[oidoff*3+1]=((saveinfo[k].chunk.off)&(0xFFFFFFFF));
        offsets[oidoff*3+2]=(saveinfo[k].chunk.achoice_size);
        k++;}
      break;}
    case FD_B32: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff;
        offsets[oidoff*2]=(saveinfo[k].chunk.off);
        offsets[oidoff*2+1]=(saveinfo[k].chunk.achoice_size);
        k++;}
      break;}
    case FD_B40: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff, w1, w2;
        convert_FD_B40_ref(saveinfo[k].chunk,&w1,&w2);
        offsets[oidoff*2]=(w1);
        offsets[oidoff*2+1]=(w2);
        k++;}
      break;}
    default:
      u8_log(LOG_WARN,"Bad offset type for %s",op->pool_cid);
      u8_free(saveinfo);
      exit(-1);}
    fd_setpos(stream,256);
    fd_dtswrite_ints(stream,load*(refsize/4),offsets);
#endif
    } else switch (op->pool_offtype) {
    case FD_B32: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff;
        fd_setpos(stream,256+oidoff*8);
        fd_dtswrite_4bytes(stream,saveinfo[k].chunk.off);
        fd_dtswrite_4bytes(stream,saveinfo[k].chunk.size);
        k++;}
      break;}
    case FD_B40: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff, w1=0, w2=0;
        fd_setpos(stream,256+oidoff*8);
        convert_FD_B40_ref(saveinfo[k].chunk,&w1,&w2);
        fd_dtswrite_4bytes(stream,w1);
        fd_dtswrite_4bytes(stream,w2);
        k++;}
      break;}
    case FD_B64: {
      int k=0; while (k<n) {
        unsigned int oidoff=saveinfo[k].oidoff;
        fd_setpos(stream,256+oidoff*12);
        fd_dtswrite_8bytes(stream,saveinfo[k].chunk.off);
        fd_dtswrite_4bytes(stream,saveinfo[k].chunk.size);
        k++;}
      break;}
    default:
      u8_log(LOG_WARN,"Bad offset type for %s",op->pool_cid);
      u8_free(saveinfo);
      exit(-1);}
  fd_setpos(stream,FD_OIDPOOL_LOAD_POS);
  fd_dtswrite_4bytes(stream,op->pool_load);
  if (op->pool_mmap) {
    int retval=munmap(op->pool_mmap,op->pool_mmap_size);
    if (retval<0) {
      u8_log(LOG_WARN,"MUNMAP","oidpool MUNMAP failed with %s",u8_strerror(errno));
      errno=0;}
    op->pool_mmap_size=u8_file_size(op->pool_cid);
    op->pool_mmap=
      mmap(NULL,op->pool_mmap_size,PROT_READ,MMAP_FLAGS,op->pool_stream.fd_fileno,0);}
  return 0;
}


static int recover_oidpool(struct FD_OIDPOOL *fp)
{
  struct FD_DTYPE_STREAM *stream=&(fp->pool_stream);
  fd_off_t recovery_data_pos;
  unsigned int i=0, new_load, n_changes;
  struct OIDPOOL_SAVEINFO *saveinfo;
  fd_endpos(stream); fd_movepos(stream,-8);
  recovery_data_pos=fd_dtsread_8bytes(stream);
  fd_setpos(stream,recovery_data_pos);
  new_load=fd_dtsread_4bytes(stream);
  n_changes=fd_dtsread_4bytes(stream);
  saveinfo=u8_alloc_n(n_changes,struct OIDPOOL_SAVEINFO);
  while (i<n_changes) {
    saveinfo[i].oidoff=fd_dtsread_4bytes(stream);
    saveinfo[i].chunk.off=(fd_off_t)fd_dtsread_8bytes(stream);
    saveinfo[i].chunk.size=(fd_off_t)fd_dtsread_4bytes(stream);
    i++;}
  if (oidpool_finalize(fp,stream,n_changes,saveinfo,new_load)<0) {
    u8_free(saveinfo);
    return -1;}
  else {
    u8_free(saveinfo);
    return 0;}
}

static fdtype oidpool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE; int i=0;
  fd_oidpool op=(fd_oidpool)p;
  fd_lock_pool(op);
  if (!(FD_OIDPOOL_LOCKED(op))) lock_oidpool_file(op,0);
  if (op->pool_load+n>=op->pool_capacity) {
    fd_unlock_pool(op);
    return fd_err(fd_ExhaustedPool,"file_pool_alloc",p->pool_cid,FD_VOID);}
  while (i < n) {
    FD_OID new_addr=FD_OID_PLUS(op->pool_base,op->pool_load);
    fdtype new_oid=fd_make_oid(new_addr);
    FD_ADD_TO_CHOICE(results,new_oid);
    op->pool_load++; i++; op->pool_n_locked++;}
  fd_unlock_pool(op);
  return fd_simplify_choice(results);
}

static int oidpool_lock(fd_pool p,fdtype oids)
{
  struct FD_OIDPOOL *fp=(struct FD_OIDPOOL *)p;
  int retval=lock_oidpool_file(fp,1);
  if (retval)
    fp->pool_n_locked=fp->pool_n_locked+FD_CHOICE_SIZE(oids);
  return retval;
}

static int oidpool_unlock(fd_pool p,fdtype oids)
{
  struct FD_OIDPOOL *fp=(struct FD_OIDPOOL *)p;
  if (fp->pool_n_locked == 0) return 0;
  else if (!(FD_OIDPOOL_LOCKED(fp))) return 0;
  else if (FD_CHOICEP(oids))
    fp->pool_n_locked=fp->pool_n_locked-FD_CHOICE_SIZE(oids);
  else if (FD_EMPTY_CHOICEP(oids)) {}
  else fp->pool_n_locked--;
  if (fp->pool_n_locked == 0) {
    /* This unlocks the underlying file, not the stream itself */
    fd_dts_unlockfile(&(fp->pool_stream));
    fd_reset_hashtable(&(fp->pool_changes),0,1);}
  return 1;
}

static void oidpool_setcache(fd_pool p,int level)
{
  fd_oidpool op=(fd_oidpool)p;
  int chunk_ref_size=get_chunk_ref_size(op);
  if (chunk_ref_size<0) {
    u8_log(LOG_WARN,fd_CorruptedPool,"Pool structure invalid: %s",p->pool_cid);
    return;}
  if ( ( (level<2) && (op->pool_offsets == NULL) ) ||
       ( (level==2) && ( op->pool_offsets != NULL ) ) )
    return;
  fd_lock_pool(op);
  if ( ( (level<2) && (op->pool_offsets == NULL) ) ||
       ( (level==2) && ( op->pool_offsets != NULL ) ) ) {
    fd_unlock_pool(op);
    return;}
#if (!(HAVE_MMAP))
  if (level < 2) {
    if (op->pool_offsets) {
      u8_free(op->pool_offsets);
      op->pool_offsets=NULL;}
    fd_unlock_pool(op);
    return;}
  else {
    unsigned int *offsets;
    fd_dtype_stream s=&(op->pool_stream);
    if (LOCK_POOLSTREAM(op)<0) {
      fd_clear_errors(1);}
    else {
      size_t offsets_size=chunk_ref_size*(op->pool_load);
      fd_dts_start_read(s);
      fd_setpos(s,12);
      op->pool_load=load=fd_dtsread_4bytes(s);
      offsets=u8_malloc(offsets_size);
      fd_setpos(s,24);
      fd_dtsread_ints(s,load,offsets);
      op->pool_offsets=offsets;
      op->pool_offsets_size=offsets_size;
      UNLOCK_POOLSTREAM(op);}
    fd_unlock_pool(op);
    return;}
#else /* HAVE_MMAP */
  if ( (level < 2) && (op->pool_offsets) ) {
    /* Unmap the offsets cache */
    int retval;
    size_t offsets_size=op->pool_offsets_size;
    size_t header_size=256+offsets_size;
    /* The address to munmap is 64 (not 256) because op->pool_offsets is an
       (unsigned int *) */
    retval=munmap((op->pool_offsets)-64,header_size);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "oidpool_setcache:munmap %s",op->pool_cid);
      op->pool_offsets=NULL;
      U8_CLEAR_ERRNO();}
    op->pool_offsets=NULL;
    op->pool_offsets_size=0;}

  if ( (level < 3) && (op->pool_mmap) ) {
    /* Unmap the file */
    int retval=munmap(op->pool_mmap,op->pool_mmap_size);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "hash_index_setcache:munmap %s",op->pool_cid);
      U8_CLEAR_ERRNO();}
    op->pool_mmap=NULL;
    op->pool_mmap_size=-1;}

  if ( (LOCK_POOLSTREAM(op,"oidpool_setcache")) < 0) {
    u8_log(LOGWARN,"PoolStreamClosed",
           "During oidpool_setcache for %s",op->pool_cid);
    UNLOCK_POOLSTREAM(op);
    fd_unlock_pool(op);
    return;}

  if ( (level >= 2) && (op->pool_offsets == NULL) ) {
    unsigned int *offsets, *newmmap;
    /* Sizes here are in bytes */
    size_t offsets_size=(op->pool_load)*chunk_ref_size;
    size_t header_size=256+offsets_size;
    /* Map the offsets */
    newmmap=
      mmap(NULL,header_size,PROT_READ,MAP_SHARED|MAP_NORESERVE,
           op->pool_stream.fd_fileno,
           0);
    if ((newmmap==NULL) || (newmmap==((void *)-1))) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "oidpool_setcache:mmap %s",op->pool_cid);
      op->pool_offsets=NULL;
      op->pool_offsets_size=0;
      U8_CLEAR_ERRNO();}
    else {
      op->pool_offsets=offsets=newmmap+64;
      op->pool_offsets_size=offsets_size;} }

  if ( (level >= 3) && (op->pool_mmap == NULL) ) {
      unsigned char *mmapped=NULL;
      ssize_t mmap_size=u8_file_size(op->pool_cid);
      if (mmap_size<0) {
        u8_log(LOG_WARN,u8_strerror(errno),
               "oidpool u8_file_size for mmap %s",op->pool_source);
        errno=0;
        UNLOCK_POOLSTREAM(op);
        fd_unlock_pool(op);
        return;}
      mmapped=
        mmap(NULL,mmap_size,PROT_READ,MMAP_FLAGS,
             op->pool_stream.fd_fileno,0);
      if (mmapped) {
        op->pool_mmap=mmapped;
        op->pool_mmap_size=mmap_size;}
      else {
        u8_log(LOG_WARN,u8_strerror(errno),
               "oidpool_setcache:mmap %s",op->pool_source);
        op->pool_mmap=mmapped;
        op->pool_mmap_size=mmap_size;
        errno=0;}}

  UNLOCK_POOLSTREAM(op);
  fd_unlock_pool(op);
#endif /* HAVE_MMAP */
}

/* Write values:
  * 0: just for reading, open up to the *load* of the pool
  * 1: for writing, open up to the capcity of the pool
  * -1: for reading, but sync before remapping
*/
static void reload_offsets(fd_oidpool op,int lock,int write)
{
#if HAVE_MMAP
  /* With MMAP, this consists of unmapping the current buffer
     and mapping a new one with the new load. */
  int retval=0, chunk_ref_size=get_chunk_ref_size(op);
  if (lock) fd_lock_pool(op);

  if (write<0)
    retval=msync(op->pool_offsets-64,op->pool_offsets_size+256,MS_SYNC|MS_INVALIDATE);
  if (retval<0) {
    u8_log(LOG_WARN,u8_strerror(errno),"oidpool/reload_offsets:msync %s",op->pool_cid);
    retval=0;}
  /* Unmap the current buffer */
  retval=munmap((op->pool_offsets)-64,(op->pool_offsets_size)+256);
  if (retval<0) {
    u8_log(LOG_WARN,u8_strerror(errno),"oidpool/reload_offsets:munmap %s",op->pool_cid);
    op->pool_offsets=NULL; errno=0;}
  else {
    fd_dtype_stream s=&(op->pool_stream);
    size_t mmap_size=(write>0)?
      (chunk_ref_size*(op->pool_capacity)):
      (chunk_ref_size*(op->pool_load));
    unsigned int *newmmap;
    /* Map with the new load */
    newmmap=
      /* When allocating an offset buffer to read, we only have to make it as
         big as the file pools load. */
      mmap(NULL,(mmap_size)+256,
           ((write>0) ? (PROT_READ|PROT_WRITE) : (PROT_READ)),
           ((write>0) ? (MAP_SHARED) : (MAP_SHARED|MAP_NORESERVE)),
           s->fd_fileno,0);
    if ((newmmap==NULL) || (newmmap==((void *)-1))) {
      u8_log(LOG_WARN,u8_strerror(errno),"oidpool/reload_offsets:mmap %s",op->pool_cid);
      op->pool_offsets=NULL; op->pool_offsets_size=0; errno=0;}
    op->pool_offsets=newmmap+64;
    op->pool_offsets_size=mmap_size;}
  if (lock) fd_unlock_pool(op);
#else
  fd_dtype_stream s=&(op->pool_stream);
  /* Read new offsets table, compare it with the current, and
     only void those OIDs */
  unsigned int new_load, *offsets, *nscan, *oscan, *olim;
  struct FD_DTYPE_STREAM *s=&(op->pool_stream);
  if (lock) fd_lock_pool(op);
  if ( (LOCK_POOLSTREAM(op,"oidpool/reload_offsets")) < 0) {
    u8_log(LOGWARN,"PoolStreamClosed",
           "During oidpool_reload_offsets for %s",op->pool_cid);
    UNLOCK_POOLSTREAM(op)
    fd_unlock_pool(op);
    return;}
  oscan=op->pool_offsets; olim=oscan+(op->pool_offsets_size/4);
  fd_setpos(s,0x10); new_load=fd_dtsread_4bytes(s);
  nscan=offsets=u8_alloc_n(new_load,unsigned int);
  fd_setpos(s,0x100);
  fd_dtsread_ints(s,new_load,offsets);
  while (oscan < olim)
    if (*oscan == *nscan) {oscan++; nscan++;}
    else {
      FD_OID addr=FD_OID_PLUS(op->pool_base,(nscan-offsets));
      fdtype changed_oid=fd_make_oid(addr);
      fd_hashtable_op(&(op->pool_cache),fd_table_replace,changed_oid,FD_VOID);
      oscan++; nscan++;}
  u8_free(op->pool_offsets);
  op->pool_offsets=offsets;
  op->pool_load=new_load;
  op->pool_offsets_size=new_load*get_chunk_ref_size(op);
  update_modtime(op);
  UNLOCK_POOLSTREAM(op)
  if (lock) fd_unlock_pool(op);
#endif
}

static void oidpool_close(fd_pool p)
{
  fd_oidpool op=(fd_oidpool)p;
  fd_lock_pool(op);
  if (op->pool_offsets) {
#if HAVE_MMAP
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval=munmap((op->pool_offsets)-64,op->pool_offsets_size+256);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),
             "oidpool_close:munmap offsets %s",op->pool_cid);
      errno=0;}
#else
    u8_free(op->pool_offsets);
#endif
    op->pool_offsets=NULL; op->pool_offsets_size=0;
    op->pool_cache_level=-1;}
#if HAVE_MMAP
  if (op->pool_mmap) {
    int retval=munmap(op->pool_mmap,op->pool_mmap_size);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),"oidpool_close:munmap %s",op->pool_cid);
      errno=0;}}
#endif
  fd_dtsfree(&(op->pool_stream),1);
  fd_unlock_pool(op);
}

static void oidpool_setbuf(fd_pool p,int bufsiz)
{
  fd_oidpool op=(fd_oidpool)p;
  fd_lock_pool(op);
  fd_dtsbufsize(&(op->pool_stream),bufsiz);
  fd_unlock_pool(op);
}

static fdtype oidpool_metadata(fd_pool p,fdtype md)
{
  fd_oidpool op=(fd_oidpool)p;
  if (FD_VOIDP(md)) {
    fdtype metadata; fd_off_t metadata_pos;
    fd_dtype_stream stream=&(op->pool_stream);
    fd_lock_pool(op);
    fd_setpos(stream,FD_OIDPOOL_METADATA_POS);
    metadata_pos=fd_dtsread_off_t(stream);
    if (metadata_pos) {
      fd_setpos(stream,metadata_pos);
      metadata=fd_dtsread_dtype(stream);
      fd_unlock_pool(op);
      return metadata;}
    else {
      fd_unlock_pool(op);
      return FD_VOID;}}
  else if (op->pool_read_only)
    return fd_err(fd_ReadOnlyPool,"oidpool_metadata",op->pool_cid,FD_VOID);
  else {
    fd_dtype_stream stream=&(op->pool_stream);
    fd_off_t metadata_pos=fd_endpos(stream);
    fd_dtswrite_dtype(stream,md);
    fd_setpos(stream,FD_OIDPOOL_METADATA_POS);
    fd_dtswrite_8bytes(stream,metadata_pos);
    fd_dtsflush(stream); fsync(stream->fd_fileno);
    return fd_incref(md);}
}

/* Module (file) Initialization */

static struct FD_POOL_HANDLER oidpool_handler={
  "oidpool", 1, sizeof(struct FD_OIDPOOL), 12,
  oidpool_close, /* close */
  oidpool_setcache, /* setcache */
  oidpool_setbuf, /* setbuf */
  oidpool_alloc, /* alloc */
  oidpool_fetch, /* fetch */
  oidpool_fetchn, /* fetchn */
  oidpool_load, /* getload */
  oidpool_lock, /* lock */
  oidpool_unlock, /* release */
  oidpool_storen, /* storen */
  NULL, /* swapout */
  oidpool_metadata, /* metadata */
  NULL}; /* sync */


FD_EXPORT void fd_init_oidpools_c()
{
  u8_register_source_file(_FILEINFO);

  fd_register_pool_opener
    (FD_OIDPOOL_MAGIC_NUMBER,
     open_oidpool,fd_read_pool_metadata,fd_write_pool_metadata);
  fd_register_pool_opener
    (FD_OIDPOOL_TO_RECOVER,
     open_oidpool,fd_read_pool_metadata,fd_write_pool_metadata);
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
