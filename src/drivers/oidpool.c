/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"
#define KNO_INLINE_POOLS 1
#define KNO_INLINE_BUFIO 1

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/numbers.h"
#include "kno/storage.h"
#include "kno/streams.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

#include "headers/oidpool.h"

#include <libu8/libu8.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>

#include <zlib.h>

#if (KNO_USE_MMAP)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#define MMAP_FLAGS MAP_SHARED
#endif

/* Locking oid pool streams */

#define POOLFILE_LOCKEDP(op)                                            \
  (U8_BITP((op->pool_stream.stream_flags),KNO_STREAM_FILE_LOCKED))

KNO_FASTOP int LOCK_POOLSTREAM(kno_oidpool op,u8_string caller)
{
  kno_lock_stream(&(op->pool_stream));
  if (op->pool_stream.stream_fileno < 0) {
    u8_seterr("PoolStreamClosed",caller,u8_strdup(op->poolid));
    kno_unlock_stream(&(op->pool_stream));
    return -1;}
  else return 1;
}

#define UNLOCK_POOLSTREAM(op) kno_unlock_stream(&(op->pool_stream))

static void update_modtime(struct KNO_OIDPOOL *fp);
static void reload_offdata(struct KNO_OIDPOOL *fp,int lock);

static struct KNO_POOL_HANDLER oidpool_handler;

u8_condition kno_InvalidSchemaDef=_("Invalid schema definition data");
u8_condition kno_InvalidSchemaRef=_("Invalid encoded schema reference");
u8_condition kno_SchemaInconsistency=_("Inconsistent schema reference and value data");

static u8_condition InvalidOffset=_("Invalid offset in OIDPOOL");

#define KNO_OIDPOOL_LOAD_POS      0x10

#define KNO_OIDPOOL_LABEL_POS             0x18
#define KNO_OIDPOOL_METADATA_POS          0x24
#define KNO_OIDPOOL_SCHEMAS_POS           0x30

#define KNO_OIDPOOL_FETCHBUF_SIZE 8000

/* OIDPOOLs are the next generation of object pool data file.  While
   previous formats have all stored OIDs for years and years, OIDPOOLs
   are supposed to be the best to date and somewhat paradigmatic.
   The design focuses on performance and extensibility.  With KNO_HASHINDEX,
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
   0x20 XXXX     byte length of label dtype representation (4 bytes)
   0x24 XXXX     file offset of pool metadata (8 bytes)
   XXXX
   0x2c XXXX     byte length of pool metadata dtype representation (4 bytes)
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
   0x0020      Read only: set if this pool is intended to be read-only
   0x0040      Phased: set if this pool should implement phased commits

   The offsets block starts at 0x100 and goes for either capacity*8 or capacity*12
   bytes.  The offset values are stored as pairs of big-endian binary representations.
   For the 32B and 64B forms, these are just straightforward integers of the same size.
   For the 40B form, which is designed to better use memory and cache, the high 8
   bits of the second word are taken as the high eight bits of a forty-byte offset.

*/

/* Getting chunk refs */

typedef long long int ll;
typedef unsigned long long ull;

static ssize_t get_chunk_ref_size(kno_oidpool p)
{
  switch (p->pool_offtype) {
  case KNO_B32: case KNO_B40:
    return 8;
  case KNO_B64: return 12;}
  return -1;
}

static kno_size_t get_maxpos(kno_oidpool p)
{
  switch (p->pool_offtype) {
  case KNO_B32:
    return ((kno_size_t)(((kno_size_t)1)<<32));
  case KNO_B40:
    return ((kno_size_t)(((kno_size_t)1)<<40));
  case KNO_B64:
    return ((kno_size_t)(((kno_size_t)1)<<63));
  default:
    return -1;}
}

static size_t get_offdata_length(kno_oidpool p)
{
  size_t ref_size = get_chunk_ref_size(p);
  return ref_size*(p->pool_capacity);
}

/* Schema functions */

/* Making and opening oidpools */

static int init_schemas(kno_oidpool,lispval);

static kno_pool open_oidpool(u8_string fname,
                            kno_storage_flags open_flags,
                            lispval opts)
{
  KNO_OID base = KNO_NULL_OID_INIT;
  unsigned int hi, lo, magicno, capacity, load, oidpool_format;
  kno_off_t label_loc, schemas_loc; lispval label;
  struct KNO_OIDPOOL *pool = u8_alloc(struct KNO_OIDPOOL);
  int read_only = U8_BITP(open_flags,KNO_STORAGE_READ_ONLY) ||
    (!(u8_file_writablep(fname)));
  kno_stream_mode mode=
    ((read_only) ? (KNO_FILE_READ) : (KNO_FILE_MODIFY));
  long long cache_level = kno_getfixopt(opts,"CACHELEVEL",kno_default_cache_level);
  int stream_flags = KNO_STREAM_CAN_SEEK | KNO_STREAM_NEEDS_LOCK |
    ( (read_only) ? (KNO_STREAM_READ_ONLY) : (0) ) |
    ( (cache_level>=3) ? (KNO_STREAM_USEMMAP) : (0) );
  u8_string realpath = u8_realpath(fname,NULL);
  u8_string abspath = u8_abspath(fname,NULL);
  struct KNO_STREAM *stream=
    kno_init_file_stream(&(pool->pool_stream),fname,
                        mode,stream_flags,kno_driver_bufsize);
  struct KNO_INBUF *instream = (stream) ? (kno_readbuf(stream)) : (NULL);

  if (instream == NULL) {
    u8_seterr("FileNotFound","open_oidpool",u8_strdup(fname));
    return NULL;}

  /* See if it ended up read only */
  if ((stream->stream_flags)&(KNO_STREAM_READ_ONLY)) read_only = 1;
  stream->stream_flags &= ~KNO_STREAM_IS_CONSED;
  magicno = kno_read_4bytes(instream);

  if (magicno != KNO_OIDPOOL_MAGIC_NUMBER) {
    kno_close_stream(stream,0);
    u8_seterr("Not an OIDPool","open_oidpool",u8_strdup(fname));
    return NULL;}

  /* Read POOL base etc. */
  hi = kno_read_4bytes(instream); lo = kno_read_4bytes(instream);
  KNO_SET_OID_HI(base,hi); KNO_SET_OID_LO(base,lo);
  pool->pool_capacity = capacity = kno_read_4bytes(instream);
  pool->pool_load = load = kno_read_4bytes(instream);
  oidpool_format = kno_read_4bytes(instream);
  pool->oidpool_format = oidpool_format;

  if (load > capacity) {
    u8_logf(LOG_CRIT,kno_PoolOverflow,
            "The oidpool %s specifies a load (%lld) > its capacity (%lld)",
            fname,load,capacity);
    pool->pool_load=load=capacity;}

  if ((U8_BITP(oidpool_format,KNO_OIDPOOL_READ_ONLY))&&
      (!(kno_testopt(opts,FDSYM_READONLY,KNO_FALSE)))) {
    /* If the pool is intrinsically read-only, make it so. */
    kno_unlock_stream(stream);
    kno_close_stream(stream,0);
    kno_init_file_stream(stream,fname,KNO_FILE_READ,-1,kno_driver_bufsize);
    kno_lock_stream(stream);
    kno_setpos(stream,KNO_OIDPOOL_LABEL_POS);
    open_flags |= KNO_STORAGE_READ_ONLY;}

  if (U8_BITP(oidpool_format,KNO_OIDPOOL_ADJUNCT))
    open_flags |= KNO_POOL_ADJUNCT;
  if (U8_BITP(oidpool_format,KNO_OIDPOOL_SPARSE))
    open_flags |= KNO_POOL_SPARSE;

  pool->pool_offtype =
    (kno_offset_type)((oidpool_format)&(KNO_OIDPOOL_OFFMODE));
  pool->oidpool_compression=
    (kno_compress_type)(((oidpool_format)&(KNO_OIDPOOL_COMPRESSION))>>3);

  kno_init_pool((kno_pool)pool,base,capacity,&oidpool_handler,
               fname,abspath,realpath,
               KNO_STORAGE_ISPOOL,KNO_VOID,opts);
  pool->pool_flags=open_flags;
  u8_free(realpath);
  u8_free(abspath);

  /* Get the label */
  label_loc = kno_read_8bytes(instream);
  /* label_size = */ kno_read_4bytes(instream);
  /* Skip the metadata field */
  kno_read_8bytes(instream);
  kno_read_4bytes(instream); /* Ignore size */
  /* Read and initialize the schemas_loc */
  schemas_loc = kno_read_8bytes(instream);
  kno_read_4bytes(instream); /* Ignore size */
  if (label_loc) {
    if (kno_setpos(stream,label_loc)>0) {
      label = kno_read_dtype(instream);
      if (STRINGP(label)) pool->pool_label = u8_strdup(CSTRING(label));
      else u8_logf(LOG_WARN,kno_BadFilePoolLabel,kno_lisp2string(label));
      kno_decref(label);}
    else {
      kno_seterr(kno_BadFilePoolLabel,"open_oidpool","bad label loc",
                KNO_INT(label_loc));
      kno_close_stream(stream,0);
      u8_free(pool);
      return NULL;}}
  if (schemas_loc) {
    lispval schemas;
    kno_setpos(stream,schemas_loc);
    schemas = kno_read_dtype(instream);
    init_schemas(pool,schemas);
    kno_decref(schemas);}
  else init_schemas(pool,VOID);
  /* Offsets size is the malloc'd size (in unsigned ints) of the offsets.
     We don't fill this in until we actually need it. */
  pool->pool_offdata = NULL;
  if (read_only)
    U8_SETBITS(pool->pool_flags,KNO_STORAGE_READ_ONLY);
  else U8_CLEARBITS(pool->pool_flags,KNO_STORAGE_READ_ONLY);
  kno_register_pool((kno_pool)pool);
  update_modtime(pool);
  return (kno_pool)pool;
}

static void update_modtime(struct KNO_OIDPOOL *fp)
{
  struct stat fileinfo;
  if ((fstat(fp->pool_stream.stream_fileno,&fileinfo))<0)
    fp->pool_mtime = (time_t)-1;
  else fp->pool_mtime = fileinfo.st_mtime;
}

/* Maintaing the schema table */

static int init_schema_entry(struct KNO_SCHEMA_ENTRY *e,int pos,lispval vec);
static int compare_schema_vals(const void *p1,const void *p2);
static void sort_schema(lispval *v,int n);

static int init_schemas(kno_oidpool op,lispval schema_vec)
{
  if (!(VECTORP(schema_vec))) {
    op->oidpool_n_schemas = 0;
    op->oidpool_schemas = NULL;
    op->oidpool_schbyval = NULL;
    op->oidpool_max_slotids = 0;
    return 0;}
  else {
    int i = 0, n = VEC_LEN(schema_vec), max_slotids = 0;
    struct KNO_SCHEMA_ENTRY *schemas = u8_alloc_n(n,KNO_SCHEMA_ENTRY);
    struct KNO_SCHEMA_LOOKUP *schbyval = u8_alloc_n(n,KNO_SCHEMA_LOOKUP);
    while (i<n) {
      lispval slotids = VEC_REF(schema_vec,i);
      int n_slotids;
      if (VECTORP(slotids)) n_slotids = VEC_LEN(slotids);
      else {
        u8_free(schemas);
        u8_free(schbyval);
        return kno_reterr
          (kno_InvalidSchemaDef,"oidpool/init_schemas",NULL,schema_vec);}
      if (n_slotids>max_slotids) max_slotids = n_slotids;
      init_schema_entry(&(schemas[i]),i,slotids);
      schbyval[i].op_schema_id = i; schbyval[i].op_nslots = n_slotids;
      schbyval[i].op_slotids = schemas[i].op_slotids;
      i++;}
    op->oidpool_schemas = schemas; op->oidpool_schbyval = schbyval;
    op->oidpool_max_slotids = max_slotids; op->oidpool_n_schemas = n;
    qsort(schbyval,n,sizeof(struct KNO_SCHEMA_LOOKUP),
          compare_schema_vals);
    return n;}
}

static int init_schema_entry(struct KNO_SCHEMA_ENTRY *e,int pos,lispval vec)
{
  int i = 0, len = VEC_LEN(vec);
  lispval *slotids = u8_alloc_n((len+1),lispval);
  unsigned int *mapin = u8_alloc_n(len,unsigned int);
  unsigned int *mapout = u8_alloc_n(len,unsigned int);
  e->op_schema_id = pos;
  e->op_nslots = len;
  e->normal = kno_incref(vec);
  e->op_slotids = slotids;
  e->op_slotmapin = mapin;
  e->op_slotmapout = mapout;
  while (i<len) {
    lispval val = VEC_REF(vec,i);
    slotids[i]=kno_incref(val);
    i++;}
  sort_schema(slotids,len);
  /* This will make it fast to get the pos from the schema pointer */
  slotids[len]=KNO_INT(pos);
  i = 0; while (i<len) {
    lispval val = VEC_REF(vec,i);
    int j = 0; while (j<len)
                 if (slotids[j]==val) break;
                 else j++;
    /* assert(i<len); assert(j<len); */
    mapin[i]=j; mapout[j]=i;
    i++;}
  return 0;
}

static int compare_schema_vals(const void *p1,const void *p2)
{
  struct KNO_SCHEMA_LOOKUP *se1 = (struct KNO_SCHEMA_LOOKUP *)p1;
  struct KNO_SCHEMA_LOOKUP *se2 = (struct KNO_SCHEMA_LOOKUP *)p2;
  if (se1->op_nslots<se2->op_nslots) return -1;
  else if (se1->op_nslots>se2->op_nslots) return 1;
  else {
    lispval *slotids1 = se1->op_slotids, *slotids2 = se2->op_slotids;
    int i = 0, n = se1->op_nslots; while (i<n) {
      if (slotids1[i]<slotids2[i]) return -1;
      else if (slotids1[i]<slotids2[i]) return 1;
      else i++;}
    return 0;}
}

static int compare_schemas(struct KNO_SCHEMA_LOOKUP *e,lispval *slotids,int n)
{
  if (n<e->op_nslots) return -1;
  else if (n>e->op_nslots) return 1;
  else {
    lispval *oslotids = e->op_slotids;
    int i = 0; while (i<n)
                 if (slotids[i]<oslotids[i]) return -1;
                 else if (slotids[i]>oslotids[i]) return 1;
                 else i++;
    return 0;}
}

static int find_schema_byval(kno_oidpool op,lispval *slotids,int n)
{
  int size = op->oidpool_n_schemas, cmp;
  struct KNO_SCHEMA_LOOKUP *table = op->oidpool_schbyval;
  struct KNO_SCHEMA_LOOKUP *max = table+size;
  struct KNO_SCHEMA_LOOKUP *top = max;
  struct KNO_SCHEMA_LOOKUP *bot = table;
  struct KNO_SCHEMA_LOOKUP *middle = bot+(size)/2;
  while (top>bot) {
    cmp = compare_schemas(middle,slotids,n);
    if (cmp==0) return middle->op_schema_id;
    else if (cmp<0) {
      top = middle-1; middle = bot+(top-bot)/2;}
    else {
      bot = middle+1; middle = bot+(top-bot)/2;}}
  if ((middle<max) && (middle>=table) &&
      (compare_schemas(middle,slotids,n)==0))
    return middle->op_schema_id;
  else return -1;
}

KNO_FASTOP void lispv_swap(lispval *a,lispval *b)
{
  lispval t;
  t = *a;
  *a = *b;
  *b = t;
}

static void sort_schema(lispval *v,int n)
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
      kno_sort_schema(ln, v); v += j; n = rn;}
    else {kno_sort_schema(rn,v + j); n = ln;}}
}

U8_MAYBE_UNUSED static int schema_sortedp(lispval *v,int n)
{
  int i = 0; while (i<n-1)
               if (v[i]<v[i+1]) i++;
               else return 0;
  return 1;
}

/* These assume that the pool itself is locked */
static int write_oidpool_load(kno_oidpool op)
{
  if (KNO_POOLSTREAM_LOCKEDP(op)) {
    /* Update the load */
    long long load;
    kno_stream stream = &(op->pool_stream);
    load = kno_read_4bytes_at(stream,16,KNO_ISLOCKED);
    if (load<0) {
      return -1;}
    else if (op->pool_load>load) {
      int rv = kno_write_4bytes_at(stream,op->pool_load,16);
      if (rv<0) return rv;
      else return rv;}
    else {
      return 0;}}
  else return 0;
}

static int read_oidpool_load(kno_oidpool op)
{
  long long load;
  kno_stream stream = &(op->pool_stream);
  if (KNO_POOLSTREAM_LOCKEDP(op)) {
    return op->pool_load;}
  else if (op->pool_load == op->pool_capacity)
    return op->pool_load;
  else if (kno_streamctl(stream,kno_stream_lockfile,NULL)<0)
    return -1;
  load = kno_read_4bytes_at(stream,16,KNO_ISLOCKED);
  if (load<0) {
    kno_streamctl(stream,kno_stream_unlockfile,NULL);
    return -1;}
  kno_streamctl(stream,kno_stream_unlockfile,NULL);
  op->pool_load = load;
  return load;
}

/* Lock the underlying OIDpool */

static int lock_oidpool_file(struct KNO_OIDPOOL *op,int use_mutex)
{
  if (POOLFILE_LOCKEDP(op)) return 1;
  else if ((op->pool_flags)&(KNO_STORAGE_READ_ONLY))
    return 0;
  else {
    struct KNO_STREAM *s = &(op->pool_stream);
    struct stat fileinfo;
    if (use_mutex) kno_lock_pool_struct((kno_pool)op,1);
    if (POOLFILE_LOCKEDP(op)) {
      /* Another thread got here first */
      if (use_mutex) kno_unlock_pool_struct((kno_pool)op);
      return 1;}
    LOCK_POOLSTREAM(op,"lock_oidpool_file");
    if (kno_streamctl(s,kno_stream_lockfile,NULL)==0) {
      if (use_mutex) kno_unlock_pool_struct((kno_pool)op);
      UNLOCK_POOLSTREAM(op);
      return 0;}
    fstat( s->stream_fileno, &fileinfo);
    if ( fileinfo.st_mtime > op->pool_mtime ) {
      /* Make sure we're up to date. */
      read_oidpool_load(op);
      if (op->pool_offdata) reload_offdata(op,0);
      else {
        kno_reset_hashtable(&(op->pool_cache),-1,1);
        kno_reset_hashtable(&(op->pool_changes),32,1);}}
    if (use_mutex) kno_unlock_pool_struct((kno_pool)op);
    UNLOCK_POOLSTREAM(op);
    return 1;}
}

/* Methods */

static int oidpool_load(kno_pool p)
{
  kno_oidpool op = (kno_oidpool)p;
  if (KNO_OIDPOOL_LOCKED(op))
    /* If we have the file locked, the stored load is good. */
    return op->pool_load;
  else {
    /* Otherwise, we need to read the load from the file */
    int load;
    kno_lock_pool_struct((kno_pool)op,1);
    kno_lock_stream(&(op->pool_stream));
    load = read_oidpool_load(op);
    kno_unlock_stream(&(op->pool_stream));
    kno_unlock_pool_struct((kno_pool)op);
    return load;}
}

static lispval read_oid_value(kno_oidpool op,
                              kno_inbuf in,
                              const u8_string cxt)
{
  int zip_code;
  zip_code = kno_read_zint(in);
  if (PRED_FALSE(zip_code>(op->oidpool_n_schemas)))
    return kno_err(kno_InvalidSchemaRef,"oidpool_fetch",op->poolid,VOID);
  else if (zip_code==0)
    return kno_read_dtype(in);
  else {
    struct KNO_SCHEMA_ENTRY *se = &(op->oidpool_schemas[zip_code-1]);
    int n_vals = kno_read_zint(in), n_slotids = se->op_nslots;
    if (PRED_TRUE(n_vals == n_slotids)) {
      lispval *values = u8_alloc_n(n_vals,lispval);
      unsigned int i = 0, *mapin = se->op_slotmapin;
      /* We reorder the values coming in to agree with the
         schema sorting done in memory for fast lookup. That
         translation is stored in the mapin field. */
      while (i<n_vals) {
        values[mapin[i]]=kno_read_dtype(in); i++;}
      return kno_make_schemap(NULL,n_vals,KNO_SCHEMAP_SORTED|KNO_SCHEMAP_TAGGED,
                             se->op_slotids,values);}
    else return kno_err(kno_SchemaInconsistency,cxt,op->poolid,VOID);}
}

static lispval read_oid_value_at(kno_oidpool op,
                                 KNO_CHUNK_REF ref,
                                 const u8_string cxt)
{
  if (ref.off<=0) return VOID;
  else {
    int free_buf=0;
    unsigned char _buf[KNO_OIDPOOL_FETCHBUF_SIZE], *buf;
    if (ref.size>KNO_OIDPOOL_FETCHBUF_SIZE) {
      buf = u8_malloc(ref.size);
      free_buf = 1;}
    else buf = _buf;
    if (buf == NULL)
      return KNO_ERROR;
    else if ((kno_read_block(&(op->pool_stream),buf,ref.size,ref.off,1))<0) {
      if (free_buf) u8_free(buf);
      return KNO_ERROR;}
    else if (op->oidpool_compression == KNO_NOCOMPRESS)
      if (free_buf) {
        KNO_INBUF in = { 0 };
        KNO_INIT_BYTE_INPUT(&in,buf,ref.size);
        lispval result = read_oid_value(op,&in,cxt);
        u8_free(buf);
        return result;}
      else {
        KNO_INBUF in = { 0 };
        KNO_INIT_BYTE_INPUT(&in,buf,ref.size);
        return read_oid_value(op,&in,cxt);}
    else {
      unsigned char _ubuf[KNO_OIDPOOL_FETCHBUF_SIZE*3], *ubuf=_ubuf;
      size_t ubuf_size = KNO_OIDPOOL_FETCHBUF_SIZE*3;
      switch (op->oidpool_compression) {
      case KNO_ZLIB:
        if (ref.size>KNO_OIDPOOL_FETCHBUF_SIZE)
          ubuf = do_zuncompress(buf,ref.size,&ubuf_size,NULL);
        else ubuf = do_zuncompress(buf,ref.size,&ubuf_size,_ubuf);
        break;
      default:
        if (free_buf) u8_free(buf);
        if (ubuf!=_ubuf) u8_free(ubuf);
        return kno_err(_("Bad compress level"),"oidpool_fetch",op->poolid,
                      VOID);}
      if (ubuf == NULL) {
        if (free_buf) u8_free(buf);
        if (ubuf!=_ubuf) u8_free(ubuf);
        return KNO_ERROR;}
      else if ((free_buf) || (ubuf!=_ubuf)) {
        KNO_INBUF in = { 0 }; lispval result;
        KNO_INIT_BYTE_INPUT(&in,ubuf,ubuf_size);
        result = read_oid_value(op,&in,cxt);
        if (free_buf) u8_free(buf);
        if (ubuf!=_ubuf) u8_free(ubuf);
        return result;}
      else {
        KNO_INBUF in = { 0 };
        KNO_INIT_BYTE_INPUT(&in,ubuf,ubuf_size);
        return read_oid_value(op,&in,cxt);}}}
}

static lispval oidpool_fetch(kno_pool p,lispval oid)
{
  kno_oidpool op = (kno_oidpool)p;
  KNO_OID addr = KNO_OID_ADDR(oid);
  int offset = KNO_OID_DIFFERENCE(addr,op->pool_base);
  if (PRED_FALSE( offset >= op->pool_load )) {
    /* Double check by fetching the load */
    if ( offset >= (oidpool_load(p)) ) {
      if ( (p->pool_flags) & (KNO_POOL_ADJUNCT) ) {
        return KNO_EMPTY_CHOICE;}
      else return KNO_UNALLOCATED_OID;}}

  unsigned int *offdata = op->pool_offdata;
  unsigned int offdata_len = op->pool_capacity;
  if (offdata) {
    KNO_CHUNK_REF ref=
      kno_get_chunk_ref(offdata,op->pool_offtype,offset,offdata_len);
    if (ref.off<0) return KNO_ERROR;
    else if (ref.off==0)
      return EMPTY;
    else {
      lispval value;
      kno_lock_stream(&(op->pool_stream));
      value = read_oid_value_at(op,ref,"oidpool_fetch");
      kno_unlock_stream(&(op->pool_stream));
      return value;}}
  else {
    kno_lock_stream(&(op->pool_stream)); {
      kno_stream stream = &(op->pool_stream);
      KNO_CHUNK_REF ref =
        kno_fetch_chunk_ref(stream,256,op->pool_offtype,offset,1);
      if (ref.off<0) {
        kno_unlock_stream(&(op->pool_stream));
        return KNO_ERROR;}
      else if (ref.off==0) {
        kno_unlock_stream(&(op->pool_stream));
        return EMPTY;}
      else {
        lispval value;
        value = read_oid_value_at(op,ref,"oidpool_fetch");
        kno_unlock_stream(&(op->pool_stream));
        return value;}}}
}

static int compare_offsets(const void *x1,const void *x2)
{
  const struct OIDPOOL_FETCH_SCHEDULE *s1 = x1, *s2 = x2;
  if (s1->location.off<s2->location.off) return -1;
  else if (s1->location.off>s2->location.off) return 1;
  else return 0;
}

static lispval *oidpool_fetchn(kno_pool p,int n,lispval *oids)
{
  kno_oidpool op = (kno_oidpool)p; KNO_OID base = p->pool_base;
  lispval *values = u8_big_alloc_n(n,lispval);
  if (op->pool_offdata == NULL) {
    /* Don't bother being clever if you don't even have an offsets
       table.  This could be fixed later for small memory implementations. */
    int i = 0; while (i<n) {
      values[i]=oidpool_fetch(p,oids[i]); i++;}
    return values;}
  else {
    unsigned int *offdata = op->pool_offdata;
    unsigned int offdata_len = op->pool_capacity;
    struct OIDPOOL_FETCH_SCHEDULE *schedule=
      u8_big_alloc_n(n,struct OIDPOOL_FETCH_SCHEDULE);
    kno_lock_stream(&(op->pool_stream));
    int i = 0;
    while (i<n) {
      lispval oid = oids[i]; KNO_OID addr = KNO_OID_ADDR(oid);
      unsigned int off = KNO_OID_DIFFERENCE(addr,base);
      schedule[i].value_at = i;
      schedule[i].location =
        kno_get_chunk_ref(offdata,op->pool_offtype,off,offdata_len);
      if (schedule[i].location.off<0) {
        kno_seterr(InvalidOffset,"oidpool_fetchn",p->poolid,oid);
        u8_big_free(schedule);
        u8_big_free(values);
        kno_unlock_stream(&(op->pool_stream));
        return NULL;}
      else i++;}
    /* Sort to try and take advantage of locality */
    qsort(schedule,n,sizeof(struct OIDPOOL_FETCH_SCHEDULE),
          compare_offsets);
    i = 0; while (i<n) {
      lispval value = read_oid_value_at(op,schedule[i].location,"oidpool_fetchn");
      if (KNO_ABORTP(value)) {
        int j = 0; while (j<i) { kno_decref(values[j]); j++;}
        u8_big_free(schedule);
        u8_big_free(values);
        u8_condition c = (u8_current_exception) ?       \
          (u8_current_exception->u8x_cond) :
          NULL;
        // Add more debugging context
        kno_seterr(c,"oidpool_fetchn/read",op->poolid,
                  oids[schedule[i].value_at]);
        kno_unlock_stream(&(op->pool_stream));
        return NULL;}
      else values[schedule[i].value_at]=value;
      i++;}
    kno_unlock_stream(&(op->pool_stream));
    u8_big_free(schedule);
    return values;}
}

static int get_schema_id(kno_oidpool op,lispval value)
{
  if ( (SCHEMAPP(value)) && (KNO_SCHEMAP_SORTEDP(value)) ) {
    struct KNO_SCHEMAP *sm = (kno_schemap)value;
    lispval *slotids = sm->table_schema, size = sm->schema_length;
    if (sm->schemap_tagged) {
      lispval pos = slotids[size];
      int intpos = kno_getint(pos);
      if ((intpos<op->oidpool_n_schemas) &&
          (op->oidpool_schemas[intpos].op_slotids == slotids))
        return intpos;}
    return find_schema_byval(op,slotids,size);}
  else if (SLOTMAPP(value)) {
    lispval _tmp_slotids[32], *tmp_slotids;
    struct KNO_SLOTMAP *sm = (kno_slotmap)value;
    int i = 0, size = KNO_XSLOTMAP_NUSED(sm);
    if (size<32)
      tmp_slotids=_tmp_slotids;
    else tmp_slotids = u8_alloc_n(size,lispval);
    while (i<size) {
      tmp_slotids[i]=sm->sm_keyvals[i].kv_key; i++;}
    /* assert(schema_sortedp(tmp_slotids,size)); */
    if (tmp_slotids==_tmp_slotids)
      return find_schema_byval(op,tmp_slotids,size);
    else {
      int retval = find_schema_byval(op,tmp_slotids,size);
      u8_free(tmp_slotids);
      return retval;}}
  else return -1;
}

static int oidpool_write_value(lispval value,kno_stream stream,
                               kno_oidpool p,struct KNO_OUTBUF *tmpout,
                               unsigned char **zbuf,int *zbuf_size)
{
  kno_outbuf outstream = kno_writebuf(stream);
  if ((p->oidpool_compression == KNO_NOCOMPRESS) &&
      (p->oidpool_n_schemas==0)) {
    kno_write_byte(outstream,0);
    return 1+kno_write_dtype(outstream,value);}
  tmpout->bufwrite = tmpout->buffer;
  if (p->oidpool_n_schemas==0) {
    kno_write_byte(tmpout,0);
    kno_write_dtype(tmpout,value);}
  else if ((SCHEMAPP(value)) || (SLOTMAPP(value))) {
    int schema_id = get_schema_id(p,value);
    if (schema_id<0) {
      kno_write_byte(tmpout,0);
      kno_write_dtype(tmpout,value);}
    else {
      struct KNO_SCHEMA_ENTRY *se = &(p->oidpool_schemas[schema_id]);
      kno_write_zint(tmpout,schema_id+1);
      if (SCHEMAPP(value)) {
        struct KNO_SCHEMAP *sm = (kno_schemap)value;
        lispval *values = sm->schema_values;
        int i = 0, size = sm->schema_length;
        kno_write_zint(tmpout,size);
        while (i<size) {
          kno_write_dtype(tmpout,values[se->op_slotmapout[i]]);
          i++;}}
      else {
        struct KNO_SLOTMAP *sm = (kno_slotmap)value;
        struct KNO_KEYVAL *data = sm->sm_keyvals;
        int i = 0, size = KNO_XSLOTMAP_NUSED(sm);
        kno_write_zint(tmpout,size);
        while (i<size) {
          kno_write_dtype(tmpout,data[se->op_slotmapin[i]].kv_val);
          i++;}}}}
  else {
    kno_write_byte(tmpout,0);
    kno_write_dtype(tmpout,value);}
  if (p->oidpool_compression == KNO_NOCOMPRESS) {
    kno_write_bytes(outstream,tmpout->buffer,tmpout->bufwrite-tmpout->buffer);
    return tmpout->bufwrite-tmpout->buffer;}
  else if (p->oidpool_compression == KNO_ZLIB) {
    unsigned char _cbuf[KNO_OIDPOOL_FETCHBUF_SIZE], *cbuf;
    size_t cbuf_size = KNO_OIDPOOL_FETCHBUF_SIZE;
    cbuf = do_zcompress(tmpout->buffer,tmpout->bufwrite-tmpout->buffer,
                        &cbuf_size,_cbuf,9);
    kno_write_bytes(outstream,cbuf,cbuf_size);
    if (cbuf!=_cbuf) u8_free(cbuf);
    return cbuf_size;}
  else {
    u8_logf(LOG_WARN,_("Out of luck"),
            "Compressed oidpools of this type are not yet yet supported");
    exit(-1);}
}

static ssize_t write_offdata
(struct KNO_OIDPOOL *bp, kno_stream stream,
 int n, struct OIDPOOL_SAVEINFO *saveinfo);

static int oidpool_storen(kno_pool p,int n,lispval *oids,lispval *values)
{
  KNO_OID base = p->pool_base;
  int isadjunct = (p->pool_flags) & (KNO_POOL_ADJUNCT);
  kno_oidpool op = (kno_oidpool)p;
  struct KNO_STREAM *stream = &(op->pool_stream);
  struct KNO_OUTBUF *outstream = kno_writebuf(stream);
  if ((LOCK_POOLSTREAM(op,"oidpool_storen"))<0) return -1;
  double started = u8_elapsed_time();
  u8_logf(LOG_INFO,"OidpoolStore",
          "Storing %d oid values in oidpool %s",n,p->poolid);
  struct OIDPOOL_SAVEINFO *saveinfo=
    u8_big_alloc_n(n,struct OIDPOOL_SAVEINFO);
  struct KNO_OUTBUF tmpout = { 0 };
  unsigned char *zbuf = u8_malloc(KNO_INIT_ZBUF_SIZE);
  unsigned int i = 0, zbuf_size = KNO_INIT_ZBUF_SIZE;
  unsigned int init_buflen = 2048*n;
  size_t maxpos = get_maxpos(op);
  unsigned int new_load = op->pool_load;
  kno_off_t endpos;
  if (init_buflen>262144) init_buflen = 262144;
  KNO_INIT_BYTE_OUTPUT(&tmpout,init_buflen);
  endpos = kno_endpos(stream);
  if ((op->oidpool_format)&(KNO_OIDPOOL_DTYPEV2))
    tmpout.buf_flags = tmpout.buf_flags|KNO_USE_DTYPEV2|KNO_IS_WRITING;
  while (i<n) {
    KNO_OID addr = KNO_OID_ADDR(oids[i]);
    unsigned int offset = KNO_OID_DIFFERENCE(addr,base);
    lispval value = values[i];
    int n_bytes = oidpool_write_value(value,stream,op,&tmpout,&zbuf,&zbuf_size);
    if (n_bytes<0) {
      u8_free(zbuf);
      u8_big_free(saveinfo);
      kno_close_outbuf(&tmpout);
      UNLOCK_POOLSTREAM(op);
      return n_bytes;}
    if ((endpos+n_bytes)>=maxpos) {
      u8_free(zbuf);
      u8_big_free(saveinfo);
      kno_close_outbuf(&tmpout);
      u8_seterr(kno_DataFileOverflow,"oidpool_storen",
                u8_strdup(p->poolid));
      UNLOCK_POOLSTREAM(op);
      return -1;}

    if ( (isadjunct) && (offset >= new_load) ) new_load=offset+1;

    saveinfo[i].chunk.off = endpos;
    saveinfo[i].chunk.size = n_bytes;
    saveinfo[i].oidoff = KNO_OID_DIFFERENCE(addr,base);

    endpos = endpos+n_bytes;
    i++;}
  kno_close_outbuf(&tmpout);
  u8_free(zbuf);

  kno_lock_pool_struct(p,1);
  write_offdata(op,stream,n,saveinfo);
  op->pool_load = new_load;
  write_oidpool_load(op);

  u8_big_free(saveinfo);
  kno_start_write(stream,0);
  kno_write_4bytes(outstream,KNO_OIDPOOL_MAGIC_NUMBER);
  kno_flush_stream(stream);
  fsync(stream->stream_fileno);
  u8_logf(LOG_NOTICE,"OidpoolStore",
          "Stored %d oid values in oidpool %s in %f seconds",
          n,p->poolid,u8_elapsed_time()-started);
  UNLOCK_POOLSTREAM(op);
  kno_unlock_pool_struct(p);
  return n;
}

static int oidpool_commit(kno_pool p,kno_commit_phase phase,
                          struct KNO_POOL_COMMITS *commits)
{
  struct KNO_OIDPOOL *op = (kno_oidpool) p;
  int chunk_ref_size = get_chunk_ref_size(op);
  switch (phase) {
  case kno_commit_start:
    return kno_write_rollback("oidpool_commit",
                             p->poolid,p->pool_source,
                             (256+(chunk_ref_size*p->pool_capacity)));
  case kno_commit_write: {
    return oidpool_storen(p,commits->commit_count,
                          commits->commit_oids,
                          commits->commit_vals);}
  case kno_commit_sync:
    return 0;
  case kno_commit_cleanup: {
    u8_string source = p->pool_source;
    u8_string rollback_file = u8_mkstring("%s.rollback",source);
    if (u8_file_existsp(rollback_file)) {
      int rv = u8_removefile(rollback_file);
      if (rv<0) {
        int saved_errno = errno; errno=0;
        u8_logf(LOG_WARN,"OidPoolCleanupFailed",
                "Rollback file %s couldn't be deleted errno=%d:%s",
                rollback_file,saved_errno,u8_strerror(saved_errno));}
      u8_free(rollback_file);
      return 0;}
    else {
      u8_logf(LOG_WARN,"Rollback file %s was deleted",rollback_file);
      u8_free(rollback_file);
      return -1;}}
  case kno_commit_rollback: {
    u8_string source = p->pool_source;
    u8_string rollback_file = u8_mkstring("%s.rollback",source);
    if (u8_file_existsp(rollback_file)) {
      ssize_t rv = kno_apply_head(rollback_file,source);
      u8_free(rollback_file);
      if (rv<0) return -1; else return 1;}
    else {
      u8_logf(LOG_CRIT,"NoRollbackFile",
              "The rollback file %s for %s doesn't exist",
              rollback_file,p->poolid);
      u8_free(rollback_file);
      return -1;}}
  default: {
    u8_logf(LOG_INFO,"NoPhasedCommit",
            "The pool %s doesn't support phased commits",
            p->poolid);
    return 0;}
  }
}

/* Three different ways to write offdata */

static ssize_t mmap_write_offdata
(struct KNO_OIDPOOL *op,kno_stream stream,
 int n, struct OIDPOOL_SAVEINFO *saveinfo,
 unsigned int min_off,unsigned int max_off);
static ssize_t cache_write_offdata
(struct KNO_OIDPOOL *op,kno_stream stream,
 int n, struct OIDPOOL_SAVEINFO *saveinfo,
 unsigned int min_off,unsigned int max_off);
static ssize_t direct_write_offdata
(struct KNO_OIDPOOL *op,kno_stream stream,
 int n, struct OIDPOOL_SAVEINFO *saveinfo);

static ssize_t write_offdata
(struct KNO_OIDPOOL *op, kno_stream stream,
 int n, struct OIDPOOL_SAVEINFO *saveinfo)
{
  unsigned int min_off=op->pool_load,  max_off=0, i=0;
  kno_offset_type offtype = op->pool_offtype;
  if (!((offtype == KNO_B32)||(offtype = KNO_B40)||(offtype = KNO_B64))) {
    u8_logf(LOG_WARN,"Corrupted oidpool struct",
            "Bad offset type code (%d) for %s",(int)offtype,op->poolid);
    u8_seterr("CorruptedOidpoolStruct","oidpool:write_offdata",
              u8_strdup(op->poolid));
    u8_big_free(saveinfo);
    return -1;}
  else if (op->pool_offdata)
    while (i<n) {
      unsigned int oidoff = saveinfo[i++].oidoff;
      if (oidoff>max_off) max_off = oidoff;
      if (oidoff<min_off) min_off = oidoff;}

  if (op->pool_offdata) {
#if KNO_USE_MMAP
    ssize_t result=mmap_write_offdata(op,stream,n,saveinfo,min_off,max_off);
    if (result>=0) return result;
#endif
    result=cache_write_offdata(op,stream,n,saveinfo,min_off,max_off);
    if (result>=0) {
      kno_clear_errors(0);
      return result;}}
  return direct_write_offdata(op,stream,n,saveinfo);
}

static ssize_t mmap_write_offdata
(struct KNO_OIDPOOL *op,kno_stream stream,
 int n, struct OIDPOOL_SAVEINFO *saveinfo,
 unsigned int min_off,unsigned int max_off)
{
  int chunk_ref_size = get_chunk_ref_size(op);
  int retval = -1;
  u8_logf(LOG_INFO,"oidpool:write_offdata",
          "Finalizing %d oid values for %s",n,op->poolid);

  unsigned int *offdata = NULL;
  size_t byte_length =
    (op->pool_flags&KNO_POOL_ADJUNCT) ?
    (chunk_ref_size*(op->pool_capacity)) :
    (chunk_ref_size*(op->pool_load));
  /* Map a second version of offdata to modify */
  unsigned int *memblock=
    mmap(NULL,256+(byte_length),(PROT_READ|PROT_WRITE),MAP_SHARED,
         stream->stream_fileno,0);
  if ( (memblock==NULL) || (memblock == MAP_FAILED) ) {
    u8_logf(LOG_CRIT,u8_strerror(errno),
            "Failed MMAP of %lld bytes of offdata for oidpool %s",
            256+(byte_length),op->poolid);
    U8_CLEAR_ERRNO();
    u8_graberrno("oidpool_write_offdata",u8_strdup(op->poolid));
    return -1;}
  else offdata = memblock+64;
  switch (op->pool_offtype) {
  case KNO_B64: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      offdata[oidoff*3]=kno_net_order((saveinfo[k].chunk.off)>>32);
      offdata[oidoff*3+1]=kno_net_order((saveinfo[k].chunk.off)&(0xFFFFFFFF));
      offdata[oidoff*3+2]=kno_net_order(saveinfo[k].chunk.size);
      k++;}
    break;}
  case KNO_B32: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      offdata[oidoff*2]=kno_net_order(saveinfo[k].chunk.off);
      offdata[oidoff*2+1]=kno_net_order(saveinfo[k].chunk.size);
      k++;}
    break;}
  case KNO_B40: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff, w1 = 0, w2 = 0;
      kno_convert_KNO_B40_ref(saveinfo[k].chunk,&w1,&w2);
      offdata[oidoff*2]=kno_net_order(w1);
      offdata[oidoff*2+1]=kno_net_order(w2);
      k++;}
    break;}
  default:
    u8_logf(LOG_WARN,"Bad offset type for %s",op->poolid);
    u8_big_free(saveinfo);
    exit(-1);}
  retval = msync(offdata-64,256+byte_length,MS_SYNC|MS_INVALIDATE);
  if (retval<0) {
    u8_logf(LOG_WARN,u8_strerror(errno),
            "oidpool:write_offdata:msync %s",op->poolid);
    u8_graberrno("oidpool_write_offdata:msync",u8_strdup(op->poolid));}
  retval = munmap(offdata-64,256+byte_length);
  if (retval<0) {
    u8_logf(LOG_WARN,u8_strerror(errno),
            "oidpool/oidpool_write_offdata:munmap %s",op->poolid);
    u8_graberrno("oidpool_write_offdata:munmap",u8_strdup(op->poolid));
    return -1;}
  return n;
}

static ssize_t cache_write_offdata
(struct KNO_OIDPOOL *op,kno_stream stream,
 int n, struct OIDPOOL_SAVEINFO *saveinfo,
 unsigned int min_off,unsigned int max_off)
{
  int chunk_ref_size = get_chunk_ref_size(op);
  size_t offdata_modified_length = chunk_ref_size*max_off-min_off;
  size_t offdata_modified_start = chunk_ref_size*min_off;
  unsigned int *offdata = u8_big_alloc(offdata_modified_length);
  if (offdata == NULL) {
    u8_graberrno("oidpool:write_offdata:malloc",u8_strdup(op->poolid));
    return -1;}
  memcpy(offdata,op->pool_offdata+offdata_modified_start,
         offdata_modified_length);
  switch (op->pool_offtype) {
  case KNO_B64: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      offdata[oidoff*3]=(saveinfo[k].chunk.off)>>32;
      offdata[oidoff*3+1]=(saveinfo[k].chunk.off)&(0xFFFFFFFF);
      offdata[oidoff*3+2]=saveinfo[k].chunk.size;
      k++;}
    break;}
  case KNO_B32: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      offdata[oidoff*2]=saveinfo[k].chunk.off;
      offdata[oidoff*2+1]=saveinfo[k].chunk.size;
      k++;}
    break;}
  case KNO_B40: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff, w1 = 0, w2 = 0;
      kno_convert_KNO_B40_ref(saveinfo[k].chunk,&w1,&w2);
      offdata[oidoff*2]=w1;
      offdata[oidoff*2+1]=w2;
      k++;}
    break;}
  default:
    u8_logf(LOG_WARN,"Bad offset type for %s",op->poolid);
    u8_big_free(saveinfo);
    exit(-1);}
  kno_setpos(stream,256+offdata_modified_start);
  kno_write_ints(stream,offdata_modified_length,offdata);
  u8_big_free(offdata);
  return n;
}

static ssize_t direct_write_offdata(struct KNO_OIDPOOL *op,kno_stream stream,
                                    int n, struct OIDPOOL_SAVEINFO *saveinfo)
{
  kno_outbuf outstream = kno_writebuf(stream);
  switch (op->pool_offtype) {
  case KNO_B32: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      kno_setpos(stream,256+oidoff*8);
      kno_write_4bytes(outstream,saveinfo[k].chunk.off);
      kno_write_4bytes(outstream,saveinfo[k].chunk.size);
      k++;}
    break;}
  case KNO_B40: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff, w1 = 0, w2 = 0;
      kno_setpos(stream,256+oidoff*8);
      kno_convert_KNO_B40_ref(saveinfo[k].chunk,&w1,&w2);
      kno_write_4bytes(outstream,w1);
      kno_write_4bytes(outstream,w2);
      k++;}
    break;}
  case KNO_B64: {
    int k = 0; while (k<n) {
      unsigned int oidoff = saveinfo[k].oidoff;
      kno_setpos(stream,256+oidoff*12);
      kno_write_8bytes(outstream,saveinfo[k].chunk.off);
      kno_write_4bytes(outstream,saveinfo[k].chunk.size);
      k++;}
    break;}
  default:
    u8_logf(LOG_WARN,"Bad offset type for %s",op->poolid);
    u8_big_free(saveinfo);
    exit(-1);}
  return n;
}

/* Allocating OIDs */

static lispval oidpool_alloc(kno_pool p,int n)
{
  lispval results = EMPTY; int i = 0;
  kno_oidpool op = (kno_oidpool)p;
  KNO_OID base = op->pool_base;
  unsigned int start;
  kno_lock_pool_struct((kno_pool)op,1);
  if (!(KNO_OIDPOOL_LOCKED(op))) lock_oidpool_file(op,0);
  if (op->pool_load+n>=op->pool_capacity) {
    kno_unlock_pool_struct((kno_pool)op);
    return kno_err(kno_ExhaustedPool,"oidpool_alloc",p->poolid,VOID);}
  start=op->pool_load; op->pool_load+=n;
  kno_unlock_pool_struct(p);
  while (i < n) {
    KNO_OID new_addr = KNO_OID_PLUS(base,start+i);
    lispval new_oid = kno_make_oid(new_addr);
    CHOICE_ADD(results,new_oid);
    i++;}
  return kno_simplify_choice(results);
}

/* Locking */

static int oidpool_lock(kno_pool p,lispval oids)
{
  struct KNO_OIDPOOL *fp = (struct KNO_OIDPOOL *)p;
  int retval = lock_oidpool_file(fp,1);
  return retval;
}

static int oidpool_unlock(kno_pool p,lispval oids)
{
  struct KNO_OIDPOOL *fp = (struct KNO_OIDPOOL *)p;
  if (fp->pool_changes.table_n_keys == 0)
    /* This unlocks the underlying file, not the stream itself */
    kno_streamctl(&(fp->pool_stream),kno_stream_unlockfile,NULL);
  return 1;
}

/* Setting the cachelevel */

static void oidpool_setcache(kno_pool p,int level)
{
  kno_oidpool op = (kno_oidpool)p;
  int chunk_ref_size = get_chunk_ref_size(op);
  if (chunk_ref_size<0) {
    u8_logf(LOG_WARN,kno_CorruptedPool,"Pool structure invalid: %s",p->poolid);
    return;}
  if ( ( (level<2) && (op->pool_offdata == NULL) ) ||
       ( (level==2) && ( op->pool_offdata != NULL ) ) )
    return;
  kno_lock_pool_struct((kno_pool)op,1);
  if ( ( (level<2) && (op->pool_offdata == NULL) ) ||
       ( (level==2) && ( op->pool_offdata != NULL ) ) ) {
    kno_unlock_pool_struct((kno_pool)op);
    return;}
#if (!(KNO_USE_MMAP))
  if (level < 2) {
    if (op->pool_offdata) {
      u8_big_free(op->pool_offdata);
      op->pool_offdata = NULL;}
    kno_unlock_pool_struct((kno_pool)op);
    return;}
  else {
    unsigned int *offsets;
    kno_stream s = &(op->pool_stream);
    kno_inbuf ins = kno_readbuf(s);
    if (LOCK_POOLSTREAM(op)<0) {
      kno_clear_errors(1);}
    else {
      size_t offsets_size = chunk_ref_size*(op->pool_load);
      kno_stream_start_read(s);
      kno_setpos(s,12);
      op->pool_load = load = kno_read_4bytes(ins);
      offsets = u8_big_alloc(offsets_size);
      kno_setpos(s,24);
      kno_read_ints(ins,load,offsets);
      op->pool_offdata = offsets;
      UNLOCK_POOLSTREAM(op);}
    kno_unlock_pool_struct((kno_pool)op);
    return;}
#else /* KNO_USE_MMAP */
  int stream_flags=op->pool_stream.stream_flags;

  if ( (level < 3) && (U8_BITP(stream_flags,KNO_STREAM_MMAPPED)) )
    kno_setbufsize(&(op->pool_stream),kno_filestream_bufsize);

  if ( (level < 2) && (op->pool_offdata) ) {
    /* Unmap the offsets cache */
    int retval;
    unsigned int *offdata = op->pool_offdata;
    size_t offdata_length = get_offdata_length(op);
    size_t header_size = 256+offdata_length;

    op->pool_offdata = NULL;

    /* The address to munmap is 64 (not 256) because offdata is
       an (unsigned int *) */
    retval = munmap(offdata-64,header_size);

    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),
              "oidpool_setcache:munmap %s",op->poolid);
      U8_CLEAR_ERRNO();}}

  if ( (LOCK_POOLSTREAM(op,"oidpool_setcache")) < 0) {
    u8_logf(LOG_WARN,"PoolStreamClosed",
            "During oidpool_setcache for %s",op->poolid);
    UNLOCK_POOLSTREAM(op);
    kno_unlock_pool_struct((kno_pool)op);
    return;}

  /* Everything below here requires a file descriptor */

  if ( (level >= 3) && (!(U8_BITP(stream_flags,KNO_STREAM_MMAPPED)) ) )
    kno_setbufsize(&(op->pool_stream),-1);

  if ( (level >= 2) && (op->pool_offdata == NULL) ) {
    unsigned int *offsets, *newmmap;
    /* Sizes here are in bytes */
    size_t offsets_size = (op->pool_capacity)*chunk_ref_size;
    size_t header_size = 256+offsets_size;
    /* Map the offsets */
    newmmap=
      mmap(NULL,header_size,PROT_READ,MAP_SHARED|MAP_NORESERVE,
           op->pool_stream.stream_fileno,
           0);
    if ((newmmap == NULL) || (newmmap == ((void *)-1))) {
      u8_logf(LOG_WARN,u8_strerror(errno),
              "oidpool_setcache:mmap %s",op->poolid);
      op->pool_offdata = NULL;
      U8_CLEAR_ERRNO();}
    else {
      op->pool_offdata = offsets = newmmap+64;} }

  UNLOCK_POOLSTREAM(op);
  kno_unlock_pool_struct((kno_pool)op);
#endif /* KNO_USE_MMAP */
}

#if KNO_USE_MMAP
static void reload_offdata(kno_oidpool op,int lock) {}
#else
static void reload_offdata(kno_oidpool op)
{
  unsigned int *offdata = op->pool_offdata;
  if (offdata==NULL) return;
  double start = u8_elapsed_time();
  kno_stream stream = &(op->pool_stream);
  kno_inbuf  readbuf = kno_readbuf(stream);
  /* Read new offsets table, compare it with the current, and
     only void those OIDs */
  unsigned int load;
  if ( (LOCK_POOLSTREAM(op,"oidpool/reload_offdata")) < 0) {
    u8_logf(LOG_WARN,"PoolStreamClosed",
            "During oidpool_reload_offdata for %s",op->poolid);
    UNLOCK_POOLSTREAM(op);
    return;}
  kno_setpos(s,0x10); new_load = kno_read_4bytes(ins);
  kno_setpos(s,0x100);
  kno_read_ints(ins,new_load,offdata);
  /* We need to clear cached values whose offsets have changed */
  op->pool_load = new_load;
  update_modtime(op);
  UNLOCK_POOLSTREAM(op)
    u8_logf(LOG_INFO,"ReloadOffsets",
            "Offsets for %s reloaded in %f secs",
            op->poolid,u8_elapsed_time()-start);
}
#endif

static void oidpool_close(kno_pool p)
{
  kno_oidpool op = (kno_oidpool)p;
  kno_lock_pool_struct(p,1);
  /* Close the stream */
  kno_close_stream(&(op->pool_stream),0);
  size_t offdata_length = 256+((op->pool_capacity)*get_chunk_ref_size(op));
  if (op->pool_offdata) {
    unsigned int *offdata=op->pool_offdata;
    op->pool_offdata = NULL;
    /* TODO: Be more careful about freeing/unmapping the
       offdata. Users might get a seg fault rather than a "file not
       open error". */
#if KNO_USE_MMAP
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval = munmap(offdata-64,offdata_length);
    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),
              "oidpool_close:munmap offsets %s",op->poolid);
      errno = 0;}
#else
    u8_big_free(op->pool_offdata);
#endif
    op->pool_cache_level = -1;}
  kno_unlock_pool_struct(p);
}

static void oidpool_setbuf(kno_pool p,ssize_t bufsize)
{
  kno_oidpool op = (kno_oidpool)p;
  kno_lock_pool_struct(p,1);
  kno_setbufsize(&(op->pool_stream),bufsize);
  kno_unlock_pool_struct(p);
}

/* Creating oidpool */

static unsigned int get_oidpool_format(kno_storage_flags sflags,lispval opts)
{
  unsigned int flags = 0;
  lispval offtype = kno_intern("OFFTYPE");
  lispval compression = kno_intern("COMPRESSION");
  if ( kno_testopt(opts,offtype,kno_intern("B64"))  ||
       kno_testopt(opts,offtype,KNO_INT(64)))
    flags |= KNO_B64;
  else if ( kno_testopt(opts,offtype,kno_intern("B40"))  ||
            kno_testopt(opts,offtype,KNO_INT(40)))
    flags |= KNO_B40;
  else if ( kno_testopt(opts,offtype,kno_intern("B32"))  ||
            kno_testopt(opts,offtype,KNO_INT(32)))
    flags |= KNO_B32;
  else flags |= KNO_B40;

  if (kno_testopt(opts,compression,kno_intern("ZLIB")))
    flags |= ((KNO_ZLIB)<<3);
  else if (kno_testopt(opts,compression,VOID))
    flags |= ((KNO_ZLIB)<<3);
  else {}

  if (kno_testopt(opts,kno_intern("DTYPEV2"),VOID))
    flags |= KNO_OIDPOOL_DTYPEV2;

  if ( (sflags) & (KNO_STORAGE_READ_ONLY) ||
       (kno_testopt(opts,kno_intern("READONLY"),VOID)) )
    flags |= KNO_OIDPOOL_READ_ONLY;

  if ( (kno_testopt(opts,FDSYM_ISADJUNCT,VOID)) ||
       (kno_testopt(opts,FDSYM_FLAGS,FDSYM_ISADJUNCT)) )
    flags |= KNO_OIDPOOL_ADJUNCT;

  if ( (sflags) & (KNO_POOL_ADJUNCT) ||
       (kno_testopt(opts,kno_intern("SPARSE"),VOID)) )
    flags |= KNO_OIDPOOL_SPARSE;


  return flags;
}

static int make_oidpool
(u8_string fname,u8_string label,
 KNO_OID base,unsigned int capacity,unsigned int load,
 unsigned int oidpool_format,lispval schemas_init,
 time_t ctime,time_t mtime,int cycles)
{
  time_t now = time(NULL);
  kno_off_t schemas_pos = 0, metadata_pos = 0, label_pos = 0;
  size_t schemas_size = 0, metadata_size = 0, label_size = 0;
  if (load>capacity) {
    u8_seterr(kno_PoolOverflow,"make_bigpool",
              u8_sprintf(NULL,256,
                         "Specified load (%u) > capacity (%u) for '%s'",
                         load,capacity,fname));
    return -1;}
  if (ctime<0) ctime = now;
  if (mtime<0) mtime = now;

  struct KNO_STREAM _stream, *stream=
    kno_init_file_stream(&_stream,fname,KNO_FILE_CREATE,-1,kno_driver_bufsize);
  kno_outbuf outstream = (stream) ? (kno_writebuf(stream)) : (NULL);
  kno_offset_type offtype =
    (kno_offset_type) ((oidpool_format)&(KNO_OIDPOOL_OFFMODE));
  if (outstream == NULL) return -1;
  else if ((stream->stream_flags)&KNO_STREAM_READ_ONLY) {
    kno_seterr3(kno_CantWrite,"kno_make_oidpool",fname);
    kno_free_stream(stream);
    return -1;}

  u8_logf(LOG_INFO,"CreateOIDPool",
          "Creating an oidpool '%s' for %u OIDs based at %x/%x",
          fname,capacity,KNO_OID_HI(base),KNO_OID_LO(base));

  stream->stream_flags &= ~KNO_STREAM_IS_CONSED;
  kno_lock_stream(stream);
  kno_setpos(stream,0);
  kno_write_4bytes(outstream,KNO_OIDPOOL_MAGIC_NUMBER);
  kno_write_4bytes(outstream,KNO_OID_HI(base));
  kno_write_4bytes(outstream,KNO_OID_LO(base));
  kno_write_4bytes(outstream,capacity);
  kno_write_4bytes(outstream,load);
  kno_write_4bytes(outstream,oidpool_format);

  kno_write_8bytes(outstream,0); /* Pool label */
  kno_write_4bytes(outstream,0); /* Pool label */

  kno_write_8bytes(outstream,0); /* metdata */
  kno_write_4bytes(outstream,0); /* metdata */

  kno_write_8bytes(outstream,0); /* schema data */
  kno_write_4bytes(outstream,0); /* schema data */

  /* Write the index creation time */
  kno_write_4bytes(outstream,0);
  kno_write_4bytes(outstream,((unsigned int)ctime));

  /* Write the index repack time */
  kno_write_4bytes(outstream,0);
  kno_write_4bytes(outstream,((unsigned int)now));

  /* Write the index modification time */
  kno_write_4bytes(outstream,0);
  kno_write_4bytes(outstream,((unsigned int)mtime));

  /* Write the number of repack cycles */
  if (mtime<0) mtime = now;
  kno_write_4bytes(outstream,0);
  kno_write_4bytes(outstream,cycles);

  /* Fill the rest of the space. */
  {
    int i = 0, bytes_to_write = 256-kno_getpos(stream);
    while (i<bytes_to_write) {
      kno_write_byte(outstream,0); i++;}}

  /* Write the top level bucket table */
  {
    int i = 0;
    if ((offtype == KNO_B32) || (offtype == KNO_B40))
      while (i<capacity) {
        kno_write_4bytes(outstream,0);
        kno_write_4bytes(outstream,0);
        i++;}
    else {
      while (i<capacity) {
        kno_write_8bytes(outstream,0);
        kno_write_4bytes(outstream,0);
        i++;}}}

  if (label) {
    int len = strlen(label);
    label_pos = kno_getpos(stream);
    kno_write_byte(outstream,dt_string);
    kno_write_4bytes(outstream,len);
    kno_write_bytes(outstream,label,len);
    label_size = kno_getpos(stream)-label_pos;}

  /* Write the schemas */
  if (VECTORP(schemas_init)) {
    schemas_pos = kno_getpos(stream);
    kno_write_dtype(outstream,schemas_init);
    schemas_size = kno_getpos(stream)-schemas_pos;}

  if (label_pos) {
    kno_setpos(stream,KNO_OIDPOOL_LABEL_POS);
    kno_write_8bytes(outstream,label_pos);
    kno_write_4bytes(outstream,label_size);}
  if (metadata_pos) {
    kno_setpos(stream,KNO_OIDPOOL_METADATA_POS);
    kno_write_8bytes(outstream,metadata_pos);
    kno_write_4bytes(outstream,metadata_size);}
  if (schemas_pos) {
    kno_setpos(stream,KNO_OIDPOOL_SCHEMAS_POS);
    kno_write_8bytes(outstream,schemas_pos);
    kno_write_4bytes(outstream,schemas_size);}

  kno_flush_stream(stream);
  kno_unlock_stream(stream);
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  return 0;
}

static
kno_pool oidpool_create
(u8_string spec,void *type_data,
 kno_storage_flags storage_flags,
 lispval opts)
{
  lispval base_oid = kno_getopt(opts,kno_intern("BASE"),VOID);
  lispval capacity_arg = kno_getopt(opts,kno_intern("CAPACITY"),VOID);
  lispval load_arg = kno_getopt(opts,kno_intern("LOAD"),KNO_FIXZERO);
  lispval label = kno_getopt(opts,FDSYM_LABEL,VOID);
  lispval schemas = kno_getopt(opts,kno_intern("SCHEMAS"),VOID);
  unsigned int capacity, load;
  int rv = 0;
  if (u8_file_existsp(spec)) {
    kno_seterr(_("FileAlreadyExists"),"oidpool_create",spec,VOID);
    return NULL;}
  else if (!(OIDP(base_oid))) {
    kno_seterr("Not a base oid","oidpool_create",spec,base_oid);
    rv = -1;}
  else if (KNO_ISINT(capacity_arg)) {
    int capval = kno_getint(capacity_arg);
    if (capval<=0) {
      kno_seterr("Not a valid capacity","oidpool_create",
                spec,capacity_arg);
      rv = -1;}
    else capacity = capval;}
  else {
    kno_seterr("Not a valid capacity","oidpool_create",spec,capacity_arg);
    rv = -1;}
  if (rv<0) {}
  else if (KNO_ISINT(load_arg)) {
    int loadval = kno_getint(load_arg);
    if (loadval<0) {
      kno_seterr("Not a valid load","oidpool_create",spec,load_arg);
      rv = -1;}
    else if (loadval > capacity) {
      kno_seterr(kno_PoolOverflow,"oidpool_create",spec,load_arg);
      rv = -1;}
    else load = loadval;}
  else if ( (FALSEP(load_arg)) || (EMPTYP(load_arg)) ||
            (VOIDP(load_arg)) || (load_arg == KNO_DEFAULT_VALUE))
    load=0;
  else {
    kno_seterr("Not a valid load","oidpool_create",
              spec,load_arg);
    rv = -1;}
  if (rv<0) return NULL;
  else rv = make_oidpool(spec,
                         ((STRINGP(label)) ? (CSTRING(label)) : (spec)),
                         KNO_OID_ADDR(base_oid),capacity,load,
                         get_oidpool_format(storage_flags,opts),
                         schemas,
                         time(NULL),
                         time(NULL),1);
  if (rv>=0) {
    kno_set_file_opts(spec,opts);
    return kno_open_pool(spec,storage_flags,opts);}
  else return NULL;
}

/* OIDPOOL get oids */

static lispval oidpool_getoids(kno_oidpool op)
{
  if (op->pool_cache_level<0) {
    kno_pool_setcache((kno_pool)op,kno_default_cache_level);}
  lispval results = EMPTY;
  KNO_OID base = op->pool_base;
  unsigned int i=0, load=op->pool_load;
  kno_stream stream = &(op->pool_stream);
  if (op->pool_offdata) {
    unsigned int *offdata = op->pool_offdata;
    unsigned int offdata_len = op->pool_capacity;
    while (i<load) {
      struct KNO_CHUNK_REF ref=
        kno_get_chunk_ref(offdata,op->pool_offtype,i,offdata_len);
      if (ref.off>0) {
        KNO_OID addr = KNO_OID_PLUS(base,i);
        KNO_ADD_TO_CHOICE(results,kno_make_oid(addr));}
      i++;}}
  else while (i<load) {
      struct KNO_CHUNK_REF ref=
        kno_fetch_chunk_ref(stream,256,op->pool_offtype,i,0);
      if (ref.off>0) {
        KNO_OID addr = KNO_OID_PLUS(base,i);
        KNO_ADD_TO_CHOICE(results,kno_make_oid(addr));}
      i++;}
  return results;
}

/* OIDPOOL ops */

static lispval oidpool_ctl(kno_pool p,lispval op,int n,lispval *args)
{
  struct KNO_OIDPOOL *fp = (struct KNO_OIDPOOL *)p;
  if ((n>0)&&(args == NULL))
    return kno_err("BadPoolOpCall","oidpool_op",fp->poolid,VOID);
  else if (n<0)
    return kno_err("BadPoolOpCall","oidpool_op",fp->poolid,VOID);
  else if (op == kno_cachelevel_op) {
    if (n==0)
      return KNO_INT(fp->pool_cache_level);
    else {
      lispval arg = (args)?(args[0]):(VOID);
      if ((FIXNUMP(arg))&&(FIX2INT(arg)>=0)&&
          (FIX2INT(arg)<0x100)) {
        oidpool_setcache(p,FIX2INT(arg));
        return KNO_INT(fp->pool_cache_level);}
      else return kno_type_error
             (_("cachelevel"),"oidpool_op/cachelevel",arg);}}
  else if (op == kno_reload_op) {
    if (fp->pool_offdata) reload_offdata(fp,0);
    kno_pool_swapout((kno_pool)fp,((n==0)?(KNO_VOID):(args[0])));
    return KNO_TRUE;}
  else if (op == kno_bufsize_op) {
    if (n==0)
      return KNO_INT(fp->pool_stream.buf.raw.buflen);
    else if (FIXNUMP(args[0])) {
      oidpool_setbuf(p,FIX2INT(args[0]));
      return KNO_INT(fp->pool_stream.buf.raw.buflen);}
    else return kno_type_error("buffer size","oidpool_op/bufsize",args[0]);}
  else if (op == kno_capacity_op)
    return KNO_INT(fp->pool_capacity);
  else if (op == kno_load_op)
    return KNO_INT(fp->pool_load);
  else if (op == kno_keys_op) {
    lispval keys = oidpool_getoids(fp);
    return kno_simplify_choice(keys);}
  else return kno_default_poolctl(p,op,n,args);
}

/* Initializing the driver module */

static struct KNO_POOL_HANDLER oidpool_handler={
  "oidpool", 1, sizeof(struct KNO_OIDPOOL), 12,
  oidpool_close, /* close */
  oidpool_alloc, /* alloc */
  oidpool_fetch, /* fetch */
  oidpool_fetchn, /* fetchn */
  oidpool_load, /* getload */
  oidpool_lock, /* lock */
  oidpool_unlock, /* release */
  oidpool_commit, /* commit */
  NULL, /* swapout */
  oidpool_create, /* create */
  NULL,  /* walk */
  NULL, /* recycle */
  oidpool_ctl  /* poolctl */
};

KNO_EXPORT void kno_init_oidpool_c()
{
  u8_register_source_file(_FILEINFO);

  kno_register_pool_type
    ("oidpool",
     &oidpool_handler,
     open_oidpool,
     kno_match_pool_file,
     (void*)U8_INT2PTR(KNO_OIDPOOL_MAGIC_NUMBER));
  kno_register_pool_type
    ("damaged_oidpool",
     &oidpool_handler,
     open_oidpool,
     kno_match_pool_file,
     (void*)U8_INT2PTR(KNO_OIDPOOL_TO_RECOVER));

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
