/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif


// TODO: Update this documentation
// TODO: Make the implementation actually track the number of keyblocks and valueblocks

/* Notes:
   A normal hash index with N buckets consists of 256 bytes of
   header, followed by N*8 bytes of offset table, followed by an arbitrary
   number of "data blocks", each starting with a varint-encoded byte,
   count and a varint-encoded element count;

   The header consists of: a 4-byte magic number (identifying the file
   type), a 4-byte number indicating the number of buckets, 1 byte
   indicating the "hash function" for the bucket and 3 bytes indicating
   other flags and followed by a 4 byte number reserved for customizing
   the hash function. This is followed by a 4 byte offset and a 4 byte
   size pointing to a XTYPE representation for the index's metadata.
   Following this are

   0x00	    XXXX    4-byte magic number
   0x04	    XXXX    number of buckets
   0x08	    XXXX    flags, including hash function identifier
   0x0C	    XXXX    hash function constant
   0x10	    XXXX    number of keys
   0x14	    XXXX    file offset of slotids vector
   0x18	    XXXX     (64 bits)
   0x1C	    XXXX    size of slotids XTYPE representation
   0x20	    XXXX    file offset of baseoids vector
   0x28	    XXXX     (64 bits)
   0x2C	    XXXX    size of baseoids XTYPE representation
   0x30	    XXXX    file offset of index metadata
   0x34	    XXXX     (64 bits)
   0x38	    XXXX    size of metadata XTYPE representation
   0x3C	    XXXX    number of used buckets
   0x40	    XXXX    number of keyblocks
   0x44	    XXXX     (64 bits)
   0x48	    XXXX    number of value blocks
   0x4C	    XXXX     (64 bits)
   0x80	    XXXX    reserved for future use
   ....
   0xEA	    XXXX    end of valid data
   0xEC	    XXXX     (64 bits)
   0x100    XXXX    beginning of offsets table

   There are two basic kinds of data blocks: key blocks and value
   blocks.  Continuation offsets are not currently supported for key
   blocks.  A value block consists of a byte count, an element count
   (N), and a (possibly zero) continuation pointer, followed by N
   "zvalue" xtype representations.  A key block consists of a
   byte_count, an element_count, and a number of key entries.  A key
   entry consists of a varint-coded size (in bytes) followed by a zkey
   xtype representation, followed by a varint value_count.  If the
   value_count is zero, the entry ends, if it is one, it is followed
   by a single zvalue xtype.  Otherwise, it is followed by a 4 byte
   file offset to a value block and a varint coded block size.

   A zvalue xtype representation consists of a varint encoded OID serial
   number.  If this is zero, it is followed by a regular XTYPE representation.
   Otherwise, it is followed by three bytes of OID offset which are added
   to the base OID associated with the serial number.

   A zkey xtype representation consists of a varint encoded SLOTID
   serial number followed by a regular XTYPE representation.  If the
   serial number is zero, the key is the following XTYPE; otherwise,
   the key is a pair of the corresponding SLOTID and the following
   XTYPE.
*/

extern int kno_storage_loglevel;
static int kindex_loglevel = -1;
#define U8_LOGLEVEL (kno_int_default(kindex_loglevel,(kno_storage_loglevel-1)))

#define KNO_INLINE_BUFIO 1
#define KNO_INLINE_XTYPE_REFS 1
#define KNO_INLINE_CHOICES (!(KNO_AVOID_INLINE))
#define KNO_FAST_CHOICE_CONTAINSP (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/xtypes.h"
#include "kno/numbers.h"
#include "kno/storage.h"
#include "kno/streams.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

#include "headers/kindex.h"

#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8pathfns.h>
#include <libu8/libu8io.h>

#include <errno.h>
#include <math.h>
#include <sys/stat.h>

#if (KNO_USE_MMAP)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#define MMAP_FLAGS MAP_SHARED
#endif

#ifndef KNO_DEBUG_KINDEXES
#define KNO_DEBUG_KINDEXES 0
#endif

#ifndef KNO_DEBUG_XTYPEIO
#define KNO_DEBUG_XTYPEIO 0
#endif

#ifndef KX_KEYBUF_SIZE
#define KX_KEYBUF_SIZE 8000
#endif

#ifndef KX_VALBUF_SIZE
#define KX_VALBUF_SIZE 16000
#endif

#define LOCK_STREAM 1
#define DONT_LOCK_STREAM 0

#if KNO_DEBUG_KINDEXES
#define CHECK_POS(pos,stream)					    \
  if ((pos)!=(kno_getpos(stream)))				     \
    u8_logf(LOG_CRIT,"FILEPOS error","position mismatch %ld/%ld",   \
	    pos,kno_getpos(stream));				     \
  else {}
#define CHECK_ENDPOS(pos,stream)					\
  { ssize_t curpos = kno_getpos(stream), endpos = kno_endpos(stream);	  \
    if (((pos)!=(curpos)) && ((pos)!=(endpos)))				\
      u8_logf(LOG_CRIT,"ENDPOS error","position mismatch %ld/%ld/%ld",	\
	      pos,curpos,endpos);					\
    else {}								\
  }
#else
#define CHECK_POS(pos,stream)
#define CHECK_ENDPOS(pos,stream)
#endif

KNO_EXPORT u8_condition kno_TooManyArgs;

static kno_size_t kindex_default_size=32000;

#if 1
static int skip_xtype(kno_inbuf in,xtype_refs refs)
{
  return kno_validate_xtype(in,NULL);
}
#else
static int skip_xtype(kno_inbuf in,xtype_refs refs)
{
  lispval v = kno_read_xtype(in,refs);
  if (KNO_ABORTED(v)) return -1;
  kno_decref(v);
  return 1;
}
#endif

static kno_size_t get_maxpos(kno_kindex p)
{
  switch (p->index_offtype) {
  case KNO_B32:
    return ((kno_size_t)(((kno_size_t)1)<<32));
  case KNO_B40:
    return ((kno_size_t)(((kno_size_t)1)<<40));
  case KNO_B64:
    return ((kno_size_t)(((kno_size_t)1)<<62));
  default:
    return -1;}
}

static void init_cache_level(kno_index ix)
{
  if (RARELY(ix->index_cache_level<0)) {
    lispval opts = ix->index_opts;
    long long level=kno_getfixopt(opts,"CACHELEVEL",kno_default_cache_level);
    kno_index_setcache(ix,level);}
}

static lispval read_values(kno_kindex,lispval,int,kno_off_t,size_t);
static ssize_t update_xrefs(kno_kindex kx,xtype_refs refs,
			    struct KNO_STREAM *stream,
			    struct KNO_STREAM *head);

static struct KNO_INDEX_HANDLER kindex_handler;

static u8_condition CorruptedKIndex=_("Corrupted kindex file");
static u8_condition BadHashFn=_("kindex has unknown hash function");

static lispval set_symbol, drop_symbol, keycounts_symbol, created_upsym;
static lispval slotids_symbol, baseoids_symbol, buckets_symbol, nkeys_symbol;

/* Utilities for XTYPE I/O */

#define nobytes(in,nbytes) (RARELY(!(kno_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (USUALLY(kno_request_bytes(in,nbytes)))

#define output_byte(out,b)				\
  if (kno_write_byte(out,b)<0) return -1; else {}
#define output_4bytes(out,w)				\
  if (kno_write_4bytes(out,w)<0) return -1; else {}
#define output_bytes(out,bytes,n)			\
  if (kno_write_bytes(out,bytes,n)<0) return -1; else {}

/* Custom readers */

static ssize_t read_key(kno_inbuf in,xtype_refs refs)
{
  return kno_read_xtype(in,refs);
}

static ssize_t write_key(kno_outbuf in,lispval key,xtype_refs refs)
{
  return kno_write_xtype(in,key,refs);
}

static ssize_t read_value(kno_inbuf in,xtype_refs refs)
{
  return kno_read_xtype(in,refs);
}

static ssize_t write_value(kno_outbuf in,lispval val,xtype_refs refs)
{
  return kno_write_xtype(in,val,refs);
}

static ssize_t read_raw(kno_inbuf in)
{
  return kno_read_xtype(in,NULL);
}

static ssize_t write_raw(kno_outbuf in,lispval val)
{
  return kno_write_xtype(in,val,NULL);
}

/* Getting chunk refs */

static ssize_t get_chunk_ref_size(kno_kindex ix)
{
  switch (ix->index_offtype) {
  case KNO_B32: case KNO_B40: return 8;
  case KNO_B64: return 12;}
  return -1;
}

/* Opening a hash index */

static int load_header(struct KNO_KINDEX *index,struct KNO_STREAM *stream);

static kno_index open_kindex(u8_string fname,kno_storage_flags open_flags,
			       lispval opts)
{
  struct KNO_KINDEX *index = u8_alloc(struct KNO_KINDEX);
  int read_only = U8_BITP(open_flags,KNO_STORAGE_READ_ONLY);

  if ( (read_only == 0) && (u8_file_writablep(fname)) ) {
    if (kno_check_rollback("open_kindex",fname)<0) {
      /* If we can't apply the rollback, open the file read-only */
      u8_log(LOG_WARN,"RollbackFailed",
	     "Opening kindex %s as read-only due to failed rollback",
	     fname);
      kno_clear_errors(1);
      read_only=1;}}
  else read_only=1;

  u8_string abspath = u8_abspath(fname,NULL);
  u8_string realpath = u8_realpath(fname,NULL);
  unsigned int magicno;
  ssize_t bufsize = kno_get_bufsize(opts,kno_driver_bufsize,0);
  kno_stream_mode mode=
    ((read_only) ? (KNO_FILE_READ) : (KNO_FILE_MODIFY));

  kno_init_index((kno_index)index,&kindex_handler,
		 fname,abspath,realpath,
		 open_flags,KNO_VOID,opts);

  int stream_flags =
    KNO_STREAM_CAN_SEEK | KNO_STREAM_NEEDS_LOCK | KNO_STREAM_READ_ONLY;
  kno_stream stream=
    kno_init_file_stream(&(index->index_stream),abspath,mode,stream_flags,bufsize);
  u8_free(abspath); u8_free(realpath);

  if (stream == NULL) {
    u8_free(index);
    kno_seterr3(u8_CantOpenFile,"open_kindex",fname);
    return NULL;}
  /* See if it ended up read only */
  if (!(u8_file_writablep(fname)))
    read_only = 1;
  stream->stream_flags &= ~KNO_STREAM_IS_CONSED;
  magicno = kno_read_4bytes_at(stream,0,KNO_ISLOCKED);
  if ( magicno != KNO_KINDEX_MAGIC_NUMBER) {
    kno_seterr3(kno_NotAFileIndex,"open_kindex",fname);
    u8_free(index);
    kno_close_stream(stream,KNO_STREAM_FREEDATA|KNO_STREAM_NOFLUSH);
    return NULL;}

  index->index_n_buckets =
    kno_read_4bytes_at(stream,KNO_KINDEX_NBUCKETS_POS,KNO_ISLOCKED);
  index->index_offdata = NULL;
  index->kindex_format =
    kno_read_4bytes_at(stream,KNO_KINDEX_FORMAT_POS,KNO_ISLOCKED);
  if (read_only) {
    U8_SETBITS(index->index_flags,KNO_STORAGE_READ_ONLY);}
  else if ((index->kindex_format) & (KNO_KINDEX_READ_ONLY) ) {
    U8_SETBITS(index->index_flags,KNO_STORAGE_READ_ONLY);}
  else NO_ELSE;

  if ((open_flags) & (KNO_INDEX_ONESLOT) ) {}
  else if ((index->kindex_format) & (KNO_KINDEX_ONESLOT) ) {
    U8_SETBITS(index->index_flags,KNO_INDEX_ONESLOT);}
  else NO_ELSE;

  if (((index->kindex_format)&(KNO_KINDEX_FN_MASK))!=0)	 {
    u8_free(index);
    kno_seterr3(BadHashFn,"open_kindex",NULL);
    return NULL;}

  index->index_offtype = (kno_offset_type)
    (((index->kindex_format)&(KNO_KINDEX_OFFTYPE_MASK))>>4);

  index->index_custom = kno_read_4bytes_at(stream,12,KNO_ISLOCKED);

  index->table_n_keys = kno_read_4bytes_at(stream,KNO_KINDEX_NKEYS_POS,KNO_ISLOCKED);

  int rv = load_header(index,stream);

  if (rv < 0) {
    kno_free_stream(stream);
    u8_free(index);
    return NULL;}
  else NO_ELSE;

  u8_init_mutex(&(index->index_lock));

  kno_register_index((kno_index)index);

  return (kno_index)index;
}

static int load_header(struct KNO_KINDEX *index,struct KNO_STREAM *stream)
{
  u8_string fname = index->index_source;

  int err = 0;
  ssize_t n_xrefs = kno_read_4bytes_at
    (stream,KNO_KINDEX_NREFS_POS,KNO_ISLOCKED);
  kno_off_t xrefs_pos = kno_read_8bytes_at
    (stream,KNO_KINDEX_XREFS_POS,KNO_ISLOCKED,&err);
  ssize_t xrefs_size = kno_read_4bytes_at
    (stream,KNO_KINDEX_XREFS_POS+8,KNO_ISLOCKED);

  int flags = XTYPE_REFS_ADD_OIDS | XTYPE_REFS_ADD_SYMS;

  if ( (n_xrefs) && (xrefs_pos) && (xrefs_size) ) {
    kno_inbuf in = kno_start_read(stream,xrefs_pos);
    int max_xrefs = ((n_xrefs/256)+1)*256;
    lispval *elts = u8_alloc_n(max_xrefs,lispval);
    int i = 0; while (i < n_xrefs) {
      elts[i] = read_raw(in);
      i++;}
    while (i < max_xrefs) elts[i++]=KNO_VOID;
    kno_init_xrefs(&(index->index_xrefs),
		   n_xrefs,max_xrefs,-1,flags,
		   elts,NULL);}
  else {
    lispval *elts = u8_alloc_n(256,lispval);
    int i = 0; while (i < 256) elts[i++]=KNO_VOID;
    kno_init_xrefs(&(index->index_xrefs),
		   0,256,-1,flags,
		   elts,NULL);}

  kno_off_t metadata_loc  = kno_read_8bytes_at
    (stream,KNO_KINDEX_METADATA_POS,KNO_ISLOCKED,NULL);
  ssize_t metadata_size = kno_read_4bytes_at
    (stream,KNO_KINDEX_METADATA_POS+8,KNO_ISLOCKED);

  int modified_metadata = 0;
  lispval metadata=KNO_VOID;
  if ( (metadata_size) && (metadata_loc) ) {
    if (kno_setpos(stream,metadata_loc)>0) {
      kno_inbuf in = kno_readbuf(stream);
      metadata = read_raw(in);}
    else {
      kno_seterr("BadMetaData","open_kindex",
		"BadMetadataLocation",KNO_INT(metadata_loc));
      metadata=KNO_ERROR_VALUE;}}
  else metadata=KNO_FALSE;

  if (KNO_FALSEP(metadata))
    metadata = KNO_VOID;
  else if (KNO_VOIDP(metadata)) {}
  else if (KNO_SLOTMAPP(metadata)) {
    lispval lisp_ctime = kno_get(metadata,created_upsym,KNO_VOID);
    if (!(KNO_VOIDP(lisp_ctime))) {
      /* Lowercase metadata symbols, reopen/read with FIXCASE */
      u8_log(LOGWARN,"LegacySymbols",
	     "Opening %s with legacy FramerD symbols, re-reading metadata",
	     fname);
      if (kno_setpos(stream,metadata_loc)>0) {
	kno_inbuf in = kno_readbuf(stream);
	lispval new_metadata = read_raw(in);
	kno_decref(metadata);
	metadata=new_metadata;}
      else {
	metadata=KNO_ERROR_VALUE;}}
    kno_decref(lisp_ctime);}
  else if ( index->index_flags & KNO_STORAGE_REPAIR ) {
    u8_log(LOG_WARN,"BadMetadata",
	       "Repairing bad metadata for %s @%ll_d+%ll_d = %q",
	       fname,metadata_loc,metadata_size,metadata);
    if (KNO_ABORTP(metadata)) kno_clear_errors(1);
    kno_decref(metadata);
    metadata=kno_empty_slotmap();
    modified_metadata=1;}
  else return -1;

  if (KNO_VOIDP(metadata)) {}
  else if (KNO_SLOTMAPP(metadata)) {
    kno_index_set_metadata((kno_index)index,metadata);
    if (modified_metadata) {
      KNO_XTABLE_SET_MODIFIED(&(index->index_metadata),1);}
    index->kx_metadata_pos = metadata_loc;}
  else {
    u8_log(LOG_WARN,"BadMetadata",
	   "Bad metadata for %s @%ll_d+%ll_d: %q",
	   fname,metadata_loc,metadata_size,metadata);}
  kno_decref(metadata);
  metadata = KNO_VOID;

  index->table_n_keys = kno_read_4bytes_at(stream,16,KNO_ISLOCKED);

  return 1;
}

static kno_index recover_kindex(u8_string fname,
				   kno_storage_flags open_flags,
				   lispval opts)
{
  u8_string recovery_file=u8_string_append(fname,".rollback",NULL);
  if (u8_file_existsp(recovery_file)) {
    ssize_t rv=kno_restore_head(recovery_file,fname);
    if (rv<0) {
      u8_graberrno("recover_kindex",recovery_file);
      return NULL;}
    else if (rv == 0)
      u8_logf(LOG_CRIT,CorruptedKIndex,
	      "The kindex file %s has a corrupted recovery file %s",
	      fname,recovery_file);
    else {
      kno_index opened = open_kindex(fname,open_flags,opts);
      if (opened) {
	u8_removefile(recovery_file);
	u8_free(recovery_file);
	return opened;}
      if (! (kno_testopt(opts,kno_intern("fixup"),KNO_VOID))) {
	kno_seterr("RecoveryFailed","recover_kindex",fname,KNO_VOID);
	return NULL;}
      else {
	u8_logf(LOG_ERR,"RecoveryFailed",
		"Recovering %s using %s failed",fname,recovery_file);
	u8_removefile(recovery_file);}}}
  else u8_logf(LOG_CRIT,CorruptedKIndex,
	       "The kindex file %s doesn't have a recovery file %s",
	       fname,recovery_file);
  if (kno_testopt(opts,kno_intern("fixup"),KNO_VOID)) {
    char *src = u8_tolibc(fname);
    KNO_DECL_OUTBUF(headbuf,256);
    unsigned int magicno = KNO_KINDEX_MAGIC_NUMBER;
    int out=open(src,O_RDWR);
    kno_write_4bytes(&headbuf,magicno);
#if HAVE_PREAD
    int rv = pwrite(out,headbuf.buffer,4,0);
#else
    lseek(out,SEEK_SET,0);
    int rv = write(out,headbuf.buffer,4);
#endif
    fsync(out);
    close(out);
    u8_free(src);
    if (rv>0) {
      u8_free(recovery_file);
      return open_kindex(fname,open_flags,opts);}
    else u8_seterr("FailedRecovery","recover_kindex",recovery_file);}
  return NULL;
}

/* Making a hash index */

KNO_EXPORT int make_kindex
(u8_string fname,
 int n_buckets,
 unsigned int flags,
 unsigned int hashconst,
 lispval metadata_init,
 lispval xrefs_init,
 time_t ctime,
 time_t mtime)
{
  time_t now = time(NULL);
  int n_xrefs = 0;
  ssize_t xrefs_pos = 0, xrefs_size = 0;
  ssize_t metadata_pos = 0, metadata_size = 0;
  int offtype = (kno_offset_type)(((flags)&(KNO_KINDEX_OFFTYPE_MASK))>>4);
  struct KNO_STREAM _stream, *stream=
    kno_init_file_stream(&_stream,fname,KNO_FILE_CREATE,-1,kno_driver_bufsize);
  struct KNO_OUTBUF *outstream = (stream) ? (kno_writebuf(stream)) : (NULL);

  if (outstream == NULL)
    return -1;
  else if ((stream->stream_flags)&KNO_STREAM_READ_ONLY) {
    kno_seterr3(kno_CantWrite,"make_kindex",fname);
    kno_free_stream(stream);
    return -1;}
  stream->stream_flags &= ~KNO_STREAM_IS_CONSED;

  outstream->buf_flags |= (KNO_WRITE_OPAQUE | KNO_NATSORT_VALUES);

  u8_logf(LOG_INFO,"CreateKIndex",
	  "Creating a kindex '%s' with %ld buckets",
	  fname,n_buckets);

  /* Remove leftover files */
  kno_remove_suffix(fname,".commit");
  kno_remove_suffix(fname,".rollback");

  kno_setpos(stream,0);
  kno_write_4bytes(outstream,KNO_KINDEX_MAGIC_NUMBER);
  kno_write_4bytes(outstream,n_buckets);
  kno_write_4bytes(outstream,flags);
  kno_write_4bytes(outstream,hashconst); /* No custom hash constant */
  kno_write_4bytes(outstream,0); /* No keys to start */

  /* This is where we store the offset and size for the slotid init */
  kno_write_8bytes(outstream,0);
  kno_write_4bytes(outstream,0);

  /* This is where we store the offset and size for the baseoids init */
  kno_write_8bytes(outstream,0);
  kno_write_4bytes(outstream,0);

  /* This is where we store the offset and size for the metadata */
  kno_write_8bytes(outstream,0);
  kno_write_4bytes(outstream,0);

  /* Write the index creation time */
  if (ctime<0) ctime = now;
  kno_write_4bytes(outstream,0);
  kno_write_4bytes(outstream,((unsigned int)ctime));

  /* Write the index repack time */
  kno_write_4bytes(outstream,0);
  kno_write_4bytes(outstream,((unsigned int)now));

  /* Write the index modification time */
  if (mtime<0) mtime = now;
  kno_write_4bytes(outstream,0);
  kno_write_4bytes(outstream,((unsigned int)mtime));

  /* Fill the rest of the space. */
  {
    int i = 0, bytes_to_write = 256-kno_getpos(stream);
    while (i<bytes_to_write) {
      kno_write_byte(outstream,0); i++;}}

  /* Write the top level bucket table */
  {
    int i = 0; while (i<n_buckets) {
      kno_write_4bytes(outstream,0);
      kno_write_4bytes(outstream,0);
      if (offtype == KNO_B64)
	kno_write_4bytes(outstream,0);
      else {}
      i++;}}

  /* Write the slotids */
  if (VECTORP(xrefs_init)) {
    lispval *elts = KNO_VECTOR_ELTS(xrefs_init);
    n_xrefs = KNO_VECTOR_LENGTH(xrefs_init);
    xrefs_pos = kno_getpos(stream);
    int i = 0; while (i<n_xrefs) {
      write_raw(outstream,elts[i]);
      i++;}
    xrefs_size = kno_getpos(stream)-xrefs_pos;}

  if (!(KNO_VOIDP(metadata_init))) {
    metadata_pos = kno_getpos(stream);
    write_raw(outstream,metadata_init);
    metadata_size = kno_getpos(stream)-metadata_pos;}

  if (xrefs_pos) {
    kno_setpos(stream,KNO_KINDEX_NREFS_POS);
    kno_write_4bytes(outstream,n_xrefs);
    kno_write_8bytes(outstream,xrefs_pos);
    kno_write_4bytes(outstream,xrefs_size);}

  if (metadata_pos) {
    kno_setpos(stream,KNO_KINDEX_METADATA_POS);
    kno_write_8bytes(outstream,metadata_pos);
    kno_write_4bytes(outstream,metadata_size);}

  kno_flush_stream(stream);
  kno_close_stream(stream,KNO_STREAM_FREEDATA);

  return 0;
}

/* Hash functions */

typedef unsigned long long ull;

KNO_FASTOP unsigned int hash_mult(unsigned int x,unsigned int y)
{
  unsigned long long prod = ((ull)x)*((ull)y);
  return (prod*2100000523)%(MYSTERIOUS_MODULUS);
}

KNO_FASTOP unsigned int hash_combine(unsigned int x,unsigned int y)
{
  if ((x == 0) && (y == 0)) return MAGIC_MODULUS+2;
  else if ((x == 0) || (y == 0))
    return x+y;
  else return hash_mult(x,y);
}

KNO_FASTOP unsigned int hash_bytes(const unsigned char *start,int len)
{
  unsigned int prod = 1, asint = 0;
  const unsigned char *ptr = start, *limit = ptr+len;
  /* Compute a starting place */
  while (ptr < limit) prod = prod+*ptr++;
  /* Now do a multiplication */
  ptr = start; limit = ptr+((len%4) ? (4*(len/4)) : (len));
  while (ptr < limit) {
    asint = (ptr[0]<<24)|(ptr[1]<<16)|(ptr[2]<<8)|(ptr[3]);
    prod = hash_combine(prod,asint); ptr = ptr+4;}
  switch (len%4) {
  case 0: asint = 1; break;
  case 1: asint = ptr[0]; break;
  case 2: asint = ptr[0]|(ptr[1]<<8); break;
  case 3: asint = ptr[0]|(ptr[1]<<8)|(ptr[2]<<16); break;}
  return hash_combine(prod,asint);
}

KNO_EXPORT ssize_t kindex_bucket(struct KNO_KINDEX *kx,lispval key,ssize_t modulate)
{
  struct KNO_OUTBUF out = { 0 }; unsigned char buf[1024];
  unsigned int hashval; int xtype_len;
  KNO_INIT_BYTE_OUTBUF(&out,buf,1024);
  out.buf_flags |= KNO_WRITE_OPAQUE;
  xtype_len = write_key(&out,key,&(kx->index_xrefs));
  hashval = hash_bytes(out.buffer,xtype_len);
  kno_close_outbuf(&out);
  if (modulate<0)
    return hashval%(kx->index_n_buckets);
  else if (modulate==0)
    return hashval;
  else return hashval%modulate;
}

/* Fetching */

static lispval kindex_fetch(kno_index ix,lispval key)
{
  struct KNO_KINDEX *kx = (kno_kindex)ix;
  struct KNO_STREAM *stream=&(kx->index_stream);
  unsigned int *offdata=kx->index_offdata;
  unsigned char buf[KX_KEYBUF_SIZE];
  struct KNO_OUTBUF out = { 0 };
  unsigned int hashval, bucket, n_keys, i, xtype_len, n_values;
  kno_off_t vblock_off; size_t vblock_size;
  KNO_CHUNK_REF keyblock;
  KNO_INIT_BYTE_OUTBUF(&out,buf,KX_KEYBUF_SIZE);
  xtype_refs refs = &(kx->index_xrefs);
  kno_hashtable lookup = refs->xt_lookup;
#if KNO_DEBUG_KINDEXES
  /* u8_message("Fetching the key %q from %s",key,kx->indexid); */
#endif
  /* If the index doesn't have oddkeys and you're looking up some feature (pair)
     whose slotid isn't in the slotids, the key isn't in the table. */
  if (PAIRP(key)) {
    lispval slotid = KNO_CAR(key);
    if ((SYMBOLP(slotid)) || (OIDP(slotid))) {
      if (kno_hashtable_test(lookup,slotid,KNO_VOID) == 0) {
#if KNO_DEBUG_KINDEXES
	u8_message("The slotid %q isn't indexed in %s, returning {}",
		   slotid,kx->indexid);
#endif
	kno_close_outbuf(&out);
	return EMPTY;}}}
  out.buf_flags |= KNO_WRITE_OPAQUE;
  xtype_len = write_key(&out,key,&(kx->index_xrefs));
  hashval = hash_bytes(out.buffer,xtype_len);
  bucket = hashval%(kx->index_n_buckets);
  if (offdata)
    keyblock = kno_get_chunk_ref(offdata,kx->index_offtype,bucket,
				kx->index_n_buckets);
  else keyblock=kno_fetch_chunk_ref(stream,256,kx->index_offtype,bucket,0);
  if (kno_bad_chunk(&keyblock)) {
    kno_close_outbuf(&out);
    return KNO_ERROR_VALUE;}
  else if (keyblock.size==0) {
    kno_close_outbuf(&out);
    return EMPTY;}
  else {
    unsigned char keybuf[KX_KEYBUF_SIZE];
    struct KNO_INBUF keystream={0};
    if (keyblock.size<KX_KEYBUF_SIZE) {
      KNO_INIT_INBUF(&keystream,keybuf,KX_KEYBUF_SIZE,KNO_STATIC_BUFFER);}
    struct KNO_INBUF *opened=
      kno_open_block(stream,&keystream,keyblock.off,keyblock.size,1);
    if (opened==NULL) {
      kno_close_outbuf(&out);
      return KNO_ERROR_VALUE;}
    n_keys = kno_read_varint(&keystream);
    i = 0; while (i<n_keys) {
      int key_len = kno_read_varint(&keystream);
      if ((key_len == xtype_len) &&
	  (memcmp(keystream.bufread,out.buffer,xtype_len)==0)) {
	keystream.bufread = keystream.bufread+key_len;
	n_values = kno_read_varint(&keystream);
	if (n_values==0) {
	  kno_close_inbuf(&keystream);
	  kno_close_outbuf(&out);
	  return EMPTY;}
	else if (n_values==1) {
	  lispval value = read_value(&keystream,&(kx->index_xrefs));
	  kno_close_inbuf(&keystream);
	  kno_close_outbuf(&out);
	  return value;}
	else {
	  vblock_off = (kno_off_t)kno_read_varint(&keystream);
	  vblock_size = (size_t)kno_read_varint(&keystream);
	  kno_close_inbuf(&keystream);
	  kno_close_outbuf(&out);
	  return read_values(kx,key,n_values,vblock_off,vblock_size);}}
      else {
	keystream.bufread = keystream.bufread+key_len;
	n_values = kno_read_varint(&keystream);
	if (n_values==0) {}
	else if (n_values==1) {
	  if (skip_xtype(&keystream,&(kx->index_xrefs))<0)
	    return KNO_ERROR;}
	else {
	  kno_read_varint(&keystream);
	  kno_read_varint(&keystream);}}
      i++;}
    kno_close_inbuf(&keystream);}
  kno_close_outbuf(&out);
  return EMPTY;
}

static KNO_CHUNK_REF read_value_block
(kno_kindex kx,lispval key,
 int n_values,KNO_CHUNK_REF chunk,
 int *n_readp,lispval *values,int *consp)
{
  kno_stream stream = &(kx->index_stream);
  int n_read=*n_readp;
  KNO_CHUNK_REF result = {-1,-1};
  kno_off_t vblock_off = chunk.off;
  size_t  vblock_size = chunk.size;
  struct KNO_INBUF instream={0};
  unsigned char stackbuf[KX_VALBUF_SIZE];
  if ( vblock_size < KX_VALBUF_SIZE ) {
    KNO_INIT_INBUF(&instream,stackbuf,KX_VALBUF_SIZE,0);}
  else if (! KNO_USE_MMAP) {
    unsigned char *usebuf = u8_malloc(vblock_size);
    KNO_INIT_INBUF(&instream,usebuf,KX_VALBUF_SIZE,KNO_HEAP_BUFFER);}
  else {}
  kno_inbuf vblock = kno_open_block(stream,&instream,vblock_off,vblock_size,1);
  if (vblock == NULL) {
    if (kx->index_flags & KNO_STORAGE_REPAIR) {
      u8_log(LOG_WARN,"BadBlockRef",
	     "Couldn't open value block (%_d/%_d) at %ll_d+%ll_d in %s for %q",
	     n_read,n_values,vblock_off,vblock_size,kx->index_source,key);
      result.off=0; result.size=0;
      return result;}
    else {
      u8_seterr("BadBlockRef","read_value_block/kindex",
		u8_mkstring("Couldn't open value block (%_d/%_d) at %ll_d+%ll_d in %s for %q",
			    n_read,n_values,vblock_off,vblock_size,kx->index_source,key));
      return result;}}
  kno_off_t next_off;
  ssize_t next_size=-1;
  int i=0, atomicp = 1;
  ssize_t n_elts = kno_read_varint(vblock);
  if (n_elts<0) {
    kno_close_inbuf(vblock);
    return result;}
  i = 0; while ( (i<n_elts) && (n_read < n_values) ) {
    lispval val = read_value(vblock,&(kx->index_xrefs));
    if (KNO_ABORTP(val)) {
      if (!(atomicp)) *consp=1;
      *n_readp = n_read;
      kno_close_inbuf(vblock);
      return result;}
    else if (CONSP(val)) atomicp = 0;
    else {}
    values[n_read]=val;
    n_read++;
    i++;}
  /* For vblock continuation pointers, we make the size be first,
     so that we don't need to store an offset if it's zero. */
  next_size = kno_read_varint(vblock);
  if (next_size<0) {}
  else if (next_size)
    next_off = kno_read_varint(vblock);
  else next_off = 0;
  if ( (next_size<0) || (next_off < 0)) {
    *n_readp = n_read;
    if (!(atomicp)) *consp=0;
    kno_close_inbuf(vblock);
    return result;}
  result.off=next_off; result.size=next_size;
  *n_readp = n_read;
  if (!(atomicp)) *consp=1;
  kno_close_inbuf(vblock);
  return result;
}

static lispval read_values
(kno_kindex kx,lispval key,int n_values,
 kno_off_t vblock_off,size_t vblock_size)

{
  struct KNO_CHOICE *result = kno_alloc_choice(n_values);
  KNO_CHUNK_REF chunk_ref = { vblock_off, vblock_size };
  lispval *values = (lispval *)KNO_XCHOICE_DATA(result);
  int consp = 0, n_read=0;
  while ( (chunk_ref.off>0) && (n_read < n_values) )
    chunk_ref=read_value_block(kx,key,n_values,chunk_ref,
			       &n_read,values,&consp);
  if (chunk_ref.off<0) {
    u8_byte buf[64];
    kno_seterr("KIndexError","read_values",
	      u8_sprintf(buf,64,"reading %_d values from %s",
			 n_values,kx->indexid),
	      key);
    result->choice_size=n_read;
    kno_decref_ptr(result);
    return KNO_ERROR;}
  else if (n_read != n_values) {
    u8_logf(LOG_WARN,"InconsistentValueSize",
	    "In '%s', the number of stored values "
	    "for %q, %ll_d != %ll_d (expected)",
	    kx->indexid,key,n_read,n_values);
    /* This makes freeing the pointer work */
    result->choice_size=n_read;
    kno_seterr("InconsistentValueSize","read_values",NULL,key);
    kno_decref_ptr(result);
    return KNO_ERROR;}
  else if (n_values == 1) {
    lispval v = values[0];
    kno_incref(v);
    kno_decref_ptr(result);
    return v;}
  else {
    return kno_init_choice
      (result,n_values,NULL,
       KNO_CHOICE_DOSORT|KNO_CHOICE_REALLOC|
       ((consp)?(KNO_CHOICE_ISCONSES):
	(KNO_CHOICE_ISATOMIC)));}
}


static int kindex_fetchsize(kno_index ix,lispval key)
{
  kno_kindex kx = (kno_kindex)ix;
  kno_stream stream = &(kx->index_stream);
  unsigned int *offdata=kx->index_offdata;
  struct KNO_OUTBUF out = { 0 }; unsigned char buf[64];
  unsigned int hashval, bucket, n_keys, i, xtype_len, n_values;
  KNO_CHUNK_REF keyblock;
  KNO_INIT_BYTE_OUTBUF(&out,buf,64);
  out.buf_flags |= KNO_WRITE_OPAQUE;
  xtype_len = write_key(&out,key,&(kx->index_xrefs));
  hashval = hash_bytes(out.buffer,xtype_len);
  bucket = hashval%(kx->index_n_buckets);
  if (offdata)
    keyblock = kno_get_chunk_ref(offdata,kx->index_offtype,bucket,
				kx->index_n_buckets);
  else keyblock = kno_fetch_chunk_ref
	 (&(kx->index_stream),256,kx->index_offtype,bucket,0);
  if ( (keyblock.off<0) || (keyblock.size<0) )
    return -1;
  else if (keyblock.size == 0)
    return keyblock.size;
  else {
    struct KNO_INBUF keystream={0};
    struct KNO_INBUF *opened=
      kno_open_block(stream,&keystream,keyblock.off,keyblock.size,1);
    if (opened==NULL)
      return -1;
    n_keys = kno_read_varint(&keystream);
    i = 0; while (i<n_keys) {
      int key_len = kno_read_varint(&keystream);
      n_values = kno_read_varint(&keystream);
      /* vblock_off = */ (void)(kno_off_t)kno_read_varint(&keystream);
      /* vblock_size = */ (void)(size_t)kno_read_varint(&keystream);
      if (key_len!=xtype_len)
	keystream.bufread = keystream.bufread+key_len;
      else if (memcmp(keystream.bufread,out.buffer,xtype_len)==0) {
	kno_close_inbuf(&keystream);
	kno_close_outbuf(&out);
	return n_values;}
      else keystream.bufread = keystream.bufread+key_len;
      i++;}
    kno_close_inbuf(&keystream);}
  kno_close_outbuf(&out);
  return 0;
}


/* Fetching multiple keys */

static int sort_ks_by_bucket(const void *k1,const void *k2)
{
  struct KEY_SCHEDULE *ks1 = (struct KEY_SCHEDULE *)k1;
  struct KEY_SCHEDULE *ks2 = (struct KEY_SCHEDULE *)k2;
  if (ks1->ksched_bucket<ks2->ksched_bucket) return -1;
  else if (ks1->ksched_bucket>ks2->ksched_bucket) return 1;
  else return 0;
}

static int sort_ks_by_refoff(const void *k1,const void *k2)
{
  struct KEY_SCHEDULE *ks1 = (struct KEY_SCHEDULE *)k1;
  struct KEY_SCHEDULE *ks2 = (struct KEY_SCHEDULE *)k2;
  if (ks1->ksched_chunk.off<ks2->ksched_chunk.off) return -1;
  else if (ks1->ksched_chunk.off>ks2->ksched_chunk.off) return 1;
  else return 0;
}

static int sort_vs_by_refoff(const void *v1,const void *v2)
{
  struct VALUE_SCHEDULE *vs1 = (struct VALUE_SCHEDULE *)v1;
  struct VALUE_SCHEDULE *vs2 = (struct VALUE_SCHEDULE *)v2;
  if (vs1->vsched_chunk.off<vs2->vsched_chunk.off) return -1;
  else if (vs1->vsched_chunk.off>vs2->vsched_chunk.off) return 1;
  else return 0;
}

static int match_keybuf(u8_string buf,int size,
			struct KEY_SCHEDULE *ksched,
			u8_string keyreps)
{
  return ((size == ksched->ksched_xtsize) &&
	  (memcmp(keyreps+ksched->ksched_keyoff,buf,size)==0));
}

static lispval *fetchn(struct KNO_KINDEX *kx,int n,const lispval *keys)
{
  if (n == 0) return NULL;

  lispval *values = u8_big_alloc_n(n,lispval);
  /* This is a buffer where we write keybuf representations of all of the
     keys, which let's use do memcmp to match them to on-disk data */
  struct KNO_OUTBUF keysbuf = { 0 };
  /* This is used to fetch information about keys, sorted to be linear */
  struct KEY_SCHEDULE *ksched = u8_big_alloc_n(n,struct KEY_SCHEDULE);
  /* This is used to fetch the chained values in the table, also
     sorted to be linear. */
  struct VALUE_SCHEDULE *vsched = u8_big_alloc_n(n,struct VALUE_SCHEDULE);
  if ( (values==NULL) || (ksched==NULL) || (vsched==NULL) ) {
    u8_seterr(kno_MallocFailed,"kindex_fetchn",NULL);
    if (values) u8_big_free(values);
    if (ksched) u8_big_free(ksched);
    if (vsched) u8_big_free(vsched);
    return NULL;}

  unsigned int *offdata = kx->index_offdata;
  unsigned char *keyreps;
  int i = 0, n_entries = 0, vsched_size = 0;
  size_t vbuf_size=0;
  kno_stream stream = &(kx->index_stream);
  ssize_t keybuf_size = ( (n*200) > 8000 ) ? (n*200) : (8000);
#if KNO_DEBUG_KINDEXES
  u8_message("Reading %_d keys from %s",n,kx->indexid);
#endif
  /* Initialize sized based on assuming 32 bytes per key */
  KNO_INIT_BYTE_OUTPUT(&keysbuf,keybuf_size);
  keysbuf.buf_flags |= KNO_WRITE_OPAQUE;

  /* Fill out a fetch schedule, computing hashes and buckets for each
     key.  If we have an offsets table, we compute the offsets during
     this phase, otherwise we defer to an additional loop.

     This also writes out XTYPE representations for all of the keys
     and we use a direct memcmp to match requested keys to byte ranges
     in fetched buckets. */
  xtype_refs refs = &(kx->index_xrefs);
  kno_hashtable lookup = refs->xt_lookup;
  while (i<n) {
    lispval key = keys[i];
    int xt_start = keysbuf.bufwrite-keysbuf.buffer;
    int xt_size, bucket;
    /* If the index doesn't have oddkeys and you're looking up some feature (pair)
       whose slotid isn't in the slotids, the key isn't in the table. */
    if (PAIRP(key)) {
      lispval slotid = KNO_CAR(key);
      if ((SYMBOLP(slotid)) || (OIDP(slotid))) {
	if (kno_hashtable_test(lookup,slotid,KNO_VOID) == 0) {
	  values[i++]=EMPTY;
	  continue;}}}
    ksched[n_entries].ksched_i = i;
    ksched[n_entries].ksched_key = key;
    ksched[n_entries].ksched_keyoff = xt_start;
    write_key(&keysbuf,key,&(kx->index_xrefs));
    xt_size = (keysbuf.bufwrite-keysbuf.buffer)-xt_start;
    ksched[n_entries].ksched_xtsize = xt_size;
    ksched[n_entries].ksched_bucket = bucket=
      hash_bytes(keysbuf.buffer+xt_start,xt_size)%(kx->index_n_buckets);
    if (offdata) {
      /* Because we have an offsets table, we can use kno_get_chunk_ref,
	 which doesn't touch the stream, and use it immediately. */
      ksched[n_entries].ksched_chunk =
	kno_get_chunk_ref(offdata,kx->index_offtype,bucket,
			 kx->index_n_buckets);
      size_t keyblock_size = ksched[n_entries].ksched_chunk.size;
      if (keyblock_size==0) {
	/* It is empty, so we don't even need to handle this entry. */
	values[i]=EMPTY;
	/* We don't need to keep its xtype representation around either,
	   so we reset the key stream. */
	keysbuf.bufwrite = keysbuf.buffer+xt_start;}
      else n_entries++;}
    else n_entries++;
    i++;}
  keyreps=keysbuf.buffer;
  if (offdata == NULL) {
    int write_at = 0;
    /* When fetching bucket references, we sort the schedule first, so that
       we're accessing them in order in the file. */
    qsort(ksched,n_entries,sizeof(struct KEY_SCHEDULE),
	  sort_ks_by_bucket);
    i = 0; while (i<n_entries) {
      ksched[i].ksched_chunk = kno_fetch_chunk_ref
	(stream,256,kx->index_offtype,ksched[i].ksched_bucket,0);
      size_t keyblock_size=ksched[i].ksched_chunk.size;
      if (keyblock_size==0) {
	values[ksched[i].ksched_i]=EMPTY;
	i++;}
      else if (write_at == i) {
	write_at++;
	i++;}
      else {
	ksched[write_at++]=ksched[i++];}}
    n_entries = write_at;}
  /* We now have the entries of all the keyblocks we're touching, so
     we sort them for serial access. */
  qsort(ksched,n_entries,sizeof(struct KEY_SCHEDULE),
	sort_ks_by_refoff);
  {
    unsigned char keyblock_buf[KX_KEYBUF_SIZE];
    struct KNO_INBUF keyblock={0};
    KNO_INIT_INBUF(&keyblock,keyblock_buf,KX_KEYBUF_SIZE,KNO_STATIC_BUFFER);
    int bucket = -1, j = 0, k = 0, n_keys=0;
    const unsigned char *keyblock_start=NULL;
    while (j<n_entries) {
      int found = 0;
      kno_off_t blockpos = ksched[j].ksched_chunk.off;
      kno_size_t blocksize = ksched[j].ksched_chunk.size;
      if (ksched[j].ksched_bucket != bucket) {
	/* If we're in a new bucket, open it as input */
	if (kno_open_block(stream,&keyblock,blockpos,blocksize,1) == NULL) {
	  if (kx->index_flags & KNO_STORAGE_REPAIR) {
	    u8_log(LOG_WARN,"BadBlockRef",
		   "Couldn't open bucket %_d at %ll_d+%ll_d in %s for %q",
		   bucket,blockpos,blocksize,kx->index_source,
		   ksched[j].ksched_key);
	    bucket=ksched[j].ksched_bucket;
	    while (ksched[j].ksched_bucket == bucket) j++;
	    continue;}
	  else {
	    u8_seterr("BadBlockRef","kindex_fetchn",u8_mkstring
		      ("Couldn't open bucket %_d at %ll_d+%ll_d in %s for %q",
		       bucket,blockpos,blocksize,kx->index_source,
		       ksched[j].ksched_key));
	    kno_close_inbuf(&keyblock);
	    u8_big_free(ksched);
	    u8_big_free(vsched);
	    kno_close_outbuf(&keysbuf);
	    return NULL;}}
	keyblock_start = keyblock.bufread;
	/* And initialize bucket position and limit */
	k=0; n_keys = kno_read_varint(&keyblock);
	keyblock_start = keyblock.bufread;}
      else {
	/* We could be smarter here, but it's probably not worth it */
	keyblock.bufread=keyblock_start;
	k=0;}
      while (k<n_keys) {
	int n_vals;
	kno_size_t dtsize = kno_read_varint(&keyblock);
	if (match_keybuf(keyblock.bufread,dtsize,&ksched[j],keyreps)) {
	  found = 1;
	  keyblock.bufread = keyblock.bufread+dtsize;
	  n_vals = kno_read_varint(&keyblock);
	  if (n_vals==0)
	    values[ksched[j].ksched_i]=EMPTY;
	  else if (n_vals==1)
	    /* Single values are stored inline in the keyblocks */
	    values[ksched[j].ksched_i]=
	      read_value(&keyblock,&(kx->index_xrefs));
	  else {
	    /* Populate the values schedule for that key */
	    kno_off_t block_off = kno_read_varint(&keyblock);
	    kno_size_t block_size = kno_read_varint(&keyblock);
	    struct KNO_CHOICE *result = kno_alloc_choice(n_vals);
	    /* Track the max vblock size for later buffer allocation */
	    if (block_size>vbuf_size) vbuf_size=block_size;
	    KNO_SET_CONS_TYPE(result,kno_choice_type);
	    result->choice_size = n_vals;
	    values[ksched[j].ksched_i]=(lispval)result;
	    vsched[vsched_size].vsched_i = ksched[j].ksched_i;
	    vsched[vsched_size].vsched_chunk.off = block_off;
	    vsched[vsched_size].vsched_chunk.size = block_size;
	    vsched[vsched_size].vsched_write = (lispval *)KNO_XCHOICE_DATA(result);
	    vsched[vsched_size].vsched_atomicp = 1;
	    vsched_size++;}
	  /* Advance the key index in case we have other keys to read
	     from this bucket */
	  k++;
	  /* This breaks out the loop iterating over the keys in this
	     bucket. */
	  break;}
	else {
	  /* Skip this key */
	  keyblock.bufread = keyblock.bufread+dtsize;
	  n_vals = kno_read_varint(&keyblock);
	  if (n_vals==0) {}
	  else if (n_vals==1) {
	    /* Read the one inline value */
	    /* TODO: replace with skip_zvalue */
	    lispval v = read_value(&keyblock,refs);
	    kno_decref(v);}
	  else {
	    /* Skip offset information */
	    kno_read_varint(&keyblock);
	    kno_read_varint(&keyblock);}}
	k++;}
      if (!(found))
	values[ksched[j].ksched_i]=EMPTY;
      j++;}
    kno_close_inbuf(&keyblock);}
  /* Now we're done with the ksched */
  u8_big_free(ksched);
  {
    struct KNO_INBUF vblock={0}, bigvblock={};
    unsigned char valbuf[KX_VALBUF_SIZE];
    KNO_INIT_INBUF(&vblock,valbuf,KX_VALBUF_SIZE,0);
    while (vsched_size) {
      qsort(vsched,vsched_size,sizeof(struct VALUE_SCHEDULE),
	    sort_vs_by_refoff);
      i = 0; while (i<vsched_size) {
	int j = 0, n_vals;
	kno_size_t next_size;
	off_t off = vsched[i].vsched_chunk.off;
	size_t size = vsched[i].vsched_chunk.size;
	kno_inbuf valstream =
	  ( (KNO_USE_MMAP) && (size > vblock.buflen) &&
	    (size > kno_bigbuf_threshold) ) ?
	  ( kno_open_block(stream,&bigvblock,off,size,1) ) :
	  ( kno_open_block(stream,&vblock,off,size,1) );
	n_vals = kno_read_varint(valstream);
	while (j<n_vals) {
	  lispval v = read_value(valstream,refs);
	  if (CONSP(v)) vsched[i].vsched_atomicp = 0;
	  *((vsched[i].vsched_write)++) = v;
	  j++;}
	next_size = kno_read_varint(valstream);
	if (next_size) {
	  vsched[i].vsched_chunk.size = next_size;
	  vsched[i].vsched_chunk.off = kno_read_varint(valstream);}
	else {
	  vsched[i].vsched_chunk.size = 0;
	  vsched[i].vsched_chunk.off = 0;}
	i++;}
      {
	struct VALUE_SCHEDULE *read = &(vsched[0]), *write = &(vsched[0]);
	struct VALUE_SCHEDULE *limit = read+vsched_size;
	vsched_size = 0; while (read<limit) {
	  if (read->vsched_chunk.size) {
	    *write++= *read++;
	    vsched_size++;}
	  else {
	    /* We're now done with this value, so we finalize it,
	       using kno_init_choice to sort it.  Note that (rarely),
	       kno_init_choice will free what was passed to it (if it
	       had zero or one element), so the real result is what is
	       returned and not what was passed in. */
	    int index = read->vsched_i, atomicp = read->vsched_atomicp;
	    struct KNO_CHOICE *result = (struct KNO_CHOICE *)values[index];
	    int n_values = result->choice_size;
	    lispval realv = kno_init_choice(result,n_values,NULL,
					    KNO_CHOICE_DOSORT|
					    ((atomicp)?(KNO_CHOICE_ISATOMIC):
					     (KNO_CHOICE_ISCONSES))|
					    KNO_CHOICE_REALLOC);
	    values[index]=realv;
	    read++;}}}}
    kno_close_inbuf(&vblock);
    kno_close_inbuf(&bigvblock);}
  u8_big_free(vsched);
#if KNO_DEBUG_KINDEXES
  u8_message("Finished reading %_d keys from %s",n,kx->indexid);
#endif
  kno_close_outbuf(&keysbuf);
  return values;
}

/* This is the handler exposed by the index handler struct */
static lispval *kindex_fetchn(kno_index ix,int n,const lispval *keys)
{
  return fetchn((kno_kindex)ix,n,keys);
}


/* Getting all keys */

static int sort_blockrefs_by_off(const void *v1,const void *v2)
{
  struct KNO_CHUNK_REF *br1 = (struct KNO_CHUNK_REF *)v1;
  struct KNO_CHUNK_REF *br2 = (struct KNO_CHUNK_REF *)v2;
  if (br1->off<br2->off) return -1;
  else if (br1->off>br2->off) return 1;
  else return 0;
}

static lispval *kindex_fetchkeys(kno_index ix,int *n)
{
  lispval *results = NULL;
  struct KNO_KINDEX *kx = (struct KNO_KINDEX *)ix;
  kno_stream s = &(kx->index_stream);
  unsigned int *offdata = kx->index_offdata;
  kno_offset_type offtype = kx->index_offtype;
  int i = 0, n_buckets = (kx->index_n_buckets), n_to_fetch = 0;
  int total_keys = 0, key_count = 0, buckets_len, results_len;
  KNO_CHUNK_REF *buckets;
  kno_lock_stream(s);
  total_keys = kno_read_4bytes(kno_start_read(s,16));
  if (total_keys==0) {
    kno_unlock_stream(s);
    *n = 0;
    return NULL;}
  buckets = u8_big_alloc_n(total_keys,KNO_CHUNK_REF);
  buckets_len=total_keys;
  if (buckets == NULL)
    return NULL;
  else {
    results = u8_big_alloc_n(total_keys,lispval);
    results_len=total_keys;}
  if (results == NULL) {
    kno_unlock_stream(s);
    u8_big_free(buckets);
    u8_seterr(kno_MallocFailed,"kindex_fetchkeys",NULL);
    return NULL;}
  /* If we have chunk offsets in memory, we don't need to keep the
     stream locked while we get them. */
  if (offdata) {
    kno_unlock_stream(s);
    while (i<n_buckets) {
      KNO_CHUNK_REF ref =
	kno_get_chunk_ref(offdata,offtype,i,kx->index_n_buckets);
      if (ref.size>0) {
	if (n_to_fetch >= buckets_len) {
	  u8_logf(LOG_WARN,"BadKeyCount",
		  "Bad key count in %s: %_d",ix->indexid,total_keys);
	  buckets=u8_big_realloc_n(buckets,n_buckets,KNO_CHUNK_REF);
	  buckets_len=n_buckets;}
	buckets[n_to_fetch++]=ref;}
      i++;}}
  else {
    while (i<n_buckets) {
      KNO_CHUNK_REF ref = kno_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) {
	if (n_to_fetch >= buckets_len) {
	  u8_logf(LOG_WARN,"BadKeyCount",
		  "Bad key count in %s: %_d",ix->indexid,total_keys);
	  buckets=u8_big_realloc_n(buckets,n_buckets,KNO_CHUNK_REF);
	  buckets_len=n_buckets;}
	buckets[n_to_fetch++]=ref;}
      i++;}
    kno_unlock_stream(s);}
  if (n_to_fetch > total_keys) {
    /* If total_keys is wrong, resize results to n_to_fetch */
    results = u8_big_realloc_n(results,n_to_fetch,lispval);
    results_len = n_to_fetch;}
  qsort(buckets,n_to_fetch,sizeof(KNO_CHUNK_REF),sort_blockrefs_by_off);
  unsigned char keyblock_buf[KX_KEYBUF_SIZE];
  struct KNO_INBUF keyblock={0};
  KNO_INIT_INBUF(&keyblock,keyblock_buf,KX_KEYBUF_SIZE,0);
  i = 0; while (i<n_to_fetch) {
    int j = 0, n_keys;
    if (!kno_open_block(s,&keyblock,buckets[i].off,buckets[i].size,1)) {
      kno_unlock_stream(s);
      kno_decref_elts(results,key_count);
      u8_big_free(buckets);
      u8_big_free(results);
      kno_close_inbuf(&keyblock);
      *n=-1;
      return NULL;}
    n_keys = kno_read_varint(&keyblock);
    while (j<n_keys) {
      lispval key; int n_vals;
      ssize_t xtype_len = kno_read_varint(&keyblock); /* IGNORE size */
      if (xtype_len<0) {
	kno_seterr(kno_UnexpectedEOD,"kindex_fetchkeys",
		   kx->indexid,KNO_VOID);
	key=KNO_ERROR_VALUE;}
      else {
	const unsigned char *key_end = keyblock.bufread+xtype_len;
	key = read_key(&keyblock,&(kx->index_xrefs));
	if (KNO_ABORTP(key)) keyblock.bufread=key_end;}
      if ( (n_vals = kno_read_varint(&keyblock)) < 0) {
	kno_seterr(kno_UnexpectedEOD,"kindex_fetchkeys",
		   kx->indexid,KNO_VOID);
	key=KNO_ERROR_VALUE;}
      if (!(KNO_TROUBLEP(key))) {
	if (key_count >= results_len) {
	  results=u8_big_realloc(results,LISPVEC_BYTELEN(results_len*2));
	  results_len = results_len * 2;
	  if (results == NULL) {
	    kno_unlock_stream(s);
	    u8_seterr(u8_MallocFailed,"kindex_fetchkeys",kx->indexid);
	    kno_decref_elts(results,key_count);
	    u8_big_free(buckets);
	    u8_big_free(results);
	    kno_close_inbuf(&keyblock);
	    *n=-1;
	    return NULL;}}
	results[key_count++]=key;}
      if (n_vals<0) {}
      else if (n_vals==0) {}
      else if (n_vals==1) {
	if (skip_xtype(&keyblock,&(kx->index_xrefs))<0)
	  key=KNO_ERROR;
	else NO_ELSE;}
      else {
	kno_read_varint(&keyblock);
	kno_read_varint(&keyblock);}
      if (KNO_ABORTP(key)) {
	if ( kx->index_flags & KNO_STORAGE_REPAIR ) {
	  kno_clear_errors(0);
	  u8_log(LOG_CRIT,"CorruptedKIndex",
		 "Error reading %_d keys @%ll_d+%ll_d in %s",
		 n_keys,buckets[i].off,buckets[i].size,kx->index_source);
	  j=n_keys;}
	else {
	  lispval off_pair = kno_init_pair(NULL,KNO_INT(buckets[i].off),
					  KNO_INT(buckets[i].size));
	  kno_seterr("CorruptedKIndex","kindex_fetchkeys",
		    kx->indexid,off_pair);
	  kno_decref(off_pair);
	  kno_close_inbuf(&keyblock);
	  u8_big_free(buckets);
	  kno_decref_elts(results,key_count);
	  u8_big_free(results);
	  *n = -1;
	  return NULL;}}
      else j++;}
    i++;}
  kno_close_inbuf(&keyblock);
  u8_big_free(buckets);
  *n = key_count;
  return results;
}

static void free_keysizes(struct KNO_KEY_SIZE *sizes,int n)
{
  int i=0; while (i<n) {
    lispval key = sizes[i++].keysize_key;
    kno_decref(key);}
}

static struct KNO_KEY_SIZE *kindex_fetchinfo(kno_index ix,kno_choice filter,int *n)
{
  struct KNO_KEY_SIZE *sizes = NULL;
  struct KNO_KINDEX *kx = (struct KNO_KINDEX *)ix;
  kno_stream s = &(kx->index_stream);
  unsigned int *offdata = kx->index_offdata;
  kno_offset_type offtype = kx->index_offtype;
  int i = 0, n_buckets = (kx->index_n_buckets), n_to_fetch = 0;
  int total_keys = 0, key_count = 0, buckets_len, sizes_len;
  KNO_CHUNK_REF *buckets;
  kno_lock_stream(s);
  total_keys = kno_read_4bytes(kno_start_read(s,16));
  if (total_keys==0) {
    kno_unlock_stream(s);
    *n = 0;
    return NULL;}
  buckets = u8_big_alloc_n(total_keys,KNO_CHUNK_REF);
  buckets_len=total_keys;
  if (buckets == NULL)
    return NULL;
  else {
    sizes = u8_big_alloc_n(total_keys,KNO_KEY_SIZE);
    sizes_len=total_keys;}
  if (sizes == NULL) {
    kno_unlock_stream(s);
    u8_big_free(buckets);
    u8_seterr(kno_MallocFailed,"kindex_fetchinfo",NULL);
    return NULL;}
  /* If we have chunk offsets in memory, we don't need to keep the
     stream locked while we get them. */
  if (offdata) {
    kno_unlock_stream(s);
    while (i<n_buckets) {
      KNO_CHUNK_REF ref =
	kno_get_chunk_ref(offdata,offtype,i,kx->index_n_buckets);
      if (ref.size>0) {
	if (n_to_fetch >= buckets_len) {
	  u8_logf(LOG_WARN,"BadKeyCount",
		  "Bad key count in %s: %_d",ix->indexid,total_keys);
	  buckets=u8_big_realloc_n(buckets,n_buckets,KNO_CHUNK_REF);
	  buckets_len=n_buckets;}
	buckets[n_to_fetch++]=ref;}
      i++;}}
  else {
    while (i<n_buckets) {
      KNO_CHUNK_REF ref = kno_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) {
	if (n_to_fetch >= buckets_len) {
	  u8_logf(LOG_WARN,"BadKeyCount",
		  "Bad key count in %s: %_d",ix->indexid,total_keys);
	  buckets=u8_big_realloc_n(buckets,n_buckets,KNO_CHUNK_REF);
	  buckets_len=n_buckets;}
	buckets[n_to_fetch++]=ref;}
      i++;}
    kno_unlock_stream(s);}
  if (n_to_fetch > total_keys) {
    /* If total_keys is wrong, resize results to n_to_fetch */
    sizes = u8_big_realloc_n(sizes,n_to_fetch,struct KNO_KEY_SIZE);
    sizes_len = n_to_fetch;}
  qsort(buckets,n_to_fetch,sizeof(KNO_CHUNK_REF),sort_blockrefs_by_off);
  unsigned char keyblock_buf[KX_KEYBUF_SIZE];
  struct KNO_INBUF keyblock={0};
  KNO_INIT_INBUF(&keyblock,keyblock_buf,KX_KEYBUF_SIZE,0);
  i = 0; while (i<n_to_fetch) {
    int j = 0, n_keys;
    if (!kno_open_block(s,&keyblock,buckets[i].off,buckets[i].size,1)) {
      kno_unlock_stream(s);
      u8_big_free(buckets);
      free_keysizes(sizes,key_count);
      u8_big_free(sizes);
      kno_close_inbuf(&keyblock);
      *n=-1;
      return NULL;}
    n_keys = kno_read_varint(&keyblock);
    while (j<n_keys) {
      lispval key; int n_vals;
      ssize_t xtype_len = kno_read_varint(&keyblock);
      if (xtype_len<0) {
	kno_seterr(kno_UnexpectedEOD,"kindex_fetchinfo",
		   kx->indexid,KNO_VOID);
	key=KNO_ERROR_VALUE;}
      else {
	const unsigned char *key_end = keyblock.bufread+xtype_len;
	key = read_key(&keyblock,&(kx->index_xrefs));
	if (KNO_ABORTP(key)) {keyblock.bufread=key_end;}}
      if ( (n_vals = kno_read_varint(&keyblock)) < 0) {
	kno_seterr(kno_UnexpectedEOD,"kindex_fetchinfo",
		   kx->indexid,KNO_VOID);
	key=KNO_ERROR_VALUE;}
      if ( (!(KNO_TROUBLEP(key))) &&
	   ( (filter == NULL) || (fast_choice_containsp(key,filter)) ) ) {
	if (key_count >= sizes_len) {
	  sizes=u8_big_realloc(sizes,2*sizes_len*sizeof(struct KNO_KEY_SIZE));
	  sizes_len = sizes_len * 2;}
	if (sizes == NULL) {
	  kno_unlock_stream(s);
	  u8_big_free(buckets);
	  free_keysizes(sizes,key_count);
	  u8_big_free(sizes);
	  kno_close_inbuf(&keyblock);
	  *n=-1;
	  u8_seterr(u8_MallocFailed,"kindex_fetchinfo",kx->indexid);
	  return NULL;}
	sizes[key_count].keysize_key = key;
	sizes[key_count].keysize_count = n_vals;
	key_count++;}
      if (n_vals<0) {}
      else if (n_vals==0) {}
      else if (n_vals==1) {
	if (skip_xtype(&keyblock,&(kx->index_xrefs))<0)
	  key = KNO_ERROR;
	else NO_ELSE;}
      else {
	kno_read_varint(&keyblock);
	kno_read_varint(&keyblock);}
      if (KNO_ABORTP(key)) {
	if ( kx->index_flags & KNO_STORAGE_REPAIR ) {
	  kno_clear_errors(0);
	  u8_log(LOG_CRIT,"CorruptedKIndex",
		 "Error reading key @%ll_d+%ll_d in %s",
		 buckets[i].off,buckets[i].size,kx->index_source);
	  j=n_keys;}
	else {
	  lispval off_pair = kno_init_pair(NULL,KNO_INT(buckets[i].off),
					  KNO_INT(buckets[i].size));
	  kno_seterr("CorruptedKIndex","kindex_fetchinfo",
		    kx->indexid,off_pair);
	  kno_decref(off_pair);
	  kno_close_inbuf(&keyblock);
	  u8_big_free(buckets);
	  free_keysizes(sizes,key_count);
	  u8_big_free(sizes);
	  *n = -1;
	  return NULL;}}
      else j++;}
    i++;}
  kno_close_inbuf(&keyblock);
  u8_big_free(buckets);
  *n = key_count;
  return sizes;
}

static void kindex_getstats(struct KNO_KINDEX *kx,
			       int *nf,int *max,int *singles,int *n2sum)
{
  kno_stream s = &(kx->index_stream);
  kno_inbuf ins = kno_readbuf(s);
  int i = 0, n_buckets = (kx->index_n_buckets);
  int n_to_fetch = 0, total_keys = 0, buckets_len;
  unsigned int *offdata = kx->index_offdata;
  kno_offset_type offtype = kx->index_offtype;
  int max_keyblock_size=0;
  KNO_CHUNK_REF *buckets;
  kno_lock_index(kx);
  kno_lock_stream(s);
  kno_setpos(s,16); total_keys = kno_read_4bytes(ins);
  if (total_keys==0) {
    kno_unlock_stream(s);
    *nf = 0; *max = 0; *singles = 0; *n2sum = 0;
    return;}
  buckets = u8_big_alloc_n(total_keys,KNO_CHUNK_REF);
  /* If we don't have chunk offsets in memory, we keep the stream
     locked while we get them. */
  if (offdata) {
    kno_unlock_stream(s);
    while (i<n_buckets) {
      KNO_CHUNK_REF ref =
	kno_get_chunk_ref(offdata,offtype,i,kx->index_n_buckets);
      if (ref.size>0) {
	if (n_to_fetch >= buckets_len) {
	  u8_logf(LOG_WARN,"BadKeyCount",
		  "Bad key count in %s: %_d",kx->indexid,total_keys);
	  buckets=u8_realloc_n(buckets,n_buckets,KNO_CHUNK_REF);
	  buckets_len=n_buckets;}
	buckets[n_to_fetch++]=ref;}
      if (ref.size>max_keyblock_size)
	max_keyblock_size=ref.size;
      i++;}}
  else {
    while (i<n_buckets) {
      KNO_CHUNK_REF ref = kno_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) buckets[n_to_fetch++]=ref;
      if (ref.size>max_keyblock_size) max_keyblock_size=ref.size;
      i++;}
    kno_unlock_stream(s);}
  *nf = n_to_fetch;
  /* Now we actually unlock it if we kept it locked. */
  kno_unlock_index(kx);
  qsort(buckets,n_to_fetch,sizeof(KNO_CHUNK_REF),sort_blockrefs_by_off);
  struct KNO_INBUF keyblkstrm={0};
  int n_keys;
  i = 0; while (i<n_to_fetch) {
    kno_open_block(s,&keyblkstrm,buckets[i].off,buckets[i].size,1);
    n_keys = kno_read_varint(&keyblkstrm);
    if (n_keys==1) (*singles)++;
    if (n_keys>(*max)) *max = n_keys;
    *n2sum = *n2sum+(n_keys*n_keys);
    i++;}
  kno_close_inbuf(&keyblkstrm);
  if (buckets) u8_big_free(buckets);
}


/* Cache setting */

static void kindex_setcache(struct KNO_KINDEX *kx,int level)
{
  int chunk_ref_size = get_chunk_ref_size(kx);
  if (chunk_ref_size<0) {
    u8_logf(LOG_WARN,kno_CorruptedIndex,
	    "Index structure invalid: %s",kx->indexid);
    return;}
  kno_stream stream = &(kx->index_stream);
  size_t cur_bufsize  = kno_stream_bufsize(stream);
  size_t use_bufsize = kno_get_bufsize(kx->index_opts,kno_driver_bufsize,1);

  /* Update the bufsize */
  if (use_bufsize > cur_bufsize)
    kno_setbufsize(stream,use_bufsize);

  if (level >= 2) {
    if (kx->index_offdata)
      return;
    else {
      kno_stream s = &(kx->index_stream);
      size_t n_buckets = kx->index_n_buckets;
      unsigned int *buckets, *newmmap;
#if KNO_USE_MMAP
      newmmap=
	mmap(NULL,(n_buckets*chunk_ref_size)+256,
	     PROT_READ,MMAP_FLAGS,s->stream_fileno,0);
      if ((newmmap == NULL) || (newmmap == MAP_FAILED)) {
	u8_logf(LOG_WARN,u8_strerror(errno),
		"kindex_setcache:mmap %s",kx->index_source);
	kx->index_offdata = NULL;
	errno = 0;}
      else kx->index_offdata = buckets = newmmap+64;
#else
      ht_buckets = u8_big_alloc_n
	(chunk_ref_size*(kx->index_n_buckets),unsigned int);
      kno_lock_stream(s);
      stream_start_read(s);
      kno_setpos(s,256);
      retval = kno_read_ints
	(s,(chunk_ref_size/4)*(kx->index_n_buckets),ht_buckets);
      if (retval<0) {
	u8_logf(LOG_WARN,u8_strerror(errno),
		"kindex_setcache:read offsets %s",kx->index_source);
	errno = 0;}
      else kx->index_offdata = ht_buckets;
      kno_unlock_stream(s);
#endif
    }}

  else if (level < 2) {

    int retval=0;
    unsigned int *offdata = kx->index_offdata;
    if (offdata == NULL)
      return;
    else kx->index_offdata=NULL;
    /* TODO: We should be more careful before unmapping or freeing
       this, since somebody could still have a pointer to it. */
#if KNO_USE_MMAP
    retval = munmap(offdata-64,((kx->index_n_buckets)*chunk_ref_size)+256);
    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),
	      "kindex_setcache:munmap %s",kx->index_source);
      errno = 0;}
#else
    u8_big_free(offdata);
#endif
  }
  else {}
}


/* Populating a hash index
   This writes data into the hashtable but ignores what is already there.
   It is commonly used when initializing a hash index. */

static int sort_br_by_bucket(const void *p1,const void *p2)
{
  struct BUCKET_REF *ps1 = (struct BUCKET_REF *)p1;
  struct BUCKET_REF *ps2 = (struct BUCKET_REF *)p2;
  if (ps1->bucketno<ps2->bucketno) return -1;
  else if (ps1->bucketno>ps2->bucketno) return 1;
  else return 0;
}

static int sort_br_by_off(const void *p1,const void *p2)
{
  struct BUCKET_REF *ps1 = (struct BUCKET_REF *)p1;
  struct BUCKET_REF *ps2 = (struct BUCKET_REF *)p2;
  if (ps1->bck_ref.off<ps2->bck_ref.off) return -1;
  else if (ps1->bck_ref.off>ps2->bck_ref.off) return 1;
  else return 0;
}


/* COMMIT */

/* General design:

   We start by swapping the current adds and edits into into separate
   hashtables to allow other threads to write new adds and edits to
   the index. We then process these tables in order to populate a
   COMMIT_SCHEDULE for each key we're going to change.

   Handle edits first with 'process_edits', converting drops into
   stores by fetching the current values and adding them as stores to
   the commit schedule. (Drops are supposed to be rare).

   Pointers from the edits table are incref'd before being saved to
   the commit schedule. Also, adds are integrated into the commits
   from the edits table.

   Then handle adds with 'process_adds', pushing them onto the commit
   schedule, adding them to the current values in the schedule if they
   weren't integrated into results from the edits table.

   Pointers from the adds table **are not** incref'd but the keyvalues
   in the adds table are VOIDed to avoid double pointers.

   After this, the adds and edits can be reset, to zero because we're
   not going to use them again.

   We then figure out which buckets each of the keys goes in and sort
   the commit schedule by bucket. For each bucket, we then get the
   location of the bucket data on disk (either from offdata or by
   going to disk). This is stored in an BUCKET_REF array, which
   we sort by file position and read the buckets we're changing in
   order. Each bucket is read from disk and stored into an array of
   KEYBUCKET structs.

   A keybucket structure consists of an array of KEYENTRY structures
   which contain information about each key in the bucket. It also
   contains a buffer with the XTYPE representations of all of the
   keys, to avoid regenerating them.

   There will often be fewer buckets than keys and so the
   COMMIT_SCHEDULE array may be longer than the KEYBUCKET
   array. However, the number of KEYENTRY structures in a bucket will
   often be greater than the number of corresponding keys in the
   commit schedule because it includes entries for keys in the bucket
   which aren't being changed.

   We then sort both the commit schedule and the keybuckets by bucket
   number. This allows us to march along them in parallel and operate
   on a keybucket together with all its changing keys.

   We extend each keybucket (using 'update_keybucket') with the
   additional keys or new values. This is where the actual values are
   written and the corresponding chunk locations stored in the
   KEYENTRY structs for each key. When a key has only a single value,
   it is stored in the keyblock itself. Otherwise, a separate value
   block is written which includes a reference to the value block from
   the keyblock read from disk.

   After the values are all written, we write the keybucket itself to
   disk. The new location of the keybucket is saved in the same
   BUCKET_REF array used to fetch the keybuckets in the first
   place.

   After the keybuckets (and their values) have been written to disk,
   we may (if kno_acid_files is not zero) write recovery data in the
   form of the changed buckets from the BUCKET_REF array. Once
   this is written to disk, we write the position where the recovery
   information started to the end of the file and set the initial word
   of the file to the code KNO_KINDEX_TO_RECOVER.

   After the recovery data is written, we write the new offset data
   and change the initial word. Then we truncate the file to remove
   the recovery data. And that's it.
*/

static int sort_cs_by_bucket(const void *k1,const void *k2)
{
  struct COMMIT_SCHEDULE *ks1 = (struct COMMIT_SCHEDULE *)k1;
  struct COMMIT_SCHEDULE *ks2 = (struct COMMIT_SCHEDULE *)k2;
  if (ks1->commit_bucket<ks2->commit_bucket) return -1;
  else if (ks1->commit_bucket>ks2->commit_bucket) return 1;
  else return 0;
}

static int sort_kb_by_bucket(const void *k1,const void *k2)
{
  struct KEYBUCKET **kb1 = (struct KEYBUCKET **)k1;
  struct KEYBUCKET **kb2 = (struct KEYBUCKET **)k2;
  if ((*kb1)->kb_bucketno<(*kb2)->kb_bucketno) return -1;
  else if ((*kb1)->kb_bucketno>(*kb2)->kb_bucketno) return 1;
  else return 0;
}

static int process_stores(struct KNO_KINDEX *kx,
			  struct KNO_CONST_KEYVAL *stores,int n_stores,
			  struct COMMIT_SCHEDULE *s,
			  int i)
{
  int oddkeys = ((kx->kindex_format)&(KNO_KINDEX_ODDKEYS));
  int store_i = 0; while ( store_i < n_stores) {
    lispval key = stores[store_i].kv_key, val = stores[store_i].kv_val;
    s[i].commit_key	= key;
    s[i].commit_values	= val;
    s[i].free_values	= 0;
    s[i].commit_replace = 1;
    store_i++;
    i++;}

  /* Record if there were any odd keys */
  if (oddkeys) kx->kindex_format |= (KNO_KINDEX_ODDKEYS);

  return i;
}

static int process_drops(struct KNO_KINDEX *kx,
			 struct KNO_CONST_KEYVAL *drops,int n_drops,
			 struct COMMIT_SCHEDULE *s,
			 int sched_i)
{
  lispval *to_fetch = u8_big_alloc_n(n_drops,lispval); int n_fetches = 0;
  int *fetch_scheds = u8_big_alloc_n(n_drops,unsigned int);
  int oddkeys = ((kx->kindex_format)&(KNO_KINDEX_ODDKEYS));

  /* For all of the drops, we need to fetch their values to do the
     drop, so we accumulate them in to_fetch[]. We're not checking the
     cache for those values (which could be there), because the cache
     might have additional changes made before the commit. (Got
     that?) */
  int drop_i = 0; while ( drop_i < n_drops) {
    lispval key = drops[drop_i].kv_key, val = drops[drop_i].kv_val;

    s[sched_i].commit_key = key;
    /* We temporarily store the dropped values in the
       schedule. We'll replace them with the call to kno_difference
       below. */
    s[sched_i].commit_values = val;
    s[sched_i].free_values   = 0;
    to_fetch[n_fetches]	     = key;
    fetch_scheds[n_fetches++]  = sched_i;
    s[sched_i].commit_replace = 1;
    drop_i++;
    sched_i++;}

  /* Record if there were any odd (non (slotid . val)) keys */
  if (oddkeys) kx->kindex_format |= (KNO_KINDEX_ODDKEYS);

  /* Get the current values of all the keys you're dropping, to turn
     the drops into stores. */
  lispval *drop_vals = fetchn(kx,n_fetches,to_fetch);

  int j = 0; while (j<n_fetches) {
    int sched_ref   = fetch_scheds[j];
    lispval ondisk  = drop_vals[j];
    lispval todrop  = s[sched_ref].commit_values;
    lispval reduced = kno_difference(ondisk,todrop);
    s[sched_ref].commit_values = reduced;
    s[sched_ref].free_values   = 1;
    j++;}

  kno_decref_elts(drop_vals,n_fetches);
  u8_big_free(drop_vals);
  u8_big_free(fetch_scheds);
  u8_big_free(to_fetch);

  return sched_i;
}

static int process_adds(struct KNO_KINDEX *kx,
			struct KNO_CONST_KEYVAL *adds,int n_adds,
			struct COMMIT_SCHEDULE *s,
			int i)
{
  int add_i = 0;
  int oddkeys = ((kx->kindex_format)&(KNO_KINDEX_ODDKEYS));
  while ( add_i < n_adds ) {
    lispval key = adds[add_i].kv_key;
    lispval val = adds[add_i].kv_val;
    if (KNO_EMPTYP(val)) {
      add_i++;
      continue;}
    else s[i].commit_key = key;
    if (PRECHOICEP(val)) {
      s[i].commit_values  = kno_make_simple_choice(val);
      s[i].free_values	  = 1;}
    else {
      s[i].commit_values  = val;
      s[i].free_values	  = 0;}
    s[i].commit_replace = 0;
    add_i++;
    i++;}
  if (oddkeys)
    kx->kindex_format |= (KNO_KINDEX_ODDKEYS);
  return i;
}

KNO_FASTOP void parse_keybucket(kno_kindex kx,struct KEYBUCKET *kb,
			       kno_inbuf in,int n_keys)
{
  int i = 0; struct KEYENTRY *base_entry = &(kb->kb_elt0);
  xtype_refs refs = &(kx->index_xrefs);
  kb->kb_n_keys = n_keys;
  while (i<n_keys) {
    int xt_size = kno_read_varint(in), n_values;
    struct KEYENTRY *entry = base_entry+i;
    entry->ke_xtrep_size = xt_size;
    entry->ke_xtstart	 = in->bufread;
    in->bufread		 = in->bufread+xt_size;
    entry->ke_nvals	 = n_values = kno_read_varint(in);
    if (n_values==0)
      entry->ke_values = EMPTY;
    else if (n_values==1) {
      lispval v = read_value(in,refs);
      entry->ke_values = v;
      if (KNO_EMPTYP(v)) entry->ke_nvals = 0;}
    else {
      entry->ke_values	  = VOID;
      entry->ke_vref.off  = kno_read_varint(in);
      entry->ke_vref.size = kno_read_varint(in);}
    i++;}
}

KNO_FASTOP KNO_CHUNK_REF write_value_block
(struct KNO_KINDEX *kx,kno_stream stream,xtype_refs refs,
 lispval values,lispval extra,
 kno_off_t cont_off,kno_off_t cont_size,
 kno_off_t startpos)
{
  struct KNO_OUTBUF *outstream = kno_writebuf(stream);
  KNO_CHUNK_REF retval; kno_off_t endpos = startpos;
  if (CHOICEP(values)) {
    int full_size = KNO_CHOICE_SIZE(values)+((VOIDP(extra))?0:1);
    endpos = endpos+kno_write_varint(outstream,full_size);
    if (!(VOIDP(extra)))
      endpos = endpos+write_value(outstream,extra,refs);
    {DO_CHOICES(value,values)
	endpos = endpos+write_value(outstream,value,refs);}}
  else if (VOIDP(extra)) {
    endpos = endpos+kno_write_varint(outstream,1);
    endpos = endpos+write_value(outstream,values,refs);}
  else {
    endpos = endpos+kno_write_varint(outstream,2);
    endpos = endpos+write_value(outstream,extra,refs);
    endpos = endpos+write_value(outstream,values,refs);}
  endpos = endpos+kno_write_varint(outstream,cont_size);
  if (cont_size)
    endpos = endpos+kno_write_varint(outstream,cont_off);
  CHECK_POS(endpos,stream);
  retval.off  = startpos;
  retval.size = endpos-startpos;
  return retval;
}

/* This adds new entries to a keybucket, writing value blocks to the
   file where neccessary (more than one value). */
KNO_FASTOP kno_off_t update_keybucket
(kno_kindex kx,kno_stream stream,xtype_refs refs,
 struct KEYBUCKET *kb, /* The keybucket representation */
 /* The schedule for writing to the index file */
 struct COMMIT_SCHEDULE *schedule,
 /* [i,j] is the the range of schedule entries to go into the bucket */
 int i,int j,
 kno_outbuf newkeys,int *new_valueblocksp,
 kno_off_t endpos,ssize_t maxpos)
{
  int k = i, free_keyvecs = 0;
  int _keyoffs[16], _keysizes[16], *keyoffs, *keysizes;
  if (RARELY((j-i)>16) )  {
    keyoffs = u8_alloc_n((j-i),int);
    keysizes = u8_alloc_n((j-i),int);
    free_keyvecs = 1;}
  else {keyoffs=_keyoffs; keysizes=_keysizes;}
  while (k<j) {
    int off = keyoffs[k-i] = newkeys->bufwrite-newkeys->buffer;
    write_key(newkeys,schedule[k].commit_key,refs);
    keysizes[k-i]=(newkeys->bufwrite-newkeys->buffer)-off;
    k++;}
  k = i; while (k<j) {
    unsigned char *keydata = newkeys->buffer+keyoffs[k-i];
    struct KEYENTRY *ke = &(kb->kb_elt0);
    int keysize = keysizes[k-i];
    int key_i = 0, n_keys = kb->kb_n_keys, n_values;
    while (key_i<n_keys) {
      if ((ke[key_i].ke_xtrep_size)!= keysize) key_i++;
      else if (memcmp(keydata,ke[key_i].ke_xtstart,keysize)) key_i++;
      else if (schedule[k].commit_replace) {
	/* The key is already in there, but we are ignoring
	   it's current value.	If key has more than one associated
	   values, we write a value block, otherwise we store the value
	   in the key entry. */
	int n_values = KNO_CHOICE_SIZE(schedule[k].commit_values);
	ke[key_i].ke_nvals = n_values;
	if (n_values==0) {
	  ke[key_i].ke_values = EMPTY;
	  ke[key_i].ke_vref.off = 0;
	  ke[key_i].ke_vref.size = 0;}
	else if (n_values==1) {
	  /* If there is only one value we're saving, so we'll write
	     it as part of the keyblock (that's what being in
	     .ke_values means) */
	  lispval current = ke[key_i].ke_values;
	  ke[key_i].ke_values = kno_incref(schedule[k].commit_values);
	  ke[key_i].ke_vref.off = 0;
	  ke[key_i].ke_vref.size = 0;
	  kno_decref(current);}
	else {
	  ke[key_i].ke_values = VOID;
	  ke[key_i].ke_vref=
	    write_value_block(kx,stream,refs,
			      schedule[k].commit_values,VOID,
			      0,0,endpos);
	  (*new_valueblocksp)++;
	  endpos = ke[key_i].ke_vref.off+ke[key_i].ke_vref.size;}
	if (endpos>=maxpos) {
	  if (free_keyvecs) {
	    u8_free(keyoffs);
	    u8_free(keysizes);
	    u8_seterr(kno_DataFileOverflow,"update_keybucket",
		      u8_mkstring("%s: %ll_d >= %ll_d",
				  kx->indexid,endpos,maxpos));
	    return -1;}}
	break;}
      else {
	/* The key is already in there and has values, so we write
	   a value block with a continuation pointer to the current
	   value block and update the key entry.  */
	int n_values = ke[key_i].ke_nvals;
	int n_values_added = KNO_CHOICE_SIZE(schedule[k].commit_values);
	ke[key_i].ke_nvals = n_values+n_values_added;
	if ( (n_values==0) && (n_values_added == 1) ) {
	  /* This is the case where there was an entry with no values
	     and we're adding one value */
	  lispval one_val = schedule[k].commit_values;
	  ke[key_i].ke_values=one_val;
	  kno_incref(one_val);}
	else if (n_values==1) {
	  /* This is the special case is where there is one current
	     value and we are adding to that.  The value block we
	     write must contain both the values we are adding and the
	     current singleton value.  The current singleton value is
	     passed as the fourth (extra) argument to
	     write_value_block.	 */
	  lispval current = ke[key_i].ke_values;
	  ke[key_i].ke_vref=
	    write_value_block(kx,stream,refs,
			      schedule[k].commit_values,
			      current,0,0,endpos);
	  endpos = ke[key_i].ke_vref.off+ke[key_i].ke_vref.size;
	  /* We void this because it's now on disk */
	  kno_decref(current);
	  ke[key_i].ke_values = VOID;}
	else {
	  ke[key_i].ke_vref=
	    write_value_block(kx,stream,refs,
			      schedule[k].commit_values,VOID,
			      ke[key_i].ke_vref.off,ke[key_i].ke_vref.size,
			      endpos);
	  if ( (ke[key_i].ke_values != VOID) &&
	       (ke[key_i].ke_values != EMPTY) )
	    u8_logf(LOG_WARN,"NotVoid",
		    "This value for key %_d is %q, not VOID/EMPTY as expected",
		    key_i,ke[key_i].ke_values);
	  endpos = ke[key_i].ke_vref.off+ke[key_i].ke_vref.size;}
	if (endpos>=maxpos) {
	  if (free_keyvecs) {
	    u8_free(keyoffs);
	    u8_free(keysizes);}
	  u8_seterr(kno_DataFileOverflow,"update_keybucket",
		    u8_mkstring("%s: %ll_d >= %ll_d",
				kx->indexid,endpos,maxpos));
	  return -1;}
	break;}}
    /* This is the case where we are adding a new key to the bucket. */
    if (key_i == n_keys) { /* This should always be true */
      ke[n_keys].ke_xtrep_size = keysize;
      ke[n_keys].ke_nvals = n_values =
	KNO_CHOICE_SIZE(schedule[k].commit_values);
      ke[n_keys].ke_xtstart = newkeys->buffer+keyoffs[k-i];
      if (n_values==0) ke[n_keys].ke_values = EMPTY;
      else if (n_values==1)
	/* As above, we don't need to incref this because any value in
	   it comes from the key schedule, so we won't decref it when
	   we reclaim the keybuckets. */
	ke[n_keys].ke_values = kno_incref(schedule[k].commit_values);
      else {
	ke[n_keys].ke_values = VOID;
	ke[n_keys].ke_vref=
	  write_value_block(kx,stream,refs,
			    schedule[k].commit_values,VOID,
			    0,0,endpos);
	endpos = ke[key_i].ke_vref.off+ke[key_i].ke_vref.size;
	if (endpos>=maxpos) {
	  if (free_keyvecs) {
	    u8_free(keyoffs);
	    u8_free(keysizes);}
	  u8_seterr(kno_DataFileOverflow,"update_keybucket",
		    u8_mkstring("%s: %ll_d >= %ll_d",
				kx->indexid,endpos,maxpos));
	  return -1;}}
      kb->kb_n_keys++;}
    k++;}
  if (free_keyvecs) {
    u8_free(keyoffs);
    u8_free(keysizes);}
  return endpos;
}

KNO_FASTOP kno_off_t write_keybucket
(kno_kindex kx,kno_stream stream,xtype_refs refs,
 struct KEYBUCKET *kb,
 kno_off_t endpos,kno_off_t maxpos)
{
  int i = 0, n_keys = kb->kb_n_keys;
  struct KEYENTRY *ke = &(kb->kb_elt0);
  struct KNO_OUTBUF *outstream = kno_writebuf(stream);
  endpos = endpos+kno_write_varint(outstream,n_keys);
  while (i<n_keys) {
    int xtype_size = ke[i].ke_xtrep_size, n_values = ke[i].ke_nvals;
    endpos = endpos+kno_write_varint(outstream,xtype_size);
    endpos = endpos+kno_write_bytes(outstream,ke[i].ke_xtstart,xtype_size);
    endpos = endpos+kno_write_varint(outstream,n_values);
    if (n_values==0) {}
    else if (n_values==1) {
      endpos = endpos+write_value(outstream,ke[i].ke_values,refs);
      kno_decref(ke[i].ke_values);
      ke[i].ke_values = VOID;}
    else {
      endpos = endpos+kno_write_varint(outstream,ke[i].ke_vref.off);
      endpos = endpos+kno_write_varint(outstream,ke[i].ke_vref.size);}
    i++;}
  if (endpos>=maxpos) {
    u8_seterr(kno_DataFileOverflow,"write_keybucket",
	      u8_mkstring("%s: %ll_d >= %ll_d",kx->indexid,endpos,maxpos));
    return -1;}
  return endpos;
}

KNO_FASTOP struct KEYBUCKET *read_keybucket
(kno_kindex kx,kno_stream stream,
 int bucket,KNO_CHUNK_REF ref,int extra)
{
  int n_keys;
  struct KEYBUCKET *kb;
  /* We allocate this dynamically because we're going to store it on
     the keybucket. */
  if (ref.size>0) {
    unsigned char *keybuf=u8_malloc(ref.size);
    ssize_t read_result=kno_read_block(stream,keybuf,ref.size,ref.off,1);
    if (read_result<0) {
      kno_seterr("FailedBucketRead","read_keybucket/kindex",
		kx->indexid,KNO_INT(bucket));
      return NULL;}
    else {
      struct KNO_INBUF keystream = { 0 };
      KNO_INIT_INBUF(&keystream,keybuf,ref.size,0);
      n_keys = kno_read_varint(&keystream);
      kb = (struct KEYBUCKET *)
	u8_malloc(sizeof(struct KEYBUCKET)+
		  sizeof(struct KEYENTRY)*((extra+n_keys)-1));
      kb->kb_bucketno = bucket;
      kb->kb_n_keys   = n_keys;
      kb->kb_keybuf   = keybuf;
      parse_keybucket(kx,kb,&keystream,n_keys);}}
  else {
    kb = (struct KEYBUCKET *)
      u8_malloc(sizeof(struct KEYBUCKET)+
		sizeof(struct KEYENTRY)*(extra-1));
    kb->kb_bucketno = bucket;
    kb->kb_n_keys   = 0;
    kb->kb_keybuf   = NULL;}
  return kb;
}

static int update_kindex_ondisk
(kno_kindex kx,lispval metadata,
 unsigned int flags,unsigned int new_keys,
 unsigned int changed_buckets,
 struct BUCKET_REF *bucket_locs,
 struct XTYPE_REFS *refs,
 struct KNO_STREAM *stream,
 struct KNO_STREAM *head);
static int update_kindex_metadata(kno_kindex,lispval,kno_stream,kno_stream);

static void free_keybuckets(int n,struct KEYBUCKET **keybuckets);

static int kindex_save(struct KNO_KINDEX *kx,
			 struct KNO_INDEX_COMMITS *commits,
			 struct KNO_STREAM *stream,
			 struct KNO_STREAM *head)
{
  struct KNO_CONST_KEYVAL *adds = commits->commit_adds;
  int n_adds = commits->commit_n_adds;
  struct KNO_CONST_KEYVAL *drops = commits->commit_drops;
  int n_drops = commits->commit_n_drops;
  struct KNO_CONST_KEYVAL *stores = commits->commit_stores;
  int n_stores = commits->commit_n_stores;
  lispval changed_metadata = commits->commit_metadata;
  xtype_refs refs = &(kx->index_xrefs);
  kno_lock_index(kx);
  struct BUCKET_REF *bucket_locs;
  kno_offset_type offtype = kx->index_offtype;
  if (!((offtype == KNO_B32)||(offtype = KNO_B40)||(offtype = KNO_B64))) {
    u8_logf(LOG_WARN,CorruptedKIndex,
	    "Bad offset type code=%d for %s",(int)offtype,kx->indexid);
    u8_seterr(CorruptedKIndex,"kindex_save/offtype",
	      u8_strdup(kx->indexid));
    kno_unlock_index(kx);
    return -1;}

  if (	(n_adds==0) && (n_drops==0) && (n_stores==0) ) {
    if (KNO_SLOTMAPP(changed_metadata))
      update_kindex_metadata(kx,changed_metadata,stream,head);
    kno_unlock_index(kx);
    return 0;}

  int new_keys = 0, n_keys, new_buckets = 0;
  int schedule_max, changed_buckets = 0, total_keys = kx->table_n_keys;
  int new_keyblocks = 0, new_valueblocks = 0;
  ssize_t endpos, maxpos = get_maxpos(kx);
  double started = u8_elapsed_time();
  size_t n_buckets = kx->index_n_buckets;
  unsigned int *offdata = kx->index_offdata;
  schedule_max = n_adds + n_drops + n_stores;
  bucket_locs = u8_big_alloc_n(schedule_max,struct BUCKET_REF);

  /* This is where we write everything to disk */
  int sched_i = 0, bucket_i = 0;
  int schedule_size = 0;
  struct COMMIT_SCHEDULE *schedule=
    u8_big_alloc_n(schedule_max,struct COMMIT_SCHEDULE);

  /* We're going to write XTYPE representations of keys and values,
     so we create streams where they'll all be written to one big buffer
     (which we free at the end). */
  struct KNO_OUTBUF out = { 0 }, newkeys = { 0 };
  KNO_INIT_BYTE_OUTPUT(&out,1024);
  KNO_INIT_BYTE_OUTPUT(&newkeys,schedule_max*16);
  out.buf_flags |= KNO_WRITE_OPAQUE;
  newkeys.buf_flags |= KNO_WRITE_OPAQUE;

  /* Get all the keys we need to write and put then in the commit
     schedule */
  schedule_size = process_stores(kx,stores,n_stores,schedule,schedule_size);
  schedule_size = process_drops(kx,drops,n_drops,schedule,schedule_size);
  schedule_size = process_adds(kx,adds,n_adds,schedule,schedule_size);

  /* The commit schedule is now filled and we determine the bucket for
     each key. */
  sched_i = 0; while (sched_i<schedule_size) {
    lispval key = schedule[sched_i].commit_key; int bucket;
    if (KNO_PAIRP(key)) {
      lispval slotid = KNO_CAR(key);
      if ( (OIDP(slotid)) || (SYMBOLP(slotid)) ) {
	ssize_t code = kno_xtype_ref(slotid,refs,1);
	if (code<0)
	  u8_log(LOGWARN,"XRefError","Couldn't get an xref code for %q",slotid);}}
    out.bufwrite = out.buffer;
    write_key(&out,key,refs);
    schedule[sched_i].commit_bucket = bucket =
      hash_bytes(out.buffer,out.bufwrite-out.buffer)%n_buckets;
    sched_i++;}

  /* Get all the bucket locations, sorting the schedule to avoid
     multiple lookups */
  qsort(schedule,schedule_size,sizeof(struct COMMIT_SCHEDULE),
	sort_cs_by_bucket);
  sched_i = 0; bucket_i = 0; while (sched_i<schedule_size) {
    int bucket = schedule[sched_i].commit_bucket;
    int bucket_last_key = sched_i;
    bucket_locs[changed_buckets].bucketno = bucket;
    bucket_locs[changed_buckets].bck_ref = (offdata) ? /* location on disk */
      (kno_get_chunk_ref(offdata,offtype,bucket,kx->index_n_buckets)):
      (kno_fetch_chunk_ref(stream,256,offtype,bucket,1));
    int keys_in_bucket = 1;
    /* Scan over all committed keys in the same bucket */
    while ( (bucket_last_key<schedule_size) &&
	    (schedule[bucket_last_key].commit_bucket == bucket) ) {
      bucket_last_key++; keys_in_bucket++;}
    bucket_locs[changed_buckets].max_new = keys_in_bucket;
    sched_i = bucket_last_key;
    changed_buckets++;}

  /* Now we have all the bucket locations, which we'll read in
     order. */
  struct KEYBUCKET **keybuckets=
    u8_big_alloc_n(changed_buckets,struct KEYBUCKET *);
  qsort(bucket_locs,changed_buckets,sizeof(struct BUCKET_REF),
	sort_br_by_off);
  bucket_i = 0; while (bucket_i<changed_buckets) {
    keybuckets[bucket_i]=
      read_keybucket(kx,stream,bucket_locs[bucket_i].bucketno,
		     bucket_locs[bucket_i].bck_ref,
		     bucket_locs[bucket_i].max_new);
    if ((keybuckets[bucket_i]->kb_n_keys)==0) new_buckets++;
    bucket_i++;}

  /* Now all the keybuckets have been read and buckets have been
     created for keys that didn't have buckets before. */
  /* schedule is already sorted by bucket ? */
#if 0
  qsort(schedule,schedule_size,sizeof(struct COMMIT_SCHEDULE),
	sort_cs_by_bucket);
#endif
  qsort(keybuckets,changed_buckets,sizeof(struct KEYBUCKET *),
	sort_kb_by_bucket);
  /* bucket_locs is currently sorted by offset, so we resort it by
     bucket because we're going to iterate by bucket and write the new
     bucket locations into it. */
  qsort(bucket_locs,changed_buckets,sizeof(struct BUCKET_REF),
	sort_br_by_bucket);

  /* March along the commit schedule (keys) and keybuckets (buckets)
     in parallel, updating each bucket.	 This is where values are
     written out and their new offsets stored in the schedule. */
  sched_i = 0; bucket_i = 0; endpos = kno_endpos(stream);
  while (sched_i<schedule_size) {
    struct KEYBUCKET *kb = keybuckets[bucket_i];
    int bucket = schedule[sched_i].commit_bucket;
    int j = sched_i, cur_keys = kb->kb_n_keys;
    if (KNO_RARELY(bucket != kb->kb_bucketno)) {
      u8_log(LOG_CRIT,"KIndexError",
	     "Bucket at sched_i=%_d/%_d was %_d != %_d (expected) in %s",
	     sched_i,schedule_size,bucket,kb->kb_bucketno,kx->indexid);}
    while ((j<schedule_size) && (schedule[j].commit_bucket == bucket)) j++;
    /* This may write values to disk, so we use the returned endpos */
    endpos = update_keybucket(kx,stream,refs,kb,schedule,sched_i,j,
			      &newkeys,&new_valueblocks,
			      endpos,maxpos);
    CHECK_POS(endpos,stream);
    new_keys = new_keys+(kb->kb_n_keys-cur_keys);
    {
      kno_off_t startpos = endpos;
      /* This writes the keybucket itself. */
      endpos = write_keybucket(kx,stream,refs,kb,endpos,maxpos);
      new_keyblocks++;
      if (endpos<0) {
	u8_big_free(bucket_locs);
	u8_big_free(schedule);
	u8_big_free(keybuckets);
	kno_unlock_index(kx);
	kno_close_stream(stream,KNO_STREAM_FREEDATA);
	u8_seterr("WriteKeyBucketFailed","kindex_commit",
		  u8_strdup(kx->indexid));
	return -1;}
      CHECK_POS(endpos,stream);
      bucket_locs[bucket_i].bck_ref.off = startpos;
      bucket_locs[bucket_i].bck_ref.size = endpos-startpos;}
    sched_i = j;
    /* Reset the newkeys stream */
    newkeys.bufwrite=newkeys.buffer;
    bucket_i++;}
  kno_flush_stream(stream);

  /* Free all the keybuckets */
  free_keybuckets(changed_buckets,keybuckets);

  /* Now we free the keys and values in the schedule. */
  sched_i = 0; while (sched_i<schedule_size) {
    if (schedule[sched_i].free_values) {
      lispval v = schedule[sched_i].commit_values;
      schedule[sched_i].commit_values = VOID;
      kno_decref(v);}
    sched_i++;}
  u8_big_free(schedule);
  kno_close_outbuf(&out);
  kno_close_outbuf(&newkeys);
  n_keys = schedule_size;

  total_keys += new_keys;
  kx->table_n_keys = total_keys;

  int final_rv = update_kindex_ondisk
    (kx,changed_metadata,
     kx->kindex_format,total_keys,
     changed_buckets,bucket_locs,
     refs,stream,head);

  /* Free the bucket locations */
  u8_big_free(bucket_locs);
  /* And unlock the index */
  kno_unlock_index(kx);

  if (final_rv < 0)
    u8_logf(LOG_ERR,"KIndexCommit",
	    "Saving header information failed");
  else u8_logf(LOG_INFO,"KIndexCommit",
	       "Saved mappings for %_d keys (%_d/%_d new/total) to %s in %f secs",
	       n_keys,new_keys,total_keys,
	       kx->indexid,u8_elapsed_time()-started);

  if (final_rv<0)
    return final_rv;
  else return n_keys;
}

static kno_stream get_commit_stream(kno_index ix,struct KNO_INDEX_COMMITS *commit)
{
  if (commit->commit_stream)
    return commit->commit_stream;
  else if (u8_file_writablep(ix->index_source)) {
    ssize_t bufsize = kno_get_bufsize(ix->index_opts,kno_driver_bufsize,1);
    struct KNO_STREAM *new_stream =
      kno_init_file_stream(NULL,ix->index_source,KNO_FILE_MODIFY,-1,bufsize);
    /* Lock the file descriptor */
    if (kno_streamctl(new_stream,kno_stream_lockfile,NULL)<0) {
      kno_close_stream(new_stream,KNO_STREAM_FREEDATA);
      u8_free(new_stream);
      kno_seterr("CantLockFile","get_commit_stream/kindex",
		ix->index_source,KNO_VOID);
      return NULL;}
    commit->commit_stream = new_stream;
    return new_stream;}
  else {
    kno_seterr("CantWriteFile","get_commit_stream/kindex",
	      ix->index_source,KNO_VOID);
    return NULL;}
}

static void release_commit_stream(kno_index ix,struct KNO_INDEX_COMMITS *commit)
{
  kno_stream stream = commit->commit_stream;
  if (stream == NULL) return;
  else commit->commit_stream=NULL;
  if (kno_streamctl(stream,kno_stream_unlockfile,NULL)<0)
    u8_logf(LOG_WARN,"CantUnLockFile",
	    "For commit stream of kindex %s",ix->indexid);
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  u8_free(stream);
}

static int kindex_commit(kno_index ix,kno_commit_phase phase,
			    struct KNO_INDEX_COMMITS *commits)
{
  struct KNO_KINDEX *kx = (kno_kindex) ix;
  int ref_size = get_chunk_ref_size(kx);
  size_t n_buckets = kx->index_n_buckets;
  kno_stream stream = get_commit_stream(ix,commits);
  u8_string source = ix->index_source;
  if (stream == NULL)
    return -1;
  switch (phase) {
  case kno_no_commit:
    u8_seterr("BadCommitPhase(commit_none)","kindex_commit",
	      u8_strdup(ix->indexid));
    return -1;
  case kno_commit_start:
    return kno_write_rollback("kindex_commit",ix->indexid,source,
			     256+(ref_size*n_buckets));
  case kno_commit_write: {
    struct KNO_STREAM *head_stream = NULL;
    if (commits->commit_2phase) {
      size_t recovery_size = 256+(ref_size*n_buckets);
      u8_string commit_file = u8_mkstring("%s.commit",source);
      ssize_t head_saved = kno_save_head(source,commit_file,recovery_size);
      head_stream = (head_saved>=0) ?
	(kno_init_file_stream(NULL,commit_file,KNO_FILE_MODIFY,-1,-1)) :
	(NULL);
      if (head_stream == NULL) {
	u8_seterr("CantOpenCommitFile","kindex_commit",commit_file);
	return -1;}
      else u8_free(commit_file);}
    else head_stream=stream;
    int rv = kindex_save((struct KNO_KINDEX *)ix,commits,stream,head_stream);
    if (rv<0) {
      u8_string commit_file = u8_mkstring("%s.commit",source);
      u8_seterr("KIndexSaveFailed","kindex_commit",u8_strdup(ix->indexid));
      if (u8_removefile(commit_file)<0)
	u8_log(LOG_CRIT,"RemoveFileFailed","Couldn't remove file %s for %s",
	       commit_file,ix->indexid);
      u8_free(commit_file);}
    else if (head_stream != stream) {
      size_t endpos = kno_endpos(stream);
      size_t head_endpos = kno_endpos(head_stream);
      kno_write_8bytes_at(head_stream,endpos,head_endpos);
      kno_flush_stream(head_stream);
      kno_close_stream(head_stream,KNO_STREAM_FREEDATA);
      u8_free(head_stream);
      head_stream=NULL;
      commits->commit_phase = kno_commit_sync;}
    else commits->commit_phase = kno_commit_flush;
    kno_flush_stream(&(kx->index_stream));
    return rv;}
  case kno_commit_rollback: {
    u8_string rollback_file = u8_string_append(source,".rollback",NULL);
    if (u8_file_existsp(rollback_file)) {
      ssize_t rv = kno_apply_head(rollback_file,source);
      if (rv<0) {
	u8_log(LOG_CRIT,"RollbackFailed",
	       "Couldn't apply rollback %s to %s for %s",
	       rollback_file,source,ix->indexid);
	u8_free(rollback_file);
	return -1; }
      u8_free(rollback_file);
      return 1;}
    else {
      u8_seterr("RollbackFailed","kindex_commit",
		u8_mkstring("The rollback file %s for %s doesn't exist",
			    rollback_file,ix->indexid));
      u8_free(rollback_file);
      return -1;}}
  case kno_commit_sync: {
    u8_string commit_file = u8_mkstring("%s.commit",source);
    ssize_t rv = kno_apply_head(commit_file,source);
    if (rv<0) {
      u8_seterr("FinishCommitFailed","kindex_commit",
		u8_mkstring("Couldn't apply commit file %s to %s for %s",
			    commit_file,source,ix->indexid));
      u8_free(commit_file);
      return -1;}
    else {
      u8_free(commit_file);
      kno_flush_stream(&(kx->index_stream));
      int rv = load_header(kx,stream);
      if (rv<0) {
	u8_seterr("FinishCommitFailed","kindex_commit",
		  u8_mkstring("Couldn't reload header for restored %s (%s)",
			      source,ix->indexid));
	return -1;}
      else return 1;}}
  case kno_commit_flush: {
    kno_flush_stream(&(kx->index_stream));
    return 1;}
  case kno_commit_cleanup: {
    if (commits->commit_stream) release_commit_stream(ix,commits);
    u8_string rollback_file = u8_string_append(source,".rollback",NULL);
    int rollback_cleanup = 0, commit_cleanup = 0;
    if (u8_file_existsp(rollback_file))
      rollback_cleanup = u8_removefile(rollback_file);
    else u8_log(LOG_WARN,"MissingRollbackFile",
		"Rollback file %s went missing",rollback_file);
    if (commits->commit_2phase) {
      u8_string commit_file = u8_string_append(source,".commit",NULL);
      if (u8_file_existsp(commit_file))
	commit_cleanup = u8_removefile(commit_file);
      else u8_logf(LOG_WARN,"MissingRollbackFile",
		   "Commit file %s went missing",commit_file);
      u8_free(commit_file);}
    u8_free(rollback_file);
    if ( ( rollback_cleanup < 0) || ( commit_cleanup < 0) )
      return -1;
    else return 1;}
  default: {
    u8_logf(LOG_WARN,"NoPhasedCommit",
	    "The index %s doesn't support phased commits",
	    ix->indexid);
    return -1;}
  }
}

static void free_keybuckets(int n,struct KEYBUCKET **keybuckets)
{
  int i = 0; while (i<n) {
    struct KEYBUCKET *kb = keybuckets[i++];
    /* We don't need to decref the ke_values of the individual key
       entries, because they're simply copied (without incref) from
       the commit schedule from where they'll be freed. */
    if (kb->kb_keybuf) u8_free(kb->kb_keybuf);
    u8_free(kb);}
  u8_big_free(keybuckets);
}

static int update_kindex_metadata(kno_kindex kx,
				     lispval metadata,
				     struct KNO_STREAM *stream,
				     struct KNO_STREAM *head)
{
  int error=0;
  u8_logf(LOG_WARN,"WriteMetadata",
	  "Writing modified metadata for %s",kx->indexid);
  ssize_t metadata_pos = kno_endpos(stream);
  if (metadata_pos>0) {
    kno_outbuf outbuf = kno_writebuf(stream);
    ssize_t new_metadata_size = write_raw(outbuf,metadata);
    ssize_t metadata_end = kno_getpos(stream);
    if (new_metadata_size<0)
      error=1;
    else {
      if ((metadata_end-metadata_pos) != new_metadata_size) {
	u8_logf(LOG_CRIT,"MetadataSizeIconsistency",
		"There was an inconsistency writing the metadata for %s",
		kx->indexid);}
      kno_write_8bytes_at(head,metadata_pos,KNO_KINDEX_METADATA_POS);
      kno_write_4bytes_at(head,metadata_end-metadata_pos,KNO_KINDEX_METADATA_POS+8);}}
  else error=1;
  if (error) {
    u8_seterr("MetaDataWriteError","update_kindex_metadata/endpos",
	      u8_strdup(kx->index_source));
    return -1;}
  else return 1;
}

static ssize_t update_xrefs(kno_kindex kx,xtype_refs refs,
			    struct KNO_STREAM *stream,
			    struct KNO_STREAM *head)
{
  u8_logf(LOG_NOTICE,"WriteOIDMap",
	  "Writing modified kindex oidcodes for %s",kx->indexid);
  ssize_t xrefs_pos  = kno_endpos(stream);
  ssize_t xrefs_size = -1;
  int n_refs = refs->xt_n_refs;
  lispval *elts = refs->xt_refs;
  if (xrefs_pos>0) {
    kno_outbuf outbuf = kno_writebuf(stream);
    int i = 0; while (i<n_refs) {
      ssize_t rv = write_raw(outbuf,elts[i]);
      if (rv<0) break;
      else i++;}
    xrefs_size = kno_getpos(stream)-xrefs_pos;}
  if (xrefs_size<0) {
    u8_seterr("MetaDataWriteError","kindex(update_xrefs)",
	      u8_strdup(kx->index_source));
    return -1;}
  else {
    kno_write_4bytes_at(head,n_refs,KNO_KINDEX_NREFS_POS);
    kno_write_8bytes_at(head,xrefs_pos,KNO_KINDEX_XREFS_POS);
    kno_write_4bytes_at(head,xrefs_size,KNO_KINDEX_XREFS_SIZE_POS);
    return xrefs_size;}
}

static int update_kindex_ondisk
(kno_kindex kx,lispval metadata,
 unsigned int flags,unsigned int cur_keys,
 unsigned int changed_buckets,
 struct BUCKET_REF *bucket_locs,
 struct XTYPE_REFS *refs,
 struct KNO_STREAM *stream,
 struct KNO_STREAM *head)
{
  if (KNO_SLOTMAPP(metadata))
    update_kindex_metadata(kx,metadata,stream,head);

  if ( (refs->xt_refs_flags) & (XTYPE_REFS_CHANGED) )
    update_xrefs(kx,refs,stream,head);

  struct KNO_OUTBUF *outstream = kno_writebuf(head);

  int i = 0;
  unsigned int *offdata = NULL;
  size_t n_buckets = kx->index_n_buckets;
  unsigned int chunk_ref_size = get_chunk_ref_size(kx);
  ssize_t offdata_byte_length = n_buckets*chunk_ref_size;
#if KNO_USE_MMAP
  unsigned int *memblock=
    mmap(NULL,256+offdata_byte_length,
	 PROT_READ|PROT_WRITE,
	 MMAP_FLAGS,
	 head->stream_fileno,
	 0);
  if ((memblock==NULL) || (memblock == MAP_FAILED)) {
    u8_graberrno("update_kindex_ondisk:mmap",u8_strdup(kx->indexid));
    return -1;}
  else offdata = memblock+64;
#else
  size_t offdata_length = n_buckets*chunk_ref_size;
  offdata = u8_big_alloc(offdata_length);
  kno_setpos(head,256);
  int rv = kno_read_ints(head,offdata_length/4,offdata);
  if (rv<0) {
    u8_graberrno("update_kindex_ondisk:kno_read_ints",u8_strdup(kx->indexid));
    u8_big_free(offdata);
    return -1;}
#endif
  /* Update the buckets if you have them */
  if ((offdata) && (kx->index_offtype == KNO_B64)) {
    while (i<changed_buckets) {
      unsigned int word1, word2, word3, bucket = bucket_locs[i].bucketno;
      word1 = (((bucket_locs[i].bck_ref.off)>>32)&(0xFFFFFFFF));
      word2 = (((bucket_locs[i].bck_ref.off))&(0xFFFFFFFF));
      word3 = (bucket_locs[i].bck_ref.size);
#if ((KNO_USE_MMAP) && (!(WORDS_BIGENDIAN)))
      offdata[bucket*3]=kno_flip_word(word1);
      offdata[bucket*3+1]=kno_flip_word(word2);
      offdata[bucket*3+2]=kno_flip_word(word3);
#else
      offdata[bucket*3]=word1;
      offdata[bucket*3+1]=word2;
      offdata[bucket*3+2]=word3;
#endif
      i++;}}
  else if ((offdata) && (kx->index_offtype == KNO_B32)) {
    while (i<changed_buckets) {
      unsigned int word1, word2, bucket = bucket_locs[i].bucketno;
      word1 = ((bucket_locs[i].bck_ref.off)&(0xFFFFFFFF));
      word2 = (bucket_locs[i].bck_ref.size);
#if ((KNO_USE_MMAP) && (!(WORDS_BIGENDIAN)))
      offdata[bucket*2]=kno_flip_word(word1);
      offdata[bucket*2+1]=kno_flip_word(word2);
#else
      offdata[bucket*2]=word1;
      offdata[bucket*2+1]=word2;
#endif
      i++;}}
  else if ((offdata) && (kx->index_offtype == KNO_B40)) {
    while (i<changed_buckets) {
      unsigned int word1 = 0, word2 = 0, bucket = bucket_locs[i].bucketno;
      kno_convert_KNO_B40_ref(bucket_locs[i].bck_ref,&word1,&word2);
#if ((KNO_USE_MMAP) && (!(WORDS_BIGENDIAN)))
      offdata[bucket*2]=kno_flip_word(word1);
      offdata[bucket*2+1]=kno_flip_word(word2);
#else
      offdata[bucket*2]=word1;
      offdata[bucket*2+1]=word2;
#endif
      i++;}}
  /* If you don't have offsets in memory, write them by hand, stepping
     through the file. */
  else if (kx->index_offtype == KNO_B64) {
    size_t bucket_start = 256;
    size_t bytes_in_bucket = 3*SIZEOF_INT;
    while (i<changed_buckets) {
      unsigned int bucket_no = bucket_locs[i].bucketno;
      unsigned int bucket_pos = bucket_start+bytes_in_bucket*bucket_no;
      kno_start_write(head,bucket_pos);
      unsigned long long off = bucket_locs[i].bck_ref.off;
      unsigned int word1 = ((off)>>32) & 0xFFFFFFFF;
      unsigned int word2 = (off) & 0xFFFFFFFF;
      kno_write_4bytes(outstream,word1);
      kno_write_4bytes(outstream,word2);
      kno_write_4bytes(outstream,bucket_locs[i].bck_ref.size);
      i++;}}
  else if (kx->index_offtype == KNO_B32) {
    size_t bucket_start = 256;
    size_t bytes_in_bucket = 2*SIZEOF_INT;
    while (i<changed_buckets) {
      unsigned int bucket_no = bucket_locs[i].bucketno;
      unsigned int bucket_pos = bucket_start+bytes_in_bucket*bucket_no;
      kno_start_write(head,bucket_pos);
      kno_write_4bytes(outstream,bucket_locs[i].bck_ref.off);
      kno_write_4bytes(outstream,bucket_locs[i].bck_ref.size);
      i++;}}
  else {
    /* This is KNO_B40 encoding, 2 words */
    size_t bucket_start = 256;
    size_t bytes_in_bucket = 2*SIZEOF_INT;
    while (i<changed_buckets) {
      unsigned int bucket_no = bucket_locs[i].bucketno;
      unsigned int bucket_pos = bucket_start+bytes_in_bucket*bucket_no;
      unsigned int word1 = 0, word2 = 0;
      kno_convert_KNO_B40_ref(bucket_locs[i].bck_ref,&word1,&word2);
      kno_start_write(head,bucket_pos);
      kno_write_4bytes(outstream,word1);
      kno_write_4bytes(outstream,word2);
      i++;}}
  /* The offsets have now been updated in memory */
  if (offdata) {
#if (KNO_USE_MMAP)
    /* If you have MMAP, make them unwritable which swaps them out to
       the file. */
    int retval = msync(offdata-64,
		       256+offdata_byte_length,
		       MS_SYNC|MS_INVALIDATE);
    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),
	      "update_kindex_ondisk:msync %s",kx->indexid);
      u8_graberrno("update_kindex_ondisk:msync",u8_strdup(kx->indexid));}
    retval = munmap(offdata-64,256+offdata_byte_length);
    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),
	      "update_kindex_ondisk:munmap %s",kx->indexid);
      u8_graberrno("update_kindex_ondisk:msync",u8_strdup(kx->indexid));}
#else
    struct KNO_OUTBUF *out = kno_start_write(head,256);
    if (kx->index_offtype == KNO_B64)
      kno_write_ints(outstream,3*SIZEOF_INT*n_buckets,offdata);
    else kno_write_ints(outstream,2*SIZEOF_INT*n_buckets,offdata);
    u8_big_free(offdata);
#endif
  }

  kno_flush_stream(stream);
  fsync(stream->stream_fileno);
  size_t end_pos = kno_endpos(stream);
  kno_write_8bytes_at(head,end_pos,256-8);

  /* Write any changed flags */
  kno_write_4bytes_at(head,flags,8);
  kno_write_4bytes_at(head,cur_keys,16);
  kno_write_4bytes_at(head,KNO_KINDEX_MAGIC_NUMBER,0);
  kno_flush_stream(stream);
  kno_flush_stream(head);

  return 0;
}

static void reload_offdata(struct KNO_INDEX *ix)
#if KNO_USE_MMAP
{
}
#else
{
  struct KNO_KINDEX *kx=(kno_kindex)ix;
  unsigned int *offdata=NULL;
  if (kx->index_offdata==NULL)
    return;
  else offdata=kx->index_offdata;
  kno_stream stream = &(kx->index_stream);
  size_t n_buckets = kx->index_n_buckets;
  unsigned int chunk_ref_size = get_chunk_ref_size(kx);
  kno_setpos(s,256);
  int retval = kno_read_ints(stream,int_len,offdata);
  if (retval<0)
    u8_logf(LOG_CRIT,"reload_offdata",
	    "Couldn't reload offdata for %s",kx->indexid);
}
#endif


/* Miscellaneous methods */

static void kindex_close(kno_index ix)
{
  struct KNO_KINDEX *kx = (struct KNO_KINDEX *)ix;
  unsigned int chunk_ref_size = get_chunk_ref_size(kx);
  unsigned int *offdata = kx->index_offdata;
  size_t n_buckets = kx->index_n_buckets;
  u8_logf(LOG_DEBUG,"KINDEX","Closing hash index %s",ix->indexid);
  kno_lock_index(kx);
  kno_close_stream(&(kx->index_stream),KNO_STREAM_FREEDATA);
  if (offdata) {
#if KNO_USE_MMAP
    int retval=
      munmap(offdata-64,(chunk_ref_size*n_buckets)+256);
    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),
	      "kindex_close:munmap %s",kx->index_source);
      errno = 0;}
#else
    u8_big_free(offdata);
#endif
    kx->index_offdata = NULL;
    kx->index_cache_level = -1;}
  u8_logf(LOG_DEBUG,"KINDEX","Closed hash index %s",ix->indexid);
  kno_unlock_index(kx);
}

static void kindex_recycle(kno_index ix)
{
  struct KNO_KINDEX *kx = (struct KNO_KINDEX *)ix;
  if ( (kx->index_offdata) || (kx->index_stream.stream_fileno > 0) )
    kindex_close(ix);
  if (kx->index_stream.streamid) {
    u8_free((kx->index_stream.streamid));
    kx->index_stream.streamid=NULL;}
  kno_recycle_xrefs(&(kx->index_xrefs));
}

/* Creating a hash ksched_i handler */

static int interpret_kindex_flags(lispval opts)
{
  int flags = 0;
  lispval offtype = kno_intern("offtype");
  if ( kno_testopt(opts,offtype,kno_intern("b64"))  ||
       kno_testopt(opts,offtype,KNO_INT(64))	    ||
       kno_testopt(opts,KNOSYM_FLAGS,kno_intern("b64")))
    flags |= (KNO_B64<<4);
  else if ( kno_testopt(opts,offtype,kno_intern("b40"))	 ||
	    kno_testopt(opts,offtype,KNO_INT(40))	 ||
	    kno_testopt(opts,KNOSYM_FLAGS,kno_intern("b40")) )
    flags |= (KNO_B40<<4);
  else if ( kno_testopt(opts,offtype,kno_intern("b32"))	 ||
	    kno_testopt(opts,offtype,KNO_INT(32))	 ||
	    kno_testopt(opts,KNOSYM_FLAGS,kno_intern("b32")))
    flags |= (KNO_B32<<4);
  else flags |= (KNO_B40<<4);

  return flags;
}

static kno_index kindex_create(u8_string spec,void *typedata,
				 kno_storage_flags flags,
				 lispval opts)
{
  int rv = 0;
  lispval metadata_init = kno_getopt(opts,kno_intern("metadata"),KNO_VOID);
  lispval xrefs_arg = kno_getopt(opts,kno_intern("xrefs"),VOID);
  lispval buckets_arg = kno_getopt(opts,kno_intern("buckets"),KNO_VOID);
  lispval size_arg = kno_getopt(opts,KNOSYM_SIZE,KNO_INT(kindex_default_size));
  lispval hashconst = kno_getopt(opts,kno_intern("hashconst"),KNO_FIXZERO);
  int n_buckets = kindex_default_size;
  if (KNO_FIXNUMP(buckets_arg))
    /* A negative number indicates an exact number of buckets */
    n_buckets = KNO_FIX2INT(buckets_arg);
  else if (KNO_FIXNUMP(size_arg))
    n_buckets = kno_get_hashtable_size(KNO_FIX2INT(size_arg));
  else {
    kno_seterr("InvalidBucketCount","kindex_create",spec,opts);
    rv = -1;}

  if (!(KNO_INTEGERP(hashconst))) {
    kno_seterr("InvalidHashConst","kindex_create",spec,hashconst);
    rv = -1;}

  lispval metadata = VOID;
  lispval created_symbol = kno_intern("created");
  lispval assembled_symbol = kno_intern("assembled");

  if (KNO_SCHEMAPP(metadata_init))
    metadata = kno_schemap2slotmap(metadata_init);
  else if (KNO_TABLEP(metadata_init)) {
    metadata = kno_deep_copy(metadata_init);}
  else metadata = kno_make_slotmap(8,0,NULL);

  lispval ltime = kno_make_timestamp(NULL);
  if (!(kno_test(metadata,created_symbol,KNO_VOID)))
    kno_store(metadata,created_symbol,ltime);
  kno_store(metadata,assembled_symbol,ltime);
  kno_decref(ltime); ltime = KNO_VOID;

  lispval keyslot = kno_getopt(opts,KNOSYM_KEYSLOT,KNO_VOID);

  if ( (KNO_VOIDP(keyslot)) || (KNO_FALSEP(keyslot)) ) {}
  else if ( (KNO_SYMBOLP(keyslot)) || (KNO_OIDP(keyslot)) )
    kno_store(metadata,KNOSYM_KEYSLOT,keyslot);
  else if (KNO_AMBIGP(keyslot)) {
    lispval reduced = KNO_EMPTY;
    KNO_DO_CHOICES(slotid,keyslot) {
      if ( (KNO_OIDP(slotid)) || (KNO_SYMBOLP(slotid)) ) {
	KNO_ADD_TO_CHOICE(reduced,slotid);}}
    if (KNO_EMPTYP(reduced)) {
      u8_log(LOG_WARN,"InvalidKeySlot",
	     "Not initializing keyslot of %s to %q",spec,keyslot);
      kno_decref(keyslot);
      keyslot = KNO_VOID;}
    else {
      kno_decref(keyslot);
      keyslot = kno_simplify_choice(reduced);
      kno_store(metadata,KNOSYM_KEYSLOT,keyslot);}}
  else u8_log(LOG_WARN,"InvalidKeySlot",
	      "Not initializing keyslot of %s to %q",spec,keyslot);

  if (rv<0)
    return NULL;
  else rv = make_kindex
	 (spec,n_buckets,
	  interpret_kindex_flags(opts),
	  KNO_INT(hashconst),
	  metadata,
	  xrefs_arg,
	  -1,-1);

  kno_decref(metadata);
  kno_decref(keyslot);
  kno_decref(metadata_init);
  kno_decref(xrefs_arg);
  kno_decref(buckets_arg);
  kno_decref(size_arg);
  kno_decref(hashconst);

  if (rv<0)
    return NULL;
  else {
    kno_set_file_opts(spec,opts);
    return kno_open_index(spec,flags,VOID);}
}


/* Useful functions */

KNO_EXPORT int kno_kindexp(struct KNO_INDEX *ix)
{
  return (ix->index_handler== &kindex_handler);
}

static lispval kindex_stats(struct KNO_KINDEX *kx)
{
  lispval result = kno_empty_slotmap();
  int n_filled = 0, maxk = 0, n_singles = 0, n2sum = 0;
  kno_add(result,kno_intern("nbuckets"),KNO_INT(kx->index_n_buckets));
  kno_add(result,kno_intern("nkeys"),KNO_INT(kx->table_n_keys));
  kno_add(result,kno_intern("nxrefs"),KNO_INT(kx->index_xrefs.xt_n_refs));
  kindex_getstats(kx,&n_filled,&maxk,&n_singles,&n2sum);
  kno_add(result,kno_intern("nfilled"),KNO_INT(n_filled));
  kno_add(result,kno_intern("nsingles"),KNO_INT(n_singles));
  kno_add(result,kno_intern("maxkeys"),KNO_INT(maxk));
  kno_add(result,kno_intern("n2sum"),KNO_INT(n2sum));
  {
    double avg = (kx->table_n_keys*1.0)/(n_filled*1.0);
    double sd2 = (n2sum*1.0)/(n_filled*n_filled*1.0);
    kno_add(result,kno_intern("mean"),kno_make_flonum(avg));
    kno_add(result,kno_intern("sd2"),kno_make_flonum(sd2));
  }
  return result;
}

KNO_EXPORT ssize_t kno_kindex_bucket(lispval ixarg,lispval key,
				      lispval mod_arg)
{
  struct KNO_INDEX *ix=kno_lisp2index(ixarg);
  ssize_t modulate = (KNO_FIXNUMP(mod_arg)) ? (KNO_FIX2INT(mod_arg)) : (-1);
  if (ix==NULL) {
    kno_type_error("kindex","kno_kindex_bucket",ixarg);
    return -1;}
  else if (ix->index_handler != &kindex_handler) {
    kno_type_error("kindex","kno_kindex_bucket",ixarg);
    return -1;}
  else return kindex_bucket(((struct KNO_KINDEX *)ix),key,modulate);
}

/* Getting more key/bucket info */

static lispval keyinfo_schema[5];

KNO_EXPORT lispval kno_kindex_keyinfo(lispval lix,
				       lispval min_count,
				       lispval max_count)
{
  kno_index ix = kno_lisp2index(lix);
  if (ix == NULL)
    return KNO_ERROR_VALUE;
  else if (ix->index_handler != &kindex_handler)
    return kno_err(_("NotAKIndex"),"kno_kindex_keyinfo",NULL,lix);
  else NO_ELSE;
  long long min_thresh =
    (KNO_FIXNUMP(min_count)) ? (KNO_FIX2INT(min_count)) : (-1);
  long long max_thresh =
    (KNO_FIXNUMP(min_count)) ? (KNO_FIX2INT(max_count)) : (-1);
  struct KNO_KINDEX *kx = (struct KNO_KINDEX *)ix;
  kno_stream s = &(kx->index_stream);
  unsigned int *offdata = kx->index_offdata;
  kno_offset_type offtype = kx->index_offtype;
  kno_inbuf ins = kno_readbuf(s);
  xtype_refs refs = &(kx->index_xrefs);
  int i = 0, n_buckets = (kx->index_n_buckets), total_keys;
  int n_to_fetch = 0, key_count = 0;
  kno_lock_stream(s);
  kno_setpos(s,16); total_keys = kno_read_4bytes(ins);
  if (total_keys==0) {
    kno_unlock_stream(s);
    return KNO_EMPTY_CHOICE;}
  struct KNO_CHOICE *choice = kno_alloc_choice(total_keys);
  lispval *elts	 = (lispval *) KNO_CHOICE_DATA(choice);
  KNO_CHUNK_REF *buckets = u8_big_alloc_n(total_keys,KNO_CHUNK_REF);
  int *bucket_no = u8_big_alloc_n(total_keys,unsigned int);
  if (offdata) {
    /* If we have chunk offsets in memory, we just read them off. */
    kno_unlock_stream(s);
    while (i<n_buckets) {
      KNO_CHUNK_REF ref =
	kno_get_chunk_ref(offdata,offtype,i,kx->index_n_buckets);
      if (ref.size>0) {
	bucket_no[n_to_fetch]=i;
	buckets[n_to_fetch]=ref;
	n_to_fetch++;}
      i++;}}
  else {
    /* If we don't have chunk offsets in memory, we keep the stream
       locked while we get them. */
    while (i<n_buckets) {
      KNO_CHUNK_REF ref = kno_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) {
	bucket_no[n_to_fetch]=i;
	buckets[n_to_fetch]=ref;
	n_to_fetch++;}
      i++;}
    kno_unlock_stream(s);}
  qsort(buckets,n_to_fetch,sizeof(KNO_CHUNK_REF),sort_blockrefs_by_off);
  struct KNO_INBUF keyblkstrm={0};
  i = 0; while (i<n_to_fetch) {
    int j = 0, n_keys;
    if (!kno_open_block(s,&keyblkstrm,buckets[i].off,buckets[i].size,1)) {
      kno_close_inbuf(&keyblkstrm);
      u8_big_free(buckets);
      u8_big_free(bucket_no);
      kno_decref_elts(elts,key_count);
      return KNO_EMPTY_CHOICE;}
    n_keys = kno_read_varint(&keyblkstrm);
    while (j<n_keys) {
      size_t key_rep_len = kno_read_varint(&keyblkstrm);
      const unsigned char *start = keyblkstrm.bufread;
      lispval key = read_key(&keyblkstrm,&(kx->index_xrefs));
      int n_vals = kno_read_varint(&keyblkstrm);
      assert(key!=0);
      if ( ( (min_thresh < 0) || (n_vals > min_thresh) ) &&
	   ( (max_thresh < 0) || (n_vals < max_thresh) ) ) {
	lispval *keyinfo_values=u8_alloc_n(4,lispval);
	int hashval = hash_bytes(start,key_rep_len);
	keyinfo_values[0] = key;
	keyinfo_values[1] = KNO_INT(n_vals);
	keyinfo_values[2] = KNO_INT(bucket_no[i]);
	keyinfo_values[3] = KNO_INT(hashval);
	lispval sm =
	  kno_make_schemap(NULL,4,
			   KNO_SCHEMAP_FIXED_SCHEMA|KNO_TABLE_READONLY,
			   keyinfo_schema,keyinfo_values);
	elts[key_count++]=(lispval)sm;}
      else kno_decref(key);
      if (n_vals==0) {}
      else if (n_vals==1) {
	int code = kno_read_varint(&keyblkstrm);
	if (code==0) {
	  lispval val=read_value(&keyblkstrm,refs);
	  kno_decref(val);}
	else kno_read_varint(&keyblkstrm);}
      else {
	kno_read_varint(&keyblkstrm);
	kno_read_varint(&keyblkstrm);}
      j++;}
    i++;}
  kno_close_inbuf(&keyblkstrm);
  u8_big_free(buckets);
  u8_big_free(bucket_no);
  return kno_init_choice(choice,key_count,NULL,
			 KNO_CHOICE_REALLOC|
			 KNO_CHOICE_ISCONSES|
			 KNO_CHOICE_DOSORT);
}

static lispval get_hashbuckets(struct KNO_KINDEX *kx)
{
  lispval buckets = KNO_EMPTY;
  kno_stream s = &(kx->index_stream);
  kno_offset_type offtype = kx->index_offtype;
  int i = 0, n_buckets = (kx->index_n_buckets);
  init_cache_level((kno_index)kx);
  unsigned int *offdata = kx->index_offdata;
  if (offdata) {
    /* If we have chunk offsets in memory, we just read them off. */
    while (i<n_buckets) {
      KNO_CHUNK_REF ref =
	kno_get_chunk_ref(offdata,offtype,i,kx->index_n_buckets);
      if (ref.size>0) {
	lispval fixnum = KNO_INT2LISP(i);
	KNO_ADD_TO_CHOICE(buckets,fixnum);}
      i++;}}
  else {
    kno_lock_stream(s);
    /* If we don't have chunk offsets in memory, we keep the stream
       locked while we get them. */
    while (i<n_buckets) {
      KNO_CHUNK_REF ref = kno_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) {
	lispval fixnum = KNO_INT2LISP(i);
	KNO_ADD_TO_CHOICE(buckets,fixnum);}
      i++;}
    kno_unlock_stream(s);}
  return kno_simplify_choice(buckets);
}

static lispval hashbucket_info(struct KNO_KINDEX *kx,lispval bucket_nums)
{
  kno_stream s = &(kx->index_stream);
  unsigned int *offdata = kx->index_offdata;
  kno_offset_type offtype = kx->index_offtype;
  int i = 0, n_buckets = (kx->index_n_buckets);
  int n_to_fetch = KNO_CHOICE_SIZE(bucket_nums), n_refs=0;
  init_cache_level((kno_index)kx);
  kno_lock_stream(s);
  xtype_refs refs = &(kx->index_xrefs);
  lispval keyinfo = KNO_EMPTY;
  KNO_CHUNK_REF *buckets = u8_big_alloc_n(n_to_fetch,KNO_CHUNK_REF);
  int *bucket_no = u8_big_alloc_n(n_to_fetch,unsigned int);
  if (offdata) {
    /* If we have chunk offsets in memory, we just read them off. */
    kno_unlock_stream(s);
    KNO_DO_CHOICES(bucket,bucket_nums) {
      if (KNO_FIXNUMP(bucket)) {
	long long bucket_val = KNO_FIX2INT(bucket);
	if ( (bucket_val >= 0) && (bucket_val < n_buckets) ) {
	  KNO_CHUNK_REF ref =
	    kno_get_chunk_ref(offdata,offtype,bucket_val,n_buckets);
	  if (ref.size>0) {
	    bucket_no[n_refs]=i;
	    buckets[n_refs]=ref;
	    n_refs++;}}}}}
  else {
    KNO_DO_CHOICES(bucket,bucket_nums) {
      if (KNO_FIXNUMP(bucket)) {
	long long bucket_val = KNO_FIX2INT(bucket);
	if ( (bucket_val >= 0) && (bucket_val < n_buckets) ) {
	  KNO_CHUNK_REF ref =
	    kno_fetch_chunk_ref(s,256,offtype,bucket_val,1);
	  if (ref.size>0) {
	    bucket_no[n_refs]=bucket_val;
	    buckets[n_refs]=ref;
	    n_refs++;}}}}}
  qsort(buckets,n_refs,sizeof(KNO_CHUNK_REF),sort_blockrefs_by_off);
  struct KNO_INBUF keyblkstrm={0};
  i = 0; while (i<n_refs) {
    int j = 0, n_keys;
    if (!kno_open_block(s,&keyblkstrm,buckets[i].off,buckets[i].size,1)) {
      kno_close_inbuf(&keyblkstrm);
      u8_big_free(buckets);
      u8_big_free(bucket_no);
      kno_decref(keyinfo);
      return KNO_EMPTY_CHOICE;}
    n_keys = kno_read_varint(&keyblkstrm);
    while (j<n_keys) {
      size_t key_rep_len = kno_read_varint(&keyblkstrm);
      const unsigned char *start = keyblkstrm.bufread;
      lispval key = read_key(&keyblkstrm,&(kx->index_xrefs));
      int n_vals = kno_read_varint(&keyblkstrm);
      assert(key!=0);
      int n_slots = (n_vals>1) ? (4) : (5);
      lispval *keyinfo_values = u8_alloc_n(n_slots,lispval);
      int hashval = hash_bytes(start,key_rep_len);
      keyinfo_values[0] = key;
      keyinfo_values[1] = KNO_INT(n_vals);
      keyinfo_values[2] = KNO_INT(bucket_no[i]);
      keyinfo_values[3] = KNO_INT(hashval);
      if (n_vals==0)
	keyinfo_values[4] = KNO_EMPTY_CHOICE;
      else if (n_vals==1) {
	lispval value = read_value(&keyblkstrm,refs);
	keyinfo_values[4]=value;}
      else {
	kno_read_varint(&keyblkstrm);
	kno_read_varint(&keyblkstrm);}
      lispval sm =
	kno_make_schemap(NULL,n_slots,
			KNO_SCHEMAP_FIXED_SCHEMA|KNO_TABLE_READONLY,
			keyinfo_schema,keyinfo_values);
      KNO_ADD_TO_CHOICE(keyinfo,sm);
      j++;}
    i++;}
  kno_close_inbuf(&keyblkstrm);
  u8_big_free(buckets);
  u8_big_free(bucket_no);
  return keyinfo;
}

static lispval hashrange_info(struct KNO_KINDEX *kx,
			      unsigned int start,unsigned int end)
{
  kno_stream s = &(kx->index_stream);
  unsigned int *offdata = kx->index_offdata;
  kno_offset_type offtype = kx->index_offtype;
  int n_to_fetch = end-start, n_refs=0;
  xtype_refs refs = &(kx->index_xrefs);
  init_cache_level((kno_index)kx);
  kno_lock_stream(s);
  lispval keyinfo = KNO_EMPTY;
  KNO_CHUNK_REF *buckets = u8_big_alloc_n(n_to_fetch,KNO_CHUNK_REF);
  int *bucket_no = u8_big_alloc_n(n_to_fetch,unsigned int);
  if (offdata) {
    /* If we have chunk offsets in memory, we just read them off. */
    kno_unlock_stream(s);
    int i=start; while (i<end) {
      KNO_CHUNK_REF ref =
	kno_get_chunk_ref(offdata,offtype,i,kx->index_n_buckets);
      if (ref.size>0) {
	bucket_no[n_refs]=i;
	buckets[n_refs]=ref;
	n_refs++;}
      i++;}}
  else {
    /* If we have chunk offsets in memory, we just read them off. */
    kno_unlock_stream(s);
    int i=start; while (i<end) {
      KNO_CHUNK_REF ref =kno_fetch_chunk_ref(s,256,offtype,i,1);
      if (ref.size>0) {
	bucket_no[n_refs]=i;
	buckets[n_refs]=ref;
	n_refs++;}
      i++;}}
  qsort(buckets,n_refs,sizeof(KNO_CHUNK_REF),sort_blockrefs_by_off);
  struct KNO_INBUF keyblkstrm={0};
  int i = 0; while (i<n_refs) {
    int j = 0, n_keys;
    if (!kno_open_block(s,&keyblkstrm,buckets[i].off,buckets[i].size,1)) {
      kno_close_inbuf(&keyblkstrm);
      u8_big_free(buckets);
      u8_big_free(bucket_no);
      kno_decref(keyinfo);
      return KNO_EMPTY_CHOICE;}
    n_keys = kno_read_varint(&keyblkstrm);
    while (j<n_keys) {
      size_t key_rep_len = kno_read_varint(&keyblkstrm);
      const unsigned char *start = keyblkstrm.bufread;
      lispval key = read_key(&keyblkstrm,&(kx->index_xrefs));
      int n_vals = kno_read_varint(&keyblkstrm);
      assert(key!=0);
      int n_slots = (n_vals>1) ? (4) : (5);
      lispval *keyinfo_values = u8_alloc_n(n_slots,lispval);
      int hashval = hash_bytes(start,key_rep_len);
      keyinfo_values[0] = key;
      keyinfo_values[1] = KNO_INT(n_vals);
      keyinfo_values[2] = KNO_INT(bucket_no[i]);
      keyinfo_values[3] = KNO_INT(hashval);
      if (n_vals==0)
	keyinfo_values[4] = KNO_EMPTY_CHOICE;
      else if (n_vals==1) {
	lispval value = read_value(&keyblkstrm,refs);
	keyinfo_values[4]=value;}
      else {
	kno_read_varint(&keyblkstrm);
	kno_read_varint(&keyblkstrm);}
      lispval sm = kno_make_schemap
	(NULL,n_slots,
	 KNO_SCHEMAP_FIXED_SCHEMA|KNO_TABLE_READONLY,
	 keyinfo_schema,keyinfo_values);
      KNO_ADD_TO_CHOICE(keyinfo,sm);
      j++;}
    i++;}
  kno_close_inbuf(&keyblkstrm);
  u8_big_free(buckets);
  u8_big_free(bucket_no);
  return keyinfo;
}

/* Modifying readonly status */

static int kindex_set_read_only(kno_kindex kx,int read_only)
{
  struct KNO_STREAM _stream, *stream = kno_init_file_stream
    (&_stream,kx->index_source,KNO_FILE_MODIFY,-1,-1);
  if (stream == NULL) return -1;
  kno_lock_index(kx);
  unsigned int format =
    kno_read_4bytes_at(stream,KNO_KINDEX_FORMAT_POS,KNO_STREAM_ISLOCKED);
  if (read_only)
    format = format | (KNO_KINDEX_READ_ONLY);
  else format = format & (~(KNO_KINDEX_READ_ONLY));
  ssize_t v = kno_write_4bytes_at(stream,format,KNO_KINDEX_FORMAT_POS);
  if (v>=0) {
    kx->kindex_format = format;
    if (read_only)
      kx->index_flags |=  KNO_STORAGE_READ_ONLY;
    else kx->index_flags &=  (~(KNO_STORAGE_READ_ONLY));}
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  kno_unlock_index(kx);
  return v;
}

/* The control function */

static lispval metadata_readonly_props = KNO_VOID;

static lispval kindex_ctl(kno_index ix,lispval op,int n,kno_argvec args)
{
  struct KNO_KINDEX *kx = (struct KNO_KINDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return kno_err("BadIndexOpCall","kindex_ctl",
		  kx->indexid,VOID);
  else if (op == kno_cachelevel_op) {
    if (n==0)
      return KNO_INT(kx->index_cache_level);
    else {
      lispval arg = (args)?(args[0]):(VOID);
      if ((FIXNUMP(arg))&&(FIX2INT(arg)>=0)&&
	  (FIX2INT(arg)<0x100)) {
	kindex_setcache(kx,FIX2INT(arg));
	return KNO_INT(kx->index_cache_level);}
      else return kno_type_error
	     (_("cachelevel"),"kindex_ctl/cachelevel",arg);}}
  else if (op == kno_bufsize_op) {
    if (n==0)
      return KNO_INT(kx->index_stream.buf.raw.buflen);
    else if (FIXNUMP(args[0])) {
      kno_lock_index(kx);
      kno_setbufsize(&(kx->index_stream),FIX2INT(args[0]));
      kno_unlock_index(kx);
      return KNO_INT(kx->index_stream.buf.raw.buflen);}
    else return kno_type_error("buffer size","kindex_ctl/bufsize",args[0]);}
  else if (op == kno_index_hashop) {
    if (n==0)
      return KNO_INT(kx->index_n_buckets);
    else {
      lispval mod_arg = (n>1) ? (args[1]) : (VOID);
      ssize_t bucket = kindex_bucket(kx,args[0],0);
      if (FIXNUMP(mod_arg))
	return KNO_INT((bucket%FIX2INT(mod_arg)));
      else if ((FALSEP(mod_arg))||(VOIDP(mod_arg)))
	return KNO_INT(bucket);
      else return KNO_INT((bucket%(kx->index_n_buckets)));}}
  else if (op == KNOSYM_XREFS) {
    struct XTYPE_REFS *refs = &(kx->index_xrefs);
    int n_refs = refs->xt_n_refs;
    if (n == 0) {
      lispval vec = kno_make_vector(n_refs,refs->xt_refs);
      kno_incref_elts(refs->xt_refs,n_refs);
      return vec;}
    else {
      int i = 0, new_refs = 0; while (i<n) {
	ssize_t rv = kno_add_xtype_ref(args[i],refs);
	if (rv<0) return KNO_ERROR;
	else if (rv>=n_refs) new_refs++;
	else NO_ELSE;
	i++;}
      return KNO_INT(new_refs);}}
  else if (op == kno_index_bucketsop) {
    if (n==0)
      return get_hashbuckets(kx);
    else if (n==1)
      return hashbucket_info(kx,args[0]);
    else if (n==2) {
      lispval arg0 = args[0], arg1 = args[1];
      if (!(KNO_UINTP(arg0)))
	return kno_type_error("lowerhashbucket","kindex_ctl/buckets",arg0);
      else if (!(KNO_UINTP(arg0)))
	return kno_type_error("lowerhashbucket","kindex_ctl/buckets",arg1);
      else {
	long long lower = kno_getint(arg0);
	long long upper = kno_getint(arg1);
	if (upper==lower) return hashbucket_info(kx,args[0]);
	else if (upper < lower)
	  return kno_err(kno_DisorderedRange,"kindex_ctl",kx->indexid,
			kno_init_pair(NULL,arg0,arg1));
	else if (upper > kx->index_n_buckets)
	  return kno_err(kno_RangeError,"kindex_ctl",kx->indexid,arg1);
	else return hashrange_info(kx,lower,upper);}}
    else return kno_err(kno_TooManyArgs,"kindex_ctl",kx->indexid,op);}
  else if ( (op == kno_metadata_op) && (n == 0) ) {
    lispval base = kno_index_base_metadata(ix);
    int n_refs = kx->index_xrefs.xt_n_refs;
    if ( kx->kindex_format & KNO_KINDEX_READ_ONLY )
      kno_store(base,KNOSYM_READONLY,KNO_TRUE);
    kno_store(base,kno_intern("nxrefs"),KNO_INT(n_refs));
    kno_store(base,buckets_symbol,KNO_INT(kx->index_n_buckets));
    kno_store(base,nkeys_symbol,KNO_INT(kx->table_n_keys));
    kno_add(base,metadata_readonly_props,slotids_symbol);
    kno_add(base,metadata_readonly_props,baseoids_symbol);
    kno_add(base,metadata_readonly_props,buckets_symbol);
    kno_add(base,metadata_readonly_props,nkeys_symbol);
    return base;}
  else if ( ( op == KNOSYM_READONLY ) && (n == 0) ) {
    if ( (ix->index_flags) & (KNO_STORAGE_READ_ONLY) )
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if ( ( op == kno_metadata_op ) && (n == 1) &&
	    ( args[0] == KNOSYM_READONLY ) )  {
    if ( (kx->kindex_format) & (KNO_KINDEX_READ_ONLY) )
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if ( ( op == kno_metadata_op ) && (n == 2) &&
	    ( args[0] == KNOSYM_READONLY ) )  {
    lispval arg = args[1];
    int rv = (KNO_FALSEP(arg)) ? (kindex_set_read_only(kx,0)) :
      (kindex_set_read_only(kx,1));
    if (rv<0)
      return KNO_ERROR;
    else return kno_incref(arg);}
  else if ( ( op == KNOSYM_READONLY ) && (n == 1) ) {
    lispval val = args[0];
    if ( (KNO_FALSEP(val)) ? (!( (kx->index_flags) & (KNO_STORAGE_READ_ONLY) )) :
	 ( (kx->index_flags) & (KNO_STORAGE_READ_ONLY) ) )
      return KNO_FALSE;
    else if (!(KNO_FALSEP(val))) {
      kx->index_flags |= KNO_STORAGE_READ_ONLY;
      return KNO_TRUE;}
    else if (!( (kx->kindex_format) & (KNO_KINDEX_READ_ONLY) )) {
      kx->index_flags &= (~KNO_STORAGE_READ_ONLY);
      return KNO_TRUE;}
    else return KNO_FALSE;}
  else if (op == kno_stats_op)
    return kindex_stats(kx);
  else if (op == kno_reload_op) {
    reload_offdata(ix);
    kno_index_swapout(ix,((n==0)?(KNO_VOID):(args[0])));
    return KNO_TRUE;}
  else if (op == kno_xrefs_op) {
    if (n == 0)
      return kno_make_vector(kx->index_xrefs.xt_n_refs,
			     kx->index_xrefs.xt_refs);
    else return kno_err(kno_TooManyArgs,"kindex_ctl/slotids",kx->indexid,KNO_VOID);}
  else if (op == kno_capacity_op)
    return KNO_INT(kx->index_n_buckets);
  else if (op == kno_load_op)
    return KNO_INT(kx->table_n_keys);
  else if (op == kno_keycount_op)
    return KNO_INT(kx->table_n_keys);
  else if (op == keycounts_symbol) {
    int n_keys=0;
    kno_choice filter;
    struct KNO_CHOICE static_choice;
    init_cache_level(ix);
    if (n==0) filter=NULL;
    else {
      lispval arg0 = args[0];
      if (EMPTYP(arg0))
	return arg0;
      else if (CHOICEP(arg0))
	filter = (kno_choice) arg0;
      else {
	static_choice.choice_size=1;
	static_choice.choice_isatomic=(!(KNO_CONSP(arg0)));
	static_choice.choice_0=arg0;
	filter=&static_choice;}}
    struct KNO_KEY_SIZE *info = kindex_fetchinfo(ix,filter,&n_keys);
    struct KNO_HASHTABLE *table= (kno_hashtable) kno_make_hashtable(NULL,n_keys);
    int i=0; while (i<n_keys) {
      kno_hashtable_op_nolock(table,kno_table_store,
			     info[i].keysize_key,
			     KNO_INT(info[i].keysize_count));
      kno_decref(info[i].keysize_key);
      i++;}
    u8_big_free(info);
    return (lispval)table;}
  else if (op == KNOSYM_FILENAME)
    return knostring(ix->index_source);
  else return kno_default_indexctl(ix,op,n,args);
}


/* Initializing the driver module */

static struct KNO_INDEX_HANDLER kindex_handler={
  "kindex", 1, sizeof(struct KNO_KINDEX), 14, NULL,
  kindex_close, /* close */
  kindex_commit, /* commit */
  kindex_fetch, /* fetch */
  kindex_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  kindex_fetchn, /* fetchn */
  kindex_fetchkeys, /* fetchkeys */
  kindex_fetchinfo, /* fetchinfo */
  NULL, /* batchadd */
  kindex_create, /* create */
  NULL, /* walk */
  kindex_recycle, /* recycle */
  kindex_ctl /* indexctl */
};

/* Module (file) Initialization */

KNO_EXPORT void kno_init_kindex_c()
{
  set_symbol = kno_intern("set");
  drop_symbol = kno_intern("drop");
  keycounts_symbol = kno_intern("keycounts");
  slotids_symbol = kno_intern("slotids");
  baseoids_symbol = kno_intern("baseoids");
  buckets_symbol = kno_intern("buckets");
  nkeys_symbol = kno_intern("keys");
  created_upsym = kno_intern("CREATED");

  metadata_readonly_props = kno_intern("_readonly_props");

  keyinfo_schema[0] = kno_intern("key");
  keyinfo_schema[1] = kno_intern("count");
  keyinfo_schema[2] = kno_intern("bucket");
  keyinfo_schema[3] = kno_intern("hash");
  keyinfo_schema[4] = kno_intern("value");

  u8_register_source_file(_FILEINFO);

  kno_register_index_type
    ("kindex",
     &kindex_handler,
     open_kindex,
     kno_match_index_file,
     (void *)(U8_INT2PTR(KNO_KINDEX_MAGIC_NUMBER)));
  kno_register_index_type
    ("knoindex",
     &kindex_handler,
     open_kindex,
     kno_match_index_file,
     (void *)(U8_INT2PTR(KNO_KINDEX_MAGIC_NUMBER)));
  kno_register_index_type
    ("damaged_kindex",
     &kindex_handler,
     recover_kindex,
     kno_match_index_file,
     (void *)(U8_INT2PTR(KNO_KINDEX_TO_RECOVER)));

  kno_register_config("KINDEX:SIZE","The default size for kindexes",
		     kno_sizeconfig_get,kno_sizeconfig_set,
		     &kindex_default_size);
  kno_register_config("KINDEX:LOGLEVEL",
		     "The default loglevel for kindexes",
		     kno_intconfig_get,kno_loglevelconfig_set,
		     &kindex_loglevel);

  kno_set_default_index_type("kindex");
}
