/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_STREAMS_H
#define KNO_STREAMS_H 1
#ifndef KNO_STREAMS_H_INFO
#define KNO_STREAMS_H_INFO "include/kno/streams.h"
#endif

#include "lisp.h"
#include "dtypeio.h"
#include "xtypes.h"

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#ifndef KNO_STREAM_BUFSIZE
#define KNO_STREAM_BUFSIZE 8000
#endif

#ifndef KNO_FILESTREAM_BUFSIZE
#define KNO_FILESTREAM_BUFSIZE 16000
#endif

#ifndef KNO_NETWORK_BUFSIZE
#define KNO_NETWORK_BUFSIZE 16000
#endif

#ifndef KNO_INLINE_STREAMIO
#define KNO_INLINE_STREAMIO 0
#endif

KNO_EXPORT size_t kno_stream_bufsize;
KNO_EXPORT size_t kno_network_bufsize;
KNO_EXPORT size_t kno_filestream_bufsize;

typedef int kno_stream_flags;

#define KNO_STREAM_NOFLAGS        (0x00)
#define KNO_STREAM_FLAGS          (0x1)
#define KNO_STREAM_LOCKED         (KNO_STREAM_FLAGS << 0)
#define KNO_STREAM_READ_ONLY      (KNO_STREAM_FLAGS << 1)
#define KNO_STREAM_WRITE_ONLY     (KNO_STREAM_FLAGS << 2)
#define KNO_STREAM_CAN_SEEK       (KNO_STREAM_FLAGS << 3)
#define KNO_STREAM_NEEDS_LOCK     (KNO_STREAM_FLAGS << 4)
#define KNO_STREAM_FILE_LOCKED    (KNO_STREAM_FLAGS << 5)
#define KNO_STREAM_SOCKET         (KNO_STREAM_FLAGS << 6)
#define KNO_STREAM_DOSYNC         (KNO_STREAM_FLAGS << 7)
#define KNO_STREAM_IS_CONSED      (KNO_STREAM_FLAGS << 8)
#define KNO_STREAM_OWNS_FILENO    (KNO_STREAM_FLAGS << 9)
#define KNO_STREAM_MMAPPED        (KNO_STREAM_FLAGS << 10)
/* DType related flags */
#define KNO_STREAM_DTYPEV2        (KNO_STREAM_FLAGS << 16)
#define KNO_STREAM_LOUDSYMS        (KNO_STREAM_FLAGS << 17)

#define KNO_DEFAULT_FILESTREAM_FLAGS \
  (KNO_STREAM_CAN_SEEK|KNO_STREAM_OWNS_FILENO)

#if KNO_USE_MMAP
#define KNO_STREAM_USEMMAP (KNO_STREAM_MMAPPED)
#else
#define KNO_STREAM_USEMMAP (0)
#endif

#define KNO_STREAM_ISLOCKED  1
#define KNO_STREAM_NOTLOCKED 0

typedef struct KNO_STREAM {
  KNO_ANNOTATED_HEADER;
  kno_stream_flags stream_flags;
  int stream_fileno;
  kno_off_t stream_filepos, stream_maxpos;
  u8_string streamid;
  union {
    struct KNO_INBUF in;
    struct KNO_OUTBUF out;
    struct KNO_RAWBUF raw;} buf;
  long long stream_locker;
#if KNO_USE_MMAP
  struct timespec mmap_time;
  u8_rwlock mmap_lock;
#endif
  u8_mutex stream_lock;
  lispval stream_lisprefs;} KNO_STREAM;
typedef struct KNO_STREAM *kno_stream;

typedef enum KNO_STREAM_MODE {
  KNO_FILE_READ, /* Read only, must exist */
  KNO_FILE_MODIFY, /* Read/write, must exist */
  KNO_FILE_WRITE, /* Read/write, may exist */
  KNO_FILE_CREATE, /* Read/write, must not exist */
  KNO_FILE_INIT, /* Read/write, created if missing */
  KNO_FILE_TRUNC, /* Read/write, created or truncated */
  KNO_FILE_NOVAL=-1 /* Read/write, truncated */
} kno_stream_mode;

typedef enum KNO_BYTEFLOW {
  kno_byteflow_read = 0,
  kno_byteflow_write = 1 } kno_byteflow;

#define KNO_STREAM_ISREADING(s) \
  (!(U8_BITP((s->buf.raw.buf_flags),(KNO_IS_WRITING))))
#define KNO_STREAM_ISWRITING(s) \
  (U8_BITP((s->buf.raw.buf_flags),(KNO_IS_WRITING)))

KNO_EXPORT ssize_t kno_fill_stream(kno_stream df,size_t n);

KNO_EXPORT
struct KNO_STREAM *kno_init_stream
(kno_stream s,u8_string id,int fileno,int flags,ssize_t bufsiz);

KNO_EXPORT struct KNO_STREAM *kno_init_byte_stream
(kno_stream stream,u8_string streamid,int flags,
 ssize_t n_bytes,const unsigned char *bytes);

KNO_EXPORT
kno_stream kno_init_file_stream 
(kno_stream stream,u8_string filename,kno_stream_mode mode,
 kno_stream_flags flags,ssize_t bufsiz);

KNO_EXPORT kno_stream kno_open_file(u8_string filename,kno_stream_mode mode);

KNO_EXPORT
kno_stream kno_reopen_file_stream
(kno_stream stream,kno_stream_mode mode,ssize_t bufsiz);

typedef enum kno_streamop {
  kno_stream_close,
  kno_stream_setbuf,
  kno_stream_lockfile,
  kno_stream_unlockfile,
  kno_stream_setread,
  kno_stream_setwrite,
  kno_stream_mmap } kno_streamop;

#define KNO_STREAM_FREEDATA    1
#define KNO_STREAM_NOCLOSE 2
#define KNO_STREAM_NOFLUSH 4
#define KNO_STREAM_CLOSE_FULL KNO_STREAM_FREEDATA

KNO_EXPORT void kno_close_stream(kno_stream s,int flags);
KNO_EXPORT void kno_free_stream(kno_stream s);

KNO_EXPORT lispval kno_read_dtype_from_file(u8_string filename);
KNO_EXPORT ssize_t _kno_write_dtype_to_file(lispval,u8_string,size_t,int);
KNO_EXPORT ssize_t kno_write_dtype_to_file(lispval obj,u8_string filename);
KNO_EXPORT ssize_t kno_write_ztype_to_file(lispval obj,u8_string filename);
KNO_EXPORT ssize_t kno_add_dtype_to_file(lispval obj,u8_string filename);

KNO_EXPORT int kno_read_byte_at(kno_stream s,kno_off_t off,int locked);
KNO_EXPORT long long kno_read_4bytes_at(kno_stream s,kno_off_t off,int locked);
KNO_EXPORT kno_8bytes kno_read_8bytes_at(kno_stream s,kno_off_t off,int locked,int *err);
KNO_EXPORT int kno_write_4bytes_at(kno_stream s,kno_4bytes w,kno_off_t off);
KNO_EXPORT int kno_write_8bytes_at(kno_stream s,kno_8bytes w,kno_off_t off);

KNO_EXPORT lispval kno_zread_dtype(struct KNO_INBUF *in);
KNO_EXPORT int kno_zwrite_dtype(struct KNO_OUTBUF *s,lispval x);
KNO_EXPORT int kno_zwrite_dtypes(struct KNO_OUTBUF *s,lispval x);

KNO_EXPORT ssize_t kno_write_ints(kno_stream s,size_t len,unsigned int *words);
KNO_EXPORT ssize_t kno_read_ints(kno_stream s,size_t len,unsigned int *words);

KNO_EXPORT ssize_t kno_stream_write(kno_stream s,size_t len,unsigned char *bytes);
KNO_EXPORT ssize_t kno_stream_read(kno_stream s,size_t len,unsigned char *bytes);

KNO_EXPORT int kno_set_direction(kno_stream s,kno_byteflow direction);
KNO_EXPORT long long kno_streamctl(kno_stream s,kno_streamop,void *data);
KNO_EXPORT lispval kno_streamctl_x(kno_stream s,kno_streamop,void *data);

#define kno_stream_bufsize(s) ((s)->buf.raw.buflen)

/* Stream position operations */

KNO_EXPORT kno_inbuf _kno_readbuf(kno_stream s);
KNO_EXPORT kno_outbuf _kno_writebuf(kno_stream s);
KNO_EXPORT kno_outbuf _kno_start_write(kno_stream s,kno_off_t pos);
KNO_EXPORT kno_inbuf _kno_start_read(kno_stream s,kno_off_t pos);
KNO_EXPORT kno_off_t _kno_setpos(kno_stream s,kno_off_t pos);
KNO_EXPORT kno_off_t _kno_endpos(kno_stream s);

KNO_EXPORT kno_off_t _kno_getpos(kno_stream s);

KNO_EXPORT ssize_t kno_read_block(kno_stream s,unsigned char *buf,
                                size_t count,kno_off_t offset,
                                int stream_locked);
KNO_EXPORT kno_inbuf kno_open_block(kno_stream s,kno_inbuf in,
                                 kno_off_t offset,ssize_t count,
                                 int stream_locked);

#if ( (KNO_INLINE_BUFIO) && (!(KNO_EXTREME_PROFILING)) )

KNO_FASTOP kno_off_t kno_getpos(kno_stream s)
{
  if (((s)->stream_flags)&KNO_STREAM_CAN_SEEK) {
    if (((s)->stream_filepos)>=0) {
      if (KNO_STREAM_ISREADING(s))
        /* If you're reading, subtract what's buffered from the file pos. */
        return (((s)->stream_filepos)-(((s)->buf.raw.buflim)-((s)->buf.raw.bufpoint)));
      /* If you're writing, add what's buffered to the file pos. */
      else return (((s)->stream_filepos)+(((s)->buf.raw.bufpoint)-((s)->buf.raw.buffer)));}
    else return _kno_getpos(s);}
  else return -1;
}

KNO_FASTOP kno_off_t kno_setpos(kno_stream s,kno_off_t pos)
{
  /* Have the linked version do the error handling. */
  if ((((s->stream_flags)&KNO_STREAM_CAN_SEEK) == 0)||(pos<0))
    return _kno_setpos(s,pos);
  else if ((s->stream_filepos>=0)&&
           (!((s->buf.raw.buf_flags)&(KNO_IS_WRITING)))&&
           (pos<s->stream_filepos)&&
           (pos>=(s->stream_filepos-(s->buf.raw.buflim-s->buf.raw.buffer)))) {
    /* The location is in the read buffer, so just move the pointer */
    kno_off_t delta = pos-s->stream_filepos;
    s->buf.raw.bufpoint = s->buf.raw.buflim+delta;
    return pos;}
  else if ((s->stream_filepos>=0)&&(kno_getpos(s) == pos))
    return pos;
  else return _kno_setpos(s,pos);
}
KNO_FASTOP kno_off_t kno_endpos(kno_stream s)
{
  /* Have the common version do the error handling. */
  if (((s->stream_flags)&KNO_STREAM_CAN_SEEK) == 0)
    return _kno_endpos(s);
  else if ((s->stream_filepos>=0)&&(s->stream_maxpos>=0)&&
           (s->stream_filepos == s->stream_maxpos))
    if ((s->buf.raw.buf_flags)&(KNO_IS_WRITING))
      return s->stream_maxpos+(s->buf.raw.bufpoint-s->buf.raw.buffer);
    else return s->stream_maxpos;
  else return _kno_endpos(s);
}
KNO_FASTOP kno_inbuf kno_readbuf(kno_stream s)
{
  if ((s->buf.raw.buf_flags)&(KNO_IS_WRITING))
    kno_set_direction(s,kno_byteflow_read);
  return &(s->buf.in);
}

KNO_FASTOP kno_outbuf kno_writebuf(kno_stream s)
{
  if (!((s->buf.raw.buf_flags)&(KNO_IS_WRITING)))
    kno_set_direction(s,kno_byteflow_write);
  return &(s->buf.out);
}
KNO_FASTOP kno_inbuf kno_start_read(kno_stream s,kno_off_t pos)
{
  if ((s->buf.raw.buf_flags)&(KNO_IS_WRITING))
    kno_set_direction(s,kno_byteflow_read);
  if (pos<0)
    kno_setpos(s,s->stream_maxpos+pos);
  else kno_setpos(s,pos);
  return &(s->buf.in);
}

KNO_FASTOP kno_outbuf kno_start_write(kno_stream s,kno_off_t pos)
{
  if (!((s->buf.raw.buf_flags)&(KNO_IS_WRITING)))
    kno_set_direction(s,kno_byteflow_write);
  if (pos<0) kno_endpos(s);
  else kno_setpos(s,pos);
  return &(s->buf.out);
}
#else
#define kno_getpos(s) _kno_getpos(s)
#define kno_setpos(s,p) _kno_setpos(s,p)
#define kno_endpos(s) _kno_endpos(s)
#define kno_readbuf(s) _kno_readbuf(s)
#define kno_writebuf(s) _kno_writebuf(s)
#define kno_start_read(s,p) _kno_start_read(s,p)
#define kno_start_write(s,p) _kno_start_write(s,p)
#endif

#define kno_streambuf(stream) (&((stream)->buf.raw))

/* Structure functions and macros */

KNO_EXPORT kno_off_t _kno_setpos(kno_stream s,kno_off_t pos);
KNO_EXPORT kno_off_t _kno_endpos(kno_stream s);
KNO_EXPORT kno_off_t kno_movepos(kno_stream s,kno_off_t delta);

KNO_EXPORT int stream_set_read(kno_stream s,int read);
KNO_EXPORT int kno_set_read(kno_stream s,int read);

KNO_EXPORT ssize_t kno_flush_stream(kno_stream s);

KNO_EXPORT ssize_t kno_setbufsize(kno_stream s,ssize_t bufsize);

KNO_EXPORT int _kno_lock_stream(kno_stream s);
KNO_EXPORT int _kno_unlock_stream(kno_stream s);
KNO_EXPORT int _kno_using_stream(kno_stream s);

#if KNO_INLINE_STREAMIO
KNO_FASTOP int kno_lock_stream(kno_stream s)
{
  long long tid = u8_threadid();
  if (s->stream_locker == tid) {
    u8_string id = s->streamid;
    u8_log(LOG_CRIT,"RecursiveStreamLock",
           "Recursively locking stream %s%s%p",
           ((id)?(id):(U8SNUL)),((id)?((u8_string)" "):(U8SNUL)),s);
    u8_seterr("RecursiveStreamLock","kno_lock_stream",id);
    return -1;}
  else {
    int rv = u8_lock_mutex(&(s->stream_lock));
    if (rv) {
      u8_seterr("MutexLockFailed","kno_lock_stream",s->streamid);
      return -1;}
    s->stream_locker = tid;
    return 1;}
}
KNO_FASTOP int kno_using_stream(kno_stream s)
{
  long long tid = u8_threadid();
  if (s->stream_locker==0) {
    int rv=u8_lock_mutex(&(s->stream_lock));
    if (rv) {
      u8_seterr("MutexLockFailed","kno_lock_stream",s->streamid);
      return -1;}
    s->stream_locker=tid;
    return 1;}
  else if (s->stream_locker==tid)
    return 0;
  else {
    int rv=u8_lock_mutex(&(s->stream_lock));
    if (rv) {
      u8_seterr("MutexLockFailed","kno_lock_stream",s->streamid);
      return -1;}
    s->stream_locker=tid;
    return 1;}
}
KNO_FASTOP int kno_unlock_stream(kno_stream s)
{
  long long tid = u8_threadid();
  if (tid != s->stream_locker) {
    u8_string id = s->streamid;
    u8_log(LOG_CRIT,"BadStreamUnlock",
           "Stream %s %p is owned by T%lld, not current T%lld",
           ((id)?(id):(U8SNUL)),((id)?((u8_string)" "):(U8SNUL)),
	   s,s->stream_locker,tid);
    u8_seterr("BadStreamUnlock","kno_unlock_stream",s->streamid);
    return -1;}
  s->stream_locker = 0;
  int rv = u8_unlock_mutex(&(s->stream_lock));
  if (rv) {
    u8_seterr("MutexUnLockFailed","kno_unlock_stream",s->streamid);
    return -1;}
  return 1;
}
#else
#define kno_lock_stream(s) _kno_lock_stream(s)
#define kno_unlock_stream(s) _kno_unlock_stream(s)
#define kno_using_stream(s) _kno_using_stream(s)
#endif

/* Chunk refs for file-based drivers */

KNO_EXPORT u8_condition kno_InvalidOffsetType;

typedef enum KNO_OFFSET_TYPE { KNO_B32 = 0, KNO_B40 = 1, KNO_B64 = 2 }
  kno_offset_type;
typedef enum KNO_COMPRESS_TYPE {
  KNO_BADTYPE = -1,
  KNO_NOCOMPRESS = 0,
  KNO_ZLIB = 1,
  KNO_ZLIB9 = 2,
  KNO_SNAPPY = 3,
  KNO_ZSTD = 4,
  KNO_ZSTD9 = 5,
  KNO_ZSTD19 = 6}
  kno_compress_type;

KNO_EXPORT kno_compress_type kno_compression_type(lispval,kno_compress_type);

KNO_EXPORT unsigned char *kno_compress(kno_compress_type,ssize_t *,
                                     const unsigned char *,size_t,
                                     void *);
KNO_EXPORT unsigned char *kno_uncompress(kno_compress_type,ssize_t *,
                                       const unsigned char *,size_t,
                                       void *);
KNO_EXPORT lispval kno_compression_name(kno_compress_type ctype);

KNO_EXPORT ssize_t kno_compress_xtype
(kno_outbuf out,lispval x,xtype_refs refs,
 kno_compress_type compress,
 int embed);

/* Full sized chunk refs, usually passed and returned but not
   directly stored on disk. */
typedef struct KNO_CHUNK_REF {
  kno_off_t off;
  ssize_t size;} KNO_CHUNK_REF;
typedef struct KNO_CHUNK_REF *kno_chunk_ref;

KNO_FASTOP int kno_chunk_ref_size(kno_offset_type offtype)
{
  switch (offtype) {
  case KNO_B32: case KNO_B40: return 8;
  case KNO_B64: return 12;}
  return -1;
}

KNO_FASTOP int kno_bad_chunk(struct KNO_CHUNK_REF *ref)
{
  return ( (ref->off < 0) || (ref->size < 0) );
}

typedef unsigned long long kno_ull;
typedef long long kno_ll;

KNO_FASTOP
KNO_CHUNK_REF kno_get_chunk_ref(unsigned int *offsets,
                              kno_offset_type offtype,
                              size_t offset,
                              size_t offmax)
{
  struct KNO_CHUNK_REF result;
  if (offset>=offmax) {
    result.off = 0; result.size = 0;
    return result;}
  else switch (offtype) {
  case KNO_B32: {
#if ((KNO_USE_MMAP) && (!(WORDS_BIGENDIAN)))
    unsigned int word1 = kno_flip_word((offsets)[offset*2]);
    unsigned int word2 = kno_flip_word((offsets)[offset*2+1]);
#else
    unsigned int word1 = (offsets)[offset*2];
    unsigned int word2 = (offsets)[offset*2+1];
#endif
    result.off = word1; result.size = word2;
    break;}
  case KNO_B40: {
#if ((KNO_USE_MMAP) && (!(WORDS_BIGENDIAN)))
    unsigned int word1 = kno_flip_word((offsets)[offset*2]);
    unsigned int word2 = kno_flip_word((offsets)[offset*2+1]);
#else
    unsigned int word1 = (offsets)[offset*2];
    unsigned int word2 = (offsets)[offset*2+1];
#endif
    result.off = (((((kno_ull)(word2))&(0xFF000000))<<8)|((kno_ull)word1));
    result.size = ((word2)&(0x00FFFFFF));
    break;}
  case KNO_B64: {
#if ((KNO_USE_MMAP) && (!(WORDS_BIGENDIAN)))
    unsigned int word1 = kno_flip_word((offsets)[offset*3]);
    unsigned int word2 = kno_flip_word((offsets)[offset*3+1]);
    unsigned int word3 = kno_flip_word((offsets)[offset*3+2]);
#else
    unsigned int word1 = (offsets)[offset*3];
    unsigned int word2 = (offsets)[offset*3+1];
    unsigned int word3 = (offsets)[offset*3+2];
#endif
    result.off = (kno_off_t) ((((kno_ll)word1)<<32)|(((kno_ll)word2)));
    result.size = (size_t) word3;
    break;}
  default:
    u8_seterr(kno_InvalidOffsetType,"get_chunkref",NULL);
    result.off = -1; result.size = -1;}
  return result;
}

U8_MAYBE_UNUSED static
int kno_convert_KNO_B40_ref(KNO_CHUNK_REF ref,
                       unsigned int *word1,
                       unsigned int *word2)
{
  if (ref.size>=0x1000000) return -1;
  else if (ref.off<0x100000000LL) {
    *word1 = (ref.off)&(0xFFFFFFFFLL);
    *word2 = ref.size;}
  else {
    *word1 = (ref.off)&(0xFFFFFFFFLL);
    *word2 = ref.size|(((ref.off)>>8)&(0xFF000000LL));}
  return 0;
}

/* Support for drivers */

KNO_EXPORT KNO_CHUNK_REF
kno_fetch_chunk_ref(struct KNO_STREAM *stream,
                   kno_off_t base,
                   kno_offset_type offtype,
                   unsigned int offset,
                   int locked);

/* Exceptions */

KNO_EXPORT u8_condition kno_ReadOnlyStream;
KNO_EXPORT u8_condition kno_CantWrite, kno_CantRead, kno_CantSeek;
KNO_EXPORT u8_condition kno_BadLSEEK, kno_OverSeek, kno_UnderSeek;

#endif /* KNO_STREAMS_H */

