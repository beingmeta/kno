/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_BUFIO_H
#define KNO_BUFIO_H 1
#ifndef KNO_BUFIO_H_INFO
#define KNO_BUFIO_H_INFO "include/kno/dtypeio.h"
#endif

KNO_EXPORT u8_condition kno_IsWriteBuf, kno_IsReadBuf;

#ifndef KNO_DEFAULT_ZLEVEL
#define KNO_DEFAULT_ZLEVEL 7
#endif

#ifndef KNO_BIGBUF_THRESHOLD
#define KNO_BIGBUF_THRESHOLD 0x100000
#endif

KNO_EXPORT size_t kno_zlib_level;
KNO_EXPORT size_t kno_bigbuf_threshold;

#if KNO_USE_MMAP
#include <sys/mman.h>
#endif

/* Byte Streams */

typedef struct KNO_RAWBUF *kno_rawbuf;
typedef struct KNO_OUTBUF *kno_outbuf;
typedef struct KNO_INBUF *kno_inbuf;

typedef struct KNO_OUTBUF {
  int buf_flags; ssize_t buflen; void *buf_data;
  unsigned char *buffer, *bufwrite, *buflim;
  /* KNO_OUTBUF has a fillfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  ssize_t (*buf_fillfn)(kno_inbuf,size_t,void *);
  ssize_t (*buf_flushfn)(kno_outbuf,void *);
  void (*buf_closefn)(kno_outbuf,void *);} KNO_OUTBUF;

typedef struct KNO_INBUF {
  int buf_flags; ssize_t buflen; void *buf_data;
  const unsigned char *buffer, *bufread, *buflim;
  /* KNO_INBUF has a flushfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  ssize_t (*buf_fillfn)(kno_inbuf,size_t,void *);
  ssize_t (*buf_flushfn)(kno_outbuf,void *);
  void (*buf_closefn)(kno_inbuf,void *);} KNO_INBUF;

typedef struct KNO_RAWBUF {
  int buf_flags; ssize_t buflen; void *buf_data;
  unsigned char *buffer, *bufpoint, *buflim;
  /* KNO_INBUF has a flushfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  ssize_t (*buf_fillfn)(kno_inbuf,size_t,void *);
  ssize_t (*buf_flushfn)(kno_outbuf,void *);
  void (*buf_closefn)(kno_rawbuf,void *);} KNO_RAWBUF;

typedef size_t (*kno_byte_fillfn)(kno_inbuf,size_t,void *);
typedef size_t (*kno_byte_flushfn)(kno_outbuf,void *);

/* Flags for all byte I/O buffers */

#define KNO_BUFIO_FLAGS       ( 1 << 0 )
#define KNO_IS_WRITING        0x0001
#define KNO_IN_STREAM         0x0002
#define KNO_BUFFER_NO_FLUSH   0x0004
#define KNO_BUFFER_NO_GROW    0x0008
#define KNO_BUFFER_OVERFLOW   0x0010
/* This is the mask for the BUFIO_ALLOC value */
#define KNO_BUFFER_ALLOC      0x0060

typedef enum BUFIO_ALLOC {
  KNO_STATIC_BUFFER     = 0x0000,
  KNO_HEAP_BUFFER       = 0x0020,
  KNO_BIGALLOC_BUFFER   = 0x0040,
  KNO_MMAP_BUFFER       = 0x0060}
  bufio_alloc;

/* This is the max flag value reserved for BUFIO itself */
#define KNO_BUFIO_MAX_FLAG    0x0800

#define KNO_ISWRITING(buf) (((buf)->buf_flags)&(KNO_IS_WRITING))
#define KNO_ISREADING(buf) (!(KNO_ISWRITING(buf)))

#define BUFIO_ALLOC(buf) ((bufio_alloc)(((buf)->buf_flags)&(KNO_BUFFER_ALLOC)))
#define BUFIO_SET_ALLOC(buf,v) \
  (buf)->buf_flags = \
    ( ( ((buf)->buf_flags) & (~(KNO_BUFFER_ALLOC)) ) | ( (v) & KNO_BUFFER_ALLOC) );

#define DT_BUILD(len,step) \
  if ((len)>=0) {      \
    ssize_t _outlen = step; \
    if (_outlen<0) len = -1; \
    else len += _outlen;}

/* Freeing the buffer */

KNO_FASTOP void _BUFIO_FREE(struct KNO_RAWBUF *buf)
{
  bufio_alloc alloc_type = (bufio_alloc) (buf->buf_flags&KNO_BUFFER_ALLOC);
  unsigned char *curbuf = buf->buffer;
  ssize_t curlen = buf->buflen;
  buf->bufpoint=buf->buflim=buf->buffer=NULL;
  switch (alloc_type) {
  case KNO_STATIC_BUFFER: return;
  case KNO_HEAP_BUFFER: u8_free(curbuf); return;
  case KNO_BIGALLOC_BUFFER: u8_big_free(curbuf); return;
  case KNO_MMAP_BUFFER: {
#if KNO_USE_MMAP
    int rv = munmap(curbuf,curlen);
    if (rv == 0) return;
    u8_log(LOG_WARN,"BufferUnmapFailed","errno=%d (%s)",
           errno,u8_strerror(errno));
    errno=0;}
#else
    u8_log(LOG_CRIT,"Bad BUFIO buffer",
           "When freeing buffer for %llx, it claims to be MMAPPED but "
           "we were not compiled with MMAP support",
           buf);
#endif
  }
}
#define BUFIO_FREE(buf) _BUFIO_FREE((kno_rawbuf)buf)

#define BUFIO_POINT(buf) ((((kno_rawbuf)(buf))->bufpoint)-((buf)->buffer))
#define BUFIO_LIMIT(buf) ((((kno_rawbuf)(buf))->buflim)-((buf)->buffer))

/* Initializing macros */

KNO_FASTOP void _KNO_INIT_OUTBUF
(struct KNO_OUTBUF *bo,unsigned char *buf,size_t sz,int flags)
{
  (bo)->bufwrite = (bo)->buffer = buf;
  (bo)->buflim = (bo)->buffer+sz;
  (bo)->buflen = sz;
  (bo)->buf_flags = flags|KNO_IS_WRITING;
  (bo)->buf_data = NULL;
  (bo)->buf_flushfn = NULL;
  (bo)->buf_fillfn = NULL;
  (bo)->buf_closefn = NULL;
}
#define KNO_INIT_OUTBUF(bo,buf,sz,flags) \
  _KNO_INIT_OUTBUF((kno_outbuf)bo,(unsigned char *)buf,(size_t)sz,(int)flags)

KNO_FASTOP void _KNO_INIT_BYTE_OUTPUT(struct KNO_OUTBUF *bo,size_t sz)
{
  if ( sz < kno_bigbuf_threshold ) {
    (bo)->bufwrite = (bo)->buffer = u8_malloc(sz);
    (bo)->buf_flags = KNO_HEAP_BUFFER|KNO_IS_WRITING;}
  else {
    (bo)->bufwrite = (bo)->buffer = u8_big_alloc(sz);
    (bo)->buf_flags = KNO_BIGALLOC_BUFFER|KNO_IS_WRITING;}
  (bo)->buflim = (bo)->buffer+sz;
  (bo)->buflen = sz;
  (bo)->buf_data = NULL;
  (bo)->buf_flushfn = NULL;
  (bo)->buf_fillfn = NULL;
  (bo)->buf_closefn = NULL;
}
#define KNO_INIT_BYTE_OUTPUT(bo,sz) \
  _KNO_INIT_BYTE_OUTPUT((kno_outbuf)(bo),(size_t)sz)

KNO_FASTOP void _KNO_INIT_BYTE_OUTBUF(kno_outbuf bo,unsigned char *buf,size_t sz)
 {
   (bo)->bufwrite = (bo)->buffer = buf;
   (bo)->buflim = (bo)->buffer+sz;
   (bo)->buflen = sz;
   (bo)->buf_flags = KNO_IS_WRITING;
   (bo)->buf_fillfn = NULL;
   (bo)->buf_flushfn = NULL;
   (bo)->buf_closefn = NULL;
 }
#define KNO_INIT_BYTE_OUTBUF(bo,buf,sz)       \
  _KNO_INIT_BYTE_OUTBUF((kno_outbuf)bo,(unsigned char *)buf,(size_t)sz)

#define KNO_DECL_OUTBUF(v,size)       \
  struct KNO_OUTBUF v;                \
  unsigned char v ## buf[size];      \
  KNO_INIT_BYTE_OUTBUF(&v,v ## buf,size)

#define KNO_INIT_INBUF(bi,buf,sz,flags)          \
  (bi)->bufread = (bi)->buffer = buf;           \
  (bi)->buflim = (bi)->buffer+sz;               \
  (bi)->buflen = sz;                            \
  (bi)->buf_flags = flags;                      \
  (bi)->buf_data = NULL;                        \
  (bi)->buf_fillfn = NULL;                      \
  (bi)->buf_flushfn = NULL;                     \
  (bi)->buf_closefn = NULL

#define KNO_INIT_BYTE_INPUT(bi,b,sz)     \
  (bi)->bufread = (bi)->buffer = b;     \
  (bi)->buflim = b+(sz);                \
  (bi)->buflen = sz;                    \
  (bi)->buf_flags = 0;                  \
  (bi)->buf_data = NULL;                \
  (bi)->buf_fillfn = NULL;              \
  (bi)->buf_flushfn = NULL;             \
  (bi)->buf_closefn = NULL

#define KNO_DECL_INBUF(v,data,len,flags)   \
  struct KNO_INBUF v;			   \
  KNO_INIT_INBUF(&v,data,len,flags)

/* Utility functions for growing buffers */

KNO_EXPORT int kno_needs_space(struct KNO_OUTBUF *b,size_t delta);
KNO_EXPORT ssize_t kno_grow_byte_input(struct KNO_INBUF *b,size_t len);

/* Read/write error signalling functions */

KNO_EXPORT int kno_iswritebuf(struct KNO_INBUF *b);
KNO_EXPORT int kno_isreadbuf(struct KNO_OUTBUF *b);
KNO_EXPORT lispval kno_lisp_isreadbuf(struct KNO_OUTBUF *b);
KNO_EXPORT lispval kno_lisp_iswritebuf(struct KNO_INBUF *b);

KNO_EXPORT int kno_reset_inbuf(struct KNO_INBUF *b);

/* Handle endianness */

KNO_FASTOP unsigned int kno_flip_word(unsigned int _w)
{ return ((((_w) << 24) & 0xff000000) | (((_w) << 8) & 0x00ff0000) |
          (((_w) >> 8) & 0x0000ff00) | ((_w) >>24) );}

KNO_FASTOP unsigned long long kno_flip_word8(unsigned long long _w)
{ return (((_w&(0xFF)) << 56) |
          ((_w&(0xFF00)) << 48) |
          ((_w&(0xFF0000)) << 24) |
          ((_w&(0xFF000000)) << 8) |
          ((_w>>56) & 0xFF) |
          ((_w>>40) & 0xFF00) |
          ((_w>>24) & 0xFF0000) |
          ((_w>>8) & 0xFF000000));}

KNO_FASTOP unsigned int kno_flip_ushort(unsigned short _w)
{ return ((((_w) >> 8) & 0x0000ff) | (((_w) << 8) & 0x0000ff00) );}

#if WORDS_BIGENDIAN
#define kno_net_order(x) (x)
#define kno_host_order(x) (x)
#define kno_ushort_net_order(x) (x)
#define kno_ushort_host_order(x) (x)
#else
#define kno_net_order(x) kno_flip_word(x)
#define kno_host_order(x) kno_flip_word(x)
#define kno_ushort_host_order(x) kno_flip_ushort(x)
#define kno_ushort_net_order(x) kno_flip_ushort(x)
#endif

KNO_EXPORT void kno_need_bytes(struct KNO_OUTBUF *b,int size);
KNO_EXPORT int _kno_write_byte(struct KNO_OUTBUF *,unsigned char);
KNO_EXPORT int _kno_write_bytes(struct KNO_OUTBUF *,
                              const unsigned char *,int len);
KNO_EXPORT int _kno_write_4bytes(struct KNO_OUTBUF *,unsigned int);
KNO_EXPORT int _kno_write_8bytes(struct KNO_OUTBUF *,kno_8bytes);

#define kno_write_byte(buf,b)                              \
  ((KNO_RARELY(KNO_ISREADING(buf))) ? (kno_isreadbuf(buf)) : \
   ((KNO_USUALLY((buf)->bufwrite < (buf)->buflim)) ?         \
    (*((buf)->bufwrite)++=(b),1) :                              \
    (_kno_write_byte((buf),(b)))))
#define _raw_write_byte(buf,b) (*((buf)->bufwrite)++=b,1)

#define kno_write_4bytes(buf,w)                          \
  ((KNO_RARELY(KNO_ISREADING(buf))) ? (kno_isreadbuf(buf)) : \
   ((KNO_USUALLY((buf)->bufwrite+4 < (buf)->buflim)) ?       \
    (*((buf)->bufwrite++) = ((unsigned char)(((w)>>24)&0xFF)),  \
     *((buf)->bufwrite++) = ((unsigned char)(((w)>>16)&0xFF)),  \
     *((buf)->bufwrite++) = ((unsigned char)(((w)>>8)&0xFF)),   \
     *((buf)->bufwrite++) = ((unsigned char)(((w)>>0)&0xFF)),   \
     4)                                                         \
    : (_kno_write_4bytes((buf),w))))

#define _ull(x) ((unsigned long long)x)
#define kno_write_8bytes(s,w)                         \
  ((KNO_RARELY(KNO_ISREADING(s))) ? (kno_isreadbuf(s)) :     \
   ((KNO_USUALLY(((s)->bufwrite)+8<(s)->buflim)) ?           \
    ((*((s)->bufwrite++) = (((_ull(w))>>56)&0xFF)),                       \
     (*((s)->bufwrite++) = (((_ull(w))>>48)&0xFF)),                       \
     (*((s)->bufwrite++) = (((_ull(w))>>40)&0xFF)),                       \
     (*((s)->bufwrite++) = (((_ull(w))>>32)&0xFF)),                       \
     (*((s)->bufwrite++) = (((_ull(w))>>24)&0xFF)),                       \
     (*((s)->bufwrite++) = (((_ull(w))>>16)&0xFF)),                       \
     (*((s)->bufwrite++) = (((_ull(w))>>8)&0xFF)),                        \
     (*((s)->bufwrite++) = (((_ull(w))>>0)&0xFF))) :              \
    (_kno_write_8bytes((s),(_ull(w))))))

#define kno_write_bytes(buf,bvec,len)  \
  ((KNO_RARELY(KNO_ISREADING(buf))) ? (kno_isreadbuf(buf)) : \
   ((KNO_USUALLY((((buf)->bufwrite)+(len)) < ((buf)->buflim))) ?     \
    (memcpy(((buf)->bufwrite),bvec,(len)),                              \
     (buf)->bufwrite+=(len),                                            \
     len)                                                               \
    : (_kno_write_bytes(buf,bvec,len))))

KNO_FASTOP int kno_write_varint(struct KNO_OUTBUF *s,kno_8bytes n)
{
  if (KNO_RARELY(KNO_ISREADING(s)))
    return kno_isreadbuf(s);
  else if (n < (((kno_8bytes)1)<<7)) {
    return kno_write_byte(s,n);}
  else if (n < (((kno_8bytes)1)<<14)) {
    if (kno_write_byte(s,((0x80)|(n>>7)))<0) return -1;
    if (kno_write_byte(s,n&0x7F)<0) return -1;
    return 2;}
  else if (n < (((kno_8bytes)1)<<21)) {
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;;
    if (kno_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;;
    if (kno_write_byte(s,n&0x7F)<0) return -1;
    return 3;}
  else if (n < (((kno_8bytes)1)<<28)) {
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (kno_write_byte(s,n&0x7F)<0) return -1;
    return 4;}
  else if (n < (((kno_8bytes)1)<<35)) {
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (kno_write_byte(s,n&0x7F)<0) return -1;
    return 5;}
  else if (n < (((kno_8bytes)1)<<42)) {
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (kno_write_byte(s,n&0x7F)<0) return -1;
    return 6;}
  else if (n < (((kno_8bytes)1)<<49)) {
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>42))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (kno_write_byte(s,n&0x7F)<0) return -1;
    return 7;}
  else if (n < (((kno_8bytes)1)<<56)) {
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>49))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>42))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (kno_write_byte(s,n&0x7F)<0) return -1;
    return 8;}
  else {
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>56))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>49))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>42))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (kno_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (kno_write_byte(s,n&0x7F)<0) return -1;
    return 9;}
}

#define kno_get_byte(membuf) ((unsigned long long)(*(membuf)))
#define _getbyte(base,off,lsb) \
  (((unsigned long long)(base[off]))<<(lsb))
#define kno_get_bytes(bytes,membuf,len)          \
   memcpy(bytes,membuf,len)
KNO_FASTOP kno_8bytes kno_get_4bytes(const unsigned char *membuf)
{
  return _getbyte(membuf,0,24)|_getbyte(membuf,1,16)|
    _getbyte(membuf,2,8)|_getbyte(membuf,3,0);
}
KNO_FASTOP kno_8bytes kno_get_8bytes(const unsigned char *membuf)
{
  return _getbyte(membuf,0,56)|_getbyte(membuf,1,48)|
    _getbyte(membuf,2,40)|_getbyte(membuf,3,32)|
    _getbyte(membuf,4,24)|_getbyte(membuf,5,16)|
    _getbyte(membuf,6,8)|_getbyte(membuf,7,0);
}

KNO_EXPORT int _kno_grow_outbuf(struct KNO_OUTBUF *b,size_t delta);
#define kno_grow_outbuf(b,d) \
  if (((b)->bufwrite+(d)) > ((b)->buflim))      \
    return _kno_grow_outbuf(b,d,b->buf_data);    \
  else return 1

KNO_EXPORT int _kno_grow_inbuf(struct KNO_INBUF *b,size_t delta);
#define kno_grow_inbuf(b,d) \
  if (((b)->bufread+(d)) > ((b)->buflim))       \
    return _kno_grow_inbuf(b,d);                 \
  else return 1

#define _kno_request_bytes(buf,n)                                         \
  ((KNO_RARELY(KNO_ISWRITING(buf))) ? (kno_iswritebuf(buf)) :        \
   (KNO_USUALLY((buf)->bufread+n <= (buf)->buflim)) ? (1) :          \
   ((buf)->buf_fillfn) ?                                                \
   ((((buf)->buf_fillfn)(((kno_inbuf)buf),n,(buf)->buf_data)),           \
    (KNO_USUALLY((buf)->bufread+n <= (buf)->buflim))):               \
   (0))
#define kno_request_bytes(buf,n) \
  (KNO_USUALLY(_kno_request_bytes((buf),n)))

#define _kno_has_bytes(buf,n)                                            \
  ((KNO_RARELY(KNO_ISWRITING(buf))) ? (kno_iswritebuf(buf)) :        \
   ((buf)->bufread+n <= (buf)->buflim))
#define kno_has_bytes(buf,n)  (KNO_USUALLY(_kno_has_bytes(buf,n)))

KNO_EXPORT int _kno_read_byte(struct KNO_INBUF *buf);
KNO_EXPORT int _kno_probe_byte(struct KNO_INBUF *buf);
KNO_EXPORT int _kno_unread_byte(struct KNO_INBUF *buf,int byte);
KNO_EXPORT long long _kno_read_4bytes(struct KNO_INBUF *buf);
KNO_EXPORT kno_8bytes _kno_read_8bytes(struct KNO_INBUF *buf);
KNO_EXPORT int _kno_read_bytes
  (unsigned char *bytes,struct KNO_INBUF *buf,int len);
KNO_EXPORT int _kno_read_varint(struct KNO_INBUF *buf);

KNO_EXPORT size_t _kno_raw_closebuf(struct KNO_RAWBUF *buf);

#if KNO_INLINE_BUFIO
KNO_FASTOP size_t kno_raw_closebuf(struct KNO_RAWBUF *buf)
{
  if (buf->buf_closefn) (buf->buf_closefn)(buf,buf->buf_data);
  if (buf->buf_flags&KNO_BUFFER_ALLOC) {
    BUFIO_FREE(buf);
    return buf->buflen;}
  else return 0;
}
#else
#define kno_raw_closebuf(buf) _kno_raw_closebuf(buf)
#endif

KNO_FASTOP size_t kno_close_inbuf(struct KNO_INBUF *buf)
{
  return kno_raw_closebuf((struct KNO_RAWBUF *)buf);
}
KNO_FASTOP size_t kno_close_outbuf(struct KNO_OUTBUF *buf)
{
  return kno_raw_closebuf((struct KNO_RAWBUF *)buf);
}

#if KNO_INLINE_BUFIO
#define kno_read_byte(buf) \
  ((KNO_RARELY(KNO_ISWRITING(buf))) ? (kno_iswritebuf(buf)) :        \
   ((kno_request_bytes(buf,1)) ? (*((buf)->bufread++))                   \
    : (-1)))
#define kno_probe_byte(buf) \
  ((KNO_RARELY(KNO_ISWRITING(buf))) ? (kno_iswritebuf(buf)) :        \
   ((kno_request_bytes((buf),1)) ?                                       \
    ((int)(*((buf)->bufread)))  :                                       \
    ((int)-1)))

KNO_FASTOP int kno_unread_byte(struct KNO_INBUF *buf,int byte)
{
  if (KNO_RARELY(KNO_ISWRITING(buf)))
    return kno_iswritebuf(buf);
  else if ( (buf->bufread>buf->buffer) &&
       (buf->bufread[-1]==byte)) {
    buf->bufread--;
    return 0;}
  else return _kno_unread_byte(buf,byte);
}

KNO_FASTOP long long kno_read_4bytes(struct KNO_INBUF *buf)
{
  if (KNO_RARELY(KNO_ISWRITING(buf)))
    return kno_iswritebuf(buf);
  else if (kno_request_bytes(buf,4)) {
    kno_8bytes value = kno_get_4bytes(buf->bufread);
    buf->bufread = buf->bufread+4;
    return value;}
  else return _kno_read_4bytes(buf);
}

KNO_FASTOP kno_8bytes kno_read_8bytes(struct KNO_INBUF *buf)
{
  if (KNO_RARELY(KNO_ISWRITING(buf)))
    return kno_iswritebuf(buf);
  else if (kno_request_bytes(buf,8)) {
    kno_8bytes value = kno_get_8bytes(buf->bufread);
    buf->bufread = buf->bufread+8;
    return value;}
  else return _kno_read_8bytes(buf);
}

KNO_FASTOP int kno_read_bytes
  (unsigned char *bytes,struct KNO_INBUF *buf,size_t len)
{
  if (KNO_RARELY(KNO_ISWRITING(buf)))
    return kno_iswritebuf(buf);
  else if (kno_request_bytes(buf,len)) {
    memcpy(bytes,buf->bufread,len);
    buf->bufread = buf->bufread+len;
    return len;}
  else return -1;
}
KNO_FASTOP kno_8bytes kno_read_varint(struct KNO_INBUF *s)
{
  kno_8bytes result = 0; int probe;
  while ((probe = (kno_read_byte(s))))
    if (probe<0) return -1;
    else if (probe&0x80) result = result<<7|(probe&0x7F);
    else break;
  return result<<7|probe;
}
#else /*  KNO_INLINE_BUFIO */
#define kno_read_byte   _kno_read_byte
#define kno_probe_byte   _kno_probe_byte
#define kno_unread_byte _kno_unread_byte
#define kno_read_4bytes _kno_read_4bytes
#define kno_read_8bytes _kno_read_8bytes
#define kno_read_bytes  _kno_read_bytes
#define kno_read_varint   _kno_read_varint
#endif

#define kno_output_byte(out,b) \
  if (kno_write_byte(out,b)<0) return -1; else {}
#define kno_output_4bytes(out,w) \
  if (kno_write_4bytes(out,w)<0) return -1; else {}
#define kno_output_bytes(out,bytes,n)                    \
  if (kno_write_bytes(out,bytes,n)<0) return -1; else {}

/* Compress and decompress */

KNO_EXPORT
unsigned char *kno_snappy_compress
(unsigned char *in,size_t n_bytes,
 unsigned char *out,ssize_t *z_len);

KNO_EXPORT
unsigned char *kno_zlib_compress
(unsigned char *in,size_t n_bytes,
 unsigned char *out,ssize_t *z_len,
 int level_arg);

#endif /* KNO_BUFIO_H */

