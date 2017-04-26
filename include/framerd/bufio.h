/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_BUFIO_H
#define FRAMERD_BUFIO_H 1
#ifndef FRAMERD_BUFIO_H_INFO
#define FRAMERD_BUFIO_H_INFO "include/framerd/dtypeio.h"
#endif

fd_exception fd_IsWriteBuf, fd_IsReadBuf;

/* Byte Streams */

typedef struct FD_RAWBUF *fd_rawbuf;
typedef struct FD_OUTBUF *fd_outbuf;
typedef struct FD_INBUF *fd_inbuf;

typedef struct FD_OUTBUF {
  int buf_flags; ssize_t buflen; void *buf_data;
  unsigned char *buffer, *bufwrite, *buflim;
  /* FD_OUTBUF has a fillfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  ssize_t (*buf_fillfn)(fd_inbuf,size_t,void *);
  ssize_t (*buf_flushfn)(fd_outbuf,void *);} FD_OUTBUF;

typedef struct FD_INBUF {
  int buf_flags; ssize_t buflen; void *buf_data;
  const unsigned char *buffer, *bufread, *buflim;
  /* FD_INBUF has a flushfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  ssize_t (*buf_fillfn)(fd_inbuf,size_t,void *);
  ssize_t (*buf_flushfn)(fd_outbuf,void *);} FD_INBUF;

typedef struct FD_RAWBUF {
  int buf_flags; ssize_t buflen; void *buf_data;
  unsigned char *buffer, *bufpoint, *buflim;
  /* FD_INBUF has a flushfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  ssize_t (*buf_fillfn)(fd_inbuf,size_t,void *);
  ssize_t (*buf_flushfn)(fd_outbuf,void *);} FD_RAWBUF;

typedef size_t (*fd_byte_fillfn)(fd_inbuf,size_t,void *);
typedef size_t (*fd_byte_flushfn)(fd_outbuf,void *);

/* Flags for all byte I/O buffers */

#define FD_BUFIO_FLAGS       (1 << 0 )
#define FD_IS_WRITING        (FD_BUFIO_FLAGS << 0)
#define FD_BUFFER_IS_MALLOCD (FD_BUFIO_FLAGS << 1)
#define FD_IN_STREAM         (FD_BUFIO_FLAGS << 2)
#define FD_BUFFER_NO_FLUSH   (FD_BUFIO_FLAGS << 3)
#define FD_BUFFER_NO_GROW    (FD_BUFIO_FLAGS << 4)
#define FD_BUFIO_MAX_FLAG    (FD_BUFIO_FLAGS << 10)

#define FD_ISWRITING(buf) (((buf)->buf_flags)&(FD_IS_WRITING))
#define FD_ISREADING(buf) (!(FD_ISWRITING(buf)))

/* Initializing macros */

/* These are for input or output */
#define FD_INIT_BYTE_OUTBUF(bo,sz)			\
  (bo)->bufwrite = (bo)->buffer = u8_malloc(sz);		\
  (bo)->buflim = (bo)->buffer+sz;			\
  (bo)->buflen = sz;					\
  (bo)->buf_flags = FD_BUFFER_IS_MALLOCD|FD_IS_WRITING;	\
  (bo)->buf_fillfn = NULL; (bo)->buf_flushfn = NULL;

#define FD_INIT_FIXED_BYTE_OUTBUF(bo,buf,sz) \
  (bo)->bufwrite = (bo)->buffer = buf;	     \
  (bo)->buflim = (bo)->buffer+sz;	     \
  (bo)->buf_fillfn = NULL;		     \
  (bo)->buf_flushfn = NULL;		     \
  (bo)->buf_flags = FD_IS_WRITING

#define FD_INIT_BYTE_INPUT(bi,b,sz) \
  (bi)->bufread = (bi)->buffer = b;   \
  (bi)->buflim = b+(sz);		    \
  (bi)->buf_fillfn = NULL;	    \
  (bi)->buf_flushfn = NULL;	    \
  (bi)->buf_flags = 0;

/* Utility functions for growing buffers */

FD_EXPORT int fd_needs_space(struct FD_OUTBUF *b,size_t delta);
FD_EXPORT int fd_grow_byte_input(struct FD_INBUF *b,size_t len);

/* Read/write error signalling functions */

FD_EXPORT int fd_iswritebuf(struct FD_INBUF *b);
FD_EXPORT int fd_isreadbuf(struct FD_OUTBUF *b);
FD_EXPORT fdtype fdt_isreadbuf(struct FD_OUTBUF *b);
FD_EXPORT fdtype fdt_iswritebuf(struct FD_INBUF *b);

/* Handle endianness */

FD_FASTOP unsigned int fd_flip_word(unsigned int _w)
{ return ((((_w) << 24) & 0xff000000) | (((_w) << 8) & 0x00ff0000) |
          (((_w) >> 8) & 0x0000ff00) | ((_w) >>24) );}

FD_FASTOP unsigned long long fd_flip_word8(unsigned long long _w)
{ return (((_w&(0xFF)) << 56) |
          ((_w&(0xFF00)) << 48) |
          ((_w&(0xFF0000)) << 24) |
          ((_w&(0xFF000000)) << 8) |
          ((_w>>56) & 0xFF) |
          ((_w>>40) & 0xFF00) |
          ((_w>>24) & 0xFF0000) |
          ((_w>>8) & 0xFF000000));}

FD_FASTOP unsigned int fd_flip_ushort(unsigned short _w)
{ return ((((_w) >> 8) & 0x0000ff) | (((_w) << 8) & 0x0000ff00) );}

#if WORDS_BIGENDIAN
#define fd_net_order(x) (x)
#define fd_host_order(x) (x)
#define fd_ushort_net_order(x) (x)
#define fd_ushort_host_order(x) (x)
#else
#define fd_net_order(x) fd_flip_word(x)
#define fd_host_order(x) fd_flip_word(x)
#define fd_ushort_host_order(x) fd_flip_ushort(x)
#define fd_ushort_net_order(x) fd_flip_ushort(x)
#endif

FD_EXPORT void fd_need_bytes(struct FD_OUTBUF *b,int size);
FD_EXPORT int _fd_write_byte(struct FD_OUTBUF *,unsigned char);
FD_EXPORT int _fd_write_bytes(struct FD_OUTBUF *,
			      const unsigned char *,int len);
FD_EXPORT int _fd_write_4bytes(struct FD_OUTBUF *,unsigned int);
FD_EXPORT int _fd_write_8bytes(struct FD_OUTBUF *,fd_8bytes);

#define fd_write_byte(buf,b)                              \
  ((FD_EXPECT_FALSE(FD_ISREADING(buf))) ? (fd_isreadbuf(buf)) : \
   ((FD_EXPECT_TRUE((buf)->bufwrite < (buf)->buflim)) ?		\
    (*((buf)->bufwrite)++=b,1) :				\
    (_fd_write_byte((buf),b))))
#define _raw_write_byte(buf,b) (*((buf)->bufwrite)++=b,1)

#define fd_write_4bytes(buf,w)				\
  ((FD_EXPECT_FALSE(FD_ISREADING(buf))) ? (fd_isreadbuf(buf)) : \
   ((FD_EXPECT_TRUE((buf)->bufwrite+4 < (buf)->buflim)) ?	\
    (*((buf)->bufwrite++) = ((unsigned char)((w>>24)&0xFF)),	\
     *((buf)->bufwrite++) = ((unsigned char)((w>>16)&0xFF)),	\
     *((buf)->bufwrite++) = ((unsigned char)((w>>8)&0xFF)),	\
     *((buf)->bufwrite++) = ((unsigned char)((w>>0)&0xFF)),	\
     4)								\
    : (_fd_write_4bytes((buf),w))))

#define _ull(x) ((unsigned long long)x)
#define fd_write_8bytes(s,w)			     \
  ((FD_EXPECT_FALSE(FD_ISREADING(s))) ? (fd_isreadbuf(s)) :	\
   ((FD_EXPECT_TRUE(((s)->bufwrite)+8<(s)->buflim)) ?		\
    ((*(s->bufwrite++) = (((_ull(w))>>56)&0xFF)),			\
     (*(s->bufwrite++) = (((_ull(w))>>48)&0xFF)),			\
     (*(s->bufwrite++) = (((_ull(w))>>40)&0xFF)),			\
     (*(s->bufwrite++) = (((_ull(w))>>32)&0xFF)),			\
     (*(s->bufwrite++) = (((_ull(w))>>24)&0xFF)),			\
     (*(s->bufwrite++) = (((_ull(w))>>16)&0xFF)),			\
     (*(s->bufwrite++) = (((_ull(w))>>8)&0xFF)),			\
     (*(s->bufwrite++) = (((_ull(w))>>0)&0xFF))) :		\
    (_fd_write_8bytes((s),(_ull(w))))))

#define fd_write_bytes(buf,bvec,len)  \
  ((FD_EXPECT_FALSE(FD_ISREADING(buf))) ? (fd_isreadbuf(buf)) : \
   ((FD_EXPECT_TRUE((((buf)->bufwrite)+(len)) < ((buf)->buflim))) ?	\
    (memcpy(((buf)->bufwrite),bvec,(len)),				\
     (buf)->bufwrite+=(len),						\
     len)								\
    : (_fd_write_bytes(buf,bvec,len))))

FD_FASTOP int fd_write_zint(struct FD_OUTBUF *s,fd_8bytes n)
{
  if (FD_EXPECT_FALSE(FD_ISREADING(s))) return fd_isreadbuf(s);
  else if (n < (((fd_8bytes)1)<<7)) {
    return fd_write_byte(s,n);}
  else if (n < (((fd_8bytes)1)<<14)) {
    if (fd_write_byte(s,((0x80)|(n>>7)))<0) return -1;
    if (fd_write_byte(s,n&0x7F)<0) return -1;
    return 2;}
  else if (n < (((fd_8bytes)1)<<21)) {
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;;
    if (fd_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;;
    if (fd_write_byte(s,n&0x7F)<0) return -1;
    return 3;}
  else if (n < (((fd_8bytes)1)<<28)) {
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_write_byte(s,n&0x7F)<0) return -1;
    return 4;}
  else if (n < (((fd_8bytes)1)<<35)) {
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_write_byte(s,n&0x7F)<0) return -1;
    return 5;}
  else if (n < (((fd_8bytes)1)<<42)) {
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_write_byte(s,n&0x7F)<0) return -1;
    return 6;}
  else if (n < (((fd_8bytes)1)<<49)) {
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>42))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_write_byte(s,n&0x7F)<0) return -1;
    return 7;}
  else if (n < (((fd_8bytes)1)<<56)) {
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>49))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>42))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_write_byte(s,n&0x7F)<0) return -1;
    return 8;}
  else {
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>56))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>49))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>42))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_write_byte(s,n&0x7F)<0) return -1;
    return 9;}
}

#define fd_get_byte(membuf) (*(membuf))
#define fd_get_4bytes(membuf)             \
  ((*(membuf))<<24|(*(membuf+1))<<16|   \
   (*(membuf+2))<<8|(*(membuf+3)))
#define _getbyte(base,off,lsb) \
  ((unsigned long long)(((unsigned long long)(base[off]))<<(lsb)))
#define fd_get_bytes(bytes,membuf,len)		\
   memcpy(bytes,membuf,len)
FD_FASTOP fd_8bytes fd_get_8bytes(const unsigned char *membuf)
{
  return _getbyte(membuf,0,56)|_getbyte(membuf,1,48)|
    _getbyte(membuf,2,40)|_getbyte(membuf,3,32)|
    _getbyte(membuf,4,24)|_getbyte(membuf,5,16)|
    _getbyte(membuf,6,8)|_getbyte(membuf,7,0);
}

FD_EXPORT int _fd_grow_outbuf(struct FD_OUTBUF *b,size_t delta);
#define fd_grow_outbuf(b,d) \
  if (((b)->bufwrite+(d)) > ((b)->buflim))	\
    return _fd_grow_outbuf(b,d,b->buf_data);	\
  else return 1

FD_EXPORT int _fd_grow_inbuf(struct FD_INBUF *b,size_t delta);
#define fd_grow_inbuf(b,d) \
  if (((b)->bufread+(d)) > ((b)->buflim))	\
    return _fd_grow_inbuf(b,d);			\
  else return 1

#define _fd_needs_bytes(buf,n)					 \
  ((U8_EXPECT_FALSE(FD_ISWRITING(buf))) ? (fd_iswritebuf(buf)) : \
   (FD_EXPECT_TRUE((buf)->bufread+n <= (buf)->buflim)) ? (1) :	 \
   ((buf)->buf_fillfn) ?					 \
   (((buf)->buf_fillfn)(((fd_inbuf)buf),n,buf->buf_data)) :	 \
   (0))
#define fd_needs_bytes(buf,n) \
  (FD_EXPECT_TRUE(_fd_needs_bytes((buf),n)))

#define _fd_has_bytes(buf,n)						\
  ((FD_EXPECT_FALSE(FD_ISWRITING(buf))) ? (fd_iswritebuf(buf)) :	\
   ((buf)->bufread+n <= (buf)->buflim))
#define fd_has_bytes(buf,n)  (FD_EXPECT_TRUE(_fd_has_bytes(buf,n)))

FD_EXPORT int _fd_read_byte(struct FD_INBUF *buf);
FD_EXPORT int _fd_unread_byte(struct FD_INBUF *buf,int byte);
FD_EXPORT fd_4bytes _fd_read_4bytes(struct FD_INBUF *buf);
FD_EXPORT fd_8bytes _fd_read_8bytes(struct FD_INBUF *buf);
FD_EXPORT int _fd_read_bytes
  (unsigned char *bytes,struct FD_INBUF *buf,int len);
FD_EXPORT int _fd_read_zint(struct FD_INBUF *buf);

FD_EXPORT size_t _fd_raw_closebuf(struct FD_RAWBUF *buf);

#if FD_INLINE_BUFIO
FD_FASTOP size_t fd_raw_closebuf(struct FD_RAWBUF *buf)
{
  if (buf->buf_flags&FD_BUFFER_IS_MALLOCD) {
    u8_free(buf->buffer);
    return buf->buflen;}
  else return 0;
}
#else
#define fd_raw_closebuf(buf) _fd_raw_closebuf(buf)
#endif

FD_FASTOP size_t fd_close_inbuf(struct FD_INBUF *buf)
{
  return fd_raw_closebuf((struct FD_RAWBUF *)buf);
}
FD_FASTOP size_t fd_close_outbuf(struct FD_OUTBUF *buf)
{
  return fd_raw_closebuf((struct FD_RAWBUF *)buf);
}

#if FD_INLINE_BUFIO
#define fd_read_byte(buf) \
  ((FD_EXPECT_FALSE(FD_ISWRITING(buf))) ? (fd_iswritebuf(buf)) :	\
   ((fd_needs_bytes(buf,1)) ? (*(buf->bufread++))			\
    : (-1)))
#define fd_probe_byte(buf) \
  ((FD_EXPECT_FALSE(FD_ISWRITING(buf))) ? (fd_iswritebuf(buf)) :	\
   ((fd_needs_bytes((buf),1)) ?						\
    ((int)(*((buf)->bufread)))	:					\
    ((int)-1)))

FD_FASTOP int fd_unread_byte(struct FD_INBUF *buf,int byte)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if ( (buf->bufread>buf->buffer) &&
       (buf->bufread[-1]==byte)) {
    buf->bufread--; return 0;}
  else return _fd_unread_byte(buf,byte);
}

FD_FASTOP fd_4bytes fd_read_4bytes(struct FD_INBUF *buf)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if (fd_needs_bytes(buf,4)) {
    fd_8bytes value = fd_get_4bytes(buf->bufread);
    buf->bufread = buf->bufread+4;
    return value;}
  else return _fd_read_4bytes(buf);
}

FD_FASTOP fd_8bytes fd_read_8bytes(struct FD_INBUF *buf)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if (fd_needs_bytes(buf,8)) {
    fd_8bytes value = fd_get_8bytes(buf->bufread);
    buf->bufread = buf->bufread+8;
    return value;}
  else return _fd_read_8bytes(buf);
}

FD_FASTOP int fd_read_bytes
  (unsigned char *bytes,struct FD_INBUF *buf,size_t len)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if (fd_needs_bytes(buf,len)) {
    memcpy(bytes,buf->bufread,len);
    buf->bufread = buf->bufread+len;
    return len;}
  else return -1;
}
FD_FASTOP fd_8bytes fd_read_zint(struct FD_INBUF *s)
{
  fd_8bytes result = 0; int probe;
  while ((probe = (fd_read_byte(s))))
    if (probe<0) return -1;
    else if (probe&0x80) result = result<<7|(probe&0x7F);
    else break;
  return result<<7|probe;
}
#else /*  FD_INLINE_BUFIO */
#define fd_read_byte   _fd_read_byte
#define fd_unread_byte _fd_unread_byte
#define fd_read_4bytes _fd_read_4bytes
#define fd_read_bytes  _fd_read_bytes
#define fd_read_zint   _fd_read_zint
#endif

#define fd_output_byte(out,b) \
  if (fd_write_byte(out,b)<0) return -1; else {}
#define fd_output_4bytes(out,w) \
  if (fd_write_4bytes(out,w)<0) return -1; else {}
#define fd_output_bytes(out,bytes,n)			\
  if (fd_write_bytes(out,bytes,n)<0) return -1; else {}

#endif /* FRAMERD_BUFIO_H */
