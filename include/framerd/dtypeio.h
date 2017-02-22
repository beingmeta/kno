/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_DTYPEIO_H
#define FRAMERD_DTYPEIO_H 1
#ifndef FRAMERD_DTYPEIO_H_INFO
#define FRAMERD_DTYPEIO_H_INFO "include/framerd/dtypeio.h"
#endif

fd_exception fd_IsWriteBuf, fd_IsReadBuf;

/* Byte Streams */

typedef struct FD_BYTE_OUTBUF *fd_byte_outbuf;
typedef struct FD_BYTE_INBUF *fd_byte_inbuf;

typedef struct FD_BYTE_OUTBUF {
  unsigned char *bufbase, *bufpoint, *buflim;
  int buf_flags; size_t buflen;
  /* FD_BYTE_OUTBUF has a fillfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  size_t (*buf_fillfn)(fd_byte_inbuf,size_t);
  size_t (*buf_flushfn)(fd_byte_outbuf);} FD_BYTE_OUTBUF;

typedef struct FD_BYTE_INBUF {
  const unsigned char *bufbase, *bufpoint, *buflim;
  int buf_flags; size_t buflen;
  /* FD_BYTE_INBUF has a flushfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  size_t (*buf_fillfn)(fd_byte_inbuf,size_t);
  size_t (*buf_flushfn)(fd_byte_outbuf);} FD_BYTE_INBUF;

struct FD_BYTE_RAWBUF {
  unsigned char *bufbase, *bufpoint, *buflim;
  int buf_flags; size_t buflen;
  /* FD_BYTE_INBUF has a flushfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  size_t (*buf_fillfn)(fd_byte_inbuf,size_t);
  size_t (*buf_flushfn)(fd_byte_outbuf);};

typedef size_t (*fd_byte_fillfn)(fd_byte_inbuf,size_t);
typedef size_t (*fd_byte_flushfn)(fd_byte_outbuf);

/* Flags for all byte I/O buffers */

#define FD_BYTEBUF_FLAGS       (1 << 0 )
#define FD_IS_WRITING          (FD_BYTEBUF_FLAGS << 0)
#define FD_BUFFER_IS_MALLOCD   (FD_BYTEBUF_FLAGS << 1)
#define FD_USE_DTYPEV2         (FD_BYTEBUF_FLAGS << 2)
#define FD_WRITE_OPAQUE        (FD_BYTEBUF_FLAGS << 3)
#define FD_NATSORT_VALUES      (FD_BYTEBUF_FLAGS << 4)
#define FD_IS_BYTESTREAM       (FD_BYTEBUF_FLAGS << 5)

#define FD_ISWRITING(buf) (((buf)->buf_flags)&(FD_IS_WRITING))
#define FD_ISREADING(buf) (!(FD_ISWRITING(buf)))

/* Initializing macros */

/* These are for input or output */
#define FD_INIT_BYTE_OUTBUF(bo,sz)			\
  (bo)->bufpoint=(bo)->bufbase=u8_malloc(sz);		\
  (bo)->buflim=(bo)->bufbase+sz;			\
  (bo)->buflen=sz;					\
  (bo)->buf_flags=FD_BUFFER_IS_MALLOCD|FD_IS_WRITING;	\
  (bo)->buf_fillfn=NULL; (bo)->buf_flushfn=NULL;

#define FD_INIT_FIXED_BYTE_OUTBUF(bo,buf,sz) \
  (bo)->bufpoint=(bo)->bufbase=buf;	     \
  (bo)->buflim=(bo)->bufbase+sz;	     \
  (bo)->buf_fillfn=NULL;		     \
  (bo)->buf_flushfn=NULL;		     \
  (bo)->buf_flags=FD_IS_WRITING

#define FD_INIT_BYTE_INPUT(bi,b,sz) \
  (bi)->bufpoint=(bi)->bufbase=b;   \
  (bi)->buflim=b+(sz);		    \
  (bi)->buf_fillfn=NULL;	    \
  (bi)->buf_flushfn=NULL;	    \
  (bi)->buf_flags=0;

/* Some top level functions */

FD_EXPORT int fd_write_dtype(struct FD_BYTE_OUTBUF *out,fdtype x);
FD_EXPORT fdtype fd_read_dtype(struct FD_BYTE_INBUF *in);

/* Read/write error signalling functions */

FD_EXPORT int fd_iswritebuf(struct FD_BYTE_INBUF *b);
FD_EXPORT int fd_isreadbuf(struct FD_BYTE_OUTBUF *b);
FD_EXPORT fdtype fdt_isreadbuf(struct FD_BYTE_OUTBUF *b);
FD_EXPORT fdtype fdt_iswritebuf(struct FD_BYTE_INBUF *b);

/* DTYPE constants */

typedef enum dt_type_code {
  dt_invalid = 0x00,
  dt_empty_list = 0x01,
  dt_boolean = 0x02,
  dt_fixnum = 0x03,
  dt_flonum = 0x04,
  dt_packet = 0x05,
  dt_string = 0x06,
  dt_symbol = 0x07,
  dt_pair = 0x08,
  dt_vector = 0x09,
  dt_void = 0x0a,
  dt_compound = 0x0b,
  dt_error = 0x0c,
  dt_exception = 0x0d,
  dt_oid = 0x0e,
  dt_zstring = 0x0f,
  /* These are all for DTYPE protocol V2 */
  dt_tiny_symbol=0x10,
  dt_tiny_string=0x11,
  dt_tiny_choice=0x12,
  dt_empty_choice=0x13,
  dt_block=0x14,

  dt_character_package = 0x40,
  dt_numeric_package = 0x41,
  dt_framerd_package = 0x42,

  dt_ztype = 0x70

} dt_type_code;

typedef unsigned char dt_subcode;

enum dt_numeric_subcodes {
  dt_small_bigint=0x00, dt_bigint=0x40, dt_double=0x01,
  dt_rational=0x81, dt_complex=0x82,
  dt_short_int_vector=0x03, dt_int_vector=0x43,
  dt_short_short_vector=0x04, dt_short_vector=0x44,
  dt_short_float_vector=0x05, dt_float_vector=0x45,
  dt_short_double_vector=0x06, dt_double_vector=0x46,
  dt_short_long_vector=0x07, dt_long_vector=0x47};
enum dt_character_subcodes
     { dt_ascii_char=0x00, dt_unicode_char=0x01,
       dt_unicode_string=0x42, dt_unicode_short_string=0x02,
       dt_unicode_symbol=0x43, dt_unicode_short_symbol=0x03,
       dt_unicode_zstring=0x44, dt_unicode_short_zstring=0x04,
       dt_secret_packet=0x45, dt_short_secret_packet=0x05};
enum dt_framerd_subtypes
  { dt_choice=0xC0, dt_small_choice=0x80,
    dt_slotmap=0xC1, dt_small_slotmap=0x81,
    dt_hashtable=0xC2, dt_small_hashtable=0x82,
    dt_qchoice=0xC3, dt_small_qchoice=0x83,
    dt_hashset=0xC4, dt_small_hashset=0x84};

FD_EXPORT int fd_use_dtblock;

/* Arithmetic stubs */

FD_EXPORT fdtype(*_fd_make_rational)(fdtype car,fdtype cdr);
FD_EXPORT void(*_fd_unpack_rational)(fdtype,fdtype *,fdtype *);
FD_EXPORT fdtype(*_fd_make_complex)(fdtype car,fdtype cdr);
FD_EXPORT void(*_fd_unpack_complex)(fdtype,fdtype *,fdtype *);
FD_EXPORT fdtype(*_fd_make_double)(double);

/* Packing and unpacking */

typedef fdtype(*fd_packet_unpacker)(unsigned int len,unsigned char *packet);
typedef fdtype(*fd_vector_unpacker)(unsigned int len,fdtype *vector);

FD_EXPORT int fd_register_vector_unpacker
  (unsigned int code,unsigned int subcode,fd_vector_unpacker f);

FD_EXPORT int fd_register_packet_unpacker
  (unsigned int code,unsigned int subcode,fd_packet_unpacker f);

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

FD_EXPORT void fd_need_bytes(struct FD_BYTE_OUTBUF *b,int size);
FD_EXPORT int _fd_write_byte(struct FD_BYTE_OUTBUF *,unsigned char);
FD_EXPORT int _fd_write_bytes(struct FD_BYTE_OUTBUF *,
			      const unsigned char *,int len);
FD_EXPORT int _fd_write_4bytes(struct FD_BYTE_OUTBUF *,unsigned int);
FD_EXPORT int _fd_write_8bytes(struct FD_BYTE_OUTBUF *,fd_8bytes);

#define fd_write_byte(buf,b)                              \
  ((FD_EXPECT_FALSE(FD_ISREADING(buf))) ? (fd_isreadbuf(buf)) : \
   ((FD_EXPECT_TRUE((buf)->bufpoint < (buf)->buflim)) ?		\
    (*((buf)->bufpoint)++=b,1) :				\
    (_fd_write_byte((buf),b))))
#define _raw_write_byte(buf,b) (*((buf)->bufpoint)++=b,1)

#define fd_write_4bytes(buf,w)				\
  ((FD_EXPECT_FALSE(FD_ISREADING(buf))) ? (fd_isreadbuf(buf)) : \
   ((FD_EXPECT_TRUE((buf)->bufpoint+4 < (buf)->buflim)) ?	\
    (*((buf)->bufpoint++)=((unsigned char)((w>>24)&0xFF)),	\
     *((buf)->bufpoint++)=((unsigned char)((w>>16)&0xFF)),	\
     *((buf)->bufpoint++)=((unsigned char)((w>>8)&0xFF)),	\
     *((buf)->bufpoint++)=((unsigned char)((w>>0)&0xFF)),	\
     4)								\
    : (_fd_write_4bytes((buf),w))))

#define _ull(x) ((unsigned long long)x)
#define fd_write_8bytes(s,w)			     \
  ((FD_EXPECT_FALSE(FD_ISREADING(s))) ? (fd_isreadbuf(s)) :	\
   ((FD_EXPECT_TRUE(((s)->bufpoint)+8<(s)->buflim)) ?		\
    ((*(s->bufpoint++)=(((_ull(w))>>56)&0xFF)),			\
     (*(s->bufpoint++)=(((_ull(w))>>48)&0xFF)),			\
     (*(s->bufpoint++)=(((_ull(w))>>40)&0xFF)),			\
     (*(s->bufpoint++)=(((_ull(w))>>32)&0xFF)),			\
     (*(s->bufpoint++)=(((_ull(w))>>24)&0xFF)),			\
     (*(s->bufpoint++)=(((_ull(w))>>16)&0xFF)),			\
     (*(s->bufpoint++)=(((_ull(w))>>8)&0xFF)),			\
     (*(s->bufpoint++)=(((_ull(w))>>0)&0xFF))) :		\
    (_fd_write_8bytes((s),(_ull(w))))))

#define fd_write_bytes(buf,bvec,len)  \
  ((FD_EXPECT_FALSE(FD_ISREADING(buf))) ? (fd_isreadbuf(buf)) : \
   ((FD_EXPECT_TRUE((((buf)->bufpoint)+(len)) < ((buf)->buflim))) ?	\
    (memcpy(((buf)->bufpoint),bvec,(len)),				\
     (buf)->bufpoint+=(len),						\
     len)								\
    : (_fd_write_bytes(buf,bvec,len))))

FD_FASTOP int fd_write_zint(struct FD_BYTE_OUTBUF *s,fd_8bytes n)
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

FD_EXPORT int _fd_grow_outbuf(struct FD_BYTE_OUTBUF *b,size_t delta);
#define fd_grow_outbuf(b,d) \
  if (((b)->bufpoint+(d)) > ((b)->buflim)) \
    return _fd_grow_outbuf(b,d);	   \
  else return 1

FD_EXPORT int _fd_grow_inbuf(struct FD_BYTE_INBUF *b,size_t delta);
#define fd_grow_inbuf(b,d) \
  if (((b)->bufpoint+(d)) > ((b)->buflim))	\
    return _fd_grow_inbuf(b,d);			\
  else return 1

#define _fd_needs_bytes(buf,n)					 \
  ((U8_EXPECT_FALSE(FD_ISWRITING(buf))) ? (fd_iswritebuf(buf)) : \
   (FD_EXPECT_TRUE((buf)->bufpoint+n <= (buf)->buflim)) ? (1) :	 \
   ((buf)->buf_fillfn) ?					 \
   (((buf)->buf_fillfn)(((fd_byte_inbuf)buf),n)) :		 \
   (0))
#define fd_needs_bytes(buf,n) (FD_EXPECT_TRUE(_fd_needs_bytes((buf),n)))

#define _fd_has_bytes(buf,n)						\
  ((FD_EXPECT_FALSE(FD_ISWRITING(buf))) ? (fd_iswritebuf(buf)) :	\
   ((buf)->bufpoint+n <= (buf)->buflim))
#define fd_has_bytes(buf,n)  (FD_EXPECT_TRUE(_fd_has_bytes(buf,n)))

FD_EXPORT int _fd_read_byte(struct FD_BYTE_INBUF *buf);
FD_EXPORT int _fd_unread_byte(struct FD_BYTE_INBUF *buf,int byte);
FD_EXPORT fd_4bytes _fd_read_4bytes(struct FD_BYTE_INBUF *buf);
FD_EXPORT fd_8bytes _fd_read_8bytes(struct FD_BYTE_INBUF *buf);
FD_EXPORT int _fd_read_bytes
  (unsigned char *bytes,struct FD_BYTE_INBUF *buf,int len);
FD_EXPORT int _fd_read_zint(struct FD_BYTE_INBUF *buf);

#if FD_INLINE_DTYPEIO
#define fd_read_byte(buf) \
  ((FD_EXPECT_FALSE(FD_ISWRITING(buf))) ? (fd_iswritebuf(buf)) :	\
   ((fd_needs_bytes(buf,1)) ? (*(buf->bufpoint++))			\
    : (-1)))
#define fd_probe_byte(buf) \
  ((FD_EXPECT_FALSE(FD_ISWRITING(buf))) ? (fd_iswritebuf(buf)) :	\
   ((fd_needs_bytes((buf),1)) ?						\
    ((int)(*((buf)->bufpoint)))	:					\
    ((int)-1)))

FD_FASTOP int fd_unread_byte(struct FD_BYTE_INBUF *buf,int byte)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if ( (buf->bufpoint>buf->bufbase) &&
       (buf->bufpoint[-1]==byte)) {
    buf->bufpoint--; return 0;}
  else return _fd_unread_byte(buf,byte);
}

FD_FASTOP fd_4bytes fd_read_4bytes(struct FD_BYTE_INBUF *buf)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if (fd_needs_bytes(buf,4)) {
    fd_8bytes value=fd_get_4bytes(buf->bufpoint);
    buf->bufpoint=buf->bufpoint+4;
    return value;}
  else return _fd_read_4bytes(buf);
}

FD_FASTOP fd_8bytes fd_read_8bytes(struct FD_BYTE_INBUF *buf)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if (fd_needs_bytes(buf,8)) {
    fd_8bytes value=fd_get_8bytes(buf->bufpoint);
    buf->bufpoint=buf->bufpoint+8;
    return value;}
  else return _fd_read_8bytes(buf);
}

FD_FASTOP int fd_read_bytes
  (unsigned char *bytes,struct FD_BYTE_INBUF *buf,size_t len)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if (fd_needs_bytes(buf,len)) {
    memcpy(bytes,buf->bufpoint,len);
    buf->bufpoint=buf->bufpoint+len;
    return len;}
  else return -1;
}
FD_FASTOP fd_8bytes fd_read_zint(struct FD_BYTE_INBUF *s)
{
  fd_8bytes result=0; int probe;
  while ((probe=(fd_read_byte(s))))
    if (probe<0) return -1;
    else if (probe&0x80) result=result<<7|(probe&0x7F);
    else break;
  return result<<7|probe;
}
#else /*  FD_INLINE_DTYPEIO */
#define fd_read_byte   _fd_read_byte
#define fd_unread_byte _fd_unread_byte
#define fd_read_4bytes _fd_read_4bytes
#define fd_read_bytes  _fd_read_bytes
#define fd_read_zint   _fd_read_zint
#endif

typedef struct FD_COMPOUND_CONSTRUCTOR {
  fdtype (*make)(fdtype tag,fdtype data);
  struct FD_COMPOUND_CONSTRUCTOR *next;} FD_COMPOUND_CONSTRUCTOR;
typedef struct FD_COMPOUND_CONSTRUCTOR *fd_compound_constructor;


/* Returning error codes */

#if FD_DEBUG_DTYPEIO
#define fd_return_errcode(x) ((fdtype)(_fd_return_errcode(x)))
#else
#define fd_return_errcode(x) ((fdtype)(x))
#endif

/* Utility functions */

FD_EXPORT int fd_needs_space(struct FD_BYTE_OUTBUF *b,size_t delta);
FD_EXPORT int fd_grow_byte_input(struct FD_BYTE_INBUF *b,size_t len);

#endif /* FRAMERD_DTYPEIO_H */
