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

/* Byte Streams */

typedef struct FD_BYTE_OUTPUT *fd_byte_output;
typedef struct FD_BYTE_INPUT *fd_byte_input;

#define FD_BYTEBUF_MALLOCD 1
#define FD_DTYPEV2         ((FD_BYTEBUF_MALLOCD)<<1)
#define FD_WRITE_OPAQUE    ((FD_DTYPEV2)<<1)

typedef struct FD_BYTE_OUTPUT {
  unsigned char *bs_bufstart, *bs_bufptr, *bs_buflim;
  int bs_flags;
  /* FD_BYTE_OUTPUT has a fillfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  int (*bs_fillfn)(fd_byte_input,int);
  int (*bs_flushfn)(fd_byte_output);} FD_BYTE_OUTPUT;

typedef struct FD_BYTE_INPUT {
  const unsigned char *bs_bufstart, *bs_bufptr, *bs_buflim;
  int bs_flags;
  /* FD_BYTE_INPUT has a flushfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  int (*bs_fillfn)(fd_byte_input,int);
  int (*bs_flushfn)(fd_byte_output);} FD_BYTE_INPUT;

FD_EXPORT int fd_write_dtype(struct FD_BYTE_OUTPUT *out,fdtype x);
FD_EXPORT fdtype fd_read_dtype(struct FD_BYTE_INPUT *in);

/* These are for input or output */
#define FD_INIT_BYTE_OUTPUT(bo,sz)     \
  (bo)->bs_bufptr=(bo)->bs_bufstart=u8_malloc(sz); \
  (bo)->bs_buflim=(bo)->bs_bufstart+sz;            \
  (bo)->bs_flags=FD_BYTEBUF_MALLOCD;      \
  (bo)->bs_fillfn=NULL; (bo)->bs_flushfn=NULL;

#define FD_INIT_FIXED_BYTE_OUTPUT(bo,buf,sz)           \
  (bo)->bs_bufptr=(bo)->bs_bufstart=buf; (bo)->bs_buflim=(bo)->bs_bufstart+sz; \
  (bo)->bs_flags=0; (bo)->bs_fillfn=NULL; (bo)->bs_flushfn=NULL;

#define FD_INIT_BYTE_INPUT(bi,b,sz)                          \
  (bi)->bs_bufptr=(bi)->bs_bufstart=b; (bi)->bs_buflim=b+(sz); (bi)->bs_flags=0;  \
  (bi)->bs_fillfn=NULL; (bi)->bs_flushfn=NULL
  /* flushfn might not be used */

FD_EXPORT void fd_need_bytes(struct FD_BYTE_OUTPUT *b,int size);
FD_EXPORT int _fd_write_byte(struct FD_BYTE_OUTPUT *,unsigned char);
FD_EXPORT int _fd_write_bytes(struct FD_BYTE_OUTPUT *,
			      const unsigned char *,int len);
FD_EXPORT int _fd_write_4bytes(struct FD_BYTE_OUTPUT *,unsigned int);
FD_EXPORT int _fd_write_8bytes(struct FD_BYTE_OUTPUT *,fd_8bytes);

#define fd_write_byte(stream,b)                                      \
  ((FD_EXPECT_TRUE((stream)->bs_bufptr < (stream)->bs_buflim)) ? (*((stream)->bs_bufptr)++=b,1) \
   : (_fd_write_byte((stream),b)))

#define fd_write_4bytes(stream,w)                  \
  ((FD_EXPECT_TRUE((stream)->bs_bufptr+4 < (stream)->bs_buflim)) ?  \
   (*((stream)->bs_bufptr++)=((unsigned char)((w>>24)&0xFF)), \
    *((stream)->bs_bufptr++)=((unsigned char)((w>>16)&0xFF)), \
    *((stream)->bs_bufptr++)=((unsigned char)((w>>8)&0xFF)),  \
    *((stream)->bs_bufptr++)=((unsigned char)((w>>0)&0xFF)),4)        \
   : (_fd_write_4bytes((stream),w)))

#define fd_write_8bytes(stream,w)                       \
  ((FD_EXPECT_TRUE((stream)->bs_bufptr+8 < (stream)->bs_buflim)) ?  \
   ((*(s->bs_bufptr++)=((w>>56)&0xFF)),                       \
    (*(s->bs_bufptr++)=((w>>48)&0xFF)),                       \
    (*(s->bs_bufptr++)=((w>>40)&0xFF)),                       \
    (*(s->bs_bufptr++)=((w>>32)&0xFF)),                       \
    (*(s->bs_bufptr++)=((w>>24)&0xFF)),                       \
    (*(s->bs_bufptr++)=((w>>16)&0xFF)),                       \
    (*(s->bs_bufptr++)=((w>>8)&0xFF)),                        \
    (*(s->bs_bufptr++)=((w>>0)&0xFF))) :                      \
   (_fd_write_8bytes((stream),w)))

#define fd_write_bytes(stream,bvec,len)  \
  ((FD_EXPECT_TRUE((stream)->bs_bufptr+len < (stream)->bs_buflim)) ? \
   (memcpy(((stream)->bs_bufptr),bvec,len),    \
    (stream)->bs_bufptr=(stream)->bs_bufptr+len,     \
    len)                                 \
  : (_fd_write_bytes(stream,bvec,len)))

FD_FASTOP int fd_write_zint(struct FD_BYTE_OUTPUT *s,int n)
{
  if (n < (1<<7)) {
    return fd_write_byte(s,n);}
  else if (n < (1<<14)) {
    if (fd_write_byte(s,((0x80)|(n>>7)))<0) return -1;
    if (fd_write_byte(s,n&0x7F)<0) return -1;
    return 2;}
  else if (n < (1<<21)) {
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;;
    if (fd_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;;
    if (fd_write_byte(s,n&0x7F)<0) return -1;
    return 3;}
  else if (n < (1<<28)) {
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_write_byte(s,n&0x7F)<0) return -1;
    return 4;}
  else {
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_write_byte(s,n&0x7F)<0) return -1;
    return 5;}
}

FD_FASTOP int fd_write_zint8(struct FD_BYTE_OUTPUT *s,fd_8bytes n)
{
  if (n < (((fd_8bytes)1)<<7)) {
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
#define fd_get_8bytes(membuf)                                           \
  ((((fd_8bytes)(*(membuf)))<<56)|(((fd_8bytes)(*(membuf+1)))<<48)   |  \
   (((fd_8bytes)(*(membuf+2)))<<40)|(((fd_8bytes)(*(membuf+3)))<<32) |  \
   (((fd_8bytes)(*(membuf+4)))<<24)|(((fd_8bytes)(*(membuf+5)))<<16) |  \
   (((fd_8bytes)(*(membuf+6)))<<8)|((fd_8bytes)(*(membuf+7))))
#define fd_get_bytes(bytes,membuf,len) \
   memcpy(bytes,membuf,len)

#define _fd_needs_bytes(stream,n)					\
  (((stream)->bs_bufptr+n <= (stream)->bs_buflim) ? (1) :		\
   ((stream)->bs_fillfn) ?						\
   (((stream)->bs_fillfn)(((fd_byte_input)stream),n)) : (0))
#define fd_needs_bytes(stream,n) (FD_EXPECT_TRUE(_fd_needs_bytes(stream,n)))
#define _fd_has_bytes(stream,n) \
  ((stream)->bs_bufptr+n <= (stream)->bs_buflim)
#define fd_has_bytes(stream,n) (FD_EXPECT_TRUE(_fd_has_bytes(stream,n)))

FD_EXPORT int _fd_read_byte(struct FD_BYTE_INPUT *stream);
FD_EXPORT int _fd_unread_byte(struct FD_BYTE_INPUT *stream,int byte);
FD_EXPORT fd_4bytes _fd_read_4bytes(struct FD_BYTE_INPUT *stream);
FD_EXPORT fd_8bytes _fd_read_8bytes(struct FD_BYTE_INPUT *stream);
FD_EXPORT int _fd_read_bytes
  (unsigned char *bytes,struct FD_BYTE_INPUT *stream,int len);
FD_EXPORT int _fd_read_zint(struct FD_BYTE_INPUT *stream);
FD_EXPORT fd_8bytes _fd_read_zint8(struct FD_BYTE_INPUT *stream);

#if FD_INLINE_DTYPEIO
#define fd_read_byte(stream) \
  ((fd_needs_bytes(stream,1)) ? (*(stream->bs_bufptr++)) \
   : (-1))

FD_FASTOP int fd_unread_byte(struct FD_BYTE_INPUT *stream,int byte)
{
  if ( (stream->bs_bufptr>stream->bs_bufstart) && 
       (stream->bs_bufptr[-1]==byte)) {
    stream->bs_bufptr--; return 0;}
  else return _fd_unread_byte(stream,byte);
}

FD_FASTOP fd_4bytes fd_read_4bytes(struct FD_BYTE_INPUT *stream)
{
  if (fd_needs_bytes(stream,4)) {
    unsigned int bytes=fd_get_4bytes(stream->bs_bufptr);
    stream->bs_bufptr=stream->bs_bufptr+4;
    return bytes;}
  else return _fd_read_4bytes(stream);
}

FD_FASTOP fd_8bytes fd_read_8bytes(struct FD_BYTE_INPUT *stream)
{
  if (fd_needs_bytes(stream,8)) {
    unsigned int bytes=fd_get_8bytes(stream->bs_bufptr);
    stream->bs_bufptr=stream->bs_bufptr+8;
    return bytes;}
  else return _fd_read_8bytes(stream);
}

FD_FASTOP int fd_read_bytes
  (unsigned char *bytes,struct FD_BYTE_INPUT *stream,int len)
{
  if (fd_needs_bytes(stream,len)) {
    memcpy(bytes,stream->bs_bufptr,len);
    stream->bs_bufptr=stream->bs_bufptr+len;
    return len;}
  else return -1;
}
FD_FASTOP int fd_read_zint(struct FD_BYTE_INPUT *s)
{
  unsigned int result=0; int probe;
  while ((probe=(fd_read_byte(s))))
    if (probe<0) return -1;
    else if (probe&0x80) result=result<<7|(probe&0x7F);
    else break;
  return result<<7|probe;
}
FD_FASTOP fd_8bytes fd_read_zint8(struct FD_BYTE_INPUT *s)
{
  fd_8bytes result=0; int probe;
  while ((probe=(fd_read_byte(s))))
    if (probe<0) return -1;
    else if (probe&0x80) result=result<<7|(probe&0x7F);
    else break;
  return result<<7|probe;
}
#else /*  FD_INLINE_DTYPEIO */
#define fd_read_byte _fd_read_byte
#define fd_unread_byte _fd_unread_byte
#define fd_read_4bytes _fd_read_4bytes
#define fd_read_bytes _fd_read_bytes
#define fd_read_zint _fd_read_zint
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

FD_EXPORT int fd_needs_space(struct FD_BYTE_OUTPUT *b,size_t delta);
FD_EXPORT int fd_grow_byte_input(struct FD_BYTE_INPUT *b,size_t len);

#endif /* FRAMERD_DTYPEIO_H */
