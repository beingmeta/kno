/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_DTYPEIO_H
#define FDB_DTYPEIO_H 1
#define FDB_DTYPEIO_H_VERSION "$Id$"

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

  dt_character_package = 0x40,
  dt_numeric_package = 0x41,
  dt_framerd_package = 0x42
} dt_type_code;

typedef unsigned char dt_subcode;

enum dt_numeric_subcodes {
  dt_small_bigint=0x00, dt_bigint=0x40, dt_double=0x01,
  dt_rational=0x81, dt_complex=0x82,
  dt_short_int_vector=0x03, dt_int_vector=0x43,
  dt_short_short_vector=0x04, dt_short_vector=0x44,
  dt_short_float_vector=0x05, dt_float_vector=0x45,
  dt_short_double_vector=0x06, dt_double_vector=0x46};
enum dt_character_subcodes
     { dt_ascii_char=0x00, dt_unicode_char=0x01,
       dt_unicode_string=0x42, dt_unicode_short_string=0x02,
       dt_unicode_symbol=0x43, dt_unicode_short_symbol=0x03,
       dt_unicode_zstring=0x44, dt_unicode_short_zstring=0x04};
enum dt_framerd_subtypes
  { dt_choice=0xC0, dt_small_choice=0x80, 
    dt_slotmap=0xC1, dt_small_slotmap=0x81,
    dt_hashtable=0xC2, dt_small_hashtable=0x82,
    dt_qchoice=0xC3, dt_small_qchoice=0x83,
    dt_hashset=0xC4, dt_small_hashset=0x84};

/* Arithmetic stubs */

FD_EXPORT fdtype
  (*_fd_make_rational)(FD_MEMORY_POOL_TYPE *,fdtype car,fdtype cdr);
FD_EXPORT void
  (*_fd_unpack_rational)(fdtype,fdtype *,fdtype *);
FD_EXPORT fdtype
  (*_fd_make_complex)(FD_MEMORY_POOL_TYPE *,fdtype car,fdtype cdr);
FD_EXPORT void
  (*_fd_unpack_complex)(fdtype,fdtype *,fdtype *);
FD_EXPORT fdtype
  (*_fd_make_double)(FD_MEMORY_POOL_TYPE *,double);

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

typedef struct FD_BYTE_OUTPUT {
  unsigned char *start, *ptr, *end;
  FD_MEMORY_POOL_TYPE *mpool;
  /* FD_BYTE_OUTPUT has a fillfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  int (*fillfn)(fd_byte_input,int);
  int (*flushfn)(fd_byte_output);} FD_BYTE_OUTPUT;

typedef struct FD_BYTE_INPUT {
  unsigned char *start, *ptr, *end;
  FD_MEMORY_POOL_TYPE *mpool;
  /* FD_BYTE_INPUT has a flushfn because DTYPE streams
     alias as both input and output streams, so we need
     to have both pointers. */
  int (*fillfn)(fd_byte_input,int);
  int (*flushfn)(fd_byte_output);} FD_BYTE_INPUT;

FD_EXPORT int fd_write_dtype(struct FD_BYTE_OUTPUT *out,fdtype x);
FD_EXPORT fdtype fd_read_dtype
(struct FD_BYTE_INPUT *in,FD_MEMORY_POOL_TYPE *p);

/* These are for input */
#define FD_INIT_BYTE_OUTPUT(bo,sz,mp)      \
  (bo)->ptr=(bo)->start=u8_pmalloc(mp,sz); \
  (bo)->end=(bo)->start+sz; (bo)->mpool=(mp)

#define FD_INIT_BYTE_INPUT(bi,b,sz)		   \
  (bi)->ptr=(bi)->start=b; (bi)->end=b+(sz); (bi)->fillfn=NULL; \
  (bi)->flushfn=NULL /* Might not be used */

FD_EXPORT void fd_need_bytes(struct FD_BYTE_OUTPUT *b,int size);
FD_EXPORT int _fd_write_bytes
   (struct FD_BYTE_OUTPUT *,unsigned char *,int len);

#define fd_write_byte(stream,b)					     \
  ((FD_EXPECT_TRUE(stream->ptr < stream->end)) ? (*stream->ptr++=b,1) \
   : (_fd_write_byte(stream,b)))
#define fd_write_4bytes(stream,w)		   \
  ((FD_EXPECT_TRUE(stream->ptr+4 < stream->end)) ? \
   (*(stream->ptr++)=(((w>>24)&0xFF)), \
    *(stream->ptr++)=(((w>>16)&0xFF)), \
    *(stream->ptr++)=(((w>>8)&0xFF)),  \
    *(stream->ptr++)=(((w>>0)&0xFF)),4) \
   : (_fd_write_4bytes(stream,w)))
#define fd_write_bytes(stream,bvec,len)	 \
  ((FD_EXPECT_TRUE((stream)->ptr+len < (stream)->end)) ? \
   (memcpy(((stream)->ptr),bvec,len),	 \
    (stream)->ptr=(stream)->ptr+len,     \
    len)                                 \
  : (_fd_write_bytes(stream,bvec,len)))

#define fd_get_byte(membuf) (*(membuf))
#define fd_get_4bytes(membuf)		  \
  ((*(membuf))<<24|(*(membuf+1))<<16|	\
   (*(membuf+2))<<8|(*(membuf+3)))
#define fd_get_bytes(bytes,membuf,len) \
   memcpy(bytes,membuf,len)

#define _fd_needs_bytes(stream,n) \
  (((stream)->ptr+n <= (stream)->end) ? (1) : \
   ((stream)->fillfn) ? (((stream)->fillfn)(((fd_byte_input)stream),n)) : (0))
#define fd_needs_bytes(stream,n) (FD_EXPECT_TRUE(_fd_needs_bytes(stream,n)))
#define _fd_has_bytes(stream,n) \
  ((stream)->ptr+n <= (stream)->end)
#define fd_has_bytes(stream,n) (FD_EXPECT_TRUE(_fd_has_bytes(stream,n)))

FD_EXPORT int _fd_read_byte(struct FD_BYTE_INPUT *stream);
FD_EXPORT int _fd_unread_byte(struct FD_BYTE_INPUT *stream,int byte);
FD_EXPORT unsigned int _fd_read_4bytes(struct FD_BYTE_INPUT *stream);
FD_EXPORT int _fd_read_bytes
  (unsigned char *bytes,struct FD_BYTE_INPUT *stream,int len);

#if FD_INLINE_DTYPEIO
#define fd_read_byte(stream) \
  ((fd_needs_bytes(stream,1)) ? (*(stream->ptr++)) \
   : (-1))

FD_FASTOP int fd_unread_byte(struct FD_BYTE_INPUT *stream,int byte)
{
  if ((stream->ptr>stream->start) && (stream->ptr[-1]==byte)) {
    stream->ptr--; return 0;}
  else return _fd_unread_byte(stream,byte);
}

FD_FASTOP unsigned int fd_read_4bytes(struct FD_BYTE_INPUT *stream)
{
  if (fd_needs_bytes(stream,4)) {
    unsigned int bytes=fd_get_4bytes(stream->ptr);
    stream->ptr=stream->ptr+4;
    return bytes;}
  else return _fd_read_4bytes(stream);
}

FD_FASTOP int fd_read_bytes
  (unsigned char *bytes,struct FD_BYTE_INPUT *stream,int len) 
{
  if (fd_needs_bytes(stream,len)) {
    memcpy(bytes,stream->ptr,len);
    stream->ptr=stream->ptr+len;
    return len;}
  else return -1;
}
#else /*  FD_INLINE_DTYPEIO */
#define fd_read_byte _fd_read_byte
#define fd_unread_byte _fd_unread_byte
#define fd_read_4bytes _fd_read_4bytes
#define fd_read_bytes _fd_read_bytes
#endif

typedef struct FD_COMPOUND_CONSTRUCTOR {
  fdtype (*make)(FD_MEMORY_POOL_TYPE *p,fdtype tag,fdtype data);
  struct FD_COMPOUND_CONSTRUCTOR *next;} FD_COMPOUND_CONSTRUCTOR;
typedef struct FD_COMPOUND_CONSTRUCTOR *fd_compound_constructor;

#endif /* FDB_DTYPEIO_H */
