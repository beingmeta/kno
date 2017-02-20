/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FD_DTYPESTREAM_H
#define FD_DTYPESTREAM_H 1
#ifndef FRAMERD_DTYPESTREAM_H_INFO
#define FRAMERD_DTYPESTREAM_H_INFO "include/framerd/dtypestream.h"
#endif

#include "dtype.h"
#include "dtypeio.h"

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

FD_EXPORT unsigned int fd_check_dtsize;

FD_EXPORT fd_exception fd_ReadOnlyStream;
FD_EXPORT fd_exception fd_CantWrite, fd_CantRead, fd_CantSeek;
FD_EXPORT fd_exception fd_BadLSEEK, fd_OverSeek, fd_UnderSeek;

#define FD_DTSTREAM_BUFSIZ_DEFAULT 32*1024
#define FD_DTSTREAM_READING        ((FD_DTYPEV2)<<1)
#define FD_DTSTREAM_READ_ONLY      ((FD_DTSTREAM_READING)<<1)
#define FD_DTSTREAM_CANSEEK        ((FD_DTSTREAM_READ_ONLY)<<1)
#define FD_DTSTREAM_NEEDS_LOCK     ((FD_DTSTREAM_CANSEEK)<<1)
#define FD_DTSTREAM_LOCKED         ((FD_DTSTREAM_NEEDS_LOCK)<<1)
#define FD_DTSTREAM_SOCKET         ((FD_DTSTREAM_LOCKED)<<1)
#define FD_DTSTREAM_DOSYNC         ((FD_DTSTREAM_SOCKET)<<1)

typedef enum FD_DTSTREAM_MODE {
  FD_DTSTREAM_READ, /* Read only, must exist */
  FD_DTSTREAM_MODIFY, /* Read/write, must exist */
  FD_DTSTREAM_WRITE, /* Read/write, may exist */
  FD_DTSTREAM_CREATE /* Read/write, truncated */
} fd_dtstream_mode;

typedef struct FD_DTYPE_STREAM {
  unsigned char *bs_bufstart, *bs_bufptr, *bs_buflim;
  int bs_flags;
  int (*bs_fillfn)(struct FD_DTYPE_STREAM *,int);
  int (*bs_flushfn)(struct FD_DTYPE_STREAM *);
  u8_string dts_idstring; int dts_mallocd, dts_bufsiz;
  fd_off_t dts_diskpos, dts_maxpos;
  u8_mutex stream_lock;
  int fd_fileno;} FD_DTYPE_STREAM;
typedef struct FD_DTYPE_STREAM *fd_dtype_stream;

FD_EXPORT struct FD_DTYPE_STREAM *fd_init_dtype_stream
  (struct FD_DTYPE_STREAM *s,int sock,int bufsiz);

FD_EXPORT fd_dtype_stream fd_init_dtype_file_stream
   (struct FD_DTYPE_STREAM *stream,
    u8_string filename,fd_dtstream_mode mode,int bufsiz);

FD_EXPORT fd_dtype_stream fd_open_dtype_file
  (u8_string filename,fd_dtstream_mode mode,int bufsiz);
#define fd_dtsopen(filename,mode) \
  fd_open_dtype_file(filename,mode,FD_DTSTREAM_BUFSIZ_DEFAULT)

#define FD_DTS_UNLOCK 1
#define FD_STREAM_LOCKED 0

#define FD_DTS_FREE    1
#define FD_DTS_NOCLOSE 2
#define FD_DTS_NOFLUSH 4
#define FD_DTSCLOSE_FULL FD_DTS_FREE
FD_EXPORT void fd_dtsclose(fd_dtype_stream s,int flags);
FD_EXPORT void fd_dtsfree(fd_dtype_stream s,int flags);

FD_EXPORT fdtype fd_read_dtype_from_file(u8_string filename);
FD_EXPORT ssize_t _fd_write_dtype_to_file(fdtype,u8_string,size_t,int);
FD_EXPORT ssize_t fd_write_dtype_to_file(fdtype obj,u8_string filename);
FD_EXPORT ssize_t fd_write_ztype_to_file(fdtype obj,u8_string filename);
FD_EXPORT ssize_t fd_add_dtype_to_file(fdtype obj,u8_string filename);

/* Structure functions and macros */

FD_EXPORT fd_off_t _fd_getpos(fd_dtype_stream s);
FD_EXPORT fd_off_t _dts_getpos(fd_dtype_stream s,int unlock);
#define fd_getpos(s) \
  ((((s)->bs_flags)&FD_DTSTREAM_CANSEEK) ?				\
   ((((s)->dts_diskpos)>=0) ?						\
    ((FD_DTS_ISREADING(s)) ?						\
     (((s)->dts_diskpos)-(((s)->bs_buflim)-((s)->bs_bufptr))) :		\
     (((s)->dts_diskpos)+(((s)->bs_bufptr)-((s)->bs_bufstart)))) :	\
    (_fd_getpos(s)))							\
   : (-1))
#define dts_getpos(s,unlock)						\
  ((((s)->bs_flags)&FD_DTSTREAM_CANSEEK) ?				\
   ((((s)->dts_diskpos)>=0) ?						\
    ((FD_DTS_ISREADING(s)) ?						\
     (((s)->dts_diskpos)-(((s)->bs_buflim)-((s)->bs_bufptr))) :		\
     (((s)->dts_diskpos)+(((s)->bs_bufptr)-((s)->bs_bufstart)))) :	\
    (_dts_getpos(s,unlock)))						\
   : (-1))

FD_EXPORT fd_off_t fd_setpos(fd_dtype_stream s,fd_off_t pos);
FD_EXPORT fd_off_t dts_setpos(fd_dtype_stream s,fd_off_t pos);
FD_EXPORT fd_off_t fd_movepos(fd_dtype_stream s,int delta);
FD_EXPORT fd_off_t fd_endpos(fd_dtype_stream s);
FD_EXPORT fd_off_t dts_endpos(fd_dtype_stream s);

FD_EXPORT int fd_set_read(fd_dtype_stream s,int read);
FD_EXPORT int fd_dtsflush(fd_dtype_stream s);
FD_EXPORT int dts_set_read(fd_dtype_stream s,int read,int unlock);
FD_EXPORT int dts_dtsflush(fd_dtype_stream s,int unlock);

FD_EXPORT void dts_lock(struct FD_DTYPE_STREAM *s);
FD_EXPORT void dts_unlock(struct FD_DTYPE_STREAM *s);

FD_EXPORT int fd_dtslockfile(fd_dtype_stream s);
FD_EXPORT int fd_dtsunlockfile(fd_dtype_stream s);
FD_EXPORT int dts_lockfile(fd_dtype_stream s);
FD_EXPORT int dts_unlockfile(fd_dtype_stream s);

#define fd_dts_lockfile(s) fd_dtslockfile(s)
#define fd_dts_unlockfile(s) fd_dtsunlockfile(s)

#define FD_DTS_ISREADING(s) (U8_BITP((s->bs_flags),(FD_DTSTREAM_READING)))
#define FD_DTS_ISWRITING(s) (!(U8_BITP((s->bs_flags),(FD_DTSTREAM_READING))))
#define FD_DTS_CANSEEK(s) (!(U8_BITP((s->bs_flags),(FD_DTSTREAM_CANSEEK))))

FD_EXPORT void fd_dtsbufsize(fd_dtype_stream s,int bufsiz);

#define fd_dts_start_read(s)			      \
  if ((((s)->bs_flags)&FD_DTSTREAM_READING) == 0) \
    if (fd_set_read(s,1)<0) return -1;
#define fd_dts_start_write(s)		       \
  if (((s)->bs_flags)&FD_DTSTREAM_READING) \
    if (fd_set_read(s,0)<0) return -1;
#define dts_start_read(s,unlock)			      \
  if ((((s)->bs_flags)&FD_DTSTREAM_READING) == 0) \
    if (dts_set_read(s,1,unlock)<0) return -1;
#define dts_start_write(s,unlock)		       \
  if (((s)->bs_flags)&FD_DTSTREAM_READING) \
    if (dts_set_read(s,0,unlock)<0) return -1;

/* Readers and writers */

FD_EXPORT fdtype fd_dtsread_dtype(fd_dtype_stream s);
FD_EXPORT fdtype dts_read_dtype(fd_dtype_stream s,int unlock);

FD_EXPORT int fd_dtswrite_dtype(fd_dtype_stream s,fdtype x);
FD_EXPORT int dts_write_dtype(fd_dtype_stream s,fdtype x,int unlock);

FD_EXPORT int fd_dtsread_ints(fd_dtype_stream s,int len,unsigned int *words);
FD_EXPORT int dts_read_ints(fd_dtype_stream s,int len,unsigned int *words);

FD_EXPORT int fd_dtswrite_ints(fd_dtype_stream s,int len,unsigned int *words);
FD_EXPORT int dts_write_ints(fd_dtype_stream s,int len,unsigned int *words);

FD_EXPORT int fd_zwrite_dtype(struct FD_DTYPE_STREAM *s,fdtype x);
FD_EXPORT int dts_zwrite_dtype(struct FD_DTYPE_STREAM *s,fdtype x,int unlock);
FD_EXPORT fdtype fd_zread_dtype(struct FD_DTYPE_STREAM *s);
FD_EXPORT fdtype dts_zread_dtype(struct FD_DTYPE_STREAM *s,int unlock);

FD_EXPORT int fd_zwrite_dtypes(struct FD_DTYPE_STREAM *s,fdtype x);
FD_EXPORT int dts_zwrite_dtypes(struct FD_DTYPE_STREAM *s,fdtype x,int unlock);

/* All of these assume the stream's mutex is locked */

FD_EXPORT int _fd_dtsread_byte(struct FD_DTYPE_STREAM *stream);
FD_EXPORT int _fd_dtsprobe_byte(struct FD_DTYPE_STREAM *stream);
FD_EXPORT fd_4bytes _fd_dtsread_4bytes(struct FD_DTYPE_STREAM *stream);
FD_EXPORT fd_8bytes _fd_dtsread_8bytes(struct FD_DTYPE_STREAM *stream);
FD_EXPORT int _fd_dtsread_bytes
  (struct FD_DTYPE_STREAM *stream,unsigned char *bytes,int len);
FD_EXPORT int _dts_read_bytes
  (struct FD_DTYPE_STREAM *stream,unsigned char *bytes,int len,u8_mutex *unlock);
FD_EXPORT fd_off_t _fd_dtsread_off_t(struct FD_DTYPE_STREAM *stream);
FD_EXPORT fd_4bytes _fd_dtsread_zint(fd_dtype_stream s);
FD_EXPORT fd_8bytes _fd_dtsread_zint8(fd_dtype_stream s);

FD_EXPORT int _fd_dtswrite_byte(struct FD_DTYPE_STREAM *stream,int b);
FD_EXPORT int _fd_dtswrite_4bytes(struct FD_DTYPE_STREAM *stream,fd_4bytes w);
FD_EXPORT int fd_dtswrite_bytes(struct FD_DTYPE_STREAM *stream,
				const unsigned char *bytes,int len);

FD_EXPORT int _dts_write_bytes(struct FD_DTYPE_STREAM *stream,
			       const unsigned char *bytes,int len);

FD_EXPORT int _fd_dtswrite_zint(struct FD_DTYPE_STREAM *stream,fd_4bytes w);
FD_EXPORT int _fd_dtswrite_zint8(struct FD_DTYPE_STREAM *stream,fd_8bytes b);

FD_EXPORT int dts_write4_at(fd_dtype_stream s,fd_off_t off,fd_4bytes w);
FD_EXPORT int dts_write8_at(fd_dtype_stream s,fd_off_t off,fd_8bytes w);
FD_EXPORT fd_4bytes dts_read4_at(fd_dtype_stream s,fd_off_t off,int *err);
FD_EXPORT fd_8bytes dts_read8_at(fd_dtype_stream s,fd_off_t off,int *err);

#if FD_INLINE_DTYPEIO
FD_FASTOP int fd_dtsread_byte(fd_dtype_stream s)
{
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,1))
    return (*(s->bs_bufptr++));
  else return -1;
}

FD_FASTOP int fd_dtsprobe_byte(fd_dtype_stream s)
{
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,1))
    return (*(s->bs_bufptr));
  else return -1;
}

FD_FASTOP unsigned int fd_dtsread_4bytes(fd_dtype_stream s)
{
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+4;
    return bytes;}
  else {
    fd_whoops(fd_UnexpectedEOD);
    return 0;}
}

FD_FASTOP fd_8bytes fd_dtsread_8bytes(fd_dtype_stream s)
{
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,8)) {
    fd_8bytes bytes=fd_get_8bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+8;
    return bytes;}
  else {
    fd_whoops(fd_UnexpectedEOD);
    return 0;}
}

FD_FASTOP int fd_dtsread_bytes
  (fd_dtype_stream s,unsigned char *bytes,int len)
{
  fd_dts_start_read(s);
  /* This is special because we don't use the intermediate buffer
     if the data isn't fully buffered. */
  if (fd_has_bytes(s,len)) {
    memcpy(bytes,s->bs_bufptr,len);
    s->bs_bufptr=s->bs_bufptr+len;
    return len;}
   else return _fd_dtsread_bytes(s,bytes,len);
}
FD_FASTOP int dts_read_bytes
   (fd_dtype_stream s,unsigned char *bytes,int len,u8_mutex *unlock)
{
  dts_start_read(s,FD_STREAM_LOCKED);
  /* This is special because we don't use the intermediate buffer
     if the data isn't fully buffered. */
  if (fd_has_bytes(s,len)) {
    memcpy(bytes,s->bs_bufptr,len);
    s->bs_bufptr=s->bs_bufptr+len;
    if (unlock) u8_unlock_mutex(unlock);
    return len;}
  else return _dts_read_bytes(s,bytes,len,unlock);
}

FD_FASTOP fd_off_t fd_dtsread_off_t(fd_dtype_stream s)
{
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+4;
    return (fd_off_t) bytes;}
  else return (fd_off_t) -1;
}

FD_FASTOP unsigned int fd_dtsread_zint(fd_dtype_stream s)
{
  unsigned int result=0, probe;
  while ((probe=(fd_dtsread_byte(s))))
    if (probe&0x80) result=result<<7|(probe&0x7F);
    else break;
  return result<<7|probe;
}

FD_FASTOP fd_8bytes fd_dtsread_zint8(fd_dtype_stream s)
{
  fd_8bytes result=0, probe;
  while ((probe=(fd_dtsread_byte(s))))
    if (probe&0x80) result=result<<7|(probe&0x7F);
    else break;
  return result<<7|probe;
}

/* Inline writers */

FD_FASTOP int fd_dtswrite_byte(fd_dtype_stream s,int b)
{
  fd_dts_start_write(s);
  if (s->bs_bufptr>=s->bs_buflim)
    if (fd_dtsflush(s)<0) return -1;
  *(s->bs_bufptr++)=b;
  return 1;
}

FD_FASTOP int fd_dtswrite_4bytes(fd_dtype_stream s,unsigned int w)
{
  fd_dts_start_write(s);
  if (s->bs_bufptr+4>=s->bs_buflim)
    if (fd_dtsflush(s)<0) return -1;
  *(s->bs_bufptr++)=w>>24;
  *(s->bs_bufptr++)=((w>>16)&0xFF);
  *(s->bs_bufptr++)=((w>>8)&0xFF);
  *(s->bs_bufptr++)=((w>>0)&0xFF);
  return 4;
}

FD_FASTOP int fd_dtswrite_8bytes(fd_dtype_stream s,unsigned long long w)
{
  fd_dts_start_write(s);
  if (s->bs_bufptr+8>=s->bs_buflim)
    if (fd_dtsflush(s)<0) return -1;
  *(s->bs_bufptr++)=((w>>56)&0xFF);
  *(s->bs_bufptr++)=((w>>48)&0xFF);
  *(s->bs_bufptr++)=((w>>40)&0xFF);
  *(s->bs_bufptr++)=((w>>32)&0xFF);
  *(s->bs_bufptr++)=((w>>24)&0xFF);
  *(s->bs_bufptr++)=((w>>16)&0xFF);
  *(s->bs_bufptr++)=((w>>8)&0xFF);
  *(s->bs_bufptr++)=((w>>0)&0xFF);
  return 8;
}

FD_FASTOP int dts_write_bytes
  (fd_dtype_stream s,const unsigned char *bytes,int n)
{
  fd_dts_start_write(s);
  if (s->bs_bufptr+n>=s->bs_buflim)
    if (fd_dtsflush(s)<0) return -1;
  if (s->bs_bufptr+n>=s->bs_buflim)
    return _dts_write_bytes(s,bytes,n);
  else {
    memcpy(s->bs_bufptr,bytes,n); s->bs_bufptr=s->bs_bufptr+n;}
  return n;
}
static U8_MAYBE_UNUSED int fd_dtswrite_zint(fd_dtype_stream s,unsigned int n)
{
  if ((n < (1<<7))) {
    return fd_dtswrite_byte(s,n);}
  else if ((n < (1<<14))) {
    if (fd_dtswrite_byte(s,((0x80)|(n>>7)))<0) return -1;
    if (fd_dtswrite_byte(s,n&0x7F)<0) return -1;
    return 2;}
  else if (n < (1<<21)) {
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;;
    if (fd_dtswrite_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;;
    if (fd_dtswrite_byte(s,n&0x7F)<0) return -1;
    return 3;}
  else if (n < (1<<28)) {
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_dtswrite_byte(s,n&0x7F)<0) return -1;
    return 4;}
  else {
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_dtswrite_byte(s,n&0x7F)<0) return -1;
    return 5;}
}
static U8_MAYBE_UNUSED int fd_dtswrite_zint8(fd_dtype_stream s,fd_8bytes n)
{
  if (n < (((fd_8bytes)1)<<7)) {
    return fd_dtswrite_byte(s,n);}
  else if (n < (((fd_8bytes)1)<<14)) {
    if (fd_dtswrite_byte(s,((0x80)|(n>>7)))<0) return -1;
    if (fd_dtswrite_byte(s,n&0x7F)<0) return -1;
    return 2;}
  else if (n < (((fd_8bytes)1)<<21)) {
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;;
    if (fd_dtswrite_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;;
    if (fd_dtswrite_byte(s,n&0x7F)<0) return -1;
    return 3;}
  else if (n < (((fd_8bytes)1)<<28)) {
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_dtswrite_byte(s,n&0x7F)<0) return -1;
    return 4;}
  else if (n < (((fd_8bytes)1)<<35)) {
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_dtswrite_byte(s,n&0x7F)<0) return -1;
    return 5;}
  else if (n < (((fd_8bytes)1)<<42)) {
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_dtswrite_byte(s,n&0x7F)<0) return -1;
    return 6;}
  else if (n < (((fd_8bytes)1)<<49)) {
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>42))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_dtswrite_byte(s,n&0x7F)<0) return -1;
    return 7;}
  else if (n < (((fd_8bytes)1)<<56)) {
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>49))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>42))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_dtswrite_byte(s,n&0x7F)<0) return -1;
    return 8;}
  else {
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>56))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>49))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>42))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (fd_dtswrite_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (fd_dtswrite_byte(s,n&0x7F)<0) return -1;
    return 9;}
}
#else /* FD_INLINE_DTYPEIO */
#define fd_dtsread_byte   _fd_dtsread_byte
#define fd_dtprobe_byte   _fd_dtsprobe_byte
#define fd_dtsread_4bytes _fd_dtsread_4bytes
#define fd_dtsread_bytes  _fd_dtsread_bytes
#define fd_dtsread_zint  _fd_dtsread_zint

#define fd_dtswrite_byte   _fd_dtswrite_byte
#define fd_dtswrite_4bytes _fd_dtswrite_4bytes
#define dts_write_bytes    _dts_write_bytes
#define fd_dtswrite_zint  _fd_dtswrite_zint

#endif  /* FD_INLINE_DTYPEIO */
#endif /* FD_DTYPESTREAM_H */
