/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FD_BYTESTREAM_H
#define FD_BYTESTREAM_H 1
#ifndef FRAMERD_BYTESTREAM_H_INFO
#define FRAMERD_BYTESTREAM_H_INFO "include/framerd/bytestream.h"
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

#define FD_BYTESTREAM_BUFSIZ_DEFAULT 32*1024

#define FD_BYTESTREAM_FLAG_BASE      (1<<12)
#define FD_BYTESTREAM_READING        (FD_BYTESTREAM_FLAG_BASE * 1)
#define FD_BYTESTREAM_READ_ONLY      (FD_BYTESTREAM_FLAG_BASE * 2)
#define FD_BYTESTREAM_CANSEEK        (FD_BYTESTREAM_FLAG_BASE * 4)
#define FD_BYTESTREAM_NEEDS_LOCK     (FD_BYTESTREAM_FLAG_BASE * 8)
#define FD_BYTESTREAM_LOCKED         (FD_BYTESTREAM_FLAG_BASE * 16)
#define FD_BYTESTREAM_SOCKET         (FD_BYTESTREAM_FLAG_BASE * 32)
#define FD_BYTESTREAM_DOSYNC         (FD_BYTESTREAM_FLAG_BASE * 64)

typedef enum FD_BYTESTREAM_MODE {
  FD_BYTESTREAM_READ, /* Read only, must exist */
  FD_BYTESTREAM_MODIFY, /* Read/write, must exist */
  FD_BYTESTREAM_WRITE, /* Read/write, may exist */
  FD_BYTESTREAM_CREATE /* Read/write, truncated */
} fd_bytestream_mode;

typedef struct FD_BYTESTREAM {
  unsigned char *bs_bufstart, *bs_bufptr, *bs_buflim;
  int bs_flags;
  int (*bs_fillfn)(struct FD_BYTESTREAM *,int);
  int (*bs_flushfn)(struct FD_BYTESTREAM *);
  u8_string bytestream_idstring; int bytestream_mallocd, bytestream_bufsiz;
  fd_off_t bytestream_diskpos, bytestream_maxpos;
  u8_mutex stream_lock;
  int fd_fileno;} FD_BYTESTREAM;
typedef struct FD_BYTESTREAM *fd_bytestream;

FD_EXPORT struct FD_BYTESTREAM *fd_init_bytestream
  (struct FD_BYTESTREAM *s,int sock,int bufsiz);

FD_EXPORT fd_bytestream fd_init_file_bytestream
   (struct FD_BYTESTREAM *stream,
    u8_string filename,fd_bytestream_mode mode,int bufsiz);

FD_EXPORT fd_bytestream fd_open_dtype_file
  (u8_string filename,fd_bytestream_mode mode,int bufsiz);
#define fd_bytestream_open(filename,mode) \
  fd_open_dtype_file(filename,mode,FD_BYTESTREAM_BUFSIZ_DEFAULT)

#define FD_BYTESTREAM_UNLOCK 1
#define FD_STREAM_LOCKED 0

#define FD_BYTESTREAM_FREE    1
#define FD_BYTESTREAM_NOCLOSE 2
#define FD_BYTESTREAM_NOFLUSH 4
#define FD_BYTESTREAM_CLOSE_FULL FD_BYTESTREAM_FREE
FD_EXPORT void fd_bytestream_close(fd_bytestream s,int flags);
FD_EXPORT void fd_bytestream_free(fd_bytestream s,int flags);

FD_EXPORT fdtype fd_read_dtype_from_file(u8_string filename);
FD_EXPORT ssize_t _fd_write_dtype_to_file(fdtype,u8_string,size_t,int);
FD_EXPORT ssize_t fd_write_dtype_to_file(fdtype obj,u8_string filename);
FD_EXPORT ssize_t fd_write_ztype_to_file(fdtype obj,u8_string filename);
FD_EXPORT ssize_t fd_add_dtype_to_file(fdtype obj,u8_string filename);

FD_EXPORT long long fd_read_4bytes_at(fd_bytestream s,fd_off_t off);
FD_EXPORT fd_8bytes fd_read_8bytes_at(fd_bytestream s,fd_off_t off,int *err);
FD_EXPORT int fd_write_4bytes_at(fd_bytestream s,fd_4bytes w,fd_off_t off);
FD_EXPORT int fd_write_8bytes_at(fd_bytestream s,fd_8bytes w,fd_off_t off);

/* Structure functions and macros */

FD_EXPORT fd_off_t _fd_getpos(fd_bytestream s);
FD_EXPORT fd_off_t _bytestream_getpos(fd_bytestream s,int unlock);
#define fd_getpos(s) \
  ((((s)->bs_flags)&FD_BYTESTREAM_CANSEEK) ?				\
   ((((s)->bytestream_diskpos)>=0) ?						\
    ((FD_BYTESTREAM_ISREADING(s)) ?						\
     (((s)->bytestream_diskpos)-(((s)->bs_buflim)-((s)->bs_bufptr))) :		\
     (((s)->bytestream_diskpos)+(((s)->bs_bufptr)-((s)->bs_bufstart)))) :	\
    (_fd_getpos(s)))							\
   : (-1))
#define bytestream_getpos(s,unlock)						\
  ((((s)->bs_flags)&FD_BYTESTREAM_CANSEEK) ?				\
   ((((s)->bytestream_diskpos)>=0) ?						\
    ((FD_BYTESTREAM_ISREADING(s)) ?						\
     (((s)->bytestream_diskpos)-(((s)->bs_buflim)-((s)->bs_bufptr))) :		\
     (((s)->bytestream_diskpos)+(((s)->bs_bufptr)-((s)->bs_bufstart)))) :	\
    (_bytestream_getpos(s,unlock)))						\
   : (-1))

FD_EXPORT fd_off_t bytestream_setpos(fd_bytestream s,fd_off_t pos);
FD_EXPORT fd_off_t bytestream_movepos(fd_bytestream s,int delta);
FD_EXPORT fd_off_t bytestream_endpos(fd_bytestream s);
FD_EXPORT fd_off_t fd_setpos(fd_bytestream s,fd_off_t pos);
FD_EXPORT fd_off_t fd_movepos(fd_bytestream s,int delta);
FD_EXPORT fd_off_t fd_endpos(fd_bytestream s);

FD_EXPORT int bytestream_set_read(fd_bytestream s,int read);
FD_EXPORT int bytestream_flush(fd_bytestream s);
FD_EXPORT int fd_set_read(fd_bytestream s,int read);
FD_EXPORT int fd_bytestream_flush(fd_bytestream s);

FD_EXPORT void bytestream_lock(struct FD_BYTESTREAM *s);
FD_EXPORT void bytestream_unlock(struct FD_BYTESTREAM *s);

FD_EXPORT int fd_bytestream_lockfile(fd_bytestream s);
FD_EXPORT int fd_bytestream_unlockfile(fd_bytestream s);
FD_EXPORT int bytestream_lockfile(fd_bytestream s);
FD_EXPORT int bytestream_unlockfile(fd_bytestream s);

#define fd_bytestream_lockfile(s) fd_bytestream_lockfile(s)
#define fd_bytestream_unlockfile(s) fd_bytestream_unlockfile(s)

#define FD_BYTESTREAM_ISREADING(s) (U8_BITP((s->bs_flags),(FD_BYTESTREAM_READING)))
#define FD_BYTESTREAM_ISWRITING(s) (!(U8_BITP((s->bs_flags),(FD_BYTESTREAM_READING))))

FD_EXPORT void fd_bytestream_bufsize(fd_bytestream s,int bufsiz);

#define bytestream_start_read(s)			      \
  if ((((s)->bs_flags)&FD_BYTESTREAM_READING) == 0) \
    if (bytestream_set_read(s,1)<0) return -1;
#define bytestream_start_write(s)		       \
  if (((s)->bs_flags)&FD_BYTESTREAM_READING) \
    if (bytestream_set_read(s,0)<0) return -1;

/* Readers and writers */

FD_EXPORT fdtype bytestream_read_dtype(fd_bytestream s,int unlock);
FD_EXPORT fdtype fd_bytestream_read_dtype(fd_bytestream s);

FD_EXPORT int bytestream_write_dtype(fd_bytestream s,fdtype x,int unlock);
FD_EXPORT int fd_bytestream_write_dtype(fd_bytestream s,fdtype x);

FD_EXPORT int bytestream_read_ints(fd_bytestream s,int len,unsigned int *words);
FD_EXPORT int fd_bytestream_read_ints(fd_bytestream s,int len,unsigned int *words);

FD_EXPORT int bytestream_write_ints(fd_bytestream s,int len,unsigned int *words);
FD_EXPORT int fd_bytestream_write_ints(fd_bytestream s,int len,unsigned int *words);

FD_EXPORT int bytestream_zwrite_dtype(struct FD_BYTESTREAM *s,fdtype x,int unlock);
FD_EXPORT int fd_zwrite_dtype(struct FD_BYTESTREAM *s,fdtype x);
FD_EXPORT fdtype bytestream_zread_dtype(struct FD_BYTESTREAM *s,int unlock);
FD_EXPORT fdtype fd_zread_dtype(struct FD_BYTESTREAM *s);

FD_EXPORT int bytestream_zwrite_dtypes(struct FD_BYTESTREAM *s,fdtype x,int unlock);
FD_EXPORT int fd_zwrite_dtypes(struct FD_BYTESTREAM *s,fdtype x);

/* All of these assume the stream's mutex is locked */

FD_EXPORT int _bytestream_read_byte(struct FD_BYTESTREAM *stream);
FD_EXPORT int _bytestream_probe_byte(struct FD_BYTESTREAM *stream);
FD_EXPORT fd_4bytes _bytestream_read_4bytes(struct FD_BYTESTREAM *stream);
FD_EXPORT fd_8bytes _fd_bytestream_read_8bytes(struct FD_BYTESTREAM *stream);
FD_EXPORT int _bytestream_read_bytes
  (struct FD_BYTESTREAM *stream,unsigned char *bytes,int len,u8_mutex *unlock);
FD_EXPORT fd_off_t _bytestream_read_off_t(struct FD_BYTESTREAM *stream);
FD_EXPORT fd_8bytes _bytestream_read_zint(fd_bytestream s);

FD_EXPORT int _bytestream_write_byte(struct FD_BYTESTREAM *stream,int b);
FD_EXPORT int _bytestream_write_4bytes(struct FD_BYTESTREAM *stream,fd_4bytes w);
FD_EXPORT int _bytestream_write_bytes(struct FD_BYTESTREAM *stream,
				const unsigned char *bytes,int len);

FD_EXPORT int _bytestream_write_zint(struct FD_BYTESTREAM *stream,fd_8bytes w);

FD_EXPORT int bytestream_write4_at(fd_bytestream s,fd_4bytes w,fd_off_t off);
FD_EXPORT int bytestream_write8_at(fd_bytestream s,fd_8bytes w,fd_off_t off);
FD_EXPORT long long bytestream_read4_at(fd_bytestream s,fd_off_t off);
FD_EXPORT fd_8bytes bytestream_read8_at(fd_bytestream s,fd_off_t off,int *err);

#if FD_INLINE_DTYPEIO
FD_FASTOP int bytestream_read_byte(fd_bytestream s)
{
  bytestream_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,1))
    return (*(s->bs_bufptr++));
  else return -1;
}

FD_FASTOP int bytestream_probe_byte(fd_bytestream s)
{
  bytestream_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,1))
    return (*(s->bs_bufptr));
  else return -1;
}

FD_FASTOP fd_4bytes bytestream_read_4bytes(fd_bytestream s)
{
  bytestream_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+4;
    return bytes;}
  else {
    fd_whoops(fd_UnexpectedEOD);
    return 0;}
}

FD_FASTOP fd_8bytes bytestream_read_8bytes(fd_bytestream s)
{
  bytestream_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,8)) {
    fd_8bytes bytes=fd_get_8bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+8;
    return bytes;}
  else {
    fd_whoops(fd_UnexpectedEOD);
    return 0;}
}

FD_FASTOP int bytestream_read_bytes
   (fd_bytestream s,unsigned char *bytes,int len,u8_mutex *unlock)
{
  bytestream_start_read(s);
  /* This is special because we don't use the intermediate buffer
     if the data isn't fully buffered. */
  if (fd_has_bytes(s,len)) {
    memcpy(bytes,s->bs_bufptr,len);
    s->bs_bufptr=s->bs_bufptr+len;
    if (unlock) u8_unlock_mutex(unlock);
    return len;}
  else return _bytestream_read_bytes(s,bytes,len,unlock);
}

FD_FASTOP fd_off_t bytestream_read_off_t(fd_bytestream s)
{
  bytestream_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+4;
    return (fd_off_t) bytes;}
  else return (fd_off_t) -1;
}

FD_FASTOP fd_8bytes bytestream_read_zint(fd_bytestream s)
{
  unsigned int result=0, probe;
  while ((probe=(bytestream_read_byte(s))))
    if (probe&0x80) result=result<<7|(probe&0x7F);
    else break;
  return result<<7|probe;
}

/* Inline writers */

FD_FASTOP int bytestream_write_byte(fd_bytestream s,int b)
{
  bytestream_start_write(s);
  if (s->bs_bufptr>=s->bs_buflim)
    if (fd_bytestream_flush(s)<0) return -1;
  *(s->bs_bufptr++)=b;
  return 1;
}

FD_FASTOP int bytestream_write_4bytes(fd_bytestream s,unsigned int w)
{
  bytestream_start_write(s);
  if (s->bs_bufptr+4>=s->bs_buflim)
    if (fd_bytestream_flush(s)<0) return -1;
  *(s->bs_bufptr++)=w>>24;
  *(s->bs_bufptr++)=((w>>16)&0xFF);
  *(s->bs_bufptr++)=((w>>8)&0xFF);
  *(s->bs_bufptr++)=((w>>0)&0xFF);
 return 4;
}

FD_FASTOP int bytestream_write_8bytes(fd_bytestream s,unsigned long long w)
{
  bytestream_start_write(s);
  if (s->bs_bufptr+8>=s->bs_buflim)
    if (fd_bytestream_flush(s)<0) return -1;
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

FD_FASTOP int bytestream_write_bytes
  (fd_bytestream s,const unsigned char *bytes,int n)
{
  bytestream_start_write(s);
  if (s->bs_bufptr+n>=s->bs_buflim)
    if (fd_bytestream_flush(s)<0) return -1;
  if (s->bs_bufptr+n>=s->bs_buflim)
    return _bytestream_write_bytes(s,bytes,n);
  else {
    memcpy(s->bs_bufptr,bytes,n); s->bs_bufptr=s->bs_bufptr+n;}
  return n;
}
static U8_MAYBE_UNUSED int bytestream_write_zint(fd_bytestream s,fd_8bytes n)
{
  if (n < (((fd_8bytes)1)<<7)) {
    return bytestream_write_byte(s,n);}
  else if (n < (((fd_8bytes)1)<<14)) {
    if (bytestream_write_byte(s,((0x80)|(n>>7)))<0) return -1;
    if (bytestream_write_byte(s,n&0x7F)<0) return -1;
    return 2;}
  else if (n < (((fd_8bytes)1)<<21)) {
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;;
    if (bytestream_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;;
    if (bytestream_write_byte(s,n&0x7F)<0) return -1;
    return 3;}
  else if (n < (((fd_8bytes)1)<<28)) {
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (bytestream_write_byte(s,n&0x7F)<0) return -1;
    return 4;}
  else if (n < (((fd_8bytes)1)<<35)) {
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (bytestream_write_byte(s,n&0x7F)<0) return -1;
    return 5;}
  else if (n < (((fd_8bytes)1)<<42)) {
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (bytestream_write_byte(s,n&0x7F)<0) return -1;
    return 6;}
  else if (n < (((fd_8bytes)1)<<49)) {
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>42))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (bytestream_write_byte(s,n&0x7F)<0) return -1;
    return 7;}
  else if (n < (((fd_8bytes)1)<<56)) {
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>49))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>42))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (bytestream_write_byte(s,n&0x7F)<0) return -1;
    return 8;}
  else {
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>56))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>49))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>42))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>35))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>28))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>21))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7F&(n>>14))))<0) return -1;
    if (bytestream_write_byte(s,((0x80)|(0x7f&(n>>7))))<0) return -1;
    if (bytestream_write_byte(s,n&0x7F)<0) return -1;
    return 9;}
}
#else /* FD_INLINE_DTYPEIO */
#define bytestream_read_byte      _bytestream_read_byte
#define bytestream_probe_byte     _bytestream_probe_byte
#define bytestream_read_4bytes    _bytestream_read_4bytes
#define bytestream_read_8bytes    _bytestream_read_8bytes
#define bytestream_read_bytes     _bytestream_read_bytes
#define bytestream_read_zint      _bytestream_read_zint

#define bytestream_write_byte   _bytestream_write_byte
#define bytestream_write_4bytes _bytestream_write_4bytes
#define bytestream_write_8bytes _bytestream_write_8bytes
#define bytestream_write_bytes  _bytestream_write_bytes
#define bytestream_write_zint   _bytestream_write_zint

#endif  /* FD_INLINE_DTYPEIO */
#endif /* FD_BYTESTREAM_H */
