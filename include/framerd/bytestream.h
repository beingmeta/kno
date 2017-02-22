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

typedef struct FD_BYTESTREAM {
  unsigned char *bufbase, *bufpoint, *buflim;
  int buf_flags; size_t buflen;
  size_t (*buf_fillfn)(fd_byte_inbuf,size_t);
  size_t (*buf_flushfn)(fd_byte_outbuf);
  int stream_fileno;
  u8_mutex stream_lock;
  u8_string streamid; int stream_mallocd;
  fd_off_t stream_filepos, stream_maxpos;} FD_BYTESTREAM;
typedef struct FD_BYTESTREAM *fd_bytestream;

#define FD_BYTESTREAM_BUFSIZ_DEFAULT 32*1024

#define FD_STREAM_FLAGS          (0x1000)
#define FD_STREAM_LOCKED         (FD_STREAM_FLAGS << 0)
#define FD_STREAM_READ_ONLY      (FD_STREAM_FLAGS << 1)
#define FD_STREAM_WRITE_ONLY     (FD_STREAM_FLAGS << 2)
#define FD_STREAM_CAN_SEEK        (FD_STREAM_FLAGS << 3)
#define FD_STREAM_NEEDS_LOCK     (FD_STREAM_FLAGS << 4)
#define FD_STREAM_FILE_LOCKED    (FD_STREAM_FLAGS << 5)
#define FD_STREAM_SOCKET         (FD_STREAM_FLAGS << 6)
#define FD_STREAM_DOSYNC         (FD_STREAM_FLAGS << 7)

typedef enum FD_BYTESTREAM_MODE {
  FD_BYTESTREAM_READ, /* Read only, must exist */
  FD_BYTESTREAM_MODIFY, /* Read/write, must exist */
  FD_BYTESTREAM_WRITE, /* Read/write, may exist */
  FD_BYTESTREAM_CREATE /* Read/write, truncated */
} fd_bytestream_mode;

typedef enum FD_BYTEFLOW {
  fd_byteflow_read = 0,
  fd_byteflow_write = 1 } fd_byteflow;

#define FD_BYTESTREAM_ISREADING(s) (!(U8_BITP((s->buf_flags),(FD_IS_WRITING))))
#define FD_BYTESTREAM_ISWRITING(s) (U8_BITP((s->buf_flags),(FD_IS_WRITING)))

FD_EXPORT size_t fd_fill_bytestream(fd_bytestream df,size_t n);

FD_EXPORT
struct FD_BYTESTREAM *fd_init_bytestream(fd_bytestream s,int fileno,int bufsiz);

FD_EXPORT
fd_bytestream fd_init_file_bytestream (fd_bytestream stream,
				       u8_string filename,
				       fd_bytestream_mode mode,int bufsiz);

FD_EXPORT fd_bytestream fd_open_dtype_file
  (u8_string filename,fd_bytestream_mode mode,int bufsiz);

#define fd_bytestream_open(filename,mode) \
  fd_open_dtype_file(filename,mode,FD_BYTESTREAM_BUFSIZ_DEFAULT)

#define FD_BYTESTREAM_FREE    1
#define FD_BYTESTREAM_NOCLOSE 2
#define FD_BYTESTREAM_NOFLUSH 4
#define FD_BYTESTREAM_CLOSE_FULL FD_BYTESTREAM_FREE

FD_EXPORT void fd_close_bytestream(fd_bytestream s,int flags);
FD_EXPORT void fd_free_bytestream(fd_bytestream s,int flags);

FD_EXPORT fdtype fd_read_dtype_from_file(u8_string filename);
FD_EXPORT ssize_t _fd_write_dtype_to_file(fdtype,u8_string,size_t,int);
FD_EXPORT ssize_t fd_write_dtype_to_file(fdtype obj,u8_string filename);
FD_EXPORT ssize_t fd_write_ztype_to_file(fdtype obj,u8_string filename);
FD_EXPORT ssize_t fd_add_dtype_to_file(fdtype obj,u8_string filename);

FD_EXPORT long long fd_read_4bytes_at(fd_bytestream s,fd_off_t off);
FD_EXPORT fd_8bytes fd_read_8bytes_at(fd_bytestream s,fd_off_t off,int *err);
FD_EXPORT int fd_write_4bytes_at(fd_bytestream s,fd_4bytes w,fd_off_t off);
FD_EXPORT int fd_write_8bytes_at(fd_bytestream s,fd_8bytes w,fd_off_t off);

FD_EXPORT fdtype fd_zread_dtype(struct FD_BYTE_INBUF *in);
FD_EXPORT int fd_zwrite_dtype(struct FD_BYTE_OUTBUF *s,fdtype x);
FD_EXPORT int fd_zwrite_dtypes(struct FD_BYTE_OUTBUF *s,fdtype x);

FD_EXPORT int fd_write_ints(fd_bytestream s,int len,unsigned int *words);
FD_EXPORT int fd_read_ints(fd_bytestream s,int len,unsigned int *words);

FD_EXPORT int fd_set_direction(fd_bytestream s,fd_byteflow direction);

/* File positions */

FD_EXPORT fd_byte_inbuf _fd_readbuf(fd_bytestream s);
FD_EXPORT fd_byte_outbuf _fd_writebuf(fd_bytestream s);
FD_EXPORT fd_byte_outbuf _fd_start_write(fd_bytestream s,fd_off_t pos);
FD_EXPORT fd_byte_inbuf _fd_start_read(fd_bytestream s,fd_off_t pos);
FD_EXPORT fd_off_t _fd_setpos(fd_bytestream s,fd_off_t pos);
FD_EXPORT fd_off_t _fd_endpos(fd_bytestream s);

FD_EXPORT fd_off_t _fd_getpos(fd_bytestream s);

#if FD_INLINE_DTYPEIO

FD_FASTOP fd_off_t fd_getpos(fd_bytestream s)
{
  if (((s)->buf_flags)&FD_STREAM_CAN_SEEK) {
    if (((s)->stream_filepos)>=0) {
      if (FD_BYTESTREAM_ISREADING(s))
	/* If you're reading, subtract what's buffered from the file pos. */
	return (((s)->stream_filepos)-(((s)->buflim)-((s)->bufpoint)));
      /* If you're writing, add what's buffered to the file pos. */
      else return (((s)->stream_filepos)+(((s)->bufpoint)-((s)->bufbase)));}
    else return _fd_getpos(s);}
  else return -1;
}

FD_FASTOP fd_off_t fd_setpos(fd_bytestream s,fd_off_t pos)
{
  /* Have the common version do the error handling. */
  if ((((s->buf_flags)&FD_STREAM_CAN_SEEK) == 0)||(pos<0))
    return _fd_setpos(s,pos);

  if ((s->stream_filepos>=0)&&
      (!((s->buf_flags)&(FD_IS_WRITING)))&&
      (pos<s->stream_filepos)&&
      (pos>=(s->stream_filepos-(s->buflim-s->bufbase)))) {
    fd_off_t delta=pos-s->stream_filepos;
    s->bufpoint=s->buflim+delta;
    return pos;}
  else if ((s->stream_filepos>=0)&&(fd_getpos(s)==pos))
    return pos;
  else return _fd_setpos(s,pos);
}
FD_FASTOP fd_off_t fd_endpos(fd_bytestream s)
{
  /* Have the common version do the error handling. */
  if (((s->buf_flags)&FD_STREAM_CAN_SEEK) == 0)
    return _fd_endpos(s);
  else if ((s->stream_filepos>=0)&&(s->stream_maxpos>=0)&&
	   (s->stream_filepos==s->stream_maxpos))
    if ((s->buf_flags)&(FD_IS_WRITING))
      return s->stream_maxpos+(s->bufpoint-s->bufbase);
    else return s->stream_maxpos;
  else return _fd_endpos(s);
}
FD_FASTOP fd_byte_inbuf fd_readbuf(fd_bytestream s)
{
  if ((s->buf_flags)&(FD_IS_WRITING))
    fd_set_direction(s,fd_byteflow_read);
  return (struct FD_BYTE_INBUF *)s;
}

FD_FASTOP fd_byte_outbuf fd_writebuf(fd_bytestream s)
{
  if (!((s->buf_flags)&(FD_IS_WRITING)))
    fd_set_direction(s,fd_byteflow_write);
  return (struct FD_BYTE_OUTBUF *)s;
}
FD_FASTOP fd_byte_inbuf fd_start_read(fd_bytestream s,fd_off_t pos)
{
  if ((s->buf_flags)&(FD_IS_WRITING))
    fd_set_direction(s,fd_byteflow_read);
  if (pos<0)
    fd_setpos(s,s->stream_maxpos+pos);
  else fd_setpos(s,pos);
  return (struct FD_BYTE_INBUF *)s;
}

FD_FASTOP fd_byte_outbuf fd_start_write(fd_bytestream s,fd_off_t pos)
{
  if (!((s->buf_flags)&(FD_IS_WRITING)))
    fd_set_direction(s,fd_byteflow_write);
  if (pos<0) fd_endpos(s);
  else fd_setpos(s,pos);
  return (struct FD_BYTE_OUTBUF *)s;
}
#else
#define fd_getpos(s) _fd_getpos(s)
#define fd_setpos(s,p) _fd_setpos(s,p)
#define fd_endpos(s) _fd_endpos(s)
#define fd_readbuf(s) _fd_readbuf(s)
#define fd_writebuf(s) _fd_writebuf(s)
#define fd_start_read(s,p) _fd_start_read(s,p)
#define fd_start_write(s,p) _fd_start_write(s,p)
#endif

/* Structure functions and macros */

FD_EXPORT fd_off_t _fd_setpos(fd_bytestream s,fd_off_t pos);
FD_EXPORT fd_off_t _fd_endpos(fd_bytestream s);
FD_EXPORT fd_off_t fd_movepos(fd_bytestream s,fd_off_t delta);

FD_EXPORT int bytestream_set_read(fd_bytestream s,int read);
FD_EXPORT int fd_set_read(fd_bytestream s,int read);

FD_EXPORT int fd_flush_bytestream(fd_bytestream s);

FD_EXPORT void fd_lock_stream(fd_bytestream s);
FD_EXPORT void fd_unlock_stream(fd_bytestream s);

FD_EXPORT int fd_lockfile(fd_bytestream s);
FD_EXPORT int fd_unlockfile(fd_bytestream s);

FD_EXPORT void fd_bytestream_setbuf(fd_bytestream s,int bufsiz);

#endif /* FD_BYTESTREAM_H */
