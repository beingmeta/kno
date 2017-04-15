/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FD_STREAMS_H
#define FD_STREAMS_H 1
#ifndef FRAMERD_STREAMS_H_INFO
#define FRAMERD_STREAMS_H_INFO "include/framerd/streams.h"
#endif

#include "dtype.h"
#include "dtypeio.h"

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#ifndef FD_STREAM_BUFSIZE
#define FD_STREAM_BUFSIZE 8000
#endif

#ifndef FD_FILESTREAM_BUFSIZE
#define FD_FILESTREAM_BUFSIZE 16000
#endif

#ifndef FD_NETWORK_BUFSIZE
#define FD_NETWORK_BUFSIZE 16000
#endif

#ifndef FD_INLINE_STREAMIO
#define FD_INLINE_STREAMIO 0
#endif

FD_EXPORT size_t fd_stream_bufsize;
FD_EXPORT size_t fd_network_bufsize;
FD_EXPORT size_t fd_filestream_bufsize;

typedef int fd_stream_flags;

#define FD_STREAM_NOFLAGS        (0x00)
#define FD_STREAM_FLAGS          (0x1)
#define FD_STREAM_LOCKED         (FD_STREAM_FLAGS << 0)
#define FD_STREAM_READ_ONLY      (FD_STREAM_FLAGS << 1)
#define FD_STREAM_WRITE_ONLY     (FD_STREAM_FLAGS << 2)
#define FD_STREAM_CAN_SEEK       (FD_STREAM_FLAGS << 3)
#define FD_STREAM_NEEDS_LOCK     (FD_STREAM_FLAGS << 4)
#define FD_STREAM_FILE_LOCKED    (FD_STREAM_FLAGS << 5)
#define FD_STREAM_SOCKET         (FD_STREAM_FLAGS << 6)
#define FD_STREAM_DOSYNC         (FD_STREAM_FLAGS << 7)
#define FD_STREAM_IS_CONSED      (FD_STREAM_FLAGS << 8)
#define FD_STREAM_OWNS_FILENO    (FD_STREAM_FLAGS << 9)
#define FD_STREAM_MMAPPED        (FD_STREAM_FLAGS << 10)

#define FD_DEFAULT_FILESTREAM_FLAGS \
  (FD_STREAM_CAN_SEEK|FD_STREAM_OWNS_FILENO)

typedef struct FD_STREAM {
  FD_CONS_HEADER;
  fd_stream_flags stream_flags;
  int stream_fileno;
  fd_off_t stream_filepos, stream_maxpos;
  u8_string streamid;
  union {
    struct FD_INBUF in;
    struct FD_OUTBUF out;
    struct FD_RAWBUF raw;} buf;
  long long stream_locker;
  u8_mutex stream_lock;} FD_STREAM;
typedef struct FD_STREAM *fd_stream;

typedef enum FD_STREAM_MODE {
  FD_FILE_READ, /* Read only, must exist */
  FD_FILE_MODIFY, /* Read/write, must exist */
  FD_FILE_WRITE, /* Read/write, may exist */
  FD_FILE_CREATE /* Read/write, truncated */
} fd_stream_mode;

typedef enum FD_BYTEFLOW {
  fd_byteflow_read = 0,
  fd_byteflow_write = 1 } fd_byteflow;

#define FD_STREAM_ISREADING(s) \
  (!(U8_BITP((s->buf.raw.buf_flags),(FD_IS_WRITING))))
#define FD_STREAM_ISWRITING(s) \
  (U8_BITP((s->buf.raw.buf_flags),(FD_IS_WRITING)))

FD_EXPORT ssize_t fd_fill_stream(fd_stream df,size_t n);

FD_EXPORT
struct FD_STREAM *fd_init_stream(fd_stream s,
				 u8_string id,
				 int fileno,
				 int flags,
				 ssize_t bufsiz);

FD_EXPORT
fd_stream fd_init_file_stream (fd_stream stream,
			       u8_string filename,
			       fd_stream_mode mode,
			       fd_stream_flags flags,
			       ssize_t bufsiz);

FD_EXPORT fd_stream fd_open_file(u8_string filename,fd_stream_mode mode);

typedef enum fd_streamop {
  fd_stream_close,
  fd_stream_setbuf,
  fd_stream_lockfile,
  fd_stream_unlockfile,
  fd_stream_setread,
  fd_stream_setwrite } fd_streamop;

#define FD_STREAM_FREEDATA    1
#define FD_STREAM_NOCLOSE 2
#define FD_STREAM_NOFLUSH 4
#define FD_STREAM_CLOSE_FULL FD_STREAM_FREEDATA

FD_EXPORT void fd_close_stream(fd_stream s,int flags);
FD_EXPORT void fd_free_stream(fd_stream s);

FD_EXPORT fdtype fd_read_dtype_from_file(u8_string filename);
FD_EXPORT ssize_t _fd_write_dtype_to_file(fdtype,u8_string,size_t,int);
FD_EXPORT ssize_t fd_write_dtype_to_file(fdtype obj,u8_string filename);
FD_EXPORT ssize_t fd_write_ztype_to_file(fdtype obj,u8_string filename);
FD_EXPORT ssize_t fd_add_dtype_to_file(fdtype obj,u8_string filename);

FD_EXPORT long long fd_read_4bytes_at(fd_stream s,fd_off_t off);
FD_EXPORT fd_8bytes fd_read_8bytes_at(fd_stream s,fd_off_t off,int *err);
FD_EXPORT int fd_write_4bytes_at(fd_stream s,fd_4bytes w,fd_off_t off);
FD_EXPORT int fd_write_8bytes_at(fd_stream s,fd_8bytes w,fd_off_t off);

FD_EXPORT fdtype fd_zread_dtype(struct FD_INBUF *in);
FD_EXPORT int fd_zwrite_dtype(struct FD_OUTBUF *s,fdtype x);
FD_EXPORT int fd_zwrite_dtypes(struct FD_OUTBUF *s,fdtype x);

FD_EXPORT int fd_write_ints(fd_stream s,int len,unsigned int *words);
FD_EXPORT int fd_read_ints(fd_stream s,int len,unsigned int *words);

FD_EXPORT int fd_set_direction(fd_stream s,fd_byteflow direction);
FD_EXPORT fdtype fd_streamctl(fd_stream s,fd_streamop,void *data);

/* Stream position operations */

FD_EXPORT fd_inbuf _fd_readbuf(fd_stream s);
FD_EXPORT fd_outbuf _fd_writebuf(fd_stream s);
FD_EXPORT fd_outbuf _fd_start_write(fd_stream s,fd_off_t pos);
FD_EXPORT fd_inbuf _fd_start_read(fd_stream s,fd_off_t pos);
FD_EXPORT fd_off_t _fd_setpos(fd_stream s,fd_off_t pos);
FD_EXPORT fd_off_t _fd_endpos(fd_stream s);

FD_EXPORT fd_off_t _fd_getpos(fd_stream s);

FD_EXPORT ssize_t fd_read_block(fd_stream s,unsigned char *buf,
				size_t count,fd_off_t offset,
				int stream_locked);

#if FD_INLINE_BUFIO

FD_FASTOP fd_off_t fd_getpos(fd_stream s)
{
  if (((s)->stream_flags)&FD_STREAM_CAN_SEEK) {
    if (((s)->stream_filepos)>=0) {
      if (FD_STREAM_ISREADING(s))
	/* If you're reading, subtract what's buffered from the file pos. */
	return (((s)->stream_filepos)-(((s)->buf.raw.buflim)-((s)->buf.raw.bufpoint)));
      /* If you're writing, add what's buffered to the file pos. */
      else return (((s)->stream_filepos)+(((s)->buf.raw.bufpoint)-((s)->buf.raw.buffer)));}
    else return _fd_getpos(s);}
  else return -1;
}

FD_FASTOP fd_off_t fd_setpos(fd_stream s,fd_off_t pos)
{
  /* Have the linked version do the error handling. */
  if ((((s->stream_flags)&FD_STREAM_CAN_SEEK) == 0)||(pos<0))
    return _fd_setpos(s,pos);
  else if ((s->stream_filepos>=0)&&
	   (!((s->buf.raw.buf_flags)&(FD_IS_WRITING)))&&
	   (pos<s->stream_filepos)&&
	   (pos>=(s->stream_filepos-(s->buf.raw.buflim-s->buf.raw.buffer)))) {
    /* The location is in the read buffer, so just move the pointer */
    fd_off_t delta = pos-s->stream_filepos;
    s->buf.raw.bufpoint = s->buf.raw.buflim+delta;
    return pos;}
  else if ((s->stream_filepos>=0)&&(fd_getpos(s) == pos))
    return pos;
  else return _fd_setpos(s,pos);
}
FD_FASTOP fd_off_t fd_endpos(fd_stream s)
{
  /* Have the common version do the error handling. */
  if (((s->stream_flags)&FD_STREAM_CAN_SEEK) == 0)
    return _fd_endpos(s);
  else if ((s->stream_filepos>=0)&&(s->stream_maxpos>=0)&&
	   (s->stream_filepos == s->stream_maxpos))
    if ((s->buf.raw.buf_flags)&(FD_IS_WRITING))
      return s->stream_maxpos+(s->buf.raw.bufpoint-s->buf.raw.buffer);
    else return s->stream_maxpos;
  else return _fd_endpos(s);
}
FD_FASTOP fd_inbuf fd_readbuf(fd_stream s)
{
  if ((s->buf.raw.buf_flags)&(FD_IS_WRITING))
    fd_set_direction(s,fd_byteflow_read);
  return &(s->buf.in);
}

FD_FASTOP fd_outbuf fd_writebuf(fd_stream s)
{
  if (!((s->buf.raw.buf_flags)&(FD_IS_WRITING)))
    fd_set_direction(s,fd_byteflow_write);
  return &(s->buf.out);
}
FD_FASTOP fd_inbuf fd_start_read(fd_stream s,fd_off_t pos)
{
  if ((s->buf.raw.buf_flags)&(FD_IS_WRITING))
    fd_set_direction(s,fd_byteflow_read);
  if (pos<0)
    fd_setpos(s,s->stream_maxpos+pos);
  else fd_setpos(s,pos);
  return &(s->buf.in);
}

FD_FASTOP fd_outbuf fd_start_write(fd_stream s,fd_off_t pos)
{
  if (!((s->buf.raw.buf_flags)&(FD_IS_WRITING)))
    fd_set_direction(s,fd_byteflow_write);
  if (pos<0) fd_endpos(s);
  else fd_setpos(s,pos);
  return &(s->buf.out);
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

#define fd_streambuf(stream) (&((stream)->buf.raw))

/* Structure functions and macros */

FD_EXPORT fd_off_t _fd_setpos(fd_stream s,fd_off_t pos);
FD_EXPORT fd_off_t _fd_endpos(fd_stream s);
FD_EXPORT fd_off_t fd_movepos(fd_stream s,fd_off_t delta);

FD_EXPORT int stream_set_read(fd_stream s,int read);
FD_EXPORT int fd_set_read(fd_stream s,int read);

FD_EXPORT ssize_t fd_flush_stream(fd_stream s);

FD_EXPORT int fd_lockfile(fd_stream s);
FD_EXPORT int fd_unlockfile(fd_stream s);

FD_EXPORT void fd_stream_setbufsize(fd_stream s,size_t bufsiz);

FD_EXPORT int _fd_lock_stream(fd_stream s);
FD_EXPORT int _fd_unlock_stream(fd_stream s);

#if FD_INLINE_STREAMIO
FD_FASTOP int fd_lock_stream(fd_stream s)
{
  long long tid = u8_threadid();
  if (s->stream_locker == tid) {
    u8_string id = s->streamid;
    u8_log(LOGCRIT,"RecursiveStreamLock",
	   "Recursively locking stream %s%s0x%llx",
	   ((id)?(id):(U8S0())),((id)?((u8_string)" "):(U8S0())),
	   (U8_PTR2INT(s)));
    u8_seterr("RecursiveStreamLock","fd_lock_stream",id);
    return -1;}
  else {
    int rv = u8_lock_mutex(&(s->stream_lock));
    if (rv) {
      u8_seterr("MutexLockFailed","fd_lock_stream",s->streamid);
      return -1;}
    s->stream_locker = tid;
    return 1;}
}
FD_FASTOP int fd_unlock_stream(fd_stream s)
{
  long long tid = u8_threadid();
  if (tid != s->stream_locker) {
    u8_string id = s->streamid;
    u8_log(LOGCRIT,"BadStreamUnlock",
           "Stream %s 0x%llx is owned by T%lld, not current T%lld",
	   ((id)?(id):(U8S0())),((id)?((u8_string)" "):(U8S0())),
           (U8_PTR2INT(s)),s->stream_locker,tid);
    u8_seterr("BadStreamUnlock","fd_lock_stream",s->streamid);
    return -1;}
  s->stream_locker = 0;
  int rv = u8_unlock_mutex(&(s->stream_lock));
  if (rv) {
    return -1;}
  return 1;
}
#else
#define fd_lock_stream(s) _fd_lock_stream(s)
#define fd_unlock_stream(s) _fd_unlock_stream(s)
#endif

/* Exceptions */

FD_EXPORT fd_exception fd_ReadOnlyStream;
FD_EXPORT fd_exception fd_CantWrite, fd_CantRead, fd_CantSeek;
FD_EXPORT fd_exception fd_BadLSEEK, fd_OverSeek, fd_UnderSeek;

#endif /* FD_STREAMS_H */
