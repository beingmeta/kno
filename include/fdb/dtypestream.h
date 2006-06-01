/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FD_DTYPESTREAM_H
#define FD_DTYPESTREAM_H 1
#define FDB_DTYPESTREAM_H_VERSION "$Id: dtypestream.h,v 1.24 2006/01/26 14:44:32 haase Exp $"

#include "dtype.h"
#include "dtypeio.h"

#include <unistd.h>
#include <fcntl.h>

FD_EXPORT fd_exception fd_CantWrite, fd_CantRead;

#define FD_DTSTREAM_BUFSIZ_DEFAULT 32*1024
#define FD_DTSTREAM_READING 1
#define FD_DTSTREAM_READ_ONLY 2
#define FD_DTSTREAM_CANSEEK 4
#define FD_DTSTREAM_NEEDS_LOCK 8
#define FD_DTSTREAM_LOCKED 16
#define FD_DTSTREAM_SOCKET 32
#define FD_DTSTREAM_DOSYNC 64

typedef enum FD_DTSTREAM_MODE {
  FD_DTSTREAM_READ, /* Read only, must exist */
  FD_DTSTREAM_MODIFY, /* Read/write, must exist */
  FD_DTSTREAM_WRITE, /* Read/write, may exist */
  FD_DTSTREAM_CREATE /* Read/write, truncated */
} fd_dtstream_mode;

typedef struct FD_DTYPE_STREAM {
  unsigned char *start, *ptr, *end;
  FD_MEMORY_POOL_TYPE *mpool;
  int (*fillfn)(struct FD_DTYPE_STREAM *,int);
  FD_MEMORY_POOL_TYPE *conspool; u8_string id;
  int mallocd, bufsiz; off_t filepos, maxpos; 
  int fd, bits;} FD_DTYPE_STREAM;
typedef struct FD_DTYPE_STREAM *fd_dtype_stream;

FD_EXPORT void fd_init_dtype_stream
  (struct FD_DTYPE_STREAM *s,int sock,int bufsiz,
   FD_MEMORY_POOL_TYPE *bpool,
   FD_MEMORY_POOL_TYPE *dpool);

FD_EXPORT fd_dtype_stream fd_init_dtype_file_stream
   (struct FD_DTYPE_STREAM *stream,
    u8_string filename,fd_dtstream_mode mode,int bufsiz,
    FD_MEMORY_POOL_TYPE *bufpool,
    FD_MEMORY_POOL_TYPE *conspool);

FD_EXPORT fd_dtype_stream fd_open_dtype_file_x
  (u8_string filename,fd_dtstream_mode mode,int bufsiz,
   FD_MEMORY_POOL_TYPE *bufpool,
   FD_MEMORY_POOL_TYPE *conspool);
#define fd_dtsopen(filename,mode) \
  fd_open_dtype_file_x(filename,mode,FD_DTSTREAM_BUFSIZ_DEFAULT, \
                       NULL,NULL)
#define FD_DTSCLOSE_FD 1
#define FD_DTSCLOSE_FULL 2
FD_EXPORT void fd_dtsclose(fd_dtype_stream s,int close_fd);

/* Structure functions and macros */

FD_EXPORT off_t _fd_getpos(fd_dtype_stream s);
#define fd_getpos(s) \
  ((s->bits&FD_DTSTREAM_CANSEEK) ? \
   ((s->filepos>=0) ? (s->filepos+(s->ptr-s->start)) : (_fd_getpos(s))) \
   : (-1))
FD_EXPORT off_t fd_setpos(fd_dtype_stream s,off_t pos);
FD_EXPORT off_t fd_movepos(fd_dtype_stream s,off_t dleta);
FD_EXPORT off_t fd_endpos(fd_dtype_stream s);

FD_EXPORT int fd_set_read(fd_dtype_stream s,int read);
FD_EXPORT void fd_dtsflush(fd_dtype_stream s);
FD_EXPORT int fd_dtslock(fd_dtype_stream s);
FD_EXPORT int fd_dtsunlock(fd_dtype_stream s);

FD_EXPORT void fd_dtsbufsize(fd_dtype_stream s,int bufsiz);

#define fd_dts_start_read(s) \
  if ((s->bits&FD_DTSTREAM_READING) == 0) if (fd_set_read(s,1)<0) return -1;
#define fd_dts_start_write(s) \
  if (s->bits&FD_DTSTREAM_READING) if (fd_set_read(s,0)<0) return -1;

/* Readers and writers */

FD_EXPORT fdtype fd_dtsread_dtype(fd_dtype_stream s);
FD_EXPORT int fd_dtswrite_dtype(fd_dtype_stream s,fdtype x);

FD_EXPORT int fd_dtsread_ints(fd_dtype_stream s,int len,unsigned int *words);
FD_EXPORT int fd_dtswrite_ints(fd_dtype_stream s,int len,unsigned int *words);

#if FD_INLINE_DTYPEIO
FD_FASTOP int fd_dtsread_byte(fd_dtype_stream s)
{
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,1)) 
    return (*(s->ptr++));
  else return -1;
}

FD_FASTOP unsigned int fd_dtsread_4bytes(fd_dtype_stream s)
{
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->ptr);
    s->ptr=s->ptr+4;
    return bytes;}
  else fd_whoops(fd_UnexpectedEOD);
}

FD_FASTOP int fd_dtsread_bytes
  (fd_dtype_stream s,unsigned char *bytes,int len) 
{
  fd_dts_start_read(s);
  /* This is special because we don't use the intermediate buffer
     if the data isn't fully buffered. */
  if (fd_has_bytes(s,len)) {
    memcpy(bytes,s->ptr,len);
    s->ptr=s->ptr+len;
    return len;}
  else {
    int n_buffered=s->end-s->ptr;
    int n_read=0, n_to_read=len-n_buffered;
    unsigned char *start=bytes+n_buffered;
    memcpy(bytes,s->ptr,n_buffered);
    s->end=s->ptr=s->start;
    while (n_read<n_to_read) {
      int delta=read(s->fd,start,n_to_read);
      if (delta<0) return -1;
      n_read=n_read+delta; start=start+delta;}
    s->filepos=s->filepos+n_buffered+n_read;
    return len;}
}

FD_FASTOP off_t fd_dtsread_off_t(fd_dtype_stream s)
{
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->ptr);
    s->ptr=s->ptr+4;
    return (off_t) bytes;}
  else return (off_t) -1;
}

FD_FASTOP unsigned int fd_dtsread_zint(fd_dtype_stream s)
{
  unsigned int result=0, probe;
  while (probe=fd_dtsread_byte(s))
    if (probe&0x80) result=result<<7|(probe&0x7F);
    else break;
  return result<<7|probe;
}


/* Inline writers */

FD_FASTOP int fd_dtswrite_byte(fd_dtype_stream s,int b)
{
  fd_dts_start_write(s);
  if (s->ptr>=s->end) fd_dtsflush(s);
  *(s->ptr++)=b;
  return 1;
}
FD_FASTOP int fd_dtswrite_4bytes(fd_dtype_stream s,unsigned int w)
{
  fd_dts_start_write(s);
  if (s->ptr+4>=s->end) fd_dtsflush(s);
  *(s->ptr++)=w>>24;
  *(s->ptr++)=((w>>16)&0xFF);
  *(s->ptr++)=((w>>8)&0xFF);
  *(s->ptr++)=((w>>0)&0xFF);
  return 4;
}
FD_FASTOP int fd_dtswrite_bytes
  (fd_dtype_stream s,unsigned char *bytes,int n)
{
  fd_dts_start_write(s);
  if (s->ptr+n>=s->end) fd_dtsflush(s);
  if (s->ptr+n>=s->end) {
    int bytes_written=0;
    while (bytes_written < n) {
      int delta=write(s->fd,bytes,n);
      bytes_written=bytes_written+delta;}
    s->filepos=s->filepos+bytes_written;}
  else {
    memcpy(s->ptr,bytes,n); s->ptr=s->ptr+n;}
  return n;
}
static int fd_dtswrite_zint(fd_dtype_stream s,int n)
{
  if (n < (1<<7)) {
    return fd_dtswrite_byte(s,n);}    
  else if (n < (1<<14)) {
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
#else /* FD_INLINE_DTYPEIO */
FD_EXPORT unsigned int _fd_dtsread_byte(struct FD_DTYPE_STREAM *stream);
FD_EXPORT unsigned int _fd_dtsread_4bytes(struct FD_DTYPE_STREAM *stream);
FD_EXPORT void _fd_dtsread_bytes
  (struct FD_DTYPE_STREAM *stream,unsigned char *bytes,int len);
FD_EXPORT off_t _fd_dtsread_off_t(struct FD_DTYPE_STREAM *stream);
#define fd_dtsread_byte   _fd_dtsread_byte(x)
#define fd_dtsread_4bytes _fd_dtsread_4bytes 
#define fd_dtsread_bytes  _fd_dtsread_bytes 

FD_EXPORT int _fd_dtswrite_byte
  (struct FD_DTYPE_STREAM *stream,int b);
FD_EXPORT int _fd_dtswrite_4bytes
  (struct FD_DTYPE_STREAM *stream,int w);
FD_EXPORT int _fd_dtswrite_bytes
  (struct FD_DTYPE_STREAM *stream,unsigned char *bytes,int len);
#define fd_dtswrite_byte   _fd_dtswrite_byte 
#define fd_dtswrite_4bytes _fd_dtswrite_4bytes 
#define fd_dtswrite_bytes  _fd_dtswrite_bytes 

#endif  /* FD_INLINE_DTYPEIO */
#endif /* FD_DTYPESTREAM_H */
