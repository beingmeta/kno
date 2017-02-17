/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_DTYPEIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dtypestream.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <zlib.h>

#define FD_DEFAULT_ZLEVEL 9

#if HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#if ((FD_LARGEFILES_ENABLED) && (defined(O_LARGEFILE)))
#define POSIX_OPEN_FLAGS O_LARGEFILE
#else
#define POSIX_OPEN_FLAGS 0
#endif

#define lock_stream(s) u8_lock_mutex(&(s->stream_lock))
#define unlock_stream(s) u8_unlock_mutex(&(s->stream_lock))

unsigned int fd_check_dtsize=0;

fd_exception fd_ReadOnlyStream=_("Read-only stream");
fd_exception fd_CantRead=_("Can't read data");
fd_exception fd_CantWrite=_("Can't write data");
fd_exception fd_BadSeek=_("Bad seek argument");
fd_exception fd_CantSeek=_("Can't seek on stream");
fd_exception fd_BadLSEEK=_("lseek() failed");
fd_exception fd_OverSeek=_("Seeking past end of file");
fd_exception fd_UnderSeek=_("Seeking before the beginning of the file");

#define FD_DEBUG_DTYPEIO 0

static fdtype zread_dtype(struct FD_DTYPE_STREAM *s,int unlock);
static int fill_dtype_stream(struct FD_DTYPE_STREAM *df,int n);
static int dtsflush(struct FD_DTYPE_STREAM * s,int unlock);

static u8_byte _dbg_outbuf[FD_DEBUG_OUTBUF_SIZE];

/* Utility functions */

static int writeall(int fd,const unsigned char *data,int n)
{
  int written=0;
  while (written<n) {
    int delta=write(fd,data+written,n-written);
    if (delta<0)
      if (errno==EAGAIN) errno=0;
      else {
        u8_log(LOG_ERROR,
               "write failed","writeall %d errno=%d (%s) written=%d/%d",
                fd,errno,strerror(errno),written,n);
        return delta;}
    else written=written+delta;}
  return n;
}

/* Initialization functions */

FD_EXPORT struct FD_DTYPE_STREAM *fd_init_dtype_stream
  (struct FD_DTYPE_STREAM *s,int sock,int bufsiz)
{
  if (sock<0) return NULL;
  else {
    unsigned char *buf=u8_malloc(bufsiz);
    /* If you can't get a whole buffer, try smaller */
    while ((bufsiz>=1024) && (buf==NULL)) {
      bufsiz=bufsiz/2; buf=u8_malloc(bufsiz);}
    if (buf==NULL) bufsiz=0;
    /* Initialize the on-demand reader */
    FD_INIT_BYTE_INPUT(s,buf,bufsiz);
    s->bs_buflim=s->bs_bufptr; s->dts_bufsiz=bufsiz;
    s->dts_mallocd=0; s->fd_fileno=sock; s->dts_idstring=NULL;
    s->dts_diskpos=-1; s->dts_maxpos=-1;
    s->bs_fillfn=fill_dtype_stream; s->bs_flushfn=NULL;
    s->bs_flags|=FD_DTSTREAM_READING|FD_BYTEBUF_MALLOCD;
    u8_init_mutex(&(s->stream_lock));
    return s;}
}

FD_EXPORT fd_dtype_stream fd_init_dtype_file_stream
   (struct FD_DTYPE_STREAM *stream,
    u8_string fname,fd_dtstream_mode mode,int bufsiz)
{
  int fd, flags=POSIX_OPEN_FLAGS, lock=0, writing=0;
  char *localname=u8_localpath(fname);
  switch (mode) {
  case FD_DTSTREAM_READ:
    flags=flags|O_RDONLY; break;
  case FD_DTSTREAM_MODIFY:
    flags=flags|O_RDWR; lock=1; writing=1; break;
  case FD_DTSTREAM_WRITE:
    flags=flags|O_RDWR|O_CREAT; lock=1; writing=1; break;
  case FD_DTSTREAM_CREATE:
    flags=flags|O_CREAT|O_TRUNC|O_RDWR; lock=1; writing=1; break;
  }
  fd=open(localname,flags,0666); stream->bs_flags=0;
  /* If we fail and we're modifying, try to open read-only */
  if ((fd<0) && (mode == FD_DTSTREAM_MODIFY)) {
    fd=open(localname,O_RDONLY,0666);
    if (fd>0) writing=0;}
  if (fd>0) {
    fd_init_dtype_stream(stream,fd,bufsiz);
    stream->dts_mallocd=1; stream->dts_idstring=u8_strdup(fname);
    stream->bs_flags=stream->bs_flags|FD_DTSTREAM_CANSEEK;
    if (lock) stream->bs_flags=stream->bs_flags|FD_DTSTREAM_NEEDS_LOCK;
    if (writing == 0) stream->bs_flags=stream->bs_flags|FD_DTSTREAM_READ_ONLY;
    stream->dts_maxpos=lseek(fd,0,SEEK_END);
    stream->dts_diskpos=lseek(fd,0,SEEK_SET);
    u8_init_mutex(&(stream->stream_lock));
    u8_free(localname);
    return stream;}
  else {
    fd_seterr3(fd_CantOpenFile,"fd_init_dtype_file_stream",localname);
    return NULL;}
}

FD_EXPORT fd_dtype_stream fd_open_dtype_file
   (u8_string fname,fd_dtstream_mode mode,int bufsiz)
{
  struct FD_DTYPE_STREAM *stream=
    u8_alloc(struct FD_DTYPE_STREAM);
  struct FD_DTYPE_STREAM *opened=
    fd_init_dtype_file_stream(stream,fname,mode,bufsiz);
  if (opened) return opened;
  else {
    u8_free(stream);
    return NULL;}
}

FD_EXPORT void fd_dtsclose(fd_dtype_stream s,int flags)
{
  int dofree   =   (U8_BITP(flags,FD_DTS_FREE));
  int close_fd = ! (U8_BITP(flags,FD_DTS_NOCLOSE));
  int flush    = !  (U8_BITP(flags,FD_DTS_NOFLUSH));

  /* Already closed */
  if (s->fd_fileno<0) return;

  /* Lock before closing */
  lock_stream(s);

  /* Flush data */
  if (flush) {
    dtsflush(s,FD_STREAM_LOCKED);
    fsync(s->fd_fileno);}

  if (close_fd) {
    if (s->bs_flags&FD_DTSTREAM_SOCKET)
      shutdown(s->fd_fileno,SHUT_RDWR);
    close(s->fd_fileno);}
  s->fd_fileno=-1;

  if (dofree) {
    if (s->dts_idstring) {
      u8_free(s->dts_idstring);
      s->dts_idstring=NULL;}
    if (s->bs_bufstart) {
      u8_free(s->bs_bufstart);
      s->bs_bufstart=s->bs_bufptr=s->bs_buflim=NULL;}
    unlock_stream(s);
    u8_destroy_mutex(&(s->stream_lock));
    if (s->dts_mallocd) u8_free(s);}
  else unlock_stream(s);
}

FD_EXPORT void fd_dtsfree(fd_dtype_stream s,int flags)
{
  fd_dtsclose(s,flags|FD_DTS_FREE);
}

FD_EXPORT void fd_dtsbufsize(fd_dtype_stream s,int bufsiz)
{
  lock_stream(s);
  dtsflush(s,FD_STREAM_LOCKED);
  {
    unsigned int ptroff=s->bs_bufptr-s->bs_bufstart;
    unsigned int endoff=s->bs_buflim-s->bs_bufstart;
    s->bs_bufstart=u8_realloc(s->bs_bufstart,bufsiz);
    s->bs_bufptr=s->bs_bufstart+ptroff; s->bs_buflim=s->bs_bufstart+endoff;
    s->dts_bufsiz=bufsiz;
  }
  unlock_stream(s);
}

FD_EXPORT fdtype dts_read_dtype(fd_dtype_stream s,int unlock)
{
  int first_byte;

  if ((s->bs_flags&FD_DTSTREAM_READING) == 0)
    if (fd_set_read(s,1)<0) {
      if (unlock) unlock_stream(s);
      return FD_ERROR_VALUE;}

  first_byte=fd_dtsprobe_byte(s);

  if (first_byte==dt_ztype) {
    fd_dtsread_byte(s);
    return zread_dtype(s,unlock);}
  else if (first_byte>=0x80) {
    /* Probably compressed */
    fd_seterr("NYI","fd_dtsread_type/zip",s->dts_idstring,FD_VOID);
    return FD_ERROR_VALUE;}
  else {
    fdtype result=fd_read_dtype((struct FD_BYTE_INPUT *)s);
    if (unlock) unlock_stream(s);
    return result;}
}
FD_EXPORT fdtype fd_dtsread_dtype(fd_dtype_stream s)
{
  lock_stream(s);
  return dts_read_dtype(s,FD_DTS_UNLOCK);
}

FD_EXPORT int dts_write_dtype(fd_dtype_stream s,fdtype x,int unlock)
{
  int n_bytes; fd_off_t start;
  if ((s->bs_flags)&(FD_DTSTREAM_READING))
    if (dts_set_read(s,0,FD_STREAM_LOCKED)<0) {
      if (unlock) unlock_stream(s);
      return -1;}
  if (fd_check_dtsize)
    start=fd_getpos(s);
  else start=(fd_off_t)-1;
  n_bytes=fd_write_dtype((struct FD_BYTE_OUTPUT *)s,x);
  if ((fd_check_dtsize) && (start>=0)) {
    fd_off_t end=fd_getpos(s);
    if ((end-start)!= n_bytes)
      u8_log((((s->bs_flags)&(FD_DTSTREAM_CANSEEK)) ?
              (LOG_CRIT) : (LOG_ERR)),
             fd_InconsistentDTypeSize,
             "Inconsistent dtype length %d/%d for: %s",
             n_bytes,end-start,
             fd_dtype2buf(x,FD_DEBUG_OUTBUF_SIZE,_dbg_outbuf));
    else {
      dtsflush(s,FD_STREAM_LOCKED);
      end=fd_getpos(s);
      if ((end-start)!= n_bytes)
        u8_log((((s->bs_flags)&(FD_DTSTREAM_CANSEEK)) ? (LOG_CRIT) : (LOG_ERR)),
               fd_InconsistentDTypeSize,
               "Inconsistent dtype length (on disk) %d/%d for: %s",
               n_bytes,end-start,
               fd_dtype2buf(x,FD_DEBUG_OUTBUF_SIZE,_dbg_outbuf));}}
  if (unlock) {
    if ((s->bs_bufptr-s->bs_bufstart)*4>=(s->dts_bufsiz*3))
      dtsflush(s,FD_STREAM_LOCKED);
    unlock_stream(s);}
  return n_bytes;
}
FD_EXPORT int fd_dtswrite_dtype(fd_dtype_stream s,fdtype x)
{
  lock_stream(s);
  return dts_write_dtype(s,x,FD_DTS_UNLOCK);
}

/* Structure functions */

static int fill_dtype_stream(struct FD_DTYPE_STREAM *df,int n)
{
  int n_buffered, n_to_read, read_request, bytes_read=0;

  /* Shrink what you've already read, adjusting the filepos */

  lock_stream(df);

  bytes_read=(df->bs_bufptr-df->bs_bufstart);
  n_buffered=(df->bs_buflim-df->bs_bufptr);
  memmove(df->bs_bufstart,df->bs_bufptr,n_buffered);
  df->bs_buflim=(df->bs_bufstart)+n_buffered;
  df->bs_bufptr=df->bs_bufstart;
  bytes_read=0;

  /* Make sure that there's enough space */
  if (n>df->dts_bufsiz) {
    int new_size=df->dts_bufsiz;
    unsigned char *newbuf;
    size_t end_pos=df->bs_buflim-df->bs_bufstart;
    size_t ptr_pos=df->bs_bufptr-df->bs_bufstart;
    while (new_size<n)
      if (new_size>=0x40000) new_size=new_size+0x40000;
      else new_size=new_size*2;
    newbuf=u8_realloc(df->bs_bufstart,new_size);
    df->bs_bufstart=newbuf;
    df->bs_bufptr=newbuf+ptr_pos;
    df->bs_buflim=newbuf+end_pos;
    df->dts_bufsiz=new_size;}
  n_to_read=n-n_buffered;
  read_request=df->dts_bufsiz-n_buffered;
  while (bytes_read < n_to_read) {
    int delta;
    if ((delta=read(df->fd_fileno,df->bs_buflim,read_request-bytes_read))==0) break;
    if ((delta<0) && (errno) && (errno != EWOULDBLOCK)) {
      fd_seterr3(u8_strerror(errno),"fill_dtype_stream",u8s(df->dts_idstring));
      unlock_stream(df);
      return 0;}
    else if (delta<0) delta=0;
    df->bs_buflim=df->bs_buflim+delta;
    if (df->dts_diskpos>=0)
      df->dts_diskpos=df->dts_diskpos+delta;
    bytes_read=bytes_read+delta;}
  unlock_stream(df);
  return bytes_read;
}

static int dtsflush(fd_dtype_stream s,int unlock)
{
  if (FD_DTS_ISREADING(s)) {
    if (s->bs_buflim==s->bs_bufptr) return 0;
    else {
      s->bs_bufptr=s->bs_buflim=s->bs_bufstart;
      if (unlock) unlock_stream(s);
      return 0;}}
  else if (s->bs_bufptr>s->bs_bufstart) {
    int bytes_written=writeall(s->fd_fileno,s->bs_bufstart,s->bs_bufptr-s->bs_bufstart);
    if (bytes_written<0) {
      if (unlock) unlock_stream(s);
      return -1;}
    if ((s->bs_flags)&FD_DTSTREAM_DOSYNC) fsync(s->fd_fileno);
    if ( (s->bs_flags&FD_DTSTREAM_CANSEEK) && (s->dts_diskpos>=0) )
      s->dts_diskpos=s->dts_diskpos+bytes_written;
    if ((s->dts_maxpos>=0) && (s->dts_diskpos>s->dts_maxpos))
      s->dts_maxpos=s->dts_diskpos;
    /* Reset the buffer pointers */
    s->bs_bufptr=s->bs_bufstart;
    if (unlock) unlock_stream(s);
    return bytes_written;}
  else {
    if (unlock) unlock_stream(s);
    return 0;}
}

FD_EXPORT int fd_dtsflush(fd_dtype_stream s)
{
  lock_stream(s);
  return dtsflush(s,FD_DTS_UNLOCK);
}

/* Locking and unlocking */

FD_EXPORT void dts_lock(struct FD_DTYPE_STREAM *s)
{
  lock_stream(s);
}

FD_EXPORT void dts_unlock(struct FD_DTYPE_STREAM *s)
{
  unlock_stream(s);
}

FD_EXPORT int fd_dts_lockfile(fd_dtype_stream s)
{
  if (s->bs_flags&FD_DTSTREAM_LOCKED)
    return 1;
  else {
    int rv=-1;
    lock_stream(s);
    if ((rv=u8_lock_fd(s->fd_fileno,1))>=0) {
      s->bs_flags=s->bs_flags|FD_DTSTREAM_LOCKED;
      rv=1;}
    else rv=0;
    unlock_stream(s);
    return rv;}
}

FD_EXPORT int fd_dts_unlockfile(fd_dtype_stream s)
{
  if (!((s->bs_flags)&FD_DTSTREAM_LOCKED))
    return 1;
  else {
    int rv=-1;
    lock_stream(s);
    if ((rv=u8_unlock_fd(s->fd_fileno))>=0) {
      s->bs_flags=s->bs_flags&(~FD_DTSTREAM_LOCKED);
      rv=1;}
    else rv=0;
    unlock_stream(s);
    return rv;}
}

FD_EXPORT int dts_set_read(fd_dtype_stream s,int read,int unlock)
{
  if ((s->bs_flags)&FD_DTSTREAM_READ_ONLY)
    if (read==0) {
      fd_seterr(fd_ReadOnlyStream,"fd_set_read",u8s(s->dts_idstring),FD_VOID);
      return -1;}
    else return 1;
  else if ((s->bs_flags)&FD_DTSTREAM_READING)
    if (read) return 1;
    else {
      /* Lock the file descriptor if we need to. */
      if ((s->bs_flags)&FD_DTSTREAM_NEEDS_LOCK) {
        if (u8_lock_fd(s->fd_fileno,1)) {
          (s->bs_flags)=(s->bs_flags)|FD_DTSTREAM_LOCKED;}
        else {
          if (unlock) unlock_stream(s);
          return 0;}}

      /* If we were reading, in order to start writing, we need
         to reset the pointer and make the ->bs_buflim point to the
         end of the allocated buffer. */
      s->bs_bufptr=s->bs_bufstart;
      s->bs_buflim=s->bs_bufstart+s->dts_bufsiz;
      /* Now we clear the bit */
      (s->bs_flags)=(s->bs_flags)&(~FD_DTSTREAM_READING);
      if (unlock) unlock_stream(s);
      return 1;}
  else if (read == 0) {
    if (unlock) lock_stream(s);
    return 1;}
  else {
    /* If we were writing, in order to start reading, we need
       to flush what is buffered to the output and collapse all
       of the pointers into the start. We also need to update bufsiz
       in case the output buffer grew while we were writing. */
    if (dtsflush(s,FD_STREAM_LOCKED)<0) {
      if (unlock) unlock_stream(s);
      return -1;}
    /* Now we reset bufsiz in case we grew the buffer */
    s->dts_bufsiz=s->bs_buflim-s->bs_bufstart;
    /* Finally, we reset the pointers */
    s->bs_buflim=s->bs_bufptr=s->bs_bufstart;
    /* And set the reading bit */
    (s->bs_flags)=(s->bs_flags)|FD_DTSTREAM_READING;
    if (unlock) unlock_stream(s);
    return 1;}
}

FD_EXPORT int fd_set_read(fd_dtype_stream s,int read)
{
  lock_stream(s);
  return dts_set_read(s,read,FD_DTS_UNLOCK);
}

/* This gets the position when it isn't cached on the stream. */
FD_EXPORT fd_off_t _dts_getpos(fd_dtype_stream s,int unlock)
{
  fd_off_t current, pos;
  if (((s->bs_flags)&FD_DTSTREAM_CANSEEK) == 0) {
    if (unlock) unlock_stream(s);
    return fd_reterr(fd_CantSeek,"fd_getpos",u8s(s->dts_idstring),FD_INT(pos));}
  if ((s->bs_flags)&FD_DTSTREAM_READING) {
    current=lseek(s->fd_fileno,0,SEEK_CUR);
    /* If we are reading, we subtract the amount buffered from the
       actual filepos */
    s->dts_diskpos=current;
    pos=current-(s->bs_buflim-s->bs_bufstart);}
  else {
    current=lseek(s->fd_fileno,0,SEEK_CUR);
    s->dts_diskpos=current;
    /* If we are writing, we add the amount buffered for output to the
       actual filepos */
    pos=current+(s->bs_bufptr-s->bs_bufstart);}
  if (unlock) unlock_stream(s);
  return pos;
}
FD_EXPORT fd_off_t _fd_getpos(fd_dtype_stream s)
{
  fd_off_t current, pos;
  if (((s->bs_flags)&FD_DTSTREAM_CANSEEK) == 0)
    return fd_reterr(fd_CantSeek,"fd_getpos",u8s(s->dts_idstring),FD_INT(pos));
  lock_stream(s);
  return _dts_getpos(s,FD_DTS_UNLOCK);
}

FD_EXPORT fd_off_t dts_setpos(fd_dtype_stream s,fd_off_t pos)
{
  /* This is optimized for the case where the new position is
     in the range we have buffered. */
  if (((s->bs_flags)&FD_DTSTREAM_CANSEEK) == 0) {
    return fd_reterr(fd_CantSeek,"fd_setpos",u8s(s->dts_idstring),FD_INT(pos));}
  else if (pos<0) {
    return fd_reterr(fd_BadSeek,"fd_setpos",u8s(s->dts_idstring),FD_INT(pos));}
  else {}

  /* Otherwise, you're going to move the file position, so flush any
     buffered data. */
  if ( (s->dts_diskpos>=0) && (FD_DTS_ISREADING(s)) ) {
    fd_off_t delta=(pos-s->dts_diskpos);
    unsigned char *relptr=s->bs_buflim+delta;
    if ( (relptr >= s->bs_bufstart) && (relptr < s->bs_buflim )) {
      s->bs_bufptr=relptr;
      return pos;}}
  /* We're jumping out of what we have buffered */
  if (dtsflush(s,FD_STREAM_LOCKED)<0) {
    return -1;}
  fd_off_t newpos=lseek(s->fd_fileno,pos,SEEK_SET);
  if (newpos>=0) {
    s->dts_diskpos=newpos;
    return newpos;}
  else if (errno==EINVAL) {
    fd_off_t maxpos=lseek(s->fd_fileno,(fd_off_t)0,SEEK_END);
    s->dts_maxpos=s->dts_diskpos=maxpos;
    return fd_reterr(fd_OverSeek,"fd_setpos",u8s(s->dts_idstring),FD_INT(pos));}
  else {
    u8_graberrno("fd_setpos",u8s(s->dts_idstring));
    return -1;}
}
FD_EXPORT fd_off_t fd_setpos(fd_dtype_stream s,fd_off_t pos)
{
  lock_stream(s); {
    fd_off_t off=dts_setpos(s,pos);
    unlock_stream(s);
    return off;}
}

FD_EXPORT fd_off_t fd_movepos(fd_dtype_stream s,int delta)
{
  fd_off_t cur, rv;
  if (((s->bs_flags)&FD_DTSTREAM_CANSEEK) == 0)
    return fd_reterr(fd_CantSeek,"fd_movepos",u8s(s->dts_idstring),FD_INT(delta));
  lock_stream(s);
  cur=fd_getpos(s);
  rv=dts_setpos(s,cur+delta);
  unlock_stream(s);
  return rv;
}
FD_EXPORT fd_off_t fd_endpos(fd_dtype_stream s)
{
  fd_off_t rv;
  if (((s->bs_flags)&FD_DTSTREAM_CANSEEK) == 0)
    return fd_reterr(fd_CantSeek,"fd_endpos",u8s(s->dts_idstring),FD_VOID);
  lock_stream(s);
  dtsflush(s,FD_STREAM_LOCKED);
  rv=s->dts_maxpos=s->dts_diskpos=(lseek(s->fd_fileno,0,SEEK_END));
  unlock_stream(s);
  return rv;
}

FD_EXPORT fd_off_t dts_endpos(fd_dtype_stream s)
{
  fd_off_t rv;
  if (((s->bs_flags)&FD_DTSTREAM_CANSEEK) == 0)
    return fd_reterr(fd_CantSeek,"fd_endpos",u8s(s->dts_idstring),FD_VOID);
  dtsflush(s,FD_STREAM_LOCKED);
  rv=s->dts_maxpos=s->dts_diskpos=(lseek(s->fd_fileno,0,SEEK_END));
  return rv;
}

/* Input functions */

FD_EXPORT int _fd_dtsread_byte(fd_dtype_stream s)
{
  if (((s->bs_flags)&FD_DTSTREAM_READING) == 0)
    if (dts_set_read(s,1,FD_STREAM_LOCKED)<0) return -1;
  if (fd_needs_bytes((fd_byte_input)s,1))
    return (*(s->bs_bufptr++));
  else return -1;
}

FD_EXPORT int _fd_dtsprobe_byte(fd_dtype_stream s)
{
  if (((s->bs_flags)&FD_DTSTREAM_READING) == 0)
    if (dts_set_read(s,1,FD_STREAM_LOCKED)<0) return -1;
  if (fd_needs_bytes((fd_byte_input)s,1))
    return (*(s->bs_bufptr));
  else return -1;
}

FD_EXPORT unsigned int _fd_dtsread_4bytes(fd_dtype_stream s)
{
  dts_start_read(s,FD_STREAM_LOCKED);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+4;
    return bytes;}
  else {fd_whoops(fd_UnexpectedEOD); return 0;}
}

FD_EXPORT fd_8bytes _fd_dtsread_8bytes(fd_dtype_stream s)
{
  dts_start_read(s,FD_STREAM_LOCKED);
  if (fd_needs_bytes((fd_byte_input)s,8)) {
    fd_8bytes bytes=fd_get_8bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+8;
    return bytes;}
  else {fd_whoops(fd_UnexpectedEOD); return 0;}
}

FD_EXPORT int _dts_read_bytes(fd_dtype_stream s,
                              unsigned char *bytes,int len,
                              u8_mutex *unlock)
{
  dts_start_read(s,FD_STREAM_LOCKED);
  /* This is special because we don't use the intermediate buffer
     if the data isn't fully buffered. */
  if (fd_has_bytes(s,len)) {
    memcpy(bytes,s->bs_bufptr,len);
    s->bs_bufptr=s->bs_bufptr+len;
    if (unlock) u8_unlock_mutex(unlock);
    return len;}
  else {
    int n_buffered=s->bs_buflim-s->bs_bufptr;
    int n_read=0, n_to_read=len-n_buffered;
    unsigned char *start=bytes+n_buffered;
    memcpy(bytes,s->bs_bufptr,n_buffered);
    s->bs_buflim=s->bs_bufptr=s->bs_bufstart;
    while (n_read<n_to_read) {
      int delta=read(s->fd_fileno,start,n_to_read);
      if (delta<0) {
        if (unlock) u8_unlock_mutex(unlock);
        return -1;}
      n_read=n_read+delta; start=start+delta;}
    s->dts_diskpos=s->dts_diskpos+n_read;
    if (unlock) u8_unlock_mutex(unlock);
    return len;}
}
FD_EXPORT int _fd_dtsread_bytes
  (fd_dtype_stream s,unsigned char *bytes,int len)
{
  lock_stream(s);
  return dts_read_bytes(s,bytes,len,&(s->stream_lock));
}

FD_EXPORT fd_4bytes _fd_dtsread_zint(fd_dtype_stream stream)
{
  return fd_dtsread_zint(stream);
}

FD_EXPORT fd_8bytes _fd_dtsread_zint8(fd_dtype_stream stream)
{
  return fd_dtsread_zint8(stream);
}

FD_EXPORT fd_off_t _fd_dtsread_off_t(fd_dtype_stream s)
{
  dts_start_read(s,FD_STREAM_LOCKED);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+4;
    return (fd_off_t) bytes;}
  else return ((fd_off_t)(-1));
}

FD_EXPORT int dts_read_ints(fd_dtype_stream s,int n_words,unsigned int *words)
{
  if (((s->bs_flags)&FD_DTSTREAM_READING) == 0)
    if (dts_set_read(s,1,FD_STREAM_LOCKED)<0) {
      return -1;}
  /* This is special because we ignore the buffer if we can. */
  if ((s->bs_flags)&FD_DTSTREAM_CANSEEK) {
    fd_off_t real_pos=fd_getpos(s);
    int bytes_read=0, bytes_needed=n_words*4;
    lseek(s->fd_fileno,real_pos,SEEK_SET);
    while (bytes_read<bytes_needed) {
      int delta=read(s->fd_fileno,words+bytes_read,bytes_needed-bytes_read);
      if (delta<0)
        if (errno==EAGAIN) errno=0;
        else {
          return delta;}
      else {
        s->dts_diskpos=s->dts_diskpos+delta;
        bytes_read+=delta;}}
#if (!(WORDS_BIGENDIAN))
    {int i=0; while (i < n_words) {
        words[i]=fd_host_order(words[i]); i++;}}
#endif
    return bytes_read;}
  else if (fd_needs_bytes((fd_byte_input)s,n_words*4)) {
    int i=0; while (i<n_words) {
      int word=fd_dtsread_4bytes(s);
      words[i++]=word;}
    return n_words*4;}
  else {
    return -1;}
}
FD_EXPORT int fd_dtsread_ints(fd_dtype_stream s,
                              int n_words,
                              unsigned int *words)
{
  lock_stream(s); {
    int n=dts_read_ints(s,n_words,words);
    unlock_stream(s);
    return n;}
}

/* Write functions */

FD_EXPORT int _fd_dtswrite_byte(fd_dtype_stream s,int b)
{
  if ((s->bs_flags)&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  if (s->bs_bufptr>=s->bs_buflim)
    dtsflush(s,FD_STREAM_LOCKED);
  *(s->bs_bufptr++)=b;
  return 1;
}

FD_EXPORT int _fd_dtswrite_4bytes(fd_dtype_stream s,fd_4bytes w)
{
  if ((s->bs_flags)&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  if (s->bs_bufptr+4>=s->bs_buflim)
    dtsflush(s,FD_STREAM_LOCKED);
  *(s->bs_bufptr++)=w>>24;
  *(s->bs_bufptr++)=((w>>16)&0xFF);
  *(s->bs_bufptr++)=((w>>8)&0xFF);
  *(s->bs_bufptr++)=((w>>0)&0xFF);
  return 4;
}

FD_EXPORT int _fd_dtswrite_8bytes(fd_dtype_stream s,fd_8bytes w)
{
  if ((s->bs_flags)&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  if (s->bs_bufptr+8>=s->bs_buflim)
    dtsflush(s,FD_STREAM_LOCKED);
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

FD_EXPORT int _fd_dtswrite_zint(struct FD_DTYPE_STREAM *s,fd_4bytes val)
{
  return fd_dtswrite_zint(s,val);
}

FD_EXPORT int _dts_write_bytes(fd_dtype_stream s,const unsigned char *bytes,int n)
{
  if ((s->bs_flags)&FD_DTSTREAM_READING)
    if (dts_set_read(s,0,FD_STREAM_LOCKED)<0) {
      return -1;}
  /* If there isn't space, flush the stream */
  if (s->bs_bufptr+n>=s->bs_buflim)
    dtsflush(s,FD_STREAM_LOCKED);
  /* If there still isn't space (bufsiz too small),
     call writeall and advance the filepos to reflect the
     written bytes. */
  if (s->bs_bufptr+n>=s->bs_buflim) {
    int bytes_written=writeall(s->fd_fileno,bytes,n);
#if FD_DEBUG_DTYPE_IO
    u8_log(LOG_DEBUG,"DTSWRITE",
           "Wrote %d/%d bytes to %s#%d, %d/%d bytes in buffer",
           bytes_written,n,
           s->fd_dtsid,s->fd_fileno,
           s->bs_bufptr-s->bs_bufstart,
           s->bs_buflim-s->bs_bufstart);
#endif
    if (bytes_written<0) return bytes_written;
    s->dts_diskpos=s->dts_diskpos+bytes_written;}
  else {
    memcpy(s->bs_bufptr,bytes,n); s->bs_bufptr=s->bs_bufptr+n;}
  return n;
}
FD_EXPORT int fd_dtswrite_bytes
  (fd_dtype_stream s,const unsigned char *bytes,int n)
{
  lock_stream(s); {
    int n=_dts_write_bytes(s,bytes,n);
    unlock_stream(s);
    return n;}
}

FD_EXPORT int dts_write_ints(fd_dtype_stream s,int len,unsigned int *words)
{
  if (((s->bs_flags))&FD_DTSTREAM_READING)
    if (dts_set_read(s,0,FD_STREAM_LOCKED)<0) return -1;
  /* This is special because we ignore the buffer if we can. */
  if (((s->bs_flags))&FD_DTSTREAM_CANSEEK) {
    fd_off_t real_pos; int bytes_written;
    dtsflush(s,FD_STREAM_LOCKED);
    real_pos=dts_getpos(s,FD_STREAM_LOCKED);
    lseek(s->fd_fileno,real_pos,SEEK_SET);
#if (!(WORDS_BIGENDIAN))
    {int i=0; while (i < len) {
        words[i]=fd_net_order(words[i]); i++;}}
#endif
    bytes_written=writeall(s->fd_fileno,(unsigned char *)words,len*4);
#if (!(WORDS_BIGENDIAN))
    {int i=0; while (i < len) {
        words[i]=fd_host_order(words[i]); i++;}}
#endif
    return bytes_written;}
  else {
    int i=0; while (i<len) {
      int word=words[i++];
      fd_dtswrite_4bytes(s,word);}
    return len*4;}
}
FD_EXPORT int fd_dtswrite_ints(fd_dtype_stream s,int len,unsigned int *words)
{
  lock_stream(s); {
    int n=dts_write_ints(s,len,words);
    unlock_stream(s);
    return n;}
}

/* Writing compressed DTYPEs */

/* Reading compressed oid values */

static unsigned char *do_uncompress
  (unsigned char *bytes,size_t n_bytes,ssize_t *dbytes)
{
  int error;
  uLongf x_lim=4*n_bytes, x_bytes=x_lim;
  Bytef *fdata=(Bytef *)bytes, *xdata=u8_malloc(x_bytes);
  while ((error=uncompress(xdata,&x_bytes,fdata,n_bytes)) < Z_OK)
    if (error == Z_MEM_ERROR) {
      u8_free(xdata);
      fd_seterr1("ZLIB Out of Memory");
      return NULL;}
    else if (error == Z_BUF_ERROR) {
      xdata=u8_realloc(xdata,x_lim*2); x_bytes=x_lim=x_lim*2;}
    else if (error == Z_DATA_ERROR) {
      u8_free(xdata);
      fd_seterr1("ZLIB Data error");
      return NULL;}
    else {
      u8_free(xdata);
      fd_seterr1("Bad ZLIB return code");
      return NULL;}
  *dbytes=x_bytes;
  return xdata;
}

static unsigned char *do_compress(unsigned char *bytes,size_t n_bytes,
                                  ssize_t *zbytes)
{
  int error; Bytef *zdata;
  uLongf zlen, zlim;
  zlen=zlim=2*n_bytes; zdata=u8_malloc(zlen);
  while ((error=compress2(zdata,&zlen,bytes,n_bytes,FD_DEFAULT_ZLEVEL)) < Z_OK)
    if (error == Z_MEM_ERROR) {
      u8_free(zdata);
      fd_seterr1("ZLIB Out of Memory");
      return NULL;}
    else if (error == Z_BUF_ERROR) {
      zdata=u8_realloc(zdata,zlim*2); zlen=zlim=zlim*2;}
    else if (error == Z_DATA_ERROR) {
      u8_free(zdata);
      fd_seterr1("ZLIB Data error");
      return NULL;}
    else {
      u8_free(zdata);
      fd_seterr1("Bad ZLIB return code");
      return NULL;}
  *zbytes=zlen;
  return zdata;
}

/* This reads a non frame value with compression. */
static fdtype zread_dtype(struct FD_DTYPE_STREAM *s,int unlock)
{
  fdtype result;
  struct FD_BYTE_INPUT in;
  ssize_t n_bytes=fd_dtsread_zint(s), dbytes;
  unsigned char *bytes;
  int retval=-1;
  bytes=u8_malloc(n_bytes);
  retval=fd_dtsread_bytes(s,bytes,n_bytes);
  if (retval<n_bytes) {
    u8_free(bytes);
    if (unlock) unlock_stream(s);
    return FD_ERROR_VALUE;}
  memset(&in,0,sizeof(in));
  in.bs_bufptr=in.bs_bufstart=do_uncompress(bytes,n_bytes,&dbytes);
  in.bs_flags=FD_BYTEBUF_MALLOCD;
  if (in.bs_bufstart==NULL) {
    u8_free(bytes);
    if (unlock) unlock_stream(s);
    return FD_ERROR_VALUE;}
  in.bs_buflim=in.bs_bufstart+dbytes; in.bs_fillfn=NULL;
  result=fd_read_dtype(&in);
  u8_free(bytes); u8_free(in.bs_bufstart);
  if (unlock) unlock_stream(s);
  return result;
}

FD_EXPORT fdtype fd_zread_dtype(struct FD_DTYPE_STREAM *s)
{
  lock_stream(s);
  return zread_dtype(s,FD_DTS_UNLOCK);
}

/* This reads a non frame value with compression. */
static int zwrite_dtype(struct FD_DTYPE_STREAM *s,fdtype x,int unlock)
{
  unsigned char *zbytes; ssize_t zlen=-1, size;
  struct FD_BYTE_OUTPUT out; memset(&out,0,sizeof(out));
  out.bs_bufptr=out.bs_bufstart=u8_malloc(2048);
  out.bs_buflim=out.bs_bufstart+2048;
  out.bs_flags=FD_BYTEBUF_MALLOCD;
  if (fd_write_dtype(&out,x)<0) {
    u8_free(out.bs_bufstart);
    if (unlock) unlock_stream(s);
    return FD_ERROR_VALUE;}
  zbytes=do_compress(out.bs_bufstart,out.bs_bufptr-out.bs_bufstart,&zlen);
  if (zlen<0) {
    u8_free(out.bs_bufstart);
    if (unlock) unlock_stream(s);
    return FD_ERROR_VALUE;}
  fd_dtswrite_byte(s,dt_ztype);
  size=fd_dtswrite_zint(s,zlen); size=size+zlen;
  if (dts_write_bytes(s,zbytes,zlen)<0) size=-1;
  dtsflush(s,FD_STREAM_LOCKED);
  u8_free(zbytes); u8_free(out.bs_bufstart);
  if (unlock) unlock_stream(s);
  return size;
}

FD_EXPORT int fd_zwrite_dtype(struct FD_DTYPE_STREAM *s,fdtype x)
{
  lock_stream(s);
  return zwrite_dtype(s,x,FD_DTS_UNLOCK);
}

static int zwrite_dtypes(struct FD_DTYPE_STREAM *s,fdtype x,int unlock)
{
  unsigned char *zbytes=NULL; ssize_t zlen=-1, size; int retval=0;
  struct FD_BYTE_OUTPUT out; memset(&out,0,sizeof(out));
  out.bs_bufptr=out.bs_bufstart=u8_malloc(2048);
  out.bs_buflim=out.bs_bufstart+2048;
  out.bs_flags=FD_BYTEBUF_MALLOCD;
  if (FD_CHOICEP(x)) {
    FD_DO_CHOICES(v,x) {
      retval=fd_write_dtype(&out,v);
      if (retval<0) {FD_STOP_DO_CHOICES; break;}}}
  else if (FD_VECTORP(x)) {
    int i=0, len=FD_VECTOR_LENGTH(x); fdtype *data=FD_VECTOR_DATA(x);
    while (i<len) {
      retval=fd_write_dtype(&out,data[i]); i++;
      if (retval<0) break;}}
  else retval=fd_write_dtype(&out,x);
  if (retval>=0)
    zbytes=do_compress(out.bs_bufstart,out.bs_bufptr-out.bs_bufstart,&zlen);
  if ((retval<0)||(zlen<0)) {
    if (zbytes) u8_free(zbytes); u8_free(out.bs_bufstart);
    if (unlock) unlock_stream(s);
    return -1;}
  fd_dtswrite_byte(s,dt_ztype);
  size=1+fd_dtswrite_zint(s,zlen); size=size+zlen;
  retval=dts_write_bytes(s,zbytes,zlen);
  u8_free(zbytes); u8_free(out.bs_bufstart);
  if (unlock) unlock_stream(s);
  if (retval<0) return retval;
  else return size;
}

FD_EXPORT int fd_zwrite_dtypes(struct FD_DTYPE_STREAM *s,fdtype x)
{
  lock_stream(s);
  return zwrite_dtypes(s,x,FD_DTS_UNLOCK);
}

/* Files 2 dtypes */

FD_EXPORT fdtype fd_read_dtype_from_file(u8_string filename)
{
  struct FD_DTYPE_STREAM *stream=u8_alloc(struct FD_DTYPE_STREAM);
  ssize_t filesize=u8_file_size(filename);
  ssize_t bufsize=(filesize+1024);
  if (filesize<0) {
    fd_seterr(fd_FileNotFound,"fd_file2dtype",u8_strdup(filename),FD_VOID);
    return FD_ERROR_VALUE;}
  else if (filesize==0) {
    fd_seterr("Zero-length file","fd_file2dtype",u8_strdup(filename),FD_VOID);
    return FD_ERROR_VALUE;}
  else {
    struct FD_DTYPE_STREAM *opened=
      fd_init_dtype_file_stream(stream,filename,FD_DTSTREAM_READ,bufsize);
    if (opened) lock_stream(opened);
    if (opened) {
      fdtype result=FD_VOID;
      int byte1=fd_dtsread_byte(opened);
      int zip=(byte1>=0x80);
      if (opened->bs_bufptr > opened->bs_bufstart)
        opened->bs_bufptr--;
      else fd_setpos(opened,0);
      if (zip)
        result=zread_dtype(opened,FD_STREAM_LOCKED);
      else result=dts_read_dtype(opened,FD_STREAM_LOCKED);
      unlock_stream(opened);
      fd_dtsfree(opened,1);
      return result;}
    else {
      u8_free(stream);
      return FD_ERROR_VALUE;}}
}

FD_EXPORT ssize_t _fd_write_dtype_to_file(fdtype object,
                                          u8_string filename,
                                          size_t bufsize,
                                          int zip)
{
  struct FD_DTYPE_STREAM *stream=u8_alloc(struct FD_DTYPE_STREAM);
  struct FD_DTYPE_STREAM *opened=
    fd_init_dtype_file_stream(stream,filename,FD_DTSTREAM_WRITE,bufsize);
  if (opened) lock_stream(opened);
  if (opened) {
    size_t len=(zip)?
      (zwrite_dtype(opened,object,FD_STREAM_LOCKED)):
      (dts_write_dtype(opened,object,FD_STREAM_LOCKED));
    unlock_stream(opened);
    fd_dtsfree(opened,1);
    return len;}
  else return -1;
}

FD_EXPORT ssize_t fd_write_dtype_to_file(fdtype object,u8_string filename)
{
  return _fd_write_dtype_to_file(object,filename,1024*64,0);
}

FD_EXPORT ssize_t fd_write_zdtype_to_file(fdtype object,u8_string filename)
{
  return _fd_write_dtype_to_file(object,filename,1024*64,1);
}

/* Initialization of file */

FD_EXPORT void fd_init_dtypestream_c()
{
  fd_register_config
    ("CHECKDTSIZE",_("whether to check returned and real dtype sizes"),
     fd_boolconfig_get,fd_boolconfig_set,&fd_check_dtsize);

  u8_register_source_file(_FILEINFO);
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
