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
#include "framerd/bytestream.h"

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

static fdtype zread_dtype(struct FD_BYTESTREAM *s,int unlock);
static int fill_bytestream(struct FD_BYTESTREAM *df,int n);

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

FD_EXPORT struct FD_BYTESTREAM *fd_init_bytestream
  (struct FD_BYTESTREAM *s,int sock,int bufsiz)
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
    s->bs_buflim=s->bs_bufptr; s->bytestream_bufsiz=bufsiz;
    s->bytestream_mallocd=0; s->fd_fileno=sock; s->bytestream_idstring=NULL;
    s->bytestream_diskpos=-1; s->bytestream_maxpos=-1;
    s->bs_fillfn=fill_bytestream; s->bs_flushfn=NULL;
    s->bs_flags|=FD_BYTESTREAM_READING|FD_BYTEBUF_MALLOCD;
    u8_init_mutex(&(s->stream_lock));
    return s;}
}

FD_EXPORT fd_bytestream fd_init_file_bytestream
   (struct FD_BYTESTREAM *stream,
    u8_string fname,fd_bytestream_mode mode,int bufsiz)
{
  int fd, flags=POSIX_OPEN_FLAGS, lock=0, writing=0;
  char *localname=u8_localpath(fname);
  switch (mode) {
  case FD_BYTESTREAM_READ:
    flags=flags|O_RDONLY; break;
  case FD_BYTESTREAM_MODIFY:
    flags=flags|O_RDWR; lock=1; writing=1; break;
  case FD_BYTESTREAM_WRITE:
    flags=flags|O_RDWR|O_CREAT; lock=1; writing=1; break;
  case FD_BYTESTREAM_CREATE:
    flags=flags|O_CREAT|O_TRUNC|O_RDWR; lock=1; writing=1; break;
  }
  fd=open(localname,flags,0666); stream->bs_flags=0;
  /* If we fail and we're modifying, try to open read-only */
  if ((fd<0) && (mode == FD_BYTESTREAM_MODIFY)) {
    fd=open(localname,O_RDONLY,0666);
    if (fd>0) writing=0;}
  if (fd>0) {
    fd_init_bytestream(stream,fd,bufsiz);
    stream->bytestream_mallocd=1; stream->bytestream_idstring=u8_strdup(fname);
    stream->bs_flags=stream->bs_flags|FD_BYTESTREAM_CANSEEK;
    if (lock) stream->bs_flags=stream->bs_flags|FD_BYTESTREAM_NEEDS_LOCK;
    if (writing == 0) stream->bs_flags=stream->bs_flags|FD_BYTESTREAM_READ_ONLY;
    stream->bytestream_maxpos=lseek(fd,0,SEEK_END);
    stream->bytestream_diskpos=lseek(fd,0,SEEK_SET);
    u8_init_mutex(&(stream->stream_lock));
    u8_free(localname);
    return stream;}
  else {
    fd_seterr3(fd_CantOpenFile,"fd_init_file_bytestream",localname);
    return NULL;}
}

FD_EXPORT fd_bytestream fd_open_dtype_file
   (u8_string fname,fd_bytestream_mode mode,int bufsiz)
{
  struct FD_BYTESTREAM *stream=
    u8_alloc(struct FD_BYTESTREAM);
  struct FD_BYTESTREAM *opened=
    fd_init_file_bytestream(stream,fname,mode,bufsiz);
  if (opened) return opened;
  else {
    u8_free(stream);
    return NULL;}
}

FD_EXPORT void fd_bytestream_close(fd_bytestream s,int flags)
{
  int dofree   =   (U8_BITP(flags,FD_BYTESTREAM_FREE));
  int close_fd = ! (U8_BITP(flags,FD_BYTESTREAM_NOCLOSE));
  int flush    = !  (U8_BITP(flags,FD_BYTESTREAM_NOFLUSH));

  /* Already closed */
  if (s->fd_fileno<0) return;

  /* Lock before closing */
  lock_stream(s);

  /* Flush data */
  if (flush) {
    bytestream_flush(s);
    fsync(s->fd_fileno);}

  if (close_fd) {
    if (s->bs_flags&FD_BYTESTREAM_SOCKET)
      shutdown(s->fd_fileno,SHUT_RDWR);
    close(s->fd_fileno);}
  s->fd_fileno=-1;

  if (dofree) {
    if (s->bytestream_idstring) {
      u8_free(s->bytestream_idstring);
      s->bytestream_idstring=NULL;}
    if (s->bs_bufstart) {
      u8_free(s->bs_bufstart);
      s->bs_bufstart=s->bs_bufptr=s->bs_buflim=NULL;}
    unlock_stream(s);
    u8_destroy_mutex(&(s->stream_lock));
    if (s->bytestream_mallocd) u8_free(s);}
  else unlock_stream(s);
}

FD_EXPORT void fd_bytestream_free(fd_bytestream s,int flags)
{
  fd_bytestream_close(s,flags|FD_BYTESTREAM_FREE);
}

FD_EXPORT void fd_bytestream_bufsize(fd_bytestream s,int bufsiz)
{
  lock_stream(s);
  bytestream_flush(s);
  {
    unsigned int ptroff=s->bs_bufptr-s->bs_bufstart;
    unsigned int endoff=s->bs_buflim-s->bs_bufstart;
    s->bs_bufstart=u8_realloc(s->bs_bufstart,bufsiz);
    s->bs_bufptr=s->bs_bufstart+ptroff; s->bs_buflim=s->bs_bufstart+endoff;
    s->bytestream_bufsiz=bufsiz;
  }
  unlock_stream(s);
}

FD_EXPORT fdtype bytestream_read_dtype(fd_bytestream s,int unlock)
{
  int first_byte;

  if ((s->bs_flags&FD_BYTESTREAM_READING) == 0)
    if (fd_set_read(s,1)<0) {
      if (unlock) unlock_stream(s);
      return FD_ERROR_VALUE;}

  first_byte=bytestream_probe_byte(s);

  if (first_byte==dt_ztype) {
    bytestream_read_byte(s);
    return zread_dtype(s,unlock);}
  else if (first_byte>=0x80) {
    /* Probably compressed */
    fd_seterr("NYI","fd_bytestream_read_type/zip",s->bytestream_idstring,FD_VOID);
    return FD_ERROR_VALUE;}
  else {
    fdtype result=fd_read_dtype((struct FD_BYTE_INPUT *)s);
    if (unlock) unlock_stream(s);
    return result;}
}
FD_EXPORT fdtype fd_bytestream_read_dtype(fd_bytestream s)
{
  lock_stream(s);
  return bytestream_read_dtype(s,FD_BYTESTREAM_UNLOCK);
}

FD_EXPORT int bytestream_write_dtype(fd_bytestream s,fdtype x,int unlock)
{
  int n_bytes; fd_off_t start;
  if ((s->bs_flags)&(FD_BYTESTREAM_READING))
    if (bytestream_set_read(s,0)<0) {
      if (unlock) unlock_stream(s);
      return -1;}
  if (fd_check_dtsize)
    start=fd_getpos(s);
  else start=(fd_off_t)-1;
  n_bytes=fd_write_dtype((struct FD_BYTE_OUTPUT *)s,x);
  if ((fd_check_dtsize) && (start>=0)) {
    fd_off_t end=fd_getpos(s);
    if ((end-start)!= n_bytes)
      u8_log((((s->bs_flags)&(FD_BYTESTREAM_CANSEEK)) ?
              (LOG_CRIT) : (LOG_ERR)),
             fd_InconsistentDTypeSize,
             "Inconsistent dtype length %d/%d for: %s",
             n_bytes,end-start,
             fd_dtype2buf(x,FD_DEBUG_OUTBUF_SIZE,_dbg_outbuf));
    else {
      bytestream_flush(s);
      end=fd_getpos(s);
      if ((end-start)!= n_bytes)
        u8_log((((s->bs_flags)&(FD_BYTESTREAM_CANSEEK)) ? (LOG_CRIT) : (LOG_ERR)),
               fd_InconsistentDTypeSize,
               "Inconsistent dtype length (on disk) %d/%d for: %s",
               n_bytes,end-start,
               fd_dtype2buf(x,FD_DEBUG_OUTBUF_SIZE,_dbg_outbuf));}}
  if (unlock) {
    if ((s->bs_bufptr-s->bs_bufstart)*4>=(s->bytestream_bufsiz*3))
      bytestream_flush(s);
    unlock_stream(s);}
  return n_bytes;
}
FD_EXPORT int fd_bytestream_write_dtype(fd_bytestream s,fdtype x)
{
  lock_stream(s);
  return bytestream_write_dtype(s,x,FD_BYTESTREAM_UNLOCK);
}

/* Structure functions */

static int fill_bytestream(struct FD_BYTESTREAM *df,int n)
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
  if (n>df->bytestream_bufsiz) {
    int new_size=df->bytestream_bufsiz;
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
    df->bytestream_bufsiz=new_size;}
  n_to_read=n-n_buffered;
  read_request=df->bytestream_bufsiz-n_buffered;
  while (bytes_read < n_to_read) {
    int delta;
    if ((delta=read(df->fd_fileno,df->bs_buflim,read_request-bytes_read))==0) break;
    if ((delta<0) && (errno) && (errno != EWOULDBLOCK)) {
      fd_seterr3(u8_strerror(errno),"fill_bytestream",u8s(df->bytestream_idstring));
      unlock_stream(df);
      return 0;}
    else if (delta<0) delta=0;
    df->bs_buflim=df->bs_buflim+delta;
    if (df->bytestream_diskpos>=0)
      df->bytestream_diskpos=df->bytestream_diskpos+delta;
    bytes_read=bytes_read+delta;}
  unlock_stream(df);
  return bytes_read;
}

FD_EXPORT
int bytestream_flush(fd_bytestream s)
{
  if (FD_BYTESTREAM_ISREADING(s)) {
    if (s->bs_buflim==s->bs_bufptr) return 0;
    else {
      s->bs_bufptr=s->bs_buflim=s->bs_bufstart;
      return 0;}}
  else if (s->bs_bufptr>s->bs_bufstart) {
    int bytes_written=writeall(s->fd_fileno,s->bs_bufstart,s->bs_bufptr-s->bs_bufstart);
    if (bytes_written<0) {
      return -1;}
    if ((s->bs_flags)&FD_BYTESTREAM_DOSYNC) fsync(s->fd_fileno);
    if ( (s->bs_flags&FD_BYTESTREAM_CANSEEK) && (s->bytestream_diskpos>=0) )
      s->bytestream_diskpos=s->bytestream_diskpos+bytes_written;
    if ((s->bytestream_maxpos>=0) && (s->bytestream_diskpos>s->bytestream_maxpos))
      s->bytestream_maxpos=s->bytestream_diskpos;
    /* Reset the buffer pointers */
    s->bs_bufptr=s->bs_bufstart;
    return bytes_written;}
  else {
    return 0;}
}

FD_EXPORT int fd_bytestream_flush(fd_bytestream s)
{
  lock_stream(s); {
    int rv=bytestream_flush(s);
    unlock_stream(s);
    return rv;}
}

/* Locking and unlocking */

FD_EXPORT void bytestream_lock(struct FD_BYTESTREAM *s)
{
  lock_stream(s);
}

FD_EXPORT void bytestream_unlock(struct FD_BYTESTREAM *s)
{
  unlock_stream(s);
}

FD_EXPORT int bytestream_lockfile(fd_bytestream s)
{
  if (s->bs_flags&FD_BYTESTREAM_LOCKED)
    return 1;
  else if ((u8_lock_fd(s->fd_fileno,1))>=0) {
    s->bs_flags=s->bs_flags|FD_BYTESTREAM_LOCKED;
    return 1;}
  return 0;
}

FD_EXPORT int bytestream_unlockfile(fd_bytestream s)
{
  if (!(s->bs_flags&FD_BYTESTREAM_LOCKED))
    return 1;
  else if ((u8_unlock_fd(s->fd_fileno))>=0) {
    s->bs_flags=s->bs_flags|FD_BYTESTREAM_LOCKED;
    return 1;}
  return 0;
}

FD_EXPORT int fd_bytestream_lockfile(fd_bytestream s)
{
  lock_stream(s); {
    int rv=bytestream_lockfile(s);
    unlock_stream(s);
    return rv;}
}

FD_EXPORT int fd_bytestream_unlockfile(fd_bytestream s)
{
  lock_stream(s); {
    int rv=bytestream_unlockfile(s);
    unlock_stream(s);
    return rv;}
}

FD_EXPORT int bytestream_set_read(fd_bytestream s,int read)
{
  if ((s->bs_flags)&FD_BYTESTREAM_READ_ONLY)
    if (read==0) {
      fd_seterr(fd_ReadOnlyStream,"fd_set_read",u8s(s->bytestream_idstring),FD_VOID);
      return -1;}
    else return 1;
  else if ((s->bs_flags)&FD_BYTESTREAM_READING)
    if (read) return 1;
    else {
      /* Lock the file descriptor if we need to. */
      if ((s->bs_flags)&FD_BYTESTREAM_NEEDS_LOCK) {
        if (u8_lock_fd(s->fd_fileno,1)) {
          (s->bs_flags)=(s->bs_flags)|FD_BYTESTREAM_LOCKED;}
        else return 0;}

      /* If we were reading, in order to start writing, we need
         to reset the pointer and make the ->bs_buflim point to the
         end of the allocated buffer. */
      s->bs_bufptr=s->bs_bufstart;
      s->bs_buflim=s->bs_bufstart+s->bytestream_bufsiz;
      /* Now we clear the bit */
      (s->bs_flags)=(s->bs_flags)&(~FD_BYTESTREAM_READING);
      return 1;}
  else if (read == 0) return 1;
  else {
    /* If we were writing, in order to start reading, we need
       to flush what is buffered to the output and collapse all
       of the pointers into the start. We also need to update bufsiz
       in case the output buffer grew while we were writing. */
    if (bytestream_flush(s)<0) {
      return -1;}
    /* Now we reset bufsiz in case we grew the buffer */
    s->bytestream_bufsiz=s->bs_buflim-s->bs_bufstart;
    /* Finally, we reset the pointers */
    s->bs_buflim=s->bs_bufptr=s->bs_bufstart;
    /* And set the reading bit */
    (s->bs_flags)=(s->bs_flags)|FD_BYTESTREAM_READING;
    return 1;}
}

FD_EXPORT int fd_set_read(fd_bytestream s,int read)
{
  lock_stream(s); {
    int rv=bytestream_set_read(s,read);
    unlock_stream(s);
    return rv;}
}

/* This gets the position when it isn't cached on the stream. */
FD_EXPORT fd_off_t _bytestream_getpos(fd_bytestream s,int unlock)
{
  fd_off_t current, pos;
  if (((s->bs_flags)&FD_BYTESTREAM_CANSEEK) == 0) {
    if (unlock) unlock_stream(s);
    return fd_reterr(fd_CantSeek,"fd_getpos",u8s(s->bytestream_idstring),FD_INT(pos));}
  if ((s->bs_flags)&FD_BYTESTREAM_READING) {
    current=lseek(s->fd_fileno,0,SEEK_CUR);
    /* If we are reading, we subtract the amount buffered from the
       actual filepos */
    s->bytestream_diskpos=current;
    pos=current-(s->bs_buflim-s->bs_bufstart);}
  else {
    current=lseek(s->fd_fileno,0,SEEK_CUR);
    s->bytestream_diskpos=current;
    /* If we are writing, we add the amount buffered for output to the
       actual filepos */
    pos=current+(s->bs_bufptr-s->bs_bufstart);}
  if (unlock) unlock_stream(s);
  return pos;
}
FD_EXPORT fd_off_t _fd_getpos(fd_bytestream s)
{
  fd_off_t current, pos;
  if (((s->bs_flags)&FD_BYTESTREAM_CANSEEK) == 0)
    return fd_reterr(fd_CantSeek,"fd_getpos",u8s(s->bytestream_idstring),FD_INT(pos));
  lock_stream(s);
  return _bytestream_getpos(s,FD_BYTESTREAM_UNLOCK);
}

FD_EXPORT fd_off_t bytestream_setpos(fd_bytestream s,fd_off_t pos)
{
  /* This is optimized for the case where the new position is
     in the range we have buffered. */
  if (((s->bs_flags)&FD_BYTESTREAM_CANSEEK) == 0) {
    return fd_reterr(fd_CantSeek,"fd_setpos",u8s(s->bytestream_idstring),FD_INT(pos));}
  else if (pos<0) {
    return fd_reterr(fd_BadSeek,"fd_setpos",u8s(s->bytestream_idstring),FD_INT(pos));}
  else {}

  /* Otherwise, you're going to move the file position, so flush any
     buffered data. */
  if ( (s->bytestream_diskpos>=0) && (FD_BYTESTREAM_ISREADING(s)) ) {
    fd_off_t delta=(pos-s->bytestream_diskpos);
    unsigned char *relptr=s->bs_buflim+delta;
    if ( (relptr >= s->bs_bufstart) && (relptr < s->bs_buflim )) {
      s->bs_bufptr=relptr;
      return pos;}}
  /* We're jumping out of what we have buffered */
  if (bytestream_flush(s)<0) {
    return -1;}
  fd_off_t newpos=lseek(s->fd_fileno,pos,SEEK_SET);
  if (newpos>=0) {
    s->bytestream_diskpos=newpos;
    return newpos;}
  else if (errno==EINVAL) {
    fd_off_t maxpos=lseek(s->fd_fileno,(fd_off_t)0,SEEK_END);
    s->bytestream_maxpos=s->bytestream_diskpos=maxpos;
    return fd_reterr(fd_OverSeek,"fd_setpos",u8s(s->bytestream_idstring),FD_INT(pos));}
  else {
    u8_graberrno("fd_setpos",u8s(s->bytestream_idstring));
    return -1;}
}
FD_EXPORT fd_off_t fd_setpos(fd_bytestream s,fd_off_t pos)
{
  lock_stream(s); {
    fd_off_t off=bytestream_setpos(s,pos);
    unlock_stream(s);
    return off;}
}

FD_EXPORT fd_off_t bytestream_movepos(fd_bytestream s,int delta)
{
  fd_off_t cur, rv;
  if (((s->bs_flags)&FD_BYTESTREAM_CANSEEK) == 0)
    return fd_reterr(fd_CantSeek,"fd_movepos",u8s(s->bytestream_idstring),FD_INT(delta));
  cur=fd_getpos(s);
  rv=bytestream_setpos(s,cur+delta);
  return rv;
}
FD_EXPORT fd_off_t fd_movepos(fd_bytestream s,int delta)
{
  lock_stream(s); {
    fd_off_t off=bytestream_movepos(s,delta);
    unlock_stream(s);
    return off;}
}

FD_EXPORT fd_off_t bytestream_endpos(fd_bytestream s)
{
  fd_off_t rv;
  if (((s->bs_flags)&FD_BYTESTREAM_CANSEEK) == 0)
    return fd_reterr(fd_CantSeek,"fd_endpos",u8s(s->bytestream_idstring),FD_VOID);
  bytestream_flush(s);
  rv=s->bytestream_maxpos=s->bytestream_diskpos=(lseek(s->fd_fileno,0,SEEK_END));
  return rv;
}

FD_EXPORT fd_off_t fd_endpos(fd_bytestream s)
{
  fd_off_t rv;
  if (((s->bs_flags)&FD_BYTESTREAM_CANSEEK) == 0)
    return fd_reterr(fd_CantSeek,"fd_endpos",u8s(s->bytestream_idstring),FD_VOID);
  lock_stream(s);
  bytestream_flush(s);
  rv=s->bytestream_maxpos=s->bytestream_diskpos=(lseek(s->fd_fileno,0,SEEK_END));
  unlock_stream(s);
  return rv;
}

/* Input functions */

FD_EXPORT int _bytestream_read_byte(fd_bytestream s)
{
  if (((s->bs_flags)&FD_BYTESTREAM_READING) == 0)
    if (bytestream_set_read(s,1)<0) return -1;
  if (fd_needs_bytes((fd_byte_input)s,1))
    return (*(s->bs_bufptr++));
  else return -1;
}

FD_EXPORT int _bytestream_probe_byte(fd_bytestream s)
{
  if (((s->bs_flags)&FD_BYTESTREAM_READING) == 0)
    if (bytestream_set_read(s,1)<0) return -1;
  if (fd_needs_bytes((fd_byte_input)s,1))
    return (*(s->bs_bufptr));
  else return -1;
}

FD_EXPORT unsigned int _bytestream_read_4bytes(fd_bytestream s)
{
  bytestream_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+4;
    return bytes;}
  else {fd_whoops(fd_UnexpectedEOD); return 0;}
}

FD_EXPORT fd_8bytes _fd_bytestream_read_8bytes(fd_bytestream s)
{
  bytestream_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,8)) {
    fd_8bytes bytes=fd_get_8bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+8;
    return bytes;}
  else {fd_whoops(fd_UnexpectedEOD); return 0;}
}

FD_EXPORT int _bytestream_read_bytes(fd_bytestream s,
                              unsigned char *bytes,int len,
                              u8_mutex *unlock)
{
  bytestream_start_read(s);
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
    s->bytestream_diskpos=s->bytestream_diskpos+n_read;
    if (unlock) u8_unlock_mutex(unlock);
    return len;}
}

FD_EXPORT fd_8bytes fd_bytestream_read_zint(fd_bytestream stream)
{
  lock_stream(stream); {
    int retval=bytestream_read_zint(stream);
    unlock_stream(stream);
    return retval;}
}

FD_EXPORT fd_8bytes _bytestream_read_zint(fd_bytestream stream)
{
  return bytestream_read_zint(stream);
}

FD_EXPORT fd_off_t _bytestream_read_off_t(fd_bytestream s)
{
  bytestream_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+4;
    return (fd_off_t) bytes;}
  else return ((fd_off_t)(-1));
}

FD_EXPORT int bytestream_read_ints(fd_bytestream s,int n_words,unsigned int *words)
{
  if (((s->bs_flags)&FD_BYTESTREAM_READING) == 0)
    if (bytestream_set_read(s,1)<0) {
      return -1;}
  /* This is special because we ignore the buffer if we can. */
  if ((s->bs_flags)&FD_BYTESTREAM_CANSEEK) {
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
        s->bytestream_diskpos=s->bytestream_diskpos+delta;
        bytes_read+=delta;}}
#if (!(WORDS_BIGENDIAN))
    {int i=0; while (i < n_words) {
        words[i]=fd_host_order(words[i]); i++;}}
#endif
    return bytes_read;}
  else if (fd_needs_bytes((fd_byte_input)s,n_words*4)) {
    int i=0; while (i<n_words) {
      int word=bytestream_read_4bytes(s);
      words[i++]=word;}
    return n_words*4;}
  else {
    return -1;}
}
FD_EXPORT int fd_bytestream_read_ints(fd_bytestream s,
                              int n_words,
                              unsigned int *words)
{
  lock_stream(s); {
    int n=bytestream_read_ints(s,n_words,words);
    unlock_stream(s);
    return n;}
}

/* Write functions */

FD_EXPORT int _bytestream_write_byte(fd_bytestream s,int b)
{
  if ((s->bs_flags)&FD_BYTESTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  if (s->bs_bufptr>=s->bs_buflim)
    bytestream_flush(s);
  *(s->bs_bufptr++)=b;
  return 1;
}

FD_EXPORT int _bytestream_write_4bytes(fd_bytestream s,fd_4bytes w)
{
  if ((s->bs_flags)&FD_BYTESTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  if (s->bs_bufptr+4>=s->bs_buflim)
    bytestream_flush(s);
  *(s->bs_bufptr++)=w>>24;
  *(s->bs_bufptr++)=((w>>16)&0xFF);
  *(s->bs_bufptr++)=((w>>8)&0xFF);
  *(s->bs_bufptr++)=((w>>0)&0xFF);
  return 4;
}

FD_EXPORT int _bytestream_write_8bytes(fd_bytestream s,fd_8bytes w)
{
  if ((s->bs_flags)&FD_BYTESTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  if (s->bs_bufptr+8>=s->bs_buflim)
    bytestream_flush(s);
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

FD_EXPORT int bytestream_write4_at(fd_bytestream s,fd_4bytes w,fd_off_t off)
{
  if ((s->bs_flags)&FD_BYTESTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  bytestream_flush(s);
  if (off>=0) bytestream_setpos(s,off);
  *(s->bs_bufptr++)=w>>24;
  *(s->bs_bufptr++)=((w>>16)&0xFF);
  *(s->bs_bufptr++)=((w>>8)&0xFF);
  *(s->bs_bufptr++)=((w>>0)&0xFF);
  bytestream_flush(s);
  return 4;
}
FD_EXPORT int fd_write_4bytes_at(fd_bytestream s,fd_4bytes w,fd_off_t off)
{
  bytestream_lock(s); {
    int rv=bytestream_write4_at(s,off,w);
    bytestream_unlock(s);
    return rv;}
}

FD_EXPORT long long bytestream_read4_at(fd_bytestream s,fd_off_t off)
{
  if (!((s->bs_flags)&FD_BYTESTREAM_READING))
    if (fd_set_read(s,1)<0) {
      return -1;}
  if (off>=0) bytestream_setpos(s,off);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    fd_8bytes bytes=fd_get_4bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+4;
    return bytes;}
  else return -1;
}
FD_EXPORT long long fd_read_4bytes_at(fd_bytestream s,fd_off_t off)
{
  bytestream_lock(s); {
    long long rv=bytestream_read4_at(s,off);
    bytestream_unlock(s);
    return rv;}
}

FD_EXPORT int bytestream_write8_at(fd_bytestream s,fd_8bytes w,fd_off_t off)
{
  if ((s->bs_flags)&FD_BYTESTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  bytestream_flush(s);
  if (off>=0) bytestream_setpos(s,off);
  *(s->bs_bufptr++)=((w>>56)&0xFF);
  *(s->bs_bufptr++)=((w>>48)&0xFF);
  *(s->bs_bufptr++)=((w>>40)&0xFF);
  *(s->bs_bufptr++)=((w>>32)&0xFF);
  *(s->bs_bufptr++)=((w>>24)&0xFF);
  *(s->bs_bufptr++)=((w>>16)&0xFF);
  *(s->bs_bufptr++)=((w>>8)&0xFF);
  *(s->bs_bufptr++)=((w>>0)&0xFF);
  bytestream_flush(s);
  return 4;
}
FD_EXPORT int fd_write_8bytes_at(fd_bytestream s,fd_8bytes w,fd_off_t off)
{
  bytestream_lock(s); {
    int rv=bytestream_write8_at(s,off,w);
    bytestream_unlock(s);
    return rv;}
}

FD_EXPORT fd_8bytes bytestream_read8_at(fd_bytestream s,fd_off_t off,int *err)
{
  if (!((s->bs_flags)&FD_BYTESTREAM_READING))
    if (fd_set_read(s,1)<0) {
      if (err) *err=-1;
      return 0;}
  if (off>=0) bytestream_setpos(s,off);
  if (fd_needs_bytes((fd_byte_input)s,8)) {
    fd_8bytes bytes=fd_get_8bytes(s->bs_bufptr);
    s->bs_bufptr=s->bs_bufptr+8;
    return bytes;}
  else {
    if (err) *err=-1;
    return 0;}
}
FD_EXPORT fd_8bytes fd_read_8bytes_at(fd_bytestream s,fd_off_t off,int *err)
{
  bytestream_lock(s); {
    int rv=bytestream_read8_at(s,off,err);
    bytestream_unlock(s);
    return rv;}
}

FD_EXPORT int _bytestream_write_zint(struct FD_BYTESTREAM *s,fd_8bytes val)
{
  return bytestream_write_zint(s,val);
}

FD_EXPORT int _bytestream_write_bytes(fd_bytestream s,const unsigned char *bytes,int n)
{
  if ((s->bs_flags)&FD_BYTESTREAM_READING)
    if (bytestream_set_read(s,0)<0) {
      return -1;}
  /* If there isn't space, flush the stream */
  if (s->bs_bufptr+n>=s->bs_buflim)
    bytestream_flush(s);
  /* If there still isn't space (bufsiz too small),
     call writeall and advance the filepos to reflect the
     written bytes. */
  if (s->bs_bufptr+n>=s->bs_buflim) {
    int bytes_written=writeall(s->fd_fileno,bytes,n);
#if FD_DEBUG_DTYPE_IO
    u8_log(LOG_DEBUG,"DTSWRITE",
           "Wrote %d/%d bytes to %s#%d, %d/%d bytes in buffer",
           bytes_written,n,
           s->fd_bytestream_id,s->fd_fileno,
           s->bs_bufptr-s->bs_bufstart,
           s->bs_buflim-s->bs_bufstart);
#endif
    if (bytes_written<0) return bytes_written;
    s->bytestream_diskpos=s->bytestream_diskpos+bytes_written;}
  else {
    memcpy(s->bs_bufptr,bytes,n); s->bs_bufptr=s->bs_bufptr+n;}
  return n;
}
FD_EXPORT int fd_bytestream_write_bytes
  (fd_bytestream s,const unsigned char *bytes,int n)
{
  lock_stream(s); {
    int n=_bytestream_write_bytes(s,bytes,n);
    unlock_stream(s);
    return n;}
}

FD_EXPORT int bytestream_write_ints(fd_bytestream s,int len,unsigned int *words)
{
  if (((s->bs_flags))&FD_BYTESTREAM_READING)
    if (bytestream_set_read(s,0)<0) return -1;
  /* This is special because we ignore the buffer if we can. */
  if (((s->bs_flags))&FD_BYTESTREAM_CANSEEK) {
    fd_off_t real_pos; int bytes_written;
    bytestream_flush(s);
    real_pos=bytestream_getpos(s,FD_STREAM_LOCKED);
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
    /* Otherwise, we write them a word at a time. */
    int i=0; while (i<len) {
      int word=words[i++];
      bytestream_write_4bytes(s,word);}
    return len*4;}
}
FD_EXPORT int fd_bytestream_write_ints(fd_bytestream s,int len,unsigned int *words)
{
  lock_stream(s); {
    int n=bytestream_write_ints(s,len,words);
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
static fdtype zread_dtype(struct FD_BYTESTREAM *s,int unlock)
{
  fdtype result;
  struct FD_BYTE_INPUT in;
  ssize_t n_bytes=fd_bytestream_read_zint(s), dbytes;
  unsigned char *bytes;
  int retval=-1;
  bytes=u8_malloc(n_bytes);
  if (unlock)
    retval=bytestream_read_bytes(s,bytes,n_bytes,&(s->stream_lock));
  else retval=bytestream_read_bytes(s,bytes,n_bytes,NULL);
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

FD_EXPORT fdtype fd_zread_dtype(struct FD_BYTESTREAM *s)
{
  lock_stream(s);
  return zread_dtype(s,FD_BYTESTREAM_UNLOCK);
}

/* This reads a non frame value with compression. */
static int zwrite_dtype(struct FD_BYTESTREAM *s,fdtype x,int unlock)
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
  bytestream_write_byte(s,dt_ztype);
  size=bytestream_write_zint(s,zlen); size=size+zlen;
  if (bytestream_write_bytes(s,zbytes,zlen)<0) size=-1;
  bytestream_flush(s);
  u8_free(zbytes); u8_free(out.bs_bufstart);
  if (unlock) unlock_stream(s);
  return size;
}

FD_EXPORT int fd_zwrite_dtype(struct FD_BYTESTREAM *s,fdtype x)
{
  lock_stream(s);
  return zwrite_dtype(s,x,FD_BYTESTREAM_UNLOCK);
}

static int zwrite_dtypes(struct FD_BYTESTREAM *s,fdtype x,int unlock)
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
  bytestream_write_byte(s,dt_ztype);
  size=1+bytestream_write_zint(s,zlen); size=size+zlen;
  retval=bytestream_write_bytes(s,zbytes,zlen);
  u8_free(zbytes); u8_free(out.bs_bufstart);
  if (unlock) unlock_stream(s);
  if (retval<0) return retval;
  else return size;
}

FD_EXPORT int fd_zwrite_dtypes(struct FD_BYTESTREAM *s,fdtype x)
{
  lock_stream(s);
  return zwrite_dtypes(s,x,FD_BYTESTREAM_UNLOCK);
}

/* Files 2 dtypes */

FD_EXPORT fdtype fd_read_dtype_from_file(u8_string filename)
{
  struct FD_BYTESTREAM *stream=u8_alloc(struct FD_BYTESTREAM);
  ssize_t filesize=u8_file_size(filename);
  ssize_t bufsize=(filesize+1024);
  if (filesize<0) {
    fd_seterr(fd_FileNotFound,"fd_file2dtype",u8_strdup(filename),FD_VOID);
    return FD_ERROR_VALUE;}
  else if (filesize==0) {
    fd_seterr("Zero-length file","fd_file2dtype",u8_strdup(filename),FD_VOID);
    return FD_ERROR_VALUE;}
  else {
    struct FD_BYTESTREAM *opened=
      fd_init_file_bytestream(stream,filename,FD_BYTESTREAM_READ,bufsize);
    if (opened) lock_stream(opened);
    if (opened) {
      fdtype result=FD_VOID;
      int byte1=bytestream_read_byte(opened);
      int zip=(byte1>=0x80);
      if (opened->bs_bufptr > opened->bs_bufstart)
        opened->bs_bufptr--;
      else fd_setpos(opened,0);
      if (zip)
        result=zread_dtype(opened,FD_STREAM_LOCKED);
      else result=bytestream_read_dtype(opened,FD_STREAM_LOCKED);
      unlock_stream(opened);
      fd_bytestream_free(opened,1);
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
  struct FD_BYTESTREAM *stream=u8_alloc(struct FD_BYTESTREAM);
  struct FD_BYTESTREAM *opened=
    fd_init_file_bytestream(stream,filename,FD_BYTESTREAM_WRITE,bufsize);
  if (opened) lock_stream(opened);
  if (opened) {
    size_t len=(zip)?
      (zwrite_dtype(opened,object,FD_STREAM_LOCKED)):
      (bytestream_write_dtype(opened,object,FD_STREAM_LOCKED));
    unlock_stream(opened);
    fd_bytestream_free(opened,1);
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

FD_EXPORT void fd_init_bytestream_c()
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
