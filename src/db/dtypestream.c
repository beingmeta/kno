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

static fdtype zread_dtype(struct FD_DTYPE_STREAM *s);

static int fill_dtype_stream(struct FD_DTYPE_STREAM *df,int n);

static u8_byte _dbg_outbuf[FD_DEBUG_OUTBUF_SIZE];

/* Locking functions */

#if WIN32
#include <io.h>
#include <sys/locking.h>
FD_EXPORT int fd_lock_fd(int fd,int for_write)
{
#if 0
  return _locking(fd,_LK_LOCK,0);
#endif
  return 1;
}
FD_EXPORT int fd_unlock_fd(int fd)
{
#if 0
  return _locking(fd,_LK_UNLCK,0);
#endif
  return 1;
}
#elif FD_WITH_FILE_LOCKING
FD_EXPORT int fd_lock_fd(int fd,int for_write)
{
  struct flock lock_data;
  int retval;
  lock_data.l_whence=0; lock_data.l_start=0; lock_data.l_len=0;
  lock_data.l_type=((for_write) ? (F_WRLCK) : (F_RDLCK));
  lock_data.l_pid=getpid();
  retval=fcntl(fd,F_SETLK,&lock_data);
  if (retval == 0) {errno=0;}
  return retval;
}
FD_EXPORT int fd_unlock_fd(int fd)
{
  struct flock lock_data;
  int retval;
  lock_data.l_whence=0; lock_data.l_start=0; lock_data.l_len=0;
  lock_data.l_type=F_UNLCK; lock_data.l_pid=getpid();
  retval=fcntl(fd,F_SETLK,&lock_data);
  errno=0;
  return retval;
}
#else
FD_EXPORT int fd_lock_fd(int fd,int for_write)
{
  return 1;
}
FD_EXPORT int fd_unlock_fd(int fd)
{
  return 1;
}
#endif

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
    s->fd_buflim=s->fd_bufptr; s->fd_bufsiz=bufsiz;
    s->fd_mallocd=0; s->fd_fileno=sock; s->fd_dtsid=NULL;
    s->fd_filepos=-1; s->fd_maxpos=-1;
    s->fd_dts_fillfn=fill_dtype_stream; s->fd_dts_flushfn=NULL;
    s->fd_dts_flags|=FD_DTSTREAM_READING|FD_BYTEBUF_MALLOCD;
    u8_init_mutex(&(s->fd_lock));
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
  fd=open(localname,flags,0666); stream->fd_dts_flags=0;
  /* If we fail and we're modifying, try to open read-only */
  if ((fd<0) && (mode == FD_DTSTREAM_MODIFY)) {
    fd=open(localname,O_RDONLY,0666);
    if (fd>0) writing=0;}
  if (fd>0) {
    fd_init_dtype_stream(stream,fd,bufsiz);
    stream->fd_mallocd=1; stream->fd_dtsid=u8_strdup(fname);
    stream->fd_dts_flags=stream->fd_dts_flags|FD_DTSTREAM_CANSEEK;
    if (lock) stream->fd_dts_flags=stream->fd_dts_flags|FD_DTSTREAM_NEEDS_LOCK;
    if (writing == 0) stream->fd_dts_flags=stream->fd_dts_flags|FD_DTSTREAM_READ_ONLY;
    stream->fd_maxpos=lseek(fd,0,SEEK_END);
    stream->fd_filepos=lseek(fd,0,SEEK_SET);
    u8_init_mutex(&(stream->fd_lock));
    u8_free(localname);
    return stream;}
  else {
    fd_seterr3(fd_CantOpenFile,"fd_init_dtype_file_stream",localname);
    return NULL;}
}

FD_EXPORT fd_dtype_stream fd_open_dtype_file_x
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

FD_EXPORT void fd_dtsclose(fd_dtype_stream s,int close_fd)
{
  /* Already closed */
  if (s->fd_fileno<0) return;
  /* Flush data */
  if ((s->fd_dts_flags&FD_DTSTREAM_READING) == 0)
    fd_dtsflush(s);

  fd_lock_struct(s);

  if (s->fd_bufstart) {
    u8_free(s->fd_bufstart);
    s->fd_bufstart=s->fd_bufptr=s->fd_buflim=NULL;}
  else {/* Redundant close.  Warn? */}
  if (close_fd>0) {
    fsync(s->fd_fileno);
    if (s->fd_dts_flags&FD_DTSTREAM_SOCKET)
      shutdown(s->fd_fileno,SHUT_RDWR);
    close(s->fd_fileno);}
  s->fd_fileno=-1;
  if (s->fd_dtsid) {
    u8_free(s->fd_dtsid);
    s->fd_dtsid=NULL;}
  u8_destroy_mutex(&(s->fd_lock));
  if (s->fd_mallocd) u8_free(s);
}

FD_EXPORT void fd_dtsbufsize(fd_dtype_stream s,int bufsiz)
{
  fd_dtsflush(s);
  {
    unsigned int ptroff=s->fd_bufptr-s->fd_bufstart, endoff=s->fd_buflim-s->fd_bufstart;
    s->fd_bufstart=u8_realloc(s->fd_bufstart,bufsiz);
    s->fd_bufptr=s->fd_bufstart+ptroff; s->fd_buflim=s->fd_bufstart+endoff;
    s->fd_bufsiz=bufsiz;
  }
}

FD_EXPORT fdtype fd_dtsread_dtype(fd_dtype_stream s)
{
  int first_byte;
  if ((s->fd_dts_flags&FD_DTSTREAM_READING) == 0)
    if (fd_set_read(s,1)<0) return FD_ERROR_VALUE;
  first_byte=fd_dtsprobe_byte(s);
  if (first_byte==dt_ztype) {
    fd_dtsread_byte(s);
    return zread_dtype(s);}
  else if (first_byte>=0x80) {
    /* Probably compressed */
    fd_seterr("NYI","fd_dtsread_type/zip",s->id,FD_VOID);
    return FD_ERROR_VALUE;}
  else return fd_read_dtype((struct FD_BYTE_INPUT *)s);
}
FD_EXPORT int fd_dtswrite_dtype(fd_dtype_stream s,fdtype x)
{
  int n_bytes; fd_off_t start;
  if ((s->fd_dts_flags)&(FD_DTSTREAM_READING))
    if (fd_set_read(s,0)<0) return -1;
  if (fd_check_dtsize) start=fd_getpos(s); else start=(fd_off_t)-1;
  n_bytes=fd_write_dtype((struct FD_BYTE_OUTPUT *)s,x);
  if ((fd_check_dtsize) && (start>=0)) {
    fd_off_t end=fd_getpos(s);
    if ((end-start)!= n_bytes)
      u8_log((((s->fd_dts_flags)&(FD_DTSTREAM_CANSEEK)) ?
              (LOG_CRIT) :
              (LOG_ERR)),
             fd_InconsistentDTypeSize,
             "Inconsistent dtype length %d/%d for: %s",
             n_bytes,end-start,
             fd_dtype2buf(x,FD_DEBUG_OUTBUF_SIZE,_dbg_outbuf));
    else {
      fd_dtsflush(s); end=fd_getpos(s);
      if ((end-start)!= n_bytes)
        u8_log((((s->fd_dts_flags)&(FD_DTSTREAM_CANSEEK)) ? (LOG_CRIT) : (LOG_ERR)),
               fd_InconsistentDTypeSize,
               "Inconsistent dtype length (on disk) %d/%d for: %s",
               n_bytes,end-start,
               fd_dtype2buf(x,FD_DEBUG_OUTBUF_SIZE,_dbg_outbuf));}}
  if ((s->fd_bufptr-s->fd_bufstart)*4>=(s->fd_bufsiz*3))
    fd_dtsflush(s);
  return n_bytes;
}

/* Structure functions */

static int fill_dtype_stream(struct FD_DTYPE_STREAM *df,int n)
{
  int n_buffered, n_to_read, read_request, bytes_read=0;
  /* Shrink what you've already read, adjusting the filepos */
  if (df->fd_filepos>=0) df->fd_filepos=df->fd_filepos+(df->fd_bufptr-df->fd_bufstart);
  memmove(df->fd_bufstart,df->fd_bufptr,(df->fd_buflim-df->fd_bufptr));
  df->fd_buflim=(df->fd_bufstart)+(df->fd_buflim-df->fd_bufptr);
  df->fd_bufptr=df->fd_bufstart;
  /* Make sure that there's enough space */
  if (n>df->fd_bufsiz) {
    int new_size=df->fd_bufsiz;
    unsigned char *newbuf;
    size_t end_pos=df->fd_buflim-df->fd_bufstart;
    size_t ptr_pos=df->fd_bufptr-df->fd_bufstart;
    while (new_size<n)
      if (new_size>=0x40000) new_size=new_size+0x40000;
      else new_size=new_size*2;
    newbuf=u8_realloc(df->fd_bufstart,new_size);
    df->fd_bufstart=newbuf; df->fd_bufptr=newbuf+ptr_pos; df->fd_buflim=newbuf+end_pos;
    df->fd_bufsiz=new_size;}
  n_buffered=df->fd_buflim-df->fd_bufstart;
  n_to_read=n-n_buffered;
  read_request=df->fd_bufsiz-n_buffered;
  while (bytes_read < n_to_read) {
    int delta;
    if ((delta=read(df->fd_fileno,df->fd_buflim,read_request-bytes_read))==0) break;
    if ((delta<0) && (errno) && (errno != EWOULDBLOCK)) {
      fd_seterr3(u8_strerror(errno),"fill_dtype_stream",
                 ((df->fd_dtsid) ?(u8_strdup(df->fd_dtsid)):(NULL)));
      return 0;}
    else if (delta<0) delta=0;
    df->fd_buflim=df->fd_buflim+delta;
    bytes_read=bytes_read+delta;}
  return bytes_read;
}

FD_EXPORT int fd_dtsflush(fd_dtype_stream s)
{
  fd_lock_struct(s);
  if (s->fd_dts_flags&FD_DTSTREAM_READING) {
    /* When flushing a read stream, we just discard whatever
       is in the input buffer. */
    int leftover=s->fd_buflim-s->fd_bufstart;
    if ((s->fd_dts_flags&FD_DTSTREAM_CANSEEK) && (s->fd_filepos>=0))
      /* Advance the virtual file pointer to reflect where
         the file pointer really is. */
      s->fd_filepos=s->fd_filepos+(s->fd_buflim-s->fd_bufstart);
    /* Reset the buffer pointers */
    s->fd_buflim=s->fd_bufptr=s->fd_bufstart;
    fd_unlock_struct(s);
    return leftover;}
  else {
    int bytes_written=writeall(s->fd_fileno,s->fd_bufstart,s->fd_bufptr-s->fd_bufstart);
#if FD_DEBUG_DTYPEIO
    u8_log(LOG_DEBUG,"DTSFLUSH",
           "Wrote %d bytes to %s#%d, %d/%d bytes in buffer",
           bytes_written,s->fd_dtsid,s->fd_fileno,s->fd_bufptr-s->fd_bufstart,s->fd_buflim-s->fd_bufstart);
#endif
    if (bytes_written<0) {
      fd_unlock_struct(s);
      return -1;}
    if ((s->fd_dts_flags)&FD_DTSTREAM_DOSYNC) fsync(s->fd_fileno);
    if ((s->fd_dts_flags&FD_DTSTREAM_CANSEEK) && (s->fd_filepos>=0))
      s->fd_filepos=s->fd_filepos+bytes_written;
    if (bytes_written<0) {

      return -1;}
    if ((s->fd_dts_flags)&FD_DTSTREAM_DOSYNC) fsync(s->fd_fileno);
    if ((s->fd_dts_flags&FD_DTSTREAM_CANSEEK) && (s->fd_filepos>=0))
      s->fd_filepos=s->fd_filepos+bytes_written;
    /* Reset maxpos if neccessary. */
    if ((s->fd_maxpos>=0) && (s->fd_filepos>s->fd_maxpos))
      s->fd_maxpos=s->fd_filepos;
    /* Reset the buffer pointers */
    s->fd_bufptr=s->fd_bufstart;
    fd_unlock_struct(s);
    return bytes_written;}
}

FD_EXPORT int fd_dtslock(fd_dtype_stream s)
{
  if (s->fd_dts_flags&FD_DTSTREAM_LOCKED) return 1;
  else if (fd_lock_fd(s->fd_fileno,1)) {
    s->fd_dts_flags=s->fd_dts_flags|FD_DTSTREAM_LOCKED;
    return 1;}
  else return 0;
}

FD_EXPORT int fd_dtsunlock(fd_dtype_stream s)
{
  if (!((s->fd_dts_flags)&FD_DTSTREAM_LOCKED)) return 1;
  else if (fd_unlock_fd(s->fd_fileno)) {
    s->fd_dts_flags=s->fd_dts_flags&(~FD_DTSTREAM_LOCKED);
    return 1;}
  else return 0;
}

FD_EXPORT int fd_set_read(fd_dtype_stream s,int read)
{
  if ((s->fd_dts_flags)&FD_DTSTREAM_READ_ONLY)
    if (read==0) {
      fd_seterr(fd_ReadOnlyStream,"fd_set_read",u8_strdup(s->fd_dtsid),FD_VOID);
      return -1;}
    else return 1;
  else if ((s->fd_dts_flags)&FD_DTSTREAM_READING)
    if (read) return 1;
    else {
      /* Lock the file descriptor if we need to. */
      if ((s->fd_dts_flags)&FD_DTSTREAM_NEEDS_LOCK) {
        if (fd_lock_fd(s->fd_fileno,1)) {
          (s->fd_dts_flags)=(s->fd_dts_flags)|FD_DTSTREAM_LOCKED;}
        else return 0;}
      if (((s->fd_dts_flags)&FD_DTSTREAM_CANSEEK) && (s->fd_filepos>=0))
        /* We reset the virtual filepos to match the real filepos
           based on how much we still had to read in the buffer. */
        s->fd_filepos=s->fd_filepos+(s->fd_buflim-s->fd_bufstart);
      /* If we were reading, in order to start writing, we need
         to reset the pointer and make the ->fd_buflim point to the
         end of the allocated buffer. */
      s->fd_bufptr=s->fd_bufstart; s->fd_buflim=s->fd_bufstart+s->fd_bufsiz;
      /* Now we clear the bit */
      (s->fd_dts_flags)=(s->fd_dts_flags)&(~FD_DTSTREAM_READING);
      return 1;}
  else if (read == 0) return 1;
  else {
    /* If we were writing, in order to start reading, we need
       to flush what is buffered to the output and collapse all
       of the pointers into the start. We also need to update bufsiz
       in case the output buffer grew while we were writing. */
    if (fd_dtsflush(s)<0) return -1;
    /* Now we reset bufsiz in case we grew the buffer */
    s->fd_bufsiz=s->fd_buflim-s->fd_bufstart;
    /* Finally, we reset the pointers */
    s->fd_buflim=s->fd_bufptr=s->fd_bufstart;
    /* And set the reading bit */
    (s->fd_dts_flags)=(s->fd_dts_flags)|FD_DTSTREAM_READING;
    return 1;}
}

/* This gets the position when it isn't cached on the stream. */
FD_EXPORT fd_off_t _fd_getpos(fd_dtype_stream s)
{
  if (((s->fd_dts_flags)&FD_DTSTREAM_CANSEEK) == 0) return -1;
  else if ((s->fd_dts_flags)&FD_DTSTREAM_READING) {
    fd_off_t current=lseek(s->fd_fileno,0,SEEK_CUR);
    s->fd_filepos=current-(s->fd_buflim-s->fd_bufstart);
    return s->fd_filepos+(s->fd_bufptr-s->fd_bufstart);}
  else {
    fd_off_t current=lseek(s->fd_fileno,0,SEEK_CUR);
    s->fd_filepos=current;
    return s->fd_filepos+(s->fd_bufptr-s->fd_bufstart);}
}
FD_EXPORT fd_off_t fd_setpos(fd_dtype_stream s,fd_off_t pos)
{
  /* This is optimized for the case where the new position is
     in the range we have buffered. */
  if (((s->fd_dts_flags)&FD_DTSTREAM_CANSEEK) == 0) return -1;
  else if (pos<0)
    return fd_reterr(fd_BadSeek,"fd_setpos",
                     ((s->fd_dtsid)?(u8_strdup(s->fd_dtsid)):(NULL)),
                     FD_INT(pos));
  else if (s->fd_filepos<0) {
    /* We're not tracking filepos, so we'll check by hand. */
    fd_off_t maxpos;
    if (fd_dtsflush(s)<0) return -1;
    maxpos=lseek(s->fd_fileno,(fd_off_t)0,SEEK_END);
    if (maxpos<0)
      return fd_reterr(fd_BadLSEEK,"fd_setpos",
                       ((s->fd_dtsid)?(u8_strdup(s->fd_dtsid)):(NULL)),
                       FD_INT(pos));
    else if (pos<maxpos)
      return lseek(s->fd_fileno,pos,SEEK_SET);
    else return fd_reterr(fd_OverSeek,"fd_setpos",
                          ((s->fd_dtsid)?(u8_strdup(s->fd_dtsid)):(NULL)),
                          FD_INT(pos));}
  else if ((s->fd_filepos>=0) && (pos>s->fd_maxpos)) {
    /* We just sought out of bounds.  */
    fd_dtsflush(s); /* Reset the buffer pointers. */
    if (pos<s->fd_maxpos) /* OK if that made us legal. */
      return (s->fd_filepos=(lseek(s->fd_fileno,pos,SEEK_SET)));
    /* Check the disk file for real size info */
    else s->fd_filepos=s->fd_maxpos=lseek(s->fd_fileno,0,SEEK_END);
    /* If we're legal now (someone else wrote to the file),
       go ahead and seek, otherwise return -1. */
    if (pos<=s->fd_maxpos)
      return (s->fd_filepos=(lseek(s->fd_fileno,pos,SEEK_SET)));
    else return fd_reterr(fd_OverSeek,"fd_setpos",
                          ((s->fd_dtsid)?(u8_strdup(s->fd_dtsid)):(NULL)),
                          FD_INT(pos));}
  else if ((s->fd_dts_flags)&FD_DTSTREAM_READING)
    if ((pos>s->fd_filepos) && ((pos-s->fd_filepos)<(s->fd_buflim-s->fd_bufstart))) {
      /* Here's the case where we are seeking within the buffer. */
      s->fd_bufptr=s->fd_bufstart+(pos-s->fd_filepos);
      return pos;}
  fd_dtsflush(s);
  return (s->fd_filepos=(lseek(s->fd_fileno,pos,SEEK_SET)));
}
FD_EXPORT fd_off_t fd_movepos(fd_dtype_stream s,int delta)
{
  if (((s->fd_dts_flags)&FD_DTSTREAM_CANSEEK) == 0) return -1;
  else if (s->fd_filepos<0) {
    /* We're not tracking filepos, so we'll check by hand. */
    fd_off_t pos, maxpos;
    fd_dtsflush(s);
    pos=lseek(s->fd_fileno,0,SEEK_CUR);
    maxpos=lseek(s->fd_fileno,0,SEEK_END);
    if ((pos<0)||(maxpos<0))
      return fd_reterr(fd_BadLSEEK,"fd_movepos",
                       ((s->fd_dtsid)?(u8_strdup(s->fd_dtsid)):(NULL)),
                       FD_INT(delta));
    else if ((pos+delta)<0)
      return fd_reterr(fd_UnderSeek,"fd_movepos",
                       ((s->fd_dtsid)?(u8_strdup(s->fd_dtsid)):(NULL)),
                       FD_INT(pos));
    else if ((pos+delta)<maxpos)
      return lseek(s->fd_fileno,(pos+delta),SEEK_SET);
    else return fd_reterr(fd_OverSeek,"fd_movepos",
                          ((s->fd_dtsid)?(u8_strdup(s->fd_dtsid)):(NULL)),
                          FD_INT(pos));}
  else if ((s->fd_filepos+delta)>s->fd_maxpos) {
    int pos=s->fd_filepos+delta;
    /* We just sought out of bounds.  */
    fd_dtsflush(s);
    /* Reset the buffer pointers, this flushes output to disk
       and may update s->fd_maxpos (if it went at the end of the file). */
    if (pos<s->fd_maxpos) /* OK if that made us legal. */
      return (s->fd_filepos=(lseek(s->fd_fileno,pos,SEEK_SET)));
    /* Check the disk file for real size info */
    else s->fd_filepos=s->fd_maxpos=lseek(s->fd_fileno,0,SEEK_END);
    /* If we're legal now (someone else wrote to the file),
       go ahead and seek, otherwise return -1. */
    if (pos<=s->fd_maxpos) return (s->fd_filepos=(lseek(s->fd_fileno,pos,SEEK_SET)));
    else return fd_reterr(fd_OverSeek,"fd_movepos",
                          ((s->fd_dtsid)?(u8_strdup(s->fd_dtsid)):(NULL)),
                          FD_INT(pos));}
  else if ((s->fd_dts_flags)&FD_DTSTREAM_READING) {
    unsigned char *relpos=s->fd_bufptr+delta;
    if ((relpos>s->fd_bufstart) && (relpos<s->fd_buflim)) {
      s->fd_bufptr=relpos; return s->fd_filepos+(s->fd_bufptr-s->fd_bufstart);}}
  fd_dtsflush(s);
  return (s->fd_filepos=(lseek(s->fd_fileno,delta,SEEK_CUR)));
}
FD_EXPORT fd_off_t fd_endpos(fd_dtype_stream s)
{
  if (((s->fd_dts_flags)&FD_DTSTREAM_CANSEEK) == 0) return -1;
  fd_dtsflush(s);
  return (s->fd_maxpos=s->fd_filepos=(lseek(s->fd_fileno,0,SEEK_END)));
}

/* Input functions */

FD_EXPORT int _fd_dtsread_byte(fd_dtype_stream s)
{
  if (((s->fd_dts_flags)&FD_DTSTREAM_READING) == 0)
    if (fd_set_read(s,1)<0) return -1;
  if (fd_needs_bytes((fd_byte_input)s,1))
    return (*(s->fd_bufptr++));
  else return -1;
}

FD_EXPORT int _fd_dtsprobe_byte(fd_dtype_stream s)
{
  if (((s->fd_dts_flags)&FD_DTSTREAM_READING) == 0)
    if (fd_set_read(s,1)<0) return -1;
  if (fd_needs_bytes((fd_byte_input)s,1))
    return (*(s->fd_bufptr));
  else return -1;
}

FD_EXPORT unsigned int _fd_dtsread_4bytes(fd_dtype_stream s)
{
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->fd_bufptr);
    s->fd_bufptr=s->fd_bufptr+4;
    return bytes;}
  else {fd_whoops(fd_UnexpectedEOD); return 0;}
}

FD_EXPORT fd_8bytes _fd_dtsread_8bytes(fd_dtype_stream s)
{
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,8)) {
    fd_8bytes bytes=fd_get_8bytes(s->fd_bufptr);
    s->fd_bufptr=s->fd_bufptr+8;
    return bytes;}
  else {fd_whoops(fd_UnexpectedEOD); return 0;}
}

FD_EXPORT int _fd_dtsread_bytes
  (fd_dtype_stream s,unsigned char *bytes,int len)
{
  fd_dts_start_read(s);
  /* This is special because we don't use the intermediate buffer
     if the data isn't fully buffered. */
  if (fd_has_bytes(s,len)) {
    memcpy(bytes,s->fd_bufptr,len);
    s->fd_bufptr=s->fd_bufptr+len;
    return len;}
  else {
    int n_buffered=s->fd_buflim-s->fd_bufptr;
    int n_read=0, n_to_read=len-n_buffered;
    unsigned char *start=bytes+n_buffered;
    memcpy(bytes,s->fd_bufptr,n_buffered);
    s->fd_buflim=s->fd_bufptr=s->fd_bufstart;
    while (n_read<n_to_read) {
      int delta=read(s->fd_fileno,start,n_to_read);
      if (delta<0) return -1;
      n_read=n_read+delta; start=start+delta;}
    s->fd_filepos=s->fd_filepos+n_buffered+n_read;
    return len;}
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
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->fd_bufptr);
    s->fd_bufptr=s->fd_bufptr+4;
    return (fd_off_t) bytes;}
  else return ((fd_off_t)(-1));
}

FD_EXPORT int fd_dtsread_ints(fd_dtype_stream s,int len,unsigned int *words)
{
  if (((s->fd_dts_flags)&FD_DTSTREAM_READING) == 0)
    if (fd_set_read(s,1)<0) return -1;
  /* This is special because we ignore the buffer if we can. */
  if ((s->fd_dts_flags)&FD_DTSTREAM_CANSEEK) {
    fd_off_t real_pos=fd_getpos(s); int bytes_read=0, bytes_needed=len*4;
    lseek(s->fd_fileno,real_pos,SEEK_SET);
    while (bytes_read<bytes_needed) {
      int delta=read(s->fd_fileno,words+bytes_read,bytes_needed-bytes_read);
      if (delta<0)
        if (errno==EAGAIN) errno=0;
        else return delta;
      else bytes_read=bytes_read+delta;}
#if (!(WORDS_BIGENDIAN))
    {int i=0; while (i < len) {
      words[i]=fd_host_order(words[i]); i++;}}
#endif
    s->fd_filepos=real_pos+len*4;
    /* Reset the buffer */
    s->fd_bufptr=s->fd_buflim=s->fd_bufstart;
    return len*4;}
  else if (fd_needs_bytes((fd_byte_input)s,len*4)) {
    int i=0; while (i<len) {
      int word=fd_dtsread_4bytes(s);
      words[i++]=word;}
    return len*4;}
  else return -1;
}

/* Write functions */

FD_EXPORT int _fd_dtswrite_byte(fd_dtype_stream s,int b)
{
  if ((s->fd_dts_flags)&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  if (s->fd_bufptr>=s->fd_buflim) fd_dtsflush(s);
  *(s->fd_bufptr++)=b;
  return 1;
}

FD_EXPORT int _fd_dtswrite_4bytes(fd_dtype_stream s,fd_4bytes w)
{
  if ((s->fd_dts_flags)&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  if (s->fd_bufptr+4>=s->fd_buflim) fd_dtsflush(s);
  *(s->fd_bufptr++)=w>>24;
  *(s->fd_bufptr++)=((w>>16)&0xFF);
  *(s->fd_bufptr++)=((w>>8)&0xFF);
  *(s->fd_bufptr++)=((w>>0)&0xFF);
  return 4;
}

FD_EXPORT int _fd_dtswrite_8bytes(fd_dtype_stream s,fd_8bytes w)
{
  if ((s->fd_dts_flags)&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  if (s->fd_bufptr+8>=s->fd_buflim) fd_dtsflush(s);
  *(s->fd_bufptr++)=((w>>56)&0xFF);
  *(s->fd_bufptr++)=((w>>48)&0xFF);
  *(s->fd_bufptr++)=((w>>40)&0xFF);
  *(s->fd_bufptr++)=((w>>32)&0xFF);
  *(s->fd_bufptr++)=((w>>24)&0xFF);
  *(s->fd_bufptr++)=((w>>16)&0xFF);
  *(s->fd_bufptr++)=((w>>8)&0xFF);
  *(s->fd_bufptr++)=((w>>0)&0xFF);
  return 8;
}

FD_EXPORT int _fd_dtswrite_zint(struct FD_DTYPE_STREAM *s,fd_4bytes val)
{
  return fd_dtswrite_zint(s,val);
}

FD_EXPORT int _fd_dtswrite_bytes
  (fd_dtype_stream s,const unsigned char *bytes,int n)
{
  if ((s->fd_dts_flags)&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  /* If there isn't space, flush the stream */
  if (s->fd_bufptr+n>=s->fd_buflim) fd_dtsflush(s);
  /* If there still isn't space (bufsiz too small),
     call writeall and advance the filepos to reflect the
     written bytes. */
  if (s->fd_bufptr+n>=s->fd_buflim) {
    int bytes_written=writeall(s->fd_fileno,bytes,n);
#if FD_DEBUG_DTYPE_IO
    u8_log(LOG_DEBUG,"DTSWRITE",
           "Wrote %d/%d bytes to %s#%d, %d/%d bytes in buffer",
           bytes_written,n,
           s->fd_dtsid,s->fd_fileno,s->fd_bufptr-s->fd_bufstart,s->fd_buflim-s->fd_bufstart);
#endif
    if (bytes_written<0) return bytes_written;
    s->fd_filepos=s->fd_filepos+bytes_written;}
  else {
    memcpy(s->fd_bufptr,bytes,n); s->fd_bufptr=s->fd_bufptr+n;}
  return n;
}

FD_EXPORT int fd_dtswrite_ints(fd_dtype_stream s,int len,unsigned int *words)
{
  if (((s->fd_dts_flags))&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  /* This is special because we ignore the buffer if we can. */
  if (((s->fd_dts_flags))&FD_DTSTREAM_CANSEEK) {
    fd_off_t real_pos; int bytes_written;
    fd_dtsflush(s);
    real_pos=fd_getpos(s);
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
static fdtype zread_dtype(struct FD_DTYPE_STREAM *s)
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
    return FD_ERROR_VALUE;}
  memset(&in,0,sizeof(in));
  in.fd_bufptr=in.fd_bufstart=do_uncompress(bytes,n_bytes,&dbytes);
  in.fd_dts_flags=FD_BYTEBUF_MALLOCD;
  if (in.fd_bufstart==NULL) {
    u8_free(bytes);
    return FD_ERROR_VALUE;}
  in.fd_buflim=in.fd_bufstart+dbytes; in.fd_dts_fillfn=NULL;
  result=fd_read_dtype(&in);
  u8_free(bytes); u8_free(in.fd_bufstart);
  return result;
}

FD_EXPORT fdtype fd_zread_dtype(struct FD_DTYPE_STREAM *s)
{
  return zread_dtype(s);
}

/* This reads a non frame value with compression. */
static int zwrite_dtype(struct FD_DTYPE_STREAM *s,fdtype x)
{
  unsigned char *zbytes; ssize_t zlen=-1, size;
  struct FD_BYTE_OUTPUT out; memset(&out,0,sizeof(out));
  out.fd_bufptr=out.fd_bufstart=u8_malloc(2048);
  out.fd_buflim=out.fd_bufstart+2048;
  out.fd_dts_flags=FD_BYTEBUF_MALLOCD;
  if (fd_write_dtype(&out,x)<0) {
    u8_free(out.fd_bufstart);
    return FD_ERROR_VALUE;}
  zbytes=do_compress(out.fd_bufstart,out.fd_bufptr-out.fd_bufstart,&zlen);
  if (zlen<0) {
    u8_free(out.fd_bufstart);
    return FD_ERROR_VALUE;}
  fd_dtswrite_byte(s,dt_ztype);
  size=fd_dtswrite_zint(s,zlen); size=size+zlen;
  if (fd_dtswrite_bytes(s,zbytes,zlen)<0) size=-1;
  fd_dtsflush(s);
  u8_free(zbytes);
  u8_free(out.fd_bufstart);
  return size;
}

FD_EXPORT int fd_zwrite_dtype(struct FD_DTYPE_STREAM *s,fdtype x)
{
  return zwrite_dtype(s,x);
}

static int zwrite_dtypes(struct FD_DTYPE_STREAM *s,fdtype x)
{
  unsigned char *zbytes=NULL; ssize_t zlen=-1, size; int retval=0;
  struct FD_BYTE_OUTPUT out; memset(&out,0,sizeof(out));
  out.fd_bufptr=out.fd_bufstart=u8_malloc(2048);
  out.fd_buflim=out.fd_bufstart+2048;
  out.fd_dts_flags=FD_BYTEBUF_MALLOCD;
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
    zbytes=do_compress(out.fd_bufstart,out.fd_bufptr-out.fd_bufstart,&zlen);
  if ((retval<0)||(zlen<0)) {
    if (zbytes) u8_free(zbytes); u8_free(out.fd_bufstart);
    return -1;}
  fd_dtswrite_byte(s,dt_ztype);
  size=1+fd_dtswrite_zint(s,zlen); size=size+zlen;
  retval=fd_dtswrite_bytes(s,zbytes,zlen);
  u8_free(zbytes); u8_free(out.fd_bufstart);
  if (retval<0) return retval;
  else return size;
}

FD_EXPORT int fd_zwrite_dtypes(struct FD_DTYPE_STREAM *s,fdtype x)
{
  return zwrite_dtypes(s,x);
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
    if (opened) {
      fdtype result=FD_VOID;
      int byte1=fd_dtsread_byte(opened);
      int zip=(byte1>=0x80);
      if (opened->fd_bufptr > opened->fd_bufstart) 
        opened->fd_bufptr--;
      else fd_setpos(opened,0);
      if (zip)
        result=zread_dtype(opened);
      else result=fd_dtsread_dtype(opened);
      fd_dtsclose(opened,1);
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
  if (opened) {
    size_t len=(zip)?
      (zwrite_dtype(opened,object)):
      (fd_dtswrite_dtype(opened,object));
    fd_dtsclose(opened,1);
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
