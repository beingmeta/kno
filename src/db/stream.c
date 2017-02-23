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
#include "framerd/stream.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>
#include <libu8/u8printf.h>

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
fd_exception fd_WriteOnlyStream=_("Write-only stream");
fd_exception fd_CantRead=_("Can't read data");
fd_exception fd_CantWrite=_("Can't write data");
fd_exception fd_BadSeek=_("Bad seek argument");
fd_exception fd_CantSeek=_("Can't seek on stream");
fd_exception fd_BadLSEEK=_("lseek() failed");
fd_exception fd_OverSeek=_("Seeking past end of file");
fd_exception fd_UnderSeek=_("Seeking before the beginning of the file");

#define FD_DEBUG_DTYPEIO 0

FD_EXPORT ssize_t fd_fill_stream(fd_stream df,size_t n);
static ssize_t stream_fillfn(fd_inbuf buf,size_t n,void *vdata)
{
  return fd_fill_stream((struct FD_STREAM *)vdata,n);
}

static U8_MAYBE_UNUSED u8_byte _dbg_outbuf[FD_DEBUG_OUTBUF_SIZE];

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

/* Unwrappers */

FD_EXPORT fd_inbuf _fd_readbuf(fd_stream s)
{
  if (FD_STREAM_ISWRITING(s))
    fd_set_direction(s,fd_byteflow_read);
  return &(s->buf.in);
}

FD_EXPORT fd_outbuf _fd_writebuf(fd_stream s)
{
  if (!(FD_STREAM_ISWRITING(s)))
    fd_set_direction(s,fd_byteflow_write);
  return &(s->buf.out);
}

FD_EXPORT fd_inbuf _fd_start_read(fd_stream s,fd_off_t pos)
{
  if (FD_STREAM_ISWRITING(s))
    fd_set_direction(s,fd_byteflow_read);
  if (pos<0) fd_endpos(s);
  else fd_setpos(s,pos);
  return &(s->buf.in);
}

FD_EXPORT fd_outbuf _fd_start_write(fd_stream s,fd_off_t pos)
{
  if (!(FD_STREAM_ISWRITING(s)))
    fd_set_direction(s,fd_byteflow_write);
  if (pos<0) fd_endpos(s);
  else fd_setpos(s,pos);
  return &(s->buf.out);
}

/* Initialization functions */

FD_EXPORT struct FD_STREAM *fd_init_stream(fd_stream stream,int fileno,int bufsiz)
{
  if (fileno<0) return NULL;
  else {
    unsigned char *buf=u8_malloc(bufsiz);
    struct FD_RAWBUF *bufptr=&(stream->buf.raw);
    /* If you can't get a whole buffer, try smaller */
    while ((bufsiz>=1024) && (buf==NULL)) {
      bufsiz=bufsiz/2; buf=u8_malloc(bufsiz);}
    FD_SET_CONS_TYPE(stream,fd_stream_type);
    /* Initializing the stream fields */
    stream->stream_fileno=fileno;
    stream->streamid=NULL;
    stream->stream_filepos=-1;
    stream->stream_maxpos=-1;
    u8_init_mutex(&(stream->stream_lock));
    if (buf==NULL) bufsiz=0;
    /* Initialize the buffer fields */
    FD_INIT_BYTE_INPUT((struct FD_INBUF *)bufptr,buf,bufsiz);
    bufptr->buflim=bufptr->bufpoint; bufptr->buflen=bufsiz;
    bufptr->buf_fillfn=stream_fillfn;
    bufptr->buf_flushfn=NULL;
    bufptr->buf_flags|=FD_BUFFER_IS_MALLOCD|FD_IN_STREAM;
    bufptr->buf_data=(void *)stream;
    return stream;}
}

FD_EXPORT fd_stream fd_init_file_stream
   (fd_stream stream,u8_string fname,fd_stream_mode mode,int bufsiz)
{
  int fd, flags=POSIX_OPEN_FLAGS, lock=0, writing=0;
  char *localname=u8_localpath(fname);
  switch (mode) {
  case FD_STREAM_READ:
    flags=flags|O_RDONLY; break;
  case FD_STREAM_MODIFY:
    flags=flags|O_RDWR; lock=1; writing=1; break;
  case FD_STREAM_WRITE:
    flags=flags|O_RDWR|O_CREAT; lock=1; writing=1; break;
  case FD_STREAM_CREATE:
    flags=flags|O_CREAT|O_TRUNC|O_RDWR; lock=1; writing=1; break;
  }
  fd=open(localname,flags,0666);
  stream->stream_fileno=fd;
  stream->stream_flags=FD_STREAM_OWNS_FILENO;
  /* If we fail and we're modifying, try to open read-only */
  if ((fd<0) && (mode == FD_STREAM_MODIFY)) {
    fd=open(localname,O_RDONLY,0666);
    if (fd>0) writing=0;}
  if (fd>0) {
    fd_init_stream(stream,fd,bufsiz);
    stream->streamid=u8_strdup(fname);
    stream->stream_flags|=FD_STREAM_CAN_SEEK;
    if (lock) stream->stream_flags|=FD_STREAM_NEEDS_LOCK;
    if (writing == 0)
      stream->stream_flags|=FD_STREAM_READ_ONLY;
    stream->stream_maxpos=lseek(fd,0,SEEK_END);
    stream->stream_filepos=lseek(fd,0,SEEK_SET);
    u8_init_mutex(&(stream->stream_lock));
    u8_free(localname);
    return stream;}
  else {
    fd_seterr3(fd_CantOpenFile,"fd_init_file_stream",localname);
    return NULL;}
}

FD_EXPORT fd_stream fd_open_filestream
   (u8_string fname,fd_stream_mode mode,int bufsiz)
{
  struct FD_STREAM *stream=u8_alloc(struct FD_STREAM);
  struct FD_STREAM *opened=
    fd_init_file_stream(stream,fname,mode,bufsiz);
  if (opened) {
    opened->stream_flags|=FD_STREAM_IS_MALLOCD;
    return opened;}
  else {
    u8_free(stream);
    return NULL;}
}

FD_EXPORT void fd_close_stream(fd_stream s,int flags)
{
  int dofree   =   (U8_BITP(flags,FD_STREAM_FREE));
  int close_fd = ! (U8_BITP(flags,FD_STREAM_NOCLOSE));
  int flush    = !  (U8_BITP(flags,FD_STREAM_NOFLUSH));

  /* Already closed */
  if (s->stream_fileno<0) return;

  /* Lock before closing */
  lock_stream(s);

  /* Flush data */
  if (flush) {
    fd_flush_stream(s);
    fsync(s->stream_fileno);}

  if (close_fd) {
    if (s->stream_flags&FD_STREAM_SOCKET)
      shutdown(s->stream_fileno,SHUT_RDWR);
    close(s->stream_fileno);}
  s->stream_fileno=-1;

  if (dofree) {
    struct FD_RAWBUF *buf=&(s->buf.raw);
    if (s->streamid) {
      u8_free(s->streamid);
      s->streamid=NULL;}
    if (buf->bytebuf) {
      u8_free(buf->bytebuf);
      buf->bytebuf=buf->bufpoint=buf->buflim=NULL;}
    unlock_stream(s);
    u8_destroy_mutex(&(s->stream_lock));
    if (s->stream_flags&(FD_STREAM_IS_MALLOCD)) u8_free(s);}
  else fd_unlock_stream(s);
}

FD_EXPORT void fd_free_stream(fd_stream s,int flags)
{
  fd_close_stream(s,flags|FD_STREAM_FREE);
}

FD_EXPORT void fd_stream_setbuf(fd_stream s,int bufsiz)
{
  fd_lock_stream(s);
  fd_flush_stream(s);
  {
    struct FD_RAWBUF *buf=&(s->buf.raw);
    unsigned int ptroff=buf->bufpoint-buf->bytebuf;
    unsigned int endoff=buf->buflim-buf->bytebuf;
    buf->bytebuf=u8_realloc(buf->bytebuf,bufsiz);
    buf->bufpoint=buf->bytebuf+ptroff; buf->buflim=buf->bytebuf+endoff;
    buf->buflen=bufsiz;
  }
  fd_unlock_stream(s);
}

/* CONS handlers */

static int unparse_stream(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_STREAM *stream=(struct FD_STREAM *)x;
  if (stream->streamid) {
    u8_printf(out,"#<Stream (%s) #!%llx>",stream->streamid,x);}
  else u8_printf(out,"#<Stream #!%llx>",x);
  return 1;
}

static void recycle_stream(struct FD_CONS *c)
{
  struct FD_STREAM *stream=(struct FD_STREAM *)c;
  fd_close_stream(stream,(stream->stream_flags&FD_STREAM_OWNS_FILENO));
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Structure functions */

FD_EXPORT
ssize_t fd_fill_stream(fd_stream stream,size_t n)
{
  struct FD_RAWBUF *buf=&(stream->buf.raw);
  int n_buffered, n_to_read, read_request, bytes_read=0;
  int fileno=stream->stream_fileno;

  bytes_read=(buf->bufpoint-buf->bytebuf);
  n_buffered=(buf->buflim-buf->bufpoint);
  memmove(buf->bytebuf,buf->bufpoint,n_buffered);
  buf->buflim=(buf->bytebuf)+n_buffered;
  buf->bufpoint=buf->bytebuf;
  bytes_read=0;

  /* Make sure that there's enough space */
  if (n>buf->buflen) {
    int new_size=buf->buflen;
    unsigned char *newbuf;
    size_t end_pos=(buf->bytebuf) ? (buf->buflim-buf->bytebuf) : (0);
    size_t ptr_pos=(buf->bytebuf) ? (buf->bufpoint-buf->bytebuf) : (0);
    if (new_size<=0) new_size=256;
    while (new_size<n)
      if (new_size>=0x40000) new_size=new_size+0x40000;
      else new_size=new_size*2;
    newbuf=u8_realloc(buf->bytebuf,new_size);
    buf->bytebuf=newbuf;
    buf->bufpoint=newbuf+ptr_pos;
    buf->buflim=newbuf+end_pos;
    buf->buflen=new_size;}
  n_to_read=n-n_buffered;
  read_request=buf->buflen-n_buffered;
  while (bytes_read < n_to_read) {
    size_t this_request=read_request-bytes_read;
    unsigned char *this_buf=buf->buflim;
    ssize_t delta=read(fileno,this_buf,this_request);
    if (delta==0) break;
    if ((delta<0) && (errno) && (errno != EWOULDBLOCK)) {
      fd_seterr3(u8_strerror(errno),"fill_stream",
                 u8s(stream->streamid));
      return 0;}
    else if (delta<0) delta=0;
    buf->buflim=buf->buflim+delta;
    if (stream->stream_filepos>=0)
      stream->stream_filepos=stream->stream_filepos+delta;
    bytes_read=bytes_read+delta;}

  return bytes_read;
}

FD_EXPORT
int fd_flush_stream(fd_stream stream)
{
  struct FD_RAWBUF *buf=&(stream->buf.raw);
  if (FD_STREAM_ISREADING(stream)) {
    if (buf->buflim==buf->bufpoint) return 0;
    else {
      buf->bufpoint=buf->buflim=buf->bytebuf;
      return 0;}}
  else if (buf->bufpoint>buf->bytebuf) {
    int fileno=stream->stream_fileno;
    size_t n_buffered=buf->bufpoint-buf->bytebuf;
    unsigned char *bytebuf=buf->bytebuf;
    int bytes_written=writeall(fileno,buf->bytebuf,n_buffered);
    if (bytes_written<0) {
      return -1;}
    if ((stream->stream_flags)&FD_STREAM_DOSYNC) fsync(fileno);
    if ( (stream->stream_flags&FD_STREAM_CAN_SEEK) && (fileno>=0) )
      stream->stream_filepos=stream->stream_filepos+bytes_written;
    if ((stream->stream_maxpos>=0) &&
        (stream->stream_filepos>stream->stream_maxpos))
      stream->stream_maxpos=stream->stream_filepos;
    /* Reset the buffer pointers */
    buf->bufpoint=buf->bytebuf;
    return bytes_written;}
  else {
    return 0;}
}

/* Locking and unlocking */

FD_EXPORT void fd_lock_stream(fd_stream s)
{
  lock_stream(s);
}

FD_EXPORT void fd_unlock_stream(fd_stream s)
{
  unlock_stream(s);
}

FD_EXPORT int fd_lockfile(fd_stream s)
{
  if (s->stream_flags&FD_STREAM_FILE_LOCKED)
    return 1;
  else if ((u8_lock_fd(s->stream_fileno,1))>=0) {
    s->stream_flags=s->stream_flags|FD_STREAM_FILE_LOCKED;
    return 1;}
  return 0;
}

FD_EXPORT int fd_unlockfile(fd_stream s)
{
  if (!(s->stream_flags&FD_STREAM_FILE_LOCKED))
    return 1;
  else if ((u8_unlock_fd(s->stream_fileno))>=0) {
    s->stream_flags=s->stream_flags|FD_STREAM_FILE_LOCKED;
    return 1;}
  return 0;
}

FD_EXPORT int fd_set_direction(fd_stream s,fd_byteflow direction)
{
  struct FD_RAWBUF *buf=&(s->buf.raw);
  if (direction == fd_byteflow_write) {
    if (FD_STREAM_ISWRITING(s))
      return 0;
    else if ((s->stream_flags)&FD_STREAM_READ_ONLY) {
      fd_seterr(fd_ReadOnlyStream,"fd_set_direction",
                u8s(s->streamid),FD_VOID);
      return -1;}
    else {
      if ((s->stream_flags)&FD_STREAM_NEEDS_LOCK) {
        if (u8_lock_fd(s->stream_fileno,1)) {
          (s->stream_flags)=(s->stream_flags)|FD_STREAM_FILE_LOCKED;}
        else return -1;}
      buf->bufpoint=buf->bytebuf;
      buf->buflim=buf->bytebuf+buf->buflen;
      /* Now we clear the bit */
      buf->buf_flags|=FD_IS_WRITING;
      return 1;}}
  else if (direction == fd_byteflow_read) {
    if (!((buf->buf_flags)&(FD_IS_WRITING)))
      return 0;
    else  if ((s->stream_flags)&FD_STREAM_WRITE_ONLY) {
      fd_seterr(fd_WriteOnlyStream,"fd_set_direction",
                u8s(s->streamid),FD_VOID);
      return -1;}
    else {
      /* If we were writing, in order to start reading, we need
         to flush what is buffered to the output and collapse all
         of the pointers into the start. We also need to update bufsiz
         in case the output buffer grew while we were writing. */
      if (fd_flush_stream(s)<0) {
        return -1;}
      /* Now we reset bufsiz in case we grew the buffer */
      buf->buflen=buf->buflim-buf->bytebuf;
      /* Finally, we reset the pointers */
      buf->buflim=buf->bufpoint=buf->bytebuf;
      buf->buf_flags&=~FD_IS_WRITING;
      return 1;}}
  else return 0;
}

/* This gets the position when it isn't cached on the stream. */
FD_EXPORT fd_off_t _fd_getpos(fd_stream s)
{
  struct FD_RAWBUF *buf=&(s->buf.raw);
  fd_off_t current, pos;
  if (((s->stream_flags)&FD_STREAM_CAN_SEEK) == 0) {
    return fd_reterr(fd_CantSeek,"fd_getpos",u8s(s->streamid),FD_VOID);}
  if (!((buf->buf_flags)&FD_IS_WRITING)) {
    current=lseek(s->stream_fileno,0,SEEK_CUR);
    /* If we are reading, we subtract the amount buffered from the
       actual filepos */
    s->stream_filepos=current;
    pos=current-(buf->buflim-buf->bytebuf);}
  else {
    current=lseek(s->stream_fileno,0,SEEK_CUR);
    s->stream_filepos=current;
    /* If we are writing, we add the amount buffered for output to the
       actual filepos */
    pos=current+(buf->bufpoint-buf->bytebuf);}
  return pos;
}
FD_EXPORT fd_off_t _fd_setpos(fd_stream s,fd_off_t pos)
{
  struct FD_RAWBUF *buf=&(s->buf.raw);
  /* This is optimized for the case where the new position is
     in the range we have buffered. */
  if (((s->stream_flags)&FD_STREAM_CAN_SEEK) == 0) {
    return fd_reterr(fd_CantSeek,"fd_setpos",u8s(s->streamid),FD_INT(pos));}
  else if (pos<0) {
    return fd_reterr(fd_BadSeek,"fd_setpos",u8s(s->streamid),FD_INT(pos));}
  else {}

  /* First, if we're reading, see if the designated position is already
     in the buffer. */
  if ( (s->stream_filepos>=0) && (FD_STREAM_ISREADING(s)) ) {
    fd_off_t delta=(pos-s->stream_filepos);
    unsigned char *relptr=buf->buflim+delta;
    if ( (relptr >= buf->bytebuf) && (relptr < buf->buflim )) {
      buf->bufpoint=relptr;
      return pos;}}
  /* We could check here that we're not jumping back to something in
     the buffer, but we're not optimizing for that, expecting it to be
     relatively rare. So at this point, we're commited to moving the
     filepos (after flushing buffered output of course). */
  if (fd_flush_stream(s)<0) {
    return -1;}
  fd_off_t newpos=lseek(s->stream_fileno,pos,SEEK_SET);
  if (newpos>=0) {
    s->stream_filepos=newpos;
    return newpos;}
  else if (errno==EINVAL) {
    fd_off_t maxpos=lseek(s->stream_fileno,(fd_off_t)0,SEEK_END);
    s->stream_maxpos=s->stream_filepos=maxpos;
    return fd_reterr(fd_OverSeek,"fd_setpos",u8s(s->streamid),FD_INT(pos));}
  else {
    u8_graberrno("fd_setpos",u8s(s->streamid));
    return -1;}
}
FD_EXPORT fd_off_t _fd_endpos(fd_stream s)
{
  fd_off_t rv;
  if (((s->stream_flags)&FD_STREAM_CAN_SEEK) == 0)
    return fd_reterr(fd_CantSeek,"fd_endpos",u8s(s->streamid),FD_VOID);
  fd_flush_stream(s);
  rv=s->stream_maxpos=s->stream_filepos=(lseek(s->stream_fileno,0,SEEK_END));
  return rv;
}

FD_EXPORT fd_off_t fd_movepos(fd_stream s,fd_off_t delta)
{
  fd_off_t cur, rv;
  if (((s->stream_flags)&FD_STREAM_CAN_SEEK) == 0)
    return fd_reterr(fd_CantSeek,"fd_movepos",u8s(s->streamid),FD_INT(delta));
  cur=fd_getpos(s);
  rv=fd_setpos(s,cur+delta);
  return rv;
}

FD_EXPORT int fd_write_4bytes_at(fd_stream s,fd_4bytes w,fd_off_t off)
{
  fd_outbuf out= (off>=0) ? (fd_start_write(s,off)) : (fd_writebuf(s)) ;
  *(out->bufwrite++)=w>>24;
  *(out->bufwrite++)=((w>>16)&0xFF);
  *(out->bufwrite++)=((w>>8)&0xFF);
  *(out->bufwrite++)=((w>>0)&0xFF);
  fd_flush_stream(s);
  return 4;
}

FD_EXPORT long long fd_read_4bytes_at(fd_stream s,fd_off_t off)
{
  struct FD_INBUF *in=(off>=0) ? (fd_start_read(s,off)) : (fd_readbuf(s));
  if (fd_needs_bytes(in,4)) {
    fd_8bytes bytes=fd_get_4bytes(in->bufread);
    in->bufread=in->bufread+4;
    return bytes;}
  else return -1;
}

FD_EXPORT int fd_write8bytes_at(fd_stream s,fd_8bytes w,fd_off_t off)
{
  fd_outbuf out= (off>=0) ? (fd_start_write(s,off)) : (fd_writebuf(s)) ;
  *(out->bufwrite++)=((w>>56)&0xFF);
  *(out->bufwrite++)=((w>>48)&0xFF);
  *(out->bufwrite++)=((w>>40)&0xFF);
  *(out->bufwrite++)=((w>>32)&0xFF);
  *(out->bufwrite++)=((w>>24)&0xFF);
  *(out->bufwrite++)=((w>>16)&0xFF);
  *(out->bufwrite++)=((w>>8)&0xFF);
  *(out->bufwrite++)=((w>>0)&0xFF);
  fd_flush_stream(s);
  return 4;
}

FD_EXPORT fd_8bytes fd_read_8bytes_at(fd_stream s,fd_off_t off,int *err)
{
  struct FD_INBUF *in=(off>=0) ? (fd_start_read(s,off)) : (fd_readbuf(s));
  if (fd_needs_bytes(in,8)) {
    fd_8bytes bytes=fd_get_8bytes(in->bufread);
    in->bufread=in->bufread+8;
    return bytes;}
  else {
    if (err) *err=-1;
    return 0;}
}

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
FD_EXPORT fdtype fd_zread_dtype(struct FD_INBUF *in)
{
  fdtype result;
  ssize_t n_bytes=fd_read_zint(in), dbytes;
  unsigned char *bytes=u8_malloc(n_bytes);
  int retval=fd_read_bytes(bytes,in,n_bytes);
  struct FD_INBUF tmp;
  if (retval<n_bytes) {
    u8_free(bytes);
    return FD_ERROR_VALUE;}
  memset(&tmp,0,sizeof(tmp));
  tmp.bufread=tmp.bytebuf=do_uncompress(bytes,n_bytes,&dbytes);
  tmp.buf_flags=FD_BUFFER_IS_MALLOCD;
  tmp.buflim=tmp.bytebuf+dbytes;
  result=fd_read_dtype(&tmp);
  u8_free(bytes); u8_free(tmp.bytebuf);
  return result;
}

/* This reads a non frame value with compression. */
FD_EXPORT int fd_zwrite_dtype(struct FD_OUTBUF *s,fdtype x)
{
  unsigned char *zbytes; ssize_t zlen=-1, size;
  struct FD_OUTBUF out;
  memset(&out,0,sizeof(out));
  out.bufwrite=out.bytebuf=u8_malloc(2048);
  out.buflim=out.bytebuf+2048;
  out.buf_flags=FD_BUFFER_IS_MALLOCD;
  if (fd_write_dtype(&out,x)<0) {
    u8_free(out.bytebuf);
    return FD_ERROR_VALUE;}
  zbytes=do_compress(out.bytebuf,out.bufwrite-out.bytebuf,&zlen);
  if (zlen<0) {
    u8_free(out.bytebuf);
    return FD_ERROR_VALUE;}
  fd_write_byte(s,dt_ztype);
  size=fd_write_zint(s,zlen); size=size+zlen;
  if (fd_write_bytes(s,zbytes,zlen)<0) size=-1;
  u8_free(zbytes); u8_free(out.bytebuf);
  return size;
}

FD_EXPORT int fd_zwrite_dtypes(struct FD_OUTBUF *s,fdtype x)
{
  unsigned char *zbytes=NULL; ssize_t zlen=-1, size; int retval=0;
  struct FD_OUTBUF out; memset(&out,0,sizeof(out));
  out.bufwrite=out.bytebuf=u8_malloc(2048);
  out.buflim=out.bytebuf+2048;
  out.buf_flags=FD_BUFFER_IS_MALLOCD;
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
    zbytes=do_compress(out.bytebuf,out.bufwrite-out.bytebuf,&zlen);
  if ((retval<0)||(zlen<0)) {
    if (zbytes) u8_free(zbytes); u8_free(out.bytebuf);
    return -1;}
  fd_write_byte(s,dt_ztype);
  size=1+fd_write_zint(s,zlen); size=size+zlen;
  retval=fd_write_bytes(s,zbytes,zlen);
  u8_free(zbytes); u8_free(out.bytebuf);
  if (retval<0) return retval;
  else return size;
}

FD_EXPORT int fd_write_ints(fd_stream s,int len,unsigned int *words)
{
  fd_set_direction(s,fd_byteflow_write);
  /* We handle the case where we can write directly to the file */
  if (((s->stream_flags))&FD_STREAM_CAN_SEEK) {
    int bytes_written;
    fd_off_t real_pos=fd_getpos(s);
    fd_set_direction(s,fd_byteflow_write);
    fd_setpos(s,real_pos);
#if (!(WORDS_BIGENDIAN))
    {int i=0; while (i < len) {
        words[i]=fd_net_order(words[i]); i++;}}
#endif
    bytes_written=writeall(s->stream_fileno,(unsigned char *)words,len*4);
#if (!(WORDS_BIGENDIAN))
    {int i=0; while (i < len) {
        words[i]=fd_host_order(words[i]); i++;}}
#endif
    fd_setpos(s,real_pos+4*len);
    return bytes_written;}
  else {
    fd_outbuf out=fd_writebuf(s);
    int i=0; while (i<len) {
      int word=words[i++];
      fd_write_4bytes(out,word);}
    return len*4;}
}

FD_EXPORT int fd_read_ints(fd_stream s,int len,unsigned int *words)
{
  /* This is special because we ignore the buffer if we can. */
  if ((s->stream_flags)&FD_STREAM_CAN_SEEK) {
    /* real_pos is the file position plus any data buffered for output
       (or minus any data buffered for input) */
    fd_off_t real_pos=fd_getpos(s);
    int bytes_read=0, bytes_needed=len*4;
    /* This will flush any pending write data */
    fd_set_direction(s,fd_byteflow_read);
    fd_setpos(s,real_pos);
    while (bytes_read<bytes_needed) {
      int delta=read(s->stream_fileno,words+bytes_read,bytes_needed-bytes_read);
      if (delta<0)
        if (errno==EAGAIN) errno=0;
        else return delta;
      else {
        s->stream_filepos=s->stream_filepos+delta;
        bytes_read+=delta;}}
#if (!(WORDS_BIGENDIAN))
    {int i=0; while (i < len) {
        words[i]=fd_host_order(words[i]); i++;}}
#endif
    return bytes_read;}
  else if (fd_needs_bytes(fd_readbuf(s),len*4)) {
    struct FD_INBUF *in=fd_readbuf(s);
    int i=0; while (i<len) {
      int word=fd_read_4bytes(in);
      words[i++]=word;}
    return len*4;}
  else return -1;
}


/* Files 2 dtypes */

FD_EXPORT fdtype fd_read_dtype_from_file(u8_string filename)
{
  struct FD_STREAM *stream=u8_alloc(struct FD_STREAM);
  ssize_t filesize=u8_file_size(filename);
  ssize_t bufsize=(filesize+1024);
  if (filesize<0) {
    fd_seterr(fd_FileNotFound,"fd_file2dtype",u8_strdup(filename),FD_VOID);
    return FD_ERROR_VALUE;}
  else if (filesize==0) {
    fd_seterr("Zero-length file","fd_file2dtype",u8_strdup(filename),FD_VOID);
    return FD_ERROR_VALUE;}
  else {
    struct FD_STREAM *opened=
      fd_init_file_stream(stream,filename,FD_STREAM_READ,bufsize);
    if (opened) {
      fdtype result=FD_VOID;
      struct FD_INBUF *in=fd_readbuf(opened);
      int byte1=fd_probe_byte(in);
      int zip=(byte1>=0x80);
      if (zip)
        result=fd_zread_dtype(in);
      else result=fd_read_dtype(in);
      fd_free_stream(opened,1);
      return result;}
    else {
      u8_free(stream);
      return FD_ERROR_VALUE;}}
}

FD_EXPORT ssize_t fd_dtype2file(fdtype object, u8_string filename,
                                size_t bufsize,int zip)
{
  struct FD_STREAM *stream=u8_alloc(struct FD_STREAM);
  struct FD_STREAM *opened=
    fd_init_file_stream(stream,filename,FD_STREAM_WRITE,bufsize);
  if (opened) {
    size_t len=(zip)?
      (fd_zwrite_dtype(fd_writebuf(opened),object)):
      (fd_write_dtype(fd_writebuf(opened),object));
    fd_free_stream(opened,1);
    return len;}
  else return -1;
}

FD_EXPORT ssize_t fd_write_dtype_to_file(fdtype object,u8_string filename)
{
  return fd_dtype2file(object,filename,1024*64,0);
}

FD_EXPORT ssize_t fd_write_zdtype_to_file(fdtype object,u8_string filename)
{
  return fd_dtype2file(object,filename,1024*64,1);
}

/* Initialization of file */

FD_EXPORT void fd_init_stream_c()
{
  fd_register_config
    ("CHECKDTSIZE",_("whether to check returned and real dtype sizes"),
     fd_boolconfig_get,fd_boolconfig_set,&fd_check_dtsize);

  fd_unparsers[fd_stream_type]=unparse_stream;
  fd_recyclers[fd_stream_type]=recycle_stream;

  u8_register_source_file(_FILEINFO);
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
