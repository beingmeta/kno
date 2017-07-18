/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1
#define FD_INLINE_STREAMIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/streams.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>
#include <libu8/u8printf.h>
#include <libu8/libu8io.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <zlib.h>

#if HAVE_STRUCT_STAT_ST_MTIM
#define stat_mtime st_mtim
#elif HAVE_STRUCT_STAT_ST_MTIMESPEC
#define stat_mtime st_mtimespec
#else
#define stat_mtime st_mtime
#endif

#if HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#if HAVE_MMAP
#include <sys/mman.h>
#endif

#if ((FD_LARGEFILES_ENABLED) && (defined(O_LARGEFILE)))
#define POSIX_OPEN_FLAGS O_LARGEFILE
#else
#define POSIX_OPEN_FLAGS 0
#endif

size_t fd_stream_bufsize = FD_STREAM_BUFSIZE;
size_t fd_filestream_bufsize = FD_FILESTREAM_BUFSIZE;

fd_exception fd_ReadOnlyStream=_("Read-only stream");
fd_exception fd_WriteOnlyStream=_("Write-only stream");
fd_exception fd_CantRead=_("Can't read data");
fd_exception fd_CantWrite=_("Can't write data");
fd_exception fd_BadSeek=_("Bad seek argument");
fd_exception fd_CantSeek=_("Can't seek on stream");
fd_exception fd_BadLSEEK=_("lseek() failed");
fd_exception fd_OverSeek=_("Seeking past end of file");
fd_exception fd_UnderSeek=_("Seeking before the beginning of the file");

fd_exception fd_CantMMAP=_("Can't MMAP stream buffer");

#define FD_DEBUG_DTYPEIO 0

FD_EXPORT ssize_t fd_fill_stream(fd_stream df,size_t n);

static ssize_t stream_fillfn(fd_inbuf buf,size_t n,void *vdata)
{
  return fd_fill_stream((struct FD_STREAM *)vdata,n);
}
static ssize_t stream_flushfn(fd_outbuf buf,void *vdata)
{
  return fd_flush_stream((struct FD_STREAM *)vdata);
}

static ssize_t mmap_read_update(struct FD_STREAM *stream)
{
  struct FD_RAWBUF *buf = &(stream->buf.raw);
  int fd = stream->stream_fileno;
  int mallocd = ( (buf->buf_flags) & (FD_BUFFER_IS_MALLOCD) );
  struct stat fileinfo;
  if (fstat(fd,&fileinfo)<0) {
    u8_graberrno("mmap_read_update:fstat",u8_strdup(stream->streamid));
    return -1;}
  else if ( (mallocd==0) &&
            ((stream->mmap_time.tv_sec==fileinfo.stat_mtime.tv_sec) &&
             (stream->mmap_time.tv_nsec==fileinfo.stat_mtime.tv_nsec)) )
    /* Not changed */
    return 0;
  else {
    u8_write_lock(&(stream->mmap_lock));
    if (fstat(fd,&fileinfo)<0) {
      u8_rw_unlock(&(stream->mmap_lock));
      u8_graberrno("mmap_read_update:fstat",u8_strdup(stream->streamid));
      return -1;}
    if ((stream->mmap_time.tv_sec==fileinfo.stat_mtime.tv_sec) &&
        (stream->mmap_time.tv_nsec==fileinfo.stat_mtime.tv_nsec)) {
      u8_rw_unlock(&(stream->mmap_lock));
      return 0;}
    else stream->mmap_time=fileinfo.stat_mtime;
    size_t old_size = buf->buflen, new_size = fileinfo.st_size;
    int prot = (U8_BITP(stream->stream_flags,FD_STREAM_READ_ONLY)) ?
      (PROT_READ) : (PROT_READ|PROT_WRITE) ;
    int flags = (U8_BITP(stream->stream_flags,FD_STREAM_READ_ONLY)) ?
      (MAP_NORESERVE|MAP_SHARED) :
      (MAP_SHARED);
    unsigned char *oldbuf = buf->buffer;
    ssize_t point_off = (oldbuf) ? (buf->bufpoint-buf->buffer) : (0);
    unsigned char *newbuf = mmap(NULL,new_size,prot,flags,fd,0);
    if ((newbuf==NULL)||(newbuf==MAP_FAILED)) {
      u8_log(LOGWARN,u8_strerror(errno),
             "mmap_read_update:mmap of %s",stream->streamid);
      u8_graberrno("mmap_read_update:mmap",u8_strdup(stream->streamid));
      u8_rw_unlock(&(stream->mmap_lock));
      return -1;}
    buf->buflen = new_size;
    buf->buffer = newbuf;
    buf->bufpoint = newbuf+point_off;
    buf->buflim = newbuf+fileinfo.st_size;
    buf->buf_data = stream;
    stream->stream_filepos=stream->stream_maxpos=fileinfo.st_size;
    u8_rw_unlock(&(stream->mmap_lock));
    if (oldbuf) {
      if (mallocd) {
        u8_free(oldbuf);
        buf->buf_flags &= ~FD_BUFFER_IS_MALLOCD;}
      else if (oldbuf==newbuf) {}
      else if (munmap(oldbuf,old_size)<0) {
        u8_log(LOGCRIT,"FailedUnmap",
               "Couldn't (%s:%d) unmap old buffer for %s",
               u8_strerror(errno),errno,
               stream->streamid);
        errno=0;}}
    return new_size;}
}

static ssize_t mmap_write_update(struct FD_STREAM *stream,ssize_t grow)
{
  struct FD_RAWBUF *buf = &(stream->buf.raw);
  u8_write_lock(&(stream->mmap_lock));
  if ( (grow == 0) && (buf->buffer) ) {
    if ( (stream->stream_maxpos) < (buf->bufpoint-buf->buffer) )
      stream->stream_maxpos=buf->bufpoint-buf->buffer;
    if (msync(buf->buffer,buf->buflen,MS_SYNC) < 0) {
      u8_graberrno("mmap_write_update:msync",u8_strdup(stream->streamid));
      u8_rw_unlock(&(stream->mmap_lock));
      return -1;}
    return buf->buflen;}
  int fd = stream->stream_fileno;
  ssize_t point_off=0;
  struct stat fileinfo;
  if (fstat(fd,&fileinfo)<0) {
    u8_graberrno("mmap_write_update:fstat",u8_strdup(stream->streamid));
    u8_rw_unlock(&(stream->mmap_lock));
    return -1;}
  else stream->mmap_time=fileinfo.stat_mtime;
  ssize_t file_size=fileinfo.st_size;
  size_t maxpos=(stream->stream_maxpos>0) ? (stream->stream_maxpos) :
    (file_size);
  if ((buf->buffer) && (maxpos<(buf->bufpoint-buf->buffer)))
    stream->stream_maxpos = maxpos = buf->bufpoint-buf->buffer;
  size_t min_size = maxpos;
  size_t new_size = ((grow>0) ? (min_size+grow) : (min_size));
  if (buf->buffer) {
    if (msync(buf->buffer,buf->buflen,MS_SYNC|MS_INVALIDATE) < 0) {
      u8_graberrno("mmap_write_update:msync",u8_strdup(stream->streamid));
      u8_rw_unlock(&(stream->mmap_lock));
      return -1;}
    point_off = buf->bufpoint-buf->buffer;
    stream->stream_maxpos=buf->bufpoint-buf->buffer;
    if ( (grow >= 0) && (new_size == buf->buflen) )
      return buf->buflen;
    if (munmap(buf->buffer,buf->buflen) < 0) {
      u8_graberrno("mmap_write_update:munmap",u8_strdup(stream->streamid));
      u8_rw_unlock(&(stream->mmap_lock));
      return -1;}
    else {
      buf->buffer = NULL; buf->bufpoint = NULL;
      buf->buflim = NULL; buf->buflen = 0;}}

  if (grow<0) {
    if (stream->stream_maxpos>0) {
      int rv=ftruncate(fd,stream->stream_maxpos);
      if (rv<0) {
        u8_graberrno("mmap_write_update:ftruncate:-1",
                     u8_strdup(stream->streamid));
        u8_rw_unlock(&(stream->mmap_lock));
        return -1;}}
    return 0;}
  else if (new_size<=0) new_size=fd_stream_bufsize;
  if (file_size!=new_size) {
    if (ftruncate(fd,new_size)<0) {
      u8_graberrno("mmap_write_update:ftruncate",
                   u8_strdup(stream->streamid));
      u8_rw_unlock(&(stream->mmap_lock));
      return -1;}}
  unsigned char *newbuf=mmap
    (NULL,new_size,(PROT_READ|PROT_WRITE),MAP_SHARED,fd,0);
  if (fstat(fd,&fileinfo)<0) {
    u8_graberrno("mmap_write_update:fstat",u8_strdup(stream->streamid));
    u8_rw_unlock(&(stream->mmap_lock));
    return -1;}
  else stream->mmap_time=fileinfo.stat_mtime;
  u8_rw_unlock(&(stream->mmap_lock));
  if (newbuf==NULL) {
    u8_graberrno("mmap_write_update:mmap",u8_strdup(stream->streamid));
    return -1;}
  buf->buflen = new_size;
  buf->buffer = newbuf;
  buf->bufpoint = newbuf+point_off;
  buf->buflim = newbuf+new_size;
  stream->stream_maxpos=fileinfo.st_size;
  return new_size;
}

static void release_mmap(struct FD_STREAM *stream)
{
  if ((stream->stream_flags)&(FD_STREAM_MMAPPED)) {
    struct FD_RAWBUF *rawbuf=&(stream->buf.raw);
    if ((rawbuf->buffer)&&(rawbuf->buflen>=0)) {
      int rv=munmap(rawbuf->buffer,rawbuf->buflen);
      if (rv<0) {
        u8_log(LOGWARN,_("MUNMAP failed"),
               "Failed to unmap %s (errno=%d) %s",
               stream->streamid,errno,u8_strerror(errno));
        U8_CLEAR_ERRNO();}
      rawbuf->buffer=rawbuf->bufpoint=rawbuf->buflim=NULL;
      rawbuf->buflen=-1;}}
  else u8_log(LOGWARN,_("Not MMapped"),
              "Attempting to munmap a non-mapped stream %s",
              stream->streamid);
}

static ssize_t mmap_fillfn(fd_inbuf buf,size_t n,void *vdata)
{
  return mmap_read_update((fd_stream)vdata);
}
static ssize_t mmap_flushfn(fd_outbuf buf,void *vdata)
{
  return mmap_write_update((fd_stream)vdata,fd_filestream_bufsize);
}

static U8_MAYBE_UNUSED u8_byte _dbg_outbuf[FD_DEBUG_OUTBUF_SIZE];

/* Utility functions */

static int writeall(int fd,const unsigned char *data,int n)
{
  int written = 0;
  while (written<n) {
    int delta = write(fd,data+written,n-written);
    if (delta<0)
      if (errno == EAGAIN) errno = 0;
      else {
        u8_log(LOG_ERROR,
               "write failed","writeall %d errno=%d (%s) written=%d/%d",
                fd,errno,strerror(errno),written,n);
        return delta;}
    else written = written+delta;}
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

FD_EXPORT struct FD_STREAM *fd_init_stream(fd_stream stream,
                                           u8_string streamid,
                                           int fileno,
                                           int flags,
                                           ssize_t bufsiz)
{
  struct FD_RAWBUF *streambuf = &stream->buf.raw;
  if (bufsiz<0) bufsiz = fd_stream_bufsize;
  if (fileno<0) return NULL;
  if (flags&FD_STREAM_IS_CONSED) {
    FD_INIT_FRESH_CONS(stream,fd_stream_type);}
  else {FD_INIT_STATIC_CONS(stream,fd_stream_type);}
  /* Initializing the stream fields */
  stream->stream_fileno = fileno;
  stream->streamid = u8dup(streamid);
  stream->stream_filepos = -1;
  stream->stream_maxpos = -1;
  stream->stream_flags = flags;
#if HAVE_MMAP
  u8_init_rwlock(&(stream->mmap_lock));
  if (U8_BITP(flags,FD_STREAM_MMAPPED)) {
    if (U8_BITP(flags,FD_STREAM_READ_ONLY)) {
      ssize_t rv= (U8_BITP(flags,FD_STREAM_READ_ONLY)) ?
        (mmap_read_update(stream)) :
        (mmap_write_update(stream,bufsiz));
      if (rv>=0) {
        u8_init_mutex(&(stream->stream_lock));
        streambuf->buf_fillfn  = mmap_fillfn;
        streambuf->buf_flushfn = mmap_flushfn;
        streambuf->buf_data    = (void *) stream;
        streambuf->buf_flags   = FD_IN_STREAM|FD_BUFFER_NO_GROW;
        streambuf->buf_data = (void *)stream;
        streambuf->buflen = rv;
        return stream;}
      else {
        u8_log(LOG_INFO,"FailedStreamMMAP",
               "Unable to mmap '%s'; errno=%d (%s)",
               streamid,errno,u8_strerror(errno));
        stream->stream_flags&=~FD_STREAM_MMAPPED;
        errno = 0;}}
    else stream->stream_flags&=~FD_STREAM_MMAPPED;}
#endif
  unsigned char *buf = u8_malloc(bufsiz);
  struct FD_RAWBUF *bufptr = &(stream->buf.raw);
  /* If you can't get a whole buffer, try smaller */
  while ((bufsiz>=1024) && (buf == NULL)) {
    u8_log(LOGWARN,"BigBuffer",
           "Can't allocate %lld-byte buffer for %s, trying %lld",
           bufsiz,(U8ALT(streamid,"somestream")),bufsiz/2);
    bufsiz = bufsiz/2; buf = u8_malloc(bufsiz);}
  u8_init_mutex(&(stream->stream_lock));
  if (buf == NULL) bufsiz = 0;
  /* Initialize the buffer fields */
  FD_INIT_BYTE_INPUT((struct FD_INBUF *)bufptr,buf,bufsiz);
  bufptr->buflim = bufptr->bufpoint; bufptr->buflen = bufsiz;
  bufptr->buf_fillfn = stream_fillfn;
  bufptr->buf_flushfn = stream_flushfn;
  bufptr->buf_flags|=FD_BUFFER_IS_MALLOCD|FD_IN_STREAM;
  bufptr->buf_data = (void *)stream;
  return stream;
}

FD_EXPORT fd_stream fd_init_file_stream
   (fd_stream stream,u8_string fname,
    fd_stream_mode mode,
    fd_stream_flags stream_flags,
    ssize_t bufsiz)
{
  if (bufsiz<0) bufsiz = fd_filestream_bufsize;
  if (stream_flags<0)
    stream_flags = FD_DEFAULT_FILESTREAM_FLAGS;
  stream_flags |= FD_STREAM_OWNS_FILENO;
  int fd, open_flags = POSIX_OPEN_FLAGS;
  char *localname = u8_localpath(fname);
  if (mode<0) {
    if ( (stream_flags) & (FD_STREAM_READ_ONLY) )
      mode=FD_FILE_READ;
    else if (!(u8_file_existsp(fname)))
      mode=FD_FILE_CREATE;
    else if (!(u8_file_writablep(fname)))
      mode=FD_FILE_MODIFY;
    else mode=FD_FILE_READ;}
  switch (mode) {
  case FD_FILE_READ:
    open_flags |= O_RDONLY;
    stream_flags |= FD_STREAM_READ_ONLY;
    break;
  case FD_FILE_MODIFY:
    open_flags |= O_RDWR;
    stream_flags |= FD_STREAM_NEEDS_LOCK;
    break;
  case FD_FILE_WRITE:
    open_flags |= O_RDWR|O_CREAT;
    stream_flags |= FD_STREAM_NEEDS_LOCK;
    break;
  case FD_FILE_CREATE:
    open_flags |= O_CREAT|O_TRUNC|O_RDWR;
    stream_flags |= FD_STREAM_NEEDS_LOCK;
    break;}
  fd = open(localname,open_flags,0666);
  /* If we fail and we're modifying, try to open read-only */
  if ((fd<0) && (mode == FD_FILE_MODIFY)) {
    U8_CLEAR_ERRNO();
    fd = open(localname,O_RDONLY,0666);}
  if (fd>0) {
    U8_CLEAR_ERRNO();
    fd_init_stream(stream,fname,fd,stream_flags,bufsiz);
    if (stream->stream_maxpos<=0) {
      stream->stream_maxpos = lseek(fd,0,SEEK_END);
      stream->stream_filepos = lseek(fd,0,SEEK_SET);}
    u8_init_mutex(&(stream->stream_lock));
    u8_free(localname);
    return stream;}
  else {
    fd_seterr3(u8_CantOpenFile,"fd_init_file_stream",localname);
    return NULL;}
}

FD_EXPORT fd_stream fd_open_file(u8_string fname,fd_stream_mode mode)
{
  struct FD_STREAM *stream = u8_alloc(struct FD_STREAM);
  struct FD_STREAM *opened;
  fd_stream_flags flags = FD_STREAM_IS_CONSED|FD_DEFAULT_FILESTREAM_FLAGS;
  FD_INIT_CONS(stream,fd_stream_type);
  if ((opened = fd_init_file_stream(stream,fname,mode,flags,-1))) {
    return opened;}
  else {
    u8_free(stream);
    return NULL;}
}

FD_EXPORT fd_stream fd_reopen_file_stream
(fd_stream stream,
 fd_stream_mode mode,
 ssize_t bufsiz)
{
  fd_stream_flags stream_flags=stream->stream_flags;
  if (bufsiz<0) bufsiz = fd_filestream_bufsize;
  stream_flags |= FD_STREAM_OWNS_FILENO;
  int fd, open_flags = POSIX_OPEN_FLAGS;
  int needs_lock=0, read_only=0;
  char *localname = u8_localpath(stream->streamid);
  if (mode<0) {
    if ( (stream_flags) & (FD_STREAM_READ_ONLY) )
      mode=FD_FILE_READ;
    else mode=FD_FILE_MODIFY;}
  switch (mode) {
  case FD_FILE_READ:
    open_flags |= O_RDONLY;
    read_only=1;
    break;
  case FD_FILE_MODIFY:
    open_flags |= O_RDWR;
    needs_lock=1;
    stream_flags |= FD_STREAM_NEEDS_LOCK;
    break;
  case FD_FILE_WRITE:
    open_flags |= O_RDWR|O_CREAT;
    needs_lock=1;
    break;
  case FD_FILE_CREATE:
    open_flags |= O_CREAT|O_TRUNC|O_RDWR;
    needs_lock=1;
    break;}

  if (read_only)
    stream_flags |= FD_STREAM_READ_ONLY;
  else stream_flags &= (~(FD_STREAM_READ_ONLY));
  if (needs_lock)
    stream_flags |= FD_STREAM_NEEDS_LOCK;
  else stream_flags &= (~(FD_STREAM_NEEDS_LOCK));

  fd = open(localname,open_flags,0666);

  /* If we fail and we're modifying, try to open read-only */

  if (fd>0) {
    U8_CLEAR_ERRNO();
    struct stat info;
    int rv=fstat(fd,&info);
    stream->stream_fileno=fd;
    stream->stream_flags=stream_flags;
    if (rv<0) {
      u8_log(LOGWARN,"Reopen/StatFailed","Couldn't call stat on fileno %s(fd=%d)",
             stream->streamid,fd);
      stream->stream_maxpos = lseek(fd,0,SEEK_END);
      stream->stream_filepos = lseek(fd,0,SEEK_SET);}
    else {
      int at_end = (stream->stream_maxpos==stream->stream_filepos);
      stream->stream_maxpos = info.st_size;
      if (stream->stream_filepos>0) {
        if ((stream->stream_filepos) > (stream->stream_maxpos)) {
          u8_log(LOGWARN,"Reopen/FileTruncated",
                 "The file %s(fd=%d) is now smaller (%lld bytes) "
                 "than the previous offset (%lld bytes), positioning %s",
                 stream->streamid,fd,
                 stream->stream_maxpos,stream->stream_filepos,
                 ((at_end) ? ("at_end") : ("at beginning")));
          if (at_end)
            stream->stream_filepos = lseek(fd,0,SEEK_END);
          else stream->stream_filepos = lseek(fd,0,SEEK_SET);}
        else stream->stream_filepos =
               lseek(fd,stream->stream_filepos,SEEK_SET);}
      else stream->stream_filepos = lseek(fd,0,SEEK_SET);}
    u8_free(localname);
    return stream;}
  else {
    fd_seterr3(u8_CantOpenFile,"fd_init_file_stream",stream->streamid);
    u8_free(localname);
    return NULL;}
}

FD_EXPORT void fd_close_stream(fd_stream s,int flags)
{
  int dofree    = (U8_BITP(flags,FD_STREAM_FREEDATA));
  int flush     = !(U8_BITP(flags,FD_STREAM_NOFLUSH));

  /* Already closed */
  if (s->stream_fileno<0) {
    struct FD_RAWBUF *buf = &(s->buf.raw);
    if (!(buf->buffer)) return;}

  /* Lock before closing */
  fd_lock_stream(s);

  /* Flush data */
  if (!(flush)) {}
#if HAVE_MMAP
  else if ((U8_BITP(s->stream_flags,FD_STREAM_MMAPPED))&&
           (!((U8_BITP(s->stream_flags,FD_STREAM_READ_ONLY))))) {
    ssize_t rv = mmap_write_update(s,-1);
    if (rv<0) fd_clear_errors(1);}
#endif
  else if ((U8_BITP(s->buf.raw.buf_flags,FD_IS_WRITING))&&
           (s->buf.out.bufwrite>s->buf.out.buffer)) {
    if (s->stream_fileno<0) {
      u8_log(LOGCRIT,_("StreamClosed"),
             "Stream %s (0x%llx) was closed with %d bytes still buffered",
             U8ALT(s->streamid,"somestream"),
             (unsigned long long)s,
             (s->buf.out.bufwrite-s->buf.out.buffer));}
    else {
      fd_flush_stream(s);
      fsync(s->stream_fileno);}}
  else {}

  if (s->stream_fileno>=0) {
    if ((s->stream_flags&FD_STREAM_OWNS_FILENO)&&
        (!(flags&FD_STREAM_NOCLOSE))) {
      if (s->stream_flags&FD_STREAM_SOCKET)
        shutdown(s->stream_fileno,SHUT_RDWR);
      close(s->stream_fileno);}
    s->stream_fileno = -1;}

  if (dofree) {
    struct FD_RAWBUF *buf = &(s->buf.raw);
    if (s->streamid) {
      u8_free(s->streamid);
      s->streamid = NULL;}
    if (buf->buffer) {
      if (s->stream_flags&FD_STREAM_MMAPPED)
        release_mmap(s);
      else u8_free(buf->buffer);
      buf->buffer = buf->bufpoint = buf->buflim = NULL;}
    fd_unlock_stream(s);
    u8_destroy_mutex(&(s->stream_lock));}
  else fd_unlock_stream(s);
}

FD_EXPORT void fd_free_stream(fd_stream s)
{
  struct FD_CONS *cons = (struct FD_CONS *)s;
  lispval sptr = (lispval)s;
  if (FD_STATIC_CONSP(cons)) {
    u8_log(LOGWARN,_("FreeingStaticStream"),
           "Attempting to free the static stream %s 0x%llx",
           U8ALT(s->streamid,"somestream"),
           (unsigned long long)s);}
  else if (FD_CONS_REFCOUNT(cons)>1) {
    u8_log(LOGWARN,_("DanglingStreamPointers"),
           "Freeing a stream with dangling pointers %s 0x%llx",
           U8ALT(s->streamid,"somestream"),
           (unsigned long long)s);
    fd_decref(sptr);}
  else fd_decref(sptr);
}

FD_EXPORT ssize_t fd_setbufsize(fd_stream s,ssize_t bufsize)
{
  int unlock=fd_using_stream(s);
  fd_flush_stream(s);
  struct FD_RAWBUF *buf = &(s->buf.raw);
  int flags=s->buf.raw.buf_flags;
  if (bufsize<0) {
#if HAVE_MMAP
    if (U8_BITP(s->stream_flags,FD_STREAM_MMAPPED))
      return 0;
    else if (!(U8_BITP(s->stream_flags,FD_STREAM_READ_ONLY))) {
      u8_log(LOG_INFO,fd_CantMMAP,"Stream '%s' not read-only",s->streamid);
      if (unlock) fd_unlock_stream(s);
      return -1;}
    else {
      ssize_t new_size=mmap_read_update(s);
      if (unlock) fd_unlock_stream(s);
      return new_size;}
#else
    return 0;
#endif
  }
  if (bufsize) {
    unsigned char *oldbuf=buf->buffer, *newbuf;
    size_t point_off=buf->bufpoint-oldbuf, lim_off=buf->buflim-oldbuf;
    if (U8_BITP(s->stream_flags,FD_STREAM_MMAPPED)) {
      int rv=munmap(buf->buffer,buf->buflen);
      if (rv<0) {
        u8_log(LOGWARN,"MunmapFailed",
               "Could unmap buffer for stream '%s'",s->streamid);
        U8_CLEAR_ERRNO();}
      newbuf = u8_malloc(bufsize);}
    else if (flags&FD_BUFFER_IS_MALLOCD)
      newbuf = u8_realloc(buf->buffer,bufsize);
    else newbuf = u8_malloc(bufsize);
    if (newbuf == NULL) {
      u8_seterr("MallocFailed","fd_setbufsize",u8_strdup(s->streamid));
      if (unlock) fd_unlock_stream(s);
      return -1;}
    buf->buffer=newbuf;
    if (point_off < bufsize)
      buf->bufpoint = newbuf+point_off;
    else buf->bufpoint=buf->buffer;
    if (lim_off < bufsize)
      buf->buflim = newbuf+lim_off;
    else buf->buflim=buf->buffer+bufsize;
    buf->buflen = bufsize;
    s->buf.raw.buf_flags |= FD_BUFFER_IS_MALLOCD;
    if (unlock) fd_unlock_stream(s);
    return bufsize;}
  else {
    if (U8_BITP(s->stream_flags,FD_STREAM_MMAPPED)) {
      int rv=munmap(buf->buffer,buf->buflen);
      if (rv<0) {
        u8_log(LOGWARN,"MunmapFailed",
               "Could unmap buffer for stream '%s'",s->streamid);
        U8_CLEAR_ERRNO();}}
    if (flags&FD_BUFFER_IS_MALLOCD) u8_free(buf->buffer);
    buf->buffer=NULL;
    buf->bufpoint=NULL;
    buf->buflim=NULL;
    buf->buflen=-1;
    s->buf.raw.buf_flags &= ~FD_BUFFER_IS_MALLOCD;
    if (unlock) fd_unlock_stream(s);
    return 0;}
}

/* CONS handlers */

static int unparse_stream(struct U8_OUTPUT *out,lispval x)
{
  struct FD_STREAM *stream = (struct FD_STREAM *)x;
  if (stream->streamid) {
    u8_printf(out,"#<Stream (%s) #!%llx>",stream->streamid,x);}
  else u8_printf(out,"#<Stream #!%llx>",x);
  return 1;
}

static void recycle_stream(struct FD_RAW_CONS *c)
{
  struct FD_STREAM *stream = (struct FD_STREAM *)c;
  fd_close_stream(stream,FD_STREAM_FREEDATA);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Structure functions */

FD_EXPORT
ssize_t fd_fill_stream(fd_stream stream,size_t n)
{
  struct FD_RAWBUF *buf = &(stream->buf.raw);
  int n_buffered, n_to_read, read_request, bytes_read = 0;
  int fileno = stream->stream_fileno;

  bytes_read = (buf->bufpoint-buf->buffer);
  n_buffered = (buf->buflim-buf->bufpoint);
  memmove(buf->buffer,buf->bufpoint,n_buffered);
  buf->buflim = (buf->buffer)+n_buffered;
  buf->bufpoint = buf->buffer;
  bytes_read = 0;

  /* Make sure that there's enough space */
  if (n>buf->buflen) {
    int new_size = buf->buflen;
    unsigned char *newbuf;
    size_t end_pos = (buf->buffer) ? (buf->buflim-buf->buffer) : (0);
    size_t ptr_pos = (buf->buffer) ? (buf->bufpoint-buf->buffer) : (0);
    if (new_size<=0) new_size = 256;
    while (new_size<n)
      if (new_size>=0x40000) new_size = new_size+0x40000;
      else new_size = new_size*2;
    newbuf = u8_realloc(buf->buffer,new_size);
    buf->buffer = newbuf;
    buf->bufpoint = newbuf+ptr_pos;
    buf->buflim = newbuf+end_pos;
    buf->buflen = new_size;}
  n_to_read = n-n_buffered;
  read_request = buf->buflen-n_buffered;
  while (bytes_read < n_to_read) {
    size_t this_request = read_request-bytes_read;
    unsigned char *this_buf = buf->buflim;
    ssize_t delta = read(fileno,this_buf,this_request);
    if (delta == 0) break;
    if ((delta<0) && (errno) && (errno != EWOULDBLOCK)) {
      fd_seterr3(u8_strerror(errno),"fill_stream",stream->streamid);
      return 0;}
    else if (delta<0) delta = 0;
    buf->buflim = buf->buflim+delta;
    if (stream->stream_filepos>=0)
      stream->stream_filepos = stream->stream_filepos+delta;
    bytes_read = bytes_read+delta;}

  if (bytes_read<n)
    return -1;
  else return bytes_read;
}

FD_EXPORT
ssize_t fd_flush_stream(fd_stream stream)
{
  if (U8_BITP(stream->stream_flags,FD_STREAM_MMAPPED))
    return mmap_write_update(stream,0);
  else {
    struct FD_RAWBUF *buf = &(stream->buf.raw);
    if (FD_STREAM_ISREADING(stream)) {
      if (buf->buflim == buf->bufpoint) return 0;
      else {
        buf->bufpoint = buf->buflim = buf->buffer;
        return 0;}}
    else if (buf->bufpoint>buf->buffer) {
      int fileno = stream->stream_fileno;
      size_t n_buffered = buf->bufpoint-buf->buffer;
      int bytes_written = writeall(fileno,buf->buffer,n_buffered);
      if (bytes_written<0) {
        return -1;}
      if ((stream->stream_flags)&FD_STREAM_DOSYNC) fsync(fileno);
      if ( (stream->stream_flags&FD_STREAM_CAN_SEEK) && (fileno>=0) )
        stream->stream_filepos = stream->stream_filepos+bytes_written;
      if ((stream->stream_maxpos>=0) &&
          (stream->stream_filepos>stream->stream_maxpos))
        stream->stream_maxpos = stream->stream_filepos;
      /* Reset the buffer pointers */
      buf->bufpoint = buf->buffer;
      return bytes_written;}
    else {
      return 0;}}
}

/* Locking and unlocking */

FD_EXPORT int _fd_lock_stream(fd_stream s)
{
  return fd_lock_stream(s);
}

FD_EXPORT int _fd_unlock_stream(fd_stream s)
{
  return fd_unlock_stream(s);
}

FD_EXPORT int _fd_using_stream(fd_stream s)
{
  return fd_using_stream(s);
}

FD_EXPORT int fd_lockfile(fd_stream s)
{
  if (s->stream_flags&FD_STREAM_FILE_LOCKED)
    return 1;
  else if ((u8_lock_fd(s->stream_fileno,1))>=0) {
    s->stream_flags = s->stream_flags|FD_STREAM_FILE_LOCKED;
    return 1;}
  return 0;
}

FD_EXPORT int fd_unlockfile(fd_stream s)
{
  if (!(s->stream_flags&FD_STREAM_FILE_LOCKED))
    return 1;
  else if ((u8_unlock_fd(s->stream_fileno))>=0) {
    s->stream_flags = s->stream_flags|FD_STREAM_FILE_LOCKED;
    return 1;}
  return 0;
}

FD_EXPORT int fd_set_direction(fd_stream s,fd_byteflow direction)
{
  struct FD_RAWBUF *buf = &(s->buf.raw);
  if (direction == fd_byteflow_write) {
    if (FD_STREAM_ISWRITING(s))
      return 0;
    else if ((s->stream_flags)&FD_STREAM_READ_ONLY) {
      fd_seterr(fd_ReadOnlyStream,"fd_set_direction",s->streamid,VOID);
      return -1;}
    else {
      if ((s->stream_flags)&FD_STREAM_NEEDS_LOCK) {
        if (u8_lock_fd(s->stream_fileno,1)) {
          (s->stream_flags) = (s->stream_flags)|FD_STREAM_FILE_LOCKED;}
        else return -1;}
      buf->bufpoint = buf->buffer;
      buf->buflim = buf->buffer+buf->buflen;
      /* Now we clear the bit */
      buf->buf_flags |= FD_IS_WRITING;
      return 1;}}
  else if (direction == fd_byteflow_read) {
    if (!((buf->buf_flags)&(FD_IS_WRITING)))
      return 0;
    else  if ((s->stream_flags)&FD_STREAM_WRITE_ONLY) {
      fd_seterr(fd_WriteOnlyStream,"fd_set_direction",s->streamid,VOID);
      return -1;}
    else {
      /* If we were writing, in order to start reading, we need
         to flush what is buffered to the output and collapse all
         of the pointers into the start. We also need to update bufsiz
         in case the output buffer grew while we were writing. */
      if (fd_flush_stream(s)<0) {
        return -1;}
      /* Now we reset bufsiz in case we grew the buffer */
      buf->buflen = buf->buflim-buf->buffer;
      /* Finally, we reset the pointers */
      buf->buflim = buf->bufpoint = buf->buffer;
      buf->buf_flags &= ~FD_IS_WRITING;
      return 1;}}
  else return 0;
}

/* This gets the position when it isn't cached on the stream. */
FD_EXPORT fd_off_t _fd_getpos(fd_stream s)
{
  struct FD_RAWBUF *buf = &(s->buf.raw);
  fd_off_t current, pos;
  if (((s->stream_flags)&FD_STREAM_CAN_SEEK) == 0) {
    return fd_reterr(fd_CantSeek,"fd_getpos",u8s(s->streamid),VOID);}
  if (!((buf->buf_flags)&FD_IS_WRITING)) {
    current = lseek(s->stream_fileno,0,SEEK_CUR);
    /* If we are reading, we subtract the amount buffered from the
       actual filepos */
    s->stream_filepos = current;
    pos = current-(buf->buflim-buf->buffer);}
  else {
    current = lseek(s->stream_fileno,0,SEEK_CUR);
    s->stream_filepos = current;
    /* If we are writing, we add the amount buffered for output to the
       actual filepos */
    pos = current+(buf->bufpoint-buf->buffer);}
  return pos;
}
FD_EXPORT fd_off_t _fd_setpos(fd_stream s,fd_off_t pos)
{
  struct FD_RAWBUF *buf = &(s->buf.raw);

  if ( (s->stream_flags) & (FD_STREAM_MMAPPED) ) {
    size_t curpos = s->buf.raw.bufpoint - s->buf.raw.buffer;
    size_t maxpos = s->stream_maxpos;
    if ( curpos > maxpos ) s->stream_maxpos = maxpos = curpos;
    if (pos <= maxpos) {
      s->buf.raw.bufpoint = s->buf.raw.buffer+pos;
      return pos;}
    else {
      s->buf.raw.bufpoint = s->buf.raw.buflim;
      return fd_reterr(fd_OverSeek,"fd_setpos",
                       u8s(s->streamid),FD_INT(pos));}}

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
    fd_off_t delta = (pos-s->stream_filepos);
    unsigned char *relptr = buf->buflim+delta;
    if ( (relptr >= buf->buffer) && (relptr < buf->buflim )) {
      buf->bufpoint = relptr;
      return pos;}}
  /* We could check here that we're not jumping back to something in
     the buffer, but we're not optimizing for that, expecting it to be
     relatively rare. So at this point, we're commited to moving the
     filepos (after flushing buffered output of course). */
  if (fd_flush_stream(s)<0) {
    return -1;}
  fd_off_t newpos = lseek(s->stream_fileno,pos,SEEK_SET);
  if (newpos>=0) {
    s->stream_filepos = newpos;
    return newpos;}
  else if (errno == EINVAL) {
    fd_off_t maxpos = lseek(s->stream_fileno,(fd_off_t)0,SEEK_END);
    s->stream_maxpos = s->stream_filepos = maxpos;
    return fd_reterr(fd_OverSeek,"fd_setpos",u8s(s->streamid),FD_INT(pos));}
  else {
    u8_graberrno("fd_setpos",u8s(s->streamid));
    return -1;}
}
FD_EXPORT fd_off_t _fd_endpos(fd_stream s)
{
  fd_off_t rv;
  if (((s->stream_flags)&FD_STREAM_CAN_SEEK) == 0)
    return fd_reterr(fd_CantSeek,"fd_endpos",u8s(s->streamid),VOID);
  fd_flush_stream(s);
  rv = s->stream_maxpos =
    s->stream_filepos = (lseek(s->stream_fileno,0,SEEK_END));
  return rv;
}

FD_EXPORT fd_off_t fd_movepos(fd_stream s,fd_off_t delta)
{
  fd_off_t cur, rv;
  if (((s->stream_flags)&FD_STREAM_CAN_SEEK) == 0)
    return fd_reterr(fd_CantSeek,"fd_movepos",u8s(s->streamid),FD_INT(delta));
  cur = fd_getpos(s);
  rv = fd_setpos(s,cur+delta);
  return rv;
}

FD_EXPORT ssize_t fd_read_block(fd_stream s,unsigned char *buf,
                                size_t count,fd_off_t offset,
                                int stream_locked)
{
#if HAVE_PREAD
  if ((!(stream_locked)) &&
      (FD_ISWRITING(&(s->buf.raw))) &&
      ((offset+count)>(s->stream_filepos))) {
    /* This is the case where the stream is being written to and we're
       reading from beyond the position it's writing at. In this case,
       we lock the stream to do our read, which will block until the
       writer is done. At that point, we'll set the direction on the
       stream which will flush any buffered output. */
    ssize_t result;
    fd_lock_stream(s);
    fd_set_direction(s,fd_byteflow_read);
    result = pread(s->stream_fileno,buf,count,offset);
    fd_unlock_stream(s);
    return result;}
  else return pread(s->stream_fileno,buf,count,offset);
#else
  fd_off_t result = -1;
  if (!(stream_locked)) fd_lock_stream(s);
  fd_setpos(s,offset);
  result = fd_read_bytes(fd_readbuf(s),buf,count);
  if (!(stream_locked)) fd_unlock_stream(s);
  return result;
#endif
}

static void unlock_stream_mmap(struct FD_INBUF *out,void *streamptr)
{
  fd_stream s = (fd_stream) streamptr;
  out->buffer=out->bufread=out->buflim=NULL;
  out->buflen=0;
  u8_rw_unlock(&(s->mmap_lock));
}

FD_EXPORT fd_inbuf fd_open_block(fd_stream s,fd_inbuf in,
                                 fd_off_t offset,size_t len,
                                 int stream_locked)
{
  if ((s->stream_flags) & (FD_STREAM_MMAPPED)) {
    if (s->buf.in.buffer != in->buffer) {
      u8_read_lock(&(s->mmap_lock));
      in->buffer=s->buf.in.buffer;
      in->buflen=s->buf.in.buflen;
      in->buf_data=s;
      in->buf_closefn=unlock_stream_mmap;}
    in->bufread=in->buffer+offset;
    in->buflim=in->buffer+offset+len;
    return in;}
#if HAVE_PREAD
  ssize_t result;
  unsigned char *buf;
  if ( ( in->buffer ) && (len < in->buflen) )
    buf=(unsigned char *)in->buffer;
  else {
    if (fd_grow_byte_input(in,len)<0)
      return NULL;
    buf=(unsigned char *)in->buffer;}
  in->buflim=buf; in->bufread=buf;
  if ((!(stream_locked)) &&
      (FD_ISWRITING(&(s->buf.raw))) &&
      ((offset+len)>(s->stream_filepos))) {
    /* This is the case where the stream is being written to and we're
       reading from beyond the position it's writing at. In this case,
       we lock the stream to do our read, which will block until the
       writer is done. At that point, we'll set the direction on the
       stream which will flush any buffered output. */
    fd_lock_stream(s);
    fd_set_direction(s,fd_byteflow_read);
    result = pread(s->stream_fileno,buf,len,offset);
    fd_unlock_stream(s);}
  else result=pread(s->stream_fileno,buf,len,offset);
  if (result>=0) {
    in->buflim=in->buffer+result;
    return in;}
  else return NULL;
#else
  fd_off_t result = -1;
  if (!(stream_locked)) fd_lock_stream(s);
  fd_setpos(s,offset);
  result = fd_read_bytes(fd_readbuf(s),buf,len);
  if (!(stream_locked)) fd_unlock_stream(s);
  return result;
#endif
}

FD_EXPORT int fd_write_4bytes_at(fd_stream s,fd_4bytes w,fd_off_t off)
{
  fd_outbuf out = (off>=0) ? (fd_start_write(s,off)) : (fd_writebuf(s)) ;
  *(out->bufwrite++) = w>>24;
  *(out->bufwrite++) = ((w>>16)&0xFF);
  *(out->bufwrite++) = ((w>>8)&0xFF);
  *(out->bufwrite++) = ((w>>0)&0xFF);
  fd_flush_stream(s);
  return 4;
}

FD_EXPORT long long fd_read_4bytes_at(fd_stream s,fd_off_t off,int locked)
{
#if HAVE_PREAD
  unsigned char bytes[4];
  int rv = pread(s->stream_fileno,bytes,4,off);
  if (rv==4) {
    struct FD_INBUF tmp;
    FD_INIT_INBUF(&tmp,bytes,4,0);
    return fd_read_4bytes(&tmp);}
#endif
  if (locked==0) fd_lock_stream(s);
  struct FD_INBUF *in = (off>=0) ? (fd_start_read(s,off)) : (fd_readbuf(s));
  if (fd_request_bytes(in,4)) {
    fd_8bytes bytes = fd_get_4bytes(in->bufread);
    in->bufread = in->bufread+4;
    if (locked==0) fd_unlock_stream(s);
    return bytes;}
  else {
    if (locked==0) fd_unlock_stream(s);
    return -1;}
}

FD_EXPORT int fd_write_8bytes_at(fd_stream s,fd_8bytes w,fd_off_t off)
{
  fd_outbuf out = (off>=0) ? (fd_start_write(s,off)) : (fd_writebuf(s)) ;
  *(out->bufwrite++) = ((w>>56)&0xFF);
  *(out->bufwrite++) = ((w>>48)&0xFF);
  *(out->bufwrite++) = ((w>>40)&0xFF);
  *(out->bufwrite++) = ((w>>32)&0xFF);
  *(out->bufwrite++) = ((w>>24)&0xFF);
  *(out->bufwrite++) = ((w>>16)&0xFF);
  *(out->bufwrite++) = ((w>>8)&0xFF);
  *(out->bufwrite++) = ((w>>0)&0xFF);
  fd_flush_stream(s);
  return 4;
}

FD_EXPORT fd_8bytes fd_read_8bytes_at(fd_stream s,fd_off_t off,int locked,int *err)
{
#if HAVE_PREAD
  unsigned char bytes[8];
  int rv = pread(s->stream_fileno,bytes,8,off);
  if (rv==8) {
    struct FD_INBUF tmp;
    FD_INIT_INBUF(&tmp,bytes,8,0);
    return fd_read_8bytes(&tmp);}
#endif
  if (locked==0) fd_lock_stream(s);
  struct FD_INBUF *in = (off>=0) ? (fd_start_read(s,off)) : (fd_readbuf(s));
  if (fd_request_bytes(in,8)) {
    fd_8bytes bytes = fd_get_8bytes(in->bufread);
    in->bufread = in->bufread+8;
    if (locked==0) fd_unlock_stream(s);
    return bytes;}
  else {
    if (err) *err = -1;
    if (locked==0) fd_unlock_stream(s);
    return 0;}
}

FD_EXPORT
ssize_t fd_write_ints(fd_stream s,size_t len,unsigned int *words)
{
  fd_set_direction(s,fd_byteflow_write);
  /* We handle the case where we can write directly to the file */
  if (((s->stream_flags))&FD_STREAM_CAN_SEEK) {
    int bytes_written;
    fd_off_t real_pos = fd_getpos(s);
    fd_set_direction(s,fd_byteflow_write);
    fd_setpos(s,real_pos);
#if (!(WORDS_BIGENDIAN))
    {int i = 0; while (i < len) {
        words[i]=fd_net_order(words[i]); i++;}}
#endif
    bytes_written = writeall(s->stream_fileno,(unsigned char *)words,len*4);
#if (!(WORDS_BIGENDIAN))
    {int i = 0; while (i < len) {
        words[i]=fd_host_order(words[i]); i++;}}
#endif
    fd_setpos(s,real_pos+4*len);
    return bytes_written;}
  else {
    fd_outbuf out = fd_writebuf(s);
    int i = 0; while (i<len) {
      int word = words[i++];
      fd_write_4bytes(out,word);}
    return len*4;}
}

FD_EXPORT
ssize_t fd_stream_write(fd_stream s,size_t len,unsigned char *bytes)
{
  fd_set_direction(s,fd_byteflow_write);
  /* We handle the case where we can write directly to the file */
  if (((s->stream_flags))&FD_STREAM_CAN_SEEK) {
    int bytes_written;
    fd_off_t real_pos = fd_getpos(s);
    fd_set_direction(s,fd_byteflow_write);
    fd_setpos(s,real_pos);
    bytes_written = writeall(s->stream_fileno,bytes,len);
    fd_setpos(s,real_pos+len);
    return bytes_written;}
  else {
    fd_outbuf out = fd_writebuf(s);
    return fd_write_bytes(out,bytes,len);}
}

FD_EXPORT
ssize_t fd_read_ints(fd_stream s,size_t len,unsigned int *words)
{
  /* This is special because we ignore the buffer if we can. */
  if ((s->stream_flags)&FD_STREAM_CAN_SEEK) {
    /* real_pos is the file position plus any data buffered for output
       (or minus any data buffered for input) */
    fd_off_t real_pos = fd_getpos(s);
    int bytes_read = 0, bytes_needed = len*4;
    /* This will flush any pending write data */
    fd_set_direction(s,fd_byteflow_read);
    fd_setpos(s,real_pos);
    while (bytes_read<bytes_needed) {
      int delta = read(s->stream_fileno,words+bytes_read,bytes_needed-bytes_read);
      if (delta<0)
        if (errno == EAGAIN) errno = 0;
        else return delta;
      else {
        s->stream_filepos = s->stream_filepos+delta;
        bytes_read+=delta;}}
#if (!(WORDS_BIGENDIAN))
    {int i = 0; while (i < len) {
        words[i]=fd_host_order(words[i]); i++;}}
#endif
    return bytes_read;}
  else if (fd_request_bytes(fd_readbuf(s),len*4)) {
    struct FD_INBUF *in = fd_readbuf(s);
    int i = 0; while (i<len) {
      int word = fd_read_4bytes(in);
      words[i++]=word;}
    return len*4;}
  else return -1;
}

FD_EXPORT
ssize_t fd_stream_read(fd_stream s,size_t len,unsigned char *bytes)
{
  /* This is special because we ignore the buffer if we can. */
  if ((s->stream_flags)&FD_STREAM_CAN_SEEK) {
    /* real_pos is the file position plus any data buffered for output
       (or minus any data buffered for input) */
    fd_off_t real_pos = fd_getpos(s);
    int bytes_read = 0, bytes_needed = len;
    /* This will flush any pending write data */
    fd_set_direction(s,fd_byteflow_read);
    fd_setpos(s,real_pos);
    while (bytes_read<bytes_needed) {
      int delta = read(s->stream_fileno,bytes+bytes_read,
                       bytes_needed-bytes_read);
      if (delta<0)
        if (errno == EAGAIN) errno = 0;
        else return delta;
      else {
        s->stream_filepos = s->stream_filepos+delta;
        bytes_read+=delta;}}
    return bytes_read;}
  else if (fd_request_bytes(fd_readbuf(s),len*4)) {
    struct FD_INBUF *in = fd_readbuf(s);
    return fd_read_bytes(bytes,in,len);}
  else return -1;
}

/* Reading chunk refs */

typedef long long int ll;
typedef unsigned long long ull;

FD_EXPORT FD_CHUNK_REF
fd_fetch_chunk_ref(struct FD_STREAM *stream,
                   fd_off_t base,
                   fd_offset_type offtype,
                   unsigned int offset,
                   int locked)
{
  FD_CHUNK_REF result={-1,-1};
  int chunk_size = fd_chunk_ref_size(offtype);
  fd_off_t ref_off = offset*chunk_size;
#if HAVE_PREAD
  struct FD_INBUF _in, *in = &_in;
  unsigned char buf[16];
  memset(&_in,0,sizeof(_in));
  FD_INIT_BYTE_INPUT(&_in,buf,chunk_size);
  if (fd_read_block(stream,buf,chunk_size,base+ref_off,1)!=chunk_size) {
    u8_log(LOGCRIT,"BlockReadFailed",
           "Reading %d-byte block from stream %s failed",
           chunk_size,stream->streamid);
    u8_seterr("Block read failed","fetch_chunk_ref",u8_strdup(stream->streamid));
    return result;}
  else switch (offtype) {
  case FD_B32:
    result.off = fd_read_4bytes(in);
    result.size = fd_read_4bytes(in);
    break;
  case FD_B40: {
    unsigned int word1, word2;
    word1 = fd_read_4bytes(in);
    word2 = fd_read_4bytes(in);
    result.off = ((((ll)((word2)&(0xFF000000)))<<8)|word1);
    result.size = (ll)((word2)&(0x00FFFFFF));}
    break;
  case FD_B64:
    result.off = fd_read_8bytes(in);
    result.size = fd_read_4bytes(in);
    break;
  default:
    u8_log(LOGCRIT,"InvalidOffsetType",
           "Invalid offset type 0x%x for data stream %s",
           offtype,stream->streamid);
    u8_seterr("Invalid Offset type","read_chunk_ref",NULL);
    result.off = -1;
    result.size = -1;} /* switch (p->kb_offtype) */
#else
  if (!(locked)) fd_lock_stream(stream);
  if ( (fd_setpos(stream,base+ref_off)) < 0 ) {
    result.off = (fd_off_t)-1; result.size = (size_t)-1;}
  else {
    fd_inbuf in = fd_readbuf(stream);
    switch (offtype) {
    case FD_B32:
      result.off = fd_read_4bytes(in);
      result.size = fd_read_4bytes(in);
      break;
    case FD_B40: {
      unsigned int word1, word2;
      word1 = fd_read_4bytes(in);
      word2 = fd_read_4bytes(in);
      result.off = ((((ll)((word2)&(0xFF000000)))<<8)|word1);
      result.size = (ll)((word2)&(0x00FFFFFF));
      break;}
    case FD_B64:
      result.off = fd_read_8bytes(in);
      result.size = fd_read_4bytes(in);
      break;
    default:
      u8_log(LOGCRIT,"InvalidOffsetType",
	     "Invalid offset type 0x%x for data stream %s",
	     offtype,stream->streamid);
      u8_seterr("Invalid Offset type","read_chunk_ref",NULL);
      result.off = -1;
      result.size = -1;} /* switch (p->kb_offtype) */
  }
  if (!(locked)) fd_unlock_stream(stream);
#endif
  return result;
}


/* Stream ctl */

FD_EXPORT
lispval fd_streamctl(fd_stream s,fd_streamop op,void *data)
{
  switch (op) {
  case fd_stream_close:
    fd_close_stream(s,0);
    return VOID;
  case fd_stream_lockfile: {
    int rv = fd_lockfile(s);
    if (rv<0) return FD_ERROR;
    else if (rv) return FD_TRUE;
    else return FD_FALSE;}
  case fd_stream_unlockfile: {
    int rv = fd_unlockfile(s);
    if (rv<0) return FD_ERROR;
    else if (rv) return FD_TRUE;
    else return FD_FALSE;}
  case fd_stream_setread: {
    int rv = fd_set_direction(s,fd_byteflow_read);
    if (rv<0) return FD_ERROR;
    else if (rv) return FD_TRUE;
    else return FD_FALSE;}
  case fd_stream_setwrite: {
    int rv = fd_set_direction(s,fd_byteflow_write);
    if (rv<0) return FD_ERROR;
    else if (rv) return FD_TRUE;
    else return FD_FALSE;}
  case fd_stream_setbuf:
    fd_setbufsize(s,(ssize_t)data);
    return VOID;
  case fd_stream_mmap: {
    unsigned long long enable = (unsigned long long) data;
    // Only read-only streams are mmapped for now
    if (!(U8_BITP(s->stream_flags,FD_STREAM_READ_ONLY) )) enable=0;
    if ( ( (enable) && (U8_BITP(s->stream_flags,FD_STREAM_MMAPPED)) ) ||
         ( (!enable) && (!(U8_BITP(s->stream_flags,FD_STREAM_MMAPPED))) ) )
      return FD_FALSE;
    else if (enable) {
      ssize_t rv=fd_setbufsize(s,-1);
      if (rv<0) return FD_ERROR;
      else return FD_INT(rv);}
    else {
      ssize_t rv=fd_setbufsize(s,fd_stream_bufsize);
      if (rv<0) return FD_ERROR;
      else return FD_INT(rv);}}
  default:
    fd_seterr("Unhandled Operation","fd_streamctl",s->streamid,VOID);
    return FD_ERROR;}
}

/* Files 2 dtypes */

FD_EXPORT lispval fd_read_dtype_from_file(u8_string filename)
{
  struct FD_STREAM *stream = u8_alloc(struct FD_STREAM);
  ssize_t filesize = u8_file_size(filename);
  if (filesize<0) {
    fd_seterr(fd_FileNotFound,"fd_read_dtype_from_file",filename,VOID);
    return FD_ERROR;}
  else if (filesize == 0) {
    fd_seterr("Zero-length file","fd_read_dtype_from_file",filename,VOID);
    return FD_ERROR;}
  else {
    struct FD_STREAM *opened=
      fd_init_file_stream(stream,filename,
                          FD_FILE_READ,
                          FD_STREAM_IS_CONSED|
                          FD_STREAM_READ_ONLY|
                          ( (HAVE_MMAP) ? (FD_STREAM_MMAPPED) : (0)),
                          fd_filestream_bufsize);
    if (opened) {
      lispval result = VOID;
      struct FD_INBUF *in = fd_readbuf(opened);
      int byte1 = fd_probe_byte(in);
      int zip = (byte1>=0x80);
      if (zip)
        result = fd_zread_dtype(in);
      else if (byte1 == dt_ztype) {
        fd_read_byte(in);
        result = fd_zread_dtype(in);}
      else result = fd_read_dtype(in);
      fd_free_stream(opened);
      return result;}
    else {
      u8_free(stream);
      return FD_ERROR;}}
}

FD_EXPORT ssize_t fd_lisp2file(lispval object, u8_string filename,
                                ssize_t bufsize,int zip)
{
  struct FD_STREAM *stream = u8_alloc(struct FD_STREAM);
  struct FD_STREAM *opened=
    fd_init_file_stream(stream,filename,
                        FD_FILE_WRITE,
                        FD_STREAM_IS_CONSED,
                        bufsize);
  if (opened) {
    size_t len = (zip)?
      (fd_zwrite_dtype(fd_writebuf(opened),object)):
      (fd_write_dtype(fd_writebuf(opened),object));
    fd_free_stream(opened);
    return len;}
  else return -1;
}

FD_EXPORT ssize_t fd_write_dtype_to_file(lispval object,u8_string filename)
{
  return fd_lisp2file(object,filename,1024*64,0);
}

FD_EXPORT ssize_t fd_write_zdtype_to_file(lispval object,u8_string filename)
{
  return fd_lisp2file(object,filename,1024*64,1);
}

/* Initialization of file */

FD_EXPORT void fd_init_streams_c()
{
  fd_unparsers[fd_stream_type]=unparse_stream;
  fd_recyclers[fd_stream_type]=recycle_stream;

  u8_register_source_file(_FILEINFO);
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
