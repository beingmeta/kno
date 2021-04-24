/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static int stream_loglevel;
#define U8_LOGLEVEL stream_loglevel

#define KNO_INLINE_BUFIO 1
#define KNO_INLINE_STREAMIO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/streams.h"
#include "kno/getsource.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>
#include <libu8/u8printf.h>
#include <libu8/u8rusage.h>
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

#if KNO_USE_MMAP
#include <sys/mman.h>
#endif

static size_t pagesize = 512;

#if ((KNO_LARGEFILES_ENABLED) && (defined(O_LARGEFILE)))
#define POSIX_OPEN_FLAGS O_LARGEFILE
#else
#define POSIX_OPEN_FLAGS 0
#endif

static int stream_loglevel = LOG_WARN;

size_t kno_stream_bufsize = KNO_STREAM_BUFSIZE;
size_t kno_filestream_bufsize = KNO_FILESTREAM_BUFSIZE;

u8_condition kno_ReadOnlyStream=_("Read-only stream");
u8_condition kno_WriteOnlyStream=_("Write-only stream");
u8_condition kno_CantRead=_("Can't read data");
u8_condition kno_CantWrite=_("Can't write data");
u8_condition kno_BadSeek=_("Bad seek argument");
u8_condition kno_CantSeek=_("Can't seek on stream");
u8_condition kno_BadLSEEK=_("lseek() failed");
u8_condition kno_OverSeek=_("Seeking past end of file");
u8_condition kno_UnderSeek=_("Seeking before the beginning of the file");
u8_condition kno_mmap_failed=_("Call to mmap() failed");
u8_condition kno_munmap_failed = _("Call to munmap() failed");
u8_condition kno_CantMMAP=_("Can't MMAP stream buffer");

#define KNO_DEBUG_DTYPEIO 0

KNO_EXPORT ssize_t kno_fill_stream(kno_stream df,size_t n);

static ssize_t stream_fillfn(kno_inbuf buf,size_t n,void *vdata)
{
  return kno_fill_stream((struct KNO_STREAM *)vdata,n);
}
static ssize_t stream_flushfn(kno_outbuf buf,void *vdata)
{
  return kno_flush_stream((struct KNO_STREAM *)vdata);
}

static ssize_t mmap_read_update(struct KNO_STREAM *stream)
{
  /* This updates (or sets up) the MMAP buffer for *stream* if
     needed. Note that his is just for the case where the entire file
     is mmapped; the byte buffer may be anonymously mmapped, but this
     is orthogonal to that. */
  struct KNO_RAWBUF *buf = &(stream->buf.raw);
  int fd = stream->stream_fileno;
  struct stat fileinfo;
  if (fstat(fd,&fileinfo)<0) {
    u8_graberrno("mmap_read_update:fstat",u8_strdup(stream->streamid));
    return -1;}
  else if ( (stream->stream_flags & KNO_STREAM_MMAPPED) &&
            (stream->mmap_time.tv_sec==fileinfo.stat_mtime.tv_sec) &&
            (stream->mmap_time.tv_nsec==fileinfo.stat_mtime.tv_nsec) )
    /* Already mmapped and underlying file not changed */
    return 0;
  else {
    u8_write_lock(&(stream->mmap_lock));
    if (fstat(fd,&fileinfo)<0) {
      u8_rw_unlock(&(stream->mmap_lock));
      u8_graberrno("mmap_read_update:fstat",u8_strdup(stream->streamid));
      return -1;}
    if ((stream->mmap_time.tv_sec==fileinfo.stat_mtime.tv_sec) &&
        (stream->mmap_time.tv_nsec==fileinfo.stat_mtime.tv_nsec)) {
      /* No change. This handles a potential race condition when two
         threads are both doing updates and the first threads update
         obviates the second threads update. */
      u8_rw_unlock(&(stream->mmap_lock));
      return 0;}
    else stream->mmap_time=fileinfo.stat_mtime;
    size_t old_size = buf->buflen, new_size = fileinfo.st_size;
    int prot = (U8_BITP(stream->stream_flags,KNO_STREAM_READ_ONLY)) ?
      (PROT_READ) : (PROT_READ|PROT_WRITE) ;
    int flags = (U8_BITP(stream->stream_flags,KNO_STREAM_READ_ONLY)) ?
      (MAP_NORESERVE|MAP_SHARED) :
      (MAP_SHARED);
    unsigned char *oldbuf = buf->buffer;
    ssize_t point_off = (oldbuf) ? (buf->bufpoint-buf->buffer) : (0);
    unsigned char *newbuf = mmap(NULL,new_size,prot,flags,fd,0);
    if ( (newbuf==NULL) || (newbuf==MAP_FAILED) ) {
      u8_logf(LOG_WARN,u8_strerror(errno),
              "mmap_read_update:mmap of %s",stream->streamid);
      u8_graberrno("mmap_read_update:mmap",u8_strdup(stream->streamid));
      u8_rw_unlock(&(stream->mmap_lock));
      return -1;}
    /* This will unmmap it if neccessary */
    if ( ( oldbuf ) && ( oldbuf != newbuf ) ) {
      int rv = munmap(oldbuf,old_size);
      if (rv!=0)
        u8_logf(LOG_WARN,kno_munmap_failed,
		"Couldn't unmap buffer for %s (%p)",stream->streamid,stream);}
    buf->buflen   = new_size;
    buf->buffer   = newbuf;
    buf->bufpoint = newbuf+point_off;
    buf->buflim   = newbuf+new_size;
    buf->buf_data = stream;
    stream->stream_filepos=stream->stream_maxpos=fileinfo.st_size;
    BUFIO_SET_ALLOC(buf,KNO_MMAP_BUFFER);
    u8_rw_unlock(&(stream->mmap_lock));
    return new_size;}
}

static ssize_t mmap_write_update(struct KNO_STREAM *stream,ssize_t grow)
{
  struct KNO_RAWBUF *buf = &(stream->buf.raw);
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
  else if (new_size<=0) new_size=kno_stream_bufsize;
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

static void release_mmap(struct KNO_STREAM *stream)
{
  if ((stream->stream_flags)&(KNO_STREAM_MMAPPED)) {
    struct KNO_RAWBUF *rawbuf=&(stream->buf.raw);
    if ((rawbuf->buffer)&&(rawbuf->buflen>=0)) {
      int rv=munmap(rawbuf->buffer,rawbuf->buflen);
      if (rv<0) {
        u8_logf(LOG_WARN,_("MUNMAP failed"),
                "Failed to unmap %s (errno=%d) %s",
                stream->streamid,errno,u8_strerror(errno));
        U8_CLEAR_ERRNO();}
      rawbuf->buffer=rawbuf->bufpoint=rawbuf->buflim=NULL;
      rawbuf->buflen=-1;}}
  else u8_logf(LOG_WARN,_("Not MMapped"),
               "Attempting to munmap a non-mapped stream %s",
               stream->streamid);
}

static ssize_t mmap_fillfn(kno_inbuf buf,size_t n,void *vdata)
{
  return mmap_read_update((kno_stream)vdata);
}
static ssize_t mmap_flushfn(kno_outbuf buf,void *vdata)
{
  return mmap_write_update((kno_stream)vdata,kno_filestream_bufsize);
}

static U8_MAYBE_UNUSED u8_byte _dbg_outbuf[KNO_DEBUG_OUTBUF_SIZE];

/* Utility functions */

static int writeall(int fd,const unsigned char *data,int n)
{
  int written = 0;
  while (written<n) {
    int delta = write(fd,data+written,n-written);
    if (delta<0)
      if (errno == EAGAIN) errno = 0;
      else {
        u8_logf(LOG_ERROR,
                "write failed","writeall %d errno=%d (%s) written=%_d/%_d",
                fd,errno,strerror(errno),written,n);
        return delta;}
    else written = written+delta;}
  return n;
}

static ssize_t failed_pread(int fileno,size_t offset,size_t len)
{
  struct stat fileinfo;
  if (errno) u8_graberrno("failed_pread",NULL);
  int rv = fstat(fileno,&fileinfo);
  if (rv<0) {
    u8_seterr("StatFailed","failed_pread",NULL);
    return -1;}
  else if (offset > fileinfo.st_size) {
    u8_seterr("InvalidOffset","failed_pread",
              u8_mkstring("%lld+%lld > %lld",offset,len,fileinfo.st_size));
    return -1;}
  else if (offset+len > fileinfo.st_size) {
    u8_seterr("InvalidBlockRef","failed_pread",
              u8_mkstring("%lld+%lld > %lld",offset,len,fileinfo.st_size));
    return -1;}
  else {
    u8_seterr("InvalidBlockRef","failed_pread",
              u8_mkstring("%lld+%lld > %lld",offset,len,fileinfo.st_size));
    return -1;}
}

#if HAVE_PREAD
static ssize_t pread_all(int fileno,unsigned char *buf,size_t len,size_t offset)
{
  ssize_t to_read = len, delta = 0;
  unsigned char *point = buf;
  while (to_read > 0) {
    delta = pread(fileno,point,to_read,offset);
    if (delta < 0) break;
    if ( (delta == 0) && (to_read > 0) )
      return failed_pread(fileno,offset,len);
    to_read -= delta;
    offset  += delta;
    point   += delta;}
  if (delta>=0)
    return len-to_read;
  else return delta;
}
static ssize_t stream_pread(struct KNO_STREAM *s,int locked,
                            unsigned char *buf,size_t len,size_t offset)
{
  if ((KNO_ISWRITING(&(s->buf.raw))) &&
      ((offset+len)>(s->stream_filepos))) {
    /* This is the case where the stream is being written to and we're
       reading from beyond the position it's writing at. In this case,
       we lock the stream to do our read, which will block until the
       writer is done. At that point, we'll set the direction on the
       stream which will flush any buffered output. */
    ssize_t result;
    if (!(locked)) kno_lock_stream(s);
    kno_set_direction(s,kno_byteflow_read);
    result = pread_all(s->stream_fileno,buf,len,offset);
    if (!(locked)) kno_unlock_stream(s);
    return result;}
  else return pread_all(s->stream_fileno,buf,len,offset);
}
#endif

/* Unwrappers */

KNO_EXPORT kno_inbuf _kno_readbuf(kno_stream s)
{
  if (KNO_STREAM_ISWRITING(s))
    kno_set_direction(s,kno_byteflow_read);
  return &(s->buf.in);
}

KNO_EXPORT kno_outbuf _kno_writebuf(kno_stream s)
{
  if (!(KNO_STREAM_ISWRITING(s)))
    kno_set_direction(s,kno_byteflow_write);
  return &(s->buf.out);
}

KNO_EXPORT kno_inbuf _kno_start_read(kno_stream s,kno_off_t pos)
{
  if (KNO_STREAM_ISWRITING(s))
    kno_set_direction(s,kno_byteflow_read);
  if (pos<0) kno_endpos(s);
  else kno_setpos(s,pos);
  return &(s->buf.in);
}

KNO_EXPORT kno_outbuf _kno_start_write(kno_stream s,kno_off_t pos)
{
  if (!(KNO_STREAM_ISWRITING(s)))
    kno_set_direction(s,kno_byteflow_write);
  if (pos<0) kno_endpos(s);
  else kno_setpos(s,pos);
  return &(s->buf.out);
}

/* Initialization functions */

KNO_EXPORT struct KNO_STREAM *kno_init_stream
(kno_stream stream,u8_string streamid,int fileno,int flags,ssize_t bufsiz)
{
  struct KNO_RAWBUF *streambuf = &stream->buf.raw;
  if (bufsiz<0) bufsiz = kno_stream_bufsize;
  /* We need to check before this is called that the fileno is valid */
  /* if (fileno<0) return NULL; */
  if (flags&KNO_STREAM_IS_CONSED) {
    KNO_INIT_FRESH_CONS(stream,kno_stream_type);}
  else {KNO_INIT_STATIC_CONS(stream,kno_stream_type);}
  /* Initializing the stream fields */
  stream->stream_fileno = fileno;
  stream->streamid = u8dup(streamid);
  stream->stream_filepos = -1;
  stream->stream_maxpos = -1;
  stream->stream_flags = flags;
  stream->stream_lisprefs = KNO_EMPTY;
#if KNO_USE_MMAP
  u8_init_rwlock(&(stream->mmap_lock));
  if (U8_BITP(flags,KNO_STREAM_MMAPPED)) {
    if (U8_BITP(flags,KNO_STREAM_READ_ONLY)) {
      ssize_t rv= (U8_BITP(flags,KNO_STREAM_READ_ONLY)) ?
        (mmap_read_update(stream)) :
        (mmap_write_update(stream,bufsiz));
      if (rv>=0) {
        u8_init_mutex(&(stream->stream_lock));
        streambuf->buf_fillfn  = mmap_fillfn;
        streambuf->buf_flushfn = mmap_flushfn;
        streambuf->buf_data    = (void *) stream;
        streambuf->buf_flags   = KNO_IN_STREAM|KNO_BUFFER_NO_GROW;
        streambuf->buf_data = (void *)stream;
        streambuf->buflen = rv;
        return stream;}
      else {
        u8_logf(LOG_INFO,"FailedStreamMMAP",
                "Unable to mmap '%s'; errno=%d (%s)",
                streamid,errno,u8_strerror(errno));
        stream->stream_flags&=~KNO_STREAM_MMAPPED;
        errno = 0;}}
    else stream->stream_flags&=~KNO_STREAM_MMAPPED;}
#endif
  int buf_flags = ( KNO_HEAP_BUFFER | KNO_IN_STREAM );
  unsigned char *buf = u8_malloc(bufsiz);
  if (flags & KNO_STREAM_DTYPEV2) buf_flags |= KNO_USE_DTYPEV2;
  /* If you can't get a whole buffer, try smaller */
  while ((bufsiz>=1024) && (buf == NULL)) {
    u8_logf(LOG_WARN,"BigBuffer",
            "Can't allocate %lld-byte buffer for %s, trying %lld",
            bufsiz,(U8ALT(streamid,"somestream")),bufsiz/2);
    bufsiz = bufsiz/2; buf = u8_malloc(bufsiz);}
  u8_init_mutex(&(stream->stream_lock));
  if (buf == NULL) bufsiz = 0;
  /* Initialize the buffer fields */
  KNO_INIT_BYTE_INPUT((struct KNO_INBUF *)streambuf,buf,bufsiz);
  streambuf->buflim      = streambuf->bufpoint;
  streambuf->buf_fillfn  = stream_fillfn;
  streambuf->buf_flushfn = stream_flushfn;
  streambuf->buf_flags  |= buf_flags;
  streambuf->buf_data    = (void *)stream;
  streambuf->buflen      = bufsiz;
  return stream;
}

KNO_EXPORT struct KNO_STREAM *kno_init_byte_stream
(kno_stream stream,u8_string streamid,int flags,
 ssize_t n_bytes,const unsigned char *bytes)
{
  struct KNO_RAWBUF *streambuf = &stream->buf.raw;
  /* We need to check before this is called that the fileno is valid */
  /* if (fileno<0) return NULL; */
  if (flags&KNO_STREAM_IS_CONSED) {
    KNO_INIT_FRESH_CONS(stream,kno_stream_type);}
  else {KNO_INIT_STATIC_CONS(stream,kno_stream_type);}
  /* Initializing the stream fields */
  stream->stream_fileno = -1;
  stream->streamid = u8dup(streamid);
  stream->stream_filepos = -1;
  stream->stream_maxpos = -1;
  stream->stream_flags = flags;
  stream->stream_lisprefs = KNO_EMPTY;
  int buf_flags = ( KNO_HEAP_BUFFER | KNO_IN_STREAM );
  if (flags & KNO_STREAM_DTYPEV2) buf_flags |= KNO_USE_DTYPEV2;
  u8_init_mutex(&(stream->stream_lock));
  /* Initialize the buffer fields */
  KNO_INIT_BYTE_INPUT((struct KNO_INBUF *)streambuf,bytes,n_bytes);
  streambuf->buflim      = streambuf->bufpoint;
  streambuf->buf_flags  |= buf_flags;
  streambuf->buf_data    = (void *)stream;
  streambuf->buflen      = n_bytes;
  return stream;
}

KNO_EXPORT kno_stream kno_init_file_stream
(kno_stream stream,u8_string fname,
 kno_stream_mode mode,
 kno_stream_flags stream_flags,
 ssize_t bufsiz)
{
  if (bufsiz<0) bufsiz = kno_filestream_bufsize;
  if (stream_flags<0)
    stream_flags = KNO_DEFAULT_FILESTREAM_FLAGS;
  stream_flags |= KNO_STREAM_OWNS_FILENO;
  int fd, open_flags = POSIX_OPEN_FLAGS;
  char *localname = u8_localpath(fname);
  if (mode<0) {
    if ( (stream_flags) & (KNO_STREAM_READ_ONLY) )
      mode=KNO_FILE_READ;
    else if (!(u8_file_existsp(fname)))
      mode=KNO_FILE_CREATE;
    else if (u8_file_writablep(fname))
      mode=KNO_FILE_MODIFY;
    else mode=KNO_FILE_READ;}
  switch (mode) {
  case KNO_FILE_READ:
    open_flags |= O_RDONLY;
    stream_flags |= KNO_STREAM_READ_ONLY;
    break;
  case KNO_FILE_MODIFY:
    open_flags |= O_RDWR;
    stream_flags |= KNO_STREAM_NEEDS_LOCK;
    break;
  case KNO_FILE_WRITE:
    open_flags |= O_RDWR|O_CREAT;
    stream_flags |= KNO_STREAM_NEEDS_LOCK;
    break;
  case KNO_FILE_CREATE:
    open_flags |= O_CREAT|O_EXCL|O_RDWR;
    stream_flags |= KNO_STREAM_NEEDS_LOCK;
    break;
  case KNO_FILE_INIT:
    open_flags |= O_CREAT|O_RDWR;
    stream_flags |= KNO_STREAM_NEEDS_LOCK;
    break;
  case KNO_FILE_TRUNC:
    open_flags |= O_CREAT|O_RDWR;
    stream_flags |= KNO_STREAM_NEEDS_LOCK;
    break;
  case KNO_FILE_NOVAL: /* Never reached */
    break;}
  fd = open(localname,open_flags,0666);
  /* If we fail and we're modifying, try to open read-only */
  if (fd>0) {
    U8_CLEAR_ERRNO();
    if (stream == NULL) {
      stream = u8_alloc(struct KNO_STREAM);
      stream_flags |= KNO_STREAM_IS_CONSED;}
    kno_init_stream(stream,fname,fd,stream_flags,bufsiz);
    if (stream->stream_maxpos<=0) {
      stream->stream_maxpos = lseek(fd,0,SEEK_END);
      stream->stream_filepos = lseek(fd,0,SEEK_SET);}
    u8_init_mutex(&(stream->stream_lock));
    u8_free(localname);
    return stream;}
  else {
    kno_seterr3(u8_strerror(errno),"kno_init_file_stream",fname);
    u8_free(localname);
    U8_CLEAR_ERRNO();
    return NULL;}
}

KNO_EXPORT kno_stream kno_open_file(u8_string fname,kno_stream_mode mode)
{
  struct KNO_STREAM *stream = u8_alloc(struct KNO_STREAM);
  struct KNO_STREAM *opened;
  kno_stream_flags flags = KNO_STREAM_IS_CONSED|KNO_DEFAULT_FILESTREAM_FLAGS;
  KNO_INIT_CONS(stream,kno_stream_type);
  if ((opened = kno_init_file_stream(stream,fname,mode,flags,-1))) {
    return opened;}
  else {
    u8_free(stream);
    return NULL;}
}

KNO_EXPORT kno_stream kno_reopen_file_stream
(kno_stream stream,
 kno_stream_mode mode,
 ssize_t bufsiz)
{
  kno_stream_flags stream_flags=stream->stream_flags;
  if (bufsiz<0) bufsiz = kno_filestream_bufsize;
  stream_flags |= KNO_STREAM_OWNS_FILENO;
  int fd, open_flags = POSIX_OPEN_FLAGS;
  int needs_lock=0, read_only=0;
  u8_string fname = stream->streamid;
  char *localname = u8_localpath(fname);
  if (mode<0) {
    if ( (stream_flags) & (KNO_STREAM_READ_ONLY) )
      mode=KNO_FILE_READ;
    else mode=KNO_FILE_MODIFY;}
  switch (mode) {
  case KNO_FILE_READ:
    open_flags |= O_RDONLY;
    read_only=1;
    break;
  case KNO_FILE_MODIFY:
    open_flags |= O_RDWR;
    needs_lock=1;
    stream_flags |= KNO_STREAM_NEEDS_LOCK;
    break;
  case KNO_FILE_WRITE:
    open_flags |= O_RDWR|O_CREAT;
    needs_lock=1;
    break;
  case KNO_FILE_INIT:
    open_flags |= O_RDWR|O_EXCL|O_CREAT;
    needs_lock=1;
    break;
  case KNO_FILE_TRUNC:
    open_flags |= O_RDWR|O_TRUNC|O_CREAT;
    needs_lock=1;
    break;
  case KNO_FILE_CREATE:
    open_flags |= O_CREAT|O_TRUNC|O_RDWR;
    needs_lock=1;
    break;
  case KNO_FILE_NOVAL:
    break;}

  if (read_only)
    stream_flags |= KNO_STREAM_READ_ONLY;
  else stream_flags &= (~(KNO_STREAM_READ_ONLY));
  if (needs_lock)
    stream_flags |= KNO_STREAM_NEEDS_LOCK;
  else stream_flags &= (~(KNO_STREAM_NEEDS_LOCK));

  fd = open(localname,open_flags,0666);

  /* If we fail and we're modifying, try to open read-only */

  if (fd>0) {
    U8_CLEAR_ERRNO();
    struct stat info;
    int rv=fstat(fd,&info);
    stream->stream_fileno=fd;
    stream->stream_flags=stream_flags;
    if (rv<0) {
      u8_logf(LOG_WARN,"Reopen/StatFailed","Couldn't call stat on fileno %s(fd=%d)",
              stream->streamid,fd);
      stream->stream_maxpos = lseek(fd,0,SEEK_END);
      stream->stream_filepos = lseek(fd,0,SEEK_SET);}
    else {
      int at_end = (stream->stream_maxpos==stream->stream_filepos);
      stream->stream_maxpos = info.st_size;
      if (stream->stream_filepos>0) {
        if ((stream->stream_filepos) > (stream->stream_maxpos)) {
          u8_logf(LOG_WARN,"Reopen/FileTruncated",
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
    u8_graberrno("open",u8_fromlibc(localname));
    kno_seterr3(u8_CantOpenFile,"kno_init_file_stream",fname);
    u8_free(localname);
    return NULL;}
}

KNO_EXPORT void kno_close_stream(kno_stream s,int flags)
{
  int dofree    = (U8_BITP(flags,KNO_STREAM_FREEDATA));
  int flush     = !(U8_BITP(flags,KNO_STREAM_NOFLUSH));

  /* Already closed */
  if (s->stream_fileno<0) {
    struct KNO_RAWBUF *buf = &(s->buf.raw);
    if (!(buf->buffer)) return;}

  /* Lock before closing */
  kno_lock_stream(s);

  /* Flush data */
  if (!(flush)) {}
#if KNO_USE_MMAP
  else if ((U8_BITP(s->stream_flags,KNO_STREAM_MMAPPED))&&
           (!((U8_BITP(s->stream_flags,KNO_STREAM_READ_ONLY))))) {
    ssize_t rv = mmap_write_update(s,-1);
    if (rv<0) kno_clear_errors(1);}
#endif
  else if ((U8_BITP(s->buf.raw.buf_flags,KNO_IS_WRITING))&&
           (s->buf.out.bufwrite>s->buf.out.buffer)) {
    if (s->stream_fileno<0) {
      u8_logf(LOG_CRIT,_("StreamClosed"),
              "Stream %s (%p) was closed with %_d bytes still buffered",
              U8ALT(s->streamid,"somestream"),s,
              (s->buf.out.bufwrite-s->buf.out.buffer));}
    else kno_flush_stream(s);}
  else {}

  if ((flush) && (s->stream_fileno>=0))
    fsync(s->stream_fileno);

  if (s->stream_fileno>=0) {
    if ((s->stream_flags&KNO_STREAM_OWNS_FILENO)&&
        (!(flags&KNO_STREAM_NOCLOSE))) {
      if (s->stream_flags&KNO_STREAM_SOCKET)
        shutdown(s->stream_fileno,SHUT_RDWR);
      close(s->stream_fileno);}
    s->stream_fileno = -1;}

  if (dofree) {
    struct KNO_RAWBUF *buf = &(s->buf.raw);
    if (s->streamid) {
      u8_free(s->streamid);
      s->streamid = NULL;}
    if (buf->buffer) {
      if (s->stream_flags&KNO_STREAM_MMAPPED)
        release_mmap(s);
      else if ( (BUFIO_ALLOC(buf)) == KNO_HEAP_BUFFER )
        u8_free(buf->buffer);
      else if ( (BUFIO_ALLOC(buf)) == KNO_BIGALLOC_BUFFER )
        u8_big_free(buf->buffer);
      else {}
      buf->buffer = buf->bufpoint = buf->buflim = NULL;}
    kno_unlock_stream(s);
    u8_destroy_mutex(&(s->stream_lock));}
  else kno_unlock_stream(s);
}

KNO_EXPORT void kno_free_stream(kno_stream s)
{
  struct KNO_CONS *cons = (struct KNO_CONS *)s;
  lispval sptr = (lispval)s;
  if (KNO_STATIC_CONSP(cons)) {
    u8_logf(LOG_WARN,_("FreeingStaticStream"),
            "Attempting to free the static stream %s %p",
            U8ALT(s->streamid,"somestream"),s);}
  else if (KNO_CONS_REFCOUNT(cons)>1) {
    u8_logf(LOG_WARN,_("DanglingStreamPointers"),
            "Freeing a stream with dangling pointers %s %p",
            U8ALT(s->streamid,"somestream"),s);
    kno_decref(sptr);}
  else kno_decref(sptr);
}

static ssize_t set_mmapped(kno_stream s,ssize_t bufsize,int unlock);

KNO_EXPORT ssize_t kno_setbufsize(kno_stream s,ssize_t bufsize)
{
  int unlock=kno_using_stream(s); kno_flush_stream(s);
  struct KNO_RAWBUF *bufptr = &(s->buf.raw);
  if (bufsize<0)
    return set_mmapped(s,bufsize,unlock);
  else if (bufsize) {
    int flags = bufptr->buf_flags;
    unsigned char *oldbuf=bufptr->buffer, *newbuf;
    size_t point=bufptr->bufpoint-oldbuf, maxpoint=bufptr->buflim-oldbuf;
    bufio_alloc cur_alloc = BUFIO_ALLOC(bufptr), new_alloc=cur_alloc;
    if (oldbuf == NULL) {}
    else if ( (U8_BITP(flags,KNO_STREAM_MMAPPED)) ||
              (cur_alloc == KNO_MMAP_BUFFER) ){
      int rv=munmap(bufptr->buffer,bufptr->buflen);
      if (rv<0) {
        u8_logf(LOG_WARN,kno_munmap_failed,
                "Couldn't unmap buffer for stream '%s'",s->streamid);
        U8_CLEAR_ERRNO();}
      oldbuf=NULL;}
    if (oldbuf == NULL) {
      if ( bufsize < kno_bigbuf_threshold ) {
        newbuf    = u8_malloc(bufsize);
        new_alloc = KNO_HEAP_BUFFER;}
      else {
        newbuf    = u8_big_alloc(bufsize);
        new_alloc = KNO_BIGALLOC_BUFFER;}}
    else if (cur_alloc == KNO_BIGALLOC_BUFFER)
      newbuf = u8_big_realloc(oldbuf,bufsize);
    else if ( bufsize >= kno_bigbuf_threshold ) {
      newbuf    = u8_big_copy(oldbuf,bufsize,point);
      new_alloc = KNO_BIGALLOC_BUFFER;}
    else if (cur_alloc)
      newbuf = u8_realloc(oldbuf,bufsize);
    else if ( bufsize >= kno_bigbuf_threshold ) {
      newbuf = u8_big_copy(oldbuf,bufsize,point);
      new_alloc = KNO_BIGALLOC_BUFFER;}
    else {
      newbuf = u8_malloc(bufsize);
      memcpy(newbuf,oldbuf,point);}
    if (newbuf == NULL) {
      u8_seterr("MallocFailed","kno_setbufsize",u8_strdup(s->streamid));
      if (unlock) kno_unlock_stream(s);
      return -1;}
    bufptr->buffer=newbuf;
    if (point < bufsize)
      bufptr->bufpoint = newbuf+point;
    else bufptr->bufpoint=bufptr->buffer;
    if (maxpoint < bufsize)
      bufptr->buflim = newbuf+maxpoint;
    else bufptr->buflim=bufptr->buffer+bufsize;
    bufptr->buflen = bufsize;
    BUFIO_SET_ALLOC(bufptr,new_alloc);
    if (unlock) kno_unlock_stream(s);
    return bufsize;}
  else {
    bufio_alloc cur_alloc = BUFIO_ALLOC(bufptr);
    if ( (U8_BITP(s->stream_flags,KNO_STREAM_MMAPPED)) ||
         (cur_alloc == KNO_MMAP_BUFFER) ) {
      int rv=munmap(bufptr->buffer,bufptr->buflen);
      if (rv<0) {
        u8_logf(LOG_WARN,"MunmapFailed",
                "Could unmap buffer for stream '%s'",s->streamid);
        U8_CLEAR_ERRNO();}}
    else if (cur_alloc ==  KNO_HEAP_BUFFER)
      u8_free(bufptr->buffer);
    else if (cur_alloc ==  KNO_BIGALLOC_BUFFER)
      u8_big_free(bufptr->buffer);
    else {}
    bufptr->buffer=NULL;
    bufptr->bufpoint=NULL;
    bufptr->buflim=NULL;
    bufptr->buflen=-1;
    BUFIO_SET_ALLOC(bufptr,KNO_STATIC_BUFFER);
    if (unlock) kno_unlock_stream(s);
    return 0;}
}

static ssize_t set_mmapped(kno_stream s,ssize_t bufsize,int unlock)
{
#if KNO_USE_MMAP
  if (U8_BITP(s->stream_flags,KNO_STREAM_MMAPPED))
    return 0;
  else if (!(U8_BITP(s->stream_flags,KNO_STREAM_READ_ONLY))) {
    /* We're only MMAPPing read-only streams (for now?) */
    u8_logf(LOG_INFO,kno_CantMMAP,"Stream '%s' not read-only",s->streamid);
    if (unlock) kno_unlock_stream(s);
    return -1;}
  else {
    /* This will set up the stream for MMAP */
    ssize_t new_size=mmap_read_update(s);
    if (unlock) kno_unlock_stream(s);
    return new_size;}
#else
  u8_logf(LOG_WARN,kno_CantMMAP,
          "Trying to mmap the stream %llx (%s), "
          "but this library was not built with MMAP support",
          s,s->streamid);
  return 0;
#endif
}

/* CONS handlers */

static int unparse_stream(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_STREAM *stream = (struct KNO_STREAM *)x;
  if (stream->streamid) {
    u8_printf(out,"#<Stream (%s) #!%llx>",stream->streamid,x);}
  else u8_printf(out,"#<Stream #!%llx>",x);
  return 1;
}

static void recycle_stream(struct KNO_RAW_CONS *c)
{
  struct KNO_STREAM *stream = (struct KNO_STREAM *)c;
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  if (stream->stream_lisprefs != KNO_NULL)
    kno_decref(stream->stream_lisprefs);
  if (KNO_MALLOCD_CONSP(c)) u8_free(c);
}

/* Structure functions */

KNO_EXPORT
ssize_t kno_fill_stream(kno_stream stream,size_t n)
{
  struct KNO_RAWBUF *buf = &(stream->buf.raw);
  int n_buffered, n_to_read, read_request, bytes_read = 0;
  int fileno = stream->stream_fileno;

  if (fileno<0) return 0;

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
      kno_seterr3(u8_strerror(errno),"fill_stream",stream->streamid);
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

KNO_EXPORT
ssize_t kno_flush_stream(kno_stream stream)
{
  if (U8_BITP(stream->stream_flags,KNO_STREAM_MMAPPED))
    return mmap_write_update(stream,0);
  else {
    struct KNO_RAWBUF *buf = &(stream->buf.raw);
    if (KNO_STREAM_ISREADING(stream)) {
      buf->bufpoint = buf->buflim = buf->buffer;
      return 0;}
    else if (buf->bufpoint>buf->buffer) {
      int fileno = stream->stream_fileno;
      size_t n_buffered = buf->bufpoint-buf->buffer;
      int bytes_written = writeall(fileno,buf->buffer,n_buffered);
      if (bytes_written<0) {
        return -1;}
      if ((stream->stream_flags)&KNO_STREAM_DOSYNC) fsync(fileno);
      if ( (stream->stream_flags&KNO_STREAM_CAN_SEEK) && (fileno>=0) )
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

KNO_EXPORT int _kno_lock_stream(kno_stream s)
{
  return kno_lock_stream(s);
}

KNO_EXPORT int _kno_unlock_stream(kno_stream s)
{
  return kno_unlock_stream(s);
}

KNO_EXPORT int _kno_using_stream(kno_stream s)
{
  return kno_using_stream(s);
}

static int lock_filestream(kno_stream s)
{
  if (s->stream_flags&KNO_STREAM_FILE_LOCKED)
    return 1;
  else if ((u8_lock_fd(s->stream_fileno,1))>=0) {
    s->stream_flags = s->stream_flags|KNO_STREAM_FILE_LOCKED;
    return 1;}
  return 0;
}

static int unlock_filestream(kno_stream s)
{
  if (!(s->stream_flags&KNO_STREAM_FILE_LOCKED))
    return 1;
  else if ((u8_unlock_fd(s->stream_fileno))>=0) {
    s->stream_flags = s->stream_flags|KNO_STREAM_FILE_LOCKED;
    return 1;}
  return 0;
}

KNO_EXPORT int kno_set_direction(kno_stream s,kno_byteflow direction)
{
  struct KNO_RAWBUF *buf = &(s->buf.raw);
  if (direction == kno_byteflow_write) {
    if (KNO_STREAM_ISWRITING(s))
      return 0;
    else if ((s->stream_fileno<0) || ((s->stream_flags)&KNO_STREAM_READ_ONLY) ) {
      kno_seterr(kno_ReadOnlyStream,"kno_set_direction",s->streamid,VOID);
      return -1;}
    else {
      if ((s->stream_flags)&KNO_STREAM_NEEDS_LOCK) {
        if (u8_lock_fd(s->stream_fileno,1)) {
          (s->stream_flags) = (s->stream_flags)|KNO_STREAM_FILE_LOCKED;}
        else return -1;}
      buf->bufpoint = buf->buffer;
      buf->buflim = buf->buffer+buf->buflen;
      /* Now we clear the bit */
      buf->buf_flags |= KNO_IS_WRITING;
      return 1;}}
  else if (direction == kno_byteflow_read) {
    if (!((buf->buf_flags)&(KNO_IS_WRITING)))
      return 0;
    else  if ((s->stream_flags)&KNO_STREAM_WRITE_ONLY) {
      kno_seterr(kno_WriteOnlyStream,"kno_set_direction",s->streamid,VOID);
      return -1;}
    else {
      /* If we were writing, in order to start reading, we need
         to flush what is buffered to the output and collapse all
         of the pointers into the start. We also need to update bufsiz
         in case the output buffer grew while we were writing. */
      if (kno_flush_stream(s)<0) {
        return -1;}
      /* Now we reset bufsiz in case we grew the buffer */
      buf->buflen = buf->buflim-buf->buffer;
      /* Finally, we reset the pointers */
      buf->buflim = buf->bufpoint = buf->buffer;
      buf->buf_flags &= ~KNO_IS_WRITING;
      return 1;}}
  else return 0;
}

/* This gets the position when it isn't cached on the stream. */
KNO_EXPORT kno_off_t _kno_getpos(kno_stream s)
{
  struct KNO_RAWBUF *buf = &(s->buf.raw);
  kno_off_t current, pos;
  if (((s->stream_flags)&KNO_STREAM_CAN_SEEK) == 0) {
    return kno_reterr(kno_CantSeek,"kno_getpos",u8s(s->streamid),VOID);}
  if (!((buf->buf_flags)&KNO_IS_WRITING)) {
    current = lseek(s->stream_fileno,0,SEEK_CUR);
    /* If we are reading, we subtract the amount buffered from the
       actual filepos */
    s->stream_filepos = current;
    pos = current-(buf->buflim-buf->bufpoint);}
  else {
    current = lseek(s->stream_fileno,0,SEEK_CUR);
    s->stream_filepos = current;
    /* If we are writing, we add the amount buffered for output to the
       actual filepos */
    pos = current+(buf->bufpoint-buf->buffer);}
  return pos;
}
KNO_EXPORT kno_off_t _kno_setpos(kno_stream s,kno_off_t pos)
{
  struct KNO_RAWBUF *buf = &(s->buf.raw);

  if ( (s->stream_flags) & (KNO_STREAM_MMAPPED) ) {
    size_t curpos = s->buf.raw.bufpoint - s->buf.raw.buffer;
    size_t maxpos = s->stream_maxpos;
    if ( curpos > maxpos ) s->stream_maxpos = maxpos = curpos;
    if (pos <= maxpos) {
      s->buf.raw.bufpoint = s->buf.raw.buffer+pos;
      return pos;}
    else {
      s->buf.raw.bufpoint = s->buf.raw.buflim;
      return kno_reterr(kno_OverSeek,"kno_setpos",
                        u8s(s->streamid),KNO_INT(pos));}}

  /* This is optimized for the case where the new position is
     in the range we have buffered. */
  if (((s->stream_flags)&KNO_STREAM_CAN_SEEK) == 0) {
    return kno_reterr(kno_CantSeek,"kno_setpos",u8s(s->streamid),KNO_INT(pos));}
  else if (pos<0) {
    return kno_reterr(kno_BadSeek,"kno_setpos",u8s(s->streamid),KNO_INT(pos));}
  else {}

  /* First, if we're reading, see if the designated position is already
     in the buffer. */
  if ( (s->stream_filepos>=0) && (KNO_STREAM_ISREADING(s)) ) {
    kno_off_t delta = (pos-s->stream_filepos);
    unsigned char *relptr = buf->buflim+delta;
    if ( (relptr >= buf->buffer) && (relptr < buf->buflim )) {
      buf->bufpoint = relptr;
      return pos;}}
  /* We could check here that we're not jumping back to something in
     the buffer, but we're not optimizing for that, expecting it to be
     relatively rare. So at this point, we're commited to moving the
     filepos (after flushing buffered output of course). */
  if (kno_flush_stream(s)<0) {
    return -1;}
  kno_off_t newpos = lseek(s->stream_fileno,pos,SEEK_SET);
  if (newpos>=0) {
    s->stream_filepos = newpos;
    return newpos;}
  else if (errno == EINVAL) {
    kno_off_t maxpos = lseek(s->stream_fileno,(kno_off_t)0,SEEK_END);
    s->stream_maxpos = s->stream_filepos = maxpos;
    return kno_reterr(kno_OverSeek,"kno_setpos",u8s(s->streamid),KNO_INT(pos));}
  else {
    u8_graberrno("kno_setpos",u8s(s->streamid));
    return -1;}
}
KNO_EXPORT kno_off_t _kno_endpos(kno_stream s)
{
  kno_off_t rv;
  if (((s->stream_flags)&KNO_STREAM_CAN_SEEK) == 0)
    return kno_reterr(kno_CantSeek,"kno_endpos",u8s(s->streamid),VOID);
  kno_flush_stream(s);
  rv = s->stream_maxpos =
    s->stream_filepos = (lseek(s->stream_fileno,0,SEEK_END));
  return rv;
}

KNO_EXPORT kno_off_t kno_movepos(kno_stream s,kno_off_t delta)
{
  kno_off_t cur, rv;
  if (((s->stream_flags)&KNO_STREAM_CAN_SEEK) == 0)
    return kno_reterr(kno_CantSeek,"kno_movepos",u8s(s->streamid),KNO_INT(delta));
  cur = kno_getpos(s);
  rv = kno_setpos(s,cur+delta);
  return rv;
}

KNO_EXPORT ssize_t kno_read_block(kno_stream s,unsigned char *buf,
                                  size_t count,kno_off_t offset,
                                  int stream_locked)
{
#if HAVE_PREAD
  return stream_pread(s,stream_locked,buf,count,offset);
#else
  kno_off_t result = -1;
  if (!(stream_locked)) kno_lock_stream(s);
  kno_setpos(s,offset);
  result = kno_read_bytes(kno_readbuf(s),buf,count);
  if (!(stream_locked)) kno_unlock_stream(s);
  return result;
#endif
}

static void unlock_stream_mmap(struct KNO_INBUF *out,void *streamptr)
{
  kno_stream s = (kno_stream) streamptr;
  out->buffer=out->bufread=out->buflim=NULL;
  out->buflen=0;
  u8_rw_unlock(&(s->mmap_lock));
}

KNO_EXPORT kno_inbuf kno_open_block(kno_stream s,kno_inbuf in,
                                    kno_off_t offset,ssize_t len,
                                    int stream_locked)
{
  if ((offset<0) || (len<0) ) {
    u8_seterr("InvalidOffsets","kno_open_block",s->streamid);
    return NULL;}
  if ((s->stream_flags) & (KNO_STREAM_MMAPPED)) {
    /* If the stream (whole file) is mmapped, just reference a slice
       of the mapped file. */
    if (s->buf.in.buffer != in->buffer) {
      u8_read_lock(&(s->mmap_lock));
      in->buffer=s->buf.in.buffer;
      in->buflen=s->buf.in.buflen;
      in->buf_data=s;
      in->buf_closefn=unlock_stream_mmap;}
    in->bufread=in->buffer+offset;
    in->buflim=in->buffer+offset+len;
    return in;}
#if KNO_USE_MMAP
  /* If the input stream doesn't have a buffer or it already has an
     MMAPPed buffer, use MMAP */
  if ( (in->buffer == NULL) || (BUFIO_ALLOC(in) == KNO_MMAP_BUFFER) ) {
    if ( (in->buffer) && (BUFIO_ALLOC(in) == KNO_MMAP_BUFFER) ) {
      BUFIO_FREE(in);}
    errno = 0;
    kno_off_t page_offset = pagesize * (offset/pagesize);
    kno_off_t read_offset  = offset-page_offset;
    size_t buflen         = (offset+len)-page_offset;
    unsigned char *buf = mmap(NULL,buflen,PROT_READ,MAP_SHARED,
                              s->stream_fileno,page_offset);
    if ( (buf) && (buf != MAP_FAILED) ) {
      if ( errno ) {
        u8_logf(LOG_WARN,"MMAP:Errno",
                "Errno set by mmap %d %s ptr = %llx",
                errno,u8_strerror(errno),buf);
        errno=0;}
      in->buffer  = buf;
      in->bufread = buf+read_offset;
      in->buflim  = buf+buflen;
      in->buflen  = buflen;
      BUFIO_SET_ALLOC(in,KNO_MMAP_BUFFER);
      return in;}
    else {
      u8_logf(LOG_WARN,kno_mmap_failed,
              "Couldn't open block into %llx (%s), errno=%d (%s)",
              s,s->streamid,errno,u8_strerror(errno));
      errno=0;}
  }
#endif
  unsigned char *buf;
  if ( ( in->buffer ) && (len < in->buflen) )
    buf=(unsigned char *)in->buffer;
  else {
    if (kno_grow_byte_input(in,len)<0)
      return NULL;
    buf=(unsigned char *)in->buffer;}
  in->buflim=buf;
  in->bufread=buf;
  unsigned char *writebuf = (unsigned char *) in->buffer;
#if HAVE_PREAD
  ssize_t result = stream_pread(s,stream_locked,writebuf,len,offset);
  if (result>=0) {
    in->buflim=in->buffer+len;
    return in;}
  else return NULL;
#else
  kno_off_t result = -1;
  if (!(stream_locked)) kno_lock_stream(s);
  kno_setpos(s,offset);
  result = kno_read_bytes(writebuf,buf,len);
  if (!(stream_locked)) kno_unlock_stream(s);
  if (result >= 0) {
    in->buflim=in->buffer+len;
    return in;}
  else return NULL;
#endif
}

KNO_EXPORT int kno_write_4bytes_at(kno_stream s,kno_4bytes w,kno_off_t off)
{
  kno_outbuf out = (off>=0) ? (kno_start_write(s,off)) : (kno_writebuf(s)) ;
  *(out->bufwrite++) = w>>24;
  *(out->bufwrite++) = ((w>>16)&0xFF);
  *(out->bufwrite++) = ((w>>8)&0xFF);
  *(out->bufwrite++) = ((w>>0)&0xFF);
  kno_flush_stream(s);
  return 4;
}

KNO_EXPORT long long kno_read_4bytes_at(kno_stream s,kno_off_t off,int locked)
{
#if HAVE_PREAD
  unsigned char bytes[4];
  int rv = pread(s->stream_fileno,bytes,4,off);
  if (rv==4) {
    struct KNO_INBUF tmp = { 0 };
    KNO_INIT_INBUF(&tmp,bytes,4,0);
    return kno_read_4bytes(&tmp);}
#endif
  if (locked==0) kno_lock_stream(s);
  struct KNO_INBUF *in = (off>=0) ? (kno_start_read(s,off)) : (kno_readbuf(s));
  if (kno_request_bytes(in,4)) {
    kno_8bytes bytes = kno_get_4bytes(in->bufread);
    in->bufread = in->bufread+4;
    if (locked==0) kno_unlock_stream(s);
    return bytes;}
  else {
    if (locked==0) kno_unlock_stream(s);
    return -1;}
}

KNO_EXPORT int kno_read_byte_at(kno_stream s,kno_off_t off,int locked)
{
#if HAVE_PREAD
  unsigned char byte;
  int rv = pread(s->stream_fileno,&byte,1,off);
  if (rv==1)
    return byte;
#endif
  if (locked==0) kno_lock_stream(s);
  struct KNO_INBUF *in = (off>=0) ? (kno_start_read(s,off)) : (kno_readbuf(s));
  if (kno_request_bytes(in,1)) {
    int byte = kno_get_byte(in->bufread);
    in->bufread = in->bufread+1;
    if (locked==0) kno_unlock_stream(s);
    return byte;}
  else {
    if (locked==0) kno_unlock_stream(s);
    return -1;}
}

KNO_EXPORT int kno_write_8bytes_at(kno_stream s,kno_8bytes w,kno_off_t off)
{
  kno_outbuf out = (off>=0) ? (kno_start_write(s,off)) : (kno_writebuf(s)) ;
  *(out->bufwrite++) = ((w>>56)&0xFF);
  *(out->bufwrite++) = ((w>>48)&0xFF);
  *(out->bufwrite++) = ((w>>40)&0xFF);
  *(out->bufwrite++) = ((w>>32)&0xFF);
  *(out->bufwrite++) = ((w>>24)&0xFF);
  *(out->bufwrite++) = ((w>>16)&0xFF);
  *(out->bufwrite++) = ((w>>8)&0xFF);
  *(out->bufwrite++) = ((w>>0)&0xFF);
  kno_flush_stream(s);
  return 8;
}

KNO_EXPORT kno_8bytes kno_read_8bytes_at(kno_stream s,kno_off_t off,int locked,int *err)
{
#if HAVE_PREAD
  unsigned char bytes[8];
  int rv = pread(s->stream_fileno,bytes,8,off);
  if (rv==8) {
    struct KNO_INBUF tmp = { 0 };
    KNO_INIT_INBUF(&tmp,bytes,8,0);
    return kno_read_8bytes(&tmp);}
#endif
  if (locked==0) kno_lock_stream(s);
  struct KNO_INBUF *in = (off>=0) ? (kno_start_read(s,off)) : (kno_readbuf(s));
  if (kno_request_bytes(in,8)) {
    kno_8bytes bytes = kno_get_8bytes(in->bufread);
    in->bufread = in->bufread+8;
    if (locked==0) kno_unlock_stream(s);
    return bytes;}
  else {
    if (err) *err = -1;
    if (locked==0) kno_unlock_stream(s);
    return 0;}
}

KNO_EXPORT
ssize_t kno_write_ints(kno_stream s,size_t len,unsigned int *words)
{
  kno_set_direction(s,kno_byteflow_write);
  /* We handle the case where we can write directly to the file */
  if (((s->stream_flags))&KNO_STREAM_CAN_SEEK) {
    int bytes_written;
    kno_off_t real_pos = kno_getpos(s);
    kno_set_direction(s,kno_byteflow_write);
    kno_setpos(s,real_pos);
#if (!(WORDS_BIGENDIAN))
    {int i = 0; while (i < len) {
        words[i]=kno_net_order(words[i]); i++;}}
#endif
    bytes_written = writeall(s->stream_fileno,(unsigned char *)words,len*4);
#if (!(WORDS_BIGENDIAN))
    {int i = 0; while (i < len) {
        words[i]=kno_host_order(words[i]); i++;}}
#endif
    kno_setpos(s,real_pos+4*len);
    return bytes_written;}
  else {
    kno_outbuf out = kno_writebuf(s);
    int i = 0; while (i<len) {
      int word = words[i++];
      kno_write_4bytes(out,word);}
    return len*4;}
}

KNO_EXPORT
ssize_t kno_stream_write(kno_stream s,size_t len,unsigned char *bytes)
{
  kno_set_direction(s,kno_byteflow_write);
  /* We handle the case where we can write directly to the file */
  if (((s->stream_flags))&KNO_STREAM_CAN_SEEK) {
    int bytes_written;
    kno_off_t real_pos = kno_getpos(s);
    kno_set_direction(s,kno_byteflow_write);
    kno_setpos(s,real_pos);
    bytes_written = writeall(s->stream_fileno,bytes,len);
    kno_setpos(s,real_pos+len);
    return bytes_written;}
  else {
    kno_outbuf out = kno_writebuf(s);
    return kno_write_bytes(out,bytes,len);}
}

KNO_EXPORT
ssize_t kno_read_ints(kno_stream s,size_t len,unsigned int *words)
{
  /* This is special because we ignore the buffer if we can. */
  if ((s->stream_flags)&KNO_STREAM_CAN_SEEK) {
    /* real_pos is the file position plus any data buffered for output
       (or minus any data buffered for input) */
    kno_off_t real_pos = kno_getpos(s);
    int bytes_read = 0, bytes_needed = len*4;
    /* This will flush any pending write data */
    kno_set_direction(s,kno_byteflow_read);
    kno_setpos(s,real_pos);
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
        words[i]=kno_host_order(words[i]); i++;}}
#endif
    return bytes_read;}
  else if (kno_request_bytes(kno_readbuf(s),len*4)) {
    struct KNO_INBUF *in = kno_readbuf(s);
    int i = 0; while (i<len) {
      int word = kno_read_4bytes(in);
      words[i++]=word;}
    return len*4;}
  else return -1;
}

KNO_EXPORT
ssize_t kno_stream_read(kno_stream s,size_t len,unsigned char *bytes)
{
  /* This is special because we ignore the buffer if we can. */
  if ((s->stream_flags)&KNO_STREAM_CAN_SEEK) {
    /* real_pos is the file position plus any data buffered for output
       (or minus any data buffered for input) */
    kno_off_t real_pos = kno_getpos(s);
    int bytes_read = 0, bytes_needed = len;
    /* This will flush any pending write data */
    kno_set_direction(s,kno_byteflow_read);
    kno_setpos(s,real_pos);
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
  else if (kno_request_bytes(kno_readbuf(s),len*4)) {
    struct KNO_INBUF *in = kno_readbuf(s);
    return kno_read_bytes(bytes,in,len);}
  else return -1;
}

/* Reading chunk refs */

typedef long long int ll;
typedef unsigned long long ull;

KNO_EXPORT KNO_CHUNK_REF
kno_fetch_chunk_ref(struct KNO_STREAM *stream,
                    kno_off_t base,
                    kno_offset_type offtype,
                    unsigned int offset,
                    int locked)
{
  KNO_CHUNK_REF result={-1,-1};
  int chunk_size = kno_chunk_ref_size(offtype);
  kno_off_t ref_off = offset*chunk_size;
#if HAVE_PREAD
  struct KNO_INBUF _in, *in = &_in;
  unsigned char buf[16];
  memset(&_in,0,sizeof(_in));
  KNO_INIT_BYTE_INPUT(&_in,buf,chunk_size);
  if (kno_read_block(stream,buf,chunk_size,base+ref_off,1)!=chunk_size) {
    u8_logf(LOG_CRIT,"BlockReadFailed",
            "Reading %_d-byte block from stream %s failed",
            chunk_size,stream->streamid);
    u8_seterr("Block read failed","fetch_chunk_ref",u8_strdup(stream->streamid));
    return result;}
  else switch (offtype) {
    case KNO_B32:
      result.off = kno_read_4bytes(in);
      result.size = kno_read_4bytes(in);
      break;
    case KNO_B40: {
      unsigned int word1, word2;
      word1 = kno_read_4bytes(in);
      word2 = kno_read_4bytes(in);
      result.off = ((((ll)((word2)&(0xFF000000)))<<8)|word1);
      result.size = (ll)((word2)&(0x00FFFFFF));}
      break;
    case KNO_B64: {
      unsigned int word1, word2;
      word1 = kno_read_4bytes(in);
      word2 = kno_read_4bytes(in);
      result.off = (kno_off_t) ((((kno_ll)word1)<<32)|(((kno_ll)word2)));
      result.size = kno_read_4bytes(in);
      break;}
    default:
      u8_logf(LOG_CRIT,"InvalidOffsetType",
              "Invalid offset type 0x%x for data stream %s",
              offtype,stream->streamid);
      u8_seterr("Invalid Offset type","read_chunk_ref",NULL);
      result.off = -1;
      result.size = -1;} /* switch (p->kb_offtype) */
#else
  if (!(locked)) kno_lock_stream(stream);
  if ( (kno_setpos(stream,base+ref_off)) < 0 ) {
    result.off = (kno_off_t)-1; result.size = (size_t)-1;}
  else {
    kno_inbuf in = kno_readbuf(stream);
    switch (offtype) {
    case KNO_B32:
      result.off = kno_read_4bytes(in);
      result.size = kno_read_4bytes(in);
      break;
    case KNO_B40: {
      unsigned int word1, word2;
      word1 = kno_read_4bytes(in);
      word2 = kno_read_4bytes(in);
      result.off = ((((ll)((word2)&(0xFF000000)))<<8)|word1);
      result.size = (ll)((word2)&(0x00FFFFFF));
      break;}
    case KNO_B64:
      unsigned int word1, word2;
      word1 = kno_read_4bytes(in);
      word2 = kno_read_4bytes(in);
      result.off = (kno_off_t) ((((kno_ll)word1)<<32)|(((kno_ll)word2)));
      result.size = kno_read_4bytes(in);
      break;
    default:
      u8_logf(LOG_CRIT,"InvalidOffsetType",
              "Invalid offset type 0x%x for data stream %s",
              offtype,stream->streamid);
      u8_seterr("Invalid Offset type","read_chunk_ref",NULL);
      result.off = -1;
      result.size = -1;} /* switch (p->kb_offtype) */
  }
  if (!(locked)) kno_unlock_stream(stream);
#endif
  return result;
}


/* Stream ctl */

KNO_EXPORT
lispval kno_streamctl_x(kno_stream s,kno_streamop op,void *data)
{
  switch (op) {
  case kno_stream_close:
    kno_close_stream(s,0);
    return VOID;
  case kno_stream_lockfile: {
    int rv = lock_filestream(s);
    if (rv<0) return KNO_ERROR;
    else if (rv) return KNO_TRUE;
    else return KNO_FALSE;}
  case kno_stream_unlockfile: {
    int rv = unlock_filestream(s);
    if (rv<0) return KNO_ERROR;
    else if (rv) return KNO_TRUE;
    else return KNO_FALSE;}
  case kno_stream_setread: {
    int rv = kno_set_direction(s,kno_byteflow_read);
    if (rv<0) return KNO_ERROR;
    else if (rv) return KNO_TRUE;
    else return KNO_FALSE;}
  case kno_stream_setwrite: {
    int rv = kno_set_direction(s,kno_byteflow_write);
    if (rv<0) return KNO_ERROR;
    else if (rv) return KNO_TRUE;
    else return KNO_FALSE;}
  case kno_stream_setbuf:
    kno_setbufsize(s,(ssize_t)data);
    return VOID;
  case kno_stream_mmap: {
    kno_ptrval enable = (kno_ptrval) data;
    // Only read-only streams are mmapped for now
    if (!(U8_BITP(s->stream_flags,KNO_STREAM_READ_ONLY) )) enable=0;
    if ( ( (enable) && (U8_BITP(s->stream_flags,KNO_STREAM_MMAPPED)) ) ||
         ( (!enable) && (!(U8_BITP(s->stream_flags,KNO_STREAM_MMAPPED))) ) )
      return KNO_FALSE;
    else if (enable) {
      ssize_t rv=kno_setbufsize(s,-1);
      if (rv<0) return KNO_ERROR;
      else return KNO_INT(rv);}
    else {
      ssize_t rv=kno_setbufsize(s,kno_stream_bufsize);
      if (rv<0) return KNO_ERROR;
      else return KNO_INT(rv);}}
  default:
    kno_seterr("Unhandled Operation","kno_streamctl",s->streamid,VOID);
    return KNO_ERROR;}
}

KNO_EXPORT long long kno_streamctl(kno_stream s,kno_streamop op,void *data)
{
  lispval v = kno_streamctl_x(s,op,data);
  if (KNO_ABORTP(v))
    return -1;
  else if ( (KNO_FALSEP(v)) || (KNO_EMPTYP(v)) )
    return 0;
  else if (KNO_TRUEP(v))
    return 1;
  else if (KNO_FIXNUMP(v))
    return kno_getint(v);
  else {
    kno_decref(v);
    return 1;}
}

/* Files 2 dtypes */

KNO_EXPORT lispval kno_read_dtype_from_file(u8_string filename)
{
  if (u8_file_existsp(filename)) {
    ssize_t filesize = u8_file_size(filename);
    if (filesize<0) {
      kno_seterr(kno_FileNotFound,"kno_read_dtype_from_file",filename,VOID);
      return KNO_ERROR;}
    else if (filesize == 0) {
      kno_seterr("Zero-length file","kno_read_dtype_from_file",filename,VOID);
      return KNO_ERROR;}
    else {
      struct KNO_STREAM *stream = u8_alloc(struct KNO_STREAM);
      struct KNO_STREAM *opened=
	kno_init_file_stream(stream,filename,
			     KNO_FILE_READ,
			     KNO_STREAM_IS_CONSED|
			     KNO_STREAM_READ_ONLY|
			     ( (KNO_USE_MMAP) ? (KNO_STREAM_MMAPPED) : (0)),
			     kno_filestream_bufsize);
      if (opened) {
	lispval result = VOID;
	struct KNO_INBUF *in = kno_readbuf(opened);
	int byte1 = kno_probe_byte(in);
	int zip = (byte1>=0x80);
	if (zip)
	  result = kno_zread_dtype(in);
	else if (byte1 == dt_ztype) {
	  kno_read_byte(in);
	  result = kno_zread_dtype(in);}
	else result = kno_read_dtype(in);
	kno_free_stream(opened);
	if ( (ABORTED(result)) && (u8_current_exception==NULL))
	  kno_seterr("UndeclaredError","kno_read_dtype_from_file",filename,VOID);
	return result;}
      else {
	u8_free(stream);
	return KNO_ERROR;}}}
  else if (strstr(filename,".zip/")) {
    ssize_t n_bytes = -1;
    u8_string bytes = kno_get_source(filename,"bytes",NULL,NULL,&n_bytes);
    if (bytes) {
      struct KNO_INBUF inbuf;
      KNO_INIT_INBUF(&inbuf,bytes,n_bytes,0);
      lispval v = kno_read_dtype(&inbuf);
      u8_free(bytes);
      return v;}
    else return KNO_ERROR;}
  else {
    kno_seterr(kno_FileNotFound,"kno_read_dtype_from_file",filename,VOID);
    return KNO_ERROR;}
}

KNO_EXPORT ssize_t kno_lisp2file(lispval object, u8_string filename,
                                 ssize_t bufsize,int zip)
{
  struct KNO_STREAM *stream = u8_alloc(struct KNO_STREAM);
  struct KNO_STREAM *opened=
    kno_init_file_stream(stream,filename,
                         KNO_FILE_WRITE,
                         KNO_STREAM_IS_CONSED,
                         bufsize);
  if (opened) {
    size_t len = (zip)?
      (kno_zwrite_dtype(kno_writebuf(opened),object)):
      (kno_write_dtype(kno_writebuf(opened),object));
    kno_free_stream(opened);
    return len;}
  else return -1;
}

KNO_EXPORT ssize_t kno_write_dtype_to_file(lispval object,u8_string filename)
{
  return kno_lisp2file(object,filename,1024*64,0);
}

KNO_EXPORT ssize_t kno_write_zdtype_to_file(lispval object,u8_string filename)
{
  return kno_lisp2file(object,filename,1024*64,1);
}

/* Initialization of file */

KNO_EXPORT void kno_init_streams_c()
{
  pagesize = u8_getpagesize();

  kno_unparsers[kno_stream_type]=unparse_stream;
  kno_recyclers[kno_stream_type]=recycle_stream;

  kno_tablefns[kno_stream_type]=kno_annotated_tablefns;

  kno_register_config("STREAMS:LOGLEVEL",_("LOGLEVEL for binary streams"),
                      kno_intconfig_get,kno_loglevelconfig_set,
                      &stream_loglevel);

  u8_register_source_file(_FILEINFO);
}

