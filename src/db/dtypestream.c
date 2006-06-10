/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_DTYPEIO 1

#include "fdb/dtype.h"
#include "fdb/dtypestream.h"

#include <libu8/filefns.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#if HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#if ((FD_LARGEFILES_ENABLED) && (defined(O_LARGEFILE)))
#define POSIX_OPEN_FLAGS O_LARGEFILE
#else
#define POSIX_OPEN_FLAGS 0
#endif


fd_exception fd_ReadOnlyStream=_("Read-only stream");
fd_exception fd_CantRead=_("Can't read data");
fd_exception fd_CantWrite=_("Can't write data");
fd_exception fd_BadSeek=_("Bad seek argument");
fd_exception fd_BadLSEEK=_("lseek() failed");
fd_exception fd_OverSeek=_("Seeking past end of file");
fd_exception fd_UnderSeek=_("Seeking before the beginning of the file");

static int fill_dtype_stream(struct FD_DTYPE_STREAM *df,int n);

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

/* Initialization functions */

FD_EXPORT void fd_init_dtype_stream
  (struct FD_DTYPE_STREAM *s,int sock,int bufsiz,
   FD_MEMORY_POOL_TYPE *bufpool,
   FD_MEMORY_POOL_TYPE *conspool)
{
  unsigned char *buf=u8_pmalloc(bufpool,bufsiz);
  /* If you can't get a whole buffer, try smaller */
  while ((bufsiz>=1024) && (buf==NULL)) {
    bufsiz=bufsiz/2; buf=u8_pmalloc(bufpool,bufsiz);}
  if (buf==NULL) bufsiz=0;
  s->mallocd=0; s->fd=sock; s->filepos=-1; s->maxpos=-1; s->id=NULL;
  FD_INIT_BYTE_INPUT((fd_byte_input)s,buf,bufsiz);
  s->end=s->start; /* Initialize to not having anything to read */
  s->mpool=bufpool; s->conspool=conspool; /* Initialize memory pools */
  /* Initialize the on-demand reader */
  s->fillfn=fill_dtype_stream;
  s->bufsiz=bufsiz; s->bits=FD_DTSTREAM_READING;
}

FD_EXPORT fd_dtype_stream fd_init_dtype_file_stream
   (struct FD_DTYPE_STREAM *stream,
    u8_string fname,fd_dtstream_mode mode,int bufsiz,
    FD_MEMORY_POOL_TYPE *bufpool,
    FD_MEMORY_POOL_TYPE *conspool)
 {
  int fd, flags=POSIX_OPEN_FLAGS, lock, reading=0, writing=0;
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
  fd=open(localname,flags,0666); stream->bits=0;
  /* If we fail and we're modifying, try to open read-only */
  if ((fd<0) && (mode == FD_DTSTREAM_MODIFY)) {
    fd=open(localname,O_RDWR,0666);
    if (fd>0) writing=0;}
  if (fd>0) {
    fd_init_dtype_stream(stream,fd,bufsiz,bufpool,conspool);
    stream->mallocd=1; stream->id=u8_strdup(fname);
    stream->bits=stream->bits|FD_DTSTREAM_CANSEEK|FD_DTSTREAM_NEEDS_LOCK;
    if (writing == 0) stream->bits=stream->bits|FD_DTSTREAM_READ_ONLY;
    stream->maxpos=lseek(fd,0,SEEK_END);
    stream->filepos=lseek(fd,0,SEEK_SET); 
    u8_free(localname);
    return stream;}
  else {
    fd_seterr3(fd_CantOpenFile,"fd_init_dtype_file_stream",localname);
    return NULL;}
}

FD_EXPORT fd_dtype_stream fd_open_dtype_file_x
   (u8_string fname,fd_dtstream_mode mode,int bufsiz,
    FD_MEMORY_POOL_TYPE *bufpool,
    FD_MEMORY_POOL_TYPE *conspool)
{
  struct FD_DTYPE_STREAM *stream=
    u8_malloc(sizeof(struct FD_DTYPE_STREAM));
  struct FD_DTYPE_STREAM *opened=
    fd_init_dtype_file_stream(stream,fname,mode,bufsiz,bufpool,conspool);
  if (opened) return opened;
  else {
    u8_free(stream);
    return NULL;}
}

FD_EXPORT void fd_dtsclose(fd_dtype_stream s,int close_fd)
{
  if ((s->bits&FD_DTSTREAM_READING) == 0) fd_dtsflush(s);
  u8_pfree_x(s->mpool,s->start,s->bufsiz);
  if (close_fd>0)
    if (s->bits&FD_DTSTREAM_SOCKET)
      shutdown(s->fd,SHUT_RDWR);
    else close(s->fd);
  s->fd=-1;
  if (s->id)
    u8_pfree_x(s->mpool,s->id,strlen(s->id));
  if (s->mallocd)
    u8_pfree_x(s->mpool,s,sizef(struct FD_DTYPE_STREAM *));
}

FD_EXPORT void fd_dtsbufsize(fd_dtype_stream s,int bufsiz)
{
  fd_dtsflush(s);
  {
    unsigned int ptroff=s->ptr-s->start, endoff=s->end-s->start;
    s->start=u8_realloc(s->start,bufsiz);
    s->ptr=s->start+ptroff; s->end=s->start+endoff;
  }
}

FD_EXPORT fdtype fd_dtsread_dtype(fd_dtype_stream s)
{
  if ((s->bits&FD_DTSTREAM_READING) == 0)
    if (fd_set_read(s,1)<0) return fd_erreify();
  return fd_read_dtype((struct FD_BYTE_INPUT *)s,s->conspool);
}
FD_EXPORT int fd_dtswrite_dtype(fd_dtype_stream s,fdtype x)
{
  int n_bytes;
  if (s->bits&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  n_bytes=fd_write_dtype((struct FD_BYTE_OUTPUT *)s,x);
  if ((s->ptr-s->start)*4>=(s->bufsiz*3)) fd_dtsflush(s);
  return n_bytes;
}

/* Structure functions */

static int fill_dtype_stream(struct FD_DTYPE_STREAM *df,int n)
{
  int n_buffered, n_to_read, read_request, bytes_read=0;
  /* Shrink what you've already read, adjusting the filepos */
  if (df->filepos>=0) df->filepos=df->filepos+(df->ptr-df->start);
  memmove(df->start,df->ptr,(df->end-df->ptr));
  df->end=(df->start)+(df->end-df->ptr);
  df->ptr=df->start;
  /* Make sure that there's enough space */
  if (n>df->bufsiz) {
    int new_size=df->bufsiz;
    unsigned char *newbuf;
    unsigned int end_pos=df->end-df->start;
    unsigned int ptr_pos=df->ptr-df->start;
    while (new_size<n)
      if (new_size>=0x40000) new_size=new_size+0x40000;
      else new_size=new_size*2;
    newbuf=u8_prealloc(df->mpool,df->start,new_size);
    df->start=newbuf; df->ptr=newbuf+ptr_pos; df->end=newbuf+end_pos;
    df->bufsiz=new_size;}
  n_buffered=df->end-df->start;
  n_to_read=n-n_buffered;
  read_request=df->bufsiz-n_buffered;
  while (bytes_read < n_to_read) {
    int delta;
    if ((delta=read(df->fd,df->end,read_request-bytes_read))==0) break;
    if ((delta<0) && (errno) && (errno != EWOULDBLOCK)) {
      
      fd_seterr3(u8_strerror(errno),"fill_dtype_stream",
		 ((df->id) ?(u8_strdup(df->id)):(NULL)));
      return 0;}
    df->end=df->end+delta;
    bytes_read=bytes_read+delta;}
  return bytes_read;
}

FD_EXPORT void fd_dtsflush(fd_dtype_stream s)
{
  if (s->bits&FD_DTSTREAM_READING) {
    /* When flushing a read stream, we just discard whatever
       is in the input buffer. */
    if ((s->bits&FD_DTSTREAM_CANSEEK) && (s->filepos>=0))
      /* Advance the virtual file pointer to reflect where
	 the file pointer really is. */
      s->filepos=s->filepos+(s->end-s->start);
    /* Reset the buffer pointers */
    s->end=s->ptr=s->start;}
  else {
    int bytes_written=0, bytes_to_write=s->ptr-s->start;
    unsigned char *bytes=s->start;
    while (bytes_written<bytes_to_write) {
      int delta=write(s->fd,bytes+bytes_written,
		      bytes_to_write-bytes_written);
      bytes_written=bytes_written+delta;}
    if ((s->bits)&FD_DTSTREAM_DOSYNC) fsync(s->fd);
    if ((s->bits&FD_DTSTREAM_CANSEEK) && (s->filepos>=0))
      s->filepos=s->filepos+bytes_written;
    /* Reset maxpos if neccessary. */
    if ((s->maxpos>=0) && (s->filepos>s->maxpos))
      s->maxpos=s->filepos;
    /* Reset the buffer pointers */
    s->ptr=s->start;}
}

FD_EXPORT int fd_dtslock(fd_dtype_stream s)
{
  if (s->bits&FD_DTSTREAM_LOCKED) return 1;
  else if (fd_lock_fd(s->fd,1)) {
    s->bits=s->bits|FD_DTSTREAM_LOCKED;
    return 1;}
  else return 0;
}

FD_EXPORT int fd_dtsunlock(fd_dtype_stream s)
{
  if (!(s->bits&FD_DTSTREAM_LOCKED)) return 1;
  else if (fd_unlock_fd(s->fd)) {
    s->bits=s->bits&(~FD_DTSTREAM_LOCKED);
    return 1;}
  else return 0;
}

FD_EXPORT int fd_set_read(fd_dtype_stream s,int read)
{
  if (s->bits&FD_DTSTREAM_READ_ONLY)
    if (read==0) {
      fd_seterr(fd_ReadOnlyStream,"fd_set_read",u8_strdup(s->id),FD_VOID);
      return -1;}
    else return 1;
  else if (s->bits&FD_DTSTREAM_READING) 
    if (read) return 1;
    else {
      /* Lock the file descriptor if we need to. */
      if (s->bits&FD_DTSTREAM_NEEDS_LOCK)
	if (fd_lock_fd(s->fd,1)) {
	  s->bits=s->bits|FD_DTSTREAM_LOCKED;}
	else return 0;
      if ((s->bits&FD_DTSTREAM_CANSEEK) && (s->filepos>=0))
	/* We reset the virtual filepos to match the real filepos
	   based on how much we still had to read in the buffer. */
	s->filepos=s->filepos+(s->end-s->start);
      /* If we were reading, in order to start writing, we need
	 to reset the pointer and make the ->end point to the
	 end of the allocated buffer. */
      s->ptr=s->start; s->end=s->start+s->bufsiz;
      /* Now we clear the bit */
      s->bits=s->bits&(~FD_DTSTREAM_READING);
      return 1;}
  else if (read == 0) return 1;
  else {
    /* If we were writing, in order to start reading, we need
       to flush what is buffered to the output and collapse all
       of the pointers into the start. We also need to update bufsiz
       in case the output buffer grew while we were writing. */
    fd_dtsflush(s);
    /* Now we reset bufsiz in case we grew the buffer */
    s->bufsiz=s->end-s->start;
    /* Finally, we reset the pointers */
    s->end=s->ptr=s->start;
    /* And set the reading bit */
    s->bits=s->bits|FD_DTSTREAM_READING;
    return 1;}
}

/* This gets the position when it isn't cached on the stream. */
FD_EXPORT off_t _fd_getpos(fd_dtype_stream s)
{
  if ((s->bits&FD_DTSTREAM_CANSEEK) == 0) return -1;
  else if (s->bits&FD_DTSTREAM_READING) {
    off_t current=lseek(s->fd,0,SEEK_CUR);
    s->filepos=current-(s->end-s->start);
    return s->filepos+(s->ptr-s->start);}
  else {
    off_t current=lseek(s->fd,0,SEEK_CUR);
    s->filepos=current;
    return s->filepos+(s->ptr-s->start);}
}
FD_EXPORT off_t fd_setpos(fd_dtype_stream s,off_t pos)
{
  /* This is optimized for the case where the new position is
     in the range we have buffered. */
  if ((s->bits&FD_DTSTREAM_CANSEEK) == 0) return -1;
  else if (pos<0) 
    return fd_reterr(fd_BadSeek,"fd_setpos",
		     ((s->id)?(u8_strdup(s->id)):(NULL)),
		     FD_INT2DTYPE(pos));
  else if (s->filepos<0) {
    /* We're not tracking filepos, so we'll check by hand. */
    off_t maxpos;
    fd_dtsflush(s);
    maxpos=lseek(s->fd,0,SEEK_END);
    if (maxpos<0)
      return fd_reterr(fd_BadLSEEK,"fd_setpos",
		       ((s->id)?(u8_strdup(s->id)):(NULL)),
		       FD_INT2DTYPE(pos));
    else if (pos<maxpos)
      return lseek(s->fd,pos,SEEK_SET);
    else return fd_reterr(fd_OverSeek,"fd_setpos",
			  ((s->id)?(u8_strdup(s->id)):(NULL)),
			  FD_INT2DTYPE(pos));}
  else if ((s->filepos>=0) && (pos>s->maxpos)) {
    /* We just sought out of bounds.  */
    fd_dtsflush(s); /* Reset the buffer pointers. */
    if (pos<s->maxpos) /* OK if that made us legal. */
      return (s->filepos=(lseek(s->fd,pos,SEEK_SET)));
    /* Check the disk file for real size info */
    else s->filepos=s->maxpos=lseek(s->fd,0,SEEK_END);
    /* If we're legal now (someone else wrote to the file),
       go ahead and seek, otherwise return -1. */
    if (pos<=s->maxpos)
      return (s->filepos=(lseek(s->fd,pos,SEEK_SET)));
    else return fd_reterr(fd_OverSeek,"fd_setpos",
			  ((s->id)?(u8_strdup(s->id)):(NULL)),
			  FD_INT2DTYPE(pos));}
  else if (s->bits&FD_DTSTREAM_READING)
    if ((pos>s->filepos) && ((pos-s->filepos)<(s->end-s->start))) {
      /* Here's the case where we are seeking within the buffer. */
      s->ptr=s->start+(pos-s->filepos);
      return pos;}
  fd_dtsflush(s);
  return (s->filepos=(lseek(s->fd,pos,SEEK_SET)));
}
FD_EXPORT off_t fd_movepos(fd_dtype_stream s,off_t delta)
{
  if ((s->bits&FD_DTSTREAM_CANSEEK) == 0) return -1;
  else if (s->filepos<0) {
    /* We're not tracking filepos, so we'll check by hand. */
    off_t pos, maxpos;
    fd_dtsflush(s);
    pos=lseek(s->fd,0,SEEK_CUR);
    maxpos=lseek(s->fd,0,SEEK_END);
    if ((pos<0)||(maxpos<0))
      return fd_reterr(fd_BadLSEEK,"fd_movepos",
		       ((s->id)?(u8_strdup(s->id)):(NULL)),
		       FD_INT2DTYPE(delta));
    else if ((pos+delta)<0)
      return fd_reterr(fd_UnderSeek,"fd_setpos",
		       ((s->id)?(u8_strdup(s->id)):(NULL)),
		       FD_INT2DTYPE(pos));
    else if ((pos+delta)<maxpos)
      return lseek(s->fd,(pos+delta),SEEK_SET);
    else return fd_reterr(fd_OverSeek,"fd_setpos",
			  ((s->id)?(u8_strdup(s->id)):(NULL)),
			  FD_INT2DTYPE(pos));}
  else if ((s->filepos+delta)>s->maxpos) {
    int pos=s->filepos+delta;
    /* We just sought out of bounds.  */
    fd_dtsflush(s);
    /* Reset the buffer pointers, this flushes output to disk
       and may update s->maxpos (if it went at the end of the file). */
    if (pos<s->maxpos) /* OK if that made us legal. */
      return (s->filepos=(lseek(s->fd,pos,SEEK_SET)));
    /* Check the disk file for real size info */
    else s->filepos=s->maxpos=lseek(s->fd,0,SEEK_END);
    /* If we're legal now (someone else wrote to the file),
       go ahead and seek, otherwise return -1. */
    if (pos<=s->maxpos) return (s->filepos=(lseek(s->fd,pos,SEEK_SET)));
    else return fd_reterr(fd_OverSeek,"fd_setpos",
			  ((s->id)?(u8_strdup(s->id)):(NULL)),
			  FD_INT2DTYPE(pos));}
  else if (s->bits&FD_DTSTREAM_READING) {
    unsigned char *relpos=s->ptr+delta;
    if ((relpos>s->start) && (relpos<s->end)) {
      s->ptr=relpos; return s->filepos+(s->ptr-s->start);}}
  fd_dtsflush(s);
  return (s->filepos=(lseek(s->fd,delta,SEEK_CUR)));
}
FD_EXPORT off_t fd_endpos(fd_dtype_stream s)
{
  if ((s->bits&FD_DTSTREAM_CANSEEK) == 0) return -1;
  fd_dtsflush(s);
  return (s->maxpos=s->filepos=(lseek(s->fd,0,SEEK_END)));
}

/* Input functions */

FD_EXPORT int _fd_dtsread_byte(fd_dtype_stream s)
{
  if ((s->bits&FD_DTSTREAM_READING) == 0)
    if (fd_set_read(s,1)<0) -1;
  if (fd_needs_bytes((fd_byte_input)s,1)) 
    return (*(s->ptr++));
  else return -1;
}

FD_EXPORT unsigned int _fd_dtsread_4bytes(fd_dtype_stream s)
{
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->ptr);
    s->ptr=s->ptr+4;
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
    memcpy(bytes,s->ptr,len);
    s->ptr=s->ptr+len;
    return len;}
  else {
    int n_buffered=s->ptr-s->start;
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

FD_EXPORT off_t _fd_dtsread_off_t(fd_dtype_stream s)
{
  fd_dts_start_read(s);
  if (fd_needs_bytes((fd_byte_input)s,4)) {
    unsigned int bytes=fd_get_4bytes(s->ptr);
    s->ptr=s->ptr+4;
    return (off_t) bytes;}
  else return ((off_t)(-1));
}

FD_EXPORT int fd_dtsread_ints(fd_dtype_stream s,int len,unsigned int *words)
{
  if ((s->bits&FD_DTSTREAM_READING) == 0)
    if (fd_set_read(s,1)<0) return -1;
  /* This is special because we ignore the buffer if we can. */
  if (s->bits&FD_DTSTREAM_CANSEEK) {
    off_t real_pos=fd_getpos(s);
    lseek(s->fd,real_pos,SEEK_SET);
    read(s->fd,words,len*4);
#if (!(WORDS_BIGENDIAN))
    {int i=0; while (i < len) {
      words[i]=fd_host_order(words[i]); i++;}}
#endif
    s->filepos=real_pos+len*4;
    /* Reset the buffer */
    s->ptr=s->end=s->start;
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
  if (s->bits&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;  
  if (s->ptr>=s->end) fd_dtsflush(s);
  *(s->ptr++)=b;
  return 1;
}
FD_EXPORT int _fd_dtswrite_4bytes(fd_dtype_stream s,unsigned int w)
{
  if (s->bits&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;  
  if (s->ptr+4>=s->end) fd_dtsflush(s);
  *(s->ptr++)=w>>24;
  *(s->ptr++)=((w>>16)&0xFF);
  *(s->ptr++)=((w>>8)&0xFF);
  *(s->ptr++)=((w>>0)&0xFF);
  return 4;
}

FD_EXPORT int _fd_dtswrite_bytes
  (fd_dtype_stream s,unsigned char *bytes,int n)
{
  if (s->bits&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;  
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

FD_EXPORT int fd_dtswrite_ints(fd_dtype_stream s,int len,unsigned int *words)
{
  if (s->bits&FD_DTSTREAM_READING)
    if (fd_set_read(s,0)<0) return -1;
  /* This is special because we ignore the buffer if we can. */
  if (s->bits&FD_DTSTREAM_CANSEEK) {
    off_t real_pos=fd_getpos(s);
    lseek(s->fd,real_pos,SEEK_SET);
#if (!(WORDS_BIGENDIAN))
    {int i=0; while (i < len) {
	words[i]=fd_net_order(words[i]); i++;}}
#endif
    write(s->fd,words,len*4);
#if (!(WORDS_BIGENDIAN))
    {int i=0; while (i < len) {
	words[i]=fd_host_order(words[i]); i++;}}
#endif
    return len*4;}
  else {
    int i=0; while (i<len) {
      int word=words[i++];
      fd_dtswrite_4bytes(s,word);}
    return len*4;}
}

FD_EXPORT int fd_init_dtypestream_c()
{
  fd_register_source_file(versionid);
}


/* The CVS log for this file
   $Log: dtypestream.c,v $
   Revision 1.44  2006/01/31 13:47:23  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.43  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.42  2006/01/07 23:12:46  haase
   Moved framerd object dtype handling into the main fd_read_dtype core, which led to substantial performanc improvements

   Revision 1.41  2006/01/07 14:01:13  haase
   Fixed some leaks

   Revision 1.40  2006/01/05 18:20:33  haase
   Added missing return keyword

   Revision 1.39  2006/01/05 18:03:58  haase
   Made fd_setpos and fd_movepos require that their location be within the file

   Revision 1.38  2005/12/19 00:44:19  haase
   Made stream closing always free the associated id string

   Revision 1.37  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.36  2005/08/08 16:23:58  haase
   Added FD_DTSTREAM_WRITE mode for non-truncating creation

   Revision 1.35  2005/06/17 01:02:47  haase
   Added O_LARGEFILE conditionalization

   Revision 1.34  2005/06/04 18:42:29  haase
   Made dtsread_ints reset the buffer

   Revision 1.33  2005/05/26 11:03:21  haase
   Fixed some bugs with read-only pools and indices

   Revision 1.32  2005/05/23 00:53:24  haase
   Fixes to header ordering to get off_t consistently defined

   Revision 1.31  2005/05/22 20:30:03  haase
   Pass initialization errors out of config-def! and other error improvements

   Revision 1.30  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.29  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.28  2005/04/12 20:41:58  haase
   Added flag to control syncing

   Revision 1.27  2005/04/06 18:31:51  haase
   Fixed mmap error calls to produce warnings rather than raising errors and to use u8_strerror to get the condition name

   Revision 1.26  2005/03/31 16:27:34  haase
   Provide headers for u8_localpath

   Revision 1.25  2005/03/31 01:08:36  haase
   Use u8_localpath when opening dtypestreams

   Revision 1.24  2005/03/30 15:30:00  haase
   Made calls to new seterr do appropriate strdups

   Revision 1.23  2005/03/29 04:12:36  haase
   Added pool/index making primitives

   Revision 1.22  2005/03/25 19:49:47  haase
   Removed base library for eframerd, deferring to libu8

   Revision 1.21  2005/03/24 17:16:16  haase
   Made dtsclose use shutdown for sockets

   Revision 1.20  2005/03/07 19:22:13  haase
   Add commented out dtype size checking code

   Revision 1.19  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.18  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.17  2005/02/27 03:02:11  haase
   Fix filepos updates

   Revision 1.16  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
