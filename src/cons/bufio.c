/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dtypeio.h"
#include <errno.h>

#ifndef FD_DEBUG_BUFIO
#define FD_DEBUG_BUFIO 0
#endif

fd_exception fd_IsWriteBuf=_("Reading from a write buffer");
fd_exception fd_IsReadBuf=_("Writing to a read buffer");

static fd_exception BadUnReadByte=_("Inconsistent read/unread byte");

/* Byte output */

FD_EXPORT int fd_isreadbuf(struct FD_OUTBUF *b)
{
  u8_log(LOGCRIT,fd_IsReadBuf,
         "Trying to write to an input buffer 0x%llx",
         (unsigned long long)b);
  u8_seterr(fd_IsReadBuf,NULL,NULL);
  return -1;
}

FD_EXPORT int fd_iswritebuf(struct FD_INBUF *b)
{
  u8_log(LOGCRIT,fd_IsWriteBuf,
         "Trying to read from an output buffer 0x%llx",
         (unsigned long long)b);
  u8_seterr(fd_IsWriteBuf,NULL,NULL);
  return -1;
}

FD_EXPORT fdtype fdt_isreadbuf(struct FD_OUTBUF *b)
{
  u8_log(LOGCRIT,"WriteToRead",
         "Trying to write to an input buffer 0x%llx",
         (unsigned long long)b);
  u8_seterr(fd_IsReadBuf,"ReturningDType",NULL);
  return FD_ERROR_VALUE;
}

FD_EXPORT fdtype fdt_iswritebuf(struct FD_INBUF *b)
{
  u8_log(LOGCRIT,fd_IsWriteBuf,
         "Trying to read from an output buffer 0x%llx",
         (unsigned long long)b);
  u8_seterr(fd_IsWriteBuf,"ReturningDType",NULL);
  return FD_ERROR_VALUE;
}

/* Closing (freeing) buffers */

FD_EXPORT size_t _fd_raw_closebuf(struct FD_RAWBUF *buf)
{
  if (buf->buf_flags&FD_BUFFER_IS_MALLOCD) {
    u8_free(buf->buffer);
    return buf->buflen;}
  else return 0;
}

/* Growing buffers */

static int grow_output_buffer(struct FD_OUTBUF *b,size_t delta)
{
  size_t current_size=b->bufwrite-b->buffer;
  size_t current_limit=b->buflim-b->buffer;
  size_t new_limit=current_limit;
  size_t need_size=current_size+delta;
  unsigned char *new;
  if ((b->buf_flushfn)&&(!(b->buf_flags&FD_BUFFER_NO_FLUSH))) {
    ssize_t result=b->buf_flushfn(b,b->buf_data);
    if (result<0) {
      u8_log(LOGWARN,"WriteFailed",
	     "Can't flush output to file");
      return result;}
    /* See if that fixed it */
    else if (b->bufwrite+delta<b->buflim) 
      return 1;
    else {/* Go ahead and grow the buffer */}}
  if (new_limit<=0) new_limit=1000;
  while (new_limit < need_size)
    if (new_limit>=250000) new_limit=new_limit+250000;
    else new_limit=new_limit*2;
  if ((b->buf_flags)&(FD_BUFFER_IS_MALLOCD))
    new=u8_realloc(b->buffer,new_limit);
  else {
    new=u8_malloc(new_limit);
    if (new) memcpy(new,b->buffer,current_size);
    b->buf_flags|=FD_BUFFER_IS_MALLOCD;}
  if (new == NULL) return 0;
  b->buffer=new; b->bufwrite=new+current_size;
  b->buflim=b->buffer+new_limit;
  b->buflen=new_limit;
  return 1;
}

static int grow_input_buffer(struct FD_INBUF *in,int delta)
{
  struct FD_RAWBUF *b=(struct FD_RAWBUF *)in;
  size_t current_size=b->bufpoint-b->buffer;
  size_t current_limit=b->buflim-b->buffer;
  size_t new_limit=current_limit;
  size_t need_size=current_size+delta;
  unsigned char *new;
  if (new_limit<=0) new_limit=1000;
  while (new_limit < need_size)
    if (new_limit>=250000) new_limit=new_limit+25000;
    else new_limit=new_limit*2;
  if ((b->buf_flags)&(FD_BUFFER_IS_MALLOCD))
    new=u8_realloc(b->buffer,new_limit);
  else {
    new=u8_malloc(new_limit);
    if (new) memcpy(new,b->buffer,current_size);
    b->buf_flags=b->buf_flags|FD_BUFFER_IS_MALLOCD;}
  if (new == NULL) return 0;
  b->buffer=new; b->bufpoint=new+current_size;
  b->buflim=b->buffer+new_limit;
  b->buflen=new_limit;
  return 1;
}

FD_EXPORT int fd_needs_space(struct FD_OUTBUF *b,size_t delta)
{
  if (b->bufwrite+delta > b->buflim)
    return grow_output_buffer(b,delta);
  else return 1;
}
FD_EXPORT int _fd_grow_outbuf(struct FD_OUTBUF *b,size_t delta)
{
  if (b->bufwrite+delta > b->buflim)
    return grow_output_buffer(b,delta);
  else return 1;
}
FD_EXPORT int _fd_grow_inbuf(struct FD_INBUF *b,size_t delta)
{
  if (b->bufread+delta > b->buflim)
    return grow_input_buffer(b,delta);
  else return 1;
}

/* Writing stuff */

FD_EXPORT int _fd_write_byte(struct FD_OUTBUF *b,unsigned char byte)
{
  if (FD_EXPECT_FALSE(FD_ISREADING(b))) return fd_isreadbuf(b);
  else if (fd_needs_space(b,1)) {
    *(b->bufwrite++)=byte; 
    return 1;}
  else return -1;
}

FD_EXPORT int _fd_write_4bytes(struct FD_OUTBUF *b,fd_4bytes w)
{
  if (FD_EXPECT_FALSE(FD_ISREADING(b))) return fd_isreadbuf(b);
  else if (fd_needs_space(b,4)==0)
    return -1;
  *(b->bufwrite++)=(((w>>24)&0xFF));
  *(b->bufwrite++)=(((w>>16)&0xFF));
  *(b->bufwrite++)=(((w>>8)&0xFF));
  *(b->bufwrite++)=(((w>>0)&0xFF));
  return 4;
}

FD_EXPORT int _fd_write_8bytes(struct FD_OUTBUF *b,fd_8bytes w)
{
  if (FD_EXPECT_FALSE(FD_ISREADING(b))) return fd_isreadbuf(b);
  else if (fd_needs_space(b,8)==0)
    return -1;
  *(b->bufwrite++)=((w>>56)&0xFF);
  *(b->bufwrite++)=((w>>48)&0xFF);
  *(b->bufwrite++)=((w>>40)&0xFF);
  *(b->bufwrite++)=((w>>32)&0xFF);
  *(b->bufwrite++)=((w>>24)&0xFF);
  *(b->bufwrite++)=((w>>16)&0xFF);
  *(b->bufwrite++)=((w>>8)&0xFF);
  *(b->bufwrite++)=((w>>0)&0xFF);
  return 8;
}

FD_EXPORT int _fd_write_bytes
   (struct FD_OUTBUF *b,const unsigned char *data,int size)
{
  if (FD_EXPECT_FALSE(FD_ISREADING(b))) return fd_isreadbuf(b);
  else if (fd_needs_space(b,size)==0) return -1;
  memcpy(b->bufwrite,data,size); b->bufwrite=b->bufwrite+size;
  return size;
}

/* Byte input */

#define nobytes(in,nbytes) (FD_EXPECT_FALSE(!(fd_needs_bytes(in,nbytes))))
#define havebytes(in,nbytes) (FD_EXPECT_TRUE(fd_needs_bytes(in,nbytes)))

FD_EXPORT int fd_grow_byte_input(struct FD_INBUF *b,size_t len)
{
  unsigned int current_off=b->bufread-b->buffer;
  unsigned int current_limit=b->buflim-b->buffer;
  unsigned char *old=(unsigned char *)b->buffer, *new;
  if ((b->buf_flags)&(FD_BUFFER_IS_MALLOCD))
    new=u8_realloc(old,len);
  else {
    new=u8_malloc(len);
    if (new) memcpy(new,old,current_limit);
    b->buf_flags=b->buf_flags|FD_BUFFER_IS_MALLOCD;}
  if (new == NULL) return 0;
  b->buffer=new; b->bufread=new+current_off;
  b->buflim=b->buffer+current_limit;
  return 1;
}

/* Exported functions */

FD_EXPORT int _fd_read_byte(struct FD_INBUF *buf)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf))) return fd_iswritebuf(buf);
  else if (fd_needs_bytes(buf,1)) return (*(buf->bufread++));
  else return -1;
}

FD_EXPORT int _fd_unread_byte(struct FD_INBUF *buf,int byte)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf))) return fd_iswritebuf(buf);
  else if (buf->bufread==buf->buffer) {
    fd_seterr(BadUnReadByte,"_fd_unread_byte",NULL,FD_VOID);
    return -1;}
  else if (buf->bufread[-1]!=byte) {
    fd_seterr(BadUnReadByte,"_fd_unread_byte",NULL,FD_VOID);
    return -1;}
  else {buf->bufread--; return 0;}
}

FD_EXPORT unsigned int _fd_read_4bytes(struct FD_INBUF *buf)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf))) return fd_iswritebuf(buf);
  else if (fd_needs_bytes(buf,4)) {
    fd_4bytes value=fd_get_4bytes(buf->bufread);
    buf->bufread=buf->bufread+4;
    return value;}
  else {
    fd_seterr1(fd_UnexpectedEOD);
    return 0;}
}

FD_EXPORT fd_8bytes _fd_read_8bytes(struct FD_INBUF *buf)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf))) return fd_iswritebuf(buf);
  else if (fd_needs_bytes(buf,8)) {
    fd_8bytes value=fd_get_8bytes(buf->bufread);
    buf->bufread=buf->bufread+8;
    return value;}
  else {
    fd_seterr1(fd_UnexpectedEOD);
    return 0;}
}

FD_EXPORT int
  _fd_read_bytes(unsigned char *bytes,struct FD_INBUF *buf,int len)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf))) return fd_iswritebuf(buf);
  else if (fd_needs_bytes(buf,len)) {
    memcpy(bytes,buf->bufread,len);
    buf->bufread=buf->bufread+len;
    return len;}
  else return -1;
}

FD_EXPORT int _fd_read_zint(struct FD_INBUF *buf)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(buf))) return fd_iswritebuf(buf);
  else return fd_read_zint(buf);
}

