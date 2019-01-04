/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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

#include <zlib.h>
#include <errno.h>

#if HAVE_SNAPPYC_H
#include <snappy-c.h>
#endif

#ifndef FD_DEBUG_BUFIO
#define FD_DEBUG_BUFIO 0
#endif

size_t fd_zlib_level = FD_DEFAULT_ZLEVEL;

u8_condition fd_IsWriteBuf=_("Reading from a write buffer");
u8_condition fd_IsReadBuf=_("Writing to a read buffer");

static u8_condition BadUnReadByte=_("Inconsistent read/unread byte");

size_t fd_bigbuf_threshold = FD_BIGBUF_THRESHOLD;

/* Byte output */

FD_EXPORT int fd_isreadbuf(struct FD_OUTBUF *b)
{
  u8_log(LOG_CRIT,fd_IsReadBuf,
         "Trying to write to an input buffer 0x%llx",
         (unsigned long long)b);
  u8_seterr(fd_IsReadBuf,NULL,NULL);
  return -1;
}

FD_EXPORT int fd_iswritebuf(struct FD_INBUF *b)
{
  u8_log(LOG_CRIT,fd_IsWriteBuf,
         "Trying to read from an output buffer 0x%llx",
         (unsigned long long)b);
  u8_seterr(fd_IsWriteBuf,NULL,NULL);
  return -1;
}

FD_EXPORT lispval fdt_isreadbuf(struct FD_OUTBUF *b)
{
  u8_log(LOG_CRIT,"WriteToRead",
         "Trying to write to an input buffer 0x%llx",
         (unsigned long long)b);
  u8_seterr(fd_IsReadBuf,"ReturningDType",NULL);
  return FD_ERROR;
}

FD_EXPORT lispval fdt_iswritebuf(struct FD_INBUF *b)
{
  u8_log(LOG_CRIT,fd_IsWriteBuf,
         "Trying to read from an output buffer 0x%llx",
         (unsigned long long)b);
  u8_seterr(fd_IsWriteBuf,"ReturningDType",NULL);
  return FD_ERROR;
}

/* Closing (freeing) buffers */

FD_EXPORT size_t _fd_raw_closebuf(struct FD_RAWBUF *buf)
{
  if (buf->buf_flags&FD_BUFFER_ALLOC) {
    BUFIO_FREE(buf);
    return buf->buflen;}
  else return 0;
}

/* Growing buffers */

static ssize_t grow_output_buffer(struct FD_OUTBUF *b,size_t delta)
{
  int flags = b->buf_flags;
  if ((b->buf_flushfn) &&
      ( (U8_BITP(flags,FD_BUFFER_NO_GROW)) ||
        ( (b->bufwrite > b->buffer) &&
          (!(flags&FD_BUFFER_NO_FLUSH)) ) ) ) {
    ssize_t result = b->buf_flushfn(b,b->buf_data);
    if (result<0) {
      u8_log(LOG_WARN,"WriteFailed",
             "Can't flush output to file");
      return result;}
    /* See if that fixed it */
    else if (b->bufwrite+delta<b->buflim)
      return b->buflen;
    else if (((b->bufwrite-b->buffer)+delta)<b->buflen) {
      /* You've got the space in the buffer */
      b->buflim = b->bufwrite+delta;
      return (b->bufwrite+delta)-b->buffer;}
    else if (U8_BITP(flags,FD_BUFFER_NO_GROW)) {
      u8_log(LOG_WARN,"WriteFailed","Can't grow buffer");
      return -1;}
    else {/* Go ahead and grow the buffer */}}
  size_t current_size = b->bufwrite-b->buffer;
  size_t current_limit = b->buflim-b->buffer;
  size_t new_limit = current_limit;
  size_t need_size = current_size+delta;
  unsigned char *old = b->buffer, *new=NULL;
  if (new_limit<=0) new_limit = 1000;
  while (new_limit < need_size)
    if (new_limit>=250000) new_limit = new_limit+250000;
    else new_limit = new_limit*2;
  bufio_alloc cur_alloc = BUFIO_ALLOC(b), new_alloc = cur_alloc;
  int free_old = 0;
  if (new_limit > fd_bigbuf_threshold) {
    if (cur_alloc == FD_BIGALLOC_BUFFER)
      new = u8_big_realloc(old,new_limit);
    else new = u8_big_copy(old,new_limit,current_size);
    free_old = (cur_alloc != FD_BIGALLOC_BUFFER);
    new_alloc = FD_BIGALLOC_BUFFER;}
  else if (cur_alloc == FD_BIGALLOC_BUFFER)
    new = u8_big_realloc(old,new_limit);
  else if (cur_alloc == FD_HEAP_BUFFER)
    new = u8_realloc(old,new_limit);
  else {
    new=u8_malloc(new_limit);
    memcpy(new,old,current_size);
    new_alloc = FD_HEAP_BUFFER;
    free_old = cur_alloc;}
  if (new == NULL) return 0;
  if (free_old == 0) {}
  else
    if (cur_alloc == FD_HEAP_BUFFER)
      u8_free(old);
    else if (cur_alloc == FD_BIGALLOC_BUFFER)
      u8_big_free(old);
    else if (cur_alloc == FD_MMAP_BUFFER) {
      int rv = munmap(old,b->buflen);
      if (rv) {
        u8_log(LOG_ERR,"MUnmapFailed",
               "For buffer %llx (len=%lld) in %llx "
               "with errno=%d (%s), keeping new %llx",
               old,b->buflen,b,errno,u8_strerror(errno),new);
        errno=0;}}
    else {}
  b->buf_flags = ( (flags) & (~FD_BUFFER_ALLOC) ) | new_alloc;
  b->buffer = new;
  b->bufwrite = new+current_size;
  b->buflim = b->buffer+new_limit;
  b->buflen = new_limit;
  return 1;
}

static ssize_t grow_input_buffer(struct FD_INBUF *in,size_t need_size)
{
  int flags = in->buf_flags;
  struct FD_RAWBUF *b = (struct FD_RAWBUF *)in;
  size_t current_point = b->bufpoint-b->buffer;
  size_t current_limit = b->buflim-b->buffer;
  size_t cur_size = b->buflen, new_size=cur_size;
  unsigned char *old = b->buffer, *new;
  int free_old = 0;
  if (cur_size > need_size) return cur_size;
  if (U8_BITP(flags,FD_BUFFER_NO_GROW)) {
    u8_seterr("CantGrowInputBuffer","grow_input_buffer",NULL);
    return -1;}
  if (cur_size<=0) new_size=need_size+1;
  else {
    new_size = cur_size;
    while (new_size < need_size)
      if (new_size >= 2500000)
        new_size = new_size + 250000;
      else new_size = new_size*2;}
  bufio_alloc cur_alloc = BUFIO_ALLOC(in), new_alloc = cur_alloc;
  if (new_size > fd_bigbuf_threshold) {
    if (cur_alloc == FD_BIGALLOC_BUFFER)
      new = u8_big_realloc(old,new_size);
    else new = u8_big_copy(old,new_size,current_point);
    free_old = (cur_alloc != FD_BIGALLOC_BUFFER);
    new_alloc = FD_BIGALLOC_BUFFER;}
  else if (cur_alloc == FD_BIGALLOC_BUFFER)
    new = u8_big_realloc(old,new_size);
  else if (cur_alloc == FD_HEAP_BUFFER)
    new = u8_realloc(old,new_size);
  else {
    new=u8_malloc(new_size);
    memcpy(new,old,current_point);
    new_alloc = FD_HEAP_BUFFER;
    free_old = cur_alloc;}
  if (new == NULL) return 0;
  b->buf_flags = ( (flags) & (~FD_BUFFER_ALLOC) ) | new_alloc;
  b->buffer = new;
  b->bufpoint = new+current_point;
  b->buflim = b->buffer+current_limit;
  b->buflen = new_size;
  if (free_old==0) {}
  else if (cur_alloc == FD_HEAP_BUFFER)
    u8_free(old);
  else if (cur_alloc == FD_BIGALLOC_BUFFER)
    u8_big_free(old);
  else {}
  return new_size;
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
  if (b->bufread+delta > b->buflim) {
    ssize_t need_bytes = delta + (b->buflim-b->buffer);
    return grow_input_buffer(b,need_bytes);}
  else return 1;
}

/* Writing stuff */

FD_EXPORT int _fd_write_byte(struct FD_OUTBUF *b,unsigned char byte)
{
  if (PRED_FALSE(FD_ISREADING(b))) return fd_isreadbuf(b);
  else if (fd_needs_space(b,1)) {
    *(b->bufwrite++) = byte;
    return 1;}
  else return -1;
}

FD_EXPORT int _fd_write_4bytes(struct FD_OUTBUF *b,fd_4bytes w)
{
  if (PRED_FALSE(FD_ISREADING(b))) return fd_isreadbuf(b);
  else if (fd_needs_space(b,4) == 0)
    return -1;
  *(b->bufwrite++) = (((w>>24)&0xFF));
  *(b->bufwrite++) = (((w>>16)&0xFF));
  *(b->bufwrite++) = (((w>>8)&0xFF));
  *(b->bufwrite++) = (((w>>0)&0xFF));
  return 4;
}

FD_EXPORT int _fd_write_8bytes(struct FD_OUTBUF *b,fd_8bytes w)
{
  if (PRED_FALSE(FD_ISREADING(b))) return fd_isreadbuf(b);
  else if (fd_needs_space(b,8) == 0)
    return -1;
  *(b->bufwrite++) = ((w>>56)&0xFF);
  *(b->bufwrite++) = ((w>>48)&0xFF);
  *(b->bufwrite++) = ((w>>40)&0xFF);
  *(b->bufwrite++) = ((w>>32)&0xFF);
  *(b->bufwrite++) = ((w>>24)&0xFF);
  *(b->bufwrite++) = ((w>>16)&0xFF);
  *(b->bufwrite++) = ((w>>8)&0xFF);
  *(b->bufwrite++) = ((w>>0)&0xFF);
  return 8;
}

FD_EXPORT int _fd_write_bytes
   (struct FD_OUTBUF *b,const unsigned char *data,int size)
{
  if (PRED_FALSE(FD_ISREADING(b))) return fd_isreadbuf(b);
  else if (fd_needs_space(b,size) == 0) return -1;
  memcpy(b->bufwrite,data,size); b->bufwrite = b->bufwrite+size;
  return size;
}

/* Byte input */

#define nobytes(in,nbytes) (PRED_FALSE(!(fd_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (PRED_TRUE(fd_request_bytes(in,nbytes)))

FD_EXPORT ssize_t fd_grow_byte_input(struct FD_INBUF *b,size_t len)
{
  if ( b->buflen >= len)
    return len;
  else return grow_input_buffer(b,len);
}

/* Exported functions */

FD_EXPORT int _fd_read_byte(struct FD_INBUF *buf)
{
  if (PRED_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if (fd_request_bytes(buf,1))
    return (*(buf->bufread++));
  else return -1;
}

FD_EXPORT int _fd_probe_byte(struct FD_INBUF *buf)
{
  if (PRED_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if (fd_request_bytes(buf,1))
    return (*(buf->bufread));
  else return -1;
}

FD_EXPORT int _fd_unread_byte(struct FD_INBUF *buf,int byte)
{
  if (PRED_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if (buf->bufread == buf->buffer) {
    fd_seterr2(BadUnReadByte,"_fd_unread_byte");
    return -1;}
  else if (buf->bufread[-1]!=byte) {
    fd_seterr2(BadUnReadByte,"_fd_unread_byte");
    return -1;}
  else {buf->bufread--; return 0;}
}

FD_EXPORT long long _fd_read_4bytes(struct FD_INBUF *buf)
{
  if (PRED_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if (fd_request_bytes(buf,4)) {
    fd_4bytes value = fd_get_4bytes(buf->bufread);
    buf->bufread = buf->bufread+4;
    return value;}
  else {
    fd_seterr1(fd_UnexpectedEOD);
    return -1;}
}

FD_EXPORT fd_8bytes _fd_read_8bytes(struct FD_INBUF *buf)
{
  if (PRED_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if (fd_request_bytes(buf,8)) {
    fd_8bytes value = fd_get_8bytes(buf->bufread);
    buf->bufread = buf->bufread+8;
    return value;}
  else {
    fd_seterr1(fd_UnexpectedEOD);
    return 0;}
}

FD_EXPORT int
  _fd_read_bytes(unsigned char *bytes,struct FD_INBUF *buf,int len)
{
  if (PRED_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else if (fd_request_bytes(buf,len)) {
    memcpy(bytes,buf->bufread,len);
    buf->bufread = buf->bufread+len;
    return len;}
  else return -1;
}

FD_EXPORT int _fd_read_zint(struct FD_INBUF *buf)
{
  if (PRED_FALSE(FD_ISWRITING(buf)))
    return fd_iswritebuf(buf);
  else return fd_read_zint(buf);
}

/* Compression functions */

FD_EXPORT
unsigned char *fd_zlib_compress
(unsigned char *in,size_t n_bytes,
 unsigned char *out,ssize_t *z_len,
 int level_arg)
{
  Bytef *zbuf  = (Bytef *)out;
  ssize_t buf_len = *z_len;
  uLongf z_lim = (((uLongf)(n_bytes*1.001))+12);
  int level = (level_arg>=0) ? (level_arg) : FD_DEFAULT_ZLEVEL;
  if ( (out==NULL) || (buf_len < z_lim) ) {
    zbuf    = u8_malloc(z_lim);
    buf_len = z_lim;}
  int error = compress2(zbuf,&buf_len,in,n_bytes,level);
  if (error) {
    switch (error) {
    case Z_MEM_ERROR:
      fd_seterr1("ZLIB Out of Memory"); break;
    case Z_BUF_ERROR:
      fd_seterr1("ZLIB Buffer error"); break;
    case Z_DATA_ERROR:
      fd_seterr1("ZLIB Data error"); break;
    default:
      fd_seterr1("Bad ZLIB return code");}
    if (zbuf!=out) u8_free(zbuf);
    return NULL;}
  *z_len = buf_len;
  return zbuf;
}

FD_EXPORT
unsigned char *fd_snappy_compress
(unsigned char *in,size_t n_bytes,
 unsigned char *out,ssize_t *z_len)
{
  unsigned char *zbuf = out;
  size_t max_outlen = snappy_max_compressed_length(n_bytes);
  ssize_t buf_len = *z_len;
  if (max_outlen>buf_len) {
    zbuf = u8_malloc(max_outlen);
    if (zbuf==NULL) {
      u8_seterr(fd_MallocFailed,"fd_snappy_compress",NULL);
      return NULL;}
    zbuf = u8_malloc(max_outlen);
    *z_len = max_outlen;}
  snappy_status compress_rv=snappy_compress(in,n_bytes,zbuf,z_len);
  if (compress_rv == SNAPPY_OK)
    return zbuf;
  else {
    u8_seterr("SnappyFailed","fd_snappy_compress",NULL);
    if (zbuf != out ) u8_free(zbuf);
    return NULL;}
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
