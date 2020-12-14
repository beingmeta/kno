/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_BUFIO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/dtypeio.h"

#include <zlib.h>
#include <errno.h>

#if HAVE_SNAPPYC_H
#include <snappy-c.h>
#endif

#ifndef KNO_DEBUG_BUFIO
#define KNO_DEBUG_BUFIO 0
#endif

size_t kno_zlib_level = KNO_DEFAULT_ZLEVEL;

u8_condition kno_IsWriteBuf=_("Reading from a write buffer");
u8_condition kno_IsReadBuf=_("Writing to a read buffer");

static u8_condition BadUnReadByte=_("Inconsistent read/unread byte");

size_t kno_bigbuf_threshold = KNO_BIGBUF_THRESHOLD;

/* Byte output */

KNO_EXPORT int kno_isreadbuf(struct KNO_OUTBUF *b)
{
  u8_log(LOG_CRIT,kno_IsReadBuf,
         "Trying to write to an input buffer 0x%llx",
         KNO_LONGVAL(b));
  u8_seterr(kno_IsReadBuf,NULL,NULL);
  return -1;
}

KNO_EXPORT int kno_iswritebuf(struct KNO_INBUF *b)
{
  u8_log(LOG_CRIT,kno_IsWriteBuf,
         "Trying to read from an output buffer 0x%llx",
         KNO_LONGVAL(b));
  u8_seterr(kno_IsWriteBuf,NULL,NULL);
  return -1;
}

KNO_EXPORT lispval kno_lisp_isreadbuf(struct KNO_OUTBUF *b)
{
  u8_log(LOG_CRIT,"WriteToRead",
         "Trying to write to an input buffer 0x%llx",
         KNO_LONGVAL(b));
  u8_seterr(kno_IsReadBuf,"ReturningDType",NULL);
  return KNO_ERROR;
}

KNO_EXPORT lispval kno_lisp_iswritebuf(struct KNO_INBUF *b)
{
  u8_log(LOG_CRIT,kno_IsWriteBuf,
         "Trying to read from an output buffer 0x%llx",
         KNO_LONGVAL(b));
  u8_seterr(kno_IsWriteBuf,"ReturningDType",NULL);
  return KNO_ERROR;
}

KNO_EXPORT int kno_reset_inbuf(struct KNO_INBUF *b)
{
  if (RARELY(KNO_ISWRITING(b)))
    return kno_iswritebuf(b);
  b->bufread=b->buflim=b->buffer;
  return 0;
}

/* Closing (freeing) buffers */

KNO_EXPORT size_t _kno_raw_closebuf(struct KNO_RAWBUF *buf)
{
  if (buf->buf_flags&KNO_BUFFER_ALLOC) {
    BUFIO_FREE(buf);
    return buf->buflen;}
  else return 0;
}

/* Growing buffers */

static ssize_t grow_output_buffer(struct KNO_OUTBUF *b,size_t delta)
{
  int flags = b->buf_flags;
  if ((b->buf_flushfn) &&
      ( (U8_BITP(flags,KNO_BUFFER_NO_GROW)) ||
        ( (b->bufwrite > b->buffer) &&
          (!(flags&KNO_BUFFER_NO_FLUSH)) ) ) ) {
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
    else if (U8_BITP(flags,KNO_BUFFER_NO_GROW)) {
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
  if (new_limit > kno_bigbuf_threshold) {
    if (cur_alloc == KNO_BIGALLOC_BUFFER)
      new = u8_big_realloc(old,new_limit);
    else new = u8_big_copy(old,new_limit,current_size);
    free_old = (cur_alloc != KNO_BIGALLOC_BUFFER);
    new_alloc = KNO_BIGALLOC_BUFFER;}
  else if (cur_alloc == KNO_BIGALLOC_BUFFER)
    new = u8_big_realloc(old,new_limit);
  else if (cur_alloc == KNO_HEAP_BUFFER)
    new = u8_realloc(old,new_limit);
  else {
    new=u8_malloc(new_limit);
    memcpy(new,old,current_size);
    new_alloc = KNO_HEAP_BUFFER;
    free_old = cur_alloc;}
  if (new == NULL) return 0;
  if (free_old == 0) {}
  else
    if (cur_alloc == KNO_HEAP_BUFFER)
      u8_free(old);
    else if (cur_alloc == KNO_BIGALLOC_BUFFER)
      u8_big_free(old);
    else if (cur_alloc == KNO_MMAP_BUFFER) {
      int rv = munmap(old,b->buflen);
      if (rv) {
        u8_log(LOG_ERR,"MUnmapFailed",
               "For buffer %llx (len=%lld) in %llx "
               "with errno=%d (%s), keeping new %llx",
               old,b->buflen,b,errno,u8_strerror(errno),new);
        errno=0;}}
    else {}
  b->buf_flags = ( (flags) & (~KNO_BUFFER_ALLOC) ) | new_alloc;
  b->buffer = new;
  b->bufwrite = new+current_size;
  b->buflim = b->buffer+new_limit;
  b->buflen = new_limit;
  return 1;
}

static ssize_t grow_input_buffer(struct KNO_INBUF *in,size_t need_size)
{
  int flags = in->buf_flags;
  struct KNO_RAWBUF *b = (struct KNO_RAWBUF *)in;
  size_t current_point = b->bufpoint-b->buffer;
  size_t current_limit = b->buflim-b->buffer;
  size_t cur_size = b->buflen, new_size=cur_size;
  unsigned char *old = b->buffer, *new;
  int free_old = 0;
  if (cur_size > need_size) return cur_size;
  if (U8_BITP(flags,KNO_BUFFER_NO_GROW)) {
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
  if (new_size > kno_bigbuf_threshold) {
    if (cur_alloc == KNO_BIGALLOC_BUFFER)
      new = u8_big_realloc(old,new_size);
    else new = u8_big_copy(old,new_size,current_point);
    free_old = (cur_alloc != KNO_BIGALLOC_BUFFER);
    new_alloc = KNO_BIGALLOC_BUFFER;}
  else if (cur_alloc == KNO_BIGALLOC_BUFFER)
    new = u8_big_realloc(old,new_size);
  else if (cur_alloc == KNO_HEAP_BUFFER)
    new = u8_realloc(old,new_size);
  else {
    new=u8_malloc(new_size);
    memcpy(new,old,current_point);
    new_alloc = KNO_HEAP_BUFFER;
    free_old = cur_alloc;}
  if (new == NULL) return 0;
  b->buf_flags = ( (flags) & (~KNO_BUFFER_ALLOC) ) | new_alloc;
  b->buffer = new;
  b->bufpoint = new+current_point;
  b->buflim = b->buffer+current_limit;
  b->buflen = new_size;
  if (free_old==0) {}
  else if (cur_alloc == KNO_HEAP_BUFFER)
    u8_free(old);
  else if (cur_alloc == KNO_BIGALLOC_BUFFER)
    u8_big_free(old);
  else {}
  return new_size;
}

KNO_EXPORT int kno_needs_space(struct KNO_OUTBUF *b,size_t delta)
{
  if (b->bufwrite+delta > b->buflim)
    return grow_output_buffer(b,delta);
  else return 1;
}
KNO_EXPORT int _kno_grow_outbuf(struct KNO_OUTBUF *b,size_t delta)
{
  if (b->bufwrite+delta > b->buflim)
    return grow_output_buffer(b,delta);
  else return 1;
}
KNO_EXPORT int _kno_grow_inbuf(struct KNO_INBUF *b,size_t delta)
{
  if (b->bufread+delta > b->buflim) {
    ssize_t need_bytes = delta + (b->buflim-b->buffer);
    return grow_input_buffer(b,need_bytes);}
  else return 1;
}

/* Writing stuff */

KNO_EXPORT int _kno_write_byte(struct KNO_OUTBUF *b,unsigned char byte)
{
  if (RARELY(KNO_ISREADING(b))) return kno_isreadbuf(b);
  else if (kno_needs_space(b,1)) {
    *(b->bufwrite++) = byte;
    return 1;}
  else return -1;
}

KNO_EXPORT int _kno_write_4bytes(struct KNO_OUTBUF *b,kno_4bytes w)
{
  if (RARELY(KNO_ISREADING(b))) return kno_isreadbuf(b);
  else if (kno_needs_space(b,4) == 0)
    return -1;
  *(b->bufwrite++) = (((w>>24)&0xFF));
  *(b->bufwrite++) = (((w>>16)&0xFF));
  *(b->bufwrite++) = (((w>>8)&0xFF));
  *(b->bufwrite++) = (((w>>0)&0xFF));
  return 4;
}

KNO_EXPORT int _kno_write_8bytes(struct KNO_OUTBUF *b,kno_8bytes w)
{
  if (RARELY(KNO_ISREADING(b))) return kno_isreadbuf(b);
  else if (kno_needs_space(b,8) == 0)
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

KNO_EXPORT int _kno_write_bytes
(struct KNO_OUTBUF *b,const unsigned char *data,int size)
{
  if (RARELY(KNO_ISREADING(b))) return kno_isreadbuf(b);
  else if (kno_needs_space(b,size) == 0) return -1;
  memcpy(b->bufwrite,data,size); b->bufwrite = b->bufwrite+size;
  return size;
}

/* Byte input */

#define nobytes(in,nbytes) (RARELY(!(kno_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (USUALLY(kno_request_bytes(in,nbytes)))

KNO_EXPORT ssize_t kno_grow_byte_input(struct KNO_INBUF *b,size_t len)
{
  if ( b->buflen >= len)
    return len;
  else return grow_input_buffer(b,len);
}

/* Exported functions */

KNO_EXPORT int _kno_read_byte(struct KNO_INBUF *buf)
{
  if (RARELY(KNO_ISWRITING(buf)))
    return kno_iswritebuf(buf);
  else if (kno_request_bytes(buf,1))
    return (*(buf->bufread++));
  else return -1;
}

KNO_EXPORT int _kno_probe_byte(struct KNO_INBUF *buf)
{
  u8_exception pre_ex = u8_current_exception;
  if (RARELY(KNO_ISWRITING(buf)))
    return kno_iswritebuf(buf);
  else if (kno_request_bytes(buf,1))
    return (*(buf->bufread));
  else {
    if (u8_current_exception != pre_ex) u8_pop_exception();
    return -1;}
}

KNO_EXPORT int _kno_unread_byte(struct KNO_INBUF *buf,int byte)
{
  if (RARELY(KNO_ISWRITING(buf)))
    return kno_iswritebuf(buf);
  else if (buf->bufread == buf->buffer)
    return KNO_ERR2(-1,BadUnReadByte,"_kno_unread_byte");
  else if (buf->bufread[-1]!=byte)
    return KNO_ERR2(-1,BadUnReadByte,"_kno_unread_byte");
  else {
    buf->bufread--;
    return 0;}
}

KNO_EXPORT long long _kno_read_4bytes(struct KNO_INBUF *buf)
{
  if (RARELY(KNO_ISWRITING(buf)))
    return kno_iswritebuf(buf);
  else if (kno_request_bytes(buf,4)) {
    kno_4bytes value = kno_get_4bytes(buf->bufread);
    buf->bufread = buf->bufread+4;
    return value;}
  else return KNO_ERR1(-1,kno_UnexpectedEOD);
}

KNO_EXPORT kno_8bytes _kno_read_8bytes(struct KNO_INBUF *buf)
{
  if (RARELY(KNO_ISWRITING(buf)))
    return kno_iswritebuf(buf);
  else if (kno_request_bytes(buf,8)) {
    kno_8bytes value = kno_get_8bytes(buf->bufread);
    buf->bufread = buf->bufread+8;
    return value;}
  else return KNO_ERR1(0,kno_UnexpectedEOD);
}

KNO_EXPORT int
_kno_read_bytes(unsigned char *bytes,struct KNO_INBUF *buf,int len)
{
  if (RARELY(KNO_ISWRITING(buf)))
    return kno_iswritebuf(buf);
  else if (kno_request_bytes(buf,len)) {
    memcpy(bytes,buf->bufread,len);
    buf->bufread = buf->bufread+len;
    return len;}
  else return -1;
}

KNO_EXPORT int _kno_read_varint(struct KNO_INBUF *buf)
{
  if (RARELY(KNO_ISWRITING(buf)))
    return kno_iswritebuf(buf);
  else return kno_read_varint(buf);
}

/* Compression functions */

KNO_EXPORT
unsigned char *kno_zlib_compress
(unsigned char *in,size_t n_bytes,
 unsigned char *out,ssize_t *z_len,
 int level_arg)
{
  Bytef *zbuf  = (Bytef *)out;
  uLongf buf_len = *z_len;
  uLongf z_lim = (((uLongf)(n_bytes*1.001))+12);
  int level = (level_arg>=0) ? (level_arg) : KNO_DEFAULT_ZLEVEL;
  if ( (out==NULL) || (buf_len < z_lim) ) {
    zbuf    = u8_malloc(z_lim);
    buf_len = z_lim;}
  int error = compress2(zbuf,&buf_len,in,n_bytes,level);
  if (error) {
    switch (error) {
    case Z_MEM_ERROR:
      kno_seterr1("ZLIB Out of Memory"); break;
    case Z_BUF_ERROR:
      kno_seterr1("ZLIB Buffer error"); break;
    case Z_DATA_ERROR:
      kno_seterr1("ZLIB Data error"); break;
    default:
      kno_seterr1("Bad ZLIB return code");}
    if (zbuf!=out) u8_free(zbuf);
    return NULL;}
  *z_len = buf_len;
  return zbuf;
}

KNO_EXPORT
unsigned char *kno_snappy_compress
(unsigned char *in,size_t n_bytes,
 unsigned char *out,ssize_t *z_len)
{
  unsigned char *zbuf = out;
  size_t max_outlen = snappy_max_compressed_length(n_bytes);
  ssize_t buf_len = *z_len;
  if (max_outlen>buf_len) {
    zbuf = u8_malloc(max_outlen);
    if (zbuf==NULL) {
      u8_seterr(kno_MallocFailed,"kno_snappy_compress",NULL);
      return NULL;}
    zbuf = u8_malloc(max_outlen);
    *z_len = max_outlen;}
  snappy_status compress_rv=snappy_compress(in,n_bytes,zbuf,z_len);
  if (compress_rv == SNAPPY_OK)
    return zbuf;
  else {
    u8_seterr("SnappyFailed","kno_snappy_compress",NULL);
    if (zbuf != out ) u8_free(zbuf);
    return NULL;}
}

