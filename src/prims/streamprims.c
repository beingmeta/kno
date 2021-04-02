/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/streams.h"
#include "kno/dtypeio.h"
#include "kno/ports.h"
#include "kno/cprims.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>
#include <libu8/u8filefns.h>

#include <zlib.h>


#ifndef KNO_DTWRITE_SIZE
#define KNO_DTWRITE_SIZE 10000
#endif

DEFC_PRIM("write-bytes",write_bytes,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "(WRITE-BYTES *obj* *stream* [*pos*]) "
	  "writes the bytes in *obj* to *stream* at *pos*. "
	  "*obj* is a string or a packet and *pos* defaults "
	  "to the current file position of the stream.",
	  {"object",kno_any_type,KNO_VOID},
	  {"stream",kno_stream_type,KNO_VOID},
	  {"pos",kno_any_type,KNO_VOID})
static lispval write_bytes(lispval object,lispval stream,lispval pos)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  if (! ( (KNO_VOIDP(pos)) ||
	  ( (KNO_INTEGERP(pos)) && (kno_numcompare(pos,KNO_FIXZERO) >= 0))) )
    return kno_err(kno_TypeError,"write_bytes","filepos",pos);
  const unsigned char *bytes = NULL;
  ssize_t n_bytes = -1;
  if (STRINGP(object)) {
    bytes   = CSTRING(object);
    n_bytes = STRLEN(object);}
  else if (PACKETP(object)) {
    bytes   = KNO_PACKET_DATA(object);
    n_bytes = KNO_PACKET_LENGTH(object);}
  else return kno_type_error("string or packet","write_bytes",object);
  if (KNO_VOIDP(pos)) {
    int rv = kno_write_bytes(kno_writebuf(ds),bytes,n_bytes);
    if (rv<0)
      return KNO_ERROR;
    else return KNO_INT(n_bytes);}
  int rv = 0;
  kno_off_t filepos = kno_getint(pos);
#if HAVE_PREAD
  ssize_t to_write = n_bytes;
  const unsigned char *point=bytes;
  while (to_write>0) {
    ssize_t delta = pwrite(ds->stream_fileno,point,to_write,filepos);
    if (delta>0) {
      to_write -= delta;
      point    += delta;
      filepos  += delta;}
    else if (delta<0) {rv=-1; break;}
    else break;}
#else
  rv = kno_lock_stream(ds);
  rv = kno_setpos(ds,filepos);
  if (rv>=0) rv = kno_write_bytes(kno_writebuf(ds),bytes,n_bytes);
  rv = kno_unlock_stream(ds);
#endif
  if (rv<0)
    return KNO_ERROR;
  else {
    kno_flush_stream(ds);
    fsync(ds->stream_fileno);
    return KNO_INT(n_bytes);}
}

DEFC_PRIM("read-byte",read_abyte,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "(READ-4BYTES *stream* [*pos*]) "
	  "reads a bigendian 4-byte integer from *stream*. "
	  "If pos is provided, the value is read from the "
	  "stream at *pos*; if not, the varint is read from "
	  "the current position of the stream and that "
	  "position is advanced by one byte.",
	  {"stream",kno_stream_type,KNO_VOID},
	  {"pos",kno_any_type,KNO_VOID})
static lispval read_abyte(lispval stream,lispval pos)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  if (VOIDP(pos)) {
    kno_lock_stream(ds);
    struct KNO_INBUF *in = kno_readbuf(ds);
    int ival = kno_read_byte(in);
    kno_unlock_stream(ds);
    if (ival<0)
      return KNO_ERROR;
    else return KNO_INT(ival);}
  else {
    long long filepos = (KNO_VOIDP(pos)) ? (-1) : (kno_getint(pos));
    int ival = kno_read_byte_at(ds,filepos,KNO_UNLOCKED);
    if (ival < 0) return KNO_ERROR;
    else return KNO_INT(ival);}
}

DEFC_PRIM("read-4bytes",read_4bytes,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "(READ-4BYTES *stream* [*pos*]) "
	  "reads a bigendian 4-byte integer from *stream*. "
	  "If pos is provided, the value is read from the "
	  "stream at *pos*; if not, the varint is read from "
	  "the current position of the stream and that "
	  "position is advanced by four bytes.",
	  {"stream",kno_stream_type,KNO_VOID},
	  {"pos",kno_any_type,KNO_VOID})
static lispval read_4bytes(lispval stream,lispval pos)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  if (VOIDP(pos)) {
    kno_lock_stream(ds);
    struct KNO_INBUF *in = kno_readbuf(ds);
    long long ival = kno_read_4bytes(in);
    kno_unlock_stream(ds);
    if (ival<0)
      return KNO_ERROR;
    else return KNO_INT(ival);}
  else {
    long long filepos = kno_getint(pos);
    long long ival = kno_read_4bytes_at(ds,filepos,KNO_UNLOCKED);
    if (ival<0) return KNO_ERROR;
    if (VOIDP(pos)) kno_setpos(ds,filepos+8);
    return KNO_INT(ival);}
}

DEFC_PRIM("read-8bytes",read_8bytes,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "(READ-8BYTES *stream* [*pos*]) "
	  "reads a bigendian 8-byte integer from *stream*. "
	  "If pos is provided, the value is read from the "
	  "stream at *pos*; if not, the varint is read from "
	  "the current position of the stream and that "
	  "position is advanced by eight bytes.",
	  {"stream",kno_stream_type,KNO_VOID},
	  {"pos",kno_any_type,KNO_VOID})
static lispval read_8bytes(lispval stream,lispval pos)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  int err = 0;
  long long filepos = (VOIDP(pos)) ? (kno_getpos(ds)) : kno_getint(pos);
  unsigned long long ival = kno_read_8bytes_at(ds,filepos,KNO_UNLOCKED,&err);
  if (err)
    return KNO_ERROR;
  if (VOIDP(pos)) kno_setpos(ds,filepos+8);
  return KNO_INT(ival);
}

DEFC_PRIM("read-varint",read_varint,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "(read-varint *stream* [*pos*]) "
	  "reads an encoded varint, up to 8 bytes in decoded "
	  "length from stream. If pos is provided, the value "
	  "is read from the stream at *pos*; if not, the "
	  "varint is read from the current position of the "
	  "stream and that position is advanced based on the "
	  "data read.",
	  {"stream",kno_stream_type,KNO_VOID},
	  {"pos",kno_any_type,KNO_VOID})
static lispval read_varint(lispval stream,lispval pos)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  if (VOIDP(pos)) {
    kno_lock_stream(ds);
    struct KNO_INBUF *in = kno_readbuf(ds);
    long long ival = kno_read_varint(in);
    kno_unlock_stream(ds);
    if (ival<0)
      return KNO_ERROR;
    else return KNO_INT(ival);}
  else {
    unsigned char bytes[16];
    long long ival = -1;
    kno_off_t filepos = kno_getint(pos);
#if HAVE_PREAD
    ssize_t n_read = pread(ds->stream_fileno,bytes,16,filepos);
    if (n_read<0) {
      u8_graberr(errno,"read_varint/pread",u8_strdup(ds->streamid));
      errno=0;
      return KNO_ERROR_VALUE;}
    else {
      struct KNO_INBUF in;
      KNO_INIT_INBUF(&in,bytes,n_read,0);
      ival = kno_read_varint(&in);}
#else
    int irv = kno_lock_stream(ds);
    if (irv<0) return KNO_ERROR;
    kno_off_t old_pos = kno_getpos(ds);
    kno_off_t rv = kno_setpos(ds,filepos);
    if (rv<0) {
      kno_unlock_stream(ds);
      return KNO_ERROR;}
    struct KNO_INBUF *in = kno_readbuf(ds);
    ival = kno_read_varint(in);
    rv = kno_setpos(ds,oldpos);
    if (rv<0) {
      kno_unlock_stream(ds);
      return KNO_ERROR;}
    irv = kno_unlock_stream(ds);
    if (irv<0) return KNO_ERROR;
#endif
    if (ival < 0) {
      u8_byte buf[64];
      return kno_err("VarIntReadFailed","read_varint",
		     u8_bprintf(buf,"pos=%lld",filepos),
		     stream);}
    else return KNO_INT(ival);}
}

DEFC_PRIM("read-bytes",read_bytes,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "(READ-BYTES *stream* *n*] [*pos*]) "
	  "reads *n bytes from *stream* at *pos* (or the "
	  "current location, if none)",
	  {"stream",kno_stream_type,KNO_VOID},
	  {"n",kno_any_type,KNO_VOID},
	  {"pos",kno_any_type,KNO_VOID})
static lispval read_bytes(lispval stream,lispval n,lispval pos)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  long long filepos = (KNO_VOIDP(pos)) ? (kno_getpos(ds)) : (kno_getint(pos));
  ssize_t n_bytes;
  if (KNO_INTEGERP(n))
    n_bytes = kno_getint(n);
  else return kno_type_error("integer size","read_bytes",n);
  unsigned char *bytes = u8_malloc(n_bytes);
#if HAVE_PREAD
  ssize_t to_read = n_bytes;
  unsigned char *point = bytes;
  while (to_read>0) {
    ssize_t delta = pread(ds->stream_fileno,point,to_read,filepos);
    if (delta>0) {
      to_read -= delta;
      point   += delta;
      filepos += delta;}
    else {
      u8_free(bytes);
      u8_graberr(errno,"read_bytes/pread",u8_strdup(ds->streamid));
      errno=0;
      return KNO_ERROR_VALUE;}}
  if (to_read==0) {
    if (VOIDP(pos)) kno_setpos(ds,filepos+n_bytes);
    return kno_init_packet(NULL,n_bytes,bytes);}
#elif KNO_USE_MMAP
  ssize_t page_off = (filepos/512)*512;
  ssize_t map_len  = (pos+n_bytes)-page_off;
  ssize_t buf_off  = filepos - pos;
  unsigned char *mapbuf =
    mmap(NULL,n_bytes,PROT_READ,MAP_PRIVATE,ds->stream_fileno,pos);
  if (mapbuf) {
    memcpy(bytes,mapbuf+buf_off,n_bytes);
    int rv = munmap(mapbuf,map_len);
    if (rv<0) {
      u8_log(LOG_CRIT,kno_failed_unmap,
	     "Couldn't unmap buffer for %s (%p)",ds->streamid,ds);}
    else {
      if (VOIDP(pos)) kno_setpos(ds,filepos+n_bytes);
      return kno_init_packet(NULL,n_bytes,bytes);}}
#endif
  kno_lock_stream(ds);
  if (! (KNO_VOIDP(pos)) ) kno_setpos(ds,pos);
  ssize_t result = kno_read_bytes(bytes,kno_readbuf(ds),n_bytes);
  if (result<0) {
    u8_free(bytes);
    return KNO_ERROR;}
  else return kno_init_packet(NULL,n_bytes,bytes);
}

DEFC_PRIM("write-4bytes",write_4bytes,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "(WRITE-4BYTES *intval* *stream* [*pos*]) "
	  "writes a bigendian 4-byte integer to *stream* at "
	  "*pos* (or the current location, if none)",
	  {"object",kno_any_type,KNO_VOID},
	  {"stream",kno_stream_type,KNO_VOID},
	  {"pos",kno_any_type,KNO_VOID})
static lispval write_4bytes(lispval object,lispval stream,lispval pos)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  long long filepos = (KNO_VOIDP(pos)) ? (-1) : (kno_getint(pos));
  long long ival = kno_getint(object);
  if ( (ival < 0) || (ival >= 0x100000000))
    return kno_type_error("positive 4-byte value","write_4bytes",object);
  int n_bytes = kno_write_4bytes_at(ds,ival,filepos);
  if (n_bytes<0)
    return KNO_ERROR;
  else return KNO_INT(n_bytes);
}

DEFC_PRIM("write-8bytes",write_8bytes,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "(WRITE-8BYTES *intval* *stream* [*pos*]) "
	  "writes a bigendian 8-byte integer to *stream* at "
	  "*pos* (or the current location, if none)",
	  {"object",kno_any_type,KNO_VOID},
	  {"stream",kno_stream_type,KNO_VOID},
	  {"pos",kno_any_type,KNO_VOID})
static lispval write_8bytes(lispval object,lispval stream,lispval pos)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  long long filepos = (KNO_VOIDP(pos)) ? (-1) : (kno_getint(pos));
  long long ival;
  if (KNO_FIXNUMP(object)) {
    ival = KNO_FIX2INT(object);
    if (ival<0)
      return kno_type_error("positive 8-byte value","write_8bytes",object);}
  else if (KNO_BIGINTP(object)) {
    struct KNO_BIGINT *bi = (kno_bigint) object;
    if (kno_bigint_negativep(bi))
      return kno_type_error("positive 4-byte value","write_8bytes",object);
    else if (kno_bigint_fits_in_word_p(bi,8,0))
      ival = kno_bigint_to_ulong_long(bi);
    else return kno_type_error("positive 4-byte value","write_8bytes",object);}
  else return kno_type_error("positive 4-byte value","write_8bytes",object);
  int n_bytes = kno_write_8bytes_at(ds,ival,filepos);
  if (n_bytes<0)
    return KNO_ERROR;
  else return KNO_INT(n_bytes);
}

DEFC_PRIM("zread-int",zread_int,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"stream",kno_stream_type,KNO_VOID})
static lispval zread_int(lispval stream)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  unsigned int ival = kno_read_varint(kno_readbuf(ds));
  return KNO_INT(ival);
}

DEFC_PRIM("zwrite-int",zwrite_int,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"object",kno_any_type,KNO_VOID},
	  {"stream",kno_stream_type,KNO_VOID})
static lispval zwrite_int(lispval object,lispval stream)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  int ival = kno_getint(object);
  int bytes = kno_write_varint(kno_writebuf(ds),ival);
  if (bytes<0) return KNO_ERROR;
  else return KNO_INT(bytes);
}

DEFC_PRIM("open-byte-output",open_byte_output_file,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"fname",kno_string_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval open_byte_output_file(lispval fname,lispval opts)
{
  u8_string filename = CSTRING(fname);
  struct KNO_STREAM *dts=
    (u8_file_existsp(filename)) ?
    (kno_open_file(filename,KNO_FILE_MODIFY)) :
    (kno_open_file(filename,KNO_FILE_CREATE));
  if (dts) {
    U8_CLEAR_ERRNO();
    return LISP_CONS(dts);}
  else {
    u8_free(dts);
    u8_graberrno("open_byte_output_file",u8_strdup(filename));
    return KNO_ERROR;}
}

DEFC_PRIM("open-byte-input",open_byte_input_file,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"fname",kno_string_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval open_byte_input_file(lispval fname,lispval opts)
{
  u8_string filename = CSTRING(fname);
  if (!(u8_file_existsp(filename))) {
    kno_seterr(kno_FileNotFound,"open_byte_input_file",filename,VOID);
    return KNO_ERROR;}
  else {
    struct KNO_STREAM *stream = kno_open_file(filename,KNO_STREAM_READ_ONLY);
    if (stream) {
      U8_CLEAR_ERRNO();
      return (lispval) stream;}
    else return KNO_ERROR_VALUE;}
}

DEFC_PRIM("open-packet",open_packet_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"fname",kno_packet_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval open_packet_prim(lispval packet,lispval opts)
{
  int flags = KNO_STREAM_IS_CONSED | KNO_STREAM_READ_ONLY |
    KNO_STREAM_CAN_SEEK;
  u8_byte buf[100];
  u8_string streamid =
    u8_bprintf(buf,"packet[%d]",KNO_PACKET_LENGTH(packet));
  struct KNO_STREAM *stream = kno_init_byte_stream
    (u8_alloc(struct KNO_STREAM),streamid,flags,
     KNO_PACKET_LENGTH(packet),
     KNO_PACKET_DATA(packet));
  if (stream)
    return (lispval) stream;
  else return KNO_ERROR;
}

DEFC_PRIM("extend-byte-output",extend_byte_output,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"fname",kno_string_type,KNO_VOID})
static lispval extend_byte_output(lispval fname)
{
  u8_string filename = CSTRING(fname);
  struct KNO_STREAM *stream = NULL;
  if (u8_file_existsp(filename))
    stream = kno_open_file(filename,KNO_FILE_MODIFY);
  else stream = kno_open_file(filename,KNO_FILE_CREATE);
  if (stream == NULL)
    return KNO_ERROR_VALUE;
  else {
    kno_endpos(stream);
    U8_CLEAR_ERRNO();
    return (lispval) stream;}
}

DEFC_PRIM("byte-stream?",streamp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval streamp(lispval arg)
{
  if (TYPEP(arg,kno_stream_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("byte-input?",byte_inputp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval byte_inputp(lispval arg)
{
  if (TYPEP(arg,kno_stream_type)) {
    struct KNO_STREAM *dts = (kno_stream)arg;
    if (U8_BITP(dts->buf.raw.buf_flags,KNO_IS_WRITING))
      return KNO_FALSE;
    else return KNO_TRUE;}
  else return KNO_FALSE;
}

DEFC_PRIM("byte-output?",byte_outputp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval byte_outputp(lispval arg)
{
  if (TYPEP(arg,kno_stream_type)) {
    struct KNO_STREAM *dts = (kno_stream)arg;
    if (U8_BITP(dts->buf.raw.buf_flags,KNO_IS_WRITING))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

/* Streampos prim */

DEFC_PRIM("streampos",streampos_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "(STREAMPOS *stream* [*setpos*]) "
	  "gets or sets the position of *stream*",
	  {"stream_arg",kno_stream_type,KNO_VOID},
	  {"pos",kno_any_type,KNO_VOID})
static lispval streampos_prim(lispval stream_arg,lispval pos)
{
  struct KNO_STREAM *stream = (kno_stream)stream_arg;
  if (VOIDP(pos)) {
    kno_off_t curpos = kno_getpos(stream);
    return KNO_INT(curpos);}
  else if (KNO_ISINT64(pos)) {
    kno_off_t maxpos = kno_endpos(stream);
    long long intval = kno_getint(pos);
    if (intval<0) {
      kno_off_t target = maxpos-(intval+1);
      if ((target>=0)&&(target<maxpos)) {
	kno_off_t result = kno_setpos(stream,target);
	if (result<0) return KNO_ERROR;
	else return KNO_INT(result);}
      else {
	kno_seterr(_("Out of file range"),"streampos_prim",stream->streamid,pos);
	return KNO_ERROR;}}
    else if (intval<maxpos) {
      kno_off_t result = kno_setpos(stream,intval);
      if (result<0) return KNO_ERROR;
      else return KNO_INT(result);}
    else {
      kno_seterr(_("Out of file range"),"streampos_prim",stream->streamid,pos);
      return KNO_ERROR;}}
  else return kno_type_error("stream position","streampos_prim",pos);
}

/* Truncate prim */

DEFC_PRIM("ftruncate",ftruncate_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "(FTRUNCATE *nameorstream* *newsize*) "
	  "truncates a file (name or stream) to a particular "
	  "length",
	  {"arg",kno_any_type,KNO_VOID},
	  {"offset",kno_any_type,KNO_VOID})
static lispval ftruncate_prim(lispval arg,lispval offset)
{
  off_t new_len = -1;
  if (! ( (KNO_FIXNUMP(offset)) || (KNO_BIGINTP(offset))) )
    return kno_type_error("integer","ftruncate_prim",offset);
  else if (kno_numcompare(offset,KNO_FIXZERO)<0)
    return kno_type_error("positive integer","ftruncate_prim",offset);
  else if ( (KNO_BIGINTP(new_len)) &&
	    ( ! (kno_bigint_fits_in_word_p((kno_bigint)arg,sizeof(new_len),1)) ) )
    return kno_type_error("file size","ftruncate_prim",offset);
  else new_len = kno_getint64(arg);
  if (KNO_STRINGP(arg)) {
    char *libc_string = u8_tolibc(KNO_CSTRING(arg));
    int rv = truncate(libc_string,new_len);
    if (rv<0) {
      u8_graberrno("ftruncate_prim",u8_strdup(KNO_CSTRING(arg)));
      u8_free(libc_string);
      return KNO_ERROR_VALUE;}
    else {
      u8_free(libc_string);
      return offset;}}
  else if (KNO_TYPEP(arg,kno_stream_type)) {
    kno_stream s = (kno_stream) arg;
    kno_lock_stream(s);
    int fd = s->stream_fileno;
    if (fd<0) {
      u8_seterr("ClosedStream","ftruncate_prim",s->streamid);
      kno_unlock_stream(s);
      return KNO_ERROR_VALUE;}
    else {
      kno_off_t old_pos = s->stream_filepos;
      if (old_pos > new_len) kno_setpos(s,new_len);
      int rv = ftruncate(fd,new_len);
      if (rv<0) {
	u8_graberr(errno,"ftruncate_prim",u8_strdup(KNO_CSTRING(arg)));
	kno_setpos(s,old_pos);
	kno_unlock_stream(s);
	return KNO_ERROR_VALUE;}
      else {
	s->stream_maxpos=new_len;
	if (s->stream_flags & KNO_STREAM_MMAPPED)
	  kno_reopen_file_stream(s,-1,-1);
	kno_unlock_stream(s);
	return offset;}}}
  else return kno_type_error("Filename or stream","ftruncate_prim",arg);
}

static int scheme_streamprims_initialized = 0;

KNO_EXPORT void kno_init_streamprims_c()
{
  if (scheme_streamprims_initialized) return;
  scheme_streamprims_initialized = 1;
  u8_register_source_file(_FILEINFO);
  link_local_cprims();
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("ftruncate",ftruncate_prim,2,kno_binio_module);
  KNO_LINK_CPRIM("streampos",streampos_prim,2,kno_binio_module);
  KNO_LINK_CPRIM("byte-output?",byte_outputp,1,kno_binio_module);
  KNO_LINK_CPRIM("byte-input?",byte_inputp,1,kno_binio_module);
  KNO_LINK_CPRIM("byte-stream?",streamp,1,kno_binio_module);
  KNO_LINK_CPRIM("extend-byte-output",extend_byte_output,1,kno_binio_module);
  KNO_LINK_CPRIM("open-byte-input",open_byte_input_file,2,kno_binio_module);
  KNO_LINK_CPRIM("open-byte-output",open_byte_output_file,2,kno_binio_module);
  KNO_LINK_CPRIM("zwrite-int",zwrite_int,2,kno_binio_module);
  KNO_LINK_CPRIM("zread-int",zread_int,1,kno_binio_module);

  KNO_LINK_CPRIM("open-packet",open_packet_prim,2,kno_binio_module);

  KNO_LINK_CPRIM("write-8bytes",write_8bytes,3,kno_binio_module);
  KNO_LINK_CPRIM("write-4bytes",write_4bytes,3,kno_binio_module);
  KNO_LINK_CPRIM("read-bytes",read_bytes,3,kno_binio_module);
  KNO_LINK_CPRIM("read-varint",read_varint,2,kno_binio_module);
  KNO_LINK_CPRIM("read-8bytes",read_8bytes,2,kno_binio_module);
  KNO_LINK_CPRIM("read-4bytes",read_4bytes,2,kno_binio_module);
  KNO_LINK_CPRIM("read-byte",read_abyte,2,kno_binio_module);
  KNO_LINK_CPRIM("write-bytes",write_bytes,3,kno_binio_module);

  KNO_LINK_ALIAS("read-int",read_4bytes,kno_binio_module);
  KNO_LINK_ALIAS("write-int",write_4bytes,kno_binio_module);
  KNO_LINK_ALIAS("extend-byte-file",extend_byte_output,kno_binio_module);

  KNO_LINK_ALIAS("dtype-stream?",streamp,kno_binio_module);
  KNO_LINK_ALIAS("dtype-input?",byte_inputp,kno_binio_module);
  KNO_LINK_ALIAS("dtype-output?",byte_outputp,kno_binio_module);
  KNO_LINK_ALIAS("extend-dtype-file",extend_byte_output,kno_binio_module);
  KNO_LINK_ALIAS("open-dtype-input",open_byte_input_file,kno_binio_module);
  KNO_LINK_ALIAS("open-dtype-output",open_byte_output_file,kno_binio_module);
  KNO_LINK_ALIAS("open-dtype-file",open_byte_input_file,kno_binio_module);

}
