/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL 1

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/numbers.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/streams.h"
#include "kno/dtypeio.h"
#include "kno/ports.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>
#include <libu8/u8filefns.h>

#include <zlib.h>

#ifndef KNO_DTWRITE_SIZE
#define KNO_DTWRITE_SIZE 10000
#endif

static lispval read_dtype(lispval stream,lispval pos,lispval len)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  if (KNO_VOIDP(pos)) {
    lispval object = kno_read_dtype(kno_readbuf(ds));
    if (object == KNO_EOD)
      return KNO_EOF;
    else return object;}
  else if (KNO_VOIDP(len)) {
    long long filepos = KNO_FIX2INT(pos);
    if (filepos<0)
      return kno_type_error("file position","read_type",pos);
    kno_lock_stream(ds);
    kno_setpos(ds,filepos);
    lispval object = kno_read_dtype(kno_readbuf(ds));
    kno_unlock_stream(ds);
    return object;}
  else {
    long long off     = KNO_FIX2INT(pos);
    ssize_t   n_bytes = KNO_FIX2INT(len);
    struct KNO_INBUF _inbuf,
      *in = kno_open_block(ds,&_inbuf,off,n_bytes,0);
    lispval object = kno_read_dtype(in);
    kno_close_inbuf(in);
    return object;}
}

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

static lispval write_dtype(lispval object,lispval stream,
                           lispval pos,lispval max_bytes)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  if (! ( (KNO_VOIDP(pos)) ||
          ( (KNO_INTEGERP(pos)) && (kno_numcompare(pos,KNO_FIXZERO) >= 0))) )
    return kno_err(kno_TypeError,"write_bytes","filepos",pos);
  long long byte_len = (KNO_VOIDP(max_bytes)) ? (-1) : (KNO_FIX2INT(max_bytes));
  unsigned char *bytes = NULL;
  ssize_t n_bytes = -1, init_len = (byte_len>0) ? (byte_len) : (1000);
  unsigned char *bytebuf =  u8_big_alloc(init_len);
  KNO_OUTBUF out = { 0 };
  KNO_INIT_OUTBUF(&out,bytebuf,init_len,KNO_BIGALLOC_BUFFER);
  n_bytes    = kno_write_dtype(&out,object);
  bytebuf    = out.buffer;
  if (!(KNO_VOIDP(max_bytes))) {
    ssize_t max_len = kno_getint(max_bytes);
    if ( (out.bufwrite-out.buffer) > max_len) {
      kno_seterr("TooBig","write_dtype",
                u8_mkstring("The object's DType representation was longer "
                            "than %lld bytes",max_len),
                object);
      kno_close_outbuf(&out);
      return KNO_ERROR;}}
  if (KNO_VOIDP(pos)) {
    int rv = kno_write_bytes(kno_writebuf(ds),bytes,n_bytes);
    u8_big_free(bytes);
    if (rv<0)
      return KNO_ERROR;
    else return KNO_INT(n_bytes);}
  int rv = 0;
  kno_off_t filepos = kno_getint(pos);
#if HAVE_PREAD
  ssize_t to_write = n_bytes;
  unsigned char *point=bytes;
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
  u8_big_free(bytes);
  if (rv<0)
    return KNO_ERROR;
  else {
    kno_flush_stream(ds);
    fsync(ds->stream_fileno);
    return KNO_INT(n_bytes);}
}

static lispval read_4bytes(lispval stream,lispval pos)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  long long filepos = (KNO_VOIDP(pos)) ? (-1) : (kno_getint(pos));
  long long ival = kno_read_4bytes_at(ds,filepos,KNO_UNLOCKED);
  if (ival<0)
    return KNO_ERROR;
  else return KNO_INT(ival);
}

static lispval read_8bytes(lispval stream,lispval pos)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  int err = 0;
  long long filepos = (KNO_VOIDP(pos)) ? (-1) : (kno_getint(pos));
  unsigned long long ival = kno_read_8bytes_at(ds,filepos,KNO_UNLOCKED,&err);
  if (err)
    return KNO_ERROR;
  else return KNO_INT(ival);
}

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
  if (to_read==0)
    return kno_init_packet(NULL,n_bytes,bytes);
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
             "Couldn't unmap buffer for %s (0x%llx)",ds->streamid,ds);}
    else return kno_init_packet(NULL,n_bytes,bytes);}
#endif
  kno_lock_stream(ds);
  if (! (KNO_VOIDP(pos)) ) kno_setpos(ds,pos);
  ssize_t result = kno_read_bytes(bytes,kno_readbuf(ds),n_bytes);
  if (result<0) {
    u8_free(bytes);
    return KNO_ERROR;}
  else return kno_init_packet(NULL,n_bytes,bytes);
}

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

static lispval zread_dtype(lispval stream)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  lispval object = kno_zread_dtype(kno_readbuf(ds));
  if (object == KNO_EOD) return KNO_EOF;
  else return object;
}

static lispval zwrite_dtype(lispval object,lispval stream)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  int bytes = kno_zwrite_dtype(kno_writebuf(ds),object);
  if (bytes<0) return KNO_ERROR;
  else return KNO_INT(bytes);
}

static lispval zwrite_dtypes(lispval object,lispval stream)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  int bytes = kno_zwrite_dtypes(kno_writebuf(ds),object);
  if (bytes<0) return KNO_ERROR;
  else return KNO_INT(bytes);
}

static lispval zread_int(lispval stream)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  unsigned int ival = kno_read_varint(kno_readbuf(ds));
  return KNO_INT(ival);
}

static lispval zwrite_int(lispval object,lispval stream)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  int ival = kno_getint(object);
  int bytes = kno_write_varint(kno_writebuf(ds),ival);
  if (bytes<0) return KNO_ERROR;
  else return KNO_INT(bytes);
}

/* Reading and writing DTYPEs */

static lispval lisp2zipfile(lispval object,lispval filename,lispval bufsiz);

static lispval lisp2file(lispval object,lispval filename,lispval bufsiz)
{
  if ((STRINGP(filename))&&
      ((u8_has_suffix(CSTRING(filename),".ztype",1))||
       (u8_has_suffix(CSTRING(filename),".gz",1)))) {
    return lisp2zipfile(object,filename,bufsiz);}
  else if (STRINGP(filename)) {
    u8_string temp_name = u8_mkstring("%s.part",CSTRING(filename));
    struct KNO_STREAM *out = kno_open_file(temp_name,KNO_FILE_CREATE);
    struct KNO_OUTBUF *outstream = NULL;
    ssize_t bytes;
    if (out == NULL) return KNO_ERROR;
    else outstream = kno_writebuf(out);
    if (FIXNUMP(bufsiz))
      kno_setbufsize(out,FIX2INT(bufsiz));
    bytes = kno_write_dtype(outstream,object);
    if (bytes<0) {
      kno_free_stream(out);
      u8_free(temp_name);
      return KNO_ERROR;}
    kno_free_stream(out);
    int rv = u8_movefile(temp_name,CSTRING(filename));
    if (rv<0) {
      u8_log(LOG_WARN,"MoveFailed",
             "Couldn't move the completed file into %s, leaving in %s",
             CSTRING(filename),temp_name);
      u8_seterr("MoveFailed","lisp2file",u8_strdup(CSTRING(filename)));
      u8_free(temp_name);
      return KNO_ERROR_VALUE;}
    else {
      u8_free(temp_name);
      return KNO_INT(bytes);}}
  else if (TYPEP(filename,kno_stream_type)) {
    KNO_DECL_OUTBUF(tmp,KNO_DTWRITE_SIZE);
    struct KNO_STREAM *stream=
      kno_consptr(struct KNO_STREAM *,filename,kno_stream_type);
    ssize_t bytes = kno_write_dtype(&tmp,object);
    if (bytes<0) {
      kno_close_outbuf(&tmp);
      return KNO_ERROR;}
    else {
      bytes=kno_stream_write(stream,bytes,tmp.buffer);
      kno_close_outbuf(&tmp);
      if (bytes<0)
        return KNO_ERROR;
      else return KNO_INT(bytes);}}
  else return kno_type_error(_("string"),"lisp2file",filename);
}

static lispval lisp2zipfile(lispval object,lispval filename,lispval bufsiz)
{
  if (STRINGP(filename)) {
    u8_string temp_name = u8_mkstring("%s.part",CSTRING(filename));
    struct KNO_STREAM *stream = kno_open_file(temp_name,KNO_FILE_CREATE);
    kno_outbuf out = NULL;
    int bytes;
    if (stream == NULL) {
      u8_free(temp_name);
      return KNO_ERROR;}
    else out = kno_writebuf(stream);
    if (FIXNUMP(bufsiz))
      kno_setbufsize(stream,FIX2INT(bufsiz));
    bytes = kno_zwrite_dtype(out,object);
    if (bytes<0) {
      kno_close_stream(stream,KNO_STREAM_CLOSE_FULL);
      u8_free(temp_name);
      return KNO_ERROR;}
    kno_close_stream(stream,KNO_STREAM_CLOSE_FULL);
    u8_movefile(temp_name,CSTRING(filename));
    u8_free(temp_name);
    return KNO_INT(bytes);}
  else if (TYPEP(filename,kno_stream_type)) {
    KNO_DECL_OUTBUF(tmp,KNO_DTWRITE_SIZE);
    struct KNO_STREAM *stream=
      kno_consptr(struct KNO_STREAM *,filename,kno_stream_type);
    ssize_t bytes = kno_zwrite_dtype(&tmp,object);
    if (bytes<0) {
      kno_close_outbuf(&tmp);
      return KNO_ERROR;}
    else {
      bytes=kno_stream_write(stream,bytes,tmp.buffer);
      kno_close_outbuf(&tmp);
      if (bytes<0)
        return KNO_ERROR;
      else return KNO_INT(bytes);}}
  else return kno_type_error(_("string"),"lisp2zipfile",filename);
}

static lispval add_lisp2zipfile(lispval object,lispval filename);

#define flushp(b) ( (((b).buflim)-((b).bufwrite)) < ((b).buflen)/5 )

static ssize_t write_dtypes(lispval dtypes,struct KNO_STREAM *out)
{
  ssize_t bytes=0, rv=0;
  kno_off_t start= kno_endpos(out);
  if ( start != out->stream_maxpos ) start=-1;
  struct KNO_OUTBUF tmp = { 0 };
  unsigned char tmpbuf[1000];
  KNO_INIT_BYTE_OUTBUF(&tmp,tmpbuf,1000);
  if ((CHOICEP(dtypes))||(PRECHOICEP(dtypes))) {
    /* This writes out the objects sequentially, writing into memory
       first and then to disk, to reduce the danger of malformed
       DTYPEs on the disk. */
    DO_CHOICES(dtype,dtypes) {
      ssize_t write_size=0;
      if ( (flushp(tmp)) ) {
        write_size=kno_stream_write(out,tmp.bufwrite-tmp.buffer,tmp.buffer);
        tmp.bufwrite=tmp.buffer;}
      if (write_size>=0) {
        ssize_t dtype_size=kno_write_dtype(&tmp,dtype);
        if (dtype_size<0)
          write_size=dtype_size;
        else if (flushp(tmp)) {
          write_size=kno_stream_write(out,tmp.bufwrite-tmp.buffer,tmp.buffer);
          tmp.bufwrite=tmp.buffer;}
        else write_size=dtype_size;}
      if (write_size<0) {
        rv=write_size;
        KNO_STOP_DO_CHOICES;
        break;}
      else bytes=bytes+write_size;}
    if (tmp.bufwrite > tmp.buffer) {
      ssize_t written = kno_stream_write(out,tmp.bufwrite-tmp.buffer,tmp.buffer);
      tmp.bufwrite=tmp.buffer;
      bytes += written;}}
  else {
    bytes=kno_write_dtype(&tmp,dtypes);
    if (bytes>0)
      rv=kno_stream_write (out,tmp.bufwrite-tmp.buffer,tmp.buffer);}
  kno_close_outbuf(&tmp);
  if (rv<0) {
    if (start>=0) {
      int rv = ftruncate(out->stream_fileno,start);
      if (rv<0) {
        int got_err = errno; errno=0;
        u8_log(LOG_WARN,"TruncateFailed",
               "Couldn't undo write to %s (errno=%d:%s)",
               out->streamid,got_err,u8_strerror(got_err));}}
    return rv;}
  else return bytes;
}

static lispval add_dtypes2file(lispval object,lispval filename)
{
  if ((STRINGP(filename))&&
      ((u8_has_suffix(CSTRING(filename),".ztype",1))||
       (u8_has_suffix(CSTRING(filename),".gz",1)))) {
    return add_lisp2zipfile(object,filename);}
  else if (STRINGP(filename)) {
    struct KNO_STREAM *stream;
    if (u8_file_existsp(CSTRING(filename)))
      stream = kno_open_file(CSTRING(filename),KNO_FILE_MODIFY);
    else stream = kno_open_file(CSTRING(filename),KNO_FILE_CREATE);
    if (stream == NULL)
      return KNO_ERROR;
    ssize_t bytes = write_dtypes(object,stream);
    kno_close_stream(stream,KNO_STREAM_CLOSE_FULL);
    u8_free(stream);
    if (bytes<0)
      return KNO_ERROR;
    else return KNO_INT(bytes);}
  else if (TYPEP(filename,kno_stream_type)) {
    struct KNO_STREAM *stream=
      kno_consptr(struct KNO_STREAM *,filename,kno_stream_type);
    ssize_t bytes=write_dtypes(object,stream);
    if (bytes<0)
      return KNO_ERROR;
    else return KNO_INT(bytes);}
  else return kno_type_error(_("string"),"add_dtypes2file",filename);
}

static lispval add_lisp2zipfile(lispval object,lispval filename)
{
  if (STRINGP(filename)) {
    struct KNO_STREAM *out; int bytes;
    if (u8_file_existsp(CSTRING(filename)))
      out = kno_open_file(CSTRING(filename),KNO_FILE_WRITE);
    else out = kno_open_file(CSTRING(filename),KNO_FILE_WRITE);
    if (out == NULL) return KNO_ERROR;
    kno_endpos(out);
    bytes = kno_zwrite_dtype(kno_writebuf(out),object);
    kno_close_stream(out,KNO_STREAM_CLOSE_FULL);
    return KNO_INT(bytes);}
  else if (TYPEP(filename,kno_stream_type)) {
    struct KNO_OUTBUF tmp = { 0 };
    unsigned char tmpbuf[1000];
    KNO_INIT_BYTE_OUTBUF(&tmp,tmpbuf,1000);
    struct KNO_STREAM *stream=
      kno_consptr(struct KNO_STREAM *,filename,kno_stream_type);
    int bytes = kno_zwrite_dtype(&tmp,object);
    if (bytes<0) {
      kno_close_outbuf(&tmp);
      return KNO_ERROR;}
    else {
      kno_stream_write(stream,tmp.bufwrite-tmp.buffer,tmp.buffer);
      kno_close_outbuf(&tmp);
      return KNO_INT(bytes);}}
  else return kno_type_error(_("string"),"add_lisp2zipfile",filename);
}

static lispval zipfile2dtype(lispval filename);

static lispval file2dtype(lispval filename)
{
  if (STRINGP(filename))
    return kno_read_dtype_from_file(CSTRING(filename));
  else if (TYPEP(filename,kno_stream_type)) {
    struct KNO_STREAM *in=
      kno_consptr(struct KNO_STREAM *,filename,kno_stream_type);
    lispval object = kno_read_dtype(kno_readbuf(in));
    if (object == KNO_EOD) return KNO_EOF;
    else return object;}
  else return kno_type_error(_("string"),"read_dtype",filename);
}

static lispval zipfile2dtype(lispval filename)
{
  if (STRINGP(filename)) {
    struct KNO_STREAM *in;
    lispval object = VOID;
    in = kno_open_file(CSTRING(filename),KNO_FILE_READ);
    if (in == NULL) return KNO_ERROR;
    else object = kno_zread_dtype(kno_readbuf(in));
    kno_close_stream(in,KNO_STREAM_CLOSE_FULL);
    u8_free(in);
    return object;}
  else if (TYPEP(filename,kno_stream_type)) {
    struct KNO_STREAM *in=
      kno_consptr(struct KNO_STREAM *,filename,kno_stream_type);
    lispval object = kno_zread_dtype(kno_readbuf(in));
    if (object == KNO_EOD) return KNO_EOF;
    else return object;}
  else return kno_type_error(_("string"),"zipfile2dtype",filename);
}

static lispval zipfile2dtypes(lispval filename);

static lispval file2dtypes(lispval filename)
{
  if ((STRINGP(filename))&&
      ((u8_has_suffix(CSTRING(filename),".ztype",1))||
       (u8_has_suffix(CSTRING(filename),".gz",1)))) {
    return zipfile2dtypes(filename);}
  else if (STRINGP(filename)) {
    struct KNO_STREAM _in, *in =
      kno_init_file_stream(&_in,CSTRING(filename),KNO_FILE_READ,
                          ( (KNO_USE_MMAP) ? (KNO_STREAM_MMAPPED) : (0)),
                          ( (KNO_USE_MMAP) ? (0) : (kno_filestream_bufsize)));
    lispval results = EMPTY, object = VOID;
    if (in == NULL)
      return KNO_ERROR;
    else {
      kno_inbuf inbuf = kno_readbuf(in);
      object = kno_read_dtype(inbuf);
      while (!(KNO_EODP(object))) {
        if (KNO_ABORTP(object)) {
          kno_decref(results);
          kno_close_stream(in,KNO_STREAM_FREEDATA);
          return object;}
        CHOICE_ADD(results,object);
        object = kno_read_dtype(inbuf);}
      kno_close_stream(in,KNO_STREAM_FREEDATA);
      return results;}}
  else return kno_type_error(_("string"),"file2dtypes",filename);
}

static lispval zipfile2dtypes(lispval filename)
{
  if (STRINGP(filename)) {
    struct KNO_STREAM *in = kno_open_file(CSTRING(filename),KNO_FILE_READ);
    lispval results = EMPTY, object = VOID;
    if (in == NULL) return KNO_ERROR;
    else {
      U8_CLEAR_ERRNO();
      kno_inbuf inbuf = kno_readbuf(in);
      object = kno_zread_dtype(inbuf);
      while (!(KNO_EODP(object))) {
        CHOICE_ADD(results,object);
        object = kno_zread_dtype(inbuf);}
      kno_close_stream(in,KNO_STREAM_CLOSE_FULL);
      return results;}}
  else return kno_type_error(_("string"),"zipfile2dtypes",filename);;
}

static lispval open_dtype_output_file(lispval fname)
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
    u8_graberrno("open_dtype_output_file",u8_strdup(filename));
    return KNO_ERROR;}
}

static lispval open_dtype_input_file(lispval fname)
{
  u8_string filename = CSTRING(fname);
  if (!(u8_file_existsp(filename))) {
    kno_seterr(kno_FileNotFound,"open_dtype_input_file",filename,VOID);
    return KNO_ERROR;}
  else {
    struct KNO_STREAM *stream = kno_open_file(filename,KNO_STREAM_READ_ONLY);
    if (stream) {
      U8_CLEAR_ERRNO();
      return (lispval) stream;}
    else return KNO_ERROR_VALUE;}
}

static lispval extend_dtype_file(lispval fname)
{
  u8_string filename = CSTRING(fname);
  struct KNO_STREAM *stream = NULL;
  if (u8_file_existsp(filename))
    stream = kno_open_file(filename,KNO_FILE_MODIFY);
  else stream = kno_open_file(filename,KNO_FILE_CREATE);
  if (stream == NULL)
    return KNO_ERROR_VALUE;
  else {
    U8_CLEAR_ERRNO();
    return (lispval) stream;}
}

static lispval streamp(lispval arg)
{
  if (TYPEP(arg,kno_stream_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval dtype_inputp(lispval arg)
{
  if (TYPEP(arg,kno_stream_type)) {
    struct KNO_STREAM *dts = (kno_stream)arg;
    if (U8_BITP(dts->buf.raw.buf_flags,KNO_IS_WRITING))
      return KNO_FALSE;
    else return KNO_TRUE;}
  else return KNO_FALSE;
}

static lispval dtype_outputp(lispval arg)
{
  if (TYPEP(arg,kno_stream_type)) {
    struct KNO_STREAM *dts = (kno_stream)arg;
    if (U8_BITP(dts->buf.raw.buf_flags,KNO_IS_WRITING))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

/* Streampos prim */

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
  lispval streamprims_module;

  if (scheme_streamprims_initialized) return;
  scheme_streamprims_initialized = 1;
  kno_init_scheme();
  kno_init_drivers();
  streamprims_module =
    kno_new_cmodule("streamprims",(KNO_MODULE_DEFAULT),kno_init_streamprims_c);
  u8_register_source_file(_FILEINFO);

  kno_idefn3(kno_scheme_module,"READ-DTYPE",read_dtype,1,
            "(READ-DTYPE *stream* [*off*] [*len*]) reads "
            "the dtype representation store at *off* in *stream*. "
            "If *off* is not provided, it reads from the current position "
            "of the stream; if *len* is provided, it is a maximum "
            "size of the dtype representation and is used to prefetch "
            "bytes from the file when possible.",
            kno_stream_type,VOID,
            kno_fixnum_type,KNO_VOID,
            kno_fixnum_type,KNO_VOID);
  
  kno_idefn4(kno_scheme_module,"WRITE-DTYPE",write_dtype,2,
            "(WRITE-DTYPE *obj* *stream* [*pos*] [*max*]) writes "
            "a dtype representation of *obj* to *stream* at "
            "file position *pos* *(defaults to the current file "
            "position of the stream). *max*, if provided, "
            "is the maximum size of *obj*'s DType representation. "
            "It is an error if the object has a larger representation "
            "and the value may also be used for allocating temporary buffers, "
            "etc.",
            -1,VOID,kno_stream_type,VOID,
            kno_fixnum_type,KNO_VOID,
            kno_fixnum_type,KNO_VOID);
  
  kno_idefn3(kno_scheme_module,"WRITE-BYTES",write_bytes,2,
            "(WRITE-BYTES *obj* *stream* [*pos*]) writes "
            "the bytes in *obj* to *stream* at *pos*. "
            "*obj* is a string or a packet and *pos* defaults to "
            "the current file position of the stream.",
            -1,VOID,kno_stream_type,VOID,-1,KNO_VOID);
  
  kno_idefn2(kno_scheme_module,"READ-4BYTES",read_4bytes,1,
            "(READ-4BYTES *stream* [*pos*]) reads a bigendian 4-byte integer "
            "from *stream* at *pos* (or the current location, if none)",
            kno_stream_type,VOID,-1,KNO_VOID);
  kno_idefn2(kno_scheme_module,"READ-8BYTES",read_8bytes,1,
            "(READ-8BYTES *stream* [*pos*]) reads a bigendian 8-byte integer "
            "from *stream* at *pos* (or the current location, if none)",
            kno_stream_type,VOID,-1,KNO_VOID);

  kno_idefn3(kno_scheme_module,"READ-BYTES",read_bytes,2,
            "(READ-BYTES *stream* *n*] [*pos*]) reads *n bytes "
            "from *stream* at *pos* (or the current location, if none)",
            kno_stream_type,VOID,-1,KNO_VOID,-1,KNO_VOID);

  kno_idefn3(kno_scheme_module,"WRITE-4BYTES",write_4bytes,2,
            "(WRITE-4BYTES *intval* *stream* [*pos*]) writes a "
            "bigendian 4-byte integer to *stream* at *pos* "
            "(or the current location, if none)",
            -1,KNO_VOID,kno_stream_type,VOID,-1,KNO_VOID);
  kno_idefn3(kno_scheme_module,"WRITE-8BYTES",write_8bytes,2,
            "(WRITE-8BYTES *intval* *stream* [*pos*]) writes a "
            "bigendian 8-byte integer to *stream* at *pos* "
            "(or the current location, if none)",
            -1,KNO_VOID,kno_stream_type,VOID,-1,KNO_VOID);

  kno_defalias(kno_scheme_module,"READ-INT","READ-4BYTES");
  kno_defalias(kno_scheme_module,"WRITE-INT","WRITE-4BYTES");

  kno_idefn(kno_scheme_module,
           kno_make_cprim1x("ZREAD-DTYPE",
                           zread_dtype,1,kno_stream_type,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_cprim2x("ZWRITE-DTYPE",zwrite_dtype,2,
                           -1,VOID,kno_stream_type,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_cprim2x("ZWRITE-DTYPES",zwrite_dtypes,2,
                           -1,VOID,kno_stream_type,VOID));

  kno_idefn(kno_scheme_module,
           kno_make_cprim1x("ZREAD-INT",
                           zread_int,1,kno_stream_type,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_cprim2x("ZWRITE-INT",zwrite_int,2,
                           -1,VOID,kno_stream_type,VOID));

  kno_idefn(streamprims_module,
           kno_make_ndprim(kno_make_cprim3("DTYPE->FILE",lisp2file,2)));
  kno_idefn(streamprims_module,
           kno_make_ndprim(kno_make_cprim2("DTYPES->FILE+",add_dtypes2file,2)));
  kno_defalias(streamprims_module,"DTYPE->FILE+","DTYPES->FILE+");
  kno_idefn(streamprims_module,
           kno_make_ndprim(kno_make_cprim3("DTYPE->ZFILE",lisp2zipfile,2)));
  kno_idefn(streamprims_module,
           kno_make_ndprim(kno_make_cprim2("DTYPE->ZFILE+",add_lisp2zipfile,2)));

  /* We make these aliases because the output file isn't really a zip
     file, but we don't want to break code which uses the old
     names. */
  kno_defalias(streamprims_module,"DTYPE->ZIPFILE","DTYPE->ZFILE");
  kno_defalias(streamprims_module,"DTYPE->ZIPFILE+","DTYPE->ZFILE+");
  kno_idefn(streamprims_module,kno_make_cprim1("FILE->DTYPE",file2dtype,1));
  kno_idefn(streamprims_module,kno_make_cprim1("FILE->DTYPES",file2dtypes,1));
  kno_idefn(streamprims_module,kno_make_cprim1("ZFILE->DTYPE",zipfile2dtype,1));
  kno_idefn(streamprims_module,kno_make_cprim1("ZFILE->DTYPES",zipfile2dtypes,1));
  kno_defalias(streamprims_module,"ZIPFILE->DTYPE","ZFILE->DTYPE");
  kno_defalias(streamprims_module,"ZIPFILE->DTYPES","ZFILE->DTYPES");

  kno_idefn(streamprims_module,
           kno_make_cprim1x("OPEN-DTYPE-FILE",open_dtype_input_file,1,
                           kno_string_type,VOID));
  kno_idefn(streamprims_module,
           kno_make_cprim1x("OPEN-DTYPE-INPUT",open_dtype_input_file,1,
                           kno_string_type,VOID));
  kno_idefn(streamprims_module,
           kno_make_cprim1x("OPEN-DTYPE-OUTPUT",open_dtype_output_file,1,
                           kno_string_type,VOID));
  kno_idefn(streamprims_module,
           kno_make_cprim1x("EXTEND-DTYPE-FILE",extend_dtype_file,1,
                           kno_string_type,VOID));

  kno_idefn(streamprims_module,kno_make_cprim1("DTYPE-STREAM?",streamp,1));
  kno_idefn(streamprims_module,kno_make_cprim1("DTYPE-INPUT?",dtype_inputp,1));
  kno_idefn(streamprims_module,kno_make_cprim1("DTYPE-OUTPUT?",dtype_outputp,1));

  kno_idefn2(streamprims_module,"STREAMPOS",streampos_prim,1,
            "(STREAMPOS *stream* [*setpos*]) gets or sets the position of *stream*",
            kno_stream_type,VOID,-1,VOID);

  kno_idefn2(streamprims_module,"FTRUNCATE",ftruncate_prim,2,
            "(FTRUNCATE *nameorstream* *newsize*) truncates "
            "a file (name or stream) to a particular length",
            -1,VOID,-1,VOID);

  kno_finish_module(streamprims_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
