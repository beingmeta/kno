/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/streams.h"
#include "framerd/dtypeio.h"
#include "framerd/ports.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>
#include <libu8/u8filefns.h>

#include <zlib.h>

#ifndef FD_DTWRITE_SIZE
#define FD_DTWRITE_SIZE 10000
#endif

static lispval read_dtype(lispval stream,lispval pos,lispval len)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  if (FD_VOIDP(pos)) {
    lispval object = fd_read_dtype(fd_readbuf(ds));
    if (object == FD_EOD)
      return FD_EOF;
    else return object;}
  else if (FD_VOIDP(len)) {
    long long filepos = FD_FIX2INT(pos);
    if (filepos<0)
      return fd_type_error("file position","read_type",pos);
    fd_lock_stream(ds);
    fd_setpos(ds,filepos);
    lispval object = fd_read_dtype(fd_readbuf(ds));
    fd_unlock_stream(ds);
    return object;}
  else {
    long long off     = FD_FIX2INT(pos);
    ssize_t   n_bytes = FD_FIX2INT(len);
    struct FD_INBUF _inbuf,
      *in = fd_open_block(ds,&_inbuf,off,n_bytes,0);
    lispval object = fd_read_dtype(in);
    fd_close_inbuf(in);
    return object;}
}

static lispval write_bytes(lispval object,lispval stream,lispval pos)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  if (! ( (FD_VOIDP(pos)) ||
          ( (FD_INTEGERP(pos)) && (fd_numcompare(pos,FD_FIXZERO) >= 0))) )
    return fd_err(fd_TypeError,"write_bytes","filepos",pos);
  const unsigned char *bytes = NULL;
  ssize_t n_bytes = -1;
  if (STRINGP(object)) {
    bytes   = CSTRING(object);
    n_bytes = STRLEN(object);}
  else if (PACKETP(object)) {
    bytes   = FD_PACKET_DATA(object);
    n_bytes = FD_PACKET_LENGTH(object);}
  else return fd_type_error("string or packet","write_bytes",object);
  if (FD_VOIDP(pos)) {
    int rv = fd_write_bytes(fd_writebuf(ds),bytes,n_bytes);
    if (rv<0)
      return FD_ERROR;
    else return FD_INT(n_bytes);}
  int rv = 0;
  fd_off_t filepos = fd_getint(pos);
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
  rv = fd_lock_stream(ds);
  rv = fd_setpos(ds,filepos);
  if (rv>=0) rv = fd_write_bytes(fd_writebuf(ds),bytes,n_bytes);
  rv = fd_unlock_stream(ds);
#endif
  if (rv<0)
    return FD_ERROR;
  else {
    fd_flush_stream(ds);
    fsync(ds->stream_fileno);
    return FD_INT(n_bytes);}
}

static lispval write_dtype(lispval object,lispval stream,
                           lispval pos,lispval max_bytes)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  if (! ( (FD_VOIDP(pos)) ||
          ( (FD_INTEGERP(pos)) && (fd_numcompare(pos,FD_FIXZERO) >= 0))) )
    return fd_err(fd_TypeError,"write_bytes","filepos",pos);
  long long byte_len = (FD_VOIDP(max_bytes)) ? (-1) : (FD_FIX2INT(max_bytes));
  unsigned char *bytes = NULL;
  ssize_t n_bytes = -1, init_len = (byte_len>0) ? (byte_len) : (1000);
  unsigned char *bytebuf =  u8_big_alloc(init_len);
  FD_OUTBUF out = { 0 };
  FD_INIT_OUTBUF(&out,bytebuf,init_len,FD_BIGALLOC_BUFFER);
  n_bytes    = fd_write_dtype(&out,object);
  bytebuf    = out.buffer;
  if (!(FD_VOIDP(max_bytes))) {
    ssize_t max_len = fd_getint(max_bytes);
    if ( (out.bufwrite-out.buffer) > max_len) {
      fd_seterr("TooBig","write_dtype",
                u8_mkstring("The object's DType representation was longer "
                            "than %lld bytes",max_len),
                object);
      fd_close_outbuf(&out);
      return FD_ERROR;}}
  if (FD_VOIDP(pos)) {
    int rv = fd_write_bytes(fd_writebuf(ds),bytes,n_bytes);
    u8_big_free(bytes);
    if (rv<0)
      return FD_ERROR;
    else return FD_INT(n_bytes);}
  int rv = 0;
  fd_off_t filepos = fd_getint(pos);
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
  rv = fd_lock_stream(ds);
  rv = fd_setpos(ds,filepos);
  if (rv>=0) rv = fd_write_bytes(fd_writebuf(ds),bytes,n_bytes);
  rv = fd_unlock_stream(ds);
#endif
  u8_big_free(bytes);
  if (rv<0)
    return FD_ERROR;
  else {
    fd_flush_stream(ds);
    fsync(ds->stream_fileno);
    return FD_INT(n_bytes);}
}

static lispval read_4bytes(lispval stream,lispval pos)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  long long filepos = (FD_VOIDP(pos)) ? (-1) : (fd_getint(pos));
  long long ival = fd_read_4bytes_at(ds,filepos,FD_UNLOCKED);
  if (ival<0)
    return FD_ERROR;
  else return FD_INT(ival);
}

static lispval read_8bytes(lispval stream,lispval pos)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  int err = 0;
  long long filepos = (FD_VOIDP(pos)) ? (-1) : (fd_getint(pos));
  unsigned long long ival = fd_read_8bytes_at(ds,filepos,FD_UNLOCKED,&err);
  if (err)
    return FD_ERROR;
  else return FD_INT(ival);
}

static lispval read_bytes(lispval stream,lispval n,lispval pos)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  long long filepos = (FD_VOIDP(pos)) ? (fd_getpos(ds)) : (fd_getint(pos));
  ssize_t n_bytes;
  if (FD_INTEGERP(n))
    n_bytes = fd_getint(n);
  else return fd_type_error("integer size","read_bytes",n);
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
      return FD_ERROR_VALUE;}}
  if (to_read==0)
    return fd_init_packet(NULL,n_bytes,bytes);
#elif FD_USE_MMAP
  ssize_t page_off = (filepos/512)*512;
  ssize_t map_len  = (pos+n_bytes)-page_off;
  ssize_t buf_off  = filepos - pos;
  unsigned char *mapbuf =
    mmap(NULL,n_bytes,PROT_READ,MAP_PRIVATE,ds->stream_fileno,pos);
  if (mapbuf) {
    memcpy(bytes,mapbuf+buf_off,n_bytes);
    int rv = munmap(mapbuf,map_len);
    if (rv<0) {
      u8_log(LOG_CRIT,fd_failed_unmap,
             "Couldn't unmap buffer for %s (0x%llx)",ds->streamid,ds);}
    else return fd_init_packet(NULL,n_bytes,bytes);}
#endif
  fd_lock_stream(ds);
  if (! (FD_VOIDP(pos)) ) fd_setpos(ds,pos);
  ssize_t result = fd_read_bytes(bytes,fd_readbuf(ds),n_bytes);
  if (result<0) {
    u8_free(bytes);
    return FD_ERROR;}
  else return fd_init_packet(NULL,n_bytes,bytes);
}

static lispval write_4bytes(lispval object,lispval stream,lispval pos)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  long long filepos = (FD_VOIDP(pos)) ? (-1) : (fd_getint(pos));
  long long ival = fd_getint(object);
  if ( (ival < 0) || (ival >= 0x100000000))
    return fd_type_error("positive 4-byte value","write_4bytes",object);
  int n_bytes = fd_write_4bytes_at(ds,ival,filepos);
  if (n_bytes<0)
    return FD_ERROR;
  else return FD_INT(n_bytes);
}

static lispval write_8bytes(lispval object,lispval stream,lispval pos)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  long long filepos = (FD_VOIDP(pos)) ? (-1) : (fd_getint(pos));
  long long ival;
  if (FD_FIXNUMP(object)) {
    ival = FD_FIX2INT(object);
    if (ival<0)
      return fd_type_error("positive 8-byte value","write_8bytes",object);}
  else if (FD_BIGINTP(object)) {
    struct FD_BIGINT *bi = (fd_bigint) object;
    if (fd_bigint_negativep(bi))
      return fd_type_error("positive 4-byte value","write_8bytes",object);
    else if (fd_bigint_fits_in_word_p(bi,8,0))
      ival = fd_bigint_to_ulong_long(bi);
    else return fd_type_error("positive 4-byte value","write_8bytes",object);}
  else return fd_type_error("positive 4-byte value","write_8bytes",object);
  int n_bytes = fd_write_8bytes_at(ds,ival,filepos);
  if (n_bytes<0)
    return FD_ERROR;
  else return FD_INT(n_bytes);
}

static lispval zread_dtype(lispval stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  lispval object = fd_zread_dtype(fd_readbuf(ds));
  if (object == FD_EOD) return FD_EOF;
  else return object;
}

static lispval zwrite_dtype(lispval object,lispval stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  int bytes = fd_zwrite_dtype(fd_writebuf(ds),object);
  if (bytes<0) return FD_ERROR;
  else return FD_INT(bytes);
}

static lispval zwrite_dtypes(lispval object,lispval stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  int bytes = fd_zwrite_dtypes(fd_writebuf(ds),object);
  if (bytes<0) return FD_ERROR;
  else return FD_INT(bytes);
}

static lispval zread_int(lispval stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  unsigned int ival = fd_read_zint(fd_readbuf(ds));
  return FD_INT(ival);
}

static lispval zwrite_int(lispval object,lispval stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  int ival = fd_getint(object);
  int bytes = fd_write_zint(fd_writebuf(ds),ival);
  if (bytes<0) return FD_ERROR;
  else return FD_INT(bytes);
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
    struct FD_STREAM *out = fd_open_file(temp_name,FD_FILE_CREATE);
    struct FD_OUTBUF *outstream = NULL;
    ssize_t bytes;
    if (out == NULL) return FD_ERROR;
    else outstream = fd_writebuf(out);
    if (FIXNUMP(bufsiz))
      fd_setbufsize(out,FIX2INT(bufsiz));
    bytes = fd_write_dtype(outstream,object);
    if (bytes<0) {
      fd_free_stream(out);
      u8_free(temp_name);
      return FD_ERROR;}
    fd_free_stream(out);
    int rv = u8_movefile(temp_name,CSTRING(filename));
    if (rv<0) {
      u8_log(LOG_WARN,"MoveFailed",
             "Couldn't move the completed file into %s, leaving in %s",
             CSTRING(filename),temp_name);
      u8_seterr("MoveFailed","lisp2file",u8_strdup(CSTRING(filename)));
      u8_free(temp_name);
      return FD_ERROR_VALUE;}
    else {
      u8_free(temp_name);
      return FD_INT(bytes);}}
  else if (TYPEP(filename,fd_stream_type)) {
    FD_DECL_OUTBUF(tmp,FD_DTWRITE_SIZE);
    struct FD_STREAM *stream=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    ssize_t bytes = fd_write_dtype(&tmp,object);
    if (bytes<0) {
      fd_close_outbuf(&tmp);
      return FD_ERROR;}
    else {
      bytes=fd_stream_write(stream,bytes,tmp.buffer);
      fd_close_outbuf(&tmp);
      if (bytes<0)
        return FD_ERROR;
      else return FD_INT(bytes);}}
  else return fd_type_error(_("string"),"lisp2file",filename);
}

static lispval lisp2zipfile(lispval object,lispval filename,lispval bufsiz)
{
  if (STRINGP(filename)) {
    u8_string temp_name = u8_mkstring("%s.part",CSTRING(filename));
    struct FD_STREAM *stream = fd_open_file(temp_name,FD_FILE_CREATE);
    fd_outbuf out = NULL;
    int bytes;
    if (stream == NULL) {
      u8_free(temp_name);
      return FD_ERROR;}
    else out = fd_writebuf(stream);
    if (FIXNUMP(bufsiz))
      fd_setbufsize(stream,FIX2INT(bufsiz));
    bytes = fd_zwrite_dtype(out,object);
    if (bytes<0) {
      fd_close_stream(stream,FD_STREAM_CLOSE_FULL);
      u8_free(temp_name);
      return FD_ERROR;}
    fd_close_stream(stream,FD_STREAM_CLOSE_FULL);
    u8_movefile(temp_name,CSTRING(filename));
    u8_free(temp_name);
    return FD_INT(bytes);}
  else if (TYPEP(filename,fd_stream_type)) {
    FD_DECL_OUTBUF(tmp,FD_DTWRITE_SIZE);
    struct FD_STREAM *stream=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    ssize_t bytes = fd_zwrite_dtype(&tmp,object);
    if (bytes<0) {
      fd_close_outbuf(&tmp);
      return FD_ERROR;}
    else {
      bytes=fd_stream_write(stream,bytes,tmp.buffer);
      fd_close_outbuf(&tmp);
      if (bytes<0)
        return FD_ERROR;
      else return FD_INT(bytes);}}
  else return fd_type_error(_("string"),"lisp2zipfile",filename);
}

static lispval add_lisp2zipfile(lispval object,lispval filename);

#define flushp(b) ( (((b).buflim)-((b).bufwrite)) < ((b).buflen)/5 )

static ssize_t write_dtypes(lispval dtypes,struct FD_STREAM *out)
{
  ssize_t bytes=0, rv=0;
  fd_off_t start= fd_endpos(out);
  if ( start != out->stream_maxpos ) start=-1;
  struct FD_OUTBUF tmp = { 0 };
  unsigned char tmpbuf[1000];
  FD_INIT_BYTE_OUTBUF(&tmp,tmpbuf,1000);
  if ((CHOICEP(dtypes))||(PRECHOICEP(dtypes))) {
    /* This writes out the objects sequentially, writing into memory
       first and then to disk, to reduce the danger of malformed
       DTYPEs on the disk. */
    DO_CHOICES(dtype,dtypes) {
      ssize_t write_size=0;
      if ( (flushp(tmp)) ) {
        write_size=fd_stream_write(out,tmp.bufwrite-tmp.buffer,tmp.buffer);
        tmp.bufwrite=tmp.buffer;}
      if (write_size>=0) {
        ssize_t dtype_size=fd_write_dtype(&tmp,dtype);
        if (dtype_size<0)
          write_size=dtype_size;
        else if (flushp(tmp)) {
          write_size=fd_stream_write(out,tmp.bufwrite-tmp.buffer,tmp.buffer);
          tmp.bufwrite=tmp.buffer;}
        else write_size=dtype_size;}
      if (write_size<0) {
        rv=write_size;
        FD_STOP_DO_CHOICES;
        break;}
      else bytes=bytes+write_size;}
    if (tmp.bufwrite > tmp.buffer) {
      ssize_t written = fd_stream_write(out,tmp.bufwrite-tmp.buffer,tmp.buffer);
      tmp.bufwrite=tmp.buffer;
      bytes += written;}}
  else {
    bytes=fd_write_dtype(&tmp,dtypes);
    if (bytes>0)
      rv=fd_stream_write (out,tmp.bufwrite-tmp.buffer,tmp.buffer);}
  fd_close_outbuf(&tmp);
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
    struct FD_STREAM *stream;
    if (u8_file_existsp(CSTRING(filename)))
      stream = fd_open_file(CSTRING(filename),FD_FILE_MODIFY);
    else stream = fd_open_file(CSTRING(filename),FD_FILE_CREATE);
    if (stream == NULL)
      return FD_ERROR;
    ssize_t bytes = write_dtypes(object,stream);
    fd_close_stream(stream,FD_STREAM_CLOSE_FULL);
    u8_free(stream);
    if (bytes<0)
      return FD_ERROR;
    else return FD_INT(bytes);}
  else if (TYPEP(filename,fd_stream_type)) {
    struct FD_STREAM *stream=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    ssize_t bytes=write_dtypes(object,stream);
    if (bytes<0)
      return FD_ERROR;
    else return FD_INT(bytes);}
  else return fd_type_error(_("string"),"add_dtypes2file",filename);
}

static lispval add_lisp2zipfile(lispval object,lispval filename)
{
  if (STRINGP(filename)) {
    struct FD_STREAM *out; int bytes;
    if (u8_file_existsp(CSTRING(filename)))
      out = fd_open_file(CSTRING(filename),FD_FILE_WRITE);
    else out = fd_open_file(CSTRING(filename),FD_FILE_WRITE);
    if (out == NULL) return FD_ERROR;
    fd_endpos(out);
    bytes = fd_zwrite_dtype(fd_writebuf(out),object);
    fd_close_stream(out,FD_STREAM_CLOSE_FULL);
    return FD_INT(bytes);}
  else if (TYPEP(filename,fd_stream_type)) {
    struct FD_OUTBUF tmp = { 0 };
    unsigned char tmpbuf[1000];
    FD_INIT_BYTE_OUTBUF(&tmp,tmpbuf,1000);
    struct FD_STREAM *stream=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    int bytes = fd_zwrite_dtype(&tmp,object);
    if (bytes<0) {
      fd_close_outbuf(&tmp);
      return FD_ERROR;}
    else {
      fd_stream_write(stream,tmp.bufwrite-tmp.buffer,tmp.buffer);
      fd_close_outbuf(&tmp);
      return FD_INT(bytes);}}
  else return fd_type_error(_("string"),"add_lisp2zipfile",filename);
}

static lispval zipfile2dtype(lispval filename);

static lispval file2dtype(lispval filename)
{
  if (STRINGP(filename))
    return fd_read_dtype_from_file(CSTRING(filename));
  else if (TYPEP(filename,fd_stream_type)) {
    struct FD_STREAM *in=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    lispval object = fd_read_dtype(fd_readbuf(in));
    if (object == FD_EOD) return FD_EOF;
    else return object;}
  else return fd_type_error(_("string"),"read_dtype",filename);
}

static lispval zipfile2dtype(lispval filename)
{
  if (STRINGP(filename)) {
    struct FD_STREAM *in;
    lispval object = VOID;
    in = fd_open_file(CSTRING(filename),FD_FILE_READ);
    if (in == NULL) return FD_ERROR;
    else object = fd_zread_dtype(fd_readbuf(in));
    fd_close_stream(in,FD_STREAM_CLOSE_FULL);
    u8_free(in);
    return object;}
  else if (TYPEP(filename,fd_stream_type)) {
    struct FD_STREAM *in=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    lispval object = fd_zread_dtype(fd_readbuf(in));
    if (object == FD_EOD) return FD_EOF;
    else return object;}
  else return fd_type_error(_("string"),"zipfile2dtype",filename);
}

static lispval zipfile2dtypes(lispval filename);

static lispval file2dtypes(lispval filename)
{
  if ((STRINGP(filename))&&
      ((u8_has_suffix(CSTRING(filename),".ztype",1))||
       (u8_has_suffix(CSTRING(filename),".gz",1)))) {
    return zipfile2dtypes(filename);}
  else if (STRINGP(filename)) {
    struct FD_STREAM _in, *in =
      fd_init_file_stream(&_in,CSTRING(filename),FD_FILE_READ,
                          ( (FD_USE_MMAP) ? (FD_STREAM_MMAPPED) : (0)),
                          ( (FD_USE_MMAP) ? (0) : (fd_filestream_bufsize)));
    lispval results = EMPTY, object = VOID;
    if (in == NULL)
      return FD_ERROR;
    else {
      fd_inbuf inbuf = fd_readbuf(in);
      object = fd_read_dtype(inbuf);
      while (!(FD_EODP(object))) {
        if (FD_ABORTP(object)) {
          fd_decref(results);
          fd_close_stream(in,FD_STREAM_FREEDATA);
          return object;}
        CHOICE_ADD(results,object);
        object = fd_read_dtype(inbuf);}
      fd_close_stream(in,FD_STREAM_FREEDATA);
      return results;}}
  else return fd_type_error(_("string"),"file2dtypes",filename);
}

static lispval zipfile2dtypes(lispval filename)
{
  if (STRINGP(filename)) {
    struct FD_STREAM *in = fd_open_file(CSTRING(filename),FD_FILE_READ);
    lispval results = EMPTY, object = VOID;
    if (in == NULL) return FD_ERROR;
    else {
      U8_CLEAR_ERRNO();
      fd_inbuf inbuf = fd_readbuf(in);
      object = fd_zread_dtype(inbuf);
      while (!(FD_EODP(object))) {
        CHOICE_ADD(results,object);
        object = fd_zread_dtype(inbuf);}
      fd_close_stream(in,FD_STREAM_CLOSE_FULL);
      return results;}}
  else return fd_type_error(_("string"),"zipfile2dtypes",filename);;
}

static lispval open_dtype_output_file(lispval fname)
{
  u8_string filename = CSTRING(fname);
  struct FD_STREAM *dts=
    (u8_file_existsp(filename)) ?
    (fd_open_file(filename,FD_FILE_MODIFY)) :
    (fd_open_file(filename,FD_FILE_CREATE));
  if (dts) {
    U8_CLEAR_ERRNO();
    return LISP_CONS(dts);}
  else {
    u8_free(dts);
    u8_graberrno("open_dtype_output_file",u8_strdup(filename));
    return FD_ERROR;}
}

static lispval open_dtype_input_file(lispval fname)
{
  u8_string filename = CSTRING(fname);
  if (!(u8_file_existsp(filename))) {
    fd_seterr(fd_FileNotFound,"open_dtype_input_file",filename,VOID);
    return FD_ERROR;}
  else {
    struct FD_STREAM *stream = fd_open_file(filename,FD_STREAM_READ_ONLY);
    if (stream) {
      U8_CLEAR_ERRNO();
      return (lispval) stream;}
    else return FD_ERROR_VALUE;}
}

static lispval extend_dtype_file(lispval fname)
{
  u8_string filename = CSTRING(fname);
  struct FD_STREAM *stream = NULL;
  if (u8_file_existsp(filename))
    stream = fd_open_file(filename,FD_FILE_MODIFY);
  else stream = fd_open_file(filename,FD_FILE_CREATE);
  if (stream == NULL)
    return FD_ERROR_VALUE;
  else {
    U8_CLEAR_ERRNO();
    return (lispval) stream;}
}

static lispval streamp(lispval arg)
{
  if (TYPEP(arg,fd_stream_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval dtype_inputp(lispval arg)
{
  if (TYPEP(arg,fd_stream_type)) {
    struct FD_STREAM *dts = (fd_stream)arg;
    if (U8_BITP(dts->buf.raw.buf_flags,FD_IS_WRITING))
      return FD_FALSE;
    else return FD_TRUE;}
  else return FD_FALSE;
}

static lispval dtype_outputp(lispval arg)
{
  if (TYPEP(arg,fd_stream_type)) {
    struct FD_STREAM *dts = (fd_stream)arg;
    if (U8_BITP(dts->buf.raw.buf_flags,FD_IS_WRITING))
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

/* Streampos prim */

static lispval streampos_prim(lispval stream_arg,lispval pos)
{
  struct FD_STREAM *stream = (fd_stream)stream_arg;
  if (VOIDP(pos)) {
    fd_off_t curpos = fd_getpos(stream);
    return FD_INT(curpos);}
  else if (FD_ISINT64(pos)) {
    fd_off_t maxpos = fd_endpos(stream);
    long long intval = fd_getint(pos);
    if (intval<0) {
      fd_off_t target = maxpos-(intval+1);
      if ((target>=0)&&(target<maxpos)) {
        fd_off_t result = fd_setpos(stream,target);
        if (result<0) return FD_ERROR;
        else return FD_INT(result);}
      else {
        fd_seterr(_("Out of file range"),"streampos_prim",stream->streamid,pos);
        return FD_ERROR;}}
    else if (intval<maxpos) {
      fd_off_t result = fd_setpos(stream,intval);
      if (result<0) return FD_ERROR;
      else return FD_INT(result);}
    else {
        fd_seterr(_("Out of file range"),"streampos_prim",stream->streamid,pos);
        return FD_ERROR;}}
  else return fd_type_error("stream position","streampos_prim",pos);
}

/* Truncate prim */

static lispval ftruncate_prim(lispval arg,lispval offset)
{
  off_t new_len = -1;
  if (! ( (FD_FIXNUMP(offset)) || (FD_BIGINTP(offset))) )
    return fd_type_error("integer","ftruncate_prim",offset);
  else if (fd_numcompare(offset,FD_FIXZERO)<0)
    return fd_type_error("positive integer","ftruncate_prim",offset);
  else if ( (FD_BIGINTP(new_len)) &&
            ( ! (fd_bigint_fits_in_word_p((fd_bigint)arg,sizeof(new_len),1)) ) )
    return fd_type_error("file size","ftruncate_prim",offset);
  else new_len = fd_getint64(arg);
  if (FD_STRINGP(arg)) {
    char *libc_string = u8_tolibc(FD_CSTRING(arg));
    int rv = truncate(libc_string,new_len);
    if (rv<0) {
      u8_graberrno("ftruncate_prim",u8_strdup(FD_CSTRING(arg)));
      u8_free(libc_string);
      return FD_ERROR_VALUE;}
    else {
      u8_free(libc_string);
      return offset;}}
  else if (FD_TYPEP(arg,fd_stream_type)) {
    fd_stream s = (fd_stream) arg;
    fd_lock_stream(s);
    int fd = s->stream_fileno;
    if (fd<0) {
      u8_seterr("ClosedStream","ftruncate_prim",s->streamid);
      fd_unlock_stream(s);
      return FD_ERROR_VALUE;}
    else {
      fd_off_t old_pos = s->stream_filepos;
      if (old_pos > new_len) fd_setpos(s,new_len);
      int rv = ftruncate(fd,new_len);
      if (rv<0) {
        u8_graberr(errno,"ftruncate_prim",u8_strdup(FD_CSTRING(arg)));
        fd_setpos(s,old_pos);
        fd_unlock_stream(s);
        return FD_ERROR_VALUE;}
      else {
        s->stream_maxpos=new_len;
        if (s->stream_flags & FD_STREAM_MMAPPED)
          fd_reopen_file_stream(s,-1,-1);
        fd_unlock_stream(s);
        return offset;}}}
  else return fd_type_error("Filename or stream","ftruncate_prim",arg);
}

static int scheme_streamprims_initialized = 0;

FD_EXPORT void fd_init_streamprims_c()
{
  lispval streamprims_module;

  if (scheme_streamprims_initialized) return;
  scheme_streamprims_initialized = 1;
  fd_init_scheme();
  fd_init_drivers();
  streamprims_module =
    fd_new_cmodule("STREAMPRIMS",(FD_MODULE_DEFAULT),fd_init_streamprims_c);
  u8_register_source_file(_FILEINFO);

  fd_idefn3(fd_scheme_module,"READ-DTYPE",read_dtype,1,
            "(READ-DTYPE *stream* [*off*] [*len*]) reads "
            "the dtype representation store at *off* in *stream*. "
            "If *off* is not provided, it reads from the current position "
            "of the stream; if *len* is provided, it is a maximum "
            "size of the dtype representation and is used to prefetch "
            "bytes from the file when possible.",
            fd_stream_type,VOID,
            fd_fixnum_type,FD_VOID,
            fd_fixnum_type,FD_VOID);
  
  fd_idefn4(fd_scheme_module,"WRITE-DTYPE",write_dtype,2,
            "(WRITE-DTYPE *obj* *stream* [*pos*] [*max*]) writes "
            "a dtype representation of *obj* to *stream* at "
            "file position *pos* *(defaults to the current file "
            "position of the stream). *max*, if provided, "
            "is the maximum size of *obj*'s DType representation. "
            "It is an error if the object has a larger representation "
            "and the value may also be used for allocating temporary buffers, "
            "etc.",
            -1,VOID,fd_stream_type,VOID,
            fd_fixnum_type,FD_VOID,
            fd_fixnum_type,FD_VOID);
  
  fd_idefn3(fd_scheme_module,"WRITE-BYTES",write_bytes,2,
            "(WRITE-BYTES *obj* *stream* [*pos*]) writes "
            "the bytes in *obj* to *stream* at *pos*. "
            "*obj* is a string or a packet and *pos* defaults to "
            "the current file position of the stream.",
            -1,VOID,fd_stream_type,VOID,-1,FD_VOID);
  
  fd_idefn2(fd_scheme_module,"READ-4BYTES",read_4bytes,1,
            "(READ-4BYTES *stream* [*pos*]) reads a bigendian 4-byte integer "
            "from *stream* at *pos* (or the current location, if none)",
            fd_stream_type,VOID,-1,FD_VOID);
  fd_idefn2(fd_scheme_module,"READ-8BYTES",read_8bytes,1,
            "(READ-8BYTES *stream* [*pos*]) reads a bigendian 8-byte integer "
            "from *stream* at *pos* (or the current location, if none)",
            fd_stream_type,VOID,-1,FD_VOID);

  fd_idefn3(fd_scheme_module,"READ-BYTES",read_bytes,2,
            "(READ-BYTES *stream* *n*] [*pos*]) reads *n bytes "
            "from *stream* at *pos* (or the current location, if none)",
            fd_stream_type,VOID,-1,FD_VOID,-1,FD_VOID);

  fd_idefn3(fd_scheme_module,"WRITE-4BYTES",write_4bytes,2,
            "(WRITE-4BYTES *intval* *stream* [*pos*]) writes a "
            "bigendian 4-byte integer to *stream* at *pos* "
            "(or the current location, if none)",
            -1,FD_VOID,fd_stream_type,VOID,-1,FD_VOID);
  fd_idefn3(fd_scheme_module,"WRITE-8BYTES",write_8bytes,2,
            "(WRITE-8BYTES *intval* *stream* [*pos*]) writes a "
            "bigendian 8-byte integer to *stream* at *pos* "
            "(or the current location, if none)",
            -1,FD_VOID,fd_stream_type,VOID,-1,FD_VOID);

  fd_defalias(fd_scheme_module,"READ-INT","READ-4BYTES");
  fd_defalias(fd_scheme_module,"WRITE-INT","WRITE-4BYTES");

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("ZREAD-DTYPE",
                           zread_dtype,1,fd_stream_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("ZWRITE-DTYPE",zwrite_dtype,2,
                           -1,VOID,fd_stream_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("ZWRITE-DTYPES",zwrite_dtypes,2,
                           -1,VOID,fd_stream_type,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("ZREAD-INT",
                           zread_int,1,fd_stream_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("ZWRITE-INT",zwrite_int,2,
                           -1,VOID,fd_stream_type,VOID));

  fd_idefn(streamprims_module,
           fd_make_ndprim(fd_make_cprim3("DTYPE->FILE",lisp2file,2)));
  fd_idefn(streamprims_module,
           fd_make_ndprim(fd_make_cprim2("DTYPES->FILE+",add_dtypes2file,2)));
  fd_defalias(streamprims_module,"DTYPE->FILE+","DTYPES->FILE+");
  fd_idefn(streamprims_module,
           fd_make_ndprim(fd_make_cprim3("DTYPE->ZFILE",lisp2zipfile,2)));
  fd_idefn(streamprims_module,
           fd_make_ndprim(fd_make_cprim2("DTYPE->ZFILE+",add_lisp2zipfile,2)));

  /* We make these aliases because the output file isn't really a zip
     file, but we don't want to break code which uses the old
     names. */
  fd_defalias(streamprims_module,"DTYPE->ZIPFILE","DTYPE->ZFILE");
  fd_defalias(streamprims_module,"DTYPE->ZIPFILE+","DTYPE->ZFILE+");
  fd_idefn(streamprims_module,fd_make_cprim1("FILE->DTYPE",file2dtype,1));
  fd_idefn(streamprims_module,fd_make_cprim1("FILE->DTYPES",file2dtypes,1));
  fd_idefn(streamprims_module,fd_make_cprim1("ZFILE->DTYPE",zipfile2dtype,1));
  fd_idefn(streamprims_module,fd_make_cprim1("ZFILE->DTYPES",zipfile2dtypes,1));
  fd_defalias(streamprims_module,"ZIPFILE->DTYPE","ZFILE->DTYPE");
  fd_defalias(streamprims_module,"ZIPFILE->DTYPES","ZFILE->DTYPES");

  fd_idefn(streamprims_module,
           fd_make_cprim1x("OPEN-DTYPE-FILE",open_dtype_input_file,1,
                           fd_string_type,VOID));
  fd_idefn(streamprims_module,
           fd_make_cprim1x("OPEN-DTYPE-INPUT",open_dtype_input_file,1,
                           fd_string_type,VOID));
  fd_idefn(streamprims_module,
           fd_make_cprim1x("OPEN-DTYPE-OUTPUT",open_dtype_output_file,1,
                           fd_string_type,VOID));
  fd_idefn(streamprims_module,
           fd_make_cprim1x("EXTEND-DTYPE-FILE",extend_dtype_file,1,
                           fd_string_type,VOID));

  fd_idefn(streamprims_module,fd_make_cprim1("DTYPE-STREAM?",streamp,1));
  fd_idefn(streamprims_module,fd_make_cprim1("DTYPE-INPUT?",dtype_inputp,1));
  fd_idefn(streamprims_module,fd_make_cprim1("DTYPE-OUTPUT?",dtype_outputp,1));

  fd_idefn2(streamprims_module,"STREAMPOS",streampos_prim,1,
            "(STREAMPOS *stream* [*setpos*]) gets or sets the position of *stream*",
            fd_stream_type,VOID,-1,VOID);

  fd_idefn2(streamprims_module,"FTRUNCATE",ftruncate_prim,2,
            "(FTRUNCATE *nameorstream* *newsize*) truncates "
            "a file (name or stream) to a particular length",
            -1,VOID,-1,VOID);

  fd_finish_module(streamprims_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
