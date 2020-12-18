/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* #define KNO_EVAL_INTERNALS 1 */

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
#include "kno/getsource.h"

#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>
#include <libu8/u8filefns.h>

#include <zlib.h>

#ifndef KNO_DTWRITE_SIZE
#define KNO_DTWRITE_SIZE 10000
#endif

static u8_string get_filedata(u8_string path,ssize_t *lenp)
{
  if (u8_file_existsp(path))
    return u8_filedata(path,lenp);
  else {
    const unsigned char *data = kno_get_source(path,"bytes",NULL,NULL,lenp);
    if (data == NULL) kno_seterr3(kno_FileNotFound,"filestring",path);
    return data;}
}

DEFCPRIM("read-dtype",read_dtype,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "(READ-DTYPE *stream* [*off*] [*len*]) "
	 "reads the dtype representation store at *off* in "
	 "*stream*. If *off* is not provided, it reads from "
	 "the current position of the stream; if *len* is "
	 "provided, it is a maximum size of the dtype "
	 "representation and is used to prefetch bytes from "
	 "the file when possible.",
	 {"stream",kno_stream_type,KNO_VOID},
	 {"pos",kno_fixnum_type,KNO_VOID},
	 {"len",kno_fixnum_type,KNO_VOID})
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

DEFCPRIM("write-dtype",write_dtype,
	 KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "(WRITE-DTYPE *obj* *stream* [*pos*] [*max*]) "
	 "writes a dtype representation of *obj* to "
	 "*stream* at file position *pos* *(defaults to the "
	 "current file position of the stream). *max*, if "
	 "provided, is the maximum size of *obj*'s DType "
	 "representation. It is an error if the object has "
	 "a larger representation and the value may also be "
	 "used for allocating temporary buffers, etc.",
	 {"object",kno_any_type,KNO_VOID},
	 {"stream",kno_stream_type,KNO_VOID},
	 {"pos",kno_fixnum_type,KNO_VOID},
	 {"max_bytes",kno_fixnum_type,KNO_VOID})
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

DEFCPRIM("zread-dtype",zread_dtype,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ZREAD-DTYPE *arg0*)` "
	 "**undocumented**",
	 {"stream",kno_stream_type,KNO_VOID})
static lispval zread_dtype(lispval stream)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  lispval object = kno_zread_dtype(kno_readbuf(ds));
  if (object == KNO_EOD) return KNO_EOF;
  else return object;
}

DEFCPRIM("zwrite-dtype",zwrite_dtype,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(ZWRITE-DTYPE *arg0* *arg1*)` "
	 "**undocumented**",
	 {"object",kno_any_type,KNO_VOID},
	 {"stream",kno_stream_type,KNO_VOID})
static lispval zwrite_dtype(lispval object,lispval stream)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  int bytes = kno_zwrite_dtype(kno_writebuf(ds),object);
  if (bytes<0) return KNO_ERROR;
  else return KNO_INT(bytes);
}

DEFCPRIM("zwrite-dtypes",zwrite_dtypes,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(ZWRITE-DTYPES *arg0* *arg1*)` "
	 "**undocumented**",
	 {"object",kno_any_type,KNO_VOID},
	 {"stream",kno_stream_type,KNO_VOID})
static lispval zwrite_dtypes(lispval object,lispval stream)
{
  struct KNO_STREAM *ds=
    kno_consptr(struct KNO_STREAM *,stream,kno_stream_type);
  int bytes = kno_zwrite_dtypes(kno_writebuf(ds),object);
  if (bytes<0) return KNO_ERROR;
  else return KNO_INT(bytes);
}

/* Reading and writing DTYPEs */

DEFCPRIM("dtype->zfile",lisp2zipfile,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(DTYPE->ZFILE *arg0* *arg1* [*arg2*])` "
	 "**undocumented**",
	 {"object",kno_any_type,KNO_VOID},
	 {"filename",kno_any_type,KNO_VOID},
	 {"bufsiz",kno_any_type,KNO_VOID})
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
      u8_free(stream);
      return KNO_ERROR;}
    kno_close_stream(stream,KNO_STREAM_CLOSE_FULL);
    u8_movefile(temp_name,CSTRING(filename));
    u8_free(temp_name);
    u8_free(stream);
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

DEFCPRIM("dtype->file",lisp2file,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(DTYPE->FILE *arg0* *arg1* [*arg2*])` "
	 "**undocumented**",
	 {"object",kno_any_type,KNO_VOID},
	 {"filename",kno_any_type,KNO_VOID},
	 {"bufsiz",kno_any_type,KNO_VOID})
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

#define flushp(b) ( (((b).buflim)-((b).bufwrite)) < ((b).buflen)/5 )

static ssize_t write_dtypes(lispval dtypes,struct KNO_STREAM *out)
{
  ssize_t bytes=0, rv=0;
  kno_off_t start= kno_endpos(out);
  if ( start != out->stream_maxpos ) start=-1;
  struct KNO_OUTBUF tmp = { 0 };
  unsigned char tmpbuf[1000];
  KNO_INIT_BYTE_OUTBUF(&tmp,tmpbuf,1000);
  if (CHOICEP(dtypes)) {
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

DEFCPRIM("dtype->zfile+",add_lisp2zipfile,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(DTYPE->ZFILE+ *arg0* *arg1*)` "
	 "**undocumented**",
	 {"object",kno_any_type,KNO_VOID},
	 {"filename",kno_any_type,KNO_VOID})
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

DEFCPRIM("dtypes->file+",add_dtypes2file,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(DTYPES->FILE+ *arg0* *arg1*)` "
	 "**undocumented**",
	 {"object",kno_any_type,KNO_VOID},
	 {"filename",kno_any_type,KNO_VOID})
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

DEFCPRIM("zfile->dtype",zipfile2dtype,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ZFILE->DTYPE *arg0*)` "
	 "**undocumented**",
	 {"filename",kno_any_type,KNO_VOID})
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

DEFCPRIM("file->dtype",file2dtype,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE->DTYPE *arg0*)` "
	 "**undocumented**",
	 {"filename",kno_any_type,KNO_VOID})
static lispval file2dtype(lispval filename)
{
  if (STRINGP(filename))
    return kno_read_dtype_from_file(CSTRING(filename));
  else if (TYPEP(filename,kno_stream_type)) {
    struct KNO_STREAM *in=
      kno_consptr(struct KNO_STREAM *,filename,kno_stream_type);
    lispval object = kno_read_dtype(kno_readbuf(in));
    if (object == KNO_EOD) return KNO_EOF;
    if ( (ABORTED(object)) && (u8_current_exception==NULL))
      kno_undeclared_error("zipfile2dtype",in->streamid,filename);
    return object;}
  else return kno_type_error(_("string"),"read_dtype",filename);
}

DEFCPRIM("zfile->dtypes",zipfile2dtypes,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ZFILE->DTYPES *arg0*)` "
	 "**undocumented**",
	 {"filename",kno_any_type,KNO_VOID})
static lispval zipfile2dtypes(lispval filename)
{
  if (STRINGP(filename)) {
    if (u8_file_existsp(KNO_CSTRING(filename))) {
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
    else {
      ssize_t len = -1;
      u8_string sourcepath = KNO_CSTRING(filename);
      const unsigned char *filedata = get_filedata(sourcepath,&len);
      if (filedata == NULL) {
	kno_seterr3(kno_FileNotFound,"open_stream",sourcepath);
	return KNO_ERROR;}
      struct KNO_STREAM _in, *in =
	kno_init_byte_stream(&_in,sourcepath,KNO_FILE_READ,len,filedata);
      if (in==NULL) return KNO_ERROR;
      kno_inbuf inbuf = kno_readbuf(in);
      lispval results = EMPTY;
      lispval object = kno_zread_dtype(inbuf);
      while (!(KNO_EODP(object))) {
	if (KNO_ABORTP(object)) {
	  kno_decref(results);
	  kno_close_stream(in,KNO_STREAM_FREEDATA);
	  return object;}
	CHOICE_ADD(results,object);
	object = kno_zread_dtype(inbuf);}
      kno_close_stream(in,KNO_STREAM_FREEDATA);
      return results;}}
  else return kno_type_error(_("string"),"zipfile2dtypes",filename);;
}

DEFCPRIM("file->dtypes",file2dtypes,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE->DTYPES *arg0*)` "
	 "**undocumented**",
	 {"filename",kno_any_type,KNO_VOID})
static lispval file2dtypes(lispval filename)
{
  if ((STRINGP(filename))&&
      ((u8_has_suffix(CSTRING(filename),".ztype",1))||
       (u8_has_suffix(CSTRING(filename),".gz",1)))) {
    return zipfile2dtypes(filename);}
  else if (STRINGP(filename)) {
    if (u8_file_existsp(KNO_CSTRING(filename))) {
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
    else {
      ssize_t len = -1;
      u8_string sourcepath = KNO_CSTRING(filename);
      const unsigned char *filedata = get_filedata(sourcepath,&len);
      if (filedata == NULL) {
	kno_seterr3(kno_FileNotFound,"open_stream",sourcepath);
	return KNO_ERROR;}
      struct KNO_STREAM _in, *in =
	kno_init_byte_stream(&_in,sourcepath,KNO_FILE_READ,len,filedata);
      if (in==NULL) return KNO_ERROR;
      kno_inbuf inbuf = kno_readbuf(in);
      lispval results = EMPTY;
      lispval object = kno_read_dtype(inbuf);
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

static int scheme_dtypeprims_initialized = 0;

lispval dtypeprims_module;

KNO_EXPORT void kno_init_dtypeprims_c()
{
  if (scheme_dtypeprims_initialized) return;
  scheme_dtypeprims_initialized = 1;
  kno_init_scheme();
  kno_init_drivers();
  dtypeprims_module =
    kno_new_cmodule("dtypeprims",(KNO_MODULE_DEFAULT),kno_init_dtypeprims_c);
  u8_register_source_file(_FILEINFO);

  link_local_cprims();

  kno_finish_module(dtypeprims_module);
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("zfile->dtypes",zipfile2dtypes,1,dtypeprims_module);
  KNO_LINK_CPRIM("file->dtypes",file2dtypes,1,dtypeprims_module);
  KNO_LINK_CPRIM("zfile->dtypes",zipfile2dtypes,1,dtypeprims_module);
  KNO_LINK_CPRIM("zfile->dtype",zipfile2dtype,1,dtypeprims_module);
  KNO_LINK_CPRIM("file->dtype",file2dtype,1,dtypeprims_module);
  KNO_LINK_CPRIM("zfile->dtype",zipfile2dtype,1,dtypeprims_module);
  KNO_LINK_CPRIM("dtype->zfile+",add_lisp2zipfile,2,dtypeprims_module);
  KNO_LINK_CPRIM("dtypes->file+",add_dtypes2file,2,dtypeprims_module);
  KNO_LINK_CPRIM("dtype->zfile+",add_lisp2zipfile,2,dtypeprims_module);
  KNO_LINK_CPRIM("dtype->zfile",lisp2zipfile,3,dtypeprims_module);
  KNO_LINK_CPRIM("dtype->file",lisp2file,3,dtypeprims_module);
  KNO_LINK_CPRIM("dtype->zfile",lisp2zipfile,3,dtypeprims_module);
  KNO_LINK_CPRIM("zwrite-dtypes",zwrite_dtypes,2,kno_scheme_module);
  KNO_LINK_CPRIM("zwrite-dtype",zwrite_dtype,2,kno_scheme_module);
  KNO_LINK_CPRIM("zread-dtype",zread_dtype,1,kno_scheme_module);
  KNO_LINK_CPRIM("write-dtype",write_dtype,4,kno_scheme_module);
  KNO_LINK_CPRIM("read-dtype",read_dtype,3,kno_scheme_module);

  KNO_LINK_ALIAS("dtype->file+",add_dtypes2file,dtypeprims_module);
  KNO_LINK_ALIAS("dtype->zipfile",lisp2zipfile,dtypeprims_module);
  KNO_LINK_ALIAS("dtype->zipfile+",add_lisp2zipfile,dtypeprims_module);
  KNO_LINK_ALIAS("zipfile->dtype",zipfile2dtype,dtypeprims_module);
  KNO_LINK_ALIAS("zipfile->dtypes",zipfile2dtypes,dtypeprims_module);

}
