/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
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

#ifndef FD_DTREAD_SIZE
#define FD_DTREAD_SIZE 2000
#endif

#ifndef FD_DTWRITE_SIZE
#define FD_DTWRITE_SIZE 2000
#endif

static lispval read_dtype(lispval stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  lispval object = fd_read_dtype(fd_readbuf(ds));
  if (object == FD_EOD) return FD_EOF;
  else return object;
}

static lispval write_dtype(lispval object,lispval stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  int bytes = fd_write_dtype(fd_writebuf(ds),object);
  if (bytes<0) return FD_ERROR;
  else return FD_INT(bytes);
}

static lispval write_bytes(lispval object,lispval stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  if (STRINGP(object)) {
    fd_write_bytes(fd_writebuf(ds),CSTRING(object),STRLEN(object));
    return STRLEN(object);}
  else if (PACKETP(object)) {
    fd_write_bytes
      (fd_writebuf(ds),FD_PACKET_DATA(object),FD_PACKET_LENGTH(object));
    return FD_PACKET_LENGTH(object);}
  else {
    int bytes = fd_write_dtype(fd_writebuf(ds),object);
    if (bytes<0) return FD_ERROR;
    else return FD_INT(bytes);}
}

static lispval read_int(lispval stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  unsigned int ival = fd_read_4bytes_at(ds,-1);
  fd_unlock_stream(ds);
  return FD_INT(ival);
}

static lispval write_int(lispval object,lispval stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  int ival = fd_getint(object);
  int bytes = fd_write_4bytes_at(ds,ival,-1);
  if (bytes<0) return FD_ERROR;
  else return FD_INT(bytes);
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
    struct FD_STREAM *out=
      fd_open_file(temp_name,FD_FILE_CREATE);
    struct FD_OUTBUF *outstream = NULL;
    int bytes;
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
    u8_movefile(temp_name,CSTRING(filename));
    u8_free(temp_name);
    return FD_INT(bytes);}
  else if (FD_TYPEP(filename,fd_stream_type)) {
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
    struct FD_STREAM *stream=
      fd_open_file(temp_name,FD_FILE_CREATE);
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
  else if (FD_TYPEP(filename,fd_stream_type)) {
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

static ssize_t write_dtypes(lispval dtypes,struct FD_STREAM *out)
{
  ssize_t bytes=0, rv=0;
  fd_off_t start= fd_endpos(out);
  if ( start != out->stream_maxpos ) start=-1;
  struct FD_OUTBUF tmp; unsigned char tmpbuf[1000];
  FD_INIT_BYTE_OUTBUF(&tmp,tmpbuf,1000);
  if ((CHOICEP(dtypes))||(PRECHOICEP(dtypes))) {
    DO_CHOICES(dtype,dtypes) {
      ssize_t write_size=0;
      if ((tmp.buflim-tmp.bufwrite)<(tmp.buflen/5)) {
	write_size=fd_stream_write
	  (out,tmp.bufwrite-tmp.buffer,tmp.buffer);
	tmp.bufwrite=tmp.buffer;}
      if (write_size>=0) {
	ssize_t dtype_size=fd_write_dtype(&tmp,dtype);
	if (dtype_size<0)
	  write_size=dtype_size;
	else if ((tmp.buflim-tmp.bufwrite)<(tmp.buflen/5)) {
	  write_size=fd_stream_write
	    (out,tmp.bufwrite-tmp.buffer,tmp.buffer);
	  tmp.bufwrite=tmp.buffer;}
	else write_size=dtype_size;}
      if (write_size<0) {
	rv=write_size;
	FD_STOP_DO_CHOICES;
	break;}
      else bytes=bytes+write_size;}}
  else {
    bytes=fd_write_dtype(&tmp,dtypes);
    if (bytes>0)
      rv=fd_stream_write
	(out,tmp.bufwrite-tmp.buffer,tmp.buffer);}
  fd_close_outbuf(&tmp);
  if (rv<0) {
    if (start>=0)
      ftruncate(out->stream_fileno,start);
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
    if (stream == NULL) return FD_ERROR;
    ssize_t bytes = write_dtypes(object,stream);
    fd_close_stream(stream,FD_STREAM_CLOSE_FULL);
    if (bytes<0) return FD_ERROR;
    else return FD_INT(bytes);}
  else if (FD_TYPEP(filename,fd_stream_type)) {
    struct FD_STREAM *stream=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    ssize_t bytes=write_dtypes(object,stream);
    if (bytes<0) return FD_ERROR;
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
  else if (FD_TYPEP(filename,fd_stream_type)) {
    struct FD_OUTBUF tmp; unsigned char tmpbuf[1000];
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
  else if (FD_TYPEP(filename,fd_stream_type)) {
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
    return object;}
  else if (FD_TYPEP(filename,fd_stream_type)) {
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
    struct FD_STREAM *in = fd_open_file(CSTRING(filename),FD_FILE_READ);
    lispval results = EMPTY, object = VOID;
    if (in == NULL) return FD_ERROR;
    else {
      fd_inbuf inbuf = fd_readbuf(in);
      object = fd_read_dtype(inbuf);
      while (!(FD_EODP(object))) {
	if (FD_ABORTP(object)) {
	  fd_decref(results);
	  return object;}
        CHOICE_ADD(results,object);
        object = fd_read_dtype(inbuf);}
      fd_free_stream(in);
      return results;}}
  else return fd_type_error(_("string"),"file2dtypes",filename);
}

static lispval zipfile2dtypes(lispval filename)
{
  if (STRINGP(filename)) {
    struct FD_STREAM *in=
      fd_open_file(CSTRING(filename),FD_FILE_READ);
    lispval results = EMPTY, object = VOID;
    if (in == NULL) return FD_ERROR;
    else {
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
    u8_graberr(-1,"open_dtype_output_file",u8_strdup(filename));
    return FD_ERROR;}
}

static lispval open_dtype_input_file(lispval fname)
{
  u8_string filename = CSTRING(fname);
  if (!(u8_file_existsp(filename))) {
    fd_seterr(fd_FileNotFound,"open_dtype_input_file",filename,VOID);
    return FD_ERROR;}
  else return (lispval)
	 fd_open_file(filename,FD_STREAM_READ_ONLY);
}

static lispval extend_dtype_file(lispval fname)
{
  u8_string filename = CSTRING(fname);
  if (u8_file_existsp(filename))
    return (lispval)
      fd_open_file(filename,FD_FILE_MODIFY);
  else return (lispval)
	 fd_open_file(filename,FD_FILE_CREATE);
}

static lispval streamp(lispval arg)
{
  if (FD_TYPEP(arg,fd_stream_type)) 
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval dtype_inputp(lispval arg)
{
  if (FD_TYPEP(arg,fd_stream_type)) {
    struct FD_STREAM *dts = (fd_stream)arg;
    if (U8_BITP(dts->buf.raw.buf_flags,FD_IS_WRITING))
      return FD_FALSE;
    else return FD_TRUE;}
  else return FD_FALSE;
}

static lispval dtype_outputp(lispval arg)
{
  if (FD_TYPEP(arg,fd_stream_type)) {
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

static int scheme_streamprims_initialized = 0;

FD_EXPORT void fd_init_streamprims_c()
{
  lispval streamprims_module;

  if (scheme_streamprims_initialized) return;
  scheme_streamprims_initialized = 1;
  fd_init_scheme();
  fd_init_drivers();
  streamprims_module = fd_new_module("STREAMPRIMS",(FD_MODULE_DEFAULT));
  u8_register_source_file(_FILEINFO);

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("READ-DTYPE",read_dtype,1,fd_stream_type,VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("WRITE-DTYPE",write_dtype,2,
			   -1,VOID,fd_stream_type,VOID));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("WRITE-BYTES",write_bytes,2,
			   -1,VOID,fd_stream_type,VOID));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("READ-INT",read_int,1,fd_stream_type,VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("WRITE-INT",write_int,2,
			   -1,VOID,fd_stream_type,VOID));

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
  fd_idefn(streamprims_module,
	   fd_make_cprim1("FILE->DTYPE",file2dtype,1));
  fd_idefn(streamprims_module,
	   fd_make_cprim1("FILE->DTYPES",file2dtypes,1));
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

  fd_idefn(streamprims_module,
	   fd_make_cprim2x("STREAMPOS",streampos_prim,1,
			   fd_stream_type,VOID,
			   -1,VOID));

  fd_finish_module(streamprims_module);
}

