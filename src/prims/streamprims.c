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
#include "framerd/fddb.h"
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

#define FD_DEFAULT_ZLEVEL 9

static fdtype read_dtype(fdtype stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  fdtype object=fd_read_dtype(fd_readbuf(ds));
  if (object == FD_EOD) return FD_EOF;
  else return object;
}

static fdtype write_dtype(fdtype object,fdtype stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  int bytes=fd_write_dtype(fd_writebuf(ds),object);
  if (bytes<0) return FD_ERROR_VALUE;
  else return FD_INT(bytes);
}

static fdtype write_bytes(fdtype object,fdtype stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  if (FD_STRINGP(object)) {
    fd_write_bytes(fd_writebuf(ds),FD_STRDATA(object),FD_STRLEN(object));
    return FD_STRLEN(object);}
  else if (FD_PACKETP(object)) {
    fd_write_bytes
      (fd_writebuf(ds),FD_PACKET_DATA(object),FD_PACKET_LENGTH(object));
    return FD_PACKET_LENGTH(object);}
  else {
    int bytes=fd_write_dtype(fd_writebuf(ds),object);
    if (bytes<0) return FD_ERROR_VALUE;
    else return FD_INT(bytes);}
}

static fdtype read_int(fdtype stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  unsigned int ival=fd_read_4bytes_at(ds,-1);
  fd_unlock_stream(ds);
  return FD_INT(ival);
}

static fdtype write_int(fdtype object,fdtype stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  int ival=fd_getint(object);
  int bytes=fd_write_4bytes_at(ds,ival,-1);
  if (bytes<0) return FD_ERROR_VALUE;
  else return FD_INT(bytes);
}

static fdtype zread_dtype(fdtype stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  fdtype object=fd_zread_dtype(fd_readbuf(ds));
  if (object == FD_EOD) return FD_EOF;
  else return object;
}

static fdtype zwrite_dtype(fdtype object,fdtype stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  int bytes=fd_zwrite_dtype(fd_writebuf(ds),object);
  if (bytes<0) return FD_ERROR_VALUE;
  else return FD_INT(bytes);
}

static fdtype zwrite_dtypes(fdtype object,fdtype stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  int bytes=fd_zwrite_dtypes(fd_writebuf(ds),object);
  if (bytes<0) return FD_ERROR_VALUE;
  else return FD_INT(bytes);
}

static fdtype zread_int(fdtype stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  unsigned int ival=fd_read_zint(fd_readbuf(ds));
  return FD_INT(ival);
}

static fdtype zwrite_int(fdtype object,fdtype stream)
{
  struct FD_STREAM *ds=
    fd_consptr(struct FD_STREAM *,stream,fd_stream_type);
  int ival=fd_getint(object);
  int bytes=fd_write_zint(fd_writebuf(ds),ival);
  if (bytes<0) return FD_ERROR_VALUE;
  else return FD_INT(bytes);
}

/* Reading and writing DTYPEs */

static fdtype dtype2zipfile(fdtype object,fdtype filename,fdtype bufsiz);

static fdtype dtype2file(fdtype object,fdtype filename,fdtype bufsiz)
{
  if ((FD_STRINGP(filename))&&
      ((u8_has_suffix(FD_STRDATA(filename),".ztype",1))||
       (u8_has_suffix(FD_STRDATA(filename),".gz",1)))) {
    return dtype2zipfile(object,filename,bufsiz);}
  else if (FD_STRINGP(filename)) {
    u8_string temp_name=u8_mkstring("%s.part",FD_STRDATA(filename));
    struct FD_STREAM *out=
      fd_open_file(temp_name,FD_FILE_CREATE);
    struct FD_OUTBUF *outstream=NULL;
    int bytes;
    if (out==NULL) return FD_ERROR_VALUE;
    else outstream=fd_writebuf(out);
    if (FD_FIXNUMP(bufsiz))
      fd_stream_setbuf(out,FD_FIX2INT(bufsiz));
    bytes=fd_write_dtype(outstream,object);
    if (bytes<0) {
      fd_free_stream(out);
      u8_free(temp_name);
      return FD_ERROR_VALUE;}
    fd_free_stream(out);
    u8_movefile(temp_name,FD_STRDATA(filename));
    u8_free(temp_name);
    return FD_INT(bytes);}
  else if (FD_TYPEP(filename,fd_stream_type)) {
    struct FD_STREAM *stream=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    int bytes=fd_write_dtype(fd_writebuf(stream),object);
    if (bytes<0) return FD_ERROR_VALUE;
    else return FD_INT(bytes);}
  else return fd_type_error(_("string"),"dtype2file",filename);
}

static fdtype dtype2zipfile(fdtype object,fdtype filename,fdtype bufsiz)
{
  if (FD_STRINGP(filename)) {
    u8_string temp_name=u8_mkstring("%s.part",FD_STRDATA(filename));
    struct FD_STREAM *stream=
      fd_open_file(temp_name,FD_FILE_CREATE);
    fd_outbuf out=NULL;
    int bytes;
    if (stream==NULL) {
      u8_free(temp_name);
      return FD_ERROR_VALUE;}
    else out=fd_writebuf(stream);
    if (FD_FIXNUMP(bufsiz))
      fd_stream_setbuf(stream,FD_FIX2INT(bufsiz));
    bytes=fd_zwrite_dtype(out,object);
    if (bytes<0) {
      fd_close_stream(stream,FD_STREAM_CLOSE_FULL);
      u8_free(temp_name);
      return FD_ERROR_VALUE;}
    fd_close_stream(stream,FD_STREAM_CLOSE_FULL);
    u8_movefile(temp_name,FD_STRDATA(filename));
    u8_free(temp_name);
    return FD_INT(bytes);}
  else if (FD_TYPEP(filename,fd_stream_type)) {
    struct FD_STREAM *out=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    int bytes=fd_zwrite_dtype(fd_writebuf(out),object);
    if (bytes<0) return FD_ERROR_VALUE;
    else return FD_INT(bytes);}
  else return fd_type_error(_("string"),"dtype2zipfile",filename);
}

static fdtype add_dtype2zipfile(fdtype object,fdtype filename);

static fdtype add_dtype2file(fdtype object,fdtype filename)
{
  if ((FD_STRINGP(filename))&&
      ((u8_has_suffix(FD_STRDATA(filename),".ztype",1))||
       (u8_has_suffix(FD_STRDATA(filename),".gz",1)))) {
    return add_dtype2zipfile(object,filename);}
  else if (FD_STRINGP(filename)) {
    struct FD_STREAM *stream; int bytes;
    struct FD_OUTBUF *out;
    if (u8_file_existsp(FD_STRDATA(filename)))
      stream=fd_open_file(FD_STRDATA(filename),FD_FILE_MODIFY);
    else stream=fd_open_file(FD_STRDATA(filename),FD_FILE_CREATE);
    if (stream==NULL) return FD_ERROR_VALUE;
    else out=fd_writebuf(stream);
    fd_endpos(stream);
    bytes=fd_write_dtype(out,object);
    fd_close_stream(stream,FD_STREAM_CLOSE_FULL);
    return FD_INT(bytes);}
  else if (FD_TYPEP(filename,fd_stream_type)) {
    struct FD_STREAM *out=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    int bytes=fd_write_dtype(fd_writebuf(out),object);
    if (bytes<0) return FD_ERROR_VALUE;
    else return FD_INT(bytes);}
  else return fd_type_error(_("string"),"add_dtype2file",filename);
}

static fdtype add_dtype2zipfile(fdtype object,fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_STREAM *out; int bytes;
    if (u8_file_existsp(FD_STRDATA(filename)))
      out=fd_open_file(FD_STRDATA(filename),FD_FILE_WRITE);
    else out=fd_open_file(FD_STRDATA(filename),FD_FILE_WRITE);
    if (out==NULL) return FD_ERROR_VALUE;
    fd_endpos(out);
    bytes=fd_zwrite_dtype(fd_writebuf(out),object);
    fd_close_stream(out,FD_STREAM_CLOSE_FULL);
    return FD_INT(bytes);}
  else if (FD_TYPEP(filename,fd_stream_type)) {
    struct FD_STREAM *out=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    int bytes=fd_zwrite_dtype(fd_writebuf(out),object);
    if (bytes<0) return FD_ERROR_VALUE;
    else return FD_INT(bytes);}
  else return fd_type_error(_("string"),"add_dtype2zipfile",filename);
}

static fdtype zipfile2dtype(fdtype filename);

static fdtype file2dtype(fdtype filename)
{
  if (FD_STRINGP(filename)) 
    return fd_read_dtype_from_file(FD_STRDATA(filename));
  else if (FD_TYPEP(filename,fd_stream_type)) {
    struct FD_STREAM *in=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    fdtype object=fd_read_dtype(fd_readbuf(in));
    if (object == FD_EOD) return FD_EOF;
    else return object;}
  else return fd_type_error(_("string"),"read_dtype",filename);
}

static fdtype zipfile2dtype(fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_STREAM *in;
    fdtype object=FD_VOID;
    in=fd_open_file(FD_STRDATA(filename),FD_FILE_READ);
    if (in==NULL) return FD_ERROR_VALUE;
    else object=fd_zread_dtype(fd_readbuf(in));
    fd_close_stream(in,FD_STREAM_CLOSE_FULL);
    return object;}
  else if (FD_TYPEP(filename,fd_stream_type)) {
    struct FD_STREAM *in=
      fd_consptr(struct FD_STREAM *,filename,fd_stream_type);
    fdtype object=fd_zread_dtype(fd_readbuf(in));
    if (object == FD_EOD) return FD_EOF;
    else return object;}
  else return fd_type_error(_("string"),"zipfile2dtype",filename);
}

static fdtype zipfile2dtypes(fdtype filename);

static fdtype file2dtypes(fdtype filename)
{
  if ((FD_STRINGP(filename))&&
      ((u8_has_suffix(FD_STRDATA(filename),".ztype",1))||
       (u8_has_suffix(FD_STRDATA(filename),".gz",1)))) {
    return zipfile2dtypes(filename);}
  else if (FD_STRINGP(filename)) {
    struct FD_STREAM *in=fd_open_file(FD_STRDATA(filename),FD_FILE_READ);
    fdtype results=FD_EMPTY_CHOICE, object=FD_VOID;
    if (in==NULL) return FD_ERROR_VALUE;
    else {
      fd_inbuf inbuf=fd_readbuf(in);
      object=fd_read_dtype(inbuf);
      while (!(FD_EODP(object))) {
        FD_ADD_TO_CHOICE(results,object);
        object=fd_read_dtype(inbuf);}
      fd_free_stream(in);
      return results;}}
  else return fd_type_error(_("string"),"file2dtypes",filename);
}

static fdtype zipfile2dtypes(fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_STREAM *in=
      fd_open_file(FD_STRDATA(filename),FD_FILE_READ);
    fdtype results=FD_EMPTY_CHOICE, object=FD_VOID;
    if (in==NULL) return FD_ERROR_VALUE;
    else {
      fd_inbuf inbuf=fd_readbuf(in);
      object=fd_zread_dtype(inbuf);
      while (!(FD_EODP(object))) {
        FD_ADD_TO_CHOICE(results,object);
        object=fd_zread_dtype(inbuf);}
      fd_close_stream(in,FD_STREAM_CLOSE_FULL);
      return results;}}
  else return fd_type_error(_("string"),"zipfile2dtypes",filename);;
}

static fdtype open_dtype_output_file(fdtype fname)
{
  u8_string filename=FD_STRDATA(fname);
  struct FD_STREAM *dts=
    (u8_file_existsp(filename)) ?
    (fd_open_file(filename,FD_FILE_MODIFY)) :
    (fd_open_file(filename,FD_FILE_CREATE));
  if (dts) {
    U8_CLEAR_ERRNO();
    return FDTYPE_CONS(dts);}
  else {
    u8_free(dts);
    u8_graberr(-1,"open_dtype_output_file",u8_strdup(filename));
    return FD_ERROR_VALUE;}
}

static fdtype open_dtype_input_file(fdtype fname)
{
  u8_string filename=FD_STRDATA(fname);
  if (!(u8_file_existsp(filename))) {
    fd_seterr(fd_FileNotFound,"open_dtype_input_file",
              u8_strdup(filename),FD_VOID);
    return FD_ERROR_VALUE;}
  else return (fdtype)
	 fd_open_file(filename,FD_STREAM_READ_ONLY);
}

static fdtype extend_dtype_file(fdtype fname)
{
  u8_string filename=FD_STRDATA(fname);
  if (u8_file_existsp(filename))
    return (fdtype)
      fd_open_file(filename,FD_FILE_MODIFY);
  else return (fdtype)
	 fd_open_file(filename,FD_FILE_CREATE);
}

static fdtype streamp(fdtype arg)
{
  if (FD_TYPEP(arg,fd_stream_type)) 
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype dtype_inputp(fdtype arg)
{
  if (FD_TYPEP(arg,fd_stream_type)) {
    struct FD_STREAM *dts=(fd_stream)arg;
    if (U8_BITP(dts->buf.raw.buf_flags,FD_IS_WRITING))
      return FD_FALSE;
    else return FD_TRUE;}
  else return FD_FALSE;
}

static fdtype dtype_outputp(fdtype arg)
{
  if (FD_TYPEP(arg,fd_stream_type)) {
    struct FD_STREAM *dts=(fd_stream)arg;
    if (U8_BITP(dts->buf.raw.buf_flags,FD_IS_WRITING))
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

/* Streampos prim */

static fdtype streampos_prim(fdtype stream_arg,fdtype pos)
{
  struct FD_STREAM *stream=(fd_stream)stream_arg;
  if (FD_VOIDP(pos)) {
    fd_off_t curpos=fd_getpos(stream);
    return FD_INT(curpos);}
  else if (FD_ISINT64(pos)) {
    fd_off_t maxpos=fd_endpos(stream);
    long long intval=fd_getint(pos);
    if (intval<0) {
      fd_off_t target=maxpos-(intval+1);
      if ((target>=0)&&(target<maxpos)) {
	fd_off_t result=fd_setpos(stream,target);
	if (result<0) return FD_ERROR_VALUE;
	else return FD_INT(result);}
      else {
	fd_seterr(_("Out of file range"),"streampos_prim",
		  stream->streamid,pos);
	return FD_ERROR_VALUE;}}
    else if (intval<maxpos) {
      fd_off_t result=fd_setpos(stream,intval);
      if (result<0) return FD_ERROR_VALUE;
      else return FD_INT(result);}
    else {
	fd_seterr(_("Out of file range"),"streampos_prim",
		  stream->streamid,pos);
	return FD_ERROR_VALUE;}}
  else return fd_type_error("stream position","streampos_prim",pos);
}

static int scheme_streamprims_initialized=0;

FD_EXPORT void fd_init_streamprims_c()
{
  fdtype streamprims_module;

  if (scheme_streamprims_initialized) return;
  scheme_streamprims_initialized=1;
  fd_init_fdscheme();
  fd_init_dbs();
  streamprims_module=fd_new_module("STREAMPRIMS",(FD_MODULE_DEFAULT));
  u8_register_source_file(_FILEINFO);

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("READ-DTYPE",read_dtype,1,fd_stream_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("WRITE-DTYPE",write_dtype,2,
			   -1,FD_VOID,fd_stream_type,FD_VOID));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("WRITE-BYTES",write_bytes,2,
			   -1,FD_VOID,fd_stream_type,FD_VOID));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("READ-INT",read_int,1,fd_stream_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("WRITE-INT",write_int,2,
			   -1,FD_VOID,fd_stream_type,FD_VOID));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("ZREAD-DTYPE",
			   zread_dtype,1,fd_stream_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("ZWRITE-DTYPE",zwrite_dtype,2,
			   -1,FD_VOID,fd_stream_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("ZWRITE-DTYPES",zwrite_dtypes,2,
			   -1,FD_VOID,fd_stream_type,FD_VOID));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("ZREAD-INT",
			   zread_int,1,fd_stream_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("ZWRITE-INT",zwrite_int,2,
			   -1,FD_VOID,fd_stream_type,FD_VOID));

  fd_idefn(streamprims_module,
	   fd_make_ndprim(fd_make_cprim3("DTYPE->FILE",dtype2file,2)));
  fd_idefn(streamprims_module,
	   fd_make_ndprim(fd_make_cprim2("DTYPE->FILE+",add_dtype2file,2)));
  fd_idefn(streamprims_module,
	   fd_make_ndprim(fd_make_cprim3("DTYPE->ZFILE",dtype2zipfile,2)));
  fd_idefn(streamprims_module,
	   fd_make_ndprim(fd_make_cprim2("DTYPE->ZFILE+",add_dtype2zipfile,2)));

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
			   fd_string_type,FD_VOID));
  fd_idefn(streamprims_module,
	   fd_make_cprim1x("OPEN-DTYPE-INPUT",open_dtype_input_file,1,
			   fd_string_type,FD_VOID));
  fd_idefn(streamprims_module,
	   fd_make_cprim1x("OPEN-DTYPE-OUTPUT",open_dtype_output_file,1,
			   fd_string_type,FD_VOID));
  fd_idefn(streamprims_module,
	   fd_make_cprim1x("EXTEND-DTYPE-FILE",extend_dtype_file,1,
			   fd_string_type,FD_VOID));

  fd_idefn(streamprims_module,fd_make_cprim1("DTYPE-STREAM?",streamp,1));
  fd_idefn(streamprims_module,fd_make_cprim1("DTYPE-INPUT?",dtype_inputp,1));
  fd_idefn(streamprims_module,fd_make_cprim1("DTYPE-OUTPUT?",dtype_outputp,1));

  fd_idefn(streamprims_module,
	   fd_make_cprim2x("STREAMPOS",streampos_prim,1,
			   fd_stream_type,FD_VOID,
			   -1,FD_VOID));

  fd_finish_module(streamprims_module);
}

