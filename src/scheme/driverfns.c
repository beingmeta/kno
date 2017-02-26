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
#include "framerd/fddb.h"
#include "framerd/eval.h"
#include "framerd/ports.h"
#include "framerd/sequences.h"
#include "framerd/drivers.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>

static fdtype baseoids_symbol;

FD_EXPORT fdtype _fd_deprecated_make_file_pool_prim
  (fdtype fname,fdtype base,fdtype capacity,fdtype opt1,fdtype opt2);
FD_EXPORT fdtype _fd_deprecated_label_file_pool_prim(fdtype fname,fdtype label);

FD_EXPORT fdtype _fd_make_oidpool_deprecated(int n,fdtype *args);

FD_EXPORT fdtype _fd_deprecated_make_legacy_file_index_prim(fdtype fname,
                                                            fdtype size,
                                                            fdtype metadata);
FD_EXPORT fdtype _fd_deprecated_make_file_index_prim(fdtype fname,
                                                     fdtype size,
                                                     fdtype metadata);

FD_EXPORT fdtype _fd_make_hash_index_deprecated(fdtype fname,fdtype size,
                                                fdtype slotids,fdtype baseoids,
                                                fdtype metadata,
                                                fdtype flags_arg);
FD_EXPORT fdtype _fd_populate_hash_index_deprecated
  (fdtype ix_arg,fdtype from,fdtype blocksize_arg,fdtype keys);
FD_EXPORT fdtype _fd_hash_index_bucket_deprecated(fdtype ix_arg,fdtype key,fdtype modulus);
FD_EXPORT fdtype _fd_hash_index_stats_deprecated(fdtype ix_arg);
FD_EXPORT fdtype _fd_hash_index_slotids_deprecated(fdtype ix_arg);

/* Cache forcing */

/* This forces the cache into memory for a pool or index.
   It's relevant when the cache is MMAPd. */

static unsigned int load_cache(unsigned int *cache,int length)
{
  unsigned int combo=0;
  int i=0; while (i<length) combo=combo^cache[i++];
  return combo;
}

/* Hashing functions */

static fdtype lisphashdtype1(fdtype x)
{
  int hash=fd_hash_dtype1(x);
  return FD_INT(hash);
}
static fdtype lisphashdtype2(fdtype x)
{
  int hash=fd_hash_dtype2(x);
  return FD_INT(hash);
}

static fdtype lisphashdtype3(fdtype x)
{
  int hash=fd_hash_dtype3(x);
  return FD_INT(hash);
}

static fdtype lisphashdtyperep(fdtype x)
{
  unsigned int hash=fd_hash_dtype_rep(x);
  return FD_INT(hash);
}

/* Opening unregistered file pools */

static fdtype open_file_pool(fdtype name)
{
  fd_pool p=fd_unregistered_file_pool(FD_STRDATA(name));
  if (p) return (fdtype) p;
  else return FD_ERROR_VALUE;
}

static fdtype file_pool_prefetch(fdtype pool,fdtype oids)
{
  fd_pool p=(fd_pool)pool;
  int retval=fd_pool_prefetch(p,oids);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_VOID;
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
      fd_open_stream(temp_name,FD_STREAM_CREATE);
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
    fd_close_stream(out,FD_STREAM_CLOSE_FULL);
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
      fd_open_stream(temp_name,FD_STREAM_CREATE);
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
      stream=fd_open_stream(FD_STRDATA(filename),FD_STREAM_MODIFY);
    else stream=fd_open_stream(FD_STRDATA(filename),FD_STREAM_CREATE);
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
      out=fd_open_stream(FD_STRDATA(filename),FD_STREAM_MODIFY);
    else out=fd_open_stream(FD_STRDATA(filename),FD_STREAM_CREATE);
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
    in=fd_open_stream(FD_STRDATA(filename),FD_STREAM_READ);
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
    struct FD_STREAM *in=
      fd_open_stream(FD_STRDATA(filename),FD_STREAM_READ);
    fdtype results=FD_EMPTY_CHOICE, object=FD_VOID;
    if (in==NULL) return FD_ERROR_VALUE;
    else {
      fd_inbuf inbuf=fd_readbuf(in);
      object=fd_read_dtype(inbuf);
      while (!(FD_EODP(object))) {
        FD_ADD_TO_CHOICE(results,object);
        object=fd_read_dtype(inbuf);}
      fd_close_stream(in,FD_STREAM_CLOSE_FULL);
      return results;}}
  else return fd_type_error(_("string"),"file2dtypes",filename);
}

static fdtype zipfile2dtypes(fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_STREAM *in=
      fd_open_stream(FD_STRDATA(filename),FD_STREAM_READ);
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
    (fd_open_stream(filename,FD_STREAM_MODIFY)) :
    (fd_open_stream(filename,FD_STREAM_CREATE));
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
         fd_open_stream(filename,FD_STREAM_READ_ONLY);
}

static fdtype extend_dtype_file(fdtype fname)
{
  u8_string filename=FD_STRDATA(fname);
  if (u8_file_existsp(filename))
    return (fdtype)
      fd_open_stream(filename,FD_STREAM_MODIFY);
  else return (fdtype)
         fd_open_stream(filename,FD_STREAM_CREATE);
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

/* The init function */

static int scheme_driverfns_initialized=0;

FD_EXPORT void fd_init_driverfns_c()
{
  fdtype driverfns_module;

  baseoids_symbol=fd_intern("%BASEOIDS");

  if (scheme_driverfns_initialized) return;
  scheme_driverfns_initialized=1;
  fd_init_fdscheme();
  fd_init_dbs();
  driverfns_module=fd_new_module("DRIVERFNS",(FD_MODULE_DEFAULT));
  u8_register_source_file(_FILEINFO);

  fd_idefn(driverfns_module,
           fd_make_cprim3x("MAKE-FILE-INDEX",
                           _fd_deprecated_make_file_index_prim,2,
                           fd_string_type,FD_VOID,
                           fd_fixnum_type,FD_VOID,
                           fd_slotmap_type,FD_VOID));
  fd_idefn(driverfns_module,
           fd_make_cprim3x("MAKE-LEGACY-FILE-INDEX",
                           _fd_deprecated_make_legacy_file_index_prim,2,
                           fd_string_type,FD_VOID,
                           fd_fixnum_type,FD_VOID,
                           fd_slotmap_type,FD_VOID));
  fd_idefn(driverfns_module,
           fd_make_cprim5x("MAKE-FILE-POOL",
                           _fd_deprecated_make_file_pool_prim,3,
                           fd_string_type,FD_VOID,
                           fd_oid_type,FD_VOID,
                           fd_fixnum_type,FD_VOID,
                           -1,FD_VOID,-1,FD_VOID));

  fd_idefn(driverfns_module,
           fd_make_cprimn("MAKE-OIDPOOL",_fd_make_oidpool_deprecated,3));

  fd_idefn(driverfns_module,fd_make_cprim2x("LABEL-FILE-POOL!",
                                            _fd_deprecated_label_file_pool_prim,2,
                                            fd_string_type,FD_VOID,
                                            fd_string_type,FD_VOID));
  
  fd_idefn(driverfns_module,fd_make_cprim1x("OPEN-FILE-POOL",open_file_pool,1,
                                         fd_string_type,FD_VOID));
  fd_idefn(driverfns_module,
           fd_make_ndprim
           (fd_make_cprim2x("FILE-POOL-PREFETCH!",file_pool_prefetch,2,
                            fd_raw_pool_type,FD_VOID,-1,FD_VOID)));
  

  fd_idefn(driverfns_module,
           fd_make_cprim4x("POPULATE-HASH-INDEX",
                           _fd_populate_hash_index_deprecated,2,
                           -1,FD_VOID,-1,FD_VOID,
                           fd_fixnum_type,FD_VOID,-1,FD_VOID));
  fd_idefn(driverfns_module,
           fd_make_cprim6x("MAKE-HASH-INDEX",
                           _fd_make_hash_index_deprecated,2,
                           fd_string_type,FD_VOID,
                           fd_fixnum_type,FD_VOID,
                           -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,
                           -1,FD_FALSE));
  fd_idefn(driverfns_module,
           fd_make_cprim3x("HASH-INDEX-BUCKET",
                           _fd_hash_index_bucket_deprecated,2,
                           -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_idefn(driverfns_module,
           fd_make_cprim1("HASH-INDEX-SLOTIDS",_fd_hash_index_slotids_deprecated,1));
  fd_idefn(driverfns_module,
           fd_make_cprim1("HASH-INDEX-STATS",_fd_hash_index_stats_deprecated,1));


  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE",lisphashdtype2,1));
  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE2",lisphashdtype2,1));
  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE3",lisphashdtype3,1));
  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE1",lisphashdtype1,1));

  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE-REP",lisphashdtyperep,1));

  fd_idefn(driverfns_module,
           fd_make_ndprim(fd_make_cprim3("DTYPE->FILE",dtype2file,2)));
  fd_idefn(driverfns_module,
           fd_make_ndprim(fd_make_cprim2("DTYPE->FILE+",add_dtype2file,2)));
  fd_idefn(driverfns_module,
           fd_make_ndprim(fd_make_cprim3("DTYPE->ZFILE",dtype2zipfile,2)));
  fd_idefn(driverfns_module,
           fd_make_ndprim(fd_make_cprim2("DTYPE->ZFILE+",add_dtype2zipfile,2)));

  /* We make these aliases because the output file isn't really a zip
     file, but we don't want to break code which uses the old
     names. */
  fd_defalias(driverfns_module,"DTYPE->ZIPFILE","DTYPE->ZFILE");
  fd_defalias(driverfns_module,"DTYPE->ZIPFILE+","DTYPE->ZFILE+");
  fd_idefn(driverfns_module,
           fd_make_cprim1("FILE->DTYPE",file2dtype,1));
  fd_idefn(driverfns_module,
           fd_make_cprim1("FILE->DTYPES",file2dtypes,1));
  fd_idefn(driverfns_module,fd_make_cprim1("ZFILE->DTYPE",zipfile2dtype,1));
  fd_idefn(driverfns_module,fd_make_cprim1("ZFILE->DTYPES",zipfile2dtypes,1));
  fd_defalias(driverfns_module,"ZIPFILE->DTYPE","ZFILE->DTYPE");
  fd_defalias(driverfns_module,"ZIPFILE->DTYPES","ZFILE->DTYPES");

  fd_idefn(driverfns_module,
           fd_make_cprim1x("OPEN-DTYPE-FILE",open_dtype_input_file,1,
                           fd_string_type,FD_VOID));
  fd_idefn(driverfns_module,
           fd_make_cprim1x("OPEN-DTYPE-INPUT",open_dtype_input_file,1,
                           fd_string_type,FD_VOID));
  fd_idefn(driverfns_module,
           fd_make_cprim1x("OPEN-DTYPE-OUTPUT",open_dtype_output_file,1,
                           fd_string_type,FD_VOID));
  fd_idefn(driverfns_module,
           fd_make_cprim1x("EXTEND-DTYPE-FILE",extend_dtype_file,1,
                           fd_string_type,FD_VOID));

  fd_idefn(driverfns_module,fd_make_cprim1("DTYPE-STREAM?",streamp,1));
  fd_idefn(driverfns_module,fd_make_cprim1("DTYPE-INPUT?",dtype_inputp,1));
  fd_idefn(driverfns_module,fd_make_cprim1("DTYPE-OUTPUT?",dtype_outputp,1));

  fd_finish_module(driverfns_module);
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
