/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/dbfile.h"
#include "fdb/pools.h"
#include "fdb/indices.h"
#include "fdb/frames.h"

static fdtype baseoids_symbol, slotids_symbol;

static fdtype make_file_pool
  (fdtype fname,fdtype base,fdtype capacity,fdtype opt1,fdtype opt2)
{
  fdtype metadata; unsigned int load;
  int retval;
  if (FD_FIXNUMP(opt1)) {
    load=FD_FIX2INT(opt1); metadata=opt2;}
  else {load=0; metadata=opt1;}
  if (FD_VOIDP(metadata)) {}
  else if (!(FD_SLOTMAPP(metadata)))
    return fd_type_error(_("slotmap"),"make_file_pool",metadata);
  retval=fd_make_file_pool(FD_STRDATA(fname),FD_FILE_POOL_MAGIC_NUMBER,
			   FD_OID_ADDR(base),fd_getint(capacity),
			   load,metadata);
  if (retval<0) return fd_erreify();
  else return FD_TRUE;
}

static fdtype make_zpool
  (fdtype fname,fdtype base,fdtype capacity,fdtype opt1,fdtype opt2)
{
  fdtype metadata; unsigned int load;
  int retval;
  if (FD_FIXNUMP(opt1)) {
    load=FD_FIX2INT(opt1); metadata=opt2;}
  else {load=0; metadata=opt1;}
  if (FD_VOIDP(metadata)) {}
  else if (!(FD_SLOTMAPP(metadata)))
    return fd_type_error(_("slotmap"),"make_file_pool",metadata);
  retval=
    fd_make_file_pool(FD_STRDATA(fname),FD_ZPOOL_MAGIC_NUMBER,
		      FD_OID_ADDR(base),fd_getint(capacity),
		      load,metadata);
  if (retval<0) return fd_erreify();
  else return FD_TRUE;
}

static fdtype label_file_pool(fdtype fname,fdtype label)
{
  int retval=-1;
  fd_dtype_stream stream
    =fd_dtsopen(FD_STRING_DATA(fname),FD_DTSTREAM_MODIFY);
  if (stream) {
    off_t endpos=fd_endpos(stream);
    if (endpos>0) {
      int bytes=fd_dtswrite_dtype(stream,label);
      if (bytes>0) {
	fd_setpos(stream,20);
	if (fd_write_4bytes(((fd_byte_output)stream),(unsigned int)endpos)>=0) {
	  retval=1; fd_dtsclose(stream,1);}}}}
  if (retval<0) return fd_erreify();
  else return FD_TRUE;
}

static fdtype make_file_index(fdtype fname,fdtype size,fdtype metadata)
{
  int retval=
    fd_make_file_index(FD_STRDATA(fname),FD_MULT_FILE_INDEX_MAGIC_NUMBER,
		       fd_getint(size),metadata);
  if (retval<0) return fd_erreify();
  else return FD_TRUE;
}

static fdtype make_legacy_file_index(fdtype fname,fdtype size,fdtype metadata)
{
  int retval=
    fd_make_file_index(FD_STRDATA(fname),FD_FILE_INDEX_MAGIC_NUMBER,
		       fd_getint(size),metadata);
  if (retval<0) return fd_erreify();
  else return FD_TRUE;
}

static fdtype make_zindex(fdtype fname,fdtype size,fdtype metadata)
{
  int retval=
    fd_make_file_index(FD_STRDATA(fname),FD_ZINDEX_MAGIC_NUMBER,
		       fd_getint(size),metadata);
  if (retval<0) return fd_erreify();
  else return FD_TRUE;
}

static fdtype make_hash_index(fdtype fname,fdtype size,fdtype slotids,fdtype baseoids,fdtype metadata)
{
  int retval, blocksize=-1; fd_index ix;
  retval=fd_make_hash_index(FD_STRDATA(fname),FD_FIX2INT(size),
			   slotids,baseoids,metadata,-1,-1);
  if (retval<0) return fd_erreify();
  else return FD_VOID;
}

static fdtype populate_hash_index
  (fdtype ix_arg,fdtype from,fdtype blocksize_arg,fdtype keys)
{
  fd_index ix=fd_lisp2index(ix_arg); int blocksize=-1, retval;
  if (!(fd_hash_indexp(ix)))
    return fd_type_error(_("hash index"),"populate_hash_index",ix_arg);
  if (FD_FIXNUMP(blocksize_arg)) blocksize=FD_FIX2INT(blocksize_arg);
  if (FD_VOIDP(keys)) keys=fd_getkeys(from);
  retval=fd_populate_hash_index((struct FD_HASH_INDEX *)ix,from,keys,blocksize);
  if (retval<0) return fd_erreify();
  else return FD_INT2DTYPE(retval);
}

static fdtype hash_index_bucket(fdtype ix_arg,fdtype key,fdtype modulus)
{
  fd_index ix=fd_lisp2index(ix_arg); int bucket;
  if (!(fd_hash_indexp(ix)))
    return fd_type_error(_("hash index"),"populate_hash_index",ix_arg);
  bucket=fd_hash_index_bucket((struct FD_HASH_INDEX *)ix,key,FD_VOIDP(modulus));
  if (FD_FIXNUMP(modulus))
    return FD_INT2DTYPE((bucket%FD_FIX2INT(modulus)));
  else return FD_INT2DTYPE(bucket);
}

/* Hashing functions */

static fdtype lisphashdtype1(fdtype x)
{
  int hash=fd_hash_dtype1(x);
  return FD_INT2DTYPE(hash);
}
static fdtype lisphashdtype2(fdtype x)
{
  int hash=fd_hash_dtype2(x);
  return FD_INT2DTYPE(hash);
}

static fdtype lisphashdtype3(fdtype x)
{
  int hash=fd_hash_dtype3(x);
  return FD_INT2DTYPE(hash);
}

static fdtype lisphashdtyperep(fdtype x)
{
  unsigned int hash=fd_hash_dtype_rep(x);
  return FD_INT2DTYPE(hash);
}

/* The init function */

static int scheme_filedb_initialized=0;

FD_EXPORT void fd_init_filedb_c()
{
  fdtype filedb_module;

  baseoids_symbol=fd_intern("%BASEOIDS");
  slotids_symbol=fd_intern("%SLOTIDS");

  if (scheme_filedb_initialized) return;
  scheme_filedb_initialized=1;
  fd_init_fdscheme();
  fd_init_dbfile();
  filedb_module=fd_new_module("FILEDB",(FD_MODULE_DEFAULT));
  fd_register_source_file(versionid);
  
  fd_idefn(filedb_module,
	   fd_make_cprim3x("MAKE-ZINDEX",make_zindex,2,
			   fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_VOID,
			   fd_slotmap_type,FD_VOID));
  fd_idefn(filedb_module,
	   fd_make_cprim3x("MAKE-FILE-INDEX",make_file_index,2,
			   fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_VOID,
			   fd_slotmap_type,FD_VOID));
  fd_idefn(filedb_module,
	   fd_make_cprim3x("MAKE-LEGACY-FILE-INDEX",make_legacy_file_index,2,
			   fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_VOID,
			   fd_slotmap_type,FD_VOID));
  fd_idefn(filedb_module,
	   fd_make_cprim5x("MAKE-FILE-POOL",make_file_pool,3,
			   fd_string_type,FD_VOID,
			   fd_oid_type,FD_VOID,
			   fd_fixnum_type,FD_VOID,
			   -1,FD_VOID,-1,FD_VOID));
  fd_idefn(filedb_module,
	   fd_make_cprim5x("MAKE-ZPOOL",make_zpool,3,
			   fd_string_type,FD_VOID,
			   fd_oid_type,FD_VOID,
			   fd_fixnum_type,FD_VOID,
			   -1,FD_VOID,-1,FD_VOID));
  fd_idefn(filedb_module,fd_make_cprim2x("LABEL-POOL!",label_file_pool,2,
					 fd_string_type,FD_VOID,
					 fd_string_type,FD_VOID));

  fd_idefn(filedb_module,fd_make_cprim4x("POPULATE-HASH-INDEX",populate_hash_index,2,
					 -1,FD_VOID,-1,FD_VOID,
					 fd_fixnum_type,FD_VOID,-1,FD_VOID));
  fd_idefn(filedb_module,fd_make_cprim5x("MAKE-HASH-INDEX",make_hash_index,2,
					 fd_string_type,FD_VOID,
					 fd_fixnum_type,FD_VOID,
					 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_idefn(filedb_module,fd_make_cprim3x("HASH-INDEX-BUCKET",hash_index_bucket,2,
					 -1,FD_VOID,-1,FD_VOID));


  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE",lisphashdtype2,1));
  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE2",lisphashdtype2,1));
  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE3",lisphashdtype3,1));
  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE1",lisphashdtype1,1));

  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE-REP",lisphashdtyperep,1));

  fd_finish_module(filedb_module);
  fd_persist_module(filedb_module);
}

