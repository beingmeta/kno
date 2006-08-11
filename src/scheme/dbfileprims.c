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

static fdtype make_file_pool
  (fdtype fname,fdtype base,fdtype capacity,fdtype metadata)
{
  int retval=
    fd_make_file_pool(FD_STRDATA(fname),FD_FILE_POOL_MAGIC_NUMBER,
		      FD_OID_ADDR(base),fd_getint(capacity),
		      metadata);
  if (retval<0) return fd_erreify();
  else return FD_TRUE;
}

static fdtype make_zpool
  (fdtype fname,fdtype base,fdtype capacity,fdtype metadata)
{
  int retval=
    fd_make_file_pool(FD_STRDATA(fname),FD_ZPOOL_MAGIC_NUMBER,
		      FD_OID_ADDR(base),fd_getint(capacity),
		      metadata);
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
	if (fd_write_4bytes(stream,(unsigned int)endpos)>=0) {
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

/* The init function */

static int scheme_filedb_initialized=0;

FD_EXPORT void fd_init_filedb_c()
{
  fdtype filedb_module;
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
	   fd_make_cprim4x("MAKE-FILE-POOL",make_file_pool,3,
			   fd_string_type,FD_VOID,
			   fd_oid_type,FD_VOID,
			   fd_fixnum_type,FD_VOID,
			   fd_slotmap_type,FD_VOID));
  fd_idefn(filedb_module,
	   fd_make_cprim4x("MAKE-ZPOOL",make_zpool,3,
			   fd_string_type,FD_VOID,
			   fd_oid_type,FD_VOID,
			   fd_fixnum_type,FD_VOID,
			   fd_slotmap_type,FD_VOID));
  fd_idefn(filedb_module,fd_make_cprim2x("LABEL-POOL!",label_file_pool,2,
					 fd_string_type,FD_VOID,
					 fd_string_type,FD_VOID));

  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE",lisphashdtype2,1));
  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE2",lisphashdtype2,1));
  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE3",lisphashdtype3,1));
  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE1",lisphashdtype1,1));

  fd_finish_module(filedb_module);
}


/* The CVS log for this file
   $Log: filedb.c,v $
   Revision 1.11  2006/02/09 22:46:28  haase
   Added LABEL-POOL

   Revision 1.10  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.9  2005/09/13 03:34:52  haase
   Make filedb init dbfile

   Revision 1.8  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.7  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.6  2005/05/17 20:30:03  haase
   Made default make-file-index create a version 2 index and added make-legacy-file-index

   Revision 1.5  2005/05/09 20:04:19  haase
   Move dtype hash functions into dbfile and made libfdscheme independent of libfddbfile

   Revision 1.4  2005/05/04 09:42:42  haase
   Added module loading locking stuff

   Revision 1.3  2005/04/28 14:31:28  haase
   Created modules for FILEIO and FILEDB

   Revision 1.2  2005/04/21 19:03:26  haase
   Add initialization procedures

   Revision 1.1  2005/03/29 04:12:36  haase
   Added pool/index making primitives


*/
