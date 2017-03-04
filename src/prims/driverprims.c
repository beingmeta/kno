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

  /* These are all primitives which access the internals of various
     drivers (back when they were exposed). We're leaving them here
     until there are appropriate opaque replacements. */

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

  fd_idefn(driverfns_module,
           fd_make_cprim2x("LABEL-FILE-POOL!",
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

  fd_idefn(driverfns_module,
           fd_make_cprim1("HASH-DTYPE-REP",lisphashdtyperep,1));

  fd_finish_module(driverfns_module);
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
