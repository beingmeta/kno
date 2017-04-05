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
#include "framerd/fdkbase.h"
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

FD_EXPORT fdtype _fd_make_hashindex_deprecated(fdtype fname,fdtype size,
                                                fdtype slotids,fdtype baseoids,
                                                fdtype metadata,
                                                fdtype flags_arg);
FD_EXPORT fdtype _fd_populate_hashindex_deprecated
  (fdtype ix_arg,fdtype from,fdtype blocksize_arg,fdtype keys);
FD_EXPORT fdtype _fd_hashindex_bucket_deprecated(fdtype ix_arg,fdtype key,fdtype modulus);
FD_EXPORT fdtype _fd_hashindex_stats_deprecated(fdtype ix_arg);
FD_EXPORT fdtype _fd_hashindex_slotids_deprecated(fdtype ix_arg);

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

static fdtype access_pool_prim(fdtype name)
{
  fd_pool p=fd_unregistered_file_pool(FD_STRDATA(name));
  if (p) return (fdtype) p;
  else return FD_ERROR_VALUE;
}

static fdtype pool_prefetch(fdtype pool,fdtype oids)
{
  fd_pool p=(fd_pool)pool;
  int retval=fd_pool_prefetch(p,oids);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_VOID;
}

/* Various OPS */

static fdtype index_slotids(fdtype index_arg)
{
  struct FD_INDEX *ix=fd_lisp2index(index_arg);
  if (ix==NULL)
    return FD_ERROR_VALUE;
  else return fd_index_ctl(ix,FD_INDEXOP_SLOTIDS,0,NULL);
}

static int getindexctlop(fdtype x)
{
  if (FD_INTP(x))
    return FD_FIX2INT(x);
  else if (FD_SYMBOLP(x)) {
    u8_string pname=FD_SYMBOL_NAME(x);
    if ((strcasecmp(pname,"CACHELEVEL")==0)||
        (strcasecmp(pname,"SETCACHELEVEL")==0))
      return FD_INDEXOP_CACHELEVEL;
    else if ((strcasecmp(pname,"BUFSIZE")==0)||
             (strcasecmp(pname,"BUF")==0))
      return FD_INDEXOP_BUFSIZE;
    else if (strcasecmp(pname,"STATS")==0)
      return FD_INDEXOP_STATS;
    else if (strcasecmp(pname,"BUCKET")==0)
      return FD_INDEXOP_HASH;
    else if (strcasecmp(pname,"MMAP")==0)
      return FD_INDEXOP_MMAP;
    else if (strcasecmp(pname,"PRELOAD")==0)
      return FD_INDEXOP_PRELOAD;
    else if (strcasecmp(pname,"POPULATE")==0)
      return FD_INDEXOP_POPULATE;
    else if (strcasecmp(pname,"HASHTABLE")==0)
      return FD_INDEXOP_HASHTABLE;
    else {
      fd_seterr("UnknownIndexOp","indexctl_prim",NULL,x);
      return -1;}}
  else return -1;
}

static fdtype indexctl_prim(int n,fdtype *args)
{
  struct FD_INDEX *ix=fd_lisp2index(args[0]);
  if (ix==NULL)
    return FD_ERROR_VALUE;
  int op=getindexctlop(args[1]);
  if (op<0)
    return fd_err("BadIndexOp","index_ctl",ix->indexid,args[1]);
  else return fd_index_ctl(ix,op,n-1,args+1);
}

static int getpoolctlop(fdtype x)
{
  if (FD_INTP(x))
    return FD_FIX2INT(x);
  else if (FD_SYMBOLP(x)) {
    u8_string pname=FD_SYMBOL_NAME(x);
    if (strcasecmp(pname,"CACHELEVEL")==0)
      return FD_POOLOP_CACHELEVEL;
    else if ((strcasecmp(pname,"BUFSIZE")==0)||
             (strcasecmp(pname,"BUF")==0))
      return FD_POOLOP_BUFSIZE;
    else if (strcasecmp(pname,"STATS")==0)
      return FD_POOLOP_STATS;
    else if (strcasecmp(pname,"LABEL")==0)
      return FD_POOLOP_LABEL;
    else if (strcasecmp(pname,"MMAP")==0)
      return FD_POOLOP_MMAP;
    else if (strcasecmp(pname,"PRELOAD")==0)
      return FD_POOLOP_PRELOAD;
    else if (strcasecmp(pname,"POPULATE")==0)
      return FD_POOLOP_POPULATE;
    else {
      fd_seterr("UnknownPoolOp","poolctl_prim",NULL,x);
      return -1;}}
  else return -1;
}

static fdtype poolctl_prim(int n,fdtype *args)
{
  struct FD_POOL *p=fd_lisp2pool(args[0]);
  if (p==NULL)
    return FD_ERROR_VALUE;
  int op=getpoolctlop(args[1]);
  if (op<0)
    return fd_err("BadPoolOp","pool_ctl",p->poolid,args[1]);
  else return fd_pool_ctl(p,op,n-2,args+2);
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
  fd_init_kbdrivers();
  driverfns_module=fd_new_module("DRIVERFNS",(FD_MODULE_DEFAULT));
  u8_register_source_file(_FILEINFO);

  fd_idefn(driverfns_module,
           fd_make_cprim1x("ACCESS-POOL",access_pool_prim,1,
                           fd_string_type,FD_VOID));

  fd_idefn(driverfns_module,
           fd_make_ndprim(fd_make_cprim2x("POOL-PREFETCH!",pool_prefetch,2,
                                          fd_raw_pool_type,FD_VOID,-1,FD_VOID)));

  fd_idefn(fd_xscheme_module,fd_make_cprim1("INDEX-SLOTIDS",index_slotids,1));
  fd_defalias(fd_xscheme_module,"HASH-INDEX-SLOTIDS","INDEX-SLOTIDS");


  fd_idefn(fd_xscheme_module,fd_make_cprimn("INDEXCTL",indexctl_prim,2));
  fd_idefn(fd_xscheme_module,fd_make_cprimn("POOLCTL",poolctl_prim,2));
  
  /* These are all primitives which access the internals of various
     drivers (back when they were exposed). We're leaving them here
     until there are appropriate opaque replacements. */

  fd_idefn(driverfns_module,
           fd_make_cprim4x("POPULATE-HASH-INDEX",
                           _fd_populate_hashindex_deprecated,2,
                           -1,FD_VOID,-1,FD_VOID,
                           fd_fixnum_type,FD_VOID,-1,FD_VOID));

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
