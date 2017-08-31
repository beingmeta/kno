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
#include "framerd/apply.h"
#include "framerd/storage.h"
#include "framerd/eval.h"
#include "framerd/ports.h"
#include "framerd/sequences.h"
#include "framerd/drivers.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>

static lispval baseoids_symbol;

/* Hashing functions */

static lispval lisphash1(lispval x)
{
  int hash = fd_hash_lisp1(x);
  return FD_INT(hash);
}
static lispval lisphash2(lispval x)
{
  int hash = fd_hash_lisp2(x);
  return FD_INT(hash);
}

static lispval lisphash3(lispval x)
{
  int hash = fd_hash_lisp3(x);
  return FD_INT(hash);
}

static lispval lisphashdtype(lispval x)
{
  unsigned int hash = fd_hash_dtype_rep(x);
  return FD_INT(hash);
}

/* Various OPS */

static lispval index_slotids(lispval index_arg)
{
  struct FD_INDEX *ix = fd_lisp2index(index_arg);
  if (ix == NULL)
    return FD_ERROR;
  else return fd_index_ctl(ix,fd_slotids_op,0,NULL);
}

static lispval indexctl_prim(int n,lispval *args)
{
  struct FD_INDEX *ix = fd_lisp2index(args[0]);
  if (ix == NULL)
    return FD_ERROR;
  else if (!(SYMBOLP(args[1])))
    return fd_err("BadIndexOp","indexctl_prim",NULL,args[1]);
  else return fd_index_ctl(ix,args[1],n-2,args+2);
}

static lispval indexctl_default_prim(int n,lispval *args)
{
  struct FD_INDEX *ix = fd_lisp2index(args[0]);
  if (ix == NULL)
    return FD_ERROR;
  else if (!(SYMBOLP(args[1])))
    return fd_err("BadIndexOp","indexctl_prim",NULL,args[1]);
  else return fd_default_indexctl(ix,args[1],n-2,args+2);
}

static lispval poolctl_prim(int n,lispval *args)
{
  struct FD_POOL *p = fd_lisp2pool(args[0]);
  if (p == NULL)
    return FD_ERROR;
  else if (!(SYMBOLP(args[1])))
    return fd_err("BadPoolOp","poolctl_prim",NULL,args[1]);
  else return fd_pool_ctl(p,args[1],n-2,args+2);
}

static lispval poolctl_default_prim(int n,lispval *args)
{
  struct FD_POOL *p = fd_lisp2pool(args[0]);
  if (p == NULL)
    return FD_ERROR;
  else if (!(SYMBOLP(args[1])))
    return fd_err("BadPoolOp","poolctl_prim",NULL,args[1]);
  else return fd_default_poolctl(p,args[1],n-2,args+2);
}
/* The init function */

static int scheme_driverfns_initialized = 0;

FD_EXPORT void fd_init_driverfns_c()
{
  lispval driverfns_module;

  baseoids_symbol = fd_intern("%BASEOIDS");

  if (scheme_driverfns_initialized) return;
  scheme_driverfns_initialized = 1;
  fd_init_scheme();
  fd_init_drivers();
  driverfns_module = fd_new_module("DRIVERFNS",(FD_MODULE_DEFAULT));
  u8_register_source_file(_FILEINFO);

  fd_idefn(fd_xscheme_module,fd_make_cprim1("INDEX-SLOTIDS",index_slotids,1));
  fd_defalias(fd_xscheme_module,"HASH-INDEX-SLOTIDS","INDEX-SLOTIDS");


  fd_idefn(fd_xscheme_module,fd_make_cprimn("INDEXCTL",indexctl_prim,2));
  fd_idefn(fd_xscheme_module,fd_make_cprimn("POOLCTL",poolctl_prim,2));

  fd_idefn(fd_xscheme_module,fd_make_cprimn("INDEXCTL/DEFAULT",indexctl_default_prim,2));
  fd_idefn(fd_xscheme_module,fd_make_cprimn("POOLCTL/DEFAULT",poolctl_default_prim,2));

  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE",lisphash2,1));
  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE2",lisphash2,1));
  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE3",lisphash3,1));
  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE1",lisphash1,1));

  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE-REP",lisphashdtype,1));

  fd_finish_module(driverfns_module);
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
