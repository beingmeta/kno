/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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
  if (FD_AMBIGP(args[0])) {
    lispval inner[n], indexes = args[0];
    memcpy(inner,args,n*sizeof(lispval));
    lispval results = FD_EMPTY;
    DO_CHOICES(each,indexes) {
      inner[0]=each;
      lispval result = indexctl_prim(n,inner);
      if (FD_ABORTP(result)) {
        fd_decref(results);
        FD_STOP_DO_CHOICES;
        return result;}
      else if (FD_VOIDP(result)) {}
      else {FD_ADD_TO_CHOICE(results,result);}}
    return results;}
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
  if (FD_AMBIGP(args[0])) {
    lispval inner[n], pools = args[0];
    memcpy(inner,args,n*sizeof(lispval));
    lispval results = FD_EMPTY;
    DO_CHOICES(each,pools) {
      inner[0]=each;
      lispval result = poolctl_prim(n,inner);
      if (FD_ABORTP(result)) {
        fd_decref(results);
        FD_STOP_DO_CHOICES;
        return result;}
      else if (FD_VOIDP(result)) {}
      else {FD_ADD_TO_CHOICE(results,result);}}
    return results;}
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

/* DBCTL */

static lispval dbctl_prim(int n,lispval *args)
{
  lispval db = args[0];
  if (FD_AMBIGP(db)) {
    lispval inner[n];
    memcpy(inner,args,n*sizeof(lispval));
    lispval results = FD_EMPTY;
    DO_CHOICES(each,db) {
      inner[0]=each;
      lispval result = dbctl_prim(n,inner);
      if (FD_ABORTP(result)) {
        fd_decref(results);
        FD_STOP_DO_CHOICES;
        return result;}
      else if (FD_VOIDP(result)) {}
      else {FD_ADD_TO_CHOICE(results,result);}}
    return results;}
  else if (!(SYMBOLP(args[1])))
    return fd_err("BadDatabaseOp","indexctl_prim",NULL,args[1]);
  else if ( (FD_INDEXP(db)) || (FD_TYPEP(db,fd_consed_index_type)) ) {
    struct FD_INDEX *ix = fd_lisp2index(db);
    if (ix == NULL)
      return fd_type_error("DB(index)","dbctl_prim",db);
    else return fd_index_ctl(ix,args[1],n-2,args+2);}
  else if ( (FD_POOLP(db)) || (FD_TYPEP(db,fd_consed_pool_type)) ) {
    struct FD_POOL *p = fd_lisp2pool(db);
    if (p == NULL)
      return fd_type_error("DB(pool)","dbctl_prim",db);
    else return fd_pool_ctl(p,args[1],n-2,args+2);}
  else return fd_type_error("DB(pool/index)","dbctl_prim",db);
}


/* ALCOR bindings */

static lispval alcor_save_prim(lispval source,lispval head,lispval size_arg)
{
  ssize_t size = FD_FIX2INT(size_arg);
  ssize_t rv = fd_save_head(FD_CSTRING(source),FD_CSTRING(head),size);
  if (rv<0)
    return FD_ERROR;
  else return FD_INT(rv);
}

static lispval alcor_apply_prim(lispval head,lispval source)
{
  ssize_t rv = fd_apply_head(FD_CSTRING(head),FD_CSTRING(source));
  if (rv<0)
    return FD_ERROR;
  else return FD_INT(rv);
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
  driverfns_module = fd_new_cmodule("DRIVERFNS",(FD_MODULE_DEFAULT),
                                    fd_init_driverfns_c);
  u8_register_source_file(_FILEINFO);

  fd_idefn(fd_xscheme_module,fd_make_cprim1("INDEX-SLOTIDS",index_slotids,1));
  fd_defalias(fd_xscheme_module,"HASH-INDEX-SLOTIDS","INDEX-SLOTIDS");


  fd_idefnN(fd_xscheme_module,"DBCTL",dbctl_prim,FD_NEEDS_2_ARGS|FD_NDCALL,
            "(DBCTL *dbref* *op* ... *args*) performs an operation *op* "
            "on the pool or index *dbref* with *args*. *op* is a symbol and "
            "*dbref* must be a pool or index object.");
  fd_idefnN(fd_xscheme_module,"POOLCTL",poolctl_prim,FD_NEEDS_2_ARGS|FD_NDCALL,
            "(POOLCTL *pool* *op* ... *args*) performs an operation *op* "
            "on the pool *pool* with *args*. *op* is a symbol and *pool* is "
            "a pool object.");
  fd_idefnN(fd_xscheme_module,"INDEXCTL",indexctl_prim,FD_NEEDS_2_ARGS|FD_NDCALL,
            "(INDEXCTL *index* *op* ... *args*) performs an operation *op* "
            "on the index *index* with *args*. *op* is a symbol and *index* "
            "is an index object.");

  fd_idefn(fd_xscheme_module,fd_make_cprimn("INDEXCTL",indexctl_prim,2));
  fd_idefn(fd_xscheme_module,fd_make_cprimn("POOLCTL",poolctl_prim,2));

  fd_idefn(fd_xscheme_module,fd_make_cprimn("INDEXCTL/DEFAULT",indexctl_default_prim,2));
  fd_idefn(fd_xscheme_module,fd_make_cprimn("POOLCTL/DEFAULT",poolctl_default_prim,2));

  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE",lisphash2,1));
  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE2",lisphash2,1));
  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE3",lisphash3,1));
  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE1",lisphash1,1));

  fd_idefn(driverfns_module,fd_make_cprim1("HASH-DTYPE-REP",lisphashdtype,1));

  fd_idefn3(driverfns_module,"ALCOR/SAVE!",alcor_save_prim,3,
            "(ALCOR/SAVE! src head len) copies the first *len* bytes "
            "of *src* into *head*",
            fd_string_type,FD_VOID,fd_string_type,FD_VOID,
            fd_fixnum_type,FD_VOID);
  fd_idefn2(driverfns_module,"ALCOR/APPLY!",alcor_apply_prim,2,
            "(ALCOR/APPLY! head src) copies *head* into the "
            "beginning of *src* (all but the last 8 bytes) and "
            "truncates *src* to the length specified in the last "
            "eight bytes of *head*",
            fd_string_type,FD_VOID,fd_string_type,FD_VOID);



  fd_finish_module(driverfns_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
