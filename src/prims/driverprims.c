/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL 1

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/apply.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/sequences.h"
#include "kno/drivers.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>

static lispval baseoids_symbol;

/* Hashing functions */

static lispval lisphash1(lispval x)
{
  int hash = kno_hash_lisp1(x);
  return KNO_INT(hash);
}
static lispval lisphash2(lispval x)
{
  int hash = kno_hash_lisp2(x);
  return KNO_INT(hash);
}

static lispval lisphash3(lispval x)
{
  int hash = kno_hash_lisp3(x);
  return KNO_INT(hash);
}

static lispval lisphashdtype(lispval x)
{
  unsigned int hash = kno_hash_dtype_rep(x);
  return KNO_INT(hash);
}

/* Various OPS */

static lispval index_slotids(lispval index_arg)
{
  struct KNO_INDEX *ix = kno_lisp2index(index_arg);
  if (ix == NULL)
    return KNO_ERROR;
  else return kno_index_ctl(ix,kno_slotids_op,0,NULL);
}

static lispval indexctl_prim(int n,lispval *args)
{
  if (KNO_AMBIGP(args[0])) {
    lispval inner[n], indexes = args[0];
    memcpy(inner,args,n*sizeof(lispval));
    lispval results = KNO_EMPTY;
    DO_CHOICES(each,indexes) {
      inner[0]=each;
      lispval result = indexctl_prim(n,inner);
      if (KNO_ABORTP(result)) {
        kno_decref(results);
        KNO_STOP_DO_CHOICES;
        return result;}
      else if (KNO_VOIDP(result)) {}
      else {KNO_ADD_TO_CHOICE(results,result);}}
    return results;}
  struct KNO_INDEX *ix = kno_lisp2index(args[0]);
  if (ix == NULL)
    return KNO_ERROR;
  else if (!(SYMBOLP(args[1])))
    return kno_err("BadIndexOp","indexctl_prim",NULL,args[1]);
  else return kno_index_ctl(ix,args[1],n-2,args+2);
}

static lispval indexctl_default_prim(int n,lispval *args)
{
  struct KNO_INDEX *ix = kno_lisp2index(args[0]);
  if (ix == NULL)
    return KNO_ERROR;
  else if (!(SYMBOLP(args[1])))
    return kno_err("BadIndexOp","indexctl_prim",NULL,args[1]);
  else return kno_default_indexctl(ix,args[1],n-2,args+2);
}

static lispval poolctl_prim(int n,lispval *args)
{
  if (KNO_AMBIGP(args[0])) {
    lispval inner[n], pools = args[0];
    memcpy(inner,args,n*sizeof(lispval));
    lispval results = KNO_EMPTY;
    DO_CHOICES(each,pools) {
      inner[0]=each;
      lispval result = poolctl_prim(n,inner);
      if (KNO_ABORTP(result)) {
        kno_decref(results);
        KNO_STOP_DO_CHOICES;
        return result;}
      else if (KNO_VOIDP(result)) {}
      else {KNO_ADD_TO_CHOICE(results,result);}}
    return results;}
  struct KNO_POOL *p = kno_lisp2pool(args[0]);
  if (p == NULL)
    return KNO_ERROR;
  else if (!(SYMBOLP(args[1])))
    return kno_err("BadPoolOp","poolctl_prim",NULL,args[1]);
  else return kno_pool_ctl(p,args[1],n-2,args+2);
}

static lispval poolctl_default_prim(int n,lispval *args)
{
  struct KNO_POOL *p = kno_lisp2pool(args[0]);
  if (p == NULL)
    return KNO_ERROR;
  else if (!(SYMBOLP(args[1])))
    return kno_err("BadPoolOp","poolctl_prim",NULL,args[1]);
  else return kno_default_poolctl(p,args[1],n-2,args+2);
}

/* DBCTL */

static lispval dbctl_prim(int n,lispval *args)
{
  lispval db = args[0];
  if (KNO_AMBIGP(db)) {
    lispval inner[n];
    memcpy(inner,args,n*sizeof(lispval));
    lispval results = KNO_EMPTY;
    DO_CHOICES(each,db) {
      inner[0]=each;
      lispval result = dbctl_prim(n,inner);
      if (KNO_ABORTP(result)) {
        kno_decref(results);
        KNO_STOP_DO_CHOICES;
        return result;}
      else if (KNO_VOIDP(result)) {}
      else {KNO_ADD_TO_CHOICE(results,result);}}
    return results;}
  else if (!(SYMBOLP(args[1])))
    return kno_err("BadDatabaseOp","indexctl_prim",NULL,args[1]);
  else if ( (KNO_INDEXP(db)) || (KNO_TYPEP(db,kno_consed_index_type)) ) {
    struct KNO_INDEX *ix = kno_lisp2index(db);
    if (ix == NULL)
      return kno_type_error("DB(index)","dbctl_prim",db);
    else return kno_index_ctl(ix,args[1],n-2,args+2);}
  else if ( (KNO_POOLP(db)) || (KNO_TYPEP(db,kno_consed_pool_type)) ) {
    struct KNO_POOL *p = kno_lisp2pool(db);
    if (p == NULL)
      return kno_type_error("DB(pool)","dbctl_prim",db);
    else return kno_pool_ctl(p,args[1],n-2,args+2);}
  else return kno_type_error("DB(pool/index)","dbctl_prim",db);
}


/* ALCOR bindings */

static lispval alcor_save_prim(lispval source,lispval head,lispval size_arg)
{
  ssize_t size = KNO_FIX2INT(size_arg);
  ssize_t rv = kno_save_head(KNO_CSTRING(source),KNO_CSTRING(head),size);
  if (rv<0)
    return KNO_ERROR;
  else return KNO_INT(rv);
}

static lispval alcor_apply_prim(lispval head,lispval source)
{
  ssize_t rv = kno_apply_head(KNO_CSTRING(head),KNO_CSTRING(source));
  if (rv<0)
    return KNO_ERROR;
  else return KNO_INT(rv);
}

/* The init function */

static int scheme_driverfns_initialized = 0;

KNO_EXPORT void kno_init_driverfns_c()
{
  lispval driverfns_module;

  baseoids_symbol = kno_intern("%BASEOIDS");

  if (scheme_driverfns_initialized) return;
  scheme_driverfns_initialized = 1;
  kno_init_scheme();
  kno_init_drivers();
  driverfns_module = kno_new_cmodule("DRIVERFNS",(KNO_MODULE_DEFAULT),
                                    kno_init_driverfns_c);
  u8_register_source_file(_FILEINFO);

  kno_idefn(kno_xscheme_module,kno_make_cprim1("INDEX-SLOTIDS",index_slotids,1));
  kno_defalias(kno_xscheme_module,"HASH-INDEX-SLOTIDS","INDEX-SLOTIDS");


  kno_idefnN(kno_xscheme_module,"DBCTL",dbctl_prim,KNO_NEEDS_2_ARGS|KNO_NDCALL,
            "(DBCTL *dbref* *op* ... *args*) performs an operation *op* "
            "on the pool or index *dbref* with *args*. *op* is a symbol and "
            "*dbref* must be a pool or index object.");
  kno_idefnN(kno_xscheme_module,"POOLCTL",poolctl_prim,KNO_NEEDS_2_ARGS|KNO_NDCALL,
            "(POOLCTL *pool* *op* ... *args*) performs an operation *op* "
            "on the pool *pool* with *args*. *op* is a symbol and *pool* is "
            "a pool object.");
  kno_idefnN(kno_xscheme_module,"INDEXCTL",indexctl_prim,KNO_NEEDS_2_ARGS|KNO_NDCALL,
            "(INDEXCTL *index* *op* ... *args*) performs an operation *op* "
            "on the index *index* with *args*. *op* is a symbol and *index* "
            "is an index object.");

  kno_idefn(kno_xscheme_module,kno_make_cprimn("INDEXCTL",indexctl_prim,2));
  kno_idefn(kno_xscheme_module,kno_make_cprimn("POOLCTL",poolctl_prim,2));

  kno_idefn(kno_xscheme_module,kno_make_cprimn("INDEXCTL/DEFAULT",indexctl_default_prim,2));
  kno_idefn(kno_xscheme_module,kno_make_cprimn("POOLCTL/DEFAULT",poolctl_default_prim,2));

  kno_idefn(driverfns_module,kno_make_cprim1("HASH-DTYPE",lisphash2,1));
  kno_idefn(driverfns_module,kno_make_cprim1("HASH-DTYPE2",lisphash2,1));
  kno_idefn(driverfns_module,kno_make_cprim1("HASH-DTYPE3",lisphash3,1));
  kno_idefn(driverfns_module,kno_make_cprim1("HASH-DTYPE1",lisphash1,1));

  kno_idefn(driverfns_module,kno_make_cprim1("HASH-DTYPE-REP",lisphashdtype,1));

  kno_idefn3(driverfns_module,"ALCOR/SAVE!",alcor_save_prim,3,
            "(ALCOR/SAVE! src head len) copies the first *len* bytes "
            "of *src* into *head*",
            kno_string_type,KNO_VOID,kno_string_type,KNO_VOID,
            kno_fixnum_type,KNO_VOID);
  kno_idefn2(driverfns_module,"ALCOR/APPLY!",alcor_apply_prim,2,
            "(ALCOR/APPLY! head src) copies *head* into the "
            "beginning of *src* (all but the last 8 bytes) and "
            "truncates *src* to the length specified in the last "
            "eight bytes of *head*",
            kno_string_type,KNO_VOID,kno_string_type,KNO_VOID);



  kno_finish_module(driverfns_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
