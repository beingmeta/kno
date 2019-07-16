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
#include "kno/lisp.h"
#include "kno/apply.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/sequences.h"
#include "kno/drivers.h"
#include "kno/cprims.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>


static lispval baseoids_symbol;

/* Hashing functions */

DEFPRIM1("hash-dtype1",lisphash1,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(HASH-DTYPE1 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lisphash1(lispval x)
{
  int hash = kno_hash_lisp1(x);
  return KNO_INT(hash);
}
DEFPRIM1("hash-dtype2",lisphash2,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(HASH-DTYPE2 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lisphash2(lispval x)
{
  int hash = kno_hash_lisp2(x);
  return KNO_INT(hash);
}

DEFPRIM1("hash-dtype3",lisphash3,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(HASH-DTYPE3 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lisphash3(lispval x)
{
  int hash = kno_hash_lisp3(x);
  return KNO_INT(hash);
}

DEFPRIM1("hash-dtype-rep",lisphashdtype,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(HASH-DTYPE-REP *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lisphashdtype(lispval x)
{
  unsigned int hash = kno_hash_dtype_rep(x);
  return KNO_INT(hash);
}

/* Various OPS */

DEFPRIM1("index-slotids",index_slotids,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(INDEX-SLOTIDS *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval index_slotids(lispval index_arg)
{
  struct KNO_INDEX *ix = kno_lisp2index(index_arg);
  if (ix == NULL)
    return KNO_ERROR;
  else return kno_index_ctl(ix,kno_slotids_op,0,NULL);
}

DEFPRIM("indexctl",indexctl_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(INDEXCTL *arg0* *arg1* *args...*)` **undocumented**");
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

DEFPRIM("indexctl/default",indexctl_default_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(INDEXCTL/DEFAULT *arg0* *arg1* *args...*)` **undocumented**");
static lispval indexctl_default_prim(int n,lispval *args)
{
  struct KNO_INDEX *ix = kno_lisp2index(args[0]);
  if (ix == NULL)
    return KNO_ERROR;
  else if (!(SYMBOLP(args[1])))
    return kno_err("BadIndexOp","indexctl_prim",NULL,args[1]);
  else return kno_default_indexctl(ix,args[1],n-2,args+2);
}

DEFPRIM("poolctl",poolctl_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(POOLCTL *arg0* *arg1* *args...*)` **undocumented**");
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

DEFPRIM("poolctl/default",poolctl_default_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(POOLCTL/DEFAULT *arg0* *arg1* *args...*)` **undocumented**");
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

DEFPRIM("dbctl",dbctl_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"(DBCTL *dbref* *op* ... *args*) "
	"performs an operation *op* on the pool or index "
	"*dbref* with *args*. *op* is a symbol and *dbref* "
	"must be a pool or index object.");
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

DEFPRIM3("alcor/save!",alcor_save_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	 "(ALCOR/SAVE! src head len) "
	 "copies the first *len* bytes of *src* into *head*",
	 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_VOID);
static lispval alcor_save_prim(lispval source,lispval head,lispval size_arg)
{
  ssize_t size = KNO_FIX2INT(size_arg);
  ssize_t rv = kno_save_head(KNO_CSTRING(source),KNO_CSTRING(head),size);
  if (rv<0)
    return KNO_ERROR;
  else return KNO_INT(rv);
}

DEFPRIM2("alcor/apply!",alcor_apply_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "(ALCOR/APPLY! head src) "
	 "copies *head* into the beginning of *src* (all "
	 "but the last 8 bytes) and truncates *src* to the "
	 "length specified in the last eight bytes of *head*",
	 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID);
static lispval alcor_apply_prim(lispval head,lispval source)
{
  ssize_t rv = kno_apply_head(KNO_CSTRING(head),KNO_CSTRING(source));
  if (rv<0)
    return KNO_ERROR;
  else return KNO_INT(rv);
}

/* The init function */

static int scheme_driverfns_initialized = 0;

static lispval driverfns_module;

KNO_EXPORT void kno_init_driverfns_c()
{
  baseoids_symbol = kno_intern("%baseoids");

  if (scheme_driverfns_initialized) return;
  scheme_driverfns_initialized = 1;
  kno_init_scheme();
  kno_init_drivers();
  driverfns_module = kno_new_cmodule
    ("driverfns",(KNO_MODULE_DEFAULT),kno_init_driverfns_c);
  u8_register_source_file(_FILEINFO);
  init_local_cprims();

  kno_finish_module(driverfns_module);
}


static void init_local_cprims()
{
  lispval db_module = kno_db_module;

  KNO_LINK_PRIM("alcor/apply!",alcor_apply_prim,2,driverfns_module);
  KNO_LINK_PRIM("alcor/save!",alcor_save_prim,3,driverfns_module);
  KNO_LINK_VARARGS("dbctl",dbctl_prim,kno_db_module);
  KNO_LINK_VARARGS("poolctl/default",poolctl_default_prim,kno_db_module);
  KNO_LINK_VARARGS("poolctl",poolctl_prim,kno_db_module);
  KNO_LINK_VARARGS("indexctl/default",indexctl_default_prim,kno_db_module);
  KNO_LINK_VARARGS("indexctl",indexctl_prim,kno_db_module);
  KNO_LINK_PRIM("index-slotids",index_slotids,1,kno_db_module);
  KNO_LINK_PRIM("hash-dtype-rep",lisphashdtype,1,driverfns_module);
  KNO_LINK_PRIM("hash-dtype3",lisphash3,1,driverfns_module);
  KNO_LINK_ALIAS("hash-dtype",lisphash2,driverfns_module);
  KNO_LINK_PRIM("hash-dtype2",lisphash2,1,driverfns_module);
  KNO_LINK_PRIM("hash-dtype1",lisphash1,1,driverfns_module);

  KNO_LINK_ALIAS("hash-index-slotids",index_slotids,db_module);
}
