/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_POOLS KNO_DO_INLINE
#define KNO_INLINE_CHOICES KNO_DO_INLINE
#define KNO_INLINE_IPEVAL KNO_DO_INLINE
static int dbserv_loglevel;
#define U8_LOGLEVEL dbserv_loglevel

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/apply.h"
#include "kno/cprims.h"
#include "kno/services.h"

#include <libu8/u8printf.h>
#include <libu8/u8filefns.h>
#if HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#include <libu8/u8srvfns.h>

static kno_pool primary_pool = NULL;
static kno_pool served_pools[KNO_DBSERV_MAX_POOLS];
static int n_served_pools = 0;
struct KNO_AGGREGATE_INDEX *primary_index = NULL;
static int dbserv_loglevel = LOG_NOTICE;

u8_condition kno_PrivateOID=_("private OID");

lispval get_oid_value(lispval oid,kno_pool p);

static int served_poolp(kno_pool p)
{
  int i = 0; while (i<n_served_pools)
               if (served_pools[i]==p) return 1;
               else i++;
  return 0;
}

/* pool DB methods */

static lispval server_get_load(lispval oid_arg)
{
  if (VOIDP(oid_arg))
    if (primary_pool) {
      int load = kno_pool_load(primary_pool);
      if (load<0) return KNO_ERROR;
      else return KNO_INT(load);}
    else return kno_err(_("No primary pool"),"server_get_load",NULL,VOID);
  else if (OIDP(oid_arg)) {
    kno_pool p = kno_oid2pool(oid_arg);
    int load = kno_pool_load(p);
    if (load<0) return KNO_ERROR;
    else return KNO_INT(load);}
  else return kno_type_error("OID","server_get_load",oid_arg);
}

static lispval server_oid_value(lispval x)
{
  kno_pool p = kno_oid2pool(x);
  if (p == NULL)
    return kno_err(kno_AnonymousOID,"server_oid_value",NULL,x);
  else if (served_poolp(p))
    return get_oid_value(x,p);
  else return kno_err(kno_PrivateOID,"server_oid_value",NULL,x);
}

static lispval server_fetch_oids(lispval oidvec)
{
  /* We assume here that all the OIDs in oidvec are in the same pool.  This should
     be the case because clients see the different pools and sort accordingly.  */
  kno_pool p = NULL;
  int n = VEC_LEN(oidvec), fetchn = 0;
  lispval *elts = VEC_DATA(oidvec);
  if (n==0)
    return kno_empty_vector(0);
  else if (!(OIDP(elts[0])))
    return kno_type_error(_("oid vector"),"server_fetch_oids",oidvec);
  else if ((p = (kno_oid2pool(elts[0]))))
    if (served_poolp(p)) {
      lispval *results = NULL;
      if (p->pool_handler->fetchn) {
        lispval *fetch = u8_alloc_n(n,lispval);
        kno_hashtable cache = &(p->pool_cache), locks = &(p->pool_changes);
        int i = 0; while (i<n)
                     if ((kno_hashtable_probe_novoid(cache,elts[i])==0) &&
                         (kno_hashtable_probe_novoid(locks,elts[i])==0))
                       fetch[fetchn++]=elts[i++];
                     else i++;
        results = p->pool_handler->fetchn(p,fetchn,fetch);
        return kno_cons_vector(NULL,n,1,results);}
      else {
        results = u8_big_alloc_n(n,lispval);
        int i = 0; while (i<n) {
	  results[i]=get_oid_value(elts[i],p); i++;}
        return kno_cons_vector(NULL,n,1,results);}}
    else return kno_err(kno_PrivateOID,"server_oid_value",NULL,elts[0]);
  else return kno_err(kno_AnonymousOID,"server_oid_value",NULL,elts[0]);
}

DEF_KNOSYM(base); DEF_KNOSYM(capacity); DEF_KNOSYM(label); DEF_KNOSYM(readonly);

static lispval server_pool_data(lispval session_id)
{
  int len = n_served_pools;
  lispval *elts = u8_alloc_n(len,lispval);
  int i = 0; while (i<len) {
    kno_pool p = served_pools[i];
    lispval base = kno_make_oid(p->pool_base);
    lispval capacity = KNO_INT(p->pool_capacity);
    lispval adjunct = p->pool_adjunct;
    lispval info = kno_make_slotmap(7,0,NULL);
    kno_store(info,KNOSYM(base),base);
    kno_store(info,KNOSYM(capacity),capacity);
    if (U8_BITP(p->pool_flags,KNO_STORAGE_READ_ONLY))
      kno_store(info,KNOSYM(readonly),KNO_TRUE);
    if ( (KNO_OIDP(adjunct)) || (KNO_SYMBOLP(adjunct)) || (KNO_TRUEP(adjunct)) )
      kno_store(info,KNOSYM_ADJUNCT,adjunct);
    if (p->pool_label) {
      lispval s = kno_mkstring(p->pool_label);
      kno_store(info,KNOSYM(label),s);
      kno_decref(s);}
    elts[i++] = info;}
  return kno_wrap_vector(len,elts);
}

/* index DB methods */

static lispval iserver_get(lispval key)
{
  if (PAIRP(key))
    return kno_prim_find((lispval)primary_index,KNO_CAR(key),KNO_CDR(key));
  else return kno_index_get((kno_index)(primary_index),key);
}
static lispval iserver_bulk_get(lispval keys)
{
  if (VECTORP(keys)) {
    int i = 0, n = VEC_LEN(keys);
    lispval *data = VEC_DATA(keys), *results = u8_alloc_n(n,lispval);
    /* |KNO_CHOICE_ISATOMIC */
    lispval aschoice = kno_make_choice
      (n,data,(KNO_CHOICE_DOSORT|KNO_CHOICE_INCREF));
    while (i<n) {
      lispval key = data[i];
      if (KNO_PAIRP(key))
	results[i]=kno_prim_find((lispval)primary_index,KNO_CAR(key),KNO_CDR(key));
      else results[i]=kno_index_get((kno_index)(primary_index),key);
      i++;}
    kno_decref(aschoice);
    return kno_wrap_vector(n,results);}
  else return kno_type_error("vector","iserver_bulk_get",keys);
}
static lispval iserver_get_size(lispval key)
{
  lispval value = (PAIRP(key)) ?
    (kno_prim_find((lispval)primary_index,KNO_CAR(key),KNO_CDR(key))) :
    (kno_index_get((kno_index)(primary_index),key));
  int size = KNO_CHOICE_SIZE(value);
  kno_decref(value);
  return KNO_INT(size);
}
static lispval iserver_keys(lispval key)
{
  return kno_index_keys((kno_index)(primary_index));
}
static lispval iserver_sizes(lispval key)
{
  return kno_index_sizes((kno_index)(primary_index));
}

static lispval ixserver_get(lispval index,lispval key)
{
  if (KNO_INDEXP(index)) {
    if (PAIRP(key))
      return kno_prim_find(index,KNO_CAR(key),KNO_CDR(key));
    else return kno_index_get((kno_index)index,key);}
  else if (TABLEP(index))
    return kno_get(index,key,EMPTY);
  else return kno_type_error("index","ixserver_get",VOID);
}
static lispval ixserver_bulk_get(lispval index,lispval keys)
{
  if (VECTORP(keys)) {
    int i = 0, n = VEC_LEN(keys);
    lispval *data = VEC_DATA(keys), *results = u8_alloc_n(n,lispval);
    while (i<n) {
      lispval key = data[i];
      if (KNO_PAIRP(key))
	results[i]=kno_prim_find(index,KNO_CAR(key),KNO_CDR(key));
      else results[i]=kno_index_get((kno_index)index,key);
      i++;}
    return kno_wrap_vector(n,results);}
  else return kno_type_error("vector","iserver_bulk_get",keys);
}

static lispval ixserver_get_size(lispval index,lispval key)
{
  if (KNO_INDEXP(index)) {
    lispval value = kno_index_get(kno_indexptr(index),key);
    int size = KNO_CHOICE_SIZE(value);
    kno_decref(value);
    return KNO_INT(size);}
  else if (TABLEP(index)) {
    lispval value = kno_get(index,key,EMPTY);
    int size = KNO_CHOICE_SIZE(value);
    kno_decref(value);
    return KNO_INT(size);}
  else return kno_type_error("index","ixserver_get",VOID);
}
static lispval ixserver_keys(lispval index)
{
  if (KNO_INDEXP(index))
    return kno_index_keys(kno_indexptr(index));
  else if (TABLEP(index))
    return kno_getkeys(index);
  else return kno_type_error("index","ixserver_get",VOID);
}
static lispval ixserver_sizes(lispval index)
{
  if (KNO_INDEXP(index))
    return kno_index_sizes(kno_indexptr(index));
  else if (TABLEP(index)) {
    lispval results = EMPTY, keys = kno_getkeys(index);
    DO_CHOICES(key,keys) {
      lispval value = kno_get(index,key,EMPTY);
      lispval keypair = kno_conspair(kno_incref(key),KNO_INT(KNO_CHOICE_SIZE(value)));
      CHOICE_ADD(results,keypair);
      kno_decref(value);}
    kno_decref(keys);
    return results;}
  else return kno_type_error("index","ixserver_get",VOID);
}

/* Configuration methods */

static int serve_pool(lispval var,lispval val,void *data)
{
  kno_pool p;
  if (CHOICEP(val)) {
    DO_CHOICES(v,val) {
      int retval = serve_pool(var,v,data);
      if (retval<0) return retval;}
    return 1;}
  else if (KNO_POOLP(val)) p = kno_lisp2pool(val);
  else if (STRINGP(val)) {
    if ((p = kno_name2pool(CSTRING(val))) == NULL)
      p = kno_use_pool(CSTRING(val),0,VOID);}
  else return kno_reterr(kno_NotAPool,"serve_pool",NULL,val);
  if (p)
    if (served_poolp(p)) return 0;
    else if (n_served_pools>=KNO_DBSERV_MAX_POOLS)
      return KNO_ERR(-1,_("too many pools to serve"),"serve_pool",NULL,val);
    else {
      u8_logf(LOG_NOTICE,"SERVE_POOL","Serving objects from %s",p->poolid);
      served_pools[n_served_pools++]=p;
      return n_served_pools;}
  else return kno_reterr(kno_NotAPool,"serve_pool",NULL,val);
}

static lispval get_served_pools(lispval var,void *data)
{
  lispval result = EMPTY;
  int i = 0; while (i<n_served_pools) {
    kno_pool p = served_pools[i++];
    lispval lp = kno_pool2lisp(p);
    CHOICE_ADD(result,lp);}
  return result;
}

static int serve_primary_pool(lispval var,lispval val,void *data)
{
  kno_pool p;
  if (KNO_POOLP(val)) p = kno_lisp2pool(val);
  else if (STRINGP(val)) {
    if ((p = kno_name2pool(CSTRING(val))) == NULL)
      p = kno_use_pool(CSTRING(val),0,VOID);}
  else return kno_reterr(kno_NotAPool,"serve_pool",NULL,val);
  if (p)
    if (p == primary_pool) return 0;
    else {primary_pool = p; return 1;}
  else return kno_reterr(kno_NotAPool,"serve_pool",NULL,val);
}

static lispval get_primary_pool(lispval var,void *data)
{
  if (primary_pool) return kno_pool2lisp(primary_pool);
  else return EMPTY;
}

static int serve_index(lispval var,lispval val,void *data)
{
  kno_index ix = NULL;
  if (CHOICEP(val)) {
    DO_CHOICES(v,val) {
      int retval = serve_index(var,v,data);
      if (retval<0) return retval;}
    return 1;}
  else if (KNO_INDEXP(val))
    ix = kno_indexptr(val);
  else if (STRINGP(val))
    ix = kno_get_index(CSTRING(val),0,VOID);
  else if (val == KNO_TRUE)
    if (kno_default_background) ix = (kno_index)kno_default_background;
    else {
      u8_logf(LOG_WARN,_("No background"),
              "No current background index to serve");
      return 0;}
  else {}
  if (ix) {
    u8_logf(LOG_NOTICE,"SERVE_INDEX","Serving index %s",ix->indexid);
    kno_add_to_aggregate_index(primary_index,ix);
    return 1;}
  else return kno_reterr(kno_BadIndexSpec,"serve_index",NULL,val);
}

static lispval get_served_indexes(lispval var,void *data)
{
  return kno_index_ref((kno_index)(primary_index));
}

/* Initialization */

lispval kno_dbserv_module;

static int dbserv_init = 0;

void kno_init_dbserv_c()
{
  lispval module;

  if (dbserv_init) return; else dbserv_init = 1;

  module = kno_make_hashtable(NULL,67);

  kno_defn(module,kno_make_cprim1
           ("POOL-DATA",server_pool_data,MIN_ARGS(0),
            NULL));
  kno_defn(module,kno_make_cprim1
           ("OID-VALUE",server_oid_value,MIN_ARGS(1),
            NULL));
  kno_defn(module,kno_make_cprim1
           ("FETCH-OIDS",server_fetch_oids,MIN_ARGS(1),
            NULL));
  kno_defn(module,kno_make_cprim1
           ("GET-LOAD",server_get_load,MIN_ARGS(0),
            NULL));

  kno_defn(module,kno_make_cprim1
           ("ISERVER-GET",iserver_get,MIN_ARGS(1),
            NULL));
  kno_defn(module,kno_make_cprim1
           ("ISERVER-BULK-GET",iserver_bulk_get,MIN_ARGS(1),
	    NULL));
  kno_defn(module,kno_make_cprim1
           ("ISERVER-GET-SIZE",iserver_get_size,MIN_ARGS(1),
            NULL));
  kno_defn(module,kno_make_cprim0
           ("ISERVER-KEYS",iserver_keys,MIN_ARGS(0),
            NULL));
  kno_defn(module,kno_make_cprim0
           ("ISERVER-SIZES",iserver_sizes,MIN_ARGS(0),
            NULL));

  kno_defn(module,kno_make_cprim2
           ("IXSERVER-GET",ixserver_get,MIN_ARGS(2),
            NULL));
  kno_defn(module,kno_make_cprim2
           ("IXSERVER-BULK-GET",ixserver_bulk_get,MIN_ARGS(2),
            NULL));
  kno_defn(module,kno_make_cprim2
           ("IXSERVER-GET-SIZE",ixserver_get_size,MIN_ARGS(2),
            NULL));
  kno_defn(module,kno_make_cprim1
           ("IXSERVER-KEYS",ixserver_keys,MIN_ARGS(1),
            NULL));
  kno_defn(module,kno_make_cprim1
           ("IXSERVER-SIZES",ixserver_sizes,MIN_ARGS(1),
            NULL));

  kno_register_config("SERVEPOOLS","OID pools to be served",
                      get_served_pools,
                      serve_pool,
                      NULL);
  kno_register_config("PRIMARYPOOL","OID pool where new OIDs are allocated",
                      get_primary_pool,
                      serve_primary_pool,
                      NULL);
  kno_register_config("SERVEINDEXES","indexes to be served",
                      get_served_indexes,
                      serve_index,
                      NULL);
  kno_register_config("DBSERV:LOGLEVEL","the dbserv loglevel",
                      kno_intconfig_get,
                      kno_loglevelconfig_set,
                      &dbserv_loglevel);

  primary_index = kno_make_aggregate_index(KNO_FALSE,32,0,NULL);

  kno_dbserv_module = module;
}

static int dbserv_initialized = 0;

KNO_EXPORT int kno_init_dbserv()
{
  if (dbserv_initialized) return dbserv_initialized;
  dbserv_initialized = 211*kno_init_storage();

  u8_register_source_file(_FILEINFO);
  link_local_cprims();
  kno_init_dbserv_c();

  return 1;
}

static void link_local_cprims()
{
}
