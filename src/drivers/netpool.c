/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"
#define KNO_INLINE_POOLS 1
#define KNO_INLINE_BUFIO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"
#include "kno/remote.h"

#include "headers/netpool.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>

#include <stdarg.h>

static struct KNO_POOL_HANDLER netpool_handler;

static lispval pool_data_symbol, new_oid_symbol, get_load_symbol;
static lispval oid_value_symbol, fetch_oids_symbol;
static lispval lock_oid_symbol, unlock_oid_symbol, clear_oid_lock_symbol;
static lispval boundp, bulk_commit_symbol;

static lispval client_id = VOID;
static void init_client_id(void);
static u8_mutex client_id_lock;

static int server_supportsp(struct KNO_NETWORK_POOL *np,lispval operation)
{
  lispval request=
    kno_conspair(boundp,kno_conspair(operation,NIL));
  lispval response = kno_neteval(np->pool_server,request);
  kno_decref(request);
  if (FALSEP(response)) return 0;
  else {kno_decref(response); return 1;}
}

static void init_network_pool
(struct KNO_NETWORK_POOL *p,lispval netinfo,
 u8_string spec,u8_string source,kno_storage_flags flags,
 lispval opts)
{
  lispval scan = netinfo;
  KNO_OID addr; unsigned int capacity; u8_string label;
  addr = KNO_OID_ADDR(KNO_CAR(scan)); scan = KNO_CDR(scan);
  capacity = kno_getint(KNO_CAR(scan)); scan = KNO_CDR(scan);
  kno_init_pool((kno_pool)p,addr,capacity,&netpool_handler,
               spec,source,source,
               flags,KNO_VOID,opts);
  /* Network pool specific stuff */
  if (FALSEP(KNO_CAR(scan)))
    p->pool_flags |= KNO_STORAGE_READ_ONLY;
  scan = KNO_CDR(scan);
  if ((PAIRP(scan)) && (STRINGP(KNO_CAR(scan))))
    label = CSTRING(KNO_CAR(scan));
  else label = NULL;
  if (label)
    p->pool_label = u8_strdup(label);
  else p->pool_label = NULL;
  /* Register the pool */
  kno_register_pool((kno_pool)p);
}

static lispval get_pool_data(u8_string spec,u8_string *xid)
{
  lispval request, result;
  u8_socket c = u8_connect_x(spec,xid);
  struct KNO_STREAM _stream, *stream=
    kno_init_stream(&_stream,spec,c,
                   KNO_STREAM_DOSYNC|KNO_STREAM_SOCKET,
                   KNO_NETWORK_BUFSIZE);
  struct KNO_OUTBUF *outstream = (stream) ? (kno_writebuf(stream)) :(NULL);
  if (stream == NULL)
    return KNO_ERROR;
  if (VOIDP(client_id)) init_client_id();
  request = kno_make_list(2,pool_data_symbol,kno_incref(client_id));
  /* u8_logf(LOG_WARN,"GETPOOLDATA","Making request (on #%d) for %q",c,request); */
  if (kno_write_dtype(outstream,request)<0) {
    kno_free_stream(stream);
    kno_decref(request);
    return KNO_ERROR;}
  kno_decref(request);
  result = kno_read_dtype(kno_readbuf(stream));
  /* u8_logf(LOG_WARN,"GETPOOLDATA","Got result (on #%d)",c,request); */
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  return result;
}

KNO_EXPORT kno_pool kno_open_network_pool(u8_string spec,
                                       kno_storage_flags flags,
                                       lispval opts)
{
  struct KNO_NETWORK_POOL *np = u8_alloc(struct KNO_NETWORK_POOL);
  u8_string xid = NULL;
  lispval pooldata = get_pool_data(spec,&xid);
  u8_string cid = u8_canonical_addr(spec);
  if (KNO_ABORTP(pooldata)) {
    u8_free(np); u8_free(cid);
    return NULL;}
  if (VOIDP(client_id)) init_client_id();
  np->poolid = cid; np->pool_source = xid;
  np->pool_server = kno_open_evalserver(spec,opts);
  if (((np)->pool_server) == NULL) {
    u8_free(np); u8_free(cid);
    kno_decref(pooldata);
    return NULL;}
  else {
    kno_decref(pooldata);
    pooldata = kno_remote_call(np->pool_server,2,pool_data_symbol,client_id);}
  if (KNO_ABORTP(pooldata)) {
    u8_free(np); u8_free(cid); u8_free(xid);
    return NULL;}
  /* The server actually serves multiple pools */
  else if ((CHOICEP(pooldata)) || (VECTORP(pooldata))) {
    const lispval *scan, *limit; int n_pools = 0;
    if (CHOICEP(pooldata)) {
      scan = KNO_CHOICE_DATA(pooldata);
      limit = scan+KNO_CHOICE_SIZE(pooldata);}
    else {
      scan = VEC_DATA(pooldata);
      limit = scan+VEC_LEN(pooldata);}
    while (scan<limit) {
      struct KNO_NETWORK_POOL *p; lispval pd = *scan++;
      if (n_pools==0) p = np;
      else p = u8_alloc(struct KNO_NETWORK_POOL);
      init_network_pool(p,pd,spec,cid,flags,opts);
      p->pool_source = xid;
      p->pool_server = np->pool_server;
      n_pools++;}}
  else init_network_pool(np,pooldata,spec,cid,flags,opts);
  u8_free(cid);
  np->bulk_commitp = server_supportsp(np,bulk_commit_symbol);
  kno_decref(pooldata);
  return (kno_pool)np;
}

static int network_pool_load(kno_pool p)
{
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  lispval value;
  value = kno_remote_call(np->pool_server,2,get_load_symbol,kno_make_oid(p->pool_base));
  if (KNO_UINTP(value)) return FIX2INT(value);
  else if (KNO_ABORTP(value))
    return kno_interr(value);
  else {
    kno_seterr(kno_BadServerResponse,"POOL-LOAD",NULL,value);
    return -1;}
}

static lispval network_pool_fetch(kno_pool p,lispval oid)
{
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  lispval value;
  value = kno_remote_call(np->pool_server,2,oid_value_symbol,oid);
  return value;
}

static lispval *network_pool_fetchn(kno_pool p,int n,lispval *oids)
{
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  lispval oidvec = kno_make_vector(n,oids);
  lispval value = kno_remote_call(np->pool_server,2,fetch_oids_symbol,oidvec);
  kno_decref(oidvec);
  if (VECTORP(value)) {
    lispval *values = u8_alloc_n(n,lispval);
    memcpy(values,KNO_VECTOR_ELTS(value),LISPVEC_BYTELEN(n));
    return values;}
  else {
    kno_seterr(kno_BadServerResponse,"netpool_fetchn",
              np->poolid,kno_incref(value));
    return NULL;}
}

static int network_pool_lock(kno_pool p,lispval oid)
{
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  lispval value;
  value = kno_remote_call(np->pool_server,3,lock_oid_symbol,oid,client_id);
  if (VOIDP(value)) return 0;
  else if (KNO_ABORTP(value))
    return kno_interr(value);
  else {
    kno_hashtable_store(&(p->pool_cache),oid,value);
    kno_decref(value);
    return 1;}
}

static int network_pool_unlock(kno_pool p,lispval oids)
{
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  lispval result;
  result = kno_remote_call(np->pool_server,3,clear_oid_lock_symbol,oids,client_id);
  if (KNO_ABORTP(result)) {
    kno_decref(result); return 0;}
  else {kno_decref(result); return 1;}
}

static int network_pool_storen(kno_pool p,int n,lispval *oids,lispval *values)
{
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  if (np->bulk_commitp) {
    int i = 0;
    lispval *storevec = u8_alloc_n(n*2,lispval), vec, result;
    while (i < n) {
      storevec[i*2]=oids[i];
      storevec[i*2+1]=values[i];
      i++;}
    vec = kno_wrap_vector(n*2,storevec);
    result = kno_remote_call(np->pool_server,3,bulk_commit_symbol,client_id,vec);
    /* Don't decref the individual elements because you didn't incref them. */
    u8_free((struct KNO_CONS *)vec); u8_free(storevec);
    kno_decref(result);
    return 1;}
  else {
    int i = 0;
    while (i < n) {
      lispval result = kno_remote_call(np->pool_server,4,unlock_oid_symbol,oids[i],client_id,values[i]);
      kno_decref(result); i++;}
    return 1;}
}

static int network_pool_commit(kno_pool p,kno_commit_phase phase,
                               struct KNO_POOL_COMMITS *commits)
{
  switch (phase) {
  case kno_commit_write:
    return network_pool_storen(p,commits->commit_count,
                               commits->commit_oids,
                               commits->commit_vals);
  default: {
    u8_logf(LOG_INFO,"NoPhasedCommit",
            "The pool %s doesn't support phased commits",
            p->poolid);
    return 0;}
  }
}

static void network_pool_close(kno_pool p)
{
}

static lispval network_pool_alloc(kno_pool p,int n)
{
  lispval results = EMPTY, request; int i = 0;
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  request = kno_conspair(new_oid_symbol,NIL);
  while (i < n) {
    lispval result = kno_neteval(np->pool_server,request);
    CHOICE_ADD(results,result);
    i++;}
  return results;
}

static struct KNO_POOL_HANDLER netpool_handler={
  "netpool", 1, sizeof(struct KNO_NETWORK_POOL), 12,
  network_pool_close, /* close */
  network_pool_alloc, /* alloc */
  network_pool_fetch, /* fetch */
  network_pool_fetchn, /* fetchn */
  network_pool_load, /* getload */
  network_pool_lock, /* lock */
  network_pool_unlock, /* release */
  network_pool_commit, /* commit */
  NULL, /* swapout */
  NULL, /* create */
  NULL,  /* walk */
  NULL, /* recycle */
  NULL  /* poolctl */
};

static void init_client_id()
{
  u8_lock_mutex(&client_id_lock);
  if (VOIDP(client_id)) client_id = kno_mkstring(u8_sessionid());
  u8_unlock_mutex(&client_id_lock);
}

KNO_EXPORT void kno_init_netpool_c()
{
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&client_id_lock);

  pool_data_symbol = kno_intern("pool-data");
  new_oid_symbol = kno_intern("new-oid");
  oid_value_symbol = kno_intern("oid-value");
  lock_oid_symbol = kno_intern("lock-oid");
  unlock_oid_symbol = kno_intern("unlock-oid");
  clear_oid_lock_symbol = kno_intern("clear-oid-lock");
  fetch_oids_symbol = kno_intern("fetch-oids");
  bulk_commit_symbol = kno_intern("bulk-commit");
  get_load_symbol = kno_intern("get-load");
  boundp = kno_intern("bound?");

  kno_register_pool_type
    ("network_pool",
     &netpool_handler,
     kno_open_network_pool,
     kno_netspecp,
     (void*)NULL);


}

