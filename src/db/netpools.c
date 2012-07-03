/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2012 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_DTYPEIO 1

#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/dtcall.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>

#include <stdarg.h>

static struct FD_POOL_HANDLER netpool_handler;

static fdtype pool_data_symbol, new_oid_symbol, get_load_symbol;
static fdtype oid_value_symbol, fetch_oids_symbol;
static fdtype lock_oid_symbol, unlock_oid_symbol, clear_oid_lock_symbol;
static fdtype boundp, bulk_commit_symbol, quote_symbol;

static fdtype client_id=FD_VOID;
static void init_client_id(void);
#if FD_THREADS_ENABLED
static u8_mutex client_id_lock;
#endif

static int server_supportsp(struct FD_NETWORK_POOL *np,fdtype operation)
{
  fdtype request=
    fd_init_pair(NULL,boundp,fd_init_pair(NULL,operation,FD_EMPTY_LIST));
  fdtype response=fd_dteval(np->connpool,request);
  if (FD_FALSEP(response)) return 0;
  else {fd_decref(response); return 1;}
}

static void init_network_pool
  (struct FD_NETWORK_POOL *p,fdtype scan,u8_string spec,u8_string cid)
{
  FD_OID addr; unsigned int capacity; u8_string label;  
  addr=FD_OID_ADDR(FD_CAR(scan)); scan=FD_CDR(scan);
  capacity=fd_getint(FD_CAR(scan)); scan=FD_CDR(scan);
  fd_init_pool((fd_pool)p,addr,capacity,&netpool_handler,spec,cid);
  /* Network pool specific stuff */
  p->read_only=FD_FALSEP(FD_CAR(scan)); scan=FD_CDR(scan);
  if ((FD_PAIRP(scan)) && (FD_STRINGP(FD_CAR(scan))))
    label=FD_STRDATA(FD_CAR(scan));
  else label=NULL;
  if (label) p->label=u8_strdup(label); else p->label=NULL;
  /* Register the pool */
  fd_register_pool((fd_pool)p);
}

static fdtype get_pool_data(u8_string spec,u8_string *xid)
{
  fdtype request, result;
  u8_connection c=u8_connect_x(spec,xid);
  struct FD_DTYPE_STREAM _stream, *stream=fd_init_dtype_stream(&_stream,c,FD_NET_BUFSIZE);
  if (stream==NULL)
    return FD_ERROR_VALUE;
  if (FD_VOIDP(client_id)) init_client_id();
  stream->flags=stream->flags|FD_DTSTREAM_DOSYNC;
  request=fd_make_list(2,pool_data_symbol,fd_incref(client_id));
  if (fd_dtswrite_dtype(stream,request)<0) {
    fd_dtsclose(stream,1);
    return FD_ERROR_VALUE;}
  result=fd_dtsread_dtype(stream);
  fd_decref(request);
  fd_dtsclose(stream,1);
  return result;
}

FD_EXPORT fd_pool fd_open_network_pool(u8_string spec,int read_only)
{
  struct FD_NETWORK_POOL *np=u8_alloc(struct FD_NETWORK_POOL);
  u8_string xid=NULL;
  fdtype pooldata=get_pool_data(spec,&xid);
  u8_string cid=u8_canonical_addr(spec);
  if (FD_ABORTP(pooldata)) {
    u8_free(np); u8_free(cid);
    return NULL;}
  if (FD_VOIDP(client_id)) init_client_id();
  np->cid=cid; np->xid=xid;
  np->connpool=u8_open_connpool(spec,fd_dbconn_reserve_default,fd_dbconn_cap_default,fd_dbconn_init_default);
  if (((np)->connpool)==NULL) {
    u8_free(np); u8_free(cid);
    return NULL;}
  else pooldata=fd_dtcall(np->connpool,2,pool_data_symbol,client_id);
  if (FD_ABORTP(pooldata)) {
    u8_free(np); u8_free(cid); u8_free(xid);
    return NULL;}
  /* The server actually serves multiple pools */
  else if ((FD_CHOICEP(pooldata)) || (FD_VECTORP(pooldata))) {
    const fdtype *scan, *limit; int n_pools=0;
    if (FD_CHOICEP(pooldata)) {
      scan=FD_CHOICE_DATA(pooldata); limit=scan+FD_CHOICE_SIZE(pooldata);}
    else {
      scan=FD_VECTOR_DATA(pooldata); limit=scan+FD_VECTOR_LENGTH(pooldata);}
    while (scan<limit) {
      struct FD_NETWORK_POOL *p; fdtype pd=*scan++;
      if (n_pools==0) p=np;
      else p=u8_alloc(struct FD_NETWORK_POOL);
      init_network_pool(p,pd,spec,cid);
      p->xid=xid; p->connpool=np->connpool;
      n_pools++;}}
  else init_network_pool(np,pooldata,spec,cid);
  u8_free(cid);
  np->bulk_commitp=server_supportsp(np,bulk_commit_symbol);
  fd_decref(pooldata);
  return (fd_pool)np;
}

static int network_pool_load(fd_pool p)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype value;
  value=fd_dtcall(np->connpool,2,get_load_symbol,fd_make_oid(p->base));
  if (FD_FIXNUMP(value)) return FD_FIX2INT(value);
  else if (FD_ABORTP(value))
    return fd_interr(value);
  else {
    fd_seterr(fd_BadServerResponse,"POOL-LOAD",NULL,value);
    return -1;}
}

static fdtype network_pool_fetch(fd_pool p,fdtype oid)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype value;
  value=fd_dtcall(np->connpool,2,oid_value_symbol,oid);
  return value;
}

static fdtype *network_pool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype oidvec=fd_make_vector(n,oids);
  fdtype value=fd_dtcall(np->connpool,2,fetch_oids_symbol,oidvec);
  fd_decref(oidvec);
  if (FD_VECTORP(value)) {
    fdtype *values=u8_alloc_n(n,fdtype);
    memcpy(values,FD_VECTOR_ELTS(value),sizeof(fdtype)*n);
    return values;}
  else {
    fd_seterr(fd_BadServerResponse,"netpool_fetchn",
	      u8_strdup(np->cid),fd_incref(value));
    return NULL;}
}

static int network_pool_lock(fd_pool p,fdtype oid)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype value;
  value=fd_dtcall(np->connpool,3,lock_oid_symbol,oid,client_id);
  if (FD_VOIDP(value)) return 0;
  else if (FD_ABORTP(value)) 
    return fd_interr(value);
  else {
    fd_hashtable_store(&(p->cache),oid,value);
    fd_decref(value);
    return 1;}
}

static int network_pool_unlock(fd_pool p,fdtype oids)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype result;
  result=fd_dtcall(np->connpool,3,clear_oid_lock_symbol,oids,client_id);
  if (FD_ABORTP(result)) {
    fd_decref(result); return 0;}
  else {fd_decref(result); return 1;}
}

static int network_pool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  if (np->bulk_commitp) {
    int i=0;
    fdtype *storevec=u8_alloc_n(n*2,fdtype), vec, result;
    while (i < n) {
      storevec[i*2]=oids[i];
      storevec[i*2+1]=values[i];
      i++;}
    vec=fd_init_vector(NULL,n*2,storevec);
    result=fd_dtcall(np->connpool,3,bulk_commit_symbol,client_id,vec);
    /* Don't decref the individual elements because you didn't incref them. */
    u8_free((struct FD_CONS *)vec); u8_free(storevec);
    fd_decref(result);
    return 1;}
  else {
    int i=0;
    while (i < n) {
      fdtype result=fd_dtcall(np->connpool,4,unlock_oid_symbol,oids[i],client_id,values[i]);
      fd_decref(result); i++;}
    return 1;}
}

static void network_pool_close(fd_pool p)
{
}

static void network_pool_setbuf(fd_pool p,int bufsiz)
{
}

static fdtype network_pool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE, request; int i=0;
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  request=fd_init_pair(NULL,new_oid_symbol,FD_EMPTY_LIST);
  while (i < n) {
    fdtype result=fd_dteval(np->connpool,request);
    FD_ADD_TO_CHOICE(results,result);
    i++;}
  return results;
}

static struct FD_POOL_HANDLER netpool_handler={
  "netpool", 1, sizeof(struct FD_NETWORK_POOL), 12,
  network_pool_close, /* close */
  NULL, /* setcache */
  network_pool_setbuf, /* setbuf */
  network_pool_alloc, /* alloc */
  network_pool_fetch, /* fetch */
  network_pool_fetchn, /* fetchn */
  network_pool_load, /* getload */
  network_pool_lock, /* lock */
  network_pool_unlock, /* release */
  network_pool_storen, /* storen */
  NULL, /* swapout */
  NULL, /* metadata */
  NULL}; /* sync */

static void init_client_id()
{
  fd_lock_mutex(&client_id_lock);
  if (FD_VOIDP(client_id)) client_id=fdtype_string(u8_sessionid());
  fd_unlock_mutex(&client_id_lock);
}

FD_EXPORT void fd_init_netpools_c()
{
  fd_register_source_file(_FILEINFO);

#if FD_THREADS_ENABLED
  fd_init_mutex(&client_id_lock);
#endif

  pool_data_symbol=fd_intern("POOL-DATA");
  new_oid_symbol=fd_intern("NEW-OID");
  oid_value_symbol=fd_intern("OID-VALUE");
  lock_oid_symbol=fd_intern("LOCK-OID");
  unlock_oid_symbol=fd_intern("UNLOCK-OID");
  clear_oid_lock_symbol=fd_intern("CLEAR-OID-LOCK");
  fetch_oids_symbol=fd_intern("FETCH-OIDS");
  bulk_commit_symbol=fd_intern("BULK-COMMIT");
  get_load_symbol=fd_intern("GET-LOAD");
  boundp=fd_intern("BOUND?");
  quote_symbol=fd_intern("QUOTE");
}
