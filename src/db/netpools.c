/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
   "$Id$";

#define FD_INLINE_DTYPEIO 1

#include "fdb/dtype.h"
#include "fdb/fddb.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>

#include <stdarg.h>

static struct FD_POOL_HANDLER netpool_handler;

static fdtype pool_data_symbol, new_oid_symbol, get_load_symbol;
static fdtype oid_value_symbol, fetch_oids_symbol;
static fdtype lock_oid_symbol, unlock_oid_symbol, clear_oid_lock_symbol;
static fdtype boundp, bulk_commit_symbol;

static fdtype client_id=FD_VOID;
static void init_client_id(void);
#if FD_THREADS_ENABLED
static u8_mutex client_id_lock;
#endif

static int reopen_network_pool(struct FD_NETWORK_POOL *p);

static fdtype dteval(struct FD_NETWORK_POOL *np,fdtype expr)
{
  fd_dtype_stream stream=&(np->stream);
  if (stream->fd<0)
    if (reopen_network_pool(np)<0) return FD_VOID;
  fd_dtswrite_dtype(stream,expr);
  return fd_dtsread_dtype(stream); 
}

static fdtype dtcall(struct FD_NETWORK_POOL *np,int n_elts,...)
{
  fd_dtype_stream stream=&(np->stream); 
  int i=0; fdtype *params=u8_alloc_n(n_elts,fdtype), request, result;
  va_list args;
  if (stream->fd<0)
    if (reopen_network_pool(np)<0) return FD_ERROR_VALUE;
  va_start(args,n_elts);
  while (i<n_elts) params[i++]=va_arg(args,fdtype);
  request=FD_EMPTY_LIST; i=n_elts-1;
  while (i>=0) {
    request=fd_init_pair(NULL,params[i],request);
    fd_incref(params[i]); i--;}
  u8_free(params);
  if ((fd_dtswrite_dtype(stream,request)<0) ||
      (fd_dtsflush(stream)<0)) {
    /* Close the stream and sleep a second before reconnecting. */
    u8_log(LOG_WARN,fd_ServerReconnect,"Resetting connection to %s",np->xid);
    fd_dtsclose(stream,1); sleep(1);
    if ((reopen_network_pool(np)<0) ||
	(fd_dtswrite_dtype(stream,request)<0)) {
      fd_decref(request);
      return FD_ERROR_VALUE;}}
  result=fd_dtsread_dtype(stream);
  if (FD_EQ(result,FD_EOD)) {
    /* Close the stream and sleep a second before reconnecting. */
    u8_log(LOG_WARN,fd_ServerReconnect,"Resetting connection to %s",np->xid);
    fd_dtsclose(stream,1); sleep(1);
    if ((reopen_network_pool(np)<0) ||
	(fd_dtswrite_dtype(stream,request)<0)) {
      fd_decref(request);
      return FD_ERROR_VALUE;}
    else result=fd_dtsread_dtype(stream);
    if (FD_EQ(result,FD_EOD))
      return fd_err(fd_UnexpectedEOD,"dtcall/netpools.c",np->xid,FD_VOID);}
  fd_decref(request);
  return result;
}

static int server_supportsp(struct FD_NETWORK_POOL *np,fdtype operation)
{
  fd_dtype_stream stream=&(np->stream);
  fdtype request=
    fd_init_pair(NULL,boundp,fd_init_pair(NULL,operation,FD_EMPTY_LIST));
  fdtype response;
  if (stream->fd<0)
    if (reopen_network_pool(np)<0) return FD_VOID;
  fd_dtswrite_dtype(stream,request); fd_decref(request);
  response=fd_dtsread_dtype(stream);
  if (FD_FALSEP(response)) return 0;
  else {fd_decref(response); return 1;}
}

static void init_network_pool
  (struct FD_NETWORK_POOL *p,fdtype scan,u8_string spec,u8_string cid)
{
  FD_OID addr; unsigned int capacity, load; u8_string label;  
  addr=FD_OID_ADDR(FD_CAR(scan)); scan=FD_CDR(scan);
  capacity=fd_getint(FD_CAR(scan)); scan=FD_CDR(scan);
  fd_init_pool((fd_pool)p,addr,capacity,&netpool_handler,spec,cid);
  /* Network pool specific stuff */
  p->read_only=FD_FALSEP(FD_CAR(scan)); scan=FD_CDR(scan);
  if ((FD_PAIRP(scan)) && (FD_STRINGP(FD_CAR(scan))))
    label=FD_STRDATA(FD_CAR(scan));
  else label=NULL;
  fd_init_mutex(&(p->lock));
  if (label) p->label=u8_strdup(label); else p->label=NULL;
  /* Register the pool */
  fd_register_pool((fd_pool)p);
}

FD_EXPORT fd_pool fd_open_network_pool(u8_string spec,int read_only)
{
  struct FD_NETWORK_POOL *np=u8_alloc(struct FD_NETWORK_POOL);
  fd_dtype_stream s=&(np->stream); u8_string xid=NULL;
  u8_connection sock=u8_connect_x(spec,&xid);
  fdtype pooldata=FD_VOID;
  int n_pools=0; 
  u8_string cid=u8_canonical_addr(spec);
  if (sock<0) {
    u8_free(np);
    return (fd_pool) NULL;}
  else if (u8_set_nodelay(sock,1)<0) {
    u8_free(np);
    return (fd_pool) NULL;}
  if (FD_VOIDP(client_id)) init_client_id();
  fd_init_dtype_stream(s,sock,FD_NET_BUFSIZE);
  s->flags=s->flags|FD_DTSTREAM_DOSYNC;
  np->cid=cid; np->xid=xid;
  pooldata=dtcall(np,2,pool_data_symbol,client_id);
  if (FD_ABORTP(pooldata)) {
    fd_interr(pooldata);
    fd_dtsclose(s,1);
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
      else {
	u8_connection newsock=u8_connect_x(spec,&xid);
	if (u8_set_nodelay(sock,1)<0) 
	  u8_log(LOG_WARN,"DelayError","Can't set delay flag for socket");
	p=u8_alloc(struct FD_NETWORK_POOL);
	fd_init_dtype_stream(&(p->stream),newsock,FD_NET_BUFSIZE);
	p->xid=xid;
	p->stream.mallocd=0;}
      init_network_pool(p,pd,spec,cid);
      p->xid=xid;
      n_pools++;}}
  else init_network_pool(np,pooldata,spec,cid);
  u8_free(cid);
  np->bulk_commitp=server_supportsp(np,bulk_commit_symbol);
  fd_decref(pooldata);
  return (fd_pool)np;
}

static int reopen_network_pool(struct FD_NETWORK_POOL *p)
{
  /* This reopens the socket for a closed network pool. */
  if (p->stream.fd>=0) return 0;
  else {
    u8_string xid=NULL;
    u8_connection newsock=u8_connect_x(p->source,&xid);
    if (u8_set_nodelay(newsock,1)<0) return -1;
    if (p->xid) {u8_free(p->xid); p->xid=NULL;}
    if (newsock>=0) {
      p->xid=xid;
      fd_init_dtype_stream(&(p->stream),newsock,FD_NET_BUFSIZE);
      return 1;}
    else return -1;}
}

static int network_pool_load(fd_pool p)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype value;
  fd_lock_struct(np);
  value=dtcall(np,2,get_load_symbol,fd_make_oid(p->base));
  fd_unlock_struct(np);
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
  fd_lock_struct(np);
  value=dtcall(np,2,oid_value_symbol,oid);
  fd_unlock_struct(np);
  return value;
}

static fdtype *network_pool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype vector, request, value, *values;
  struct FD_VECTOR *v; int i;
  fd_lock_struct(np);
  vector=fd_init_vector(NULL,n,oids); fd_incref(vector);
  value=dtcall(np,2,fetch_oids_symbol,vector);
  if (FD_VECTORP(value)) {
    v=FD_GET_CONS(value,fd_vector_type,struct FD_VECTOR *);
    values=v->data;
    /* Note that calling fd_free directly (rather than fd_decref)
       doesn't free the internal data of the vector, which is just
       what we want.in this case. */
    u8_free((struct FD_VECTOR *)vector); u8_free(v);
    fd_unlock_struct(np);
    return values;}
  else {
    fd_unlock_struct(np);
    fd_seterr(fd_BadServerResponse,"netpool_fetchn",
	      u8_strdup(np->cid),fd_incref(value));
    return NULL;}
}

static int network_pool_lock(fd_pool p,fdtype oid)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype value;
  fd_lock_struct(np);
  value=dtcall(np,3,lock_oid_symbol,oid,client_id);
  fd_unlock_struct(np);
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
  fd_lock_struct(np);
  result=dtcall(np,3,clear_oid_lock_symbol,oids,client_id);
  fd_unlock_struct(np);
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
    fd_lock_struct(np);
    while (i < n) {
      storevec[i*2]=oids[i];
      storevec[i*2+1]=values[i];
      i++;}
    vec=fd_init_vector(NULL,n*2,storevec);
    result=dtcall(np,3,bulk_commit_symbol,client_id,vec);
    /* Don't decref the individual elements because you didn't incref them. */
    u8_free((struct FD_CONS *)vec); u8_free(storevec);
    fd_unlock_struct(np);
    return 1;}
  else {
    int i=0;
    fd_lock_struct(np);
    while (i < n) {
      fdtype result=dtcall(np,4,unlock_oid_symbol,oids[i],client_id,values[i]);
      fd_decref(result); i++;}
    fd_unlock_struct(np);
    return 1;}
}

static void network_pool_close(fd_pool p)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fd_lock_struct(np);
  fd_dtsclose(&(np->stream),1);
  fd_unlock_struct(np);
}

static void network_pool_setbuf(fd_pool p,int bufsiz)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fd_lock_struct(np);
  fd_dtsbufsize(&(np->stream),bufsiz);
  fd_unlock_struct(np);
}

static fdtype network_pool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE, request; int i=0;
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fd_dtype_stream stream=&(np->stream);
  request=fd_init_pair(NULL,new_oid_symbol,FD_EMPTY_LIST);
  fd_lock_struct(np);
  while (i < n) {
    fdtype result=dteval(np,request);
    FD_ADD_TO_CHOICE(results,result);
    i++;}
  fd_unlock_struct(np);
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
  fd_register_source_file(versionid);

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
}
