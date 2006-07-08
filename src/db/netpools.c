/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
   "$Id$";

#define FD_INLINE_DTYPEIO 1

#include "fdb/dtype.h"
#include "fdb/pools.h"

#include <libu8/u8.h>
#include <libu8/netfns.h>

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
  int i=0; fdtype *params=u8_malloc(sizeof(fdtype)*n_elts), request;
  va_list args;
  if (stream->fd<0)
    if (reopen_network_pool(np)<0) return FD_VOID;
  va_start(args,n_elts);
  while (i<n_elts) params[i++]=va_arg(args,fdtype);
  request=FD_EMPTY_LIST; i=n_elts-1;
  while (i>=0) {
    request=fd_init_pair(NULL,params[i],request);
    fd_incref(params[i]); i--;}
  fd_dtswrite_dtype(stream,request); fd_decref(request);
  u8_free(params);
  return fd_dtsread_dtype(stream); 
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
  u8_init_mutex(&(p->lock));
  if (label) p->label=u8_strdup(label); else p->label=NULL;
  /* Register the pool */
  fd_register_pool((fd_pool)p);
}

FD_EXPORT fd_pool fd_open_network_pool(u8_string spec,int read_only)
{
  struct FD_NETWORK_POOL *np=u8_malloc(sizeof(struct FD_NETWORK_POOL));
  fd_dtype_stream s=&(np->stream);
  u8_connection sock=u8_connect(spec);
  fdtype pooldata=FD_VOID;
  int n_pools=0; 
  u8_string cid=u8_canonical_addr(spec);
  if (sock<0) {
    u8_free(np);
    return (fd_pool) NULL;}
  if (FD_VOIDP(client_id)) init_client_id();
  fd_init_dtype_stream(s,sock,FD_NET_BUFSIZE,NULL,NULL);
  s->bits=s->bits|FD_DTSTREAM_DOSYNC;
  np->cid=cid; 
  pooldata=dtcall(np,2,pool_data_symbol,client_id);
  if (FD_ABORTP(pooldata)) {
    fd_interr(pooldata);
    fd_dtsclose(s,1);
    u8_free(np); u8_free(cid);
    return NULL;}
  else if (FD_CHOICEP(pooldata)) {
    FD_DO_CHOICES(pd,pooldata) {
      struct FD_NETWORK_POOL *p;
      fdtype scan;
      if (n_pools==0) p=np;
      else {
	u8_connection newsock=u8_connect(spec);
	p=u8_malloc(sizeof(struct FD_NETWORK_POOL));
	fd_init_dtype_stream(&(p->stream),newsock,FD_NET_BUFSIZE,NULL,NULL);
	p->stream.mallocd=0;}
      init_network_pool(p,pd,spec,cid);}}
  else if (FD_VECTORP(pooldata)) {
    int i=1, len=FD_VECTOR_LENGTH(pooldata);
    init_network_pool(np,FD_VECTOR_REF(pooldata,0),spec,cid);
    while (i < len) {
      u8_connection newsock=u8_connect(spec);
      struct FD_NETWORK_POOL *p=u8_malloc(sizeof(struct FD_NETWORK_POOL));
      fd_init_dtype_stream(&(p->stream),newsock,FD_NET_BUFSIZE,NULL,NULL);
      p->stream.mallocd=0;
      init_network_pool(p,FD_VECTOR_REF(pooldata,i),spec,cid);
      i++;}}
  else init_network_pool(np,pooldata,spec,cid);
  u8_free(cid);
  np->bulk_commitp=server_supportsp(np,bulk_commit_symbol);
  fd_decref(pooldata);
  return (fd_pool)np;
}

static int reopen_network_pool(struct FD_NETWORK_POOL *p)
{
  if (p->stream.fd>=0) return 0;
  else {
    u8_connection newsock=u8_connect(p->source);
    if (newsock>=0) {
      fd_init_dtype_stream(&(p->stream),newsock,FD_NET_BUFSIZE,NULL,NULL);
      return 1;}
    else return -1;}
}

static int network_pool_load(fd_pool p)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype value;
  u8_lock_mutex(&(np->lock));
  value=dtcall(np,2,get_load_symbol,fd_make_oid(p->base));
  u8_unlock_mutex(&(np->lock));
  if (FD_FIXNUMP(value)) return FD_FIX2INT(value);
  else if (FD_EXCEPTIONP(value))
    return fd_interr(value);
  else {
    fd_seterr(fd_BadServerResponse,"POOL-LOAD",NULL,value);
    return -1;}
}

static fdtype network_pool_fetch(fd_pool p,fdtype oid)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype value;
  u8_lock_mutex(&(np->lock));
  value=dtcall(np,2,oid_value_symbol,oid);
  u8_unlock_mutex(&(np->lock));
  return value;
}

static fdtype *network_pool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype vector, request, value, *values;
  struct FD_VECTOR *v; int i;
  u8_lock_mutex(&(np->lock));
  vector=fd_init_vector(NULL,n,oids); fd_incref(vector);
  value=dtcall(np,2,fetch_oids_symbol,vector);
  if (FD_VECTORP(value)) {
    v=FD_GET_CONS(value,fd_vector_type,struct FD_VECTOR *);
    values=v->data;
    /* Note that calling fd_free directly (rather than fd_decref)
       doesn't free the internal data of the vector, which is just
       what we want.in this case. */
    u8_free((struct FD_VECTOR *)vector); u8_free(v);
    u8_unlock_mutex(&(np->lock));
    return values;}
  else {
    u8_unlock_mutex(&(np->lock));
    fd_seterr(fd_BadServerResponse,"netpool_fetchn",
	      u8_strdup(np->cid),fd_incref(value));
    return NULL;}
}

static int network_pool_lock(fd_pool p,fdtype oid)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype value;
  u8_lock_mutex(&(np->lock));
  value=dtcall(np,3,lock_oid_symbol,oid,client_id);
  u8_unlock_mutex(&(np->lock));
  if (FD_VOIDP(value)) return 0;
  else {
    fd_hashtable_store(&(p->cache),oid,value);
    fd_decref(value);
    return 1;}
}

static int network_pool_unlock(fd_pool p,fdtype oids)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fdtype result;
  u8_lock_mutex(&(np->lock));
  result=dtcall(np,3,clear_oid_lock_symbol,oids,client_id);
  u8_unlock_mutex(&(np->lock));
  if (FD_EXCEPTIONP(result)) {
    fd_decref(result); return 0;}
  else {fd_decref(result); return 1;}
}

static int network_pool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  if (np->bulk_commitp) {
    int i=0;
    fdtype *storevec=u8_malloc(sizeof(fdtype)*n*2), vec, result;
    u8_lock_mutex(&(np->lock));
    while (i < n) {
      storevec[i*2]=oids[i];
      storevec[i*2+1]=values[i];
      i++;}
    vec=fd_init_vector(NULL,n*2,storevec);
    result=dtcall(np,3,bulk_commit_symbol,client_id,vec);
    /* Don't decref the individual elements because you didn't incref them. */
    u8_free((struct FD_CONS *)vec); u8_free(storevec);
    u8_unlock_mutex(&(np->lock));
    return 1;}
  else {
    int i=0;
    u8_lock_mutex(&(np->lock));
    while (i < n) {
      fdtype result=dtcall(np,4,unlock_oid_symbol,oids[i],client_id,values[i]);
      fd_decref(result); i++;}
    u8_unlock_mutex(&(np->lock));
    return 1;}
}

static void network_pool_close(fd_pool p)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  u8_lock_mutex(&(np->lock));
  fd_dtsclose(&(np->stream),1);
  u8_unlock_mutex(&(np->lock));
}

static void network_pool_setbuf(fd_pool p,int bufsiz)
{
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  u8_lock_mutex(&(np->lock));
  fd_dtsbufsize(&(np->stream),bufsiz);
  u8_unlock_mutex(&(np->lock));
}

static fdtype network_pool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE, request; int i=0;
  struct FD_NETWORK_POOL *np=(struct FD_NETWORK_POOL *)p;
  fd_dtype_stream stream=&(np->stream);
  request=fd_init_pair(NULL,new_oid_symbol,FD_EMPTY_LIST);
  u8_lock_mutex(&(np->lock));
  while (i < n) {
    fdtype result=dteval(np,request);
    FD_ADD_TO_CHOICE(results,result);
    i++;}
  u8_unlock_mutex(&(np->lock));
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
  u8_lock_mutex(&client_id_lock);
  if (FD_VOIDP(client_id)) client_id=fdtype_string(u8_sessionid());
  u8_unlock_mutex(&client_id_lock);
}

FD_EXPORT fd_init_netpools_c()
{
  fd_register_source_file(versionid);

#if FD_THREADS_ENABLED
  u8_init_mutex(&client_id_lock);
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


/* The CVS log for this file
   $Log: netpools.c,v $
   Revision 1.39  2006/01/31 13:47:23  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.38  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.37  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.36  2005/12/06 14:58:37  haase
   Fixed network pool getload function

   Revision 1.35  2005/08/11 12:43:10  haase
   Trying to use a network pool adopts any errors returned from the POOL-DATA request

   Revision 1.34  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.33  2005/06/04 12:44:09  haase
   Fixed error catching for prefetches

   Revision 1.32  2005/05/30 00:03:54  haase
   Fixes to pool declaration, allowing the USE-POOL primitive to return multiple pools correctly when given a ; spearated list or a pool server which provides multiple pools

   Revision 1.31  2005/05/26 20:48:42  haase
   Made USE-POOL return a pool, rather than void, again

   Revision 1.30  2005/05/23 00:53:24  haase
   Fixes to header ordering to get off_t consistently defined

   Revision 1.29  2005/05/22 20:30:03  haase
   Pass initialization errors out of config-def! and other error improvements

   Revision 1.28  2005/05/21 18:00:19  haase
   Enlarged network buffer sizes and regularized in FD_NET_BUFSIZE

   Revision 1.27  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.26  2005/04/17 14:06:41  haase
   Fix network pool calls to dtcall

   Revision 1.25  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.24  2005/04/14 00:33:43  haase
   Made newtork streams be synchronized

   Revision 1.23  2005/04/04 22:20:36  haase
   Improved integration of error facilities

   Revision 1.22  2005/03/31 16:35:00  haase
   Delay getting session id until needed

   Revision 1.21  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.20  2005/03/28 19:19:36  haase
   Added metadata reading and writing and file pool/index creation

   Revision 1.19  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.18  2005/02/19 16:25:02  haase
   Replaced fd_parse with fd_intern

   Revision 1.17  2005/02/11 19:04:12  haase
   Fixes to compilation using POSIX tls

   Revision 1.16  2005/02/11 18:55:12  haase
   Add some cleaning casts

   Revision 1.15  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
