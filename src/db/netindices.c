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

static struct FD_INDEX_HANDLER netindex_handler;

static fdtype boundp, quote;
static fdtype iserver_writable, iserver_fetchkeys, iserver_fetch, iserver_fetchsize, iserver_fetchn;
static fdtype iserver_add, iserver_drop, iserver_addn, iserver_changes, iserver_reset;

static fdtype ixserver_writable, ixserver_fetchkeys, ixserver_fetch, ixserver_fetchsize, ixserver_fetchn;
static fdtype ixserver_changes, ixserver_add, ixserver_drop, ixserver_addn, ixserver_reset;

static fdtype set_symbol, drop_symbol;

fd_exception fd_NoServerMethod=_("Server doesn't support method");

static int reopen_network_index(struct FD_NETWORK_INDEX *ix);

FD_FASTOP fdtype quote_lisp(fdtype x)
{
  if ((FD_SYMBOLP(x)) || (FD_PAIRP(x)))
    return fd_init_pair
      (NULL,quote,fd_init_pair(NULL,fd_incref(x),FD_EMPTY_LIST));
  else return fd_incref(x);
}

static fdtype dteval(struct FD_NETWORK_INDEX *ni,fdtype expr)
{
  fd_dtype_stream stream=&(ni->stream);
  if (stream->fd<0)
    if (reopen_network_index(ni)<0) return FD_VOID;
  fd_dtswrite_dtype(stream,expr);
  return fd_dtsread_dtype(stream); 
}

static fdtype dtcall(struct FD_NETWORK_INDEX *ni,int n_elts,...)
{
  fd_dtype_stream stream=&(ni->stream);
  int i=0; fdtype *params=u8_alloc_n(n_elts,fdtype), request, result;
  va_list args;
  if (stream->fd<0)
    if (reopen_network_index(ni)<0) return FD_ERROR_VALUE;
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
    u8_log(LOG_WARN,fd_ServerReconnect,"Resetting connection to %s",ni->xid);
    fd_dtsclose(stream,1); sleep(1);
    if ((reopen_network_index(ni)<0) ||
	(fd_dtswrite_dtype(stream,request)<0)) {
      fd_decref(request);
      return FD_ERROR_VALUE;}}
  result=fd_dtsread_dtype(stream);
  if (FD_EQ(result,FD_EOD)) {
    /* Close the stream and sleep a second before reconnecting. */
    u8_log(LOG_WARN,fd_ServerReconnect,"Resetting connection to %s",ni->xid);
    fd_dtsclose(stream,1); sleep(1);
    if ((reopen_network_index(ni)<0) ||
	(fd_dtswrite_dtype(stream,request)<0)) {
      fd_decref(request);
      return FD_ERROR_VALUE;}
    else result==fd_dtsread_dtype(stream);
    if (FD_EQ(result,FD_EOD))
      return fd_err(fd_UnexpectedEOD,"dtcall/indices.c",ni->xid,FD_VOID);}
  fd_decref(request);
  return result;
}

static fdtype dtcallnr(struct FD_NETWORK_INDEX *ni,int n_elts,...)
{
  fd_dtype_stream stream=&(ni->stream);
  int i=0; fdtype *params=u8_alloc_n(n_elts,fdtype), request, result;
  va_list args;
  if (stream->fd<0)
    if (reopen_network_index(ni)<0) return FD_ERROR_VALUE;
  va_start(args,n_elts);
  while (i<n_elts) params[i++]=va_arg(args,fdtype);
  request=FD_EMPTY_LIST; i=n_elts-1;
  while (i>=0) {
    request=fd_init_pair(NULL,params[i],request); i--;}
  u8_free(params);
  if ((fd_dtswrite_dtype(stream,request)<0) ||
      (fd_dtsflush(stream)<0)) {
    /* Close the stream and sleep a second before reconnecting. */
    fd_dtsclose(stream,1); sleep(1);
    if ((reopen_network_index(ni)<0) ||
	(fd_dtswrite_dtype(stream,request)<0)) {
      fd_decref(request);
      return FD_ERROR_VALUE;}}
  result=fd_dtsread_dtype(stream);
  if (FD_EQ(result,FD_EOD)) {
    /* Close the stream and sleep a second before reconnecting. */
    fd_dtsclose(stream,1); sleep(1);
    if ((reopen_network_index(ni)<0) ||
	(fd_dtswrite_dtype(stream,request)<0)) {
      fd_decref(request);
      return FD_ERROR_VALUE;}
    else result==fd_dtsread_dtype(stream);
    if (FD_EQ(result,FD_EOD))
      return fd_err(fd_UnexpectedEOD,"dtcall/indices.c",ni->xid,FD_VOID);}
  fd_decref(request);
  return result;
}

static int server_supportsp(struct FD_NETWORK_INDEX *ni,fdtype operation)
{
  fd_dtype_stream stream=&(ni->stream);
  fdtype request=
    fd_init_pair(NULL,boundp,fd_init_pair(NULL,operation,FD_EMPTY_LIST));
  fdtype response;
  if (stream->fd<0)
    if (reopen_network_index(ni)<0) return FD_VOID;
  fd_dtswrite_dtype(stream,request); fd_decref(request);
  response=fd_dtsread_dtype(stream);
  if (FD_FALSEP(response)) return 0;
  else {fd_decref(response); return 1;}
}

FD_EXPORT fd_index fd_open_network_index(u8_string spec,fdtype xname)
{
  struct FD_NETWORK_INDEX *ix=u8_alloc(struct FD_NETWORK_INDEX);
  fdtype writable_response; u8_string xid=NULL;
  fd_dtype_stream s=&(ix->stream);
  u8_connection sock=u8_connect_x(spec,&xid);
  int n_pools=0;
  if (sock<0) {
    u8_free(ix);
    return NULL;}
  else if (u8_set_nodelay(sock,1)<0) {
    u8_free(ix);
    return NULL;}
  else {
    fd_init_index((fd_index)ix,&netindex_handler,spec);
    ix->xname=xname; ix->xid=xid;
    fd_init_dtype_stream(s,sock,FD_NET_BUFSIZE);
    s->mallocd=0; s->flags=s->flags|FD_DTSTREAM_DOSYNC;
    if (FD_VOIDP(xname))
      writable_response=dtcall(ix,1,iserver_writable);
    else writable_response=dtcall(ix,2,ixserver_writable,xname);
    if (FD_ABORTP(writable_response)) ix->read_only=1;
    else if (!(FD_FALSEP(writable_response))) ix->read_only=0;
    fd_decref(writable_response);

    ix->capabilities=0;
    if (server_supportsp(ix,iserver_fetchn)) ix->capabilities|=FD_ISERVER_FETCHN;
    if (server_supportsp(ix,iserver_addn)) ix->capabilities|=FD_ISERVER_ADDN;
    if (server_supportsp(ix,iserver_drop)) ix->capabilities|=FD_ISERVER_DROP;
    if (server_supportsp(ix,iserver_reset)) ix->capabilities|=FD_ISERVER_RESET;

    fd_init_mutex(&(ix->lock));
    ix->sock=sock;
    if (ix) fd_register_index((fd_index)ix);
    return (fd_index) ix;}
}

static int reopen_network_index(struct FD_NETWORK_INDEX *ix)
{
  if (ix->stream.fd>=0) return 0;
  else {
    u8_string xid=NULL;
    u8_connection newsock=u8_connect_x(ix->source,&xid);
    if (newsock<0) {
      return newsock;}
    if (u8_set_nodelay(newsock,1)<0) return -1;
    if (ix->xid) u8_free(ix->xid); ix->xid=NULL;
    if (newsock>=0) {
      ix->xid=xid;
      fd_init_dtype_stream(&(ix->stream),newsock,FD_NET_BUFSIZE);
      return 1;}
    else return -1;}
}

static fdtype netindex_fetch(fd_index ix,fdtype key)
{
  struct FD_NETWORK_INDEX *nix=(struct FD_NETWORK_INDEX *)ix;
  fdtype result, qkey=quote_lisp(key);
  fd_lock_struct(nix);
  if (FD_VOIDP(nix->xname))
    result=dtcall(nix,2,iserver_fetch,qkey);
  else result=dtcall(nix,3,ixserver_fetch,nix->xname,qkey);
  fd_unlock_struct(nix);
  fd_decref(qkey);
  return result;
}

static int netindex_fetchsize(fd_index ix,fdtype key)
{
  struct FD_NETWORK_INDEX *nix=(struct FD_NETWORK_INDEX *)ix;
  fdtype result, qkey=quote_lisp(key);
  fd_lock_struct(nix);
  if (FD_VOIDP(nix->xname))
    result=dtcall(nix,2,iserver_fetchsize,qkey);
  else result=dtcall(nix,3,ixserver_fetchsize,nix->xname,qkey);
  fd_unlock_struct(nix);
  fd_decref(qkey);
  return fd_getint(result);
}

static fdtype *netindex_fetchn(fd_index ix,int n,fdtype *keys)
{
  struct FD_NETWORK_INDEX *nix=(struct FD_NETWORK_INDEX *)ix;
  fdtype vector, result, *results; struct FD_VECTOR *vp;
  fd_lock_struct(nix);
  vector=fd_init_vector(NULL,n,keys);
  if (FD_VOIDP(nix->xname))
    result=dtcall(nix,2,iserver_fetchn,vector);
  else result=dtcall(nix,3,ixserver_fetchn,nix->xname,vector);
  if (FD_VECTORP(result)) {
    vp=FD_XVECTOR(result); results=vp->data;  u8_free(vp);
    vp=FD_XVECTOR(vector); u8_free(vp);
    fd_unlock_struct(nix);
    return results;}
  else {
    fd_unlock_struct(nix);
    fd_seterr(fd_BadServerResponse,"netindex_fetchn",
	      u8_strdup(ix->cid),fd_incref(result));
    return NULL;}
}

static fdtype *netindex_fetchkeys(fd_index ix,int *n)
{
  struct FD_NETWORK_INDEX *nix=(struct FD_NETWORK_INDEX *)ix;
  fdtype result;
  fd_lock_struct(nix);
  if (FD_VOIDP(nix->xname))
    result=dtcall(nix,1,iserver_fetchkeys);
  else result=dtcall(nix,2,ixserver_fetchkeys,nix->xname);
  if (FD_ABORTP(result)) {
    fd_unlock_struct(nix); *n=-1;
    fd_interr(result);
    return NULL;}
  fd_unlock_struct(nix);
  if (FD_CHOICEP(result)) {
    int size=FD_CHOICE_SIZE(result);
    fdtype *dtypes=u8_alloc_n(size,fdtype);
    memcpy(dtypes,FD_CHOICE_DATA(result),sizeof(fdtype)*size);
    *n=size;
    u8_free((fd_cons)result);
    return dtypes;}
  else {
    fdtype *dtypes=u8_alloc_n(1,fdtype); *n=1;
    dtypes[0]=result;
    return dtypes;}
}

static int netindex_commit(fd_index ix)
{
  struct FD_NETWORK_INDEX *nix=(struct FD_NETWORK_INDEX *)ix;
  struct FD_DTYPE_STREAM *stream=&(nix->stream); 
  int n_transactions=0;
  fd_lock_struct(nix);
  fd_lock_struct(&(nix->adds));
  fd_lock_struct(&(nix->edits));
  if (nix->edits.n_keys) {
    int n_edits;
    struct FD_KEYVAL *kvals=fd_hashtable_keyvals(&(nix->adds),&n_edits,0);
    struct FD_KEYVAL *scan=kvals, *limit=kvals+n_edits;
    while (scan<limit) {
      fdtype key=scan->key;
      if ((FD_PAIRP(key)) && (FD_EQ(FD_CAR(key),set_symbol))) 
	if (nix->capabilities&FD_ISERVER_RESET) {
	  n_transactions++;
	  if (FD_VOIDP(nix->xname))
	    dtcallnr(nix,3,iserver_reset,fd_incref(FD_CDR(key)),
		     fd_incref(scan->value));
	  else dtcallnr(nix,4,ixserver_reset,nix->xname,
			fd_incref(FD_CDR(key)),fd_incref(scan->value));}
	else u8_log(LOG_WARN,fd_NoServerMethod,"Server %s doesn't support resets",ix->source);
      else if ((FD_PAIRP(key)) && (FD_EQ(FD_CAR(key),drop_symbol))) 
	if (nix->capabilities&FD_ISERVER_DROP) {
	  n_transactions++;
	  if (FD_VOIDP(nix->xname))
	    dtcallnr(nix,3,iserver_drop,
		     fd_incref(FD_CDR(key)),fd_incref(scan->value));
	  else dtcallnr(nix,4,ixserver_drop,nix->xname,
			fd_incref(FD_CDR(key)),fd_incref(scan->value));}
	else u8_log(LOG_WARN,fd_NoServerMethod,"Server %s doesn't support drops",ix->source);
      else u8_raise(_("Bad edit key in index"),"fd_netindex_commit",NULL);
      scan++;}
    scan=kvals; while (scan<kvals) {
      fd_decref(scan->key); fd_decref(scan->value); scan++;}}
  if (nix->capabilities&FD_ISERVER_ADDN) {
    int n_adds;
    struct FD_KEYVAL *kvals=fd_hashtable_keyvals(&(nix->adds),&n_adds,0);
    fdtype vector=fd_init_vector(NULL,n_adds*2,(fdtype *)kvals);
    if (FD_VOIDP(nix->xname))
      dtcallnr(nix,2,iserver_addn,vector);
    else dtcallnr(nix,3,ixserver_addn,nix->xname,vector);
    return n_transactions+1;}
  else {
    int n_adds; fdtype xname=nix->xname;
    struct FD_KEYVAL *kvals=fd_hashtable_keyvals(&(nix->adds),&n_adds,0);
    struct FD_KEYVAL *scan=kvals, *limit=scan+n_adds;
    if (FD_VOIDP(xname))
      while (scan<limit) {
	n_transactions++;
	dtcallnr(nix,3,iserver_add,scan->key,scan->value); scan++;}
    else while (scan<limit) {
      n_transactions++;
      dtcallnr(nix,3,ixserver_add,xname,scan->key,scan->value); scan++;}
    scan=kvals; while (scan<kvals) {
      fd_decref(scan->key); fd_decref(scan->value); scan++;}
    u8_free(kvals);
    fd_reset_hashtable(&(nix->adds),67,0);
    fd_reset_hashtable(&(nix->edits),67,0);
    fd_unlock_struct(nix);
    fd_unlock_struct(&(nix->adds));
    fd_unlock_struct(&(nix->edits));
    return n_transactions;}
}

static void netindex_close(fd_index ix)
{
  struct FD_NETWORK_INDEX *nix=(struct FD_NETWORK_INDEX *)ix;
  fd_lock_struct(nix);
  fd_dtsclose(&(nix->stream),1);
  fd_unlock_struct(nix);
}

static void netindex_setbuf(fd_index ix,int bufsiz)
{
  struct FD_NETWORK_INDEX *ni=(struct FD_NETWORK_INDEX *)ix;
  fd_lock_struct(ni);
  fd_dtsbufsize(&(ni->stream),bufsiz);
  fd_unlock_struct(ni);
}

static struct FD_INDEX_HANDLER netindex_handler={
  "netindex", 1, sizeof(struct FD_NETWORK_INDEX), 12,
  netindex_close, /* close */
  netindex_commit, /* commit */
  NULL, /* setcache */
  netindex_setbuf, /* setbuf */
  netindex_fetch, /* fetch */
  netindex_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  netindex_fetchn, /* fetchn */
  netindex_fetchkeys, /* fetchkeys */
  NULL, /* fetchsizes */
  NULL, /* metadata */
  NULL /* sync */
};

FD_EXPORT void fd_init_netindices_c()
{
  fd_register_source_file(versionid);

  boundp=fd_intern("BOUND?");
  quote=fd_intern("QUOTE");
  set_symbol=fd_intern("SET");
  drop_symbol=fd_intern("DROP");

  iserver_writable=fd_intern("ISERVER-WRITABLE?");
  iserver_fetchkeys=fd_intern("ISERVER-KEYS");
  iserver_changes=fd_intern("ISERVER-CHANGES");
  iserver_fetch=fd_intern("ISERVER-GET");
  iserver_fetchsize=fd_intern("ISERVER-GET-SIZE");
  iserver_add=fd_intern("ISERVER-ADD!");
  iserver_drop=fd_intern("ISERVER-DROP!");
  iserver_fetchn=fd_intern("ISERVER-BULK-GET");
  iserver_addn=fd_intern("ISERVER-BULK-ADD!");
  iserver_reset=fd_intern("ISERVER-RESET!");

  ixserver_writable=fd_intern("IXSERVER-WRITABLE?");
  ixserver_fetchkeys=fd_intern("IXSERVER-KEYS");
  ixserver_changes=fd_intern("IXSERVER-CHANGES");
  ixserver_fetch=fd_intern("IXSERVER-GET");
  ixserver_fetchsize=fd_intern("IXSERVER-GET-SIZE");
  ixserver_add=fd_intern("IXSERVER-ADD!");
  ixserver_drop=fd_intern("IXSERVER-DROP!");
  ixserver_fetchn=fd_intern("IXSERVER-BULK-GET");
  ixserver_addn=fd_intern("IXSERVER-BULK-ADD!");
  iserver_reset=fd_intern("IXSERVER-RESET!");
}
