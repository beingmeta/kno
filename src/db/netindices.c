/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: netindices.c,v 1.34 2006/01/26 14:44:32 haase Exp $";

#define FD_INLINE_DTYPEIO 1

#include "fdb/dtype.h"
#include "fdb/dtypestream.h"
#include "fdb/indices.h"

#include <libu8/u8.h>
#include <libu8/netfns.h>

static struct FD_INDEX_HANDLER netindex_handler;

static fdtype boundp, quote;
static fdtype iserver_writable, iserver_fetchkeys, iserver_fetch, iserver_fetchsize, iserver_fetchn;
static fdtype iserver_add, iserver_drop, iserver_addn, iserver_changes, iserver_reset;

static fdtype ixserver_writable, ixserver_fetchkeys, ixserver_fetch, ixserver_fetchsize, ixserver_fetchn;
static fdtype ixserver_changes, ixserver_add, ixserver_drop, ixserver_addn, ixserver_reset;

static fdtype set_symbol, drop_symbol;

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
  int i=0; fdtype *params=u8_malloc(sizeof(fdtype)*n_elts), request;
  va_list args;
  if (stream->fd<0)
    if (reopen_network_index(ni)<0) return FD_VOID;
  va_start(args,n_elts);
  while (i<n_elts) params[i++]=va_arg(args,fdtype);
  request=FD_EMPTY_LIST; i=n_elts-1;
  while (i>=0) {
    request=fd_init_pair(NULL,params[i],request);
    fd_incref(params[i]); i--;}
  fd_dtswrite_dtype(stream,request); fd_dtsflush(stream);
  fd_decref(request); u8_free(params);
  return fd_dtsread_dtype(stream); 
}

static void dtcallnr(struct FD_NETWORK_INDEX *ni,int n_elts,...)
{
  fd_dtype_stream stream=&(ni->stream);
  int i=0; fdtype *params=u8_malloc(sizeof(fdtype)*n_elts), request, result;
  va_list args;
  if (stream->fd<0)
    if (reopen_network_index(ni)<0) return;
  va_start(args,n_elts);
  while (i<n_elts) params[i++]=va_arg(args,fdtype);
  request=FD_EMPTY_LIST; i=n_elts-1;
  while (i>=0) {
    request=fd_init_pair(NULL,params[i],request);
    fd_incref(params[i]); i--;}
  fd_dtswrite_dtype(stream,request); fd_decref(request);
  u8_free(params);
  result=fd_dtsread_dtype(stream);
  fd_decref(result);
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
  struct FD_NETWORK_INDEX *ix=u8_malloc(sizeof(struct FD_NETWORK_INDEX));
  fdtype writable_response;
  fd_dtype_stream s=&(ix->stream);
  long sock=u8_connect(spec), n_pools=0; 
  if (sock>0) {
    fd_init_index(ix,&netindex_handler,spec); ix->xname=xname;
    fd_init_dtype_stream(s,sock,FD_NET_BUFSIZE,NULL,NULL);
    s->mallocd=0; s->bits=s->bits|FD_DTSTREAM_DOSYNC;
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

    u8_init_mutex(&(ix->lock));
    ix->sock=sock;
    if (ix) fd_register_index(ix);
    return (fd_index) ix;}
  else {
    u8_free(ix);
    return NULL;}
}

static int reopen_network_index(struct FD_NETWORK_INDEX *ix)
{
  if (ix->stream.fd>=0) return 0;
  else {
    long newsock=u8_connect(ix->source);
    if (newsock>=0) {
      fd_init_dtype_stream(&(ix->stream),newsock,FD_NET_BUFSIZE,NULL,NULL);
      return 1;}
    else return -1;}
}

static fdtype netindex_fetch(fd_index ix,fdtype key)
{
  struct FD_NETWORK_INDEX *nix=(struct FD_NETWORK_INDEX *)ix;
  fdtype result, qkey=quote_lisp(key);
  u8_lock_mutex(&(nix->lock));
  if (FD_VOIDP(nix->xname))
    result=dtcall(nix,2,iserver_fetch,qkey);
  else result=dtcall(nix,3,ixserver_fetch,nix->xname,qkey);
  u8_unlock_mutex(&(nix->lock));
  fd_decref(qkey);
  return result;
}

static int netindex_fetchsize(fd_index ix,fdtype key)
{
  struct FD_NETWORK_INDEX *nix=(struct FD_NETWORK_INDEX *)ix;
  fdtype result, qkey=quote_lisp(key);
  u8_lock_mutex(&(nix->lock));
  if (FD_VOIDP(nix->xname))
    result=dtcall(nix,2,iserver_fetchsize,qkey);
  else result=dtcall(nix,3,ixserver_fetchsize,nix->xname,qkey);
  u8_unlock_mutex(&(nix->lock));
  fd_decref(qkey);
  return fd_getint(result);
}

static fdtype *netindex_fetchn(fd_index ix,int n,fdtype *keys)
{
  struct FD_NETWORK_INDEX *nix=(struct FD_NETWORK_INDEX *)ix;
  fdtype vector, result, *results; struct FD_VECTOR *vp;
  u8_lock_mutex(&(nix->lock));
  vector=fd_init_vector(NULL,n,keys);
  if (FD_VOIDP(nix->xname))
    result=dtcall(nix,2,iserver_fetchn,vector);
  else result=dtcall(nix,3,ixserver_fetchn,nix->xname,vector);
  if (FD_VECTORP(result)) {
    vp=FD_XVECTOR(result); results=vp->data;  u8_free(vp);
    vp=FD_XVECTOR(vector); u8_free(vp);
    u8_unlock_mutex(&(nix->lock));
    return results;}
  else {
    u8_unlock_mutex(&(nix->lock));
    fd_seterr(fd_BadServerResponse,"netindex_fetchn",
	      u8_strdup(ix->cid),fd_incref(result));
    return NULL;}
}

static fdtype netindex_fetchkeys(fd_index ix)
{
  struct FD_NETWORK_INDEX *nix=(struct FD_NETWORK_INDEX *)ix;
  fdtype result;
  u8_lock_mutex(&(nix->lock));
  if (FD_VOIDP(nix->xname))
    result=dtcall(nix,1,iserver_fetchkeys);
  else result=dtcall(nix,2,ixserver_fetchkeys,nix->xname);
  u8_unlock_mutex(&(nix->lock));
  return result;
}

static int netindex_commit(fd_index ix)
{
  struct FD_NETWORK_INDEX *nix=(struct FD_NETWORK_INDEX *)ix;
  struct FD_DTYPE_STREAM *stream=&(nix->stream); 
  int n_transactions=0;
  u8_lock_mutex(&(nix->lock));
  u8_lock_mutex(&(nix->adds.lock));
  u8_lock_mutex(&(nix->edits.lock));
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
	else u8_warn("Server %s doesn't support resets",ix->source);
      else if ((FD_PAIRP(key)) && (FD_EQ(FD_CAR(key),drop_symbol))) 
	if (nix->capabilities&FD_ISERVER_DROP) {
	  n_transactions++;
	  if (FD_VOIDP(nix->xname))
	    dtcallnr(nix,3,iserver_drop,
		     fd_incref(FD_CDR(key)),fd_incref(scan->value));
	  else dtcallnr(nix,4,ixserver_drop,nix->xname,
			fd_incref(FD_CDR(key)),fd_incref(scan->value));}
	else u8_warn("Server %s doesn't support drops",ix->source);
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
    u8_unlock_mutex(&(nix->lock));
    u8_unlock_mutex(&(nix->adds.lock));
    u8_unlock_mutex(&(nix->edits.lock));
    return n_transactions;}
}

static void netindex_close(fd_index ix)
{
  struct FD_NETWORK_INDEX *nix=(struct FD_NETWORK_INDEX *)ix;
  u8_lock_mutex(&(nix->lock));
  fd_dtsclose(&(nix->stream),1);
  u8_unlock_mutex(&(nix->lock));
}

static void netindex_setbuf(fd_index ix,int bufsiz)
{
  struct FD_NETWORK_INDEX *ni=(struct FD_NETWORK_INDEX *)ix;
  u8_lock_mutex(&(ni->lock));
  fd_dtsbufsize(&(ni->stream),bufsiz);
  u8_unlock_mutex(&(ni->lock));
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

FD_EXPORT fd_init_netindices_c()
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


/* The CVS log for this file
   $Log: netindices.c,v $
   Revision 1.34  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.33  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.32  2005/10/13 16:01:02  haase
   Made indices which return errors for iserver-writable? be read-only

   Revision 1.31  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.30  2005/06/04 12:44:09  haase
   Fixed error catching for prefetches

   Revision 1.29  2005/05/26 12:41:11  haase
   More return value error handling fixes

   Revision 1.28  2005/05/23 00:53:24  haase
   Fixes to header ordering to get off_t consistently defined

   Revision 1.27  2005/05/21 18:00:19  haase
   Enlarged network buffer sizes and regularized in FD_NET_BUFSIZE

   Revision 1.26  2005/05/21 17:51:23  haase
   Added DTPROCs (remote procedures)

   Revision 1.25  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.24  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.23  2005/04/14 00:33:43  haase
   Made newtork streams be synchronized

   Revision 1.22  2005/04/04 22:20:36  haase
   Improved integration of error facilities

   Revision 1.21  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.20  2005/03/28 19:19:36  haase
   Added metadata reading and writing and file pool/index creation

   Revision 1.19  2005/03/26 04:47:21  haase
   Added fd_index_sizes

   Revision 1.18  2005/03/25 19:49:47  haase
   Removed base library for eframerd, deferring to libu8

   Revision 1.17  2005/02/19 16:25:02  haase
   Replaced fd_parse with fd_intern

   Revision 1.16  2005/02/11 18:55:12  haase
   Add some cleaning casts

   Revision 1.15  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
