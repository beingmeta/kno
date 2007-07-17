/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_DTYPEIO 1

#include "fdb/dtype.h"
#include "fdb/dtypestream.h"
#include "fdb/apply.h"
#include "fdb/fddb.h"
#include "fdb/dtproc.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#include <libu8/u8printf.h>

static fd_exception ServerUndefined=_("Server unconfigured");
static fdtype quote_symbol;

FD_EXPORT fdtype fd_make_dtproc(u8_string name,u8_string server,int ndcall,int arity,int min_arity)
{
  struct FD_DTPROC *f=u8_alloc(struct FD_DTPROC);
  FD_INIT_CONS(f,fd_dtproc_type);
  f->name=u8_mkstring("%s/%s",name,server); f->filename=u8_strdup(server);
  f->server=u8_strdup(server); f->fcnsym=fd_intern(name);
  f->ndprim=ndcall; f->min_arity=min_arity; f->arity=arity; f->xprim=1;
  f->typeinfo=NULL; f->defaults=NULL;
  f->handler.fnptr=NULL;
#if FD_THREADS_ENABLED
  fd_init_mutex(&(f->lock));
#endif
  f->stream.fd=-1;
  return FDTYPE_CONS(f);
}

static int unparse_dtproc(u8_output out,fdtype x)
{
  struct FD_DTPROC *f=FD_GET_CONS(x,fd_dtproc_type,fd_dtproc);
  u8_printf(out,"#<!DTPROC %s using %s>",f->name,f->server);
  return 1;
}

static void recycle_dtproc(FD_CONS *c)
{
  struct FD_DTPROC *f=(fd_dtproc)c;
  fd_dtsclose(&(f->stream),1);
  u8_free(f->name); u8_free(f->filename); u8_free(f->server);
  if (f->typeinfo) u8_free(f->typeinfo);
  if (f->defaults) u8_free(f->defaults);
  fd_destroy_mutex(&(f->lock));
  u8_free(f);
}

static int open_server(fd_dtproc dtp)
{
  u8_string server_spec; u8_connection sock;
  if (strchr(dtp->server,'@'))
    server_spec=u8_strdup(dtp->server);
  else {
    fdtype serverid=fd_config_get(dtp->server);
    if (FD_STRINGP(serverid))
      server_spec=u8_strdup(FD_STRDATA(serverid));
    else {
      fd_seterr(ServerUndefined,"open_server",u8_strdup(dtp->server),serverid);
      return -1;}
    fd_decref(serverid);}
  sock=u8_connect(server_spec);
  u8_free(server_spec);
  if (sock>=0)
    fd_init_dtype_stream(&(dtp->stream),sock,FD_NET_BUFSIZE);
  else return -1;
  return 1;
}

static int server_reconnect(fd_dtproc dtp)
{
  u8_connection newsock;
  /* This reopens the socket for a closed network pool. */
  if (dtp->stream.fd>=0) fd_dtsclose(&(dtp->stream),1);
  if (strchr(dtp->server,'@')) {
    u8_warn(fd_ServerReconnect,"Resetting connection to %s for %q",dtp->server,dtp->fcnsym);
    newsock=u8_connect(dtp->server);}
  else {
    fdtype serverid=fd_config_get(dtp->server);
    if (FD_STRINGP(serverid)) {
      u8_warn(fd_ServerReconnect,"Resetting connection to %s (%s) for %q",
	      dtp->server,serverid,dtp->fcnsym);
      newsock=u8_connect(dtp->server);}
    else {
      fd_seterr(ServerUndefined,"open_server",u8_strdup(dtp->server),serverid);
      return -1;}
    fd_decref(serverid);}
  if (newsock<0) return -1;
  else if (u8_set_nodelay(newsock,1)<0) return -1;
  fd_init_dtype_stream(&(dtp->stream),newsock,FD_NET_BUFSIZE);
  return 1;
}

static fdtype dtapply(struct FD_DTPROC *dtp,int n,fdtype *args)
{
  fdtype expr=FD_EMPTY_LIST, result; int i=n-1;
  fd_lock_struct(dtp);
  if (dtp->stream.fd<0)
    if (open_server(dtp)<0)
      return fd_erreify();
  while (i>=0) {
    if ((FD_SYMBOLP(args[i])) || (FD_PAIRP(args[i]))) 
      expr=fd_init_pair(NULL,fd_make_list(2,quote_symbol,fd_incref(args[i])),
			expr);
    else expr=fd_init_pair(NULL,fd_incref(args[i]),expr);
    i--;}
  expr=fd_init_pair(NULL,dtp->fcnsym,expr);
  if ((fd_dtswrite_dtype(&(dtp->stream),expr)<0) ||
      (fd_dtsflush(&(dtp->stream))<0)) {
    if (server_reconnect(dtp)<0) return fd_erreify();}
  result=fd_dtsread_dtype(&(dtp->stream));
  if (FD_EQ(result,FD_EOD)) {
    if ((server_reconnect(dtp)<0) ||
	(fd_dtswrite_dtype(&(dtp->stream),expr)<0) ||
	(fd_dtsflush(&(dtp->stream))<0))
      return fd_erreify();
    else result=fd_dtsread_dtype(&(dtp->stream));
    if (FD_EQ(result,FD_EOD))
      return fd_err(fd_UnexpectedEOD,"",dtp->server,expr);}
  if (FD_ABORTP(result)) {
    struct FD_EXCEPTION_OBJECT *exo=(struct FD_EXCEPTION_OBJECT *)result;
    if (exo->data.cxt==NULL) exo->data.cxt=dtp->server;}
  fd_unlock_struct(dtp);
  return result;
}

FD_EXPORT void fd_init_dtproc_c()
{
  quote_symbol=fd_intern("QUOTE");
  
  fd_register_source_file(versionid);
  fd_register_source_file(FDB_DTPROC_H_VERSION);

  fd_applyfns[fd_dtproc_type]=(fd_applyfn)dtapply;
  fd_unparsers[fd_dtproc_type]=unparse_dtproc;
  fd_recyclers[fd_dtproc_type]=recycle_dtproc;
}
