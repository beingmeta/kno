/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id:$";

#define FD_INLINE_DTYPEIO 1

#include "fdb/dtype.h"
#include "fdb/dtypestream.h"
#include "fdb/apply.h"
#include "fdb/fddb.h"
#include "fdb/dtcall.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#include <libu8/u8printf.h>

static fd_exception ServerUndefined=_("Server unconfigured");
static fdtype quote_symbol;
static int dtcall_init_done=0;


fd_ptr_type fd_dtserver_type;

FD_EXPORT fdtype fd_open_dtserver(u8_string server,int bufsiz)
{
  struct FD_DTSERVER *dts=u8_alloc(struct FD_DTSERVER);
  u8_string server_addr; int socket;
  if ((*server)==':') {
    fdtype server_id=fd_config_get(server+1);
    if (FD_STRINGP(server_id))
      server_addr=u8_strdup(FD_STRDATA(server_id));
    else  {
      fd_seterr(ServerUndefined,"open_server",u8_strdup(dts->server),server_id);
      u8_free(dts);
      return -1;}}
  else server_addr=u8_strdup(server);
  dts->server=u8_strdup(server); dts->addr=server_addr;
  socket=u8_connect_x(server,&(dts->addr));
  if (socket<0) {
    u8_free(dts->server); u8_free(dts->addr); u8_free(dts); 
    return socket;}
  fd_init_dtype_stream(&(dts->stream),socket,
		       ((bufsiz<0) ? (FD_NET_BUFSIZE) : (bufsiz)));
  fd_init_mutex(&(dts->lock));
  FD_INIT_CONS(dts,fd_dtserver_type);
  return FDTYPE_CONS(dts);
}

static int server_reconnect(fd_dtserver dts)
{
  u8_connection newsock; u8_string server, server_addr; int bufsiz=-1;
  /* This reopens the socket for a closed network pool. */
  if (dts->stream.fd>=0) {
    fd_dtsclose(&(dts->stream),1);
    bufsiz=dts->stream.bufsiz;}
  server=dts->server; server_addr=dts->addr;
  if (dts->addr) {u8_free(dts->addr); dts->addr=NULL;}
  if ((*server)==':') {
    fdtype server_id=fd_config_get(server+1);
    if (FD_STRINGP(server_id))
      server_addr=u8_strdup(FD_STRDATA(server_id));
    else  {
      fd_decref(server_id);
      fd_seterr(ServerUndefined,"server_reconnect",
		u8_strdup(dts->server),server_id);
      return -1;}}
  else server_addr=u8_strdup(server);
  u8_log(LOG_WARN,fd_ServerReconnect,"Resetting connection to %s",dts->server);
  newsock=u8_connect_x(server,&(dts->addr));
  if (newsock<0) {
    u8_log(LOG_WARN,fd_ServerReconnect,"Failed to reconnect to %s",dts->server);
    u8_free(server_addr); return newsock;}
  else if (u8_set_nodelay(newsock,1)<0) {
    u8_log(LOG_WARN,fd_ServerReconnect,"Failed to set nodelay on socket to %s",
	    dts->server);
    u8_free(server_addr); return -1;}
  fd_init_dtype_stream(&(dts->stream),newsock,
		       ((bufsiz<0) ? (FD_NET_BUFSIZE) : (bufsiz)));
  return newsock;
}

FD_EXPORT fdtype fd_dteval(fd_dtserver dts,fdtype expr)
{
  if ((fd_dtswrite_dtype(&(dts->stream),expr)<0) ||
      (fd_dtsflush(&(dts->stream))<0)) {
    if (server_reconnect(dts)<0) return FD_ERROR_VALUE;
    else if ((fd_dtswrite_dtype(&(dts->stream),expr)<0) ||
	     (fd_dtsflush(&(dts->stream))<0))
      return FD_ERROR_VALUE;}
  return fd_dtsread_dtype(&(dts->stream));
}

FD_EXPORT fdtype fd_dtcall(fd_dtserver dts,u8_string fcn,int n,...)
{
  fd_dtype_stream stream=&(dts->stream);
  fdtype *params=u8_alloc_n((n+1),fdtype), request, result;
  int i=0; va_list args;
  fd_lock_struct(dts);
  if (stream->fd<0)
    if (server_reconnect(dts)<0) {
      fd_unlock_struct(dts);
      return FD_ERROR_VALUE;}
    else {}
  params[i++]=fd_intern(fcn);
  va_start(args,n);
  while (i<n) params[i++]=va_arg(args,fdtype);
  request=FD_EMPTY_LIST; i=n-1;
  while (i>=0) {
    if ((i>0) && ((FD_SYMBOLP(params[i])) || (FD_PAIRP(params[i]))))
      request=fd_init_pair(NULL,fd_make_list(2,quote_symbol,params[i]),request);
    else request=fd_init_pair(NULL,params[i],request);
    fd_incref(params[i]); i--;}
  u8_free(params);
  if ((fd_dtswrite_dtype(stream,request)<0) ||
      (fd_dtsflush(stream)<0)) {
    /* Close the stream and sleep a second before reconnecting. */
    u8_log(LOG_WARN,fd_ServerReconnect,"Resetting connection to %s",dts->server);
    fd_dtsclose(stream,1); sleep(1);
    if ((server_reconnect(dts)<0) ||
	(fd_dtswrite_dtype(stream,request)<0) ||
	(fd_dtsflush(&(dts->stream))<0)) {
      fd_decref(request);
      return FD_ERROR_VALUE;}}
  result=fd_dtsread_dtype(stream);
  if (FD_EQ(result,FD_EOD)) {
    /* Close the stream and sleep a second before reconnecting. */
    u8_log(LOG_WARN,fd_ServerReconnect,"Resetting connection to %s/",dts->server,dts->addr);
    fd_dtsclose(stream,1); sleep(1);
    if ((server_reconnect(dts)<0) ||
	(fd_dtswrite_dtype(stream,request)<0)  ||
	(fd_dtsflush(&(dts->stream))<0)) {
      fd_decref(request);
      return FD_ERROR_VALUE;}
    else result==fd_dtsread_dtype(stream);
    if (FD_EQ(result,FD_EOD))
      return fd_err(fd_UnexpectedEOD,"fd_dtcall",dts->addr,FD_VOID);}
  fd_decref(request);
  return result;
}

static int unparse_dtserver(u8_output out,fdtype x)
{
  struct FD_DTSERVER *dts=
    FD_GET_CONS(x,fd_dtserver_type,struct FD_DTSERVER *);
  u8_printf(out,"#<DTSERVER \"%s\" %s>",dts->server,dts->addr);
  return 1;
}

FD_EXPORT void recycle_dtserver(struct FD_CONS *c)
{
  struct FD_DTSERVER *dts=(struct FD_DTSERVER *)c;
  fd_dtsclose(&(dts->stream),1);
  u8_free(dts);
}

FD_EXPORT void fd_init_dtcall_c()
{
  if (dtcall_init_done) return;
  dtcall_init_done=0;
  fd_dtserver_type=fd_register_cons_type(_("thread"));
  fd_recyclers[fd_dtserver_type]=recycle_dtserver;
  fd_unparsers[fd_dtserver_type]=unparse_dtserver;

  quote_symbol=fd_intern("QUOTE");
  
  fd_register_source_file(versionid);
  fd_register_source_file(FDB_DTCALL_H_VERSION);

  
}
