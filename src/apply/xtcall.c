/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/xtcall.h"
#include "kno/streams.h"

#include <libu8/u8netfns.h>

#include <stdarg.h>

static int log_eval_request = 0, log_eval_response = 0;
static int default_async = KNO_DEFAULT_ASYNC;

static lispval xteval_sock(u8_socket conn,lispval expr,xtype_refs refs)
{
  lispval response; int retval;
  if (conn<0) {
    u8_seterr("BadSocket","xteval_sock",NULL);
    return KNO_ERROR;}
  struct KNO_STREAM _stream, *stream;
  struct KNO_OUTBUF *out;
  memset(&_stream,0,sizeof(_stream));
  stream = kno_init_stream
    (&_stream,NULL,conn,
     KNO_STREAM_DOSYNC|KNO_STREAM_SOCKET|KNO_STREAM_OWNS_FILENO,
     kno_network_bufsize);
  out = kno_writebuf(stream);
  if (log_eval_request)
    u8_logf(LOG_DEBUG,"XTEVAL","On #%d: %q",conn,expr);
  retval = kno_write_xtype(out,expr,refs);
  if ((retval<0) || (kno_flush_stream(stream)<0)) {
    kno_close_stream(stream,KNO_STREAM_FREEDATA|KNO_STREAM_NOCLOSE);
    return KNO_ERROR;}
  else response = kno_read_xtype(kno_readbuf(stream),refs);
  if (log_eval_response)
    u8_logf(LOG_DEBUG,"XTEVAL","On #%d: REQUEST %q\n\t==>\t%q",
            conn,response);
  kno_close_stream(stream,KNO_STREAM_FREEDATA|KNO_STREAM_NOCLOSE);
  return response;
}
static lispval xteval_connpool(struct U8_CONNPOOL *cpool,
			       lispval expr,xtype_refs refs)
{
  lispval result; int retval;
  struct KNO_STREAM stream;
  u8_socket conn = u8_get_connection(cpool);
  if (conn<0) return KNO_ERROR;
  if (log_eval_request)
    u8_logf(LOG_DEBUG,"XTEVAL","On %s#%d: %q",
            cpool->u8cp_id,conn,expr);
  memset(&stream,0,sizeof(stream));
  kno_init_stream(&stream,cpool->u8cp_id,conn,
                 KNO_STREAM_SOCKET,
                 kno_network_bufsize);
  kno_outbuf out = kno_writebuf(&stream);
  stream.stream_flags |= KNO_STREAM_DOSYNC;
  retval = kno_write_xtype(out,expr,refs);
  if ((retval<0)||(kno_flush_stream(&stream)<0)) {
    u8_logf(LOG_ERR,"XTEVAL","Error with request to %s#%d for: %q",
            cpool->u8cp_id,conn,expr);
    kno_clear_errors(1);
    u8_logf(LOG_ERR,"XTEVAL","Reconnecting to %s for %q",cpool->u8cp_id,expr);
    if ((conn = u8_reconnect(cpool,conn))<0) {
      u8_logf(LOG_ERR,"XTEVAL","Reconnection failed to %s for %q",
              cpool->u8cp_id,expr);
      u8_discard_connection(cpool,conn);
      return KNO_ERROR;}
    else {
      u8_logf(LOG_ERR,"XTEVAL","Reconnected to %s#%d for %q",
              cpool->u8cp_id,conn,expr);}
    kno_flush_stream(&stream);}
  result = kno_read_xtype(kno_readbuf(&stream),refs);
  if (KNO_EQ(result,KNO_EOD)) {
    kno_clear_errors(1);
    if (((conn = u8_reconnect(cpool,conn))<0) ||
        (kno_write_xtype(kno_writebuf(&stream),expr,refs)<0) ||
        (kno_flush_stream(&stream)<0)) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return KNO_ERROR;}
    else result = kno_read_xtype(kno_readbuf(&stream),refs);
    if (KNO_EQ(result,KNO_EOD)) {
      u8_discard_connection(cpool,conn);
      return kno_err(kno_UnexpectedEOD,"",NULL,expr);}}
  if (log_eval_response) {
    if (CONSP(result))
      u8_logf(LOG_DEBUG,"XTEVAL","On %s#%d ==> %hq",
              cpool->u8cp_id,conn,KNO_TYPEOF(result));
    else u8_logf(LOG_DEBUG,"XTEVAL","On %s#%d ==> %q",
                 cpool->u8cp_id,conn,result);}
  kno_close_stream(&stream,KNO_STREAM_FREEDATA);
  u8_return_connection(cpool,conn);
  return result;
}
KNO_EXPORT lispval kno_xteval(struct U8_CONNPOOL *cp,lispval expr,xtype_refs refs)
{
  return xteval_connpool(cp,expr,refs);
}
KNO_EXPORT lispval kno_sock_xteval(u8_socket sock,lispval expr,xtype_refs refs)
{
  return xteval_sock(sock,expr,refs);
}

KNO_FASTOP lispval quote_lisp(lispval x,int flags)
{
  if ( (!(flags&XTCALL_FREE_ARGS)) && (KNO_CONSP(x)) ) kno_incref(x);
  if (flags&XTCALL_EVAL_ARGS)
    return x;
  else if ((SYMBOLP(x)) || (PAIRP(x)))
    return kno_conspair(KNOSYM_QUOTE,kno_conspair(x,NIL));
  else return kno_conspair(KNOSYM_QUOTE,kno_conspair(x,NIL));
}
KNO_EXPORT lispval kno_xtcall(struct U8_CONNPOOL *cp,xtype_refs refs,int flags,
			      int n,lispval *args)
{
  lispval request = NIL, result = VOID;
  n--; while (n>=0) {
    request = kno_conspair(quote_lisp(args[n],flags),
                          request);
    n--;}
  result = xteval_connpool(cp,request,refs);
  kno_decref(request);
  return result;
}

KNO_EXPORT lispval kno_xtcall_n(struct U8_CONNPOOL *cp,xtype_refs refs,int flags,
			      int n,...)
{
  int i = 0; va_list arglist;
  lispval args[n];
  va_start(arglist,n);
  while (i<n) args[i++]=va_arg(arglist,lispval);
  va_end(arglist);
  return kno_xtcall(cp,refs,n,flags,args);
}

KNO_EXPORT void kno_init_xtcall_c()
{
  kno_register_config("ASYNC",
                     _("Assume asynchronous XType eval servers"),
                     kno_boolconfig_get,kno_boolconfig_set,&default_async);
  u8_register_source_file(_FILEINFO);
}
