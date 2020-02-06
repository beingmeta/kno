/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
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

#ifndef KNO_USE_XTBLOCK
#define KNO_USE_XTBLOCK 0
#endif

static int log_eval_request = 0, log_eval_response = 0;
static int default_async = KNO_DEFAULT_ASYNC;

static lispval xteval_sock(u8_socket conn,lispval expr)
{
  lispval response; int retval;
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
  retval = kno_write_xtype(out,expr);
  if ((retval<0) || (kno_flush_stream(stream)<0)) {
    kno_close_stream(stream,KNO_STREAM_FREEDATA|KNO_STREAM_NOCLOSE);
    return KNO_ERROR;}
  else response = kno_read_xtype(kno_readbuf(stream));
  if (log_eval_response)
    u8_logf(LOG_DEBUG,"XTEVAL","On #%d: REQUEST %q\n\t==>\t%q",
            conn,response);
  kno_close_stream(stream,KNO_STREAM_FREEDATA|KNO_STREAM_NOCLOSE);
  return response;
}
static lispval xteval_connpool(struct U8_CONNPOOL *cpool,lispval expr,int async)
{
  lispval result; int retval;
  struct KNO_STREAM stream;
  u8_socket conn = u8_get_connection(cpool);
  if (conn<0) return KNO_ERROR;
  if (log_eval_request)
    u8_logf(LOG_DEBUG,"XTEVAL","On %s%s#%d: %q",
            (((async)&&(kno_use_xtblock))?(" (async/xtblock) "):
             (async)?(" (async) "):("")),
            cpool->u8cp_id,conn,expr);
  memset(&stream,0,sizeof(stream));
  kno_init_stream(&stream,cpool->u8cp_id,conn,
                 KNO_STREAM_SOCKET,
                 kno_network_bufsize);
  if ((async)&&(kno_use_xtblock)) { /*  */
    size_t xtype_len;
    kno_outbuf out = kno_writebuf(&stream);
    retval = kno_write_byte(out,xt_block);
    if (retval>0) retval = kno_write_4bytes(out,0);
    if (retval>0) retval = kno_write_xtype(out,expr);
    xtype_len = (out->bufwrite-out->buffer)-5;
    out->bufwrite = out->buffer+1;
    kno_write_4bytes(out,xtype_len);
    out->bufwrite = out->buffer+(xtype_len+5);}
  else {
    kno_outbuf out = kno_writebuf(&stream);
    stream.stream_flags |= KNO_STREAM_DOSYNC;
    retval = kno_write_xtype(out,expr);}
  if ((retval<0)||(kno_flush_stream(&stream)<0)) {
    u8_logf(LOG_ERR,"XTEVAL","Error with request to %s%s#%d for: %q",
            (((async)&&(kno_use_xtblock))?(" (async/xtblock) "):
             (async)?(" (async) "):("")),
            cpool->u8cp_id,conn,expr);
    kno_clear_errors(1);
    u8_logf(LOG_ERR,"XTEVAL","Reconnecting %s to %s for %q",
            (((async)&&(kno_use_xtblock))?(" (async/xtblock) "):
             (async)?(" (async) "):("")),
            cpool->u8cp_id,expr);
    if ((conn = u8_reconnect(cpool,conn))<0) {
      u8_logf(LOG_ERR,"XTEVAL","Reconnection %s failed to %s for %q",
              (((async)&&(kno_use_xtblock))?(" (async/xtblock) "):
               (async)?(" (async) "):("")),
              cpool->u8cp_id,expr);
      u8_discard_connection(cpool,conn);
      return KNO_ERROR;}
    else {
      u8_logf(LOG_ERR,"XTEVAL","Reconnected %s to %s#%d for %q",
              (((async)&&(kno_use_xtblock))?(" (async/xtblock) "):
               (async)?(" (async) "):("")),
              cpool->u8cp_id,conn,expr);}
    kno_flush_stream(&stream);}
  result = kno_read_xtype(kno_readbuf(&stream));
  if (KNO_EQ(result,KNO_EOD)) {
    kno_clear_errors(1);
    if (((conn = u8_reconnect(cpool,conn))<0) ||
        (kno_write_xtype(kno_writebuf(&stream),expr)<0) ||
        (kno_flush_stream(&stream)<0)) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return KNO_ERROR;}
    else result = kno_read_xtype(kno_readbuf(&stream));
    if (KNO_EQ(result,KNO_EOD)) {
      u8_discard_connection(cpool,conn);
      return kno_err(kno_UnexpectedEOD,"",NULL,expr);}}
  if (log_eval_response) {
    if (CONSP(result))
      u8_logf(LOG_DEBUG,"XTEVAL","On %s%s#%d ==> %hq",
              (((async)&&(kno_use_xtblock))?(" (async/xtblock) "):
               (async)?(" (async) "):("")),
              cpool->u8cp_id,conn,KNO_TYPEOF(result));
    else u8_logf(LOG_DEBUG,"XTEVAL","On %s%s#%d ==> %q",
                 (((async)&&(kno_use_xtblock))?(" (async/xtblock) "):
                  (async)?(" (async) "):("")),
                 cpool->u8cp_id,conn,result);}
  kno_close_stream(&stream,KNO_STREAM_FREEDATA);
  u8_return_connection(cpool,conn);
  return result;
}
KNO_EXPORT lispval kno_xteval(struct U8_CONNPOOL *cp,lispval expr)
{
  return xteval_connpool(cp,expr,default_async);
}
KNO_EXPORT lispval kno_xteval_sync(struct U8_CONNPOOL *cp,lispval expr)
{
  return xteval_connpool(cp,expr,0);
}
KNO_EXPORT lispval kno_xteval_async(struct U8_CONNPOOL *cp,lispval expr)
{
  return xteval_connpool(cp,expr,1);
}
KNO_EXPORT lispval kno_sock_xteval(u8_socket sock,lispval expr)
{
  return xteval_sock(sock,expr);
}

static lispval quote_symbol;

KNO_FASTOP lispval quote_lisp(lispval x,int dorefs,int doeval)
{
  if (dorefs) kno_incref(x);
  if (doeval) return x;
  else if ((SYMBOLP(x)) || (PAIRP(x)))
    return kno_conspair(quote_symbol,kno_conspair(x,NIL));
  else return x;
}
static lispval xtapply(struct U8_CONNPOOL *cp,int n,int dorefs,int doeval,
                       lispval *args)
{
  lispval request = NIL, result = VOID;
  n--; while (n>0) {
    request = kno_conspair(quote_lisp(args[n],dorefs,((doeval)&(1<<n))),
                          request);
    n--;}
  if (dorefs) kno_incref(args[0]);
  request = kno_conspair(args[0],request);
  result = xteval_connpool(cp,request,default_async);
  kno_decref(request);
  return result;
}
KNO_EXPORT lispval kno_xtapply(struct U8_CONNPOOL *cp,int n,lispval *args)
{
  return xtapply(cp,n,1,0,args);
}

KNO_EXPORT lispval kno_xtcall(struct U8_CONNPOOL *cp,int n,...)
{
  int i = 0; va_list arglist;
  lispval *args, _args[8], result;
  va_start(arglist,n);
  if (n>8)
    args = u8_alloc_n(n,lispval);
  else args=_args;
  while (i<n) args[i++]=va_arg(arglist,lispval);
  result = xtapply(cp,n,1,0,args);
  if (args!=_args) u8_free(args);
  return result;
}

KNO_EXPORT lispval kno_xtcall_x(struct U8_CONNPOOL *cp,int doeval,int n,...)
{
  int i = 0; va_list arglist;
  lispval *args, _args[8], result;
  va_start(arglist,n);
  if (n>8)
    args = u8_alloc_n(n,lispval);
  else args=_args;
  while (i<n) args[i++]=va_arg(arglist,lispval);
  result = xtapply(cp,n,1,doeval,args);
  if (args!=_args) u8_free(args);
  return result;
}

KNO_EXPORT lispval kno_xtcall_nr(struct U8_CONNPOOL *cp,int n,...)
{
  int i = 0; va_list arglist;
  lispval *args, _args[8], result;
  va_start(arglist,n);
  if (n>8)
    args = u8_alloc_n(n,lispval);
  else args=_args;
  while (i<n) args[i++]=va_arg(arglist,lispval);
  result = xtapply(cp,n,0,0,args);
  if (args!=_args) u8_free(args);
  return result;
}

KNO_EXPORT lispval kno_xtcall_nrx(struct U8_CONNPOOL *cp,int doeval,int n,...)
{
  int i = 0; va_list arglist;
  lispval *args, _args[8], result;
  va_start(arglist,n);
  if (n>8)
    args = u8_alloc_n(n,lispval);
  else args=_args;
  while (i<n) args[i++]=va_arg(arglist,lispval);
  result = xtapply(cp,n,0,doeval,args);
  if (args!=_args) u8_free(args);
  return result;
}

KNO_EXPORT void kno_init_xtcall_c()
{
  quote_symbol = kno_intern("quote");
  kno_register_config("ASYNC",
                     _("Assume asynchronous XType eval servers"),
                     kno_boolconfig_get,kno_boolconfig_set,&default_async);
  u8_register_source_file(_FILEINFO);
}
