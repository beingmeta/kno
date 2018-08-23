/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dtcall.h"
#include "framerd/streams.h"

#include <libu8/u8netfns.h>

#include <stdarg.h>

#ifndef FD_USE_DTBLOCK
#define FD_USE_DTBLOCK 0
#endif

static int log_eval_request = 0, log_eval_response = 0;
static int default_async = FD_DEFAULT_ASYNC;

static lispval dteval_sock(u8_socket conn,lispval expr)
{
  lispval response; int retval;
  struct FD_STREAM _stream, *stream;
  struct FD_OUTBUF *out;
  memset(&_stream,0,sizeof(_stream));
  stream = fd_init_stream
    (&_stream,NULL,conn,
     FD_STREAM_DOSYNC|FD_STREAM_SOCKET|FD_STREAM_OWNS_FILENO,
     fd_network_bufsize);
  out = fd_writebuf(stream);
  if (log_eval_request)
    u8_logf(LOG_DEBUG,"DTEVAL","On #%d: %q",conn,expr);
  retval = fd_write_dtype(out,expr);
  if ((retval<0) || (fd_flush_stream(stream)<0)) {
    fd_close_stream(stream,FD_STREAM_FREEDATA|FD_STREAM_NOCLOSE);
    return FD_ERROR;}
  else response = fd_read_dtype(fd_readbuf(stream));
  if (log_eval_response)
    u8_logf(LOG_DEBUG,"DTEVAL","On #%d: REQUEST %q\n\t==>\t%q",
            conn,response);
  fd_close_stream(stream,FD_STREAM_FREEDATA|FD_STREAM_NOCLOSE);
  return response;
}
static lispval dteval_connpool(struct U8_CONNPOOL *cpool,lispval expr,int async)
{
  lispval result; int retval;
  struct FD_STREAM stream;
  u8_socket conn = u8_get_connection(cpool);
  if (conn<0) return FD_ERROR;
  if (log_eval_request)
    u8_logf(LOG_DEBUG,"DTEVAL","On %s%s#%d: %q",
            (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
             (async)?(" (async) "):("")),
            cpool->u8cp_id,conn,expr);
  memset(&stream,0,sizeof(stream));
  fd_init_stream(&stream,cpool->u8cp_id,conn,
                 FD_STREAM_SOCKET,
                 fd_network_bufsize);
  if ((async)&&(fd_use_dtblock)) { /*  */
    size_t dtype_len;
    fd_outbuf out = fd_writebuf(&stream);
    retval = fd_write_byte(out,dt_block);
    if (retval>0) retval = fd_write_4bytes(out,0);
    if (retval>0) retval = fd_write_dtype(out,expr);
    dtype_len = (out->bufwrite-out->buffer)-5;
    out->bufwrite = out->buffer+1;
    fd_write_4bytes(out,dtype_len);
    out->bufwrite = out->buffer+(dtype_len+5);}
  else {
    fd_outbuf out = fd_writebuf(&stream);
    stream.stream_flags |= FD_STREAM_DOSYNC;
    retval = fd_write_dtype(out,expr);}
  if ((retval<0)||(fd_flush_stream(&stream)<0)) {
    u8_logf(LOG_ERR,"DTEVAL","Error with request to %s%s#%d for: %q",
            (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
             (async)?(" (async) "):("")),
            cpool->u8cp_id,conn,expr);
    fd_clear_errors(1);
    u8_logf(LOG_ERR,"DTEVAL","Reconnecting %s to %s for %q",
            (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
             (async)?(" (async) "):("")),
            cpool->u8cp_id,expr);
    if ((conn = u8_reconnect(cpool,conn))<0) {
      u8_logf(LOG_ERR,"DTEVAL","Reconnection %s failed to %s for %q",
              (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
               (async)?(" (async) "):("")),
              cpool->u8cp_id,expr);
      u8_discard_connection(cpool,conn);
      return FD_ERROR;}
    else {
      u8_logf(LOG_ERR,"DTEVAL","Reconnected %s to %s#%d for %q",
              (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
               (async)?(" (async) "):("")),
              cpool->u8cp_id,conn,expr);}
    fd_flush_stream(&stream);}
  result = fd_read_dtype(fd_readbuf(&stream));
  if (FD_EQ(result,FD_EOD)) {
    fd_clear_errors(1);
    if (((conn = u8_reconnect(cpool,conn))<0) ||
        (fd_write_dtype(fd_writebuf(&stream),expr)<0) ||
        (fd_flush_stream(&stream)<0)) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return FD_ERROR;}
    else result = fd_read_dtype(fd_readbuf(&stream));
    if (FD_EQ(result,FD_EOD)) {
      u8_discard_connection(cpool,conn);
      return fd_err(fd_UnexpectedEOD,"",NULL,expr);}}
  if (log_eval_response) {
    if (CONSP(result))
      u8_logf(LOG_DEBUG,"DTEVAL","On %s%s#%d ==> %hq",
              (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
               (async)?(" (async) "):("")),
              cpool->u8cp_id,conn,FD_PTR_TYPE(result));
    else u8_logf(LOG_DEBUG,"DTEVAL","On %s%s#%d ==> %q",
                 (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
                  (async)?(" (async) "):("")),
                 cpool->u8cp_id,conn,result);}
  fd_close_stream(&stream,FD_STREAM_FREEDATA);
  u8_return_connection(cpool,conn);
  return result;
}
FD_EXPORT lispval fd_dteval(struct U8_CONNPOOL *cp,lispval expr)
{
  return dteval_connpool(cp,expr,default_async);
}
FD_EXPORT lispval fd_dteval_sync(struct U8_CONNPOOL *cp,lispval expr)
{
  return dteval_connpool(cp,expr,0);
}
FD_EXPORT lispval fd_dteval_async(struct U8_CONNPOOL *cp,lispval expr)
{
  return dteval_connpool(cp,expr,1);
}
FD_EXPORT lispval fd_sock_dteval(u8_socket sock,lispval expr)
{
  return dteval_sock(sock,expr);
}

static lispval quote_symbol;

FD_FASTOP lispval quote_lisp(lispval x,int dorefs,int doeval)
{
  if (dorefs) fd_incref(x);
  if (doeval) return x;
  else if ((SYMBOLP(x)) || (PAIRP(x)))
    return fd_conspair(quote_symbol,fd_conspair(x,NIL));
  else return x;
}
static lispval dtapply(struct U8_CONNPOOL *cp,int n,int dorefs,int doeval,
                       lispval *args)
{
  lispval request = NIL, result = VOID;
  n--; while (n>0) {
    request = fd_conspair(quote_lisp(args[n],dorefs,((doeval)&(1<<n))),
                          request);
    n--;}
  if (dorefs) fd_incref(args[0]);
  request = fd_conspair(args[0],request);
  result = dteval_connpool(cp,request,default_async);
  fd_decref(request);
  return result;
}
FD_EXPORT lispval fd_dtapply(struct U8_CONNPOOL *cp,int n,lispval *args)
{
  return dtapply(cp,n,1,0,args);
}

FD_EXPORT lispval fd_dtcall(struct U8_CONNPOOL *cp,int n,...)
{
  int i = 0; va_list arglist;
  lispval *args, _args[8], result;
  va_start(arglist,n);
  if (n>8)
    args = u8_alloc_n(n,lispval);
  else args=_args;
  while (i<n) args[i++]=va_arg(arglist,lispval);
  result = dtapply(cp,n,1,0,args);
  if (args!=_args) u8_free(args);
  return result;
}

FD_EXPORT lispval fd_dtcall_x(struct U8_CONNPOOL *cp,int doeval,int n,...)
{
  int i = 0; va_list arglist;
  lispval *args, _args[8], result;
  va_start(arglist,n);
  if (n>8)
    args = u8_alloc_n(n,lispval);
  else args=_args;
  while (i<n) args[i++]=va_arg(arglist,lispval);
  result = dtapply(cp,n,1,doeval,args);
  if (args!=_args) u8_free(args);
  return result;
}

FD_EXPORT lispval fd_dtcall_nr(struct U8_CONNPOOL *cp,int n,...)
{
  int i = 0; va_list arglist;
  lispval *args, _args[8], result;
  va_start(arglist,n);
  if (n>8)
    args = u8_alloc_n(n,lispval);
  else args=_args;
  while (i<n) args[i++]=va_arg(arglist,lispval);
  result = dtapply(cp,n,0,0,args);
  if (args!=_args) u8_free(args);
  return result;
}

FD_EXPORT lispval fd_dtcall_nrx(struct U8_CONNPOOL *cp,int doeval,int n,...)
{
  int i = 0; va_list arglist;
  lispval *args, _args[8], result;
  va_start(arglist,n);
  if (n>8)
    args = u8_alloc_n(n,lispval);
  else args=_args;
  while (i<n) args[i++]=va_arg(arglist,lispval);
  result = dtapply(cp,n,0,doeval,args);
  if (args!=_args) u8_free(args);
  return result;
}

FD_EXPORT void fd_init_dtcall_c()
{
  quote_symbol = fd_intern("QUOTE");
  fd_register_config("ASYNC",
                     _("Assume asynchronous DType servers"),
                     fd_boolconfig_get,fd_boolconfig_set,&default_async);
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
