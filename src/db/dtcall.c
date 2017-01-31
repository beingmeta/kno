/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dtcall.h"
#include "framerd/dtypestream.h"

#include <libu8/u8netfns.h>

#include <stdarg.h>

#ifndef FD_USE_DTBLOCK
#define FD_USE_DTBLOCK 0
#endif

static int log_eval_request=0, log_eval_response=0;
static int default_async=FD_DEFAULT_ASYNC;

static fdtype dteval_sock(u8_socket conn,fdtype expr)
{
  int retval;
  struct FD_DTYPE_STREAM stream;
  fd_init_dtype_stream(&stream,conn,8192);
  stream.flags=stream.flags|FD_DTSTREAM_DOSYNC;
  if (log_eval_request)
    u8_log(LOG_DEBUG,"DTEVAL","On #%d: %q",conn,expr);
  retval=fd_dtswrite_dtype(&stream,expr);
  if ((retval<0) || (fd_dtsflush(&stream)<0)) {
    return FD_ERROR_VALUE;}
  if (log_eval_response) {
    fdtype response=fd_dtsread_dtype(&stream);
    u8_log(LOG_DEBUG,"DTEVAL","On #%d: REQUEST %q\n\t==>\t%q",
           conn,response);
    return response;}
  else return fd_dtsread_dtype(&stream);
}
static fdtype dteval_pool(struct U8_CONNPOOL *cpool,fdtype expr,int async)
{
  fdtype result; int retval;
  struct FD_DTYPE_STREAM stream;
  u8_socket conn=u8_get_connection(cpool);
  if (conn<0) return FD_ERROR_VALUE;
  if (log_eval_request)
    u8_log(LOG_DEBUG,"DTEVAL","On %s%s#%d: %q",
           (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
            (async)?(" (async) "):("")),
           cpool->u8cp_id,conn,expr);
  fd_init_dtype_stream(&stream,conn,8192);
  if ((async)&&(fd_use_dtblock)) { /*  */
    size_t dtype_len;
    struct FD_BYTE_OUTPUT *binout=
      (struct FD_BYTE_OUTPUT *)&stream;
    retval=fd_write_byte(binout,dt_block);
    if (retval>0) retval=fd_write_4bytes(binout,0);
    if (retval>0) retval=fd_write_dtype(binout,expr);
    dtype_len=(binout->ptr-binout->start)-5;
    binout->ptr=binout->start+1;
    fd_write_4bytes(binout,dtype_len);
    binout->ptr=binout->start+(dtype_len+5);}
  else {
    stream.flags=stream.flags|FD_DTSTREAM_DOSYNC;
    retval=fd_dtswrite_dtype(&stream,expr);}
  if ((retval<0)||(fd_dtsflush(&stream)<0)) {
    u8_log(LOG_ERR,"DTEVAL","Error with request to %s%s#%d for: %q",
           (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
            (async)?(" (async) "):("")),
           cpool->u8cp_id,conn,expr);
    fd_clear_errors(1);
    u8_log(LOG_ERR,"DTEVAL","Reconnecting %s to %s for %q",
           (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
            (async)?(" (async) "):("")),
           cpool->u8cp_id,expr);
    if ((conn=u8_reconnect(cpool,conn))<0) {
      u8_log(LOG_ERR,"DTEVAL","Reconnection %s failed to %s for %q",
             (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
              (async)?(" (async) "):("")),
             cpool->u8cp_id,expr);
      u8_discard_connection(cpool,conn);
      return FD_ERROR_VALUE;}
    else {
      u8_log(LOG_ERR,"DTEVAL","Reconnected %s to %s#%d for %q",
             (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
              (async)?(" (async) "):("")),
             cpool->u8cp_id,conn,expr);}
    fd_dtsflush(&stream);}
  result=fd_dtsread_dtype(&stream);
  if (FD_EQ(result,FD_EOD)) {
    fd_clear_errors(1);
    if (((conn=u8_reconnect(cpool,conn))<0) ||
        (fd_dtswrite_dtype(&stream,expr)<0) ||
        (fd_dtsflush(&stream)<0)) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return FD_ERROR_VALUE;}
    else result=fd_dtsread_dtype(&stream);
    if (FD_EQ(result,FD_EOD)) {
      u8_discard_connection(cpool,conn);
      return fd_err(fd_UnexpectedEOD,"",NULL,expr);}}
  if (log_eval_response) {
    if (FD_CONSP(result))
      u8_log(LOG_DEBUG,"DTEVAL","On %s%s#%d ==> %hq",
             (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
            (async)?(" (async) "):("")),
             cpool->u8cp_id,conn,FD_PTR_TYPE(result));
    else u8_log(LOG_DEBUG,"DTEVAL","On %s%s#%d ==> %q",
                (((async)&&(fd_use_dtblock))?(" (async/dtblock) "):
                 (async)?(" (async) "):("")),
                cpool->u8cp_id,conn,result);}
  fd_dtsclose(&stream,0);
  u8_return_connection(cpool,conn);
  return result;
}
FD_EXPORT fdtype fd_dteval(struct U8_CONNPOOL *cp,fdtype expr)
{
  return dteval_pool(cp,expr,default_async);
}
FD_EXPORT fdtype fd_dteval_sync(struct U8_CONNPOOL *cp,fdtype expr)
{
  return dteval_pool(cp,expr,0);
}
FD_EXPORT fdtype fd_dteval_async(struct U8_CONNPOOL *cp,fdtype expr)
{
  return dteval_pool(cp,expr,1);
}
FD_EXPORT fdtype fd_sock_dteval(u8_socket sock,fdtype expr)
{
  return dteval_sock(sock,expr);
}

static fdtype quote_symbol;

FD_FASTOP fdtype quote_lisp(fdtype x,int dorefs,int doeval)
{
  if (dorefs) fd_incref(x);
  if (doeval) return x;
  else if ((FD_SYMBOLP(x)) || (FD_PAIRP(x)))
    return fd_conspair(quote_symbol,fd_conspair(x,FD_EMPTY_LIST));
  else return x;
}
static fdtype dtapply(struct U8_CONNPOOL *cp,int n,int dorefs,int doeval,
                      fdtype *args)
{
  fdtype request=FD_EMPTY_LIST, result=FD_VOID;
  n--; while (n>0) {
    request=fd_conspair(quote_lisp(args[n],dorefs,((doeval)&(1<<n))),
                        request);
    n--;}
  if (dorefs) fd_incref(args[0]);
  request=fd_conspair(args[0],request);
  result=dteval_pool(cp,request,default_async);
  fd_decref(request);
  return result;
}
FD_EXPORT fdtype fd_dtapply(struct U8_CONNPOOL *cp,int n,fdtype *args)
{
  return dtapply(cp,n,1,0,args);
}

FD_EXPORT fdtype fd_dtcall(struct U8_CONNPOOL *cp,int n,...)
{
  int i=0; va_list arglist;
  fdtype *args, _args[8], result;
  va_start(arglist,n);
  if (n>8)
    args=u8_alloc_n(n,fdtype);
  else args=_args;
  while (i<n) args[i++]=va_arg(arglist,fdtype);
  result=dtapply(cp,n,1,0,args);
  if (args!=_args) u8_free(args);
  return result;
}

FD_EXPORT fdtype fd_dtcall_x(struct U8_CONNPOOL *cp,int doeval,int n,...)
{
  int i=0; va_list arglist;
  fdtype *args, _args[8], result;
  va_start(arglist,n);
  if (n>8)
    args=u8_alloc_n(n,fdtype);
  else args=_args;
  while (i<n) args[i++]=va_arg(arglist,fdtype);
  result=dtapply(cp,n,1,doeval,args);
  if (args!=_args) u8_free(args);
  return result;
}

FD_EXPORT fdtype fd_dtcall_nr(struct U8_CONNPOOL *cp,int n,...)
{
  int i=0; va_list arglist;
  fdtype *args, _args[8], result;
  va_start(arglist,n);
  if (n>8)
    args=u8_alloc_n(n,fdtype);
  else args=_args;
  while (i<n) args[i++]=va_arg(arglist,fdtype);
  result=dtapply(cp,n,0,0,args);
  if (args!=_args) u8_free(args);
  return result;
}

FD_EXPORT fdtype fd_dtcall_nrx(struct U8_CONNPOOL *cp,int doeval,int n,...)
{
  int i=0; va_list arglist;
  fdtype *args, _args[8], result;
  va_start(arglist,n);
  if (n>8)
    args=u8_alloc_n(n,fdtype);
  else args=_args;
  while (i<n) args[i++]=va_arg(arglist,fdtype);
  result=dtapply(cp,n,0,doeval,args);
  if (args!=_args) u8_free(args);
  return result;
}

FD_EXPORT void fd_init_dtcall_c()
{
  quote_symbol=fd_intern("QUOTE");
  fd_register_config("ASYNC",
                     _("Assume asynchronous DType servers"),
                     fd_boolconfig_get,fd_boolconfig_set,&default_async);
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
