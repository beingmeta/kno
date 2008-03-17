/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: apply.c 2163 2007-12-08 16:07:24Z haase $";

#include "fdb/dtype.h"
#include "fdb/dtcall.h"
#include "fdb/dtypestream.h"

#include <libu8/u8netfns.h>

#include <stdarg.h>

static fdtype dteval(struct U8_CONNPOOL *cpool,fdtype expr)
{
  fdtype result; 
  struct FD_DTYPE_STREAM stream;
  u8_connection conn=u8_get_connection(cpool);
  fd_init_dtype_stream(&stream,conn,8192);
  stream.flags=stream.flags|FD_DTSTREAM_DOSYNC;
  /* u8_log(LOG_DEBUG,"DTEVAL","Using connection %d",conn); */
  if ((fd_dtswrite_dtype(&stream,expr)<0) ||
      (fd_dtsflush(&stream)<0)) {
    if ((conn=u8_reconnect(cpool,conn))<0) {
      u8_discard_connection(cpool,conn);
      return FD_ERROR_VALUE;}}
  result=fd_dtsread_dtype(&stream);
  if (FD_EQ(result,FD_EOD)) {
    if (((conn=u8_reconnect(cpool,conn))<0) ||
	(fd_dtswrite_dtype(&stream,expr)<0) ||
	(fd_dtsflush(&stream)<0)) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return FD_ERROR_VALUE;}
    else result=fd_dtsread_dtype(&stream);
    if (FD_EQ(result,FD_EOD)) {
      u8_discard_connection(cpool,conn);
      return fd_err(fd_UnexpectedEOD,"",NULL,expr);}}
  /* u8_log(LOG_DEBUG,"DTEVAL","Done with %d",conn); */
  u8_return_connection(cpool,conn);
  return result;
}
FD_EXPORT fdtype fd_dteval(struct U8_CONNPOOL *cp,fdtype expr)
{
  return dteval(cp,expr);
}

static fdtype quote_symbol;

FD_FASTOP fdtype quote_lisp(fdtype x,int dorefs)
{
  if (dorefs) fd_incref(x);
  if ((FD_SYMBOLP(x)) || (FD_PAIRP(x)))
    return fd_init_pair(NULL,quote_symbol,fd_init_pair(NULL,x,FD_EMPTY_LIST));
  else return x;
}
static fdtype dtapply(struct U8_CONNPOOL *cp,int n,int dorefs,fdtype *args)
{
  fdtype request=FD_EMPTY_LIST, result=FD_VOID;
  n--; while (n>0) {
    request=fd_init_pair(NULL,quote_lisp(args[n],dorefs),request);
    n--;}
  if (dorefs) fd_incref(args[0]);
  request=fd_init_pair(NULL,args[0],request);
  result=dteval(cp,request);
  fd_decref(request);
  return result;
}
FD_EXPORT fdtype fd_dtapply(struct U8_CONNPOOL *cp,int n,fdtype *args)
{
  return dtapply(cp,n,1,args);
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
  result=dtapply(cp,n,1,args);
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
  result=dtapply(cp,n,0,args);
  if (args!=_args) u8_free(args);
  return result;
}

FD_EXPORT int fd_init_dtcall_c()
{
  quote_symbol=fd_intern("QUOTE");
}
