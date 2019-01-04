/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/streams.h"
#include "framerd/apply.h"
#include "framerd/storage.h"
#include "framerd/dtproc.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#include <libu8/u8printf.h>

static lispval quote_symbol;

FD_EXPORT lispval fd_make_dtproc(u8_string name,u8_string server,
                                 int ndcall,int arity,int min_arity,
                                 int minsock,int maxsock,int initsock)
{
  struct FD_DTPROC *f = u8_alloc(struct FD_DTPROC);
  FD_INIT_CONS(f,fd_dtproc_type);
  f->fcn_name = u8_mkstring("%s/%s",name,server);
  f->fcn_filename = u8_strdup(server);
  f->dtprocserver = u8_strdup(server);
  f->dtprocname = fd_intern(name);
  f->fcn_ndcall = ndcall; f->fcn_min_arity = min_arity;
  f->fcn_arity = arity; f->fcn_xcall = 1;
  f->fcn_typeinfo = NULL; f->fcn_defaults = NULL;
  f->fcn_handler.fnptr = NULL;
  if (minsock<0) minsock = 2;
  if (maxsock<0) maxsock = minsock+3;
  if (initsock<0) initsock = 1;
  f->connpool =
    u8_open_connpool(f->dtprocserver,minsock,maxsock,initsock);
  if (f->connpool == NULL) {
    u8_free(f->fcn_name);
    u8_free(f->fcn_filename);
    u8_free(f);
    return FD_ERROR;}
  else return LISP_CONS(f);
}

static int unparse_dtproc(u8_output out,lispval x)
{
  struct FD_DTPROC *f = fd_consptr(fd_dtproc,x,fd_dtproc_type);
  u8_printf(out,"#<!DTPROC %s using %s>",f->fcn_name,
            f->dtprocserver);
  return 1;
}

static void recycle_dtproc(struct FD_RAW_CONS *c)
{
  struct FD_DTPROC *f = (fd_dtproc)c;
  u8_free(f->fcn_name);
  u8_free(f->fcn_filename);
  u8_free(f->dtprocserver);
  if (f->fcn_typeinfo) u8_free(f->fcn_typeinfo);
  if (f->fcn_defaults) u8_free(f->fcn_defaults);
  if (!(FD_STATIC_CONSP(f))) u8_free(f);
}

static lispval dtapply(struct FD_DTPROC *dtp,int n,lispval *args)
{
  struct FD_STREAM stream;
  u8_connpool cpool = dtp->connpool;
  lispval expr = NIL, result; int i = n-1;
  u8_socket conn = u8_get_connection(cpool);
  if (conn<0) return FD_ERROR;
  fd_init_stream(&stream,NULL,conn,FD_STREAM_SOCKET,fd_network_bufsize);
  while (i>=0) {
    if ((SYMBOLP(args[i])) || (PAIRP(args[i])))
      expr = fd_conspair(fd_make_list(2,quote_symbol,fd_incref(args[i])),
                         expr);
    else expr = fd_conspair(fd_incref(args[i]),expr);
    i--;}
  expr = fd_conspair(dtp->dtprocname,expr);
  /* u8_logf(LOG_DEBUG,"DTPROC","Using connection %d",conn); */
  if ((fd_write_dtype(fd_writebuf(&stream),expr)<0) ||
      (fd_flush_stream(&stream)<0)) {
    fd_clear_errors(1);
    if ((conn = u8_reconnect(cpool,conn))<0) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return FD_ERROR;}}
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
      if (conn>0) u8_discard_connection(cpool,conn);
      return fd_err(fd_UnexpectedEOD,"",dtp->dtprocserver,expr);}}
  /* u8_logf(LOG_DEBUG,"DTPROC","Freeing %d",conn); */
  u8_return_connection(cpool,conn);
  return result;
}

FD_EXPORT void fd_init_dtproc_c()
{
  quote_symbol = fd_intern("QUOTE");

  u8_register_source_file(_FILEINFO);
  u8_register_source_file(FRAMERD_DTPROC_H_INFO);

  fd_type_names[fd_dtproc_type]=_("dtproc");
  fd_applyfns[fd_dtproc_type]=(fd_applyfn)dtapply;
  fd_functionp[fd_dtproc_type]=1;

  fd_unparsers[fd_dtproc_type]=unparse_dtproc;
  fd_recyclers[fd_dtproc_type]=recycle_dtproc;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
