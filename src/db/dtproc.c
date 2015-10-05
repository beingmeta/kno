/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_DTYPEIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dtypestream.h"
#include "framerd/apply.h"
#include "framerd/fddb.h"
#include "framerd/dtproc.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#include <libu8/u8printf.h>

static fdtype quote_symbol;

FD_EXPORT fdtype fd_make_dtproc(u8_string name,u8_string server,int ndcall,int arity,int min_arity,int minsock,int maxsock,int initsock)
{
  struct FD_DTPROC *f=u8_alloc(struct FD_DTPROC);
  FD_INIT_CONS(f,fd_dtproc_type);
  f->name=u8_mkstring("%s/%s",name,server); f->filename=u8_strdup(server);
  f->server=u8_strdup(server); f->fcnsym=fd_intern(name);
  f->ndprim=ndcall; f->min_arity=min_arity; f->arity=arity; f->xprim=1;
  f->typeinfo=NULL; f->defaults=NULL;
  f->handler.fnptr=NULL;
  if (minsock<0) minsock=2;
  if (maxsock<0) maxsock=minsock+3;
  if (initsock<0) initsock=1;
  f->connpool=u8_open_connpool(f->server,minsock,maxsock,initsock);
  if (f->connpool==NULL) {
    u8_free(f->name); u8_free(f->filename); u8_free(f);
    return FD_ERROR_VALUE;}
  else return FDTYPE_CONS(f);
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
  u8_free(f->name); u8_free(f->filename); u8_free(f->server);
  if (f->typeinfo) u8_free(f->typeinfo);
  if (f->defaults) u8_free(f->defaults);
  u8_free(f);
}

static fdtype dtapply(struct FD_DTPROC *dtp,int n,fdtype *args)
{
  struct FD_DTYPE_STREAM stream;
  u8_connpool cpool=dtp->connpool;
  fdtype expr=FD_EMPTY_LIST, result; int i=n-1;
  u8_socket conn=u8_get_connection(cpool);
  if (conn<0) return FD_ERROR_VALUE;
  fd_init_dtype_stream(&stream,conn,8192);
  while (i>=0) {
    if ((FD_SYMBOLP(args[i])) || (FD_PAIRP(args[i])))
      expr=fd_conspair(fd_make_list(2,quote_symbol,fd_incref(args[i])),
                       expr);
    else expr=fd_conspair(fd_incref(args[i]),expr);
    i--;}
  expr=fd_conspair(dtp->fcnsym,expr);
  /* u8_log(LOG_DEBUG,"DTPROC","Using connection %d",conn); */
  if ((fd_dtswrite_dtype(&stream,expr)<0) ||
      (fd_dtsflush(&stream)<0)) {
    fd_clear_errors(1);
    if ((conn=u8_reconnect(cpool,conn))<0) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return FD_ERROR_VALUE;}}
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
      if (conn>0) u8_discard_connection(cpool,conn);
      return fd_err(fd_UnexpectedEOD,"",dtp->server,expr);}}
  /* u8_log(LOG_DEBUG,"DTPROC","Freeing %d",conn); */
  u8_return_connection(cpool,conn);
  return result;
}

FD_EXPORT void fd_init_dtproc_c()
{
  quote_symbol=fd_intern("QUOTE");

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
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
