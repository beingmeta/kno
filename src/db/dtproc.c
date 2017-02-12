/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
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

FD_EXPORT fdtype fd_make_dtproc(u8_string name,u8_string server,
                                int ndcall,int arity,int min_arity,
                                int minsock,int maxsock,int initsock)
{
  struct FD_DTPROC *f=u8_alloc(struct FD_DTPROC);
  FD_INIT_CONS(f,fd_dtproc_type);
  f->fdfn_name=u8_mkstring("%s/%s",name,server);
  f->fdfn_filename=u8_strdup(server);
  f->fd_dtprocserver=u8_strdup(server);
  f->fd_dtprocname=fd_intern(name);
  f->fdfn_ndcall=ndcall; f->fdfn_min_arity=min_arity;
  f->fdfn_arity=arity; f->fdfn_xcall=1;
  f->fdfn_typeinfo=NULL; f->fdfn_defaults=NULL;
  f->fdfn_handler.fnptr=NULL;
  if (minsock<0) minsock=2;
  if (maxsock<0) maxsock=minsock+3;
  if (initsock<0) initsock=1;
  f->fd_connpool=u8_open_connpool(f->fd_dtprocserver,minsock,maxsock,initsock);
  if (f->fd_connpool==NULL) {
    u8_free(f->fdfn_name);
    u8_free(f->fdfn_filename); 
    u8_free(f);
    return FD_ERROR_VALUE;}
  else return FDTYPE_CONS(f);
}

static int unparse_dtproc(u8_output out,fdtype x)
{
  struct FD_DTPROC *f=FD_GET_CONS(x,fd_dtproc_type,fd_dtproc);
  u8_printf(out,"#<!DTPROC %s using %s>",f->fdfn_name,f->fd_dtprocserver);
  return 1;
}

static void recycle_dtproc(FD_CONS *c)
{
  struct FD_DTPROC *f=(fd_dtproc)c;
  u8_free(f->fdfn_name); 
  u8_free(f->fdfn_filename); 
  u8_free(f->fd_dtprocserver);
  if (f->fdfn_typeinfo) u8_free(f->fdfn_typeinfo);
  if (f->fdfn_defaults) u8_free(f->fdfn_defaults);
  if (!(FD_STATIC_CONSP(f))) u8_free(f);
}

static fdtype dtapply(struct FD_DTPROC *dtp,int n,fdtype *args)
{
  struct FD_DTYPE_STREAM stream;
  u8_connpool cpool=dtp->fd_connpool;
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
  expr=fd_conspair(dtp->fd_dtprocname,expr);
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
      return fd_err(fd_UnexpectedEOD,"",dtp->fd_dtprocserver,expr);}}
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
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
