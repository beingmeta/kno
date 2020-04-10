/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_BUFIO 1
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/streams.h"
#include "kno/apply.h"
#include "kno/storage.h"
#include "kno/netproc.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#include <libu8/u8printf.h>

KNO_EXPORT lispval kno_make_netproc(kno_evalserver server,u8_string name,
				    int ndcall,int arity,int min_arity)
{
  struct KNO_NETPROC *f = u8_alloc(struct KNO_NETPROC);
  KNO_INIT_CONS(f,kno_rpc_type);
  f->fcn_name = u8_mkstring("%s/%s",name,server);
  f->fcn_filename = u8_strdup(server->evalserverid);
  f->netprocname = kno_intern(name);
  f->evalserver = server; kno_incref((lispval)server);
  if (ndcall)
    f->fcn_call |= KNO_CALL_NDCALL;
  f->fcn_min_arity = min_arity;
  f->fcn_call_width = f->fcn_arity = arity;
  f->fcn_call |= KNO_CALL_XCALL;
  f->fcn_handler.fnptr = NULL;
  return LISP_CONS(f);
}

static int unparse_netproc(u8_output out,lispval x)
{
  struct KNO_NETPROC *f = kno_consptr(kno_netproc,x,kno_rpc_type);
  u8_printf(out,"#<!NETPROC %s using %s>",f->fcn_name,
            f->evalserver->evalserver_addr);
  return 1;
}

static void recycle_netproc(struct KNO_RAW_CONS *c)
{
  struct KNO_NETPROC *f = (kno_netproc)c;
  kno_decref((lispval)(f->evalserver));
  u8_free(f->fcn_name);
  u8_free(f->fcn_filename);
  /* close eval server */
  if (!(KNO_STATIC_CONSP(f))) u8_free(f);
}

static lispval netproc_apply(struct KNO_NETPROC *dtp,int n,lispval *args)
{
  struct KNO_STREAM stream;
  u8_connpool cpool = dtp->evalserver->connection.connpool;
  lispval expr = NIL, result; int i = n-1;
  u8_socket conn = u8_get_connection(cpool);
  if (conn<0) return KNO_ERROR;
  kno_init_stream(&stream,NULL,conn,KNO_STREAM_SOCKET,kno_network_bufsize);
  while (i>=0) {
    if ((SYMBOLP(args[i])) || (PAIRP(args[i])))
      expr = kno_conspair(kno_make_list(2,KNOSYM_QUOTE,kno_incref(args[i])),
                         expr);
    else expr = kno_conspair(kno_incref(args[i]),expr);
    i--;}
  expr = kno_conspair(dtp->netprocname,expr);
  /* u8_logf(LOG_DEBUG,"NETPROC","Using connection %d",conn); */
  if ((kno_write_dtype(kno_writebuf(&stream),expr)<0) ||
      (kno_flush_stream(&stream)<0)) {
    kno_clear_errors(1);
    if ((conn = u8_reconnect(cpool,conn))<0) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return KNO_ERROR;}}
  result = kno_read_dtype(kno_readbuf(&stream));
  if (KNO_EQ(result,KNO_EOD)) {
    kno_clear_errors(1);
    if (((conn = u8_reconnect(cpool,conn))<0) ||
        (kno_write_dtype(kno_writebuf(&stream),expr)<0) ||
        (kno_flush_stream(&stream)<0)) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return KNO_ERROR;}
    else result = kno_read_dtype(kno_readbuf(&stream));
    if (KNO_EQ(result,KNO_EOD)) {
      if (conn>0) u8_discard_connection(cpool,conn);
      return kno_err(kno_UnexpectedEOD,"",dtp->evalserver->evalserverid,expr);}}
  /* u8_logf(LOG_DEBUG,"NETPROC","Freeing %d",conn); */
  u8_return_connection(cpool,conn);
  return result;
}

KNO_EXPORT void kno_init_netproc_c()
{
  u8_register_source_file(_FILEINFO);
  u8_register_source_file(KNO_NETPROC_H_INFO);

  kno_type_names[kno_rpc_type]=_("netproc");
  kno_applyfns[kno_rpc_type]=(kno_applyfn)netproc_apply;
  kno_isfunctionp[kno_rpc_type]=1;

  kno_unparsers[kno_rpc_type]=unparse_netproc;
  kno_recyclers[kno_rpc_type]=recycle_netproc;
}

