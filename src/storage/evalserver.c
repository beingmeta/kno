/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_EVAL 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include "kno/support.h"
#include "kno/xtypes.h"
#include "kno/dtypeio.h"
#include "kno/remote.h"
#include "kno/netproc.h"
#include "kno/evalserver.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>

static lispval xtype_symbol, dtype_symbol, protocol_symbol = 0;

/* Remote evaluation */

static u8_condition ServerUndefined=_("Server unconfigured");

KNO_EXPORT lispval kno_init_evalserver
(struct KNO_EVALSERVER *evalserver,
 u8_string server,lispval opts)
{
  if (evalserver == NULL)
    evalserver = u8_alloc(struct KNO_EVALSERVER);
  enum EVALSERVER_PROTOCOL protocol;
  int minsock = -1, maxsock = -1, initsock = 1; 
  u8_string server_addr; u8_socket socket;
  /* Start out by parsing the address */
  if ((*server)==':') {
    lispval server_id = kno_config_get(server+1);
    if (STRINGP(server_id))
      server_addr = u8_strdup(CSTRING(server_id));
    else  {
      kno_seterr(ServerUndefined,"open_server",
		 evalserver->evalserverid,server_id);
      u8_free(evalserver);
      return -1;}}
  else server_addr = u8_strdup(server);
  if (opts == xtype_symbol)
    protocol = xtype_protocol;
  else protocol = dtype_protocol;
  if (KNO_TABLEP(opts)) {
    lispval protocol_val = kno_getopt(opts,KNOSYM(protocol),KNO_VOID);
    minsock = kno_getfixopt(opts,"minsock",-1);
    maxsock = kno_getfixopt(opts,"maxsock",-1);
    initsock = kno_getfixopt(opts,"initsock",-1);
    if (protocol_val == xtype_symbol)
      protocol = xtype_protocol;
    else protocol = dtype_protocol;}
  evalserver->evalserverid = u8_strdup(server);
  evalserver->evalserver_addr = server_addr;
  /* Then try to connect, just to see if that works */
  socket = u8_connect_x(server,&(evalserver->evalserver_addr));
  if (socket<0) {
    /* If connecting fails, signal an error rather than creating
       the evalserver connection pool. */
    u8_free(evalserver->evalserverid);
    u8_free(evalserver->evalserver_addr);
    u8_free(evalserver);
    return kno_err(kno_ConnectionFailed,"kno_open_evalserver",
		   u8_strdup(server),VOID);}
  /* Otherwise, close the socket */
  else close(socket);
  kno_init_xrefs(&(evalserver->refs),0,0,0,0,NULL,NULL);
  evalserver->evalserver_transport = simple_socket;
  evalserver->evalserver_protocol = protocol;
  /* And create a connection pool */
  evalserver->connection.connpool =
    u8_open_connpool(evalserver->evalserverid,minsock,maxsock,initsock);
  /* If creating the connection pool fails for some reason,
     cleanup and return an error value. */
  if (evalserver->connection.connpool == NULL) {
    u8_free(evalserver->evalserverid);
    u8_free(evalserver->evalserver_addr);
    u8_free(evalserver);
    return KNO_ERROR;}
  /* Otherwise, returh a evalserver object */
  KNO_INIT_CONS(evalserver,kno_evalserver_type);
  return LISP_CONS(evalserver);
}

KNO_EXPORT kno_evalserver kno_open_evalserver(u8_string server,lispval opts)
{
  lispval made = kno_init_evalserver(NULL,server,opts);
  if (KNO_TYPEP(made,kno_evalserver_type))
    return (kno_evalserver) made;
  else return NULL;
}

KNO_EXPORT void kno_init_evalserver_c()
{
  xtype_symbol = kno_intern("xtype");
  dtype_symbol = kno_intern("dtype");

  u8_register_source_file(_FILEINFO);
  u8_register_source_file(KNO_NETPROC_H_INFO);
}

