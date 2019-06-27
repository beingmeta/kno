/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include "kno/support.h"
#include "kno/cprims.h"
#include "kno/dtcall.h"
#include "kno/dtypeio.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>

/* Remote evaluation */

static u8_condition ServerUndefined=_("Server unconfigured");

KNO_EXPORT lispval kno_open_dtserver(u8_string server,int bufsiz)
{
  struct KNO_DTSERVER *dts = u8_alloc(struct KNO_DTSERVER);
  u8_string server_addr; u8_socket socket;
  /* Start out by parsing the address */
  if ((*server)==':') {
    lispval server_id = kno_config_get(server+1);
    if (STRINGP(server_id))
      server_addr = u8_strdup(CSTRING(server_id));
    else  {
      kno_seterr(ServerUndefined,"open_server",
                dts->dtserverid,server_id);
      u8_free(dts);
      return -1;}}
  else server_addr = u8_strdup(server);
  dts->dtserverid = u8_strdup(server);
  dts->dtserver_addr = server_addr;
  /* Then try to connect, just to see if that works */
  socket = u8_connect_x(server,&(dts->dtserver_addr));
  if (socket<0) {
    /* If connecting fails, signal an error rather than creating
       the dtserver connection pool. */
    u8_free(dts->dtserverid);
    u8_free(dts->dtserver_addr);
    u8_free(dts);
    return kno_err(kno_ConnectionFailed,"kno_open_dtserver",
                  u8_strdup(server),VOID);}
  /* Otherwise, close the socket */
  else close(socket);
  /* And create a connection pool */
  dts->connpool = u8_open_connpool(dts->dtserverid,2,4,1);
  /* If creating the connection pool fails for some reason,
     cleanup and return an error value. */
  if (dts->connpool == NULL) {
    u8_free(dts->dtserverid);
    u8_free(dts->dtserver_addr);
    u8_free(dts);
    return KNO_ERROR;}
  /* Otherwise, returh a dtserver object */
  KNO_INIT_CONS(dts,kno_dtserver_type);
  return LISP_CONS(dts);
}

static lispval dtserverp(lispval arg)
{
  if (TYPEP(arg,kno_dtserver_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval dtserver_id(lispval arg)
{
  struct KNO_DTSERVER *dts = (struct KNO_DTSERVER *) arg;
  return kno_mkstring(dts->dtserverid);
}

static lispval dtserver_address(lispval arg)
{
  struct KNO_DTSERVER *dts = (struct KNO_DTSERVER *) arg;
  return kno_mkstring(dts->dtserver_addr);
}

static lispval dteval(lispval server,lispval expr)
{
  if (TYPEP(server,kno_dtserver_type))  {
    struct KNO_DTSERVER *dtsrv=
      kno_consptr(kno_stream_erver,server,kno_dtserver_type);
    return kno_dteval(dtsrv->connpool,expr);}
  else if (STRINGP(server)) {
    lispval s = kno_open_dtserver(CSTRING(server),-1);
    if (KNO_ABORTED(s)) return s;
    else {
      lispval result = kno_dteval(((kno_stream_erver)s)->connpool,expr);
      kno_decref(s);
      return result;}}
  else return kno_type_error(_("server"),"dteval",server);
}

static lispval dtcall(int n,lispval *args)
{
  lispval server; lispval request = NIL, result; int i = n-1;
  if (n<2) return kno_err(kno_SyntaxError,"dtcall",NULL,VOID);
  if (TYPEP(args[0],kno_dtserver_type))
    server = kno_incref(args[0]);
  else if (STRINGP(args[0])) server = kno_open_dtserver(CSTRING(args[0]),-1);
  else return kno_type_error(_("server"),"eval/dtcall",args[0]);
  if (KNO_ABORTED(server)) return server;
  while (i>=1) {
    lispval param = args[i];
    if ((i>1) && ((SYMBOLP(param)) || (PAIRP(param))))
      request = kno_conspair(kno_make_list(2,KNOSYM_QUOTE,param),request);
    else request = kno_conspair(param,request);
    kno_incref(param); i--;}
  result = kno_dteval(((kno_stream_erver)server)->connpool,request);
  kno_decref(request);
  kno_decref(server);
  return result;
}

static lispval open_dtserver(lispval server,lispval bufsiz)
{
  return kno_open_dtserver(CSTRING(server),
                             ((VOIDP(bufsiz)) ? (-1) :
                              (FIX2INT(bufsiz))));
}

/* Initialization */

KNO_EXPORT void kno_init_dteval_c()
{
  u8_register_source_file(_FILEINFO);

  kno_idefn(kno_scheme_module,kno_make_cprim2("DTEVAL",dteval,2));
  kno_idefn(kno_scheme_module,kno_make_cprimn("DTCALL",dtcall,2));
  kno_idefn(kno_scheme_module,kno_make_cprim2x("OPEN-DTSERVER",open_dtserver,1,
                                            kno_string_type,VOID,
                                            kno_fixnum_type,VOID));
  kno_idefn1(kno_scheme_module,"DTSERVER?",dtserverp,KNO_NEEDS_1_ARG,
            "Returns true if it's argument is a dtype server object",
            -1,VOID);
  kno_idefn1(kno_scheme_module,"DTSERVER-ID",
            dtserver_id,KNO_NEEDS_1_ARG,
            "Returns the ID of a dtype server (the argument used to create it)",
            kno_dtserver_type,VOID);
  kno_idefn1(kno_scheme_module,"DTSERVER-ADDRESS",
            dtserver_address,KNO_NEEDS_1_ARG,
            "Returns the address (host/port) of a dtype server",
            kno_dtserver_type,VOID);
}
