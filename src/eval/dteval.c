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
#include "kno/dtproc.h"

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

DEFPRIM1("dtserver?",dtserverp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if it's argument is a dtype server "
	 "object",
	 kno_any_type,KNO_VOID);
static lispval dtserverp(lispval arg)
{
  if (TYPEP(arg,kno_dtserver_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("dtserver-id",dtserver_id,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the ID of a dtype server (the argument "
	 "used to create it)",
	 kno_dtserver_type,KNO_VOID);
static lispval dtserver_id(lispval arg)
{
  struct KNO_DTSERVER *dts = (struct KNO_DTSERVER *) arg;
  return kno_mkstring(dts->dtserverid);
}

DEFPRIM1("dtserver-address",dtserver_address,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the address (host/port) of a dtype server",
	 kno_dtserver_type,KNO_VOID);
static lispval dtserver_address(lispval arg)
{
  struct KNO_DTSERVER *dts = (struct KNO_DTSERVER *) arg;
  return kno_mkstring(dts->dtserver_addr);
}

DEFPRIM2("dteval",dteval,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(DTEVAL *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
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

DEFPRIM("dtcall",dtcall,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(DTCALL *arg0* *arg1* *args...*)` **undocumented**");
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

DEFPRIM2("open-dtserver",open_dtserver,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(OPEN-DTSERVER *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval open_dtserver(lispval server,lispval bufsiz)
{
  return kno_open_dtserver(CSTRING(server),
			   ((VOIDP(bufsiz)) ? (-1) :
			    (FIX2INT(bufsiz))));
}

/* Making DTPROCs */

DEFPRIM7("dtproc",make_dtproc,KNO_MAX_ARGS(7)|KNO_MIN_ARGS(2),
	 "`(DTPROC *arg0* *arg1* [*arg2*] [*arg3*] [*arg4*] [*arg5*] [*arg6*])` **undocumented**",
	 kno_symbol_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(2),kno_fixnum_type,KNO_CPP_INT(4),
	 kno_fixnum_type,KNO_CPP_INT(1));
static lispval make_dtproc(lispval name,lispval server,lispval min_arity,
			   lispval arity,lispval minsock,lispval maxsock,
			   lispval initsock)
{
  lispval result;
  if (VOIDP(min_arity))
    result = kno_make_dtproc(SYM_NAME(name),CSTRING(server),1,-1,-1,
			     FIX2INT(minsock),FIX2INT(maxsock),
			     FIX2INT(initsock));
  else if (VOIDP(arity))
    result = kno_make_dtproc
      (SYM_NAME(name),CSTRING(server),
       1,kno_getint(arity),kno_getint(arity),
       FIX2INT(minsock),FIX2INT(maxsock),
       FIX2INT(initsock));
  else result=
	 kno_make_dtproc
	 (SYM_NAME(name),CSTRING(server),1,
	  kno_getint(arity),kno_getint(min_arity),
	  FIX2INT(minsock),FIX2INT(maxsock),
	  FIX2INT(initsock));
  return result;
}

/* Initialization */

KNO_EXPORT void kno_init_dteval_c()
{
  u8_register_source_file(_FILEINFO);

  link_local_cprims();
}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_PRIM("open-dtserver",open_dtserver,2,scheme_module);
  KNO_LINK_VARARGS("dtcall",dtcall,scheme_module);
  KNO_LINK_PRIM("dteval",dteval,2,scheme_module);
  KNO_LINK_PRIM("dtserver-address",dtserver_address,1,scheme_module);
  KNO_LINK_PRIM("dtserver-id",dtserver_id,1,scheme_module);
  KNO_LINK_PRIM("dtserver?",dtserverp,1,scheme_module);

  KNO_LINK_PRIM("dtproc",make_dtproc,7,kno_scheme_module);
}
