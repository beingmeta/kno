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
#include "kno/cprims.h"
#include "kno/xtypes.h"
#include "kno/dtypeio.h"
#include "kno/dtcall.h"
#include "kno/netproc.h"
#include "kno/evalserver.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>


/* Remote evaluation */

static u8_condition ServerUndefined=_("Server unconfigured");

KNO_EXPORT lispval kno_open_evalserver(u8_string server,
				       enum EVALSERVER_PROTOCOL protocol,
				       int minsock,int maxsock,int initsock,ssize_t bufsiz)
{
  return kno_init_evalserver(NULL,server,protocol,
			     minsock,maxslock,initsock,bufsiz);
}

DEFPRIM1("evalserver?",evalserverp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if it's argument is a dtype server "
	 "object",
	 kno_any_type,KNO_VOID);
static lispval evalserverp(lispval arg)
{
  if (TYPEP(arg,kno_evalserver_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("evalserver-id",evalserver_id,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the ID of a dtype server (the argument "
	 "used to create it)",
	 kno_evalserver_type,KNO_VOID);
static lispval evalserver_id(lispval arg)
{
  struct KNO_EVALSERVER *evalserver = (struct KNO_EVALSERVER *) arg;
  return kno_mkstring(evalserver->evalserverid);
}

DEFPRIM1("evalserver-address",evalserver_address,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the address (host/port) of a dtype server",
	 kno_evalserver_type,KNO_VOID);
static lispval evalserver_address(lispval arg)
{
  struct KNO_EVALSERVER *evalserver = (struct KNO_EVALSERVER *) arg;
  return kno_mkstring(evalserver->evalserver_addr);
}

DEFPRIM2("neteval",neteval,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(NETEVAL *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval neteval(lispval server,lispval expr)
{
  if (TYPEP(server,kno_evalserver_type))  {
    struct KNO_EVALSERVER *evalserver=
      kno_consptr(kno_stream_erver,server,kno_evalserver_type);
    return kno_neteval(evalserver->connpool,expr);}
  else if (STRINGP(server)) {
    lispval s = kno_open_evalserver(CSTRING(server),-1);
    if (KNO_ABORTED(s)) return s;
    else {
      lispval result = kno_neteval(((kno_stream_erver)s)->connpool,expr);
      kno_decref(s);
      return result;}}
  else return kno_type_error(_("server"),"neteval",server);
}

DEFPRIM("dtcall",dtcall,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(DTCALL *arg0* *arg1* *args...*)` **undocumented**");
static lispval dtcall(int n,kno_argvec args)
{
  lispval server; lispval request = NIL, result; int i = n-1;
  if (n<2) return kno_err(kno_SyntaxError,"dtcall",NULL,VOID);
  if (TYPEP(args[0],kno_evalserver_type))
    server = kno_incref(args[0]);
  else if (STRINGP(args[0])) server = kno_open_evalserver(CSTRING(args[0]),-1);
  else return kno_type_error(_("server"),"eval/dtcall",args[0]);
  if (KNO_ABORTED(server)) return server;
  while (i>=1) {
    lispval param = args[i];
    if ((i>1) && ((SYMBOLP(param)) || (PAIRP(param))))
      request = kno_conspair(kno_make_list(2,KNOSYM_QUOTE,param),request);
    else request = kno_conspair(param,request);
    kno_incref(param); i--;}
  result = kno_neteval(((kno_stream_erver)server)->connpool,request);
  kno_decref(request);
  kno_decref(server);
  return result;
}

DEFPRIM2("open-evalserver",open_evalserver,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(OPEN-EVALSERVER *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval open_evalserver(lispval server,lispval bufsiz)
{
  return kno_open_evalserver(CSTRING(server),
			   ((VOIDP(bufsiz)) ? (-1) :
			    (FIX2INT(bufsiz))));
}

/* Making NETPROCs */

DEFPRIM7("netproc",make_netproc,KNO_MAX_ARGS(7)|KNO_MIN_ARGS(2),
	 "`(NETPROC *arg0* *arg1* [*arg2*] [*arg3*] [*arg4*] [*arg5*] [*arg6*])` **undocumented**",
	 kno_symbol_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(2),kno_fixnum_type,KNO_CPP_INT(4),
	 kno_fixnum_type,KNO_CPP_INT(1));
static lispval make_netproc(lispval name,lispval server,lispval min_arity,
			   lispval arity,lispval minsock,lispval maxsock,
			   lispval initsock)
{
  lispval result;
  if (VOIDP(min_arity))
    result = kno_make_netproc(SYM_NAME(name),CSTRING(server),1,-1,-1,
			     FIX2INT(minsock),FIX2INT(maxsock),
			     FIX2INT(initsock));
  else if (VOIDP(arity))
    result = kno_make_netproc
      (SYM_NAME(name),CSTRING(server),
       1,kno_getint(arity),kno_getint(arity),
       FIX2INT(minsock),FIX2INT(maxsock),
       FIX2INT(initsock));
  else result=
	 kno_make_netproc
	 (SYM_NAME(name),CSTRING(server),1,
	  kno_getint(arity),kno_getint(min_arity),
	  FIX2INT(minsock),FIX2INT(maxsock),
	  FIX2INT(initsock));
  return result;
}

/* Initialization */

KNO_EXPORT void kno_init_neteval_c()
{
  u8_register_source_file(_FILEINFO);

  link_local_cprims();
}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_PRIM("open-evalserver",open_evalserver,2,scheme_module);
  KNO_LINK_VARARGS("dtcall",dtcall,scheme_module);
  KNO_LINK_PRIM("neteval",neteval,2,scheme_module);
  KNO_LINK_PRIM("evalserver-address",evalserver_address,1,scheme_module);
  KNO_LINK_PRIM("evalserver-id",evalserver_id,1,scheme_module);
  KNO_LINK_PRIM("evalserver?",evalserverp,1,scheme_module);

  KNO_LINK_PRIM("netproc",make_netproc,7,kno_scheme_module);
}
