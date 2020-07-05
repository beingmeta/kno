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
#include "kno/services.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>


/* Remote evaluation */

DEFPRIM1("service?",servicep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if it's argument is a dtype server "
	 "object",
	 kno_any_type,KNO_VOID);
static lispval servicep(lispval arg)
{
  if (TYPEP(arg,kno_service_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("service-id",service_id,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the ID of a dtype server (the argument "
	 "used to create it)",
	 kno_service_type,KNO_VOID);
static lispval service_id(lispval arg)
{
  struct KNO_SERVICE *service = (struct KNO_SERVICE *) arg;
  return kno_mkstring(service->service_id);
}

DEFPRIM1("service-address",service_address,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the address (host/port) of a dtype server",
	 kno_service_type,KNO_VOID);
static lispval service_address(lispval arg)
{
  struct KNO_SERVICE *service = (struct KNO_SERVICE *) arg;
  return kno_mkstring(service->service_addr);
}

DEFPRIM4("service/apply",service_apply_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(SERVICE/APPLY *service* *op* [*args*] [*callopts*])` **undocumented**",
	 kno_service_type,KNO_VOID,
	 kno_symbol_type,KNO_VOID,
	 kno_any_type,KNO_FALSE,
	 kno_any_type,KNO_FALSE);
static lispval service_apply_prim
(lispval srv,lispval op,lispval args,lispval opts)
{
  struct KNO_SERVICE *service = (kno_service) srv;
  if (KNO_VECTORP(args))
    return kno_service_xapply(service,op,
			     KNO_VECTOR_LENGTH(args),
			     KNO_VECTOR_ELTS(args),
			     opts);
  else if (KNO_PAIRP(args)) {
    int i = 0, len = kno_list_length(args);
    lispval argvec[len], scan = args;
    while (KNO_PAIRP(scan)) {
      lispval elt = KNO_CAR(scan);
      argvec[i++] = elt;
      scan = KNO_CDR(scan);}
    return kno_service_xapply(service,op,len,argvec,opts);}
  else return kno_type_error(_("sequence"),"service/apply",args);
}

DEFPRIM("service/call",service_call_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(SERVICE/CALL *service* *op* *args...*)` **undocumented**");
static lispval service_call_prim(int n,kno_argvec args)
{
  lispval srv = args[0];
  lispval op =  args[1];
  if (!(KNO_TYPEP(srv,kno_service_type)))
    return kno_type_error(_("service"),"service/call",srv);
  else if (!(KNO_SYMBOLP(op)))
    return kno_type_error(_("serviceop(symbol)"),"service/call",op);
  struct KNO_SERVICE *service = (kno_service) srv;
  return kno_service_apply(service,op,n-2,args+2);
}

DEFPRIM("service/xcall",service_xcall_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(3),
	"`(SERVICE/CALL *service* *opts* *op* *args...*)` **undocumented**");
static lispval service_xcall_prim(int n,kno_argvec args)
{
  lispval srv  =  args[0];
  lispval opts =  args[1];
  lispval op   =  args[2];
  if (!(KNO_TYPEP(srv,kno_service_type)))
    return kno_type_error(_("service"),"service/xcall",srv);
  else if (!(KNO_SYMBOLP(op)))
    return kno_type_error(_("serviceop(symbol)"),"service/xcall",op);
  struct KNO_SERVICE *service = (kno_service) srv;
  return kno_service_xapply(service,op,n-2,args+2,opts);
}

DEFPRIM2("open-service",open_service,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(OPEN-SERVICE *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval open_service(lispval server,lispval opts)
{
  struct KNO_SERVICE *s = kno_open_service(server,opts);
  if (s)
    return (lispval) s;
  else return KNO_ERROR;
}

/* Making NETPROCs */

DEFPRIM5("netproc",make_netproc,KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
	 "`(NETPROC *arg0* *arg1* [*arg2*] [*arg3*] [*arg4*] [*opts*])` **undocumented**",
	 kno_service_type,KNO_VOID,kno_symbol_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_FALSE);
static lispval make_netproc(lispval server,lispval name,
			    lispval arity,lispval min_arity,
			    lispval opts)
{
  lispval result;
  if (VOIDP(min_arity))
    result = kno_make_netproc((kno_service)server,SYM_NAME(name),1,-1,-1,opts);
  else if (VOIDP(arity))
    result = kno_make_netproc
      ((kno_service)server,SYM_NAME(name),
       1,kno_getint(arity),kno_getint(arity),
       opts);
  else result=
	 kno_make_netproc
	 ((kno_service)server,SYM_NAME(name),1,
	  kno_getint(arity),kno_getint(min_arity),
	  opts);
  return result;
}

/* Initialization */

KNO_EXPORT void kno_init_srvcall_c()
{
  u8_register_source_file(_FILEINFO);

  link_local_cprims();
}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_PRIM("open-service",open_service,2,scheme_module);
  KNO_LINK_PRIM("service-address",service_address,1,scheme_module);
  KNO_LINK_PRIM("service-id",service_id,1,scheme_module);
  KNO_LINK_PRIM("service?",servicep,1,scheme_module);

  KNO_LINK_PRIM("service/apply",service_apply_prim,4,scheme_module);
  KNO_LINK_VARARGS("service/call",service_call_prim,scheme_module);
  KNO_LINK_VARARGS("service/xcall",service_xcall_prim,scheme_module);

  KNO_LINK_PRIM("netproc",make_netproc,5,kno_scheme_module);
}
