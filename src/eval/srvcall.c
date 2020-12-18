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

DEFCPRIM("service?",servicep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if it's argument is a dtype server "
	 "object",
	 {"arg",kno_any_type,KNO_VOID})
static lispval servicep(lispval arg)
{
  if (TYPEP(arg,kno_service_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("service-id",service_id,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the ID of a dtype server (the argument "
	 "used to create it)",
	 {"arg",kno_service_type,KNO_VOID})
static lispval service_id(lispval arg)
{
  struct KNO_SERVICE *service = (struct KNO_SERVICE *) arg;
  return kno_mkstring(service->service_id);
}

DEFCPRIM("service-address",service_address,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the address (host/port) of a dtype server",
	 {"arg",kno_service_type,KNO_VOID})
static lispval service_address(lispval arg)
{
  struct KNO_SERVICE *service = (struct KNO_SERVICE *) arg;
  return kno_mkstring(service->service_addr);
}

DEFCPRIM("service/apply",service_apply_prim,
	 KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 ""
	 "**undocumented**",
	 {"srv",kno_service_type,KNO_VOID},
	 {"op",kno_symbol_type,KNO_VOID},
	 {"args",kno_any_type,KNO_FALSE},
	 {"opts",kno_any_type,KNO_FALSE})
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

DEFCPRIMN("service/call",service_call_prim,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	  ""
	  "**undocumented**")
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

DEFCPRIMN("service/xcall",service_xcall_prim,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(3),
	  ""
	  "**undocumented**")
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

DEFCPRIM("open-service",open_service,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 ""
	 "**undocumented**",
	 {"server",kno_string_type,KNO_VOID},
	 {"opts",kno_any_type,KNO_VOID})
static lispval open_service(lispval server,lispval opts)
{
  struct KNO_SERVICE *s = kno_open_service(server,opts);
  if (s)
    return (lispval) s;
  else return KNO_ERROR;
}

/* Making NETPROCs */

DEFCPRIM("netproc",make_netproc,
	 KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
	 ""
	 "**undocumented**",
	 {"server",kno_service_type,KNO_VOID},
	 {"name",kno_symbol_type,KNO_VOID},
	 {"arity",kno_any_type,KNO_VOID},
	 {"min_arity",kno_any_type,KNO_VOID},
	 {"opts",kno_any_type,KNO_FALSE})
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

/* Support for server data */

DEFCPRIM("srv/getconfig",srvconfig_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(0),
	 "**undocumented**",
	 {"prop",kno_any_type,KNO_VOID},
	 {"dflt",kno_any_type,KNO_FALSE})
static lispval srvconfig_prim(lispval prop,lispval dflt)
{
  lispval config = kno_server_data;
  if (KNO_VOIDP(prop))
    return kno_incref(config);
  else return kno_getopt(config,prop,dflt);
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

  KNO_LINK_CPRIM("open-service",open_service,2,scheme_module);
  KNO_LINK_CPRIM("service-address",service_address,1,scheme_module);
  KNO_LINK_CPRIM("service-id",service_id,1,scheme_module);
  KNO_LINK_CPRIM("service?",servicep,1,scheme_module);

  KNO_LINK_CPRIM("service/apply",service_apply_prim,4,scheme_module);
  KNO_LINK_CVARARGS("service/call",service_call_prim,scheme_module);
  KNO_LINK_CVARARGS("service/xcall",service_xcall_prim,scheme_module);

  KNO_LINK_CPRIM("netproc",make_netproc,5,kno_scheme_module);

  KNO_LINK_CPRIM("srv/getconfig",srvconfig_prim,2,kno_scheme_module);

}
