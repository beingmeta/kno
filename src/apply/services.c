/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
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
#include "kno/services.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>

static struct KNO_SERVICE_HANDLERS *service_handlers = NULL;
static u8_mutex service_handlers_lock;

u8_condition kno_ConnectionFailed=_("Connection to server failed");
u8_condition kno_ServerReconnect=_("Resetting server connection");

/* Remote evaluation */

static u8_condition UnconfiguredServer=_("Server unconfigured");

KNO_EXPORT kno_service kno_open_service(lispval spec,lispval opts)
{
  if (KNO_SYMBOLP(spec)) {
    lispval config_val = kno_config_get(KNO_SYMBOL_NAME(spec));
    if (KNO_CONSP(config_val)) {
      kno_service result = kno_open_service(config_val,opts);
      kno_decref(config_val);
      return result;}
    kno_seterr(UnconfiguredServer,"kno_open_source",
	       KNO_SYMBOL_NAME(spec),KNO_VOID);
    return NULL;}
  struct KNO_SERVICE_HANDLERS *scan = service_handlers;
  while (scan) {
    kno_service s = scan->open(spec,scan,opts);
    if (s) return s;
    else scan = scan->more_handlers;}
  return NULL;
}

static int unparse_service(u8_output out,lispval x)
{
  struct KNO_SERVICE *s = kno_consptr(kno_service,x,kno_service_type);
  struct KNO_SERVICE_HANDLERS *h = s->handlers;
  u8_string typename = ( (h) && (h->service_typename) ) ?
    (h->service_typename) :
    (U8S("corrupted"));
  u8_printf(out,"#<!SERVICE/%s %s",typename,s->service_id);
  if (s->service_spec) {
    if (strcmp(s->service_id,s->service_spec))
      u8_printf(out,"=%s",s->service_spec);}
  if (s->service_addr)
    u8_printf(out,"=%s",s->service_addr);
  else NO_ELSE;
  u8_printf(out," #!%llx>",(unsigned long long)x);
  return 1;
}

static void recycle_service(struct KNO_RAW_CONS *c)
{
  struct KNO_SERVICE *s = (kno_service)c;
  struct KNO_SERVICE_HANDLERS *h = s->handlers;
  if ( (h) && (h->recycle) ) h->recycle(s);
  
  if (!(KNO_STATIC_CONSP(s))) u8_free(s);
}

KNO_EXPORT int kno_service_close(struct KNO_SERVICE *s)
{
  return s->handlers->close(s);
}

KNO_EXPORT lispval kno_service_apply
(struct KNO_SERVICE *s,lispval op,int n,kno_argvec args)
{
  return s->handlers->apply(s,op,n,args,KNO_FALSE);
}

KNO_EXPORT lispval kno_service_xapply
(struct KNO_SERVICE *s,lispval op,int n,kno_argvec args,lispval opts)
{
  return s->handlers->apply(s,op,n,args,opts);
}

KNO_EXPORT int kno_register_service_handler(kno_service_handler handler)
{
  u8_lock_mutex(&service_handlers_lock); {
    kno_service_handlers scan = service_handlers;
    while (scan) {
      if (scan == handler) {
	u8_unlock_mutex(&service_handlers_lock);
	return 0;}
      else scan = scan->more_handlers;}
    handler->more_handlers=service_handlers;
    service_handlers = handler;
    u8_unlock_mutex(&service_handlers_lock);}
  return 1;
}

/* Apply/call */


KNO_EXPORT lispval kno_service_call(kno_service s,lispval op,int n,...)
{
  int i = 0; va_list arglist;
  lispval args[n], result = KNO_VOID;
  va_start(arglist,n);
  while (i<n) args[i++]=va_arg(arglist,lispval);
  result = kno_service_apply(s,op,n,args);
  return result;
}

KNO_EXPORT lispval kno_service_xcall(kno_service s,lispval opts,
				     lispval op,int n,...)
{
  int i = 0; va_list arglist;
  lispval args[n], result = KNO_VOID;
  va_start(arglist,n);
  while (i<n) args[i++]=va_arg(arglist,lispval);
  result = kno_service_xapply(s,op,n,args,opts);
  return result;
}

/* Netprocs */

KNO_EXPORT lispval kno_make_netproc(kno_service server,u8_string name,
				    int ndcall,int arity,int min_arity,
				    lispval opts)
{
  struct KNO_NETPROC *f = u8_alloc(struct KNO_NETPROC);
  KNO_INIT_CONS(f,kno_rpc_type);
  f->fcn_name = u8_mkstring("%s@%s",name,server->service_id);
  f->fcn_filename = u8_strdup(server->service_id);
  f->netproc_name = kno_intern(name);
  f->netproc_opts = kno_incref(opts);
  kno_incref((lispval)server);
  f->service = server;
  if (ndcall)
    f->fcn_call |= KNO_CALL_NDCALL;
  f->fcn_min_arity = min_arity;
  f->fcn_call_width = f->fcn_arity = arity;
  f->fcn_arginfo_len = 0;
  f->fcn_argnames = NULL;
  f->fcn_call |= KNO_CALL_XCALL;
  f->fcn_handler.fnptr = NULL;
  return LISP_CONS(f);
}

static int unparse_netproc(u8_output out,lispval x)
{
  struct KNO_NETPROC *f = kno_consptr(kno_netproc,x,kno_rpc_type);
  u8_printf(out,"#<!NETPROC %s using %s>",f->fcn_name,f->service->service_addr);
  return 1;
}

static void recycle_netproc(struct KNO_RAW_CONS *c)
{
  struct KNO_NETPROC *f = (kno_netproc)c;
  kno_decref((lispval)(f->service));
  u8_free(f->fcn_name);
  u8_free(f->fcn_filename);
  /* close eval server */
  if (!(KNO_STATIC_CONSP(f))) u8_free(f);
}

KNO_EXPORT
lispval kno_netproc_apply(struct KNO_NETPROC *np,int n,kno_argvec args)
{
  kno_service s = np->service;
  return s->handlers->apply(s,np->netproc_name,n,args,np->netproc_opts);
}

KNO_EXPORT
lispval kno_netproc_xapply(struct KNO_NETPROC *np,int n,kno_argvec args,
			   lispval opts)
{
  kno_service s = np->service;
  lispval name = np->netproc_name;
  lispval use_opts = KNO_VOID;
  int free_opts = 0;
  if (!(KNO_TABLEP(opts))) use_opts = np->netproc_opts;
  else if (!(KNO_TABLEP(opts)))
    use_opts = opts;
  else {
    free_opts = 1;
    use_opts = kno_make_pair(opts,np->netproc_opts);}
  lispval result = s->handlers->apply(s,name,n,args,use_opts);
  if (free_opts) kno_decref(use_opts);
  return result;
}

/* Support for servers */

#if (KNO_USE_TLS)
u8_tld_key _kno_server_data_key;
#elif (U8_USE__THREAD)
__thread lispval kno_server_data=KNO_FALSE;
#else
lispval kno_server_data=KNO_FALSE;
#endif

#if (KNO_USE_TLS)
KNO_EXPORT void kno_set_server_data(lispval data)
{
  lispval cur = (lispval) u8_tld_get(_kno_server_data_key);
  if (cur == data) return;
  kno_incref(data);
  u8_tld_set(_kno_server_data_key,(void *)data);
  if (cur) kno_decref(cur);
}
#else
KNO_EXPORT void kno_set_server_data(lispval data)
{
  lispval cur = kno_server_data;
  if (cur == data) return;
  kno_incref(data);
  kno_server_data = data;
  if (cur) kno_decref(cur);
}
#endif


/* Initialization */

KNO_EXPORT void kno_init_services_c()
{
  u8_register_source_file(_FILEINFO);
  u8_register_source_file(KNO_SERVICES_H_INFO);

  u8_init_mutex(&service_handlers_lock);

  kno_type_names[kno_service_type]=_("service");

  kno_type_names[kno_rpc_type]=_("netproc");
  kno_applyfns[kno_rpc_type]=(kno_applyfn)kno_netproc_apply;
  kno_isfunctionp[kno_rpc_type]=1;

  kno_unparsers[kno_service_type]=unparse_service;
  kno_recyclers[kno_service_type]=recycle_service;

  kno_tablefns[kno_service_type]=kno_annotated_tablefns;

  kno_unparsers[kno_rpc_type]=unparse_netproc;
  kno_recyclers[kno_rpc_type]=recycle_netproc;

#if ((KNO_THREADS_ENABLED)&&(KNO_USE_TLS))
  u8_new_threadkey(&_kno_server_data_key,NULL);
#endif


}

