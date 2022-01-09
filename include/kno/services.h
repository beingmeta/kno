/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_SERVICES_H
#define KNO_SERVICES_H 1
#ifndef KNO_SERVICES_H_INFO
#define KNO_SERVICES_H_INFO "include/kno/services.h"
#endif

#include "xtypes.h"
#include "apply.h"

KNO_EXPORT u8_condition kno_ConnectionFailed, kno_BadServerResponse;

#define KNO_SERVICE_HEADER				\
  KNO_ANNOTATED_HEADER;					\
  u8_string service_id, service_spec, service_addr;	\
  unsigned int service_bits, struct_size;		\
  lispval spec;						\
  lispval opts;						\
  struct KNO_SERVICE_HANDLERS *handlers

typedef struct KNO_SERVICE {
  KNO_SERVICE_HEADER;}
  KNO_SERVICE;
typedef struct KNO_SERVICE *kno_service;

KNO_EXPORT kno_service kno_open_service(lispval spec,lispval opts);
KNO_EXPORT int kno_service_close(struct KNO_SERVICE *s);
KNO_EXPORT lispval kno_service_apply
(struct KNO_SERVICE *s,lispval op,int n,kno_argvec args);
KNO_EXPORT lispval kno_service_xapply
(struct KNO_SERVICE *s,lispval op,int n,kno_argvec args,lispval opts);
KNO_EXPORT lispval kno_service_call(kno_service s,lispval op,int n,...);
KNO_EXPORT lispval kno_service_xcall(kno_service s,lispval opts,
				     lispval op,int n,...);


/* Netprocs */

typedef struct KNO_NETPROC {
  KNO_FUNCTION_FIELDS;
  lispval netproc_name;
  lispval netproc_opts;
  struct KNO_SERVICE *service;} KNO_NETPROC;
typedef struct KNO_NETPROC *kno_netproc;

KNO_EXPORT lispval kno_make_netproc(kno_service server,u8_string name,
				    int ndcall,int arity,int min_arity,
				    lispval opts);
KNO_EXPORT lispval kno_netproc_apply
(struct KNO_NETPROC *np,int n,kno_argvec args);
KNO_EXPORT lispval kno_netproc_xapply
(struct KNO_NETPROC *np,int n,kno_argvec args,
 lispval opts);


/* Service Handlers */

typedef struct KNO_SERVICE_HANDLERS *kno_service_handler;

typedef kno_service (*net_open_handler)(lispval,kno_service_handler,lispval);
typedef lispval (*net_apply_handler)(kno_service,lispval,int,kno_argvec,lispval);
typedef lispval (*net_control_handler)(kno_service,lispval,int,kno_argvec);
typedef int (*net_close_handler)(struct KNO_SERVICE *);
typedef int (*net_recycler)(struct KNO_SERVICE *);

typedef struct KNO_SERVICE_HANDLERS {
  u8_string service_typename;
  struct KNO_SERVICE_HANDLERS *more_handlers;
  size_t struct_size;
  net_open_handler open;
  net_apply_handler apply;
  net_close_handler close;
  net_control_handler control;
  net_recycler recycle;}
  *kno_service_handlers;

KNO_EXPORT int kno_register_service_handler(kno_service_handler handler);

/* Support for server state */

KNO_EXPORT void kno_set_server_data(lispval data);

#if (U8_USE_TLS)
KNO_EXPORT u8_tld_key _kno_server_data_key;
#define kno_server_data \
  (((lispval)(u8_tld_get(_kno_server_data_key)))||(KNO_FALSE))
#elif (U8_USE__THREAD)
KNO_EXPORT __thread lispval kno_server_data;
#else
KNO_EXPORT lispval kno_server_data;
#endif

#endif
