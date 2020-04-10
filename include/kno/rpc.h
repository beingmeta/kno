/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_RPC_H
#define KNO_RPC_H 1
#ifndef KNO_RPC_H_INFO
#define KNO_RPC_H_INFO "include/kno/rpc.h"
#endif

#include "ptr.h"
#include "apply.h"
#include <libu8/u8netfns.h>
#include <stdarg.h>

typedef struct KNO_RPC {
  KNO_CONS_HEADER;
  u8_string rpc_type;
  u8_string rpc_spec, rpc_addr, rpc_id;
  lispval rpc_opts;
  lispval rpc_procs;
  struct KNO_RPC_HANDLERS *rpc_handlers;}
  KNO_RPC;
typedef struct KNO_RPC *kno_rpc;

#define KNO_RPC_OBJECT_HEADER			\
  KNO_CONS_HEADER;				\
  u8_string rpc_type;				\
  u8_string rpc_spec, rpc_addr, rpc_id;		\
  lispval rpc_opts;				\
  lispval rpc_procs;				\
  struct KNO_RPC_HANDLERS *rpc_handlers

typedef lispval (*kno_rpc_callback)(struct KNO_RPC *pt,lispval e,lispval obj);

typedef struct KNO_RPC_HANDLERS {
  u8_string rpc_type_name;
  lispval (*rpc_apply)(struct KNO_RPC *,lispval method,int n,lispval *args);
  /*
  int (*rpc_start)(struct KNO_RPC *,lispval opts,
		   lispval method,int n,lispval *args,
		   uuid_t *,lispval *result,
		   kno_rpc_callback callback);
  lispval (*rpc_status)(struct KNO_RPC *,uuid_t uuid,lispval attr);
  lispval (*rpc_wait)(struct KNO_RPC *,uuid_t uuid);
  int (*rpc_cancel)(struct KNO_RPC *,uuid_t uuid);
  */
} *kno_rpc_handlers;

typedef struct KNO_RPROC {
  KNO_FUNCTION_FIELDS;
  struct KNO_RPC *rproc_rpc;
  lispval rproc_opts;
  lispval rpc_name;} KNO_RPROC;
typedef struct KNO_RPROC *kno_rproc;

KNO_EXPORT int kno_register_rpc(struct KNO_RPC *rpc);
KNO_EXPORT int kno_deregister_rpc(struct KNO_RPC *rpc);
KNO_EXPORT struct KNO_RPC *kno_lookup_rpc(u8_string spec);
KNO_EXPORT lispval kno_rpc_apply(kno_rpc pt,lispval m,int n,lispval *args);
KNO_EXPORT lispval kno_rpc_call(kno_rpc pt,lispval method,int n,...);
KNO_EXPORT lispval kno_rpc_ccall(kno_rpc pt,u8_string m,u8_string args,...);

#if KNO_SOURCE
typedef struct KNO_RPC RPC;
typedef struct KNO_RPC_HANDLERS RPC_HANDLERS;
typedef kno_rpc_handlers rpc_handlers;
typedef kno_rpc rpc;
#endif

#endif /* ndef KNO_RPC_H */


