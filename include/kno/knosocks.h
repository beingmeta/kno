/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_KNOSOCKS_H
#define KNO_KNOSOCKS_H 1
#ifndef KNO_KNOSOCKS_H_INFO
#define KNO_KNOSOCKS_H_INFO "include/kno/knosocks.h"
#endif

#include "xtypes.h"
#include "services.h"

#include <libu8/u8srvfns.h>
#include <libu8/u8netfns.h>

/* This is for the client-side */
typedef struct KNOSOCKS_SERVICE {
  KNO_SERVICE_HEADER;
  enum KNO_WIRE_PROTOCOL
    { xtype_protocol=0, dtype_protocol=1, unknown_protocol=-1}
    protocol;
  struct XTYPE_REFS xrefs;
  struct U8_CONNPOOL *connpool;}
  KNOSOCKS_SERVICE;
typedef struct KNOSOCKS_SERVICE *knosocks_service;

/* This is for the server-side */

typedef struct KNOSOCKS_SERVER *knosocks_server;
typedef struct KNOSOCKS_CLIENT *knosocks_client;

typedef struct KNOSOCKS_SERVER {
  struct U8_SERVER sockserver;
  struct U8_OUTPUT *server_log;
  lispval server_env, server_opts, server_data, server_fn;
  lispval server_wrapper;
  unsigned char logeval, logtrans, logerrs, logstack;
  unsigned char async, stealsockets, loglevel, stateful;
  enum KNO_WIRE_PROTOCOL server_protocol;
  struct XTYPE_REFS server_xrefs;
  unsigned int server_bits;
  U8_UUID server_uuid;
  u8_string server_password;
  struct U8_XTIME server_started;
  u8_mutex server_lock;} KNOSOCKS_SERVER;

/* This represents a live client connection and its environment. */
typedef struct KNOSOCKS_CLIENT {
  U8_CLIENT_FIELDS;
  struct KNOSOCKS_SERVER *client_server;
  time_t client_started, client_lastlive;
  double client_livetime, client_runtime, client_reqstart;
  unsigned int client_bits;
  struct XTYPE_REFS *client_xrefs;
  struct KNO_STREAM client_stream;
  struct U8_OUTPUT *client_log;
  lispval client_env, client_data;
  U8_UUID client_uuid;} KNOSOCKS_CLIENT;

KNO_EXPORT struct KNOSOCKS_SERVER *new_knosocks_listener
(u8_string serverid,lispval listen,lispval data,lispval env,lispval opts);

KNO_EXPORT knosocks_client knosocks_getclient(void);
KNO_EXPORT knosocks_server knosocks_getserver(void);
KNO_EXPORT void knosocks_setclient(knosocks_client cl);

KNO_EXPORT int knosocks_listen(knosocks_server,lispval);
KNO_EXPORT int knosocks_start(knosocks_server);
KNO_EXPORT int knosocks_shutdown(knosocks_server,double);
KNO_EXPORT void knosocks_recycle(struct KNOSOCKS_SERVER *server,double grace);

KNO_EXPORT lispval knosocks_base_module;

#endif
