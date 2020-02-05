/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_EVALSERVER_H
#define KNO_EVALSERVER_H 1
#ifndef KNO_EVALSERVER_H_INFO
#define KNO_EVALSERVER_H_INFO "include/kno/evalserver.h"
#endif

#include "xtypes.h"

/* DT servers */

typedef struct KNO_EVALSERVER {
  KNO_CONS_HEADER;
  u8_string evalserverid, evalserver_addr;
  enum EVALSERVER_PROTOCOL { dtype_protocol=0, xtype_protocol=1 } evalserver_protocol;
  enum EVALSERVER_TRANSPORT { simple_socket=0 } evalserver_transport;
  struct XTYPE_REFS refs;
  union {
    struct U8_CONNPOOL *connpool;}
    evalserver_connection;} 
  KNO_EVALSERVER;
typedef struct KNO_EVALSERVER *kno_evalserver;

KNO_EXPORT lispval kno_init_evalserver
(struct KNO_EVALSERVER *evalserver,
 u8_string server,enum EVALSERVER_PROTOCOL protocol,
 int minsock,int maxsock,int initsock,ssize_t bufsiz);
  
#endif
