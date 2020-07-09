/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_NETPROC_H
#define KNO_NETPROC_H 1
#ifndef KNO_NETPROC_H_INFO
#define KNO_NETPROC_H_INFO "include/kno/netproc.h"
#endif

#include <libu8/u8netfns.h>
#include <kno/services.h>

typedef struct KNO_NETPROC {
  KNO_FUNCTION_FIELDS;
  lispval netproc_name;
  lispval netproc_opts;
  struct KNO_SERVICE *service;} KNO_NETPROC;
typedef struct KNO_NETPROC *kno_netproc;

KNO_EXPORT lispval kno_make_netproc(kno_service server,u8_string name,
				    int ndcall,int arity,int min_arity,
				    lispval opts);

#endif

