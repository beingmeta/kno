/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_DTPROC_H
#define KNO_DTPROC_H 1
#ifndef KNO_DTPROC_H_INFO
#define KNO_DTPROC_H_INFO "include/kno/dtproc.h"
#endif

#include <libu8/u8netfns.h>

typedef struct KNO_DTPROC {
  KNO_FUNCTION_FIELDS;
  u8_string dtprocserver; lispval dtprocname;
  struct U8_CONNPOOL *connpool;} KNO_DTPROC;
typedef struct KNO_DTPROC *kno_dtproc;

KNO_EXPORT lispval kno_make_dtproc
  (u8_string name,u8_string server,int ndcall,int arity,int min_arity,
   int minsock,int maxsock,int initsock);

#endif

