/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_REMOTE_CALL_H
#define KNO_REMOTE_CALL_H 1
#ifndef KNO_REMOTE_CALL_H_INFO
#define KNO_REMOTE_CALL_H_INFO "include/kno/remote.h"
#endif

#include "ptr.h"
#include "evalserver.h"
#include <libu8/u8netfns.h>
#include <stdarg.h>

KNO_EXPORT lispval kno_neteval(kno_evalserver es,lispval expr);
KNO_EXPORT lispval kno_neteval_sock(lispval expr,u8_socket sock);
KNO_EXPORT lispval kno_remote_apply(kno_evalserver es,int n,lispval *args);
KNO_EXPORT lispval kno_remote_call(kno_evalserver es,int n,...);
KNO_EXPORT lispval kno_remote_call_nr(kno_evalserver es,int n,...);
KNO_EXPORT lispval kno_remote_call_x(kno_evalserver es,int doeval,int n,...);
KNO_EXPORT lispval kno_remote_call_nrx(kno_evalserver es,int doeval,int n,...);


#endif

