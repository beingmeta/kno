/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_XTCALL_H
#define KNO_XTCALL_H 1
#ifndef KNO_XTCALL_H_INFO
#define KNO_XTCALL_H_INFO "include/kno/xtcall.h"
#endif

#include "ptr.h"
#include <libu8/u8netfns.h>
#include <stdarg.h>

KNO_EXPORT lispval kno_xteval(struct U8_CONNPOOL *cp,xtype_refs refs,lispval expr);
KNO_EXPORT lispval kno_xteval_sync(struct U8_CONNPOOL *cp,xtype_refs refs,lispval expr);
KNO_EXPORT lispval kno_xteval_async(struct U8_CONNPOOL *cp,xtype_refs refs,lispval expr);
KNO_EXPORT lispval kno_xteval_sock(lispval expr,xtype_refs refs,u8_socket sock);
KNO_EXPORT lispval kno_xtapply(struct U8_CONNPOOL *cp,xtype_refs refs,int n,lispval *args);
KNO_EXPORT lispval kno_xtcall(struct U8_CONNPOOL *cp,xtype_refs refs,int n,...);
KNO_EXPORT lispval kno_xtcall_nr(struct U8_CONNPOOL *cp,xtype_refs refs,int n,...);
KNO_EXPORT lispval kno_xtcall_x(struct U8_CONNPOOL *cp,xtype_refs refs,int doeval,int n,...);
KNO_EXPORT lispval kno_xtcall_nrx(struct U8_CONNPOOL *cp,xtype_refs refs,int doeval,int n,...);


#endif

