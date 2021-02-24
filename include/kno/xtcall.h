/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_XTCALL_H
#define KNO_XTCALL_H 1
#ifndef KNO_XTCALL_H_INFO
#define KNO_XTCALL_H_INFO "include/kno/xtcall.h"
#endif

#include "ptr.h"
#include "xtypes.h"
#include <libu8/u8netfns.h>
#include <stdarg.h>

KNO_EXPORT lispval kno_xteval(struct U8_CONNPOOL *cp,lispval expr,xtype_refs refs);
KNO_EXPORT lispval kno_sock_xteval(u8_socket sock,lispval expr,xtype_refs refs);
KNO_EXPORT lispval kno_xtcall(struct U8_CONNPOOL *cp,xtype_refs refs,int flags,
			      int n,lispval *args);
KNO_EXPORT lispval kno_xtcall_n(struct U8_CONNPOOL *cp,xtype_refs refs,int flags,
				int n,...);

#define XTCALL_FREE_ARGS 1
#define XTCALL_EVAL_ARGS 2


#endif

