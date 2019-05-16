/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_DTCALL_H
#define KNO_DTCALL_H 1
#ifndef KNO_DTCALL_H_INFO
#define KNO_DTCALL_H_INFO "include/kno/dtcall.h"
#endif

#include "ptr.h"
#include <libu8/u8netfns.h>
#include <stdarg.h>

KNO_EXPORT lispval kno_dteval(struct U8_CONNPOOL *cp,lispval expr);
KNO_EXPORT lispval kno_dteval_sync(struct U8_CONNPOOL *cp,lispval expr);
KNO_EXPORT lispval kno_dteval_async(struct U8_CONNPOOL *cp,lispval expr);
KNO_EXPORT lispval kno_dteval_sock(lispval expr,u8_socket sock);
KNO_EXPORT lispval kno_dtapply(struct U8_CONNPOOL *cp,int n,lispval *args);
KNO_EXPORT lispval kno_dtcall(struct U8_CONNPOOL *cp,int n,...);
KNO_EXPORT lispval kno_dtcall_nr(struct U8_CONNPOOL *cp,int n,...);
KNO_EXPORT lispval kno_dtcall_x(struct U8_CONNPOOL *cp,int doeval,int n,...);
KNO_EXPORT lispval kno_dtcall_nrx(struct U8_CONNPOOL *cp,int doeval,int n,...);


#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
