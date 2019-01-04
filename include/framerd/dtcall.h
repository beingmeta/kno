/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_DTCALL_H
#define FRAMERD_DTCALL_H 1
#ifndef FRAMERD_DTCALL_H_INFO
#define FRAMERD_DTCALL_H_INFO "include/framerd/dtcall.h"
#endif

#include "ptr.h"
#include <libu8/u8netfns.h>
#include <stdarg.h>

FD_EXPORT lispval fd_dteval(struct U8_CONNPOOL *cp,lispval expr);
FD_EXPORT lispval fd_dteval_sync(struct U8_CONNPOOL *cp,lispval expr);
FD_EXPORT lispval fd_dteval_async(struct U8_CONNPOOL *cp,lispval expr);
FD_EXPORT lispval fd_dteval_sock(lispval expr,u8_socket sock);
FD_EXPORT lispval fd_dtapply(struct U8_CONNPOOL *cp,int n,lispval *args);
FD_EXPORT lispval fd_dtcall(struct U8_CONNPOOL *cp,int n,...);
FD_EXPORT lispval fd_dtcall_nr(struct U8_CONNPOOL *cp,int n,...);
FD_EXPORT lispval fd_dtcall_x(struct U8_CONNPOOL *cp,int doeval,int n,...);
FD_EXPORT lispval fd_dtcall_nrx(struct U8_CONNPOOL *cp,int doeval,int n,...);


#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
