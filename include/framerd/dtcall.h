/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2012 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_DTCALL_H
#define FDB_DTCALL_H 1
#ifndef FDB_DTCALL_H_INFO
#define FDB_DTCALL_H_INFO "include/framerd/dtcall.h"
#endif

#include "ptr.h"
#include <libu8/u8netfns.h>
#include <stdarg.h>

FD_EXPORT fdtype fd_dteval(struct U8_CONNPOOL *cp,fdtype expr);
FD_EXPORT fdtype fd_dteval_sock(fdtype expr,u8_socket sock);
FD_EXPORT fdtype fd_dtapply(struct U8_CONNPOOL *cp,int n,fdtype *args);
FD_EXPORT fdtype fd_dtcall(struct U8_CONNPOOL *cp,int n,...);
FD_EXPORT fdtype fd_dtcall_nr(struct U8_CONNPOOL *cp,int n,...);
FD_EXPORT fdtype fd_dtcall_x(struct U8_CONNPOOL *cp,int doeval,int n,...);
FD_EXPORT fdtype fd_dtcall_nrx(struct U8_CONNPOOL *cp,int doeval,int n,...);


#endif
