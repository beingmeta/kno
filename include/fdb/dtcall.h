/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_DTCALL_H
#define FDB_DTCALL_H 1
#define FDB_DTCALL_H_VERSION "$Id:$"

#include "ptr.h"
#include <stdarg.h>

FD_EXPORT fd_ptr_type fd_dtserver_type;

typedef struct FD_DTSERVER {
  FD_CONS_HEADER;
  u8_string server, addr;
  struct FD_DTYPE_STREAM stream;
#if FD_THREADS_ENABLED
  u8_mutex lock;
#endif
} FD_DTSERVER;
typedef struct FD_DTSERVER *fd_dtserver;

FD_EXPORT fdtype fd_open_dtserver(u8_string server,int bufsiz);
FD_EXPORT fdtype fd_dteval(fd_dtserver srv,fdtype expr);
FD_EXPORT fdtype fd_dtcall(fd_dtserver srv,u8_string,int,...);


#endif
