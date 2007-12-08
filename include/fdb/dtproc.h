/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_DTPROC_H
#define FDB_DTPROC_H 1
#define FDB_DTPROC_H_VERSION "$Id$"

#include <libu8/u8netfns.h>

typedef struct FD_DTPROC {
  FD_FUNCTION_FIELDS;
  u8_string server; fdtype fcnsym;
  struct U8_CONNPOOL *connpool;} FD_DTPROC;
typedef struct FD_DTPROC *fd_dtproc;

FD_EXPORT fdtype fd_make_dtproc
  (u8_string name,u8_string server,int ndcall,int arity,int min_arity,
   int minsock,int maxsock,int initsock);

#endif
