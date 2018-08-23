/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_DTPROC_H
#define FRAMERD_DTPROC_H 1
#ifndef FRAMERD_DTPROC_H_INFO
#define FRAMERD_DTPROC_H_INFO "include/framerd/dtproc.h"
#endif

#include <libu8/u8netfns.h>

typedef struct FD_DTPROC {
  FD_FUNCTION_FIELDS;
  u8_string dtprocserver; lispval dtprocname;
  struct U8_CONNPOOL *connpool;} FD_DTPROC;
typedef struct FD_DTPROC *fd_dtproc;

FD_EXPORT lispval fd_make_dtproc
  (u8_string name,u8_string server,int ndcall,int arity,int min_arity,
   int minsock,int maxsock,int initsock);

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
