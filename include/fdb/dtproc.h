/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_DTPROC_H
#define FDB_DTPROC_H 1
#define FDB_DTPROC_H_VERSION "$Id: dtproc.h,v 1.3 2006/01/26 14:44:32 haase Exp $"

typedef struct FD_DTPROC {
  FD_FUNCTION_FIELDS;
  u8_string server; fdtype fcnsym;
#if FD_THREADS_ENABLED
  u8_mutex lock;
#endif
  struct FD_DTYPE_STREAM stream;} FD_DTPROC;
typedef struct FD_DTPROC *fd_dtproc;

FD_EXPORT fdtype fd_make_dtproc
  (u8_string name,u8_string server,int ndcall,int arity,int min_arity);

#endif
