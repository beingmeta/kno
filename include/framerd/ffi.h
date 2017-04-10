/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_FFI_H
#define FRAMERD_FFI_H 1
#ifndef FRAMERD_FFI_H_INFO
#define FRAMERD_FFI_H_INFO "include/framerd/eval.h"
#endif

#include "framerd/apply.h"
#if HAVE_FFI_H
#include <ffi.h>

typedef struct FD_FFI_PROC {
  FD_FUNCTION_FIELDS;
  ffi_cif *ffi_cif; 
  ffi_type *ffi_return_type;
  ffi_type *ffi_argtypes;
  int ffi_uselock;
  void (*ffi_dlsym)(void);
  U8_MUTEX_DECL(ffi_lock);
} FD_FFI_PROC;
typedef struct FD_FFI_PROC *fd_ffi_proc;

FD_EXPORT struct FD_FFI_PROC *fd_make_ffi_proc
  (u8_string name,u8_string filename,int arity,
   ffi_type *return_type,ffi_type *argtypes,
   fdtype *defaults);

FD_EXPORT fdtype fd_ffi_call(struct FD_FUNCTION *fn,int n,fdtype *args);

#endif

#endif /* FRAMERD_FFI_H */

