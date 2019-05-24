/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_FFI_H
#define KNO_FFI_H 1
#ifndef KNO_FFI_H_INFO
#define KNO_FFI_H_INFO "include/kno/eval.h"
#endif

#include "kno/apply.h"
#if HAVE_FFI_H
#include <ffi.h>

typedef struct KNO_FFI_PROC {
  KNO_FUNCTION_FIELDS;
  ffi_cif ffi_interface;
  ffi_type *ffi_return_type;
  ffi_type **ffi_argtypes;
  lispval ffi_return_spec, *ffi_argspecs;
  int ffi_uselock;
  void (*ffi_dlsym)(void);
  U8_MUTEX_DECL(ffi_lock);
} KNO_FFI_PROC;
typedef struct KNO_FFI_PROC *kno_ffi_proc;

KNO_EXPORT struct KNO_FFI_PROC *kno_make_ffi_proc
  (u8_string name,u8_string filename,int arity,
   lispval return_type,lispval *argspecs);

KNO_EXPORT lispval kno_ffi_call(struct KNO_FUNCTION *fn,int n,lispval *args);

#endif

#endif /* KNO_FFI_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/