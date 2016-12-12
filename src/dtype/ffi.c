/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"
#include "framerd/support.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>

#if HAVE_FFI_H && HAVE_LIBFFI
#include <ffi.h>

static fdtype ffi_caller(struct FD_FUNCTION *fn,int n,fdtype *args);
static fdtype applymysqlproc(struct FD_FUNCTION *fn,int n,fdtype *args,
                             int reconn)


FD_EXPORT fdtype fd_make_ffi_proc(u8_string name,int arity,ffi_type rtype,
				  ffi_type *argtypes,fdtype *defaults)
{
  struct FD_FFI_PROC *proc=u8_alloc(struct FD_FFI_PROC);
  memset(proc,0,sizeof struct FD_FFI_PROC);
  FD_INIT_CONS(proc,fd_ffi_type);
  ffi_status rv=
    ffi_prep_cif(&(proc->cif), FFI_DEFAULT_ABI, arity,
		 rtype,argtypes);
  if (rv == FFI_OK) {
    proc->name=u8_strdup(name); proc->filename=NULL;
    proc->ndcall=0; proc->xcall=1;
    // Defer arity checking to ffi_caller
    proc->min_arity=0; proc->arity=-1; 
    proc->typeinfo=argtypes; proc->defaults=defaults;
    proc->handler.xcalln=ffi_caller;
    return proc;}
  else {
    return FD_ERROR_VALUE;}
}

static fdtype ffi_caller(struct FD_FUNCTION *fn,int n,fdtype *args)
{
  if (FD_CONS_TYPE(fn)==fd_ffi_type) {
    struct FD_FFI_PROC *proc=(struct FD_FFI_PROC *) fn;
    return FD_VOID;}
  else return fd_err(_("Not an foreign function interface"),
		     "ffi_caller",u8_strdup(fn->name),
		     NULL);
}

/* Generic object methods */

static void recycle_ffi(struct FD_CONS *c)
{
  struct FD_FFI_PROC *proc=(struct FD_FFI_PROC *)c;
  u8_free(ffi->name); 
  u8_free(ffi->argtypes);
  u8_free(ffi->defaults);
  u8_free(ffi);
}

static int unparse_ffi_proc(u8_output out,fdtype x)
{
  struct FD_FFI_PROC *ffi=(struct FD_FFI_PROC *)x;
  u8_printf(out,"#<FFI '%s' #!%llx>",ffi->name,(long long) ffi);
  return 1;
}

/* Initializations */

FD_EXPORT void fd_init_ffi_c()
{
  fd_type_names[fd_ffi_type]="foreign-function";

  fd_unparsers[fd_ffi_type]=unparse_ffi_proc;
  fd_recyclers[fd_ffi_type]=recycle_ffi_proc;

  u8_register_source_file(_FILEINFO);
}

#else /* HAVE_FFI_H && HAVE_LIBFFI */
/* No FFI */
FD_EXPORT void fd_init_ffi_c()
{
  fd_type_names[fd_ffi_type]="foreign-function";

  u8_register_source_file(_FILEINFO);
}
#endif

