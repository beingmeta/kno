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
#include "framerd/apply.h"
#include "framerd/ffi.h"
#include "framerd/eval.h"
#include "framerd/numbers.h"
#include "framerd/support.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>
#include <dlfcn.h>

u8_condition fd_ffi_BadTypeinfo=_("Bad FFI type info");
u8_condition fd_ffi_BadABI=_("Bad FFI ABI value");
u8_condition fd_ffi_FFIError=_("Unknown libffi error");

#if HAVE_FFI_H && HAVE_LIBFFI
#include <ffi.h>

#ifndef u8_xfree
#define u8_xfree(ptr) if (ptr) free((char *)ptr); else ptr=ptr;
#endif

static fdtype ffi_caller(struct FD_FUNCTION *fn,int n,fdtype *args);

/** Change notes: 

    Make fd_make_ffi_proc take fdtypes for the return type and
    argtypes, and convert them into ffi_types to be passed in.
**/

FD_EXPORT struct FD_FFI_PROC *fd_make_ffi_proc
 (u8_string name,int arity,
  ffi_type *return_type,ffi_type *argtypes,
  fdtype *defaults)
{
  ffi_cif *cif = u8_zalloc_for("fd_make_ffi_proc",ffi_cif);
  ffi_status rv=
    ffi_prep_cif(cif, FFI_DEFAULT_ABI, arity,
		 return_type,&argtypes);
  if (rv == FFI_OK) {
    struct FD_FFI_PROC *proc=
      u8_zalloc_for("fd_make_ffi_proc",struct FD_FFI_PROC);
    FD_INIT_CONS(proc,fd_ffi_type);
    /* Set up generic function fields */
    proc->fcn_name=u8_strdup(name);
    proc->fcn_filename=NULL;
    proc->fd_ffi_arity=arity;
    proc->fd_ffi_defaults=defaults;
    proc->fd_ffi_rtype=return_type;
    proc->fd_ffi_argtypes=argtypes;
    proc->fcn_ndcall=0;
    proc->fcn_xcall=1;
    // Defer arity checking to fd_ffi_call
    proc->fcn_min_arity=0;
    proc->fcn_arity=-1;
    proc->fcn_handler.xcalln=fd_ffi_call;
    proc->fcn_dlsym=dlsym(RTLD_DEFAULT,name);
    return proc;}
  else {
    u8_free(cif);
    if (rv == FFI_BAD_TYPEDEF)
      u8_seterr(fd_ffi_BadTypeinfo,"fd_make_ffi_proc",NULL);
    else if (rv == FFI_BAD_ABI)
      u8_seterr(fd_ffi_BadABI,"fd_make_ffi_proc",NULL);
    else u8_seterr(fd_ffi_FFIError,"fd_make_ffi_proc",NULL);
    return NULL;}
}

FD_EXPORT fdtype fd_ffi_call(struct FD_FUNCTION *fn,int n,fdtype *args)
{
  if (FD_CONS_TYPE(fn)==fd_ffi_type) {
    struct FD_FFI_PROC *proc=(struct FD_FFI_PROC *) fn;
    int arity=proc->fcn->arity;
    void *argvalues=u8_alloc_n(arity,void *);
    void **argptrs=u8_alloc_n(arity,void *);
    fdtype lispval; unsigned char *bytesval; long long ival;
    int i=0, rv=-1; while (i<arity) {argptrs[i]=&(argvalues[i]); i++;}
    i=0; while (i<arity) {
      fdtype arg=args[i];
      ffi_type *argtype=proc->ffi_argtypes[i];
      argvalues[i]=0;
      i++;}
    if (1)
      rv=ffi_call(proc->ffi_cif,proc->ffi_fcn,&bytesval);
    else if (1)
      rv=ffi_call(proc->ffi_cif,proc->ffi_fcn,&ival);
    else rv=ffi_call(proc->ffi_cif,proc->ffi_fcn,&lispval);
    return result;}
  else return fd_err(_("Not an foreign function interface"),
		     "ffi_caller",u8_strdup(fn->fcn_name),FD_VOID);
}

/* Generic object methods */

static void recycle_ffi_proc(struct FD_RAW_CONS *c)
{
  struct FD_FFI_PROC *ffi=(struct FD_FFI_PROC *)c;
  int arity=ffi->fd_ffi_arity;
  if (ffi->fd_ffi_defaults) {
    fdtype *values=ffi->fd_ffi_defaults;
    int i=0; while (i<arity) {
      fdtype v=values[i++]; fd_decref(v);}
    u8_free(values);}
  u8_free(ffi->fcn_name); 
  u8_free(ffi->fd_ffi_cif); 
  u8_free(ffi->fd_ffi_rtype); 
  u8_free(ffi->fd_ffi_argtypes);
  u8_xfree(ffi->fd_ffi_defaults);
  if (!(FD_STATIC_CONSP(ffi))) u8_free(ffi);
}

static int unparse_ffi_proc(u8_output out,fdtype x)
{
  struct FD_FFI_PROC *ffi=(struct FD_FFI_PROC *)x;
  u8_printf(out,"#<FFI '%s' #!%llx>",ffi->fcn_name,(long long) ffi);
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

