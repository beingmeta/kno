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

static fdtype fd_uint_symbol, fd_sint_symbol, fd_ushort_symbol, fd_sshort_symbol;
static fdtype fd_ulong_symbol, fd_slong_symbol, fd_uchar_symbol, fd_schar_symbol;
static fdtype fd_string_symbol, fd_packet_symbol, fd_ptr_symbol, fd_cons_symbol;
static fdtype fd_float_symbol, fd_double_symbol, fd_size_symbol, fd_lisp_symbol;

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
(u8_string name,u8_string filename,int arity,
 ffi_type *return_type,ffi_type *argtypes,
 fdtype *defaults)
{
  ffi_cif *cif = u8_zalloc_for("fd_make_ffi_proc",ffi_cif);
  void *mod_arg=(filename==NULL) ? ((void *)NULL) : 
    (u8_dynamic_load(filename));
  if (FD_EXPECT_FALSE((filename) && (mod_arg==NULL)))
    return NULL;
  ffi_status rv=
    ffi_prep_cif(cif, FFI_DEFAULT_ABI, arity,
		 return_type,&argtypes);
  if (rv == FFI_OK) {
    struct FD_FFI_PROC *proc=
      u8_zalloc_for("fd_make_ffi_proc",struct FD_FFI_PROC);
    FD_INIT_CONS(proc,fd_ffi_type);
    /* Set up generic function fields */
    proc->fcn_name=u8_strdup(name);
    proc->fcn_filename=u8_strdup(filename);
    proc->fcn_arity=arity;
    proc->fcn_defaults=defaults;
    proc->fcn_ndcall=0;
    proc->fcn_xcall=1;
    proc->fcn_arity=arity;
    if (defaults==NULL)
      proc->fcn_min_arity=arity;
    else {
      int min=0, i=0; while (i<arity) {
	if (FD_VOIDP(defaults[i])) break;
	else min=i++;}
      proc->fcn_min_arity=min;}
    proc->ffi_return_type=return_type;
    proc->ffi_argtypes=argtypes;
    // Defer arity checking to fd_ffi_call
    proc->fcn_handler.xcalln=fd_ffi_call;
    proc->ffi_dlsym=u8_dynamic_symbol(name,mod_arg);
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
    int arity=proc->fcn_arity;
    void **argvalues=u8_alloc_n(arity,void *);
    fdtype result=FD_VOID;
    fdtype lispval; unsigned char *bytesval; long long ival;
    int i=0, rv=-1;
    i=0; while (i<arity) {
      fdtype arg=args[i];
      ffi_type argtype=proc->ffi_argtypes[i];
      /* Do the right thing here */
      argvalues[i]=0;
      i++;}
    if (1)
      ffi_call(proc->ffi_cif,proc->ffi_dlsym,&bytesval,argvalues);
    else if (1)
      ffi_call(proc->ffi_cif,proc->ffi_dlsym,&ival,argvalues);
    else ffi_call(proc->ffi_cif,proc->ffi_dlsym,&lispval,argvalues);
    return result;}
  else return fd_err(_("Not an foreign function interface"),
		     "ffi_caller",u8_strdup(fn->fcn_name),FD_VOID);
}

/* Generic object methods */

static void recycle_ffi_proc(struct FD_RAW_CONS *c)
{
  struct FD_FFI_PROC *ffi=(struct FD_FFI_PROC *)c;
  int arity=ffi->fcn_arity;
  if (ffi->fcn_defaults) {
    fdtype *values=ffi->fcn_defaults;
    int i=0; while (i<arity) {
      fdtype v=values[i++]; fd_decref(v);}
    u8_free(values);}
  u8_free(ffi->ffi_argtypes);
  if (ffi->fcn_typeinfo) u8_xfree(ffi->fcn_typeinfo);
  if (ffi->fcn_defaults) u8_xfree(ffi->fcn_defaults);
  u8_free(ffi->fcn_name); 
  if (ffi->fcn_filename) u8_free(ffi->fcn_filename); 
  u8_free(ffi->ffi_cif); 
  u8_free(ffi->ffi_return_type); 
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

  fd_double_symbol=fd_intern("DOUBLE");
  fd_float_symbol=fd_intern("FLOAT");
  fd_uint_symbol=fd_intern("UNSIGNEDINT");
  fd_sint_symbol=fd_intern("SIGNEDINT");
  fd_ushort_symbol=fd_intern("UNSIGNEDSHORT");
  fd_sshort_symbol=fd_intern("SIGNEDSHORT");
  fd_ulong_symbol=fd_intern("UNSIGNEDLONG");
  fd_slong_symbol=fd_intern("SIGNEDLONG");
  fd_uchar_symbol=fd_intern("UNSIGNEDCHAR");
  fd_byte_symbol=fd_intern("BYTE");
  fd_size_t_symbol=fd_intern("SIZE");
  fd_bool_symbol=fd_intern("BOOLEAN");
  fd_schar_symbol=fd_intern("SIGNEDCHAR");
  fd_string_symbol=fd_intern("STRING");
  fd_packet_symbol=fd_intern("PACKET");
  fd_ptr_symbol=fd_intern("PTR");
  fd_cons_symbol=fd_intern("CONS");
  fd_lisp_symbol=fd_intern("LISP");

  u8_register_source_file(_FILEINFO);
}
#endif

