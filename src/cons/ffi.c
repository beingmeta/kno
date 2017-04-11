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

static fdtype uint_symbol, sint_symbol, ushort_symbol, sshort_symbol;
static fdtype ulong_symbol, slong_symbol, uchar_symbol, schar_symbol;
static fdtype string_symbol, packet_symbol, ptr_symbol, cons_symbol;
static fdtype float_symbol, double_symbol, size_symbol, lisp_symbol;
static fdtype byte_symbol, basetype_symbol;

#if HAVE_FFI_H && HAVE_LIBFFI
#include <ffi.h>

#ifndef u8_xfree
#define u8_xfree(ptr) if (ptr) free((char *)ptr); else ptr=ptr;
#endif

static fdtype ffi_caller(struct FD_FUNCTION *fn,int n,fdtype *args);

static ffi_type *get_ffi_type(fdtype arg)
{
  if (FD_TABLEP(arg)) {
    fdtype typename=fd_getopt(arg,basetype_symbol,FD_VOID);
    if (FD_VOIDP(typename)) {
      fd_seterr("NoFFItype","get_ffi_type",NULL,arg);
      return NULL;}
    else if (!(FD_SYMBOLP(typename))) {
      fd_seterr("BadFFItype","get_ffi_type",NULL,typename);
      return NULL;}
    else return get_ffi_type(typename);}
  else if (!(FD_SYMBOLP(arg))) {
    fd_seterr("BadFFItype","get_ffi_type",NULL,arg);
    return NULL;}
  else if ((arg==ulong_symbol) || (arg==lisp_symbol))
    return &ffi_type_uint64;
  else if ((arg==slong_symbol)||(size_symbol))
    return &ffi_type_sint64;
  else if (arg==uint_symbol)
    return &ffi_type_uint32;
  else if (arg==sint_symbol)
    return &ffi_type_sint32;
  else if (arg==ushort_symbol)
    return &ffi_type_uint16;
  else if (arg==sshort_symbol)
    return &ffi_type_sint16;
  else if ((arg==uchar_symbol)||(arg==byte_symbol))
    return &ffi_type_uint8;
  else if (arg==schar_symbol)
    return &ffi_type_sint8;
  else if (arg==float_symbol)
    return &ffi_type_float;
  else if (arg==double_symbol)
    return &ffi_type_double;
  else if ((arg==ptr_symbol) || (arg==cons_symbol) ||
	   (arg==string_symbol) || (arg==packet_symbol))
    return &ffi_type_pointer;
  else {
    fd_seterr("BadFFItype","get_ffi_type",NULL,arg);
    return NULL;}
}

/** Change notes: 

    Make fd_make_ffi_proc take fdtypes for the return type and
    argtypes, and convert them into ffi_types to be passed in.
**/

FD_EXPORT struct FD_FFI_PROC *fd_make_ffi_proc
(u8_string name,u8_string filename,int arity,
 fdtype return_type,fdtype *argspecs)
{
  void *mod_arg=(filename==NULL) ? ((void *)NULL) : 
    (u8_dynamic_load(filename));
  if (FD_EXPECT_FALSE((filename) && (mod_arg==NULL)))
    return NULL;
  ffi_type *ffi_return_type=get_ffi_type(return_type);
  if (ffi_return_type==NULL) return NULL;
  fdtype *savespecs=u8_alloc_n(arity,fdtype);
  ffi_type **ffi_argtypes=u8_alloc_n(arity,ffi_type *);
  int i=0; while (i<arity) {
    fdtype argspec=argspecs[i];
    ffi_type *ffi_argtype=get_ffi_type(argspec);
    if (ffitype==NULL) {
      u8_free(savespecs);
      u8_free(argtypes);
      return NULL;}
    ffi_argtypes[i]=ffi_argtype;
    savespecs[i]=argspec;
    i++;}
  struct FD_FFI_PROC *proc=
    u8_zalloc_for("fd_make_ffi_proc",struct FD_FFI_PROC);
  FD_INIT_CONS(proc,fd_ffi_type);
  ffi_status rv=
    ffi_prep_cif(&(proc->ffi_interface), FFI_DEFAULT_ABI, arity,
		 ffi_return_type,ffi_argtypes);
  if (rv == FFI_OK) {
    /* Set up generic function fields */
    i=0; while (i<arity) {fd_incref(savespecs[i]); i++;}
    proc->fcn_name=u8_strdup(name);
    proc->fcn_filename=u8_strdup(filename);
    proc->fcn_arity=arity;
    proc->fcn_defaults=NULL;
    proc->ffi_return_type=ffi_return_type;
    proc->ffi_argtypes=ffi_argtypes;
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
    if (rv == FFI_BAD_TYPEDEF)
      u8_seterr(fd_ffi_BadTypeinfo,"fd_make_ffi_proc",NULL);
    else if (rv == FFI_BAD_ABI)
      u8_seterr(fd_ffi_BadABI,"fd_make_ffi_proc",NULL);
    else u8_seterr(fd_ffi_FFIError,"fd_make_ffi_proc",NULL);
    u8_free(savespecs);
    u8_free(ffi_argtypes);
    return NULL;}
}

FD_EXPORT fdtype fd_ffi_call(struct FD_FUNCTION *fn,int n,fdtype *args)
{
  if (FD_CONS_TYPE(fn)==fd_ffi_type) {
    fdtype result=FD_VOID;
    struct FD_FFI_PROC *proc=(struct FD_FFI_PROC *) fn;
    int arity=proc->fcn_arity;
    void **argvalues=u8_alloc_n(arity,void *);
    fdtype lispval; unsigned char *bytesval; long long ival;
    int i=0, rv=-1;
    i=0; while (i<arity) {
      fdtype arg=args[i];
      ffi_type argtype=proc->ffi_argtypes[i];
      /* Do the right thing here */
      argvalues[i]=0;
      i++;}
    if (1) /* Branch on return_type */
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
  if (ffi->fcn_name) u8_free(ffi->fcn_name);
  if (ffi->fcn_filename) u8_free(ffi->fcn_filename); 
  if (ffi->fcn_typeinfo) u8_free(ffi->fcn_typeinfo);
  if (ffi->fcn_defaults) {
    fdtype *default_values=ffi->fcn_defaults;
    int i=0; while (i<arity) {
      fdtype v=default_values[i++]; fd_decref(v);}
    u8_free(default_values);}
  u8_free(ffi->ffi_argtypes);
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

  double_symbol=fd_intern("DOUBLE");
  float_symbol=fd_intern("FLOAT");
  uint_symbol=fd_intern("UNSIGNEDINT");
  sint_symbol=fd_intern("SIGNEDINT");
  ushort_symbol=fd_intern("UNSIGNEDSHORT");
  sshort_symbol=fd_intern("SIGNEDSHORT");
  ulong_symbol=fd_intern("UNSIGNEDLONG");
  slong_symbol=fd_intern("SIGNEDLONG");
  uchar_symbol=fd_intern("UNSIGNEDCHAR");
  byte_symbol=fd_intern("BYTE");
  size_symbol=fd_intern("SIZE");
  schar_symbol=fd_intern("SIGNEDCHAR");
  string_symbol=fd_intern("STRING");
  packet_symbol=fd_intern("PACKET");
  ptr_symbol=fd_intern("PTR");
  cons_symbol=fd_intern("CONS");
  lisp_symbol=fd_intern("LISP");

  basetype_symbol=fd_intern("BASETYPE");

  u8_register_source_file(_FILEINFO);
}
#endif

