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

static fdtype uint_symbol, int_symbol, ushort_symbol, short_symbol;
static fdtype ulong_symbol, long_symbol, uchar_symbol, char_symbol;
static fdtype string_symbol, packet_symbol, ptr_symbol, cons_symbol;
static fdtype float_symbol, double_symbol, size_symbol, lisp_symbol;
static fdtype byte_symbol, basetype_symbol;

#if FD_ENABLE_FFI
#include <ffi.h>

#ifndef u8_xfree
#define u8_xfree(ptr) if (ptr) free((char *)ptr); else ptr=ptr;
#endif

FD_EXPORT fdtype fd_ffi_call(struct FD_FUNCTION *fn,int n,fdtype *args);

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
  else if ((arg==long_symbol)||(arg==size_symbol))
    return &ffi_type_sint64;
  else if (arg==uint_symbol)
    return &ffi_type_uint32;
  else if (arg==int_symbol)
    return &ffi_type_sint32;
  else if (arg==ushort_symbol)
    return &ffi_type_uint16;
  else if (arg==short_symbol)
    return &ffi_type_sint16;
  else if ((arg==uchar_symbol)||(arg==byte_symbol))
    return &ffi_type_uint8;
  else if (arg==char_symbol)
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
 fdtype return_spec,fdtype *argspecs)
{
  void *mod_arg=(filename==NULL) ? ((void *)NULL) : 
    (u8_dynamic_load(filename));
  if (FD_EXPECT_FALSE((filename) && (mod_arg==NULL)))
    return NULL;
  ffi_type *return_type=get_ffi_type(return_spec);
  if (return_type==NULL) return NULL;
  fdtype *savespecs=u8_alloc_n(arity,fdtype);
  ffi_type **ffi_argtypes=u8_alloc_n(arity,ffi_type *);
  int i=0; while (i<arity) {
    fdtype argspec=argspecs[i];
    ffi_type *ffi_argtype=get_ffi_type(argspec);
    if (ffi_argtype==NULL) {
      u8_free(ffi_argtypes);
      u8_free(savespecs);
      return NULL;}
    ffi_argtypes[i]=ffi_argtype;
    savespecs[i]=argspec;
    i++;}
  struct FD_FFI_PROC *proc=
    u8_zalloc_for("fd_make_ffi_proc",struct FD_FFI_PROC);
  FD_INIT_CONS(proc,fd_ffi_type);
  ffi_status rv=
    ffi_prep_cif(&(proc->ffi_interface), FFI_DEFAULT_ABI, arity,
		 return_type,ffi_argtypes);
  if (rv == FFI_OK) {
    /* Set up generic function fields */
    fd_incref_vec(savespecs,arity);
    proc->fcn_name=u8_strdup(name);
    proc->fcn_filename=u8_strdup(filename);
    proc->fcn_arity=arity;
    proc->fcn_min_arity=arity;
    proc->fcn_defaults=NULL;
    proc->ffi_return_type=return_type;
    proc->ffi_argtypes=ffi_argtypes;
    proc->ffi_return_spec=return_spec; fd_incref(return_spec);
    proc->ffi_argspecs=argspecs;
    proc->fcn_ndcall=0;
    proc->fcn_xcall=1;
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

static int handle_ffi_arg(fdtype arg,fdtype spec,void **valptr,void **argptr)
{
  if (spec==lisp_symbol) {
    *argptr=valptr;
    return 1;}
  else if (spec==cons_symbol) {
    if (FD_CONSP(arg)) {
      *argptr=(void *)arg;
      return 1;}
    else {
      fd_xseterr("NotACons","handle_ffi_arg",NULL,arg);
      return -1;}}
  else if (FD_FIXNUMP(arg)) {
    long long ival=FD_FIX2INT(arg);
    if (spec==int_symbol) 
      *valptr=(void *)ival;
    else if (spec==uint_symbol)
      *valptr=(void *)ival;
    else if (spec==long_symbol)
      *valptr=(void *)ival;
    else if (spec==ulong_symbol)
      *valptr=(void *)ival;
    else {
      fd_xseterr("BadIntType","handle_ffi_arg",NULL,spec);
      return -1;}
    *argptr=valptr;
    return 1;}
  else if (FD_STRINGP(arg)) {
    *argptr=(void *)FD_STRDATA(arg);
    return 1;}
  else {
    fd_seterr("BadFFIArg","handle_ffi_arg",NULL,arg);
    return -1;}
}

FD_EXPORT fdtype fd_ffi_call(struct FD_FUNCTION *fn,int n,fdtype *args)
{
  if (FD_CONS_TYPE(fn)==fd_ffi_type) {
    struct FD_FFI_PROC *proc=(struct FD_FFI_PROC *) fn;
    fdtype *argspecs=proc->ffi_argspecs;
    fdtype return_spec=proc->ffi_return_spec;
    void *vals[10], *argptrs[10];
    int arity=proc->fcn_arity;
    int i=0;
    i=0; while (i<arity) {
      int rv=handle_ffi_arg(args[i],argspecs[i],&(vals[i]),&argptrs[i]);
      if (rv<0) return FD_ERROR_VALUE;
      else i++;}
    if (return_spec==lisp_symbol) {
      fdtype result;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&result,argptrs);
      return result;}
    else if (return_spec==string_symbol) {
      u8_string stringval=NULL;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&stringval,argptrs);
      return fd_make_string(NULL,-1,stringval);}
    else if (return_spec==int_symbol) {
      int intval=-1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else if (return_spec==uint_symbol) {
      unsigned int intval=0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else if ((return_spec==long_symbol)||(return_spec==size_symbol)) {
      long long intval=-1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else if (return_spec==ulong_symbol) {
      unsigned long long intval=-1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else return FD_VOID;}
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
  u8_free(ffi->ffi_return_type); 
  if (!(FD_STATIC_CONSP(ffi))) u8_free(ffi);
}

static int unparse_ffi_proc(u8_output out,fdtype x)
{
  struct FD_FFI_PROC *ffi=(struct FD_FFI_PROC *)x;
  u8_printf(out,"#<FFI '%s' #!%llx>",ffi->fcn_name,(long long) ffi);
  return 1;
}

FD_EXPORT long long fd_test_ffi_plus(int x,int y)
{
  long long result=x+y;
  return result;
}

FD_EXPORT size_t fd_test_ffi_strlen(u8_string s)
{
  return strlen(s);
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
  uint_symbol=fd_intern("UINT");
  int_symbol=fd_intern("INT");
  ushort_symbol=fd_intern("USHORT");
  short_symbol=fd_intern("SHORT");
  ulong_symbol=fd_intern("ULONG");
  long_symbol=fd_intern("LONG");
  uchar_symbol=fd_intern("UCHAR");
  byte_symbol=fd_intern("BYTE");
  size_symbol=fd_intern("SIZE");
  char_symbol=fd_intern("CHAR");
  string_symbol=fd_intern("STRING");
  packet_symbol=fd_intern("PACKET");
  ptr_symbol=fd_intern("PTR");
  cons_symbol=fd_intern("CONS");
  lisp_symbol=fd_intern("LISP");

  basetype_symbol=fd_intern("BASETYPE");

  u8_register_source_file(_FILEINFO);
}
#endif

