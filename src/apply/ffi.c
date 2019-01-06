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

static lispval uint_symbol, int_symbol, ushort_symbol, short_symbol;
static lispval ulong_symbol, long_symbol, uchar_symbol, char_symbol;
static lispval string_symbol, packet_symbol, ptr_symbol, cons_symbol;
static lispval float_symbol, double_symbol, size_symbol, lisp_symbol;
static lispval lispref_symbol, strcpy_symbol, void_symbol;
static lispval byte_symbol, basetype_symbol;

#if FD_ENABLE_FFI
#if HAVE_FFI_FFI_H
#include <ffi/ffi.h>
#elif HAVE_FFI_H
#include <ffi.h>
#endif

#ifndef u8_xfree
#define u8_xfree(ptr) if (ptr) free((char *)ptr); else ptr = ptr;
#endif

FD_EXPORT lispval fd_ffi_call(struct FD_FUNCTION *fn,int n,lispval *args);

static ffi_type *get_ffi_type(lispval arg)
{
  if (TABLEP(arg)) {
    lispval typename = fd_getopt(arg,basetype_symbol,VOID);
    if (VOIDP(typename)) {
      fd_seterr("NoFFItype","get_ffi_type",NULL,arg);
      return NULL;}
    else if (!(SYMBOLP(typename))) {
      fd_seterr("BadFFItype","get_ffi_type",NULL,typename);
      return NULL;}
    else return get_ffi_type(typename);}
  else if (!(SYMBOLP(arg))) {
    fd_seterr("BadFFItype","get_ffi_type",NULL,arg);
    return NULL;}
  else if ((arg == ulong_symbol) || (arg == lisp_symbol))
    return &ffi_type_uint64;
  else if ((arg == long_symbol)||(arg == size_symbol))
    return &ffi_type_sint64;
  else if (arg == uint_symbol)
    return &ffi_type_uint32;
  else if (arg == int_symbol)
    return &ffi_type_sint32;
  else if (arg == ushort_symbol)
    return &ffi_type_uint16;
  else if (arg == short_symbol)
    return &ffi_type_sint16;
  else if ((arg == uchar_symbol)||(arg == byte_symbol))
    return &ffi_type_uint8;
  else if (arg == char_symbol)
    return &ffi_type_sint8;
  else if (arg == float_symbol)
    return &ffi_type_float;
  else if (arg == double_symbol)
    return &ffi_type_double;
  else if (arg == void_symbol)
    return &ffi_type_void;
  else if ((arg == ptr_symbol) || (arg == cons_symbol) ||
           (arg == string_symbol) || (arg == packet_symbol))
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
 lispval return_spec,lispval *argspecs)
{
  if (name==NULL) {
    u8_seterr("NullArg","fd_make_ffi_proc/name",NULL);
    return NULL;}
  void *mod_arg = (filename == NULL) ? ((void *)NULL) :
    (u8_dynamic_load(filename));
  if ((filename) && (mod_arg == NULL))
    return NULL;
  void *symbol=u8_dynamic_symbol(name,mod_arg);
  if (symbol == NULL) {
    u8_seterr("NoSuchLink","fd_make_ffi_proc/u8_dynamic_symbol",
              u8_strdup(name));
    return NULL;}
  ffi_type *return_type = get_ffi_type(return_spec);
  if (return_type == NULL) return NULL;
  lispval *savespecs = u8_alloc_n(arity,lispval);
  ffi_type **ffi_argtypes = u8_alloc_n(arity,ffi_type *);
  int i = 0; while (i<arity) {
    lispval argspec = argspecs[i];
    ffi_type *ffi_argtype = get_ffi_type(argspec);
    if (ffi_argtype == NULL) {
      u8_free(ffi_argtypes);
      u8_free(savespecs);
      return NULL;}
    ffi_argtypes[i]=ffi_argtype;
    savespecs[i]=argspec;
    i++;}
  struct FD_FFI_PROC *proc=u8_alloc(struct FD_FFI_PROC);
  FD_INIT_FRESH_CONS(proc,fd_ffi_type);
  ffi_status rv=
    ffi_prep_cif(&(proc->ffi_interface), FFI_DEFAULT_ABI, arity,
                 return_type,ffi_argtypes);
  if (rv == FFI_OK) {
    /* Set up generic function fields */
    fd_incref_vec(savespecs,arity);
    proc->fcn_name = u8_strdup(name);
    proc->fcn_filename = u8dup(filename);
    proc->fcn_arity = arity;
    proc->fcn_min_arity = arity;
    proc->fcn_defaults = NULL;
    proc->ffi_return_type = return_type;
    proc->ffi_argtypes = ffi_argtypes;
    proc->ffi_return_spec = return_spec; fd_incref(return_spec);
    proc->ffi_argspecs = savespecs;
    proc->fcn_ndcall = 0;
    proc->fcn_xcall = 1;
    // Defer arity checking to fd_ffi_call
    proc->fcn_handler.xcalln = NULL;
    proc->ffi_dlsym = symbol;
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

static int ffi_type_error(u8_context expecting,lispval arg)
{
  fd_seterr(_("FFI Type error"),expecting,NULL,arg);
  return -1;
}

static int handle_ffi_arg(lispval arg,lispval spec,
                          void **valptr,void **argptr)
{
  if (spec == lisp_symbol)
    *valptr = (void *) arg;
  else if (spec == lispref_symbol) {
    *valptr = (void *) arg;
    fd_incref(arg);}
  else if (spec == cons_symbol) {
    if (CONSP(arg))
      *valptr = (void *)arg;
    else return ffi_type_error("CONS",arg);}
  else if (spec == ptr_symbol) {
    if (FD_PRIM_TYPEP(arg,fd_rawptr_type)) {
      struct FD_RAWPTR *raw=(fd_rawptr)arg;
      *valptr=raw->ptrval;}
    else if (CONSP(arg))
      *valptr = (void *)arg;
    else return ffi_type_error("pointer",arg);}
  else if (FIXNUMP(arg)) {
    long long ival = FIX2INT(arg);
    if (spec == int_symbol) {
      if ((ival <= INT_MAX) && (ival >= INT_MIN))
        *((int *)valptr) = ival;
      else return ffi_type_error("int(C)",arg);}
    else if (spec == uint_symbol) {
      if ((ival <= UINT_MAX) && (ival >= 0 ))
        *((unsigned int *)valptr) = ival;
      else return ffi_type_error("uint(C)",arg);}
    else if (spec == long_symbol) {
      if ((ival <= LONG_MAX) && (ival >= LONG_MIN ))
        *((long *)valptr) = ival;
      else return ffi_type_error("long(C)",arg);}
    else if (spec == ulong_symbol) {
      if ((ival <= ULONG_MAX) && (ival >= 0 ))
        *((unsigned long *)valptr) = ival;
      else return ffi_type_error("ulong(C)",arg);}
    else if (spec == short_symbol) {
      if ((ival <= SHRT_MAX) && (ival >= SHRT_MIN ))
        *((short *)valptr) = ival;
      else return ffi_type_error("short(C)",arg);}
    else if (spec == ushort_symbol) {
      if ((ival <= USHRT_MAX) && (ival >= 0 ))
        *((unsigned short *)valptr) = ival;
      else return ffi_type_error("ushort(C)",arg);}
    else if (spec == char_symbol) {
      if ((ival <= CHAR_MAX) && (ival >= CHAR_MIN ))
        *((char *)valptr) = ival;
      else return ffi_type_error("char(C)",arg);}
    else if (spec == uchar_symbol) {
      if ((ival <= UCHAR_MAX) && (ival >= 0 ))
        *((unsigned char *)valptr) = ival;
      else return ffi_type_error("uchar(C)",arg);}
    else if (spec == byte_symbol) {
      if ((ival <= UCHAR_MAX) && (ival >= 0 ))
        *((unsigned char *)valptr) = ival;
      else return ffi_type_error("byte/uchar(C)",arg);}
    else if (spec == size_symbol) {
      if ((ival <= LONG_MAX) && (ival >= LONG_MIN ))
        *((ssize_t *)valptr) = ival;
      else return ffi_type_error("ssize_t(C)",arg);}
    else if (spec == float_symbol) {
      float f=(float)ival;
      *((float *)valptr) = f;}
    else if (spec == float_symbol) {
      double f=(double)ival;
      *((double *)valptr) = f;}
    else {
      fd_seterr("BadIntType","handle_ffi_arg",NULL,spec);
      return -1;}
    *argptr = valptr;
    return 1;}
  else if (FD_FLONUMP(arg)) {
    if (spec == float_symbol) {
      float f=(float)FD_FLONUM(arg);
      *((float *)valptr)=f;}
    else if (spec == double_symbol) {
      double f=(float)FD_FLONUM(arg);
      *((double *)valptr)=f;}
    else if (SYMBOLP(spec))
      return fd_type_error(SYM_NAME(spec),"handle_ffi_arg",spec);
    else return fd_type_error("ctype","handle_ffi_arg",spec);
    *argptr = valptr;
    return 1;}
  else if (STRINGP(arg)) {
    if (spec == string_symbol) {
      *valptr = (void *)CSTRING(arg);
      *argptr = valptr;
      return 1;}
    else if (SYMBOLP(spec))
      return ffi_type_error(SYM_NAME(spec),arg);
    else return ffi_type_error("ctype",arg);}
  else if (PACKETP(arg)) {
    if (spec == packet_symbol) {
      *valptr = (void *)CSTRING(arg);
      *argptr = valptr;
      return 1;}
    else if (SYMBOLP(spec))
      return ffi_type_error(SYM_NAME(spec),arg);
    else return ffi_type_error("ctype",arg);}
  else {
    fd_seterr("BadFFIArg","handle_ffi_arg",NULL,arg);
    return -1;}
  *argptr=valptr;
  return 1;
}

FD_EXPORT lispval fd_ffi_call(struct FD_FUNCTION *fn,int n,lispval *args)
{
  if (FD_CONS_TYPE(fn) == fd_ffi_type) {
    struct FD_FFI_PROC *proc = (struct FD_FFI_PROC *) fn;
    lispval *argspecs = proc->ffi_argspecs;
    lispval return_spec = proc->ffi_return_spec;
    void *vals[10], *argptrs[10];
    int arity = proc->fcn_arity;
    int i = 0;
    i = 0; while (i<arity) {
      int rv = handle_ffi_arg(args[i],argspecs[i],&(vals[i]),&(argptrs[i]));
      if (rv<0) return FD_ERROR;
      else i++;}
    if (return_spec == lisp_symbol) {
      lispval result;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&result,argptrs);
      return result;}
    else if (return_spec == lispref_symbol) {
      lispval result;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&result,argptrs);
      return fd_incref(result);}
    else if ( (return_spec == string_symbol) ||
              (return_spec == strcpy_symbol) ) {
      u8_string stringval = NULL;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&stringval,argptrs);
      return fd_make_string(NULL,-1,stringval);}
    else if (return_spec == strcpy_symbol) {
      u8_string stringval = NULL;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&stringval,argptrs);
      return fd_make_string(NULL,-1,stringval);}
    else if (return_spec == int_symbol) {
      int intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else if (return_spec == uint_symbol) {
      unsigned int intval = 0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else if (return_spec == short_symbol) {
      short intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else if (return_spec == ushort_symbol) {
      unsigned short intval = 0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else if (return_spec == char_symbol) {
      char intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else if (return_spec == uchar_symbol) {
      unsigned char intval = 0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else if (return_spec == byte_symbol) {
      unsigned char intval = 0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else if ((return_spec == long_symbol)||(return_spec == size_symbol)) {
      long long intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else if (return_spec == ulong_symbol) {
      unsigned long long intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return FD_INT(intval);}
    else if (return_spec == double_symbol) {
      double dval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&dval,argptrs);
      return fd_make_flonum(dval);}
    else if (return_spec == float_symbol) {
      float dval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&dval,argptrs);
      return fd_make_flonum(dval);}
    else return VOID;}
  else return fd_err(_("Not an foreign function interface"),
                     "ffi_caller",u8_strdup(fn->fcn_name),VOID);
}

/* Generic object methods */

static void recycle_ffi_proc(struct FD_RAW_CONS *c)
{
  struct FD_FFI_PROC *ffi = (struct FD_FFI_PROC *)c;
  int arity = ffi->fcn_arity;
  if (ffi->fcn_name) u8_free(ffi->fcn_name);
  if (ffi->fcn_filename) u8_free(ffi->fcn_filename);
  if ( (ffi->fcn_doc) && (ffi->fcn_freedoc) )
    u8_free(ffi->fcn_doc);
  if (ffi->fcn_typeinfo) u8_free(ffi->fcn_typeinfo);
  if (ffi->fcn_defaults) {
    lispval *default_values = ffi->fcn_defaults;
    int i = 0; while (i<arity) {
      lispval v = default_values[i++]; fd_decref(v);}
    u8_free(default_values);}
  u8_free(ffi->ffi_argtypes);
  if (!(FD_STATIC_CONSP(ffi))) u8_free(ffi);
}

static int unparse_ffi_proc(u8_output out,lispval x)
{
  struct FD_FFI_PROC *ffi = (struct FD_FFI_PROC *)x;
  lispval *argspecs=ffi->ffi_argspecs;
  int i=0, n=ffi->fcn_arity;
  u8_printf(out,"#<FFI '%s'(",ffi->fcn_name);
  while (i<n) {
    if (i>0)
      u8_printf(out,",%q",argspecs[i++]);
    else u8_printf(out,"%q",argspecs[i++]);}
  u8_printf(out,") #!%llx>",FD_LONGVAL( ffi));
  return 1;
}

static ssize_t write_ffi_dtype(struct FD_OUTBUF *out,lispval x)
{
  int n_elts=0;
  struct FD_FFI_PROC *fcn = (struct FD_FFI_PROC *)x;
  unsigned char buf[200], *tagname="%FFI";
  struct FD_OUTBUF tmp = { 0 };
  FD_INIT_OUTBUF(&tmp,buf,100,0);
  fd_write_byte(&tmp,dt_compound);
  fd_write_byte(&tmp,dt_symbol);
  fd_write_4bytes(&tmp,strlen(tagname));
  fd_write_bytes(&tmp,tagname,strlen(tagname));
  if (fcn->fcn_name) n_elts++;
  if (fcn->fcn_filename) n_elts++;
  fd_write_4bytes(&tmp,n_elts);
  if (fcn->fcn_name) {
    size_t len=strlen(fcn->fcn_name);
    fd_write_byte(&tmp,dt_symbol);
    fd_write_4bytes(&tmp,len);
    fd_write_bytes(&tmp,fcn->fcn_name,len);}
  if (fcn->fcn_filename) {
    size_t len=strlen(fcn->fcn_filename);
    fd_write_byte(&tmp,dt_string);
    fd_write_4bytes(&tmp,len);
    fd_write_bytes(&tmp,fcn->fcn_filename,len);}
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  fd_write_bytes(out,tmp.buffer,n_bytes);
  fd_close_outbuf(&tmp);
  return n_bytes;
}

FD_EXPORT long long ffitest_ipi(int x,int y)
{
  long long result = x+y;
  return result;
}

FD_EXPORT int ffitest_sps(short x,short y)
{
  int result = x + y;
  return result;
}

FD_EXPORT double ffitest_fpf(float x,float y)
{
  double result = x + y;
  return result;
}

FD_EXPORT size_t ffitest_strlen(u8_string s)
{
  return strlen(s);
}

FD_EXPORT int ffitest_chr(u8_string s,int off)
{
  int len=strlen(s);
  if (off<0) return -1;
  else if (off>=len) return -1;
  else return s[off];
}


/* Initializations */

FD_EXPORT void fd_init_ffi_c()
{
  fd_type_names[fd_ffi_type]="foreign-function";
  fd_dtype_writers[fd_ffi_type]=write_ffi_dtype;

  fd_unparsers[fd_ffi_type]=unparse_ffi_proc;
  fd_recyclers[fd_ffi_type]=recycle_ffi_proc;

  fd_functionp[fd_ffi_type]=1;
  fd_applyfns[fd_ffi_type]=(fd_applyfn)fd_ffi_call;

  void_symbol = fd_intern("VOID");
  double_symbol = fd_intern("DOUBLE");
  float_symbol = fd_intern("FLOAT");
  uint_symbol = fd_intern("UINT");
  int_symbol = fd_intern("INT");
  ushort_symbol = fd_intern("USHORT");
  short_symbol = fd_intern("SHORT");
  ulong_symbol = fd_intern("ULONG");
  long_symbol = fd_intern("LONG");
  uchar_symbol = fd_intern("UCHAR");
  byte_symbol = fd_intern("BYTE");
  size_symbol = fd_intern("SIZE");
  char_symbol = fd_intern("CHAR");
  string_symbol = fd_intern("STRING");
  packet_symbol = fd_intern("PACKET");
  ptr_symbol = fd_intern("PTR");
  cons_symbol = fd_intern("CONS");
  lisp_symbol = fd_intern("LISP");

  basetype_symbol = fd_intern("BASETYPE");

  u8_register_source_file(_FILEINFO);
}

#else /* HAVE_FFI_H && HAVE_LIBFFI */
FD_EXPORT void fd_init_ffi_c()
{
  fd_type_names[fd_ffi_type]="foreign-function";
  fd_dtype_writers[fd_ffi_type]=NULL;

  void_symbol = fd_intern("VOID");
  double_symbol = fd_intern("DOUBLE");
  float_symbol = fd_intern("FLOAT");
  uint_symbol = fd_intern("UINT");
  int_symbol = fd_intern("INT");
  ushort_symbol = fd_intern("USHORT");
  short_symbol = fd_intern("SHORT");
  ulong_symbol = fd_intern("ULONG");
  long_symbol = fd_intern("LONG");
  uchar_symbol = fd_intern("UCHAR");
  byte_symbol = fd_intern("BYTE");
  size_symbol = fd_intern("SIZE");
  char_symbol = fd_intern("CHAR");
  string_symbol = fd_intern("STRING");
  packet_symbol = fd_intern("PACKET");
  ptr_symbol = fd_intern("PTR");
  cons_symbol = fd_intern("CONS");
  lisp_symbol = fd_intern("LISP");
  lispref_symbol = fd_intern("LISPREF");
  strcpy_symbol = fd_intern("STRCPY");

  basetype_symbol = fd_intern("BASETYPE");

  u8_register_source_file(_FILEINFO);
}
#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
