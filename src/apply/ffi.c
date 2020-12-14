/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/apply.h"
#include "kno/ffi.h"
#include "kno/eval.h"
#include "kno/numbers.h"
#include "kno/support.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>
#include <dlfcn.h>

u8_condition kno_ffi_BadTypeinfo=_("Bad FFI type info");
u8_condition kno_ffi_BadABI=_("Bad FFI ABI value");
u8_condition kno_ffi_FFIError=_("Unknown libffi error");
u8_condition kno_ffi_NoFunction=_("Unknown foreign function");
u8_condition kno_FFI_TypeError=(_("FFI type error"));


static lispval uint_symbol, int_symbol, ushort_symbol, short_symbol;
static lispval ulong_symbol, long_symbol, uchar_symbol, char_symbol;
static lispval string_symbol, packet_symbol, ptr_symbol, cons_symbol;
static lispval float_symbol, double_symbol, size_t_symbol, lisp_symbol;
static lispval lispref_symbol, strcpy_symbol, void_symbol, mallocd_symbol;
static lispval byte_symbol, basetype_symbol, typetag_symbol;
static lispval ffi_result_symbol, time_t_symbol, null_symbol;
static lispval strlen_symbol, packetlen_symbol, ptrloc_symbol, loclen_symbol;
static lispval nullable_symbol;

static void init_symbols(void);

#if KNO_ENABLE_FFI
#if HAVE_FFI_FFI_H
#include <ffi/ffi.h>
#elif HAVE_FFI_H
#include <ffi.h>
#endif

#ifndef u8_xfree
#define u8_xfree(ptr) if (ptr) free((char *)ptr); else ptr = ptr;
#endif

KNO_EXPORT lispval kno_ffi_call(struct KNO_FUNCTION *fn,int n,lispval *args);

static ffi_type *get_ffi_type(lispval arg)
{
  if (TABLEP(arg)) {
    lispval typename = kno_getopt(arg,basetype_symbol,VOID);
    if (VOIDP(typename))
      return KNO_ERR(NULL,"NoFFItype","get_ffi_type",NULL,arg);
    else if (!(SYMBOLP(typename)))
      return KNO_ERR(NULL,"BadFFItype","get_ffi_type",NULL,typename);
    else return get_ffi_type(typename);}
  else if (!(SYMBOLP(arg)))
    return KNO_ERR(NULL,"BadFFItype","get_ffi_type",NULL,arg);
  else if ( (arg == ulong_symbol) || (arg == lisp_symbol) )
    return &ffi_type_uint64;
  else if ( (arg == long_symbol) || (arg == size_t_symbol) ||
	    (arg == packetlen_symbol) || (arg == strlen_symbol) ||
	    (arg == loclen_symbol) )
    return &ffi_type_sint64;
  else if (arg == uint_symbol)
    return &ffi_type_uint32;
  else if (arg == int_symbol)
    return &ffi_type_sint32;
  else if (arg == ushort_symbol)
    return &ffi_type_uint16;
  else if (arg == short_symbol)
    return &ffi_type_sint16;
  else if ( (arg == uchar_symbol) || (arg == byte_symbol) )
    return &ffi_type_uint8;
  else if (arg == char_symbol)
    return &ffi_type_sint8;
  else if (arg == float_symbol)
    return &ffi_type_float;
  else if (arg == double_symbol)
    return &ffi_type_double;
  else if (arg == void_symbol)
    return &ffi_type_void;
  else if ( (arg == ptr_symbol) || (arg == cons_symbol) ||
	    (arg == string_symbol) || (arg == packet_symbol) ||
	    (arg == null_symbol) || (arg == ptrloc_symbol) )
    return &ffi_type_pointer;
  else if ( (arg == time_t_symbol) && (sizeof(time_t) == 8) )
    return &ffi_type_sint64;
  else if ( (arg == time_t_symbol) && (sizeof(time_t) == 4) )
    return &ffi_type_sint32;
  else return KNO_ERR(NULL,"BadFFItype","get_ffi_type",NULL,arg);
}

/** Change notes:

    Make kno_make_ffi_proc take knotypes for the return type and
    argtypes, and convert them into ffi_types to be passed in.
**/

KNO_EXPORT struct KNO_FFI_PROC *kno_make_ffi_proc
(u8_string name,u8_string filename,int arity,
 lispval return_spec,const lispval *argspecs)
{
  if (name==NULL) {
    u8_seterr("NullArg","kno_make_ffi_proc/name",NULL);
    return NULL;}
  void *mod_arg = (filename == NULL) ? ((void *)NULL) :
    (u8_dynamic_load(filename));
  if ((filename) && (mod_arg == NULL))
    return NULL;
  void *symbol=u8_dynamic_symbol(name,mod_arg);
  if (symbol == NULL) {
    u8_seterr(kno_ffi_NoFunction,"kno_make_ffi_proc/u8_dynamic_symbol",
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
  struct KNO_FFI_PROC *proc=u8_alloc(struct KNO_FFI_PROC);
  KNO_INIT_FRESH_CONS(proc,kno_ffi_type);
  ffi_status rv=
    ffi_prep_cif(&(proc->ffi_interface), FFI_DEFAULT_ABI, arity,
                 return_type,ffi_argtypes);
  if (rv == FFI_OK) {
    /* Set up generic function fields */
    kno_incref_vec(savespecs,arity);
    proc->fcn_name = u8_strdup(name);
    proc->fcn_filename = u8dup(filename);
    proc->fcn_call_width = proc->fcn_arity = arity;
    proc->fcn_arginfo_len = 0;
    proc->fcn_schema = NULL;
    proc->fcn_min_arity = arity;
    proc->ffi_return_type = return_type;
    proc->ffi_argtypes = ffi_argtypes;
    proc->ffi_return_spec = return_spec; kno_incref(return_spec);
    proc->ffi_argspecs = savespecs;
    proc->fcn_call = KNO_CALL_XCALL;
    // Defer arity checking to kno_ffi_call
    proc->fcn_handler.xcalln = NULL;
    proc->ffi_dlsym = symbol;
    return proc;}
  else {
    if (rv == FFI_BAD_TYPEDEF)
      u8_seterr(kno_ffi_BadTypeinfo,"kno_make_ffi_proc",NULL);
    else if (rv == FFI_BAD_ABI)
      u8_seterr(kno_ffi_BadABI,"kno_make_ffi_proc",NULL);
    else u8_seterr(kno_ffi_FFIError,"kno_make_ffi_proc",NULL);
    u8_free(savespecs);
    u8_free(ffi_argtypes);
    return NULL;}
}

static int ffi_type_error(u8_context expecting,lispval arg)
{
  return KNO_ERR(-1,kno_FFI_TypeError,expecting,NULL,arg);
}

static int handle_ffi_arg(lispval arg,lispval spec,
                          void **valptr,void **argptr)
{
  lispval basetype = (SYMBOLP(spec)) ? (spec) :
    (TABLEP(spec)) ? (kno_getopt(spec,basetype_symbol,VOID)) :
    (VOID);
  lispval typetag = (TABLEP(spec)) ?
    (kno_getopt(spec,typetag_symbol,VOID)) :
    (KNO_VOID);
  int nullable = (TABLEP(spec)) && (kno_testopt(spec,nullable_symbol,VOID));
  int defaultp = (TABLEP(spec)) && (kno_testopt(spec,KNOSYM_DEFAULT,VOID));
  if (VOIDP(basetype)) {
    u8_byte buf[256];
    kno_seterr("BadTypeSpec","handle_ffi_arg",
               u8_bprintf(buf,"%q",arg),spec);}
  if ( (defaultp) && ( (VOIDP(arg)) || (KNO_DEFAULTP(arg)) ) ) {
    if (!(VOIDP(arg))) {
      lispval dflt = kno_getopt(spec,KNOSYM_DEFAULT,KNO_VOID);
      lispval inner = handle_ffi_arg(dflt,spec,valptr,argptr);
      kno_decref(dflt);
      return inner;}}
  if (basetype == lisp_symbol)
    *valptr = (void *) arg;
  else if (basetype == lispref_symbol) {
    *valptr = (void *) arg;
    kno_incref(arg);}
  else if (basetype == cons_symbol) {
    if (CONSP(arg))
      *valptr = (void *)arg;
    else return ffi_type_error("CONS",arg);}
  else if (basetype == ptrloc_symbol) {
    if (KNO_PRIM_TYPEP(arg,kno_rawptr_type)) {
      struct KNO_RAWPTR *raw=(kno_rawptr)arg;
      if (raw->typetag != typetag)
	return ffi_type_error("rawpointer",arg);
      *valptr=&(raw->ptrval);}
    else return ffi_type_error("rawpointer",arg);}
  else if (spec == ptr_symbol) {
    if (KNO_PRIM_TYPEP(arg,kno_rawptr_type)) {
      struct KNO_RAWPTR *raw=(kno_rawptr)arg;
      *valptr=raw->ptrval;}
    else if ( (nullable) &&
              ( (FALSEP(arg)) || (KNO_DEFAULTP(arg)) ||
                (EMPTYP(arg)) || (NILP(arg) ) ||
                (VOIDP(arg) ) ) )
      *valptr = (void *)NULL;
    else return ffi_type_error("rawpointer",arg);}
  else if ( basetype == ptr_symbol) {
    if (KNO_PRIM_TYPEP(arg,kno_rawptr_type)) {
      struct KNO_RAWPTR *raw=(kno_rawptr)arg;
      lispval typetag = kno_get(spec,typetag_symbol,KNO_VOID);
      if ( KNO_EQUALP(typetag,raw->typetag) )
        *valptr=raw->ptrval;
      else if (KNO_SYMBOLP(typetag))
        return ffi_type_error(KNO_SYMBOL_NAME(typetag),arg);
      else {
        kno_decref(typetag);
        return ffi_type_error("tagged pointer",arg);}}
    else if ( (nullable) &&
              ( (FALSEP(arg)) || (DEFAULTP(arg)) ||
                (EMPTYP(arg)) || (NILP(arg) ) ) )
      *valptr = (void *)NULL;
    else {
      u8_byte buf[256];
      return KNO_ERR(-1,kno_FFI_TypeError,"handle_ffi_arg",
                     u8_bprintf(buf,"%q",spec),
                     arg);}}
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
    else if (spec == size_t_symbol) {
      if ((ival <= LONG_MAX) && (ival >= LONG_MIN ))
        *((ssize_t *)valptr) = ival;
      else return ffi_type_error("ssize_t(C)",arg);}
    else if (spec == float_symbol) {
      float f=(float)ival;
      *((float *)valptr) = f;}
    else if (spec == float_symbol) {
      double f=(double)ival;
      *((double *)valptr) = f;}
    else if (spec == time_t_symbol) {
      time_t tval = (time_t)ival;
      *((time_t *)valptr) = tval;}
    else return KNO_ERR(-1,"BadIntType","handle_ffi_arg",NULL,spec);
    *argptr = valptr;
    return 1;}
  else if (KNO_FLONUMP(arg)) {
    if (basetype == float_symbol) {
      float f=(float)KNO_FLONUM(arg);
      *((float *)valptr)=f;}
    else if (basetype == double_symbol) {
      double f=(float)KNO_FLONUM(arg);
      *((double *)valptr)=f;}
    else if (SYMBOLP(basetype)) {
      kno_type_error(SYM_NAME(basetype),"handle_ffi_arg",spec);
      return -1;}
    else {
      kno_type_error("ctype","handle_ffi_arg",spec);
      return -1;}
    *argptr = valptr;
    return 1;}
  else if (STRINGP(arg)) {
    if (basetype == string_symbol) {
      *valptr = (void *)CSTRING(arg);
      *argptr = valptr;
      return 1;}
    else if (basetype == strlen_symbol) {
      size_t len = KNO_STRLEN(arg);
      *valptr = (void *)len;
      *argptr = valptr;
      return 1;}
    else if (SYMBOLP(spec))
      return ffi_type_error(SYM_NAME(spec),arg);
    else return ffi_type_error("ctype",arg);}
  else if (PACKETP(arg)) {
    if (basetype == packet_symbol) {
      *valptr = (void *)CSTRING(arg);
      *argptr = valptr;
      return 1;}
    else if (basetype == packetlen_symbol) {
      size_t len = KNO_PACKET_LENGTH(arg);
      *valptr = (void *)len;
      *argptr = valptr;
      return 1;}
    else if (SYMBOLP(spec))
      return ffi_type_error(SYM_NAME(spec),arg);
    else return ffi_type_error("ctype",arg);}
  else if ( (basetype == time_t_symbol) &&
            (KNO_TYPEP(arg,kno_timestamp_type))  ) {
    struct KNO_TIMESTAMP *tstamp = (kno_timestamp) arg;
    time_t tval = tstamp->u8xtimeval.u8_tick;
    *((time_t *)valptr) = tval;
    return 1;}
  else if ( (nullable) &&
            ( (basetype == string_symbol) ||
              (basetype == packet_symbol) ) ) {
    *valptr = (void *)CSTRING(arg);
    *argptr = valptr;
    return 1;}
  else {
    u8_byte buf[256];
    return KNO_ERR(-1,kno_FFI_TypeError,"handle_ffi_arg",
                   u8_bprintf(buf,"%q",spec),
                   arg);}
  *argptr=valptr;
  return 1;
}

KNO_EXPORT lispval kno_ffi_call(struct KNO_FUNCTION *fn,int n,lispval *args)
{
  if (KNO_CONS_TYPE(fn) == kno_ffi_type) {
    struct KNO_FFI_PROC *proc = (struct KNO_FFI_PROC *) fn;
    lispval *argspecs = proc->ffi_argspecs;
    lispval return_spec = proc->ffi_return_spec;
    lispval return_type = (SYMBOLP(return_spec)) ? (return_spec) :
      (TABLEP(return_spec)) ?
      (kno_getopt(return_spec,basetype_symbol,VOID)) :
      (VOID);
    int mallocdp = (TABLEP(return_spec)) &&
      kno_testopt(return_spec,mallocd_symbol,KNO_VOID);
    if (VOIDP(return_type))
      return kno_err("BadFFIReturnSpec","kno_ffi_call",fn->fcn_name,return_spec);
    void *vals[10], *argptrs[10];
    int arity = proc->fcn_arity;
    int i = 0;
    i = 0; while (i<arity) {
      int rv = handle_ffi_arg(args[i],argspecs[i],&(vals[i]),&(argptrs[i]));
      if (rv<0) return KNO_ERROR;
      else i++;}
    if ( (return_type == lisp_symbol) || (return_type == lispref_symbol) ) {
      lispval result = KNO_NULL;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&result,argptrs);
      if (result == KNO_NULL)
        return kno_err(kno_NullPtr,"kno_ffi_call",NULL,VOID);
      else if (return_type == lispref_symbol)
        return kno_incref(result);
      else return result;}
    else if ( (KNO_TABLEP(return_spec)) &&
              (kno_test(return_spec,typetag_symbol,KNO_VOID )) ) {
      lispval return_type = kno_get(return_spec,typetag_symbol,KNO_VOID);
      if ( ! (VOIDP(return_type)) ) {}
      else if (fn->fcn_name)
        return_type = kno_intern(fn->fcn_name);
      else return_type = ffi_result_symbol;
      void *value = NULL;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&value,argptrs);
      kno_raw_recyclefn recycler = (mallocdp) ? (_u8_free) : (NULL);
      ssize_t len = kno_getfixopt(return_spec,"length",-1);
      lispval wv = kno_wrap_pointer(value,len,recycler,return_type,NULL);
      kno_decref(return_type);
      return wv;}
    else if (return_type == string_symbol) {
      u8_string stringval = NULL;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&stringval,argptrs);
      if (stringval)
        if (mallocdp)
          return kno_init_string(NULL,-1,stringval);
        else return kno_make_string(NULL,-1,stringval);
      else return KNO_FALSE;}
    else if (return_type == strcpy_symbol) {
      u8_string stringval = NULL;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&stringval,argptrs);
      if (stringval) {
        lispval v = kno_make_string(NULL,-1,stringval);
        if (mallocdp) u8_free(stringval);
        return v;}
      else return KNO_FALSE;}
    else if (return_type == int_symbol) {
      int intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return KNO_INT(intval);}
    else if (return_type == uint_symbol) {
      unsigned int intval = 0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return KNO_INT(intval);}
    else if (return_type == short_symbol) {
      short intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return KNO_INT(intval);}
    else if (return_type == ushort_symbol) {
      unsigned short intval = 0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return KNO_INT(intval);}
    else if (return_type == char_symbol) {
      char intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return KNO_INT(intval);}
    else if (return_type == uchar_symbol) {
      unsigned char intval = 0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return KNO_INT(intval);}
    else if (return_type == byte_symbol) {
      unsigned char intval = 0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return KNO_INT(intval);}
    else if ((return_type == long_symbol)||(return_type == size_t_symbol)) {
      long long intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return KNO_INT(intval);}
    else if (return_type == ulong_symbol) {
      unsigned long long intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      return KNO_INT(intval);}
    else if (return_type == double_symbol) {
      double dval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&dval,argptrs);
      return kno_make_flonum(dval);}
    else if (return_type == float_symbol) {
      float fval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&fval,argptrs);
      return kno_make_flonum(fval);}
    else if (return_type == time_t_symbol) {
      time_t tval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&tval,argptrs);
      if (tval < 0)
        return KNO_FALSE;
      else return kno_time2timestamp(tval);}
    else return VOID;}
  else return kno_err(_("Not an foreign function interface"),
                      "ffi_caller",u8_strdup(fn->fcn_name),VOID);
}

/* Generic object methods */

static void recycle_ffi_proc(struct KNO_RAW_CONS *c)
{
  struct KNO_FFI_PROC *ffi = (struct KNO_FFI_PROC *)c;
  int free_flags = ffi->fcn_free;
  if (ffi->fcn_name) u8_free(ffi->fcn_name);
  if (ffi->fcn_filename) u8_free(ffi->fcn_filename);
  if ( (ffi->fcn_doc) && (free_flags&KNO_FCN_FREE_DOC) )
    u8_free(ffi->fcn_doc);
  u8_free(ffi->ffi_argtypes);
  if (!(KNO_STATIC_CONSP(ffi))) u8_free(ffi);
}

static int unparse_ffi_proc(u8_output out,lispval x)
{
  struct KNO_FFI_PROC *ffi = (struct KNO_FFI_PROC *)x;
  lispval *argspecs=ffi->ffi_argspecs;
  int i=0, n=ffi->fcn_arity;
  u8_printf(out,"#<FFI '%s'(",ffi->fcn_name);
  while (i<n) {
    if (i>0)
      u8_printf(out,",%q",argspecs[i++]);
    else u8_printf(out,"%q",argspecs[i++]);}
  u8_printf(out,") #!%llx>",KNO_LONGVAL( ffi));
  return 1;
}

static ssize_t write_ffi_dtype(struct KNO_OUTBUF *out,lispval x)
{
  int n_elts=0;
  struct KNO_FFI_PROC *fcn = (struct KNO_FFI_PROC *)x;
  unsigned char buf[200], *tagname="%FFI";
  struct KNO_OUTBUF tmp = { 0 };
  KNO_INIT_OUTBUF(&tmp,buf,100,0);
  kno_write_byte(&tmp,dt_compound);
  kno_write_byte(&tmp,dt_symbol);
  kno_write_4bytes(&tmp,strlen(tagname));
  kno_write_bytes(&tmp,tagname,strlen(tagname));
  if (fcn->fcn_name) n_elts++;
  if (fcn->fcn_filename) n_elts++;
  kno_write_4bytes(&tmp,n_elts);
  if (fcn->fcn_name) {
    size_t len=strlen(fcn->fcn_name);
    kno_write_byte(&tmp,dt_symbol);
    kno_write_4bytes(&tmp,len);
    kno_write_bytes(&tmp,fcn->fcn_name,len);}
  if (fcn->fcn_filename) {
    size_t len=strlen(fcn->fcn_filename);
    kno_write_byte(&tmp,dt_string);
    kno_write_4bytes(&tmp,len);
    kno_write_bytes(&tmp,fcn->fcn_filename,len);}
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  kno_write_bytes(out,tmp.buffer,n_bytes);
  kno_close_outbuf(&tmp);
  return n_bytes;
}

KNO_EXPORT long long ffitest_ipi(int x,int y)
{
  long long result = x+y;
  return result;
}

KNO_EXPORT int ffitest_sps(short x,short y)
{
  int result = x + y;
  return result;
}

KNO_EXPORT double ffitest_fpf(float x,float y)
{
  double result = x + y;
  return result;
}

KNO_EXPORT size_t ffitest_strlen(u8_string s)
{
  return strlen(s);
}

KNO_EXPORT int ffitest_chr(u8_string s,int off)
{
  int len=strlen(s);
  if (off<0) return -1;
  else if (off>=len) return -1;
  else return s[off];
}


/* Initializations */

KNO_EXPORT void kno_init_ffi_c()
{
  kno_type_names[kno_ffi_type]="foreign-function";
  kno_dtype_writers[kno_ffi_type]=write_ffi_dtype;

  kno_unparsers[kno_ffi_type]=unparse_ffi_proc;
  kno_recyclers[kno_ffi_type]=recycle_ffi_proc;

  kno_isfunctionp[kno_ffi_type]=1;
  kno_applyfns[kno_ffi_type]=(kno_applyfn)kno_ffi_call;

  init_symbols();

  u8_register_source_file(_FILEINFO);
}

#else /* HAVE_FFI_H && HAVE_LIBFFI */
KNO_EXPORT void kno_init_ffi_c()
{
  kno_type_names[kno_ffi_type]="foreign-function";
  kno_dtype_writers[kno_ffi_type]=NULL;

  init_symbols();

  u8_register_source_file(_FILEINFO);
}
#endif

static void init_symbols()
{
  void_symbol = kno_intern("void");
  double_symbol = kno_intern("double");
  float_symbol = kno_intern("float");
  uint_symbol = kno_intern("uint");
  int_symbol = kno_intern("int");
  ushort_symbol = kno_intern("ushort");
  short_symbol = kno_intern("short");
  ulong_symbol = kno_intern("ulong");
  long_symbol = kno_intern("long");
  uchar_symbol = kno_intern("uchar");
  byte_symbol = kno_intern("byte");
  size_t_symbol = kno_intern("size_t");
  char_symbol = kno_intern("char");
  string_symbol = kno_intern("string");
  strcpy_symbol = kno_intern("strcpy");
  packet_symbol = kno_intern("packet");
  strlen_symbol = kno_intern("strlen");
  packetlen_symbol = kno_intern("packetlen");
  ptr_symbol = kno_intern("ptr");
  cons_symbol = kno_intern("cons");
  lisp_symbol = kno_intern("lisp");
  lispref_symbol = kno_intern("lispref");
  time_t_symbol = kno_intern("time_t");
  null_symbol = kno_intern("null");
  nullable_symbol = kno_intern("nullable");
  mallocd_symbol = kno_intern("mallocd");

  basetype_symbol = kno_intern("basetype");
  typetag_symbol = kno_intern("typetag");

  ptrloc_symbol = kno_intern("ptrloc");
  loclen_symbol = kno_intern("loclen");

  ffi_result_symbol = kno_intern("result");
}

