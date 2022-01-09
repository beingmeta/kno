/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
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

#define FFI_MAX_ARGS 17

u8_condition kno_ffi_BadTypeinfo=_("Bad FFI type info");
u8_condition kno_ffi_BadABI=_("Bad FFI ABI value");
u8_condition kno_ffi_FFIError=_("Unknown libffi error");
u8_condition kno_ffi_NoFunction=_("Unknown foreign function");
u8_condition kno_FFI_TypeError=(_("FFI type error"));

static lispval ffi_lispifier_type, ffi_cifier_type;

static lispval uint_symbol, int_symbol, ushort_symbol, short_symbol;
static lispval ulong_symbol, long_symbol, uchar_symbol, char_symbol;
static lispval string_symbol, stringptr_symbol, packet_symbol, ptr_symbol, cons_symbol;
static lispval float_symbol, double_symbol, size_t_symbol, lisp_symbol;
static lispval lispref_symbol, strcpy_symbol, void_symbol, mallocd_symbol;
static lispval byte_symbol, basetype_symbol, typetag_symbol;
static lispval ffi_result_symbol, time_t_symbol, null_symbol;
static lispval strlen_symbol, packetlen_symbol, ptrloc_symbol, loclen_symbol;
static lispval nullable_symbol, ffitype_symbol, intptr_symbol, resultptr_symbol;
static lispval stringlist_symbol;

static lispval ffi_types = KNO_EMPTY;

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

#if (SIZEOF_LONG == 4)
#define SIGNED_LONG (&ffi_type_sint32)
#define UNSIGNED_LONG (&ffi_type_uint32)
#else
#define SIGNED_LONG (&ffi_type_sint64)
#define UNSIGNED_LONG (&ffi_type_uint64)
#endif

#if (SIZEOF_SIZE_T == 4)
#define SIGNED_SIZE (&ffi_type_sint32)
#define UNSIGNED_SIZE (&ffi_type_uint32)
#else
#define SIGNED_SIZE (&ffi_type_sint64)
#define UNSIGNED_SIZE (&ffi_type_uint64)
#endif

static ffi_type *get_ffi_type(lispval typespec)
{
  if (KNO_TYPEINFOP(typespec)) {
    struct KNO_TYPEINFO *info = (kno_typeinfo) typespec;
    lispval ffi_typetag = kno_get(info->type_props,ffitype_symbol,KNO_VOID);
    if (KNO_VOIDP(ffi_typetag))
      return KNO_ERR(NULL,"NotAnFFIType","get_ffi_type",NULL,typespec);
    else return get_ffi_type(ffi_typetag);}
  else if (TABLEP(typespec)) {
    lispval typename = kno_getopt(typespec,ffitype_symbol,VOID);
    if (VOIDP(typename))
      return KNO_ERR(NULL,"NoFFItype","get_ffi_type",NULL,typespec);
    else if (!(SYMBOLP(typename)))
      return KNO_ERR(NULL,"BadFFItype","get_ffi_type",NULL,typename);
    else return get_ffi_type(typename);}
  else if (!(SYMBOLP(typespec)))
    return KNO_ERR(NULL,"BadFFItype","get_ffi_type",NULL,typespec);
  else if (typespec == lisp_symbol)
    return &ffi_type_uint64;
  else if (typespec == ulong_symbol)
    return (UNSIGNED_LONG);
  else if (typespec == long_symbol)
    return (SIGNED_LONG);
  else if ( (typespec == size_t_symbol) ||
	    (typespec == packetlen_symbol) || (typespec == strlen_symbol) ||
	    (typespec == loclen_symbol) )
    return (SIGNED_SIZE);
  else if (typespec == uint_symbol)
    return &ffi_type_uint32;
  else if (typespec == int_symbol)
    return &ffi_type_sint32;
  else if (typespec == ushort_symbol)
    return &ffi_type_uint16;
  else if (typespec == short_symbol)
    return &ffi_type_sint16;
  else if ( (typespec == uchar_symbol) || (typespec == byte_symbol) )
    return &ffi_type_uint8;
  else if (typespec == char_symbol)
    return &ffi_type_sint8;
  else if (typespec == float_symbol)
    return &ffi_type_float;
  else if (typespec == double_symbol)
    return &ffi_type_double;
  else if (typespec == void_symbol)
    return &ffi_type_void;
  else if ( (typespec == ptr_symbol) || (typespec == cons_symbol) ||
	    (typespec == string_symbol) || (typespec == stringptr_symbol) || 
	    (typespec == packet_symbol) ||
	    (typespec == null_symbol) || (typespec == ptrloc_symbol) ||
	    (typespec == intptr_symbol) || (typespec == resultptr_symbol) ||
	    (typespec == stringlist_symbol))
    return &ffi_type_pointer;
  else if ( (typespec == time_t_symbol) && (sizeof(time_t) == 8) )
    return &ffi_type_sint64;
  else if ( (typespec == time_t_symbol) && (sizeof(time_t) == 4) )
    return &ffi_type_sint32;
  else {
    struct KNO_TYPEINFO *info = kno_probe_typeinfo(typespec);
    if (info == NULL) return KNO_ERR(NULL,"BadFFItype","get_ffi_type",NULL,typespec);
    lispval ffi_typetag = info->typetag;
    if (KNO_VOIDP(ffi_typetag))
      return KNO_ERR(NULL,"NotAnFFIType","get_ffi_type",NULL,typespec);
    else return KNO_ERR(NULL,"BadFFItype","get_ffi_type",NULL,typespec);}
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
  void *mod_arg = (filename == NULL) ? ((void *)NULL) : (u8_dynamic_load(filename));
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
    /* A false argspec indicates that there is a C arg which is actually provided by
       the previous spec */
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
    kno_incref_elts(savespecs,arity);
    proc->fcn_name = u8_strdup(name);
    proc->fcn_filename = u8dup(filename);
    proc->fcn_call_width = proc->fcn_arity = arity;
    proc->fcn_arginfo_len = 0;
    proc->fcn_argnames = NULL;
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
    u8_byte _details[512]; 
    u8_string details = u8_bprintf(_details,"%s[%d](%s)",name,arity,filename);
    if (rv == FFI_BAD_TYPEDEF)
      kno_seterr(kno_ffi_BadTypeinfo,"kno_make_ffi_proc",details,KNO_VOID);
    else if (rv == FFI_BAD_ABI)
      kno_seterr(kno_ffi_BadABI,"kno_make_ffi_proc",details,KNO_VOID);
    else kno_seterr(kno_ffi_FFIError,"kno_make_ffi_proc",details,KNO_VOID);
    u8_free(savespecs);
    u8_free(ffi_argtypes);
    return NULL;}
}

static int ffi_type_error(u8_context expecting,lispval arg)
{
  return KNO_ERR(-1,kno_FFI_TypeError,expecting,NULL,arg);
}

typedef void (*ffi_arg_releasefn)
(lispval *result,int i,lispval *specs,void **argptr,void **cell,void **scratch);

static void handle_resultptr(lispval *resultp,int i,
			     lispval *specs,void **argptrs,void **cells,void **scratch)
{
  lispval result = *resultp;
  if (KNO_ABORTED(result)) return;
  lispval spec = specs[i];
  if (scratch[i]) {
    lispval result_tag = kno_getopt(spec,typetag_symbol,KNO_VOID);
    ssize_t len = kno_getfixopt(spec,"length",-1);
    lispval wrapped = kno_wrap_pointer(scratch[i],len,NULL,result_tag,NULL);
    *resultp=wrapped;}
  else *resultp=KNO_FALSE;
}

static void free_mallocd_arg(lispval *resultp,int i,
			     lispval *specs,void **argptrs,
			     void **cells,void **scratch)
{
  if (scratch[i]) u8_free(scratch[i]);
}

static int handle_ffi_arg(lispval arg,lispval spec,int i,
			  ffi_arg_releasefn *releasefns,
			  void **argptrs,
			  void **cells,
			  void **scratch)
{
  void **valptr=&(cells[i]);
  lispval ffitype = (SYMBOLP(spec)) ? (spec) :
    (TABLEP(spec)) ? (kno_getopt(spec,ffitype_symbol,VOID)) :
    (VOID);
  lispval typetag = (TABLEP(spec)) ?
    (kno_getopt(spec,typetag_symbol,VOID)) :
    (KNO_VOID);
  int nullable = (TABLEP(spec)) && (kno_testopt(spec,nullable_symbol,VOID));
  int defaultp = (TABLEP(spec)) && (kno_testopt(spec,KNOSYM_DEFAULT,VOID));
  if (ffitype==stringptr_symbol) {
    ffitype=string_symbol;
    nullable=1;}
  if (VOIDP(ffitype)) {
    u8_byte buf[256];
    kno_seterr("BadTypeSpec","handle_ffi_arg",
               u8_bprintf(buf,"%q",arg),spec);}
  if ( (defaultp) && ( (VOIDP(arg)) || (KNO_DEFAULTP(arg)) ) ) {
    if (!(VOIDP(arg))) {
      lispval dflt = kno_getopt(spec,KNOSYM_DEFAULT,KNO_VOID);
      int next = handle_ffi_arg(dflt,spec,i,releasefns,argptrs,cells,scratch);
      kno_decref(dflt);
      return next;}}
  if (ffitype == lisp_symbol)
    *(valptr++) = (void *) arg;
  else if (ffitype == lispref_symbol) {
    *(valptr++) = (void *) arg;
    kno_incref(arg);}
  else if (ffitype == cons_symbol) {
    if (CONSP(arg))
      *(valptr++) = (void *)arg;
    else return ffi_type_error("CONS",arg);}
  else if (ffitype == ptrloc_symbol) {
    if (KNO_PRIM_TYPEP(arg,kno_rawptr_type)) {
      struct KNO_RAWPTR *raw=(kno_rawptr)arg;
      if (raw->typetag != typetag)
	return ffi_type_error("rawpointer",arg);
      *(valptr++) = &(raw->ptrval);}
    else return ffi_type_error("rawpointer",arg);}
  else if (ffitype == stringlist_symbol) {
    int n = ( (KNO_EMPTYP(arg)) || (KNO_EMPTY_LISTP(arg)) ) ? (0) :
      (KNO_STRINGP(arg)) ? (1) : (KNO_SYMBOLP(arg)) ? (1) :
      (KNO_CHOICEP(arg)) ? (KNO_CHOICE_SIZE(arg)) :
      (KNO_VECTORP(arg)) ? (KNO_VECTOR_LENGTH(arg)) :
      (-1);
    if (n<0) return ffi_type_error("stringlist",arg);
    else if (n==0)
      *(valptr++) = NULL;
    else {
      u8_string *list = u8_alloc_n(n+1,u8_string), *write=list;
      if (KNO_STRINGP(arg))
	list[0]=KNO_CSTRING(arg);
      else if (KNO_SYMBOLP(arg))
	list[0]=KNO_SYMBOL_NAME(arg);
      else if (KNO_CHOICEP(arg)) {
	KNO_DO_CHOICES(elt,arg) {
	  if (KNO_STRINGP(elt))
	    *write++=KNO_CSTRING(elt);
	  else if (KNO_SYMBOLP(elt))
	    *write++=KNO_SYMBOL_NAME(elt);
	  /* Error ? */
	  else NO_ELSE;}}
      else if (KNO_CHOICEP(arg)) {
	KNO_DO_CHOICES(elt,arg) {
	  if (KNO_STRINGP(elt))
	    *write++=KNO_CSTRING(elt);
	  else if (KNO_SYMBOLP(elt))
	    *write++=KNO_SYMBOL_NAME(elt);
	  /* Error ? */
	  else NO_ELSE;}}
      else if (KNO_VECTORP(arg)) {
	int i = 0, len = KNO_VECTOR_LENGTH(arg);
	lispval *elts = KNO_VECTOR_ELTS(arg);
	while (i<len) {
	  lispval elt = elts[i++];
	  if (KNO_STRINGP(elt))
	    *write++=KNO_CSTRING(elt);
	  else if (KNO_SYMBOLP(elt))
	    *write++=KNO_SYMBOL_NAME(elt);
	  /* Error ? */
	  else NO_ELSE;}}
      else NO_ELSE;
      *write++=NULL;
      scratch[i]=list;
      releasefns[i]=free_mallocd_arg;
      *(valptr++)=list;}}
  else if (ffitype == resultptr_symbol) {
    scratch[i]=NULL;
    *(valptr++)=&(scratch[i]);
    releasefns[i]=handle_resultptr;}
  else if (spec == ptr_symbol) {
    if (KNO_PRIM_TYPEP(arg,kno_rawptr_type)) {
      struct KNO_RAWPTR *raw=(kno_rawptr)arg;
      *(valptr++) = raw->ptrval;}
    else if ( (nullable) &&
              ( (FALSEP(arg)) || (KNO_DEFAULTP(arg)) ||
                (EMPTYP(arg)) || (NILP(arg) ) ||
                (VOIDP(arg) ) ) )
      *(valptr++) = (void *)NULL;
    else return ffi_type_error("rawpointer",arg);}
  else if ( ffitype == ptr_symbol) {
    if (KNO_PRIM_TYPEP(arg,kno_rawptr_type)) {
      struct KNO_RAWPTR *raw=(kno_rawptr)arg;
      lispval typetag = kno_get(spec,typetag_symbol,KNO_VOID);
      if ( KNO_EQUALP(typetag,raw->typetag) )
        *(valptr++) = raw->ptrval;
      else if (KNO_SYMBOLP(typetag))
        return ffi_type_error(KNO_SYMBOL_NAME(typetag),arg);
      else {
        kno_decref(typetag);
        return ffi_type_error("tagged pointer",arg);}}
    else if ( (nullable) &&
              ( (FALSEP(arg)) || (DEFAULTP(arg)) ||
                (EMPTYP(arg)) || (NILP(arg) ) ) )
      *(valptr++) = (void *)NULL;
    else {
      u8_byte buf[256];
      return KNO_ERR(-1,kno_FFI_TypeError,"handle_ffi_arg",
                     u8_bprintf(buf,"%q",spec),
                     arg);}}
  else if (FIXNUMP(arg)) {
    long long ival = FIX2INT(arg);
    if (spec == int_symbol) {
      if ((ival <= INT_MAX) && (ival >= INT_MIN))
        *((int *)(valptr++)) = ival;
      else return ffi_type_error("int(C)",arg);}
    else if (spec == uint_symbol) {
      if ((ival <= UINT_MAX) && (ival >= 0 ))
        *((unsigned int *)(valptr++)) = ival;
      else return ffi_type_error("uint(C)",arg);}
    else if (spec == long_symbol) {
      if ((ival <= LONG_MAX) && (ival >= LONG_MIN ))
        *((long *)(valptr++)) = ival;
      else return ffi_type_error("long(C)",arg);}
    else if (spec == ulong_symbol) {
      if ((ival <= ULONG_MAX) && (ival >= 0 ))
        *((unsigned long *)(valptr++)) = ival;
      else return ffi_type_error("ulong(C)",arg);}
    else if (spec == short_symbol) {
      if ((ival <= SHRT_MAX) && (ival >= SHRT_MIN ))
        *((short *)(valptr++)) = ival;
      else return ffi_type_error("short(C)",arg);}
    else if (spec == ushort_symbol) {
      if ((ival <= USHRT_MAX) && (ival >= 0 ))
        *((unsigned short *)(valptr++)) = ival;
      else return ffi_type_error("ushort(C)",arg);}
    else if (spec == char_symbol) {
      if ((ival <= CHAR_MAX) && (ival >= CHAR_MIN ))
        *((char *)(valptr++)) = ival;
      else return ffi_type_error("char(C)",arg);}
    else if (spec == uchar_symbol) {
      if ((ival <= UCHAR_MAX) && (ival >= 0 ))
        *((unsigned char *)(valptr++)) = ival;
      else return ffi_type_error("uchar(C)",arg);}
    else if (spec == byte_symbol) {
      if ((ival <= UCHAR_MAX) && (ival >= 0 ))
        *((unsigned char *)(valptr++)) = ival;
      else return ffi_type_error("byte/uchar(C)",arg);}
    else if (spec == size_t_symbol) {
      if ((ival <= LONG_MAX) && (ival >= LONG_MIN ))
        *((ssize_t *)(valptr++)) = ival;
      else return ffi_type_error("ssize_t(C)",arg);}
    else if (spec == float_symbol) {
      float f=(float)ival;
      *((float *)(valptr++)) = f;}
    else if (spec == float_symbol) {
      double f=(double)ival;
      *((double *)(valptr++)) = f;}
    else if (spec == time_t_symbol) {
      time_t tval = (time_t)ival;
      *((time_t *)(valptr++)) = tval;}
    else if (spec == intptr_symbol) {
      if ((ival <= INT_MAX) && (ival >= INT_MIN)) {
	*((int *)&(scratch[i]))=ival;
        *((int **)(valptr++)) = ((int *)&(scratch[i]));}
      else return ffi_type_error("int(C)",arg);}
    else return KNO_ERR(-1,"BadIntType","handle_ffi_arg",NULL,spec);}
  else if (KNO_FLONUMP(arg)) {
    if (ffitype == float_symbol) {
      float f=(float)KNO_FLONUM(arg);
      *((float *)(valptr++))=f;}
    else if (ffitype == double_symbol) {
      double f=(float)KNO_FLONUM(arg);
      *((double *)(valptr++))=f;}
    else if (SYMBOLP(ffitype)) {
      kno_type_error(SYM_NAME(ffitype),"handle_ffi_arg",spec);
      return -1;}
    else {
      kno_type_error("ctype","handle_ffi_arg",spec);
      return -1;}}
  else if (STRINGP(arg)) {
    if (ffitype == string_symbol) {
      *(valptr++) = (void *)CSTRING(arg);}
    else if (ffitype == strlen_symbol) {
      size_t len = KNO_STRLEN(arg);
      *(valptr++) = (void *)len;}
    else if (SYMBOLP(spec))
      return ffi_type_error(SYM_NAME(spec),arg);
    else return ffi_type_error("ctype",arg);}
  else if (PACKETP(arg)) {
    if (ffitype == packet_symbol) {
      *(valptr++) = (void *)CSTRING(arg);}
    else if (ffitype == packetlen_symbol) {
      size_t len = KNO_PACKET_LENGTH(arg);
      *(valptr++) = (void *)len;}
    else if (SYMBOLP(spec))
      return ffi_type_error(SYM_NAME(spec),arg);
    else return ffi_type_error("ctype",arg);}
  else if ( (ffitype == time_t_symbol) &&
            (KNO_TYPEP(arg,kno_timestamp_type))  ) {
    struct KNO_TIMESTAMP *tstamp = (kno_timestamp) arg;
    time_t tval = tstamp->u8xtimeval.u8_tick;
    *((time_t *)(valptr++)) = tval;}
  else if ( (nullable) &&
            ( (ffitype == string_symbol) ||
              (ffitype == packet_symbol) ) ) {
    *(valptr++) = (void *)NULL;}
  else {
    u8_byte buf[256];
    return KNO_ERR(-1,kno_FFI_TypeError,"handle_ffi_arg",
                   u8_bprintf(buf,"%q",spec),
                   arg);}
  int width = (valptr-cells)-i;
  int limit = i+width;
  while (i<limit) {
    argptrs[i] = &(cells[i]);
    i++;}
  return valptr-cells;
}

static void release_ffi_args(lispval *result,int n,
			     ffi_arg_releasefn *releasefns,
			     lispval *typespecs,
			     void **argptrs,
			     void **cells,
			     void **scratch)
{
  int i = 0; while (i<n) {
    if (releasefns[i]) {
      releasefns[i](result,i,typespecs,argptrs,cells,scratch);
      releasefns[i]=NULL;
      argptrs[i]=NULL;
      cells[i]=NULL;
      scratch[i]=NULL;}
    i++;}
}

static lispval get_result_type(lispval spec)
{
  if (PAIRP(spec)) return kno_incref(CAR(spec));
  else if (TYPEINFOP(spec)) {
    struct KNO_TYPEINFO *info = (kno_typeinfo) spec;
    lispval ffi_typetag = kno_get(info->type_props,ffitype_symbol,KNO_VOID);
    if (KNO_VOIDP(ffi_typetag))
      return kno_err("NotAnFFIType","get_ffi_type",NULL,spec);
    else return ffi_typetag;}
  else if (TABLEP(spec)) 
    return kno_getopt(spec,ffitype_symbol,VOID);
  else if (SYMBOLP(spec)) {
    if (kno_containsp(spec,ffi_types)) return spec;
    struct KNO_TYPEINFO *info = kno_probe_typeinfo(spec);
    if (info==NULL) return kno_err("UnknownFFIType","get_result_type",NULL,spec);
    lispval ffi_typetag = kno_get(info->type_props,ffitype_symbol,KNO_VOID);
    if (KNO_VOIDP(ffi_typetag))
      return kno_err("NotAnFFIType","get_ffi_type",NULL,spec);
    else return ffi_typetag;}
  else return kno_err("BadFFIResultType","get_result_type",NULL,spec);
}

static lispval get_result_tag(lispval spec)
{
 if (PAIRP(spec)) return kno_incref(CDR(spec));
 else if (TYPEINFOP(spec)) {
   struct KNO_TYPEINFO *info = (kno_typeinfo) spec;
   return info->typetag;}
 else if (TABLEP(spec)) 
   return kno_getopt(spec,typetag_symbol,VOID);
 else if (SYMBOLP(spec)) {
   if (kno_containsp(spec,ffi_types)) return KNO_VOID;
    struct KNO_TYPEINFO *info = kno_probe_typeinfo(spec);
    if (info==NULL) return kno_err("UnknownFFIType","get_result_type",NULL,spec);
    lispval tag = info->typetag;
    if (KNO_VOIDP(tag))
      return kno_err("NotAnFFIType","get_ffi_type",NULL,spec);
    else return tag;}
 else return KNO_VOID;
}

#define LISPIFIERP(x) (KNO_RAW_TYPEP(x,ffi_lispifier_type))
#define CIFIERP(x) (KNO_RAW_TYPEP(x,ffi_cifier_type))

KNO_EXPORT lispval kno_ffi_call(struct KNO_FUNCTION *fn,int n,lispval *args)
{
  if (KNO_CONS_TYPEOF(fn) == kno_ffi_type) {
    struct KNO_FFI_PROC *proc = (struct KNO_FFI_PROC *) fn;
    lispval *argspecs = proc->ffi_argspecs;
    lispval return_spec = proc->ffi_return_spec;
    lispval result_type = get_result_type(return_spec);
    lispval result_tag = (PAIRP(return_spec)) ? (CDR(return_spec)) :
      (get_result_tag(return_spec));
    int mallocdp = (TABLEP(return_spec)) && kno_testopt(return_spec,mallocd_symbol,KNO_VOID);
    if (VOIDP(result_type))
      return kno_err("BadFFIReturnSpec","kno_ffi_call",fn->fcn_name,return_spec);
    else if (!(SYMBOLP(result_type)))
      return kno_err("BadFFIReturnSpec","kno_ffi_call",fn->fcn_name,return_spec);
    else NO_ELSE;
    ffi_arg_releasefn releasefns[FFI_MAX_ARGS] = { NULL };
    lispval typespecs[FFI_MAX_ARGS] = { KNO_FALSE };
    void *argptrs[FFI_MAX_ARGS] = { NULL }, *cells[FFI_MAX_ARGS] = { NULL };
    void *scratch[FFI_MAX_ARGS] = { NULL };
    int arity = proc->fcn_arity;
    int arg_i = 0, call_i = 0; while (arg_i<arity) {
      typespecs[call_i]=argspecs[arg_i];
      int rv = handle_ffi_arg(args[arg_i],argspecs[arg_i],call_i,
			      releasefns,argptrs,cells,scratch);
      if (rv<0) {
	release_ffi_args(NULL,call_i,releasefns,typespecs,argptrs,cells,scratch);
	return KNO_ERROR;}
      call_i=rv;
      arg_i++;}
    lispval result = KNO_NULL;
    if ( (result_type == lisp_symbol) || (result_type == lispref_symbol) ) {
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&result,argptrs);
      if (result == KNO_NULL)
	return kno_err(kno_NullPtr,"kno_ffi_call",NULL,VOID);
      else if (result_type == lispref_symbol)
	kno_incref(result);
      else NO_ELSE;}
    else if (result_type == string_symbol) {
      u8_string stringval = NULL;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&stringval,argptrs);
      if (stringval)
	if (mallocdp)
	  result=kno_init_string(NULL,-1,stringval);
	else result=kno_make_string(NULL,-1,stringval);
      else result=KNO_FALSE;}
    else if (result_type == strcpy_symbol) {
      u8_string stringval = NULL;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&stringval,argptrs);
      if (stringval) {
	result = kno_make_string(NULL,-1,stringval);
	if (mallocdp) u8_free(stringval);}
      else result=KNO_FALSE;}
    else if (result_type == int_symbol) {
      int intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      result=KNO_INT(intval);}
    else if (result_type == uint_symbol) {
      unsigned int intval = 0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      result=KNO_INT(intval);}
    else if (result_type == short_symbol) {
      short intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      result=KNO_INT(intval);}
    else if (result_type == ushort_symbol) {
      unsigned short intval = 0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      result=KNO_INT(intval);}
    else if (result_type == char_symbol) {
      char intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      result=KNO_INT(intval);}
    else if (result_type == uchar_symbol) {
      unsigned char intval = 0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      result=KNO_INT(intval);}
    else if (result_type == byte_symbol) {
      unsigned char intval = 0;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      result=KNO_INT(intval);}
#if (SIZEOF_SIZE_T == 4)
    else if (result_type == size_t_symbol) {
      long intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      result=KNO_INT(intval);}
#else
    else if (result_type == size_t_symbol) {
      long long intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      result=KNO_INT(intval);}
#endif
    else if (result_type == long_symbol) {
      long intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      result=KNO_INT(intval);}
    else if (result_type == ulong_symbol) {
      unsigned long intval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&intval,argptrs);
      result=KNO_INT(intval);}
    else if (result_type == double_symbol) {
      double dval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&dval,argptrs);
      result=kno_make_flonum(dval);}
    else if (result_type == float_symbol) {
      float fval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&fval,argptrs);
      result=kno_make_flonum(fval);}
    else if (result_type == time_t_symbol) {
      time_t tval = -1;
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&tval,argptrs);
      if (tval < 0)
	result=KNO_FALSE;
      else result=kno_time2timestamp(tval);}
    else {
      /* Assume it's a raw pointer */
      void *value = NULL;
      if (KNO_VOIDP(result_tag)) {
	if (fn->fcn_name)
	  result_tag = kno_intern(fn->fcn_name);
	else result_tag = ffi_result_symbol;}
      ffi_call(&(proc->ffi_interface),proc->ffi_dlsym,&value,argptrs);
      if ( value == NULL ) {
	result=KNO_FALSE;
	result_tag=KNO_VOID;}
      else {
	kno_raw_recyclefn recycler = (mallocdp) ? (_u8_free) : (NULL);
	ssize_t len = kno_getfixopt(return_spec,"length",-1);
	result = kno_wrap_pointer(value,len,recycler,result_tag,NULL);
	result_tag=KNO_VOID;}}
    if (!(VOIDP(result_tag))) result = kno_init_compound(NULL,result_tag,0,1,result);
    release_ffi_args(&result,call_i,releasefns,typespecs,argptrs,cells,scratch);
    return result;}
  else return kno_err(_("Not an foreign function"),"ffi_caller",u8_strdup(fn->fcn_name),VOID);
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
  void_symbol = kno_intern("void"); KNO_ADD_TO_CHOICE(ffi_types,void_symbol);
  double_symbol = kno_intern("double"); KNO_ADD_TO_CHOICE(ffi_types,double_symbol);
  float_symbol = kno_intern("float"); KNO_ADD_TO_CHOICE(ffi_types,float_symbol);
  uint_symbol = kno_intern("uint"); KNO_ADD_TO_CHOICE(ffi_types,uint_symbol);
  int_symbol = kno_intern("int"); KNO_ADD_TO_CHOICE(ffi_types,int_symbol);
  ushort_symbol = kno_intern("ushort"); KNO_ADD_TO_CHOICE(ffi_types,ushort_symbol);
  short_symbol = kno_intern("short"); KNO_ADD_TO_CHOICE(ffi_types,short_symbol);
  ulong_symbol = kno_intern("ulong"); KNO_ADD_TO_CHOICE(ffi_types,ulong_symbol);
  long_symbol = kno_intern("long"); KNO_ADD_TO_CHOICE(ffi_types,long_symbol);
  uchar_symbol = kno_intern("uchar"); KNO_ADD_TO_CHOICE(ffi_types,uchar_symbol);
  byte_symbol = kno_intern("byte"); KNO_ADD_TO_CHOICE(ffi_types,byte_symbol);
  size_t_symbol = kno_intern("size_t"); KNO_ADD_TO_CHOICE(ffi_types,size_t_symbol);
  char_symbol = kno_intern("char"); KNO_ADD_TO_CHOICE(ffi_types,char_symbol);
  string_symbol = kno_intern("string"); KNO_ADD_TO_CHOICE(ffi_types,string_symbol);
  stringptr_symbol = kno_intern("stringptr"); KNO_ADD_TO_CHOICE(ffi_types,stringptr_symbol);
  strcpy_symbol = kno_intern("strcpy"); KNO_ADD_TO_CHOICE(ffi_types,strcpy_symbol);
  packet_symbol = kno_intern("packet"); KNO_ADD_TO_CHOICE(ffi_types,packet_symbol);
  strlen_symbol = kno_intern("strlen"); KNO_ADD_TO_CHOICE(ffi_types,strlen_symbol);
  packetlen_symbol = kno_intern("packetlen"); KNO_ADD_TO_CHOICE(ffi_types,packetlen_symbol);
  ptr_symbol = kno_intern("ptr"); KNO_ADD_TO_CHOICE(ffi_types,ptr_symbol);
  cons_symbol = kno_intern("cons"); KNO_ADD_TO_CHOICE(ffi_types,cons_symbol);
  lisp_symbol = kno_intern("lisp"); KNO_ADD_TO_CHOICE(ffi_types,lisp_symbol);
  lispref_symbol = kno_intern("lispref"); KNO_ADD_TO_CHOICE(ffi_types,lispref_symbol);
  time_t_symbol = kno_intern("time_t"); KNO_ADD_TO_CHOICE(ffi_types,time_t_symbol);
  null_symbol = kno_intern("null"); KNO_ADD_TO_CHOICE(ffi_types,null_symbol);
  ptrloc_symbol = kno_intern("ptrloc"); KNO_ADD_TO_CHOICE(ffi_types,ptrloc_symbol);
  loclen_symbol = kno_intern("loclen"); KNO_ADD_TO_CHOICE(ffi_types,loclen_symbol);
  intptr_symbol = kno_intern("intptr"); KNO_ADD_TO_CHOICE(ffi_types,intptr_symbol);
  resultptr_symbol = kno_intern("resultptr"); KNO_ADD_TO_CHOICE(ffi_types,resultptr_symbol);
  stringlist_symbol = kno_intern("stringlist"); KNO_ADD_TO_CHOICE(ffi_types,stringlist_symbol);

  ffi_types = kno_simplify_choice(ffi_types);

  nullable_symbol = kno_intern("nullable");
  ffitype_symbol = kno_intern("ffitype");
  mallocd_symbol = kno_intern("mallocd");

  basetype_symbol = kno_intern("basetype");
  typetag_symbol = kno_intern("typetag");


  ffi_result_symbol = kno_intern("result");
}

