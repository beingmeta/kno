/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#define KNO_INLINE_FCNIDS    (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/eval.h"
#include "kno/ffi.h"
#include "kno/cprims.h"

#include "libu8/u8streamio.h"
#include "libu8/u8printf.h"

#include <sys/resource.h>
#include "kno/profiles.h"

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static lispval source_symbol, void_symbol;

#define GETEVALFN(x) ((kno_evalfn)(kno_fcnid_ref(x)))

DEFC_PRIM("macro?",macrop,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if its argument is an evaluator macro",
	  {"x",kno_any_type,KNO_VOID})
static lispval macrop(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (TYPEP(x,kno_macro_type)) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("lambda?",lambdap,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if its argument is a lambda (a compound procedure)",
	  {"x",kno_any_type,KNO_VOID})
static lispval lambdap(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_LAMBDAP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("evalfn?",evalfnp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if its argument is an interpreter *evalfn* "
	  "(a special form)",
	  {"x",kno_any_type,KNO_VOID})
static lispval evalfnp(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (TYPEP(x,kno_evalfn_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("primitive?",primitivep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if *x* is a primitive function implemented in C",
	  {"x",kno_any_type,KNO_VOID})
static lispval primitivep(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (TYPEP(x,kno_cprim_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("procedure?",procedurep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if *x* is a `procedure`, an "
	  "applicable object with standardized calling metadata",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedurep(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("procedure-name",procedure_name,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the name of the procedure *x*",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedure_name(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(x);
    if (f->fcn_name)
      return kno_mkstring(f->fcn_name);
    else return KNO_FALSE;}
  else if (KNO_APPLICABLEP(x))
    return KNO_FALSE;
  else if (TYPEP(x,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return kno_mkstring(sf->evalfn_name);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_macro_type)) {
    struct KNO_MACRO *m = (kno_macro) x;
    if (m->macro_name)
      return kno_mkstring(m->macro_name);
    else return KNO_FALSE;}
  else return kno_type_error(_("function"),"procedure_name",x);
}

DEF_KNOSYM(enter); DEF_KNOSYM(entry); DEF_KNOSYM(call);
DEF_KNOSYM(exit); DEF_KNOSYM(return); DEF_KNOSYM(result); DEF_KNOSYM(results);
DEF_KNOSYM(args); DEF_KNOSYM(arglist);
DEF_KNOSYM(debug); DEF_KNOSYM(break);
DEF_KNOSYM(custom);

DEFC_PRIM("procedure-tracing",procedure_tracing,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns any tracing flags set for the procedure *proc*",
	  {"proc",kno_any_type,KNO_VOID})
static lispval procedure_tracing(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(x);
    int bits = f->fcn_trace;
    if (bits==0) return KNO_FALSE;
    else {
      lispval results = KNO_EMPTY;
      if (bits&KNO_FCN_TRACE_ENTER) {
	ADD_TO_CHOICE(results,KNOSYM(enter));}
      if (bits&KNO_FCN_TRACE_EXIT) {
	ADD_TO_CHOICE(results,KNOSYM(exit));}
      if (bits&KNO_FCN_TRACE_ARGS) {
	ADD_TO_CHOICE(results,KNOSYM(args));}
      if (bits&KNO_FCN_TRACE_BREAK) {
	ADD_TO_CHOICE(results,KNOSYM(debug));}
      if (bits&KNO_FCN_TRACE_CUSTOM) {
	ADD_TO_CHOICE(results,KNOSYM(custom));}
      return results;}}
  else return KNO_FALSE;
}

static int get_trace_bits(lispval flag)
{
  if ( (flag == KNOSYM(enter)) ||
       (flag == KNOSYM(entry)) ||
       (flag == KNOSYM(call)) )
    return KNO_FCN_TRACE_ENTER;
  else if ( (flag == KNOSYM(exit)) ||
	    (flag == KNOSYM(return)) ||
	    (flag == KNOSYM(result)) ||
	    (flag == KNOSYM(results)) )
    return KNO_FCN_TRACE_EXIT;
  else if ( (flag == KNOSYM(args)) || (flag == KNOSYM(arglist)) )
    return KNO_FCN_TRACE_ARGS;
  else if ( (flag == KNOSYM(debug)) ||
	    (flag == KNOSYM(break)) )
    return KNO_FCN_TRACE_BREAK;
  else if (flag == KNOSYM(custom))
    return KNO_FCN_TRACE_CUSTOM;
  else return -1;
}

DEFC_PRIM("procedure/trace!",procedure_set_trace,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "sets tracing flags for the procedure *proc*. *flags* "
	  "can be a small positive fixnum (<128), a symbolic name "
	  "or a table of trace options and booleans. Trace options "
	  "include `enter`, `exit`, `args`, `break`, and `custom`.",
	  {"proc",kno_any_type,KNO_VOID},
	  {"flags",kno_any_type,KNO_VOID})
static lispval procedure_set_trace(lispval proc,lispval flags)
{
  if (KNO_CHOICEP(proc)) {
    KNO_DO_CHOICES(each,proc) {
      lispval v = procedure_set_trace(each,flags);
      if (KNO_ABORTED(v)) return v;
      else kno_decref(v);}
    return KNO_VOID;}
  if (KNO_FCNIDP(proc)) proc = kno_fcnid_ref(proc);
  if (KNO_FUNCTIONP(proc)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(proc);
    if (KNO_FIXNUMP(flags)) {
      long long intval = KNO_FIX2INT(flags);
      if ( (intval<0) || (intval>=0x100) )
	return kno_err(kno_TypeError,"procedure_set_trace","flags",flags);
      else f->fcn_trace = (unsigned char) (intval&0xFF);}
    else if (KNO_SYMBOLP(flags)) {
      int bits = get_trace_bits(flags);
      if (bits < 0)
	return kno_err(kno_TypeError,"procedure_set_trace","flags",flags);
      else f->fcn_trace |= bits;}
    else if (KNO_TABLEP(flags)) {
      lispval keys = kno_getkeys(flags);
      KNO_DO_CHOICES(key,keys) {
	int bits = get_trace_bits(key);
	if (bits<0)
	  u8_log(LOGWARN,"UnknownTraceFlag","Couldn't recognize %q",key);
	else {
	  lispval val = kno_get(flags,key,KNO_VOID);
	  if ( (KNO_VOIDP(val)) || (KNO_FALSEP(val)) || (KNO_EMPTYP(val)) )
	    f->fcn_trace &= (~bits);
	  else {
	    f->fcn_trace |= bits;
	    kno_decref(val);}}}
      kno_decref(keys);}
    else return kno_err(kno_TypeError,"procedure_set_trace","flags",flags);
    return KNO_VOID;}
  else if (KNO_APPLICABLEP(proc)) {
    u8_log(LOGWARN,"OpaqueFunction",
	   "The applicable function %q is *opaque* and can't be traced",
	   proc);
    return KNO_VOID;}
  else {
    u8_log(LOGERR,kno_NotAFunction,"The object %q can't be traced",proc);
    return KNO_VOID;}
}

DEFC_PRIM("procedure-args",procedure_args,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns a vector of argument names for a procedure.",
	  {"fcn",kno_any_type,KNO_VOID})
static lispval procedure_args(lispval fcn)
{
  if (KNO_FCNIDP(fcn)) fcn = kno_fcnid_ref(fcn);
  if (KNO_FUNCTIONP(fcn)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(fcn);
    if (f->fcn_schema)
      return kno_make_vector(f->fcn_arginfo_len,f->fcn_schema);
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFC_PRIM("procedure-cname",procedure_cname,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the C name (if any) for the implementation of *x*",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedure_cname(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_TYPEP(x,kno_cprim_type)) {
    struct KNO_CPRIM *f = (kno_cprim) x;
    if (f->cprim_name)
      return knostring(f->cprim_name);
    else return KNO_FALSE;}
  else if (KNO_TYPEP(x,kno_ffi_type)) {
    struct KNO_FFI_PROC *f = (kno_ffi_proc) x;
    if (f->fcn_name)
      return knostring(f->fcn_name);
    else return KNO_FALSE;}
  else if (KNO_APPLICABLEP(x))
    return KNO_FALSE;
  else return kno_type_error("function","procedure_cname",x);
}

DEFC_PRIM("procedure-fileinfo",procedure_fileinfo,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns information about the file where the "
	  "procedure *x* is defined. This can include both the "
	  "filename and some hash or version information. "
	  "Note that this also works for evalfns and macros.",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedure_fileinfo(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x)) {
    struct KNO_FUNCTION *f = (kno_function)(x);
    if (f->fcn_filename)
      return kno_mkstring(f->fcn_filename);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_filename)
      return kno_mkstring(sf->evalfn_filename);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_macro_type)) {
    struct KNO_MACRO *m = (kno_macro) x;
    if (m->macro_filename)
      return kno_mkstring(m->macro_filename);
    else return KNO_FALSE;}
  else return kno_type_error(_("function"),"procedure_filename",x);
}

static lispval strip_filename(u8_string s)
{
  u8_string space = strchr(s,' ');
  if (space)
    return kno_extract_string(NULL,s,space);
  else return knostring(s);
}

DEFC_PRIM("procedure-filename",procedure_filename,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the name of the file where the "
	  "procedure *x* is defined. "
	  "Note that this also works for evalfns and macros.",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedure_filename(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x)) {
    struct KNO_FUNCTION *f = (kno_function) x;
    if (f->fcn_filename)
      return strip_filename(f->fcn_filename);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_filename)
      return strip_filename(sf->evalfn_filename);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_macro_type)) {
    struct KNO_MACRO *m = (kno_macro) x;
    if (m->macro_filename)
      return strip_filename(m->macro_filename);
    else return KNO_FALSE;}
  else return kno_type_error(_("function"),"procedure_filename",x);
}

DEFC_PRIM("procedure-module",procedure_moduleid,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the module, if any, where the procedure *x* is defined.",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedure_moduleid(lispval x)
{
  return kno_get_moduleid(x,1);
}

DEFC_PRIM("procedure-symbol",procedure_symbol,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the symbolic name for the procedure *x*. "
	  "Note that this also works for evalfns and macros.",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedure_symbol(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_APPLICABLEP(x)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(x);
    if (f->fcn_name)
      return kno_getsym(f->fcn_name);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return kno_getsym(sf->evalfn_name);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_macro_type)) {
    struct KNO_MACRO *m = (kno_macro) x;
    if (m->macro_name)
      return kno_getsym(m->macro_name);
    else return KNO_FALSE;}
  else return kno_type_error(_("function"),"procedure_symbol",x);
}

DEFC_PRIM("procedure-id",procedure_id,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedure_id(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_APPLICABLEP(x)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(x);
    if (f->fcn_name)
      return kno_intern(f->fcn_name);
    else return kno_incref(x);}
  else if (TYPEP(x,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return kno_intern(sf->evalfn_name);
    else return kno_incref(x);}
  else return kno_incref(x);
}

DEFC_PRIM("documentation",get_documentation,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the documentation for the procedure *x*. "
	  "Note that this also works for evalfns and macros.",
	  {"x",kno_any_type,KNO_VOID})
static lispval get_documentation(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  u8_string doc = kno_get_documentation(x);
  if (doc)
    return kno_wrapstring(doc);
  else return KNO_FALSE;
}

DEFC_PRIM("set-documentation!",set_documentation,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Sets the documentation string for *x*",
	  {"x",kno_any_type,KNO_VOID},
	  {"doc",kno_string_type,KNO_VOID})
static lispval set_documentation(lispval x,lispval doc)
{
  lispval proc = (KNO_FCNIDP(x)) ? (kno_fcnid_ref(x)) : (x);
  kno_lisp_type proctype = KNO_TYPEOF(proc);
  if (kno_isfunctionp[proctype]) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(x);
    u8_string to_free = ( (f->fcn_doc) && (KNO_FCN_FREE_DOCP(f)) ) ?
      (f->fcn_doc) : (NULL);
    f->fcn_doc = u8_strdup(CSTRING(doc));
    f->fcn_free |= KNO_FCN_FREE_DOC;
    if (to_free) u8_free(to_free);
    return VOID;}
  else if (TYPEP(proc,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(proc);
    u8_string prev = sf->evalfn_documentation;
    sf->evalfn_documentation = u8_strdup(CSTRING(doc));
    if (prev) u8_free(prev);
    return VOID;}
  else return kno_err("Not Handled","set_procedure_documentation",NULL,x);
}

DEFC_PRIM("tailable?",procedure_tailablep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *fcn* can be tail called, and "
	  "false otherwise. By default, all procedures are "
	  "tailable when called deterministically.",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedure_tailablep(lispval x)
{
  lispval proc = (KNO_FCNIDP(x)) ? (kno_fcnid_ref(x)) : (x);
  kno_lisp_type proctype = KNO_TYPEOF(proc);
  if (kno_isfunctionp[proctype]) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(x);
    if (FCN_NOTAILP(f))
      return KNO_FALSE;
    else return KNO_TRUE;}
  else return kno_err("Not Handled","procedure_tailablep",NULL,x);
}

DEFC_PRIM("set-tailable!",set_tailablep,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Disables/enables tail calls for *procedure*. "
	  "Returns true if *procedure* supports the option, "
	  "false if it is applicable but doesn't support the option "
	  "and an error otherwise",
	  {"procedure",kno_any_type,KNO_VOID},
	  {"bool",kno_any_type,KNO_VOID})
static lispval set_tailablep(lispval procedure,lispval bool)
{
  lispval proc = (KNO_FCNIDP(procedure)) ?
    (kno_fcnid_ref(procedure)) : (procedure);
  if (KNO_LAMBDAP(proc)) {
    struct KNO_LAMBDA *f = (kno_lambda) proc;
    if (KNO_FALSEP(bool))
      f->fcn_call |= KNO_CALL_NOTAIL;
    else f->fcn_call &= ~KNO_CALL_NOTAIL;
    return KNO_TRUE;}
  else if (KNO_APPLICABLEP(proc))
    return KNO_FALSE;
  else return kno_err("Not Handled","set_procedure_tailable",NULL,proc);
}

DEFC_PRIM("procedure-arity",procedure_arity,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the maximum number of args for the procedure *x*.",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedure_arity(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_APPLICABLEP(x)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(x);
    int arity = f->fcn_arity;
    if (arity<0) return KNO_FALSE;
    else return KNO_INT(arity);}
  else return kno_type_error(_("procedure"),"procedure_arity",x);
}

DEFC_PRIM("non-deterministic?",non_deterministicp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if the procedure *x* is `non-deterministic` meaning "
	  "that it doesn't automatically iterate over choices but takes "
	  "the choices as direct arguments.",
	  {"x",kno_any_type,KNO_VOID})
static lispval non_deterministicp(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_APPLICABLEP(x)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(x);
    if (FCN_NDOPP(f))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return kno_type_error(_("procedure"),"non_deterministicp",x);
}

DEFC_PRIM("synchronized?",synchronizedp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if the procedure *x* is *synchronized*: this means "
	  "that *x* will never be running in more than one thread at the "
	  "same time.",
	  {"x",kno_any_type,KNO_VOID})
static lispval synchronizedp(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (TYPEP(x,kno_lambda_type)) {
    kno_lambda f = (kno_lambda)x;
    if (f->lambda_synchronized)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (KNO_APPLICABLEP(x))
    return KNO_FALSE;
  else return kno_type_error(_("procedure"),"non_deterministicp",x);
}

DEFC_PRIM("procedure-min-arity",procedure_min_arity,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the minimum number of arguments required "
	  "by the procedure *x*",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedure_min_arity(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(x);
    int arity = f->fcn_min_arity;
    return KNO_INT(arity);}
  else if (KNO_APPLICABLEP(x))
    return KNO_FALSE;
  else return kno_type_error(_("procedure"),"procedure_min_arity",x);
}

DEFC_PRIM("procedure-typeinfo",procedure_typeinfo,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns a typeinfo vector for *fcn* if it has "
	  "one, or #f. Note that this doesn't error on "
	  "non-functions but just returns #f",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedure_typeinfo(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x)) {
    struct KNO_FUNCTION *fcn = (kno_function) x;
    int arity = fcn->fcn_arity;
    int info_len = fcn->fcn_arginfo_len;
    int result_len = (arity>info_len) ? (arity) : (info_len);
    if (result_len>=0) {
      lispval result = kno_make_vector(result_len,NULL);
      int i = 0;
      if (fcn->fcn_typeinfo)  {
	lispval *typeinfo = fcn->fcn_typeinfo;
	while (i<info_len) {
	  lispval typeval = typeinfo[i];
	  if (KNO_VOIDP(typeval)) {
	    KNO_VECTOR_SET(result,i,KNO_FALSE);}
	  else if (KNO_CTYPEP(typeval)) {
	    kno_lisp_type typecode = KNO_CTYPE_CODE(typeval);
	    if ( (typecode>=0) && (typecode<kno_max_xtype) ) {
	      u8_string name = kno_type2name(typecode);
	      if (name) {
		KNO_VECTOR_SET(result,i,knostring(name));}
	      else KNO_VECTOR_SET(result,i,KNO_INT(typecode));}
	    else {KNO_VECTOR_SET(result,i,knostring("badtype"));}}
	  else if ( (KNO_SYMBOLP(typeval)) || (KNO_OIDP(typeval)) ) {
	    KNO_VECTOR_SET(result,i,typeval);}
	  else {KNO_VECTOR_SET(result,i,knostring("badtype"));}
	  i++;}}
      while ((arity>0) && (i<arity) ) {
	KNO_VECTOR_SET(result,i,KNO_FALSE);
	i++;}
      return result;}
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFC_PRIM("procedure-defaults",procedure_defaults,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns a vector of default values for *fcn* if "
	  "they're specified, or #f. Note that this doesn't "
	  "error on non-functions but just returns #f",
	  {"x",kno_any_type,KNO_VOID})
static lispval procedure_defaults(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_CPRIMP(x)) {
    struct KNO_CPRIM *fcn = (kno_cprim) x;
    int arity = fcn->fcn_arity;
    if (fcn->fcn_defaults) {
      const lispval *defaults = fcn->fcn_defaults;
      lispval result = kno_make_vector(arity,NULL);
      int i = 0; while (i<arity) {
	lispval dflt = defaults[i];
	if (VOIDP(dflt)) {
	  KNO_VECTOR_SET(result,i,void_symbol);}
	else {
	  kno_incref(dflt);
	  KNO_VECTOR_SET(result,i,dflt);}
	i++;}
      return result;}
    else if (fcn->fcn_arity >= 0) {
      lispval result = kno_make_vector(arity,NULL);
      int i = 0; while (i<arity) {
	KNO_VECTOR_SET(result,i,void_symbol);
	i++;}
      return result;}
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

/* Procedure attribs */


static lispval get_proc_attribs(lispval x,int create)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  kno_lisp_type proctype = KNO_TYPEOF(x);
  if (kno_isfunctionp[proctype]) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(x);
    lispval attribs = f->fcn_attribs;
    if (!(create)) {
      if ((attribs!=KNO_NULL)&&(TABLEP(attribs)))
	return attribs;
      else return VOID;}
    if ((attribs == KNO_NULL)||(!(TABLEP(attribs))))
      f->fcn_attribs = attribs = kno_init_slotmap(NULL,4,NULL);
    return attribs;}
  else return kno_type_error("function","get_proc_attribs",x);
}

DEFC_PRIM("reflect/attribs",get_procedure_attribs,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the attributes object for the procedure *x*.",
	  {"x",kno_any_type,KNO_VOID})
static lispval get_procedure_attribs(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  lispval attribs = get_proc_attribs(x,1);
  if (KNO_ABORTP(attribs))
    return attribs;
  else kno_incref(attribs);
  return attribs;
}

DEFC_PRIM("reflect/set-attribs!",set_procedure_attribs,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Replaces the attributes for the procedure object *x* "
	  "with table.",
	  {"x",kno_any_type,KNO_VOID},
	  {"table",kno_table_type,KNO_VOID})
static lispval set_procedure_attribs(lispval x,lispval value)
{
  kno_lisp_type proctype = KNO_TYPEOF(x);
  if (kno_isfunctionp[proctype]) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(x);
    lispval table = f->fcn_attribs;
    if (table!=KNO_NULL) kno_decref(table);
    f->fcn_attribs = kno_incref(value);
    return VOID;}
  else return kno_err("Not Handled","set_procedure_attribs",NULL,x);
}

DEFC_PRIM("reflect/get",reflect_get,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Returns a meta-property of a procedure",
	  {"x",kno_any_type,KNO_VOID},
	  {"attrib",kno_any_type,KNO_VOID})
static lispval reflect_get(lispval x,lispval attrib)
{
  lispval attribs = get_proc_attribs(x,0);
  if (ABORTED(attribs)) return attribs;
  else if (TABLEP(attribs))
    return kno_get(attribs,attrib,KNO_FALSE);
  else return KNO_FALSE;
}

DEFC_PRIM("reflect/store!",reflect_store,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	  "Sets the meta-property *attrib* of the procedure *x* to *value*.",
	  {"x",kno_any_type,KNO_VOID},
	  {"attrib",kno_any_type,KNO_VOID},
	  {"value",kno_any_type,KNO_VOID})
static lispval reflect_store(lispval x,lispval attrib,lispval value)
{
  lispval attribs = get_proc_attribs(x,1);
  if (ABORTED(attribs)) return attribs;
  else if (TABLEP(attribs)) {
    int rv = kno_store(attribs,attrib,value);
    if (rv<0)
      return KNO_ERROR;
    else return KNO_VOID;}
  else return KNO_ERROR;
}

DEFC_PRIM("reflect/add!",reflect_add,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	  "Adds *value to the meta-property *attrib* of the procedure *x*.",
	  {"x",kno_any_type,KNO_VOID},
	  {"attrib",kno_any_type,KNO_VOID},
	  {"value",kno_any_type,KNO_VOID})
static lispval reflect_add(lispval x,lispval attrib,lispval value)
{
  lispval attribs = get_proc_attribs(x,1);
  if (ABORTED(attribs)) return attribs;
  else if (TABLEP(attribs)) {
    int rv = kno_add(attribs,attrib,value);
    if (rv<0)
      return KNO_ERROR;
    else return KNO_VOID;}
  else return KNO_ERROR;
}

DEFC_PRIM("reflect/drop!",reflect_drop,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "Drops *value from the meta-property *attrib* of the procedure *x*.",
	  {"x",kno_any_type,KNO_VOID},
	  {"attrib",kno_any_type,KNO_VOID},
	  {"value",kno_any_type,KNO_VOID})
static lispval reflect_drop(lispval x,lispval attrib,lispval value)
{
  lispval attribs = get_proc_attribs(x,1);
  if (ABORTED(attribs)) return attribs;
  else if (TABLEP(attribs)) {
    int rv = kno_drop(attribs,attrib,value);
    if (rv<0)
      return KNO_ERROR;
    else return KNO_VOID;}
  else return KNO_ERROR;
}

/* LAMBDA functions */

DEFC_PRIM("lambda-args",lambda_args,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the defined argument list of the lambda *lambda*",
	  {"lambda",kno_any_type,KNO_VOID})
static lispval lambda_args(lispval lambda)
{
  lispval x = kno_fcnid_ref(lambda);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)x;
    return kno_incref(proc->lambda_arglist);}
  else return kno_type_error("lambda","lambda_args",x);
}

DEFC_PRIM("set-lambda-args!",set_lambda_args,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Sets the argument list of *lambda* to *list*.",
	  {"lambda",kno_lambda_type,KNO_VOID},
	  {"list",kno_any_type,KNO_VOID})
static lispval set_lambda_args(lispval lambda,lispval list)
{
  struct KNO_LAMBDA *proc = (kno_lambda)lambda;
  lispval arglist = proc->lambda_arglist;
  proc->lambda_arglist = kno_incref(list);
  kno_decref(arglist);
  return VOID;
}

DEFC_PRIM("lambda-env",lambda_env,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the lexical environment for *lambda*",
	  {"lambda",kno_any_type,KNO_VOID})
static lispval lambda_env(lispval lambda)
{
  lispval x = kno_fcnid_ref(lambda);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)kno_fcnid_ref(x);
    return (lispval) kno_copy_env(proc->lambda_env);}
  else return kno_type_error("lambda","lambda_env",x);
}

DEFC_PRIM("lambda-body",lambda_body,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the defined body for *lambda*",
	  {"lambda",kno_any_type,KNO_VOID})
static lispval lambda_body(lispval lambda)
{
  lispval x = kno_fcnid_ref(lambda);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)kno_fcnid_ref(x);
    return kno_incref(proc->lambda_body);}
  else return kno_type_error("lambda","lambda_body",x);
}

DEFC_PRIM("lambda-entry",lambda_entry,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the optimized body for *lambda*",
	  {"lambda",kno_any_type,KNO_VOID})
static lispval lambda_entry(lispval lambda)
{
  lispval x = kno_fcnid_ref(lambda);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)kno_fcnid_ref(x);
    lispval start = proc->lambda_entry;
    if ( (KNO_CONSP(start)) && (KNO_STATIC_CONSP(start)) )
      return kno_copier(start,KNO_DEEP_COPY);
    else return kno_incref(proc->lambda_entry);}
  else return kno_type_error("lambda","lambda_entry",x);
}

DEFC_PRIM("lambda-source",lambda_source,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the defining expression for *lambda*",
	  {"lambda",kno_any_type,KNO_VOID})
static lispval lambda_source(lispval lambda)
{
  lispval x = kno_fcnid_ref(lambda);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)kno_fcnid_ref(x);
    if (VOIDP(proc->lambda_source)) return KNO_FALSE;
    else return kno_incref(proc->lambda_source);}
  else return kno_type_error("lambda","lambda_source",x);
}

DEFC_PRIM("set-lambda-body!",set_lambda_body,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Sets the body of *lambda* to *new_body*. "
	  "Note that this will delete any optimized body "
	  "for the lambda.",
	  {"lambda",kno_lambda_type,KNO_VOID},
	  {"new_body",kno_any_type,KNO_VOID})
static lispval set_lambda_body(lispval lambda,lispval new_body)
{
  struct KNO_LAMBDA *proc = (kno_lambda)lambda;
  lispval old_body = proc->lambda_body;
  proc->lambda_body = kno_incref(new_body);
  if (proc->lambda_consblock) {
    lispval cb = (lispval) (proc->lambda_consblock);
    kno_decref(cb);
    proc->lambda_consblock = NULL;}
  else if (old_body != proc->lambda_entry) {
    kno_decref(proc->lambda_entry);}
  else NO_ELSE;
  proc->lambda_body  = new_body;
  proc->lambda_entry = new_body;
  kno_decref(old_body);
  return VOID;
}

DEFC_PRIM("set-lambda-entry!",set_lambda_entry,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Sets the optimized body of *lambda* to *optimized*.",
	  {"lambda",kno_lambda_type,KNO_VOID},
	  {"optimized",kno_any_type,KNO_VOID})
static lispval set_lambda_entry(lispval lambda,lispval optimized)
{
  struct KNO_LAMBDA *proc = (kno_lambda)lambda;
  lispval old_entry = proc->lambda_entry;
  proc->lambda_entry = kno_incref(optimized);
  if (old_entry != proc->lambda_body)
    kno_decref(old_entry);
  if (proc->lambda_consblock) {
    lispval cb = (lispval) (proc->lambda_consblock);
    kno_decref(cb);
    proc->lambda_consblock = NULL;}
  return VOID;
}

DEFC_PRIM("optimize-lambda-body!",optimize_lambda_body,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Sets the optimized body of *lambda* to a copy of *optimized* "
	  "allocated in a single block of memory. "
	  "If *optimized* is #f, this deletes the currently optimized "
	  "body (essentially unoptimizing it), if *optimized* is #t, "
	  "this replaces the optimized body a copy of the unoptoimized body",
	  {"lambda",kno_lambda_type,KNO_VOID},
	  {"optimized",kno_any_type,KNO_VOID})
static lispval optimize_lambda_body(lispval lambda,lispval optimized)
{
  struct KNO_LAMBDA *proc = (kno_lambda)lambda;
  if (KNO_FALSEP(optimized)) {
    if (proc->lambda_consblock) {
      lispval cb = (lispval) (proc->lambda_consblock);
      proc->lambda_consblock = NULL;
      kno_decref(cb);}
    else if (proc->lambda_entry != proc->lambda_body)
      kno_decref(proc->lambda_entry);
    else NO_ELSE;
    proc->lambda_entry = proc->lambda_body;}
  else {
    lispval new_consblock = (KNO_TRUEP(optimized)) ?
      (kno_make_consblock(proc->lambda_body)) :
      (kno_make_consblock(optimized));
    if (ABORTED(new_consblock)) return new_consblock;
    else if (KNO_TYPEP(new_consblock,kno_consblock_type)) {
      struct KNO_CONSBLOCK *cb = (kno_consblock) new_consblock;
      proc->lambda_entry = cb->consblock_head;}
    else proc->lambda_entry = new_consblock;
    if (proc->lambda_consblock) {
      lispval cb = (lispval) (proc->lambda_consblock);
      kno_decref(cb);}
    if (KNO_TYPEP(new_consblock,kno_consblock_type))
      proc->lambda_consblock = (kno_consblock) new_consblock;
    else proc->lambda_consblock = NULL;}
  return VOID;
}

DEFC_PRIM("set-lambda-source!",set_lambda_source,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Sets the recorded source for *lambda* to *source*",
	  {"lambda",kno_lambda_type,KNO_VOID},
	  {"source",kno_any_type,KNO_VOID})
static lispval set_lambda_source(lispval lambda,lispval source)
{
  struct KNO_LAMBDA *proc = (kno_lambda)lambda;
  lispval old_source = proc->lambda_source;
  proc->lambda_source = kno_incref(source);
  kno_decref(old_source);
  return VOID;
}

/* Function IDs */

DEFC_PRIM("fcnid/ref",fcnid_refprim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Resolves the functionid *fcnid* and returns "
	  "an incref'd pointer to the value.",
	  {"fcnid",kno_fcnid_type,KNO_VOID})
static lispval fcnid_refprim(lispval fcnid)
{
  lispval result = kno_fcnid_ref(fcnid);
  kno_incref(result);
  return result;
}

DEFC_PRIM("fcnid/register",fcnid_registerprim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns a function id for *fcn*, registering it "
	  "if needed.",
	  {"fcn",kno_any_type,KNO_VOID})
static lispval fcnid_registerprim(lispval fcn)
{
  if (KNO_FCNIDP(fcn))
    return fcn;
  else return kno_register_fcnid(fcn);
}

DEFC_PRIM("fcnid/set!",fcnid_setprim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Updates the value of *fcnid* to be *fcn*",
	  {"fcnid",kno_fcnid_type,KNO_VOID},
	  {"fcn",kno_any_type,KNO_VOID})
static lispval fcnid_setprim(lispval fcnid,lispval fcn)
{
  return kno_set_fcnid(fcnid,fcn);
}

/* Macro expand */

DEFC_PRIM("macroexpand",macroexpand,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Uses the macro *expander* to expand *expr*",
	  {"expander",kno_any_type,KNO_VOID},
	  {"expr",kno_any_type,KNO_VOID})
static lispval macroexpand(lispval expander,lispval expr)
{
  if (PAIRP(expr)) {
    if (TYPEP(expander,kno_macro_type)) {
      struct KNO_MACRO *macrofn = (struct KNO_MACRO *)kno_fcnid_ref(expander);
      kno_lisp_type xformer_type = KNO_TYPEOF(macrofn->macro_transformer);
      if (kno_applyfns[xformer_type]) {
	/* These are evalfns which do all the evaluating themselves */
	lispval new_expr=
	  kno_dcall(kno_stackptr,kno_fcnid_ref(macrofn->macro_transformer),
		    1,&expr);
	if (ABORTED(new_expr)) return kno_err(kno_SyntaxError,_("macro expansion"),NULL,new_expr);
	else return new_expr;}
      else return kno_err(kno_InvalidMacro,NULL,macrofn->macro_name,expr);}
    else return kno_type_error("macro","macroexpand",expander);}
  else return kno_incref(expr);
}

/* Module bindings */

DEFC_PRIM("module-binds",module_binds_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the symbols bound in a module's "
	  "environment",
	  {"arg",kno_any_type,KNO_VOID})
static lispval module_binds_prim(lispval arg)
{
  if (KNO_LEXENVP(arg)) {
    kno_lexenv envptr = kno_consptr(kno_lexenv,arg,kno_lexenv_type);
    return kno_getkeys(envptr->env_bindings);}
  else if (TABLEP(arg))
    return kno_getkeys(arg);
  else return kno_type_error(_("module"),"module_binds_prim",arg);
}

DEFC_PRIM("module-source",module_getsource,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the source (a string) from which module "
	  "was loaded",
	  {"arg",kno_any_type,KNO_VOID})
static lispval module_getsource(lispval arg)
{
  lispval ids = KNO_EMPTY;
  if (KNO_LEXENVP(arg)) {
    kno_lexenv envptr = kno_consptr(kno_lexenv,arg,kno_lexenv_type);
    ids = kno_get(envptr->env_bindings,source_symbol,KNO_VOID);
    if (KNO_VOIDP(ids))
      ids = kno_get(envptr->env_bindings,KNOSYM_MODULEID,KNO_VOID);}
  else if (TABLEP(arg)) {
    ids = kno_get(arg,source_symbol,KNO_VOID);
    if (KNO_VOIDP(ids))
      ids = kno_get(arg,KNOSYM_MODULEID,KNO_VOID);}
  else return kno_type_error(_("module"),"module_getsource",arg);
  if (KNO_VOIDP(ids)) return KNO_FALSE;
  else {
    KNO_DO_CHOICES(id,ids) {
      if (KNO_STRINGP(id)) {
	kno_incref(id);
	kno_decref(ids);
	KNO_STOP_DO_CHOICES;
	return id;}}
    return KNO_FALSE;}
}

DEFC_PRIM("module-exports",module_exports_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the exports table for this module",
	  {"arg",kno_any_type,KNO_VOID})
static lispval module_exports_prim(lispval arg)
{
  if (KNO_LEXENVP(arg)) {
    kno_lexenv envptr = kno_consptr(kno_lexenv,arg,kno_lexenv_type);
    if (KNO_TABLEP(envptr->env_exports))
      return kno_incref(envptr->env_exports);
    else return KNO_FALSE;}
  else if (TABLEP(arg))
    return kno_incref(arg);
  else return kno_type_error(_("module"),"module_exports",arg);
}

DEFC_PRIM("module-bindings",module_bindings_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the table used for this module's internal "
	  "environment",
	  {"arg",kno_any_type,KNO_VOID})
static lispval module_bindings_prim(lispval arg)
{
  if (KNO_LEXENVP(arg)) {
    kno_lexenv envptr = kno_consptr(kno_lexenv,arg,kno_lexenv_type);
    if ( ( (envptr->env_bindings) != (envptr->env_exports) ) &&
	 (KNO_TABLEP(envptr->env_bindings)) )
      return kno_incref(envptr->env_bindings);
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFC_PRIM("module?",modulep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *arg* is a module.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval modulep(lispval arg)
{
  if (KNO_LEXENVP(arg)) {
    struct KNO_LEXENV *env=
      kno_consptr(struct KNO_LEXENV *,arg,kno_lexenv_type);
    if (kno_test(env->env_bindings,KNOSYM_MODULEID,VOID))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if ((HASHTABLEP(arg)) || (SLOTMAPP(arg)) || (SCHEMAPP(arg))) {
    if (kno_test(arg,KNOSYM_MODULEID,VOID))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFC_PRIM("module-exported",module_exported,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the symbols exported from a module",
	  {"arg",kno_any_type,KNO_VOID})
static lispval module_exported(lispval arg)
{
  if (KNO_LEXENVP(arg)) {
    kno_lexenv envptr = kno_consptr(kno_lexenv,arg,kno_lexenv_type);
    return kno_getkeys(envptr->env_exports);}
  else if (TABLEP(arg))
    return kno_getkeys(arg);
  else if (SYMBOLP(arg)) {
    lispval module = kno_find_module(arg,1);
    if (ABORTED(module)) return module;
    else if (VOIDP(module)) return kno_type_error(_("module"),"module_exported",arg);
    else {
      lispval v = module_exported(module);
      kno_decref(module);
      return v;}}
  else return kno_type_error(_("module"),"module_exported",arg);
}

DEFC_EVALFN("%bindings",local_bindings_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(%bindings)` returns the current local bindings as a "
	    "hashtable.");
static lispval local_bindings_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  if (env->env_copy)
    return kno_incref(env->env_copy->env_bindings);
  else {
    kno_lexenv copied = kno_copy_env(env);
    lispval bindings = copied->env_bindings;
    kno_incref(bindings);
    kno_decref((lispval)copied);
    return bindings;}
}

/* Finding where a symbol comes from */


DEFC_EVALFN("wherefrom",wherefrom_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(wherefrom *symbol* [*env*])` returns the module "
	    "(the table itself) used by *env* (which defaults "
	    "to the current environment) where *symbol* "
	    "gets its value.");
static lispval wherefrom_evalfn(lispval expr,kno_lexenv call_env,
				kno_stack _stack)
{
  lispval symbol_arg = kno_get_arg(expr,1);
  lispval symbol = kno_eval(symbol_arg,call_env,_stack);
  if (SYMBOLP(symbol)) {
    kno_lexenv env = NULL, scan = env;
    int lookup_ids = 1, decref_env = 0;
    lispval env_arg = kno_get_arg(expr,2);
    lispval env_val = kno_get_arg(expr,2);
    if (KNO_VOIDP(env_arg))
      env = call_env;
    else {
      env_val = kno_eval(env_arg,call_env,_stack);
      if (KNO_ABORTED(env_val))
	return env_val;
      else if (TYPEP(env_val,kno_lexenv_type)) {
	env = kno_consptr(kno_lexenv,env_val,kno_lexenv_type);
	decref_env = 1;}
      else  {
	lispval err = kno_type_error(_("environment"),"wherefrom",env_val);
	kno_decref(env_val);
	return err;}}
    lispval lookup = kno_get_arg(expr,3);
    if (!(KNO_VOIDP(lookup))) {
      lookup = kno_eval(lookup,call_env,_stack);
      if (KNO_ABORTED(lookup)) {
	if (decref_env) kno_decref(env_val);
	return lookup;}
      else if (KNO_FALSEP(lookup))
	lookup_ids = 0;
      else lookup_ids = 1;
      kno_decref(lookup);}
    if (env->env_copy)
      scan = env->env_copy;
    else scan = env;
    while (scan) {
      if (kno_test(scan->env_bindings,symbol,VOID)) {
	lispval bindings = scan->env_bindings;
	if (!(CONSP(bindings))) return KNO_FALSE;
	lispval id = kno_get(bindings,KNOSYM_MODULEID,KNO_VOID);
	if ( (KNO_SYMBOLP(id)) &&
	     ( (lookup_ids) || (!(KNO_MALLOCD_CONSP((kno_cons)bindings))) ) ) {
	  lispval mod = kno_get_module(id);
	  if ( (KNO_ABORTP(mod)) || (KNO_TABLEP(mod))  ) {
	    if (decref_env) kno_decref(env_val);
	    return mod;}
	  else kno_decref(mod);}
	if (KNO_MALLOCD_CONSP((kno_cons)bindings)) {
	  lispval result = (lispval) scan;
	  kno_incref(result);
	  if (decref_env) kno_decref(env_val);
	  return result;}
	else return KNO_FALSE;}
      scan = scan->env_parent;
      if ((scan) && (scan->env_copy))
	scan = scan->env_copy;}
    if (decref_env) kno_decref(env_val);
    return KNO_FALSE;}
  else if (KNO_ABORTED(symbol)) return symbol;
  else {
    lispval err = kno_type_error(_("symbol"),"wherefrom",symbol);
    kno_decref(symbol);
    return err;}
}

/* Finding all the modules used from an environment */

DEFC_EVALFN("getmodules",getmodules_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(getmodules [*env*])` returns the names of all the modules "
	    "used by *env*, which defaults to the current environment. ")
static lispval getmodules_evalfn(lispval expr,kno_lexenv call_env,kno_stack _stack)
{
  lispval env_arg = kno_eval(kno_get_arg(expr,1),call_env,_stack);
  lispval modules = EMPTY;
  kno_lexenv env  = call_env;
  if (VOIDP(env_arg)) {}
  else if (TYPEP(env_arg,kno_lexenv_type))
    env = kno_consptr(kno_lexenv,env_arg,kno_lexenv_type);
  else {
    lispval err =
      kno_type_error(_("environment"),"wherefrom",env_arg);
    kno_decref(env_arg);
    return err;}
  if (env->env_copy) env = env->env_copy;
  while (env) {
    if (kno_test(env->env_bindings,KNOSYM_MODULEID,VOID)) {
      lispval ids = kno_get(env->env_bindings,KNOSYM_MODULEID,VOID);
      if (CHOICEP(ids)) {
	DO_CHOICES(id,ids) {
	  if (SYMBOLP(id)) {CHOICE_ADD(modules,id);}}}
      else if (SYMBOLP(ids)) {CHOICE_ADD(modules,ids);}
      else {}}
    env = env->env_parent;
    if ((env) && (env->env_copy)) env = env->env_copy;}
  kno_decref(env_arg);
  return modules;
}

/* CONSBLOCKS */

DEFC_PRIM("consblock",make_consblock,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(CONSBLOCK *obj*) "
	  "returns a consblock structure which copies *obj* "
	  "into a static contiguous block of memory.",
	  {"obj",kno_any_type,KNO_VOID})
static lispval make_consblock(lispval obj)
{
  return kno_make_consblock(obj);
}

DEFC_PRIM("consblock-origin",consblock_original,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(CONSBLOCK-ORIGIN *consblock*) "
	  "returns the original object from which a "
	  "consblock was generated.",
	  {"obj",kno_consblock_type,KNO_VOID})
static lispval consblock_original(lispval obj)
{
  struct KNO_CONSBLOCK *cb = (kno_consblock) obj;
  return kno_incref(cb->consblock_original);
}

DEFC_PRIM("consblock-head",consblock_head,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(CONSBLOCK-HEAD *consblock*) "
	  "returns the head of the consblock",
	  {"obj",kno_consblock_type,KNO_VOID})
static lispval consblock_head(lispval obj)
{
  struct KNO_CONSBLOCK *cb = (kno_consblock) obj;
  return cb->consblock_head;
}

/* Profiling */

DEFC_PRIM("reflect/profile!",profile_fcn_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Enables profiling for the function *fcn* or if "
	  "*boolean* is false, disable profiling for *fcn*",
	  {"fcn",kno_any_type,KNO_VOID},
	  {"bool",kno_any_type,KNO_TRUE})
static lispval profile_fcn_prim(lispval fcn,lispval bool)
{
  if (KNO_FUNCTIONP(fcn)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(fcn);
    struct KNO_PROFILE *profile = f->fcn_profile;
    if (KNO_FALSEP(bool)) {
      if (profile) {
	profile->prof_disabled=1;
	return KNO_FALSE;}
      else return KNO_FALSE;}
    else if (f->fcn_profile)
      profile->prof_disabled=0;
    else f->fcn_profile = kno_make_profile(f->fcn_name);
    return KNO_TRUE;}
  else return kno_type_error("function","profile_fcn",fcn);
}

DEFC_PRIM("profile/reset!",profile_reset_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "resets the profile counts for *fcn*",
	  {"fcn",kno_any_type,KNO_VOID})
static lispval profile_reset_prim(lispval fcn)
{
  if (KNO_FUNCTIONP(fcn)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(fcn);
    struct KNO_PROFILE *profile = f->fcn_profile;
    if (profile == NULL) return KNO_FALSE;
#if HAVE_STDATOMIC_H
    profile->prof_calls = ATOMIC_VAR_INIT(0);
    profile->prof_items = ATOMIC_VAR_INIT(0);
    profile->prof_nsecs = ATOMIC_VAR_INIT(0);
#else
    u8_lock_mutex(&(profile->prof_lock));
    profile->prof_calls = 0;
    profile->prof_items = 0;
    profile->prof_nsecs = 0;
    u8_unlock_mutex(&(profile->prof_lock));
#endif
    return KNO_TRUE;}
  else return kno_type_error("function","profile_reset(profile)",fcn);
}

DEFC_PRIM("reflect/profiled?",profiledp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *arg* is being profiled, and "
	  "false otherwise. It also returns false if the "
	  "argument is not a function or not a function "
	  "which supports profiling.",
	  {"fcn",kno_any_type,KNO_VOID})
static lispval profiledp_prim(lispval fcn)
{
  if (KNO_FUNCTIONP(fcn)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(fcn);
    if (f->fcn_profile)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

static lispval call_profile_symbol;

static int getprofile_info(lispval fcn,int err,
			   long long *calls_ptr,long long *items_ptr,
			   long long *nsecs_ptr,
			   double *etime,double *utime,double *stime,
			   long long *waits_ptr,long long *pauses_ptr,
			   long long *faults_ptr)
{
  if (KNO_FUNCTIONP(fcn)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(fcn);
    struct KNO_PROFILE *p = f->fcn_profile;
    if (p==NULL)
      return 0;
#if HAVE_STDATOMIC_H
    long long calls = atomic_load(&(p->prof_calls));
    long long items = atomic_load(&(p->prof_items));
    long long nsecs = atomic_load(&(p->prof_nsecs));
    long long user_nsecs = atomic_load(&(p->prof_nsecs_user));
    long long system_nsecs = atomic_load(&(p->prof_nsecs_system));
    long long n_waits = atomic_load(&(p->prof_n_waits));
    long long n_pauses = atomic_load(&(p->prof_n_pauses));
    long long n_faults = atomic_load(&(p->prof_n_faults));
#else
    long long calls = p->prof_calls;
    long long items = p->prof_items;
    long long nsecs = p->prof_nsecs;
    long long user_nsecs = p->prof_nsecs_user;
    long long system_nsecs = p->prof_nsecs_system;
    long long n_waits = prof_n_waits;
    long long n_pauses = prof_n_pauses;
    long long n_faults = p->prof_n_faults;
#endif
    double exec_time = ((double)((nsecs)|(calls)))/1000000000.0;
    double user_time = ((double)((user_nsecs)|(calls)))/1000000000.0;
    double system_time = ((double)((system_nsecs)|(calls)))/1000000000.0;
    if (nsecs_ptr) *nsecs_ptr = nsecs;
    if (calls_ptr) *calls_ptr = calls;
    if (items_ptr) *items_ptr = items;
    if (etime) *etime = exec_time;
    if (utime) *utime = user_time;
    if (stime) *stime = system_time;
    if (waits_ptr) *waits_ptr = n_waits;
    if (pauses_ptr) *pauses_ptr = n_pauses;
    if (faults_ptr) *faults_ptr = n_faults;
    return 1;}
  else if (err) {
    kno_seterr("function","getprofile_info(profile)",NULL,fcn);
    return -1;}
  else return 0;
}

DEFC_PRIM("profile/getcalls",getcalls_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Returns the profile information for *fcn*, a "
	  "vector of *fcn*, the number of calls, the number "
	  "of nanoseconds spent in *fcn*. If *fcn* is a "
	  "valid function but isn't being profiled, this "
	  "returns #f. If *fcn* is not a function, this "
	  "returns an error unless *error* is false.",
	  {"fcn",kno_any_type,KNO_VOID},
	  {"errp",kno_any_type,KNO_VOID})
static lispval getcalls_prim(lispval fcn,lispval errp)
{
  if (KNO_FUNCTIONP(fcn)) {
    long long calls = 0, items = 0, nsecs = 0;
    double exec_time = 0, user_time = 0, system_time = 0;
    long long n_waits = 0, n_pauses = 0, n_faults = 0;
    int rv = getprofile_info(fcn,(!(KNO_FALSEP(errp))),
			     &calls,&items,&nsecs,
			     &exec_time,&user_time,&system_time,
			     &n_waits,&n_pauses,&n_faults);
    if (rv < 0)
      return KNO_ERROR_VALUE;
    else if (rv) {
      kno_incref(fcn);
      return kno_init_compound
	(NULL,call_profile_symbol,KNO_COMPOUND_USEREF,10,
	 fcn,kno_make_flonum(exec_time),
	 kno_make_flonum(user_time),
	 kno_make_flonum(system_time),
	 KNO_INT(n_waits),KNO_INT(n_pauses),KNO_INT(n_faults),
	 KNO_INT(nsecs),KNO_INT(calls),KNO_INT(items));}
    else return KNO_FALSE;}
  else if (KNO_FALSEP(errp))
    return KNO_FALSE;
  else return kno_type_error("function","getcalls_prim(profile)",fcn);
}

/* Accessors */

DEFC_PRIM("profile/fcn",profile_getfcn,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the function a profile describes",
	  {"profile",kno_any_type,KNO_VOID})
static lispval profile_getfcn(lispval profile)
{
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,0);
    return kno_incref(v);}
  else return kno_type_error("call profile","profile_getfcn",profile);
}

DEFC_PRIM("profile/time",profile_gettime,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of seconds spent in the "
	  "profiled function",
	  {"profile",kno_any_type,KNO_VOID})
static lispval profile_gettime(lispval profile)
{
  if (KNO_FCNIDP(profile)) profile = kno_fcnid_ref(profile);
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,1);
    return kno_incref(v);}
  else if (KNO_FUNCTIONP(profile)) {
    double time = 0;
    int rv = getprofile_info(profile,0,NULL,NULL,NULL,
			     &time,NULL,NULL,
			     NULL,NULL,NULL);
    if (rv)
      return kno_make_flonum(time);
    else return KNO_FALSE;}
  else return kno_type_error("call profile","profile_gettime",profile);
}

DEFC_PRIM("profile/utime",profile_getutime,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of seconds of user time spent "
	  "in the profiled function",
	  {"profile",kno_any_type,KNO_VOID})
static lispval profile_getutime(lispval profile)
{
  if (KNO_FCNIDP(profile)) profile = kno_fcnid_ref(profile);
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,2);
    return kno_incref(v);}
  else if (KNO_FUNCTIONP(profile)) {
    double time = 0;
    int rv = getprofile_info(profile,0,NULL,NULL,NULL,
			     NULL,&time,NULL,
			     NULL,NULL,NULL);
    if (rv)
      return kno_make_flonum(time);
    else return KNO_FALSE;}
  else return kno_type_error("call profile","profile_getutime",profile);
}

DEFC_PRIM("profile/stime",profile_getstime,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of seconds of system time "
	  "spent in the profiled function",
	  {"profile",kno_any_type,KNO_VOID})
static lispval profile_getstime(lispval profile)
{
  if (KNO_FCNIDP(profile)) profile = kno_fcnid_ref(profile);
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,3);
    return kno_incref(v);}
  else if (KNO_FUNCTIONP(profile)) {
    double time = 0;
    int rv = getprofile_info(profile,0,NULL,NULL,NULL,
			     NULL,NULL,&time,
			     NULL,NULL,NULL);
    if (rv)
      return kno_make_flonum(time);
    else return KNO_FALSE;}
  else return kno_type_error("call profile","profile_getstime",profile);
}

DEFC_PRIM("profile/waits",profile_getwaits,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of voluntary context switches "
	  "(usually when waiting for something) during the "
	  "execution of the profiled function",
	  {"profile",kno_any_type,KNO_VOID})
static lispval profile_getwaits(lispval profile)
{
  if (KNO_FCNIDP(profile)) profile = kno_fcnid_ref(profile);
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,4);
    return kno_incref(v);}
  else if (KNO_FUNCTIONP(profile)) {
    long long count = 0;
    int rv = getprofile_info(profile,0,NULL,NULL,NULL,
			     NULL,NULL,NULL,
			     &count,NULL,NULL);
    if (rv)
      return KNO_INT(count);
    else return KNO_FALSE;}
  else return kno_type_error("call profile","profile_getwaits",profile);
}

DEFC_PRIM("profile/pauses",profile_getpauses,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of involuntary context "
	  "switches (often indicating contested resources) "
	  "during the execution of the profiled function",
	  {"profile",kno_any_type,KNO_VOID})
static lispval profile_getpauses(lispval profile)
{
  if (KNO_FCNIDP(profile)) profile = kno_fcnid_ref(profile);
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,5);
    return kno_incref(v);}
  else if (KNO_FUNCTIONP(profile)) {
    long long count = 0;
    int rv = getprofile_info(profile,0,NULL,NULL,NULL,
			     NULL,NULL,NULL,
			     NULL,&count,NULL);
    if (rv)
      return KNO_INT(count);
    else return KNO_FALSE;}
  else return kno_type_error("call profile","profile_getpauses",profile);
}

DEFC_PRIM("profile/faults",profile_getfaults,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of (major) page faults during "
	  "the execution of the profiled function",
	  {"profile",kno_any_type,KNO_VOID})
static lispval profile_getfaults(lispval profile)
{
  if (KNO_FCNIDP(profile)) profile = kno_fcnid_ref(profile);
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,6);
    return kno_incref(v);}
  else if (KNO_FUNCTIONP(profile)) {
    long long count = 0;
    int rv = getprofile_info(profile,0,NULL,NULL,NULL,
			     NULL,NULL,NULL,
			     NULL,NULL,&count);
    if (rv)
      return KNO_INT(count);
    else return KNO_FALSE;}
  else return kno_type_error("call profile","profile_getfaults",profile);
}

DEFC_PRIM("profile/nsecs",profile_nsecs,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of nanoseconds spent in the "
	  "profiled function",
	  {"profile",kno_any_type,KNO_VOID})
static lispval profile_nsecs(lispval profile)
{
  if (KNO_FCNIDP(profile)) profile = kno_fcnid_ref(profile);
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,7);
    return kno_incref(v);}
  else if (KNO_FUNCTIONP(profile)) {
    long long nsecs = 0;
    int rv = getprofile_info(profile,0,
			     NULL,NULL,&nsecs,
			     NULL,NULL,NULL,
			     NULL,NULL,NULL);
    if (rv)
      return KNO_INT(nsecs);
    else return KNO_FALSE;}
  else return kno_type_error("call profile","profile_nsecs",profile);
}

DEFC_PRIM("profile/ncalls",profile_ncalls,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of calls to the profiled "
	  "function",
	  {"profile",kno_any_type,KNO_VOID})
static lispval profile_ncalls(lispval profile)
{
  if (KNO_FCNIDP(profile)) profile = kno_fcnid_ref(profile);
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,8);
    return kno_incref(v);}
  else if (KNO_FUNCTIONP(profile)) {
    long long count = 0;
    int rv = getprofile_info(profile,0,
			     &count,NULL,NULL,
			     NULL,NULL,NULL,
			     NULL,NULL,NULL);
    if (rv)
      return KNO_INT(count);
    else return KNO_FALSE;}
  else return kno_type_error("call profile","profile_ncalls",profile);
}

DEFC_PRIM("profile/nitems",profile_nitems,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of items noted as processed by "
	  "the profiled function",
	  {"profile",kno_any_type,KNO_VOID})
static lispval profile_nitems(lispval profile)
{
  if (KNO_FCNIDP(profile)) profile = kno_fcnid_ref(profile);
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,9);
    return kno_incref(v);}
  else if (KNO_FUNCTIONP(profile)) {
    long long count = 0;
    int rv = getprofile_info(profile,0,
			     NULL,&count,NULL,
			     NULL,NULL,NULL,
			     NULL,NULL,NULL);
    if (rv)
      return KNO_INT(count);
    else return KNO_FALSE;}
  else return kno_type_error("call profile","profile_nitems",profile);
}

static u8_string profile_schema_init[10] =
  { "fcn", "time", "utime", "stime", "waits", "pauses",
    "faults", "nsecs", "ncalls", "nitems" };
static lispval profile_schema[10];

#define PROFILE_SCHEMAP_FLAGS \
  ( (KNO_SCHEMAP_FIXED_SCHEMA) | (KNO_SCHEMAP_STATIC_VALUES) )

DEFC_PRIM("profile/unpack",profile_unpack,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a table from a callinfo record",
	  {"profile",kno_any_type,KNO_VOID})
static lispval profile_unpack(lispval profile)
{
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval *vals=KNO_COMPOUND_ELTS(profile);
    return kno_make_schemap(NULL,10,KNO_SCHEMAP_STATIC_VALUES,
			    profile_schema,vals);}
  else return kno_type_error("call profile","profile_getfcn",profile);
}

/* Getting all modules */

DEFC_PRIM("all-modules",get_all_modules_prim,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "Returns all loaded modules as an alist of module "
	  "names and modules")
static lispval get_all_modules_prim()
{
  return kno_all_modules();
}

/* Initialization */

static lispval reflection_module;
static lispval profiling_module;

KNO_EXPORT void kno_init_reflection_c()
{
  reflection_module = kno_new_cmodule("kno/reflect",0,kno_init_reflection_c);
  profiling_module  = kno_new_cmodule("kno/profile",0,kno_init_reflection_c);

  source_symbol = kno_intern("%source");
  call_profile_symbol = kno_intern("%callprofile");
  void_symbol = kno_intern("%void");

  link_local_cprims();

  int i = 0; while (i < 10) {
    profile_schema[i]=kno_intern(profile_schema_init[i]);
    i++;}

  KNO_LINK_EVALFN(reflection_module,local_bindings_evalfn);
  KNO_LINK_EVALFN(reflection_module,wherefrom_evalfn);
  KNO_LINK_EVALFN(reflection_module,getmodules_evalfn);

  kno_finish_cmodule(reflection_module);
  kno_finish_cmodule(profiling_module);
}

static void link_local_cprims()
{
  lispval reflection = reflection_module;
  lispval profiling = profiling_module;
  KNO_LINK_CPRIM("all-modules",get_all_modules_prim,0,reflection);
  KNO_LINK_CPRIM("consblock-head",consblock_head,1,reflection);
  KNO_LINK_CPRIM("consblock-origin",consblock_original,1,reflection);
  KNO_LINK_CPRIM("consblock",make_consblock,1,reflection);
  KNO_LINK_CPRIM("module-exported",module_exported,1,reflection);
  KNO_LINK_CPRIM("module?",modulep,1,reflection);
  KNO_LINK_CPRIM("module-bindings",module_bindings_prim,1,reflection);
  KNO_LINK_CPRIM("module-exports",module_exports_prim,1,reflection);
  KNO_LINK_CPRIM("module-source",module_getsource,1,reflection);
  KNO_LINK_CPRIM("module-binds",module_binds_prim,1,reflection);
  KNO_LINK_CPRIM("macroexpand",macroexpand,2,reflection);
  KNO_LINK_CPRIM("fcnid/set!",fcnid_setprim,2,reflection);
  KNO_LINK_CPRIM("fcnid/register",fcnid_registerprim,1,reflection);
  KNO_LINK_CPRIM("fcnid/ref",fcnid_refprim,1,reflection);
  KNO_LINK_CPRIM("set-lambda-source!",set_lambda_source,2,reflection);
  KNO_LINK_CPRIM("optimize-lambda-body!",optimize_lambda_body,2,reflection);
  KNO_LINK_CPRIM("set-lambda-body!",set_lambda_body,2,reflection);
  KNO_LINK_CPRIM("set-lambda-entry!",set_lambda_entry,2,reflection);
  KNO_LINK_CPRIM("lambda-source",lambda_source,1,reflection);
  KNO_LINK_CPRIM("lambda-entry",lambda_entry,1,reflection);
  KNO_LINK_CPRIM("lambda-body",lambda_body,1,reflection);
  KNO_LINK_CPRIM("lambda-env",lambda_env,1,reflection);
  KNO_LINK_CPRIM("set-lambda-args!",set_lambda_args,2,reflection);
  KNO_LINK_CPRIM("lambda-args",lambda_args,1,reflection);
  KNO_LINK_CPRIM("reflect/drop!",reflect_drop,3,reflection);
  KNO_LINK_CPRIM("reflect/add!",reflect_add,3,reflection);
  KNO_LINK_CPRIM("reflect/store!",reflect_store,3,reflection);
  KNO_LINK_CPRIM("reflect/get",reflect_get,2,reflection);
  KNO_LINK_CPRIM("reflect/set-attribs!",set_procedure_attribs,2,reflection);
  KNO_LINK_CPRIM("reflect/attribs",get_procedure_attribs,1,reflection);
  KNO_LINK_CPRIM("procedure-args",procedure_args,1,reflection);
  KNO_LINK_CPRIM("procedure-defaults",procedure_defaults,1,reflection);
  KNO_LINK_CPRIM("procedure-typeinfo",procedure_typeinfo,1,reflection);
  KNO_LINK_CPRIM("procedure-min-arity",procedure_min_arity,1,reflection);
  KNO_LINK_CPRIM("procedure-tracing",procedure_tracing,1,reflection);
  KNO_LINK_CPRIM("procedure/trace!",procedure_set_trace,2,reflection);
  KNO_LINK_CPRIM("synchronized?",synchronizedp,1,reflection);
  KNO_LINK_CPRIM("non-deterministic?",non_deterministicp,1,reflection);
  KNO_LINK_CPRIM("procedure-arity",procedure_arity,1,reflection);
  KNO_LINK_CPRIM("set-tailable!",set_tailablep,2,reflection);
  KNO_LINK_CPRIM("tailable?",procedure_tailablep,1,reflection);
  KNO_LINK_CPRIM("set-documentation!",set_documentation,2,reflection);
  KNO_LINK_CPRIM("documentation",get_documentation,1,reflection);
  KNO_LINK_CPRIM("procedure-id",procedure_id,1,reflection);
  KNO_LINK_CPRIM("procedure-symbol",procedure_symbol,1,reflection);
  KNO_LINK_CPRIM("procedure-module",procedure_moduleid,1,reflection);
  KNO_LINK_CPRIM("procedure-filename",procedure_filename,1,reflection);
  KNO_LINK_CPRIM("procedure-fileinfo",procedure_fileinfo,1,reflection);
  KNO_LINK_CPRIM("procedure-cname",procedure_cname,1,reflection);
  KNO_LINK_CPRIM("procedure-name",procedure_name,1,reflection);
  KNO_LINK_CPRIM("procedure?",procedurep,1,reflection);
  KNO_LINK_CPRIM("primitive?",primitivep,1,reflection);
  KNO_LINK_CPRIM("evalfn?",evalfnp,1,reflection);
  KNO_LINK_CPRIM("lambda?",lambdap,1,reflection);
  KNO_LINK_CPRIM("macro?",macrop,1,reflection);

  KNO_LINK_ALIAS("lambda-args",lambda_args,reflection);
  KNO_LINK_ALIAS("lambda-start",lambda_entry,reflection);
  KNO_LINK_ALIAS("procedure-env",lambda_env,reflection);
  KNO_LINK_ALIAS("procedure-body",lambda_body,reflection);
  KNO_LINK_ALIAS("compound-procedure?",lambdap,reflection);
  KNO_LINK_ALIAS("special-form?",evalfnp,reflection);
  KNO_LINK_ALIAS("procedure-tailable?",procedure_tailablep,reflection);
  KNO_LINK_ALIAS("set-procedure-tailable!",set_tailablep,reflection);
  KNO_LINK_ALIAS("set-procedure-documentation!",set_documentation,reflection);
  KNO_LINK_ALIAS("procedure-documentation",get_documentation,reflection);

  KNO_LINK_CPRIM("profile/nitems",profile_nitems,1,profiling);
  KNO_LINK_CPRIM("profile/ncalls",profile_ncalls,1,profiling);
  KNO_LINK_CPRIM("profile/nsecs",profile_nsecs,1,profiling);
  KNO_LINK_CPRIM("profile/faults",profile_getfaults,1,profiling);
  KNO_LINK_CPRIM("profile/pauses",profile_getpauses,1,profiling);
  KNO_LINK_CPRIM("profile/waits",profile_getwaits,1,profiling);
  KNO_LINK_CPRIM("profile/stime",profile_getstime,1,profiling);
  KNO_LINK_CPRIM("profile/utime",profile_getutime,1,profiling);
  KNO_LINK_CPRIM("profile/time",profile_gettime,1,profiling);
  KNO_LINK_CPRIM("profile/fcn",profile_getfcn,1,profiling);
  KNO_LINK_CPRIM("profile/getcalls",getcalls_prim,2,profiling);
  KNO_LINK_CPRIM("profile/unpack",profile_unpack,1,profiling);
  KNO_LINK_CPRIM("profile/reset!",profile_reset_prim,1,profiling);
  KNO_LINK_CPRIM("profiled?",profiledp_prim,1,profiling);
  KNO_LINK_CPRIM("profile!",profile_fcn_prim,2,profiling);
  KNO_LINK_ALIAS("reflect/profiled?",profiledp_prim,profiling);
  KNO_LINK_ALIAS("reflect/profile!",profile_fcn_prim,profiling);

}
