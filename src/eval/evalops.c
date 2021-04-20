/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/opcodes.h"
#include "kno/ffi.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include "kno/support.h"
#include "kno/cprims.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>

static u8_condition ExpiredThrow  =_("Continuation is no longer valid");
static u8_condition DoubleThrow	  =_("Continuation used twice");
static u8_condition LostThrow	  =_("Lost invoked continuation");

/* Lexrefs */

DEFC_PRIM("%lexref",lexref_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "(%LEXREF *up* *across*) "
	  "returns a lexref (lexical reference) given a "
	  "'number of environments' *up* and a 'number of "
	  "bindings' across",
	  {"upv",kno_fixnum_type,KNO_VOID},
	  {"acrossv",kno_fixnum_type,KNO_VOID})
static lispval lexref_prim(lispval upv,lispval acrossv)
{
  long long up = FIX2INT(upv), across = FIX2INT(acrossv);
  long long combined = ((up<<5)|(across));
  /* Not the exact range limits, but good enough */
  if ((up>=0)&&(across>=0)&&(up<256)&&(across<256))
    return LISPVAL_IMMEDIATE(kno_lexref_type,combined);
  else if ((up<0)||(up>256))
    return kno_type_error("short","lexref_prim",up);
  else return kno_type_error("short","lexref_prim",across);
}

DEFC_PRIM("%lexref?",lexrefp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(%LEXREF? *val*) "
	  "returns true if it's argument is a lexref "
	  "(lexical reference)",
	  {"ref",kno_any_type,KNO_VOID})
static lispval lexrefp_prim(lispval ref)
{
  if (KNO_TYPEP(ref,kno_lexref_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("%lexrefval",lexref_value_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(%LEXREFVAL *lexref*) "
	  "returns the offsets of a lexref as a pair (*up* . "
	  "*across*)",
	  {"lexref",kno_lexref_type,KNO_VOID})
static lispval lexref_value_prim(lispval lexref)
{
  int code = KNO_GET_IMMEDIATE(lexref,kno_lexref_type);
  int up = code/32, across = code%32;
  return kno_init_pair(NULL,KNO_INT(up),KNO_INT(across));
}

/* Code refs */

DEFC_PRIM("%coderef",coderef_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(%CODEREF *nelts*) "
	  "returns a 'coderef' (a relative position) value",
	  {"offset",kno_fixnum_type,KNO_VOID})
static lispval coderef_prim(lispval offset)
{
  long long off = FIX2INT(offset);
  return LISPVAL_IMMEDIATE(kno_coderef_type,off);
}

DEFC_PRIM("coderef?",coderefp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(CODEREF? *nelts*) "
	  "returns #t if *arg* is a coderef",
	  {"ref",kno_any_type,KNO_VOID})
static lispval coderefp_prim(lispval ref)
{
  if (KNO_TYPEP(ref,kno_coderef_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("%coderefval",coderef_value_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(%CODEREFVAL *coderef*) "
	  "returns the integer relative offset of a coderef",
	  {"offset",kno_coderef_type,KNO_VOID})
static lispval coderef_value_prim(lispval offset)
{
  long long off = KNO_GET_IMMEDIATE(offset,kno_coderef_type);
  return KNO_INT(off);
}

DEFC_PRIM("make-coderef",make_coderef,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(MAKE-CODEREF <fixnum>)\n"
	  "Returns a coderef object",
	  {"x",kno_fixnum_type,KNO_VOID})
static lispval make_coderef(lispval x)
{
  if ( (KNO_UINTP(x)) && ((KNO_FIX2INT(x)) < KNO_IMMEDIATE_MAX) ) {
    int off = FIX2INT(x);
    return KNO_ENCODEREF(off);}
  else return kno_err(kno_RangeError,"make_coderef",NULL,x);
}


/* Opcodes */

DEFC_PRIM("opcode?",opcodep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
static lispval opcodep(lispval x)
{
  if (KNO_OPCODEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("name->opcode",name2opcode_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval name2opcode_prim(lispval arg)
{
  if (KNO_SYMBOLP(arg))
    return kno_get_opcode(SYM_NAME(arg));
  else if (STRINGP(arg))
    return kno_get_opcode(CSTRING(arg));
  else return kno_type_error(_("opcode name"),"name2opcode_prim",arg);
}

DEFC_PRIM("make-opcode",make_opcode,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"x",kno_fixnum_type,KNO_VOID})
static lispval make_opcode(lispval x)
{
  if ( (KNO_UINTP(x)) && ((KNO_FIX2INT(x)) < KNO_IMMEDIATE_MAX) )
    return KNO_OPCODE(FIX2INT(x));
  else return kno_err(kno_RangeError,"make_opcode",NULL,x);
}

DEFC_PRIM("opcode-name",opcode_name_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(OPCODE-NAME *opcode*) "
	  "returns the name of *opcode* or #f it's valid but "
	  "anonymous, and errors if it is not an opcode or "
	  "invalid",
	  {"opcode",kno_opcode_type,KNO_VOID})
static lispval opcode_name_prim(lispval opcode)
{
  long opcode_offset = (KNO_GET_IMMEDIATE(opcode,kno_opcode_type));
  if ((opcode_offset<kno_opcodes_length) &&
      (kno_opcode_names[opcode_offset]))
    return knostring(kno_opcode_names[opcode_offset]);
  else if (opcode_offset<kno_opcodes_length)
    return KNO_FALSE;
  else return kno_err("InvalidOpcode","opcode_name_prim",NULL,opcode);
}

int kno_opcode_class(lispval opcode)
{
  return KNO_OPCODE_CLASS(opcode);
}

/* Call/cc */

static lispval call_continuation(struct KNO_STACK *stack,
				 struct KNO_FUNCTION *f,
				 int n,kno_argvec args)
{
  struct KNO_CONTINUATION *cont = (struct KNO_CONTINUATION *)f;
  lispval arg = args[0];
  if (cont->retval == KNO_NULL)
    return kno_err(ExpiredThrow,"call_continuation",NULL,arg);
  else if (VOIDP(cont->retval)) {
    cont->retval = kno_incref(arg);
    return KNO_THROW_VALUE;}
  else return kno_err(DoubleThrow,"call_continuation",NULL,arg);
}

DEFC_PRIM("call/cc",callcc,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "applies *fn.callback* to a single argument, which "
	  "is a *continuation* procedure. When the "
	  "application of *fn.callback* calls *continuation* "
	  "to an argument, that argument is immediately "
	  "returned by the call to `call/cc`. If "
	  "*continuation* is never called, `call/cc` simply "
	  "returns the value returned by *fn.callback*.",
	  {"proc",kno_any_type,KNO_VOID})
static lispval callcc(lispval proc)
{
  lispval continuation, value;
  struct KNO_CONTINUATION *f = u8_alloc(struct KNO_CONTINUATION);
  KNO_INIT_FRESH_CONS(f,kno_cprim_type);
  f->fcn_name="continuation"; f->fcn_filename = NULL;
  f->fcn_call = KNO_CALL_NDCALL | KNO_CALL_XCALL;
  f->fcn_call_width = f->fcn_arity = 1;
  f->fcn_min_arity = 1;
  f->fcn_arginfo_len = 0;
  f->fcn_argnames = NULL;
  f->fcn_handler.xcalln = call_continuation;
  f->fcn_typeinfo = NULL;
  f->fcn_defaults = NULL;
  f->retval = VOID;
  continuation = LISP_CONS(f);
  value = kno_apply(proc,1,&continuation);
  if ((value == KNO_THROW_VALUE) && (!(VOIDP(f->retval)))) {
    lispval retval = f->retval;
    f->retval = KNO_NULL;
    if (KNO_CONS_REFCOUNT(f)>1)
      u8_log(LOG_WARN,ExpiredThrow,"Dangling pointer exists to continuation");
    kno_decref(continuation);
    return retval;}
  else {
    if (KNO_CONS_REFCOUNT(f)>1)
      u8_log(LOG_WARN,LostThrow,"Dangling pointer exists to continuation");
    kno_decref(continuation);
    return value;}
}

/* Stack queries */

DEFC_PRIM("%stack",getstack_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(0),
	  "Returns a subset of the stack",
	  {"test",kno_any_type,KNO_VOID},
	  {"count",kno_fixnum_type,KNO_VOID})
static lispval getstack_prim(lispval test,lispval count)
{
  kno_stack cur = kno_stackptr;
  int depth = cur->stack_depth, n;
  if ( (KNO_VOIDP(count)) || (KNO_FALSEP(count)) )
    n = depth;
  else if (KNO_FIXNUMP(count)) {
    long long iv = KNO_FIX2INT(count);
    if ( (iv<=0) || (iv>depth) ) n = depth;
    else depth=iv;}
  else NO_ELSE;
  if ( (KNO_VOIDP(test)) || (KNO_FALSEP(test)) ||
       (KNO_TRUEP(test)) || (KNO_DEFAULTP(test)) ||
       (KNO_APPLICABLEP(test)) ) {
    lispval results[n];
    int i = 0; kno_stack scan = cur;
    while ( (scan) && (i<n) ) {
      lispval stackobj = kno_stack2lisp(scan);
      if (KNO_APPLICABLEP(test)) {
	lispval keep = kno_apply(test,1,&stackobj);
	if (KNO_ABORTED(keep)) {
	  kno_decref_vec(results,i);
	  return keep;}
	else if (KNO_FALSEP(keep))
	  kno_decref(stackobj);
	else results[i++]=stackobj;
	kno_decref(keep);}
      else results[i++]=stackobj;
      scan = scan->stack_caller;}
    return kno_make_vector(i,results);}
  else return kno_err("StackTestPredicate","%stack",NULL,test);
}

DEFC_PRIM("%stackdepth",stackdepth_prim,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "Returns the depth of the Scheme stack")
static lispval stackdepth_prim()
{
  kno_stack cur = kno_stackptr;
  if (cur) return KNO_INT(cur->stack_depth);
  else return KNO_INT(0);
}

/* Environments */

DEFC_PRIM("symbol-bound-in?",symbol_boundin_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"symbol",kno_any_type,KNO_VOID},
	  {"envarg",kno_any_type,KNO_VOID})
static lispval symbol_boundin_prim(lispval symbol,lispval envarg)
{
  if (!(SYMBOLP(symbol)))
    return kno_type_error(_("symbol"),"symbol_boundp_prim",symbol);
  else if (KNO_LEXENVP(envarg)) {
    kno_lexenv env = (kno_lexenv)envarg;
    lispval val = kno_symeval(symbol,env);
    if (KNO_ABORTED(val))
      return val;
    else if (VOIDP(val))
      return KNO_FALSE;
    else if (val == KNO_DEFAULT_VALUE)
      return KNO_FALSE;
    else if (val == KNO_UNBOUND)
      return KNO_FALSE;
    else {
      kno_decref(val);
      return KNO_TRUE;}}
  else if (TABLEP(envarg)) {
    lispval val = kno_get(envarg,symbol,VOID);
    if (VOIDP(val))
      return KNO_FALSE;
    else {
      kno_decref(val);
      return KNO_TRUE;}}
  else return kno_type_error(_("environment"),"symbol_boundp_prim",envarg);
}

DEFC_PRIM("environment?",environmentp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval environmentp_prim(lispval arg)
{
  if (KNO_LEXENVP(arg))
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* GET-ARG */

DEFC_PRIM("get-arg",get_arg_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "returns the *i*'th parameter in *expression*, or "
	  "*default* (otherwise)",
	  {"expr",kno_any_type,KNO_VOID},
	  {"elt",kno_fixnum_type,KNO_VOID},
	  {"dflt",kno_any_type,KNO_VOID})
static lispval get_arg_prim(lispval expr,lispval elt,lispval dflt)
{
  if (PAIRP(expr))
    if (KNO_UINTP(elt)) {
      int i = 0, lim = FIX2INT(elt); lispval scan = expr;
      while ((i<lim) && (PAIRP(scan))) {
	scan = KNO_CDR(scan);
	i++;}
      if (PAIRP(scan))
	return kno_incref(KNO_CAR(scan));
      else return kno_incref(dflt);}
    else return kno_type_error(_("fixnum"),"get_arg_prim",elt);
  else return kno_type_error(_("pair"),"get_arg_prim",expr);
}

/* APPLY */

DEFC_PRIMN("apply",apply_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "`(apply *fns* *args...* *rest*)` applies *fns* (a choice) to "
	   "the concatneation of *args* and the value of *rest*, "
	   "which may be either a pair, a vector, or the empty list.")
static lispval apply_lexpr(int n,kno_argvec args)
{
  DO_CHOICES(fn,args[0])
    if (!(KNO_APPLICABLEP(args[0]))) {
      KNO_STOP_DO_CHOICES;
      return kno_type_error("function","apply_lexpr",args[0]);}
  {
    lispval results = EMPTY;
    /* TODO: Pre-assemble the argcount and catch non-sequences in the
       last position. */
    DO_CHOICES(fn,args[0]) {
      DO_CHOICES(final_arg,args[n-1]) {
	lispval result = VOID;
	int final_length = kno_seq_length(final_arg);
	int n_args = (n-2)+final_length;
	lispval *values = u8_alloc_n(n_args,lispval);
	int i = 1, j = 0, lim = n-1;
	/* Copy regular arguments */
	while (i<lim) {values[j]=kno_incref(args[i]); j++; i++;}
	i = 0; while (j<n_args) {
	  values[j]=kno_seq_elt(final_arg,i);
	  j++; i++;}
	result = kno_apply(fn,n_args,values);
	if (KNO_ABORTED(result)) {
	  kno_decref(results);
	  KNO_STOP_DO_CHOICES;
	  i = 0; while (i<n_args) {
	    kno_decref(values[i]);
	    i++;}
	  u8_free(values);
	  results = result;
	  break;}
	else {CHOICE_ADD(results,result);}
	i = 0; while (i<n_args) {
	  kno_decref(values[i]);
	  i++;}
	u8_free(values);}
      if (KNO_ABORTED(results)) {
	KNO_STOP_DO_CHOICES;
	return results;}}
    if (KNO_PRECHOICEP(results))
      return kno_simplify_value(results);
    else return results;
  }
}


/* Caching support */

DEFC_PRIMN("cachecall",cachecall,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "**undocumented**")
static lispval cachecall(int n,kno_argvec args)
{
  if (HASHTABLEP(args[0]))
    return kno_xcachecall((kno_hashtable)args[0],args[1],n-2,args+2);
  else return kno_cachecall(args[0],n-1,args+1);
}

DEFC_PRIMN("cachecall/probe",cachecall_probe,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "**undocumented**")
static lispval cachecall_probe(int n,kno_argvec args)
{
  if (HASHTABLEP(args[0]))
    return kno_xcachecall_try((kno_hashtable)args[0],args[1],n-2,args+2);
  else return kno_cachecall_try(args[0],n-1,args+1);
}

DEFC_PRIMN("cachedcall?",cachedcallp,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "**undocumented**")
static lispval cachedcallp(int n,kno_argvec args)
{
  if (HASHTABLEP(args[0]))
    if (kno_xcachecall_probe((kno_hashtable)args[0],args[1],n-2,args+2))
      return KNO_TRUE;
    else return KNO_FALSE;
  else if (kno_cachecall_probe(args[0],n-1,args+1))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("clear-callcache!",clear_callcache,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval clear_callcache(lispval arg)
{
  kno_clear_callcache(arg);
  return VOID;
}

#if 0
DEFC_PRIMN("thread/cachecall",tcachecall,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "**undocumented**")
static lispval tcachecall(int n,kno_argvec args)
{
  return kno_tcachecall(args[0],n-1,args+1);
}
#endif

static lispval with_threadcache_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct KNO_THREAD_CACHE *tc = kno_push_threadcache(NULL);
  lispval value = VOID;
  KNO_DOLIST(each,KNO_CDR(expr)) {
    kno_decref(value);
    value = kno_eval(each,env,_stack);
    if (KNO_ABORTED(value)) {
      kno_pop_threadcache(tc);
      return value;}}
  kno_pop_threadcache(tc);
  return value;
}

static lispval using_threadcache_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct KNO_THREAD_CACHE *tc = kno_use_threadcache();
  lispval value = VOID;
  KNO_DOLIST(each,KNO_CDR(expr)) {
    kno_decref(value);
    value = kno_eval(each,env,_stack);
    if (KNO_ABORTED(value)) {
      if (tc) kno_pop_threadcache(tc);
      return value;}}
  if (tc) kno_pop_threadcache(tc);
  return value;
}

DEFC_PRIM("use-threadcache",use_threadcache_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval use_threadcache_prim(lispval arg)
{
  if (FALSEP(arg)) {
    if (!(kno_threadcache)) return KNO_FALSE;
    while (kno_threadcache) kno_pop_threadcache(kno_threadcache);
    return KNO_TRUE;}
  else {
    struct KNO_THREAD_CACHE *tc = kno_use_threadcache();
    if (tc) return KNO_TRUE;
    else return KNO_FALSE;}
}

/* FFI */

#if KNO_ENABLE_FFI
DEFC_PRIMN("ffi/proc",ffi_proc,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(3),
	   "**undocumented**")
static lispval ffi_proc(int n,kno_argvec args)
{
  lispval name_arg = args[0], filename_arg = args[1];
  lispval return_type = args[2];
  u8_string name = (STRINGP(name_arg)) ? (CSTRING(name_arg)) : (NULL);
  u8_string filename = (STRINGP(filename_arg)) ?
    (CSTRING(filename_arg)) :
    (NULL);
  if (!(name))
    return kno_type_error("String","ffi_proc/name",name_arg);
  else if (!( (STRINGP(filename_arg)) || (FALSEP(filename_arg)) ))
    return kno_type_error("String","ffi_proc/filename",filename_arg);
  else {
    struct KNO_FFI_PROC *fcn=
      kno_make_ffi_proc(name,filename,n-3,return_type,args+3);
    if (fcn)
      return (lispval) fcn;
    else return KNO_ERROR;}
}

DEFC_PRIM("ffi/found?",ffi_foundp_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"name",kno_string_type,KNO_VOID},
	  {"modname",kno_any_type,KNO_VOID})
static lispval ffi_foundp_prim(lispval name,lispval modname)
{
  void *module=NULL, *sym=NULL;
  if (STRINGP(modname)) {
    module=u8_dynamic_load(CSTRING(modname));
    if (module==NULL) {
      kno_clear_errors(0);
      return KNO_FALSE;}}
  sym=u8_dynamic_symbol(CSTRING(name),module);
  if (sym)
    return KNO_TRUE;
  else return KNO_FALSE;
}
#else
DEFC_PRIMN("ffi/proc",ffi_proc,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(3),
	   "**undocumented**")
static lispval ffi_proc(int n,lispval *args)
{
  u8_seterr("NotImplemented","ffi_proc",
	    u8_strdup("No FFI support is available in this build of Kno"));
  return KNO_ERROR;
}
DEFC_PRIM("ffi/found?",ffi_foundp_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"name",kno_string_type,KNO_VOID},
	  {"modname",kno_any_type,KNO_VOID})
static lispval ffi_foundp_prim(lispval name,lispval modname)
{
  return KNO_FALSE;
}
#endif

/* Choice operations */

DEFC_PRIM("%fixchoice",fixchoice_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval fixchoice_prim(lispval arg)
{
  if (PRECHOICEP(arg))
    return kno_make_simple_choice(arg);
  else return kno_incref(arg);
}

DEFC_PRIM("%choiceref",choiceref_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID},
	  {"off",kno_any_type,KNO_VOID})
static lispval choiceref_prim(lispval arg,lispval off)
{
  if (USUALLY(FIXNUMP(off))) {
    long long i = FIX2INT(off);
    if (EMPTYP(arg)) {
      kno_seterr(kno_RangeError,"choiceref_prim","0",arg);
      return KNO_ERROR;}
    else if (CHOICEP(arg)) {
      struct KNO_CHOICE *ch = (kno_choice)arg;
      if (i>ch->choice_size) {
	u8_byte buf[50];
	return kno_err(kno_RangeError,"choiceref_prim",
		       u8_sprintf(buf,50,"%lld",i),
		       arg);}
      else {
	lispval elt = KNO_XCHOICE_DATA(ch)[i];
	return kno_incref(elt);}}
    else if (i == 0)
      return kno_incref(arg);
    else {
      u8_byte buf[50];
      return kno_err(kno_RangeError,"choiceref_prim",
		     u8_sprintf(buf,50,"%lld",i),
		     arg);}}
  else return kno_type_error("fixnum","choiceref_prim",off);
}

/* Checking version numbers */

static int check_num(lispval arg,int num)
{
  if ((!(FIXNUMP(arg)))||(FIX2INT(arg)<0)) {
    kno_seterr(kno_TypeError,"check_version_prim",NULL,arg);
    return -2;}
  else {
    int n = FIX2INT(arg);
    if (n<num)
      return 1;
    else if (num == n)
      return 0;
    else return -1;}
}

DEFC_PRIMN("check-version",check_version_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "**undocumented**")
static lispval check_version_prim(int n,kno_argvec args)
{
  int rv = check_num(args[0],KNO_MAJOR_VERSION);
  if (rv<-1) return KNO_ERROR;
  else if (rv>0) return KNO_TRUE;
  else if (rv<0) return KNO_FALSE;
  else if (n==1) return KNO_TRUE;
  else rv = check_num(args[1],KNO_MINOR_VERSION);
  if (rv<-1) return KNO_ERROR;
  else if (rv>0) return KNO_TRUE;
  else if (rv>0) return KNO_TRUE;
  else if (rv<0) return KNO_FALSE;
  else if (n==2) return KNO_TRUE;
  else rv = check_num(args[2],KNO_RELEASE_VERSION);
  if (rv<-1) return KNO_ERROR;
  else if (rv>0) return KNO_TRUE;
  else if (rv<0) return KNO_FALSE;
  else if (n==3) return KNO_TRUE;
  else rv = check_num(args[2],KNO_RELEASE_VERSION-1);
  /* The fourth argument should be a patch level, but we're not
     getting that in builds yet. So if there are more arguments,
     we see if required release number is larger than release-1
     (which means that we should be okay, since patch levels
     are reset with releases. */
  if (rv<1) return KNO_ERROR;
  else if (rv<0) return KNO_FALSE;
  else if (rv>0) return KNO_TRUE;
  else {
    int i = 3; while (i<n) {
      if (!(FIXNUMP(args[i])))
	return kno_err(kno_TypeError,"check_version_prim",NULL,args[i]);
      else i++;}
    return KNO_TRUE;}
}

DEFC_PRIMN("require-version",require_version_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "**undocumented**")
static lispval require_version_prim(int n,kno_argvec args)
{
  lispval result = check_version_prim(n,args);
  if (KNO_ABORTP(result))
    return result;
  else if (KNO_TRUEP(result))
    return result;
  else {
    u8_byte buf[50];
    int i = 0; while (i<n) {
      if (!(KNO_FIXNUMP(args[i])))
	return kno_type_error("version number(integer)","require_version_prim",args[i]);
      else i++;}
    lispval version_vec = kno_make_vector(n,(lispval *)args);
    kno_seterr("VersionError","require_version_prim",
	       u8_sprintf(buf,50,"Version is %s",KNO_REVISION),
	       /* We don't need to incref *args* because they're all fixnums */
	       version_vec);
    kno_decref(version_vec);
    return KNO_ERROR;}
}



/* Documentation, etc */

DEFC_PRIM("documentation",get_documentation,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
static lispval get_documentation(lispval x)
{
  u8_string doc = kno_get_documentation(x);
  if (doc)
    return kno_wrapstring(doc);
  else return KNO_FALSE;
}

/* Apropos */

/* Apropos */

DEFC_PRIM("apropos",apropos_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval apropos_prim(lispval arg)
{
  u8_string seeking; lispval all, results = EMPTY;
  regex_t *regex = NULL; u8_mutex *lock = NULL;
  if (SYMBOLP(arg)) seeking = SYM_NAME(arg);
  else if (STRINGP(arg)) seeking = CSTRING(arg);
  else if (KNO_REGEXP(arg)) {
    struct KNO_REGEX *krx = (kno_regex) arg;
    regex = &(krx->rxcompiled);
    lock  = &(krx->rx_lock);
    u8_lock_mutex(lock);}
  else return kno_type_error(_("string or symbol"),"apropos",arg);
  all = kno_all_symbols();
  {DO_CHOICES(sym,all) {
      u8_string name = SYM_NAME(sym);
      if (regex) {
	int rv = regexec(regex,name,0,NULL,0);
	if (rv == REG_NOMATCH) {}
	else if (rv) {}
	else {CHOICE_ADD(results,sym);}}
      else if (strcasestr(name,seeking)) {
	CHOICE_ADD(results,sym);}
      else NO_ELSE;}}
  kno_decref(all);
  if (lock) u8_unlock_mutex(lock);
  return results;
}

/* Environment functions */

DEFC_PRIM("fcn/getalias",fcn_getalias_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "tries to return a function alias for *sym* in "
	  "*module*",
	  {"sym",kno_symbol_type,KNO_VOID},
	  {"env_arg",kno_any_type,KNO_VOID})
static lispval fcn_getalias_prim(lispval sym,lispval env_arg)
{
  lispval use_env = KNO_VOID, result = KNO_VOID;
  if ( (KNO_SYMBOLP(env_arg)) || (KNO_STRINGP(env_arg)) )
    use_env = kno_find_module(env_arg,1);
  else if (KNO_LEXENVP(env_arg))
    use_env = env_arg;
  else if ( (KNO_HASHTABLEP(env_arg)) &&
	    (kno_test(env_arg,KNOSYM_MODULEID,KNO_VOID)) )
    use_env = env_arg;
  else return kno_err("NotAModule","fcn_getalias_prim",
		      KNO_SYMBOL_NAME(sym),
		      env_arg);
  if (KNO_ABORTED(use_env))
    return use_env;
  else if (KNO_VOIDP(use_env))
    result = kno_err("BadEnvArg","fcnalias_prim",
		     KNO_SYMBOL_NAME(sym),
		     env_arg);
  else NO_ELSE;
  if (KNO_VOIDP(result)) {
    lispval val = (KNO_LEXENVP(use_env)) ?
      (kno_symeval(sym,(kno_lexenv)env_arg)) :
      (KNO_TABLEP(use_env)) ?
      (kno_get(use_env,sym,KNO_VOID)) :
      (KNO_VOID);
    if (KNO_ABORTED(val))
      result = val;
    else if (KNO_VOIDP(val))
      result = kno_err("Unbound","fcnalias_prim",
		       KNO_SYMBOL_NAME(sym),
		       use_env);
    else if ( (KNO_CONSP(val)) &&
	      ( (KNO_FUNCTIONP(val)) ||
		(KNO_APPLICABLEP(val)) ||
		(KNO_EVALFNP(val)) ||
		(KNO_MACROP(val)) ))
      result = kno_fcn_ref(sym,use_env,val);
    else result = kno_incref(val);}
  if ( use_env != env_arg) kno_decref(use_env);
  return result;
}

/* Access to kno_exec, the database layer interpreter */

DEFC_PRIM("kno/exec",kno_exec_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "calls the query interpreter on *expr* with "
	  "handlers from *envopts*",
	  {"expr",kno_any_type,KNO_VOID},
	  {"env",kno_any_type,KNO_VOID})
static lispval kno_exec_prim(lispval expr,lispval env)
{
  struct KNO_STACK exec_stack = { 0 };
  KNO_SETUP_STACK(&exec_stack,"kno/exec");
  KNO_STACK_SET_CALLER(&exec_stack,kno_stackptr);
  exec_stack.stack_op = expr;
  KNO_PUSH_STACK(&exec_stack);
  lispval val = kno_exec(expr,env,&exec_stack);
  KNO_POP_STACK(&exec_stack);
  return val;
}

/* The init function */

KNO_EXPORT void kno_init_evalops_c()
{
  u8_register_source_file(_FILEINFO);

  /* This pushes a new threadcache */
  kno_def_evalfn(kno_scheme_module,"WITH-THREADCACHE",with_threadcache_evalfn,
		 "*undocumented*");
  /* This ensures that there's an active threadcache, pushing a new one if
     needed or using the current one if it exists. */
  kno_def_evalfn(kno_scheme_module,"USING-THREADCACHE",using_threadcache_evalfn,
		 "*undocumented*");

  link_local_cprims();
}

DEFC_PRIM("%buildinfo",kno_get_build_info,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "Information about the build and startup "
	  "environment");

static void link_local_cprims()
{
  KNO_LINK_CPRIM("kno/exec",kno_exec_prim,2,kno_scheme_module);

  KNO_LINK_CPRIM("call/cc",callcc,1,kno_scheme_module);
  KNO_LINK_ALIAS("call-with-current-continuation",callcc,kno_scheme_module);

  KNO_LINK_CPRIMN("ffi/proc",ffi_proc,kno_scheme_module);
  KNO_LINK_CPRIM("ffi/found?",ffi_foundp_prim,2,kno_scheme_module);

  KNO_LINK_CPRIM("symbol-bound-in?",symbol_boundin_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("%choiceref",choiceref_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("get-arg",get_arg_prim,3,kno_scheme_module);

  KNO_LINK_CPRIM("fcn/getalias",fcn_getalias_prim,2,kno_scheme_module);

  KNO_LINK_CPRIM("documentation",get_documentation,1,kno_scheme_module);
  KNO_LINK_CPRIM("apropos",apropos_prim,1,kno_scheme_module);

  KNO_LINK_CPRIM("environment?",environmentp_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("%lexref",lexref_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("%lexrefval",lexref_value_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("%lexref?",lexrefp_prim,1,kno_scheme_module);

  KNO_LINK_CPRIM("%coderef",coderef_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("%coderefval",coderef_value_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("coderef?",coderefp_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("make-coderef",make_coderef,1,kno_scheme_module);

  KNO_LINK_CPRIM("%stack",getstack_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("%stackdepth",stackdepth_prim,0,kno_scheme_module);

  KNO_LINK_CPRIM("opcode-name",opcode_name_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("name->opcode",name2opcode_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("make-opcode",make_opcode,1,kno_scheme_module);
  KNO_LINK_CPRIM("opcode?",opcodep,1,kno_scheme_module);

  KNO_LINK_CPRIM("%fixchoice",fixchoice_prim,1,kno_scheme_module);

  KNO_LINK_CPRIMN("apply",apply_lexpr,kno_scheme_module);

  KNO_LINK_CPRIM("use-threadcache",use_threadcache_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("clear-callcache!",clear_callcache,1,kno_scheme_module);
#if 0
  KNO_LINK_CPRIMN("thread/cachecall",tcachecall,kno_scheme_module);
#endif
  KNO_LINK_CPRIMN("cachecall",cachecall,kno_scheme_module);
  KNO_LINK_CPRIMN("cachecall/probe",cachecall_probe,kno_scheme_module);
  KNO_LINK_CPRIMN("cachedcall?",cachedcallp,kno_scheme_module);

  KNO_LINK_CPRIMN("check-version",check_version_prim,kno_scheme_module);
  KNO_LINK_CPRIMN("require-version",require_version_prim,kno_scheme_module);
  KNO_LINK_CPRIM("%buildinfo",kno_get_build_info,0,kno_scheme_module);

}

