/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_APPLY_H
#define KNO_APPLY_H 1
#ifndef KNO_APPLY_H_INFO
#define KNO_APPLY_H_INFO "include/kno/apply.h"
#endif

#if HAVE_STDATOMIC_H
#define _ATOMIC_DECL _Atomic
#define _ATOMIC_INIT(x) ATOMIC_VAR_INIT(x)
#else
#define _ATOMIC_DECL
#define _ATOMIC_INIT(x) (x)
#endif

#include "stacks.h"

KNO_EXPORT u8_condition kno_NotAFunction, kno_TooManyArgs, kno_TooFewArgs;

KNO_EXPORT int kno_wrap_apply;

#ifndef KNO_WRAP_APPLY_DEFAULT
#define KNO_WRAP_APPLY_DEFAULT 0
#endif

#ifndef KNO_INLINE_STACKS
#define KNO_INLINE_STACKS 0
#endif

#ifndef KNO_INLINE_APPLY
#define KNO_INLINE_APPLY 0
#endif

typedef struct KNO_FUNCTION KNO_FUNCTION;
typedef struct KNO_FUNCTION *kno_function;

#ifndef KNO_MAX_APPLYFCNS
#define KNO_MAX_APPLYFCNS 16
#endif

/* Various callables */

typedef lispval (*kno_cprim0)();
typedef lispval (*kno_cprim1)(lispval);
typedef lispval (*kno_cprim2)(lispval,lispval);
typedef lispval (*kno_cprim3)(lispval,lispval,lispval);
typedef lispval (*kno_cprim4)(lispval,lispval,lispval,lispval);
typedef lispval (*kno_cprim5)(lispval,lispval,lispval,lispval,lispval);
typedef lispval (*kno_cprim6)(lispval,lispval,lispval,lispval,lispval,lispval);
typedef lispval (*kno_cprim7)(lispval,lispval,lispval,lispval,lispval,lispval,lispval);
typedef lispval (*kno_cprim8)(lispval,lispval,lispval,
			     lispval,lispval,lispval,
			     lispval,lispval);
typedef lispval (*kno_cprim9)(lispval,lispval,lispval,
			     lispval,lispval,lispval,
			     lispval,lispval,lispval);
typedef lispval (*kno_cprim10)(lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval);
typedef lispval (*kno_cprim11)(lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval);
typedef lispval (*kno_cprim12)(lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval,lispval);
typedef lispval (*kno_cprim13)(lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval);
typedef lispval (*kno_cprim14)(lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval);
typedef lispval (*kno_cprim15)(lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval,lispval,
			      lispval,lispval,lispval);
typedef lispval (*kno_cprimn)(int n,lispval *);

typedef lispval (*kno_xprimn)(kno_stack,kno_function,int n,lispval *);

#define KNO_FUNCTION_FIELDS						   \
  KNO_CONS_HEADER;							   \
  u8_string fcn_name, fcn_filename;					  \
  u8_string fcn_doc;							  \
  lispval fcn_moduleid;							  \
  unsigned char fcn_call, fcn_trace, fcn_free, fcn_other;		\
  lispval fcnid;							  \
  short fcn_call_len, fcn_arity, fcn_min_arity;				\
  lispval fcn_attribs;							  \
  struct KNO_PROFILE *fcn_profile;					   \
  union {								  \
    kno_cprim0 call0; kno_cprim1 call1; kno_cprim2 call2;		     \
    kno_cprim3 call3; kno_cprim4 call4; kno_cprim5 call5;		     \
    kno_cprim6 call6; kno_cprim7 call7; kno_cprim8 call8;		     \
    kno_cprim9 call9; kno_cprim10 call10; kno_cprim11 call11;		     \
    kno_cprim12 call12; kno_cprim13 call13; kno_cprim14 call14;		     \
    kno_cprim15 call15;							   \
    kno_cprimn calln;							   \
    kno_xprimn xcalln;							   \
    void *fnptr;}							  \
    fcn_handler

#define KNO_FCN_FREE_DOC 1
#define KNO_FCN_FREE_TYPEINFO 2
#define KNO_FCN_FREE_DEFAULTS 4

#define KNO_FCN_TRACE_PROFILE 1
#define KNO_FCN_TRACE_LOGGING 2
#define KNO_FCN_TRACE_TRACEFN 4
#define KNO_FCN_TRACE_BREAK   8

#define KNO_FCN_CALL_NDCALL 1
#define KNO_FCN_CALL_LEXPR  2
#define KNO_FCN_CALL_NOTAIL 4
#define KNO_FCN_CALL_CPRIM  8
#define KNO_FCN_CALL_XCALL  16

#define KNO_FCN_PROFILEP(f) ( ((f)->fcn_trace) & (KNO_FCN_TRACE_PROFILE) )
#define KNO_FCN_LOGGEDP(f)  ( ((f)->fcn_trace) & (KNO_FCN_CALL_LOGGING) )
#define KNO_FCN_TRACEDP(f)  ( ((f)->fcn_trace) & (KNO_FCN_CALL_TRACEFN) )
#define KNO_FCN_BREAKP(f)   ( ((f)->fcn_trace) & (KNO_FCN_CALL_BREAK) )

#define KNO_FCN_NDCALLP(f) ( ((f)->fcn_call) & (KNO_FCN_CALL_NDCALL) )
#define KNO_FCN_LEXPRP(f) ( ((f)->fcn_call) & (KNO_FCN_CALL_LEXPR) )
#define KNO_FCN_NOTAILP(f) ( ((f)->fcn_call) & (KNO_FCN_CALL_NOTAIL) )
#define KNO_FCN_CPRIMP(f) ( ((f)->fcn_call) & (KNO_FCN_CALL_CPRIM) )
#define KNO_FCN_XCALLP(f) ( ((f)->fcn_call) & (KNO_FCN_CALL_XCALL) )

#define KNO_FCN_FREE_DOCP(f)	  ( ((f)->fcn_free) & (KNO_FCN_FREE_DOC) )
#define KNO_FCN_FREE_TYPEINFOP(f) ( ((f)->fcn_free) & (KNO_FCN_FREE_TYPEINFO) )
#define KNO_FCN_FREE_DEFAULTSP(f) ( ((f)->fcn_free) & (KNO_FCN_FREE_DEFAULTS) )

struct KNO_FUNCTION {
  KNO_FUNCTION_FIELDS;
};

struct KNO_CPRIM {
  KNO_FUNCTION_FIELDS;
  u8_string cprim_name;
  int *fcn_typeinfo;
  lispval *fcn_defaults;};
typedef struct KNO_CPRIM KNO_CPRIM;
typedef struct KNO_CPRIM *kno_cprim;

typedef struct KNO_CPRIM_INFO {
  u8_string pname, cname, filename, docstring;
  int arity, flags;} KNO_CPRIM_INFO;

KNO_EXPORT u8_string kno_fcn_sig(struct KNO_FUNCTION *fcn,u8_byte namebuf[100]);

KNO_EXPORT lispval kno_init_cprim2
(u8_string name,u8_string cname,u8_string filename,u8_string doc,kno_cprim2 fn,int flags,
 int types[2],lispval dflts[2]);

KNO_EXPORT struct KNO_CPRIM *kno_init_cprim
(u8_string name,u8_string cname,
 int arity,
 u8_string filename,
 u8_string doc,
 int flags,
 int *typeinfo,
 lispval *defaults);

KNO_EXPORT lispval kno_cons_cprim0
(u8_string name,u8_string cname,u8_string filename,u8_string doc,int flags,
 kno_cprim0 fn);
KNO_EXPORT lispval kno_cons_cprim1
(u8_string name,u8_string cname,u8_string filename,u8_string doc,int flags,
 kno_cprim1 fn);
KNO_EXPORT lispval kno_cons_cprim2
(u8_string name,u8_string cname,u8_string filename,u8_string doc,int flags,
 kno_cprim2 fn);
KNO_EXPORT lispval kno_cons_cprim3
(u8_string name,u8_string cname,u8_string filename,u8_string doc,int flags,
 kno_cprim3 fn);
KNO_EXPORT lispval kno_cons_cprimN
(u8_string name,u8_string cname,u8_string filename,u8_string doc,int flags,
 kno_cprimn fn);

#ifndef STRINGIFY
#define TOSTRING(x) #x
#define STRINGIFY(x) TOSTRING(x)
#endif

#define kno_make_cprim0(name,fn,flags,doc) \
  kno_cons_cprim0(name,# name,\
		  _FILEINFO " L#" STRINGIFY(__LINE__),	\
		  doc,flags,fn)
#define kno_make_cprim1(name,fn,flags,doc) \
  kno_cons_cprim1(name,# name,\
		  _FILEINFO " L#" STRINGIFY(__LINE__),	\
		  doc,flags,fn)
#define kno_make_cprim2(name,fn,flags,doc) \
  kno_cons_cprim2(name,# name,\
		  _FILEINFO " L#" STRINGIFY(__LINE__),	\
		  doc,flags,fn)
#define kno_make_cprim3(name,fn,flags,doc) \
  kno_cons_cprim3(name,# name,\
		  _FILEINFO " L#" STRINGIFY(__LINE__),	\
		  doc,flags,fn)

/* Adding primitives */

#define KNO_XCALL   0x10000
#define KNO_NDCALL  0x20000
#define KNO_LEXPR   0x40000
#define KNO_VARARGS KNO_LEXPR

/* Useful macros */

#define KNO_FUNCTION_TYPEP(typecode) \
  ( (typecode == kno_cprim_type) || (typecode == kno_lambda_type) || \
    (kno_functionp[typecode]) )

#define KNO_FUNCTIONP(x) \
  (KNO_FUNCTION_TYPEP(KNO_PRIM_TYPE(x)))
#define KNO_XFUNCTION(x)				      \
  ((KNO_FUNCTIONP(x)) ?							\
   ((struct KNO_FUNCTION *)(KNO_CONS_DATA(kno_fcnid_ref(x)))) :		\
   ((struct KNO_FUNCTION *)(u8_raise(kno_TypeError,"function",NULL),NULL)))
#define KNO_FUNCTION_ARITY(x)						\
  ((KNO_FUNCTIONP(x)) ?							\
   (((struct KNO_FUNCTION *)(KNO_CONS_DATA(kno_fcnid_ref(x))))->fcn_arity) : \
   (0))

/* #define KNO_XFUNCTION(x) (kno_consptr(struct KNO_FUNCTION *,x,kno_cprim_type)) */
#define KNO_PRIMITIVEP(x)				 \
  ((KNO_FCNIDP(x)) ?					 \
   (KNO_TYPEP((kno_fcnid_ref(x)),kno_cprim_type)) :	   \
   (KNO_TYPEP((x),kno_cprim_type)))

/* Forward reference. Note that kno_lambda_type is defined in the
   pointer type enum in ptr.h. */

#define KNO_LAMBDAP(x)					 \
  ((KNO_FCNIDP(x)) ?					 \
   (KNO_TYPEP((kno_fcnid_ref(x)),kno_lambda_type)) :	   \
   (KNO_TYPEP((x),kno_lambda_type)))

KNO_EXPORT lispval kno_make_ndprim(lispval prim);

/* Primitive defining macros */

#define KNO_CPRIM(cname,scm_name, ...)		 \
  static lispval cname(__VA_ARGS__)
#define KNO_NDPRIM(cname,scm_name, ...)		 \
  static lispval cname(__VA_ARGS__)

#define KNO_CPRIMP(x) (KNO_TYPEP(x,kno_cprim_type))
#define KNO_XCPRIM(x)					   \
  ((KNO_CPRIMP(x)) ?						     \
   ((struct KNO_CPRIM *)(KNO_CONS_DATA(kno_fcnid_ref(x)))) :	     \
   ((struct KNO_CPRIM *)(u8_raise(kno_TypeError,"function",NULL),NULL)))


/* Definining functions in tables. */

KNO_EXPORT void kno_defn(lispval table,lispval fcn);
KNO_EXPORT void kno_idefn(lispval table,lispval fcn);
KNO_EXPORT void kno_defalias(lispval table,u8_string to,u8_string from);
KNO_EXPORT void kno_defalias2(lispval table,u8_string to,lispval src,u8_string from);

/* Stack checking */

#if ((KNO_THREADS_ENABLED)&&(KNO_USE_TLS))
KNO_EXPORT u8_tld_key kno_stack_limit_key;
#define kno_stack_limit ((ssize_t)u8_tld_get(kno_stack_limit_key))
#define kno_set_stack_limit(sz) u8_tld_set(kno_stack_limit_key,(void *)(sz))
#elif ((KNO_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
KNO_EXPORT __thread ssize_t kno_stack_limit;
#define kno_set_stack_limit(sz) kno_stack_limit = (sz)
#else
KNO_EXPORT ssize_t stack_limit;
#define kno_set_stack_limit(sz) kno_stack_limit = (sz)
#endif

KNO_EXPORT ssize_t kno_stack_setsize(ssize_t limit);
KNO_EXPORT ssize_t kno_stack_resize(double factor);
KNO_EXPORT int kno_stackcheck(void);
KNO_EXPORT ssize_t kno_init_cstack(void);

#define KNO_INIT_CSTACK() kno_init_cstack()

/* Tail calls */

#define KNO_TAILCALL_ND_ARGS	 1
#define KNO_TAILCALL_ATOMIC_ARGS 2
#define KNO_TAILCALL_VOID_VALUE	 4

typedef struct KNO_TAILCALL {
  KNO_CONS_HEADER;
  int tailcall_flags;
  int tailcall_arity;
  lispval tailcall_head;} *kno_tailcall;

KNO_EXPORT lispval kno_tail_call(lispval fcn,int n,lispval *vec);
KNO_EXPORT lispval kno_step_call(lispval c);
KNO_EXPORT lispval _kno_finish_call(lispval);

#define KNO_TAILCALLP(x) (KNO_TYPEP((x),kno_tailcall_type))

KNO_INLINE_FCN lispval kno_finish_call(lispval pt)
{
  if (!(KNO_EXPECT_TRUE(KNO_CHECK_PTR(pt))))
    return kno_badptr_err(pt,"kno_finish_call",NULL);
  else if (KNO_TAILCALLP(pt)) {
    lispval v = _kno_finish_call(pt);
    if (KNO_PRECHOICEP(v))
      return kno_simplify_choice(v);
    else return v;}
  else if (KNO_PRECHOICEP(pt))
    return kno_simplify_choice(pt);
  else return pt;
}

/* Apply functions */

KNO_EXPORT lispval kno_call(struct KNO_STACK *stack,lispval fp,int n,lispval *args);
KNO_EXPORT lispval kno_ndcall(struct KNO_STACK *stack,lispval,int n,lispval *args);
KNO_EXPORT lispval kno_dcall(struct KNO_STACK *stack,lispval,int n,lispval *args);

KNO_EXPORT lispval _kno_stack_apply(struct KNO_STACK *stack,lispval fn,int n_args,lispval *args);
KNO_EXPORT lispval _kno_stack_dapply(struct KNO_STACK *stack,lispval fn,int n_args,lispval *args);
KNO_EXPORT lispval _kno_stack_ndapply(struct KNO_STACK *stack,lispval fn,int n_args,lispval *args);

#if KNO_INLINE_APPLY
U8_MAYBE_UNUSED static
lispval kno_stack_apply(struct KNO_STACK *stack,lispval fn,int n_args,lispval *args)
{
  lispval result= (stack) ?
    (kno_call(stack,fn,n_args,args)) :
    (kno_call(kno_stackptr,fn,n_args,args));
  return kno_finish_call(result);
}
static U8_MAYBE_UNUSED
lispval kno_stack_dapply(struct KNO_STACK *stack,lispval fn,int n_args,lispval *args)
{
  lispval result= (stack) ?
    (kno_dcall(stack,fn,n_args,args)) :
    (kno_dcall(kno_stackptr,fn,n_args,args));
  return kno_finish_call(result);
}
static U8_MAYBE_UNUSED
lispval kno_stack_ndapply(struct KNO_STACK *stack,lispval fn,int n_args,lispval *args)
{
  lispval result= (stack) ?
    (kno_ndcall(stack,fn,n_args,args)) :
    (kno_ndcall(kno_stackptr,fn,n_args,args));
  return kno_finish_call(result);
}
#else
#define kno_stack_apply _kno_stack_apply
#define kno_stack_dapply _kno_stack_dapply
#define kno_stack_ndapply _kno_stack_ndapply
#endif

#define kno_apply(fn,n_args,argv) (kno_stack_apply(kno_stackptr,fn,n_args,argv))
#define kno_ndapply(fn,n_args,argv) (kno_stack_ndapply(kno_stackptr,fn,n_args,argv))
#define kno_dapply(fn,n_args,argv) (kno_stack_dapply(kno_stackptr,fn,n_args,argv))

KNO_EXPORT int _KNO_APPLICABLEP(lispval x);
KNO_EXPORT int _KNO_APPLICABLE_TYPEP(int typecode);

#if KNO_EXTREME_PROFILING
#define KNO_APPLICABLEP _KNO_APPLICABLEP
#define KNO_APPLICABLE_TYPEP _KNO_APPLICABLE_TYPEP
#else
#define KNO_APPLICABLE_TYPEP(typecode) \
  ( ( ((typecode) >= kno_cprim_type) && ((typecode) <= kno_dtproc_type) ) || \
    ( (kno_applyfns[typecode]) != NULL) )

#define KNO_APPLICABLEP(x)			 \
  ((KNO_TYPEP(x,kno_fcnid_type)) ?		  \
   (KNO_APPLICABLE_TYPEP(KNO_FCNID_TYPE(x))) :	  \
   (KNO_APPLICABLE_TYPEP(KNO_PRIM_TYPE(x))))
#endif

#define KNO_GETFUNCTION(x)		\
  ((KNO_FCNIDP(x)) ?		       \
   ((kno_function)(kno_fcnid_ref(x))) : \
   ((kno_function)x))

KNO_EXPORT lispval kno_get_backtrace(struct KNO_STACK *stack);
KNO_EXPORT void kno_html_backtrace(u8_output out,lispval rep);

KNO_EXPORT int kno_profiling;

/* Unparsing */

KNO_EXPORT int kno_unparse_function
(u8_output out,lispval x,u8_string name,u8_string before,u8_string after);

/* KNO_SOURCE aliases */

#if KNO_SOURCE
#define FCN_NDCALLP KNO_FCN_NDCALLP
#define FCN_NOTAILP KNO_FCN_NOTAILP
#define FCN_XCALLP KNO_FCN_XCALLP
#define FCN_LEXPRP KNO_FCN_LEXPRP
#endif

#endif /* KNO_APPLY_H */

