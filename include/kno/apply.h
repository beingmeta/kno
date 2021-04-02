/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
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
KNO_EXPORT u8_condition kno_VoidArgument, kno_SyntaxError;
KNO_EXPORT u8_condition kno_BadHandler, kno_NoHandler;

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
typedef lispval (*kno_cprimn)(int n,kno_argvec);

typedef lispval (*kno_xprimn)(kno_stack,kno_function,int n,kno_argvec);

typedef struct KNO_ARGINFO {
  lispval name;
  lispval type;
  lispval default_value;
  unsigned int bits;} *kno_arginfo;

#define KNO_FUNCTION_FIELDS						   \
  KNO_CONS_HEADER;							\
  u8_string fcn_name, fcn_filename;					\
  u8_string fcn_doc;							\
  lispval fcn_moduleid;							\
  unsigned char fcn_call, fcn_trace, fcn_free, fcn_other;		\
  short fcn_arity, fcn_min_arity, fcn_call_width, fcn_arginfo_len;	\
  lispval *fcn_argnames;							\
  lispval *fcn_typeinfo;						\
  lispval fcnid;							\
  lispval fcn_attribs;							\
  struct KNO_PROFILE *fcn_profile;					\
  union {								\
    kno_cprim0 call0; kno_cprim1 call1; kno_cprim2 call2;		\
    kno_cprim3 call3; kno_cprim4 call4; kno_cprim5 call5;		\
    kno_cprim6 call6; kno_cprim7 call7; kno_cprim8 call8;		\
    kno_cprim9 call9; kno_cprim10 call10; kno_cprim11 call11;		\
    kno_cprim12 call12; kno_cprim13 call13; kno_cprim14 call14;		\
    kno_cprim15 call15;							\
    kno_cprimn calln;							\
    kno_xprimn xcalln;							\
    void *fnptr;}							\
    fcn_handler

#define KNO_CALL_XITER    0x01
#define KNO_CALL_VARARGS  0x02
#define KNO_CALL_NOTAIL   0x04
#define KNO_CALL_CPRIM    0x08
#define KNO_CALL_XCALL    0x10
#define KNO_CALL_XPRUNE   0x20

#define KNO_FCN_FREE_DOC        0x01
#define KNO_FCN_FREE_TYPEINFO   0x02
#define KNO_FCN_FREE_DEFAULTS   0x04
#define KNO_FCN_FREE_SCHEMA     0x08
#define KNO_FCN_DECREF_TYPEINFO 0x10

#define KNO_FCN_TRACE_ENTER     0x01
#define KNO_FCN_TRACE_EXIT      0x02
#define KNO_FCN_TRACE_ARGS      0x04
#define KNO_FCN_TRACE_CUSTOM    0x08
#define KNO_FCN_TRACE_BREAK     0x10

#define KNO_CALL_NDCALL  (KNO_CALL_XITER|KNO_CALL_XPRUNE)

#define KNO_FCN_LOGGEDP(f)  ( ((f)->fcn_trace) & (KNO_CALL_LOGGING) )
#define KNO_FCN_TRACEDP(f)  ( ((f)->fcn_trace) & (KNO_CALL_TRACEFN) )
#define KNO_FCN_BREAKP(f)   ( ((f)->fcn_trace) & (KNO_CALL_BREAK) )

#define KNO_FCN_ITERP(f)   (! ( ((f)->fcn_call) & (KNO_CALL_XITER) ) )
#define KNO_FCN_VARARGP(f) (  ((f)->fcn_call) & (KNO_CALL_VARARGS) )
#define KNO_FCN_NOTAILP(f) (  ((f)->fcn_call) & (KNO_CALL_NOTAIL) )
#define KNO_FCN_CPRIMP(f)  (  ((f)->fcn_call) & (KNO_CALL_CPRIM) )
#define KNO_FCN_XCALLP(f)  (  ((f)->fcn_call) & (KNO_CALL_XCALL) )
#define KNO_FCN_PRUNEP(f)  (! (((f)->fcn_call) & (KNO_CALL_XPRUNE) ) )

#define KNO_FCN_NDCALLP(f) ( ((f)->fcn_call) & (KNO_CALL_NDCALL) )

#define KNO_FCN_FREE_DOCP(f)	  ( ((f)->fcn_free) & (KNO_FCN_FREE_DOC) )
#define KNO_FCN_FREE_TYPEINFOP(f) ( ((f)->fcn_free) & (KNO_FCN_FREE_TYPEINFO) )
#define KNO_FCN_FREE_DEFAULTSP(f) ( ((f)->fcn_free) & (KNO_FCN_FREE_DEFAULTS) )

struct KNO_FUNCTION {
  KNO_FUNCTION_FIELDS;
};

#define KNO_FCN_NAME(x) (((kno_function)x)->fcn_name)
#define KNO_FCN_FILENAME(x) (((kno_function)x)->fcn_filename)
#define KNO_FCN_DOCSTRING(x) (((kno_function)x)->fcn_doc)
#define KNO_FCN_MODULE(x) (((kno_function)x)->fcn_moduleid)
#define KNO_FCN_ARITY(x) (((kno_function)x)->fcn_arity)
#define KNO_FCN_MIN_ARITY(x) (((kno_function)x)->fcn_min_arity)
#define KNO_FCN_CALL_WIDTH(x) (((kno_function)x)->fcn_call_width)
#define KNO_FCN_ARGINFO_LEN(x) (((kno_function)x)->fcn_arginfo_len)
#define KNO_FCN_ARGNAMES(x) (((kno_function)x)->fcn_argnames)
#define KNO_FCN_ATTRIBS(x) (((kno_function)x)->fcn_attribs)
#define KNO_FCN_FCNID(x) (((kno_function)x)->fcn_fcnid)
#define KNO_FCN_PROFILE(x) (((kno_function)x)->fcn_profile)
#define KNO_FCN_PROFILEP(x) ((((kno_function)x)->fcn_profile)!=NULL)

struct KNO_CPRIM {
  KNO_FUNCTION_FIELDS;
  u8_string cprim_name;
  const lispval *fcn_defaults;};
typedef struct KNO_CPRIM KNO_CPRIM;
typedef struct KNO_CPRIM *kno_cprim;

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
 lispval *typeinfo,
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

/* FLags for primitive definitions */

/* [][priminfo][min][max] */

#define KNO_OPT_ARGS  0x08000
#define KNO_VAR_ARGS  0x00080
#define KNO_MAX_ARGS(n) ( (n < 0) ? (0x80) : ((n)&(0x7F)) )
#define KNO_MIN_ARGS(n) ( (n < 0) ? (0x00) : ( (0x8000) | (((n)&(0x7F))<<8) ) )

#define KNO_XCALL    0x10000
#define KNO_NDCALL   0x20000
#define KNO_NDOP     KNO_NDCALL
#define KNO_CHOICEOP KNO_NDCALL

#define KNO_ARGINFO_LEN(flags) ( ((flags)&(0xFF000000)) >> 16)

/* Useful macros */

KNO_EXPORT int _KNO_FUNCTION_TYPEP(int typecode);
KNO_EXPORT int _KNO_FUNCTIONP(lispval x);

KNO_EXPORT int _KNO_APPLICABLEP(lispval x);
KNO_EXPORT int _KNO_APPLICABLE_TYPEP(int typecode);

#if KNO_EXTREME_PROFILING
#define KNO_FUNCTIONP _KNO_FUNCTIONP
#define KNO_FUNCTION_TYPEP _KNO_FUNCTION_TYPEP
#else
#define KNO_FUNCTION_TYPEP(typecode) \
  ( (((typecode)&0xfc) == kno_function_type) ||	\
    (kno_isfunctionp[typecode]) )
#define KNO_FAST_FUNCTIONP(obj) \
  ( (KNO_CONSP(obj)) && \
    (((KNO_CONS_TYPEOF(obj))&0xfc) == kno_function_type) )

#define KNO_FUNCTIONP(x)		       \
  ( (KNO_XXCONS_TYPEP(x,kno_function_type)) ||		\
    ( ( KNO_FCNIDP(x) ) ?					\
      (KNO_FUNCTION_TYPEP(KNO_TYPEOF(kno_fcnid_ref(x)))) :	\
      (KNO_FUNCTION_TYPEP(KNO_TYPEOF(x))) ))

KNO_FASTOP kno_function KNO_XFUNCTION(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x))
    return (kno_function) x;
  else return KNO_ERR(NULL,kno_TypeError,"function",NULL,x);
}
#endif

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

KNO_EXPORT int _KNO_LAMBDAP(lispval x);

#if KNO_EXTREME_PROFILING
#define KNO_LAMBDAP _KNO_LAMBDAP
#else
#define KNO_LAMBDAP(x)					 \
  ((KNO_FCNIDP(x)) ?					 \
   (KNO_TYPEP((kno_fcnid_ref(x)),kno_lambda_type)) :	   \
   (KNO_TYPEP((x),kno_lambda_type)))
#endif

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

/* Apply functions */

KNO_EXPORT lispval kno_call(struct KNO_STACK *,lispval fp,int n,kno_argvec);
KNO_EXPORT lispval kno_dcall(struct KNO_STACK *,lispval,int n,kno_argvec);

#define kno_apply(fn,n_args,argv) (kno_call(kno_stackptr,fn,n_args,argv))
#define kno_dapply(fn,n_args,argv) (kno_dcall(kno_stackptr,fn,n_args,argv))

typedef int kno_dispatch_flags;
#define KNO_DISPATCH_ARG_MASK 0x0000FFFF
#define KNO_DISPATCH_OPTIONAL 0x80000000
#define KNO_DISPATCH_NOERR    0x40000000
#define KNO_DISPATCH_DCALL    0x20000000
#define KNO_DISPATCH_DECREF   0x10000000
#define KNO_DISPATCH_LOOKUP   0x08000000

KNO_EXPORT lispval kno_get_handler(lispval obj,lispval m);
KNO_EXPORT lispval kno_dispatch(kno_stack stack,
				lispval obj,lispval method,
				kno_dispatch_flags flags,
				kno_argvec args);
KNO_EXPORT lispval kno_type_dispatch(struct KNO_STACK *stack,
				     struct KNO_TYPEINFO *typeinfo,
				     lispval obj,lispval m,
				     kno_dispatch_flags flags,
				     kno_argvec args);
KNO_EXPORT lispval kno_dispatch_apply(struct KNO_STACK *stack,lispval handler,
				      kno_dispatch_flags flags,
				      int n_args,kno_argvec args);

typedef int (*kno_function_tracefn)
(lispval *,kno_function,int,kno_argvec,kno_stack,lispval *);
KNO_EXPORT kno_function_tracefn kno_call_tracefn;

KNO_EXPORT int kno_trace_call(kno_stack stack,kno_function f,
			      int n,kno_argvec argvec);

KNO_EXPORT int kno_trace_exit(kno_stack stack,lispval r,kno_function f,
				int n,kno_argvec argvec);


#define kno_send_flags kno_dispatch_flags
#define kno_send(obj,method,flags,args) (kno_dispatch(NULL,obj,method,flags,args))

KNO_EXPORT lispval kno_dispatch_unhandled(lispval obj,lispval message);
KNO_EXPORT lispval kno_dispatch_bad_handler(lispval obj,lispval m,lispval h);

KNO_EXPORT lispval kno_exec(lispval expr,lispval handlers,kno_stack stack);
KNO_EXPORT lispval kno_exec_extend(lispval add,lispval path);

KNO_EXPORT int _KNO_APPLICABLEP(lispval x);
KNO_EXPORT int _KNO_APPLICABLE_TYPEP(int typecode);

#if KNO_EXTREME_PROFILING
#define KNO_APPLICABLEP _KNO_APPLICABLEP
#define KNO_APPLICABLE_TYPEP _KNO_APPLICABLE_TYPEP
#else
#define KNO_APPLICABLE_TYPEP(typecode) \
  ( ( ((typecode) >= kno_cprim_type) && ((typecode) <= kno_rpc_type) ) || \
    ( (kno_applyfns[typecode]) != NULL) )

#define KNO_APPLICABLEP(x)				  \
  ((KNO_TYPEP(x,kno_fcnid_type)) ?			  \
   (KNO_APPLICABLE_TYPEP(KNO_FCNID_TYPEOF(x))) :	  \
   (KNO_APPLICABLE_TYPEP(KNO_PRIM_TYPE(x))) )

#endif

#define KNO_PREDICATEP(p) \
  ((KNO_FUNCTIONP(p)) ? \
  ( (KNO_FCN_ARITY(p)>0) && (KNO_FCN_MIN_ARITY(p)==1) ) : \
   (KNO_APPLICABLEP(p)))

KNO_EXPORT kno_function _KNO_GETFUNCTION(lispval x);

#if KNO_EXTREME_PROFILING
#define KNO_GETFUNCTION _KNO_GETFUNCTION
#else
#define KNO_GETFUNCTION(x)		\
  ((KNO_FCNIDP(x)) ?		       \
   ((kno_function)(kno_fcnid_ref(x))) : \
   ((kno_function)x))
#endif

KNO_EXPORT lispval kno_get_backtrace(struct KNO_STACK *stack);
KNO_EXPORT void kno_html_backtrace(u8_output out,lispval rep);

KNO_EXPORT int kno_extended_profiling;

/* Unparsing */

KNO_EXPORT int kno_unparse_function
(u8_output out,lispval x,u8_string name,u8_string before,u8_string after);

/* KNO_SOURCE aliases */

#if KNO_SOURCE
#define FCN_NDOPP KNO_FCN_NDCALLP
#define FCN_NOTAILP KNO_FCN_NOTAILP
#define FCN_XCALLP KNO_FCN_XCALLP
#define FCN_VARARGP KNO_FCN_VARARGP
#endif

#endif /* KNO_APPLY_H */

