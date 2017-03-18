/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_APPLY_H
#define FRAMERD_APPLY_H 1
#ifndef FRAMERD_APPLY_H_INFO
#define FRAMERD_APPLY_H_INFO "include/framerd/apply.h"
#endif

FD_EXPORT fd_exception fd_NotAFunction, fd_TooManyArgs, fd_TooFewArgs;

FD_EXPORT u8_context fd_apply_context;

typedef struct FD_FUNCTION FD_FUNCTION;
typedef struct FD_FUNCTION *fd_function;

#ifndef FD_MAX_APPLYFCNS
#define FD_MAX_APPLYFCNS 16
#endif

/* Various callables */

typedef fdtype (*fd_cprim0)();
typedef fdtype (*fd_cprim1)(fdtype);
typedef fdtype (*fd_cprim2)(fdtype,fdtype);
typedef fdtype (*fd_cprim3)(fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim4)(fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim5)(fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim6)(fdtype,fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim7)(fdtype,fdtype,fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim8)(fdtype,fdtype,fdtype,fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim9)(fdtype,fdtype,fdtype,fdtype,fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprimn)(int n,fdtype *);

typedef fdtype (*fd_xprim0)(fd_function);
typedef fdtype (*fd_xprim1)(fd_function,fdtype);
typedef fdtype (*fd_xprim2)(fd_function,fdtype,fdtype);
typedef fdtype (*fd_xprim3)(fd_function,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim4)(fd_function,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim5)(fd_function,
                            fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim6)(fd_function,fdtype,fdtype,
                            fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim7)(fd_function,fdtype,fdtype,
                            fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim8)(fd_function,fdtype,fdtype,
                            fdtype,fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim9)(fd_function,fdtype,fdtype,
                            fdtype,fdtype,fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprimn)(fd_function,int n,fdtype *);

#define FD_FUNCTION_FIELDS \
  FD_CONS_HEADER;							\
  u8_string fcn_name, fcn_filename;					\
  u8_stream fcn_documentation;						\
  unsigned int fcn_ndcall:1, fcn_xcall:1;				\
  short fcn_arity, fcn_min_arity;					\
  int *fcn_typeinfo;							\
  fdtype *fcn_defaults;							\
  union {                                                               \
    fd_cprim0 call0; fd_cprim1 call1; fd_cprim2 call2;                  \
    fd_cprim3 call3; fd_cprim4 call4; fd_cprim5 call5;                  \
    fd_cprim6 call6; fd_cprim7 call7; fd_cprim8 call8;                  \
    fd_cprim9 call9; fd_cprimn calln;                                   \
    fd_xprim0 xcall0; fd_xprim1 xcall1; fd_xprim2 xcall2;               \
    fd_xprim3 xcall3; fd_xprim4 xcall4; fd_xprim5 xcall5;               \
    fd_xprim6 xcall6; fd_xprim7 xcall7; fd_xprim8 xcall8;               \
    fd_xprim9 xcall9; fd_xprimn xcalln;                                 \
    void *fnptr;}                                                       \
  fcn_handler

struct FD_FUNCTION {
  FD_FUNCTION_FIELDS;
};

/* This maps types to whether they have function (FD_FUNCTION_FIELDS) header. */
FD_EXPORT short fd_functionp[];

/* Various primitive defining functions.  Declare an explicit type,
   like fd_cprim1, as an argument, will generate warnings when
   the declaration and the implementation don't match.  */
FD_EXPORT fdtype fd_make_cprimn(u8_string name,fd_cprimn fn,int mina);
FD_EXPORT fdtype fd_make_cprim0(u8_string name,fd_cprim0 fn,int mina);
FD_EXPORT fdtype fd_make_cprim1(u8_string name,fd_cprim1 fn,int mina);
FD_EXPORT fdtype fd_make_cprim2(u8_string name,fd_cprim2 fn,int mina);
FD_EXPORT fdtype fd_make_cprim3(u8_string name,fd_cprim3 fn,int mina);
FD_EXPORT fdtype fd_make_cprim4(u8_string name,fd_cprim4 fn,int mina);
FD_EXPORT fdtype fd_make_cprim5(u8_string name,fd_cprim5 fn,int mina);
FD_EXPORT fdtype fd_make_cprim6(u8_string name,fd_cprim6 fn,int mina);
FD_EXPORT fdtype fd_make_cprim7(u8_string name,fd_cprim7 fn,int mina);
FD_EXPORT fdtype fd_make_cprim8(u8_string name,fd_cprim8 fn,int mina);
FD_EXPORT fdtype fd_make_cprim9(u8_string name,fd_cprim9 fn,int mina);
FD_EXPORT fdtype fd_make_cprim0x(u8_string name,fd_cprim0 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim1x(u8_string name,fd_cprim1 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim2x(u8_string name,fd_cprim2 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim3x(u8_string name,fd_cprim3 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim4x(u8_string name,fd_cprim4 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim5x(u8_string name,fd_cprim5 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim6x(u8_string name,fd_cprim6 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim7x(u8_string name,fd_cprim7 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim8x(u8_string name,fd_cprim8 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim9x(u8_string name,fd_cprim9 fn,int mina,...);

#define FD_FUNCTIONP(x) (fd_functionp[FD_PRIM_TYPE(x)])
#define FD_XFUNCTION(x) \
  ((FD_FUNCTIONP(x)) ? \
   ((struct FD_FUNCTION *)(FD_CONS_DATA(fd_fcnid_ref(x)))) : \
   ((struct FD_FUNCTION *)(u8_raise(fd_TypeError,"function",NULL),NULL)))
#define FD_FUNCTION_ARITY(x)  \
  ((FD_FUNCTIONP(x)) ? \
   (((struct FD_FUNCTION *)(FD_CONS_DATA(fd_fcnid_ref(x))))->fcn_arity) : \
   (0))

/* #define FD_XFUNCTION(x) (fd_consptr(struct FD_FUNCTION *,x,fd_primfcn_type)) */
#define FD_PRIMITIVEP(x) (FD_TYPEP(x,fd_primfcn_type))

/* Forward reference. Note that fd_sproc_type is defined in the
   pointer type enum in ptr.h. */
#define FD_SPROCP(x) (FD_TYPEP((x),fd_sproc_type))

FD_EXPORT fdtype fd_make_ndprim(fdtype prim);

/* Definining functions in tables. */

FD_EXPORT void fd_defn(fdtype table,fdtype fcn);
FD_EXPORT void fd_idefn(fdtype table,fdtype fcn);
FD_EXPORT void fd_defalias(fdtype table,u8_string to,u8_string from);
FD_EXPORT void fd_defalias2(fdtype table,u8_string to,fdtype src,u8_string from);

/* Stack checking */

#if ((FD_THREADS_ENABLED)&&(FD_USE_TLS))
FD_EXPORT u8_tld_key fd_stack_limit_key;
#define fd_stack_limit ((ssize_t)u8_tld_get(fd_stack_limit_key))
#define fd_set_stack_limit(sz) u8_tld_set(fd_stack_limit_key,(void *)(sz))
#elif ((FD_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
FD_EXPORT __thread ssize_t fd_stack_limit;
#define fd_set_stack_limit(sz) fd_stack_limit=(sz)
#else
FD_EXPORT ssize_t stack_limit;
#define fd_set_stack_limit(sz) fd_stack_limit=(sz)
#endif

FD_EXPORT ssize_t fd_stack_setsize(ssize_t limit);
FD_EXPORT ssize_t fd_stack_resize(double factor);
FD_EXPORT int fd_stackcheck(void);
FD_EXPORT ssize_t fd_init_stack(void);

#define FD_INIT_STACK() fd_init_stack()

/* Profiling */

FD_EXPORT const int fd_calltrack_enabled;

#if FD_CALLTRACK_ENABLED
#include <stdio.h>
#ifndef FD_MAX_CALLTRACK_SENSORS
#define FD_MAX_CALLTRACK_SENSORS 64
#endif

#if ( (FD_CALLTRACK_ENABLED) && (FD_USE_TLS) )
FD_EXPORT u8_tld_key _fd_calltracking_key;
#define fd_calltracking (u8_tld_get(_fd_calltracking_key))
#elif ( (FD_CALLTRACK_ENABLED) && (HAVE__THREAD) )
FD_EXPORT __thread int fd_calltracking;
#elif (FD_CALLTRACK_ENABLED)
FD_EXPORT int fd_calltracking;
#else /* (! FD_CALLTRACK_ENABLED ) */
#define fd_calltracking (0)
#endif

typedef long (*fd_int_sensor)(void);
typedef double (*fd_dbl_sensor)(void);

typedef struct FD_CALLTRACK_SENSOR {
  u8_string name; int enabled;
  fd_int_sensor intfcn; fd_dbl_sensor dblfcn;} FD_CALLTRACK_SENSOR;
typedef FD_CALLTRACK_SENSOR *fd_calltrack_sensor;

typedef struct FD_CALLTRACK_DATUM {
  enum { ct_double, ct_int }  ct_type;
  union { double dblval; int intval;} ct_value;} FD_CALLTRACK_DATUM;
typedef struct FD_CALLTRACK_DATUM *fd_calltrack_datum;

FD_EXPORT fd_calltrack_sensor fd_get_calltrack_sensor(u8_string id,int);

FD_EXPORT fdtype fd_calltrack_sensors(void);
FD_EXPORT fdtype fd_calltrack_sense(int);

FD_EXPORT int fd_start_profiling(u8_string name);
FD_EXPORT void fd_profile_call(u8_string name);
FD_EXPORT void fd_profile_return(u8_string name);
#else
#define fd_start_calltrack(x) (-1)
#define fd_calltrack_call(name);
#define fd_calltrack_return(name);
#endif

/* Apply functions */

typedef fdtype (*fd_applyfn)(fdtype f,int n,fdtype *);
FD_EXPORT fd_applyfn fd_applyfns[];

FD_EXPORT fdtype fd_apply(fdtype,int n,fdtype *args);
FD_EXPORT fdtype fd_ndapply(fdtype,int n,fdtype *args);
FD_EXPORT fdtype fd_deterministic_apply(fdtype,int n,fdtype *args);

#if FD_CALLTRACK_ENABLED
FD_EXPORT fdtype fd_calltrack_apply(fdtype fn,int n_args,fdtype *argv);
#define fd_dapply(fn,n,argv)			\
  ((FD_EXPECT_FALSE(fd_calltracking)) ?		\
   (fd_calltrack_apply(fn,n,argv)) :		\
   (fd_deterministic_apply(fn,n,argv)))
#else
#define fd_dapply fd_deterministic_apply
#endif

#define FD_APPLICABLEP(x) \
  ((FD_TYPEP(x,fd_fcnid_type)) ?		\
   ((fd_applyfns[FD_FCNID_TYPE(x)])!=NULL) :	\
   ((fd_applyfns[FD_PRIM_TYPE(x)])!=NULL))

/* Tail calls */

#define FD_TAILCALL_ND_ARGS     1
#define FD_TAILCALL_ATOMIC_ARGS 2
#define FD_TAILCALL_VOID_VALUE  4

typedef struct FD_TAILCALL {
  FD_CONS_HEADER;
  int tailcall_flags;
  int tailcall_arity;
  fdtype tailcall_head;} *fd_tailcall;

FD_EXPORT fdtype fd_tail_call(fdtype fcn,int n,fdtype *vec);
FD_EXPORT fdtype fd_step_call(fdtype c);
FD_EXPORT fdtype _fd_finish_call(fdtype);

#define FD_TAILCALLP(x) (FD_TYPEP((x),fd_tailcall_type))

FD_INLINE_FCN fdtype fd_finish_call(fdtype pt)
{
  if (FD_TAILCALLP(pt))
    return _fd_finish_call(pt);
  else return pt;
}

#define FD_DTYPE2FCN(x)		     \
  ((FD_FCNIDP(x)) ?		     \
   ((fd_function)(fd_fcnid_ref(x))) : \
   ((fd_function)x))

/* Unparsing */

FD_EXPORT int fd_unparse_function
  (u8_output out,fdtype x,u8_string name,u8_string before,u8_string after);

#endif /* FRAMERD_APPLY_H */
