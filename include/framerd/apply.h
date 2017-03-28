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

#ifndef FD_INLINE_STACKS
#define FD_INLINE_STACKS 0
#endif

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
  u8_string fcn_documentation;						\
  unsigned int fcn_ndcall:1, fcn_xcall:1;				\
  short fcn_arity, fcn_min_arity;					\
  fdtype fcn_attribs;							\
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
FD_EXPORT fdtype fd_make_cprimn(u8_string name,fd_cprimn fn,int min_arity);
FD_EXPORT fdtype fd_make_cprim0(u8_string name,fd_cprim0 fn,int min_arity);
FD_EXPORT fdtype fd_make_cprim1(u8_string name,fd_cprim1 fn,int min_arity);
FD_EXPORT fdtype fd_make_cprim2(u8_string name,fd_cprim2 fn,int min_arity);
FD_EXPORT fdtype fd_make_cprim3(u8_string name,fd_cprim3 fn,int min_arity);
FD_EXPORT fdtype fd_make_cprim4(u8_string name,fd_cprim4 fn,int min_arity);
FD_EXPORT fdtype fd_make_cprim5(u8_string name,fd_cprim5 fn,int min_arity);
FD_EXPORT fdtype fd_make_cprim6(u8_string name,fd_cprim6 fn,int min_arity);
FD_EXPORT fdtype fd_make_cprim7(u8_string name,fd_cprim7 fn,int min_arity);
FD_EXPORT fdtype fd_make_cprim8(u8_string name,fd_cprim8 fn,int min_arity);
FD_EXPORT fdtype fd_make_cprim9(u8_string name,fd_cprim9 fn,int min_arity);
FD_EXPORT fdtype fd_make_cprim0x(u8_string name,fd_cprim0 fn,int min_arity);
FD_EXPORT fdtype fd_make_cprim1x
(u8_string name,fd_cprim1 fn,int min_arity,int type0,fdtype dflt0);
FD_EXPORT fdtype fd_make_cprim2x
(u8_string name,fd_cprim2 fn,
   int min_arity,int type0,fdtype dflt0,
   int type1,fdtype dflt1);
FD_EXPORT fdtype fd_make_cprim3x
(u8_string name,fd_cprim3 fn,
   int min_arity,int type0,fdtype dflt0,
   int type1,fdtype dflt1,int type2,fdtype dflt2);
FD_EXPORT fdtype fd_make_cprim4x
(u8_string name,fd_cprim4 fn,
   int min_arity,int type0,fdtype dflt0,
   int type1,fdtype dflt1,int type2,fdtype dflt2,
   int type3,fdtype dflt3);
FD_EXPORT fdtype fd_make_cprim5x
(u8_string name,fd_cprim5 fn,
   int min_arity,int type0,fdtype dflt0,
   int type1,fdtype dflt1,int type2,fdtype dflt2,
   int type3,fdtype dflt3,int type4,fdtype dflt4);
FD_EXPORT fdtype fd_make_cprim6x
(u8_string name,fd_cprim6 fn,
   int min_arity,int type0,fdtype dflt0,
   int type1,fdtype dflt1,int type2,fdtype dflt2,
   int type3,fdtype dflt3,int type4,fdtype dflt4,
   int type5,fdtype dflt5);
FD_EXPORT fdtype fd_make_cprim7x
(u8_string name,fd_cprim7 fn,
   int min_arity,int type0,fdtype dflt0,
   int type1,fdtype dflt1,int type2,fdtype dflt2,
   int type3,fdtype dflt3,int type4,fdtype dflt4,
   int type5,fdtype dflt5,int type6,fdtype dflt6);
FD_EXPORT fdtype fd_make_cprim8x
(u8_string name,fd_cprim8 fn,
   int min_arity,int type0,fdtype dflt0,
   int type1,fdtype dflt1,int type2,fdtype dflt2,
   int type3,fdtype dflt3,int type4,fdtype dflt4,
   int type5,fdtype dflt5,int type6,fdtype dflt6,
   int type7,fdtype dflt7);
FD_EXPORT fdtype fd_make_cprim9x
(u8_string name,fd_cprim9 fn,
   int min_arity,int type0,fdtype dflt0,
   int type1,fdtype dflt1,int type2,fdtype dflt2,
   int type3,fdtype dflt3,int type4,fdtype dflt4,
   int type5,fdtype dflt5,int type6,fdtype dflt6,
   int type7,fdtype dflt7,int type8,fdtype dflt8);

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

/* Stack frames (experimental) */

typedef enum FD_STACK_CLEANOP {
  FD_FREE_MEMORY,
  FD_DECREF,
  FD_FREE_VEC,
  FD_UNLOCK_MUTEX,
  FD_UNLOCK_RWLOCK,
  FD_DECREF_VEC} fd_stack_cleanop;

#define FD_STACK_DEFAULT_ARGVEC 10
#define FD_STACK_CLEANUP_QUANTUM 3

struct FD_STACK_CLEANUP {
  enum FD_STACK_CLEANOP cleanop;
  void *arg0, *arg1;};

typedef struct FD_STACK {
  FD_CONS_HEADER; /* We're not using this right now */
  u8_string stack_label;
  fdtype stack_op;
  fdtype stack_argbuf[FD_STACK_DEFAULT_ARGVEC];
  fdtype *stack_args;
  unsigned int stack_retvoid:1, stack_ndcall:1, stack_apply:1;
  unsigned int stack_free_op:1, stack_free_args:1, stack_free_argvec:1;
  unsigned int stack_mallocd:1;
  short stack_n_args, stack_n_cleanups;
  struct FD_STACK_CLEANUP _stack_cleanups[FD_STACK_CLEANUP_QUANTUM];
  struct FD_STACK_CLEANUP *stack_cleanups;
  struct FD_STACK *stack_caller;} *fd_stack;

#if (U8_USE_TLS)
FD_EXPORT u8_tld_key fd_call_stack_key;
#define fd_call_stack ((struct FD_STACK *)(u8_tld_get(fd_call_stack_key)))
#define set_call_stack(s) u8_tld_set(fd_call_stack_key,(s))
#elif (U8_USE__THREAD)
FD_EXPORT __thread struct FD_STACK *fd_call_stack;
#define set_call_stack(s) fd_call_stack=s
#else
#define set_call_stack(s) fd_call_stack=s
#endif

FD_EXPORT
struct FD_STACK *
_fd_setup_stack(struct FD_STACK *stack,struct FD_STACK *caller,
		u8_string label,fdtype op,short n_args,fdtype *args);
FD_EXPORT void
_fd_push_stack(struct FD_STACK *stack,struct FD_STACK *caller,
	       u8_string label,fdtype op,short n_args,fdtype *args);
FD_EXPORT void _fd_free_stack(struct FD_STACK *stack);
FD_EXPORT void _fd_pop_stack(struct FD_STACK *stack);
FD_EXPORT struct FD_STACK_CLEANUP *_fd_push_cleanup
(struct FD_STACK *stack,fd_stack_cleanop op);

#if FD_INLINE_STACKS
FD_FASTOP
fd_stack fd_setup_stack(struct FD_STACK *stack,struct FD_STACK *caller,
			u8_string label,fdtype op,
			short n_args,fdtype *args)
{
  FD_INIT_STATIC_CONS(stack,fd_stackframe_type);
  if (caller)
    stack->stack_caller=caller;
  else stack->stack_caller=fd_call_stack;
  if (label) stack->stack_label=label;
  if (n_args>0) {
    stack->stack_n_args=n_args;
    if (args) {
      stack->stack_args=args;}
    else if (n_args>5) {
      stack->stack_args=u8_alloc_n(n_args,fdtype);
      stack->stack_free_argvec=1;}
    else stack->stack_args=stack->stack_argbuf;}
  else stack->stack_n_args=n_args;
  stack->stack_cleanups=stack->_stack_cleanups;
  return stack;
}

FD_FASTOP void fd_push_stack(struct FD_STACK *stack,struct FD_STACK *caller,
			     u8_string label,fdtype op,
			     short n_args,fdtype *args)
{
  fd_stack fresh=fd_setup_stack(stack,caller,label,op,n_args,args);
  set_call_stack(fresh);
}

FD_FASTOP void fd_free_stack(struct FD_STACK *stack)
{
  if (stack->stack_free_op) fd_decref(stack->stack_op);
  if ((stack->stack_free_args)&&(stack->stack_args)) {
    fdtype *args=stack->stack_args;
    int i=0, n=stack->stack_n_args; while (i<n) {
      fdtype arg=args[i++]; fd_decref(arg);}}
  if (stack->stack_free_argvec) {
    if (stack->stack_args) u8_free(stack->stack_args);}
  if (stack->stack_n_cleanups) {
    struct FD_STACK_CLEANUP *cleanups=stack->stack_cleanups;
    int i=0, n=stack->stack_n_cleanups;
    while (i<n) {
      switch (cleanups[i].cleanop) {
      case FD_FREE_MEMORY:
	u8_free(cleanups[i].arg0); break;
      case FD_DECREF: {
	fdtype arg=(fdtype)cleanups[i].arg0;
	fd_decref(arg); break;}
      case FD_UNLOCK_MUTEX: {
	u8_mutex *lock=(u8_mutex *)cleanups[i].arg0;
	u8_unlock_mutex(lock); break;}
      case FD_UNLOCK_RWLOCK: {
	u8_rwlock *lock=(u8_rwlock *)cleanups[i].arg0;
	u8_rw_unlock(lock); break;}
      case FD_FREE_VEC: {
	fdtype *vec=(fdtype *)cleanups[i].arg0;
	ssize_t *sizep=(ssize_t *)cleanups[i].arg1;
	ssize_t size=*sizep;
	int i=0; while (i<size) {
	  fdtype elt=vec[i++]; fd_decref(elt);}
	break;}}
      i++;}}
}
FD_FASTOP void fd_pop_stack(struct FD_STACK *stack)
{
  struct FD_STACK *caller=stack->stack_caller;
  fd_free_stack(stack);
  set_call_stack(caller);
}

FD_FASTOP
struct FD_STACK_CLEANUP *
fd_push_cleanup(struct FD_STACK *stack,fd_stack_cleanop op)
{
  if ((stack->stack_n_cleanups==0) ||
      (stack->stack_n_cleanups%FD_STACK_CLEANUP_QUANTUM)) {
    int i=stack->stack_n_cleanups++;
    stack->stack_cleanups[i].cleanop=op;
    return &stack->stack_cleanups[i];}
  else {
    int size=stack->stack_n_cleanups;
    int new_size=size+FD_STACK_CLEANUP_QUANTUM;
    struct FD_STACK_CLEANUP *cleanups=stack->stack_cleanups;
    struct FD_STACK_CLEANUP *new_cleanups=
      u8_zalloc_n(new_size,struct FD_STACK_CLEANUP);
    memcpy(new_cleanups,stack->stack_cleanups,
	   size*sizeof(struct FD_STACK_CLEANUP));
    stack->stack_cleanups=new_cleanups;
    if (size>FD_STACK_CLEANUP_QUANTUM)
      u8_free(cleanups);
    int i=stack->stack_n_cleanups++;
    stack->stack_cleanups[i].cleanop=op;
    return &stack->stack_cleanups[i];}
}
#else /* not FD_INLINE_STACKS */
#define fd_setup_stack(stack,caller,label,op,n_args,args) \
  (_fd_setup_stack(stack,caller,label,op,n_args,args))
#define fd_pop_stack(stack) (_fd_pop_stack(stack))
#define fd_push_cleanup(stack,op) (_fd_push_cleanup(stack op))
#endif

#define fd_return(v) return (fd_pop_stack(_fdstack),(v))

#define FD_WITH_STACK(label,caller,op,n,args) \
  struct FD_STACK __fdstack, *_fdstack=&__fdstack; \
  fd_setup_stack(stack,label,caller,op,n,args)

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
