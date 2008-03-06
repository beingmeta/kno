/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_APPLY_H
#define FDB_APPLY_H 1
#define FDB_APPLY_H_VERSION "$Id$"
#include "fdb/dtype.h"

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
typedef fdtype (*fd_xprimn)(fd_function,int n,fdtype *);

#define FD_FUNCTION_FIELDS \
  FD_CONS_HEADER; u8_string name, filename;                             \
  short ndprim, xprim, arity, min_arity;                                \
  int *typeinfo; fdtype *defaults;                                      \
  union {                                                               \
    fd_cprim0 call0; fd_cprim1 call1; fd_cprim2 call2;                  \
    fd_cprim3 call3; fd_cprim4 call4; fd_cprim5 call5;                  \
    fd_cprim6 call6; fd_cprim7 call7;                                   \
    fd_cprimn calln;	                                                \
    fd_xprim0 xcall0; fd_xprim1 xcall1; fd_xprim2 xcall2;               \
    fd_xprim3 xcall3; fd_xprim4 xcall4; fd_xprim5 xcall5;               \
    fd_xprim6 xcall6; fd_xprim7 xcall7; fd_xprimn xcalln;		\
    void *fnptr;}                                                       \
  handler


struct FD_FUNCTION {
  FD_FUNCTION_FIELDS;
};

FD_EXPORT fdtype fd_make_cprimn(u8_string name,fd_cprimn fn,int mina);
FD_EXPORT fdtype fd_make_cprim0(u8_string name,fd_cprim0 fn,int mina);
FD_EXPORT fdtype fd_make_cprim1(u8_string name,fd_cprim1 fn,int mina);
FD_EXPORT fdtype fd_make_cprim2(u8_string name,fd_cprim2 fn,int mina);
FD_EXPORT fdtype fd_make_cprim3(u8_string name,fd_cprim3 fn,int mina);
FD_EXPORT fdtype fd_make_cprim4(u8_string name,fd_cprim4 fn,int mina);
FD_EXPORT fdtype fd_make_cprim5(u8_string name,fd_cprim5 fn,int mina);
FD_EXPORT fdtype fd_make_cprim6(u8_string name,fd_cprim6 fn,int mina);
FD_EXPORT fdtype fd_make_cprim7(u8_string name,fd_cprim7 fn,int mina);
FD_EXPORT fdtype fd_make_cprim0x(u8_string name,fd_cprim0 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim1x(u8_string name,fd_cprim1 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim2x(u8_string name,fd_cprim2 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim3x(u8_string name,fd_cprim3 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim4x(u8_string name,fd_cprim4 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim5x(u8_string name,fd_cprim5 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim6x(u8_string name,fd_cprim6 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim7x(u8_string name,fd_cprim7 fn,int mina,...);

#define FD_FUNCTIONP(x) (FD_PTR_TYPEP(x,fd_function_type))
#define FD_XFUNCTION(x) (FD_GET_CONS(x,fd_function_type,struct FD_FUNCTION *))

FD_EXPORT fdtype fd_make_ndprim(fdtype prim);

/* Definining functions in tables. */

FD_EXPORT void fd_defn(fdtype table,fdtype fcn);
FD_EXPORT void fd_idefn(fdtype table,fdtype fcn);
FD_EXPORT void fd_defalias(fdtype table,u8_string to,u8_string from);

/* Apply functions */

typedef fdtype (*fd_applyfn)(fdtype f,int n,fdtype *);
FD_EXPORT fd_applyfn fd_applyfns[];

FD_EXPORT fdtype fd_apply(fdtype,int n,fdtype *args);
FD_EXPORT fdtype fd_ndapply(fdtype,int n,fdtype *args);
FD_EXPORT fdtype fd_dapply(fdtype,int n,fdtype *args);

#define FD_APPLICABLEP(x) ((fd_applyfns[FD_PRIM_TYPE(x)])!=NULL)

/* Tail calls */

#define FD_TAIL_CALL_ND_ARGS     1
#define FD_TAIL_CALL_ATOMIC_ARGS 2

struct FD_TAIL_CALL {
  FD_CONS_HEADER;
  int n_elts, flags; fdtype head;};

FD_EXPORT fdtype fd_tail_call(fdtype fcn,int n,fdtype *vec);
FD_EXPORT fdtype fd_step_call(fdtype c);
FD_EXPORT fdtype _fd_finish_call(fdtype);

FD_INLINE_FCN fdtype fd_finish_call(fdtype pt)
{
  if (FD_PTR_TYPEP(pt,fd_tail_call_type))
    return _fd_finish_call(pt);
  else return pt;
}

#define FD_DTYPE2FCN(x) ((FD_PPTRP(x)) ? ((fd_function)(fd_pptr_ref(x))) : ((fd_function)x))

/* Profiling */

#if FD_CALLTRACK_ENABLED
#include <stdio.h>
#ifndef FD_MAX_CALLTRACK_SENSORS
#define FD_MAX_CALLTRACK_SENSORS 64
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

#endif /* FDB_APPLY_H */

