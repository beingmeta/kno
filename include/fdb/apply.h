/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_APPLY_H
#define FDB_APPLY_H 1
#define FDB_APPLY_H_VERSION "$Id$"
#include "fdb/dtype.h"

FD_EXPORT fd_exception fd_NotAFunction, fd_TooManyArgs, fd_TooFewArgs;

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
typedef fdtype (*fd_xprimn)(fd_function,int n,fdtype *);

#define FD_FUNCTION_FIELDS \
  FD_CONS_HEADER; u8_string name, filename;    \
  short ndprim, xprim, arity, min_arity;       \
  int *typeinfo; fdtype *defaults;             \
  union {                                      \
    fd_cprim0 call0; fd_cprim1 call1; fd_cprim2 call2; fd_cprim3 call3;          \
    fd_cprim4 call4; fd_cprim5 call5; fd_cprim6 call6; fd_cprimn calln;          \
    fd_xprim0 xcall0; fd_xprim1 xcall1; fd_xprim2 xcall2; fd_xprim3 xcall3;      \
    fd_xprim4 xcall4; fd_xprim5 xcall5; fd_xprim6 xcall6; fd_xprimn xcalln;      \
    void *fnptr;}                                                                \
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
FD_EXPORT fdtype fd_make_cprim0x(u8_string name,fd_cprim0 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim1x(u8_string name,fd_cprim1 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim2x(u8_string name,fd_cprim2 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim3x(u8_string name,fd_cprim3 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim4x(u8_string name,fd_cprim4 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim5x(u8_string name,fd_cprim5 fn,int mina,...);
FD_EXPORT fdtype fd_make_cprim6x(u8_string name,fd_cprim6 fn,int mina,...);
#define FD_FUNCTIONP(x) (FD_PRIM_TYPEP(x,fd_function_type))
#define FD_XFUNCTION(x) (FD_GET_CONS(x,fd_function_type,struct FD_FUNCTION *))

FD_EXPORT fdtype fd_make_ndprim(fdtype prim);

/* Definining functions in tables. */

FD_EXPORT void fd_defn(fdtype table,fdtype fcn);
FD_EXPORT void fd_idefn(fdtype table,fdtype fcn);
FD_EXPORT void fd_defalias(fdtype table,u8_string to,u8_string from);

/* Apply functions */

typedef fdtype (*fd_applyfn)(fdtype f,int n,fdtype *);
FD_EXPORT fd_applyfn fd_applyfns[];

FD_EXPORT fdtype fd_apply(struct FD_FUNCTION *,int n,fdtype *args);
FD_EXPORT fdtype fd_ndapply(struct FD_FUNCTION *,int n,fdtype *args);
FD_EXPORT fdtype fd_dapply(struct FD_FUNCTION *,int n,fdtype *args);

#define FD_APPLICABLEP(x) ((fd_applyfns[FD_PTR_TYPE(x)])!=NULL)

/* Profiling */

#if FD_CALLTRACK_ENABLED
#include <stdio.h>
FD_EXPORT int fd_start_profiling(u8_string name);
FD_EXPORT void fd_profile_call(u8_string name);
FD_EXPORT void fd_profile_return(u8_string name);
#else
#define fd_start_calltrack(x) (-1)
#define fd_calltrack_call(name);
#define fd_calltrack_return(name);
#endif

#endif /* FDB_APPLY_H */

