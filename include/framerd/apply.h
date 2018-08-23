/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_APPLY_H
#define FRAMERD_APPLY_H 1
#ifndef FRAMERD_APPLY_H_INFO
#define FRAMERD_APPLY_H_INFO "include/framerd/apply.h"
#endif

#if HAVE_STDATOMIC_H
#define _ATOMIC_DECL _Atomic
#define _ATOMIC_INIT(x) ATOMIC_VAR_INIT(x)
#else
#define _ATOMIC_DECL
#define _ATOMIC_INIT(x) (x)
#endif

#include "stacks.h"

FD_EXPORT u8_condition fd_NotAFunction, fd_TooManyArgs, fd_TooFewArgs;

FD_EXPORT int fd_wrap_apply;

#ifndef FD_WRAP_APPLY_DEFAULT
#define FD_WRAP_APPLY_DEFAULT 0
#endif

#ifndef FD_INLINE_STACKS
#define FD_INLINE_STACKS 0
#endif

#ifndef FD_INLINE_APPLY
#define FD_INLINE_APPLY 0
#endif

typedef struct FD_FUNCTION FD_FUNCTION;
typedef struct FD_FUNCTION *fd_function;

#ifndef FD_MAX_APPLYFCNS
#define FD_MAX_APPLYFCNS 16
#endif

/* Various callables */

typedef lispval (*fd_cprim0)();
typedef lispval (*fd_cprim1)(lispval);
typedef lispval (*fd_cprim2)(lispval,lispval);
typedef lispval (*fd_cprim3)(lispval,lispval,lispval);
typedef lispval (*fd_cprim4)(lispval,lispval,lispval,lispval);
typedef lispval (*fd_cprim5)(lispval,lispval,lispval,lispval,lispval);
typedef lispval (*fd_cprim6)(lispval,lispval,lispval,lispval,lispval,lispval);
typedef lispval (*fd_cprim7)(lispval,lispval,lispval,lispval,lispval,lispval,lispval);
typedef lispval (*fd_cprim8)(lispval,lispval,lispval,
                             lispval,lispval,lispval,
                             lispval,lispval);
typedef lispval (*fd_cprim9)(lispval,lispval,lispval,
                             lispval,lispval,lispval,
                             lispval,lispval,lispval);
typedef lispval (*fd_cprim10)(lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval);
typedef lispval (*fd_cprim11)(lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval);
typedef lispval (*fd_cprim12)(lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval);
typedef lispval (*fd_cprim13)(lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval);
typedef lispval (*fd_cprim14)(lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval);
typedef lispval (*fd_cprim15)(lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval);
typedef lispval (*fd_cprimn)(int n,lispval *);

typedef lispval (*fd_xprim0)(fd_function);
typedef lispval (*fd_xprim1)(fd_function,lispval);
typedef lispval (*fd_xprim2)(fd_function,lispval,lispval);
typedef lispval (*fd_xprim3)(fd_function,lispval,lispval,lispval);
typedef lispval (*fd_xprim4)(fd_function,lispval,lispval,lispval,lispval);
typedef lispval (*fd_xprim5)(fd_function,
                             lispval,lispval,lispval,lispval,lispval);
typedef lispval (*fd_xprim6)(fd_function,lispval,lispval,
                             lispval,lispval,lispval,lispval);
typedef lispval (*fd_xprim7)(fd_function,lispval,lispval,
                             lispval,lispval,lispval,lispval,lispval);
typedef lispval (*fd_xprim8)(fd_function,lispval,lispval,
                             lispval,lispval,lispval,lispval,lispval,lispval);
typedef lispval (*fd_xprim9)(fd_function,
                             lispval,lispval,lispval,
                             lispval,lispval,lispval,
                             lispval,lispval,lispval);
typedef lispval (*fd_xprim10)(fd_function,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval);
typedef lispval (*fd_xprim11)(fd_function,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval);
typedef lispval (*fd_xprim12)(fd_function,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval);
typedef lispval (*fd_xprim13)(fd_function,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval);
typedef lispval (*fd_xprim14)(fd_function,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval);
typedef lispval (*fd_xprim15)(fd_function,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval,
                              lispval,lispval,lispval);
typedef lispval (*fd_xprimn)(fd_function,int n,lispval *);

#define FD_FUNCTION_FIELDS                                                \
  FD_CONS_HEADER;                                                         \
  u8_string fcn_name, fcn_filename;                                       \
  u8_string fcn_documentation;                                            \
  lispval fcn_moduleid;                                                   \
  unsigned int fcn_ndcall:1, fcn_xcall:1, fcn_wrap_calls:1, fcn_notail:1; \
  unsigned int fcn_break:1, fcn_trace:3;                                  \
  lispval fcnid;                                                          \
  short fcn_arity, fcn_min_arity;                                         \
  lispval fcn_attribs;                                                    \
  int *fcn_typeinfo;                                                      \
  lispval *fcn_defaults;                                                  \
  struct FD_PROFILE *fcn_profile;                                         \
  union {                                                                 \
    fd_cprim0 call0; fd_cprim1 call1; fd_cprim2 call2;                    \
    fd_cprim3 call3; fd_cprim4 call4; fd_cprim5 call5;                    \
    fd_cprim6 call6; fd_cprim7 call7; fd_cprim8 call8;                    \
    fd_cprim9 call9; fd_cprim10 call10; fd_cprim11 call11;                \
    fd_cprim12 call12; fd_cprim13 call13; fd_cprim14 call14;              \
    fd_cprim15 call15;                                                    \
    fd_cprimn calln;                                                      \
    fd_xprim0 xcall0; fd_xprim1 xcall1; fd_xprim2 xcall2;                 \
    fd_xprim3 xcall3; fd_xprim4 xcall4; fd_xprim5 xcall5;                 \
    fd_xprim6 xcall6; fd_xprim7 xcall7; fd_xprim8 xcall8;                 \
    fd_xprim9 xcall9; fd_xprim10 xcall10; fd_xprim11 xcall11;             \
    fd_xprim12 xcall12; fd_xprim13 xcall13; fd_xprim14 xcall14;           \
    fd_xprim15 xcall15;                                                   \
    fd_xprimn xcalln;                                                     \
    void *fnptr;}                                                         \
    fcn_handler

struct FD_FUNCTION {
  FD_FUNCTION_FIELDS;
};

/* This maps types to whether they have function (FD_FUNCTION_FIELDS) header. */
FD_EXPORT short fd_functionp[];

FD_EXPORT lispval fd_new_cprimn(u8_string name,u8_string filename,u8_string doc,fd_cprimn fn,int min_arity,int ndcall,int xcall);
FD_EXPORT lispval fd_new_cprim0(u8_string name,u8_string filename,u8_string doc,fd_cprim0 fn,int xcall);
FD_EXPORT lispval fd_new_cprim1(u8_string name,u8_string filename,u8_string doc,fd_cprim1 fn,int min_arity,int ndcall,int xcall,int type0,lispval dflt0);
FD_EXPORT lispval fd_new_cprim2(u8_string name,u8_string filename,u8_string doc,fd_cprim2 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1);
FD_EXPORT lispval fd_new_cprim3(u8_string name,u8_string filename,u8_string doc,fd_cprim3 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2);
FD_EXPORT lispval fd_new_cprim4(u8_string name,u8_string filename,u8_string doc,fd_cprim4 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2,int type3,lispval dflt3);
FD_EXPORT lispval fd_new_cprim5(u8_string name,u8_string filename,u8_string doc,fd_cprim5 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2,int type3,lispval dflt3,int type4,lispval dflt4);
FD_EXPORT lispval fd_new_cprim6(u8_string name,u8_string filename,u8_string doc,fd_cprim6 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2,int type3,lispval dflt3,int type4,lispval dflt4,int type5,lispval dflt5);
FD_EXPORT lispval fd_new_cprim7(u8_string name,u8_string filename,u8_string doc,fd_cprim7 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2,int type3,lispval dflt3,int type4,lispval dflt4,int type5,lispval dflt5,int type6,lispval dflt6);
FD_EXPORT lispval fd_new_cprim8(u8_string name,u8_string filename,u8_string doc,fd_cprim8 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2,int type3,lispval dflt3,int type4,lispval dflt4,int type5,lispval dflt5,int type6,lispval dflt6,int type7,lispval dflt7);
FD_EXPORT lispval fd_new_cprim9(u8_string name,u8_string filename,u8_string doc,fd_cprim9 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2,int type3,lispval dflt3,int type4,lispval dflt4,int type5,lispval dflt5,int type6,lispval dflt6,int type7,lispval dflt7,int type8,lispval dflt8);
FD_EXPORT lispval fd_new_cprim10(u8_string name,u8_string filename,u8_string doc,fd_cprim10 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2,int type3,lispval dflt3,int type4,lispval dflt4,int type5,lispval dflt5,int type6,lispval dflt6,int type7,lispval dflt7,int type8,lispval dflt8,int type9,lispval dflt9);
FD_EXPORT lispval fd_new_cprim11(u8_string name,u8_string filename,u8_string doc,fd_cprim11 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2,int type3,lispval dflt3,int type4,lispval dflt4,int type5,lispval dflt5,int type6,lispval dflt6,int type7,lispval dflt7,int type8,lispval dflt8,int type9,lispval dflt9,int type10,lispval dflt10);FD_EXPORT lispval fd_new_cprim12(u8_string name,u8_string filename,u8_string doc,fd_cprim12 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2,int type3,lispval dflt3,int type4,lispval dflt4,int type5,lispval dflt5,int type6,lispval dflt6,int type7,lispval dflt7,int type8,lispval dflt8,int type9,lispval dflt9,int type10,lispval dflt10,int type11,lispval dflt11);
FD_EXPORT lispval fd_new_cprim13(u8_string name,u8_string filename,u8_string doc,fd_cprim13 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2,int type3,lispval dflt3,int type4,lispval dflt4,int type5,lispval dflt5,int type6,lispval dflt6,int type7,lispval dflt7,int type8,lispval dflt8,int type9,lispval dflt9,int type10,lispval dflt10,int type11,lispval dflt11,int type12,lispval dflt12);
FD_EXPORT lispval fd_new_cprim14(u8_string name,u8_string filename,u8_string doc,fd_cprim14 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2,int type3,lispval dflt3,int type4,lispval dflt4,int type5,lispval dflt5,int type6,lispval dflt6,int type7,lispval dflt7,int type8,lispval dflt8,int type9,lispval dflt9,int type10,lispval dflt10,int type11,lispval dflt11,int type12,lispval dflt12,int type13,lispval dflt13);
FD_EXPORT lispval fd_new_cprim15(u8_string name,u8_string filename,u8_string doc,fd_cprim15 fn,int min_arity,int ndcall,int call,int type0,lispval dflt0,int type1,lispval dflt1,int type2,lispval dflt2,int type3,lispval dflt3,int type4,lispval dflt4,int type5,lispval dflt5,int type6,lispval dflt6,int type7,lispval dflt7,int type8,lispval dflt8,int type9,lispval dflt9,int type10,lispval dflt10,int type11,lispval dflt11,int type12,lispval dflt12,int type13,lispval dflt13,int type14,lispval dflt14);

#define fd_make_cprimn(name,fn,min_arity)               \
  fd_new_cprimn(name,_FILEINFO,NULL,fn,min_arity,0,0)
#define fd_make_cprimN(name,fn,min_arity)               \
  fd_new_cprimn(name,_FILEINFO,NULL,fn,min_arity,0,0)

#define fd_make_cprim0(name,fn)                 \
  fd_new_cprim0(name,_FILEINFO,NULL,fn,0)
#define fd_make_cprim1(name,fn,min)                                     \
  fd_new_cprim1(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                -1,FD_VOID)
#define fd_make_cprim2(name,fn,min)                                     \
  fd_new_cprim2(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                -1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim3(name,fn,min)                                     \
  fd_new_cprim3(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                -1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim4(name,fn,min)                                     \
  fd_new_cprim4(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim5(name,fn,min)                                     \
  fd_new_cprim5(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim6(name,fn,min)                                     \
  fd_new_cprim6(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID, \
                -1,FD_VOID)
#define fd_make_cprim7(name,fn,min)                                     \
  fd_new_cprim7(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                -1,FD_VOID-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
                -1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim8(name,fn,min)                                     \
  fd_new_cprim8(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                -1,FD_VOID-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
                -1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim9(name,fn,min)                                     \
  fd_new_cprim9(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                -1,FD_VOID-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
                -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim10(name,fn,min)                                    \
  fd_new_cprim10(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim11(name,fn,min)                                    \
  fd_new_cprim11(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID, \
                 -1,FD_VOID)
#define fd_make_cprim12(name,fn,min)                                    \
  fd_new_cprim12(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID, \
                 -1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim13(name,fn,min)                                    \
  fd_new_cprim13(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1)
#define fd_make_cprim14(name,fn,min)                                    \
  fd_new_cprim14(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim15(name,fn,min)                                    \
  fd_new_cprim15(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID, \
                 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)

#define fd_make_cprim1x(name,fn,min,...)                                \
  fd_new_cprim1(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim2x(name,fn,min,...)                                \
  fd_new_cprim2(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim3x(name,fn,min,...)                                \
  fd_new_cprim3(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim4x(name,fn,min,...)                                \
  fd_new_cprim4(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim5x(name,fn,min,...)                                \
  fd_new_cprim5(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim6x(name,fn,min,...)                                \
  fd_new_cprim6(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim7x(name,fn,min,...)                                \
  fd_new_cprim7(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim8x(name,fn,min,...)                                \
  fd_new_cprim8(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim9x(name,fn,min,...)                                \
  fd_new_cprim9(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim10x(name,fn,min,...)                               \
  fd_new_cprim10(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim11x(name,fn,min,...)                               \
  fd_new_cprim11(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim12x(name,fn,min,...)                               \
  fd_new_cprim12(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim13x(name,fn,min,...)                               \
  fd_new_cprim13(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim14x(name,fn,min,...)                               \
  fd_new_cprim14(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim15x(name,fn,min,...)                               \
  fd_new_cprim15(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)

#define fd_idefnN(module,name,fn,min,doc)                               \
  fd_idefn(module,fd_new_cprimn                                         \
           (name,_FILEINFO,doc,fn,((min)&0xFFFF),((min)&0x10000),0))

#define fd_idefn0(module,name,fn,doc)                           \
  fd_idefn(module,fd_new_cprim0(name,_FILEINFO,doc,fn,0))
#define fd_idefn1(module,name,fn,min,doc,...)                           \
  fd_idefn(module,fd_new_cprim1(name,_FILEINFO,doc,fn,((min)&0xFFFF),   \
                                ((min)&0x10000),0,                      \
                                __VA_ARGS__))
#define fd_idefn2(module,name,fn,min,doc,...)           \
  fd_idefn(module,fd_new_cprim2(name,_FILEINFO,doc,fn,                  \
                                ((min)&0xFFFF),((min)&0x10000),0,       \
                                __VA_ARGS__))
#define fd_idefn3(module,name,fn,min,doc,...)           \
  fd_idefn(module,fd_new_cprim3(name,_FILEINFO,doc,fn,                  \
                                ((min)&0xFFFF),((min)&0x10000),0,       \
                                __VA_ARGS__))
#define fd_idefn4(module,name,fn,min,doc,...)           \
  fd_idefn(module,fd_new_cprim4(name,_FILEINFO,doc,fn,                  \
                                ((min)&0xFFFF),((min)&0x10000),0,       \
                                __VA_ARGS__))
#define fd_idefn5(module,name,fn,min,doc,...)           \
  fd_idefn(module,fd_new_cprim5(name,_FILEINFO,doc,fn,                  \
                                ((min)&0xFFFF),((min)&0x10000),0,       \
                                __VA_ARGS__))
#define fd_idefn6(module,name,fn,min,doc,...)           \
  fd_idefn(module,fd_new_cprim6(name,_FILEINFO,doc,fn,                  \
                                ((min)&0xFFFF),((min)&0x10000),0,       \
                                __VA_ARGS__))
#define fd_idefn7(module,name,fn,min,doc,...)           \
  fd_idefn(module,fd_new_cprim7(name,_FILEINFO,doc,fn,                  \
                                ((min)&0xFFFF),((min)&0x10000),0,       \
                                __VA_ARGS__))
#define fd_idefn8(module,name,fn,min,doc,...)           \
  fd_idefn(module,fd_new_cprim8(name,_FILEINFO,doc,fn,                  \
                                ((min)&0xFFFF),((min)&0x10000),0,       \
                                __VA_ARGS__))
#define fd_idefn9(module,name,fn,min,doc,...)           \
  fd_idefn(module,fd_new_cprim9(name,_FILEINFO,doc,fn,                  \
                                ((min)&0xFFFF),((min)&0x10000),0,       \
                                __VA_ARGS__))
#define fd_idefn10(module,name,fn,min,doc,...)          \
  fd_idefn(module,fd_new_cprim10(name,_FILEINFO,doc,fn,                 \
                                 ((min)&0xFFFF),((min)&0x10000),0,      \
                                 __VA_ARGS__))
#define fd_idefn11(module,name,fn,min,doc,...)                          \
  fd_idefn(module,fd_new_cprim11(name,_FILEINFO,doc,fn,                 \
                                 ((min)&0xFFFF),((min)&0x10000),0,      \
                                 __VA_ARGS__))
#define fd_idefn12(module,name,fn,min,doc,...)          \
  fd_idefn(module,fd_new_cprim12(name,_FILEINFO,doc,fn,                 \
                                 ((min)&0xFFFF),((min)&0x10000),0,      \
                                 __VA_ARGS__))
#define fd_idefn13(module,name,fn,min,doc,...)          \
  fd_idefn(module,fd_new_cprim13(name,_FILEINFO,doc,fn,                 \
                                 ((min)&0xFFFF),((min)&0x10000),0,      \
                                 __VA_ARGS__))
#define fd_idefn14(module,name,fn,min,doc,...)          \
  fd_idefn(module,fd_new_cprim14(name,_FILEINFO,doc,fn,                 \
                                 ((min)&0xFFFF),((min)&0x10000),0,      \
                                 __VA_ARGS__))
#define fd_idefn15(module,name,fn,min,doc,...)                          \
  fd_idefn(module,fd_new_cprim15(name,_FILEINFO,doc,fn,                 \
                                 ((min)&0xFFFF),((min)&0x10000),0,      \
                                 __VA_ARGS__))

#define FD_NEEDS_0_ARGS 0
#define FD_NEEDS_1_ARG  1
#define FD_NEEDS_1_ARGS 1
#define FD_NEEDS_2_ARGS 2
#define FD_NEEDS_3_ARGS 3
#define FD_NEEDS_4_ARGS 4
#define FD_NEEDS_5_ARGS 5
#define FD_NEEDS_6_ARGS 6
#define FD_NEEDS_7_ARGS 7
#define FD_NEEDS_8_ARGS 8
#define FD_NEEDS_9_ARGS 9
#define FD_NEEDS_10_ARGS 10
#define FD_NEEDS_11_ARGS 11
#define FD_NEEDS_12_ARGS 12
#define FD_NEEDS_13_ARGS 13
#define FD_NEEDS_14_ARGS 14
#define FD_NEEDS_15_ARGS 15

#define FD_NDCALL 0x10000

#define FD_FUNCTIONP(x) (fd_functionp[FD_PRIM_TYPE(x)])
#define FD_XFUNCTION(x)                         \
  ((FD_FUNCTIONP(x)) ?                                       \
   ((struct FD_FUNCTION *)(FD_CONS_DATA(fd_fcnid_ref(x)))) :            \
   ((struct FD_FUNCTION *)(u8_raise(fd_TypeError,"function",NULL),NULL)))
#define FD_FUNCTION_ARITY(x)                    \
  ((FD_FUNCTIONP(x)) ?                                                  \
   (((struct FD_FUNCTION *)(FD_CONS_DATA(fd_fcnid_ref(x))))->fcn_arity) : \
   (0))

/* #define FD_XFUNCTION(x) (fd_consptr(struct FD_FUNCTION *,x,fd_cprim_type)) */
#define FD_PRIMITIVEP(x)                                \
  ((FD_FCNIDP(x)) ?                                     \
   (FD_TYPEP((fd_fcnid_ref(x)),fd_cprim_type)) :        \
   (FD_TYPEP((x),fd_cprim_type)))

/* Forward reference. Note that fd_lambda_type is defined in the
   pointer type enum in ptr.h. */

#define FD_LAMBDAP(x)                                   \
  ((FD_FCNIDP(x)) ?                                     \
   (FD_TYPEP((fd_fcnid_ref(x)),fd_lambda_type)) :       \
   (FD_TYPEP((x),fd_lambda_type)))

FD_EXPORT lispval fd_make_ndprim(lispval prim);

/* Primitive defining macros */

#define FD_CPRIM(cname,scm_name, ...)           \
  static lispval cname(__VA_ARGS__)
#define FD_NDPRIM(cname,scm_name, ...)          \
  static lispval cname(__VA_ARGS__)

/* Definining functions in tables. */

FD_EXPORT void fd_defn(lispval table,lispval fcn);
FD_EXPORT void fd_idefn(lispval table,lispval fcn);
FD_EXPORT void fd_defalias(lispval table,u8_string to,u8_string from);
FD_EXPORT void fd_defalias2(lispval table,u8_string to,lispval src,u8_string from);

/* Stack checking */

#if ((FD_THREADS_ENABLED)&&(FD_USE_TLS))
FD_EXPORT u8_tld_key fd_stack_limit_key;
#define fd_stack_limit ((ssize_t)u8_tld_get(fd_stack_limit_key))
#define fd_set_stack_limit(sz) u8_tld_set(fd_stack_limit_key,(void *)(sz))
#elif ((FD_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
FD_EXPORT __thread ssize_t fd_stack_limit;
#define fd_set_stack_limit(sz) fd_stack_limit = (sz)
#else
FD_EXPORT ssize_t stack_limit;
#define fd_set_stack_limit(sz) fd_stack_limit = (sz)
#endif

FD_EXPORT ssize_t fd_stack_setsize(ssize_t limit);
FD_EXPORT ssize_t fd_stack_resize(double factor);
FD_EXPORT int fd_stackcheck(void);
FD_EXPORT ssize_t fd_init_cstack(void);

#define FD_INIT_CSTACK() fd_init_cstack()

/* Tail calls */

#define FD_TAILCALL_ND_ARGS     1
#define FD_TAILCALL_ATOMIC_ARGS 2
#define FD_TAILCALL_VOID_VALUE  4

typedef struct FD_TAILCALL {
  FD_CONS_HEADER;
  int tailcall_flags;
  int tailcall_arity;
  lispval tailcall_head;} *fd_tailcall;

FD_EXPORT lispval fd_tail_call(lispval fcn,int n,lispval *vec);
FD_EXPORT lispval fd_step_call(lispval c);
FD_EXPORT lispval _fd_finish_call(lispval);

#define FD_TAILCALLP(x) (FD_TYPEP((x),fd_tailcall_type))

FD_INLINE_FCN lispval fd_finish_call(lispval pt)
{
  if (!(FD_EXPECT_TRUE(FD_CHECK_PTR(pt))))
    return fd_badptr_err(pt,"fd_finish_call",NULL);
  else if (FD_TAILCALLP(pt)) {
    lispval v = _fd_finish_call(pt);
    if (FD_PRECHOICEP(v))
      return fd_simplify_choice(v);
    else return v;}
  else if (FD_PRECHOICEP(pt))
    return fd_simplify_choice(pt);
  else return pt;
}

/* Apply functions */

typedef lispval (*fd_applyfn)(lispval f,int n,lispval *);
FD_EXPORT fd_applyfn fd_applyfns[];

FD_EXPORT lispval fd_call(struct FD_STACK *stack,lispval fp,int n,lispval *args);
FD_EXPORT lispval fd_ndcall(struct FD_STACK *stack,lispval,int n,lispval *args);
FD_EXPORT lispval fd_dcall(struct FD_STACK *stack,lispval,int n,lispval *args);

FD_EXPORT lispval _fd_stack_apply(struct FD_STACK *stack,lispval fn,int n_args,lispval *args);
FD_EXPORT lispval _fd_stack_dapply(struct FD_STACK *stack,lispval fn,int n_args,lispval *args);
FD_EXPORT lispval _fd_stack_ndapply(struct FD_STACK *stack,lispval fn,int n_args,lispval *args);

#if FD_INLINE_APPLY
U8_MAYBE_UNUSED static
lispval fd_stack_apply(struct FD_STACK *stack,lispval fn,int n_args,lispval *args)
{
  lispval result= (stack) ?
    (fd_call(stack,fn,n_args,args)) :
    (fd_call(fd_stackptr,fn,n_args,args));
  return fd_finish_call(result);
}
static U8_MAYBE_UNUSED
lispval fd_stack_dapply(struct FD_STACK *stack,lispval fn,int n_args,lispval *args)
{
  lispval result= (stack) ?
    (fd_dcall(stack,fn,n_args,args)) :
    (fd_dcall(fd_stackptr,fn,n_args,args));
  return fd_finish_call(result);
}
static U8_MAYBE_UNUSED
lispval fd_stack_ndapply(struct FD_STACK *stack,lispval fn,int n_args,lispval *args)
{
  lispval result= (stack) ?
    (fd_ndcall(stack,fn,n_args,args)) :
    (fd_ndcall(fd_stackptr,fn,n_args,args));
  return fd_finish_call(result);
}
#else
#define fd_stack_apply _fd_stack_apply
#define fd_stack_dapply _fd_stack_dapply
#define fd_stack_ndapply _fd_stack_ndapply
#endif

#define fd_apply(fn,n_args,argv) (fd_stack_apply(fd_stackptr,fn,n_args,argv))
#define fd_ndapply(fn,n_args,argv) (fd_stack_ndapply(fd_stackptr,fn,n_args,argv))
#define fd_dapply(fn,n_args,argv) (fd_stack_dapply(fd_stackptr,fn,n_args,argv))

#define FD_APPLICABLEP(x)                       \
  ((FD_TYPEP(x,fd_fcnid_type)) ?                \
   ((fd_applyfns[FD_FCNID_TYPE(x)])!=NULL) :    \
   ((fd_applyfns[FD_PRIM_TYPE(x)])!=NULL))

#define FD_DTYPE2FCN(x)              \
  ((FD_FCNIDP(x)) ?                   \
   ((fd_function)(fd_fcnid_ref(x))) : \
   ((fd_function)x))

FD_EXPORT lispval fd_get_backtrace(struct FD_STACK *stack);
FD_EXPORT void fd_html_backtrace(u8_output out,lispval rep);

FD_EXPORT int fd_profiling;

/* Unparsing */

FD_EXPORT int fd_unparse_function
(u8_output out,lispval x,u8_string name,u8_string before,u8_string after);

#endif /* FRAMERD_APPLY_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
