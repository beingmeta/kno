/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_CPRIMS_H
#define FRAMERD_CPRIMS_H 1
#ifndef FRAMERD_CPRIMS_H_INFO
#define FRAMERD_CPRIMS_H_INFO "include/framerd/cprims.h"
#endif

#include "framerd/apply.h"

#define TOSTRING(x) #x
#define STRINGIFY(x) TOSTRING(x)

/* DEFPRIM */

#define FD_DEFPRIM_DECL(pname,cname,arity,docstring)       \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, ((flags)&0x7f), flags}; \
  static int *cname ## _typeinfo = NULL;                                \
  static lispval *cname ## _defaults = NULL

#define FD_DEFPRIM(pname,cname,arity,docstring)       \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, ((flags)&0x7f), flags}; \
  static int *cname ## _typeinfo = NULL;                                \
  static lispval *cname ## _defaults = NULL;                            \
  static lispval cname

#define FD_DEFPRIM1(pname,cname,flags,docstring,t1,d1)      \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 1, flags};   \
  static int cname ## _typeinfo[1] = { t1 };                            \
  static lispval cname ## _defaults[1] = { d1 };                        \
  static lispval cname

#define FD_DEFPRIM2(pname,cname,flags,docstring,t1,d1,t2,d2)        \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 2, flags};   \
  static int cname ## _typeinfo[2] = { t1, t2 };                        \
  static lispval cname ## _defaults[2] = { d1, d2 };                    \
  static lispval cname

#define FD_DEFPRIM3(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3)  \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 3, flags};   \
  static int cname ## _typeinfo[3] = { t1, t2, t3 };                    \
  static lispval cname ## _defaults[3] = { d1, d2, d3 };                \
  static lispval cname

#define FD_DEFPRIM4(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3,t4,d4)    \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 4, flags};   \
  static int cname ## _typeinfo[4] = { t1, t2, t3, t4 };                \
  static lispval cname ## _defaults[4] = { d1, d2, d3, d4 };            \
  static lispval cname

#define FD_DEFPRIM5(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3,t4,d4,t5,d5) \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 5, flags};   \
  static int cname ## _typeinfo[5] = { t1, t2, t3, t4, t5 };            \
  static lispval cname ## _defaults[5] = { d1, d2, d3, d4, d5 };        \
  static lispval cname

#define FD_DEFPRIM6(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3,t4,d4,t5,d5,t6,d6) \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 6, flags};   \
  static int cname ## _typeinfo[6] = { t1, t2, t3, t4, t5, t6 };        \
  static lispval cname ## _defaults[6] = { d1, d2, d3, d4, d5, d6 };    \
  static lispval cname

#define FD_DEFPRIM7(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3,          \
                t4,d4,t5,d5,t6,d6,t7,d7)                                \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 7, flags};   \
  static int cname ## _typeinfo[7] = { t1, t2, t3, t4, t5, t6, t7 };    \
  static lispval cname ## _defaults[7] = { d1, d2, d3, d4, d5, d6, d7 }; \
  static lispval cname

#define FD_DEFPRIM8(pname,cname,flags,docstring,t1,d1,t2,d2,                \
                t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8)                    \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 8, flags};   \
  static int cname ## _typeinfo[8] = { t1, t2, t3, t4, t5, t6, t7, t8 }; \
  static lispval cname ## _defaults[8] = { d1, d2, d3, d4, d5, d6, d7, d8 }; \
  static lispval cname

#define FD_DEFPRIM9(pname,cname,flags,docstring,t1,d1,t2,d2,                \
                t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9)              \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 9, flags};   \
  static int cname ## _typeinfo[9] =                                    \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9 };                             \
  static lispval cname ## _defaults[9] =                                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9 };                             \
  static lispval cname

#define FD_DEFPRIM10(pname,cname,flags,docstring,t1,d1,t2,d2,               \
                 t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,t10,d10)     \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 10, flags};  \
  static int cname ## _typeinfo[10] =                                   \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9, t10 };                        \
  static lispval cname ## _defaults[7] =                                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9, d10 };                        \
  static lispval cname

#define FD_DEFPRIM11(pname,cname,flags,docstring,t1,d1,t2,d2,       \
                 t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,             \
                 t10,d10,t11,d11)                                       \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 11, flags};  \
  static int cname ## _typeinfo[11] =                                   \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11 };                   \
  static lispval cname ## _defaults[7] =                                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11 };                   \
  static lispval cname

#define FD_DEFPRIM12(pname,cname,flags,docstring,t1,d1,t2,d2,       \
                 t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,             \
                 t10,d10,t11,d11,t12,d12)                               \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 12, flags};  \
  static int cname ## _typeinfo[12] =                                   \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12 };              \
  static lispval cname ## _defaults[7] =                                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12 };              \
  static lispval cname

#define FD_DEFPRIM13(pname,cname,flags,docstring,t1,d1,t2,d2,       \
                 t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,             \
                 t10,d10,t11,d11,t12,d12,t13,d13)                       \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 13, flags};  \
  static int cname ## _typeinfo[12] =                                   \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13 };         \
  static lispval cname ## _defaults[7] =                                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13 };         \
  static lispval cname

#define FD_DEFPRIM14(pname,cname,flags,docstring,t1,d1,t2,d2,       \
                 t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,             \
                 t10,d10,t11,d11,t12,d12,t13,d13,t14,d14)               \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 14, flags};  \
  static int cname ## _typeinfo[12] =                                   \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14 };    \
  static lispval cname ## _defaults[7] =                                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14 };    \
  static lispval cname

#define FD_DEFPRIM15(pname,cname,flags,docstring,t1,d1,t2,d2,               \
                 t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,             \
                 t10,d10,t11,d11,t12,d12,t13,d13,t14,d14,t15,d15)       \
  static struct FD_CPRIM_INFO cname ## _info = {                        \
    pname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 15, flags};  \
  static int cname ## _typeinfo[12] =                                   \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15 }; \
  static lispval cname ## _defaults[7] =                                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 }; \
  static lispval cname


/* Registering the primitives */

#define FD_DECL_PRIM(cname,arity,module)        \
  fd_defprim ## arity(module,cname,&cname ## _info,             \
                      cname ## _typeinfo,                       \
                      cname ## _defaults)

#define FD_DECL_PRIM_N(cname,module)            \
  fd_defprimN(module,cname,&cname ## _info);

#define FD_DECL_ALIAS(alias,cname,module)              \
  fd_defalias(module,alias,cname ## _info.pname)

#if FRAMERD_SOURCE
#define DECL_PRIM    FD_DECL_PRIM
#define DECL_PRIM_N  FD_DECL_PRIM_N
#define DECL_ALIAS   FD_DECL_ALIAS

#define DEFPRIM_DECL FD_DEFPRIM_DECL
#define DEFPRIM      FD_DEFPRIM
#define DEFPRIM1     FD_DEFPRIM1
#define DEFPRIM2     FD_DEFPRIM2
#define DEFPRIM3     FD_DEFPRIM3
#define DEFPRIM4     FD_DEFPRIM4
#define DEFPRIM5     FD_DEFPRIM5
#define DEFPRIM6     FD_DEFPRIM6
#define DEFPRIM7     FD_DEFPRIM7
#define DEFPRIM8     FD_DEFPRIM8
#define DEFPRIM9     FD_DEFPRIM9
#define DEFPRIM10    FD_DEFPRIM10
#define DEFPRIM11    FD_DEFPRIM11
#define DEFPRIM12    FD_DEFPRIM12
#define DEFPRIM13    FD_DEFPRIM13
#define DEFPRIM14    FD_DEFPRIM14
#define DEFPRIM15    FD_DEFPRIM15
#endif

FD_EXPORT void fd_defprimN(lispval module,fd_cprimn fn,
                           struct FD_CPRIM_INFO *info);
FD_EXPORT void fd_defprim0(lispval module,fd_cprim0 fn,
                           struct FD_CPRIM_INFO *info,
                           unsigned int typeinfo[0],
                           lispval defaults[0]);
FD_EXPORT void fd_defprim1(lispval module,fd_cprim1 fn,
                           struct FD_CPRIM_INFO *info,
                           unsigned int typeinfo[1],
                           lispval defaults[1]);
FD_EXPORT void fd_defprim2(lispval module,fd_cprim2 fn,
                           struct FD_CPRIM_INFO *info,
                           unsigned int typeinfo[2],
                           lispval defaults[2]);
FD_EXPORT void fd_defprim3(lispval module,fd_cprim3 fn,
                           struct FD_CPRIM_INFO *info,
                           unsigned int typeinfo[3],
                           lispval defaults[3]);
FD_EXPORT void fd_defprim4(lispval module,fd_cprim4 fn,
                           struct FD_CPRIM_INFO *info,
                           unsigned int typeinfo[4],
                           lispval defaults[4]);
FD_EXPORT void fd_defprim5(lispval module,fd_cprim5 fn,
                           struct FD_CPRIM_INFO *info,
                           unsigned int typeinfo[5],
                           lispval defaults[5]);
FD_EXPORT void fd_defprim6(lispval module,fd_cprim6 fn,
                           struct FD_CPRIM_INFO *info,
                           unsigned int typeinfo[6],
                           lispval defaults[6]);
FD_EXPORT void fd_defprim7(lispval module,fd_cprim7 fn,
                           struct FD_CPRIM_INFO *info,
                           unsigned int typeinfo[7],
                           lispval defaults[7]);
FD_EXPORT void fd_defprim8(lispval module,fd_cprim8 fn,
                           struct FD_CPRIM_INFO *info,
                           unsigned int typeinfo[8],
                           lispval defaults[8]);
FD_EXPORT void fd_defprim9(lispval module,fd_cprim9 fn,
                           struct FD_CPRIM_INFO *info,
                           unsigned int typeinfo[9],
                           lispval defaults[9]);
FD_EXPORT void fd_defprim10(lispval module,fd_cprim10 fn,
                            struct FD_CPRIM_INFO *info,
                            unsigned int typeinfo[10],
                            lispval defaults[10]);
FD_EXPORT void fd_defprim11(lispval module,fd_cprim11 fn,
                            struct FD_CPRIM_INFO *info,
                            unsigned int typeinfo[11],
                            lispval defaults[11]);
FD_EXPORT void fd_defprim12(lispval module,fd_cprim12 fn,
                            struct FD_CPRIM_INFO *info,
                            unsigned int typeinfo[12],
                            lispval defaults[12]);
FD_EXPORT void fd_defprim7(lispval module,fd_cprim7 fn,
                           struct FD_CPRIM_INFO *info,
                           unsigned int typeinfo[7],
                           lispval defaults[7]);
FD_EXPORT void fd_defprim14(lispval module,fd_cprim14 fn,
                            struct FD_CPRIM_INFO *info,
                            unsigned int typeinfo[14],
                            lispval defaults[14]);
FD_EXPORT void fd_defprim7(lispval module,fd_cprim7 fn,
                           struct FD_CPRIM_INFO *info,
                           unsigned int typeinfo[7],
                           lispval defaults[7]);

#endif /* FRAMERD_CPRIMS_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
