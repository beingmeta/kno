/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_CPRIMS_H
#define KNO_CPRIMS_H 1
#ifndef KNO_CPRIMS_H_INFO
#define KNO_CPRIMS_H_INFO "include/kno/cprims.h"
#endif

#include "kno/apply.h"

#define TOSTRING(x) #x
#define STRINGIFY(x) TOSTRING(x)

#define KNO_MAX_ARGS(n) ( (n < 0) ? (0x80) : ((n)&(0x7F)) )
#define KNO_MIN_ARGS(n) ( (n < 0) ? (0x00) : ( (0x8000) | (((n)&(0x7F))<<8) ) )
#define KNO_OPTARGS   0x08000
#define KNO_N_ARGS    0x00080
#define KNO_VAR_ARGS  0x00080

#define KNO_FNFLAGS(max_arity,min_arity,ndcall,xcall)    \
  ( (KNO_MAX_ARGS(max_arity)) |                          \
    (KNO_MIN_ARGS(min_arity)) |                          \
    ((ndcall) ? (KNO_NDCALL) : (0)) |                    \
    ((xcall) ? (KNO_XCALL) : (0)) )

/* DEFPRIM */

#define KNO_DEFPRIM_DECL(pname,cname,flags,docstring)              \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__),                         \
    docstring, ((flags)&0x7f), flags};                                  \
  static U8_MAYBE_UNUSED int *cname ## _typeinfo = NULL;                \
  static U8_MAYBE_UNUSED lispval *cname ## _defaults = NULL

#define KNO_DCLPRIM(pname,cname,flags,docstring)                         \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__),                         \
    docstring, ((flags)&0x7f), flags};                                  \
  static U8_MAYBE_UNUSED int *cname ## _typeinfo = NULL;                \
  static U8_MAYBE_UNUSED lispval *cname ## _defaults = NULL;

#define KNO_DEFPRIM(pname,cname,flags,docstring) \
  KNO_DCLPRIM(pname,cname,flags,docstring)       \
  static lispval cname

#define KNO_DCLPRIM1(pname,cname,flags,docstring,t1,d1)                  \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 1, flags};   \
  static U8_MAYBE_UNUSED int cname## _typeinfo[1] = { t1 };             \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[1] = { d1 };

#define KNO_DEFPRIM1(pname,cname,flags,docstring,t1,d1)                  \
  KNO_DCLPRIM1(pname,cname,flags,docstring,t1,d1);                       \
  static lispval cname

#define KNO_DCLPRIM2(pname,cname,flags,docstring,t1,d1,t2,d2)            \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 2, flags};   \
  static U8_MAYBE_UNUSED int cname## _typeinfo[2] = { t1, t2 };         \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[2] = { d1, d2 };

#define KNO_DEFPRIM2(pname,cname,flags,docstring,t1,d1,t2,d2)            \
  KNO_DCLPRIM2(pname,cname,flags,docstring,t1,d1,t2,d2);                 \
  static lispval cname

#define KNO_DCLPRIM3(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3)      \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 3, flags};   \
  static U8_MAYBE_UNUSED int cname## _typeinfo[3] = { t1, t2, t3 };     \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[3] = { d1, d2, d3 };

#define KNO_DEFPRIM3(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3)      \
  KNO_DCLPRIM3(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3);           \
  static lispval cname

#define KNO_DCLPRIM4(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3,t4,d4) \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 4, flags};   \
  static U8_MAYBE_UNUSED int cname## _typeinfo[4] = { t1, t2, t3, t4 }; \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[4] = { d1, d2, d3, d4 };

#define KNO_DEFPRIM4(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3,t4,d4) \
  KNO_DCLPRIM4(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3,t4,d4);     \
  static lispval cname

#define KNO_DCLPRIM5(pname,cname,flags,docstring,                        \
                    t1,d1,t2,d2,t3,d3,t4,d4,t5,d5)                      \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 5, flags};   \
  static U8_MAYBE_UNUSED int cname## _typeinfo[5] = { t1, t2, t3, t4, t5 }; \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[5] = { d1, d2, d3, d4, d5 };
#define KNO_DEFPRIM5(pname,cname,flags,docstring,                        \
                    t1,d1,t2,d2,t3,d3,t4,d4,t5,d5)                      \
  KNO_DCLPRIM5(pname,cname,flags,docstring,                             \
              t1,d1,t2,d2,t3,d3,t4,d4,t5,d5);                          \
  static lispval cname


#define KNO_DCLPRIM6(pname,cname,flags,docstring,                        \
                    t1,d1,t2,d2,t3,d3,t4,d4,t5,d5,t6,d6)                \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 6, flags};   \
  static U8_MAYBE_UNUSED int cname## _typeinfo[6] = { t1, t2, t3, t4, t5, t6 }; \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[6] = { d1, d2, d3, d4, d5, d6 };

#define KNO_DEFPRIM6(pname,cname,flags,docstring,                        \
                    t1,d1,t2,d2,t3,d3,t4,d4,t5,d5,t6,d6)                \
  KNO_DCLPRIM6(pname,cname,flags,docstring,                             \
              t1,d1,t2,d2,t3,d3,t4,d4,t5,d5,t6,d6);                    \
  static lispval cname

#define KNO_DCLPRIM7(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3,      \
                    t4,d4,t5,d5,t6,d6,t7,d7)                            \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname const, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 7, flags}; \
  static U8_MAYBE_UNUSED int cname## _typeinfo[7] = { t1, t2, t3, t4, t5, t6, t7 }; \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[7] = { d1, d2, d3, d4, d5, d6, d7 };

#define KNO_DEFPRIM7(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3,      \
                    t4,d4,t5,d5,t6,d6,t7,d7)                            \
  KNO_DCLPRIM7(pname,cname,flags,docstring,t1,d1,t2,d2,t3,d3,            \
              t4,d4,t5,d5,t6,d6,t7,d7);                                 \
  static lispval cname

#define KNO_DCLPRIM8(pname,cname,flags,docstring,t1,d1,t2,d2,            \
                    t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8)                \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    const, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 8, flags};   \
  static U8_MAYBE_UNUSED int cname## _typeinfo[8] = { t1, t2, t3, t4, t5, t6, t7, t8 }; \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[8] = { d1, d2, d3, d4, d5, d6, d7, d8 };

#define KNO_DEFPRIM8(pname,cname,flags,docstring,t1,d1,t2,d2,    \
                    t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8)        \
  KNO_DCLPRIM8(pname,cname,flags,docstring,t1,d1,t2,d2,          \
              t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8);             \
  static lispval cname

#define KNO_DCLPRIM9(pname,cname,flags,docstring,t1,d1,t2,d2,            \
                    t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9)          \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 9, flags};   \
  static U8_MAYBE_UNUSED int cname## _typeinfo[9] =                     \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9 };                             \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[9] =                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9 };

#define KNO_DEFPRIM9(pname,cname,flags,docstring,t1,d1,t2,d2,   \
  t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9)                   \
  KNO_DCLPRIM9(pname,cname,flags,docstring,t1,d1,t2,d2,         \
                t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9);    \
  static lispval cname

#define KNO_DCLPRIM10(pname,cname,flags,docstring,t1,d1,t2,d2,           \
                     t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,t10,d10) \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 10, flags};  \
  static U8_MAYBE_UNUSED int cname## _typeinfo[10] =                    \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9, t10 };                        \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[7] =                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9, d10 };

#define KNO_DEFPRIM10(pname,cname,flags,docstring,t1,d1,t2,d2,           \
                     t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,t10,d10) \
  KNO_DCLPRIM10(pname,cname,flags,docstring,t1,d1,t2,d2,                 \
                 t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,t10,d10);    \
  static lispval cname

#define KNO_DCLPRIM11(pname,cname,flags,docstring,t1,d1,t2,d2,           \
                     t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,         \
                     t10,d10,t11,d11)                                   \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 11, flags};  \
  static U8_MAYBE_UNUSED int cname## _typeinfo[11] =                    \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11 };                   \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[7] =                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11 };

#define KNO_DEFPRIM11(pname,cname,flags,docstring,t1,d1,t2,d2,           \
  t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,                            \
  t10,d10,t11,d11)                                                      \
  KNO_DCLPRIM11(pname,cname,flags,docstring,t1,d1,t2,d2,                 \
               t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9;,              \
               t10,d10,t11,d11);                                        \
  static lispval cname

#define KNO_DCLPRIM12(pname,cname,flags,docstring,t1,d1,t2,d2,           \
                     t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,         \
                     t10,d10,t11,d11,t12,d12)                           \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 12, flags};  \
  static U8_MAYBE_UNUSED int cname## _typeinfo[12] =                    \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12 };              \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[7] =                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12 };

#define KNO_DEFPRIM12(pname,cname,flags,docstring,t1,d1,t2,d2,           \
                     t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,         \
                     t10,d10,t11,d11,t12,d12)                           \
  KNO_DCLPRIM12(pname,cname,flags,docstring,t1,d1,t2,d2,                 \
               t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,               \
               t10,d10,t11,d11,t12,d12);                                \
  static lispval cname

#define KNO_DCLPRIM13(pname,cname,flags,docstring,t1,d1,t2,d2,           \
                     t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,         \
                     t10,d10,t11,d11,t12,d12,t13,d13)                   \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 13, flags};  \
  static U8_MAYBE_UNUSED int cname## _typeinfo[12] =                    \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13 };         \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[7] =                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13 };

#define KNO_DEFPRIM13(pname,cname,flags,docstring,t1,d1,t2,d2,           \
                     t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,         \
                     t10,d10,t11,d11,t12,d12,t13,d13)                   \
  KNO_DCLPRIM13(pname,cname,flags,docstring,t1,d1,t2,d2,                 \
               t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,               \
               t10,d10,t11,d11,t12,d12,t13,d13);                        \
  static lispval cname

#define KNO_DCLPRIM14(pname,cname,flags,docstring,t1,d1,t2,d2,           \
                     t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,         \
                     t10,d10,t11,d11,t12,d12,t13,d13,t14,d14)           \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 14, flags};  \
  static U8_MAYBE_UNUSED int cname## _typeinfo[12] =                    \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14 };    \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[7] =                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14 };

#define KNO_DEFPRIM14(pname,cname,flags,docstring,t1,d1,t2,d2,           \
                     t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,         \
                     t10,d10,t11,d11,t12,d12,t13,d13,t14,d14)           \
  KNO_DCLPRIM14(pname,cname,flags,docstring,t1,d1,t2,d2,                 \
               t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,               \
               t10,d10,t11,d11,t12,d12,t13,d13,t14,d14);                \
  static lispval cname

#define KNO_DCLPRIM15(pname,cname,flags,docstring,t1,d1,t2,d2,           \
                     t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,         \
                     t10,d10,t11,d11,t12,d12,t13,d13,t14,d14,t15,d15)   \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), docstring, 15, flags};  \
  static U8_MAYBE_UNUSED int cname## _typeinfo[12] =                    \
    { t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15 }; \
  static U8_MAYBE_UNUSED lispval cname ## _defaults[7] =                \
    { d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15 };

#define KNO_DEFPRIM15(pname,cname,flags,docstring,t1,d1,t2,d2,           \
                     t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,         \
                     t10,d10,t11,d11,t12,d12,t13,d13,t14,d14,t15,d15)   \
  KNO_DCLPRIM15(pname,cname,flags,docstring,t1,d1,t2,d2,                 \
               t3,d3,t4,d4,t5,d5,t6,d6,t7,d7,t8,d8,t9,d9,               \
               t10,d10,t11,d11,t12,d12,t13,d13,t14,d14,t15,d15);        \
  static lispval cname

/* Registering the primitives */

#define KNO_DECL_PRIM(cname,arity,module)                        \
  kno_defprim ## arity(module,cname,&cname ## _info,             \
                      cname ## _typeinfo,                       \
                      cname ## _defaults)

#define KNO_DECL_PRIM_ARGS(cname,arity,module,typeinfo,defaults) \
  kno_defprim ## arity(module,cname,&cname ## _info,typeinfo,defaults)


#define KNO_DECL_PRIM_N(cname,module)            \
  kno_defprimN(module,cname,&cname ## _info);

#define KNO_DECL_ALIAS(alias,cname,module)               \
  kno_defalias(module,alias,cname ## _info.pname)

#if KNO_SOURCE
#define DECL_PRIM        KNO_DECL_PRIM
#define DECL_PRIM_N      KNO_DECL_PRIM_N
#define DECL_ALIAS       KNO_DECL_ALIAS
#define DECL_PRIM_ARGS    KNO_DECL_PRIM_ARGS

#define MAX_ARGS     KNO_MAX_ARGS
#define MIN_ARGS     KNO_MIN_ARGS
#define NDCALL       KNO_NDCALL

#define DEFPRIM_DECL KNO_DEFPRIM_DECL
#define DEFPRIM      KNO_DEFPRIM
#define DEFPRIM1     KNO_DEFPRIM1
#define DEFPRIM2     KNO_DEFPRIM2
#define DEFPRIM3     KNO_DEFPRIM3
#define DEFPRIM4     KNO_DEFPRIM4
#define DEFPRIM5     KNO_DEFPRIM5
#define DEFPRIM6     KNO_DEFPRIM6
#define DEFPRIM7     KNO_DEFPRIM7
#define DEFPRIM8     KNO_DEFPRIM8
#define DEFPRIM9     KNO_DEFPRIM9
#define DEFPRIM10    KNO_DEFPRIM10
#define DEFPRIM11    KNO_DEFPRIM11
#define DEFPRIM12    KNO_DEFPRIM12
#define DEFPRIM13    KNO_DEFPRIM13
#define DEFPRIM14    KNO_DEFPRIM14
#define DEFPRIM15    KNO_DEFPRIM15
#define DCLPRIM      KNO_DCLPRIM
#define DCLPRIM1     KNO_DCLPRIM1
#define DCLPRIM2     KNO_DCLPRIM2
#define DCLPRIM3     KNO_DCLPRIM3
#define DCLPRIM4     KNO_DCLPRIM4
#define DCLPRIM5     KNO_DCLPRIM5
#define DCLPRIM6     KNO_DCLPRIM6
#define DCLPRIM7     KNO_DCLPRIM7
#define DCLPRIM8     KNO_DCLPRIM8
#define DCLPRIM9     KNO_DCLPRIM9
#define DCLPRIM10    KNO_DCLPRIM10
#define DCLPRIM11    KNO_DCLPRIM11
#define DCLPRIM12    KNO_DCLPRIM12
#define DCLPRIM13    KNO_DCLPRIM13
#define DCLPRIM14    KNO_DCLPRIM14
#define DCLPRIM15    KNO_DCLPRIM15
#endif

KNO_EXPORT void kno_defprimN(lispval module,kno_cprimn fn,
                           struct KNO_CPRIM_INFO *info);
KNO_EXPORT void kno_defprim0(lispval module,kno_cprim0 fn,
                           struct KNO_CPRIM_INFO *info,
                           int typeinfo[0],
                           lispval defaults[0]);
KNO_EXPORT void kno_defprim1(lispval module,kno_cprim1 fn,
                           struct KNO_CPRIM_INFO *info,
                           int typeinfo[1],
                           lispval defaults[1]);
KNO_EXPORT void kno_defprim2(lispval module,kno_cprim2 fn,
                           struct KNO_CPRIM_INFO *info,
                           int typeinfo[2],
                           lispval defaults[2]);
KNO_EXPORT void kno_defprim3(lispval module,kno_cprim3 fn,
                           struct KNO_CPRIM_INFO *info,
                           int typeinfo[3],
                           lispval defaults[3]);
KNO_EXPORT void kno_defprim4(lispval module,kno_cprim4 fn,
                           struct KNO_CPRIM_INFO *info,
                           int typeinfo[4],
                           lispval defaults[4]);
KNO_EXPORT void kno_defprim5(lispval module,kno_cprim5 fn,
                           struct KNO_CPRIM_INFO *info,
                           int typeinfo[5],
                           lispval defaults[5]);
KNO_EXPORT void kno_defprim6(lispval module,kno_cprim6 fn,
                           struct KNO_CPRIM_INFO *info,
                           int typeinfo[6],
                           lispval defaults[6]);
KNO_EXPORT void kno_defprim7(lispval module,kno_cprim7 fn,
                           struct KNO_CPRIM_INFO *info,
                           int typeinfo[7],
                           lispval defaults[7]);
KNO_EXPORT void kno_defprim8(lispval module,kno_cprim8 fn,
                           struct KNO_CPRIM_INFO *info,
                           int typeinfo[8],
                           lispval defaults[8]);
KNO_EXPORT void kno_defprim9(lispval module,kno_cprim9 fn,
                           struct KNO_CPRIM_INFO *info,
                           int typeinfo[9],
                           lispval defaults[9]);
KNO_EXPORT void kno_defprim10(lispval module,kno_cprim10 fn,
                            struct KNO_CPRIM_INFO *info,
                            int typeinfo[10],
                            lispval defaults[10]);
KNO_EXPORT void kno_defprim11(lispval module,kno_cprim11 fn,
                            struct KNO_CPRIM_INFO *info,
                            int typeinfo[11],
                            lispval defaults[11]);
KNO_EXPORT void kno_defprim12(lispval module,kno_cprim12 fn,
                            struct KNO_CPRIM_INFO *info,
                            int typeinfo[12],
                            lispval defaults[12]);
KNO_EXPORT void kno_defprim13(lispval module,kno_cprim13 fn,
                            struct KNO_CPRIM_INFO *info,
                            int typeinfo[13],
                            lispval defaults[13]);
KNO_EXPORT void kno_defprim14(lispval module,kno_cprim14 fn,
                            struct KNO_CPRIM_INFO *info,
                            int typeinfo[14],
                            lispval defaults[14]);
KNO_EXPORT void kno_defprim15(lispval module,kno_cprim15 fn,
                            struct KNO_CPRIM_INFO *info,
                            int typeinfo[15],
                            lispval defaults[15]);

#endif /* KNO_CPRIMS_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
