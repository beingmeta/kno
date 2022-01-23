/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_CPRIMS_H
#define KNO_CPRIMS_H 1
#ifndef KNO_CPRIMS_H_INFO
#define KNO_CPRIMS_H_INFO "include/kno/cprims.h"
#endif

#include "kno/apply.h"

#define TOSTRING(x) #x
#define STRINGIFY(x) TOSTRING(x)

typedef struct KNO_CPRIM_INFO {
  u8_string pname, cname, filename, docstring;
  int arity, flags, arginfo_len;} KNO_CPRIM_INFO;
typedef struct KNO_CPRIM_ARGINFO {
  u8_string argname;
  long int argtype;
  lispval default_value;} *kno_definfo;

#define KNO_FNFLAGS(max_arity,min_arity,ndcall,xcall)    \
  ( (KNO_MAX_ARGS(max_arity)) |                          \
    (KNO_MIN_ARGS(min_arity)) |                          \
    ((ndcall) ? (KNO_NDCALL) : (0)) |                    \
    ((xcall) ? (KNO_XCALL) : (0)) )

#define KNO_DEFC_PRIM(pname,cname,flags,docstring,...)	\
  static struct KNO_CPRIM_INFO cname ## _info = {	\
   pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), \
   docstring, ((flags)&(0x7f)), flags, -1};			\
  static U8_MAYBE_UNUSED struct KNO_CPRIM_ARGINFO		\
    cname## _arginfo[((flags)&(0x7f))] = { __VA_ARGS__ };

#define KNO_DEFC_PRIMN(pname,cname,flags,docstring)                         \
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__),                         \
    docstring, ((flags)&0x7f), flags, -1};					\
  static U8_MAYBE_UNUSED struct KNO_CPRIM_ARGINFO *cname ## _arginfo = NULL;

#define KNO_DEFC_PRIMNN(pname,cname,flags,docstring,...)		\
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__),                         \
    docstring, ((flags)&0x7f), flags, -1};					\
  static U8_MAYBE_UNUSED struct KNO_CPRIM_ARGINFO cname ## _arginfo[(((flags)>>8)&(0x7F))]  = \
    { __VA_ARGS__ };

#define KNO_DEFC_PRIMNx(pname,cname,flags,docstring,info_len,...)		\
  static struct KNO_CPRIM_INFO cname ## _info = {                        \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__),                         \
    docstring, ((flags)&0x7f), flags, -1};					\
  static U8_MAYBE_UNUSED struct KNO_CPRIM_ARGINFO cname ## _arginfo[info_len]  = \
    { __VA_ARGS__ };

/* Registering the primitives */

#define KNO_LINK_CPRIM(pname,cname,arity,module)			\
  kno_defcprim ## arity(module,cname,&cname ## _info,cname ## _arginfo)
/* We separate this case because we want arity mismatches to be detected
   at compile time. */
#define KNO_LINK_CPRIMN(pname,cname,module)				\
  kno_defcprimN(module,cname,&cname ## _info,cname ## _arginfo);

#define KNO_LINK_ALIAS(alias,cname,module)               \
  kno_defalias(module,alias,cname ## _info.pname)

#if KNO_SOURCE
#define MAX_ARGS     KNO_MAX_ARGS
#define MIN_ARGS     KNO_MIN_ARGS
#define NDOP         KNO_NDCALL

#define DEFC_PRIM      KNO_DEFC_PRIM
#define DEFC_PRIMN     KNO_DEFC_PRIMN
#define DEFC_PRIMNx    KNO_DEFC_PRIMNx
#define DEFC_PRIMNN    KNO_DEFC_PRIMNN
#endif

KNO_EXPORT void kno_defcprimN(lispval module,kno_cprimn fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO *arginfo);
KNO_EXPORT void kno_defcprim0(lispval module,kno_cprim0 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO *arginfo);
KNO_EXPORT void kno_defcprim1(lispval module,kno_cprim1 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[1]);
KNO_EXPORT void kno_defcprim2(lispval module,kno_cprim2 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[2]);
KNO_EXPORT void kno_defcprim3(lispval module,kno_cprim3 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[3]);
KNO_EXPORT void kno_defcprim4(lispval module,kno_cprim4 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[4]);
KNO_EXPORT void kno_defcprim5(lispval module,kno_cprim5 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[5]);
KNO_EXPORT void kno_defcprim6(lispval module,kno_cprim6 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[6]);
KNO_EXPORT void kno_defcprim7(lispval module,kno_cprim7 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[7]);
KNO_EXPORT void kno_defcprim8(lispval module,kno_cprim8 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[8]);
KNO_EXPORT void kno_defcprim9(lispval module,kno_cprim9 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[9]);
KNO_EXPORT void kno_defcprim10(lispval module,kno_cprim10 fn,
			       struct KNO_CPRIM_INFO *info,
			       struct KNO_CPRIM_ARGINFO arginfo[10]);
KNO_EXPORT void kno_defcprim11(lispval module,kno_cprim11 fn,
			       struct KNO_CPRIM_INFO *info,
			       struct KNO_CPRIM_ARGINFO arginfo[11]);
KNO_EXPORT void kno_defcprim12(lispval module,kno_cprim12 fn,
			       struct KNO_CPRIM_INFO *info,
			       struct KNO_CPRIM_ARGINFO arginfo[12]);
KNO_EXPORT void kno_defcprim13(lispval module,kno_cprim13 fn,
			       struct KNO_CPRIM_INFO *info,
			       struct KNO_CPRIM_ARGINFO arginfo[13]);
KNO_EXPORT void kno_defcprim14(lispval module,kno_cprim14 fn,
			       struct KNO_CPRIM_INFO *info,
			       struct KNO_CPRIM_ARGINFO arginfo[14]);
KNO_EXPORT void kno_defcprim15(lispval module,kno_cprim15 fn,
			       struct KNO_CPRIM_INFO *info,
			       struct KNO_CPRIM_ARGINFO arginfo[15]);

static U8_MAYBE_UNUSED void link_local_cprims(void);

#endif /* KNO_CPRIMS_H */

