/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_TYPEINFO_H
#define KNO_TYPEINFO_H 1
#ifndef KNO_TYPEINFO_H_INFO
#define KNO_TYPEINFO_H_INFO "include/kno/typeinfo.h"
#endif

/* Typeinfo */
typedef struct KNO_TYPEINFO *kno_typeinfo;
typedef int (*kno_type_unparsefn)(u8_output out,lispval,kno_typeinfo);
typedef lispval (*kno_type_parsefn)(int n,lispval *,kno_typeinfo);
typedef int (*kno_type_freefn)(lispval,kno_typeinfo);
typedef lispval (*kno_type_dumpfn)(lispval,kno_typeinfo);
typedef lispval (*kno_type_restorefn)(lispval,lispval,kno_typeinfo);

typedef struct KNO_TYPEINFO {
  KNO_CONS_HEADER;
  lispval typeinfo_tag, typeinfo_metadata;
  short typeinfo_corelen;
  char typeinfo_isopaque;
  char typeinfo_ismutable;
  kno_typeinfo_parsefn typeinfo_parser;
  kno_typeinfo_unparsefn typeinfo_unparser;
  kno_typeinfo_freefn typeinfo_freefn;
  kno_typeinfo_dumpfn typeinfo_dumpfn;
  kno_typeinfo_restorefn typeinfo_restorefn;
  struct KNO_TABLEFNS *typeinfo_tablefns;
  struct KNO_SEQFNS *typeinfo_seqfns;} KNO_TYPEINFO;
KNO_EXPORT struct KNO_TYPEINFO *kno_typeinfo;

#endif
