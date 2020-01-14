/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
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

typedef lispval kno_probe_typeinfo(lispval tag);
typedef lispval kno_use_typeinfo(lispval tag);

typedef struct KNO_TYPEINFO {
  KNO_CONS_HEADER;
  lispval typetag, type_props;
  lispval type_handlers;
  kno_type_parsefn type_parsefn;
  kno_type_unparsefn type_unparsefn;
  kno_type_freefn type_freefn;
  kno_type_dumpfn type_dumpfn;
  kno_type_restorefn type_restorefn;} KNO_TYPEINFO;
KNO_EXPORT struct KNO_TYPEINFO *kno_typeinfo;

#endif
