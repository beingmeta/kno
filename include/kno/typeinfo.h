/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_TYPEINFO_H
#define KNO_TYPEINFO_H 1
#ifndef KNO_TYPEINFO_H_INFO
#define KNO_TYPEINFO_H_INFO "include/kno/typeinfo.h"
#endif

/* Typeinfo */

typedef struct KNO_TYPEINFO *kno_typeinfo;
typedef int (*kno_type_unparsefn)(u8_output out,lispval,kno_typeinfo);
typedef lispval (*kno_type_consfn)(int n,lispval *,kno_typeinfo);
typedef int (*kno_type_testfn)(lispval,kno_typeinfo);
typedef int (*kno_type_freefn)(lispval,kno_typeinfo);
typedef lispval (*kno_type_dumpfn)(lispval,kno_typeinfo);
typedef lispval (*kno_type_restorefn)(lispval,lispval,kno_typeinfo);
typedef lispval (*kno_type_dispatchfn)(lispval,lispval,int,
				       const lispval *,
				       kno_typeinfo);

typedef struct KNO_TYPEINFO {
  KNO_CONS_HEADER;
  lispval typetag, type_props, type_usetag;
  u8_string type_name, type_description;
  char type_isopaque, type_ismutable, type_issequence, type_istable;
  kno_lisp_type type_basetype;
  kno_type_testfn type_testfn;
  kno_type_consfn type_consfn;
  kno_type_unparsefn type_unparsefn;
  kno_type_freefn type_freefn;
  kno_type_dumpfn type_dumpfn;
  kno_type_restorefn type_restorefn;
  kno_type_dispatchfn type_dispatchfn;
  struct KNO_TABLEFNS *type_tablefns;
  struct KNO_SEQFNS *type_seqfns;} KNO_TYPEINFO;

KNO_EXPORT kno_type_unparsefn kno_default_unparsefn;
KNO_EXPORT kno_type_consfn kno_default_consfn;
KNO_EXPORT kno_type_freefn kno_default_freefn;
KNO_EXPORT kno_type_dumpfn kno_default_dumpfn;
KNO_EXPORT kno_type_restorefn kno_default_restorefn;

KNO_EXPORT int kno_set_unparsefn(lispval tag,kno_type_unparsefn fn);
KNO_EXPORT int kno_set_consfn(lispval tag,kno_type_consfn fn);
KNO_EXPORT int kno_set_testfn(lispval tag,kno_type_testfn fn);
KNO_EXPORT int kno_set_freefn(lispval tag,kno_type_freefn fn);
KNO_EXPORT int kno_set_dumpfn(lispval tag,kno_type_dumpfn fn);
KNO_EXPORT int kno_set_restorefn(lispval tag,kno_type_restorefn fn);
KNO_EXPORT int kno_set_dispatchfn(lispval tag,kno_type_dispatchfn fn);



#endif
