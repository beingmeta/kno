/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_TYPEINFO_H
#define KNO_TYPEINFO_H 1
#ifndef KNO_TYPEINFO_H_INFO
#define KNO_TYPEINFO_H_INFO "include/kno/typeinfo.h"
#endif

/* Typeinfo */

KNO_EXPORT u8_condition kno_UnDumpable;

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

#define KNO_TYPEINFOP(x) (KNO_TYPEP((x),kno_typeinfo_type))

typedef struct KNO_SCHEMA_ENTRY {
  lispval name;
  lispval type;
  unsigned int bits;} *kno_schema_entry;

typedef struct KNO_TYPEINFO {
  KNO_CONS_HEADER;
  lispval typetag, type_props, type_usetag, type_schema;
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
  char type_free_tablefns, type_free_seqfns, type_free_defdata;
  struct KNO_TABLEFNS *type_tablefns;
  struct KNO_SEQFNS *type_seqfns;
  void *type_defdata;} KNO_TYPEINFO;

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

typedef kno_typeinfo(*kno_subtypefn)(lispval);
KNO_EXPORT struct KNO_TYPEINFO *kno_ctypeinfo[KNO_TYPE_MAX];
KNO_EXPORT kno_subtypefn kno_subtypefns[KNO_TYPE_MAX];

KNO_EXPORT kno_typeinfo kno_register_tag_type(lispval tag,long int longcode);
KNO_EXPORT lispval kno_restore_tagged(lispval tag,lispval data,kno_typeinfo info);


#endif
