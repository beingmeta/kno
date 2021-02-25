/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Dealing with DTYPE (dynamically typed) objects */

#ifndef KNO_PPRINT_H
#define KNO_PPRINT_H 1
#ifndef KNO_PPRINT_H_INFO
#define KNO_PPRINT_H_INFO "include/kno/pprint.h"
#endif

#include "common.h"
#include "ptr.h"
#include "cons.h"

/* -1 = unlimited, 0 means use default */
KNO_EXPORT int pprint_maxcol;
KNO_EXPORT int pprint_fudge;
KNO_EXPORT int pprint_maxchars;
KNO_EXPORT int pprint_maxbytes;
KNO_EXPORT int pprint_maxelts;
KNO_EXPORT int pprint_maxdepth;
KNO_EXPORT int pprint_list_max;
KNO_EXPORT int pprint_vector_max;
KNO_EXPORT int pprint_choice_max;
KNO_EXPORT int pprint_maxkeys;
KNO_EXPORT u8_string pprint_margin;
KNO_EXPORT lispval pprint_default_rules;

typedef struct PPRINT_CONTEXT {
  u8_string pp_margin;
  int pp_margin_len;
  /* -1 = unlimited, 0 means use default */
  int pp_maxcol;
  int pp_maxdepth;
  int pp_fudge;
  int pp_maxelts;
  int pp_maxchars;
  int pp_maxbytes;
  int pp_maxkeys;
  int pp_list_max;
  int pp_vector_max;
  int pp_choice_max;
  /* Reserved for future use */
  void *pp_customfn; void *pp_customdata;
  /* For customization */
  lispval pp_rules;
  /* For other display options */
  lispval pp_opts;}
  *pprint_context;

typedef int (*kno_pprintfn)(u8_output out,lispval x,
			   int indent,int col,int depth,
			   struct PPRINT_CONTEXT *ppcxt,
			   void *data);

KNO_EXPORT
int kno_pprinter(u8_output out,lispval x,int indent,int col,int depth,
		kno_pprintfn customfn,void *customdata,
		struct PPRINT_CONTEXT *ppcxt);
KNO_EXPORT int kno_pprint_x
(u8_output out,lispval x,u8_string margin,
 int indent,int col,int maxcol,
 kno_pprintfn fn,void *data);
KNO_EXPORT int kno_pprint
(u8_output out,lispval x,u8_string margin,
 int indent,int col,int maxcol);

KNO_EXPORT
int kno_pprint_table(u8_output out,lispval x,
		    const lispval *keys,size_t n_keys,
		    int indent,int col,int depth,
		    kno_pprintfn customfn,void *customdata,
		    struct PPRINT_CONTEXT *ppcxt);

#endif /* ndef KNO_PPRINT_H */

