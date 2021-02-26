/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES      (!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_FCNIDS       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_STACKS       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_LEXENV       (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/support.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/dtproc.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/ports.h"
#include "kno/dtcall.h"
#include "kno/ffi.h"
#include "kno/cprims.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>


static int optionsp(lispval arg)
{
  if ( (KNO_FALSEP(arg)) || (KNO_NILP(arg)) || (KNO_EMPTYP(arg)) )
    return 1;
  else if (KNO_AMBIGP(arg)) {
    KNO_DO_CHOICES(elt,arg) {
      if (! (optionsp(elt)) ) {
	KNO_STOP_DO_CHOICES;
	return 0;}}
    return 1;}
  else if (KNO_PAIRP(arg)) {
    if (optionsp(KNO_CAR(arg)))
      return optionsp(KNO_CDR(arg));
    else return 0;}
  else if ( (KNO_TABLEP(arg)) &&
	    (!(KNO_POOLP(arg))) &&
	    (!(KNO_INDEXP(arg))) )
    return 1;
  else return 0;
}

static lispval getopt_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval opts_arg = kno_get_arg(expr,1);
  if (KNO_VOIDP(opts_arg))
    return kno_err(kno_SyntaxError,"getopt_evalfn/arg1",NULL,expr);
  lispval opts = kno_eval(opts_arg,env,_stack);
  if (KNO_ABORTED(opts))
    return opts;
  else {
    lispval keys_arg = kno_get_arg(expr,2);
    if (KNO_VOIDP(keys_arg)) {
      kno_decref(opts);
      return kno_err(kno_SyntaxError,"getopt_evalfn/arg2",NULL,expr);}
    lispval keys = kno_eval(keys_arg,env,_stack);
    if (KNO_ABORTED(keys)) {
      kno_decref(opts);
      return keys;}
    else {
      lispval results = EMPTY;
      DO_CHOICES(opt,opts) {
	DO_CHOICES(key,keys) {
	  lispval v = kno_getopt(opt,key,VOID);
	  if (KNO_ABORTED(v)) {
	    kno_decref(results); results = v;
	    KNO_STOP_DO_CHOICES;}
	  else if (!(VOIDP(v))) {CHOICE_ADD(results,v);}}
	if (KNO_ABORTED(results)) {KNO_STOP_DO_CHOICES;}}
      kno_decref(keys);
      kno_decref(opts);
      if (KNO_ABORTED(results)) {
	return results;}
      else if (EMPTYP(results)) {
	lispval dflt_expr = kno_get_arg(expr,3);
	if (VOIDP(dflt_expr)) return KNO_FALSE;
	else return kno_eval(dflt_expr,env,_stack);}
      else if (KNO_PRECHOICEP(results))
	return kno_simplify_value(results);
      else return results;}}
}

DEFC_PRIM("%getopt",getopt_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "gets any *name* option from opts, returning "
	  "*default* if there isn't any. This is a real "
	  "procedure (unlike `GETOPT`) so that *default* "
	  "will be evaluated even if the option exists and "
	  "is returned.",
	  {"opts",kno_any_type,KNO_VOID},
	  {"keys",kno_symbol_type,KNO_VOID},
	  {"dflt",kno_any_type,KNO_FALSE})
static lispval getopt_prim(lispval opts,lispval keys,lispval dflt)
{
  lispval results = EMPTY;
  DO_CHOICES(opt,opts) {
    DO_CHOICES(key,keys) {
      lispval v = kno_getopt(opt,key,VOID);
      if (!(VOIDP(v))) {CHOICE_ADD(results,v);}}}
  if (EMPTYP(results)) {
    kno_incref(dflt); return dflt;}
  else if (PRECHOICEP(results))
    return kno_simplify_value(results);
  else return results;
}

DEFC_PRIM("testopt",testopt_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "returns true if the option *name* is specified in "
	  "*opts* and it includes *value* (if provided).",
	  {"opts",kno_any_type,KNO_VOID},
	  {"key",kno_symbol_type,KNO_VOID},
	  {"val",kno_any_type,KNO_VOID})
static lispval testopt_prim(lispval opts,lispval key,lispval val)
{
  if (kno_testopt(opts,key,val))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("opts?",optionsp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "returns true if *opts* is a valid options object.",
	  {"opts",kno_any_type,KNO_VOID})
static lispval optionsp_prim(lispval opts)
{
  if (optionsp(opts))
    return KNO_TRUE;
  else return KNO_FALSE;
}
#define nulloptsp(v) ( (v == KNO_FALSE) || (v == KNO_DEFAULT) )

DEFC_PRIMN("opts+",opts_plus_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDCALL,
	   "or `(OPTS+ *optname* *value* *opts*) returns a "
	   "new options object (a pair).")
static lispval opts_plus_prim(int n,kno_argvec args)
{
  int i = 0, new_front = 0;
  /* *back* is the options list and *front* is where specified values
     should be stored. We walk the arguments, either adding tables to
     the options list, setting options on *front*, or creating new
     slotmaps to get *front*. This was made more complicated when
     schemaps started to be common elements in options lists.
  */
  lispval back = KNO_FALSE, front = KNO_VOID;
  if (n == 0)
    return kno_make_slotmap(3,0,NULL);
  while (i < n) {
    lispval arg = args[i++];
    if (KNO_TABLEP(arg)) {
      kno_incref(arg);
      if (KNO_FALSEP(back))
	back = arg;
      else {
	if (!(KNO_VOIDP(front))) {
	  back = kno_init_pair(NULL,front,back);
	  front=KNO_VOID;}
	back = kno_init_pair(NULL,arg,back);}}
    else if ( (nulloptsp(arg)) || (KNO_EMPTYP(arg)) ) {}
    else {
      if (KNO_VOIDP(front)) {
	front = kno_make_slotmap(n,0,NULL);
	new_front = 1;}
      if (i < n) {
	lispval optval = args[i++];
	kno_add(front,arg,optval);}
      else kno_store(front,arg,KNO_TRUE);}}
  if (KNO_VOIDP(front))
    return back;
  else if (KNO_FALSEP(back)) {
    if (new_front == 0)
      return front;
    else if ((KNO_SLOTMAPP(front)) &&
	     (KNO_SLOTMAP_NSLOTS(front) == 0)) {
      kno_decref(front);
      return KNO_FALSE;}
    else return front;}
  else return kno_init_pair(NULL,front,back);
}


/* Initialization */

KNO_EXPORT void kno_init_eval_getopt_c()
{
  u8_register_source_file(_FILEINFO);

  kno_def_evalfn(kno_scheme_module,"GETOPT",getopt_evalfn,
		 "`(GETOPT *opts* *name* [*default*=#f])` returns any *name* "
		 "option defined in *opts* or *default* otherwise. "
		 "If *opts* or *name* are choices, this only returns *default* "
		 "if none of the alternatives yield results.");

  link_local_cprims();
}



static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_CPRIMN("opts+",opts_plus_prim,scheme_module);
  KNO_LINK_CPRIM("opts?",optionsp_prim,1,scheme_module);
  KNO_LINK_CPRIM("testopt",testopt_prim,3,scheme_module);
  KNO_LINK_CPRIM("%getopt",getopt_prim,3,scheme_module);

  KNO_LINK_ALIAS("opt+",opts_plus_prim,scheme_module);
}
