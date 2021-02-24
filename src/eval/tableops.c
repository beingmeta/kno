/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES   (!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES    (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/cprims.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"

DEFC_PRIM("table?",tablep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *obj* is a table, #f otherwise.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval tablep(lispval arg)
{
  if (TABLEP(arg))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("getkeyvec",getkeyvec_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a vector of all the keys in *table*.",
	  {"table",kno_any_type,KNO_VOID})
static lispval getkeyvec_prim(lispval table)
{
  int len = 0;
  lispval *keyvec = kno_getkeyvec_n(table,&len);
  if (len<0) return KNO_ERROR;
  else if (len == 0)
    return kno_make_vector(0,NULL);
  else return kno_init_vector(NULL,len,keyvec);
}

DEFC_PRIM("%get",table_get,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "returns the value of *key* in *table* or "
	  "*default* if *table* does not contain *key*. "
	  "*default* defaults to the empty choice {}.Note "
	  "that this does no inference, use GET to enable "
	  "inference.",
	  {"table",kno_any_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"dflt",kno_any_type,KNO_VOID})
static lispval table_get(lispval table,lispval key,lispval dflt)
{
  if (VOIDP(dflt))
    return kno_get(table,key,EMPTY);
  else return kno_get(table,key,dflt);
}

DEFC_PRIM("add!",table_add,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "adds *value* to the associations of *key* in "
	  "*table*. Note that this does no inference, use "
	  "ASSERT! to enable inference.",
	  {"table",kno_any_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"val",kno_any_type,KNO_VOID})
static lispval table_add(lispval table,lispval key,lispval val)
{
  if (EMPTYP(table)) return VOID;
  else if (EMPTYP(key)) return VOID;
  else if (kno_add(table,key,val)<0)
    return KNO_ERROR;
  else return VOID;
}

DEFC_PRIM("drop!",table_drop,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "removes *values* from *key* of *table*. If "
	  "*values* is not provided, all values associated "
	  "with *key* are removed. Note that this does no "
	  "inference, use RETRACT! to enable inference.",
	  {"table",kno_any_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"val",kno_any_type,KNO_VOID})
static lispval table_drop(lispval table,lispval key,lispval val)
{
  if (EMPTYP(table)) return VOID;
  if (EMPTYP(key)) return VOID;
  else if (kno_drop(table,key,val)<0) return KNO_ERROR;
  else return VOID;
}

DEFC_PRIM("store!",table_store,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "stores *value* in *table* under *key*, removing "
	  "all existing values. If *value* is a choice, the "
	  "entire choice is stored under *key*. Note that "
	  "this does no inference.",
	  {"table",kno_any_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"val",kno_any_type,KNO_VOID})
static lispval table_store(lispval table,lispval key,lispval val)
{
  if (EMPTYP(table)) return VOID;
  else if (EMPTYP(key)) return VOID;
  else if (QCHOICEP(val)) {
    struct KNO_QCHOICE *qch = KNO_XQCHOICE(val);
    if (kno_store(table,key,qch->qchoiceval)<0)
      return KNO_ERROR;
    else return VOID;}
  else if (kno_store(table,key,val)<0)
    return KNO_ERROR;
  else return VOID;
}

DEFC_PRIM("%test",table_test,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "returns true if any of *values* is stored undery "
	  "any of *keys* in any of *tables*. If *values* is "
	  "not provided, returns true if any values are "
	  "stored under any of *keys* in any of *tables*. "
	  "Note that this does no inference.",
	  {"table",kno_any_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"val",kno_any_type,KNO_VOID})
static lispval table_test(lispval table,lispval key,lispval val)
{
  if (EMPTYP(table)) return KNO_FALSE;
  else if (EMPTYP(key)) return KNO_FALSE;
  else if (EMPTYP(val)) return KNO_FALSE;
  else {
    int retval = kno_test(table,key,val);
    if (retval<0) return KNO_ERROR;
    else if (retval) return KNO_TRUE;
    else return KNO_FALSE;}
}

/* Even more generic get */

DEFC_PRIM("xget",xget_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Parses a text representation (a string) into a LISP object.",
	  {"obj",kno_any_type,KNO_VOID},
	  {"attrib",kno_any_type,KNO_VOID})
static lispval xget_prim(lispval obj,lispval attrib)
{
  if (CHOICEP(attrib)) {
    lispval results = KNO_EMPTY;
    DO_CHOICES(a,attrib) {
      lispval result;
      if (SLOTIDP(a))
	result = kno_get(obj,a,KNO_EMPTY);
      else if (APPLICABLEP(a))
	result = kno_apply(a,1,&obj);
      else if (TABLEP(a))
	result = kno_get(a,obj,KNO_EMPTY);
      else result = KNO_EMPTY;
      if (KNO_ABORTED(result)) {
	kno_decref(results);
	return result;}
      else {ADD_TO_CHOICE(results,result);}}
    return results;}
  else if (SLOTIDP(attrib))
    return kno_get(obj,attrib,KNO_EMPTY);
  else if (APPLICABLEP(attrib))
    return kno_apply(attrib,1,&obj);
  else if (TABLEP(attrib))
    return kno_get(attrib,obj,KNO_EMPTY);
  else return KNO_EMPTY;
}

/* DB related table ops */

DEFC_PRIM("pool?",poolp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *arg* is a pool.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval poolp(lispval arg)
{
  if (POOLP(arg))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("index?",indexp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *arg* is a pool.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval indexp(lispval arg)
{
  if (INDEXP(arg))
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Slot access */

DEFC_PRIM("get",kno_fget,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "**undocumented**",
	  {"frames",kno_any_type,KNO_VOID},
	  {"slotids",kno_any_type,KNO_VOID})
KNO_EXPORT lispval kno_fget(lispval frames,lispval slotids)
{
  if (!(CHOICEP(frames)))
    if (!(CHOICEP(slotids)))
      if (OIDP(frames))
	return kno_frame_get(frames,slotids);
      else return kno_get(frames,slotids,EMPTY);
    else if (OIDP(frames)) {
      lispval result = EMPTY;
      DO_CHOICES(slotid,slotids) {
	lispval value = kno_frame_get(frames,slotid);
	if (KNO_ABORTED(value)) {
	  kno_decref(result);
	  return value;}
	CHOICE_ADD(result,value);}
      return result;}
    else {
      lispval result = EMPTY;
      DO_CHOICES(slotid,slotids) {
	lispval value = kno_get(frames,slotid,EMPTY);
	if (KNO_ABORTED(value)) {
	  kno_decref(result);
	  return value;}
	CHOICE_ADD(result,value);}
      return result;}
  else {
    int all_adjuncts = 1;
    if (CHOICEP(slotids)) {
      DO_CHOICES(slotid,slotids) {
	int adjunctp = 0;
	DO_CHOICES(adjslotid,kno_adjunct_slotids) {
	  if (KNO_EQ(slotid,adjslotid)) {adjunctp = 1; break;}}
	if (adjunctp==0) {all_adjuncts = 0; break;}}}
    else {
      int adjunctp = 0;
      DO_CHOICES(adjslotid,kno_adjunct_slotids) {
	if (KNO_EQ(slotids,adjslotid)) {adjunctp = 1; break;}}
      if (adjunctp==0) all_adjuncts = 0;}
    if ((kno_prefetch) && (kno_ipeval_status()==0) &&
	(CHOICEP(frames)) && (all_adjuncts==0))
      kno_prefetch_oids(frames);
    {
      lispval results = EMPTY;
      DO_CHOICES(frame,frames)
	if (OIDP(frame)) {
	  DO_CHOICES(slotid,slotids) {
	    lispval v = kno_frame_get(frame,slotid);
	    if (KNO_ABORTED(v)) {
	      KNO_STOP_DO_CHOICES;
	      kno_decref(results);
	      return v;}
	    else {CHOICE_ADD(results,v);}}}
	else {
	  DO_CHOICES(slotid,slotids) {
	    lispval v = kno_get(frame,slotid,EMPTY);
	    if (KNO_ABORTED(v)) {
	      KNO_STOP_DO_CHOICES;
	      kno_decref(results);
	      return v;}
	    else {CHOICE_ADD(results,v);}}}
      return kno_simplify_choice(results);}}
}

DEFC_PRIM("test",kno_ftest,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "**undocumented**",
	  {"frames",kno_any_type,KNO_VOID},
	  {"slotids",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID})
KNO_EXPORT lispval kno_ftest(lispval frames,lispval slotids,lispval values)
{
  if (EMPTYP(frames))
    return KNO_FALSE;
  else if ((!(CHOICEP(frames))) && (!(OIDP(frames))))
    if (CHOICEP(slotids)) {
      int found = 0;
      DO_CHOICES(slotid,slotids)
	if (kno_test(frames,slotid,values)) {
	  found = 1; KNO_STOP_DO_CHOICES; break;}
	else {}
      if (found) return KNO_TRUE;
      else return KNO_FALSE;}
    else {
      int testval = kno_test(frames,slotids,values);
      if (testval<0) return KNO_ERROR;
      else if (testval) return KNO_TRUE;
      else return KNO_FALSE;}
  else {
    DO_CHOICES(frame,frames) {
      DO_CHOICES(slotid,slotids) {
	DO_CHOICES(value,values)
	  if (OIDP(frame)) {
	    int result = kno_frame_test(frame,slotid,value);
	    if (result<0) return KNO_ERROR;
	    else if (result) return KNO_TRUE;}
	  else {
	    int result = kno_test(frame,slotid,value);
	    if (result<0) return KNO_ERROR;
	    else if (result) return KNO_TRUE;}}}
    return KNO_FALSE;}
}

DEFC_PRIM("assert!",kno_assert,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "**undocumented**",
	  {"frames",kno_any_type,KNO_VOID},
	  {"slotids",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID})
KNO_EXPORT lispval kno_assert(lispval frames,lispval slotids,lispval values)
{
  if (EMPTYP(values)) return VOID;
  else {
    DO_CHOICES(frame,frames) {
      DO_CHOICES(slotid,slotids) {
	DO_CHOICES(value,values) {
	  if (kno_frame_add(frame,slotid,value)<0)
	    return KNO_ERROR;}}}
    return VOID;}
}

DEFC_PRIM("retract!",kno_retract,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "**undocumented**",
	  {"frames",kno_any_type,KNO_VOID},
	  {"slotids",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID})
KNO_EXPORT lispval kno_retract(lispval frames,lispval slotids,lispval values)
{
  if (EMPTYP(values)) return VOID;
  else {
    DO_CHOICES(frame,frames) {
      DO_CHOICES(slotid,slotids) {
	if (VOIDP(values)) {
	  lispval values = kno_frame_get(frame,slotid);
	  DO_CHOICES(value,values) {
	    if (kno_frame_drop(frame,slotid,value)<0) {
	      kno_decref(values);
	      return KNO_ERROR;}}
	  kno_decref(values);}
	else {
	  DO_CHOICES(value,values) {
	    if (kno_frame_drop(frame,slotid,value)<0)
	      return KNO_ERROR;}}}}
    return VOID;}
}

DEFC_PRIMN("testp",testp,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(3)|KNO_NDCALL,
	   "**undocumented**")
static lispval testp(int n,kno_argvec args)
{
  lispval frames = args[0], slotids = args[1], testfns = args[2];
  if ((EMPTYP(frames)) || (EMPTYP(slotids)))
    return KNO_FALSE;
  else {
    DO_CHOICES(frame,frames) {
      DO_CHOICES(slotid,slotids) {
	lispval values = kno_fget(frame,slotid);
	DO_CHOICES(testfn,testfns)
	  if (KNO_APPLICABLEP(testfn)) {
	    lispval test_result = KNO_FALSE;
	    lispval test_args[n-2]; test_args[0] = values;
	    memcpy(test_args+1,args+3,(n-3)*sizeof(lispval));
	    test_result = kno_apply(testfn,n-2,test_args);
	    if (KNO_ABORTED(test_result)) {
	      kno_decref(values);
	      return test_result;}
	    else if (KNO_TRUEP(test_result)) {
	      kno_decref(values);
	      kno_decref(test_result);
	      return KNO_TRUE;}
	    else {}}
	  else if ((SYMBOLP(testfn)) || (OIDP(testfn))) {
	    lispval test_result = KNO_FALSE;
	    lispval recursive_args[n-2]; recursive_args[0] = values;
	    memcpy(recursive_args+1,args+4,(n-3)*sizeof(lispval));
	    test_result = testp(n-1,recursive_args);
	    if (KNO_ABORTED(test_result)) {
	      kno_decref(values);
	      return test_result;}
	    else if (KNO_TRUEP(test_result)) {
	      kno_decref(values);
	      return test_result;}
	    else kno_decref(test_result);}
	  else if (TABLEP(testfn)) {
	    DO_CHOICES(value,values) {
	      lispval mapsto = kno_get(testfn,value,VOID);
	      if (KNO_ABORTED(mapsto)) {
		kno_decref(values);
		return mapsto;}
	      else if (VOIDP(mapsto)) {}
	      else if (n>3)
		if (kno_overlapp(mapsto,args[3])) {
		  kno_decref(mapsto);
		  kno_decref(values);
		  return KNO_TRUE;}
		else {kno_decref(mapsto);}
	      else {
		kno_decref(mapsto);
		kno_decref(values);
		return KNO_TRUE;}}}
	  else {
	    kno_decref(values);
	    return kno_type_error(_("value test"),"testp",testfn);}
	kno_decref(values);}}
    return KNO_FALSE;}
}

DEFC_PRIMN("getpath",getpath_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "**undocumented**")
static lispval getpath_prim(int n,kno_argvec args)
{
  lispval result = kno_getpath(args[0],n-1,args+1,1,0);
  return kno_simplify_choice(result);
}

DEFC_PRIMN("getpath*",getpathstar_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "**undocumented**")
static lispval getpathstar_prim(int n,kno_argvec args)
{
  lispval result = kno_getpath(args[0],n-1,args+1,1,0);
  return kno_simplify_choice(result);
}

/* Index/search ops */

/* Finding frames, etc. */

DEFC_PRIMN("find-frames",find_frames_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	   "`(find-frames *index* [*slots* *values*]...)` "
	   "Searches in *index* for frames with the specified slot-values.")
static lispval find_frames_lexpr(int n,kno_argvec args)
{
  if (n%2)
    if (FALSEP(args[0]))
      return kno_bgfinder(n-1,args+1);
    else return kno_finder(args[0],n-1,args+1);
  else return kno_bgfinder(n,args);
}

/* This is like find_frames but ignores any slot/value pairs
   whose values are empty (and thus would rule out any results at all). */

DEFC_PRIMN("xfind-frames",xfind_frames_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	   "`(xfind-frames *index* [*slots* *values*]...)` "
	   "Searches in *index* for frames with the specified slot-values. "
	   "This ignores clauses which are 'empty' and have no indexed matches.")
static lispval xfind_frames_lexpr(int n,kno_argvec args)
{
  int i = (n%2); while (i<n)
		   if (EMPTYP(args[i+1])) {
		     lispval *slotvals = u8_alloc_n((n),lispval), results;
		     int j = 0; i = 1; while (i<n)
					 if (EMPTYP(args[i+1])) i = i+2;
					 else {
					   slotvals[j]=args[i]; j++; i++;
					   slotvals[j]=args[i]; j++; i++;}
		     if (n%2)
		       if (FALSEP(args[0]))
			 results = kno_bgfinder(j,slotvals);
		       else results = kno_finder(args[0],j,slotvals);
		     else results = kno_bgfinder(j,slotvals);
		     u8_free(slotvals);
		     return results;}
		   else i = i+2;
  if (n%2)
    if (FALSEP(args[0]))
      return kno_bgfinder(n-1,args+1);
    else return kno_finder(args[0],n-1,args+1);
  else return kno_bgfinder(n,args);
}

/* Initializations for this file */

KNO_EXPORT void kno_init_tableops_c()
{
  u8_register_source_file(_FILEINFO);

  link_local_cprims();
}

DEFC_PRIM("getkeys",kno_getkeys,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns all the keys in *table*.",
	  {"table",kno_any_type,KNO_VOID});

DEFC_PRIM("getvalues",kno_getvalues,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns all the values associated with all of the "
	  "keys in *table*.",
	  {"table",kno_any_type,KNO_VOID});

DEFC_PRIM("getassocs",kno_getassocs,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns (key . values) pairs for all of the keys "
	  "in *table*.",
	  {"table",kno_any_type,KNO_VOID});

static void link_local_cprims()
{
  KNO_LINK_CPRIM("table?",tablep,1,kno_scheme_module);
  KNO_LINK_CPRIM("%get",table_get,3,kno_scheme_module);
  KNO_LINK_CPRIM("add!",table_add,3,kno_scheme_module);
  KNO_LINK_CPRIM("drop!",table_drop,3,kno_scheme_module);
  KNO_LINK_CPRIM("store!",table_store,3,kno_scheme_module);
  KNO_LINK_CPRIM("%test",table_test,3,kno_scheme_module);
  KNO_LINK_CPRIM("getkeys",kno_getkeys,1,kno_scheme_module);
  KNO_LINK_CPRIM("getvalues",kno_getvalues,1,kno_scheme_module);
  KNO_LINK_CPRIM("getassocs",kno_getassocs,1,kno_scheme_module);
  KNO_LINK_CPRIM("getkeyvec",getkeyvec_prim,1,kno_scheme_module);

  KNO_LINK_CPRIM("xget",xget_prim,2,kno_scheme_module);

  KNO_LINK_CPRIM("index?",indexp,1,kno_scheme_module);
  KNO_LINK_CPRIM("pool?",poolp,1,kno_scheme_module);

  KNO_LINK_CPRIM("get",kno_fget,2,kno_scheme_module);
  KNO_LINK_CPRIM("test",kno_ftest,3,kno_scheme_module);
  KNO_LINK_CPRIM("assert!",kno_assert,3,kno_scheme_module);
  KNO_LINK_CPRIM("retract!",kno_retract,3,kno_scheme_module);
  KNO_LINK_CPRIMN("getpath*",getpathstar_prim,kno_scheme_module);
  KNO_LINK_CPRIMN("getpath",getpath_prim,kno_scheme_module);
  KNO_LINK_CPRIMN("testp",testp,kno_scheme_module);
  KNO_LINK_CPRIMN("xfind-frames",xfind_frames_lexpr,kno_scheme_module);
  KNO_LINK_CPRIMN("find-frames",find_frames_lexpr,kno_scheme_module);

  KNO_LINK_ALIAS("??",find_frames_lexpr,kno_scheme_module);
}
