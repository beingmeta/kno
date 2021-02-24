/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/tables.h"
#include "kno/storage.h"
#include "kno/apply.h"
#include "kno/methods.h"
#include "kno/cprims.h"

static lispval frame_symbol, slot_symbol, value_symbol;
static lispval through_slot, derive_slot, inverse_slot;
static lispval multi_slot, multi_primary_slot, key_slot;
static lispval closure_of_slot, index_slot;

static struct KNO_HASHTABLE method_table; lispval kno_method_table;

/* Walk proc */

static int keep_walking(struct KNO_HASHSET *seen,lispval node,lispval slotids,
			kno_tree_walkfn walk,void *data)
{
  if (kno_hashset_get(seen,node))
    return 1;
  else {
    int retval = 0;
    if (kno_hashset_mod(seen,node,1)<0)
      return -1;
    if ((walk == NULL) || ((retval = walk(node,data))>0)) {
      DO_CHOICES(slotid,slotids) {
	lispval values = kno_frame_get(node,slotid);
	if (KNO_ABORTP(values))
	  return kno_interr(values);
	else {
	  int retval = 0;
	  DO_CHOICES(v,values)
	    if (!(OIDP(v))) {}
	    else if (kno_hashset_get(seen,v)) {}
	    else if ((retval = keep_walking(seen,v,slotids,walk,data))>0) {}
	    else {
	      kno_decref(values);
	      return retval;}
	  kno_decref(values);}}
      return 1;}
    else return retval;}
}

KNO_EXPORT int
kno_walk_tree(lispval roots,lispval slotids,kno_tree_walkfn walk,void *data)
{
  struct KNO_HASHSET ht; int retval = 0;
  memset(&ht,0,sizeof(ht));
  kno_init_hashset(&ht,1024,KNO_STACK_CONS);
  {DO_CHOICES(root,roots)
      if (!((KNO_OIDP(root)) || (KNO_SLOTMAPP(root)) || (KNO_SCHEMAPP(root) ))) {}
      else if ((retval = keep_walking(&ht,root,slotids,walk,data))<=0) {
	kno_recycle_hashset(&ht);
	return retval;}
      else NO_ELSE;}
  kno_recycle_hashset(&ht);
  return retval;
}

KNO_EXPORT int
kno_collect_tree(struct KNO_HASHSET *h,
		 lispval roots,lispval slotids)
{
  DO_CHOICES(root,roots)
    if (keep_walking(h,root,slotids,NULL,NULL)<0)
      return -1;
  return 1;
}

/* Get bottom and Get top */

KNO_EXPORT lispval kno_get_basis(lispval collection,lispval lattice)
{
  struct KNO_HASHSET ht;
  lispval root = EMPTY, result = EMPTY;
  memset(&ht,0,sizeof(ht));
  kno_init_hashset(&ht,1024,KNO_STACK_CONS);
  {DO_CHOICES(node,collection) {
      DO_CHOICES(slotid,lattice) {
	lispval v = kno_frame_get(node,slotid);
	if (KNO_ABORTP(v)) {
	  kno_decref(root);
	  kno_recycle_hashset(&ht);
	  return v;}
	else {CHOICE_ADD(root,v);}}}}
  kno_collect_tree(&ht,root,lattice);
  {DO_CHOICES(node,collection)
      if (kno_hashset_get(&ht,node)) {}
      else {
	kno_incref(node);
	CHOICE_ADD(result,node);}}
  kno_decref(root);
  kno_recycle_hashset(&ht);
  return result;
}

/* Inference procedures */

static int inherit_inferred_values_fn(lispval node,void *data)
{
  struct KNO_IVSTRUCT *ivs = (struct KNO_IVSTRUCT *)data;
  lispval values = EMPTY;
  DO_CHOICES(slotid,ivs->slotids) {
    lispval v = kno_frame_get(node,slotid);
    if (KNO_ABORTP(v)) {
      kno_decref(values);
      return kno_interr(v);}
    else {CHOICE_ADD(values,v);}}
  CHOICE_ADD(ivs->result,values);
  return 1;
}

static int inherit_values_fn(lispval node,void *data)
{
  struct KNO_IVSTRUCT *ivs = (struct KNO_IVSTRUCT *)data;
  lispval values = EMPTY;
  DO_CHOICES(slotid,ivs->slotids) {
    lispval v = ((OIDP(node)) ? (kno_oid_get(node,slotid,EMPTY)) :
		 (kno_get(node,slotid,EMPTY)));
    if (KNO_ABORTP(v)) {
      kno_decref(values);
      return kno_interr(v);}
    else {CHOICE_ADD(values,v);}}
  CHOICE_ADD(ivs->result,values);
  return 1;
}

KNO_EXPORT
/* kno_inherit_values:
   Arguments: a frame and two slotids
   Returns: a lispval pointer
   Searches for a value for the first slotid through the lattice
   defined by the second slotid. */
lispval kno_inherit_values(lispval root,lispval slotid,lispval through)
{
  struct KNO_IVSTRUCT ivs;
  ivs.result = EMPTY; ivs.slotids = slotid;
  if (kno_walk_tree(root,through,inherit_values_fn,&ivs)<0) {
    kno_decref(ivs.result);
    return KNO_ERROR;}
  else return kno_simplify_choice(ivs.result);
}

KNO_EXPORT
/* kno_inherit_values:
   Arguments: a frame and two slotids
   Returns: a lispval pointer
   Searches for a value for the first slotid through the lattice
   defined by the second slotid. */
lispval kno_inherit_inferred_values(lispval root,lispval slotid,lispval through)
{
  struct KNO_IVSTRUCT ivs;
  ivs.result = EMPTY; ivs.slotids = slotid;
  if (kno_walk_tree(root,through,inherit_inferred_values_fn,&ivs)<0) {
    kno_decref(ivs.result);
    return KNO_ERROR;}
  else return kno_simplify_choice(ivs.result);
}

static int inherits_inferred_valuesp_fn(lispval node,void *data)
{
  struct KNO_IVPSTRUCT *ivps = (struct KNO_IVPSTRUCT *)data;
  DO_CHOICES(slotid,ivps->slotids) {
    int result = kno_test(node,slotid,ivps->value);
    if (result<0) return -1;
    else if (result) {
      ivps->result = 1; return 0;}}
  return 1;
}

static int inherits_valuesp_fn(lispval node,void *data)
{
  struct KNO_IVPSTRUCT *ivps = (struct KNO_IVPSTRUCT *)data;
  DO_CHOICES(slotid,ivps->slotids) {
    int testval=
      ((OIDP(node)) ?
       (kno_oid_test(node,slotid,ivps->value)) :
       (kno_test(node,slotid,ivps->value)));
    if (testval<0) return -1;
    else if (testval) {
      ivps->result = 1; return 0;}}
  return 1;
}

KNO_EXPORT
/* kno_inherits_valuep:
   Arguments: a frame, two slotids, and a value
   Returns: 1 or 0
   Returns 1 if the value can be inherited for the first slotid
   going through the lattice defined by the second slotid. */
int kno_inherits_valuep
(lispval root,lispval slotid,lispval through,lispval value)
{
  struct KNO_IVPSTRUCT ivps;
  ivps.value = value; ivps.slotids = slotid; ivps.result = 0;
  if (kno_walk_tree(root,through,inherits_valuesp_fn,&ivps)<0)
    return -1;
  else return ivps.result;
}

KNO_EXPORT
/* kno_inherits_valuep:
   Arguments: a frame, two slotids, and a value
   Returns: 1 or 0
   Returns 1 if the value can be inherited for the first slotid
   going through the lattice defined by the second slotid. */
int kno_inherits_inferred_valuep
(lispval root,lispval slotid,lispval through,lispval value)
{
  struct KNO_IVPSTRUCT ivps;
  ivps.value = value; ivps.slotids = slotid; ivps.result = 0;
  if (kno_walk_tree(root,through,inherits_inferred_valuesp_fn,&ivps)<0)
    return -1;
  else return ivps.result;
}

static int pathp_fn(lispval node,void *data)
{
  struct KNO_PATHPSTRUCT *ps = (struct KNO_PATHPSTRUCT *)data;
  if (node == ps->target) {
    ps->result = 1; return 0;}
  else return 1;
}

KNO_EXPORT
/* kno_pathp:
   Arguments: a frame, a slotid, and a frame
   Returns: 1 or 0
   Returns 1 if there is a path through slotid between the two frames */
int kno_pathp(lispval root,lispval slotid,lispval to)
{
  struct KNO_PATHPSTRUCT pathstruct;
  pathstruct.target = to; pathstruct.result = 0;
  if (kno_walk_tree(root,slotid,pathp_fn,&pathstruct)<0)
    return -1;
  else return pathstruct.result;
}

/* Methods */

DEFC_PRIM("kno:inherited-get",inherited_get_method,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID});
static lispval inherited_get_method(lispval root,lispval slotid)
{
  lispval result, through, baseslot
    =kno_oid_get(slotid,slot_symbol,slotid);
  if (KNO_ABORTP(baseslot)) return  baseslot;
  else through = kno_oid_get(slotid,through_slot,EMPTY);
  if (KNO_ABORTP(through)) return through;
  else if (KNO_EQ(baseslot,slotid))
    result = kno_inherit_values(root,baseslot,through);
  else result = kno_inherit_inferred_values(root,baseslot,through);
  kno_decref(through); kno_decref(baseslot);
  return result;
}

DEFC_PRIM("kno:multi-get",multi_get_method,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID});
static lispval multi_get_method(lispval root,lispval mslotid)
{
  lispval slotids = kno_oid_get(mslotid,multi_slot,EMPTY);
  if (KNO_ABORTP(slotids)) return slotids;
  else {
    lispval answer = EMPTY;
    DO_CHOICES(slotid,slotids)
      if (PAIRP(slotid)) {
	lispval v = kno_oid_get(root,KNO_CAR(slotid),EMPTY);
	CHOICE_ADD(answer,v);}
      else {
	lispval v=
	  ((slotid == mslotid)?
	   (kno_oid_get(root,slotid,EMPTY)):
	   (kno_frame_get(root,slotid)));
	CHOICE_ADD(answer,v);}
    kno_decref(slotids);
    return answer;}
}

DEFC_PRIM("kno:multi-test",multi_test_method,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval multi_test_method(lispval root,lispval mslotid,lispval value)
{
  lispval slotids = kno_oid_get(mslotid,multi_slot,EMPTY);
  if (KNO_ABORTP(slotids)) return slotids;
  else {
    int answer = 0;
    DO_CHOICES(slotid,slotids)
      if (PAIRP(slotid)) {
	if (kno_test(root,KNO_CAR(slotid),value)) {
	  answer = 1; break;}}
      else {
	if (mslotid == slotid) {
	  if (kno_oid_test(root,slotid,value)) {answer = 1; break;}}
	else if (kno_frame_test(root,slotid,value)) {answer = 1; break;}}
    kno_decref(slotids);
    if (answer) return KNO_TRUE; else return KNO_FALSE;}
}

DEFC_PRIM("kno:multi-add",multi_add_method,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval multi_add_method(lispval root,lispval slotid,lispval value)
{
  lispval primary_slot = kno_oid_get(slotid,multi_primary_slot,EMPTY);
  if (KNO_ABORTP(primary_slot)) return primary_slot;
  else if (EMPTYP(primary_slot)) {
    lispval slotids = kno_oid_get(slotid,multi_slot,EMPTY);
    if (KNO_ABORTP(slotids)) return slotids;
    else {
      DO_CHOICES(subslotid,slotids) {
	lispval v = kno_frame_get(root,subslotid);
	if (KNO_ABORTP(v)) return v;
	else if (!(EMPTYP(v)))
	  if (kno_frame_add(root,subslotid,value)<0)
	    return KNO_ERROR;
	kno_decref(v);}
      kno_decref(slotids);}}
  else if (primary_slot == slotid) {
    if (kno_add(root,primary_slot,value)<0) {
      kno_decref(primary_slot);
      return KNO_ERROR;}}
  else if (kno_frame_add(root,primary_slot,value)<0) {
    kno_decref(primary_slot);
    return KNO_ERROR;}
  kno_decref(primary_slot);
  return VOID;
}

DEFC_PRIM("kno:multi-drop",multi_drop_method,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval multi_drop_method(lispval root,lispval slotid,lispval value)
{
  lispval slotids = kno_oid_get(slotid,multi_slot,EMPTY);
  if (KNO_ABORTP(slotids)) return slotids;
  else {
    lispval primary_slot = kno_oid_get(slotid,multi_primary_slot,EMPTY);
    if (KNO_ABORTP(primary_slot)) return primary_slot;
    else if ((SYMBOLP(primary_slot)) || (OIDP(primary_slot))) {
      if (primary_slot == slotid) {
	if (kno_drop(root,primary_slot,value)<0) {
	  kno_decref(primary_slot);
	  return KNO_ERROR;}}
      else if (kno_frame_drop(root,primary_slot,value)<0) {
	kno_decref(primary_slot);
	return KNO_ERROR;}}
    {DO_CHOICES(subslotid,slotids) {
	int probe = kno_frame_test(root,subslotid,value);
	if (probe<0) return KNO_ERROR;
	else if (probe)
	  if (kno_frame_drop(root,subslotid,value)<0)
	    return KNO_ERROR;}}
    kno_decref(slotids);
    return VOID;}
}

static inline lispval getter(lispval f,lispval s)
{
  if (OIDP(f)) return kno_frame_get(f,s);
  else if (HASHTABLEP(f))
    return kno_hashtable_get(KNO_XHASHTABLE(f),s,EMPTY);
  else if (HASHTABLEP(s))
    return kno_hashtable_get(KNO_XHASHTABLE(s),f,EMPTY);
  else return EMPTY;
}

static int kleene_get_helper
(struct KNO_HASHSET *ht,lispval frames,lispval slotids)
{
  DO_CHOICES(frame,frames) {
    int probe = kno_hashset_get(ht,frame);
    if (probe<0) return probe;
    else if (probe) {}
    else {
      if (kno_hashset_mod(ht,frame,1)<0)
	return -1;
      else {
	DO_CHOICES(slotid,slotids) {
	  lispval values = getter(frame,slotid);
	  if (KNO_ABORTP(values)) return kno_interr(values);
	  if (kleene_get_helper(ht,values,slotids)<0) {
	    kno_decref(values);
	    return -1;}
	  else kno_decref(values);}}}}
  return 1;
}

DEFC_PRIM("kno:kleene*-get",kleene_star_get_method,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_frame_type,KNO_VOID},
	  {"slotid",kno_slotid_type,KNO_VOID});
static lispval kleene_star_get_method(lispval root,lispval slotid)
{
  lispval slotids = kno_frame_get(slotid,closure_of_slot), results;
  if (KNO_ABORTP(slotids)) return slotids;
  else {
    struct KNO_HASHSET hs; memset(&hs,0,sizeof(hs));
    kno_init_hashset(&hs,1024,KNO_STACK_CONS);
    if (kleene_get_helper(&hs,root,slotids)<0) {
      kno_recycle_hashset(&hs);
      kno_decref(slotids);
      return KNO_ERROR;}
    else {
      results = kno_hashset_elts(&hs,1);
      kno_decref(slotids);
      return results;}}
}

DEFC_PRIM("kno:kleene+-get",kleene_plus_get_method,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_frame_type,KNO_VOID},
	  {"slotid",kno_slotid_type,KNO_VOID});
static lispval kleene_plus_get_method(lispval root,lispval slotid)
{
  lispval slotids = kno_frame_get(slotid,closure_of_slot), results;
  if (KNO_ABORTP(slotids)) return slotids;
  else {
    struct KNO_HASHSET hs; memset(&hs,0,sizeof(hs));
    kno_init_hashset(&hs,1024,KNO_STACK_CONS);
    if (kleene_get_helper(&hs,root,slotids)<0) {
      kno_recycle_hashset(&hs);
      kno_decref(slotids);
      return KNO_ERROR;}
    else {
      DO_CHOICES(r,root) {kno_hashset_drop(&hs,r);}
      results = kno_hashset_elts(&hs,1);
      kno_decref(slotids);
      return results;}}
}

DEFC_PRIM("kno:inherited-test",inherited_test_method,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"through",kno_any_type,KNO_VOID});
static lispval inherited_test_method(lispval root,lispval slotid,lispval value)
{
  lispval baseslot = kno_oid_get(slotid,slot_symbol,slotid);
  lispval through = kno_oid_get(slotid,through_slot,EMPTY);
  int answer = 0;
  if (KNO_ABORTP(baseslot)) {
    kno_decref(through); return baseslot;}
  else if (KNO_ABORTP(through)) {
    kno_decref(through); return baseslot;}
  else if (KNO_EQ(baseslot,slotid))
    answer = kno_inherits_valuep(root,baseslot,through,value);
  else answer = kno_inherits_inferred_valuep(root,baseslot,through,value);
  kno_decref(through); kno_decref(baseslot);
  if (answer<0) return KNO_ERROR;
  else if (answer) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("kno:inverse-getbase",inverse_getbase_method,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID});
static lispval inverse_getbase_method(lispval root,lispval slotid)
{
  lispval answer, inv_slots, others;
  answer = kno_oid_get(root,slotid,EMPTY);
  if (KNO_ABORTP(answer)) return answer;
  else inv_slots = kno_oid_get(slotid,inverse_slot,EMPTY);
  if (KNO_ABORTP(inv_slots)) {
    kno_decref(answer);
    return inv_slots;}
  else {
    lispval root_as_list = kno_conspair(root,NIL);
    others = kno_bgfind(inv_slots,root_as_list,VOID);
    kno_decref(root_as_list);}
  if (KNO_ABORTP(others)) {
    kno_decref(inv_slots); kno_decref(answer);
    return others;}
  else {
    CHOICE_ADD(answer,others);
    kno_decref(inv_slots);
    return answer;}
}

DEFC_PRIM("kno:inverse-get",inverse_get_method,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID});
static lispval inverse_get_method(lispval root,lispval slotid)
{
  lispval answer, inv_slots, others;
  answer = kno_oid_get(root,slotid,EMPTY);
  if (KNO_ABORTP(answer)) return answer;
  else inv_slots = kno_oid_get(slotid,inverse_slot,EMPTY);
  if (KNO_ABORTP(inv_slots)) {
    kno_decref(answer);
    return inv_slots;}
  else others = kno_bgfind(inv_slots,root,VOID);
  if (KNO_ABORTP(others)) {
    kno_decref(inv_slots); kno_decref(answer);
    return others;}
  else {
    CHOICE_ADD(answer,others);
    kno_decref(inv_slots);
    return answer;}
}

DEFC_PRIM("kno:inverse-test",inverse_test_method,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval inverse_test_method(lispval root,lispval slotid,lispval value)
{
  int direct_test = kno_oid_test(root,slotid,value);
  if (direct_test<0) return KNO_ERROR;
  else if (direct_test) return (KNO_TRUE);
  else if (!(OIDP(value))) return KNO_FALSE;
  else {
    lispval inv_slots = kno_oid_get(slotid,inverse_slot,EMPTY);
    if (KNO_ABORTP(inv_slots)) return inv_slots;
    else {
      int found = 0;
      DO_CHOICES(inv_slot,inv_slots) {
	int testval = kno_frame_test(value,inv_slot,root);
	if (testval) {found = testval; break;}}
      kno_decref(inv_slots);
      if (found<0) return KNO_ERROR;
      else if (found) return (KNO_TRUE);
      else return (KNO_FALSE);}}
}

DEFC_PRIM("kno:assoc-get",assoc_get_method,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID});
static lispval assoc_get_method(lispval f,lispval slotid)
{
  lispval answers = EMPTY, through, key;
  through = kno_frame_get(slotid,through_slot);
  if (KNO_ABORTP(through)) return through;
  else key = kno_frame_get(slotid,key_slot);
  if (KNO_ABORTP(key)) {
    kno_decref(through); return key;}
  else {
    DO_CHOICES(th_slot,through) {
      lispval entries = kno_frame_get(f,th_slot);
      if (KNO_ABORTP(entries)) {
	kno_decref(answers); kno_decref(through); kno_decref(key);
	return entries;}
      else {
	int ambigkey = CHOICEP(key);
	DO_CHOICES(e,entries)
	  if (PAIRP(e)) {
	    lispval car = KNO_CAR(e), cdr = KNO_CDR(e);
	    if (KNO_EQ(car,key)) {
	      kno_incref(cdr);
	      CHOICE_ADD(answers,cdr);}
	    else if ((!ambigkey) && (!(CHOICEP(car)))) {
	      if (LISP_EQUAL(car,key)) {
		kno_incref(cdr);
		CHOICE_ADD(answers,cdr);}}
	    else if (kno_overlapp(car,key)) {
	      kno_incref(cdr);
	      CHOICE_ADD(answers,cdr);}
	    else {}}
	  else if ( (KNO_TABLEP(e)) && (kno_test(e,key,KNO_VOID)) ) {
	    lispval v = kno_get(e,key,KNO_VOID);
	    CHOICE_ADD(answers,v);}
	  else {}}
      kno_decref(entries);}
    kno_decref(through); kno_decref(key);
    return answers;}
}

DEFC_PRIM("kno:assoc-test",assoc_test_method,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval assoc_test_method(lispval f,lispval slotid,lispval values)
{
  lispval answers = EMPTY, through, key;
  through = kno_frame_get(slotid,through_slot);
  if (KNO_ABORTP(through)) return through;
  else key = kno_frame_get(slotid,key_slot);
  if (KNO_ABORTP(key)) {
    kno_decref(through); return key;}
  else {
    DO_CHOICES(th_slot,through) {
      lispval entries = kno_frame_get(f,th_slot);
      if (KNO_ABORTP(entries)) {
	kno_decref(answers); kno_decref(through); kno_decref(key);
	return entries;}
      else {
	int ambigkey = CHOICEP(key), ambigval = CHOICEP(values);
	DO_CHOICES(e,entries)
	  if (PAIRP(e)) {
	    lispval car = KNO_CAR(e), cdr = KNO_CDR(e);
	    if (((ambigkey|(CHOICEP(car))) ?
		 (kno_overlapp(car,key)) :
		 (KNO_EQ(car,key))) &&
		((VOIDP(values)) ||
		 ((ambigval|(CHOICEP(cdr))) ? (kno_overlapp(cdr,values)) :
		  (KNO_EQUAL(cdr,values))))) {
	      kno_decref(entries); kno_decref(key); kno_decref(through);
	      KNO_STOP_DO_CHOICES;
	      return KNO_TRUE;}}
	  else if ( (KNO_TABLEP(e)) && (kno_test(e,th_slot,values)) ) {
	    KNO_STOP_DO_CHOICES;
	    return KNO_TRUE;}
	  else {}}
      kno_decref(entries);}
    kno_decref(through); kno_decref(key);
    return KNO_FALSE;}
}

DEFC_PRIM("kno:assoc-add",assoc_add_method,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval assoc_add_method(lispval f,lispval slotid,lispval value)
{
  lispval through, key;
  through = kno_frame_get(slotid,through_slot);
  if (KNO_ABORTP(through))
    return through;
  else key = kno_frame_get(slotid,key_slot);
  if (KNO_ABORTP(key)) {
    kno_decref(through);
    return key;}
  else if (KNO_SLOTMAPP(through)) {
    kno_add(through,key,value);
    kno_oid_store(f,through_slot,through);
    return VOID;}
  else {
    lispval pair = kno_conspair(kno_incref(key),kno_incref(value));
    kno_oid_add(f,through,pair);
    kno_decref(pair);
    return VOID;}
}

DEFC_PRIM("kno:assoc-drop",assoc_drop_method,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval assoc_drop_method(lispval f,lispval slotid,lispval value)
{
  lispval through, key;
  through = kno_frame_get(slotid,through_slot);
  if (KNO_ABORTP(through)) return through;
  else key = kno_frame_get(slotid,key_slot);
  if (KNO_ABORTP(key)) {
    kno_decref(through); return key;}
  else if (KNO_SLOTMAPP(through)) {
    kno_drop(through,key,value);
    kno_store(f,through_slot,through);
    return VOID;}
  else {
    lispval pair = kno_conspair(kno_incref(key),kno_incref(value));
    kno_oid_drop(f,through,pair);
    kno_decref(pair);
    return VOID;}
}

DEFC_PRIM("kno:car-get",car_get_method,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID});
static lispval car_get_method(lispval f,lispval slotid)
{
  lispval result = (EMPTY);
  lispval values = kno_oid_get(f,slotid,EMPTY);
  if (KNO_ABORTP(values))
    return values;
  else {
    DO_CHOICES(value,values)
      if (PAIRP(value)) {
	lispval car = KNO_CAR(value);
	kno_incref(car);
	CHOICE_ADD(result,car);}
      else {
	kno_decref(result);
	return kno_type_error("pair","car_get_method",value);}}
  kno_decref(values);
  return result;
}

DEFC_PRIM("kno:paired-get",paired_get_method,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID});
static lispval paired_get_method(lispval f,lispval slotid)
{
  lispval result = (EMPTY);
  lispval values = kno_oid_get(f,slotid,EMPTY);
  if (KNO_ABORTP(values)) return values;
  else {
    DO_CHOICES(value,values)
      if (PAIRP(value)) {
	lispval car = KNO_CAR(value);
	kno_incref(car);
	CHOICE_ADD(result,car);}
      else {
	kno_incref(value);
	CHOICE_ADD(result,value);}}
  kno_decref(values);
  return result;
}

DEFC_PRIM("kno:paired-test",paired_test_method,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval paired_test_method(lispval f,lispval slotid,lispval v)
{
  int found = 0;
  lispval values = kno_oid_get(f,slotid,EMPTY);
  if (KNO_ABORTP(values)) return values;
  else {
    DO_CHOICES(value,values) {
      if (PAIRP(value)) {
	if (KNO_EQUAL(KNO_CAR(value),v)) {found = 1; break;}}
      else if (KNO_EQUAL(value,v)) {found = 1; break;}}
    kno_decref(values);
    if (found) return KNO_TRUE;
    else return KNO_FALSE;}
}

DEFC_PRIM("kno:paired-drop",paired_drop_method,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval paired_drop_method(lispval f,lispval slotid,lispval v)
{
  if (PAIRP(v)) {
    if (kno_oid_drop(f,slotid,v)<0)
      return KNO_ERROR;
    else return VOID;}
  else {
    int found = 0;
    lispval values = kno_oid_get(f,slotid,EMPTY);
    if (KNO_ABORTP(values)) return values;
    else {
      DO_CHOICES(value,values) {
	if (PAIRP(value)) {
	  if (KNO_EQUAL(KNO_CAR(value),v)) {found = 1; break;}}
	else if (KNO_EQUAL(value,v)) {found = 1; break;}}
      if (found) {
	lispval new_values = EMPTY;
	DO_CHOICES(value,values) {
	  if (PAIRP(value))
	    if (KNO_EQUAL(KNO_CAR(value),v)) {}
	    else {
	      kno_incref(value);
	      CHOICE_ADD(new_values,value);}
	  else if (KNO_EQUAL(value,v)) {}
	  else {
	    kno_incref(value);
	    CHOICE_ADD(new_values,value);}}
	if (kno_store(f,slotid,new_values)<0) {
	  kno_decref(values); kno_decref(new_values);
	  return KNO_ERROR;}
	kno_decref(values); kno_decref(new_values);
	return VOID;}
      else {
	kno_decref(values); return VOID;}}}
}

static lispval implies_slot;

DEFC_PRIM("kno:clear-implies!",clear_implies_effect,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval clear_implies_effect(lispval f,lispval slotid,lispval v)
{
  lispval implies = kno_get(slotid,implies_slot,EMPTY);
  kno_clear_slotcache_entry(slotid,f);
  {DO_CHOICES(imply,implies) clear_implies_effect(f,imply,v);}
  return EMPTY;
}

static void init_symbols()
{
  frame_symbol = kno_intern("frame");
  slot_symbol = kno_intern("slotid");
  value_symbol = kno_intern("value");
  through_slot = kno_intern("through");
  key_slot = kno_intern("key");
  derive_slot = kno_intern("derivation");
  inverse_slot = kno_intern("inverse");
  closure_of_slot = kno_intern("closure-of");
  multi_slot = kno_intern("slots");
  multi_primary_slot = kno_intern("primary-slot");
  index_slot = kno_intern("index");
  implies_slot = kno_intern("implies");
}

DEFC_PRIM("kno:add!",lisp_add_effect,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval lisp_add_effect(lispval f,lispval s,lispval v)
{
  if (kno_add(f,s,v)<0) return KNO_ERROR;
  else return VOID;
}
DEFC_PRIM("kno:drop!",lisp_drop_effect,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval lisp_drop_effect(lispval f,lispval s,lispval v)
{
  if (kno_drop(f,s,v)<0) return KNO_ERROR;
  else return VOID;
}

DEFC_PRIM("kno:%get",lisp_pget_method,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"default",kno_any_type,KNO_VOID});
static lispval lisp_pget_method(lispval f,lispval s,lispval dflt)
{
  if ( (KNO_VOIDP(dflt)) || (KNO_DEFAULTP(dflt)) )
    dflt = KNO_EMPTY_CHOICE;
  return kno_get(f,s,dflt);
}

DEFC_PRIM("kno:get",lisp_get_method,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID});
static lispval lisp_get_method(lispval f,lispval s)
{
  return kno_frame_get(f,s);
}
DEFC_PRIM("kno:test",lisp_test_method,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "**undocumented**",
	  {"frame",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID});
static lispval lisp_test_method(lispval f,lispval s,lispval v)
{
  if (kno_frame_test(f,s,v))
    return KNO_TRUE;
  else return KNO_FALSE;
}


KNO_EXPORT void kno_init_methods_c()
{
  lispval m;

  u8_register_source_file(_FILEINFO);

  init_symbols();

  KNO_INIT_STATIC_CONS(&method_table,kno_hashtable_type);
  kno_make_hashtable(&method_table,67);
  m = kno_method_table = ((lispval)(&method_table));

  KNO_LINK_CPRIM("kno:get",lisp_get_method,2,m);
  KNO_LINK_CPRIM("kno:test",lisp_test_method,3,m);

  KNO_LINK_CPRIM("kno:%get",lisp_pget_method,3,m);
  KNO_LINK_CPRIM("kno:add!",lisp_add_effect,3,m);
  KNO_LINK_CPRIM("kno:drop!",lisp_drop_effect,3,m);

  KNO_LINK_CPRIM("kno:inherited-get",inherited_get_method,2,m);
  KNO_LINK_CPRIM("kno:inverse-get",inverse_get_method,2,m);
  KNO_LINK_CPRIM("kno:inverse-getbase",inverse_getbase_method,2,m);
  KNO_LINK_CPRIM("kno:assoc-get",assoc_get_method,2,m);
  KNO_LINK_CPRIM("kno:multi-get",multi_get_method,2,m);
  KNO_LINK_CPRIM("kno:car-get",car_get_method,2,m);
  KNO_LINK_CPRIM("kno:paired-get",paired_get_method,2,m);
  KNO_LINK_CPRIM("kno:kleene*-get",kleene_star_get_method,2,m);
  KNO_LINK_CPRIM("kno:kleene+-get",kleene_plus_get_method,2,m);

  KNO_LINK_CPRIM("kno:multi-test",multi_test_method,3,m);
  KNO_LINK_CPRIM("kno:inherited-test",inherited_test_method,3,m);
  KNO_LINK_CPRIM("kno:inverse-test",inverse_test_method,3,m);
  KNO_LINK_CPRIM("kno:assoc-test",assoc_test_method,3,m);
  KNO_LINK_CPRIM("kno:paired-test",paired_test_method,3,m);

  KNO_LINK_CPRIM("kno:multi-add",multi_add_method,3,m);
  KNO_LINK_CPRIM("kno:multi-drop",multi_drop_method,3,m);

  KNO_LINK_CPRIM("kno:assoc-add",assoc_add_method,3,m);
  KNO_LINK_CPRIM("kno:assoc-drop",assoc_drop_method,3,m);
  KNO_LINK_CPRIM("kno:paired-drop",paired_drop_method,3,m);

  KNO_LINK_CPRIM("kno:clear-implies!",clear_implies_effect,3,m);

  KNO_LINK_ALIAS("get",lisp_get_method,m);
  KNO_LINK_ALIAS("test",lisp_test_method,m);
  KNO_LINK_ALIAS("%get",lisp_pget_method,m);
  KNO_LINK_ALIAS("add!",lisp_add_effect,m);
  KNO_LINK_ALIAS("add",lisp_add_effect,m);
  KNO_LINK_ALIAS("drop!",lisp_drop_effect,m);
  KNO_LINK_ALIAS("drop",lisp_drop_effect,m);

  KNO_LINK_ALIAS("kno:kleene-get",kleene_plus_get_method,m);
  KNO_LINK_ALIAS("kno:inv-get",inverse_get_method,m);
  KNO_LINK_ALIAS("kno:inv-getbase",inverse_getbase_method,m);
  KNO_LINK_ALIAS("kno:inv-test",inverse_test_method,m);

  KNO_LINK_ALIAS("fd:add!",lisp_add_effect,m);
  KNO_LINK_ALIAS("fd:drop!",lisp_drop_effect,m);
  KNO_LINK_ALIAS("fd:inherited-get",inherited_get_method,m);
  KNO_LINK_ALIAS("fd:inverse-get",inverse_get_method,m);
  KNO_LINK_ALIAS("fd:inverse-getbase",inverse_getbase_method,m);
  KNO_LINK_ALIAS("fd:assoc-get",assoc_get_method,m);
  KNO_LINK_ALIAS("fd:multi-get",multi_get_method,m);
  KNO_LINK_ALIAS("fd:car-get",car_get_method,m);
  KNO_LINK_ALIAS("fd:paired-get",paired_get_method,m);
  KNO_LINK_ALIAS("fd:kleene*-get",kleene_star_get_method,m);
  KNO_LINK_ALIAS("fd:kleene+-get",kleene_plus_get_method,m);
  KNO_LINK_ALIAS("fd:multi-test",multi_test_method,m);
  KNO_LINK_ALIAS("fd:inherited-test",inherited_test_method,m);
  KNO_LINK_ALIAS("fd:inverse-test",inverse_test_method,m);
  KNO_LINK_ALIAS("fd:assoc-test",assoc_test_method,m);
  KNO_LINK_ALIAS("fd:paired-test",paired_test_method,m);
  KNO_LINK_ALIAS("fd:multi-add",multi_add_method,m);
  KNO_LINK_ALIAS("fd:multi-drop",multi_drop_method,m);
  KNO_LINK_ALIAS("fd:assoc-add",assoc_add_method,m);
  KNO_LINK_ALIAS("fd:assoc-drop",assoc_drop_method,m);
  KNO_LINK_ALIAS("fd:paired-drop",paired_drop_method,m);
  KNO_LINK_ALIAS("fd:clear-implies!",clear_implies_effect,m);

  KNO_LINK_ALIAS("fd:kleene-get",kleene_plus_get_method,m);
  KNO_LINK_ALIAS("fd:inv-get",inverse_get_method,m);
  KNO_LINK_ALIAS("fd:inv-getbase",inverse_getbase_method,m);
  KNO_LINK_ALIAS("fd:inv-test",inverse_test_method,m);

  link_local_cprims();

}

static void link_local_cprims()
{
}
