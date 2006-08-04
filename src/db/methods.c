/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/pools.h"
#include "fdb/indices.h"
#include "fdb/apply.h"
#include "fdb/frames.h"
#include "fdb/methods.h"

static fdtype frame_symbol, slot_symbol, value_symbol;
static fdtype through_slot, derive_slot, inverse_slot;
static fdtype multi_slot, multi_primary_slot, key_slot;
static fdtype closure_of_slot, index_slot;

static struct FD_HASHTABLE method_table; fdtype fd_method_table;

/* Walk proc */

static int keep_walking(struct FD_HASHSET *seen,fdtype node,fdtype slotids,
			fd_tree_walkfn walk,void *data)
{
  if (fd_hashset_get(seen,node)) return 1;
  else {
    int retval=0;
    fd_hashset_mod(seen,node,1);
    if ((walk==NULL) || ((retval=walk(node,data))>0)) {
      FD_DO_CHOICES(slotid,slotids) {
	fdtype values=fd_frame_get(node,slotid);
	if (FD_ABORTP(values))
	  return fd_interr(values);
	else {
	  int retval=0;
	  FD_DO_CHOICES(v,values)
	    if (fd_hashset_get(seen,v)) {}
	    else if ((retval=keep_walking(seen,v,slotids,walk,data))>0) {}
	    else {
	      fd_decref(values);
	      return retval;}
	  fd_decref(values);}}
      return 1;}
    else return retval;}
}

FD_EXPORT int
fd_walk_tree(fdtype roots,fdtype slotids,fd_tree_walkfn walk,void *data)
{
  struct FD_HASHSET ht; fdtype h; int retval=0;
  ht.consbits=0; fd_init_hashset(&ht,1024);
  {FD_DO_CHOICES(root,roots) 
     if ((retval=keep_walking(&ht,root,slotids,walk,data))<=0) {
       fd_recycle_hashset(&ht);
       return retval;}}
  fd_recycle_hashset(&ht);
  return retval;
}

FD_EXPORT int
fd_collect_tree(struct FD_HASHSET *h,
		fdtype roots,fdtype slotids)
{
  FD_DO_CHOICES(root,roots) 
    if (keep_walking(h,root,slotids,NULL,NULL)<0)
      return -1;
  return 1;
}

/* Get bottom and Get top */

FD_EXPORT fdtype fd_get_basis(fdtype collection,fdtype lattice)
{
  struct FD_HASHSET ht;
  fdtype root=FD_EMPTY_CHOICE, result=FD_EMPTY_CHOICE;
  ht.consbits=0; fd_init_hashset(&ht,1024);
  {FD_DO_CHOICES(node,collection) {
    FD_DO_CHOICES(slotid,lattice) {
      fdtype v=fd_frame_get(node,slotid);
      if (FD_EXCEPTIONP(v)) {
	fd_decref(root);
	fd_recycle_hashtable(&ht);
	return v;}
      else {FD_ADD_TO_CHOICE(root,v);}}}}
  fd_collect_tree(&ht,root,lattice);
  {FD_DO_CHOICES(node,collection)
     if (fd_hashset_get(&ht,node)) {}
     else {
       FD_ADD_TO_CHOICE(result,fd_incref(node));}}
  fd_decref(root);
  fd_recycle_hashset(&ht);
  return result;
}

/* Inference procedures */

static int inherit_inferred_values_fn(fdtype node,void *data)
{
  struct FD_IVSTRUCT *ivs=(struct FD_IVSTRUCT *)data;
  fdtype values=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(slotid,ivs->slotids) {
    fdtype v=fd_frame_get(node,slotid);
    if (FD_ABORTP(v)) {
      fd_decref(values);
      return fd_interr(v);}
    else {FD_ADD_TO_CHOICE(values,v);}}
  FD_ADD_TO_CHOICE(ivs->result,values);
  return 1;
}

static int inherit_values_fn(fdtype node,void *data)
{
  struct FD_IVSTRUCT *ivs=(struct FD_IVSTRUCT *)data;
  fdtype values=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(slotid,ivs->slotids) {
    fdtype v=((FD_OIDP(node)) ? (fd_oid_get(node,slotid,FD_EMPTY_CHOICE)) :
	       (fd_get(node,slotid,FD_EMPTY_CHOICE)));
    if (FD_ABORTP(v)) {
      fd_decref(values);
      return fd_interr(v);}
    else {FD_ADD_TO_CHOICE(values,v);}}
  FD_ADD_TO_CHOICE(ivs->result,values);
  return 1;
}

FD_EXPORT
/* fd_inherit_values:
     Arguments: a frame and two slotids
     Returns: a fdtype pointer
     Searches for a value for the first slotid through the lattice
     defined by the second slotid. */
fdtype fd_inherit_values(fdtype root,fdtype slotid,fdtype through)
{
  struct FD_IVSTRUCT ivs;
  ivs.result=FD_EMPTY_CHOICE; ivs.slotids=slotid;
  if (fd_walk_tree(root,through,inherit_values_fn,&ivs)<0) {
    fd_decref(ivs.result);
    return fd_erreify();}
  else return fd_simplify_choice(ivs.result);
}

FD_EXPORT
/* fd_inherit_values:
     Arguments: a frame and two slotids
     Returns: a fdtype pointer
  Searches for a value for the first slotid through the lattice
  defined by the second slotid. */
fdtype fd_inherit_inferred_values(fdtype root,fdtype slotid,fdtype through)
{
  struct FD_IVSTRUCT ivs;
  ivs.result=FD_EMPTY_CHOICE; ivs.slotids=slotid;
  if (fd_walk_tree(root,through,inherit_inferred_values_fn,&ivs)<0) {
    fd_decref(ivs.result);
    return fd_erreify();}
  else return fd_simplify_choice(ivs.result);
}

static int inherits_inferred_valuesp_fn(fdtype node,void *data)
{
  struct FD_IVPSTRUCT *ivps=(struct FD_IVPSTRUCT *)data;
  fdtype values=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(slotid,ivps->slotids) {
    int result=fd_test(node,slotid,ivps->value);
    if (result<0) return -1;
    else if (result) {
      ivps->result=1; return 0;}}
  return 1;
}

static int inherits_valuesp_fn(fdtype node,void *data)
{
  struct FD_IVPSTRUCT *ivps=(struct FD_IVPSTRUCT *)data;
  fdtype values=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(slotid,ivps->slotids) {
    int testval=
      ((FD_OIDP(node)) ?
       (fd_oid_test(node,slotid,ivps->value)) :
       (fd_test(node,slotid,ivps->value)));
    if (testval<0) return -1;
    else if (testval) {
      ivps->result=1; return 0;}}
  return 1;
}

FD_EXPORT
/* fd_inherits_valuep:
     Arguments: a frame, two slotids, and a value
     Returns: 1 or 0
  Returns 1 if the value can be inherited for the first slotid
  going through the lattice defined by the second slotid. */
int fd_inherits_valuep
  (fdtype root,fdtype slotid,fdtype through,fdtype value)
{
  struct FD_IVPSTRUCT ivps;
  ivps.value=value; ivps.slotids=slotid; ivps.result=0;
  if (fd_walk_tree(root,through,inherits_valuesp_fn,&ivps)<0)
    return -1;
  else return ivps.result;
}
  
FD_EXPORT
/* fd_inherits_valuep:
     Arguments: a frame, two slotids, and a value
     Returns: 1 or 0
  Returns 1 if the value can be inherited for the first slotid
  going through the lattice defined by the second slotid. */
int fd_inherits_inferred_valuep
  (fdtype root,fdtype slotid,fdtype through,fdtype value)
{
  struct FD_IVPSTRUCT ivps;
  ivps.value=value; ivps.slotids=slotid; ivps.result=0;
  if (fd_walk_tree(root,through,inherits_inferred_valuesp_fn,&ivps)<0)
    return -1;
  else return ivps.result;
}

static int pathp_fn(fdtype node,void *data)
{
  struct FD_PATHPSTRUCT *ps=(struct FD_PATHPSTRUCT *)data;
  if (node == ps->target) {
    ps->result=1; return 0;}
  else return 1;
}

FD_EXPORT
/* fd_pathp:
     Arguments: a frame, a slotid, and a frame
     Returns: 1 or 0
  Returns 1 if there is a path through slotid between the two frames */
int fd_pathp(fdtype root,fdtype slotid,fdtype to)
{
  struct FD_PATHPSTRUCT pathstruct;
  pathstruct.target=to; pathstruct.result=0;
  if (fd_walk_tree(root,slotid,pathp_fn,&pathstruct)<0)
    return -1;
  else return pathstruct.result;
}

/* Methods */

static fdtype inherited_get_method(fdtype root,fdtype slotid)
{
  fdtype result, through, baseslot
    =fd_oid_get(slotid,slot_symbol,slotid);
  if (FD_ABORTP(baseslot)) return  baseslot;
  else through=fd_oid_get(slotid,through_slot,FD_EMPTY_CHOICE);
  if (FD_ABORTP(through)) return through;
  else if (FD_EQ(baseslot,slotid))
    result=fd_inherit_values(root,baseslot,through);
  else result=fd_inherit_inferred_values(root,baseslot,through);
  fd_decref(through); fd_decref(baseslot);
  return result;
}

static fdtype multi_get_method(fdtype root,fdtype slotid)
{
  fdtype slotids=fd_oid_get(slotid,multi_slot,FD_EMPTY_CHOICE);
  if (FD_ABORTP(slotids)) return slotids;
  else {
    fdtype answer=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(slid,slotids) {
      fdtype v=
	((slid==slotid)?
	 (fd_oid_get(root,slid,FD_EMPTY_CHOICE)):
	 (fd_frame_get(root,slid)));
      FD_ADD_TO_CHOICE(answer,v);}
    fd_decref(slotids);
    return answer;}
}

static fdtype multi_test_method(fdtype root,fdtype slotid,fdtype value)
{
  fdtype slotids=fd_oid_get(slotid,multi_slot,FD_EMPTY_CHOICE);
  if (FD_ABORTP(slotids)) return slotids;
  else {
    int answer=0;
    FD_DO_CHOICES(slid,slotids) {
      if (slid==slotid) {
	if (fd_oid_test(root,slid,value)) {answer=1; break;}}
      else if (fd_frame_test(root,slid,value)) {answer=1; break;}}
    fd_decref(slotids);
    if (answer) return FD_TRUE; else return FD_FALSE;}
}

static fdtype multi_add_method(fdtype root,fdtype slotid,fdtype value)
{
  fdtype primary_slot=fd_oid_get(slotid,multi_primary_slot,FD_EMPTY_CHOICE);
  if (FD_ABORTP(primary_slot)) return primary_slot;
  else if (FD_EMPTY_CHOICEP(primary_slot)) {
    fdtype slotids=fd_oid_get(slotid,multi_slot,FD_EMPTY_CHOICE);
    if (FD_ABORTP(slotids)) return slotids;
    else {
      FD_DO_CHOICES(subslotid,slotids) {
	fdtype v=fd_frame_get(root,subslotid);
	if (FD_ABORTP(v)) return v;
	else if (!(FD_EMPTY_CHOICEP(v)))
	  if (fd_frame_add(root,subslotid,value)<0)
	    return fd_erreify();
	fd_decref(v);}
      fd_decref(slotids);}}
  else if (primary_slot==slotid) {
    if (fd_add(root,primary_slot,value)<0) {
      fd_decref(primary_slot);
      return fd_erreify();}}
  else if (fd_frame_add(root,primary_slot,value)<0) {
    fd_decref(primary_slot);
    return fd_erreify();}
  fd_decref(primary_slot);
  return FD_VOID;
}

static fdtype multi_drop_method(fdtype root,fdtype slotid,fdtype value)
{
  fdtype slotids=fd_oid_get(slotid,multi_slot,FD_EMPTY_CHOICE);
  if (FD_ABORTP(slotids)) return slotids;
  else {
    fdtype primary_slot=fd_oid_get(slotid,multi_primary_slot,FD_EMPTY_CHOICE);
    if (FD_ABORTP(primary_slot)) return primary_slot;
    else if ((FD_SYMBOLP(primary_slot)) || (FD_OIDP(primary_slot)))
      if (primary_slot==slotid) {
	if (fd_drop(root,primary_slot,value)<0) {
	  fd_decref(primary_slot);
	  return fd_erreify();}}
      else if (fd_frame_drop(root,primary_slot,value)<0) {
	fd_decref(primary_slot);
	return fd_erreify();}
    {FD_DO_CHOICES(subslotid,slotids) {
      int probe=fd_frame_test(root,subslotid,value);
      if (probe<0) return fd_erreify();
      else if (probe)
	if (fd_frame_drop(root,subslotid,value)<0)
	  return fd_erreify();}}
    fd_decref(slotids);
    return FD_VOID;}
}

static inline fdtype getter(fdtype f,fdtype s)
{
  if (FD_OIDP(f)) return fd_frame_get(f,s);
  else if (FD_PRIM_TYPEP(f,fd_hashtable_type))
    return fd_hashtable_get(FD_XHASHTABLE(f),s,FD_EMPTY_CHOICE);
  else if (FD_PRIM_TYPEP(s,fd_hashtable_type))
    return fd_hashtable_get(FD_XHASHTABLE(s),f,FD_EMPTY_CHOICE);
  else return FD_EMPTY_CHOICE;
}

static int kleene_get_helper
  (struct FD_HASHSET *ht,fdtype frames,fdtype slotids)
{
  FD_DO_CHOICES(frame,frames) {
    int probe=fd_hashset_get(ht,frame);
    if (probe<0) return probe;
    else if (probe) {}
    else {
      if (fd_hashset_mod(ht,frame,1)<0)
	return -1;
      else {
	FD_DO_CHOICES(slotid,slotids) {
	  fdtype values=getter(frame,slotid);
	  if (FD_ABORTP(values)) return fd_interr(values);
	  if (kleene_get_helper(ht,values,slotids)<0) {
	    fd_decref(values);
	    return -1;}
	  else fd_decref(values);}}}}
  return 1;
}

static fdtype kleene_get_method(fdtype root,fdtype slotid)
{
  fdtype slotids=fd_frame_get(slotid,closure_of_slot), results;
  if (FD_ABORTP(slotids)) return slotids;
  else {
    struct FD_HASHSET hs; hs.consbits=0;
    fd_init_hashset(&hs,1024);
    if (kleene_get_helper(&hs,root,slotids)<0) {
      fd_recycle_hashset(&hs);
      fd_decref(slotids);
      return fd_erreify();}
    else {
      results=fd_hashset_elts(&hs,1);
      fd_decref(slotids); 
      return results;}}
}

static fdtype inherited_test_method(fdtype root,fdtype slotid,fdtype value)
{
  fdtype baseslot=fd_oid_get(slotid,slot_symbol,slotid);
  fdtype through=fd_oid_get(slotid,through_slot,FD_EMPTY_CHOICE);
  int answer=0;
  if (FD_ABORTP(baseslot)) {
    fd_decref(through); return baseslot;}
  else if (FD_ABORTP(through)) {
    fd_decref(through); return baseslot;}
  else if (FD_EQ(baseslot,slotid))
    answer=fd_inherits_valuep(root,baseslot,through,value);
  else answer=fd_inherits_inferred_valuep(root,baseslot,through,value);
  fd_decref(through); fd_decref(baseslot);
  if (answer<0) return fd_erreify();
  else if (answer) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype inverse_get_method(fdtype root,fdtype slotid)
{
  fdtype answer, inv_slots, others;
  answer=fd_oid_get(root,slotid,FD_EMPTY_CHOICE);
  if (FD_ABORTP(answer)) return answer;
  else inv_slots=fd_oid_get(slotid,inverse_slot,FD_EMPTY_CHOICE);
  if (FD_ABORTP(inv_slots)) {
    fd_decref(answer);
    return inv_slots;}
  else others=fd_bgfind(inv_slots,root,FD_VOID);
  if (FD_ABORTP(others)) {
    fd_decref(inv_slots); fd_decref(answer);
    return others;}
  else {
    FD_ADD_TO_CHOICE(answer,others);
    fd_decref(inv_slots);
    return answer;}
}

static fdtype inverse_test_method(fdtype root,fdtype slotid,fdtype value)
{
  int direct_test=fd_oid_test(root,slotid,value);
  if (direct_test<0) return fd_erreify();
  else if (direct_test) return (FD_TRUE);
  else {
    fdtype inv_slots=fd_oid_get(slotid,inverse_slot,FD_EMPTY_CHOICE);
    if (FD_ABORTP(inv_slots)) return inv_slots;
    else {
      int found=0;
      FD_DO_CHOICES(inv_slot,inv_slots) {
	int testval=fd_frame_test(value,inv_slot,root);
	if (testval) {found=testval; break;}}
      fd_decref(inv_slots);
      if (found<0) return fd_erreify();
      else if (found) return (FD_TRUE);
      else return (FD_FALSE);}}
}

static fdtype assoc_get_method(fdtype f,fdtype slotid)
{
  fdtype answers=FD_EMPTY_CHOICE, through, key;
  through=fd_frame_get(slotid,through_slot);
  if (FD_ABORTP(through)) return through;
  else key=fd_frame_get(slotid,key_slot);
  if (FD_ABORTP(key)) {
    fd_decref(through); return key;}
  else {
    FD_DO_CHOICES(th_slot,through) {
      fdtype entries=fd_frame_get(f,th_slot);
      if (FD_ABORTP(entries)) {
	fd_decref(answers); fd_decref(through); fd_decref(key);
	return entries;}
      else {
	FD_DO_CHOICES(e,entries)
	  if ((FD_PAIRP(e)) && 
	      ((FD_CHOICEP(key)) ? (fd_choice_containsp(FD_CAR(e),key)) :
	       (FD_EQ(FD_CAR(e),key)))) {
	    fdtype v=((FD_PAIRP(FD_CDR(e))) ? (FD_CAR(FD_CDR(e))) : (FD_CDR(e)));
	    FD_ADD_TO_CHOICE(answers,fd_incref(v));}
	fd_decref(entries);}}
    fd_decref(through); fd_decref(key);
    return answers;}
}

static fdtype assoc_add_method(fdtype f,fdtype slotid,fdtype value)
{
  fdtype answers=FD_EMPTY_CHOICE, through, key;
  through=fd_frame_get(slotid,through_slot);
  if (FD_ABORTP(through)) return through;
  else key=fd_frame_get(slotid,key_slot);
  if (FD_ABORTP(key)) {
    fd_decref(through); return key;}
  else {
    fdtype pair=fd_init_pair(NULL,fd_incref(key),fd_incref(value));
    fd_oid_add(f,through,pair);
    fd_decref(pair);
    return FD_VOID;}
}

static fdtype assoc_drop_method(fdtype f,fdtype slotid,fdtype value)
{
  fdtype answers=FD_EMPTY_CHOICE, through, key;
  through=fd_frame_get(slotid,through_slot);
  if (FD_ABORTP(through)) return through;
  else key=fd_frame_get(slotid,key_slot);
  if (FD_ABORTP(key)) {
    fd_decref(through); return key;}
  else {
    fdtype pair=fd_init_pair(NULL,fd_incref(key),fd_incref(value));
    fd_oid_drop(f,through,pair);
    fd_decref(pair);
    return FD_VOID;}
}

static fdtype car_get_method(fdtype f,fdtype slotid)
{
  fdtype result=(FD_EMPTY_CHOICE);
  fdtype values=fd_oid_get(f,slotid,FD_EMPTY_CHOICE);
  if (FD_ABORTP(values)) return values;
  else {
    FD_DO_CHOICES(value,values)
      if (FD_PAIRP(value)) {
	fdtype v=FD_CAR(value);
	FD_ADD_TO_CHOICE(result,fd_incref(v));}
      else {
	fd_decref(result);
	return fd_type_error("pair","car_get_method",value);}}
  fd_decref(values);
  return result;
}

static fdtype paired_get_method(fdtype f,fdtype slotid)
{
  fdtype result=(FD_EMPTY_CHOICE);
  fdtype values=fd_oid_get(f,slotid,FD_EMPTY_CHOICE);
  if (FD_ABORTP(values)) return values;
  else {
    FD_DO_CHOICES(value,values)
      if (FD_PAIRP(value)) {
	fdtype v=FD_CAR(value);
	FD_ADD_TO_CHOICE(result,fd_incref(v));}
      else {
	FD_ADD_TO_CHOICE(result,fd_incref(value));}}
  fd_decref(values);
  return result;
}

static fdtype paired_test_method(fdtype f,fdtype slotid,fdtype v)
{
  int found=0;
  fdtype values=fd_oid_get(f,slotid,FD_EMPTY_CHOICE);
  if (FD_ABORTP(values)) return values;
  else {
    FD_DO_CHOICES(value,values) {
      if (FD_PAIRP(value)) {
	if (FD_EQUAL(FD_CAR(value),v)) {found=1; break;}}
      else if (FD_EQUAL(value,v)) {found=1; break;}}
    fd_decref(values);
    if (found) return FD_TRUE;
    else return FD_FALSE;}
}

static fdtype paired_drop_method(fdtype f,fdtype slotid,fdtype v)
{
  if (FD_PAIRP(v)) {
    if (fd_oid_drop(f,slotid,v)<0)
      return fd_erreify();
    else return FD_VOID;}
  else {
    int found=0;
    fdtype values=fd_oid_get(f,slotid,FD_EMPTY_CHOICE);
    if (FD_ABORTP(values)) return values;
    else {
      FD_DO_CHOICES(value,values) {
	if (FD_PAIRP(value)) {
	  if (FD_EQUAL(FD_CAR(value),v)) {found=1; break;}}
	else if (FD_EQUAL(value,v)) {found=1; break;}}
      if (found) {
	fdtype new_values=FD_EMPTY_CHOICE;
	FD_DO_CHOICES(value,values) {
	  if (FD_PAIRP(value)) 
	    if (FD_EQUAL(FD_CAR(value),v)) {}
	    else {
	      FD_ADD_TO_CHOICE(new_values,fd_incref(value));}
	  else if (FD_EQUAL(value,v)) {}
	  else {
	    FD_ADD_TO_CHOICE(new_values,fd_incref(value));}}
	if (fd_store(f,slotid,new_values)<0) {
	  fd_decref(values); fd_decref(new_values);
	  return fd_erreify();}
	fd_decref(values); fd_decref(new_values);
	return FD_VOID;}
      else {
	fd_decref(values); return FD_VOID;}}}
}

static fdtype implies_slot;

static fdtype clear_implies_effect(fdtype f,fdtype slotid,fdtype v)
{
  fdtype implies=fd_get(slotid,implies_slot,FD_EMPTY_CHOICE);
  fd_clear_slotcache_entry(slotid,f);
  {FD_DO_CHOICES(imply,implies) clear_implies_effect(f,imply,v);}
  return FD_EMPTY_CHOICE;
}

static void init_symbols()
{
  frame_symbol=fd_intern("FRAME");
  slot_symbol=fd_intern("SLOTID");
  value_symbol=fd_intern("VALUE");
  through_slot=fd_intern("THROUGH");
  key_slot=fd_intern("KEY");
  derive_slot=fd_intern("DERIVATION");
  inverse_slot=fd_intern("INVERSE");
  closure_of_slot=fd_intern("CLOSURE-OF");
  multi_slot=fd_intern("SLOTS");
  multi_primary_slot=fd_intern("PRIMARY-SLOT");
  index_slot=fd_intern("INDEX");
  implies_slot=fd_intern("IMPLIES");
}

static fdtype lisp_add(fdtype f,fdtype s,fdtype v)
{
  if (fd_add(f,s,v)<0) return fd_erreify();
  else return FD_VOID;
}
static fdtype lisp_drop(fdtype f,fdtype s,fdtype v)
{
  if (fd_drop(f,s,v)<0) return fd_erreify();
  else return FD_VOID;
}

FD_EXPORT void fd_init_methods_c()
{
  fdtype m;

  fd_register_source_file(versionid);

  init_symbols();

  fd_make_hashtable(&method_table,67,NULL);
  m=fd_method_table=((fdtype)(&method_table));

  fd_defn(m,fd_make_cprim2("FD:INHERITED-GET",inherited_get_method,2));
  fd_defn(m,fd_make_cprim3("FD:INHERITED-TEST",inherited_test_method,3));
  fd_defn(m,fd_make_cprim2("FD:MULTI-GET",multi_get_method,2));
  fd_defn(m,fd_make_cprim3("FD:MULTI-TEST",multi_test_method,3));
  fd_defn(m,fd_make_cprim3("FD:MULTI-ADD",multi_add_method,3));
  fd_defn(m,fd_make_cprim3("FD:MULTI-DROP",multi_drop_method,3));
  {
    fdtype invget=fd_make_cprim2("FD:INVERSE-GET",inverse_get_method,2);
    fdtype invtest=fd_make_cprim3("FD:INVERSE-TEST",inverse_test_method,3);
    fd_defn(m,invget); fd_defn(m,invtest);
    fd_store(m,fd_intern("FD:INV-GET"),invget);
    fd_store(m,fd_intern("FD:INV-TEST"),invtest);}

  fd_defn(m,fd_make_cprim2("FD:ASSOC-GET",assoc_get_method,2));
  fd_defn(m,fd_make_cprim3("FD:ASSOC-ADD",assoc_add_method,3));
  fd_defn(m,fd_make_cprim3("FD:ASSOC-DROP",assoc_drop_method,3));
  fd_defn(m,fd_make_cprim2("FD:CAR-GET",car_get_method,2));
  fd_defn(m,fd_make_cprim2("FD:KLEENE-GET",kleene_get_method,2));
#if 0
  fd_defn(m,fd_make_cprim2("FD:IX-GET",ix_get_method,2));
  fd_defn(m,fd_make_cprim3("FD:IX-TEST",ix_test_method,3));
  fd_defn(m,fd_make_cprim3("FD:IX-ADD",ix_add_method,3));
  fd_defn(m,fd_make_cprim3("FD:IX-DROP",ix_drop_method,3));
#endif
  fd_defn(m,fd_make_cprim2("FD:PAIRED-GET",paired_get_method,2));
  fd_defn(m,fd_make_cprim3("FD:PAIRED-TEST",paired_test_method,3));
  fd_defn(m,fd_make_cprim3("FD:PAIRED-DROP",paired_drop_method,3));
  fd_defn(m,fd_make_cprim3("FD:ADD",lisp_add,3));
  fd_defn(m,fd_make_cprim3("FD:DROP",lisp_drop,3));
  
  fd_defn(m,fd_make_cprim3("FD:CLEAR-IMPLIES",clear_implies_effect,3));

}


/* The CVS log for this file
   $Log: methods.c,v $
   Revision 1.29  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.28  2005/10/31 00:27:33  haase
   Fixed multi slot implementation

   Revision 1.27  2005/10/25 18:32:11  haase
   Made it possible to handle multi slots which include theirselves (their stored values).

   Revision 1.26  2005/08/28 00:38:40  haase
   Added fd:add fd:drop and fd:clear-implies effects

   Revision 1.25  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.24  2005/08/02 22:51:54  haase
   Moved inference methods to use hashsets rather than hashtables

   Revision 1.23  2005/08/02 22:34:12  haase
   Fixed error passing in tree walking methods

   Revision 1.22  2005/07/17 21:15:19  haase
   Fixes to inheritance methods to avoid recursive (and failing) slot accesses

   Revision 1.21  2005/06/01 13:07:55  haase
   Fixes for less forgiving compilers

   Revision 1.20  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.19  2005/04/10 01:11:52  haase
   Don't bother increfing the static method table

   Revision 1.18  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.17  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.16  2005/02/26 22:31:41  haase
   Remodularized choice and oid add into xtables.c

   Revision 1.15  2005/02/26 21:39:25  haase
   Made fd_oid_get take a default argument

   Revision 1.14  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
