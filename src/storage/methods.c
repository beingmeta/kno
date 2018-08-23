/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/storage.h"
#include "framerd/apply.h"
#include "framerd/methods.h"

static lispval frame_symbol, slot_symbol, value_symbol;
static lispval through_slot, derive_slot, inverse_slot;
static lispval multi_slot, multi_primary_slot, key_slot;
static lispval closure_of_slot, index_slot;

static struct FD_HASHTABLE method_table; lispval fd_method_table;

/* Walk proc */

static int keep_walking(struct FD_HASHSET *seen,lispval node,lispval slotids,
                        fd_tree_walkfn walk,void *data)
{
  if (fd_hashset_get(seen,node))
    return 1;
  else {
    int retval = 0;
    if (fd_hashset_mod(seen,node,1)<0)
      return -1;
    if ((walk == NULL) || ((retval = walk(node,data))>0)) {
      DO_CHOICES(slotid,slotids) {
        lispval values = fd_frame_get(node,slotid);
        if (FD_ABORTP(values))
          return fd_interr(values);
        else {
          int retval = 0;
          DO_CHOICES(v,values)
            if (!(OIDP(v))) {}
            else if (fd_hashset_get(seen,v)) {}
            else if ((retval = keep_walking(seen,v,slotids,walk,data))>0) {}
            else {
              fd_decref(values);
              return retval;}
          fd_decref(values);}}
      return 1;}
    else return retval;}
}

FD_EXPORT int
fd_walk_tree(lispval roots,lispval slotids,fd_tree_walkfn walk,void *data)
{
  struct FD_HASHSET ht; int retval = 0;
  memset(&ht,0,sizeof(ht));
  fd_init_hashset(&ht,1024,FD_STACK_CONS);
  {DO_CHOICES(root,roots)
      if ((retval = keep_walking(&ht,root,slotids,walk,data))<=0) {
        fd_recycle_hashset(&ht);
        return retval;}}
  fd_recycle_hashset(&ht);
  return retval;
}

FD_EXPORT int
fd_collect_tree(struct FD_HASHSET *h,
                lispval roots,lispval slotids)
{
  DO_CHOICES(root,roots)
    if (keep_walking(h,root,slotids,NULL,NULL)<0)
      return -1;
  return 1;
}

/* Get bottom and Get top */

FD_EXPORT lispval fd_get_basis(lispval collection,lispval lattice)
{
  struct FD_HASHSET ht;
  lispval root = EMPTY, result = EMPTY;
  memset(&ht,0,sizeof(ht));
  fd_init_hashset(&ht,1024,FD_STACK_CONS);
  {DO_CHOICES(node,collection) {
      DO_CHOICES(slotid,lattice) {
        lispval v = fd_frame_get(node,slotid);
        if (FD_ABORTP(v)) {
          fd_decref(root);
          fd_recycle_hashset(&ht);
          return v;}
        else {CHOICE_ADD(root,v);}}}}
  fd_collect_tree(&ht,root,lattice);
  {DO_CHOICES(node,collection)
      if (fd_hashset_get(&ht,node)) {}
      else {
        fd_incref(node);
        CHOICE_ADD(result,node);}}
  fd_decref(root);
  fd_recycle_hashset(&ht);
  return result;
}

/* Inference procedures */

static int inherit_inferred_values_fn(lispval node,void *data)
{
  struct FD_IVSTRUCT *ivs = (struct FD_IVSTRUCT *)data;
  lispval values = EMPTY;
  DO_CHOICES(slotid,ivs->slotids) {
    lispval v = fd_frame_get(node,slotid);
    if (FD_ABORTP(v)) {
      fd_decref(values);
      return fd_interr(v);}
    else {CHOICE_ADD(values,v);}}
  CHOICE_ADD(ivs->result,values);
  return 1;
}

static int inherit_values_fn(lispval node,void *data)
{
  struct FD_IVSTRUCT *ivs = (struct FD_IVSTRUCT *)data;
  lispval values = EMPTY;
  DO_CHOICES(slotid,ivs->slotids) {
    lispval v = ((OIDP(node)) ? (fd_oid_get(node,slotid,EMPTY)) :
                 (fd_get(node,slotid,EMPTY)));
    if (FD_ABORTP(v)) {
      fd_decref(values);
      return fd_interr(v);}
    else {CHOICE_ADD(values,v);}}
  CHOICE_ADD(ivs->result,values);
  return 1;
}

FD_EXPORT
/* fd_inherit_values:
   Arguments: a frame and two slotids
   Returns: a lispval pointer
   Searches for a value for the first slotid through the lattice
   defined by the second slotid. */
lispval fd_inherit_values(lispval root,lispval slotid,lispval through)
{
  struct FD_IVSTRUCT ivs;
  ivs.result = EMPTY; ivs.slotids = slotid;
  if (fd_walk_tree(root,through,inherit_values_fn,&ivs)<0) {
    fd_decref(ivs.result);
    return FD_ERROR;}
  else return fd_simplify_choice(ivs.result);
}

FD_EXPORT
/* fd_inherit_values:
   Arguments: a frame and two slotids
   Returns: a lispval pointer
   Searches for a value for the first slotid through the lattice
   defined by the second slotid. */
lispval fd_inherit_inferred_values(lispval root,lispval slotid,lispval through)
{
  struct FD_IVSTRUCT ivs;
  ivs.result = EMPTY; ivs.slotids = slotid;
  if (fd_walk_tree(root,through,inherit_inferred_values_fn,&ivs)<0) {
    fd_decref(ivs.result);
    return FD_ERROR;}
  else return fd_simplify_choice(ivs.result);
}

static int inherits_inferred_valuesp_fn(lispval node,void *data)
{
  struct FD_IVPSTRUCT *ivps = (struct FD_IVPSTRUCT *)data;
  DO_CHOICES(slotid,ivps->slotids) {
    int result = fd_test(node,slotid,ivps->value);
    if (result<0) return -1;
    else if (result) {
      ivps->result = 1; return 0;}}
  return 1;
}

static int inherits_valuesp_fn(lispval node,void *data)
{
  struct FD_IVPSTRUCT *ivps = (struct FD_IVPSTRUCT *)data;
  DO_CHOICES(slotid,ivps->slotids) {
    int testval=
      ((OIDP(node)) ?
       (fd_oid_test(node,slotid,ivps->value)) :
       (fd_test(node,slotid,ivps->value)));
    if (testval<0) return -1;
    else if (testval) {
      ivps->result = 1; return 0;}}
  return 1;
}

FD_EXPORT
/* fd_inherits_valuep:
   Arguments: a frame, two slotids, and a value
   Returns: 1 or 0
   Returns 1 if the value can be inherited for the first slotid
   going through the lattice defined by the second slotid. */
int fd_inherits_valuep
(lispval root,lispval slotid,lispval through,lispval value)
{
  struct FD_IVPSTRUCT ivps;
  ivps.value = value; ivps.slotids = slotid; ivps.result = 0;
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
(lispval root,lispval slotid,lispval through,lispval value)
{
  struct FD_IVPSTRUCT ivps;
  ivps.value = value; ivps.slotids = slotid; ivps.result = 0;
  if (fd_walk_tree(root,through,inherits_inferred_valuesp_fn,&ivps)<0)
    return -1;
  else return ivps.result;
}

static int pathp_fn(lispval node,void *data)
{
  struct FD_PATHPSTRUCT *ps = (struct FD_PATHPSTRUCT *)data;
  if (node == ps->target) {
    ps->result = 1; return 0;}
  else return 1;
}

FD_EXPORT
/* fd_pathp:
   Arguments: a frame, a slotid, and a frame
   Returns: 1 or 0
   Returns 1 if there is a path through slotid between the two frames */
int fd_pathp(lispval root,lispval slotid,lispval to)
{
  struct FD_PATHPSTRUCT pathstruct;
  pathstruct.target = to; pathstruct.result = 0;
  if (fd_walk_tree(root,slotid,pathp_fn,&pathstruct)<0)
    return -1;
  else return pathstruct.result;
}

/* Methods */

static lispval inherited_get_method(lispval root,lispval slotid)
{
  lispval result, through, baseslot
    =fd_oid_get(slotid,slot_symbol,slotid);
  if (FD_ABORTP(baseslot)) return  baseslot;
  else through = fd_oid_get(slotid,through_slot,EMPTY);
  if (FD_ABORTP(through)) return through;
  else if (FD_EQ(baseslot,slotid))
    result = fd_inherit_values(root,baseslot,through);
  else result = fd_inherit_inferred_values(root,baseslot,through);
  fd_decref(through); fd_decref(baseslot);
  return result;
}

static lispval multi_get_method(lispval root,lispval mslotid)
{
  lispval slotids = fd_oid_get(mslotid,multi_slot,EMPTY);
  if (FD_ABORTP(slotids)) return slotids;
  else {
    lispval answer = EMPTY;
    DO_CHOICES(slotid,slotids)
      if (PAIRP(slotid)) {
        lispval v = fd_oid_get(root,FD_CAR(slotid),EMPTY);
        CHOICE_ADD(answer,v);}
      else {
        lispval v=
          ((slotid == mslotid)?
           (fd_oid_get(root,slotid,EMPTY)):
           (fd_frame_get(root,slotid)));
        CHOICE_ADD(answer,v);}
    fd_decref(slotids);
    return answer;}
}

static lispval multi_test_method(lispval root,lispval mslotid,lispval value)
{
  lispval slotids = fd_oid_get(mslotid,multi_slot,EMPTY);
  if (FD_ABORTP(slotids)) return slotids;
  else {
    int answer = 0;
    DO_CHOICES(slotid,slotids)
      if (PAIRP(slotid)) {
        if (fd_test(root,FD_CAR(slotid),value)) {
          answer = 1; break;}}
      else {
        if (mslotid == slotid) {
          if (fd_oid_test(root,slotid,value)) {answer = 1; break;}}
        else if (fd_frame_test(root,slotid,value)) {answer = 1; break;}}
    fd_decref(slotids);
    if (answer) return FD_TRUE; else return FD_FALSE;}
}

static lispval multi_add_method(lispval root,lispval slotid,lispval value)
{
  lispval primary_slot = fd_oid_get(slotid,multi_primary_slot,EMPTY);
  if (FD_ABORTP(primary_slot)) return primary_slot;
  else if (EMPTYP(primary_slot)) {
    lispval slotids = fd_oid_get(slotid,multi_slot,EMPTY);
    if (FD_ABORTP(slotids)) return slotids;
    else {
      DO_CHOICES(subslotid,slotids) {
        lispval v = fd_frame_get(root,subslotid);
        if (FD_ABORTP(v)) return v;
        else if (!(EMPTYP(v)))
          if (fd_frame_add(root,subslotid,value)<0)
            return FD_ERROR;
        fd_decref(v);}
      fd_decref(slotids);}}
  else if (primary_slot == slotid) {
    if (fd_add(root,primary_slot,value)<0) {
      fd_decref(primary_slot);
      return FD_ERROR;}}
  else if (fd_frame_add(root,primary_slot,value)<0) {
    fd_decref(primary_slot);
    return FD_ERROR;}
  fd_decref(primary_slot);
  return VOID;
}

static lispval multi_drop_method(lispval root,lispval slotid,lispval value)
{
  lispval slotids = fd_oid_get(slotid,multi_slot,EMPTY);
  if (FD_ABORTP(slotids)) return slotids;
  else {
    lispval primary_slot = fd_oid_get(slotid,multi_primary_slot,EMPTY);
    if (FD_ABORTP(primary_slot)) return primary_slot;
    else if ((SYMBOLP(primary_slot)) || (OIDP(primary_slot))) {
      if (primary_slot == slotid) {
        if (fd_drop(root,primary_slot,value)<0) {
          fd_decref(primary_slot);
          return FD_ERROR;}}
      else if (fd_frame_drop(root,primary_slot,value)<0) {
        fd_decref(primary_slot);
        return FD_ERROR;}}
    {DO_CHOICES(subslotid,slotids) {
        int probe = fd_frame_test(root,subslotid,value);
        if (probe<0) return FD_ERROR;
        else if (probe)
          if (fd_frame_drop(root,subslotid,value)<0)
            return FD_ERROR;}}
    fd_decref(slotids);
    return VOID;}
}

static inline lispval getter(lispval f,lispval s)
{
  if (OIDP(f)) return fd_frame_get(f,s);
  else if (HASHTABLEP(f))
    return fd_hashtable_get(FD_XHASHTABLE(f),s,EMPTY);
  else if (HASHTABLEP(s))
    return fd_hashtable_get(FD_XHASHTABLE(s),f,EMPTY);
  else return EMPTY;
}

static int kleene_get_helper
(struct FD_HASHSET *ht,lispval frames,lispval slotids)
{
  DO_CHOICES(frame,frames) {
    int probe = fd_hashset_get(ht,frame);
    if (probe<0) return probe;
    else if (probe) {}
    else {
      if (fd_hashset_mod(ht,frame,1)<0)
        return -1;
      else {
        DO_CHOICES(slotid,slotids) {
          lispval values = getter(frame,slotid);
          if (FD_ABORTP(values)) return fd_interr(values);
          if (kleene_get_helper(ht,values,slotids)<0) {
            fd_decref(values);
            return -1;}
          else fd_decref(values);}}}}
  return 1;
}

static lispval kleene_star_get_method(lispval root,lispval slotid)
{
  lispval slotids = fd_frame_get(slotid,closure_of_slot), results;
  if (FD_ABORTP(slotids)) return slotids;
  else {
    struct FD_HASHSET hs; memset(&hs,0,sizeof(hs));
    fd_init_hashset(&hs,1024,FD_STACK_CONS);
    if (kleene_get_helper(&hs,root,slotids)<0) {
      fd_recycle_hashset(&hs);
      fd_decref(slotids);
      return FD_ERROR;}
    else {
      results = fd_hashset_elts(&hs,1);
      fd_decref(slotids);
      return results;}}
}

static lispval kleene_plus_get_method(lispval root,lispval slotid)
{
  lispval slotids = fd_frame_get(slotid,closure_of_slot), results;
  if (FD_ABORTP(slotids)) return slotids;
  else {
    struct FD_HASHSET hs; memset(&hs,0,sizeof(hs));
    fd_init_hashset(&hs,1024,FD_STACK_CONS);
    if (kleene_get_helper(&hs,root,slotids)<0) {
      fd_recycle_hashset(&hs);
      fd_decref(slotids);
      return FD_ERROR;}
    else {
      DO_CHOICES(r,root) {fd_hashset_drop(&hs,r);}
      results = fd_hashset_elts(&hs,1);
      fd_decref(slotids);
      return results;}}
}

static lispval inherited_test_method(lispval root,lispval slotid,lispval value)
{
  lispval baseslot = fd_oid_get(slotid,slot_symbol,slotid);
  lispval through = fd_oid_get(slotid,through_slot,EMPTY);
  int answer = 0;
  if (FD_ABORTP(baseslot)) {
    fd_decref(through); return baseslot;}
  else if (FD_ABORTP(through)) {
    fd_decref(through); return baseslot;}
  else if (FD_EQ(baseslot,slotid))
    answer = fd_inherits_valuep(root,baseslot,through,value);
  else answer = fd_inherits_inferred_valuep(root,baseslot,through,value);
  fd_decref(through); fd_decref(baseslot);
  if (answer<0) return FD_ERROR;
  else if (answer) return FD_TRUE;
  else return FD_FALSE;
}

static lispval inverse_getbase_method(lispval root,lispval slotid)
{
  lispval answer, inv_slots, others;
  answer = fd_oid_get(root,slotid,EMPTY);
  if (FD_ABORTP(answer)) return answer;
  else inv_slots = fd_oid_get(slotid,inverse_slot,EMPTY);
  if (FD_ABORTP(inv_slots)) {
    fd_decref(answer);
    return inv_slots;}
  else {
    lispval root_as_list = fd_conspair(root,NIL);
    others = fd_bgfind(inv_slots,root_as_list,VOID);
    fd_decref(root_as_list);}
  if (FD_ABORTP(others)) {
    fd_decref(inv_slots); fd_decref(answer);
    return others;}
  else {
    CHOICE_ADD(answer,others);
    fd_decref(inv_slots);
    return answer;}
}

static lispval inverse_get_method(lispval root,lispval slotid)
{
  lispval answer, inv_slots, others;
  answer = fd_oid_get(root,slotid,EMPTY);
  if (FD_ABORTP(answer)) return answer;
  else inv_slots = fd_oid_get(slotid,inverse_slot,EMPTY);
  if (FD_ABORTP(inv_slots)) {
    fd_decref(answer);
    return inv_slots;}
  else others = fd_bgfind(inv_slots,root,VOID);
  if (FD_ABORTP(others)) {
    fd_decref(inv_slots); fd_decref(answer);
    return others;}
  else {
    CHOICE_ADD(answer,others);
    fd_decref(inv_slots);
    return answer;}
}

static lispval inverse_test_method(lispval root,lispval slotid,lispval value)
{
  int direct_test = fd_oid_test(root,slotid,value);
  if (direct_test<0) return FD_ERROR;
  else if (direct_test) return (FD_TRUE);
  else if (!(OIDP(value))) return FD_FALSE;
  else {
    lispval inv_slots = fd_oid_get(slotid,inverse_slot,EMPTY);
    if (FD_ABORTP(inv_slots)) return inv_slots;
    else {
      int found = 0;
      DO_CHOICES(inv_slot,inv_slots) {
        int testval = fd_frame_test(value,inv_slot,root);
        if (testval) {found = testval; break;}}
      fd_decref(inv_slots);
      if (found<0) return FD_ERROR;
      else if (found) return (FD_TRUE);
      else return (FD_FALSE);}}
}

static lispval assoc_get_method(lispval f,lispval slotid)
{
  lispval answers = EMPTY, through, key;
  through = fd_frame_get(slotid,through_slot);
  if (FD_ABORTP(through)) return through;
  else key = fd_frame_get(slotid,key_slot);
  if (FD_ABORTP(key)) {
    fd_decref(through); return key;}
  else {
    DO_CHOICES(th_slot,through) {
      lispval entries = fd_frame_get(f,th_slot);
      if (FD_ABORTP(entries)) {
        fd_decref(answers); fd_decref(through); fd_decref(key);
        return entries;}
      else {
        int ambigkey = CHOICEP(key);
        DO_CHOICES(e,entries)
          if (PAIRP(e)) {
            lispval car = FD_CAR(e), cdr = FD_CDR(e);
            if (FD_EQ(car,key)) {
              fd_incref(cdr);
              CHOICE_ADD(answers,cdr);}
            else if ((!ambigkey) && (!(CHOICEP(car)))) {
              if (LISP_EQUAL(car,key)) {
                fd_incref(cdr);
                CHOICE_ADD(answers,cdr);}}
            else if (fd_overlapp(car,key)) {
              fd_incref(cdr);
              CHOICE_ADD(answers,cdr);}
            else {}}
          else if ( (FD_TABLEP(e)) && (fd_test(e,key,FD_VOID)) ) {
            lispval v = fd_get(e,key,FD_VOID);
            CHOICE_ADD(answers,v);}
          else {}}
      fd_decref(entries);}
    fd_decref(through); fd_decref(key);
    return answers;}
}

static lispval assoc_test_method(lispval f,lispval slotid,lispval values)
{
  lispval answers = EMPTY, through, key;
  through = fd_frame_get(slotid,through_slot);
  if (FD_ABORTP(through)) return through;
  else key = fd_frame_get(slotid,key_slot);
  if (FD_ABORTP(key)) {
    fd_decref(through); return key;}
  else {
    DO_CHOICES(th_slot,through) {
      lispval entries = fd_frame_get(f,th_slot);
      if (FD_ABORTP(entries)) {
        fd_decref(answers); fd_decref(through); fd_decref(key);
        return entries;}
      else {
        int ambigkey = CHOICEP(key), ambigval = CHOICEP(values);
        DO_CHOICES(e,entries)
          if (PAIRP(e)) {
            lispval car = FD_CAR(e), cdr = FD_CDR(e);
            if (((ambigkey|(CHOICEP(car))) ?
                 (fd_overlapp(car,key)) :
                 (FD_EQ(car,key))) &&
                ((VOIDP(values)) ||
                 ((ambigval|(CHOICEP(cdr))) ? (fd_overlapp(cdr,values)) :
                  (FD_EQUAL(cdr,values))))) {
              fd_decref(entries); fd_decref(key); fd_decref(through);
              FD_STOP_DO_CHOICES;
              return FD_TRUE;}}
          else if ( (FD_TABLEP(e)) && (fd_test(e,th_slot,values)) ) {
            FD_STOP_DO_CHOICES;
            return FD_TRUE;}
          else {}}
      fd_decref(entries);}
    fd_decref(through); fd_decref(key);
    return FD_FALSE;}
}

static lispval assoc_add_method(lispval f,lispval slotid,lispval value)
{
  lispval through, key;
  through = fd_frame_get(slotid,through_slot);
  if (FD_ABORTP(through))
    return through;
  else key = fd_frame_get(slotid,key_slot);
  if (FD_ABORTP(key)) {
    fd_decref(through);
    return key;}
  else if (FD_SLOTMAPP(through)) {
    fd_add(through,key,value);
    fd_oid_store(f,through_slot,through);
    return VOID;}
  else {
    lispval pair = fd_conspair(fd_incref(key),fd_incref(value));
    fd_oid_add(f,through,pair);
    fd_decref(pair);
    return VOID;}
}

static lispval assoc_drop_method(lispval f,lispval slotid,lispval value)
{
  lispval through, key;
  through = fd_frame_get(slotid,through_slot);
  if (FD_ABORTP(through)) return through;
  else key = fd_frame_get(slotid,key_slot);
  if (FD_ABORTP(key)) {
    fd_decref(through); return key;}
  else if (FD_SLOTMAPP(through)) {
    fd_drop(through,key,value);
    fd_store(f,through_slot,through);
    return VOID;}
  else {
    lispval pair = fd_conspair(fd_incref(key),fd_incref(value));
    fd_oid_drop(f,through,pair);
    fd_decref(pair);
    return VOID;}
}

static lispval car_get_method(lispval f,lispval slotid)
{
  lispval result = (EMPTY);
  lispval values = fd_oid_get(f,slotid,EMPTY);
  if (FD_ABORTP(values))
    return values;
  else {
    DO_CHOICES(value,values)
      if (PAIRP(value)) {
        lispval car = FD_CAR(value);
        fd_incref(car);
        CHOICE_ADD(result,car);}
      else {
        fd_decref(result);
        return fd_type_error("pair","car_get_method",value);}}
  fd_decref(values);
  return result;
}

static lispval paired_get_method(lispval f,lispval slotid)
{
  lispval result = (EMPTY);
  lispval values = fd_oid_get(f,slotid,EMPTY);
  if (FD_ABORTP(values)) return values;
  else {
    DO_CHOICES(value,values)
      if (PAIRP(value)) {
        lispval car = FD_CAR(value);
        fd_incref(car);
        CHOICE_ADD(result,car);}
      else {
        fd_incref(value);
        CHOICE_ADD(result,value);}}
  fd_decref(values);
  return result;
}

static lispval paired_test_method(lispval f,lispval slotid,lispval v)
{
  int found = 0;
  lispval values = fd_oid_get(f,slotid,EMPTY);
  if (FD_ABORTP(values)) return values;
  else {
    DO_CHOICES(value,values) {
      if (PAIRP(value)) {
        if (FD_EQUAL(FD_CAR(value),v)) {found = 1; break;}}
      else if (FD_EQUAL(value,v)) {found = 1; break;}}
    fd_decref(values);
    if (found) return FD_TRUE;
    else return FD_FALSE;}
}

static lispval paired_drop_method(lispval f,lispval slotid,lispval v)
{
  if (PAIRP(v)) {
    if (fd_oid_drop(f,slotid,v)<0)
      return FD_ERROR;
    else return VOID;}
  else {
    int found = 0;
    lispval values = fd_oid_get(f,slotid,EMPTY);
    if (FD_ABORTP(values)) return values;
    else {
      DO_CHOICES(value,values) {
        if (PAIRP(value)) {
          if (FD_EQUAL(FD_CAR(value),v)) {found = 1; break;}}
        else if (FD_EQUAL(value,v)) {found = 1; break;}}
      if (found) {
        lispval new_values = EMPTY;
        DO_CHOICES(value,values) {
          if (PAIRP(value))
            if (FD_EQUAL(FD_CAR(value),v)) {}
            else {
              fd_incref(value);
              CHOICE_ADD(new_values,value);}
          else if (FD_EQUAL(value,v)) {}
          else {
            fd_incref(value);
            CHOICE_ADD(new_values,value);}}
        if (fd_store(f,slotid,new_values)<0) {
          fd_decref(values); fd_decref(new_values);
          return FD_ERROR;}
        fd_decref(values); fd_decref(new_values);
        return VOID;}
      else {
        fd_decref(values); return VOID;}}}
}

static lispval implies_slot;

static lispval clear_implies_effect(lispval f,lispval slotid,lispval v)
{
  lispval implies = fd_get(slotid,implies_slot,EMPTY);
  fd_clear_slotcache_entry(slotid,f);
  {DO_CHOICES(imply,implies) clear_implies_effect(f,imply,v);}
  return EMPTY;
}

static void init_symbols()
{
  frame_symbol = fd_intern("FRAME");
  slot_symbol = fd_intern("SLOTID");
  value_symbol = fd_intern("VALUE");
  through_slot = fd_intern("THROUGH");
  key_slot = fd_intern("KEY");
  derive_slot = fd_intern("DERIVATION");
  inverse_slot = fd_intern("INVERSE");
  closure_of_slot = fd_intern("CLOSURE-OF");
  multi_slot = fd_intern("SLOTS");
  multi_primary_slot = fd_intern("PRIMARY-SLOT");
  index_slot = fd_intern("INDEX");
  implies_slot = fd_intern("IMPLIES");
}

static lispval lisp_add(lispval f,lispval s,lispval v)
{
  if (fd_add(f,s,v)<0) return FD_ERROR;
  else return VOID;
}
static lispval lisp_drop(lispval f,lispval s,lispval v)
{
  if (fd_drop(f,s,v)<0) return FD_ERROR;
  else return VOID;
}

FD_EXPORT void fd_init_methods_c()
{
  lispval m;

  u8_register_source_file(_FILEINFO);

  init_symbols();

  FD_INIT_STATIC_CONS(&method_table,fd_hashtable_type);
  fd_make_hashtable(&method_table,67);
  m = fd_method_table = ((lispval)(&method_table));

  fd_defn(m,fd_make_cprim2("FD:INHERITED-GET",inherited_get_method,2));
  fd_defn(m,fd_make_cprim3("FD:INHERITED-TEST",inherited_test_method,3));
  fd_defn(m,fd_make_cprim2("FD:MULTI-GET",multi_get_method,2));
  fd_defn(m,fd_make_cprim3("FD:MULTI-TEST",multi_test_method,3));
  fd_defn(m,fd_make_cprim3("FD:MULTI-ADD",multi_add_method,3));
  fd_defn(m,fd_make_cprim3("FD:MULTI-DROP",multi_drop_method,3));
  {
    lispval invget = fd_make_cprim2("FD:INVERSE-GET",inverse_get_method,2);
    lispval invgetbase = fd_make_cprim2("FD:INVERSE-GETBASE",inverse_getbase_method,2);
    lispval invtest = fd_make_cprim3("FD:INVERSE-TEST",inverse_test_method,3);
    fd_defn(m,invget); fd_defn(m,invtest); fd_defn(m,invgetbase);
    fd_store(m,fd_intern("FD:INV-GET"),invget);
    fd_store(m,fd_intern("FD:INV-GETBASE"),invgetbase);
    fd_store(m,fd_intern("FD:INV-TEST"),invtest);}

  fd_defn(m,fd_make_cprim2("FD:ASSOC-GET",assoc_get_method,2));
  fd_defn(m,fd_make_cprim3("FD:ASSOC-TEST",assoc_test_method,2));
  fd_defn(m,fd_make_cprim3("FD:ASSOC-ADD",assoc_add_method,3));
  fd_defn(m,fd_make_cprim3("FD:ASSOC-DROP",assoc_drop_method,3));
  fd_defn(m,fd_make_cprim2("FD:CAR-GET",car_get_method,2));
  fd_defn(m,fd_make_cprim2("FD:KLEENE-GET",kleene_plus_get_method,2));
  fd_defn(m,fd_make_cprim2("FD:KLEENE+-GET",kleene_plus_get_method,2));
  fd_defn(m,fd_make_cprim2("FD:KLEENE*-GET",kleene_star_get_method,2));
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

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
