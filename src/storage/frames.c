/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_POOLS 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_IPEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/apply.h"

#include <stdarg.h>

static fd_exception OddFindFramesArgs=_("Odd number of args to find frames");
static fd_exception CorruptOverlay=_("Corrupt overlay table");

static struct FD_HASHTABLE slot_caches, test_caches;
static fdtype get_methods, compute_methods, test_methods;
static fdtype add_effects, drop_effects;

static fdtype slot_overlay = FD_VOID, index_overlay = FD_VOID;

static u8_mutex slotcache_lock;

/* Frame overlays */

#if FD_USE_TLS
u8_tld_key _fd_inhibit_overlay_key;
FD_EXPORT void fd_inhibit_overlays(int flag)
{
  fd_wideint iflag = flag;
  u8_tld_set(_fd_inhibit_overlay_key,(void *)iflag);
}
#else
__thread int fd_inhibit_overlay = 0;
FD_EXPORT void fd_inhibit_overlays(int flag) { fd_inhibit_overlay = flag; }
#endif

static fdtype _overlay_get
  (fdtype overlay,fdtype values,fdtype car,fdtype cdr)
{
  if ((FD_VOIDP(overlay)) || (fd_inhibit_overlay))
    return values;
  else if (FD_HASHTABLEP(overlay)) {
    fdtype tmp_key;
    struct FD_PAIR p;
    struct FD_HASHTABLE *h = (fd_hashtable)overlay;
    FD_INIT_STATIC_CONS(&p,fd_pair_type);
    tmp_key = (fdtype)&p;
    p.car = car; p.cdr = cdr;
    if (fd_hashtable_probe(h,tmp_key)) {
      fdtype v = fd_hashtable_get(h,tmp_key,FD_VOID);
      if ((FD_VOIDP(v)) || (FD_EMPTY_CHOICEP(v)))
        return values;
      else if (FD_PAIRP(v)) {
        fdtype combined = FD_EMPTY_CHOICE, adds = FD_CAR(v), drops = FD_CDR(v);
        fd_incref(values); fd_incref(adds);
        FD_ADD_TO_CHOICE(combined,values);
        FD_ADD_TO_CHOICE(combined,adds);
        if (FD_EMPTY_CHOICEP(drops)) {
          fd_decref(v);
          return fd_simplify_choice(combined);}
        else {
          fdtype results = fd_difference(combined,drops);
          fd_decref(combined); fd_decref(v);
          return fd_simplify_choice(results);}}
      else if ((FD_VECTORP(v)) && (FD_VECTOR_LENGTH(v)==1)) {
        fdtype new_values = FD_VECTOR_REF(v,0);
        fd_decref(values); fd_incref(new_values); fd_decref(v);
        return new_values;}
      else {
        fd_decref(v);
        return fd_err(CorruptOverlay,"overlay_get",NULL,overlay);}}
    else return values;}
  else if (FD_INDEXP(overlay)) {
    fdtype tmp_key, mods;
    struct FD_PAIR p;
    fd_index ix = fd_indexptr(overlay);
    FD_INIT_STATIC_CONS(&p,fd_pair_type);
    p.car = car; p.cdr = cdr;
    tmp_key = (fdtype)&p;
    mods = fd_index_get(ix,tmp_key);
    if ((FD_VOIDP(mods)) || (FD_EMPTY_CHOICEP(mods)))
      return values;
    else if (FD_PAIRP(mods)) {
      fdtype combined = FD_EMPTY_CHOICE, adds = FD_CAR(mods), drops = FD_CDR(mods);
      fd_incref(values); fd_incref(adds);
      FD_ADD_TO_CHOICE(combined,values);
      FD_ADD_TO_CHOICE(combined,adds);
      if (FD_EMPTY_CHOICEP(drops)) {
        fd_decref(mods);
        return fd_simplify_choice(combined);}
      else {
        fdtype results = fd_difference(combined,drops);
        fd_decref(combined); fd_decref(mods);
        return fd_simplify_choice(results);}}
    else if (FD_VECTORP(mods)) {
      fdtype new_values = FD_VECTOR_REF(mods,0);
      fd_decref(values); fd_incref(new_values); fd_decref(mods);
      return new_values;}
    else {
      fd_decref(mods);
      return fd_err(CorruptOverlay,"overlay_get",NULL,overlay);}}
  else return values;
}

static fdtype overlay_get(fdtype overlay,fdtype values,fdtype car,fdtype cdr)
{
  if (FD_EXPECT_TRUE(FD_VOIDP(overlay))) return values;
  else return _overlay_get(overlay,values,car,cdr);
}

static int _overlay_test
  (fdtype overlay,int dflt,fdtype frame,fdtype slotid,fdtype value)
{
  if (FD_VOIDP(overlay)) return dflt;
  else if (fd_inhibit_overlay) return dflt;
  else if (!(FD_OIDP(frame)))  return dflt;
  else if (FD_HASHTABLEP(overlay)) {
    fdtype tmp_key;
    struct FD_PAIR p;
    FD_INIT_STATIC_CONS(&p,fd_pair_type);
    tmp_key = (fdtype)&p;
    p.car = slotid; p.cdr = frame;
    if (fd_hashtable_probe((fd_hashtable)slot_overlay,tmp_key)) {
      int retval;
      fdtype v = fd_hashtable_get((fd_hashtable)slot_overlay,tmp_key,FD_VOID);
      if ((FD_VOIDP(v)) || (FD_EMPTY_CHOICEP(v))) return dflt;
      else if ((FD_VECTORP(v)) && (FD_VECTOR_LENGTH(v)==1))
        retval = fd_overlapp(FD_VECTOR_REF(v,0),value);
      else if (!(FD_PAIRP(v))) {
        fd_seterr(CorruptOverlay,"overlay_get",NULL,overlay);
        retval = -1;}
      else if (fd_overlapp(value,FD_CAR(v)))
        retval = 1;
      else if (fd_overlapp(value,FD_CDR(v)))
        retval = 0;
      else retval = dflt;
      fd_decref(v);
      return retval;}
    else return dflt;}
  else if (FD_INDEXP(overlay)) {
    fdtype tmp_key, mods;
    struct FD_PAIR p;
    fd_index ix = fd_indexptr(overlay);
    FD_INIT_STATIC_CONS(&p,fd_pair_type);
    p.car = slotid; p.cdr = frame;
    tmp_key = (fdtype)&p;
    mods = fd_index_get(ix,tmp_key);
    if (FD_EMPTY_CHOICEP(mods)) return dflt;
    else if (fd_overlapp(value,FD_CAR(mods))) {
      fd_decref(mods); return 1;}
    else if (fd_overlapp(value,FD_CDR(mods))) {
      fd_decref(mods); return 0;}
    else {
      fd_decref(mods); return dflt;}}
  else return dflt;
}

static int overlay_test
  (fdtype overlay,int dflt,fdtype frame,fdtype slotid,fdtype value)
{
  if (FD_EXPECT_TRUE(FD_VOIDP(overlay))) return dflt;
  else return _overlay_test(overlay,dflt,frame,slotid,value);
}

static fdtype overlay_add
   (fdtype overlay,fdtype frame,fdtype slotid,fdtype value)
{
  int retval = 0;
  fdtype key = fd_conspair(slotid,fd_incref(frame));
  fdtype entry = ((FD_INDEXP(overlay)) ?
                (fd_index_get(fd_indexptr(overlay),key)) :
                (fd_hashtable_get((fd_hashtable)overlay,key,FD_VOID))),
    new_entry;
  if (FD_ABORTP(entry)) {fd_decref(key); return entry;}
  else if ((FD_VOIDP(entry)) || (FD_EMPTY_CHOICEP(entry)))
    new_entry = fd_conspair(fd_incref(value),FD_EMPTY_CHOICE);
  else if (FD_PAIRP(entry)) {
    fdtype adds = fd_make_simple_choice(FD_CAR(entry)), drops = FD_CDR(entry);
    fd_incref(value); FD_ADD_TO_CHOICE(adds,value);
    new_entry = fd_conspair(adds,fd_difference(drops,value));}
  else if (FD_VECTORP(entry)) {
    fdtype values = FD_VECTOR_REF(entry,0), *elts = u8_alloc_n(1,fdtype);
    /* Leak?  This fd_incref wasn't there before, but it looks like it should be. */
    fd_incref(value); FD_ADD_TO_CHOICE(values,value);
    elts[0]=fd_make_simple_choice(values);
    new_entry = fd_init_vector(NULL,1,elts);}
  else {
    fd_seterr(CorruptOverlay,"overlay_add",NULL,entry);
    fd_decref(entry); fd_decref(key);
    return -1;}
  if (FD_HASHTABLEP(overlay))
    retval = fd_hashtable_store((fd_hashtable)overlay,key,new_entry);
  else if (FD_INDEXP(overlay))
    retval = fd_index_store(fd_indexptr(overlay),key,new_entry);
  else {
    fd_seterr(fd_TypeError,"overlay_add",NULL,overlay);
    retval = -1;}
  fd_decref(key); fd_decref(entry); fd_decref(new_entry);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_VOID;
}

static fdtype overlay_drop
   (fdtype overlay,fdtype frame,fdtype slotid,fdtype value)
{
  int retval = 0;
  fdtype key = fd_conspair(slotid,fd_incref(frame));
  fdtype entry = ((FD_INDEXP(overlay)) ?
                (fd_index_get(fd_indexptr(overlay),key)) :
                (fd_hashtable_get((fd_hashtable)overlay,key,FD_VOID))),
    new_entry;
  if (FD_ABORTP(entry)) {fd_decref(key); return entry;}
  else if ((FD_VOIDP(entry)) || (FD_EMPTY_CHOICEP(entry)))
    new_entry = fd_conspair(FD_EMPTY_CHOICE,fd_incref(value));
  else if (FD_PAIRP(entry)) {
    fdtype adds = FD_CAR(entry), drops = fd_make_simple_choice(FD_CDR(entry));
    fd_incref(value); FD_ADD_TO_CHOICE(drops,value);
    new_entry = fd_conspair(fd_difference(adds,value),drops);}
  else if (FD_VECTORP(entry)) {
    fdtype values = FD_VECTOR_REF(entry,0), *elts = u8_alloc_n(1,fdtype);
    elts[0]=fd_difference(values,value);
    new_entry = fd_init_vector(NULL,1,elts);}
  else {
    fd_seterr(CorruptOverlay,"overlay_add",NULL,entry);
    fd_decref(entry); fd_decref(key);
    return -1;}
  if (FD_HASHTABLEP(overlay))
    retval = fd_hashtable_store((fd_hashtable)overlay,key,new_entry);
  else if (FD_INDEXP(overlay))
    retval = fd_index_store(fd_indexptr(overlay),key,new_entry);
  else {
    fd_seterr(fd_TypeError,"overlay_add",NULL,overlay);
    retval = -1;}
  fd_decref(key); fd_decref(entry); fd_decref(new_entry);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_VOID;
}

static fdtype overlay_store
   (fdtype overlay,fdtype frame,fdtype slotid,fdtype value)
{
  fdtype key = fd_conspair(slotid,fd_incref(frame));
  fdtype *data = u8_alloc_n(1,fdtype);
  fdtype entry = fd_init_vector(NULL,1,data);
  data[0]=value; fd_incref(value);
  if (FD_HASHTABLEP(overlay))
    fd_hashtable_store((fd_hashtable)overlay,key,entry);
  else if (FD_INDEXP(overlay)) {
    fd_index ix = fd_indexptr(overlay);
    fd_index_store(ix,key,entry);}
  else {
    fd_decref(entry); fd_decref(key);
    return fd_err(CorruptOverlay,"overlay_store",NULL,overlay);}
  fd_decref(entry); fd_decref(key);
  return FD_VOID;
}

/* External functions */

FD_EXPORT fdtype fd_overlay_get(fdtype frame,fdtype slotid,int index)
{
  if (index)
    if (FD_EXPECT_FALSE(FD_VOIDP(index_overlay)))
      return FD_EMPTY_CHOICE;
    else return overlay_get(index_overlay,FD_EMPTY_CHOICE,slotid,frame);
  else if (FD_EXPECT_FALSE(FD_VOIDP(slot_overlay)))
    return FD_EMPTY_CHOICE;
  else return overlay_get(slot_overlay,FD_EMPTY_CHOICE,slotid,frame);
}

FD_EXPORT fdtype fd_overlay_add
  (fdtype frame,fdtype slotid,fdtype value,int index)
{
  if (index)
    if (FD_EXPECT_FALSE(FD_VOIDP(index_overlay)))
      return fd_err("No active overlay","fd_overlay_drop",NULL,FD_VOID);
    else return overlay_add(index_overlay,value,slotid,frame);
  else if (FD_EXPECT_FALSE(FD_VOIDP(slot_overlay)))
    return fd_err("No active overlay","fd_overlay_drop",NULL,FD_VOID);
  else return overlay_add(slot_overlay,frame,slotid,value);
}

FD_EXPORT fdtype fd_overlay_drop
  (fdtype frame,fdtype slotid,fdtype value,int index)
{
  if (index)
    if (FD_EXPECT_FALSE(FD_VOIDP(index_overlay)))
      return fd_err("No active overlay","fd_overlay_drop",NULL,FD_VOID);
    else return overlay_drop(index_overlay,value,slotid,frame);
  else if (FD_EXPECT_FALSE(FD_VOIDP(slot_overlay)))
    return fd_err("No active overlay","fd_overlay_drop",NULL,FD_VOID);
  else return overlay_drop(slot_overlay,frame,slotid,value);
}

FD_EXPORT fdtype fd_overlay_store
  (fdtype frame,fdtype slotid,fdtype value,int index)
{
  if (index)
    if (FD_EXPECT_FALSE(FD_VOIDP(index_overlay)))
      return fd_err("No active overlay","fd_overlay_drop",NULL,FD_VOID);
    else return overlay_store(index_overlay,value,slotid,frame);
  else if (FD_EXPECT_FALSE(FD_VOIDP(slot_overlay)))
    return fd_err("No active overlay","fd_overlay_drop",NULL,FD_VOID);
  else return overlay_store(slot_overlay,frame,slotid,value);
}

FD_EXPORT int fd_overlayp()
{
  if ((FD_VOIDP(index_overlay)) && (FD_VOIDP(slot_overlay)))
    return 0;
  else return 1;
}

/* Configuration settings for overlays. */

static fdtype overlay_config_get(fdtype ignored,void *vptr)
{
  fdtype *hptr = (fdtype *)vptr;
  if (FD_VOIDP(*hptr)) return FD_FALSE;
  else {
    fdtype table = (fdtype)(*(hptr));
    return fd_incref(table);}
}
static int overlay_config_set(fdtype ignored,fdtype v,void *vptr)
{
  fdtype *hptr = (fdtype *)vptr;
  fdtype current = *hptr, new;
  if ((FD_HASHTABLEP(v)) || (FD_INDEXP(v))) {
    new = v; fd_incref(v);}
  else if (FD_STRINGP(v)) {
    fd_index ix = fd_get_index(FD_STRDATA(v),0,FD_VOID);
    if (ix == NULL) return FD_ERROR_VALUE;
    new = fd_index2lisp(ix);}
  else if (FD_TRUEP(v))
    new = fd_make_hashtable(NULL,1024);
  else if (FD_FALSEP(v))
    new = FD_VOID;
  else return fd_reterr(fd_TypeError,"overlay_config_set",NULL,v);
  *hptr = new; fd_decref(current);
  return 1;
}


/* The operations stack */

/* How it works:

    FramerD inference operations rely on a dynamic *operations
     stack* which serves multipe roles.  The operations stack was
     first introduced to handle the infinite recurrences which
     commonly occur when inference methods refer to one another.  It
     is also used in dependency tracking (described below).

    The key idea is to prune recursive method executions by keeping
     track of the slot values being computed or tested and causing
     recursive calls with identical arguments to fail.  For example,
     suppose we have three interdependent methods for figuring out the
     area, length, and width of a rectangle.  If in the course of if
     we're are executing methods to figure out the area of a square,
     and one of those methods requires the area of the square
     (recursively), we prune that method's execution while allowing
     other methods to continue.  This handles a common case where, for
     example, the area is defined in terms of the height and width and
     the height and width are each defined in terms of the area.
     These natural mutually recursive definitions would get us into
     trouble without something like this method.

    In practice, a method fails by returning either the empty choice (if it is computing a value)
     or 0/false (if it is testing for a value).
    The stack of frame operations is a simple linked list, allocated on the stack and
     stored in a global/thread specific variable.  Each entry records the operation and
     three arguments: a frame, a slotid, and (optionally) a value.  There are other fields
     associated with the dependency mechanism implemented below.
*/

#if FD_USE_TLS
static u8_tld_key opstack_key;
#else
static __thread struct FD_FRAMEOP_STACK *opstack = NULL;
#endif

static struct FD_FRAMEOP_STACK *get_opstack()
{
#if FD_USE_TLS
  return u8_tld_get(opstack_key);
#else
  return opstack;
#endif
}

/* Returns true if the operation specified by the stack entry
   is currently in progress. */
FD_EXPORT int fd_in_progressp(struct FD_FRAMEOP_STACK *op)
{
  struct FD_FRAMEOP_STACK *start = get_opstack(), *ops = start;
  /* We initialize this field here, because we've gotten it anyway. */
  op->next = start;
  while (ops)
    if ((ops->op == op->op) &&
        (ops->frame == op->frame) &&
        (ops->slotid == op->slotid) &&
        (FDTYPE_EQUAL(ops->value,op->value)))
      return 1;
    else ops = ops->next;
  return 0;
}

/* Adds an entry to the frame operation stack. */
FD_EXPORT void fd_push_opstack(struct FD_FRAMEOP_STACK *op)
{
  struct FD_FRAMEOP_STACK *ops = get_opstack();
  op->next = ops;
  op->dependencies = NULL;
  op->n_deps = op->max_deps = 0;
#if (FD_USE_TLS)
  u8_tld_set(opstack_key,op);
#else
  opstack = op;
#endif
}

/* Pops an entry from the frame operation stack. */
FD_EXPORT int fd_pop_opstack(struct FD_FRAMEOP_STACK *op,int normal)
{
  struct FD_FRAMEOP_STACK *ops = get_opstack();
  if (ops != op) u8_raise("Corrupted ops stack","fd_pop_opstack",NULL);
  else if (op->dependencies) {
    if (normal) u8_free(op->dependencies);
    else {
      struct FD_DEPENDENCY_RECORD *scan = op->dependencies;
      struct FD_DEPENDENCY_RECORD *limit = scan+op->n_deps;
      while (scan<limit) {
        fd_decref(scan->frame); fd_decref(scan->slotid); fd_decref(scan->value);
        scan++;}
      u8_free(op->dependencies);}}
#if (FD_USE_TLS)
  u8_tld_set(opstack_key,op->next);
#else
  opstack = op->next;
#endif
  return 1;
}


/* Slot caches */

/* How it works:
     Slot caches record the results of executing slot GET and TEST methods
      in order to speed up subsequent repetitions of the same operation.  The
      validity of the slot caches are maintained by the dependency mechanisms
      described below.  This comment describes the structure of the slot caches
      themselves.
     There are two global tables, one for slot caches (which cache whole values)
      and one for test caches (which cache tests for particular values).  These
      are different because many methods may depend on the presence or absence of
      one particular value.
     Each of these global tables map slotids into tables which map frames into
      'cache entries'.  For the slot caches, the cache entry is just the cached
      value itself.  For the test caches, the cache entry is a pair of two *hashsets*,
      indicating if the test returned true (the CAR) or false (the CDR).
     To put it formulaically:
       slotid(frame) is cached as (slot_caches(slotid))(frame)
       slotid(frame) = value is cached as
         (CAR(test_caches(slotid))(frame)) contains value
       slotid(frame)!=value is cached as
         (CDR(test_caches(slotid))(frame)) contains value
     where the '=' relationship above really indicates 'has value' since slots
      are naturally multi-valued.
*/

static struct FD_HASHTABLE *make_slot_cache(fdtype slotid)
{
  fdtype table = fd_make_hashtable(NULL,17);
  u8_lock_mutex(&slotcache_lock);
  fd_hashtable_store(&slot_caches,slotid,table);
  u8_unlock_mutex(&slotcache_lock);
  return (struct FD_HASHTABLE *)table;
}

static struct FD_HASHTABLE *make_test_cache(fdtype slotid)
{
  fdtype table = fd_make_hashtable(NULL,17);
  u8_lock_mutex(&slotcache_lock);
  fd_hashtable_store(&test_caches,slotid,table);
  u8_unlock_mutex(&slotcache_lock);
  return (struct FD_HASHTABLE *)table;
}

FD_EXPORT void fd_clear_slotcache(fdtype slotid)
{
  u8_lock_mutex(&slotcache_lock);
  if (fd_hashtable_probe(&slot_caches,slotid))
    fd_hashtable_store(&slot_caches,slotid,FD_VOID);
  if (fd_hashtable_probe(&test_caches,slotid))
    fd_hashtable_store(&test_caches,slotid,FD_VOID);
  u8_unlock_mutex(&slotcache_lock);
}

FD_EXPORT void fd_clear_slotcache_entry(fdtype frame,fdtype slotid)
{
  fdtype slotcache = fd_hashtable_get(&slot_caches,slotid,FD_VOID);
  fdtype testcache = fd_hashtable_get(&test_caches,slotid,FD_VOID);
  if (FD_HASHTABLEP(slotcache))
    fd_hashtable_op(FD_XHASHTABLE(slotcache),fd_table_replace,frame,FD_VOID);
  /* Need to do this more selectively. */
  if (FD_HASHTABLEP(testcache))
    fd_hashtable_op(FD_XHASHTABLE(testcache),fd_table_replace,frame,FD_VOID);
}

FD_EXPORT void fd_clear_testcache_entry(fdtype frame,fdtype slotid,fdtype value)
{
  fdtype testcache = fd_hashtable_get(&test_caches,slotid,FD_VOID);
  if (FD_HASHTABLEP(testcache)) {
    if (FD_VOIDP(value))
      fd_hashtable_op(FD_XHASHTABLE(testcache),fd_table_replace,frame,FD_VOID);
    else {
      fdtype cache = fd_hashtable_get(FD_XHASHTABLE(testcache),frame,FD_EMPTY_CHOICE);
      if (FD_PAIRP(cache)) {
        fd_hashset_drop((fd_hashset)FD_CAR(cache),value);
        fd_hashset_drop((fd_hashset)FD_CDR(cache),value);}}}
}


/* Dependency maintenance */

/* How it works:
     Dependency maintenance ensures that cached values are flushed when the values
      they depend on change.  The overall model is very simple: when inferences
      happen through GET and TEST methods, they start tracking slot accesses and
      tests.  Each access or test is recorded and, when the method finishes, a
      dependency is recorded from the accessed or tested slot/values and the result
      of the methods.  Then, when a value is changed, this table is used to find the
      dependent values and reset their cache entries.  Note that this method is ancient,
      dating back to at least 1984, when it was a part of Haase's ARLO language.
     In practice, individual accesses are represented by lisp objects which we will
      call *factoids* but are just conses.  The CAR of the factoid is a frame and
      the CDR is either a slotid (symbol or OID) or a pair of a slotid and a value.
      The latter case represents the results of *tests* (e.g. does slotid(frame) = value)
      while the former represents all the values of a slot (e.g. slotid(frame)).
     Dependency tracking uses the frame operation stack which is also used to avoid
      infinite recurrences.  When a slot is gotten or tested, the stack of frame
      operations is climbed to find the first operation with a non-NULL dependencies
      field.  The current operation is added to this value (which is a vector of
      FD_DEPENDENCY_RECORD structures), with an FD_VOID value distinguishing the
      case of a GET and a TEST.
     When a computed value is returned from a given operation, the accumulated dependencies
      (stored on current frame operations stack entry) are converted into entries
      in the 'implications' hashtable to be used by fd_decache() when any of the
      dependencies are changed. */

static struct FD_HASHTABLE implications;
fd_hashtable fd_implications_table = &implications;

static void init_dependencies(fd_frameop_stack *cxt)
{
  if (cxt->dependencies) return;
  else {
    cxt->dependencies = u8_alloc_n(32,struct FD_DEPENDENCY_RECORD);
    cxt->n_deps = 0; cxt->max_deps = 32;}
}

static void note_dependency(fd_frameop_stack *cxt,fdtype frame,fdtype slotid,fdtype value)
{
  fd_frameop_stack *scan = cxt;
  while (scan)
    if (scan->dependencies) {
      int n = scan->n_deps; struct FD_DEPENDENCY_RECORD *records = NULL;
      if (scan->n_deps>=scan->max_deps) {
        scan->dependencies = records=
          u8_realloc_n(scan->dependencies,scan->max_deps*2,struct FD_DEPENDENCY_RECORD);
        scan->max_deps = scan->max_deps*2;}
      else records = scan->dependencies;
      records[n].frame = fd_incref(frame);
      records[n].slotid = fd_incref(slotid);
      records[n].value = fd_incref(value);
      scan->n_deps = n+1;
      return;}
    else scan = scan->next;
}

static void record_dependencies(fd_frameop_stack *cxt,fdtype factoid)
{
  if (cxt->dependencies) {
    int i = 0, n = cxt->n_deps;
    struct FD_DEPENDENCY_RECORD *records = cxt->dependencies;
    fdtype *factoids = u8_alloc_n(n,fdtype);
    while (i<n) {
      fdtype depends;
      if (FD_VOIDP(records[i].value))
        depends = fd_conspair(records[i].frame,records[i].slotid);
      else depends = fd_make_list(3,records[i].frame,records[i].slotid,
                                fd_incref(records[i].value));
      factoids[i++]=depends;}
    fd_hashtable_iterkeys(&implications,fd_table_add,n,factoids,factoid);
    i = 0; while (i<n) {fdtype factoid = factoids[i++]; fd_decref(factoid);}
    u8_free(factoids);}
}

static void decache_factoid(fdtype factoid)
{
  fdtype frame = FD_CAR(factoid);
  fdtype predicate = FD_CDR(factoid);
  if (FD_PAIRP(predicate)) {
    fdtype value = FD_CDR(predicate);
    fd_clear_slotcache_entry(frame,predicate);
    fd_clear_testcache_entry(frame,predicate,value);}
  else {
    fd_clear_slotcache_entry(frame,predicate);
    fd_clear_testcache_entry(frame,predicate,FD_VOID);}
}

static void decache_implications(fdtype factoid)
{
  fdtype implies = fd_hashtable_get(&implications,factoid,FD_EMPTY_CHOICE);
  decache_factoid(factoid);
  if (!(FD_EMPTY_CHOICEP(implies)))
    fd_hashtable_store(&implications,factoid,FD_EMPTY_CHOICE);
  {FD_DO_CHOICES(imply,implies) decache_implications(imply);}
  fd_decref(implies);
}

FD_EXPORT void fd_decache(fdtype frame,fdtype slotid,fdtype value)
{
  fdtype factoid;
  if (!(FD_VOIDP(value))) {
    /* Do a value specific decache */
    factoid = fd_conspair
      (fd_incref(frame),fd_conspair(fd_incref(slotid),fd_incref(value)));
    decache_implications(factoid);
    fd_decref(factoid);}
  factoid = fd_conspair(fd_incref(frame),fd_incref(slotid));
  decache_implications(factoid);
  fd_decref(factoid);
}

FD_EXPORT void fd_clear_slotcaches()
{
  u8_lock_mutex(&slotcache_lock);
  if (slot_caches.table_n_keys)
    fd_reset_hashtable(&slot_caches,17,1);
  if (test_caches.table_n_keys)
    fd_reset_hashtable(&test_caches,17,1);
  if (implications.table_n_keys)
    fd_reset_hashtable(&implications,17,1);
  u8_unlock_mutex(&slotcache_lock);
}


/* Method lookup */

static struct FD_FUNCTION *lookup_method(fdtype arg)
{
  if (FD_FUNCTIONP(arg))
    return FD_XFUNCTION(arg);
  else if (FD_SYMBOLP(arg)) {
    fdtype lookup=
      fd_hashtable_get(FD_XHASHTABLE(fd_method_table),arg,FD_EMPTY_CHOICE);
    if (FD_FUNCTIONP(lookup)) {
      struct FD_FUNCTION *fn = FD_XFUNCTION(lookup); fd_decref(lookup);
      return fn;}
    else return NULL;}
  else return NULL;
}

static fdtype get_slotid_methods(fdtype slotid,fdtype method_name)
{
  fd_pool p = fd_oid2pool(slotid); fdtype smap, result;
  if (FD_EXPECT_FALSE(p == NULL))
    return fd_anonymous_oid("get_slotid_methods",slotid);
  else smap = fd_fetch_oid(p,slotid);
  if (FD_ABORTP(smap))
    return smap;
  else if (FD_SLOTMAPP(smap))
    result = fd_slotmap_get((fd_slotmap)smap,method_name,FD_EMPTY_CHOICE);
  else if (FD_SCHEMAPP(smap))
    result = fd_schemap_get((fd_schemap)smap,method_name,FD_EMPTY_CHOICE);
  else if (FD_TABLEP(smap))
    result = fd_get(smap,slotid,FD_EMPTY_CHOICE);
  else result = FD_VOID;
  fd_decref(smap);
  return result;
}


/* Frame Operations */

FD_EXPORT fdtype fd_frame_get(fdtype f,fdtype slotid)
{
  if (FD_OIDP(slotid)) {
    struct FD_FRAMEOP_STACK fop;
    FD_INIT_FRAMEOP_STACK_ENTRY(fop,fd_getop,f,slotid,FD_VOID);
    if (fd_in_progressp(&fop)) return FD_EMPTY_CHOICE;
    else {
      int ipestate = fd_ipeval_status();
      struct FD_HASHTABLE *cache;
      fdtype methods, cachev, cached, computed = FD_EMPTY_CHOICE;
      cachev = fd_hashtable_get(&slot_caches,slotid,FD_VOID);
      if (FD_VOIDP(cachev)) {
        cache = make_slot_cache(slotid);
        cached = FD_VOID; cachev = (fdtype)cache;}
      else if (FD_EMPTY_CHOICEP(cachev)) {
        cache = NULL; cached = FD_VOID;}
      else {
        cache = FD_XHASHTABLE(cachev);
        cached = fd_hashtable_get(cache,f,FD_VOID);}
      if (!(FD_VOIDP(cached))) {
        fd_decref(cachev);
        return overlay_get(slot_overlay,cached,slotid,f);}
      methods = get_slotid_methods(slotid,get_methods);
      /* Methods will be void only if the value of the slotoid isn't a table,
         which is typically only the case when it hasn't been fetched yet. */
      note_dependency(&fop,f,slotid,FD_VOID);
      if (FD_VOIDP(methods))
        return overlay_get(slot_overlay,FD_EMPTY_CHOICE,slotid,f);
      else if (FD_EMPTY_CHOICEP(methods)) {
        fdtype value = fd_oid_get(f,slotid,FD_EMPTY_CHOICE);
        if (FD_EMPTY_CHOICEP(value))
          methods = get_slotid_methods(slotid,compute_methods);
        else {
          fd_decref(cachev);
          return overlay_get(slot_overlay,value,slotid,f);}}
      if (FD_VOIDP(methods)) return FD_EMPTY_CHOICE;
      else if (FD_ABORTP(methods)) {
        fd_decref(cachev);
        return methods;}
      /* At this point, we're computing the slot value */
      fd_push_opstack(&fop);
      init_dependencies(&fop);
      {FD_DO_CHOICES(method,methods) {
          struct FD_FUNCTION *fn = lookup_method(method);
          if (fn) {
            fdtype args[2], value; args[0]=f; args[1]=slotid;
            value = fd_finish_call(fd_dapply((fdtype)fn,2,args));
            if (FD_ABORTP(value)) {
              fd_push_error_context
                (fd_apply_context,NULL,
                 fd_make_nvector(3,method,f,slotid));
              fd_decref(computed); fd_decref(methods);
              fd_pop_opstack(&fop,0);
              fd_decref(cachev);
              return value;}
            FD_ADD_TO_CHOICE(computed,value);}}}
      computed = fd_simplify_choice(computed);
      fd_decref(methods);
      if ((cache) && ((fd_ipeval_status() == ipestate))) {
        fdtype factoid = fd_conspair(fd_incref(f),fd_incref(slotid));
        record_dependencies(&fop,factoid); fd_decref(factoid);
        fd_hashtable_store(cache,f,computed);
        fd_pop_opstack(&fop,1);}
      else fd_pop_opstack(&fop,0);
      fd_decref(cachev);
      return overlay_get(slot_overlay,computed,slotid,f);}}
  else if (FD_EMPTY_CHOICEP(f)) return FD_EMPTY_CHOICE;
  else {
    struct FD_FRAMEOP_STACK *ptr = get_opstack();
    if (ptr) note_dependency(ptr,f,slotid,FD_VOID);
    return overlay_get
      (slot_overlay,fd_oid_get(f,slotid,FD_EMPTY_CHOICE),slotid,f);}
}

FD_EXPORT int fd_frame_test(fdtype f,fdtype slotid,fdtype value)
{
  if (!(FD_OIDP(f)))
    return overlay_test(slot_overlay,fd_test(f,slotid,value),f,slotid,value);
  else if (FD_OIDP(slotid)) {
    struct FD_FRAMEOP_STACK fop;
    FD_INIT_FRAMEOP_STACK_ENTRY(fop,fd_testop,f,slotid,value);
    if (fd_in_progressp(&fop)) return 0;
    else if (FD_VOIDP(value)) {
      fdtype values = fd_frame_get(f,slotid);
      note_dependency(&fop,f,slotid,value);
      if (FD_EMPTY_CHOICEP(values))
        return overlay_test(slot_overlay,0,f,slotid,value);
      else {
        fd_decref(values);
        return overlay_test(slot_overlay,1,f,slotid,value);}}
    else {
      struct FD_HASHTABLE *cache; int result = 0;
      fdtype cachev = fd_hashtable_get(&test_caches,slotid,FD_VOID);
      fdtype cached, methods;
      if (FD_VOIDP(cachev)) {
        cache = make_test_cache(slotid); cachev = (fdtype)cache;
        cached = fd_conspair(fd_make_hashset(),fd_make_hashset());
        fd_hashtable_store(cache,f,cached);}
      else if (FD_EMPTY_CHOICEP(cachev)) {
        cache = NULL; cached = FD_VOID;}
      else {
        cache = FD_XHASHTABLE(cachev);
        cached = fd_hashtable_get(cache,f,FD_VOID);
        if (FD_VOIDP(cached)) {
          cached = fd_conspair(fd_make_hashset(),fd_make_hashset());
          fd_hashtable_store(cache,f,cached);}}
      if (FD_PAIRP(cached)) {
        fdtype in = FD_CAR(cached), out = FD_CDR(cached);
        if (fd_hashset_get(FD_XHASHSET(in),value)) {
          fd_decref(cached); fd_decref(cachev);
          return overlay_test(slot_overlay,1,f,slotid,value);}
        else if (fd_hashset_get(FD_XHASHSET(out),value)) {
          fd_decref(cached); fd_decref(cachev);
          return overlay_test(slot_overlay,0,f,slotid,value);}
        else methods = get_slotid_methods(slotid,test_methods);}
      else methods = get_slotid_methods(slotid,test_methods);
      if (FD_VOIDP(methods)) {
        fd_decref(cached); fd_decref(cachev);
        return overlay_test(slot_overlay,0,f,slotid,value);}
      else if (FD_EMPTY_CHOICEP(methods)) {
        fdtype values = fd_frame_get(f,slotid);
        result = fd_overlapp(value,values);
        fd_decref(values);}
      else {
        fdtype args[3];
        args[0]=f; args[1]=slotid; args[2]=value;
        fd_push_opstack(&fop);
        init_dependencies(&fop);
        {
          FD_DO_CHOICES(method,methods) {
            struct FD_FUNCTION *fn = lookup_method(method);
            if (fn) {
              fdtype v = fd_apply((fdtype)fn,3,args);
              if (FD_ABORTP(v)) {
                fd_push_error_context
                  (fd_apply_context,NULL,
                   fd_make_nvector(4,method,f,slotid,fd_incref(value)));
                fd_decref(methods); fd_decref(cachev);
                fd_pop_opstack(&fop,0);
                return fd_interr(v);}
              else if (FD_TRUEP(v)) {
                result = 1; fd_decref(v); break;}
              else {}}}}
        if ((cache) && (!(fd_ipeval_failp()))) {
          fdtype factoid = fd_make_list(3,fd_incref(f),
                                      fd_incref(slotid),
                                      fd_incref(value));
          record_dependencies(&fop,factoid); fd_decref(factoid);
          fd_pop_opstack(&fop,0);}
        else fd_pop_opstack(&fop,0);}
      if (result) fd_hashset_add(FD_XHASHSET(FD_CAR(cached)),value);
      else fd_hashset_add(FD_XHASHSET(FD_CDR(cached)),value);
      fd_decref(cached); fd_decref(cachev);
      return overlay_test(slot_overlay,result,f,slotid,value);}}
  else if (FD_EMPTY_CHOICEP(f)) return 0;
  else {
    struct FD_FRAMEOP_STACK *ptr = get_opstack();
    if (ptr) note_dependency(ptr,f,slotid,value);
    return overlay_test
      (slot_overlay,fd_oid_test(f,slotid,value),f,slotid,value);}
}

FD_EXPORT int fd_frame_add(fdtype f,fdtype slotid,fdtype value)
{
  if (FD_OIDP(slotid)) {
    struct FD_FRAMEOP_STACK fop;
    FD_INIT_FRAMEOP_STACK_ENTRY(fop,fd_addop,f,slotid,value);
    if (fd_in_progressp(&fop)) return 0;
    else {
      fdtype methods = fd_oid_get(slotid,add_effects,FD_EMPTY_CHOICE);
      fd_decache(f,slotid,value);
      if (FD_EMPTY_CHOICEP(methods)) {
        return fd_oid_add(f,slotid,value);}
      else {
        fdtype args[3];
        fd_push_opstack(&fop);
        args[0]=f; args[1]=slotid; args[2]=value;
        {
          FD_DO_CHOICES(method,methods) {
            struct FD_FUNCTION *fn = lookup_method(method);
            if (fn) {
              fdtype v = fd_apply((fdtype)fn,3,args);
              if (FD_ABORTP(v)) {
                fd_pop_opstack(&fop,0);
                fd_decref(methods);
                return fd_interr(v);}
              else fd_decref(v);}}}
        fd_pop_opstack(&fop,1);
        return 1;}}}
  else if (FD_EMPTY_CHOICEP(f)) return 0;
  else {
    fd_decache(f,slotid,value);
    return fd_oid_add(f,slotid,value);}
}

FD_EXPORT int fd_frame_drop(fdtype f,fdtype slotid,fdtype value)
{
  if (FD_OIDP(slotid)) {
    struct FD_FRAMEOP_STACK fop;
    FD_INIT_FRAMEOP_STACK_ENTRY(fop,fd_dropop,f,slotid,value);
    if (fd_in_progressp(&fop)) return 0;
    else {
      fdtype methods = fd_oid_get(slotid,drop_effects,FD_EMPTY_CHOICE);
      fd_decache(f,slotid,value);
      if (FD_EMPTY_CHOICEP(methods)) {
        fd_oid_drop(f,slotid,value);
        return 1;}
      else {
        fdtype args[3];
        fd_clear_slotcache_entry(slotid,f);
        fd_push_opstack(&fop);
        args[0]=f; args[1]=slotid; args[2]=value;
        {
          FD_DO_CHOICES(method,methods) {
            struct FD_FUNCTION *fn = lookup_method(method);
            if (fn) {
              fdtype v = fd_apply((fdtype)fn,3,args);
              if (FD_ABORTP(v)) {
                fd_pop_opstack(&fop,0);
                fd_decref(methods);
                return fd_interr(v);}
              else fd_decref(v);}}}
        fd_pop_opstack(&fop,1);
        return 1;}}}
  else if (FD_EMPTY_CHOICEP(f)) return 0;
  else {
    fd_decache(f,slotid,value);
    return fd_oid_drop(f,slotid,value);}
}


/* Creating frames */

FD_EXPORT fdtype fd_new_frame(fdtype pool_spec,fdtype initval,int copyflags)
{
  fd_pool p; fdtype oid;
  /* #f no pool, just create a slotmap
     #t use the default pool
     pool (use the pool!) */
  if (FD_FALSEP(pool_spec))
    return fd_empty_slotmap();
  else if ((FD_DEFAULTP(pool_spec)) || (FD_VOIDP(pool_spec)))
    if (fd_default_pool) p = fd_default_pool;
    else return fd_err(_("No default pool"),"frame_create_lexpr",NULL,FD_VOID);
  else if ((FD_TRUEP(pool_spec))||(pool_spec == FD_FIXZERO))
    p = fd_zero_pool;
  else if ((p = fd_lisp2pool(pool_spec)) == NULL)
    return fd_err(fd_NoSuchPool,"frame_create_lexpr",NULL,pool_spec);
  /* At this point, we have p!=NULL and we get an OID */
  oid = fd_pool_alloc(p,1);
  if (FD_ABORTP(oid)) return oid;
  /* Now we figure out what to store in the OID */
  if (FD_VOIDP(initval)) initval = fd_empty_slotmap();
  else if ((FD_OIDP(initval)) && (copyflags)) {
    /* Avoid aliasing */
    fdtype oidval = fd_oid_value(initval);
    if (FD_ABORTP(oidval)) return oidval;
    initval = fd_copier(oidval,copyflags);
    fd_decref(oidval);}
  else if (copyflags)
    initval = fd_deep_copy(initval);
  else fd_incref(initval);
  /* Now we actually set the OID */
  if ((fd_set_oid_value(oid,initval))<0) {
    fd_decref(initval); return FD_ERROR_VALUE;}
  else {
    fd_decref(initval);
    return oid;}
}


/* Searching */

static fdtype make_features(fdtype slotids,fdtype values)
{
  fdtype results = FD_EMPTY_CHOICE;
  FD_DO_CHOICES(slotid,slotids) {
    FD_DO_CHOICES(value,values) {
      fdtype feature = fd_make_pair(slotid,value);
      FD_ADD_TO_CHOICE(results,feature);}}
  return results;
}

FD_EXPORT fdtype fd_prim_find(fdtype indexes,fdtype slotids,fdtype values)
{
  if (FD_CHOICEP(indexes)) {
    fdtype combined = FD_EMPTY_CHOICE;
    FD_DO_CHOICES(index,indexes)
      if ((FD_INDEXP(index))||(FD_TYPEP(index,fd_consed_index_type))) {
        fd_index ix = fd_indexptr(index);
        if (ix == NULL) {
          fd_decref(combined);
          return FD_ERROR_VALUE;}
        else {
          FD_DO_CHOICES(slotid,slotids) {
            FD_DO_CHOICES(value,values) {
              fdtype key = fd_make_pair(slotid,value);
              fdtype result = fd_index_get(ix,key);
              FD_ADD_TO_CHOICE(combined,result);
              fd_decref(key);}}}}
      else if (FD_TABLEP(index)) {
        FD_DO_CHOICES(slotid,slotids) {
          FD_DO_CHOICES(value,values) {
            fdtype key = fd_make_pair(slotid,value);
            fdtype result = fd_get(index,key,FD_EMPTY_CHOICE);
            FD_ADD_TO_CHOICE(combined,result);
            fd_decref(key);}}}
      else {
        fd_decref(combined);
        return fd_type_error(_("index"),"fd_prim_find",index);}
    return combined;}
  else if (FD_TABLEP(indexes)) {
    fdtype combined = FD_EMPTY_CHOICE;
    FD_DO_CHOICES(slotid,slotids) {
      FD_DO_CHOICES(value,values) {
        fdtype key = fd_make_pair(slotid,value);
        fdtype result = fd_get(indexes,key,FD_EMPTY_CHOICE);
        FD_ADD_TO_CHOICE(combined,result);
        fd_decref(key);}}
    return combined;}
  else {
    fdtype combined = FD_EMPTY_CHOICE;
    fd_index ix = fd_indexptr(indexes);
    if (ix == NULL) {
      fd_decref(combined);
      return FD_ERROR_VALUE;}
    else {
      FD_DO_CHOICES(slotid,slotids) {
        FD_DO_CHOICES(value,values) {
          fdtype key = fd_make_pair(slotid,value);
          fdtype result = fd_index_get(ix,key);
          FD_ADD_TO_CHOICE(combined,result);
          fd_decref(key);}}
      return combined;}}
}

FD_EXPORT fdtype fd_finder(fdtype indexes,int n,fdtype *slotvals)
{
  int i = 0, n_conjuncts = n/2;
  fdtype _conjuncts[6], *conjuncts=_conjuncts, result;
  if (FD_EMPTY_CHOICEP(indexes)) return FD_EMPTY_CHOICE;
  if (n_conjuncts>6) conjuncts = u8_alloc_n(n_conjuncts,fdtype);
  while (i < n_conjuncts) {
    conjuncts[i]=fd_prim_find(indexes,slotvals[i*2],slotvals[i*2+1]);
    if (FD_ABORTP(conjuncts[i])) {
      fdtype error = conjuncts[i];
      int j = 0; while (j<i) {fd_decref(conjuncts[j]); j++;}
      if (conjuncts != _conjuncts) u8_free(conjuncts);
      return error;}
    if (FD_EMPTY_CHOICEP(conjuncts[i])) {
      int j = 0; while (j<i) {fd_decref(conjuncts[j]); j++;}
      return FD_EMPTY_CHOICE;}
    i++;}
  result = fd_intersection(conjuncts,n_conjuncts);
  i = 0; while (i < n_conjuncts) {
    fdtype cj=_conjuncts[i++]; fd_decref(cj);}
  if (_conjuncts != conjuncts) u8_free(conjuncts);
  return result;
}

FD_EXPORT fdtype fd_find_frames(fdtype indexes,...)
{
  fdtype _slotvals[64], *slotvals=_slotvals, val;
  int n_slotvals = 0, max_slotvals = 64; va_list args;
  va_start(args,indexes); val = va_arg(args,fdtype);
  while (!(FD_VOIDP(val))) {
    if (n_slotvals>=max_slotvals) {
      if (max_slotvals == 64) {
        fdtype *newsv = u8_alloc_n(128,fdtype); int i = 0;
        while (i<64) {newsv[i]=slotvals[i]; i++;}
        slotvals = newsv; max_slotvals = 128;}
      else {
        slotvals = u8_realloc(slotvals,sizeof(fdtype)*max_slotvals*2);
        max_slotvals = max_slotvals*2;}}
    slotvals[n_slotvals++]=val; val = va_arg(args,fdtype);}
  if (n_slotvals%2) {
    if (slotvals != _slotvals) u8_free(slotvals);
    return fd_err(OddFindFramesArgs,"fd_find_frames",NULL,FD_VOID);}
  if (slotvals != _slotvals) {
    fdtype result = fd_finder(indexes,n_slotvals,slotvals);
    u8_free(slotvals);
    return result;}
  else return fd_finder(indexes,n_slotvals,slotvals);
}


/* Find prefetching */

/* This prefetches a set of slotvalue keys from an index.
   The main advantage of this over assembling a set of keys
   externally is that this doesn't bother sorting the vector
   of generated keys.  This can save time, especially when the
   numbers of values is large and values may be strings. */

FD_EXPORT
int fd_find_prefetch(fd_index ix,fdtype slotids,fdtype values)
{
  if ((ix->index_handler->fetchn) == NULL) {
    fdtype keys = FD_EMPTY_CHOICE;
    FD_DO_CHOICES(slotid,slotids) {
      FD_DO_CHOICES(value,values) {
        fdtype key = fd_conspair(slotid,value);
        FD_ADD_TO_CHOICE(keys,key);}}
    fd_index_prefetch(ix,keys);
    fd_decref(keys);
    return 1;}
  else {
    int max_keys = FD_CHOICE_SIZE(slotids)*FD_CHOICE_SIZE(values);
    fdtype *keyv = u8_alloc_n(max_keys,fdtype);
    fdtype *valuev = NULL;
    int n_keys = 0;
    FD_DO_CHOICES(slotid,slotids) {
      FD_DO_CHOICES(value,values) {
        fdtype key = fd_conspair(slotid,value);
        keyv[n_keys++]=key;}}
    valuev = (ix->index_handler->fetchn)(ix,n_keys,keyv);
    fd_hashtable_iter(&(ix->index_cache),fd_table_add_empty_noref,
                      n_keys,keyv,valuev);
    u8_free(keyv); u8_free(valuev);
    return 1;}
}


/* Indexing frames */

#define FD_LOOP_BREAK() FD_STOP_DO_CHOICES; break

FD_EXPORT
int fd_index_frame(fd_index ix,fdtype frames,fdtype slotids,fdtype values)
{
  if (FD_VOIDP(values)) {
    int rv = 0, sum = 0;
    FD_DO_CHOICES(f,frames) {
      fdtype frame_features = FD_EMPTY_CHOICE;
      FD_DO_CHOICES(slotid,slotids) {
        fdtype values = fd_frame_get(f,slotid);
        if (FD_ABORTP(values)) {
          frame_features = values; rv = -1;
          /* break from iterating over slotids */
          FD_LOOP_BREAK();}
        else {
          fdtype features = make_features(slotid,values);
          FD_ADD_TO_CHOICE(frame_features,features);
          fd_decref(values);}}
      if (rv>=0) {
        rv = fd_index_add(ix,frame_features,f);
        if (rv>0) sum = sum+rv;}
      fd_decref(frame_features);}
    if (rv<0) return rv;
    else return sum;}
  else {
    int rv = 0, sum = 0;
    fdtype features = make_features(slotids,values);
    if (FD_CHOICEP(frames)) {
      FD_DO_CHOICES(f,frames) {
        rv = fd_index_add(ix,features,f);
        if (rv<0) {
          FD_LOOP_BREAK();}
        else sum = sum+rv;}}
    else {
      rv = fd_index_add(ix,features,frames);
      if (rv>0) sum = rv;}
    fd_decref(features);
    if (rv<0) return rv;
    else return sum;}
}


/* Background searching */

FD_EXPORT fdtype fd_bg_get(fdtype slotid,fdtype value)
{
  if (fd_background) {
    fdtype results = FD_EMPTY_CHOICE;
    fdtype features = make_features(slotid,value);
    if ((fd_prefetch) && (fd_ipeval_status()==0) &&
        (FD_CHOICEP(features)) &&
        (fd_index_prefetch((fd_index)fd_background,features)<0)) {
      fd_decref(features); return FD_ERROR_VALUE;}
    else {
      FD_DO_CHOICES(feature,features) {
        fdtype result = fd_index_get((fd_index)fd_background,feature);
        if (FD_ABORTP(result)) {
          fd_decref(results); fd_decref(features);
          return result;}
        else if (FD_EXPECT_TRUE(FD_VOIDP(index_overlay))) {
          FD_ADD_TO_CHOICE(results,result);}
        else {
          fdtype overlaid_results=
            overlay_get(index_overlay,result,slotid,value);
          FD_ADD_TO_CHOICE(results,overlaid_results);}}
      fd_decref(features);}
    return fd_simplify_choice(results);}
  else return FD_EMPTY_CHOICE;
}

FD_EXPORT fdtype fd_bgfinder(int n,fdtype *slotvals)
{
  int i = 0, n_conjuncts = n/2;
  fdtype _conjuncts[6], *conjuncts=_conjuncts, result;
  if (fd_background == NULL) return FD_EMPTY_CHOICE;
  if (n_conjuncts>6) conjuncts = u8_alloc_n(n_conjuncts,fdtype);
  while (i < n_conjuncts) {
    _conjuncts[i]=fd_bg_get(slotvals[i*2],slotvals[i*2+1]); i++;}
  result = fd_intersection(conjuncts,n_conjuncts);
  i = 0; while (i < n_conjuncts) {
    fdtype cj=_conjuncts[i++]; fd_decref(cj);}
  if (_conjuncts != conjuncts) u8_free(conjuncts);
  return result;
}

FD_EXPORT fdtype fd_bgfind(fdtype slotid,fdtype values,...)
{
  fdtype _slotvals[64], *slotvals=_slotvals, val;
  int n_slotvals = 0, max_slotvals = 64; va_list args;
  va_start(args,values); val = va_arg(args,fdtype);
  slotvals[n_slotvals++]=slotid; slotvals[n_slotvals++]=values;
  while (!(FD_VOIDP(val))) {
    if (n_slotvals>=max_slotvals) {
      if (max_slotvals == 64) {
        fdtype *newsv = u8_alloc_n(128,fdtype); int i = 0;
        while (i<64) {newsv[i]=slotvals[i]; i++;}
        slotvals = newsv; max_slotvals = 128;}
      else {
        slotvals = u8_realloc_n(slotvals,max_slotvals*2,fdtype);
        max_slotvals = max_slotvals*2;}}
    slotvals[n_slotvals++]=val; val = va_arg(args,fdtype);}
  if (n_slotvals%2) {
    if (slotvals != _slotvals) u8_free(slotvals);
    return fd_err(OddFindFramesArgs,"fd_bgfind",NULL,FD_VOID);}
  if (slotvals != _slotvals) {
    fdtype result = fd_bgfinder(n_slotvals,slotvals);
    u8_free(slotvals);
    return result;}
  else return fd_bgfinder(n_slotvals,slotvals);
}

FD_EXPORT int fd_bg_prefetch(fdtype keys)
{
  if (fd_background)
    return fd_index_prefetch((fd_index)fd_background,keys);
  else return 0;
}

/* Checking the cache load for the slot/test caches */

static int hashtable_cachecount(fdtype key,fdtype v,void *ptr)
{
  if (FD_HASHTABLEP(v)) {
    fd_hashtable h = (fd_hashtable)v;
    int *count = (int *)ptr;
    *count = *count+h->table_n_keys;}
  return 0;
}

FD_EXPORT int fd_slot_cache_load()
{
  int count = 0;
  fd_for_hashtable(&slot_caches,hashtable_cachecount,&count,1);
  fd_for_hashtable(&test_caches,hashtable_cachecount,&count,1);
  return count;
}


/* Initialization */

FD_EXPORT void fd_init_frames_c()
{
  u8_register_source_file(_FILEINFO);

  get_methods = fd_intern("GET-METHODS");
  compute_methods = fd_intern("COMPUTE-METHODS");
  test_methods = fd_intern("TEST-METHODS");
  add_effects = fd_intern("ADD-EFFECTS");
  drop_effects = fd_intern("DROP-EFFECTS");

  FD_INIT_STATIC_CONS(&slot_caches,fd_hashtable_type);
  FD_INIT_STATIC_CONS(&test_caches,fd_hashtable_type);
  fd_make_hashtable(&slot_caches,17);
  fd_make_hashtable(&test_caches,17);

  FD_INIT_STATIC_CONS(&implications,fd_hashtable_type);
  fd_make_hashtable(&implications,17);

  fd_register_config("SLOTOVERLAY",_("Slot overlay table"),
                     overlay_config_get,
                     overlay_config_set,
                     &slot_overlay);
  fd_register_config("INDEXOVERLAY",_("Index overlay table"),
                     overlay_config_get,
                     overlay_config_set,
                     &index_overlay);

  u8_init_mutex(&slotcache_lock);

#if FD_USE_TLS
  u8_new_threadkey(&_fd_inhibit_overlay_key,NULL);
  u8_new_threadkey(&opstack_key,NULL);
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
