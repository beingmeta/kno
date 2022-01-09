/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_POOLS KNO_DO_INLINE
#define KNO_INLINE_INDEXES KNO_DO_INLINE
#define KNO_INLINE_TABLES KNO_DO_INLINE
#define KNO_INLINE_CHOICES KNO_DO_INLINE
#define KNO_INLINE_IPEVAL KNO_DO_INLINE
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/apply.h"

#include <stdarg.h>

static struct KNO_HASHTABLE slot_caches, test_caches;
static lispval get_methods, compute_methods, test_methods;
static lispval add_effects, drop_effects;

static u8_mutex slotcache_lock;

lispval get_oid_value(lispval oid,kno_pool p);

/* The operations stack */

/* How it works:

   Kno inference operations rely on a dynamic *operations
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

#if KNO_USE_TLS
static u8_tld_key opstack_key;
#else
static __thread struct KNO_FRAMEOP_STACK *opstack = NULL;
#endif

static struct KNO_FRAMEOP_STACK *get_opstack()
{
#if KNO_USE_TLS
  return u8_tld_get(opstack_key);
#else
  return opstack;
#endif
}

/* Returns true if the operation specified by the stack entry
   is currently in progress. */
KNO_EXPORT int kno_in_progressp(struct KNO_FRAMEOP_STACK *op)
{
  struct KNO_FRAMEOP_STACK *start = get_opstack(), *ops = start;
  /* We initialize this field here, because we've gotten it anyway. */
  op->next = start;
  while (ops)
    if ((ops->op == op->op) &&
        (ops->frame == op->frame) &&
        (ops->slotid == op->slotid) &&
        (LISP_EQUAL(ops->value,op->value)))
      return 1;
    else ops = ops->next;
  return 0;
}

/* Adds an entry to the frame operation stack. */
KNO_EXPORT void kno_push_opstack(struct KNO_FRAMEOP_STACK *op)
{
  struct KNO_FRAMEOP_STACK *ops = get_opstack();
  op->next = ops;
  op->dependencies = NULL;
  op->n_deps = op->max_deps = 0;
#if (KNO_USE_TLS)
  u8_tld_set(opstack_key,op);
#else
  opstack = op;
#endif
}

/* Pops an entry from the frame operation stack. */
KNO_EXPORT int kno_pop_opstack(struct KNO_FRAMEOP_STACK *op,int normal)
{
  struct KNO_FRAMEOP_STACK *ops = get_opstack();
  if (ops != op) kno_raisex("Corrupted ops stack","kno_pop_opstack",NULL);
  else if (op->dependencies) {
    if (normal) u8_free(op->dependencies);
    else {
      struct KNO_DEPENDENCY_RECORD *scan = op->dependencies;
      struct KNO_DEPENDENCY_RECORD *limit = scan+op->n_deps;
      while (scan<limit) {
        kno_decref(scan->frame); kno_decref(scan->slotid); kno_decref(scan->value);
        scan++;}
      u8_free(op->dependencies);}}
#if (KNO_USE_TLS)
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

static struct KNO_HASHTABLE *make_slot_cache(lispval slotid)
{
  lispval table = kno_make_hashtable(NULL,17);
  u8_lock_mutex(&slotcache_lock);
  kno_hashtable_store(&slot_caches,slotid,table);
  u8_unlock_mutex(&slotcache_lock);
  return (struct KNO_HASHTABLE *)table;
}

static struct KNO_HASHTABLE *make_test_cache(lispval slotid)
{
  lispval table = kno_make_hashtable(NULL,17);
  u8_lock_mutex(&slotcache_lock);
  kno_hashtable_store(&test_caches,slotid,table);
  u8_unlock_mutex(&slotcache_lock);
  return (struct KNO_HASHTABLE *)table;
}

KNO_EXPORT void kno_clear_slotcache(lispval slotid)
{
  u8_lock_mutex(&slotcache_lock);
  if (kno_hashtable_probe(&slot_caches,slotid))
    kno_hashtable_store(&slot_caches,slotid,VOID);
  if (kno_hashtable_probe(&test_caches,slotid))
    kno_hashtable_store(&test_caches,slotid,VOID);
  u8_unlock_mutex(&slotcache_lock);
}

KNO_EXPORT void kno_clear_slotcache_entry(lispval frame,lispval slotid)
{
  lispval slotcache = kno_hashtable_get(&slot_caches,slotid,VOID);
  lispval testcache = kno_hashtable_get(&test_caches,slotid,VOID);
  if (HASHTABLEP(slotcache))
    kno_hashtable_op(KNO_XHASHTABLE(slotcache),kno_table_replace,frame,VOID);
  /* Need to do this more selectively. */
  if (HASHTABLEP(testcache))
    kno_hashtable_op(KNO_XHASHTABLE(testcache),kno_table_replace,frame,VOID);
  kno_decref(slotcache);
  kno_decref(testcache);
}

KNO_EXPORT void kno_clear_testcache_entry(lispval frame,lispval slotid,lispval value)
{
  lispval testcache = kno_hashtable_get(&test_caches,slotid,VOID);
  if (HASHTABLEP(testcache)) {
    if (VOIDP(value))
      kno_hashtable_op(KNO_XHASHTABLE(testcache),kno_table_replace,frame,VOID);
    else {
      lispval cache = kno_hashtable_get(KNO_XHASHTABLE(testcache),frame,EMPTY);
      if (PAIRP(cache)) {
        kno_hashset_drop((kno_hashset)KNO_CAR(cache),value);
        kno_hashset_drop((kno_hashset)KNO_CDR(cache),value);}}}
  kno_decref(testcache);
}


static struct KNO_HASHTABLE *get_slot_cache(lispval slotid)
{
  lispval cachev = kno_hashtable_get(&slot_caches,slotid,KNO_VOID);
  if (KNO_VOIDP(cachev))
    return make_slot_cache(slotid);
  else if (KNO_HASHTABLEP(cachev))
    return (kno_hashtable) cachev;
  else {
    kno_decref(cachev);
    return NULL;}
}

static struct KNO_PAIR *get_test_cache(lispval f,lispval slotid)
{
  lispval cachev = kno_hashtable_get(&test_caches,slotid,KNO_VOID);
  struct KNO_HASHTABLE *cache = NULL;
  if (KNO_VOIDP(cachev)) {
    cache = make_test_cache(slotid);
    cachev = (lispval) cache;}
  else if (KNO_HASHTABLEP(cachev))
    cache = (kno_hashtable) cachev;
  else {
    kno_decref(cachev);
    cache = NULL;
    return NULL;}
  lispval inout = kno_hashtable_get(cache,f,KNO_VOID);
  if (KNO_PAIRP(inout)) {
    kno_decref(cachev);
    return (kno_pair) inout;}
  else if (KNO_VOIDP(inout)) {
    inout = kno_init_pair(NULL,kno_make_hashset(),kno_make_hashset());
    if (kno_hashtable_op(cache,kno_table_default,f,inout) == 0) {
      kno_decref(inout);
      inout = kno_hashtable_get(cache,f,KNO_VOID);}
    kno_decref(cachev);
    return (kno_pair) inout;}
  else {
    kno_decref(cachev);
    return NULL;}
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
   KNO_DEPENDENCY_RECORD structures), with an VOID value distinguishing the
   case of a GET and a TEST.
   When a computed value is returned from a given operation, the accumulated dependencies
   (stored on current frame operations stack entry) are converted into entries
   in the 'implications' hashtable to be used by kno_decache() when any of the
   dependencies are changed. */

static struct KNO_HASHTABLE implications;
kno_hashtable kno_implications_table = &implications;

static void init_dependencies(kno_frameop_stack *cxt)
{
  if (cxt->dependencies) return;
  else {
    cxt->dependencies = u8_alloc_n(32,struct KNO_DEPENDENCY_RECORD);
    cxt->n_deps = 0; cxt->max_deps = 32;}
}

static void note_dependency
(kno_frameop_stack *cxt,lispval frame,lispval slotid,lispval value)
{
  kno_frameop_stack *scan = cxt;
  while (scan)
    if (scan->dependencies) {
      int n = scan->n_deps; struct KNO_DEPENDENCY_RECORD *records = NULL;
      if (scan->n_deps>=scan->max_deps) {
        scan->dependencies = records=
          u8_realloc_n(scan->dependencies,scan->max_deps*2,
                       struct KNO_DEPENDENCY_RECORD);
        scan->max_deps = scan->max_deps*2;}
      else records = scan->dependencies;
      records[n].frame = kno_incref(frame);
      records[n].slotid = kno_incref(slotid);
      records[n].value = kno_incref(value);
      scan->n_deps = n+1;
      return;}
    else scan = scan->next;
}

static void record_dependencies(kno_frameop_stack *cxt,lispval factoid)
{
  if (cxt->dependencies) {
    int i = 0, n = cxt->n_deps;
    struct KNO_DEPENDENCY_RECORD *records = cxt->dependencies;
    lispval *factoids = u8_alloc_n(n,lispval);
    while (i<n) {
      lispval depends;
      if (VOIDP(records[i].value))
        depends = kno_conspair(records[i].frame,records[i].slotid);
      else depends = kno_make_list(3,records[i].frame,records[i].slotid,
                                  kno_incref(records[i].value));
      factoids[i++]=depends;}
    kno_hashtable_iterkeys(&implications,kno_table_add,n,factoids,factoid);
    i = 0; while (i<n) {lispval factoid = factoids[i++]; kno_decref(factoid);}
    u8_free(factoids);}
}

static void decache_factoid(lispval factoid)
{
  lispval frame = KNO_CAR(factoid);
  lispval predicate = KNO_CDR(factoid);
  if (PAIRP(predicate)) {
    lispval value = KNO_CDR(predicate);
    kno_clear_slotcache_entry(frame,predicate);
    kno_clear_testcache_entry(frame,predicate,value);}
  else {
    kno_clear_slotcache_entry(frame,predicate);
    kno_clear_testcache_entry(frame,predicate,VOID);}
}

static void decache_implications(lispval factoid)
{
  lispval implies = kno_hashtable_get(&implications,factoid,EMPTY);
  decache_factoid(factoid);
  if (!(EMPTYP(implies)))
    kno_hashtable_store(&implications,factoid,EMPTY);
  {DO_CHOICES(imply,implies) decache_implications(imply);}
  kno_decref(implies);
}

KNO_EXPORT void kno_decache(lispval frame,lispval slotid,lispval value)
{
  lispval factoid;
  if (!(VOIDP(value))) {
    /* Do a value specific decache */
    factoid = kno_conspair
      (kno_incref(frame),kno_conspair(kno_incref(slotid),kno_incref(value)));
    decache_implications(factoid);
    kno_decref(factoid);}
  factoid = kno_conspair(kno_incref(frame),kno_incref(slotid));
  decache_implications(factoid);
  kno_decref(factoid);
}

KNO_EXPORT void kno_clear_slotcaches()
{
  u8_lock_mutex(&slotcache_lock);
  if (slot_caches.table_n_keys)
    kno_reset_hashtable(&slot_caches,17,1);
  if (test_caches.table_n_keys)
    kno_reset_hashtable(&test_caches,17,1);
  if (implications.table_n_keys)
    kno_reset_hashtable(&implications,17,1);
  u8_unlock_mutex(&slotcache_lock);
}


/* Method lookup */

static struct KNO_FUNCTION *lookup_method(lispval arg)
{
  if (KNO_FUNCTIONP(arg))
    return KNO_FUNCTION_INFO(arg);
  else if (SYMBOLP(arg)) {
    lispval lookup =
      kno_hashtable_get(KNO_XHASHTABLE(kno_method_table),arg,EMPTY);
    if (KNO_FUNCTIONP(lookup)) {
      kno_decref(lookup);
      return KNO_FUNCTION_INFO(lookup);}
    else return NULL;}
  else return NULL;
}

static lispval get_slotid_methods(lispval slotid,lispval method_name)
{
  kno_pool p = kno_oid2pool(slotid); lispval smap, result;
  if (RARELY(p == NULL))
    return VOID;
  else smap = get_oid_value(slotid,p);
  if (KNO_ABORTP(smap))
    return smap;
  else if (SLOTMAPP(smap))
    result = kno_slotmap_get((kno_slotmap)smap,method_name,EMPTY);
  else if (SCHEMAPP(smap))
    result = kno_schemap_get((kno_schemap)smap,method_name,EMPTY);
  else if (TABLEP(smap))
    result = kno_get(smap,slotid,EMPTY);
  else result = VOID;
  kno_decref(smap);
  return result;
}


/* Frame Operations */

KNO_EXPORT lispval kno_frame_get(lispval f,lispval slotid)
{
  if (OIDP(slotid)) {
    struct KNO_FRAMEOP_STACK fop;
    KNO_INIT_FRAMEOP_STACK_ENTRY(fop,kno_getop,f,slotid,VOID);
    if (kno_in_progressp(&fop)) return EMPTY;
    else {
      int ipestate = kno_ipeval_status();
      struct KNO_HASHTABLE *cache = get_slot_cache(slotid);
      lispval methods, cached = VOID, computed = EMPTY;
      if (cache) {
        cached = kno_hashtable_get(cache,f,VOID);
        kno_decref(((lispval)cache));
        cache = NULL;}
      if (!(VOIDP(cached)))
        return cached;
      methods = get_slotid_methods(slotid,get_methods);
      /* Methods will be void only if the value of the slotoid isn't a table,
         which is typically only the case when it hasn't been fetched yet. */
      note_dependency(&fop,f,slotid,VOID);
      if (VOIDP(methods))
        return EMPTY;
      else if (EMPTYP(methods)) {
        lispval value = kno_oid_get(f,slotid,EMPTY);
        if (EMPTYP(value))
          methods = get_slotid_methods(slotid,compute_methods);
        else return value;}
      if (VOIDP(methods))
        return EMPTY;
      else if (KNO_ABORTP(methods))
        return methods;
      else NO_ELSE;
      /* At this point, we're computing the slot value */
      kno_push_opstack(&fop);
      init_dependencies(&fop);
      {DO_CHOICES(method,methods) {
          struct KNO_FUNCTION *fn = lookup_method(method);
          if (fn) {
            lispval args[2], value; args[0]=f; args[1]=slotid;
	    value = kno_dapply((lispval)fn,2,args);
            if (KNO_ABORTP(value)) {
              kno_decref(computed);
              kno_decref(methods);
              kno_pop_opstack(&fop,0);
              return value;}
            CHOICE_ADD(computed,value);}}}
      computed = kno_simplify_choice(computed);
      kno_decref(methods);
      if (kno_ipeval_status() == ipestate) {
        if ((cache = get_slot_cache(slotid))) {
          lispval factoid = kno_conspair(kno_incref(f),kno_incref(slotid));
          record_dependencies(&fop,factoid);
          kno_decref(factoid);
          kno_hashtable_store(cache,f,computed);
          kno_decref(((lispval)cache));
          kno_pop_opstack(&fop,1);}
        else kno_pop_opstack(&fop,1);}
      else kno_pop_opstack(&fop,0);
      return computed;}}
  else if (EMPTYP(f)) return EMPTY;
  else {
    struct KNO_FRAMEOP_STACK *ptr = get_opstack();
    if (ptr) note_dependency(ptr,f,slotid,VOID);
    return kno_oid_get(f,slotid,EMPTY);}
}

KNO_EXPORT int kno_frame_test(lispval f,lispval slotid,lispval value)
{
  if (!(OIDP(f)))
    return kno_test(f,slotid,value);
  else if (OIDP(slotid)) {
    struct KNO_FRAMEOP_STACK fop;
    KNO_INIT_FRAMEOP_STACK_ENTRY(fop,kno_testop,f,slotid,value);
    if (kno_in_progressp(&fop)) return 0;
    else if (VOIDP(value)) {
      lispval values = kno_frame_get(f,slotid);
      note_dependency(&fop,f,slotid,value);
      if (EMPTYP(values))
        return 0;
      else return 1;}
    else {
      int result = 0;
      struct KNO_PAIR *inout = get_test_cache(f,slotid);
      lispval methods = EMPTY;
      if (inout) {
        lispval in = inout->car, out = inout->cdr;
        if (kno_hashset_get(KNO_XHASHSET(in),value)) {
          kno_decref(((lispval)inout));
          return 1;}
        else if (kno_hashset_get(KNO_XHASHSET(out),value)) {
          kno_decref(((lispval)inout));
          return 0;}
        else methods = get_slotid_methods(slotid,test_methods);}
      else methods = get_slotid_methods(slotid,test_methods);
      if (VOIDP(methods)) {
        if (inout) kno_decref(((lispval)inout));
        return 0;}
      else if (EMPTYP(methods)) {
        lispval values = kno_frame_get(f,slotid);
        result = kno_overlapp(value,values);
        kno_decref(values);}
      else {
        lispval args[3];
        args[0]=f; args[1]=slotid; args[2]=value;
        kno_push_opstack(&fop);
        init_dependencies(&fop);
        {
          DO_CHOICES(method,methods) {
            struct KNO_FUNCTION *fn = lookup_method(method);
            if (fn) {
              lispval v = kno_apply((lispval)fn,3,args);
              if (KNO_ABORTP(v)) {
                kno_decref(methods);
                if (inout) kno_decref(((lispval)inout));
                kno_pop_opstack(&fop,0);
                return kno_interr(v);}
              else if (KNO_TRUEP(v)) {
                result = 1;
                kno_decref(v);
                break;}
              else {}}}}
        if (!(kno_ipeval_failp())) {
          lispval factoid = kno_make_list(3,kno_incref(f),
                                         kno_incref(slotid),
                                         kno_incref(value));
          record_dependencies(&fop,factoid);
          kno_decref(factoid);
          kno_pop_opstack(&fop,0);}
        else kno_pop_opstack(&fop,0);}
      if (inout==NULL) {}
      else if (result)
        kno_hashset_add(KNO_XHASHSET(inout->car),value);
      else kno_hashset_add(KNO_XHASHSET(inout->cdr),value);
      if (inout) kno_decref(((lispval)inout));
      return result;}}
  else if (EMPTYP(f)) return 0;
  else {
    struct KNO_FRAMEOP_STACK *ptr = get_opstack();
    if (ptr) note_dependency(ptr,f,slotid,value);
    return kno_oid_test(f,slotid,value);}
}

KNO_EXPORT int kno_frame_add(lispval f,lispval slotid,lispval value)
{
  if (OIDP(slotid)) {
    struct KNO_FRAMEOP_STACK fop;
    KNO_INIT_FRAMEOP_STACK_ENTRY(fop,kno_addop,f,slotid,value);
    if (kno_in_progressp(&fop)) return 0;
    else {
      lispval methods = kno_oid_get(slotid,add_effects,EMPTY);
      kno_decache(f,slotid,value);
      if (EMPTYP(methods)) {
        return kno_oid_add(f,slotid,value);}
      else {
        lispval args[3];
        kno_push_opstack(&fop);
        args[0]=f; args[1]=slotid; args[2]=value;
        {
          DO_CHOICES(method,methods) {
            struct KNO_FUNCTION *fn = lookup_method(method);
            if (fn) {
              lispval v = kno_apply((lispval)fn,3,args);
              if (KNO_ABORTP(v)) {
                kno_pop_opstack(&fop,0);
                kno_decref(methods);
                return kno_interr(v);}
              else kno_decref(v);}}}
        kno_pop_opstack(&fop,1);
        return 1;}}}
  else if (EMPTYP(f)) return 0;
  else {
    kno_decache(f,slotid,value);
    return kno_oid_add(f,slotid,value);}
}

KNO_EXPORT int kno_frame_drop(lispval f,lispval slotid,lispval value)
{
  if (OIDP(slotid)) {
    struct KNO_FRAMEOP_STACK fop;
    KNO_INIT_FRAMEOP_STACK_ENTRY(fop,kno_dropop,f,slotid,value);
    if (kno_in_progressp(&fop)) return 0;
    else {
      lispval methods = kno_oid_get(slotid,drop_effects,EMPTY);
      kno_decache(f,slotid,value);
      if (EMPTYP(methods)) {
        kno_oid_drop(f,slotid,value);
        return 1;}
      else {
        lispval args[3];
        kno_clear_slotcache_entry(slotid,f);
        kno_push_opstack(&fop);
        args[0]=f; args[1]=slotid; args[2]=value;
        {
          DO_CHOICES(method,methods) {
            struct KNO_FUNCTION *fn = lookup_method(method);
            if (fn) {
              lispval v = kno_apply((lispval)fn,3,args);
              if (KNO_ABORTP(v)) {
                kno_pop_opstack(&fop,0);
                kno_decref(methods);
                return kno_interr(v);}
              else kno_decref(v);}}}
        kno_pop_opstack(&fop,1);
        return 1;}}}
  else if (EMPTYP(f)) return 0;
  else {
    kno_decache(f,slotid,value);
    return kno_oid_drop(f,slotid,value);}
}


/* Creating frames */

KNO_EXPORT lispval kno_new_frame(lispval pool_spec,lispval initval,int copyflags)
{
  kno_pool p; lispval oid;
  /* #f no pool, just create a slotmap
     #t use the default pool
     pool (use the pool!) */
  if (FALSEP(pool_spec))
    return kno_empty_slotmap();
  else if ((KNO_DEFAULTP(pool_spec)) || (VOIDP(pool_spec)))
    if (kno_default_pool) p = kno_default_pool;
    else return kno_err(_("No default pool"),"kno_new_frame",NULL,VOID);
  else if ((KNO_TRUEP(pool_spec))||(pool_spec == KNO_FIXZERO))
    p = kno_zero_pool;
  else if ((p = kno_lisp2pool(pool_spec)) == NULL)
    return kno_err(kno_NoSuchPool,"kno_new_frame",NULL,pool_spec);
  /* At this point, we have p!=NULL and we get an OID */
  oid = kno_pool_alloc(p,1);
  if (KNO_ABORTP(oid))
    return oid;
  else if (!(KNO_OIDP(oid)))
    return kno_err("PoolAllocFailed","kno_new_frame",p->poolid,VOID);
  else {}
  /* Now we figure out what to store in the OID */
  if (VOIDP(initval)) {
    if (kno_hashtable_probe(&(p->pool_changes),oid))
      initval = kno_hashtable_get(&(p->pool_changes),oid,KNO_VOID);
    else if (kno_hashtable_probe(&(p->pool_cache),oid))
      initval = kno_hashtable_get(&(p->pool_cache),oid,KNO_VOID);
    else NO_ELSE;
    if ( (KNO_IMMEDIATEP(initval)) &&
         ( (initval == KNO_VOID) ||
           (initval == KNO_EMPTY_CHOICE) ||
           (initval == KNO_LOCKHOLDER) ||
           (initval == KNO_DEFAULT_VALUE) ||
           (initval == KNO_UNALLOCATED_OID) ) )
      initval = kno_empty_slotmap();}
  else if ((OIDP(initval)) && (copyflags)) {
    /* Avoid aliasing */
    lispval oidval = kno_oid_value(initval);
    if (KNO_ABORTP(oidval)) return oidval;
    initval = kno_copier(oidval,copyflags);
    kno_decref(oidval);}
  else if (copyflags)
    initval = kno_deep_copy(initval);
  else kno_incref(initval);
  /* Now we actually set the OID's value */
  if ((kno_set_oid_value(oid,initval))<0) {
    kno_decref(initval);
    return KNO_ERROR;}
  else {
    kno_decref(initval);
    return oid;}
}

/* Checking the cache load for the slot/test caches */

static int hashtable_cachecount(lispval key,lispval v,void *ptr)
{
  if (HASHTABLEP(v)) {
    kno_hashtable h = (kno_hashtable)v;
    int *count = (int *)ptr;
    *count = *count+h->table_n_keys;}
  return 0;
}

KNO_EXPORT int kno_slot_cache_load()
{
  int count = 0;
  kno_for_hashtable(&slot_caches,hashtable_cachecount,&count,1);
  kno_for_hashtable(&test_caches,hashtable_cachecount,&count,1);
  return count;
}


/* Initialization */

KNO_EXPORT void kno_init_frames_c()
{
  u8_register_source_file(_FILEINFO);

  get_methods = kno_intern("get-methods");
  compute_methods = kno_intern("compute-methods");
  test_methods = kno_intern("test-methods");
  add_effects = kno_intern("add-effects");
  drop_effects = kno_intern("drop-effects");

  KNO_INIT_STATIC_CONS(&slot_caches,kno_hashtable_type);
  KNO_INIT_STATIC_CONS(&test_caches,kno_hashtable_type);
  kno_make_hashtable(&slot_caches,17);
  kno_make_hashtable(&test_caches,17);

  KNO_INIT_STATIC_CONS(&implications,kno_hashtable_type);
  kno_make_hashtable(&implications,17);

  u8_init_mutex(&slotcache_lock);

#if KNO_USE_TLS
  u8_new_threadkey(&opstack_key,NULL);
#endif

}

