/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_POOLS 1
#define FD_INLINE_INDEXES 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_IPEVAL 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/apply.h"

#include <stdarg.h>

static u8_condition OddFindFramesArgs=_("Odd number of args to find frames");

static struct FD_HASHTABLE slot_caches, test_caches;
static lispval get_methods, compute_methods, test_methods;
static lispval add_effects, drop_effects;

static u8_mutex slotcache_lock;


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
        (LISP_EQUAL(ops->value,op->value)))
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

static struct FD_HASHTABLE *make_slot_cache(lispval slotid)
{
  lispval table = fd_make_hashtable(NULL,17);
  u8_lock_mutex(&slotcache_lock);
  fd_hashtable_store(&slot_caches,slotid,table);
  u8_unlock_mutex(&slotcache_lock);
  return (struct FD_HASHTABLE *)table;
}

static struct FD_HASHTABLE *make_test_cache(lispval slotid)
{
  lispval table = fd_make_hashtable(NULL,17);
  u8_lock_mutex(&slotcache_lock);
  fd_hashtable_store(&test_caches,slotid,table);
  u8_unlock_mutex(&slotcache_lock);
  return (struct FD_HASHTABLE *)table;
}

FD_EXPORT void fd_clear_slotcache(lispval slotid)
{
  u8_lock_mutex(&slotcache_lock);
  if (fd_hashtable_probe(&slot_caches,slotid))
    fd_hashtable_store(&slot_caches,slotid,VOID);
  if (fd_hashtable_probe(&test_caches,slotid))
    fd_hashtable_store(&test_caches,slotid,VOID);
  u8_unlock_mutex(&slotcache_lock);
}

FD_EXPORT void fd_clear_slotcache_entry(lispval frame,lispval slotid)
{
  lispval slotcache = fd_hashtable_get(&slot_caches,slotid,VOID);
  lispval testcache = fd_hashtable_get(&test_caches,slotid,VOID);
  if (HASHTABLEP(slotcache))
    fd_hashtable_op(FD_XHASHTABLE(slotcache),fd_table_replace,frame,VOID);
  /* Need to do this more selectively. */
  if (HASHTABLEP(testcache))
    fd_hashtable_op(FD_XHASHTABLE(testcache),fd_table_replace,frame,VOID);
}

FD_EXPORT void fd_clear_testcache_entry(lispval frame,lispval slotid,lispval value)
{
  lispval testcache = fd_hashtable_get(&test_caches,slotid,VOID);
  if (HASHTABLEP(testcache)) {
    if (VOIDP(value))
      fd_hashtable_op(FD_XHASHTABLE(testcache),fd_table_replace,frame,VOID);
    else {
      lispval cache = fd_hashtable_get(FD_XHASHTABLE(testcache),frame,EMPTY);
      if (PAIRP(cache)) {
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
   FD_DEPENDENCY_RECORD structures), with an VOID value distinguishing the
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

static void note_dependency
(fd_frameop_stack *cxt,lispval frame,lispval slotid,lispval value)
{
  fd_frameop_stack *scan = cxt;
  while (scan)
    if (scan->dependencies) {
      int n = scan->n_deps; struct FD_DEPENDENCY_RECORD *records = NULL;
      if (scan->n_deps>=scan->max_deps) {
        scan->dependencies = records=
          u8_realloc_n(scan->dependencies,scan->max_deps*2,
                       struct FD_DEPENDENCY_RECORD);
        scan->max_deps = scan->max_deps*2;}
      else records = scan->dependencies;
      records[n].frame = fd_incref(frame);
      records[n].slotid = fd_incref(slotid);
      records[n].value = fd_incref(value);
      scan->n_deps = n+1;
      return;}
    else scan = scan->next;
}

static void record_dependencies(fd_frameop_stack *cxt,lispval factoid)
{
  if (cxt->dependencies) {
    int i = 0, n = cxt->n_deps;
    struct FD_DEPENDENCY_RECORD *records = cxt->dependencies;
    lispval *factoids = u8_alloc_n(n,lispval);
    while (i<n) {
      lispval depends;
      if (VOIDP(records[i].value))
        depends = fd_conspair(records[i].frame,records[i].slotid);
      else depends = fd_make_list(3,records[i].frame,records[i].slotid,
                                  fd_incref(records[i].value));
      factoids[i++]=depends;}
    fd_hashtable_iterkeys(&implications,fd_table_add,n,factoids,factoid);
    i = 0; while (i<n) {lispval factoid = factoids[i++]; fd_decref(factoid);}
    u8_free(factoids);}
}

static void decache_factoid(lispval factoid)
{
  lispval frame = FD_CAR(factoid);
  lispval predicate = FD_CDR(factoid);
  if (PAIRP(predicate)) {
    lispval value = FD_CDR(predicate);
    fd_clear_slotcache_entry(frame,predicate);
    fd_clear_testcache_entry(frame,predicate,value);}
  else {
    fd_clear_slotcache_entry(frame,predicate);
    fd_clear_testcache_entry(frame,predicate,VOID);}
}

static void decache_implications(lispval factoid)
{
  lispval implies = fd_hashtable_get(&implications,factoid,EMPTY);
  decache_factoid(factoid);
  if (!(EMPTYP(implies)))
    fd_hashtable_store(&implications,factoid,EMPTY);
  {DO_CHOICES(imply,implies) decache_implications(imply);}
  fd_decref(implies);
}

FD_EXPORT void fd_decache(lispval frame,lispval slotid,lispval value)
{
  lispval factoid;
  if (!(VOIDP(value))) {
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

static struct FD_FUNCTION *lookup_method(lispval arg)
{
  if (FD_FUNCTIONP(arg))
    return FD_XFUNCTION(arg);
  else if (SYMBOLP(arg)) {
    lispval lookup=
      fd_hashtable_get(FD_XHASHTABLE(fd_method_table),arg,EMPTY);
    if (FD_FUNCTIONP(lookup)) {
      struct FD_FUNCTION *fn = FD_XFUNCTION(lookup); fd_decref(lookup);
      return fn;}
    else return NULL;}
  else return NULL;
}

static lispval get_slotid_methods(lispval slotid,lispval method_name)
{
  fd_pool p = fd_oid2pool(slotid); lispval smap, result;
  if (PRED_FALSE(p == NULL))
    return VOID;
  else smap = fd_fetch_oid(p,slotid);
  if (FD_ABORTP(smap))
    return smap;
  else if (SLOTMAPP(smap))
    result = fd_slotmap_get((fd_slotmap)smap,method_name,EMPTY);
  else if (SCHEMAPP(smap))
    result = fd_schemap_get((fd_schemap)smap,method_name,EMPTY);
  else if (TABLEP(smap))
    result = fd_get(smap,slotid,EMPTY);
  else result = VOID;
  fd_decref(smap);
  return result;
}


/* Frame Operations */

FD_EXPORT lispval fd_frame_get(lispval f,lispval slotid)
{
  if (OIDP(slotid)) {
    struct FD_FRAMEOP_STACK fop;
    FD_INIT_FRAMEOP_STACK_ENTRY(fop,fd_getop,f,slotid,VOID);
    if (fd_in_progressp(&fop)) return EMPTY;
    else {
      int ipestate = fd_ipeval_status();
      struct FD_HASHTABLE *cache;
      lispval methods, cachev, cached, computed = EMPTY;
      cachev = fd_hashtable_get(&slot_caches,slotid,VOID);
      if (VOIDP(cachev)) {
        cache = make_slot_cache(slotid);
        cached = VOID; cachev = (lispval)cache;}
      else if (EMPTYP(cachev)) {
        cache = NULL; cached = VOID;}
      else {
        cache = FD_XHASHTABLE(cachev);
        cached = fd_hashtable_get(cache,f,VOID);}
      if (!(VOIDP(cached)))
        return cached;
      methods = get_slotid_methods(slotid,get_methods);
      /* Methods will be void only if the value of the slotoid isn't a table,
         which is typically only the case when it hasn't been fetched yet. */
      note_dependency(&fop,f,slotid,VOID);
      if (VOIDP(methods))
        return EMPTY;
      else if (EMPTYP(methods)) {
        lispval value = fd_oid_get(f,slotid,EMPTY);
        if (EMPTYP(value))
          methods = get_slotid_methods(slotid,compute_methods);
        else return value;}
      if (VOIDP(methods)) return EMPTY;
      else if (FD_ABORTP(methods)) {
        fd_decref(cachev);
        return methods;}
      /* At this point, we're computing the slot value */
      fd_push_opstack(&fop);
      init_dependencies(&fop);
      {DO_CHOICES(method,methods) {
          struct FD_FUNCTION *fn = lookup_method(method);
          if (fn) {
            lispval args[2], value; args[0]=f; args[1]=slotid;
            value = fd_finish_call(fd_dapply((lispval)fn,2,args));
            if (FD_ABORTP(value)) {
              fd_decref(computed); fd_decref(methods);
              fd_pop_opstack(&fop,0);
              fd_decref(cachev);
              return value;}
            CHOICE_ADD(computed,value);}}}
      computed = fd_simplify_choice(computed);
      fd_decref(methods);
      if ((cache) && ((fd_ipeval_status() == ipestate))) {
        lispval factoid = fd_conspair(fd_incref(f),fd_incref(slotid));
        record_dependencies(&fop,factoid); fd_decref(factoid);
        fd_hashtable_store(cache,f,computed);
        fd_pop_opstack(&fop,1);}
      else fd_pop_opstack(&fop,0);
      fd_decref(cachev);
      return computed;}}
  else if (EMPTYP(f)) return EMPTY;
  else {
    struct FD_FRAMEOP_STACK *ptr = get_opstack();
    if (ptr) note_dependency(ptr,f,slotid,VOID);
    return fd_oid_get(f,slotid,EMPTY);}
}

FD_EXPORT int fd_frame_test(lispval f,lispval slotid,lispval value)
{
  if (!(OIDP(f)))
    return fd_test(f,slotid,value);
  else if (OIDP(slotid)) {
    struct FD_FRAMEOP_STACK fop;
    FD_INIT_FRAMEOP_STACK_ENTRY(fop,fd_testop,f,slotid,value);
    if (fd_in_progressp(&fop)) return 0;
    else if (VOIDP(value)) {
      lispval values = fd_frame_get(f,slotid);
      note_dependency(&fop,f,slotid,value);
      if (EMPTYP(values))
        return 0;
      else return 1;}
    else {
      struct FD_HASHTABLE *cache; int result = 0;
      lispval cachev = fd_hashtable_get(&test_caches,slotid,VOID);
      lispval cached, methods;
      if (VOIDP(cachev)) {
        cache = make_test_cache(slotid); cachev = (lispval)cache;
        cached = fd_conspair(fd_make_hashset(),fd_make_hashset());
        fd_hashtable_store(cache,f,cached);}
      else if (EMPTYP(cachev)) {
        cache = NULL; cached = VOID;}
      else {
        cache = FD_XHASHTABLE(cachev);
        cached = fd_hashtable_get(cache,f,VOID);
        if (VOIDP(cached)) {
          cached = fd_conspair(fd_make_hashset(),fd_make_hashset());
          fd_hashtable_store(cache,f,cached);}}
      if (PAIRP(cached)) {
        lispval in = FD_CAR(cached), out = FD_CDR(cached);
        if (fd_hashset_get(FD_XHASHSET(in),value)) {
          fd_decref(cached); fd_decref(cachev);
          return 1;}
        else if (fd_hashset_get(FD_XHASHSET(out),value)) {
          fd_decref(cached); fd_decref(cachev);
          return 0;}
        else methods = get_slotid_methods(slotid,test_methods);}
      else methods = get_slotid_methods(slotid,test_methods);
      if (VOIDP(methods)) {
        fd_decref(cached); fd_decref(cachev);
        return 0;}
      else if (EMPTYP(methods)) {
        lispval values = fd_frame_get(f,slotid);
        result = fd_overlapp(value,values);
        fd_decref(values);}
      else {
        lispval args[3];
        args[0]=f; args[1]=slotid; args[2]=value;
        fd_push_opstack(&fop);
        init_dependencies(&fop);
        {
          DO_CHOICES(method,methods) {
            struct FD_FUNCTION *fn = lookup_method(method);
            if (fn) {
              lispval v = fd_apply((lispval)fn,3,args);
              if (FD_ABORTP(v)) {
                fd_decref(methods); fd_decref(cachev);
                fd_pop_opstack(&fop,0);
                return fd_interr(v);}
              else if (FD_TRUEP(v)) {
                result = 1; fd_decref(v); break;}
              else {}}}}
        if ((cache) && (!(fd_ipeval_failp()))) {
          lispval factoid = fd_make_list(3,fd_incref(f),
                                         fd_incref(slotid),
                                         fd_incref(value));
          record_dependencies(&fop,factoid); fd_decref(factoid);
          fd_pop_opstack(&fop,0);}
        else fd_pop_opstack(&fop,0);}
      if (result) fd_hashset_add(FD_XHASHSET(FD_CAR(cached)),value);
      else fd_hashset_add(FD_XHASHSET(FD_CDR(cached)),value);
      fd_decref(cached); fd_decref(cachev);
      return result;}}
  else if (EMPTYP(f)) return 0;
  else {
    struct FD_FRAMEOP_STACK *ptr = get_opstack();
    if (ptr) note_dependency(ptr,f,slotid,value);
    return fd_oid_test(f,slotid,value);}
}

FD_EXPORT int fd_frame_add(lispval f,lispval slotid,lispval value)
{
  if (OIDP(slotid)) {
    struct FD_FRAMEOP_STACK fop;
    FD_INIT_FRAMEOP_STACK_ENTRY(fop,fd_addop,f,slotid,value);
    if (fd_in_progressp(&fop)) return 0;
    else {
      lispval methods = fd_oid_get(slotid,add_effects,EMPTY);
      fd_decache(f,slotid,value);
      if (EMPTYP(methods)) {
        return fd_oid_add(f,slotid,value);}
      else {
        lispval args[3];
        fd_push_opstack(&fop);
        args[0]=f; args[1]=slotid; args[2]=value;
        {
          DO_CHOICES(method,methods) {
            struct FD_FUNCTION *fn = lookup_method(method);
            if (fn) {
              lispval v = fd_apply((lispval)fn,3,args);
              if (FD_ABORTP(v)) {
                fd_pop_opstack(&fop,0);
                fd_decref(methods);
                return fd_interr(v);}
              else fd_decref(v);}}}
        fd_pop_opstack(&fop,1);
        return 1;}}}
  else if (EMPTYP(f)) return 0;
  else {
    fd_decache(f,slotid,value);
    return fd_oid_add(f,slotid,value);}
}

FD_EXPORT int fd_frame_drop(lispval f,lispval slotid,lispval value)
{
  if (OIDP(slotid)) {
    struct FD_FRAMEOP_STACK fop;
    FD_INIT_FRAMEOP_STACK_ENTRY(fop,fd_dropop,f,slotid,value);
    if (fd_in_progressp(&fop)) return 0;
    else {
      lispval methods = fd_oid_get(slotid,drop_effects,EMPTY);
      fd_decache(f,slotid,value);
      if (EMPTYP(methods)) {
        fd_oid_drop(f,slotid,value);
        return 1;}
      else {
        lispval args[3];
        fd_clear_slotcache_entry(slotid,f);
        fd_push_opstack(&fop);
        args[0]=f; args[1]=slotid; args[2]=value;
        {
          DO_CHOICES(method,methods) {
            struct FD_FUNCTION *fn = lookup_method(method);
            if (fn) {
              lispval v = fd_apply((lispval)fn,3,args);
              if (FD_ABORTP(v)) {
                fd_pop_opstack(&fop,0);
                fd_decref(methods);
                return fd_interr(v);}
              else fd_decref(v);}}}
        fd_pop_opstack(&fop,1);
        return 1;}}}
  else if (EMPTYP(f)) return 0;
  else {
    fd_decache(f,slotid,value);
    return fd_oid_drop(f,slotid,value);}
}


/* Creating frames */

FD_EXPORT lispval fd_new_frame(lispval pool_spec,lispval initval,int copyflags)
{
  fd_pool p; lispval oid;
  /* #f no pool, just create a slotmap
     #t use the default pool
     pool (use the pool!) */
  if (FALSEP(pool_spec))
    return fd_empty_slotmap();
  else if ((FD_DEFAULTP(pool_spec)) || (VOIDP(pool_spec)))
    if (fd_default_pool) p = fd_default_pool;
    else return fd_err(_("No default pool"),"frame_create_lexpr",NULL,VOID);
  else if ((FD_TRUEP(pool_spec))||(pool_spec == FD_FIXZERO))
    p = fd_zero_pool;
  else if ((p = fd_lisp2pool(pool_spec)) == NULL)
    return fd_err(fd_NoSuchPool,"frame_create_lexpr",NULL,pool_spec);
  /* At this point, we have p!=NULL and we get an OID */
  oid = fd_pool_alloc(p,1);
  if (FD_ABORTP(oid)) return oid;
  /* Now we figure out what to store in the OID */
  if (VOIDP(initval)) initval = fd_empty_slotmap();
  else if ((OIDP(initval)) && (copyflags)) {
    /* Avoid aliasing */
    lispval oidval = fd_oid_value(initval);
    if (FD_ABORTP(oidval)) return oidval;
    initval = fd_copier(oidval,copyflags);
    fd_decref(oidval);}
  else if (copyflags)
    initval = fd_deep_copy(initval);
  else fd_incref(initval);
  /* Now we actually set the OID's value */
  if ((fd_set_oid_value(oid,initval))<0) {
    fd_decref(initval);
    return FD_ERROR;}
  else {
    fd_decref(initval);
    return oid;}
}

/* Searching */

static lispval make_features(lispval slotids,lispval values)
{
  if (EMPTYP(values))
    return values;
  else {
    lispval results = EMPTY;
    DO_CHOICES(slotid,slotids) {
      DO_CHOICES(value,values) {
        lispval feature = fd_make_pair(slotid,value);
        CHOICE_ADD(results,feature);}}
    return results;}
}

static lispval aggregate_prim_find
(fd_aggregate_index ax,lispval slotids,lispval values);

static lispval index_prim_find(fd_index ix,lispval slotids,lispval values)
{
  if (fd_aggregate_indexp(ix))
    return aggregate_prim_find((fd_aggregate_index)ix,slotids,values);
  lispval combined = FD_EMPTY_CHOICE;
  lispval keyslot = ix->index_keyslot;
  DO_CHOICES(slotid,slotids) {
    if (slotid == keyslot) {
      if (FD_CHOICEP(values))
        fd_index_prefetch(ix,values);
      DO_CHOICES(value,values) {
        lispval result = fd_index_get(ix,value);
        if (FD_ABORTP(result)) {
          FD_STOP_DO_CHOICES;
          fd_decref(combined);
          return result;}
        CHOICE_ADD(combined,result);}}
    else {
      DO_CHOICES(value,values) {
        lispval key = fd_make_pair(slotid,value);
        lispval result = fd_index_get(ix,key);
        if (FD_ABORTP(result)) {
          FD_STOP_DO_CHOICES;
          fd_decref(combined);
          return result;}
        CHOICE_ADD(combined,result);
        fd_decref(key);}}}
  return combined;
}

static lispval aggregate_prim_find
(fd_aggregate_index ax,lispval slotids,lispval values)
{
  lispval combined = FD_EMPTY;
  int i = 0, n =ax->ax_n_indexes;
  fd_index *indexes = ax->ax_indexes;
  while (i<n) {
    fd_index ex = indexes[i++];
    lispval v = index_prim_find(ex,slotids,values);
    if (FD_ABORTP(v)) {
      fd_decref(combined);
      return v;}
    CHOICE_ADD(combined,v);}
  return combined;
}

FD_EXPORT lispval fd_prim_find(lispval indexes,lispval slotids,lispval values)
{
  if (CHOICEP(indexes)) {
    lispval combined = EMPTY;
    DO_CHOICES(index,indexes) {
      if (FD_INDEXP(index)) {
        fd_index ix = fd_indexptr(index);
        lispval indexed = index_prim_find(ix,slotids,values);
        if (FD_ABORTP(indexed)) {
          fd_decref(combined);
          return indexed;}
        CHOICE_ADD(combined,indexed);}
      else if (TABLEP(index)) {
        DO_CHOICES(slotid,slotids) {
          DO_CHOICES(value,values) {
            lispval key = fd_make_pair(slotid,value);
            lispval result = fd_get(index,key,EMPTY);
            if (FD_ABORTP(result)) {
              fd_decref(combined);
              return result;}
            CHOICE_ADD(combined,result);
            fd_decref(key);}}}
      else {
        fd_decref(combined);
        return fd_type_error(_("index"),"fd_prim_find",index);}}
    return combined;}
  else if (FD_INDEXP(indexes))
    return index_prim_find(fd_indexptr(indexes),slotids,values);
  else if (TABLEP(indexes)) {
    lispval combined = EMPTY;
    DO_CHOICES(slotid,slotids) {
      DO_CHOICES(value,values) {
        lispval key = fd_make_pair(slotid,value);
        lispval result = fd_get(indexes,key,EMPTY);
        if (FD_ABORTP(result)) {
          fd_decref(combined);
          return result;}
        CHOICE_ADD(combined,result);
        fd_decref(key);}}
    return combined;}
  else return fd_type_error("index/table","fd_prim_find",indexes);
}

FD_EXPORT lispval fd_finder(lispval indexes,int n,lispval *slotvals)
{
  int i = 0, n_conjuncts = n/2;
  lispval _conjuncts[6], *conjuncts=_conjuncts, result;
  if (EMPTYP(indexes)) return EMPTY;
  if (n_conjuncts>6) conjuncts = u8_alloc_n(n_conjuncts,lispval);
  while (i < n_conjuncts) {
    conjuncts[i]=fd_prim_find(indexes,slotvals[i*2],slotvals[i*2+1]);
    if (FD_ABORTP(conjuncts[i])) {
      lispval error = conjuncts[i];
      int j = 0; while (j<i) {fd_decref(conjuncts[j]); j++;}
      if (conjuncts != _conjuncts) u8_free(conjuncts);
      return error;}
    if (EMPTYP(conjuncts[i])) {
      int j = 0; while (j<i) {fd_decref(conjuncts[j]); j++;}
      return EMPTY;}
    i++;}
  result = fd_intersection(conjuncts,n_conjuncts);
  i = 0; while (i < n_conjuncts) {
    lispval cj=_conjuncts[i++]; fd_decref(cj);}
  if (_conjuncts != conjuncts) u8_free(conjuncts);
  return result;
}

FD_EXPORT lispval fd_find_frames(lispval indexes,...)
{
  lispval _slotvals[64], *slotvals=_slotvals, val;
  int n_slotvals = 0, max_slotvals = 64; va_list args;
  va_start(args,indexes); val = va_arg(args,lispval);
  while (!(VOIDP(val))) {
    if (n_slotvals>=max_slotvals) {
      if (max_slotvals == 64) {
        lispval *newsv = u8_alloc_n(128,lispval); int i = 0;
        while (i<64) {newsv[i]=slotvals[i]; i++;}
        slotvals = newsv; max_slotvals = 128;}
      else {
        slotvals = u8_realloc(slotvals,LISPVEC_BYTELEN(max_slotvals)*2);
        max_slotvals = max_slotvals*2;}}
    slotvals[n_slotvals++]=val; val = va_arg(args,lispval);}
  if (n_slotvals%2) {
    if (slotvals != _slotvals) u8_free(slotvals);
    return fd_err(OddFindFramesArgs,"fd_find_frames",NULL,VOID);}
  if (slotvals != _slotvals) {
    lispval result = fd_finder(indexes,n_slotvals,slotvals);
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
int fd_find_prefetch(fd_index ix,lispval slotids,lispval values)
{
  lispval keyslot = ix->index_keyslot;
  if ((ix->index_handler->fetchn) == NULL) {
    lispval keys = EMPTY;
    DO_CHOICES(slotid,slotids) {
      if (slotid == keyslot) {
        fd_incref(values);
        CHOICE_ADD(keys,values);}
      else {
        DO_CHOICES(value,values) {
          lispval key = fd_conspair(slotid,value);
          CHOICE_ADD(keys,key);}}}
    fd_index_prefetch(ix,keys);
    fd_decref(keys);
    return 1;}
  else {
    int max_keys = FD_CHOICE_SIZE(slotids)*FD_CHOICE_SIZE(values);
    lispval *keyv = u8_alloc_n(max_keys,lispval);
    lispval *valuev = NULL;
    int n_keys = 0;
    DO_CHOICES(slotid,slotids) {
      if (keyslot == slotid) {
        DO_CHOICES(value,values) {
          keyv[n_keys++]=value;
          fd_incref(value);}}
      else {
        DO_CHOICES(value,values) {
          lispval key = fd_conspair(slotid,value);
          keyv[n_keys++]=key;}}}
    valuev = (ix->index_handler->fetchn)(ix,n_keys,keyv);
    fd_hashtable_iter(&(ix->index_cache),fd_table_add_empty_noref,
                      n_keys,keyv,valuev);
    u8_free(keyv); u8_free(valuev);
    return 1;}
}


/* Indexing frames */

#define FD_LOOP_BREAK() FD_STOP_DO_CHOICES; break

static fd_index get_writable_slotindex(fd_index ix,lispval slotid)
{
  if (fd_aggregate_indexp(ix)) {
    fd_index writable = NULL, generic = NULL;
    struct FD_AGGREGATE_INDEX *aix = (fd_aggregate_index) ix;
    fd_index *indexes = aix->ax_indexes;
    int i = 0, n = aix->ax_n_indexes; while (i<n) {
      fd_index possible = indexes[i++];
      fd_index use_front = fd_get_writable_index(possible);
      if (use_front) {
        if (possible->index_keyslot == slotid) {
          lispval ptr = fd_index2lisp(use_front);
          if (FD_ABORTP(ptr))
            return NULL;
          else {
            if (FD_CONSP(ptr)) {fd_incref(ptr);}
            return (fd_index) possible;}}
        else if ( (FD_VOIDP(use_front->index_keyslot)) ||
                  (FD_FALSEP(use_front->index_keyslot)) ||
                  (FD_EMPTYP(use_front->index_keyslot)) ) {
          if (generic == NULL) generic = use_front;
          if (writable) writable = use_front;}
        else if (writable == NULL)
          writable = use_front;
        else NO_ELSE;}}
    if (generic) {
      lispval ptr = fd_index2lisp(generic);
      if (FD_CONSP(ptr)) fd_incref(ptr);
      return generic;}
    else if (writable) {
      lispval ptr = fd_index2lisp(writable);
      if (FD_CONSP(ptr)) fd_incref(ptr);
      return writable;}
    else return NULL;}
  else return fd_get_writable_index(ix);
}


FD_EXPORT
int fd_index_frame(fd_index ix,lispval frames,lispval slotids,lispval values)
{
  int rv = 0;
  DO_CHOICES(slotid,slotids) {
    fd_index write_index = get_writable_slotindex(ix,slotid);
    lispval keyslot = write_index->index_keyslot;
    DO_CHOICES(frame,frames) {
      int add_rv = 0;
      lispval use_values = (FD_VOIDP(values)) ?
        (fd_frame_get(frame,slotid)) : (values);
      if (FD_ABORTP(use_values)) add_rv = -1;
      else if (FD_EMPTYP(use_values)) {}
      else if (slotid == keyslot)
        add_rv = fd_index_add(write_index,use_values,frame);
      else {
        lispval features = FD_EMPTY;
        FD_DO_CHOICES(value,use_values) {
          lispval feature = fd_init_pair(NULL,slotid,value);
          fd_incref(slotid); fd_incref(value);
          CHOICE_ADD(features,feature);}
        add_rv = fd_index_add(write_index,features,frame);
        fd_decref(features);}
      if (use_values != values) fd_decref(use_values);
      if (add_rv < 0) { rv = -1; FD_LOOP_BREAK();}
      else rv += add_rv;}
    lispval ptr = fd_index2lisp(write_index);
    fd_decref(ptr);
    if (rv<0) { FD_LOOP_BREAK(); }}
  return rv;
}

/* Background searching */

FD_EXPORT lispval fd_bg_get(lispval slotid,lispval value)
{
  if (fd_background) {
    lispval results = EMPTY;
    lispval features = make_features(slotid,value);
    if ((fd_prefetch) && (fd_ipeval_status()==0) &&
        (CHOICEP(features)) &&
        (fd_index_prefetch((fd_index)fd_background,features)<0)) {
      fd_decref(features); return FD_ERROR;}
    else {
      DO_CHOICES(feature,features) {
        lispval result = fd_index_get((fd_index)fd_background,feature);
        if (FD_ABORTP(result)) {
          fd_decref(results); fd_decref(features);
          return result;}
        else {CHOICE_ADD(results,result);}}
      fd_decref(features);}
    return fd_simplify_choice(results);}
  else return EMPTY;
}

FD_EXPORT lispval fd_bgfinder(int n,lispval *slotvals)
{
  int i = 0, n_conjuncts = n/2;
  lispval _conjuncts[6], *conjuncts=_conjuncts, result;
  if (fd_background == NULL) return EMPTY;
  if (n_conjuncts>6) conjuncts = u8_alloc_n(n_conjuncts,lispval);
  while (i < n_conjuncts) {
    _conjuncts[i]=fd_bg_get(slotvals[i*2],slotvals[i*2+1]); i++;}
  result = fd_intersection(conjuncts,n_conjuncts);
  i = 0; while (i < n_conjuncts) {
    lispval cj=_conjuncts[i++]; fd_decref(cj);}
  if (_conjuncts != conjuncts) u8_free(conjuncts);
  return result;
}

FD_EXPORT lispval fd_bgfind(lispval slotid,lispval values,...)
{
  lispval _slotvals[64], *slotvals=_slotvals, val;
  int n_slotvals = 0, max_slotvals = 64; va_list args;
  va_start(args,values); val = va_arg(args,lispval);
  slotvals[n_slotvals++]=slotid; slotvals[n_slotvals++]=values;
  while (!(VOIDP(val))) {
    if (n_slotvals>=max_slotvals) {
      if (max_slotvals == 64) {
        lispval *newsv = u8_alloc_n(128,lispval); int i = 0;
        while (i<64) {newsv[i]=slotvals[i]; i++;}
        slotvals = newsv; max_slotvals = 128;}
      else {
        slotvals = u8_realloc_n(slotvals,max_slotvals*2,lispval);
        max_slotvals = max_slotvals*2;}}
    slotvals[n_slotvals++]=val; val = va_arg(args,lispval);}
  if (n_slotvals%2) {
    if (slotvals != _slotvals) u8_free(slotvals);
    return fd_err(OddFindFramesArgs,"fd_bgfind",NULL,VOID);}
  if (slotvals != _slotvals) {
    lispval result = fd_bgfinder(n_slotvals,slotvals);
    u8_free(slotvals);
    return result;}
  else return fd_bgfinder(n_slotvals,slotvals);
}

FD_EXPORT int fd_bg_prefetch(lispval keys)
{
  if (fd_background)
    return fd_index_prefetch((fd_index)fd_background,keys);
  else return 0;
}

/* Checking the cache load for the slot/test caches */

static int hashtable_cachecount(lispval key,lispval v,void *ptr)
{
  if (HASHTABLEP(v)) {
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

  u8_init_mutex(&slotcache_lock);

#if FD_USE_TLS
  u8_new_threadkey(&opstack_key,NULL);
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
