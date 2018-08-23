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
  fd_decref(slotcache);
  fd_decref(testcache);
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
  fd_decref(testcache);
}


static struct FD_HASHTABLE *get_slot_cache(lispval slotid)
{
  lispval cachev = fd_hashtable_get(&slot_caches,slotid,FD_VOID);
  if (FD_VOIDP(cachev))
    return make_slot_cache(slotid);
  else if (FD_HASHTABLEP(cachev))
    return (fd_hashtable) cachev;
  else {
    fd_decref(cachev);
    return NULL;}
}

static struct FD_PAIR *get_test_cache(lispval f,lispval slotid)
{
  lispval cachev = fd_hashtable_get(&test_caches,slotid,FD_VOID);
  struct FD_HASHTABLE *cache = NULL;
  if (FD_VOIDP(cachev)) {
    cache = make_test_cache(slotid);
    cachev = (lispval) cache;}
  else if (FD_HASHTABLEP(cachev))
    cache = (fd_hashtable) cachev;
  else {
    fd_decref(cachev);
    cache = NULL;
    return NULL;}
  lispval inout = fd_hashtable_get(cache,f,FD_VOID);
  if (FD_PAIRP(inout)) {
    fd_decref(cachev);
    return (fd_pair) inout;}
  else if (FD_VOIDP(inout)) {
    inout = fd_init_pair(NULL,fd_make_hashset(),fd_make_hashset());
    if (fd_hashtable_op(cache,fd_table_default,f,inout) == 0) {
      fd_decref(inout);
      inout = fd_hashtable_get(cache,f,FD_VOID);}
    fd_decref(cachev);
    return (fd_pair) inout;}
  else {
    fd_decref(cachev);
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
      struct FD_HASHTABLE *cache = get_slot_cache(slotid);
      lispval methods, cached, computed = EMPTY;
      if (cache) {
        cached = fd_hashtable_get(cache,f,VOID);
        fd_decref(((lispval)cache));
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
        lispval value = fd_oid_get(f,slotid,EMPTY);
        if (EMPTYP(value))
          methods = get_slotid_methods(slotid,compute_methods);
        else return value;}
      if (VOIDP(methods))
        return EMPTY;
      else if (FD_ABORTP(methods))
        return methods;
      else NO_ELSE;
      /* At this point, we're computing the slot value */
      fd_push_opstack(&fop);
      init_dependencies(&fop);
      {DO_CHOICES(method,methods) {
          struct FD_FUNCTION *fn = lookup_method(method);
          if (fn) {
            lispval args[2], value; args[0]=f; args[1]=slotid;
            value = fd_finish_call(fd_dapply((lispval)fn,2,args));
            if (FD_ABORTP(value)) {
              fd_decref(computed);
              fd_decref(methods);
              fd_pop_opstack(&fop,0);
              return value;}
            CHOICE_ADD(computed,value);}}}
      computed = fd_simplify_choice(computed);
      fd_decref(methods);
      if (fd_ipeval_status() == ipestate) {
        if ((cache = get_slot_cache(slotid))) {
          lispval factoid = fd_conspair(fd_incref(f),fd_incref(slotid));
          record_dependencies(&fop,factoid);
          fd_decref(factoid);
          fd_hashtable_store(cache,f,computed);
          fd_decref(((lispval)cache));
          fd_pop_opstack(&fop,1);}
        else fd_pop_opstack(&fop,1);}
      else fd_pop_opstack(&fop,0);
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
      int result = 0;
      struct FD_PAIR *inout = get_test_cache(f,slotid);
      lispval methods = EMPTY;
      if (inout) {
        lispval in = inout->car, out = inout->cdr;
        if (fd_hashset_get(FD_XHASHSET(in),value)) {
          fd_decref(((lispval)inout));
          return 1;}
        else if (fd_hashset_get(FD_XHASHSET(out),value)) {
          fd_decref(((lispval)inout));
          return 0;}
        else methods = get_slotid_methods(slotid,test_methods);}
      else methods = get_slotid_methods(slotid,test_methods);
      if (VOIDP(methods)) {
        if (inout) fd_decref(((lispval)inout));
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
                fd_decref(methods);
                if (inout) fd_decref(((lispval)inout));
                fd_pop_opstack(&fop,0);
                return fd_interr(v);}
              else if (FD_TRUEP(v)) {
                result = 1;
                fd_decref(v);
                break;}
              else {}}}}
        if (!(fd_ipeval_failp())) {
          lispval factoid = fd_make_list(3,fd_incref(f),
                                         fd_incref(slotid),
                                         fd_incref(value));
          record_dependencies(&fop,factoid);
          fd_decref(factoid);
          fd_pop_opstack(&fop,0);}
        else fd_pop_opstack(&fop,0);}
      if (inout==NULL) {}
      else if (result)
        fd_hashset_add(FD_XHASHSET(inout->car),value);
      else fd_hashset_add(FD_XHASHSET(inout->cdr),value);
      if (inout) fd_decref(((lispval)inout));
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
  if (FD_ABORTP(oid))
    return oid;
  else if (!(FD_OIDP(oid)))
    return fd_err("PoolAllocFailed","fd_new_frame",p->poolid,VOID);
  else {}
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
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
