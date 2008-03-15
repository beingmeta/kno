/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_POOLS 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_IPEVAL 1

#include "fdb/dtype.h"
#include "fdb/fddb.h"
#include "fdb/apply.h"

#include <stdarg.h>

static fd_exception OddFindFramesArgs=_("Odd number of args to find frames");

static struct FD_HASHTABLE slot_caches, test_caches;
static fdtype get_methods, compute_methods, test_methods;
static fdtype add_effects, drop_effects;

#if FD_THREADS_ENABLED
static u8_mutex slotcache_lock;
#endif


/* The operations stack */

/* How it works:

    FramerD/FDB inference operations rely on a dynamic *operations
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

#if FD_USE__THREAD
static __thread struct FD_FRAMEOP_STACK *opstack=NULL;
#elif FD_THREADS_ENABLED
static u8_tld_key opstack_key;
#else
static struct FD_FRAMEOP_STACK *opstack=NULL;
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
  struct FD_FRAMEOP_STACK *start=get_opstack(), *ops=start;
  /* We initialize this field here, because we've gotten it anyway. */
  op->next=start;
  while (ops)
    if ((ops->op == op->op) &&
	(ops->frame == op->frame) &&
	(ops->slotid == op->slotid) &&
	(FDTYPE_EQUAL(ops->value,op->value)))
      return 1;
    else ops=ops->next;
  return 0;
}

/* Adds an entry to the frame operation stack. */
FD_EXPORT void fd_push_opstack(struct FD_FRAMEOP_STACK *op)
{
  struct FD_FRAMEOP_STACK *ops=get_opstack();
  op->next=ops;
  op->dependencies=NULL;
  op->n_deps=op->max_deps=0;
#if ((FD_THREADS_ENABLED) && (!(FD_USE__THREAD)))
  u8_tld_set(opstack_key,op);
#else
  opstack=op;
#endif
}

/* Pops an entry from the frame operation stack. */
FD_EXPORT int fd_pop_opstack(struct FD_FRAMEOP_STACK *op,int normal)
{
  struct FD_FRAMEOP_STACK *ops=get_opstack();
  if (ops != op) u8_raise("Corrupted ops stack","fd_pop_opstack",NULL);
  else if (op->dependencies) 
    if (normal) u8_free(op->dependencies);
    else {
      struct FD_DEPENDENCY_RECORD *scan=op->dependencies; 
      struct FD_DEPENDENCY_RECORD *limit=scan+op->n_deps;
      while (scan<limit) {
	fd_decref(scan->frame); fd_decref(scan->slotid); fd_decref(scan->value);
	scan++;}
      u8_free(op->dependencies);}
#if ((FD_THREADS_ENABLED) && (!(FD_USE__THREAD)))
  u8_tld_set(opstack_key,op->next);
#else
  opstack=op->next;
#endif
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
       slotid(frame)=value is cached as
         (CAR(test_caches(slotid))(frame)) contains value
       slotid(frame)!=value is cached as
         (CDR(test_caches(slotid))(frame)) contains value
     where the '=' relationship above really indicates 'has value' since slots
      are naturally multi-valued.
*/

static struct FD_HASHTABLE *make_slot_cache(fdtype slotid)
{
  fdtype table=fd_make_hashtable(NULL,17);
  fd_lock_mutex(&slotcache_lock);
  fd_hashtable_store(&slot_caches,slotid,table);
  fd_unlock_mutex(&slotcache_lock);
  return (struct FD_HASHTABLE *)table;
}

static struct FD_HASHTABLE *make_test_cache(fdtype slotid)
{
  fdtype table=fd_make_hashtable(NULL,17);
  fd_lock_mutex(&slotcache_lock);
  fd_hashtable_store(&test_caches,slotid,table);
  fd_unlock_mutex(&slotcache_lock);
  return (struct FD_HASHTABLE *)table;
}

FD_EXPORT void fd_clear_slotcache(fdtype slotid)
{
  fd_lock_mutex(&slotcache_lock);
  if (fd_hashtable_probe(&slot_caches,slotid))
    fd_hashtable_store(&slot_caches,slotid,FD_VOID);
  if (fd_hashtable_probe(&test_caches,slotid))
    fd_hashtable_store(&test_caches,slotid,FD_VOID);
  fd_unlock_mutex(&slotcache_lock);
}

FD_EXPORT void fd_clear_slotcache_entry(fdtype frame,fdtype slotid)
{
  fdtype slotcache=fd_hashtable_get(&slot_caches,slotid,FD_VOID);
  fdtype testcache=fd_hashtable_get(&test_caches,slotid,FD_VOID);
  if (FD_HASHTABLEP(slotcache)) 
    fd_hashtable_op(FD_XHASHTABLE(slotcache),fd_table_replace,frame,FD_VOID);
  /* Need to do this more selectively. */
  if (FD_HASHTABLEP(testcache))
    fd_hashtable_op(FD_XHASHTABLE(testcache),fd_table_replace,frame,FD_VOID);
}

FD_EXPORT void fd_clear_testcache_entry(fdtype frame,fdtype slotid,fdtype value)
{
  fdtype testcache=fd_hashtable_get(&test_caches,slotid,FD_VOID);
  if (FD_HASHTABLEP(testcache))
    if (FD_VOIDP(value))
      fd_hashtable_op(FD_XHASHTABLE(testcache),fd_table_replace,frame,FD_VOID);
    else {
      fdtype cache=fd_hashtable_get(FD_XHASHTABLE(testcache),frame,FD_EMPTY_CHOICE);
      if (FD_PAIRP(cache)) {
	fd_hashset_drop((fd_hashset)FD_CAR(cache),value);
	fd_hashset_drop((fd_hashset)FD_CDR(cache),value);}}
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
      The latter case represents the results of *tests* (e.g. does slotid(frame)=value)
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
fd_hashtable fd_implications_table=&implications;

static void init_dependencies(fd_frameop_stack *cxt)
{
  if (cxt->dependencies) return;
  else {
    cxt->dependencies=u8_alloc_n(32,struct FD_DEPENDENCY_RECORD);
    cxt->n_deps=0; cxt->max_deps=32;}
}

static void note_dependency(fd_frameop_stack *cxt,fdtype frame,fdtype slotid,fdtype value)
{
  fd_frameop_stack *scan=cxt;
  while (scan)
    if (scan->dependencies) {
      int n=scan->n_deps; struct FD_DEPENDENCY_RECORD *records=NULL;
      if (scan->n_deps>=scan->max_deps) {
	scan->dependencies=records=
	  u8_realloc_n(scan->dependencies,scan->max_deps*2,struct FD_DEPENDENCY_RECORD);
	scan->max_deps=scan->max_deps*2;}
      else records=scan->dependencies;
      records[n].frame=fd_incref(frame);
      records[n].slotid=fd_incref(slotid);
      records[n].value=fd_incref(value);
      scan->n_deps=n+1;
      return;}
    else scan=scan->next;
}

static void record_dependencies(fd_frameop_stack *cxt,fdtype factoid)
{
  if (cxt->dependencies) {
    int i=0, n=cxt->n_deps;
    struct FD_DEPENDENCY_RECORD *records=cxt->dependencies;
    fdtype *factoids=u8_alloc_n(n,fdtype);
    while (i<n) {
      fdtype depends;
      if (FD_VOIDP(records[i].value))
	depends=fd_init_pair(NULL,records[i].frame,records[i].slotid);
      else depends=fd_make_list(3,records[i].frame,records[i].slotid,fd_incref(records[i].value));
      factoids[i++]=depends;}
    fd_hashtable_iterkeys(&implications,fd_table_add,n,factoids,factoid);
    i=0; while (i<n) {fdtype factoid=factoids[i++]; fd_decref(factoid);}
    u8_free(factoids);}
}

static void decache_factoid(fdtype factoid)
{
  fdtype frame=FD_CAR(factoid);
  fdtype predicate=FD_CDR(factoid);
  if (FD_PAIRP(predicate)) {
    fdtype slotid=FD_CAR(predicate);
    fdtype value=FD_CDR(predicate);
    fd_clear_slotcache_entry(frame,predicate);
    fd_clear_testcache_entry(frame,predicate,value);}
  else {
    fd_clear_slotcache_entry(frame,predicate);
    fd_clear_testcache_entry(frame,predicate,FD_VOID);}
}

static void decache_implications(fdtype factoid)
{
  fdtype implies=fd_hashtable_get(&implications,factoid,FD_EMPTY_CHOICE);
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
    factoid=fd_init_pair(NULL,fd_incref(frame),
			 fd_init_pair(NULL,fd_incref(slotid),fd_incref(value)));
    decache_implications(factoid);
    fd_decref(factoid);}
  factoid=fd_init_pair(NULL,fd_incref(frame),fd_incref(slotid));
  decache_implications(factoid);
  fd_decref(factoid);
}

FD_EXPORT void fd_clear_slotcaches()
{
  fd_lock_mutex(&slotcache_lock);
  if (slot_caches.n_keys)
    fd_reset_hashtable(&slot_caches,17,1);
  if (test_caches.n_keys)
    fd_reset_hashtable(&test_caches,17,1);
  if (implications.n_keys)
    fd_reset_hashtable(&implications,17,1);
  fd_unlock_mutex(&slotcache_lock);
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
      struct FD_FUNCTION *fn=FD_XFUNCTION(lookup); fd_decref(lookup);
      return fn;}
    else return NULL;}
  else return NULL;
}

static fdtype get_slotid_methods(fdtype slotid,fdtype method_name)
{
  fd_pool p=fd_oid2pool(slotid); fdtype smap, result;
  if (FD_EXPECT_FALSE(p == NULL)) return fd_anonymous_oid("get_slotid_methods",slotid);
  else smap=fd_fetch_oid(p,slotid);
  if (FD_ABORTP(smap))
    return smap;
  else if (FD_SLOTMAPP(smap)) 
    result=fd_slotmap_get((fd_slotmap)smap,method_name,FD_EMPTY_CHOICE);
  else if (FD_SCHEMAPP(smap)) 
    result=fd_schemap_get((fd_schemap)smap,method_name,FD_EMPTY_CHOICE);
  else if (FD_TABLEP(smap)) 
    result=fd_get(smap,slotid,FD_EMPTY_CHOICE);
  else result=FD_VOID;
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
      int ipestate=fd_ipeval_status();
      struct FD_HASHTABLE *cache;
      fdtype methods, cachev, cached, computed=FD_EMPTY_CHOICE;
      cachev=fd_hashtable_get(&slot_caches,slotid,FD_VOID);
      if (FD_VOIDP(cachev)) {
	cache=make_slot_cache(slotid);
	cached=FD_VOID; cachev=(fdtype)cache;}
      else if (FD_EMPTY_CHOICEP(cachev)) {
	cache=NULL; cached=FD_VOID;}
      else {
	cache=FD_XHASHTABLE(cachev);
	cached=fd_hashtable_get(cache,f,FD_VOID);}
      if (!(FD_VOIDP(cached))) {
	fd_decref(cachev); return cached;}
      methods=get_slotid_methods(slotid,get_methods);
      /* Methods will be void only if the value of the slotoid isn't a table,
	 which is typically only the case when it hasn't been fetched yet. */
      note_dependency(&fop,f,slotid,FD_VOID);
      if (FD_VOIDP(methods)) return FD_EMPTY_CHOICE;
      else if (FD_EMPTY_CHOICEP(methods)) {
	fdtype value=fd_oid_get(f,slotid,FD_EMPTY_CHOICE);
	if (FD_EMPTY_CHOICEP(value))
	  methods=get_slotid_methods(slotid,compute_methods);
	else {
	  fd_decref(cachev);
	  return value;}}
      if (FD_VOIDP(methods)) return FD_EMPTY_CHOICE;
      else if (FD_ABORTP(methods)) {
	fd_decref(cachev);
	return methods;}
      /* At this point, we're computing the slot value */
      fd_push_opstack(&fop);
      init_dependencies(&fop);
      {FD_DO_CHOICES(method,methods) {
	  struct FD_FUNCTION *fn=lookup_method(method);
	  if (fn) {
	    fdtype args[2], value; args[0]=f; args[1]=slotid;
	    value=fd_finish_call(fd_dapply((fdtype)fn,2,args));
	    if (FD_ABORTP(value)) {
	      fd_push_error_context(fd_apply_context,fd_make_vector(3,method,f,slotid));
	      fd_decref(computed); fd_decref(methods);
	      fd_pop_opstack(&fop,0);
	      fd_decref(cachev);
	      return value;}
	    FD_ADD_TO_CHOICE(computed,value);}}}
      computed=fd_simplify_choice(computed);
      fd_decref(methods);
      if ((cache) && ((fd_ipeval_status()==ipestate))) {
	fdtype factoid=fd_init_pair(NULL,fd_incref(f),fd_incref(slotid));
	record_dependencies(&fop,factoid); fd_decref(factoid);
	fd_hashtable_store(cache,f,computed);
	fd_pop_opstack(&fop,1);}
      else fd_pop_opstack(&fop,0);
      fd_decref(cachev);
      return computed;}}
  else if (FD_EMPTY_CHOICEP(f)) return FD_EMPTY_CHOICE;
  else {
    struct FD_FRAMEOP_STACK *ptr=get_opstack();
    if (ptr) note_dependency(ptr,f,slotid,FD_VOID);
    return fd_oid_get(f,slotid,FD_EMPTY_CHOICE);}
}

FD_EXPORT int fd_frame_test(fdtype f,fdtype slotid,fdtype value)
{
  if (!(FD_OIDP(f)))
    return fd_test(f,slotid,value);
  else if (FD_OIDP(slotid)) {
    struct FD_FRAMEOP_STACK fop;
    FD_INIT_FRAMEOP_STACK_ENTRY(fop,fd_testop,f,slotid,value);
    if (fd_in_progressp(&fop)) return 0;
    else if (FD_VOIDP(value)) {
      fdtype values=fd_frame_get(f,slotid);
      note_dependency(&fop,f,slotid,value);
      if (FD_EMPTY_CHOICEP(values)) return 0;
      else {
	fd_decref(values); return 1;}}
    else {
      struct FD_HASHTABLE *cache; int result=0;
      fdtype cachev=fd_hashtable_get(&test_caches,slotid,FD_VOID);
      fdtype cached, computed=FD_EMPTY_CHOICE, methods, key;
      if (FD_VOIDP(cachev)) {
	cache=make_test_cache(slotid); cachev=(fdtype)cache;
	cached=fd_init_pair(NULL,fd_make_hashset(),fd_make_hashset());
	fd_hashtable_store(cache,f,cached);}
      else if (FD_EMPTY_CHOICEP(cachev)) {
	cache=NULL; cached=FD_VOID;}
      else {
	cache=FD_XHASHTABLE(cachev);
	cached=fd_hashtable_get(cache,f,FD_VOID);
	if (FD_VOIDP(cached)) {
	  cached=fd_init_pair(NULL,fd_make_hashset(),fd_make_hashset());
	  fd_hashtable_store(cache,f,cached);}}
      if (FD_PAIRP(cached)) {
	fdtype in=FD_CAR(cached), out=FD_CDR(cached);
	if (fd_hashset_get(FD_XHASHSET(in),value)) {
	  fd_decref(cached); fd_decref(cachev);
	  return 1;}
	else if (fd_hashset_get(FD_XHASHSET(out),value)) {
	  fd_decref(cached); fd_decref(cachev);
	  return 0;}
	else methods=get_slotid_methods(slotid,test_methods);}
      else methods=get_slotid_methods(slotid,test_methods);
      if (FD_VOIDP(methods)) {
	fd_decref(cached); fd_decref(cachev);
	return 0;}
      else if (FD_EMPTY_CHOICEP(methods)) {
	fdtype values=fd_frame_get(f,slotid);
	result=fd_overlapp(value,values);
	fd_decref(values);}
      else {
	fdtype args[3];
	args[0]=f; args[1]=slotid; args[2]=value;
	fd_push_opstack(&fop);
	init_dependencies(&fop);
	{
	  FD_DO_CHOICES(method,methods) {
	    struct FD_FUNCTION *fn=lookup_method(method);
	    if (fn) {
	      fdtype v=fd_apply((fdtype)fn,3,args);
	      if (FD_ABORTP(v)) {
		fd_push_error_context
		  (fd_apply_context,fd_make_vector(4,method,f,slotid,fd_incref(value)));
		fd_decref(methods); fd_decref(cachev);
		fd_pop_opstack(&fop,0);
		return fd_interr(v);}
	      else if (FD_TRUEP(v)) {
		result=1; fd_decref(v); break;}
	      else {}}}}
	if ((cache) && (!(fd_ipeval_failp()))) {
	  fdtype factoid=fd_make_list(3,fd_incref(f),
				      fd_incref(slotid),
				      fd_incref(value));
	  record_dependencies(&fop,factoid); fd_decref(factoid);
	  fd_pop_opstack(&fop,0);}
	else fd_pop_opstack(&fop,0);}
      if (result) fd_hashset_add(FD_XHASHSET(FD_CAR(cached)),value);
      else fd_hashset_add(FD_XHASHSET(FD_CDR(cached)),value);
      fd_decref(cached); fd_decref(cachev);
      return result;}}
  else if (FD_EMPTY_CHOICEP(f)) return 0;
  else {
    struct FD_FRAMEOP_STACK *ptr=get_opstack();
    if (ptr) note_dependency(ptr,f,slotid,value);
    return fd_oid_test(f,slotid,value);}
}

FD_EXPORT int fd_frame_add(fdtype f,fdtype slotid,fdtype value)
{
  if (FD_OIDP(slotid)) {
    struct FD_FRAMEOP_STACK fop;
    FD_INIT_FRAMEOP_STACK_ENTRY(fop,fd_addop,f,slotid,value);
    if (fd_in_progressp(&fop)) return 0;
    else {
      fdtype methods=fd_oid_get(slotid,add_effects,FD_EMPTY_CHOICE);
      fd_decache(f,slotid,value);
      if (FD_EMPTY_CHOICEP(methods)) {
	return fd_oid_add(f,slotid,value);}
      else {
	fdtype args[3];
	fd_push_opstack(&fop);
	args[0]=f; args[1]=slotid; args[2]=value;
	{
	  FD_DO_CHOICES(method,methods) {
	    struct FD_FUNCTION *fn=lookup_method(method);
	    if (fn) {
	      fdtype v=fd_apply((fdtype)fn,3,args);
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
      fdtype methods=fd_oid_get(slotid,drop_effects,FD_EMPTY_CHOICE);
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
	    struct FD_FUNCTION *fn=lookup_method(method);
	    if (fn) {
	      fdtype v=fd_apply((fdtype)fn,3,args);
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


/* Searching */

static fdtype make_features(fdtype slotids,fdtype values)
{
  fdtype results=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(slotid,slotids) {
    FD_DO_CHOICES(value,values) {
      fdtype feature=fd_make_pair(slotid,value);
      FD_ADD_TO_CHOICE(results,feature);}}
  return results;
}

FD_EXPORT fdtype fd_prim_find(fdtype indices,fdtype slotids,fdtype values)
{
  if (FD_CHOICEP(indices)) {
    fdtype combined=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(index,indices)
      if (FD_INDEXP(index)) {
	fd_index ix=fd_lisp2index(index);
	if (ix==NULL) {
	  fd_decref(combined);
	  return FD_ERROR_VALUE;}
	else {
	  FD_DO_CHOICES(slotid,slotids) {
	    FD_DO_CHOICES(value,values) {
	      fdtype key=fd_make_pair(slotid,value);
	      fdtype result=fd_index_get(ix,key);
	      FD_ADD_TO_CHOICE(combined,result);
	      fd_decref(key);}}}}
      else if (FD_TABLEP(index)) {
	FD_DO_CHOICES(slotid,slotids) {
	  FD_DO_CHOICES(value,values) {
	    fdtype key=fd_make_pair(slotid,value);
	    fdtype result=fd_get(index,key,FD_EMPTY_CHOICE);
	    FD_ADD_TO_CHOICE(combined,result);
	    fd_decref(key);}}}
      else {
	fd_decref(combined);
	return fd_type_error(_("index"),"fd_prim_find",index);}
    return combined;}
  else if (FD_TABLEP(indices)) {
    fdtype combined=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(slotid,slotids) {
      FD_DO_CHOICES(value,values) {
	fdtype key=fd_make_pair(slotid,value);
	fdtype result=fd_get(indices,key,FD_EMPTY_CHOICE);
	FD_ADD_TO_CHOICE(combined,result);
	fd_decref(key);}}
    return combined;}
  else {
    fdtype combined=FD_EMPTY_CHOICE;
    fd_index ix=fd_lisp2index(indices);
    if (ix==NULL) {
      fd_decref(combined);
      return FD_ERROR_VALUE;}
    else {
      FD_DO_CHOICES(slotid,slotids) {
	FD_DO_CHOICES(value,values) {
	  fdtype key=fd_make_pair(slotid,value);
	  fdtype result=fd_index_get(ix,key);
	  FD_ADD_TO_CHOICE(combined,result);
	  fd_decref(key);}}
      return combined;}}
}

FD_EXPORT fdtype fd_finder(fdtype indices,int n,fdtype *slotvals)
{
  int i=0, n_conjuncts=n/2;
  fdtype _conjuncts[6], *conjuncts=_conjuncts, result;
  if (FD_EMPTY_CHOICEP(indices)) return FD_EMPTY_CHOICE;
  if (n_conjuncts>6) conjuncts=u8_alloc_n(n_conjuncts,fdtype);
  while (i < n_conjuncts) {
    conjuncts[i]=fd_prim_find(indices,slotvals[i*2],slotvals[i*2+1]);
    if (FD_ABORTP(conjuncts[i])) {
      fdtype error=conjuncts[i];
      int j=0; while (j<i) {fd_decref(conjuncts[j]); j++;}
      if (conjuncts != _conjuncts) u8_free(conjuncts);
      return error;}
    if (FD_EMPTY_CHOICEP(conjuncts[i])) {
      int j=0; while (j<i) {fd_decref(conjuncts[j]); j++;}
      return FD_EMPTY_CHOICE;}
    i++;}
  result=fd_intersection(conjuncts,n_conjuncts);
  i=0; while (i < n_conjuncts) {
    fdtype cj=_conjuncts[i++]; fd_decref(cj);}
  if (_conjuncts != conjuncts) u8_free(conjuncts);
  return result;
}

FD_EXPORT fdtype fd_find_frames(fdtype indices,...)
{
  fdtype _slotvals[64], *slotvals=_slotvals, val;
  int n_slotvals=0, max_slotvals=64; va_list args;
  va_start(args,indices); val=va_arg(args,fdtype);
  while (!(FD_VOIDP(val))) {
    if (n_slotvals>=max_slotvals)
      if (max_slotvals == 64) {
	fdtype *newsv=u8_alloc_n(128,fdtype); int i=0;
	while (i<64) {newsv[i]=slotvals[i]; i++;}
	slotvals=newsv; max_slotvals=128;}
      else {
	slotvals=u8_realloc(slotvals,sizeof(fdtype)*max_slotvals*2);
	max_slotvals=max_slotvals*2;}
    slotvals[n_slotvals++]=val; val=va_arg(args,fdtype);}
  if (n_slotvals%2) {
    if (slotvals != _slotvals) u8_free(slotvals);
    return fd_err(OddFindFramesArgs,"fd_find_frames",NULL,FD_VOID);}
  if (slotvals != _slotvals) {
    fdtype result=fd_finder(indices,n_slotvals,slotvals);
    u8_free(slotvals);
    return result;}
  else return fd_finder(indices,n_slotvals,slotvals);
}


/* Indexing frames */

FD_EXPORT 
int fd_index_frame(fd_index ix,fdtype frame,fdtype slotid,fdtype values)
{
  fdtype features;
  if (FD_CHOICEP(slotid)) {
    FD_DO_CHOICES(sl,slotid) {
      int retval=fd_index_frame(ix,frame,sl,values);
      if (retval<0) return retval;}
    return 1;}
  else if ((FD_VOIDP(values)) && (FD_CHOICEP(frame))) {
    FD_DO_CHOICES(f,frame) {
      FD_DO_CHOICES(sl,slotid) {
	int retval=fd_index_frame(ix,f,sl,values);
	if (retval<0) return retval;}}
    return 1;}
  else if (FD_VOIDP(values))
    values=fd_frame_get(frame,slotid);
  else fd_incref(values);
  if (FD_ABORTP(values)) 
    return fd_interr(values);
  else features=make_features(slotid,values);
  {FD_DO_CHOICES(feature,features) {
    int retval=fd_index_add(ix,feature,frame);
    if (retval<0) {
      fd_decref(features); fd_decref(values);
      return retval;}}}
  fd_decref(values);
  fd_decref(features);
  return 1;
}


/* Background searching */

FD_EXPORT fdtype fd_bg_get(fdtype slotid,fdtype value)
{
  if (fd_background) {
    fdtype results=FD_EMPTY_CHOICE;
    fdtype features=make_features(slotid,value);
    if ((fd_prefetch) && (fd_ipeval_status()==0) &&
	(FD_CHOICEP(features)) &&
	(fd_index_prefetch((fd_index)fd_background,features)<0)) {
      fd_decref(features); return FD_ERROR_VALUE;}
    else {
      FD_DO_CHOICES(feature,features) {
	fdtype result=fd_index_get((fd_index)fd_background,feature);
	if (FD_ABORTP(result)) {
	  fd_decref(results); fd_decref(features);
	  return result;}
	else {FD_ADD_TO_CHOICE(results,result);}}}
    fd_decref(features);
    return fd_simplify_choice(results);}
  else return FD_EMPTY_CHOICE;
}

FD_EXPORT fdtype fd_bgfinder(int n,fdtype *slotvals)
{
  int i=0, n_conjuncts=n/2;
  fdtype _conjuncts[6], *conjuncts=_conjuncts, result;
  if (fd_background==NULL) return FD_EMPTY_CHOICE;
  if (n_conjuncts>6) conjuncts=u8_alloc_n(n_conjuncts,fdtype);
  while (i < n_conjuncts) {
    _conjuncts[i]=fd_bg_get(slotvals[i*2],slotvals[i*2+1]); i++;}
  result=fd_intersection(conjuncts,n_conjuncts);
  i=0; while (i < n_conjuncts) {
    fdtype cj=_conjuncts[i++]; fd_decref(cj);}
  if (_conjuncts != conjuncts) u8_free(conjuncts);
  return result;
}

FD_EXPORT fdtype fd_bgfind(fdtype slotid,fdtype values,...)
{
  fdtype _slotvals[64], *slotvals=_slotvals, val;
  int n_slotvals=0, max_slotvals=64; va_list args;
  va_start(args,values); val=va_arg(args,fdtype);
  slotvals[n_slotvals++]=slotid; slotvals[n_slotvals++]=values;
  while (!(FD_VOIDP(val))) {
    if (n_slotvals>=max_slotvals)
      if (max_slotvals == 64) {
	fdtype *newsv=u8_alloc_n(128,fdtype); int i=0;
	while (i<64) {newsv[i]=slotvals[i]; i++;}
	slotvals=newsv; max_slotvals=128;}
      else {
	slotvals=u8_realloc_n(slotvals,max_slotvals*2,fdtype);
	max_slotvals=max_slotvals*2;}
    slotvals[n_slotvals++]=val; val=va_arg(args,fdtype);}
  if (n_slotvals%2) {
    if (slotvals != _slotvals) u8_free(slotvals);
    return fd_err(OddFindFramesArgs,"fd_bgfind",NULL,FD_VOID);}
  if (slotvals != _slotvals) {
    fdtype result=fd_bgfinder(n_slotvals,slotvals);
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
    fd_hashtable h=(fd_hashtable)v;
    int *count=(int *)ptr;
    *count=*count+h->n_keys;}
  return 0;
}

FD_EXPORT int fd_slot_cache_load()
{
  int count=0;
  fd_for_hashtable(&slot_caches,hashtable_cachecount,&count,1);
  fd_for_hashtable(&test_caches,hashtable_cachecount,&count,1);
  return count;
}


/* Initialization */

FD_EXPORT void fd_init_frames_c()
{
  fd_register_source_file(versionid);

  get_methods=fd_intern("GET-METHODS");
  compute_methods=fd_intern("COMPUTE-METHODS");
  test_methods=fd_intern("TEST-METHODS");
  add_effects=fd_intern("ADD-EFFECTS");
  drop_effects=fd_intern("DROP-EFFECTS");

  fd_make_hashtable(&slot_caches,17);
  fd_make_hashtable(&test_caches,17);

  fd_make_hashtable(&implications,17);

#if FD_THREADS_ENABLED
  fd_init_mutex(&slotcache_lock);
#if FD_USE_TLS
  u8_new_threadkey(&opstack_key,NULL);
#endif
#endif
}
