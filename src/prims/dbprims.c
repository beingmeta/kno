/* -*- mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES	  (!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES	  (!(KNO_AVOID_INLINE))
#define KNO_FAST_CHOICE_CONTAINSP (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"
#include "kno/bloom.h"
#include "kno/frames.h"
#include "kno/methods.h"
#include "kno/sequences.h"
#include "kno/dbprims.h"
#include "kno/numbers.h"
#include "kno/cprims.h"

#include "libu8/u8printf.h"


static lispval pools_symbol, indexes_symbol, id_symbol, drop_symbol;
static lispval flags_symbol, register_symbol, readonly_symbol, phased_symbol;
static lispval background_symbol, adjunct_symbol, sparse_symbol, repair_symbol;

DEFPRIM1("slotid?",slotidp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SLOTID? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval slotidp(lispval arg)
{
  if ((OIDP(arg)) || (SYMBOLP(arg))) return KNO_TRUE;
  else return KNO_FALSE;
}

#define INDEXP(x) ( (KNO_INDEXP(x)) || (TYPEP((x),kno_consed_index_type)) )
#define POOLP(x)  ( (KNO_POOLP(x))  || (TYPEP((x),kno_consed_pool_type)) )

/* These are called when the lisp version of its pool/index argument
   is being returned and needs to be incref'd if it is consed. */
KNO_FASTOP lispval index_ref(kno_index ix)
{
  if (ix == NULL)
    return KNO_ERROR;
  else if (ix->index_serialno>=0)
    return LISPVAL_IMMEDIATE(kno_index_type,ix->index_serialno);
  else {
    lispval lix = (lispval)ix;
    kno_incref(lix);
    return lix;}
}

#define FALSE_ARGP(x) ( ((x)==KNO_VOID) || ((x)==KNO_FALSE) || ((x)==KNO_FIXZERO) )

static int load_db_module(lispval opts,u8_context context)
{
  if ( (KNO_VOIDP(opts)) || (KNO_EMPTYP(opts)) ||
       (KNO_FALSEP(opts)) || (KNO_DEFAULTP(opts)) )
    return 0;
  else {
    lispval modules = kno_getopt(opts,KNOSYM_MODULE,KNO_VOID);
    if ( (KNO_VOIDP(modules)) || (KNO_FALSEP(modules)) || (KNO_EMPTYP(modules)) )
      return 0;
    else {
      lispval mod = kno_find_module(modules,1);
      kno_decref(modules);
      if (KNO_ABORTP(mod)) {
	kno_seterr("MissingDBModule",context,
		   ((KNO_SYMBOLP(modules)) ? (KNO_SYMBOL_NAME(modules)) : (NULL)),
		   modules);
	return -1;}
      else {
	kno_decref(mod);
	return 1;}}}
}

/* Finding frames, etc. */

DEFPRIM("find-frames",find_frames_lexpr,
	KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"`(FIND-FRAMES *arg0* *arg1* *arg2* *args...*)` **undocumented**");
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
DEFPRIM("xfind-frames",xfind_frames_lexpr,
	KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"`(XFIND-FRAMES *arg0* *arg1* *arg2* *args...*)` **undocumented**");
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

DEFPRIM3("prefetch-slotvals!",prefetch_slotvals,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	 "`(PREFETCH-SLOTVALS! *arg0* *arg1* *arg2*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval prefetch_slotvals(lispval index,lispval slotids,lispval values)
{
  kno_index ix = kno_indexptr(index);
  if (ix) kno_find_prefetch(ix,slotids,values);
  else return kno_type_error("index","prefetch_slotvals",index);
  return VOID;
}

DEFPRIM("find-frames/prefetch!",find_frames_prefetch,KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"`(FIND-FRAMES/PREFETCH! *arg0* *arg1* *args...*)` **undocumented**");
static lispval find_frames_prefetch(int n,kno_argvec args)
{
  int i = (n%2);
  kno_index ix = ((n%2) ? (kno_indexptr(args[0])) : ((kno_index)(kno_background)));
  if (PRED_FALSE(ix == NULL))
    return kno_type_error("index","prefetch_slotvals",args[0]);
  else while (i<n) {
      DO_CHOICES(slotid,args[i]) {
	if ((SYMBOLP(slotid)) || (OIDP(slotid))) {}
	else return kno_type_error("slotid","find_frames_prefetch",slotid);}
      i = i+2;}
  i = (n%2); while (i<n) {
    lispval slotids = args[i], values = args[i+1];
    kno_find_prefetch(ix,slotids,values);
    i = i+2;}
  return VOID;
}

static void hashtable_index_frame(lispval ix,
				  lispval frames,lispval slotids,
				  lispval values)
{
  if (VOIDP(values)) {
    DO_CHOICES(frame,frames) {
      DO_CHOICES(slotid,slotids) {
	lispval values = ((OIDP(frame)) ?
			  (kno_frame_get(frame,slotid)) :
			  (kno_get(frame,slotid,EMPTY)));
	DO_CHOICES(value,values) {
	  lispval key = kno_conspair(kno_incref(slotid),kno_incref(value));
	  kno_add(ix,key,frame);
	  kno_decref(key);}
	kno_decref(values);}}}
  else {
    DO_CHOICES(slotid,slotids) {
      DO_CHOICES(value,values) {
	lispval key = kno_conspair(kno_incref(slotid),kno_incref(value));
	kno_add(ix,key,frames);
	kno_decref(key);}}}
}

DEFPRIM4("index-frame",index_frame_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	 "`(INDEX-FRAME *arg0* *arg1* *arg2* [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval index_frame_prim
(lispval indexes,lispval frames,lispval slotids,lispval values)
{
  if (CHOICEP(indexes)) {
    DO_CHOICES(index,indexes)
      if (HASHTABLEP(index))
	hashtable_index_frame(index,frames,slotids,values);
      else {
	kno_index ix = kno_indexptr(index);
	if (PRED_FALSE(ix == NULL))
	  return kno_type_error("index","index_frame_prim",index);
	else if (kno_index_frame(ix,frames,slotids,values)<0)
	  return KNO_ERROR;}}
  else if (HASHTABLEP(indexes)) {
    hashtable_index_frame(indexes,frames,slotids,values);
    return VOID;}
  else {
    kno_index ix = kno_indexptr(indexes);
    if (PRED_FALSE(ix == NULL))
      return kno_type_error("index","index_frame_prim",indexes);
    else if (kno_index_frame(ix,frames,slotids,values)<0)
      return KNO_ERROR;}
  return VOID;
}

/* Pool and index functions */

DEFPRIM1("pool?",poolp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(POOL? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval poolp(lispval arg)
{
  if (POOLP(arg))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("index?",indexp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(INDEX? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval indexp(lispval arg)
{
  if (INDEXP(arg))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("name->pool",getpool,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(NAME->POOL *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval getpool(lispval arg)
{
  kno_pool p = NULL;
  if (KNO_POOLP(arg)) return kno_incref(arg);
  else if (STRINGP(arg))
    p = kno_name2pool(CSTRING(arg));
  else if (OIDP(arg)) p = kno_oid2pool(arg);
  if (p) return kno_pool2lisp(p);
  else return EMPTY;
}

static u8_condition Unknown_PoolName=_("Unknown pool name");

DEFPRIM2("set-pool-namefn!",set_pool_namefn,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(SET-POOL-NAMEFN! *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval set_pool_namefn(lispval arg,lispval method)
{
  kno_pool p = NULL;
  if (KNO_POOLP(arg))
    p = kno_lisp2pool(arg);
  else if (STRINGP(arg)) {
    p = kno_name2pool(CSTRING(arg));
    if (!(p)) return kno_err
		(Unknown_PoolName,"set_pool_namefn",NULL,arg);}
  else if (OIDP(arg))
    p = kno_oid2pool(arg);
  else return kno_type_error(_("pool"),"set_pool_namefn",arg);
  if ((OIDP(method))||(SYMBOLP(method))||(KNO_APPLICABLEP(method))) {
    kno_set_pool_namefn(p,method);
    return VOID;}
  else return kno_type_error(_("namefn"),"set_pool_namefn",method);
}

DEFPRIM2("set-cache-level!",set_cache_level,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(SET-CACHE-LEVEL! *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval set_cache_level(lispval arg,lispval level)
{
  if (!(KNO_UINTP(level)))
    return kno_type_error("fixnum","set_cache_level",level);
  else if (KNO_POOLP(arg)) {
    kno_pool p = kno_lisp2pool(arg);
    if (p) kno_pool_setcache(p,FIX2INT(level));
    else return KNO_ERROR;
    return VOID;}
  else if (INDEXP(arg)) {
    kno_index ix = kno_indexptr(arg);
    if (ix) kno_index_setcache(ix,FIX2INT(level));
    else return kno_type_error("index","index_frame_prim",arg);
    return VOID;}
  else return kno_type_error("pool or index","set_cache_level",arg);
}

DEFPRIM2("try-pool",try_pool,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(TRY-POOL *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval try_pool(lispval arg1,lispval opts)
{
  if (load_db_module(opts,"try_pool")<0)
    return KNO_ERROR;
  else	if ( (KNO_POOLP(arg1)) || (TYPEP(arg1,kno_consed_pool_type)) )
    return kno_incref(arg1);
  else if (!(STRINGP(arg1)))
    return kno_type_error(_("string"),"load_pool",arg1);
  else {
    kno_storage_flags flags = kno_get_dbflags(opts,KNO_STORAGE_ISPOOL) |
      KNO_STORAGE_NOERR;
    kno_pool p = kno_get_pool(CSTRING(arg1),flags,opts);
    if (p)
      return kno_pool2lisp(p);
    else return KNO_FALSE;}
}

DEFPRIM2("adjunct-pool",adjunct_pool,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(ADJUNCT-POOL *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval adjunct_pool(lispval arg1,lispval opts)
{
  if (load_db_module(opts,"adjunct_pool")<0)
    return KNO_ERROR;
  else if ( (KNO_POOLP(arg1)) || (TYPEP(arg1,kno_consed_pool_type)) )
    // TODO: Should check it's really adjunct, if that's the right thing?
    return kno_incref(arg1);
  else if (!(STRINGP(arg1)))
    return kno_type_error(_("string"),"adjunct_pool",arg1);
  else {
    kno_storage_flags flags = kno_get_dbflags(opts,KNO_STORAGE_ISPOOL) |
      KNO_POOL_ADJUNCT;
    kno_pool p = kno_get_pool(CSTRING(arg1),flags,opts);
    if (p)
      return kno_pool2lisp(p);
    else return KNO_ERROR;}
}

DEFPRIM2("use-pool",use_pool,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(USE-POOL *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval use_pool(lispval arg1,lispval opts)
{
  if (load_db_module(opts,"use_pool")<0)
    return KNO_ERROR;
  else if ( (KNO_POOLP(arg1)) || (TYPEP(arg1,kno_consed_pool_type)) )
    // TODO: Should check to make sure that it's in the background
    return kno_incref(arg1);
  else if (!(STRINGP(arg1)))
    return kno_type_error(_("string"),"use_pool",arg1);
  else {
    kno_pool p = kno_get_pool(CSTRING(arg1),-1,opts);
    if (p) return kno_pool2lisp(p);
    else return kno_err(kno_NoSuchPool,"use_pool",
			CSTRING(arg1),VOID);}
}

DEFPRIM2("use-index",use_index,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(USE-INDEX *spec* [*opts*]) "
	 "adds an index to the search background",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval use_index(lispval arg,lispval opts)
{
  kno_index ixresult = NULL;
  if (load_db_module(opts,"use_index")<0)
    return KNO_ERROR;
  else if (INDEXP(arg)) {
    ixresult = kno_indexptr(arg);
    if (ixresult) kno_add_to_background(ixresult);
    else return kno_type_error("index","index_frame_prim",arg);
    return kno_incref(arg);}
  else if (STRINGP(arg))
    if (strchr(CSTRING(arg),';')) {
      /* We explicitly handle ; separated arguments here, so that
	 we can return the choice of index. */
      lispval results = EMPTY;
      u8_byte *copy = u8_strdup(CSTRING(arg));
      u8_byte *start = copy, *end = strchr(start,';');
      *end='\0'; while (start) {
	kno_index ix = kno_use_index(start,
				     kno_get_dbflags(opts,KNO_STORAGE_ISINDEX),
				     opts);
	if (ix) {
	  lispval ixv = index_ref(ix);
	  CHOICE_ADD(results,ixv);}
	else {
	  u8_free(copy);
	  kno_decref(results);
	  return KNO_ERROR;}
	if ((end) && (end[1])) {
	  start = end+1; end = strchr(start,';');
	  if (end) *end='\0';}
	else start = NULL;}
      u8_free(copy);
      return results;}
    else ixresult = kno_use_index
	   (CSTRING(arg),kno_get_dbflags(opts,KNO_STORAGE_ISINDEX),opts);
  else return kno_type_error(_("index spec"),"use_index",arg);
  if (ixresult)
    return index_ref(ixresult);
  else return KNO_ERROR;
}

static lispval open_index_helper(lispval arg,lispval opts,int registered)
{
  kno_storage_flags flags = kno_get_dbflags(opts,KNO_STORAGE_ISINDEX);
  kno_index ix = NULL;
  lispval modules = kno_getopt(opts,KNOSYM_MODULE,KNO_VOID);
  lispval mod = (KNO_VOIDP(modules)) ? (KNO_VOID) : (kno_get_module(modules));
  kno_decref(modules);
  if (KNO_ABORTP(mod))
    return mod;
  else kno_decref(mod);
  if (registered == 0)
    flags |= KNO_STORAGE_UNREGISTERED;
  else if (registered>0)
    flags &= ~KNO_STORAGE_UNREGISTERED;
  else {}
  if (STRINGP(arg)) {
    if (strchr(CSTRING(arg),';')) {
      /* We explicitly handle ; separated arguments here, so that
	 we can return the choice of index. */
      lispval results = EMPTY;
      u8_byte *copy = u8_strdup(CSTRING(arg));
      u8_byte *start = copy, *end = strchr(start,';');
      *end='\0'; while (start) {
	kno_index ix = kno_get_index(start,flags,opts);
	if (ix == NULL) {
	  u8_free(copy);
	  kno_decref(results);
	  return KNO_ERROR;}
	else {
	  lispval ixv = index_ref(ix);
	  CHOICE_ADD(results,ixv);}
	if ((end) && (end[1])) {
	  start = end+1; end = strchr(start,';');
	  if (end) *end='\0';}
	else start = NULL;}
      u8_free(copy);
      return results;}
    else return index_ref(kno_get_index(CSTRING(arg),flags,opts));}
  else if (KNO_ETERNAL_INDEXP(arg))
    return arg;
  else if (KNO_CONSED_INDEXP(arg))
    return kno_incref(arg);
  else kno_seterr(kno_TypeError,"use_index",NULL,kno_incref(arg));
  if (ix)
    return index_ref(ix);
  else return KNO_ERROR;
}

DEFPRIM2("open-index",open_index,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(OPEN-INDEX *spec* [*opts*]) "
	 "opens and returns an index",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval open_index(lispval arg,lispval opts)
{
  if (load_db_module(opts,"open_index")<0)
    return KNO_ERROR;
  else return open_index_helper(arg,opts,-1);
}

DEFPRIM2("register-index",register_index,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(REGISTER-INDEX *spec* [*opts*]) "
	 "opens, registers, and returns an index",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval register_index(lispval arg,lispval opts)
{
  if (load_db_module(opts,"register_index")<0)
    return KNO_ERROR;
  else return open_index_helper(arg,opts,1);
}

DEFPRIM2("cons-index",cons_index,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(CONS-INDEX *spec* [*opts*]) "
	 "opens and returns an unregistered index",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval cons_index(lispval arg,lispval opts)
{
  if (load_db_module(opts,"cons_index")<0)
    return KNO_ERROR;
  else return open_index_helper(arg,opts,0);
}

DEFPRIM2("make-pool",make_pool,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(MAKE-POOL *arg0* *arg1*)` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval make_pool(lispval path,lispval opts)
{
  if (load_db_module(opts,"make_pool")<0) return KNO_ERROR;
  kno_pool p = NULL;
  lispval type = kno_getopt(opts,KNOSYM_TYPE,VOID);
  kno_storage_flags flags = kno_get_dbflags(opts,KNO_STORAGE_ISPOOL);
  if (KNO_VOIDP(type)) type = kno_getopt(opts,KNOSYM_MODULE,VOID);
  if (VOIDP(type))
    p = kno_make_pool(CSTRING(path),NULL,flags,opts);
  else if (SYMBOLP(type))
    p = kno_make_pool(CSTRING(path),SYM_NAME(type),flags,opts);
  else if (STRINGP(type))
    p = kno_make_pool(CSTRING(path),CSTRING(type),flags,opts);
  else if (STRINGP(path))
    return kno_err(_("BadPoolType"),"make_pool",CSTRING(path),type);
  else return kno_err(_("BadPoolType"),"make_pool",NULL,type);
  kno_decref(type);
  if (p)
    return kno_pool2lisp(p);
  else return KNO_ERROR;
}

DEFPRIM2("open-pool",open_pool,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(OPEN-POOL *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval open_pool(lispval path,lispval opts)
{
  if (load_db_module(opts,"open_pool")<0) return KNO_ERROR;
  kno_storage_flags flags = kno_get_dbflags(opts,KNO_STORAGE_ISPOOL);
  kno_pool p = kno_open_pool(CSTRING(path),flags,opts);
  if (p)
    return kno_pool2lisp(p);
  else return KNO_ERROR;
}

DEFPRIM2("make-index",make_index,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(MAKE-INDEX *arg0* *arg1*)` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval make_index(lispval path,lispval opts)
{
  if (load_db_module(opts,"make_index")<0) return KNO_ERROR;
  kno_index ix = NULL;
  lispval type = kno_getopt(opts,KNOSYM_TYPE,VOID);
  kno_storage_flags flags =
    (FIXNUMP(opts)) ?
    (KNO_STORAGE_ISINDEX) :
    (kno_get_dbflags(opts,KNO_STORAGE_ISINDEX)) ;
  if (KNO_VOIDP(type)) type = kno_getopt(opts,KNOSYM_MODULE,VOID);
  if (VOIDP(type))
    ix = kno_make_index(CSTRING(path),NULL,flags,opts);
  else if (SYMBOLP(type))
    ix = kno_make_index(CSTRING(path),SYM_NAME(type),flags,opts);
  else if (STRINGP(type))
    ix = kno_make_index(CSTRING(path),CSTRING(type),flags,opts);
  else if (STRINGP(path))
    return kno_err(_("BadIndexType"),"make_index",CSTRING(path),type);
  else return kno_err(_("BadIndexType"),"make_index",NULL,type);
  kno_decref(type);
  if (ix)
    return index_ref(ix);
  else return KNO_ERROR;
}

DEFPRIM1("oid-value",oidvalue,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(OID-VALUE *arg0*)` **undocumented**",
	 kno_oid_type,KNO_VOID);
static lispval oidvalue(lispval arg)
{
  return kno_oid_value(arg);
}
DEFPRIM3("set-oid-value!",setoidvalue,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(SET-OID-VALUE! *oid* *value* [*nocopy*])` "
	 "directly sets the value of *oid* to *value*. If "
	 "the value is a slotmap or schemap, a copy is "
	 "stored unless *nocopy* is not false (the default).",
	 kno_oid_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_FALSE);
static lispval setoidvalue(lispval o,lispval v,lispval nocopy)
{
  int retval;
  if (KNO_TRUEP(nocopy)) {kno_incref(v);}
  else if (SLOTMAPP(v)) {
    v = kno_deep_copy(v);
    KNO_TABLE_SET_MODIFIED(v,1);}
  else if (SCHEMAPP(v)) {
    v = kno_deep_copy(v);
    KNO_TABLE_SET_MODIFIED(v,1);}
  else if (HASHTABLEP(v)) {
    v = kno_deep_copy(v);
    KNO_TABLE_SET_MODIFIED(v,1);}
  else v = kno_incref(v);
  retval = kno_set_oid_value(o,v);
  kno_decref(v);
  if (retval<0)
    return KNO_ERROR;
  else return VOID;
}
DEFPRIM2("%set-oid-value!",xsetoidvalue,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(%SET-OID-VALUE! *oid* *value* [*nocopy*])` "
	 "directly sets the value of *oid* to *value*. If "
	 "the value is a slotmap or schemap, a copy is "
	 "stored unless *nocopy* is not false (the default).",
	 kno_oid_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval xsetoidvalue(lispval o,lispval v)
{
  int retval;
  kno_incref(v);
  retval = kno_replace_oid_value(o,v);
  kno_decref(v);
  if (retval<0)
    return KNO_ERROR;
  else return VOID;
}

DEFPRIM2("lock-oid!",lockoid,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(LOCK-OID! *arg0* *arg1*)` **undocumented**",
	 kno_oid_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval lockoid(lispval o,lispval soft)
{
  int retval = kno_lock_oid(o);
  if (retval<0)
    if (KNO_TRUEP(soft)) {
      kno_poperr(NULL,NULL,NULL,NULL);
      return KNO_FALSE;}
    else return KNO_ERROR;
  else return KNO_INT(retval);
}

DEFPRIM1("locked?",oidlockedp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(LOCKED? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval oidlockedp(lispval arg)
{
  if (!(OIDP(arg)))
    return KNO_FALSE;
  else {
    kno_pool p = kno_oid2pool(arg);
    if ( (p) && (kno_hashtable_probe_novoid(&(p->pool_changes),arg)) )
      return KNO_TRUE;
    else return KNO_FALSE;}
}

DEFPRIM1("lock-oids!",lockoids,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "`(LOCK-OIDS! *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lockoids(lispval oids)
{
  int retval = kno_lock_oids(oids);
  if (retval<0)
    return KNO_ERROR;
  else return KNO_INT(retval);
}

DEFPRIM1("locked-oids",lockedoids,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(LOCKED-OIDS *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lockedoids(lispval pool)
{
  kno_pool p = kno_lisp2pool(pool);
  return kno_hashtable_keys(&(p->pool_changes));
}

DEFPRIM2("unlock-oids!",unlockoids,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(0)|KNO_NDCALL,
	 "`(UNLOCK-OIDS! [*arg0*] [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval unlockoids(lispval oids,lispval commitp)
{
  int force_commit = (VOIDP(commitp)) ? (commit_modified) :
    (KNO_FALSEP(commitp)) ? (discard_modified) :
    (leave_modified);
  if (VOIDP(oids)) {
    kno_unlock_pools(force_commit);
    return KNO_FALSE;}
  else if ((TYPEP(oids,kno_pool_type))||(STRINGP(oids))) {
    kno_pool p = ((TYPEP(oids,kno_pool_type)) ? (kno_lisp2pool(oids)) :
		  (kno_name2pool(CSTRING(oids))));
    if (p) {
      int retval = kno_pool_unlock_all(p,force_commit);
      if (retval<0) return KNO_ERROR;
      else return KNO_INT(retval);}
    else return kno_type_error("pool or OID","unlockoids",oids);}
  else {
    int retval = kno_unlock_oids(oids,force_commit);
    if (retval<0)
      return KNO_ERROR;
    else return KNO_INT(retval);}
}

DEFPRIM2("make-aggregate-index",make_aggregate_index,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "Creates an aggregate index from a collection of "
	 "other indexes",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval make_aggregate_index(lispval sources,lispval opts)
{
  int n_sources = KNO_CHOICE_SIZE(sources), n_partitions=0;
  if (n_sources == 0)
    return kno_index2lisp((kno_index)kno_make_aggregate_index(opts,8,0,NULL));
  kno_index partitions[n_sources];
  KNO_DO_CHOICES(source,sources) {
    kno_index ix = NULL;
    if (STRINGP(source))
      ix = kno_get_index(kno_strdata(source),0,VOID);
    else if (INDEXP(source))
      ix = kno_indexptr(source);
    else if (SYMBOLP(source)) {
      lispval val = kno_config_get(SYM_NAME(source));
      if (STRINGP(val)) ix = kno_get_index(KNO_CSTRING(val),0,VOID);
      else if (INDEXP(val)) ix = kno_indexptr(val);
      else NO_ELSE;}
    else if ( (KNO_PAIRP(source)) || (KNO_SLOTMAPP(source)) ) {
      lispval spec = kno_getopt(source,KNOSYM_SOURCE,KNO_VOID);
      if (KNO_STRINGP(spec))
	ix = kno_open_index(KNO_CSTRING(spec),-1,source);
      kno_decref(spec);}
    else {}
    if (ix)
      partitions[n_partitions++] = ix;
    else {
      KNO_STOP_DO_CHOICES;
      return kno_type_error("index","make_aggregate_index",source);}}
  int n_alloc = 8;
  while (n_partitions > n_alloc)
    n_alloc=n_alloc*2;
  kno_aggregate_index aggregate =
    kno_make_aggregate_index(opts,n_alloc,n_partitions,partitions);
  return kno_index2lisp((kno_index)aggregate);
}

DEFPRIM1("aggregate-index?",aggregate_indexp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(AGGREGATE-INDEX? *arg*) "
	 "=> true if *arg* is an aggregate index",
	 kno_any_type,KNO_VOID);
static lispval aggregate_indexp(lispval arg)
{
  kno_index ix = kno_indexptr(arg);
  if (ix == NULL)
    return KNO_FALSE;
  else if (kno_aggregate_indexp(ix))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM2("extend-aggregate-index!",extend_aggregate_index,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "(EXTEND-AGGREGATE-INDEX! *agg* *index*) "
	 "adds *index* to the aggregate index *agg*",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval extend_aggregate_index(lispval into_arg,lispval partition_arg)
{
  kno_index into = kno_indexptr(into_arg);
  kno_index partition = kno_indexptr(partition_arg);
  if (kno_aggregate_indexp(into)) {
    kno_aggregate_index aggregate = (kno_aggregate_index) into;
    if (partition) {
      if (partition->index_serialno < 0) kno_register_index(partition);
      if (partition->index_serialno > 0) {
	if (kno_add_to_aggregate_index(aggregate,partition)<0)
	  return KNO_ERROR;
	else return VOID;}
      else return kno_type_error(_("eternal (not-ephemeral) index"),
				 "add_to_aggregate_index",
				 partition_arg);}
    else return kno_type_error
	   (_("index"),"add_to_aggregate_index",partition_arg);}
  else return kno_type_error(_("aggregate index"),"add_to_aggregate_index",
			     into_arg);
}

DEFPRIM1("tempindex?",tempindexp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(TEMPINDEX? *arg*) "
	 "returns #t if *arg* is a temporary index.",
	 kno_any_type,KNO_VOID);
static lispval tempindexp(lispval arg)
{
  kno_index ix = kno_indexptr(arg);
  if (ix == NULL)
    return KNO_FALSE;
  else if (kno_tempindexp(ix))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM6("make-mempool",make_mempool,KNO_MAX_ARGS(6)|KNO_MIN_ARGS(2),
	 "`(MAKE-MEMPOOL *arg0* *arg1* [*arg2*] [*arg3*] [*arg4*] [*arg5*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_oid_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(1048576),kno_fixnum_type,KNO_CPP_INT(0),
	 kno_any_type,KNO_FALSE,kno_any_type,KNO_FALSE);
static lispval make_mempool(lispval label,lispval base,lispval cap,
			    lispval load,lispval noswap,lispval opts)
{
  if (!(KNO_UINTP(cap))) return kno_type_error("uint","make_mempool",cap);
  if (!(KNO_UINTP(load))) return kno_type_error("uint","make_mempool",load);
  kno_pool p = kno_make_mempool
    (CSTRING(label),KNO_OID_ADDR(base),
     FIX2INT(cap),
     FIX2INT(load),
     (!(FALSEP(noswap))),
     opts);
  if (p == NULL)
    return KNO_ERROR;
  else return kno_pool2lisp(p);
}

DEFPRIM1("clean-mempool",clean_mempool,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(CLEAN-MEMPOOL *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval clean_mempool(lispval pool_arg)
{
  int retval = kno_clean_mempool(kno_lisp2pool(pool_arg));
  if (retval<0) return KNO_ERROR;
  else return KNO_INT(retval);
}

DEFPRIM1("reset-mempool",reset_mempool,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(RESET-MEMPOOL *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval reset_mempool(lispval pool_arg)
{
  int retval = kno_reset_mempool(kno_lisp2pool(pool_arg));
  if (retval<0) return KNO_ERROR;
  else return KNO_INT(retval);
}

DEFPRIM6("make-procpool",make_procpool,KNO_MAX_ARGS(6)|KNO_MIN_ARGS(4),
	 "Returns a pool implemented by userspace functions",
	 kno_string_type,KNO_VOID,kno_oid_type,KNO_VOID,
	 kno_fixnum_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_CPP_INT(0));
static lispval make_procpool(lispval label,
			     lispval base,lispval cap,
			     lispval opts,lispval state,
			     lispval load)
{
  if (load_db_module(opts,"make_procpool")<0) return KNO_ERROR;
  if (!(KNO_UINTP(cap)))
    return kno_type_error("uint","make_procpool",cap);
  if (KNO_VOIDP(load))
    load=KNO_FIXZERO;
  else if (load == KNO_DEFAULT_VALUE)
    load=KNO_FIXZERO;
  else if (!(KNO_UINTP(load)))
    return kno_type_error("uint","make_procpool",load);
  kno_pool p = kno_make_procpool
    (KNO_OID_ADDR(base),FIX2INT(cap),FIX2INT(load),
     opts,state,CSTRING(label),NULL);
  return kno_pool2lisp(p);
}

DEFPRIM10("make-extpool",make_extpool,KNO_MAX_ARGS(10)|KNO_MIN_ARGS(4),
	  "`(MAKE-EXTPOOL *arg0* *arg1* *arg2* *arg3* [*arg4*] [*arg5*] [*arg6*] [*arg7*] [*arg8*] [*arg9*])` **undocumented**",
	  kno_string_type,KNO_VOID,kno_oid_type,KNO_VOID,
	  kno_fixnum_type,KNO_VOID,kno_any_type,KNO_VOID,
	  kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	  kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	  kno_any_type,KNO_TRUE,kno_any_type,KNO_FALSE);
static lispval make_extpool(lispval label,lispval base,lispval cap,
			    lispval fetchfn,lispval savefn,
			    lispval lockfn,lispval allocfn,
			    lispval state,lispval cache,
			    lispval opts)
{
  if (!(KNO_UINTP(cap))) return kno_type_error("uint","make_mempool",cap);
  kno_pool p = kno_make_extpool
    (CSTRING(label),KNO_OID_ADDR(base),FIX2INT(cap),
     fetchfn,savefn,lockfn,allocfn,state,opts);
  if (FALSEP(cache)) kno_pool_setcache(p,0);
  return kno_pool2lisp(p);
}

DEFPRIM3("extpool-cache!",extpool_setcache,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	 "`(EXTPOOL-CACHE! *arg0* *arg1* *arg2*)` **undocumented**",
	 kno_pool_type,KNO_VOID,kno_oid_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval extpool_setcache(lispval pool,lispval oid,lispval value)
{
  kno_pool p = kno_lisp2pool(pool);
  if (kno_extpool_cache_value(p,oid,value)<0)
    return KNO_ERROR;
  else return VOID;
}

DEFPRIM1("extpool-fetchfn",extpool_fetchfn,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXTPOOL-FETCHFN *arg0*)` **undocumented**",
	 kno_pool_type,KNO_VOID);
static lispval extpool_fetchfn(lispval pool)
{
  kno_pool p = kno_lisp2pool(pool);
  if ( (p) && (p->pool_handler== &kno_extpool_handler) ) {
    struct KNO_EXTPOOL *ep = (struct KNO_EXTPOOL *)p;
    return kno_incref(ep->fetchfn);}
  else return kno_type_error("extpool","extpool_fetchfn",pool);
}

DEFPRIM1("extpool-savefn",extpool_savefn,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXTPOOL-SAVEFN *arg0*)` **undocumented**",
	 kno_pool_type,KNO_VOID);
static lispval extpool_savefn(lispval pool)
{
  kno_pool p = kno_lisp2pool(pool);
  if ( (p) && (p->pool_handler== &kno_extpool_handler) ) {
    struct KNO_EXTPOOL *ep = (struct KNO_EXTPOOL *)p;
    return kno_incref(ep->savefn);}
  else return kno_type_error("extpool","extpool_savefn",pool);
}

DEFPRIM1("extpool-lockfn",extpool_lockfn,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXTPOOL-LOCKFN *arg0*)` **undocumented**",
	 kno_pool_type,KNO_VOID);
static lispval extpool_lockfn(lispval pool)
{
  kno_pool p = kno_lisp2pool(pool);
  if ( (p) && (p->pool_handler== &kno_extpool_handler) ) {
    struct KNO_EXTPOOL *ep = (struct KNO_EXTPOOL *)p;
    return kno_incref(ep->lockfn);}
  else return kno_type_error("extpool","extpool_lockfn",pool);
}

DEFPRIM1("extpool-state",extpool_state,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXTPOOL-STATE *arg0*)` **undocumented**",
	 kno_pool_type,KNO_VOID);
static lispval extpool_state(lispval pool)
{
  kno_pool p = kno_lisp2pool(pool);
  if ( (p) && (p->pool_handler== &kno_extpool_handler) ) {
    struct KNO_EXTPOOL *ep = (struct KNO_EXTPOOL *)p;
    return kno_incref(ep->state);}
  else return kno_type_error("extpool","extpool_state",pool);
}

/* Proc indexes */

DEFPRIM5("make-procindex",make_procindex,KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
	 "Returns a pool implemented by userspace functions",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_string_type,KNO_VOID);
static lispval make_procindex(lispval id,
			      lispval opts,lispval state,
			      lispval source,lispval typeid)
{
  if (load_db_module(opts,"make_procindex")<0) return KNO_ERROR;
  kno_index ix = kno_make_procindex
    (opts,state,KNO_CSTRING(id),
     ((KNO_VOIDP(source)) ? (NULL) : (KNO_CSTRING(source))),
     ((KNO_VOIDP(typeid)) ? (NULL) : (KNO_CSTRING(typeid))));
  return kno_index2lisp(ix);
}

/* External indexes */

DEFPRIM6("make-extindex",make_extindex,KNO_MAX_ARGS(6)|KNO_MIN_ARGS(2),
	 "`(MAKE-EXTINDEX *arg0* *arg1* [*arg2*] [*arg3*] [*arg4*] [*arg5*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_TRUE,kno_any_type,KNO_FALSE);
static lispval make_extindex(lispval label,lispval fetchfn,lispval commitfn,
			     lispval state,lispval usecache,
			     lispval opts)
{
  kno_index ix = kno_make_extindex
    (CSTRING(label),
     ((FALSEP(fetchfn))?(VOID):(fetchfn)),
     ((FALSEP(commitfn))?(VOID):(commitfn)),
     ((FALSEP(state))?(VOID):(state)),
     -1,
     opts);
  if (FALSEP(usecache)) kno_index_setcache(ix,0);
  return kno_index2lisp(ix);
}

DEFPRIM6("cons-extindex",cons_extindex,KNO_MAX_ARGS(6)|KNO_MIN_ARGS(2),
	 "`(CONS-EXTINDEX *arg0* *arg1* [*arg2*] [*arg3*] [*arg4*] [*arg5*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_TRUE,kno_any_type,KNO_FALSE);
static lispval cons_extindex(lispval label,lispval fetchfn,lispval commitfn,
			     lispval state,lispval usecache,lispval opts)
{
  kno_index ix = kno_make_extindex
    (CSTRING(label),
     ((FALSEP(fetchfn))?(VOID):(fetchfn)),
     ((FALSEP(commitfn))?(VOID):(commitfn)),
     ((FALSEP(state))?(VOID):(state)),
     -1,
     opts);
  if (FALSEP(usecache)) kno_index_setcache(ix,0);
  if (ix->index_serialno>=0) return index_ref(ix);
  else return (lispval)ix;
}

DEFPRIM3("extindex-cacheadd!",extindex_cacheadd,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	 "`(EXTINDEX-CACHEADD! *arg0* *arg1* *arg2*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval extindex_cacheadd(lispval index,lispval key,lispval values)
{
  KNOTC *knotc = kno_threadcache;
  kno_index ix = kno_indexptr(index);
  if ( (ix) && (ix->index_handler == &kno_extindex_handler) )
    if (kno_hashtable_add(&(ix->index_cache),key,values)<0)
      return KNO_ERROR;
    else {}
  else return kno_type_error("extindex","extindex_cacheadd",index);
  if (knotc) {
    struct KNO_PAIR tempkey;
    struct KNO_HASHTABLE *h = &(knotc->indexes);
    KNO_INIT_STATIC_CONS(&tempkey,kno_pair_type);
    tempkey.car = kno_index2lisp(ix); tempkey.cdr = key;
    if (kno_hashtable_probe(h,(lispval)&tempkey)) {
      kno_hashtable_store(h,(lispval)&tempkey,VOID);}}
  return VOID;
}

DEFPRIM2("extindex-decache!",extindex_decache,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(EXTINDEX-DECACHE! *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval extindex_decache(lispval index,lispval key)
{
  KNOTC *knotc = kno_threadcache;
  kno_index ix = kno_indexptr(index);
  lispval lix = kno_index2lisp(ix);
  if ( (ix) && (ix->index_handler == &kno_extindex_handler) )
    if (VOIDP(key))
      if (kno_reset_hashtable(&(ix->index_cache),ix->index_cache.ht_n_buckets,1)<0)
	return KNO_ERROR;
      else {}
    else if (kno_hashtable_store(&(ix->index_cache),key,VOID)<0)
      return KNO_ERROR;
    else {}
  else return kno_type_error("extindex","extindex_decache",index);
  if ((knotc)&&(!(VOIDP(key)))) {
    struct KNO_PAIR tempkey;
    struct KNO_HASHTABLE *h = &(knotc->indexes);
    KNO_INIT_STATIC_CONS(&tempkey,kno_pair_type);
    tempkey.car = kno_index2lisp(ix); tempkey.cdr = key;
    if (kno_hashtable_probe(h,(lispval)&tempkey)) {
      kno_hashtable_store(h,(lispval)&tempkey,VOID);}}
  else if (knotc) {
    struct KNO_HASHTABLE *h = &(knotc->indexes);
    lispval keys = kno_hashtable_keys(h), drop = EMPTY;
    DO_CHOICES(key,keys) {
      if ((PAIRP(key))&&(KNO_CAR(key) == lix)) {
	kno_incref(key); CHOICE_ADD(drop,key);}}
    if (!(EMPTYP(drop))) {
      DO_CHOICES(d,drop) kno_hashtable_drop(h,d,VOID);}
    kno_decref(drop); kno_decref(keys);}
  else {}
  return VOID;
}

DEFPRIM1("extindex-fetchfn",extindex_fetchfn,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXTINDEX-FETCHFN *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval extindex_fetchfn(lispval index)
{
  kno_index ix = kno_indexptr(index);
  if ( (ix) && (ix->index_handler== &kno_extindex_handler) ) {
    struct KNO_EXTINDEX *eix = (struct KNO_EXTINDEX *)ix;
    return kno_incref(eix->fetchfn);}
  else return kno_type_error("extindex","extindex_fetchfn",index);
}

DEFPRIM1("extindex-commitfn",extindex_commitfn,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXTINDEX-COMMITFN *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval extindex_commitfn(lispval index)
{
  kno_index ix = kno_indexptr(index);
  if ( (ix) && (ix->index_handler== &kno_extindex_handler) ) {
    struct KNO_EXTINDEX *eix = (struct KNO_EXTINDEX *)ix;
    return kno_incref(eix->commitfn);}
  else return kno_type_error("extindex","extindex_commitfn",index);
}

DEFPRIM1("extindex-state",extindex_state,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXTINDEX-STATE *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval extindex_state(lispval index)
{
  kno_index ix = kno_indexptr(index);
  if ( (ix) && (ix->index_handler== &kno_extindex_handler) ) {
    struct KNO_EXTINDEX *eix = (struct KNO_EXTINDEX *)ix;
    return kno_incref(eix->state);}
  else return kno_type_error("extindex","extindex_state",index);
}

DEFPRIM1("extindex?",extindexp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXTINDEX *arg*)` "
	 "returns #t if *arg* is an extindex, #f otherwise",
	 kno_any_type,KNO_VOID);
static lispval extindexp(lispval index)
{
  kno_index ix = kno_indexptr(index);
  if ( (ix) && (ix->index_handler== &kno_extindex_handler) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Adding adjuncts */

static lispval padjuncts_symbol;

DEFPRIM3("use-adjunct",use_adjunct,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "(table [slot] [pool])\n"
	 "arranges for *table* to store values of the "
	 "slotid *slot* for objects in *pool*. If *pool* is "
	 "not specified, the adjunct is declared globally.",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval use_adjunct(lispval adjunct,lispval slotid,lispval pool_arg)
{
  if (STRINGP(adjunct)) {
    kno_index ix = kno_get_index(CSTRING(adjunct),0,VOID);
    if (ix) adjunct = kno_index2lisp(ix);
    else return kno_type_error("adjunct spec","use_adjunct",adjunct);}
  if ((VOIDP(slotid)) && (TABLEP(adjunct)))
    slotid = kno_get(adjunct,padjuncts_symbol,VOID);
  if ((SYMBOLP(slotid)) || (OIDP(slotid)))
    if (VOIDP(pool_arg))
      if (kno_set_adjunct(NULL,slotid,adjunct)<0)
	return KNO_ERROR;
      else return VOID;
    else {
      kno_pool p = kno_lisp2pool(pool_arg);
      if (p == NULL) return KNO_ERROR;
      else if (kno_set_adjunct(p,slotid,adjunct)<0)
	return KNO_ERROR;
      else return VOID;}
  else return kno_type_error(_("slotid"),"use_adjunct",slotid);
}

DEFPRIM3("adjunct!",add_adjunct,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	 "(pool slot table)\n"
	 "arranges for *table* to store values of the "
	 "slotid *slot* for objects in *pool*. Table can be "
	 "an in-memory table, an index or an adjunct pool",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval add_adjunct(lispval pool_arg,lispval slotid,lispval adjunct)
{
  if (STRINGP(adjunct)) {
    kno_index ix = kno_get_index(CSTRING(adjunct),0,VOID);
    if (ix) adjunct = kno_index2lisp(ix);
    else return kno_type_error("adjunct spec","use_adjunct",adjunct);}
  if ((VOIDP(slotid)) && (TABLEP(adjunct)))
    slotid = kno_get(adjunct,padjuncts_symbol,VOID);
  if ((SYMBOLP(slotid)) || (OIDP(slotid))) {
    kno_pool p = kno_lisp2pool(pool_arg);
    if (p == NULL)
      return KNO_ERROR;
    else if (kno_set_adjunct(p,slotid,adjunct)<0)
      return KNO_ERROR;
    else return VOID;}
  else return kno_type_error(_("slotid"),"use_adjunct",slotid);
}

DEFPRIM1("get-adjuncts",get_adjuncts_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(GET_ADJUNCTS pool)"
	 "\\nGets the adjuncts associated with the specified "
	 "pool",
	 kno_any_type,KNO_VOID);
static lispval get_adjuncts_prim(lispval pool_arg)
{
  kno_pool p=kno_lisp2pool(pool_arg);
  if (p==NULL)
    return KNO_ERROR;
  else return kno_get_adjuncts(p);
}

DEFPRIM2("get-adjunct",get_adjunct_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(GET_ADJUNCT pool *slotid*)"
	 "\\nGets the adjunct for *slotid* associated with the *pool*",
	 kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval get_adjunct_prim(lispval pool_arg,lispval slotid)
{
  if (KNO_VOIDP(slotid)) {
    kno_adjunct adj = kno_get_adjunct(NULL,pool_arg);
    if (adj == NULL) return KNO_FALSE;
    else return kno_incref(adj->table);}
  kno_pool p = (KNO_OIDP(pool_arg)) ?
    (kno_oid2pool(pool_arg)) :
    (kno_lisp2pool(pool_arg));
  if (p == NULL) return KNO_ERROR;
  kno_adjunct adj = kno_get_adjunct(p,slotid);
  if (adj == NULL) return KNO_FALSE;
  else return kno_incref(adj->table);
}

DEFPRIM2("adjunct-value",adjunct_value_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(ADJUNCT-VALUE *obj* *slotid*)"
	 "\\nGets the adjunct value for *slotid* of *obj*",
	 kno_oid_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval adjunct_value_prim(lispval oid,lispval slotid)
{
  kno_pool p = kno_oid2pool(oid);
  if (p==NULL)
    return kno_err("UncoveredOID","get_adjunct_prim",NULL,oid);
  else {
    kno_adjunct adj = kno_get_adjunct(p,slotid);
    if (adj == NULL) return KNO_FALSE;
    lispval table = adj->table;
    if (KNO_POOLP(table))
      return kno_pool_value(kno_lisp2pool(table),oid);
    else if (KNO_INDEXP(table))
      return kno_index_get(kno_lisp2index(table),oid);
    else return kno_get(table,oid,KNO_EMPTY);}
}

DEFPRIM1("adjunct?",isadjunctp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ADJUNCT? pool)`\n"
	 "Returns true if *pool* is an adjunct pool",
	 kno_any_type,KNO_VOID);
static lispval isadjunctp(lispval pool_arg)
{
  kno_pool p=kno_lisp2pool(pool_arg);
  if (p==NULL)
    return KNO_ERROR;
  else if ((p->pool_flags) & (KNO_POOL_ADJUNCT))
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* DB control functions */

DEFPRIM("swapout",swapout_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDCALL,
	"`(SWAPOUT *args...*)` **undocumented**");
static lispval swapout_lexpr(int n,kno_argvec args)
{
  if (n == 0) {
    kno_swapout_indexes();
    kno_swapout_pools();
    return VOID;}
  else if (n == 1) {
    long long rv_sum = 0;
    lispval arg = args[0];
    if (KNO_EMPTY_CHOICEP(arg)) {}
    else if (CHOICEP(arg)) {
      int rv = 0;
      lispval oids = EMPTY;
      DO_CHOICES(e,arg) {
	if (OIDP(e)) {CHOICE_ADD(oids,e);}
	else if (KNO_POOLP(e))
	  rv = kno_pool_swapout(kno_lisp2pool(e),VOID);
	else if (INDEXP(e))
	  kno_index_swapout(kno_indexptr(e),VOID);
	else if (TYPEP(arg,kno_consed_pool_type))
	  rv = kno_pool_swapout((kno_pool)arg,VOID);
	else if (STRINGP(e)) {
	  kno_pool p = kno_name2pool(CSTRING(e));
	  if (!(p)) {
	    kno_decref(oids);
	    return kno_type_error(_("pool, index, or OIDs"),
				  "swapout_lexpr",e);}
	  else rv = kno_pool_swapout(p,VOID);}
	else {
	  kno_decref(oids);
	  return kno_type_error(_("pool, index, or OIDs"),
				"swapout_lexpr",e);}
	if (rv<0) {
	  u8_log(LOG_WARN,"SwapoutFailed","Error swapping out %q",e);
	  kno_clear_errors(1);}
	else rv_sum = rv_sum+rv;}
      kno_swapout_oids(oids);
      kno_decref(oids);
      return KNO_INT(rv_sum);}
    else if (OIDP(arg))
      rv_sum = kno_swapout_oid(arg);
    else if (TYPEP(arg,kno_index_type))
      kno_index_swapout(kno_indexptr(arg),VOID);
    else if (TYPEP(arg,kno_pool_type))
      rv_sum = kno_pool_swapout(kno_lisp2pool(arg),VOID);
    else if (TYPEP(arg,kno_consed_index_type))
      kno_index_swapout(kno_indexptr(arg),VOID);
    else if (TYPEP(arg,kno_consed_pool_type))
      rv_sum = kno_pool_swapout((kno_pool)arg,VOID);
    else return kno_type_error(_("pool, index, or OIDs"),"swapout_lexpr",arg);
    return KNO_INT(rv_sum);}
  else if (n>2)
    return kno_err(kno_TooManyArgs,"swapout",NULL,VOID);
  else if (KNO_EMPTY_CHOICEP(args[1]))
    return KNO_INT(0);
  else {
    lispval arg, keys; int rv_sum = 0;
    if ((TYPEP(args[0],kno_pool_type))||
	(TYPEP(args[0],kno_index_type))||
	(TYPEP(args[0],kno_consed_pool_type))||
	(TYPEP(args[0],kno_consed_index_type))) {
      arg = args[0]; keys = args[1];}
    else {arg = args[0]; keys = args[1];}
    if (TYPEP(arg,kno_index_type))
      kno_index_swapout(kno_indexptr(arg),keys);
    else if (TYPEP(arg,kno_pool_type))
      rv_sum = kno_pool_swapout(kno_lisp2pool(arg),keys);
    else if (TYPEP(arg,kno_consed_index_type))
      kno_index_swapout(kno_indexptr(arg),keys);
    else if (TYPEP(arg,kno_consed_pool_type))
      rv_sum = kno_pool_swapout((kno_pool)arg,keys);
    else return kno_type_error(_("pool, index, or OIDs"),"swapout_lexpr",arg);
    if (rv_sum<0) return KNO_ERROR;
    else return KNO_INT(rv_sum);}
}

DEFPRIM("commit",commit_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(0),
	"`(COMMIT *args...*)` **undocumented**");
static lispval commit_lexpr(int n,kno_argvec args)
{
  if (n == 0) {
    if (kno_commit_indexes()<0)
      return KNO_ERROR;
    if (kno_commit_pools()<0)
      return KNO_ERROR;
    return VOID;}
  else if (n == 1) {
    lispval arg = args[0]; int retval = 0;
    if (TYPEP(arg,kno_index_type))
      retval = kno_commit_index(kno_indexptr(arg));
    else if (TYPEP(arg,kno_pool_type))
      retval = kno_commit_all_oids(kno_lisp2pool(arg));
    else if (TYPEP(arg,kno_consed_index_type))
      retval = kno_commit_index(kno_indexptr(arg));
    else if (TYPEP(arg,kno_consed_pool_type))
      retval = kno_commit_all_oids((kno_pool)arg);
    else if (OIDP(arg))
      retval = kno_commit_oids(arg);
    else return kno_type_error(_("pool or index"),"commit_lexpr",arg);
    if (retval<0) return KNO_ERROR;
    else return VOID;}
  else return kno_err(kno_TooManyArgs,"commit",NULL,VOID);
}

DEFPRIM1("commit-oids",commit_oids,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "`(COMMIT-OIDS *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval commit_oids(lispval oids)
{
  int rv = kno_commit_oids(oids);
  if (rv<0)
    return KNO_ERROR;
  else return VOID;
}

DEFPRIM2("finish-oids",finish_oids,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "`(FINISH-OIDS *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_pool_type,KNO_VOID);
static lispval finish_oids(lispval oids,lispval pool)
{
  kno_pool p = (VOIDP(pool))? (NULL) : (kno_lisp2pool(pool));
  if (EMPTYP(oids)) return VOID;
  else if (p) {
    int rv = kno_pool_finish(p,oids);
    if (rv<0)
      return KNO_ERROR;
    else return VOID;}
  else {
    int rv = kno_finish_oids(oids);
    if (rv<0)
      return KNO_ERROR;
    else return VOID;}
}

DEFPRIM2("commit-pool",commit_pool,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(COMMIT-POOL *arg0* [*arg1*])` **undocumented**",
	 kno_pool_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval commit_pool(lispval pool,lispval opts)
{
  kno_pool p = kno_lisp2pool(pool);
  if (!(p))
    return kno_type_error("pool","commit_pool",pool);
  else {
    int rv = kno_commit_pool(p,VOID);
    if (rv<0)
      return KNO_ERROR;
    else return VOID;}
}

DEFPRIM1("commit-finished",commit_finished,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(COMMIT-FINISHED *arg0*)` **undocumented**",
	 kno_pool_type,KNO_VOID);
static lispval commit_finished(lispval pool)
{
  kno_pool p = kno_lisp2pool(pool);
  if (!(p))
    return kno_type_error("pool","commit_finished",pool);
  else {
    int rv = kno_commit_pool(p,KNO_TRUE);
    if (rv<0)
      return KNO_ERROR;
    else return VOID;}
}

DEFPRIM3("pool/storen!",pool_storen_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	 "Stores values in a pool, skipping the object cache",
	 kno_any_type,KNO_VOID,kno_vector_type,KNO_VOID,
	 kno_vector_type,KNO_VOID);
static lispval pool_storen_prim(lispval pool,lispval oids,lispval values)
{
  kno_pool p = kno_lisp2pool(pool);
  if (!(p))
    return kno_type_error("pool","pool_storen_prim",pool);

  long long oid_len = KNO_VECTOR_LENGTH(oids);
  if (KNO_VECTOR_LENGTH(oids) != KNO_VECTOR_LENGTH(values)) {
    long long v_len = KNO_VECTOR_LENGTH(values);
    lispval intpair = kno_make_pair(KNO_INT(oid_len),KNO_INT(v_len));
    lispval rlv=kno_err("OIDs/Values mismatch","pool_storen_prim",p->poolid,
			intpair);
    kno_decref(intpair);
    return rlv;}
  int rv = kno_pool_storen(p,KNO_VECTOR_LENGTH(oids),
			   KNO_VECTOR_ELTS(oids),
			   KNO_VECTOR_ELTS(values));
  if (rv<0)
    return KNO_ERROR_VALUE;
  else return KNO_INT(oid_len);
}

DEFPRIM2("pool/fetchn",pool_fetchn_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "Fetches values from a pool, skipping the object "
	 "cache",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval pool_fetchn_prim(lispval pool,lispval oids)
{
  kno_pool p = kno_lisp2pool(pool);
  if (!(p))
    return kno_type_error("pool","pool_fetchn_prim",pool);

  return kno_pool_fetchn(p,oids);
}

DEFPRIM1("clear-slotcache!",clear_slotcache,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	 "`(CLEAR-SLOTCACHE! [*arg0*])` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval clear_slotcache(lispval arg)
{
  if (VOIDP(arg)) kno_clear_slotcaches();
  else kno_clear_slotcache(arg);
  return VOID;
}

DEFPRIM("clearcaches",clearcaches,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	"`(CLEARCACHES)` **undocumented**");
static lispval clearcaches()
{
  kno_clear_callcache(VOID);
  kno_clear_slotcaches();
  kno_swapout_indexes();
  kno_swapout_pools();
  return VOID;
}

DEFPRIM("swapcheck",swapcheck_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	"`(SWAPCHECK)` **undocumented**");
static lispval swapcheck_prim()
{
  if (kno_swapcheck()) return KNO_TRUE;
  else return KNO_FALSE;
}

static kno_pool arg2pool(lispval arg)
{
  if (KNO_POOLP(arg)) return kno_lisp2pool(arg);
  else if (TYPEP(arg,kno_consed_pool_type))
    return (kno_pool)arg;
  else if (STRINGP(arg)) {
    kno_pool p = kno_name2pool(CSTRING(arg));
    if (p) return p;
    else return kno_use_pool(CSTRING(arg),0,VOID);}
  else if (SYMBOLP(arg)) {
    lispval v = kno_config_get(SYM_NAME(arg));
    if (STRINGP(v))
      return kno_use_pool(CSTRING(v),0,VOID);
    else return NULL;}
  else return NULL;
}

DEFPRIM1("pool-load",pool_load,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(POOL-LOAD *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval pool_load(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_load",arg);
  else {
    int load = kno_pool_load(p);
    if (load>=0) return KNO_INT(load);
    else return KNO_ERROR;}
}

DEFPRIM1("pool-capacity",pool_capacity,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(POOL-CAPACITY *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval pool_capacity(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_capacity",arg);
  else return KNO_INT(p->pool_capacity);
}

DEFPRIM1("pool-base",pool_base,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(POOL-BASE *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval pool_base(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_base",arg);
  else return kno_make_oid(p->pool_base);
}

DEFPRIM3("pool-elts",pool_elts,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "`(POOL-ELTS *arg0* [*arg1*] [*arg2*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval pool_elts(lispval arg,lispval start,lispval count)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_elts",arg);
  else {
    int i = 0, lim = kno_pool_load(p);
    lispval result = EMPTY;
    KNO_OID base = p->pool_base;
    if (lim<0) return KNO_ERROR;
    if (VOIDP(start)) {}
    else if (KNO_UINTP(start))
      if (FIX2INT(start)<0)
	return kno_type_error(_("pool offset"),"pool_elts",start);
      else i = FIX2INT(start);
    else if (OIDP(start))
      i = KNO_OID_DIFFERENCE(KNO_OID_ADDR(start),base);
    else return kno_type_error(_("pool offset"),"pool_elts",start);
    if (VOIDP(count)) {}
    else if (KNO_UINTP(count)) {
      int count_arg = FIX2INT(count);
      if (count_arg<0)
	return kno_type_error(_("pool offset"),"pool_elts",count);
      else if (i+count_arg<lim) lim = i+count_arg;}
    else if (OIDP(start)) {
      int lim_arg = KNO_OID_DIFFERENCE(KNO_OID_ADDR(count),base);
      if (lim_arg<lim) lim = lim_arg;}
    else return kno_type_error(_("pool offset"),"pool_elts",count);
    int off=i, partition=-1, bucket_no=-1, bucket_start=-1;
    while (off<lim) {
      if ( (partition<0) || ((off/KNO_OID_BUCKET_SIZE) != partition) ) {
	KNO_OID addr=KNO_OID_PLUS(base,off);
	bucket_no=kno_get_oid_base_index(addr,1);
	partition=off/KNO_OID_BUCKET_SIZE;
	bucket_start=partition*KNO_OID_BUCKET_SIZE;}
      lispval each=KNO_CONSTRUCT_OID(bucket_no,off-bucket_start);
      CHOICE_ADD(result,each);
      off++;}
    return result;}
}

DEFPRIM2("pool-label",pool_label,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(POOL-LABEL *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval pool_label(lispval arg,lispval use_source)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_label",arg);
  else if (p->pool_label)
    return kno_mkstring(p->pool_label);
  else if (FALSEP(use_source)) return KNO_FALSE;
  else if (p->pool_source)
    return kno_mkstring(p->pool_source);
  else return KNO_FALSE;
}

DEFPRIM1("pool-id",pool_id,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(POOL-ID *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval pool_id(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_id",arg);
  else if (p->pool_label)
    return kno_mkstring(p->pool_label);
  else if (p->poolid)
    return kno_mkstring(p->poolid);
  else if (p->pool_source)
    return kno_mkstring(p->pool_source);
  else if (p->pool_prefix)
    return kno_mkstring(p->pool_prefix);
  else return KNO_FALSE;
}

DEFPRIM1("pool-source",pool_source,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(POOL-SOURCE *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval pool_source(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_label",arg);
  else if (p->pool_source)
    return kno_mkstring(p->pool_source);
  else if (p->poolid)
    return kno_mkstring(p->pool_source);
  else return KNO_FALSE;
}

DEFPRIM1("pool-prefix",pool_prefix,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(POOL-PREFIX *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval pool_prefix(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_label",arg);
  else if (p->pool_prefix)
    return kno_mkstring(p->pool_prefix);
  else return KNO_FALSE;
}

DEFPRIM2("set-pool-prefix!",set_pool_prefix,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(SET-POOL-PREFIX! *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID);
static lispval set_pool_prefix(lispval arg,lispval prefix_arg)
{
  kno_pool p = arg2pool(arg);
  u8_string prefix = CSTRING(prefix_arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"set_pool_prefix",arg);
  else if ((p->pool_prefix)&&(strcmp(p->pool_prefix,prefix)==0))
    return KNO_FALSE;
  else if (p->pool_prefix) {
    u8_string copy = u8_strdup(prefix);
    u8_string cur = p->pool_prefix;
    p->pool_prefix = copy;
    return kno_init_string(NULL,-1,cur);}
  else {
    u8_string copy = u8_strdup(prefix);
    p->pool_prefix = copy;
    return KNO_TRUE;}
}

DEFPRIM1("pool-close",pool_close_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(POOL-CLOSE *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval pool_close_prim(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_close",arg);
  else {
    kno_pool_close(p);
    return VOID;}
}

DEFPRIM2("oid-range",oid_range,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(OID-RANGE *arg0* *arg1*)` **undocumented**",
	 kno_oid_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval oid_range(lispval start,lispval end)
{
  int i = 0, lim = kno_getint(end);
  lispval result = EMPTY;
  KNO_OID base = KNO_OID_ADDR(start);
  if (lim<0) return KNO_ERROR;
  else while (i<lim) {
      lispval each = kno_make_oid(KNO_OID_PLUS(base,i));
      CHOICE_ADD(result,each); i++;}
  return result;
}

DEFPRIM2("oid-vector",oid_vector,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(OID-VECTOR *arg0* *arg1*)` **undocumented**",
	 kno_oid_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval oid_vector(lispval start,lispval end)
{
  int i = 0, lim = kno_getint(end);
  if (lim<0) return KNO_ERROR;
  else {
    lispval result = kno_empty_vector(lim);
    lispval *data = VEC_DATA(result);
    KNO_OID base = KNO_OID_ADDR(start);
    while (i<lim) {
      lispval each = kno_make_oid(KNO_OID_PLUS(base,i));
      data[i++]=each;}
    return result;}
}

DEFPRIM1("random-oid",random_oid,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(RANDOM-OID *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval random_oid(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"random_oid",arg);
  else {
    int load = kno_pool_load(p);
    if (load>0) {
      KNO_OID base = p->pool_base; int i = u8_random(load);
      return kno_make_oid(KNO_OID_PLUS(base,i));}
    kno_seterr("No OIDs","random_oid",p->poolid,arg);
    return KNO_ERROR;}
}

DEFPRIM1("pool-vector",pool_vec,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(POOL-VECTOR *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval pool_vec(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_vec",arg);
  else {
    int i = 0, lim = kno_pool_load(p);
    if (lim<0) return KNO_ERROR;
    else {
      lispval result = kno_empty_vector(lim);
      KNO_OID base = p->pool_base;
      if (lim<0) {
	kno_seterr("No OIDs","pool_vec",p->poolid,arg);
	return KNO_ERROR;}
      else while (i<lim) {
	  lispval each = kno_make_oid(KNO_OID_PLUS(base,i));
	  KNO_VECTOR_SET(result,i,each); i++;}
      return result;}}
}

DEFPRIM1("cachecount",cachecount,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	 "`(CACHECOUNT [*arg0*])` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval cachecount(lispval arg)
{
  kno_pool p = NULL; kno_index ix = NULL;
  if (VOIDP(arg)) {
    int count = kno_object_cache_load()+kno_index_cache_load();
    return KNO_INT(count);}
  else if (KNO_EQ(arg,pools_symbol)) {
    int count = kno_object_cache_load();
    return KNO_INT(count);}
  else if (KNO_EQ(arg,indexes_symbol)) {
    int count = kno_index_cache_load();
    return KNO_INT(count);}
  else if ((p = (kno_lisp2pool(arg)))) {
    int count = p->pool_cache.table_n_keys;
    return KNO_INT(count);}
  else if ((ix = (kno_indexptr(arg)))) {
    int count = ix->index_cache.table_n_keys;
    return KNO_INT(count);}
  else return kno_type_error(_("pool or index"),"cachecount",arg);
}

/* OID functions */

DEFPRIM1("oid-hi",oid_hi,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(OID-HI *arg0*)` **undocumented**",
	 kno_oid_type,KNO_VOID);
static lispval oid_hi(lispval x)
{
  KNO_OID addr = KNO_OID_ADDR(x);
  return KNO_INT(KNO_OID_HI(addr));
}

DEFPRIM1("oid-lo",oid_lo,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(OID-LO *arg0*)` **undocumented**",
	 kno_oid_type,KNO_VOID);
static lispval oid_lo(lispval x)
{
  KNO_OID addr = KNO_OID_ADDR(x);
  return KNO_INT(KNO_OID_LO(addr));
}

DEFPRIM2("oid-base",oid_base,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(OID-LO *arg0*)` **undocumented**",
	 kno_oid_type,KNO_VOID,kno_fixnum_type,KNO_INT(0x100000));
static lispval oid_base(lispval oid,lispval modulo)
{
  long long modval = KNO_FIX2INT(modulo);
  if (modval > 100000000)
    return kno_err("Modulo too large","oid_base",NULL,modulo);
  if (modval <= KNO_OID_BUCKET_SIZE) {
    long base = KNO_OID_BASE_ID(oid);
    long off  = KNO_OID_BASE_OFFSET(oid);
    return KNO_CONSTRUCT_OID(base,modval*(off/modval));}
  KNO_OID addr = KNO_OID_ADDR(oid);
  long hi = KNO_OID_HI(addr);
  long lo = KNO_OID_LO(addr);
  return KNO_MAKE_OID(hi,modval*(lo/modval));
}

DEFPRIM1("oid?",oidp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(OID? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval oidp(lispval x)
{
  if (OIDP(x)) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("oid-pool",oidpool,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(OID-POOL *arg0*)` **undocumented**",
	 kno_oid_type,KNO_VOID);
static lispval oidpool(lispval x)
{
  kno_pool p = kno_oid2pool(x);
  if (p == NULL) return EMPTY;
  else return kno_pool2lisp(p);
}

DEFPRIM2("in-pool?",inpoolp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(IN-POOL? *arg0* *arg1*)` **undocumented**",
	 kno_oid_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval inpoolp(lispval x,lispval pool_arg)
{
  kno_pool p = kno_lisp2pool(pool_arg);
  if ( (p->pool_flags) & (KNO_POOL_ADJUNCT) ) {
    KNO_OID base = p->pool_base;
    KNO_OID edge = KNO_OID_PLUS(base,p->pool_capacity);
    KNO_OID addr = KNO_OID_ADDR(x);
    if ( (addr >= base) && (addr < edge) )
      return KNO_TRUE;
    else return KNO_FALSE;}
  else {
    kno_pool op = kno_oid2pool(x);
    if (p == op)
      return KNO_TRUE;
    else return KNO_FALSE;}
}

DEFPRIM2("valid-oid?",validoidp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(VALID-OID? *arg0* [*arg1*])` **undocumented**",
	 kno_oid_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval validoidp(lispval x,lispval pool_arg)
{
  if (VOIDP(pool_arg)) {
    kno_pool p = kno_oid2pool(x);
    if (p == NULL) return KNO_FALSE;
    else {
      KNO_OID base = p->pool_base, addr = KNO_OID_ADDR(x);
      unsigned int offset = KNO_OID_DIFFERENCE(addr,base);
      int load = kno_pool_load(p);
      if (load<0) return KNO_ERROR;
      else if (offset<load) return KNO_TRUE;
      else return KNO_FALSE;}}
  else {
    kno_pool p = kno_lisp2pool(pool_arg);
    kno_pool op = kno_oid2pool(x);
    if (p == op) {
      KNO_OID base = p->pool_base, addr = KNO_OID_ADDR(x);
      unsigned int offset = KNO_OID_DIFFERENCE(addr,base);
      int load = kno_pool_load(p);
      if (load<0) return KNO_ERROR;
      else if (offset<load) return KNO_TRUE;
      else return KNO_FALSE;}
    else return KNO_FALSE;}
}

/* Prefetching functions */

DEFPRIM2("pool-prefetch!",pool_prefetch_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "'(POOL-PREFETCH! pool oids)' prefetches OIDs from "
	 "pool",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval pool_prefetch_prim(lispval pool,lispval oids)
{
  if ( (VOIDP(pool)) || (KNO_FALSEP(pool)) || (KNO_TRUEP(pool)) ) {
    if (kno_prefetch_oids(oids)>=0)
      return KNO_TRUE;
    else return KNO_ERROR;}
  else if (! (CHOICEP(pool)) ) {
    kno_pool p=kno_lisp2pool(pool);
    if (p==NULL)
      return KNO_ERROR_VALUE;
    else if (kno_pool_prefetch(p,oids)>=0)
      return KNO_TRUE;
    else return KNO_ERROR;}
  else {
    {KNO_DO_CHOICES(spec,pool) {
	kno_pool p=kno_lisp2pool(spec);
	if (p==NULL) {
	  KNO_STOP_DO_CHOICES;
	  return KNO_ERROR;}
	else {}}}
    {int ok=0;
      KNO_DO_CHOICES(spec,pool) {
	kno_pool p=kno_lisp2pool(spec);
	int rv=kno_pool_prefetch(p,oids);
	if (rv<0) ok=rv;}
      return (ok) ? (KNO_TRUE) : (KNO_FALSE);}}
}

DEFPRIM2("prefetch-oids!",prefetch_oids_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "'(PREFETCH-OIDS! oids [pool])' prefetches OIDs "
	 "from pool",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval prefetch_oids_prim(lispval oids,lispval parg)
{
  if ( (VOIDP(parg)) || (KNO_FALSEP(parg)) || (KNO_TRUEP(parg)) ) {
    if (kno_prefetch_oids(oids)>=0)
      return KNO_TRUE;
    else return KNO_ERROR;}
  else return pool_prefetch_prim(parg,oids);
}

DEFPRIM1("fetchoids",fetchoids_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "(FETCHOIDS *oids*) "
	 "returns *oids* after prefetching their values.",
	 kno_any_type,KNO_VOID);
static lispval fetchoids_prim(lispval oids)
{
  kno_prefetch_oids(oids);
  return kno_incref(oids);
}

DEFPRIM2("prefetch-keys!",prefetch_keys,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "`(PREFETCH-KEYS! *index* *keys*)` or `(PREFETCH-KEYS! *keys*)`, moves "
	 "mappings for *keys* in *index* into its cache. With one argument "
	 "caches from the background.",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval prefetch_keys(lispval arg1,lispval arg2)
{
  if (VOIDP(arg2)) {
    if (kno_bg_prefetch(arg1)<0)
      return KNO_ERROR;
    else return VOID;}
  else {
    DO_CHOICES(arg,arg1) {
      if (INDEXP(arg)) {
	kno_index ix = kno_indexptr(arg);
	if (kno_index_prefetch(ix,arg2)<0) {
	  KNO_STOP_DO_CHOICES;
	  return KNO_ERROR;}}
      else return kno_type_error(_("index"),"prefetch_keys",arg);}
    return VOID;}
}

DEFPRIM2("index-prefetch!",index_prefetch_keys,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "(INDEX-PREFETCH! *index* *keys*)  moves mappings "
	 "for *keys* in *index* into its cache.",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval index_prefetch_keys(lispval ix_arg,lispval keys)
{
  DO_CHOICES(arg,ix_arg) {
    if (INDEXP(arg)) {
      kno_index ix = kno_indexptr(arg);
      if (kno_index_prefetch(ix,keys)<0) {
	KNO_STOP_DO_CHOICES;
	return KNO_ERROR;}}
    else return kno_type_error(_("index"),"prefetch_keys",arg);}
  return KNO_VOID;
}

/* Getting cached OIDs */

DEFPRIM1("cached-oids",cached_oids,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	 "`(CACHED-OIDS [*arg0*])` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval cached_oids(lispval pool)
{
  if ((VOIDP(pool)) || (KNO_TRUEP(pool)))
    return kno_cached_oids(NULL);
  else {
    kno_pool p = kno_lisp2pool(pool);
    if (p)
      return kno_cached_oids(p);
    else return kno_type_error(_("pool"),"cached_oids",pool);}
}

DEFPRIM1("cached-keys",cached_keys,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	 "`(CACHED-KEYS [*arg0*])` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval cached_keys(lispval index)
{
  if ((VOIDP(index)) || (KNO_TRUEP(index)))
    return kno_cached_keys(NULL);
  else {
    kno_index ix = kno_indexptr(index);
    if (ix)
      return kno_cached_keys(ix);
    else return kno_type_error(_("index"),"cached_keys",index);}
}

DEFPRIM1("cache-load",cache_load,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(CACHE-LOAD *pool-or-index*) "
	 "returns the number of cached items in a database.",
	 kno_any_type,KNO_VOID);
static lispval cache_load(lispval db)
{
  if ( (KNO_POOLP(db)) || (TYPEP(db,kno_consed_pool_type) ) ) {
    kno_pool p = kno_lisp2pool(db);
    int n_keys = p->pool_cache.table_n_keys;
    return KNO_INT(n_keys);}
  else if (INDEXP(db)) {
    kno_index ix = kno_lisp2index(db);
    int n_keys = ix->index_cache.table_n_keys;
    return KNO_INT(n_keys);}
  else return kno_err(kno_TypeError,"cache_load",_("pool or index"),db);
}

DEFPRIM1("change-load",change_load,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the number of items modified (or locked "
	 "for modification) in a database",
	 kno_any_type,KNO_VOID);
static lispval change_load(lispval db)
{
  if ( (KNO_POOLP(db)) || (TYPEP(db,kno_consed_pool_type) ) ) {
    kno_pool p = kno_lisp2pool(db);
    int n_pending = p->pool_changes.table_n_keys;
    return KNO_INT(n_pending);}
  else if (INDEXP(db)) {
    kno_index ix = kno_lisp2index(db);
    int n_pending = ix->index_adds.table_n_keys +
      ix->index_drops.table_n_keys +
      ix->index_stores.table_n_keys ;
    return KNO_INT(n_pending);}
  else return kno_err(kno_TypeError,"change_load",_("pool or index"),db);
}

/* Frame get functions */

DEFPRIM2("get",kno_fget,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(GET *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
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

DEFPRIM3("test",kno_ftest,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(TEST *arg0* *arg1* [*arg2*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
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

DEFPRIM3("assert!",kno_assert,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	 "`(ASSERT! *arg0* *arg1* *arg2*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
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
DEFPRIM3("retract!",kno_retract,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(RETRACT! *arg0* *arg1* [*arg2*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
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

DEFPRIM("testp",testp,KNO_VAR_ARGS|KNO_MIN_ARGS(3)|KNO_NDCALL,
	"`(TESTP *arg0* *arg1* *arg2* *args...*)` **undocumented**");
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

DEFPRIM("getpath",getpath_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	"`(GETPATH *arg0* *args...*)` **undocumented**");
static lispval getpath_prim(int n,kno_argvec args)
{
  lispval result = kno_getpath(args[0],n-1,args+1,1,0);
  return kno_simplify_choice(result);
}

DEFPRIM("getpath*",getpathstar_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	"`(GETPATH* *arg0* *args...*)` **undocumented**");
static lispval getpathstar_prim(int n,kno_argvec args)
{
  lispval result = kno_getpath(args[0],n-1,args+1,1,0);
  return kno_simplify_choice(result);
}

/* Cache gets */

static lispval cacheget_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval table_arg = kno_get_arg(expr,1), key_arg = kno_get_arg(expr,2);
  lispval default_expr = kno_get_arg(expr,3);
  if (PRED_FALSE((VOIDP(table_arg)) ||
		 (VOIDP(key_arg)) ||
		 (VOIDP(default_expr))))
    return kno_err(kno_SyntaxError,"cacheget_evalfn",NULL,expr);
  else {
    lispval table = kno_eval(table_arg,env,_stack,0), key, value;
    if (KNO_ABORTED(table)) return table;
    else if (TABLEP(table)) key = kno_eval(key_arg,env,_stack,0);
    else return kno_type_error(_("table"),"cachget_evalfn",table);
    if (KNO_ABORTED(key)) {
      kno_decref(table); return key;}
    else value = kno_get(table,key,VOID);
    if (VOIDP(value)) {
      lispval dflt = kno_eval(default_expr,env,_stack,0);
      if (KNO_ABORTED(dflt)) {
	kno_decref(table); kno_decref(key);
	return dflt;}
      kno_store(table,key,dflt);
      return dflt;}
    else return value;}
}

/* Getting info from indexes */

static kno_index arg2index(lispval arg)
{
  if (KNO_INDEXP(arg))
    return kno_lisp2index(arg);
  else if (TYPEP(arg,kno_consed_index_type))
    return (kno_index)arg;
  else if (STRINGP(arg)) {
    kno_index ix = kno_find_index(CSTRING(arg));
    if (ix) return ix;
    else return NULL;}
  else if (SYMBOLP(arg)) {
    lispval v = kno_config_get(SYM_NAME(arg));
    if (STRINGP(v))
      return kno_find_index(CSTRING(v));
    else return NULL;}
  else return NULL;
}

DEFPRIM1("index-id",index_id,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(INDEX-ID *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval index_id(lispval arg)
{
  kno_index ix = arg2index(arg);
  if (ix == NULL)
    return kno_type_error(_("index spec"),"index_id",arg);
  else if (ix->indexid)
    return kno_mkstring(ix->indexid);
  else if (ix->index_source)
    return kno_mkstring(ix->index_source);
  else return KNO_FALSE;
}

DEFPRIM1("index-source",index_source_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(INDEX-SOURCE *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval index_source_prim(lispval arg)
{
  kno_index p = arg2index(arg);
  if (p == NULL)
    return kno_type_error(_("index spec"),"index_label",arg);
  else if (p->index_source)
    return kno_mkstring(p->index_source);
  else return KNO_FALSE;
}

/* Index operations */

DEFPRIM2("index-get",index_get,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(INDEX-GET *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval index_get(lispval ixarg,lispval key)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL)
    return KNO_ERROR;
  else return kno_index_get(ix,key);
}

DEFPRIM3("index-add!",index_add,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	 "`(INDEX-ADD! *arg0* *arg1* *arg2*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval index_add(lispval ixarg,lispval key,lispval values)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL) return KNO_ERROR;
  kno_index_add(ix,key,values);
  return VOID;
}

DEFPRIM3("index-set!",index_set,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	 "`(INDEX-SET! *arg0* *arg1* *arg2*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval index_set(lispval ixarg,lispval key,lispval values)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL) return KNO_ERROR;
  kno_index_store(ix,key,values);
  return VOID;
}

DEFPRIM3("index-decache",index_decache,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(INDEX-DECACHE *arg0* *arg1* [*arg2*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval index_decache(lispval ixarg,lispval key,lispval value)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL) return KNO_ERROR;
  if (VOIDP(value))
    kno_hashtable_op(&(ix->index_cache),kno_table_replace,key,VOID);
  else {
    lispval keypair = kno_conspair(kno_incref(key),kno_incref(value));
    kno_hashtable_op(&(ix->index_cache),kno_table_replace,keypair,VOID);
    kno_decref(keypair);}
  return VOID;
}

DEFPRIM2("bgdecache",bgdecache,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(BGDECACHE *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval bgdecache(lispval key,lispval value)
{
  kno_index ix = (kno_index)kno_background;
  if (ix == NULL) return KNO_ERROR;
  if (VOIDP(value))
    kno_hashtable_op(&(ix->index_cache),kno_table_replace,key,VOID);
  else {
    lispval keypair = kno_conspair(kno_incref(key),kno_incref(value));
    kno_hashtable_op(&(ix->index_cache),kno_table_replace,keypair,VOID);
    kno_decref(keypair);}
  return VOID;
}

DEFPRIM1("index-keys",index_keys,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(INDEX-KEYS *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval index_keys(lispval ixarg)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL) kno_type_error("index","index_keys",ixarg);
  return kno_index_keys(ix);
}

DEFPRIM2("index-sizes",index_sizes,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(INDEX-SIZES *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval index_sizes(lispval ixarg,lispval keys_arg)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL)
    kno_type_error("index","index_sizes",ixarg);
  if (KNO_VOIDP(keys_arg))
    return kno_index_sizes(ix);
  else return kno_index_keysizes(ix,keys_arg);
}

DEFPRIM1("index-keysvec",index_keysvec,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(INDEX-KEYSVEC *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval index_keysvec(lispval ixarg)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL) kno_type_error("index","index_keysvec",ixarg);
  if (ix->index_handler->fetchkeys) {
    lispval *keys; unsigned int n_keys;
    keys = ix->index_handler->fetchkeys(ix,&n_keys);
    return kno_cons_vector(NULL,n_keys,1,keys);}
  else return kno_index_keys(ix);
}

DEFPRIM2("index/merge!",index_merge,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "Merges a hashtable into the ADDS of an index as a "
	 "batch operation",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval index_merge(lispval ixarg,lispval addstable)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL)
    return kno_type_error("index","index_merge",ixarg);
  else if (KNO_HASHTABLEP(addstable)) {
    int rv = kno_index_merge(ix,(kno_hashtable)addstable);
    return KNO_INT(rv);}
  else {
    kno_index add_index = kno_indexptr(addstable);
    if (add_index == NULL)
      return kno_type_error("tempindex|hashtable","index_merge",addstable);
    else if (kno_tempindexp(add_index)) {
      int rv = kno_index_merge(ix,&(add_index->index_adds));
      return KNO_INT(rv);}
    else return kno_type_error("tempindex|hashtable","index_merge",addstable);}
}

DEFPRIM2("slotindex/merge!",slotindex_merge,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "Merges a hashtable or temporary index into the "
	 "ADDS of an index as a batch operation, trying to "
	 "handle conversions between slotkeys if needed.",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval slotindex_merge(lispval ixarg,lispval add)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL)
    return kno_type_error("index","index_merge",ixarg);
  else {
    int rv = kno_slotindex_merge(ix,add);
    return KNO_INT(rv);}
}

DEFPRIM1("index-source",index_source,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(INDEX-SOURCE *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval index_source(lispval ix_arg)
{
  kno_index ix = kno_indexptr(ix_arg);
  if (ix == NULL)
    return kno_type_error("index","index_source",ix_arg);
  else if (ix->index_source)
    return kno_mkstring(ix->index_source);
  else return EMPTY;
}

DEFPRIM1("close-index",close_index_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(INDEX-CLOSE *index*) "
	 "closes any resources associated with *index*",
	 kno_any_type,KNO_VOID);
static lispval close_index_prim(lispval ix_arg)
{
  kno_index ix = kno_indexptr(ix_arg);
  if (ix == NULL)
    return kno_type_error("index","index_close",ix_arg);
  kno_index_close(ix);
  return VOID;
}

DEFPRIM1("commit-index",commit_index_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(INDEX-COMMIT *index*) "
	 "saves any buffered changes to *index*",
	 kno_any_type,KNO_VOID);
static lispval commit_index_prim(lispval ix_arg)
{
  kno_index ix = kno_indexptr(ix_arg);
  if (ix == NULL)
    return kno_type_error("index","index_close",ix_arg);
  kno_commit_index(ix);
  return VOID;
}

DEFPRIM5("index/save!",index_save_prim,KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
	 "(INDEX-PREFETCH! *index* *keys*) **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval index_save_prim(lispval index,
			       lispval adds,lispval drops,
			       lispval stores,
			       lispval metadata)
{
  kno_index ix = kno_lisp2index(index);
  if (!(ix))
    return kno_type_error("index","index_save_prim",index);

  int rv = kno_index_save(ix,adds,drops,stores,metadata);
  if (rv<0)
    return KNO_ERROR_VALUE;
  else return KNO_INT(rv);
}

DEFPRIM2("index/fetchn",index_fetchn_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "Fetches values from an index, skipping the index "
	 "cache",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval index_fetchn_prim(lispval index,lispval keys)
{
  kno_index ix = kno_lisp2index(index);
  if (!(ix))
    return kno_type_error("index","index_fetchn_prim",index);

  return kno_index_fetchn(ix,keys);
}


DEFPRIM1("suggest-hash-size",suggest_hash_size,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SUGGEST-HASH-SIZE *arg0*)` **undocumented**",
	 kno_fixnum_type,KNO_VOID);
static lispval suggest_hash_size(lispval size)
{
  unsigned int suggestion = kno_get_hashtable_size(kno_getint(size));
  return KNO_INT(suggestion);
}

/* PICK and REJECT */

KNO_FASTOP int test_selector_predicate(lispval candidate,lispval test,int datalevel);
KNO_FASTOP int test_relation_regex(lispval candidate,lispval pred,lispval regex);

KNO_FASTOP int test_selector_relation(lispval f,lispval pred,lispval val,int datalevel)
{
  if (CHOICEP(pred)) {
    DO_CHOICES(p,pred) {
      int retval;
      if ((retval = test_selector_relation(f,p,val,datalevel))) {
	{KNO_STOP_DO_CHOICES;}
	return retval;}}
    return 0;}
  else if ((OIDP(f)) && ((SYMBOLP(pred)) || (OIDP(pred)))) {
    if ((!datalevel)&&(TYPEP(val,kno_regex_type)))
      return test_relation_regex(f,pred,val);
    else if (datalevel)
      return kno_test(f,pred,val);
    else return kno_frame_test(f,pred,val);}
  else if ((TABLEP(f)) && ((SYMBOLP(pred)) || (OIDP(pred)))) {
    if ((!datalevel)&&(TYPEP(val,kno_regex_type)))
      return test_relation_regex(f,pred,val);
    else return kno_test(f,pred,val);}
  else if (TABLEP(pred))
    return kno_test(pred,f,val);
  else if ((SYMBOLP(pred)) || (OIDP(pred)))
    return 0;
  else if (KNO_APPLICABLEP(pred)) {
    if (KNO_FCNIDP(pred)) pred = kno_fcnid_ref(pred);
    lispval rail[2], result = VOID;
    /* Handle the case where the 'slotid' is a unary function which can
       be used to extract an argument. */
    if ((KNO_LAMBDAP(pred)) || (TYPEP(pred,kno_cprim_type))) {
      kno_function fcn = KNO_GETFUNCTION(pred); int retval = -1;
      if (fcn->fcn_min_arity == 2) {
	rail[0]=f; rail[1]=val;
	result = kno_apply(pred,2,rail);
	if (KNO_ABORTP(result))
	  retval = -1;
	else if ( (KNO_FALSEP(result)) || (KNO_EMPTYP(result)) )
	  retval = 0;
	else {
	  kno_decref(result);
	  retval=1;}
	return retval;}
      else if (fcn->fcn_min_arity == 1) {
	lispval value = kno_apply(pred,1,&f); int retval = -1;
	if (EMPTYP(value)) return 0;
	else if (kno_overlapp(value,val)) retval = 1;
	else if (datalevel) retval = 0;
	else if ((KNO_HASHSETP(val))||
		 (HASHTABLEP(val))||
		 (KNO_APPLICABLEP(val))||
		 ((datalevel==0)&&(TYPEP(val,kno_regex_type)))||
		 ((PAIRP(val))&&(KNO_APPLICABLEP(KNO_CAR(val)))))
	  retval = test_selector_predicate(value,val,datalevel);
	else retval = 0;
	kno_decref(value);
	return retval;}
      else result = kno_err(kno_TypeError,"test_selector_relation",
			    "invalid relation",pred);}
    if (KNO_ABORTED(result))
      return kno_interr(result);
    else if ((FALSEP(result)) || (EMPTYP(result)) || (VOIDP(result)) )
      return 0;
    else {
      kno_decref(result);
      return 1;}}
  else if (VECTORP(pred)) {
    int len = VEC_LEN(pred), retval;
    lispval *data = VEC_DATA(pred), scan;
    if (len==0) return 0;
    else if (len==1) return test_selector_relation(f,data[0],val,datalevel);
    else scan = kno_getpath(f,len-1,data,datalevel,0);
    retval = test_selector_relation(scan,data[len-1],val,datalevel);
    kno_decref(scan);
    return retval;}
  else return kno_type_error(_("test relation"),"test_selector_relation",pred);
}

KNO_FASTOP int test_relation_regex(lispval candidate,lispval pred,lispval regex)
{
  lispval values = ((OIDP(candidate))?
		    (kno_frame_get(candidate,pred)):
		    (kno_get(candidate,pred,EMPTY)));
  if (EMPTYP(values)) return 0;
  else {
    struct KNO_REGEX *rx = (struct KNO_REGEX *)regex;
    DO_CHOICES(value,values) {
      if (STRINGP(value)) {
	regmatch_t results[1];
	u8_string data = CSTRING(value);
	int retval = regexec(&(rx->rxcompiled),data,1,results,0);
	if (retval!=REG_NOMATCH) {
	  if (retval) {
	    u8_byte buf[512];
	    regerror(retval,&(rx->rxcompiled),buf,512);
	    kno_seterr(kno_RegexError,"regex_pick",buf,VOID);
	    KNO_STOP_DO_CHOICES;
	    kno_decref(values);
	    return -1;}
	  else if (results[0].rm_so>=0) {
	    KNO_STOP_DO_CHOICES;
	    kno_decref(values);
	    return 1;}}}}
    return 0;}
}

KNO_FASTOP int test_selector_predicate(lispval candidate,lispval test,
				       int datalevel)
{
  if (EMPTYP(candidate)) return 0;
  else if (EMPTYP(test)) return 0;
  else if (CHOICEP(test)) {
    int retval = 0;
    DO_CHOICES(t,test) {
      if ((retval = test_selector_predicate(candidate,t,datalevel))) {
	{KNO_STOP_DO_CHOICES;}
	return retval;}}
    return retval;}
  else if ((OIDP(candidate)) && ((SYMBOLP(test)) || (OIDP(test))))
    if (datalevel)
      return kno_test(candidate,test,VOID);
    else return kno_frame_test(candidate,test,VOID);
  else if ((!(datalevel))&&(TYPEP(test,kno_regex_type))) {
    if (STRINGP(candidate)) {
      struct KNO_REGEX *rx = (struct KNO_REGEX *)test;
      regmatch_t results[1];
      u8_string data = CSTRING(candidate);
      int retval = regexec(&(rx->rxcompiled),data,1,results,0);
      if (retval == REG_NOMATCH) return 0;
      else if (retval) {
	u8_byte buf[512];
	regerror(retval,&(rx->rxcompiled),buf,512);
	kno_seterr(kno_RegexError,"regex_pick",buf,VOID);
	return -1;}
      else if (results[0].rm_so<0) return 0;
      else return 1;}
    else return 0;}
  else if (TYPEP(test,kno_hashset_type))
    if (kno_hashset_get((kno_hashset)test,candidate))
      return 1;
    else return 0;
  else if ((TABLEP(candidate)&&
	    ((OIDP(test)) || (SYMBOLP(test))))) {
    if ((!(datalevel))&&(OIDP(candidate)))
      return kno_frame_test(candidate,test,VOID);
    else return kno_test(candidate,test,VOID);}
  else if (KNO_APPLICABLEP(test)) {
    lispval v = kno_apply(test,1,&candidate);
    if (KNO_ABORTED(v)) return kno_interr(v);
    else if ((FALSEP(v)) || (EMPTYP(v)) || (VOIDP(v)))
      return 0;
    else {kno_decref(v); return 1;}}
  else if ((PAIRP(test))&&(KNO_APPLICABLEP(KNO_CAR(test)))) {
    lispval fcn = KNO_CAR(test), newval = KNO_FALSE;
    lispval args = KNO_CDR(test), argv[7]; int j = 1;
    argv[0]=candidate;
    if (PAIRP(args))
      while (PAIRP(args)) {
	argv[j++]=KNO_CAR(args); args = KNO_CDR(args);}
    else argv[j++]=args;
    if (j>7)
      newval = kno_err(kno_RangeError,"test_selector_relation",
		       "too many elements in test condition",VOID);
    else newval = kno_apply(j,fcn,argv);
    if (KNO_ABORTED(newval)) return newval;
    else if ((FALSEP(newval))||(EMPTYP(newval)))
      return 0;
    else {
      kno_decref(newval);
      return 1;}}
  else if (KNO_POOLP(test))
    if (OIDP(candidate)) {
      kno_pool p = kno_lisp2pool(test);
      if ( (p->pool_flags) & (KNO_POOL_ADJUNCT) ) {
	KNO_OID addr = KNO_OID_ADDR(candidate);
	long long diff = KNO_OID_DIFFERENCE(addr,p->pool_base);
	if ( (diff > 0) && (diff < p->pool_capacity) )
	  return 1;
	else return 0;}
      else if (kno_oid2pool(candidate) == p)
	return 1;
      else return 0;}
    else return 0;
  else if (TABLEP(test))
    return kno_test(test,candidate,VOID);
  else if (TABLEP(candidate))
    return kno_test(candidate,test,VOID);
  else {
    lispval ev = kno_type_error(_("test object"),"test_selector_predicate",test);
    return kno_interr(ev);}
}

KNO_FASTOP int test_selector_clauses(lispval candidate,int n,kno_argvec args,
				     int datalevel)
{
  if (n==1)
    if (EMPTYP(args[0])) return 0;
    else return test_selector_predicate(candidate,args[0],datalevel);
  else if (n==3) {
    lispval scan = kno_getpath(candidate,1,args,datalevel,0);
    int retval = test_selector_relation(scan,args[1],args[2],datalevel);
    kno_decref(scan);
    return retval;}
  else if (n%2) {
    kno_seterr(kno_TooManyArgs,"test_selector_clauses",
	       "odd number of args/clauses in db pick/reject",VOID);
    return -1;}
  else {
    int i = 0; while (i<n) {
      lispval slotids = args[i], values = args[i+1];
      int retval = test_selector_relation(candidate,slotids,values,datalevel);
      if (retval<0) return retval;
      else if (retval) i = i+2;
      else return 0;}
    return 1;}
}

/* PICK etc */

static lispval pick_helper(lispval candidates,int n,kno_argvec tests,
			   int datalevel)
{
  int retval;
  if (CHOICEP(candidates)) {
    int n_elts = KNO_CHOICE_SIZE(candidates);
    kno_choice read_choice = KNO_XCHOICE(candidates);
    kno_choice write_choice = kno_alloc_choice(n_elts);
    const lispval *read = KNO_XCHOICE_DATA(read_choice), *limit = read+n_elts;
    lispval *write = (lispval *)KNO_XCHOICE_DATA(write_choice);
    int n_results = 0, atomic_results = 1;
    while (read<limit) {
      lispval candidate = *read++;
      int retval = test_selector_clauses(candidate,n,tests,datalevel);
      if (retval<0) {
	const lispval *scan = KNO_XCHOICE_DATA(write_choice);
	const lispval *limit = scan+n_results;
	while (scan<limit) {lispval v = *scan++; kno_decref(v);}
	kno_free_choice(write_choice);
	return KNO_ERROR;}
      else if (retval) {
	*write++=candidate; n_results++;
	if (CONSP(candidate)) {
	  atomic_results = 0;
	  kno_incref(candidate);}}}
    if (n_results==0) {
      kno_free_choice(write_choice);
      return EMPTY;}
    else if (n_results==1) {
      lispval result = KNO_XCHOICE_DATA(write_choice)[0];
      kno_free_choice(write_choice);
      /* This was incref'd during the loop. */
      return result;}
    else return kno_init_choice
	   (write_choice,n_results,NULL,
	    ((atomic_results)?(KNO_CHOICE_ISATOMIC):(KNO_CHOICE_ISCONSES)));}
  else if (EMPTYP(candidates))
    return EMPTY;
  else if (n==1)
    if ((retval = (test_selector_predicate(candidates,tests[0],datalevel))))
      if (retval<0) return KNO_ERROR;
      else return kno_incref(candidates);
    else return EMPTY;
  else if ((retval = (test_selector_clauses(candidates,n,tests,datalevel))))
    if (retval<0) return KNO_ERROR;
    else return kno_incref(candidates);
  else return EMPTY;
}

static lispval hashset_filter(lispval candidates,kno_hashset hs,int pick)
{
  if (hs->hs_n_elts==0) {
    if (pick)
      return EMPTY;
    else return kno_incref(candidates);}
  kno_read_lock_table(hs); {
    lispval simple = kno_make_simple_choice(candidates);
    int n = KNO_CHOICE_SIZE(simple), isatomic = 1;
    lispval *buckets = hs->hs_buckets; int n_slots = hs->hs_n_buckets;
    lispval *keep = u8_alloc_n(n,lispval), *write = keep;
    DO_CHOICES(c,candidates) {
      int hash = kno_hash_lisp(c), probe = hash%n_slots, found=-1;
      lispval contents = buckets[probe];
      if (KNO_EMPTY_CHOICEP(contents))
	found=0;
      else if (!(KNO_AMBIGP(contents)))
	found = KNO_EQUALP(c,contents);
      else if (KNO_CHOICEP(contents))
	found = fast_choice_containsp(c,(kno_choice)contents);
      else {
	lispval normal = kno_make_simple_choice(contents);
	if (KNO_CHOICEP(normal))
	  found = fast_choice_containsp(c,(kno_choice)normal);
	else found=KNO_EQUALP(c,normal);
	kno_decref(normal);}
      if ( ((found)&&(pick)) || ((!found)&&((!pick))) ) {
	if ((isatomic)&&(CONSP(c))) isatomic = 0;
	*write++=c;
	kno_incref(c);}}
    kno_decref(simple);
    kno_unlock_table(hs);
    if (write == keep) {
      u8_free(keep);
      return EMPTY;}
    else if ((write-keep)==1) {
      lispval v = keep[0];
      u8_free(keep);
      return v;}
    else return kno_init_choice
	   (NULL,write-keep,keep,
	    ((isatomic)?(KNO_CHOICE_ISATOMIC):(0))|
	    (KNO_CHOICE_FREEDATA));}
}
#define hashset_pick(c,h) (hashset_filter(c,(kno_hashset)h,1))
#define hashset_reject(c,h) (hashset_filter(c,(kno_hashset)h,0))

static lispval hashtable_filter(lispval candidates,kno_hashtable ht,int pick)
{
  if (ht->table_n_keys==0) {
    if (pick)
      return EMPTY;
    else return kno_incref(candidates);}
  else {
    lispval simple = kno_make_simple_choice(candidates);
    int n = KNO_CHOICE_SIZE(simple), unlock = 0, isatomic = 1;
    lispval *keep = u8_alloc_n(n,lispval), *write = keep;
    if (KNO_XTABLE_USELOCKP(ht)) {kno_read_lock_table(ht); unlock = 1;}
    {struct KNO_HASH_BUCKET **slots = ht->ht_buckets;
      int n_slots = ht->ht_n_buckets;
      DO_CHOICES(c,candidates) {
	struct KNO_KEYVAL *result = kno_hashvec_get(c,slots,n_slots);
	lispval rv = ( (result) ? (result->kv_val) : (VOID) );
	if ( (VOIDP(rv)) || (EMPTYP(rv)) ) result = NULL;
	if ( ((result) && (pick)) ||
	     ((result == NULL) && (!(pick))) ) {
	  if ((isatomic)&&(CONSP(c))) isatomic = 0;
	  kno_incref(c);
	  *write++=c;}}
      if (unlock) u8_rw_unlock(&(ht->table_rwlock));
      kno_decref(simple);
      if (write == keep) {
	u8_free(keep);
	return EMPTY;}
      else if ((write-keep)==1) {
	lispval v = keep[0];
	u8_free(keep);
	return v;}
      else return kno_init_choice
	     (NULL,write-keep,keep,
	      ((isatomic)?(KNO_CHOICE_ISATOMIC):(0))|
	      (KNO_CHOICE_FREEDATA));}}
}
#define hashtable_pick(c,h) (hashtable_filter(c,(kno_hashtable)h,1))
#define hashtable_reject(c,h) (hashtable_filter(c,(kno_hashtable)h,0))

DEFPRIM("pick",pick_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"`(PICK *arg0* *arg1* *args...*)` **undocumented**");
static lispval pick_lexpr(int n,kno_argvec args)
{
  if (KNO_EMPTYP(args[0]))
    return KNO_EMPTY;
  else if ((n==2)&&(HASHTABLEP(args[1])))
    return hashtable_pick(args[0],(kno_hashtable)args[1]);
  else if ((n==2)&&(KNO_HASHSETP(args[1])))
    return hashset_pick(args[0],(kno_hashset)args[1]);
  else if ((n<=4)||(n%2))
    return pick_helper(args[0],n-1,args+1,0);
  else return kno_err
	 (kno_TooManyArgs,"pick_lexpr",
	  "PICK requires two or 2n+1 arguments",VOID);
}

DEFPRIM("prefer",prefer_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"`(PREFER *arg0* *arg1* *args...*)` **undocumented**");
static lispval prefer_lexpr(int n,kno_argvec args)
{
  if ((n==2)&&(HASHTABLEP(args[1]))) {
    lispval results = hashtable_pick(args[0],(kno_hashtable)args[1]);
    if (EMPTYP(results))
      return kno_incref(args[0]);
    else return results;}
  else if ((n==2)&&(KNO_HASHSETP(args[1]))) {
    lispval results = hashset_pick(args[0],(kno_hashset)args[1]);
    if (EMPTYP(results))
      return kno_incref(args[0]);
    else return results;}
  else if ((n<=4)||(n%2)) {
    lispval results = pick_helper(args[0],n-1,args+1,0);
    if (EMPTYP(results))
      return kno_incref(args[0]);
    else return results;}
  else return kno_err(kno_TooManyArgs,"prefer_lexpr",
		      "PICK PREFER two or 2n+1 arguments",VOID);
}

DEFPRIM("%pick",prim_pick_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"`(%PICK *arg0* *arg1* *args...*)` **undocumented**");
static lispval prim_pick_lexpr(int n,kno_argvec args)
{
  if ((n==2)&&(HASHTABLEP(args[1])))
    return hashtable_pick(args[0],(kno_hashtable)args[1]);
  else if ((n==2)&&(KNO_HASHSETP(args[1])))
    return hashset_pick(args[0],(kno_hashset)args[1]);
  else if ((n<=4)||(n%2))
    return pick_helper(args[0],n-1,args+1,1);
  else return kno_err(kno_TooManyArgs,"prim_pick_lexpr",
		      "%PICK requires two or 2n+1 arguments",VOID);
}

DEFPRIM("%prefer",prim_prefer_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"`(%PREFER *arg0* *arg1* *args...*)` **undocumented**");
static lispval prim_prefer_lexpr(int n,kno_argvec args)
{
  if ((n<=4)||(n%2)) {
    lispval results = pick_helper(args[0],n-1,args+1,1);
    if (EMPTYP(results))
      return kno_incref(args[0]);
    else return results;}
  else return kno_err(kno_TooManyArgs,"prim_prefer_lexpr",
		      "%PICK requires two or 2n+1 arguments",VOID);
}

/* REJECT etc */

static lispval reject_helper(lispval candidates,int n,kno_argvec tests,
			     int datalevel)
{
  int retval;
  if (CHOICEP(candidates)) {
    int n_elts = KNO_CHOICE_SIZE(candidates);
    kno_choice read_choice = KNO_XCHOICE(candidates);
    kno_choice write_choice = kno_alloc_choice(n_elts);
    const lispval *read = KNO_XCHOICE_DATA(read_choice), *limit = read+n_elts;
    lispval *write = (lispval *)KNO_XCHOICE_DATA(write_choice);
    int n_results = 0, atomic_results = 1;
    while (read<limit) {
      lispval candidate = *read++;
      int retval = test_selector_clauses(candidate,n,tests,datalevel);
      if (retval<0) {
	const lispval *scan  = KNO_XCHOICE_DATA(write_choice);
	const lispval *limit = scan+n_results;
	while (scan<limit) {
	  lispval v = *scan++;
	  kno_decref(v);}
	kno_free_choice(write_choice);
	return KNO_ERROR;}
      else if (retval==0) {
	*write++=candidate; n_results++;
	if (CONSP(candidate)) {
	  atomic_results = 0;
	  kno_incref(candidate);}}}
    if (n_results==0) {
      kno_free_choice(write_choice);
      return EMPTY;}
    else if (n_results==1) {
      lispval result = KNO_XCHOICE_DATA(write_choice)[0];
      kno_free_choice(write_choice);
      /* This was incref'd during the loop. */
      return result;}
    else return kno_init_choice
	   (write_choice,n_results,NULL,
	    ((atomic_results)?(KNO_CHOICE_ISATOMIC):(KNO_CHOICE_ISCONSES)));}
  else if (EMPTYP(candidates))
    return EMPTY;
  else if (n==1)
    if ((retval = (test_selector_predicate(candidates,tests[0],datalevel))))
      if (retval<0) return KNO_ERROR;
      else return EMPTY;
    else return kno_incref(candidates);
  else if ((retval = (test_selector_clauses(candidates,n,tests,datalevel))))
    if (retval<0) return KNO_ERROR;
    else return EMPTY;
  else return kno_incref(candidates);
}

DEFPRIM("reject",reject_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"`(REJECT *arg0* *arg1* *args...*)` **undocumented**");
static lispval reject_lexpr(int n,kno_argvec args)
{
  if (KNO_EMPTYP(args[0]))
    return KNO_EMPTY;
  else if ((n==2)&&(HASHTABLEP(args[1])))
    return hashtable_reject(args[0],args[1]);
  else if ((n==2)&&(KNO_HASHSETP(args[1])))
    return hashset_reject(args[0],args[1]);
  else if ((n<=4)||(n%2))
    return reject_helper(args[0],n-1,args+1,0);
  else return kno_err(kno_TooManyArgs,"reject_lexpr",
		      "REJECT requires two or 2n+1 arguments",VOID);
}

DEFPRIM("avoid",avoid_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"`(AVOID *arg0* *arg1* *args...*)` **undocumented**");
static lispval avoid_lexpr(int n,kno_argvec args)
{
  if ((n==2)&&(HASHTABLEP(args[1]))) {
    lispval results = hashtable_reject(args[0],(kno_hashtable)args[1]);
    if (EMPTYP(results))
      return kno_incref(args[0]);
    else return results;}
  else if ((n==2)&&(KNO_HASHSETP(args[1]))) {
    lispval results = hashtable_reject(args[0],(kno_hashset)args[1]);
    if (EMPTYP(results))
      return kno_incref(args[0]);
    else return results;}
  else if ((n<=4)||(n%2)) {
    lispval values = reject_helper(args[0],n-1,args+1,0);
    if (EMPTYP(values))
      return kno_incref(args[0]);
    else return values;}
  else return kno_err(kno_TooManyArgs,"avoid_lexpr",
		      "AVOID requires two or 2n+1 arguments",VOID);
}

DEFPRIM("%reject",prim_reject_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"`(%REJECT *arg0* *arg1* *args...*)` **undocumented**");
static lispval prim_reject_lexpr(int n,kno_argvec args)
{
  if ((n==2)&&(HASHTABLEP(args[1])))
    return hashtable_reject(args[0],args[1]);
  else if ((n==2)&&(KNO_HASHSETP(args[1])))
    return hashset_reject(args[0],args[1]);
  else if ((n<=4)||(n%2))
    return reject_helper(args[0],n-1,args+1,1);
  else return kno_err(kno_TooManyArgs,"prim_reject_lexpr",
		      "%REJECT requires two or 2n+1 arguments",VOID);
}

DEFPRIM("%avoid",prim_avoid_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"`(%AVOID *arg0* *arg1* *args...*)` **undocumented**");
static lispval prim_avoid_lexpr(int n,kno_argvec args)
{
  if ((n==2)&&(HASHTABLEP(args[1]))) {
    lispval results = hashtable_reject(args[0],(kno_hashtable)args[1]);
    if (EMPTYP(results))
      return kno_incref(args[0]);
    else return results;}
  else if ((n==2)&&(KNO_HASHSETP(args[1]))) {
    lispval results = hashtable_reject(args[0],(kno_hashset)args[1]);
    if (EMPTYP(results))
      return kno_incref(args[0]);
    else return results;}
  else if ((n<=4)||(n%2)) {
    lispval values = reject_helper(args[0],n-1,args+1,1);
    if (EMPTYP(values))
      return kno_incref(args[0]);
    else return values;}
  else return kno_err(kno_TooManyArgs,"prim_avoid_lexpr",
		      "%AVOID requires two or 2n+1 arguments",VOID);
}

/* Kleene* operations */

static lispval getroots(lispval frames)
{
  lispval roots = KNO_EMPTY;
  if (KNO_AMBIGP(frames)) {
    KNO_DO_CHOICES(frame,frames) {
      if ( (KNO_OIDP(frame)) || (KNO_SLOTMAPP(frame)) || (KNO_SCHEMAPP(frame)) ) {
	CHOICE_ADD(roots,frame);}}}
  else if ((KNO_OIDP(frames)) || (KNO_SLOTMAPP(frames)) || (KNO_SCHEMAPP(frames)))
    roots = frames;
  else NO_ELSE;
  return roots;
}

DEFPRIM2("get*",getstar,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(GET* *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval getstar(lispval frames,lispval slotids)
{
  lispval roots = getroots(frames);
  if (KNO_EMPTYP(roots))
    return KNO_EMPTY;
  lispval results = kno_incref(roots);
  DO_CHOICES(slotid,slotids) {
    lispval all = kno_inherit_values(roots,slotid,slotid);
    if (KNO_ABORTED(all)) {
      kno_decref(results);
      return all;}
    else {CHOICE_ADD(results,all);}}
  return kno_simplify_choice(results);
}

DEFPRIM3("inherit",inherit_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	 "`(INHERIT *arg0* *arg1* *arg2*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval inherit_prim(lispval slotids,lispval frames,lispval through)
{
  lispval roots = getroots(frames);
  if (KNO_EMPTYP(roots))
    return KNO_EMPTY;
  lispval results = kno_incref(frames);
  DO_CHOICES(through_id,through) {
    lispval all = kno_inherit_values(frames,slotids,through_id);
    CHOICE_ADD(results,all);}
  return kno_simplify_choice(results);
}

DEFPRIM3("path?",pathp,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	 "`(PATH? *arg0* *arg1* *arg2*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval pathp(lispval frames,lispval slotids,lispval values)
{
  lispval roots = getroots(frames);
  if (KNO_EMPTYP(roots))
    return KNO_FALSE;
  else if (kno_pathp(frames,slotids,values))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM2("get-basis",getbasis,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(GET-BASIS *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval getbasis(lispval frames,lispval lattice)
{
  return kno_simplify_choice(kno_get_basis(frames,lattice));
}

/* Frame creation */

DEFPRIM2("allocate-oids",allocate_oids,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(ALLOCATE-OIDS *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval allocate_oids(lispval pool,lispval howmany)
{
  kno_pool p = arg2pool(pool);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"allocate_oids",pool);
  if (VOIDP(howmany))
    return kno_pool_alloc(p,1);
  else if (KNO_UINTP(howmany))
    return kno_pool_alloc(p,FIX2INT(howmany));
  else return kno_type_error(_("fixnum"),"allocate_oids",howmany);
}

DEFPRIM("frame-create",frame_create_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	"`(FRAME-CREATE *arg0* *args...*)` **undocumented**");
static lispval frame_create_lexpr(int n,kno_argvec args)
{
  lispval result;
  int i = (n%2);
  if ((n>=1)&&(EMPTYP(args[0])))
    return EMPTY;
  else if (n==1) return kno_new_frame(args[0],VOID,0);
  else if (n==2) return kno_new_frame(args[0],args[1],1);
  else if (n%2) {
    if ((SLOTMAPP(args[0]))||(SCHEMAPP(args[0]))||(OIDP(args[0]))) {
      result = kno_deep_copy(args[0]);}
    else result = kno_new_frame(args[0],VOID,0);}
  else if ((SYMBOLP(args[0]))||(OIDP(args[0])))
    result = kno_new_frame(KNO_DEFAULT_VALUE,VOID,0);
  else return kno_err(kno_SyntaxError,"frame_create_lexpr",NULL,VOID);
  if (KNO_ABORTP(result))
    return result;
  else if (KNO_OIDP(result)) while (i<n) {
      DO_CHOICES(slotid,args[i]) {
	int rv = kno_frame_add(result,slotid,args[i+1]);
	if (rv < 0) {
	  KNO_STOP_DO_CHOICES;
	  return KNO_ERROR;}}
      i = i+2;}
  else while (i<n) {
      DO_CHOICES(slotid,args[i]) {
	int rv = kno_add(result,slotid,args[i+1]);
	if (rv < 0) {
	  KNO_STOP_DO_CHOICES;
	  return KNO_ERROR;}}
      i = i+2;}
  return result;
}

DEFPRIM("frame-update",frame_update_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(3)|KNO_NDCALL,
	"`(FRAME-UPDATE *arg0* *arg1* *arg2* *args...*)` **undocumented**");
static lispval frame_update_lexpr(int n,kno_argvec args)
{
  if ((n>=1)&&(EMPTYP(args[0])))
    return EMPTY;
  else if (n<3)
    return kno_err(kno_SyntaxError,"frame_update_lexpr",NULL,VOID);
  else if (!(TABLEP(args[0])))
    return kno_err(kno_TypeError,"frame_update_lexpr","not a table",args[0]);
  else if (n%2) {
    lispval result = kno_deep_copy(args[0]); int i = 1;
    while (i<n) {
      if (kno_store(result,args[i],args[i+1])<0) {
	kno_decref(result);
	return KNO_ERROR;}
      else i = i+2;}
    return result;}
  else return kno_err(kno_SyntaxError,"frame_update_lexpr",
		      _("wrong number of args"),VOID);
}

DEFPRIM4("seq->frame",seq2frame_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	 "`(SEQ->FRAME *arg0* *arg1* *arg2* [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval seq2frame_prim
(lispval poolspec,lispval values,lispval schema,lispval dflt)
{
  if (!(KNO_SEQUENCEP(schema)))
    return kno_type_error(_("sequence"),"seq2frame_prim",schema);
  else if (!(KNO_SEQUENCEP(values)))
    return kno_type_error(_("sequence"),"seq2frame_prim",values);
  else {
    lispval frame;
    int i = 0, slen = kno_seq_length(schema), vlen = kno_seq_length(values);
    if (FALSEP(poolspec))
      frame = kno_empty_slotmap();
    else if (OIDP(poolspec)) frame = poolspec;
    else {
      lispval oid, slotmap;
      kno_pool p = arg2pool(poolspec);
      if (p == NULL)
	return kno_type_error(_("pool spec"),"seq2frame_prim",poolspec);
      oid = kno_pool_alloc(p,1);
      if (KNO_ABORTED(oid)) return oid;
      slotmap = kno_empty_slotmap();
      if (kno_set_oid_value(oid,slotmap)<0) {
	kno_decref(slotmap);
	return KNO_ERROR;}
      else {
	kno_decref(slotmap); frame = oid;}}
    while ((i<slen) && (i<vlen)) {
      lispval slotid = kno_seq_elt(schema,i);
      lispval value = kno_seq_elt(values,i);
      int retval;
      if (OIDP(frame))
	retval = kno_frame_add(frame,slotid,value);
      else retval = kno_add(frame,slotid,value);
      if (retval<0) {
	kno_decref(slotid); kno_decref(value);
	if (FALSEP(poolspec)) kno_decref(frame);
	return KNO_ERROR;}
      kno_decref(slotid); kno_decref(value);
      i++;}
    if (!(VOIDP(dflt)))
      while (i<slen) {
	lispval slotid = kno_seq_elt(schema,i);
	int retval;
	if (OIDP(frame))
	  retval = kno_frame_add(frame,slotid,dflt);
	else retval = kno_add(frame,slotid,dflt);
	if (retval<0) {
	  kno_decref(slotid);
	  if (FALSEP(poolspec)) kno_decref(frame);
	  return KNO_ERROR;}
	kno_decref(slotid); i++;}
    return frame;}
}

static int doassert(lispval f,lispval s,lispval v)
{
  if (EMPTYP(v)) return 0;
  else if (OIDP(f))
    return kno_frame_add(f,s,v);
  else return kno_add(f,s,v);
}

static int doretract(lispval f,lispval s,lispval v)
{
  if (EMPTYP(v)) return 0;
  else if (OIDP(f))
    return kno_frame_drop(f,s,v);
  else return kno_drop(f,s,v);
}

DEFPRIM("modify-frame",modify_frame_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(3)|KNO_NDCALL,
	"`(MODIFY-FRAME *arg0* *arg1* *arg2* *args...*)` **undocumented**");
static lispval modify_frame_lexpr(int n,kno_argvec args)
{
  if (n%2==0)
    return kno_err(kno_SyntaxError,"FRAME-MODIFY",NULL,VOID);
  else {
    DO_CHOICES(frame,args[0]) {
      int i = 1; while (i<n) {
	DO_CHOICES(slotid,args[i])
	  if (PAIRP(slotid))
	    if ((PAIRP(KNO_CDR(slotid))) &&
		((OIDP(KNO_CAR(slotid))) || (SYMBOLP(KNO_CAR(slotid)))) &&
		((KNO_EQ(KNO_CADR(slotid),drop_symbol)))) {
	      if (doretract(frame,KNO_CAR(slotid),args[i+1])<0) {
		KNO_STOP_DO_CHOICES;
		return KNO_ERROR;}
	      else {}}
	    else {
	      u8_log(LOG_WARN,kno_TypeError,"frame_modify_lexpr","slotid");}
	  else if ((SYMBOLP(slotid)) || (OIDP(slotid)))
	    if (doassert(frame,slotid,args[i+1])<0) {
	      KNO_STOP_DO_CHOICES;
	      return KNO_ERROR;}
	    else {}
	  else u8_log(LOG_WARN,kno_TypeError,"frame_modify_lexpr","slotid");
	i = i+2;}}
    return kno_incref(args[0]);}
}

/* OID operations */

DEFPRIM2("oid-plus",oid_plus_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "Adds an integer to an OID address and returns "
	 "that OID",
	 kno_oid_type,KNO_VOID,kno_fixnum_type,KNO_CPP_INT(1));
static lispval oid_plus_prim(lispval oid,lispval increment)
{
  KNO_OID base = KNO_OID_ADDR(oid), next;
  int delta = kno_getint(increment);
  next = KNO_OID_PLUS(base,delta);
  return kno_make_oid(next);
}

DEFPRIM2("oid-offset",oid_offset_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "Returns the offset of an OID from a container and "
	 "#f if it is not in the container. When the second "
	 "argument is an OID, it is used as the base of "
	 "container; if it is a pool, it is used as the "
	 "container. Without a second argument the "
	 "registered pool for the OID is used as the "
	 "container.",
	 kno_oid_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval oid_offset_prim(lispval oidarg,lispval against)
{
  KNO_OID oid = KNO_OID_ADDR(oidarg), base; int cap = -1;
  if (OIDP(against)) {
    base = KNO_OID_ADDR(against);}
  else if (KNO_POOLP(against)) {
    kno_pool p = kno_lisp2pool(against);
    if (p) {base = p->pool_base; cap = p->pool_capacity;}
    else return KNO_ERROR;}
  else if ((VOIDP(against)) || (FALSEP(against))) {
    kno_pool p = kno_oid2pool(oidarg);
    if (p) {base = p->pool_base; cap = p->pool_capacity;}
    else return KNO_INT((KNO_OID_LO(oid))%0x100000);}
  else return kno_type_error(_("offset base"),"oid_offset_prim",against);
  if ((KNO_OID_HI(oid)) == (KNO_OID_HI(base))) {
    int diff = (KNO_OID_LO(oid))-(KNO_OID_LO(base));
    if ((diff>=0) && ((cap<0) || (diff<cap)))
      return KNO_INT(diff);
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFPRIM2("oid-minus",oid_minus_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "Returns the difference between two OIDs. If the "
	 "second argument is a pool, the base of the pool "
	 "is used for comparision",
	 kno_oid_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval oid_minus_prim(lispval oidarg,lispval against)
{
  KNO_OID oid = KNO_OID_ADDR(oidarg), base;
  if (OIDP(against)) {
    base = KNO_OID_ADDR(against);}
  else if (KNO_POOLP(against)) {
    kno_pool p = kno_lisp2pool(against);
    if (p) base = p->pool_base;
    else return KNO_ERROR;}
  else return kno_type_error(_("offset base"),"oid_offset_prim",against);
  unsigned long long oid_difference = KNO_OID_DIFFERENCE(oid,base);
  return KNO_INT(oid_difference);
}

#ifdef KNO_OID_BASE_ID
DEFPRIM1("oid-ptrdata",oid_ptrdata_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(OID-PTRDATA *arg0*)` **undocumented**",
	 kno_oid_type,KNO_VOID);
static lispval oid_ptrdata_prim(lispval oid)
{
  return kno_conspair(KNO_INT(KNO_OID_BASE_ID(oid)),
		      KNO_INT(KNO_OID_BASE_OFFSET(oid)));
}
#endif

DEFPRIM2("make-oid",make_oid_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(MAKE-OID *addr* [*lo*]) "
	 "returns an OID from numeric components. If *lo* "
	 "is not provided (or #f) *addr* is used as the "
	 "complete address. Otherwise, *addr* specifies the "
	 "high part of the OID address.",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval make_oid_prim(lispval high,lispval low)
{
  if ( (VOIDP(low)) || (KNO_FALSEP(low)) ) {
    KNO_OID oid;
    unsigned long long addr;
    if (FIXNUMP(high))
      addr = FIX2INT(high);
    else if (KNO_BIGINTP(high))
      addr = kno_bigint2uint64((kno_bigint)high);
    else return kno_type_error("integer","make_oid_prim",high);
#if KNO_STRUCT_OIDS
    memset(&oid,0,sizeof(KNO_OID));
    oid.hi = (addr>>32)&0xFFFFFFFF;
    oid.log = (addr)&0xFFFFFFFF;
#else
    oid = addr;
#endif
    return kno_make_oid(oid);}
  else if (OIDP(high)) {
    KNO_OID base = KNO_OID_ADDR(high), oid; unsigned int off;
    if (KNO_UINTP(low))
      off = FIX2INT(low);
    else if (KNO_BIGINTP(low)) {
      if ((off = kno_bigint2uint((struct KNO_BIGINT *)low))==0)
	return kno_type_error("uint32","make_oid_prim",low);}
    else return kno_type_error("uint32","make_oid_prim",low);
    oid = KNO_OID_PLUS(base,off);
    return kno_make_oid(oid);}
  else {
    unsigned int hi, lo;
#if KNO_STRUCT_OIDS
    KNO_OID addr; memset(&addr,0,sizeof(KNO_OID));
#else
    KNO_OID addr = 0;
#endif
    KNO_SET_OID_HI(addr,0); KNO_SET_OID_LO(addr,0);
    if (FIXNUMP(high))
      if (((FIX2INT(high))<0)||((FIX2INT(high))>UINT_MAX))
	return kno_type_error("uint32","make_oid_prim",high);
      else hi = FIX2INT(high);
    else if (KNO_BIGINTP(high)) {
      hi = kno_bigint2uint((struct KNO_BIGINT *)high);
      if (hi==0) return kno_type_error("uint32","make_oid_prim",high);}
    else return kno_type_error("uint32","make_oid_prim",high);
    if (FIXNUMP(low))
      if (((FIX2INT(low))<0)||((FIX2INT(low))>UINT_MAX))
	return kno_type_error("uint32","make_oid_prim",low);
      else lo = FIX2INT(low);
    else if (KNO_BIGINTP(low)) {
      lo = kno_bigint2uint((struct KNO_BIGINT *)low);
      if (lo==0) return kno_type_error("uint32","make_oid_prim",low);}
    else return kno_type_error("uint32","make_oid_prim",low);
    KNO_SET_OID_HI(addr,hi);
    KNO_SET_OID_LO(addr,lo);
    return kno_make_oid(addr);}
}

DEFPRIM2("oid->string",oid2string_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(OID->STRING *arg0* [*arg1*])` **undocumented**",
	 kno_oid_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval oid2string_prim(lispval oid,lispval name)
{
  KNO_OID addr = KNO_OID_ADDR(oid);
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);

  if (VOIDP(name))
    u8_printf(&out,"@%x/%x",KNO_OID_HI(addr),KNO_OID_LO(addr));
  else if ((STRINGP(name)) || (CHOICEP(name)) ||
	   (PAIRP(name)) || (VECTORP(name)))
    u8_printf(&out,"@%x/%x%q",KNO_OID_HI(addr),KNO_OID_LO(addr),name);
  else u8_printf(&out,"@%x/%x{%q}",KNO_OID_HI(addr),KNO_OID_LO(addr),name);

  return kno_stream2string(&out);
}

DEFPRIM2("oid->hex",oidhex_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(OID->HEX *arg0* [*arg1*])` **undocumented**",
	 kno_oid_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval oidhex_prim(lispval oid,lispval base_arg)
{
  char buf[32]; int offset;
  KNO_OID addr = KNO_OID_ADDR(oid);
  if ((VOIDP(base_arg)) || (FALSEP(base_arg))) {
    kno_pool p = kno_oid2pool(oid);
    if (p) {
      KNO_OID base = p->pool_base;
      offset = KNO_OID_DIFFERENCE(addr,base);}
    else offset = (KNO_OID_LO(addr))%0x100000;}
  else if (KNO_POOLP(base_arg)) {
    kno_pool p = kno_poolptr(base_arg);
    if (p) {
      KNO_OID base = p->pool_base;
      offset = KNO_OID_DIFFERENCE(addr,base);}
    else offset = (KNO_OID_LO(addr))%0x100000;}
  else if (OIDP(base_arg)) {
    KNO_OID base = KNO_OID_ADDR(base_arg);
    offset = KNO_OID_DIFFERENCE(addr,base);}
  else offset = (KNO_OID_LO(addr))%0x100000;
  return kno_make_string(NULL,-1,u8_uitoa16(offset,buf));
}

#if 0
static lispval oidb32_prim(lispval oid,lispval base_arg)
{
  char buf[64]; int offset, buflen = 64;
  KNO_OID addr = KNO_OID_ADDR(oid);
  if ((VOIDP(base_arg)) || (FALSEP(base_arg))) {
    kno_pool p = kno_oid2pool(oid);
    if (p) {
      KNO_OID base = p->pool_base;
      offset = KNO_OID_DIFFERENCE(addr,base);}
    else offset = (KNO_OID_LO(addr))%0x100000;}
  else if (KNO_POOLP(base_arg)) {
    kno_pool p = kno_poolptr(base_arg);
    if (p) {
      KNO_OID base = p->pool_base;
      offset = KNO_OID_DIFFERENCE(addr,base);}
    else offset = (KNO_OID_LO(addr))%0x100000;}
  else if (OIDP(base_arg)) {
    KNO_OID base = KNO_OID_ADDR(base_arg);
    offset = KNO_OID_DIFFERENCE(addr,base);}
  else offset = (KNO_OID_LO(addr))%0x100000;
  kno_ulonglong_to_b32(offset,buf,&buflen);
  return kno_make_string(NULL,buflen,buf);
}
#endif

static lispval oidplus(KNO_OID base,int delta)
{
  KNO_OID next = KNO_OID_PLUS(base,delta);
  return kno_make_oid(next);
}

DEFPRIM2("hex->oid",hex2oid_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(HEX->OID *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval hex2oid_prim(lispval arg,lispval base_arg)
{
  long long offset;
  if (STRINGP(arg))
    offset = strtol(CSTRING(arg),NULL,16);
  else if (FIXNUMP(arg))
    offset = FIX2INT(arg);
  else return kno_type_error("hex offset","hex2oid_prim",arg);
  if (offset<0) return kno_type_error("hex offset","hex2oid_prim",arg);
  if (OIDP(base_arg)) {
    KNO_OID base = KNO_OID_ADDR(base_arg);
    return oidplus(base,offset);}
  else if (KNO_POOLP(base_arg)) {
    kno_pool p = kno_poolptr(base_arg);
    return oidplus(p->pool_base,offset);}
  else if (STRINGP(base_arg)) {
    kno_pool p = kno_name2pool(CSTRING(base_arg));
    if (p) return oidplus(p->pool_base,offset);
    else return kno_type_error("pool id","hex2oid_prim",base_arg);}
  else return kno_type_error("pool id","hex2oid_prim",base_arg);
}

#if 0
static lispval b32oid_prim(lispval arg,lispval base_arg)
{
  long long offset;
  if (STRINGP(arg)) {
    offset = kno_b32_to_longlong(CSTRING(arg));
    if (offset<0) {
      kno_seterr(kno_ParseError,"b32oid","invalid B32 string",arg);
      return KNO_ERROR;}}
  else if (FIXNUMP(arg))
    offset = FIX2INT(arg);
  else return kno_type_error("b32 offset","b32oid_prim",arg);
  if (offset<0) return kno_type_error("hex offset","hex2oid_prim",arg);
  if (OIDP(base_arg)) {
    KNO_OID base = KNO_OID_ADDR(base_arg);
    return oidplus(base,offset);}
  else if (KNO_POOLP(base_arg)) {
    kno_pool p = kno_poolptr(base_arg);
    return oidplus(p->pool_base,offset);}
  else if (STRINGP(base_arg)) {
    kno_pool p = kno_name2pool(CSTRING(base_arg));
    if (p) return oidplus(p->pool_base,offset);
    else return kno_type_error("pool id","hex2oid_prim",base_arg);}
  else return kno_type_error("pool id","hex2oid_prim",base_arg);
}
#endif

DEFPRIM1("oid-addr",oidaddr_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the absolute numeric address of an OID",
	 kno_oid_type,KNO_VOID);
static lispval oidaddr_prim(lispval oid)
{
  KNO_OID oidaddr = KNO_OID_ADDR(oid);
  if (KNO_OID_HI(oidaddr)) {
#if KNO_STRUCT_OIDS
    unsigned long long addr = ((oid.hi)<<32)|(oid.lo);
#else
    unsigned long long addr = oidaddr;
#endif
    return KNO_INT(addr);}
  else return KNO_INT(KNO_OID_LO(oid));
}

/* sumframe function */

DEFPRIM2("sumframe",sumframe_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(SUMFRAME *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval sumframe_prim(lispval frames,lispval slotids)
{
  lispval results = EMPTY;
  DO_CHOICES(frame,frames) {
    lispval slotmap = kno_empty_slotmap();
    DO_CHOICES(slotid,slotids) {
      lispval value = kno_get(frame,slotid,EMPTY);
      if (KNO_ABORTED(value)) {
	kno_decref(results); return value;}
      else if (kno_add(slotmap,slotid,value)<0) {
	kno_decref(results); kno_decref(value);
	return KNO_ERROR;}
      kno_decref(value);}
    if (OIDP(frame)) {
      KNO_OID addr = KNO_OID_ADDR(frame);
      u8_string s = u8_mkstring("@%x/%x",KNO_OID_HI(addr),KNO_OID_LO(addr));
      lispval idstring = kno_wrapstring(s);
      if (kno_add(slotmap,id_symbol,idstring)<0) {
	kno_decref(results); kno_decref(idstring);
	return KNO_ERROR;}
      else kno_decref(idstring);}
    CHOICE_ADD(results,slotmap);}
  return results;
}

/* Graph walking functions */

static lispval applyfn(lispval fn,lispval node)
{
  if ((OIDP(fn)) || (SYMBOLP(fn)))
    return kno_frame_get(node,fn);
  else if (CHOICEP(fn)) {
    lispval results = EMPTY;
    DO_CHOICES(f,fn) {
      lispval v = applyfn(fn,node);
      if (KNO_ABORTED(v)) {
	kno_decref(results);
	return v;}
      else {CHOICE_ADD(results,v);}}
    return results;}
  else if (KNO_APPLICABLEP(fn))
    return kno_apply(fn,1,&node);
  else if (TABLEP(fn))
    return kno_get(fn,node,EMPTY);
  else return EMPTY;
}

static int walkgraph(lispval fn,lispval state,lispval arcs,
		     struct KNO_HASHSET *seen,lispval *results)
{
  lispval next_state = EMPTY;
  DO_CHOICES(node,state)
    if (!(kno_hashset_get(seen,node))) {
      lispval output, expand;
      kno_hashset_add(seen,node);
      /* Apply the function */
      output = applyfn(fn,node);
      if (KNO_ABORTED(output)) return kno_interr(output);
      else if (results) {CHOICE_ADD(*results,output);}
      else kno_decref(output);
      /* Do the expansion */
      expand = applyfn(arcs,node);
      if (KNO_ABORTED(expand)) return kno_interr(expand);
      else {
	DO_CHOICES(next,expand)
	  if (!(kno_hashset_get(seen,next))) {
	    kno_incref(next); CHOICE_ADD(next_state,next);}}
      kno_decref(expand);}
  kno_decref(state);
  if (!(EMPTYP(next_state)))
    return walkgraph(fn,next_state,arcs,seen,results);
  else return 1;
}

DEFPRIM3("forgraph",forgraph,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	 "`(FORGRAPH *arg0* *arg1* *arg2*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval forgraph(lispval fcn,lispval roots,lispval arcs)
{
  struct KNO_HASHSET hashset; int retval;
  if ((OIDP(arcs)) || (SYMBOLP(arcs)) ||
      (KNO_APPLICABLEP(arcs)) || (TABLEP(arcs))) {}
  else if (CHOICEP(arcs)) {
    DO_CHOICES(each,arcs)
      if (!((OIDP(each)) || (SYMBOLP(each)) ||
	    (KNO_APPLICABLEP(each)) || (TABLEP(each))))
	return kno_type_error("mapfn","mapgraph",each);}
  else return kno_type_error("mapfn","mapgraph",arcs);
  kno_init_hashset(&hashset,1024,KNO_STACK_CONS);
  kno_incref(roots);
  retval = walkgraph(fcn,roots,arcs,&hashset,NULL);
  if (retval<0) {
    kno_decref((lispval)&hashset);
    return KNO_ERROR;}
  else {
    kno_decref((lispval)&hashset);
    return VOID;}
}

DEFPRIM3("mapgraph",mapgraph,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	 "`(MAPGRAPH *arg0* *arg1* *arg2*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval mapgraph(lispval fcn,lispval roots,lispval arcs)
{
  lispval results = EMPTY; int retval; struct KNO_HASHSET hashset;
  if ((OIDP(fcn)) || (SYMBOLP(fcn)) ||
      (KNO_APPLICABLEP(fcn)) || (TABLEP(fcn))) {}
  else if (CHOICEP(fcn)) {
    DO_CHOICES(each,fcn)
      if (!((OIDP(each)) || (SYMBOLP(each)) ||
	    (KNO_APPLICABLEP(each)) || (TABLEP(each))))
	return kno_type_error("mapfn","mapgraph",each);}
  else return kno_type_error("mapfn","mapgraph",fcn);
  if ((OIDP(arcs)) || (SYMBOLP(arcs)) ||
      (KNO_APPLICABLEP(arcs)) || (TABLEP(arcs))) {}
  else if (CHOICEP(arcs)) {
    DO_CHOICES(each,arcs)
      if (!((OIDP(each)) || (SYMBOLP(each)) ||
	    (KNO_APPLICABLEP(each)) || (TABLEP(each))))
	return kno_type_error("mapfn","mapgraph",each);}
  else return kno_type_error("mapfn","mapgraph",arcs);
  kno_init_hashset(&hashset,1024,KNO_STACK_CONS); kno_incref(roots);
  retval = walkgraph(fcn,roots,arcs,&hashset,&results);
  if (retval<0) {
    kno_decref((lispval)&hashset);
    kno_decref(results);
    return KNO_ERROR;}
  else {
    kno_decref((lispval)&hashset);
    return results;}
}

/* Helpful predicates */

DEFPRIM2("loaded?",dbloadedp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(LOADED? *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval dbloadedp(lispval arg1,lispval arg2)
{
  if (VOIDP(arg2))
    if (OIDP(arg1)) {
      kno_pool p = kno_oid2pool(arg1);
      if (kno_hashtable_probe(&(p->pool_changes),arg1)) {
	lispval v = kno_hashtable_probe(&(p->pool_changes),arg1);
	if ((v!=VOID) || (v!=KNO_LOCKHOLDER)) {
	  kno_decref(v); return KNO_TRUE;}
	else return KNO_FALSE;}
      else if (kno_hashtable_probe(&(p->pool_cache),arg1))
	return KNO_TRUE;
      else return KNO_FALSE;}
    else if (kno_hashtable_probe(&(kno_background->index_cache),arg1))
      return KNO_TRUE;
    else return KNO_FALSE;
  else if (INDEXP(arg2)) {
    kno_index ix = kno_indexptr(arg2);
    if (ix == NULL)
      return kno_type_error("index","loadedp",arg2);
    else if (kno_hashtable_probe(&(ix->index_cache),arg1))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (KNO_POOLP(arg2))
    if (OIDP(arg1)) {
      kno_pool p = kno_lisp2pool(arg2);
      if (p == NULL)
	return kno_type_error("pool","loadedp",arg2);
      if (kno_hashtable_probe(&(p->pool_changes),arg1)) {
	lispval v = kno_hashtable_probe(&(p->pool_changes),arg1);
	if ((v!=VOID) || (v!=KNO_LOCKHOLDER)) {
	  kno_decref(v); return KNO_TRUE;}
	else return KNO_FALSE;}
      else if (kno_hashtable_probe(&(p->pool_cache),arg1))
	return KNO_TRUE;
      else return KNO_FALSE;}
    else return KNO_FALSE;
  else if ((STRINGP(arg2)) && (OIDP(arg1))) {
    kno_pool p = kno_lisp2pool(arg2); kno_index ix;
    if (p)
      if (kno_hashtable_probe(&(p->pool_cache),arg1))
	return KNO_TRUE;
      else return KNO_FALSE;
    else ix = kno_indexptr(arg2);
    if (ix == NULL)
      return kno_type_error("pool/index","loadedp",arg2);
    else if (kno_hashtable_probe(&(ix->index_cache),arg1))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return kno_type_error("pool/index","loadedp",arg2);
}

static int oidmodifiedp(kno_pool p,lispval oid)
{
  if (kno_hashtable_probe(&(p->pool_changes),oid)) {
    lispval v = kno_hashtable_get(&(p->pool_changes),oid,VOID);
    int modified = 1;
    if ((VOIDP(v)) || (v == KNO_LOCKHOLDER))
      modified = 0;
    else if (SLOTMAPP(v))
      if (KNO_TABLE_MODIFIEDP(v)) {}
      else modified = 0;
    else if (SCHEMAPP(v)) {
      if (KNO_TABLE_MODIFIEDP(v)) {}
      else modified = 0;}
    else {}
    kno_decref(v);
    return modified;}
  else return 0;
}

DEFPRIM2("modified?",dbmodifiedp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(MODIFIED? *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval dbmodifiedp(lispval arg1,lispval arg2)
{
  if (VOIDP(arg2))
    if (OIDP(arg1)) {
      kno_pool p = kno_oid2pool(arg1);
      if (oidmodifiedp(p,arg1))
	return KNO_TRUE;
      else return KNO_FALSE;}
    else if ((KNO_POOLP(arg1))||(TYPEP(arg1,kno_consed_pool_type))) {
      kno_pool p = kno_lisp2pool(arg1);
      if (p->pool_changes.table_n_keys)
	return KNO_TRUE;
      else return KNO_FALSE;}
    else if (INDEXP(arg1)) {
      kno_index ix = kno_lisp2index(arg1);
      if ((ix->index_adds.table_n_keys) ||
	  (ix->index_drops.table_n_keys) ||
	  (ix->index_stores.table_n_keys))
	return KNO_TRUE;
      else return KNO_FALSE;}
    else if (TABLEP(arg1)) {
      kno_lisp_type ttype = KNO_TYPEOF(arg1);
      if ((kno_tablefns[ttype])&&(kno_tablefns[ttype]->modified)) {
	if ((kno_tablefns[ttype]->modified)(arg1,-1))
	  return KNO_TRUE;
	else return KNO_FALSE;}
      else return KNO_FALSE;}
    else return KNO_FALSE;
  else if (INDEXP(arg2)) {
    kno_index ix = kno_indexptr(arg2);
    if (ix == NULL)
      return kno_type_error("index","loadedp",arg2);
    else if ((kno_hashtable_probe(&(ix->index_adds),arg1)) ||
	     (kno_hashtable_probe(&(ix->index_drops),arg1)) ||
	     (kno_hashtable_probe(&(ix->index_stores),arg1)))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (KNO_POOLP(arg2))
    if (OIDP(arg1)) {
      kno_pool p = kno_lisp2pool(arg2);
      if (p == NULL)
	return kno_type_error("pool","loadedp",arg2);
      else if (oidmodifiedp(p,arg1))
	return KNO_TRUE;
      else return KNO_FALSE;}
    else return KNO_FALSE;
  else if ((STRINGP(arg2)) && (OIDP(arg1))) {
    kno_pool p = kno_lisp2pool(arg2); kno_index ix;
    if (p)
      if (oidmodifiedp(p,arg1))
	return KNO_TRUE;
      else return KNO_FALSE;
    else ix = kno_indexptr(arg2);
    if (ix == NULL)
      return kno_type_error("pool/index","loadedp",arg2);
    else if ((kno_hashtable_probe(&(ix->index_adds),arg1)) ||
	     (kno_hashtable_probe(&(ix->index_drops),arg1)) ||
	     (kno_hashtable_probe(&(ix->index_stores),arg1)))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return kno_type_error("pool/index","loadedp",arg2);
}

DEFPRIM1("db/writable?",db_writablep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(DB/WRITABLE? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval db_writablep(lispval db)
{
  if (KNO_POOLP(db)) {
    kno_pool p = kno_lisp2pool(db);
    int flags = p->pool_flags;
    if ( (flags) & (KNO_STORAGE_READ_ONLY) )
      return KNO_FALSE;
    else return KNO_TRUE;}
  else if (KNO_INDEXP(db)) {
    kno_index ix = kno_lisp2index(db);
    int flags = ix->index_flags;
    if ( (flags) & (KNO_STORAGE_READ_ONLY) )
      return KNO_FALSE;
    else return KNO_TRUE;}
  else if (KNO_TABLEP(db)) {
    if (kno_readonlyp(db))
      return KNO_FALSE;
    else return KNO_TRUE;}
  else return KNO_FALSE;
}

/* Bloom filters */

DEFPRIM2("make-bloom-filter",make_bloom_filter,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "Creates a bloom filter for a a number of items "
	 "and an error rate",
	 kno_fixnum_type,KNO_VOID,kno_flonum_type,KNO_VOID);
static lispval make_bloom_filter(lispval n_entries,lispval allowed_error)
{
  struct KNO_BLOOM *filter = (VOIDP(allowed_error))?
    (kno_init_bloom_filter(NULL,FIX2INT(n_entries),0.0004)) :
    (kno_init_bloom_filter(NULL,FIX2INT(n_entries),KNO_FLONUM(allowed_error)));
  if (filter)
    return (lispval) filter;
  else return KNO_ERROR;
}

#define BLOOM_DTYPE_LEN 1000

DEFPRIM4("bloom/add!",bloom_add,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "(BLOOM/ADD! *filter* *key* [*raw*]) "
	 "adds a key to a bloom filter. The *raw* argument "
	 "indicates that the key is a string or packet "
	 "should be added to the filter. Otherwise, the "
	 "binary DTYPE representation for the value is "
	 "added to the filter.",
	 kno_bloom_filter_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_FALSE,kno_any_type,KNO_FALSE);
static lispval bloom_add(lispval filter,lispval value,
			 lispval raw_arg,
			 lispval ignore_errors)
{
  struct KNO_BLOOM *bloom = (struct KNO_BLOOM *)filter;
  int raw = (!(FALSEP(raw_arg)));
  int noerr = (!(FALSEP(ignore_errors)));
  int rv = kno_bloom_op(bloom,value,
			( ( (raw) ? (KNO_BLOOM_RAW) : (0)) |
			  ( (noerr) ? (0) : (KNO_BLOOM_ERR) ) |
			  (KNO_BLOOM_ADD) ));
  if (rv<0)
    return KNO_ERROR;
  else if (rv == 0)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM4("bloom/check",bloom_check,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "(BLOOM/CHECK *filter* *keys* [*raw*] [*noerr*]) "
	 "returns true if any of *keys* are probably found "
	 "in *filter*. If *raw* is true, *keys* must be "
	 "strings or packets and their byte values are used "
	 "directly; otherwise, values are converted to "
	 "dtypes before being tested. If *noerr* is true, "
	 "type or conversion errors are just considered "
	 "misses and ignored.",
	 kno_bloom_filter_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_FALSE,kno_any_type,KNO_FALSE);
static lispval bloom_check(lispval filter,lispval value,
			   lispval raw_arg,
			   lispval ignore_errors)
{
  struct KNO_BLOOM *bloom = (struct KNO_BLOOM *)filter;
  int raw = (!(FALSEP(raw_arg)));
  int noerr = (!(FALSEP(ignore_errors)));
  int rv = kno_bloom_op(bloom,value,
			( ( (raw) ? (KNO_BLOOM_RAW) : (0)) |
			  ( (noerr) ? (0) : (KNO_BLOOM_ERR) ) |
			  (KNO_BLOOM_CHECK) ));
  if (rv<0)
    return KNO_ERROR;
  else if (rv)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM4("bloom/hits",bloom_hits,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "(BLOOM/HITS *filter* *keys* [*raw*] [*noerr*]) "
	 "returns the number of *keys* probably found in "
	 "*filter*. If *raw* is true, *keys* must be "
	 "strings or packets and their byte values are used "
	 "directly; otherwise, values are converted to "
	 "dtypes before being tested. If *noerr* is true, "
	 "type or conversion errors are just considered "
	 "misses and ignored.",
	 kno_bloom_filter_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_FALSE,kno_any_type,KNO_FALSE);
static lispval bloom_hits(lispval filter,lispval value,
			  lispval raw_arg,
			  lispval ignore_errors)
{
  struct KNO_BLOOM *bloom = (struct KNO_BLOOM *)filter;
  int raw = (!(FALSEP(raw_arg)));
  int noerr = (!(FALSEP(ignore_errors)));
  int rv = kno_bloom_op(bloom,value,
			( ( (raw) ? (KNO_BLOOM_RAW) : (0)) |
			  ( (noerr) ? (0) : (KNO_BLOOM_ERR) ) ));
  if (rv<0)
    return KNO_ERROR;
  else return KNO_INT(rv);
}

DEFPRIM1("bloom-size",bloom_size,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the size (in bytes) of a bloom filter ",
	 kno_bloom_filter_type,KNO_VOID);
static lispval bloom_size(lispval filter)
{
  struct KNO_BLOOM *bloom = (struct KNO_BLOOM *)filter;
  return KNO_INT(bloom->entries);
}

DEFPRIM1("bloom-count",bloom_count,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the number of objects added to a bloom "
	 "filter",
	 kno_bloom_filter_type,KNO_VOID);
static lispval bloom_count(lispval filter)
{
  struct KNO_BLOOM *bloom = (struct KNO_BLOOM *)filter;
  return KNO_INT(bloom->bloom_adds);
}

DEFPRIM1("bloom-error",bloom_error,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the error threshold (a flonum) for the "
	 "filter",
	 kno_bloom_filter_type,KNO_VOID);
static lispval bloom_error(lispval filter)
{
  struct KNO_BLOOM *bloom = (struct KNO_BLOOM *)filter;
  return kno_make_double(bloom->error);
}

DEFPRIM1("bloom-data",bloom_data,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the bytes of the filter as a packet",
	 kno_bloom_filter_type,KNO_VOID);
static lispval bloom_data(lispval filter)
{
  struct KNO_BLOOM *bloom = (struct KNO_BLOOM *)filter;
  return kno_bytes2packet(NULL,bloom->bytes,bloom->bf);
}

/* Checking pool and index typeids */

DEFPRIM1("pooltype?",pool_typep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *arg* is a known pool type",
	 kno_symbol_type,KNO_VOID);
static lispval pool_typep(lispval typesym)
{
  struct KNO_POOL_TYPEINFO *ptype =
    kno_get_pool_typeinfo(KNO_SYMBOL_NAME(typesym));
  if (ptype)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("indextype?",index_typep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *arg* is a known index type",
	 kno_symbol_type,KNO_VOID);
static lispval index_typep(lispval typesym)
{
  struct KNO_INDEX_TYPEINFO *ptype =
    kno_get_index_typeinfo(KNO_SYMBOL_NAME(typesym));
  if (ptype)
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Registering procpools and procindexes */

DEFPRIM2("defpooltype",def_procpool,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "Registers handlers for a procpool",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval def_procpool(lispval typesym,lispval handlers)
{
  if (!(KNO_TABLEP(handlers)))
    return kno_err(kno_TypeError,"register_procpool",
		   "not a handler table",handlers);
  else if (KNO_SYMBOLP(typesym)) {
    kno_register_procpool(KNO_SYMBOL_NAME(typesym),handlers);
    return kno_incref(handlers);}
  else if (KNO_STRINGP(typesym)) {
    kno_register_procpool(KNO_CSTRING(typesym),handlers);
    return kno_incref(handlers);}
  else return kno_err(kno_TypeError,"register_procpool",NULL,typesym);
}

DEFPRIM2("defindextype",def_procindex,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "Registers handlers for a procindex",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval def_procindex(lispval typesym,lispval handlers)
{
  if (!(KNO_TABLEP(handlers)))
    return kno_err(kno_TypeError,"register_procpool",
		   "not a handler table",handlers);
  else if (KNO_SYMBOLP(typesym)) {
    kno_register_procindex(KNO_SYMBOL_NAME(typesym),handlers);
    return kno_incref(handlers);}
  else if (KNO_STRINGP(typesym)) {
    kno_register_procindex(KNO_CSTRING(typesym),handlers);
    return kno_incref(handlers);}
  else return kno_err(kno_TypeError,"register_procpool",NULL,typesym);
}

DEFPRIM1("procpool?",procpoolp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns #t if it's argument is a procpool",
	 kno_any_type,KNO_VOID);
static lispval procpoolp(lispval pool)
{
  kno_pool p = kno_poolptr(pool);
  if ( (p) && (p->pool_handler == &kno_procpool_handler) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("procindex?",procindexp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns #t if it's argument is a procindex",
	 kno_any_type,KNO_VOID);
static lispval procindexp(lispval index)
{
  kno_index ix = kno_indexptr(index);
  if ( (ix) && (ix->index_handler == &kno_procindex_handler) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Initializing */

KNO_EXPORT void kno_init_dbprims_c()
{
  u8_register_source_file(_FILEINFO);

  link_local_cprims();

  kno_def_evalfn(kno_db_module,"CACHEGET",cacheget_evalfn,
		 "*undocumented*");

  id_symbol = kno_intern("%id");
  padjuncts_symbol = kno_intern("%adjuncts");
  pools_symbol = kno_intern("pools");
  indexes_symbol = kno_intern("indexes");
  drop_symbol = kno_intern("drop");
  flags_symbol = kno_intern("flags");
  register_symbol = kno_intern("register");
  readonly_symbol = kno_intern("readonly");
  phased_symbol = kno_intern("phased");

  sparse_symbol	   = kno_intern("sparse");
  adjunct_symbol    = kno_intern("adjunct");
  background_symbol = kno_intern("background");
  repair_symbol = kno_intern("repair");

}

static void link_local_cprims()
{
  KNO_LINK_PRIM("procindex?",procindexp,1,kno_db_module);
  KNO_LINK_PRIM("procpool?",procpoolp,1,kno_db_module);
  KNO_LINK_PRIM("indextype?",index_typep,1,kno_db_module);
  KNO_LINK_PRIM("pooltype?",pool_typep,1,kno_db_module);
  KNO_LINK_PRIM("defindextype",def_procindex,2,kno_db_module);
  KNO_LINK_PRIM("defpooltype",def_procpool,2,kno_db_module);
  KNO_LINK_PRIM("bloom-data",bloom_data,1,kno_db_module);
  KNO_LINK_PRIM("bloom-error",bloom_error,1,kno_db_module);
  KNO_LINK_PRIM("bloom-count",bloom_count,1,kno_db_module);
  KNO_LINK_PRIM("bloom-size",bloom_size,1,kno_db_module);
  KNO_LINK_PRIM("bloom/hits",bloom_hits,4,kno_db_module);
  KNO_LINK_PRIM("bloom/check",bloom_check,4,kno_db_module);
  KNO_LINK_ALIAS("bloom/add",bloom_add,kno_db_module);
  KNO_LINK_PRIM("bloom/add!",bloom_add,4,kno_db_module);
  KNO_LINK_PRIM("make-bloom-filter",make_bloom_filter,2,kno_db_module);
  KNO_LINK_PRIM("db/writable?",db_writablep,1,kno_db_module);
  KNO_LINK_PRIM("modified?",dbmodifiedp,2,kno_db_module);
  KNO_LINK_PRIM("loaded?",dbloadedp,2,kno_db_module);
  KNO_LINK_PRIM("mapgraph",mapgraph,3,kno_db_module);
  KNO_LINK_PRIM("forgraph",forgraph,3,kno_db_module);
  KNO_LINK_PRIM("sumframe",sumframe_prim,2,kno_db_module);
  KNO_LINK_PRIM("oid-addr",oidaddr_prim,1,kno_db_module);
  KNO_LINK_PRIM("hex->oid",hex2oid_prim,2,kno_db_module);
  KNO_LINK_PRIM("oid->hex",oidhex_prim,2,kno_db_module);
  KNO_LINK_PRIM("oid->string",oid2string_prim,2,kno_db_module);
  KNO_LINK_PRIM("make-oid",make_oid_prim,2,kno_db_module);
  KNO_LINK_PRIM("oid-ptrdata",oid_ptrdata_prim,1,kno_db_module);
  KNO_LINK_PRIM("oid-minus",oid_minus_prim,2,kno_db_module);
  KNO_LINK_PRIM("oid-offset",oid_offset_prim,2,kno_db_module);
  KNO_LINK_PRIM("oid-plus",oid_plus_prim,2,kno_db_module);
  KNO_LINK_VARARGS("modify-frame",modify_frame_lexpr,kno_db_module);
  KNO_LINK_PRIM("seq->frame",seq2frame_prim,4,kno_db_module);
  KNO_LINK_VARARGS("frame-update",frame_update_lexpr,kno_db_module);
  KNO_LINK_VARARGS("frame-create",frame_create_lexpr,kno_db_module);
  KNO_LINK_PRIM("allocate-oids",allocate_oids,2,kno_db_module);
  KNO_LINK_PRIM("get-basis",getbasis,2,kno_db_module);
  KNO_LINK_PRIM("path?",pathp,3,kno_db_module);
  KNO_LINK_PRIM("inherit",inherit_prim,3,kno_db_module);
  KNO_LINK_PRIM("get*",getstar,2,kno_db_module);
  KNO_LINK_VARARGS("%avoid",prim_avoid_lexpr,kno_db_module);
  KNO_LINK_VARARGS("%reject",prim_reject_lexpr,kno_db_module);
  KNO_LINK_VARARGS("avoid",avoid_lexpr,kno_db_module);
  KNO_LINK_VARARGS("reject",reject_lexpr,kno_db_module);
  KNO_LINK_VARARGS("%prefer",prim_prefer_lexpr,kno_db_module);
  KNO_LINK_VARARGS("%pick",prim_pick_lexpr,kno_db_module);
  KNO_LINK_VARARGS("prefer",prefer_lexpr,kno_db_module);
  KNO_LINK_VARARGS("pick",pick_lexpr,kno_db_module);
  KNO_LINK_PRIM("suggest-hash-size",suggest_hash_size,1,kno_db_module);
  KNO_LINK_PRIM("index/fetchn",index_fetchn_prim,2,kno_db_module);
  KNO_LINK_PRIM("index/save!",index_save_prim,5,kno_db_module);
  KNO_LINK_PRIM("commit-index",commit_index_prim,1,kno_db_module);
  KNO_LINK_PRIM("close-index",close_index_prim,1,kno_db_module);
  KNO_LINK_PRIM("index-source",index_source,1,kno_db_module);
  KNO_LINK_PRIM("slotindex/merge!",slotindex_merge,2,kno_db_module);
  KNO_LINK_PRIM("index/merge!",index_merge,2,kno_db_module);
  KNO_LINK_PRIM("index-keysvec",index_keysvec,1,kno_db_module);
  KNO_LINK_PRIM("index-sizes",index_sizes,2,kno_db_module);
  KNO_LINK_PRIM("index-keys",index_keys,1,kno_db_module);
  KNO_LINK_PRIM("bgdecache",bgdecache,2,kno_db_module);
  KNO_LINK_PRIM("index-decache",index_decache,3,kno_db_module);
  KNO_LINK_PRIM("index-set!",index_set,3,kno_db_module);
  KNO_LINK_PRIM("index-add!",index_add,3,kno_db_module);
  KNO_LINK_PRIM("index-get",index_get,2,kno_db_module);
  KNO_LINK_PRIM("index-source",index_source_prim,1,kno_db_module);
  KNO_LINK_PRIM("index-id",index_id,1,kno_db_module);
  KNO_LINK_VARARGS("getpath*",getpathstar_prim,kno_db_module);
  KNO_LINK_VARARGS("getpath",getpath_prim,kno_db_module);
  KNO_LINK_VARARGS("testp",testp,kno_db_module);
  KNO_LINK_PRIM("retract!",kno_retract,3,kno_db_module);
  KNO_LINK_PRIM("assert!",kno_assert,3,kno_db_module);
  KNO_LINK_PRIM("test",kno_ftest,3,kno_db_module);
  KNO_LINK_PRIM("get",kno_fget,2,kno_db_module);
  KNO_LINK_PRIM("change-load",change_load,1,kno_db_module);
  KNO_LINK_PRIM("cache-load",cache_load,1,kno_db_module);
  KNO_LINK_PRIM("cached-keys",cached_keys,1,kno_db_module);
  KNO_LINK_PRIM("cached-oids",cached_oids,1,kno_db_module);
  KNO_LINK_PRIM("index-prefetch!",index_prefetch_keys,2,kno_db_module);
  KNO_LINK_PRIM("prefetch-keys!",prefetch_keys,2,kno_db_module);
  KNO_LINK_PRIM("fetchoids",fetchoids_prim,1,kno_db_module);
  KNO_LINK_PRIM("prefetch-oids!",prefetch_oids_prim,2,kno_db_module);
  KNO_LINK_PRIM("pool-prefetch!",pool_prefetch_prim,2,kno_db_module);
  KNO_LINK_PRIM("valid-oid?",validoidp,2,kno_db_module);
  KNO_LINK_PRIM("in-pool?",inpoolp,2,kno_db_module);
  KNO_LINK_PRIM("oid-pool",oidpool,1,kno_db_module);
  KNO_LINK_PRIM("oid?",oidp,1,kno_db_module);
  KNO_LINK_PRIM("oid-lo",oid_lo,1,kno_db_module);
  KNO_LINK_PRIM("oid-hi",oid_hi,1,kno_db_module);
  KNO_LINK_PRIM("oid-base",oid_base,2,kno_db_module);
  KNO_LINK_PRIM("cachecount",cachecount,1,kno_db_module);
  KNO_LINK_PRIM("pool-vector",pool_vec,1,kno_db_module);
  KNO_LINK_PRIM("random-oid",random_oid,1,kno_db_module);
  KNO_LINK_PRIM("oid-vector",oid_vector,2,kno_db_module);
  KNO_LINK_PRIM("oid-range",oid_range,2,kno_db_module);
  KNO_LINK_PRIM("pool-close",pool_close_prim,1,kno_db_module);
  KNO_LINK_PRIM("set-pool-prefix!",set_pool_prefix,2,kno_db_module);
  KNO_LINK_PRIM("pool-prefix",pool_prefix,1,kno_db_module);
  KNO_LINK_PRIM("pool-source",pool_source,1,kno_db_module);
  KNO_LINK_PRIM("pool-id",pool_id,1,kno_db_module);
  KNO_LINK_PRIM("pool-label",pool_label,2,kno_db_module);
  KNO_LINK_PRIM("pool-elts",pool_elts,3,kno_db_module);
  KNO_LINK_PRIM("pool-base",pool_base,1,kno_db_module);
  KNO_LINK_PRIM("pool-capacity",pool_capacity,1,kno_db_module);
  KNO_LINK_PRIM("pool-load",pool_load,1,kno_db_module);
  KNO_LINK_PRIM("swapcheck",swapcheck_prim,0,kno_db_module);
  KNO_LINK_PRIM("clearcaches",clearcaches,0,kno_db_module);
  KNO_LINK_PRIM("clear-slotcache!",clear_slotcache,1,kno_db_module);
  KNO_LINK_PRIM("pool/fetchn",pool_fetchn_prim,2,kno_db_module);
  KNO_LINK_PRIM("pool/storen!",pool_storen_prim,3,kno_db_module);
  KNO_LINK_PRIM("commit-finished",commit_finished,1,kno_db_module);
  KNO_LINK_PRIM("commit-pool",commit_pool,2,kno_db_module);
  KNO_LINK_PRIM("finish-oids",finish_oids,2,kno_db_module);
  KNO_LINK_PRIM("commit-oids",commit_oids,1,kno_db_module);
  KNO_LINK_VARARGS("commit",commit_lexpr,kno_db_module);
  KNO_LINK_VARARGS("swapout",swapout_lexpr,kno_db_module);
  KNO_LINK_PRIM("adjunct?",isadjunctp,1,kno_db_module);
  KNO_LINK_PRIM("get-adjuncts",get_adjuncts_prim,1,kno_db_module);
  KNO_LINK_PRIM("get-adjunct",get_adjunct_prim,2,kno_db_module);
  KNO_LINK_PRIM("adjunct-valuye",adjunct_value_prim,2,kno_db_module);
  KNO_LINK_PRIM("adjunct!",add_adjunct,3,kno_db_module);
  KNO_LINK_PRIM("use-adjunct",use_adjunct,3,kno_db_module);
  KNO_LINK_PRIM("extindex?",extindexp,1,kno_db_module);
  KNO_LINK_PRIM("extindex-state",extindex_state,1,kno_db_module);
  KNO_LINK_PRIM("extindex-commitfn",extindex_commitfn,1,kno_db_module);
  KNO_LINK_PRIM("extindex-fetchfn",extindex_fetchfn,1,kno_db_module);
  KNO_LINK_PRIM("extindex-decache!",extindex_decache,2,kno_db_module);
  KNO_LINK_PRIM("extindex-cacheadd!",extindex_cacheadd,3,kno_db_module);
  KNO_LINK_PRIM("cons-extindex",cons_extindex,6,kno_db_module);
  KNO_LINK_PRIM("make-extindex",make_extindex,6,kno_db_module);
  KNO_LINK_PRIM("make-procindex",make_procindex,5,kno_db_module);
  KNO_LINK_PRIM("extpool-state",extpool_state,1,kno_db_module);
  KNO_LINK_PRIM("extpool-lockfn",extpool_lockfn,1,kno_db_module);
  KNO_LINK_PRIM("extpool-savefn",extpool_savefn,1,kno_db_module);
  KNO_LINK_PRIM("extpool-fetchfn",extpool_fetchfn,1,kno_db_module);
  KNO_LINK_PRIM("extpool-cache!",extpool_setcache,3,kno_db_module);
  KNO_LINK_PRIM("make-extpool",make_extpool,10,kno_db_module);
  KNO_LINK_PRIM("make-procpool",make_procpool,6,kno_db_module);
  KNO_LINK_PRIM("reset-mempool",reset_mempool,1,kno_db_module);
  KNO_LINK_PRIM("clean-mempool",clean_mempool,1,kno_db_module);
  KNO_LINK_PRIM("make-mempool",make_mempool,6,kno_db_module);
  KNO_LINK_PRIM("tempindex?",tempindexp,1,kno_db_module);
  KNO_LINK_PRIM("extend-aggregate-index!",extend_aggregate_index,2,kno_db_module);
  KNO_LINK_PRIM("aggregate-index?",aggregate_indexp,1,kno_db_module);
  KNO_LINK_PRIM("make-aggregate-index",make_aggregate_index,2,kno_db_module);
  KNO_LINK_PRIM("unlock-oids!",unlockoids,2,kno_db_module);
  KNO_LINK_PRIM("locked-oids",lockedoids,1,kno_db_module);
  KNO_LINK_PRIM("lock-oids!",lockoids,1,kno_db_module);
  KNO_LINK_PRIM("locked?",oidlockedp,1,kno_db_module);
  KNO_LINK_PRIM("lock-oid!",lockoid,2,kno_db_module);
  KNO_LINK_PRIM("%set-oid-value!",xsetoidvalue,2,kno_db_module);
  KNO_LINK_PRIM("set-oid-value!",setoidvalue,3,kno_db_module);
  KNO_LINK_PRIM("oid-value",oidvalue,1,kno_db_module);
  KNO_LINK_PRIM("make-index",make_index,2,kno_db_module);
  KNO_LINK_PRIM("open-pool",open_pool,2,kno_db_module);
  KNO_LINK_PRIM("make-pool",make_pool,2,kno_db_module);
  KNO_LINK_PRIM("cons-index",cons_index,2,kno_db_module);
  KNO_LINK_PRIM("register-index",register_index,2,kno_db_module);
  KNO_LINK_PRIM("open-index",open_index,2,kno_db_module);
  KNO_LINK_PRIM("use-index",use_index,2,kno_db_module);
  KNO_LINK_PRIM("use-pool",use_pool,2,kno_db_module);
  KNO_LINK_PRIM("adjunct-pool",adjunct_pool,2,kno_db_module);
  KNO_LINK_PRIM("try-pool",try_pool,2,kno_db_module);
  KNO_LINK_PRIM("set-cache-level!",set_cache_level,2,kno_db_module);
  KNO_LINK_PRIM("set-pool-namefn!",set_pool_namefn,2,kno_db_module);
  KNO_LINK_PRIM("name->pool",getpool,1,kno_db_module);
  KNO_LINK_PRIM("index?",indexp,1,kno_db_module);
  KNO_LINK_PRIM("pool?",poolp,1,kno_db_module);
  KNO_LINK_PRIM("index-frame",index_frame_prim,4,kno_db_module);
  KNO_LINK_VARARGS("find-frames/prefetch!",find_frames_prefetch,kno_db_module);
  KNO_LINK_PRIM("prefetch-slotvals!",prefetch_slotvals,3,kno_db_module);
  KNO_LINK_VARARGS("xfind-frames",xfind_frames_lexpr,kno_db_module);
  KNO_LINK_VARARGS("find-frames",find_frames_lexpr,kno_db_module);
  KNO_LINK_PRIM("slotid?",slotidp,1,kno_db_module);

  KNO_LINK_ALIAS("??",find_frames_lexpr,kno_db_module);
  KNO_LINK_ALIAS("getpool",getpool,kno_db_module);
  KNO_LINK_ALIAS("load-pool",try_pool,kno_db_module);
  KNO_LINK_ALIAS("temp-index",cons_index,kno_db_module);
  KNO_LINK_ALIAS("oid+",oid_plus_prim,kno_db_module);
  KNO_LINK_ALIAS("oid-",oid_minus_prim,kno_db_module);
  KNO_LINK_ALIAS("oid@",oidaddr_prim,kno_db_module);
  KNO_LINK_ALIAS("oidhex",oidhex_prim,kno_db_module);
  KNO_LINK_ALIAS("hexoid",oidhex_prim,kno_db_module);
  KNO_LINK_ALIAS("find-frames-prefetch!",find_frames_prefetch,kno_db_module);
  KNO_LINK_ALIAS("\?\?/prefetch!",find_frames_prefetch,kno_db_module);
  KNO_LINK_ALIAS("\?\?!",find_frames_prefetch,kno_db_module);
  KNO_LINK_ALIAS("add-to-aggregate-index!",extend_aggregate_index,
		 kno_db_module);
  KNO_LINK_ALIAS("index-merge!",index_merge,kno_db_module);
  KNO_LINK_ALIAS("add-adjunct!",add_adjunct,kno_db_module);

}
