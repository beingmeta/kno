/* -*- mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
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

#define STR_EXTRACT(into,start,end)					\
  size_t into ## _strlen = (end) ? ((end)-(start)) : (strlen(start));	\
  u8_byte into[into ## _strlen +1];					\
  memcpy(into,start,into ## _strlen);					\
  into[into ## _strlen]='\0'

#define BLOOM_FILTER_TYPE 0x63845e

/* These are called when the lisp version of its pool/index argument
   is being returned and needs to be incref'd if it is consed. */
KNO_FASTOP lispval index_ref(kno_index ix)
{
  if (ix == NULL)
    return KNO_ERROR;
  else if (ix->index_serialno>=0)
    return LISPVAL_IMMEDIATE(kno_indexref_type,ix->index_serialno);
  else {
    lispval lix = (lispval)ix;
    /* kno_incref(lix); */
    return lix;}
}

#define FALSE_ARGP(x) ( ((x)==KNO_VOID) || ((x)==KNO_FALSE) || ((x)==KNO_FIXZERO) )

static void maybe_load_modules(lispval modules,u8_context context)
{
  if ( (KNO_VOIDP(modules)) || (KNO_FALSEP(modules)) || (KNO_EMPTYP(modules)) )
    return;
  else {
    lispval mod = kno_find_module(modules,0);
    if (KNO_VOIDP(mod)) {
      u8_log(LOGWARN,"MissingDBModule",
	     "Couldn't find %q <%s>",modules,context);}
    else kno_decref(mod);}
}

DEF_KNOSYM(indextype); DEF_KNOSYM(pooltype);

static void load_db_module(lispval opts,u8_context context,int index)
{
  if ( (KNO_VOIDP(opts)) || (KNO_EMPTYP(opts)) ||
       (KNO_FALSEP(opts)) || (KNO_DEFAULTP(opts)) )
    return;
  else {
    lispval optname = (index) ? (KNOSYM(indextype)) : (KNOSYM(pooltype));
    lispval type = kno_getopt(opts,optname,KNO_VOID);
    if (KNO_VOIDP(type)) type = kno_getopt(opts,KNOSYM_TYPE,KNO_VOID);
    u8_string typename = (KNO_SYMBOLP(type)) ? (KNO_SYMBOL_NAME(type)) :
      (KNO_STRINGP(type)) ? (KNO_CSTRING(type)) : (U8S(NULL));
    if ( typename == NULL) {}
    else if ( (index) ? (kno_get_index_typeinfo(typename)!=NULL) :
	      (kno_get_pool_typeinfo(typename)!=NULL) ) {}
    else if (strchr(typename,':')) {
      u8_string colon = strchr(typename,':');
      STR_EXTRACT(modname,typename,colon);
      lispval modspec = kno_intern(modname);
      lispval mod = kno_find_module(modspec,0);
      if (KNO_VOIDP(mod)) {
	modspec = knostring(modname);
	mod = kno_find_module(modspec,0);
	kno_decref(modspec);}
      kno_decref(mod);}
    else NO_ELSE;
    kno_decref(type);
    lispval modules = kno_getopt(opts,KNOSYM_MODULE,KNO_VOID);
    maybe_load_modules(modules,context);
    kno_decref(modules);}
}

DEFC_PRIM("prefetch-slotvals!",prefetch_slotvals,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "Prefetches all index entries where any *slotids* have any *values*.",
	  {"index",kno_any_type,KNO_VOID},
	  {"slotids",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID})
static lispval prefetch_slotvals(lispval index,lispval slotids,lispval values)
{
  kno_index ix = kno_indexptr(index);
  if (ix) kno_find_prefetch(ix,slotids,values);
  else return kno_type_error("index","prefetch_slotvals",index);
  return VOID;
}

DEFC_PRIMN("find-frames/prefetch!",find_frames_prefetch,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	   "`(find-frames-prefetch! *index* [*slots* *values*]...)` "
	   "Prefeteches index entries for doing a find-frames search.")
static lispval find_frames_prefetch(int n,kno_argvec args)
{
  int i = (n%2);
  kno_index ix = ((n%2) ? (kno_indexptr(args[0])) : ((kno_index)(kno_default_background)));
  if (RARELY(ix == NULL))
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

DEFC_PRIM("index-frame",index_frame_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "Adds index entries in *indexes* to *frames* for "
	  "the specified slot-value pairs. If *values* is not "
	  "provided, the current values of each slot on each *frame* "
	  "are used.",
	  {"indexes",kno_any_type,KNO_VOID},
	  {"frames",kno_any_type,KNO_VOID},
	  {"slotids",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID})
static lispval index_frame_prim
(lispval indexes,lispval frames,lispval slotids,lispval values)
{
  if (CHOICEP(indexes)) {
    DO_CHOICES(index,indexes)
      if (HASHTABLEP(index))
	hashtable_index_frame(index,frames,slotids,values);
      else {
	kno_index ix = kno_indexptr(index);
	if (RARELY(ix == NULL))
	  return kno_type_error("index","index_frame_prim",index);
	else if (kno_index_frame(ix,frames,slotids,values)<0)
	  return KNO_ERROR;}}
  else if (HASHTABLEP(indexes)) {
    hashtable_index_frame(indexes,frames,slotids,values);
    return VOID;}
  else {
    kno_index ix = kno_indexptr(indexes);
    if (RARELY(ix == NULL))
      return kno_type_error("index","index_frame_prim",indexes);
    else if (kno_index_frame(ix,frames,slotids,values)<0)
      return KNO_ERROR;}
  return VOID;
}

/* Pool and index functions */

DEFC_PRIM("source->pool",source2pool,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the pool stored in *source*, if it has been "
	  "loaded into the current session.",
	  {"source",kno_string_type,KNO_VOID})
static lispval source2pool(lispval source)
{
  kno_pool p = kno_find_pool_by_source(KNO_CSTRING(source));
  if (p) return kno_pool2lisp(p); else return KNO_FALSE;
}

DEFC_PRIM("source->index",source2index,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the index stored in *source*, if it has been "
	  "loaded into the current session.",
	  {"source",kno_string_type,KNO_VOID})
static lispval source2index(lispval source)
{
  kno_index ix = kno_find_index_by_source(KNO_CSTRING(source));
  if (ix) return kno_index2lisp(ix); else return KNO_FALSE;
}

DEFC_PRIM("name->pool",name2pool,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the pool stored in *name*, if it has been "
	  "loaded into the current session.",
	  {"name",kno_string_type,KNO_VOID})
static lispval name2pool(lispval name)
{
  kno_pool p = kno_find_pool(KNO_CSTRING(name));
  if (p) return kno_pool2lisp(p); else return KNO_FALSE;
}

DEFC_PRIM("name->index",name2index,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the index stored in *name*, if it has been "
	  "loaded into the current session.",
	  {"name",kno_string_type,KNO_VOID})
static lispval name2index(lispval name)
{
  kno_index ix = kno_find_index(KNO_CSTRING(name));
  if (ix) return kno_index2lisp(ix); else return KNO_FALSE;
}

DEFC_PRIM("getpool",getpool,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a pool based on *arg*. If *arg* is an "
	  "OID, it's containing pool is returned, if it's a "
	  "pool itself, it's returned as is, and if it's a "
	  "string, it tries to resolve it to a pool",
	  {"arg",kno_any_type,KNO_VOID})
static lispval getpool(lispval arg)
{
  kno_pool p = NULL;
  if (OIDP(arg)) p = kno_oid2pool(arg);
  else if (KNO_POOLP(arg)) return kno_incref(arg);
  else if (STRINGP(arg))
    p = kno_name2pool(CSTRING(arg));
  else NO_ELSE;
  if (p) return kno_pool2lisp(p);
  else return EMPTY;
}

DEFC_PRIM("oid->pool",oid2pool,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the pool containing OID or {} if it's not "
	  "known.",
	  {"oid",kno_oid_type,KNO_VOID})
static lispval oid2pool(lispval oid)
{
  kno_pool p = kno_oid2pool(oid);
  if (p) return kno_pool2lisp(p);
  else return KNO_EMPTY;
}

static u8_condition Unknown_PoolName=_("Unknown pool name");

DEFC_PRIM("set-pool-namefn!",set_pool_namefn,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Sets the namefn of *pool* to *method*. When OIDs are being "
	  "displayed verbosely, *method* is used to generate OID reference "
	  "strings, which appear after the OID address. *method* can be a "
	  "slotid or an applicable function.",
	  {"arg",kno_any_type,KNO_VOID},
	  {"method",kno_any_type,KNO_VOID})
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

DEFC_PRIM("set-cache-level!",set_cache_level,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Sets the cachelevel of *pool* to *level*. While individual "
	  "pool drivers interpret this level, in general zero (0) "
	  "means no caching, 1 means just OID value caching, 2 means "
	  "cache file offset tables.",
	  {"arg",kno_any_type,KNO_VOID},
	  {"level",kno_any_type,KNO_VOID})
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

DEFC_PRIM("use-pool",use_pool,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Adds the pool maintained at *source* to the "
	  "object background of the current session. ",
	  {"source",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval use_pool(lispval source,lispval opts)
{
  load_db_module(opts,"use_pool",0);
  if (KNO_POOLP(source))
    // TODO: Should check to make sure that it's in the background
    return kno_incref(source);
  else if (!(STRINGP(source)))
    return kno_type_error(_("string"),"use_pool",source);
  else {
    kno_pool p = kno_get_pool(CSTRING(source),-1,opts);
    if (p) return kno_pool2lisp(p);
    else return kno_err(kno_NoSuchPool,"use_pool",
			CSTRING(source),VOID);}
}

DEFC_PRIM("adjunct-pool",adjunct_pool,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Gets and returns the pool maintained at *source*. This "
	  "does not add the pool to the object background.",
	  {"source",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval adjunct_pool(lispval source,lispval opts)
{
  load_db_module(opts,"adjunct_pool",0);
  if ( (KNO_POOLP(source)) || (TYPEP(source,kno_consed_pool_type)) )
    // TODO: Should check it's really adjunct, if that's the right thing?
    return kno_incref(source);
  else if (!(STRINGP(source)))
    return kno_type_error(_("string"),"adjunct_pool",source);
  else {
    kno_storage_flags flags = kno_get_dbflags(opts,KNO_STORAGE_ISPOOL) |
      KNO_POOL_ADJUNCT;
    kno_pool p = kno_get_pool(CSTRING(source),flags,opts);
    if (p)
      return kno_pool2lisp(p);
    else return KNO_ERROR;}
}

DEFC_PRIM("try-pool",try_pool,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Tries to add a pool maintained at *source* to the "
	  "object background of the current session. Returns false "
	  "if it fails",
	  {"source",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval try_pool(lispval source,lispval opts)
{
  load_db_module(opts,"try_pool",0);
  if ( (KNO_POOLP(source)) || (TYPEP(source,kno_consed_pool_type)) )
    return kno_incref(source);
  else if (!(STRINGP(source)))
    return kno_type_error(_("string"),"load_pool",source);
  else {
    kno_storage_flags flags = kno_get_dbflags(opts,KNO_STORAGE_ISPOOL) |
      KNO_STORAGE_NOERR;
    kno_pool p = kno_get_pool(CSTRING(source),flags,opts);
    if (p)
      return kno_pool2lisp(p);
    else return KNO_FALSE;}
}

DEFC_PRIM("use-index",use_index,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "adds the index at *source* to the search background",
	  {"source",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval use_index(lispval source,lispval opts)
{
  kno_index ixresult = NULL;
  load_db_module(opts,"use_index",1);
  if (INDEXP(source)) {
    ixresult = kno_indexptr(source);
    if (ixresult) kno_add_to_background(ixresult);
    else return kno_type_error("index","index_frame_prim",source);
    return kno_incref(source);}
  else if (STRINGP(source))
    if (strchr(CSTRING(source),';')) {
      /* We explicitly handle ; separated arguments here, so that
	 we can return the choice of index. */
      lispval results = EMPTY;
      u8_byte *copy = u8_strdup(CSTRING(source));
      u8_byte *start = copy, *end = strchr(start,';');
      *end='\0'; while (start) {
	kno_index ix = kno_use_index
	  (start,kno_get_dbflags(opts,KNO_STORAGE_ISINDEX),opts);
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
	   (CSTRING(source),kno_get_dbflags(opts,KNO_STORAGE_ISINDEX),opts);
  else return kno_type_error(_("index spec"),"use_index",source);
  if (ixresult)
    return index_ref(ixresult);
  else return KNO_ERROR;
}

static lispval open_index_helper(lispval source,lispval opts,int registered)
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
  if (STRINGP(source)) {
    if (strchr(CSTRING(source),';')) {
      /* We explicitly handle ; separated arguments here, so that
	 we can return the choice of index. */
      lispval results = EMPTY;
      u8_byte *copy = u8_strdup(CSTRING(source));
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
    else return index_ref(kno_get_index(CSTRING(source),flags,opts));}
  else if (KNO_ETERNAL_INDEXP(source))
    return source;
  else if (KNO_CONSED_INDEXP(source))
    return kno_incref(source);
  else kno_seterr(kno_TypeError,"use_index",NULL,kno_incref(source));
  if (ix)
    return index_ref(ix);
  else return KNO_ERROR;
}

DEFC_PRIM("open-index",open_index,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "opens the index at *source* and returns it, "
	  "not adding it to the current background.",
	  {"source",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval open_index(lispval source,lispval opts)
{
  load_db_module(opts,"open_index",1);
  return open_index_helper(source,opts,-1);
}

DEFC_PRIM("register-index",register_index,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "(REGISTER-INDEX *spec* [*opts*]) "
	  "opens, registers, and returns an index",
	  {"arg",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval register_index(lispval arg,lispval opts)
{
  load_db_module(opts,"register_index",1);
  return open_index_helper(arg,opts,1);
}

DEFC_PRIM("cons-index",cons_index,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "(CONS-INDEX *spec* [*opts*]) "
	  "opens and returns an unregistered index",
	  {"arg",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval cons_index(lispval arg,lispval opts)
{
  load_db_module(opts,"cons_index",1);
  return open_index_helper(arg,opts,0);
}

DEFC_PRIM("make-pool",make_pool,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Creates a new file pool (of some type) at *path* "
	  "using *opts*. Opts must include a base (an OID), "
	  "a capacity (a positive integer power of 2), and "
	  "a type. Unless the option *background* is #f or "
	  "the option *adjunct* is **not** #f, this pool "
	  "is added to the session background used for OID "
	  "resolution.",
	  {"path",kno_string_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval make_pool(lispval path,lispval opts)
{
  load_db_module(opts,"make_pool",0);
  kno_pool p = NULL;
  lispval type = kno_getopt(opts,KNOSYM(pooltype),VOID);
  if (KNO_VOIDP(type)) type = kno_getopt(opts,KNOSYM_TYPE,VOID);
  if (KNO_VOIDP(type)) type = kno_getopt(opts,KNOSYM_DRIVER,VOID);
  if (KNO_VOIDP(type)) type = kno_getopt(opts,KNOSYM_MODULE,VOID);
  kno_storage_flags flags = kno_get_dbflags(opts,KNO_STORAGE_ISPOOL);
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

DEFC_PRIM("pool-type?",known_pool_typep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *stringy* (a symbol or string) is a "
	  "valid 'pooltype argument.",
	  {"string",kno_any_type,KNO_VOID})
static lispval known_pool_typep(lispval stringy)
{
  kno_pool_typeinfo ptype = (KNO_SYMBOLP(stringy)) ?
    (kno_get_pool_typeinfo(KNO_SYMBOL_NAME(stringy))) :
    (KNO_STRINGP(stringy)) ?
    (kno_get_pool_typeinfo(KNO_CSTRING(stringy))) :
    (NULL);
  if (ptype) return KNO_TRUE; else return KNO_FALSE;
}

DEFC_PRIM("pool-type",get_pool_type,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the type of *pool* or, if *pool* is a string, "
	  "then corresponding named type is returned.",
	  {"pool",kno_any_type,KNO_VOID})
static lispval get_pool_type(lispval spec)
{
  if (KNO_STRINGP(spec)) {
    kno_pool_typeinfo ptype = kno_get_pool_typeinfo(KNO_CSTRING(spec));
    if (ptype) return ptype->pool_typeid;
    else return KNO_FALSE;}
  else if (KNO_SYMBOLP(spec)) {
    kno_pool_typeinfo ptype = kno_get_pool_typeinfo(KNO_SYMBOL_NAME(spec));
    if (ptype) return ptype->pool_typeid;
    else return KNO_FALSE;}
  else if (KNO_POOLP(spec)) {
    kno_pool ix = kno_lisp2pool(spec);
    kno_pool_typeinfo ptype = kno_pool_typeinfo_by_handler(ix->pool_handler);
    if (ix) return ptype->pool_typeid;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFC_PRIM("open-pool",open_pool,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Returns an open file pool object for *path* "
	  "using *opts*. Unless the option *background* is #f "
	  "or the file pool itself is declared **adjunct**, this "
	  "pool is added to the session background used for OID "
	  "resolution.",
	  {"path",kno_string_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_FALSE})
static lispval open_pool(lispval path,lispval opts)
{
  load_db_module(opts,"open_pool",0);
  kno_storage_flags flags = kno_get_dbflags(opts,KNO_STORAGE_ISPOOL);
  kno_pool p = kno_open_pool(CSTRING(path),flags,opts);
  if (p)
    return kno_pool2lisp(p);
  else return KNO_ERROR;
}

DEFC_PRIM("make-index",make_index,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Creates a new file index (of some type) at *path* "
	  "using *opts*. Unless the option *background* is #f or "
	  "the option *adjunct* is **not** #f, this pool "
	  "is added to the session background used for OID "
	  "resolution. The *size* option, when provided, may "
	  "determine the new index file's capacity.",
	  {"path",kno_string_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval make_index(lispval path,lispval opts)
{
  load_db_module(opts,"make_index",1);
  kno_index ix = NULL;
  lispval type = kno_getopt(opts,KNOSYM(indextype),VOID);
  if (KNO_VOIDP(type)) type = kno_getopt(opts,KNOSYM_TYPE,VOID);
  if (KNO_VOIDP(type)) type = kno_getopt(opts,KNOSYM_DRIVER,VOID);
  if (KNO_VOIDP(type)) type = kno_getopt(opts,KNOSYM_MODULE,VOID);
  kno_storage_flags flags =
    (FIXNUMP(opts)) ?
    (KNO_STORAGE_ISINDEX) :
    (kno_get_dbflags(opts,KNO_STORAGE_ISINDEX)) ;
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

DEFC_PRIM("index-type?",known_index_typep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *stringy* (a symbol or string) is a "
	  "valid 'indextype argument.",
	  {"stringy",kno_any_type,KNO_VOID})
static lispval known_index_typep(lispval stringy)
{
  kno_index_typeinfo ixtype = (KNO_SYMBOLP(stringy)) ?
    (kno_get_index_typeinfo(KNO_SYMBOL_NAME(stringy))) :
    (KNO_STRINGP(stringy)) ?
    (kno_get_index_typeinfo(KNO_CSTRING(stringy))) :
    (NULL);
  if (ixtype) return KNO_TRUE; else return KNO_FALSE;
}

DEFC_PRIM("index-type",get_index_type,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the type of *index* or, if *index* is a string, "
	  "then corresponding named type is returned.",
	  {"index",kno_any_type,KNO_VOID})
static lispval get_index_type(lispval spec)
{
  if (KNO_STRINGP(spec)) {
    kno_index_typeinfo ixtype = kno_get_index_typeinfo(KNO_CSTRING(spec));
    if (ixtype) return ixtype->index_typeid;
    else return KNO_FALSE;}
  else if (KNO_SYMBOLP(spec)) {
    kno_index_typeinfo ixtype = kno_get_index_typeinfo(KNO_SYMBOL_NAME(spec));
    if (ixtype) return ixtype->index_typeid;
    else return KNO_FALSE;}
  else if (KNO_INDEXP(spec)) {
    kno_index ix = kno_lisp2index(spec);
    kno_index_typeinfo ixtype = kno_index_typeinfo_by_handler(ix->index_handler);
    if (ix) return ixtype->index_typeid;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFC_PRIM("oid-value",oidvalue,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Resolves the value of OID in the pools declared "
	  "for the background of the current session",
	  {"arg",kno_oid_type,KNO_VOID})
static lispval oidvalue(lispval arg)
{
  return kno_oid_value(arg);
}

DEFC_PRIM("set-oid-value!",setoidvalue,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "directly sets the value of *oid* to *value*. If "
	  "the value is a slotmap or schemap, a copy is "
	  "stored unless *nocopy* is not false (the default).",
	  {"o",kno_oid_type,KNO_VOID},
	  {"v",kno_any_type,KNO_VOID},
	  {"nocopy",kno_any_type,KNO_FALSE})
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

DEFC_PRIM("%set-oid-value!",xsetoidvalue,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "directly sets the value of *oid* to *value*. If "
	  "the value is a slotmap or schemap, a copy is "
	  "stored unless *nocopy* is not false (the default).",
	  {"o",kno_oid_type,KNO_VOID},
	  {"v",kno_any_type,KNO_VOID})
static lispval xsetoidvalue(lispval o,lispval v)
{
  int retval;
  kno_incref(v);
  retval = kno_set_oid_value(o,v);
  kno_decref(v);
  if (retval<0)
    return KNO_ERROR;
  else return VOID;
}

DEFC_PRIM("lock-oid!",lockoid,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Tries to lock the OID *oid* for editing by "
	  "locking it on the underlying pool. If *soft* is #t, "
	  "this returns #f if it fails, rather than signalling "
	  "an error.",
	  {"oid",kno_oid_type,KNO_VOID},
	  {"soft",kno_any_type,KNO_VOID})
static lispval lockoid(lispval oid,lispval soft)
{
  int retval = kno_lock_oid(oid);
  if (retval<0)
    if (KNO_TRUEP(soft)) {
      kno_poperr(NULL,NULL,NULL,NULL);
      return KNO_FALSE;}
    else return KNO_ERROR;
  else return KNO_INT(retval);
}

DEFC_PRIM("locked?",oidlockedp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns #t if *oid* is locked, and #f "
	  "if it either isn't an OID or isn't locked.",
	  {"oid",kno_any_type,KNO_VOID})
static lispval oidlockedp(lispval oid)
{
  if (!(OIDP(oid)))
    return KNO_FALSE;
  else {
    kno_pool p = kno_oid2pool(oid);
    if ( (p) && (kno_hashtable_probe_novoid(&(p->pool_changes),oid)) )
      return KNO_TRUE;
    else return KNO_FALSE;}
}

DEFC_PRIM("lock-oids!",lockoids,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "Locks OIDs on their underlying pools.",
	  {"oids",kno_any_type,KNO_VOID})
static lispval lockoids(lispval oids)
{
  int retval = kno_lock_oids(oids);
  if (retval<0)
    return KNO_ERROR;
  else return KNO_INT(retval);
}

DEFC_PRIM("locked-oids",lockedoids,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the OIDs currently locked for the pool *pool*.",
	  {"pool",kno_any_type,KNO_VOID})
static lispval lockedoids(lispval pool)
{
  kno_pool p = kno_lisp2pool(pool);
  return kno_hashtable_keys(&(p->pool_changes));
}

DEFC_PRIM("unlock-oids!",unlockoids,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(0)|KNO_NDCALL,
	  "Unlocks *oids* on their corresponding pools. If "
	  "*commitp* is unspecified or #default, this automatically "
	  "commits any modified values. If *commitp* is #f, any modifications "
	  "are discarded. Otherwise, local modifications are made local and "
	  "ephemeral.",
	  {"oids",kno_any_type,KNO_VOID},
	  {"commitp",kno_any_type,KNO_VOID})
static lispval unlockoids(lispval oids,lispval commitp)
{
  int force_commit = (VOIDP(commitp)) ? (commit_modified) :
    (DEFAULTP(commitp)) ? (commit_modified) :
    (FALSEP(commitp)) ? (discard_modified) :
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

DEFC_PRIM("make-aggregate-index",make_aggregate_index,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "Creates an aggregate index from a collection of "
	  "other indexes",
	  {"sources",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_FALSE})
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

DEFC_PRIM("aggregate-index?",aggregate_indexp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(AGGREGATE-INDEX? *arg*) "
	  "=> true if *arg* is an aggregate index",
	  {"arg",kno_any_type,KNO_VOID})
static lispval aggregate_indexp(lispval arg)
{
  kno_index ix = kno_indexptr(arg);
  if (ix == NULL)
    return KNO_FALSE;
  else if (kno_aggregate_indexp(ix))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("extend-aggregate-index!",extend_aggregate_index,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "(EXTEND-AGGREGATE-INDEX! *agg* *index*) "
	  "adds *index* to the aggregate index *agg*",
	  {"into_arg",kno_any_type,KNO_VOID},
	  {"partition_arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("tempindex?",tempindexp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(TEMPINDEX? *arg*) "
	  "returns #t if *arg* is a temporary index.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval tempindexp(lispval arg)
{
  kno_index ix = kno_indexptr(arg);
  if (ix == NULL)
    return KNO_FALSE;
  else if (kno_tempindexp(ix))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("make-mempool",make_mempool,
	  KNO_MAX_ARGS(6)|KNO_MIN_ARGS(2),
	  "Makes an emphemeral in-memory pool with a given base "
	  "and capacity.",
	  {"label",kno_string_type,KNO_VOID},
	  {"base",kno_oid_type,KNO_VOID},
	  {"cap",kno_fixnum_type,KNO_INT(1048576)},
	  {"load",kno_fixnum_type,KNO_INT(0)},
	  {"noswap",kno_any_type,KNO_FALSE},
	  {"opts",kno_any_type,KNO_FALSE})
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

DEFC_PRIM("clean-mempool",clean_mempool,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Removes all OID values from *pool* which aren't referenced from "
	  "anywhere else.",
	  {"pool_arg",kno_any_type,KNO_VOID})
static lispval clean_mempool(lispval pool)
{
  int retval = kno_clean_mempool(kno_lisp2pool(pool));
  if (retval<0) return KNO_ERROR;
  else return KNO_INT(retval);
}

DEFC_PRIM("reset-mempool",reset_mempool,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Removes all OID values from *pool* and resets the load, "
	  "so those addresses may be reallocated.",
	  {"pool",kno_any_type,KNO_VOID})
static lispval reset_mempool(lispval pool)
{
  int retval = kno_reset_mempool(kno_lisp2pool(pool));
  if (retval<0) return KNO_ERROR;
  else return KNO_INT(retval);
}

DEFC_PRIM("make-procpool",make_procpool,
	  KNO_MAX_ARGS(6)|KNO_MIN_ARGS(4),
	  "Returns a pool implemented by userspace functions",
	  {"label",kno_string_type,KNO_VOID},
	  {"base",kno_oid_type,KNO_VOID},
	  {"cap",kno_fixnum_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID},
	  {"state",kno_any_type,KNO_VOID},
	  {"load",kno_fixnum_type,KNO_INT(0)})
static lispval make_procpool(lispval label,
			     lispval base,lispval cap,
			     lispval opts,lispval state,
			     lispval load)
{
  load_db_module(opts,"make_procpool",0);
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

DEFC_PRIM("make-extpool",make_extpool,
	  KNO_MAX_ARGS(10)|KNO_MIN_ARGS(4),
	  "Returns a pool implemented by userspace functions. These "
	  "are simpler than **procpools** and intended mostly to wrap "
	  "simple external databses (e.g. SQL).",
	  {"label",kno_string_type,KNO_VOID},
	  {"base",kno_oid_type,KNO_VOID},
	  {"cap",kno_fixnum_type,KNO_VOID},
	  {"fetchfn",kno_any_type,KNO_VOID},
	  {"savefn",kno_any_type,KNO_VOID},
	  {"lockfn",kno_any_type,KNO_VOID},
	  {"allocfn",kno_any_type,KNO_VOID},
	  {"state",kno_any_type,KNO_VOID},
	  {"cache",kno_any_type,KNO_TRUE},
	  {"opts",kno_any_type,KNO_FALSE})
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

DEFC_PRIM("extpool-cache!",extpool_setcache,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	  "Sets the cached value for *oid* in *extpool* to *value*",
	  {"extpool",kno_pool_type,KNO_VOID},
	  {"oid",kno_oid_type,KNO_VOID},
	  {"value",kno_any_type,KNO_VOID})
static lispval extpool_setcache(lispval extpool,lispval oid,lispval value)
{
  kno_pool p = kno_lisp2pool(extpool);
  if (kno_extpool_cache_value(p,oid,value)<0)
    return KNO_ERROR;
  else return VOID;
}

DEFC_PRIM("extpool-fetchfn",extpool_fetchfn,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the fetchfn for *extpool*",
	  {"extpool",kno_pool_type,KNO_VOID})
static lispval extpool_fetchfn(lispval extpool)
{
  kno_pool p = kno_lisp2pool(extpool);
  if ( (p) && (p->pool_handler== &kno_extpool_handler) ) {
    struct KNO_EXTPOOL *ep = (struct KNO_EXTPOOL *)p;
    return kno_incref(ep->fetchfn);}
  else return kno_type_error("extpool","extpool_fetchfn",extpool);
}

DEFC_PRIM("extpool-savefn",extpool_savefn,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the savefn for *extpool*",
	  {"extpool",kno_pool_type,KNO_VOID})
static lispval extpool_savefn(lispval extpool)
{
  kno_pool p = kno_lisp2pool(extpool);
  if ( (p) && (p->pool_handler== &kno_extpool_handler) ) {
    struct KNO_EXTPOOL *ep = (struct KNO_EXTPOOL *)p;
    return kno_incref(ep->savefn);}
  else return kno_type_error("extpool","extpool_savefn",extpool);
}

DEFC_PRIM("extpool-lockfn",extpool_lockfn,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the lockfn for *extpool*",
	  {"extpool",kno_pool_type,KNO_VOID})
static lispval extpool_lockfn(lispval extpool)
{
  kno_pool p = kno_lisp2pool(extpool);
  if ( (p) && (p->pool_handler== &kno_extpool_handler) ) {
    struct KNO_EXTPOOL *ep = (struct KNO_EXTPOOL *)p;
    return kno_incref(ep->lockfn);}
  else return kno_type_error("extpool","extpool_lockfn",extpool);
}

DEFC_PRIM("extpool-state",extpool_state,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the state object of an extpool",
	  {"pool",kno_pool_type,KNO_VOID})
static lispval extpool_state(lispval extpool)
{
  kno_pool p = kno_lisp2pool(extpool);
  if ( (p) && (p->pool_handler== &kno_extpool_handler) ) {
    struct KNO_EXTPOOL *ep = (struct KNO_EXTPOOL *)p;
    return kno_incref(ep->state);}
  else return kno_type_error("extpool","extpool_state",extpool);
}

/* Proc indexes */

DEFC_PRIM("make-procindex",make_procindex,
	  KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
	  "returns an index implemented by userspace methods",
	  {"id",kno_string_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID},
	  {"state",kno_any_type,KNO_VOID},
	  {"source",kno_string_type,KNO_VOID},
	  {"typeid",kno_string_type,KNO_VOID})
static lispval make_procindex(lispval id,
			      lispval opts,lispval state,
			      lispval source,lispval typeid)
{
  load_db_module(opts,"make_procindex",1);
  kno_index ix = kno_make_procindex
    (opts,state,KNO_CSTRING(id),
     ((KNO_VOIDP(source)) ? (NULL) : (KNO_CSTRING(source))),
     ((KNO_VOIDP(typeid)) ? (NULL) : (KNO_CSTRING(typeid))));
  return kno_index2lisp(ix);
}

/* External indexes */

DEFC_PRIM("make-extindex",make_extindex,
	  KNO_MAX_ARGS(6)|KNO_MIN_ARGS(2),
	  "Returns an index implemented by userspace functions. These "
	  "are simpler than **procindexes** and intended mostly to wrap "
	  "simple external databses (e.g. SQL).",
	  {"label",kno_string_type,KNO_VOID},
	  {"fetchfn",kno_any_type,KNO_VOID},
	  {"commitfn",kno_any_type,KNO_VOID},
	  {"state",kno_any_type,KNO_VOID},
	  {"usecache",kno_any_type,KNO_TRUE},
	  {"opts",kno_any_type,KNO_FALSE})
static lispval make_extindex(lispval label,lispval fetchfn,lispval commitfn,
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

DEFC_PRIM("extindex-cacheadd!",extindex_cacheadd,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	  "**undocumented**",
	  {"index",kno_any_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID})
static lispval extindex_cacheadd(lispval index,lispval key,lispval values)
{
  kno_index ix = kno_indexptr(index);
  if ( (ix) && (ix->index_handler == &kno_extindex_handler) )
    if (kno_hashtable_add(&(ix->index_cache),key,values)<0)
      return KNO_ERROR;
    else {}
  else return kno_type_error("extindex","extindex_cacheadd",index);
  return VOID;
}

DEFC_PRIM("extindex-decache!",extindex_decache,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"index",kno_any_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID})
static lispval extindex_decache(lispval index,lispval key)
{
  kno_index ix = kno_indexptr(index);
  if ( (ix) && (ix->index_handler == &kno_extindex_handler) )
    if (VOIDP(key))
      if (kno_reset_hashtable(&(ix->index_cache),ix->index_cache.ht_n_buckets,1)<0)
	return KNO_ERROR;
      else {}
    else if (kno_hashtable_store(&(ix->index_cache),key,VOID)<0)
      return KNO_ERROR;
    else {}
  else return kno_type_error("extindex","extindex_decache",index);
  return VOID;
}

DEFC_PRIM("extindex-fetchfn",extindex_fetchfn,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"index",kno_any_type,KNO_VOID})
static lispval extindex_fetchfn(lispval index)
{
  kno_index ix = kno_indexptr(index);
  if ( (ix) && (ix->index_handler== &kno_extindex_handler) ) {
    struct KNO_EXTINDEX *eix = (struct KNO_EXTINDEX *)ix;
    return kno_incref(eix->fetchfn);}
  else return kno_type_error("extindex","extindex_fetchfn",index);
}

DEFC_PRIM("extindex-commitfn",extindex_commitfn,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"index",kno_any_type,KNO_VOID})
static lispval extindex_commitfn(lispval index)
{
  kno_index ix = kno_indexptr(index);
  if ( (ix) && (ix->index_handler== &kno_extindex_handler) ) {
    struct KNO_EXTINDEX *eix = (struct KNO_EXTINDEX *)ix;
    return kno_incref(eix->commitfn);}
  else return kno_type_error("extindex","extindex_commitfn",index);
}

DEFC_PRIM("extindex-state",extindex_state,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"index",kno_any_type,KNO_VOID})
static lispval extindex_state(lispval index)
{
  kno_index ix = kno_indexptr(index);
  if ( (ix) && (ix->index_handler== &kno_extindex_handler) ) {
    struct KNO_EXTINDEX *eix = (struct KNO_EXTINDEX *)ix;
    return kno_incref(eix->state);}
  else return kno_type_error("extindex","extindex_state",index);
}

DEFC_PRIM("extindex?",extindexp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *arg* is an extindex, #f otherwise",
	  {"index",kno_any_type,KNO_VOID})
static lispval extindexp(lispval index)
{
  kno_index ix = kno_indexptr(index);
  if ( (ix) && (ix->index_handler== &kno_extindex_handler) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Adding adjuncts */

static lispval padjuncts_symbol;

DEFC_PRIM("use-adjunct",use_adjunct,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "(table [slot] [pool])\n"
	  "arranges for *table* to store values of the "
	  "slotid *slot* for objects in *pool*. If *pool* is "
	  "not specified, the adjunct is declared globally.",
	  {"adjunct",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"pool_arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("adjunct!",add_adjunct,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	  "(pool slot table)\n"
	  "arranges for *table* to store values of the "
	  "slotid *slot* for objects in *pool*. Table can be "
	  "an in-memory table, an index or an adjunct pool",
	  {"pool_arg",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID},
	  {"adjunct",kno_any_type,KNO_VOID})
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

DEFC_PRIM("get-adjuncts",get_adjuncts_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Gets the adjuncts associated with the specified pool",
	  {"pool_arg",kno_any_type,KNO_VOID})
static lispval get_adjuncts_prim(lispval pool_arg)
{
  kno_pool p=kno_lisp2pool(pool_arg);
  if (p==NULL)
    return KNO_ERROR;
  else return kno_get_adjuncts(p);
}

DEFC_PRIM("get-adjunct",get_adjunct_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Gets the adjunct for *slotid* associated with *pool*",
	  {"pool_arg",kno_any_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID})
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

DEFC_PRIM("adjunct-value",adjunct_value_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Gets the adjunct value for *slotid* of *obj*",
	  {"oid",kno_oid_type,KNO_VOID},
	  {"slotid",kno_any_type,KNO_VOID})
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

DEFC_PRIM("adjunct?",isadjunctp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "\n"
	  "Returns true if *pool* is an adjunct pool",
	  {"pool_arg",kno_any_type,KNO_VOID})
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

DEFC_PRIMN("swapout",swapout_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDCALL,
	   "**undocumented**")
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
    else if (TYPEP(arg,kno_indexref_type))
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
    return kno_err(kno_TooManyArgs,"swapout",NULL,args[0]);
  else if (KNO_EMPTY_CHOICEP(args[1]))
    return KNO_INT(0);
  else {
    lispval arg, keys; int rv_sum = 0;
    if ((TYPEP(args[0],kno_poolref_type))||
	(TYPEP(args[0],kno_indexref_type))||
	(TYPEP(args[0],kno_consed_pool_type))||
	(TYPEP(args[0],kno_consed_index_type))) {
      arg = args[0]; keys = args[1];}
    else {arg = args[0]; keys = args[1];}
    if (TYPEP(arg,kno_indexref_type))
      kno_index_swapout(kno_indexptr(arg),keys);
    else if (TYPEP(arg,kno_poolref_type))
      rv_sum = kno_pool_swapout(kno_lisp2pool(arg),keys);
    else if (TYPEP(arg,kno_consed_index_type))
      kno_index_swapout(kno_indexptr(arg),keys);
    else if (TYPEP(arg,kno_consed_pool_type))
      rv_sum = kno_pool_swapout((kno_pool)arg,keys);
    else return kno_type_error(_("pool, index, or OIDs"),"swapout_lexpr",arg);
    if (rv_sum<0) return KNO_ERROR;
    else return KNO_INT(rv_sum);}
}

DEFC_PRIMN("commit",commit_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(0),
	   "**undocumented**")
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
    if (TYPEP(arg,kno_indexref_type))
      retval = kno_commit_index(kno_indexptr(arg));
    else if (TYPEP(arg,kno_poolref_type))
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
  else return kno_err(kno_TooManyArgs,"commit",NULL,args[0]);
}

DEFC_PRIM("commit-oids",commit_oids,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"oids",kno_any_type,KNO_VOID})
static lispval commit_oids(lispval oids)
{
  int rv = kno_commit_oids(oids);
  if (rv<0)
    return KNO_ERROR;
  else return VOID;
}

DEFC_PRIM("finish-oids",finish_oids,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"oids",kno_any_type,KNO_VOID},
	  {"pool",kno_pool_type,KNO_VOID})
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

DEFC_PRIM("commit-pool",commit_pool,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"pool",kno_pool_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
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

DEFC_PRIM("commit-finished",commit_finished,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"pool",kno_pool_type,KNO_VOID})
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

DEFC_PRIM("pool/storen!",pool_storen_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	  "Stores values in a pool, skipping the object cache",
	  {"pool",kno_any_type,KNO_VOID},
	  {"oids",kno_vector_type,KNO_VOID},
	  {"values",kno_vector_type,KNO_VOID})
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

DEFC_PRIM("pool/fetchn",pool_fetchn_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Fetches values from a pool, skipping the object "
	  "cache",
	  {"pool",kno_any_type,KNO_VOID},
	  {"oids",kno_any_type,KNO_VOID})
static lispval pool_fetchn_prim(lispval pool,lispval oids)
{
  kno_pool p = kno_lisp2pool(pool);
  if (!(p))
    return kno_type_error("pool","pool_fetchn_prim",pool);

  return kno_pool_fetchn(p,oids);
}

DEFC_PRIM("clear-slotcache!",clear_slotcache,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval clear_slotcache(lispval arg)
{
  if (VOIDP(arg)) kno_clear_slotcaches();
  else kno_clear_slotcache(arg);
  return VOID;
}

DEFC_PRIM("clearcaches",clearcaches,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "**undocumented**")
static lispval clearcaches()
{
  kno_clear_callcache(VOID);
  kno_clear_slotcaches();
  kno_swapout_indexes();
  kno_swapout_pools();
  return VOID;
}

DEFC_PRIM("swapcheck",swapcheck_prim,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "**undocumented**")
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

DEFC_PRIM("pool-load",pool_load,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("pool-capacity",pool_capacity,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval pool_capacity(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_capacity",arg);
  else return KNO_INT(p->pool_capacity);
}

DEFC_PRIM("pool-base",pool_base,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval pool_base(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_base",arg);
  else return kno_make_oid(p->pool_base);
}

DEFC_PRIM("pool-elts",pool_elts,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID},
	  {"start",kno_any_type,KNO_VOID},
	  {"count",kno_any_type,KNO_VOID})
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

DEFC_PRIM("pool-label",pool_label,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID},
	  {"use_source",kno_any_type,KNO_FALSE})
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

DEFC_PRIM("pool-id",pool_id,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("pool-source",pool_source,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("pool-prefix",pool_prefix,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval pool_prefix(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_label",arg);
  else if (p->pool_prefix)
    return kno_mkstring(p->pool_prefix);
  else return KNO_FALSE;
}

DEFC_PRIM("set-pool-prefix!",set_pool_prefix,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID},
	  {"prefix_arg",kno_string_type,KNO_VOID})
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

DEFC_PRIM("pool-close",pool_close_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval pool_close_prim(lispval arg)
{
  kno_pool p = arg2pool(arg);
  if (p == NULL)
    return kno_type_error(_("pool spec"),"pool_close",arg);
  else {
    kno_pool_close(p);
    return VOID;}
}

DEFC_PRIM("oid-range",oid_range,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"start",kno_oid_type,KNO_VOID},
	  {"end",kno_fixnum_type,KNO_VOID})
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

DEFC_PRIM("oid-vector",oid_vector,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"start",kno_oid_type,KNO_VOID},
	  {"end",kno_fixnum_type,KNO_VOID})
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

DEFC_PRIM("random-oid",random_oid,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("pool-vector",pool_vec,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("cachecount",cachecount,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("oid-hi",oid_hi,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"x",kno_oid_type,KNO_VOID})
static lispval oid_hi(lispval x)
{
  KNO_OID addr = KNO_OID_ADDR(x);
  return KNO_INT(KNO_OID_HI(addr));
}

DEFC_PRIM("oid-lo",oid_lo,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"x",kno_oid_type,KNO_VOID})
static lispval oid_lo(lispval x)
{
  KNO_OID addr = KNO_OID_ADDR(x);
  return KNO_INT(KNO_OID_LO(addr));
}

DEFC_PRIM("oid-base",oid_base,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"oid",kno_oid_type,KNO_VOID},
	  {"modulo",kno_fixnum_type,KNO_INT(1048576)})
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

DEFC_PRIM("oid?",oidp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
static lispval oidp(lispval x)
{
  if (OIDP(x)) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("oid-pool",oidpool,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"x",kno_oid_type,KNO_VOID})
static lispval oidpool(lispval x)
{
  kno_pool p = kno_oid2pool(x);
  if (p == NULL) return EMPTY;
  else return kno_pool2lisp(p);
}

DEFC_PRIM("in-pool?",inpoolp,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"x",kno_oid_type,KNO_VOID},
	  {"pool_arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("valid-oid?",validoidp,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"x",kno_oid_type,KNO_VOID},
	  {"pool_arg",kno_any_type,KNO_VOID})
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

static int pool_argp(lispval x)
{
  if (KNO_POOLP(x)) return 1;
  else if (KNO_CHOICEP(x)) {
    int poolp = -1;
    KNO_DO_CHOICES(each,x) {
      if (KNO_POOLP(x)) {
	if (poolp<0) poolp=1;
	else return -1;}
      else if (poolp<0) poolp=0;
      else return -1;}
    return poolp;}
  else return 0;
}

DEFC_PRIM("pool-prefetch!",pool_prefetch_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "'(POOL-PREFETCH! pool oids)' prefetches OIDs from "
	  "pool",
	  {"pool",kno_any_type,KNO_VOID},
	  {"oids",kno_any_type,KNO_VOID})
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

DEFC_PRIM("prefetch-oids!",prefetch_oids_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "prefetches OIDs from pool(s). With no *pool*, prefetches "
	  "OIDS from the background.",
	  {"oids",kno_any_type,KNO_VOID},
	  {"pool",kno_any_type,KNO_VOID})
static lispval prefetch_oids_prim(lispval oids,lispval pool)
{
  if ( (VOIDP(pool)) || (KNO_FALSEP(pool)) || (KNO_TRUEP(pool)) ) {
    if (kno_prefetch_oids(oids)>=0)
      return KNO_TRUE;
    else return KNO_ERROR;}
  int pool_ok = pool_argp(pool);
  if (pool_ok<0)
    return kno_err("NotAPool","prefetch_oids_prim",NULL,pool);
  else if (pool_ok)
    return pool_prefetch_prim(pool,oids);
  else if (pool_argp(oids)==1) {
    lispval use_pool = oids;
    lispval use_oids = pool;
    return pool_prefetch_prim(use_pool,use_oids);}
  else return kno_err("NotAPool","prefetch_oids_prim",NULL,pool);
}

DEFC_PRIM("fetchoids",fetchoids_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "(FETCHOIDS *oids*) "
	  "returns *oids* after prefetching their values.",
	  {"oids",kno_any_type,KNO_VOID})
static lispval fetchoids_prim(lispval oids)
{
  kno_prefetch_oids(oids);
  return kno_incref(oids);
}

static int index_argp(lispval x)
{
  if (KNO_INDEXP(x)) return 1;
  else if (KNO_CHOICEP(x)) {
    int indexp = -1;
    KNO_DO_CHOICES(each,x) {
      if (KNO_INDEXP(x)) {
	if (indexp<0) indexp=1;
	else return -1;}
      else if (indexp<0) indexp=0;
      else return -1;}
    return indexp;}
  else return 0;
}

DEFC_PRIM("prefetch-keys!",prefetch_keys,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "moves mappings for *keys* in *index* into its cache. "
	  "With one argument (no *index*) caches keys from the "
	  "background index.",
	  {"keys",kno_any_type,KNO_VOID},
	  {"index",kno_any_type,KNO_VOID})
static lispval prefetch_keys(lispval keys,lispval index)
{
  if (VOIDP(index)) {
    if (kno_bg_prefetch(keys)<0)
      return KNO_ERROR;
    else return VOID;}
  else if (KNO_EMPTYP(index)) return VOID;
  int index_ok = index_argp(index);
  if (index_ok<0)
    return kno_err("NotAnIndex","prefetch-keys!",NULL,index);
  else if (KNO_EMPTYP(keys)) return KNO_VOID;
  else if (index_ok) {
    DO_CHOICES(each_ix,index) {
      kno_index ix = kno_indexptr(each_ix);
      if (kno_index_prefetch(ix,keys)<0) {
	KNO_STOP_DO_CHOICES;
	return KNO_ERROR;}}
    return KNO_VOID;}
  else if (index_argp(keys)==1) {
    lispval use_index = keys;
    lispval use_keys = index;
    DO_CHOICES(each_ix,use_index) {
      kno_index ix = kno_indexptr(each_ix);
      if (kno_index_prefetch(ix,use_keys)<0) {
	KNO_STOP_DO_CHOICES;
	return KNO_ERROR;}}
    return KNO_VOID;}
  else return kno_err("NotAnIndex","prefetch-keys!",NULL,index);
}

DEFC_PRIM("index-prefetch!",index_prefetch_keys,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "(INDEX-PREFETCH! *index* *keys*)  "
	  "moves mappings for *keys* in *index* into its "
	  "cache.",
	  {"index",kno_any_type,KNO_VOID},
	  {"keys",kno_any_type,KNO_VOID})
static lispval index_prefetch_keys(lispval index,lispval keys)
{
  if (index_argp(index)<0)
    return kno_err("NotAnIndex","prefetch-keys!",NULL,index);
  else {
    DO_CHOICES(ix_arg,index) {
      kno_index ix = kno_indexptr(ix_arg);
      if (kno_index_prefetch(ix,keys)<0) {
	KNO_STOP_DO_CHOICES;
	return KNO_ERROR;}}
    return KNO_VOID;}
}

/* Getting cached OIDs */

DEFC_PRIM("cached-oids",cached_oids,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "**undocumented**",
	  {"pool",kno_any_type,KNO_VOID})
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

DEFC_PRIM("cached-keys",cached_keys,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "**undocumented**",
	  {"index",kno_any_type,KNO_VOID})
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

DEFC_PRIM("cache-load",cache_load,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(CACHE-LOAD *pool-or-index*) "
	  "returns the number of cached items in a database.",
	  {"db",kno_any_type,KNO_VOID})
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

DEFC_PRIM("change-load",change_load,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the number of items modified (or locked "
	  "for modification) in a database",
	  {"db",kno_any_type,KNO_VOID})
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

/* Cache gets */

static lispval cacheget_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval table_arg = kno_get_arg(expr,1), key_arg = kno_get_arg(expr,2);
  lispval default_expr = kno_get_arg(expr,3);
  if (RARELY((VOIDP(table_arg)) ||
		 (VOIDP(key_arg)) ||
		 (VOIDP(default_expr))))
    return kno_err(kno_SyntaxError,"cacheget_evalfn",NULL,expr);
  else {
    lispval table = kno_eval(table_arg,env,_stack), key, value;
    if (KNO_ABORTED(table)) return table;
    else if (TABLEP(table)) key = kno_eval(key_arg,env,_stack);
    else return kno_type_error(_("table"),"cachget_evalfn",table);
    if (KNO_ABORTED(key)) {
      kno_decref(table); return key;}
    else value = kno_get(table,key,VOID);
    if (VOIDP(value)) {
      lispval dflt = kno_eval(default_expr,env,_stack);
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

DEFC_PRIM("index-source",index_source_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval index_source_prim(lispval arg)
{
  kno_index p = arg2index(arg);
  if (p == NULL)
    return kno_type_error(_("index spec"),"index_label",arg);
  else if (p->index_source)
    return kno_mkstring(p->index_source);
  else return KNO_FALSE;
}

DEFC_PRIM("index-id",index_id,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
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

/* Index operations */

DEFC_PRIM("index-get",index_get,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"ixarg",kno_any_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID})
static lispval index_get(lispval ixarg,lispval key)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL)
    return KNO_ERROR;
  else return kno_index_get(ix,key);
}

DEFC_PRIM("index-add!",index_add,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	  "**undocumented**",
	  {"ixarg",kno_any_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID})
static lispval index_add(lispval ixarg,lispval key,lispval values)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL) return KNO_ERROR;
  kno_index_add(ix,key,values);
  return VOID;
}

DEFC_PRIM("index-set!",index_set,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	  "**undocumented**",
	  {"ixarg",kno_any_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID})
static lispval index_set(lispval ixarg,lispval key,lispval values)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL) return KNO_ERROR;
  kno_index_store(ix,key,values);
  return VOID;
}

DEFC_PRIM("index-decache",index_decache,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"ixarg",kno_any_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"value",kno_any_type,KNO_VOID})
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

DEFC_PRIM("bgdecache",bgdecache,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"key",kno_any_type,KNO_VOID},
	  {"value",kno_any_type,KNO_VOID})
static lispval bgdecache(lispval key,lispval value)
{
  kno_index ix = (kno_index)kno_default_background;
  if (ix == NULL) return KNO_ERROR;
  if (VOIDP(value))
    kno_hashtable_op(&(ix->index_cache),kno_table_replace,key,VOID);
  else {
    lispval keypair = kno_conspair(kno_incref(key),kno_incref(value));
    kno_hashtable_op(&(ix->index_cache),kno_table_replace,keypair,VOID);
    kno_decref(keypair);}
  return VOID;
}

DEFC_PRIM("index-keys",index_keys,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"ixarg",kno_any_type,KNO_VOID})
static lispval index_keys(lispval ixarg)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL) kno_type_error("index","index_keys",ixarg);
  return kno_index_keys(ix);
}

DEFC_PRIM("index-sizes",index_sizes,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"ixarg",kno_any_type,KNO_VOID},
	  {"keys_arg",kno_any_type,KNO_VOID})
static lispval index_sizes(lispval ixarg,lispval keys_arg)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL)
    kno_type_error("index","index_sizes",ixarg);
  if (KNO_VOIDP(keys_arg))
    return kno_index_sizes(ix);
  else return kno_index_keysizes(ix,keys_arg);
}

DEFC_PRIM("index-keysvec",index_keysvec,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"ixarg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("index/merge!",index_merge,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Merges a hashtable into the ADDS of an index as a "
	  "batch operation",
	  {"ixarg",kno_any_type,KNO_VOID},
	  {"addstable",kno_any_type,KNO_VOID})
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

DEFC_PRIM("slotindex/merge!",slotindex_merge,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Merges a hashtable or temporary index into the "
	  "ADDS of an index as a batch operation, trying to "
	  "handle conversions between slotkeys if needed.",
	  {"ixarg",kno_any_type,KNO_VOID},
	  {"add",kno_any_type,KNO_VOID})
static lispval slotindex_merge(lispval ixarg,lispval add)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix == NULL)
    return kno_type_error("index","index_merge",ixarg);
  else {
    int rv = kno_slotindex_merge(ix,add);
    return KNO_INT(rv);}
}

DEFC_PRIM("close-index",close_index_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(INDEX-CLOSE *index*) "
	  "closes any resources associated with *index*",
	  {"ix_arg",kno_any_type,KNO_VOID})
static lispval close_index_prim(lispval ix_arg)
{
  kno_index ix = kno_indexptr(ix_arg);
  if (ix == NULL)
    return kno_type_error("index","index_close",ix_arg);
  kno_index_close(ix);
  return VOID;
}

DEFC_PRIM("commit-index",commit_index_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(INDEX-COMMIT *index*) "
	  "saves any buffered changes to *index*",
	  {"ix_arg",kno_any_type,KNO_VOID})
static lispval commit_index_prim(lispval ix_arg)
{
  kno_index ix = kno_indexptr(ix_arg);
  if (ix == NULL)
    return kno_type_error("index","index_close",ix_arg);
  kno_commit_index(ix);
  return VOID;
}

DEFC_PRIM("index/save!",index_save_prim,
	  KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
	  "(INDEX-PREFETCH! *index* *keys*) "
	  "**undocumented**",
	  {"index",kno_any_type,KNO_VOID},
	  {"adds",kno_any_type,KNO_VOID},
	  {"drops",kno_any_type,KNO_VOID},
	  {"stores",kno_any_type,KNO_VOID},
	  {"metadata",kno_any_type,KNO_VOID})
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

DEFC_PRIM("index/fetchn",index_fetchn_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Fetches values from an index, skipping the index "
	  "cache",
	  {"index",kno_any_type,KNO_VOID},
	  {"keys",kno_any_type,KNO_VOID})
static lispval index_fetchn_prim(lispval index,lispval keys)
{
  kno_index ix = kno_lisp2index(index);
  if (!(ix))
    return kno_type_error("index","index_fetchn_prim",index);

  return kno_index_fetchn(ix,keys);
}


DEFC_PRIM("suggest-hash-size",suggest_hash_size,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"size",kno_fixnum_type,KNO_VOID})
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
    if (KNO_QONSTP(pred)) pred = kno_qonst_val(pred);
    lispval rail[2], result = VOID;
    /* Handle the case where the 'slotid' is a unary function which can
       be used to extract an argument. */
    if ((KNO_LAMBDAP(pred)) || (TYPEP(pred,kno_cprim_type))) {
      kno_function fcn = KNO_FUNCTION_INFO(pred); int retval = -1;
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

DEFC_PRIMN("pick",pick_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	   "**undocumented**")
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

DEFC_PRIMN("prefer",prefer_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	   "**undocumented**")
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

DEFC_PRIMN("%pick",prim_pick_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	   "**undocumented**")
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

DEFC_PRIMN("%prefer",prim_prefer_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	   "**undocumented**")
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

DEFC_PRIMN("reject",reject_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	   "**undocumented**")
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

DEFC_PRIMN("avoid",avoid_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	   "**undocumented**")
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

DEFC_PRIMN("%reject",prim_reject_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	   "**undocumented**")
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

DEFC_PRIMN("%avoid",prim_avoid_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	   "**undocumented**")
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

DEFC_PRIM("get*",getstar,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "**undocumented**",
	  {"frames",kno_any_type,KNO_VOID},
	  {"slotids",kno_any_type,KNO_VOID})
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

DEFC_PRIM("inherit",inherit_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "**undocumented**",
	  {"slotids",kno_any_type,KNO_VOID},
	  {"frames",kno_any_type,KNO_VOID},
	  {"through",kno_any_type,KNO_VOID})
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

DEFC_PRIM("path?",pathp,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "**undocumented**",
	  {"frames",kno_any_type,KNO_VOID},
	  {"slotids",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID})
static lispval pathp(lispval frames,lispval slotids,lispval values)
{
  lispval roots = getroots(frames);
  if (KNO_EMPTYP(roots))
    return KNO_FALSE;
  else if (kno_pathp(frames,slotids,values))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("get-basis",getbasis,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "**undocumented**",
	  {"frames",kno_any_type,KNO_VOID},
	  {"lattice",kno_any_type,KNO_VOID})
static lispval getbasis(lispval frames,lispval lattice)
{
  return kno_simplify_choice(kno_get_basis(frames,lattice));
}

/* Frame creation */

DEFC_PRIM("allocate-oids",allocate_oids,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"pool",kno_any_type,KNO_VOID},
	  {"howmany",kno_any_type,KNO_VOID})
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

DEFC_PRIMN("frame-create",frame_create_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "**undocumented**")
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

DEFC_PRIMN("frame-update",frame_update_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(3)|KNO_NDCALL,
	   "**undocumented**")
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

DEFC_PRIM("seq->frame",seq2frame_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "**undocumented**",
	  {"poolspec",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID},
	  {"schema",kno_any_type,KNO_VOID},
	  {"dflt",kno_any_type,KNO_VOID})
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

DEFC_PRIMN("modify-frame",modify_frame_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(3)|KNO_NDCALL,
	   "Modifies each of *frames* by adding alternating "
	   "*slotid* *value* pairs.")
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

DEFC_PRIM("oid-plus",oid_plus_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Adds an integer to an OID address and returns "
	  "that OID",
	  {"oid",kno_oid_type,KNO_VOID},
	  {"increment",kno_fixnum_type,KNO_INT(1)})
static lispval oid_plus_prim(lispval oid,lispval increment)
{
  KNO_OID base = KNO_OID_ADDR(oid), next;
  int delta = kno_getint(increment);
  next = KNO_OID_PLUS(base,delta);
  return kno_make_oid(next);
}

DEFC_PRIM("oid-offset",oid_offset_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Returns the offset of an OID from a container and "
	  "#f if it is not in the container. When the second "
	  "argument is an OID, it is used as the base of "
	  "container; if it is a pool, it is used as the "
	  "container. Without a second argument the "
	  "registered pool for the OID is used as the "
	  "container.",
	  {"oidarg",kno_oid_type,KNO_VOID},
	  {"against",kno_any_type,KNO_VOID})
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

DEFC_PRIM("oid-minus",oid_minus_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Returns the difference between two OIDs. If the "
	  "second argument is a pool, the base of the pool "
	  "is used for comparision",
	  {"oidarg",kno_oid_type,KNO_VOID},
	  {"against",kno_any_type,KNO_VOID})
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

DEFC_PRIM("oid-ptrdata",oid_ptrdata_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the session-specific baseid and offset for *oid*",
	  {"oid",kno_oid_type,KNO_VOID})
static lispval oid_ptrdata_prim(lispval oid)
{
  return kno_conspair(KNO_INT(KNO_OID_BASE_ID(oid)),
		      KNO_INT(KNO_OID_BASE_OFFSET(oid)));
}
#endif

DEFC_PRIM("make-oid",make_oid_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "(MAKE-OID *addr* [*lo*]) "
	  "returns an OID from numeric components. If *lo* "
	  "is not provided (or #f) *addr* is used as the "
	  "complete address. Otherwise, *addr* specifies the "
	  "high part of the OID address.",
	  {"high",kno_any_type,KNO_VOID},
	  {"low",kno_any_type,KNO_VOID})
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

DEFC_PRIM("oid->string",oid2string_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Converns an OID into a string, optionally with suffix label *name*",
	  {"oid",kno_oid_type,KNO_VOID},
	  {"name",kno_any_type,KNO_VOID})
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

DEFC_PRIM("oid->hex",oidhex_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Generates the hex form of *oid*'s address relative to "
	  "*base*.",
	  {"oid",kno_oid_type,KNO_VOID},
	  {"base_arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("hex->oid",hex2oid_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Returns an OID based on *address* which is converted "
	  "into an integer and added to *base*",
	  {"address",kno_any_type,KNO_VOID},
	  {"base",kno_any_type,KNO_VOID})
static lispval hex2oid_prim(lispval address,lispval base)
{
  long long offset;
  if (STRINGP(address))
    offset = strtol(CSTRING(address),NULL,16);
  else if (FIXNUMP(address))
    offset = FIX2INT(address);
  else return kno_type_error("hex offset","hex2oid_prim",address);
  if (offset<0) return kno_type_error("hex offset","hex2oid_prim",address);
  if (OIDP(base)) {
    KNO_OID oid_base = KNO_OID_ADDR(base);
    return oidplus(oid_base,offset);}
  else if (KNO_POOLP(base)) {
    kno_pool p = kno_poolptr(base);
    return oidplus(p->pool_base,offset);}
  else if (STRINGP(base)) {
    kno_pool p = kno_name2pool(CSTRING(base));
    if (p) return oidplus(p->pool_base,offset);
    else return kno_type_error("pool id","hex2oid_prim",base);}
  else return kno_type_error("pool id","hex2oid_prim",base);
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

DEFC_PRIM("oid-addr",oidaddr_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the absolute numeric address of an OID",
	  {"oid",kno_oid_type,KNO_VOID})
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

DEFC_PRIM("sumframe",sumframe_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Returns a set of **summaries** based on extracting *slotids* "
	  "from all of *frames*",
	  {"frames",kno_any_type,KNO_VOID},
	  {"slotids",kno_any_type,KNO_VOID})
static lispval sumframe_prim(lispval frames,lispval slotids)
{
  lispval results = EMPTY;
  DO_CHOICES(frame,frames) {
    lispval slotmap = kno_empty_slotmap();
    DO_CHOICES(slotid,slotids) {
      lispval value = kno_get(frame,slotid,EMPTY);
      if (KNO_ABORTED(value)) {
	kno_decref(results);
	return value;}
      else if (kno_add(slotmap,slotid,value)<0) {
	kno_decref(results);
	kno_decref(value);
	return KNO_ERROR;}
      kno_decref(value);}
    if (OIDP(frame)) {
      KNO_OID addr = KNO_OID_ADDR(frame);
      u8_string s = u8_mkstring("@%x/%x",KNO_OID_HI(addr),KNO_OID_LO(addr));
      lispval idstring = kno_wrapstring(s);
      if (kno_add(slotmap,id_symbol,idstring)<0) {
	kno_decref(results);
	kno_decref(idstring);
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

DEFC_PRIM("forgraph",forgraph,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "**undocumented**",
	  {"fcn",kno_any_type,KNO_VOID},
	  {"roots",kno_any_type,KNO_VOID},
	  {"arcs",kno_any_type,KNO_VOID})
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

DEFC_PRIM("mapgraph",mapgraph,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "**undocumented**",
	  {"fcn",kno_any_type,KNO_VOID},
	  {"roots",kno_any_type,KNO_VOID},
	  {"arcs",kno_any_type,KNO_VOID})
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

DEFC_PRIM("loaded?",dbloadedp,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Returns true if *dbkey* has been fetched in *db*",
	  {"dbkey",kno_any_type,KNO_VOID},
	  {"db",kno_any_type,KNO_VOID})
static lispval dbloadedp(lispval dbkey,lispval db)
{
  if (VOIDP(db))
    if (OIDP(dbkey)) {
      kno_pool p = kno_oid2pool(dbkey);
      if (kno_hashtable_probe(&(p->pool_changes),dbkey)) {
	lispval v = kno_hashtable_probe(&(p->pool_changes),dbkey);
	if ((v!=VOID) || (v!=KNO_LOCKHOLDER)) {
	  kno_decref(v); return KNO_TRUE;}
	else return KNO_FALSE;}
      else if (kno_hashtable_probe(&(p->pool_cache),dbkey))
	return KNO_TRUE;
      else return KNO_FALSE;}
    else if (kno_hashtable_probe(&(kno_default_background->index_cache),dbkey))
      return KNO_TRUE;
    else return KNO_FALSE;
  else if (INDEXP(db)) {
    kno_index ix = kno_indexptr(db);
    if (ix == NULL)
      return kno_type_error("index","loadedp",db);
    else if (kno_hashtable_probe(&(ix->index_cache),dbkey))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (KNO_POOLP(db))
    if (OIDP(dbkey)) {
      kno_pool p = kno_lisp2pool(db);
      if (p == NULL)
	return kno_type_error("pool","loadedp",db);
      if (kno_hashtable_probe(&(p->pool_changes),dbkey)) {
	lispval v = kno_hashtable_probe(&(p->pool_changes),dbkey);
	if ((v!=VOID) || (v!=KNO_LOCKHOLDER)) {
	  kno_decref(v); return KNO_TRUE;}
	else return KNO_FALSE;}
      else if (kno_hashtable_probe(&(p->pool_cache),dbkey))
	return KNO_TRUE;
      else return KNO_FALSE;}
    else return KNO_FALSE;
  else if ((STRINGP(db)) && (OIDP(dbkey))) {
    kno_pool p = kno_lisp2pool(db); kno_index ix;
    if (p)
      if (kno_hashtable_probe(&(p->pool_cache),dbkey))
	return KNO_TRUE;
      else return KNO_FALSE;
    else ix = kno_indexptr(db);
    if (ix == NULL)
      return kno_type_error("pool/index","loadedp",db);
    else if (kno_hashtable_probe(&(ix->index_cache),dbkey))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return kno_type_error("pool/index","loadedp",db);
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

DEFC_PRIM("modified?",dbmodifiedp,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Returns true if *db* has been modified",
	  {"arg1",kno_any_type,KNO_VOID},
	  {"arg2",kno_any_type,KNO_VOID})
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

DEFC_PRIM("db/writable?",db_writablep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if the database *db* is writable. This returns "
	  "#f if *db* isn't a database.",
	  {"db",kno_any_type,KNO_VOID})
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

DEFC_PRIM("make-bloom-filter",make_bloom_filter,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Creates a bloom filter for a a number of items "
	  "and an error rate",
	  {"n_entries",kno_fixnum_type,KNO_VOID},
	  {"allowed_error",kno_flonum_type,KNO_VOID})
static lispval make_bloom_filter(lispval n_entries,lispval allowed_error)
{
  return kno_make_bloom_filter
    (FIX2INT(n_entries),
     (VOIDP(allowed_error))?(0.0004):KNO_FLONUM(allowed_error));
}

static struct KNO_BLOOM *getbloom(lispval x,u8_context caller)
{
  if (KNO_RAW_TYPEP(x,kno_bloom_filter_typetag))
    return (struct KNO_BLOOM *)KNO_RAWPTR_VALUE(x);
  kno_seterr(kno_TypeError,caller,NULL,x);
  return NULL;
}

#define BLOOM_DTYPE_LEN 1000

DEFC_PRIM("bloom/add!",bloom_add,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "(BLOOM/ADD! *filter* *key* [*raw*]) "
	  "adds a key to a bloom filter. The *raw* argument "
	  "indicates that the key is a string or packet "
	  "should be added to the filter. Otherwise, the "
	  "binary DTYPE representation for the value is "
	  "added to the filter.",
	  {"filter",BLOOM_FILTER_TYPE,KNO_VOID},
	  {"value",kno_any_type,KNO_VOID},
	  {"raw_arg",kno_any_type,KNO_FALSE},
	  {"ignore_errors",kno_any_type,KNO_FALSE})
static lispval bloom_add(lispval filter,lispval value,
			 lispval raw_arg,
			 lispval ignore_errors)
{
  struct KNO_BLOOM *bloom = getbloom(filter,"bloom_add");
  if (bloom==NULL) return KNO_ERROR;
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

DEFC_PRIM("bloom/check",bloom_check,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "(BLOOM/CHECK *filter* *keys* [*raw*] [*noerr*]) "
	  "returns true if any of *keys* are probably found "
	  "in *filter*. If *raw* is true, *keys* must be "
	  "strings or packets and their byte values are used "
	  "directly; otherwise, values are converted to "
	  "dtypes before being tested. If *noerr* is true, "
	  "type or conversion errors are just considered "
	  "misses and ignored.",
	  {"filter",BLOOM_FILTER_TYPE,KNO_VOID},
	  {"value",kno_any_type,KNO_VOID},
	  {"raw_arg",kno_any_type,KNO_FALSE},
	  {"ignore_errors",kno_any_type,KNO_FALSE})
static lispval bloom_check(lispval filter,lispval value,
			   lispval raw_arg,
			   lispval ignore_errors)
{
  struct KNO_BLOOM *bloom = getbloom(filter,"bloom_check");
  if (bloom==NULL) return KNO_ERROR;
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

DEFC_PRIM("bloom/hits",bloom_hits,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "(BLOOM/HITS *filter* *keys* [*raw*] [*noerr*]) "
	  "returns the number of *keys* probably found in "
	  "*filter*. If *raw* is true, *keys* must be "
	  "strings or packets and their byte values are used "
	  "directly; otherwise, values are converted to "
	  "dtypes before being tested. If *noerr* is true, "
	  "type or conversion errors are just considered "
	  "misses and ignored.",
	  {"filter",BLOOM_FILTER_TYPE,KNO_VOID},
	  {"value",kno_any_type,KNO_VOID},
	  {"raw_arg",kno_any_type,KNO_FALSE},
	  {"ignore_errors",kno_any_type,KNO_FALSE})
static lispval bloom_hits(lispval filter,lispval value,
			  lispval raw_arg,
			  lispval ignore_errors)
{
  struct KNO_BLOOM *bloom = getbloom(filter,"bloom/hits");
  if (bloom==NULL) return KNO_ERROR;
  int raw = (!(FALSEP(raw_arg)));
  int noerr = (!(FALSEP(ignore_errors)));
  int rv = kno_bloom_op(bloom,value,
			( ( (raw) ? (KNO_BLOOM_RAW) : (0)) |
			  ( (noerr) ? (0) : (KNO_BLOOM_ERR) ) ));
  if (rv<0)
    return KNO_ERROR;
  else return KNO_INT(rv);
}

DEFC_PRIM("bloom-size",bloom_size,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the size (in bytes) of a bloom filter ",
	  {"filter",BLOOM_FILTER_TYPE,KNO_VOID})
static lispval bloom_size(lispval filter)
{
  struct KNO_BLOOM *bloom = getbloom(filter,"bloom_size");
  if (bloom==NULL) return KNO_ERROR;
  return KNO_INT(bloom->entries);
}

DEFC_PRIM("bloom-count",bloom_count,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the number of objects added to a bloom "
	  "filter",
	  {"filter",BLOOM_FILTER_TYPE,KNO_VOID})
static lispval bloom_count(lispval filter)
{
  struct KNO_BLOOM *bloom = getbloom(filter,"bloom-count");
  if (bloom==NULL) return KNO_ERROR;
  return KNO_INT(bloom->bloom_adds);
}

DEFC_PRIM("bloom-error",bloom_error,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the error threshold (a flonum) for the "
	  "filter",
	  {"filter",BLOOM_FILTER_TYPE,KNO_VOID})
static lispval bloom_error(lispval filter)
{
  struct KNO_BLOOM *bloom = getbloom(filter,"bloom-error");
  if (bloom==NULL) return KNO_ERROR;
  return kno_make_double(bloom->error);
}

DEFC_PRIM("bloom-data",bloom_data,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the bytes of the filter as a packet",
	  {"filter",BLOOM_FILTER_TYPE,KNO_VOID})
static lispval bloom_data(lispval filter)
{
  struct KNO_BLOOM *bloom = getbloom(filter,"bloom_data");
  if (bloom==NULL) return KNO_ERROR;
  return kno_bytes2packet(NULL,bloom->bytes,bloom->bf);
}

/* Checking pool and index typeids */

DEFC_PRIM("pooltype?",pool_typep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *arg* is a known pool type",
	  {"typesym",kno_symbol_type,KNO_VOID})
static lispval pool_typep(lispval typesym)
{
  struct KNO_POOL_TYPEINFO *ptype =
    kno_get_pool_typeinfo(KNO_SYMBOL_NAME(typesym));
  if (ptype)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("indextype?",index_typep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *arg* is a known index type",
	  {"typesym",kno_symbol_type,KNO_VOID})
static lispval index_typep(lispval typesym)
{
  struct KNO_INDEX_TYPEINFO *ptype =
    kno_get_index_typeinfo(KNO_SYMBOL_NAME(typesym));
  if (ptype)
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Registering procpools and procindexes */

DEFC_PRIM("defpooltype",def_procpool,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Registers handlers for a procpool",
	  {"typesym",kno_any_type,KNO_VOID},
	  {"handlers",kno_any_type,KNO_VOID})
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

DEFC_PRIM("defindextype",def_procindex,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Registers handlers for a procindex",
	  {"typesym",kno_any_type,KNO_VOID},
	  {"handlers",kno_any_type,KNO_VOID})
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

DEFC_PRIM("procpool?",procpoolp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns #t if it's argument is a procpool",
	  {"pool",kno_any_type,KNO_VOID})
static lispval procpoolp(lispval pool)
{
  kno_pool p = kno_poolptr(pool);
  if ( (p) && (p->pool_handler == &kno_procpool_handler) )
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("procindex?",procindexp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns #t if it's argument is a procindex",
	  {"index",kno_any_type,KNO_VOID})
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

  kno_register_tag_type(kno_bloom_filter_typetag,BLOOM_FILTER_TYPE);

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

static void import_schemefn(u8_string name)
{
  kno_defalias2(kno_db_module,name,kno_scheme_module,name);
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("procindex?",procindexp,1,kno_db_module);
  KNO_LINK_CPRIM("procpool?",procpoolp,1,kno_db_module);
  KNO_LINK_CPRIM("indextype?",index_typep,1,kno_db_module);
  KNO_LINK_CPRIM("pooltype?",pool_typep,1,kno_db_module);
  KNO_LINK_CPRIM("defindextype",def_procindex,2,kno_db_module);
  KNO_LINK_CPRIM("defpooltype",def_procpool,2,kno_db_module);
  KNO_LINK_CPRIM("bloom-data",bloom_data,1,kno_db_module);
  KNO_LINK_CPRIM("bloom-error",bloom_error,1,kno_db_module);
  KNO_LINK_CPRIM("bloom-count",bloom_count,1,kno_db_module);
  KNO_LINK_CPRIM("bloom-size",bloom_size,1,kno_db_module);
  KNO_LINK_CPRIM("bloom/hits",bloom_hits,4,kno_db_module);
  KNO_LINK_CPRIM("bloom/check",bloom_check,4,kno_db_module);
  KNO_LINK_ALIAS("bloom/add",bloom_add,kno_db_module);
  KNO_LINK_CPRIM("bloom/add!",bloom_add,4,kno_db_module);
  KNO_LINK_CPRIM("make-bloom-filter",make_bloom_filter,2,kno_db_module);
  KNO_LINK_CPRIM("db/writable?",db_writablep,1,kno_db_module);
  KNO_LINK_CPRIM("modified?",dbmodifiedp,2,kno_db_module);
  KNO_LINK_CPRIM("loaded?",dbloadedp,2,kno_db_module);
  KNO_LINK_CPRIM("mapgraph",mapgraph,3,kno_db_module);
  KNO_LINK_CPRIM("forgraph",forgraph,3,kno_db_module);
  KNO_LINK_CPRIM("sumframe",sumframe_prim,2,kno_db_module);
  KNO_LINK_CPRIM("oid-addr",oidaddr_prim,1,kno_db_module);
  KNO_LINK_CPRIM("hex->oid",hex2oid_prim,2,kno_db_module);
  KNO_LINK_CPRIM("oid->hex",oidhex_prim,2,kno_db_module);
  KNO_LINK_CPRIM("oid->string",oid2string_prim,2,kno_db_module);
  KNO_LINK_CPRIM("make-oid",make_oid_prim,2,kno_db_module);
  KNO_LINK_CPRIM("oid-ptrdata",oid_ptrdata_prim,1,kno_db_module);
  KNO_LINK_CPRIM("oid-minus",oid_minus_prim,2,kno_db_module);
  KNO_LINK_CPRIM("oid-offset",oid_offset_prim,2,kno_db_module);
  KNO_LINK_CPRIM("oid-plus",oid_plus_prim,2,kno_db_module);
  KNO_LINK_CPRIMN("modify-frame",modify_frame_lexpr,kno_db_module);
  KNO_LINK_CPRIM("seq->frame",seq2frame_prim,4,kno_db_module);
  KNO_LINK_CPRIMN("frame-update",frame_update_lexpr,kno_db_module);
  KNO_LINK_CPRIMN("frame-create",frame_create_lexpr,kno_db_module);
  KNO_LINK_CPRIM("allocate-oids",allocate_oids,2,kno_db_module);
  KNO_LINK_CPRIM("get-basis",getbasis,2,kno_db_module);
  KNO_LINK_CPRIM("path?",pathp,3,kno_db_module);
  KNO_LINK_CPRIM("inherit",inherit_prim,3,kno_db_module);
  KNO_LINK_CPRIM("get*",getstar,2,kno_db_module);
  KNO_LINK_CPRIMN("%avoid",prim_avoid_lexpr,kno_db_module);
  KNO_LINK_CPRIMN("%reject",prim_reject_lexpr,kno_db_module);
  KNO_LINK_CPRIMN("avoid",avoid_lexpr,kno_db_module);
  KNO_LINK_CPRIMN("reject",reject_lexpr,kno_db_module);
  KNO_LINK_CPRIMN("%prefer",prim_prefer_lexpr,kno_db_module);
  KNO_LINK_CPRIMN("%pick",prim_pick_lexpr,kno_db_module);
  KNO_LINK_CPRIMN("prefer",prefer_lexpr,kno_db_module);
  KNO_LINK_CPRIMN("pick",pick_lexpr,kno_db_module);
  KNO_LINK_CPRIM("suggest-hash-size",suggest_hash_size,1,kno_db_module);
  KNO_LINK_CPRIM("index/fetchn",index_fetchn_prim,2,kno_db_module);
  KNO_LINK_CPRIM("index/save!",index_save_prim,5,kno_db_module);
  KNO_LINK_CPRIM("commit-index",commit_index_prim,1,kno_db_module);
  KNO_LINK_CPRIM("close-index",close_index_prim,1,kno_db_module);
  KNO_LINK_CPRIM("index-source",index_source_prim,1,kno_db_module);
  KNO_LINK_CPRIM("slotindex/merge!",slotindex_merge,2,kno_db_module);
  KNO_LINK_CPRIM("index/merge!",index_merge,2,kno_db_module);
  KNO_LINK_CPRIM("index-keysvec",index_keysvec,1,kno_db_module);
  KNO_LINK_CPRIM("index-sizes",index_sizes,2,kno_db_module);
  KNO_LINK_CPRIM("index-keys",index_keys,1,kno_db_module);
  KNO_LINK_CPRIM("bgdecache",bgdecache,2,kno_db_module);
  KNO_LINK_CPRIM("index-decache",index_decache,3,kno_db_module);
  KNO_LINK_CPRIM("index-set!",index_set,3,kno_db_module);
  KNO_LINK_CPRIM("index-add!",index_add,3,kno_db_module);
  KNO_LINK_CPRIM("index-get",index_get,2,kno_db_module);
  KNO_LINK_CPRIM("index-id",index_id,1,kno_db_module);
  KNO_LINK_CPRIM("change-load",change_load,1,kno_db_module);
  KNO_LINK_CPRIM("cache-load",cache_load,1,kno_db_module);
  KNO_LINK_CPRIM("cached-keys",cached_keys,1,kno_db_module);
  KNO_LINK_CPRIM("cached-oids",cached_oids,1,kno_db_module);
  KNO_LINK_CPRIM("index-prefetch!",index_prefetch_keys,2,kno_db_module);
  KNO_LINK_CPRIM("prefetch-keys!",prefetch_keys,2,kno_db_module);
  KNO_LINK_CPRIM("fetchoids",fetchoids_prim,1,kno_db_module);
  KNO_LINK_CPRIM("prefetch-oids!",prefetch_oids_prim,2,kno_db_module);
  KNO_LINK_CPRIM("pool-prefetch!",pool_prefetch_prim,2,kno_db_module);
  KNO_LINK_CPRIM("valid-oid?",validoidp,2,kno_db_module);
  KNO_LINK_CPRIM("in-pool?",inpoolp,2,kno_db_module);
  KNO_LINK_CPRIM("oid-pool",oidpool,1,kno_db_module);
  KNO_LINK_CPRIM("oid?",oidp,1,kno_db_module);
  KNO_LINK_CPRIM("oid-lo",oid_lo,1,kno_db_module);
  KNO_LINK_CPRIM("oid-hi",oid_hi,1,kno_db_module);
  KNO_LINK_CPRIM("oid-base",oid_base,2,kno_db_module);
  KNO_LINK_CPRIM("cachecount",cachecount,1,kno_db_module);
  KNO_LINK_CPRIM("pool-vector",pool_vec,1,kno_db_module);
  KNO_LINK_CPRIM("random-oid",random_oid,1,kno_db_module);
  KNO_LINK_CPRIM("oid-vector",oid_vector,2,kno_db_module);
  KNO_LINK_CPRIM("oid-range",oid_range,2,kno_db_module);
  KNO_LINK_CPRIM("pool-close",pool_close_prim,1,kno_db_module);
  KNO_LINK_CPRIM("set-pool-prefix!",set_pool_prefix,2,kno_db_module);
  KNO_LINK_CPRIM("pool-prefix",pool_prefix,1,kno_db_module);
  KNO_LINK_CPRIM("pool-source",pool_source,1,kno_db_module);
  KNO_LINK_CPRIM("pool-id",pool_id,1,kno_db_module);
  KNO_LINK_CPRIM("pool-label",pool_label,2,kno_db_module);
  KNO_LINK_CPRIM("pool-elts",pool_elts,3,kno_db_module);
  KNO_LINK_CPRIM("pool-base",pool_base,1,kno_db_module);
  KNO_LINK_CPRIM("pool-capacity",pool_capacity,1,kno_db_module);
  KNO_LINK_CPRIM("pool-load",pool_load,1,kno_db_module);
  KNO_LINK_CPRIM("swapcheck",swapcheck_prim,0,kno_db_module);
  KNO_LINK_CPRIM("clearcaches",clearcaches,0,kno_db_module);
  KNO_LINK_CPRIM("clear-slotcache!",clear_slotcache,1,kno_db_module);
  KNO_LINK_CPRIM("pool/fetchn",pool_fetchn_prim,2,kno_db_module);
  KNO_LINK_CPRIM("pool/storen!",pool_storen_prim,3,kno_db_module);
  KNO_LINK_CPRIM("commit-finished",commit_finished,1,kno_db_module);
  KNO_LINK_CPRIM("commit-pool",commit_pool,2,kno_db_module);
  KNO_LINK_CPRIM("finish-oids",finish_oids,2,kno_db_module);
  KNO_LINK_CPRIM("commit-oids",commit_oids,1,kno_db_module);
  KNO_LINK_CPRIMN("commit",commit_lexpr,kno_db_module);
  KNO_LINK_CPRIMN("swapout",swapout_lexpr,kno_db_module);
  KNO_LINK_CPRIM("adjunct?",isadjunctp,1,kno_db_module);
  KNO_LINK_CPRIM("get-adjuncts",get_adjuncts_prim,1,kno_db_module);
  KNO_LINK_CPRIM("get-adjunct",get_adjunct_prim,2,kno_db_module);
  KNO_LINK_CPRIM("adjunct-valuye",adjunct_value_prim,2,kno_db_module);
  KNO_LINK_CPRIM("adjunct!",add_adjunct,3,kno_db_module);
  KNO_LINK_CPRIM("use-adjunct",use_adjunct,3,kno_db_module);

  KNO_LINK_CPRIM("make-extindex",make_extindex,6,kno_db_module);
  KNO_LINK_CPRIM("extindex?",extindexp,1,kno_db_module);
  KNO_LINK_CPRIM("extindex-state",extindex_state,1,kno_db_module);
  KNO_LINK_CPRIM("extindex-commitfn",extindex_commitfn,1,kno_db_module);
  KNO_LINK_CPRIM("extindex-fetchfn",extindex_fetchfn,1,kno_db_module);
  KNO_LINK_CPRIM("extindex-decache!",extindex_decache,2,kno_db_module);
  KNO_LINK_CPRIM("extindex-cacheadd!",extindex_cacheadd,3,kno_db_module);
  KNO_LINK_ALIAS("cons-extindex",make_extindex,kno_db_module);

  KNO_LINK_CPRIM("make-procindex",make_procindex,5,kno_db_module);
  KNO_LINK_CPRIM("extpool-state",extpool_state,1,kno_db_module);
  KNO_LINK_CPRIM("extpool-lockfn",extpool_lockfn,1,kno_db_module);
  KNO_LINK_CPRIM("extpool-savefn",extpool_savefn,1,kno_db_module);
  KNO_LINK_CPRIM("extpool-fetchfn",extpool_fetchfn,1,kno_db_module);
  KNO_LINK_CPRIM("extpool-cache!",extpool_setcache,3,kno_db_module);
  KNO_LINK_CPRIM("make-extpool",make_extpool,10,kno_db_module);
  KNO_LINK_CPRIM("make-procpool",make_procpool,6,kno_db_module);
  KNO_LINK_CPRIM("reset-mempool",reset_mempool,1,kno_db_module);
  KNO_LINK_CPRIM("clean-mempool",clean_mempool,1,kno_db_module);
  KNO_LINK_CPRIM("make-mempool",make_mempool,6,kno_db_module);
  KNO_LINK_CPRIM("tempindex?",tempindexp,1,kno_db_module);
  KNO_LINK_CPRIM("extend-aggregate-index!",extend_aggregate_index,2,kno_db_module);
  KNO_LINK_CPRIM("aggregate-index?",aggregate_indexp,1,kno_db_module);
  KNO_LINK_CPRIM("make-aggregate-index",make_aggregate_index,2,kno_db_module);
  KNO_LINK_CPRIM("unlock-oids!",unlockoids,2,kno_db_module);
  KNO_LINK_CPRIM("locked-oids",lockedoids,1,kno_db_module);
  KNO_LINK_CPRIM("lock-oids!",lockoids,1,kno_db_module);
  KNO_LINK_CPRIM("locked?",oidlockedp,1,kno_db_module);
  KNO_LINK_CPRIM("lock-oid!",lockoid,2,kno_db_module);
  KNO_LINK_CPRIM("%set-oid-value!",xsetoidvalue,2,kno_db_module);
  KNO_LINK_CPRIM("set-oid-value!",setoidvalue,3,kno_db_module);
  KNO_LINK_CPRIM("oid-value",oidvalue,1,kno_db_module);
  KNO_LINK_CPRIM("make-index",make_index,2,kno_db_module);
  KNO_LINK_CPRIM("index-type?",known_index_typep,1,kno_db_module);
  KNO_LINK_CPRIM("index-type",get_index_type,1,kno_db_module);
  KNO_LINK_CPRIM("open-pool",open_pool,2,kno_db_module);
  KNO_LINK_CPRIM("make-pool",make_pool,2,kno_db_module);
  KNO_LINK_CPRIM("pool-type?",known_pool_typep,1,kno_db_module);
  KNO_LINK_CPRIM("pool-type",get_pool_type,1,kno_db_module);
  KNO_LINK_CPRIM("cons-index",cons_index,2,kno_db_module);
  KNO_LINK_CPRIM("register-index",register_index,2,kno_db_module);
  KNO_LINK_CPRIM("open-index",open_index,2,kno_db_module);
  KNO_LINK_CPRIM("use-index",use_index,2,kno_db_module);
  KNO_LINK_CPRIM("use-pool",use_pool,2,kno_db_module);
  KNO_LINK_CPRIM("adjunct-pool",adjunct_pool,2,kno_db_module);
  KNO_LINK_CPRIM("try-pool",try_pool,2,kno_db_module);
  KNO_LINK_CPRIM("set-cache-level!",set_cache_level,2,kno_db_module);
  KNO_LINK_CPRIM("set-pool-namefn!",set_pool_namefn,2,kno_db_module);
  KNO_LINK_CPRIM("getpool",getpool,1,kno_db_module);
  KNO_LINK_CPRIM("oid->pool",oid2pool,1,kno_db_module);
  KNO_LINK_CPRIM("index-frame",index_frame_prim,4,kno_db_module);
  KNO_LINK_CPRIMN("find-frames/prefetch!",find_frames_prefetch,kno_db_module);
  KNO_LINK_CPRIM("prefetch-slotvals!",prefetch_slotvals,3,kno_db_module);

  KNO_LINK_CPRIM("name->pool",name2pool,1,kno_db_module);
  KNO_LINK_CPRIM("source->pool",source2pool,1,kno_db_module);
  KNO_LINK_CPRIM("name->index",name2index,1,kno_db_module);
  KNO_LINK_CPRIM("source->index",source2index,1,kno_db_module);

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

  import_schemefn("get");
  import_schemefn("%get");
  import_schemefn("add!");
  import_schemefn("drop!");
  import_schemefn("store!");
  import_schemefn("%test");
  import_schemefn("getkeys");
  import_schemefn("getvalues");
  import_schemefn("getassocs");
  import_schemefn("getkeyvec");
  import_schemefn("get");
  import_schemefn("test");
  import_schemefn("assert!");
  import_schemefn("retract!");
  import_schemefn("getpath*");
  import_schemefn("getpath");
  import_schemefn("testp");
  import_schemefn("xfind-frames");
  import_schemefn("find-frames");
  import_schemefn("??");
}
