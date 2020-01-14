/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/apply.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8rusage.h>

#include <stdarg.h>
/* We include this for sscanf, but we're not really using stdio. */
#include <stdio.h>

u8_condition kno_NoStorageMetadata=_("No storage metadata for resource"),
  kno_AmbiguousObjectName=_("Ambiguous object name"),
  kno_UnknownObjectName=_("Unknown object name"),
  kno_BadServerResponse=_("bad server response"),
  kno_NoBackground=_("No default background indexes"),
  kno_UnallocatedOID=_("Reference to unallocated OID"),
  kno_HomelessOID=_("Reference to homeless OID");
u8_condition kno_ConnectionFailed=_("Connection to server failed");
u8_condition kno_Commitment=_("COMMIT");
u8_condition kno_ServerReconnect=_("Resetting server connection");
static u8_condition SwapCheck=_("SwapCheck");
u8_condition kno_BadMetaData=_("Error getting metadata");

int kno_default_cache_level = 1;
int kno_oid_display_level = 2;
int kno_storage_loglevel = LOG_NOTICE;
int *kno_storage_loglevel_ptr = &kno_storage_loglevel;
int kno_prefetch = KNO_PREFETCHING_ENABLED;
int kno_require_mmap = KNO_USE_MMAP;
int kno_norm_syms = 0;

size_t kno_network_bufsize = KNO_NETWORK_BUFSIZE;

int kno_dbconn_reserve_default = KNO_DBCONN_RESERVE_DEFAULT;
int kno_dbconn_cap_default = KNO_DBCONN_CAP_DEFAULT;
int kno_dbconn_init_default = KNO_DBCONN_INIT_DEFAULT;

lispval kno_commit_phases[8];

static lispval id_symbol, flags_symbol, background_symbol,
  readonly_symbol, repair_symbol, fixsyms_symbol,
  sparse_symbol, register_symbol, virtual_symbol, phased_symbol,
  oidcodes_symbol, slotcodes_symbol;

static lispval lookupfns = KNO_NIL;

static int knostorage_initialized = 0;

static int testopt(lispval opts,lispval sym,int dflt)
{
  if (!(KNO_TABLEP(opts)))
    return dflt;
  lispval val = kno_getopt(opts,sym,KNO_VOID);
  if ( (val == KNO_VOID) || (val == KNO_DEFAULT_VALUE) )
    return dflt;
  else if ( (val == KNO_FALSE) || (val == KNO_FIXZERO) )
    return 0;
  else {
    kno_decref(val);
    return 1;}
}

/* Getting storage flags */

KNO_EXPORT kno_storage_flags
kno_get_dbflags(lispval opts,kno_storage_flags init_flags)
{
  if (FIXNUMP(opts)) {
    long long val=FIX2INT(opts);
    if (val<0) return val;
    else if (val>0xFFFFFFFF)
      return -1;
    else return val;}
  else if (TABLEP(opts)) {
    lispval flags_val=kno_getopt(opts,flags_symbol,VOID);
    kno_storage_flags flags =
      ( (FIXNUMP(flags_val)) ? (FIX2INT(flags_val)) : (0) ) |
      (init_flags);
    int is_index = (flags&KNO_STORAGE_ISINDEX);
    if (testopt(opts,readonly_symbol,0))
      flags |= KNO_STORAGE_READ_ONLY;
    if (testopt(opts,phased_symbol,0))
      flags |= KNO_STORAGE_PHASED;
    if (!(testopt(opts,register_symbol,1)))
      flags |= KNO_STORAGE_UNREGISTERED;
    if (!(testopt(opts,virtual_symbol,1)))
      flags |= KNO_STORAGE_VIRTUAL;
    if (testopt(opts,repair_symbol,0))
      flags |= KNO_STORAGE_REPAIR;
    if (testopt(opts,fixsyms_symbol,0))
      flags |= KNO_STORAGE_LOUDSYMS;
    if ( (is_index) && (testopt(opts,background_symbol,0)) )
      flags |= KNO_INDEX_IN_BACKGROUND;
    if ( (!(is_index)) &&
         ( (testopt(opts,KNOSYM_ADJUNCT,0)) ||
           (testopt(opts,KNOSYM_ISADJUNCT,0)) ||
           (kno_testopt(opts,KNOSYM_FORMAT,KNOSYM_ISADJUNCT)) ||
           (kno_testopt(opts,KNOSYM_FORMAT,KNOSYM_ADJUNCT))) )
      flags |= KNO_POOL_ADJUNCT;
    if ( (!(is_index)) && (testopt(opts,sparse_symbol,0)) )
      flags |= KNO_POOL_SPARSE;
    kno_decref(flags_val);
    return flags;}
  else {
    if (FALSEP(opts)) {
      if (init_flags&KNO_STORAGE_ISPOOL)
        return (init_flags & (~(KNO_STORAGE_UNREGISTERED)));
      else return (init_flags | KNO_STORAGE_UNREGISTERED);}
    else return init_flags;}
}

static lispval better_parse_oid(u8_string start,int len)
{
  if (start[1]=='?') {
    const u8_byte *scan = start+2;
    lispval name = kno_parse(scan);
    if (scan-start>len) return VOID;
    else if (KNO_ABORTP(name)) return name;
    else if (kno_background) {
      lispval key = kno_conspair(id_symbol,kno_incref(name));
      lispval item = kno_index_get((kno_index)kno_background,key);
      kno_decref(key);
      if (OIDP(item)) return item;
      else if (CHOICEP(item)) {
        kno_decref(item);
        return kno_err(kno_AmbiguousObjectName,"better_parse_oid",NULL,name);}
      else if (!((EMPTYP(item))||(FALSEP(item)))) {
        kno_decref(item);
        return kno_type_error("oid","better_parse_oid",item);}}
    else {}
    if (lookupfns!=NIL) {
      KNO_DOLIST(method,lookupfns) {
        if ((SYMBOLP(method))||(OIDP(method))) {
          lispval key = kno_conspair(method,kno_incref(name));
          lispval item = kno_index_get((kno_index)kno_background,key);
          kno_decref(key);
          if (OIDP(item)) return item;
          else if (!((EMPTYP(item))||
                     (FALSEP(item))||
                     (VOIDP(item)))) continue;
          else return kno_type_error("oid","better_parse_oid",item);}
        else if (KNO_APPLICABLEP(method)) {
          lispval item = kno_apply(method,1,&name);
          if (KNO_ABORTP(item)) return item;
          else if (OIDP(item)) return item;
          else if ((EMPTYP(item))||
                   (FALSEP(item))||
                   (VOIDP(item))) continue;
          else return kno_type_error("oid","better_parse_oid",item);}
        else return kno_type_error("lookup method","better_parse_oid",method);}
      return kno_err(kno_UnknownObjectName,"better_parse_oid",NULL,name);}
    else {
      return kno_err(kno_UnknownObjectName,"better_parse_oid",NULL,name);}}
  else if (((strchr(start,'/')))&&
           (((u8_string)strchr(start,'/'))<(start+len))) {
    KNO_OID base = KNO_NULL_OID_INIT, result = KNO_NULL_OID_INIT;
    unsigned int delta;
    u8_byte prefix[64], suffix[64];
    const u8_byte *copy_start, *copy_end;
    copy_start = ((start[1]=='/') ? (start+2) : (start+1));
    copy_end = strchr(copy_start,'/');
    if (copy_end == NULL) return KNO_PARSE_ERROR;
    strncpy(prefix,copy_start,(copy_end-copy_start));
    prefix[(copy_end-copy_start)]='\0';
    if (start[1]=='/') {
      kno_pool p = kno_find_pool_by_prefix(prefix);
      if (p == NULL) return VOID;
      else base = p->pool_base;}
    else {
      unsigned int hi;
      if (sscanf(prefix,"%x",&hi)<1) return KNO_PARSE_ERROR;
      KNO_SET_OID_LO(base,0);
      KNO_SET_OID_HI(base,hi);}
    copy_start = copy_end+1; copy_end = start+len;
    strncpy(suffix,copy_start,(copy_end-copy_start));
    suffix[(copy_end-copy_start)]='\0';
    if (sscanf(suffix,"%x",&delta)<1)  return KNO_PARSE_ERROR;
    result = KNO_OID_PLUS(base,delta);
    return kno_make_oid(result);}
  else return kno_parse_oid_addr(start,len);
}

static lispval oid_name_slotids = NIL;

static lispval default_get_oid_name(kno_pool p,lispval oid)
{
  if (KNO_APPLICABLEP(p->pool_namefn))
    return VOID;
  else {
    kno_pool p = kno_oid2pool(oid);
    if (p == NULL)
      return VOID;
    lispval ov = kno_oid_value(oid);
    if (KNO_ABORTP(ov)) {
      u8_exception ex = u8_erreify();
      if (ex) u8_free_exception(ex,1);
      kno_decref(ov);
      return VOID;}
    if (! ((KNO_SLOTMAPP(ov)) || (KNO_SCHEMAPP(ov))) ) {
      kno_decref(ov);
      return VOID;}
    kno_decref(ov);
    if ((OIDP(p->pool_namefn)) || (SYMBOLP(p->pool_namefn))) {
      lispval probe = kno_frame_get(oid,p->pool_namefn);
      if (EMPTYP(probe)) {}
      else if (KNO_ABORTP(probe)) {kno_decref(probe);}
      else return probe;}
    KNO_DOLIST(slotid,oid_name_slotids) {
      lispval probe = kno_frame_get(oid,slotid);
      if (EMPTYP(probe)) {}
      else if (KNO_ABORTP(probe)) {
        u8_exception ex = u8_erreify();
        if (ex) u8_free_exception(ex,1);
        kno_decref(probe);
        return VOID;}
      else return probe;}
    return VOID;}
}

lispval (*kno_get_oid_name)(kno_pool p,lispval oid) = default_get_oid_name;

static int print_oid_name(u8_output out,lispval name,int top)
{
  if ((VOIDP(name)) || (EMPTYP(name))) return 0;
  else if (NILP(name))
    return u8_puts(out,"()");
  else if (OIDP(name)) {
    KNO_OID addr = KNO_OID_ADDR(name);
    unsigned int hi = KNO_OID_HI(addr), lo = KNO_OID_LO(addr);
    return u8_printf(out,"@%x/%x",hi,lo);}
  else if ((SYMBOLP(name)) ||
           (NUMBERP(name)) ||
           (KNO_CONSTANTP(name)))
    if (top) {
      int retval = -1;
      u8_puts(out,"{"); retval = kno_unparse(out,name);
      if (retval<0) return retval;
      else retval = u8_puts(out,"}");
      return retval;}
    else return kno_unparse(out,name);
  else if (STRINGP(name))
    return kno_unparse(out,name);
  else if ((CHOICEP(name)) || (PRECHOICEP(name))) {
    int i = 0; u8_putc(out,'{'); {
      DO_CHOICES(item,name) {
        if (i++>0) u8_putc(out,' ');
        if (print_oid_name(out,item,0)<0) return -1;}}
    return u8_putc(out,'}');}
  else if (PAIRP(name)) {
    lispval scan = name; u8_putc(out,'(');
    if (print_oid_name(out,KNO_CAR(scan),0)<0) return -1;
    else scan = KNO_CDR(scan);
    while (PAIRP(scan)) {
      u8_putc(out,' ');
      if (print_oid_name(out,KNO_CAR(scan),0)<0) return -1;
      scan = KNO_CDR(scan);}
    if (NILP(scan))
      return u8_putc(out,')');
    else {
      u8_puts(out," . ");
      print_oid_name(out,scan,0);
      return u8_putc(out,')');}}
  else if (VECTORP(name)) {
    int i = 0, len = VEC_LEN(name);
    u8_puts(out,"#(");
    while (i< len) {
      if (i>0) u8_putc(out,' ');
      if (print_oid_name(out,VEC_REF(name,i),0)<0)
        return -1;
      i++;}
    return u8_puts(out,")");}
  else if (top) return 0;
  else return kno_unparse(out,name);
}

static int better_unparse_oid(u8_output out,lispval x)
{
  if ( (kno_oid_display_level<1) || (out->u8_streaminfo&U8_STREAM_TACITURN) ) {
    KNO_OID addr = KNO_OID_ADDR(x); char buf[128];
    unsigned int hi = KNO_OID_HI(addr), lo = KNO_OID_LO(addr);
    sprintf(buf,"@%x/%x",hi,lo);
    u8_puts(out,buf);
    return 1;}
  else {
    kno_pool p = kno_oid2pool(x);
    KNO_OID addr = KNO_OID_ADDR(x); char buf[128];
    unsigned int hi = KNO_OID_HI(addr), lo = KNO_OID_LO(addr);
    if ((p == NULL) || (p->pool_prefix == NULL) ||
        (kno_numeric_oids) || (hi == 0) )
      sprintf(buf,"@%x/%x",hi,lo);
    else {
      unsigned int off = KNO_OID_DIFFERENCE(addr,p->pool_base);
      sprintf(buf,"@/%s/%x",p->pool_prefix,off);}
    u8_puts(out,buf);
    if (p == NULL)
      return 1;
    else if (kno_oid_display_level<2)
      return 1;
    else if ((hi>0) && (kno_oid_display_level<3) ) {
	lispval v = kno_hashtable_get(&(p->pool_cache),x,KNO_VOID);
	if ( (v == KNO_VOID) || (v == KNO_UNALLOCATED_OID) ) {
	  v = kno_hashtable_get(&(p->pool_changes),x,KNO_VOID);
	  if ( (v == KNO_VOID) || (v == KNO_UNALLOCATED_OID) )
	    return 1;
	  else kno_decref(v);}
	else kno_decref(v);}
    lispval name = kno_get_oid_name(p,x);
    int retval = print_oid_name(out,name,1);
    kno_decref(name);
    return retval;}
}

/* CONFIG settings */

static int set_default_cache_level(lispval var,lispval val,void *data)
{
  if (KNO_INTP(val)) {
    int new_level = FIX2INT(val);
    kno_default_cache_level = new_level;
    return 1;}
  else {
    kno_type_error("small fixnum","set_default_cache_level",val);
    return -1;}
}
static lispval get_default_cache_level(lispval var,void *data)
{
  return KNO_INT(kno_default_cache_level);
}

static int set_oid_display_level(lispval var,lispval val,void *data)
{
  if (KNO_INTP(val)) {
    kno_oid_display_level = FIX2INT(val);
    return 1;}
  else {
    kno_type_error("small fixnum","set_oid_display_level",val);
    return -1;}
}
static lispval get_oid_display_level(lispval var,void *data)
{
  return KNO_INT(kno_oid_display_level);
}

static int set_prefetch(lispval var,lispval val,void *data)
{
  if (FALSEP(val)) kno_prefetch = 0;
  else kno_prefetch = 1;
  return 1;
}
static lispval get_prefetch(lispval var,void *data)
{
  if (kno_prefetch) return KNO_TRUE; else return KNO_FALSE;
}

static lispval config_get_pools(lispval var,void *data)
{
  return kno_all_pools();
}
static int config_use_pool(lispval var,lispval spec,void *data)
{
  if (STRINGP(spec))
    if (kno_use_pool(CSTRING(spec),0,VOID))
      return 1;
    else return -1;
  else return kno_reterr(kno_TypeError,"config_use_pool",
                        u8_strdup(_("pool spec")),VOID);
}

/* Config methods */

static lispval config_get_indexes(lispval var,void *data)
{
  return kno_get_all_indexes();
}
static int config_open_index(lispval var,lispval spec,void *data)
{
  if (STRINGP(spec))
    if (kno_get_index(CSTRING(spec),0,VOID)) return 1;
    else return -1;
  else {
    kno_seterr(kno_TypeError,"config_open_index",NULL,kno_incref(spec));
    return -1;}
}

static lispval config_get_background(lispval var,void *data)
{
  lispval results = EMPTY;
  if (kno_background == NULL) return results;
  else {
    int i = 0, n; kno_index *indexes;
    u8_lock_mutex(&(kno_background->index_lock));
    n = kno_background->ax_n_indexes;
    indexes = kno_background->ax_indexes;
    while (i<n) {
      lispval lix = kno_index_ref(indexes[i]); i++;
      CHOICE_ADD(results,lix);}
    u8_unlock_mutex(&(kno_background->index_lock));
    return results;}
}
static int config_use_index(lispval var,lispval spec,void *data)
{
  if (STRINGP(spec))
    if (kno_use_index(CSTRING(spec),0,VOID)) return 1;
    else return -1;
  else if (KNO_INDEXP(spec))
    if (kno_add_to_background(kno_indexptr(spec))) return 1;
    else return -1;

  else {
    kno_seterr(kno_TypeError,"config_use_index",NULL,kno_incref(spec));
    return -1;}
}

/* Global pool/index functions */

KNO_EXPORT int kno_commit_all()
{
  /* Not really ACID, but probably better than doing pools first. */
  if (kno_commit_indexes()<0) return -1;
  else return kno_commit_pools();
}

KNO_EXPORT void kno_swapout_all()
{
  kno_swapout_indexes();
  kno_swapout_pools();
}

/* Fast swap outs. */

/* This swaps stuff out while trying to hold locks for the minimal possible
   time.  It does this by using kno_fast_reset_hashtable() and actually freeing
   the storage outside of the lock.  This is good for swapping out on
   multi-threaded servers. */

/* This stores a hashtable's data to free after it has been reset. */
struct HASHVEC_TO_FREE {
  struct KNO_HASH_BUCKET **slots;
  int n_slots, big_buckets;};
struct HASHVECS_TODO {
  struct HASHVEC_TO_FREE *to_free;
  int n_to_free, max_to_free, big_buckets;};

static void fast_reset_hashtable
(kno_hashtable h,int n_slots_arg,
 struct HASHVECS_TODO *todo)
{
  int i = todo->n_to_free;
  if (todo->n_to_free == todo->max_to_free) {
    todo->to_free = u8_realloc_n
      (todo->to_free,(todo->max_to_free)*2,struct HASHVEC_TO_FREE);
    todo->max_to_free = (todo->max_to_free)*2;}
  kno_fast_reset_hashtable(h,n_slots_arg,1,
                          &(todo->to_free[i].slots),
                          &(todo->to_free[i].n_slots),
                          &(todo->to_free[i].big_buckets));
  todo->n_to_free = i+1;
}

static int fast_swapout_index(kno_index ix,void *data)
{
  struct HASHVECS_TODO *todo = (struct HASHVECS_TODO *)data;
  if ((!((ix->index_flags)&(KNO_STORAGE_NOSWAP))) &&
      (ix->index_cache.table_n_keys)) {
    if ((ix->index_flags)&(KNO_STORAGE_KEEP_CACHESIZE))
      fast_reset_hashtable(&(ix->index_cache),-1,todo);
    else fast_reset_hashtable(&(ix->index_cache),0,todo);}
  return 0;
}

static int fast_swapout_pool(kno_pool p,void *data)
{
  struct HASHVECS_TODO *todo = (struct HASHVECS_TODO *)data;
  fast_reset_hashtable(&(p->pool_cache),67,todo);
  if (p->pool_changes.table_n_keys)
    kno_devoid_hashtable(&(p->pool_changes),0);
  return 0;
}

KNO_EXPORT void kno_fast_swapout_all()
{
  int i = 0;
  struct HASHVECS_TODO todo;
  todo.to_free = u8_alloc_n(128,struct HASHVEC_TO_FREE);
  todo.n_to_free = 0; todo.max_to_free = 128;
  kno_for_indexes(fast_swapout_index,(void *)&todo);
  kno_for_pools(fast_swapout_pool,(void *)&todo);
  while (i<todo.n_to_free) {
    kno_free_buckets(todo.to_free[i].slots,
                    todo.to_free[i].n_slots,
                    todo.to_free[i].big_buckets);
    i++;}
  u8_free(todo.to_free);
}

/* kno_save */

static u8_mutex dosave_lock;
static u8_mutex onsave_handlers_lock;

static struct KNO_ONSAVE {
  lispval onsave_handler;
  struct KNO_ONSAVE *onsave_next;} *onsave_handlers=NULL;
static int n_onsave_handlers=0;

KNO_EXPORT int kno_save(lispval arg)
{
  u8_lock_mutex(&dosave_lock);
  struct KNO_ONSAVE *scan=onsave_handlers;
  while (scan) {
    lispval handler=scan->onsave_handler, result=KNO_VOID;
    if ((KNO_FUNCTIONP(handler))&&(KNO_FUNCTION_ARITY(handler)))
      result = kno_apply(handler,1,&arg);
    else result = kno_apply(handler,0,NULL);
    if (KNO_ABORTP(result)) {
      u8_unlock_mutex(&dosave_lock);
      kno_decref(result);
      return -1;}
    else scan=scan->onsave_next;}
  int rv = kno_commit_all();
  u8_unlock_mutex(&dosave_lock);
  return rv;
}

static lispval config_onsave_get(lispval var,void *data)
{
  struct KNO_ONSAVE *scan; int i = 0; lispval result;
  u8_lock_mutex(&onsave_handlers_lock);
  result = kno_make_vector(n_onsave_handlers,NULL);
  scan = onsave_handlers; while (scan) {
    lispval handler = scan->onsave_handler; kno_incref(handler);
    KNO_VECTOR_SET(result,i,handler);
    scan = scan->onsave_next; i++;}
  u8_unlock_mutex(&onsave_handlers_lock);
  return result;
}

static int config_onsave_set(lispval var,lispval val,void *data)
{
  struct KNO_ONSAVE *fresh = u8_malloc(sizeof(struct KNO_ONSAVE));
  if (!(KNO_APPLICABLEP(val))) {
    kno_type_error("applicable","config_onsave",val);
    return -1;}
  u8_lock_mutex(&onsave_handlers_lock);
  fresh->onsave_next = onsave_handlers;
  fresh->onsave_handler = val;
  kno_incref(val);
  n_onsave_handlers++; onsave_handlers = fresh;
  u8_unlock_mutex(&onsave_handlers_lock);
  return 1;
}

/* Swap out to reduce memory footprint */

static size_t membase = 0;

u8_mutex kno_swapcheck_lock;

KNO_EXPORT int kno_swapcheck()
{
  long long memgap; ssize_t usage = u8_memusage();
  lispval l_memgap = kno_config_get("SWAPCHECK");
  if (FIXNUMP(l_memgap)) memgap = FIX2INT(l_memgap);
  else if (!(VOIDP(l_memgap))) {
    u8_logf(LOG_WARN,kno_TypeError,"Bad SWAPCHECK config: %q",l_memgap);
    kno_decref(l_memgap);
    return -1;}
  else return 0;
  if (usage<(membase+memgap)) return 0;
  u8_lock_mutex(&kno_swapcheck_lock);
  if (usage>(membase+memgap)) {
    u8_logf(LOG_NOTICE,SwapCheck,"Swapping because %ld>%ld+%ld",
            usage,membase,memgap);
    kno_clear_slotcaches();
    kno_clear_callcache(VOID);
    kno_swapout_all();
    membase = u8_memusage();
    u8_logf(LOG_NOTICE,SwapCheck,
            "Swapped out, next swap at=%ld, swap at %d=%d+%d",
            membase+memgap,membase,memgap);
    u8_unlock_mutex(&kno_swapcheck_lock);}
  else {
    u8_unlock_mutex(&kno_swapcheck_lock);}
  return 1;
}

/* Init stuff */

static void register_header_files()
{
  u8_register_source_file(KNO_STORAGE_H_INFO);
  u8_register_source_file(KNO_POOLS_H_INFO);
  u8_register_source_file(KNO_INDEXES_H_INFO);
  u8_register_source_file(KNO_DRIVERS_H_INFO);
}

KNO_EXPORT void kno_init_streams_c(void);
KNO_EXPORT void kno_init_alcor_c(void);
KNO_EXPORT void kno_init_hashdtype_c(void);
KNO_EXPORT void kno_init_threadcache_c(void);
KNO_EXPORT void kno_init_pools_c(void);
KNO_EXPORT void kno_init_compress_c(void);
KNO_EXPORT void kno_init_indexes_c(void);
KNO_EXPORT void kno_init_dtcall_c(void);
KNO_EXPORT void kno_init_oidobj_c(void);
KNO_EXPORT void kno_init_dtproc_c(void);
KNO_EXPORT void kno_init_frames_c(void);
KNO_EXPORT void kno_init_cachecall_c(void);
KNO_EXPORT void kno_init_ipeval_c(void);
KNO_EXPORT void kno_init_methods_c(void);
KNO_EXPORT int kno_init_drivers_c(void);
KNO_EXPORT void kno_init_bloom_c(void);

KNO_EXPORT int kno_init_storage()
{
  if (knostorage_initialized) return knostorage_initialized;
  knostorage_initialized = 211*kno_init_lisp_types();

  register_header_files();
  u8_register_source_file(_FILEINFO);

  kno_init_threadcache_c();
  kno_init_streams_c();
  kno_init_alcor_c();
  kno_init_hashdtype_c();
  kno_init_oidobj_c();
  kno_init_cachecall_c();
  kno_init_compress_c();
  kno_init_bloom_c();
  kno_init_pools_c();
  kno_init_indexes_c();
  kno_init_frames_c();
  kno_init_drivers_c();
  kno_init_dtcall_c();
  kno_init_dtproc_c();
#if KNO_IPEVAL_ENABLED
  kno_init_ipeval_c();
#endif
  kno_init_methods_c();

  id_symbol = kno_intern("%id");
  flags_symbol = kno_intern("flags");
  background_symbol = kno_intern("background");
  readonly_symbol = kno_intern("readonly");
  repair_symbol = kno_intern("repair");
  sparse_symbol = kno_intern("sparse");
  register_symbol = kno_intern("register");
  phased_symbol = kno_intern("phased");
  slotcodes_symbol = kno_intern("slotcodes");
  oidcodes_symbol = kno_intern("oidcodes");
  virtual_symbol = kno_intern("virtual");
  fixsyms_symbol = kno_intern("fixsyms");

  kno_set_oid_parser(better_parse_oid);
  kno_unparsers[kno_oid_type]=better_unparse_oid;
  oid_name_slotids = kno_make_list(2,kno_intern("%id"),kno_intern("obj-name"));

  u8_init_mutex(&kno_swapcheck_lock);
  u8_init_mutex(&onsave_handlers_lock);
  u8_init_mutex(&dosave_lock);

  kno_commit_phases[0] = kno_intern("none");
  kno_commit_phases[1] = kno_intern("start");
  kno_commit_phases[2] = kno_intern("write");
  kno_commit_phases[3] = kno_intern("sync");
  kno_commit_phases[4] = kno_intern("rollback");
  kno_commit_phases[5] = kno_intern("flush");
  kno_commit_phases[6] = kno_intern("cleanup");
  kno_commit_phases[7] = kno_intern("done");

  u8_threadcheck();

  kno_register_config
    ("CACHELEVEL",_("Sets a level of time/memory tradeoff [0-3], default 1"),
     get_default_cache_level,
     set_default_cache_level,
     NULL);
  kno_register_config
    ("OIDDISPLAY",_("Default oid display level [0-3]"),
     get_oid_display_level,
     set_oid_display_level,
     NULL);
  kno_register_config
    ("DBLOGLEVEL",_("Default log level for database messages"),
     kno_intconfig_get,kno_loglevelconfig_set,&kno_storage_loglevel);

  kno_register_config
    ("PREFETCH",_("Whether to prefetch for large operations"),
     get_prefetch,
     set_prefetch,
     NULL);
  kno_register_config
    ("POOLS",_("pools used for OID resolution"),
     config_get_pools,config_use_pool,NULL);
  kno_register_config
    ("INDEXES",_("indexes opened"),
     config_get_indexes,config_open_index,NULL);
  kno_register_config
    ("BACKGROUND",_("indexes in the default search background"),
     config_get_background,config_use_index,NULL);

  kno_register_config
    ("DBCONNRESERVE",
     _("Number of connections to keep for each DB server"),
     kno_intconfig_get,kno_intconfig_set,&kno_dbconn_reserve_default);
  kno_register_config
    ("DBCONNCAP",_("Max number of connections (default) for each DB server"),
     kno_intconfig_get,kno_intconfig_set,&kno_dbconn_cap_default);
  kno_register_config
    ("DBCONNINIT",
     _("Number of connections to initially create for each DB server"),
     kno_intconfig_get,kno_intconfig_set,&kno_dbconn_init_default);
  kno_register_config
    ("LOOKUPOID",
     _("Functions and slotids for lookup up objects by name (@?name)"),
     kno_lconfig_get,kno_lconfig_push,&lookupfns);
  kno_register_config
    ("ONSAVE",_("Functions to run when saving databases"),
     config_onsave_get,
     config_onsave_set,
     NULL);
  kno_register_config
    ("STORAGE:LOUDSYMS",_("Whether to convert legacy uppercase to lowercase"),
     kno_boolconfig_get,
     kno_boolconfig_set,
     &kno_norm_syms);


  return knostorage_initialized;
}

