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
#include "framerd/storage.h"
#include "framerd/apply.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8rusage.h>

#include <stdarg.h>
/* We include this for sscanf, but we're not really using stdio. */
#include <stdio.h>

u8_condition fd_NoStorageMetadata=_("No storage metadata for resource"),
  fd_AmbiguousObjectName=_("Ambiguous object name"),
  fd_UnknownObjectName=_("Unknown object name"),
  fd_BadServerResponse=_("bad server response"),
  fd_NoBackground=_("No default background indexes"),
  fd_UnallocatedOID=_("Reference to unallocated OID"),
  fd_HomelessOID=_("Reference to homeless OID");
u8_condition fd_ConnectionFailed=_("Connection to server failed");
u8_condition fd_Commitment=_("COMMIT");
u8_condition fd_ServerReconnect=_("Resetting server connection");
static u8_condition SwapCheck=_("SwapCheck");
u8_condition fd_BadMetaData=_("Error getting metadata");

int fd_default_cache_level = 1;
int fd_oid_display_level = 2;
int fd_storage_loglevel = LOG_NOTICE;
int *fd_storage_loglevel_ptr = &fd_storage_loglevel;
int fd_prefetch = FD_PREFETCHING_ENABLED;
int fd_require_mmap = FD_USE_MMAP;

size_t fd_network_bufsize = FD_NETWORK_BUFSIZE;

int fd_dbconn_reserve_default = FD_DBCONN_RESERVE_DEFAULT;
int fd_dbconn_cap_default = FD_DBCONN_CAP_DEFAULT;
int fd_dbconn_init_default = FD_DBCONN_INIT_DEFAULT;

lispval fd_commit_phases[8];

static lispval id_symbol, flags_symbol, background_symbol,
  readonly_symbol, repair_symbol, adjunct_symbol,
  sparse_symbol, register_symbol, virtual_symbol, phased_symbol,
  oidcodes_symbol, slotcodes_symbol;

static lispval lookupfns = FD_NIL;

static int fdstorage_initialized = 0;

static int testopt(lispval opts,lispval sym,int dflt)
{
  if (!(FD_TABLEP(opts)))
    return dflt;
  lispval val = fd_getopt(opts,sym,FD_VOID);
  if ( (val == FD_VOID) || (val == FD_DEFAULT_VALUE) )
    return dflt;
  else if ( (val == FD_FALSE) || (val == FD_FIXZERO) )
    return 0;
  else {
    fd_decref(val);
    return 1;}
}

/* Getting storage flags */

FD_EXPORT fd_storage_flags
fd_get_dbflags(lispval opts,fd_storage_flags init_flags)
{
  if (FIXNUMP(opts)) {
    long long val=FIX2INT(opts);
    if (val<0) return val;
    else if (val>0xFFFFFFFF)
      return -1;
    else return val;}
  else if (TABLEP(opts)) {
    lispval flags_val=fd_getopt(opts,flags_symbol,VOID);
    fd_storage_flags flags =
      ( (FIXNUMP(flags_val)) ? (FIX2INT(flags_val)) : (0) ) |
      (init_flags);
    int is_index = (flags&FD_STORAGE_ISINDEX);
    if (testopt(opts,readonly_symbol,0))
      flags |= FD_STORAGE_READ_ONLY;
    if (testopt(opts,phased_symbol,0))
      flags |= FD_STORAGE_PHASED;
    if (!(testopt(opts,register_symbol,1)))
      flags |= FD_STORAGE_UNREGISTERED;
    if (!(testopt(opts,virtual_symbol,1)))
      flags |= FD_STORAGE_VIRTUAL;
    if (testopt(opts,repair_symbol,0))
      flags |= FD_STORAGE_REPAIR;
    if ( (is_index) && (testopt(opts,background_symbol,0)) )
      flags |= FD_INDEX_IN_BACKGROUND;
    if ( (!(is_index)) && (testopt(opts,adjunct_symbol,0)) )
      flags |= FD_POOL_ADJUNCT | FD_POOL_SPARSE;
    if ( (!(is_index)) && (testopt(opts,sparse_symbol,0)) )
      flags |= FD_POOL_SPARSE;
    fd_decref(flags_val);
    return flags;}
  else if (FALSEP(opts))
    if (init_flags&FD_STORAGE_ISPOOL)
      return (init_flags & (~(FD_STORAGE_UNREGISTERED)));
    else return (init_flags | FD_STORAGE_UNREGISTERED);
  else return init_flags;
}

static lispval better_parse_oid(u8_string start,int len)
{
  if (start[1]=='?') {
    const u8_byte *scan = start+2;
    lispval name = fd_parse(scan);
    if (scan-start>len) return VOID;
    else if (FD_ABORTP(name)) return name;
    else if (fd_background) {
      lispval key = fd_conspair(id_symbol,fd_incref(name));
      lispval item = fd_index_get((fd_index)fd_background,key);
      fd_decref(key);
      if (OIDP(item)) return item;
      else if (CHOICEP(item)) {
        fd_decref(item);
        return fd_err(fd_AmbiguousObjectName,"better_parse_oid",NULL,name);}
      else if (!((EMPTYP(item))||(FALSEP(item)))) {
        fd_decref(item);
        return fd_type_error("oid","better_parse_oid",item);}}
    else {}
    if (lookupfns!=NIL) {
      FD_DOLIST(method,lookupfns) {
        if ((SYMBOLP(method))||(OIDP(method))) {
          lispval key = fd_conspair(method,fd_incref(name));
          lispval item = fd_index_get((fd_index)fd_background,key);
          fd_decref(key);
          if (OIDP(item)) return item;
          else if (!((EMPTYP(item))||
                     (FALSEP(item))||
                     (VOIDP(item)))) continue;
          else return fd_type_error("oid","better_parse_oid",item);}
        else if (FD_APPLICABLEP(method)) {
          lispval item = fd_apply(method,1,&name);
          if (FD_ABORTP(item)) return item;
          else if (OIDP(item)) return item;
          else if ((EMPTYP(item))||
                   (FALSEP(item))||
                   (VOIDP(item))) continue;
          else return fd_type_error("oid","better_parse_oid",item);}
        else return fd_type_error("lookup method","better_parse_oid",method);}
      return fd_err(fd_UnknownObjectName,"better_parse_oid",NULL,name);}
    else {
      return fd_err(fd_UnknownObjectName,"better_parse_oid",NULL,name);}}
  else if (((strchr(start,'/')))&&
           (((u8_string)strchr(start,'/'))<(start+len))) {
    FD_OID base = FD_NULL_OID_INIT, result = FD_NULL_OID_INIT;
    unsigned int delta;
    u8_byte prefix[64], suffix[64];
    const u8_byte *copy_start, *copy_end;
    copy_start = ((start[1]=='/') ? (start+2) : (start+1));
    copy_end = strchr(copy_start,'/');
    if (copy_end == NULL) return FD_PARSE_ERROR;
    strncpy(prefix,copy_start,(copy_end-copy_start));
    prefix[(copy_end-copy_start)]='\0';
    if (start[1]=='/') {
      fd_pool p = fd_find_pool_by_prefix(prefix);
      if (p == NULL) return VOID;
      else base = p->pool_base;}
    else {
      unsigned int hi;
      if (sscanf(prefix,"%x",&hi)<1) return FD_PARSE_ERROR;
      FD_SET_OID_LO(base,0);
      FD_SET_OID_HI(base,hi);}
    copy_start = copy_end+1; copy_end = start+len;
    strncpy(suffix,copy_start,(copy_end-copy_start));
    suffix[(copy_end-copy_start)]='\0';
    if (sscanf(suffix,"%x",&delta)<1)  return FD_PARSE_ERROR;
    result = FD_OID_PLUS(base,delta);
    return fd_make_oid(result);}
  else return fd_parse_oid_addr(start,len);
}

static lispval oid_name_slotids = NIL;

static lispval default_get_oid_name(fd_pool p,lispval oid)
{
  if (FD_APPLICABLEP(p->pool_namefn))
    return VOID;
  else {
    fd_pool p = fd_oid2pool(oid);
    if (p == NULL)
      return VOID;
    lispval ov = fd_oid_value(oid);
    if (FD_ABORTP(ov)) {
      u8_exception ex = u8_erreify();
      if (ex) u8_free_exception(ex,1);
      fd_decref(ov);
      return VOID;}
    if (! ((FD_SLOTMAPP(ov)) || (FD_SCHEMAPP(ov))) ) {
      fd_decref(ov);
      return VOID;}
    fd_decref(ov);
    if ((OIDP(p->pool_namefn)) || (SYMBOLP(p->pool_namefn))) {
      lispval probe = fd_frame_get(oid,p->pool_namefn);
      if (EMPTYP(probe)) {}
      else if (FD_ABORTP(probe)) {fd_decref(probe);}
      else return probe;}
    FD_DOLIST(slotid,oid_name_slotids) {
      lispval probe = fd_frame_get(oid,slotid);
      if (EMPTYP(probe)) {}
      else if (FD_ABORTP(probe)) {
        u8_exception ex = u8_erreify();
        if (ex) u8_free_exception(ex,1);
        fd_decref(probe);
        return VOID;}
      else return probe;}
    return VOID;}
}

lispval (*fd_get_oid_name)(fd_pool p,lispval oid) = default_get_oid_name;

static int print_oid_name(u8_output out,lispval name,int top)
{
  if ((VOIDP(name)) || (EMPTYP(name))) return 0;
  else if (NILP(name))
    return u8_puts(out,"()");
  else if (OIDP(name)) {
    FD_OID addr = FD_OID_ADDR(name);
    unsigned int hi = FD_OID_HI(addr), lo = FD_OID_LO(addr);
    return u8_printf(out,"@%x/%x",hi,lo);}
  else if ((SYMBOLP(name)) ||
           (NUMBERP(name)) ||
           (FD_CONSTANTP(name)))
    if (top) {
      int retval = -1;
      u8_puts(out,"{"); retval = fd_unparse(out,name);
      if (retval<0) return retval;
      else retval = u8_puts(out,"}");
      return retval;}
    else return fd_unparse(out,name);
  else if (STRINGP(name))
    return fd_unparse(out,name);
  else if ((CHOICEP(name)) || (PRECHOICEP(name))) {
    int i = 0; u8_putc(out,'{'); {
      DO_CHOICES(item,name) {
        if (i++>0) u8_putc(out,' ');
        if (print_oid_name(out,item,0)<0) return -1;}}
    return u8_putc(out,'}');}
  else if (PAIRP(name)) {
    lispval scan = name; u8_putc(out,'(');
    if (print_oid_name(out,FD_CAR(scan),0)<0) return -1;
    else scan = FD_CDR(scan);
    while (PAIRP(scan)) {
      u8_putc(out,' ');
      if (print_oid_name(out,FD_CAR(scan),0)<0) return -1;
      scan = FD_CDR(scan);}
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
  else return fd_unparse(out,name);
}

static int better_unparse_oid(u8_output out,lispval x)
{
  if ( (fd_oid_display_level<1) || (out->u8_streaminfo&U8_STREAM_TACITURN) ) {
    FD_OID addr = FD_OID_ADDR(x); char buf[128];
    unsigned int hi = FD_OID_HI(addr), lo = FD_OID_LO(addr);
    sprintf(buf,"@%x/%x",hi,lo);
    u8_puts(out,buf);
    return 1;}
  else {
    fd_pool p = fd_oid2pool(x);
    FD_OID addr = FD_OID_ADDR(x); char buf[128];
    unsigned int hi = FD_OID_HI(addr), lo = FD_OID_LO(addr);
    if ((p == NULL) || (p->pool_prefix == NULL) ||
        (fd_numeric_oids) || (hi == 0) )
      sprintf(buf,"@%x/%x",hi,lo);
    else {
      unsigned int off = FD_OID_DIFFERENCE(addr,p->pool_base);
      sprintf(buf,"@/%s/%x",p->pool_prefix,off);}
    u8_puts(out,buf);
    if (p == NULL)
      return 1;
    else if (fd_oid_display_level<2)
      return 1;
    else if ((hi>0) && (fd_oid_display_level<3) &&
             (!(fd_hashtable_probe_novoid(&(p->pool_cache),x))) &&
             (!(fd_hashtable_probe_novoid(&(p->pool_changes),x))))
      return 1;
    else {
      lispval name = fd_get_oid_name(p,x);
      int retval = print_oid_name(out,name,1);
      fd_decref(name);
      return retval;}}
}

/* CONFIG settings */

static int set_default_cache_level(lispval var,lispval val,void *data)
{
  if (FD_INTP(val)) {
    int new_level = FIX2INT(val);
    fd_default_cache_level = new_level;
    return 1;}
  else {
    fd_type_error("small fixnum","set_default_cache_level",val);
    return -1;}
}
static lispval get_default_cache_level(lispval var,void *data)
{
  return FD_INT(fd_default_cache_level);
}

static int set_oid_display_level(lispval var,lispval val,void *data)
{
  if (FD_INTP(val)) {
    fd_oid_display_level = FIX2INT(val);
    return 1;}
  else {
    fd_type_error("small fixnum","set_oid_display_level",val);
    return -1;}
}
static lispval get_oid_display_level(lispval var,void *data)
{
  return FD_INT(fd_oid_display_level);
}

static int set_prefetch(lispval var,lispval val,void *data)
{
  if (FALSEP(val)) fd_prefetch = 0;
  else fd_prefetch = 1;
  return 1;
}
static lispval get_prefetch(lispval var,void *data)
{
  if (fd_prefetch) return FD_TRUE; else return FD_FALSE;
}

static lispval config_get_pools(lispval var,void *data)
{
  return fd_all_pools();
}
static int config_use_pool(lispval var,lispval spec,void *data)
{
  if (STRINGP(spec))
    if (fd_use_pool(CSTRING(spec),0,VOID))
      return 1;
    else return -1;
  else return fd_reterr(fd_TypeError,"config_use_pool",
                        u8_strdup(_("pool spec")),VOID);
}

/* Config methods */

static lispval config_get_indexes(lispval var,void *data)
{
  return fd_get_all_indexes();
}
static int config_open_index(lispval var,lispval spec,void *data)
{
  if (STRINGP(spec))
    if (fd_get_index(CSTRING(spec),0,VOID)) return 1;
    else return -1;
  else {
    fd_seterr(fd_TypeError,"config_open_index",NULL,fd_incref(spec));
    return -1;}
}

static lispval config_get_background(lispval var,void *data)
{
  lispval results = EMPTY;
  if (fd_background == NULL) return results;
  else {
    int i = 0, n; fd_index *indexes;
    u8_lock_mutex(&(fd_background->index_lock));
    n = fd_background->ax_n_indexes;
    indexes = fd_background->ax_indexes;
    while (i<n) {
      lispval lix = fd_index_ref(indexes[i]); i++;
      CHOICE_ADD(results,lix);}
    u8_unlock_mutex(&(fd_background->index_lock));
    return results;}
}
static int config_use_index(lispval var,lispval spec,void *data)
{
  if (STRINGP(spec))
    if (fd_use_index(CSTRING(spec),0,VOID)) return 1;
    else return -1;
  else if (FD_INDEXP(spec))
    if (fd_add_to_background(fd_indexptr(spec))) return 1;
    else return -1;

  else {
    fd_seterr(fd_TypeError,"config_use_index",NULL,fd_incref(spec));
    return -1;}
}

/* Global pool/index functions */

FD_EXPORT int fd_commit_all()
{
  /* Not really ACID, but probably better than doing pools first. */
  if (fd_commit_indexes()<0) return -1;
  else return fd_commit_pools();
}

FD_EXPORT void fd_swapout_all()
{
  fd_swapout_indexes();
  fd_swapout_pools();
}

/* Fast swap outs. */

/* This swaps stuff out while trying to hold locks for the minimal possible
   time.  It does this by using fd_fast_reset_hashtable() and actually freeing
   the storage outside of the lock.  This is good for swapping out on
   multi-threaded servers. */

/* This stores a hashtable's data to free after it has been reset. */
struct HASHVEC_TO_FREE {
  struct FD_HASH_BUCKET **slots;
  int n_slots, big_buckets;};
struct HASHVECS_TODO {
  struct HASHVEC_TO_FREE *to_free;
  int n_to_free, max_to_free, big_buckets;};

static void fast_reset_hashtable
(fd_hashtable h,int n_slots_arg,
 struct HASHVECS_TODO *todo)
{
  int i = todo->n_to_free;
  if (todo->n_to_free == todo->max_to_free) {
    todo->to_free = u8_realloc_n
      (todo->to_free,(todo->max_to_free)*2,struct HASHVEC_TO_FREE);
    todo->max_to_free = (todo->max_to_free)*2;}
  fd_fast_reset_hashtable(h,n_slots_arg,1,
                          &(todo->to_free[i].slots),
                          &(todo->to_free[i].n_slots),
                          &(todo->to_free[i].big_buckets));
  todo->n_to_free = i+1;
}

static int fast_swapout_index(fd_index ix,void *data)
{
  struct HASHVECS_TODO *todo = (struct HASHVECS_TODO *)data;
  if ((!((ix->index_flags)&(FD_STORAGE_NOSWAP))) &&
      (ix->index_cache.table_n_keys)) {
    if ((ix->index_flags)&(FD_STORAGE_KEEP_CACHESIZE))
      fast_reset_hashtable(&(ix->index_cache),-1,todo);
    else fast_reset_hashtable(&(ix->index_cache),0,todo);}
  return 0;
}

static int fast_swapout_pool(fd_pool p,void *data)
{
  struct HASHVECS_TODO *todo = (struct HASHVECS_TODO *)data;
  fast_reset_hashtable(&(p->pool_cache),67,todo);
  if (p->pool_changes.table_n_keys)
    fd_devoid_hashtable(&(p->pool_changes),0);
  return 0;
}

FD_EXPORT void fd_fast_swapout_all()
{
  int i = 0;
  struct HASHVECS_TODO todo;
  todo.to_free = u8_alloc_n(128,struct HASHVEC_TO_FREE);
  todo.n_to_free = 0; todo.max_to_free = 128;
  fd_for_indexes(fast_swapout_index,(void *)&todo);
  fd_for_pools(fast_swapout_pool,(void *)&todo);
  while (i<todo.n_to_free) {
    fd_free_buckets(todo.to_free[i].slots,
                    todo.to_free[i].n_slots,
                    todo.to_free[i].big_buckets);
    i++;}
  u8_free(todo.to_free);
}

/* fd_save */

static u8_mutex dosave_lock;
static u8_mutex onsave_handlers_lock;

static struct FD_ONSAVE {
  lispval onsave_handler;
  struct FD_ONSAVE *onsave_next;} *onsave_handlers=NULL;
static int n_onsave_handlers=0;

FD_EXPORT int fd_save(lispval arg)
{
  u8_lock_mutex(&dosave_lock);
  struct FD_ONSAVE *scan=onsave_handlers;
  while (scan) {
    lispval handler=scan->onsave_handler, result=FD_VOID;
    if ((FD_FUNCTIONP(handler))&&(FD_FUNCTION_ARITY(handler)))
      result = fd_apply(handler,1,&arg);
    else result = fd_apply(handler,0,NULL);
    if (FD_ABORTP(result)) {
      u8_unlock_mutex(&dosave_lock);
      fd_decref(result);
      return -1;}
    else scan=scan->onsave_next;}
  int rv = fd_commit_all();
  u8_unlock_mutex(&dosave_lock);
  return rv;
}

static lispval config_onsave_get(lispval var,void *data)
{
  struct FD_ONSAVE *scan; int i = 0; lispval result;
  u8_lock_mutex(&onsave_handlers_lock);
  result = fd_make_vector(n_onsave_handlers,NULL);
  scan = onsave_handlers; while (scan) {
    lispval handler = scan->onsave_handler; fd_incref(handler);
    FD_VECTOR_SET(result,i,handler);
    scan = scan->onsave_next; i++;}
  u8_unlock_mutex(&onsave_handlers_lock);
  return result;
}

static int config_onsave_set(lispval var,lispval val,void *data)
{
  struct FD_ONSAVE *fresh = u8_malloc(sizeof(struct FD_ONSAVE));
  if (!(FD_APPLICABLEP(val))) {
    fd_type_error("applicable","config_onsave",val);
    return -1;}
  u8_lock_mutex(&onsave_handlers_lock);
  fresh->onsave_next = onsave_handlers;
  fresh->onsave_handler = val;
  fd_incref(val);
  n_onsave_handlers++; onsave_handlers = fresh;
  u8_unlock_mutex(&onsave_handlers_lock);
  return 1;
}

/* Swap out to reduce memory footprint */

static size_t membase = 0;

u8_mutex fd_swapcheck_lock;

FD_EXPORT int fd_swapcheck()
{
  long long memgap; ssize_t usage = u8_memusage();
  lispval l_memgap = fd_config_get("SWAPCHECK");
  if (FIXNUMP(l_memgap)) memgap = FIX2INT(l_memgap);
  else if (!(VOIDP(l_memgap))) {
    u8_logf(LOG_WARN,fd_TypeError,"Bad SWAPCHECK config: %q",l_memgap);
    fd_decref(l_memgap);
    return -1;}
  else return 0;
  if (usage<(membase+memgap)) return 0;
  u8_lock_mutex(&fd_swapcheck_lock);
  if (usage>(membase+memgap)) {
    u8_logf(LOG_NOTICE,SwapCheck,"Swapping because %ld>%ld+%ld",
            usage,membase,memgap);
    fd_clear_slotcaches();
    fd_clear_callcache(VOID);
    fd_swapout_all();
    membase = u8_memusage();
    u8_logf(LOG_NOTICE,SwapCheck,
            "Swapped out, next swap at=%ld, swap at %d=%d+%d",
            membase+memgap,membase,memgap);
    u8_unlock_mutex(&fd_swapcheck_lock);}
  else {
    u8_unlock_mutex(&fd_swapcheck_lock);}
  return 1;
}

/* Init stuff */

static void register_header_files()
{
  u8_register_source_file(FRAMERD_STORAGE_H_INFO);
  u8_register_source_file(FRAMERD_POOLS_H_INFO);
  u8_register_source_file(FRAMERD_INDEXES_H_INFO);
  u8_register_source_file(FRAMERD_DRIVERS_H_INFO);
}

FD_EXPORT void fd_init_streams_c(void);
FD_EXPORT void fd_init_alcor_c(void);
FD_EXPORT void fd_init_hashdtype_c(void);
FD_EXPORT void fd_init_threadcache_c(void);
FD_EXPORT void fd_init_pools_c(void);
FD_EXPORT void fd_init_compress_c(void);
FD_EXPORT void fd_init_indexes_c(void);
FD_EXPORT void fd_init_dtcall_c(void);
FD_EXPORT void fd_init_oidobj_c(void);
FD_EXPORT void fd_init_dtproc_c(void);
FD_EXPORT void fd_init_frames_c(void);
FD_EXPORT void fd_init_cachecall_c(void);
FD_EXPORT void fd_init_ipeval_c(void);
FD_EXPORT void fd_init_methods_c(void);
FD_EXPORT int fd_init_drivers_c(void);
FD_EXPORT void fd_init_bloom_c(void);

FD_EXPORT int fd_init_storage()
{
  if (fdstorage_initialized) return fdstorage_initialized;
  fdstorage_initialized = 211*fd_init_lisp_types();

  register_header_files();
  u8_register_source_file(_FILEINFO);

  fd_init_threadcache_c();
  fd_init_streams_c();
  fd_init_alcor_c();
  fd_init_hashdtype_c();
  fd_init_oidobj_c();
  fd_init_cachecall_c();
  fd_init_compress_c();
  fd_init_bloom_c();
  fd_init_pools_c();
  fd_init_indexes_c();
  fd_init_frames_c();
  fd_init_drivers_c();
  fd_init_dtcall_c();
  fd_init_dtproc_c();
#if FD_IPEVAL_ENABLED
  fd_init_ipeval_c();
#endif
  fd_init_methods_c();

  id_symbol = fd_intern("%ID");
  flags_symbol = fd_intern("FLAGS");
  background_symbol = fd_intern("BACKGROUND");
  readonly_symbol = fd_intern("READONLY");
  repair_symbol = fd_intern("REPAIR");
  adjunct_symbol = fd_intern("ADJUNCT");
  sparse_symbol = fd_intern("SPARSE");
  register_symbol = fd_intern("REGISTER");
  phased_symbol = fd_intern("PHASED");
  slotcodes_symbol = fd_intern("SLOTCODES");
  oidcodes_symbol = fd_intern("OIDCODES");
  virtual_symbol = fd_intern("VIRTUAL");

  fd_set_oid_parser(better_parse_oid);
  fd_unparsers[fd_oid_type]=better_unparse_oid;
  oid_name_slotids = fd_make_list(2,fd_intern("%ID"),fd_intern("OBJ-NAME"));

  u8_init_mutex(&fd_swapcheck_lock);
  u8_init_mutex(&onsave_handlers_lock);
  u8_init_mutex(&dosave_lock);

  fd_commit_phases[0] = fd_intern("NONE");
  fd_commit_phases[1] = fd_intern("START");
  fd_commit_phases[2] = fd_intern("WRITE");
  fd_commit_phases[3] = fd_intern("SYNC");
  fd_commit_phases[4] = fd_intern("ROLLBACK");
  fd_commit_phases[5] = fd_intern("FLUSH");
  fd_commit_phases[6] = fd_intern("CLEANUP");
  fd_commit_phases[7] = fd_intern("DONE");

  u8_threadcheck();

  fd_register_config
    ("CACHELEVEL",_("Sets a level of time/memory tradeoff [0-3], default 1"),
     get_default_cache_level,
     set_default_cache_level,
     NULL);
  fd_register_config
    ("OIDDISPLAY",_("Default oid display level [0-3]"),
     get_oid_display_level,
     set_oid_display_level,
     NULL);
  fd_register_config
    ("DBLOGLEVEL",_("Default log level for database messages"),
     fd_intconfig_get,fd_loglevelconfig_set,&fd_storage_loglevel);

  fd_register_config
    ("PREFETCH",_("Whether to prefetch for large operations"),
     get_prefetch,
     set_prefetch,
     NULL);
  fd_register_config
    ("POOLS",_("pools used for OID resolution"),
     config_get_pools,config_use_pool,NULL);
  fd_register_config
    ("INDEXES",_("indexes opened"),
     config_get_indexes,config_open_index,NULL);
  fd_register_config
    ("BACKGROUND",_("indexes in the default search background"),
     config_get_background,config_use_index,NULL);

  fd_register_config
    ("DBCONNRESERVE",
     _("Number of connections to keep for each DB server"),
     fd_intconfig_get,fd_intconfig_set,&fd_dbconn_reserve_default);
  fd_register_config
    ("DBCONNCAP",_("Max number of connections (default) for each DB server"),
     fd_intconfig_get,fd_intconfig_set,&fd_dbconn_cap_default);
  fd_register_config
    ("DBCONNINIT",
     _("Number of connections to initially create for each DB server"),
     fd_intconfig_get,fd_intconfig_set,&fd_dbconn_init_default);
  fd_register_config
    ("LOOKUPOID",
     _("Functions and slotids for lookup up objects by name (@?name)"),
     fd_lconfig_get,fd_lconfig_push,&lookupfns);
  fd_register_config
    ("ONSAVE",_("Functions to run when saving databases"),
     config_onsave_get,
     config_onsave_set,
     NULL);


  return fdstorage_initialized;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
