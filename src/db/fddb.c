/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/fddb.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8rusage.h>

#include <stdarg.h>
/* We include this for sscanf, but we're not really using stdio. */
#include <stdio.h>

fd_exception fd_InternalError=_("FramerD Database internal error"),
  fd_AmbiguousObjectName=_("Ambiguous object name"),
  fd_UnknownObjectName=_("Unknown object name"),
  fd_BadServerResponse=_("bad server response"),
  fd_NoBackground=_("No default background indices"),
  fd_UnallocatedOID=_("Reference to unallocated OID");
u8_condition fd_Commitment=_("COMMIT");
u8_condition fd_ServerReconnect=_("Resetting server connection");
static u8_condition SwapCheck=_("SwapCheck");
fd_exception fd_BadMetaData=_("Error getting metadata");


int fd_default_cache_level=1;
int fd_oid_display_level=2;
int fd_prefetch=FD_PREFETCHING_ENABLED;

static fdtype id_symbol;

static int fddb_initialized=0;

static fdtype better_parse_oid(u8_string start,int len)
{
  if (start[1]=='?') {
    u8_byte *scan=start+2;
    fdtype name=fd_parse(scan), found=FD_VOID;
    if (scan-start>len) return FD_VOID;
    else if (fd_background) {
      fdtype key=fd_init_pair(NULL,id_symbol,fd_incref(name));
      fdtype item=fd_index_get((fd_index)fd_background,key);
      fd_decref(key);
      if (FD_OIDP(item)) return item;
      else if (FD_CHOICEP(item)) {
	fd_decref(item);
	return fd_err(fd_AmbiguousObjectName,"better_parse_oid",NULL,name);}
      else {
	fd_decref(item);
	return fd_err(fd_UnknownObjectName,"better_parse_oid",NULL,name);}}
    else return fd_err(fd_NoBackground,"better_parse_oid",NULL,name);}
  else {
    FD_OID base, result; unsigned int delta;
    u8_byte prefix[64], suffix[64], *copy_start, *copy_end;
    copy_start=((start[1]=='/') ? (start+2) : (start+1));
    copy_end=strchr(copy_start,'/');
    if (copy_end==NULL) return FD_PARSE_ERROR;
    strncpy(prefix,copy_start,(copy_end-copy_start));
    prefix[(copy_end-copy_start)]='\0';
    if (start[1]=='/') {
      fd_pool p=fd_find_pool_by_prefix(prefix);
      if (p==NULL) return FD_VOID;
      else base=p->base;}
    else {
      unsigned int hi;
      if (sscanf(prefix,"%x",&hi)<1) return FD_PARSE_ERROR;
      FD_SET_OID_LO(base,0);
      FD_SET_OID_HI(base,hi);}
    copy_start=copy_end+1; copy_end=start+len;
    strncpy(suffix,copy_start,(copy_end-copy_start));
    suffix[(copy_end-copy_start)]='\0';
    if (sscanf(suffix,"%x",&delta)<1)  return FD_PARSE_ERROR;
    result=FD_OID_PLUS(base,delta);
    return fd_make_oid(result);}
}

static fdtype oid_name_slotids=FD_EMPTY_LIST;

static fdtype default_get_oid_name(fdtype oid)
{
  fdtype ov=fd_oid_value(oid);
  if ((!(FD_CHOICEP(ov))) && (FD_TABLEP(ov))) {
    FD_DOLIST(slotid,oid_name_slotids) {
      fdtype probe=fd_frame_get(oid,slotid);
      if (FD_EMPTY_CHOICEP(probe)) {}
      else if (FD_ABORTP(probe)) {fd_decref(probe);}
      else {fd_decref(ov); return probe;}}
    fd_decref(ov);
    return FD_VOID;}
  else {fd_decref(ov); return FD_VOID;}
}

fdtype (*fd_get_oid_name)(fdtype oid)=default_get_oid_name;

static int print_oid_name(u8_output out,fdtype name,int top)
{
  if ((FD_VOIDP(name)) || (FD_EMPTY_CHOICEP(name))) return 0;
  else if ((FD_EMPTY_LISTP(name))) 
    return u8_puts(out,"()");
  else if (FD_OIDP(name)) {
    FD_OID addr=FD_OID_ADDR(name);
    unsigned int hi=FD_OID_HI(addr), lo=FD_OID_LO(addr);
    return u8_printf(out,"@%x/%x",hi,lo);}
  else if ((FD_SYMBOLP(name)) ||
	   (FD_NUMBERP(name)) ||
	   (FDTYPE_CONSTANTP(name)))
    if (top) {
      int retval=-1;
      u8_puts(out,"{"); retval=fd_unparse(out,name);
      if (retval<0) return retval;
      else retval=u8_puts(out,"}");
      return retval;}
    else return fd_unparse(out,name);
  else if (FD_STRINGP(name)) 
    return fd_unparse(out,name);
  else if ((FD_CHOICEP(name)) || (FD_ACHOICEP(name))) {
    int i=0; u8_putc(out,'{'); {
      FD_DO_CHOICES(item,name) {
	if (i++>0) u8_putc(out,' ');
	if (print_oid_name(out,item,0)<0) return -1;}}
    u8_putc(out,'}'); }
  else if (FD_PAIRP(name)) {
    int i=0; fdtype scan=name; u8_putc(out,'(');
    if (print_oid_name(out,FD_CAR(scan),0)<0) return -1;
    else scan=FD_CDR(scan);
    while (FD_PAIRP(scan)) {
      u8_putc(out,' ');
      if (print_oid_name(out,FD_CAR(scan),0)<0) return -1;
      scan=FD_CDR(scan);}
    if (FD_EMPTY_LISTP(scan))
      return u8_putc(out,')');
    else {
      u8_puts(out," . ");
      print_oid_name(out,scan,0);
      return u8_putc(out,')');}}
  else if (FD_VECTORP(name)) {
    int i=0, len=FD_VECTOR_LENGTH(name);
    u8_puts(out,"#(");
    while (i< len) {
      if (i>0) u8_putc(out,' ');
      if (print_oid_name(out,FD_VECTOR_REF(name,i),0)<0)
	return -1;
      i++;}
    return u8_puts(out,")");}
  else return 0;
}

static int better_unparse_oid(u8_output out,fdtype x)
{
  if ((fd_oid_display_level<1) || (out->u8_streaminfo&U8_STREAM_TACITURN)) {
    FD_OID addr=FD_OID_ADDR(x); char buf[128];
    unsigned int hi=FD_OID_HI(addr), lo=FD_OID_LO(addr);
    sprintf(buf,"@%x/%x",hi,lo);
    u8_puts(out,buf);
    return 1;}
  else {
    fd_pool p=fd_oid2pool(x);
    if ((p == NULL) || (p->prefix==NULL)) {
      FD_OID addr=FD_OID_ADDR(x); char buf[128];
      unsigned int hi=FD_OID_HI(addr), lo=FD_OID_LO(addr);
      sprintf(buf,"@%x/%x",hi,lo);
      u8_puts(out,buf);}
    else {
      FD_OID addr=FD_OID_ADDR(x); char buf[128];
      unsigned int off=FD_OID_DIFFERENCE(addr,p->base);
      sprintf(buf,"@/%s/%x",p->prefix,off);
      u8_puts(out,buf);}
    if (p == NULL) return 1;
    else if (fd_oid_display_level<2) return 1;
    else if ((fd_oid_display_level<3) &&
	     (!(fd_hashtable_probe_novoid(&(p->cache),x))) &&
	     (!(fd_hashtable_probe_novoid(&(p->locks),x))))
      return 1;
    else {
      fdtype name=fd_get_oid_name(x);
      int retval=print_oid_name(out,name,1);
      fd_decref(name);
      return retval;}}
}

/* CONFIG settings */

static int set_default_cache_level(fdtype var,fdtype val,void *data)
{
  if (FD_FIXNUMP(val)) {
    int new_level=FD_FIX2INT(val);
    fd_default_cache_level=new_level;
    return 1;}
  else return -1;
}
static fdtype get_default_cache_level(fdtype var,void *data)
{
  return FD_INT2DTYPE(fd_default_cache_level);
}

static int set_oid_display_level(fdtype var,fdtype val,void *data)
{
  if (FD_FIXNUMP(val)) {
    fd_oid_display_level=FD_FIX2INT(val);
    return 1;}
  else return -1;
}
static fdtype get_oid_display_level(fdtype var,void *data)
{
  return FD_INT2DTYPE(fd_oid_display_level);
}

static int set_prefetch(fdtype var,fdtype val,void *data)
{
  if (FD_FALSEP(val)) fd_prefetch=0;
  else fd_prefetch=1;
  return 1;
}
static fdtype get_prefetch(fdtype var,void *data)
{
  if (fd_prefetch) return FD_TRUE; else return FD_FALSE;
}

static fdtype config_get_pools(fdtype var,void *data)
{
  return fd_all_pools();
}
static int config_use_pool(fdtype var,fdtype spec,void *data)
{
  if (FD_STRINGP(spec))
    if (fd_use_pool(FD_STRDATA(spec))) return 1;
    else return -1;
  else return fd_reterr(fd_TypeError,"config_use_pool",
			u8_strdup(_("pool spec")),FD_VOID);
}

/* Config methods */

static fdtype config_get_indices(fdtype var,void *data)
{
  fdtype results=FD_EMPTY_CHOICE;
  int i=0; while (i < fd_n_primary_indices) {
    fdtype lindex=fd_index2lisp(fd_primary_indices[i]);
    FD_ADD_TO_CHOICE(results,lindex);
    i++;}
  if (i>=fd_n_primary_indices) return results;
  i=0; while (i < fd_n_secondary_indices) {
    fdtype lindex=fd_index2lisp(fd_secondary_indices[i]);
    FD_ADD_TO_CHOICE(results,lindex);
    i++;}
  return results;
}
static int config_open_index(fdtype var,fdtype spec,void *data)
{
  if (FD_STRINGP(spec))
    if (fd_open_index(FD_STRDATA(spec))) return 1;
    else return -1;
  else {
    fd_seterr(fd_TypeError,"config_open_index",NULL,fd_incref(spec));
    return -1;}
}

static fdtype config_get_background(fdtype var,void *data)
{
  fdtype results=FD_EMPTY_CHOICE;
  if (fd_background==NULL) return results;
  else {
    int i=0, n; fd_index *indices;
    fd_lock_mutex(&(fd_background->lock));
    n=fd_background->n_indices; indices=fd_background->indices;
    while (i<n) {
      fdtype lix=fd_index2lisp(indices[i]); i++;
      FD_ADD_TO_CHOICE(results,lix);}
    fd_unlock_mutex(&(fd_background->lock));
    return results;}
}
static int config_use_index(fdtype var,fdtype spec,void *data)
{
  if (FD_STRINGP(spec))
    if (fd_use_index(FD_STRDATA(spec))) return 1;
    else return -1;
  else if (FD_INDEXP(spec))
    if (fd_add_to_background(fd_lisp2index(spec))) return 1;
    else return -1;

  else {
    fd_seterr(fd_TypeError,"config_use_index",NULL,fd_incref(spec));
    return -1;}
}

/* Global pool/index functions */

FD_EXPORT int fd_commit_all()
{
  /* Not really ACID, but probably better than doing pools first. */
  if (fd_commit_indices()<0) return -1;
  else return fd_commit_pools();
}

FD_EXPORT void fd_swapout_all()
{
  fd_swapout_indices();
  fd_swapout_pools();
}

/* Fast swap outs.

/* This swaps stuff out while trying to hold locks for the minimal possible
   time.  It does this by using fd_fast_reset_hashtable() and actually freeing
   the storage outside of the lock.  This is good for swapping out on
   multi-threaded servers. */

/* This stores a hashtable's data to free after it has been reset. */
struct HASHVEC_TO_FREE {
  struct FD_HASHENTRY **slots; int n_slots;};
struct HASHVECS_TODO {
  struct HASHVEC_TO_FREE *to_free; int n_to_free, max_to_free;};

static void fast_reset_hashtable
  (fd_hashtable h,int n_slots_arg,struct HASHVECS_TODO *todo)
{
  int i=todo->n_to_free;
  if (todo->n_to_free==todo->max_to_free) {
    todo->to_free=u8_realloc_n
      (todo->to_free,(todo->max_to_free)*2,struct HASHVEC_TO_FREE);
    todo->max_to_free=(todo->max_to_free)*2;}
  fd_fast_reset_hashtable(h,n_slots_arg,1,
			  &(todo->to_free[i].slots),
			  &(todo->to_free[i].n_slots));
  todo->n_to_free=i+1;
}

static int fast_swapout_index(fd_index ix,void *data)
{
  struct HASHVECS_TODO *todo=(struct HASHVECS_TODO *)data;
  if ((((ix->flags)&FD_INDEX_NOSWAP)==0) && (ix->cache.n_keys)) {
    if ((ix->flags)&(FD_STICKY_CACHESIZE))
      fast_reset_hashtable(&(ix->cache),-1,todo);
    else fast_reset_hashtable(&(ix->cache),0,todo);}
  return 0;
}

static int fast_swapout_pool(fd_pool p,void *data)
{
  struct HASHVECS_TODO *todo=(struct HASHVECS_TODO *)data;
  fast_reset_hashtable(&(p->cache),67,todo);
  if (p->locks.n_keys) fd_devoid_hashtable(&(p->locks));
  return 0;
}

FD_EXPORT void fd_fast_swapout_all()
{
  int i=0;
  struct HASHVECS_TODO todo;
  todo.to_free=u8_alloc_n(128,struct HASHVEC_TO_FREE);
  todo.n_to_free=0; todo.max_to_free=128;
  fd_for_indices(fast_swapout_index,(void *)&todo);
  fd_for_pools(fast_swapout_pool,(void *)&todo);
  while (i<todo.n_to_free) {
    fd_free_hashvec(todo.to_free[i].slots,todo.to_free[i].n_slots);
    u8_free(todo.to_free[i].slots);
    i++;}
  u8_free(todo.to_free);
}

/* *

/* Swap out to reduce memory footprint */

static long membase=0;

#if FD_THREADS_ENABLED
u8_mutex fd_swapcheck_lock;
#endif

static int cache_load()
{
  return fd_object_cache_load()+fd_index_cache_load()+
    fd_slot_cache_load()+fd_callcache_load();
}

FD_EXPORT int fd_swapcheck()
{
  int memgap; size_t usage=u8_memusage();
  fdtype l_memgap=fd_config_get("SWAPCHECK");
  if (FD_FIXNUMP(l_memgap)) memgap=FD_FIX2INT(l_memgap);
  else if (!(FD_VOIDP(l_memgap))) {
    u8_warn(fd_TypeError,"Bad SWAPCHECK config: %q",l_memgap);
    fd_decref(l_memgap);
    return -1;}
  else return 0;
  if (usage<(membase+memgap)) return 0;
  fd_lock_mutex(&fd_swapcheck_lock);
  if (usage>(membase+memgap)) {
    u8_notify(SwapCheck,"Swapping because %ld>%ld+%ld",
	      usage,membase,memgap);
    fd_clear_slotcaches();
    fd_clear_callcache(FD_VOID);
    fd_swapout_all();
    membase=u8_memusage();
    u8_notify(SwapCheck,"Swapped out, new membase=%ld",
	      membase);
    fd_unlock_mutex(&fd_swapcheck_lock);}
  else {
    fd_unlock_mutex(&fd_swapcheck_lock);}
  return 1;
}

/* Init stuff */

static void register_header_files()
{
  fd_register_source_file(FDB_FDDB_H_VERSION);
  fd_register_source_file(FDB_POOLS_H_VERSION);
  fd_register_source_file(FDB_INDICES_H_VERSION);
}

FD_EXPORT void fd_init_pools_c(void);
FD_EXPORT void fd_init_indices_c(void);
FD_EXPORT void fd_init_netpools_c(void);
FD_EXPORT void fd_init_netindices_c(void);
FD_EXPORT void fd_init_xtables_c(void);
FD_EXPORT void fd_init_apply_c(void);
FD_EXPORT void fd_init_dtproc_c(void);
FD_EXPORT void fd_init_dtcall_c(void);
FD_EXPORT void fd_init_frames_c(void);
FD_EXPORT void fd_init_ipeval_c(void);
FD_EXPORT void fd_init_methods_c(void);

FD_EXPORT int fd_init_db()
{
  if (fddb_initialized) return fddb_initialized;
  fddb_initialized=211*fd_init_dtypelib();

  register_header_files();
  fd_register_source_file(versionid);

  fd_init_pools_c();
  fd_init_indices_c();
  fd_init_netpools_c();
  fd_init_netindices_c();
  fd_init_xtables_c();
  fd_init_apply_c();
  fd_init_dtproc_c();
  fd_init_dtcall_c();
  fd_init_frames_c();
  fd_init_ipeval_c();
  fd_init_methods_c();
  id_symbol=fd_intern("%ID");
  fd_set_oid_parser(better_parse_oid);
  fd_unparsers[fd_oid_type]=better_unparse_oid;
  oid_name_slotids=fd_make_list(2,fd_intern("%ID"),fd_intern("OBJ-NAME"));

#if FD_THREADS_ENABLED
  fd_init_mutex(&fd_swapcheck_lock);
#endif

  fd_register_config("CACHELEVEL",
		     get_default_cache_level,
		     set_default_cache_level,
		     NULL);
  fd_register_config("OIDDISPLAY",
		     get_oid_display_level,
		     set_oid_display_level,
		     NULL);
  fd_register_config("PREFETCH",
		     get_prefetch,
		     set_prefetch,
		     NULL);
  fd_register_config("POOLS",config_get_pools,config_use_pool,NULL);
  fd_register_config("INDICES",config_get_indices,config_open_index,NULL);
  fd_register_config("BACKGROUND",config_get_background,config_use_index,NULL);

  return fddb_initialized;
}
