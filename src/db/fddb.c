/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/apply.h"

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
fd_exception fd_ConnectionFailed=_("Connection to server failed");
u8_condition fd_Commitment=_("COMMIT");
u8_condition fd_ServerReconnect=_("Resetting server connection");
static u8_condition SwapCheck=_("SwapCheck");
fd_exception fd_BadMetaData=_("Error getting metadata");

int fd_default_cache_level=1;
int fd_oid_display_level=2;
int fddb_loglevel=LOG_NOTICE;
int fd_prefetch=FD_PREFETCHING_ENABLED;

int fd_dbconn_reserve_default=FD_DBCONN_RESERVE_DEFAULT;
int fd_dbconn_cap_default=FD_DBCONN_CAP_DEFAULT;
int fd_dbconn_init_default=FD_DBCONN_INIT_DEFAULT;

static fdtype id_symbol;
static fdtype lookupfns=FD_EMPTY_CHOICE;

static int fddb_initialized=0;

static fdtype better_parse_oid(u8_string start,int len)
{
  if (start[1]=='?') {
    const u8_byte *scan=start+2;
    fdtype name=fd_parse(scan);
    if (scan-start>len) return FD_VOID;
    else if (FD_ABORTP(name)) return name;
    else if (fd_background) {
      fdtype key=fd_conspair(id_symbol,fd_incref(name));
      fdtype item=fd_index_get((fd_index)fd_background,key);
      fd_decref(key);
      if (FD_OIDP(item)) return item;
      else if (FD_CHOICEP(item)) {
        fd_decref(item);
        return fd_err(fd_AmbiguousObjectName,"better_parse_oid",NULL,name);}
      else if (!((FD_EMPTY_CHOICEP(item))||(FD_FALSEP(item)))) {
        fd_decref(item);
        return fd_type_error("oid","better_parse_oid",item);}}
    else {}
    if (lookupfns!=FD_EMPTY_LIST) {
      FD_DOLIST(method,lookupfns) {
        if ((FD_SYMBOLP(method))||(FD_OIDP(method))) {
          fdtype key=fd_conspair(method,fd_incref(name));
          fdtype item=fd_index_get((fd_index)fd_background,key);
          fd_decref(key);
          if (FD_OIDP(item)) return item;
          else if (!((FD_EMPTY_CHOICEP(item))||
                     (FD_FALSEP(item))||
                     (FD_VOIDP(item)))) continue;
          else return fd_type_error("oid","better_parse_oid",item);}
        else if (FD_APPLICABLEP(method)) {
          fdtype item=fd_apply(method,1,&name);
          if (FD_ABORTP(item)) return item;
          else if (FD_OIDP(item)) return item;
          else if ((FD_EMPTY_CHOICEP(item))||
                   (FD_FALSEP(item))||
                   (FD_VOIDP(item))) continue;
          else return fd_type_error("oid","better_parse_oid",item);}
        else return fd_type_error("lookup method","better_parse_oid",method);}
      return fd_err(fd_UnknownObjectName,"better_parse_oid",NULL,name);}
    else {
      return fd_err(fd_UnknownObjectName,"better_parse_oid",NULL,name);}}
  else if (((strchr(start,'/')))&&
           (((u8_string)strchr(start,'/'))<(start+len))) {
    FD_OID base=FD_NULL_OID_INIT, result=FD_NULL_OID_INIT;
    unsigned int delta;
    u8_byte prefix[64], suffix[64];
    const u8_byte *copy_start, *copy_end;
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
  else return fd_parse_oid_addr(start,len);
}

static fdtype oid_name_slotids=FD_EMPTY_LIST;

static fdtype default_get_oid_name(fd_pool p,fdtype oid)
{
  if (FD_APPLICABLEP(p->oidnamefn)) return FD_VOID;
  else {
    fdtype ov=fd_oid_value(oid);
    if (((FD_OIDP(p->oidnamefn)) || (FD_SYMBOLP(p->oidnamefn))) &&
        (!(FD_CHOICEP(ov))) && (FD_TABLEP(ov))) {
      fdtype probe=fd_frame_get(oid,p->oidnamefn);
      if (FD_EMPTY_CHOICEP(probe)) {}
      else if (FD_ABORTP(probe)) {fd_decref(probe);}
      else {fd_decref(ov); return probe;}}
    if ((!(FD_CHOICEP(ov))) && (FD_TABLEP(ov))) {
      FD_DOLIST(slotid,oid_name_slotids) {
        fdtype probe=fd_frame_get(oid,slotid);
        if (FD_EMPTY_CHOICEP(probe)) {}
        else if (FD_ABORTP(probe)) {fd_decref(probe);}
        else {fd_decref(ov); return probe;}}
      fd_decref(ov);
      return FD_VOID;}
    else {fd_decref(ov); return FD_VOID;}}
}

fdtype (*fd_get_oid_name)(fd_pool p,fdtype oid)=default_get_oid_name;

static int print_oid_name(u8_output out,fdtype name,int top)
{
  if ((FD_VOIDP(name)) || (FD_EMPTY_CHOICEP(name))) return 0;
  else if (FD_EMPTY_LISTP(name))
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
    return u8_putc(out,'}');}
  else if (FD_PAIRP(name)) {
    fdtype scan=name; u8_putc(out,'(');
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
      fdtype name=fd_get_oid_name(p,x);
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
  return FD_INT(fd_default_cache_level);
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
  return FD_INT(fd_oid_display_level);
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
    fd_lock_mutex(&(fd_background->fd_lock));
    n=fd_background->n_indices; indices=fd_background->indices;
    while (i<n) {
      fdtype lix=fd_index2lisp(indices[i]); i++;
      FD_ADD_TO_CHOICE(results,lix);}
    fd_unlock_mutex(&(fd_background->fd_lock));
    return results;}
}
static int config_use_index(fdtype var,fdtype spec,void *data)
{
  if (FD_STRINGP(spec))
    if (fd_use_index(FD_STRDATA(spec))) return 1;
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
  if (fd_commit_indices()<0) return -1;
  else return fd_commit_pools();
}

FD_EXPORT void fd_swapout_all()
{
  fd_swapout_indices();
  fd_swapout_pools();
}

/* Fast swap outs. */

/* This swaps stuff out while trying to hold locks for the minimal possible
   time.  It does this by using fd_fast_reset_hashtable() and actually freeing
   the storage outside of the lock.  This is good for swapping out on
   multi-threaded servers. */

/* This stores a hashtable's data to free after it has been reset. */
struct HASHVEC_TO_FREE {
  struct FD_HASH_BUCKET **slots; int n_slots;};
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
  if ((((ix->flags)&FD_INDEX_NOSWAP)==0) && (ix->cache.fd_n_keys)) {
    if ((ix->flags)&(FD_STICKY_CACHESIZE))
      fast_reset_hashtable(&(ix->cache),-1,todo);
    else fast_reset_hashtable(&(ix->cache),0,todo);}
  return 0;
}

static int fast_swapout_pool(fd_pool p,void *data)
{
  struct HASHVECS_TODO *todo=(struct HASHVECS_TODO *)data;
  fast_reset_hashtable(&(p->cache),67,todo);
  if (p->locks.fd_n_keys) fd_devoid_hashtable(&(p->locks));
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

/* */

/* Swap out to reduce memory footprint */

static size_t membase=0;

#if FD_THREADS_ENABLED
u8_mutex fd_swapcheck_lock;
#endif

#if 0
static int cache_load()
{
  return fd_object_cache_load()+fd_index_cache_load()+
    fd_slot_cache_load()+fd_callcache_load();
}
#endif

FD_EXPORT int fd_swapcheck()
{
  int memgap; ssize_t usage=u8_memusage();
  fdtype l_memgap=fd_config_get("SWAPCHECK");
  if (FD_FIXNUMP(l_memgap)) memgap=FD_FIX2INT(l_memgap);
  else if (!(FD_VOIDP(l_memgap))) {
    u8_log(LOG_WARN,fd_TypeError,"Bad SWAPCHECK config: %q",l_memgap);
    fd_decref(l_memgap);
    return -1;}
  else return 0;
  if (usage<(membase+memgap)) return 0;
  fd_lock_mutex(&fd_swapcheck_lock);
  if (usage>(membase+memgap)) {
    u8_log(LOG_NOTICE,SwapCheck,"Swapping because %ld>%ld+%ld",
           usage,membase,memgap);
    fd_clear_slotcaches();
    fd_clear_callcache(FD_VOID);
    fd_swapout_all();
    membase=u8_memusage();
    u8_log(LOG_NOTICE,SwapCheck,
           "Swapped out, next swap at=%ld, swap at %d=%d+%d",
           membase+memgap,membase,memgap);
    fd_unlock_mutex(&fd_swapcheck_lock);}
  else {
    fd_unlock_mutex(&fd_swapcheck_lock);}
  return 1;
}

/* Init stuff */

static void register_header_files()
{
  u8_register_source_file(FRAMERD_FDDB_H_INFO);
  u8_register_source_file(FRAMERD_POOLS_H_INFO);
  u8_register_source_file(FRAMERD_INDICES_H_INFO);
}

FD_EXPORT void fd_init_threadcache_c(void);
FD_EXPORT void fd_init_pools_c(void);
FD_EXPORT void fd_init_indices_c(void);
FD_EXPORT void fd_init_dtcall_c(void);
FD_EXPORT void fd_init_netpools_c(void);
FD_EXPORT void fd_init_netindices_c(void);
FD_EXPORT void fd_init_xtables_c(void);
FD_EXPORT void fd_init_apply_c(void);
FD_EXPORT void fd_init_dtproc_c(void);
FD_EXPORT void fd_init_frames_c(void);
FD_EXPORT void fd_init_cachecall_c(void);
FD_EXPORT void fd_init_ipeval_c(void);
FD_EXPORT void fd_init_methods_c(void);

FD_EXPORT int fd_init_db()
{
  if (fddb_initialized) return fddb_initialized;
  fddb_initialized=211*fd_init_dtypelib();

  fd_init_dtypelib();

  register_header_files();
  u8_register_source_file(_FILEINFO);

  fd_init_threadcache_c();
  fd_init_pools_c();
  fd_init_indices_c();
  fd_init_dtcall_c();
  fd_init_netpools_c();
  fd_init_netindices_c();
  fd_init_xtables_c();
  fd_init_apply_c();
  fd_init_dtproc_c();
  fd_init_frames_c();
  fd_init_cachecall_c();
#if FD_IPEVAL_ENABLED
  fd_init_ipeval_c();
#endif
  fd_init_methods_c();
  id_symbol=fd_intern("%ID");
  fd_set_oid_parser(better_parse_oid);
  fd_unparsers[fd_oid_type]=better_unparse_oid;
  oid_name_slotids=fd_make_list(2,fd_intern("%ID"),fd_intern("OBJ-NAME"));

#if FD_THREADS_ENABLED
  fd_init_mutex(&fd_swapcheck_lock);
#endif

  u8_threadcheck();

  fd_register_config("CACHELEVEL",_("Sets a level of time/memory tradeoff [0-3], default 1"),
                     get_default_cache_level,
                     set_default_cache_level,
                     NULL);
  fd_register_config("OIDDISPLAY",_("Default oid display level [0-3]"),
                     get_oid_display_level,
                     set_oid_display_level,
                     NULL);
  fd_register_config("FDDBLOGLEVEL",_("Default log level for database messages"),
                     fd_intconfig_get,fd_intconfig_set,&fddb_loglevel);

  fd_register_config("PREFETCH",_("Whether to prefetch for large operations"),
                     get_prefetch,
                     set_prefetch,
                     NULL);
  fd_register_config("POOLS",_("pools used for OID resolution"),
                     config_get_pools,config_use_pool,NULL);
  fd_register_config("INDICES",_("indices opened"),
                     config_get_indices,config_open_index,NULL);
  fd_register_config("BACKGROUND",_("indices in the default search background"),
                     config_get_background,config_use_index,NULL);

  fd_register_config("DBCONNRESERVE",_("Number of connections (default) to keep for each DB server"),
                     fd_intconfig_get,fd_intconfig_set,&fd_dbconn_reserve_default);
  fd_register_config("DBCONNCAP",_("Max number of connections (default) for each DB server"),
                     fd_intconfig_get,fd_intconfig_set,&fd_dbconn_cap_default);
  fd_register_config("DBCONNINIT",_("Number of connections (default) to initially create for each DB server"),
                     fd_intconfig_get,fd_intconfig_set,&fd_dbconn_init_default);

  fd_register_config("LOOKUPOID",
                     _("Functions and slotids for lookup up objects by name (@?name)"),
                     fd_lconfig_get,fd_lconfig_push,&lookupfns);

  return fddb_initialized;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
