/* -*- mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1
#define FD_FAST_CHOICE_CONTAINSP 1


#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"
#include "framerd/bloom.h"
#include "framerd/frames.h"
#include "framerd/methods.h"
#include "framerd/sequences.h"
#include "framerd/dbprims.h"
#include "framerd/numbers.h"

#include "libu8/u8printf.h"

static lispval pools_symbol, indexes_symbol, id_symbol, drop_symbol;
static lispval flags_symbol, register_symbol, readonly_symbol, phased_symbol;
static lispval background_symbol, adjunct_symbol, sparse_symbol, repair_symbol;

static lispval slotidp(lispval arg)
{
  if ((OIDP(arg)) || (SYMBOLP(arg))) return FD_TRUE;
  else return FD_FALSE;
}

#define INDEXP(x) ( (FD_INDEXP(x)) || (TYPEP((x),fd_consed_index_type)) )
#define POOLP(x)  ( (FD_POOLP(x))  || (TYPEP((x),fd_consed_pool_type)) )

/* These are called when the lisp version of its pool/index argument
   is being returned and needs to be incref'd if it is consed. */
FD_FASTOP lispval index2lisp(fd_index ix)
{
  if (ix == NULL)
    return FD_ERROR;
  else if (ix->index_serialno>=0)
    return LISPVAL_IMMEDIATE(fd_index_type,ix->index_serialno);
  else return (lispval)ix;
}
FD_FASTOP lispval index_ref(fd_index ix)
{
  if (ix == NULL)
    return FD_ERROR;
  else if (ix->index_serialno>=0)
    return LISPVAL_IMMEDIATE(fd_index_type,ix->index_serialno);
  else {
    lispval lix = (lispval)ix;
    fd_incref(lix);
    return lix;}
}
static lispval pool2lisp(fd_pool p)
{
  lispval poolval=fd_pool2lisp(p);
  fd_incref(poolval);
  return poolval;
}

#define FALSE_ARGP(x) ( ((x)==FD_VOID) || ((x)==FD_FALSE) || ((x)==FD_FIXZERO) )

static int load_db_module(lispval opts,u8_context context)
{
  if ( (FD_VOIDP(opts)) || (FD_EMPTYP(opts)) ||
       (FD_FALSEP(opts)) || (FD_DEFAULTP(opts)) )
    return 0;
  else {
    lispval modules = fd_getopt(opts,FDSYM_MODULE,FD_VOID);
    if ( (FD_VOIDP(modules)) || (FD_FALSEP(modules)) || (FD_EMPTYP(modules)) )
      return 0;
    else {
      lispval mod = fd_find_module(modules,0,1);
      fd_decref(modules);
      if (FD_ABORTP(mod)) {
        fd_seterr("MissingDBModule",context,
                  ((FD_SYMBOLP(modules)) ? (FD_SYMBOL_NAME(modules)) : (NULL)),
                  modules);
        return -1;}
      else {
        fd_decref(mod);
        return 1;}}}
}

/* Finding frames, etc. */

static lispval find_frames_lexpr(int n,lispval *args)
{
  if (n%2)
    if (FALSEP(args[0]))
      return fd_bgfinder(n-1,args+1);
    else return fd_finder(args[0],n-1,args+1);
  else return fd_bgfinder(n,args);
}

/* This is like find_frames but ignores any slot/value pairs
   whose values are empty (and thus would rule out any results at all). */
static lispval xfind_frames_lexpr(int n,lispval *args)
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
          results = fd_bgfinder(j,slotvals);
        else results = fd_finder(args[0],j,slotvals);
      else results = fd_bgfinder(j,slotvals);
      u8_free(slotvals);
      return results;}
    else i = i+2;
  if (n%2)
    if (FALSEP(args[0]))
      return fd_bgfinder(n-1,args+1);
    else return fd_finder(args[0],n-1,args+1);
  else return fd_bgfinder(n,args);
}

static lispval prefetch_slotvals(lispval index,lispval slotids,lispval values)
{
  fd_index ix = fd_indexptr(index);
  if (ix) fd_find_prefetch(ix,slotids,values);
  else return fd_type_error("index","prefetch_slotvals",index);
  return VOID;
}

static lispval find_frames_prefetch(int n,lispval *args)
{
  int i = (n%2);
  fd_index ix = ((n%2) ? (fd_indexptr(args[0])) : ((fd_index)(fd_background)));
  if (PRED_FALSE(ix == NULL))
    return fd_type_error("index","prefetch_slotvals",args[0]);
  else while (i<n) {
    DO_CHOICES(slotid,args[i]) {
      if ((SYMBOLP(slotid)) || (OIDP(slotid))) {}
      else return fd_type_error("slotid","find_frames_prefetch",slotid);}
    i = i+2;}
  i = (n%2); while (i<n) {
    lispval slotids = args[i], values = args[i+1];
    fd_find_prefetch(ix,slotids,values);
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
                          (fd_frame_get(frame,slotid)) :
                          (fd_get(frame,slotid,EMPTY)));
        DO_CHOICES(value,values) {
          lispval key = fd_conspair(fd_incref(slotid),fd_incref(value));
          fd_add(ix,key,frame);
          fd_decref(key);}
        fd_decref(values);}}}
  else {
    DO_CHOICES(slotid,slotids) {
      DO_CHOICES(value,values) {
        lispval key = fd_conspair(fd_incref(slotid),fd_incref(value));
        fd_add(ix,key,frames);
        fd_decref(key);}}}
}

static lispval index_frame_prim
  (lispval indexes,lispval frames,lispval slotids,lispval values)
{
  if (CHOICEP(indexes)) {
    DO_CHOICES(index,indexes)
      if (HASHTABLEP(index))
        hashtable_index_frame(index,frames,slotids,values);
      else {
        fd_index ix = fd_indexptr(index);
        if (PRED_FALSE(ix == NULL))
          return fd_type_error("index","index_frame_prim",index);
        else if (fd_index_frame(ix,frames,slotids,values)<0)
          return FD_ERROR;}}
  else if (HASHTABLEP(indexes)) {
    hashtable_index_frame(indexes,frames,slotids,values);
    return VOID;}
  else {
    fd_index ix = fd_indexptr(indexes);
    if (PRED_FALSE(ix == NULL))
      return fd_type_error("index","index_frame_prim",indexes);
    else if (fd_index_frame(ix,frames,slotids,values)<0)
      return FD_ERROR;}
  return VOID;
}

/* Pool and index functions */

static lispval poolp(lispval arg)
{
  if (POOLP(arg))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval indexp(lispval arg)
{
  if (INDEXP(arg))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval getpool(lispval arg)
{
  fd_pool p = NULL;
  if (FD_POOLP(arg)) return fd_incref(arg);
  else if (STRINGP(arg))
    p = fd_name2pool(CSTRING(arg));
  else if (OIDP(arg)) p = fd_oid2pool(arg);
  if (p) return pool2lisp(p);
  else return EMPTY;
}

static u8_condition Unknown_PoolName=_("Unknown pool name");

static lispval set_pool_namefn(lispval arg,lispval method)
{
  fd_pool p = NULL;
  if (FD_POOLP(arg))
    p = fd_lisp2pool(arg);
  else if (STRINGP(arg)) {
    p = fd_name2pool(CSTRING(arg));
    if (!(p)) return fd_err
                (Unknown_PoolName,"set_pool_namefn",NULL,arg);}
  else if (OIDP(arg))
    p = fd_oid2pool(arg);
  else return fd_type_error(_("pool"),"set_pool_namefn",arg);
  if ((OIDP(method))||(SYMBOLP(method))||(FD_APPLICABLEP(method))) {
    fd_set_pool_namefn(p,method);
    return VOID;}
  else return fd_type_error(_("namefn"),"set_pool_namefn",method);
}

static lispval set_cache_level(lispval arg,lispval level)
{
  if (!(FD_UINTP(level)))
    return fd_type_error("fixnum","set_cache_level",level);
  else if (FD_POOLP(arg)) {
    fd_pool p = fd_lisp2pool(arg);
    if (p) fd_pool_setcache(p,FIX2INT(level));
    else return FD_ERROR;
    return VOID;}
  else if (INDEXP(arg)) {
    fd_index ix = fd_indexptr(arg);
    if (ix) fd_index_setcache(ix,FIX2INT(level));
    else return fd_type_error("index","index_frame_prim",arg);
    return VOID;}
  else return fd_type_error("pool or index","set_cache_level",arg);
}

static lispval try_pool(lispval arg1,lispval opts)
{
  if (load_db_module(opts,"try_pool")<0)
    return FD_ERROR;
  else  if ( (FD_POOLP(arg1)) || (TYPEP(arg1,fd_consed_pool_type)) )
    return fd_incref(arg1);
  else if (!(STRINGP(arg1)))
    return fd_type_error(_("string"),"load_pool",arg1);
  else {
    fd_storage_flags flags = fd_get_dbflags(opts,FD_STORAGE_ISPOOL) | 
      FD_STORAGE_NOERR;
    fd_pool p = fd_get_pool(CSTRING(arg1),flags,opts);
    if (p)
      return pool2lisp(p);
    else return FD_FALSE;}
}

static lispval adjunct_pool(lispval arg1,lispval opts)
{
  if (load_db_module(opts,"adjunct_pool")<0)
    return FD_ERROR;
  else if ( (FD_POOLP(arg1)) || (TYPEP(arg1,fd_consed_pool_type)) )
    // TODO: Should check it's really adjunct, if that's the right thing?
    return fd_incref(arg1);
  else if (!(STRINGP(arg1)))
    return fd_type_error(_("string"),"adjunct_pool",arg1);
  else {
    fd_storage_flags flags = fd_get_dbflags(opts,FD_STORAGE_ISPOOL) |
      FD_POOL_ADJUNCT;
    fd_pool p = fd_get_pool(CSTRING(arg1),flags,opts);
    if (p)
      return pool2lisp(p);
    else return FD_ERROR;}
}

static lispval use_pool(lispval arg1,lispval opts)
{
  if (load_db_module(opts,"use_pool")<0)
    return FD_ERROR;
  else if ( (FD_POOLP(arg1)) || (TYPEP(arg1,fd_consed_pool_type)) )
    // TODO: Should check to make sure that it's in the background
    return fd_incref(arg1);
  else if (!(STRINGP(arg1)))
    return fd_type_error(_("string"),"use_pool",arg1);
  else {
    fd_pool p = fd_get_pool(CSTRING(arg1),-1,opts);
    if (p) return pool2lisp(p);
    else return fd_err(fd_NoSuchPool,"use_pool",
                       CSTRING(arg1),VOID);}
}

static lispval use_index(lispval arg,lispval opts)
{
  fd_index ixresult = NULL;
  if (load_db_module(opts,"use_index")<0)
    return FD_ERROR;
  else if (INDEXP(arg)) {
    ixresult = fd_indexptr(arg);
    if (ixresult) fd_add_to_background(ixresult);
    else return fd_type_error("index","index_frame_prim",arg);
    return fd_incref(arg);}
  else if (STRINGP(arg))
    if (strchr(CSTRING(arg),';')) {
      /* We explicitly handle ; separated arguments here, so that
         we can return the choice of index. */
      lispval results = EMPTY;
      u8_byte *copy = u8_strdup(CSTRING(arg));
      u8_byte *start = copy, *end = strchr(start,';');
      *end='\0'; while (start) {
        fd_index ix = fd_use_index(start,
                                   fd_get_dbflags(opts,FD_STORAGE_ISINDEX),
                                   opts);
        if (ix) {
          lispval ixv = index_ref(ix);
          CHOICE_ADD(results,ixv);}
        else {
          u8_free(copy);
          fd_decref(results);
          return FD_ERROR;}
        if ((end) && (end[1])) {
          start = end+1; end = strchr(start,';');
          if (end) *end='\0';}
        else start = NULL;}
      u8_free(copy);
      return results;}
    else ixresult = fd_use_index
           (CSTRING(arg),fd_get_dbflags(opts,FD_STORAGE_ISINDEX),opts);
  else return fd_type_error(_("index spec"),"use_index",arg);
  if (ixresult)
    return index_ref(ixresult);
  else return FD_ERROR;
}

static lispval open_index_helper(lispval arg,lispval opts,int registered)
{
  fd_storage_flags flags = fd_get_dbflags(opts,FD_STORAGE_ISINDEX);
  fd_index ix = NULL;
  lispval modules = fd_getopt(opts,FDSYM_MODULE,FD_VOID);
  lispval mod = (FD_VOIDP(modules)) ? (FD_VOID) :
    (fd_get_module(modules,0));
  fd_decref(modules);
  if (FD_ABORTP(mod))
    return mod;
  else fd_decref(mod);
  if (registered == 0)
    flags |= FD_STORAGE_UNREGISTERED;
  else if (registered>0)
    flags &= ~FD_STORAGE_UNREGISTERED;
  else {}
  if (STRINGP(arg)) {
    if (strchr(CSTRING(arg),';')) {
      /* We explicitly handle ; separated arguments here, so that
         we can return the choice of index. */
      lispval results = EMPTY;
      u8_byte *copy = u8_strdup(CSTRING(arg));
      u8_byte *start = copy, *end = strchr(start,';');
      *end='\0'; while (start) {
        fd_index ix = fd_get_index(start,flags,opts);
        if (ix == NULL) {
          u8_free(copy);
          fd_decref(results);
          return FD_ERROR;}
        else {
          lispval ixv = index_ref(ix);
          CHOICE_ADD(results,ixv);}
        if ((end) && (end[1])) {
          start = end+1; end = strchr(start,';');
          if (end) *end='\0';}
        else start = NULL;}
      u8_free(copy);
      return results;}
    else return index_ref(fd_get_index(CSTRING(arg),flags,opts));}
  else if (FD_ETERNAL_INDEXP(arg))
    return arg;
  else if (FD_CONSED_INDEXP(arg))
    return fd_incref(arg);
  else fd_seterr(fd_TypeError,"use_index",NULL,fd_incref(arg));
  if (ix)
    return index_ref(ix);
  else return FD_ERROR;
}

static lispval open_index(lispval arg,lispval opts)
{
  if (load_db_module(opts,"open_index")<0)
    return FD_ERROR;
  else return open_index_helper(arg,opts,-1);
}

static lispval register_index(lispval arg,lispval opts)
{
  if (load_db_module(opts,"register_index")<0)
    return FD_ERROR;
  else return open_index_helper(arg,opts,1);
}

static lispval cons_index(lispval arg,lispval opts)
{
  if (load_db_module(opts,"cons_index")<0)
    return FD_ERROR;
  else return open_index_helper(arg,opts,0);
}

static lispval make_pool(lispval path,lispval opts)
{
  if (load_db_module(opts,"make_pool")<0) return FD_ERROR;
  fd_pool p = NULL;
  lispval type = fd_getopt(opts,FDSYM_TYPE,VOID);
  fd_storage_flags flags = fd_get_dbflags(opts,FD_STORAGE_ISPOOL);
  if (FD_VOIDP(type)) type = fd_getopt(opts,FDSYM_MODULE,VOID);
  if (VOIDP(type))
    p = fd_make_pool(CSTRING(path),NULL,flags,opts);
  else if (SYMBOLP(type))
    p = fd_make_pool(CSTRING(path),SYM_NAME(type),flags,opts);
  else if (STRINGP(type))
    p = fd_make_pool(CSTRING(path),CSTRING(type),flags,opts);
  else if (STRINGP(path))
    return fd_err(_("BadPoolType"),"make_pool",CSTRING(path),type);
  else return fd_err(_("BadPoolType"),"make_pool",NULL,type);
  fd_decref(type);
  if (p)
    return pool2lisp(p);
  else return FD_ERROR;
}

static lispval open_pool(lispval path,lispval opts)
{
  if (load_db_module(opts,"open_pool")<0) return FD_ERROR;
  fd_storage_flags flags = fd_get_dbflags(opts,FD_STORAGE_ISPOOL);
  fd_pool p = fd_open_pool(CSTRING(path),flags,opts);
  if (p)
    return pool2lisp(p);
  else return FD_ERROR;
}

static lispval make_index(lispval path,lispval opts)
{
  if (load_db_module(opts,"make_index")<0) return FD_ERROR;
  fd_index ix = NULL;
  lispval type = fd_getopt(opts,FDSYM_TYPE,VOID);
  fd_storage_flags flags =
    (FIXNUMP(opts)) ?
    (FD_STORAGE_ISINDEX) :
    (fd_get_dbflags(opts,FD_STORAGE_ISINDEX)) ;
  if (FD_VOIDP(type)) type = fd_getopt(opts,FDSYM_MODULE,VOID);
  if (VOIDP(type))
    ix = fd_make_index(CSTRING(path),NULL,flags,opts);
  else if (SYMBOLP(type))
    ix = fd_make_index(CSTRING(path),SYM_NAME(type),flags,opts);
  else if (STRINGP(type))
    ix = fd_make_index(CSTRING(path),CSTRING(type),flags,opts);
  else if (STRINGP(path))
    return fd_err(_("BadIndexType"),"make_index",CSTRING(path),type);
  else return fd_err(_("BadIndexType"),"make_index",NULL,type);
  fd_decref(type);
  if (ix)
    return index_ref(ix);
  else return FD_ERROR;
}

static lispval oidvalue(lispval arg)
{
  return fd_oid_value(arg);
}
static lispval setoidvalue(lispval o,lispval v,lispval nocopy)
{
  int retval;
  if (FD_TRUEP(nocopy)) {fd_incref(v);}
  else if (SLOTMAPP(v)) {
    v = fd_deep_copy(v);
    FD_SLOTMAP_MARK_MODIFIED(v);}
  else if (SCHEMAPP(v)) {
    v = fd_deep_copy(v);
    FD_SCHEMAP_MARK_MODIFIED(v);}
  else if (HASHTABLEP(v)) {
    v = fd_deep_copy(v);
    FD_HASHTABLE_MARK_MODIFIED(v);}
  else v = fd_incref(v);
  retval = fd_set_oid_value(o,v);
  fd_decref(v);
  if (retval<0)
    return FD_ERROR;
  else return VOID;
}
static lispval xsetoidvalue(lispval o,lispval v)
{
  int retval;
  fd_incref(v);
  retval = fd_replace_oid_value(o,v);
  fd_decref(v);
  if (retval<0)
    return FD_ERROR;
  else return VOID;
}

static lispval lockoid(lispval o,lispval soft)
{
  int retval = fd_lock_oid(o);
  if (retval<0)
    if (FD_TRUEP(soft)) {
      fd_poperr(NULL,NULL,NULL,NULL);
      return FD_FALSE;}
    else return FD_ERROR;
  else return FD_INT(retval);
}

static lispval oidlockedp(lispval arg)
{
  fd_pool p = fd_oid2pool(arg);
  if (fd_hashtable_probe_novoid(&(p->pool_changes),arg))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval lockoids(lispval oids)
{
  int retval = fd_lock_oids(oids);
  if (retval<0)
    return FD_ERROR;
  else return FD_INT(retval);
}

static lispval lockedoids(lispval pool)
{
  fd_pool p = fd_lisp2pool(pool);
  return fd_hashtable_keys(&(p->pool_changes));
}

static lispval unlockoids(lispval oids,lispval commitp)
{
  int force_commit = (VOIDP(commitp)) ? (0) :
    (FD_FALSEP(commitp)) ? (-1) :
    (1);
  if (VOIDP(oids)) {
    fd_unlock_pools(force_commit);
    return FD_FALSE;}
  else if ((TYPEP(oids,fd_pool_type))||(STRINGP(oids))) {
    fd_pool p = ((TYPEP(oids,fd_pool_type)) ? (fd_lisp2pool(oids)) :
               (fd_name2pool(CSTRING(oids))));
    if (p) {
      int retval = fd_pool_unlock_all(p,force_commit);
      if (retval<0) return FD_ERROR;
      else return FD_INT(retval);}
    else return fd_type_error("pool or OID","unlockoids",oids);}
  else {
    int retval = fd_unlock_oids(oids,force_commit);
    if (retval<0)
      return FD_ERROR;
    else return FD_INT(retval);}
}

static lispval make_aggregate_index(lispval sources,lispval opts)
{
  int n_sources = FD_CHOICE_SIZE(sources), n_partitions=0;
  if (n_sources == 0)
    return index2lisp((fd_index)fd_make_aggregate_index(opts,8,0,NULL));
  fd_index partitions[n_sources];
  FD_DO_CHOICES(source,sources) {
    fd_index ix = NULL;
    if (STRINGP(source))
      ix = fd_get_index(fd_strdata(source),0,VOID);
    else if (INDEXP(source))
      ix = fd_indexptr(source);
    else if (SYMBOLP(source)) {
      lispval val = fd_config_get(SYM_NAME(source));
      if (STRINGP(val)) ix = fd_get_index(FD_CSTRING(val),0,VOID);
      else if (INDEXP(val)) ix = fd_indexptr(val);
      else NO_ELSE;}
    else if ( (FD_PAIRP(source)) || (FD_SLOTMAPP(source)) ) {
      lispval spec = fd_getopt(source,FDSYM_SOURCE,FD_VOID);
      if (FD_STRINGP(spec))
        ix = fd_open_index(FD_CSTRING(spec),-1,source);
      fd_decref(spec);}
    else {}
    if (ix)
      partitions[n_partitions++] = ix;
    else {
      FD_STOP_DO_CHOICES;
      return fd_type_error("index","make_aggregate_index",source);}}
  int n_alloc = 8;
  while (n_partitions > n_alloc)
    n_alloc=n_alloc*2;
  fd_aggregate_index aggregate =
    fd_make_aggregate_index(opts,n_alloc,n_partitions,partitions);
  return index2lisp((fd_index)aggregate);
}

static lispval aggregate_indexp(lispval arg)
{
  fd_index ix = fd_indexptr(arg);
  if (ix == NULL)
    return FD_FALSE;
  else if (fd_aggregate_indexp(ix))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval extend_aggregate_index(lispval into_arg,lispval partition_arg)
{
  fd_index into = fd_indexptr(into_arg);
  fd_index partition = fd_indexptr(partition_arg);
  if (fd_aggregate_indexp(into)) {
    fd_aggregate_index aggregate = (fd_aggregate_index) into;
    if (partition) {
      if (partition->index_serialno < 0) fd_register_index(partition);
      if (partition->index_serialno > 0) {
        if (fd_add_to_aggregate_index(aggregate,partition)<0)
          return FD_ERROR;
        else return VOID;}
      else return fd_type_error(_("eternal (not-ephemeral) index"),
                                "add_to_aggregate_index",
                                partition_arg);}
    else return fd_type_error
           (_("index"),"add_to_aggregate_index",partition_arg);}
  else return fd_type_error(_("aggregate index"),"add_to_aggregate_index",
                            into_arg);
}

static lispval tempindexp(lispval arg)
{
  fd_index ix = fd_indexptr(arg);
  if (ix == NULL)
    return FD_FALSE;
  else if (fd_tempindexp(ix))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval make_mempool(lispval label,lispval base,lispval cap,
                            lispval load,lispval noswap,lispval opts)
{
  if (!(FD_UINTP(cap))) return fd_type_error("uint","make_mempool",cap);
  if (!(FD_UINTP(load))) return fd_type_error("uint","make_mempool",load);
  fd_pool p = fd_make_mempool
    (CSTRING(label),FD_OID_ADDR(base),
     FIX2INT(cap),
     FIX2INT(load),
     (!(FALSEP(noswap))),
     opts);
  if (p == NULL)
    return FD_ERROR;
  else return pool2lisp(p);
}

static lispval clean_mempool(lispval pool_arg)
{
  int retval = fd_clean_mempool(fd_lisp2pool(pool_arg));
  if (retval<0) return FD_ERROR;
  else return FD_INT(retval);
}

static lispval reset_mempool(lispval pool_arg)
{
  int retval = fd_reset_mempool(fd_lisp2pool(pool_arg));
  if (retval<0) return FD_ERROR;
  else return FD_INT(retval);
}

static lispval make_procpool(lispval label,
                             lispval base,lispval cap,
                             lispval opts,lispval state,
                             lispval load)
{
  if (load_db_module(opts,"make_procpool")<0) return FD_ERROR;
  if (!(FD_UINTP(cap)))
    return fd_type_error("uint","make_procpool",cap);
  if (FD_VOIDP(load))
    load=FD_FIXZERO;
  else if (load == FD_DEFAULT_VALUE)
    load=FD_FIXZERO;
  else if (!(FD_UINTP(load)))
    return fd_type_error("uint","make_procpool",load);
  fd_pool p = fd_make_procpool
    (FD_OID_ADDR(base),FIX2INT(cap),FIX2INT(load),
     opts,state,CSTRING(label),NULL);
return pool2lisp(p);
}

static lispval make_extpool(lispval label,lispval base,lispval cap,
                            lispval fetchfn,lispval savefn,
                            lispval lockfn,lispval allocfn,
                            lispval state,lispval cache,
                            lispval opts)
{
  if (!(FD_UINTP(cap))) return fd_type_error("uint","make_mempool",cap);
  fd_pool p = fd_make_extpool
    (CSTRING(label),FD_OID_ADDR(base),FIX2INT(cap),
     fetchfn,savefn,lockfn,allocfn,state,opts);
  if (FALSEP(cache)) fd_pool_setcache(p,0);
  return pool2lisp(p);
}

static lispval extpool_setcache(lispval pool,lispval oid,lispval value)
{
  fd_pool p = fd_lisp2pool(pool);
  if (fd_extpool_cache_value(p,oid,value)<0)
    return FD_ERROR;
  else return VOID;
}

static lispval extpool_fetchfn(lispval pool)
{
  fd_pool p = fd_lisp2pool(pool);
  if ( (p) && (p->pool_handler== &fd_extpool_handler) ) {
    struct FD_EXTPOOL *ep = (struct FD_EXTPOOL *)p;
    return fd_incref(ep->fetchfn);}
  else return fd_type_error("extpool","extpool_fetchfn",pool);
}

static lispval extpool_savefn(lispval pool)
{
  fd_pool p = fd_lisp2pool(pool);
  if ( (p) && (p->pool_handler== &fd_extpool_handler) ) {
    struct FD_EXTPOOL *ep = (struct FD_EXTPOOL *)p;
    return fd_incref(ep->savefn);}
  else return fd_type_error("extpool","extpool_savefn",pool);
}

static lispval extpool_lockfn(lispval pool)
{
  fd_pool p = fd_lisp2pool(pool);
  if ( (p) && (p->pool_handler== &fd_extpool_handler) ) {
    struct FD_EXTPOOL *ep = (struct FD_EXTPOOL *)p;
    return fd_incref(ep->lockfn);}
  else return fd_type_error("extpool","extpool_lockfn",pool);
}

static lispval extpool_state(lispval pool)
{
  fd_pool p = fd_lisp2pool(pool);
  if ( (p) && (p->pool_handler== &fd_extpool_handler) ) {
    struct FD_EXTPOOL *ep = (struct FD_EXTPOOL *)p;
    return fd_incref(ep->state);}
  else return fd_type_error("extpool","extpool_state",pool);
}

/* Proc indexes */

static lispval make_procindex(lispval id,
                              lispval opts,lispval state,
                              lispval source,lispval typeid)
{
  if (load_db_module(opts,"make_procindex")<0) return FD_ERROR;
  fd_index ix = fd_make_procindex
    (opts,state,FD_CSTRING(id),
     ((FD_VOIDP(source)) ? (NULL) : (FD_CSTRING(source))),
     ((FD_VOIDP(typeid)) ? (NULL) : (FD_CSTRING(typeid))));
  return index2lisp(ix);
}

/* External indexes */

static lispval make_extindex(lispval label,lispval fetchfn,lispval commitfn,
                             lispval state,lispval usecache,
                             lispval opts)
{
  fd_index ix = fd_make_extindex
    (CSTRING(label),
     ((FALSEP(fetchfn))?(VOID):(fetchfn)),
     ((FALSEP(commitfn))?(VOID):(commitfn)),
     ((FALSEP(state))?(VOID):(state)),
     -1,
     opts);
  if (FALSEP(usecache)) fd_index_setcache(ix,0);
  return index2lisp(ix);
}

static lispval cons_extindex(lispval label,lispval fetchfn,lispval commitfn,
                             lispval state,lispval usecache,lispval opts)
{
  fd_index ix = fd_make_extindex
    (CSTRING(label),
     ((FALSEP(fetchfn))?(VOID):(fetchfn)),
     ((FALSEP(commitfn))?(VOID):(commitfn)),
     ((FALSEP(state))?(VOID):(state)),
     -1,
     opts);
  if (FALSEP(usecache)) fd_index_setcache(ix,0);
  if (ix->index_serialno>=0) return index_ref(ix);
  else return (lispval)ix;
}

static lispval extindex_cacheadd(lispval index,lispval key,lispval values)
{
  FDTC *fdtc = fd_threadcache;
  fd_index ix = fd_indexptr(index);
  if ( (ix) && (ix->index_handler == &fd_extindex_handler) )
    if (fd_hashtable_add(&(ix->index_cache),key,values)<0)
      return FD_ERROR;
    else {}
  else return fd_type_error("extindex","extindex_cacheadd",index);
  if (fdtc) {
    struct FD_PAIR tempkey;
    struct FD_HASHTABLE *h = &(fdtc->indexes);
    FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
    tempkey.car = index2lisp(ix); tempkey.cdr = key;
    if (fd_hashtable_probe(h,(lispval)&tempkey)) {
      fd_hashtable_store(h,(lispval)&tempkey,VOID);}}
  return VOID;
}

static lispval extindex_decache(lispval index,lispval key)
{
  FDTC *fdtc = fd_threadcache;
  fd_index ix = fd_indexptr(index);
  lispval lix = index2lisp(ix);
  if ( (ix) && (ix->index_handler == &fd_extindex_handler) )
    if (VOIDP(key))
      if (fd_reset_hashtable(&(ix->index_cache),ix->index_cache.ht_n_buckets,1)<0)
        return FD_ERROR;
      else {}
    else if (fd_hashtable_store(&(ix->index_cache),key,VOID)<0)
      return FD_ERROR;
    else {}
  else return fd_type_error("extindex","extindex_decache",index);
  if ((fdtc)&&(!(VOIDP(key)))) {
    struct FD_PAIR tempkey;
    struct FD_HASHTABLE *h = &(fdtc->indexes);
    FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
    tempkey.car = index2lisp(ix); tempkey.cdr = key;
    if (fd_hashtable_probe(h,(lispval)&tempkey)) {
      fd_hashtable_store(h,(lispval)&tempkey,VOID);}}
  else if (fdtc) {
    struct FD_HASHTABLE *h = &(fdtc->indexes);
    lispval keys = fd_hashtable_keys(h), drop = EMPTY;
    DO_CHOICES(key,keys) {
      if ((PAIRP(key))&&(FD_CAR(key) == lix)) {
        fd_incref(key); CHOICE_ADD(drop,key);}}
    if (!(EMPTYP(drop))) {
      DO_CHOICES(d,drop) fd_hashtable_drop(h,d,VOID);}
    fd_decref(drop); fd_decref(keys);}
  else {}
return VOID;
}

static lispval extindex_fetchfn(lispval index)
{
  fd_index ix = fd_indexptr(index);
  if ( (ix) && (ix->index_handler== &fd_extindex_handler) ) {
    struct FD_EXTINDEX *eix = (struct FD_EXTINDEX *)ix;
    return fd_incref(eix->fetchfn);}
  else return fd_type_error("extindex","extindex_fetchfn",index);
}

static lispval extindex_commitfn(lispval index)
{
  fd_index ix = fd_indexptr(index);
  if ( (ix) && (ix->index_handler== &fd_extindex_handler) ) {
    struct FD_EXTINDEX *eix = (struct FD_EXTINDEX *)ix;
    return fd_incref(eix->commitfn);}
  else return fd_type_error("extindex","extindex_commitfn",index);
}

static lispval extindex_state(lispval index)
{
  fd_index ix = fd_indexptr(index);
  if ( (ix) && (ix->index_handler== &fd_extindex_handler) ) {
    struct FD_EXTINDEX *eix = (struct FD_EXTINDEX *)ix;
    return fd_incref(eix->state);}
  else return fd_type_error("extindex","extindex_state",index);
}

static lispval extindexp(lispval index)
{
  fd_index ix = fd_indexptr(index);
  if ( (ix) && (ix->index_handler== &fd_extindex_handler) )
    return FD_TRUE;
  else return FD_FALSE;
}

/* Adding adjuncts */

static lispval padjuncts_symbol;

static lispval use_adjunct(lispval adjunct,lispval slotid,lispval pool_arg)
{
  if (STRINGP(adjunct)) {
    fd_index ix = fd_get_index(CSTRING(adjunct),0,VOID);
    if (ix) adjunct = index2lisp(ix);
    else return fd_type_error("adjunct spec","use_adjunct",adjunct);}
  if ((VOIDP(slotid)) && (TABLEP(adjunct)))
    slotid = fd_get(adjunct,padjuncts_symbol,VOID);
  if ((SYMBOLP(slotid)) || (OIDP(slotid)))
    if (VOIDP(pool_arg))
      if (fd_set_adjunct(NULL,slotid,adjunct)<0)
        return FD_ERROR;
      else return VOID;
    else {
      fd_pool p = fd_lisp2pool(pool_arg);
      if (p == NULL) return FD_ERROR;
      else if (fd_set_adjunct(p,slotid,adjunct)<0)
        return FD_ERROR;
      else return VOID;}
  else return fd_type_error(_("slotid"),"use_adjunct",slotid);
}

static lispval add_adjunct(lispval pool_arg,lispval slotid,lispval adjunct)
{
  if (STRINGP(adjunct)) {
    fd_index ix = fd_get_index(CSTRING(adjunct),0,VOID);
    if (ix) adjunct = index2lisp(ix);
    else return fd_type_error("adjunct spec","use_adjunct",adjunct);}
  if ((VOIDP(slotid)) && (TABLEP(adjunct)))
    slotid = fd_get(adjunct,padjuncts_symbol,VOID);
  if ((SYMBOLP(slotid)) || (OIDP(slotid))) {
    fd_pool p = fd_lisp2pool(pool_arg);
    if (p == NULL)
      return FD_ERROR;
    else if (fd_set_adjunct(p,slotid,adjunct)<0)
      return FD_ERROR;
    else return VOID;}
  else return fd_type_error(_("slotid"),"use_adjunct",slotid);
}

static lispval get_adjuncts(lispval pool_arg)
{
  fd_pool p=fd_lisp2pool(pool_arg);
  if (p==NULL)
    return FD_ERROR;
  else return fd_get_adjuncts(p);
}

static lispval isadjunctp(lispval pool_arg)
{
  fd_pool p=fd_lisp2pool(pool_arg);
  if (p==NULL)
    return FD_ERROR;
  else if ((p->pool_flags) & (FD_POOL_ADJUNCT))
    return FD_TRUE;
  else return FD_FALSE;
}

/* DB control functions */

static lispval swapout_lexpr(int n,lispval *args)
{
  if (n == 0) {
    fd_swapout_indexes();
    fd_swapout_pools();
    return VOID;}
  else if (n == 1) {
    long long rv_sum = 0;
    lispval arg = args[0];
    if (FD_EMPTY_CHOICEP(arg)) {}
    else if (CHOICEP(arg)) {
      int rv = 0;
      lispval oids = EMPTY;
      DO_CHOICES(e,arg) {
        if (OIDP(e)) {CHOICE_ADD(oids,e);}
        else if (FD_POOLP(e))
          rv = fd_pool_swapout(fd_lisp2pool(e),VOID);
        else if (INDEXP(e))
          fd_index_swapout(fd_indexptr(e),VOID);
        else if (TYPEP(arg,fd_consed_pool_type))
          rv = fd_pool_swapout((fd_pool)arg,VOID);
        else if (STRINGP(e)) {
          fd_pool p = fd_name2pool(CSTRING(e));
          if (!(p)) {
            fd_decref(oids);
            return fd_type_error(_("pool, index, or OIDs"),
                                 "swapout_lexpr",e);}
          else rv = fd_pool_swapout(p,VOID);}
        else {
          fd_decref(oids);
          return fd_type_error(_("pool, index, or OIDs"),
                               "swapout_lexpr",e);}
        if (rv<0) {
          u8_log(LOG_WARN,"SwapoutFailed","Error swapping out %q",e);
          fd_clear_errors(1);}
        else rv_sum = rv_sum+rv;}
      fd_swapout_oids(oids);
      fd_decref(oids);
      return FD_INT(rv_sum);}
    else if (OIDP(arg))
      rv_sum = fd_swapout_oid(arg);
    else if (TYPEP(arg,fd_index_type))
      fd_index_swapout(fd_indexptr(arg),VOID);
    else if (TYPEP(arg,fd_pool_type))
      rv_sum = fd_pool_swapout(fd_lisp2pool(arg),VOID);
    else if (TYPEP(arg,fd_consed_index_type))
      fd_index_swapout(fd_indexptr(arg),VOID);
    else if (TYPEP(arg,fd_consed_pool_type))
      rv_sum = fd_pool_swapout((fd_pool)arg,VOID);
    else return fd_type_error(_("pool, index, or OIDs"),"swapout_lexpr",arg);
    return FD_INT(rv_sum);}
  else if (n>2)
    return fd_err(fd_TooManyArgs,"swapout",NULL,VOID);
  else if (FD_EMPTY_CHOICEP(args[1]))
    return FD_INT(0);
  else {
    lispval arg, keys; int rv_sum = 0;
    if ((TYPEP(args[0],fd_pool_type))||
        (TYPEP(args[0],fd_index_type))||
        (TYPEP(args[0],fd_consed_pool_type))||
        (TYPEP(args[0],fd_consed_index_type))) {
      arg = args[0]; keys = args[1];}
    else {arg = args[0]; keys = args[1];}
    if (TYPEP(arg,fd_index_type))
      fd_index_swapout(fd_indexptr(arg),keys);
    else if (TYPEP(arg,fd_pool_type))
      rv_sum = fd_pool_swapout(fd_lisp2pool(arg),keys);
    else if (TYPEP(arg,fd_consed_index_type))
      fd_index_swapout(fd_indexptr(arg),keys);
    else if (TYPEP(arg,fd_consed_pool_type))
      rv_sum = fd_pool_swapout((fd_pool)arg,keys);
    else return fd_type_error(_("pool, index, or OIDs"),"swapout_lexpr",arg);
    if (rv_sum<0) return FD_ERROR;
    else return FD_INT(rv_sum);}
}

static lispval commit_lexpr(int n,lispval *args)
{
  if (n == 0) {
    if (fd_commit_indexes()<0)
      return FD_ERROR;
    if (fd_commit_pools()<0)
      return FD_ERROR;
    return VOID;}
  else if (n == 1) {
    lispval arg = args[0]; int retval = 0;
    if (TYPEP(arg,fd_index_type))
      retval = fd_commit_index(fd_indexptr(arg));
    else if (TYPEP(arg,fd_pool_type))
      retval = fd_commit_all_oids(fd_lisp2pool(arg));
    else if (TYPEP(arg,fd_consed_index_type))
      retval = fd_commit_index(fd_indexptr(arg));
    else if (TYPEP(arg,fd_consed_pool_type))
      retval = fd_commit_all_oids((fd_pool)arg);
    else if (OIDP(arg))
      retval = fd_commit_oids(arg);
    else return fd_type_error(_("pool or index"),"commit_lexpr",arg);
    if (retval<0) return FD_ERROR;
    else return VOID;}
  else return fd_err(fd_TooManyArgs,"commit",NULL,VOID);
}

static lispval commit_oids(lispval oids)
{
  int rv = fd_commit_oids(oids);
  if (rv<0)
    return FD_ERROR;
  else return VOID;
}

static lispval finish_oids(lispval oids,lispval pool)
{
  fd_pool p = (VOIDP(pool))? (NULL) : (fd_lisp2pool(pool));
  if (EMPTYP(oids)) return VOID;
  else if (p) {
    int rv = fd_pool_finish(p,oids);
    if (rv<0)
      return FD_ERROR;
    else return VOID;}
  else {
    int rv = fd_finish_oids(oids);
    if (rv<0)
      return FD_ERROR;
    else return VOID;}
}

static lispval commit_pool(lispval pool,lispval opts)
{
  fd_pool p = fd_lisp2pool(pool);
  if (!(p))
    return fd_type_error("pool","commit_pool",pool);
  else {
    int rv = fd_commit_pool(p,VOID);
    if (rv<0)
      return FD_ERROR;
    else return VOID;}
}

static lispval commit_finished(lispval pool)
{
  fd_pool p = fd_lisp2pool(pool);
  if (!(p))
    return fd_type_error("pool","commit_finished",pool);
  else {
    int rv = fd_commit_pool(p,FD_TRUE);
    if (rv<0)
      return FD_ERROR;
    else return VOID;}
}

static lispval pool_storen_prim(lispval pool,lispval oids,lispval values)
{
  fd_pool p = fd_lisp2pool(pool);
  if (!(p))
    return fd_type_error("pool","pool_storen_prim",pool);

  long long oid_len = FD_VECTOR_LENGTH(oids);
  if (FD_VECTOR_LENGTH(oids) != FD_VECTOR_LENGTH(values)) {
    long long v_len = FD_VECTOR_LENGTH(values);
    lispval intpair = fd_make_pair(FD_INT(oid_len),FD_INT(v_len));
    lispval rlv=fd_err("OIDs/Values mismatch","pool_storen_prim",p->poolid,
                       intpair);
    fd_decref(intpair);
    return rlv;}
  int rv = fd_pool_storen(p,FD_VECTOR_LENGTH(oids),
                          FD_VECTOR_ELTS(oids),
                          FD_VECTOR_ELTS(values));
  if (rv<0)
    return FD_ERROR_VALUE;
  else return FD_INT(oid_len);
}

static lispval pool_fetchn_prim(lispval pool,lispval oids)
{
  fd_pool p = fd_lisp2pool(pool);
  if (!(p))
    return fd_type_error("pool","pool_fetchn_prim",pool);

  return fd_pool_fetchn(p,oids);
}

static lispval clear_slotcache(lispval arg)
{
  if (VOIDP(arg)) fd_clear_slotcaches();
  else fd_clear_slotcache(arg);
  return VOID;
}

static lispval clearcaches()
{
  fd_clear_callcache(VOID);
  fd_clear_slotcaches();
  fd_swapout_indexes();
  fd_swapout_pools();
  return VOID;
}

static lispval swapcheck_prim()
{
  if (fd_swapcheck()) return FD_TRUE;
  else return FD_FALSE;
}

static fd_pool arg2pool(lispval arg)
{
  if (FD_POOLP(arg)) return fd_lisp2pool(arg);
  else if (TYPEP(arg,fd_consed_pool_type))
    return (fd_pool)arg;
  else if (STRINGP(arg)) {
    fd_pool p = fd_name2pool(CSTRING(arg));
    if (p) return p;
    else return fd_use_pool(CSTRING(arg),0,VOID);}
  else if (SYMBOLP(arg)) {
    lispval v = fd_config_get(SYM_NAME(arg));
    if (STRINGP(v))
      return fd_use_pool(CSTRING(v),0,VOID);
    else return NULL;}
  else return NULL;
}

static lispval pool_load(lispval arg)
{
  fd_pool p = arg2pool(arg);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"pool_load",arg);
  else {
    int load = fd_pool_load(p);
    if (load>=0) return FD_INT(load);
    else return FD_ERROR;}
}

static lispval pool_capacity(lispval arg)
{
  fd_pool p = arg2pool(arg);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"pool_capacity",arg);
  else return FD_INT(p->pool_capacity);
}

static lispval pool_base(lispval arg)
{
  fd_pool p = arg2pool(arg);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"pool_base",arg);
  else return fd_make_oid(p->pool_base);
}

static lispval pool_elts(lispval arg,lispval start,lispval count)
{
  fd_pool p = arg2pool(arg);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"pool_elts",arg);
  else {
    int i = 0, lim = fd_pool_load(p);
    lispval result = EMPTY;
    FD_OID base = p->pool_base;
    if (lim<0) return FD_ERROR;
    if (VOIDP(start)) {}
    else if (FD_UINTP(start))
      if (FIX2INT(start)<0)
        return fd_type_error(_("pool offset"),"pool_elts",start);
      else i = FIX2INT(start);
    else if (OIDP(start))
      i = FD_OID_DIFFERENCE(FD_OID_ADDR(start),base);
    else return fd_type_error(_("pool offset"),"pool_elts",start);
    if (VOIDP(count)) {}
    else if (FD_UINTP(count)) {
      int count_arg = FIX2INT(count);
      if (count_arg<0)
        return fd_type_error(_("pool offset"),"pool_elts",count);
      else if (i+count_arg<lim) lim = i+count_arg;}
    else if (OIDP(start)) {
      int lim_arg = FD_OID_DIFFERENCE(FD_OID_ADDR(count),base);
      if (lim_arg<lim) lim = lim_arg;}
    else return fd_type_error(_("pool offset"),"pool_elts",count);
    int off=i, partition=-1, bucket_no=-1, bucket_start=-1;
    while (off<lim) {
      if ( (partition<0) || ((off/FD_OID_BUCKET_SIZE) != partition) ) {
        FD_OID addr=FD_OID_PLUS(base,off);
        bucket_no=fd_get_oid_base_index(addr,1);
        partition=off/FD_OID_BUCKET_SIZE;
        bucket_start=off;}
      lispval each=FD_CONSTRUCT_OID(bucket_no,off-bucket_start);
      CHOICE_ADD(result,each);
      off++;}
    return result;}
}

static lispval pool_label(lispval arg,lispval use_source)
{
  fd_pool p = arg2pool(arg);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"pool_label",arg);
  else if (p->pool_label)
    return lispval_string(p->pool_label);
  else if (FALSEP(use_source)) return FD_FALSE;
  else if (p->pool_source)
    return lispval_string(p->pool_source);
  else return FD_FALSE;
}

static lispval pool_id(lispval arg)
{
  fd_pool p = arg2pool(arg);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"pool_id",arg);
  else if (p->pool_label)
    return lispval_string(p->pool_label);
  else if (p->poolid)
    return lispval_string(p->poolid);
  else if (p->pool_source)
    return lispval_string(p->pool_source);
  else if (p->pool_prefix)
    return lispval_string(p->pool_prefix);
  else return FD_FALSE;
}

static lispval pool_source(lispval arg)
{
  fd_pool p = arg2pool(arg);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"pool_label",arg);
  else if (p->pool_source)
    return lispval_string(p->pool_source);
  else if (p->poolid)
    return lispval_string(p->pool_source);
  else return FD_FALSE;
}

static lispval pool_prefix(lispval arg)
{
  fd_pool p = arg2pool(arg);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"pool_label",arg);
  else if (p->pool_prefix)
    return lispval_string(p->pool_prefix);
  else return FD_FALSE;
}

static lispval set_pool_prefix(lispval arg,lispval prefix_arg)
{
  fd_pool p = arg2pool(arg);
  u8_string prefix = CSTRING(prefix_arg);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"set_pool_prefix",arg);
  else if ((p->pool_prefix)&&(strcmp(p->pool_prefix,prefix)==0))
    return FD_FALSE;
  else if (p->pool_prefix) {
    u8_string copy = u8_strdup(prefix);
    u8_string cur = p->pool_prefix;
    p->pool_prefix = copy;
    return fd_init_string(NULL,-1,cur);}
  else {
    u8_string copy = u8_strdup(prefix);
    p->pool_prefix = copy;
    return FD_TRUE;}
}

static lispval pool_close_prim(lispval arg)
{
  fd_pool p = arg2pool(arg);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"pool_close",arg);
  else {
    fd_pool_close(p);
    return VOID;}
}

static lispval oid_range(lispval start,lispval end)
{
  int i = 0, lim = fd_getint(end);
  lispval result = EMPTY;
  FD_OID base = FD_OID_ADDR(start);
  if (lim<0) return FD_ERROR;
  else while (i<lim) {
    lispval each = fd_make_oid(FD_OID_PLUS(base,i));
    CHOICE_ADD(result,each); i++;}
  return result;
}

static lispval oid_vector(lispval start,lispval end)
{
  int i = 0, lim = fd_getint(end);
  if (lim<0) return FD_ERROR;
  else {
    lispval result = fd_empty_vector(lim);
    lispval *data = VEC_DATA(result);
    FD_OID base = FD_OID_ADDR(start);
    while (i<lim) {
      lispval each = fd_make_oid(FD_OID_PLUS(base,i));
      data[i++]=each;}
    return result;}
}

static lispval random_oid(lispval arg)
{
  fd_pool p = arg2pool(arg);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"random_oid",arg);
  else {
    int load = fd_pool_load(p);
    if (load>0) {
      FD_OID base = p->pool_base; int i = u8_random(load);
      return fd_make_oid(FD_OID_PLUS(base,i));}
    fd_seterr("No OIDs","random_oid",p->poolid,arg);
    return FD_ERROR;}
}

static lispval pool_vec(lispval arg)
{
  fd_pool p = arg2pool(arg);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"pool_vec",arg);
  else {
    int i = 0, lim = fd_pool_load(p);
    if (lim<0) return FD_ERROR;
    else {
      lispval result = fd_empty_vector(lim);
      FD_OID base = p->pool_base;
      if (lim<0) {
        fd_seterr("No OIDs","pool_vec",p->poolid,arg);
        return FD_ERROR;}
      else while (i<lim) {
          lispval each = fd_make_oid(FD_OID_PLUS(base,i));
          FD_VECTOR_SET(result,i,each); i++;}
      return result;}}
}

static lispval cachecount(lispval arg)
{
  fd_pool p = NULL; fd_index ix = NULL;
  if (VOIDP(arg)) {
    int count = fd_object_cache_load()+fd_index_cache_load();
    return FD_INT(count);}
  else if (FD_EQ(arg,pools_symbol)) {
    int count = fd_object_cache_load();
    return FD_INT(count);}
  else if (FD_EQ(arg,indexes_symbol)) {
    int count = fd_index_cache_load();
    return FD_INT(count);}
  else if ((p = (fd_lisp2pool(arg)))) {
    int count = p->pool_cache.table_n_keys;
    return FD_INT(count);}
  else if ((ix = (fd_indexptr(arg)))) {
    int count = ix->index_cache.table_n_keys;
    return FD_INT(count);}
  else return fd_type_error(_("pool or index"),"cachecount",arg);
}

/* OID functions */

static lispval oidhi(lispval x)
{
  FD_OID addr = FD_OID_ADDR(x);
  return FD_INT(FD_OID_HI(addr));
}

static lispval oidlo(lispval x)
{
  FD_OID addr = FD_OID_ADDR(x);
  return FD_INT(FD_OID_LO(addr));
}

static lispval oidp(lispval x)
{
  if (OIDP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static lispval oidpool(lispval x)
{
  fd_pool p = fd_oid2pool(x);
  if (p == NULL) return EMPTY;
  else return pool2lisp(p);
}

static lispval inpoolp(lispval x,lispval pool_arg)
{
  fd_pool p = fd_lisp2pool(pool_arg);
  if ( (p->pool_flags) & (FD_POOL_ADJUNCT) ) {
    FD_OID base = p->pool_base;
    FD_OID edge = FD_OID_PLUS(base,p->pool_capacity);
    FD_OID addr = FD_OID_ADDR(x);
    if ( (addr >= base) && (addr < edge) )
      return FD_TRUE;
    else return FD_FALSE;}
  else {
    fd_pool op = fd_oid2pool(x);
    if (p == op)
      return FD_TRUE;
    else return FD_FALSE;}
}

static lispval validoidp(lispval x,lispval pool_arg)
{
  if (VOIDP(pool_arg)) {
    fd_pool p = fd_oid2pool(x);
    if (p == NULL) return FD_FALSE;
    else {
      FD_OID base = p->pool_base, addr = FD_OID_ADDR(x);
      unsigned int offset = FD_OID_DIFFERENCE(addr,base);
      int load = fd_pool_load(p);
      if (load<0) return FD_ERROR;
      else if (offset<load) return FD_TRUE;
      else return FD_FALSE;}}
  else {
    fd_pool p = fd_lisp2pool(pool_arg);
    fd_pool op = fd_oid2pool(x);
    if (p == op) {
      FD_OID base = p->pool_base, addr = FD_OID_ADDR(x);
      unsigned int offset = FD_OID_DIFFERENCE(addr,base);
      int load = fd_pool_load(p);
      if (load<0) return FD_ERROR;
      else if (offset<load) return FD_TRUE;
      else return FD_FALSE;}
    else return FD_FALSE;}
}

/* Prefetching functions */

static lispval pool_prefetch_prim(lispval pool,lispval oids)
{
  if ( (VOIDP(pool)) || (FD_FALSEP(pool)) || (FD_TRUEP(pool)) ) {
    if (fd_prefetch_oids(oids)>=0)
      return FD_TRUE;
    else return FD_ERROR;}
  else if (! ( (CHOICEP(pool)) || (PRECHOICEP(pool)) ) ) {
    fd_pool p=fd_lisp2pool(pool);
    if (p==NULL)
      return FD_ERROR_VALUE;
    else if (fd_pool_prefetch(p,oids)>=0)
      return FD_TRUE;
    else return FD_ERROR;}
  else {
    {FD_DO_CHOICES(spec,pool) {
        fd_pool p=fd_lisp2pool(spec);
        if (p==NULL) {
          FD_STOP_DO_CHOICES;
          return FD_ERROR;}
        else {}}}
    {int ok=0;
      FD_DO_CHOICES(spec,pool) {
        fd_pool p=fd_lisp2pool(spec);
        int rv=fd_pool_prefetch(p,oids);
        if (rv<0) ok=rv;}
      return (ok) ? (FD_TRUE) : (FD_FALSE);}}
}

static lispval prefetch_oids_prim(lispval oids,lispval parg)
{
  if ( (VOIDP(parg)) || (FD_FALSEP(parg)) || (FD_TRUEP(parg)) ) {
    if (fd_prefetch_oids(oids)>=0)
      return FD_TRUE;
    else return FD_ERROR;}
  else return pool_prefetch_prim(parg,oids);
}

static lispval fetchoids_prim(lispval oids)
{
  fd_prefetch_oids(oids);
  return fd_incref(oids);
}

static lispval prefetch_keys(lispval arg1,lispval arg2)
{
  if (VOIDP(arg2)) {
    if (fd_bg_prefetch(arg1)<0)
      return FD_ERROR;
    else return VOID;}
  else {
    DO_CHOICES(arg,arg1) {
      if (INDEXP(arg)) {
        fd_index ix = fd_indexptr(arg);
        if (fd_index_prefetch(ix,arg2)<0) {
          FD_STOP_DO_CHOICES;
          return FD_ERROR;}}
      else return fd_type_error(_("index"),"prefetch_keys",arg);}
    return VOID;}
}

static lispval index_prefetch_keys(lispval ix_arg,lispval keys)
{
  DO_CHOICES(arg,ix_arg) {
    if (INDEXP(arg)) {
      fd_index ix = fd_indexptr(arg);
      if (fd_index_prefetch(ix,keys)<0) {
        FD_STOP_DO_CHOICES;
        return FD_ERROR;}}
    else return fd_type_error(_("index"),"prefetch_keys",arg);}
  return FD_VOID;
}

/* Getting cached OIDs */

static lispval cached_oids(lispval pool)
{
  if ((VOIDP(pool)) || (FD_TRUEP(pool)))
    return fd_cached_oids(NULL);
  else {
    fd_pool p = fd_lisp2pool(pool);
    if (p)
      return fd_cached_oids(p);
    else return fd_type_error(_("pool"),"cached_oids",pool);}
}

static lispval cached_keys(lispval index)
{
  if ((VOIDP(index)) || (FD_TRUEP(index)))
    return fd_cached_keys(NULL);
  else {
    fd_index ix = fd_indexptr(index);
    if (ix)
      return fd_cached_keys(ix);
    else return fd_type_error(_("index"),"cached_keys",index);}
}

static lispval cache_load(lispval db)
{
  if ( (FD_POOLP(db)) || (TYPEP(db,fd_consed_pool_type) ) ) {
    fd_pool p = fd_lisp2pool(db);
    int n_keys = p->pool_cache.table_n_keys;
    return FD_INT(n_keys);}
  else if (INDEXP(db)) {
    fd_index ix = fd_lisp2index(db);
    int n_keys = ix->index_cache.table_n_keys;
    return FD_INT(n_keys);}
  else return fd_err(fd_TypeError,"cache_load",_("pool or index"),db);
}

static lispval change_load(lispval db)
{
  if ( (FD_POOLP(db)) || (TYPEP(db,fd_consed_pool_type) ) ) {
    fd_pool p = fd_lisp2pool(db);
    int n_pending = p->pool_changes.table_n_keys;
    return FD_INT(n_pending);}
  else if (INDEXP(db)) {
    fd_index ix = fd_lisp2index(db);
    int n_pending = ix->index_adds.table_n_keys +
      ix->index_drops.table_n_keys +
      ix->index_stores.table_n_keys ;
    return FD_INT(n_pending);}
  else return fd_err(fd_TypeError,"change_load",_("pool or index"),db);
}

/* Frame get functions */

FD_EXPORT
lispval fd_fget(lispval frames,lispval slotids)
{
  if (!(CHOICEP(frames)))
    if (!(CHOICEP(slotids)))
      if (OIDP(frames))
        return fd_frame_get(frames,slotids);
      else return fd_get(frames,slotids,EMPTY);
    else if (OIDP(frames)) {
      lispval result = EMPTY;
      DO_CHOICES(slotid,slotids) {
        lispval value = fd_frame_get(frames,slotid);
        if (FD_ABORTED(value)) {
          fd_decref(result);
          return value;}
        CHOICE_ADD(result,value);}
      return result;}
    else {
      lispval result = EMPTY;
      DO_CHOICES(slotid,slotids) {
        lispval value = fd_get(frames,slotid,EMPTY);
        if (FD_ABORTED(value)) {
          fd_decref(result);
          return value;}
        CHOICE_ADD(result,value);}
      return result;}
  else {
    int all_adjuncts = 1;
    if (CHOICEP(slotids)) {
      DO_CHOICES(slotid,slotids) {
        int adjunctp = 0;
        DO_CHOICES(adjslotid,fd_adjunct_slotids) {
          if (FD_EQ(slotid,adjslotid)) {adjunctp = 1; break;}}
        if (adjunctp==0) {all_adjuncts = 0; break;}}}
    else {
      int adjunctp = 0;
      DO_CHOICES(adjslotid,fd_adjunct_slotids) {
        if (FD_EQ(slotids,adjslotid)) {adjunctp = 1; break;}}
      if (adjunctp==0) all_adjuncts = 0;}
    if ((fd_prefetch) && (fd_ipeval_status()==0) &&
        (CHOICEP(frames)) && (all_adjuncts==0))
      fd_prefetch_oids(frames);
    {
      lispval results = EMPTY;
      DO_CHOICES(frame,frames)
        if (OIDP(frame)) {
          DO_CHOICES(slotid,slotids) {
            lispval v = fd_frame_get(frame,slotid);
            if (FD_ABORTED(v)) {
              FD_STOP_DO_CHOICES;
              fd_decref(results);
              return v;}
            else {CHOICE_ADD(results,v);}}}
        else {
          DO_CHOICES(slotid,slotids) {
            lispval v = fd_get(frame,slotid,EMPTY);
            if (FD_ABORTED(v)) {
              FD_STOP_DO_CHOICES;
              fd_decref(results);
              return v;}
            else {CHOICE_ADD(results,v);}}}
      return fd_simplify_choice(results);}}
}

FD_EXPORT
lispval fd_ftest(lispval frames,lispval slotids,lispval values)
{
  if (EMPTYP(frames))
    return FD_FALSE;
  else if ((!(CHOICEP(frames))) && (!(OIDP(frames))))
    if (CHOICEP(slotids)) {
      int found = 0;
      DO_CHOICES(slotid,slotids)
        if (fd_test(frames,slotid,values)) {
          found = 1; FD_STOP_DO_CHOICES; break;}
        else {}
      if (found) return FD_TRUE;
      else return FD_FALSE;}
    else {
      int testval = fd_test(frames,slotids,values);
      if (testval<0) return FD_ERROR;
      else if (testval) return FD_TRUE;
      else return FD_FALSE;}
  else {
    DO_CHOICES(frame,frames) {
      DO_CHOICES(slotid,slotids) {
        DO_CHOICES(value,values)
          if (OIDP(frame)) {
            int result = fd_frame_test(frame,slotid,value);
            if (result<0) return FD_ERROR;
            else if (result) return FD_TRUE;}
          else {
            int result = fd_test(frame,slotid,value);
            if (result<0) return FD_ERROR;
            else if (result) return FD_TRUE;}}}
    return FD_FALSE;}
}

FD_EXPORT
lispval fd_assert(lispval frames,lispval slotids,lispval values)
{
  if (EMPTYP(values)) return VOID;
  else {
    DO_CHOICES(frame,frames) {
      DO_CHOICES(slotid,slotids) {
        DO_CHOICES(value,values) {
          if (fd_frame_add(frame,slotid,value)<0)
            return FD_ERROR;}}}
    return VOID;}
}
FD_EXPORT
lispval fd_retract(lispval frames,lispval slotids,lispval values)
{
  if (EMPTYP(values)) return VOID;
  else {
    DO_CHOICES(frame,frames) {
      DO_CHOICES(slotid,slotids) {
        if (VOIDP(values)) {
          lispval values = fd_frame_get(frame,slotid);
          DO_CHOICES(value,values) {
            if (fd_frame_drop(frame,slotid,value)<0) {
              fd_decref(values);
              return FD_ERROR;}}
          fd_decref(values);}
        else {
          DO_CHOICES(value,values) {
            if (fd_frame_drop(frame,slotid,value)<0)
              return FD_ERROR;}}}}
    return VOID;}
}

static lispval testp(int n,lispval *args)
{
  lispval frames = args[0], slotids = args[1], testfns = args[2];
  if ((EMPTYP(frames)) || (EMPTYP(slotids)))
    return FD_FALSE;
  else {
    DO_CHOICES(frame,frames) {
      DO_CHOICES(slotid,slotids) {
        lispval values = fd_fget(frame,slotid);
        DO_CHOICES(testfn,testfns)
          if (FD_APPLICABLEP(testfn)) {
            lispval test_result = FD_FALSE;
            args[2]=values;
            test_result = fd_apply(testfn,n-2,args+2);
            args[2]=testfns;
            if (FD_ABORTED(test_result)) {
              fd_decref(values); return test_result;}
            else if (FD_TRUEP(test_result)) {
              fd_decref(values); fd_decref(test_result);
              return FD_TRUE;}
            else {}}
          else if ((SYMBOLP(testfn)) || (OIDP(testfn))) {
            lispval test_result;
            args[1]=values; args[2]=testfn;
            test_result = testp(n-1,args+1);
            args[1]=slotids; args[2]=testfns;
            if (FD_ABORTED(test_result)) {
              fd_decref(values); return test_result;}
            else if (FD_TRUEP(test_result)) {
              fd_decref(values); return test_result;}
            else fd_decref(test_result);}
          else if (TABLEP(testfn)) {
            DO_CHOICES(value,values) {
              lispval mapsto = fd_get(testfn,value,VOID);
              if (FD_ABORTED(mapsto)) {
                fd_decref(values);
                return mapsto;}
              else if (VOIDP(mapsto)) {}
              else if (n>3)
                if (fd_overlapp(mapsto,args[3])) {
                  fd_decref(mapsto); fd_decref(values);
                  return FD_TRUE;}
                else {fd_decref(mapsto);}
              else {
                fd_decref(mapsto); fd_decref(values);
                return FD_TRUE;}}}
          else {
            fd_decref(values);
            return fd_type_error(_("value test"),"testp",testfn);}
        fd_decref(values);}}
    return FD_FALSE;}
}

static lispval getpath_prim(int n,lispval *args)
{
  lispval result = fd_getpath(args[0],n-1,args+1,1,0);
  return fd_simplify_choice(result);
}

static lispval getpathstar_prim(int n,lispval *args)
{
  lispval result = fd_getpath(args[0],n-1,args+1,1,0);
  return fd_simplify_choice(result);
}

/* Cache gets */

static lispval cacheget_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval table_arg = fd_get_arg(expr,1), key_arg = fd_get_arg(expr,2);
  lispval default_expr = fd_get_arg(expr,3);
  if (PRED_FALSE((VOIDP(table_arg)) ||
                      (VOIDP(key_arg)) ||
                      (VOIDP(default_expr))))
    return fd_err(fd_SyntaxError,"cacheget_evalfn",NULL,expr);
  else {
    lispval table = fd_eval(table_arg,env), key, value;
    if (FD_ABORTED(table)) return table;
    else if (TABLEP(table)) key = fd_eval(key_arg,env);
    else return fd_type_error(_("table"),"cachget_evalfn",table);
    if (FD_ABORTED(key)) {
      fd_decref(table); return key;}
    else value = fd_get(table,key,VOID);
    if (VOIDP(value)) {
      lispval dflt = fd_eval(default_expr,env);
      if (FD_ABORTED(dflt)) {
        fd_decref(table); fd_decref(key);
        return dflt;}
      fd_store(table,key,dflt);
      return dflt;}
    else return value;}
}

/* Getting info from indexes */

static fd_index arg2index(lispval arg)
{
  if (FD_INDEXP(arg))
    return fd_lisp2index(arg);
  else if (TYPEP(arg,fd_consed_index_type))
    return (fd_index)arg;
  else if (STRINGP(arg)) {
    fd_index ix = fd_find_index(CSTRING(arg));
    if (ix) return ix;
    else return NULL;}
  else if (SYMBOLP(arg)) {
    lispval v = fd_config_get(SYM_NAME(arg));
    if (STRINGP(v))
      return fd_find_index(CSTRING(v));
    else return NULL;}
  else return NULL;
}

static lispval index_id(lispval arg)
{
  fd_index ix = arg2index(arg);
  if (ix == NULL)
    return fd_type_error(_("index spec"),"index_id",arg);
  else if (ix->indexid)
    return lispval_string(ix->indexid);
  else if (ix->index_source)
    return lispval_string(ix->index_source);
  else return FD_FALSE;
}

static lispval index_source_prim(lispval arg)
{
  fd_index p = arg2index(arg);
  if (p == NULL)
    return fd_type_error(_("index spec"),"index_label",arg);
  else if (p->index_source)
    return lispval_string(p->index_source);
  else return FD_FALSE;
}

/* Index operations */

static lispval index_get(lispval ixarg,lispval key)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix == NULL)
    return FD_ERROR;
  else return fd_index_get(ix,key);
}

static lispval index_add(lispval ixarg,lispval key,lispval values)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix == NULL) return FD_ERROR;
  fd_index_add(ix,key,values);
  return VOID;
}

static lispval index_set(lispval ixarg,lispval key,lispval values)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix == NULL) return FD_ERROR;
  fd_index_store(ix,key,values);
  return VOID;
}

static lispval index_decache(lispval ixarg,lispval key,lispval value)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix == NULL) return FD_ERROR;
  if (VOIDP(value))
    fd_hashtable_op(&(ix->index_cache),fd_table_replace,key,VOID);
  else {
    lispval keypair = fd_conspair(fd_incref(key),fd_incref(value));
    fd_hashtable_op(&(ix->index_cache),fd_table_replace,keypair,VOID);
    fd_decref(keypair);}
  return VOID;
}

static lispval bgdecache(lispval key,lispval value)
{
  fd_index ix = (fd_index)fd_background;
  if (ix == NULL) return FD_ERROR;
  if (VOIDP(value))
    fd_hashtable_op(&(ix->index_cache),fd_table_replace,key,VOID);
  else {
    lispval keypair = fd_conspair(fd_incref(key),fd_incref(value));
    fd_hashtable_op(&(ix->index_cache),fd_table_replace,keypair,VOID);
    fd_decref(keypair);}
  return VOID;
}

static lispval index_keys(lispval ixarg)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix == NULL) fd_type_error("index","index_keys",ixarg);
  return fd_index_keys(ix);
}

static lispval index_sizes(lispval ixarg,lispval keys_arg)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix == NULL)
    fd_type_error("index","index_sizes",ixarg);
  if (FD_VOIDP(keys_arg))
    return fd_index_sizes(ix);
  else return fd_index_keysizes(ix,keys_arg);
}

static lispval index_keysvec(lispval ixarg)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix == NULL) fd_type_error("index","index_keysvec",ixarg);
  if (ix->index_handler->fetchkeys) {
    lispval *keys; unsigned int n_keys;
    keys = ix->index_handler->fetchkeys(ix,&n_keys);
    return fd_cons_vector(NULL,n_keys,1,keys);}
  else return fd_index_keys(ix);
}

static lispval index_merge(lispval ixarg,lispval addstable)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix == NULL)
    return fd_type_error("index","index_merge",ixarg);
  else if (FD_HASHTABLEP(addstable)) {
    int rv = fd_index_merge(ix,(fd_hashtable)addstable);
    return FD_INT(rv);}
  else {
    fd_index add_index = fd_indexptr(addstable);
    if (add_index == NULL)
      return fd_type_error("tempindex|hashtable","index_merge",addstable);
    else if (fd_tempindexp(add_index)) {
      int rv = fd_index_merge(ix,&(add_index->index_adds));
      return FD_INT(rv);}
    else return fd_type_error("tempindex|hashtable","index_merge",addstable);}
}

static lispval slotindex_merge(lispval ixarg,lispval add)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix == NULL)
    return fd_type_error("index","index_merge",ixarg);
  else {
    int rv = fd_slotindex_merge(ix,add);
    return FD_INT(rv);}
}

static lispval index_source(lispval ix_arg)
{
  fd_index ix = fd_indexptr(ix_arg);
  if (ix == NULL)
    return fd_type_error("index","index_source",ix_arg);
  else if (ix->index_source)
    return lispval_string(ix->index_source);
  else return EMPTY;
}

static lispval close_index_prim(lispval ix_arg)
{
  fd_index ix = fd_indexptr(ix_arg);
  if (ix == NULL)
    return fd_type_error("index","index_close",ix_arg);
  fd_index_close(ix);
  return VOID;
}

static lispval commit_index_prim(lispval ix_arg)
{
  fd_index ix = fd_indexptr(ix_arg);
  if (ix == NULL)
    return fd_type_error("index","index_close",ix_arg);
  fd_commit_index(ix);
  return VOID;
}

static lispval index_save_prim(lispval index,
                               lispval adds,lispval drops,
                               lispval stores,
                               lispval metadata)
{
  fd_index ix = fd_lisp2index(index);
  if (!(ix))
    return fd_type_error("index","index_save_prim",index);

  int rv = fd_index_save(ix,adds,drops,stores,metadata);
  if (rv<0)
    return FD_ERROR_VALUE;
  else return FD_INT(rv);
}

static lispval index_fetchn_prim(lispval index,lispval keys)
{
  fd_index ix = fd_lisp2index(index);
  if (!(ix))
    return fd_type_error("index","index_fetchn_prim",index);

  return fd_index_fetchn(ix,keys);
}


static lispval suggest_hash_size(lispval size)
{
  unsigned int suggestion = fd_get_hashtable_size(fd_getint(size));
  return FD_INT(suggestion);
}

/* PICK and REJECT */

FD_FASTOP int test_selector_predicate(lispval candidate,lispval test,int datalevel);
FD_FASTOP int test_relation_regex(lispval candidate,lispval pred,lispval regex);

FD_FASTOP int test_selector_relation(lispval f,lispval pred,lispval val,int datalevel)
{
  if (CHOICEP(pred)) {
    DO_CHOICES(p,pred) {
      int retval;
      if ((retval = test_selector_relation(f,p,val,datalevel))) {
        {FD_STOP_DO_CHOICES;}
        return retval;}}
    return 0;}
  else if ((OIDP(f)) && ((SYMBOLP(pred)) || (OIDP(pred)))) {
    if ((!datalevel)&&(TYPEP(val,fd_regex_type)))
      return test_relation_regex(f,pred,val);
    else if (datalevel)
      return fd_test(f,pred,val);
    else return fd_frame_test(f,pred,val);}
  else if ((TABLEP(f)) && ((SYMBOLP(pred)) || (OIDP(pred)))) {
    if ((!datalevel)&&(TYPEP(val,fd_regex_type)))
      return test_relation_regex(f,pred,val);
    else return fd_test(f,pred,val);}
  else if (TABLEP(pred))
    return fd_test(pred,f,val);
  else if ((SYMBOLP(pred)) || (OIDP(pred)))
    return 0;
  else if (FD_APPLICABLEP(pred)) {
    if (FD_FCNIDP(pred)) pred = fd_fcnid_ref(pred);
    lispval rail[2], result = VOID;
    /* Handle the case where the 'slotid' is a unary function which can
       be used to extract an argument. */
    if ((FD_LAMBDAP(pred)) || (TYPEP(pred,fd_cprim_type))) {
      fd_function fcn = FD_DTYPE2FCN(pred); int retval = -1;
      if (fcn->fcn_min_arity == 2) {
        rail[0]=f; rail[1]=val;
        result = fd_apply(pred,2,rail);
        if (FD_ABORTP(result))
          retval = -1;
        else if ( (FD_FALSEP(result)) || (FD_EMPTYP(result)) )
          retval = 0;
        else {
          fd_decref(result);
          retval=1;}
        return retval;}
      else if (fcn->fcn_min_arity == 1) {
        lispval value = fd_apply(pred,1,&f); int retval = -1;
        if (EMPTYP(value)) return 0;
        else if (fd_overlapp(value,val)) retval = 1;
        else if (datalevel) retval = 0;
        else if ((FD_HASHSETP(val))||
                 (HASHTABLEP(val))||
                 (FD_APPLICABLEP(val))||
                 ((datalevel==0)&&(TYPEP(val,fd_regex_type)))||
                 ((PAIRP(val))&&(FD_APPLICABLEP(FD_CAR(val)))))
          retval = test_selector_predicate(value,val,datalevel);
        else retval = 0;
        fd_decref(value);
        return retval;}
      else result = fd_err(fd_TypeError,"test_selector_relation",
                         "invalid relation",pred);}
    if (FD_ABORTED(result))
      return fd_interr(result);
    else if ((FALSEP(result)) || (EMPTYP(result)) || (VOIDP(result)) )
      return 0;
    else {
      fd_decref(result);
      return 1;}}
  else if (VECTORP(pred)) {
    int len = VEC_LEN(pred), retval;
    lispval *data = VEC_DATA(pred), scan;
    if (len==0) return 0;
    else if (len==1) return test_selector_relation(f,data[0],val,datalevel);
    else scan = fd_getpath(f,len-1,data,datalevel,0);
    retval = test_selector_relation(scan,data[len-1],val,datalevel);
    fd_decref(scan);
    return retval;}
  else return fd_type_error(_("test relation"),"test_selector_relation",pred);
}

FD_FASTOP int test_relation_regex(lispval candidate,lispval pred,lispval regex)
{
  lispval values = ((OIDP(candidate))?
                 (fd_frame_get(candidate,pred)):
                 (fd_get(candidate,pred,EMPTY)));
  if (EMPTYP(values)) return 0;
  else {
    struct FD_REGEX *rx = (struct FD_REGEX *)regex;
    DO_CHOICES(value,values) {
      if (STRINGP(value)) {
        regmatch_t results[1];
        u8_string data = CSTRING(value);
        int retval = regexec(&(rx->rxcompiled),data,1,results,0);
        if (retval!=REG_NOMATCH) {
          if (retval) {
            u8_byte buf[512];
            regerror(retval,&(rx->rxcompiled),buf,512);
            fd_seterr(fd_RegexError,"regex_pick",buf,VOID);
            FD_STOP_DO_CHOICES;
            fd_decref(values);
            return -1;}
          else if (results[0].rm_so>=0) {
            FD_STOP_DO_CHOICES;
            fd_decref(values);
            return 1;}}}}
    return 0;}
}

FD_FASTOP int test_selector_predicate(lispval candidate,lispval test,
                                      int datalevel)
{
  if (EMPTYP(candidate)) return 0;
  else if (EMPTYP(test)) return 0;
  else if (CHOICEP(test)) {
    int retval = 0;
    DO_CHOICES(t,test) {
      if ((retval = test_selector_predicate(candidate,t,datalevel))) {
        {FD_STOP_DO_CHOICES;}
        return retval;}}
    return retval;}
  else if ((OIDP(candidate)) && ((SYMBOLP(test)) || (OIDP(test))))
    if (datalevel)
      return fd_test(candidate,test,VOID);
    else return fd_frame_test(candidate,test,VOID);
  else if ((!(datalevel))&&(TYPEP(test,fd_regex_type))) {
    if (STRINGP(candidate)) {
      struct FD_REGEX *rx = (struct FD_REGEX *)test;
      regmatch_t results[1];
      u8_string data = CSTRING(candidate);
      int retval = regexec(&(rx->rxcompiled),data,1,results,0);
      if (retval == REG_NOMATCH) return 0;
      else if (retval) {
        u8_byte buf[512];
        regerror(retval,&(rx->rxcompiled),buf,512);
        fd_seterr(fd_RegexError,"regex_pick",buf,VOID);
        return -1;}
      else if (results[0].rm_so<0) return 0;
      else return 1;}
    else return 0;}
  else if (TYPEP(test,fd_hashset_type))
    if (fd_hashset_get((fd_hashset)test,candidate))
      return 1;
    else return 0;
  else if ((TABLEP(candidate)&&
            ((OIDP(test)) || (SYMBOLP(test))))) {
    if ((!(datalevel))&&(OIDP(candidate)))
      return fd_frame_test(candidate,test,VOID);
    else return fd_test(candidate,test,VOID);}
  else if (FD_APPLICABLEP(test)) {
    lispval v = fd_apply(test,1,&candidate);
    if (FD_ABORTED(v)) return fd_interr(v);
    else if ((FALSEP(v)) || (EMPTYP(v)) || (VOIDP(v)))
      return 0;
    else {fd_decref(v); return 1;}}
  else if ((PAIRP(test))&&(FD_APPLICABLEP(FD_CAR(test)))) {
    lispval fcn = FD_CAR(test), newval = FD_FALSE;
    lispval args = FD_CDR(test), argv[7]; int j = 1;
    argv[0]=candidate;
    if (PAIRP(args))
      while (PAIRP(args)) {
        argv[j++]=FD_CAR(args); args = FD_CDR(args);}
    else argv[j++]=args;
    if (j>7)
      newval = fd_err(fd_RangeError,"test_selector_relation",
                    "too many elements in test condition",VOID);
    else newval = fd_apply(j,fcn,argv);
    if (FD_ABORTED(newval)) return newval;
    else if ((FALSEP(newval))||(EMPTYP(newval)))
      return 0;
    else {
      fd_decref(newval);
      return 1;}}
  else if (FD_POOLP(test))
    if (OIDP(candidate)) {
      fd_pool p = fd_lisp2pool(test);
      if ( (p->pool_flags) & (FD_POOL_ADJUNCT) ) {
        FD_OID addr = FD_OID_ADDR(candidate);
        long long diff = FD_OID_DIFFERENCE(addr,p->pool_base);
        if ( (diff > 0) && (diff < p->pool_capacity) )
          return 1;
        else return 0;}
      else if (fd_oid2pool(candidate) == p)
        return 1;
      else return 0;}
    else return 0;
  else if (TABLEP(test))
    return fd_test(test,candidate,VOID);
  else if (TABLEP(candidate))
    return fd_test(candidate,test,VOID);
  else {
    lispval ev = fd_type_error(_("test object"),"test_selector_predicate",test);
    return fd_interr(ev);}
}

FD_FASTOP int test_selector_clauses(lispval candidate,int n,lispval *args,
                                    int datalevel)
{
  if (n==1)
    if (EMPTYP(args[0])) return 0;
    else return test_selector_predicate(candidate,args[0],datalevel);
  else if (n==3) {
    lispval scan = fd_getpath(candidate,1,args,datalevel,0);
    int retval = test_selector_relation(scan,args[1],args[2],datalevel);
    fd_decref(scan);
    return retval;}
  else if (n%2) {
    fd_seterr(fd_TooManyArgs,"test_selector_clauses",
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

static lispval pick_helper(lispval candidates,int n,lispval *tests,int datalevel)
{
  int retval;
  if (CHOICEP(candidates)) {
    int n_elts = FD_CHOICE_SIZE(candidates);
    fd_choice read_choice = FD_XCHOICE(candidates);
    fd_choice write_choice = fd_alloc_choice(n_elts);
    const lispval *read = FD_XCHOICE_DATA(read_choice), *limit = read+n_elts;
    lispval *write = (lispval *)FD_XCHOICE_DATA(write_choice);
    int n_results = 0, atomic_results = 1;
    while (read<limit) {
      lispval candidate = *read++;
      int retval = test_selector_clauses(candidate,n,tests,datalevel);
      if (retval<0) {
        const lispval *scan = FD_XCHOICE_DATA(write_choice);
        const lispval *limit = scan+n_results;
        while (scan<limit) {lispval v = *scan++; fd_decref(v);}
        fd_free_choice(write_choice);
        return FD_ERROR;}
      else if (retval) {
        *write++=candidate; n_results++;
        if (CONSP(candidate)) {
          atomic_results = 0;
          fd_incref(candidate);}}}
    if (n_results==0) {
      fd_free_choice(write_choice);
      return EMPTY;}
    else if (n_results==1) {
      lispval result = FD_XCHOICE_DATA(write_choice)[0];
      fd_free_choice(write_choice);
      /* This was incref'd during the loop. */
      return result;}
    else return fd_init_choice
           (write_choice,n_results,NULL,
            ((atomic_results)?(FD_CHOICE_ISATOMIC):(FD_CHOICE_ISCONSES)));}
  else if (EMPTYP(candidates))
    return EMPTY;
  else if (n==1)
    if ((retval = (test_selector_predicate(candidates,tests[0],datalevel))))
      if (retval<0) return FD_ERROR;
      else return fd_incref(candidates);
    else return EMPTY;
  else if ((retval = (test_selector_clauses(candidates,n,tests,datalevel))))
    if (retval<0) return FD_ERROR;
    else return fd_incref(candidates);
  else return EMPTY;
}

static lispval hashset_filter(lispval candidates,fd_hashset hs,int pick)
{
  if (hs->hs_n_elts==0) {
    if (pick)
      return EMPTY;
    else return fd_incref(candidates);}
  fd_read_lock_table(hs); {
    lispval simple = fd_make_simple_choice(candidates);
    int n = FD_CHOICE_SIZE(simple), isatomic = 1;
    lispval *buckets = hs->hs_buckets; int n_slots = hs->hs_n_buckets;
    lispval *keep = u8_alloc_n(n,lispval), *write = keep;
    DO_CHOICES(c,candidates) {
      int hash = fd_hash_lisp(c), probe = hash%n_slots, found=-1;
      lispval contents = buckets[probe];
      if (FD_EMPTY_CHOICEP(contents))
        found=0;
      else if (!(FD_AMBIGP(contents)))
        found = FD_EQUALP(c,contents);
      else if (FD_CHOICEP(contents))
        found = fast_choice_containsp(c,(fd_choice)contents);
      else {
        lispval normal = fd_make_simple_choice(contents);
        if (FD_CHOICEP(normal))
          found = fast_choice_containsp(c,(fd_choice)normal);
        else found=FD_EQUALP(c,normal);
        fd_decref(normal);}
      if ( ((found)&&(pick)) || ((!found)&&((!pick))) ) {
        if ((isatomic)&&(CONSP(c))) isatomic = 0;
        *write++=c;
        fd_incref(c);}}
    fd_decref(simple);
    fd_unlock_table(hs);
    if (write == keep) {
      u8_free(keep);
      return EMPTY;}
    else if ((write-keep)==1) {
      lispval v = keep[0];
      u8_free(keep);
      return v;}
    else return fd_init_choice
           (NULL,write-keep,keep,
            ((isatomic)?(FD_CHOICE_ISATOMIC):(0))|
            (FD_CHOICE_FREEDATA));}
}
#define hashset_pick(c,h) (hashset_filter(c,(fd_hashset)h,1))
#define hashset_reject(c,h) (hashset_filter(c,(fd_hashset)h,0))

static lispval hashtable_filter(lispval candidates,fd_hashtable ht,int pick)
{
  if (ht->table_n_keys==0) {
    if (pick)
      return EMPTY;
    else return fd_incref(candidates);}
  else {
    lispval simple = fd_make_simple_choice(candidates);
    int n = FD_CHOICE_SIZE(simple), unlock = 0, isatomic = 1;
    lispval *keep = u8_alloc_n(n,lispval), *write = keep;
    if (ht->table_uselock) {fd_read_lock_table(ht); unlock = 1;}
    {struct FD_HASH_BUCKET **slots = ht->ht_buckets;
      int n_slots = ht->ht_n_buckets;
      DO_CHOICES(c,candidates) {
        struct FD_KEYVAL *result = fd_hashvec_get(c,slots,n_slots);
        lispval rv = ( (result) ? (result->kv_val) : (VOID) );
        if ( (VOIDP(rv)) || (EMPTYP(rv)) ) result = NULL;
        if ( ((result) && (pick)) ||
             ((result == NULL) && (!(pick))) ) {
          if ((isatomic)&&(CONSP(c))) isatomic = 0;
          fd_incref(c);
          *write++=c;}}
      if (unlock) u8_rw_unlock(&(ht->table_rwlock));
      fd_decref(simple);
      if (write == keep) {
        u8_free(keep);
        return EMPTY;}
      else if ((write-keep)==1) {
        lispval v = keep[0];
        u8_free(keep);
        return v;}
      else return fd_init_choice
             (NULL,write-keep,keep,
              ((isatomic)?(FD_CHOICE_ISATOMIC):(0))|
              (FD_CHOICE_FREEDATA));}}
}
#define hashtable_pick(c,h) (hashtable_filter(c,(fd_hashtable)h,1))
#define hashtable_reject(c,h) (hashtable_filter(c,(fd_hashtable)h,0))

static lispval pick_lexpr(int n,lispval *args)
{
  if (FD_EMPTYP(args[0]))
    return FD_EMPTY;
  else if ((n==2)&&(HASHTABLEP(args[1])))
    return hashtable_pick(args[0],(fd_hashtable)args[1]);
  else if ((n==2)&&(FD_HASHSETP(args[1])))
    return hashset_pick(args[0],(fd_hashset)args[1]);
  else if ((n<=4)||(n%2))
    return pick_helper(args[0],n-1,args+1,0);
  else return fd_err
         (fd_TooManyArgs,"pick_lexpr",
          "PICK requires two or 2n+1 arguments",VOID);
}

static lispval prefer_lexpr(int n,lispval *args)
{
  if ((n==2)&&(HASHTABLEP(args[1]))) {
    lispval results = hashtable_pick(args[0],(fd_hashtable)args[1]);
    if (EMPTYP(results))
      return fd_incref(args[0]);
    else return results;}
  else if ((n==2)&&(FD_HASHSETP(args[1]))) {
    lispval results = hashset_pick(args[0],(fd_hashset)args[1]);
    if (EMPTYP(results))
      return fd_incref(args[0]);
    else return results;}
  else if ((n<=4)||(n%2)) {
    lispval results = pick_helper(args[0],n-1,args+1,0);
    if (EMPTYP(results))
      return fd_incref(args[0]);
    else return results;}
  else return fd_err(fd_TooManyArgs,"prefer_lexpr",
                     "PICK PREFER two or 2n+1 arguments",VOID);
}

static lispval prim_pick_lexpr(int n,lispval *args)
{
  if ((n==2)&&(HASHTABLEP(args[1])))
    return hashtable_pick(args[0],(fd_hashtable)args[1]);
  else if ((n==2)&&(FD_HASHSETP(args[1])))
    return hashset_pick(args[0],(fd_hashset)args[1]);
  else if ((n<=4)||(n%2))
    return pick_helper(args[0],n-1,args+1,1);
  else return fd_err(fd_TooManyArgs,"prim_pick_lexpr",
                     "%PICK requires two or 2n+1 arguments",VOID);
}

static lispval prim_prefer_lexpr(int n,lispval *args)
{
  if ((n<=4)||(n%2)) {
    lispval results = pick_helper(args[0],n-1,args+1,1);
    if (EMPTYP(results))
      return fd_incref(args[0]);
    else return results;}
  else return fd_err(fd_TooManyArgs,"prim_prefer_lexpr",
                     "%PICK requires two or 2n+1 arguments",VOID);
}

/* REJECT etc */

static lispval reject_helper(lispval candidates,int n,lispval *tests,int datalevel)
{
  int retval;
  if (CHOICEP(candidates)) {
    int n_elts = FD_CHOICE_SIZE(candidates);
    fd_choice read_choice = FD_XCHOICE(candidates);
    fd_choice write_choice = fd_alloc_choice(n_elts);
    const lispval *read = FD_XCHOICE_DATA(read_choice), *limit = read+n_elts;
    lispval *write = (lispval *)FD_XCHOICE_DATA(write_choice);
    int n_results = 0, atomic_results = 1;
    while (read<limit) {
      lispval candidate = *read++;
      int retval = test_selector_clauses(candidate,n,tests,datalevel);
      if (retval<0) {
        const lispval *scan  = FD_XCHOICE_DATA(write_choice);
        const lispval *limit = scan+n_results;
        while (scan<limit) {
          lispval v = *scan++; 
          fd_decref(v);}
        fd_free_choice(write_choice);
        return FD_ERROR;}
      else if (retval==0) {
        *write++=candidate; n_results++;
        if (CONSP(candidate)) {
          atomic_results = 0;
          fd_incref(candidate);}}}
    if (n_results==0) {
      fd_free_choice(write_choice);
      return EMPTY;}
    else if (n_results==1) {
      lispval result = FD_XCHOICE_DATA(write_choice)[0];
      fd_free_choice(write_choice);
      /* This was incref'd during the loop. */
      return result;}
    else return fd_init_choice
           (write_choice,n_results,NULL,
            ((atomic_results)?(FD_CHOICE_ISATOMIC):(FD_CHOICE_ISCONSES)));}
  else if (EMPTYP(candidates))
    return EMPTY;
  else if (n==1)
    if ((retval = (test_selector_predicate(candidates,tests[0],datalevel))))
      if (retval<0) return FD_ERROR;
      else return EMPTY;
    else return fd_incref(candidates);
  else if ((retval = (test_selector_clauses(candidates,n,tests,datalevel))))
    if (retval<0) return FD_ERROR;
    else return EMPTY;
  else return fd_incref(candidates);
}

static lispval reject_lexpr(int n,lispval *args)
{
  if (FD_EMPTYP(args[0]))
    return FD_EMPTY;
  else if ((n==2)&&(HASHTABLEP(args[1])))
    return hashtable_reject(args[0],args[1]);
  else if ((n==2)&&(FD_HASHSETP(args[1])))
    return hashset_reject(args[0],args[1]);
  else if ((n<=4)||(n%2))
    return reject_helper(args[0],n-1,args+1,0);
  else return fd_err(fd_TooManyArgs,"reject_lexpr",
                     "REJECT requires two or 2n+1 arguments",VOID);
}

static lispval avoid_lexpr(int n,lispval *args)
{
  if ((n==2)&&(HASHTABLEP(args[1]))) {
    lispval results = hashtable_reject(args[0],(fd_hashtable)args[1]);
    if (EMPTYP(results))
      return fd_incref(args[0]);
    else return results;}
  else if ((n==2)&&(FD_HASHSETP(args[1]))) {
    lispval results = hashtable_reject(args[0],(fd_hashset)args[1]);
    if (EMPTYP(results))
      return fd_incref(args[0]);
    else return results;}
  else if ((n<=4)||(n%2)) {
    lispval values = reject_helper(args[0],n-1,args+1,0);
    if (EMPTYP(values))
      return fd_incref(args[0]);
    else return values;}
  else return fd_err(fd_TooManyArgs,"avoid_lexpr",
                     "AVOID requires two or 2n+1 arguments",VOID);
}

static lispval prim_reject_lexpr(int n,lispval *args)
{
  if ((n==2)&&(HASHTABLEP(args[1])))
    return hashtable_reject(args[0],args[1]);
  else if ((n==2)&&(FD_HASHSETP(args[1])))
    return hashset_reject(args[0],args[1]);
  else if ((n<=4)||(n%2))
    return reject_helper(args[0],n-1,args+1,1);
  else return fd_err(fd_TooManyArgs,"prim_reject_lexpr",
                     "%REJECT requires two or 2n+1 arguments",VOID);
}

static lispval prim_avoid_lexpr(int n,lispval *args)
{
  if ((n==2)&&(HASHTABLEP(args[1]))) {
    lispval results = hashtable_reject(args[0],(fd_hashtable)args[1]);
    if (EMPTYP(results))
      return fd_incref(args[0]);
    else return results;}
  else if ((n==2)&&(FD_HASHSETP(args[1]))) {
    lispval results = hashtable_reject(args[0],(fd_hashset)args[1]);
    if (EMPTYP(results))
      return fd_incref(args[0]);
    else return results;}
  else if ((n<=4)||(n%2)) {
    lispval values = reject_helper(args[0],n-1,args+1,1);
    if (EMPTYP(values))
      return fd_incref(args[0]);
    else return values;}
  else return fd_err(fd_TooManyArgs,"prim_avoid_lexpr",
                     "%AVOID requires two or 2n+1 arguments",VOID);
}

/* Kleene* operations */

static lispval getroots(lispval frames)
{
  lispval roots = FD_EMPTY;
  if (FD_AMBIGP(frames)) {
    FD_DO_CHOICES(frame,frames) {
      if ( (FD_OIDP(frame)) || (FD_SLOTMAPP(frame)) || (FD_SCHEMAPP(frame)) ) {
        CHOICE_ADD(roots,frame);}}}
  else if ((FD_OIDP(frames)) || (FD_SLOTMAPP(frames)) || (FD_SCHEMAPP(frames)))
    roots = frames;
  else NO_ELSE;
  return roots;
}

static lispval getstar(lispval frames,lispval slotids)
{
  lispval roots = getroots(frames);
  if (FD_EMPTYP(roots))
    return FD_EMPTY;
  lispval results = fd_incref(roots);
  DO_CHOICES(slotid,slotids) {
    lispval all = fd_inherit_values(roots,slotid,slotid);
    if (FD_ABORTED(all)) {
      fd_decref(results);
      return all;}
    else {CHOICE_ADD(results,all);}}
  return fd_simplify_choice(results);
}

static lispval inherit_prim(lispval slotids,lispval frames,lispval through)
{
  lispval roots = getroots(frames);
  if (FD_EMPTYP(roots))
    return FD_EMPTY;
  lispval results = fd_incref(frames);
  DO_CHOICES(through_id,through) {
    lispval all = fd_inherit_values(frames,slotids,through_id);
    CHOICE_ADD(results,all);}
  return fd_simplify_choice(results);
}

static lispval pathp(lispval frames,lispval slotids,lispval values)
{
  lispval roots = getroots(frames);
  if (FD_EMPTYP(roots))
    return FD_FALSE;
  else if (fd_pathp(frames,slotids,values))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval getbasis(lispval frames,lispval lattice)
{
  return fd_simplify_choice(fd_get_basis(frames,lattice));
}

/* Frame creation */

static lispval allocate_oids(lispval pool,lispval howmany)
{
  fd_pool p = arg2pool(pool);
  if (p == NULL)
    return fd_type_error(_("pool spec"),"allocate_oids",pool);
  if (VOIDP(howmany))
    return fd_pool_alloc(p,1);
  else if (FD_UINTP(howmany))
    return fd_pool_alloc(p,FIX2INT(howmany));
  else return fd_type_error(_("fixnum"),"allocate_oids",howmany);
}

static lispval frame_create_lexpr(int n,lispval *args)
{
  lispval result;
  int i = (n%2);
  if ((n>=1)&&(EMPTYP(args[0])))
    return EMPTY;
  else if (n==1) return fd_new_frame(args[0],VOID,0);
  else if (n==2) return fd_new_frame(args[0],args[1],1);
  else if (n%2) {
    if ((SLOTMAPP(args[0]))||(SCHEMAPP(args[0]))||(OIDP(args[0]))) {
      result = fd_deep_copy(args[0]);}
    else result = fd_new_frame(args[0],VOID,0);}
  else if ((SYMBOLP(args[0]))||(OIDP(args[0])))
    result = fd_new_frame(FD_DEFAULT_VALUE,VOID,0);
  else return fd_err(fd_SyntaxError,"frame_create_lexpr",NULL,VOID);
  if (FD_ABORTP(result))
    return result;
  else if (FD_OIDP(result)) while (i<n) {
      DO_CHOICES(slotid,args[i]) {
        int rv = fd_frame_add(result,slotid,args[i+1]);
        if (rv < 0) {
          FD_STOP_DO_CHOICES;
          return FD_ERROR;}}
      i = i+2;}
  else while (i<n) {
      DO_CHOICES(slotid,args[i]) {
        int rv = fd_add(result,slotid,args[i+1]);
        if (rv < 0) {
          FD_STOP_DO_CHOICES;
          return FD_ERROR;}}
      i = i+2;}
  return result;
}

static lispval frame_update_lexpr(int n,lispval *args)
{
  if ((n>=1)&&(EMPTYP(args[0])))
    return EMPTY;
  else if (n<3)
    return fd_err(fd_SyntaxError,"frame_update_lexpr",NULL,VOID);
  else if (!(TABLEP(args[0])))
    return fd_err(fd_TypeError,"frame_update_lexpr","not a table",args[0]);
  else if (n%2) {
    lispval result = fd_deep_copy(args[0]); int i = 1;
    while (i<n) {
      if (fd_store(result,args[i],args[i+1])<0) {
        fd_decref(result);
        return FD_ERROR;}
      else i = i+2;}
    return result;}
  else return fd_err(fd_SyntaxError,"frame_update_lexpr",
                     _("wrong number of args"),VOID);
}

static lispval seq2frame_prim
  (lispval poolspec,lispval values,lispval schema,lispval dflt)
{
  if (!(FD_SEQUENCEP(schema)))
    return fd_type_error(_("sequence"),"seq2frame_prim",schema);
  else if (!(FD_SEQUENCEP(values)))
    return fd_type_error(_("sequence"),"seq2frame_prim",values);
  else {
    lispval frame;
    int i = 0, slen = fd_seq_length(schema), vlen = fd_seq_length(values);
    if (FALSEP(poolspec))
      frame = fd_empty_slotmap();
    else if (OIDP(poolspec)) frame = poolspec;
    else {
      lispval oid, slotmap;
      fd_pool p = arg2pool(poolspec);
      if (p == NULL)
        return fd_type_error(_("pool spec"),"seq2frame_prim",poolspec);
      oid = fd_pool_alloc(p,1);
      if (FD_ABORTED(oid)) return oid;
      slotmap = fd_empty_slotmap();
      if (fd_set_oid_value(oid,slotmap)<0) {
        fd_decref(slotmap);
        return FD_ERROR;}
      else {
        fd_decref(slotmap); frame = oid;}}
    while ((i<slen) && (i<vlen)) {
      lispval slotid = fd_seq_elt(schema,i);
      lispval value = fd_seq_elt(values,i);
      int retval;
      if (OIDP(frame))
        retval = fd_frame_add(frame,slotid,value);
      else retval = fd_add(frame,slotid,value);
      if (retval<0) {
        fd_decref(slotid); fd_decref(value);
        if (FALSEP(poolspec)) fd_decref(frame);
        return FD_ERROR;}
      fd_decref(slotid); fd_decref(value);
      i++;}
    if (!(VOIDP(dflt)))
      while (i<slen) {
        lispval slotid = fd_seq_elt(schema,i);
        int retval;
        if (OIDP(frame))
          retval = fd_frame_add(frame,slotid,dflt);
        else retval = fd_add(frame,slotid,dflt);
        if (retval<0) {
          fd_decref(slotid);
          if (FALSEP(poolspec)) fd_decref(frame);
          return FD_ERROR;}
        fd_decref(slotid); i++;}
    return frame;}
}

static int doassert(lispval f,lispval s,lispval v)
{
  if (EMPTYP(v)) return 0;
  else if (OIDP(f))
    return fd_frame_add(f,s,v);
  else return fd_add(f,s,v);
}

static int doretract(lispval f,lispval s,lispval v)
{
  if (EMPTYP(v)) return 0;
  else if (OIDP(f))
    return fd_frame_drop(f,s,v);
  else return fd_drop(f,s,v);
}

static lispval modify_frame_lexpr(int n,lispval *args)
{
  if (n%2==0)
    return fd_err(fd_SyntaxError,"FRAME-MODIFY",NULL,VOID);
  else {
    DO_CHOICES(frame,args[0]) {
      int i = 1; while (i<n) {
        DO_CHOICES(slotid,args[i])
          if (PAIRP(slotid))
            if ((PAIRP(FD_CDR(slotid))) &&
                ((OIDP(FD_CAR(slotid))) || (SYMBOLP(FD_CAR(slotid)))) &&
                ((FD_EQ(FD_CADR(slotid),drop_symbol)))) {
              if (doretract(frame,FD_CAR(slotid),args[i+1])<0) {
                FD_STOP_DO_CHOICES;
                return FD_ERROR;}
              else {}}
            else {
              u8_log(LOG_WARN,fd_TypeError,"frame_modify_lexpr","slotid");}
          else if ((SYMBOLP(slotid)) || (OIDP(slotid)))
            if (doassert(frame,slotid,args[i+1])<0) {
              FD_STOP_DO_CHOICES;
              return FD_ERROR;}
            else {}
          else u8_log(LOG_WARN,fd_TypeError,"frame_modify_lexpr","slotid");
        i = i+2;}}
    return fd_incref(args[0]);}
}

/* OID operations */

static lispval oid_plus_prim(lispval oid,lispval increment)
{
  FD_OID base = FD_OID_ADDR(oid), next;
  int delta = fd_getint(increment);
  next = FD_OID_PLUS(base,delta);
  return fd_make_oid(next);
}

static lispval oid_offset_prim(lispval oidarg,lispval against)
{
  FD_OID oid = FD_OID_ADDR(oidarg), base; int cap = -1;
  if (OIDP(against)) {
    base = FD_OID_ADDR(against);}
  else if (FD_POOLP(against)) {
    fd_pool p = fd_lisp2pool(against);
    if (p) {base = p->pool_base; cap = p->pool_capacity;}
    else return FD_ERROR;}
  else if ((VOIDP(against)) || (FALSEP(against))) {
    fd_pool p = fd_oid2pool(oidarg);
    if (p) {base = p->pool_base; cap = p->pool_capacity;}
    else return FD_INT((FD_OID_LO(oid))%0x100000);}
  else return fd_type_error(_("offset base"),"oid_offset_prim",against);
  if ((FD_OID_HI(oid)) == (FD_OID_HI(base))) {
    int diff = (FD_OID_LO(oid))-(FD_OID_LO(base));
    if ((diff>=0) && ((cap<0) || (diff<cap)))
      return FD_INT(diff);
    else return FD_FALSE;}
  else return FD_FALSE;
}

static lispval oid_minus_prim(lispval oidarg,lispval against)
{
  FD_OID oid = FD_OID_ADDR(oidarg), base;
  if (OIDP(against)) {
    base = FD_OID_ADDR(against);}
  else if (FD_POOLP(against)) {
    fd_pool p = fd_lisp2pool(against);
    if (p) base = p->pool_base;
    else return FD_ERROR;}
  else return fd_type_error(_("offset base"),"oid_offset_prim",against);
  unsigned long long oid_difference = FD_OID_DIFFERENCE(oid,base);
  return FD_INT(oid_difference);
}

#ifdef FD_OID_BASE_ID
static lispval oid_ptrdata_prim(lispval oid)
{
  return fd_conspair(FD_INT(FD_OID_BASE_ID(oid)),
                     FD_INT(FD_OID_BASE_OFFSET(oid)));
}
#endif

static lispval make_oid_prim(lispval high,lispval low)
{
  if ( (VOIDP(low)) || (FD_FALSEP(low)) ) {
    FD_OID oid;
    unsigned long long addr;
    if (FIXNUMP(high))
      addr = FIX2INT(high);
    else if (FD_BIGINTP(high))
      addr = fd_bigint2uint64((fd_bigint)high);
    else return fd_type_error("integer","make_oid_prim",high);
#if FD_STRUCT_OIDS
    memset(&oid,0,sizeof(FD_OID));
    oid.hi = (addr>>32)&0xFFFFFFFF;
    oid.log = (addr)&0xFFFFFFFF;
#else
    oid = addr;
#endif
    return fd_make_oid(oid);}
  else if (OIDP(high)) {
    FD_OID base = FD_OID_ADDR(high), oid; unsigned int off;
    if (FD_UINTP(low))
      off = FIX2INT(low);
    else if (FD_BIGINTP(low)) {
      if ((off = fd_bigint2uint((struct FD_BIGINT *)low))==0)
        return fd_type_error("uint32","make_oid_prim",low);}
    else return fd_type_error("uint32","make_oid_prim",low);
    oid = FD_OID_PLUS(base,off);
    return fd_make_oid(oid);}
  else {
    unsigned int hi, lo;
#if FD_STRUCT_OIDS
    FD_OID addr; memset(&addr,0,sizeof(FD_OID));
#else
    FD_OID addr = 0;
#endif
    FD_SET_OID_HI(addr,0); FD_SET_OID_LO(addr,0);
    if (FIXNUMP(high))
      if (((FIX2INT(high))<0)||((FIX2INT(high))>UINT_MAX))
        return fd_type_error("uint32","make_oid_prim",high);
      else hi = FIX2INT(high);
    else if (FD_BIGINTP(high)) {
      hi = fd_bigint2uint((struct FD_BIGINT *)high);
      if (hi==0) return fd_type_error("uint32","make_oid_prim",high);}
    else return fd_type_error("uint32","make_oid_prim",high);
    if (FIXNUMP(low))
      if (((FIX2INT(low))<0)||((FIX2INT(low))>UINT_MAX))
        return fd_type_error("uint32","make_oid_prim",low);
      else lo = FIX2INT(low);
    else if (FD_BIGINTP(low)) {
      lo = fd_bigint2uint((struct FD_BIGINT *)low);
      if (lo==0) return fd_type_error("uint32","make_oid_prim",low);}
    else return fd_type_error("uint32","make_oid_prim",low);
    FD_SET_OID_HI(addr,hi);
    FD_SET_OID_LO(addr,lo);
    return fd_make_oid(addr);}
}

static lispval oid2string_prim(lispval oid,lispval name)
{
  FD_OID addr = FD_OID_ADDR(oid);
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);

  if (VOIDP(name))
    u8_printf(&out,"@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));
  else if ((STRINGP(name)) || (CHOICEP(name)) ||
           (PAIRP(name)) || (VECTORP(name)))
    u8_printf(&out,"@%x/%x%q",FD_OID_HI(addr),FD_OID_LO(addr),name);
  else u8_printf(&out,"@%x/%x{%q}",FD_OID_HI(addr),FD_OID_LO(addr),name);

  return fd_stream2string(&out);
}

static lispval oidhex_prim(lispval oid,lispval base_arg)
{
  char buf[32]; int offset;
  FD_OID addr = FD_OID_ADDR(oid);
  if ((VOIDP(base_arg)) || (FALSEP(base_arg))) {
    fd_pool p = fd_oid2pool(oid);
    if (p) {
      FD_OID base = p->pool_base;
      offset = FD_OID_DIFFERENCE(addr,base);}
    else offset = (FD_OID_LO(addr))%0x100000;}
  else if (FD_POOLP(base_arg)) {
    fd_pool p = fd_poolptr(base_arg);
    if (p) {
      FD_OID base = p->pool_base;
      offset = FD_OID_DIFFERENCE(addr,base);}
    else offset = (FD_OID_LO(addr))%0x100000;}
  else if (OIDP(base_arg)) {
    FD_OID base = FD_OID_ADDR(base_arg);
    offset = FD_OID_DIFFERENCE(addr,base);}
  else offset = (FD_OID_LO(addr))%0x100000;
  return fd_make_string(NULL,-1,u8_uitoa16(offset,buf));
}

static lispval oidb32_prim(lispval oid,lispval base_arg)
{
  char buf[64]; int offset, buflen = 64;
  FD_OID addr = FD_OID_ADDR(oid);
  if ((VOIDP(base_arg)) || (FALSEP(base_arg))) {
    fd_pool p = fd_oid2pool(oid);
    if (p) {
      FD_OID base = p->pool_base;
      offset = FD_OID_DIFFERENCE(addr,base);}
    else offset = (FD_OID_LO(addr))%0x100000;}
  else if (FD_POOLP(base_arg)) {
    fd_pool p = fd_poolptr(base_arg);
    if (p) {
      FD_OID base = p->pool_base;
      offset = FD_OID_DIFFERENCE(addr,base);}
    else offset = (FD_OID_LO(addr))%0x100000;}
  else if (OIDP(base_arg)) {
    FD_OID base = FD_OID_ADDR(base_arg);
    offset = FD_OID_DIFFERENCE(addr,base);}
  else offset = (FD_OID_LO(addr))%0x100000;
  fd_ulonglong_to_b32(offset,buf,&buflen);
  return fd_make_string(NULL,buflen,buf);
}

static lispval oidplus(FD_OID base,int delta)
{
  FD_OID next = FD_OID_PLUS(base,delta);
  return fd_make_oid(next);
}

static lispval hex2oid_prim(lispval arg,lispval base_arg)
{
  long long offset;
  if (STRINGP(arg))
    offset = strtol(CSTRING(arg),NULL,16);
  else if (FIXNUMP(arg))
    offset = FIX2INT(arg);
  else return fd_type_error("hex offset","hex2oid_prim",arg);
  if (offset<0) return fd_type_error("hex offset","hex2oid_prim",arg);
  if (OIDP(base_arg)) {
    FD_OID base = FD_OID_ADDR(base_arg);
    return oidplus(base,offset);}
  else if (FD_POOLP(base_arg)) {
    fd_pool p = fd_poolptr(base_arg);
    return oidplus(p->pool_base,offset);}
  else if (STRINGP(base_arg)) {
    fd_pool p = fd_name2pool(CSTRING(base_arg));
    if (p) return oidplus(p->pool_base,offset);
    else return fd_type_error("pool id","hex2oid_prim",base_arg);}
  else return fd_type_error("pool id","hex2oid_prim",base_arg);
}

static lispval b32oid_prim(lispval arg,lispval base_arg)
{
  long long offset;
  if (STRINGP(arg)) {
    offset = fd_b32_to_longlong(CSTRING(arg));
    if (offset<0) {
      fd_seterr(fd_ParseError,"b32oid","invalid B32 string",arg);
      return FD_ERROR;}}
  else if (FIXNUMP(arg))
    offset = FIX2INT(arg);
  else return fd_type_error("b32 offset","b32oid_prim",arg);
  if (offset<0) return fd_type_error("hex offset","hex2oid_prim",arg);
  if (OIDP(base_arg)) {
    FD_OID base = FD_OID_ADDR(base_arg);
    return oidplus(base,offset);}
  else if (FD_POOLP(base_arg)) {
    fd_pool p = fd_poolptr(base_arg);
    return oidplus(p->pool_base,offset);}
  else if (STRINGP(base_arg)) {
    fd_pool p = fd_name2pool(CSTRING(base_arg));
    if (p) return oidplus(p->pool_base,offset);
    else return fd_type_error("pool id","hex2oid_prim",base_arg);}
  else return fd_type_error("pool id","hex2oid_prim",base_arg);
}

static lispval oidaddr_prim(lispval oid)
{
  FD_OID oidaddr = FD_OID_ADDR(oid);
  if (FD_OID_HI(oidaddr)) {
#if FD_STRUCT_OIDS
    unsigned long long addr = ((oid.hi)<<32)|(oid.lo);
#else
    unsigned long long addr = oidaddr;
#endif
    return FD_INT(addr);}
  else return FD_INT(FD_OID_LO(oid));
}

/* sumframe function */

static lispval sumframe_prim(lispval frames,lispval slotids)
{
  lispval results = EMPTY;
  DO_CHOICES(frame,frames) {
    lispval slotmap = fd_empty_slotmap();
    DO_CHOICES(slotid,slotids) {
      lispval value = fd_get(frame,slotid,EMPTY);
      if (FD_ABORTED(value)) {
        fd_decref(results); return value;}
      else if (fd_add(slotmap,slotid,value)<0) {
        fd_decref(results); fd_decref(value);
        return FD_ERROR;}
      fd_decref(value);}
    if (OIDP(frame)) {
      FD_OID addr = FD_OID_ADDR(frame);
      u8_string s = u8_mkstring("@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));
      lispval idstring = fd_lispstring(s);
      if (fd_add(slotmap,id_symbol,idstring)<0) {
        fd_decref(results); fd_decref(idstring);
        return FD_ERROR;}
      else fd_decref(idstring);}
    CHOICE_ADD(results,slotmap);}
  return results;
}

/* Graph walking functions */

static lispval applyfn(lispval fn,lispval node)
{
  if ((OIDP(fn)) || (SYMBOLP(fn)))
    return fd_frame_get(node,fn);
  else if (CHOICEP(fn)) {
    lispval results = EMPTY;
    DO_CHOICES(f,fn) {
      lispval v = applyfn(fn,node);
      if (FD_ABORTED(v)) {
        fd_decref(results);
        return v;}
      else {CHOICE_ADD(results,v);}}
    return results;}
  else if (FD_APPLICABLEP(fn))
    return fd_apply(fn,1,&node);
  else if (TABLEP(fn))
    return fd_get(fn,node,EMPTY);
  else return EMPTY;
}

static int walkgraph(lispval fn,lispval state,lispval arcs,
                      struct FD_HASHSET *seen,lispval *results)
{
  lispval next_state = EMPTY;
  DO_CHOICES(node,state)
    if (!(fd_hashset_get(seen,node))) {
      lispval output, expand;
      fd_hashset_add(seen,node);
      /* Apply the function */
      output = applyfn(fn,node);
      if (FD_ABORTED(output)) return fd_interr(output);
      else if (results) {CHOICE_ADD(*results,output);}
      else fd_decref(output);
      /* Do the expansion */
      expand = applyfn(arcs,node);
      if (FD_ABORTED(expand)) return fd_interr(expand);
      else {
        DO_CHOICES(next,expand)
          if (!(fd_hashset_get(seen,next))) {
            fd_incref(next); CHOICE_ADD(next_state,next);}}
      fd_decref(expand);}
  fd_decref(state);
  if (!(EMPTYP(next_state)))
    return walkgraph(fn,next_state,arcs,seen,results);
  else return 1;
}

static lispval forgraph(lispval fcn,lispval roots,lispval arcs)
{
  struct FD_HASHSET hashset; int retval;
  if ((OIDP(arcs)) || (SYMBOLP(arcs)) ||
      (FD_APPLICABLEP(arcs)) || (TABLEP(arcs))) {}
  else if (CHOICEP(arcs)) {
    DO_CHOICES(each,arcs)
      if (!((OIDP(each)) || (SYMBOLP(each)) ||
            (FD_APPLICABLEP(each)) || (TABLEP(each))))
        return fd_type_error("mapfn","mapgraph",each);}
  else return fd_type_error("mapfn","mapgraph",arcs);
  fd_init_hashset(&hashset,1024,FD_STACK_CONS);
  fd_incref(roots);
  retval = walkgraph(fcn,roots,arcs,&hashset,NULL);
  if (retval<0) {
    fd_decref((lispval)&hashset);
    return FD_ERROR;}
  else {
    fd_decref((lispval)&hashset);
    return VOID;}
}

static lispval mapgraph(lispval fcn,lispval roots,lispval arcs)
{
  lispval results = EMPTY; int retval; struct FD_HASHSET hashset;
  if ((OIDP(fcn)) || (SYMBOLP(fcn)) ||
      (FD_APPLICABLEP(fcn)) || (TABLEP(fcn))) {}
  else if (CHOICEP(fcn)) {
    DO_CHOICES(each,fcn)
      if (!((OIDP(each)) || (SYMBOLP(each)) ||
            (FD_APPLICABLEP(each)) || (TABLEP(each))))
        return fd_type_error("mapfn","mapgraph",each);}
  else return fd_type_error("mapfn","mapgraph",fcn);
  if ((OIDP(arcs)) || (SYMBOLP(arcs)) ||
      (FD_APPLICABLEP(arcs)) || (TABLEP(arcs))) {}
  else if (CHOICEP(arcs)) {
    DO_CHOICES(each,arcs)
      if (!((OIDP(each)) || (SYMBOLP(each)) ||
            (FD_APPLICABLEP(each)) || (TABLEP(each))))
        return fd_type_error("mapfn","mapgraph",each);}
  else return fd_type_error("mapfn","mapgraph",arcs);
  fd_init_hashset(&hashset,1024,FD_STACK_CONS); fd_incref(roots);
  retval = walkgraph(fcn,roots,arcs,&hashset,&results);
  if (retval<0) {
    fd_decref((lispval)&hashset);
    fd_decref(results);
    return FD_ERROR;}
  else {
    fd_decref((lispval)&hashset);
    return results;}
}

/* Helpful predicates */

static lispval dbloadedp(lispval arg1,lispval arg2)
{
  if (VOIDP(arg2))
    if (OIDP(arg1)) {
      fd_pool p = fd_oid2pool(arg1);
      if (fd_hashtable_probe(&(p->pool_changes),arg1)) {
        lispval v = fd_hashtable_probe(&(p->pool_changes),arg1);
        if ((v!=VOID) || (v!=FD_LOCKHOLDER)) {
          fd_decref(v); return FD_TRUE;}
        else return FD_FALSE;}
      else if (fd_hashtable_probe(&(p->pool_cache),arg1))
        return FD_TRUE;
      else return FD_FALSE;}
    else if (fd_hashtable_probe(&(fd_background->index_cache),arg1))
      return FD_TRUE;
    else return FD_FALSE;
  else if (INDEXP(arg2)) {
    fd_index ix = fd_indexptr(arg2);
    if (ix == NULL)
      return fd_type_error("index","loadedp",arg2);
    else if (fd_hashtable_probe(&(ix->index_cache),arg1))
      return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_POOLP(arg2))
    if (OIDP(arg1)) {
      fd_pool p = fd_lisp2pool(arg2);
      if (p == NULL)
        return fd_type_error("pool","loadedp",arg2);
      if (fd_hashtable_probe(&(p->pool_changes),arg1)) {
        lispval v = fd_hashtable_probe(&(p->pool_changes),arg1);
        if ((v!=VOID) || (v!=FD_LOCKHOLDER)) {
          fd_decref(v); return FD_TRUE;}
        else return FD_FALSE;}
      else if (fd_hashtable_probe(&(p->pool_cache),arg1))
        return FD_TRUE;
      else return FD_FALSE;}
    else return FD_FALSE;
  else if ((STRINGP(arg2)) && (OIDP(arg1))) {
    fd_pool p = fd_lisp2pool(arg2); fd_index ix;
    if (p)
      if (fd_hashtable_probe(&(p->pool_cache),arg1))
        return FD_TRUE;
      else return FD_FALSE;
    else ix = fd_indexptr(arg2);
    if (ix == NULL)
      return fd_type_error("pool/index","loadedp",arg2);
    else if (fd_hashtable_probe(&(ix->index_cache),arg1))
      return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("pool/index","loadedp",arg2);
}

static int oidmodifiedp(fd_pool p,lispval oid)
{
  if (fd_hashtable_probe(&(p->pool_changes),oid)) {
    lispval v = fd_hashtable_get(&(p->pool_changes),oid,VOID);
    int modified = 1;
    if ((VOIDP(v)) || (v == FD_LOCKHOLDER))
      modified = 0;
    else if (SLOTMAPP(v))
      if (FD_SLOTMAP_MODIFIEDP(v)) {}
      else modified = 0;
    else if (SCHEMAPP(v)) {
      if (FD_SCHEMAP_MODIFIEDP(v)) {}
      else modified = 0;}
    else {}
    fd_decref(v);
    return modified;}
  else return 0;
}

static lispval dbmodifiedp(lispval arg1,lispval arg2)
{
  if (VOIDP(arg2))
    if (OIDP(arg1)) {
      fd_pool p = fd_oid2pool(arg1);
      if (oidmodifiedp(p,arg1))
        return FD_TRUE;
      else return FD_FALSE;}
    else if ((FD_POOLP(arg1))||(TYPEP(arg1,fd_consed_pool_type))) {
      fd_pool p = fd_lisp2pool(arg1);
      if (p->pool_changes.table_n_keys)
        return FD_TRUE;
      else return FD_FALSE;}
    else if (INDEXP(arg1)) {
      fd_index ix = fd_lisp2index(arg1);
      if ((ix->index_adds.table_n_keys) ||
          (ix->index_drops.table_n_keys) ||
          (ix->index_stores.table_n_keys))
        return FD_TRUE;
      else return FD_FALSE;}
    else if (TABLEP(arg1)) {
      fd_ptr_type ttype = FD_PTR_TYPE(arg1);
      if ((fd_tablefns[ttype])&&(fd_tablefns[ttype]->modified)) {
        if ((fd_tablefns[ttype]->modified)(arg1,-1))
          return FD_TRUE;
        else return FD_FALSE;}
      else return FD_FALSE;}
    else return FD_FALSE;
  else if (INDEXP(arg2)) {
    fd_index ix = fd_indexptr(arg2);
    if (ix == NULL)
      return fd_type_error("index","loadedp",arg2);
    else if ((fd_hashtable_probe(&(ix->index_adds),arg1)) ||
             (fd_hashtable_probe(&(ix->index_drops),arg1)) ||
             (fd_hashtable_probe(&(ix->index_stores),arg1)))
      return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_POOLP(arg2))
    if (OIDP(arg1)) {
      fd_pool p = fd_lisp2pool(arg2);
      if (p == NULL)
        return fd_type_error("pool","loadedp",arg2);
      else if (oidmodifiedp(p,arg1))
        return FD_TRUE;
      else return FD_FALSE;}
    else return FD_FALSE;
  else if ((STRINGP(arg2)) && (OIDP(arg1))) {
    fd_pool p = fd_lisp2pool(arg2); fd_index ix;
    if (p)
      if (oidmodifiedp(p,arg1))
        return FD_TRUE;
      else return FD_FALSE;
    else ix = fd_indexptr(arg2);
    if (ix == NULL)
      return fd_type_error("pool/index","loadedp",arg2);
    else if ((fd_hashtable_probe(&(ix->index_adds),arg1)) ||
             (fd_hashtable_probe(&(ix->index_drops),arg1)) ||
             (fd_hashtable_probe(&(ix->index_stores),arg1)))
      return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("pool/index","loadedp",arg2);
}

static lispval db_writablep(lispval db)
{
  if (FD_POOLP(db)) {
    fd_pool p = fd_lisp2pool(db);
    int flags = p->pool_flags;
    if ( (flags) & (FD_STORAGE_READ_ONLY) )
      return FD_FALSE;
    else return FD_TRUE;}
  else if (FD_INDEXP(db)) {
    fd_index ix = fd_lisp2index(db);
    int flags = ix->index_flags;
    if ( (flags) & (FD_STORAGE_READ_ONLY) )
      return FD_FALSE;
    else return FD_TRUE;}
  else if (FD_TABLEP(db)) {
    if (fd_readonlyp(db))
      return FD_FALSE;
    else return FD_TRUE;}
  else return FD_FALSE;
}

/* Bloom filters */

static lispval make_bloom_filter(lispval n_entries,lispval allowed_error)
{
  struct FD_BLOOM *filter = (VOIDP(allowed_error))?
    (fd_init_bloom_filter(NULL,FIX2INT(n_entries),0.0004)) :
    (fd_init_bloom_filter(NULL,FIX2INT(n_entries),FD_FLONUM(allowed_error)));
  if (filter)
    return (lispval) filter;
  else return FD_ERROR;
}

#define BLOOM_DTYPE_LEN 1000

static lispval bloom_add(lispval filter,lispval value,
                        lispval raw_arg,
                        lispval ignore_errors)
{
  struct FD_BLOOM *bloom = (struct FD_BLOOM *)filter;
  int raw = (!(FALSEP(raw_arg)));
  int noerr = (!(FALSEP(ignore_errors)));
  int rv = fd_bloom_op(bloom,value,
                       ( ( (raw) ? (FD_BLOOM_RAW) : (0)) |
                         ( (noerr) ? (0) : (FD_BLOOM_ERR) ) |
                         (FD_BLOOM_ADD) ));
  if (rv<0)
    return FD_ERROR;
  else if (rv == 0)
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval bloom_check(lispval filter,lispval value,
                          lispval raw_arg,
                          lispval ignore_errors)
{
  struct FD_BLOOM *bloom = (struct FD_BLOOM *)filter;
  int raw = (!(FALSEP(raw_arg)));
  int noerr = (!(FALSEP(ignore_errors)));
  int rv = fd_bloom_op(bloom,value,
                       ( ( (raw) ? (FD_BLOOM_RAW) : (0)) |
                         ( (noerr) ? (0) : (FD_BLOOM_ERR) ) |
                         (FD_BLOOM_CHECK) ));
  if (rv<0)
    return FD_ERROR;
  else if (rv)
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval bloom_hits(lispval filter,lispval value,
                         lispval raw_arg,
                         lispval ignore_errors)
{
  struct FD_BLOOM *bloom = (struct FD_BLOOM *)filter;
  int raw = (!(FALSEP(raw_arg)));
  int noerr = (!(FALSEP(ignore_errors)));
  int rv = fd_bloom_op(bloom,value,
                       ( ( (raw) ? (FD_BLOOM_RAW) : (0)) |
                         ( (noerr) ? (0) : (FD_BLOOM_ERR) ) ));
  if (rv<0)
    return FD_ERROR;
  else return FD_INT(rv);
}

static lispval bloom_size(lispval filter)
{
  struct FD_BLOOM *bloom = (struct FD_BLOOM *)filter;
  return FD_INT(bloom->entries);
}

static lispval bloom_count(lispval filter)
{
  struct FD_BLOOM *bloom = (struct FD_BLOOM *)filter;
  return FD_INT(bloom->bloom_adds);
}

static lispval bloom_error(lispval filter)
{
  struct FD_BLOOM *bloom = (struct FD_BLOOM *)filter;
  return fd_make_double(bloom->error);
}

static lispval bloom_data(lispval filter)
{
  struct FD_BLOOM *bloom = (struct FD_BLOOM *)filter;
  return fd_bytes2packet(NULL,bloom->bytes,bloom->bf);
}

/* Registering procpools and procindexes */

static lispval def_procpool(lispval typesym,lispval handlers)
{
  if (!(FD_TABLEP(handlers)))
    return fd_err(fd_TypeError,"register_procpool",
                  "not a handler table",handlers);
  else if (FD_SYMBOLP(typesym)) {
    fd_register_procpool(FD_SYMBOL_NAME(typesym),handlers);
    return fd_incref(handlers);}
  else if (FD_STRINGP(typesym)) {
    fd_register_procpool(FD_CSTRING(typesym),handlers);
    return fd_incref(handlers);}
  else return fd_err(fd_TypeError,"register_procpool",NULL,typesym);
}

static lispval def_procindex(lispval typesym,lispval handlers)
{
  if (!(FD_TABLEP(handlers)))
    return fd_err(fd_TypeError,"register_procpool",
                  "not a handler table",handlers);
  else if (FD_SYMBOLP(typesym)) {
    fd_register_procindex(FD_SYMBOL_NAME(typesym),handlers);
    return fd_incref(handlers);}
  else if (FD_STRINGP(typesym)) {
    fd_register_procindex(FD_CSTRING(typesym),handlers);
    return fd_incref(handlers);}
  else return fd_err(fd_TypeError,"register_procpool",NULL,typesym);
}

static lispval procpoolp(lispval pool)
{
  fd_pool p = fd_poolptr(pool);
  if ( (p) && (p->pool_handler == &fd_procpool_handler) )
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval procindexp(lispval index)
{
  fd_index ix = fd_indexptr(index);
  if ( (ix) && (ix->index_handler == &fd_procindex_handler) )
    return FD_TRUE;
  else return FD_FALSE;
}

/* Initializing */

FD_EXPORT void fd_init_dbprims_c()
{
  u8_register_source_file(_FILEINFO);

  fd_idefn(fd_scheme_module,fd_make_cprim1("SLOTID?",slotidp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("LOADED?",dbloadedp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MODIFIED?",dbmodifiedp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("DB/WRITABLE?",db_writablep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("LOCKED?",oidlockedp,1));

  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim2("GET",fd_fget,2)));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim3("TEST",fd_ftest,2)));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprimn("TESTP",testp,3)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("ASSERT!",fd_assert,3)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("RETRACT!",fd_retract,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("GETSLOTS",fd_getkeys,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("SUMFRAME",sumframe_prim,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("GETPATH",getpath_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("GETPATH*",getpathstar_prim,1)));

  fd_def_evalfn(fd_scheme_module,"CACHEGET","",cacheget_evalfn);

  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim2("GET*",getstar,2)));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim3("PATH?",pathp,3)));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim3("INHERIT",inherit_prim,3)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("GET-BASIS",getbasis,2)));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("OID-VALUE",oidvalue,1,fd_oid_type,VOID));

  fd_idefn3(fd_scheme_module,"SET-OID-VALUE!",
            setoidvalue,FD_NEEDS_2_ARGS|FD_NDCALL,
            "`(SET-OID-VALUE! *oid* *value* [*nocopy*])` "
            "directly sets the value of *oid* to *value*. If "
            "the value is a slotmap or schemap, a copy is stored unless "
            "*nocopy* is not false (the default).",
            fd_oid_type,VOID,-1,VOID,-1,FD_FALSE);
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("LOCK-OID!",lockoid,2,
                           fd_oid_type,VOID,-1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("LOCK-OIDS!",lockoids,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("UNLOCK-OIDS!",unlockoids,0)));
  fd_idefn(fd_scheme_module,fd_make_cprim1("LOCKED-OIDS",lockedoids,1));

  fd_idefn2(fd_scheme_module,"%SET-OID-VALUE!",
            xsetoidvalue,FD_NEEDS_2_ARGS|FD_NDCALL,
            "`(%SET-OID-VALUE! *oid* *value* [*nocopy*])` "
            "directly sets the value of *oid* to *value*. If "
            "the value is a slotmap or schemap, a copy is stored unless "
            "*nocopy* is not false (the default).",
            fd_oid_type,VOID,-1,VOID);


  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL?",poolp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("INDEX?",indexp,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2("SET-CACHE-LEVEL!",set_cache_level,2));

  fd_idefn(fd_scheme_module,fd_make_cprim1("NAME->POOL",getpool,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("GETPOOL",getpool,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("POOL-LABEL",pool_label,1,
                                            -1,VOID,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-SOURCE",pool_source,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-BASE",pool_base,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-CAPACITY",pool_capacity,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-LOAD",pool_load,1));
  fd_idefn(fd_scheme_module,fd_make_cprim3("POOL-ELTS",pool_elts,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2("SET-POOL-NAMEFN!",set_pool_namefn,2));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-PREFIX",pool_prefix,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("SET-POOL-PREFIX!",set_pool_prefix,2,
            -1,VOID,fd_string_type,VOID));

  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-SOURCE",pool_source,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-ID",pool_id,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("OID-RANGE",oid_range,2,
                           fd_oid_type,VOID,fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("OID-VECTOR",oid_vector,2,
                           fd_oid_type,VOID,fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("RANDOM-OID",random_oid,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-VECTOR",pool_vec,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("OID-HI",oidhi,1,fd_oid_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("OID-LO",oidlo,1,fd_oid_type,VOID));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("OID?",oidp,1));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim1x("OID-POOL",oidpool,1,fd_oid_type,VOID));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x("IN-POOL?",inpoolp,2,
                           fd_oid_type,VOID,-1,VOID));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x("VALID-OID?",validoidp,1,
                           fd_oid_type,VOID,-1,VOID));

  fd_idefn(fd_xscheme_module,fd_make_cprim2("USE-POOL",use_pool,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim2("ADJUNCT-POOL",adjunct_pool,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim2("TRY-POOL",try_pool,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("CACHECOUNT",cachecount,0));
  fd_defalias(fd_xscheme_module,"LOAD-POOL","TRY-POOL");

  fd_idefn2(fd_xscheme_module,"USE-INDEX",use_index,1,
            "(USE-INDEX *spec* [*opts*]) adds an index to the search background",
            -1,VOID,-1,VOID);
  fd_idefn2(fd_xscheme_module,"OPEN-INDEX",open_index,1,
            "(OPEN-INDEX *spec* [*opts*]) opens and returns an index",
            -1,VOID,-1,VOID);
  fd_idefn2(fd_xscheme_module,"REGISTER-INDEX",register_index,1,
            "(REGISTER-INDEX *spec* [*opts*]) opens, registers, and returns an index",
            -1,VOID,-1,VOID);
  fd_idefn2(fd_xscheme_module,"CONS-INDEX",cons_index,1,
            "(CONS-INDEX *spec* [*opts*]) opens and returns an unregistered index",
            -1,VOID,-1,VOID);
  fd_defalias(fd_xscheme_module,"TEMP-INDEX","CONS-INDEX");

  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x("MAKE-INDEX",make_index,2,
                           fd_string_type,VOID,-1,VOID));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x("MAKE-POOL",make_pool,2,
                           fd_string_type,VOID,-1,VOID));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x("OPEN-POOL",open_pool,1,
                           fd_string_type,VOID,-1,FD_FALSE));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim
           (fd_make_cprimn("FRAME-CREATE",frame_create_lexpr,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("FRAME-UPDATE",frame_update_lexpr,3)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("MODIFY-FRAME",modify_frame_lexpr,3)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim4("SEQ->FRAME",seq2frame_prim,3)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2("ALLOCATE-OIDS",allocate_oids,1));

  fd_idefn2(fd_scheme_module,"OID-PLUS",oid_plus_prim,1,
            "Adds an integer to an OID address and returns that OID",
            fd_oid_type,VOID,
            fd_fixnum_type,FD_INT(1));
  fd_idefn2(fd_scheme_module,"OID-MINUS",oid_minus_prim,1,
            "Returns the difference between two OIDs. If the second "
            "argument is a pool, the base of the pool is used for "
            "comparision",
            fd_oid_type,VOID,-1,FD_VOID);
  fd_defalias(fd_scheme_module,"OID+","OID-PLUS");
  fd_defalias(fd_scheme_module,"OID-","OID-MINUS");

  fd_idefn2(fd_scheme_module,"OID-OFFSET",oid_offset_prim,1,
            "Returns the offset of an OID from a container and #f if "
            "it is not in the container. When the second argument "
            "is an OID, it is used as the base of container; if it "
            "is a pool, it is used as the container. Without a second "
            "argument the registered pool for the OID is used as the "
            "container.",
            fd_oid_type,VOID,-1,VOID);

  fd_idefn2(fd_scheme_module,"MAKE-OID",make_oid_prim,1,
            "(MAKE-OID *addr* [*lo*]) returns an OID from numeric "
            "components. If *lo* is not provided (or #f) *addr* is used "
            "as the complete address. Otherwise, *addr* specifies "
            "the high part of the OID address.",
            -1,FD_VOID,-1,FD_VOID);
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("OID->STRING",oid2string_prim,1,
                           fd_oid_type,VOID,-1,VOID));
  fd_idefn1(fd_scheme_module,"OID-ADDR",oidaddr_prim,1,
            "Returns the absolute numeric address of an OID",
            fd_oid_type,VOID);
  fd_defalias(fd_scheme_module,"OID@","OID-ADDR");

#ifdef FD_OID_BASE_ID
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("OID-PTRDATA",oid_ptrdata_prim,1,
                           fd_oid_type,VOID));
#endif

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("OID->HEX",oidhex_prim,1,
                           fd_oid_type,VOID,
                           -1,VOID));
  fd_defalias(fd_scheme_module,"OIDHEX","OID->HEX");
  fd_defalias(fd_scheme_module,"HEXOID","OID->HEX");
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("HEX->OID",hex2oid_prim,2,
                           -1,VOID,-1,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("OID/B32",oidb32_prim,1,
                           fd_oid_type,VOID,
                           -1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("B32/OID",b32oid_prim,2,
                           -1,VOID,-1,VOID));


  fd_idefn(fd_scheme_module,
           fd_make_cprim6x("MAKE-MEMPOOL",make_mempool,2,
                           fd_string_type,VOID,
                           fd_oid_type,VOID,
                           fd_fixnum_type,(FD_INT(1024*1024)),
                           fd_fixnum_type,(FD_INT(0)),
                           -1,FD_FALSE,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("CLEAN-MEMPOOL",clean_mempool,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("RESET-MEMPOOL",reset_mempool,1));

  fd_idefn6(fd_scheme_module,"MAKE-PROCPOOL",make_procpool,4,
            "Returns a pool implemented by userspace functions",
            fd_string_type,FD_VOID,fd_oid_type,FD_VOID,fd_fixnum_type,FD_VOID,
            -1,FD_VOID,-1,FD_VOID,fd_fixnum_type,FD_FIXZERO);

  fd_idefn5(fd_scheme_module,"MAKE-PROCINDEX",make_procindex,2,
            "Returns a pool implemented by userspace functions",
            fd_string_type,FD_VOID,-1,FD_VOID,-1,FD_VOID,
            fd_string_type,FD_VOID,fd_string_type,FD_VOID);

  fd_idefn2(fd_scheme_module,"DEFPOOLTYPE",def_procpool,2,
            "Registers handlers for a procpool",
            -1,FD_VOID,-1,FD_VOID);
  fd_idefn2(fd_scheme_module,"DEFINDEXTYPE",def_procindex,2,
            "Registers handlers for a procindex",
            -1,FD_VOID,-1,FD_VOID);

  fd_idefn1(fd_scheme_module,"PROCPOOL?",procpoolp,1,
            "Returns #t if it's argument is a procpool",
            -1,FD_VOID);
  fd_idefn1(fd_scheme_module,"PROCINDEX?",procindexp,1,
            "Returns #t if it's argument is a procindex",
            -1,FD_VOID);


  fd_idefn(fd_scheme_module,
           fd_make_cprim10x("MAKE-EXTPOOL",make_extpool,4,
                            fd_string_type,VOID,
                            fd_oid_type,VOID,
                            fd_fixnum_type,VOID,
                            -1,VOID,-1,VOID,
                            -1,VOID,-1,VOID,
                            -1,VOID,-1,FD_TRUE,
                            -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("EXTPOOL-CACHE!",extpool_setcache,3,
                           fd_pool_type,VOID,fd_oid_type,VOID,
                           -1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTPOOL-FETCHFN",extpool_fetchfn,1,
                           fd_pool_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTPOOL-SAVEFN",extpool_savefn,1,
                           fd_pool_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTPOOL-LOCKFN",extpool_lockfn,1,
                           fd_pool_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTPOOL-STATE",extpool_state,1,
                           fd_pool_type,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim6x("MAKE-EXTINDEX",make_extindex,2,
                           fd_string_type,VOID,
                           -1,VOID,-1,VOID,-1,VOID,
                           -1,FD_TRUE,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim6x("CONS-EXTINDEX",cons_extindex,2,
                           fd_string_type,VOID,
                           -1,VOID,-1,VOID,-1,VOID,
                           -1,FD_TRUE,-1,FD_FALSE));

  fd_idefn1(fd_scheme_module,"EXTINDEX?",extindexp,1,
            "`(EXTINDEX *arg*)` returns #t if *arg* is an extindex, "
            "#f otherwise",
            -1,VOID);

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("EXTINDEX-DECACHE!",extindex_decache,1,
                           -1,VOID,-1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("EXTINDEX-CACHEADD!",extindex_cacheadd,3,
                           -1,VOID,-1,VOID,
                           -1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTINDEX-FETCHFN",extindex_fetchfn,1,
                           -1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTINDEX-COMMITFN",extindex_commitfn,1,
                           -1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTINDEX-STATE",extindex_state,1,
                           -1,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("??",find_frames_lexpr,2)));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprimn("FIND-FRAMES",find_frames_lexpr,3)));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprimn("XFIND-FRAMES",xfind_frames_lexpr,3)));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprim3
                          ("PREFETCH-SLOTVALS!",prefetch_slotvals,3)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn
                          ("FIND-FRAMES/PREFETCH!",find_frames_prefetch,2)));
  fd_defalias(fd_scheme_module,"FIND-FRAMES-PREFETCH!","FIND-FRAMES/PREFETCH!");
  fd_defalias(fd_scheme_module,"\?\?/PREFETCH!","FIND-FRAMES/PREFETCH!");
  fd_defalias(fd_scheme_module,"\?\?!","FIND-FRAMES/PREFETCH!");

  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprimn("SWAPOUT",swapout_lexpr,0)));
  fd_idefn(fd_xscheme_module,fd_make_cprimn("COMMIT",commit_lexpr,0));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprim2x("FINISH-OIDS",finish_oids,1,
                                          -1,VOID,
                                          fd_pool_type,VOID)));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprim1x("COMMIT-OIDS",commit_oids,
                                          1,-1,VOID)));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x("COMMIT-POOL",commit_pool,1,
                           fd_pool_type,VOID,-1,VOID));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim1x("COMMIT-FINISHED",commit_finished,1,
                           fd_pool_type,VOID));
  fd_idefn3(fd_xscheme_module,"POOL/STOREN!",pool_storen_prim,3,
            "Stores values in a pool, skipping the object cache",
            -1,VOID,fd_vector_type,VOID,
            fd_vector_type,VOID);
  fd_idefn2(fd_xscheme_module,"POOL/FETCHN",pool_fetchn_prim,2|FD_NDCALL,
            "Fetches values from a pool, skipping the object cache",
            -1,VOID,-1,VOID);

  fd_idefn(fd_xscheme_module,fd_make_cprim1("POOL-CLOSE",pool_close_prim,1));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim1("CLEAR-SLOTCACHE!",clear_slotcache,0));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim0("CLEARCACHES",clearcaches));

  fd_idefn(fd_xscheme_module,fd_make_cprim0("SWAPCHECK",swapcheck_prim));

  fd_idefn2(fd_scheme_module,"PREFETCH-OIDS!",
            prefetch_oids_prim,(FD_NEEDS_1_ARG|FD_NDCALL),
            "'(PREFETCH-OIDS! oids [pool])' prefetches OIDs from pool",
            -1,VOID,-1,VOID);
  fd_idefn2(fd_scheme_module,"POOL-PREFETCH!",
            pool_prefetch_prim,(FD_NEEDS_2_ARGS|FD_NDCALL),
            "'(POOL-PREFETCH! pool oids)' prefetches OIDs from pool",
            -1,VOID,-1,VOID);
  fd_idefn2(fd_scheme_module,"PREFETCH-KEYS!",prefetch_keys,
            (FD_NEEDS_1_ARG|FD_NDCALL),
            "(PREFETCH-KEYS! *keys*) or (PREFETCH-KEYS! *index* *keys*)",
            -1,FD_VOID,-1,FD_VOID);
  fd_idefn2(fd_scheme_module,"INDEX-PREFETCH!",index_prefetch_keys,
            (FD_NEEDS_2_ARGS|FD_NDCALL),"(INDEX-PREFETCH! *index* *keys*)",
            -1,FD_VOID,-1,FD_VOID);
  fd_idefn2(fd_xscheme_module,"INDEX/FETCHN",index_fetchn_prim,2|FD_NDCALL,
            "Fetches values from an index, skipping the index cache",
            -1,VOID,-1,VOID);
  fd_idefn5(fd_scheme_module,"INDEX/SAVE!",index_save_prim,FD_NEEDS_2_ARGS,
            "(INDEX-PREFETCH! *index* *keys*)",
            -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID);
  fd_idefn1(fd_scheme_module,"FETCHOIDS",fetchoids_prim,FD_NEEDS_1_ARG|FD_NDCALL,
            "(FETCHOIDS *oids*) returns *oids* after prefetching their values.",
            -1,FD_VOID);

  fd_idefn(fd_scheme_module,fd_make_cprim1("CACHED-OIDS",cached_oids,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CACHED-KEYS",cached_keys,0));

  fd_idefn1(fd_scheme_module,"CACHE-LOAD",cache_load,1,
            "(CACHE-LOAD *pool-or-index*) returns the number of "
            "cached items in a database.",
            -1,FD_VOID);
  fd_idefn1(fd_scheme_module,"CHANGE-LOAD",change_load,1,
            "Returns the number of items modified "
            "(or locked for modification) in a database",
            -1,FD_VOID);

  fd_idefn(fd_scheme_module,fd_make_cprim1("INDEX-SOURCE",index_source_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("INDEX-ID",index_id,1));

  fd_idefn2(fd_xscheme_module,"MAKE-AGGREGATE-INDEX",make_aggregate_index,
            FD_NDCALL|FD_NEEDS_1_ARG,
            "Creates an aggregate index from a collection of other indexes",
            -1,FD_VOID,-1,FD_FALSE);
  fd_idefn1(fd_xscheme_module,"AGGREGATE-INDEX?",aggregate_indexp,1,
            "(AGGREGATE-INDEX? *arg*) => true if *arg* is an aggregate index",
            -1,FD_VOID);
  fd_idefn2(fd_xscheme_module,
            "EXTEND-AGGREGATE-INDEX!",extend_aggregate_index,2,
            "(EXTEND-AGGREGATE-INDEX! *agg* *index*) "
            "adds *index* to the aggregate index *agg*",
            -1,FD_VOID,-1,FD_VOID);
  fd_defalias(fd_xscheme_module,"ADD-TO-AGGREGATE-INDEX!",
              "EXTEND-AGGREGATE-INDEX!");

  fd_idefn1(fd_xscheme_module,"TEMPINDEX?",tempindexp,1,
            "(TEMPINDEX? *arg*) returns #t if *arg* is a temporary index.",
            -1,VOID);
  
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprim4("INDEX-FRAME",index_frame_prim,3)));
  fd_idefn(fd_xscheme_module,fd_make_cprim3("INDEX-SET!",index_set,3));
  fd_idefn(fd_xscheme_module,fd_make_cprim3("INDEX-ADD!",index_add,3));
  fd_idefn(fd_xscheme_module,fd_make_cprim2("INDEX-GET",index_get,2));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("INDEX-KEYS",index_keys,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("INDEX-KEYSVEC",index_keysvec,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim2("INDEX-SIZES",index_sizes,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("INDEX-SOURCE",index_source,1));
  fd_idefn2(fd_xscheme_module,"INDEX/MERGE!",index_merge,2,
            "Merges a hashtable into the ADDS of an index "
            "as a batch operation",
            -1,VOID,-1,VOID);
  fd_defalias(fd_xscheme_module,"INDEX-MERGE!","INDEX/MERGE!");
  fd_idefn2(fd_xscheme_module,"SLOTINDEX/MERGE!",slotindex_merge,2,
            "Merges a hashtable or temporary index into the ADDS of an index "
            "as a batch operation, trying to handle conversions between "
            "slotkeys if needed.",
            -1,VOID,-1,VOID);
  fd_idefn1(fd_xscheme_module,"CLOSE-INDEX",close_index_prim,1,
            "(INDEX-CLOSE *index*) closes any resources associated with *index*",
            -1,VOID);
  fd_idefn1(fd_xscheme_module,"COMMIT-INDEX",commit_index_prim,1,
            "(INDEX-COMMIT *index*) saves any buffered changes to *index*",
            -1,VOID);

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("SUGGEST-HASH-SIZE",suggest_hash_size,1,
                           fd_fixnum_type,VOID));
  fd_idefn(fd_xscheme_module,fd_make_cprim3("INDEX-DECACHE",index_decache,2));
  fd_idefn(fd_xscheme_module,fd_make_cprim2("BGDECACHE",bgdecache,1));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprimn("PICK",pick_lexpr,2)));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprimn("PREFER",prefer_lexpr,2)));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprimn("REJECT",reject_lexpr,2)));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprimn("AVOID",avoid_lexpr,2)));

  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprimn("%PICK",prim_pick_lexpr,2)));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprimn("%PREFER",prim_prefer_lexpr,2)));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprimn("%REJECT",prim_reject_lexpr,2)));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprimn("%AVOID",prim_avoid_lexpr,2)));

  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprim3("MAPGRAPH",mapgraph,3)));
  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprim3("FORGRAPH",forgraph,3)));

  fd_idefn3(fd_xscheme_module,"USE-ADJUNCT",use_adjunct,1,
            "(table [slot] [pool])\n"
            "arranges for *table* to store values of the slotid *slot* "
            "for objects in *pool*. If *pool* is not specified, "
            "the adjunct is declared globally.",
            -1,VOID,-1,VOID,-1,VOID);
  fd_idefn3(fd_xscheme_module,"ADJUNCT!",add_adjunct,3,
            "(pool slot table)\n"
            "arranges for *table* to store values of the slotid *slot* "
            "for objects in *pool*. Table can be an in-memory table, "
            "an index or an adjunct pool",
            -1,VOID,-1,VOID,-1,VOID);
  fd_defalias(fd_xscheme_module,"ADD-ADJUNCT!","ADJUNCT!");
  fd_idefn1(fd_xscheme_module,"GET-ADJUNCTS",get_adjuncts,1,
            "`(GET_ADJUNCTS pool)\\n"
            "Gets the adjuncts associated with the specified pool",
            -1,VOID);

  fd_idefn1(fd_xscheme_module,"ADJUNCT?",isadjunctp,1,
            "`(ADJUNCT? pool)`\n"
            "Returns true if *pool* is an adjunct pool",
            -1,VOID);

  fd_idefn2(fd_scheme_module,"MAKE-BLOOM-FILTER",make_bloom_filter,1,
            "Creates a bloom filter for a a number of items and an error rate",
            fd_fixnum_type,VOID,fd_flonum_type,VOID);

  fd_idefn4(fd_scheme_module,"BLOOM/ADD!",bloom_add,FD_NEEDS_2_ARGS|FD_NDCALL,
            "(BLOOM/ADD! *filter* *key* [*raw*]) adds a key to a bloom filter. "
            "The *raw* argument indicates that the key is a string or packet "
            "should be added to the filter. Otherwise, the binary DTYPE "
            "representation for the value is added to the filter.",
            fd_bloom_filter_type,VOID,-1,VOID,-1,FD_FALSE,-1,FD_FALSE);

  fd_idefn4(fd_scheme_module,"BLOOM/ADD",bloom_add,1|FD_NDCALL,
            "(BLOOM/ADD! *filter* *keys* [*raw*] [*noerr*]) "
            "adds the keys *keys* to *filter*, returning #t if it was added "
            "(and, so, not originally present). "
            "If *raw* is true, *values* must be strings or packets and their "
            "byte values are used directly; otherwise, values are converted to "
            "dtypes before being tested. If *noerr* is true, type or conversion "
            "errors are just considered misses and ignored.",
            fd_bloom_filter_type,VOID,-1,VOID,-1,FD_FALSE,-1,FD_FALSE);

  fd_idefn4(fd_scheme_module,"BLOOM/CHECK",bloom_check,1|FD_NDCALL,
            "(BLOOM/CHECK *filter* *keys* [*raw*] [*noerr*]) "
            "returns true if any of *keys* are probably found in *filter*. "
            "If *raw* is true, *keys* must be strings or packets and their "
            "byte values are used directly; otherwise, values are converted to "
            "dtypes before being tested. If *noerr* is true, type or conversion "
            "errors are just considered misses and ignored.",
            fd_bloom_filter_type,VOID,-1,VOID,-1,FD_FALSE,-1,FD_FALSE);
  fd_idefn4(fd_scheme_module,"BLOOM/HITS",bloom_hits,1|FD_NDCALL,
            "(BLOOM/HITS *filter* *keys* [*raw*] [*noerr*]) "
            "returns the number of *keys* probably found in *filter*. "
            "If *raw* is true, *keys* must be strings or packets and their "
            "byte values are used directly; otherwise, values are converted to "
            "dtypes before being tested. If *noerr* is true, type or conversion "
            "errors are just considered misses and ignored.",
            fd_bloom_filter_type,VOID,-1,VOID,-1,FD_FALSE,-1,FD_FALSE);

  fd_idefn1(fd_scheme_module,"BLOOM-SIZE",bloom_size,1,
            "Returns the size (in bytes) of a bloom filter ",
            fd_bloom_filter_type,VOID);
  fd_idefn1(fd_scheme_module,"BLOOM-COUNT",bloom_count,1,
            "Returns the number of objects added to a bloom filter",
            fd_bloom_filter_type,VOID);
  fd_idefn1(fd_scheme_module,"BLOOM-ERROR",bloom_error,1,
            "Returns the error threshold (a flonum) for the filter",
            fd_bloom_filter_type,VOID);
  fd_idefn1(fd_scheme_module,"BLOOM-DATA",bloom_data,1,
            "Returns the bytes of the filter as a packet",
            fd_bloom_filter_type,VOID);


  id_symbol = fd_intern("%ID");
  padjuncts_symbol = fd_intern("%ADJUNCTS");
  pools_symbol = fd_intern("POOLS");
  indexes_symbol = fd_intern("INDEXES");
  drop_symbol = fd_intern("DROP");
  flags_symbol = fd_intern("FLAGS");
  register_symbol = fd_intern("REGISTER");
  readonly_symbol = fd_intern("READONLY");
  phased_symbol = fd_intern("PHASED");

  sparse_symbol    = fd_intern("SPARSE");
  adjunct_symbol    = fd_intern("ADJUNCT");
  background_symbol = fd_intern("BACKGROUND");
  repair_symbol = fd_intern("REPAIR");

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
