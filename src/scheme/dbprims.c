/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/methods.h"
#include "framerd/sequences.h"
#include "framerd/dbprims.h"
#include "framerd/numbers.h"
#include "framerd/fdregex.h"

#include "libu8/u8printf.h"

static fdtype pools_symbol, indices_symbol, id_symbol, drop_symbol;

static fdtype slotidp(fdtype arg)
{
  if ((FD_OIDP(arg)) || (FD_SYMBOLP(arg))) return FD_TRUE;
  else return FD_FALSE;
}

/* Finding frames, etc. */

static fdtype find_frames_lexpr(int n,fdtype *args)
{
  if (n%2)
    if (FD_FALSEP(args[0]))
      return fd_bgfinder(n-1,args+1);
    else return fd_finder(args[0],n-1,args+1);
  else return fd_bgfinder(n,args);
}

/* This is like find_frames but ignores any slot/value pairs
   whose values are empty (and thus would rule out any results at all). */
static fdtype xfind_frames_lexpr(int n,fdtype *args)
{
  int i=(n%2); while (i<n)
    if (FD_EMPTY_CHOICEP(args[i+1])) {
      fdtype *slotvals=u8_alloc_n((n),fdtype), results;
      int j=0; i=1; while (i<n)
        if (FD_EMPTY_CHOICEP(args[i+1])) i=i+2;
        else {
          slotvals[j]=args[i]; j++; i++;
          slotvals[j]=args[i]; j++; i++;}
      if (n%2)
        if (FD_FALSEP(args[0]))
          results=fd_bgfinder(j,slotvals);
        else results=fd_finder(args[0],j,slotvals);
      else results=fd_bgfinder(j,slotvals);
      u8_free(slotvals);
      return results;}
    else i=i+2;
  if (n%2)
    if (FD_FALSEP(args[0]))
      return fd_bgfinder(n-1,args+1);
    else return fd_finder(args[0],n-1,args+1);
  else return fd_bgfinder(n,args);
}

static fdtype prefetch_slotvals(fdtype index,fdtype slotids,fdtype values)
{
  fd_index ix=fd_indexptr(index);
  if (ix) fd_find_prefetch(ix,slotids,values);
  else return fd_type_error("index","prefetch_slotvals",index);
  return FD_VOID;
}

static fdtype find_frames_prefetch(int n,fdtype *args)
{
  int i=(n%2);
  fd_index ix=((n%2) ? (fd_indexptr(args[0])) : ((fd_index)(fd_background)));
  if (FD_EXPECT_FALSE(ix==NULL))
    return fd_type_error("index","prefetch_slotvals",args[0]);
  else while (i<n) {
    FD_DO_CHOICES(slotid,args[i]) {
      if ((FD_SYMBOLP(slotid)) || (FD_OIDP(slotid))) {}
      else return fd_type_error("slotid","find_frames_prefetch",slotid);}
    i=i+2;}
  i=(n%2); while (i<n) {
    fdtype slotids=args[i], values=args[i+1];
    fd_find_prefetch(ix,slotids,values);
    i=i+2;}
  return FD_VOID;
}

static void hashtable_index_frame(fdtype ix,
                                  fdtype frames,fdtype slotids,
                                  fdtype values)
{
  if (FD_VOIDP(values)) {
    FD_DO_CHOICES(frame,frames) {
      FD_DO_CHOICES(slotid,slotids) {
        fdtype values=((FD_OIDP(frame)) ?
                       (fd_frame_get(frame,slotid)) :
                       (fd_get(frame,slotid,FD_EMPTY_CHOICE)));
        FD_DO_CHOICES(value,values) {
          fdtype key=fd_conspair(fd_incref(slotid),fd_incref(value));
          fd_add(ix,key,frame);
          fd_decref(key);}
        fd_decref(values);}}}
  else {
    FD_DO_CHOICES(slotid,slotids) {
      FD_DO_CHOICES(value,values) {
        fdtype key=fd_conspair(fd_incref(slotid),fd_incref(value));
        fd_add(ix,key,frames);
        fd_decref(key);}}}
}

static fdtype index_frame_prim
  (fdtype indices,fdtype frames,fdtype slotids,fdtype values)
{
  if (FD_CHOICEP(indices)) {
    FD_DO_CHOICES(index,indices)
      if (FD_HASHTABLEP(index))
        hashtable_index_frame(index,frames,slotids,values);
      else {
        fd_index ix=fd_indexptr(index);
        if (FD_EXPECT_FALSE(ix==NULL))
          return fd_type_error("index","index_frame_prim",index);
        else if (fd_index_frame(ix,frames,slotids,values)<0)
          return FD_ERROR_VALUE;}}
  else if (FD_HASHTABLEP(indices)) {
    hashtable_index_frame(indices,frames,slotids,values);
    return FD_VOID;}
  else {
    fd_index ix=fd_indexptr(indices);
    if (FD_EXPECT_FALSE(ix==NULL))
      return fd_type_error("index","index_frame_prim",indices);
    else if (fd_index_frame(ix,frames,slotids,values)<0)
      return FD_ERROR_VALUE;}
  return FD_VOID;
}

/* Pool and index functions */

static fdtype poolp(fdtype arg)
{
  if (FD_POOLP(arg)) return FD_TRUE; else return FD_FALSE;
}

static fdtype indexp(fdtype arg)
{
  if ((FD_INDEXP(arg))||(FD_PRIM_TYPEP(arg,fd_raw_index_type)))
    return FD_TRUE; else return FD_FALSE;
}

static fdtype getpool(fdtype arg)
{
  fd_pool p=NULL;
  if (FD_POOLP(arg)) return fd_incref(arg);
  else if (FD_STRINGP(arg))
    p=fd_name2pool(FD_STRDATA(arg));
  else if (FD_OIDP(arg)) p=fd_oid2pool(arg);
  if (p) return fd_pool2lisp(p);
  else return FD_EMPTY_CHOICE;
}

static fd_exception Unknown_PoolName=_("Unknown pool name");

static fdtype set_pool_namefn(fdtype arg,fdtype method)
{
  fd_pool p=NULL;
  if (FD_POOLP(arg))
    p=fd_lisp2pool(arg);
  else if (FD_STRINGP(arg)) {
    p=fd_name2pool(FD_STRDATA(arg));
    if (!(p)) return fd_err
                (Unknown_PoolName,"set_pool_namefn",NULL,arg);}
  else if (FD_OIDP(arg))
    p=fd_oid2pool(arg);
  else return fd_type_error(_("pool"),"set_pool_namefn",arg);
  if ((FD_OIDP(method))||(FD_SYMBOLP(method))||(FD_APPLICABLEP(method))) {
    fd_set_pool_namefn(p,method);
    return FD_VOID;}
  else return fd_type_error(_("namefn"),"set_pool_namefn",method);
}

static fdtype set_cache_level(fdtype arg,fdtype level)
{
  if (!(FD_FIXNUMP(level)))
    return fd_type_error("fixnum","set_cache_level",level);
  else if (FD_POOLP(arg)) {
    fd_pool p=fd_lisp2pool(arg);
    if (p) fd_pool_setcache(p,FD_FIX2INT(level));
    else return FD_ERROR_VALUE;
    return FD_VOID;}
  else if ((FD_INDEXP(arg))||(FD_PRIM_TYPEP(arg,fd_raw_index_type))) {
    fd_index ix=fd_indexptr(arg);
    if (ix) fd_index_setcache(ix,FD_FIX2INT(level));
    else return fd_type_error("index","index_frame_prim",arg);
    return FD_VOID;}
  else return fd_type_error("pool or index","set_cache_level",arg);
}

static fdtype use_pool(fdtype arg1,fdtype arg2)
{
  if (FD_POOLP(arg1)) return fd_incref(arg1);
  else if (!(FD_STRINGP(arg1)))
    return fd_type_error(_("string"),"use_pool",arg1);
  else if (FD_VOIDP(arg2)) {
    fdtype results=FD_EMPTY_CHOICE;
    u8_byte *copy=u8_strdup(FD_STRDATA(arg1));
    u8_byte *start=copy, *end=strchr(start,';');
    if (end) *end='\0'; while (start) {
      if (strchr(start,'@')) {
        fdtype temp;
        if (fd_use_pool(start)==NULL) {
          fd_decref(results); u8_free(copy);
          return FD_ERROR_VALUE;}
        else temp=fd_find_pools_by_cid(start);
        FD_ADD_TO_CHOICE(results,temp);}
      else {
        fd_pool p=fd_use_pool(start);
        if (p==NULL) {
          fd_decref(results); u8_free(copy);
          return FD_ERROR_VALUE;}
        else {
          fdtype pval=fd_pool2lisp(p);
          FD_ADD_TO_CHOICE(results,pval);}}
      if ((end) && (end[1])) {
        start=end+1; end=strchr(start,';');
        if (end) *end='\0';}
      else start=NULL;}
    u8_free(copy);
    return results;}
  else if (!(FD_STRINGP(arg2)))
    return fd_type_error(_("string"),"use_pool",arg2);
  else {
    fd_pool p=fd_name2pool(FD_STRDATA(arg2));
    if (p==NULL) p=fd_use_pool(FD_STRDATA(arg1));
    if (p) return fd_pool2lisp(p);
    else return FD_ERROR_VALUE;}
}

static fdtype use_index(fdtype arg)
{
  fd_index ix=NULL;
  if (FD_INDEXP(arg)) {
    ix=fd_indexptr(arg);
    if (ix) fd_add_to_background(ix);
    else return fd_type_error("index","index_frame_prim",arg);
    return fd_incref(arg);}
  else if (FD_STRINGP(arg))
    if (strchr(FD_STRDATA(arg),';')) {
      /* We explicitly handle ; separated arguments here, so that
         we can return the choice of indices. */
      fdtype results=FD_EMPTY_CHOICE;
      u8_byte *copy=u8_strdup(FD_STRDATA(arg));
      u8_byte *start=copy, *end=strchr(start,';');
      *end='\0'; while (start) {
        fd_index ix=fd_use_index(start);
        if (ix==NULL) {
          u8_free(copy);
          fd_decref(results);
          return FD_ERROR_VALUE;}
        else {
          fdtype ixv=fd_index2lisp(ix);
          FD_ADD_TO_CHOICE(results,ixv);}
        if ((end) && (end[1])) {
          start=end+1; end=strchr(start,';');
          if (end) *end='\0';}
        else start=NULL;}
      u8_free(copy);
      return results;}
    else ix=fd_use_index(FD_STRDATA(arg));
  else return fd_type_error(_("index spec"),"use_index",arg);
  if (ix) return fd_index2lisp(ix);
  else return FD_ERROR_VALUE;
}

static fdtype open_index(fdtype arg,fdtype consed_arg)
{
  int consed=(!((FD_FALSEP(consed_arg))||(FD_VOIDP(consed_arg))));
  fd_index ix=NULL;
  if (FD_STRINGP(arg))
    if (strchr(FD_STRDATA(arg),';')) {
      /* We explicitly handle ; separated arguments here, so that
         we can return the choice of indices. */
      fdtype results=FD_EMPTY_CHOICE;
      u8_byte *copy=u8_strdup(FD_STRDATA(arg));
      u8_byte *start=copy, *end=strchr(start,';');
      *end='\0'; while (start) {
        fd_index ix=fd_open_index_x(start,consed);
        if (ix==NULL) {
          u8_free(copy);
          fd_decref(results);
          return FD_ERROR_VALUE;}
        else {
          fdtype ixv=fd_index2lisp(ix);
          FD_ADD_TO_CHOICE(results,ixv);}
        if ((end) && (end[1])) {
          start=end+1; end=strchr(start,';');
          if (end) *end='\0';}
        else start=NULL;}
      u8_free(copy);
      return results;}
    else return fd_index2lisp(fd_open_index_x(FD_STRDATA(arg),consed));
  else if (FD_INDEXP(arg)) return arg;
  else if (FD_PRIM_TYPEP(arg,fd_raw_index_type))
    return fd_incref(arg);
  else fd_seterr(fd_TypeError,"use_index",NULL,fd_incref(arg));
  if (ix) return fd_index2lisp(ix);
  else return FD_ERROR_VALUE;
}
static fdtype oidvalue(fdtype arg)
{
  return fd_oid_value(arg);
}
static fdtype setoidvalue(fdtype o,fdtype v,fdtype nocopy)
{
  int retval;
  if (FD_TRUEP(nocopy)) {fd_incref(v);}
  else if (FD_SLOTMAPP(v)) {
    v=fd_deep_copy(v);
    FD_SLOTMAP_MARK_MODIFIED(v);}
  else if (FD_SCHEMAPP(v)) {
    v=fd_deep_copy(v);
    FD_SCHEMAP_MARK_MODIFIED(v);}
  else if (FD_HASHTABLEP(v)) {
    v=fd_deep_copy(v);
    FD_HASHTABLE_MARK_MODIFIED(v);}
  else v=fd_incref(v);
  retval=fd_set_oid_value(o,v);
  fd_decref(v);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_VOID;
}

static fdtype lockoid(fdtype o,fdtype soft)
{
  int retval=fd_lock_oid(o);
  if (retval<0)
    if (FD_TRUEP(soft)) {
      fd_poperr(NULL,NULL,NULL,NULL);
      return FD_FALSE;}
    else return FD_ERROR_VALUE;
  else return FD_INT(retval);
}

static fdtype oidlockedp(fdtype arg)
{
  fd_pool p=fd_oid2pool(arg);
  if (fd_hashtable_probe_novoid(&(p->locks),arg))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype lockoids(fdtype oids)
{
  int retval=fd_lock_oids(oids);
  if (retval<0)
    return FD_ERROR_VALUE;
  else return FD_INT(retval);
}

static fdtype lockedoids(fdtype pool)
{
  fd_pool p=fd_lisp2pool(pool);
  return fd_hashtable_keys(&(p->locks));
}

static fdtype unlockoids(fdtype oids,fdtype commitp)
{
  int force_commit=(!((FD_VOIDP(commitp)) || (FD_FALSEP(commitp))));
  if (FD_VOIDP(oids)) {
    fd_unlock_pools(force_commit);
    return FD_FALSE;}
  else if ((FD_PRIM_TYPEP(oids,fd_pool_type))||(FD_STRINGP(oids))) {
    fd_pool p=((FD_PRIM_TYPEP(oids,fd_pool_type)) ? (fd_lisp2pool(oids)) :
               (fd_name2pool(FD_STRDATA(oids))));
    if (p) {
      int retval=fd_pool_unlock_all(p,force_commit);
      if (retval<0) return FD_ERROR_VALUE;
      else return FD_INT(retval);}
    else return fd_type_error("pool or OID","unlockoids",oids);}
  else {
    int retval=fd_unlock_oids(oids,force_commit);
    if (retval<0)
      return FD_ERROR_VALUE;
    else return FD_INT(retval);}
}

static fdtype make_compound_index(int n,fdtype *args)
{
  fd_index *sources=u8_alloc_n(8,fd_index);
  int n_sources=0, max_sources=8;
  int i=0; while (i<n) {
    FD_DO_CHOICES(source,args[i]) {
      fd_index ix=NULL;
      if (FD_STRINGP(source)) ix=fd_open_index(fd_strdata(source));
      else if (FD_INDEXP(source)) ix=fd_indexptr(source);
      else if (FD_PRIM_TYPEP(source,fd_raw_index_type)) ix=fd_indexptr(source);
      else if (FD_SYMBOLP(source)) {
        fdtype val=fd_config_get(FD_SYMBOL_NAME(source));
        if (FD_STRINGP(val)) ix=fd_open_index(fd_strdata(val));
        else if (FD_INDEXP(val)) ix=fd_indexptr(source);
        else if (FD_PRIM_TYPEP(val,fd_raw_index_type)) ix=fd_indexptr(val);}
      else {}
      if (ix) {
        if (n_sources>=max_sources) {
          sources=u8_realloc_n(sources,max_sources+8,fd_index);
          max_sources=max_sources+8;}
        if (ix->serialno<0) {fdtype lix=(fdtype)ix; fd_incref(lix);}
        sources[n_sources++]=ix;}
      else {
        u8_free(sources);
        return fd_type_error("index","make_compound_index",source);}}
    i++;}
  return fd_index2lisp(fd_make_compound_index(n_sources,sources));
}

static fdtype add_to_compound_index(fdtype lcx,fdtype aix)
{
  if (FD_INDEXP(lcx)) {
    fd_index ix=fd_indexptr(lcx);
    if (FD_EXPECT_FALSE(ix==NULL))
      return fd_type_error("index","add_to_compound_index",lcx);
    else if (fd_add_to_compound_index((struct FD_COMPOUND_INDEX *)ix,
                                      fd_indexptr(aix))<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;}
  else return fd_type_error(_("index"),"add_to_compound_index",lcx);
}

static fdtype make_mempool(fdtype label,fdtype base,fdtype cap,
                           fdtype load,fdtype noswap)
{
  fd_pool p=fd_make_mempool
    (FD_STRDATA(label),FD_OID_ADDR(base),FD_FIX2INT(cap),FD_FIX2INT(load),
     (!(FD_FALSEP(noswap))));
  if (p==NULL) return FD_ERROR_VALUE;
  else return fd_pool2lisp(p);
}

static fdtype clean_mempool(fdtype pool_arg)
{
  int retval=fd_clean_mempool(fd_lisp2pool(pool_arg));
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_INT(retval);
}

static fdtype reset_mempool(fdtype pool_arg)
{
  int retval=fd_reset_mempool(fd_lisp2pool(pool_arg));
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_INT(retval);
}

static fdtype make_extpool(fdtype label,fdtype base,fdtype cap,
                           fdtype fetchfn,fdtype savefn,
                           fdtype lockfn,fdtype allocfn,
                           fdtype state,fdtype cache)
{
  fd_pool p=fd_make_extpool
    (FD_STRDATA(label),FD_OID_ADDR(base),FD_FIX2INT(cap),
     fetchfn,savefn,lockfn,allocfn,state);
  if (FD_FALSEP(cache)) fd_pool_setcache(p,0);
  return fd_pool2lisp(p);
}

static fdtype extpool_setcache(fdtype pool,fdtype oid,fdtype value)
{
  fd_pool p=fd_lisp2pool(pool);
  if (fd_extpool_cache_value(p,oid,value)<0)
    return FD_ERROR_VALUE;
  else return FD_VOID;
}

static fdtype extpool_fetchfn(fdtype pool)
{
  fd_pool p=fd_lisp2pool(pool);
  if (p->handler==&fd_extpool_handler) {
    struct FD_EXTPOOL *ep=(struct FD_EXTPOOL *)p;
    return fd_incref(ep->fetchfn);}
  else return fd_type_error("extpool","extpool_fetchfn",pool);
}

static fdtype extpool_savefn(fdtype pool)
{
  fd_pool p=fd_lisp2pool(pool);
  if (p->handler==&fd_extpool_handler) {
    struct FD_EXTPOOL *ep=(struct FD_EXTPOOL *)p;
    return fd_incref(ep->savefn);}
  else return fd_type_error("extpool","extpool_savefn",pool);
}

static fdtype extpool_lockfn(fdtype pool)
{
  fd_pool p=fd_lisp2pool(pool);
  if (p->handler==&fd_extpool_handler) {
    struct FD_EXTPOOL *ep=(struct FD_EXTPOOL *)p;
    return fd_incref(ep->lockfn);}
  else return fd_type_error("extpool","extpool_lockfn",pool);
}

static fdtype extpool_state(fdtype pool)
{
  fd_pool p=fd_lisp2pool(pool);
  if (p->handler==&fd_extpool_handler) {
    struct FD_EXTPOOL *ep=(struct FD_EXTPOOL *)p;
    return fd_incref(ep->state);}
  else return fd_type_error("extpool","extpool_state",pool);
}

/* External indices */

static fdtype make_extindex(fdtype label,fdtype fetchfn,fdtype commitfn,
                            fdtype state,fdtype usecache)
{
  fd_index ix=fd_make_extindex
    (FD_STRDATA(label),
     ((FD_FALSEP(fetchfn))?(FD_VOID):(fetchfn)),
     ((FD_FALSEP(commitfn))?(FD_VOID):(commitfn)),
     ((FD_FALSEP(state))?(FD_VOID):(state)),
     1);
  if (FD_FALSEP(usecache)) fd_index_setcache(ix,0);
  return fd_index2lisp(ix);
}

static fdtype cons_extindex(fdtype label,fdtype fetchfn,fdtype commitfn,
                            fdtype state,fdtype usecache)
{
  fd_index ix=fd_make_extindex
    (FD_STRDATA(label),
     ((FD_FALSEP(fetchfn))?(FD_VOID):(fetchfn)),
     ((FD_FALSEP(commitfn))?(FD_VOID):(commitfn)),
     ((FD_FALSEP(state))?(FD_VOID):(state)),
     0);
  if (FD_FALSEP(usecache)) fd_index_setcache(ix,0);
  if (ix->serialno>=0) return fd_index2lisp(ix);
  else return (fdtype)ix;
}

static fdtype extindex_cacheadd(fdtype index,fdtype key,fdtype values)
{
  FDTC *fdtc=fd_threadcache;
  fd_index ix=fd_indexptr(index);
  if (ix->handler==&fd_extindex_handler)
    if (fd_hashtable_add(&(ix->cache),key,values)<0)
      return FD_ERROR_VALUE;
    else {}
  else return fd_type_error("extindex","extindex_cacheadd",index);
  if (fdtc) {
    struct FD_PAIR tempkey;
    struct FD_HASHTABLE *h=&(fdtc->indices);
    FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
    tempkey.car=fd_index2lisp(ix); tempkey.cdr=key;
    if (fd_hashtable_probe(h,(fdtype)&tempkey)) {
      fd_hashtable_store(h,(fdtype)&tempkey,FD_VOID);}}
  return FD_VOID;
}

static fdtype extindex_decache(fdtype index,fdtype key)
{
  FDTC *fdtc=fd_threadcache;
  fd_index ix=fd_indexptr(index);
  fdtype lix=fd_index2lisp(ix);
  if (ix->handler==&fd_extindex_handler)
    if (FD_VOIDP(key))
      if (fd_reset_hashtable(&(ix->cache),ix->cache.n_slots,1)<0)
        return FD_ERROR_VALUE;
      else {}
    else if (fd_hashtable_store(&(ix->cache),key,FD_VOID)<0)
      return FD_ERROR_VALUE;
    else {}
  else return fd_type_error("extindex","extindex_decache",index);
  if ((fdtc)&&(!(FD_VOIDP(key)))) {
    struct FD_PAIR tempkey;
    struct FD_HASHTABLE *h=&(fdtc->indices);
    FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
    tempkey.car=fd_index2lisp(ix); tempkey.cdr=key;
    if (fd_hashtable_probe(h,(fdtype)&tempkey)) {
      fd_hashtable_store(h,(fdtype)&tempkey,FD_VOID);}}
  else if (fdtc) {
    struct FD_HASHTABLE *h=&(fdtc->indices);
    fdtype keys=fd_hashtable_keys(h), drop=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(key,keys) {
      if ((FD_PAIRP(key))&&(FD_CAR(key)==lix)) {
        fd_incref(key); FD_ADD_TO_CHOICE(drop,key);}}
    if (!(FD_EMPTY_CHOICEP(drop))) {
      FD_DO_CHOICES(d,drop) fd_hashtable_drop(h,d,FD_VOID);}
    fd_decref(drop); fd_decref(keys);}
  else {}
return FD_VOID;
}

static fdtype extindex_fetchfn(fdtype index)
{
  fd_index ix=fd_indexptr(index);
  if (ix->handler==&fd_extindex_handler) {
    struct FD_EXTINDEX *eix=(struct FD_EXTINDEX *)ix;
    return fd_incref(eix->fetchfn);}
  else return fd_type_error("extindex","extindex_fetchfn",index);
}

static fdtype extindex_commitfn(fdtype index)
{
  fd_index ix=fd_indexptr(index);
  if (ix->handler==&fd_extindex_handler) {
    struct FD_EXTINDEX *eix=(struct FD_EXTINDEX *)ix;
    return fd_incref(eix->commitfn);}
  else return fd_type_error("extindex","extindex_commitfn",index);
}

static fdtype extindex_state(fdtype index)
{
  fd_index ix=fd_indexptr(index);
  if (ix->handler==&fd_extindex_handler) {
    struct FD_EXTINDEX *eix=(struct FD_EXTINDEX *)ix;
    return fd_incref(eix->state);}
  else return fd_type_error("extindex","extindex_state",index);
}

/* Adding adjuncts */

static fdtype adjunct_symbol;

static fdtype use_adjunct(fdtype adjunct,fdtype slotid,fdtype pool_arg)
{
  if (FD_STRINGP(adjunct)) {
    fd_index ix=fd_open_index(FD_STRDATA(adjunct));
    if (ix) adjunct=fd_index2lisp(ix);
    else return fd_type_error("adjunct spec","use_adjunct",adjunct);}
  if ((FD_VOIDP(slotid)) && (FD_TABLEP(adjunct)))
    slotid=fd_get(adjunct,adjunct_symbol,FD_VOID);
  if ((FD_SYMBOLP(slotid)) || (FD_OIDP(slotid)))
    if (FD_VOIDP(pool_arg))
      if (fd_set_adjunct(NULL,slotid,adjunct)<0) return FD_ERROR_VALUE;
      else return FD_VOID;
    else {
      fd_pool p=fd_lisp2pool(pool_arg);
      if (p==NULL) return FD_ERROR_VALUE;
      else if (fd_set_adjunct(p,slotid,adjunct)<0) return FD_ERROR_VALUE;
      else return FD_VOID;}
  else return fd_type_error(_("slotid"),"use_adjunct",slotid);
}

/* Definiing pool ops */

static fdtype set_pool_op(fdtype pool_arg,fdtype op,fdtype slotid,fdtype handler)
{
  fd_pool p=fd_lisp2pool(pool_arg);
  if (p==NULL) return FD_ERROR_VALUE;
  else if (fd_pool_setop(p,op,slotid,handler)<0) return FD_ERROR_VALUE;
  else return FD_VOID;
}

/* DB control functions */

static fdtype swapout_lexpr(int n,fdtype *args)
{
  if (n == 0) {
    fd_swapout_indices();
    fd_swapout_pools();
    return FD_VOID;}
  else if (n == 1) {
    fdtype arg=args[0];
    if (FD_CHOICEP(arg)) {
      fdtype oids=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(e,arg) {
        if (FD_OIDP(e)) {FD_ADD_TO_CHOICE(oids,e);}
        else if (FD_POOLP(e))
          fd_pool_swapout(fd_lisp2pool(e));
        else if (FD_INDEXP(e))
          fd_index_swapout(fd_indexptr(e));
        else if (FD_PRIM_TYPEP(e,fd_raw_index_type))
          fd_index_swapout(fd_indexptr(e));
        else if (FD_PTR_TYPEP(arg,fd_raw_pool_type))
          fd_pool_swapout((fd_pool)arg);
        else if (FD_STRINGP(e)) {
          fd_pool p=fd_name2pool(FD_STRDATA(e));
          if (!(p)) {
            fd_decref(oids);
            return fd_type_error(_("pool, index, or OIDs"),
                                 "swapout_lexpr",e);}
          else fd_pool_swapout(p);}
        else {
          fd_decref(oids);
          return fd_type_error(_("pool, index, or OIDs"),
                               "swapout_lexpr",e);}}
      fd_swapout_oids(oids);
      fd_decref(oids);
      return FD_VOID;}
    else if (FD_OIDP(arg)) fd_swapout_oid(arg);
    else if (FD_PTR_TYPEP(arg,fd_index_type))
      fd_index_swapout(fd_indexptr(arg));
    else if (FD_PTR_TYPEP(arg,fd_pool_type))
      fd_pool_swapout(fd_lisp2pool(arg));
    else if (FD_PTR_TYPEP(arg,fd_raw_index_type))
      fd_index_swapout(fd_indexptr(arg));
    else if (FD_PTR_TYPEP(arg,fd_raw_pool_type))
      fd_pool_swapout((fd_pool)arg);
    else return fd_type_error(_("pool, index, or OIDs"),"swapout_lexpr",arg);
    return FD_VOID;}
  else return fd_err(fd_TooManyArgs,"swapout",NULL,FD_VOID);
}

static fdtype commit_lexpr(int n,fdtype *args)
{
  if (n == 0) {
    if (fd_commit_indices()<0)
      return FD_ERROR_VALUE;
    if (fd_commit_pools()<0)
      return FD_ERROR_VALUE;
    return FD_VOID;}
  else if (n == 1) {
    fdtype arg=args[0]; int retval=0;
    if (FD_PTR_TYPEP(arg,fd_index_type))
      retval=fd_index_commit(fd_indexptr(arg));
    else if (FD_PTR_TYPEP(arg,fd_pool_type))
      retval=fd_pool_commit_all(fd_lisp2pool(arg),1);
    else if (FD_PTR_TYPEP(arg,fd_raw_index_type))
      retval=fd_index_commit(fd_indexptr(arg));
    else if (FD_PTR_TYPEP(arg,fd_raw_pool_type))
      retval=fd_pool_commit_all((fd_pool)arg,1);
    else if (FD_OIDP(arg))
      retval=fd_commit_oids(arg,1);
    else return fd_type_error(_("pool or index"),"commit_lexpr",arg);
    if (retval<0) return FD_ERROR_VALUE;
    else return FD_VOID;}
  else return fd_err(fd_TooManyArgs,"commit",NULL,FD_VOID);
}

static fdtype clear_slotcache(fdtype arg)
{
  if (FD_VOIDP(arg)) fd_clear_slotcaches();
  else fd_clear_slotcache(arg);
  return FD_VOID;
}

static fdtype clearcaches()
{
  fd_clear_callcache(FD_VOID);
  fd_clear_slotcaches();
  fd_swapout_indices();
  fd_swapout_pools();
  return FD_VOID;
}

static fdtype swapcheck_prim()
{
  if (fd_swapcheck()) return FD_TRUE;
  else return FD_FALSE;
}

static fd_pool arg2pool(fdtype arg)
{
  if (FD_POOLP(arg)) return fd_lisp2pool(arg);
  else if (FD_PRIM_TYPEP(arg,fd_raw_pool_type))
    return (fd_pool)arg;
  else if (FD_STRINGP(arg)) {
    fd_pool p=fd_name2pool(FD_STRDATA(arg));
    if (p) return p;
    else return fd_use_pool(FD_STRDATA(arg));}
  else if (FD_SYMBOLP(arg)) {
    fdtype v=fd_config_get(FD_SYMBOL_NAME(arg));
    if (FD_STRINGP(v)) return fd_use_pool(FD_STRDATA(v));
    else return NULL;}
  else return NULL;
}

static fdtype pool_load(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_load",arg);
  else {
    int load=fd_pool_load(p);
    if (load>=0) return FD_INT(load);
    else return FD_ERROR_VALUE;}
}

static fdtype pool_capacity(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_capacity",arg);
  else return FD_INT(p->capacity);
}

static fdtype pool_base(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_base",arg);
  else return fd_make_oid(p->base);
}

static fdtype pool_elts(fdtype arg,fdtype start,fdtype count)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_elts",arg);
  else {
    int i=0, lim=fd_pool_load(p);
    fdtype result=FD_EMPTY_CHOICE;
    FD_OID base=p->base;
    if (lim<0) return FD_ERROR_VALUE;
    if (FD_VOIDP(start)) {}
    else if (FD_FIXNUMP(start))
      if (FD_FIX2INT(start)<0)
        return fd_type_error(_("pool offset"),"pool_elts",start);
      else i=FD_FIX2INT(start);
    else if (FD_OIDP(start)) i=FD_OID_DIFFERENCE(FD_OID_ADDR(start),base);
    else return fd_type_error(_("pool offset"),"pool_elts",start);
    if (FD_VOIDP(count)) {}
    else if (FD_FIXNUMP(count)) {
      int count_arg=FD_FIX2INT(count);
      if (count_arg<0)
        return fd_type_error(_("pool offset"),"pool_elts",count);
      else if (i+count_arg<lim) lim=i+count_arg;}
    else if (FD_OIDP(start)) {
      int lim_arg=FD_OID_DIFFERENCE(FD_OID_ADDR(count),base);
      if (lim_arg<lim) lim=lim_arg;}
    else return fd_type_error(_("pool offset"),"pool_elts",count);
    if ((i>0) || ((lim-i)<(FD_OID_BUCKET_SIZE))) {
      while (i<lim) {
        fdtype each=fd_make_oid(FD_OID_PLUS(base,i));
        FD_ADD_TO_CHOICE(result,each); i++;}}
    else {
      int k=0, n_buckets=((lim-i)/(FD_OID_BUCKET_SIZE))+1;
      while (k<FD_OID_BUCKET_SIZE) {
        int j=0; while (j<n_buckets) {
          unsigned int off=(j*FD_OID_BUCKET_SIZE)+k;
          if (off<lim) {
            fdtype each=fd_make_oid(FD_OID_PLUS(base,off));
            FD_ADD_TO_CHOICE(result,each);}
          j++;}
        k++;}
      return result;}
    return result;}
}

static fdtype pool_label(fdtype arg,fdtype use_source)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_label",arg);
  else if (p->label)
    return fdtype_string(p->label);
  else if (FD_FALSEP(use_source)) return FD_FALSE;
  else if (p->source)
    return fdtype_string(p->source);
  else return FD_FALSE;
}

static fdtype pool_prefix(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_label",arg);
  else if (p->prefix)
    return fdtype_string(p->prefix);
  else return FD_FALSE;
}

static fdtype set_pool_prefix(fdtype arg,fdtype prefix_arg)
{
  fd_pool p=arg2pool(arg);
  u8_string prefix=FD_STRDATA(prefix_arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"set_pool_prefix",arg);
  else if ((p->prefix)&&(strcmp(p->prefix,prefix)==0))
    return FD_FALSE;
  else if (p->prefix) {
    u8_string copy=u8_strdup(prefix);
    u8_string cur=p->prefix;
    p->prefix=copy;
    return fd_init_string(NULL,-1,cur);}
  else {
    u8_string copy=u8_strdup(prefix);
    p->prefix=copy;
    return FD_TRUE;}
}

static fdtype pool_source(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_label",arg);
  else if (p->source)
    return fdtype_string(p->source);
  else if (p->cid)
    return fdtype_string(p->source);
  else return FD_FALSE;
}

static fdtype pool_close_prim(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_close",arg);
  else {
    fd_pool_close(p);
    return FD_VOID;}
}

static fdtype oid_range(fdtype start,fdtype end)
{
  int i=0, lim=fd_getint(end);
  fdtype result=FD_EMPTY_CHOICE;
  FD_OID base=FD_OID_ADDR(start);
  if (lim<0) return FD_ERROR_VALUE;
  else while (i<lim) {
    fdtype each=fd_make_oid(FD_OID_PLUS(base,i));
    FD_ADD_TO_CHOICE(result,each); i++;}
  return result;
}

static fdtype oid_vector(fdtype start,fdtype end)
{
  int i=0, lim=fd_getint(end);
  if (lim<0) return FD_ERROR_VALUE;
  else {
    fdtype result=fd_init_vector(NULL,lim,NULL);
    fdtype *data=FD_VECTOR_DATA(result);
    FD_OID base=FD_OID_ADDR(start);
    while (i<lim) {
      fdtype each=fd_make_oid(FD_OID_PLUS(base,i));
      data[i++]=each;}
    return result;}
}

static fdtype random_oid(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"random_oid",arg);
  else {
    int load=fd_pool_load(p);
    if (load) {
      FD_OID base=p->base; int i=u8_random(load);
      return fd_make_oid(FD_OID_PLUS(base,i));}
    return FD_EMPTY_CHOICE;}
}

static fdtype pool_vec(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_vec",arg);
  else {
    int i=0, lim=fd_pool_load(p);
    fdtype result=fd_init_vector(NULL,lim,NULL);
    FD_OID base=p->base;
    if (lim<0) return FD_ERROR_VALUE;
    else while (i<lim) {
      fdtype each=fd_make_oid(FD_OID_PLUS(base,i));
      FD_VECTOR_SET(result,i,each); i++;}
    return result;}
}

static fdtype cachecount(fdtype arg)
{
  fd_pool p=NULL; fd_index ix=NULL;
  if (FD_VOIDP(arg)) {
    int count=fd_object_cache_load()+fd_index_cache_load();
    return FD_INT(count);}
  else if (FD_EQ(arg,pools_symbol)) {
    int count=fd_object_cache_load();
    return FD_INT(count);}
  else if (FD_EQ(arg,indices_symbol)) {
    int count=fd_index_cache_load();
    return FD_INT(count);}
  else if ((p=(fd_lisp2pool(arg)))) {
    int count=p->cache.n_keys;
    return FD_INT(count);}
  else if ((ix=(fd_indexptr(arg)))) {
    int count=ix->cache.n_keys;
    return FD_INT(count);}
  else return fd_type_error(_("pool or index"),"cachecount",arg);
}

/* OID functions */

static fdtype oidhi(fdtype x)
{
  FD_OID addr=FD_OID_ADDR(x);
  return FD_INT(FD_OID_HI(addr));
}

static fdtype oidlo(fdtype x)
{
  FD_OID addr=FD_OID_ADDR(x);
  return FD_INT(FD_OID_LO(addr));
}

static fdtype oidp(fdtype x)
{
  if (FD_OIDP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype oidpool(fdtype x)
{
  fd_pool p=fd_oid2pool(x);
  if (p==NULL) return FD_EMPTY_CHOICE;
  else return fd_pool2lisp(p);
}

static fdtype inpoolp(fdtype x,fdtype pool_arg)
{
  fd_pool p=fd_lisp2pool(pool_arg);
  fd_pool op=fd_oid2pool(x);
  if (p == op) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype validoidp(fdtype x,fdtype pool_arg)
{
  if (FD_VOIDP(pool_arg)) {
    fd_pool p=fd_oid2pool(x);
    if (p==NULL) return FD_FALSE;
    else {
      FD_OID base=p->base, addr=FD_OID_ADDR(x);
      unsigned int offset=FD_OID_DIFFERENCE(addr,base);
      unsigned int load=fd_pool_load(p);
      if (offset<load) return FD_TRUE;
      else return FD_FALSE;}}
  else {
    fd_pool p=fd_lisp2pool(pool_arg);
    fd_pool op=fd_oid2pool(x);
    if (p == op) {
      FD_OID base=p->base, addr=FD_OID_ADDR(x);
      unsigned int offset=FD_OID_DIFFERENCE(addr,base);
      unsigned int load=fd_pool_load(p);
      if (offset<load) return FD_TRUE;
      else return FD_FALSE;}
    else return FD_FALSE;}
}

/* Prefetching functions */

static fdtype prefetch_oids(fdtype oids)
{
  if (fd_prefetch_oids(oids)>=0) return FD_TRUE;
  else return FD_ERROR_VALUE;
}

static fdtype fetchoids_prim(fdtype oids)
{
  fd_prefetch_oids(oids);
  return fd_incref(oids);
}

static fdtype prefetch_keys(fdtype arg1,fdtype arg2)
{
  if (FD_VOIDP(arg2)) {
    if (fd_bg_prefetch(arg1)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;}
  else {
    FD_DO_CHOICES(arg,arg1) {
      if ((FD_INDEXP(arg))||(FD_PRIM_TYPEP(arg,fd_raw_index_type))) {
        fd_index ix=fd_indexptr(arg);
        if (fd_index_prefetch(ix,arg2)<0) {
          FD_STOP_DO_CHOICES;
          return FD_ERROR_VALUE;}}
      else return fd_type_error(_("index"),"prefetch_keys",arg);}
    return FD_VOID;}
}

/* Getting cached OIDs */

static fdtype cached_oids(fdtype pool)
{
  if ((FD_VOIDP(pool)) || (FD_TRUEP(pool)))
    return fd_cached_oids(NULL);
  else {
    fd_pool p=fd_lisp2pool(pool);
    if (p) return fd_cached_oids(p);
    else return fd_type_error(_("pool"),"cached_oids",pool);}
}

static fdtype cached_keys(fdtype index)
{
  if ((FD_VOIDP(index)) || (FD_TRUEP(index)))
    return fd_cached_keys(NULL);
  else {
    fd_index ix=fd_indexptr(index);
    if (ix) return fd_cached_keys(ix);
    else return fd_type_error(_("index"),"cached_keys",index);}
}

/* Frame get functions */

FD_EXPORT
fdtype fd_fget(fdtype frames,fdtype slotids)
{
  if (!(FD_CHOICEP(frames)))
    if (!(FD_CHOICEP(slotids)))
      if (FD_OIDP(frames))
        return fd_frame_get(frames,slotids);
      else return fd_get(frames,slotids,FD_EMPTY_CHOICE);
    else if (FD_OIDP(frames)) {
      fdtype result=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(slotid,slotids) {
        fdtype value=fd_frame_get(frames,slotid);
        if (FD_ABORTP(value)) {
          fd_decref(result);
          return value;}
        FD_ADD_TO_CHOICE(result,value);}
      return result;}
    else {
      fdtype result=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(slotid,slotids) {
        fdtype value=fd_get(frames,slotid,FD_EMPTY_CHOICE);
        if (FD_ABORTP(value)) {
          fd_decref(result);
          return value;}
        FD_ADD_TO_CHOICE(result,value);}
      return result;}
  else {
    int all_adjuncts=1;
    if (FD_CHOICEP(slotids)) {
      FD_DO_CHOICES(slotid,slotids) {
        int adjunctp=0;
        FD_DO_CHOICES(adjslotid,fd_adjunct_slotids) {
          if (FD_EQ(slotid,adjslotid)) {adjunctp=1; break;}}
        if (adjunctp==0) {all_adjuncts=0; break;}}}
    else {
      int adjunctp=0;
      FD_DO_CHOICES(adjslotid,fd_adjunct_slotids) {
        if (FD_EQ(slotids,adjslotid)) {adjunctp=1; break;}}
      if (adjunctp==0) all_adjuncts=0;}
    if ((fd_prefetch) && (fd_ipeval_status()==0) &&
        (FD_CHOICEP(frames)) && (all_adjuncts==0))
      fd_prefetch_oids(frames);
    {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(frame,frames)
        if (FD_OIDP(frame)) {
          FD_DO_CHOICES(slotid,slotids) {
            fdtype v=fd_frame_get(frame,slotid);
            if (FD_ABORTP(v)) {
              FD_STOP_DO_CHOICES;
              fd_decref(results);
              return v;}
            else {FD_ADD_TO_CHOICE(results,v);}}}
        else {
          FD_DO_CHOICES(slotid,slotids) {
            fdtype v=fd_get(frame,slotid,FD_EMPTY_CHOICE);
            if (FD_ABORTP(v)) {
              FD_STOP_DO_CHOICES;
              fd_decref(results);
              return v;}
            else {FD_ADD_TO_CHOICE(results,v);}}}
      return fd_simplify_choice(results);}}
}

FD_EXPORT
fdtype fd_ftest(fdtype frames,fdtype slotids,fdtype values)
{
  if (FD_EMPTY_CHOICEP(frames))
    return FD_FALSE;
  else if ((!(FD_CHOICEP(frames))) && (!(FD_OIDP(frames))))
    if (FD_CHOICEP(slotids)) {
      int found=0;
      FD_DO_CHOICES(slotid,slotids)
        if (fd_test(frames,slotid,values)) {
          found=1; FD_STOP_DO_CHOICES; break;}
        else {}
      if (found) return FD_TRUE;
      else return FD_FALSE;}
    else {
      int testval=fd_test(frames,slotids,values);
      if (testval<0) return FD_ERROR_VALUE;
      else if (testval) return FD_TRUE;
      else return FD_FALSE;}
  else {
    FD_DO_CHOICES(frame,frames) {
      FD_DO_CHOICES(slotid,slotids) {
        FD_DO_CHOICES(value,values)
          if (FD_OIDP(frame)) {
            int result=fd_frame_test(frame,slotid,value);
            if (result<0) return FD_ERROR_VALUE;
            else if (result) return FD_TRUE;}
          else {
            int result=fd_test(frame,slotid,value);
            if (result<0) return FD_ERROR_VALUE;
            else if (result) return FD_TRUE;}}}
    return FD_FALSE;}
}

FD_EXPORT
fdtype fd_fassert(fdtype frames,fdtype slotids,fdtype values)
{
  if (FD_EMPTY_CHOICEP(values)) return FD_VOID;
  else {
    FD_DO_CHOICES(frame,frames) {
      FD_DO_CHOICES(slotid,slotids) {
        FD_DO_CHOICES(value,values) {
          if (fd_frame_add(frame,slotid,value)<0)
            return FD_ERROR_VALUE;}}}
    return FD_VOID;}
}
FD_EXPORT
fdtype fd_fretract(fdtype frames,fdtype slotids,fdtype values)
{
  if (FD_EMPTY_CHOICEP(values)) return FD_VOID;
  else {
    FD_DO_CHOICES(frame,frames) {
      FD_DO_CHOICES(slotid,slotids) {
        if (FD_VOIDP(values)) {
          fdtype values=fd_frame_get(frame,slotid);
          FD_DO_CHOICES(value,values) {
            if (fd_frame_drop(frame,slotid,value)<0) {
              fd_decref(values);
              return FD_ERROR_VALUE;}}
          fd_decref(values);}
        else {
          FD_DO_CHOICES(value,values) {
            if (fd_frame_drop(frame,slotid,value)<0)
              return FD_ERROR_VALUE;}}}}
    return FD_VOID;}
}

static fdtype testp(int n,fdtype *args)
{
  fdtype frames=args[0], slotids=args[1], testfns=args[2];
  if ((FD_EMPTY_CHOICEP(frames)) || (FD_EMPTY_CHOICEP(slotids)))
    return FD_FALSE;
  else {
    FD_DO_CHOICES(frame,frames) {
      FD_DO_CHOICES(slotid,slotids) {
        fdtype values=fd_fget(frame,slotid);
        FD_DO_CHOICES(testfn,testfns)
          if (FD_APPLICABLEP(testfn)) {
            fdtype test_result=FD_FALSE;
            args[2]=values;
            test_result=fd_apply(testfn,n-2,args+2);
            args[2]=testfns;
            if (FD_ABORTP(test_result)) {
              fd_decref(values); return test_result;}
            else if (FD_TRUEP(test_result)) {
              fd_decref(values); fd_decref(test_result);
              return FD_TRUE;}
            else {}}
          else if ((FD_SYMBOLP(testfn)) || (FD_OIDP(testfn))) {
            fdtype test_result;
            args[1]=values; args[2]=testfn;
            test_result=testp(n-1,args+1);
            args[1]=slotids; args[2]=testfns;
            if (FD_ABORTP(test_result)) {
              fd_decref(values); return test_result;}
            else if (FD_TRUEP(test_result)) {
              fd_decref(values); return test_result;}
            else fd_decref(test_result);}
          else if (FD_TABLEP(testfn)) {
            FD_DO_CHOICES(value,values) {
              fdtype mapsto=fd_get(testfn,value,FD_VOID);
              if (FD_ABORTP(mapsto)) {
                fd_decref(values);
                return mapsto;}
              else if (FD_VOIDP(mapsto)) {}
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

static fdtype getpath_prim(int n,fdtype *args)
{
  fdtype result=fd_getpath(args[0],n-1,args+1,1,0);
  return fd_simplify_choice(result);
}

static fdtype getpathstar_prim(int n,fdtype *args)
{
  fdtype result=fd_getpath(args[0],n-1,args+1,1,0);
  return fd_simplify_choice(result);
}

/* Cache gets */

static fdtype cacheget_handler(fdtype expr,fd_lispenv env)
{
  fdtype table_arg=fd_get_arg(expr,1), key_arg=fd_get_arg(expr,2), default_expr=fd_get_arg(expr,3);
  if (FD_EXPECT_FALSE((FD_VOIDP(table_arg)) || (FD_VOIDP(key_arg)) || (FD_VOIDP(default_expr))))
    return fd_err(fd_SyntaxError,"cacheget_handler",NULL,expr);
  else {
    fdtype table=fd_eval(table_arg,env), key, value;
    if (FD_ABORTP(table)) return table;
    else if (FD_TABLEP(table)) key=fd_eval(key_arg,env);
    else return fd_type_error(_("table"),"cachget_handler",table);
    if (FD_ABORTP(key)) {
      fd_decref(table); return key;}
    else value=fd_get(table,key,FD_VOID);
    if (FD_VOIDP(value)) {
      fdtype dflt=fd_eval(default_expr,env);
      if (FD_ABORTP(dflt)) {
        fd_decref(table); fd_decref(key);
        return dflt;}
      fd_store(table,key,dflt);
      return dflt;}
    else return value;}
}

/* Index operations */

static fdtype indexget(fdtype ixarg,fdtype key)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix==NULL) return FD_ERROR_VALUE;
  else return fd_index_get(ix,key);
}

static fdtype indexadd(fdtype ixarg,fdtype key,fdtype values)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix==NULL) return FD_ERROR_VALUE;
  fd_index_add(ix,key,values);
  return FD_VOID;
}

static fdtype indexset(fdtype ixarg,fdtype key,fdtype values)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix==NULL) return FD_ERROR_VALUE;
  fd_index_store(ix,key,values);
  return FD_VOID;
}

static fdtype indexdecache(fdtype ixarg,fdtype key,fdtype value)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix==NULL) return FD_ERROR_VALUE;
  if (FD_VOIDP(value))
    fd_hashtable_op(&(ix->cache),fd_table_replace,key,FD_VOID);
  else {
    fdtype keypair=fd_conspair(fd_incref(key),fd_incref(value));
    fd_hashtable_op(&(ix->cache),fd_table_replace,keypair,FD_VOID);
    fd_decref(keypair);}
  return FD_VOID;
}

static fdtype bgdecache(fdtype key,fdtype value)
{
  fd_index ix=(fd_index)fd_background;
  if (ix==NULL) return FD_ERROR_VALUE;
  if (FD_VOIDP(value))
    fd_hashtable_op(&(ix->cache),fd_table_replace,key,FD_VOID);
  else {
    fdtype keypair=fd_conspair(fd_incref(key),fd_incref(value));
    fd_hashtable_op(&(ix->cache),fd_table_replace,keypair,FD_VOID);
    fd_decref(keypair);}
  return FD_VOID;
}

static fdtype indexkeys(fdtype ixarg)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix==NULL) fd_type_error("index","indexkeys",ixarg);
  return fd_index_keys(ix);
}

static fdtype indexsizes(fdtype ixarg)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix==NULL) fd_type_error("index","indexsizes",ixarg);
  return fd_index_sizes(ix);
}

static fdtype indexkeysvec(fdtype ixarg)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix==NULL) fd_type_error("index","indexkeysvec",ixarg);
  if (ix->handler->fetchkeys) {
    fdtype *keys; unsigned int n_keys;
    keys=ix->handler->fetchkeys(ix,&n_keys);
    return fd_init_vector(NULL,n_keys,keys);}
  else return fd_index_keys(ix);
}

static fdtype indexsource(fdtype ix_arg)
{
  fd_index ix=fd_indexptr(ix_arg);
  if (ix==NULL) fd_type_error("index","indexsource",ix_arg);
  return fdtype_string(ix->source);
}

static fdtype suggest_hash_size(fdtype size)
{
  unsigned int suggestion=fd_get_hashtable_size(fd_getint(size));
  return FD_INT(suggestion);
}

/* PICK and REJECT */

FD_FASTOP int test_selector_predicate(fdtype candidate,fdtype test,int datalevel);
FD_FASTOP int test_relation_regex(fdtype candidate,fdtype pred,fdtype regex);

FD_FASTOP int test_selector_relation(fdtype f,fdtype pred,fdtype val,int datalevel)
{
  if (FD_CHOICEP(pred)) {
    FD_DO_CHOICES(p,pred) {
      int retval;
      if ((retval=test_selector_relation(f,p,val,datalevel))) {
        {FD_STOP_DO_CHOICES;}
        return retval;}}
    return 0;}
  else if ((!datalevel)&&(FD_PRIM_TYPEP(val,fd_regex_type)))
    return test_relation_regex(f,pred,val);
  else if ((FD_OIDP(f)) && ((FD_SYMBOLP(pred)) || (FD_OIDP(pred))))
    if (datalevel)
      return fd_test(f,pred,val);
    else return fd_frame_test(f,pred,val);
  else if ((FD_TABLEP(f)) && ((FD_SYMBOLP(pred)) || (FD_OIDP(pred))))
    return fd_test(f,pred,val);
  else if (FD_TABLEP(pred))
    return fd_test(pred,f,val);
  else if (FD_APPLICABLEP(pred)) {
    fdtype rail[2], result;
    /* Handle the case where the 'slotid' is a unary function which can
       be used to extract an argument. */
    if ((FD_SPROCP(pred)) ||
        (FD_PRIM_TYPEP(pred,fd_function_type))) {
      fd_function fcn=FD_DTYPE2FCN(pred);
      if (fcn->min_arity==1) {
        fdtype value=fd_apply(pred,1,&f); int retval=-1;
        if (FD_EMPTY_CHOICEP(value)) return 0;
        else if (fd_overlapp(value,val)) retval=1;
        else if (datalevel) retval=0;
        else if ((FD_HASHTABLEP(val))||(FD_HASHSETP(val))||(FD_APPLICABLEP(val))||
                 ((FD_PAIRP(val))&&(FD_APPLICABLEP(FD_CAR(val)))))
          retval=test_selector_predicate(value,val,datalevel);
        else retval=0;
        fd_decref(value);
        return retval;}
      else if (fcn->min_arity==2) {
          rail[0]=f; rail[1]=val; result=fd_apply(pred,2,rail);}
      else result=fd_err(fd_TypeError,"test_selector_relation","invalid relation",pred);}
    if (FD_ABORTP(result))
      return fd_interr(result);
    else if ((FD_FALSEP(result)) || (FD_EMPTY_CHOICEP(result)))
      return 0;
    else {
      fd_decref(result);
      return 1;}}
  else if (FD_VECTORP(pred)) {
    int len=FD_VECTOR_LENGTH(pred), retval;
    fdtype *data=FD_VECTOR_DATA(pred), scan;
    if (len==0) return 0;
    else if (len==1) return test_selector_relation(f,data[0],val,datalevel);
    else scan=fd_getpath(f,len-1,data,datalevel,0);
    retval=test_selector_relation(scan,data[len-1],val,datalevel);
    fd_decref(scan);
    return retval;}
  else return fd_type_error(_("test relation"),"test_selector_relation",pred);
}

FD_FASTOP int test_relation_regex(fdtype candidate,fdtype pred,fdtype regex)
{
  fdtype values=((FD_OIDP(candidate))?
                 (fd_frame_get(candidate,pred)):
                 (fd_get(candidate,pred,FD_EMPTY_CHOICE)));
  if (FD_EMPTY_CHOICEP(values)) return 0;
  else {
    struct FD_REGEX *fdrx=(struct FD_REGEX *)regex;
    FD_DO_CHOICES(value,values) {
      if (FD_STRINGP(value)) {
        regmatch_t results[1];
        u8_string data=FD_STRDATA(value);
        int retval=regexec(&(fdrx->compiled),data,1,results,0);
        if (retval!=REG_NOMATCH) {
          if (retval) {
            u8_byte buf[512];
            regerror(retval,&(fdrx->compiled),buf,512);
            fd_seterr(fd_RegexError,"regex_pick",u8_strdup(buf),FD_VOID);
            FD_STOP_DO_CHOICES;
            fd_decref(values);
            return -1;}
          else if (results[0].rm_so>=0) {
            FD_STOP_DO_CHOICES;
            fd_decref(values);
            return 1;}}}}
    return 0;}
}

FD_FASTOP int test_selector_predicate(fdtype candidate,fdtype test,int datalevel)
{
  if (FD_EMPTY_CHOICEP(candidate)) return 0;
  else if (FD_EMPTY_CHOICEP(test)) return 0;
  else if (FD_CHOICEP(test)) {
    int retval=0;
    FD_DO_CHOICES(t,test) {
      if ((retval=test_selector_predicate(candidate,t,datalevel))) {
        {FD_STOP_DO_CHOICES;}
        return retval;}}
    return retval;}
  else if ((FD_OIDP(candidate)) && ((FD_SYMBOLP(test)) || (FD_OIDP(test))))
    if (datalevel)
      return fd_test(candidate,test,FD_VOID);
    else return fd_frame_test(candidate,test,FD_VOID);
  else if ((!(datalevel))&&(FD_PTR_TYPEP(test,fd_regex_type))) {
    if (FD_STRINGP(candidate)) {
      struct FD_REGEX *fdrx=(struct FD_REGEX *)test;
      regmatch_t results[1];
      u8_string data=FD_STRDATA(candidate);
      int retval=regexec(&(fdrx->compiled),data,1,results,0);
      if (retval==REG_NOMATCH) return 0;
      else if (retval) {
        u8_byte buf[512];
        regerror(retval,&(fdrx->compiled),buf,512);
        fd_seterr(fd_RegexError,"regex_pick",u8_strdup(buf),FD_VOID);
        return -1;}
      else if (results[0].rm_so<0) return 0;
      else return 1;}
    else return 0;}
  else if (FD_PTR_TYPEP(test,fd_hashset_type))
    if (fd_hashset_get((fd_hashset)test,candidate))
      return 1;
    else return 0;
  else if ((FD_TABLEP(candidate)&&
            ((FD_OIDP(test)) || (FD_SYMBOLP(test)))))
    return fd_test(candidate,test,FD_VOID);
  else if (FD_APPLICABLEP(test)) {
    fdtype v=fd_apply(test,1,&candidate);
    if (FD_ABORTP(v)) return fd_interr(v);
    else if ((FD_FALSEP(v)) || (FD_EMPTY_CHOICEP(v)) || (FD_VOIDP(v))) return 0;
    else {fd_decref(v); return 1;}}
  else if ((FD_PAIRP(test))&&(FD_APPLICABLEP(FD_CAR(test)))) {
    fdtype fcn=FD_CAR(test), newval=FD_FALSE;
    fdtype args=FD_CDR(test), argv[7]; int j=1;
    argv[0]=candidate;
    if (FD_PAIRP(args))
      while (FD_PAIRP(args)) {
        argv[j++]=FD_CAR(args); args=FD_CDR(args);}
    else argv[j++]=args;
    if (j>7) newval=fd_err(fd_RangeError,"test_selector_relation","too many elements in test condition",FD_VOID);
    else newval=fd_apply(j,fcn,argv);
    if (FD_ABORTP(newval)) return newval;
    else if ((FD_FALSEP(newval))||(FD_EMPTY_CHOICEP(newval)))
      return 0;
    else {
      fd_decref(newval);
      return 1;}}
  else if (FD_POOLP(test))
    if (FD_OIDP(candidate)) {
      fd_pool p=fd_lisp2pool(test);
      if (fd_oid2pool(candidate)==p) return 1;
      else return 0;}
    else return 0;
  else if (FD_TABLEP(test))
    return fd_test(test,candidate,FD_VOID);
  else if (FD_TABLEP(candidate)) 
    return fd_test(candidate,test,FD_VOID);
  else {
    fdtype ev=fd_type_error(_("test object"),"test_selector_predicate",test);
    return fd_interr(ev);}
}

FD_FASTOP int test_selector_clauses(fdtype candidate,int n,fdtype *args,int datalevel)
{
  if (n==1)
    if (FD_EMPTY_CHOICEP(args[0])) return 0;
    else return test_selector_predicate(candidate,args[0],datalevel);
  else if (n==3) {
    fdtype scan=fd_getpath(candidate,1,args,datalevel,0);
    int retval=test_selector_relation(scan,args[1],args[2],datalevel);
    fd_decref(scan);
    return retval;}
  else if (n%2) {
    fd_seterr(fd_TooManyArgs,"test_selector_clauses","odd number of args/clauses in db pick/reject",FD_VOID);
    return -1;}
  else {
    int i=0; while (i<n) {
      fdtype slotids=args[i], values=args[i+1];
      int retval=test_selector_relation(candidate,slotids,values,datalevel);
      if (retval<0) return retval;
      else if (retval) i=i+2;
      else return 0;}
    return 1;}
}

/* PICK etc */

static fdtype pick_helper(fdtype candidates,int n,fdtype *tests,int datalevel)
{
  int retval;
  if (FD_CHOICEP(candidates)) {
    int n_elts=FD_CHOICE_SIZE(candidates);
    fd_choice read_choice=FD_XCHOICE(candidates);
    fd_choice write_choice=fd_alloc_choice(n_elts);
    const fdtype *read=FD_XCHOICE_DATA(read_choice), *limit=read+n_elts;
    fdtype *write=(fdtype *)FD_XCHOICE_DATA(write_choice);
    int n_results=0, atomic_results=1;
    while (read<limit) {
      fdtype candidate=*read++;
      int retval=test_selector_clauses(candidate,n,tests,datalevel);
      if (retval<0) {
        const fdtype *scan=FD_XCHOICE_DATA(write_choice), *limit=scan+n_results;
        while (scan<limit) {fdtype v=*scan++; fd_decref(v);}
        u8_free(write_choice);
        return FD_ERROR_VALUE;}
      else if (retval) {
        *write++=candidate; n_results++;
        if (FD_CONSP(candidate)) {
          atomic_results=0;
          fd_incref(candidate);}}}
    if (n_results==0) {
      u8_free(write_choice);
      return FD_EMPTY_CHOICE;}
    else if (n_results==1) {
      fdtype result=FD_XCHOICE_DATA(write_choice)[0];
      u8_free(write_choice);
      /* This was incref'd during the loop. */
      return result;}
    else if (n_results*2<n_elts)
      return fd_init_choice
        (fd_realloc_choice(write_choice,n_results),n_results,NULL,
         ((atomic_results)?(FD_CHOICE_ISATOMIC):(FD_CHOICE_ISCONSES)));
    else return fd_init_choice
           (write_choice,n_results,NULL,
            ((atomic_results)?(FD_CHOICE_ISATOMIC):(FD_CHOICE_ISCONSES)));}
  else if (FD_EMPTY_CHOICEP(candidates))
    return FD_EMPTY_CHOICE;
  else if (n==1)
    if ((retval=(test_selector_predicate(candidates,tests[0],datalevel))))
      if (retval<0) return FD_ERROR_VALUE;
      else return fd_incref(candidates);
    else return FD_EMPTY_CHOICE;
  else if ((retval=(test_selector_clauses(candidates,n,tests,datalevel))))
    if (retval<0) return FD_ERROR_VALUE;
    else return fd_incref(candidates);
  else return FD_EMPTY_CHOICE;
}

static fdtype hashset_filter(fdtype candidates,fd_hashset hs,int pick)
{
  if (hs->n_keys==0) {
    if (pick) return FD_EMPTY_CHOICE;
    else return fd_incref(candidates);}
  u8_lock_mutex(&(hs->lock)); {
    fdtype simple=fd_make_simple_choice(candidates);
    int n=FD_CHOICE_SIZE(simple), isatomic=1;
    fdtype *slots=hs->slots; int n_slots=hs->n_slots;
    fdtype *keep=u8_alloc_n(n,fdtype), *write=keep;
    FD_DO_CHOICES(c,candidates) {
      int hash=fd_hash_lisp(c), probe=hash%n_slots, n_probes=0, found=0;
      while (n_probes<512) {
        fdtype pv=slots[probe];
        if (FD_NULLP(pv)) break;
        else if (FDTYPE_EQUAL(c,pv)) {found=1; break;}
        else {
          probe++; n_probes++;
          if (probe>=n_slots) probe=0;}}
      if (((found)&&(pick))||((!(found))&&((!pick)))) {
        if ((isatomic)&&(FD_CONSP(c))) isatomic=0;
        *write++=c; fd_incref(c);}}
    u8_unlock_mutex(&(hs->lock));
    fd_decref(simple);
    if (write==keep) {
      u8_free(keep); return FD_EMPTY_CHOICE;}
    else if ((write-keep)==1) {
      fdtype v=keep[0]; u8_free(keep);
      return v;}
    else return fd_init_choice
           (NULL,write-keep,keep,
            ((isatomic)?(FD_CHOICE_ISATOMIC):(0))|
            (FD_CHOICE_FREEDATA));}
}
#define hashset_pick(c,h) (hashset_filter(c,(fd_hashset)h,1))
#define hashset_reject(c,h) (hashset_filter(c,(fd_hashset)h,0))

static fdtype hashtable_filter(fdtype candidates,fd_hashtable ht,int pick)
{
  if (ht->n_keys==0) {
    if (pick) return FD_EMPTY_CHOICE;
    else return fd_hashtable_keys(ht);}
  else {
    fdtype simple=fd_make_simple_choice(candidates);
    int n=FD_CHOICE_SIZE(simple), unlock=0, isatomic=1;
    fdtype *keep=u8_alloc_n(n,fdtype), *write=keep;
    if (ht->uselock) {fd_read_lock_struct(ht); unlock=1;}
    {struct FD_HASHENTRY **slots=ht->slots; int n_slots=ht->n_slots;
      FD_DO_CHOICES(c,candidates) {
        struct FD_KEYVAL *result=fd_hashvec_get(c,slots,n_slots);
        fdtype rv=((result)?(result->value):(FD_VOID));
        if ((FD_VOIDP(rv))||(FD_EMPTY_CHOICEP(rv))) result=NULL;
        if (((result)&&(pick))||((result==NULL)&&(!(pick)))) {
          if ((isatomic)&&(FD_CONSP(c))) isatomic=0;
          *write++=c; fd_incref(c);}}
      if (unlock) fd_rw_unlock(&(ht->rwlock));
      fd_decref(simple);
      if (write==keep) {
        u8_free(keep); return FD_EMPTY_CHOICE;}
      else if ((write-keep)==1) {
        fdtype v=keep[0]; u8_free(keep);
        return v;}
      else return fd_init_choice
             (NULL,write-keep,keep,
              ((isatomic)?(FD_CHOICE_ISATOMIC):(0))|
              (FD_CHOICE_FREEDATA));}}
}
#define hashtable_pick(c,h) (hashtable_filter(c,(fd_hashtable)h,1))
#define hashtable_reject(c,h) (hashtable_filter(c,(fd_hashtable)h,0))

static fdtype pick_lexpr(int n,fdtype *args)
{
  if ((n==2)&&(FD_HASHTABLEP(args[1])))
    return hashtable_pick(args[0],(fd_hashtable)args[1]);
  else if ((n==2)&&(FD_HASHSETP(args[1])))
    return hashset_pick(args[0],(fd_hashset)args[1]);
  else if ((n<=4)||(n%2))
    return pick_helper(args[0],n-1,args+1,0);
  else return fd_err
         (fd_TooManyArgs,"pick_lexpr",
          "PICK requires two or 2n+1 arguments",FD_VOID);
}

static fdtype prefer_lexpr(int n,fdtype *args)
{
  if ((n==2)&&(FD_HASHTABLEP(args[1]))) {
    fdtype results=hashtable_pick(args[0],(fd_hashtable)args[1]);
    if (FD_EMPTY_CHOICEP(results))
      return fd_incref(args[0]);
    else return results;}
  else if ((n==2)&&(FD_HASHSETP(args[1]))) {
    fdtype results=hashset_pick(args[0],(fd_hashset)args[1]);
    if (FD_EMPTY_CHOICEP(results))
      return fd_incref(args[0]);
    else return results;}
  else if ((n<=4)||(n%2)) {
    fdtype results=pick_helper(args[0],n-1,args+1,0);
    if (FD_EMPTY_CHOICEP(results))
      return fd_incref(args[0]);
    else return results;}
  else return fd_err(fd_TooManyArgs,"prefer_lexpr",
                     "PICK PREFER two or 2n+1 arguments",FD_VOID);
}

static fdtype prim_pick_lexpr(int n,fdtype *args)
{
  if ((n==2)&&(FD_HASHTABLEP(args[1])))
    return hashtable_pick(args[0],(fd_hashtable)args[1]);
  else if ((n==2)&&(FD_HASHSETP(args[1])))
    return hashset_pick(args[0],(fd_hashset)args[1]);
  else if ((n<=4)||(n%2))
    return pick_helper(args[0],n-1,args+1,1);
  else return fd_err(fd_TooManyArgs,"prim_pick_lexpr",
                     "%PICK requires two or 2n+1 arguments",FD_VOID);
}

static fdtype prim_prefer_lexpr(int n,fdtype *args)
{
  if ((n<=4)||(n%2)) {
    fdtype results=pick_helper(args[0],n-1,args+1,1);
    if (FD_EMPTY_CHOICEP(results))
      return fd_incref(args[0]);
    else return results;}
  else return fd_err(fd_TooManyArgs,"prim_prefer_lexpr",
                     "%PICK requires two or 2n+1 arguments",FD_VOID);
}

/* REJECT etc */

static fdtype reject_helper(fdtype candidates,int n,fdtype *tests,int datalevel)
{
  int retval;
  if (FD_CHOICEP(candidates)) {
    int n_elts=FD_CHOICE_SIZE(candidates);
    fd_choice read_choice=FD_XCHOICE(candidates);
    fd_choice write_choice=fd_alloc_choice(n_elts);
    const fdtype *read=FD_XCHOICE_DATA(read_choice), *limit=read+n_elts;
    fdtype *write=(fdtype *)FD_XCHOICE_DATA(write_choice);
    int n_results=0, atomic_results=1;
    while (read<limit) {
      fdtype candidate=*read++;
      int retval=test_selector_clauses(candidate,n,tests,datalevel);
      if (retval<0) {
        const fdtype *scan=FD_XCHOICE_DATA(write_choice), *limit=scan+n_results;
        while (scan<limit) {fdtype v=*scan++; fd_decref(v);}
        u8_free(write_choice);
        return FD_ERROR_VALUE;}
      else if (retval==0) {
        *write++=candidate; n_results++;
        if (FD_CONSP(candidate)) {
          atomic_results=0;
          fd_incref(candidate);}}}
    if (n_results==0) {
      u8_free(write_choice);
      return FD_EMPTY_CHOICE;}
    else if (n_results==1) {
      fdtype result=FD_XCHOICE_DATA(write_choice)[0];
      u8_free(write_choice);
      /* This was incref'd during the loop. */
      return result;}
    else if (n_results*2<n_elts)
      return fd_init_choice
        (fd_realloc_choice(write_choice,n_results),n_results,NULL,
         ((atomic_results)?(FD_CHOICE_ISATOMIC):(FD_CHOICE_ISCONSES)));
    else return fd_init_choice
           (write_choice,n_results,NULL,
            ((atomic_results)?(FD_CHOICE_ISATOMIC):(FD_CHOICE_ISCONSES)));}
  else if (FD_EMPTY_CHOICEP(candidates))
    return FD_EMPTY_CHOICE;
  else if (n==1)
    if ((retval=(test_selector_predicate(candidates,tests[0],datalevel))))
      if (retval<0) return FD_ERROR_VALUE;
      else return FD_EMPTY_CHOICE;
    else return fd_incref(candidates);
  else if ((retval=(test_selector_clauses(candidates,n,tests,datalevel))))
    if (retval<0) return FD_ERROR_VALUE;
    else return FD_EMPTY_CHOICE;
  else return fd_incref(candidates);
}

static fdtype reject_lexpr(int n,fdtype *args)
{
  if ((n==2)&&(FD_HASHTABLEP(args[1])))
    return hashtable_reject(args[0],args[1]);
  else if ((n==2)&&(FD_HASHSETP(args[1])))
    return hashset_reject(args[0],args[1]);
  else if ((n<=4)||(n%2))
    return reject_helper(args[0],n-1,args+1,0);
  else return fd_err(fd_TooManyArgs,"reject_lexpr",
                     "REJECT requires two or 2n+1 arguments",FD_VOID);
}

static fdtype avoid_lexpr(int n,fdtype *args)
{
  if ((n==2)&&(FD_HASHTABLEP(args[1]))) {
    fdtype results=hashtable_reject(args[0],(fd_hashtable)args[1]);
    if (FD_EMPTY_CHOICEP(results))
      return fd_incref(args[0]);
    else return results;}
  else if ((n==2)&&(FD_HASHSETP(args[1]))) {
    fdtype results=hashtable_reject(args[0],(fd_hashset)args[1]);
    if (FD_EMPTY_CHOICEP(results))
      return fd_incref(args[0]);
    else return results;}
  else if ((n<=4)||(n%2)) {
    fdtype values=reject_helper(args[0],n-1,args+1,0);
    if (FD_EMPTY_CHOICEP(values))
      return fd_incref(args[0]);
    else return values;}
  else return fd_err(fd_TooManyArgs,"avoid_lexpr",
                     "AVOID requires two or 2n+1 arguments",FD_VOID);
}

static fdtype prim_reject_lexpr(int n,fdtype *args)
{
  if ((n==2)&&(FD_HASHTABLEP(args[1])))
    return hashtable_reject(args[0],args[1]);
  else if ((n==2)&&(FD_HASHSETP(args[1])))
    return hashset_reject(args[0],args[1]);
  else if ((n<=4)||(n%2))
    return reject_helper(args[0],n-1,args+1,1);
  else return fd_err(fd_TooManyArgs,"prim_reject_lexpr",
                     "%REJECT requires two or 2n+1 arguments",FD_VOID);
}

static fdtype prim_avoid_lexpr(int n,fdtype *args)
{
  if ((n==2)&&(FD_HASHTABLEP(args[1]))) {
    fdtype results=hashtable_reject(args[0],(fd_hashtable)args[1]);
    if (FD_EMPTY_CHOICEP(results))
      return fd_incref(args[0]);
    else return results;}
  else if ((n==2)&&(FD_HASHSETP(args[1]))) {
    fdtype results=hashtable_reject(args[0],(fd_hashset)args[1]);
    if (FD_EMPTY_CHOICEP(results))
      return fd_incref(args[0]);
    else return results;}
  else if ((n<=4)||(n%2)) {
    fdtype values=reject_helper(args[0],n-1,args+1,1);
    if (FD_EMPTY_CHOICEP(values))
      return fd_incref(args[0]);
    else return values;}
  else return fd_err(fd_TooManyArgs,"prim_avoid_lexpr",
                     "%AVOID requires two or 2n+1 arguments",FD_VOID);
}

/* Kleene* operations */

static fdtype getstar(fdtype frames,fdtype slotids)
{
  fdtype results=fd_incref(frames);
  FD_DO_CHOICES(slotid,slotids) {
    fdtype all=fd_inherit_values(frames,slotid,slotid);
    if (FD_ABORTP(all)) {
      fd_decref(results);
      return all;}
    else {FD_ADD_TO_CHOICE(results,all);}}
  return fd_simplify_choice(results);
}

static fdtype inherit_prim(fdtype slotids,fdtype frames,fdtype through)
{
  fdtype results=fd_incref(frames);
  FD_DO_CHOICES(through_id,through) {
    fdtype all=fd_inherit_values(frames,slotids,through_id);
    FD_ADD_TO_CHOICE(results,all);}
  return fd_simplify_choice(results);
}

static fdtype pathp(fdtype frames,fdtype slotids,fdtype values)
{
  if (fd_pathp(frames,slotids,values)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype getbasis(fdtype frames,fdtype lattice)
{
  return fd_simplify_choice(fd_get_basis(frames,lattice));
}

/* Frame creation */

static fdtype allocate_oids(fdtype pool,fdtype howmany)
{
  fd_pool p=arg2pool(pool);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"allocate_oids",pool);
  if (FD_VOIDP(howmany))
    return fd_pool_alloc(p,1);
  else if (FD_FIXNUMP(howmany))
    return fd_pool_alloc(p,FD_FIX2INT(howmany));
  else return fd_type_error(_("fixnum"),"allocate_oids",howmany);
}

static fdtype frame_create_lexpr(int n,fdtype *args)
{
  fdtype result; int i=(n%2);
  if (n==1) return fd_new_frame(args[0],FD_VOID,0);
  else if (n==2) return fd_new_frame(args[0],args[1],1);
  else if (n%2) {
    if ((FD_SLOTMAPP(args[0]))||(FD_SCHEMAPP(args[0]))||
        (FD_OIDP(args[0])))
      result=fd_deep_copy(args[0]);
    else result=fd_new_frame(args[0],FD_VOID,0);}
  else result=fd_new_frame(FD_TRUE,FD_VOID,0);
  if (FD_OIDP(result))
    while (i<n) {
      FD_DO_CHOICES(slotid,args[i])
        fd_frame_add(result,slotid,args[i+1]);
      i=i+2;}
  else while (i<n) {
      FD_DO_CHOICES(slotid,args[i])
        fd_add(result,slotid,args[i+1]);
      i=i+2;}
  return result;
}

static fdtype seq2frame_prim
  (fdtype poolspec,fdtype values,fdtype schema,fdtype dflt)
{
  if (!(FD_SEQUENCEP(schema)))
    return fd_type_error(_("sequence"),"seq2frame_prim",schema);
  else if (!(FD_SEQUENCEP(values)))
    return fd_type_error(_("sequence"),"seq2frame_prim",values);
  else {
    fdtype frame;
    int i=0, slen=fd_seq_length(schema), vlen=fd_seq_length(values);
    if (FD_FALSEP(poolspec))
      frame=fd_empty_slotmap();
    else if (FD_OIDP(poolspec)) frame=poolspec;
    else {
      fdtype oid, slotmap;
      fd_pool p=arg2pool(poolspec);
      if (p==NULL)
        return fd_type_error(_("pool spec"),"seq2frame_prim",poolspec);
      oid=fd_pool_alloc(p,1);
      if (FD_ABORTP(oid)) return oid;
      slotmap=fd_empty_slotmap();
      if (fd_set_oid_value(oid,slotmap)<0) {
        fd_decref(slotmap);
        return FD_ERROR_VALUE;}
      else {
        fd_decref(slotmap); frame=oid;}}
    while ((i<slen) && (i<vlen)) {
      fdtype slotid=fd_seq_elt(schema,i);
      fdtype value=fd_seq_elt(values,i);
      int retval;
      if (FD_OIDP(frame))
        retval=fd_frame_add(frame,slotid,value);
      else retval=fd_add(frame,slotid,value);
      if (retval<0) {
        fd_decref(slotid); fd_decref(value);
        if (FD_FALSEP(poolspec)) fd_decref(frame);
        return FD_ERROR_VALUE;}
      fd_decref(slotid); fd_decref(value);
      i++;}
    if (!(FD_VOIDP(dflt)))
      while (i<slen) {
        fdtype slotid=fd_seq_elt(schema,i);
        int retval;
        if (FD_OIDP(frame))
          retval=fd_frame_add(frame,slotid,dflt);
        else retval=fd_add(frame,slotid,dflt);
        if (retval<0) {
          fd_decref(slotid);
          if (FD_FALSEP(poolspec)) fd_decref(frame);
          return FD_ERROR_VALUE;}
        fd_decref(slotid); i++;}
    return frame;}
}

static int doassert(fdtype f,fdtype s,fdtype v)
{
  if (FD_EMPTY_CHOICEP(v)) return 0;
  else if (FD_OIDP(f))
    return fd_frame_add(f,s,v);
  else return fd_add(f,s,v);
}

static int doretract(fdtype f,fdtype s,fdtype v)
{
  if (FD_EMPTY_CHOICEP(v)) return 0;
  else if (FD_OIDP(f))
    return fd_frame_drop(f,s,v);
  else return fd_drop(f,s,v);
}

static fdtype modify_frame_lexpr(int n,fdtype *args)
{
  if (n%2==0)
    return fd_err(fd_SyntaxError,"FRAME-MODIFY",NULL,FD_VOID);
  else {
    FD_DO_CHOICES(frame,args[0]) {
      int i=1; while (i<n) {
        FD_DO_CHOICES(slotid,args[i])
          if (FD_PAIRP(slotid))
            if ((FD_PAIRP(FD_CDR(slotid))) &&
                ((FD_OIDP(FD_CAR(slotid))) || (FD_SYMBOLP(FD_CAR(slotid)))) &&
                (FD_EQ(FD_CADR(slotid),drop_symbol)))
              if (doretract(frame,FD_CAR(slotid),args[i+1])<0) {
                FD_STOP_DO_CHOICES;
                return FD_ERROR_VALUE;}
              else {}
            else {
              u8_log(LOG_WARN,fd_TypeError,"frame_modify_lexpr","slotid");}
          else if ((FD_SYMBOLP(slotid)) || (FD_OIDP(slotid)))
            if (doassert(frame,slotid,args[i+1])<0) {
              FD_STOP_DO_CHOICES;
              return FD_ERROR_VALUE;}
            else {}
          else u8_log(LOG_WARN,fd_TypeError,"frame_modify_lexpr","slotid");
        i=i+2;}}
    return fd_incref(args[0]);}
}

/* OID operations */

static fdtype oid_plus_prim(fdtype oid,fdtype increment)
{
  FD_OID base=FD_OID_ADDR(oid), next;
  int delta=fd_getint(increment);
  next=FD_OID_PLUS(base,delta);
  return fd_make_oid(next);
}

static fdtype oid_offset_prim(fdtype oidarg,fdtype against)
{
  FD_OID oid=FD_OID_ADDR(oidarg), base; int cap=-1;
  if (FD_OIDP(against)) {
    base=FD_OID_ADDR(against);}
  else if (FD_POOLP(against)) {
    fd_pool p=fd_lisp2pool(against);
    if (p) {base=p->base; cap=p->capacity;}
    else return FD_ERROR_VALUE;}
  else if ((FD_VOIDP(against)) || (FD_FALSEP(against))) {
    fd_pool p=fd_oid2pool(oidarg);
    if (p) {base=p->base; cap=p->capacity;}
    else return FD_INT((FD_OID_LO(oid))%0x100000);}
  else return fd_type_error(_("offset base"),"oid_offset_prim",against);
  if ((FD_OID_HI(oid))==(FD_OID_HI(base))) {
    int diff=(FD_OID_LO(oid))-(FD_OID_LO(base));
    if ((diff>=0) && ((cap<0) || (diff<cap)))
      return FD_INT(diff);
    else return FD_FALSE;}
  else return FD_FALSE;
}

#ifdef FD_OID_BASE_ID
static fdtype oid_ptrdata_prim(fdtype oid)
{
  return fd_conspair(FD_INT(FD_OID_BASE_ID(oid)),
                     FD_INT(FD_OID_BASE_OFFSET(oid)));
}
#endif

static fdtype make_oid_prim(fdtype high,fdtype low)
{
  if (FD_VOIDP(low)) {
    FD_OID oid;
    unsigned long long addr;
    if (FD_FIXNUMP(high))
      addr=FD_FIX2INT(high);
    else if (FD_BIGINTP(high))
      addr=fd_bigint2uint64((fd_bigint)high);
    else return fd_type_error("integer","make_oid_prim",high);
#if FD_STRUCT_OIDS
    memset(&oid,0,sizeof(FD_OID));
    oid.hi=(addr>>32)&0xFFFFFFFF;
    oid.log=(addr)&0xFFFFFFFF;
#else
    oid=addr;
#endif
    return fd_make_oid(oid);}
  else if (FD_OIDP(high)) {
    FD_OID base=FD_OID_ADDR(high), oid; unsigned int off;
    if (FD_FIXNUMP(low))
      off=FD_FIX2INT(low);
    else if (FD_BIGINTP(low)) {
      if ((off=fd_bigint2uint((struct FD_BIGINT *)low))==0)
        return fd_type_error("uint32","make_oid_prim",low);}
    else return fd_type_error("uint32","make_oid_prim",low);
    oid=FD_OID_PLUS(base,off);
    return fd_make_oid(oid);}
  else {
    unsigned int hi, lo;
#if FD_STRUCT_OIDS
    FD_OID addr; memset(&addr,0,sizeof(FD_OID));
#else
    FD_OID addr=0;
#endif
    FD_SET_OID_HI(addr,0); FD_SET_OID_LO(addr,0);
    if (FD_FIXNUMP(high))
      if ((FD_FIX2INT(high))<0)
        return fd_type_error("uint32","make_oid_prim",high);
      else hi=FD_FIX2INT(high);
    else if (FD_BIGINTP(high)) {
      hi=fd_bigint2uint((struct FD_BIGINT *)high);
      if (hi==0) return fd_type_error("uint32","make_oid_prim",high);}
    else return fd_type_error("uint32","make_oid_prim",high);
    if (FD_FIXNUMP(low))
      if ((FD_FIX2INT(low))<0)
        return fd_type_error("uint32","make_oid_prim",low);
      else lo=FD_FIX2INT(low);
    else if (FD_BIGINTP(low)) {
      lo=fd_bigint2uint((struct FD_BIGINT *)low);
      if (lo==0) return fd_type_error("uint32","make_oid_prim",low);}
    else return fd_type_error("uint32","make_oid_prim",low);
    FD_SET_OID_HI(addr,hi);
    FD_SET_OID_LO(addr,lo);
    return fd_make_oid(addr);}
}

static fdtype oid2string_prim(fdtype oid,fdtype name)
{
  FD_OID addr=FD_OID_ADDR(oid);
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);

  if (FD_VOIDP(name))
    u8_printf(&out,"@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));
  else if ((FD_STRINGP(name)) || (FD_CHOICEP(name)) ||
           (FD_PAIRP(name)) || (FD_VECTORP(name)))
    u8_printf(&out,"@%x/%x%q",FD_OID_HI(addr),FD_OID_LO(addr),name);
  else u8_printf(&out,"@%x/%x{%q}",FD_OID_HI(addr),FD_OID_LO(addr),name);

  return fd_stream2string(&out);
}

static fdtype oidhex_prim(fdtype oid,fdtype base_arg)
{
  char buf[64]; int offset;
  FD_OID addr=FD_OID_ADDR(oid);
  if ((FD_VOIDP(base_arg)) || (FD_FALSEP(base_arg))) {
    fd_pool p=fd_oid2pool(oid);
    if (p) {
      FD_OID base=p->base;
      offset=FD_OID_DIFFERENCE(addr,base);}
    else offset=(FD_OID_LO(addr))%0x100000;}
  else if (FD_POOLP(base_arg)) {
    fd_pool p=fd_get_poolptr(base_arg);
    if (p) {
      FD_OID base=p->base;
      offset=FD_OID_DIFFERENCE(addr,base);}
    else offset=(FD_OID_LO(addr))%0x100000;}
  else if (FD_OIDP(base_arg)) {
    FD_OID base=FD_OID_ADDR(base_arg);
    offset=FD_OID_DIFFERENCE(addr,base);}
  else offset=(FD_OID_LO(addr))%0x100000;
  sprintf(buf,"%x",offset);
  return fd_make_string(NULL,-1,buf);
}

static fdtype oidb32_prim(fdtype oid,fdtype base_arg)
{
  char buf[64]; int offset, buflen=64;
  FD_OID addr=FD_OID_ADDR(oid);
  if ((FD_VOIDP(base_arg)) || (FD_FALSEP(base_arg))) {
    fd_pool p=fd_oid2pool(oid);
    if (p) {
      FD_OID base=p->base;
      offset=FD_OID_DIFFERENCE(addr,base);}
    else offset=(FD_OID_LO(addr))%0x100000;}
  else if (FD_POOLP(base_arg)) {
    fd_pool p=fd_get_poolptr(base_arg);
    if (p) {
      FD_OID base=p->base;
      offset=FD_OID_DIFFERENCE(addr,base);}
    else offset=(FD_OID_LO(addr))%0x100000;}
  else if (FD_OIDP(base_arg)) {
    FD_OID base=FD_OID_ADDR(base_arg);
    offset=FD_OID_DIFFERENCE(addr,base);}
  else offset=(FD_OID_LO(addr))%0x100000;
  fd_ulonglong_to_b32(offset,buf,&buflen);
  return fd_make_string(NULL,buflen,buf);
}

static fdtype oidplus(FD_OID base,int delta)
{
  FD_OID next=FD_OID_PLUS(base,delta);
  return fd_make_oid(next);
}

static fdtype hex2oid_prim(fdtype arg,fdtype base_arg)
{
  char buf[64]; long offset; FD_OID base;
  if (FD_STRINGP(arg))
    offset=strtol(FD_STRDATA(arg),NULL,16);
  else if (FD_FIXNUMP(arg))
    offset=FD_FIX2INT(arg);
  else return fd_type_error("hex offset","hex2oid_prim",arg);
  if (offset<0) return fd_type_error("hex offset","hex2oid_prim",arg);
  if (FD_OIDP(base_arg)) {
    FD_OID base=FD_OID_ADDR(base_arg);
    return oidplus(base,offset);}
  else if (FD_POOLP(base_arg)) {
    fd_pool p=fd_get_poolptr(base_arg);
    return oidplus(p->base,offset);}
  else if (FD_STRINGP(base_arg)) {
    fd_pool p=fd_name2pool(FD_STRDATA(base_arg));
    if (p) return oidplus(p->base,offset);
    else return fd_type_error("pool id","hex2oid_prim",base_arg);}
  else return fd_type_error("pool id","hex2oid_prim",base_arg);
}

static fdtype b32oid_prim(fdtype arg,fdtype base_arg)
{
  char buf[64]; long long offset; 
  FD_OID base; 
  if (FD_STRINGP(arg)) {
    offset=fd_b32_to_longlong(FD_STRDATA(arg));
    if (offset<0) {
      fd_seterr(fd_ParseError,"b32oid",u8_strdup("invalid B32 string"),arg);
      return FD_ERROR_VALUE;}}
  else if (FD_FIXNUMP(arg))
    offset=FD_FIX2INT(arg);
  else return fd_type_error("b32 offset","b32oid_prim",arg);
  if (offset<0) return fd_type_error("hex offset","hex2oid_prim",arg);
  if (FD_OIDP(base_arg)) {
    FD_OID base=FD_OID_ADDR(base_arg);
    return oidplus(base,offset);}
  else if (FD_POOLP(base_arg)) {
    fd_pool p=fd_get_poolptr(base_arg);
    return oidplus(p->base,offset);}
  else if (FD_STRINGP(base_arg)) {
    fd_pool p=fd_name2pool(FD_STRDATA(base_arg));
    if (p) return oidplus(p->base,offset);
    else return fd_type_error("pool id","hex2oid_prim",base_arg);}
  else return fd_type_error("pool id","hex2oid_prim",base_arg);
}

static fdtype oidaddr_prim(fdtype oid)
{
  FD_OID oidaddr=FD_OID_ADDR(oid);
  if (FD_OID_HI(oidaddr)) {
#if FD_STRUCT_OIDS
    unsigned long long addr=((oid.hi)<<32)|(oid.lo);
#else
    unsigned long long addr=oidaddr;
#endif
    return FD_INT(addr);}
  else return FD_INT(FD_OID_LO(oid));
}

/* sumframe function */

static fdtype sumframe_prim(fdtype frames,fdtype slotids)
{
  fdtype results=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(frame,frames) {
    fdtype slotmap=fd_empty_slotmap();
    FD_DO_CHOICES(slotid,slotids) {
      fdtype value=fd_get(frame,slotid,FD_EMPTY_CHOICE);
      if (FD_ABORTP(value)) {
        fd_decref(results); return value;}
      else if (fd_add(slotmap,slotid,value)<0) {
        fd_decref(results); fd_decref(value);
        return FD_ERROR_VALUE;}
      fd_decref(value);}
    if (FD_OIDP(frame)) {
      FD_OID addr=FD_OID_ADDR(frame);
      u8_string s=u8_mkstring("@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));
      fdtype idstring=fd_lispstring(s);
      if (fd_add(slotmap,id_symbol,idstring)<0) {
        fd_decref(results); fd_decref(idstring);
        return FD_ERROR_VALUE;}
      else fd_decref(idstring);}
    FD_ADD_TO_CHOICE(results,slotmap);}
  return results;
}

/* Graph walking functions */

static fdtype applyfn(fdtype fn,fdtype node)
{
  if ((FD_OIDP(fn)) || (FD_SYMBOLP(fn)))
    return fd_frame_get(node,fn);
  else if (FD_CHOICEP(fn)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(f,fn) {
      fdtype v=applyfn(fn,node);
      if (FD_ABORTP(v)) {
        fd_decref(results);
        return v;}
      else {FD_ADD_TO_CHOICE(results,v);}}
    return results;}
  else if (FD_APPLICABLEP(fn))
    return fd_apply(fn,1,&node);
  else if (FD_TABLEP(fn))
    return fd_get(fn,node,FD_EMPTY_CHOICE);
  else return FD_EMPTY_CHOICE;
}

static int walkgraph(fdtype fn,fdtype state,fdtype arcs,
                      struct FD_HASHSET *seen,fdtype *results)
{
  fdtype next_state=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(node,state)
    if (!(fd_hashset_get(seen,node))) {
      fdtype output, expand;
      fd_hashset_add(seen,node);
      /* Apply the function */
      output=applyfn(fn,node);
      if (FD_ABORTP(output)) return fd_interr(output);
      else if (results) {FD_ADD_TO_CHOICE(*results,output);}
      else fd_decref(output);
      /* Do the expansion */
      expand=applyfn(arcs,node);
      if (FD_ABORTP(expand)) return fd_interr(expand);
      else {
        FD_DO_CHOICES(next,expand)
          if (!(fd_hashset_get(seen,next))) {
            fd_incref(next); FD_ADD_TO_CHOICE(next_state,next);}}
      fd_decref(expand);}
  fd_decref(state);
  if (!(FD_EMPTY_CHOICEP(next_state)))
    return walkgraph(fn,next_state,arcs,seen,results);
  else return 1;
}

static fdtype forgraph(fdtype fcn,fdtype roots,fdtype arcs)
{
  struct FD_HASHSET hashset; int retval;
  if ((FD_OIDP(arcs)) || (FD_SYMBOLP(arcs)) ||
      (FD_APPLICABLEP(arcs)) || (FD_TABLEP(arcs))) {}
  else if (FD_CHOICEP(arcs)) {
    FD_DO_CHOICES(each,arcs)
      if (!((FD_OIDP(each)) || (FD_SYMBOLP(each)) ||
            (FD_APPLICABLEP(each)) || (FD_TABLEP(each))))
        return fd_type_error("mapfn","mapgraph",each);}
  else return fd_type_error("mapfn","mapgraph",arcs);
  fd_init_hashset(&hashset,1024,FD_STACK_CONS);
  fd_incref(roots);
  retval=walkgraph(fcn,roots,arcs,&hashset,NULL);
  if (retval<0) {
    fd_decref((fdtype)&hashset);
    return FD_ERROR_VALUE;}
  else {
    fd_decref((fdtype)&hashset);
    return FD_VOID;}
}

static fdtype mapgraph(fdtype fcn,fdtype roots,fdtype arcs)
{
  fdtype results=FD_EMPTY_CHOICE; int retval; struct FD_HASHSET hashset;
  if ((FD_OIDP(fcn)) || (FD_SYMBOLP(fcn)) ||
      (FD_APPLICABLEP(fcn)) || (FD_TABLEP(fcn))) {}
  else if (FD_CHOICEP(fcn)) {
    FD_DO_CHOICES(each,fcn)
      if (!((FD_OIDP(each)) || (FD_SYMBOLP(each)) ||
            (FD_APPLICABLEP(each)) || (FD_TABLEP(each))))
        return fd_type_error("mapfn","mapgraph",each);}
  else return fd_type_error("mapfn","mapgraph",fcn);
  if ((FD_OIDP(arcs)) || (FD_SYMBOLP(arcs)) ||
      (FD_APPLICABLEP(arcs)) || (FD_TABLEP(arcs))) {}
  else if (FD_CHOICEP(arcs)) {
    FD_DO_CHOICES(each,arcs)
      if (!((FD_OIDP(each)) || (FD_SYMBOLP(each)) ||
            (FD_APPLICABLEP(each)) || (FD_TABLEP(each))))
        return fd_type_error("mapfn","mapgraph",each);}
  else return fd_type_error("mapfn","mapgraph",arcs);
  fd_init_hashset(&hashset,1024,FD_STACK_CONS); fd_incref(roots);
  retval=walkgraph(fcn,roots,arcs,&hashset,&results);
  if (retval<0) {
    fd_decref((fdtype)&hashset);
    fd_decref(results);
    return FD_ERROR_VALUE;}
  else {
    fd_decref((fdtype)&hashset);
    return results;}
}

/* Helpful predicates */

static fdtype dbloadedp(fdtype arg1,fdtype arg2)
{
  if (FD_VOIDP(arg2))
    if (FD_OIDP(arg1)) {
      fd_pool p=fd_oid2pool(arg1);
      if (fd_hashtable_probe(&(p->locks),arg1)) {
        fdtype v=fd_hashtable_probe(&(p->locks),arg1);
        if ((v!=FD_VOID) || (v!=FD_LOCKHOLDER)) {
          fd_decref(v); return FD_TRUE;}
        else return FD_FALSE;}
      else if (fd_hashtable_probe(&(p->cache),arg1))
        return FD_TRUE;
      else return FD_FALSE;}
    else if (fd_hashtable_probe(&(fd_background->cache),arg1))
      return FD_TRUE;
    else return FD_FALSE;
  else if ((FD_INDEXP(arg2))||(FD_PRIM_TYPEP(arg2,fd_raw_index_type))) {
    fd_index ix=fd_indexptr(arg2);
    if (ix==NULL)
      return fd_type_error("index","loadedp",arg2);
    else if (fd_hashtable_probe(&(ix->cache),arg1))
      return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_POOLP(arg2))
    if (FD_OIDP(arg1)) {
      fd_pool p=fd_lisp2pool(arg2);
      if (p==NULL)
        return fd_type_error("pool","loadedp",arg2);
      if (fd_hashtable_probe(&(p->locks),arg1)) {
        fdtype v=fd_hashtable_probe(&(p->locks),arg1);
        if ((v!=FD_VOID) || (v!=FD_LOCKHOLDER)) {
          fd_decref(v); return FD_TRUE;}
        else return FD_FALSE;}
      else if (fd_hashtable_probe(&(p->cache),arg1))
        return FD_TRUE;
      else return FD_FALSE;}
    else return FD_FALSE;
  else if ((FD_STRINGP(arg2)) && (FD_OIDP(arg1))) {
    fd_pool p=fd_lisp2pool(arg2); fd_index ix;
    if (p)
      if (fd_hashtable_probe(&(p->cache),arg1))
        return FD_TRUE;
      else return FD_FALSE;
    else ix=fd_indexptr(arg2);
    if (ix==NULL)
      return fd_type_error("pool/index","loadedp",arg2);
    else if (fd_hashtable_probe(&(ix->cache),arg1))
      return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("pool/index","loadedp",arg2);
}

static int oidmodifiedp(fd_pool p,fdtype oid)
{
  if (fd_hashtable_probe(&(p->locks),oid)) {
    fdtype v=fd_hashtable_get(&(p->locks),oid,FD_VOID);
    int modified=1;
    if ((FD_VOIDP(v)) || (v==FD_LOCKHOLDER))
      modified=0;
    else if (FD_SLOTMAPP(v))
      if (FD_SLOTMAP_MODIFIEDP(v)) {}
      else modified=0;
    else if (FD_SCHEMAPP(v)) {
      if (FD_SCHEMAP_MODIFIEDP(v)) {}
      else modified=0;}
    else {}
    fd_decref(v);
    if (modified) return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

static fdtype dbmodifiedp(fdtype arg1,fdtype arg2)
{
  if (FD_VOIDP(arg2))
    if (FD_OIDP(arg1)) {
      fd_pool p=fd_oid2pool(arg1);
      if (oidmodifiedp(p,arg1))
        return FD_TRUE;
      else return FD_FALSE;}
    else if ((fd_hashtable_probe(&(fd_background->adds),arg1)) ||
             (fd_hashtable_probe(&(fd_background->edits),arg1)))
      return FD_TRUE;
    else return FD_FALSE;
  else if ((FD_INDEXP(arg2))||(FD_PRIM_TYPEP(arg2,fd_raw_index_type))) {
    fd_index ix=fd_indexptr(arg2);
    if (ix==NULL)
      return fd_type_error("index","loadedp",arg2);
    else if ((fd_hashtable_probe(&(ix->adds),arg1)) ||
             (fd_hashtable_probe(&(ix->edits),arg1)))
      return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_POOLP(arg2))
    if (FD_OIDP(arg1)) {
      fd_pool p=fd_lisp2pool(arg2);
      if (p==NULL)
        return fd_type_error("pool","loadedp",arg2);
      else if (oidmodifiedp(p,arg1))
        return FD_TRUE;
      else return FD_FALSE;}
    else return FD_FALSE;
  else if ((FD_STRINGP(arg2)) && (FD_OIDP(arg1))) {
    fd_pool p=fd_lisp2pool(arg2); fd_index ix;
    if (p)
      if (oidmodifiedp(p,arg1))
        return FD_TRUE;
      else return FD_FALSE;
    else ix=fd_indexptr(arg2);
    if (ix==NULL)
      return fd_type_error("pool/index","loadedp",arg2);
    else if ((fd_hashtable_probe(&(ix->adds),arg1)) ||
             (fd_hashtable_probe(&(ix->edits),arg1)))
      return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("pool/index","loadedp",arg2);
}

/* Overlays */

static fdtype overlay_get(fdtype f,fdtype slotid)
{
  return fd_overlay_get(f,slotid,0);
}

static fdtype overlay_add(fdtype f,fdtype slotid,fdtype v)
{
  return fd_overlay_add(f,slotid,v,0);
}

static fdtype overlay_drop(fdtype f,fdtype slotid,fdtype v)
{
  return fd_overlay_drop(f,slotid,v,0);
}

static fdtype overlay_store(fdtype f,fdtype slotid,fdtype v)
{
  return fd_overlay_store(f,slotid,v,0);
}

static fdtype overlay_index_get(fdtype f,fdtype slotid)
{
  return fd_overlay_get(f,slotid,1);
}

static fdtype overlay_index_add(fdtype f,fdtype slotid,fdtype v)
{
  return fd_overlay_add(f,slotid,v,1);
}

static fdtype overlay_index_drop(fdtype f,fdtype slotid,fdtype v)
{
  return fd_overlay_drop(f,slotid,v,1);
}

static fdtype overlay_index_store(fdtype f,fdtype slotid,fdtype v)
{
  return fd_overlay_store(f,slotid,v,1);
}


static fdtype wooverlay_handler(fdtype expr,fd_lispenv env)
{
  fdtype value=FD_VOID;
  if ((fd_inhibit_overlay) || (!(fd_overlayp()))) {
    FD_DOBODY(body_elt,expr,1) {
      fd_decref(value); value=fd_eval(body_elt,env);
      if (FD_ABORTP(value)) return value;}
    return value;}
  fd_inhibit_overlays(1);
  {FD_DOBODY(body_elt,expr,1) {
    fd_decref(value); value=fd_eval(body_elt,env);
    if (FD_ABORTP(value)) {
      fd_inhibit_overlays(0);
      return value;}}}
  fd_inhibit_overlays(0);
  return value;
}

/* Initializing */

FD_EXPORT void fd_init_dbfns_c()
{
  u8_register_source_file(_FILEINFO);

  fd_idefn(fd_scheme_module,fd_make_cprim1("SLOTID?",slotidp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("LOADED?",dbloadedp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MODIFIED?",dbmodifiedp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("LOCKED?",oidlockedp,1));

  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim2("GET",fd_fget,2)));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim3("TEST",fd_ftest,2)));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprimn("TESTP",testp,3)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("ASSERT!",fd_fassert,3)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("RETRACT!",fd_fretract,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("GETSLOTS",fd_getkeys,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("SUMFRAME",sumframe_prim,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("GETPATH",getpath_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("GETPATH*",getpathstar_prim,1)));

  fd_defspecial(fd_scheme_module,"CACHEGET",cacheget_handler);

  fd_idefn(fd_scheme_module,fd_make_cprim2("OV/GET",overlay_get,2));
  fd_idefn(fd_scheme_module,fd_make_cprim3("OV/ADD!",overlay_add,3));
  fd_idefn(fd_scheme_module,fd_make_cprim3("OV/DROP!",overlay_drop,3));
  fd_idefn(fd_scheme_module,fd_make_cprim3("OV/STORE!",overlay_store,3));

  fd_idefn(fd_scheme_module,fd_make_cprim2("OV/INDEX-GET",overlay_index_get,2));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3("OV/INDEX-ADD!",overlay_index_add,3));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3("OV/INDEX-DROP!",overlay_index_drop,3));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3("OV/INDEX-STORE!",overlay_index_store,3));

  fd_defspecial(fd_scheme_module,"W/O/OVERLAY",wooverlay_handler);


  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim2("GET*",getstar,2)));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim3("PATH?",pathp,3)));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim3("INHERIT",inherit_prim,3)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("GET-BASIS",getbasis,2)));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("OID-VALUE",oidvalue,1,fd_oid_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("SET-OID-VALUE!",setoidvalue,2,
                           fd_oid_type,FD_VOID,-1,FD_VOID,
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("LOCK-OID!",lockoid,2,
                           fd_oid_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("LOCK-OIDS!",lockoids,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("UNLOCK-OIDS!",unlockoids,0)));
  fd_idefn(fd_scheme_module,fd_make_cprim1("LOCKED-OIDS",lockedoids,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL?",poolp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("INDEX?",indexp,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2("SET-CACHE-LEVEL!",set_cache_level,2));

  fd_idefn(fd_scheme_module,fd_make_cprim1("NAME->POOL",getpool,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("GETPOOL",getpool,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("POOL-LABEL",pool_label,1,
                                            -1,FD_VOID,-1,FD_FALSE));
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
            -1,FD_VOID,fd_string_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("OID-RANGE",oid_range,2,
                           fd_oid_type,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("OID-VECTOR",oid_vector,2,
                           fd_oid_type,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("RANDOM-OID",random_oid,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-VECTOR",pool_vec,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("OID-HI",oidhi,1,fd_oid_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("OID-LO",oidlo,1,fd_oid_type,FD_VOID));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("OID?",oidp,1));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim1x("OID-POOL",oidpool,1,fd_oid_type,FD_VOID));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x("IN-POOL?",inpoolp,2,
                           fd_oid_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x("VALID-OID?",validoidp,1,
                           fd_oid_type,FD_VOID,-1,FD_VOID));

  fd_idefn(fd_xscheme_module,fd_make_cprim2("USE-POOL",use_pool,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("USE-INDEX",use_index,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim2("OPEN-INDEX",open_index,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("CACHECOUNT",cachecount,0));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim
           (fd_make_cprimn("FRAME-CREATE",frame_create_lexpr,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("MODIFY-FRAME",modify_frame_lexpr,3)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim4("SEQ->FRAME",seq2frame_prim,3)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2("ALLOCATE-OIDS",allocate_oids,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("OID-PLUS",oid_plus_prim,1,
                           fd_oid_type,FD_VOID,
                           fd_fixnum_type,FD_INT(1)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("OID-OFFSET",oid_offset_prim,1,
                           fd_oid_type,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MAKE-OID",make_oid_prim,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("OID->STRING",oid2string_prim,1,
                           fd_oid_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("OID-ADDR",oidaddr_prim,1,
                           fd_oid_type,FD_VOID));
#ifdef FD_OID_BASE_ID
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("OID-PTRDATA",oid_ptrdata_prim,1,
                           fd_oid_type,FD_VOID));
#endif

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("OID->HEX",oidhex_prim,1,
                           fd_oid_type,FD_VOID,
                           -1,FD_VOID));
  fd_defalias(fd_scheme_module,"OIDHEX","OID->HEX");
  fd_defalias(fd_scheme_module,"HEXOID","OID->HEX");
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("HEX->OID",hex2oid_prim,2,
                           -1,FD_VOID,-1,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("OID/B32",oidb32_prim,1,
                           fd_oid_type,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("B32/OID",b32oid_prim,2,
                           -1,FD_VOID,-1,FD_VOID));


  fd_idefn(fd_scheme_module,
           fd_make_cprim5x("MAKE-MEMPOOL",make_mempool,2,
                           fd_string_type,FD_VOID,
                           fd_oid_type,FD_VOID,
                           fd_fixnum_type,(FD_INT(1024*1024)),
                           fd_fixnum_type,(FD_INT(0)),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("CLEAN-MEMPOOL",clean_mempool,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("RESET-MEMPOOL",reset_mempool,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim9x("MAKE-EXTPOOL",make_extpool,4,
                           fd_string_type,FD_VOID,
                           fd_oid_type,FD_VOID,
                           fd_fixnum_type,FD_VOID,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_VOID,
                           -1,FD_TRUE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("EXTPOOL-CACHE!",extpool_setcache,3,
                           fd_pool_type,FD_VOID,fd_oid_type,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTPOOL-FETCHFN",extpool_fetchfn,1,
                           fd_pool_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTPOOL-SAVEFN",extpool_savefn,1,
                           fd_pool_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTPOOL-LOCKFN",extpool_lockfn,1,
                           fd_pool_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTPOOL-STATE",extpool_state,1,
                           fd_pool_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim5x("MAKE-EXTINDEX",make_extindex,2,
                           fd_string_type,FD_VOID,
                           -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,
                           -1,FD_TRUE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim5x("CONS-EXTINDEX",cons_extindex,2,
                           fd_string_type,FD_VOID,
                           -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,
                           -1,FD_TRUE));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("EXTINDEX-DECACHE!",extindex_decache,1,
                           -1,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("EXTINDEX-CACHEADD!",extindex_cacheadd,3,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTINDEX-FETCHFN",extindex_fetchfn,1,
                           -1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTINDEX-COMMITFN",extindex_commitfn,1,
                           -1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("EXTINDEX-STATE",extindex_state,1,
                           -1,FD_VOID));

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
                          ("FIND-FRAMES-PREFETCH!",find_frames_prefetch,2)));

  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprimn("SWAPOUT",swapout_lexpr,0)));
  fd_idefn(fd_xscheme_module,fd_make_cprimn("COMMIT",commit_lexpr,0));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("POOL-CLOSE",pool_close_prim,1));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim1("CLEAR-SLOTCACHE!",clear_slotcache,0));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim0("CLEARCACHES",clearcaches,0));

  fd_idefn(fd_xscheme_module,fd_make_cprim0("SWAPCHECK",swapcheck_prim,0));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PREFETCH-OIDS!",prefetch_oids,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("PREFETCH-KEYS!",prefetch_keys,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("FETCHOIDS",fetchoids_prim,1)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("CACHED-OIDS",cached_oids,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CACHED-KEYS",cached_keys,0));

  fd_idefn(fd_xscheme_module,
           fd_make_ndprim
           (fd_make_cprimn("MAKE-COMPOUND-INDEX",make_compound_index,1)));

  fd_idefn(fd_xscheme_module,
           fd_make_cprim2("ADD-TO-COMPOUND-INDEX!",add_to_compound_index,2));

  fd_idefn(fd_xscheme_module,
           fd_make_ndprim(fd_make_cprim4("INDEX-FRAME",index_frame_prim,3)));
  fd_idefn(fd_xscheme_module,fd_make_cprim3("INDEX-SET!",indexset,3));
  fd_idefn(fd_xscheme_module,fd_make_cprim3("INDEX-ADD!",indexadd,3));
  fd_idefn(fd_xscheme_module,fd_make_cprim2("INDEX-GET",indexget,2));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("INDEX-KEYS",indexkeys,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("INDEX-KEYSVEC",indexkeysvec,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("INDEX-SIZES",indexsizes,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("INDEX-SOURCE",indexsource,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("SUGGEST-HASH-SIZE",suggest_hash_size,1,
                                            fd_fixnum_type,FD_VOID));
  fd_idefn(fd_xscheme_module,fd_make_cprim3("INDEX-DECACHE",indexdecache,2));
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

  fd_idefn(fd_xscheme_module,fd_make_cprim3("USE-ADJUNCT",use_adjunct,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim4("SET-POOL-OP!",set_pool_op,4));

  id_symbol=fd_intern("%ID");
  adjunct_symbol=fd_intern("%ADJUNCT");
  pools_symbol=fd_intern("POOLS");
  indices_symbol=fd_intern("INDICES");
  drop_symbol=fd_intern("DROP");

}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
