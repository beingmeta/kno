/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_POOLS 1
#define FD_INLINE_BUFIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/apply.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include "libu8/u8printf.h"

/* Fetch pools */

/* Fetch pools are pools which keep their data externally and take care
   of their own data management.  They basically have a fetch function and
   a load function but may be extended to be clever about saving in some way. */

static lispval poolopt(lispval opts,u8_string name)
{
  return fd_getopt(opts,fd_intern(name),VOID);
}

FD_EXPORT
fd_pool fd_make_procpool(FD_OID base,
			 unsigned int cap,unsigned int load,
			 lispval opts,lispval state,
			 u8_string label,u8_string source)
{
  if (load>cap) {
    u8_seterr(fd_PoolOverflow,"fd_make_procpool",
	      u8_sprintf(NULL,256,
			 "Specified load (%u) > capacity (%u) for '%s'",
			 load,cap,(source)?(source):(label)));
    return NULL;}

  struct FD_PROCPOOL *pp = u8_alloc(struct FD_PROCPOOL);
  unsigned int flags = FD_STORAGE_ISPOOL | FD_STORAGE_VIRTUAL;
  memset(pp,0,sizeof(struct FD_PROCPOOL));
  lispval source_opt = FD_VOID;

  if (source == NULL) {
    source_opt = fd_getopt(opts,FDSYM_SOURCE,FD_VOID);
    if (FD_STRINGP(source_opt))
      source = CSTRING(source_opt);}

  fd_init_pool((fd_pool)pp,base,cap,
	       &fd_procpool_handler,
	       label,source);

  if (fd_testopt(opts,FDSYM_CACHELEVEL,FD_VOID)) {
    lispval v = fd_getopt(opts,FDSYM_CACHELEVEL,FD_VOID);
    if (FD_FALSEP(v))
      pp->pool_cache_level=0;
    else if (FD_FIXNUMP(v)) {
      int ival=FD_FIX2INT(v);
      pp->pool_cache_level=ival;}
    else if ( (FD_TRUEP(v)) || (v == FD_DEFAULT_VALUE) ) {}
    else u8_log(LOGCRIT,"BadCacheLevel",
		"Invalid cache level %q specified for procpool %s",
		v,label);
    fd_decref(v);}

  if (fd_testopt(opts,fd_intern("ADJUNCT"),FD_VOID))
    flags |= FD_POOL_ADJUNCT;
  if (fd_testopt(opts,fd_intern("READONLY"),FD_VOID))
    flags |= FD_STORAGE_READ_ONLY;
  if (!(fd_testopt(opts,fd_intern("REGISTER"),FD_VOID)))
    flags |= FD_STORAGE_UNREGISTERED;
  flags |= FD_POOL_SPARSE;

  pp->pool_flags = flags;
  pp->pool_opts = fd_getopt(opts,fd_intern("OPTS"),FD_FALSE);

  fd_register_pool((fd_pool)pp);
  pp->allocfn = poolopt(opts,"ALLOCFN");
  pp->getloadfn = poolopt(opts,"GETLOADFN");
  pp->fetchfn = poolopt(opts,"FETCHFN");
  pp->fetchnfn = poolopt(opts,"FETCHNFN");
  pp->lockfn = poolopt(opts,"LOCKFN");
  pp->releasefn = poolopt(opts,"RELEASEFN");
  pp->storenfn = poolopt(opts,"STOREN");
  pp->metadatafn = poolopt(opts,"METADATAFN");
  pp->createfn = poolopt(opts,"CREATEFN");
  pp->closefn = poolopt(opts,"CLOSEFN");
  pp->ctlfn = poolopt(opts,"CTLFN");
  pp->pool_state = state;
  fd_incref(state);
  pp->pool_label = label;

  fd_register_pool((fd_pool)pp);

  fd_decref(source_opt);

  return (fd_pool)pp;
}

static lispval procpool_fetch(fd_pool p,lispval oid)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  lispval args[]={lp,pp->pool_state,oid};
  if (VOIDP(pp->fetchfn)) return VOID;
  else return fd_dapply(pp->fetchfn,3,args);
}

static lispval *procpool_fetchn(fd_pool p,int n,lispval *oids)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  if (VOIDP(pp->fetchnfn)) return NULL;
  else {
    lispval oidvec = fd_make_vector(n,oids);
    lispval args[]={lp,pp->pool_state,oidvec};
    lispval result = fd_dapply(pp->fetchnfn,3,args);
    fd_decref(oidvec);
    if (VECTORP(result)) {
      lispval *vals = u8_alloc_n(n,lispval);
      int i = 0; while (i<n) {
	lispval val = VEC_REF(result,i);
	FD_VECTOR_SET(result,i,VOID);
	vals[i++]=val;}
      fd_decref(result);
      return vals;}
    else {
      fd_decref(result);
      return NULL;}}
}

static int procpool_lock(fd_pool p,lispval oid)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  if (VOIDP(pp->lockfn))
    return 0;
  else {
    lispval args[]={lp,pp->pool_state,oid};
    lispval result = fd_dapply(pp->lockfn,3,args);
    if (FIXNUMP(result)) return result;
    else if (FD_TRUEP(result)) return 1;
    else if (FALSEP(result)) return 0;
    else if (EMPTYP(result)) return 0;
    else {fd_decref(result); return -1;}}
}

static int procpool_swapout(fd_pool p,lispval oid)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  if (VOIDP(pp->swapoutfn))
    return 0;
  else {
    lispval args[]={lp,pp->pool_state,oid};
    lispval result = fd_dapply(pp->swapoutfn,3,args);
    if (FIXNUMP(result)) return result;
    else if (FD_TRUEP(result)) return 1;
    else if (FALSEP(result)) return 0;
    else if (EMPTYP(result)) return 0;
    else {fd_decref(result); return -1;}}
}

static lispval procpool_alloc(fd_pool p,int n)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  lispval n_arg = FD_INT(n);
  lispval args[3]={lp,pp->pool_state,n_arg};
  if (VOIDP(pp->fetchfn)) return VOID;
  else return fd_dapply(pp->allocfn,3,args);
}

static int procpool_release(fd_pool p,lispval oid)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  if (VOIDP(pp->releasefn))
    return 0;
  else {
    lispval args[3]={lp,pp->pool_state,oid};
    lispval result = fd_dapply(pp->releasefn,3,args);
    if (FIXNUMP(result)) return result;
    else if (FD_TRUEP(result)) return 1;
    else if (FALSEP(result)) return 0;
    else {fd_decref(result); return -1;}}
}

static int procpool_storen(fd_pool p,int n,lispval *oids,lispval *values)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  if (VOIDP(pp->storenfn)) return 0;
  else {
    lispval oidvec = fd_make_vector(n,oids);
    lispval valuevec = fd_make_vector(n,values);
    int i = 0; while (i<n) {fd_incref(values[i]); i++;}
    lispval args[4]={lp,pp->pool_state,oidvec,valuevec};
    lispval result = fd_dapply(pp->storenfn,4,args);
    fd_decref(oidvec);
    fd_decref(valuevec);
    if (FIXNUMP(result))
      return result;
    else if (FALSEP(result))
      return 0;
    else if (FD_ABORTP(result))
      return -1;
    else if (FD_TRUEP(result))
      return n;
    else {
      fd_decref(result);
      return -1;}}
}

static void procpool_close(fd_pool p)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  if (VOIDP(pp->closefn)) return;
  else {
    lispval args[2]={lp,pp->pool_state};
    lispval result = fd_dapply(pp->closefn,2,args);
    fd_decref(result);
    return;}
}

static int procpool_getload(fd_pool p)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  if ((VOIDP(pp->getloadfn)) || (FD_NULLP(pp->getloadfn)))
    return 0;
  else {
    lispval args[2]={lp,pp->pool_state};
    lispval result = fd_dapply(pp->getloadfn,2,args);
    if (FD_UINTP(result))
      return FIX2INT(result);
    else {
      fd_decref(result);
      return -1;}}
}

static lispval procpool_ctl(fd_pool p,lispval opid,int n,lispval *args)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  if (VOIDP(pp->ctlfn))
    return fd_default_poolctl(p,opid,n,args);
  else {
    lispval _argbuf[32], *argbuf=_argbuf;
    if (n+3>32) argbuf = u8_alloc_n(n+3,lispval);
    argbuf[0]=lp; argbuf[1]=pp->pool_state;
    argbuf[2]=opid;
    memcpy(argbuf+3,args,sizeof(lispval)*n);
    if (argbuf==_argbuf)
      return fd_dapply(pp->ctlfn,n+3,argbuf);
    else {
      lispval result = fd_dapply(pp->ctlfn,n+3,argbuf);
      u8_free(argbuf);
      return result;}}
}

static void recycle_procpool(fd_pool p)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  fd_decref(pp->pool_state);
  fd_decref(pp->allocfn);
  fd_decref(pp->fetchfn);
  fd_decref(pp->fetchnfn);
  fd_decref(pp->lockfn);
  fd_decref(pp->releasefn);
  fd_decref(pp->getloadfn);
  fd_decref(pp->swapoutfn);
  fd_decref(pp->storenfn);
  fd_decref(pp->metadatafn);
  fd_decref(pp->createfn);
  fd_decref(pp->closefn);
  fd_decref(pp->ctlfn);
}

struct FD_POOL_HANDLER fd_procpool_handler={
  "procpool", 1, sizeof(struct FD_PROCPOOL), 12,
  procpool_close, /* close */
  procpool_alloc, /* alloc */
  procpool_fetch, /* fetch */
  procpool_fetchn, /* fetchn */
  procpool_getload, /* getload */
  procpool_lock, /* lock */
  procpool_release, /* release */
  procpool_storen, /* storen */
  procpool_swapout, /* swapout */
  NULL, /* create */
  NULL,  /* walk */
  recycle_procpool, /* recycle */
  procpool_ctl  /* poolctl */
};

FD_EXPORT void fd_init_procpool_c()
{
  u8_register_source_file(_FILEINFO);
}
