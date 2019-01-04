/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_POOLS 1
#define FD_INLINE_BUFIO 1
#include "framerd/components/storage_layer.h"

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

#define NO_METHODP(x) ( (FD_NULLP(x)) || (FD_VOIDP(x)) )

FD_EXPORT
fd_pool fd_make_procpool(FD_OID base,
                         unsigned int cap,unsigned int load,
                         lispval opts,lispval state,
                         u8_string label,
                         u8_string source)
{
  if (load>cap) {
    u8_seterr(fd_PoolOverflow,"fd_make_procpool",
              u8_sprintf(NULL,256,
                         "Specified load (%u) > capacity (%u) for '%s'",
                         load,cap,(source)?(source):(label)));
    return NULL;}

  struct FD_PROCPOOL *pp = u8_alloc(struct FD_PROCPOOL);
  struct FD_PROCPOOL_METHODS *methods;
  unsigned int flags = FD_STORAGE_ISPOOL;
  lispval pool_type = fd_getopt(opts,FDSYM_TYPE,FD_VOID);
  lispval metadata = fd_getopt(opts,FDSYM_METADATA,FD_VOID);
  struct FD_POOL_TYPEINFO *typeinfo =
    (FD_STRINGP(pool_type)) ?
    (fd_get_pool_typeinfo(FD_CSTRING(pool_type))) :
    (FD_SYMBOLP(pool_type)) ? 
    (fd_get_pool_typeinfo(FD_SYMBOL_NAME(pool_type))) :
    (NULL);
  int cache_level = -1;

  if ( (typeinfo) && (typeinfo->type_data) )
    methods = (struct FD_PROCPOOL_METHODS *) (typeinfo->type_data);
  else {
    methods = u8_alloc(struct FD_PROCPOOL_METHODS);
    memset(methods,0,sizeof(struct FD_PROCPOOL_METHODS));
    methods->allocfn = poolopt(opts,"ALLOC");
    methods->getloadfn = poolopt(opts,"GETLOAD");
    methods->fetchfn = poolopt(opts,"FETCH");
    methods->fetchnfn = poolopt(opts,"FETCHN");
    methods->lockfn = poolopt(opts,"LOCKOIDS");
    methods->releasefn = poolopt(opts,"RELEASEOIDS");
    methods->commitfn = poolopt(opts,"COMMIT");
    methods->metadatafn = poolopt(opts,"GETMETADATA");
    methods->createfn = poolopt(opts,"CREATE");
    methods->closefn = poolopt(opts,"CLOSE");
    methods->ctlfn = poolopt(opts,"POOLCTL");}

  memset(pp,0,sizeof(struct FD_PROCPOOL));
  lispval source_opt = FD_VOID;

  if (source == NULL) {
    source_opt = fd_getopt(opts,FDSYM_SOURCE,FD_VOID);
    if (FD_STRINGP(source_opt))
      source = CSTRING(source_opt);}

  if (fd_testopt(opts,FDSYM_CACHELEVEL,FD_VOID)) {
    lispval v = fd_getopt(opts,FDSYM_CACHELEVEL,FD_VOID);
    if (FD_FALSEP(v))
      cache_level=0;
    else if (FD_FIXNUMP(v))
      cache_level=FD_FIX2INT(v);
    else if ( (FD_TRUEP(v)) || (v == FD_DEFAULT_VALUE) ) {}
    else u8_logf(LOG_CRIT,"BadCacheLevel",
                 "Invalid cache level %q specified for procpool %s",
                 v,label);
    fd_decref(v);}

  flags |= FD_POOL_SPARSE;
  if (fd_testopt(opts,fd_intern("ADJUNCT"),FD_VOID))
    flags |= FD_POOL_ADJUNCT;
  if (fd_testopt(opts,fd_intern("READONLY"),FD_VOID))
    flags |= FD_STORAGE_READ_ONLY;
  if (fd_testopt(opts,fd_intern("VIRTUAL"),FD_VOID))
    flags |= FD_STORAGE_VIRTUAL;
  lispval rval = fd_getopt(opts,fd_intern("REGISTER"),FD_VOID);
  if (FD_FALSEP(rval))
    flags |= FD_STORAGE_UNREGISTERED;
  else fd_decref(rval);

  fd_init_pool((fd_pool)pp,base,cap,
               &fd_procpool_handler,
               label,source,source,
               flags,
               metadata,
               opts);
  pp->pool_methods = methods;
  pp->pool_state   = state; fd_incref(state);
  pp->pool_label = u8_strdup(label);
  pp->pool_cache_level = cache_level;

  if (fd_testopt(opts,fd_intern("TYPEID"),VOID)) {
    lispval idval = poolopt(opts,"TYPEID");
    if (STRINGP(idval))
      pp->pool_typeid = u8_strdup(CSTRING(idval));
    else if (SYMBOLP(idval))
      pp->pool_typeid = u8_strdup(FD_SYMBOL_NAME(idval));
    else if (FD_OIDP(idval)) {
      FD_OID addr = FD_OID_ADDR(idval);
      pp->pool_typeid =
        u8_mkstring("@%lx/%lx",FD_OID_HI(addr),FD_OID_LO(addr));}
    else u8_logf(LOG_WARN,"BadPoolTypeID","%q",idval);
    fd_decref(idval);}

  fd_register_pool((fd_pool)pp);
  fd_decref(metadata);
  fd_decref(source_opt);

  return (fd_pool)pp;
}

static lispval procpool_fetch(fd_pool p,lispval oid)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  lispval args[]={lp,pp->pool_state,oid};
  if (NO_METHODP(pp->pool_methods->fetchfn))
    return VOID;
  else return fd_dapply(pp->pool_methods->fetchfn,3,args);
}

static lispval *procpool_fetchn(fd_pool p,int n,lispval *oids)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  if (NO_METHODP(pp->pool_methods->fetchnfn)) {
    lispval *vals = u8_big_alloc_n(n,lispval);
    int i = 0; while (i<n) {
      lispval oid = oids[i];
      lispval v = procpool_fetch(p,oid);
      if (FD_ABORTP(v)) {
        fd_decref_vec(vals,i);
        u8_big_free(vals);
        return NULL;}
      else vals[i++]=v;}
    return vals;}
  else {
    lispval oidvec = fd_make_vector(n,oids);
    lispval args[]={lp,pp->pool_state,oidvec};
    lispval result = fd_dapply(pp->pool_methods->fetchnfn,3,args);
    fd_decref(oidvec);
    if (VECTORP(result)) {
      lispval *vals = u8_big_alloc_n(n,lispval);
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
  if (NO_METHODP(pp->pool_methods->lockfn))
    return 0;
  else {
    lispval args[]={lp,pp->pool_state,oid};
    lispval result = fd_dapply(pp->pool_methods->lockfn,3,args);
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
  if (NO_METHODP(pp->pool_methods->swapoutfn))
    return 0;
  else {
    lispval args[]={lp,pp->pool_state,oid};
    lispval result = fd_dapply(pp->pool_methods->swapoutfn,3,args);
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
  if (NO_METHODP(pp->pool_methods->allocfn))
    return VOID;
  else return fd_dapply(pp->pool_methods->allocfn,3,args);
}

static int procpool_release(fd_pool p,lispval oid)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  if (NO_METHODP(pp->pool_methods->releasefn))
    return 0;
  else {
    lispval args[3]={lp,pp->pool_state,oid};
    lispval result = fd_dapply(pp->pool_methods->releasefn,3,args);
    if (FIXNUMP(result)) return fd_getint(result);
    else if (FD_TRUEP(result)) return 1;
    else if (FALSEP(result)) return 0;
    else {fd_decref(result); return -1;}}
}

static int procpool_commit(fd_pool p,fd_commit_phase phase,
                           struct FD_POOL_COMMITS *commits)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  if (NO_METHODP(pp->pool_methods->commitfn))
    return 0;
  else {
    lispval lp = fd_pool2lisp(p);
    struct FD_VECTOR oidvec = { 0 }, valvec = { 0 };
    FD_INIT_STATIC_CONS(&oidvec,fd_vector_type);
    FD_INIT_STATIC_CONS(&valvec,fd_vector_type);
    oidvec.vec_length = commits->commit_count;
    oidvec.vec_elts = commits->commit_oids;
    valvec.vec_length = commits->commit_count;
    valvec.vec_elts = commits->commit_vals;
    lispval args[6] = { lp, pp->pool_state, fd_commit_phases[phase],
                        ((lispval)(&oidvec)),
                        ((lispval)(&valvec)),
                        ( (FD_VOIDP(commits->commit_metadata)) ?
                          (FD_FALSE) :
                          (commits->commit_metadata) ) };
    lispval result = fd_apply(pp->pool_methods->commitfn,6,args);
    if (FIXNUMP(result))
      return FD_INT(result);
    else if (FALSEP(result))
      return 0;
    else if (FD_ABORTP(result))
      return -1;
    else if (FD_TRUEP(result))
      return 1;
    else {
      fd_decref(result);
      return -1;}}
}

static void procpool_close(fd_pool p)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  if (NO_METHODP(pp->pool_methods->closefn))
    return;
  else {
    lispval args[2]={lp,pp->pool_state};
    lispval result = fd_dapply(pp->pool_methods->closefn,2,args);
    fd_decref(result);
    return;}
}

static int procpool_getload(fd_pool p)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  lispval lp = fd_pool2lisp(p);
  if (NO_METHODP(pp->pool_methods->getloadfn))
    return 0;
  else {
    lispval args[2]={lp,pp->pool_state};
    lispval result = fd_dapply(pp->pool_methods->getloadfn,2,args);
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
  if (NO_METHODP(pp->pool_methods->ctlfn))
    return fd_default_poolctl(p,opid,n,args);
  else {
    lispval _argbuf[32], *argbuf=_argbuf;
    if ((n+3) > 32) argbuf = u8_alloc_n(n+3,lispval);
    argbuf[0]=lp; argbuf[1]=pp->pool_state;
    argbuf[2]=opid;
    memcpy(argbuf+3,args,LISPVEC_BYTELEN(n));
    if (argbuf==_argbuf)
      return fd_dapply(pp->pool_methods->ctlfn,n+3,argbuf);
    else {
      lispval result = fd_dapply(pp->pool_methods->ctlfn,n+3,argbuf);
      u8_free(argbuf);
      return result;}}
}

static void recycle_procpool(fd_pool p)
{
  struct FD_PROCPOOL *pp = (fd_procpool)p;
  fd_decref(pp->pool_state);
  fd_decref(pp->pool_methods->allocfn);
  fd_decref(pp->pool_methods->fetchfn);
  fd_decref(pp->pool_methods->fetchnfn);
  fd_decref(pp->pool_methods->lockfn);
  fd_decref(pp->pool_methods->releasefn);
  fd_decref(pp->pool_methods->getloadfn);
  fd_decref(pp->pool_methods->swapoutfn);
  fd_decref(pp->pool_methods->commitfn);
  fd_decref(pp->pool_methods->metadatafn);
  fd_decref(pp->pool_methods->createfn);
  fd_decref(pp->pool_methods->closefn);
  fd_decref(pp->pool_methods->ctlfn);
  u8_free(pp->pool_methods);
  pp->pool_methods = NULL;
}

static fd_pool open_procpool(u8_string source,fd_storage_flags flags,lispval opts)
{
  lispval pool_type = fd_getopt(opts,FDSYM_TYPE,FD_VOID);
  struct FD_POOL_TYPEINFO *typeinfo =
    (FD_STRINGP(pool_type)) ? 
    (fd_get_pool_typeinfo(FD_CSTRING(pool_type))) :
    (FD_SYMBOLP(pool_type)) ? 
    (fd_get_pool_typeinfo(FD_SYMBOL_NAME(pool_type))) :
    (NULL);
  struct FD_PROCPOOL_METHODS *methods = 
    (struct FD_PROCPOOL_METHODS *) (typeinfo->type_data);
  lispval source_arg = lispval_string(source);
  lispval args[] = { source_arg, opts };
  lispval lp = fd_apply(methods->openfn,2,args);
  fd_decref(args[0]);
  if (FD_POOLP(lp))
    return fd_lisp2pool(lp);
  return NULL;
}

static fd_pool procpool_create(u8_string spec,void *type_data,
                               fd_storage_flags storage_flags,
                               lispval opts)
{
  lispval spec_arg = lispval_string(spec);
  struct FD_PROCPOOL_METHODS *methods =
    (struct FD_PROCPOOL_METHODS *) type_data;
  lispval args[] = { spec_arg, opts };
  lispval result = fd_apply(methods->createfn,2,args);
  if (FD_VOIDP(result)) {
    fd_pool opened = open_procpool(spec,storage_flags,opts);
    fd_decref(args[0]);
    return opened;}
  else fd_decref(args[0]);
  if (FD_ABORTP(result))
    return NULL;
  else if ( (FD_POOLP(result)) || (FD_TYPEP(result,fd_consed_pool_type)) )
    return fd_lisp2pool(result);
  else {
    fd_seterr("NotAPool","procpool_create",spec,result);
    return NULL;}
}

FD_EXPORT void fd_register_procpool(u8_string typename,lispval handlers)
{
  lispval typesym = fd_symbolize(typename);
  struct FD_PROCPOOL_METHODS *methods = u8_alloc(struct FD_PROCPOOL_METHODS);

  memset(methods,0,sizeof(struct FD_PROCPOOL_METHODS));
  methods->openfn = poolopt(handlers,"OPEN");
  methods->allocfn = poolopt(handlers,"ALLOC");
  methods->getloadfn = poolopt(handlers,"GETLOAD");
  methods->fetchfn = poolopt(handlers,"FETCH");
  methods->fetchnfn = poolopt(handlers,"FETCHN");
  methods->lockfn = poolopt(handlers,"LOCKOIDS");
  methods->releasefn = poolopt(handlers,"RELEASEOIDS");
  methods->commitfn = poolopt(handlers,"COMMIT");
  methods->metadatafn = poolopt(handlers,"METADATA");
  methods->createfn = poolopt(handlers,"CREATE");
  methods->closefn = poolopt(handlers,"CLOSE");
  methods->ctlfn = poolopt(handlers,"POOLCTL");

  fd_register_pool_type(FD_SYMBOL_NAME(typesym),
                        &fd_procpool_handler,
                        open_procpool,
                        NULL,
                        methods);
}

/* The default procpool handler */

struct FD_POOL_HANDLER fd_procpool_handler={
  "procpool", 1, sizeof(struct FD_PROCPOOL), 12,
  procpool_close, /* close */
  procpool_alloc, /* alloc */
  procpool_fetch, /* fetch */
  procpool_fetchn, /* fetchn */
  procpool_getload, /* getload */
  procpool_lock, /* lock */
  procpool_release, /* release */
  procpool_commit, /* commit */
  procpool_swapout, /* swapout */
  procpool_create, /* create */
  NULL,  /* walk */
  recycle_procpool, /* recycle */
  procpool_ctl  /* poolctl */
};

FD_EXPORT void fd_init_procpool_c()
{
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
