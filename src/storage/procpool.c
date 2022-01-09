/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_POOLS 1
#define KNO_INLINE_BUFIO 1
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/apply.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

#include "libu8/u8printf.h"

static u8_condition OddResult = _("ProcPool/OddResult");
static u8_condition CommitFailed = _("ProcPoolCommit Failed");

/* Fetch pools */

/* Fetch pools are pools which keep their data externally and take care
   of their own data management.  They basically have a fetch function and
   a load function but may be extended to be clever about saving in some way. */

static lispval poolopt(lispval opts,u8_string name)
{
  return kno_getopt(opts,kno_getsym(name),VOID);
}

#define NO_METHODP(x) ( (KNO_NULLP(x)) || (KNO_VOIDP(x)) )

KNO_EXPORT
kno_pool kno_make_procpool(KNO_OID base,
                         unsigned int cap,unsigned int load,
                         lispval opts,lispval state,
                         u8_string label,
                         u8_string source)
{
  if (load>cap) {
    u8_seterr(kno_PoolOverflow,"kno_make_procpool",
              u8_sprintf(NULL,256,
                         "Specified load (%u) > capacity (%u) for '%s'",
                         load,cap,(source)?(source):(label)));
    return NULL;}

  struct KNO_PROCPOOL *pp = u8_alloc(struct KNO_PROCPOOL);
  struct KNO_PROCPOOL_METHODS *methods;
  unsigned int flags = KNO_STORAGE_ISPOOL;
  lispval pool_type = kno_getopt(opts,KNOSYM_TYPE,KNO_VOID);
  lispval metadata = kno_getopt(opts,KNOSYM_METADATA,KNO_VOID);
  struct KNO_POOL_TYPEINFO *typeinfo =
    (KNO_STRINGP(pool_type)) ?
    (kno_get_pool_typeinfo(KNO_CSTRING(pool_type))) :
    (KNO_SYMBOLP(pool_type)) ? 
    (kno_get_pool_typeinfo(KNO_SYMBOL_NAME(pool_type))) :
    (NULL);
  int cache_level = -1;

  if ( (typeinfo) && (typeinfo->type_data) )
    methods = (struct KNO_PROCPOOL_METHODS *) (typeinfo->type_data);
  else {
    methods = u8_alloc(struct KNO_PROCPOOL_METHODS);
    memset(methods,0,sizeof(struct KNO_PROCPOOL_METHODS));
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

  memset(pp,0,sizeof(struct KNO_PROCPOOL));
  lispval source_opt = KNO_VOID;

  if (source == NULL) {
    source_opt = kno_getopt(opts,KNOSYM_SOURCE,KNO_VOID);
    if (KNO_STRINGP(source_opt))
      source = CSTRING(source_opt);}

  if (kno_testopt(opts,KNOSYM_CACHELEVEL,KNO_VOID)) {
    lispval v = kno_getopt(opts,KNOSYM_CACHELEVEL,KNO_VOID);
    if (KNO_FALSEP(v))
      cache_level=0;
    else if (KNO_FIXNUMP(v))
      cache_level=KNO_FIX2INT(v);
    else if ( (KNO_TRUEP(v)) || (v == KNO_DEFAULT_VALUE) ) {}
    else u8_logf(LOG_CRIT,"BadCacheLevel",
                 "Invalid cache level %q specified for procpool %s",
                 v,label);
    kno_decref(v);}

  flags |= KNO_POOL_SPARSE;
  if (kno_testopt(opts,kno_intern("adjunct"),KNO_VOID))
    flags |= KNO_POOL_ADJUNCT;
  if (kno_testopt(opts,kno_intern("readonly"),KNO_VOID))
    flags |= KNO_STORAGE_READ_ONLY;
  if (kno_testopt(opts,kno_intern("virtual"),KNO_VOID))
    flags |= KNO_STORAGE_VIRTUAL;
  lispval rval = kno_getopt(opts,kno_intern("register"),KNO_VOID);
  if (KNO_FALSEP(rval))
    flags |= KNO_STORAGE_UNREGISTERED;
  else kno_decref(rval);

  kno_init_pool((kno_pool)pp,base,cap,
               &kno_procpool_handler,
               label,source,source,
               flags,
               metadata,
               opts);
  pp->pool_methods = methods;
  pp->pool_state   = state; kno_incref(state);
  pp->pool_label = u8_strdup(label);
  pp->pool_cache_level = cache_level;

  if (kno_testopt(opts,kno_intern("typeid"),VOID)) {
    lispval idval = poolopt(opts,"TYPEID");
    if (STRINGP(idval))
      pp->pool_typeid = u8_strdup(CSTRING(idval));
    else if (SYMBOLP(idval))
      pp->pool_typeid = u8_strdup(KNO_SYMBOL_NAME(idval));
    else if (KNO_OIDP(idval)) {
      KNO_OID addr = KNO_OID_ADDR(idval);
      pp->pool_typeid =
        u8_mkstring("@%lx/%lx",KNO_OID_HI(addr),KNO_OID_LO(addr));}
    else u8_logf(LOG_WARN,"BadPoolTypeID","%q",idval);
    kno_decref(idval);}

  kno_register_pool((kno_pool)pp);
  kno_decref(metadata);
  kno_decref(source_opt);

  return (kno_pool)pp;
}

static lispval procpool_fetch(kno_pool p,lispval oid)
{
  struct KNO_PROCPOOL *pp = (kno_procpool)p;
  lispval lp = kno_pool2lisp(p);
  lispval args[]={lp,pp->pool_state,oid};
  if (NO_METHODP(pp->pool_methods->fetchfn))
    return VOID;
  else return kno_dapply(pp->pool_methods->fetchfn,3,args);
}

static lispval *procpool_fetchn(kno_pool p,int n,lispval *oids)
{
  struct KNO_PROCPOOL *pp = (kno_procpool)p;
  lispval lp = kno_pool2lisp(p);
  if (NO_METHODP(pp->pool_methods->fetchnfn)) {
    lispval *vals = u8_big_alloc_n(n,lispval);
    int i = 0; while (i<n) {
      lispval oid = oids[i];
      lispval v = procpool_fetch(p,oid);
      if (KNO_ABORTED(v)) {
        kno_decref_elts(vals,i);
        u8_big_free(vals);
        return NULL;}
      else vals[i++]=v;}
    return vals;}
  else {
    lispval oidvec = kno_make_vector(n,oids);
    lispval args[]={lp,pp->pool_state,oidvec};
    lispval result = kno_dapply(pp->pool_methods->fetchnfn,3,args);
    kno_decref(oidvec);
    if (KNO_ABORTED(result)) {
      kno_decref(oidvec);
      return NULL;}
    else if (VECTORP(result)) {
      lispval *vals = u8_big_alloc_n(n,lispval);
      int i = 0; while (i<n) {
        lispval val = VEC_REF(result,i);
        KNO_VECTOR_SET(result,i,VOID);
        vals[i++]=val;}
      kno_decref(result);
      return vals;}
    else {
      kno_seterr(OddResult,"procpool_fetchn",p->poolid,result);
      kno_decref(result);
      return NULL;}}
}

static int procpool_lock(kno_pool p,lispval oid)
{
  struct KNO_PROCPOOL *pp = (kno_procpool)p;
  lispval lp = kno_pool2lisp(p);
  if (NO_METHODP(pp->pool_methods->lockfn))
    return 0;
  else {
    lispval args[]={lp,pp->pool_state,oid};
    lispval result = kno_dapply(pp->pool_methods->lockfn,3,args);
    if (KNO_ABORTED(result)) return result;
    else if (FIXNUMP(result)) return result;
    else if (KNO_TRUEP(result)) return 1;
    else if (FALSEP(result)) return 0;
    else if (EMPTYP(result)) return 0;
    else {
      kno_seterr(OddResult,"procpool_lock",p->poolid,result);
      kno_decref(result);
      return -1;}}
}

static int procpool_swapout(kno_pool p,lispval oid)
{
  struct KNO_PROCPOOL *pp = (kno_procpool)p;
  lispval lp = kno_pool2lisp(p);
  if (NO_METHODP(pp->pool_methods->swapoutfn))
    return 1;
  else {
    lispval args[]={lp,pp->pool_state,oid};
    lispval result = kno_dapply(pp->pool_methods->swapoutfn,3,args);
    if (FIXNUMP(result)) return result;
    else if (KNO_TRUEP(result)) return 1;
    else if (FALSEP(result)) return 0;
    else if (EMPTYP(result)) return 0;
    else {
      kno_seterr(OddResult,"procpool_swapout",p->poolid,result);
      kno_decref(result);
      return -1;}}
}

static lispval procpool_alloc(kno_pool p,int n)
{
  struct KNO_PROCPOOL *pp = (kno_procpool)p;
  lispval lp = kno_pool2lisp(p);
  lispval n_arg = KNO_INT(n);
  lispval args[3]={lp,pp->pool_state,n_arg};
  if (NO_METHODP(pp->pool_methods->allocfn))
    return VOID;
  else return kno_dapply(pp->pool_methods->allocfn,3,args);
}

static int procpool_release(kno_pool p,lispval oid)
{
  struct KNO_PROCPOOL *pp = (kno_procpool)p;
  lispval lp = kno_pool2lisp(p);
  if (NO_METHODP(pp->pool_methods->releasefn))
    return 0;
  else {
    lispval args[3]={lp,pp->pool_state,oid};
    lispval result = kno_dapply(pp->pool_methods->releasefn,3,args);
    if (FIXNUMP(result)) return kno_getint(result);
    else if (KNO_TRUEP(result)) return 1;
    else if (FALSEP(result)) return 0;
    else if (KNO_ABORTED(result)) return -1;
    else {
      kno_seterr(OddResult,"procpool_release",p->poolid,result);
      kno_decref(result);
      return -1;}}
}

static int procpool_commit(kno_pool p,kno_commit_phase phase,
                           struct KNO_POOL_COMMITS *commits)
{
  struct KNO_PROCPOOL *pp = (kno_procpool)p;
  if (NO_METHODP(pp->pool_methods->commitfn))
    return 0;
  else {
    lispval lp = kno_pool2lisp(p);
    lispval phase_sym = kno_commit_phases[phase];
    u8_context phase_name = (KNO_SYMBOLP(phase_sym)) ?
      (KNO_SYMBOL_NAME(phase_sym)) : U8S("badphase");
    struct KNO_VECTOR oidvec = { 0 }, valvec = { 0 };
    KNO_INIT_STATIC_CONS(&oidvec,kno_vector_type);
    KNO_INIT_STATIC_CONS(&valvec,kno_vector_type);
    oidvec.vec_length = commits->commit_count;
    oidvec.vec_elts = commits->commit_oids;
    valvec.vec_length = commits->commit_count;
    valvec.vec_elts = commits->commit_vals;
    lispval args[6] = { lp, pp->pool_state, phase_sym,
                        ((lispval)(&oidvec)),
                        ((lispval)(&valvec)),
                        ( (KNO_VOIDP(commits->commit_metadata)) ?
                          (KNO_FALSE) :
                          (commits->commit_metadata) ) };
    lispval result = kno_apply(pp->pool_methods->commitfn,6,args);
    if (FIXNUMP(result))
      return KNO_INT(result);
    else if (FALSEP(result))
      return 0;
    else if (KNO_ABORTP(result)) {
      kno_seterr(CommitFailed,phase_name,p->poolid,KNO_VOID);
      return -1;}
    else if (KNO_TRUEP(result))
      return 1;
    else {
      u8_log(LOGCRIT,"ProcPoolCommit",
             "Returned bad value %q",result);
      kno_seterr(CommitFailed,phase_name,p->poolid,result);
      kno_decref(result);
      return -1;}}
}

static void procpool_close(kno_pool p)
{
  struct KNO_PROCPOOL *pp = (kno_procpool)p;
  lispval lp = kno_pool2lisp(p);
  if (NO_METHODP(pp->pool_methods->closefn))
    return;
  else {
    lispval args[2]={lp,pp->pool_state};
    lispval result = kno_dapply(pp->pool_methods->closefn,2,args);
    kno_decref(result);
    return;}
}

static int procpool_getload(kno_pool p)
{
  struct KNO_PROCPOOL *pp = (kno_procpool)p;
  lispval lp = kno_pool2lisp(p);
  if (NO_METHODP(pp->pool_methods->getloadfn))
    return 0;
  else {
    lispval args[2]={lp,pp->pool_state};
    lispval result = kno_dapply(pp->pool_methods->getloadfn,2,args);
    if (KNO_UINTP(result))
      return FIX2INT(result);
    else {
      kno_seterr(OddResult,"procpool_getload",p->poolid,result);
      kno_decref(result);
      return -1;}}
}

static lispval procpool_ctl(kno_pool p,lispval opid,int n,kno_argvec args)
{
  struct KNO_PROCPOOL *pp = (kno_procpool)p;
  lispval lp = kno_pool2lisp(p);
  if (NO_METHODP(pp->pool_methods->ctlfn))
    return kno_default_poolctl(p,opid,n,args);
  else {
    lispval _argbuf[32], *argbuf=_argbuf;
    if ((n+3) > 32) argbuf = u8_alloc_n(n+3,lispval);
    argbuf[0]=lp; argbuf[1]=pp->pool_state;
    argbuf[2]=opid;
    memcpy(argbuf+3,args,LISPVEC_BYTELEN(n));
    if (argbuf==_argbuf)
      return kno_dapply(pp->pool_methods->ctlfn,n+3,argbuf);
    else {
      lispval result = kno_dapply(pp->pool_methods->ctlfn,n+3,argbuf);
      u8_free(argbuf);
      return result;}}
}

static void recycle_procpool(kno_pool p)
{
  struct KNO_PROCPOOL *pp = (kno_procpool)p;
  kno_decref(pp->pool_state);
  kno_decref(pp->pool_methods->allocfn);
  kno_decref(pp->pool_methods->fetchfn);
  kno_decref(pp->pool_methods->fetchnfn);
  kno_decref(pp->pool_methods->lockfn);
  kno_decref(pp->pool_methods->releasefn);
  kno_decref(pp->pool_methods->getloadfn);
  kno_decref(pp->pool_methods->swapoutfn);
  kno_decref(pp->pool_methods->commitfn);
  kno_decref(pp->pool_methods->metadatafn);
  kno_decref(pp->pool_methods->createfn);
  kno_decref(pp->pool_methods->closefn);
  kno_decref(pp->pool_methods->ctlfn);
  u8_free(pp->pool_methods);
  pp->pool_methods = NULL;
}

static kno_pool open_procpool(u8_string source,kno_storage_flags flags,lispval opts)
{
  lispval pool_type = kno_getopt(opts,KNOSYM_TYPE,KNO_VOID);
  struct KNO_POOL_TYPEINFO *typeinfo =
    (KNO_STRINGP(pool_type)) ?
    (kno_get_pool_typeinfo(KNO_CSTRING(pool_type))) :
    (KNO_SYMBOLP(pool_type)) ?
    (kno_get_pool_typeinfo(KNO_SYMBOL_NAME(pool_type))) :
    (NULL);
  struct KNO_PROCPOOL_METHODS *methods =
    (struct KNO_PROCPOOL_METHODS *) (typeinfo->type_data);
  lispval source_arg = kno_mkstring(source);
  lispval args[] = { source_arg, opts };
  lispval lp = kno_apply(methods->openfn,2,args);
  kno_decref(args[0]);
  if (KNO_POOLP(lp))
    return kno_lisp2pool(lp);
  return NULL;
}

static kno_pool procpool_create(u8_string spec,void *type_data,
                               kno_storage_flags storage_flags,
                               lispval opts)
{
  lispval spec_arg = kno_mkstring(spec);
  struct KNO_PROCPOOL_METHODS *methods =
    (struct KNO_PROCPOOL_METHODS *) type_data;
  lispval args[] = { spec_arg, opts };
  lispval result = kno_apply(methods->createfn,2,args);
  if (KNO_VOIDP(result)) {
    kno_pool opened = open_procpool(spec,storage_flags,opts);
    kno_decref(args[0]);
    return opened;}
  else kno_decref(args[0]);
  if (KNO_ABORTED(result))
    return NULL;
  else if (KNO_POOLP(result))
    return kno_lisp2pool(result);
  else {
    kno_seterr("NotAPool","procpool_create",spec,result);
    return NULL;}
}

KNO_EXPORT void kno_register_procpool(u8_string typename,lispval handlers)
{
  lispval typesym = kno_getsym(typename);
  struct KNO_PROCPOOL_METHODS *methods = u8_alloc(struct KNO_PROCPOOL_METHODS);

  memset(methods,0,sizeof(struct KNO_PROCPOOL_METHODS));
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

  kno_register_pool_type(KNO_SYMBOL_NAME(typesym),
                        &kno_procpool_handler,
                        open_procpool,
                        NULL,
                        methods);
}

/* The default procpool handler */

struct KNO_POOL_HANDLER kno_procpool_handler={
  "procpool", 1, sizeof(struct KNO_PROCPOOL), 12, NULL,
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

KNO_EXPORT void kno_init_procpool_c()
{
  u8_register_source_file(_FILEINFO);
}

