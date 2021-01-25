/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_BUFIO 1
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/apply.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

static u8_condition OddResult = _("ProcIndex/OddResult");

/* Fetch pools */

/* Fetch pools are pools which keep their data externally and take care
   of their own data management.  They basically have a fetch function and
   a load function but may be extended to be clever about saving in some way. */

static lispval indexopt(lispval opts,u8_string name)
{
  return kno_getopt(opts,kno_getsym(name),VOID);
}

KNO_EXPORT
kno_index kno_make_procindex(lispval opts,lispval state,
                           u8_string id,
                           u8_string source,
                           u8_string typeid)
{
  struct KNO_PROCINDEX_METHODS *methods;
  lispval index_type = kno_getopt(opts,KNOSYM_TYPE,KNO_VOID);
  lispval metadata = kno_getopt(opts,KNOSYM_METADATA,KNO_VOID);
  struct KNO_INDEX_TYPEINFO *typeinfo =
    (KNO_STRINGP(index_type)) ? 
    (kno_get_index_typeinfo(KNO_CSTRING(index_type))) :
    (KNO_SYMBOLP(index_type)) ? 
    (kno_get_index_typeinfo(KNO_SYMBOL_NAME(index_type))) :
    (NULL);

  if ( (typeinfo) && (typeinfo->type_data) )
    methods = (struct KNO_PROCINDEX_METHODS *) (typeinfo->type_data);
  else {
    methods = u8_alloc(struct KNO_PROCINDEX_METHODS);
    memset(methods,0,sizeof(struct KNO_PROCINDEX_METHODS));
    methods->fetchfn = indexopt(opts,"FETCH");
    methods->fetchsizefn = indexopt(opts,"FETCHSIZE");
    methods->fetchnfn = indexopt(opts,"FETCHN");
    methods->prefetchfn = indexopt(opts,"PREFETCH");
    methods->fetchkeysfn = indexopt(opts,"FETCHKEYS");
    methods->fetchinfofn = indexopt(opts,"FETCHINFO");
    methods->batchaddfn = indexopt(opts,"BATCHADD");
    methods->ctlfn = indexopt(opts,"INDEXCTL");
    methods->commitfn = indexopt(opts,"COMMIT");
    methods->closefn = indexopt(opts,"CLOSE");}
  
  struct KNO_PROCINDEX *pix = u8_alloc(struct KNO_PROCINDEX);
  unsigned int flags = KNO_STORAGE_ISINDEX | KNO_STORAGE_VIRTUAL;
  memset(pix,0,sizeof(struct KNO_PROCINDEX));
  lispval source_opt = KNO_VOID;

  int cache_level = -1;

  if (source == NULL) {
    source_opt = kno_getopt(opts,KNOSYM_SOURCE,KNO_VOID);
    if (KNO_STRINGP(source_opt))
      source = CSTRING(source_opt);}

  if (kno_testopt(opts,KNOSYM_CACHELEVEL,KNO_VOID)) {
    lispval v = kno_getopt(opts,KNOSYM_CACHELEVEL,KNO_VOID);
    if (KNO_FALSEP(v))
      cache_level = 0;
    else if (KNO_FIXNUMP(v)) {
      int ival=KNO_FIX2INT(v);
      cache_level=ival;}
    else if ( (KNO_TRUEP(v)) || (v == KNO_DEFAULT_VALUE) ) {}
    else u8_logf(LOG_CRIT,"BadCacheLevel",
                 "Invalid cache level %q specified for procindex %s",
                 v,id);
    kno_decref(v);}

  if (kno_testopt(opts,kno_intern("readonly"),KNO_VOID))
    flags |= KNO_STORAGE_READ_ONLY;
  if (!(kno_testopt(opts,kno_intern("register"),KNO_VOID)))
    flags |= KNO_STORAGE_UNREGISTERED;
  if (kno_testopt(opts,kno_intern("background"),KNO_VOID)) {
    flags |= KNO_INDEX_IN_BACKGROUND;
    flags &= ~KNO_STORAGE_UNREGISTERED;}

  kno_init_index((kno_index)pix,
                &kno_procindex_handler,
                id,source,source,
                flags,
                metadata,
                opts);

  lispval init_metadata = kno_getopt(opts,KNOSYM_METADATA,KNO_VOID);
  if (KNO_SLOTMAPP(init_metadata)) {
    struct KNO_SLOTMAP *index_metadata = &(pix->index_metadata);
    kno_copy_slotmap((kno_slotmap)init_metadata,index_metadata);}
  kno_decref(init_metadata);

  pix->index_opts = kno_getopt(opts,kno_intern("opts"),KNO_FALSE);
  pix->index_cache_level = cache_level;
  pix->index_methods     = methods;

  kno_register_index((kno_index)pix);
  pix->index_state = state; kno_incref(state);
  pix->index_typeid = u8_strdup(typeid);

  kno_decref(source_opt);

  return (kno_index)pix;
}

static lispval procindex_fetch(kno_index ix,lispval key)
{
  struct KNO_PROCINDEX *pix = (kno_procindex)ix;
  lispval lp = kno_index2lisp(ix);
  lispval args[]={lp,pix->index_state,key};
  if (VOIDP(pix->index_methods->fetchfn))
    return KNO_EMPTY;
  else return kno_dapply(pix->index_methods->fetchfn,3,args);
}

static int procindex_fetchsize(kno_index ix,lispval key)
{
  struct KNO_PROCINDEX *pix = (kno_procindex)ix;
  lispval lp = kno_index2lisp(ix);
  lispval args[]={lp,pix->index_state,key};
  if (VOIDP(pix->index_methods->fetchsizefn)) {
    lispval v = kno_dapply(pix->index_methods->fetchfn,3,args);
    if (KNO_ABORTED(v))
      return -1;
    int size = KNO_CHOICE_SIZE(v);
    kno_decref(v);
    return size;}
  else {
    lispval size_value = kno_dapply(pix->index_methods->fetchsizefn,3,args);
    if (KNO_ABORTED(size_value))
      return -1;
    else if ( (KNO_FIXNUMP(size_value)) && (KNO_FIX2INT(size_value)>=0) ) {
      int ival = KNO_FIX2INT(size_value);
      kno_decref(size_value);
      return ival;}
    else {
      kno_seterr(OddResult,"procpool_fetchsize",
                ix->indexid,size_value);
      kno_decref(size_value);
      return -1;}}
}

static lispval *procindex_fetchn(kno_index ix,int n,const lispval *keys)
{
  struct KNO_PROCINDEX *pix = (kno_procindex)ix;
  lispval lp = kno_index2lisp(ix);
  if (VOIDP(pix->index_methods->fetchnfn)) {
    lispval *vals = u8_big_alloc_n(n,lispval);
    int i = 0; while (i<n) {
      lispval key = keys[i];
      lispval v = procindex_fetch(ix,key);
      if (KNO_ABORTED(v)) {
        kno_decref_vec(vals,i);
        u8_big_free(vals);
        return NULL;}
      else vals[i++]=v;}
    return vals;}
  else {
    struct KNO_VECTOR vec={0};
    KNO_INIT_STATIC_CONS(&vec,kno_vector_type);
    vec.vec_free_elts=0;
    vec.vec_length=n;
    vec.vec_elts= (lispval *) keys;
    lispval keyvec = (lispval) &vec;
    lispval args[] = {lp,pix->index_state,keyvec};
    lispval result = kno_dapply(pix->index_methods->fetchnfn,3,args);
    if (KNO_ABORTED(result))
      return NULL;
    else if (VECTORP(result)) {
      lispval *vals = u8_big_alloc_n(n,lispval);
      int i = 0; while (i<n) {
        lispval val = VEC_REF(result,i);
        KNO_VECTOR_SET(result,i,VOID);
        vals[i++]=val;}
      kno_decref(result);
      return vals;}
    else {
      kno_seterr(OddResult,"procpool_fetchn",ix->indexid,result);
      kno_decref(result);
      return NULL;}}
}

static lispval *procindex_fetchkeys(kno_index ix,int *n_keys)
{
  struct KNO_PROCINDEX *pix = (kno_procindex)ix;
  lispval lp = kno_index2lisp(ix);
  if (VOIDP(pix->index_methods->fetchnfn)) {
    *n_keys=-1;
    return NULL;}
  else {
    lispval args[]={lp,pix->index_state};
    lispval result = kno_dapply(pix->index_methods->fetchkeysfn,2,args);
    if (KNO_ABORTED(result)) {
      *n_keys = -1;
      return NULL;}
    if (KNO_PRECHOICEP(result))
      result=kno_simplify_choice(result);
    if (VECTORP(result)) {
      int n = KNO_VECTOR_LENGTH(result);
      lispval *vals = u8_big_alloc_n(n,lispval);
      int i = 0; while (i<n) {
        lispval val = VEC_REF(result,i);
        KNO_VECTOR_SET(result,i,VOID);
        vals[i++]=val;}
      kno_decref(result);
      *n_keys=n;
      return vals;}
    else if (KNO_CHOICEP(result)) {
      int i=0, n = KNO_CHOICE_SIZE(result);
      lispval *vals = u8_big_alloc_n(n,lispval);
      KNO_DO_CHOICES(elt,result) {
        vals[i++]=elt; kno_incref(elt);}
      *n_keys=n;
      return vals;}
    else if (KNO_EMPTYP(result)) {
      *n_keys = 0;
      return NULL;}
    else {
      lispval *vals = u8_big_alloc_n(1,lispval);
      vals[0] = result;
      *n_keys=1;
      return vals;}}
}

static int copy_keyinfo(lispval info,struct KNO_KEY_SIZE *keyinfo)
{
  if ( (KNO_PAIRP(info)) && (KNO_INTEGERP(KNO_CDR(info))) ) {
    int retval = 0;
    lispval key = KNO_CAR(info);
    lispval count = KNO_CDR(info);
    long long icount = kno_getint(count);
    if (icount>=0) {
      keyinfo->keysize_key   = key;
      keyinfo->keysize_count = icount;
      retval=1;}
    if (KNO_REFCOUNT(info) > 1) {
      kno_incref(key);}
    else {
      KNO_SETCAR(info,KNO_VOID);
      KNO_SETCDR(info,KNO_VOID);
      kno_decref(count);}
    kno_decref(info);
    return retval;}
  else return 0;
}

static
struct KNO_KEY_SIZE *procindex_fetchinfo
(kno_index ix,kno_choice filter,int *n_ptr)
{
  struct KNO_PROCINDEX *pix = (kno_procindex)ix;
  lispval lp = kno_index2lisp(ix);
  if (VOIDP(pix->index_methods->fetchinfofn)) {
    *n_ptr = -1;
    return NULL;}
  else {
    int key_count = 0;
    lispval args[]={lp,pix->index_state};
    lispval result = kno_dapply(pix->index_methods->fetchinfofn,2,args);
    if (KNO_ABORTED(result)) {
      *n_ptr = -1;
      return NULL;}
    else if (VECTORP(result)) {
      int n = KNO_VECTOR_LENGTH(result);
      struct KNO_KEY_SIZE *info = u8_big_alloc_n(n,struct KNO_KEY_SIZE);
      int i = 0; while (i<n) {
        lispval keysize_pair = VEC_REF(result,i);
        KNO_VECTOR_SET(result,i,VOID);
        if (copy_keyinfo(keysize_pair,&(info[key_count]))) key_count++;
        i++;}
      kno_decref(result);
      *n_ptr = key_count;
      return info;}
    else {
      kno_seterr(OddResult,"procpool_fetchn",ix->indexid,result);
      kno_decref(result);
      *n_ptr = -1;
      return NULL;}}
}


static int procindex_commit(kno_index ix,kno_commit_phase phase,
                            struct KNO_INDEX_COMMITS *commits)
{
  struct KNO_PROCINDEX *pix = (kno_procindex)ix;
  lispval lx = kno_index2lisp(ix);
  if (VOIDP(pix->index_methods->commitfn))
    return 0;
  else {
    struct KNO_SLOTMAP add_table = {0}, drop_table = {0}, store_table = {0};
    kno_init_slotmap(&add_table,commits->commit_n_adds,
                    (kno_keyval)(commits->commit_adds));
    kno_init_slotmap(&drop_table,commits->commit_n_drops,
                    (kno_keyval)(commits->commit_drops));
    kno_init_slotmap(&store_table,commits->commit_n_stores,
                    (kno_keyval)(commits->commit_stores));
    KNO_XTABLE_SET_READONLY(&add_table,1);
    KNO_XTABLE_SET_READONLY(&drop_table,1);
    KNO_XTABLE_SET_READONLY(&store_table,1);
    lispval args[7]={lx,
                     pix->index_state,
                     kno_commit_phases[phase],
                     (lispval)(&add_table),
                     (lispval)(&drop_table),
                     (lispval)(&store_table),
                     ( (KNO_VOIDP(commits->commit_metadata)) ? 
                       (KNO_FALSE) :
                       (commits->commit_metadata) )};
    lispval result = kno_dapply(pix->index_methods->commitfn,7,args);
    if (KNO_ABORTED(result))
      return -1;
    else {
      if (KNO_FIXNUMP(result)) {
        int ival = KNO_FIX2INT(result);
        kno_decref(result);
        return ival;}
      else if (KNO_FALSEP(result))
        return 0;
      else if (KNO_TRUEP(result))
        return 1;
      else {
        kno_seterr(OddResult,"procindex_commit",ix->indexid,result);
        kno_decref(result);
        return 1;}}}
}

static void procindex_close(kno_index ix)
{
  struct KNO_PROCINDEX *pix = (kno_procindex)ix;
  lispval lp = kno_index2lisp(ix);
  if (VOIDP(pix->index_methods->closefn))
    return;
  else {
    lispval args[2]={lp,pix->index_state};
    lispval result = kno_dapply(pix->index_methods->closefn,2,args);
    kno_decref(result);
    return;}
}

static lispval procindex_ctl(kno_index ix,lispval opid,int n,kno_argvec args)
{
  struct KNO_PROCINDEX *pix = (kno_procindex)ix;
  lispval lx = kno_index2lisp(ix);
  if (VOIDP(pix->index_methods->ctlfn))
    return kno_default_indexctl(ix,opid,n,args);
  else {
    lispval argbuf[n+3];
    argbuf[0]=lx; argbuf[1]=pix->index_state;
    argbuf[2]=opid;
    memcpy(argbuf+3,args,LISPVEC_BYTELEN(n));
    lispval result = kno_dapply(pix->index_methods->ctlfn,n+3,argbuf);
    if (result == KNO_DEFAULT_VALUE)
      return kno_default_indexctl(ix,opid,n,args);
    else return result;}
}

static void recycle_procindex(kno_index ix)
{
  struct KNO_PROCINDEX *pix = (kno_procindex)ix;
  kno_decref(pix->index_methods->fetchfn);
  kno_decref(pix->index_methods->fetchsizefn);
  kno_decref(pix->index_methods->fetchnfn);
  kno_decref(pix->index_methods->prefetchfn);
  kno_decref(pix->index_methods->fetchkeysfn);
  kno_decref(pix->index_methods->fetchinfofn);
  kno_decref(pix->index_methods->batchaddfn);
  kno_decref(pix->index_methods->ctlfn);
  kno_decref(pix->index_methods->commitfn);
  kno_decref(pix->index_methods->closefn);
  kno_decref(pix->index_state);
}

/* Opening procindexs */

static kno_index open_procindex(u8_string source,kno_storage_flags flags,lispval opts)
{
  lispval index_type = kno_getopt(opts,KNOSYM_TYPE,KNO_VOID);
  struct KNO_INDEX_TYPEINFO *typeinfo =
    (KNO_STRINGP(index_type)) ? 
    (kno_get_index_typeinfo(KNO_CSTRING(index_type))) :
    (KNO_SYMBOLP(index_type)) ? 
    (kno_get_index_typeinfo(KNO_SYMBOL_NAME(index_type))) :
    (NULL);
  struct KNO_PROCINDEX_METHODS *methods = 
    (struct KNO_PROCINDEX_METHODS *) (typeinfo->type_data);
  lispval source_arg = kno_mkstring(source);
  lispval args[] = { source_arg, opts };
  lispval lp = kno_apply(methods->openfn,2,args);
  kno_decref(args[0]);
  if (KNO_ABORTED(lp))
    return NULL;
  else if (KNO_INDEXP(lp))
    return kno_lisp2index(lp);
  else return NULL;
}

static kno_index procindex_create(u8_string spec,void *type_data,
                                 kno_storage_flags storage_flags,
                                 lispval opts)
{
  lispval spec_arg = kno_mkstring(spec);
  struct KNO_PROCINDEX_METHODS *methods =
    (struct KNO_PROCINDEX_METHODS *) type_data;
  lispval args[] = { spec_arg, opts };
  lispval result = kno_apply(methods->createfn,2,args);
  kno_decref(spec_arg);
  if (KNO_ABORTED(result))
    return NULL;
  else if (KNO_VOIDP(result))
    return open_procindex(spec,storage_flags,opts);
  else if (KNO_INDEXP(result))
    return kno_lisp2index(result);
  else {
    kno_seterr("NotAnIndex","procindex_create",spec,result);
    return NULL;}
}

KNO_EXPORT void kno_register_procindex(u8_string typename,lispval handlers)
{
  lispval typesym = kno_getsym(typename);
  struct KNO_PROCINDEX_METHODS *methods = u8_alloc(struct KNO_PROCINDEX_METHODS);

  memset(methods,0,sizeof(struct KNO_PROCINDEX_METHODS));

  methods->openfn = indexopt(handlers,"OPEN");
  methods->createfn = indexopt(handlers,"CREATE");
  methods->fetchfn = indexopt(handlers,"FETCH");
  methods->fetchsizefn = indexopt(handlers,"FETCHSIZE");
  methods->fetchnfn = indexopt(handlers,"FETCHN");
  methods->prefetchfn = indexopt(handlers,"PREFETCH");
  methods->fetchkeysfn = indexopt(handlers,"FETCHKEYS");
  methods->fetchinfofn = indexopt(handlers,"FETCHINFO");
  methods->batchaddfn = indexopt(handlers,"BATCHADD");
  methods->ctlfn = indexopt(handlers,"INDEXCTL");
  methods->commitfn = indexopt(handlers,"COMMIT");
  methods->closefn = indexopt(handlers,"CLOSE");

  kno_register_index_type(KNO_SYMBOL_NAME(typesym),
                         &kno_procindex_handler,
                         open_procindex,
                         NULL,
                         methods);
}

/* The default procindex handler */

struct KNO_INDEX_HANDLER kno_procindex_handler={
  "procindex", 1, sizeof(struct KNO_PROCINDEX), 14,
  procindex_close, /* close */
  procindex_commit, /* commit */
  procindex_fetch, /* fetch */
  procindex_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  procindex_fetchn, /* fetchn */
  procindex_fetchkeys, /* fetchkeys */
  procindex_fetchinfo, /* fetchinfo */
  NULL, /* batchadd */
  procindex_create, /* create */
  NULL, /* walk */
  recycle_procindex, /* recycle */
  procindex_ctl /* indexctl */
};

KNO_EXPORT int kno_procindexp(kno_index ix)
{
  return (ix->index_handler == &kno_procindex_handler);
}

KNO_EXPORT void kno_init_procindex_c()
{
  u8_register_source_file(_FILEINFO);
}

