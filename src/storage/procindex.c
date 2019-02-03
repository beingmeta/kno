/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/apply.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

static u8_condition OddResult = _("ProcIndex/OddResult");

/* Fetch pools */

/* Fetch pools are pools which keep their data externally and take care
   of their own data management.  They basically have a fetch function and
   a load function but may be extended to be clever about saving in some way. */

static lispval indexopt(lispval opts,u8_string name)
{
  return fd_getopt(opts,fd_intern(name),VOID);
}

FD_EXPORT
fd_index fd_make_procindex(lispval opts,lispval state,
                           u8_string id,
                           u8_string source,
                           u8_string typeid)
{
  struct FD_PROCINDEX_METHODS *methods;
  lispval index_type = fd_getopt(opts,FDSYM_TYPE,FD_VOID);
  lispval metadata = fd_getopt(opts,FDSYM_METADATA,FD_VOID);
  struct FD_INDEX_TYPEINFO *typeinfo =
    (FD_STRINGP(index_type)) ? 
    (fd_get_index_typeinfo(FD_CSTRING(index_type))) :
    (FD_SYMBOLP(index_type)) ? 
    (fd_get_index_typeinfo(FD_SYMBOL_NAME(index_type))) :
    (NULL);

  if ( (typeinfo) && (typeinfo->type_data) )
    methods = (struct FD_PROCINDEX_METHODS *) (typeinfo->type_data);
  else {
    methods = u8_alloc(struct FD_PROCINDEX_METHODS);
    memset(methods,0,sizeof(struct FD_PROCINDEX_METHODS));
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
  
  struct FD_PROCINDEX *pix = u8_alloc(struct FD_PROCINDEX);
  unsigned int flags = FD_STORAGE_ISINDEX | FD_STORAGE_VIRTUAL;
  memset(pix,0,sizeof(struct FD_PROCINDEX));
  lispval source_opt = FD_VOID;

  int cache_level = -1;

  if (source == NULL) {
    source_opt = fd_getopt(opts,FDSYM_SOURCE,FD_VOID);
    if (FD_STRINGP(source_opt))
      source = CSTRING(source_opt);}

  if (fd_testopt(opts,FDSYM_CACHELEVEL,FD_VOID)) {
    lispval v = fd_getopt(opts,FDSYM_CACHELEVEL,FD_VOID);
    if (FD_FALSEP(v))
      cache_level = 0;
    else if (FD_FIXNUMP(v)) {
      int ival=FD_FIX2INT(v);
      cache_level=ival;}
    else if ( (FD_TRUEP(v)) || (v == FD_DEFAULT_VALUE) ) {}
    else u8_logf(LOG_CRIT,"BadCacheLevel",
                 "Invalid cache level %q specified for procindex %s",
                 v,id);
    fd_decref(v);}

  if (fd_testopt(opts,fd_intern("READONLY"),FD_VOID))
    flags |= FD_STORAGE_READ_ONLY;
  if (!(fd_testopt(opts,fd_intern("REGISTER"),FD_VOID)))
    flags |= FD_STORAGE_UNREGISTERED;
  if (fd_testopt(opts,fd_intern("BACKGROUND"),FD_VOID)) {
    flags |= FD_INDEX_IN_BACKGROUND;
    flags &= ~FD_STORAGE_UNREGISTERED;}

  fd_init_index((fd_index)pix,
                &fd_procindex_handler,
                id,source,source,
                flags,
                metadata,
                opts);

  lispval init_metadata = fd_getopt(opts,FDSYM_METADATA,FD_VOID);
  if (FD_SLOTMAPP(init_metadata)) {
    struct FD_SLOTMAP *index_metadata = &(pix->index_metadata);
    fd_copy_slotmap((fd_slotmap)init_metadata,index_metadata);}
  fd_decref(init_metadata);

  pix->index_opts = fd_getopt(opts,fd_intern("OPTS"),FD_FALSE);
  pix->index_cache_level = cache_level;
  pix->index_methods     = methods;

  fd_register_index((fd_index)pix);
  pix->index_state = state; fd_incref(state);
  pix->index_typeid = u8_strdup(typeid);

  fd_decref(source_opt);

  return (fd_index)pix;
}

static lispval procindex_fetch(fd_index ix,lispval key)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lp = fd_index2lisp(ix);
  lispval args[]={lp,pix->index_state,key};
  if (VOIDP(pix->index_methods->fetchfn))
    return FD_EMPTY;
  else return fd_dapply(pix->index_methods->fetchfn,3,args);
}

static int procindex_fetchsize(fd_index ix,lispval key)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lp = fd_index2lisp(ix);
  lispval args[]={lp,pix->index_state,key};
  if (VOIDP(pix->index_methods->fetchsizefn)) {
    lispval v = fd_dapply(pix->index_methods->fetchfn,3,args);
    if (FD_ABORTED(v))
      return -1;
    int size = FD_CHOICE_SIZE(v);
    fd_decref(v);
    return size;}
  else {
    lispval size_value = fd_dapply(pix->index_methods->fetchsizefn,3,args);
    if (FD_ABORTED(size_value))
      return -1;
    else if ( (FD_FIXNUMP(size_value)) && (FD_FIX2INT(size_value)>=0) ) {
      int ival = FD_FIX2INT(size_value);
      fd_decref(size_value);
      return ival;}
    else {
      fd_seterr(OddResult,"procpool_fetchsize",
                ix->indexid,size_value);
      fd_decref(size_value);
      return -1;}}
}

static lispval *procindex_fetchn(fd_index ix,int n,const lispval *keys)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lp = fd_index2lisp(ix);
  if (VOIDP(pix->index_methods->fetchnfn)) {
    lispval *vals = u8_big_alloc_n(n,lispval);
    int i = 0; while (i<n) {
      lispval key = keys[i];
      lispval v = procindex_fetch(ix,key);
      if (FD_ABORTED(v)) {
        fd_decref_vec(vals,i);
        u8_big_free(vals);
        return NULL;}
      else vals[i++]=v;}
    return vals;}
  else {
    struct FD_VECTOR vec={0};
    FD_INIT_STATIC_CONS(&vec,fd_vector_type);
    vec.vec_free_elts=0;
    vec.vec_length=n;
    vec.vec_elts= (lispval *) keys;
    lispval keyvec = (lispval) &vec;
    lispval args[] = {lp,pix->index_state,keyvec};
    lispval result = fd_dapply(pix->index_methods->fetchnfn,3,args);
    if (FD_ABORTED(result))
      return NULL;
    else if (VECTORP(result)) {
      lispval *vals = u8_big_alloc_n(n,lispval);
      int i = 0; while (i<n) {
        lispval val = VEC_REF(result,i);
        FD_VECTOR_SET(result,i,VOID);
        vals[i++]=val;}
      fd_decref(result);
      return vals;}
    else {
      fd_seterr(OddResult,"procpool_fetchn",ix->indexid,result);
      fd_decref(result);
      return NULL;}}
}

static lispval *procindex_fetchkeys(fd_index ix,int *n_keys)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lp = fd_index2lisp(ix);
  if (VOIDP(pix->index_methods->fetchnfn)) {
    *n_keys=-1;
    return NULL;}
  else {
    lispval args[]={lp,pix->index_state};
    lispval result = fd_dapply(pix->index_methods->fetchkeysfn,2,args);
    if (FD_ABORTED(result)) {
      *n_keys = -1;
      return NULL;}
    if (FD_PRECHOICEP(result))
      result=fd_simplify_choice(result);
    if (VECTORP(result)) {
      int n = FD_VECTOR_LENGTH(result);
      lispval *vals = u8_big_alloc_n(n,lispval);
      int i = 0; while (i<n) {
        lispval val = VEC_REF(result,i);
        FD_VECTOR_SET(result,i,VOID);
        vals[i++]=val;}
      fd_decref(result);
      *n_keys=n;
      return vals;}
    else if (FD_CHOICEP(result)) {
      int i=0, n = FD_CHOICE_SIZE(result);
      lispval *vals = u8_big_alloc_n(n,lispval);
      FD_DO_CHOICES(elt,result) {
        vals[i++]=elt; fd_incref(elt);}
      *n_keys=n;
      return vals;}
    else if (FD_EMPTYP(result)) {
      *n_keys = 0;
      return NULL;}
    else {
      lispval *vals = u8_big_alloc_n(1,lispval);
      vals[0] = result;
      *n_keys=1;
      return vals;}}
}

static int copy_keyinfo(lispval info,struct FD_KEY_SIZE *keyinfo)
{
  if ( (FD_PAIRP(info)) && (FD_INTEGERP(FD_CDR(info))) ) {
    int retval = 0;
    lispval key = FD_CAR(info);
    lispval count = FD_CDR(info);
    long long icount = fd_getint(count);
    if (icount>=0) {
      keyinfo->keysize_key   = key;
      keyinfo->keysize_count = icount;
      retval=1;}
    if (FD_CONS_REFCOUNT(info) > 1) {
      fd_incref(key);}
    else {
      FD_SETCAR(info,FD_VOID);
      FD_SETCDR(info,FD_VOID);
      fd_decref(count);}
    fd_decref(info);
    return retval;}
  else return 0;
}

static
struct FD_KEY_SIZE *procindex_fetchinfo
(fd_index ix,fd_choice filter,int *n_ptr)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lp = fd_index2lisp(ix);
  if (VOIDP(pix->index_methods->fetchinfofn)) {
    *n_ptr = -1;
    return NULL;}
  else {
    int key_count = 0;
    lispval args[]={lp,pix->index_state};
    lispval result = fd_dapply(pix->index_methods->fetchinfofn,2,args);
    if (FD_ABORTED(result)) {
      *n_ptr = -1;
      return NULL;}
    else if (VECTORP(result)) {
      int n = FD_VECTOR_LENGTH(result);
      struct FD_KEY_SIZE *info = u8_big_alloc_n(n,struct FD_KEY_SIZE);
      int i = 0; while (i<n) {
        lispval keysize_pair = VEC_REF(result,i);
        FD_VECTOR_SET(result,i,VOID);
        if (copy_keyinfo(keysize_pair,&(info[key_count]))) key_count++;
        i++;}
      fd_decref(result);
      *n_ptr = key_count;
      return info;}
    else {
      fd_seterr(OddResult,"procpool_fetchn",ix->indexid,result);
      fd_decref(result);
      *n_ptr = -1;
      return NULL;}}
}


static int procindex_commit(fd_index ix,fd_commit_phase phase,
                            struct FD_INDEX_COMMITS *commits)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lx = fd_index2lisp(ix);
  if (VOIDP(pix->index_methods->commitfn))
    return 0;
  else {
    struct FD_SLOTMAP add_table = {0}, drop_table = {0}, store_table = {0};
    fd_init_slotmap(&add_table,commits->commit_n_adds,
                    (fd_keyval)(commits->commit_adds));
    fd_init_slotmap(&drop_table,commits->commit_n_drops,
                    (fd_keyval)(commits->commit_drops));
    fd_init_slotmap(&store_table,commits->commit_n_stores,
                    (fd_keyval)(commits->commit_stores));
    add_table.table_readonly=1;
    drop_table.table_readonly=1;
    store_table.table_readonly=1;
    lispval args[7]={lx,
                     pix->index_state,
                     fd_commit_phases[phase],
                     (lispval)(&add_table),
                     (lispval)(&drop_table),
                     (lispval)(&store_table),
                     ( (FD_VOIDP(commits->commit_metadata)) ? 
                       (FD_FALSE) :
                       (commits->commit_metadata) )};
    lispval result = fd_dapply(pix->index_methods->commitfn,7,args);
    if (FD_ABORTED(result))
      return -1;
    else {
      if (FD_FIXNUMP(result)) {
        int ival = FD_FIX2INT(result);
        fd_decref(result);
        return ival;}
      else if (FD_FALSEP(result))
        return 0;
      else if (FD_TRUEP(result))
        return 1;
      else {
        fd_seterr(OddResult,"procindex_commit",ix->indexid,result);
        fd_decref(result);
        return 1;}}}
}

static void procindex_close(fd_index ix)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lp = fd_index2lisp(ix);
  if (VOIDP(pix->index_methods->closefn))
    return;
  else {
    lispval args[2]={lp,pix->index_state};
    lispval result = fd_dapply(pix->index_methods->closefn,2,args);
    fd_decref(result);
    return;}
}

static lispval procindex_ctl(fd_index ix,lispval opid,int n,lispval *args)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lx = fd_index2lisp(ix);
  if (VOIDP(pix->index_methods->ctlfn))
    return fd_default_indexctl(ix,opid,n,args);
  else {
    lispval argbuf[n+3];
    argbuf[0]=lx; argbuf[1]=pix->index_state;
    argbuf[2]=opid;
    memcpy(argbuf+3,args,LISPVEC_BYTELEN(n));
    lispval result = fd_dapply(pix->index_methods->ctlfn,n+3,argbuf);
    if (result == FD_DEFAULT_VALUE)
      return fd_default_indexctl(ix,opid,n,args);
    else return result;}
}

static void recycle_procindex(fd_index ix)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  fd_decref(pix->index_methods->fetchfn);
  fd_decref(pix->index_methods->fetchsizefn);
  fd_decref(pix->index_methods->fetchnfn);
  fd_decref(pix->index_methods->prefetchfn);
  fd_decref(pix->index_methods->fetchkeysfn);
  fd_decref(pix->index_methods->fetchinfofn);
  fd_decref(pix->index_methods->batchaddfn);
  fd_decref(pix->index_methods->ctlfn);
  fd_decref(pix->index_methods->commitfn);
  fd_decref(pix->index_methods->closefn);
  fd_decref(pix->index_state);

  if (pix->index_typeid) u8_free(pix->index_typeid);
  if (pix->index_source) u8_free(pix->index_source);
  if (pix->indexid) u8_free(pix->indexid);
}

/* Opening procindexs */

static fd_index open_procindex(u8_string source,fd_storage_flags flags,lispval opts)
{
  lispval index_type = fd_getopt(opts,FDSYM_TYPE,FD_VOID);
  struct FD_INDEX_TYPEINFO *typeinfo =
    (FD_STRINGP(index_type)) ? 
    (fd_get_index_typeinfo(FD_CSTRING(index_type))) :
    (FD_SYMBOLP(index_type)) ? 
    (fd_get_index_typeinfo(FD_SYMBOL_NAME(index_type))) :
    (NULL);
  struct FD_PROCINDEX_METHODS *methods = 
    (struct FD_PROCINDEX_METHODS *) (typeinfo->type_data);
  lispval source_arg = lispval_string(source);
  lispval args[] = { source_arg, opts };
  lispval lp = fd_apply(methods->openfn,2,args);
  fd_decref(args[0]);
  if (FD_ABORTED(lp))
    return NULL;
  else if (FD_INDEXP(lp))
    return fd_lisp2index(lp);
  else return NULL;
}

static fd_index procindex_create(u8_string spec,void *type_data,
                                 fd_storage_flags storage_flags,
                                 lispval opts)
{
  lispval spec_arg = lispval_string(spec);
  struct FD_PROCINDEX_METHODS *methods =
    (struct FD_PROCINDEX_METHODS *) type_data;
  lispval args[] = { spec_arg, opts };
  lispval result = fd_apply(methods->createfn,2,args);
  fd_decref(spec_arg);
  if (FD_ABORTED(result))
    return NULL;
  else if (FD_VOIDP(result))
    return open_procindex(spec,storage_flags,opts);
  else if (FD_INDEXP(result))
    return fd_lisp2index(result);
  else {
    fd_seterr("NotAnIndex","procindex_create",spec,result);
    return NULL;}
}

FD_EXPORT void fd_register_procindex(u8_string typename,lispval handlers)
{
  lispval typesym = fd_symbolize(typename);
  struct FD_PROCINDEX_METHODS *methods = u8_alloc(struct FD_PROCINDEX_METHODS);

  memset(methods,0,sizeof(struct FD_PROCINDEX_METHODS));

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

  fd_register_index_type(FD_SYMBOL_NAME(typesym),
                         &fd_procindex_handler,
                         open_procindex,
                         NULL,
                         methods);
}

/* The default procindex handler */

struct FD_INDEX_HANDLER fd_procindex_handler={
  "procindex", 1, sizeof(struct FD_PROCINDEX), 14,
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

FD_EXPORT int fd_procindexp(fd_index ix)
{
  return (ix->index_handler == &fd_procindex_handler);
}

FD_EXPORT void fd_init_procindex_c()
{
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
