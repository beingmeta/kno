/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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
    else u8_logf(LOGCRIT,"BadCacheLevel",
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
                id,u8_strdup(source),flags);

  pix->index_opts = fd_getopt(opts,fd_intern("OPTS"),FD_FALSE);
  pix->index_cache_level = cache_level;

  fd_register_index((fd_index)pix);
  pix->fetchfn = indexopt(opts,"FETCH");
  pix->fetchsizefn = indexopt(opts,"FETCHSIZE");
  pix->fetchnfn = indexopt(opts,"FETCHN");
  pix->prefetchfn = indexopt(opts,"PREFETCH");
  pix->fetchkeysfn = indexopt(opts,"FETCHKEYS");
  pix->fetchinfofn = indexopt(opts,"FETCHINFO");
  pix->batchaddfn = indexopt(opts,"BATCHADD");
  pix->ctlfn = indexopt(opts,"CTL");
  pix->savefn = indexopt(opts,"SAVE");
  pix->closefn = indexopt(opts,"CLOSE");
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
  if (VOIDP(pix->fetchfn)) return VOID;
  else return fd_dapply(pix->fetchfn,3,args);
}

static int procindex_fetchsize(fd_index ix,lispval key)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lp = fd_index2lisp(ix);
  lispval args[]={lp,pix->index_state,key};
  if (VOIDP(pix->fetchsizefn)) {
    lispval v = fd_dapply(pix->fetchfn,3,args);
    if (FD_ABORTP(v))
      return -1;
    int size = FD_CHOICE_SIZE(v);
    fd_decref(v);
    return size;}
  else {
    lispval size_value = fd_dapply(pix->fetchsizefn,3,args);
    if (FD_ABORTP(size_value))
      return -1;
    else {
      int ival = FD_FIX2INT(size_value);
      fd_decref(size_value);
      return ival;}}
}

static lispval *procindex_fetchn(fd_index ix,int n,const lispval *keys)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lp = fd_index2lisp(ix);
  if (VOIDP(pix->fetchnfn)) {
    lispval *vals = u8_big_alloc_n(n,lispval);
    int i = 0; while (i<n) {
      lispval key = keys[i];
      lispval v = procindex_fetch(ix,key);
      if (FD_ABORTP(v)) {
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
    lispval args[]={lp,pix->index_state,keyvec};
    lispval result = fd_dapply(pix->fetchnfn,3,args);

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

static lispval *procindex_fetchkeys(fd_index ix,int *n_keys)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lp = fd_index2lisp(ix);
  if (VOIDP(pix->fetchnfn))
    return NULL;
  else {
    lispval args[]={lp,pix->index_state};
    lispval result = fd_dapply(pix->fetchkeysfn,2,args);
    if (FD_PRECHOICEP(result))
      result=fd_simplify_choice(result);

    if (VECTORP(result)) {
      int n = FD_VECTOR_LENGTH(result);
      lispval *vals = u8_alloc_n(n,lispval);
      int i = 0; while (i<n) {
        lispval val = VEC_REF(result,i);
        FD_VECTOR_SET(result,i,VOID);
        vals[i++]=val;}
      fd_decref(result);
      *n_keys=n;
      return vals;}
    else if (FD_CHOICEP(result)) {
      int i=0, n = FD_CHOICE_SIZE(result);
      lispval *vals = u8_alloc_n(n,lispval);
      FD_DO_CHOICES(elt,result) {
        vals[i++]=elt; fd_incref(elt);}
      *n_keys=n;
      return vals;}
    else {
      fd_decref(result);
      return NULL;}}
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
struct FD_KEY_SIZE *procindex_fetchinfo(fd_index ix,fd_choice filter,int *n_ptr)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lp = fd_index2lisp(ix);
  if (VOIDP(pix->fetchinfofn)) {
    *n_ptr = -1;
    return NULL;}
  else {
    int key_count = 0;
    lispval args[]={lp,pix->index_state};
    lispval result = fd_dapply(pix->fetchinfofn,2,args);
    if (VECTORP(result)) {
      int n = FD_VECTOR_LENGTH(result);
      struct FD_KEY_SIZE *info = u8_alloc_n(n,struct FD_KEY_SIZE);
      int i = 0; while (i<n) {
        lispval keysize_pair = VEC_REF(result,i);
        FD_VECTOR_SET(result,i,VOID);
        if (copy_keyinfo(keysize_pair,&(info[key_count]))) key_count++;
        i++;}
      fd_decref(result);
      *n_ptr = key_count;
      return info;}
    else {
      fd_decref(result);
      *n_ptr = 0;
      return NULL;}}
}


static int procindex_save(struct FD_INDEX *ix,
                          struct FD_CONST_KEYVAL *adds,int n_adds,
                          struct FD_CONST_KEYVAL *drops,int n_drops,
                          struct FD_CONST_KEYVAL *stores,int n_stores,
                          lispval changed_metadata)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lx = fd_index2lisp(ix);
  if (VOIDP(pix->savefn))
    return 0;
  else {
    struct FD_SLOTMAP add_table, drop_table, store_table;
    fd_init_slotmap(&add_table,n_adds,(fd_keyval)adds);
    fd_init_slotmap(&drop_table,n_drops,(fd_keyvals)drops);
    fd_init_slotmap(&store_table,n_stores,(fd_keyvals)stores);
    add_table.table_readonly=drop_table.table_readonly=1;
    store_table.table_readonly=1;
    lispval args[]={lx,
                    pix->index_state,
                    (lispval)(&add_table),
                    (lispval)(&drop_table),
                    (lispval)(&store_table),
                    ( (FD_VOIDP(changed_metadata)) ? (FD_FALSE) :
                      (changed_metadata) )};
    lispval result = fd_dapply(pix->savefn,5,args);
    if (FD_ABORTP(result))
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
        fd_decref(result);
        return 1;}}}
}

static int procindex_commit(fd_index ix,fd_commit_phase phase,
                            struct FD_INDEX_COMMITS *commit)
{
  switch (phase) {
  case fd_commit_save: {
    return procindex_save(ix,
                          (struct FD_CONST_KEYVAL *)commit->commit_adds,
                          commit->commit_n_adds,
                          (struct FD_CONST_KEYVAL *)commit->commit_drops,
                          commit->commit_n_drops,
                          (struct FD_CONST_KEYVAL *)commit->commit_stores,
                          commit->commit_n_stores,
                          commit->commit_metadata);}
  default: {
    u8_logf(LOG_INFO,"NoPhasedCommit",
            "The index %s doesn't support phased commits",
            ix->indexid);
    return 0;}
  }
}

static void procindex_close(fd_index ix)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lp = fd_index2lisp(ix);
  if (VOIDP(pix->closefn))
    return;
  else {
    lispval args[2]={lp,pix->index_state};
    lispval result = fd_dapply(pix->closefn,2,args);
    fd_decref(result);
    return;}
}

static lispval procindex_ctl(fd_index ix,lispval opid,int n,lispval *args)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  lispval lx = fd_index2lisp(ix);
  if (VOIDP(pix->ctlfn))
    return fd_default_indexctl(ix,opid,n,args);
  else {
    lispval argbuf[n+3];
    argbuf[0]=lx; argbuf[1]=pix->index_state;
    argbuf[2]=opid;
    memcpy(argbuf+3,args,sizeof(lispval)*n);
    lispval result = fd_dapply(pix->ctlfn,n+3,argbuf);
    if (result == FD_DEFAULT_VALUE)
      return fd_default_indexctl(ix,opid,n,args);
    else return result;}
}

static void recycle_procindex(fd_index ix)
{
  struct FD_PROCINDEX *pix = (fd_procindex)ix;
  fd_decref(pix->fetchfn);
  fd_decref(pix->fetchsizefn);
  fd_decref(pix->fetchnfn);
  fd_decref(pix->prefetchfn);
  fd_decref(pix->fetchkeysfn);
  fd_decref(pix->fetchinfofn);
  fd_decref(pix->batchaddfn);
  fd_decref(pix->ctlfn);
  fd_decref(pix->savefn);
  fd_decref(pix->closefn);
  fd_decref(pix->index_state);

  if (pix->index_typeid) u8_free(pix->index_typeid);
  if (pix->index_source) u8_free(pix->index_source);
  if (pix->indexid) u8_free(pix->indexid);
}

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
  NULL, /* create */
  NULL, /* walk */
  recycle_procindex, /* recycle */
  procindex_ctl /* indexctl */
};

FD_EXPORT void fd_init_procindex_c()
{
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
