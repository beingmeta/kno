/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_IPEVAL 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/apply.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"

#include <libu8/libu8.h>
#include <libu8/u8filefns.h>
#include <libu8/u8timefns.h>

/* Cache calls */

static struct FD_HASHTABLE fcn_caches;

static struct FD_HASHTABLE *get_fcn_cache(lispval fcn,int create)
{
  lispval cache = fd_hashtable_get(&fcn_caches,fcn,VOID);
  if (VOIDP(cache)) {
    cache = fd_make_hashtable(NULL,512);
    fd_hashtable_store(&fcn_caches,fcn,cache);
    return fd_consptr(struct FD_HASHTABLE *,cache,fd_hashtable_type);}
  else return fd_consptr(struct FD_HASHTABLE *,cache,fd_hashtable_type);
}

FD_EXPORT lispval fd_cachecall(lispval fcn,int n,lispval *args)
{
  lispval vec, cached;
  struct FD_HASHTABLE *cache = get_fcn_cache(fcn,1);
  struct FD_VECTOR vecstruct;
  memset(&vecstruct,0,sizeof(vecstruct));
  vecstruct.vec_length = n;
  vecstruct.vec_free_elts = 0;
  vecstruct.vec_elts = ((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec = LISP_CONS(&vecstruct);
  cached = fd_hashtable_get(cache,vec,VOID);
  if (VOIDP(cached)) {
    int state = fd_ipeval_status();
    lispval result = fd_finish_call(fd_dapply(fcn,n,args));
    if (FD_ABORTP(result)) {
      fd_decref((lispval)cache);
      return result;}
    else if (fd_ipeval_status() == state) {
      lispval *datavec = ((n) ? (u8_alloc_n(n,lispval)) : (NULL));
      lispval key = fd_wrap_vector(n,datavec);
      int i = 0; while (i<n) {
        datavec[i]=fd_incref(args[i]); i++;}
      fd_hashtable_store(cache,key,result);
      fd_decref(key);}
    fd_decref((lispval)cache);
    return result;}
  else {
    fd_decref((lispval)cache);
    return cached;}
}

FD_EXPORT lispval fd_xcachecall
(struct FD_HASHTABLE *cache,lispval fcn,int n,lispval *args)
{
  lispval vec, cached;
  struct FD_VECTOR vecstruct;
  memset(&vecstruct,0,sizeof(vecstruct));
  vecstruct.vec_length = n;
  vecstruct.vec_free_elts = 0;
  vecstruct.vec_elts = ((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec = LISP_CONS(&vecstruct);
  cached = fd_hashtable_get(cache,vec,VOID);
  if (VOIDP(cached)) {
    int state = fd_ipeval_status();
    lispval result = fd_finish_call(fd_dapply(fcn,n,args));
    if (FD_ABORTP(result)) {
      fd_decref((lispval)cache);
      return result;}
    else if (fd_ipeval_status() == state) {
      lispval *datavec = ((n) ? (u8_alloc_n(n,lispval)) : (NULL));
      lispval key = fd_wrap_vector(n,datavec);
      int i = 0; while (i<n) {
        datavec[i]=fd_incref(args[i]); i++;}
      fd_hashtable_store(cache,key,result);
      fd_decref(key);}
    return result;}
  else return cached;
}

FD_EXPORT void fd_clear_callcache(lispval arg)
{
  if (fcn_caches.table_n_keys==0) return;
  if (VOIDP(arg)) fd_reset_hashtable(&fcn_caches,128,1);
  else if ((VECTORP(arg)) && (VEC_LEN(arg)>0)) {
    lispval fcn = VEC_REF(arg,0);
    lispval table = fd_hashtable_get(&fcn_caches,fcn,EMPTY);
    if (EMPTYP(table)) return;
    /* This should probably reall signal an error. */
    else if (!(HASHTABLEP(table))) return;
    else {
      int i = 0, n_args = VEC_LEN(arg)-1;
      lispval *datavec = u8_alloc_n(n_args,lispval);
      lispval key = fd_wrap_vector(n_args,datavec);
      while (i<n_args) {
        datavec[i]=fd_incref(VEC_REF(arg,i+1)); i++;}
      fd_hashtable_store((fd_hashtable)table,key,VOID);
      fd_decref(key);}}
  else if (fd_hashtable_probe(&fcn_caches,arg))
    fd_hashtable_store(&fcn_caches,arg,VOID);
  else return;
}

static int hashtable_cachecount(lispval key,lispval v,void *ptr)
{
  if (HASHTABLEP(v)) {
    fd_hashtable h = (fd_hashtable)v;
    int *count = (int *)ptr;
    *count = *count+h->table_n_keys;}
  return 0;
}

FD_EXPORT long fd_callcache_load()
{
  int count = 0;
  fd_for_hashtable(&fcn_caches,hashtable_cachecount,&count,1);
  return count;
}

FD_EXPORT int fd_cachecall_probe(lispval fcn,int n,lispval *args)
{
  lispval vec; int iscached = 0;
  struct FD_HASHTABLE *cache = get_fcn_cache(fcn,1);
  struct FD_VECTOR vecstruct;
  memset(&vecstruct,0,sizeof(vecstruct));
  vecstruct.vec_length = n;
  vecstruct.vec_free_elts = 0;
  vecstruct.vec_elts = ((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec = LISP_CONS(&vecstruct);
  iscached = fd_hashtable_probe(cache,vec);
  fd_decref((lispval)cache);
  return iscached;
}

FD_EXPORT int fd_xcachecall_probe(struct FD_HASHTABLE *cache,lispval fcn,int n,lispval *args)
{
  lispval vec; int iscached = 0;
  struct FD_VECTOR vecstruct;
  memset(&vecstruct,0,sizeof(vecstruct));
  vecstruct.vec_length = n;
  vecstruct.vec_free_elts = 0;
  vecstruct.vec_elts = ((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec = LISP_CONS(&vecstruct);
  iscached = fd_hashtable_probe(cache,vec);
  return iscached;
}

FD_EXPORT lispval fd_cachecall_try(lispval fcn,int n,lispval *args)
{
  lispval vec; lispval value;
  struct FD_HASHTABLE *cache = get_fcn_cache(fcn,1);
  struct FD_VECTOR vecstruct;
  memset(&vecstruct,0,sizeof(vecstruct));
  vecstruct.vec_length = n;
  vecstruct.vec_free_elts = 0;
  vecstruct.vec_elts = ((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec = LISP_CONS(&vecstruct);
  value = fd_hashtable_get(cache,vec,VOID);
  fd_decref((lispval)cache);
  if (VOIDP(value)) return EMPTY;
  else return value;
}

FD_EXPORT lispval fd_xcachecall_try(struct FD_HASHTABLE *cache,lispval fcn,int n,lispval *args)
{
  lispval vec; lispval value;
  struct FD_VECTOR vecstruct;
  memset(&vecstruct,0,sizeof(vecstruct));
  vecstruct.vec_length = n;
  vecstruct.vec_free_elts = 0;
  vecstruct.vec_elts = ((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec = LISP_CONS(&vecstruct);
  value = fd_hashtable_get(cache,vec,VOID);
  if (VOIDP(value)) return EMPTY;
  else return value;
}

/* Thread cache calls */

#define TCACHECALL_STACK_ELTS 8

FD_EXPORT lispval fd_tcachecall(lispval fcn,int n,lispval *args)
{
  if (fd_threadcache) {
    fd_thread_cache tc = fd_threadcache;
    struct FD_VECTOR vecstruct;
    lispval _elts[TCACHECALL_STACK_ELTS], *elts = NULL, vec, cached;
    /* Initialize the stack vector */
    memset(&vecstruct,0,sizeof(vecstruct));
    vecstruct.vec_free_elts = 0;
    vecstruct.vec_length = n+1;
    FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
    /* Allocate an elements vector if neccessary */
    if ((n+1)>(TCACHECALL_STACK_ELTS))
      elts = u8_alloc_n(n+1,lispval);
    else elts=_elts;
    /* Initialize the elements */
    elts[0]=fcn; memcpy(elts+1,args,LISPVEC_BYTELEN(n));
    vecstruct.vec_elts = elts;
    vec = LISP_CONS(&vecstruct);
    /* Look it up in the cache. */
    cached = fd_hashtable_get_nolock(&(tc->calls),vec,VOID);
    if (!(VOIDP(cached))) {
      if (elts!=_elts) u8_free(elts);
      return cached;}
    else {
      int state = fd_ipeval_status();
      lispval result = fd_finish_call(fd_dapply(fcn,n,args));
      if (FD_ABORTP(result)) {
        if (elts!=_elts) u8_free(elts);
        return result;}
      else if (fd_ipeval_status() == state) {
        if (elts==_elts) {
          int i = 0, nelts = n+1;
          elts = u8_alloc_n(n+1,lispval);
          while (i<nelts) {elts[i]=fd_incref(_elts[i]); i++;}
          vec = fd_wrap_vector(nelts,elts);}
        else {
          int i = 0, nelts = n+1;
          while (i<nelts) {fd_incref(elts[i]); i++;}
          vec = fd_wrap_vector(nelts,elts);}
        fd_hashtable_store(&(tc->calls),vec,result);
        fd_decref(vec);
        return result;}
      else return result;}}
  else if (fd_cachecall_probe(fcn,n,args))
    return fd_cachecall(fcn,n,args);
  else return fd_finish_call(fd_dapply(fcn,n,args));
}

/* Initialization stuff */

FD_EXPORT void fd_init_cachecall_c()
{
  u8_register_source_file(_FILEINFO);

  FD_INIT_STATIC_CONS(&fcn_caches,fd_hashtable_type);
  fd_make_hashtable(&fcn_caches,128);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
