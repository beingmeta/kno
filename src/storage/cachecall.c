/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_IPEVAL 1
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/tables.h"
#include "kno/apply.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"

#include <libu8/libu8.h>
#include <libu8/u8filefns.h>
#include <libu8/u8timefns.h>

/* Cache calls */

static struct KNO_HASHTABLE fcn_caches;

static struct KNO_HASHTABLE *get_fcn_cache(lispval fcn,int create)
{
  lispval cache = kno_hashtable_get(&fcn_caches,fcn,VOID);
  if (VOIDP(cache)) {
    cache = kno_make_hashtable(NULL,512);
    kno_hashtable_store(&fcn_caches,fcn,cache);
    return kno_consptr(struct KNO_HASHTABLE *,cache,kno_hashtable_type);}
  else return kno_consptr(struct KNO_HASHTABLE *,cache,kno_hashtable_type);
}

KNO_EXPORT lispval kno_xcachecall
(struct KNO_HASHTABLE *cache,lispval fcn,int n,kno_argvec args)
{
  lispval vec, cached;
  struct KNO_VECTOR vecstruct;
  memset(&vecstruct,0,sizeof(vecstruct));
  vecstruct.vec_length = n;
  vecstruct.vec_free_elts = 0;
  vecstruct.vec_elts = (lispval *) ((n==0) ? (NULL) : (args));
  KNO_SET_CONS_TYPE(&vecstruct,kno_vector_type);
  vec = LISP_CONS(&vecstruct);
  cached = kno_hashtable_get(cache,vec,VOID);
  if (VOIDP(cached)) {
    int state = kno_ipeval_status();
    lispval result = kno_dapply(fcn,n,args);
    if (KNO_ABORTP(result)) {
      kno_decref((lispval)cache);
      return result;}
    else if (kno_ipeval_status() == state) {
      lispval *datavec = ((n) ? (u8_alloc_n(n,lispval)) : (NULL));
      lispval key = kno_wrap_vector(n,datavec);
      int i = 0; while (i<n) {
        datavec[i]=kno_incref(args[i]); i++;}
      kno_hashtable_store(cache,key,result);
      kno_decref(key);}
    return result;}
  else return cached;
}

KNO_EXPORT lispval kno_cachecall(lispval fcn,int n,kno_argvec args)
{
  struct KNO_HASHTABLE *cache = get_fcn_cache(fcn,1);
  lispval result = kno_xcachecall(cache,fcn,n,args);
  kno_decref((lispval)cache);
  return result;
}

KNO_EXPORT void kno_clear_callcache(lispval arg)
{
  if (fcn_caches.table_n_keys==0) return;
  if (VOIDP(arg)) kno_reset_hashtable(&fcn_caches,128,1);
  else if ((VECTORP(arg)) && (VEC_LEN(arg)>0)) {
    lispval fcn = VEC_REF(arg,0);
    lispval table = kno_hashtable_get(&fcn_caches,fcn,EMPTY);
    if (EMPTYP(table)) return;
    /* This should probably reall signal an error. */
    else if (!(HASHTABLEP(table))) return;
    else {
      int i = 0, n_args = VEC_LEN(arg)-1;
      lispval *datavec = u8_alloc_n(n_args,lispval);
      lispval key = kno_wrap_vector(n_args,datavec);
      while (i<n_args) {
        datavec[i]=kno_incref(VEC_REF(arg,i+1)); i++;}
      kno_hashtable_store((kno_hashtable)table,key,VOID);
      kno_decref(key);}}
  else if (kno_hashtable_probe(&fcn_caches,arg))
    kno_hashtable_store(&fcn_caches,arg,VOID);
  else return;
}

static int hashtable_cachecount(lispval key,lispval v,void *ptr)
{
  if (HASHTABLEP(v)) {
    kno_hashtable h = (kno_hashtable)v;
    int *count = (int *)ptr;
    *count = *count+h->table_n_keys;}
  return 0;
}

KNO_EXPORT long kno_callcache_load()
{
  int count = 0;
  kno_for_hashtable(&fcn_caches,hashtable_cachecount,&count,1);
  return count;
}

KNO_EXPORT int kno_xcachecall_probe(struct KNO_HASHTABLE *cache,
				    lispval fcn,int n,kno_argvec args)
{
  lispval vec; int iscached = 0;
  struct KNO_VECTOR vecstruct;
  memset(&vecstruct,0,sizeof(vecstruct));
  vecstruct.vec_length = n;
  vecstruct.vec_free_elts = 0;
  vecstruct.vec_elts = (lispval *) ((n==0) ? (NULL) : (args));
  KNO_SET_CONS_TYPE(&vecstruct,kno_vector_type);
  vec = LISP_CONS(&vecstruct);
  iscached = kno_hashtable_probe(cache,vec);
  return iscached;
}

KNO_EXPORT int kno_cachecall_probe(lispval fcn,int n,kno_argvec args)
{
  int iscached = 0;
  struct KNO_HASHTABLE *cache = get_fcn_cache(fcn,1);
  iscached = kno_xcachecall_probe(cache,fcn,n,args);
  kno_decref((lispval)cache);
  return iscached;
}

KNO_EXPORT lispval kno_xcachecall_try(struct KNO_HASHTABLE *cache,
				      lispval fcn,int n,kno_argvec args)
{
  lispval vec; lispval value;
  struct KNO_VECTOR vecstruct;
  memset(&vecstruct,0,sizeof(vecstruct));
  vecstruct.vec_length = n;
  vecstruct.vec_free_elts = 0;
  vecstruct.vec_elts = (lispval *) ((n==0) ? (NULL) : (args));
  KNO_SET_CONS_TYPE(&vecstruct,kno_vector_type);
  vec = LISP_CONS(&vecstruct);
  value = kno_hashtable_get(cache,vec,VOID);
  if (VOIDP(value)) return EMPTY;
  else return value;
}

KNO_EXPORT lispval kno_cachecall_try(lispval fcn,int n,kno_argvec args)
{
  struct KNO_HASHTABLE *cache = get_fcn_cache(fcn,1);
  lispval value = kno_xcachecall_try(cache,fcn,n,args);
  kno_decref((lispval)cache);
  if (VOIDP(value)) return EMPTY;
  else return value;
}

/* Thread cache calls */

#define TCACHECALL_STACK_ELTS 8

KNO_EXPORT lispval kno_tcachecall(lispval fcn,int n,kno_argvec args)
{
  if (kno_threadcache) {
    kno_thread_cache tc = kno_threadcache;
    struct KNO_VECTOR vecstruct;
    lispval elts[n+1], vec, cached;
    /* Initialize the stack vector */
    memset(&vecstruct,0,sizeof(vecstruct));
    vecstruct.vec_free_elts = 0;
    vecstruct.vec_length = n+1;
    KNO_SET_CONS_TYPE(&vecstruct,kno_vector_type);
    /* Allocate an elements vector if neccessary */
    /* Initialize the elements */
    elts[0]=fcn; memcpy(elts+1,args,LISPVEC_BYTELEN(n));
    vecstruct.vec_elts = elts;
    vec = LISP_CONS(&vecstruct);
    /* Look it up in the cache. */
    cached = kno_hashtable_get_nolock(&(tc->calls),vec,VOID);
    if (!(VOIDP(cached))) {
      return cached;}
    else {
      int state = kno_ipeval_status();
      lispval result = kno_dapply(fcn,n,args);
      if (KNO_ABORTP(result)) return result;
      else if (kno_ipeval_status() == state) { /* OK to cache */
	int i = 0, nelts = n+1;
	lispval *vec_elts = u8_alloc_n(n+1,lispval);
	while (i<nelts) {vec_elts[i]=kno_incref(elts[i]); i++;}
	vec = kno_wrap_vector(nelts,vec_elts);
	kno_hashtable_store(&(tc->calls),vec,result);
	kno_decref(vec);
	return result;}
      else return result;}}
 else if (kno_cachecall_probe(fcn,n,args))
   return kno_cachecall(fcn,n,args);
 else return kno_dapply(fcn,n,args);
}

/* Initialization stuff */

KNO_EXPORT void kno_init_cachecall_c()
{
  u8_register_source_file(_FILEINFO);

  KNO_INIT_STATIC_CONS(&fcn_caches,kno_hashtable_type);
  kno_make_hashtable(&fcn_caches,128);

}

