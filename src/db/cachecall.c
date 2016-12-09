/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_IPEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/apply.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"

#include <libu8/libu8.h>
#include <libu8/u8filefns.h>
#include <libu8/u8timefns.h>

/* Cache calls */

static struct FD_HASHTABLE fcn_caches;

static struct FD_HASHTABLE *get_fcn_cache(fdtype fcn,int create)
{
  fdtype cache=fd_hashtable_get(&fcn_caches,fcn,FD_VOID);
  if (FD_VOIDP(cache)) {
    cache=fd_make_hashtable(NULL,512);
    fd_hashtable_store(&fcn_caches,fcn,cache);
    return FD_GET_CONS(cache,fd_hashtable_type,struct FD_HASHTABLE *);}
  else return FD_GET_CONS(cache,fd_hashtable_type,struct FD_HASHTABLE *);
}

FD_EXPORT fdtype fd_cachecall(fdtype fcn,int n,fdtype *args)
{
  fdtype vec, cached;
  struct FD_HASHTABLE *cache=get_fcn_cache(fcn,1);
  struct FD_VECTOR vecstruct;
  vecstruct.fd_conshead=0;
  vecstruct.fd_veclen=n;
  vecstruct.fd_freedata=0;
  vecstruct.fd_vecelts=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  cached=fd_hashtable_get(cache,vec,FD_VOID);
  if (FD_VOIDP(cached)) {
    int state=fd_ipeval_status();
    fdtype result=fd_finish_call(fd_dapply(fcn,n,args));
    if (FD_ABORTP(result)) {
      fd_decref((fdtype)cache);
      return result;}
    else if (fd_ipeval_status()==state) {
      fdtype *datavec=((n) ? (u8_alloc_n(n,fdtype)) : (NULL));
      fdtype key=fd_init_vector(NULL,n,datavec);
      int i=0; while (i<n) {
        datavec[i]=fd_incref(args[i]); i++;}
      fd_hashtable_store(cache,key,result);
      fd_decref(key);}
    fd_decref((fdtype)cache);
    return result;}
  else {
    fd_decref((fdtype)cache);
    return cached;}
}

FD_EXPORT fdtype fd_xcachecall
  (struct FD_HASHTABLE *cache,fdtype fcn,int n,fdtype *args)
{
  fdtype vec, cached;
  struct FD_VECTOR vecstruct;
  vecstruct.fd_conshead=0;
  vecstruct.fd_veclen=n;
  vecstruct.fd_freedata=0;
  vecstruct.fd_vecelts=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  cached=fd_hashtable_get(cache,vec,FD_VOID);
  if (FD_VOIDP(cached)) {
    int state=fd_ipeval_status();
    fdtype result=fd_finish_call(fd_dapply(fcn,n,args));
    if (FD_ABORTP(result)) {
      fd_decref((fdtype)cache);
      return result;}
    else if (fd_ipeval_status()==state) {
      fdtype *datavec=((n) ? (u8_alloc_n(n,fdtype)) : (NULL));
      fdtype key=fd_init_vector(NULL,n,datavec);
      int i=0; while (i<n) {
        datavec[i]=fd_incref(args[i]); i++;}
      fd_hashtable_store(cache,key,result);
      fd_decref(key);}
    return result;}
  else return cached;
}

FD_EXPORT void fd_clear_callcache(fdtype arg)
{
  if (fcn_caches.n_keys==0) return;
  if (FD_VOIDP(arg)) fd_reset_hashtable(&fcn_caches,128,1);
  else if ((FD_VECTORP(arg)) && (FD_VECTOR_LENGTH(arg)>0)) {
    fdtype fcn=FD_VECTOR_REF(arg,0);
    fdtype table=fd_hashtable_get(&fcn_caches,fcn,FD_EMPTY_CHOICE);
    if (FD_EMPTY_CHOICEP(table)) return;
    /* This should probably reall signal an error. */
    else if (!(FD_HASHTABLEP(table))) return;
    else {
      int i=0, n_args=FD_VECTOR_LENGTH(arg)-1;
      fdtype *datavec=u8_alloc_n(n_args,fdtype);
      fdtype key=fd_init_vector(NULL,n_args,datavec);
      while (i<n_args) {
        datavec[i]=fd_incref(FD_VECTOR_REF(arg,i+1)); i++;}
      fd_hashtable_store((fd_hashtable)table,key,FD_VOID);
      fd_decref(key);}}
  else if (fd_hashtable_probe(&fcn_caches,arg))
    fd_hashtable_store(&fcn_caches,arg,FD_VOID);
  else return;
}

static int hashtable_cachecount(fdtype key,fdtype v,void *ptr)
{
  if (FD_HASHTABLEP(v)) {
    fd_hashtable h=(fd_hashtable)v;
    int *count=(int *)ptr;
    *count=*count+h->n_keys;}
  return 0;
}

FD_EXPORT long fd_callcache_load()
{
  int count=0;
  fd_for_hashtable(&fcn_caches,hashtable_cachecount,&count,1);
  return count;
}

FD_EXPORT int fd_cachecall_probe(fdtype fcn,int n,fdtype *args)
{
  fdtype vec; int iscached=0;
  struct FD_HASHTABLE *cache=get_fcn_cache(fcn,1);
  struct FD_VECTOR vecstruct;
  vecstruct.fd_conshead=0;
  vecstruct.fd_veclen=n;
  vecstruct.fd_freedata=0;
  vecstruct.fd_vecelts=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  iscached=fd_hashtable_probe(cache,vec);
  fd_decref((fdtype)cache);
  return iscached;
}

FD_EXPORT int fd_xcachecall_probe(struct FD_HASHTABLE *cache,fdtype fcn,int n,fdtype *args)
{
  fdtype vec; int iscached=0;
  struct FD_VECTOR vecstruct;
  vecstruct.fd_conshead=0;
  vecstruct.fd_veclen=n;
  vecstruct.fd_freedata=0;
  vecstruct.fd_vecelts=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  iscached=fd_hashtable_probe(cache,vec);
  return iscached;
}

FD_EXPORT fdtype fd_cachecall_try(fdtype fcn,int n,fdtype *args)
{
  fdtype vec; fdtype value;
  struct FD_HASHTABLE *cache=get_fcn_cache(fcn,1);
  struct FD_VECTOR vecstruct;
  vecstruct.fd_conshead=0;
  vecstruct.fd_veclen=n;
  vecstruct.fd_freedata=0;
  vecstruct.fd_vecelts=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  value=fd_hashtable_get(cache,vec,FD_VOID);
  fd_decref((fdtype)cache);
  if (FD_VOIDP(value)) return FD_EMPTY_CHOICE;
  else return value;
}

FD_EXPORT fdtype fd_xcachecall_try(struct FD_HASHTABLE *cache,fdtype fcn,int n,fdtype *args)
{
  fdtype vec; fdtype value;
  struct FD_VECTOR vecstruct;
  vecstruct.fd_conshead=0;
  vecstruct.fd_veclen=n;
  vecstruct.fd_freedata=0;
  vecstruct.fd_vecelts=((n==0) ? (NULL) : (args));
  FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
  vec=FDTYPE_CONS(&vecstruct);
  value=fd_hashtable_get(cache,vec,FD_VOID);
  if (FD_VOIDP(value)) return FD_EMPTY_CHOICE;
  else return value;
}

/* Thread cache calls */

#define TCACHECALL_STACK_ELTS 8

FD_EXPORT fdtype fd_tcachecall(fdtype fcn,int n,fdtype *args)
{
  if (fd_threadcache) {
    fd_thread_cache tc=fd_threadcache;
    struct FD_VECTOR vecstruct;
    fdtype _elts[TCACHECALL_STACK_ELTS], *elts=NULL, vec, cached;
    /* Initialize the stack vector */
    vecstruct.fd_conshead=0;
    vecstruct.fd_freedata=0;
    vecstruct.fd_veclen=n+1;
    FD_SET_CONS_TYPE(&vecstruct,fd_vector_type);
    /* Allocate an elements vector if neccessary */
    if ((n+1)>(TCACHECALL_STACK_ELTS))
      elts=u8_alloc_n(n+1,fdtype);
    else elts=_elts;
    /* Initialize the elements */
    elts[0]=fcn; memcpy(elts+1,args,sizeof(fdtype)*n);
    vecstruct.fd_vecelts=elts;
    vec=FDTYPE_CONS(&vecstruct);
    /* Look it up in the cache. */
    cached=fd_hashtable_get_nolock(&(tc->calls),vec,FD_VOID);
    if (!(FD_VOIDP(cached))) {
      if (elts!=_elts) u8_free(elts);
      return cached;}
    else {
      int state=fd_ipeval_status();
      fdtype result=fd_finish_call(fd_dapply(fcn,n,args));
      if (FD_ABORTP(result)) {
        if (elts!=_elts) u8_free(elts);
        return result;}
      else if (fd_ipeval_status()==state) {
        if (elts==_elts) {
          int i=0, nelts=n+1;
          elts=u8_alloc_n(n+1,fdtype);
          while (i<nelts) {elts[i]=fd_incref(_elts[i]); i++;}
          vec=fd_init_vector(NULL,nelts,elts);}
        else {
          int i=0, nelts=n+1;
          while (i<nelts) {fd_incref(elts[i]); i++;}
          vec=fd_init_vector(NULL,nelts,elts);}
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
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
