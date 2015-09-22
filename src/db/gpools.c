/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_DTYPEIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/fddb.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>

#include <stdarg.h>

/*
   Thoughts on odd pools.
     The most general kind of pool has applicable lisp object
      for all the pool handler functions;
     Another interesting kind of pool is able to be fetched
      but manages storage in other ways.  This is good for a case
      where the OIDs represent objects in an external SQL database
      (like WebEchoes pings).  Note that this kind of pool doesn't
      really have an alloc function of its own, since creation happens
      on the server side.
     Another interesting variant is a memory-only pool, where
      you can create and modify frames but they are limited to
      the current memory image.  This is what mempools are.

*/

static struct FD_POOL_HANDLER gpool_handler;

FD_EXPORT
fd_pool fd_make_gpool(FD_OID base,int cap,u8_string id,
                      fdtype fetchfn,fdtype loadfn,
                      fdtype allocfn,fdtype savefn,
                      fdtype lockfn,fdtype state)
{
  struct FD_GPOOL *gp=u8_alloc(struct FD_GPOOL);
  fdtype loadval=fd_apply(loadfn,0,NULL); unsigned int load;
  if (!(FD_FIXNUMP(loadval)))
    return fd_type_error("fd_make_gpool","pool load (fixnum)",loadval);
  else load=FD_FIX2INT(loadval);
  fd_init_pool((fd_pool)gp,base,cap,&gpool_handler,id,id);
  gp->load=load;
  fd_incref(fetchfn); fd_incref(loadfn);
  fd_incref(allocfn); fd_incref(savefn);
  fd_incref(lockfn); fd_incref(state);
  gp->fetchfn=fetchfn; gp->loadfn=loadfn;
  gp->allocfn=allocfn;  gp->savefn=savefn;
  gp->lockfn=lockfn; gp->state=state;
  return gp;
}

static int gpool_load(fd_pool p)
{
  struct FD_GPOOL *np=(struct FD_GPOOL *)p;
  fdtype value;
  value=fd_dtcall(np->connpool,2,get_load_symbol,fd_make_oid(p->base));
  if (FD_FIXNUMP(value)) return FD_FIX2INT(value);
  else if (FD_ABORTP(value))
    return fd_interr(value);
  else {
    fd_seterr(fd_BadServerResponse,"POOL-LOAD",NULL,value);
    return -1;}
}

static fdtype gpool_fetch(fd_pool p,fdtype oid)
{
  struct FD_GPOOL *np=(struct FD_GPOOL *)p;
  fdtype value;
  value=fd_dtcall(np->connpool,2,oid_value_symbol,oid);
  return value;
}

static fdtype *gpool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_GPOOL *np=(struct FD_GPOOL *)p;
  fdtype vector=fd_init_vector(NULL,n,oids);
  fdtype value=fd_dtcall(np->connpool,2,fetch_oids_symbol,vector);
  fd_decref(vector);
  if (FD_VECTORP(value)) {
    struct FD_VECTOR *vstruct=(struct FD_VECTOR)value;
    fdtype *results=u8_alloc_n(n,fdtype);
    memcpy(results,vstruct->data,sizeof(fdtype)*n);
    /* Free the CONS itself (and maybe data), to avoid DECREF/INCREF
       of values. */
    if (vstruct->freedata) u8_free(vstruct->data);
    u8_free((struct FD_CONS *)value);
    return results;}
  else {
    fd_seterr(fd_BadServerResponse,"netpool_fetchn",
              u8_strdup(np->cid),fd_incref(value));
    return NULL;}
}

static int gpool_lock(fd_pool p,fdtype oid)
{
  struct FD_GPOOL *np=(struct FD_GPOOL *)p;
  fdtype value;
  value=fd_dtcall(np->connpool,3,lock_oid_symbol,oid,client_id);
  if (FD_VOIDP(value)) return 0;
  else if (FD_ABORTP(value))
    return fd_interr(value);
  else {
    fd_hashtable_store(&(p->cache),oid,value);
    fd_decref(value);
    return 1;}
}

static int gpool_unlock(fd_pool p,fdtype oids)
{
  struct FD_GPOOL *np=(struct FD_GPOOL *)p;
  fdtype result;
  result=fd_dtcall(np->connpool,3,clear_oid_lock_symbol,oids,client_id);
  if (FD_ABORTP(result)) {
    fd_decref(result); return 0;}
  else {fd_decref(result); return 1;}
}

static int gpool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  struct FD_GPOOL *np=(struct FD_GPOOL *)p;
  if (np->bulk_commitp) {
    int i=0;
    fdtype *storevec=u8_alloc_n(n*2,fdtype), vec, result;
    while (i < n) {
      storevec[i*2]=oids[i];
      storevec[i*2+1]=values[i];
      i++;}
    vec=fd_init_vector(NULL,n*2,storevec);
    result=fd_dtcall(np->connpool,3,bulk_commit_symbol,client_id,vec);
    /* Don't decref the individual elements because you didn't incref them. */
    u8_free((struct FD_CONS *)vec); u8_free(storevec);
    return 1;}
  else {
    int i=0;
    while (i < n) {
      fdtype result=fd_dtcall(np->connpool,4,unlock_oid_symbol,oids[i],client_id,values[i]);
      fd_decref(result); i++;}
    return 1;}
}

static fdtype gpool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE, request; int i=0;
  struct FD_GPOOL *np=(struct FD_GPOOL *)p;
  request=fd_conspair(new_oid_symbol,FD_EMPTY_LIST);
  while (i < n) {
    fdtype result=fd_dteval(np->connpool,request);
    FD_ADD_TO_CHOICE(results,result);
    i++;}
  return results;
}

static struct FD_POOL_HANDLER gpool_handler={
  "gpool", 1, sizeof(struct FD_GPOOL), 12,
  NULL, /* close */
  NULL, /* setcache */
  NULL, /* setbuf */
  gpool_alloc, /* alloc */
  gpool_fetch, /* fetch */
  gpool_fetchn, /* fetchn */
  gpool_load, /* getload */
  gpool_lock, /* lock */
  gpool_unlock, /* release */
  gpool_storen, /* storen */
  NULL, /* swapout */
  NULL, /* metadata */
  NULL}; /* sync */

FD_EXPORT void fd_init_gpools_c()
{
  u8_register_source_file(_FILEINFO);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
