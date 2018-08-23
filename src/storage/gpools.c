!/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"

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
                      lispval fetchfn,lispval loadfn,
                      lispval allocfn,lispval savefn,
                      lispval lockfn,lispval state)
{
  struct FD_GPOOL *gp = u8_alloc(struct FD_GPOOL);
  lispval loadval = fd_apply(loadfn,0,NULL); unsigned int load;
  if (!(FIXNUMP(loadval)))
    return fd_type_error("fd_make_gpool","pool load (fixnum)",loadval);
  else load = FIX2INT(loadval);
  fd_init_pool((fd_pool)gp,base,cap,
               &gpool_handler,
               id,id,id,
               FD_STORAGE_ISPOOL,FD_FALSE,FD_FALSE);
  gp->pool_load = load;
  fd_incref(fetchfn); fd_incref(loadfn);
  fd_incref(allocfn); fd_incref(savefn);
  fd_incref(lockfn); fd_incref(state);
  gp->fetchfn = fetchfn; gp->pool_loadfn = loadfn;
  gp->allocfn = allocfn;  gp->savefn = savefn;
  gp->lockfn = lockfn; gp->state = state;
  return gp;
}

static int gpool_load(fd_pool p)
{
  struct FD_GPOOL *np = (struct FD_GPOOL *)p;
  lispval value;
  value = fd_dtcall(np->pool_connpool,2,get_load_symbol,fd_make_oid(p->pool_base));
  if (FIXNUMP(value)) return FIX2INT(value);
  else if (FD_ABORTP(value))
    return fd_interr(value);
  else {
    fd_seterr(fd_BadServerResponse,"POOL-LOAD",NULL,value);
    return -1;}
}

static lispval gpool_fetch(fd_pool p,lispval oid)
{
  struct FD_GPOOL *np = (struct FD_GPOOL *)p;
  lispval value;
  value = fd_dtcall(np->pool_connpool,2,oid_value_symbol,oid);
  return value;
}

static lispval *gpool_fetchn(fd_pool p,int n,lispval *oids)
{
  struct FD_GPOOL *np = (struct FD_GPOOL *)p;
  lispval vector = fd_wrap_vector(n,oids);
  lispval value = fd_dtcall(np->pool_connpool,2,fetch_oids_symbol,vector);
  fd_decref(vector);
  if (VECTORP(value)) {
    struct FD_VECTOR *vstruct = (struct FD_VECTOR)value;
    lispval *results = u8_alloc_n(n,lispval);
    memcpy(results,vstruct->vec_elts,LISPVEC_BYTELEN(n));
    /* Free the CONS itself (and maybe data), to avoid DECREF/INCREF
       of values. */
    if (vstruct->str_freebytes) u8_free(vstruct->vec_elts);
    u8_free((struct FD_CONS *)value);
    return results;}
  else {
    fd_seterr(fd_BadServerResponse,"netpool_fetchn",
              np->poolid,fd_incref(value));
    return NULL;}
}

static int gpool_lock(fd_pool p,lispval oid)
{
  struct FD_GPOOL *np = (struct FD_GPOOL *)p;
  lispval value;
  value = fd_dtcall(np->pool_connpool,3,lock_oid_symbol,oid,client_id);
  if (VOIDP(value)) return 0;
  else if (FD_ABORTP(value))
    return fd_interr(value);
  else {
    fd_hashtable_store(&(p->index_cache),oid,value);
    fd_decref(value);
    return 1;}
}

static int gpool_unlock(fd_pool p,lispval oids)
{
  struct FD_GPOOL *np = (struct FD_GPOOL *)p;
  lispval result;
  result = fd_dtcall(np->pool_connpool,3,clear_oid_lock_symbol,oids,client_id);
  if (FD_ABORTP(result)) {
    fd_decref(result); return 0;}
  else {fd_decref(result); return 1;}
}

static int gpool_storen(fd_pool p,int n,lispval *oids,lispval *values)
{
  struct FD_GPOOL *np = (struct FD_GPOOL *)p;
  if (np->bulk_commitp) {
    int i = 0;
    lispval *storevec = u8_alloc_n(n*2,lispval), vec, result;
    while (i < n) {
      storevec[i*2]=oids[i];
      storevec[i*2+1]=values[i];
      i++;}
    vec = fd_wrap_vector(n*2,storevec);
    result = fd_dtcall(np->pool_connpool,3,bulk_commit_symbol,client_id,vec);
    /* Don't decref the individual elements because you didn't incref them. */
    u8_free((struct FD_CONS *)vec); u8_free(storevec);
    return 1;}
  else {
    int i = 0;
    while (i < n) {
      lispval result = fd_dtcall(np->pool_connpool,4,unlock_oid_symbol,oids[i],client_id,values[i]);
      fd_decref(result); i++;}
    return 1;}
}

static lispval gpool_alloc(fd_pool p,int n)
{
  lispval results = EMPTY, request; int i = 0;
  struct FD_GPOOL *np = (struct FD_GPOOL *)p;
  request = fd_conspair(new_oid_symbol,NIL);
  while (i < n) {
    lispval result = fd_dteval(np->pool_connpool,request);
    CHOICE_ADD(results,result);
    i++;}
  return results;
}

static struct FD_POOL_HANDLER gpool_handler={
  "gpool", 1, sizeof(struct FD_GPOOL), 12,
  NULL, /* close */
  gpool_alloc, /* alloc */
  gpool_fetch, /* fetch */
  gpool_fetchn, /* fetchn */
  gpool_load, /* getload */
  gpool_lock, /* lock */
  gpool_unlock, /* release */
  gpool_storen, /* storen */
  NULL, /* swapout */
  NULL}; /* sync */

FD_EXPORT void fd_init_gpools_c()
{
  u8_register_source_file(_FILEINFO);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
