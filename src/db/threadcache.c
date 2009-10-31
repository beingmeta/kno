/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2009 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/apply.h"
#include "fdb/fddb.h"

static u8_condition FreeingForeignThreadCache=
  _("Attempt to free foreign threadcache");

#if (FD_USE_TLS)
u8_tld_key fd_threadcache_key;

#elif (FD_USE__THREAD)
__thread struct FD_THREAD_CACHE *fd_threadcache=NULL;
#else
struct FD_THREAD_CACHE *fd_threadcache=NULL;
#endif

FD_EXPORT int fd_free_thread_cache(struct FD_THREAD_CACHE *tc)
{
  /* These may do customized things for some of the tables. */
  fd_reset_hashtable(&(tc->fdtc_calls),0,0);
  fd_reset_hashtable(&(tc->fdtc_oids),0,0);
  fd_reset_hashtable(&(tc->fdtc_bground),0,0);
  fd_reset_hashtable(&(tc->fdtc_keys),0,0);
  u8_free(tc);
  return 1;
}

FD_EXPORT int fd_pop_threadcache(struct FD_THREAD_CACHE *tc)
{
  if (tc==NULL) return 0;
  else if (tc!=fd_threadcache) {
    u8_seterr(FreeingForeignThreadCache,"fd_free_threadcache",NULL);
    return -1;}
  else {
    struct FD_THREAD_CACHE *prev=tc->fdtc_prev;
#if FD_USE_TLS
    fd_tld_set(fd_threadcache_key,prev);
#else
    fd_threadcache=prev;
#endif
    fd_free_thread_cache(tc);
    return 1;}
}

FD_EXPORT fd_thread_cache fd_cons_thread_cache(int ccsize,int ocsize,int bcsize,int kcsize)
{
  struct FD_THREAD_CACHE *tc=u8_alloc(struct FD_THREAD_CACHE);
  fd_make_hashtable(&(tc->fdtc_calls),ccsize);
  fd_make_hashtable(&(tc->fdtc_oids),ocsize);
  fd_make_hashtable(&(tc->fdtc_bground),bcsize);
  fd_make_hashtable(&(tc->fdtc_keys),kcsize);
  tc->fdtc_prev=NULL;
  return tc;
}

FD_EXPORT fd_thread_cache fd_new_thread_cache()
{
  return fd_cons_thread_cache
    (FD_THREAD_CALLCACHE_SIZE,FD_THREAD_OIDCACHE_SIZE,
     FD_THREAD_BGCACHE_SIZE,FD_THREAD_KEYCACHE_SIZE);
}

FD_EXPORT fd_thread_cache fd_push_threadcache(struct FD_THREAD_CACHE *tc)
{
  if (tc==NULL) tc=fd_new_thread_cache();
  tc->fdtc_prev=fd_threadcache;
#if FD_USE_TLS
  fd_tld_set(fd_threadcache_key,tc);
#else
  fd_threadcache=tc;
#endif
  return tc;
}

FD_EXPORT fd_thread_cache fd_use_threadcache()
{
  if (fd_threadcache) return NULL;
  else {
    struct FD_THREAD_CACHE *tc=fd_new_thread_cache();
    tc->fdtc_prev=fd_threadcache;
#if FD_USE_TLS
    fd_tld_set(fd_threadcache_key,tc);
#else
    fd_threadcache=tc;
#endif
    return tc;}
}

/* Initialization stuff */

FD_EXPORT void fd_init_threadcache_c()
{
  fd_register_source_file(versionid);
#if (FD_USE_TLS)
  u8_new_threadkey(&fd_threadcache_key,NULL);
#endif
}
