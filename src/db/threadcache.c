/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2012 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/apply.h"
#include "framerd/fddb.h"

static u8_condition FreeingForeignThreadCache=
  _("Attempt to free foreign threadcache");
static u8_condition FreeingInUseThreadCache=
  _("Attempt to free in-use threadcache");
static u8_condition PushingInUseThreadCache=
  _("Attempt to push in-use threadcache");
static u8_condition SettingInUseThreadCache=
  _("Attempt to set in-use threadcache");

#if (FD_USE_TLS)
u8_tld_key fd_threadcache_key;

#elif (FD_USE__THREAD)
__thread struct FD_THREAD_CACHE *fd_threadcache=NULL;
#else
struct FD_THREAD_CACHE *fd_threadcache=NULL;
#endif

FD_EXPORT int fd_free_thread_cache(struct FD_THREAD_CACHE *tc)
{
  FD_INTPTR ptrval=(FD_INTPTR) tc;
  if (tc->fdtc_inuse) {
    if (tc->fdtc_id)
      u8_log(LOG_WARN,FreeingInUseThreadCache,
	     "Freeing in-use threadcache: %llx (%s)",ptrval,tc->fdtc_id);
    else u8_log(LOG_WARN,FreeingInUseThreadCache,
		"Freeing in-use threadcache: %llx",ptrval);}
  /* These may do customized things for some of the tables. */
  fd_recycle_hashtable(&(tc->calls));
  fd_recycle_hashtable(&(tc->oids));
  fd_recycle_hashtable(&(tc->indices));
  fd_recycle_hashtable(&(tc->bground));
  tc->fdtc_inuse=0;
  u8_free(tc);
  return 1;
}

FD_EXPORT int fd_pop_threadcache(struct FD_THREAD_CACHE *tc)
{
  if (tc==NULL) return 0;
  else if (tc!=fd_threadcache) {
    u8_seterr(FreeingForeignThreadCache,"fd_pop_threadcache",
	      ((tc->fdtc_id)?(u8_strdup(tc->fdtc_id)):(NULL)));
    return -1;}
  else {
    struct FD_THREAD_CACHE *prev=tc->fdtc_prev;
#if FD_USE_TLS
    fd_tld_set(fd_threadcache_key,prev);
#else
    fd_threadcache=prev;
#endif
    tc->fdtc_inuse--;
    if (tc->fdtc_inuse<=0) fd_free_thread_cache(tc);
    return 1;}
}

FD_EXPORT fd_thread_cache fd_cons_thread_cache
  (int ccsize,int ocsize,int bcsize,int kcsize)
{
  struct FD_THREAD_CACHE *tc=u8_alloc(struct FD_THREAD_CACHE);
  tc->fdtc_inuse=0; tc->fdtc_id=NULL;

  FD_INIT_STATIC_CONS(&(tc->calls),fd_hashtable_type);
  fd_make_hashtable(&(tc->calls),ccsize);

  FD_INIT_STATIC_CONS(&(tc->oids),fd_hashtable_type);
  fd_make_hashtable(&(tc->oids),ocsize);

  FD_INIT_STATIC_CONS(&(tc->bground),fd_hashtable_type);
  fd_make_hashtable(&(tc->bground),bcsize);

  FD_INIT_STATIC_CONS(&(tc->indices),fd_hashtable_type);
  fd_make_hashtable(&(tc->indices),kcsize);
  
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
  FD_INTPTR ptrval=(FD_INTPTR) tc;
  if ((tc)&&(tc->fdtc_inuse)) {
    if (tc->fdtc_id)
      u8_log(LOG_WARN,PushingInUseThreadCache,
	     "Pushing in-use threadcache: %llx (%s)",ptrval,tc->fdtc_id);
    else u8_log(LOG_WARN,PushingInUseThreadCache,
		"Pushing in-use threadcache: %llx",ptrval);}
  if (tc==NULL) tc=fd_new_thread_cache();
  if (tc==fd_threadcache) return tc;
  tc->fdtc_prev=fd_threadcache;
#if FD_USE_TLS
  fd_tld_set(fd_threadcache_key,tc);
#else
  fd_threadcache=tc;
#endif
  tc->fdtc_inuse++;
  return tc;
}

FD_EXPORT fd_thread_cache fd_set_threadcache(struct FD_THREAD_CACHE *tc)
{
  FD_INTPTR ptrval=(FD_INTPTR) tc;
  if ((tc)&&(tc->fdtc_inuse)) {
    if (tc->fdtc_id)
      u8_log(LOG_WARN,SettingInUseThreadCache,
	     "Setting in-use threadcache: %llx (%s)",ptrval,tc->fdtc_id);
    else u8_log(LOG_WARN,SettingInUseThreadCache,
		"Setting in-use threadcache: %llx",ptrval);}
  if (tc==NULL) tc=fd_new_thread_cache();
  if (fd_threadcache) {
    struct FD_THREAD_CACHE *oldtc=fd_threadcache;
    oldtc->fdtc_inuse--;
    if (oldtc->fdtc_inuse<=0) fd_free_thread_cache(oldtc);}
  tc->fdtc_inuse++;
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
    tc->fdtc_prev=NULL; tc->fdtc_inuse++;
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
