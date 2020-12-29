/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/tables.h"
#include "kno/apply.h"
#include "kno/storage.h"

static u8_condition FreeingForeignThreadCache=
  _("Attempt to free foreign threadcache");
static u8_condition FreeingInUseThreadCache=
  _("Attempt to free in-use threadcache");
static u8_condition PushingInUseThreadCache=
  _("Attempt to push in-use threadcache");
static u8_condition SettingInUseThreadCache=
  _("Attempt to set in-use threadcache");

#if (KNO_USE__THREAD)
__thread struct KNO_THREAD_CACHE *kno_threadcache = NULL;
#elif (KNO_USE_TLS)
u8_tld_key kno_threadcache_key;
#else
struct KNO_THREAD_CACHE *kno_threadcache = NULL;
#endif

KNO_EXPORT int kno_free_thread_cache(struct KNO_THREAD_CACHE *tc)
{
  KNO_INTPTR ptrval = (KNO_INTPTR) tc;
  if (tc->threadcache_inuse) {
    if (tc->threadcache_id)
      u8_logf(LOG_WARN,FreeingInUseThreadCache,
              "Freeing in-use threadcache: %llx (%s)",ptrval,tc->threadcache_id);
    else u8_logf(LOG_WARN,FreeingInUseThreadCache,
                 "Freeing in-use threadcache: %llx",ptrval);}
  /* These may do customized things for some of the tables. */
  kno_recycle_hashtable(&(tc->oids));
  kno_recycle_hashtable(&(tc->background_cache));
  kno_free_slotmap(&(tc->calls));
  kno_free_slotmap(&(tc->adjuncts));
  kno_free_slotmap(&(tc->indexes));
  tc->threadcache_inuse = 0;
  u8_free(tc);
  return 1;
}

KNO_EXPORT int kno_pop_threadcache(struct KNO_THREAD_CACHE *tc)
{
  if (tc == NULL) return 0;
  else if (tc!=kno_threadcache) {
    u8_seterr(FreeingForeignThreadCache,"kno_pop_threadcache",
              ((tc->threadcache_id)?(u8_strdup(tc->threadcache_id)):(NULL)));
    return -1;}
  else {
    struct KNO_THREAD_CACHE *prev = tc->threadcache_prev;
#if KNO_USE_TLS
    u8_tld_set(kno_threadcache_key,prev);
#else
    kno_threadcache = prev;
#endif
    tc->threadcache_inuse--;
    if (tc->threadcache_inuse<=0) kno_free_thread_cache(tc);
    return 1;}
}

KNO_EXPORT kno_thread_cache kno_cons_thread_cache(int oids_size,int bg_size)
{
  struct KNO_THREAD_CACHE *tc = u8_alloc(struct KNO_THREAD_CACHE);
  tc->threadcache_inuse = 0; tc->threadcache_id = NULL;

  if (oids_size <= 0) oids_size=1000;
  if (bg_size <= 0) bg_size=1000;

  KNO_INIT_STATIC_CONS(&(tc->oids),kno_hashtable_type);
  kno_make_hashtable(&(tc->oids),oids_size);

  KNO_INIT_STATIC_CONS(&(tc->background_cache),kno_hashtable_type);
  kno_make_hashtable(&(tc->background_cache),bg_size);

  KNO_INIT_STATIC_CONS(&(tc->calls),kno_slotmap_type);
  kno_init_slotmap(&(tc->calls),7,NULL);

  KNO_INIT_STATIC_CONS(&(tc->adjuncts),kno_slotmap_type);
  kno_init_slotmap(&(tc->adjuncts),7,NULL);

  KNO_INIT_STATIC_CONS(&(tc->indexes),kno_slotmap_type);
  kno_init_slotmap(&(tc->indexes),7,NULL);

  tc->threadcache_prev = NULL;
  return tc;
}

KNO_EXPORT kno_thread_cache kno_new_thread_cache()
{
  return kno_cons_thread_cache
    (KNO_THREAD_OIDCACHE_SIZE,KNO_THREAD_BGCACHE_SIZE);
}

KNO_EXPORT kno_thread_cache kno_push_threadcache(struct KNO_THREAD_CACHE *tc)
{
  KNO_INTPTR ptrval = (KNO_INTPTR) tc;
  if ((tc)&&(tc->threadcache_inuse)) {
    if (tc->threadcache_id)
      u8_logf(LOG_WARN,PushingInUseThreadCache,
              "Pushing in-use threadcache: %llx (%s)",ptrval,tc->threadcache_id);
    else u8_logf(LOG_WARN,PushingInUseThreadCache,
                 "Pushing in-use threadcache: %llx",ptrval);}
  if (tc == NULL) tc = kno_new_thread_cache();
  if (tc == kno_threadcache) return tc;
  tc->threadcache_prev = kno_threadcache;
#if KNO_USE_TLS
  u8_tld_set(kno_threadcache_key,tc);
#else
  kno_threadcache = tc;
#endif
  tc->threadcache_inuse++;
  return tc;
}

KNO_EXPORT kno_thread_cache kno_set_threadcache(struct KNO_THREAD_CACHE *tc)
{
  KNO_INTPTR ptrval = (KNO_INTPTR) tc;
  if ((tc)&&(tc->threadcache_inuse)) {
    if (tc->threadcache_id)
      u8_logf(LOG_WARN,SettingInUseThreadCache,
              "Setting in-use threadcache: %llx (%s)",ptrval,tc->threadcache_id);
    else u8_logf(LOG_WARN,SettingInUseThreadCache,
                 "Setting in-use threadcache: %llx",ptrval);}
  if (tc == NULL) tc = kno_new_thread_cache();
  if (kno_threadcache) {
    struct KNO_THREAD_CACHE *oldtc = kno_threadcache;
    oldtc->threadcache_inuse--;
    if (oldtc->threadcache_inuse<=0) kno_free_thread_cache(oldtc);}
  tc->threadcache_inuse++;
#if KNO_USE_TLS
  u8_tld_set(kno_threadcache_key,tc);
#else
  kno_threadcache = tc;
#endif
  return tc;
}

KNO_EXPORT kno_thread_cache kno_use_threadcache()
{
  if (kno_threadcache) return NULL;
  else {
    struct KNO_THREAD_CACHE *tc = kno_new_thread_cache();
    tc->threadcache_prev = NULL; tc->threadcache_inuse++;
#if KNO_USE_TLS
    u8_tld_set(kno_threadcache_key,tc);
#else
    kno_threadcache = tc;
#endif
    return tc;}
}

/* Initialization stuff */

KNO_EXPORT void kno_init_threadcache_c()
{
  u8_register_source_file(_FILEINFO);
#if (KNO_USE_TLS)
  u8_new_threadkey(&kno_threadcache_key,NULL);
#endif
}

