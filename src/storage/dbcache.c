/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
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

#include <libu8/u8printf.h>

/* TODO: new design

   dbcaches are conses and include pointers to the pthread_t and
   threadid (long long) values for the threads where they are
   bound. It's possible to move dbcaches between threads.

   Currently the `prev` pointer in a dbcache refers to the previous
   pointer on the stack. This is to avoid keep track of threadache binding,
   but all the places where that happens could do it directly if the function
   kno_set_threadcache returned the threadache being displaced.

 */

static u8_condition FreeingForeignThreadCache=
  _("Attempt to free foreign threadcache");
static u8_condition FreeingInUseThreadCache=
  _("Attempt to free in-use threadcache");
static u8_condition PushingInUseThreadCache=
  _("Attempt to push in-use threadcache");
static u8_condition SettingInUseThreadCache=
  _("Attempt to set in-use threadcache");

#if (KNO_USE__THREAD)
__thread struct KNO_CACHE *kno_threadcache = NULL;
#elif (KNO_USE_TLS)
u8_tld_key kno_threadcache_key;
#else
struct KNO_CACHE *kno_threadcache = NULL;
#endif

KNO_EXPORT int kno_free_dbcache(struct KNO_CACHE *tc)
{
  KNO_INTPTR ptrval = (KNO_INTPTR) tc;
  if (tc->knocache_inuse) {
    if (tc->knocache_id)
      u8_logf(LOG_WARN,FreeingInUseThreadCache,
              "Freeing in-use threadcache: %llx (%s)",ptrval,tc->knocache_id);
    else u8_logf(LOG_WARN,FreeingInUseThreadCache,
                 "Freeing in-use threadcache: %llx",ptrval);}
  kno_recycle_hashtable(&(tc->oids));
  kno_recycle_hashtable(&(tc->background_cache));
  kno_free_slotmap(&(tc->calls));
  kno_free_slotmap(&(tc->adjuncts));
  kno_free_slotmap(&(tc->indexes));
  tc->knocache_inuse = 0;
  u8_free(tc);
  return 1;
}

KNO_EXPORT void recycle_dbcache(struct KNO_RAW_CONS *c)
{
  kno_free_dbcache((struct KNO_CACHE *)c);
}

KNO_EXPORT int kno_pop_threadcache(struct KNO_CACHE *tc)
{
  if (tc == NULL) return 0;
  else if (tc!=kno_threadcache) {
    u8_seterr(FreeingForeignThreadCache,"kno_pop_threadcache",
              ((tc->knocache_id)?(u8_strdup(tc->knocache_id)):(NULL)));
    return -1;}
  else {
    struct KNO_CACHE *prev = tc->knocache_prev;
#if KNO_USE_TLS
    u8_tld_set(kno_threadcache_key,prev);
#else
    kno_threadcache = prev;
#endif
    tc->knocache_inuse--;
    if (tc->knocache_inuse<=0) kno_free_dbcache(tc);
    return 1;}
}

/* Making DB caches */

KNO_EXPORT kno_cache kno_cons_dbcache(int oids_size,int bg_size)
{
  struct KNO_CACHE *tc = u8_alloc(struct KNO_CACHE);

  KNO_INIT_CONS(tc,kno_dbcache_type);

  tc->knocache_inuse = 0; tc->knocache_id = NULL;

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

  tc->knocache_prev = NULL;
  return tc;
}

KNO_EXPORT kno_cache kno_new_dbcache()
{
  return kno_cons_dbcache
    (KNO_THREAD_OIDCACHE_SIZE,KNO_THREAD_BGCACHE_SIZE);
}

static int unparse_dbcache(u8_output out,lispval x)
{
  struct KNO_CACHE *dbc = (struct KNO_CACHE *) x;
  u8_printf(out,"#<dbcache '%s' (%d)",dbc->knocache_id,dbc->knocache_inuse);
  u8_printf(out," #!%p>",dbc);
  return 1;
}

KNO_EXPORT kno_cache kno_push_threadcache(struct KNO_CACHE *tc)
{
  KNO_INTPTR ptrval = (KNO_INTPTR) tc;
  if ((tc)&&(tc->knocache_inuse)) {
    if (tc->knocache_id)
      u8_logf(LOG_WARN,PushingInUseThreadCache,
              "Pushing in-use threadcache: %llx (%s)",ptrval,tc->knocache_id);
    else u8_logf(LOG_WARN,PushingInUseThreadCache,
                 "Pushing in-use threadcache: %llx",ptrval);}
  if (tc == NULL) tc = kno_new_dbcache();
  if (tc == kno_threadcache) return tc;
  tc->knocache_prev = kno_threadcache;
#if KNO_USE_TLS
  u8_tld_set(kno_threadcache_key,tc);
#else
  kno_threadcache = tc;
#endif
  tc->knocache_inuse++;
  return tc;
}

KNO_EXPORT kno_cache kno_set_threadcache(struct KNO_CACHE *tc)
{
  KNO_INTPTR ptrval = (KNO_INTPTR) tc;
  if ((tc)&&(tc->knocache_inuse)) {
    if (tc->knocache_id)
      u8_logf(LOG_WARN,SettingInUseThreadCache,
              "Setting in-use threadcache: %llx (%s)",ptrval,tc->knocache_id);
    else u8_logf(LOG_WARN,SettingInUseThreadCache,
                 "Setting in-use threadcache: %llx",ptrval);}
  if (tc == NULL) tc = kno_new_dbcache();
  if (kno_threadcache) {
    struct KNO_CACHE *oldtc = kno_threadcache;
    oldtc->knocache_inuse--;
    if (oldtc->knocache_inuse<=0) kno_free_dbcache(oldtc);}
  tc->knocache_inuse++;
#if KNO_USE_TLS
  u8_tld_set(kno_threadcache_key,tc);
#else
  kno_threadcache = tc;
#endif
  return tc;
}

KNO_EXPORT kno_cache kno_use_threadcache()
{
  if (kno_threadcache) return NULL;
  else {
    struct KNO_CACHE *tc = kno_new_dbcache();
    tc->knocache_prev = NULL; tc->knocache_inuse++;
#if KNO_USE_TLS
    u8_tld_set(kno_threadcache_key,tc);
#else
    kno_threadcache = tc;
#endif
    return tc;}
}

/* Operations */

KNO_EXPORT int knocache_cache_oid_value(KNOCACHE *cache,lispval oid,lispval val)
{
  if (cache == NULL) return 0;
  return kno_hashtable_store(&(cache->oids),oid,val);
}

KNO_EXPORT int knocache_cache_adjunct_value(KNOCACHE *cache,lispval oid,lispval slotid,lispval val)
{
  if (cache == NULL) return 0;
  lispval oidcache = kno_slotmap_get(&(cache->adjuncts),slotid,KNO_VOID);
  if (KNO_VOIDP(oidcache)) {
    oidcache = kno_make_hashtable(NULL,117);
    kno_slotmap_store(&(cache->adjuncts),slotid,oidcache);
    int rv = kno_store(oidcache,oid,val);
    kno_decref(oidcache);
    return rv;}
  else return kno_store(oidcache,oid,val);
}

KNO_EXPORT int knocache_cache_index_key(KNOCACHE *cache,lispval ix_arg,lispval key,lispval oids)
{
  if (cache == NULL) return 0;
  lispval index_cache = kno_slotmap_get(&(cache->indexes),ix_arg,KNO_VOID);
  if (KNO_VOIDP(index_cache)) {
    index_cache = kno_make_hashtable(NULL,117);
    kno_slotmap_store(&(cache->indexes),ix_arg,index_cache);
    int rv = kno_store(index_cache,key,oids);
    kno_decref(index_cache);
    return rv;}
  else return kno_store(index_cache,key,oids);
}

int knocache_bgadd(KNOCACHE *cache,lispval key,lispval oids)
{
  return kno_hashtable_add(&(cache->background_cache),key,oids);
}

/* Initialization stuff */

KNO_EXPORT void kno_init_dbcache_c()
{
  kno_recyclers[kno_dbcache_type]=recycle_dbcache;
  kno_unparsers[kno_dbcache_type]=unparse_dbcache;

  u8_register_source_file(_FILEINFO);
#if (KNO_USE_TLS)
  u8_new_threadkey(&kno_threadcache_key,NULL);
#endif
}

