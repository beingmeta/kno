/* -*- Mode: C; Character-encoding: utf-8; fill-column: 95; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_INDEXES 1
#define KNO_INLINE_CHOICES KNO_DO_INLINE
#define KNO_INLINE_IPEVAL KNO_DO_INLINE
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/indexes.h"
#include "kno/drivers.h"
#include "kno/apply.h"

#include <libu8/libu8.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>

u8_condition kno_EphemeralIndex=_("ephemeral index");
u8_condition kno_ReadOnlyIndex=_("read-only index");
u8_condition kno_NoFileIndexes=_("file indexes are not supported");
u8_condition kno_NotAFileIndex=_("not a file index");
u8_condition kno_BadIndexSpec=_("bad index specification");
u8_condition kno_CorruptedIndex=_("corrupted index file");
u8_condition kno_IndexCommitError=_("can't save changes to index");

lispval kno_index_hashop, kno_index_slotsop, kno_index_bucketsop;

u8_condition kno_IndexCommit=_("Index/Commit");
static u8_condition ipeval_ixfetch="IXFETCH";

#define htget(ht,key)                                                   \
  ( (ht->table_n_keys) ? (kno_hashtable_get(ht,key,EMPTY)) : (EMPTY) )

#define getloglevel(ix) ()
#define elapsed_time(since) (u8_elapsed_time()-(since))

static int index_loglevel(kno_index ix)
{
  return (ix->index_loglevel > 0) ? (ix->index_loglevel) :
    (kno_storage_loglevel);
}

static lispval edit_result(lispval key,lispval result,
                           kno_hashtable adds,kno_hashtable drops)
{
  lispval added = htget(adds,key);
  CHOICE_ADD(result,added);
  if (drops->table_n_keys) {
    lispval dropped = kno_hashtable_get(drops,key,EMPTY);
    if (!(EMPTYP(dropped))) {
      lispval new_result = kno_difference(result,dropped);
      kno_decref(dropped);
      kno_decref(result);
      result=new_result;}}
  return result;
}

int kno_index_cache_init    = KNO_INDEX_CACHE_INIT;
int kno_index_adds_init     = KNO_INDEX_ADDS_INIT;
int kno_index_drops_init    = KNO_INDEX_DROPS_INIT;
int kno_index_replace_init  = KNO_INDEX_REPLACE_INIT;

static u8_rwlock indexes_lock;
kno_index kno_primary_indexes[KNO_N_PRIMARY_INDEXES];
int kno_n_primary_indexes = 0;
int kno_n_secondary_indexes = 0;
static int secondary_indexes_len = 0;
static kno_index *secondary_indexes = NULL;

static kno_index *consed_indexes = NULL;
ssize_t n_consed_indexes = 0, consed_indexes_len=0;
u8_mutex consed_indexes_lock;

static int add_consed_index(kno_index ix)
{
  u8_lock_mutex(&consed_indexes_lock);
  kno_index *scan=consed_indexes, *limit=scan+n_consed_indexes;
  while (scan<limit) {
    if ( *scan == ix ) {
      u8_unlock_mutex(&consed_indexes_lock);
      return 0;}
    else scan++;}
  if (n_consed_indexes>=consed_indexes_len) {
    ssize_t new_len=consed_indexes_len+64;
    kno_index *newvec=u8_realloc(consed_indexes,new_len*sizeof(kno_index));
    if (newvec) {
      consed_indexes=newvec;
      consed_indexes_len=new_len;}
    else {
      u8_seterr(kno_MallocFailed,"add_consed_index",u8dup(ix->indexid));
      u8_unlock_mutex(&consed_indexes_lock);
      return -1;}}
  kno_incref((lispval)ix);
  consed_indexes[n_consed_indexes++]=ix;
  u8_unlock_mutex(&consed_indexes_lock);
  return 1;
}

static int drop_consed_index(kno_index ix)
{
  u8_lock_mutex(&consed_indexes_lock);
  kno_index *scan=consed_indexes, *limit=scan+n_consed_indexes;
  while (scan<limit) {
    if ( *scan == ix ) {
      size_t to_shift=(limit-scan)-1;
      memmove(scan,scan+1,to_shift);
      n_consed_indexes--;
      u8_unlock_mutex(&consed_indexes_lock);
      return 1;}
    else scan++;}
  u8_unlock_mutex(&consed_indexes_lock);
  return 0;
}

static struct KNO_AGGREGATE_INDEX _background;
struct KNO_AGGREGATE_INDEX *kno_background = &_background;
static u8_mutex background_lock;

#if KNO_GLOBAL_IPEVAL
static lispval *index_delays;
#elif KNO_USE__THREAD
static __thread lispval *index_delays;
#elif KNO_USE_TLS
static u8_tld_key index_delays_key;
#else
static lispval *index_delays;
#endif

#if ((KNO_USE_TLS) && (!(KNO_GLOBAL_IPEVAL)))
#define get_index_delays()                      \
  ((lispval *)u8_tld_get(index_delays_key))
KNO_EXPORT void kno_init_index_delays()
{
  lispval *delays = (lispval *)u8_tld_get(index_delays_key);
  if (delays) return;
  else {
    int i = 0;
    delays = u8_alloc_n(KNO_N_INDEX_DELAYS,lispval);
    while (i<KNO_N_INDEX_DELAYS) delays[i++]=EMPTY;
    u8_tld_set(index_delays_key,delays);}

}
#else
#define get_index_delays() (index_delays)
KNO_EXPORT void kno_init_index_delays()
{
  if (index_delays) return;
  else {
    int i = 0;
    index_delays = u8_alloc_n(KNO_N_INDEX_DELAYS,lispval);
    while (i<KNO_N_INDEX_DELAYS) index_delays[i++]=EMPTY;}

}
#endif

static void clear_bg_cache(lispval key)
{
  kno_hashtable bg_cache = &(kno_background->index_cache);
  if (bg_cache->table_n_keys == 0) {}
  else if (KNO_CHOICEP(key)) {
    const lispval *keys = KNO_CHOICE_DATA(key);
    unsigned int n = KNO_CHOICE_SIZE(key);
    kno_hashtable_iterkeys
      (bg_cache,kno_table_replace,n,keys,VOID);}
  else kno_hashtable_op(bg_cache,kno_table_replace,key,VOID);
}

KNO_EXPORT lispval *kno_get_index_delays() { return get_index_delays(); }

/* Index ops */

KNO_EXPORT lispval kno_index_ctl(kno_index x,lispval indexop,int n,lispval *args)
{
  struct KNO_INDEX_HANDLER *h = x->index_handler;
  if (h->indexctl)
    return h->indexctl(x,indexop,n,args);
  else return KNO_FALSE;
}

/* Index cache levels */

/* a cache_level<0 indicates no caching has been done */

KNO_EXPORT void kno_index_setcache(kno_index ix,int level)
{
  lispval intarg = KNO_INT(level);
  lispval result = kno_index_ctl(ix,kno_cachelevel_op,1,&intarg);
  if (KNO_ABORTP(result)) {kno_clear_errors(1);}
  kno_decref(result);
  ix->index_cache_level = level;
}

static void init_cache_level(kno_index ix)
{
  if (PRED_FALSE(ix->index_cache_level<0)) {
    lispval opts = ix->index_opts;
    long long level=kno_getfixopt(opts,"CACHELEVEL",kno_default_cache_level);
    kno_index_setcache(ix,level);}
}

/* The index registry */

KNO_EXPORT void kno_register_index(kno_index ix)
{
  lispval keyslotid = ix->index_keyslot;
  if (KNO_NULLP(keyslotid))
    keyslotid = KNO_VOID;
  if ( (KNO_VOIDP(keyslotid)) && (ix->index_metadata.n_slots) )
    keyslotid = kno_slotmap_get(&(ix->index_metadata),KNOSYM_KEYSLOT,KNO_VOID);
  if (KNO_VOIDP(keyslotid)) {}
  else if ( (KNO_SYMBOLP(keyslotid)) || (KNO_OIDP(keyslotid)) ) {}
  else if (KNO_FALSEP(keyslotid))
    keyslotid = KNO_VOID;
  else if (KNO_CHOICEP(keyslotid)) {
    lispval new_keyslotid = KNO_EMPTY;
    KNO_DO_CHOICES(slotid,keyslotid) {
      if ( (KNO_SYMBOLP(slotid)) || (KNO_OIDP(slotid)) ) {
        CHOICE_ADD(new_keyslotid,slotid);}
      else u8_log(LOGWARN,"InvalidKeySlotID",
                  "The value %q isn't a valid keyslot value",
                  slotid);}
    kno_decref(keyslotid);
    keyslotid = kno_simplify_choice(new_keyslotid);}
  else {
    u8_log(LOGWARN,"InvalidKeySlotID",
           "The value %q isn't a valid keyslotid",
           keyslotid);
    kno_decref(keyslotid);
    keyslotid = KNO_VOID;}
  ix->index_keyslot = keyslotid;
  if (ix->index_flags&KNO_STORAGE_UNREGISTERED)
    return;
  else if (ix->index_serialno<0) {
    u8_write_lock(&indexes_lock);
    if (ix->index_serialno>=0) { /* Handle race condition */
      u8_rw_unlock(&indexes_lock);
      return;}
    if (kno_n_primary_indexes<KNO_N_PRIMARY_INDEXES) {
      ix->index_serialno = kno_n_primary_indexes;
      kno_primary_indexes[kno_n_primary_indexes++]=ix;}
    else {
      if (kno_n_secondary_indexes >= secondary_indexes_len) {
        secondary_indexes_len = secondary_indexes_len*2;
        secondary_indexes = u8_realloc_n
          (secondary_indexes,secondary_indexes_len,kno_index);}
      secondary_indexes[kno_n_secondary_indexes++]=ix;}
    KNO_SET_REFCOUNT(ix,0);}
  /* Make it a static cons */
  u8_rw_unlock(&indexes_lock);
  if ((ix->index_flags)&(KNO_INDEX_IN_BACKGROUND))
    kno_add_to_background(ix);
}

KNO_EXPORT lispval _kno_index2lisp(kno_index ix)
{
  return kno_index2lisp(ix);
}

KNO_EXPORT kno_index _kno_indexptr(lispval lix)
{
  return kno_indexptr(lix);
}

KNO_EXPORT lispval kno_index_ref(kno_index ix)
{
  if (ix == NULL)
    return KNO_ERROR;
  else if (ix->index_serialno>=0)
    return LISPVAL_IMMEDIATE(kno_index_type,ix->index_serialno);
  else {
    lispval lix=(lispval)ix;
    kno_incref(lix);
    return lix;}
}
KNO_EXPORT kno_index kno_lisp2index(lispval lix)
{
  if (KNO_ABORTP(lix))
    return NULL;
  else if (TYPEP(lix,kno_index_type)) {
    int serial = KNO_GET_IMMEDIATE(lix,kno_index_type);
    if (serial<KNO_N_PRIMARY_INDEXES)
      return kno_primary_indexes[serial];
    else if ( (kno_n_secondary_indexes == 0) ||
              (serial >= (kno_n_secondary_indexes+KNO_N_PRIMARY_INDEXES)) )
      return NULL;
    else {
      u8_read_lock(&indexes_lock);
      kno_index index = secondary_indexes[serial-KNO_N_PRIMARY_INDEXES];
      u8_rw_unlock(&indexes_lock);
      return index;}}
  else if (TYPEP(lix,kno_consed_index_type))
    return (kno_index) lix;
  else return KNO_ERR(NULL,kno_TypeError,_("not an index"),NULL,lix);
}

KNO_EXPORT kno_index _kno_ref2index(lispval indexval)
{
  if (KNO_ABORTP(indexval))
    return NULL;
  else if (TYPEP(indexval,kno_index_type)) {
    int serial = KNO_GET_IMMEDIATE(indexval,kno_index_type);
    if (serial<KNO_N_PRIMARY_INDEXES)
      return kno_primary_indexes[serial];
    else if ( (kno_n_secondary_indexes == 0) ||
              (serial >= (kno_n_secondary_indexes+KNO_N_PRIMARY_INDEXES)) )
      return NULL;
    else {
      u8_read_lock(&indexes_lock);
      kno_index index = secondary_indexes[serial-KNO_N_PRIMARY_INDEXES];
      u8_rw_unlock(&indexes_lock);
      return index;}}
  else return NULL;
}

/* Finding indexes by ids/sources/etc */

KNO_EXPORT kno_index kno_find_index(u8_string spec)
{
  kno_index ix=kno_find_index_by_source(spec);
  if (ix) return ix;
  else if ((ix=kno_find_index_by_id(spec)))
    return ix;
  /* TODO: Add generic method which uses the index matcher
     methods to find indexes */
  else return ix;
}
KNO_EXPORT u8_string kno_locate_index(u8_string spec)
{
  kno_index ix = kno_find_index_by_source(spec);
  if (ix) return u8_strdup(ix->index_source);
  else if ((ix=kno_find_index_by_id(spec)))
    return u8_strdup(ix->index_source);
  /* TODO: Add generic method which uses the index matcher
     methods to find indexes */
  else return NULL;
}

static int match_index_source(kno_index ix,u8_string source)
{
  return ( (kno_same_sourcep(source,ix->index_source)) ||
           (kno_same_sourcep(source,ix->canonical_source)) );
}

static int match_index_id(kno_index ix,u8_string id)
{
  return ((id)&&(ix->indexid)&&
          (strcmp(ix->indexid,id) == 0));
}

KNO_EXPORT kno_index kno_find_index_by_id(u8_string id)
{
  int i = 0;
  if (id == NULL)
    return NULL;
  else while (i<kno_n_primary_indexes)
         if (match_index_id(kno_primary_indexes[i],id))
           return kno_primary_indexes[i];
         else i++;
  if (secondary_indexes == NULL)
    return NULL;
  u8_read_lock(&indexes_lock);
  i = 0; while (i<kno_n_secondary_indexes)
           if (match_index_id(secondary_indexes[i],id)) {
             kno_index ix = secondary_indexes[i];
             u8_rw_unlock(&indexes_lock);
             return ix;}
           else i++;
  u8_rw_unlock(&indexes_lock);
  return NULL;
}
KNO_EXPORT kno_index kno_find_index_by_source(u8_string source)
{
  int i = 0;
  if (source == NULL)
    return NULL;
  else while (i<kno_n_primary_indexes)
         if (match_index_source(kno_primary_indexes[i],source))
           return kno_primary_indexes[i];
         else i++;
  if (secondary_indexes == NULL)
    return NULL;
  u8_read_lock(&indexes_lock);
  i = 0; while (i<kno_n_secondary_indexes)
           if (match_index_source(secondary_indexes[i],source)) {
             kno_index ix = secondary_indexes[i];
             u8_rw_unlock(&indexes_lock);
             return ix;}
           else i++;
  u8_rw_unlock(&indexes_lock);
  return NULL;
}

KNO_EXPORT kno_index kno_get_index
(u8_string spec,kno_storage_flags flags,lispval opts)
{
  if (strchr(spec,';')) {
    kno_index ix = NULL;
    u8_byte *copy = u8_strdup(spec);
    u8_byte *start = copy, *brk = strchr(start,';');
    while (brk) {
      if (ix == NULL) {
        *brk='\0'; ix = kno_open_index(start,flags,opts);
        if (ix) {
          brk = NULL;
          start = NULL;}
        else {
          start = brk+1;
          brk = strchr(start,';');}}}
    if (ix)
      return ix;
    else if ((start)&&(*start)) {
      int start_off = start-copy;
      u8_free(copy);
      return kno_open_index(spec+start_off,flags,opts);}
    else return NULL;}
  else return kno_open_index(spec,flags,opts);
}

/* Background indexes */

KNO_EXPORT int kno_add_to_background(kno_index ix)
{
  if (ix == NULL) return 0;
  if (ix->index_serialno<0) {
    lispval lix = (lispval)ix;
    kno_incref(lix);}
  u8_lock_mutex(&background_lock);
  ix->index_flags = ix->index_flags|KNO_INDEX_IN_BACKGROUND;
  kno_add_to_aggregate_index(kno_background,ix);
  u8_string old_id = kno_background->indexid;
  kno_background->indexid =
    u8_mkstring("background+%d",kno_background->ax_n_indexes);
  if (old_id) u8_free(old_id);

  u8_unlock_mutex(&background_lock);
  return 1;
}

KNO_EXPORT kno_index kno_use_index(u8_string spec,
                                kno_storage_flags flags,
                                lispval opts)
{
  flags&=~KNO_STORAGE_UNREGISTERED;
  if (strchr(spec,';')) {
    kno_index ix = NULL;
    u8_byte *copy = u8_strdup(spec);
    u8_byte *start = copy, *end = strchr(start,';');
    *end='\0'; while (start) {
      ix = kno_get_index(start,flags,opts);
      if (ix == NULL) {
        u8_free(copy); return NULL;}
      else kno_add_to_background(ix);
      if ((end) && (end[1])) {
        start = end+1; end = strchr(start,';');
        if (end) *end='\0';}
      else start = NULL;}
    u8_free(copy);
    return ix;}
  else {
    kno_index ix = kno_get_index(spec,flags,opts);
    if (ix) kno_add_to_background(ix);
    return ix;}
}

/* Core functions */

static void delay_index_fetch(kno_index ix,lispval keys);

KNO_EXPORT lispval kno_index_fetch(kno_index ix,lispval key)
{
  lispval result = EMPTY;
  int read_only = KNO_INDEX_READONLYP(ix);
  kno_hashtable cache = &(ix->index_cache);
  kno_hashtable adds = &(ix->index_adds);
  kno_hashtable drops = &(ix->index_drops);
  kno_hashtable stores = &(ix->index_stores);
  lispval local_value = (stores->table_n_keys) ?
    (kno_hashtable_get(stores,key,VOID)) : (VOID);
  if (!(KNO_VOIDP(local_value)))
    result=local_value;
  else if (ix->index_handler->fetch) {
    init_cache_level(ix);
    if ((kno_ipeval_status())&&
        ((ix->index_handler->prefetch)||
         (ix->index_handler->fetchn)))  {
      delay_index_fetch(ix,key);
      return EMPTY;}
    else result=ix->index_handler->fetch(ix,key);}
  else {}
  if (! read_only )
    result = edit_result(key,result,adds,drops);
  if (ix->index_cache_level>0) kno_hashtable_store(cache,key,result);
  return result;
}

static void delay_index_fetch(kno_index ix,lispval keys)
{
  struct KNO_HASHTABLE *cache = &(ix->index_cache); int delay_count = 0;
  lispval *delays = get_index_delays(), *delayp = &(delays[ix->index_serialno]);
  DO_CHOICES(key,keys) {
    if (kno_hashtable_probe(cache,key)) {}
    else {
      kno_incref(key);
      CHOICE_ADD((*delayp),key);
      delay_count++;}}
  if (delay_count) (void)kno_ipeval_delay(delay_count);
}

KNO_EXPORT void kno_delay_index_fetch(kno_index ix,lispval keys)
{
  delay_index_fetch(ix,keys);
}

static void cleanup_tmpchoice(struct KNO_CHOICE *reduced)
{
  int refcount = KNO_CONS_REFCOUNT(reduced);
  if (refcount>1) {
    lispval lptr = (lispval) reduced;
    lispval *vals = (lispval *) KNO_XCHOICE_ELTS(reduced);
    /* We need to incref these values for wherever
       delay_index_fetch stores them. */
    kno_incref_vec(vals,KNO_XCHOICE_SIZE(reduced));
    kno_incref(lptr);}
  else kno_free_choice(reduced);
}

KNO_EXPORT int kno_index_prefetch(kno_index ix,lispval keys)
{
  // KNOTC *knotc = ((KNO_USE_THREADCACHE)?(kno_threadcache):(NULL));
  struct KNO_HASHTABLE *cache = &(ix->index_cache);
  struct KNO_HASHTABLE *adds = &(ix->index_adds);
  struct KNO_HASHTABLE *drops = &(ix->index_drops);
  struct KNO_HASHTABLE *stores = &(ix->index_stores);
  int n_fetched = 0, cachelevel = 0, rv =0;
  int read_only = (ix->index_flags & (KNO_STORAGE_READ_ONLY) );
  struct KNO_CHOICE *reduced = NULL;
  lispval needed = EMPTY;

  if (ix == NULL) return -1;
  else if (KNO_EMPTYP(keys)) return 0;
  else init_cache_level(ix);

  cachelevel = ix->index_cache_level;

  if (cachelevel < 1) return 0;

  int decref_keys = 0;
  if (KNO_PRECHOICEP(keys)) {
    keys = kno_make_simple_choice(keys);
    if (KNO_EMPTYP(keys)) return 0;
    decref_keys=1;}

  /* Handle non-choice, where you can either return or needed  = keys */
  if (!(KNO_CHOICEP(keys))) {
    if (kno_hashtable_probe(cache,keys)) {
      if (decref_keys) kno_decref(keys);
      return 0;}
    else if (ix->index_handler->prefetch != NULL) {
      /* Prefetch handlers need to consult adds, drops, stores
         directly and also write directly into the cache */
      int rv = 0;
      if (kno_ipeval_status())
        delay_index_fetch(ix,keys);
      else rv = ix->index_handler->prefetch(ix,keys);
      if (decref_keys) kno_decref(keys);
      return rv;}
    else needed=keys;}
  else {
    /* Compute needed keys */
    int n = KNO_CHOICE_SIZE(keys);
    const lispval *keyv = KNO_CHOICE_DATA(keys);
    int partial = kno_hashtable_iterkeys(cache,kno_table_haskey,n,keyv,VOID);
    if (partial) {
      /* Some of the values are already in the cache */
      int atomicp = 1;
      reduced = kno_alloc_choice(n);
      lispval *elts = ((lispval *)(KNO_CHOICE_ELTS(reduced))), *write = elts;
      KNO_DO_CHOICES(key,keys) {
        if (! (kno_hashtable_probe(cache,key)) ) {
          if (kno_hashtable_probe(stores,key)) {
            lispval stored = kno_hashtable_get(stores,key,EMPTY);
            lispval local_value = edit_result(key,stored,adds,drops);
            kno_hashtable_op(cache,kno_table_store_noref,key,local_value);
            /* Not really fetched, but we count it anyway */
            n_fetched++;}
          else {
            if ( (atomicp) && (KNO_CONSP(key)) ) atomicp = 0;
            *write++=key;
            kno_incref(key);}}}
      if (write == elts) {
        u8_big_free(reduced); reduced=NULL;
        if (decref_keys) kno_decref(keys);
        return n_fetched;}
      else {
        KNO_INIT_XCHOICE(reduced,write-elts,atomicp);
        needed = (lispval) reduced;}}
    else needed = keys;}

  if (ix->index_handler->prefetch != NULL) {
    /* Prefetch handlers need to consult adds, drops, stores
       directly and also write directly into the cache */
    if (kno_ipeval_status())
      delay_index_fetch(ix,needed);
    else rv = ix->index_handler->prefetch(ix,needed);
    if (reduced) {cleanup_tmpchoice(reduced); reduced=NULL;}
    if (decref_keys) kno_decref(keys);
    if (rv>=0) rv = n_fetched + rv;}
  else if (ix->index_handler->fetchn == NULL) {
    /* No fetchn handler, fetch them individually */
    if (kno_ipeval_status()) {
      delay_index_fetch(ix,needed);
      rv = n_fetched;}
    else {
      DO_CHOICES(key,needed) {
        lispval v = kno_index_fetch(ix,key);
        if (KNO_ABORTP(v)) {
          KNO_STOP_DO_CHOICES;
          if (reduced) {cleanup_tmpchoice(reduced); reduced=NULL;}
          if (decref_keys) kno_decref(keys);
          return kno_interr(v);}
        if (! read_only ) v = edit_result(key,v,adds,drops);
        kno_hashtable_op(cache,kno_table_store_noref,key,v);
        /* if (knotc) knotc_store(ix,key,v); */
        n_fetched++;}
      rv=n_fetched;}}
  else if (kno_ipeval_status()) {
    delay_index_fetch(ix,needed);
    rv = n_fetched;}
  else if (KNO_CHOICEP(needed)) {
    const lispval *to_fetch = KNO_CHOICE_DATA(needed);
    int n_to_fetch = KNO_CHOICE_SIZE(needed);
    lispval *fetched = ix->index_handler->fetchn(ix,n_to_fetch,to_fetch);
    if (fetched) {
      if ( ! read_only ) {
        int scan_i = 0; while (scan_i < n_to_fetch) {
          lispval key = to_fetch[scan_i], v = fetched[scan_i];
          lispval added = htget(adds,key), dropped = htget(drops,key);
          if (! ( (EMPTYP(added)) && (EMPTYP(dropped)) ) ) {
            CHOICE_ADD(v,added);
            if (!(EMPTYP(dropped))) {
              lispval newv = kno_difference(v,dropped);
              kno_decref(v);
              v=newv;}
            fetched[scan_i] = v;}
          scan_i++;}}
      kno_hashtable_iter(cache,kno_table_store_noref,n_to_fetch,to_fetch,fetched);
      u8_big_free(fetched);
      rv = n_fetched+n_to_fetch;}
    else rv = -1;}
  else {
    lispval fetched = ix->index_handler->fetch(ix,needed);
    lispval val = edit_result(needed,fetched,adds,drops);
    if (!(KNO_ABORTED(val))) {
      kno_hashtable_store(cache,needed,val);
      /* knotc_store(ix,needed,val); */
      kno_decref(val);
      rv = n_fetched+1;}
    else rv=-1;}
    if (reduced) {cleanup_tmpchoice(reduced); reduced=NULL;}
  if (decref_keys) kno_decref(keys);
  return rv;
}

KNO_EXPORT lispval kno_index_fetchn(kno_index ix,lispval keys_arg)
{
  if (ix==NULL) return KNO_EMPTY;
  else if ((ix->index_handler) && (ix->index_handler->fetchn)) {
    lispval keys = kno_make_simple_choice(keys_arg);
    if (KNO_VECTORP(keys)) {
      int n = KNO_VECTOR_LENGTH(keys);
      lispval *keyv = KNO_VECTOR_ELTS(keys);
      lispval *values = ix->index_handler->fetchn(ix,n,keyv);
      kno_decref(keys);
      return kno_cons_vector(NULL,n,1,values);}
    else {
      int n = KNO_CHOICE_SIZE(keys);
      if (n==0)
        return kno_make_hashtable(NULL,8);
      else if (n==1) {
        lispval table=kno_make_hashtable(NULL,16);
        lispval value = kno_index_fetch(ix,keys);
        kno_store(table,keys,value);
        kno_decref(value);
        kno_decref(keys);
        return table;}
      else {
        init_cache_level(ix);
        const lispval *keyv=KNO_CHOICE_DATA(keys);
        const lispval *values=ix->index_handler->fetchn(ix,n,(lispval *)keyv);
        lispval table=kno_make_hashtable(NULL,n*2);
        kno_hashtable_iter((kno_hashtable)table,kno_table_store_noref,n,
                          keyv,values);
        u8_big_free((lispval *)values);
        kno_decref(keys);
        return table;}}}
  else if (ix->index_handler) {
    u8_seterr("NoHandler","kno_index_fetchn",u8_strdup(ix->indexid));
    return -1;}
  else return 0;
}

static int add_key_fn(lispval key,lispval val,void *data)
{
  lispval **write = (lispval **)data;
  *((*write)++) = kno_incref(key);
  /* Means keep going */
  return 0;
}

static int edit_key_fn(lispval key,lispval val,void *data)
{
  lispval **write = (lispval **)data;
  if (PAIRP(key)) {
    struct KNO_PAIR *pair = (struct KNO_PAIR *)key;
    if (pair->car == KNOSYM_SET) {
      lispval real_key = pair->cdr;
      kno_incref(real_key);
      *((*write)++) = real_key;}}
  /* Means keep going */
  return 0;
}

KNO_EXPORT lispval kno_index_keys(kno_index ix)
{
  if (ix->index_handler->fetchkeys) {
    int n_fetched = 0; init_cache_level(ix);
    lispval *fetched = ix->index_handler->fetchkeys(ix,&n_fetched);
    if (n_fetched < 0) return KNO_ERROR;
    int n_replaced = ix->index_stores.table_n_keys;
    int n_added = ix->index_adds.table_n_keys;
    if (n_fetched == 0) {
      if ( (n_replaced) && (n_added) ) {
        lispval keys = kno_hashtable_keys(&(ix->index_adds));
        lispval replaced =  kno_hashtable_keys(&(ix->index_stores));
        CHOICE_ADD(keys,replaced);
        return kno_simplify_choice(keys);}
      else if (n_added)
        return kno_hashtable_keys(&(ix->index_adds));
      else if (n_replaced)
        return kno_hashtable_keys(&(ix->index_stores));
      else return EMPTY;}
    else {
      kno_choice result = kno_alloc_choice(n_fetched+n_added+n_replaced);
      lispval *write_start, *write_at;
      if (n_fetched)
        memcpy(&(result->choice_0),fetched,LISPVEC_BYTELEN(n_fetched));
      write_start = &(result->choice_0);
      write_at = write_start+n_fetched;
      if (n_added) kno_for_hashtable(&(ix->index_adds),add_key_fn,&write_at,1);
      if (n_replaced) kno_for_hashtable(&(ix->index_stores),edit_key_fn,&write_at,1);
      u8_big_free(fetched);
      return kno_init_choice(result,write_at-write_start,NULL,
                            KNO_CHOICE_DOSORT|KNO_CHOICE_REALLOC);}}
  else return kno_err(kno_NoMethod,"kno_index_keys",NULL,kno_index2lisp(ix));
}

static int copy_value_sizes(lispval key,lispval value,void *vptr)
{
  struct KNO_HASHTABLE *sizes = (struct KNO_HASHTABLE *)vptr;
  int value_size = ((VOIDP(value)) ? (0) : KNO_CHOICE_SIZE(value));
  kno_hashtable_store(sizes,key,KNO_INT(value_size));
  return 0;
}

KNO_EXPORT lispval kno_index_keysizes(kno_index ix,lispval for_keys)
{
  if (ix->index_handler->fetchinfo) {
    lispval keys=for_keys;
    int n_fetched = 0, decref_keys=0;
    struct KNO_CHOICE *filter=NULL;
    if (PRECHOICEP(for_keys)) {
      keys=kno_make_simple_choice(for_keys);
      decref_keys=1;}
    if (EMPTYP(keys))
      return EMPTY;
    else if ( (VOIDP(keys)) || (keys == KNO_DEFAULT_VALUE) )
      filter=NULL;
    else if (!(CHOICEP(keys))) {
      lispval v = kno_index_get(ix,keys);
      unsigned int size = KNO_CHOICE_SIZE(v);
      lispval result = kno_init_pair
        ( NULL, kno_incref(keys), KNO_INT(size) );
      if (decref_keys) kno_decref(keys);
      kno_decref(v);
      return result;}
    else filter=(kno_choice)keys;
    init_cache_level(ix);
    struct KNO_KEY_SIZE *fetched =
      ix->index_handler->fetchinfo(ix,filter,&n_fetched);
    if ((n_fetched==0) && (ix->index_adds.table_n_keys==0)) {
      if (decref_keys) kno_decref(keys);
      return EMPTY;}
    else if ((ix->index_adds.table_n_keys)==0) {
      kno_choice result = kno_alloc_choice(n_fetched);
      lispval *write = &(result->choice_0);
      if (decref_keys) kno_decref(keys);
      int i = 0; while (i<n_fetched) {
        lispval key = fetched[i].keysize_key, pair;
        unsigned int n_values = fetched[i].keysize_count;
        pair = kno_conspair(kno_incref(key),KNO_INT(n_values));
        *write++=pair;
        i++;}
      u8_big_free(fetched);
      return kno_init_choice(result,n_fetched,NULL,
                            KNO_CHOICE_DOSORT|KNO_CHOICE_REALLOC);}
    else {
      struct KNO_HASHTABLE added_sizes;
      kno_choice result; int i = 0, n_total; lispval *write;
      /* Get the sizes for added keys. */
      KNO_INIT_STATIC_CONS(&added_sizes,kno_hashtable_type);
      kno_make_hashtable(&added_sizes,ix->index_adds.ht_n_buckets);
      kno_for_hashtable(&(ix->index_adds),copy_value_sizes,&added_sizes,1);
      n_total = n_fetched+ix->index_adds.table_n_keys;
      result = kno_alloc_choice(n_total); write = &(result->choice_0);
      while (i<n_fetched) {
        lispval key = fetched[i].keysize_key, pair;
        unsigned int n_values = fetched[i].keysize_count;
        lispval added = kno_hashtable_get(&added_sizes,key,KNO_INT(0));
        n_values = n_values+kno_getint(added);
        pair = kno_conspair(kno_incref(key),KNO_INT(n_values));
        kno_decref(added);
        *write++=pair;
        i++;}
      kno_recycle_hashtable(&added_sizes);
      if (decref_keys) kno_decref(keys);
      u8_big_free(fetched);
      return kno_init_choice(result,n_total,NULL,
                            KNO_CHOICE_DOSORT|KNO_CHOICE_REALLOC);}}
  else return kno_err(kno_NoMethod,"kno_index_keys",NULL,kno_index2lisp(ix));
}

KNO_EXPORT lispval kno_index_sizes(kno_index ix)
{
  return kno_index_keysizes(ix,VOID);
}

KNO_EXPORT lispval _kno_index_get(kno_index ix,lispval key)
{
  lispval cached; struct KNO_PAIR tempkey;
  KNOTC *knotc = kno_threadcache;
  if (knotc) {
    KNO_INIT_STATIC_CONS(&tempkey,kno_pair_type);
    tempkey.car = kno_index2lisp(ix); tempkey.cdr = key;
    cached = kno_hashtable_get(&(knotc->indexes),(lispval)&tempkey,VOID);
    if (!(VOIDP(cached))) return cached;}
  if (ix->index_cache_level == 0) cached = VOID;
  else if ((PAIRP(key)) && (!(VOIDP(ix->index_covers_slotids))) &&
           (!(kno_contains_atomp(KNO_CAR(key),ix->index_covers_slotids))))
    return EMPTY;
  else cached = kno_hashtable_get(&(ix->index_cache),key,VOID);
  if (VOIDP(cached))
    cached = kno_index_fetch(ix,key);
#if KNO_USE_THREADCACHE
  if (knotc) {
    kno_hashtable_store(&(knotc->indexes),(lispval)&tempkey,cached);}
#endif
  return cached;
}
static lispval table_indexget(lispval ixarg,lispval key,lispval dflt)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix) {
    lispval v = kno_index_get(ix,key);
    if (EMPTYP(v)) return kno_incref(dflt);
    else return v;}
  else return KNO_ERROR;
}


static void extend_slotids(kno_index ix,const lispval *keys,int n)
{
  lispval slotids = ix->index_covers_slotids;
  int i = 0; while (i<n) {
    lispval key = keys[i++], slotid;
    if (PAIRP(key)) slotid = KNO_CAR(key); else continue;
    if ((OIDP(slotid)) || (SYMBOLP(slotid))) {
      if (kno_contains_atomp(slotid,slotids)) continue;
      else {kno_decref(slotids); ix->index_covers_slotids = VOID;}}}
}

KNO_EXPORT int _kno_index_add(kno_index ix,lispval key,lispval value)
{
  // KNOTC *knotc = kno_threadcache;
  kno_hashtable adds = &(ix->index_adds), cache = &(ix->index_cache);
  kno_hashtable drops = &(ix->index_drops);

  if (ix == NULL)
    return KNO_ERR(-1,"Not an index","_kno_index_add",NULL,KNO_VOID);
  else if ( (EMPTYP(value)) || (EMPTYP(key)) )
    return 0;
  else if (U8_BITP(ix->index_flags,KNO_STORAGE_READ_ONLY)) {
    kno_index front = kno_get_writable_index(ix);
    if (KNO_INDEX_CONSEDP(front)) {
      int rv = kno_index_add(front,key,value);
      kno_decref(LISPVAL(front));
      return rv;}
    else if (front)
      return kno_index_add(front,key,value);
    else return KNO_ERR(-1,kno_ReadOnlyIndex,"_kno_index_add/front",ix->indexid,KNO_VOID);}
  else init_cache_level(ix);

  int decref_key = 0;
  if (KNO_PRECHOICEP(key)) {
    key = kno_make_simple_choice(key);
    decref_key=1;}

  if (CHOICEP(key)) {
    const lispval *keyv = KNO_CHOICE_DATA(key);
    unsigned int n = KNO_CHOICE_SIZE(key);
    kno_hashtable_iterkeys(adds,kno_table_add,n,keyv,value);
    if (cache->table_n_keys)
      kno_hashtable_iterkeys(cache,kno_table_add_if_present,n,keyv,value);
    if (drops->table_n_keys)
      kno_hashtable_iterkeys(drops,kno_table_drop,n,keyv,value);
    if (!(VOIDP(ix->index_covers_slotids))) extend_slotids(ix,keyv,n);}
  else {
    kno_hashtable_add(adds,key,value);
    if (cache->table_n_keys)
      kno_hashtable_op(cache,kno_table_add_if_present,key,value);
    if (drops->table_n_keys)
      kno_hashtable_op(drops,kno_table_drop,key,value);
    if (!(VOIDP(ix->index_covers_slotids))) extend_slotids(ix,&key,1);}

  /* if ((knotc)&&(KNO_WRITETHROUGH_THREADCACHE)) knotc_add(ix,keys,value); */

  if (ix->index_flags&KNO_INDEX_IN_BACKGROUND) clear_bg_cache(key);

  if (decref_key) kno_decref(key);

  return 1;
}

static int table_indexadd(lispval ixarg,lispval key,lispval value)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix)
    return kno_index_add(ix,key,value);
  else return -1;
}

KNO_EXPORT int kno_index_drop(kno_index ix_arg,lispval key,lispval value)
{
  if ( (EMPTYP(key)) || (EMPTYP(value)) ) return 0;

  kno_index ix = kno_get_writable_index(ix_arg);
  if (ix == NULL)
    return KNO_ERR(-1,kno_ReadOnlyIndex,"_kno_index_drop",ix_arg->indexid,VOID);
  else init_cache_level(ix);

  kno_hashtable cache = &(ix->index_cache);
  kno_hashtable drops = &(ix->index_drops);
  kno_hashtable adds = &(ix->index_adds);

  int decref_key = 0;
  if (KNO_PRECHOICEP(key)) {
    key = kno_make_simple_choice(key);
    decref_key=1;}

  if (KNO_CHOICEP(key)) {
    const lispval *keyv = KNO_CHOICE_DATA(key);
    unsigned int n = KNO_CHOICE_SIZE(key);
    kno_hashtable_iterkeys(drops,kno_table_add,n,keyv,value);

    if (cache->table_n_keys)
      kno_hashtable_iterkeys(cache,kno_table_drop,n,keyv,value);
    if (adds->table_n_keys)
      kno_hashtable_iterkeys(adds,kno_table_drop,n,keyv,value);}
  else {
    kno_hashtable_add(drops,key,value);
    if (cache->table_n_keys) kno_hashtable_drop(cache,key,value);
    if (adds->table_n_keys) kno_hashtable_drop(adds,key,value);}

  /* if ((knotc)&&(KNO_WRITETHROUGH_THREADCACHE)) knotc_drop(ix,keys,value); */

  if (ix->index_flags&KNO_INDEX_IN_BACKGROUND) clear_bg_cache(key);

  if (decref_key) kno_decref(key);

  if (KNO_INDEX_CONSEDP(ix)) kno_decref(((lispval)ix));

  return 1;
}
static int table_indexdrop(lispval ixarg,lispval key,lispval value)
{
  kno_index ix = kno_indexptr(ixarg);
  if (!(ix)) return -1;
  else if (VOIDP(value))
    return kno_index_store(ix,key,EMPTY);
  else return kno_index_drop(ix,key,value);
}

KNO_EXPORT int kno_index_store(kno_index ix_arg,lispval key,lispval value)
{
  if (EMPTYP(key)) return 0;

  kno_index ix = kno_get_writable_index(ix_arg);

  if (ix == NULL)
    return KNO_ERR(-1,kno_ReadOnlyIndex,"_kno_index_store",ix_arg->indexid,VOID);
  else init_cache_level(ix);

  kno_hashtable cache = &(ix->index_cache);
  kno_hashtable adds = &(ix->index_adds);
  kno_hashtable drops = &(ix->index_drops);
  kno_hashtable stores = &(ix->index_stores);

  int decref_key = 0;
  if (KNO_PRECHOICEP(key)) {
    key = kno_make_simple_choice(key);
    decref_key=1;}

  if (KNO_CHOICEP(key)) {
    const lispval *keyv = KNO_CHOICE_DATA(key);
    unsigned int n = KNO_CHOICE_SIZE(key);

    kno_hashtable_iterkeys(stores,kno_table_store,n,keyv,value);

    if (cache->table_n_keys)
      kno_hashtable_iterkeys(cache,kno_table_replace,n,keyv,value);
    if (adds->table_n_keys)
      kno_hashtable_iterkeys(adds,kno_table_drop,n,keyv,VOID);
    if (stores->table_n_keys)
      kno_hashtable_iterkeys(drops,kno_table_drop,n,keyv,VOID);}
  else {
    kno_hashtable_store(stores,key,value);
    if (cache->table_n_keys)
      kno_hashtable_op(cache,kno_table_replace,key,value);
    if (adds->table_n_keys) kno_hashtable_drop(adds,key,VOID);
    if (drops->table_n_keys) kno_hashtable_drop(drops,key,VOID);}

  /* if ((knotc)&&(KNO_WRITETHROUGH_THREADCACHE)) knotc_store(ix,keys,value); */

  if (ix->index_flags&KNO_INDEX_IN_BACKGROUND) clear_bg_cache(key);
  if (decref_key) kno_decref(key);

  if (KNO_INDEX_CONSEDP(ix)) kno_decref(((lispval)ix));

  return 1;
}

static int table_indexstore(lispval ixarg,lispval key,lispval value)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix) return kno_index_store(ix,key,value);
  else return -1;
}

/* Index batch operations */

static int merge_kv_into_adds(struct KNO_KEYVAL *kv,void *data)
{
  struct KNO_HASHTABLE *adds = (kno_hashtable) data;
  kno_hashtable_op_nolock(adds,kno_table_add,kv->kv_key,kv->kv_val);
  return 0;
}

KNO_EXPORT int kno_index_merge(kno_index ix,kno_hashtable table)
{
  kno_index into_index = kno_get_writable_index(ix);
  if (into_index == NULL)
    return KNO_ERR(-1,kno_ReadOnlyIndex,"kno_index_merge",ix->indexid,VOID);
  else init_cache_level(into_index);

  kno_hashtable adds = &(into_index->index_adds);
  kno_write_lock_table(adds);
  kno_read_lock_table(table);
  kno_for_hashtable_kv(table,merge_kv_into_adds,(void *)adds,0);
  kno_unlock_table(table);
  kno_unlock_table(adds);

  if (KNO_INDEX_CONSEDP(into_index)) kno_decref(LISPVAL(into_index));

  return 1;
}

KNO_EXPORT int kno_batch_add(kno_index ix_arg,lispval table)
{
  kno_index ix = kno_get_writable_index(ix_arg);
  if (ix == NULL)
    return KNO_ERR(-1,kno_ReadOnlyIndex,"_kno_batch_add",ix_arg->indexid,VOID);
  else init_cache_level(ix);
  if (HASHTABLEP(table)) {
    int rv = kno_index_merge(ix,(kno_hashtable)table);
    if (KNO_INDEX_CONSEDP(ix)) kno_decref(LISPVAL(ix));
    return rv;}
  else if (TABLEP(table)) {
    kno_hashtable adds = &(ix->index_adds);
    lispval allkeys = kno_getkeys(table);
    int i = 0, n_keys = KNO_CHOICE_SIZE(allkeys), atomic = 1;
    lispval *keys = u8_alloc_n(n_keys,lispval);
    lispval *values = u8_alloc_n(n_keys,lispval);
    DO_CHOICES(key,allkeys) {
      lispval v = kno_get(table,key,VOID);
      if (!(VOIDP(v))) {
        if (CONSP(v)) atomic = 0;
        keys[i]=key; values[i]=v; i++;}}
    kno_hashtable_iter(adds,kno_table_add,i,keys,values);
    if (!(atomic)) for (int j = 0;j<i;j++) { kno_decref(values[j]); }
    u8_big_free(keys);
    u8_big_free(values);
    kno_decref(allkeys);
    if (KNO_INDEX_CONSEDP(ix)) kno_decref(LISPVAL(ix));
    return i;}
  else {
    kno_seterr(kno_TypeError,"kno_batch_add","Not a table",table);
    if (KNO_INDEX_CONSEDP(ix)) kno_decref(LISPVAL(ix));
    return -1;}
}

static lispval table_indexkeys(lispval ixarg)
{
  kno_index ix = kno_indexptr(ixarg);
  if (ix) return kno_index_keys(ix);
  else return kno_type_error(_("index"),"table_index_keys",ixarg);
}

static int remove_keyvals(struct KNO_KEYVAL *keyvals,int n,lispval remove)
{
  int i=0; while (i<n) {
    lispval key = keyvals[i].kv_key;
    if ((CHOICEP(remove)) ? (kno_choice_containsp(key,remove)) :
        (KNO_EQUALP(key,remove)) ) {
      lispval val = keyvals[i].kv_val;
      kno_decref(key); kno_decref(val);
      memmove(keyvals+i,keyvals+i+1,KNO_KEYVAL_LEN*(n-(i+1)));
      n--;}
    i++;}
  return n;
}

typedef struct KNO_CONST_KEYVAL *const_keyvals;

static struct KNO_KEYVAL *
hashtable_keyvals(kno_hashtable ht,int *sizep,int keep_empty)
{
  /* We assume that the table is locked */
  struct KNO_KEYVAL *results, *rscan;
  if (ht->table_n_keys == 0) {
    *sizep=0;
    return NULL;}
  if (ht->ht_n_buckets) {
    int size = 0;
    struct KNO_HASH_BUCKET **scan=ht->ht_buckets;
    struct KNO_HASH_BUCKET **lim=scan+ht->ht_n_buckets;
    rscan=results=u8_big_alloc_n(ht->table_n_keys,struct KNO_KEYVAL);
    while (scan < lim)
      if (*scan) {
        struct KNO_HASH_BUCKET *e=*scan;
        int bucket_len=e->bucket_len;
        struct KNO_KEYVAL *kvscan=&(e->kv_val0);
        struct KNO_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          lispval key = kvscan->kv_key, val = kvscan->kv_val;
          if ( (EXISTSP(val)) || (keep_empty) ) {
            if (KNO_PRECHOICEP(val)) {
              val = kvscan->kv_val = kno_simplify_choice(val);}
            rscan->kv_key=key; kno_incref(key);
            rscan->kv_val=val; kno_incref(val);
            rscan++; size++;}
          kvscan++;}
        scan++;}
      else scan++;
    *sizep=size;}
  else {*sizep=0; results=NULL;}
  return results;
}

static void free_commits(struct KNO_INDEX_COMMITS *commits)
{
  struct KNO_KEYVAL *adds = (kno_keyvals) commits->commit_adds;
  struct KNO_KEYVAL *drops = (kno_keyvals) commits->commit_drops;
  struct KNO_KEYVAL *stores = (kno_keyvals) commits->commit_stores;

  if (adds) {
    kno_free_keyvals(adds,commits->commit_n_adds);
    commits->commit_adds=NULL;
    commits->commit_n_adds=0;
    u8_big_free(adds);}
  if (drops) {
    kno_free_keyvals(drops,commits->commit_n_drops);
    commits->commit_drops=NULL;
    commits->commit_n_drops=0;
    u8_big_free(drops);}
  if (stores) {
    kno_free_keyvals(stores,commits->commit_n_stores);
    commits->commit_stores=NULL;
    commits->commit_n_stores=0;
    u8_big_free(stores);}

  kno_decref(commits->commit_metadata);
}

#define record_elapsed(loc) \
  (loc=(u8_elapsed_time()-(mark)),(mark=u8_elapsed_time()))

static int index_dowrite(kno_index ix,struct KNO_INDEX_COMMITS *commits)
{
  double started = u8_elapsed_time();
  int kno_storage_loglevel = index_loglevel(ix);
  int n_changes =
    commits->commit_n_adds +
    commits->commit_n_drops +
    commits->commit_n_stores;
  if (!(KNO_VOIDP(commits->commit_metadata))) n_changes++;

  u8_logf(LOG_DEBUG,"IndexCommit/Write",
          "Writing %d edits to %s",n_changes,ix->indexid);

  int rv =ix->index_handler->commit(ix,kno_commit_write,commits);

  if (rv<0) {
    u8_exception ex = u8_current_exception;
    if (ex)
      u8_logf(LOG_CRIT,"Failed/IndexCommit/Write",
              "(after %fsecs) writing %d edits to %s: %m %s %s%s%s",
              elapsed_time(started),n_changes,ix->indexid,
              ex->u8x_cond,ex->u8x_context,
              U8OPTSTR("(",ex->u8x_details,")"));
    else u8_logf(LOG_CRIT,"Failed/IndexCommit/Write",
                 "(after %fsecs) writing %d edits to %s",
                 n_changes,ix->indexid,elapsed_time(started));}
  else u8_logf(LOG_DEBUG,"Finished/IndexCommit/Write",
               "(after %fsecs) writing %d edits to %s",
               elapsed_time(started),n_changes,ix->indexid);

  return rv;
}

static int index_rollback(kno_index ix,struct KNO_INDEX_COMMITS *commits)
{
  double started = u8_elapsed_time();
  int kno_storage_loglevel = index_loglevel(ix);
  int n_changes =
    commits->commit_n_adds + commits->commit_n_drops + commits->commit_n_stores;
  if (!(KNO_VOIDP(commits->commit_metadata))) n_changes++;

  u8_logf(LOG_WARN,"IndexRollback",
          "Rolling back %d edits to %s",n_changes,ix->indexid);

  int rollback = ix->index_handler->commit(ix,kno_commit_rollback,commits);
  if (rollback < 0) {
    u8_logf(LOG_CRIT,"Failed/IndexRollback",
            "(after %fs) Couldn't rollback failed save of %d edits to %s",
            elapsed_time(started),ix->indexid);
    kno_seterr("IndexRollbackFailed","index_dcommit/rollback",
              ix->indexid,KNO_VOID);}
  else u8_logf(LOG_WARN,"Finished/IndexRollback",
               "Rolled back %d edits to %s in %f seconds",
               n_changes,ix->indexid,
               elapsed_time(started));

  return -1;
}

static int index_dosync(kno_index ix,struct KNO_INDEX_COMMITS *commits)
{
  double started = u8_elapsed_time();
  int kno_storage_loglevel = index_loglevel(ix);
  int n_changes =
    commits->commit_n_adds + commits->commit_n_drops + commits->commit_n_stores;
  if (!(KNO_VOIDP(commits->commit_metadata))) n_changes++;

  u8_logf(LOG_DETAIL,"IndexWrite/Sync",
          _("Syncing %d changes to %s"),n_changes,ix->indexid);

  int synced = ix->index_handler->commit(ix,kno_commit_sync,commits);

  if (synced < 0) {
    kno_seterr("IndexSync/Error","index_dosync",ix->indexid,KNO_VOID);
    u8_logf(LOG_ERR,"IndexSync/Error",
            _("(after %fs) syncing %d changes to %s, rolling back"),
            elapsed_time(started),n_changes,ix->indexid);
    index_rollback(ix,commits);}
  else u8_logf(LOG_DETAIL,"IndexWrite/Synced",
               _("Synced %d changes to %s in %f secs"),
               n_changes,ix->indexid,elapsed_time(started));

  return synced;
}

static int index_doflush(kno_index ix,struct KNO_INDEX_COMMITS *commits)
{
  double started = u8_elapsed_time();
  int kno_storage_loglevel = index_loglevel(ix);
  int n_changes =
    commits->commit_n_adds + commits->commit_n_drops + commits->commit_n_stores;
  if (!(KNO_VOIDP(commits->commit_metadata))) n_changes++;

  u8_logf(LOG_DETAIL,"IndexCommit/Flush",
          "Flushing %d cached edits from %s",n_changes,ix->indexid);

  int rv= ix->index_handler->commit(ix,kno_commit_flush,commits);

  if (rv<0) {
    u8_exception ex = u8_current_exception;
    if (ex)
      u8_logf(LOG_CRIT,"Failed/IndexCommit/Write",
              "(after %fs) flushing %d cached edits from %s: %m %s %s%s%s",
              elapsed_time(started),n_changes,ix->indexid,
              ex->u8x_cond,ex->u8x_context,
              U8OPTSTR("(",ex->u8x_details,")"));
    else u8_logf(LOG_CRIT,"Failed/IndexCommit/Write",
                 "(after %fs) flushing %d cached edits from %s",
                 elapsed_time(started),n_changes,ix->indexid);}
  else u8_logf(LOG_DEBUG,"Finished/IndexCommit/Flush",
               "Flushed %d cached edits from %s in %f secons",
               n_changes,ix->indexid,
               elapsed_time(started));

  return rv;
}

static int index_docommit(kno_index ix,struct KNO_INDEX_COMMITS *use_commits)
{
  int kno_storage_loglevel = (ix->index_loglevel > 0) ? (ix->index_loglevel) :
    (*(kno_storage_loglevel_ptr));

  struct KNO_INDEX_COMMITS commits = { 0 };
  int unlock_adds = 0, unlock_drops = 0, unlock_stores = 0;
  double start_time = u8_elapsed_time(), mark = start_time;
 if (use_commits)
    memcpy(&commits,use_commits,sizeof(struct KNO_INDEX_COMMITS));
  else commits.commit_index = ix;

  commits.commit_times.base = mark;
  if ( (use_commits == NULL) ||
       (commits.commit_metadata == KNO_NULL) )
    commits.commit_metadata = KNO_VOID;

  int started = ix->index_handler->commit(ix,kno_commit_start,&commits);
  if (started < 0) {
    kno_seterr("CommitFailed","index_docommit/start",ix->indexid,KNO_VOID);
    return started;}
  record_elapsed(commits.commit_times.start);
  u8_logf(LOG_DEBUG,"IndexCommit/Started","Committing %s for %s",
          ((use_commits)?("edits"):("changes")),
          ix->indexid);

  if (use_commits == NULL) {
    struct KNO_HASHTABLE *adds_table = &(ix->index_adds);
    struct KNO_HASHTABLE *drops_table = &(ix->index_drops);
    struct KNO_HASHTABLE *stores_table = &(ix->index_stores);

    lispval metadata = (lispval) (&(ix->index_metadata));
    commits.commit_metadata =
      (kno_modifiedp(metadata)) ?
      (kno_deep_copy(metadata)) :
      (KNO_VOID);

    int n_adds=0, n_drops=0, n_stores=0;
    /* Lock the changes for the index */
    if (adds_table->table_uselock) {
      kno_write_lock_table(adds_table);
      unlock_adds=1;}
    if (drops_table->table_uselock) {
      kno_write_lock_table(drops_table);
      unlock_drops=1;}
    if (stores_table->table_uselock) {
      kno_write_lock_table(stores_table);
      unlock_stores=1;}

    /* Copy them */
    struct KNO_KEYVAL *adds = hashtable_keyvals(adds_table,&n_adds,0);
    struct KNO_KEYVAL *drops = hashtable_keyvals(drops_table,&n_drops,0);
    struct KNO_KEYVAL *stores = hashtable_keyvals(stores_table,&n_stores,1);

    /* We've got the data to save, so we unlock the tables. */
    if (unlock_adds) kno_unlock_table(adds_table);
    if (unlock_drops) kno_unlock_table(drops_table);
    if (unlock_stores) kno_unlock_table(stores_table);
    unlock_adds=unlock_stores=unlock_drops=0;

    lispval merged = EMPTY;

    /* This merges adds and drops over explicitly stored values into
       the stored values, removing the corresponding add/drops. */
    if (n_stores) {
      int i=0; while (i<n_stores) {
        lispval key = stores[i].kv_key;
        lispval value = stores[i].kv_val;
        lispval added = htget(adds_table,key);
        lispval dropped = htget(drops_table,key);
        if ( (EXISTSP(added)) || (EXISTSP(dropped)) ) {
          CHOICE_ADD(merged,key);
          CHOICE_ADD(value,added);
          if (EXISTSP(dropped)) {
            lispval newv = kno_difference(value,dropped);
            kno_decref(value);
            kno_decref(dropped);
            value = newv;}
          stores[i].kv_val = value;}
        i++;}}
    if (EXISTSP(merged)) {
      n_adds = remove_keyvals(adds,n_adds,merged);
      n_drops = remove_keyvals(drops,n_drops,merged);
      kno_decref(merged);}

    commits.commit_adds   = (kno_const_keyvals) adds;
    commits.commit_n_adds = n_adds;
    commits.commit_drops   = (kno_const_keyvals) drops;
    commits.commit_n_drops = n_drops;
    commits.commit_stores   = (kno_const_keyvals) stores;
    commits.commit_n_stores = n_stores;}
  record_elapsed(commits.commit_times.setup);

  int n_keys = commits.commit_n_adds + commits.commit_n_stores +
    commits.commit_n_drops;
  int n_changes = n_keys + (KNO_SLOTMAPP(commits.commit_metadata));
  int w_metadata = KNO_SLOTMAPP(commits.commit_metadata);

  u8_logf(LOG_INFO,kno_IndexCommit,
          _("Committing %d %s (+%d-%d:=%d%s) to %s"),
          n_changes,((use_commits) ? ("edits") : ("changes")),
          commits.commit_n_adds,
          commits.commit_n_drops,
          commits.commit_n_stores,
          (w_metadata) ? (" w/metadata") : (""),
          ix->indexid);

  if (n_keys) init_cache_level(ix);

  int written = index_dowrite(ix,&commits), synced = 0;
  record_elapsed(commits.commit_times.write);

  if (written >= 0)
    u8_logf(LOG_DEBUG,"IndexWrite/Written",
            _("Wrote %d changes to %s after %f secs"),
            n_changes,ix->indexid,commits.commit_times.write);

  if (written < 0) {
    kno_seterr("CommitFailed","index_docommit/write",ix->indexid,KNO_VOID);
    u8_logf(LOG_ERR,"IndexWrite/Error",
            _("Error writing %d changes to %s after %f secs, rolling back"),
            n_changes,ix->indexid,u8_elapsed_time()-start_time);
    synced = -1;
    index_rollback(ix,&commits);
    commits.commit_phase = kno_commit_flush;}
  else if (commits.commit_phase == kno_commit_sync) {
    synced = index_dosync(ix,&commits);
    commits.commit_phase = kno_commit_flush;}
  else {
    synced = 0;
    commits.commit_phase = kno_commit_flush;}
  record_elapsed(commits.commit_times.sync);

  /* If this returns < 0, it will have generated a warning and it doesn't require us to abort
     because the state has been saved, it's just still in memory. */
  index_doflush(ix,&commits);

  if (synced<0) {
    /* If there was an error, just leave the edits in the index hashtables */
    if (use_commits == NULL) free_commits(&commits);}
  else if (use_commits == NULL) {
    /* If you got the changes out of the index hashtables, clear them: */
    struct KNO_HASHTABLE *adds_table = &(ix->index_adds);
    struct KNO_HASHTABLE *drops_table = &(ix->index_drops);
    struct KNO_HASHTABLE *stores_table = &(ix->index_stores);
    /* Lock the adds and edits again */
    if (adds_table->table_uselock) {
      kno_write_lock_table(adds_table);
      unlock_adds=1;}
    if (drops_table->table_uselock) {
      kno_write_lock_table(drops_table);
      unlock_drops=1;}
    if (stores_table->table_uselock) {
      kno_write_lock_table(stores_table);
      unlock_stores=1;}

    /* Remove everything you just saved */

    kno_hashtable_iter_kv(adds_table,kno_table_drop,
                         (const_keyvals)commits.commit_adds,
                         commits.commit_n_adds,
                         0);
    kno_hashtable_iter_kv(drops_table,kno_table_drop,
                         (const_keyvals)commits.commit_drops,
                         commits.commit_n_drops,
                         0);
    kno_hashtable_iter_kv(stores_table,kno_table_drop,
                         (const_keyvals)commits.commit_stores,
                         commits.commit_n_stores,
                         0);

    if (unlock_adds) kno_unlock_table(&(ix->index_adds));
    if (unlock_drops) kno_unlock_table(&(ix->index_drops));
    if (unlock_stores) kno_unlock_table(&(ix->index_stores));

    free_commits(&commits);}
  else NO_ELSE;
  record_elapsed(commits.commit_times.flush);
  commits.commit_phase = kno_commit_cleanup;

  int cleanup_rv = ix->index_handler->commit(ix,kno_commit_cleanup,&commits);
  if (cleanup_rv < 0) {
    if (errno) { u8_graberrno("index_docommit",u8_strdup(ix->indexid)); }
    u8_log(LOG_WARN,"IndexCleanupFailed",
           "There was en error cleaning up after commiting %s",ix->indexid);
    kno_clear_errors(1);}
  record_elapsed(commits.commit_times.cleanup);

  commits.commit_phase = kno_commit_done;

  if (synced < 0)
    u8_logf(LOG_CRIT,"Index/Commit/Failed",
            _("for %d %supdated keys from %s after %f secs"
              "total=%f, start=%f, setup=%f, save=%f, "
              "finalize=%f, apply=%f, cleanup=%f"),
            n_changes,(w_metadata ? ("(and metadata) ") : ("") ),
            ix->indexid,
            elapsed_time(commits.commit_times.base),
            commits.commit_times.start,
            commits.commit_times.setup,
            commits.commit_times.write,
            commits.commit_times.sync,
            commits.commit_times.flush,
            commits.commit_times.cleanup);
  else u8_logf(LOG_NOTICE,"Index/Commit/Complete",
               _("Committed %d changes%s from '%s' in %f\n"
                 "total=%f, start=%f, setup=%f, save=%f, "
                 "finalize=%f, apply=%f, cleanup=%f"),
               n_changes,
               (w_metadata ? (" (and metadata) ") : ("") ),
               ix->indexid,
               elapsed_time(commits.commit_times.base),
               elapsed_time(commits.commit_times.base),
               commits.commit_times.start,
               commits.commit_times.setup,
               commits.commit_times.write,
               commits.commit_times.sync,
               commits.commit_times.flush,
               commits.commit_times.cleanup);

  if (synced < 0)
    return synced;
  else return n_changes;
}

KNO_EXPORT int kno_commit_index(kno_index ix)
{
  if (ix == NULL)
    return -1;
  else if (! ((ix->index_adds.table_n_keys) ||
              (ix->index_drops.table_n_keys) ||
              (ix->index_stores.table_n_keys) ||
              (kno_modifiedp((lispval)&(ix->index_metadata)))) )
    return 0;
  else if (ix->index_handler->commit == NULL) {
    u8_seterr("NoCommitMethod","kno_commit_index",u8_strdup(ix->indexid));
    return -1;}
  else NO_ELSE;

  u8_lock_mutex(&(ix->index_commit_lock));
  int rv = index_docommit(ix,NULL);
  u8_unlock_mutex(&(ix->index_commit_lock));
  return rv;
}

KNO_EXPORT int kno_index_save(kno_index ix,
                            lispval toadd,
                            lispval todrop,
                            lispval tostore,
                            lispval metadata)
{
  int kno_storage_loglevel = index_loglevel(ix);

  if (ix == NULL)
    return -1;
  else if (ix->index_handler->commit == NULL) {
    u8_seterr("NoCommitMethod","kno_index_save",u8_strdup(ix->indexid));
    return -1;}

  int free_adds = 0, free_drops =0, free_stores = 0, free_adds_vec = 0;
  int n_adds = 0, n_drops =0, n_stores = 0;
  struct KNO_KEYVAL *adds=NULL, *drops=NULL, *stores=NULL;

  struct KNO_INDEX_COMMITS commits = {};
  commits.commit_index = ix;

  if (KNO_VOIDP(toadd)) {}
  else if (KNO_SLOTMAPP(toadd))
    adds = KNO_SLOTMAP_KEYVALS(toadd);
  else if (KNO_HASHTABLEP(toadd)) {
    adds = hashtable_keyvals((kno_hashtable)toadd,&n_adds,0);
    free_adds = 1;}
  else if ( (KNO_PAIRP(toadd)) &&
            (KNO_VECTORP(KNO_CAR(toadd))) &&
            (KNO_VECTORP(KNO_CDR(toadd)))) {
    lispval keys = KNO_CAR(toadd), vals = KNO_CDR(toadd);
    size_t keys_len = KNO_VECTOR_LENGTH(keys);
    size_t vals_len = KNO_VECTOR_LENGTH(vals);
    if (keys_len != vals_len)
      return kno_err("KeyVec/ValVec mismatch","kno_index_save",ix->indexid,toadd);
    adds = u8_big_alloc_n(keys_len,struct KNO_KEYVAL);
    long i=0, n=0; while (i<keys_len) {
      lispval key = KNO_VECTOR_REF(keys,i);
      lispval val = KNO_VECTOR_REF(vals,i);
      if ( (KNO_EMPTYP(key)) || (KNO_EMPTYP(val)) ||
           (KNO_VOIDP(key)) || (KNO_VOIDP(val)) ||
           (KNO_NULLP(key)) || (KNO_NULLP(val)) )
        i++;
      else {
        adds[n].kv_key = key;
        adds[n].kv_val = val;
        i++; n++;}}
      free_adds = 0;
      free_adds_vec = 1;
      n_adds = n;}
  else if (KNO_TABLEP(toadd)) {
    lispval keys = kno_getkeys(toadd);
    int i=0, n = KNO_CHOICE_SIZE(keys);
    adds = u8_big_alloc_n(n,struct KNO_KEYVAL);
    KNO_DO_CHOICES(key,keys) {
      lispval val = kno_get(toadd,key,KNO_VOID);
      if (!((KNO_VOIDP(val)) || (KNO_EMPTYP(val)))) {
        adds[i].kv_key = key; kno_incref(key);
        adds[i].kv_val = val;
        i++;}}
    kno_decref(keys);
    free_adds = 1;
    toadd=i;}

  if (KNO_VOIDP(todrop)) {}
  else if (KNO_SLOTMAPP(todrop))
    drops = KNO_SLOTMAP_KEYVALS(todrop);
  else if (KNO_HASHTABLEP(todrop)) {
    drops = hashtable_keyvals((kno_hashtable)todrop,&n_drops,0);
    free_drops = 1;}
  else if (KNO_TABLEP(todrop)) {
    lispval keys = kno_getkeys(todrop);
    int i=0, n = KNO_CHOICE_SIZE(keys);
    drops = u8_big_alloc_n(n,struct KNO_KEYVAL);
    KNO_DO_CHOICES(key,keys) {
      lispval val = kno_get(todrop,key,KNO_VOID);
      if (!((KNO_VOIDP(val)) || (KNO_EMPTYP(val)))) {
        drops[i].kv_key = key; kno_incref(key);
        drops[i].kv_val = val;
        i++;}}
    kno_decref(keys);
    free_drops = 1;
    todrop=i;}

  if (KNO_VOIDP(tostore)) {}
  else if (KNO_SLOTMAPP(tostore))
    stores = KNO_SLOTMAP_KEYVALS(tostore);
  else if (KNO_HASHTABLEP(tostore)) {
    stores = hashtable_keyvals((kno_hashtable)tostore,&n_stores,1);
    free_stores = 1;}
  else if (KNO_TABLEP(tostore)) {
    lispval keys = kno_getkeys(tostore);
    int i=0, n = KNO_CHOICE_SIZE(keys);
    stores = u8_big_alloc_n(n,struct KNO_KEYVAL);
    KNO_DO_CHOICES(key,keys) {
      lispval val = kno_get(tostore,key,KNO_VOID);
      if (!((KNO_VOIDP(val)) || (KNO_EMPTYP(val)))) {
        stores[i].kv_key = key; kno_incref(key);
        stores[i].kv_val = val;
        i++;}}
    kno_decref(keys);
    free_stores = 1;
    tostore=i;}

  int n_changes = n_adds + n_drops + n_stores;
  if (!(KNO_VOIDP(metadata))) n_changes++;

  if (n_changes) {
    init_cache_level(ix);
    u8_logf(LOG_DEBUG,kno_IndexCommit,
            _("Saving %d changes (+%d-%d=%d%s) to %s"),
            n_changes,n_adds,n_drops,n_stores,
            (KNO_VOIDP(metadata)) ? ("") : ("/md"),
            ix->indexid);}

  if (KNO_SLOTMAPP(metadata))
    commits.commit_metadata = metadata;
  else commits.commit_metadata = KNO_VOID;

  commits.commit_adds     = (kno_const_keyvals) adds;
  commits.commit_n_adds   = n_adds;
  commits.commit_drops    = (kno_const_keyvals) drops;
  commits.commit_n_drops  = n_drops;
  commits.commit_stores   = (kno_const_keyvals) stores;
  commits.commit_n_stores = n_stores;

  u8_lock_mutex(&(ix->index_commit_lock));
  int saved = index_docommit(ix,&commits);
  u8_unlock_mutex(&(ix->index_commit_lock));

  if (free_adds) {
    kno_free_keyvals(adds,n_adds);
    u8_big_free(adds);}
  else if (free_adds_vec)
    u8_big_free(adds);
  else NO_ELSE;

  if (free_drops) {
    kno_free_keyvals(drops,n_drops);
    u8_big_free(drops);}

  if (free_stores) {
    kno_free_keyvals(stores,n_stores);
    u8_big_free(stores);}

  kno_decref(commits.commit_metadata);

  return saved;
}

KNO_EXPORT void kno_index_swapout(kno_index ix,lispval keys)
{
  struct KNO_HASHTABLE *cache = &(ix->index_cache);
  if (((ix->index_flags)&KNO_STORAGE_NOSWAP) ||
      (cache->table_n_keys==0))
    return;
  else if (VOIDP(keys)) {
    if ((ix->index_flags)&(KNO_STORAGE_KEEP_CACHESIZE))
      kno_reset_hashtable(&(ix->index_cache),-1,1);
    else kno_reset_hashtable(&(ix->index_cache),0,1);}
  else if (CHOICEP(keys)) {
    struct KNO_CHOICE *ch = (kno_choice)keys;
    kno_hashtable_iterkeys(cache,kno_table_replace,ch->choice_size,
                          KNO_XCHOICE_DATA(ch),VOID);
    kno_devoid_hashtable(cache,0);
    return;}
  else if (PRECHOICEP(keys)) {
    lispval simplified = kno_make_simple_choice(keys);
    kno_index_swapout(ix,simplified);
    kno_decref(simplified);}
  else {
    kno_hashtable_store(&(ix->index_cache),keys,VOID);}
}

KNO_EXPORT void kno_index_close(kno_index ix)
{
  if ((ix) && (ix->index_handler) && (ix->index_handler->close))
    ix->index_handler->close(ix);
}

static lispval cachelevel_slot, indexid_slot, source_slot, realpath_slot,
  cached_slot, adds_slot, edits_slot, flags_slot, registered_slot,
  opts_slot, drops_slot, replaced_slot;

static lispval read_only_flag, unregistered_flag, registered_flag,
  noswap_flag, noerr_flag, phased_flag, background_flag,
  canadd_flag, candrop_flag, canset_flag;

static void mdstore(lispval md,lispval slot,lispval v)
{
  if (KNO_VOIDP(v)) return;
  kno_store(md,slot,v);
  kno_decref(v);
}
static void mdstring(lispval md,lispval slot,u8_string s)
{
  if (s==NULL) return;
  lispval v=knostring(s);
  kno_store(md,slot,v);
  kno_decref(v);
}

static lispval metadata_readonly_props = KNO_VOID;

KNO_EXPORT lispval kno_index_base_metadata(kno_index ix)
{
  int flags=ix->index_flags;
  lispval metadata=kno_deep_copy((lispval)&(ix->index_metadata));
  mdstore(metadata,cachelevel_slot,KNO_INT(ix->index_cache_level));
  mdstring(metadata,indexid_slot,ix->indexid);
  mdstring(metadata,source_slot,ix->index_source);
  mdstring(metadata,realpath_slot,ix->canonical_source);
  mdstore(metadata,cached_slot,KNO_INT(ix->index_cache.table_n_keys));
  mdstore(metadata,adds_slot,KNO_INT(ix->index_adds.table_n_keys));
  mdstore(metadata,drops_slot,KNO_INT(ix->index_drops.table_n_keys));
  mdstore(metadata,replaced_slot,KNO_INT(ix->index_stores.table_n_keys));
  if ((ix->index_handler) && (ix->index_handler->name))
    mdstring(metadata,KNOSYM_TYPE,(ix->index_handler->name));

  if (U8_BITP(flags,KNO_STORAGE_READ_ONLY))
    kno_add(metadata,flags_slot,read_only_flag);
  if (U8_BITP(flags,KNO_STORAGE_UNREGISTERED))
    kno_add(metadata,flags_slot,unregistered_flag);
  else {
    kno_add(metadata,flags_slot,registered_flag);
    kno_store(metadata,registered_slot,KNO_INT(ix->index_serialno));}
  if (U8_BITP(flags,KNO_STORAGE_NOSWAP))
    kno_add(metadata,flags_slot,noswap_flag);
  if (U8_BITP(flags,KNO_STORAGE_NOERR))
    kno_add(metadata,flags_slot,noerr_flag);
  if (U8_BITP(flags,KNO_STORAGE_PHASED))
    kno_add(metadata,flags_slot,phased_flag);
  if (U8_BITP(flags,KNO_INDEX_IN_BACKGROUND))
    kno_add(metadata,flags_slot,background_flag);
  if (U8_BITP(flags,KNO_INDEX_ADD_CAPABILITY))
    kno_add(metadata,flags_slot,canadd_flag);
  if (U8_BITP(flags,KNO_INDEX_DROP_CAPABILITY))
    kno_add(metadata,flags_slot,candrop_flag);
  if (U8_BITP(flags,KNO_INDEX_SET_CAPABILITY))
    kno_add(metadata,flags_slot,canset_flag);

  if (KNO_TABLEP(ix->index_opts))
    kno_add(metadata,opts_slot,ix->index_opts);

  kno_add(metadata,metadata_readonly_props,cachelevel_slot);
  kno_add(metadata,metadata_readonly_props,indexid_slot);
  kno_add(metadata,metadata_readonly_props,realpath_slot);
  kno_add(metadata,metadata_readonly_props,source_slot);
  kno_add(metadata,metadata_readonly_props,KNOSYM_TYPE);
  kno_add(metadata,metadata_readonly_props,edits_slot);
  kno_add(metadata,metadata_readonly_props,adds_slot);
  kno_add(metadata,metadata_readonly_props,drops_slot);
  kno_add(metadata,metadata_readonly_props,replaced_slot);
  kno_add(metadata,metadata_readonly_props,cached_slot);
  kno_add(metadata,metadata_readonly_props,flags_slot);

  kno_add(metadata,metadata_readonly_props,KNOSYM_PROPS);
  kno_add(metadata,metadata_readonly_props,opts_slot);

  return metadata;
}

/* Common init function */

KNO_EXPORT void kno_init_index(kno_index ix,
                             struct KNO_INDEX_HANDLER *h,
                             u8_string id,u8_string src,u8_string csrc,
                             kno_storage_flags flags,
                             lispval metadata,
                             lispval opts)
{
  if (flags<0)
    flags = kno_get_dbflags(opts,KNO_STORAGE_ISINDEX);
  else {U8_SETBITS(flags,KNO_STORAGE_ISINDEX);}
  if (U8_BITP(flags,KNO_STORAGE_UNREGISTERED)) {
    KNO_INIT_CONS(ix,kno_consed_index_type);
    add_consed_index(ix);}
  else {KNO_INIT_STATIC_CONS(ix,kno_consed_index_type);}
  if (U8_BITP(flags,KNO_STORAGE_READ_ONLY)) {
    U8_SETBITS(flags,KNO_STORAGE_READ_ONLY); };
  ix->index_serialno = -1; ix->index_cache_level = -1; ix->index_flags = flags;
  KNO_INIT_STATIC_CONS(&(ix->index_cache),kno_hashtable_type);
  KNO_INIT_STATIC_CONS(&(ix->index_adds),kno_hashtable_type);
  KNO_INIT_STATIC_CONS(&(ix->index_drops),kno_hashtable_type);
  KNO_INIT_STATIC_CONS(&(ix->index_stores),kno_hashtable_type);
  kno_make_hashtable(&(ix->index_cache),kno_index_cache_init);
  kno_make_hashtable(&(ix->index_adds),0);
  kno_make_hashtable(&(ix->index_drops),0);
  kno_make_hashtable(&(ix->index_stores),0);

  KNO_INIT_STATIC_CONS(&(ix->index_props),kno_slotmap_type);
  kno_init_slotmap(&(ix->index_props),17,NULL);

  ix->index_handler = h;

  lispval keyslot = kno_getopt(opts,KNOSYM_KEYSLOT,VOID);
  KNO_INIT_STATIC_CONS(&(ix->index_metadata),kno_slotmap_type);
  if (KNO_SLOTMAPP(metadata)) {
    kno_copy_slotmap((kno_slotmap)metadata,&(ix->index_metadata));
    if (KNO_VOIDP(keyslot)) keyslot=kno_get(metadata,KNOSYM_KEYSLOT,VOID);}
  else {
    kno_init_slotmap(&(ix->index_metadata),17,NULL);
    ix->index_keyslot = VOID;}
  ix->index_metadata.table_modified = 0;
  ix->index_keyslot = keyslot;

  /* This was what was specified */
  ix->indexid = u8_strdup(id);
  /* Don't copy this one */
  ix->index_source = u8_strdup(src);
  ix->canonical_source = u8_strdup(csrc);
  ix->index_typeid = NULL;
  ix->index_covers_slotids = VOID;

  if ( (KNO_VOIDP(opts)) || (KNO_FALSEP(opts)) )
    ix->index_opts = KNO_FALSE;
  else ix->index_opts = kno_incref(opts);

  lispval ll = kno_getopt(opts,KNOSYM_LOGLEVEL,KNO_VOID);
  if (KNO_VOIDP(ll))
    ix->index_loglevel = -1;
  else if ( (KNO_FIXNUMP(ll)) && ( (KNO_FIX2INT(ll)) >= 0 ) &&
       ( (KNO_FIX2INT(ll)) < U8_MAX_LOGLEVEL ) )
    ix->index_loglevel = KNO_FIX2INT(ll);
  else {
    u8_log(LOG_WARN,"BadLogLevel",
           "Invalid loglevel %q for pool %s",ll,id);
    ix->index_loglevel = -1;}
  kno_decref(ll);

  u8_init_mutex(&(ix->index_commit_lock));
}

KNO_EXPORT int kno_index_set_metadata(kno_index ix,lispval metadata)
{
  if (KNO_SLOTMAPP(metadata)) {
    kno_copy_slotmap((kno_slotmap)metadata,&(ix->index_metadata));
    ix->index_keyslot = kno_get(metadata,KNOSYM_KEYSLOT,VOID);}
  else {
    KNO_INIT_STATIC_CONS(&(ix->index_metadata),kno_slotmap_type);
    kno_init_slotmap(&(ix->index_metadata),17,NULL);
    ix->index_keyslot = VOID;}
  ix->index_metadata.table_modified = 0;
  return 0;
}

KNO_EXPORT void kno_reset_index_tables
(kno_index ix,ssize_t csize,ssize_t esize,ssize_t asize)
{
  int readonly = U8_BITP(ix->index_flags,KNO_STORAGE_READ_ONLY);
  kno_hashtable cache = &(ix->index_cache);
  kno_hashtable adds = &(ix->index_adds);
  kno_hashtable drops = &(ix->index_drops);
  kno_hashtable replace = &(ix->index_stores);
  kno_reset_hashtable(cache,((csize==0)?(kno_index_cache_init):(csize)),1);
  if (adds->table_n_keys==0) {
    ssize_t level = (readonly)?(0):(asize==0)?(kno_index_adds_init):(asize);
    kno_reset_hashtable(adds,level,1);}
  if (drops->table_n_keys==0) {
    ssize_t level = (readonly)?(0):(esize==0)?(kno_index_drops_init):(esize);
    kno_reset_hashtable(drops,level,1);}
  if (replace->table_n_keys==0) {
    ssize_t level = (readonly)?(0):(esize==0)?(kno_index_replace_init):(esize);
    kno_reset_hashtable(replace,level,1);}

}

static void display_index(u8_output out,kno_index ix,lispval lix)
{
  u8_byte edits[64];
  u8_string tag = (CONSP(lix)) ? ("CONSINDEX") : ("INDEX");
  u8_string type = (ix->index_typeid) ? (ix->index_typeid) :
    ((ix->index_handler) && (ix->index_handler->name)) ?
    (ix->index_handler->name) : ((u8_string)("notype"));
  u8_string id     = ix->indexid;
  u8_string source = ix->index_source;
  u8_string useid  =
    ( (strchr(id,':')) || (strchr(id,'@')) ) ? (id) :
    (u8_strchr(id,'/',1)) ? ((u8_strchr(id,'/',-1))+1) :
    (id);
  int cached = ix->index_cache.table_n_keys;
  int added = ix->index_adds.table_n_keys;
  int dropped = ix->index_drops.table_n_keys;
  int replaced = ix->index_stores.table_n_keys;
  strcpy(edits,"~"); u8_itoa10(replaced+dropped,edits+1);
  if ( (source) && (strchr(source,'\n')) ) {
    u8_string eol = strchr(source,'\n');
    size_t prefix_len = eol-source;
    u8_byte buf[prefix_len+1];
    strncpy(buf,source,prefix_len);
    buf[prefix_len]='\0';
    u8_printf(out,_("#<%s %s (%s) cx=%d+%d%s #!%llx \"%s...\">"),
              tag,useid,type,cached,added,edits,lix,buf);}
  else if (source)
    u8_printf(out,_("#<%s %s (%s) cx=%d+%d%s #!%llx \"%s\">"),
              tag,useid,type,cached,added,edits,lix,source);
  else u8_printf(out,_("#<%s %s (%s) cx=%d+%d%s #!%llx>"),
                 tag,useid,type,cached,added,edits,lix);
}
static int unparse_index(u8_output out,lispval x)
{
  kno_index ix = kno_indexptr(x);
  if (ix == NULL) return 0;
  display_index(out,ix,x);
  return 1;
}

static int unparse_consed_index(u8_output out,lispval x)
{
  kno_index ix = (kno_index)(x);
  if (ix == NULL) return 0;
  if (ix == NULL) return 0;
  display_index(out,ix,x);
  return 1;
}

static lispval index_parsefn(int n,lispval *args,kno_compound_typeinfo e)
{
  kno_index ix = NULL;
  if (n<2) return VOID;
  else if (STRINGP(args[2]))
    ix = kno_get_index(KNO_STRING_DATA(args[2]),0,VOID);
  if (ix)
    return kno_index_ref(ix);
  else return kno_err(kno_CantParseRecord,"index_parsefn",NULL,VOID);
}

/* Operations over all indexes */

static lispval index2lisp(kno_index ix)
{
  if (ix->index_serialno>=0)
    return LISPVAL_IMMEDIATE(kno_index_type,ix->index_serialno);
  else {
    lispval v = (lispval) ix;
    kno_incref(v);
    return v;}
}

KNO_EXPORT lispval kno_get_all_indexes()
{
  lispval results = EMPTY;
  int i = 0; while (i < kno_n_primary_indexes) {
    lispval lindex = index2lisp(kno_primary_indexes[i]);
    CHOICE_ADD(results,lindex);
    i++;}

  if (i>=kno_n_primary_indexes) {
    u8_read_lock(&indexes_lock);
    i = 0; while (i < kno_n_secondary_indexes) {
      lispval lindex = index2lisp(secondary_indexes[i]);
      CHOICE_ADD(results,lindex);
      i++;}
    u8_rw_unlock(&indexes_lock);}

  if (n_consed_indexes) {
    u8_lock_mutex(&consed_indexes_lock);
    int j = 0; while (j<n_consed_indexes) {
      kno_index ix = consed_indexes[j++];
      if (ix) {
        lispval lindex = index2lisp(ix);
        CHOICE_ADD(results,lindex);}
      else _kno_debug(results);}
    u8_unlock_mutex(&consed_indexes_lock);}

  return kno_simplify_choice(results);
}

KNO_EXPORT int kno_for_indexes(int (*fcn)(kno_index ix,void *),void *data)
{
  int total = 0;
  lispval all_indexes = kno_get_all_indexes();
  DO_CHOICES(lindex,all_indexes) {
    kno_index ix = kno_lisp2index(lindex);
    int retval = fcn(ix,data);
    total++;
    if (retval<0) {
      KNO_STOP_DO_CHOICES;
      kno_decref(all_indexes);
      return retval;}
    else if (retval) {
      KNO_STOP_DO_CHOICES;
      break;}}
  kno_decref(all_indexes);
  return total;
}

static int swapout_index_handler(kno_index ix,void *data)
{
  kno_index_swapout(ix,VOID);
  return 0;
}


KNO_EXPORT void kno_swapout_indexes()
{
  kno_for_indexes(swapout_index_handler,NULL);
}

static int close_index_handler(kno_index ix,void *data)
{
  kno_index_close(ix);
  return 0;
}

KNO_EXPORT void kno_close_indexes()
{
  kno_for_indexes(close_index_handler,NULL);
}

static int commit_each_index(kno_index ix,void *data)
{
  /* Phased indexes shouldn't be committed by themselves */
  if ( (ix->index_flags) & (KNO_STORAGE_PHASED) ) return 0;
  int *count = (int *) data;
  if (ix->index_handler->commit==NULL) return 0;
  int retval = kno_commit_index(ix);
  if (retval<0)
    return retval;
  else *count += retval;
  return 0;
}

KNO_EXPORT int kno_commit_indexes()
{
  int count=0;
  int rv=kno_for_indexes(commit_each_index,&count);
  if (rv<0) return rv;
  else return count;
}

KNO_EXPORT int kno_commit_indexes_noerr()
{
  int count=0;
  int rv=kno_for_indexes(commit_each_index,&count);
  if (rv<0) return rv;
  else return count;
}

static int accumulate_cachecount(kno_index ix,void *ptr)
{
  int *count = (int *)ptr;
  *count = *count+ix->index_cache.table_n_keys;
  return 0;
}

KNO_EXPORT
long kno_index_cache_load()
{
  int result = 0, retval;
  retval = kno_for_indexes(accumulate_cachecount,(void *)&result);
  if (retval<0) return retval;
  else return result;
}

static int accumulate_cached(kno_index ix,void *ptr)
{
  lispval *vals = (lispval *)ptr;
  lispval keys = kno_hashtable_keys(&(ix->index_cache));
  CHOICE_ADD(*vals,keys);
  return 0;
}

KNO_EXPORT
lispval kno_cached_keys(kno_index ix)
{
  if (ix == NULL) {
    int retval; lispval result = EMPTY;
    retval = kno_for_indexes(accumulate_cached,(void *)&result);
    if (retval<0) {
      kno_decref(result);
      return KNO_ERROR;}
    else return result;}
  else return kno_hashtable_keys(&(ix->index_cache));
}

/* IPEVAL delay execution */

KNO_EXPORT int kno_execute_index_delays(kno_index ix,void *data)
{
  lispval *delays = get_index_delays();
  lispval todo = delays[ix->index_serialno];
  if (EMPTYP(todo)) return 0;
  else {
    int retval = -1;
    /* u8_lock_mutex(&(kno_ipeval_lock)); */
    todo = delays[ix->index_serialno];
    delays[ix->index_serialno]=EMPTY;
    /* u8_unlock_mutex(&(kno_ipeval_lock)); */
#if KNO_TRACE_IPEVAL
    if (kno_trace_ipeval>1)
      u8_logf(LOG_DEBUG,ipeval_ixfetch,"Fetching %d keys from %s: %q",
              KNO_CHOICE_SIZE(todo),ix->indexid,todo);
    else
#endif
      retval = kno_index_prefetch(ix,todo);
#if KNO_TRACE_IPEVAL
    if (kno_trace_ipeval)
      u8_logf(LOG_DEBUG,ipeval_ixfetch,"Fetched %d keys from %s",
              KNO_CHOICE_SIZE(todo),ix->indexid);
#endif
    if (retval<0) return retval;
    else return 0;}
}

KNO_EXPORT void kno_recycle_index(struct KNO_INDEX *ix)
{
  struct KNO_INDEX_HANDLER *handler = ix->index_handler;
  drop_consed_index(ix);
  if (handler->recycle) handler->recycle(ix);
  kno_recycle_hashtable(&(ix->index_cache));
  kno_recycle_hashtable(&(ix->index_adds));
  kno_recycle_hashtable(&(ix->index_drops));
  kno_recycle_hashtable(&(ix->index_stores));
  u8_free(ix->indexid);
  if (ix->index_source) u8_free(ix->index_source);
  if (ix->index_typeid) u8_free(ix->index_typeid);

  kno_free_slotmap(&(ix->index_metadata));
  kno_free_slotmap(&(ix->index_props));

  kno_decref(ix->index_covers_slotids);
  kno_decref(ix->index_opts);
}

static void recycle_consed_index(struct KNO_RAW_CONS *c)
{
  kno_index ix = (kno_index) c;
  if (ix->index_serialno >= 0) {
    u8_log(LOG_WARN,"RecylingEternalIndex",
           "There was an attempt to free the index %s, supposedly eternal",
           ix->indexid);
    return;}
  else {
    kno_recycle_index(ix);
    u8_free(c);}
}

static lispval copy_consed_index(lispval x,int deep)
{
  return kno_incref(x);
}

static int valid_keyslotp(lispval defslot)
{
  if ( (KNO_OIDP(defslot)) || (KNO_SYMBOLP(defslot)) )
    return 1;
  else if (KNO_AMBIGP(defslot)) {
    DO_CHOICES(ks,defslot) {
      if (! ( (KNO_OIDP(ks)) || (KNO_SYMBOLP(ks)) ) ) {
        KNO_STOP_DO_CHOICES;
        return 0;}}
    return 1;}
  else return 0;
}

KNO_EXPORT lispval kno_default_indexctl(kno_index ix,lispval op,
                                      int n,lispval *args)
{
  if ((n>0)&&(args == NULL))
    return kno_err("BadIndexOpCall","kno_default_indexctl",ix->indexid,VOID);
  else if (n<0)
    return kno_err("BadIndexOpCall","kno_default_indexctl",ix->indexid,VOID);
  else if (op == KNOSYM_OPTS)  {
    lispval opts = ix->index_opts;
    if (n > 1)
      return kno_err(kno_TooManyArgs,"kno_default_indexctl",ix->indexid,VOID);
    else if ( (opts == KNO_NULL) || (VOIDP(opts) ) )
      return KNO_FALSE;
    else if ( n == 1 )
      return kno_getopt(opts,args[0],KNO_FALSE);
    else return kno_incref(opts);}
  else if (op == kno_metadata_op) {
    lispval metadata = ((lispval)&(ix->index_metadata));
    lispval slotid = (n>0) ? (args[0]) : (KNO_VOID);
    /* TODO: check that slotid isn't any of the slots returned by default */
    if (n == 0)
      return kno_index_base_metadata(ix);
    else if (n == 1) {
      lispval extended=kno_index_ctl(ix,kno_metadata_op,0,NULL);
      lispval v = kno_get(extended,args[0],KNO_EMPTY);
      kno_decref(extended);
      return v;}
    else if (n == 2) {
      lispval extended=kno_index_ctl(ix,kno_metadata_op,0,NULL);
      if (kno_test(extended,KNOSYM_READONLY,slotid)) {
        kno_decref(extended);
        return kno_err("ReadOnlyMetadataProperty","kno_default_indexctl",
                      ix->indexid,slotid);}
      else kno_decref(extended);
      int rv=kno_store(metadata,slotid,args[1]);
      if (rv<0)
        return KNO_ERROR_VALUE;
      else return kno_incref(args[1]);}
    else return kno_err(kno_TooManyArgs,"kno_index_ctl/metadata",
                       KNO_SYMBOL_NAME(op),kno_index2lisp(ix));}
  else if (op == KNOSYM_PROPS) {
    lispval props = (lispval) &(ix->index_props);
    lispval slotid = (n>0) ? (args[0]) : (KNO_VOID);
    if (n == 0)
      return kno_copier(props,0);
    else if (n == 1)
      return kno_get(props,slotid,KNO_EMPTY);
    else if (n == 2) {
      int rv=kno_store(props,slotid,args[1]);
      if (rv<0)
        return KNO_ERROR_VALUE;
      else return kno_incref(args[1]);}
    else return kno_err(kno_TooManyArgs,"kno_index_ctl/props",
                       KNO_SYMBOL_NAME(op),kno_index2lisp(ix));}
  else if (op == KNOSYM_KEYSLOT) {
    if (n == 0) {
      if ( (KNO_NULLP(ix->index_keyslot)) || (KNO_VOIDP(ix->index_keyslot)) )
        return KNO_FALSE;
      else return kno_incref(ix->index_keyslot);}
    else if (n == 1) {
      lispval defslot = args[0];
      if (KNO_FALSEP(defslot)) {
        lispval old = ix->index_keyslot;
        ix->index_keyslot = KNO_VOID;
        if (KNO_VOIDP(old))
          return KNO_FALSE;
        else return old;}
      if (!( (KNO_NULLP(ix->index_keyslot)) ||
             (KNO_VOIDP(ix->index_keyslot)) )) {
        if ( (defslot == ix->index_keyslot) ||
             ( (KNO_CHOICEP(defslot)) && (KNO_CHOICEP(ix->index_keyslot)) &&
               (KNO_EQUALP(defslot,(ix->index_keyslot))) ) ) {
          u8_logf(LOG_NOTICE,"KeySlotOK",
                  "The keyslot of %s is already %q",ix->indexid,defslot);
          return kno_incref(defslot);}
        else return kno_err("KeySlotAlreadyDefined",
                           "kno_default_indexctl/keyslot",
                           ix->indexid,ix->index_keyslot);}
      else if ((KNO_OIDP(defslot)) || (KNO_SYMBOLP(defslot)) ||
               (valid_keyslotp(defslot))) {
        if (ix->index_handler->commit) {
          /* If the index persists itself (has a commit method),
             add the new keyslot to the metadata, so it will be
             saved. */
          lispval metadata = ((lispval)&(ix->index_metadata));
          kno_store(metadata,KNOSYM_KEYSLOT,defslot);}
        ix->index_keyslot=defslot;
        return kno_incref(defslot);}
      else return kno_type_error("slotid","kno_default_indexctl/keyslot",
                                defslot);}
    else return kno_err(kno_TooManyArgs,"kno_index_ctl/keyslot",
                       KNO_SYMBOL_NAME(op),kno_index2lisp(ix));}
  else if (op == KNOSYM_LOGLEVEL) {
    if (n == 0) {
      if (ix->index_loglevel < 0)
        return KNO_FALSE;
      else return KNO_INT(ix->index_loglevel);}
    else if (n == 1) {
      if (FIXNUMP(args[0])) {
        long long level = KNO_FIX2INT(args[0]);
        if ((level<0) || (level > 128))
          return kno_err(kno_RangeError,
                        "kno_default_indexctl",ix->indexid,args[0]);
        else {
          int old_loglevel = ix->index_loglevel;
          ix->index_loglevel = level;
          if (old_loglevel<0)
            return KNO_FALSE;
          else return KNO_INT(old_loglevel);}}
      else return kno_type_error("loglevel","kno_default_indexctl",args[0]);}
    else return kno_err(kno_TooManyArgs,"kno_default_indexctl",ix->indexid,VOID);}
  else if (op == kno_keys_op)
    return kno_index_keys(ix);
  else if (op == kno_partitions_op)
    return KNO_EMPTY;
  else if (op == kno_raw_metadata_op)
    return kno_deep_copy((lispval) &(ix->index_metadata));
  else if (op == KNOSYM_CACHELEVEL)
    return KNO_INT2FIX(1);
  else if (op == KNOSYM_READONLY) {
    if (KNO_INDEX_READONLYP(ix))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

/* Initialize */

kno_ptr_type kno_consed_index_type;

static int check_index(lispval x)
{
  int serial = KNO_GET_IMMEDIATE(x,kno_index_type);
  if (serial<0) return 0;
  if (serial<KNO_N_PRIMARY_INDEXES)
    if (kno_primary_indexes[serial])
      return 1;
    else return 0;
  else if (secondary_indexes) {
    int second_off=serial-KNO_N_PRIMARY_INDEXES;
    if ( (second_off < 0) || (second_off  > kno_n_secondary_indexes) )
      return 0;
    else return (secondary_indexes[second_off]!=NULL);}
  else return 0;
}

/* Initializations */

KNO_EXPORT void kno_init_indexes_c()
{
  u8_register_source_file(_FILEINFO);

  kno_type_names[kno_index_type]=_("index");
  kno_immediate_checkfns[kno_index_type]=check_index;

  kno_consed_index_type = kno_register_cons_type("raw index");
  kno_type_names[kno_consed_index_type]=_("raw index");

  u8_init_mutex(&consed_indexes_lock);
  consed_indexes = u8_malloc(64*sizeof(kno_index));
  consed_indexes_len=64;

  secondary_indexes     = u8_malloc(64*sizeof(kno_index));
  secondary_indexes_len = 64;

  kno_index_hashop=kno_intern("hash");
  kno_index_slotsop=kno_intern("slotids");
  kno_index_bucketsop=kno_intern("buckets");

  cachelevel_slot=kno_intern("cachelevel");
  indexid_slot=kno_intern("indexid");
  source_slot=kno_intern("source");
  realpath_slot=kno_intern("realpath");
  cached_slot=kno_intern("cached");
  adds_slot=kno_intern("adds");
  edits_slot=kno_intern("edits");
  drops_slot=kno_intern("drops");
  replaced_slot=kno_intern("replaced");
  flags_slot=kno_intern("flags");
  registered_slot=kno_intern("registered");
  opts_slot=kno_intern("opts");

  read_only_flag=KNOSYM_READONLY;
  unregistered_flag=kno_intern("unregistered");
  registered_flag=kno_intern("registered");
  noswap_flag=kno_intern("noswap");
  noerr_flag=kno_intern("noerr");
  phased_flag=kno_intern("phased");
  background_flag=kno_intern("background");

  {
    struct KNO_COMPOUND_TYPEINFO *e =
      kno_register_compound(kno_intern("index"),NULL,NULL);
    e->compound_parser = index_parsefn;}

  kno_tablefns[kno_index_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_index_type]->get = table_indexget;
  kno_tablefns[kno_index_type]->add = table_indexadd;
  kno_tablefns[kno_index_type]->drop = table_indexdrop;
  kno_tablefns[kno_index_type]->store = table_indexstore;
  kno_tablefns[kno_index_type]->test = NULL;
  kno_tablefns[kno_index_type]->keys = table_indexkeys;
  kno_tablefns[kno_index_type]->getsize = NULL;

  kno_tablefns[kno_consed_index_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_consed_index_type]->get = table_indexget;
  kno_tablefns[kno_consed_index_type]->add = table_indexadd;
  kno_tablefns[kno_consed_index_type]->drop = table_indexdrop;
  kno_tablefns[kno_consed_index_type]->store = table_indexstore;
  kno_tablefns[kno_consed_index_type]->test = NULL;
  kno_tablefns[kno_consed_index_type]->keys = table_indexkeys;
  kno_tablefns[kno_consed_index_type]->getsize = NULL;

  kno_recyclers[kno_consed_index_type]=recycle_consed_index;
  kno_unparsers[kno_consed_index_type]=unparse_consed_index;
  kno_copiers[kno_consed_index_type]=copy_consed_index;

  kno_unparsers[kno_index_type]=unparse_index;
  metadata_readonly_props = kno_intern("_readonly_props");

  u8_init_rwlock(&indexes_lock);
  u8_init_mutex(&background_lock);

#if (KNO_USE_TLS)
  u8_new_threadkey(&index_delays_key,NULL);
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
