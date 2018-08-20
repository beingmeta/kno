/* -*- Mode: C; Character-encoding: utf-8; fill-column: 95; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_INDEXES 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_IPEVAL 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"
#include "framerd/apply.h"

#include <libu8/libu8.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>

u8_condition fd_EphemeralIndex=_("ephemeral index");
u8_condition fd_ReadOnlyIndex=_("read-only index");
u8_condition fd_NoFileIndexes=_("file indexes are not supported");
u8_condition fd_NotAFileIndex=_("not a file index");
u8_condition fd_BadIndexSpec=_("bad index specification");
u8_condition fd_CorruptedIndex=_("corrupted index file");
u8_condition fd_IndexCommitError=_("can't save changes to index");

lispval fd_index_hashop, fd_index_slotsop, fd_index_bucketsop;

u8_condition fd_IndexCommit=_("Index/Commit");
static u8_condition ipeval_ixfetch="IXFETCH";

#define htget(ht,key)                                                   \
  ( (ht->table_n_keys) ? (fd_hashtable_get(ht,key,EMPTY)) : (EMPTY) )

#define getloglevel(ix) ()
#define elapsed_time(since) (u8_elapsed_time()-(since))

static int index_loglevel(fd_index ix)
{
  return (ix->index_loglevel > 0) ? (ix->index_loglevel) :
    (fd_storage_loglevel);
}

static lispval edit_result(lispval key,lispval result,
                           fd_hashtable adds,fd_hashtable drops)
{
  lispval added = htget(adds,key);
  CHOICE_ADD(result,added);
  if (drops->table_n_keys) {
    lispval dropped = fd_hashtable_get(drops,key,EMPTY);
    if (!(EMPTYP(dropped))) {
      lispval new_result = fd_difference(result,dropped);
      fd_decref(dropped);
      fd_decref(result);
      result=new_result;}}
  return result;
}

int fd_index_cache_init    = FD_INDEX_CACHE_INIT;
int fd_index_adds_init     = FD_INDEX_ADDS_INIT;
int fd_index_drops_init    = FD_INDEX_DROPS_INIT;
int fd_index_replace_init  = FD_INDEX_REPLACE_INIT;

static u8_rwlock indexes_lock;
fd_index fd_primary_indexes[FD_N_PRIMARY_INDEXES];
int fd_n_primary_indexes = 0;
int fd_n_secondary_indexes = 0;
static int secondary_indexes_len = 0;
static fd_index *secondary_indexes = NULL;

static fd_index *consed_indexes = NULL;
ssize_t n_consed_indexes = 0, consed_indexes_len=0;
u8_mutex consed_indexes_lock;

static int add_consed_index(fd_index ix)
{
  u8_lock_mutex(&consed_indexes_lock);
  fd_index *scan=consed_indexes, *limit=scan+n_consed_indexes;
  while (scan<limit) {
    if ( *scan == ix ) {
      u8_unlock_mutex(&consed_indexes_lock);
      return 0;}
    else scan++;}
  if (n_consed_indexes>=consed_indexes_len) {
    ssize_t new_len=consed_indexes_len+64;
    fd_index *newvec=u8_realloc(consed_indexes,new_len*sizeof(fd_index));
    if (newvec) {
      consed_indexes=newvec;
      consed_indexes_len=new_len;}
    else {
      u8_seterr(fd_MallocFailed,"add_consed_index",u8dup(ix->indexid));
      u8_unlock_mutex(&consed_indexes_lock);
      return -1;}}
  fd_incref((lispval)ix);
  consed_indexes[n_consed_indexes++]=ix;
  u8_unlock_mutex(&consed_indexes_lock);
  return 1;
}

static int drop_consed_index(fd_index ix)
{
  u8_lock_mutex(&consed_indexes_lock);
  fd_index *scan=consed_indexes, *limit=scan+n_consed_indexes;
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

static struct FD_AGGREGATE_INDEX _background;
struct FD_AGGREGATE_INDEX *fd_background = &_background;
static u8_mutex background_lock;

#if FD_GLOBAL_IPEVAL
static lispval *index_delays;
#elif FD_USE__THREAD
static __thread lispval *index_delays;
#elif FD_USE_TLS
static u8_tld_key index_delays_key;
#else
static lispval *index_delays;
#endif

#if ((FD_USE_TLS) && (!(FD_GLOBAL_IPEVAL)))
#define get_index_delays()                      \
  ((lispval *)u8_tld_get(index_delays_key))
FD_EXPORT void fd_init_index_delays()
{
  lispval *delays = (lispval *)u8_tld_get(index_delays_key);
  if (delays) return;
  else {
    int i = 0;
    delays = u8_alloc_n(FD_N_INDEX_DELAYS,lispval);
    while (i<FD_N_INDEX_DELAYS) delays[i++]=EMPTY;
    u8_tld_set(index_delays_key,delays);}

}
#else
#define get_index_delays() (index_delays)
FD_EXPORT void fd_init_index_delays()
{
  if (index_delays) return;
  else {
    int i = 0;
    index_delays = u8_alloc_n(FD_N_INDEX_DELAYS,lispval);
    while (i<FD_N_INDEX_DELAYS) index_delays[i++]=EMPTY;}

}
#endif

static void clear_bg_cache(lispval key)
{
  fd_hashtable bg_cache = &(fd_background->index_cache);
  if (bg_cache->table_n_keys == 0) {}
  else if (FD_CHOICEP(key)) {
    const lispval *keys = FD_CHOICE_DATA(key);
    unsigned int n = FD_CHOICE_SIZE(key);
    fd_hashtable_iterkeys
      (bg_cache,fd_table_replace,n,keys,VOID);}
  else fd_hashtable_op(bg_cache,fd_table_replace,key,VOID);
}

FD_EXPORT lispval *fd_get_index_delays() { return get_index_delays(); }

/* Index ops */

FD_EXPORT lispval fd_index_ctl(fd_index x,lispval indexop,int n,lispval *args)
{
  struct FD_INDEX_HANDLER *h = x->index_handler;
  if (h->indexctl)
    return h->indexctl(x,indexop,n,args);
  else return FD_FALSE;
}

/* Index cache levels */

/* a cache_level<0 indicates no caching has been done */

FD_EXPORT void fd_index_setcache(fd_index ix,int level)
{
  lispval intarg = FD_INT(level);
  lispval result = fd_index_ctl(ix,fd_cachelevel_op,1,&intarg);
  if (FD_ABORTP(result)) {fd_clear_errors(1);}
  fd_decref(result);
  ix->index_cache_level = level;
}

static void init_cache_level(fd_index ix)
{
  if (PRED_FALSE(ix->index_cache_level<0)) {
    lispval opts = ix->index_opts;
    long long level=fd_getfixopt(opts,"CACHELEVEL",fd_default_cache_level);
    fd_index_setcache(ix,level);}
}

/* The index registry */

FD_EXPORT void fd_register_index(fd_index ix)
{
  if ( (FD_VOIDP(ix->index_keyslot)) && (ix->index_metadata.n_slots) ) {
    lispval keyslotid =
      fd_slotmap_get(&(ix->index_metadata),FDSYM_KEYSLOT,FD_VOID);
    if (FD_VOIDP(keyslotid)) {}
    else if ( (FD_SYMBOLP(keyslotid)) || (FD_OIDP(keyslotid)) )
      ix->index_keyslot=keyslotid;
    else u8_logf(LOG_WARN,"BadKeySlot",
                 "Ignoring invalid keyslot from metadata: %q",keyslotid);
    fd_decref(keyslotid);}
  if (ix->index_flags&FD_STORAGE_UNREGISTERED)
    return;
  else if (ix->index_serialno<0) {
    u8_write_lock(&indexes_lock);
    if (ix->index_serialno>=0) { /* Handle race condition */
      u8_rw_unlock(&indexes_lock);
      return;}
    if (fd_n_primary_indexes<FD_N_PRIMARY_INDEXES) {
      ix->index_serialno = fd_n_primary_indexes;
      fd_primary_indexes[fd_n_primary_indexes++]=ix;}
    else {
      if (fd_n_secondary_indexes >= secondary_indexes_len) {
        secondary_indexes_len = secondary_indexes_len*2;
        secondary_indexes = u8_realloc_n
          (secondary_indexes,secondary_indexes_len,fd_index);}
      secondary_indexes[fd_n_secondary_indexes++]=ix;}
    FD_SET_REFCOUNT(ix,0);}
  /* Make it a static cons */
  u8_rw_unlock(&indexes_lock);
  if ((ix->index_flags)&(FD_INDEX_IN_BACKGROUND))
    fd_add_to_background(ix);
}

FD_EXPORT lispval _fd_index2lisp(fd_index ix)
{
  return fd_index2lisp(ix);
}

FD_EXPORT fd_index _fd_indexptr(lispval lix)
{
  return fd_indexptr(lix);
}

FD_EXPORT lispval fd_index_ref(fd_index ix)
{
  if (ix == NULL)
    return FD_ERROR;
  else if (ix->index_serialno>=0)
    return LISPVAL_IMMEDIATE(fd_index_type,ix->index_serialno);
  else {
    lispval lix=(lispval)ix;
    fd_incref(lix);
    return lix;}
}
FD_EXPORT fd_index fd_lisp2index(lispval lix)
{
  if (FD_ABORTP(lix))
    return NULL;
  else if (TYPEP(lix,fd_index_type)) {
    int serial = FD_GET_IMMEDIATE(lix,fd_index_type);
    if (serial<FD_N_PRIMARY_INDEXES)
      return fd_primary_indexes[serial];
    else if ( (fd_n_secondary_indexes == 0) ||
              (serial >= (fd_n_secondary_indexes+FD_N_PRIMARY_INDEXES)) )
      return NULL;
    else {
      u8_read_lock(&indexes_lock);
      fd_index index = secondary_indexes[serial-FD_N_PRIMARY_INDEXES];
      u8_rw_unlock(&indexes_lock);
      return index;}}
  else if (TYPEP(lix,fd_consed_index_type))
    return (fd_index) lix;
  else {
    fd_seterr(fd_TypeError,_("not an index"),NULL,lix);
    return NULL;}
}

FD_EXPORT fd_index _fd_ref2index(lispval indexval)
{
  if (FD_ABORTP(indexval))
    return NULL;
  else if (TYPEP(indexval,fd_index_type)) {
    int serial = FD_GET_IMMEDIATE(indexval,fd_index_type);
    if (serial<FD_N_PRIMARY_INDEXES)
      return fd_primary_indexes[serial];
    else if ( (fd_n_secondary_indexes == 0) ||
              (serial >= (fd_n_secondary_indexes+FD_N_PRIMARY_INDEXES)) )
      return NULL;
    else {
      u8_read_lock(&indexes_lock);
      fd_index index = secondary_indexes[serial-FD_N_PRIMARY_INDEXES];
      u8_rw_unlock(&indexes_lock);
      return index;}}
  else return NULL;
}

/* Finding indexes by ids/sources/etc */

FD_EXPORT fd_index fd_find_index(u8_string spec)
{
  fd_index ix=fd_find_index_by_source(spec);
  if (ix) return ix;
  else if ((ix=fd_find_index_by_id(spec)))
    return ix;
  /* TODO: Add generic method which uses the index matcher
     methods to find indexes */
  else return ix;
}
FD_EXPORT u8_string fd_locate_index(u8_string spec)
{
  fd_index ix = fd_find_index_by_source(spec);
  if (ix) return u8_strdup(ix->index_source);
  else if ((ix=fd_find_index_by_id(spec)))
    return u8_strdup(ix->index_source);
  /* TODO: Add generic method which uses the index matcher
     methods to find indexes */
  else return NULL;
}

static int match_index_source(fd_index ix,u8_string source)
{
  return ( (fd_same_sourcep(source,ix->index_source)) ||
           (fd_same_sourcep(source,ix->canonical_source)) );
}

static int match_index_id(fd_index ix,u8_string id)
{
  return ((id)&&(ix->indexid)&&
          (strcmp(ix->indexid,id) == 0));
}

FD_EXPORT fd_index fd_find_index_by_id(u8_string id)
{
  int i = 0;
  if (id == NULL)
    return NULL;
  else while (i<fd_n_primary_indexes)
         if (match_index_id(fd_primary_indexes[i],id))
           return fd_primary_indexes[i];
         else i++;
  if (secondary_indexes == NULL)
    return NULL;
  u8_read_lock(&indexes_lock);
  i = 0; while (i<fd_n_secondary_indexes)
           if (match_index_id(secondary_indexes[i],id)) {
             fd_index ix = secondary_indexes[i];
             u8_rw_unlock(&indexes_lock);
             return ix;}
           else i++;
  u8_rw_unlock(&indexes_lock);
  return NULL;
}
FD_EXPORT fd_index fd_find_index_by_source(u8_string source)
{
  int i = 0;
  if (source == NULL)
    return NULL;
  else while (i<fd_n_primary_indexes)
         if (match_index_source(fd_primary_indexes[i],source))
           return fd_primary_indexes[i];
         else i++;
  if (secondary_indexes == NULL)
    return NULL;
  u8_read_lock(&indexes_lock);
  i = 0; while (i<fd_n_secondary_indexes)
           if (match_index_source(secondary_indexes[i],source)) {
             fd_index ix = secondary_indexes[i];
             u8_rw_unlock(&indexes_lock);
             return ix;}
           else i++;
  u8_rw_unlock(&indexes_lock);
  return NULL;
}

FD_EXPORT fd_index fd_get_index
(u8_string spec,fd_storage_flags flags,lispval opts)
{
  if (strchr(spec,';')) {
    fd_index ix = NULL;
    u8_byte *copy = u8_strdup(spec);
    u8_byte *start = copy, *brk = strchr(start,';');
    while (brk) {
      if (ix == NULL) {
        *brk='\0'; ix = fd_open_index(start,flags,opts);
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
      return fd_open_index(spec+start_off,flags,opts);}
    else return NULL;}
  else return fd_open_index(spec,flags,opts);
}

/* Background indexes */

FD_EXPORT int fd_add_to_background(fd_index ix)
{
  if (ix == NULL) return 0;
  if (ix->index_serialno<0) {
    lispval lix = (lispval)ix;
    fd_incref(lix);}
  u8_lock_mutex(&background_lock);
  ix->index_flags = ix->index_flags|FD_INDEX_IN_BACKGROUND;
  fd_add_to_aggregate_index(fd_background,ix);
  u8_string old_id = fd_background->indexid;
  fd_background->indexid =
    u8_mkstring("background+%d",fd_background->ax_n_indexes);
  if (old_id) u8_free(old_id);

  u8_unlock_mutex(&background_lock);
  return 1;
}

FD_EXPORT fd_index fd_use_index(u8_string spec,
                                fd_storage_flags flags,
                                lispval opts)
{
  flags&=~FD_STORAGE_UNREGISTERED;
  if (strchr(spec,';')) {
    fd_index ix = NULL;
    u8_byte *copy = u8_strdup(spec);
    u8_byte *start = copy, *end = strchr(start,';');
    *end='\0'; while (start) {
      ix = fd_get_index(start,flags,opts);
      if (ix == NULL) {
        u8_free(copy); return NULL;}
      else fd_add_to_background(ix);
      if ((end) && (end[1])) {
        start = end+1; end = strchr(start,';');
        if (end) *end='\0';}
      else start = NULL;}
    u8_free(copy);
    return ix;}
  else {
    fd_index ix = fd_get_index(spec,flags,opts);
    if (ix) fd_add_to_background(ix);
    return ix;}
}

/* Core functions */

static void delay_index_fetch(fd_index ix,lispval keys);

FD_EXPORT lispval fd_index_fetch(fd_index ix,lispval key)
{
  lispval result = EMPTY;
  int read_only = FD_INDEX_READONLYP(ix);
  fd_hashtable cache = &(ix->index_cache);
  fd_hashtable adds = &(ix->index_adds);
  fd_hashtable drops = &(ix->index_drops);
  fd_hashtable stores = &(ix->index_stores);
  lispval local_value = (stores->table_n_keys) ?
    (fd_hashtable_get(stores,key,VOID)) : (VOID);
  if (!(FD_VOIDP(local_value)))
    result=local_value;
  else if (ix->index_handler->fetch) {
    init_cache_level(ix);
    if ((fd_ipeval_status())&&
        ((ix->index_handler->prefetch)||
         (ix->index_handler->fetchn)))  {
      delay_index_fetch(ix,key);
      return EMPTY;}
    else result=ix->index_handler->fetch(ix,key);}
  else {}
  if (! read_only )
    result = edit_result(key,result,adds,drops);
  if (ix->index_cache_level>0) fd_hashtable_store(cache,key,result);
  return result;
}

static void delay_index_fetch(fd_index ix,lispval keys)
{
  struct FD_HASHTABLE *cache = &(ix->index_cache); int delay_count = 0;
  lispval *delays = get_index_delays(), *delayp = &(delays[ix->index_serialno]);
  DO_CHOICES(key,keys) {
    if (fd_hashtable_probe(cache,key)) {}
    else {
      fd_incref(key);
      CHOICE_ADD((*delayp),key);
      delay_count++;}}
  if (delay_count) (void)fd_ipeval_delay(delay_count);
}

FD_EXPORT void fd_delay_index_fetch(fd_index ix,lispval keys)
{
  delay_index_fetch(ix,keys);
}

static void cleanup_tmpchoice(struct FD_CHOICE *reduced)
{
  int refcount = FD_CONS_REFCOUNT(reduced);
  if (refcount>1) {
    lispval lptr = (lispval) reduced;
    lispval *vals = (lispval *) FD_XCHOICE_ELTS(reduced);
    /* We need to incref these values for wherever
       delay_index_fetch stores them. */
    fd_incref_vec(vals,FD_XCHOICE_SIZE(reduced));
    fd_incref(lptr);}
  else fd_free_choice(reduced);
}

FD_EXPORT int fd_index_prefetch(fd_index ix,lispval keys)
{
  // FDTC *fdtc = ((FD_USE_THREADCACHE)?(fd_threadcache):(NULL));
  struct FD_HASHTABLE *cache = &(ix->index_cache);
  struct FD_HASHTABLE *adds = &(ix->index_adds);
  struct FD_HASHTABLE *drops = &(ix->index_drops);
  struct FD_HASHTABLE *stores = &(ix->index_stores);
  int n_fetched = 0, cachelevel = 0, rv =0;
  int read_only = (ix->index_flags & (FD_STORAGE_READ_ONLY) );
  struct FD_CHOICE *reduced = NULL;
  lispval needed = EMPTY;

  if (ix == NULL) return -1;
  else if (FD_EMPTYP(keys)) return 0;
  else init_cache_level(ix);

  cachelevel = ix->index_cache_level;

  if (cachelevel < 1) return 0;

  int decref_keys = 0;
  if (FD_PRECHOICEP(keys)) {
    keys = fd_make_simple_choice(keys);
    if (FD_EMPTYP(keys)) return 0;
    decref_keys=1;}

  /* Handle non-choice, where you can either return or needed  = keys */
  if (!(FD_CHOICEP(keys))) {
    if (fd_hashtable_probe(cache,keys)) {
      if (decref_keys) fd_decref(keys);
      return 0;}
    else if (ix->index_handler->prefetch != NULL) {
      /* Prefetch handlers need to consult adds, drops, stores
         directly and also write directly into the cache */
      int rv = 0;
      if (fd_ipeval_status())
        delay_index_fetch(ix,keys);
      else rv = ix->index_handler->prefetch(ix,keys);
      if (decref_keys) fd_decref(keys);
      return rv;}
    else needed=keys;}
  else {
    /* Compute needed keys */
    int n = FD_CHOICE_SIZE(keys);
    const lispval *keyv = FD_CHOICE_DATA(keys);
    int partial = fd_hashtable_iterkeys(cache,fd_table_haskey,n,keyv,VOID);
    if (partial) {
      /* Some of the values are already in the cache */
      int atomicp = 1;
      reduced = fd_alloc_choice(n);
      lispval *elts = ((lispval *)(FD_CHOICE_ELTS(reduced))), *write = elts;
      FD_DO_CHOICES(key,keys) {
        if (! (fd_hashtable_probe(cache,key)) ) {
          if (fd_hashtable_probe(stores,key)) {
            lispval stored = fd_hashtable_get(stores,key,EMPTY);
            lispval local_value = edit_result(key,stored,adds,drops);
            fd_hashtable_op(cache,fd_table_store_noref,key,local_value);
            /* Not really fetched, but we count it anyway */
            n_fetched++;}
          else {
            if ( (atomicp) && (FD_CONSP(key)) ) atomicp = 0;
            *write++=key;
            fd_incref(key);}}}
      if (write == elts) {
        u8_big_free(reduced); reduced=NULL;
        if (decref_keys) fd_decref(keys);
        return n_fetched;}
      else {
        FD_INIT_XCHOICE(reduced,write-elts,atomicp);
        needed = (lispval) reduced;}}
    else needed = keys;}

  if (ix->index_handler->prefetch != NULL) {
    /* Prefetch handlers need to consult adds, drops, stores
       directly and also write directly into the cache */
    if (fd_ipeval_status())
      delay_index_fetch(ix,needed);
    else rv = ix->index_handler->prefetch(ix,needed);
    if (reduced) {cleanup_tmpchoice(reduced); reduced=NULL;}
    if (decref_keys) fd_decref(keys);
    if (rv>=0) rv = n_fetched + rv;}
  else if (ix->index_handler->fetchn == NULL) {
    /* No fetchn handler, fetch them individually */
    if (fd_ipeval_status()) {
      delay_index_fetch(ix,needed);
      rv = n_fetched;}
    else {
      DO_CHOICES(key,needed) {
        lispval v = fd_index_fetch(ix,key);
        if (FD_ABORTP(v)) {
          FD_STOP_DO_CHOICES;
          if (reduced) {cleanup_tmpchoice(reduced); reduced=NULL;}
          if (decref_keys) fd_decref(keys);
          return fd_interr(v);}
        if (! read_only ) v = edit_result(key,v,adds,drops);
        fd_hashtable_op(cache,fd_table_store_noref,key,v);
        /* if (fdtc) fdtc_store(ix,key,v); */
        n_fetched++;}
      rv=n_fetched;}}
  else if (fd_ipeval_status()) {
    delay_index_fetch(ix,needed);
    rv = n_fetched;}
  else if (FD_CHOICEP(needed)) {
    const lispval *to_fetch = FD_CHOICE_DATA(needed);
    int n_to_fetch = FD_CHOICE_SIZE(needed);
    lispval *fetched = ix->index_handler->fetchn(ix,n_to_fetch,to_fetch);
    if (fetched) {
      if ( ! read_only ) {
        int scan_i = 0; while (scan_i < n_to_fetch) {
          lispval key = to_fetch[scan_i], v = fetched[scan_i];
          lispval added = htget(adds,key), dropped = htget(drops,key);
          if (! ( (EMPTYP(added)) && (EMPTYP(dropped)) ) ) {
            CHOICE_ADD(v,added);
            if (!(EMPTYP(dropped))) {
              lispval newv = fd_difference(v,dropped);
              fd_decref(v);
              v=newv;}
            fetched[scan_i] = v;}
          scan_i++;}}
      fd_hashtable_iter(cache,fd_table_store_noref,n_to_fetch,to_fetch,fetched);
      u8_big_free(fetched);
      rv = n_fetched+n_to_fetch;}
    else rv = -1;}
  else {
    lispval fetched = ix->index_handler->fetch(ix,needed);
    lispval val = edit_result(needed,fetched,adds,drops);
    if (!(FD_ABORTED(val))) {
      fd_hashtable_store(cache,needed,val);
      /* fdtc_store(ix,needed,val); */
      fd_decref(val);
      rv = n_fetched+1;}
    else rv=-1;}
    if (reduced) {cleanup_tmpchoice(reduced); reduced=NULL;}
  if (decref_keys) fd_decref(keys);
  return rv;
}

FD_EXPORT lispval fd_index_fetchn(fd_index ix,lispval keys_arg)
{
  if (ix==NULL) return FD_EMPTY;
  else if ((ix->index_handler) && (ix->index_handler->fetchn)) {
    lispval keys = fd_make_simple_choice(keys_arg);
    if (FD_VECTORP(keys)) {
      int n = FD_VECTOR_LENGTH(keys);
      lispval *keyv = FD_VECTOR_ELTS(keys);
      lispval *values = ix->index_handler->fetchn(ix,n,keyv);
      fd_decref(keys);
      return fd_cons_vector(NULL,n,1,values);}
    else {
      int n = FD_CHOICE_SIZE(keys);
      if (n==0)
        return fd_make_hashtable(NULL,8);
      else if (n==1) {
        lispval table=fd_make_hashtable(NULL,16);
        lispval value = fd_index_fetch(ix,keys);
        fd_store(table,keys,value);
        fd_decref(value);
        fd_decref(keys);
        return table;}
      else {
        init_cache_level(ix);
        const lispval *keyv=FD_CHOICE_DATA(keys);
        const lispval *values=ix->index_handler->fetchn(ix,n,(lispval *)keyv);
        lispval table=fd_make_hashtable(NULL,n*2);
        fd_hashtable_iter((fd_hashtable)table,fd_table_store_noref,n,
                          keyv,values);
        u8_big_free((lispval *)values);
        fd_decref(keys);
        return table;}}}
  else if (ix->index_handler) {
    u8_seterr("NoHandler","fd_index_fetchn",u8_strdup(ix->indexid));
    return -1;}
  else return 0;
}

static int add_key_fn(lispval key,lispval val,void *data)
{
  lispval **write = (lispval **)data;
  *((*write)++) = fd_incref(key);
  /* Means keep going */
  return 0;
}

static int edit_key_fn(lispval key,lispval val,void *data)
{
  lispval **write = (lispval **)data;
  if (PAIRP(key)) {
    struct FD_PAIR *pair = (struct FD_PAIR *)key;
    if (pair->car == FDSYM_SET) {
      lispval real_key = pair->cdr;
      fd_incref(real_key);
      *((*write)++) = real_key;}}
  /* Means keep going */
  return 0;
}

FD_EXPORT lispval fd_index_keys(fd_index ix)
{
  if (ix->index_handler->fetchkeys) {
    int n_fetched = 0; init_cache_level(ix);
    lispval *fetched = ix->index_handler->fetchkeys(ix,&n_fetched);
    int n_replaced = ix->index_stores.table_n_keys;
    int n_added = ix->index_adds.table_n_keys;
    if (n_fetched == 0) {
      if ( (n_replaced) && (n_added) ) {
        lispval keys = fd_hashtable_keys(&(ix->index_adds));
        lispval replaced =  fd_hashtable_keys(&(ix->index_stores));
        CHOICE_ADD(keys,replaced);
        return fd_simplify_choice(keys);}
      else if (n_added)
        return fd_hashtable_keys(&(ix->index_adds));
      else if (n_replaced)
        return fd_hashtable_keys(&(ix->index_stores));
      else return EMPTY;}
    else {
      fd_choice result = fd_alloc_choice(n_fetched+n_added+n_replaced);
      lispval *write_start, *write_at;
      if (n_fetched)
        memcpy(&(result->choice_0),fetched,LISPVEC_BYTELEN(n_fetched));
      write_start = &(result->choice_0);
      write_at = write_start+n_fetched;
      if (n_added) fd_for_hashtable(&(ix->index_adds),add_key_fn,&write_at,1);
      if (n_replaced) fd_for_hashtable(&(ix->index_stores),edit_key_fn,&write_at,1);
      u8_big_free(fetched);
      return fd_init_choice(result,write_at-write_start,NULL,
                            FD_CHOICE_DOSORT|FD_CHOICE_REALLOC);}}
  else return fd_err(fd_NoMethod,"fd_index_keys",NULL,fd_index2lisp(ix));
}

static int copy_value_sizes(lispval key,lispval value,void *vptr)
{
  struct FD_HASHTABLE *sizes = (struct FD_HASHTABLE *)vptr;
  int value_size = ((VOIDP(value)) ? (0) : FD_CHOICE_SIZE(value));
  fd_hashtable_store(sizes,key,FD_INT(value_size));
  return 0;
}

FD_EXPORT lispval fd_index_keysizes(fd_index ix,lispval for_keys)
{
  if (ix->index_handler->fetchinfo) {
    lispval keys=for_keys;
    int n_fetched = 0, decref_keys=0;
    struct FD_CHOICE *filter=NULL;
    if (PRECHOICEP(for_keys)) {
      keys=fd_make_simple_choice(for_keys);
      decref_keys=1;}
    if (EMPTYP(keys))
      return EMPTY;
    else if ( (VOIDP(keys)) || (keys == FD_DEFAULT_VALUE) )
      filter=NULL;
    else if (!(CHOICEP(keys))) {
      lispval v = fd_index_get(ix,keys);
      unsigned int size = FD_CHOICE_SIZE(v);
      lispval result = fd_init_pair
        ( NULL, fd_incref(keys), FD_INT(size) );
      if (decref_keys) fd_decref(keys);
      fd_decref(v);
      return result;}
    else filter=(fd_choice)keys;
    init_cache_level(ix);
    struct FD_KEY_SIZE *fetched =
      ix->index_handler->fetchinfo(ix,filter,&n_fetched);
    if ((n_fetched==0) && (ix->index_adds.table_n_keys==0)) {
      if (decref_keys) fd_decref(keys);
      return EMPTY;}
    else if ((ix->index_adds.table_n_keys)==0) {
      fd_choice result = fd_alloc_choice(n_fetched);
      lispval *write = &(result->choice_0);
      if (decref_keys) fd_decref(keys);
      int i = 0; while (i<n_fetched) {
        lispval key = fetched[i].keysize_key, pair;
        unsigned int n_values = fetched[i].keysize_count;
        pair = fd_conspair(fd_incref(key),FD_INT(n_values));
        *write++=pair;
        i++;}
      u8_big_free(fetched);
      return fd_init_choice(result,n_fetched,NULL,
                            FD_CHOICE_DOSORT|FD_CHOICE_REALLOC);}
    else {
      struct FD_HASHTABLE added_sizes;
      fd_choice result; int i = 0, n_total; lispval *write;
      /* Get the sizes for added keys. */
      FD_INIT_STATIC_CONS(&added_sizes,fd_hashtable_type);
      fd_make_hashtable(&added_sizes,ix->index_adds.ht_n_buckets);
      fd_for_hashtable(&(ix->index_adds),copy_value_sizes,&added_sizes,1);
      n_total = n_fetched+ix->index_adds.table_n_keys;
      result = fd_alloc_choice(n_total); write = &(result->choice_0);
      while (i<n_fetched) {
        lispval key = fetched[i].keysize_key, pair;
        unsigned int n_values = fetched[i].keysize_count;
        lispval added = fd_hashtable_get(&added_sizes,key,FD_INT(0));
        n_values = n_values+fd_getint(added);
        pair = fd_conspair(fd_incref(key),FD_INT(n_values));
        fd_decref(added);
        *write++=pair;
        i++;}
      fd_recycle_hashtable(&added_sizes);
      if (decref_keys) fd_decref(keys);
      u8_big_free(fetched);
      return fd_init_choice(result,n_total,NULL,
                            FD_CHOICE_DOSORT|FD_CHOICE_REALLOC);}}
  else return fd_err(fd_NoMethod,"fd_index_keys",NULL,fd_index2lisp(ix));
}

FD_EXPORT lispval fd_index_sizes(fd_index ix)
{
  return fd_index_keysizes(ix,VOID);
}

FD_EXPORT lispval _fd_index_get(fd_index ix,lispval key)
{
  lispval cached; struct FD_PAIR tempkey;
  FDTC *fdtc = fd_threadcache;
  if (fdtc) {
    FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
    tempkey.car = fd_index2lisp(ix); tempkey.cdr = key;
    cached = fd_hashtable_get(&(fdtc->indexes),(lispval)&tempkey,VOID);
    if (!(VOIDP(cached))) return cached;}
  if (ix->index_cache_level == 0) cached = VOID;
  else if ((PAIRP(key)) && (!(VOIDP(ix->index_covers_slotids))) &&
           (!(atomic_choice_containsp(FD_CAR(key),ix->index_covers_slotids))))
    return EMPTY;
  else cached = fd_hashtable_get(&(ix->index_cache),key,VOID);
  if (VOIDP(cached))
    cached = fd_index_fetch(ix,key);
#if FD_USE_THREADCACHE
  if (fdtc) {
    fd_hashtable_store(&(fdtc->indexes),(lispval)&tempkey,cached);}
#endif
  return cached;
}
static lispval table_indexget(lispval ixarg,lispval key,lispval dflt)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix) {
    lispval v = fd_index_get(ix,key);
    if (EMPTYP(v)) return fd_incref(dflt);
    else return v;}
  else return FD_ERROR;
}


static void extend_slotids(fd_index ix,const lispval *keys,int n)
{
  lispval slotids = ix->index_covers_slotids;
  int i = 0; while (i<n) {
    lispval key = keys[i++], slotid;
    if (PAIRP(key)) slotid = FD_CAR(key); else continue;
    if ((OIDP(slotid)) || (SYMBOLP(slotid))) {
      if (atomic_choice_containsp(slotid,slotids)) continue;
      else {fd_decref(slotids); ix->index_covers_slotids = VOID;}}}
}

FD_EXPORT int _fd_index_add(fd_index ix,lispval key,lispval value)
{
  // FDTC *fdtc = fd_threadcache;
  fd_hashtable adds = &(ix->index_adds), cache = &(ix->index_cache);
  fd_hashtable drops = &(ix->index_drops);

  if (ix == NULL) {
    fd_seterr("Not an index","_fd_index_add",NULL,FD_VOID);
    return -1;}
  else if ( (EMPTYP(value)) || (EMPTYP(key)) )
    return 0;
  else if (U8_BITP(ix->index_flags,FD_STORAGE_READ_ONLY)) {
    fd_index front = fd_get_writable_index(ix);
    if (FD_INDEX_CONSEDP(front)) {
      int rv = fd_index_add(front,key,value);
      fd_decref(LISPVAL(front));
      return rv;}
    else if (front)
      return fd_index_add(front,key,value);
    else {
      fd_seterr(fd_ReadOnlyIndex,"_fd_index_add/front",ix->indexid,FD_VOID);
      return -1;}}
  else init_cache_level(ix);

  int decref_key = 0;
  if (FD_PRECHOICEP(key)) {
    key = fd_make_simple_choice(key);
    decref_key=1;}

  if (CHOICEP(key)) {
    const lispval *keyv = FD_CHOICE_DATA(key);
    unsigned int n = FD_CHOICE_SIZE(key);
    fd_hashtable_iterkeys(adds,fd_table_add,n,keyv,value);
    if (cache->table_n_keys)
      fd_hashtable_iterkeys(cache,fd_table_add_if_present,n,keyv,value);
    if (drops->table_n_keys)
      fd_hashtable_iterkeys(drops,fd_table_drop,n,keyv,value);
    if (!(VOIDP(ix->index_covers_slotids))) extend_slotids(ix,keyv,n);}
  else {
    fd_hashtable_add(adds,key,value);
    if (cache->table_n_keys)
      fd_hashtable_op(cache,fd_table_add_if_present,key,value);
    if (drops->table_n_keys)
      fd_hashtable_op(drops,fd_table_drop,key,value);
    if (!(VOIDP(ix->index_covers_slotids))) extend_slotids(ix,&key,1);}

  /* if ((fdtc)&&(FD_WRITETHROUGH_THREADCACHE)) fdtc_add(ix,keys,value); */

  if (ix->index_flags&FD_INDEX_IN_BACKGROUND) clear_bg_cache(key);

  if (decref_key) fd_decref(key);

  return 1;
}

static int table_indexadd(lispval ixarg,lispval key,lispval value)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix)
    return fd_index_add(ix,key,value);
  else return -1;
}

FD_EXPORT int fd_index_drop(fd_index ix_arg,lispval key,lispval value)
{
  if ( (EMPTYP(key)) || (EMPTYP(value)) ) return 0;

  fd_index ix = fd_get_writable_index(ix_arg);
  if (ix == NULL) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_drop",ix_arg->indexid,VOID);
    return -1;}
  else init_cache_level(ix);

  fd_hashtable cache = &(ix->index_cache);
  fd_hashtable drops = &(ix->index_drops);
  fd_hashtable adds = &(ix->index_adds);

  int decref_key = 0;
  if (FD_PRECHOICEP(key)) {
    key = fd_make_simple_choice(key);
    decref_key=1;}

  if (FD_CHOICEP(key)) {
    const lispval *keyv = FD_CHOICE_DATA(key);
    unsigned int n = FD_CHOICE_SIZE(key);
    fd_hashtable_iterkeys(drops,fd_table_add,n,keyv,value);

    if (cache->table_n_keys)
      fd_hashtable_iterkeys(cache,fd_table_drop,n,keyv,value);
    if (adds->table_n_keys)
      fd_hashtable_iterkeys(adds,fd_table_drop,n,keyv,value);}
  else {
    fd_hashtable_add(drops,key,value);
    if (cache->table_n_keys) fd_hashtable_drop(cache,key,value);
    if (adds->table_n_keys) fd_hashtable_drop(adds,key,value);}

  /* if ((fdtc)&&(FD_WRITETHROUGH_THREADCACHE)) fdtc_drop(ix,keys,value); */

  if (ix->index_flags&FD_INDEX_IN_BACKGROUND) clear_bg_cache(key);

  if (decref_key) fd_decref(key);

  if (FD_INDEX_CONSEDP(ix)) fd_decref(((lispval)ix));

  return 1;
}
static int table_indexdrop(lispval ixarg,lispval key,lispval value)
{
  fd_index ix = fd_indexptr(ixarg);
  if (!(ix)) return -1;
  else if (VOIDP(value))
    return fd_index_store(ix,key,EMPTY);
  else return fd_index_drop(ix,key,value);
}

FD_EXPORT int fd_index_store(fd_index ix_arg,lispval key,lispval value)
{
  if (EMPTYP(key)) return 0;

  fd_index ix = fd_get_writable_index(ix_arg);

  if (ix == NULL) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_store",ix_arg->indexid,VOID);
    return -1;}
  else init_cache_level(ix);

  fd_hashtable cache = &(ix->index_cache);
  fd_hashtable adds = &(ix->index_adds);
  fd_hashtable drops = &(ix->index_drops);
  fd_hashtable stores = &(ix->index_stores);

  int decref_key = 0;
  if (FD_PRECHOICEP(key)) {
    key = fd_make_simple_choice(key);
    decref_key=1;}

  if (FD_CHOICEP(key)) {
    const lispval *keyv = FD_CHOICE_DATA(key);
    unsigned int n = FD_CHOICE_SIZE(key);

    fd_hashtable_iterkeys(stores,fd_table_store,n,keyv,value);

    if (cache->table_n_keys)
      fd_hashtable_iterkeys(cache,fd_table_replace,n,keyv,value);
    if (adds->table_n_keys)
      fd_hashtable_iterkeys(adds,fd_table_drop,n,keyv,VOID);
    if (stores->table_n_keys)
      fd_hashtable_iterkeys(drops,fd_table_drop,n,keyv,VOID);}
  else {
    fd_hashtable_store(stores,key,value);
    if (cache->table_n_keys)
      fd_hashtable_op(cache,fd_table_replace,key,value);
    if (adds->table_n_keys) fd_hashtable_drop(adds,key,VOID);
    if (drops->table_n_keys) fd_hashtable_drop(drops,key,VOID);}

  /* if ((fdtc)&&(FD_WRITETHROUGH_THREADCACHE)) fdtc_store(ix,keys,value); */

  if (ix->index_flags&FD_INDEX_IN_BACKGROUND) clear_bg_cache(key);
  if (decref_key) fd_decref(key);

  if (FD_INDEX_CONSEDP(ix)) fd_decref(((lispval)ix));

  return 1;
}

static int table_indexstore(lispval ixarg,lispval key,lispval value)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix) return fd_index_store(ix,key,value);
  else return -1;
}

/* Index batch operations */

static int merge_kv_into_adds(struct FD_KEYVAL *kv,void *data)
{
  struct FD_HASHTABLE *adds = (fd_hashtable) data;
  fd_hashtable_op_nolock(adds,fd_table_add,kv->kv_key,kv->kv_val);
  return 0;
}

FD_EXPORT int fd_index_merge(fd_index ix,fd_hashtable table)
{
  fd_index into_index = fd_get_writable_index(ix);
  if (into_index == NULL) {
    fd_seterr(fd_ReadOnlyIndex,"fd_index_merge",ix->indexid,VOID);
    return -1;}
  else init_cache_level(into_index);

  fd_hashtable adds = &(into_index->index_adds);
  fd_write_lock_table(adds);
  fd_read_lock_table(table);
  fd_for_hashtable_kv(table,merge_kv_into_adds,(void *)adds,0);
  fd_unlock_table(table);
  fd_unlock_table(adds);

  if (FD_INDEX_CONSEDP(into_index)) fd_decref(LISPVAL(into_index));

  return 1;
}

FD_EXPORT int fd_batch_add(fd_index ix_arg,lispval table)
{
  fd_index ix = fd_get_writable_index(ix_arg);
  if (ix == NULL) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_batch_add",ix_arg->indexid,VOID);
    return -1;}
  else init_cache_level(ix);
  if (HASHTABLEP(table)) {
    int rv = fd_index_merge(ix,(fd_hashtable)table);
    if (FD_INDEX_CONSEDP(ix)) fd_decref(LISPVAL(ix));
    return rv;}
  else if (TABLEP(table)) {
    fd_hashtable adds = &(ix->index_adds);
    lispval allkeys = fd_getkeys(table);
    int i = 0, n_keys = FD_CHOICE_SIZE(allkeys), atomic = 1;
    lispval *keys = u8_alloc_n(n_keys,lispval);
    lispval *values = u8_alloc_n(n_keys,lispval);
    DO_CHOICES(key,allkeys) {
      lispval v = fd_get(table,key,VOID);
      if (!(VOIDP(v))) {
        if (CONSP(v)) atomic = 0;
        keys[i]=key; values[i]=v; i++;}}
    fd_hashtable_iter(adds,fd_table_add,i,keys,values);
    if (!(atomic)) for (int j = 0;j<i;j++) { fd_decref(values[j]); }
    u8_big_free(keys);
    u8_big_free(values);
    fd_decref(allkeys);
    if (FD_INDEX_CONSEDP(ix)) fd_decref(LISPVAL(ix));
    return i;}
  else {
    fd_seterr(fd_TypeError,"fd_batch_add","Not a table",table);
    if (FD_INDEX_CONSEDP(ix)) fd_decref(LISPVAL(ix));
    return -1;}
}

static lispval table_indexkeys(lispval ixarg)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix) return fd_index_keys(ix);
  else return fd_type_error(_("index"),"table_index_keys",ixarg);
}

static int remove_keyvals(struct FD_KEYVAL *keyvals,int n,lispval remove)
{
  int i=0; while (i<n) {
    lispval key = keyvals[i].kv_key;
    if ((CHOICEP(remove)) ? (fd_choice_containsp(key,remove)) :
        (FD_EQUALP(key,remove)) ) {
      lispval val = keyvals[i].kv_val;
      fd_decref(key); fd_decref(val);
      memmove(keyvals+i,keyvals+i+1,FD_KEYVAL_LEN*(n-(i+1)));
      n--;}
    i++;}
  return n;
}

typedef struct FD_CONST_KEYVAL *const_keyvals;

static struct FD_KEYVAL *
hashtable_keyvals(fd_hashtable ht,int *sizep,int keep_empty)
{
  /* We assume that the table is locked */
  struct FD_KEYVAL *results, *rscan;
  if (ht->table_n_keys == 0) {
    *sizep=0;
    return NULL;}
  if (ht->ht_n_buckets) {
    int size = 0;
    struct FD_HASH_BUCKET **scan=ht->ht_buckets;
    struct FD_HASH_BUCKET **lim=scan+ht->ht_n_buckets;
    rscan=results=u8_big_alloc_n(ht->table_n_keys,struct FD_KEYVAL);
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan;
        int bucket_len=e->bucket_len;
        struct FD_KEYVAL *kvscan=&(e->kv_val0);
        struct FD_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          lispval key = kvscan->kv_key, val = kvscan->kv_val;
          if ( (EXISTSP(val)) || (keep_empty) ) {
            if (FD_PRECHOICEP(val)) {
              val = kvscan->kv_val = fd_simplify_choice(val);}
            rscan->kv_key=key; fd_incref(key);
            rscan->kv_val=val; fd_incref(val);
            rscan++; size++;}
          kvscan++;}
        scan++;}
      else scan++;
    *sizep=size;}
  else {*sizep=0; results=NULL;}
  return results;
}

static void free_commits(struct FD_INDEX_COMMITS *commits)
{
  struct FD_KEYVAL *adds = (fd_keyvals) commits->commit_adds;
  struct FD_KEYVAL *drops = (fd_keyvals) commits->commit_drops;
  struct FD_KEYVAL *stores = (fd_keyvals) commits->commit_stores;
  if (adds) {
    fd_free_keyvals(adds,commits->commit_n_adds);
    commits->commit_adds=NULL;
    commits->commit_n_adds=0;
    u8_big_free(adds);}
  if (drops) {
    fd_free_keyvals(drops,commits->commit_n_drops);
    commits->commit_drops=NULL;
    commits->commit_n_drops=0;
    u8_big_free(drops);}
  if (stores) {
    fd_free_keyvals(stores,commits->commit_n_stores);
    commits->commit_stores=NULL;
    commits->commit_n_stores=0;
    u8_big_free(stores);}
}

#define record_elapsed(loc) \
  (loc=(u8_elapsed_time()-(mark)),(mark=u8_elapsed_time()))

static int index_dowrite(fd_index ix,struct FD_INDEX_COMMITS *commits)
{
  double started = u8_elapsed_time();
  int fd_storage_loglevel = index_loglevel(ix);
  int n_changes =
    commits->commit_n_adds +
    commits->commit_n_drops +
    commits->commit_n_stores;
  if (!(FD_VOIDP(commits->commit_metadata))) n_changes++;

  u8_logf(LOG_DETAIL,"IndexCommit/Write",
          "Writing %d edits to %s",n_changes,ix->indexid);

  int rv =ix->index_handler->commit(ix,fd_commit_write,commits);

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
  else u8_logf(LOG_DETAIL,"Finished/IndexCommit/Write",
               "(after %fsecs) writing %d edits to %s",
               elapsed_time(started),n_changes,ix->indexid);

  return rv;
}

static int index_rollback(fd_index ix,struct FD_INDEX_COMMITS *commits)
{
  double started = u8_elapsed_time();
  int fd_storage_loglevel = index_loglevel(ix);
  int n_changes =
    commits->commit_n_adds + commits->commit_n_drops + commits->commit_n_stores;
  if (!(FD_VOIDP(commits->commit_metadata))) n_changes++;

  u8_logf(LOG_DETAIL,"IndexRollback",
          "Rolling back %d edits to %s",n_changes,ix->indexid);

  int rollback = ix->index_handler->commit(ix,fd_commit_rollback,commits);
  if (rollback < 0) {
    u8_logf(LOG_CRIT,"Failed/IndexRollback",
            "(after %fs) Couldn't rollback failed save of %d edits to %s",
            elapsed_time(started),ix->indexid);
    u8_seterr("IndexRollbackFailed","index_dcommit/rollback",
              u8_strdup(ix->indexid));}
  else u8_logf(LOG_DETAIL,"Finished/IndexRollback",
               "Rolled back %d edits to %s in %f seconds",
               n_changes,ix->indexid,
               elapsed_time(started));

  return -1;
}

static int index_dosync(fd_index ix,struct FD_INDEX_COMMITS *commits)
{
  double started = u8_elapsed_time();
  int fd_storage_loglevel = index_loglevel(ix);
  int n_changes =
    commits->commit_n_adds + commits->commit_n_drops + commits->commit_n_stores;
  if (!(FD_VOIDP(commits->commit_metadata))) n_changes++;

  u8_logf(LOG_DETAIL,"IndexWrite/Sync",
          _("Syncing %d changes to %s"),n_changes,ix->indexid);

  int synced = ix->index_handler->commit(ix,fd_commit_sync,commits);

  if (synced < 0) {
    u8_seterr("IndexSync/Error","index_dosync",u8_strdup(ix->indexid));
    u8_logf(LOG_ERR,"IndexSync/Error",
            _("(after %fs) syncing %d changes to %s, rolling back"),
            elapsed_time(started),n_changes,ix->indexid);
    index_rollback(ix,commits);}
  else u8_logf(LOG_DETAIL,"IndexWrite/Synced",
               _("Synced %d changes to %s in %f secs"),
               n_changes,ix->indexid,elapsed_time(started));

  return synced;
}

static int index_doflush(fd_index ix,struct FD_INDEX_COMMITS *commits)
{
  double started = u8_elapsed_time();
  int fd_storage_loglevel = index_loglevel(ix);
  int n_changes =
    commits->commit_n_adds + commits->commit_n_drops + commits->commit_n_stores;
  if (!(FD_VOIDP(commits->commit_metadata))) n_changes++;

  u8_logf(LOG_DETAIL,"IndexCommit/Flush",
          "Flushing %d cached edits from %s",n_changes,ix->indexid);

  int rv= ix->index_handler->commit(ix,fd_commit_flush,commits);

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
  else u8_logf(LOG_DETAIL,"Finished/IndexCommit/Flush",
               "Flushed %d cached edits from %s in %f secons",
               n_changes,ix->indexid,
               elapsed_time(started));

  return rv;
}

static void log_timings(fd_index ix,struct FD_INDEX_COMMITS *commits)
{
  u8_logf(LOG_DEBUG,"Index/Commit/Timing",
          "for '%s'\n  total=%f, start=%f, setup=%f, save=%f, "
          "finalize=%f, apply=%f, cleanup=%f",
          ix->indexid,
          elapsed_time(commits->commit_times.base),
          commits->commit_times.start,
          commits->commit_times.setup,
          commits->commit_times.write,
          commits->commit_times.sync,
          commits->commit_times.flush,
          commits->commit_times.cleanup);
}

static int index_docommit(fd_index ix,struct FD_INDEX_COMMITS *use_commits)
{
  int fd_storage_loglevel = (ix->index_loglevel > 0) ? (ix->index_loglevel) :
    (*(fd_storage_loglevel_ptr));

  struct FD_INDEX_COMMITS commits = { 0 };
  int unlock_adds = 0, unlock_drops = 0, unlock_stores = 0;
  double start_time = u8_elapsed_time(), mark = start_time;
 if (use_commits)
    memcpy(&commits,use_commits,sizeof(struct FD_INDEX_COMMITS));
  else commits.commit_index = ix;

  commits.commit_times.base = mark;
  if ( (use_commits == NULL) ||
       (commits.commit_metadata == FD_NULL) )
    commits.commit_metadata = FD_VOID;

  int started = ix->index_handler->commit(ix,fd_commit_start,&commits);
  if (started < 0) {
    u8_seterr("CommitFailed","index_docommit/start",u8_strdup(ix->indexid));
    return started;}
  record_elapsed(commits.commit_times.start);
  u8_logf(LOG_DETAIL,"IndexCommit/Started","Committing %s for %s",
          ((use_commits)?("edits"):("changes")),
          ix->indexid);

  if (use_commits == NULL) {
    struct FD_HASHTABLE *adds_table = &(ix->index_adds);
    struct FD_HASHTABLE *drops_table = &(ix->index_drops);
    struct FD_HASHTABLE *stores_table = &(ix->index_stores);

    lispval metadata = (lispval) (&(ix->index_metadata));
    commits.commit_metadata =
      (fd_modifiedp(metadata)) ?
      (fd_deep_copy(metadata)) :
      (FD_VOID);

    int n_adds=0, n_drops=0, n_stores=0;
    /* Lock the changes for the index */
    if (adds_table->table_uselock) {
      fd_write_lock_table(adds_table);
      unlock_adds=1;}
    if (drops_table->table_uselock) {
      fd_write_lock_table(drops_table);
      unlock_drops=1;}
    if (stores_table->table_uselock) {
      fd_write_lock_table(stores_table);
      unlock_stores=1;}

    /* Copy them */
    struct FD_KEYVAL *adds = hashtable_keyvals(adds_table,&n_adds,0);
    struct FD_KEYVAL *drops = hashtable_keyvals(drops_table,&n_drops,0);
    struct FD_KEYVAL *stores = hashtable_keyvals(stores_table,&n_stores,1);

    /* We've got the data to save, so we unlock the tables. */
    if (unlock_adds) fd_unlock_table(adds_table);
    if (unlock_drops) fd_unlock_table(drops_table);
    if (unlock_stores) fd_unlock_table(stores_table);
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
            lispval newv = fd_difference(value,dropped);
            fd_decref(value);
            fd_decref(dropped);
            value = newv;}
          stores[i].kv_val = value;}
        i++;}}
    if (EXISTSP(merged)) {
      n_adds = remove_keyvals(adds,n_adds,merged);
      n_drops = remove_keyvals(drops,n_drops,merged);
      fd_decref(merged);}

    commits.commit_adds   = (fd_const_keyvals) adds;
    commits.commit_n_adds = n_adds;
    commits.commit_drops   = (fd_const_keyvals) drops;
    commits.commit_n_drops = n_drops;
    commits.commit_stores   = (fd_const_keyvals) stores;
    commits.commit_n_stores = n_stores;}
  record_elapsed(commits.commit_times.setup);

  int n_keys = commits.commit_n_adds + commits.commit_n_stores +
    commits.commit_n_drops;
  int n_changes = n_keys + (FD_SLOTMAPP(commits.commit_metadata));
  int w_metadata = FD_SLOTMAPP(commits.commit_metadata);

  u8_logf(LOG_DEBUG,fd_IndexCommit,
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
    u8_logf(LOG_DETAIL,"IndexWrite/Written",
            _("Wrote %d changes to %s after %f secs"),
            n_changes,ix->indexid,commits.commit_times.write);

  if (written < 0) {
    u8_seterr("CommitFailed","index_docommit/write",u8_strdup(ix->indexid));
    u8_logf(LOG_ERR,"IndexWrite/Error",
            _("Error writing %d changes to %s after %f secs, rolling back"),
            n_changes,ix->indexid,u8_elapsed_time()-start_time);
    synced = -1;
    index_rollback(ix,&commits);
    commits.commit_phase = fd_commit_flush;}
  else if (commits.commit_phase == fd_commit_sync) {
    synced = index_dosync(ix,&commits);
    commits.commit_phase = fd_commit_flush;}
  else {
    synced = 0;
    commits.commit_phase = fd_commit_flush;}
  record_elapsed(commits.commit_times.sync);

  /* If this returns < 0, it will have generated a warning and it doesn't require us to abort
     because the state has been saved, it's just still in memory. */
  index_doflush(ix,&commits);

  if (synced<0) {
    /* If there was an error, just leave the edits in the index hashtables */
    if (use_commits == NULL) free_commits(&commits);}
  else if (use_commits == NULL) {
    /* If you got the changes out of the index hashtables, clear them: */
    struct FD_HASHTABLE *adds_table = &(ix->index_adds);
    struct FD_HASHTABLE *drops_table = &(ix->index_drops);
    struct FD_HASHTABLE *stores_table = &(ix->index_stores);
    /* Lock the adds and edits again */
    if (adds_table->table_uselock) {
      fd_write_lock_table(adds_table);
      unlock_adds=1;}
    if (drops_table->table_uselock) {
      fd_write_lock_table(drops_table);
      unlock_drops=1;}
    if (stores_table->table_uselock) {
      fd_write_lock_table(stores_table);
      unlock_stores=1;}

    /* Remove everything you just saved */

    fd_hashtable_iter_kv(adds_table,fd_table_drop,
                         (const_keyvals)commits.commit_adds,
                         commits.commit_n_adds,
                         0);
    fd_hashtable_iter_kv(drops_table,fd_table_drop,
                         (const_keyvals)commits.commit_drops,
                         commits.commit_n_drops,
                         0);
    fd_hashtable_iter_kv(stores_table,fd_table_drop,
                         (const_keyvals)commits.commit_stores,
                         commits.commit_n_stores,
                         0);

    if (unlock_adds) fd_unlock_table(&(ix->index_adds));
    if (unlock_drops) fd_unlock_table(&(ix->index_drops));
    if (unlock_stores) fd_unlock_table(&(ix->index_stores));

    free_commits(&commits);}
  else NO_ELSE;
  record_elapsed(commits.commit_times.flush);
  commits.commit_phase = fd_commit_cleanup;

  int cleanup_rv = ix->index_handler->commit(ix,fd_commit_cleanup,&commits);
  if (cleanup_rv < 0) {
    if (errno) { u8_graberrno("index_docommit",u8_strdup(ix->indexid)); }
    u8_log(LOG_WARN,"CleanupFailed",
           "There was en error cleaning up after commiting %s",ix->indexid);
    fd_clear_errors(1);}
  record_elapsed(commits.commit_times.cleanup);

  commits.commit_phase = fd_commit_done;

  if (synced < 0)
    u8_logf(LOG_CRIT,"Commit Failed",
            _("for %d %supdated keys from %s after %f secs"),
            n_changes,(w_metadata ? ("(and metadata) ") : ("") ),
            ix->indexid,u8_elapsed_time()-start_time);
  else u8_logf(LOG_INFO,fd_IndexCommit,
               _("Committed %d %s%s keys to %s in %f secs"),
               n_changes,(w_metadata ? ("(and metadata) ") : ("") ),
               (use_commits) ? ("edited") : ("changed"),
               ix->indexid,u8_elapsed_time()-start_time);

  if (fd_storage_loglevel >= LOG_INFO) log_timings(ix,&commits);

  if (synced < 0)
    return synced;
  else return n_changes;
}

FD_EXPORT int fd_commit_index(fd_index ix)
{
  if (ix == NULL)
    return -1;
  else if (! ((ix->index_adds.table_n_keys) ||
              (ix->index_drops.table_n_keys) ||
              (ix->index_stores.table_n_keys) ||
              (fd_modifiedp((lispval)&(ix->index_metadata)))) )
    return 0;
  else if (ix->index_handler->commit == NULL) {
    u8_seterr("NoCommitMethod","fd_commit_index",u8_strdup(ix->indexid));
    return -1;}
  else NO_ELSE;

  u8_lock_mutex(&(ix->index_commit_lock));
  int rv = index_docommit(ix,NULL);
  u8_unlock_mutex(&(ix->index_commit_lock));
  return rv;
}

FD_EXPORT int fd_index_save(fd_index ix,
                            lispval toadd,
                            lispval todrop,
                            lispval tostore,
                            lispval metadata)
{
  int fd_storage_loglevel = index_loglevel(ix);

  if (ix == NULL)
    return -1;
  else if (ix->index_handler->commit == NULL) {
    u8_seterr("NoCommitMethod","fd_index_save",u8_strdup(ix->indexid));
    return -1;}

  int free_adds = 0, free_drops =0, free_stores = 0, free_adds_vec = 0;
  int n_adds = 0, n_drops =0, n_stores = 0;
  struct FD_KEYVAL *adds=NULL, *drops=NULL, *stores=NULL;

  struct FD_INDEX_COMMITS commits = {};
  commits.commit_index = ix;

  if (FD_VOIDP(toadd)) {}
  else if (FD_SLOTMAPP(toadd))
    adds = FD_SLOTMAP_KEYVALS(toadd);
  else if (FD_HASHTABLEP(toadd)) {
    adds = hashtable_keyvals((fd_hashtable)toadd,&n_adds,0);
    free_adds = 1;}
  else if ( (FD_PAIRP(toadd)) &&
            (FD_VECTORP(FD_CAR(toadd))) &&
            (FD_VECTORP(FD_CDR(toadd)))) {
    lispval keys = FD_CAR(toadd), vals = FD_CDR(toadd);
    size_t keys_len = FD_VECTOR_LENGTH(keys);
    size_t vals_len = FD_VECTOR_LENGTH(vals);
    if (keys_len != vals_len)
      return fd_err("KeyVec/ValVec mismatch","fd_index_save",ix->indexid,toadd);
    adds = u8_big_alloc_n(keys_len,struct FD_KEYVAL);
    long i=0, n=0; while (i<keys_len) {
      lispval key = FD_VECTOR_REF(keys,i);
      lispval val = FD_VECTOR_REF(vals,i);
      if ( (FD_EMPTYP(key)) || (FD_EMPTYP(val)) ||
           (FD_VOIDP(key)) || (FD_VOIDP(val)) ||
           (FD_NULLP(key)) || (FD_NULLP(val)) )
        i++;
      else {
        adds[n].kv_key = key;
        adds[n].kv_val = val;
        i++; n++;}}
      free_adds = 0;
      free_adds_vec = 1;
      n_adds = n;}
  else if (FD_TABLEP(toadd)) {
    lispval keys = fd_getkeys(toadd);
    int i=0, n = FD_CHOICE_SIZE(keys);
    adds = u8_big_alloc_n(n,struct FD_KEYVAL);
    FD_DO_CHOICES(key,keys) {
      lispval val = fd_get(toadd,key,FD_VOID);
      if (!((FD_VOIDP(val)) || (FD_EMPTYP(val)))) {
        adds[i].kv_key = key; fd_incref(key);
        adds[i].kv_val = val;
        i++;}}
    fd_decref(keys);
    free_adds = 1;
    toadd=i;}

  if (FD_VOIDP(todrop)) {}
  else if (FD_SLOTMAPP(todrop))
    drops = FD_SLOTMAP_KEYVALS(todrop);
  else if (FD_HASHTABLEP(todrop)) {
    drops = hashtable_keyvals((fd_hashtable)todrop,&n_drops,0);
    free_drops = 1;}
  else if (FD_TABLEP(todrop)) {
    lispval keys = fd_getkeys(todrop);
    int i=0, n = FD_CHOICE_SIZE(keys);
    drops = u8_big_alloc_n(n,struct FD_KEYVAL);
    FD_DO_CHOICES(key,keys) {
      lispval val = fd_get(todrop,key,FD_VOID);
      if (!((FD_VOIDP(val)) || (FD_EMPTYP(val)))) {
        drops[i].kv_key = key; fd_incref(key);
        drops[i].kv_val = val;
        i++;}}
    fd_decref(keys);
    free_drops = 1;
    todrop=i;}

  if (FD_VOIDP(tostore)) {}
  else if (FD_SLOTMAPP(tostore))
    stores = FD_SLOTMAP_KEYVALS(tostore);
  else if (FD_HASHTABLEP(tostore)) {
    stores = hashtable_keyvals((fd_hashtable)tostore,&n_stores,1);
    free_stores = 1;}
  else if (FD_TABLEP(tostore)) {
    lispval keys = fd_getkeys(tostore);
    int i=0, n = FD_CHOICE_SIZE(keys);
    stores = u8_big_alloc_n(n,struct FD_KEYVAL);
    FD_DO_CHOICES(key,keys) {
      lispval val = fd_get(tostore,key,FD_VOID);
      if (!((FD_VOIDP(val)) || (FD_EMPTYP(val)))) {
        stores[i].kv_key = key; fd_incref(key);
        stores[i].kv_val = val;
        i++;}}
    fd_decref(keys);
    free_stores = 1;
    tostore=i;}

  int n_changes = n_adds + n_drops + n_stores;
  if (!(FD_VOIDP(metadata))) n_changes++;

  if (n_changes) {
    init_cache_level(ix);
    u8_logf(LOG_INFO,fd_IndexCommit,
            _("Saving %d changes (+%d-%d=%d%s) to %s"),
            n_changes,n_adds,n_drops,n_stores,
            (FD_VOIDP(metadata)) ? ("") : ("/md"),
            ix->indexid);}

  if (FD_SLOTMAPP(metadata))
    commits.commit_metadata = metadata;
  else commits.commit_metadata = FD_VOID;

  commits.commit_adds     = (fd_const_keyvals) adds;
  commits.commit_n_adds   = n_adds;
  commits.commit_drops    = (fd_const_keyvals) drops;
  commits.commit_n_drops  = n_drops;
  commits.commit_stores   = (fd_const_keyvals) stores;
  commits.commit_n_stores = n_stores;

  u8_lock_mutex(&(ix->index_commit_lock));
  int saved = index_docommit(ix,&commits);
  u8_unlock_mutex(&(ix->index_commit_lock));

  if (free_adds) {
    fd_free_keyvals(adds,n_adds);
    u8_big_free(adds);}
  else if (free_adds_vec)
    u8_big_free(adds);
  else NO_ELSE;

  if (free_drops) {
    fd_free_keyvals(drops,n_drops);
    u8_big_free(drops);}

  if (free_stores) {
    fd_free_keyvals(stores,n_stores);
    u8_big_free(stores);}

  fd_decref(commits.commit_metadata);

  return saved;
}

FD_EXPORT void fd_index_swapout(fd_index ix,lispval keys)
{
  struct FD_HASHTABLE *cache = &(ix->index_cache);
  if (((ix->index_flags)&FD_STORAGE_NOSWAP) ||
      (cache->table_n_keys==0))
    return;
  else if (VOIDP(keys)) {
    if ((ix->index_flags)&(FD_STORAGE_KEEP_CACHESIZE))
      fd_reset_hashtable(&(ix->index_cache),-1,1);
    else fd_reset_hashtable(&(ix->index_cache),0,1);}
  else if (CHOICEP(keys)) {
    struct FD_CHOICE *ch = (fd_choice)keys;
    fd_hashtable_iterkeys(cache,fd_table_replace,ch->choice_size,
                          FD_XCHOICE_DATA(ch),VOID);
    fd_devoid_hashtable(cache,0);
    return;}
  else if (PRECHOICEP(keys)) {
    lispval simplified = fd_make_simple_choice(keys);
    fd_index_swapout(ix,simplified);
    fd_decref(simplified);}
  else {
    fd_hashtable_store(&(ix->index_cache),keys,VOID);}
}

FD_EXPORT void fd_index_close(fd_index ix)
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
  if (FD_VOIDP(v)) return;
  fd_store(md,slot,v);
  fd_decref(v);
}
static void mdstring(lispval md,lispval slot,u8_string s)
{
  if (s==NULL) return;
  lispval v=fdstring(s);
  fd_store(md,slot,v);
  fd_decref(v);
}

static lispval metadata_readonly_props = FD_VOID;

FD_EXPORT lispval fd_index_base_metadata(fd_index ix)
{
  int flags=ix->index_flags;
  lispval metadata=fd_deep_copy((lispval)&(ix->index_metadata));
  mdstore(metadata,cachelevel_slot,FD_INT(ix->index_cache_level));
  mdstring(metadata,indexid_slot,ix->indexid);
  mdstring(metadata,source_slot,ix->index_source);
  mdstring(metadata,realpath_slot,ix->canonical_source);
  mdstore(metadata,cached_slot,FD_INT(ix->index_cache.table_n_keys));
  mdstore(metadata,adds_slot,FD_INT(ix->index_adds.table_n_keys));
  mdstore(metadata,drops_slot,FD_INT(ix->index_drops.table_n_keys));
  mdstore(metadata,replaced_slot,FD_INT(ix->index_stores.table_n_keys));
  if ((ix->index_handler) && (ix->index_handler->name))
    mdstring(metadata,FDSYM_TYPE,(ix->index_handler->name));

  if (U8_BITP(flags,FD_STORAGE_READ_ONLY))
    fd_add(metadata,flags_slot,read_only_flag);
  if (U8_BITP(flags,FD_STORAGE_UNREGISTERED))
    fd_add(metadata,flags_slot,unregistered_flag);
  else {
    fd_add(metadata,flags_slot,registered_flag);
    fd_store(metadata,registered_slot,FD_INT(ix->index_serialno));}
  if (U8_BITP(flags,FD_STORAGE_NOSWAP))
    fd_add(metadata,flags_slot,noswap_flag);
  if (U8_BITP(flags,FD_STORAGE_NOERR))
    fd_add(metadata,flags_slot,noerr_flag);
  if (U8_BITP(flags,FD_STORAGE_PHASED))
    fd_add(metadata,flags_slot,phased_flag);
  if (U8_BITP(flags,FD_INDEX_IN_BACKGROUND))
    fd_add(metadata,flags_slot,background_flag);
  if (U8_BITP(flags,FD_INDEX_ADD_CAPABILITY))
    fd_add(metadata,flags_slot,canadd_flag);
  if (U8_BITP(flags,FD_INDEX_DROP_CAPABILITY))
    fd_add(metadata,flags_slot,candrop_flag);
  if (U8_BITP(flags,FD_INDEX_SET_CAPABILITY))
    fd_add(metadata,flags_slot,canset_flag);

  if (FD_TABLEP(ix->index_opts))
    fd_add(metadata,opts_slot,ix->index_opts);

  fd_add(metadata,metadata_readonly_props,cachelevel_slot);
  fd_add(metadata,metadata_readonly_props,indexid_slot);
  fd_add(metadata,metadata_readonly_props,realpath_slot);
  fd_add(metadata,metadata_readonly_props,source_slot);
  fd_add(metadata,metadata_readonly_props,FDSYM_TYPE);
  fd_add(metadata,metadata_readonly_props,edits_slot);
  fd_add(metadata,metadata_readonly_props,adds_slot);
  fd_add(metadata,metadata_readonly_props,drops_slot);
  fd_add(metadata,metadata_readonly_props,replaced_slot);
  fd_add(metadata,metadata_readonly_props,cached_slot);
  fd_add(metadata,metadata_readonly_props,flags_slot);

  fd_add(metadata,metadata_readonly_props,FDSYM_PROPS);
  fd_add(metadata,metadata_readonly_props,opts_slot);

  return metadata;
}

/* Common init function */

FD_EXPORT void fd_init_index(fd_index ix,
                             struct FD_INDEX_HANDLER *h,
                             u8_string id,u8_string src,u8_string csrc,
                             fd_storage_flags flags,
                             lispval metadata,
                             lispval opts)
{
  if (flags<0)
    flags = fd_get_dbflags(opts,FD_STORAGE_ISINDEX);
  else {U8_SETBITS(flags,FD_STORAGE_ISINDEX);}
  if (U8_BITP(flags,FD_STORAGE_UNREGISTERED)) {
    FD_INIT_CONS(ix,fd_consed_index_type);
    add_consed_index(ix);}
  else {FD_INIT_STATIC_CONS(ix,fd_consed_index_type);}
  if (U8_BITP(flags,FD_STORAGE_READ_ONLY)) {
    U8_SETBITS(flags,FD_STORAGE_READ_ONLY); };
  ix->index_serialno = -1; ix->index_cache_level = -1; ix->index_flags = flags;
  FD_INIT_STATIC_CONS(&(ix->index_cache),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(ix->index_adds),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(ix->index_drops),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(ix->index_stores),fd_hashtable_type);
  fd_make_hashtable(&(ix->index_cache),fd_index_cache_init);
  fd_make_hashtable(&(ix->index_adds),0);
  fd_make_hashtable(&(ix->index_drops),0);
  fd_make_hashtable(&(ix->index_stores),0);

  FD_INIT_STATIC_CONS(&(ix->index_props),fd_slotmap_type);
  fd_init_slotmap(&(ix->index_props),17,NULL);

  ix->index_handler = h;

  lispval keyslot = fd_getopt(opts,FDSYM_KEYSLOT,VOID);
  FD_INIT_STATIC_CONS(&(ix->index_metadata),fd_slotmap_type);
  if (FD_SLOTMAPP(metadata)) {
    fd_copy_slotmap((fd_slotmap)metadata,&(ix->index_metadata));
    if (FD_VOIDP(keyslot)) keyslot=fd_get(metadata,FDSYM_KEYSLOT,VOID);}
  else {
    fd_init_slotmap(&(ix->index_metadata),17,NULL);
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

  if ( (FD_VOIDP(opts)) || (FD_FALSEP(opts)) )
    ix->index_opts = FD_FALSE;
  else ix->index_opts = fd_incref(opts);

  lispval ll = fd_getopt(opts,FDSYM_LOGLEVEL,FD_VOID);
  if (FD_VOIDP(ll))
    ix->index_loglevel = -1;
  else if ( (FD_FIXNUMP(ll)) && ( (FD_FIX2INT(ll)) >= 0 ) &&
       ( (FD_FIX2INT(ll)) < U8_MAX_LOGLEVEL ) )
    ix->index_loglevel = FD_FIX2INT(ll);
  else {
    u8_log(LOG_WARN,"BadLogLevel",
           "Invalid loglevel %q for pool %s",ll,id);
    ix->index_loglevel = -1;}
  fd_decref(ll);

  u8_init_mutex(&(ix->index_commit_lock));
}

FD_EXPORT int fd_index_set_metadata(fd_index ix,lispval metadata)
{
  if (FD_SLOTMAPP(metadata)) {
    fd_copy_slotmap((fd_slotmap)metadata,&(ix->index_metadata));
    ix->index_keyslot = fd_get(metadata,FDSYM_KEYSLOT,VOID);}
  else {
    FD_INIT_STATIC_CONS(&(ix->index_metadata),fd_slotmap_type);
    fd_init_slotmap(&(ix->index_metadata),17,NULL);
    ix->index_keyslot = VOID;}
  ix->index_metadata.table_modified = 0;
  return 0;
}

FD_EXPORT void fd_reset_index_tables
(fd_index ix,ssize_t csize,ssize_t esize,ssize_t asize)
{
  int readonly = U8_BITP(ix->index_flags,FD_STORAGE_READ_ONLY);
  fd_hashtable cache = &(ix->index_cache);
  fd_hashtable adds = &(ix->index_adds);
  fd_hashtable drops = &(ix->index_drops);
  fd_hashtable replace = &(ix->index_stores);
  fd_reset_hashtable(cache,((csize==0)?(fd_index_cache_init):(csize)),1);
  if (adds->table_n_keys==0) {
    ssize_t level = (readonly)?(0):(asize==0)?(fd_index_adds_init):(asize);
    fd_reset_hashtable(adds,level,1);}
  if (drops->table_n_keys==0) {
    ssize_t level = (readonly)?(0):(esize==0)?(fd_index_drops_init):(esize);
    fd_reset_hashtable(drops,level,1);}
  if (replace->table_n_keys==0) {
    ssize_t level = (readonly)?(0):(esize==0)?(fd_index_replace_init):(esize);
    fd_reset_hashtable(replace,level,1);}

}

static void display_index(u8_output out,fd_index ix,lispval lix)
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
  fd_index ix = fd_indexptr(x);
  if (ix == NULL) return 0;
  display_index(out,ix,x);
  return 1;
}

static int unparse_consed_index(u8_output out,lispval x)
{
  fd_index ix = (fd_index)(x);
  if (ix == NULL) return 0;
  if (ix == NULL) return 0;
  display_index(out,ix,x);
  return 1;
}

static lispval index_parsefn(int n,lispval *args,fd_compound_typeinfo e)
{
  fd_index ix = NULL;
  if (n<2) return VOID;
  else if (STRINGP(args[2]))
    ix = fd_get_index(FD_STRING_DATA(args[2]),0,VOID);
  if (ix)
    return fd_index_ref(ix);
  else return fd_err(fd_CantParseRecord,"index_parsefn",NULL,VOID);
}

/* Operations over all indexes */

static lispval index2lisp(fd_index ix)
{
  if (ix->index_serialno>=0)
    return LISPVAL_IMMEDIATE(fd_index_type,ix->index_serialno);
  else {
    lispval v = (lispval) ix;
    fd_incref(v);
    return v;}
}

FD_EXPORT lispval fd_get_all_indexes()
{
  lispval results = EMPTY;
  int i = 0; while (i < fd_n_primary_indexes) {
    lispval lindex = index2lisp(fd_primary_indexes[i]);
    CHOICE_ADD(results,lindex);
    i++;}

  if (i>=fd_n_primary_indexes) {
    u8_read_lock(&indexes_lock);
    i = 0; while (i < fd_n_secondary_indexes) {
      lispval lindex = index2lisp(secondary_indexes[i]);
      CHOICE_ADD(results,lindex);
      i++;}
    u8_rw_unlock(&indexes_lock);}

  if (n_consed_indexes) {
    u8_lock_mutex(&consed_indexes_lock);
    int j = 0; while (j<n_consed_indexes) {
      fd_index ix = consed_indexes[j++];
      if (ix) {
        lispval lindex = index2lisp(ix);
        CHOICE_ADD(results,lindex);}
      else _fd_debug(results);}
    u8_unlock_mutex(&consed_indexes_lock);}

  return fd_simplify_choice(results);
}

FD_EXPORT int fd_for_indexes(int (*fcn)(fd_index ix,void *),void *data)
{
  int total = 0;
  lispval all_indexes = fd_get_all_indexes();
  DO_CHOICES(lindex,all_indexes) {
    fd_index ix = fd_lisp2index(lindex);
    int retval = fcn(ix,data);
    total++;
    if (retval<0) {
      FD_STOP_DO_CHOICES;
      fd_decref(all_indexes);
      return retval;}
    else if (retval) {
      FD_STOP_DO_CHOICES;
      break;}}
  fd_decref(all_indexes);
  return total;
}

static int swapout_index_handler(fd_index ix,void *data)
{
  fd_index_swapout(ix,VOID);
  return 0;
}


FD_EXPORT void fd_swapout_indexes()
{
  fd_for_indexes(swapout_index_handler,NULL);
}

static int close_index_handler(fd_index ix,void *data)
{
  fd_index_close(ix);
  return 0;
}

FD_EXPORT void fd_close_indexes()
{
  fd_for_indexes(close_index_handler,NULL);
}

static int commit_each_index(fd_index ix,void *data)
{
  /* Phased indexes shouldn't be committed by themselves */
  if ( (ix->index_flags) & (FD_STORAGE_PHASED) ) return 0;
  int *count = (int *) data;
  if (ix->index_handler->commit==NULL) return 0;
  int retval = fd_commit_index(ix);
  if (retval<0)
    return retval;
  else *count += retval;
  return 0;
}

FD_EXPORT int fd_commit_indexes()
{
  int count=0;
  int rv=fd_for_indexes(commit_each_index,&count);
  if (rv<0) return rv;
  else return count;
}

FD_EXPORT int fd_commit_indexes_noerr()
{
  int count=0;
  int rv=fd_for_indexes(commit_each_index,&count);
  if (rv<0) return rv;
  else return count;
}

static int accumulate_cachecount(fd_index ix,void *ptr)
{
  int *count = (int *)ptr;
  *count = *count+ix->index_cache.table_n_keys;
  return 0;
}

FD_EXPORT
long fd_index_cache_load()
{
  int result = 0, retval;
  retval = fd_for_indexes(accumulate_cachecount,(void *)&result);
  if (retval<0) return retval;
  else return result;
}

static int accumulate_cached(fd_index ix,void *ptr)
{
  lispval *vals = (lispval *)ptr;
  lispval keys = fd_hashtable_keys(&(ix->index_cache));
  CHOICE_ADD(*vals,keys);
  return 0;
}

FD_EXPORT
lispval fd_cached_keys(fd_index ix)
{
  if (ix == NULL) {
    int retval; lispval result = EMPTY;
    retval = fd_for_indexes(accumulate_cached,(void *)&result);
    if (retval<0) {
      fd_decref(result);
      return FD_ERROR;}
    else return result;}
  else return fd_hashtable_keys(&(ix->index_cache));
}

/* IPEVAL delay execution */

FD_EXPORT int fd_execute_index_delays(fd_index ix,void *data)
{
  lispval *delays = get_index_delays();
  lispval todo = delays[ix->index_serialno];
  if (EMPTYP(todo)) return 0;
  else {
    int retval = -1;
    /* u8_lock_mutex(&(fd_ipeval_lock)); */
    todo = delays[ix->index_serialno];
    delays[ix->index_serialno]=EMPTY;
    /* u8_unlock_mutex(&(fd_ipeval_lock)); */
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval>1)
      u8_logf(LOG_INFO,ipeval_ixfetch,"Fetching %d keys from %s: %q",
              FD_CHOICE_SIZE(todo),ix->indexid,todo);
    else
#endif
      retval = fd_index_prefetch(ix,todo);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval)
      u8_logf(LOG_INFO,ipeval_ixfetch,"Fetched %d keys from %s",
              FD_CHOICE_SIZE(todo),ix->indexid);
#endif
    if (retval<0) return retval;
    else return 0;}
}

FD_EXPORT void fd_recycle_index(struct FD_INDEX *ix)
{
  struct FD_INDEX_HANDLER *handler = ix->index_handler;
  drop_consed_index(ix);
  if (handler->recycle) handler->recycle(ix);
  fd_recycle_hashtable(&(ix->index_cache));
  fd_recycle_hashtable(&(ix->index_adds));
  fd_recycle_hashtable(&(ix->index_drops));
  fd_recycle_hashtable(&(ix->index_stores));
  u8_free(ix->indexid);
  if (ix->index_source) u8_free(ix->index_source);
  if (ix->index_typeid) u8_free(ix->index_typeid);

  fd_free_slotmap(&(ix->index_metadata));
  fd_free_slotmap(&(ix->index_props));

  fd_decref(ix->index_covers_slotids);
  fd_decref(ix->index_opts);
}

static void recycle_consed_index(struct FD_RAW_CONS *c)
{
  fd_index ix = (fd_index) c;
  if (ix->index_serialno >= 0) {
    u8_log(LOG_WARN,"RecylingEternalIndex",
           "There was an attempt to free the index %s, supposedly eternal",
           ix->indexid);
    return;}
  else {
    fd_recycle_index(ix);
    u8_free(c);}
}

static lispval copy_consed_index(lispval x,int deep)
{
  return fd_incref(x);
}

FD_EXPORT lispval fd_default_indexctl(fd_index ix,lispval op,int n,lispval *args)
{
  if ((n>0)&&(args == NULL))
    return fd_err("BadIndexOpCall","fd_default_indexctl",ix->indexid,VOID);
  else if (n<0)
    return fd_err("BadIndexOpCall","fd_default_indexctl",ix->indexid,VOID);
  else if (op == FDSYM_OPTS)  {
    lispval opts = ix->index_opts;
    if (n > 1)
      return fd_err(fd_TooManyArgs,"fd_default_indexctl",ix->indexid,VOID);
    else if ( (opts == FD_NULL) || (VOIDP(opts) ) )
      return FD_FALSE;
    else if ( n == 1 )
      return fd_getopt(opts,args[0],FD_FALSE);
    else return fd_incref(opts);}
  else if (op == fd_metadata_op) {
    lispval metadata = ((lispval)&(ix->index_metadata));
    lispval slotid = (n>0) ? (args[0]) : (FD_VOID);
    /* TODO: check that slotid isn't any of the slots returned by default */
    if (n == 0)
      return fd_index_base_metadata(ix);
    else if (n == 1) {
      lispval extended=fd_index_ctl(ix,fd_metadata_op,0,NULL);
      lispval v = fd_get(extended,args[0],FD_EMPTY);
      fd_decref(extended);
      return v;}
    else if (n == 2) {
      lispval extended=fd_index_ctl(ix,fd_metadata_op,0,NULL);
      if (fd_test(extended,FDSYM_READONLY,slotid)) {
        fd_decref(extended);
        return fd_err("ReadOnlyMetadataProperty","fd_default_indexctl",
                      ix->indexid,slotid);}
      else fd_decref(extended);
      int rv=fd_store(metadata,slotid,args[1]);
      if (rv<0)
        return FD_ERROR_VALUE;
      else return fd_incref(args[1]);}
    else return fd_err(fd_TooManyArgs,"fd_index_ctl/metadata",
                       FD_SYMBOL_NAME(op),fd_index2lisp(ix));}
  else if (op == FDSYM_PROPS) {
    lispval props = (lispval) &(ix->index_props);
    lispval slotid = (n>0) ? (args[0]) : (FD_VOID);
    if (n == 0)
      return fd_copier(props,0);
    else if (n == 1)
      return fd_get(props,slotid,FD_EMPTY);
    else if (n == 2) {
      int rv=fd_store(props,slotid,args[1]);
      if (rv<0)
        return FD_ERROR_VALUE;
      else return fd_incref(args[1]);}
    else return fd_err(fd_TooManyArgs,"fd_index_ctl/props",
                       FD_SYMBOL_NAME(op),fd_index2lisp(ix));}
  else if (op == FDSYM_KEYSLOT) {
    if (n == 0) {
      if ( (FD_NULLP(ix->index_keyslot)) || (FD_VOIDP(ix->index_keyslot)) )
        return FD_FALSE;
      else return ix->index_keyslot;}
    else if (n == 1) {
      lispval defslot = args[0];
      if (FD_FALSEP(defslot)) {
        lispval old = ix->index_keyslot;
        ix->index_keyslot = FD_VOID;
        if (FD_VOIDP(old))
          return FD_FALSE;
        else return old;}
      if (!( (FD_NULLP(ix->index_keyslot)) ||
             (FD_VOIDP(ix->index_keyslot)) )) {
        if (defslot == ix->index_keyslot) {
          u8_logf(LOG_NOTICE,"KeySlotOK",
                  "The keyslot of %s is already %q",ix->indexid,defslot);
          return defslot;}
        else return fd_err("KeySlotAlreadyDefined",
                           "fd_default_indexctl/keyslot",
                           ix->indexid,ix->index_keyslot);}
      else if ((FD_OIDP(defslot)) || (FD_SYMBOLP(defslot))) {
        if (ix->index_handler->commit) {
          /* If the index persists itself (has a commit method),
             add the new keyslot to the metadata. */
          lispval metadata = ((lispval)&(ix->index_metadata));
          fd_store(metadata,FDSYM_KEYSLOT,defslot);}
        ix->index_keyslot=defslot;
        return defslot;}
      else return fd_type_error("slotid","fd_default_indexctl/keyslot",
                                defslot);}
    else return fd_err(fd_TooManyArgs,"fd_index_ctl/keyslot",
                       FD_SYMBOL_NAME(op),fd_index2lisp(ix));}
  else if (op == FDSYM_LOGLEVEL) {
    if (n == 0) {
      if (ix->index_loglevel < 0)
        return FD_FALSE;
      else return FD_INT(ix->index_loglevel);}
    else if (n == 1) {
      if (FIXNUMP(args[0])) {
        long long level = FD_FIX2INT(args[0]);
        if ((level<0) || (level > 128))
          return fd_err(fd_RangeError,
                        "fd_default_indexctl",ix->indexid,args[0]);
        else {
          int old_loglevel = ix->index_loglevel;
          ix->index_loglevel = level;
          if (old_loglevel<0)
            return FD_FALSE;
          else return FD_INT(old_loglevel);}}
      else return fd_type_error("loglevel","fd_default_indexctl",args[0]);}
    else return fd_err(fd_TooManyArgs,"fd_default_indexctl",ix->indexid,VOID);}
  else if (op == fd_keys_op)
    return fd_index_keys(ix);
  else if (op == fd_partitions_op)
    return FD_EMPTY;
  else if (op == FDSYM_CACHELEVEL)
    return FD_INT2FIX(1);
  else return FD_FALSE;
}

/* Initialize */

fd_ptr_type fd_consed_index_type;

static int check_index(lispval x)
{
  int serial = FD_GET_IMMEDIATE(x,fd_index_type);
  if (serial<0) return 0;
  if (serial<FD_N_PRIMARY_INDEXES)
    if (fd_primary_indexes[serial])
      return 1;
    else return 0;
  else if (secondary_indexes) {
    int second_off=serial-FD_N_PRIMARY_INDEXES;
    if ( (second_off < 0) || (second_off  > fd_n_secondary_indexes) )
      return 0;
    else return (secondary_indexes[second_off]!=NULL);}
  else return 0;
}

/* Initializations */

FD_EXPORT void fd_init_indexes_c()
{
  u8_register_source_file(_FILEINFO);

  fd_type_names[fd_index_type]=_("index");
  fd_immediate_checkfns[fd_index_type]=check_index;

  fd_consed_index_type = fd_register_cons_type("raw index");
  fd_type_names[fd_consed_index_type]=_("raw index");

  u8_init_mutex(&consed_indexes_lock);
  consed_indexes = u8_malloc(64*sizeof(fd_index));
  consed_indexes_len=64;

  secondary_indexes     = u8_malloc(64*sizeof(fd_index));
  secondary_indexes_len = 64;

  fd_index_hashop=fd_intern("HASH");
  fd_index_slotsop=fd_intern("SLOTIDS");
  fd_index_bucketsop=fd_intern("BUCKETS");

  cachelevel_slot=fd_intern("CACHELEVEL");
  indexid_slot=fd_intern("INDEXID");
  source_slot=fd_intern("SOURCE");
  realpath_slot=fd_intern("REALPATH");
  cached_slot=fd_intern("CACHED");
  adds_slot=fd_intern("ADDS");
  edits_slot=fd_intern("EDITS");
  drops_slot=fd_intern("DROPS");
  replaced_slot=fd_intern("REPLACED");
  flags_slot=fd_intern("FLAGS");
  registered_slot=fd_intern("REGISTERED");
  opts_slot=fd_intern("OPTS");

  read_only_flag=FDSYM_READONLY;
  unregistered_flag=fd_intern("UNREGISTERED");
  registered_flag=fd_intern("REGISTERED");
  noswap_flag=fd_intern("NOSWAP");
  noerr_flag=fd_intern("NOERR");
  phased_flag=fd_intern("PHASED");
  background_flag=fd_intern("BACKGROUND");

  {
    struct FD_COMPOUND_TYPEINFO *e =
      fd_register_compound(fd_intern("INDEX"),NULL,NULL);
    e->compound_parser = index_parsefn;}

  fd_tablefns[fd_index_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_index_type]->get = table_indexget;
  fd_tablefns[fd_index_type]->add = table_indexadd;
  fd_tablefns[fd_index_type]->drop = table_indexdrop;
  fd_tablefns[fd_index_type]->store = table_indexstore;
  fd_tablefns[fd_index_type]->test = NULL;
  fd_tablefns[fd_index_type]->keys = table_indexkeys;
  fd_tablefns[fd_index_type]->getsize = NULL;

  fd_tablefns[fd_consed_index_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_consed_index_type]->get = table_indexget;
  fd_tablefns[fd_consed_index_type]->add = table_indexadd;
  fd_tablefns[fd_consed_index_type]->drop = table_indexdrop;
  fd_tablefns[fd_consed_index_type]->store = table_indexstore;
  fd_tablefns[fd_consed_index_type]->test = NULL;
  fd_tablefns[fd_consed_index_type]->keys = table_indexkeys;
  fd_tablefns[fd_consed_index_type]->getsize = NULL;

  fd_recyclers[fd_consed_index_type]=recycle_consed_index;
  fd_unparsers[fd_consed_index_type]=unparse_consed_index;
  fd_copiers[fd_consed_index_type]=copy_consed_index;

  fd_unparsers[fd_index_type]=unparse_index;
  metadata_readonly_props = fd_intern("_READONLY_PROPS");

  u8_init_rwlock(&indexes_lock);
  u8_init_mutex(&background_lock);

#if (FD_USE_TLS)
  u8_new_threadkey(&index_delays_key,NULL);
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
