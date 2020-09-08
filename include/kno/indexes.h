/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_INDEXES_H
#define KNO_INDEXES_H 1
#ifndef KNO_INDEXES_H_INFO
#define KNO_INDEXES_H_INFO "include/kno/indexes.h"
#endif

#ifndef KNO_STORAGE_INDEX_C
#define KNO_STORAGE_INDEX_C 0
#endif

#include "streams.h"

KNO_EXPORT u8_condition kno_BadIndexSpec,
  kno_FileIndexOverflow, kno_NotAFileIndex, kno_CorruptedIndex, kno_NoFileIndexes,
  kno_IndexCommitError, kno_EphemeralIndex;

KNO_EXPORT u8_condition kno_IndexCommit;

#define kno_lock_index(p) u8_lock_mutex(&((p)->index_lock))
#define kno_unlock_index(p) u8_unlock_mutex(&((p)->index_lock))

#define KNO_INDEX_CACHE_INIT   512
#define KNO_INDEX_ADDS_INIT    256
#define KNO_INDEX_DROPS_INIT   0
#define KNO_INDEX_REPLACE_INIT 0
KNO_EXPORT int kno_index_cache_init;
KNO_EXPORT int kno_index_edits_init;
KNO_EXPORT int kno_index_adds_init;

#define KNO_INDEX_ADD_CAPABILITY  (KNO_INDEX_FLAG(1))
#define KNO_INDEX_DROP_CAPABILITY (KNO_INDEX_FLAG(2))
#define KNO_INDEX_SET_CAPABILITY  (KNO_INDEX_FLAG(3))
#define KNO_INDEX_IN_BACKGROUND   (KNO_INDEX_FLAG(4))
#define KNO_INDEX_ONESLOT         (KNO_INDEX_FLAG(5))

#define KNO_MAX_PRIMARY_INDEXES 4096

#define KNO_INDEX_FIELDS \
  KNO_CONS_HEADER;                                                  \
  u8_string indexid, index_source, canonical_source;               \
  u8_string index_typeid;                                          \
  struct KNO_INDEX_HANDLER *index_handler;                          \
  kno_storage_flags index_flags, modified_flags;                    \
  int index_serialno;                                              \
  short index_cache_level;                                         \
  short index_loglevel;                                            \
  U8_MUTEX_DECL(index_commit_lock);                                \
  struct KNO_HASHTABLE index_cache;                                 \
  struct KNO_HASHTABLE index_adds, index_drops;                     \
  struct KNO_HASHTABLE index_stores;                                \
  struct KNO_SLOTMAP index_metadata, index_props;                   \
  lispval index_keyslot;                                           \
  lispval index_covers_slotids;                                    \
  lispval index_opts

typedef struct KNO_INDEX {KNO_INDEX_FIELDS;} KNO_INDEX;
typedef struct KNO_INDEX *kno_index;

U8_MAYBE_UNUSED static void kno_incref_index(kno_index ix)
{
  if (KNO_INDEX_CONSEDP(ix)) { kno_incref(LISPVAL(ix)); }
}
U8_MAYBE_UNUSED static void kno_decref_index(kno_index ix)
{
  if (KNO_INDEX_CONSEDP(ix)) { kno_decref(LISPVAL(ix)); }
}

/* Pool commit objects */

typedef struct KNO_INDEX_COMMITS {
  kno_index commit_index;
  kno_commit_phase commit_phase;
  unsigned int commit_2phase:1;
  ssize_t commit_n_adds, commit_n_drops, commit_n_stores;
  struct KNO_CONST_KEYVAL *commit_adds;
  struct KNO_CONST_KEYVAL *commit_drops;
  struct KNO_CONST_KEYVAL *commit_stores;
  lispval commit_metadata;
  struct KNO_COMMIT_TIMES commit_times;
  struct KNO_STREAM *commit_stream;} *kno_index_commits;

/* Lookup tables */

KNO_EXPORT kno_index kno_primary_indexes[KNO_MAX_PRIMARY_INDEXES];
KNO_EXPORT int kno_n_primary_indexes;

typedef struct KNO_KEY_SIZE {
  lispval keysize_key;
  long long keysize_count;} KNO_KEY_SIZE;
typedef struct KNO_KEY_SIZE *kno_key_size;

typedef struct KNO_INDEX_HANDLER {
  u8_string name; int version, length, n_handlers;
  void (*close)(kno_index ix);
  int (*commit)(kno_index ix,kno_commit_phase,struct KNO_INDEX_COMMITS *);
  lispval (*fetch)(kno_index ix,lispval key);
  int (*fetchsize)(kno_index ix,lispval key);
  int (*prefetch)(kno_index ix,lispval keys);
  lispval *(*fetchn)(kno_index ix,int n,const lispval *keys);
  lispval *(*fetchkeys)(kno_index ix,int *n);
  struct KNO_KEY_SIZE *(*fetchinfo)(kno_index ix,struct KNO_CHOICE *filter,int *n);
  int (*batchadd)(kno_index ix,lispval);
  kno_index (*create)(u8_string spec,void *type_data,
                     kno_storage_flags flags,lispval opts);
  int (*walker)(kno_index,kno_walker,void *,kno_walk_flags,int);
  void (*recycle)(kno_index p);
  lispval (*indexctl)(kno_index ix,lispval op,int n,kno_argvec args);}
  KNO_INDEX_HANDLER;
typedef struct KNO_INDEX_HANDLER *kno_index_handler;

KNO_EXPORT lispval kno_index_ctl(kno_index p,lispval op,int n,kno_argvec args);

KNO_EXPORT lispval kno_default_indexctl(kno_index ix,lispval op,int n,kno_argvec args);
KNO_EXPORT lispval kno_index_base_metadata(kno_index ix);
KNO_EXPORT void kno_recycle_index(struct KNO_INDEX *ix);

KNO_EXPORT lispval kno_index_hashop, kno_index_slotsop, kno_index_bucketsop;

#define KNO_INDEX_READONLYP(ix) ( ((ix)->index_flags) & (KNO_STORAGE_READ_ONLY) )

#if 0
struct KNO_INDEX_HANDLER some_handler={
  "somehandler", 1, sizeof(somestruct), 4,
  NULL, /* close */
  NULL, /* save */
  NULL, /* commit */
  NULL, /* fetch */
  NULL, /* fetchsize */
  NULL, /* prefetch */
  NULL /* fetchn */
  NULL, /* fetchkeys */
  NULL, /* fetchinfo */
  NULL, /* batchadd */
  NULL, /* create */
  NULL, /* walk */
  NULL, /* recycle */
  NULL /* indexctl */
};
#endif

KNO_EXPORT int kno_for_indexes(int (*fcn)(kno_index,void *),void *data);
KNO_EXPORT lispval kno_get_all_indexes(void);

KNO_EXPORT void kno_init_index
  (kno_index ix,
   struct KNO_INDEX_HANDLER *h,
   u8_string id,u8_string src,u8_string csrc,
   kno_storage_flags flags,
   lispval metadata,
   lispval opts);
KNO_EXPORT int kno_index_set_metadata(kno_index ix,lispval metadata);
KNO_EXPORT void kno_reset_index_tables
  (kno_index ix,ssize_t cache,ssize_t edits,ssize_t adds);

KNO_EXPORT void kno_register_index(kno_index ix);

KNO_EXPORT int kno_index_store(kno_index ix,lispval key,lispval value);
KNO_EXPORT int kno_index_drop(kno_index ix,lispval key,lispval value);
KNO_EXPORT int kno_index_merge(kno_index ix,kno_hashtable table);
KNO_EXPORT int kno_commit_index(kno_index ix);
KNO_EXPORT int kno_index_save(kno_index ix,lispval,lispval,lispval,lispval);
KNO_EXPORT lispval kno_index_fetchn(kno_index ix,lispval);
KNO_EXPORT void kno_index_close(kno_index ix);
KNO_EXPORT kno_index _kno_indexptr(lispval x);
KNO_EXPORT lispval _kno_index_get(kno_index ix,lispval key);
KNO_EXPORT lispval kno_index_fetch(kno_index ix,lispval key);
KNO_EXPORT lispval kno_index_keys(kno_index ix);
KNO_EXPORT lispval kno_index_sizes(kno_index ix);
KNO_EXPORT lispval kno_index_keysizes(kno_index ix,lispval keys);
KNO_EXPORT int _kno_index_add(kno_index ix,lispval key,lispval value);
KNO_EXPORT int kno_batch_add(kno_index ix,lispval table);
KNO_EXPORT int kno_index_prefetch(kno_index ix,lispval keys);

KNO_EXPORT kno_index kno_find_index(u8_string);
KNO_EXPORT u8_string kno_locate_index(u8_string);

KNO_EXPORT kno_index kno_open_index(u8_string,kno_storage_flags,lispval);
KNO_EXPORT kno_index kno_get_index(u8_string,kno_storage_flags,lispval);
KNO_EXPORT kno_index kno_find_index_by_id(u8_string);
KNO_EXPORT kno_index kno_find_index_by_source(u8_string);

KNO_EXPORT void kno_index_swapout(kno_index ix,lispval keys);
KNO_EXPORT void kno_index_setcache(kno_index ix,int level);

KNO_EXPORT kno_index kno_use_index(u8_string spec,kno_storage_flags,lispval);

KNO_EXPORT void kno_swapout_indexes(void);
KNO_EXPORT void kno_close_indexes(void);
KNO_EXPORT int kno_commit_indexes(void);
KNO_EXPORT int kno_commit_indexes_noerr(void);
KNO_EXPORT long kno_index_cache_load(void);
KNO_EXPORT lispval kno_cached_keys(kno_index p);

KNO_EXPORT kno_index kno_lisp2index(lispval lp);
KNO_EXPORT lispval kno_index_ref(kno_index ix);

KNO_EXPORT int kno_add_to_background(kno_index ix);

/* Temp indexes */

typedef struct KNO_TEMPINDEX {
  KNO_INDEX_FIELDS;} KNO_TEMPINDEX;
typedef struct KNO_TEMP_INDEX *kno_tempindex;

KNO_EXPORT kno_index kno_make_tempindex
(u8_string name,kno_storage_flags flags,lispval opts);
KNO_EXPORT int kno_tempindexp(kno_index ix);

/* Network indexes */

/* Server capabilities */
#define KNO_ISERVER_FETCHN 1
#define KNO_ISERVER_ADDN 2
#define KNO_ISERVER_DROP 4
#define KNO_ISERVER_RESET 8

/* External indexes */

typedef struct KNO_EXTINDEX {
  KNO_INDEX_FIELDS;
  lispval fetchfn, commitfn, state;}
  KNO_EXTINDEX;
typedef struct KNO_EXTINDEX *kno_extindex;

KNO_EXPORT kno_index kno_make_extindex
  (u8_string name,lispval fetchfn,lispval commitfn,
   lispval state,kno_storage_flags flags,lispval opts);

KNO_EXPORT struct KNO_INDEX_HANDLER kno_extindex_handler;

KNO_EXPORT int kno_extindexp(kno_index ix);

/* Proc indexes */

struct KNO_PROCINDEX_METHODS {
  lispval openfn, fetchfn, fetchsizefn, fetchnfn, prefetchfn,
    fetchkeysfn, fetchinfofn,
    batchaddfn, ctlfn, commitfn, createfn, closefn;};

typedef struct KNO_PROCINDEX {
  KNO_INDEX_FIELDS;
  struct KNO_PROCINDEX_METHODS *index_methods;
  lispval index_state;}
  KNO_PROCINDEX;
typedef struct KNO_PROCINDEX *kno_procindex;

KNO_EXPORT struct KNO_INDEX_HANDLER kno_procindex_handler;

KNO_EXPORT
kno_index kno_make_procindex(lispval opts,lispval state,
                           u8_string typeid,
                           u8_string label,
                           u8_string source);
KNO_EXPORT void kno_register_procindex(u8_string typename,lispval handler);

KNO_EXPORT int kno_procindexp(kno_index ix);

/* Compound indexes */

typedef struct KNO_AGGREGATE_INDEX {
  KNO_INDEX_FIELDS;
  unsigned int ax_n_indexes, ax_n_allocd;
  kno_index *ax_indexes;
  struct U8_MEMLIST *ax_oldvecs;
  U8_MUTEX_DECL(index_lock);} KNO_AGGREGATE_INDEX;
typedef struct KNO_AGGREGATE_INDEX *kno_aggregate_index;

KNO_EXPORT int kno_add_to_aggregate_index(kno_aggregate_index ix,kno_index add);
KNO_EXPORT kno_aggregate_index kno_make_aggregate_index
(lispval opts,int n_allocd,int n_indexes,kno_index *indexes);
KNO_EXPORT int kno_aggregate_indexp(kno_index ix);

KNO_EXPORT struct KNO_INDEX_HANDLER *kno_aggregate_index_handler;

KNO_EXPORT struct KNO_AGGREGATE_INDEX *kno_background;

KNO_EXPORT kno_index _kno_indexptr(lispval lp);
KNO_EXPORT lispval _kno_index2lisp(kno_index ix);
KNO_EXPORT kno_index _kno_get_writable_index(kno_index ix);

#if KNO_INLINE_INDEXES
#define kno_indexptr           __kno_indexptr
#define kno_index2lisp         __kno_index2lisp
#define kno_get_writable_index __kno_get_writable_index
#else
#define kno_indexptr           _kno_indexptr
#define kno_index2lisp         _kno_index2lisp
#define kno_get_writable_index _kno_get_writable_index
#endif

#if KNO_INLINE_INDEXES || KNO_STORAGE_INDEX_C
KNO_FASTOP U8_MAYBE_UNUSED kno_index __kno_indexptr(lispval x)
{
  if (KNO_IMMEDIATE_TYPEP(x,kno_index_type)) {
    int serial = KNO_GET_IMMEDIATE(x,kno_index_type);
    if (serial<0) return NULL;
    else if (serial<KNO_MAX_PRIMARY_INDEXES)
      return kno_primary_indexes[serial];
    else return NULL;}
  else if ( (KNO_CONSP(x)) && (KNO_TYPEP(x,kno_consed_index_type)) )
    return (kno_index)x;
  else return (kno_index)NULL;
}
KNO_FASTOP U8_MAYBE_UNUSED lispval __kno_index2lisp(kno_index ix)
{
  if (ix == NULL)
    return KNO_ERROR;
  else if (ix->index_serialno>=0)
    return LISPVAL_IMMEDIATE(kno_index_type,ix->index_serialno);
  else return (lispval)ix;
}
U8_MAYBE_UNUSED static kno_index __kno_get_writable_index(kno_index ix)
{
  if (ix == NULL) return ix;
  else if (U8_BITP(ix->index_flags,KNO_STORAGE_READ_ONLY)) {
    lispval front_val = kno_slotmap_get(&(ix->index_props),KNOSYM_FRONT,KNO_VOID);
    if (KNO_INDEXP(front_val)) {
      kno_index front = kno_indexptr(front_val);
      if (U8_BITP(front->index_flags,KNO_STORAGE_READ_ONLY)) {
        kno_decref(front_val);
        return NULL;}
      else return front;}
    else {
      kno_decref(front_val);
      return NULL;}}
  else {
    kno_incref_index(ix);
    return ix;}
}
#endif

/* Inline index adds */

#if KNO_INLINE_INDEXES
KNO_FASTOP lispval kno_index_get(kno_index ix,lispval key)
{
  lispval cached;
#if KNO_USE_THREADCACHE
  KNOTC *knotc = kno_threadcache; struct KNO_PAIR tempkey;
  if (knotc) {
    KNO_INIT_STATIC_CONS(&tempkey,kno_pair_type);
    tempkey.car = kno_index2lisp(ix); tempkey.cdr = key;
    cached = kno_hashtable_get(&(knotc->indexes),(lispval)&tempkey,KNO_VOID);
    if (!(KNO_VOIDP(cached))) return cached;}
#endif
  if (ix->index_cache_level==0) cached = KNO_VOID;
  else if ((KNO_PAIRP(key)) && (!(KNO_VOIDP(ix->index_covers_slotids))) &&
      (!(kno_contains_atomp(KNO_CAR(key),ix->index_covers_slotids))))
    return KNO_EMPTY_CHOICE;
  else cached = kno_hashtable_get(&(ix->index_cache),key,KNO_VOID);
  if (KNO_VOIDP(cached)) cached = kno_index_fetch(ix,key);
#if KNO_USE_THREADCACHE
  if (knotc) {
    kno_hashtable_store(&(knotc->indexes),(lispval)&tempkey,cached);}
#endif
  return cached;
}
KNO_FASTOP int kno_index_add(kno_index ix_arg,lispval key,lispval value)
{
  int rv = -1;
  KNOTC *knotc = (KNO_WRITETHROUGH_THREADCACHE)?(kno_threadcache):(NULL);
  kno_index ix = kno_get_writable_index(ix_arg);
  if (ix == NULL)
    return _kno_index_add(ix_arg,key,value);

  kno_hashtable adds = &(ix->index_adds);
  kno_hashtable cache = &(ix->index_cache);

  if (KNO_CHOICEP(key)) {
    const lispval *keys = KNO_CHOICE_DATA(key);
    unsigned int n = KNO_CHOICE_SIZE(key);
    rv = kno_hashtable_iterkeys(adds,kno_table_add,n,keys,value);
    if (rv<0) {}
    else if (ix->index_cache_level>0)
      rv = kno_hashtable_iterkeys(cache,kno_table_add_if_present,n,keys,value);
    else NO_ELSE;}
  else if (KNO_PRECHOICEP(key)) {
    lispval normchoice = kno_make_simple_choice(key);
    const lispval *keys = KNO_CHOICE_DATA(normchoice);
    unsigned int n = KNO_CHOICE_SIZE(key);
    rv = kno_hashtable_iterkeys(adds,kno_table_add,n,keys,value);
    if (rv<0) {}
    else if (ix->index_cache_level>0)
      rv = kno_hashtable_iterkeys(cache,kno_table_add_if_present,n,keys,value);
    else NO_ELSE;
    kno_decref(normchoice);}
  else {
    rv = kno_hashtable_add(adds,key,value);
    if (rv<0) {}
    else if (ix->index_cache_level>0)
      rv = kno_hashtable_op(cache,kno_table_add_if_present,key,value);
    else NO_ELSE;}

  if ( (rv>=0) && (knotc) && (knotc->indexes.table_n_keys) ) {
    KNO_DO_CHOICES(akey,key) {
      struct KNO_PAIR tempkey;
      KNO_INIT_STATIC_CONS(&tempkey,kno_pair_type);
      tempkey.car = kno_index2lisp(ix); tempkey.cdr = akey;
      if (kno_hashtable_probe(&knotc->indexes,(lispval)&tempkey)) {
        kno_hashtable_add(&knotc->indexes,(lispval)&tempkey,value);}}}

  if ((rv >= 0) &&
      (ix->index_flags&KNO_INDEX_IN_BACKGROUND) &&
      (kno_background->index_cache.table_n_keys)) {
    kno_hashtable bgcache = (&(kno_background->index_cache));
    if (KNO_CHOICEP(key)) {
      const lispval *keys = KNO_CHOICE_DATA(key);
      unsigned int n = KNO_CHOICE_SIZE(key);
      /* This will force it to be re-read from the source indexes */
      rv = kno_hashtable_iterkeys(bgcache,kno_table_replace,n,keys,KNO_VOID);}
    else rv = kno_hashtable_op(bgcache,kno_table_replace,key,KNO_VOID);}

  if (rv<0) {}
  else if ((!(KNO_VOIDP(ix->index_covers_slotids))) &&
           (KNO_EXPECT_TRUE(KNO_PAIRP(key))) &&
           (KNO_EXPECT_TRUE((KNO_OIDP(KNO_CAR(key))) ||
                           (KNO_SYMBOLP(KNO_CAR(key)))))) {
    if (!(kno_contains_atomp(KNO_CAR(key),ix->index_covers_slotids))) {
      kno_decref(ix->index_covers_slotids);
      ix->index_covers_slotids = KNO_VOID;}}
  else NO_ELSE;

  if (KNO_INDEX_CONSEDP(ix)) {kno_decref(LISPVAL(ix));}

  return rv;
}
#else
#define kno_index_get(ix,key) _kno_index_get(ix,key)
#define kno_index_add(ix,key,val) _kno_index_add(ix,key,val)
#endif

/* IPEVAL delays */

#ifndef KNO_N_INDEX_DELAYS
#define KNO_N_INDEX_DELAYS 1024
#endif

KNO_EXPORT void kno_init_index_delays(void);
KNO_EXPORT lispval *kno_get_index_delays(void);
KNO_EXPORT int kno_execute_index_delays(kno_index ix,void *data);

#endif /* KNO_INDEXES_H */

