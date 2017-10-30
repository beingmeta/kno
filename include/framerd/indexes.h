/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_INDEXES_H
#define FRAMERD_INDEXES_H 1
#ifndef FRAMERD_INDEXES_H_INFO
#define FRAMERD_INDEXES_H_INFO "include/framerd/indexes.h"
#endif

#include "streams.h"

FD_EXPORT u8_condition fd_BadIndexSpec,
  fd_FileIndexOverflow, fd_NotAFileIndex, fd_CorruptedIndex, fd_NoFileIndexes,
  fd_IndexCommitError, fd_EphemeralIndex;

FD_EXPORT u8_condition fd_IndexCommit;

#define fd_lock_index(p) u8_lock_mutex(&((p)->index_lock))
#define fd_unlock_index(p) u8_unlock_mutex(&((p)->index_lock))

#define FD_INDEX_CACHE_INIT   512
#define FD_INDEX_ADDS_INIT    256
#define FD_INDEX_DROPS_INIT   0
#define FD_INDEX_REPLACE_INIT 0
FD_EXPORT int fd_index_cache_init;
FD_EXPORT int fd_index_edits_init;
FD_EXPORT int fd_index_adds_init;

#define FD_INDEX_ADD_CAPABILITY  (FD_INDEX_FLAG(1))
#define FD_INDEX_DROP_CAPABILITY (FD_INDEX_FLAG(2))
#define FD_INDEX_SET_CAPABILITY  (FD_INDEX_FLAG(3))
#define FD_INDEX_IN_BACKGROUND   (FD_INDEX_FLAG(4))

#define FD_N_PRIMARY_INDEXES 1024

#define FD_INDEX_FIELDS \
  FD_CONS_HEADER;                                                  \
  u8_string indexid, index_source;                                 \
  u8_string index_typeid;                                          \
  struct FD_INDEX_HANDLER *index_handler;                          \
  fd_storage_flags index_flags, modified_flags;                    \
  int index_serialno;                                              \
  short index_cache_level;                                         \
  U8_MUTEX_DECL(index_commit_lock);                                \
  struct FD_HASHTABLE index_cache;                                 \
  struct FD_HASHTABLE index_adds, index_drops;                     \
  struct FD_HASHTABLE index_stores;                                \
  struct FD_SLOTMAP index_metadata, index_props;                   \
  lispval index_keyslot;                                           \
  lispval index_covers_slotids;                                    \
  lispval index_opts

typedef struct FD_INDEX {FD_INDEX_FIELDS;} FD_INDEX;
typedef struct FD_INDEX *fd_index;

FD_EXPORT fd_index fd_primary_indexes[FD_N_PRIMARY_INDEXES];
FD_EXPORT fd_index *fd_secondary_indexes;
FD_EXPORT int fd_n_primary_indexes, fd_n_secondary_indexes;

typedef struct FD_KEY_SIZE {
  lispval keysize_key;
  long long keysize_count;} FD_KEY_SIZE;
typedef struct FD_KEY_SIZE *fd_key_size;

typedef struct FD_INDEX_HANDLER {
  u8_string name; int version, length, n_handlers;
  void (*close)(fd_index ix);
  int (*save)(fd_index ix,
              struct FD_CONST_KEYVAL *adds,int n_adds,
              struct FD_CONST_KEYVAL *drops,int n_drops,
              struct FD_CONST_KEYVAL *stores,int n_stores,
              lispval metadata);
  int (*commit)(fd_index ix,int phase);
  lispval (*fetch)(fd_index ix,lispval key);
  int (*fetchsize)(fd_index ix,lispval key);
  int (*prefetch)(fd_index ix,lispval keys);
  lispval *(*fetchn)(fd_index ix,int n,const lispval *keys);
  lispval *(*fetchkeys)(fd_index ix,int *n);
  struct FD_KEY_SIZE *(*fetchinfo)(fd_index ix,struct FD_CHOICE *filter,int *n);
  int (*batchadd)(fd_index ix,lispval);
  fd_index (*create)(u8_string spec,void *type_data,
                     fd_storage_flags flags,lispval opts);
  int (*walker)(fd_index,fd_walker,void *,fd_walk_flags,int);
  void (*recycle)(fd_index p);
  lispval (*indexctl)(fd_index ix,lispval op,int n,lispval *args);}
  FD_INDEX_HANDLER;
typedef struct FD_INDEX_HANDLER *fd_index_handler;

FD_EXPORT lispval fd_index_ctl(fd_index p,lispval op,int n,lispval *args);

FD_EXPORT lispval fd_default_indexctl(fd_index ix,lispval op,int n,lispval *args);
FD_EXPORT lispval fd_index_base_metadata(fd_index ix);

FD_EXPORT lispval fd_index_hashop, fd_index_slotsop, fd_index_bucketsop;

#define FD_INDEX_READONLYP(ix) ( ((ix)->index_flags) & (FD_STORAGE_READ_ONLY) )

#if 0
struct FD_INDEX_HANDLER some_handler={
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

FD_EXPORT int fd_for_indexes(int (*fcn)(fd_index,void *),void *data);

FD_EXPORT void fd_init_index
  (fd_index ix,
   struct FD_INDEX_HANDLER *h,
   u8_string id,u8_string src,
   fd_storage_flags flags);
FD_EXPORT void fd_reset_index_tables
  (fd_index ix,ssize_t cache,ssize_t edits,ssize_t adds);

FD_EXPORT void fd_register_index(fd_index ix);

FD_EXPORT int fd_index_store(fd_index ix,lispval key,lispval value);
FD_EXPORT int fd_index_drop(fd_index ix,lispval key,lispval value);
FD_EXPORT int fd_index_merge(fd_index ix,fd_hashtable table);
FD_EXPORT int fd_index_commit(fd_index ix);
FD_EXPORT int fd_index_save(fd_index ix,lispval,lispval,lispval,lispval);
FD_EXPORT void fd_index_close(fd_index ix);
FD_EXPORT fd_index _fd_indexptr(lispval x);
FD_EXPORT lispval _fd_index_get(fd_index ix,lispval key);
FD_EXPORT lispval fd_index_fetch(fd_index ix,lispval key);
FD_EXPORT lispval fd_index_keys(fd_index ix);
FD_EXPORT lispval fd_index_sizes(fd_index ix);
FD_EXPORT lispval fd_index_keysizes(fd_index ix,lispval keys);
FD_EXPORT int _fd_index_add(fd_index ix,lispval key,lispval value);
FD_EXPORT int fd_batch_add(fd_index ix,lispval table);
FD_EXPORT int fd_index_prefetch(fd_index ix,lispval keys);

FD_EXPORT fd_index fd_find_index(u8_string);
FD_EXPORT u8_string fd_locate_index(u8_string);

FD_EXPORT fd_index fd_open_index(u8_string,fd_storage_flags,lispval);
FD_EXPORT fd_index fd_get_index(u8_string,fd_storage_flags,lispval);
FD_EXPORT fd_index fd_find_index_by_id(u8_string);
FD_EXPORT fd_index fd_find_index_by_source(u8_string);

FD_EXPORT void fd_index_swapout(fd_index ix,lispval keys);
FD_EXPORT void fd_index_setcache(fd_index ix,int level);

FD_EXPORT fd_index fd_use_index(u8_string spec,fd_storage_flags,lispval);

FD_EXPORT void fd_swapout_indexes(void);
FD_EXPORT void fd_close_indexes(void);
FD_EXPORT int fd_commit_indexes(void);
FD_EXPORT int fd_commit_indexes_noerr(void);
FD_EXPORT long fd_index_cache_load(void);
FD_EXPORT lispval fd_cached_keys(fd_index p);

FD_EXPORT fd_index fd_lisp2index(lispval lp);
FD_EXPORT lispval fd_index2lisp(fd_index ix);
FD_EXPORT lispval fd_index_ref(fd_index ix);

FD_EXPORT int fd_add_to_background(fd_index ix);

/* Network indexes */

/* Server capabilities */
#define FD_ISERVER_FETCHN 1
#define FD_ISERVER_ADDN 2
#define FD_ISERVER_DROP 4
#define FD_ISERVER_RESET 8

/* External indexes */

typedef struct FD_EXTINDEX {
  FD_INDEX_FIELDS;
  lispval fetchfn, commitfn, state;}
  FD_EXTINDEX;
typedef struct FD_EXTINDEX *fd_extindex;

FD_EXPORT fd_index fd_make_extindex
  (u8_string name,lispval fetchfn,lispval commitfn,lispval state,int reg);

FD_EXPORT struct FD_INDEX_HANDLER fd_extindex_handler;

/* Proc indexes */

typedef struct FD_PROCINDEX {
  FD_INDEX_FIELDS;
  lispval index_state;
  lispval fetchfn, fetchsizefn, fetchnfn, prefetchfn,
    fetchkeysfn, fetchinfofn,
    batchaddfn, ctlfn, savefn, closefn;}
  FD_PROCINDEX;
typedef struct FD_PROCINDEX *fd_procindex;

FD_EXPORT struct FD_INDEX_HANDLER fd_procindex_handler;

FD_EXPORT
fd_index fd_make_procindex(lispval opts,lispval state,
                           u8_string typeid,
                           u8_string label,
                           u8_string source);

/* Compound indexes */

typedef struct FD_COMPOUND_INDEX {
  FD_INDEX_FIELDS;
  unsigned int n_indexes; fd_index *indexes;
  U8_MUTEX_DECL(index_lock);} FD_COMPOUND_INDEX;
typedef struct FD_COMPOUND_INDEX *fd_compound_index;

FD_EXPORT int fd_add_to_compound_index(fd_compound_index ix,fd_index add);
FD_EXPORT fd_index fd_make_compound_index(int n_indexes,fd_index *indexes);

FD_EXPORT struct FD_COMPOUND_INDEX *fd_background;

/* Inline index adds */

#if FD_INLINE_INDEXES
FD_FASTOP lispval fd_index_get(fd_index ix,lispval key)
{
  lispval cached;
#if FD_USE_THREADCACHE
  FDTC *fdtc = fd_threadcache; struct FD_PAIR tempkey;
  if (fdtc) {
    FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
    tempkey.car = fd_index2lisp(ix); tempkey.cdr = key;
    cached = fd_hashtable_get(&(fdtc->indexes),(lispval)&tempkey,FD_VOID);
    if (!(FD_VOIDP(cached))) return cached;}
#endif
  if (ix->index_cache_level==0) cached = FD_VOID;
  else if ((FD_PAIRP(key)) && (!(FD_VOIDP(ix->index_covers_slotids))) &&
      (!(atomic_choice_containsp(FD_CAR(key),ix->index_covers_slotids))))
    return FD_EMPTY_CHOICE;
  else cached = fd_hashtable_get(&(ix->index_cache),key,FD_VOID);
  if (FD_VOIDP(cached)) cached = fd_index_fetch(ix,key);
#if FD_USE_THREADCACHE
  if (fdtc) {
    fd_hashtable_store(&(fdtc->indexes),(lispval)&tempkey,cached);}
#endif
  return cached;
}
FD_FASTOP int fd_index_add(fd_index ix,lispval key,lispval value)
{
  int rv = -1;
  FDTC *fdtc = (FD_WRITETHROUGH_THREADCACHE)?(fd_threadcache):(NULL);
  fd_hashtable adds = &(ix->index_adds);
  fd_hashtable cache = &(ix->index_cache);
  if (U8_BITP(ix->index_flags,FD_STORAGE_READ_ONLY))
    /* This will signal an error */
    return _fd_index_add(ix,key,value);
  else if (FD_CHOICEP(key)) {
    const lispval *keys = FD_CHOICE_DATA(key);
    unsigned int n = FD_CHOICE_SIZE(key);
    rv = fd_hashtable_iterkeys(adds,fd_table_add,n,keys,value);
    if (rv<0) return rv;
    else if (ix->index_cache_level>0)
      rv = fd_hashtable_iterkeys(cache,fd_table_add_if_present,n,keys,value);}
  else if (FD_PRECHOICEP(key)) {
    lispval normchoice = fd_make_simple_choice(key);
    const lispval *keys = FD_CHOICE_DATA(key);
    unsigned int n = FD_CHOICE_SIZE(key);
    rv = fd_hashtable_iterkeys(adds,fd_table_add,n,keys,value);
    if (rv<0) return rv;
    else if (ix->index_cache_level>0)
      rv = fd_hashtable_iterkeys(cache,fd_table_add_if_present,n,keys,value);
    fd_decref(normchoice);}
  else {
    rv = fd_hashtable_add(adds,key,value);
    if (rv<0) return rv;
    rv = fd_hashtable_op(cache,fd_table_add_if_present,key,value);}
  if (rv<0) return rv;
  if ( (fdtc) && (fdtc->indexes.table_n_keys) ) {
    FD_DO_CHOICES(akey,key) {
      struct FD_PAIR tempkey;
      FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
      tempkey.car = fd_index2lisp(ix); tempkey.cdr = akey;
      if (fd_hashtable_probe(&fdtc->indexes,(lispval)&tempkey)) {
        fd_hashtable_add(&fdtc->indexes,(lispval)&tempkey,value);}}}

  if ((ix->index_flags&FD_INDEX_IN_BACKGROUND) &&
      (fd_background->index_cache.table_n_keys)) {
    fd_hashtable bgcache = (&(fd_background->index_cache));
    if (FD_CHOICEP(key)) {
      const lispval *keys = FD_CHOICE_DATA(key);
      unsigned int n = FD_CHOICE_SIZE(key);
      /* This will force it to be re-read from the source indexes */
      rv = fd_hashtable_iterkeys(bgcache,fd_table_replace,n,keys,FD_VOID);}
    else rv = fd_hashtable_op(bgcache,fd_table_replace,key,FD_VOID);}

  if (rv<0) return rv;

  if ((!(FD_VOIDP(ix->index_covers_slotids))) &&
      (FD_EXPECT_TRUE(FD_PAIRP(key))) &&
      (FD_EXPECT_TRUE((FD_OIDP(FD_CAR(key))) ||
                      (FD_SYMBOLP(FD_CAR(key)))))) {
    if (!(atomic_choice_containsp(FD_CAR(key),ix->index_covers_slotids))) {
      fd_decref(ix->index_covers_slotids);
      ix->index_covers_slotids = FD_VOID;}}

  return rv;
}
FD_FASTOP U8_MAYBE_UNUSED fd_index fd_indexptr(lispval x)
{
  if (FD_IMMEDIATEP(x)) {
    int serial = FD_GET_IMMEDIATE(x,fd_index_type);
    if (serial<0) return NULL;
    else if (serial<FD_N_PRIMARY_INDEXES)
      return fd_primary_indexes[serial];
    else if (serial<(FD_N_PRIMARY_INDEXES+fd_n_secondary_indexes))
      return fd_secondary_indexes[serial-FD_N_PRIMARY_INDEXES];
    else return NULL;}
  else if ((FD_CONSP(x))&&(FD_TYPEP(x,fd_consed_index_type)))
    return (fd_index)x;
  else return (fd_index)NULL;
}
#else
#define fd_index_get(ix,key) _fd_index_get(ix,key)
#define fd_index_add(ix,key,val) _fd_index_add(ix,key,val)
#define fd_indexptr(ix) _fd_indexptr(ix)
#endif

/* IPEVAL delays */

#ifndef FD_N_INDEX_DELAYS
#define FD_N_INDEX_DELAYS 1024
#endif

FD_EXPORT void fd_init_index_delays(void);
FD_EXPORT lispval *fd_get_index_delays(void);
FD_EXPORT int fd_execute_index_delays(fd_index ix,void *data);

#endif /* FRAMERD_INDEXES_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
