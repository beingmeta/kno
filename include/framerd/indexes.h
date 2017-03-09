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

FD_EXPORT fd_exception
  fd_FileIndexOverflow, fd_NotAFileIndex, fd_NoFileIndexes, fd_BadIndexSpec,
  fd_IndexCommitError, fd_EphemeralIndex;

FD_EXPORT u8_condition fd_IndexCommit;

#define fd_lock_index(p) u8_lock_mutex(&((p)->index_lock))
#define fd_unlock_index(p) u8_unlock_mutex(&((p)->index_lock))

#define FD_INDEX_CACHE_INIT 73
#define FD_INDEX_EDITS_INIT 73
#define FD_INDEX_ADDS_INIT 123
FD_EXPORT int fd_index_cache_init;
FD_EXPORT int fd_index_edits_init;
FD_EXPORT int fd_index_adds_init;

#define FD_INDEX_ADD_CAPABILITY  (FDKB_INDEX_FLAG(1))
#define FD_INDEX_DROP_CAPABILITY (FDKB_INDEX_FLAG(2))
#define FD_INDEX_SET_CAPABILITY  (FDKB_INDEX_FLAG(3))
#define FD_INDEX_IN_BACKGROUND   (FDKB_INDEX_FLAG(4))
#define FD_INDEX_NOSWAP          (FDKB_INDEX_FLAG(5))

#define FD_N_PRIMARY_INDEXES 128

#define FD_INDEX_FIELDS \
  FD_CONS_HEADER;						   \
  u8_string index_idstring, index_source, index_xinfo;		   \
  struct FD_INDEX_HANDLER *index_handler;			   \
  fdkbase_flags index_flags, modified_flags;			   \
  int index_serialno;						   \
  short index_cache_level;					   \
  struct FD_HASHTABLE index_cache, index_adds, index_edits;        \
  fdtype index_covers_slotids

typedef struct FD_INDEX {FD_INDEX_FIELDS;} FD_INDEX;
typedef struct FD_INDEX *fd_index;

FD_EXPORT fd_index fd_primary_indexes[], *fd_secondary_indexes;
FD_EXPORT int fd_n_primary_indexes, fd_n_secondary_indexes;

fd_index (*fd_file_index_type)(u8_string spec,fdkbase_flags flags);

typedef struct FD_KEY_SIZE {
  fdtype keysizekey; unsigned int keysizenvals;} FD_KEY_SIZE;
typedef struct FD_KEY_SIZE *fd_key_size;

typedef struct FD_INDEX_HANDLER {
  u8_string name; int version, length, n_handlers;
  void (*close)(fd_index ix);
  int (*commit)(fd_index ix);
  void (*setcache)(fd_index ix,int level);
  void (*setbuf)(fd_index p,int size);
  fdtype (*fetch)(fd_index ix,fdtype key);
  int (*fetchsize)(fd_index ix,fdtype key);
  int (*prefetch)(fd_index ix,fdtype keys);
  fdtype *(*fetchn)(fd_index ix,int n,fdtype *keys);
  fdtype *(*fetchkeys)(fd_index ix,int *n);
  struct FD_KEY_SIZE *(*fetchsizes)(fd_index ix,int *n);
  fdtype (*metadata)(fd_index ix,fdtype);
  int (*sync)(fd_index p);
  fd_index (*create)(u8_string spec,void *type_data,
		     fdkbase_flags flags,fdtype opts);
  fdtype (*indexop)(fd_index ix,int indexop,fdtype,fdtype,fdtype);
} FD_INDEX_HANDLER;
typedef struct FD_INDEX_HANDLER *fd_index_handler;

#define FDKB_INDEXOP_PRELOAD     (1<<0)
#define FDKB_INDEXOP_STATS       (1<<1)
#define FDKB_INDEXOP_LABEL       (1<<2)
#define FDKB_INDEXOP_POPULATE    (1<<3)

#if 0
struct FD_INDEX_HANDLER some_handler={
  "somehandler", 1, sizeof(somestruct), 4,
  NULL, /* close */
  NULL, /* commit */
  NULL, /* setcache */
  NULL, /* setbuf */
  NULL, /* fetch */
  NULL, /* fetchsize */
  NULL /* fetchn */
  NULL, /* fetchkeys */
  NULL, /* fetchsizes */
  NULL /* sync */
  NULL, /* creates */
  NULL /* indexop */
};
#endif

FD_EXPORT int fd_for_indexes(int (*fcn)(fd_index,void *),void *data);

FD_EXPORT void fd_init_index
  (fd_index ix,struct FD_INDEX_HANDLER *h,u8_string source,fdkbase_flags flags);
FD_EXPORT void fd_reset_index_tables
  (fd_index ix,ssize_t cache,ssize_t edits,ssize_t adds);

FD_EXPORT void fd_register_index(fd_index ix);

FD_EXPORT int fd_index_store(fd_index ix,fdtype key,fdtype value);
FD_EXPORT int fd_index_drop(fd_index ix,fdtype key,fdtype value);
FD_EXPORT int fd_index_merge(fd_index ix,fd_hashtable table);
FD_EXPORT int fd_index_commit(fd_index ix);
FD_EXPORT void fd_index_close(fd_index ix);
FD_EXPORT fd_index _fd_indexptr(fdtype x);
FD_EXPORT fdtype _fd_index_get(fd_index ix,fdtype key);
FD_EXPORT fdtype fd_index_fetch(fd_index ix,fdtype key);
FD_EXPORT fdtype fd_index_keys(fd_index ix);
FD_EXPORT fdtype fd_index_sizes(fd_index ix);
FD_EXPORT int _fd_index_add(fd_index ix,fdtype key,fdtype value);
FD_EXPORT int fd_index_prefetch(fd_index ix,fdtype keys);

FD_EXPORT fd_index fd_open_index(u8_string,fdkbase_flags);
FD_EXPORT fd_index fd_get_index(u8_string,fdkbase_flags);
FD_EXPORT fd_index fd_find_index_by_cid(u8_string);

FD_EXPORT void fd_index_swapout(fd_index ix);
FD_EXPORT void fd_index_setcache(fd_index ix,int level);

FD_EXPORT fd_index fd_use_index(u8_string spec,fdkbase_flags);

FD_EXPORT void fd_swapout_indexes(void);
FD_EXPORT void fd_close_indexes(void);
FD_EXPORT int fd_commit_indexes(void);
FD_EXPORT int fd_commit_indexes_noerr(void);
FD_EXPORT long fd_index_cache_load(void);
FD_EXPORT fdtype fd_cached_keys(fd_index p);

FD_EXPORT fd_index fd_lisp2index(fdtype lp);
FD_EXPORT fdtype fd_index2lisp(fd_index ix);

FD_EXPORT int fd_add_to_background(fd_index ix);

/* Network indexes */

typedef struct FD_NETWORK_INDEX {
  FD_INDEX_FIELDS;
  int sock; fdtype xname;
  int capabilities;
  struct U8_CONNPOOL *index_connpool;} FD_NETWORK_INDEX;
typedef struct FD_NETWORK_INDEX *fd_network_index;

FD_EXPORT fd_index fd_open_network_index(u8_string spec,fdkbase_flags flags);

/* Server capabilities */
#define FD_ISERVER_FETCHN 1
#define FD_ISERVER_ADDN 2
#define FD_ISERVER_DROP 4
#define FD_ISERVER_RESET 8

/* External indexes */

typedef struct FD_EXTINDEX {
  FD_INDEX_FIELDS;
  fdtype fetchfn, commitfn, state;} FD_EXTINDEX;
typedef struct FD_EXTINDEX *fd_extindex;

FD_EXPORT fd_index fd_make_extindex
  (u8_string name,fdtype fetchfn,fdtype commitfn,fdtype state,int reg);

FD_EXPORT struct FD_INDEX_HANDLER fd_extindex_handler;

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
FD_FASTOP fdtype fd_index_get(fd_index ix,fdtype key)
{
  fdtype cached;
#if FD_USE_THREADCACHE
  FDTC *fdtc=fd_threadcache; struct FD_PAIR tempkey;
  if (fdtc) {
    FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
    tempkey.fd_car=fd_index2lisp(ix); tempkey.fd_cdr=key;
    cached=fd_hashtable_get(&(fdtc->indexes),(fdtype)&tempkey,FD_VOID);
    if (!(FD_VOIDP(cached))) return cached;}
#endif
  if (ix->index_cache_level==0) cached=FD_VOID;
  else if ((FD_PAIRP(key)) && (!(FD_VOIDP(ix->index_covers_slotids))) &&
      (!(atomic_choice_containsp(FD_CAR(key),ix->index_covers_slotids))))
    return FD_EMPTY_CHOICE;
  else cached=fd_hashtable_get(&(ix->index_cache),key,FD_VOID);
  if (FD_VOIDP(cached)) cached=fd_index_fetch(ix,key);
#if FD_USE_THREADCACHE
  if (fdtc) {
    fd_hashtable_store(&(fdtc->indexes),(fdtype)&tempkey,cached);}
#endif
  return cached;
}
FD_FASTOP int fd_index_add(fd_index ix,fdtype key,fdtype value)
{
  int rv=-1;
  FDTC *fdtc=(FD_WRITETHROUGH_THREADCACHE)?(fd_threadcache):(NULL);
  fd_hashtable adds=&(ix->index_adds);
  fd_hashtable cache=&(ix->index_cache);
  if (U8_BITP(ix->index_flags,FDKB_READ_ONLY)) 
    /* This will signal an error */
    return _fd_index_add(ix,key,value);
  else if (FD_CHOICEP(key)) {
    const fdtype *keys=FD_CHOICE_DATA(key);
    unsigned int n=FD_CHOICE_SIZE(key);
    rv=fd_hashtable_iterkeys(adds,fd_table_add,n,keys,value);
    if (rv<0) return rv;
    else if (ix->index_cache_level>0)
      rv=fd_hashtable_iterkeys(cache,fd_table_add_if_present,n,keys,value);}
  else if (FD_ACHOICEP(key)) {
    fdtype normchoice=fd_make_simple_choice(key);
    const fdtype *keys=FD_CHOICE_DATA(key);
    unsigned int n=FD_CHOICE_SIZE(key);
    rv=fd_hashtable_iterkeys(adds,fd_table_add,n,keys,value);
    if (rv<0) return rv;
    else if (ix->index_cache_level>0)
      rv=fd_hashtable_iterkeys(cache,fd_table_add_if_present,n,keys,value);
    fd_decref(normchoice);}
  else {
    rv=fd_hashtable_add(adds,key,value);
    if (rv<0) return rv;
    rv=fd_hashtable_op(cache,fd_table_add_if_present,key,value);}
  if (rv<0) return rv;
  if ( (fdtc) && (fdtc->indexes.table_n_keys) ) {
    FD_DO_CHOICES(akey,key) {
      struct FD_PAIR tempkey;
      FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
      tempkey.fd_car=fd_index2lisp(ix); tempkey.fd_cdr=akey;
      if (fd_hashtable_probe(&fdtc->indexes,(fdtype)&tempkey)) {
	fd_hashtable_add(&fdtc->indexes,(fdtype)&tempkey,value);}}}

  if ((ix->index_flags&FD_INDEX_IN_BACKGROUND) && 
      (fd_background->index_cache.table_n_keys)) {
    fd_hashtable bgcache=(&(fd_background->index_cache));
    if (FD_CHOICEP(key)) {
      const fdtype *keys=FD_CHOICE_DATA(key);
      unsigned int n=FD_CHOICE_SIZE(key);
      /* This will force it to be re-read from the source indexes */
      rv=fd_hashtable_iterkeys(bgcache,fd_table_replace,n,keys,FD_VOID);}
    else rv=fd_hashtable_op(bgcache,fd_table_replace,key,FD_VOID);}

  if (rv<0) return rv;

  if ((!(FD_VOIDP(ix->index_covers_slotids))) &&
      (FD_EXPECT_TRUE(FD_PAIRP(key))) &&
      (FD_EXPECT_TRUE((FD_OIDP(FD_CAR(key))) ||
		      (FD_SYMBOLP(FD_CAR(key)))))) {
    if (!(atomic_choice_containsp(FD_CAR(key),ix->index_covers_slotids))) {
      fd_decref(ix->index_covers_slotids);
      ix->index_covers_slotids=FD_VOID;}}

  return rv;
}
FD_FASTOP U8_MAYBE_UNUSED fd_index fd_indexptr(fdtype x)
{
  if (FD_IMMEDIATEP(x)) {
    int serial=FD_GET_IMMEDIATE(x,fd_index_type);
    if (serial<0) return NULL;
    else if (serial<FD_N_PRIMARY_INDEXES)
      return fd_primary_indexes[serial];
    else if (serial<(FD_N_PRIMARY_INDEXES+fd_n_secondary_indexes))
      return fd_secondary_indexes[serial-FD_N_PRIMARY_INDEXES];
    else return NULL;}
  else if ((FD_CONSP(x))&&(FD_TYPEP(x,fd_raw_index_type)))
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
FD_EXPORT fdtype *fd_get_index_delays(void);
FD_EXPORT int fd_execute_index_delays(fd_index ix,void *data);

#endif /* FRAMERD_INDEXES_H */
