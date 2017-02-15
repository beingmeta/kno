/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_INDICES_H
#define FRAMERD_INDICES_H 1
#ifndef FRAMERD_INDICES_H_INFO
#define FRAMERD_INDICES_H_INFO "include/framerd/indices.h"
#endif

#include "fddb.h"
#include "dtypestream.h"

FD_EXPORT fd_exception
  fd_FileIndexOverflow, fd_NotAFileIndex, fd_NoFileIndices, fd_BadIndexSpec,
  fd_IndexCommitError;

FD_EXPORT u8_condition fd_IndexCommit;

#define FD_INDEX_CACHE_INIT 73
#define FD_INDEX_EDITS_INIT 73
#define FD_INDEX_ADDS_INIT 123
FD_EXPORT int fd_index_cache_init;
FD_EXPORT int fd_index_edits_init;
FD_EXPORT int fd_index_adds_init;

#define FD_INDEX_ADD_CAPABILITY  (FDB_INDEX_FLAG(1))
#define FD_INDEX_DROP_CAPABILITY (FDB_INDEX_FLAG(2))
#define FD_INDEX_SET_CAPABILITY  (FDB_INDEX_FLAG(3))
#define FD_INDEX_IN_BACKGROUND   (FDB_INDEX_FLAG(4))
#define FD_INDEX_NOSWAP          (FDB_INDEX_FLAG(5))

#define FD_N_PRIMARY_INDICES 128

#define FD_INDEX_FIELDS \
  FD_CONS_HEADER;						   \
  int index_serialno, index_read_only;				   \
  int index_cache_level, index_flags;				   \
  u8_string index_source, index_cid, index_xid;                    \
  struct FD_INDEX_HANDLER *index_handler;			   \
  struct FD_HASHTABLE index_cache, index_adds, index_edits;        \
  fdtype index_has_slotids

typedef struct FD_INDEX {FD_INDEX_FIELDS;} FD_INDEX;
typedef struct FD_INDEX *fd_index;

FD_EXPORT fd_index fd_primary_indices[], *fd_secondary_indices;
FD_EXPORT int fd_n_primary_indices, fd_n_secondary_indices;

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
  int (*sync)(fd_index p);} FD_INDEX_HANDLER;
typedef struct FD_INDEX_HANDLER *fd_index_handler;

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
    };
#endif

FD_EXPORT int fd_for_indices(int (*fcn)(fd_index,void *),void *data);

FD_EXPORT void fd_init_index
  (fd_index ix,struct FD_INDEX_HANDLER *h,u8_string source,int consed);
FD_EXPORT void fd_reset_index_tables(fd_index ix,ssize_t csize,ssize_t esize,ssize_t asize);

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

FD_EXPORT fd_index fd_open_index(u8_string);
FD_EXPORT fd_index fd_open_index_x(u8_string,int);
FD_EXPORT fd_index fd_find_index_by_cid(u8_string);

FD_EXPORT void fd_index_swapout(fd_index ix);
FD_EXPORT void fd_index_setcache(fd_index ix,int level);

FD_EXPORT fd_index fd_use_index(u8_string spec);

FD_EXPORT void fd_swapout_indices(void);
FD_EXPORT void fd_close_indices(void);
FD_EXPORT int fd_commit_indices(void);
FD_EXPORT int fd_commit_indices_noerr(void);
FD_EXPORT long fd_index_cache_load(void);
FD_EXPORT fdtype fd_cached_keys(fd_index p);

FD_EXPORT fd_index fd_lisp2index(fdtype lp);
FD_EXPORT fdtype fd_index2lisp(fd_index ix);

FD_EXPORT int fd_add_to_background(fd_index ix);

/* Network indices */

typedef struct FD_NETWORK_INDEX {
  FD_INDEX_FIELDS;
  int sock; fdtype xname;
  int capabilities;
  struct U8_CONNPOOL *fd_connpool;} FD_NETWORK_INDEX;
typedef struct FD_NETWORK_INDEX *fd_network_index;

FD_EXPORT fd_index fd_open_network_index
  (u8_string spec,u8_string source,fdtype xname);
FD_EXPORT fd_index fd_open_network_index_x
  (u8_string spec,u8_string source,fdtype xname,int consed);

/* Server capabilities */
#define FD_ISERVER_FETCHN 1
#define FD_ISERVER_ADDN 2
#define FD_ISERVER_DROP 4
#define FD_ISERVER_RESET 8

/* MEM indices */

typedef struct FD_MEM_INDEX {
  FD_INDEX_FIELDS;
  int (*commitfn)(struct FD_MEM_INDEX *,u8_string);} FD_MEM_INDEX;
typedef struct FD_MEM_INDEX *fd_mem_index;

FD_EXPORT fd_index fd_make_mem_index(int consed);

/* EXTernal indices */

typedef struct FD_EXTINDEX {
  FD_INDEX_FIELDS;
  fdtype fetchfn, commitfn, state;} FD_EXTINDEX;
typedef struct FD_EXTINDEX *fd_extindex;

FD_EXPORT fd_index fd_make_extindex
  (u8_string name,fdtype fetchfn,fdtype commitfn,fdtype state,int reg);

FD_EXPORT struct FD_INDEX_HANDLER fd_extindex_handler;

/* Compound indices */

typedef struct FD_COMPOUND_INDEX {
  FD_INDEX_FIELDS;
  unsigned int n_indices; fd_index *indices;
  U8_MUTEX_DECL(fd_lock);} FD_COMPOUND_INDEX;
typedef struct FD_COMPOUND_INDEX *fd_compound_index;

FD_EXPORT int fd_add_to_compound_index(fd_compound_index ix,fd_index add);
FD_EXPORT fd_index fd_make_compound_index(int n_indices,fd_index *indices);

FD_EXPORT struct FD_COMPOUND_INDEX *fd_background;

/* Inline index adds */

#if FD_INLINE_INDICES
FD_FASTOP fdtype fd_index_get(fd_index ix,fdtype key)
{
  fdtype cached;
#if FD_USE_THREADCACHE
  FDTC *fdtc=fd_threadcache; struct FD_PAIR tempkey;
  if (fdtc) {
    FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
    tempkey.fd_car=fd_index2lisp(ix); tempkey.fd_cdr=key;
    cached=fd_hashtable_get(&(fdtc->indices),(fdtype)&tempkey,FD_VOID);
    if (!(FD_VOIDP(cached))) return cached;}
#endif
  if (ix->index_cache_level==0) cached=FD_VOID;
  else if ((FD_PAIRP(key)) && (!(FD_VOIDP(ix->index_has_slotids))) &&
      (!(atomic_choice_containsp(FD_CAR(key),ix->index_has_slotids))))
    return FD_EMPTY_CHOICE;
  else cached=fd_hashtable_get(&(ix->index_cache),key,FD_VOID);
  if (FD_VOIDP(cached)) cached=fd_index_fetch(ix,key);
#if FD_USE_THREADCACHE
  if (fdtc) {
    fd_hashtable_store(&(fdtc->indices),(fdtype)&tempkey,cached);}
#endif
  return cached;
}
FD_FASTOP int fd_index_add(fd_index ix,fdtype key,fdtype value)
{
  int rv=-1;
  FDTC *fdtc=(FD_WRITETHROUGH_THREADCACHE)?(fd_threadcache):(NULL);
  fd_hashtable adds=&(ix->index_adds);
  fd_hashtable cache=&(ix->index_cache);
  if (ix->index_read_only)
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
  if ( (fdtc) && (fdtc->indices.table_n_keys) ) {
    FD_DO_CHOICES(akey,key) {
      struct FD_PAIR tempkey;
      FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
      tempkey.fd_car=fd_index2lisp(ix); tempkey.fd_cdr=akey;
      if (fd_hashtable_probe(&fdtc->indices,(fdtype)&tempkey)) {
	fd_hashtable_add(&fdtc->indices,(fdtype)&tempkey,value);}}}

  if ((ix->index_flags&FD_INDEX_IN_BACKGROUND) && 
      (fd_background->index_cache.table_n_keys)) {
    fd_hashtable bgcache=(&(fd_background->index_cache));
    if (FD_CHOICEP(key)) {
      const fdtype *keys=FD_CHOICE_DATA(key);
      unsigned int n=FD_CHOICE_SIZE(key);
      /* This will force it to be re-read from the source indices */
      rv=fd_hashtable_iterkeys(bgcache,fd_table_replace,n,keys,FD_VOID);}
    else rv=fd_hashtable_op(bgcache,fd_table_replace,key,FD_VOID);}

  if (rv<0) return rv;

  if ((!(FD_VOIDP(ix->index_has_slotids))) &&
      (FD_EXPECT_TRUE(FD_PAIRP(key))) &&
      (FD_EXPECT_TRUE((FD_OIDP(FD_CAR(key))) ||
		      (FD_SYMBOLP(FD_CAR(key)))))) {
    if (!(atomic_choice_containsp(FD_CAR(key),ix->index_has_slotids))) {
      fd_decref(ix->index_has_slotids);
      ix->index_has_slotids=FD_VOID;}}

  return rv;
}
FD_FASTOP U8_MAYBE_UNUSED fd_index fd_indexptr(fdtype x)
{
  if (FD_IMMEDIATEP(x)) {
    int serial=FD_GET_IMMEDIATE(x,fd_index_type);
    if (serial<0) return NULL;
    else if (serial<FD_N_PRIMARY_INDICES)
      return fd_primary_indices[serial];
    else if (serial<(FD_N_PRIMARY_INDICES+fd_n_secondary_indices))
      return fd_secondary_indices[serial-FD_N_PRIMARY_INDICES];
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

/* Opening file indices */

FD_EXPORT fd_index (*fd_file_index_opener)(u8_string,int);

/* IPEVAL delays */

#ifndef FD_N_INDEX_DELAYS
#define FD_N_INDEX_DELAYS 1024
#endif

FD_EXPORT void fd_init_index_delays(void);
FD_EXPORT fdtype *fd_get_index_delays(void);
FD_EXPORT int fd_execute_index_delays(fd_index ix,void *data);

#endif /* FRAMERD_INDICES_H */
