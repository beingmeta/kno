/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_INDICES_H
#define FDB_INDICES_H 1
#define FDB_INDICES_H_VERSION "$Id$"

#include "fddb.h"
#include "dtypestream.h"

FD_EXPORT fd_exception
  fd_FileIndexOverflow, fd_NotAFileIndex, fd_NoFileIndices, fd_BadIndexSpec;

#define FD_INDEX_ADD_CAPABILITY 1
#define FD_INDEX_DROP_CAPABILITY 2
#define FD_INDEX_SET_CAPABILITY 4
#define FD_INDEX_IN_BACKGROUND 8
#define FD_INDEX_NOSWAP 16

#define FD_N_PRIMARY_INDICES 128

#define FD_INDEX_FIELDS \
  int serialno, read_only, cache_level, flags;   \
  u8_string source, cid, xid;                    \
  struct FD_INDEX_HANDLER *handler;              \
  struct FD_HASHTABLE cache, adds, edits

typedef struct FD_INDEX {FD_INDEX_FIELDS;} FD_INDEX;
typedef struct FD_INDEX *fd_index;

FD_EXPORT fd_index fd_primary_indices[], *fd_secondary_indices;
FD_EXPORT int fd_n_primary_indices, fd_n_secondary_indices;

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
  fdtype (*fetchkeys)(fd_index ix);
  fdtype (*fetchsizes)(fd_index ix);
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

FD_EXPORT int fd_index_store(fd_index ix,fdtype key,fdtype value);
FD_EXPORT int fd_index_drop(fd_index ix,fdtype key,fdtype value);
FD_EXPORT int fd_index_commit(fd_index ix);
FD_EXPORT fdtype _fd_index_get(fd_index ix,fdtype key);
FD_EXPORT fdtype fd_index_fetch(fd_index ix,fdtype key);
FD_EXPORT fdtype fd_index_keys(fd_index ix);
FD_EXPORT fdtype fd_index_sizes(fd_index ix);
FD_EXPORT int _fd_index_add(fd_index ix,fdtype key,fdtype value);
FD_EXPORT int fd_index_prefetch(fd_index ix,fdtype keys);

FD_EXPORT fd_index fd_open_index(u8_string);
FD_EXPORT fd_index fd_find_index_by_cid(u8_string);

FD_EXPORT void fd_index_swapout(fd_index ix);
FD_EXPORT void fd_index_setcache(fd_index ix,int level);

FD_EXPORT fd_index fd_use_index(u8_string spec);

FD_EXPORT void fd_swapout_indices(void);
FD_EXPORT void fd_close_indices(void);
FD_EXPORT int fd_commit_indices(void);
FD_EXPORT int fd_commit_indices_noerr(void);
FD_EXPORT int fd_index_cache_load(void);
FD_EXPORT fdtype fd_cached_keys(fd_index p);

FD_EXPORT fd_index fd_lisp2index(fdtype lp);
FD_EXPORT fdtype fd_index2lisp(fd_index ix);

/* Network indices */

typedef struct FD_NETWORK_INDEX {
  FD_INDEX_FIELDS;
  int sock; fdtype xname;
  int capabilities;
  struct FD_DTYPE_STREAM stream;
  U8_MUTEX_DECL(lock);} FD_NETWORK_INDEX;
typedef struct FD_NETWORK_INDEX *fd_network_index;

FD_EXPORT fd_index fd_open_network_index(u8_string spec,fdtype xname);

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

FD_EXPORT fd_index fd_make_mem_index(void);

/* Compound indices */

typedef struct FD_COMPOUND_INDEX {
  FD_INDEX_FIELDS;
  unsigned int n_indices; fd_index *indices;
  U8_MUTEX_DECL(lock);} FD_COMPOUND_INDEX;
typedef struct FD_COMPOUND_INDEX *fd_compound_index;

FD_EXPORT int fd_add_to_compound_index(fd_compound_index ix,fd_index add);
FD_EXPORT fd_index fd_make_compound_index(int n_indices,fd_index *indices);

FD_EXPORT struct FD_COMPOUND_INDEX *fd_background;

/* Inline index adds */

#if FD_INLINE_INDICES
FD_FASTOP fdtype fd_index_get(fd_index ix,fdtype key)
{
  fdtype cached=fd_hashtable_get(&(ix->cache),key,FD_VOID);
  if (FD_VOIDP(cached)) return fd_index_fetch(ix,key);
  else return cached;
}
FD_FASTOP int fd_index_add(fd_index ix,fdtype key,fdtype value)
{
  if (ix->read_only)
    /* This will signal an error */
    return _fd_index_add(ix,key,value);
  else if (ix->cache.n_slots==0)
    if (FD_CHOICEP(key))
      return _fd_index_add(ix,key,value);
    else {
      int retval=fd_hashtable_add(&(ix->adds),key,value);
      if (retval<0) return retval;
      if ((ix->flags&FD_INDEX_IN_BACKGROUND) &&
	  (fd_background->cache.n_keys))
	retval=fd_hashtable_op(&(fd_background->cache),fd_table_replace,key,FD_VOID);
      return retval;}
  else return _fd_index_add(ix,key,value);
}
#else
#define fd_index_get(ix,key) _fd_index_get(ix,key) 
#define fd_index_add(ix,key,val) _fd_index_add(ix,key,val) 
#endif

/* Opening file indices */

FD_EXPORT fd_index (*fd_file_index_opener)(u8_string);

/* IPEVAL delays */

#ifndef FD_N_INDEX_DELAYS
#define FD_N_INDEX_DELAYS 1024
#endif

FD_EXPORT void fd_init_index_delays(void);
FD_EXPORT fdtype *fd_get_index_delays(void);
FD_EXPORT int fd_execute_index_delays(fd_index ix,void *data);

#endif /* FDB_INDICES_H */
