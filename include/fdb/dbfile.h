/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_DBFILE_H
#define FDB_DBFILE_H 1
#define FDB_DBFILE_H_VERSION "$Id$"

#include "fddb.h"
#include "pools.h"
#include "indices.h"

#ifndef FD_FILEDB_BUFSIZE
#define FD_FILEDB_BUFSIZE 256*1024
#endif

FD_EXPORT int fd_init_dbfile(void) FD_LIBINIT_FN;

FD_EXPORT fd_exception fd_FileIndexError;
FD_EXPORT fd_exception fd_FileSizeOverflow;
FD_EXPORT fd_exception fd_CorruptedPool;
FD_EXPORT fd_exception fd_BadMetaData, fd_FutureMetaData;
FD_EXPORT fd_exception fd_MMAPError, fd_MUNMAPError;
FD_EXPORT fd_exception fd_RecoveryRequired;

#define FD_FILE_POOL_MAGIC_NUMBER 67179521
#define FD_FILE_POOL_TO_RECOVER 67179523
#define FD_ZPOOL_MAGIC_NUMBER 436491265

/* File Pools */

typedef struct FD_POOL_OPENER {
  unsigned int initial_word;
  fd_pool (*opener)(u8_string filename,int read_only);
  fdtype (*read_metadata)(FD_DTYPE_STREAM *);
  fdtype (*write_metadata)(FD_DTYPE_STREAM *,fdtype);} FD_POOL_OPENER;
typedef struct FD_POOL_OPENER *fd_pool_opener;

typedef struct FD_FILE_POOL {
  FD_POOL_FIELDS;
  time_t modtime;
  unsigned int load, *offsets, offsets_size;
  struct FD_DTYPE_STREAM stream;
  U8_MUTEX_DECL(lock);} FD_FILE_POOL;
typedef struct FD_FILE_POOL *fd_file_pool;

#define FD_FILE_POOL_LOCKED(fp) (((fp)->stream.flags)&FD_DTSTREAM_LOCKED)

typedef struct FD_SCHEMA_TABLE {
  int schema_index; int size; fdtype *schema;} FD_SCHEMA_TABLE;
typedef struct FD_SCHEMA_TABLE *fd_schema_table;

typedef struct FD_ZPOOL {
  FD_POOL_FIELDS;
  time_t modtime;
  unsigned int load, *offsets, offsets_size;
  struct FD_DTYPE_STREAM stream;
  int n_schemas;
  struct FD_SCHEMA_TABLE *schemas, *schemas_byptr;
  struct FD_SCHEMA_TABLE *schemas_byval;
  U8_MUTEX_DECL(lock);} FD_ZPOOL;
typedef struct FD_ZPOOL *fd_zpool;

FD_EXPORT fdtype fd_read_pool_metadata(struct FD_DTYPE_STREAM *ds);
FD_EXPORT fdtype fd_write_pool_metadata(fd_dtype_stream,fdtype);
FD_EXPORT int fd_make_file_pool(u8_string,unsigned int,
				FD_OID,unsigned int,unsigned int,
				fdtype);

/* File indices */

typedef struct FD_INDEX_OPENER {
  unsigned int initial_word;
  fd_index (*opener)(u8_string filename,int read_only);
  fdtype (*read_metadata)(FD_DTYPE_STREAM *);
  fdtype (*write_metadata)(FD_DTYPE_STREAM *,fdtype);} FD_INDEX_OPENER;
typedef struct FD_INDEX_OPENER *fd_index_opener;

#define FD_FILE_INDEX_MAGIC_NUMBER 0x90e0418
#define FD_MULT_FILE_INDEX_MAGIC_NUMBER 0x90e0419
#define FD_MULT_FILE3_INDEX_MAGIC_NUMBER 0x90e041a
#define FD_FILE_INDEX_TO_RECOVER 0x90e0438
#define FD_MULT_FILE_INDEX_TO_RECOVER 0x90e0439
#define FD_MULT_FILE3_INDEX_TO_RECOVER 0x90e043a
#define FD_ZINDEX_MAGIC_NUMBER 437126168
#define FD_ZINDEX3_MAGIC_NUMBER 437126169

#define FD_HASH_INDEX_MAGIC_NUMBER 0x8011308
#define FD_HASH_INDEX_TO_RECOVER 8011328

typedef struct FD_FILE_INDEX {
  FD_INDEX_FIELDS;
  unsigned int n_slots, hashv, *offsets;
  struct FD_DTYPE_STREAM stream;
  fdtype slotids;
  U8_MUTEX_DECL(lock);} FD_FILE_INDEX;
typedef struct FD_FILE_INDEX *fd_file_index;

typedef struct FD_ZINDEX {
  FD_INDEX_FIELDS;
  unsigned int n_slots, hashv, *offsets;
  struct FD_DTYPE_STREAM stream;
  fdtype slotids; FD_OID *baseoids; int n_baseoids;
  U8_MUTEX_DECL(lock);} FD_ZINDEX;
typedef struct FD_ZINDEX *fd_zindex;

FD_EXPORT fdtype fd_read_index_metadata(struct FD_DTYPE_STREAM *ds);
FD_EXPORT fdtype fd_write_index_metadata(fd_dtype_stream,fdtype);
FD_EXPORT int fd_make_file_index(u8_string,unsigned int,int,fdtype metadata);

unsigned int fd_hash_dtype1(fdtype x);
unsigned int fd_hash_dtype2(fdtype x);
unsigned int fd_hash_dtype3(fdtype x);
unsigned int fd_hash_dtype_rep(fdtype x);

/* Hash indices */

#define MYSTERIOUS_MODULUS 2000239099 /* 256000001 */

#ifndef FD_USE_OFF_T
#define FD_USE_OFF_T 0
#endif

typedef unsigned int fd_off_t;
typedef unsigned int fd_size_t;
#if FD_USE_OFF_T
typedef off_t fd_off_t;
typedef size_t fd_size_t;
#endif

typedef struct FD_BLOCK_REF {
  fd_off_t off; fd_size_t size;} FD_BLOCK_REF;
typedef struct FD_BLOCK_REF *fd_block_ref;

typedef struct FD_SLOTID_LOOKUP {
  int zindex; fdtype slotid;} FD_SLOTID_LOOKUP;
typedef struct FD_SLOTID_LOOKUP *fd_slotid_lookup;

typedef struct FD_BASEOID_LOOKUP {
  unsigned int zindex; FD_OID baseoid;} FD_BASEOID_LOOKUP;
typedef struct FD_BASEOID_LOOKUP *fd_baseoid_lookup;

typedef struct FD_HASH_INDEX {
  FD_INDEX_FIELDS;
  U8_MUTEX_DECL(lock);
  
  /* flags controls hash functions, compression, etc.
     hxcustom is a placeholder for a value to customize
     the hash function. */
  unsigned int hxflags, hxcustom;
  
  /* This is used to store compressed keys and values. */
  int n_slotids; fdtype *slotids;
  struct FD_SLOTID_LOOKUP *slotid_lookup;
  int n_baseoids;
  unsigned int *baseoid_ids;
  short *ids2baseoids;
  
  /* Pointers into keyblocks for the hashtable */
  struct FD_BLOCK_REF *buckets; int n_buckets;
  
  /* The stream accessing the file.  This is only used
     for modification if the file is memmaped. */
  struct FD_DTYPE_STREAM stream;
  /* When non-null, a memmapped pointer to the file contents. */
  unsigned char *mmap;} FD_HASH_INDEX;
typedef struct FD_HASH_INDEX *fd_hash_index;

FD_EXPORT int fd_populate_hash_index(struct FD_HASH_INDEX *hx,fdtype from,fdtype keys,int blocksize);
FD_EXPORT int fd_make_hash_index(u8_string,int,fdtype,fdtype,fdtype,time_t,time_t);
FD_EXPORT int fd_hash_index_bucket(struct FD_HASH_INDEX *hx,fdtype key,int modulate);
FD_EXPORT int fd_hash_indexp(struct FD_INDEX *ix);


#endif /* #ifndef FDB_DBFILE_H */

