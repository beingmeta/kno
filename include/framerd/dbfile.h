/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_DBFILE_H
#define FRAMERD_DBFILE_H 1
#ifndef FRAMERD_DBFILE_H_INFO
#define FRAMERD_DBFILE_H_INFO "include/framerd/dbfile.h"
#endif

#include "fddb.h"
#include "pools.h"
#include "indices.h"

#ifndef FD_FILEDB_BUFSIZE
#define FD_FILEDB_BUFSIZE 256*1024
#endif

FD_EXPORT size_t fd_filedb_bufsize;

FD_EXPORT int fd_acid_files;

FD_EXPORT int fd_init_dbfile(void) FD_LIBINIT_FN;

typedef enum FD_OFFSET_TYPE { FD_B32=0, FD_B40=1, FD_B64=2 } fd_offset_type;
typedef enum FD_COMPRESSION_TYPE { FD_NOCOMPRESS=0, FD_ZLIB=1, FD_BZ2=2 }
  fd_compression_type;

FD_EXPORT fd_exception fd_FileIndexError;
FD_EXPORT fd_exception fd_FileSizeOverflow;
FD_EXPORT fd_exception fd_CorruptedPool;
FD_EXPORT fd_exception fd_BadMetaData, fd_FutureMetaData;
FD_EXPORT fd_exception fd_MMAPError, fd_MUNMAPError;
FD_EXPORT fd_exception fd_RecoveryRequired;

#define FD_FILE_POOL_MAGIC_NUMBER 67179521
#define FD_FILE_POOL_TO_RECOVER 67179523

#define FD_ZPOOL_MAGIC_NUMBER 436491265

#define FD_OIDPOOL_MAGIC_NUMBER ((0x17<<24)|(0x11<<16)|(0x04<<8)|(0x10))
#define FD_OIDPOOL_TO_RECOVER ((0x11<<24)|(0x09<<16)|(0x04<<8)|(0x12))

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
  unsigned int fdp_load, *fd_offsets, fd_offsets_size;
  struct FD_DTYPE_STREAM fd_stream;
  U8_MUTEX_DECL(fd_lock);} FD_FILE_POOL;
typedef struct FD_FILE_POOL *fd_file_pool;

#define FD_FILE_POOL_LOCKED(fp) (((fp)->fd_stream.fd_dts_flags)&FD_DTSTREAM_LOCKED)

typedef struct FD_SCHEMA_TABLE {
  int schema_index; int size; fdtype *schema;} FD_SCHEMA_TABLE;
typedef struct FD_SCHEMA_TABLE *fd_schema_table;

typedef struct FD_ZPOOL {
  FD_POOL_FIELDS;
  time_t modtime;
  unsigned int fdp_load, *fd_offsets, fd_offsets_size;
  struct FD_DTYPE_STREAM fd_stream;
  int fdp_n_schemas;
  struct FD_SCHEMA_TABLE *fdp_schemas, *fdp_fdp_schemas_byptr;
  struct FD_SCHEMA_TABLE *fdp_schemas_byval;
  U8_MUTEX_DECL(fd_lock);} FD_ZPOOL;
typedef struct FD_ZPOOL *fd_zpool;

/* OID Pools */

#define FD_OIDPOOL_OFFMODE 0x07
#define FD_OIDPOOL_COMPRESSION 0x78
#define FD_OIDPOOL_READONLY 0x80
#define FD_OIDPOOL_DTYPEV2 0x100

#define FD_OIDPOOL_LOCKED(x) (FD_FILE_POOL_LOCKED(x))

typedef struct FD_SCHEMA_ENTRY {
  int n_slotids, id; fdtype *slotids, normal;
  unsigned int *mapin, *mapout;}  FD_SCHEMA_ENTRY;
typedef struct FD_SCHEMA_ENTRY *fd_schema_entry;

typedef struct FD_SCHEMA_LOOKUP {
  int id, n_slotids; fdtype *slotids;}  FD_SCHEMA_LOOKUP;
typedef struct FD_SCHEMA_LOOKUOP *fd_schema_lookup;

typedef struct FD_OIDPOOL {
  FD_POOL_FIELDS;
  unsigned int dbflags;
  fd_offset_type fdp_offtype;
  fd_compression_type compression;
  time_t modtime;
  int fdp_n_schemas, fdp_max_slotids;
  struct FD_SCHEMA_ENTRY *fdp_schemas;
  struct FD_SCHEMA_LOOKUP *fdp_schbyval;
  unsigned int fdp_load, *fd_offsets, fd_offsets_size;
  struct FD_DTYPE_STREAM fd_stream;
  size_t fd_mmap_size; unsigned char *fd_mmap;
  U8_MUTEX_DECL(fd_lock);} FD_OIDPOOL;
typedef struct FD_OIDPOOL *fd_oidpool;

FD_EXPORT fdtype fd_read_pool_metadata(struct FD_DTYPE_STREAM *ds);
FD_EXPORT fdtype fd_write_pool_metadata(fd_dtype_stream,fdtype);
FD_EXPORT int fd_make_file_pool(u8_string,unsigned int,
                                FD_OID,unsigned int,unsigned int,
                                fdtype);

FD_EXPORT fd_pool fd_unregistered_file_pool(u8_string filename);


FD_EXPORT int fd_make_oidpool
  (u8_string fname,u8_string label,
   FD_OID base,unsigned int capacity,unsigned int load,
   unsigned int flags,fdtype schemas_init,fdtype metadata_init,
   time_t ctime,time_t mtime,int cycles);

FD_EXPORT fd_exception fd_InvalidSchemaDef;
FD_EXPORT fd_exception fd_InvalidSchemaRef;
FD_EXPORT fd_exception fd_SchemaInconsistency;

/* File indices */

FD_EXPORT void fd_register_index_opener
  (unsigned int id,
   fd_index (*opener)(u8_string filename,int read_only,int consed),
   fdtype (*mdreader)(FD_DTYPE_STREAM *),
   fdtype (*mdwriter)(FD_DTYPE_STREAM *,fdtype));

typedef struct FD_INDEX_OPENER {
  unsigned int initial_word;
  fd_index (*opener)(u8_string filename,int read_only,int consed);
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
#define FD_HASH_INDEX_TO_RECOVER 0x8011328

FD_EXPORT int fd_file_indexp(u8_string filename);

typedef struct FD_FILE_INDEX {
  FD_INDEX_FIELDS;
  unsigned int fd_n_slots, fd_hashv, *fd_offsets;
  struct FD_DTYPE_STREAM fd_stream;
  fdtype slotids;
  U8_MUTEX_DECL(fd_lock);} FD_FILE_INDEX;
typedef struct FD_FILE_INDEX *fd_file_index;

typedef struct FD_ZINDEX {
  FD_INDEX_FIELDS;
  unsigned int fd_n_slots, fd_hashv, *fd_offsets;
  struct FD_DTYPE_STREAM fd_stream;
  fdtype slotids; FD_OID *fdx_baseoids; int fdx_n_baseoids;
  U8_MUTEX_DECL(fd_lock);} FD_ZINDEX;
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

#define FD_HASH_INDEX_FN_MASK 0x0F
#define FD_HASH_OFFTYPE_MASK  0x30
#define FD_HASH_INDEX_DTYPEV2 0x40
/* This determines if the hash index only has pair keys
   whose cars are in its slotids.  */
#define FD_HASH_INDEX_ODDKEYS (FD_HASH_INDEX_DTYPEV2<<1)

/* Full sized chunk refs, usually passed and returned but not
   directly stored on disk. */
typedef struct FD_CHUNK_REF {
  fd_off_t off; size_t size;} FD_CHUNK_REF;
typedef struct FD_CHUNK_REF *fd_chunk_ref;

typedef struct FD_SLOTID_LOOKUP {
  int zindex; fdtype slotid;} FD_SLOTID_LOOKUP;
typedef struct FD_SLOTID_LOOKUP *fd_slotid_lookup;

typedef struct FD_BASEOID_LOOKUP {
  unsigned int zindex; FD_OID baseoid;} FD_BASEOID_LOOKUP;
typedef struct FD_BASEOID_LOOKUP *fd_baseoid_lookup;

typedef struct FD_HASH_INDEX {
  FD_INDEX_FIELDS;
  U8_MUTEX_DECL(fd_lock);

  /* flags controls hash functions, compression, etc.
     hxcustom is a placeholder for a value to customize
     the hash function. */
  unsigned int fdx_flags, fdx_custom, fd_n_keys;
  fd_offset_type fdx_offtype;

  /* This is used to store compressed keys and values. */
  int fdx_n_slotids, fdx_new_slotids; fdtype *fdx_slotids;
  struct FD_SLOTID_LOOKUP *slotid_lookup;
  int fdx_n_baseoids, fdx_new_baseoids;
  unsigned int *fdx_baseoid_ids;
  short *fdx_ids2baseoids;

  /* Pointers into keyblocks for the hashtable */
  unsigned int *fdx_offdata; int fdx_n_buckets;

  /* The stream accessing the file.  This is only used
     for modification if the file is memmaped. */
  struct FD_DTYPE_STREAM fd_stream;
  /* When non-null, a memmapped pointer to the file contents. */
  size_t fd_mmap_size; unsigned char *fd_mmap;} FD_HASH_INDEX;
typedef struct FD_HASH_INDEX *fd_hash_index;

FD_EXPORT int fd_populate_hash_index
  (struct FD_HASH_INDEX *hx,fdtype from,
   const fdtype *keys,int n_keys, int blocksize);
FD_EXPORT int fd_make_hash_index
  (u8_string,int,unsigned int,unsigned int,
   fdtype,fdtype,fdtype,time_t,time_t);
FD_EXPORT int fd_hash_index_bucket
   (struct FD_HASH_INDEX *hx,fdtype key,int modulate);
FD_EXPORT int fd_hash_indexp(struct FD_INDEX *ix);
FD_EXPORT fdtype fd_hash_index_stats(struct FD_HASH_INDEX *ix);

#endif /* #ifndef FRAMERD_DBFILE_H */
