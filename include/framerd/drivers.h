/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_DBDRIVER_H
#define FRAMERD_DBDRIVER_H 1
#ifndef FRAMERD_DBDRIVER_H_INFO
#define FRAMERD_DBDRIVER_H_INFO "include/framerd/drivers.h"
#endif

#ifndef FD_DRIVER_BUFSIZE
#define FD_DRIVER_BUFSIZE 256*1024
#endif

FD_EXPORT size_t fd_driver_bufsize;

FD_EXPORT int fd_acid_files;

FD_EXPORT int fd_init_dbs(void) FD_LIBINIT_FN;

typedef enum FD_OFFSET_TYPE { FD_B32=0, FD_B40=1, FD_B64=2 } fd_offset_type;
typedef enum FD_COMPRESSION_TYPE { FD_NOCOMPRESS=0, FD_ZLIB=1, FD_BZ2=2 }
  fd_compression_type;

FD_EXPORT fd_exception fd_FileIndexError;
FD_EXPORT fd_exception fd_FileSizeOverflow;
FD_EXPORT fd_exception fd_CorruptedPool;
FD_EXPORT fd_exception fd_InvalidOffsetType;
FD_EXPORT fd_exception fd_BadMetaData, fd_FutureMetaData;
FD_EXPORT fd_exception fd_MMAPError, fd_MUNMAPError;
FD_EXPORT fd_exception fd_RecoveryRequired;

#define FD_FILE_POOL_MAGIC_NUMBER 67179521
#define FD_FILE_POOL_TO_RECOVER 67179523

#define FD_OIDPOOL_MAGIC_NUMBER ((0x17<<24)|(0x11<<16)|(0x04<<8)|(0x10))
#define FD_OIDPOOL_TO_RECOVER ((0x11<<24)|(0x09<<16)|(0x04<<8)|(0x12))

FD_EXPORT int fd_match4bytes(u8_string file,void *data);
FD_EXPORT int fd_netspecp(u8_string file,void *data);

/* File Pools */

typedef struct FD_POOL_OPENER {
  fd_pool_handler handler;
  fd_pool (*opener)(u8_string filename,fddb_flags flags);
  int (*matcher)(u8_string filename,void *);
  void *matcher_data;} FD_POOL_OPENER;
typedef struct FD_POOL_OPENER *fd_pool_opener;

FD_EXPORT void fd_register_pool_opener
 (fd_pool_handler pool_handler,
  fd_pool (*opener)(u8_string path,fddb_flags flags),
  int (*matcher)(u8_string path,void *),
  void *matcher_data);

typedef struct FD_FILE_POOL {
  FD_POOL_FIELDS;
  time_t pool_modtime;
  unsigned int pool_load, *pool_offsets, pool_offsets_size;
  struct FD_DTYPE_STREAM pool_stream;
  U8_MUTEX_DECL(file_lock);} FD_FILE_POOL;
typedef struct FD_FILE_POOL *fd_file_pool;

FD_EXPORT int fd_make_file_pool(u8_string,unsigned int,
                                FD_OID,unsigned int,unsigned int);

#define FD_POOLFILE_LOCKEDP(fp) \
  (((fp)->pool_stream.bs_flags)&FD_DTSTREAM_LOCKED)
#define FD_LOCK_POOLFILE(fp) fd_dts_lockfile(&((fp)->pool_stream))
#define FD_UNLOCK_POOLFILE(fp) fd_dts_unlockfile(&((fp)->pool_stream))


typedef struct FD_SCHEMA_TABLE {
  int fdst_index;
  int fdst_nslots;
  fdtype *fdst_schema;} FD_SCHEMA_TABLE;
typedef struct FD_SCHEMA_TABLE *fd_schema_table;

/* OID Pools */

#define FD_OIDPOOL_OFFMODE     0x07
#define FD_OIDPOOL_COMPRESSION 0x78
#define FD_OIDPOOL_READ_ONLY   0x80
#define FD_OIDPOOL_DTYPEV2     0x100

#define FD_OIDPOOL_LOCKED(x) (FD_POOLFILE_LOCKEDP(x))

typedef struct FD_SCHEMA_ENTRY {
  int fd_nslots, fd_schema_id;
  fdtype *fd_slotids, normal;
  unsigned int *fd_slotmapin;
  unsigned int *fd_slotmapout;}  FD_SCHEMA_ENTRY;
typedef struct FD_SCHEMA_ENTRY *fd_schema_entry;

typedef struct FD_SCHEMA_LOOKUP {
  int fd_schema_id, fd_nslots;
  fdtype *fd_slotids;}  FD_SCHEMA_LOOKUP;
typedef struct FD_SCHEMA_LOOKUOP *fd_schema_lookup;

typedef struct FD_OIDPOOL {
  FD_POOL_FIELDS;
  unsigned int pool_xformat;
  fd_offset_type pool_offtype;
  fd_compression_type pool_compression;
  time_t pool_modtime;
  int pool_n_schemas, pool_max_slotids;
  struct FD_SCHEMA_ENTRY *pool_schemas;
  struct FD_SCHEMA_LOOKUP *pool_schbyval;
  unsigned int pool_load, *pool_offsets, pool_offsets_size;
  struct FD_DTYPE_STREAM pool_stream;
  size_t pool_mmap_size; unsigned char *pool_mmap;
  U8_MUTEX_DECL(file_lock);} FD_OIDPOOL;
typedef struct FD_OIDPOOL *fd_oidpool;

FD_EXPORT fd_pool fd_unregistered_file_pool(u8_string filename);

FD_EXPORT int fd_make_oidpool
  (u8_string fname,u8_string label,
   FD_OID base,unsigned int capacity,unsigned int load,
   unsigned int flags,fdtype schemas_init,
   time_t ctime,time_t mtime,int cycles);

FD_EXPORT fd_exception fd_InvalidSchemaDef;
FD_EXPORT fd_exception fd_InvalidSchemaRef;
FD_EXPORT fd_exception fd_SchemaInconsistency;

/* File indices */

typedef struct FD_INDEX_OPENER {
  fd_index_handler handler;
  fd_index (*opener)(u8_string filename,fddb_flags flags);
  int (*matcher)(u8_string filename,void *);
  void *matcher_data;} FD_INDEX_OPENER;
typedef struct FD_INDEX_OPENER *fd_index_opener;

FD_EXPORT void fd_register_index_opener
  (fd_index_handler handler,
   fd_index (*opener)(u8_string filename,fddb_flags flags),
   int (*matcher)(u8_string filename,void *),
   void *matcher_data);

#define FD_FILE_INDEX_MAGIC_NUMBER 0x90e0418
#define FD_MULT_FILE_INDEX_MAGIC_NUMBER 0x90e0419
#define FD_MULT_FILE3_INDEX_MAGIC_NUMBER 0x90e041a
#define FD_FILE_INDEX_TO_RECOVER 0x90e0438
#define FD_MULT_FILE_INDEX_TO_RECOVER 0x90e0439
#define FD_MULT_FILE3_INDEX_TO_RECOVER 0x90e043a

#define FD_HASH_INDEX_MAGIC_NUMBER 0x8011308
#define FD_HASH_INDEX_TO_RECOVER 0x8011328

FD_EXPORT int fd_file_indexp(u8_string filename);

typedef struct FD_FILE_INDEX {
  FD_INDEX_FIELDS;
  unsigned int index_n_slots, index_hashv, *index_offsets;
  struct FD_DTYPE_STREAM index_stream;
  fdtype slotids;
  U8_MUTEX_DECL(index_lock);} FD_FILE_INDEX;
typedef struct FD_FILE_INDEX *fd_file_index;

FD_EXPORT fdtype fd_read_index_metadata(struct FD_DTYPE_STREAM *ds);
FD_EXPORT fdtype fd_write_index_metadata(fd_dtype_stream,fdtype);
FD_EXPORT int fd_make_file_index(u8_string,unsigned int,int);

unsigned int fd_hash_dtype1(fdtype x);
unsigned int fd_hash_dtype2(fdtype x);
unsigned int fd_hash_dtype3(fdtype x);
unsigned int fd_hash_dtype_rep(fdtype x);

/* Hash indices */

#define MYSTERIOUS_MODULUS 2000239099 /* 256000001 */

#define FD_HASH_INDEX_FN_MASK       0x0F
#define FD_HASH_INDEX_OFFTYPE_MASK  0x30
#define FD_HASH_INDEX_DTYPEV2       0x40
#define FD_HASH_INDEX_ODDKEYS       (FD_HASH_INDEX_DTYPEV2<<1)

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
  U8_MUTEX_DECL(index_lock);

  /* flags controls hash functions, compression, etc.
     hxcustom is a placeholder for a value to customize
     the hash function. */
  unsigned int fdb_xformat, index_custom, table_n_keys;
  fd_offset_type index_offtype;

  /* This is used to store compressed keys and values. */
  int index_n_slotids, index_new_slotids; fdtype *index_slotids;
  struct FD_SLOTID_LOOKUP *slotid_lookup;
  int index_n_baseoids, index_new_baseoids;
  unsigned int *index_baseoid_ids;
  short *index_ids2baseoids;

  /* Pointers into keyblocks for the hashtable */
  unsigned int *index_offdata; int index_n_buckets;

  /* The stream accessing the file.  This is only used
     for modification if the file is memmaped. */
  struct FD_DTYPE_STREAM index_stream;
  /* When non-null, a memmapped pointer to the file contents. */
  size_t index_mmap_size; unsigned char *index_mmap;} FD_HASH_INDEX;
typedef struct FD_HASH_INDEX *fd_hash_index;

FD_EXPORT int fd_populate_hash_index
  (struct FD_HASH_INDEX *hx,fdtype from,
   const fdtype *keys,int n_keys, int blocksize);
FD_EXPORT int fd_make_hash_index
  (u8_string,int,unsigned int,unsigned int,
   fdtype,fdtype,time_t,time_t);
FD_EXPORT int fd_hash_index_bucket
   (struct FD_HASH_INDEX *hx,fdtype key,int modulate);
FD_EXPORT int fd_hash_indexp(struct FD_INDEX *ix);
FD_EXPORT fdtype fd_hash_index_stats(struct FD_HASH_INDEX *ix);

/* Functional arguments */

#define FD_ISFUNARG(fn) \
  (FD_EXPECT_TRUE((fn==FD_VOID)||(fn==FD_FALSE)||(FD_APPLICABLEP(fn))))


#endif /* #ifndef FRAMERD_DBDRIVER_H */
