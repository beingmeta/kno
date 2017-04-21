/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_DRIVERS_H
#define FRAMERD_DRIVERS_H 1
#ifndef FRAMERD_DRIVERS_H_INFO
#define FRAMERD_DRIVERS_H_INFO "include/framerd/drivers.h"
#endif

FD_EXPORT size_t fd_driver_bufsize;

FD_EXPORT int fd_acid_files;

FD_EXPORT int fd_init_kbdrivers(void) FD_LIBINIT_FN;

typedef enum FD_OFFSET_TYPE { FD_B32 = 0, FD_B40 = 1, FD_B64 = 2 } fd_offset_type;
typedef enum FD_COMPRESS_TYPE {
  FD_NOCOMPRESS = 0,
  FD_ZLIB = 1,
  FD_ZLIB9 = 2,
  FD_SNAPPY = 3}
  fd_compress_type;

FD_EXPORT fd_compress_type fd_compression_type(fdtype,fd_compress_type);

FD_EXPORT fd_exception fd_InvalidOffsetType;
FD_EXPORT fd_exception fd_BadMetaData, fd_FutureMetaData;
FD_EXPORT fd_exception fd_MMAPError, fd_MUNMAPError;
FD_EXPORT fd_exception fd_RecoveryRequired;

FD_EXPORT fd_exception fd_PoolDriverError, fd_IndexDriverError;
FD_EXPORT fd_exception fd_CantOpenPool, fd_CantOpenIndex;
FD_EXPORT fd_exception fd_FilePoolSizeOverflow, fd_FileIndexSizeOverflow;
FD_EXPORT fd_exception fd_CorruptedPool, fd_CorruptedIndex;

FD_EXPORT u8_string fd_match4bytes(u8_string file,void *data);
FD_EXPORT u8_string fd_netspecp(u8_string file,void *data);

struct FD_POOL_TYPEINFO {
  u8_string pool_typename;
  fd_pool_handler handler;
  fd_pool (*opener)(u8_string filename,fd_storage_flags flags,fdtype opts);
  u8_string (*matcher)(u8_string filename,void *);
  void *type_data;
  struct FD_POOL_TYPEINFO *next_type;};
typedef struct FD_POOL_TYPEINFO *fd_pool_typeinfo;

FD_EXPORT
void fd_register_pool_type
(u8_string name,
 fd_pool_handler pool_handler,
 fd_pool (*opener)(u8_string path,fd_storage_flags flags,fdtype opts),
 u8_string (*matcher)(u8_string path,void *),
 void *type_data);

FD_EXPORT fd_pool_handler fd_get_pool_handler(u8_string name);

FD_EXPORT fd_pool fd_make_pool(u8_string spec,u8_string pooltype,
			       fd_storage_flags flags,fdtype opts);

#define FD_POOLFILE_LOCKEDP(fp) \
  (((fp)->pool_stream.stream_flags)&FD_STREAM_FILE_LOCKED)
#define FD_LOCK_POOLFILE(fp) fd_lockfile(&((fp)->pool_stream))
#define FD_UNLOCK_POOLFILE(fp) fd_unlockfile(&((fp)->pool_stream))

FD_EXPORT fd_pool fd_unregistered_file_pool(u8_string filename);

/* File indexes */

struct FD_INDEX_TYPEINFO {
  u8_string index_typename;
  fd_index_handler handler;
  fd_index (*opener)(u8_string filename,fd_storage_flags flags,fdtype opts);
  u8_string (*matcher)(u8_string filename,void *);
  void *type_data;
  struct FD_INDEX_TYPEINFO *next_type;};
typedef struct FD_INDEX_TYPEINFO *fd_index_typeinfo;

FD_EXPORT
void fd_register_index_type
(u8_string name,
 fd_index_handler handler,
 fd_index (*opener)(u8_string spec,
		    fd_storage_flags flags,
		    fdtype opts),
 u8_string (*matcher)(u8_string spec,
		      void *),
 void *type_data);

FD_EXPORT fd_index_handler fd_get_index_handler(u8_string name);

FD_EXPORT fd_index fd_make_index(u8_string spec,u8_string indextype,
				 fd_storage_flags flags,fdtype opts);

FD_EXPORT fdtype fd_read_index_metadata(fd_stream ds);
FD_EXPORT fdtype fd_write_index_metadata(fd_stream,fdtype);
FD_EXPORT int fd_make_file_index(u8_string,unsigned int,int);

unsigned int fd_hash_dtype1(fdtype x);
unsigned int fd_hash_dtype2(fdtype x);
unsigned int fd_hash_dtype3(fdtype x);
unsigned int fd_hash_dtype_rep(fdtype x);

/* Functional arguments */

#define FD_ISFUNARG(fn) \
  (FD_EXPECT_TRUE((fn == FD_VOID)||(fn == FD_FALSE)||(FD_APPLICABLEP(fn))))


#endif /* #ifndef FRAMERD_DRIVERS_H */
