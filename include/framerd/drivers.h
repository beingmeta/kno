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

FD_EXPORT int fd_init_drivers(void) FD_LIBINIT_FN;

FD_EXPORT fd_compress_type fd_compression_type(lispval,fd_compress_type);

/* These are common pool/index ops (for use with fd_poolctl/fd_indexctl) */

FD_EXPORT lispval fd_cachelevel_op, fd_bufsize_op, fd_mmap_op, fd_preload_op;
FD_EXPORT lispval fd_metadata_op, fd_raw_metadata_op, fd_reload_op;
FD_EXPORT lispval fd_stats_op, fd_label_op, fd_populate_op, fd_swapout_op;
FD_EXPORT lispval fd_getmap_op, fd_slotids_op, fd_baseoids_op, fd_keys_op;
FD_EXPORT lispval fd_load_op, fd_capacity_op;

FD_EXPORT u8_condition fd_InvalidOffsetType;
FD_EXPORT u8_condition fd_BadMetaData, fd_FutureMetaData;
FD_EXPORT u8_condition fd_MMAPError, fd_MUNMAPError;
FD_EXPORT u8_condition fd_RecoveryRequired;

FD_EXPORT u8_condition fd_PoolDriverError, fd_IndexDriverError;
FD_EXPORT u8_condition fd_CantOpenPool, fd_CantOpenIndex;
FD_EXPORT u8_condition fd_CantFindPool, fd_CantFindIndex;
FD_EXPORT u8_condition fd_PoolFileSizeOverflow, fd_FileIndexSizeOverflow;
FD_EXPORT u8_condition fd_CorruptedPool, fd_CorruptedIndex;

FD_EXPORT u8_string fd_match4bytes(u8_string file,void *data);
FD_EXPORT u8_string fd_netspecp(u8_string file,void *data);

struct FD_POOL_TYPEINFO {
  u8_string pool_typename;
  fd_pool_handler handler;
  fd_pool (*opener)(u8_string filename,fd_storage_flags flags,lispval opts);
  u8_string (*matcher)(u8_string filename,void *);
  void *type_data;
  struct FD_POOL_TYPEINFO *next_type;};
typedef struct FD_POOL_TYPEINFO *fd_pool_typeinfo;

FD_EXPORT
void fd_register_pool_type
(u8_string name,
 fd_pool_handler pool_handler,
 fd_pool (*opener)(u8_string path,fd_storage_flags flags,lispval opts),
 u8_string (*matcher)(u8_string path,void *),
 void *type_data);

FD_EXPORT fd_pool_typeinfo fd_set_default_pool_type(u8_string name);

FD_EXPORT fd_pool_handler fd_get_pool_handler(u8_string name);

FD_EXPORT fd_pool fd_make_pool(u8_string spec,u8_string pooltype,
			       fd_storage_flags flags,lispval opts);

#define FD_POOLFILE_LOCKEDP(fp) \
  (((fp)->pool_stream.stream_flags)&FD_STREAM_FILE_LOCKED)
#define FD_LOCK_POOLFILE(fp) fd_lockfile(&((fp)->pool_stream))
#define FD_UNLOCK_POOLFILE(fp) fd_unlockfile(&((fp)->pool_stream))

/* File indexes */

struct FD_INDEX_TYPEINFO {
  u8_string index_typename;
  fd_index_handler handler;
  fd_index (*opener)(u8_string filename,fd_storage_flags flags,lispval opts);
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
		    lispval opts),
 u8_string (*matcher)(u8_string spec,
		      void *),
 void *type_data);

FD_EXPORT fd_index_typeinfo fd_set_default_index_type(u8_string name);

FD_EXPORT fd_index_handler fd_get_index_handler(u8_string name);

FD_EXPORT fd_index fd_make_index(u8_string spec,u8_string indextype,
				 fd_storage_flags flags,lispval opts);

FD_EXPORT lispval fd_read_index_metadata(fd_stream ds);
FD_EXPORT lispval fd_write_index_metadata(fd_stream,lispval);
FD_EXPORT int fd_make_fileindex(u8_string,unsigned int,int);

unsigned int fd_hash_lisp1(lispval x);
unsigned int fd_hash_lisp2(lispval x);
unsigned int fd_hash_lisp3(lispval x);
unsigned int fd_hash_dtype_rep(lispval x);

/* Functional arguments */

#define FD_ISFUNARG(fn) \
  (FD_EXPECT_TRUE((fn == FD_VOID)||(fn == FD_FALSE)||(FD_APPLICABLEP(fn))))

/* Matching db file names */

FD_EXPORT u8_string fd_match_pool_file(u8_string spec,void *data);
FD_EXPORT u8_string fd_match_index_file(u8_string spec,void *data);

#endif /* #ifndef FRAMERD_DRIVERS_H */
