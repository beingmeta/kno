/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "chunkrefs.h"

#define FD_OIDPOOL_MAGIC_NUMBER ((0x17<<24)|(0x11<<16)|(0x04<<8)|(0x10))
#define FD_OIDPOOL_TO_RECOVER ((0x11<<24)|(0x09<<16)|(0x04<<8)|(0x12))

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
  fd_compress_type pool_compression;
  time_t pool_modtime;
  int pool_n_schemas, pool_max_slotids;
  struct FD_SCHEMA_ENTRY *pool_schemas;
  struct FD_SCHEMA_LOOKUP *pool_schbyval;
  unsigned int pool_load, *pool_offdata, pool_offdata_size;
  struct FD_STREAM pool_stream;;} FD_OIDPOOL;
typedef struct FD_OIDPOOL *fd_oidpool;

FD_EXPORT int fd_make_oidpool
  (u8_string fname,u8_string label,
   FD_OID base,unsigned int capacity,unsigned int load,
   unsigned int flags,fdtype schemas_init,
   time_t ctime,time_t mtime,int cycles);

/* Schema tables */

typedef struct FD_SCHEMA_TABLE {
  int fdst_index;
  int fdst_nslots;
  fdtype *fdst_schema;} FD_SCHEMA_TABLE;
typedef struct FD_SCHEMA_TABLE *fd_schema_table;

