/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "zcompress.h"

#define FD_OIDPOOL_MAGIC_NUMBER ((0x17<<24)|(0x11<<16)|(0x04<<8)|(0x10))
#define FD_OIDPOOL_TO_RECOVER ((0x11<<24)|(0x09<<16)|(0x04<<8)|(0x12))

/* OID Pools */

#define FD_OIDPOOL_OFFMODE     0x07
#define FD_OIDPOOL_COMPRESSION 0x78
#define FD_OIDPOOL_READ_ONLY   0x80
#define FD_OIDPOOL_DTYPEV2     0x100
#define FD_OIDPOOL_SPARSE      0x200
#define FD_OIDPOOL_ADJUNCT     0x400

#define FD_OIDPOOL_LOCKED(x) (FD_POOLSTREAM_LOCKEDP(x))

typedef struct FD_SCHEMA_ENTRY {
  int op_nslots, op_schema_id;
  lispval *op_slotids, normal;
  unsigned int *op_slotmapin;
  unsigned int *op_slotmapout;}  FD_SCHEMA_ENTRY;
typedef struct FD_SCHEMA_ENTRY *fd_schema_entry;

typedef struct FD_SCHEMA_LOOKUP {
  int op_schema_id, op_nslots;
  lispval *op_slotids;}  FD_SCHEMA_LOOKUP;
typedef struct FD_SCHEMA_LOOKUOP *fd_schema_lookup;

typedef struct FD_OIDPOOL {
  FD_POOL_FIELDS;
  unsigned int pool_load;
  struct FD_STREAM pool_stream;
  time_t pool_mtime;
  fd_compress_type oidpool_compression;
  fd_offset_type pool_offtype;
  unsigned int *pool_offdata;
  int oidpool_n_schemas, oidpool_max_slotids;
  unsigned int oidpool_format;
  struct FD_SCHEMA_ENTRY *oidpool_schemas;
  struct FD_SCHEMA_LOOKUP *oidpool_schbyval;} FD_OIDPOOL;
typedef struct FD_OIDPOOL *fd_oidpool;

/* Schema tables */

typedef struct FD_SCHEMA_TABLE {
  int st_index;
  int st_nslots;
  lispval *st_schema;} FD_SCHEMA_TABLE;
typedef struct FD_SCHEMA_TABLE *fd_schema_table;

struct OIDPOOL_FETCH_SCHEDULE {
  unsigned int value_at; FD_CHUNK_REF location;};

struct OIDPOOL_SAVEINFO {
  FD_CHUNK_REF chunk; unsigned int oidoff;} OIDPOOL_SAVEINFO;

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
