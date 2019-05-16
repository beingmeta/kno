/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "zcompress.h"

#define KNO_OIDPOOL_MAGIC_NUMBER ((0x17<<24)|(0x11<<16)|(0x04<<8)|(0x10))
#define KNO_OIDPOOL_TO_RECOVER ((0x11<<24)|(0x09<<16)|(0x04<<8)|(0x12))

/* OID Pools */

#define KNO_OIDPOOL_OFFMODE     0x07
#define KNO_OIDPOOL_COMPRESSION 0x78
#define KNO_OIDPOOL_READ_ONLY   0x80
#define KNO_OIDPOOL_DTYPEV2     0x100
#define KNO_OIDPOOL_SPARSE      0x200
#define KNO_OIDPOOL_ADJUNCT     0x400

#define KNO_OIDPOOL_LOCKED(x) (KNO_POOLSTREAM_LOCKEDP(x))

typedef struct KNO_SCHEMA_ENTRY {
  int op_nslots, op_schema_id;
  lispval *op_slotids, normal;
  unsigned int *op_slotmapin;
  unsigned int *op_slotmapout;}  KNO_SCHEMA_ENTRY;
typedef struct KNO_SCHEMA_ENTRY *kno_schema_entry;

typedef struct KNO_SCHEMA_LOOKUP {
  int op_schema_id, op_nslots;
  lispval *op_slotids;}  KNO_SCHEMA_LOOKUP;
typedef struct KNO_SCHEMA_LOOKUOP *kno_schema_lookup;

typedef struct KNO_OIDPOOL {
  KNO_POOL_FIELDS;
  unsigned int pool_load;
  struct KNO_STREAM pool_stream;
  time_t pool_mtime;
  kno_compress_type oidpool_compression;
  kno_offset_type pool_offtype;
  unsigned int *pool_offdata;
  int oidpool_n_schemas, oidpool_max_slotids;
  unsigned int oidpool_format;
  struct KNO_SCHEMA_ENTRY *oidpool_schemas;
  struct KNO_SCHEMA_LOOKUP *oidpool_schbyval;} KNO_OIDPOOL;
typedef struct KNO_OIDPOOL *kno_oidpool;

/* Schema tables */

typedef struct KNO_SCHEMA_TABLE {
  int st_index;
  int st_nslots;
  lispval *st_schema;} KNO_SCHEMA_TABLE;
typedef struct KNO_SCHEMA_TABLE *kno_schema_table;

struct OIDPOOL_FETCH_SCHEDULE {
  unsigned int value_at; KNO_CHUNK_REF location;};

struct OIDPOOL_SAVEINFO {
  KNO_CHUNK_REF chunk; unsigned int oidoff;} OIDPOOL_SAVEINFO;

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
