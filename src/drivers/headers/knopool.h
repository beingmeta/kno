/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* KNOPOOL OID pools are an updated version of oidpools. */
/* The file layout has a 256-byte header block followed by
   offset information, using the same B64/B40/B32 representation
   as oidpools and hashindices. */

#include "zcompress.h"
#include "kno/xtypes.h"

#define KNO_KNOPOOL_MAGIC_NUMBER ((11<<24)|(14<<16)|(15<<8)|(16))
#define KNO_KNOPOOL_TO_RECOVER ((2<<24)|(7<<16)|(16<<8)|(0xFF))

#ifndef KNO_INIT_ZBUF_SIZE
#define KNO_INIT_ZBUF_SIZE 24000
#endif

#define KNO_KNOPOOL_LOAD_POS      0x10

#define KNO_KNOPOOL_FORMAT_POS            0x14
#define KNO_KNOPOOL_LABEL_POS             0x18
#define KNO_KNOPOOL_METADATA_POS          0x24
#define KNO_KNOPOOL_XREFS_POS             0x54

#define KNO_KNOPOOL_FETCHBUF_SIZE 8000

#define KNO_KNOPOOL_OFFMODE      0x07
#define KNO_KNOPOOL_COMPRESSION  0x78
#define KNO_KNOPOOL_READ_ONLY    0x80
#define KNO_KNOPOOL_SPARSE      0x100
#define KNO_KNOPOOL_ADJUNCT     0x200
#define KNO_KNOPOOL_SYMREFS     0x400
#define KNO_KNOPOOL_OIDREFS     0x800

typedef struct KNO_KNOPOOL {
  KNO_POOL_FIELDS;
  unsigned int pool_load;
  struct KNO_STREAM pool_stream;
  time_t pool_ctime, pool_mtime, pool_rptime, file_mtime;
  long long pool_repack_count;
  kno_compress_type pool_compression;
  kno_offset_type pool_offtype;
  unsigned int *pool_offdata;
  unsigned int pool_offlen;
  unsigned int knopool_format;
  ssize_t pool_nblocks;
  struct XTYPE_REFS pool_xrefs;} KNO_KNOPOOL;
typedef struct KNO_KNOPOOL *kno_knopool;

struct KNOPOOL_FETCH_SCHEDULE {
  unsigned int value_at;
  KNO_CHUNK_REF location;};

struct KNOPOOL_SAVEINFO {
  KNO_CHUNK_REF chunk;
  unsigned int oidoff;} KNOPOOL_SAVEINFO;

