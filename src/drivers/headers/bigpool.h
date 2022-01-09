/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu) (ken.haase@alum.mit.edu)
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu) (ken.haase@alum.mit.edu)
*/

/* BIGPOOL OID pools are an updated version of oidpools. */
/* The file layout has a 256-byte header block followed by
   offset information, using the same B64/B40/B32 representation
   as oidpools and hashindices. */

#include "zcompress.h"

#define KNO_BIGPOOL_MAGIC_NUMBER ((2<<24)|(7<<16)|(16<<8)|(12))
#define KNO_BIGPOOL_TO_RECOVER ((2<<24)|(7<<16)|(16<<8)|(0xFF))


#ifndef KNO_INIT_ZBUF_SIZE
#define KNO_INIT_ZBUF_SIZE 24000
#endif

#define KNO_BIGPOOL_LOAD_POS      0x10

#define KNO_BIGPOOL_FORMAT_POS            0x14
#define KNO_BIGPOOL_LABEL_POS             0x18
#define KNO_BIGPOOL_METADATA_POS          0x24
#define KNO_BIGPOOL_SLOTCODES_POS         0x54

#define KNO_BIGPOOL_FETCHBUF_SIZE 8000

#define KNO_BIGPOOL_OFFMODE     0x07
#define KNO_BIGPOOL_COMPRESSION 0x78
#define KNO_BIGPOOL_READ_ONLY   0x80
#define KNO_BIGPOOL_DTYPEV2     0x100
#define KNO_BIGPOOL_SPARSE      0x200
#define KNO_BIGPOOL_ADJUNCT     0x400

typedef struct KNO_BIGPOOL {
  KNO_POOL_FIELDS;
  unsigned int pool_load;
  struct KNO_STREAM pool_stream;
  time_t pool_ctime, pool_mtime, pool_rptime, file_mtime;
  long long pool_repack_count;
  kno_compress_type pool_compression;
  kno_offset_type pool_offtype;
  unsigned int *pool_offdata;
  unsigned int pool_offlen;
  unsigned int bigpool_format;
  ssize_t pool_nblocks;
  struct KNO_SLOTCODER pool_slotcodes;} KNO_BIGPOOL;
typedef struct KNO_BIGPOOL *kno_bigpool;

struct BIGPOOL_FETCH_SCHEDULE {
  unsigned int value_at; KNO_CHUNK_REF location;};

struct BIGPOOL_SAVEINFO {
  KNO_CHUNK_REF chunk; unsigned int oidoff;} BIGPOOL_SAVEINFO;

