/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* BIGPOOL OID pools are an updated version of oidpools. */
/* The file layout has a 256-byte header block followed by
   offset information, using the same B64/B40/B32 representation
   as oidpools and hashindices. */

#include "chunkrefs.h"

#define FD_BIGPOOL_MAGIC_NUMBER ((0x17<<24)|(0x11<<16)|(0x04<<8)|(0x10))
#define FD_BIGPOOL_TO_RECOVER ((0x11<<24)|(0x09<<16)|(0x04<<8)|(0x12))

/* OID Pools */

#define FD_BIGPOOL_OFFMODE     0x07
#define FD_BIGPOOL_COMPRESSION 0x78
#define FD_BIGPOOL_READ_ONLY   0x80
#define FD_BIGPOOL_DTYPEV2     0x100

typedef struct FD_BIGPOOL {
  FD_POOL_FIELDS;
  unsigned int pool_xformat;
  fd_offset_type pool_offtype;
  fd_compression_type pool_compression;
  struct FD_STREAM pool_stream;
  unsigned int pool_load;
  unsigned int *pool_offdata, pool_offdata_length;
  unsigned char *pool_mmap; size_t pool_mmap_size;
  fdtype *slotids;
  unsigned int n_slotids, slotids_length;
  struct FD_HASHTABLE slotcodes;
  time_t pool_modtime;
  U8_MUTEX_DECL(file_lock);} FD_BIGPOOL;
typedef struct FD_BIGPOOL *fd_bigpool;


