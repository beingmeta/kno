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

#define FD_BIGPOOL_MAGIC_NUMBER ((2<<24)|(7<<16)|(16<<8)|(12))
#define FD_BIGPOOL_TO_RECOVER ((2<<24)|(7<<16)|(16<<8)|(0xFF))


#ifndef FD_INIT_ZBUF_SIZE
#define FD_INIT_ZBUF_SIZE 24000
#endif

#define FD_BIGPOOL_LOAD_POS      0x10

#define FD_BIGPOOL_LABEL_POS             0x18
#define FD_BIGPOOL_METADATA_POS          0x24
#define FD_BIGPOOL_SLOTIDS_POS           0x54

#define FD_BIGPOOL_FETCHBUF_SIZE 8000

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
  fdtype *slotids, *old_slotids;
  unsigned int n_slotids, slotids_length, added_slotids;
  struct FD_HASHTABLE slotcodes;
  time_t pool_modtime;
  U8_MUTEX_DECL(file_lock);} FD_BIGPOOL;
typedef struct FD_BIGPOOL *fd_bigpool;

