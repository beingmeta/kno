/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* BIGPOOL OID pools are an updated version of oidpools. */
/* The file layout has a 256-byte header block followed by
   offset information, using the same B64/B40/B32 representation
   as oidpools and hashindices. */

#include "zcompress.h"

#define FD_BIGPOOL_MAGIC_NUMBER ((2<<24)|(7<<16)|(16<<8)|(12))
#define FD_BIGPOOL_TO_RECOVER ((2<<24)|(7<<16)|(16<<8)|(0xFF))


#ifndef FD_INIT_ZBUF_SIZE
#define FD_INIT_ZBUF_SIZE 24000
#endif

#define FD_BIGPOOL_LOAD_POS      0x10

#define FD_BIGPOOL_FORMAT_POS            0x14
#define FD_BIGPOOL_LABEL_POS             0x18
#define FD_BIGPOOL_METADATA_POS          0x24
#define FD_BIGPOOL_SLOTCODES_POS         0x54

#define FD_BIGPOOL_FETCHBUF_SIZE 8000

#define FD_BIGPOOL_OFFMODE     0x07
#define FD_BIGPOOL_COMPRESSION 0x78
#define FD_BIGPOOL_READ_ONLY   0x80
#define FD_BIGPOOL_DTYPEV2     0x100
#define FD_BIGPOOL_SPARSE      0x200
#define FD_BIGPOOL_ADJUNCT     0x400

typedef struct FD_BIGPOOL {
  FD_POOL_FIELDS;
  unsigned int pool_load;
  struct FD_STREAM pool_stream;
  time_t pool_ctime, pool_mtime, pool_rptime, file_mtime;
  long long pool_repack_count;
  fd_compress_type pool_compression;
  fd_offset_type pool_offtype;
  unsigned int *pool_offdata;
  unsigned int pool_offlen;
  unsigned int bigpool_format;
  ssize_t pool_nblocks;
  struct FD_SLOTCODER pool_slotcodes;} FD_BIGPOOL;
typedef struct FD_BIGPOOL *fd_bigpool;

struct BIGPOOL_FETCH_SCHEDULE {
  unsigned int value_at; FD_CHUNK_REF location;};

struct BIGPOOL_SAVEINFO {
  FD_CHUNK_REF chunk; unsigned int oidoff;} BIGPOOL_SAVEINFO;

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
