/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* LOGIX indexes keep themselves in memory but support fast commits
   by just appending to the disk file (like a log) */
/* The file layout has a 256-byte header block consisting of
     0x00 Magic number (4 bytes)
     0x10 Valid data size (8 bytes)
     
   The header block is followed by key value entries
   each of which consists of a 1-byte code followed by 4 bytes of
   length and the dtype representations of a key and a value.
   The code is either -1 (drop), 0 (store), or 1 (add).
*/

#include "chunkrefs.h"

#define FD_MEM_INDEX_MAGIC_NUMBER ((12<<24)|(7<<16)|(9<<8)|(24))

typedef struct FD_MEM_INDEX {
  FD_INDEX_FIELDS;
  unsigned int lix_n_commits;
  unsigned int lix_n_keys, lix_n_entries;
  unsigned int lix_n_slotids, lix_n_added;
  unsigned int lix_slotids_length;
  fdtype *lix_slotids;
  size_t lix_valid_data;
  struct FD_STREAM index_stream;} FD_LOG_INDEX;
typedef struct FD_LOG_INDEX *fd_log_index;


