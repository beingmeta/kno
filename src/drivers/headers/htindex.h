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

typedef struct FD_HT_INDEX {
  FD_INDEX_FIELDS;
  int (*commitfn)(struct FD_HT_INDEX *,u8_string);} FD_HT_INDEX;
typedef struct FD_HT_INDEX *fd_mem_index;

FD_EXPORT fd_index fd_make_ht_index(fd_storage_flags flags);

