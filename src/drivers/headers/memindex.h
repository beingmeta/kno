/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* MEMIDX indexes keep themselves in memory but support fast commits
   by just appending to the disk file (like a log) */
/* The file layout has a 256-byte header block consisting of
     0x00 Magic number (4 bytes)
     0x10 Valid data size (8 bytes)

   The header block is followed by key value entries
   each of which consists of a 1-byte code followed by 4 bytes of
   length and the dtype representations of a key and a value.
   The code is either -1 (drop), 0 (store), or 1 (add).
*/

#define KNO_MEMINDEX_MAGIC_NUMBER ((12<<24)|(7<<16)|(9<<8)|(24))

typedef struct KNO_MEMINDEX {
  KNO_INDEX_FIELDS;
  unsigned int mix_loaded;
  struct KNO_HASHTABLE mix_map;
  unsigned int mix_n_commits;
  unsigned int mix_n_keys, mix_n_entries;
  unsigned int mix_n_slotids, mix_added_slotids;
  unsigned int mix_slotids_length;
  unsigned int mix_n_baseoids, mix_added_baseoids;
  unsigned int mix_baseoids_length;
  lispval *mix_slotids, *mix_baseoids;
  size_t mix_valid_data;
  struct KNO_STREAM index_stream;} KNO_LOG_INDEX;
typedef struct KNO_LOG_INDEX *kno_log_index;

