/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu) (ken.haase@alum.mit.edu)
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu) (ken.haase@alum.mit.edu)
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

#define KNO_LOGINDEX_MAGIC_NUMBER ((12<<24)|(7<<16)|(9<<8)|(24))

typedef struct KNO_LOGINDEX {
  KNO_INDEX_FIELDS;
  unsigned int logx_loaded;
  struct KNO_HASHTABLE logx_map;
  unsigned int logx_n_commits;
  unsigned int logx_n_keys, logx_n_entries;
  unsigned int logx_n_slotids, logx_added_slotids;
  unsigned int logx_slotids_length;
  unsigned int logx_n_baseoids, logx_added_baseoids;
  unsigned int logx_baseoids_length;
  lispval *logx_slotids, *logx_baseoids;
  size_t logx_valid_data;
  struct KNO_STREAM index_stream;} KNO_LOG_INDEX;
typedef struct KNO_LOG_INDEX *kno_log_index;

