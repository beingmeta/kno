/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#define KNO_FILEINDEX_MAGIC_NUMBER 0x90e0418
#define KNO_MULT_FILEINDEX_MAGIC_NUMBER 0x90e0419
#define KNO_MULT_FILE3_INDEX_MAGIC_NUMBER 0x90e041a
#define KNO_FILEINDEX_TO_RECOVER 0x90e0438
#define KNO_MULT_FILEINDEX_TO_RECOVER 0x90e0439
#define KNO_MULT_FILE3_INDEX_TO_RECOVER 0x90e043a

typedef struct KNO_FILEINDEX {
  KNO_INDEX_FIELDS;
  unsigned int index_n_slots, index_hashv, *index_offsets;
  struct KNO_STREAM index_stream;
  lispval slotids;
  U8_MUTEX_DECL(index_lock);} KNO_FILEINDEX;
typedef struct KNO_FILEINDEX *kno_fileindex;

KNO_EXPORT int kno_make_fileindex(u8_string,unsigned int,int);

