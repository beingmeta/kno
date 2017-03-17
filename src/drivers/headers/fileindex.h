/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#define FD_FILE_INDEX_MAGIC_NUMBER 0x90e0418
#define FD_MULT_FILE_INDEX_MAGIC_NUMBER 0x90e0419
#define FD_MULT_FILE3_INDEX_MAGIC_NUMBER 0x90e041a
#define FD_FILE_INDEX_TO_RECOVER 0x90e0438
#define FD_MULT_FILE_INDEX_TO_RECOVER 0x90e0439
#define FD_MULT_FILE3_INDEX_TO_RECOVER 0x90e043a

typedef struct FD_FILE_INDEX {
  FD_INDEX_FIELDS;
  unsigned int index_n_slots, index_hashv, *index_offsets;
  struct FD_STREAM index_stream;
  fdtype slotids;
  U8_MUTEX_DECL(index_lock);} FD_FILE_INDEX;
typedef struct FD_FILE_INDEX *fd_file_index;

FD_EXPORT int fd_make_file_index(u8_string,unsigned int,int);

