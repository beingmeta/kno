/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#define FD_FILEINDEX_MAGIC_NUMBER 0x90e0418
#define FD_MULT_FILEINDEX_MAGIC_NUMBER 0x90e0419
#define FD_MULT_FILE3_INDEX_MAGIC_NUMBER 0x90e041a
#define FD_FILEINDEX_TO_RECOVER 0x90e0438
#define FD_MULT_FILEINDEX_TO_RECOVER 0x90e0439
#define FD_MULT_FILE3_INDEX_TO_RECOVER 0x90e043a

typedef struct FD_FILEINDEX {
  FD_INDEX_FIELDS;
  unsigned int index_n_slots, index_hashv, *index_offsets;
  struct FD_STREAM index_stream;
  lispval slotids;
  U8_MUTEX_DECL(index_lock);} FD_FILEINDEX;
typedef struct FD_FILEINDEX *fd_fileindex;

FD_EXPORT int fd_make_fileindex(u8_string,unsigned int,int);

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
