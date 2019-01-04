/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* File Pools */

#define FD_FILE_POOL_MAGIC_NUMBER 67179521
#define FD_FILE_POOL_TO_RECOVER 67179523

typedef struct FD_FILE_POOL {
  FD_POOL_FIELDS;
  time_t pool_mtime;
  unsigned int pool_load, *pool_offdata, pool_offdata_size;
  struct FD_STREAM pool_stream;} FD_FILE_POOL;
typedef struct FD_FILE_POOL *fd_file_pool;

FD_EXPORT int fd_make_file_pool(u8_string,unsigned int,
				FD_OID,unsigned int,unsigned int);

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
