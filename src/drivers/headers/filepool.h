/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* File Pools */

#define KNO_FILE_POOL_MAGIC_NUMBER 67179521
#define KNO_FILE_POOL_TO_RECOVER 67179523

typedef struct KNO_FILE_POOL {
  KNO_POOL_FIELDS;
  time_t pool_mtime;
  unsigned int pool_load, *pool_offdata, pool_offdata_size;
  struct KNO_STREAM pool_stream;} KNO_FILE_POOL;
typedef struct KNO_FILE_POOL *kno_file_pool;

KNO_EXPORT int kno_make_file_pool(u8_string,unsigned int,
				KNO_OID,unsigned int,unsigned int);

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
