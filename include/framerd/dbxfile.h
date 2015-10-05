/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_DBXFILE_H
#define FRAMERD_DBXFILE_H 1
#ifndef FRAMERD_DBXFILE_H_INFO
#define FRAMERD_DBXFILE_H_INFO "include/framerd/dbxfile.h"
#endif

#include "fddb.h"
#include "pools.h"
#include "indices.h"

#include <db.h>

#ifndef FD_FILEDB_BUFSIZE
#define FD_FILEDB_BUFSIZE 256*1024
#endif

FD_EXPORT int fd_init_dbxfile(void) FD_LIBINIT_FN;

/* DB Pools */

typedef struct FD_DBX_POOL {
  FD_POOL_FIELDS;
  u8_string filename, dbname;
  time_t modtime; unsigned int load;
  DB *dbp;
  U8_MUTEX_DECL(lock);} FD_DBX_POOL;
typedef struct FD_DBX_POOL *fd_dbx_pool;


#endif /* #ifndef FRAMERD_DBXFILE_H */

