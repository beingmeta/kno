/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_DBXFILE_H
#define FDB_DBXFILE_H 1
#define FDB_DBXFILE_H_VERSION "$Id: dbfile.h 775 2006-12-13 20:58:03Z haase $"

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


#endif /* #ifndef FDB_DBXFILE_H */

