/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>

#include <stdio.h>

int builtindbs_c_initialized = 0;

FD_EXPORT int fd_init_builtindbs_c()
{
  if (builtindbs_c_initialized)
    return builtindbs_c_initialized;
  builtindbs_c_initialized = u8_millitime();

  u8_register_source_file(_FILEINFO);

  return builtindbs_c_initialized;
}

int fd_init_netindex_c(void);
int fd_init_fileindex_c(void);
int fd_init_hashindex_c(void);
int fd_init_memindex_c(void);

int fd_init_netpool_c(void);
int fd_init_file_pool_c(void);
int fd_init_oidpool_c(void);
int fd_init_bigpool_c(void);

int dbs_initialized = 0;

FD_EXPORT int fd_init_drivers()
{
  if (dbs_initialized) return dbs_initialized;
  dbs_initialized = 307*fd_init_storage();

  fd_init_builtindbs_c();

  fd_init_file_pool_c();
  fd_init_oidpool_c();
  fd_init_bigpool_c();
  fd_init_netpool_c();

  fd_init_fileindex_c();
  fd_init_hashindex_c();

  fd_init_netindex_c();
  fd_init_memindex_c();

  u8_register_source_file(_FILEINFO);

  return dbs_initialized;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
