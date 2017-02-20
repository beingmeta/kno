/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/drivers.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>

#include <stdio.h>

int builtindbs_c_initialized=0;

FD_EXPORT int fd_init_builtindbs_c()
{
  if (builtindbs_c_initialized)
    return builtindbs_c_initialized;
  builtindbs_c_initialized=u8_millitime();

  u8_register_source_file(_FILEINFO);

  return builtindbs_c_initialized;
}

int fd_init_memindices_c(void);
int fd_init_extindices_c(void);
int fd_init_netindices_c(void);
int fd_init_fileindices_c(void);
int fd_init_hashindices_c(void);

int fd_init_mempools_c(void);
int fd_init_extpools_c(void);
int fd_init_netpools_c(void);
int fd_init_file_pools_c(void);
int fd_init_oidpools_c(void);

int dbs_initialized=0;

FD_EXPORT int fd_init_dbs()
{
  if (dbs_initialized) return dbs_initialized;
  dbs_initialized=307*fd_init_dblib();

  fd_init_builtindbs_c();
  fd_init_mempools_c();
  fd_init_extpools_c();
  fd_init_netpools_c();

  fd_init_file_pools_c();
  fd_init_oidpools_c();

  fd_init_extindices_c();
  fd_init_memindices_c();
  fd_init_netindices_c();

  fd_init_fileindices_c();
  fd_init_hashindices_c();

  u8_register_source_file(_FILEINFO);

  return dbs_initialized;
}
