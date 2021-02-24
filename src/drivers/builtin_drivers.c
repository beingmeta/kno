/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>

#include <stdio.h>

int builtindbs_c_initialized = 0;

KNO_EXPORT int kno_init_builtindbs_c()
{
  if (builtindbs_c_initialized)
    return builtindbs_c_initialized;
  builtindbs_c_initialized = u8_millitime();

  u8_register_source_file(_FILEINFO);

  return builtindbs_c_initialized;
}

int kno_init_netindex_c(void);
int kno_init_fileindex_c(void);
int kno_init_hashindex_c(void);
int kno_init_logindex_c(void);
int kno_init_kindex_c(void);

int kno_init_netpool_c(void);
int kno_init_file_pool_c(void);
int kno_init_bigpool_c(void);
int kno_init_kpool_c(void);

int kno_init_knosocks_c(void);
int kno_init_zpathstore_c(void);

int dbs_initialized = 0;

KNO_EXPORT int kno_init_drivers()
{
  if (dbs_initialized) return dbs_initialized;
  dbs_initialized = 307*kno_init_storage();

  kno_init_builtindbs_c();
  kno_init_knosocks_c();

  kno_init_file_pool_c();
  kno_init_netpool_c();
  kno_init_kpool_c();

  kno_init_fileindex_c();
  kno_init_kindex_c();

  kno_init_netindex_c();
  kno_init_logindex_c();

  kno_init_zpathstore_c();

  /* Deprecated */
  kno_init_bigpool_c();
  kno_init_hashindex_c();

  u8_register_source_file(_FILEINFO);

  return dbs_initialized;
}

