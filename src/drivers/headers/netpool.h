/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Network Pools */

typedef struct KNO_NETWORK_POOL {
  KNO_POOL_FIELDS;
  kno_service pool_server;
  lispval pool_dbname;
  int bulk_commitp;} KNO_NETWORK_POOL;
typedef struct KNO_NETWORK_POOL *kno_network_pool;

KNO_EXPORT kno_pool kno_open_network_pool
(u8_string spec,lispval dbname,kno_storage_flags flags,lispval opts);
