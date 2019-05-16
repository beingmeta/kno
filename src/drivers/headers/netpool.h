/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Network Pools */

typedef struct KNO_NETWORK_POOL {
  KNO_POOL_FIELDS;
  struct U8_CONNPOOL *pool_connpool;
  int bulk_commitp;} KNO_NETWORK_POOL;
typedef struct KNO_NETWORK_POOL *kno_network_pool;

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
