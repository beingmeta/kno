/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Network Pools */

typedef struct FD_NETWORK_POOL {
  FD_POOL_FIELDS;
  struct U8_CONNPOOL *pool_connpool;
  int bulk_commitp;} FD_NETWORK_POOL;
typedef struct FD_NETWORK_POOL *fd_network_pool;

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
