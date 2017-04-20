/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Network Indexes */

typedef struct FD_NETWORK_INDEX {
  FD_INDEX_FIELDS;
  int sock; fdtype xname;
  int capabilities;
  struct U8_CONNPOOL *index_connpool;} FD_NETWORK_INDEX;
typedef struct FD_NETWORK_INDEX *fd_network_index;

FD_EXPORT fd_index fd_open_network_index(u8_string spec,fd_storage_flags flags,
					 fdtype opts);

