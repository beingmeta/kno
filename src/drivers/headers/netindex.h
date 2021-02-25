/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Network Indexes */

typedef struct KNO_NETWORK_INDEX {
  KNO_INDEX_FIELDS;
  int capabilities;
  kno_service index_server;
  lispval index_dbname;} KNO_NETWORK_INDEX;
typedef struct KNO_NETWORK_INDEX *kno_network_index;

KNO_EXPORT kno_index kno_open_network_index(u8_string spec,
					    lispval dbname,
					    kno_storage_flags flags,
					    lispval opts);

