/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/


#ifndef KNO_NETDB_H
#define KNO_NETDB_H 1
#ifndef KNO_NETDB_H_INFO
#define KNO_NETDB_H_INFO "include/kno/netdb.h"
#endif

/* Network Indexes */

typedef struct KNO_NET_INDEX {
  KNO_INDEX_FIELDS;
  lispval xname;
  int capabilities;
  struct U8_CONNPOOL *index_connpool;} KNO_NET_INDEX;
typedef struct KNO_NET_INDEX *kno_net_index;

typedef struct KNO_NET_POOL {
  KNO_POOL_FIELDS;
  struct U8_CONNPOOL *pool_connpool;
  int bulk_commitp;} KNO_NET_POOL;
typedef struct KNO_NET_POOL *kno_net_pool;

KNO_EXPORT kno_index kno_open_net_index(u8_string spec,kno_storage_flags flags,
					lispval opts);
KNO_EXPORT kno_index kno_open_net_pool(u8_string spec,kno_storage_flags flags,
				       lispval opts);

#endif /* KNO_NETDB_H */
