/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_PATHSTORE_H
#define KNO_PATHSTORE_H 1
#ifndef KNO_PATHSTORE_H_INFO
#define KNO_PATHSTORE_H_INFO "include/kno/eval.h"
#endif

typedef struct KNO_PATHSTORE {
  KNO_CONS_HEADER;
  u8_string knops_id;
  u8_string knops_prefix;
  u8_string knops_filecache;
  struct KNO_HASHTABLE knops_cache;
  struct KNO_PATHSTORE_METHODS {
    int (*existsp)(struct KNO_PATHSTORE *,u8_string);
    lispval (*info)(struct KNO_PATHSTORE *,u8_string);
    lispval (*content)(struct KNO_PATHSTORE *,u8_string);}
    *knops_handlers;} *kno_pathstore;

#endif /* KNO_PATHSTORE_H */

