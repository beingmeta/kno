/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_PATHSTORE_H
#define KNO_PATHSTORE_H 1
#ifndef KNO_PATHSTORE_H_INFO
#define KNO_PATHSTORE_H_INFO "include/kno/eval.h"
#endif

typedef struct KNO_PATHSTORE {
  KNO_CONS_HEADER;
  u8_string knops_id;
  u8_string knops_mountpoint;
  ssize_t knops_mountpoint_len;
  u8_string knops_prefix;
  ssize_t knops_prefix_len;
  u8_string knops_cacheroot;
  unsigned int knops_flags;
  lispval knops_config;
  struct KNO_HASHTABLE knops_cache;
  struct KNO_PATHSTORE_HANDLERS {
    u8_string typeid; int n_handlers;
    int (*existsp)(struct KNO_PATHSTORE *,u8_string);
    lispval (*info)(struct KNO_PATHSTORE *,u8_string,int);
    lispval (*content)(struct KNO_PATHSTORE *,u8_string,u8_string,int);
    int (*close)(struct KNO_PATHSTORE *);
    int (*open)(struct KNO_PATHSTORE *);
    void (*recycle)(struct KNO_PATHSTORE *);}
    *knops_handlers;
  void *knops_data;} *kno_pathstore;

KNO_EXPORT int knops_existsp(kno_pathstore,u8_string);
KNO_EXPORT lispval knops_pathinfo(kno_pathstore,u8_string,int);
KNO_EXPORT lispval knops_content(kno_pathstore,u8_string,u8_string,int);

/* zpathstore prototypes */

KNO_EXPORT lispval kno_open_zpathstore(u8_string path,lispval opts);
KNO_EXPORT lispval kno_get_zipsource(u8_string path);

#endif /* KNO_PATHSTORE_H */

