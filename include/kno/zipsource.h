/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_ZIPSOURCE_H
#define KNO_ZIPSOURCE_H 1
#ifndef KNO_ZIPSOURCE_H_INFO
#define KNO_ZIPSOURCE_H_INFO "include/kno/zipsource.h"
#endif

#define KNO_ZIPSOURCEP(x) (KNO_RAW_TYPEP(x,KNOSYM_ZIPSOURCE))

KNO_EXPORT int kno_zipsource_existsp(lispval zs,u8_string path);
KNO_EXPORT lispval kno_zipsource_info(lispval zs,u8_string path,int follow);
KNO_EXPORT lispval kno_zipsource_content
(lispval zs,u8_string path,u8_string enc,int follow);

KNO_EXPORT lispval kno_open_zipsource(u8_string path,lispval opts);
KNO_EXPORT lispval kno_get_zipsource(u8_string path);

#endif
