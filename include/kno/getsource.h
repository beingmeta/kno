/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_GETSOURCE_H
#define KNO_GETSOURCE_H 1
#ifndef KNO_GETSOURCE_H_INFO
#define KNO_GETSOURCE_H_INFO "include/kno/getsource.h"
#endif

typedef struct KNO_SOURCEFN {
  u8_string (*getsource)(int op,lispval,u8_string,u8_string *,time_t *timep,
			 ssize_t *sizep,void *);
  void *getsource_data;
  struct KNO_SOURCEFN *getsource_next;} KNO_SOURCEFN;
typedef struct KNO_SOURCEFN *kno_sourcefn;

KNO_EXPORT u8_string kno_get_source(u8_string,u8_string,u8_string *,
				    time_t *,ssize_t *);
KNO_EXPORT int kno_probe_source(u8_string,u8_string *,time_t *,ssize_t *);
KNO_EXPORT void kno_register_sourcefn
(u8_string (*fn)(int op,lispval,u8_string,u8_string *,time_t *,ssize_t *,void *),
 void *sourcefn_data);

#define KNO_ZIPSOURCEP(x) (KNO_RAW_TYPEP(x,KNOSYM_ZIPSOURCE))
KNO_EXPORT int kno_zipsource_existsp(lispval zs,u8_string path);
KNO_EXPORT lispval kno_zipsource_info(lispval zs,u8_string path,int follow);
KNO_EXPORT lispval kno_zipsource_content
(lispval zs,u8_string path,u8_string enc,int follow);
KNO_EXPORT lispval kno_open_zipsource(u8_string path,lispval opts);

#endif
