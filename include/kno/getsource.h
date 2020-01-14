/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
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


#endif
