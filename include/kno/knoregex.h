/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_FDREGEX_H
#define KNO_FDREGEX_H 1
#ifndef KNO_FDREGEX_H_INFO
#define KNO_FDREGEX_H_INFO "include/kno/webtools.h"
#endif

enum KNO_REGEX_OP {
  rx_search=1, rx_zeromatch=2, rx_matchlen=3, rx_exactmatch=4, rx_matchspan=5,
  rx_matchstring=6 };

KNO_EXPORT ssize_t kno_regex_op(enum KNO_REGEX_OP op,lispval pat,
			  u8_string s,size_t len,
			  int eflags);
KNO_EXPORT int kno_regex_test(lispval pat,u8_string s,ssize_t len);
KNO_EXPORT ssize_t kno_regex_match(lispval pat,u8_string s,ssize_t len);
KNO_EXPORT ssize_t kno_regex_matchlen(lispval pat,u8_string s,ssize_t len);
KNO_EXPORT off_t kno_regex_search(lispval pat,u8_string s,ssize_t len);

#endif /* KNO_FDREGEX_H */

