/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_FDREGEX_H
#define KNO_FDREGEX_H 1
#ifndef KNO_FDREGEX_H_INFO
#define KNO_FDREGEX_H_INFO "include/kno/webtools.h"
#endif

enum KNO_REGEX_OP {
  rx_search, rx_zeromatch, rx_matchlen, rx_exactmatch, rx_matchpair,
  rx_matchstring};

KNO_EXPORT ssize_t kno_regex_op(enum KNO_REGEX_OP op,lispval pat,
			  u8_string s,size_t len,
			  int eflags);
KNO_EXPORT int kno_regex_test(lispval pat,u8_string s,ssize_t len);
KNO_EXPORT ssize_t kno_regex_match(lispval pat,u8_string s,ssize_t len);
KNO_EXPORT ssize_t kno_regex_matchlen(lispval pat,u8_string s,ssize_t len);
KNO_EXPORT off_t kno_regex_search(lispval pat,u8_string s,ssize_t len);

#endif /* KNO_FDREGEX_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
