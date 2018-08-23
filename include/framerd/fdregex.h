/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_FDREGEX_H
#define FRAMERD_FDREGEX_H 1
#ifndef FRAMERD_FDREGEX_H_INFO
#define FRAMERD_FDREGEX_H_INFO "include/framerd/fdweb.h"
#endif

enum FD_REGEX_OP {
  rx_search, rx_zeromatch, rx_matchlen, rx_exactmatch, rx_matchpair,
  rx_matchstring};

FD_EXPORT ssize_t fd_regex_op(enum FD_REGEX_OP op,lispval pat,
			  u8_string s,size_t len,
			  int eflags);
FD_EXPORT int fd_regex_test(lispval pat,u8_string s,ssize_t len);
FD_EXPORT ssize_t fd_regex_match(lispval pat,u8_string s,ssize_t len);
FD_EXPORT ssize_t fd_regex_matchlen(lispval pat,u8_string s,ssize_t len);
FD_EXPORT off_t fd_regex_search(lispval pat,u8_string s,ssize_t len);

#endif /* FRAMERD_FDREGEX_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
