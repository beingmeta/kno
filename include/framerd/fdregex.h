/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_FDREGEX_H
#define FRAMERD_FDREGEX_H 1
#ifndef FRAMERD_FDREGEX_H_INFO
#define FRAMERD_FDREGEX_H_INFO "include/framerd/fdweb.h"
#endif

#include <regex.h>

FD_EXPORT fd_exception fd_RegexError;

typedef struct FD_REGEX {
  FD_CONS_HEADER;
  u8_string src; int flags;
  u8_mutex lock; int active;
  regex_t compiled;} FD_REGEX;
typedef struct FD_REGEX *fd_regex;

FD_EXPORT fdtype fd_make_regex(u8_string src,int flags);

enum FD_REGEX_OP {
  rx_search, rx_zeromatch, rx_matchlen, rx_exactmatch, rx_matchpair,
  rx_matchstring};

FD_EXPORT int fd_regex_op(enum FD_REGEX_OP op,fdtype pat,
			  u8_string s,size_t len,
			  int eflags);
FD_EXPORT int fd_regex_test(fdtype pat,u8_string s,ssize_t len);
FD_EXPORT int fd_regex_match(fdtype pat,u8_string s,ssize_t len);
FD_EXPORT int fd_regex_search(fdtype pat,u8_string s,ssize_t len);
FD_EXPORT int fd_regex_matchlen(fdtype pat,u8_string s,ssize_t len);

#endif /* FRAMERD_FDREGEX_H */
