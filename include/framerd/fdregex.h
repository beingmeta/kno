/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2013 beingmeta, inc.
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

#endif /* FRAMERD_FDREGEX_H */
