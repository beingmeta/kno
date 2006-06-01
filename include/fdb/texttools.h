/* -*- Mode: C; -*- */

/* Copyright (C) 2005-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

/* This file supports various text processing primitives for FDB.
   The central piece is a text pattern matcher implemented in
   src/texttools/match.c. */

#ifndef FDB_TEXTTOOLS_H
#define FDB_TEXTTOOLS_H 1
#define FDB_TEXTTOOLS_H_VERSION \
   "$Id: texttools.h,v 1.11 2006/02/01 15:57:44 haase Exp $"

FD_EXPORT void fd_init_texttools(void) FD_LIBINIT_FN;

FD_EXPORT fd_exception fd_BadExtractData;

typedef unsigned int fd_matchflags;

/* Matcher functions */

typedef fdtype
  (*tx_matchfn)(fdtype pat,fdtype next,fd_lispenv env,u8_byte *string,
		u8_byteoff off,u8_byteoff lim,int flags);
typedef u8_byteoff
  (*tx_searchfn)(fdtype pat,fd_lispenv env,u8_byte *string,
		 u8_byteoff off,u8_byteoff lim,int flags);
typedef fdtype
  (*tx_extractfn)(fdtype pat,fdtype next,fd_lispenv env,u8_byte *string,
		  u8_byteoff off,u8_byteoff lim,int flags);

FD_EXPORT int fd_text_match
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);

FD_EXPORT fdtype fd_text_matcher
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
FD_EXPORT fdtype fd_text_domatch
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
FD_EXPORT fdtype fd_text_extract
 (fdtype pat,fd_lispenv env,
  u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
FD_EXPORT fdtype fd_text_doextract
 (fdtype pat,fdtype next,fd_lispenv env,
  u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
FD_EXPORT u8_byteoff fd_text_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
FD_EXPORT fdtype fd_tx_closure(fdtype expr,fd_lispenv env);

FD_EXPORT void fd_add_match_operator
  (u8_string label,
   tx_matchfn matcher,tx_searchfn searcher,tx_extractfn extract);
FD_EXPORT u8_byteoff fd_text_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
FD_EXPORT fdtype fd_text_subst(fdtype pat,fdtype string);

typedef struct FD_TXCLOSURE {
  FD_CONS_HEADER;
  fdtype pattern;
  fd_lispenv env;} FD_TXCLOSURE;
typedef struct FD_TXCLOSURE *fd_txclosure;

struct FD_TEXTMATCH_OPERATOR {
  fdtype symbol;
  tx_matchfn matcher;
  tx_searchfn searcher;
  tx_extractfn extract;};

#define FD_MATCH_IGNORE_CASE       (1)
#define FD_MATCH_IGNORE_DIACRITICS (FD_MATCH_IGNORE_CASE<<1)
#define FD_MATCH_COLLAPSE_SPACES   (FD_MATCH_IGNORE_DIACRITICS<<1)
#define FD_MATCH_BE_GREEDY         (FD_MATCH_COLLAPSE_SPACES<<1)
#define FD_MATCH_DO_BINDINGS       (FD_MATCH_BE_GREEDY<<1)

FD_EXPORT fdtype fd_textclosure(fdtype expr,fd_lispenv env);

FD_EXPORT fd_exception fd_InternalMatchError, fd_MatchSyntaxError;
FD_EXPORT fd_exception fd_TXInvalidPattern;

FD_EXPORT fd_ptr_type fd_txclosure_type;

#endif /* FDB_TEXTTOOLS_H */
