/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* This file supports various text processing primitives for FramerD.
   The central piece is a text pattern matcher implemented in
   src/texttools/match.c. */

#ifndef FRAMERD_TEXTTOOLS_H
#define FRAMERD_TEXTTOOLS_H 1
#ifndef FRAMERD_TEXTTOOLS_H_INFO
#define FRAMERD_TEXTTOOLS_H_INFO "include/framerd/texttools.h"
#endif

FD_EXPORT void fd_init_texttools(void) FD_LIBINIT_FN;
FD_EXPORT void fd_init_match_c(void);
FD_EXPORT void fd_init_phonetic_c(void);

FD_EXPORT fd_exception fd_BadExtractData;

typedef unsigned int fd_matchflags;

/* Matcher functions */

typedef fdtype
  (*tx_matchfn)(fdtype pat,fdtype next,fd_lispenv env,
		const u8_byte *string,u8_byteoff off,u8_byteoff lim,
		int flags);
typedef u8_byteoff
  (*tx_searchfn)(fdtype pat,fd_lispenv env,
		 const u8_byte *string,u8_byteoff off,u8_byteoff lim,
		 int flags);
typedef fdtype
  (*tx_extractfn)(fdtype pat,fdtype next,fd_lispenv env,
		  const u8_byte *string,u8_byteoff off,u8_byteoff lim,
		  int flags);

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
  (u8_string label,tx_matchfn m,tx_searchfn s,tx_extractfn ex);
FD_EXPORT int fd_matchdef(fdtype symbol,fdtype value);
FD_EXPORT fdtype fd_matchget(fdtype symbol,fd_lispenv env);
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

/* Other texttools stuff */

FD_EXPORT u8_string fd_soundex(u8_string);
FD_EXPORT u8_string fd_metaphone(u8_string,int);
FD_EXPORT fdtype fd_md5(fdtype string);

FD_EXPORT fdtype fd_words2vector(u8_string string,int keep_punct);
FD_EXPORT fdtype fd_words2list(u8_string string,int keep_punct);


#endif /* FRAMERD_TEXTTOOLS_H */
