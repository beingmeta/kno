/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2018 beingmeta, inc.
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

FD_EXPORT u8_condition fd_BadExtractData;

typedef unsigned int fd_matchflags;

/* Matcher functions */

typedef lispval
  (*tx_matchfn)(lispval pat,lispval next,fd_lexenv env,
		const u8_byte *string,u8_byteoff off,u8_byteoff lim,
		int flags);
typedef u8_byteoff
  (*tx_searchfn)(lispval pat,fd_lexenv env,
		 const u8_byte *string,u8_byteoff off,u8_byteoff lim,
		 int flags);
typedef lispval
  (*tx_extractfn)(lispval pat,lispval next,fd_lexenv env,
		  const u8_byte *string,u8_byteoff off,u8_byteoff lim,
		  int flags);

FD_EXPORT int fd_text_match
  (lispval pat,fd_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);

FD_EXPORT lispval fd_text_matcher
  (lispval pat,fd_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
FD_EXPORT lispval fd_text_domatch
  (lispval pat,lispval next,fd_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
FD_EXPORT lispval fd_text_extract
 (lispval pat,fd_lexenv env,
  u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
FD_EXPORT lispval fd_text_doextract
 (lispval pat,lispval next,fd_lexenv env,
  u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
FD_EXPORT u8_byteoff fd_text_search
  (lispval pat,fd_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
FD_EXPORT lispval fd_tx_closure(lispval expr,fd_lexenv env);

FD_EXPORT void fd_add_match_operator
  (u8_string label,tx_matchfn m,tx_searchfn s,tx_extractfn ex);
FD_EXPORT int fd_matchdef(lispval symbol,lispval value);
FD_EXPORT lispval fd_matchget(lispval symbol,fd_lexenv env);
FD_EXPORT u8_byteoff fd_text_search
  (lispval pat,fd_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
FD_EXPORT lispval fd_text_subst(lispval pat,lispval string);

typedef struct FD_TXCLOSURE {
  FD_CONS_HEADER;
  lispval fd_txpattern;
  fd_lexenv fd_txenv;} FD_TXCLOSURE;
typedef struct FD_TXCLOSURE *fd_txclosure;

struct FD_TEXTMATCH_OPERATOR {
  lispval fd_matchop;
  tx_matchfn fd_matcher;
  tx_searchfn fd_searcher;
  tx_extractfn fd_extractor;};

#define FD_MATCH_IGNORE_CASE       (1)
#define FD_MATCH_IGNORE_DIACRITICS (FD_MATCH_IGNORE_CASE<<1)
#define FD_MATCH_COLLAPSE_SPACES   (FD_MATCH_IGNORE_DIACRITICS<<1)
#define FD_MATCH_BE_GREEDY         (FD_MATCH_COLLAPSE_SPACES<<1)
#define FD_MATCH_DO_BINDINGS       (FD_MATCH_BE_GREEDY<<1)

FD_EXPORT lispval fd_textclosure(lispval expr,fd_lexenv env);

FD_EXPORT u8_condition fd_InternalMatchError, fd_MatchSyntaxError;
FD_EXPORT u8_condition fd_TXInvalidPattern;

FD_EXPORT fd_ptr_type fd_txclosure_type;

/* Other texttools stuff */

FD_EXPORT u8_string fd_soundex(u8_string);
FD_EXPORT u8_string fd_metaphone(u8_string,int);
FD_EXPORT lispval fd_md5(lispval string);

FD_EXPORT lispval fd_words2vector(u8_string string,int keep_punct);
FD_EXPORT lispval fd_words2list(u8_string string,int keep_punct);

#endif /* FRAMERD_TEXTTOOLS_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
