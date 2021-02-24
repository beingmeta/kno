/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

/* This file supports various text processing primitives for Kno.
   The central piece is a text pattern matcher implemented in
   src/texttools/match.c. */

#ifndef KNO_TEXTTOOLS_H
#define KNO_TEXTTOOLS_H 1
#ifndef KNO_TEXTTOOLS_H_INFO
#define KNO_TEXTTOOLS_H_INFO "include/kno/texttools.h"
#endif

KNO_EXPORT void kno_init_texttools(void) KNO_LIBINIT_FN;
KNO_EXPORT void kno_init_match_c(void);
KNO_EXPORT void kno_init_phonetic_c(void);

KNO_EXPORT u8_condition kno_BadExtractData;

typedef unsigned int kno_matchflags;

/* Matcher functions */

typedef lispval
  (*tx_matchfn)(lispval pat,lispval next,kno_lexenv env,
		const u8_byte *string,u8_byteoff off,u8_byteoff lim,
		int flags);
typedef u8_byteoff
  (*tx_searchfn)(lispval pat,kno_lexenv env,
		 const u8_byte *string,u8_byteoff off,u8_byteoff lim,
		 int flags);
typedef lispval
  (*tx_extractfn)(lispval pat,lispval next,kno_lexenv env,
		  const u8_byte *string,u8_byteoff off,u8_byteoff lim,
		  int flags);

KNO_EXPORT int kno_text_match
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);

KNO_EXPORT lispval kno_text_matcher
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
KNO_EXPORT lispval kno_text_domatch
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
KNO_EXPORT lispval kno_text_extract
 (lispval pat,kno_lexenv env,
  u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
KNO_EXPORT lispval kno_text_doextract
 (lispval pat,lispval next,kno_lexenv env,
  u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
KNO_EXPORT u8_byteoff kno_text_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
KNO_EXPORT lispval kno_tx_closure(lispval expr,kno_lexenv env);

KNO_EXPORT void kno_add_match_operator
  (u8_string label,tx_matchfn m,tx_searchfn s,tx_extractfn ex);
KNO_EXPORT int kno_matchdef(lispval symbol,lispval value);
KNO_EXPORT lispval kno_matchget(lispval symbol,kno_lexenv env);
KNO_EXPORT u8_byteoff kno_text_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
KNO_EXPORT lispval kno_text_subst(lispval pat,lispval string);

typedef struct KNO_TXCLOSURE {
  KNO_CONS_HEADER;
  lispval kno_txpattern;
  kno_lexenv kno_txenv;} KNO_TXCLOSURE;
typedef struct KNO_TXCLOSURE *kno_txclosure;

struct KNO_TEXTMATCH_OPERATOR {
  lispval kno_matchop;
  tx_matchfn kno_matcher;
  tx_searchfn kno_searcher;
  tx_extractfn kno_extractor;};

#define KNO_MATCH_IGNORE_CASE       (1)
#define KNO_MATCH_IGNORE_DIACRITICS (KNO_MATCH_IGNORE_CASE<<1)
#define KNO_MATCH_COLLAPSE_SPACES   (KNO_MATCH_IGNORE_DIACRITICS<<1)
#define KNO_MATCH_BE_GREEDY         (KNO_MATCH_COLLAPSE_SPACES<<1)
#define KNO_MATCH_DO_BINDINGS       (KNO_MATCH_BE_GREEDY<<1)

KNO_EXPORT lispval kno_textclosure(lispval expr,kno_lexenv env);

KNO_EXPORT u8_condition kno_InternalMatchError, kno_MatchSyntaxError;
KNO_EXPORT u8_condition kno_TXInvalidPattern;

KNO_EXPORT kno_lisp_type kno_txclosure_type;

/* Other texttools stuff */

KNO_EXPORT u8_string kno_soundex(u8_string);
KNO_EXPORT u8_string kno_metaphone(u8_string,int);
KNO_EXPORT lispval kno_md5(lispval string);

KNO_EXPORT lispval kno_words2vector(u8_string string,int keep_punct);
KNO_EXPORT lispval kno_words2list(u8_string string,int keep_punct);

#endif /* KNO_TEXTTOOLS_H */

