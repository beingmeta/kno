/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_SEQUENCES_H
#define KNO_SEQUENCES_H 1
#ifndef KNO_SEQUENCES_H_INFO
#define KNO_SEQUENCES_H_INFO "include/kno/sequences.h"
#endif

KNO_EXPORT u8_condition kno_RangeError;

typedef struct KNO_SEQFNS {
  int (*len)(lispval x);
  lispval (*elt)(lispval x,int i);
  lispval (*slice)(lispval x,int i,int j);
  int (*position)(lispval key,lispval x,int i,int j);
  int (*search)(lispval key,lispval x,int i,int j);
  lispval *(*elts)(lispval x,int *);
  lispval (*make)(int,lispval *);
} KNO_SEQFNS;

KNO_EXPORT struct KNO_SEQFNS *kno_seqfns[];

KNO_EXPORT int kno_seq_length(lispval x);
KNO_EXPORT lispval kno_seq_elt(lispval x,int i);
KNO_EXPORT lispval kno_seq2choice(lispval x);
KNO_EXPORT lispval kno_slice(lispval x,int start,int end);
KNO_EXPORT int kno_position(lispval key,lispval x,int start,int end);
KNO_EXPORT int kno_rposition(lispval key,lispval x,int start,int end);
KNO_EXPORT int kno_search(lispval key,lispval x,int start,int end);
KNO_EXPORT lispval kno_append(int n,lispval *sequences);

KNO_EXPORT lispval kno_reverse(lispval sequence);
KNO_EXPORT lispval kno_remove(lispval item,lispval sequence);
KNO_EXPORT lispval kno_removeif(lispval test,lispval sequence,int invert);

KNO_EXPORT int kno_generic_position(lispval key,lispval x,int start,int end);
KNO_EXPORT int kno_generic_search(lispval subseq,lispval seq,int start,int end);

#define KNO_SEQUENCEP(x) \
  ((KNO_EMPTY_LISTP(x)) || ((kno_seqfns[KNO_PTR_TYPE(x)])!=NULL))

lispval *kno_seq_elts(lispval seq,int *len);
lispval kno_makeseq(kno_ptr_type ctype,int n,lispval *v);

#endif /*  KNO_SEQUENCES_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
