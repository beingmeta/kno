/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_SEQUENCES_H
#define KNO_SEQUENCES_H 1
#ifndef KNO_SEQUENCES_H_INFO
#define KNO_SEQUENCES_H_INFO "include/kno/sequences.h"
#endif

KNO_EXPORT u8_condition kno_RangeError;
KNO_EXPORT u8_condition kno_SecretData;
KNO_EXPORT u8_condition kno_NotASequence;

typedef struct KNO_SEQFNS {
  int (*len)(lispval x);
  lispval (*elt)(lispval x,int i);
  lispval (*slice)(lispval x,int i,int j);
  int (*position)(lispval key,lispval x,int i,int j);
  int (*search)(lispval key,lispval x,int i,int j);
  lispval *(*elts)(lispval x,int *);
  lispval (*make)(int,kno_argvec);
  int (*sequencep)(lispval);
} KNO_SEQFNS;

KNO_EXPORT struct KNO_SEQFNS *kno_seqfns[];

KNO_EXPORT int kno_seq_length(lispval x);
KNO_EXPORT lispval kno_seq_elt(lispval x,int i);
KNO_EXPORT lispval kno_seq2choice(lispval x);
KNO_EXPORT lispval kno_slice(lispval x,int start,int end);
KNO_EXPORT int kno_position(lispval key,lispval x,int start,int end);
KNO_EXPORT int kno_rposition(lispval key,lispval x,int start,int end);
KNO_EXPORT int kno_search(lispval key,lispval x,int start,int end);
KNO_EXPORT lispval kno_append(int n,kno_argvec sequences);

KNO_EXPORT lispval kno_reverse(lispval sequence);
KNO_EXPORT lispval kno_remove(lispval item,lispval sequence);
KNO_EXPORT lispval kno_removeif(lispval test,lispval sequence,int invert);

KNO_EXPORT int kno_generic_position(lispval key,lispval x,int start,int end);
KNO_EXPORT int kno_generic_search(lispval subseq,lispval seq,int start,int end);

KNO_EXPORT int _KNO_SEQUENCEP(lispval x);

#if KNO_EXTREME_PROFILING
#define KNO_SEQUENCEP _KNO_SEQUENCEP
#else
#define KNO_SEQUENCEP(x)						\
  ( (KNO_CONSP(x)) ?							\
    ( ( (KNO_CONS_TYPEOF(x) >= kno_string_type) &&			\
	(KNO_CONS_TYPEOF(x) <= kno_pair_type) ) ||			\
      ( (KNO_CONS_TYPEOF(x) == kno_compound_type) &&			\
	( (((kno_compound)x)->compound_seqoff) >= 0) ) ||		\
      ( (kno_seqfns[KNO_CONS_TYPEOF(x)] != NULL ) &&			\
	( (kno_seqfns[KNO_CONS_TYPEOF(x)]->sequencep == NULL ) ||	\
	  (kno_seqfns[KNO_CONS_TYPEOF(x)]->sequencep(x)) ) ) ) :       \
    (x == KNO_EMPTY_LIST) ? (1) :					\
    (KNO_IMMEDIATEP(x)) ?						\
    ( (kno_seqfns[KNO_IMMEDIATE_TYPE(x)] != NULL ) &&			\
      ( (kno_seqfns[KNO_IMMEDIATE_TYPE(x)]->sequencep == NULL ) ||	\
	(kno_seqfns[KNO_IMMEDIATE_TYPE(x)]->sequencep(x)) ) )  :	\
    (0))
#endif


lispval *kno_seq_elts(lispval seq,int *len);
lispval kno_makeseq(kno_lisp_type ctype,int n,kno_argvec v);

#endif /*  KNO_SEQUENCES_H */

