/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_SEQUENCES_H
#define FRAMERD_SEQUENCES_H 1
#ifndef FRAMERD_SEQUENCES_H_INFO
#define FRAMERD_SEQUENCES_H_INFO "include/framerd/sequences.h"
#endif

FD_EXPORT u8_condition fd_RangeError;

typedef struct FD_SEQFNS {
  int (*len)(lispval x);
  lispval (*elt)(lispval x,int i);
  lispval (*slice)(lispval x,int i,int j);
  int (*position)(lispval key,lispval x,int i,int j);
  int (*search)(lispval key,lispval x,int i,int j);
  lispval *(*elts)(lispval x,int *);
  lispval (*make)(int,lispval *);
} FD_SEQFNS;

FD_EXPORT struct FD_SEQFNS *fd_seqfns[];

FD_EXPORT int fd_seq_length(lispval x);
FD_EXPORT lispval fd_seq_elt(lispval x,int i);
FD_EXPORT lispval fd_seq2choice(lispval x);
FD_EXPORT lispval fd_slice(lispval x,int start,int end);
FD_EXPORT int fd_position(lispval key,lispval x,int start,int end);
FD_EXPORT int fd_rposition(lispval key,lispval x,int start,int end);
FD_EXPORT int fd_search(lispval key,lispval x,int start,int end);
FD_EXPORT lispval fd_append(int n,lispval *sequences);

FD_EXPORT lispval fd_reverse(lispval sequence);
FD_EXPORT lispval fd_remove(lispval item,lispval sequence);
FD_EXPORT lispval fd_removeif(lispval test,lispval sequence,int invert);

FD_EXPORT int fd_generic_position(lispval key,lispval x,int start,int end);
FD_EXPORT int fd_generic_search(lispval subseq,lispval seq,int start,int end);

#define FD_SEQUENCEP(x) \
  ((FD_EMPTY_LISTP(x)) || ((fd_seqfns[FD_PTR_TYPE(x)])!=NULL))

lispval *fd_seq_elts(lispval seq,int *len);
lispval fd_makeseq(fd_ptr_type ctype,int n,lispval *v);

#endif /*  FRAMERD_SEQUENCES_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
