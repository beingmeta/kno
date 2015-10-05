/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_SEQUENCES_H
#define FRAMERD_SEQUENCES_H 1
#ifndef FRAMERD_SEQUENCES_H_INFO
#define FRAMERD_SEQUENCES_H_INFO "include/framerd/sequences.h"
#endif

FD_EXPORT fd_exception fd_RangeError;

typedef struct FD_SEQFNS {
  int (*len)(fdtype x);
  fdtype (*elt)(fdtype x,int i);
  fdtype (*slice)(fdtype x,int i,int j);
  int (*position)(fdtype key,fdtype x,int i,int j);
  int (*search)(fdtype key,fdtype x,int i,int j);
  fdtype *(*elts)(fdtype x,int *);
  fdtype (*make)(int,fdtype *);
} FD_SEQFNS;

FD_EXPORT struct FD_SEQFNS *fd_seqfns[];

FD_EXPORT int fd_seq_length(fdtype x);
FD_EXPORT fdtype fd_seq_elt(fdtype x,int i);
FD_EXPORT fdtype fd_seq_elts(fdtype x);
FD_EXPORT fdtype fd_slice(fdtype x,int start,int end);
FD_EXPORT int fd_position(fdtype key,fdtype x,int start,int end);
FD_EXPORT int fd_search(fdtype key,fdtype x,int start,int end);

#define FD_SEQUENCEP(x) ((FD_EMPTY_LISTP(x)) || ((fd_seqfns[FD_PTR_TYPE(x)])!=NULL))

fdtype *fd_elts(fdtype seq,int *len);
fdtype fd_makeseq(fd_ptr_type ctype,int n,fdtype *v);

#endif /*  FRAMERD_SEQUENCES_H */
