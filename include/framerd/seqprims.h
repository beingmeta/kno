/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_SEQPRIMS_H
#define FRAMERD_SEQPRIMS_H 1
#ifndef FRAMERD_SEQPRIMS_H_INFO
#define FRAMERD_SEQPRIMS_H_INFO "include/framerd/seqprims.h"
#endif

FD_EXPORT lispval fd_mapseq(lispval fn,int n,lispval *seqs);
FD_EXPORT lispval fd_foreach(lispval fn,int n,lispval *seqs);
FD_EXPORT lispval fd_removeif(lispval test,lispval sequence,int invert);
FD_EXPORT lispval fd_reduce(lispval fn,lispval sequence,lispval result);

#ifndef FD_SEQUENCEP
#define FD_SEQUENCEP(x) \
  ((FD_EMPTY_LISTP(x)) || ((fd_seqfns[FD_PTR_TYPE(x)])!=NULL))
#endif

#endif /*  FRAMERD_SEQUENCES_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
