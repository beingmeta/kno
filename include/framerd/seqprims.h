/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_SEQPRIMS_H
#define FRAMERD_SEQPRIMS_H 1
#ifndef FRAMERD_SEQPRIMS_H_INFO
#define FRAMERD_SEQPRIMS_H_INFO "include/framerd/sequences.h"
#endif

FD_EXPORT fdtype fd_mapseq(fdtype fn,int n,fdtype *seqs);
FD_EXPORT fdtype fd_foreach(fdtype fn,int n,fdtype *seqs);
FD_EXPORT fdtype fd_removeif(fdtype test,fdtype sequence,int invert);
FD_EXPORT fdtype fd_reduce(fdtype fn,fdtype sequence,fdtype result);


#define FD_SEQUENCEP(x) ((FD_EMPTY_LISTP(x)) || ((fd_seqfns[FD_PTR_TYPE(x)])!=NULL))

#endif /*  FRAMERD_SEQUENCES_H */
