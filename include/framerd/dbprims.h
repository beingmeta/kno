/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2008 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_DBPRIMS_H
#define FRAMERD_DBPRIMS_H 1
#ifndef FRAMERD_DBPRIMS_H_INFO
#define FRAMERD_DBPRIMS_H_INFO "include/framerd/dbprims.h"
#endif

FD_EXPORT lispval fd_fget(lispval frames,lispval slotids);
FD_EXPORT lispval fd_ftest(lispval frames,lispval slotids,lispval values);
FD_EXPORT lispval fd_assert(lispval frames,lispval slotids,lispval values);
FD_EXPORT lispval fd_retract(lispval frames,lispval slotids,lispval values);

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
