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

FD_EXPORT fdtype fd_fget(fdtype frames,fdtype slotids);
FD_EXPORT fdtype fd_ftest(fdtype frames,fdtype slotids,fdtype values);
FD_EXPORT fdtype fd_fassert(fdtype frames,fdtype slotids,fdtype values);
FD_EXPORT fdtype fd_fretract(fdtype frames,fdtype slotids,fdtype values);

#endif

