/* -*- Mode: C; -*- */

/* Copyright (C) 2008 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_DBPRIMS_H
#define FDB_DBPRIMS_H 1
#ifndef FDB_DBPRIMS_H_INFO
#define FDB_DBPRIMS_H_INFO "include/framerd/dbprims.h"
#endif

FD_EXPORT fdtype fd_fget(fdtype frames,fdtype slotids);
FD_EXPORT fdtype fd_ftest(fdtype frames,fdtype slotids,fdtype values);
FD_EXPORT fdtype fd_fassert(fdtype frames,fdtype slotids,fdtype values);
FD_EXPORT fdtype fd_fretract(fdtype frames,fdtype slotids,fdtype values);

#endif

