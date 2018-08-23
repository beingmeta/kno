/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_ARCHIVE_H
#define FRAMERD_ARCHIVE_H 1
#ifndef FRAMERD_ARCHIVE_H_INFO
#define FRAMERD_ARCHIVE_H_INFO "include/framerd/eval.h"
#endif

#if HAVE_ARCHIVE_H
#include <archive.h>
#endif

FD_EXPORT int fd_init_libarchive(void) FD_LIBINIT_FN;

#endif /* FRAMERD_ARCHIVE_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
