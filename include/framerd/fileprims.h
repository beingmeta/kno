/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_FILEPRIMS_H
#define FRAMERD_FILEPRIMS_H 1
#ifndef FRAMERD_FILEPRIMS_H_INFO
#define FRAMERD_FILEPRIMS_H_INFO "include/framerd/fileprims.h"
#endif

FD_EXPORT int fd_update_file_modules(int force);
FD_EXPORT int fd_load_latest(u8_string filename,fd_lexenv env,u8_string base);
FD_EXPORT int fd_snapshot(fd_lexenv env,u8_string filename);
FD_EXPORT int fd_snapback(fd_lexenv env,u8_string filename);
FD_EXPORT u8_string fd_tempdir(u8_string arg,int keep);

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
