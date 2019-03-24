/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_PROCPRIMS_H
#define FRAMERD_PROCPRIMS_H 1
#ifndef FRAMERD_PROCPRIMS_H_INFO
#define FRAMERD_PROCPRIMS_H_INFO "include/framerd/procprims.h"
#endif

typedef struct FD_SUBJOB {
  FD_CONS_HEADER;
  u8_string subjob_id;
  pid_t subjob_pid;
  lispval subjob_stdin, subjob_stdout, subjob_stderr;}
  *fd_subjob;

FD_EXPORT fd_ptr_type fd_subjob_type;

FD_EXPORT lispval fd_rlimit_codes;

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
