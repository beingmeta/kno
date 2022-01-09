/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_PROCPRIMS_H
#define KNO_PROCPRIMS_H 1
#ifndef KNO_PROCPRIMS_H_INFO
#define KNO_PROCPRIMS_H_INFO "include/kno/procprims.h"
#endif

typedef struct KNO_SUBPROC {
  KNO_ANNOTATED_HEADER;
  u8_string proc_id;
  pid_t proc_pid;
  int proc_status;
  int proc_loglevel;
  u8_string proc_progname;
  char **proc_argv, **proc_envp;
  double proc_started, proc_finished;
  lispval proc_stdin, proc_stdout, proc_stderr;
  lispval proc_opts;}
  *kno_subproc;

KNO_EXPORT lispval kno_rlimit_codes;

#endif

