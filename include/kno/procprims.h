/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_PROCPRIMS_H
#define KNO_PROCPRIMS_H 1
#ifndef KNO_PROCPRIMS_H_INFO
#define KNO_PROCPRIMS_H_INFO "include/kno/procprims.h"
#endif

typedef struct KNO_SUBJOB {
  KNO_CONS_HEADER;
  u8_string subjob_id;
  pid_t subjob_pid;
  lispval subjob_stdin, subjob_stdout, subjob_stderr;
  lispval subjob_opts;}
  *kno_subjob;

KNO_EXPORT lispval kno_rlimit_codes;

#endif

