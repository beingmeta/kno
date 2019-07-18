/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
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
  lispval subjob_stdin, subjob_stdout, subjob_stderr;}
  *kno_subjob;

KNO_EXPORT kno_lisp_type kno_subjob_type;

KNO_EXPORT lispval kno_rlimit_codes;

#endif

