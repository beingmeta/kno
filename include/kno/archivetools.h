/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_ARCHIVE_H
#define KNO_ARCHIVE_H 1
#ifndef KNO_ARCHIVE_H_INFO
#define KNO_ARCHIVE_H_INFO "include/kno/eval.h"
#endif

#if HAVE_ARCHIVE_H
#include <archive.h>
#endif

KNO_EXPORT int kno_init_archivetools(void) KNO_LIBINIT_FN;

#endif /* KNO_ARCHIVE_H */

