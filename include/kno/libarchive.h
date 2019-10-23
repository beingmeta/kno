/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_ARCHIVE_H
#define KNO_ARCHIVE_H 1
#ifndef KNO_ARCHIVE_H_INFO
#define KNO_ARCHIVE_H_INFO "include/kno/eval.h"
#endif

#if HAVE_ARCHIVE_H
#include <archive.h>
#endif

KNO_EXPORT int kno_init_libarchive(void) KNO_LIBINIT_FN;

#endif /* KNO_ARCHIVE_H */

