/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_FILEPRIMS_H
#define KNO_FILEPRIMS_H 1
#ifndef KNO_FILEPRIMS_H_INFO
#define KNO_FILEPRIMS_H_INFO "include/kno/fileprims.h"
#endif

KNO_EXPORT int kno_update_file_modules(int force);
KNO_EXPORT int kno_load_latest(u8_string filename,kno_lexenv env,u8_string base);
KNO_EXPORT int kno_snapshot(kno_lexenv env,u8_string filename);
KNO_EXPORT int kno_snapback(kno_lexenv env,u8_string filename);
KNO_EXPORT u8_string kno_tempdir(u8_string arg,int keep);

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
