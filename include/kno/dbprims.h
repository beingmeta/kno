/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2008 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_DBPRIMS_H
#define KNO_DBPRIMS_H 1
#ifndef KNO_DBPRIMS_H_INFO
#define KNO_DBPRIMS_H_INFO "include/kno/dbprims.h"
#endif

KNO_EXPORT lispval kno_fget(lispval frames,lispval slotids);
KNO_EXPORT lispval kno_ftest(lispval frames,lispval slotids,lispval values);
KNO_EXPORT lispval kno_assert(lispval frames,lispval slotids,lispval values);
KNO_EXPORT lispval kno_retract(lispval frames,lispval slotids,lispval values);

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
