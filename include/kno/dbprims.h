/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2008 beingmeta, inc.
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

