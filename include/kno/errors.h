/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_ERRORS_H
#define KNO_ERRORS_H 1
#ifndef KNO_ERRORS_H_INFO
#define KNO_ERRORS_H_INFO "include/kno/errors.h"
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "defines.h"

/* Errors */

KNO_EXPORT lispval kno_simple_error
(u8_condition c,u8_context cxt,u8_string details,lispval irritant,
 u8_exception *push);
KNO_EXPORT lispval (*_kno_mkerr)(u8_condition c,u8_context caller,
				 u8_string details,lispval irritant,
				 u8_exception *push);
KNO_EXPORT U8_NOINLINE void kno_raise
(u8_condition c,u8_context cxt,u8_string details,lispval irritant);

KNO_EXPORT void kno_missing_error(u8_string details);

#define kno_seterr(condition,caller,details,irritant)	\
  _kno_mkerr(condition,caller,details,irritant,NULL)
#define kno_err(condition,caller,details,irritant) \
  (_kno_mkerr(condition,caller,details,irritant,NULL))

#endif /* ndef KNO_ERRORS_H */

