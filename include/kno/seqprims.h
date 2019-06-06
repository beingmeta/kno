/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_SEQPRIMS_H
#define KNO_SEQPRIMS_H 1
#ifndef KNO_SEQPRIMS_H_INFO
#define KNO_SEQPRIMS_H_INFO "include/kno/seqprims.h"
#endif

KNO_EXPORT lispval kno_mapseq(lispval fn,int n,lispval *seqs);
KNO_EXPORT lispval kno_foreach(lispval fn,int n,lispval *seqs);
KNO_EXPORT lispval kno_removeif(lispval test,lispval sequence,int invert);
KNO_EXPORT lispval kno_reduce(lispval fn,lispval sequence,lispval result);

#endif /*  KNO_SEQUENCES_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
