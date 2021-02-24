/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_SEQPRIMS_H
#define KNO_SEQPRIMS_H 1
#ifndef KNO_SEQPRIMS_H_INFO
#define KNO_SEQPRIMS_H_INFO "include/kno/seqprims.h"
#endif

KNO_EXPORT lispval kno_mapseq(lispval fn,int n,kno_argvec seqs);
KNO_EXPORT lispval kno_foreach(lispval fn,int n,kno_argvec seqs);
KNO_EXPORT lispval kno_removeif(lispval test,lispval sequence,int invert);
KNO_EXPORT lispval kno_reduce(lispval fn,lispval sequence,lispval result);

#endif /*  KNO_SEQUENCES_H */

