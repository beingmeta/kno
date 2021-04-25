/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_HISTORY_H
#define KNO_HISTORY_H 1
#ifndef KNO_HISTORY_H_INFO
#define KNO_HISTORY_H_INFO "include/kno/eval.h"
#endif

KNO_EXPORT int kno_historyp(void);

KNO_EXPORT lispval kno_history_ref(lispval history,lispval ref);
KNO_EXPORT lispval kno_history_add(lispval history,lispval value,lispval ref);
KNO_EXPORT lispval kno_history_find(lispval history,lispval val);
KNO_EXPORT int kno_history_push(lispval history,lispval value);
KNO_EXPORT int kno_histpush(lispval value);

KNO_EXPORT lispval kno_histref(int ref);
KNO_EXPORT lispval kno_histfind(lispval value);
KNO_EXPORT lispval kno_make_history(int size);
KNO_EXPORT lispval kno_hist_init(int size);

KNO_EXPORT lispval kno_eval_histrefs(lispval obj,lispval history);
KNO_EXPORT lispval kno_eval_histref(lispval obj,lispval history);
KNO_EXPORT lispval kno_get_histref(lispval elts);

#define KNO_HISTORYP(x) (KNO_COMPOUND_TYPEP(x,KNOSYM_HISTORY_TAG))

#endif /* ndf KNO_HISTORY_H */

