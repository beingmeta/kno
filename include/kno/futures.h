/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_FUTURES_H
#define KNO_FUTURES_H 1
#ifndef KNO_FUTURES_H_INFO
#define KNO_FUTURES_H_INFO "include/kno/futures.h"
#endif

typedef lispval (*future_updatefn)(struct KNO_FUTURE *,lispval value,int flags);

typedef struct KNO_FUTURE {
  KNO_ANNOTATED_HEADER;
  int future_bits;
  lispval future_value;
  lispval future_callbacks;
  u8_mutex future_lock;
  u8_condvar future_condition;
  time_t future_updated;
  future_updatefn future_updater;
  lispval future_source;
  lispval future_context;} KNO_FUTURE;
typedef struct KNO_FUTURE *kno_future;

#define KNO_FUTURE_FINAL      0x0001
#define KNO_FUTURE_BROKEN     0x0002
#define KNO_FUTURE_PARTIAL    0x0004
/* Ignores errors */
#define KNO_FUTURE_ROBUST     0x0008

#define KNO_FUTURE_RESOLVEDP(p) (((p)->future_value)!=(KNO_NULL))

#define KNO_FUTURE_FINALP(p)    (((p)->future_bits)&(KNO_FUTURE_FINAL))
#define KNO_FUTURE_BROKENP(p)   (((p)->future_bits)&(KNO_FUTURE_BROKEN))
#define KNO_FUTURE_PARTIALP(p)  (((p)->future_bits)&(KNO_FUTURE_PARTIAL))
#define KNO_FUTURE_ROBUSTP(p)   (((p)->future_bits)&(KNO_FUTURE_ROBUST))

#define KNO_FUTURE_SET(p,flag) (p)->future_bits |= (flag)
#define KNO_FUTURE_CLEAR(p,flag) (p)->future_bits &= (~(flag))

KNO_EXPORT struct KNO_FUTURE *kno_init_future
(struct KNO_FUTURE *future,lispval init,lispval source,lispval context,
 future_updatefn updater,
 int flags);
KNO_EXPORT int kno_future_update(kno_future p,lispval value,int flags);

#endif /* KNO_FUTURES_H */

