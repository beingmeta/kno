/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_FUTURES_H
#define KNO_FUTURES_H 1
#ifndef KNO_FUTURES_H_INFO
#define KNO_FUTURES_H_INFO "include/kno/futures.h"
#endif

struct KNO_FUTURE;

typedef int (*future_handler)(struct KNO_FUTURE *,lispval value,int flags);

typedef struct KNO_FUTURE {
  KNO_ANNOTATED_HEADER;
  unsigned int future_bits;
  lispval future_value;
  lispval future_callbacks;
  lispval future_errfns;
  u8_mutex future_lock;
  u8_condvar future_condition;
  time_t future_updated;
  future_handler future_handler;
  lispval future_source;} KNO_FUTURE;
typedef struct KNO_FUTURE *kno_future;

#define KNO_FUTURE_FINALIZED     0x0001
#define KNO_FUTURE_EXCEPTION     0x0002
#define KNO_FUTURE_MONOTONIC     0x0004
#define KNO_FUTURE_ROBUST        0x0008
#define KNO_FUTURE_COMPUTABLE    0x0010

#define KNO_FUTURE_RESOLVEDP(f)   (((f)->future_value)!=(KNO_NULL))
#define KNO_FUTURE_PENDINGP(f)    (((f)->future_value)==(KNO_NULL))

#define KNO_FUTURE_FINALIZEDP(f)  (((f)->future_bits)&(KNO_FUTURE_FINALIZED))
#define KNO_FUTURE_EXCEPTIONP(f)  (((f)->future_bits)&(KNO_FUTURE_EXCEPTION))
#define KNO_FUTURE_PARTIALP(f)    (!(KNO_FUTURE_FINALIZEDP(f)))
#define KNO_FUTURE_FINALP(f)      (KNO_FUTURE_FINALIZEDP(f))
#define KNO_FUTURE_MONOTONICP(f)  (((f)->future_bits)&(KNO_FUTURE_MONOTONIC))
#define KNO_FUTURE_ROBUSTP(f)     (((f)->future_bits)&(KNO_FUTURE_ROBUST))

#define KNO_FUTURE_SET(f,flag)   (f)->future_bits |= (flag)
#define KNO_FUTURE_CLEAR(f,flag) (f)->future_bits &= (~(flag))
#define KNO_FUTURE_BITP(f,flag)  ( (((kno_future)f)->future_bits) & (flag) )

KNO_EXPORT struct KNO_FUTURE *kno_init_future
(struct KNO_FUTURE *future,lispval init,lispval source,
 future_handler updater,unsigned int flags);
KNO_EXPORT int kno_change_future(kno_future p,lispval value,unsigned int flags);
KNO_EXPORT lispval kno_force_future(kno_future f,lispval dflt);

#endif /* KNO_FUTURES_H */

