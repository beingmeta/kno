/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_THREADS_H
#define KNO_THREADS_H 1
#ifndef KNO_THREADS_H_INFO
#define KNO_THREADS_H_INFO "include/kno/support.h"
#endif

KNO_EXPORT kno_ptr_type kno_thread_type;
KNO_EXPORT kno_ptr_type kno_condvar_type;

#define KNO_THREAD_DONE       0x0001
#define KNO_THREAD_ERROR      0x0002
#define KNO_EVAL_THREAD       0x0004
#define KNO_THREAD_TRACE_EXIT 0x0080
#define KNO_THREAD_QUIET_EXIT 0x0100

typedef struct KNO_THREAD_STRUCT {
  KNO_CONS_HEADER;
  pthread_t tid;
  long long threadid;
  int flags;
  pthread_attr_t attr;
  int *errnop;
  int loglevel;
  double started;
  double finished;
  lispval threadvars;
  struct KNO_STACK *thread_stackptr;
  struct KNO_THREAD_STRUCT *ring_left, *ring_right;
  lispval *resultptr, result;
  union {
    struct {lispval expr; kno_lexenv env;} evaldata;
    struct {lispval fn, *args; int n_args;} applydata;};} KNO_THREAD;
typedef struct KNO_THREAD_STRUCT *kno_thread_struct;

typedef struct KNO_CONDVAR {
  KNO_CONS_HEADER;
  u8_mutex kno_cvlock;
  u8_condvar kno_cvar;}
  KNO_CONDVAR;
typedef struct KNO_CONDVAR *kno_consed_condvar;

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/