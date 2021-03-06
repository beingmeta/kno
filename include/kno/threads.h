/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_THREADS_H
#define KNO_THREADS_H 1
#ifndef KNO_THREADS_H_INFO
#define KNO_THREADS_H_INFO "include/kno/support.h"
#endif

#define KNO_THREAD_DONE        0x0001
#define KNO_THREAD_ERROR       0x0002
#define KNO_EVAL_THREAD        0x0004
#define KNO_THREAD_CANCELLED   0x0008
#define KNO_THREAD_TRACE_EXIT  0x0080
#define KNO_THREAD_QUIET_EXIT  0x0100
#define KNO_THREAD_TERMINATING 0x0200
#define KNO_THREAD_KEEPENV     0x10000

typedef struct KNO_THREAD {
  KNO_ANNOTATED_HEADER;
  pthread_t tid;
  long long threadid;
  lispval opts;
  int flags;
  u8_mutex exit_lock;
  u8_condvar exit_cvar;
  pthread_attr_t attr;
  int *errnop;
  int loglevel;
  double started;
  double finished;
  sigset_t sigmask;
  u8_condition interrupt;
  struct KNO_STACK *thread_stackptr;
  struct KNO_THREAD *ring_left, *ring_right;
  lispval *resultptr, result;
  union {
    struct {lispval expr; kno_lexenv env;} evaldata;
    struct {lispval fn, *args; int n_args;} applydata;};} KNO_THREAD;
typedef struct KNO_THREAD *kno_thread;

typedef struct KNO_SYNCHRONIZER {
  KNO_CONS_HEADER;
  enum { sync_condvar, sync_mutex, sync_rwlock } synctype;
  union {
    u8_rwlock rwlock;
    u8_mutex  mutex;
    struct {
      u8_mutex lock;
      u8_condvar cvar;}
      condvar;}
    obj;}
  KNO_SYNCHRONIZER;
typedef struct KNO_SYNCHORNIZER *kno_synchornizer;

#endif

