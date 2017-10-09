/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_THREADS_H
#define FRAMERD_THREADS_H 1
#ifndef FRAMERD_THREADS_H_INFO
#define FRAMERD_THREADS_H_INFO "include/framerd/support.h"
#endif

FD_EXPORT fd_ptr_type fd_thread_type;
FD_EXPORT fd_ptr_type fd_condvar_type;

#define FD_THREAD_DONE 1
#define FD_EVAL_THREAD 2
#define FD_THREAD_TRACE_EXIT 4
#define FD_THREAD_QUIET_EXIT 8
typedef struct FD_THREAD_STRUCT {
  FD_CONS_HEADER;
  pthread_t tid;
  long long threadid;
  int flags;
  int *errnop;
  int loglevel;
  double started;
  double finished;
  lispval threadvars;
  struct FD_STACK *thread_stackptr;
  lispval *resultptr, result;
  pthread_attr_t attr;
  union {
    struct {lispval expr; fd_lexenv env;} evaldata;
    struct {lispval fn, *args; int n_args;} applydata;};} FD_THREAD;
typedef struct FD_THREAD_STRUCT *fd_thread_struct;

typedef struct FD_CONDVAR {
  FD_CONS_HEADER;
  u8_mutex fd_cvlock;
  u8_condvar fd_cvar;}
  FD_CONDVAR;
typedef struct FD_CONDVAR *fd_consed_condvar;

#endif
