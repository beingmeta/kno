/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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

#define FD_THREAD_DONE       0x0001
#define FD_THREAD_ERROR      0x0002
#define FD_EVAL_THREAD       0x0004
#define FD_THREAD_TRACE_EXIT 0x0080
#define FD_THREAD_QUIET_EXIT 0x0100

typedef struct FD_THREAD_STRUCT {
  FD_CONS_HEADER;
  pthread_t tid;
  long long threadid;
  int flags;
  pthread_attr_t attr;
  int *errnop;
  int loglevel;
  double started;
  double finished;
  lispval threadvars;
  struct FD_STACK *thread_stackptr;
  struct FD_THREAD_STRUCT *ring_left, *ring_right;
  lispval *resultptr, result;
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

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
