/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_STACKS_H
#define FRAMERD_STACKS_H 1
#ifndef FRAMERD_STACKS_H_INFO
#define FRAMERD_STACKS_H_INFO "include/framerd/stacks.h"
#endif

#ifndef FD_INLINE_STACKS
#define FD_INLINE_STACKS 0
#endif

#if HAVE_OBSTACK_H
#include <obstack.h>
#endif

/* Stack frames */

typedef enum FD_STACK_CLEANOP {
  FD_FREE_MEMORY,
  FD_DECREF,
  FD_DECREF_PTRVAL,
  FD_UNLOCK_MUTEX,
  FD_UNLOCK_RWLOCK,
  FD_DECREF_VEC,
  FD_FREE_VEC,
  FD_CALLFN} fd_stack_cleanop;

#define FD_STACK_CLEANUP_QUANTUM 3

struct FD_STACK_CLEANUP {
  enum FD_STACK_CLEANOP cleanop;
  void *arg0, *arg1;};

typedef int (*fd_cleanupfn)(void *);

typedef struct FD_STACK {
  FD_CONS_HEADER; /* We're not using this right now */
  u8_string stack_type, stack_label, stack_status;
  long long threadid;
  int stack_depth;
  struct FD_STACK *stack_caller, *stack_root;

#if HAVE_OBSTACK_H
  struct obstack *stack_obstack;
#endif

  fdtype stack_op;
  fdtype *stack_args;
  fdtype stack_vals;
  short n_args;
  struct FD_ENVIRONMENT *stack_env;
  unsigned int stack_live:1;
  unsigned int stack_retvoid:1, stack_ndcall:1, stack_tail:1;
  unsigned int stack_free_label:1, stack_free_status:1;
  unsigned int stack_decref_op:1;
  struct FD_STACK_CLEANUP _cleanups[FD_STACK_CLEANUP_QUANTUM];
  struct FD_STACK_CLEANUP *cleanups;
  short n_cleanups;} *fd_stack;
typedef struct FD_ENVIRONMENT *fd_environment;
typedef struct FD_ENVIRONMENT *fd_lispenv;

#define FD_SETUP_NAMED_STACK(name,caller,type,label,op)	\
  struct FD_STACK _ ## name, *name=&_ ## name;		\
  memset(&_ ## name,0,sizeof(struct FD_STACK));		\
  if (caller) _ ## name.stack_root=caller->stack_root;	\
  if (caller)						\
    _ ## name.stack_depth = 1 + caller->stack_depth;	\
  _ ## name.stack_caller=caller;			\
  _ ## name.stack_type=type;				\
  _ ## name.stack_label=label;				\
  _ ## name.stack_op=op;				\
  _ ## name.stack_vals=FD_EMPTY_CHOICE;			\
  _ ## name.cleanups=_ ## name._cleanups;		\
  _ ## name.stack_live=1

#define FD_PUSH_STACK(name,type,label,op)	       \
  FD_SETUP_NAMED_STACK(name,_stack,type,label,op);     \
  set_call_stack(name)

#define FD_ALLOCA_STACK(name)					\
  name=alloca(sizeof(struct FD_STACK));				\
  memset(name,0,sizeof(struct FD_STACK));			\
  name->stack_caller = fd_stackptr;				\
  if (name->stack_caller)					\
    name->stack_depth=1+name->stack_caller->stack_depth;	\
  if (name->stack_caller)					\
    name->stack_root=name->stack_caller->stack_root;		\
  name->stack_type = "alloca";					\
  name->stack_vals = FD_EMPTY_CHOICE;				\
  name->stack_op = FD_VOID;					\
  name->cleanups = name->_cleanups;				\
  name->stack_live=1; \
  set_call_stack(name)


#if (U8_USE_TLS)
FD_EXPORT u8_tld_key fd_stackptr_key;
#define fd_stackptr ((struct FD_STACK *)(u8_tld_get(fd_stackptr_key)))
#define set_call_stack(s) u8_tld_set(fd_stackptr_key,(s))
#elif (U8_USE__THREAD)
FD_EXPORT __thread struct FD_STACK *fd_stackptr;
#define set_call_stack(s) fd_stackptr = s
#else
#define set_call_stack(s) fd_stackptr = s
#endif

FD_EXPORT void _fd_free_stack(struct FD_STACK *stack);
FD_EXPORT void _fd_pop_stack(struct FD_STACK *stack);
FD_EXPORT struct FD_STACK_CLEANUP *_fd_add_cleanup
(struct FD_STACK *stack,fd_stack_cleanop op,void *arg0,void *arg1);

#if FD_INLINE_STACKS
FD_FASTOP void fd_free_stack(struct FD_STACK *stack)
{
  if (stack->stack_live==0) return;

  if (stack->stack_decref_op) {
    fd_decref(stack->stack_op);
    stack->stack_op=FD_VOID;}

  if ( (stack->stack_free_label) && (stack->stack_label) ) {
    u8_free(stack->stack_label);
    stack->stack_label=NULL;
    stack->stack_free_label=0;}

  if ( (stack->stack_free_status) && (stack->stack_status) ) {
    u8_free(stack->stack_status);
    stack->stack_status=NULL;
    stack->stack_free_status=0;}

  if (stack->n_cleanups) {
    struct FD_STACK_CLEANUP *cleanups = stack->cleanups;
    int i = 0, n = stack->n_cleanups;
    while (i<n) {
      switch (cleanups[i].cleanop) {
      case FD_FREE_MEMORY:
	u8_free(cleanups[i].arg0); break;
      case FD_DECREF: {
	fdtype arg = (fdtype)cleanups[i].arg0;
	fd_decref(arg); break;}
      case FD_DECREF_PTRVAL: {
	fdtype *ptr = (fdtype *)cleanups[i].arg0;
	if (ptr) {
	  fdtype v=*ptr; fd_decref(v);}
	break;}
      case FD_UNLOCK_MUTEX: {
	u8_mutex *lock = (u8_mutex *)cleanups[i].arg0;
	u8_unlock_mutex(lock); break;}
      case FD_UNLOCK_RWLOCK: {
	u8_rwlock *lock = (u8_rwlock *)cleanups[i].arg0;
	u8_rw_unlock(lock); break;}
      case FD_DECREF_VEC: {
	fdtype *vec = (fdtype *)cleanups[i].arg0;
	ssize_t *sizep = (ssize_t *)cleanups[i].arg1;
	ssize_t size = *sizep;
	int i = 0; while (i<size) {
	  fdtype elt = vec[i++]; fd_decref(elt);}
	break;}
      case FD_FREE_VEC: {
	fdtype *vec = (fdtype *)cleanups[i].arg0;
	ssize_t *sizep = (ssize_t *)cleanups[i].arg1;
	ssize_t size = *sizep;
	int i = 0; while (i<size) {
	  fdtype elt = vec[i++]; fd_decref(elt);}
	u8_free(vec);
	break;}
      case FD_CALLFN: {
	fd_cleanupfn cleanup=(fd_cleanupfn)cleanups[i].arg0;
	cleanup(cleanups[i].arg1);
	break;}}
      i++;}
    if (cleanups != stack->_cleanups) u8_free(cleanups);}
  if (FD_CONSP(stack->stack_vals)) {
    fd_decref(stack->stack_vals);
    stack->stack_vals=FD_EMPTY_CHOICE; }
  if (stack->stack_env) {
    fd_free_environment(stack->stack_env);
    stack->stack_env=NULL;}
}
FD_FASTOP void fd_pop_stack(struct FD_STACK *stack)
{
  if (stack->stack_live) {
    struct FD_STACK *caller = stack->stack_caller;
    fd_free_stack(stack);
    stack->stack_live=0;
    set_call_stack(caller);}
}

FD_FASTOP
struct FD_STACK_CLEANUP *
fd_add_cleanup(struct FD_STACK *stack,fd_stack_cleanop op,
		 void *arg0, void* arg1)
{
  if ((stack->n_cleanups==0) ||
      (stack->n_cleanups%FD_STACK_CLEANUP_QUANTUM)) {
    int i = stack->n_cleanups++;
    stack->cleanups[i].cleanop = op;
    stack->cleanups[i].arg0 = arg0;
    stack->cleanups[i].arg1 = arg1;
    return &stack->cleanups[i];}
  else {
    int size = stack->n_cleanups;
    int new_size = size+FD_STACK_CLEANUP_QUANTUM;
    struct FD_STACK_CLEANUP *cleanups = stack->cleanups;
    struct FD_STACK_CLEANUP *new_cleanups=
      u8_alloc_n(new_size,struct FD_STACK_CLEANUP);
    memcpy(new_cleanups,cleanups,
	   size*sizeof(struct FD_STACK_CLEANUP));
    stack->cleanups = new_cleanups;
    if (size>FD_STACK_CLEANUP_QUANTUM)
      u8_free(cleanups);
    int i = stack->n_cleanups++;
    stack->cleanups[i].cleanop = op;
    stack->cleanups[i].arg0 = arg0;
    stack->cleanups[i].arg1 = arg1;
    return &stack->cleanups[i];}
}
#else /* not FD_INLINE_STACKS */
#define fd_pop_stack(stack) (_fd_pop_stack(stack))
#define fd_add_cleanup(stack,op,arg0,arg1) \
  (_fd_add_cleanup(stack,op,arg0,arg1))
#endif

#define fd_push_cleanup(stack,op,arg0,arg1) \
  (fd_add_cleanup(stack,op,(void *)arg0,(void *)arg1))

#define _return return fd_pop_stack(_stack),

#define fd_return(v) return (fd_pop_stack(_stack),(v))
#define fd_return_from(stack) return (fd_pop_stack(_stack),(v))

#define FD_WITH_STACK(label,caller,op,n,args) \
  struct FD_STACK __stack, *_stack=&__stack; \
  fd_setup_stack(_stack,caller,label,op,n,args)

FD_EXPORT fdtype fd_get_backtrace(struct FD_STACK *stack,fdtype base);

#endif /* FRAMERD_STACKS_H */
