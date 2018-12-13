/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
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

typedef struct FD_STACK {
  FD_CONS_HEADER; /* We're not using this right now */
  u8_string stack_type, stack_label, stack_status, stack_src;
  long long threadid;
  int stack_depth;
  unsigned int stack_errflags;
  struct FD_STACK *stack_caller, *stack_root;

#if HAVE_OBSTACK_H
  struct obstack *stack_obstack;
#endif

  lispval stack_op;
  lispval *stack_args;
  lispval stack_source;
  lispval stack_vals;
  short n_args;
  struct FD_LEXENV *stack_env;
  unsigned int stack_flags;} *fd_stack;
typedef struct FD_LEXENV *fd_lexenv;
typedef struct FD_LEXENV *fd_lexenv;

#define FD_STACK_LIVE 0x01
#define FD_STACK_RETVOID 0x02
#define FD_STACK_TAIL 0x04
#define FD_STACK_NDCALL 0x08
#define FD_STACK_DECREF_OP 0x10
#define FD_STACK_FREE_LABEL 0x20
#define FD_STACK_FREE_STATUS 0x40
#define FD_STACK_FREE_SRC 0x80

/* Stack error flags */

#define FD_STACK_ERR_DEBUG      0x01   /* Return stack info */
#define FD_STACK_ERR_DUMP       0x02   /* Dump stack info when critical */
#define FD_STACK_ERR_CATCH      0x04   /* Errors *might* be caught */
#define FD_STACK_ERR_CATCHALL   0x08   /* Errors **will** be caught */
#define FD_STACK_ERR_RUSAGE     0x10   /* Include rusage in errpoint */
#define FD_STACK_ERR_REQDATA    0x20   /* Include reqdata in errpoint */
#define FD_STACK_ERR_THREAD     0x40   /* Include thread bindings in errpoint */
#define FD_STACK_ERR_FULLSTACK  0x80   /* Dump entire stack (including environments) */
#define FD_STACK_ERR_CALLSTACK  0x100  /* Dump only calls and EXPRS */

#define FD_STACK_ERR_DEFAULT    \
  (FD_STACK_ERR_DEBUG | FD_STACK_ERR_DUMP | FD_STACK_ERR_FULLSTACK)

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

#define FD_CHECK_STACK_ORDER(stack,caller)                      \
  ( (caller == NULL) ||                                         \
    ( (u8_stack_direction > 0) ? ( (stack) > (caller) ) :       \
      ( (stack) < (caller) ) ) )

#define FD_SETUP_NAMED_STACK(name,caller,type,label,op) \
  struct FD_STACK _ ## name;                            \
  struct FD_STACK *name=&_ ## name;                     \
  assert( caller != name);                              \
  if (caller) _ ## name.stack_root=caller->stack_root;  \
  if (caller)                                           \
    _ ## name.stack_depth = 1 + caller->stack_depth;    \
  else  _ ## name.stack_depth = 0;                      \
  if (caller)                                           \
    _ ## name.stack_errflags = caller->stack_errflags;  \
  else _ ## name.stack_errflags = FD_STACK_ERR_DEFAULT; \
  if (caller)                                          \
    _ ## name.stack_src = caller->stack_src;            \
  else _ ## name.stack_src = NULL;                      \
  if (!(FD_CHECK_STACK_ORDER(name,caller)))             \
    u8_raise("StackCorruption","FD_SETUP_NAMED_STACK",  \
             label);                                    \
  _ ## name.stack_caller=caller;                        \
  _ ## name.stack_type=type;                            \
  _ ## name.stack_label=label;                          \
  _ ## name.stack_vals=FD_EMPTY_CHOICE;                 \
  _ ## name.stack_source=FD_VOID;                       \
  _ ## name.stack_op=op;                                \
  _ ## name.stack_status=NULL;                          \
  _ ## name.stack_src=NULL;                             \
  _ ## name.n_args=0;                                   \
  _ ## name.stack_args = NULL;                          \
  _ ## name.stack_env=NULL;                             \
  _ ## name.stack_flags=0x01

#define FD_PUSH_STACK(name,type,label,op)              \
  FD_SETUP_NAMED_STACK(name,_stack,type,label,op);     \
  set_call_stack(name)

#define FD_NEW_STACK(caller,type,label,op)             \
  FD_SETUP_NAMED_STACK(_stack,caller,type,label,op);     \
  set_call_stack(_stack)

#define FD_ALLOCA_STACK(name)                                   \
  name=alloca(sizeof(struct FD_STACK));                         \
  memset(name,0,sizeof(struct FD_STACK));                       \
  assert( fd_stackptr != name);                                 \
  name->stack_caller = fd_stackptr;                             \
  if (name->stack_caller)                                       \
    name->stack_depth=1+name->stack_caller->stack_depth;        \
  if (name->stack_caller)                                       \
    name->stack_root=name->stack_caller->stack_root;            \
  if (name->stack_caller)                                       \
    name->stack_errflags =                                      \
      name->stack_caller->stack_errflags;                       \
  else name->stack_errflags = FD_STACK_ERR_DEFAULT;             \
  name->stack_type = "alloca";                                  \
  name->stack_vals = FD_EMPTY_CHOICE;                           \
  name->stack_source = FD_VOID;                                 \
  name->stack_op = FD_VOID;                                     \
  name->stack_flags=0x01; \
  set_call_stack(name)

#define FD_INIT_STACK()                         \
  fd_init_cstack();                             \
  set_call_stack(((fd_stack)NULL))

#define FD_INIT_THREAD_STACK()                          \
  fd_init_cstack();                                     \
  u8_byte label[128];                                   \
  u8_sprintf(label,128,"thread%d",u8_threadid());       \
  FD_NEW_STACK(((struct FD_STACK *)NULL),"thread",label,FD_VOID)

FD_EXPORT void _fd_free_stack(struct FD_STACK *stack);
FD_EXPORT void _fd_pop_stack(struct FD_STACK *stack);

#define FD_STACK_DECREF(v) \
  if (FD_CONSP(v)) {FD_ADD_TO_CHOICE(_stack->stack_values,v);} else {}

#if FD_INLINE_STACKS
FD_FASTOP void fd_free_stack(struct FD_STACK *stack)
{
  if ((stack->stack_flags%2) == 0) return;

  if (U8_BITP(stack->stack_flags,FD_STACK_DECREF_OP)) {
    fd_decref(stack->stack_op);
    stack->stack_op=FD_VOID;}

  if ( (stack->stack_label) &&
       (U8_BITP(stack->stack_flags,FD_STACK_FREE_LABEL)) ) {
    u8_free(stack->stack_label);
    stack->stack_label=NULL;}

  if ( (stack->stack_status) &&
       (U8_BITP(stack->stack_flags,FD_STACK_FREE_STATUS)) ) {
    u8_free(stack->stack_status);
    stack->stack_status=NULL;}

  if ( (stack->stack_src) &&
       (U8_BITP(stack->stack_flags,FD_STACK_FREE_SRC)) ) {
    u8_free(stack->stack_src);
    stack->stack_src=NULL;}

  if (FD_CONSP(stack->stack_vals)) {
    fd_decref(stack->stack_vals);
    stack->stack_vals=FD_EMPTY_CHOICE; }
  if (stack->stack_env) {
    fd_free_lexenv(stack->stack_env);
    stack->stack_env=NULL;}
  stack->stack_flags = 0;
}
FD_FASTOP void fd_pop_stack(struct FD_STACK *stack)
{
  if (U8_BITP(stack->stack_flags,FD_STACK_LIVE)) {
    struct FD_STACK *caller = stack->stack_caller;
    fd_free_stack(stack);
    set_call_stack(caller);}
}

#else /* not FD_INLINE_STACKS */
#define fd_pop_stack(stack) (_fd_pop_stack(stack))
#endif

#define _return return fd_pop_stack(_stack),

#define fd_return(v) return (fd_pop_stack(_stack),(v))
#define fd_return_from(stack,v) return (fd_pop_stack(stack),(v))

#define FD_WITH_STACK(label,caller,op,n,args) \
  struct FD_STACK __stack, *_stack=&__stack; \
  fd_setup_stack(_stack,caller,label,op,n,args)

FD_EXPORT lispval fd_get_backtrace(struct FD_STACK *stack);

#endif /* FRAMERD_STACKS_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
