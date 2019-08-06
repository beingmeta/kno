/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_STACKS_H
#define KNO_STACKS_H 1
#ifndef KNO_STACKS_H_INFO
#define KNO_STACKS_H_INFO "include/kno/stacks.h"
#endif

#ifndef KNO_INLINE_STACKS
#define KNO_INLINE_STACKS 0
#endif

#if HAVE_OBSTACK_H
#include <obstack.h>
#endif

/* Stack frames */

typedef struct KNO_STACK {
  KNO_CONS_HEADER; /* We're not using this right now */
  unsigned short stack_flags, stack_errflags;
  short stack_arglen, stack_buflen;

  lispval stack_op;
  kno_argvec stack_args;
  lispval *stack_buf;
  lispval stack_vals;
  struct KNO_LEXENV *stack_env;

  struct KNO_STACK *stack_caller;
  struct KNO_STACK *stack_root;
  int stack_crumb;
#if HAVE_OBSTACK_H
  struct obstack *stack_obstack;
#endif
  int stack_depth;
  lispval stack_source;
  u8_string stack_type, stack_label, stack_status, stack_src;
  long long threadid;} *kno_stack;
typedef struct KNO_LEXENV *kno_lexenv;
typedef struct KNO_LEXENV *kno_lexenv;

#define KNO_STACK_LIVE 0x01
#define KNO_STACK_RETVOID 0x02
#define KNO_STACK_NDCALL 0x04
#define KNO_STACK_TAILPOS 0x08
#define KNO_STACK_TAILCALL 0x10
#define KNO_STACK_DECREF_OP 0x100
#define KNO_STACK_FREE_LABEL 0x200
#define KNO_STACK_FREE_STATUS 0x400
#define KNO_STACK_FREE_SRC 0x800

#define KNO_STACK_LIVEP(stack) ( (stack->stack_flags) & (KNO_STACK_LIVE) )
#define KNO_STACK_TAILP(stack) ( (stack->stack_flags) & (KNO_STACK_TAILPOS) )
#define KNO_STACK_NDCALLP(stack) ( (stack->stack_flags) & (KNO_STACK_NDCALL) )

/* Stack error flags */

#define KNO_STACK_ERR_DEBUG      0x01   /* Return stack info */
#define KNO_STACK_ERR_DUMP       0x02   /* Dump stack info when critical */
#define KNO_STACK_ERR_CATCH      0x04   /* Errors *might* be caught */
#define KNO_STACK_ERR_CATCHALL   0x08   /* Errors **will** be caught */
#define KNO_STACK_ERR_RUSAGE     0x10   /* Include rusage in errpoint */
#define KNO_STACK_ERR_REQDATA    0x20   /* Include reqdata in errpoint */
#define KNO_STACK_ERR_THREAD     0x40   /* Include thread bindings in errpoint */
#define KNO_STACK_ERR_FULLSTACK  0x80   /* Dump entire stack (including environments) */
#define KNO_STACK_ERR_CALLSTACK  0x100  /* Dump only calls and EXPRS */

#define KNO_STACK_ERR_DEFAULT    \
  (KNO_STACK_ERR_DEBUG | KNO_STACK_ERR_DUMP | KNO_STACK_ERR_FULLSTACK)

#if (U8_USE_TLS)
KNO_EXPORT u8_tld_key kno_stackptr_key;
#define kno_stackptr ((struct KNO_STACK *)(u8_tld_get(kno_stackptr_key)))
#define set_call_stack(s) u8_tld_set(kno_stackptr_key,(s))
#elif (U8_USE__THREAD)
KNO_EXPORT __thread struct KNO_STACK *kno_stackptr;
#define set_call_stack(s) kno_stackptr = s
#else
#define set_call_stack(s) kno_stackptr = s
#endif

#define KNO_CHECK_STACK_ORDER(stack,caller)                      \
  ( (caller == NULL) ||                                         \
    ( (u8_stack_direction > 0) ? ( (stack) > (caller) ) :       \
      ( (stack) < (caller) ) ) )

#define KNO_SETUP_NAMED_STACK(name,caller,type,label,op) \
  struct KNO_STACK _ ## name;                            \
  struct KNO_STACK *name=&_ ## name;                     \
  assert( caller != name);                              \
  if (caller) _ ## name.stack_root=caller->stack_root;  \
  if (caller)                                           \
    _ ## name.stack_depth = 1 + caller->stack_depth;    \
  else  _ ## name.stack_depth = 0;                      \
  if (caller)                                           \
    _ ## name.stack_errflags = caller->stack_errflags;  \
  else _ ## name.stack_errflags = KNO_STACK_ERR_DEFAULT; \
  _ ## name.stack_flags=0x01;                           \
  _ ## name.stack_crumb=0x00;                           \
  if (caller)                                           \
    _ ## name.stack_src = caller->stack_src;            \
  else _ ## name.stack_src = NULL;                      \
  if (!(KNO_CHECK_STACK_ORDER(name,caller)))             \
    u8_raise("StackCorruption","KNO_SETUP_NAMED_STACK",  \
             label);                                    \
  _ ## name.stack_caller=caller;                        \
  _ ## name.stack_type=type;                            \
  _ ## name.stack_label=label;                          \
  _ ## name.stack_vals=KNO_EMPTY_CHOICE;                 \
  _ ## name.stack_source=KNO_VOID;                       \
  _ ## name.stack_op=op;                                \
  _ ## name.stack_status=NULL;                          \
  _ ## name.stack_arglen=0;                                   \
  _ ## name.stack_buflen=0;                                   \
  _ ## name.stack_buf = NULL;                          \
  _ ## name.stack_args = NULL;                          \
  _ ## name.stack_env=NULL

#define KNO_PUSH_STACK(name,type,label,op)              \
  KNO_SETUP_NAMED_STACK(name,_stack,type,label,op);     \
  set_call_stack(name)

#define KNO_NEW_STACK(caller,type,label,op)             \
  KNO_SETUP_NAMED_STACK(_stack,caller,type,label,op);     \
  set_call_stack(_stack)

#define KNO_ALLOCA_STACK(name)                                   \
  name=alloca(sizeof(struct KNO_STACK));                         \
  memset(name,0,sizeof(struct KNO_STACK));                       \
  assert( kno_stackptr != name);                                 \
  name->stack_caller = kno_stackptr;                             \
  if (name->stack_caller)                                       \
    name->stack_depth=1+name->stack_caller->stack_depth;        \
  if (name->stack_caller)                                       \
    name->stack_root=name->stack_caller->stack_root;            \
  if (name->stack_caller)                                       \
    name->stack_errflags =                                      \
      name->stack_caller->stack_errflags;                       \
  else name->stack_errflags = KNO_STACK_ERR_DEFAULT;             \
  name->stack_flags=0x01;                                       \
  name->stack_crumb=0x00;                                       \
  name->stack_type = "alloca";                                  \
  name->stack_vals = KNO_EMPTY_CHOICE;                           \
  name->stack_source = KNO_VOID;                                 \
  name->stack_op = KNO_VOID;                                     \
  set_call_stack(name)

#define KNO_INIT_STACK()                         \
  kno_init_cstack();                             \
  set_call_stack(((kno_stack)NULL))

#define KNO_INIT_THREAD_STACK()                          \
  kno_init_cstack();                                     \
  u8_byte label[128];                                   \
  u8_sprintf(label,128,"thread%d",u8_threadid());       \
  KNO_NEW_STACK(((struct KNO_STACK *)NULL),"thread",label,KNO_VOID)

KNO_EXPORT void _kno_free_stack(struct KNO_STACK *stack);
KNO_EXPORT void _kno_pop_stack(struct KNO_STACK *stack);

#define KNO_STACK_DECREF(v) \
  if (KNO_CONSP(v)) {KNO_ADD_TO_CHOICE(_stack->stack_values,v);} else {}

#if KNO_INLINE_STACKS
KNO_FASTOP void kno_free_stack(struct KNO_STACK *stack)
{
  if ((stack->stack_flags%2) == 0) return;

  if (U8_BITP(stack->stack_flags,KNO_STACK_DECREF_OP)) {
    kno_decref(stack->stack_op);
    stack->stack_op=KNO_VOID;}

  stack->stack_args = NULL;

  if ( (stack->stack_label) &&
       (U8_BITP(stack->stack_flags,KNO_STACK_FREE_LABEL)) ) {
    u8_free(stack->stack_label);
    stack->stack_label=NULL;}

  if ( (stack->stack_status) &&
       (U8_BITP(stack->stack_flags,KNO_STACK_FREE_STATUS)) ) {
    u8_free(stack->stack_status);
    stack->stack_status=NULL;}

  if ( (stack->stack_src) &&
       (U8_BITP(stack->stack_flags,KNO_STACK_FREE_SRC)) ) {
    u8_free(stack->stack_src);
    stack->stack_src=NULL;}

  if (KNO_CONSP(stack->stack_vals)) {
    kno_decref(stack->stack_vals);
    stack->stack_vals=KNO_EMPTY_CHOICE; }

  if (stack->stack_env) {
    kno_free_lexenv(stack->stack_env);
    stack->stack_env=NULL;}

  stack->stack_flags = 0;
}
KNO_FASTOP void kno_pop_stack(struct KNO_STACK *stack)
{
  if (U8_BITP(stack->stack_flags,KNO_STACK_LIVE)) {
    struct KNO_STACK *caller = stack->stack_caller;
    kno_free_stack(stack);
    set_call_stack(caller);}
}

#else /* not KNO_INLINE_STACKS */
#define kno_pop_stack(stack) (_kno_pop_stack(stack))
#endif

#define _return return kno_pop_stack(_stack),

#define kno_return(v) return (kno_pop_stack(_stack),(v))
#define kno_return_from(stack,v) return (kno_pop_stack(stack),(v))

#define KNO_WITH_STACK(label,caller,op,n,args) \
  struct KNO_STACK __stack, *_stack=&__stack; \
  kno_setup_stack(_stack,caller,label,op,n,args)

KNO_EXPORT lispval kno_get_backtrace(struct KNO_STACK *stack);

#endif /* KNO_STACKS_H */

