/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
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

struct KNO_STACK_HEAD {
  long long stack_threadid;
  unsigned int stack_flags;
  struct obstack *stack_obstack; /* Unused */
};

#define KNO_STACK_HEADER				\
  KNO_CONS_HEADER; /* We're not using this right now */ \
  int stack_depth, stack_size;				\
  struct KNO_STACK_HEAD *stack_head;			\
  struct KNO_STACK *stack_caller;			\
  int (*stack_cleanupfn)(struct KNO_STACK *);		\
  lispval (*stack_dumpfn)(struct KNO_STACK *);		\
  unsigned int stack_bits, stack_flags, stack_crumb;	\
  u8_string stack_label, stack_file;			\
  lispval stack_refs, *stack_resultp;			\
  lispval stack_op;					\
  kno_argvec stack_args;				\
  unsigned int stack_width

typedef struct KNO_STACK {
  KNO_STACK_HEADER;
} KNO_STACK;
typedef struct KNO_STACK *kno_stack;

typedef int (*kno_stack_cleanupfn)(struct KNO_STACK *);
typedef lispval (*kno_stack_dumpfn)(struct KNO_STACK *);

typedef struct KNO_SCHEME_STACK {
  KNO_STACK_HEADER;
  /* Eval specific */
  lispval stack_source, stack_expr, stack_point;
  struct KNO_LEXENV *stack_env;
} KNO_SCHEME_STACK;
typedef struct KNO_SCHEME_STACK *kno_scheme_stack;

KNO_EXPORT u8_string kno_stack_types[8];

typedef enum KNO_STACK_TYPE
  {kno_dead_stack  = 0x00,
   kno_apply_stack = 0x01,
   kno_iter_stack  = 0x02,
   kno_eval_stack  = 0x03,
   kno_stack_type4 = 0x04,
   kno_stack_type5 = 0x05,
   kno_stack_type6 = 0x06,
   kno_stack_type7 = 0x07} kno_stack_type;

#define KNO_STACK_TYPE_MASK 0x7
#define KNO_STACK_TYPE(stack) \
  ((kno_stack_type)((stack->stack_bits)&(KNO_STACK_TYPE_MASK)))

#define KNO_STACK_TAILPOS        0x0008
#define KNO_STACK_NDCALL         0x0010

#define KNO_STACK_DECREF_OP      0x0100
#define KNO_STACK_DECREF_ARGS    0x0200
#define KNO_STACK_FREE_ARGS      0x0400
#define KNO_STACK_FREE_LABEL     0x1000
#define KNO_STACK_FREE_SRC       0x2000

#define KNO_STACK_NEEDS_CLEANUP						\
  (KNO_STACK_DECREF_OP|KNO_STACK_DECREF_ARGS|KNO_STACK_FREE_ARGS|	\
   KNO_STACK_FREE_LABEL|KNO_STACK_FREE_SRC)

/* Stack error flags */

#define KNO_STACK_ERR_DEBUG      0x0001   /* Return stack info */
#define KNO_STACK_ERR_DUMP       0x0002   /* Dump stack info when critical */
#define KNO_STACK_ERR_CATCH      0x0004   /* Errors *might* be caught */
#define KNO_STACK_ERR_CATCHALL   0x0008   /* Errors **will** be caught */
#define KNO_STACK_ERR_RUSAGE     0x0010   /* Include rusage in errpoint */
#define KNO_STACK_ERR_REQDATA    0x0020   /* Include reqdata in errpoint */
#define KNO_STACK_ERR_THREAD     0x0040   /* Include thread bindings in errpoint */
#define KNO_STACK_ERR_FULLSTACK  0x0080   /* Dump entire stack (including environments) */
#define KNO_STACK_ERR_CALLSTACK  0x0100  /* Dump only calls and EXPRS */

#define KNO_STACK_NO_PROFILING   0x010000  /* Don't check for profiling */
#define KNO_STACK_NO_CONTOURS    0x020000  /* Don't have apply maintain error handling contours */
/* Not currently used */
#define KNO_STACK_NO_PUSHING     0x040000  /* Don't use the stack at all */

#define KNO_STACK_ERR_DEFAULT    \
  (KNO_STACK_ERR_DEBUG | KNO_STACK_ERR_DUMP | KNO_STACK_ERR_FULLSTACK)
#define KNO_STACK_CALL_MASK    \
  (KNO_STACK_NO_PROFILING | KNO_STACK_NO_CONTOURS | KNO_STACK_NO_PUSHING)

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

#define KNO_SETUP_STACK(stack,label,bits)	\
  (stack)->stack_size   = sizeof(*(stack));	\
  (stack)->stack_label  = label;		\
  (stack)->stack_bits   = bits;			\
  (stack)->stack_refs   = KNO_EMPTY;		\
  (stack)->stack_op     = KNO_VOID

#define KNO_STACK_SET_CALLER(stack,caller)		\
  (stack)->stack_caller = caller;			\
  (stack)->stack_depth  = (caller)->stack_depth+1;	\
  (stack)->stack_head   = caller->stack_head;		\
  (stack)->stack_flags  = caller->stack_flags;

#define KNO_STACK_SET_CALL(stack,op,n,args)		\
  (stack)->stack_op    = op;				\
  (stack)->stack_width = n;				\
  (stack)->stack_args  = args

#define KNO_PUSH_STACK(stack)              \
  KNO_STACK_SET_CALLER(stack,kno_stackptr);	\
  set_call_stack(stack);

#define KNO_INIT_STACK_ROOT()			 \
  kno_init_cstack();                             \
  set_call_stack(((kno_stack)NULL))

#define KNO_INIT_THREAD_STACK()                          \
  kno_init_cstack();                                     \
  u8_byte label[128];                                   \
  u8_sprintf(label,128,"thread%d",u8_threadid());       \
  KNO_NEW_STACK(((struct KNO_STACK *)NULL),"thread",label,KNO_VOID)

KNO_EXPORT void _kno_free_stack(struct KNO_STACK *stack);
KNO_EXPORT void _kno_pop_stack(struct KNO_STACK *stack);

#define KNO_STACK_REF(stack,v)  \
  if (KNO_CONSP(v)) {KNO_ADD_TO_CHOICE((stack)->stack_refs,(v));} else {}
#define KNO_STACK_DECREF(v) \
  if (KNO_CONSP(v)) {KNO_ADD_TO_CHOICE(_stack->stack_refs,v);} else {}

#if KNO_INLINE_STACKS
KNO_FASTOP void kno_free_stack(struct KNO_STACK *stack)
{
  kno_stack_type type = KNO_STACK_TYPE(stack);

  if (type == kno_dead_stack) return;

  int rv = (stack->stack_cleanupfn) ?
    ( (stack->stack_cleanupfn)(stack) ) : (0);
  if (rv != 0) return;

  if ((stack->stack_bits)&(KNO_STACK_NEEDS_CLEANUP)) {
    if ((stack->stack_bits)&(KNO_STACK_FREE_LABEL)) {
      u8_free(stack->stack_label);
      stack->stack_label=NULL;}
    if ((stack->stack_bits)&(KNO_STACK_FREE_SRC)) {
      u8_free(stack->stack_file);
      stack->stack_file=NULL;}

    if ((stack->stack_bits)&(KNO_STACK_DECREF_OP)) {
      lispval op = stack->stack_op; stack->stack_op=KNO_VOID;
      kno_decref(op);}

    if ((stack->stack_bits)&(KNO_STACK_DECREF_ARGS)) {
      int i = 0, width = stack->stack_width;
      lispval *args = (lispval *)stack->stack_args;
      while (i<width) {
	lispval arg = args[i];
	args[i++] = KNO_VOID;
	kno_decref(arg);}}
    if ((stack->stack_bits)&(KNO_STACK_FREE_ARGS))
      u8_free(stack->stack_args);}
  stack->stack_args = NULL;

  if (KNO_CONSP(stack->stack_refs)) {
    kno_decref(stack->stack_refs);
    stack->stack_refs=KNO_EMPTY_CHOICE; }
}
KNO_FASTOP void kno_pop_stack(struct KNO_STACK *stack)
{
  if (KNO_STACK_TYPE(stack)) {
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

