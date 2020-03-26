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
  unsigned int stack_depth, stack_size;			\
  struct KNO_STACK *stack_caller;			\
  lispval (*stack_dumpfn)(struct KNO_STACK *);		\
  unsigned int stack_bits, stack_flags, stack_crumb;	\
  u8_string stack_label, stack_file;			\
  lispval *stack_refs; int stack_n_refs, stack_n_max;	\
  lispval stack_point, eval_source, eval_context;	\
  unsigned int stack_width, stack_argc;			\
  kno_argvec stack_args;				\
  struct KNO_LEXENV *eval_env


typedef struct KNO_STACK {
  KNO_STACK_HEADER;
} KNO_STACK;
typedef struct KNO_STACK *kno_stack;

typedef lispval (*kno_stack_dumpfn)(struct KNO_STACK *);

KNO_EXPORT u8_string kno_stack_types[8];

typedef enum KNO_STACK_TYPE
  {kno_null_stacktype  = 0x00,
   kno_apply_stack = 0x01,
   kno_iter_stacktype  = 0x02,
   kno_reduce_stack  = 0x03,
   /* Reserved for future use */
   kno_stacktype4      = 0x04,
   kno_stacktype5      = 0x05,
   kno_stacktype6      = 0x06,
   kno_stacktype7      = 0x07} kno_stack_type;

#define KNO_STACK_TYPE_MASK 0x7
#define KNO_STACK_TYPE(stack) \
  ((kno_stack_type)((stack->stack_bits)&(KNO_STACK_TYPE_MASK)))

#define KNO_STACK_SET(stack,bit) \
  stack->stack_bits |= bit
#define KNO_STACK_CLEAR(stack,bit) \
  stack->stack_bits &= (~(bit))

/* Stack bits */

#define KNO_STACK_FREE_ARGS	 0x0008
#define KNO_STACK_FREE_LABEL	 0x0010
#define KNO_STACK_FREE_FILE	 0x0020
#define KNO_STACK_STATIC_REFS	 0x0040

/* Reserved for the evaluator */
#define KNO_STACK_EVAL_LOOP	 0x0100
#define KNO_STACK_TAIL_POS	 0x0200
#define KNO_STACK_FREE_ENV	 0x0400
#define KNO_STACK_VOID_VAL	 0x0800

#define KNO_STACK_NEEDS_CLEANUP						\
  (KNO_STACK_FREE_ARGS|KNO_STACK_FREE_LABEL|KNO_STACK_FREE_FILE)

#define KNO_STACK_STATIC_CALL    0x0000
#define KNO_STACK_STATIC_ARGS    0x0000

/*
  Stack flags (inherited by default)
*/

/* Stack error flags */

#define KNO_STACK_ERR_DEBUG	 0x0001	  /* Return stack info */
#define KNO_STACK_ERR_DUMP	 0x0002	  /* Dump stack info when critical */
#define KNO_STACK_ERR_CATCH	 0x0004	  /* Errors *might* be caught */
#define KNO_STACK_ERR_CATCHALL	 0x0008	  /* Errors **will** be caught */
#define KNO_STACK_ERR_RUSAGE	 0x0010	  /* Include rusage in errpoint */
#define KNO_STACK_ERR_REQDATA	 0x0020	  /* Include reqdata in errpoint */
#define KNO_STACK_ERR_THREAD	 0x0040	  /* Include thread bindings in errpoint */
#define KNO_STACK_ERR_FULLSTACK	 0x0080	  /* Dump entire stack (including environments) */
#define KNO_STACK_ERR_CALLSTACK	 0x0100	 /* Dump only calls and EXPRS */

#define KNO_STACK_ERR_DEFAULT	 \
  (KNO_STACK_ERR_DEBUG | KNO_STACK_ERR_DUMP | KNO_STACK_ERR_FULLSTACK)

/* Inhibiting stack call overhead */

#define KNO_STACK_NO_PROFILING	 0x1000	 /* Don't check for profiling */
#define KNO_STACK_NO_CONTOURS	 0x2000	 /* Don't push setmp contours */
/* Not currently used */
#define KNO_STACK_NO_PUSHING	 0x4000	 /* Don't use the stack at all */

#define KNO_STACK_CALL_MASK	 0xF000	 /* Don't use the stack at all */

/* Reserved for the evaluator */

#define KNO_STACK_NO_TAIL	 0x10000
#define KNO_STACK_PUSH_ALL	 0x20000
#define KNO_STACK_KEEP_MACROS	 0x40000

#define KNO_STACK_BITP(stack,bits)       ( ((stack)->stack_bits) & (bits) )
#define KNO_STACK_BITSP(stack,bits)      \
  ( ( ((stack)->stack_bits) & (bits) ) == bits)
#define KNO_STACK_SET_BITS(stack,bits)   (stack)->stack_bits |= bits
#define KNO_STACK_CLEAR_BITS(stack,bits) (stack)->stack_bits &= (~(bits))

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

#define KNO_CHECK_STACK_ORDER(stack,caller)			 \
  ( (caller == NULL) ||						\
    ( (u8_stack_direction > 0) ? ( (stack) > (caller) ) :	\
      ( (stack) < (caller) ) ) )

#define KNO_SETUP_STACK(stack,label,bits)	\
  (stack)->stack_size	  = sizeof(*(stack));	\
  (stack)->stack_label	  = label;			\
  (stack)->stack_bits	  = bits;			\
  (stack)->stack_refs	  = NULL;			\
  (stack)->stack_n_refs	  = 0;				\
  (stack)->stack_n_max    = 0;				\
  (stack)->stack_point	  = KNO_VOID

#define KNO_STACK_SET_CALLER(stack,caller)		\
  (stack)->stack_caller = caller;				\
  if (caller) {							\
    (stack)->stack_depth	= (caller)->stack_depth+1;	\
    (stack)->stack_flags	= caller->stack_flags;}

#define KNO_STACK_SET_ARGBUF(stack,buf,width,freeit)	\
  int stack_bits = stack->stack_bits;			\
  int free_flag = (stack_bits & KNO_STACK_FREE_ARGS);	\
  if (stack->stack_args) {				\
    if (free_flag) u8_free(stack->stack_args);}		\
  stack->stack_args  = (kno_argvec)buf;			\
  stack->stack_width = width;			\
  stack->stack_argc  = 0;				\
  if (free_flag != freeit) {				\
  if (freeit)						\
    stack->stack_bits |= KNO_STACK_FREE_ARGS;		\
  else stack->stack_bits &= (~KNO_STACK_FREE_ARGS);}

#define KNO_STACK_STATIC_ARGBUF  0
#define KNO_STACK_MALLOCD_ARGBUF 1

KNO_EXPORT void
_KNO_STACK_SET_CALL(kno_stack stack,lispval op,
		    int n,kno_argvec args,
		    unsigned int flags);

#if KNO_INLINE_STACKS
KNO_FASTOP U8_MAYBE_UNUSED
void __KNO_STACK_SET_CALL(kno_stack stack,lispval op,
			  int n,kno_argvec args,
			  unsigned int flags)
{
  int bits = stack->stack_bits;
  stack->stack_point=op;
  if (args != stack->stack_args) {
    int free_cur = (bits & KNO_STACK_FREE_ARGS);
    if (free_cur) {
      kno_argvec cur_args = stack->stack_args;
      stack->stack_args = args;
      u8_free(cur_args);}
    else stack->stack_args = args;
    stack->stack_bits = ( (bits) & (~(KNO_STACK_FREE_ARGS)) ) | flags;
    stack->stack_width = n;}
  else stack->stack_args = args;
  stack->stack_argc = n;
  stack->stack_bits = ( (bits) & (~(KNO_STACK_FREE_ARGS)) ) | flags;
}
#endif

#define KNO_PUSH_STACK(stack)				\
  KNO_STACK_SET_CALLER(stack,kno_stackptr);		\
  set_call_stack(stack);

#if KNO_INLINE_STACKS
#define KNO_STACK_SET_CALL __KNO_STACK_SET_CALL
#else
#define KNO_STACK_SET_CALL _KNO_STACK_SET_CALL
#endif

#define KNO_INIT_STACK_ROOT()			 \
  kno_init_cstack();					 \
  u8_byte label[128];							\
  u8_string appid = u8_appid();						\
  u8_sprintf(label,128,"%s.root",appid);				\
  struct KNO_STACK __stack = { 0 }, *_stack=&__stack;			\
  KNO_SETUP_STACK(_stack,label,kno_apply_stack);			\
  set_call_stack(_stack)

#define KNO_INIT_THREAD_STACK()						\
  kno_init_cstack();							\
  u8_byte label[128];							\
  u8_string appid = u8_appid();						\
  u8_sprintf(label,128,"%sthread%d",appid,u8_threadid());		\
  KNO_EVAL_ROOT(_stack,label,KNO_VOID)


KNO_EXPORT int _kno_free_stack(struct KNO_STACK *stack);
KNO_EXPORT int _kno_reset_stack(struct KNO_STACK *stack);
KNO_EXPORT int _kno_pop_stack(struct KNO_STACK *stack);
KNO_EXPORT int _kno_pop_stack_error(struct KNO_STACK *stack);
KNO_EXPORT void _KNO_STACK_ADDREF(struct KNO_STACK *stack,lispval v);

#if KNO_INLINE_STACKS
KNO_FASTOP void __KNO_STACK_ADDREF(struct KNO_STACK *stack,lispval v)
{
  if (KNO_MALLOCDP(v)) {
    if (stack->stack_refs == NULL) {
      lispval *refs = u8_alloc_n(7,lispval);
      stack->stack_refs  = refs;
      stack->stack_n_max = 7;}
    else if ( stack->stack_n_refs >= stack->stack_n_max ) {
      int new_len = 2*(stack->stack_n_max);
      lispval *new_refs;
      if (stack->stack_bits & KNO_STACK_STATIC_REFS) {
	lispval *refs = stack->stack_refs;
	new_refs = u8_alloc_n(new_len,lispval);
	int i = 0, n = stack->stack_n_refs; while (i<n) {
	  new_refs[i] = refs[i]; i++;}}
      else new_refs = u8_realloc(stack->stack_refs,new_len*sizeof(lispval));
      stack->stack_refs = new_refs;
      stack->stack_n_max = new_len;}
    else NO_ELSE;
    stack->stack_refs[(stack->stack_n_refs)++] = v;}
}
KNO_FASTOP int __kno_free_stack(struct KNO_STACK *stack)
{
  kno_stack_type type = KNO_STACK_TYPE(stack);

  if (type == kno_null_stacktype) return -1;

  unsigned int bits = stack->stack_bits;

  if ((bits)&(KNO_STACK_NEEDS_CLEANUP)) {
    if ((bits)&(KNO_STACK_FREE_LABEL)) {
      u8_free(stack->stack_label);
      stack->stack_label=NULL;}
    if ((bits)&(KNO_STACK_FREE_FILE)) {
      u8_free(stack->stack_file);
      stack->stack_file=NULL;}

    KNO_STACK_SET_CALL(stack,KNO_VOID,0,NULL,KNO_STACK_STATIC_ARGS);}

  if (stack->stack_n_refs) {
    lispval *refs = stack->stack_refs;
    int i = 0, n = stack->stack_n_refs;
    while (i<n) {
      lispval ref = refs[i++];
      kno_decref(ref);}
    if (!((bits)&(KNO_STACK_STATIC_REFS)))
      u8_free(stack->stack_refs);
    stack->stack_refs = NULL;
    stack->stack_n_refs = 0;
    stack->stack_n_max  = 0;}

  kno_lexenv env = stack->eval_env;
  if ( (env) && ((bits)&(KNO_STACK_FREE_ENV)) ) {
    kno_free_lexenv(env);
    stack->stack_bits = bits = (bits & (~(KNO_STACK_FREE_ENV)) );}

  return 0;
}
KNO_FASTOP int __kno_reset_stack(struct KNO_STACK *stack)
{
  kno_stack_type type = KNO_STACK_TYPE(stack);

  if (type == kno_null_stacktype) return -1;

  unsigned int bits = stack->stack_bits;

  if (stack->stack_n_refs) {
    lispval *refs = stack->stack_refs;
    int i = 0, n = stack->stack_n_refs;
    while (i<n) {
      lispval ref = refs[i++];
      kno_decref(ref);}
    if (!((bits)&(KNO_STACK_STATIC_REFS)))
      u8_free(stack->stack_refs);
    stack->stack_refs = NULL;
    stack->stack_n_refs = 0;
    stack->stack_n_max  = 0;}

  kno_lexenv env = stack->eval_env;
  if ( (env) && ((bits)&(KNO_STACK_FREE_ENV)) ) {
    kno_free_lexenv(env);
    KNO_STACK_CLEAR_BITS(stack,KNO_STACK_FREE_ENV);}
  if (env) stack->eval_env = NULL;

  stack->stack_argc = 0;

  return 0;
}
KNO_FASTOP int __kno_pop_stack(struct KNO_STACK *stack)
{
  if ( (stack) && (KNO_STACK_TYPE(stack)) && (stack == kno_stackptr) ) {
    struct KNO_STACK *caller = stack->stack_caller;
    int rv = __kno_free_stack(stack);
    set_call_stack(caller);
    return rv;}
  else return _kno_pop_stack_error(stack);
}
#endif /* KNO_INLINE_STACKS */

#if KNO_INLINE_STACKS
#define kno_free_stack __kno_free_stack
#define kno_reset_stack __kno_reset_stack
#define kno_pop_stack __kno_pop_stack
#define KNO_STACK_ADDREF __KNO_STACK_ADDREF
#else /* not KNO_INLINE_STACKS */
#define kno_free_stack _kno_free_stack
#define kno_reset_stack _kno_reset_stack
#define kno_pop_stack _kno_pop_stack
#define KNO_STACK_ADDREF _KNO_STACK_ADDREF
#endif

#if KNO_SOURCE
#define _return return kno_pop_stack(_stack),
#endif

#define kno_return(v) return (kno_pop_stack(_stack),(v))
#define kno_return_from(stack,v) return (kno_pop_stack(stack),(v))

#define KNO_WITH_STACK(label,caller,op,n,args) \
  struct KNO_STACK __stack, *_stack=&__stack; \
  kno_setup_stack(_stack,caller,label,op,n,args)

KNO_EXPORT lispval kno_get_backtrace(struct KNO_STACK *stack);

#endif /* KNO_STACKS_H */

