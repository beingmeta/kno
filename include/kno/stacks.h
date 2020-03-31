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

/* Stack vectors (stackvecs) */

typedef struct KNO_STACKVEC {
  lispval *elts;
  unsigned int len;
  unsigned int count;} KNO_STACKVEC;
typedef struct KNO_STACKVEC *kno_stackvec;

#define KNO_DECL_STACKVEC(name,init_len) \
  lispval _ ## name ## _elts[init_len]; \
  struct KNO_STACKVEC _ ## name = { _ ## name ## _elts, init_len, 0 };	\
  struct KNO_STACKVEC * name = & _ ## name

#ifndef KNO_STACKVEC_INIT_LEN
#define KNO_STACKVEC_INIT_LEN 7
#endif

#ifndef KNO_STACKVEC_MAX_DELTA
#define KNO_STACKVEC_MAX_DELTA 1000
#endif

#define KNO_STACKVEC_HEAPBIT (0x10000000)
#define KNO_STACKVEC_LENMASK (~(0x10000000))

#define KNO_STACKVEC_ELTS(sv)   ((sv)->elts)
#define KNO_STACKVEC_ARGS(sv)   ((kno_argvec)((sv)->elts))
#define KNO_STACKVEC_COUNT(sv)  ((sv)->count)
#define KNO_STACKVEC_LEN(sv)    (((sv)->len)&(KNO_STACKVEC_LENMASK))
#define KNO_STACKVEC_ONHEAP(sv) (((sv)->len)&(KNO_STACKVEC_HEAPBIT))

#if KNO_SOURCE
#define STACKVEC_ELTS(sv)   ((sv)->elts)
#define STACKVEC_ARGS(sv)   ((kno_argvec)((sv)->elts))
#define STACKVEC_COUNT(sv)  ((sv)->count)
#define STACKVEC_LEN(sv)    (((sv)->len)&(KNO_STACKVEC_LENMASK))
#define STACKVEC_ONHEAP(sv) (((sv)->len)&(KNO_STACKVEC_HEAPBIT))
#endif

KNO_EXPORT void _kno_stackvec_push(struct KNO_STACKVEC *stack,lispval v);
KNO_EXPORT void _kno_stackvec_grow(struct KNO_STACKVEC *stack,int n);
KNO_EXPORT void _kno_decref_stackvec(struct KNO_STACKVEC *stack);
KNO_EXPORT void _kno_free_stackvec(struct KNO_STACKVEC *stack);

#if KNO_INLINE_STACKS
#define kno_stackvec_push    __kno_stackvec_push
#define kno_stackvec_grow    __kno_stackvec_grow
#define kno_decref_stackvec  __kno_decref_stackvec
#define kno_free_stackvec    __kno_free_stackvec
#else
#define kno_stackvec_push   _kno_stackvec_push
#define kno_stackvec_grow    _kno_stackvec_grow
#define kno_decref_stackvec _kno_decref_stackvec
#define kno_free_stackvec   _kno_free_stackvec
#endif

#if KNO_INLINE_STACKS
static void __kno_stackvec_grow(struct KNO_STACKVEC *sv,int len)
{
  if ( (KNO_STACKVEC_LEN(sv)) > len ) return;
  lispval *elts = KNO_STACKVEC_ELTS(sv);
  if (elts == NULL) {
    elts = u8_alloc_n(len,lispval);
    sv->elts  = elts;
    sv->count = 0;
    sv->len   = KNO_STACKVEC_HEAPBIT|len;}
  else {
    int count     = KNO_STACKVEC_COUNT (sv);
    int onheap    = KNO_STACKVEC_ONHEAP(sv);
    if (onheap) {
      lispval *new_elts = u8_realloc(elts,len*sizeof(lispval));
      sv->elts = new_elts;
      sv->len  = KNO_STACKVEC_HEAPBIT|len;
      return;}
    else {
      lispval *new_elts = u8_alloc_n(len,lispval);
      memcpy(new_elts,elts,count*sizeof(lispval));
      sv->elts = new_elts;
      sv->len  = KNO_STACKVEC_HEAPBIT|len;
      return;}}
}
KNO_FASTOP void __kno_stackvec_push(struct KNO_STACKVEC *sv,lispval v)
{
  int count = KNO_STACKVEC_COUNT(sv);
  int len   = KNO_STACKVEC_LEN(sv);
  if ( count >= len ) {
    int new_len = len +
      ( (len > KNO_STACKVEC_MAX_DELTA) ? (KNO_STACKVEC_MAX_DELTA) :
	(len>0) ? (len) : (KNO_STACKVEC_INIT_LEN) );
    __kno_stackvec_grow(sv,new_len);}
  sv->elts[(sv->count)++] = v;
}
KNO_FASTOP void __kno_decref_stackvec(struct KNO_STACKVEC *sv)
{
  lispval *elts = KNO_STACKVEC_ELTS(sv);
  int count     = KNO_STACKVEC_COUNT(sv);
  if (count) {
    int i = 0; while (i<count) {
      lispval elt = elts[i++];
      kno_decref(elt);}}
}
KNO_FASTOP void __kno_free_stackvec(struct KNO_STACKVEC *sv)
{
  lispval *elts = KNO_STACKVEC_ELTS(sv);
  if (KNO_STACKVEC_ONHEAP(sv)) u8_free(elts);
  memset(sv,0,sizeof(struct KNO_STACKVEC));
}
#endif

/* Stacks */

typedef struct KNO_STACK {
  // stack_bits bits describing this stack frame in particular
  unsigned int stack_bits;
  // stack_flags bits describing this stack context and those below it;
  // it is inherited when a new stack frame is pushed
  unsigned int stack_flags;
  // stack_caller is a pointer to the caller of this frame
  struct KNO_STACK *stack_caller;
  // stack_point  the expression being evaluated, the function
  //  being applied, or the value being processed
  lispval stack_point;
  // stack_args are a vector of ordered arguments for the current
  // operation
  struct KNO_STACKVEC stack_args;
  // stack_refs are references to be freed on exit
  struct KNO_STACKVEC stack_refs;
  // eval source is the original (unoptimized) expression for the
  // current operation
  lispval eval_source;
  //  eval_context the LISP source context for this computation,
  // this generally contains eval_source as a sub-expression.
  lispval eval_context;
  // Any bindings established by this stack frame, usually a schemap
  struct KNO_LEXENV *eval_env;
  // stack_depth is the number of frames above this one
  unsigned int stack_depth;
  // stack_size is the size of this frame in bytes
  unsigned int stack_size;
  // stack_crumb is used to uniquely identify this frame at this
  //  moment and is defined when stacks get rendered to LISP
  unsigned int stack_crumb;
  // stack_file a string indicating the source file/location of whatever
  //  code is running in this stack frame
  u8_string stack_file;
  // stack_label a string naming his stack for visualization and debugging
  u8_string stack_label;
} KNO_STACK;
typedef struct KNO_STACK *kno_stack;

#define KNO_STACK_ARGS(stack)     ((stack)->stack_args.elts)
#define KNO_STACK_WIDTH(stack)    ((stack)->stack_args.len)
#define KNO_STACK_LENGTH(stack)    ((stack)->stack_args.count)
#define KNO_STACK_ARGCOUNT(stack) ((stack)->stack_args.count)

#if KNO_SOURCE
#define STACK_ARGS(stack)     ((stack)->stack_args.elts)
#define STACK_WIDTH(stack)    ((stack)->stack_args.len)
#define STACK_LENGTH(stack)    ((stack)->stack_args.count)
#define STACK_ARGCOUNT(stack) ((stack)->stack_args.count)
#endif

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

#define KNO_STACK_DECREF_ARGS	 0x0008
#define KNO_STACK_FREE_LABEL	 0x0010
#define KNO_STACK_FREE_FILE	 0x0020
#define KNO_STACK_FREE_REFS	 0x0040

/* Reserved for the evaluator */
#define KNO_STACK_EVAL_LOOP	 0x0100
#define KNO_STACK_TAIL_POS	 0x0200
#define KNO_STACK_VOID_VAL	 0x0400
#define KNO_STACK_FREE_ENV	 0x0800

#define KNO_STACK_NEEDS_CLEANUP						\
  (KNO_STACK_FREE_LABEL|KNO_STACK_FREE_FILE)

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

#define KNO_SETUP_STACK(stack,label,bits)			\
  (stack)->stack_size	     = sizeof(*(stack));		\
  (stack)->stack_label	     = label;				\
  (stack)->stack_bits	     = bits;				\
  (stack)->stack_point	     = KNO_VOID;			\
  (stack)->eval_source	     = KNO_VOID;			\
  (stack)->eval_context      = KNO_VOID

#define KNO_STACK_SET_CALLER(stack,caller)		\
  (stack)->stack_caller = caller;				\
  if (caller) {							\
    (stack)->stack_depth	= (caller)->stack_depth+1;	\
    (stack)->stack_flags	= caller->stack_flags;}

#define KNO_STACK_SET_ENV(stack,env,freeit)		       \
  kno_lexenv _cur_env = stack->eval_env;				\
  int _free_cur = (KNO_STACK_BITP(stack,KNO_STACK_FREE_ENV) );		\
  if (_cur_env != env) {						\
    stack->eval_env = env;						\
    if (_free_cur) { kno_free_lexenv(_cur_env); }			\
    if ( (env) && (freeit != _free_cur) ) {				\
      if (freeit) KNO_STACK_SET_BITS(stack,KNO_STACK_FREE_ENV);		\
      else KNO_STACK_CLEAR_BITS(stack,KNO_STACK_FREE_ENV);}}

#define KNO_STACK_STATIC_ARGBUF  0
#define KNO_STACK_MALLOCD_ARGBUF 1


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


/* Stack set args */

#define KNO_STATIC_ARGS 0x00
#define KNO_DECREF_ARGS 0x01
#define KNO_FREE_ARGBUF 0x01

KNO_EXPORT void _KNO_STACK_SET_ARGS(kno_stack stack,lispval *buf,
				    int width,int argcount,
				    int flags);

#if KNO_INLINE_STACKS
KNO_FASTOP U8_MAYBE_UNUSED
void __KNO_STACK_SET_ARGS(kno_stack stack,lispval *buf,
			  int width,int argcount,
			  int flags)
{
  lispval *args = STACK_ARGS(stack);
  int decref  = flags & KNO_DECREF_ARGS;
  int freebuf = flags & KNO_FREE_ARGBUF;
  int stack_bits = stack->stack_bits;
  int cur_decref  = (stack_bits & KNO_STACK_DECREF_ARGS);
  if (args) {
    if ( (cur_decref) && ( ! ( (decref) && (buf == args) ) ) )
      kno_decref_stackvec(&(stack->stack_args));
    kno_free_stackvec(&(stack->stack_args));}

  STACK_ARGS(stack)     = buf;
  STACK_ARGCOUNT(stack) = argcount;
  if (freebuf)
    stack->stack_args.len = (KNO_STACKVEC_HEAPBIT|width);
  else stack->stack_args.len = width;
  if (decref != cur_decref) {
    if (decref) KNO_STACK_SET_BITS(stack,KNO_STACK_DECREF_ARGS);
    else KNO_STACK_SET_BITS(stack,KNO_STACK_DECREF_ARGS);}
}
#define KNO_STACK_SET_ARGS __KNO_STACK_SET_ARGS
#else
#define KNO_STACK_SET_ARGS _KNO_STACK_SET_ARGS
#endif

KNO_EXPORT void _kno_add_stack_ref(struct KNO_STACK *stack,lispval v);
KNO_EXPORT int _kno_free_stack(struct KNO_STACK *stack);
KNO_EXPORT int _kno_reset_stack(struct KNO_STACK *stack);
KNO_EXPORT int _kno_pop_stack(struct KNO_STACK *stack);
KNO_EXPORT int _kno_pop_stack_error(struct KNO_STACK *stack);

#if KNO_INLINE_STACKS
#define kno_add_stack_ref __kno_add_stack_ref
#define kno_free_stack __kno_free_stack
#define kno_reset_stack __kno_reset_stack
#define kno_pop_stack __kno_pop_stack
#else /* not KNO_INLINE_STACKS */
#define kno_add_stack_ref _kno_add_stack_ref
#define kno_free_stack _kno_free_stack
#define kno_reset_stack _kno_reset_stack
#define kno_pop_stack _kno_pop_stack
#endif

#define kno_free_stack_refs(stack) \
  (kno_decref_stackvec(&(stack->stack_refs)),\
   kno_free_stackvec(&(stack->stack_refs)))

#if KNO_INLINE_STACKS
KNO_FASTOP void __kno_add_stack_ref(struct KNO_STACK *stack,lispval v)
{
  if (KNO_MALLOCDP(v)) kno_stackvec_push(&(stack->stack_refs),v);
}
KNO_FASTOP int __kno_free_stack(struct KNO_STACK *stack)
{
  kno_stack_type type = KNO_STACK_TYPE(stack);

  if (type == kno_null_stacktype) return -1;

  unsigned int bits = stack->stack_bits;

  lispval *args = STACK_ARGS(stack);

  kno_lexenv env = stack->eval_env;
  if ( (env) && ((bits)&(KNO_STACK_FREE_ENV)) ) {
    KNO_STACK_CLEAR_BITS(stack,KNO_STACK_FREE_ENV);
    kno_lexenv copy = env->env_copy;
    if (copy) kno_free_lexenv(copy);}

  /* Other cleanup */

  if ((bits)&(KNO_STACK_NEEDS_CLEANUP)) {
    if ((bits)&(KNO_STACK_FREE_LABEL)) {
      u8_free(stack->stack_label);
      stack->stack_label=NULL;}
    if ((bits)&(KNO_STACK_FREE_FILE)) {
      u8_free(stack->stack_file);
      stack->stack_file=NULL;}}

  kno_decref_stackvec(&(stack->stack_refs));
  kno_free_stackvec(&(stack->stack_refs));

  if (args) {
    if (bits & KNO_STACK_DECREF_ARGS) {
      kno_decref_stackvec(&(stack->stack_args));
      kno_free_stackvec(&(stack->stack_args));
      KNO_STACK_CLEAR_BITS(stack,KNO_STACK_DECREF_ARGS);}
    else kno_free_stackvec(&(stack->stack_args));}

  stack->stack_point = KNO_VOID;
  stack->eval_env   = NULL;

  return 0;
}
KNO_FASTOP int __kno_reset_stack(struct KNO_STACK *stack)
{
  kno_stack_type type = KNO_STACK_TYPE(stack);

  if (type == kno_null_stacktype) return -1;

  unsigned int bits = stack->stack_bits;
  kno_lexenv env = stack->eval_env;
  if ( (env) && ((bits)&(KNO_STACK_FREE_ENV)) ) {
    kno_free_lexenv(env);
    KNO_STACK_CLEAR_BITS(stack,KNO_STACK_FREE_ENV);}
  if (env) stack->eval_env = NULL;

  kno_decref_stackvec(&(stack->stack_refs));

  STACK_ARGCOUNT(stack) = 0;

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

