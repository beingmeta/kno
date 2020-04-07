/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2015-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_FCNIDS 1
#define KNO_INLINE_STACKS 1
#define KNO_INLINE_LEXENV 1
#define KNO_INLINE_APPLY  1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/lexenv.h"
#include "kno/apply.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <stdarg.h>

u8_condition kno_NoStackChecking=
  _("Stack checking is not available in this build of Kno");
u8_condition kno_StackTooSmall=
  _("This value is too small for an application stack limit");
u8_condition kno_StackTooLarge=
  _("This value is larger than the available stack space");
u8_condition kno_BadStackSize=
  _("This is an invalid stack size");
u8_condition kno_BadStackFactor=
  _("This is an invalid stack resize factor");
u8_condition kno_InternalStackSizeError=
  _("Internal stack size didn't make any sense, punting");

#if ((KNO_THREADS_ENABLED)&&(KNO_USE_TLS))
u8_tld_key kno_stack_limit_key;
#elif ((KNO_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
__thread ssize_t kno_stack_limit = -1;
#else
ssize_t kno_stack_limit = -1;
#endif

#if (KNO_USE_TLS)
u8_tld_key kno_stackptr_key;
#elif (U8_USE__THREAD)
__thread struct KNO_STACK *kno_stackptr=NULL;
#else
struct KNO_STACK *kno_stackptr=NULL;
#endif

#if KNO_DEBUG_STACKS
int kno_debug_stacks = 0;
#endif

static lispval stack_entry_symbol, stack_target_symbol, opaque_symbol,
  pbound_symbol, pargs_symbol;
static lispval null_sym, unbound_sym, void_sym, default_sym, nofile_symbol;

/* This is for the C stack */
lispval kno_default_stackspec = VOID;

static lispval copy_args(int width,kno_argvec args);

/* Call stack (not used yet) */

KNO_EXPORT void _kno_stack_push_error(kno_stack stack,u8_context loc)
{
  kno_stack cur = kno_stackptr;
  kno_stack caller = stack->stack_caller;
  u8_condition cond =
    (cur == NULL) ? ("NullStackPtr(push)") :
    (caller == NULL) ? ("NullCaller(push)") :
    (!(STACK_LIVEP(caller))) ? ("DeadCaller(push)") :
    (caller < kno_stackptr) ? ("CallerAlreadyExited(push)") :
    (caller > kno_stackptr) ? ("CallerNotCurrent(push)") :
    ("OddStackError(push)");
  u8_log(LOGCRIT,cond,
	 "Push stack 0x%llx; caller 0x%llx; stackptr 0x%llx @ %s",
	 (unsigned long long)stack,
	 (unsigned long long)caller,
	 (unsigned long long)cur,
	 loc);
  u8_raise(cond,loc,NULL);
}

KNO_EXPORT void _kno_stack_pop_error(kno_stack stack,u8_context loc)
{
  kno_stack cur = kno_stackptr;
  kno_stack caller = stack->stack_caller;
  u8_condition cond =
    (cur == NULL) ? ("NullStackPtr(pop)") :
    (!(STACK_LIVEP(cur))) ? ("DeadStack(pop)") :
    (caller == NULL) ? ("NullCaller(pop)") :
    (stack < kno_stackptr) ? ("FrameAlreadyExited(pop)") :
    (stack > kno_stackptr) ? ("FrameNotCurrent(pop)") :
    ("OddStackError(pop)");
  u8_log(LOGCRIT,cond,
	 "Pop stack 0x%llx; caller 0x%llx; stackptr 0x%llx @ %s",
	 (unsigned long long)stack,
	 (unsigned long long)caller,
	 (unsigned long long)cur,
	 loc);
  u8_raise(cond,loc,NULL);
}

KNO_EXPORT void
_kno_stackvec_push(kno_stackvec sv,lispval v)
{
  kno_stackvec_push(sv,v);
}

KNO_EXPORT void
_kno_decref_stackvec(kno_stackvec sv)
{
  kno_decref_stackvec(sv);
}

KNO_EXPORT void
_kno_add_stack_ref(kno_stack stack,lispval v)
{
  kno_add_stack_ref(stack,v);
}

KNO_EXPORT void
_KNO_STACK_SET_ARGS(kno_stack stack,lispval *args,
		    int width,int n,
		    int flags)
{
  KNO_STACK_SET_ARGS(stack,args,width,n,flags);
}


KNO_EXPORT int _kno_free_stack(struct KNO_STACK *stack)
{
  return kno_free_stack(stack);
}

KNO_EXPORT int _kno_reset_stack(struct KNO_STACK *stack)
{
  return kno_reset_stack(stack);
}

KNO_EXPORT  void _KNO_STACK_SET_OP(kno_stack s,lispval new,int free)
{
  KNO_STACK_SET_OP(s,new,free);
}


/* Stacks are rendered into LISP as vectors as follows:
   1. depth  (integer, increasing with calls)
   2. type   (apply, eval, load, other)
   3. label  (often the name of a function or expression type)
   4. status (a string (or #f) describing the state of the operation)
   5. op     (the thing being executed (expression, procedure applied, etc)
   6. args   (a vector of arguments to an applied procedure, or () otherwise)
*/

#define STACK_CREATE_OPTS KNO_COMPOUND_USEREF

static lispval stack2lisp(struct KNO_STACK *stack,struct KNO_STACK *inner)
{
  lispval depth	  = KNO_INT(stack->stack_depth);
  lispval label	  = (stack->stack_label) ?
    (knostring(stack->stack_label)) :
    (KNO_FALSE);
  lispval file	  = (stack->stack_file) ?
    (knostring(stack->stack_file)) :
    (KNO_FALSE);
  lispval op	  = kno_incref(stack->stack_op);
  kno_lexenv env  = stack->eval_env;
  lispval source  = stack->eval_source;
  int is_apply = (!(KNO_PAIRP(op)));
  int show_source =  (is_apply) ? (0) : (KNO_VOIDP(source)) ? (0) :
    (!(op == source));

  unsigned int icrumb = stack->stack_crumb;
  if (icrumb == 0) {
    icrumb = u8_random(UINT_MAX);
    if (icrumb > KNO_MAX_FIXNUM) icrumb = icrumb%KNO_MAX_FIXNUM;
    stack->stack_crumb=icrumb;}

  if (op == KNO_VOID) op = KNO_FALSE;

  if (is_apply)
    return kno_init_compound
      (NULL,stack_entry_symbol,STACK_CREATE_OPTS,
       5,depth,label,file,op,
       (((STACK_ARGS(stack)) && (STACK_LENGTH(stack))) ?
	(copy_args(STACK_WIDTH(stack),STACK_ARGS(stack))) :
	(KNO_EMPTY_LIST)),
       KNO_INT(icrumb));
  else if (show_source)
    return kno_init_compound
      (NULL,stack_entry_symbol,STACK_CREATE_OPTS,
       6,depth,label,file,source,
       ((env) ? (kno_deep_copy(env->env_bindings)) :
	((STACK_ARGS(stack)) && (STACK_LENGTH(stack))) ?
	(copy_args(STACK_WIDTH(stack),STACK_ARGS(stack))) :
	(KNO_EMPTY_LIST)),
       op,KNO_INT(icrumb));
    else return kno_init_compound
	   (NULL,stack_entry_symbol,STACK_CREATE_OPTS,
	    5,depth,label,file,op,
	    ((env) ? (kno_deep_copy(env->env_bindings)) : (KNO_FALSE)),
	    KNO_INT(icrumb));
}

static lispval copy_args(int width,kno_argvec args)
{
  lispval result = kno_make_vector(width,(lispval *)args);
  lispval *elts = KNO_VECTOR_ELTS(result);
  int i = 0; while (i<width) {
    lispval elt = elts[i++];
    kno_incref(elt);}
  return result;
}

KNO_EXPORT
lispval kno_get_backtrace(struct KNO_STACK *stack)
{
  if (stack == NULL) stack=kno_stackptr;
  if (stack == NULL) return KNO_EMPTY_LIST;
  int n = stack->stack_depth+1, i = 0;
  lispval result = kno_make_vector(n,NULL);
  struct KNO_STACK *prev = NULL;
  while (stack) {
    lispval entry = stack2lisp(stack,prev);
    if (i < n) {
      KNO_VECTOR_SET(result,i,entry);}
    else u8_log(LOG_CRIT,"BacktraceOverflow",
		"Inconsistent depth %d",
		n-1);
    prev=stack;
    stack=stack->stack_caller;
    i++;}
  return result;
}

KNO_EXPORT
void kno_sum_backtrace(u8_output out,lispval stacktrace)
{
  if (KNO_VECTORP(stacktrace)) {
    int i = 0, len = KNO_VECTOR_LENGTH(stacktrace), n = 0;
    while (i<len) {
      lispval entry = KNO_VECTOR_REF(stacktrace,i);
      if (KNO_COMPOUND_TYPEP(entry,stack_entry_symbol)) {
	lispval type=KNO_COMPOUND_REF(entry,1);
	lispval label=KNO_COMPOUND_REF(entry,2);
	lispval status=KNO_COMPOUND_REF(entry,3);
	if (n) u8_puts(out," ⇒ ");
	if (STRINGP(label)) u8_puts(out,CSTRING(label));
	else u8_putc(out,'?');
	if (STRINGP(type)) {
	  u8_putc(out,'.');
	  u8_puts(out,CSTRING(type));}
	else u8_puts(out,".?");
	if (STRINGP(status)) {
	  u8_putc(out,'(');
	  u8_puts(out,CSTRING(status));
	  u8_putc(out,')');}
	n++;}
      i++;}}
}

/* Checking the C stack depth */

int kno_wrap_apply = KNO_WRAP_APPLY_DEFAULT;

#if KNO_STACKCHECK
static int stackcheck()
{
  if (kno_stack_limit>=KNO_MIN_STACKSIZE) {
    ssize_t depth = u8_stack_depth();
    if (depth>kno_stack_limit)
      return 0;
    else return 1;}
  else return 1;
}
KNO_EXPORT ssize_t kno_stack_getsize()
{
  if (kno_stack_limit>=KNO_MIN_STACKSIZE)
    return kno_stack_limit;
  else return -1;
}
KNO_EXPORT ssize_t kno_stack_setsize(ssize_t limit)
{
  if (limit<KNO_MIN_STACKSIZE) {
    char *detailsbuf = u8_malloc(64);
    u8_seterr("StackLimitTooSmall","kno_stack_setsize",
	      u8_write_long_long(limit,detailsbuf,64));
    return -1;}
  else {
    ssize_t maxstack = u8_stack_size;
    if (maxstack < KNO_MIN_STACKSIZE) {
      u8_seterr(kno_InternalStackSizeError,"kno_stack_resize",NULL);
      return -1;}
    else if (limit <=  maxstack) {
      ssize_t old = kno_stack_limit;
      kno_set_stack_limit(limit);
      return old;}
    else {
#if ( (HAVE_PTHREAD_SELF) &&			\
      (HAVE_PTHREAD_GETATTR_NP) &&		\
      (HAVE_PTHREAD_ATTR_SETSTACKSIZE) )
      pthread_t self = pthread_self();
      pthread_attr_t attr;
      if (pthread_getattr_np(self,&attr)) {
	u8_graberrno("kno_set_stacksize",NULL);
	return -1;}
      else if ((pthread_attr_setstacksize(&attr,limit))) {
	u8_graberrno("kno_set_stacksize",NULL);
	return -1;}
      else {
	ssize_t old = kno_stack_limit;
	kno_set_stack_limit(limit-limit/8);
	return old;}
#else
      char *detailsbuf = u8_malloc(64);
      u8_seterr("StackLimitTooLarge","kno_stack_setsize",
		u8_write_long_long(limit,detailsbuf,64));
      return -1;
#endif
    }}
}
KNO_EXPORT ssize_t kno_stack_resize(double factor)
{
  if ( (factor<0) || (factor>1000) ) {
    u8_log(LOG_WARN,kno_BadStackFactor,
	   "The value %f is not a valid stack resize factor",factor);
    return kno_stack_limit;}
  else {
    ssize_t current = kno_stack_limit;
    ssize_t limit = u8_stack_size;
    if (limit<KNO_MIN_STACKSIZE) limit = current;
    return kno_stack_setsize((limit*factor));}
}
#else
static int youve_been_warned = 0;
#define stackcheck() (1)
KNO_EXPORT ssize_t kno_stack_getsize()
{
  if (youve_been_warned) return -1;
  u8_log(LOG_WARN,"NoStackChecking",
	 "Stack checking is not enabled in this build");
  youve_been_warned = 1;
  return -1;
}
KNO_EXPORT ssize_t kno_stack_setsize(ssize_t limit)
{
  u8_log(LOG_WARN,"NoStackChecking",
	 "Stack checking is not enabled in this build");
  return -1;
}
KNO_EXPORT ssize_t kno_stack_resize(double factor)
{
  if (factor<0) {
    u8_log(LOG_WARN,"NoStackChecking",
	   "Stack checking is not enabled in this build and the value you provided (%f) is invalid anyway",
	   factor);}
  else if (factor<=1000) {
    u8_log(LOG_WARN,"StackResizeTooLarge",
	   "Stack checking is not enabled in this build and the value you provided (%f) is too big anyway",
	   factor);}
  else {
    u8_log(LOG_WARN,"NoStackChecking",
	   "Stack checking is not enabled in this build so setting the limit doesn't do anything",
	   factor);}
  return -1;
}
#endif

KNO_EXPORT int kno_stackcheck()
{
  return stackcheck();
}

/* Initialize C stack limits */

KNO_EXPORT ssize_t kno_init_cstack()
{
  u8_init_stack();
  if (VOIDP(kno_default_stackspec)) {
    ssize_t stacksize = u8_stack_size;
    return kno_stack_setsize(stacksize-stacksize/8);}
  else if (FIXNUMP(kno_default_stackspec))
    return kno_stack_setsize((ssize_t)(FIX2INT(kno_default_stackspec)));
  else if (KNO_FLONUMP(kno_default_stackspec))
    return kno_stack_resize(KNO_FLONUM(kno_default_stackspec));
  else if (KNO_BIGINTP(kno_default_stackspec)) {
    unsigned long long val = kno_getint(kno_default_stackspec);
    if (val) return kno_stack_setsize((ssize_t) val);
    else {
      u8_log(LOG_CRIT,kno_BadStackSize,
	     "The default stack value %q wasn't a valid stack size",
	     kno_default_stackspec);
      return -1;}}
  else {
    u8_log(LOG_CRIT,kno_BadStackSize,
	   "The default stack value %q wasn't a valid stack size",
	   kno_default_stackspec);
    return -1;}
}

static int init_thread_stack_limit()
{
  kno_init_cstack();
  return 1;
}

void kno_init_stacks_c()
{
  u8_register_threadinit(init_thread_stack_limit);

#if (KNO_USE_TLS)
  u8_new_threadkey(&kno_stackptr_key,NULL);
  u8_tld_set(kno_stackptr_key,(void *)NULL);
#else
  kno_stackptr=NULL;
#endif

  opaque_symbol = kno_intern("%opaque");
  stack_entry_symbol = kno_intern("_stack");
  stack_target_symbol = kno_intern("$<<*eval*>>$");
  pbound_symbol = kno_intern("%bound");
  pargs_symbol = kno_intern("%args");

  null_sym = kno_intern("#null");
  unbound_sym = kno_intern("#unbound");
  void_sym = kno_intern("#void");
  default_sym = kno_intern("#default");
  nofile_symbol = kno_intern("nofile");

  kno_register_config
    ("STACKLIMIT",
     _("Size of the stack (in bytes or as a factor of the current size)"),
     kno_sizeconfig_get,kno_sizeconfig_set,&kno_default_stackspec);
  kno_register_config
    ("STACK:DEBUG",_("Whether to trace all stack push/pop operations"),
     kno_boolconfig_get,kno_boolconfig_set,&kno_debug_stacks);

}

