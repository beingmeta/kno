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

#if (KNO_USE_TLS)
u8_tld_key kno_stackptr_key;
#elif (U8_USE__THREAD)
__thread struct KNO_STACK *kno_stackptr=NULL;
#else
struct KNO_STACK *kno_stackptr=NULL;
#endif

u8_string kno_stack_types[8]=
  {"deadstack","applystack","iterstack","evalstack",
   "stack4","stack5","stack6","stack7"};
static lispval stack_type_symbols[8];

static lispval stack_entry_symbol, stack_target_symbol, opaque_symbol,
  pbound_symbol, pargs_symbol;
static lispval null_sym, unbound_sym, void_sym, default_sym, nofile_symbol;

static int tidy_stack_frames = 1;

static lispval copy_args(int width,lispval *args);

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
  if (stack->stack_dumpfn)
    return stack->stack_dumpfn(stack);
  kno_stack_type type = KNO_STACK_TYPE(stack);

  lispval depth  = KNO_INT(stack->stack_depth);
  lispval type   = stack_type_symbols[type];
  lispval label  = knostring(stack->stack_label);
  lispval status = (stack->stack_status) ?
    (knostring(stack->stack_status)) : (KNO_FALSE);
  lispval src    = (stack->stack_src) ?
    (knostring(stack->stack_src)) : (KNO_FALSE);
  lispval op     = kno_incref(stack->stack_op);
  lispval args = ( (stack->stack_args) && (stack->stack_width) ) ?
    (copy_args(stack->stack_width,stack->stack_args)) :
    (KNO_EMPTY_LIST);

  unsigned int icrumb = stack->stack_crumb;
  if (icrumb == 0) {
    icrumb = u8_random(UINT_MAX);
    if (icrumb > KNO_MAX_FIXNUM) icrumb = icrumb%KNO_MAX_FIXNUM;
    stack->stack_crumb=icrumb;}

  return kno_init_compound
    (NULL,stack_entry_symbol,STACK_CREATE_OPTS,
     6,depth,type,label,src,op,args,KNO_INT(icrumb));
}

static lispval copy_args(int width,lispval *args)
{
  lispval result = kno_make_vector(width,args);
  lispval *elts = KNO_VECTOR_ELTS(result);
  int i = 0; while (i<n) {
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

void kno_init_stacks_c()
{
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

  int i = 0; while (i<8) {
    stacktype_syms[i]=kno_intern(stack_types[i]);
    i++;}

}

