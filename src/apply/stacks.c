/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2015-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_FCNIDS 1
#define FD_INLINE_STACKS 1
#define FD_INLINE_LEXENV 1
#define FD_INLINE_APPLY  1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/lexenv.h"
#include "framerd/apply.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <stdarg.h>

#if (FD_USE_TLS)
u8_tld_key fd_stackptr_key;
#elif (U8_USE__THREAD)
__thread struct FD_STACK *fd_stackptr=NULL;
#else
struct FD_STACK *fd_stackptr=NULL;
#endif

static lispval stack2lisp(struct FD_STACK *stack)
{
  lispval vec=fd_init_vector(NULL,8,NULL);
  U8_FIXED_OUTPUT(tmp,128);
  FD_VECTOR_SET(vec,0,FD_INT(stack->stack_depth));
  if (stack->stack_type)
    FD_VECTOR_SET(vec,1,lispval_string(stack->stack_type));
  else FD_VECTOR_SET(vec,1,lispval_string("uninitialized"));
  if (stack->stack_label)
    FD_VECTOR_SET(vec,2,lispval_string(stack->stack_label));
  else FD_VECTOR_SET(vec,2,FD_FALSE);
  if (stack->stack_status)
    FD_VECTOR_SET(vec,3,lispval_string(stack->stack_status));
  else FD_VECTOR_SET(vec,3,FD_FALSE);
  if (VOIDP(stack->stack_op))
    FD_VECTOR_SET(vec,4,FD_FALSE);
  else {
    lispval op=stack->stack_op;
    FD_VECTOR_SET(vec,4,op);
    fd_incref(op);}
  if ( stack->stack_args ) {
    lispval n=stack->n_args, i=0;
    lispval *args=stack->stack_args;
    lispval argvec=fd_make_vector(n,args);
    while (i<n) {lispval elt=args[i++]; fd_incref(elt);}
    FD_VECTOR_SET(vec,5,argvec);}
  else FD_VECTOR_SET(vec,5,FD_FALSE);
  if (stack->stack_env) {
    lispval bindings = stack->stack_env->env_bindings;
    if ( (SLOTMAPP(bindings)) || (SCHEMAPP(bindings)) ) {
      lispval b=fd_copy(bindings);
      FD_VECTOR_SET(vec,6,b);}
    else FD_VECTOR_SET(vec,6,FD_FALSE);}
  if (!((FD_NULLP(stack->stack_source))||(VOIDP(stack->stack_source)))) {
    lispval source = stack->stack_source;
    FD_VECTOR_SET(vec,7,source);
    fd_incref(source);}
  else FD_VECTOR_SET(vec,7,FD_FALSE);
  return vec;
}

FD_EXPORT
lispval fd_get_backtrace(struct FD_STACK *stack)
{
  lispval result = FD_EMPTY_LIST, *tail=&result;
  if (stack == NULL) stack=fd_stackptr;
  while (stack) {
    lispval entry = stack2lisp(stack);
    lispval pair  = fd_init_pair(NULL,entry,FD_EMPTY_LIST);
    *tail=pair;
    tail = &(FD_CDR(pair));
    stack=stack->stack_caller;}
  return result;
}

FD_EXPORT
void fd_sum_backtrace(u8_output out,lispval backtrace)
{
  if (fd_stacktracep(backtrace)) {
    lispval scan=backtrace;
    int n=0; while (PAIRP(scan)) {
      lispval entry = FD_CAR(scan);
      if ((VECTORP(entry)) && (VEC_LEN(entry)>=7)) {
	lispval type=VEC_REF(entry,1);
	lispval label=VEC_REF(entry,2);
	lispval status=VEC_REF(entry,3);
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
      else {}
      scan=FD_CDR(scan);}}
}

void fd_init_stacks_c()
{
#if (FD_USE_TLS)
  u8_new_threadkey(&fd_stackptr_key,NULL);
  u8_tld_set(fd_stackptr_key,(void *)NULL);
#else
  fd_stackptr=NULL;
#endif
}
