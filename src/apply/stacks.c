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

static void summarize_stack_frame(u8_output out,struct FD_STACK *stack)
{
  if (stack->stack_label)
    u8_puts(out,stack->stack_label);
  if ( (stack->stack_status) &&
       (stack->stack_status[0]) &&
       (stack->stack_status!=stack->stack_label) ) {
    u8_printf(out,"(%s)",stack->stack_status);}
  if ((stack->stack_type) &&
      (strcmp(stack->stack_type,stack->stack_label)))
    u8_printf(out,".%s",stack->stack_type);
}

static fdtype stack2lisp(struct FD_STACK *stack)
{
  fdtype vec=fd_init_vector(NULL,8,NULL);
  U8_FIXED_OUTPUT(tmp,128);
  FD_VECTOR_SET(vec,0,FD_INT(stack->stack_depth));
  if (stack->stack_type)
    FD_VECTOR_SET(vec,1,fdtype_string(stack->stack_type));
  else FD_VECTOR_SET(vec,1,fdtype_string("uninitialized"));
  if (stack->stack_label)
    FD_VECTOR_SET(vec,2,fdtype_string(stack->stack_label));
  else FD_VECTOR_SET(vec,2,FD_FALSE);
  if (stack->stack_status)
    FD_VECTOR_SET(vec,3,fdtype_string(stack->stack_status));
  else FD_VECTOR_SET(vec,3,FD_FALSE);
  if (FD_VOIDP(stack->stack_op))
    FD_VECTOR_SET(vec,4,FD_FALSE);
  else {
    fdtype op=stack->stack_op;
    FD_VECTOR_SET(vec,4,op);
    fd_incref(op);}
  if ( stack->stack_args ) {
    fdtype n=stack->n_args, i=0;
    fdtype *args=stack->stack_args;
    fdtype argvec=fd_make_vector(n,args);
    while (i<n) {fdtype elt=args[i++]; fd_incref(elt);}
    FD_VECTOR_SET(vec,5,argvec);}
  else FD_VECTOR_SET(vec,5,FD_FALSE);
  if (stack->stack_env) {
    fdtype bindings = stack->stack_env->env_bindings;
    if ( (FD_SLOTMAPP(bindings)) || (FD_SCHEMAPP(bindings)) ) {
      fdtype b=fd_copy(bindings);
      FD_VECTOR_SET(vec,6,b);}
    else FD_VECTOR_SET(vec,6,FD_FALSE);}
  if (!((FD_NULLP(stack->stack_source))||(FD_VOIDP(stack->stack_source)))) {
    fdtype source = stack->stack_source;
    FD_VECTOR_SET(vec,7,source);
    fd_incref(source);}
  else FD_VECTOR_SET(vec,7,FD_FALSE);
  return vec;
}

FD_EXPORT
fdtype fd_get_backtrace(struct FD_STACK *stack,fdtype rep)
{
  if (stack == NULL) stack=fd_stackptr;
  while (stack) {
    fdtype entry=stack2lisp(stack);
    rep=fd_init_pair(NULL,entry,rep);
    stack=stack->stack_caller;}
  return rep;
}

FD_EXPORT
void fd_sum_backtrace(u8_output out,fdtype backtrace)
{
  if (fd_stacktracep(backtrace)) {
    fdtype scan=backtrace;
    int n=0; while (FD_PAIRP(scan)) {
      fdtype entry = FD_CAR(scan);
      if ((FD_VECTORP(entry)) && (FD_VECTOR_LENGTH(entry)>=7)) {
	fdtype type=FD_VECTOR_REF(entry,1);
	fdtype label=FD_VECTOR_REF(entry,2);
	fdtype status=FD_VECTOR_REF(entry,3);
	if (n) u8_puts(out," ⇒ ");
	if (FD_STRINGP(label)) u8_puts(out,FD_STRDATA(label));
	else u8_putc(out,'?');
	if (FD_STRINGP(type)) {
	  u8_putc(out,'.');
	  u8_puts(out,FD_STRDATA(type));}
	else u8_puts(out,".?");
	if (FD_STRINGP(status)) {
	  u8_putc(out,'(');
	  u8_puts(out,FD_STRDATA(status));
	  u8_putc(out,')');}
	n++;}
      else if (FD_EXCEPTIONP(entry)) {
	struct FD_EXCEPTION_OBJECT *exo=(struct FD_EXCEPTION_OBJECT *)entry;
	u8_exception ex=exo->fdex_u8ex;
	if (n) u8_puts(out," ⇒ ");
	u8_puts(out,ex->u8x_cond);
	if (ex->u8x_context) u8_printf(out,"@%s",ex->u8x_context);
	if (ex->u8x_details) u8_printf(out," (%s)",ex->u8x_details);
	if (ex->u8x_free_xdata == fd_free_exception_xdata) {
	  fdtype irritant=(fdtype)ex->u8x_xdata;
	  char buf[32]; buf[0]='\0';
	  u8_sprintf(buf,32," =%q",irritant);
	  u8_puts(out,buf);}
	n++;}
      else {}
      scan=FD_CDR(scan);}}
}

void fd_init_stacks_c()
{
#if (FD_USE_TLS)
  u8_new_threadkey(&fd_stackptr_key,NULL);
#endif
  fd_stackptr=NULL;
}
