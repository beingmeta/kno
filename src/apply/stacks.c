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

FD_EXPORT
fdtype fd_get_backtrace(struct FD_STACK *stack,fdtype rep)
{
  if (stack == NULL) stack=fd_stackptr;
  while (stack) {
    u8_string summary=NULL;
    U8_FIXED_OUTPUT(tmp,128);
    if ( (stack->stack_label) || (stack->stack_status) ) {
      summarize_stack_frame(tmpout,stack);
      summary=tmp.u8_outbuf;}
    if (stack->stack_env) {
      fdtype bindings = stack->stack_env->env_bindings;
      if ( (FD_SLOTMAPP(bindings)) || (FD_SCHEMAPP(bindings)) ) {
	rep=fd_init_pair( NULL, fd_copy(bindings), rep);}}
    if ( stack->stack_args ) {
      fdtype *args=stack->stack_args;
      int i=0, n=stack->n_args;
      fdtype tmpbuf[n+1], *write=&tmpbuf[1];
      tmpbuf[0]=stack->stack_op;
      fd_incref(stack->stack_op);
      while (i<n) {
	fdtype v=args[i];
	fd_incref(v);
	*write++=v;
	i++;}
      fdtype vec = fd_make_vector(n+1,tmpbuf);
      rep=fd_init_pair(NULL,vec,rep);}
    else if (!(FD_VOIDP(stack->stack_op)))
      rep = fd_init_pair( NULL, fd_incref(stack->stack_op), rep);
    if (summary)
      rep = fd_init_pair(NULL,fd_make_string(NULL,-1,summary),rep);
    stack=stack->stack_caller;}
  return rep;
}

FD_EXPORT
void fd_sum_backtrace(u8_output out,fdtype backtrace)
{
  if (fd_stacktracep(backtrace)) {
    fdtype scan=backtrace;
    int n=0; while (FD_PAIRP(scan)) {
      fdtype car = FD_CAR(scan);
      if (FD_STRINGP(car)) {
	if (n) u8_puts(out," ⇒ ");
	u8_putn(out,FD_STRDATA(car),FD_STRLEN(car));
	n++;}
      else if (FD_EXCEPTIONP(car)) {
	struct FD_EXCEPTION_OBJECT *exo=
	  (struct FD_EXCEPTION_OBJECT *)car;
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
