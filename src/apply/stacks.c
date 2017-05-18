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

FD_EXPORT
fdtype fd_get_backtrace(struct FD_STACK *stack,fdtype rep)
{
  if (stack == NULL) stack=fd_stackptr;
  while (stack) {
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
    if (stack->stack_env) {
      fdtype bindings = stack->stack_env->env_bindings;
      if ( (FD_SLOTMAPP(bindings)) || (FD_SCHEMAPP(bindings)) ) {
	rep=fd_init_pair( NULL, fd_copy(bindings), rep);}}
    stack=stack->stack_caller;}
  return rep;
}

FD_EXPORT void fd_init_stacks_c()
{
}
