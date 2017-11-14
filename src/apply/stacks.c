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

static lispval stack_entry_symbol;

static lispval stringval(u8_string s)
{
  if (s==NULL)
    return FD_FALSE;
  else return lispval_string(s);
}

/* Stacks are rendered into LISP as vectors as follows:
   1. depth  (integer, increasing with calls)
   2. type   (apply, eval, load, other)
   3. label  (often the name of a function or expression type)
   4. status (a string (or #f) describing the state of the operation)
   5. op     (the thing being executed (expression, procedure applied, etc)
   6. args   (a vector of arguments to an applied procedure, or () otherwise)
   7. env    (the lexical environment established by the frame)
   8. source (the original source code for the call, if available)
 */

static lispval stack2lisp(struct FD_STACK *stack)
{
  int n = 8;
  lispval depth = FD_INT(stack->stack_depth);
  lispval type = FD_FALSE, label = FD_FALSE, status = FD_FALSE;
  lispval op = stack->stack_op, argvec = FD_FALSE, env = FD_FALSE;
  lispval source = FD_FALSE;
  if (stack->stack_type) type = fd_intern(stack->stack_type);
  if (stack->stack_label) label = lispval_string(stack->stack_label);
  if (stack->stack_status) status = lispval_string(stack->stack_status);
  if (FD_VOIDP(op)) op = FD_FALSE; else fd_incref(op);
  if ( stack->stack_args ) {
    lispval n=stack->n_args, i=0;
    lispval *args=stack->stack_args;
    argvec=fd_make_vector(n,args);
    while (i<n) {lispval elt=args[i++]; fd_incref(elt);}}
  if (stack->stack_env) {
    lispval bindings = stack->stack_env->env_bindings;
    if ( (SLOTMAPP(bindings)) || (SCHEMAPP(bindings)) ) {
      env = fd_copy(bindings);}}
  if (!((FD_NULLP(stack->stack_source))||(VOIDP(stack->stack_source)))) {
    source = stack->stack_source;
    fd_incref(source);}
  if (FD_FALSEP(status)) {
    n--; if (FD_FALSEP(env)) { n--; if (FD_FALSEP(source)) {
        n--; if (FD_FALSEP(argvec)) {
          n--;}}}}
  switch (n) {
  case 4:
    return fd_init_compound(NULL,stack_entry_symbol,0,4,depth,type,op,label);
  case 5:
    return fd_init_compound(NULL,stack_entry_symbol,0,5,
                            depth,type,op,label,argvec);
  case 6:
    return fd_init_compound(NULL,stack_entry_symbol,0,6,
                            depth,type,op,label,argvec,source);
  case 7:
    return fd_init_compound(NULL,stack_entry_symbol,0,7,
                            depth,type,op,label,argvec,source,env);
  default:
    return fd_init_compound(NULL,stack_entry_symbol,0,8,
                            depth,type,op,label,argvec,source,env,status);
  }
}

FD_EXPORT
lispval fd_get_backtrace(struct FD_STACK *stack)
{
  if (stack == NULL) stack=fd_stackptr;
  if (stack == NULL) return FD_EMPTY_LIST;
  int n = stack->stack_depth+1, i = stack->stack_depth;
  lispval result = fd_make_vector(n,NULL);
  while (stack) {
    lispval entry = stack2lisp(stack);
    FD_VECTOR_SET(result,i,entry);
    stack=stack->stack_caller;
    i--;}
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
      else if (FD_COMPOUND_TYPEP(entry,stack_entry_symbol)) {
        lispval type=FD_COMPOUND_REF(entry,1);
        lispval label=FD_COMPOUND_REF(entry,2);
        lispval status=FD_COMPOUND_REF(entry,3);
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

  stack_entry_symbol = fd_intern("%STACK");

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
