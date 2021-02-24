/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
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
#include "kno/lexenv.h"
#include "kno/stacks.h"
#include "kno/apply.h"
#include "apply_internals.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#if TIME_WITH_SYS_TIME
#include <time.h>
#include <sys/time.h>
#elif HAVE_TIME_H
#include <time.h>
#elif HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#include <sys/resource.h>
#include "kno/profiles.h"

#include <stdarg.h>

static lispval lookup(lispval name,lispval env)
{
  lispval scan = env, val = KNO_VOID;
  if (PAIRP(scan)) while (PAIRP(scan)) {
    lispval car = KNO_CAR(scan);
    val = lookup(name,car);
    if (KNO_VOIDP(val))
      scan = KNO_CDR(scan);
    else return val;}
  else if (KNO_TABLEP(scan)) {
    val = kno_get(scan,name,KNO_VOID);
    if (KNO_ABORTED(val)) {
      u8_log(LOGERR,"LookupError","Retrieving %q from %q",name,scan);
      kno_clear_errors(1);
      return KNO_VOID;}
    else return val;}
  else return KNO_VOID;
  if ( (KNO_NILP(scan)) || (KNO_FALSEP(scan)) )
    return KNO_VOID;
  else if (KNO_TABLEP(scan))
    return lookup(name,scan);
  else return KNO_VOID;
}

/* Initializations */

static lispval exec_expr(lispval expr,lispval env,kno_stack stack)
{
  lispval result = KNO_VOID, head = KNO_CAR(expr);
  if (!(KNO_SYMBOLP(head))) return kno_err(kno_SyntaxError,"exec_expr",NULL,expr);
  u8_string old_label = (stack) ? (stack->stack_label) : (NULL);
  lispval old_op = (stack) ? (stack->stack_op) : (KNO_VOID);
  lispval old_source = (stack) ? (stack->eval_source) : (KNO_VOID);
  int i = 0, n_args = kno_list_length(KNO_CDR(expr));
  if (n_args < 0) return kno_err(kno_SyntaxError,"exec_expr",NULL,expr);
  u8_string fcn_label = KNO_SYMBOL_NAME(head);
  u8_byte label_buf[128];
  stack->eval_source = expr;
  stack->stack_op = head;
  if (head == KNOSYM_QUOTE) {
    if (n_args!=1) return  kno_err(kno_SyntaxError,"exec_expr","QUOTE",expr);
    return kno_incref(KNO_CADR(expr));}
  else if (head == KNOSYM_ARG) {
    if (n_args!=1) return  kno_err(kno_SyntaxError,"exec_expr","KNOSYM",expr);
    lispval arg = KNO_CADR(expr);
    return lookup(arg,env);}
  else NO_ELSE;
  lispval handler = lookup(head,env);
  if (RARELY(!(KNO_APPLICABLEP(handler)))) {
    lispval err = kno_err(kno_NotAFunction,"exec_expr",fcn_label,handler);
    kno_decref(handler);
    return err;}
  lispval vals[n_args];
  KNO_DECL_STACKVEC(refs,n_args+1);
  kno_stackvec_push(refs,handler);
  lispval arglist = KNO_CDR(expr);
  KNO_DOLIST(arg,arglist) {
    lispval argval = arg;
    if (KNO_PAIRP(arg)) {
      stack->stack_label = u8_bprintf(label_buf,"%s[%d]",fcn_label,i);
      argval = exec_expr(arg,env,stack);
      if (KNO_ABORTED(argval)) {
	result=argval;
	goto done;}
      else if (KNO_VOIDP(argval)) {
	kno_seterr(kno_VoidArgument,"exec_expr",
		   u8_bprintf(label_buf,"%s[%d]",fcn_label,i),
		   arg);
	goto done;}
      if (KNO_CONSP(argval)) kno_stackvec_push(refs,argval);
      vals[i++]=argval;}
    else vals[i++]=arg;}
  stack->stack_label = fcn_label;
  result = kno_apply(handler,n_args,vals);
 done:
  kno_decref_stackvec(refs);
  stack->stack_label = old_label;
  stack->stack_op = old_op;
  stack->eval_source = old_source;
  return result;
}

KNO_EXPORT lispval kno_exec(lispval expr,lispval env,kno_stack stack)
{
  if (stack == NULL) stack = kno_stackptr;
  struct KNO_STACK exec_stack = { 0 };
  KNO_SETUP_STACK(&exec_stack,"kno_exec");
  KNO_STACK_SET_CALLER(&exec_stack,stack);
  exec_stack.stack_op = expr;
  exec_stack.eval_context = exec_stack.eval_source = expr;
  KNO_PUSH_STACK(&exec_stack);
  lispval val = exec_expr(expr,env,&exec_stack);
  KNO_POP_STACK(&exec_stack);
  return val;
}

/* KNO add */

static int check_path(lispval path,lispval map)
{
  if (path == map)
    return 1;
  else if (KNO_PAIRP(path)) {
    if (check_path(KNO_CAR(path),map))
      return 1;
    else if (check_path(KNO_CDR(path),map))
      return 1;
    else return 0;}
  else if (KNO_CHOICEP(path)) {
    KNO_DO_CHOICES(elt,path) {
      if (check_path(elt,map)) return 1;}
    return 0;}
  else return 0;
}

static lispval extend_env(lispval add,lispval base)
{
  if (KNO_CONSTANTP(add))
    return base;
  else if (KNO_PAIRP(add)) {
    lispval extended = extend_env(KNO_CDR(add),base);
    return extend_env(KNO_CAR(add),extended);}
  else if (KNO_TABLEP(add)) {
    if (check_path(base,add)) return base;
    else return kno_init_pair(NULL,kno_incref(add),base);}
  else if (KNO_CHOICEP(add)) {
    lispval path = base;
    KNO_DO_CHOICES(entry,add) {
      path = extend_env(entry,path);}
    return path;}
  else return base;
}

KNO_EXPORT lispval kno_exec_extend(lispval add,lispval path)
{
  return extend_env(add,path);
}

KNO_EXPORT void kno_init_exec_c()
{

}

