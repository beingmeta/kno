/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
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

/* Initializations */

KNO_EXPORT lispval kno_exec(lispval expr,lispval handlers,kno_stack stack)
{
  lispval result = KNO_VOID;
  lispval head = (KNO_PAIRP(expr)) ? (KNO_CAR(expr)) :
    (KNO_COMPOUNDP(expr)) ? (KNO_COMPOUND_TAG(expr)) : (KNO_VOID);
  if (KNO_VOIDP(head))
    return kno_incref(expr);
  lispval handler = KNO_VOID;
  int free_handler = 0, n =
    (KNO_PAIRP(expr)) ? (kno_list_length(expr)) :
    (KNO_COMPOUNDP(expr)) ? (KNO_COMPOUND_LENGTH(expr)) : (-1);
  if (n<0) return kno_err(kno_SyntaxError,"kno_exec",NULL,expr);
  if (KNO_APPLICABLEP(head))
    handler=head;
  else {
    handler = kno_get(handlers,head,KNO_VOID);
    free_handler = 1;}
  if (!(KNO_APPLICABLEP(handler)))
    return kno_err(kno_NotAFunction,"kno_exec",NULL,handler);
  lispval vals[n];
  lispval to_free[n];
  int free_n = 0;
  if (KNO_COMPOUNDP(expr)) {
    lispval *args = KNO_COMPOUND_ELTS(expr);
    int i=0; while (i<n) {
      lispval arg = args[i];
      if ( (KNO_CONSP(arg)) && ( (KNO_PAIRP(arg)) || (KNO_COMPOUNDP(arg)) ) ) {
	lispval argval = kno_exec(arg,handlers,stack);
	if (KNO_ABORTED(argval)) {result=argval; goto done;}
	vals[i++] = argval; to_free[free_n++]=argval;}
      else vals[i++]=arg;}}
  else {
    int i = 0;
    lispval arglist = KNO_CDR(expr); n--;
    KNO_DOLIST(arg,arglist) {
      if ( (KNO_CONSP(arg)) && ( (KNO_PAIRP(arg)) || (KNO_COMPOUNDP(arg)) ) ) {
	lispval argval = kno_exec(arg,handlers,stack);
	if (KNO_ABORTED(argval)) {result=argval; goto done;}
	vals[i++] = argval; to_free[free_n++]=argval;}
      else vals[i++]=arg;}}
  result = kno_apply(handler,n,vals);
  int i=0;
 done:
  while (i<free_n) { kno_decref(to_free[i]); i++;}
  return result;
}

KNO_EXPORT void kno_init_exec_c()
{

}

