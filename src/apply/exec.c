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

static u8_string namestring(lispval handler,u8_byte buf[128])
{
  if (KNO_SYMBOLP(handler))
    return KNO_SYMBOL_NAME(handler);
  else if (KNO_STRINGP(handler))
    return KNO_CSTRING(handler);
  else if (KNO_CONSTANTP(handler))
    return kno_constant_name(handler);
  else return u8_bprintf(buf,"%q",handler);
}

static lispval lookup(lispval name,lispval env)
{
  lispval scan = env, handler = KNO_VOID;
  while (PAIRP(scan)) {
    lispval car = KNO_CAR(scan);
    handler = (KNO_HASHTABLEP(car)) ?
      (kno_hashtable_get((kno_hashtable)car,name,KNO_VOID)) :
      (KNO_TABLEP(car)) ? (kno_get(car,name,KNO_VOID)) :
      (KNO_VOID);
    if (KNO_VOIDP(car))
      scan = KNO_CDR(scan);
    else return handler;}
  if ( (KNO_NILP(scan)) || (KNO_FALSEP(scan)) )
    return name;
  else if (KNO_HASHTABLEP(scan))
    return kno_hashtable_get((kno_hashtable)scan,name,name);
  else if (KNO_TABLEP(scan))
    return kno_get(scan,name,name);
  else return name;
}

/* Initializations */

KNO_EXPORT lispval kno_exec(lispval expr,lispval env,kno_stack stack)
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
  if (head == KNOSYM_QUOTE) {
    if (n>1) {
      if (KNO_PAIRP(expr))
	return (KNO_CADR(expr));
      else return KNO_COMPOUND_REF(expr,0);}
    else return kno_err(kno_SyntaxError,"kno_exec","quote",expr);}
  else if ( (KNO_SYMBOLP(head)) || (KNO_CONSTANTP(head)) ){
    handler = lookup(head,env);
    free_handler = 1;}
  else handler=head;
  if (!(KNO_APPLICABLEP(handler))) {
    u8_byte buf[128];
    lispval err = kno_err(kno_NotAFunction,"kno_exec",
			  namestring(head,buf),
			  handler);
    if (free_handler) kno_decref(handler);
    return err;}

  lispval vals[n];
  lispval to_free[n];
  int free_n = 0;
  if (KNO_COMPOUNDP(expr)) {
    lispval *args = KNO_COMPOUND_ELTS(expr);
    int i=0; while (i<n) {
      lispval arg = args[i], argval = arg;
      if ( (KNO_SYMBOLP(head)) || (KNO_CONSTANTP(head)) )
	argval = lookup(head,env);
      else if ( (KNO_CONSP(arg)) &&
		( (KNO_PAIRP(arg)) || (KNO_COMPOUNDP(arg)) ) ) {
	argval = kno_exec(arg,env,stack);
	if (KNO_ABORTED(argval)) {result=argval; goto done;}
	vals[i++] = argval;
	if (KNO_CONSP(argval)) to_free[free_n++]=argval;}
      else vals[i++]=argval;}}
  else {
    int i = 0;
    lispval arglist = KNO_CDR(expr); n--;
    KNO_DOLIST(arg,arglist) {
      lispval argval = arg;
      if ( (KNO_SYMBOLP(head)) || (KNO_CONSTANTP(head)) )
	argval = lookup(head,env);
      else if ( (KNO_CONSP(arg)) &&
		( (KNO_PAIRP(arg)) || (KNO_COMPOUNDP(arg)) ) ) {
	argval = kno_exec(arg,env,stack);
	if (KNO_ABORTED(argval)) {
	  result=argval; goto done;}}
      else {
	vals[i++]=argval;
	continue;}
      vals[i++]=argval;
      if (KNO_CONSP(argval)) to_free[free_n++]=argval;}
    result = kno_apply(handler,n,vals);}
  int i=0;
 done:
  while (i<free_n) { kno_decref(to_free[i]); i++;}
  if (free_handler) kno_decref(handler);
  return result;
}

KNO_EXPORT void kno_init_exec_c()
{

}

