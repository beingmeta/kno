/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_EVAL_INTERNALS 1
#define KNO_INLINE_CHECKTYPE    (!(KNO_AVOID_INLINE))
#define KNO_INLINE_CHOICES      (!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_STACKS       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_LEXENV       (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "eval_internals.h"

static lispval else_symbol;

static lispval if_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), test_result;
  lispval consequent_expr = kno_get_arg(expr,2);
  lispval else_expr = kno_get_arg(expr,3);
  if ((VOIDP(test_expr)) || (VOIDP(consequent_expr)))
    return kno_err(kno_TooFewExpressions,"IF",NULL,expr);
  test_result = kno_eval(test_expr,env,_stack);
  if (KNO_ABORTED(test_result)) return test_result;
  else if (EMPTYP(test_result)) return test_result;
  else if (FALSEP(test_result)) {
    if (VOIDP(else_expr))
      return KNO_VOID;
    else return doeval(else_expr,env,_stack,1);}
  else {
    kno_decref(test_result);
    return doeval(consequent_expr,env,_stack,1);}
}

static lispval tryif_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), test_result = KNO_FALSE;
  lispval first_consequent = kno_get_arg(expr,2);
  if ((VOIDP(test_expr)) || (VOIDP(first_consequent)))
    return kno_err(kno_TooFewExpressions,"TRYIF",NULL,expr);
  int cmp = testeval(test_expr,env,TESTEVAL_FAIL_FALSE,&test_result,_stack);
  if (cmp<0)
    return test_result;
  else if (cmp == 0)
    return EMPTY;
  else kno_decref(test_result);
  lispval value = VOID;
  {lispval try_clauses = kno_get_body(expr,2);
    KNO_DOLIST(clause,try_clauses) {
      kno_decref(value);
      value = kno_eval(clause,env,_stack);
      if (KNO_ABORTED(value)) return value;
      else if (VOIDP(value)) {
	kno_seterr(kno_VoidArgument,"tryif_evalfn",NULL,clause);
	return KNO_ERROR;}
      else if (!(EMPTYP(value)))
	return value;}}
  return value;
}

DEFC_PRIM("not",not_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval not_prim(lispval arg)
{
  if (FALSEP(arg)) return KNO_TRUE;
  else if (EMPTYP(arg)) return KNO_EMPTY;
  else return KNO_FALSE;
}

static lispval apply_marker;

static lispval cond_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  int tail = KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS);
  KNO_DOLIST(clause,KNO_CDR(expr)) {
    lispval test_val;
    if (!(PAIRP(clause)))
      return kno_err(kno_SyntaxError,_("invalid cond clause"),NULL,expr);
    else if (KNO_EQ(KNO_CAR(clause),else_symbol))
      return eval_body(KNO_CDR(clause),env,_stack,"COND","else",tail);
    else test_val = kno_eval(KNO_CAR(clause),env,_stack);
    if (KNO_ABORTED(test_val)) return test_val;
    /* else if (EMPTYP(test_val)) return KNO_EMPTY; */
    else if (FALSEP(test_val)) {}
    else {
      lispval applyp = ((PAIRP(KNO_CDR(clause))) &&
			(KNO_EQ(KNO_CAR(KNO_CDR(clause)),apply_marker)));
      if (applyp)
	if (PAIRP(KNO_CDR(KNO_CDR(clause)))) {
	  lispval fnexpr = KNO_CAR(KNO_CDR(KNO_CDR(clause)));
	  lispval fn = kno_eval(fnexpr,env,_stack);
	  if (KNO_ABORTED(fn)) {
	    kno_decref(test_val);
	    return fn;}
	  else if (KNO_APPLICABLEP(fn)) {
	    lispval retval = kno_apply(fn,1,&test_val);
	    kno_decref(test_val); kno_decref(fn);
	    return retval;}
	  else {
	    kno_decref(test_val);
	    return kno_type_error("function","cond_evalfn",fn);}}
	else return kno_err(kno_SyntaxError,"cond_evalfn","apply syntax",expr);
      else {
	kno_decref(test_val);
	return eval_body(KNO_CDR(clause),env,_stack,"COND",NULL,tail);}}}
  return VOID;
}

static lispval case_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval key_expr = kno_get_arg(expr,1), keyval;
  if (VOIDP(key_expr))
    return kno_err(kno_SyntaxError,"case_evalfn",NULL,expr);
  else keyval = kno_eval(key_expr,env,_stack);
  if (KNO_ABORTED(keyval)) return keyval;
  else {
    int tail = KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS);
    KNO_DOLIST(clause,KNO_CDR(KNO_CDR(expr)))
      if (PAIRP(clause))
	if (PAIRP(KNO_CAR(clause))) {
	  lispval keys = KNO_CAR(clause);
	  KNO_DOLIST(key,keys)
	    if (KNO_EQ(keyval,key))
	      return eval_body(KNO_CDR(clause),env,_stack,"CASE",NULL,tail);}
	else if (KNO_EQ(KNO_CAR(clause),else_symbol)) {
	  kno_decref(keyval);
	  return eval_body(KNO_CDR(clause),env,_stack,"CASE","else",tail);}
	else return kno_err(kno_SyntaxError,"case_evalfn",NULL,clause);
      else return kno_err(kno_SyntaxError,"case_evalfn",NULL,clause);
    return VOID;}
}

static lispval when_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), test_val;
  if (VOIDP(test_expr))
    return kno_err(kno_TooFewExpressions,"WHEN",NULL,expr);
  else test_val = kno_eval(test_expr,env,_stack);
  if (KNO_ABORTED(test_val)) return test_val;
  else if (FALSEP(test_val)) return VOID;
  else if (EMPTYP(test_val)) return VOID;
  else {
    int tail_flags = (KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS)) ?
      (KNO_TAIL_EVAL|KNO_VOID_VAL) : (0);
    kno_decref(test_val);
    lispval result = eval_body
      (KNO_CDR(KNO_CDR(expr)),env,_stack,"WHEN",NULL,tail_flags);
    return result;}
}

static lispval unless_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), test_val;
  if (VOIDP(test_expr))
    return kno_err(kno_TooFewExpressions,"UNLESS",NULL,expr);
  else test_val = kno_eval(test_expr,env,_stack);
  if (KNO_ABORTED(test_val)) return test_val;
  else if (FALSEP(test_val)) {
    int tail_flags = (KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS)) ?
      (KNO_TAIL_EVAL|KNO_VOID_VAL) : (0);
    lispval result = eval_body(KNO_CDR(KNO_CDR(expr)),env,_stack,
			       "UNLESS",NULL,
			       tail_flags);
    return result;}
  else if (EMPTYP(test_val))
    return VOID;
  else {
    kno_decref(test_val);
    return VOID;}
}

static lispval and_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval value = KNO_TRUE;
  /* Evaluate clauses until you get an error or a false/empty value */
  KNO_DOLIST(clause,KNO_CDR(expr)) {
    kno_decref(value);
    value = kno_eval(clause,env,_stack);
    if (KNO_ABORTED(value))
      return value;
    else if ( (FALSEP(value)) || (EMPTYP(value)) )
      return value;
    else NO_ELSE;}
  return value;
}

static lispval or_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval value = KNO_FALSE;
  /* Evaluate clauses until you get an error or a non-false/non-empty value */
  KNO_DOLIST(clause,KNO_CDR(expr)) {
    kno_decref(value);
    value = kno_eval(clause,env,_stack);
    if (KNO_ABORTED(value))
      return value;
    else if ( (FALSEP(value)) || (EMPTYP(value)) ) {}
    else return value;}
  return value;
}

KNO_EXPORT void kno_init_conditionals_c()
{
  u8_register_source_file(_FILEINFO);

  apply_marker = kno_intern("=>");
  else_symbol = kno_intern("else");

  link_local_cprims();

  kno_def_evalfn(kno_scheme_module,"IF",if_evalfn,
		 "(IF *test* *then* [*else*]) "
		 "returns *then* if *test* is neither #f or {}\n"
		 "and *else* (if provided) when *test* is #f. "
		 "Returns VOID otherwise.");
  kno_def_evalfn(kno_scheme_module,"TRYIF",tryif_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"COND",cond_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"CASE",case_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"WHEN",when_evalfn,
		 "(WHEN *test* *clauses*...) returns VOID and "
		 "evaluates *clauses* in order if *test* is not "
		 "#f or {}");
  kno_def_evalfn(kno_scheme_module,"UNLESS",unless_evalfn,
		 "(WHEN *test* *clauses*...) returns VOID and "
		 "evaluates *clauses* in order if *test* is #f.");
  kno_def_evalfn(kno_scheme_module,"AND",and_evalfn,
		 "(AND *clauses*..) evaluates *clauses* in order "
		 "until one returns either #f or {}, which is then "
		 "returned. If none return #f or #{}, return the result "
		 "of the last clause");
  kno_def_evalfn(kno_scheme_module,"OR",or_evalfn,
		 "(OR *clauses*..) evaluates *clauses* in order, "
		 "returning the first non #f value. If a clause returns {} "
		 "it is returned immediately.");
}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;
  KNO_LINK_CPRIM("not",not_prim,1,scheme_module);
}
