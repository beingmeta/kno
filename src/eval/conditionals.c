/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "eval_internals.h"

static lispval else_symbol;

static lispval if_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), test_result;
  lispval consequent_expr = kno_get_arg(expr,2);
  lispval else_expr = kno_get_arg(expr,3);
  if ((VOIDP(test_expr)) || (VOIDP(consequent_expr)))
    return kno_err(kno_TooFewExpressions,"IF",NULL,expr);
  test_result = kno_eval(test_expr,env);
  if (KNO_ABORTED(test_result)) return test_result;
  else if (FALSEP(test_result))
    if ((PAIRP(else_expr))||(KNO_CODEP(else_expr)))
      return kno_tail_eval(else_expr,env);
    else return kno_eval(else_expr,env);
  else {
    kno_decref(test_result);
    if ((PAIRP(consequent_expr))||(KNO_CODEP(consequent_expr)))
      return kno_tail_eval(consequent_expr,env);
    else return kno_eval(consequent_expr,env);}
}

static lispval ifstar_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), test_result;
  lispval consequent_expr = kno_get_arg(expr,2);
  if ((VOIDP(test_expr)) || (VOIDP(consequent_expr)))
    return kno_err(kno_TooFewExpressions,"IF*",NULL,expr);
  test_result = kno_eval(test_expr,env);
  if (KNO_ABORTED(test_result))
    return test_result;
  else if ((FALSEP(test_result)) || (EMPTYP(test_result))) {
    lispval val = VOID;
    lispval alt_body = kno_get_body(expr,3);
    KNO_DOLIST(alt,alt_body) {
      kno_decref(val); val = kno_eval(alt,env);
      if (KNO_ABORTED(val)) return val;}
    return val;}
  else {
    kno_decref(test_result);
    if ((PAIRP(consequent_expr))||(KNO_CODEP(consequent_expr)))
      return kno_tail_eval(consequent_expr,env);
    else return kno_eval(consequent_expr,env);}
}

static lispval ifelse_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), test_result;
  lispval consequent_expr = kno_get_arg(expr,2);
  if ((VOIDP(test_expr)) || (VOIDP(consequent_expr)))
    return kno_err(kno_TooFewExpressions,"IFELSE",NULL,expr);
  test_result = kno_eval(test_expr,env);
  if (KNO_ABORTED(test_result)) return test_result;
  else if (FALSEP(test_result)) {
    lispval val = VOID;
    lispval alt_body = kno_get_body(expr,3);
    KNO_DOLIST(alt,alt_body) {
      kno_decref(val); val = kno_eval(alt,env);
      if (KNO_ABORTED(val)) return val;}
    return val;}
  else {
    kno_decref(test_result);
    if ((PAIRP(consequent_expr))||(KNO_CODEP(consequent_expr)))
      return kno_tail_eval(consequent_expr,env);
    else return kno_eval(consequent_expr,env);}
}

static lispval tryif_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), test_result;
  lispval first_consequent = kno_get_arg(expr,2);
  if ((VOIDP(test_expr)) || (VOIDP(first_consequent)))
    return kno_err(kno_TooFewExpressions,"TRYIF",NULL,expr);
  test_result = kno_eval(test_expr,env);
  if (KNO_ABORTED(test_result)) return test_result;
  else if ((FALSEP(test_result))||(EMPTYP(test_result)))
    return EMPTY;
  else {
    lispval value = VOID; kno_decref(test_result);
    {lispval try_clauses = kno_get_body(expr,2);
      KNO_DOLIST(clause,try_clauses) {
        kno_decref(value); value = kno_eval(clause,env);
        if (KNO_ABORTED(value)) return value;
        else if (VOIDP(value)) {
          kno_seterr(kno_VoidArgument,"tryif_evalfn",NULL,clause);
          return KNO_ERROR;}
        else if (!(EMPTYP(value)))
          return value;}}
    return value;}
}

static lispval not_prim(lispval arg)
{
  if (FALSEP(arg)) return KNO_TRUE; else return KNO_FALSE;
}

static lispval apply_marker;

static lispval cond_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  KNO_DOLIST(clause,KNO_CDR(expr)) {
    lispval test_val;
    if (!(PAIRP(clause)))
      return kno_err(kno_SyntaxError,_("invalid cond clause"),NULL,expr);
    else if (KNO_EQ(KNO_CAR(clause),else_symbol))
      return kno_eval_exprs(KNO_CDR(clause),env,_stack,1);
    else test_val = kno_eval(KNO_CAR(clause),env);
    if (KNO_ABORTED(test_val)) return test_val;
    else if (FALSEP(test_val)) {}
    else {
      lispval applyp = ((PAIRP(KNO_CDR(clause))) &&
                      (KNO_EQ(KNO_CAR(KNO_CDR(clause)),apply_marker)));
      if (applyp)
        if (PAIRP(KNO_CDR(KNO_CDR(clause)))) {
          lispval fnexpr = KNO_CAR(KNO_CDR(KNO_CDR(clause)));
          lispval fn = kno_eval(fnexpr,env);
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
        return eval_inner_body("COND",NULL,clause,1,env,_stack);}}}
  return VOID;
}

static lispval case_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval key_expr = kno_get_arg(expr,1), keyval;
  if (VOIDP(key_expr))
    return kno_err(kno_SyntaxError,"case_evalfn",NULL,expr);
  else keyval = kno_eval(key_expr,env);
  if (KNO_ABORTED(keyval)) return keyval;
  else {
    KNO_DOLIST(clause,KNO_CDR(KNO_CDR(expr)))
      if (PAIRP(clause))
        if (PAIRP(KNO_CAR(clause))) {
          lispval keys = KNO_CAR(clause);
          KNO_DOLIST(key,keys)
            if (KNO_EQ(keyval,key))
              return eval_inner_body("CASE",NULL,clause,1,env,_stack);}
        else if (KNO_EQ(KNO_CAR(clause),else_symbol)) {
          kno_decref(keyval);
          return kno_eval_exprs(KNO_CDR(clause),env,_stack,1);}
        else return kno_err(kno_SyntaxError,"case_evalfn",NULL,clause);
      else return kno_err(kno_SyntaxError,"case_evalfn",NULL,clause);
    return VOID;}
}

static lispval when_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), test_val;
  if (VOIDP(test_expr))
    return kno_err(kno_TooFewExpressions,"WHEN",NULL,expr);
  else test_val = kno_eval(test_expr,env);
  if (KNO_ABORTED(test_val)) return test_val;
  else if (FALSEP(test_val)) return VOID;
  else if (EMPTYP(test_val)) return VOID;
  else {
    lispval result = kno_eval_exprs(KNO_CDR(KNO_CDR(expr)),env,_stack,1);
    kno_decref(test_val);
    KNO_VOID_RESULT(result);
    return result;}
}

static lispval unless_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), test_val;
  if (VOIDP(test_expr))
    return kno_err(kno_TooFewExpressions,"WHEN",NULL,expr);
  else test_val = kno_eval(test_expr,env);
  if (KNO_ABORTED(test_val)) return test_val;
  else if (FALSEP(test_val)) {
    lispval result = kno_eval_exprs(KNO_CDR(KNO_CDR(expr)),env,_stack,1);
    KNO_VOID_RESULT(result);
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
  KNO_DOLIST(clause,KNO_CDR(expr)) {
    kno_decref(value);
    value = kno_eval(clause,env);
    if (KNO_ABORTED(value)) return value;
    else if (FALSEP(value)) return value;}
  return value;
}

static lispval or_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval value = KNO_FALSE;
  KNO_DOLIST(clause,KNO_CDR(expr)) {
    kno_decref(value);
    value = kno_eval(clause,env);
    if (KNO_ABORTED(value)) return value;
    else if (!(FALSEP(value))) return value;}
  return value;
}

KNO_EXPORT void kno_init_conditionals_c()
{
  u8_register_source_file(_FILEINFO);

  moduleid_symbol = kno_intern("%moduleid");
  apply_marker = kno_intern("=>");
  else_symbol = kno_intern("else");

  kno_def_evalfn(kno_scheme_module,"IF",
                "(IF *test* *then* [*else*]) "
                "returns *then* if *test* is neither #f or {}\n"
                "and *else* (if provided) when *test* is #f. "
                "Returns VOID otherwise.",
                if_evalfn);
  kno_def_evalfn(kno_scheme_module,"IF*",
                "(IF* *test* *then* *else*...) "
                "returns *then* if *test* is neither #f or {}. "
                "Otherwise (even if *test* fails (is {}), "
                "evaluate the *else* clauses in order. If there "
                "are no *else* clauses, returns VOID",
                ifstar_evalfn);
  kno_def_evalfn(kno_scheme_module,"IFELSE",
                "(IFELSE *test* *then* *else*...) "
                "returns *then* if *test* is neither #f or {}. "
                "If *test* is #f, evaluate the *else* clauses in order. "
                "If there are no *else* clauses, returns VOID",
                ifelse_evalfn);
  kno_def_evalfn(kno_scheme_module,"TRYIF","",tryif_evalfn);
  kno_def_evalfn(kno_scheme_module,"COND","",cond_evalfn);
  kno_def_evalfn(kno_scheme_module,"CASE","",case_evalfn);
  kno_def_evalfn(kno_scheme_module,"WHEN",
                "(WHEN *test* *clauses*...) returns VOID and "
                "evaluates *clauses* in order if *test* is not "
                "#f or {}",
                when_evalfn);
  kno_def_evalfn(kno_scheme_module,"UNLESS",
                "(WHEN *test* *clauses*...) returns VOID and "
                "evaluates *clauses* in order if *test* is #f.",
                unless_evalfn);
  kno_def_evalfn(kno_scheme_module,"AND",
                "(AND *clauses*..) evaluates *clauses* in order "
                "until one returns either #f or {}, which is then "
                "returned. If none return #f or #{}, return the result "
                "of the last clause",
                and_evalfn);
  kno_def_evalfn(kno_scheme_module,"OR",
                "(OR *clauses*..) evaluates *clauses* in order, "
                "returning the first non #f value. If a clause returns {} "
                "it is returned immediately.",
                or_evalfn);
  kno_idefn(kno_scheme_module,kno_make_cprim1("NOT",not_prim,1));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
