/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "eval_internals.h"

static fdtype else_symbol;

static fdtype if_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype test_expr = fd_get_arg(expr,1), test_result;
  fdtype consequent_expr = fd_get_arg(expr,2);
  fdtype else_expr = fd_get_arg(expr,3);
  if ((VOIDP(test_expr)) || (VOIDP(consequent_expr)))
    return fd_err(fd_TooFewExpressions,"IF",NULL,expr);
  test_result = fd_eval(test_expr,env);
  if (FD_ABORTED(test_result)) return test_result;
  else if (FALSEP(test_result))
    if ((PAIRP(else_expr))||(FD_CODEP(else_expr)))
      return fd_tail_eval(else_expr,env);
    else return fd_eval(else_expr,env);
  else {
    fd_decref(test_result);
    if ((PAIRP(consequent_expr))||(FD_CODEP(consequent_expr)))
      return fd_tail_eval(consequent_expr,env);
    else return fd_eval(consequent_expr,env);}
}

static fdtype ifelse_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype test_expr = fd_get_arg(expr,1), test_result;
  fdtype consequent_expr = fd_get_arg(expr,2);
  if ((VOIDP(test_expr)) || (VOIDP(consequent_expr)))
    return fd_err(fd_TooFewExpressions,"IFELSE",NULL,expr);
  test_result = fd_eval(test_expr,env);
  if (FD_ABORTED(test_result)) return test_result;
  else if (FALSEP(test_result)) {
    fdtype val = VOID;
    fdtype alt_body = fd_get_body(expr,3);
    FD_DOLIST(alt,alt_body) {
      fd_decref(val); val = fd_eval(alt,env);}
    return val;}
  else {
    fd_decref(test_result);
    if ((PAIRP(consequent_expr))||(FD_CODEP(consequent_expr)))
      return fd_tail_eval(consequent_expr,env);
    else return fd_eval(consequent_expr,env);}
}

static fdtype tryif_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype test_expr = fd_get_arg(expr,1), test_result;
  fdtype first_consequent = fd_get_arg(expr,2);
  if ((VOIDP(test_expr)) || (VOIDP(first_consequent)))
    return fd_err(fd_TooFewExpressions,"TRYIF",NULL,expr);
  test_result = fd_eval(test_expr,env);
  if (FD_ABORTED(test_result))
    return test_result;
  else if ((FALSEP(test_result))||(EMPTYP(test_result)))
    return EMPTY;
  else {
    fdtype value = VOID; fd_decref(test_result);
    {fdtype try_clauses = fd_get_body(expr,2);
      FD_DOLIST(clause,try_clauses) {
        fd_decref(value); value = fd_eval(clause,env);
        if (FD_ABORTED(value))
          return value;
        else if (VOIDP(value)) {
          fd_seterr(fd_VoidArgument,"tryif_evalfn",NULL,clause);
          return FD_ERROR;}
        else if (!(EMPTYP(value)))
          return value;}}
    return value;}
}

static fdtype not_prim(fdtype arg)
{
  if (FALSEP(arg)) return FD_TRUE; else return FD_FALSE;
}

static fdtype apply_marker;

static fdtype cond_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  FD_DOLIST(clause,FD_CDR(expr)) {
    fdtype test_val;
    if (!(PAIRP(clause)))
      return fd_err(fd_SyntaxError,_("invalid cond clause"),NULL,expr);
    else if (FD_EQ(FD_CAR(clause),else_symbol))
      return fd_eval_exprs(FD_CDR(clause),env);
    else test_val = fd_eval(FD_CAR(clause),env);
    if (FD_ABORTED(test_val)) return test_val;
    else if (FALSEP(test_val)) {}
    else {
      fdtype applyp = ((PAIRP(FD_CDR(clause))) &&
                      (FD_EQ(FD_CAR(FD_CDR(clause)),apply_marker)));
      if (applyp)
        if (PAIRP(FD_CDR(FD_CDR(clause)))) {
          fdtype fnexpr = FD_CAR(FD_CDR(FD_CDR(clause)));
          fdtype fn = fd_eval(fnexpr,env);
          if (FD_ABORTED(fn)) {
            fd_decref(test_val);
            return fn;}
          else if (FD_APPLICABLEP(fn)) {
            fdtype retval = fd_apply(fn,1,&test_val);
            fd_decref(test_val); fd_decref(fn);
            return retval;}
          else {
            fd_decref(test_val);
            return fd_type_error("function","cond_evalfn",fn);}}
        else return fd_err(fd_SyntaxError,"cond_evalfn","apply syntax",expr);
      else {
        fd_decref(test_val);
        return eval_body("COND",NULL,clause,1,env,_stack);}}}
  return VOID;
}

static fdtype case_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype key_expr = fd_get_arg(expr,1), keyval;
  if (VOIDP(key_expr))
    return fd_err(fd_SyntaxError,"case_evalfn",NULL,expr);
  else keyval = fd_eval(key_expr,env);
  if (FD_ABORTED(keyval)) return keyval;
  else {
    FD_DOLIST(clause,FD_CDR(FD_CDR(expr)))
      if (PAIRP(clause))
        if (PAIRP(FD_CAR(clause))) {
          fdtype keys = FD_CAR(clause);
          FD_DOLIST(key,keys)
            if (FD_EQ(keyval,key))
              return eval_body("CASE",NULL,clause,1,env,_stack);}
        else if (FD_EQ(FD_CAR(clause),else_symbol)) {
          fd_decref(keyval);
          return fd_eval_exprs(FD_CDR(clause),env);}
        else return fd_err(fd_SyntaxError,"case_evalfn",NULL,clause);
      else return fd_err(fd_SyntaxError,"case_evalfn",NULL,clause);
    return VOID;}
}

static fdtype when_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype test_expr = fd_get_arg(expr,1), test_val;
  if (VOIDP(test_expr))
    return fd_err(fd_TooFewExpressions,"WHEN",NULL,expr);
  else test_val = fd_eval(test_expr,env);
  if (FD_ABORTED(test_val)) return test_val;
  else if (FALSEP(test_val)) return VOID;
  else if (EMPTYP(test_val)) return VOID;
  else {
    fdtype result = fd_eval_exprs(FD_CDR(FD_CDR(expr)),env);
    fd_decref(test_val);
    FD_VOID_RESULT(result);
    return result;}
}

static fdtype unless_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype test_expr = fd_get_arg(expr,1), test_val;
  if (VOIDP(test_expr))
    return fd_err(fd_TooFewExpressions,"WHEN",NULL,expr);
  else test_val = fd_eval(test_expr,env);
  if (FD_ABORTED(test_val)) return test_val;
  else if (FALSEP(test_val)) {
    fdtype result = fd_eval_exprs(FD_CDR(FD_CDR(expr)),env);
    FD_VOID_RESULT(result);
    return result;}
  else if (EMPTYP(test_val))
    return VOID;
  else {
    fd_decref(test_val);
    return VOID;}
}

static fdtype and_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype value = FD_TRUE;
  FD_DOLIST(clause,FD_CDR(expr)) {
    fd_decref(value);
    value = fd_eval(clause,env);
    if (FD_ABORTED(value)) return value;
    else if (FALSEP(value)) return value;}
  return value;
}

static fdtype or_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype value = FD_FALSE;
  FD_DOLIST(clause,FD_CDR(expr)) {
    fd_decref(value);
    value = fd_eval(clause,env);
    if (FD_ABORTED(value)) return value;
    else if (!(FALSEP(value))) return value;}
  return value;
}

FD_EXPORT void fd_init_conditionals_c()
{
  u8_register_source_file(_FILEINFO);

  moduleid_symbol = fd_intern("%MODULEID");
  apply_marker = fd_intern("=>");
  else_symbol = fd_intern("ELSE");

  fd_defspecial(fd_scheme_module,"IF",if_evalfn);
  fd_defspecial(fd_scheme_module,"IFELSE",ifelse_evalfn);
  fd_defspecial(fd_scheme_module,"TRYIF",tryif_evalfn);
  fd_defspecial(fd_scheme_module,"COND",cond_evalfn);
  fd_defspecial(fd_scheme_module,"CASE",case_evalfn);
  fd_defspecial(fd_scheme_module,"WHEN",when_evalfn);
  fd_defspecial(fd_scheme_module,"UNLESS",unless_evalfn);
  fd_defspecial(fd_scheme_module,"AND",and_evalfn);
  fd_defspecial(fd_scheme_module,"OR",or_evalfn);
  fd_idefn(fd_scheme_module,fd_make_cprim1("NOT",not_prim,1));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
