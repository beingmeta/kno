/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2010 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "eval_internals.h"

static fdtype else_symbol;

static fdtype if_handler(fdtype expr,fd_lispenv env)
{
  fdtype test_expr=fd_get_arg(expr,1), test_result;
  fdtype consequent_expr=fd_get_arg(expr,2);
  fdtype else_expr=fd_get_arg(expr,3);
  if ((FD_VOIDP(test_expr)) || (FD_VOIDP(consequent_expr)))
    return fd_err(fd_TooFewExpressions,"IF",NULL,expr);
  test_result=fd_eval(test_expr,env);
  if (FD_ABORTP(test_result)) return test_result;
  else if (FD_FALSEP(test_result))
    if (FD_PAIRP(else_expr))
      return fd_tail_eval(else_expr,env);
    else return fasteval(else_expr,env);
  else {
    fd_decref(test_result);
    if (FD_PAIRP(consequent_expr))
      return fd_tail_eval(consequent_expr,env);
    else return fasteval(consequent_expr,env);}
}

static fdtype ifelse_handler(fdtype expr,fd_lispenv env)
{
  fdtype test_expr=fd_get_arg(expr,1), test_result;
  fdtype consequent_expr=fd_get_arg(expr,2);
  if ((FD_VOIDP(test_expr)) || (FD_VOIDP(consequent_expr)))
    return fd_err(fd_TooFewExpressions,"IFELSE",NULL,expr);
  test_result=fd_eval(test_expr,env);
  if (FD_ABORTP(test_result)) return test_result;
  else if (FD_FALSEP(test_result)) {
    fdtype alt=FD_CDR(FD_CDR(expr));
    while (FD_PAIRP(alt)) {
      if (FD_PAIRP(FD_CDR(alt))) {
	fdtype alt_expr=FD_CAR(alt); alt=FD_CDR(alt);
	fdtype result=fd_eval(alt_expr,env);
	fd_decref(result);}
      else if (FD_PAIRP(FD_CAR(alt)))
	return fd_tail_eval(FD_CAR(alt),env);
      else return fasteval(FD_CAR(alt),env);}
    return FD_VOID;}
  else {
    fd_decref(test_result);
    if (FD_PAIRP(consequent_expr))
      return fd_tail_eval(consequent_expr,env);
    else return fasteval(consequent_expr,env);}
}

static fdtype tryif_handler(fdtype expr,fd_lispenv env)
{
  fdtype test_expr=fd_get_arg(expr,1), test_result;
  fdtype first_consequent=fd_get_arg(expr,2);
  if ((FD_VOIDP(test_expr)) || (FD_VOIDP(first_consequent)))
    return fd_err(fd_TooFewExpressions,"TRYIF",NULL,expr);
  test_result=fd_eval(test_expr,env);
  if (FD_ABORTP(test_result)) return test_result;
  else if (FD_FALSEP(test_result))
    return FD_EMPTY_CHOICE; 
  else {
    fdtype consequents=fd_get_body(expr,2), value=FD_VOID;
    FD_DOLIST(clause,consequents) {
      fd_decref(value); value=fd_eval(clause,env);
      if (!(FD_EMPTY_CHOICEP(value)))
	return value;}
    return value;}
}

static fdtype not_prim(fdtype arg)
{
  if (FD_FALSEP(arg)) return FD_TRUE; else return FD_FALSE;
}

static fdtype apply_marker;

static fdtype cond_handler(fdtype expr,fd_lispenv env)
{
  FD_DOLIST(clause,FD_CDR(expr)) {
    fdtype test_val;
    if (!(FD_PAIRP(clause)))
      return fd_err(fd_SyntaxError,_("invalid cond clause"),NULL,expr);
    else if (FD_EQ(FD_CAR(clause),else_symbol))
      return fd_eval_exprs(FD_CDR(clause),env);
    else test_val=fd_eval(FD_CAR(clause),env);
    if (FD_ABORTP(test_val)) return test_val;
    else if (FD_FALSEP(test_val)) {}
    else {
      fdtype applyp=((FD_PAIRP(FD_CDR(clause))) &&
		      (FD_EQ(FD_CAR(FD_CDR(clause)),apply_marker)));
      if (applyp)
	if (FD_PAIRP(FD_CDR(FD_CDR(clause)))) {
	  fdtype fnexpr=FD_CAR(FD_CDR(FD_CDR(clause)));
	  fdtype fn=fd_eval(fnexpr,env);
	  if (FD_ABORTP(fn)) {
	    fd_decref(test_val);
	    return fn;}
	  else if (FD_APPLICABLEP(fn)) {
	    fdtype retval=fd_apply(fn,1,&test_val);
	    fd_decref(test_val); fd_decref(fn);
	    return retval;}
	  else {
	    fd_decref(test_val);
	    return fd_type_error("function","cond_handler",fn);}}
	else return fd_err(fd_SyntaxError,"cond_handler","apply syntax",expr);
      else {
	fd_decref(test_val);
	return eval_body("COND",FD_CDR(clause),env);}}}
  return FD_VOID;
}

static fdtype case_handler(fdtype expr,fd_lispenv env)
{
  fdtype key_expr=fd_get_arg(expr,1), keyval;
  if (FD_VOIDP(key_expr))
    return fd_err(fd_SyntaxError,"case_handler",NULL,expr);
  else keyval=fd_eval(key_expr,env);
  if (FD_ABORTP(keyval)) return keyval;
  else {
    FD_DOLIST(clause,FD_CDR(FD_CDR(expr)))
      if (FD_PAIRP(clause))
	if (FD_PAIRP(FD_CAR(clause))) {
	  fdtype keys=FD_CAR(clause);
	  FD_DOLIST(key,keys)
	    if (FD_EQ(keyval,key))
	      return eval_body("CASE",FD_CDR(clause),env);}
	else if (FD_EQ(FD_CAR(clause),else_symbol)) {
	  fd_decref(keyval);
	  return fd_eval_exprs(FD_CDR(clause),env);}
	else return fd_err(fd_SyntaxError,"case_handler",NULL,clause);
      else return fd_err(fd_SyntaxError,"case_handler",NULL,clause);
    return FD_VOID;}
}

static fdtype when_handler(fdtype expr,fd_lispenv env)
{
  fdtype test_expr=fd_get_arg(expr,1), test_val;
  if (FD_VOIDP(test_expr))
    return fd_err(fd_TooFewExpressions,"WHEN",NULL,expr);
  else test_val=fd_eval(test_expr,env);
  if (FD_ABORTP(test_val)) return test_val;
  else if (FD_FALSEP(test_val)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(test_val)) return FD_VOID;
  else {
    fd_decref(test_val);
    return fd_eval_exprs(FD_CDR(FD_CDR(expr)),env);}
}

static fdtype unless_handler(fdtype expr,fd_lispenv env)
{
  fdtype test_expr=fd_get_arg(expr,1), test_val;
  if (FD_VOIDP(test_expr))
    return fd_err(fd_TooFewExpressions,"WHEN",NULL,expr);
  else test_val=fd_eval(test_expr,env);
  if (FD_ABORTP(test_val)) return test_val;
  else if (FD_FALSEP(test_val))
    return fd_eval_exprs(FD_CDR(FD_CDR(expr)),env);
  else if (FD_EMPTY_CHOICEP(test_val)) return FD_VOID;
  else {
    fd_decref(test_val);
    return FD_VOID;}
}

static fdtype and_handler(fdtype expr,fd_lispenv env)
{
  fdtype value=FD_TRUE;
  FD_DOLIST(clause,FD_CDR(expr)) {
    fd_decref(value);
    value=fd_eval(clause,env);
    if (FD_ABORTP(value)) return value;
    else if (FD_FALSEP(value)) return value;}
  return value;
}

static fdtype or_handler(fdtype expr,fd_lispenv env)
{
  fdtype value=FD_FALSE;
  FD_DOLIST(clause,FD_CDR(expr)) {
    fd_decref(value);
    value=fd_eval(clause,env);
    if (!(FD_FALSEP(value))) return value;}
  return value;
}

FD_EXPORT void fd_init_conditionals_c()
{
  fd_register_source_file(versionid);

  apply_marker=fd_intern("=>");
  else_symbol=fd_intern("ELSE");

  fd_defspecial(fd_scheme_module,"IF",if_handler);
  fd_defspecial(fd_scheme_module,"IFELSE",ifelse_handler);
  fd_defspecial(fd_scheme_module,"TRYIF",tryif_handler);
  fd_defspecial(fd_scheme_module,"COND",cond_handler);
  fd_defspecial(fd_scheme_module,"CASE",case_handler);
  fd_defspecial(fd_scheme_module,"WHEN",when_handler);
  fd_defspecial(fd_scheme_module,"UNLESS",unless_handler);
  fd_defspecial(fd_scheme_module,"AND",and_handler);
  fd_defspecial(fd_scheme_module,"OR",or_handler);
  fd_idefn(fd_scheme_module,fd_make_cprim1("NOT",not_prim,1));
}
