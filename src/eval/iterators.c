/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
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
#include "framerd/sequences.h"
#include "framerd/storage.h"
#include "framerd/numbers.h"
#include "eval_internals.h"

/* Helper functions */

static lispval iter_var;

/* Simple iterations */

static lispval while_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval test_expr = fd_get_arg(expr,1);
  lispval body = fd_get_body(expr,1);
  lispval result = VOID;
  if (VOIDP(test_expr))
    return fd_err(fd_TooFewExpressions,"WHILE",NULL,expr);
  else {
    while ((VOIDP(result)) &&
           (testeval(test_expr,env,&result,_stack))) {
      FD_DOLIST(iter_expr,body) {
        lispval val = fast_eval(iter_expr,env);
        if (FD_BROKEP(val))
          return FD_VOID;
        else if (FD_ABORTED(val))
          return val;
        fd_decref(val);}}}
  if (FD_ABORTED(result))
    return result;
  else return VOID;
}

static lispval until_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval test_expr = fd_get_arg(expr,1);
  lispval body = fd_get_body(expr,1);
  lispval result = VOID;
  if (VOIDP(test_expr))
    return fd_err(fd_TooFewExpressions,"UNTIL",NULL,expr);
  else {
    while ((VOIDP(result)) &&
           (!(testeval(test_expr,env,&result,_stack)))) {
      FD_DOLIST(iter_expr,body) {
        lispval val = fast_eval(iter_expr,env);
        if (FD_BROKEP(val))
          return FD_VOID;
        else if (FD_ABORTED(val))
          return val;
        else fd_decref(val);}}}
  if (FD_ABORTED(result))
    return result;
  else return VOID;
}

/* Parsing for more complex iterations. */

static lispval parse_control_spec
  (lispval expr,lispval *varp,lispval *count_var,
   fd_lexenv env,fd_stack _stack)
{
  lispval control_expr = fd_get_arg(expr,1);
  if (VOIDP(control_expr))
    return fd_err(fd_TooFewExpressions,"DO...",NULL,expr);
  else {
    lispval var = fd_get_arg(control_expr,0);
    lispval ivar = fd_get_arg(control_expr,2);
    lispval val_expr = fd_get_arg(control_expr,1), val;
    if (VOIDP(control_expr))
      return fd_err(fd_TooFewExpressions,"DO...",NULL,expr);
    else if (VOIDP(val_expr))
      return fd_err(fd_TooFewExpressions,"DO...",NULL,control_expr);
    else if (!(SYMBOLP(var)))
      return fd_err(fd_SyntaxError,
                    _("identifier is not a symbol"),NULL,control_expr);
    else if (!((VOIDP(ivar)) || (SYMBOLP(ivar))))
      return fd_err(fd_SyntaxError,
                    _("identifier is not a symbol"),NULL,control_expr);
    val = fast_eval(val_expr,env);
    if (FD_ABORTED(val))
      return val;
    *varp = var;
    if (count_var) *count_var = ivar;
    return val;}
}

/* DOTIMES */

static lispval dotimes_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  int i = 0, limit;
  lispval var, limit_val=
    parse_control_spec(expr,&var,NULL,env,_stack);;
  lispval body = fd_get_body(expr,2);
  if (FD_ABORTED(var))
    return var;
  else if (FD_ABORTED(limit_val))
    return limit_val;
  else if (!(FD_UINTP(limit_val)))
    return fd_type_error("fixnum","dotimes_evalfn",limit_val);
  else limit = FIX2INT(limit_val);
  INIT_STACK_ENV(_stack,dotimes,env,1);
  dotimes_vars[0]=var;
  dotimes_vals[0]=FD_INT(0);
  while (i < limit) {
    dotimes_vals[0]=FD_INT(i);
    {FD_DOLIST(subexpr,body) {
        lispval val = fast_eval(subexpr,dotimes);
        if (FD_BROKEP(val))
          _return VOID;
        else if (FD_ABORTED(val))
          _return val;
        else fd_decref(val);}}
    reset_env(dotimes);
    i++;}
  _return VOID;
}

/* DOSEQ */

static lispval doseq_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  int i = 0, lim, islist = 0;
  lispval var, count_var = VOID;
  lispval seq = parse_control_spec(expr,&var,&count_var,env,_stack);
  lispval body = fd_get_body(expr,2);
  lispval pairscan = VOID;
  if (FD_ABORTED(var))
    return var;
  else if (FD_ABORTED(seq))
    return seq;
  else if (EMPTYP(seq))
    return VOID;
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","doseq_evalfn",seq);
  else lim = fd_seq_length(seq);
  if (lim==0) {
    fd_decref(seq);
    return VOID;}
  INIT_STACK_ENV(_stack,doseq,env,2);
  CHOICE_ADD((_stack->stack_vals),seq);
  if (PAIRP(seq)) {
    pairscan = seq;
    islist = 1;}
  doseq_vars[0]=var;
  if (SYMBOLP(count_var))
    doseq_vars[1]=count_var;
  else doseq_bindings.schema_length=1;
  while (i<lim) {
    lispval elt = (islist) ? (fd_refcar(pairscan)) : (fd_seq_elt(seq,i));
    doseq_vals[0]=elt;
    doseq_vals[1]=FD_INT(i);
    {FD_DOLIST(subexpr,body) {
        lispval val = fast_eval(subexpr,doseq);
        if (FD_BROKEP(val))
          _return FD_VOID;
        else if (FD_ABORTED(val))
          _return val;
        else fd_decref(val);}}
    reset_env(doseq);
    fd_decref(doseq_vals[0]);
    doseq_vals[0]=VOID;
    doseq_vals[1]=VOID;
    if (islist) pairscan = FD_CDR(pairscan);
    i++;}
  _return VOID;
}

/* FORSEQ */

static lispval forseq_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  size_t i = 0, lim=0; int islist=0;
  lispval var, count_var = VOID, *results, result=VOID;
  lispval seq = parse_control_spec(expr,&var,&count_var,env,_stack);
  lispval body = fd_get_body(expr,2), pairscan=VOID;
  if (FD_ABORTED(var))
    return var;
  else if (FD_ABORTED(seq))
    return seq;
  else if (EMPTYP(seq))
    return EMPTY;
  else if (NILP(seq))
    return seq;
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","forseq_evalfn",seq);
  else lim = fd_seq_length(seq);
  if (lim==0)
    return seq;
  else if (PAIRP(seq)) {
    islist = 1;
    pairscan = seq;}
  else {}
  INIT_STACK_ENV(_stack,forseq,env,2);
  CHOICE_ADD((_stack->stack_vals),seq);
  forseq_vars[0]=var;
  if (SYMBOLP(count_var)) {
    forseq_vars[1]=count_var;
    forseq_vals[1]=FD_FIXZERO;}
  else forseq_bindings.schema_length=1;
  results = fd_init_elts(NULL,lim,VOID);
  while ( i < lim ) {
    lispval elt = (islist) ? (fd_refcar(pairscan)) : (fd_seq_elt(seq,i));
    lispval val = VOID;
    forseq_vals[0]=elt;
    forseq_vals[1]=FD_INT(i);
    {FD_DOLIST(subexpr,body) {
        fd_decref(val);
        val = fast_eval(subexpr,forseq);
        if (FD_BROKEP(val)) {
          result=fd_makeseq(FD_PTR_TYPE(seq),i,results);
          fd_decref_vec(results,i);
          u8_free(results);
          _return result;}
        else if (FD_ABORTED(val)) {
          fd_decref_vec(results,i);
          u8_free(results);
          _return val;}}}
    results[i]=val;
    reset_env(forseq);
    fd_decref(forseq_vals[0]);
    forseq_vals[0]=VOID;
    forseq_vals[1]=VOID;
    if (islist) pairscan = FD_CDR(pairscan);
    i++;}
  result=fd_makeseq(FD_PTR_TYPE(seq),lim,results);
  fd_decref_vec(results,i);
  u8_free(results);
  _return result;
}

/* TRYSEQ */

static lispval tryseq_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  size_t i = 0, lim=0; int islist=0;
  lispval var, count_var = VOID;
  lispval seq = parse_control_spec(expr,&var,&count_var,env,_stack);
  lispval body = fd_get_body(expr,2), pairscan=VOID;
  if (FD_ABORTED(var))
    return var;
  else if (FD_ABORTED(seq))
    return seq;
  else if (EMPTYP(seq))
    return EMPTY;
  else if (NILP(seq))
    return EMPTY;
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","tryseq_evalfn",seq);
  else lim = fd_seq_length(seq);
  if (lim==0)
    return EMPTY;
  else if (PAIRP(seq)) {
    islist = 1;
    pairscan = seq;}
  else {}
  INIT_STACK_ENV(_stack,tryseq,env,2);
  tryseq_vars[0]=var;
  CHOICE_ADD((_stack->stack_vals),seq);
  if (SYMBOLP(count_var)) {
    tryseq_vars[1]=count_var;
    tryseq_vals[1]=FD_FIXZERO;}
  else tryseq_bindings.schema_length=1;
  while ( i < lim ) {
    lispval elt = (islist) ? (fd_refcar(pairscan)) : (fd_seq_elt(seq,i));
    lispval val = VOID;
    tryseq_vals[0]=elt;
    tryseq_vals[1]=FD_INT(i);
    {FD_DOLIST(subexpr,body) {
        fd_decref(val);
        val = fast_eval(subexpr,tryseq);
        if (FD_BROKEP(val))
          _return EMPTY;
        else if (FD_ABORTED(val))
          _return val;}}
    reset_env(tryseq);
    fd_decref(tryseq_vals[0]);
    tryseq_vals[0]=VOID;
    tryseq_vals[1]=VOID;
    if (! (EMPTYP(val)) )
      _return val;
    else {
      if (islist) pairscan = FD_CDR(pairscan);
      i++;}}
  _return EMPTY;
}

/* DOLIST */

static lispval dolist_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  int i = 0;
  lispval var, count_var = VOID;
  lispval seq = parse_control_spec(expr,&var,&count_var,env,_stack);
  lispval body = fd_get_body(expr,2);
  lispval pairscan = VOID;
  if (FD_ABORTED(var))
    return var;
  else if (FD_ABORTED(seq))
    return seq;
  else if (EMPTYP(seq))
    return VOID;
  else if (NILP(seq))
    return VOID;
  else if (!(PAIRP(seq)))
    return fd_type_error("pair","dolist_evalfn",seq);
  else pairscan = seq;
  INIT_STACK_ENV(_stack,dolist,env,2);
  CHOICE_ADD((_stack->stack_vals),seq);
  dolist_vars[0]=var;
  if (SYMBOLP(count_var))
    dolist_vars[1]=count_var;
  else dolist_bindings.schema_length=1;
  while (PAIRP(pairscan)) {
    lispval elt = FD_CAR(pairscan); fd_incref(elt);
    dolist_vals[0]=elt;
    dolist_vals[1]=FD_INT(i);
    {FD_DOLIST(subexpr,body) {
        lispval val = fast_eval(subexpr,dolist);
        if (FD_BROKEP(val))
          _return FD_VOID;
        else if (FD_ABORTED(val))
          _return val;
        else fd_decref(val);}}
    reset_env(dolist);
    fd_decref(dolist_vals[0]);
    dolist_vals[0]=VOID;
    dolist_vals[1]=VOID;
    pairscan = FD_CDR(pairscan);
    i++;}
  _return VOID;
}

/* BEGIN, PROG1, and COMMENT */

static lispval begin_evalfn(lispval begin_expr,fd_lexenv env,fd_stack _stack)
{
  return eval_inner_body("BEGIN",NULL,begin_expr,1,env,_stack);
}

static lispval onbreak_evalfn(lispval begin_expr,fd_lexenv env,fd_stack _stack)
{
  lispval result = eval_inner_body("ONBREAK",NULL,begin_expr,2,env,_stack);
  if (FD_BREAKP(result))
    result = fast_stack_eval(fd_get_arg(begin_expr,1),env,_stack);
  return result;
}

static lispval prog1_evalfn(lispval prog1_expr,fd_lexenv env,fd_stack _stack)
{
  lispval arg1 = fd_get_arg(prog1_expr,1);
  lispval result = fd_stack_eval(arg1,env,_stack,0);
  if (FD_ABORTED(result))
    return result;
  else {
    lispval prog1_body = fd_get_body(prog1_expr,2);
    FD_DOLIST(subexpr,prog1_body) {
      lispval tmp = fast_eval(subexpr,env);
      if (FD_ABORTED(tmp)) {
        fd_decref(result);
        return tmp;}
      fd_decref(tmp);}
    return result;}
}

static lispval comment_evalfn(lispval comment_expr,fd_lexenv env,fd_stack stack)
{
  return VOID;
}

/* Initialize functions */

FD_EXPORT void fd_init_iterators_c()
{
  moduleid_symbol = fd_intern("%MODULEID");
  iter_var = fd_intern("%ITER");

  u8_register_source_file(_FILEINFO);

  fd_def_evalfn(fd_scheme_module,"UNTIL","",until_evalfn);
  fd_def_evalfn(fd_scheme_module,"WHILE","",while_evalfn);
  fd_def_evalfn(fd_scheme_module,"DOTIMES","",dotimes_evalfn);
  fd_def_evalfn(fd_scheme_module,"DOLIST","",dolist_evalfn);
  fd_def_evalfn(fd_scheme_module,"DOSEQ","",doseq_evalfn);
  fd_def_evalfn(fd_scheme_module,"FORSEQ","",forseq_evalfn);
  fd_def_evalfn(fd_scheme_module,"TRYSEQ","",tryseq_evalfn);

  fd_def_evalfn(fd_scheme_module,"BEGIN","",begin_evalfn);
  fd_def_evalfn(fd_scheme_module,"PROG1","",prog1_evalfn);
  fd_def_evalfn(fd_scheme_module,"COMMENT","",comment_evalfn);
  fd_defalias(fd_scheme_module,"*******","COMMENT");
  fd_def_evalfn(fd_scheme_module,"ONBREAK","",onbreak_evalfn);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
