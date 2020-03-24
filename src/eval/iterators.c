/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_EVAL (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/sequences.h"
#include "kno/storage.h"
#include "kno/numbers.h"
#include "eval_internals.h"

/* Helper functions */

static lispval iter_var;

/* Simple iterations */

static lispval while_evalfn(lispval expr,kno_lexenv env,kno_eval_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1);
  lispval body = kno_get_body(expr,2);
  lispval result = VOID;
  if (VOIDP(test_expr))
    return kno_err(kno_TooFewExpressions,"WHILE",NULL,expr);
  else if (! (PRED_TRUE( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"WHILE",NULL,expr);
  else {
    while (testeval(test_expr,env,TESTEVAL_FAIL_FALSE,&result,_stack) == 1) {
      KNO_DOLIST(iter_expr,body) {
	lispval val = kno_stack_eval(iter_expr,env,_stack);
        if (KNO_BROKEP(val))
          return KNO_VOID;
        else if (KNO_ABORTED(val))
          return val;
        kno_decref(val);}}}
  if (KNO_ABORTED(result))
    return result;
  else return VOID;
}

static lispval until_evalfn(lispval expr,kno_lexenv env,kno_eval_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1);
  lispval body = kno_get_body(expr,2);
  lispval result = VOID;
  if (VOIDP(test_expr))
    return kno_err(kno_TooFewExpressions,"UNTIL",NULL,expr);
  else if (! (PRED_TRUE( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"UNTIL",NULL,expr);
  else {
    while (testeval(test_expr,env,TESTEVAL_FAIL_TRUE,&result,_stack) == 0) {
      KNO_DOLIST(iter_expr,body) {
        lispval val = kno_stack_eval(iter_expr,env,_stack);
        if (KNO_BROKEP(val))
          return KNO_VOID;
        else if (KNO_ABORTED(val))
          return val;
        else kno_decref(val);}}}
  if (KNO_ABORTED(result))
    return result;
  else return VOID;
}

/* Parsing for more complex iterations. */

static lispval parse_control_spec
  (lispval expr,lispval *varp,lispval *count_var,
   kno_lexenv env,kno_eval_stack _stack)
{
  lispval control_expr = kno_get_arg(expr,1);
  if (VOIDP(control_expr))
    return kno_err(kno_TooFewExpressions,"DO...",NULL,expr);
  else {
    lispval var = kno_get_arg(control_expr,0);
    lispval ivar = kno_get_arg(control_expr,2);
    lispval val_expr = kno_get_arg(control_expr,1), val;
    if (VOIDP(val_expr))
      return kno_err(kno_TooFewExpressions,"DO...",NULL,control_expr);
    else if (!(SYMBOLP(var)))
      return kno_err(kno_SyntaxError,
                     _("identifier is not a symbol"),NULL,control_expr);
    else if (!((VOIDP(ivar)) || (SYMBOLP(ivar))))
      return kno_err(kno_SyntaxError,
                     _("identifier is not a symbol"),NULL,control_expr);
    val = fast_eval(val_expr,env);
    if (KNO_ABORTED(val))
      return val;
    *varp = var;
    if (count_var) *count_var = ivar;
    return val;}
}

/* DOTIMES */

static lispval dotimes_evalfn(lispval expr,kno_lexenv env,
			      kno_eval_stack eval_stack)
{
  int i = 0, limit;
  lispval body = kno_get_body(expr,2);
  if (! (PRED_TRUE( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"dotimes_evalfn",NULL,expr);
  lispval var, limit_val=
    parse_control_spec(expr,&var,NULL,env,eval_stack);;
  if (KNO_ABORTED(limit_val))
    return limit_val;
  else if (!(KNO_UINTP(limit_val))) {
    lispval err = kno_type_error("fixnum","dotimes_evalfn",limit_val);
    kno_decref(limit_val);
    return err;}
  else limit = FIX2INT(limit_val);
  lispval result = KNO_VOID;
  KNO_INIT_ITER_LOOP(dotimes,var,limit_val,1,eval_stack,env);
  dotimes_vars[0]=var;
  dotimes_vals[0]=KNO_INT(0);
  while (i < limit) {
    dotimes_vals[0]=KNO_INT(i);
    {KNO_DOLIST(subexpr,body) {
	lispval val = kno_evaluate(subexpr,dotimes,dotimes_stack,0);
	if (KNO_BROKEP(val)) break;
	else if (KNO_ABORTED(val)) {result=val; break;}
	else kno_decref(val);}}
    reset_env(dotimes);
    i++;}
  kno_pop_eval(dotimes_stack);
  return result;
}

/* DOSEQ */

static lispval doseq_evalfn(lispval expr,kno_lexenv env,
			    kno_eval_stack eval_stack)
{
  int i = 0, lim, islist = 0;
  lispval var, count_var = VOID;
  lispval body = kno_get_body(expr,2);
  if (! (PRED_TRUE( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"doseq_evalfn",NULL,expr);
  lispval seq = parse_control_spec(expr,&var,&count_var,env,eval_stack);
  lispval pairscan = VOID;
  if (KNO_ABORTED(seq))
    return seq;
  else if (EMPTYP(seq))
    return VOID;
  else if (!(KNO_SEQUENCEP(seq))) {
    kno_type_error("sequence","doseq_evalfn",seq);
    kno_decref(seq);
    return KNO_ERROR;}
  else lim = kno_seq_length(seq);
  if (lim==0) {
    kno_decref(seq);
    return VOID;}
  lispval result = KNO_VOID;
  KNO_INIT_ITER_LOOP(doseq,var,seq,2,eval_stack,env);
  if (PAIRP(seq)) {
    pairscan = seq;
    islist = 1;}
  doseq_vars[0]=var;
  if (SYMBOLP(count_var))
    doseq_vars[1]=count_var;
  else doseq_bindings.schema_length=1;
  int finished = 0;
  while (i<lim) {
    lispval elt = (islist) ? (kno_refcar(pairscan)) : (kno_seq_elt(seq,i));
    doseq_vals[0]=elt;
    doseq_vals[1]=KNO_INT(i);
    {KNO_DOLIST(subexpr,body) {
	lispval val = kno_evaluate(subexpr,doseq,doseq_stack,0);
	if (KNO_BROKEP(val)) {
	  finished = 1; break;}
	else if (KNO_ABORTED(val)) {
	  result = val; finished=1; break;}
        else kno_decref(val);}}
    reset_env(doseq);
    kno_decref(doseq_vals[0]);
    doseq_vals[0]=VOID;
    doseq_vals[1]=VOID;
    if (finished) break;
    if (islist) pairscan = KNO_CDR(pairscan);
    i++;}
  kno_pop_eval(doseq_stack);
  return result;
}

/* FORSEQ */

static lispval forseq_evalfn(lispval expr,kno_lexenv env,
			     kno_eval_stack eval_stack)
{
  size_t i = 0, lim=0; int islist=0;
  lispval body = kno_get_body(expr,2), pairscan=VOID;
  if (! (PRED_TRUE( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"forseq_evalfn",NULL,expr);
  lispval var, count_var = VOID, *results, result=VOID;
  lispval seq = parse_control_spec(expr,&var,&count_var,env,eval_stack);
  if (KNO_ABORTED(seq))
    return seq;
  else if (EMPTYP(seq))
    return EMPTY;
  else if (NILP(seq))
    return seq;
  else if (!(KNO_SEQUENCEP(seq))) {
    kno_type_error("sequence","forseq_evalfn",seq);
    kno_decref(seq);
    return KNO_ERROR;}
  else lim = kno_seq_length(seq);
  if (lim==0)
    return seq;
  else if (PAIRP(seq)) {
    islist = 1;
    pairscan = seq;}
  else {}

  KNO_INIT_ITER_LOOP(forseq,var,seq,2,eval_stack,env);
  forseq_vars[0]=var;
  if (SYMBOLP(count_var)) {
    forseq_vars[1]=count_var;
    forseq_vals[1]=KNO_FIXZERO;}
  else forseq_bindings.schema_length=1;
  results = kno_init_elts(NULL,lim,VOID);
  int finished = 0;
  while ( i < lim ) {
    lispval elt = (islist) ? (kno_refcar(pairscan)) : (kno_seq_elt(seq,i));
    lispval val = VOID;
    forseq_vals[0]=elt;
    forseq_vals[1]=KNO_INT(i);
    {KNO_DOLIST(subexpr,body) {
        kno_decref(val);
	val = kno_evaluate(subexpr,forseq,forseq_stack,0);
        if (KNO_BROKEP(val)) {
          result=kno_makeseq(KNO_TYPEOF(seq),i,results);
	  finished = 1;
	  break;}
	else if (KNO_ABORTED(val)) {
	  kno_decref_vec(results,i);
	  result=val;
	  finished=1;
	  break;}}
      results[i]=val;
      reset_env(forseq);
      kno_decref(forseq_vals[0]);
      forseq_vals[0]=VOID;
      forseq_vals[1]=VOID;
      if (islist) pairscan = KNO_CDR(pairscan);
      if (finished) break;
      i++;}}
  if (!(finished))
    result=kno_makeseq(KNO_TYPEOF(seq),lim,results);
  kno_decref_vec(results,i);
  u8_free(results);
  kno_pop_eval(forseq_stack);
  return result;
}

/* TRYSEQ */

static lispval tryseq_evalfn(lispval expr,kno_lexenv env,
			     kno_eval_stack eval_stack)
{
  size_t i = 0, lim=0; int islist=0;
  lispval body = kno_get_body(expr,2), pairscan=VOID;
  if (! (PRED_TRUE( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"tryseq_evalfn",NULL,expr);
  lispval var, count_var = VOID;
  lispval seq = parse_control_spec(expr,&var,&count_var,env,eval_stack);
  if (KNO_ABORTED(seq))
    return seq;
  else if (EMPTYP(seq))
    return EMPTY;
  else if (NILP(seq))
    return EMPTY;
  else if (!(KNO_SEQUENCEP(seq))) {
    kno_type_error("sequence","tryseq_evalfn",seq);
    kno_decref(seq);
    return KNO_ERROR;}
  else lim = kno_seq_length(seq);
  if (lim==0) {
    kno_decref(seq);
    return EMPTY;}
  else if (PAIRP(seq)) {
    islist = 1;
    pairscan = seq;}
  else {}
  lispval result = KNO_EMPTY;
  KNO_INIT_ITER_LOOP(tryseq,var,seq,2,eval_stack,env);
  tryseq_vars[0]=var;
  if (SYMBOLP(count_var)) {
    tryseq_vars[1]=count_var;
    tryseq_vals[1]=KNO_FIXZERO;}
  else tryseq_bindings.schema_length=1;
  int finished = 0;
  while ( i < lim ) {
    lispval elt = (islist) ? (kno_refcar(pairscan)) : (kno_seq_elt(seq,i));
    lispval val = VOID;
    tryseq_vals[0]=elt;
    tryseq_vals[1]=KNO_INT(i);
    {KNO_DOLIST(subexpr,body) {
        kno_decref(val);
	val = kno_evaluate(subexpr,tryseq,tryseq_stack,0);
	if (KNO_BROKEP(val)) {
	  finished=1; break;}
	else if (KNO_ABORTED(val)) {
	  result = val; finished=1; break;}
	else NO_ELSE;}}
    reset_env(tryseq);
    kno_decref(tryseq_vals[0]);
    tryseq_vals[0]=VOID;
    tryseq_vals[1]=VOID;
    if (finished) break;
    else if (! (EMPTYP(val)) ) {
      result = val; break;}
    else {
      if (islist) pairscan = KNO_CDR(pairscan);
      i++;}}
  kno_pop_eval(tryseq_stack);
  return result;
}

/* DOLIST */

static lispval dolist_evalfn(lispval expr,kno_lexenv env,
			     kno_eval_stack eval_stack)
{
  int i = 0;
  lispval body = kno_get_body(expr,2);
  if (! (PRED_TRUE( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"dolist_evalfn",NULL,expr);
  lispval var, count_var = VOID;
  lispval seq = parse_control_spec(expr,&var,&count_var,env,eval_stack);
  lispval pairscan = VOID;
  if (KNO_ABORTED(seq))
    return seq;
  else if (EMPTYP(seq))
    return VOID;
  else if (NILP(seq))
    return VOID;
  else if (!(PAIRP(seq))) {
    kno_type_error("pair","dolist_evalfn",seq);
    kno_decref(seq);
    return KNO_ERROR;}
  else pairscan = seq;
  lispval result = KNO_VOID;
  KNO_INIT_ITER_LOOP(dolist,var,seq,2,eval_stack,env);
  dolist_vars[0]=var;
  if (SYMBOLP(count_var))
    dolist_vars[1]=count_var;
  else dolist_bindings.schema_length=1;
  int finished = 0;
  while (PAIRP(pairscan)) {
    lispval elt = KNO_CAR(pairscan); kno_incref(elt);
    dolist_vals[0]=elt;
    dolist_vals[1]=KNO_INT(i);
    {KNO_DOLIST(subexpr,body) {
	lispval val = kno_evaluate(subexpr,dolist,dolist_stack,0);
	if (KNO_BROKEP(val)) {
	  finished=1; break;}
	else if (KNO_ABORTED(val)) {
	  result=val; finished=1; break;}
	else kno_decref(val);}}
    reset_env(dolist);
    kno_decref(dolist_vals[0]);
    dolist_vals[0]=VOID;
    dolist_vals[1]=VOID;
    if (finished) break;
    pairscan = KNO_CDR(pairscan);
    i++;}
  kno_pop_eval(dolist_stack);
  return result;
}

/* BEGIN, PROG1, and COMMENT */

static lispval begin_evalfn(lispval begin_expr,kno_lexenv env,
			    kno_eval_stack _stack)
{
  return eval_body(KNO_CDR(begin_expr),env,_stack,"BEGIN",NULL,1);
}

static lispval onbreak_evalfn(lispval begin_expr,kno_lexenv env,
			      kno_eval_stack _stack)
{
  lispval body = kno_get_body(begin_expr,2);
  lispval result = eval_body(body,env,_stack,"ONBREAK",NULL,1);
  if (KNO_BREAKP(result))
    result = kno_evaluate(kno_get_arg(begin_expr,1),env,_stack,0);
  return result;
}

static lispval prog1_evalfn(lispval prog1_expr,kno_lexenv env,
			    kno_eval_stack _stack)
{
  lispval arg1 = kno_get_arg(prog1_expr,1);
  if (VOIDP(arg1))
    return kno_err(kno_SyntaxError,"prog1_evalfn",NULL,prog1_expr);
  lispval prog1_body = kno_get_body(prog1_expr,2);
  if (! (PRED_TRUE( (KNO_PAIRP(prog1_body)) || (prog1_body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"prog1_evalfn",NULL,prog1_expr);
  lispval result = kno_stack_eval(arg1,env,_stack);
  if (KNO_ABORTED(result))
    return result;
  else {
    KNO_DOLIST(subexpr,prog1_body) {
      lispval tmp = fast_eval(subexpr,env);
      if (KNO_ABORTED(tmp)) {
        kno_decref(result);
        return tmp;}
      kno_decref(tmp);}
    return result;}
}

static lispval comment_evalfn(lispval comment_expr,kno_lexenv env,kno_eval_stack stack)
{
  return VOID;
}

/* Initialize functions */

KNO_EXPORT void kno_init_iterators_c()
{
  moduleid_symbol = kno_intern("%moduleid");
  iter_var = kno_intern("%iter");

  u8_register_source_file(_FILEINFO);

  kno_def_evalfn(kno_scheme_module,"UNTIL",until_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"WHILE",while_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"DOTIMES",dotimes_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"DOLIST",dolist_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"DOSEQ",doseq_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"FORSEQ",forseq_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"TRYSEQ",tryseq_evalfn,
		 "*undocumented*");

  kno_def_evalfn(kno_scheme_module,"BEGIN",begin_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"PROG1",prog1_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"COMMENT",comment_evalfn,
		 "*undocumented*");
  kno_defalias(kno_scheme_module,"*******","COMMENT");
  kno_def_evalfn(kno_scheme_module,"ONBREAK",onbreak_evalfn,
		 "*undocumented*");

}
