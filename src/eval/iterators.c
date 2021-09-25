/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_EVAL_INTERNALS 1
#define KNO_INLINE_TABLES       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_QONSTS	(!(KNO_AVOID_INLINE))
#define KNO_INLINE_STACKS       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_LEXENV       (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/sequences.h"
#include "kno/storage.h"
#include "kno/numbers.h"
#include "eval_internals.h"

/* Simple iterations */

DEFC_EVALFN("while",while_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(while *test* *exprs...*)` repeatedly evaluates "
	    "*test* until it is #f or {} evaluating "
	    "the expressions in *body* after each evaluation of "
	    "*test*. If a sub-expression of any of *exprs* returns #break, "
	    "this also exits. This normally returns VOID the result of the "
	    "call to *test* which terminated the loop or VOID if the body "
	    "got a #break.")
static lispval while_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1);
  lispval body = kno_get_body(expr,2);
  lispval result = VOID;
  if (VOIDP(test_expr))
    return kno_err(kno_TooFewExpressions,"WHILE",NULL,expr);
  else if (! (USUALLY( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"WHILE",NULL,expr);
  else {
    int exited = 0;
    while (testeval(test_expr,env,TESTEVAL_FAIL_FALSE,&result,_stack) == 1) {
      KNO_DOLIST(iter_expr,body) {
	lispval val = kno_eval(iter_expr,env,_stack);
	if (ABORTED(val)) {result=val; exited=1; break;}
	kno_decref(val);}
      if (exited) break;}
    if (KNO_BROKEP(result))
      return VOID;
    else return result;}
}

DEFC_EVALFN("until",until_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(until *test* *exprs...*)` repeatedly evaluates "
	    "*test* until it returns anything other than #f, evaluating "
	    "the expressions in *body* after each evaluation of "
	    "*test*. If a sub-expression of any of *exprs* returns #break, "
	    "this also exits. This normally returns VOID the result of the "
	    "call to *test* which terminated the loop or VOID if the body "
	    "got a #break.")
static lispval until_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1);
  lispval body = kno_get_body(expr,2);
  lispval result = VOID;
  if (VOIDP(test_expr))
    return kno_err(kno_TooFewExpressions,"UNTIL",NULL,expr);
  else if (! (USUALLY( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"UNTIL",NULL,expr);
  else {
    int exited = 0;
    while (testeval(test_expr,env,TESTEVAL_FAIL_TRUE,&result,_stack) == 0) {
      KNO_DOLIST(iter_expr,body) {
	lispval val = kno_eval(iter_expr,env,_stack);
	if (ABORTED(val)) { result = val; exited = 1; break;}
	kno_decref(val);}
      if (exited) break;}
    if (KNO_BROKEP(result))
      return KNO_VOID;
    else return result;}
}

/* Parsing for more complex iterations. */

/* DOTIMES */

DEFC_EVALFN("dotimes",dotimes_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(dotimes (var limit) *exprs...*)` repeatedly evaluates "
	    "*exprs* for *limit* iterations, binding *var* to the number "
	    "of iterations to date (starting with zero). "
	    "If a sub-expression of any of *exprs* returns #break, "
	    "this also exits. This always returns VOID.")
static lispval dotimes_evalfn(lispval expr,kno_lexenv env,
			      kno_stack eval_stack)
{
  lispval result = KNO_VOID;
  int i = 0, exited = 0, limit;
  lispval body = kno_get_body(expr,2);
  if (! (USUALLY( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"dotimes_evalfn",NULL,expr);
  lispval var, limit_val=
    parse_control_spec(expr,&var,NULL,NULL,env,eval_stack);;
  if (KNO_ABORTED(limit_val))
    return limit_val;
  else if (!(KNO_UINTP(limit_val))) {
    lispval err = kno_type_error("fixnum","dotimes_evalfn",limit_val);
    kno_decref(limit_val);
    return err;}
  else limit = FIX2INT(limit_val);
  KNO_INIT_ITER_LOOP(dotimes,var,limit_val,1,eval_stack,env);
  dotimes_vars[0]=var;
  dotimes_vals[0]=KNO_INT(0);
  while (i < limit) {
    dotimes_vals[0]=KNO_INT(i);
    result = eval_body(body,dotimes_env,dotimes_stack,
		       "dotimes",KNO_SYMBOL_NAME(var),
		       0);
    if (KNO_ABORTED(result)) {
      if (KNO_BROKEP(result)) result=KNO_VOID;
      exited=1;}
    else {
      kno_decref(result);
      result = KNO_VOID;}
    reset_env(dotimes);
    if (exited) break; else i++;}
  kno_pop_stack(dotimes_stack);
  return result;
}

/* DOSEQ */

DEFC_EVALFN("doseq",doseq_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(doseq (var sequence [*countvar*=|#|] [*wholevar*]) *exprs...*)` "
	    "iterates over every element of *sequence*, evaluating *exprs* "
	    "each time with *var* bound to the element, *countvar* bound to the "
	    "element index, and (when specified) *wholevar* bound to the whole "
	    "sequence. If a sub-expression of any of *exprs* returns #break, "
	    "this also exits. This always returns VOID. ")
static lispval doseq_evalfn(lispval expr,kno_lexenv env,
			    kno_stack eval_stack)
{
  int i = 0, lim, islist = 0;
  lispval var, count_var = VOID, val_var;
  lispval body = kno_get_body(expr,2);
  if (! (USUALLY( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"doseq_evalfn",NULL,expr);
  lispval seq = parse_control_spec(expr,&var,&count_var,&val_var,
				   env,eval_stack);
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
  KNO_INIT_ITER_LOOP(doseq,var,seq,3,eval_stack,env);
  if (PAIRP(seq)) {
    pairscan = seq;
    islist = 1;}
  init_iter_env(&doseq_bindings,seq,var,count_var,val_var);
  int exited = 0;
  while (i<lim) {
    lispval elt = (islist) ? (kno_refcar(pairscan)) : (kno_seq_elt(seq,i));
    doseq_vals[0]=elt;
    doseq_vals[1]=KNO_INT(i);
    lispval val = eval_body(body,doseq_env,doseq_stack,
			    "doseq",SYMBOL_NAME(var),0);
    if (KNO_ABORTED(val)) {
      if (KNO_BROKEP(val)) result = KNO_VOID;
      else result = val;
      exited=1;}
    else kno_decref(val);
    reset_env(doseq);
    kno_decref(doseq_vals[0]);
    doseq_vals[0]=VOID;
    doseq_vals[1]=VOID;
    if (exited) break;
    if (islist) pairscan = KNO_CDR(pairscan);
    i++;}
  kno_pop_stack(doseq_stack);
  return result;
}

/* FORSEQ */

DEFC_EVALFN("forseq",forseq_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(forseq (var sequence [*countvar*=|#|] [*wholevar*]) *exprs...*)` "
	    "generates a new sequence by iterating over every element of *sequence*, "
	    "evaluating *exprs* for each element time with *var* bound to the element, "
	    "*countvar* bound to the element index, and (when specified) *wholevar* "
	    "bound to the whole sequence. If a sub-expression of any of *exprs* "
	    "returns #break, this whole loop exits and returns a sequence up to "
	    "the breaking element. When possible, this returns a sequence of the same "
	    "type as its input *sequence*.")
static lispval forseq_evalfn(lispval expr,kno_lexenv env,
			     kno_stack eval_stack)
{
  size_t i = 0, lim=0; int islist=0;
  lispval body = kno_get_body(expr,2), pairscan=VOID;
  if (! (USUALLY( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"forseq_evalfn",NULL,expr);
  lispval var, count_var = VOID, val_var = VOID, *results, result=VOID;
  lispval seq = parse_control_spec(expr,&var,&count_var,&val_var,
				   env,eval_stack);
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

  KNO_INIT_ITER_LOOP(forseq,var,seq,3,eval_stack,env);
  forseq_vars[0]=var;
  init_iter_env(&forseq_bindings,seq,var,count_var,val_var);
  results = kno_init_elts(NULL,lim,KNO_FALSE);
  int exited = 0;
  while ( i < lim ) {
    lispval elt = (islist) ? (kno_refcar(pairscan)) : (kno_seq_elt(seq,i));
    forseq_vals[0]=elt;
    forseq_vals[1]=KNO_INT(i);
    lispval val = eval_body(body,forseq_env,forseq_stack,
			    "forseq",SYMBOL_NAME(var),
			    0);
    if (KNO_ABORTED(val)) {
      if (!(KNO_BROKEP(val))) result = val;
      exited = 1;}
    else results[i]=val;
    reset_env(forseq);
    kno_decref(forseq_vals[0]);
    forseq_vals[0]=VOID;
    forseq_vals[1]=VOID;
    if (islist) pairscan = KNO_CDR(pairscan);
    if (exited) break;
    i++;}
  if (KNO_VOIDP(result))
    result=kno_makeseq(KNO_TYPEOF(seq),i,results);
  kno_decref_elts(results,i);
  u8_free(results);
  kno_pop_stack(forseq_stack);
  return result;
}

/* TRYSEQ */

DEFC_EVALFN("tryseq",tryseq_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(tryseq (var sequence [*countvar*=|#|] [*wholevar*]) *exprs...*)` "
	    "iterates over every element of *sequence*, until *exprs* either return "
	    "non-empty ({}) or some subexpression returns #break. Each iteration "
	    "evaluates *epxrs* with *var* bound to the element, *countvar* bound to the "
	    "element index, and (when specified) *wholevar* bound to the whole "
	    "sequence. This always returns the non-empty result or fails (returns {}) "
	    "otherwise.")
static lispval tryseq_evalfn(lispval expr,kno_lexenv env,
			     kno_stack eval_stack)
{
  lispval result = KNO_EMPTY;
  size_t i = 0, lim=0; int islist=0;
  lispval body = kno_get_body(expr,2), pairscan=VOID;
  if (! (USUALLY( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"tryseq_evalfn",NULL,expr);
  lispval var, count_var = VOID, val_var = VOID;
  lispval seq = parse_control_spec(expr,&var,&count_var,&val_var,
				   env,eval_stack);
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
  KNO_INIT_ITER_LOOP(tryseq,var,seq,3,eval_stack,env);
  init_iter_env(&tryseq_bindings,seq,var,count_var,val_var);
  int exited = 0;
  while ( i < lim ) {
    lispval elt = (islist) ? (kno_refcar(pairscan)) : (kno_seq_elt(seq,i));
    tryseq_vals[0]=elt;
    tryseq_vals[1]=KNO_INT(i);
    lispval val = eval_body(body,tryseq_env,tryseq_stack,
			    "tryseq",SYMBOL_NAME(var),
			    0);
    if (KNO_ABORTED(val)) {
      if (!(KNO_BROKEP(val))) result = val;
      exited = 1;}
    else if (KNO_EMPTYP(val)) {}
    else {
      result = val;
      exited=1;}
    reset_env(tryseq);
    kno_decref(tryseq_vals[0]);
    tryseq_vals[0]=VOID;
    tryseq_vals[1]=VOID;
    if (exited) break;
    else if (islist) pairscan = KNO_CDR(pairscan);
    i++;}
  kno_pop_stack(tryseq_stack);
  return result;
}

/* DOLIST */

DEFC_EVALFN("dolist",dolist_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(dolist (var list [*countvar*=|#|] [wholevar]) *exprs...*)` "
	    "iterates over every element of *list*, evaluating *exprs* "
	    "each time with *var* bound to the element, *countvar* bound to "
	    "it's index, and *wholevar* (if provided) bound to the entire "
	    "list. In addition, if any of the sub expressions of *exprs* "
	    "returns #break, this exits immediately. This always returns VOID. ")
static lispval dolist_evalfn(lispval expr,kno_lexenv env,
			     kno_stack eval_stack)
{
  int i = 0;
  lispval result = KNO_VOID;
  lispval body = kno_get_body(expr,2);
  if (! (USUALLY( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"dolist_evalfn",NULL,expr);
  lispval var, count_var = VOID, val_var = VOID;
  lispval seq = parse_control_spec(expr,&var,&count_var,&val_var,
				   env,eval_stack);
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
  KNO_INIT_ITER_LOOP(dolist,var,seq,3,eval_stack,env);
  init_iter_env(&dolist_bindings,seq,var,count_var,val_var);
  int exited = 0;
  while (PAIRP(pairscan)) {
    lispval elt = KNO_CAR(pairscan); kno_incref(elt);
    dolist_vals[0]=elt;
    dolist_vals[1]=KNO_INT(i);
    lispval val = eval_body(body,dolist_env,dolist_stack,
			    "tryseq",SYMBOL_NAME(var),
			    0);
    if ((ABORTED(val))) {
      if (!(KNO_BROKEP(val))) result = val;
      exited = 1;}
    else kno_decref(val);
    reset_env(dolist);
    kno_decref(dolist_vals[0]);
    dolist_vals[0]=VOID;
    dolist_vals[1]=VOID;
    if (exited) break;
    pairscan = KNO_CDR(pairscan);
    i++;}
  kno_pop_stack(dolist_stack);
  return result;
}

/* Iterator objects */

DEFC_EVALFN("doiter",doiter_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(doiter (var iterator [*countvar*=|#|] [*wholevar*]) *exprs...*)` "
	    "repeatedly gets elements from *iterator*, evaluating *exprs* "
	    "each time with *var* bound to the element, *countvar* bound to the "
	    "number of elements processed, and (when specified) *wholevar* "
	    "bound to the whole iterator object. If a sub-expression of any of *exprs* "
	    "returns #break, this also exits. This always returns VOID. ")
static lispval doiter_evalfn(lispval expr,kno_lexenv env,
			     kno_stack eval_stack)
{
  unsigned long i = 0; int islist = 0;
  lispval var, count_var = VOID, val_var;
  lispval body = kno_get_body(expr,2);
  if (! (USUALLY( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"doseq_evalfn",NULL,expr);
  lispval iter = parse_control_spec(expr,&var,&count_var,&val_var,
				   env,eval_stack);
  if (KNO_ABORTED(iter))
    return iter;
  else if (EMPTYP(iter))
    return VOID;
  else if (!(KNO_ITERATORP(iter))) {
    kno_type_error("iterator","doiter_evalfn",iter);
    kno_decref(iter);
    return KNO_ERROR;}
  lispval result = KNO_VOID;
  KNO_INIT_ITER_LOOP(doiter,var,iter,3,eval_stack,env);
  init_iter_env(&doiter_bindings,iter,var,count_var,val_var);
  int exited = 0;
  while (exited==0) {
    lispval elt = kno_dcall(doiter_stack,iter,0,NULL);
    doiter_vals[0]=elt;
    doiter_vals[1]=KNO_INT(i);
    lispval val = eval_body(body,doiter_env,doiter_stack,
			    "doiter",SYMBOL_NAME(var),0);
    if (KNO_ABORTED(val)) {
      if (KNO_BROKEP(val)) result = KNO_VOID;
      else result = val;
      exited=1;}
    else kno_decref(val);
    reset_env(doiter);
    kno_decref(doiter_vals[0]);
    doiter_vals[0]=VOID;
    doiter_vals[1]=VOID;
    if (exited) break;
    i++;}
  kno_pop_stack(doiter_stack);
  return result;
}

/* BEGIN, PROG1, and COMMENT */

DEFC_EVALFN("begin",begin_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(begin *exprs...*)` executes all of *exprs* in order. "
	    "If any of *exprs* (or their subexpressions) returns #break, "
	    "this exits immediately and returns VOID. Otherwise the "
	    "result of the last of *exprs* is returned.")
static lispval begin_evalfn(lispval begin_expr,kno_lexenv env,
			    kno_stack _stack)
{
  return eval_body(KNO_CDR(begin_expr),env,_stack,"BEGIN",NULL,
		   (KNO_STACK_BITP(_stack,KNO_STACK_TAIL_POS)));
}

DEFC_EVALFN("onbreak",onbreak_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(onbreak *breakexpr* *exprs...*)` executes all of *exprs* in order. "
	    "If any of *exprs* (or their subexpressions) returns #break, "
	    "this evaluates *breakexpr* and returns its result. Otherwise, "
	    "it returns the result of the last of *exprs*")
static lispval onbreak_evalfn(lispval begin_expr,kno_lexenv env,
			      kno_stack _stack)
{
  lispval body = kno_get_body(begin_expr,2);
  lispval result = eval_body(body,env,_stack,"ONBREAK",NULL,0);
  if (KNO_BREAKP(result))
    result = kno_eval(kno_get_arg(begin_expr,1),env,_stack);
  return result;
}

DEFC_EVALFN("prog1",prog1_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(prog1 *return-expr* *exprs...*)` executes *return-expr* "
	    "and saves its value and then executes all of *exprs* in order. "
	    "This returns the saved result of *return-expr*. ")
static lispval prog1_evalfn(lispval prog1_expr,kno_lexenv env,
			    kno_stack _stack)
{
  lispval arg1 = kno_get_arg(prog1_expr,1);
  if (VOIDP(arg1))
    return kno_err(kno_SyntaxError,"prog1_evalfn",NULL,prog1_expr);
  lispval prog1_body = kno_get_body(prog1_expr,2);
  if (! (USUALLY( (KNO_PAIRP(prog1_body)) || (prog1_body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"prog1_evalfn",NULL,prog1_expr);
  lispval result = kno_eval(arg1,env,_stack);
  if (KNO_ABORTED(result))
    return result;
  else {
    KNO_DOLIST(subexpr,prog1_body) {
      lispval tmp = kno_eval(subexpr,env,_stack);
      if (KNO_ABORTED(tmp)) {
	kno_decref(result);
	return tmp;}
      if (KNO_BREAKP(tmp)) break;
      kno_decref(tmp);}
    return result;}
}

DEFC_EVALFN("comment",comment_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(comment *exprs...*)` always returns VOID without evaluating anyting.")
static lispval comment_evalfn(lispval comment_expr,kno_lexenv env,kno_stack stack)
{
  return VOID;
}

/* Initialize functions */

KNO_EXPORT void kno_init_iterators_c()
{
  u8_register_source_file(_FILEINFO);

  KNO_LINK_EVALFN(kno_scheme_module,until_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,while_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,dotimes_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,dolist_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,doseq_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,forseq_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,tryseq_evalfn);

  KNO_LINK_EVALFN(kno_scheme_module,doiter_evalfn);

  KNO_LINK_EVALFN(kno_scheme_module,begin_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,prog1_evalfn);
  KNO_LINK_EVALFN(kno_scheme_module,comment_evalfn);
  kno_defalias(kno_scheme_module,"*******","COMMENT");
  KNO_LINK_EVALFN(kno_scheme_module,onbreak_evalfn);

  link_local_cprims();

}

static void link_local_cprims()
{
}
