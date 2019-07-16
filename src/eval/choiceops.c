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
#include "kno/storage.h"
#include "kno/numbers.h"
#include "kno/frames.h"
#include "kno/sorting.h"

#include "eval_internals.h"
#include <kno/cprims.h>


static lispval keyfn_get(lispval val,lispval keyfn)
{
  if ( (VOIDP(keyfn)) || (KNO_FALSEP(keyfn)) || (KNO_DEFAULTP(keyfn)) )
    return kno_incref(val);
  else if (OIDP(val))
    return kno_frame_get(val,keyfn);
  else {
    kno_lisp_type type = KNO_LISP_TYPE(keyfn);
    switch (type) {
    case kno_hashtable_type:
    case kno_slotmap_type:
    case kno_schemap_type:
      return kno_get(keyfn,val,EMPTY);
    case kno_lambda_type:
    case kno_cprim_type:
      return kno_finish_call(kno_dapply(keyfn,1,&val));
    default:
      if (KNO_APPLICABLEP(keyfn))
        return kno_finish_call(kno_dapply(keyfn,1,&val));
      else if (KNO_TABLEP(keyfn))
        return kno_get(keyfn,val,EMPTY);
      else if (KNO_TABLEP(val))
        return kno_get(val,keyfn,EMPTY);
      else return KNO_EMPTY;}}
}

#define KEYFNP(x) \
  ((VOIDP(x)) || (FALSEP(x)) || (DEFAULTP(x)) || \
   (SYMBOLP(x)) || (OIDP(x)) ||                  \
   (TABLEP(x)) ||                                \
   (KNO_APPLICABLEP(x)))

/* Choice iteration */

static lispval parse_control_spec
(lispval expr,lispval *iter_var,lispval *count_var,
 kno_lexenv env,kno_stack _stack)
{
  lispval control_expr = kno_get_arg(expr,1);
  if (VOIDP(control_expr))
    return kno_err(kno_TooFewExpressions,NULL,NULL,expr);
  else if (SYMBOLP(control_expr)) {
    lispval values = fast_eval(control_expr,env);
    if (KNO_ABORTED(values)) {
      *iter_var = VOID;
      return values;}
    *iter_var = control_expr;
    *count_var = VOID;
    return kno_simplify_choice(values);}
  else {
    lispval var = kno_get_arg(control_expr,0), ivar = kno_get_arg(control_expr,2);
    lispval val_expr = kno_get_arg(control_expr,1), val;
    if (VOIDP(var))
      return kno_err(kno_TooFewExpressions,NULL,NULL,expr);
    else if (VOIDP(val_expr))
      return kno_err(kno_TooFewExpressions,NULL,NULL,control_expr);
    else if (!(SYMBOLP(var)))
      return kno_err(kno_SyntaxError,
                    _("identifier is not a symbol"),NULL,control_expr);
    else if (!((VOIDP(ivar)) || (SYMBOLP(ivar))))
      return kno_err(kno_SyntaxError,
                    _("identifier is not a symbol"),NULL,control_expr);
    val = fast_eval(val_expr,env);
    if (KNO_ABORTED(val)) {
      *iter_var = VOID;
      return val;}
    *iter_var = var;
    if (count_var) *count_var = ivar;
    return kno_simplify_choice(val);}
}

/* This iterates over a set of choices, evaluating its body for each value.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns VOID. */
static lispval dochoices_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval steps = kno_get_body(expr,2);
  if (! (PRED_TRUE( (KNO_PAIRP(steps)) || (steps == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"dochoices_evalfn",NULL,expr);
  lispval var, count_var, choices=
    parse_control_spec(expr,&var,&count_var,env,_stack);
  if (KNO_ABORTED(var)) return var;
  else if (KNO_ABORTED(choices)) return choices;
  else if (EMPTYP(choices))
    return VOID;
  KNO_ADD_TO_CHOICE(_stack->stack_vals,choices);
  INIT_STACK_ENV(_stack,dochoices,env,2);
  dochoices_vars[0]=var;
  if (SYMBOLP(count_var))
    dochoices_vars[1]=count_var;
  else dochoices_bindings.schema_length=1;
  int i = 0; DO_CHOICES(elt,choices) {
    dochoices_vals[0]=kno_incref(elt);
    dochoices_vals[1]=KNO_INT(i);
    {KNO_DOLIST(step,steps) {
        lispval val = fast_eval(step,dochoices);
        if (KNO_BROKEP(val))
          _return KNO_VOID;
        else if (KNO_ABORTED(val)) {
          _return val;}
        else kno_decref(val);}}
    reset_env(dochoices);
    kno_decref(dochoices_vals[0]);
    dochoices_vals[0]=VOID;
    kno_decref(dochoices_vals[1]);
    dochoices_vals[1]=VOID;
    i++;}
  _return VOID;
}

/* This iterates over a set of choices, evaluating its body for each value.
   It returns the first non-empty result of evaluating the body.
   Note that this treats a non-choice as a choice of one element.
   It returns VOID. */
static lispval trychoices_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval steps = kno_get_body(expr,2);
  if (! (PRED_TRUE( (KNO_PAIRP(steps)) || (steps == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"trychoices_evalfn",NULL,expr);
  lispval var, count_var, choices=
    parse_control_spec(expr,&var,&count_var,env,_stack);
  if (KNO_ABORTED(var)) return var;
  else if (KNO_ABORTED(choices)) return choices;
  else if (EMPTYP(choices))
    return EMPTY;
  KNO_ADD_TO_CHOICE(_stack->stack_vals,choices);
  INIT_STACK_ENV(_stack,trychoices,env,2);
  trychoices_vars[0]=var;
  if (SYMBOLP(count_var))
    trychoices_vars[1]=count_var;
  else trychoices_bindings.schema_length=1;
  int i = 0; DO_CHOICES(elt,choices) {
    lispval val = VOID;
    trychoices_vals[0]=kno_incref(elt);
    trychoices_vals[1]=KNO_INT(i);
    KNO_DOLIST(step,steps) {
      kno_decref(val);
      val = fast_eval(step,trychoices);
      if (KNO_BROKEP(val))
        _return EMPTY;
      else if (KNO_ABORTED(val))
        _return val;}
    reset_env(trychoices);
    kno_decref(trychoices_vals[0]);
    trychoices_vals[0]=VOID;
    kno_decref(trychoices_vals[1]);
    trychoices_vals[1]=VOID;
    if (!(EMPTYP(val))) _return val;
    i++;}
  _return EMPTY;
}

/* This iterates over a set of choices, evaluating its body for each value, and
    accumulating the results of those evaluations.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns the combined results of its body's execution. */
static lispval forchoices_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval steps = kno_get_body(expr,2);
  if (! (PRED_TRUE( (KNO_PAIRP(steps)) || (steps == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"forchoices_evalfn",NULL,expr);
  lispval results = EMPTY;
  lispval var, count_var, choices=
    parse_control_spec(expr,&var,&count_var,env,_stack);
  if (KNO_ABORTED(var)) return var;
  else if (KNO_ABORTED(choices))
    return choices;
  else if (EMPTYP(choices))
    return EMPTY;
  KNO_ADD_TO_CHOICE(_stack->stack_vals,choices);
  INIT_STACK_ENV(_stack,forchoices,env,2);
  forchoices_vars[0]=var;
  if (SYMBOLP(count_var))
    forchoices_vars[1]=count_var;
  else forchoices_bindings.schema_length=1;
  int i = 0; DO_CHOICES(elt,choices) {
    lispval val = VOID;
    forchoices_vals[0]=kno_incref(elt);
    forchoices_vals[1]=KNO_INT(i);
    KNO_DOLIST(step,steps) {
      kno_decref(val);
      val = fast_eval(step,forchoices);
      if (KNO_BROKEP(val)) {
        lispval result = kno_simplify_choice(results);
        _return result;}
      else if (KNO_ABORTED(val)) {
        kno_decref(results);
        _return val;}
      else NO_ELSE;}
    CHOICE_ADD(results,val);
    reset_env(forchoices);
    kno_decref(forchoices_vals[0]);
    forchoices_vals[0]=VOID;
    kno_decref(forchoices_vals[1]);
    forchoices_vals[1]=VOID;
    i++;}
  _return kno_simplify_choice(results);
}

/* This iterates over a set of choices, evaluating its third subexpression for each value, and
    accumulating those values for which the body returns true.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns the subset of values which pass the body. */
static lispval filterchoices_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval steps = kno_get_body(expr,2);
  if (! (PRED_TRUE( (KNO_PAIRP(steps)) || (steps == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"filterchoices_evalfn",NULL,expr);
  lispval results = EMPTY;
  lispval var, count_var, choices=
    parse_control_spec(expr,&var,&count_var,env,_stack);
  if (KNO_ABORTED(var)) return var;
  else if (KNO_ABORTED(choices))
    return choices;
  else if (EMPTYP(choices))
    return EMPTY;
  KNO_ADD_TO_CHOICE(_stack->stack_vals,choices);
  INIT_STACK_ENV(_stack,filterchoices,env,2);
  filterchoices_vars[0]=var;
  if (SYMBOLP(count_var))
    filterchoices_vars[1]=count_var;
  else filterchoices_bindings.schema_length=1;
  int i = 0; DO_CHOICES(elt,choices) {
    lispval val = VOID;
    filterchoices_vals[0]=kno_incref(elt);
    filterchoices_vals[1]=KNO_INT(i);
    KNO_DOLIST(step,steps) {
      kno_decref(val);
      val = fast_eval(step,filterchoices);
      if (KNO_BROKEP(val)) {
        lispval result = kno_simplify_choice(results);
        _return result;}
      else if (KNO_ABORTED(val)) {
        kno_decref(results);
        _return val;}
      else if (!(FALSEP(val))) {
        kno_incref(elt);
        CHOICE_ADD(results,elt);
        break;}
      else NO_ELSE;}
    reset_env(filterchoices);
    kno_decref(filterchoices_vals[0]);
    filterchoices_vals[0]=VOID;
    kno_decref(filterchoices_vals[1]);
    filterchoices_vals[1]=VOID;
    i++;}
  _return kno_simplify_choice(results);
}

/* Choice functions */

KNO_DCLPRIM("fail",fail_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
 "`(FAIL)` **undocumented**");
static lispval fail_prim()
{
  return EMPTY;
}

KNO_DCLPRIM("choice",choice_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDCALL,
 "`(CHOICE *args...*)` **undocumented**");
static lispval choice_prim(int n,lispval *args)
{
  int i = 0; lispval results = EMPTY;
  while (i < n) {
    lispval arg = args[i++]; kno_incref(arg);
    CHOICE_ADD(results,arg);}
  return kno_simplify_choice(results);
}

KNO_DCLPRIM("qchoice",qchoice_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDCALL,
 "`(QCHOICE *args...*)` **undocumented**");
static lispval qchoice_prim(int n,lispval *args)
{
  int i = 0; lispval results = EMPTY, presults;
  while (i < n) {
    lispval arg = args[i++]; kno_incref(arg);
    CHOICE_ADD(results,arg);}
  presults = kno_simplify_choice(results);
  if ((CHOICEP(presults)) || (EMPTYP(presults)))
    return kno_init_qchoice(NULL,presults);
  else return presults;
}

KNO_DCLPRIM("qchoicex",qchoicex_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDCALL,
 "`(QCHOICEX *args...*)` **undocumented**");
static lispval qchoicex_prim(int n,lispval *args)
{
  int i = 0; lispval results = EMPTY, presults;
  while (i < n) {
    lispval arg = args[i++]; kno_incref(arg);
    CHOICE_ADD(results,arg);}
  presults = kno_simplify_choice(results);
  if (EMPTYP(presults))
    return presults;
  else if (CHOICEP(presults))
   return kno_init_qchoice(NULL,presults);
  else return presults;
}

/* TRY */

static lispval try_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval value = EMPTY;
  lispval clauses = kno_get_body(expr,1);
  KNO_DOLIST(clause,clauses) {
    int ipe_state = kno_ipeval_status();
    kno_decref(value);
    value = kno_eval(clause,env);
    if (KNO_ABORTED(value))
      return value;
    else if (VOIDP(value)) {
      kno_seterr(kno_VoidArgument,"try_evalfn",NULL,clause);
      return KNO_ERROR;}
    else if (!(EMPTYP(value)))
      return value;
    else if (kno_ipeval_status()!=ipe_state)
      return value;}
  return value;
}

/* IFEXISTS */

static lispval ifexists_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval value_expr = kno_get_arg(expr,1);
  lispval value = EMPTY;
  if (VOIDP(value_expr))
    return kno_err(kno_SyntaxError,"ifexists_evalfn",NULL,expr);
  else if (!(NILP(KNO_CDR(KNO_CDR(expr)))))
   return kno_err(kno_SyntaxError,"ifexists_evalfn",NULL,expr);
  else value = kno_eval(value_expr,env);
  if (KNO_ABORTED(value))
    return value;
  if (EMPTYP(value))
    return VOID;
  else return value;
}

/* Predicates */

KNO_DCLPRIM1("empty?",emptyp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(EMPTY? *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval emptyp(lispval x)
{
  if (EMPTYP(x)) return KNO_TRUE; else return KNO_FALSE;
}

KNO_DCLPRIM1("satisfied?",satisfiedp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(SATISFIED? *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval satisfiedp(lispval x)
{
  if (EMPTYP(x)) return KNO_FALSE;
  else if (FALSEP(x)) return KNO_FALSE;
  else return KNO_TRUE;
}

KNO_DCLPRIM1("exists?",existsp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(EXISTS? *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval existsp(lispval x)
{
  if (EMPTYP(x)) return KNO_FALSE; else return KNO_TRUE;
}

KNO_DCLPRIM1("unique?",singletonp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(UNIQUE? *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval singletonp(lispval x)
{
  if (CHOICEP(x))
    return KNO_FALSE;
  else if (EMPTYP(x))
    return KNO_FALSE;
  else return KNO_TRUE;
}

KNO_DCLPRIM1("amb?",ambiguousp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(AMB? *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval ambiguousp(lispval x) /* TODO: Wasted effort around here */
{
  if (EMPTYP(x))
    return KNO_FALSE;
  else if (CHOICEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM1("singleton",singleton,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(SINGLETON *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval singleton(lispval x)
{
  if (EMPTYP(x)) return x;
  else if (CHOICEP(x))
    return EMPTY;
  else return kno_incref(x);
}

KNO_DCLPRIM2("choice-max",choice_max,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
 "`(CHOICE-MAX *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval choice_max(lispval x,lispval lim)
{
  if (EMPTYP(x)) return x;
  else if (CHOICEP(x)) {
    int max_size = kno_getint(lim);
    if (KNO_CHOICE_SIZE(x)>max_size)
      return EMPTY;
    else return kno_incref(x);}
  else return kno_incref(x);
}

KNO_DCLPRIM2("choice-min",choice_min,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
 "`(CHOICE-MIN *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval choice_min(lispval x,lispval lim)
{
  if (EMPTYP(x)) return x;
  else if (CHOICEP(x)) {
    int min_size = kno_getint(lim);
    if (KNO_CHOICE_SIZE(x)<min_size)
      return EMPTY;
    else return kno_incref(x);}
  else return KNO_EMPTY;
}

KNO_DCLPRIM1("simplify",simplify,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(SIMPLIFY *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval simplify(lispval x)
{
  return kno_make_simple_choice(x);
}

static lispval qchoicep_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  /* This is an evalfn because application often reduces qchoices to
     choices. */
  if (!((PAIRP(expr)) && (PAIRP(KNO_CDR(expr)))))
    return kno_err(kno_SyntaxError,"qchoice_evalfn",NULL,expr);
  else {
    lispval val = kno_eval(KNO_CADR(expr),env);
    if (KNO_ABORTED(val)) return val;
    if (QCHOICEP(val)) {
      kno_decref(val);
      return KNO_TRUE;}
    else {
      kno_decref(val);
      return KNO_FALSE;}}
}

/* The exists operation */

#define SKIP_ERRS 1
#define PASS_ERRS 0

static int test_exists(struct KNO_FUNCTION *fn,
                       int i,int n,lispval *nd_args,lispval *d_args,
                       int skip_errs)
{
  if (i == n) {
    lispval val = kno_finish_call(kno_dapply((lispval)fn,n,d_args));
    if ((FALSEP(val)) || (EMPTYP(val))) {
      return 0;}
    else if (KNO_ABORTED(val))
      return kno_interr(val);
    kno_decref(val);
    return 1;}
  else if (CHOICEP(nd_args[i])) {
    DO_CHOICES(v,nd_args[i]) {
      d_args[i]=v;
      int retval = test_exists(fn,i+1,n,nd_args,d_args,skip_errs);
      if (retval > 0) {
        KNO_STOP_DO_CHOICES;
        return retval;}
      else if ( (retval < 0) && (skip_errs) )
        kno_clear_errors(0);
      else if (retval < 0) {
        KNO_STOP_DO_CHOICES;
        return retval;}
      else continue;}
    return 0;}
  else {
    d_args[i]=nd_args[i];
    return test_exists(fn,i+1,n,nd_args,d_args,skip_errs);}
}

static lispval exists_helper(int n,lispval *nd_args,int skip_errs)
{
  int i = 0; while (i<n)
    if (EMPTYP(nd_args[i]))
      return KNO_FALSE;
    else i++;
  lispval d_args[n-1];
  {DO_CHOICES(fcn,nd_args[0])
      if (KNO_APPLICABLEP(fcn)) {}
      else {
        KNO_STOP_DO_CHOICES;
        return kno_type_error(_("function"),"exists_helper",nd_args[0]);}}
  {DO_CHOICES(fcn,nd_args[0]) {
      struct KNO_FUNCTION *f = (kno_function)fcn;
      int retval = test_exists(f,0,n-1,nd_args+1,d_args,skip_errs);
      if (retval == 0) continue;
      KNO_STOP_DO_CHOICES;
      if (retval<0)
        return KNO_ERROR;
      else return KNO_TRUE;}}
  return KNO_FALSE;
}

KNO_DCLPRIM("exists",exists_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(EXISTS *arg0* *args...*)` **undocumented**");
static lispval exists_lexpr(int n,lispval *nd_args)
{
  return exists_helper(n,nd_args,PASS_ERRS);
}

KNO_DCLPRIM("exists/skiperrs",exists_skiperrs,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(EXISTS/SKIPERRS *arg0* *args...*)` **undocumented**");
static lispval exists_skiperrs(int n,lispval *nd_args)
{
  return exists_helper(n,nd_args,SKIP_ERRS);
}

KNO_DCLPRIM("sometrue",sometrue_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(SOMETRUE *arg0* *args...*)` **undocumented**");
static lispval sometrue_lexpr(int n,lispval *nd_args)
{
  if (n==1)
    if (EMPTYP(nd_args[0]))
      return KNO_FALSE;
    else if (KNO_FALSEP(nd_args[0]))
      return KNO_FALSE;
    else return KNO_TRUE;
  else return exists_helper(n,nd_args,PASS_ERRS);
}

KNO_DCLPRIM("sometrue/skiperrs",sometrue_skiperrs,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(SOMETRUE/SKIPERRS *arg0* *args...*)` **undocumented**");
static lispval sometrue_skiperrs(int n,lispval *nd_args)
{
  if (n==1)
    if (EMPTYP(nd_args[0]))
      return KNO_FALSE;
    else if (KNO_FALSEP(nd_args[0]))
      return KNO_FALSE;
    else return KNO_TRUE;
  else return exists_helper(n,nd_args,SKIP_ERRS);
}

/* FORALL */

static int test_forall(struct KNO_FUNCTION *fn,int i,int n,
                       lispval *nd_args,lispval *d_args,
                       int skip_errs)
{
  if (i == n) {
    lispval val = kno_finish_call(kno_dapply((lispval)fn,n,d_args));
    if (FALSEP(val))
      return 0;
    else if (EMPTYP(val))
      return 0;
    else if (KNO_ABORTED(val))
      return -1;
    kno_decref(val);
    return 1;}
  else if (CHOICEP(nd_args[i])) {
    DO_CHOICES(v,nd_args[i]) {
      int retval;
      d_args[i]=v;
      retval = test_forall(fn,i+1,n,nd_args,d_args,skip_errs);
      if (retval > 0) continue;
      if ( (retval < 0) && (skip_errs) )
        kno_clear_errors(0);
      else {
        KNO_STOP_DO_CHOICES;
        return retval;}}
    return 1;}
  else {
    d_args[i]=nd_args[i];
    return test_forall(fn,i+1,n,nd_args,d_args,skip_errs);}
}

static lispval whenexists_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval to_eval = kno_get_arg(expr,1), value;
  if (VOIDP(to_eval))
    return kno_err(kno_SyntaxError,"whenexists_evalfn",NULL,expr);
  else value = kno_eval(to_eval,env);
  if (KNO_ABORTED(value)) {
    kno_clear_errors(0);
    return VOID;}
  else if (EMPTYP(value))
    return VOID;
  else return value;
}

static lispval forall_helper(int n,lispval *nd_args,int skip_errs)
{
  int i = 0; while (i<n)
    if (EMPTYP(nd_args[i])) return KNO_TRUE;
    else i++;
  {DO_CHOICES(fcn,nd_args[0])
      if (KNO_APPLICABLEP(fcn)) {}
      else {
        KNO_STOP_DO_CHOICES;
        return kno_type_error(_("function"),"forall_helper",nd_args[0]);}}
  lispval d_args[n-1];
  {DO_CHOICES(fcn,nd_args[0]) {
      struct KNO_FUNCTION *f = KNO_XFUNCTION(fcn);
      int retval = test_forall(f,0,n-1,nd_args+1,d_args,skip_errs);
      if ( (retval < 0) && (skip_errs == 0) )
        return KNO_ERROR;
      else if (retval == 0) {
        KNO_STOP_DO_CHOICES;
        return KNO_FALSE;}
      else {}}}
  return KNO_TRUE;
}

KNO_DCLPRIM("forall",forall_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(FORALL *arg0* *args...*)` **undocumented**");
static lispval forall_lexpr(int n,lispval *nd_args)
{
  return forall_helper(n,nd_args,PASS_ERRS);
}

KNO_DCLPRIM("forall/skiperrs",forall_skiperrs,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(FORALL/SKIPERRS *arg0* *args...*)` **undocumented**");
static lispval forall_skiperrs(int n,lispval *nd_args)
{
  return forall_helper(n,nd_args,SKIP_ERRS);
}

/* Set operations */

KNO_DCLPRIM("union",union_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(UNION *arg0* *args...*)` **undocumented**");
static lispval union_lexpr(int n,lispval *args)
{
  return kno_simplify_choice(kno_union(args,n));
}
KNO_DCLPRIM("intersection",intersection_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(INTERSECTION *arg0* *args...*)` **undocumented**");
static lispval intersection_lexpr(int n,lispval *args)
{
  return kno_simplify_choice(kno_intersection(args,n));
}
KNO_DCLPRIM("difference",difference_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(DIFFERENCE *arg0* *args...*)` **undocumented**");
static lispval difference_lexpr(int n,lispval *args)
{
  lispval result = kno_incref(args[0]); int i = 1;
  if (EMPTYP(result))
    return result;
  else while (i<n) {
    lispval new = kno_difference(result,args[i]);
    if (EMPTYP(new)) {
      kno_decref(result);
      return EMPTY;}
    else {kno_decref(result);
      result = new;}
    i++;}
  return kno_simplify_choice(result);
}

/* Prechoice elements */

#if 0
static lispval choicevec_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval sub_expr = kno_get_arg(expr,1);
  if (KNO_VOIDP(sub_expr))
    return kno_err(kno_SyntaxError,"choicevec_evalfn",NULL,expr);
  else {
    lispval result = kno_stack_eval(sub_expr,env,_stack,0);
    if (KNO_ABORTED(result))
      return result;
    else if (KNO_CHOICEP(result)) {
      struct KNO_CHOICE *ch = (kno_choice) result;
      const lispval *elts = KNO_XCHOICE_ELTS(ch);
      int size = ch->choice_size;
      lispval vector = kno_make_vector(size,(lispval *)elts);
      if (ch->choice_isatomic)
        return vector;
      else {
        kno_incref_vec((lispval *)elts,size);
        return vector;}}
    else {
      lispval data[1]={result};
      return kno_make_vector(1,data);}}
}
#endif

/* Conversion functions */

KNO_DCLPRIM2("choice->vector",choice2vector,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(CHOICE->VECTOR *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval choice2vector(lispval x,lispval sortspec)
{
  kno_compare_flags flags = kno_get_compare_flags(sortspec);
  if (EMPTYP(x))
    return kno_empty_vector(0);
  else if (CHOICEP(x)) {
    int i = 0, n = KNO_CHOICE_SIZE(x);
    struct KNO_CHOICE *ch = (kno_choice)x;
    lispval vector = kno_make_vector(n,NULL);
    lispval *vector_elts = VEC_DATA(vector);
    const lispval *choice_elts = KNO_XCHOICE_DATA(ch);
    if (KNO_XCHOICE_ATOMICP(ch))
      memcpy(vector_elts,choice_elts,LISPVEC_BYTELEN(n));
    else while (i<n) {
        vector_elts[i]=kno_incref(choice_elts[i]);
        i++;}
    if ((VOIDP(sortspec))||(FALSEP(sortspec)))
      return vector;
    else {
      lispval_sort(vector_elts,n,flags);
      return vector;}}
  else {
    kno_incref(x);
    return kno_make_vector(1,&x);}
}

KNO_DCLPRIM1("choice->list",choice2list,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(CHOICE->LIST *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval choice2list(lispval x)
{
  lispval lst = NIL;
  DO_CHOICES(elt,x) lst = kno_conspair(kno_incref(elt),lst);
  return lst;
}

/* This iterates over the subsets of a choice and is useful for
    dividing a large dataset into smaller chunks for processing.
   It tries to save effort by not incref'ing elements when creating the subset.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   This returns VOID.  */
static lispval dosubsets_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval body = kno_get_body(expr,2);
  if (! (PRED_TRUE( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"forchoices_evalfn",NULL,expr);
  lispval choices, count_var, var;
  lispval control_spec = kno_get_arg(expr,1);
  lispval bsize; long long blocksize;
  if (!((PAIRP(control_spec)) &&
        (SYMBOLP(KNO_CAR(control_spec))) &&
        (PAIRP(KNO_CDR(control_spec))) &&
        (PAIRP(KNO_CDR(KNO_CDR(control_spec))))))
    return kno_err(kno_SyntaxError,"dosubsets_evalfn",NULL,VOID);
  var = KNO_CAR(control_spec);
  count_var = kno_get_arg(control_spec,3);
  if (!((VOIDP(count_var)) || (SYMBOLP(count_var))))
    return kno_err(kno_SyntaxError,"dosubsets_evalfn",NULL,VOID);
  bsize = kno_eval(KNO_CADR(KNO_CDR(control_spec)),env);
  if (KNO_ABORTED(bsize)) return bsize;
  else if (!(FIXNUMP(bsize)))
    return kno_type_error("fixnum","dosubsets_evalfn",bsize);
  else blocksize = FIX2INT(bsize);
  choices = kno_eval(KNO_CADR(control_spec),env);
  if (KNO_ABORTED(choices)) return choices;
  else {KNO_SIMPLIFY_CHOICE(choices);}
  if (EMPTYP(choices)) return VOID;
  KNO_ADD_TO_CHOICE(_stack->stack_vals,choices);
  INIT_STACK_ENV(_stack,dosubsets,env,2);
  dosubsets_vars[0]=var;
  if (SYMBOLP(count_var))
    dosubsets_vars[1]=count_var;
  else dosubsets_bindings.schema_length=1;
  int i = 0, n = KNO_CHOICE_SIZE(choices), n_blocks = 1+n/blocksize;
  int all_atomicp = ((CHOICEP(choices)) ?
                     (KNO_ATOMIC_CHOICEP(choices)) : (0));
  const lispval *data=
    ((CHOICEP(choices))?(KNO_CHOICE_DATA(choices)):(NULL));
  if ((n%blocksize)==0) n_blocks--;
  while (i<n_blocks) {
    lispval block;
    if ((CHOICEP(choices)) && (n_blocks>1)) {
      const lispval *read = &(data[i*blocksize]), *limit = read+blocksize;
      struct KNO_CHOICE *subset = kno_alloc_choice(blocksize); int atomicp = 1;
      lispval *write = ((lispval *)(KNO_XCHOICE_DATA(subset)));
      if (limit>(data+n)) limit = data+n;
      if (all_atomicp)
        while (read<limit) *write++= *read++;
      else while (read<limit) {
          lispval v = *read++;
          if (CONSP(v)) {
            atomicp=0; kno_incref(v);}
          *write++=v;}
      {KNO_INIT_XCHOICE(subset,write-KNO_XCHOICE_DATA(subset),atomicp);}
      block = (lispval)subset;}
    else block = kno_incref(choices);
    dosubsets_vals[0]=block;
    dosubsets_vals[1]=KNO_INT(i);
    {KNO_DOLIST(subexpr,body) {
        lispval val = fast_eval(subexpr,dosubsets);
        if (KNO_BROKEP(val)) {
          _return VOID;}
        else if (KNO_ABORTED(val))
          _return val;
        else kno_decref(val);}}
    reset_env(dosubsets);
    kno_decref(dosubsets_vals[0]);
    dosubsets_vals[0]=VOID;
    kno_decref(dosubsets_vals[1]);
    dosubsets_vals[1]=VOID;
    i++;}
  _return VOID;
}

/* Standard kinds of reduce choice */

KNO_DCLPRIM1("choice-size",choicesize_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(CHOICE-SIZE *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval choicesize_prim(lispval x)
{
  int n = KNO_CHOICE_SIZE(x);
  return KNO_INT(n);
}

KNO_DCLPRIM1("pick-one",pickone,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(PICK-ONE *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval pickone(lispval x)
{
  lispval normal = kno_make_simple_choice(x), chosen = EMPTY;
  if (CHOICEP(normal)) {
    int n = KNO_CHOICE_SIZE(normal);
    if (n) {
      int i = u8_random(n);
      const lispval *data = KNO_CHOICE_DATA(normal);
      chosen = data[i];
      kno_incref(data[i]); kno_decref(normal);
      return chosen;}
    else return EMPTY;}
  else return normal;
}

KNO_DCLPRIM2("sample-n",samplen,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(SAMPLE-N *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_INT(10));
static lispval samplen(lispval x,lispval count)
{
  if (EMPTYP(x))
    return x;
  else if (FIXNUMP(count)) {
    int howmany = kno_getint(count);
    if (howmany==0)
      return EMPTY;
    else if (howmany < 0)
      return kno_type_error("positive","samplen",count);
    else if (! (KNO_CHOICEP(x)) )
      return kno_incref(x);
    else {
      lispval normal = kno_make_simple_choice(x);
      lispval results = EMPTY;
      int n = KNO_CHOICE_SIZE(normal);
      if (n<=howmany)
        return normal;
      else {
        unsigned char *used=u8_zalloc_n(n,unsigned char);
        const lispval *data = KNO_CHOICE_DATA(normal);
        int j = 0; while (j<howmany) {
          int i = u8_random(n);
          if (!(used[i])) {
            lispval elt=data[i]; kno_incref(elt);
            CHOICE_ADD(results,data[i]);
            used[i]=1;
            j++;}}
        kno_decref(normal);
        u8_free(used);
        return kno_simplify_choice(results);}}}
  else return kno_type_error("integer","samplen",count);
}

KNO_DCLPRIM3("pick-n",pickn,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
 "`(PICK-N *arg0* *arg1* [*arg2*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID,
 kno_fixnum_type,KNO_VOID);
static lispval pickn(lispval x,lispval count,lispval offset)
{
  if (FIXNUMP(count)) {
    int howmany = kno_getint(count);
    if (howmany == 0)
      return EMPTY;
    else if (x == EMPTY)
      return EMPTY;
    else if (howmany < 0)
      return kno_type_error("positive","pickn",count);
    else if (KNO_CHOICEP(x)) {
      lispval normal = kno_make_simple_choice(x);
      int start=0, n = KNO_CHOICE_SIZE(normal);
      if (n<=howmany)
        return normal;
      else if (KNO_UINTP(offset)) {
        start = FIX2INT(offset);
        if ((n-start)<howmany) howmany = n-start;}
      else if ((FIXNUMP(offset))||(KNO_BIGINTP(offset))) {
        kno_decref(normal);
        return kno_type_error("small fixnum","pickn",offset);}
      else start = u8_random(n-howmany);
      if (howmany == 1) {
        struct KNO_CHOICE *base=
          (kno_consptr(struct KNO_CHOICE *,normal,kno_choice_type));
        const lispval *read = KNO_XCHOICE_DATA(base);
        lispval one = read[start];
        return kno_incref(one);}
      else if (n) {
        struct KNO_CHOICE *base=
          (kno_consptr(struct KNO_CHOICE *,normal,kno_choice_type));
        struct KNO_CHOICE *result = kno_alloc_choice(howmany);
        const lispval *read = KNO_XCHOICE_DATA(base)+start;
        lispval *write = (lispval *)KNO_XCHOICE_DATA(result);
        if (KNO_XCHOICE_ATOMICP(base)) {
          memcpy(write,read,LISPVEC_BYTELEN(howmany));
          kno_decref(normal);
          return kno_init_choice(result,howmany,NULL,KNO_CHOICE_ISATOMIC);}
        else {
          int atomicp = 1; const lispval *readlim = read+howmany;
          while (read<readlim) {
            lispval v = *read++;
            if (ATOMICP(v)) *write++=v;
            else {atomicp = 0; kno_incref(v); *write++=v;}}
          kno_decref(normal);
          return kno_init_choice(result,howmany,NULL,
                                ((atomicp)?(KNO_CHOICE_ISATOMIC):(0)));}}
      else return EMPTY;}
    else return kno_incref(x);}
  else return kno_type_error("integer","topn",count);
}

/* SMALLEST and LARGEST */

static int compare_lisp(lispval x,lispval y)
{
  kno_lisp_type xtype = KNO_LISP_TYPE(x), ytype = KNO_LISP_TYPE(y);
  if (xtype == ytype)
    switch (xtype) {
    case kno_fixnum_type: {
      long long xval = FIX2INT(x), yval = FIX2INT(y);
      if (xval < yval) return -1;
      else if (xval > yval) return 1;
      else return 0;}
    case kno_oid_type: {
      KNO_OID xval = KNO_OID_ADDR(x), yval = KNO_OID_ADDR(y);
      return KNO_OID_COMPARE(xval,yval);}
    default:
      return KNO_FULL_COMPARE(x,y);}
  else if (xtype<ytype) return -1;
  else return 1;
}

KNO_DCLPRIM2("smallest",smallest_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(SMALLEST *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval smallest_prim(lispval elts,lispval magnitude)
{
  if (KNO_CHOICEP(magnitude)) {
    /* TODO: This probably isn't the right thing. If the magnitude is a choice,
       we might want to use the smallest score as the basis for comparison.
       But developers can also just code that on the outside by defining a
       magnitude function which returns the largest magnitude. */
    lispval results = KNO_EMPTY;
    DO_CHOICES(ok,magnitude) {
      if (KEYFNP(ok)) {
        lispval largest = smallest_prim(elts,ok);
        if (KNO_ABORTP(largest)) {
          KNO_STOP_DO_CHOICES;
          kno_decref(results);
          return ok;}
        else {KNO_ADD_TO_CHOICE(results,largest);}}
      else {
        KNO_STOP_DO_CHOICES;
        kno_decref(results);
        return kno_type_error("keyfn","smallest_prim",ok);}}
    return results;}
  else if (PRED_FALSE (! (KEYFNP(magnitude)) ) )
    return kno_type_error("keyfn","smallest_prim",magnitude);
  else {
    lispval top = EMPTY, top_score = VOID;
    DO_CHOICES(elt,elts) {
      lispval score = keyfn_get(elt,magnitude);
      if ( (KNO_ABORTED(score)) || (KNO_VOIDP(score)) ) {
        u8_byte msgbuf[100];
        kno_decref(top);
        kno_decref(top_score);
        if (VOIDP(score))
          return kno_err(kno_VoidSortKey,"smallest_prim",
                         u8_bprintf(msgbuf,"from keyfn %q",magnitude),
                         elt);
        else return score;}
      else if (VOIDP(top_score))
        if (EMPTYP(score)) {}
        else {
          top = kno_incref(elt);
          top_score = score;}
      else if (EMPTYP(score)) {}
      else {
        int comparison = compare_lisp(score,top_score);
        if (comparison>0) {
          kno_decref(score);}
        else if (comparison == 0) {
          kno_incref(elt);
          CHOICE_ADD(top,elt);
          kno_decref(score);}
        else {
          kno_decref(top);
          kno_decref(top_score);
          top = kno_incref(elt);
          top_score = score;}}}
    kno_decref(top_score);
    return top;}
}

KNO_DCLPRIM2("largest",largest_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(LARGEST *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval largest_prim(lispval elts,lispval magnitude)
{
  if (KNO_CHOICEP(magnitude)) {
    /* TODO: This probably isn't the right thing. If the magnitude is a choice,
       we might want to use the largest score as the basis for comparison.
       But developers can also just code that on the outside by defining a
       magnitude function which returns the largest magnitude. */
    lispval results = KNO_EMPTY;
    DO_CHOICES(ok,magnitude) {
      if (KEYFNP(ok)) {
        lispval largest = largest_prim(elts,ok);
        if (KNO_ABORTP(largest)) {
          KNO_STOP_DO_CHOICES;
          kno_decref(results);
          return ok;}
        else {KNO_ADD_TO_CHOICE(results,largest);}}
      else {
        KNO_STOP_DO_CHOICES;
        kno_decref(results);
        return kno_type_error("keyfn","largest_prim",ok);}}
    return results;}
  else if (PRED_FALSE (! (KEYFNP(magnitude)) ) )
    return kno_type_error("keyfn","largest_prim",magnitude);
  else {
    lispval top = EMPTY, top_score = VOID;
    DO_CHOICES(elt,elts) {
      lispval score = keyfn_get(elt,magnitude);
      if ( (KNO_ABORTED(score)) || (KNO_VOIDP(score)) ) {
        u8_byte msgbuf[100];
        kno_decref(top);
        kno_decref(top_score);
        if (VOIDP(score))
          return kno_err(kno_VoidSortKey,"largest_prim",
                         u8_bprintf(msgbuf,"from keyfn %q",magnitude),
                         elt);
        else return score;}
      else if (VOIDP(top_score))
        if (EMPTYP(score)) {}
        else {
          top = kno_incref(elt);
          top_score = score;}
      else if (EMPTYP(score)) {}
      else {
        int comparison = compare_lisp(score,top_score);
        if (comparison<0) {
          kno_decref(score);}
        else if (comparison == 0) {
          kno_incref(elt);
          CHOICE_ADD(top,elt);
          kno_decref(score);}
        else {
          kno_decref(top);
          kno_decref(top_score);
          top = kno_incref(elt);
          top_score = score;}}}
    kno_decref(top_score);
    return top;}
}

/* Reduce choice */

static int reduce_functionp(kno_function f)
{
  return ( (f->fcn_min_arity == 2) ||
           ( (f->fcn_arity < 0) && (f->fcn_min_arity < 2) ) );
}

static int reduce_operatorp(lispval f)
{
  if (KNO_FUNCTIONP(f))
    return reduce_functionp((kno_function)f);
  else if (KNO_APPLICABLEP(f))
    return 1;
  else return 0;
}

static int non_deterministicp(lispval fn)
{
  if (KNO_FUNCTIONP(fn)) {
    kno_function f = (kno_function) fn;
    return (f->fcn_ndcall);}
  else if (KNO_APPLICABLEP(fn))
    return 1;
  else return 0;
}

static lispval inner_reduce_choice
(lispval choice,lispval fn,lispval start,lispval keyfn);

KNO_DCLPRIM4("reduce-choice",reduce_choice,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2)|KNO_NDCALL,
 "`(REDUCE-CHOICE *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval reduce_choice(lispval fn,lispval choice,lispval start,
                             lispval keyfn)
{
  /* Type checking up front */
  if (KNO_CHOICEP(fn)) {
    KNO_DO_CHOICES(f,fn)
      if (! (PRED_TRUE(reduce_operatorp(f))) ) {
        KNO_STOP_DO_CHOICES;
        return kno_type_error("reduce operator","reduce_choice",f);}}
  else if (! (PRED_TRUE(reduce_operatorp(fn))) )
    return kno_type_error("reduce operator","reduce_choice",fn);
  else NO_ELSE;
  if ( (VOIDP(keyfn)) || (FALSEP(keyfn)) || (DEFAULTP(keyfn)) ) {}
  else if (PRED_TRUE(KEYFNP(keyfn))) {}
  else if (KNO_CHOICEP(keyfn)) {
    KNO_DO_CHOICES(ok,keyfn)
      if (! (PRED_TRUE(KEYFNP(ok))) ) {
        KNO_STOP_DO_CHOICES;
        return kno_type_error("object key","reduce_choice",ok);}}
  else return kno_type_error("object key","reduce_choice",keyfn);
  return kno_simplify_choice(inner_reduce_choice(choice,fn,start,keyfn));
}

static lispval inner_reduce_choice
(lispval choice,lispval fn,lispval start,lispval keyfn)
{
  if (KNO_CHOICEP(fn)) {
    lispval results = KNO_EMPTY;
    KNO_DO_CHOICES(f,fn) {
      lispval reduction = inner_reduce_choice(choice,f,start,keyfn);
      if (KNO_ABORTED(reduction)) {
        KNO_STOP_DO_CHOICES;
        kno_decref(results);
        return reduction;}
      else KNO_ADD_TO_CHOICE(results,reduction);}
    return results;}
  else if (KNO_CHOICEP(keyfn)) {
    lispval results = KNO_EMPTY;
    KNO_DO_CHOICES(ok,keyfn) {
      lispval reduction = inner_reduce_choice(choice,fn,start,ok);
      if (KNO_ABORTED(reduction)) {
        KNO_STOP_DO_CHOICES;
        kno_decref(results);
        return reduction;}
      else KNO_ADD_TO_CHOICE(results,reduction);}
    return results;}
  else {
    int nd_reducer = non_deterministicp(fn);
    lispval state = kno_incref(start);
    DO_CHOICES(each,choice) {
      lispval items = keyfn_get(each,keyfn);
      if (KNO_ABORTED(items)) {
        KNO_STOP_DO_CHOICES;
        kno_decref(state);
        return items;}
      else if (VOIDP(state)) {
        state = items;
        continue;}
      else if (EMPTYP(items)) {}
      else if ( (! (nd_reducer) ) && (CHOICEP(items)) ) {
        DO_CHOICES(item,items) {
          lispval rail[2] = {item , state}, next_state;
          next_state = kno_apply(fn,2,rail);
          if (KNO_ABORTED(next_state)) {
            kno_decref(state);
            kno_decref(items);
            KNO_STOP_DO_CHOICES;
            return next_state;}
          kno_decref(state);
          state = next_state;}}
      else {
        lispval rail[2] = {items , state}, next_state;
        next_state = kno_apply(fn,2,rail);
        if (KNO_ABORTED(next_state)) {
          kno_decref(state);
          kno_decref(items);
          KNO_STOP_DO_CHOICES;
          return next_state;}
        kno_decref(state);
        state = next_state;}
      kno_decref(items);}
    return state;}
}

/* Sorting */

enum SORTFN {
  NORMAL_SORT, LEXICAL_SORT, LEXICAL_CI_SORT, COLLATED_SORT, POINTER_SORT };

static lispval lexical_symbol, lexci_symbol, collate_symbol, pointer_symbol;

static enum SORTFN get_sortfn(lispval arg)
{
  if ( (KNO_VOIDP(arg)) || (KNO_FALSEP(arg)) || (KNO_DEFAULTP(arg)) )
    return NORMAL_SORT;
  else if ( (KNO_TRUEP(arg)) || (arg == lexci_symbol) )
    return LEXICAL_CI_SORT;
  else if (arg == lexical_symbol)
    return LEXICAL_SORT;
  else if (arg == pointer_symbol)
    return POINTER_SORT;
  else if (arg == collate_symbol)
    return COLLATED_SORT;
  else if (arg == pointer_symbol)
    return POINTER_SORT;
  else return NORMAL_SORT;
}

static lispval sorted_primfn(lispval choices,lispval keyfn,int reverse,
                             enum SORTFN sortfn)
{
  if (EMPTYP(choices))
    return kno_empty_vector(0);
  else if (CHOICEP(choices)) {
    int i = 0, n = KNO_CHOICE_SIZE(choices), j = 0;
    lispval *vecdata = u8_big_alloc_n(n,lispval);
    struct KNO_SORT_ENTRY *entries = u8_big_alloc_n(n,struct KNO_SORT_ENTRY);
    DO_CHOICES(elt,choices) {
      lispval key=_kno_apply_keyfn(elt,keyfn);
      if ( (KNO_ABORTED(key)) || (KNO_VOIDP(key)) ) {
        u8_byte msgbuf[100];
        int j = 0; while (j<i) {
          kno_decref(entries[j].sortkey);
          j++;}
        u8_big_free(entries);
        u8_big_free(vecdata);
        if (KNO_ABORTP(key))
          return key;
        else return kno_err(kno_VoidSortKey,"sorted_primfn",
                            u8_bprintf(msgbuf,"From keyfn %q",keyfn),elt);}
      entries[i].sortval = elt;
      entries[i].sortkey = key;
      i++;}
    switch (sortfn) {
    case NORMAL_SORT:
      qsort(entries,n,sizeof(struct KNO_SORT_ENTRY),_kno_sort_helper);
      break;
    case LEXICAL_SORT:
      qsort(entries,n,sizeof(struct KNO_SORT_ENTRY),_kno_lexsort_helper);
      break;
    case LEXICAL_CI_SORT:
      qsort(entries,n,sizeof(struct KNO_SORT_ENTRY),_kno_lexsort_ci_helper);
      break;
    case POINTER_SORT:
      qsort(entries,n,sizeof(struct KNO_SORT_ENTRY),_kno_pointer_sort_helper);
      break;
    case COLLATED_SORT:
      qsort(entries,n,sizeof(struct KNO_SORT_ENTRY),_kno_collate_helper);
      break;}
    i = 0; j = n-1; if (reverse) while (i < n) {
      kno_decref(entries[i].sortkey);
      vecdata[j]=kno_incref(entries[i].sortval);
      i++; j--;}
    else while (i < n) {
      kno_decref(entries[i].sortkey);
      vecdata[i]=kno_incref(entries[i].sortval);
      i++;}
    u8_big_free(entries);
    return kno_cons_vector(NULL,n,1,vecdata);}
  else {
    lispval *vec = u8_alloc_n(1,lispval);
    vec[0]=kno_incref(choices);
    return kno_wrap_vector(1,vec);}
}

KNO_DCLPRIM3("sorted",sorted_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "(SORTED *choice* *keyfn* *sortfn*)"
 ", returns a sorted vector of items in *choice*. "
 "If provided, *keyfn* is specified a property "
 "(slot, function, table-mapping, etc) is compared "
 "instead of the object itself. *sortfn* can be "
 "NORMAL (#f), LEXICAL, or COLLATE to specify "
 "whether comparison of strings is done "
 "lexicographically or using the locale's COLLATE "
 "rules.",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_VOID);
static lispval sorted_prim(lispval choices,lispval keyfn,
                           lispval sortfn_arg)
{
  enum SORTFN sortfn;
  if ( (keyfn == lexical_symbol) || (keyfn == pointer_symbol) ||
       (keyfn == collate_symbol) || (keyfn == lexci_symbol) ) {
    sortfn = get_sortfn(keyfn);
    keyfn  = KNO_VOID;}
  else sortfn = get_sortfn(sortfn_arg);
  return sorted_primfn(choices,keyfn,0,sortfn);
}

KNO_DCLPRIM3("rsorted",rsorted_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "(RSORTED *choice* *keyfn* *sortfn*)"
 ", returns a sorted vector of items in *choice*. "
 "If provided, *keyfn* is specified a property "
 "(slot, function, table-mapping, etc) is compared "
 "instead of the object itself. *sortfn* can be "
 "NORMAL (#f), LEXICAL, or COLLATE to specify "
 "whether comparison of strings is done "
 "lexicographically or using the locale's COLLATE "
 "rules.",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_VOID);
static lispval rsorted_prim(lispval choices,lispval keyfn,
                            lispval sortfn_arg)
{
  enum SORTFN sortfn = get_sortfn(sortfn_arg);
  return sorted_primfn(choices,keyfn,1,sortfn);
}

KNO_DCLPRIM2("lexsorted",lexsorted_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(LEXSORTED *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval lexsorted_prim(lispval choices,lispval keyfn)
{
  return sorted_primfn(choices,keyfn,0,COLLATED_SORT);
}

/* Selection */

static ssize_t select_helper(lispval choices,lispval keyfn,
                             size_t k,int maximize,
                             struct KNO_SORT_ENTRY *entries)
{
#define BETTERP(x) ((maximize)?((x)>0):((x)<0))
#define IS_BETTER(x,y) (BETTERP(KNO_QCOMPARE((x),(y))))
  lispval worst = VOID;
  size_t worst_off = (maximize)?(0):(k-1);
  int k_len = 0;
  DO_CHOICES(elt,choices) {
    lispval key=_kno_apply_keyfn(elt,keyfn);
    if ( (KNO_ABORTED(key)) || (KNO_VOIDP(key)) ) {
      int j = 0; while (j<k_len) {
        kno_decref(entries[j].sortkey);
        j++;}
      if (VOIDP(key)) {
        u8_byte buf[80];
        kno_seterr(kno_VoidSortKey,"select_helper",
                   u8_bprintf(buf,"The keyfn %q return VOID",keyfn),
                   elt);}
      return -1;}
    else if (k_len<k) {
      entries[k_len].sortval = elt;
      entries[k_len].sortkey = key;
      k_len++;}
    else {
      /* If we get here, we've got k filled up and have the
         'minimal' value set aside. We iterate through the rest of
         the choice and only add a value (replacing the minimal
         one) if its better than the minimal one. */
      qsort(entries,k,sizeof(struct KNO_SORT_ENTRY),_kno_sort_helper);
      worst = entries[worst_off].sortkey;
      if (IS_BETTER(key,worst)) {
        kno_decref(worst);
        entries[worst_off].sortval = elt;
        entries[worst_off].sortkey = key;
        /* This could be done faster by either by just finding
           where to insert it, either by iterating O(n) or binary
           search O(log n). */
        qsort(entries,k,sizeof(struct KNO_SORT_ENTRY),_kno_sort_helper);
        worst = entries[worst_off].sortkey;}
      else kno_decref(key);}}
  return k_len;
#undef BETTERP
#undef IS_BETTER
}

static lispval entries2choice(struct KNO_SORT_ENTRY *entries,int n)
{
  if (n<0) {
    u8_free(entries);
    return KNO_ERROR_VALUE;}
  else if (n==0) {
    if (entries) u8_free(entries);
    return EMPTY;}
  else {
    lispval results = EMPTY;
    int i = 0; while (i<n) {
      lispval elt = entries[i].sortval;
      kno_decref(entries[i].sortkey); kno_incref(elt);
      CHOICE_ADD(results,elt);
      i++;}
    u8_free(entries);
    return kno_simplify_choice(results);}
}

KNO_DCLPRIM3("pick-max",nmax_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
 "`(PICK-MAX *arg0* *arg1* [*arg2*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID,
 kno_any_type,KNO_VOID);
static lispval nmax_prim(lispval choices,lispval karg,lispval keyfn)
{
  if ( (VOIDP(keyfn)) || (DEFAULTP(keyfn)) || (FALSEP(keyfn)) ) {}
  else if (!(KNO_APPLICABLEP(keyfn)))
    return kno_type_error(_("applicable"),"nmax_prim",keyfn);
  if (KNO_UINTP(karg)) {
    size_t k = FIX2INT(karg);
    size_t n = KNO_CHOICE_SIZE(choices);
    if (k == 0)
      return KNO_EMPTY;
    else if (n<=k)
      return kno_incref(choices);
    else {
      struct KNO_SORT_ENTRY *entries = u8_alloc_n(k,struct KNO_SORT_ENTRY);
      ssize_t count = select_helper(choices,keyfn,k,1,entries);
      if (count < 0) {
        u8_free(entries);
        return KNO_ERROR;}
      else return entries2choice(entries,count);}}
  else return kno_type_error(_("fixnum"),"nmax_prim",karg);
}

KNO_DCLPRIM3("max/sorted",nmax2vec_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
 "`(MAX/SORTED *arg0* *arg1* [*arg2*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID,
 kno_any_type,KNO_VOID);
static lispval nmax2vec_prim(lispval choices,lispval karg,lispval keyfn)
{
  if ( (VOIDP(keyfn)) || (DEFAULTP(keyfn)) || (FALSEP(keyfn)) ) {}
  else if (!(KNO_APPLICABLEP(keyfn)))
    return kno_type_error(_("applicable"),"nmax2vecprim",keyfn);
  if (KNO_UINTP(karg)) {
    size_t k = FIX2INT(karg);
    size_t n = KNO_CHOICE_SIZE(choices);
    if (n==0)
      return kno_make_vector(0,NULL);
    else if (n==1) {
      kno_incref(choices);
      return kno_make_vector(1,&choices);}
    else {
      ssize_t n_entries = (n<k) ? (n) : (k);
      struct KNO_SORT_ENTRY *entries =
        u8_alloc_n(n_entries,struct KNO_SORT_ENTRY);
      ssize_t count = select_helper(choices,keyfn,k,1,entries);
      if (count < 0) {
        u8_free(entries);
        return KNO_ERROR;}
      lispval vec = kno_make_vector(count,NULL);
      int i = 0; while (i<count) {
        lispval elt = entries[i].sortval;
        int vec_off = count-1-i;
        kno_incref(elt);
        kno_decref(entries[i].sortkey);
        KNO_VECTOR_SET(vec,vec_off,elt);
        i++;}
      u8_free(entries);
      return vec;}}
  else return kno_type_error(_("fixnum"),"nmax_prim",karg);
}

KNO_DCLPRIM3("pick-min",nmin_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
 "`(PICK-MIN *arg0* *arg1* [*arg2*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID,
 kno_any_type,KNO_VOID);
static lispval nmin_prim(lispval choices,lispval karg,lispval keyfn)
{
  if ( (VOIDP(keyfn)) || (DEFAULTP(keyfn)) || (FALSEP(keyfn)) ) {}
  else if (!(KNO_APPLICABLEP(keyfn)))
    return kno_type_error(_("applicable"),"nmin_prim",keyfn);
  if (KNO_UINTP(karg)) {
    size_t k = FIX2INT(karg);
    size_t n = KNO_CHOICE_SIZE(choices);
    if (k == 0)
      return KNO_EMPTY;
    else if (n<=k)
      return kno_incref(choices);
    else {
      struct KNO_SORT_ENTRY *entries = u8_alloc_n(k,struct KNO_SORT_ENTRY);
      ssize_t count = select_helper(choices,keyfn,k,0,entries);
      if (count < 0) {
        u8_free(entries);
        return KNO_ERROR;}
      else return entries2choice(entries,count);}}
  else return kno_type_error(_("fixnum"),"nmax_prim",karg);
}

KNO_DCLPRIM3("min/sorted",nmin2vec_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
 "`(MIN/SORTED *arg0* *arg1* [*arg2*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID,
 kno_any_type,KNO_VOID);
static lispval nmin2vec_prim(lispval choices,lispval karg,lispval keyfn)
{
  if ( (VOIDP(keyfn)) || (DEFAULTP(keyfn)) || (FALSEP(keyfn)) ) {}
  else if (!(KNO_APPLICABLEP(keyfn)))
    return kno_type_error(_("applicable"),"nmin2vecprim",keyfn);
  if (KNO_UINTP(karg)) {
    size_t k = FIX2INT(karg);
    size_t n = KNO_CHOICE_SIZE(choices);
    if (n==0)
      return kno_make_vector(0,NULL);
    else if (n==1) {
      kno_incref(choices);
      return kno_make_vector(1,&choices);}
    else {
      ssize_t n_entries = (n<k) ? (n) : (k);
      struct KNO_SORT_ENTRY *entries =
        u8_alloc_n(n_entries,struct KNO_SORT_ENTRY);
      ssize_t count = select_helper(choices,keyfn,k,0,entries);
      if (count<0) {
        u8_free(entries);
        return KNO_ERROR;}
      lispval vec = kno_make_vector(count,NULL);
      int i = 0; while (i<count) {
        kno_decref(entries[i].sortkey);
        kno_incref(entries[i].sortval);
        KNO_VECTOR_SET(vec,i,entries[i].sortval);
        i++;}
      u8_free(entries);
      return vec;}}
  else return kno_type_error(_("fixnum"),"nmax_prim",karg);
}

/* GETRANGE */

KNO_DCLPRIM2("getrange",getrange_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(GETRANGE *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval getrange_prim(lispval arg1,lispval endval)
{
  long long start, end; lispval results = EMPTY;
  if (VOIDP(endval))
    if (FIXNUMP(arg1)) {
      start = 0; end = FIX2INT(arg1);}
    else return kno_type_error(_("fixnum"),"getrange_prim",arg1);
  else if ((FIXNUMP(arg1)) && (FIXNUMP(endval))) {
    start = FIX2INT(arg1); end = FIX2INT(endval);}
  else if (FIXNUMP(endval))
    return kno_type_error(_("fixnum"),"getrange_prim",arg1);
  else return kno_type_error(_("fixnum"),"getrange_prim",endval);
  if (start>end) {int tmp = start; start = end; end = tmp;}
  while (start<end) {
    CHOICE_ADD(results,KNO_INT(start)); start++;}
  return results;
}

KNO_DCLPRIM3("pick>",pick_gt_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(PICK> *arg0* [*arg1*] [*arg2*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_INT(0),
 kno_any_type,KNO_FALSE);
static lispval pick_gt_prim(lispval items,lispval num,lispval checktype)
{
  lispval lower_bound = VOID;
  DO_CHOICES(n,num) {
    if (!(NUMBERP(n))) {
      KNO_STOP_DO_CHOICES;
      return kno_type_error("number","pick_gt_prim",n);}
    else if (VOIDP(lower_bound))
      lower_bound = n;
    else if (kno_numcompare(n,lower_bound)<0)
      lower_bound = n;
    else NO_ELSE;}
  {
    lispval results = EMPTY;
    DO_CHOICES(item,items)
      if (NUMBERP(item))
        if (kno_numcompare(item,lower_bound)>0) {
          kno_incref(item);
          CHOICE_ADD(results,item);}
        else {}
      else if (checktype == KNO_TRUE) {
        KNO_STOP_DO_CHOICES;
        kno_decref(results);
        return kno_type_error("number","pick_gt_prim",item);}
      else {}
    return results;
  }
}

KNO_DCLPRIM1("pickoids",pick_oids_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(PICKOIDS *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval pick_oids_prim(lispval items)
{
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if (OIDP(item)) {
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    kno_decref(results);
    return kno_incref(items);}
  else return results;
}

KNO_DCLPRIM1("picksyms",pick_syms_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(PICKSYMS *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval pick_syms_prim(lispval items)
{
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if (SYMBOLP(item)) {
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    kno_decref(results);
    return kno_incref(items);}
  else return results;
}

KNO_DCLPRIM1("pickstrings",pick_strings_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(PICKSTRINGS *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval pick_strings_prim(lispval items)
{
  /* I don't think we need to worry about getting a PRECHOICE here. */
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if (STRINGP(item)) {
      kno_incref(item);
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    kno_decref(results);
    return kno_incref(items);}
  else return results;
}

KNO_DCLPRIM1("pickvecs",pick_vecs_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(PICKVECS *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval pick_vecs_prim(lispval items)
{
  /* I don't think we need to worry about getting a PRECHOICE here. */
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if (VECTORP(item)) {
      kno_incref(item);
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    kno_decref(results);
    return kno_incref(items);}
  else return results;
}

KNO_DCLPRIM1("pickpairs",pick_pairs_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(PICKPAIRS *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval pick_pairs_prim(lispval items)
{
  /* I don't think we need to worry about getting a PRECHOICE here. */
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if (PAIRP(item)) {
      kno_incref(item);
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    kno_decref(results);
    return kno_incref(items);}
  else return results;
}

KNO_DCLPRIM1("picknums",pick_nums_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(PICKNUMS *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval pick_nums_prim(lispval items)
{
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if (FIXNUMP(item)) {
      CHOICE_ADD(results,item);}
    else if (NUMBERP(item)) {
      kno_incref(item);
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    kno_decref(results);
    return kno_incref(items);}
  else return results;
}

KNO_DCLPRIM1("pickmaps",pick_maps_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(PICKMAPS *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval pick_maps_prim(lispval items)
{
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if ( (KNO_SLOTMAPP(item)) || (KNO_SCHEMAPP(item)) ) {
      kno_incref(item);
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    kno_decref(results);
    return kno_incref(items);}
  else return results;
}

/* Initialize functions */

KNO_EXPORT void kno_init_choicefns_c()
{
  u8_register_source_file(_FILEINFO);

  kno_def_evalfn(kno_scheme_module,"DO-CHOICES","",dochoices_evalfn);
  kno_defalias(kno_scheme_module,"DO∀","DO-CHOICES");
  kno_def_evalfn(kno_scheme_module,"FOR-CHOICES","",forchoices_evalfn);
  kno_defalias(kno_scheme_module,"FOR∀","FOR-CHOICES");
  kno_def_evalfn(kno_scheme_module,"TRY-CHOICES","",trychoices_evalfn);
  kno_defalias(kno_scheme_module,"TRY∀","TRY-CHOICES");
  kno_def_evalfn(kno_scheme_module,"FILTER-CHOICES","",filterchoices_evalfn);
  kno_defalias(kno_scheme_module,"?∀","FILTER-CHOICES");

  /* kno_def_evalfn(kno_scheme_module,"CHOICEVEC","",choicevec_evalfn); */

  kno_def_evalfn(kno_scheme_module,"DO-SUBSETS","",dosubsets_evalfn);

  init_local_cprims();

#if 0
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("CHOICE",choice_prim,0)));
  kno_idefn(kno_scheme_module,kno_make_cprim0("FAIL",fail_prim));
  {
    lispval qc_prim=
      kno_make_ndprim(kno_make_cprimn("QCHOICE",qchoice_prim,0));
    kno_idefn(kno_scheme_module,qc_prim);
    kno_store(kno_scheme_module,kno_intern("qc"),qc_prim);
  }

  {
    lispval qcx_prim=
      kno_make_ndprim(kno_make_cprimn("QCHOICEX",qchoicex_prim,0));
    kno_idefn(kno_scheme_module,qcx_prim);
    kno_store(kno_scheme_module,kno_intern("qcx"),qcx_prim);
  }

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("INTERSECTION",intersection_lexpr,1)));
  kno_defalias(kno_scheme_module,"∩","INTERSECTION");
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("UNION",union_lexpr,1)));
  kno_defalias(kno_scheme_module,"∪","UNION");
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("DIFFERENCE",difference_lexpr,1)));
  kno_defalias(kno_scheme_module,"∖","DIFFERENCE");

  kno_idefn(kno_scheme_module,
            kno_make_ndprim(kno_make_cprim2("SMALLEST",smallest_prim,1)));
  kno_idefn(kno_scheme_module,
            kno_make_ndprim(kno_make_cprim2("LARGEST",largest_prim,1)));
  {
    lispval empty_prim=
      kno_make_ndprim(kno_make_cprim1("EMPTY?",emptyp,1));
    kno_idefn(kno_scheme_module,empty_prim);
    kno_store(kno_scheme_module,kno_intern("fail?"),empty_prim);
    kno_store(kno_scheme_module,kno_intern("∄"),empty_prim);
  }




  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("SATISFIED?",satisfiedp,1)));

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("EXISTS?",existsp,1)));

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("CHOICE-SIZE",choicesize_prim,1)));
  kno_defalias(kno_scheme_module,"Ω","CHOICE-SIZE");
  kno_defalias(kno_scheme_module," ","CHOICE-SIZE");
  kno_defalias(kno_scheme_module,"","CHOICE-SIZE");

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("EXISTS",exists_lexpr,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("EXISTS/SKIPERRS",
                                           exists_skiperrs,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("SOMETRUE",sometrue_lexpr,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("SOMETRUE/SKIPERRS",
                                           sometrue_skiperrs,1)));
  kno_defalias(kno_scheme_module,"∃","SOMETRUE");

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("FORALL",forall_lexpr,1)));
  kno_defalias(kno_scheme_module,"∀","FORALL");

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("FORALL/SKIPERRS",forall_skiperrs,1)));


  {
    lispval unique_prim=
      kno_make_ndprim(kno_make_cprim1("UNIQUE?",singletonp,1));
    kno_idefn(kno_scheme_module,unique_prim);
    kno_store(kno_scheme_module,kno_intern("singleton?"),unique_prim);
    kno_store(kno_scheme_module,kno_intern("sole?"),unique_prim);}

#endif

  kno_def_evalfn(kno_scheme_module,"TRY","",try_evalfn);
  kno_def_evalfn(kno_scheme_module,"IFEXISTS","",ifexists_evalfn);

  kno_def_evalfn(kno_scheme_module,"WHENEXISTS","",whenexists_evalfn);

  kno_def_evalfn(kno_scheme_module,"QCHOICE?","",qchoicep_evalfn);

#if 0
  kno_idefn(kno_scheme_module,kno_make_ndprim(kno_make_cprim1("AMB?",ambiguousp,1)));
  kno_defalias(kno_scheme_module,"AMBIGUOUS?","AMB?");
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("SINGLETON",singleton,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim2x("CHOICE-MAX",choice_max,2,
                                          -1,VOID,kno_fixnum_type,VOID)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim2x("CHOICE-MIN",choice_min,2,
                                          -1,VOID,kno_fixnum_type,VOID)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("SIMPLIFY",simplify,1)));

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim2("CHOICE->VECTOR",choice2vector,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("CHOICE->LIST",choice2list,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim4("REDUCE-CHOICE",reduce_choice,2)));

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("PICK-ONE",pickone,1)));

  kno_idefn3(kno_scheme_module,"SORTED",sorted_prim,KNO_NEEDS_1_ARG|KNO_NDCALL,
            "(SORTED *choice* *keyfn* *sortfn*), returns a sorted vector "
            "of items in *choice*. If provided, *keyfn* is specified "
            "a property (slot, function, table-mapping, etc) is compared "
            "instead of the object itself. "
            "*sortfn* can be NORMAL (#f), LEXICAL, or COLLATE "
            "to specify whether comparison of strings is done lexicographically "
            "or using the locale's COLLATE rules.",
            -1,KNO_VOID,-1,KNO_VOID,-1,KNO_VOID);
  kno_idefn3(kno_scheme_module,"RSORTED",rsorted_prim,KNO_NEEDS_1_ARG|KNO_NDCALL,
            "(RSORTED *choice* *keyfn* *sortfn*), returns a sorted vector "
            "of items in *choice*. If provided, *keyfn* is specified "
            "a property (slot, function, table-mapping, etc) is compared "
            "instead of the object itself. "
            "*sortfn* can be NORMAL (#f), LEXICAL, or COLLATE "
            "to specify whether comparison of strings is done lexicographically "
            "or using the locale's COLLATE rules.",
            -1,KNO_VOID,-1,KNO_VOID,-1,KNO_VOID);

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim2("LEXSORTED",lexsorted_prim,1)));

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim3x("PICK>",pick_gt_prim,1,
                                          -1,VOID,
                                          -1,KNO_INT(0),
                                          -1,KNO_FALSE)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("PICKOIDS",pick_oids_prim,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("PICKNUMS",pick_nums_prim,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("PICKMAPS",pick_maps_prim,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("PICKSTRINGS",pick_strings_prim,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("PICKSYMS",pick_syms_prim,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("PICKPAIRS",pick_pairs_prim,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("PICKVECS",pick_vecs_prim,1)));

  kno_idefn(kno_scheme_module,
           kno_make_cprim2("GETRANGE",getrange_prim,1));

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim2x
                          ("SAMPLE-N",samplen,1,-1,VOID,
                           kno_fixnum_type,KNO_SHORT2DTYPE(10))));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim3x("PICK-N",pickn,2,
                                          -1,VOID,kno_fixnum_type,VOID,
                                          kno_fixnum_type,VOID)));

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim3x("PICK-MAX",nmax_prim,2,
                                          -1,VOID,kno_fixnum_type,VOID,
                                          -1,VOID)));
  kno_defalias(kno_scheme_module,"NMAX","PICK-MAX");

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim3x("MAX/SORTED",nmax2vec_prim,2,
                                          -1,VOID,kno_fixnum_type,VOID,
                                          -1,VOID)));
  kno_defalias(kno_scheme_module,"NMAX->VECTOR","MAX/SORTED");

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim3x("PICK-MIN",nmin_prim,2,
                                          -1,VOID,kno_fixnum_type,VOID,
                                          -1,VOID)));
  kno_defalias(kno_scheme_module,"NMIN","PICK-MIN");

  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim3x("MIN/SORTED",nmin2vec_prim,2,
                                          -1,VOID,kno_fixnum_type,VOID,
                                          -1,VOID)));
  kno_defalias(kno_scheme_module,"NMIN->VECTOR","MIN/SORTED");

#endif

  lexical_symbol = kno_intern("lexical");
  lexci_symbol = kno_intern("lexical/ci");
  collate_symbol = kno_intern("collate");

}

static void init_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_PRIM("pickmaps",pick_maps_prim,1,scheme_module);
  KNO_LINK_PRIM("picknums",pick_nums_prim,1,scheme_module);
  KNO_LINK_PRIM("pickpairs",pick_pairs_prim,1,scheme_module);
  KNO_LINK_PRIM("pickvecs",pick_vecs_prim,1,scheme_module);
  KNO_LINK_PRIM("pickstrings",pick_strings_prim,1,scheme_module);
  KNO_LINK_PRIM("picksyms",pick_syms_prim,1,scheme_module);
  KNO_LINK_PRIM("pickoids",pick_oids_prim,1,scheme_module);
  KNO_LINK_PRIM("pick>",pick_gt_prim,3,scheme_module);
  KNO_LINK_PRIM("getrange",getrange_prim,2,scheme_module);
  KNO_LINK_PRIM("min/sorted",nmin2vec_prim,3,scheme_module);
  KNO_LINK_PRIM("pick-min",nmin_prim,3,scheme_module);
  KNO_LINK_PRIM("max/sorted",nmax2vec_prim,3,scheme_module);
  KNO_LINK_PRIM("pick-max",nmax_prim,3,scheme_module);
  KNO_LINK_PRIM("lexsorted",lexsorted_prim,2,scheme_module);
  KNO_LINK_PRIM("rsorted",rsorted_prim,3,scheme_module);
  KNO_LINK_PRIM("sorted",sorted_prim,3,scheme_module);
  KNO_LINK_PRIM("reduce-choice",reduce_choice,4,scheme_module);
  KNO_LINK_PRIM("largest",largest_prim,2,scheme_module);
  KNO_LINK_PRIM("smallest",smallest_prim,2,scheme_module);
  KNO_LINK_PRIM("pick-n",pickn,3,scheme_module);
  KNO_LINK_PRIM("sample-n",samplen,2,scheme_module);
  KNO_LINK_PRIM("pick-one",pickone,1,scheme_module);
  KNO_LINK_PRIM("choice-size",choicesize_prim,1,scheme_module);
  KNO_LINK_PRIM("choice->list",choice2list,1,scheme_module);
  KNO_LINK_PRIM("choice->vector",choice2vector,2,scheme_module);
  KNO_LINK_VARARGS("difference",difference_lexpr,scheme_module);
  KNO_LINK_VARARGS("intersection",intersection_lexpr,scheme_module);
  KNO_LINK_VARARGS("union",union_lexpr,scheme_module);
  KNO_LINK_VARARGS("forall/skiperrs",forall_skiperrs,scheme_module);
  KNO_LINK_VARARGS("forall",forall_lexpr,scheme_module);
  KNO_LINK_VARARGS("sometrue/skiperrs",sometrue_skiperrs,scheme_module);
  KNO_LINK_VARARGS("sometrue",sometrue_lexpr,scheme_module);
  KNO_LINK_VARARGS("exists/skiperrs",exists_skiperrs,scheme_module);
  KNO_LINK_VARARGS("exists",exists_lexpr,scheme_module);
  KNO_LINK_PRIM("simplify",simplify,1,scheme_module);
  KNO_LINK_PRIM("choice-min",choice_min,2,scheme_module);
  KNO_LINK_PRIM("choice-max",choice_max,2,scheme_module);
  KNO_LINK_PRIM("singleton",singleton,1,scheme_module);
  KNO_LINK_PRIM("amb?",ambiguousp,1,scheme_module);
  KNO_LINK_PRIM("unique?",singletonp,1,scheme_module);
  KNO_LINK_PRIM("exists?",existsp,1,scheme_module);
  KNO_LINK_PRIM("satisfied?",satisfiedp,1,scheme_module);
  KNO_LINK_PRIM("empty?",emptyp,1,scheme_module);
  KNO_LINK_VARARGS("qchoicex",qchoicex_prim,scheme_module);
  KNO_LINK_VARARGS("qchoice",qchoice_prim,scheme_module);
  KNO_LINK_VARARGS("choice",choice_prim,scheme_module);
  KNO_LINK_PRIM("fail",fail_prim,0,scheme_module);

  KNO_LINK_ALIAS("qc",qchoice_prim,scheme_module);
  KNO_LINK_ALIAS("qcx",qchoicex_prim,scheme_module);
  KNO_LINK_ALIAS("fail?",emptyp,scheme_module);
  KNO_LINK_ALIAS("∄",emptyp,scheme_module);
  KNO_LINK_ALIAS("singleton?",singletonp,scheme_module);
  KNO_LINK_ALIAS("sole?",singletonp,scheme_module);
  KNO_LINK_ALIAS("ambiguous?",ambiguousp,scheme_module);
  KNO_LINK_ALIAS("∃",sometrue_lexpr,scheme_module);
  KNO_LINK_ALIAS("∀",forall_lexpr,scheme_module);
  KNO_LINK_ALIAS("∪",union_lexpr,scheme_module);
  KNO_LINK_ALIAS("∩",intersection_lexpr,scheme_module);
  KNO_LINK_ALIAS("∖",difference_lexpr,scheme_module);
  KNO_LINK_ALIAS("ω",choicesize_prim,scheme_module);
  KNO_LINK_ALIAS("| |",choicesize_prim,scheme_module);
  KNO_LINK_ALIAS("",choicesize_prim,scheme_module);
  KNO_LINK_ALIAS("nmax",nmax_prim,scheme_module);
  KNO_LINK_ALIAS("nmax->vector",nmax2vec_prim,scheme_module);
  KNO_LINK_ALIAS("nmin",nmin_prim,scheme_module);
  KNO_LINK_ALIAS("nmin->vector",nmin2vec_prim,scheme_module);

}
