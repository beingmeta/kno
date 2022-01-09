/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
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
#include "kno/cons.h"
#include "kno/storage.h"
#include "kno/numbers.h"
#include "kno/frames.h"
#include "eval_internals.h"
#include "kno/sorting.h"

static lispval keyfn_get(lispval val,lispval keyfn)
{
  if ( (VOIDP(keyfn)) || (KNO_FALSEP(keyfn)) || (KNO_DEFAULTP(keyfn)) )
    return kno_incref(val);
  else {
    kno_lisp_type type = KNO_TYPEOF(keyfn);
    switch (type) {
    case kno_hashtable_type:
    case kno_slotmap_type:
    case kno_schemap_type:
      return kno_get(keyfn,val,EMPTY);
    case kno_lambda_type:
    case kno_cprim_type:
      return kno_dapply(keyfn,1,&val);
    default:
      if (KNO_APPLICABLEP(keyfn))
	return kno_dapply(keyfn,1,&val);
      else if (KNO_TABLEP(keyfn))
	return kno_get(keyfn,val,EMPTY);
      else if (OIDP(val))
	return kno_frame_get(val,keyfn);
      else if (KNO_TABLEP(val))
	return kno_get(val,keyfn,EMPTY);
      else return KNO_EMPTY;}}
}

#define KEYFNP(x)				 \
  ((VOIDP(x)) || (FALSEP(x)) || (DEFAULTP(x)) || \
   (SYMBOLP(x)) || (OIDP(x)) ||                  \
   (TABLEP(x)) ||                                \
   (KNO_APPLICABLEP(x)))

/* Choice iteration */

/* This iterates over a set of choices, evaluating its body for each value.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns VOID. */
static lispval dochoices_evalfn(lispval expr,kno_lexenv env,
				kno_stack eval_stack)
{
  lispval steps = kno_get_body(expr,2);
  if (! (USUALLY( (KNO_PAIRP(steps)) || (steps == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"dochoices_evalfn",NULL,expr);
  lispval var, count_var, val_var, choices=
    parse_control_spec(expr,&var,&count_var,&val_var,env,eval_stack);
  if (KNO_ABORTED(var)) return var;
  else if (KNO_ABORTED(choices)) return choices;
  else if (EMPTYP(choices))
    return VOID;

  lispval result = KNO_VOID;
  KNO_INIT_ITER_LOOP(dochoices,var,choices,3,eval_stack,env);
  init_iter_env(&dochoices_bindings,choices,var,count_var,val_var);
  int finished = 0;
  int i = 0; DO_CHOICES(elt,choices) {
    dochoices_vals[0]=kno_incref(elt);
    dochoices_stack->stack_op=dochoices_vals[1]=KNO_INT(i);
    result = eval_body(steps,dochoices_env,dochoices_stack,
		       "dochoices",KNO_SYMBOL_NAME(var),
		       0);
    if (KNO_ABORTED(result)) {
      if (KNO_BROKEP(result)) result = KNO_VOID;
      finished=1;}
    reset_env(dochoices);
    kno_decref(dochoices_vals[0]);
    dochoices_vals[0]=VOID;
    kno_decref(dochoices_vals[1]);
    dochoices_vals[1]=VOID;
    if (finished) break; else i++;}
  kno_pop_stack(dochoices_stack);
  return result;
}

/* This iterates over a set of choices, evaluating its body for each value.
   It returns the first non-empty result of evaluating the body.
   Note that this treats a non-choice as a choice of one element.
   It returns VOID. */
static lispval trychoices_evalfn(lispval expr,kno_lexenv env,
				 kno_stack eval_stack)
{
  lispval steps = kno_get_body(expr,2);
  if (! (USUALLY( (KNO_PAIRP(steps)) || (steps == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"trychoices_evalfn",NULL,expr);
  lispval var, count_var, val_var, choices=
    parse_control_spec(expr,&var,&count_var,&val_var,env,eval_stack);
  if (KNO_ABORTED(var)) return var;
  else if (KNO_ABORTED(choices)) return choices;
  else if (EMPTYP(choices))
    return EMPTY;
  lispval result = KNO_EMPTY;
  KNO_INIT_ITER_LOOP(trychoices,var,choices,3,eval_stack,env);
  init_iter_env(&trychoices_bindings,choices,var,count_var,val_var);
  int finished = 0;
  int i = 0; DO_CHOICES(elt,choices) {
    lispval val = VOID;
    trychoices_vals[0]=kno_incref(elt);
    trychoices_vals[1]=KNO_INT(i);
    val = eval_body(steps,trychoices_env,trychoices_stack,
		    "trychoices",KNO_SYMBOL_NAME(var),
		    0);
    if (KNO_ABORTED(val)) {
      if (KNO_BROKEP(val))
	result = KNO_EMPTY;
      else {
	kno_decref(result);
	result = val;}
      finished=1;}
    reset_env(trychoices);
    kno_decref(trychoices_vals[0]);
    trychoices_vals[0]=VOID;
    kno_decref(trychoices_vals[1]);
    trychoices_vals[1]=VOID;
    if (finished) break;
    else if (!(EMPTYP(val))) {
      result = val;
      break;}
    else i++;}
  kno_pop_stack(trychoices_stack);
  return result;
}

/* This iterates over a set of choices, evaluating its body for each value, and
   accumulating the results of those evaluations.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns the combined results of its body's execution. */
static lispval forchoices_evalfn(lispval expr,kno_lexenv env,
				 kno_stack eval_stack)
{
  lispval steps = kno_get_body(expr,2);
  if (! (USUALLY( (KNO_PAIRP(steps)) || (steps == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"forchoices_evalfn",NULL,expr);
  lispval var, count_var, val_var, choices=
    parse_control_spec(expr,&var,&count_var,&val_var,env,eval_stack);
  if (KNO_ABORTED(var)) return var;
  else if (KNO_ABORTED(choices))
    return choices;
  else if (EMPTYP(choices))
    return EMPTY;

  lispval results = KNO_EMPTY;
  KNO_INIT_ITER_LOOP(forchoices,var,choices,3,eval_stack,env);
  init_iter_env(&forchoices_bindings,choices,var,count_var,val_var);
  int finished = 0;
  int i = 0; DO_CHOICES(elt,choices) {
    lispval val = VOID;
    forchoices_vals[0]=kno_incref(elt);
    forchoices_vals[1]=KNO_INT(i);
    val = eval_body(steps,forchoices_env,forchoices_stack,
		    "forchoices",KNO_SYMBOL_NAME(var),
		    0);
    if (KNO_ABORTED(val)) {
      if (KNO_BROKEP(val)) val = KNO_EMPTY;
      else {
	kno_decref(results);
	results = val;}
      finished=1;}
    if (!(finished)) {CHOICE_ADD(results,val);}
    reset_env(forchoices);
    kno_decref(forchoices_vals[0]);
    forchoices_vals[0]=VOID;
    kno_decref(forchoices_vals[1]);
    forchoices_vals[1]=VOID;
    if (finished) break;
    else i++;}
  kno_pop_stack(forchoices_stack);
  return kno_simplify_choice(results);
}

/* This iterates over a set of choices, evaluating its third subexpression for each value, and
   accumulating those values for which the body returns true.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns the subset of values which pass the body. */
static lispval filterchoices_evalfn(lispval expr,kno_lexenv env,
				    kno_stack eval_stack)
{
  lispval steps = kno_get_body(expr,2);
  if (! (USUALLY( (KNO_PAIRP(steps)) || (steps == KNO_NIL) )) )
    return kno_err(kno_SyntaxError,"filterchoices_evalfn",NULL,expr);
  lispval var, count_var, val_var, choices=
    parse_control_spec(expr,&var,&count_var,&val_var,env,eval_stack);
  if (KNO_ABORTED(var)) return var;
  else if (KNO_ABORTED(choices))
    return choices;
  else if (EMPTYP(choices))
    return EMPTY;
  lispval results = EMPTY;
  KNO_INIT_ITER_LOOP(filterchoices,var,choices,3,eval_stack,env);
  init_iter_env(&filterchoices_bindings,choices,var,count_var,val_var);
  int finished = 0;
  int i = 0; DO_CHOICES(elt,choices) {
    lispval val = VOID;
    filterchoices_vals[0]=kno_incref(elt);
    filterchoices_vals[1]=KNO_INT(i);
    val = eval_body(steps,filterchoices_env,filterchoices_stack,
		    "filterchoices",KNO_SYMBOL_NAME(var),
		    0);
    if (KNO_ABORTED(val)) {
      if (KNO_BROKEP(val)) val = KNO_EMPTY;
      else {
	kno_decref(results);
	results = val;}
      finished=1;}
    else if (!((EMPTYP(val)) || (FALSEP(val)))) {
      kno_incref(elt);
      CHOICE_ADD(results,elt);}
    reset_env(filterchoices);
    kno_decref(filterchoices_vals[0]);
    filterchoices_vals[0]=VOID;
    kno_decref(filterchoices_vals[1]);
    filterchoices_vals[1]=VOID;
    if (finished) break; else i++;}
  kno_pop_stack(filterchoices_stack);
  return kno_simplify_choice(results);
}

/* Choice functions */

DEFC_PRIM("fail",fail_prim,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "**undocumented**")
static lispval fail_prim()
{
  return EMPTY;
}

DEFC_PRIMN("choice",choice_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDCALL,
	   "**undocumented**")
static lispval choice_prim(int n,kno_argvec args)
{
  int i = 0; lispval results = EMPTY;
  while (i < n) {
    lispval arg = args[i++]; kno_incref(arg);
    CHOICE_ADD(results,arg);}
  return kno_simplify_choice(results);
}

DEFC_PRIMN("qchoice",qchoice_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDCALL,
	   "**undocumented**")
static lispval qchoice_prim(int n,kno_argvec args)
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

DEFC_PRIMN("qchoicex",qchoicex_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDCALL,
	   "**undocumented**")
static lispval qchoicex_prim(int n,kno_argvec args)
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
    value = kno_eval(clause,env,_stack);
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
  else value = kno_eval(value_expr,env,_stack);
  if (KNO_ABORTED(value))
    return value;
  if (EMPTYP(value))
    return VOID;
  else return value;
}

/* Predicates */

DEFC_PRIM("empty?",emptyp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
static lispval emptyp(lispval x)
{
  if (EMPTYP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFC_PRIM("satisfied?",satisfiedp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
static lispval satisfiedp(lispval x)
{
  if (EMPTYP(x)) return KNO_FALSE;
  else if (FALSEP(x)) return KNO_FALSE;
  else return KNO_TRUE;
}

DEFC_PRIM("exists?",existsp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
static lispval existsp(lispval x)
{
  if (EMPTYP(x)) return KNO_FALSE; else return KNO_TRUE;
}

DEFC_PRIM("unique?",singletonp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
static lispval singletonp(lispval x)
{
  if (CHOICEP(x))
    return KNO_FALSE;
  else if (EMPTYP(x))
    return KNO_FALSE;
  else return KNO_TRUE;
}

DEFC_PRIM("amb?",ambiguousp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
static lispval ambiguousp(lispval x) /* TODO: Wasted effort around here */
{
  if (EMPTYP(x))
    return KNO_FALSE;
  else if (CHOICEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("singleton",singleton,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
static lispval singleton(lispval x)
{
  if (EMPTYP(x)) return x;
  else if (CHOICEP(x))
    return EMPTY;
  else return kno_incref(x);
}

DEFC_PRIM("choice-max",choice_max,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID},
	  {"lim",kno_fixnum_type,KNO_VOID})
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

DEFC_PRIM("choice-min",choice_min,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID},
	  {"lim",kno_fixnum_type,KNO_VOID})
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

DEFC_PRIM("simplify",simplify,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
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
    lispval val = kno_eval(KNO_CADR(expr),env,_stack);
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

static int test_exists(lispval fn,
		       int i,int n,kno_argvec nd_args,lispval *d_args,
		       int skip_errs)
{
  if (i == n) {
    lispval val = kno_dapply(fn,n,d_args);
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

static lispval exists_helper(int n,kno_argvec nd_args,int skip_errs)
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
      int retval = test_exists(fcn,0,n-1,nd_args+1,d_args,skip_errs);
      if (retval == 0) continue;
      KNO_STOP_DO_CHOICES;
      if (retval<0)
	return KNO_ERROR;
      else return KNO_TRUE;}}
  return KNO_FALSE;
}

DEFC_PRIMN("exists",exists_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "**undocumented**")
static lispval exists_lexpr(int n,kno_argvec nd_args)
{
  return exists_helper(n,nd_args,PASS_ERRS);
}

DEFC_PRIMN("exists/skiperrs",exists_skiperrs,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "**undocumented**")
static lispval exists_skiperrs(int n,kno_argvec nd_args)
{
  return exists_helper(n,nd_args,SKIP_ERRS);
}

DEFC_PRIMN("sometrue",sometrue_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "**undocumented**")
static lispval sometrue_lexpr(int n,kno_argvec nd_args)
{
  if (n==1)
    if (EMPTYP(nd_args[0]))
      return KNO_FALSE;
    else if (KNO_FALSEP(nd_args[0]))
      return KNO_FALSE;
    else return KNO_TRUE;
  else return exists_helper(n,nd_args,PASS_ERRS);
}

DEFC_PRIMN("sometrue/skiperrs",sometrue_skiperrs,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "**undocumented**")
static lispval sometrue_skiperrs(int n,kno_argvec nd_args)
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
		       kno_argvec nd_args,lispval *d_args,
		       int skip_errs)
{
  if (i == n) {
    lispval val = kno_dapply((lispval)fn,n,d_args);
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
  else value = kno_eval(to_eval,env,_stack);
  if (KNO_ABORTED(value)) {
    kno_clear_errors(0);
    return VOID;}
  else if (EMPTYP(value))
    return VOID;
  else return value;
}

static lispval forall_helper(int n,kno_argvec nd_args,int skip_errs)
{
  int i = 0; while (i<n)
	       if (EMPTYP(nd_args[i])) return KNO_TRUE;
	       else i++;
  {DO_CHOICES(fcn,nd_args[0])
      if (KNO_FUNCTIONP(fcn)) {}
      else {
	KNO_STOP_DO_CHOICES;
	return kno_type_error(_("function"),"forall_helper",nd_args[0]);}}
  lispval d_args[n-1];
  {DO_CHOICES(fcn,nd_args[0]) {
      struct KNO_FUNCTION *f = KNO_FUNCTION_INFO(fcn);
      int retval = test_forall(f,0,n-1,nd_args+1,d_args,skip_errs);
      if ( (retval < 0) && (skip_errs == 0) )
	return KNO_ERROR;
      else if (retval == 0) {
	KNO_STOP_DO_CHOICES;
	return KNO_FALSE;}
      else {}}}
  return KNO_TRUE;
}

DEFC_PRIMN("forall",forall_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "**undocumented**")
static lispval forall_lexpr(int n,kno_argvec nd_args)
{
  return forall_helper(n,nd_args,PASS_ERRS);
}

DEFC_PRIMN("forall/skiperrs",forall_skiperrs,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "**undocumented**")
static lispval forall_skiperrs(int n,kno_argvec nd_args)
{
  return forall_helper(n,nd_args,SKIP_ERRS);
}

/* Set operations */

DEFC_PRIMN("union",union_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "**undocumented**")
static lispval union_lexpr(int n,kno_argvec args)
{
  return kno_simplify_choice(kno_union(args,n));
}

DEFC_PRIMN("intersection",intersection_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "**undocumented**")
static lispval intersection_lexpr(int n,kno_argvec args)
{
  return kno_simplify_choice(kno_intersection(args,n));
}

DEFC_PRIMN("difference",difference_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	   "**undocumented**")
static lispval difference_lexpr(int n,kno_argvec args)
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

/* Conversion functions */

DEFC_PRIM("choice->vector",choice2vector,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID},
	  {"sortspec",kno_any_type,KNO_VOID})
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

DEFC_PRIM("choice->list",choice2list,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
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
  lispval result = KNO_VOID;
  lispval body = kno_get_body(expr,2);
  if (! (USUALLY( (KNO_PAIRP(body)) || (body == KNO_NIL) )) )
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
  bsize = kno_eval(KNO_CADR(KNO_CDR(control_spec)),env,_stack);
  if (KNO_ABORTED(bsize)) return bsize;
  else if (!(FIXNUMP(bsize)))
    return kno_type_error("fixnum","dosubsets_evalfn",bsize);
  else blocksize = FIX2INT(bsize);
  choices = kno_eval(KNO_CADR(control_spec),env,_stack);
  if (KNO_ABORTED(choices)) return choices;
  else {KNO_SIMPLIFY_CHOICE(choices);}
  if (EMPTYP(choices)) return VOID;
  KNO_INIT_ITER_LOOP(dosubsets,var,choices,3,_stack,env);
  dosubsets_vars[0]=var;
  if (SYMBOLP(count_var))
    dosubsets_vars[1]=count_var;
  else dosubsets_bindings.schema_length=1;
  int i = 0, n = KNO_CHOICE_SIZE(choices), n_blocks = 1+n/blocksize;
  int all_atomicp = ((CHOICEP(choices)) ?
		     (KNO_ATOMIC_CHOICEP(choices)) : (0));
  int finished = 0;
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
	lispval val = kno_eval(subexpr,dosubsets_env,dosubsets_stack);
	if (KNO_ABORTED(val)) {
	  finished = 1;
	  if (!(KNO_BROKEP(val)))
	    result=val;
	  break;}
	else kno_decref(val);}}
    reset_env(dosubsets);
    kno_decref(dosubsets_vals[0]);
    dosubsets_vals[0]=VOID;
    kno_decref(dosubsets_vals[1]);
    dosubsets_vals[1]=VOID;
    if (!(finished)) i++;}
  kno_pop_stack(dosubsets_stack);
  return result;
}

/* Standard kinds of reduce choice */

DEFC_PRIM("choice-size",choicesize_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
static lispval choicesize_prim(lispval x)
{
#if KNO_DEEP_PROFILING
  int n = kno_choice_size(x);
#else
  int n = KNO_CHOICE_SIZE(x);
#endif
  return KNO_INT(n);
}

DEFC_PRIM("pick-one",pickone,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
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

DEFC_PRIM("sample-n",samplen,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID},
	  {"count",kno_fixnum_type,KNO_INT(10)})
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

DEFC_PRIM("pick-n",pickn,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID},
	  {"count",kno_fixnum_type,KNO_VOID},
	  {"offset",kno_fixnum_type,KNO_VOID})
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
  kno_lisp_type xtype = KNO_TYPEOF(x), ytype = KNO_TYPEOF(y);
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

DEFC_PRIM("smallest",smallest_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"elts",kno_any_type,KNO_VOID},
	  {"magnitude",kno_any_type,KNO_VOID})
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
  else if (RARELY (! (KEYFNP(magnitude)) ) )
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

DEFC_PRIM("largest",largest_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"elts",kno_any_type,KNO_VOID},
	  {"magnitude",kno_any_type,KNO_VOID})
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
  else if (RARELY (! (KEYFNP(magnitude)) ) )
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
  kno_function info = KNO_FUNCTION_INFO(f);
  if (info)
    return reduce_functionp(info);
  else if (KNO_APPLICABLEP(f))
    return 1;
  else return 0;
}

static int non_deterministicp(lispval fn)
{
  kno_function info = KNO_FUNCTION_INFO(fn);
  if (info)
    return (FCN_NDOPP(info));
  else if (KNO_APPLICABLEP(fn))
    return 1;
  else return 0;
}

#define VOID_SELECTORP(x) \
  ( (KNO_VOIDP(x)) || (KNO_EMPTYP(x)) || (KNO_FALSEP(x)) )

static int check_selector(lispval selector,lispval obj)
{
  if (VOID_SELECTORP(selector))
    return 1;
  else if (KNO_TYPE_TYPEP(selector))
    return KNO_CHECKTYPE(obj,selector);
  else if (KNO_PREDICATEP(selector)) {
    lispval result = kno_apply(selector,1,&obj);
    if (KNO_ABORTED(result)) return -1;
    else if ( (KNO_FALSEP(result)) || (KNO_EMPTYP(result)) )
      return 0;
    kno_decref(result);
    return 1;}
  else return kno_err("BadSelector","check_selector",NULL,selector);
}

static lispval inner_reduce_choice
(lispval choice,lispval fn,lispval start,lispval keyfn,lispval selector,
 int skiperrs);

DEF_KNOSYM(start); DEF_KNOSYM(keyfn); DEF_KNOSYM(selector);
DEF_KNOSYM(skiperrs);

DEFC_PRIM("reduce-choice",reduce_choice,
	  KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Uses *fn* to reduce the elements of *choice*. "
	  "For each *item* in *choice*, *fn* is called on either "
	  "the item itself or (*keyfn* *item*) and a **reduce-state** "
	  "returning the final result. When provided, *selector* "
	  "determines a subset of arguments to be combined using *fn*. "
	  "*selector* is either a type reference or a predicate function.",
	  {"fn",kno_any_type,KNO_VOID},
	  {"choice",kno_any_type,KNO_VOID},
	  {"start",kno_any_type,KNO_VOID},
	  {"keyfn",kno_any_type,KNO_VOID},
	  {"selector",kno_any_type,KNO_VOID})
static lispval reduce_choice(lispval fn,lispval choice,lispval start,
			     lispval keyfn,lispval selector)
{
  /* Type checking up front */
  if (KNO_CHOICEP(fn)) {
    KNO_DO_CHOICES(f,fn)
      if (! (USUALLY(reduce_operatorp(f))) ) {
	KNO_STOP_DO_CHOICES;
	return kno_type_error("reduce operator","reduce_choice",f);}}
  else if (! (USUALLY(reduce_operatorp(fn))) )
    return kno_type_error("reduce operator","reduce_choice",fn);
  else NO_ELSE;
  int skiperrs = 0;
  int decref_start = 0, decref_keyfn = 0, decref_selector = 0;
  if (KNO_TABLEP(start)) {
    lispval opts = start;

    if (kno_testopt(opts,KNOSYM(skiperrs),KNO_VOID)) skiperrs=1;

    start = kno_getopt(opts,KNOSYM(start),KNO_VOID);
    if (KNO_CONSP(start)) decref_start = 1;

    if ( (VOIDP(keyfn)) || (FALSEP(keyfn)) || (DEFAULTP(keyfn)) ) {
      keyfn = kno_getopt(opts,KNOSYM(keyfn),KNO_VOID);
      if (KNO_CONSP(keyfn)) decref_keyfn = 1;}

    if ( (VOIDP(selector)) || (FALSEP(selector)) || (DEFAULTP(selector)) ) {
      selector = kno_getopt(opts,KNOSYM(selector),KNO_VOID);
      if (KNO_CONSP(selector)) decref_selector = 1;}
  }

  if (USUALLY(KEYFNP(keyfn))) {}
  else if (KNO_CHOICEP(keyfn)) {
    KNO_DO_CHOICES(ok,keyfn)
      if (! (USUALLY(KEYFNP(ok))) ) {
	KNO_STOP_DO_CHOICES;
	return kno_type_error("object key","reduce_choice",ok);}}
  else return kno_type_error("object key","reduce_choice",keyfn);
  if (KNO_VOIDP(selector)) {}
  else if (!( (KNO_TYPE_TYPEP(selector)) || (KNO_PREDICATEP(selector)) ))
    return kno_type_error("object selector","reduce_choice",selector);
  else NO_ELSE;

  lispval reduced = inner_reduce_choice
    (choice,fn,start,keyfn,selector,skiperrs);
  if (decref_keyfn) kno_decref(keyfn);
  if (decref_start) kno_decref(start);
  if (decref_selector) kno_decref(selector);
  return kno_simplify_choice(reduced);
}

static lispval inner_reduce_choice
(lispval choice,lispval fn,lispval start,lispval keyfn,lispval selector,
 int skiperrs)
{
  if (KNO_CHOICEP(fn)) {
    lispval results = KNO_EMPTY;
    KNO_DO_CHOICES(f,fn) {
      lispval reduction =
	inner_reduce_choice(choice,f,start,keyfn,selector,skiperrs);
      if (KNO_ABORTED(reduction)) {
	KNO_STOP_DO_CHOICES;
	kno_decref(results);
	return reduction;}
      else KNO_ADD_TO_CHOICE(results,reduction);}
    return results;}
  else if (KNO_CHOICEP(keyfn)) {
    lispval results = KNO_EMPTY;
    KNO_DO_CHOICES(ok,keyfn) {
      lispval reduction =
	inner_reduce_choice(choice,fn,start,ok,selector,skiperrs);
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
	if (skiperrs) {
	  kno_clear_errors(0);
	  continue;}
	KNO_STOP_DO_CHOICES;
	kno_decref(state);
	return items;}
      else if (KNO_EMPTYP(items)) continue;
      else if (VOIDP(state)) {
	if (VOID_SELECTORP(selector)) {
	  state = items;
	  continue;}
	else if (CHOICEP(items)) {
	  lispval init_state = KNO_EMPTY;
	  KNO_DO_CHOICES(item,items) {
	    if (check_selector(selector,item)) {
	      KNO_ADD_TO_CHOICE(init_state,item);
	      kno_incref(item);}}
	  if (KNO_EMPTYP(init_state)) goto skip;
	  else state = init_state;}
	else if (check_selector(selector,items)) {
	  state=items;
	  continue;}
	else goto skip;}
      else if (EMPTYP(items)) {}
      else if ( (! (nd_reducer) ) && (CHOICEP(items)) ) {
	DO_CHOICES(item,items) {
	  if (!(check_selector(selector,item))) continue;
	  lispval rail[2] = {item , state}, next_state;
	  next_state = kno_apply(fn,2,rail);
	  if (KNO_ABORTED(next_state)) {
	    if (skiperrs) {
	      kno_clear_errors(0);
	      continue;}
	    kno_decref(state);
	    kno_decref(items);
	    KNO_STOP_DO_CHOICES;
	    return next_state;}
	  kno_decref(state);
	  state = next_state;}}
      else if (check_selector(selector,items)) {
	lispval rail[2] = {items , state}, next_state;
	next_state = kno_apply(fn,2,rail);
	if (KNO_ABORTED(next_state)) {
	  if (skiperrs) {
	    kno_clear_errors(0);
	    goto skip;}
	  kno_decref(state);
	  kno_decref(items);
	  KNO_STOP_DO_CHOICES;
	  return next_state;}
	kno_decref(state);
	state = next_state;}
      else NO_ELSE;
    skip:
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

DEFC_PRIM("sorted",sorted_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "(SORTED *choices* *keyfn* *sortfn*)"
	  ", returns a sorted vector of items in *choice*. "
	  "If provided, *keyfn* is specified a property "
	  "(slot, function, table-mapping, etc) is compared "
	  "instead of the object itself. *sortfn* can be "
	  "NORMAL (#f), LEXICAL, or COLLATE to specify "
	  "whether comparison of strings is done "
	  "lexicographically or using the locale's COLLATE "
	  "rules.",
	  {"choices",kno_any_type,KNO_VOID},
	  {"keyfn",kno_any_type,KNO_VOID},
	  {"sortfn_arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("rsorted",rsorted_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "(RSORTED *choices* *keyfn* *sortfn*)"
	  ", returns a sorted vector of items in *choice*. "
	  "If provided, *keyfn* is specified a property "
	  "(slot, function, table-mapping, etc) is compared "
	  "instead of the object itself. *sortfn* can be "
	  "NORMAL (#f), LEXICAL, or COLLATE to specify "
	  "whether comparison of strings is done "
	  "lexicographically or using the locale's COLLATE "
	  "rules.",
	  {"choices",kno_any_type,KNO_VOID},
	  {"keyfn",kno_any_type,KNO_VOID},
	  {"sortfn_arg",kno_any_type,KNO_VOID})
static lispval rsorted_prim(lispval choices,lispval keyfn,
			    lispval sortfn_arg)
{
  enum SORTFN sortfn = get_sortfn(sortfn_arg);
  return sorted_primfn(choices,keyfn,1,sortfn);
}

DEFC_PRIM("lexsorted",lexsorted_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"choices",kno_any_type,KNO_VOID},
	  {"keyfn",kno_any_type,KNO_VOID})
static lispval lexsorted_prim(lispval choices,lispval keyfn)
{
  return sorted_primfn(choices,keyfn,0,COLLATED_SORT);
}

/* Selection */

static ssize_t select_helper(lispval choices,lispval keyfn,
			     size_t k,int maximize,
			     struct KNO_SORT_ENTRY *entries,
			     int just_numkeys)
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
    else if ( (just_numkeys) && (!(NUMBERP(key))) ) {
      kno_decref(key);}
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

static lispval pick_scored(lispval choice,lispval keyfn)
{
  int n = KNO_CHOICE_SIZE(choice);
  struct KNO_CHOICE *out = kno_alloc_choice(n);
  const lispval *read  = KNO_CHOICE_DATA(choice), *limit = read+n;
  lispval *start = (lispval *) KNO_XCHOICE_DATA(out), *write=start;
  while (read<limit) {
    lispval elt = *read++;
    lispval key = _kno_apply_keyfn(elt,keyfn);
    if (KNO_NUMBERP(key)) {
      kno_incref(elt);
      *write++=elt;}
    kno_decref(key);}
  return kno_init_choice(out,write-start,start,KNO_NO_CHOICE_FLAGS);
}

static lispval pick_max_helper(lispval choices,size_t k,
			       lispval keyfn,
			   int just_numkeys)
{
  size_t n = KNO_CHOICE_SIZE(choices);
  if ( (k == 0) || (n==0) )
    return KNO_EMPTY;
  else if (n<=k) {
    if (just_numkeys)
      return pick_scored(choices,keyfn);
    else return kno_incref(choices);}
  else {
    struct KNO_SORT_ENTRY *entries = u8_alloc_n(k,struct KNO_SORT_ENTRY);
    ssize_t count = select_helper(choices,keyfn,k,1,entries,just_numkeys);
    if (count < 0) {
      u8_free(entries);
      return KNO_ERROR;}
    else return entries2choice(entries,count);}
}

DEFC_PRIM("pick-max",pick_max_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Returns the *k* largest items in *choices* where "
	  "the _magnitude_ is determined by *keyfn*. *keyfn* "
	  "can be any applicable object/procedure, a table, "
	  "or an OID or symbol, which is used as a _slotid_. "
	  "If *keyfn* is #f, #default, or not provided, it "
	  "is taken as the identity. Finally, if the keyfn "
	  "is a _vector_ it is a vector of keyfns, which are "
	  "used to return a vector of magnitude objects for "
	  "comparison. If *justnums* is true, only elements "
	  "with numeric magnitudes are considered",
	  {"choices",kno_any_type,KNO_VOID},
	  {"karg",kno_fixnum_type,KNO_VOID},
	  {"keyfn",kno_any_type,KNO_VOID},
	  {"justnums",kno_any_type,KNO_FALSE})
static lispval pick_max_prim(lispval choices,lispval karg,
			     lispval keyfn,lispval justnums)
{
  if ( (VOIDP(keyfn)) || (DEFAULTP(keyfn)) || (FALSEP(keyfn)) ) {}
  else if (!( (KNO_APPLICABLEP(keyfn)) || (KNO_TABLEP(keyfn)) ) )
    return kno_type_error(_("keyfn"),"nmax_prim",keyfn);
  else NO_ELSE;
  if (KNO_UINTP(karg))
    return pick_max_helper(choices,FIX2INT(karg),keyfn,KNO_TRUEP(justnums));
  else return kno_type_error(_("fixnum"),"pick_max_prim",karg);
}

static lispval max_sorted_helper(lispval choices,size_t k,
				 lispval keyfn,
				 int just_numkeys)
{
  size_t n = KNO_CHOICE_SIZE(choices);
  if ( (k == 0) || (n==0) )
    return kno_make_vector(0,NULL);
  else if (n==1) {
    if (just_numkeys) {
      lispval key = _kno_apply_keyfn(choices,keyfn);
      if (!(KNO_NUMBERP(key))) {
	kno_decref(key);
	return kno_make_vector(0,NULL);}
      else {
	kno_incref(choices);
	return kno_make_vector(1,&choices);}}
    else {
      kno_incref(choices);
      return kno_make_vector(1,&choices);}}
  else {
    ssize_t n_entries = (n<k) ? (n) : (k);
    struct KNO_SORT_ENTRY *entries =
      u8_alloc_n(n_entries,struct KNO_SORT_ENTRY);
    ssize_t count = select_helper(choices,keyfn,k,1,entries,just_numkeys);
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
    return vec;}
}


DEFC_PRIM("max/sorted",max_sorted_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Returns the *k* largest items in *choices* sorted "
	  "into a vector where the _magnitude_ is determined "
	  "by *keyfn* and the vector is sorted based on the "
	  "same *keyfn*. The *keyfn* can be any applicable "
	  "object/procedure, a table, or an OID or symbol, "
	  "which is used as a _slotid_. If *keyfn* is #f, "
	  "#default, or not provided, it is taken as the "
	  "identity. Finally, if the keyfn is a _vector_ it "
	  "is a vector of keyfns, which are used to return a "
	  "vector of magnitude objects for comparison. If "
	  "*justnums* is true, only elements with numeric "
	  "magnitudes are considered",
	  {"choices",kno_any_type,KNO_VOID},
	  {"karg",kno_fixnum_type,KNO_VOID},
	  {"keyfn",kno_any_type,KNO_VOID},
	  {"justnums",kno_any_type,KNO_FALSE})
static lispval max_sorted_prim(lispval choices,lispval karg,
			       lispval keyfn,lispval justnums)
{
  if ( (VOIDP(keyfn)) || (DEFAULTP(keyfn)) || (FALSEP(keyfn)) ) {}
  else if (!( (KNO_APPLICABLEP(keyfn)) || (KNO_TABLEP(keyfn)) ) )
    return kno_type_error(_("keyfn"),"nmax2vecprim",keyfn);
  else NO_ELSE;
  if (KNO_UINTP(karg)) {
    size_t k = FIX2INT(karg);
    return max_sorted_helper(choices,k,keyfn,KNO_TRUEP(justnums));}
  else return kno_type_error(_("fixnum"),"nmax_prim",karg);
}

static lispval pick_min_helper(lispval choices,size_t k,lispval keyfn,
			       int just_numkeys)
{
  size_t n = KNO_CHOICE_SIZE(choices);
  if ( (k == 0) || (n == 0) )
    return KNO_EMPTY;
  else if (n<=k) {
    if (just_numkeys)
      return pick_scored(choices,keyfn);
    else return kno_incref(choices);}
  else {
    struct KNO_SORT_ENTRY *entries = u8_alloc_n(k,struct KNO_SORT_ENTRY);
    ssize_t count = select_helper(choices,keyfn,k,0,entries,just_numkeys);
    if (count < 0) {
      u8_free(entries);
      return KNO_ERROR;}
    else return entries2choice(entries,count);}
}

DEFC_PRIM("pick-min",pick_min_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Returns the *k* smallest items in *choices* where "
	  "the _magnitude_ is determined by *keyfn*. *keyfn* "
	  "can be any applicable object/procedure, a table, "
	  "or an OID or symbol, which is used as a _slotid_. "
	  "If *keyfn* is #f, #default, or not provided, it "
	  "is taken as the identity. Finally, if the keyfn "
	  "is a _vector_ it is a vector of keyfns, which are "
	  "used to return a vector of magnitude objects for "
	  "comparison. If *justnums* is true, only elements "
	  "with numeric magnitudes are considered",
	  {"choices",kno_any_type,KNO_VOID},
	  {"karg",kno_fixnum_type,KNO_VOID},
	  {"keyfn",kno_any_type,KNO_VOID},
	  {"justnums",kno_any_type,KNO_FALSE})
static lispval pick_min_prim(lispval choices,lispval karg,
			     lispval keyfn,lispval justnums)
{
  if ( (VOIDP(keyfn)) || (DEFAULTP(keyfn)) || (FALSEP(keyfn)) ) {}
  else if (!( (KNO_APPLICABLEP(keyfn)) || (KNO_TABLEP(keyfn)) ) )
    return kno_type_error(_("keyfn"),"nmin_prim",keyfn);
  else NO_ELSE;
  if (KNO_UINTP(karg))
    return pick_min_helper(choices,FIX2INT(karg),keyfn,KNO_TRUEP(justnums));
  else return kno_type_error(_("fixnum"),"nmax_prim",karg);
}

static lispval min_sorted_helper(lispval choices,size_t k,lispval keyfn,
				 int just_numkeys)
{
  size_t n = KNO_CHOICE_SIZE(choices);
  if ( (k == 0) || (n == 0) )
    return kno_make_vector(0,NULL);
  else if (n==1) {
    if (just_numkeys) {
      lispval key = _kno_apply_keyfn(choices,keyfn);
      if (!(KNO_NUMBERP(key))) {
	kno_decref(key);
	return kno_make_vector(0,NULL);}
      else {
	kno_incref(choices);
	return kno_make_vector(1,&choices);}}
    else {
      kno_incref(choices);
      return kno_make_vector(1,&choices);}}
  else {
    ssize_t n_entries = (n<k) ? (n) : (k);
    struct KNO_SORT_ENTRY *entries =
      u8_alloc_n(n_entries,struct KNO_SORT_ENTRY);
    ssize_t count = select_helper(choices,keyfn,k,0,entries,just_numkeys);
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
    return vec;}
}

DEFC_PRIM("min/sorted",min_sorted_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Returns the *k* smallest items in *choices* "
	  "sorted into a vector where the _magnitude_ is "
	  "determined by *keyfn* and the vector is sorted "
	  "based on the same *keyfn*. The *keyfn* can be any "
	  "applicable object/procedure, a table, or an OID "
	  "or symbol, which is used as a _slotid_. If "
	  "*keyfn* is #f, #default, or not provided, it is "
	  "taken as the identity. Finally, if the keyfn is a "
	  "_vector_ it is a vector of keyfns, which are used "
	  "to return a vector of magnitude objects for "
	  "comparison. If *justnums* is true, only elements "
	  "with numeric magnitudes are considered",
	  {"choices",kno_any_type,KNO_VOID},
	  {"karg",kno_fixnum_type,KNO_VOID},
	  {"keyfn",kno_any_type,KNO_VOID},
	  {"justnums",kno_any_type,KNO_FALSE})
static lispval min_sorted_prim(lispval choices,lispval karg,
			       lispval keyfn,lispval justnums)
{
  if ( (VOIDP(keyfn)) || (DEFAULTP(keyfn)) || (FALSEP(keyfn)) ) {}
  else if (!( (KNO_APPLICABLEP(keyfn)) || (KNO_TABLEP(keyfn)) ) )
    return kno_type_error(_("keyfn"),"min_sorted_prim",keyfn);
  else NO_ELSE;
  if (KNO_UINTP(karg)) {
    size_t k = FIX2INT(karg);
    return min_sorted_helper(choices,k,keyfn,KNO_TRUEP(justnums));}
  else return kno_type_error(_("fixnum"),"min_sorted_prim",karg);
}

/* GETRANGE */

DEFC_PRIM("getrange",getrange_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg1",kno_any_type,KNO_VOID},
	  {"endval",kno_any_type,KNO_VOID})
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

DEFC_PRIM("pick>",pick_gt_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"items",kno_any_type,KNO_VOID},
	  {"num",kno_any_type,KNO_INT(0)},
	  {"checktype",kno_any_type,KNO_FALSE})
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

DEFC_PRIM("picktypes",picktypes_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Selects elements in *items* which satisfy any of *types*",
	  {"items",kno_any_type,KNO_VOID},
	  {"types",kno_any_type,KNO_VOID})
static lispval picktypes_prim(lispval items,lispval types)
{
  if (KNO_EMPTYP(items)) return KNO_EMPTY;
  else if (KNO_EMPTYP(types)) return KNO_EMPTY;
  else if (KNO_CHOICEP(items)) {
    int n_elts = KNO_CHOICE_SIZE(items);
    kno_choice newch = kno_alloc_choice(n_elts);
    const lispval *elts = KNO_CHOICE_ELTS(items);
    lispval *newelts = (lispval *) KNO_CHOICE_DATA(newch);
    const lispval *read=elts, *limit=elts+n_elts;;
    lispval *write=newelts;
    const lispval *typevec =
      (KNO_CHOICEP(types)) ? (KNO_CHOICE_ELTS(types)) : (&types),
      *typevec_limit = typevec+KNO_CHOICE_SIZE(types);
    while (read<limit) {
      lispval item = *read++;
      const lispval *scantype = typevec;
      while (scantype<typevec_limit) {
	if (KNO_CHECKTYPE(item,types)) {
	  *write++=item; kno_incref(item);
	  break;}
	scantype++;}}
    return kno_init_choice(newch,write-newelts,newelts,
			   KNO_CHOICE_REALLOC|KNO_CHOICE_FREEDATA);}
  else if (KNO_CHOICEP(types)) {
    KNO_DO_CHOICES(type,types) {
      if ( (KNO_CHECKTYPE(items,type)) )
	return kno_incref(items);}
    return KNO_EMPTY;}
  else if (KNO_CHECKTYPE(items,types))
    return kno_incref(items);
  else return KNO_EMPTY;
}

DEFC_PRIM("skiptypes",skiptypes_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Selects elements in *items* which _don't_ satisfy any of *types*",
	  {"items",kno_any_type,KNO_VOID},
	  {"types",kno_any_type,KNO_VOID})
static lispval skiptypes_prim(lispval items,lispval types)
{
  if (KNO_EMPTYP(items)) return KNO_EMPTY;
  else if (KNO_EMPTYP(types)) return kno_incref(items);
  else if (KNO_CHOICEP(items)) {
    int n_elts = KNO_CHOICE_SIZE(items);
    kno_choice newch = kno_alloc_choice(n_elts);
    const lispval *elts = KNO_CHOICE_ELTS(items);
    lispval *newelts = (lispval *) KNO_CHOICE_DATA(newch);
    const lispval *read=elts, *limit=elts+n_elts;;
    lispval *write=newelts;
    const lispval *typevec =
      (KNO_CHOICEP(types)) ? (KNO_CHOICE_ELTS(types)) : (&types),
      *typevec_limit = typevec+KNO_CHOICE_SIZE(types);
    while (read<limit) {
      lispval item = *read++;
      const lispval *scantype = typevec;
      while (scantype<typevec_limit) {
	if (!(KNO_CHECKTYPE(item,types))) {
	  *write++=item; kno_incref(item);
	  break;}
	scantype++;}}
    return kno_init_choice(newch,write-newelts,newelts,
			   KNO_CHOICE_REALLOC|KNO_CHOICE_FREEDATA);}
  else if (KNO_CHOICEP(types)) {
    KNO_DO_CHOICES(type,types) {
      if (! (KNO_CHECKTYPE(items,type)) )
	return KNO_EMPTY;}
    return kno_incref(items);}
  else if (KNO_CHECKTYPE(items,types))
    return KNO_EMPTY;
  else return kno_incref(items);
}

DEFC_PRIM("pickoids",pick_oids_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"items",kno_any_type,KNO_VOID})
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

DEFC_PRIM("picksyms",pick_syms_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"items",kno_any_type,KNO_VOID})
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

DEFC_PRIM("pickstrings",pick_strings_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"items",kno_any_type,KNO_VOID})
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

DEFC_PRIM("pickvecs",pick_vecs_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"items",kno_any_type,KNO_VOID})
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

DEFC_PRIM("pickpairs",pick_pairs_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"items",kno_any_type,KNO_VOID})
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

DEFC_PRIM("picknums",pick_nums_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"items",kno_any_type,KNO_VOID})
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

DEFC_PRIM("pickmaps",pick_maps_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**",
	  {"items",kno_any_type,KNO_VOID})
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

  kno_def_evalfn(kno_scheme_module,"DO-CHOICES",dochoices_evalfn,
		 "*undocumented*");
  kno_defalias(kno_scheme_module,"DO∀","DO-CHOICES");
  kno_def_evalfn(kno_scheme_module,"FOR-CHOICES",forchoices_evalfn,
		 "*undocumented*");
  kno_defalias(kno_scheme_module,"FOR∀","FOR-CHOICES");
  kno_def_evalfn(kno_scheme_module,"TRY-CHOICES",trychoices_evalfn,
		 "*undocumented*");
  kno_defalias(kno_scheme_module,"TRY∀","TRY-CHOICES");
  kno_def_evalfn(kno_scheme_module,"FILTER-CHOICES",filterchoices_evalfn,
		 "*undocumented*");
  kno_defalias(kno_scheme_module,"?∀","FILTER-CHOICES");

  kno_def_evalfn(kno_scheme_module,"DO-SUBSETS",dosubsets_evalfn,
		 "*undocumented*");

  link_local_cprims();

  kno_def_evalfn(kno_scheme_module,"TRY",try_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"IFEXISTS",ifexists_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"WHENEXISTS",whenexists_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"QCHOICE?",qchoicep_evalfn,
		 "*undocumented*");

  lexical_symbol = kno_intern("lexical");
  lexci_symbol = kno_intern("lexical/ci");
  collate_symbol = kno_intern("collate");

}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_CPRIM("pickmaps",pick_maps_prim,1,scheme_module);
  KNO_LINK_CPRIM("picknums",pick_nums_prim,1,scheme_module);
  KNO_LINK_CPRIM("pickpairs",pick_pairs_prim,1,scheme_module);
  KNO_LINK_CPRIM("pickvecs",pick_vecs_prim,1,scheme_module);
  KNO_LINK_CPRIM("pickstrings",pick_strings_prim,1,scheme_module);
  KNO_LINK_CPRIM("picksyms",pick_syms_prim,1,scheme_module);
  KNO_LINK_CPRIM("pickoids",pick_oids_prim,1,scheme_module);
  KNO_LINK_CPRIM("pick>",pick_gt_prim,3,scheme_module);
  KNO_LINK_CPRIM("getrange",getrange_prim,2,scheme_module);
  KNO_LINK_CPRIM("min/sorted",min_sorted_prim,4,scheme_module);
  KNO_LINK_CPRIM("pick-min",pick_min_prim,4,scheme_module);
  KNO_LINK_CPRIM("max/sorted",max_sorted_prim,4,scheme_module);
  KNO_LINK_CPRIM("pick-max",pick_max_prim,4,scheme_module);
  KNO_LINK_CPRIM("lexsorted",lexsorted_prim,2,scheme_module);
  KNO_LINK_CPRIM("rsorted",rsorted_prim,3,scheme_module);
  KNO_LINK_CPRIM("sorted",sorted_prim,3,scheme_module);
  KNO_LINK_CPRIM("reduce-choice",reduce_choice,5,scheme_module);
  KNO_LINK_CPRIM("largest",largest_prim,2,scheme_module);
  KNO_LINK_CPRIM("smallest",smallest_prim,2,scheme_module);
  KNO_LINK_CPRIM("pick-n",pickn,3,scheme_module);
  KNO_LINK_CPRIM("sample-n",samplen,2,scheme_module);
  KNO_LINK_CPRIM("pick-one",pickone,1,scheme_module);
  KNO_LINK_CPRIM("choice-size",choicesize_prim,1,scheme_module);
  KNO_LINK_CPRIM("choice->list",choice2list,1,scheme_module);
  KNO_LINK_CPRIM("choice->vector",choice2vector,2,scheme_module);
  KNO_LINK_CPRIMN("difference",difference_lexpr,scheme_module);
  KNO_LINK_CPRIMN("intersection",intersection_lexpr,scheme_module);
  KNO_LINK_CPRIMN("union",union_lexpr,scheme_module);
  KNO_LINK_CPRIMN("forall/skiperrs",forall_skiperrs,scheme_module);
  KNO_LINK_CPRIMN("forall",forall_lexpr,scheme_module);
  KNO_LINK_CPRIMN("sometrue/skiperrs",sometrue_skiperrs,scheme_module);
  KNO_LINK_CPRIMN("sometrue",sometrue_lexpr,scheme_module);
  KNO_LINK_CPRIMN("exists/skiperrs",exists_skiperrs,scheme_module);
  KNO_LINK_CPRIMN("exists",exists_lexpr,scheme_module);
  KNO_LINK_CPRIM("simplify",simplify,1,scheme_module);
  KNO_LINK_CPRIM("choice-min",choice_min,2,scheme_module);
  KNO_LINK_CPRIM("choice-max",choice_max,2,scheme_module);
  KNO_LINK_CPRIM("singleton",singleton,1,scheme_module);
  KNO_LINK_CPRIM("amb?",ambiguousp,1,scheme_module);
  KNO_LINK_CPRIM("unique?",singletonp,1,scheme_module);
  KNO_LINK_CPRIM("exists?",existsp,1,scheme_module);
  KNO_LINK_CPRIM("satisfied?",satisfiedp,1,scheme_module);
  KNO_LINK_CPRIM("empty?",emptyp,1,scheme_module);
  KNO_LINK_CPRIMN("qchoicex",qchoicex_prim,scheme_module);
  KNO_LINK_CPRIMN("qchoice",qchoice_prim,scheme_module);
  KNO_LINK_CPRIMN("choice",choice_prim,scheme_module);
  KNO_LINK_CPRIM("fail",fail_prim,0,scheme_module);

  KNO_LINK_CPRIM("picktypes",picktypes_prim,2,scheme_module);
  KNO_LINK_CPRIM("skiptypes",skiptypes_prim,2,scheme_module);

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
  KNO_LINK_ALIAS("nmax",pick_max_prim,scheme_module);
  KNO_LINK_ALIAS("nmax->vector",max_sorted_prim,scheme_module);
  KNO_LINK_ALIAS("nmin",pick_min_prim,scheme_module);
  KNO_LINK_ALIAS("nmin->vector",min_sorted_prim,scheme_module);

}
