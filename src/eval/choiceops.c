/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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
#include "framerd/storage.h"
#include "framerd/numbers.h"
#include "framerd/frames.h"
#include "framerd/sorting.h"

#include "eval_internals.h"

/* Choice iteration */

static lispval parse_control_spec
(lispval expr,lispval *iter_var,lispval *count_var,
 fd_lexenv env,fd_stack _stack)
{
  lispval control_expr = fd_get_arg(expr,1);
  if (VOIDP(control_expr))
    return fd_err(fd_TooFewExpressions,NULL,NULL,expr);
  else if (SYMBOLP(control_expr)) {
    lispval values = fast_eval(control_expr,env);
    if (FD_ABORTED(values)) {
      *iter_var = VOID;
      return values;}
    *iter_var = control_expr;
    *count_var = VOID;
    return fd_simplify_choice(values);}
  else {
    lispval var = fd_get_arg(control_expr,0), ivar = fd_get_arg(control_expr,2);
    lispval val_expr = fd_get_arg(control_expr,1), val;
    if (VOIDP(control_expr))
      return fd_err(fd_TooFewExpressions,NULL,NULL,expr);
    else if (VOIDP(val_expr))
      return fd_err(fd_TooFewExpressions,NULL,NULL,control_expr);
    else if (!(SYMBOLP(var)))
      return fd_err(fd_SyntaxError,
                    _("identifier is not a symbol"),NULL,control_expr);
    else if (!((VOIDP(ivar)) || (SYMBOLP(ivar))))
      return fd_err(fd_SyntaxError,
                    _("identifier is not a symbol"),NULL,control_expr);
    val = fast_eval(val_expr,env);
    if (FD_ABORTED(val)) {
      *iter_var = VOID;
      return val;}
    *iter_var = var;
    if (count_var) *count_var = ivar;
    return fd_simplify_choice(val);}
}

/* This iterates over a set of choices, evaluating its body for each value.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns VOID. */
static lispval dochoices_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval var, count_var, choices=
    parse_control_spec(expr,&var,&count_var,env,_stack);
  if (FD_ABORTED(var)) return var;
  else if (FD_ABORTED(choices)) return choices;
  else if (EMPTYP(choices))
    return VOID;
  FD_ADD_TO_CHOICE(_stack->stack_vals,choices);
  INIT_STACK_ENV(_stack,dochoices,env,2);
  dochoices_vars[0]=var;
  if (SYMBOLP(count_var))
    dochoices_vars[1]=count_var;
  else dochoices_bindings.schema_length=1;
  int i = 0; DO_CHOICES(elt,choices) {
    dochoices_vals[0]=fd_incref(elt);
    dochoices_vals[1]=FD_INT(i);
    {lispval steps = fd_get_body(expr,2);
      FD_DOLIST(step,steps) {
        lispval val = fast_eval(step,dochoices);
        if (FD_BROKEP(val))
          _return FD_VOID;
        else if (FD_ABORTED(val)) {
          _return val;}
        else fd_decref(val);}}
    reset_env(dochoices);
    fd_decref(dochoices_vals[0]);
    dochoices_vals[0]=VOID;
    fd_decref(dochoices_vals[1]);
    dochoices_vals[1]=VOID;
    i++;}
  _return VOID;
}

/* This iterates over a set of choices, evaluating its body for each value.
   It returns the first non-empty result of evaluating the body.
   Note that this treats a non-choice as a choice of one element.
   It returns VOID. */
static lispval trychoices_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval var, count_var, choices=
    parse_control_spec(expr,&var,&count_var,env,_stack);
  if (FD_ABORTED(var)) return var;
  else if (FD_ABORTED(choices)) return choices;
  else if (EMPTYP(choices))
    return EMPTY;
  FD_ADD_TO_CHOICE(_stack->stack_vals,choices);
  INIT_STACK_ENV(_stack,trychoices,env,2);
  trychoices_vars[0]=var;
  if (SYMBOLP(count_var))
    trychoices_vars[1]=count_var;
  else trychoices_bindings.schema_length=1;
  int i = 0; DO_CHOICES(elt,choices) {
    lispval val = VOID;
    trychoices_vals[0]=fd_incref(elt);
    trychoices_vals[1]=FD_INT(i);
    lispval steps = fd_get_body(expr,2);
    FD_DOLIST(step,steps) {
      fd_decref(val);
      val = fast_eval(step,trychoices);
      if (FD_BROKEP(val))
        _return EMPTY;
      else if (FD_ABORTED(val))
        _return val;}
    reset_env(trychoices);
    fd_decref(trychoices_vals[0]);
    trychoices_vals[0]=VOID;
    fd_decref(trychoices_vals[1]);
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
static lispval forchoices_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval results = EMPTY;
  lispval var, count_var, choices=
    parse_control_spec(expr,&var,&count_var,env,_stack);
  if (FD_ABORTED(var)) return var;
  else if (FD_ABORTED(choices))
    return choices;
  else if (EMPTYP(choices))
    return EMPTY;
  FD_ADD_TO_CHOICE(_stack->stack_vals,choices);
  INIT_STACK_ENV(_stack,forchoices,env,2);
  forchoices_vars[0]=var;
  if (SYMBOLP(count_var))
    forchoices_vars[1]=count_var;
  else forchoices_bindings.schema_length=1;
  int i = 0; DO_CHOICES(elt,choices) {
    lispval val = VOID;
    forchoices_vals[0]=fd_incref(elt);
    forchoices_vals[1]=FD_INT(i);
    lispval steps = fd_get_body(expr,2);
    FD_DOLIST(step,steps) {
      fd_decref(val);
      val = fast_eval(step,forchoices);
      if (FD_BROKEP(val)) {
        lispval result = fd_simplify_choice(results);
        _return result;}
      else if (FD_ABORTED(val))
        _return val;}
    CHOICE_ADD(results,val);
    reset_env(forchoices);
    fd_decref(forchoices_vals[0]);
    forchoices_vals[0]=VOID;
    fd_decref(forchoices_vals[1]);
    forchoices_vals[1]=VOID;
    i++;}
  _return fd_simplify_choice(results);
}

/* This iterates over a set of choices, evaluating its third subexpression for each value, and
    accumulating those values for which the body returns true.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns the subset of values which pass the body. */
static lispval filterchoices_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval results = EMPTY;
  lispval var, count_var, choices=
    parse_control_spec(expr,&var,&count_var,env,_stack);
  if (FD_ABORTED(var)) return var;
  else if (FD_ABORTED(choices))
    return choices;
  else if (EMPTYP(choices))
    return EMPTY;
  FD_ADD_TO_CHOICE(_stack->stack_vals,choices);
  INIT_STACK_ENV(_stack,filterchoices,env,2);
  filterchoices_vars[0]=var;
  if (SYMBOLP(count_var))
    filterchoices_vars[1]=count_var;
  else filterchoices_bindings.schema_length=1;
  lispval steps = fd_get_body(expr,2);
  int i = 0; DO_CHOICES(elt,choices) {
    lispval val = VOID;
    filterchoices_vals[0]=fd_incref(elt);
    filterchoices_vals[1]=FD_INT(i);
    FD_DOLIST(step,steps) {
      fd_decref(val);
      val = fast_eval(step,filterchoices);
      if (FD_BROKEP(val)) {
        lispval result = fd_simplify_choice(results);
        _return result;}
      else if (FD_ABORTED(val))
        _return val;}
    if (!(FALSEP(val))) {
      CHOICE_ADD(results,elt);
      fd_incref(elt);}
    fd_decref(val);
    reset_env(filterchoices);
    fd_decref(filterchoices_vals[0]);
    filterchoices_vals[0]=VOID;
    fd_decref(filterchoices_vals[1]);
    filterchoices_vals[1]=VOID;
    i++;}
  _return fd_simplify_choice(results);
}

/* This iterates over the subsets of a choice and is useful for
    dividing a large dataset into smaller chunks for processing.
   It tries to save effort by not incref'ing elements when creating the subset.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   This returns VOID.  */
static lispval dosubsets_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval choices, count_var, var;
  lispval control_spec = fd_get_arg(expr,1);
  lispval bsize; long long blocksize;
  if (!((PAIRP(control_spec)) &&
        (SYMBOLP(FD_CAR(control_spec))) &&
        (PAIRP(FD_CDR(control_spec))) &&
        (PAIRP(FD_CDR(FD_CDR(control_spec))))))
    return fd_err(fd_SyntaxError,"dosubsets_evalfn",NULL,VOID);
  var = FD_CAR(control_spec);
  count_var = fd_get_arg(control_spec,3);
  if (!((VOIDP(count_var)) || (SYMBOLP(count_var))))
    return fd_err(fd_SyntaxError,"dosubsets_evalfn",NULL,VOID);
  bsize = fd_eval(FD_CADR(FD_CDR(control_spec)),env);
  if (FD_ABORTED(bsize)) return bsize;
  else if (!(FIXNUMP(bsize)))
    return fd_type_error("fixnum","dosubsets_evalfn",bsize);
  else blocksize = FIX2INT(bsize);
  choices = fd_eval(FD_CADR(control_spec),env);
  if (FD_ABORTED(choices)) return choices;
  else {FD_SIMPLIFY_CHOICE(choices);}
  if (EMPTYP(choices)) return VOID;
  FD_ADD_TO_CHOICE(_stack->stack_vals,choices);
  INIT_STACK_ENV(_stack,dosubsets,env,2);
  dosubsets_vars[0]=var;
  if (SYMBOLP(count_var))
    dosubsets_vars[1]=count_var;
  else dosubsets_bindings.schema_length=1;
  int i = 0, n = FD_CHOICE_SIZE(choices), n_blocks = 1+n/blocksize;
  int all_atomicp = ((CHOICEP(choices)) ?
                     (FD_ATOMIC_CHOICEP(choices)) : (0));
  const lispval *data=
    ((CHOICEP(choices))?(FD_CHOICE_DATA(choices)):(NULL));
  if ((n%blocksize)==0) n_blocks--;
  while (i<n_blocks) {
    lispval block;
    if ((CHOICEP(choices)) && (n_blocks>1)) {
      const lispval *read = &(data[i*blocksize]), *limit = read+blocksize;
      struct FD_CHOICE *subset = fd_alloc_choice(blocksize); int atomicp = 1;
      lispval *write = ((lispval *)(FD_XCHOICE_DATA(subset)));
      if (limit>(data+n)) limit = data+n;
      if (all_atomicp)
        while (read<limit) *write++= *read++;
      else while (read<limit) {
          lispval v = *read++;
          if (CONSP(v)) {
            atomicp=0; fd_incref(v);}
          *write++=v;}
      {FD_INIT_XCHOICE(subset,write-FD_XCHOICE_DATA(subset),atomicp);}
      block = (lispval)subset;}
    else block = fd_incref(choices);
    dosubsets_vals[0]=block;
    dosubsets_vals[1]=FD_INT(i);
    {lispval body = fd_get_body(expr,2);
      FD_DOLIST(subexpr,body) {
        lispval val = fast_eval(subexpr,dosubsets);
        if (FD_BROKEP(val)) {
          _return VOID;}
        else if (FD_ABORTED(val))
          _return val;
        else fd_decref(val);}}
    reset_env(dosubsets);
    fd_decref(dosubsets_vals[0]);
    dosubsets_vals[0]=VOID;
    fd_decref(dosubsets_vals[1]);
    dosubsets_vals[1]=VOID;
    i++;}
  _return VOID;
}

/* SMALLEST and LARGEST */

static int compare_lisp(lispval x,lispval y)
{
  fd_ptr_type xtype = FD_PTR_TYPE(x), ytype = FD_PTR_TYPE(y);
  if (xtype == ytype)
    switch (xtype) {
    case fd_fixnum_type: {
      long long xval = FIX2INT(x), yval = FIX2INT(y);
      if (xval < yval) return -1;
      else if (xval > yval) return 1;
      else return 0;}
    case fd_oid_type: {
      FD_OID xval = FD_OID_ADDR(x), yval = FD_OID_ADDR(y);
      return FD_OID_COMPARE(xval,yval);}
    default:
      return FD_FULL_COMPARE(x,y);}
  else if (xtype<ytype) return -1;
  else return 1;
}

static lispval getmagnitude(lispval val,lispval magfn)
{
  if (VOIDP(magfn)) return fd_incref(val);
  else {
    fd_ptr_type magtype = FD_PTR_TYPE(magfn);
    switch (magtype) {
    case fd_hashtable_type: case fd_slotmap_type: case fd_schemap_type:
      return fd_get(magfn,val,EMPTY);
    default:
      if (FD_APPLICABLEP(magfn))
        return fd_finish_call(fd_dapply(magfn,1,&val));
      else return fd_get(val,magfn,EMPTY);}}
}

static lispval smallest_evalfn(lispval elts,lispval magnitude)
{
  lispval top = EMPTY, top_score = VOID;
  DO_CHOICES(elt,elts) {
    lispval score = getmagnitude(elt,magnitude);
    if (FD_ABORTED(score)) return score;
    else if (VOIDP(top_score))
      if (EMPTYP(score)) {}
      else {
        top = fd_incref(elt);
        top_score = score;}
    else if (EMPTYP(score)) {}
    else {
      int comparison = compare_lisp(score,top_score);
      if (comparison>0) {}
      else if (comparison == 0) {
        fd_incref(elt);
        CHOICE_ADD(top,elt);
        fd_decref(score);}
      else {
        fd_decref(top); fd_decref(top_score);
        top = fd_incref(elt);
        top_score = score;}}}
  fd_decref(top_score);
  return top;
}

static lispval largest_evalfn(lispval elts,lispval magnitude)
{
  lispval top = EMPTY, top_score = VOID;
  DO_CHOICES(elt,elts) {
    lispval score = getmagnitude(elt,magnitude);
    if (FD_ABORTED(score)) return score;
    else if (VOIDP(top_score))
      if (EMPTYP(score)) {}
      else {
        top = fd_incref(elt);
        top_score = score;}
    else if (EMPTYP(score)) {}
    else {
      int comparison = compare_lisp(score,top_score);
      if (comparison<0) {}
      else if (comparison == 0) {
        fd_incref(elt);
        CHOICE_ADD(top,elt);
        fd_decref(score);}
      else {
        fd_decref(top); fd_decref(top_score);
        top = fd_incref(elt);
        top_score = score;}}}
  fd_decref(top_score);
  return top;
}

/* Choice functions */

static lispval fail_prim()
{
  return EMPTY;
}

static lispval choice_prim(int n,lispval *args)
{
  int i = 0; lispval results = EMPTY;
  while (i < n) {
    lispval arg = args[i++]; fd_incref(arg);
    CHOICE_ADD(results,arg);}
  return fd_simplify_choice(results);
}

static lispval qchoice_prim(int n,lispval *args)
{
  int i = 0; lispval results = EMPTY, presults;
  while (i < n) {
    lispval arg = args[i++]; fd_incref(arg);
    CHOICE_ADD(results,arg);}
  presults = fd_simplify_choice(results);
  if ((CHOICEP(presults)) || (EMPTYP(presults)))
    return fd_init_qchoice(NULL,presults);
  else return presults;
}

static lispval qchoicex_prim(int n,lispval *args)
{
  int i = 0; lispval results = EMPTY, presults;
  while (i < n) {
    lispval arg = args[i++]; fd_incref(arg);
    CHOICE_ADD(results,arg);}
  presults = fd_simplify_choice(results);
  if (EMPTYP(presults))
    return presults;
  else if (CHOICEP(presults))
    return fd_init_qchoice(NULL,presults);
  else return presults;
}

/* TRY */

static lispval try_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval value = EMPTY;
  lispval clauses = fd_get_body(expr,1);
  FD_DOLIST(clause,clauses) {
    int ipe_state = fd_ipeval_status();
    fd_decref(value);
    value = fd_eval(clause,env);
    if (FD_ABORTED(value))
      return value;
    else if (VOIDP(value)) {
      fd_seterr(fd_VoidArgument,"try_evalfn",NULL,clause);
      return FD_ERROR;}
    else if (!(EMPTYP(value)))
      return value;
    else if (fd_ipeval_status()!=ipe_state)
      return value;}
  return value;
}

/* IFEXISTS */

static lispval ifexists_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval value_expr = fd_get_arg(expr,1);
  lispval value = EMPTY;
  if (VOIDP(value_expr))
    return fd_err(fd_SyntaxError,"ifexists_evalfn",NULL,expr);
  else if (!(NILP(FD_CDR(FD_CDR(expr)))))
    return fd_err(fd_SyntaxError,"ifexists_evalfn",NULL,expr);
  else value = fd_eval(value_expr,env);
  if (FD_ABORTED(value))
    return value;
  if (EMPTYP(value))
    return VOID;
  else return value;
}

/* Predicates */

static lispval emptyp(lispval x)
{
  if (EMPTYP(x)) return FD_TRUE; else return FD_FALSE;
}

static lispval satisfiedp(lispval x)
{
  if (EMPTYP(x)) return FD_FALSE;
  else if (FALSEP(x)) return FD_FALSE;
  else return FD_TRUE;
}

static lispval existsp(lispval x)
{
  if (EMPTYP(x)) return FD_FALSE; else return FD_TRUE;
}

static lispval singletonp(lispval x)
{
  if (CHOICEP(x))
    return FD_FALSE;
  else if (EMPTYP(x))
    return FD_FALSE;
  else if (FD_PRECHOICEP(x)) {
    lispval simple = fd_make_simple_choice(x);
    int not_single = ( (EMPTYP(simple)) || (CHOICEP(simple)) );
    fd_decref(simple);
    if (not_single) return FD_FALSE;
    else return FD_TRUE;}
  else return FD_TRUE;
}

static lispval ambiguousp(lispval x) /* TODO: Wasted effort around here */
{
  if (EMPTYP(x))
    return FD_FALSE;
  else if (CHOICEP(x))
    return FD_TRUE;
  else if (FD_PRECHOICEP(x)) {
    lispval simple = fd_make_simple_choice(x);
    int ambig=FD_CHOICEP(simple);
    fd_decref(simple);
    if (ambig)
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

static lispval singleton(lispval x)
{
  if (EMPTYP(x)) return x;
  else if (CHOICEP(x))
    return EMPTY;
  else if (FD_PRECHOICEP(x)) {
    lispval simple=fd_make_simple_choice(x);
    if (FD_CHOICEP(simple)) {
      fd_decref(simple);
      return EMPTY;}
    else return simple;}
  else return fd_incref(x);
}

static lispval choice_max(lispval x,lispval lim)
{
  if (EMPTYP(x)) return x;
  else if (CHOICEP(x)) {
    int max_size = fd_getint(lim);
    if (FD_CHOICE_SIZE(x)>max_size)
      return EMPTY;
    else return fd_incref(x);}
  else if (FD_PRECHOICEP(x)) {
    lispval simple = fd_make_simple_choice(x);
    if (CHOICEP(simple)) {
      int max_size = fd_getint(lim);
      if (FD_CHOICE_SIZE(simple)>max_size) {
        fd_decref(simple);
        return EMPTY;}
      else return simple;}
    else return simple;}
  else return fd_incref(x);
}

static lispval simplify(lispval x)
{
  return fd_make_simple_choice(x);
}

static lispval qchoicep_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  /* This is an evalfn because application often reduces qchoices to
     choices. */
  if (!((PAIRP(expr)) && (PAIRP(FD_CDR(expr)))))
    return fd_err(fd_SyntaxError,"qchoice_evalfn",NULL,expr);
  else {
    lispval val = fd_eval(FD_CADR(expr),env);
    if (FD_ABORTED(val)) return val;
    if (QCHOICEP(val)) {
      fd_decref(val);
      return FD_TRUE;}
    else {
      fd_decref(val);
      return FD_FALSE;}}
}

/* The exists operation */

static int test_exists(struct FD_FUNCTION *fn,
                       int i,int n,lispval *nd_args,
                       lispval *d_args);

static lispval exists_lexpr(int n,lispval *nd_args)
{
  lispval *d_args;
  int i = 0; while (i<n)
    if (EMPTYP(nd_args[i])) return FD_FALSE;
    else i++;
  d_args = u8_alloc_n((n-1),lispval);
  {DO_CHOICES(fcn,nd_args[0])
     if (FD_APPLICABLEP(fcn)) {
       struct FD_FUNCTION *f = (fd_function)fcn;
       int retval = test_exists(f,0,n-1,nd_args+1,d_args);
       if (retval<0) return FD_ERROR;
       else if (retval) {
         u8_free(d_args);
         return FD_TRUE;}}
     else {
       u8_free(d_args);
       return fd_type_error(_("function"),"exists_lexpr",nd_args[0]);}
  u8_free(d_args);}
  return FD_FALSE;
}

static lispval sometrue_lexpr(int n,lispval *nd_args)
{
  if (n==1)
    if (EMPTYP(nd_args[0]))
      return FD_FALSE;
    else if (FD_FALSEP(nd_args[0]))
      return FD_FALSE;
    else return FD_TRUE;
  else return exists_lexpr(n,nd_args);
}

static int test_exists(struct FD_FUNCTION *fn,int i,int n,
                       lispval *nd_args,lispval *d_args)
{
  if (i == n) {
    lispval val = fd_finish_call(fd_dapply((lispval)fn,n,d_args));
    if ((FALSEP(val)) || (EMPTYP(val))) {
      return 0;}
    else if (FD_ABORTED(val)) {
      return fd_interr(val);}
    fd_decref(val);
    return 1;}
  else if ((CHOICEP(nd_args[i])) || (PRECHOICEP(nd_args[i]))) {
    DO_CHOICES(v,nd_args[i]) {
      d_args[i]=v; int retval = test_exists(fn,i+1,n,nd_args,d_args);
      if (retval != 0) {
        FD_STOP_DO_CHOICES;
        return retval;}}
    return 0;}
  else {
    d_args[i]=nd_args[i];
    return test_exists(fn,i+1,n,nd_args,d_args);}
}

static int test_forall
  (struct FD_FUNCTION *fn,int i,int n,lispval *nd_args,lispval *d_args);

static lispval whenexists_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval to_eval = fd_get_arg(expr,1), value;
  if (VOIDP(to_eval))
    return fd_err(fd_SyntaxError,"whenexists_evalfn",NULL,expr);
  else value = fd_eval(to_eval,env);
  if (FD_ABORTED(value)) return VOID;
  else if (EMPTYP(value)) return VOID;
  else return value;
}

static lispval forall_lexpr(int n,lispval *nd_args)
{
  lispval *d_args;
  int i = 0; while (i<n)
    if (EMPTYP(nd_args[i])) return FD_TRUE;
    else i++;
  d_args = u8_alloc_n(n-1,lispval);
  {DO_CHOICES(fcn,nd_args[0])
     if (FD_APPLICABLEP(fcn)) {
       struct FD_FUNCTION *f = FD_XFUNCTION(fcn);
       int retval = test_forall(f,0,n-1,nd_args+1,d_args);
       if (retval<0) return FD_ERROR;
       else if (retval) {
         u8_free(d_args);
         return FD_TRUE;}}
     else {
       u8_free(d_args);
       return fd_type_error(_("function"),"exists_lexpr",nd_args[0]);}
  u8_free(d_args);}
  return FD_FALSE;
}

static int test_forall(struct FD_FUNCTION *fn,int i,int n,lispval *nd_args,lispval *d_args)
{
  if (i == n) {
    lispval val = fd_finish_call(fd_dapply((lispval)fn,n,d_args));
    if (FALSEP(val))
      return 0;
    else if (EMPTYP(val))
      return 1;
    else if (FD_ABORTED(val))
      return fd_interr(val);
    fd_decref(val);
    return 1;}
  else if ((CHOICEP(nd_args[i])) || (PRECHOICEP(nd_args[i]))) {
    DO_CHOICES(v,nd_args[i]) {
      int retval;
      d_args[i]=v;
      retval = test_forall(fn,i+1,n,nd_args,d_args);
      if (retval==0) return retval;}
    return 1;}
  else {
    d_args[i]=nd_args[i];
    return test_forall(fn,i+1,n,nd_args,d_args);}
}

/* Set operations */

static lispval union_lexpr(int n,lispval *args)
{
  return fd_simplify_choice(fd_union(args,n));
}
static lispval intersection_lexpr(int n,lispval *args)
{
  return fd_simplify_choice(fd_intersection(args,n));
}
static lispval difference_lexpr(int n,lispval *args)
{
  lispval result = fd_incref(args[0]); int i = 1;
  if (EMPTYP(result))
    return result;
  else while (i<n) {
    lispval new = fd_difference(result,args[i]);
    if (EMPTYP(new)) {
      fd_decref(result);
      return EMPTY;}
    else {fd_decref(result);
      result = new;}
    i++;}
  return fd_simplify_choice(result);
}

/* Prechoice elements */

static lispval choicevec_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval sub_expr = fd_get_arg(expr,1);
  if (FD_VOIDP(sub_expr))
    return fd_err(fd_SyntaxError,"choicevec_evalfn",NULL,expr);
  else {
    lispval result = fd_stack_eval(sub_expr,env,_stack,0);
    if (FD_ABORTED(result))
      return result;
    else if (FD_CHOICEP(result)) {
      struct FD_CHOICE *ch = (fd_choice) result;
      const lispval *elts = FD_XCHOICE_ELTS(ch);
      int size = ch->choice_size;
      lispval vector = fd_make_vector(size,(lispval *)elts);
      if (ch->choice_isatomic)
        return vector;
      else {
        fd_incref_vec((lispval *)elts,size);
        return vector;}}
    else if (FD_PRECHOICEP(result)) {
      struct FD_PRECHOICE *pch = (fd_prechoice) result;
      int n = pch->prechoice_size;
      lispval vec = fd_make_vector(n,NULL);
      lispval *write = FD_VECTOR_ELTS(vec);
      lispval *scan = pch->prechoice_data, *limit = pch->prechoice_limit;
      while (scan<limit) {
        lispval add = *scan++;
        if (FD_CHOICEP(add)) {
          int n_adds = FD_CHOICE_SIZE(add);
          const lispval *add_elts = FD_CHOICE_ELTS(add);
          memmove(write,add_elts,n_adds*LISPVAL_LEN);
          if (! (FD_ATOMIC_CHOICEP(add)) )
            fd_incref_vec((lispval *)add_elts,n);}
        else if (EMPTYP(add)) {}
        else {
          *write++=add;
          fd_incref(add);}}
      return vec;}
    else {
      lispval data[1]={result};
      return fd_make_vector(1,data);}}
}

/* Conversion functions */

static lispval choice2vector(lispval x,lispval sortspec)
{
  fd_compare_flags flags = fd_get_compare_flags(sortspec);
  if (EMPTYP(x))
    return fd_empty_vector(0);
  else if (PRECHOICEP(x)) {
    lispval normal = fd_make_simple_choice(x);
    lispval result = choice2vector(normal,sortspec);
    fd_decref(normal);
    return result;}
  else if (CHOICEP(x)) {
    int i = 0, n = FD_CHOICE_SIZE(x);
    struct FD_CHOICE *ch = (fd_choice)x;
    lispval vector = fd_make_vector(n,NULL);
    lispval *vector_elts = VEC_DATA(vector);
    const lispval *choice_elts = FD_XCHOICE_DATA(ch);
    if (FD_XCHOICE_ATOMICP(ch))
      memcpy(vector_elts,choice_elts,LISPVEC_BYTELEN(n));
    else while (i<n) {
        vector_elts[i]=fd_incref(choice_elts[i]);
        i++;}
    if ((VOIDP(sortspec))||(FALSEP(sortspec)))
      return vector;
    else {
      lispval_sort(vector_elts,n,flags);
      return vector;}}
  else {
    fd_incref(x);
    return fd_make_vector(1,&x);}
}

static lispval choice2list(lispval x)
{
  lispval lst = NIL;
  DO_CHOICES(elt,x) lst = fd_conspair(fd_incref(elt),lst);
  return lst;
}

static lispval get_part(lispval x,lispval part)
{
  if (VOIDP(part)) return fd_incref(x);
  else if (FD_APPLICABLEP(part))
    return fd_apply(part,1,&x);
  else if ((TABLEP(part)) && (!(OIDP(part))))
    return fd_get(part,x,EMPTY);
  else if (OIDP(x))
    return fd_frame_get(x,part);
  else if (TABLEP(x))
    return fd_get(x,part,EMPTY);
  else return EMPTY;
}

#define FD_PARTP(x) \
  ((SYMBOLP(x)) || (OIDP(x)) || (VOIDP(x)) || \
   (TABLEP(x)) || (FD_APPLICABLEP(x)))

static lispval reduce_choice(lispval fn,lispval choice,lispval start,lispval part)
{
  if ((FD_APPLICABLEP(fn)) && ((VOIDP(part)) || (FD_PARTP(part)))) {
    lispval state = fd_incref(start);
    DO_CHOICES(each,choice) {
      lispval items = get_part(each,part);
      if (FD_ABORTED(items)) {
        FD_STOP_DO_CHOICES;
        fd_decref(state);
        return items;}
      else if (VOIDP(state))
        state = fd_incref(items);
      else if (EMPTYP(items)) {}
      else if (AMBIGP(items)) {
        DO_CHOICES(item,items) {
          lispval rail[2], next_state;
          rail[0]=item; rail[1]=state;
          next_state = fd_apply(fn,2,rail);
          if (FD_ABORTED(next_state)) {
            fd_decref(state);
            fd_decref(items);
            FD_STOP_DO_CHOICES;
            return next_state;}
          fd_decref(state);
          state = next_state;}}
      else {
        lispval item = items;
        lispval rail[2], next_state;
        rail[0]=item; rail[1]=state;
        next_state = fd_apply(fn,2,rail);
        if (FD_ABORTED(next_state)) {
          fd_decref(state);
          fd_decref(items);
          FD_STOP_DO_CHOICES;
          return next_state;}
        fd_decref(state);
        state = next_state;}
      fd_decref(items);}
    return state;}
  else if (!(FD_APPLICABLEP(fn)))
    return fd_type_error(_("function"),"reduce_choice",fn);
  else return fd_type_error(_("part"),"reduce_choice",part);
}

static lispval apply_map(lispval fn,lispval val)
{
  if ((VOIDP(fn)) || (FALSEP(fn)))
    return fd_incref(val);
  else if (FD_APPLICABLEP(fn))
    return fd_apply(fn,1,&val);
  else if (TABLEP(fn))
    return fd_get(fn,val,VOID);
  else return fd_type_error(_("map function"),"xreduce_choice",fn);
}

static lispval xreduce_choice
  (lispval choice,lispval reducefn,lispval mapfn,lispval start)
{
  if (CHOICEP(reducefn)) {
    lispval result = EMPTY;
    DO_CHOICES(rfn,reducefn) {
      lispval v = xreduce_choice(choice,rfn,mapfn,start);
      if (FD_ABORTED(v)) {
        fd_decref(result); FD_STOP_DO_CHOICES;
        return result;}
      else {CHOICE_ADD(result,v);}}
    return result;}
  else if (CHOICEP(mapfn)) {
    lispval result = EMPTY;
    DO_CHOICES(mfn,mapfn) {
      lispval v = xreduce_choice(choice,reducefn,mfn,start);
      if (FD_ABORTED(v)) {
        fd_decref(result); FD_STOP_DO_CHOICES;
        return result;}
      else {CHOICE_ADD(result,v);}}
    return result;}
  else if (FD_APPLICABLEP(reducefn)) {
    lispval state = ((VOIDP(start))?(start):(apply_map(mapfn,start)));
    DO_CHOICES(item,choice)
      if (FD_ABORTED(state)) {
        FD_STOP_DO_CHOICES;
        return state;}
      else if (VOIDP(state))
        state = apply_map(mapfn,item);
      else {
        lispval item_val = apply_map(mapfn,item);
        if (!((VOIDP(item_val)) || (EMPTYP(item_val)))) {
          lispval rail[2], next_state;
          rail[0]=item_val; rail[1]=state;
          next_state = fd_apply(reducefn,2,rail);
          fd_decref(item_val); fd_decref(state);
          state = next_state;}}
    return state;}
  else return fd_type_error(_("function"),"xreduce_choice",reducefn);
}

static lispval choicesize_prim(lispval x)
{
  int n = FD_CHOICE_SIZE(x);
  return FD_INT(n);
}

static lispval pickone(lispval x)
{
  lispval normal = fd_make_simple_choice(x), chosen = EMPTY;
  if (CHOICEP(normal)) {
    int n = FD_CHOICE_SIZE(normal);
    if (n) {
      int i = u8_random(n);
      const lispval *data = FD_CHOICE_DATA(normal);
      chosen = data[i];
      fd_incref(data[i]); fd_decref(normal);
      return chosen;}
    else return EMPTY;}
  else return normal;
}

static lispval samplen(lispval x,lispval count)
{
  if (EMPTYP(x))
    return x;
  else if (FIXNUMP(count)) {
    int howmany = fd_getint(count);
    if (howmany<=0)
      return EMPTY;
    else if (! ( (FD_CHOICEP(x)) || (FD_PRECHOICEP(x)) ) )
      return fd_incref(x);
    else {
      lispval normal = fd_make_simple_choice(x);
      lispval results = EMPTY;
      int n = FD_CHOICE_SIZE(normal);
      if (n<=howmany)
        return normal;
      else {
        unsigned char *used=u8_zalloc_n(n,unsigned char);
        const lispval *data = FD_CHOICE_DATA(normal);
        int j = 0; while (j<howmany) {
          int i = u8_random(n);
          if (!(used[i])) {
            lispval elt=data[i]; fd_incref(elt);
            CHOICE_ADD(results,data[i]);
            used[i]=1;
            j++;}}
        fd_decref(normal);
        u8_free(used);
        return fd_simplify_choice(results);}}}
  else return fd_type_error("integer","samplen",count);
}

static lispval pickn(lispval x,lispval count,lispval offset)
{
  if (FIXNUMP(count)) {
    int howmany = fd_getint(count);
    if (howmany == 0)
      return EMPTY;
    else if (x == EMPTY)
      return EMPTY;
    else if ( (FD_CHOICEP(x)) || (FD_PRECHOICEP(x))) {
      lispval normal = fd_make_simple_choice(x);
      int start=0, n = FD_CHOICE_SIZE(normal);
      if (n<=howmany)
        return normal;
      else if (FD_UINTP(offset)) {
        start = FIX2INT(offset);
        if ((n-start)<howmany) howmany = n-start;}
      else if ((FIXNUMP(offset))||(FD_BIGINTP(offset))) {
        fd_decref(normal);
        return fd_type_error("small fixnum","pickn",offset);}
      else start = u8_random(n-howmany);
      if (howmany<=0) {
        fd_decref(normal);
        return EMPTY;}
      else if (n == 1) {
        struct FD_CHOICE *base=
          (fd_consptr(struct FD_CHOICE *,normal,fd_choice_type));
        const lispval *read = FD_XCHOICE_DATA(base);
        lispval one = read[0];
        return fd_incref(one);}
      else if (n) {
        struct FD_CHOICE *base=
          (fd_consptr(struct FD_CHOICE *,normal,fd_choice_type));
        struct FD_CHOICE *result = fd_alloc_choice(howmany);
        const lispval *read = FD_XCHOICE_DATA(base)+start;
        lispval *write = (lispval *)FD_XCHOICE_DATA(result);
        if (FD_XCHOICE_ATOMICP(base)) {
          memcpy(write,read,LISPVEC_BYTELEN(howmany));
          fd_decref(normal);
          return fd_init_choice(result,howmany,NULL,FD_CHOICE_ISATOMIC);}
        else {
          int atomicp = 1; const lispval *readlim = read+howmany;
          while (read<readlim) {
            lispval v = *read++;
            if (ATOMICP(v)) *write++=v;
            else {atomicp = 0; fd_incref(v); *write++=v;}}
          fd_decref(normal);
          return fd_init_choice(result,howmany,NULL,
                                ((atomicp)?(FD_CHOICE_ISATOMIC):(0)));}}
      else return EMPTY;}
    else return fd_incref(x);}
  else return fd_type_error("integer","topn",count);
}

/* Sorting */

enum SORTFN {
  NORMAL_SORT, LEXICAL_SORT, LEXICAL_CI_SORT, COLLATED_SORT, POINTER_SORT };

static lispval lexical_symbol, lexci_symbol, collate_symbol, pointer_symbol;

static enum SORTFN get_sortfn(lispval arg)
{
  if ( (FD_VOIDP(arg)) || (FD_FALSEP(arg)) || (FD_DEFAULTP(arg)) )
    return NORMAL_SORT;
  else if ( (FD_TRUEP(arg)) || (arg == lexci_symbol) )
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
    return fd_empty_vector(0);
  else if (CHOICEP(choices)) {
    int i = 0, n = FD_CHOICE_SIZE(choices), j = 0;
    lispval *vecdata = u8_big_alloc_n(n,lispval);
    struct FD_SORT_ENTRY *entries = u8_big_alloc_n(n,struct FD_SORT_ENTRY);
    DO_CHOICES(elt,choices) {
      lispval key=_fd_apply_keyfn(elt,keyfn);
      if (FD_ABORTED(key)) {
        int j = 0; while (j<i) {
          fd_decref(entries[j].sortkey);
          j++;}
        u8_free(entries);
        u8_free(vecdata);
        return key;}
      entries[i].sortval = elt;
      entries[i].sortkey = key;
      i++;}
    switch (sortfn) {
    case NORMAL_SORT:
      qsort(entries,n,sizeof(struct FD_SORT_ENTRY),_fd_sort_helper);
      break;
    case LEXICAL_SORT:
      qsort(entries,n,sizeof(struct FD_SORT_ENTRY),_fd_lexsort_helper);
      break;
    case LEXICAL_CI_SORT:
      qsort(entries,n,sizeof(struct FD_SORT_ENTRY),_fd_lexsort_ci_helper);
      break;
    case POINTER_SORT:
      qsort(entries,n,sizeof(struct FD_SORT_ENTRY),_fd_pointer_sort_helper);
      break;
    case COLLATED_SORT:
      qsort(entries,n,sizeof(struct FD_SORT_ENTRY),_fd_collate_helper);
      break;}
    i = 0; j = n-1; if (reverse) while (i < n) {
      fd_decref(entries[i].sortkey);
      vecdata[j]=fd_incref(entries[i].sortval);
      i++; j--;}
    else while (i < n) {
      fd_decref(entries[i].sortkey);
      vecdata[i]=fd_incref(entries[i].sortval);
      i++;}
    u8_big_free(entries);
    return fd_cons_vector(NULL,n,1,vecdata);}
  else {
    lispval *vec = u8_alloc_n(1,lispval);
    vec[0]=fd_incref(choices);
    return fd_wrap_vector(1,vec);}
}

static lispval sorted_prim(lispval choices,lispval keyfn,
                           lispval sortfn_arg)
{
  enum SORTFN sortfn;
  if ( (keyfn == lexical_symbol) || (keyfn == pointer_symbol) ||
       (keyfn == collate_symbol) || (keyfn == lexci_symbol) ) {
    sortfn = get_sortfn(keyfn);
    keyfn  = FD_VOID;}
  else sortfn = get_sortfn(sortfn_arg);
  return sorted_primfn(choices,keyfn,0,sortfn);
}

static lispval rsorted_prim(lispval choices,lispval keyfn,
                            lispval sortfn_arg)
{
  enum SORTFN sortfn = get_sortfn(sortfn_arg);
  return sorted_primfn(choices,keyfn,1,sortfn);
}

static lispval lexsorted_prim(lispval choices,lispval keyfn)
{
  return sorted_primfn(choices,keyfn,0,COLLATED_SORT);
}

/* Selection */

static ssize_t select_helper(lispval choices,lispval keyfn,
                             size_t k,int maximize,
                             struct FD_SORT_ENTRY *entries)
{
#define BETTERP(x) ((maximize)?((x)>0):((x)<0))
#define IS_BETTER(x,y) (BETTERP(FD_QCOMPARE((x),(y))))
  lispval worst = VOID;
  size_t worst_off = (maximize)?(0):(k-1);
  int k_len = 0;
  DO_CHOICES(elt,choices) {
    lispval key=_fd_apply_keyfn(elt,keyfn);
    if (FD_ABORTED(key)) {
      int j = 0; while (j<k_len) {
        fd_decref(entries[k_len].sortkey);
        j++;}
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
      qsort(entries,k,sizeof(struct FD_SORT_ENTRY),_fd_sort_helper);
      worst = entries[worst_off].sortkey;
      if (IS_BETTER(key,worst)) {
        fd_decref(worst);
        entries[worst_off].sortval = elt;
        entries[worst_off].sortkey = key;
        /* This could be done faster by either by just finding
           where to insert it, either by iterating O(n) or binary
           search O(log n). */
        qsort(entries,k,sizeof(struct FD_SORT_ENTRY),_fd_sort_helper);
        worst = entries[worst_off].sortkey;}
      else fd_decref(key);}}
  return k_len;
#undef BETTERP
#undef IS_BETTER
}

static lispval entries2choice(struct FD_SORT_ENTRY *entries,int n)
{
  if (n<0) {
    u8_free(entries);
    return FD_ERROR_VALUE;}
  else if (n==0) {
    if (entries) u8_free(entries);
    return EMPTY;}
  else {
    lispval results = EMPTY;
    int i = 0; while (i<n) {
      lispval elt = entries[i].sortval;
      fd_decref(entries[i].sortkey); fd_incref(elt);
      CHOICE_ADD(results,elt);
      i++;}
    u8_free(entries);
    return fd_simplify_choice(results);}
}

static lispval nmax_prim(lispval choices,lispval karg,lispval keyfn)
{
  if (FD_UINTP(karg)) {
    if (FD_PRECHOICEP(choices)) {
      lispval norm = fd_make_simple_choice(choices);
      lispval result = nmax_prim(norm,karg,keyfn);
      fd_decref(norm);
      return result;}
    else {
      size_t k = FIX2INT(karg);
      size_t n = FD_CHOICE_SIZE(choices);
      if (n==1)
        return fd_make_vector(1,&choices);
      else {
        struct FD_SORT_ENTRY *entries = u8_alloc_n(k,struct FD_SORT_ENTRY);
        ssize_t count = select_helper(choices,keyfn,k,1,entries);
        return entries2choice(entries,count);}}}
  else return fd_type_error(_("fixnum"),"nmax_prim",karg);
}

static lispval nmax2vec_prim(lispval choices,lispval karg,lispval keyfn)
{
  if (FD_UINTP(karg)) {
    if (FD_PRECHOICEP(choices)) {
      lispval norm = fd_make_simple_choice(choices);
      lispval result = nmax_prim(norm,karg,keyfn);
      fd_decref(norm);
      return result;}
    else {
      size_t k = FIX2INT(karg);
      size_t n = FD_CHOICE_SIZE(choices);
      if (n==0) return fd_make_vector(0,NULL);
      else if (n==1) {
        fd_incref(choices);
        return fd_make_vector(0,&choices);}
      else {
        ssize_t n_entries = (n<k) ? (n) : (k);
        struct FD_SORT_ENTRY *entries =
          u8_alloc_n(n_entries,struct FD_SORT_ENTRY);
        ssize_t count = select_helper(choices,keyfn,k,1,entries);
        lispval vec = fd_make_vector(count,NULL);
        int i = 0; while (i<count) {
          lispval elt = entries[i].sortval;
          int vec_off = count-1-i;
          fd_incref(elt);
          fd_decref(entries[i].sortkey);
          FD_VECTOR_SET(vec,vec_off,elt);
          i++;}
        u8_free(entries);
        return vec;}}}
  else return fd_type_error(_("fixnum"),"nmax_prim",karg);
}

static lispval nmin_prim(lispval choices,lispval karg,lispval keyfn)
{
  if (FD_UINTP(karg)) {
    if (FD_PRECHOICEP(choices)) {
      lispval norm = fd_make_simple_choice(choices);
      lispval result = nmax_prim(norm,karg,keyfn);
      fd_decref(norm);
      return result;}
    else {
      size_t k = FIX2INT(karg);
      size_t n = FD_CHOICE_SIZE(choices);
      if (n<=k) return fd_incref(choices);
      else {
        struct FD_SORT_ENTRY *entries = u8_alloc_n(k,struct FD_SORT_ENTRY);
        ssize_t count = select_helper(choices,keyfn,k,0,entries);
        return entries2choice(entries,count);}}}
  else return fd_type_error(_("fixnum"),"nmax_prim",karg);
}

static lispval nmin2vec_prim(lispval choices,lispval karg,lispval keyfn)
{
  if (FD_UINTP(karg)) {
    if (FD_PRECHOICEP(choices)) {
      lispval norm = fd_make_simple_choice(choices);
      lispval result = nmin_prim(norm,karg,keyfn);
      fd_decref(norm);
      return result;}
    else {
      size_t k = FIX2INT(karg);
      size_t n = FD_CHOICE_SIZE(choices);
      if (n==0) return fd_make_vector(0,NULL);
      else if (n==1) {
        fd_incref(choices);
        return fd_make_vector(0,&choices);}
      else {
        ssize_t n_entries = (n<k) ? (n) : (k);
        struct FD_SORT_ENTRY *entries =
          u8_alloc_n(n_entries,struct FD_SORT_ENTRY);
        ssize_t count = select_helper(choices,keyfn,k,0,entries);
        lispval vec = fd_make_vector(count,NULL);
        int i = 0; while (i<count) {
          fd_decref(entries[i].sortkey);
          fd_incref(entries[i].sortval);
          FD_VECTOR_SET(vec,i,entries[i].sortval);
          i++;}
        u8_free(entries);
        return vec;}}}
    else return fd_type_error(_("fixnum"),"nmax_prim",karg);
}

/* GETRANGE */

static lispval getrange_prim(lispval arg1,lispval endval)
{
  long long start, end; lispval results = EMPTY;
  if (VOIDP(endval))
    if (FIXNUMP(arg1)) {
      start = 0; end = FIX2INT(arg1);}
    else return fd_type_error(_("fixnum"),"getrange_prim",arg1);
  else if ((FIXNUMP(arg1)) && (FIXNUMP(endval))) {
    start = FIX2INT(arg1); end = FIX2INT(endval);}
  else if (FIXNUMP(endval))
    return fd_type_error(_("fixnum"),"getrange_prim",arg1);
  else return fd_type_error(_("fixnum"),"getrange_prim",endval);
  if (start>end) {int tmp = start; start = end; end = tmp;}
  while (start<end) {
    CHOICE_ADD(results,FD_INT(start)); start++;}
  return results;
}

static lispval pick_gt_prim(lispval items,lispval num,lispval checktype)
{
  lispval lower_bound = VOID;
  DO_CHOICES(n,num) {
    if (!(NUMBERP(n)))
      return fd_type_error("number","pick_gt_prim",n);
    else if (VOIDP(lower_bound)) lower_bound = n;
    else if (fd_numcompare(n,lower_bound)<0) lower_bound = n;}
  {
    lispval results = EMPTY;
    DO_CHOICES(item,items)
      if (NUMBERP(item))
        if (fd_numcompare(item,lower_bound)>0) {
          fd_incref(item);
          CHOICE_ADD(results,item);}
        else {}
      else if (checktype == FD_TRUE)
        return fd_type_error("number","pick_gt_prim",item);
      else {}
    return results;
  }
}

static lispval pick_oids_prim(lispval items)
{
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if (OIDP(item)) {
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

static lispval pick_syms_prim(lispval items)
{
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if (SYMBOLP(item)) {
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

static lispval pick_strings_prim(lispval items)
{
  /* I don't think we need to worry about getting a PRECHOICE here. */
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if (STRINGP(item)) {
      fd_incref(item);
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

static lispval pick_vecs_prim(lispval items)
{
  /* I don't think we need to worry about getting a PRECHOICE here. */
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if (VECTORP(item)) {
      fd_incref(item);
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

static lispval pick_pairs_prim(lispval items)
{
  /* I don't think we need to worry about getting a PRECHOICE here. */
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if (PAIRP(item)) {
      fd_incref(item);
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

static lispval pick_nums_prim(lispval items)
{
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if (FIXNUMP(item)) {
      CHOICE_ADD(results,item);}
    else if (NUMBERP(item)) {
      fd_incref(item);
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

static lispval pick_maps_prim(lispval items)
{
  lispval results = EMPTY; int no_change = 1;
  DO_CHOICES(item,items)
    if ( (FD_SLOTMAPP(item)) || (FD_SCHEMAPP(item)) ) {
      fd_incref(item);
      CHOICE_ADD(results,item);}
    else no_change = 0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

/* Initialize functions */

FD_EXPORT void fd_init_choicefns_c()
{
  u8_register_source_file(_FILEINFO);

  fd_def_evalfn(fd_scheme_module,"DO-CHOICES","",dochoices_evalfn);
  fd_defalias(fd_scheme_module,"DO∀","DO-CHOICES");
  fd_def_evalfn(fd_scheme_module,"FOR-CHOICES","",forchoices_evalfn);
  fd_defalias(fd_scheme_module,"FOR∀","FOR-CHOICES");
  fd_def_evalfn(fd_scheme_module,"TRY-CHOICES","",trychoices_evalfn);
  fd_defalias(fd_scheme_module,"TRY∀","TRY-CHOICES");
  fd_def_evalfn(fd_scheme_module,"FILTER-CHOICES","",filterchoices_evalfn);
  fd_defalias(fd_scheme_module,"?∀","FILTER-CHOICES");

  fd_def_evalfn(fd_scheme_module,"CHOICEVEC","",choicevec_evalfn);

  fd_def_evalfn(fd_scheme_module,"DO-SUBSETS","",dosubsets_evalfn);

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("CHOICE",choice_prim,0)));
  fd_idefn(fd_scheme_module,fd_make_cprim0("FAIL",fail_prim));
  {
    lispval qc_prim=
      fd_make_ndprim(fd_make_cprimn("QCHOICE",qchoice_prim,0));
    fd_idefn(fd_scheme_module,qc_prim);
    fd_store(fd_scheme_module,fd_intern("QC"),qc_prim);
  }

  {
    lispval qcx_prim=
      fd_make_ndprim(fd_make_cprimn("QCHOICEX",qchoicex_prim,0));
    fd_idefn(fd_scheme_module,qcx_prim);
    fd_store(fd_scheme_module,fd_intern("QCX"),qcx_prim);
  }

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("INTERSECTION",intersection_lexpr,1)));
  fd_defalias(fd_scheme_module,"∩","INTERSECTION");
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("UNION",union_lexpr,1)));
  fd_defalias(fd_scheme_module,"∪","UNION");
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("DIFFERENCE",difference_lexpr,1)));
  fd_defalias(fd_scheme_module,"∖","DIFFERENCE");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("SMALLEST",smallest_evalfn,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("LARGEST",largest_evalfn,1)));

  fd_def_evalfn(fd_scheme_module,"TRY","",try_evalfn);

  {
    lispval empty_prim=
      fd_make_ndprim(fd_make_cprim1("EMPTY?",emptyp,1));
    fd_idefn(fd_scheme_module,empty_prim);
    fd_store(fd_scheme_module,fd_intern("FAIL?"),empty_prim);
    fd_store(fd_scheme_module,fd_intern("∄"),empty_prim);
  }

  fd_def_evalfn(fd_scheme_module,"IFEXISTS","",ifexists_evalfn);

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("SATISFIED?",satisfiedp,1)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("EXISTS?",existsp,1)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("CHOICE-SIZE",choicesize_prim,1)));
  fd_defalias(fd_scheme_module,"Ω","CHOICE-SIZE");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("EXISTS",exists_lexpr,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("SOMETRUE",sometrue_lexpr,1)));
  fd_defalias(fd_scheme_module,"∃","SOMETRUE");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("FORALL",forall_lexpr,1)));
  fd_defalias(fd_scheme_module,"∀","FORALL");

  fd_def_evalfn(fd_scheme_module,"WHENEXISTS","",whenexists_evalfn);

  {
    lispval unique_prim=
      fd_make_ndprim(fd_make_cprim1("UNIQUE?",singletonp,1));
    fd_idefn(fd_scheme_module,unique_prim);
    fd_store(fd_scheme_module,fd_intern("SINGLETON?"),unique_prim);
    fd_store(fd_scheme_module,fd_intern("SOLE?"),unique_prim);}

  fd_def_evalfn(fd_scheme_module,"QCHOICE?","",qchoicep_evalfn);

  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim1("AMB?",ambiguousp,1)));
  fd_defalias(fd_scheme_module,"AMBIGUOUS?","AMB?");
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("SINGLETON",singleton,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2x("CHOICE-MAX",choice_max,2,
                                          -1,VOID,fd_fixnum_type,VOID)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("SIMPLIFY",simplify,1)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("CHOICE->VECTOR",choice2vector,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("CHOICE->LIST",choice2list,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim4("REDUCE-CHOICE",reduce_choice,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim4("XREDUCE",xreduce_choice,3)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICK-ONE",pickone,1)));

  fd_idefn3(fd_scheme_module,"SORTED",sorted_prim,FD_NEEDS_1_ARG|FD_NDCALL,
            "(SORTED *choice* *keyfn* *sortfn*), returns a sorted vector "
            "of items in *choice*. If provided, *keyfn* is specified "
            "a property (slot, function, table-mapping, etc) is compared "
            "instead of the object itself. "
            "*sortfn* can be NORMAL (#f), LEXICAL, or COLLATE "
            "to specify whether comparison of strings is done lexicographically "
            "or using the locale's COLLATE rules.",
            -1,FD_VOID,-1,FD_VOID,-1,FD_VOID);
  fd_idefn3(fd_scheme_module,"RSORTED",rsorted_prim,FD_NEEDS_1_ARG|FD_NDCALL,
            "(RSORTED *choice* *keyfn* *sortfn*), returns a sorted vector "
            "of items in *choice*. If provided, *keyfn* is specified "
            "a property (slot, function, table-mapping, etc) is compared "
            "instead of the object itself. "
            "*sortfn* can be NORMAL (#f), LEXICAL, or COLLATE "
            "to specify whether comparison of strings is done lexicographically "
            "or using the locale's COLLATE rules.",
            -1,FD_VOID,-1,FD_VOID,-1,FD_VOID);

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("LEXSORTED",lexsorted_prim,1)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("PICK>",pick_gt_prim,1,
                                          -1,VOID,
                                          -1,FD_INT(0),
                                          -1,FD_FALSE)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKOIDS",pick_oids_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKNUMS",pick_nums_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKMAPS",pick_maps_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKSTRINGS",pick_strings_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKSYMS",pick_syms_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKPAIRS",pick_pairs_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKVECS",pick_vecs_prim,1)));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2("GETRANGE",getrange_prim,1));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2x
                          ("SAMPLE-N",samplen,1,-1,VOID,
                           fd_fixnum_type,FD_SHORT2DTYPE(10))));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("PICK-N",pickn,2,
                                          -1,VOID,fd_fixnum_type,VOID,
                                          fd_fixnum_type,VOID)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("PICK-MAX",nmax_prim,2,
                                          -1,VOID,fd_fixnum_type,VOID,
                                          -1,VOID)));
  fd_defalias(fd_scheme_module,"NMAX","PICK-MAX");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("MAX/SORTED",nmax2vec_prim,2,
                                          -1,VOID,fd_fixnum_type,VOID,
                                          -1,VOID)));
  fd_defalias(fd_scheme_module,"NMAX->VECTOR","MAX/SORTED");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("PICK-MIN",nmin_prim,2,
                                          -1,VOID,fd_fixnum_type,VOID,
                                          -1,VOID)));
  fd_defalias(fd_scheme_module,"NMIN","PICK-MIN");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("MIN/SORTED",nmin2vec_prim,2,
                                          -1,VOID,fd_fixnum_type,VOID,
                                          -1,VOID)));
  fd_defalias(fd_scheme_module,"NMIN->VECTOR","MIN/SORTED");


  lexical_symbol = fd_intern("LEXICAL");
  lexci_symbol = fd_intern("LEXICAL/CI");
  collate_symbol = fd_intern("COLLATE");

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
