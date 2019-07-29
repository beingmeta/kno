/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#define U8_LOGLEVEL (u8_getloglevel(testops_loglevel))

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_TABLES (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_FCNIDS (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_STACKS (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_LEXENV (!(KNO_AVOID_CHOICES))

#define KNO_PROVIDE_FASTEVAL (!(KNO_AVOID_CHOICES))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/support.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/dtproc.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/ports.h"
#include "kno/dtcall.h"
#include "kno/history.h"
#include "kno/ffi.h"
#include "kno/cprims.h"

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>


static lispval err_testfail = KNO_TRUE;
static lispval err_symbol;
static lispval void_symbol;

static u8_condition TestFailed=_("Test failed");
static u8_condition TestError=_("Test error");

static int testops_loglevel = LOG_WARN;

/* Test functions */

#define LEAVE_EXSTACK 0

static lispval failed_tests = KNO_EMPTY_CHOICE;
static u8_mutex failed_tests_lock;

double test_float_precision = 0.00001;

static int test_match(lispval x,lispval y)
{
  if (x == y)
    return 1;
  else if (KNO_EQUALP(x,y))
    return 1;
  else if ( (KNO_FLONUMP(x)) && (KNO_FLONUMP(y)) ) {
    double diff = KNO_FLONUM(x) - KNO_FLONUM(y);
    if (diff < 0) diff = -diff;
    if (diff > test_float_precision)
      return 0;
    else return 1;}
  else return 0;
}

static void add_failed_test(lispval object)
{
  u8_lock_mutex(&failed_tests_lock);
  KNO_ADD_TO_CHOICE(failed_tests,object);
  u8_unlock_mutex(&failed_tests_lock);
}

static int config_failed_tests_set(lispval var,lispval val,void *data)
{
  lispval *ptr = data;
  u8_lock_mutex(&failed_tests_lock);
  lispval cur = *ptr;
  if (KNO_FALSEP(val)) {
    *ptr = KNO_EMPTY;
    kno_decref(cur);}
  else {
    kno_incref(val);
    KNO_ADD_TO_CHOICE(cur,val);
    *ptr = cur;}
  u8_unlock_mutex(&failed_tests_lock);
  return 1;
}

#define USE_EQUAL_TEST 0
#define USE_PREDICATE_TEST 1

static u8_string get_testid(lispval fn,int n,kno_argvec args)
{
  if ( (n>0) && (KNO_SYMBOLP(args[0])) )
    return u8_strdup(KNO_SYMBOL_NAME(args[0]));
  U8_STATIC_OUTPUT(testid,1024);
  u8_puts(&testid,"Testing ");
  kno_unparse(&testid,fn);
  int i = 0; while (i<n) {
    lispval arg = args[i];
    u8_putc(&testid,' ');
    if (KNO_FUNCTIONP(arg)) {
      lispval f = (KNO_FCNIDP(arg)) ? (kno_fcnid_ref(arg)) : (arg);
      if (KNO_FUNCTIONP(f)) {
	kno_function fcn = (kno_function) f;
	if (fcn->fcn_name)
	  u8_puts(&testid,fcn->fcn_name);
	else kno_unparse(&testid,arg);}
      else kno_unparse(&testid,f);}
    else kno_unparse(&testid,arg);
    i++;}
  u8_string id = u8_strdup(testid.u8_outbuf);
  u8_close_output(&testid);
  return id;
}

DEFPRIM("applytest",applytest,KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	"`(APPLYTEST *arg0* *arg1* *args...*)` **undocumented**");
static lispval applytest(int n,kno_argvec args)
{
  lispval expected = args[0], return_value;
  lispval fn = args[1], predicate = KNO_VOID, predicate_arg = KNO_VOID;
  int err = ! ( (KNO_VOIDP(err_testfail)) || (KNO_FALSEP(err_testfail)) );
  const lispval *argstart = args+2;
  int n_args = n-2;
  if (KNO_FUNCTIONP(expected)) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(expected);
    if ( (f) && (f->fcn_arity == 1) )
      predicate=expected;
    else if ( (f) &&
	      ( (f->fcn_arity > 1) || (f->fcn_arity < 0) ) &&
	      (f->fcn_min_arity < 3) &&
	      (n >= 3) ) {
      predicate=expected;
      predicate_arg = args[1];
      fn = args[2];
      argstart = args+3;
      n_args = n - 3;}
    else return kno_err("Bad applytest result predicate","applytest/predicate",
			NULL,expected);}
  else NO_ELSE;
  u8_string testid = get_testid(fn,n_args,argstart);
  lispval result = (KNO_APPLICABLEP(fn)) ? (kno_apply(fn,n_args,argstart)) :
    (n_args==0) ? (kno_incref(fn)) :
    (n_args==1) ? (kno_incref(argstart[0])) :
    (kno_err("BadApplyTest","applytest",NULL,VOID));
  if ( (KNO_ABORTP(result)) && (expected == err_symbol) ) {
    u8_exception ex = u8_erreify();
    u8_logf(LOG_INFO,"Tests/ExpectedError","%m (from %s: %s) for %s",
	    ex->u8x_cond,ex->u8x_context,ex->u8x_details,testid);
    u8_free_exception(ex,0);
    return_value = KNO_TRUE;}
  else if (KNO_ABORTP(result)) {
    u8_exception ex = u8_current_exception;
    u8_logf((err) ? (LOG_NOTICE) : (LOG_WARN),
	    "Tests/UnexpectedError","%m (from %s: %s) for %s",
	    ex->u8x_cond,ex->u8x_context,ex->u8x_details,
	    testid);
    if (err)
      return_value = result;
    else return_value = KNO_FALSE;}
  else if ( (KNO_VOIDP(result)) && (expected == void_symbol) ) {
    u8_logf(LOG_INFO,"Tests/ExpectedVoidValue","%s",testid);
    return_value = KNO_TRUE;}
  else if (KNO_VOIDP(result)) {
    u8_logf(LOG_WARN,"Tests/UnexpectedVoidValue",
	    "%s expected %q",testid,expected);
    if (err) return_value = kno_err
	       ("Tests/UnexpectedVoidValue","applytest",testid,VOID);}
  else if (KNO_VOIDP(predicate)) {
    if (test_match(expected,result)) {
      u8_logf(LOG_INFO,"Tests/Passed",
	      "Received %q from %q for %s",result,fn,testid);
      return_value = KNO_TRUE;}
    else {
      u8_logf(LOG_WARN,"Tests/UnexpectedValue",
	      "%s:\n  Expected:\t%q\n  Returned:\t%q\n",
	      testid,expected,result);
      if (err) return_value =
		 kno_err("Tests/UnexpectedValue","applytest",testid,result);}}
  else if (KNO_VOIDP(predicate_arg)) {
    lispval pred_result = kno_apply(predicate,1,&result);
    if (KNO_ABORTP(pred_result))
      return_value = pred_result;
    else if ( (KNO_FALSEP(pred_result)) || (EMPTYP(pred_result)) ) {
      u8_logf(LOG_WARN,"Tests/PredicateFailed",
	      "%s:\n  Predicate:\t %q\n  Result:\t%q\n",
	      testid,result,predicate);
      return_value = kno_err("Tests/PredicateFailed","applytest",testid,result);}
    else {
      u8_logf(LOG_INFO,"Tests/Passed",
	      "Result %q passed %q for %s",result,predicate,testid);
      kno_decref(pred_result);
      return_value = KNO_TRUE;}}
  else {
    lispval pred_args[2] = { result, predicate_arg };
    lispval pred_result = kno_apply(predicate,2,pred_args);
    if (KNO_ABORTP(pred_result)) {
      return_value = pred_result;}
    else if ( (KNO_FALSEP(pred_result)) || (EMPTYP(pred_result)) ) {
      u8_logf(LOG_WARN,"Tests/PredicateFailed",
	      "%s:\n  Predicate:\t%q\n  Result:\t%q\n  Argument:\t%q\n",
	      testid,predicate,result,predicate_arg);
      if (err) return_value =
		 kno_err("Tests/PredicateFailed","applytest",testid,result);}
    else {
      u8_logf(LOG_INFO,"Tests/Passed",
	      "Result %q passed %q for %s",result,predicate,testid);
      kno_decref(pred_result);
      return_value = KNO_TRUE;}}
  if (KNO_LEXENVP(result))
    kno_recycle_lexenv((kno_lexenv)result);
  else kno_decref(result);
  u8_free(testid);
  return return_value;
}

static u8_string name2string(lispval x)
{
  if (KNO_SYMBOLP(x)) return KNO_SYMBOL_NAME(x);
  else if (KNO_STRINGP(x)) return KNO_CSTRING(x);
  else return NULL;
}

static lispval evaltest_evalfn(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval testexpr  = kno_get_arg(expr,2), return_value = KNO_TRUE;
  if (VOIDP(testexpr))
    return kno_err(kno_SyntaxError,"evaltest",NULL,expr);
  lispval name_expr = kno_get_arg(expr,3);
  lispval name_value = (KNO_VOIDP(name_expr)) ? (KNO_VOID) :
    (KNO_SYMBOLP(name_expr)) ? (name_expr) :
    (kno_eval(name_expr,env));
  u8_string name = name2string(name_value);
  lispval expected_expr = kno_get_arg(expr,1);
  lispval expected  = kno_eval(expected_expr,env), result = KNO_VOID;
  if (KNO_ABORTED(expected)) {
    kno_seterr("BadExpectedValue","evaltest_evalfn",name,expected_expr);
    return expected;}
  else {
    result = kno_eval(testexpr,env);
    if ( (KNO_ABORTED(result)) && (expected == err_symbol) ) {
      u8_exception ex = u8_erreify();
      u8_logf(LOG_INFO,"Tests/ExpectedError","%m (from %s: %s) evaluating %q",
	      ex->u8x_cond,ex->u8x_context,ex->u8x_details,testexpr);
      u8_free_exception(ex,0);
      return_value = KNO_TRUE;}
    else if (KNO_ABORTED(result)) {
      u8_exception ex = u8_current_exception;
      u8_string msg =
	u8_mkstring("%s%s%s%q signaled an error %m <%s> %s%s%s, expecting %q",
		    U8OPTSTR("(",name,") "),testexpr,
		    ex->u8x_cond,ex->u8x_context,
		    U8OPTSTR(" (",ex->u8x_details,")"),
		    expected);
      return_value = kno_err(TestError,"evaltest",msg,result);
      u8_free(msg);}
    else if ( (KNO_VOIDP(result)) && (expected == void_symbol) ) {
      u8_logf(LOG_INFO,"Tests/ExpectedVoidValue","Evaluating %q",testexpr);
      return_value = KNO_TRUE;;}
    else if (KNO_VOIDP(result)) {
      u8_logf(LOG_NOTICE,"Tests/UnexpectedVoidValue","Evaluating %q",testexpr);
      return_value =
	kno_err("Tests/UnexpectedVoidValue","applytest",NULL,testexpr);}
    else if (test_match(result,expected)) {
      u8_logf(LOG_INFO,"Tests/Passed",
	      "Received %q from %q",result,testexpr);
      return_value = KNO_TRUE;}
    else {
      int historyp = kno_historyp();
      int got_ref = (historyp) ? (kno_histpush(result)) : (-1);
      int expect_ref = (historyp) ? (kno_histpush(expected)) : (-1);
      u8_string msg =
	((got_ref>0) || (expect_ref>0)) ?
	(u8_mkstring("%s%s%s%q"
		     "\n returned   %q (#%d)"
		     "\n instead of %q (#%d)",
		     U8OPTSTR("(",name,") "),testexpr,
		     result,got_ref,
		     expected,expect_ref)) :
	(u8_mkstring("%s%s%s%q"
		     "\n returned   %q"
		     "\n instead of %q",
		     U8OPTSTR("(",name,") "),testexpr,
		     result,expected));
      return_value = kno_err(TestFailed,"evaltest",msg,result);
      u8_free(msg);}}
  kno_decref(name_value);
  kno_decref(expected);
  if (KNO_LEXENVP(result))
    kno_recycle_lexenv((kno_lexenv)result);
  else kno_decref(result);
  return return_value;
}

static lispval errtest_evalfn(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval test_expr = kno_get_arg(expr,1);
  lispval v = kno_stack_eval(test_expr,env,s,0);
  if (KNO_ABORTP(v)) {
    u8_exception ex = u8_erreify();
    if (ex) {
      u8_log(LOG_INFO,"ExpectedError",
	     "As expected, %q generated the error %s <%s> (%s)",
	     test_expr,ex->u8x_cond,ex->u8x_context,ex->u8x_details);
      u8_free_exception(ex,1);}
    else u8_log(LOG_NOTICE,"Missing Exception",
		"As expected, %q generated an error, but no exception was set",
		test_expr);
    return KNO_TRUE;}
  else {
    u8_string details = u8_mkstring("%q",test_expr);
    kno_seterr("ExpectedErrorNotSignalled","errtest",details,v);
    u8_free(details);
    kno_decref(v);
    if (! ( (KNO_VOIDP(err_testfail)) || (KNO_FALSEP(err_testfail)) ) )
      return KNO_ERROR;
    else {
      u8_exception ex = u8_erreify();
      add_failed_test(kno_get_exception(ex));
      u8_free_exception(ex,LEAVE_EXSTACK);
      return KNO_FALSE;}}
}

/* Initialization */

KNO_EXPORT void kno_init_eval_testops_c()
{
  u8_register_source_file(_FILEINFO);

  err_symbol = kno_intern("err");
  void_symbol = kno_intern("void");

  link_local_cprims();

  kno_def_evalfn(kno_scheme_module,"EVALTEST",
		 "`(EVALTEST *expected* *expr*)` evaluates *expr* and checks "
		 "if it is EQUAL? to *expected*. If so it does nothing, "
		 "otherwise it either logs the failed test (depending on "
		 "the LOGTESTS config) or signals an error.",
		 evaltest_evalfn);
  kno_def_evalfn(kno_scheme_module,"ERRTEST",
		 "`(ERRTEST *buggy-expr*)` evalutes *bugg-expr* and ignores "
		 "any signalled error. However, if *buggy-expr* returns a "
		 "without an error, this is either logged (depending on "
		 "the LOGTESTS config) or signalled as an error",
		 errtest_evalfn);

  kno_register_config
    ("TESTS:ERROR",
     "Whether to raise errors when tests fail",
     kno_lconfig_get,kno_lconfig_set,&err_testfail);
  kno_register_config
    ("TESTS:LOGLEVEL",
     "Loglevel for reporting test results",
     kno_intconfig_get,kno_loglevelconfig_set,&testops_loglevel);
  kno_register_config
    ("FAILEDTESTS","All failed tests",
     kno_lconfig_get,config_failed_tests_set,&failed_tests);
}

static void link_local_cprims()
{
  KNO_LINK_VARARGS("applytest",applytest,kno_scheme_module);
}
