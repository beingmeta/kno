/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES 1
#define KNO_INLINE_TABLES 1
#define KNO_INLINE_FCNIDS 1
#define KNO_INLINE_STACKS 1
#define KNO_INLINE_LEXENV 1

#define KNO_PROVIDE_FASTEVAL 1

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
#include "kno/ffi.h"

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

static lispval log_testfail = KNO_FALSE;

static u8_condition TestFailed=_("Test failed");
static u8_condition TestError=_("Test error");

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

static lispval applytest_inner(int n,lispval *args)
{
  if (n<2)
    return kno_err(kno_TooFewArgs,"applytest",NULL,VOID);
  else if (KNO_APPLICABLEP(args[1])) {
    lispval value = kno_apply(args[1],n-2,args+2);
    if (KNO_ABORTP(value)) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      u8_exception ex = u8_erreify();
      u8_printf(&out,"Error returned from %q",args[1]);
      int i=2; while (i<n) {u8_printf(&out," %q",args[i]); i++;}
      u8_printf(&out,"\n   Error:    %m <%s> %s%s%s",
		ex->u8x_cond,ex->u8x_context,
		U8OPTSTR(" (",ex->u8x_details,")"));
      u8_printf(&out,"\n   Expected: %q",args[0]);
      u8_restore_exception(ex);
      lispval err = kno_err(TestError,"applytest",out.u8_outbuf,args[1]);
      u8_close_output(&out);
      kno_decref(value);
      return err;}
    else if (test_match(value,args[0])) {
      kno_decref(value);
      return KNO_TRUE;}
    else if ( (KNO_VOIDP(value)) && (args[0] == KNOSYM_VOID) ) {
      kno_decref(value);
      return KNO_TRUE;}
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      u8_printf(&out,"Unexpected result from %q",args[1]);
      u8_printf(&out,"\n   Expected: %q",args[0]);
      u8_printf(&out,"\n	Got: %q",value);
      lispval err = kno_err(TestFailed,"applytest",out.u8_outbuf,value);
      u8_close_output(&out);
      kno_decref(value);
      return err;}}
  else if (n==2) {
    if (test_match(args[1],args[0]))
      return KNO_TRUE;
    else {
      u8_string s = u8_mkstring("%q ≭ %q",args[0],args[1]);
      lispval err = kno_err(TestFailed,"applytest",s,args[1]);
      u8_free(s);
      return err;}}
  else if (n == 3) {
    if (test_match(args[2],args[0]))
      return KNO_TRUE;
    else {
      u8_string s = u8_mkstring("%q: %q ≭ %q",args[1],args[0],args[2]);
      lispval err = kno_err(TestFailed,"applytest",s,args[2]);
      u8_free(s);
      return err;}}
  else return kno_err(kno_TooManyArgs,"applytest",NULL,VOID);
}

static lispval applytest(int n,lispval *args)
{
  lispval v = applytest_inner(n,args);
  if (KNO_ABORTP(v)) {
    if ( (KNO_VOIDP(log_testfail)) || (KNO_FALSEP(log_testfail)) )
      return v;
    else {
      u8_exception ex = u8_erreify();
      if (ex->u8x_cond == TestFailed)
	u8_log(LOG_CRIT,TestFailed,"In applytest:\n %s",ex->u8x_details);
      else u8_log(LOG_CRIT,TestError,"In applytest:\n %s",ex->u8x_details);
      add_failed_test(kno_get_exception(ex));
      u8_free_exception(ex,LEAVE_EXSTACK);
      kno_clear_errors(1);
      return KNO_FALSE;}}
  else return v;
}

static u8_string name2string(lispval x)
{
  if (KNO_SYMBOLP(x)) return KNO_SYMBOL_NAME(x);
  else if (KNO_STRINGP(x)) return KNO_CSTRING(x);
  else return NULL;
}

static lispval evaltest_inner(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval testexpr  = kno_get_arg(expr,2);
  if (VOIDP(testexpr))
    return kno_err(kno_SyntaxError,"evaltest",NULL,expr);
  lispval name_expr = kno_get_arg(expr,3);
  lispval name_value = (KNO_VOIDP(name_expr)) ? (KNO_VOID) :
    (KNO_SYMBOLP(name_expr)) ? (name_expr) :
    (kno_eval(name_expr,env));
  u8_string name = name2string(name_value);

  lispval expected_expr	 = kno_get_arg(expr,1);
  lispval expected  = kno_eval(expected_expr,env);
  if (KNO_ABORTED(expected)) {
    kno_seterr("BadExpectedValue","evaltest_evalfn",name,expected_expr);
    return expected;}
  else {
    lispval value = kno_eval(testexpr,env);
    if (KNO_ABORTED(value)) {
      u8_exception ex = u8_erreify();
      u8_string msg =
        u8_mkstring("%s%s%s%q signaled an error %m <%s> %s%s%s, expecting %q",
                    U8OPTSTR("(",name,") "),testexpr,
                    ex->u8x_cond,ex->u8x_context,
                    U8OPTSTR(" (",ex->u8x_details,")"),
                    expected);
      lispval err = kno_err(TestError,"evaltest",msg,value);
      kno_decref(expected); u8_free(msg);
      return err;}
    else if (test_match(value,expected)) {
      kno_decref(name_value);
      kno_decref(expected);
      kno_decref(value);
      return KNO_TRUE;}
    else if ( (KNO_VOIDP(value)) && (expected == KNOSYM_VOID) ) {
      kno_decref(name_value);
      kno_decref(expected);
      kno_decref(value);
      return KNO_TRUE;}
    else {
      u8_string msg =
	u8_mkstring("%s%s%s%q"
		    "\n returned   %q"
		    "\n instead of %q",
		    U8OPTSTR("(",name,") "),testexpr,
		    value,expected);
      lispval err = kno_err(TestFailed,"evaltest",msg,value);
      u8_free(msg); kno_decref(name_value);
      kno_decref(value); kno_decref(expected);
      return err;}}
}

static lispval evaltest_evalfn(lispval expr,kno_lexenv env,kno_stack s)
{
  lispval v = evaltest_inner(expr,env,s);
  if (KNO_ABORTP(v)) {
    if ( (KNO_VOIDP(log_testfail)) || (KNO_FALSEP(log_testfail)) )
      return v;
    else {
      u8_exception ex = u8_erreify();
      if (ex->u8x_cond == TestFailed)
	u8_log(LOG_CRIT,TestFailed,"In evaltest:\n %s",ex->u8x_details);
      else u8_log(LOG_CRIT,TestError,"In applytest:\n %s",ex->u8x_details);
      add_failed_test(kno_get_exception(ex));
      u8_free_exception(ex,LEAVE_EXSTACK);
      kno_clear_errors(1);
      return KNO_FALSE;}}
  else return v;
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
      u8_free_exception(ex,0);}
    else u8_log(LOG_NOTICE,"Missing Exception",
                "As expected, %q generated an error, but no exception was set",
                test_expr);
    return KNO_TRUE;}
  else {
    u8_string details = u8_mkstring("%q",test_expr);
    kno_seterr("ExpectedErrorNotSignalled","errtest",details,v);
    u8_free(details);
    kno_decref(v);
    if ( (KNO_VOIDP(log_testfail)) || (KNO_FALSEP(log_testfail)) )
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

  kno_idefn(kno_scheme_module,
	   kno_make_ndprim(kno_make_cprimn("APPLYTEST",applytest,2)));
  kno_def_evalfn(kno_scheme_module,"EVALTEST",
		 "`(EVALTEST *expected* *expr*)` evalutes *expr* and checks "
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
    ("LOGTESTS",
     "Whether to log failed tests, rather than treating them as errors",
     kno_lconfig_get,kno_lconfig_set,&log_testfail);
  kno_register_config
    ("FAILEDTESTS","All failed tests",
     kno_lconfig_get,config_failed_tests_set,&failed_tests);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
