/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_FCNIDS 1
#define FD_INLINE_STACKS 1
#define FD_INLINE_LEXENV 1

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/support.h"
#include "framerd/storage.h"
#include "framerd/eval.h"
#include "framerd/dtproc.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/ports.h"
#include "framerd/dtcall.h"
#include "framerd/ffi.h"

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

static lispval log_testfail = FD_FALSE;

static u8_condition TestFailed=_("Test failed");
static u8_condition TestError=_("Test error");

/* Test functions */

#define LEAVE_EXSTACK 0

static lispval failed_tests = FD_EMPTY_CHOICE;
static u8_mutex failed_tests_lock;

static void add_failed_test(lispval object)
{
  u8_lock_mutex(&failed_tests_lock);
  FD_ADD_TO_CHOICE(failed_tests,object);
  u8_unlock_mutex(&failed_tests_lock);
}

static int config_failed_tests_set(lispval var,lispval val,void *data)
{
  lispval *ptr = data;
  u8_lock_mutex(&failed_tests_lock);
  lispval cur = *ptr;
  if (FD_FALSEP(val)) {
    *ptr = FD_EMPTY;
    fd_decref(cur);}
  else {
    fd_incref(val);
    FD_ADD_TO_CHOICE(cur,val);
    *ptr = cur;}
  u8_unlock_mutex(&failed_tests_lock);
  return 1;
}

static lispval applytest_inner(int n,lispval *args)
{
  if (n<2)
    return fd_err(fd_TooFewArgs,"applytest",NULL,VOID);
  else if (FD_APPLICABLEP(args[1])) {
    lispval value = fd_apply(args[1],n-2,args+2);
    if (FD_ABORTP(value)) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      u8_exception ex = u8_erreify();
      u8_printf(&out,"Error returned from %q",args[1]);
      int i=2; while (i<n) {u8_printf(&out," %q",args[i]); i++;}
      u8_printf(&out,"\n   Error:    %m <%s> %s%s%s",
                ex->u8x_cond,ex->u8x_context,
                U8OPTSTR(" (",ex->u8x_details,")"));
      u8_printf(&out,"\n   Expected: %q",args[0]);
      u8_restore_exception(ex);
      lispval err = fd_err(TestError,"applytest",out.u8_outbuf,args[1]);
      u8_close_output(&out);
      fd_decref(value);
      return err;}
    else if (FD_EQUAL(value,args[0])) {
      fd_decref(value);
      return FD_TRUE;}
    else if ( (FD_VOIDP(value)) && (args[0] == FDSYM_VOID) ) {
      fd_decref(value);
      return FD_TRUE;}
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      u8_printf(&out,"Unexpected result from %q",args[1]);
      u8_printf(&out,"\n   Expected: %q",args[0]);
      u8_printf(&out,"\n        Got: %q",value);
      lispval err = fd_err(TestFailed,"applytest",out.u8_outbuf,value);
      u8_close_output(&out);
      fd_decref(value);
      return err;}}
  else if (n==2) {
    if (FD_EQUAL(args[1],args[0]))
      return FD_TRUE;
    else {
      u8_string s = u8_mkstring("%q ≭ %q",args[0],args[1]);
      lispval err = fd_err(TestFailed,"applytest",s,args[1]);
      u8_free(s);
      return err;}}
  else if (n == 3) {
    if (FD_EQUAL(args[2],args[0]))
      return FD_TRUE;
    else {
      u8_string s = u8_mkstring("%q: %q ≭ %q",args[1],args[0],args[2]);
      lispval err = fd_err(TestFailed,"applytest",s,args[2]);
      u8_free(s);
      return err;}}
  else return fd_err(fd_TooManyArgs,"applytest",NULL,VOID);
}

static lispval applytest(int n,lispval *args)
{
  lispval v = applytest_inner(n,args);
  if (FD_ABORTP(v)) {
    if ( (FD_VOIDP(log_testfail)) || (FD_FALSEP(log_testfail)) )
      return v;
    else {
      u8_exception ex = u8_erreify();
      if (ex->u8x_cond == TestFailed)
        u8_log(LOG_CRIT,TestFailed,"In applytest:\n %s",ex->u8x_details);
      else u8_log(LOG_CRIT,TestError,"In applytest:\n %s",ex->u8x_details);
      add_failed_test(fd_get_exception(ex));
      u8_free_exception(ex,LEAVE_EXSTACK);
      fd_clear_errors(1);
      return FD_FALSE;}}
  else return v;
}

static u8_string name2string(lispval x)
{
  if (FD_SYMBOLP(x)) return FD_SYMBOL_NAME(x);
  else if (FD_STRINGP(x)) return FD_CSTRING(x);
  else return NULL;
}

static lispval evaltest_inner(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval testexpr  = fd_get_arg(expr,2);
  if (VOIDP(testexpr))
    return fd_err(fd_SyntaxError,"evaltest",NULL,expr);
  lispval name_expr = fd_get_arg(expr,3);
  lispval name_value = (FD_VOIDP(name_expr)) ? (FD_VOID) :
    (FD_SYMBOLP(name_expr)) ? (name_expr) :
    (fd_eval(name_expr,env));
  u8_string name = name2string(name_value);

  lispval expected_expr  = fd_get_arg(expr,1);
  lispval expected  = fd_eval(expected_expr,env);
  if (FD_ABORTED(expected)) {
    fd_seterr("BadExpectedValue","evaltest_evalfn",name,expected_expr);
    return expected;}
  else {
    lispval value = fd_eval(testexpr,env);
    if (FD_ABORTED(value)) {
      u8_exception ex = u8_erreify();
      u8_string msg =
        u8_mkstring("%s%s%s%q signaled an error %m <%s> %s%s%s, expecting %q",
                    U8OPTSTR("(",name,") "),testexpr,
                    ex->u8x_cond,ex->u8x_context,
                    U8OPTSTR(" (",ex->u8x_details,")"),
                    expected);
      lispval err = fd_err(TestError,"evaltest",msg,value);
      fd_decref(expected); u8_free(msg);
      return err;}
    else if (FD_EQUAL(value,expected)) {
      fd_decref(name_value);
      fd_decref(expected);
      fd_decref(value);
      return FD_TRUE;}
    else if ( (FD_VOIDP(value)) &&
              (expected == FDSYM_VOID) ) {
      fd_decref(name_value);
      fd_decref(expected);
      fd_decref(value);
      return FD_TRUE;}
    else {
      u8_string msg =
        u8_mkstring("%s%s%s%q"
                    "\n returned   %q"
                    "\n instead of %q",
                    U8OPTSTR("(",name,") "),testexpr,
                    value,expected);
      lispval err = fd_err(TestFailed,"evaltest",msg,value);
      u8_free(msg); fd_decref(name_value);
      fd_decref(value); fd_decref(expected);
      return err;}}
}

static lispval evaltest_evalfn(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval v = evaltest_inner(expr,env,s);
  if (FD_ABORTP(v)) {
    if ( (FD_VOIDP(log_testfail)) || (FD_FALSEP(log_testfail)) )
      return v;
    else {
      u8_exception ex = u8_erreify();
      if (ex->u8x_cond == TestFailed)
        u8_log(LOG_CRIT,TestFailed,"In evaltest:\n %s",ex->u8x_details);
      else u8_log(LOG_CRIT,TestError,"In applytest:\n %s",ex->u8x_details);
      add_failed_test(fd_get_exception(ex));
      u8_free_exception(ex,LEAVE_EXSTACK);
      fd_clear_errors(1);
      return FD_FALSE;}}
  else return v;
}

/* Initialization */

FD_EXPORT void fd_init_eval_testops_c()
{
  u8_register_source_file(_FILEINFO);

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("APPLYTEST",applytest,2)));
  fd_def_evalfn(fd_scheme_module,"EVALTEST","",evaltest_evalfn);
  
  fd_register_config
    ("LOGTESTS",
     "Whether to log failed tests, rather than treating them as errors",
     fd_lconfig_get,fd_lconfig_set,&log_testfail);
  fd_register_config
    ("FAILEDTESTS","All failed tests",
     fd_lconfig_get,config_failed_tests_set,&failed_tests);
}
