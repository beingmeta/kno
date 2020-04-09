/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/eval.h"
#include "kno/cprims.h"
#include "kno/knoregex.h"

#include <libu8/u8logging.h>


KNO_EXPORT int kno_init_testcapi(void) KNO_LIBINIT_FN;

static long long int testcapi_init = 0;

DEFPRIM("regex/testcapi",regex_testcapi,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	"Run various tests of the C API for regexes which "
	"are difficult to do directly from scheme");
static lispval regex_testcapi()
{
  lispval pattern = knostring("[0123456789]+");
  lispval regex = kno_make_regex(CSTRING(pattern),0);
  u8_string test_string = "There were 112 different people";
  size_t test_len = strlen(test_string);
  ssize_t rv = kno_regex_op(rx_search,regex,test_string,test_len,0);
  if (rv<0) {kno_clear_errors(0);}
  rv = kno_regex_op(rx_exactmatch,regex,"3457",4,0);
  if (rv<0) {kno_clear_errors(0);}
  rv = kno_regex_matchlen(regex,test_string,test_len);
  if (rv<0) {kno_clear_errors(0);}
  rv = kno_regex_matchlen(regex,test_string+11,test_len-11);
  if (rv<0) {kno_clear_errors(0);}
  rv = kno_regex_match(regex,"3333",4);
  if (rv<0) {kno_clear_errors(0);}
  rv = kno_regex_search(regex,"  abc d 3333 efg h",strlen("  abc d 3333 efg h"));;
  if (rv<0) {kno_clear_errors(0);}
  rv = kno_regex_test(regex,"  abc d 3333 efg h",strlen("  abc d 3333 efg h"));;
  if (rv<0) {kno_clear_errors(0);}
  kno_decref(regex);
  kno_decref(pattern);
  return VOID;
}

DEFPRIM4("regex/rawop",regex_rawop,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(4),
	 "Call kno_regex_op directory",
	 kno_fixnum_type,KNO_VOID,kno_regex_type,KNO_VOID,
	 kno_string_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval regex_rawop(lispval code,lispval pat,lispval string,
			   lispval flags)
{
  int opcode = kno_getint(code);
  int eflags = kno_getint(flags);
  ssize_t result = kno_regex_op((enum KNO_REGEX_OP)opcode,
				pat,
				KNO_CSTRING(string),
				KNO_STRLEN(string),
				eflags);
  if (result == -1)
    return KNO_FALSE;
  else if (result == -2)
    return KNO_ERROR;
  else return KNO_INT(result);
}

static U8_MAYBE_UNUSED int check_error(u8_string cxt,lispval result)
{
  int rv = 1;
  if (KNO_ABORTP(result)) {
    u8_exception ex = u8_erreify();
    lispval errobj = kno_get_exception(ex);
    u8_log(LOGNOTICE,cxt,"Expected error %q",errobj);
    u8_free_exception(ex,0);
    rv = 0;}
  else u8_log(LOG_ERROR,cxt,
	      "Test failed to generate expected error, returned %q",
	      result);
  kno_decref(result);
  return rv;
}

typedef void (*ptr_freefn)(void *);
#define freefn(x) ((ptr_freefn)(x))

static int  U8_MAYBE_UNUSED check_null
(u8_string cxt,void *result,ptr_freefn fn)
{
  int rv = 1;
  if (result==NULL) {
    u8_exception ex = u8_erreify();
    lispval errobj = kno_get_exception(ex);
    u8_log(LOGNOTICE,cxt,"Expected error %q",errobj);
    u8_free_exception(ex,0);
    rv = 0;}
  else u8_log(LOG_ERROR,cxt,"Test failed to generate expected NULL error"
	      "returned %llx",(unsigned long long)((kno_ptrval)result));
  if ( (fn) && (result) )
    fn(result);
  return rv;
}

static int  U8_MAYBE_UNUSED check_neg(u8_string cxt,int result)
{
  int rv = 1;
  if (result<0) {
    u8_exception ex = u8_erreify();
    lispval errobj = kno_get_exception(ex);
    u8_log(LOGNOTICE,cxt,"Expected error %q",errobj);
    u8_free_exception(ex,0);
    rv = 0;}
  else u8_log(LOG_ERROR,cxt,
	      "Test failed to generate expected negatrive error, returned %d",
	      result);
  return rv;
}

static  U8_MAYBE_UNUSED int check_result
(u8_string cxt,lispval result,lispval expected)
{
  int rv = 1;
  if (KNO_EQUALP(result,expected)) {
    u8_log(LOGNOTICE,cxt,"Got expected result");
    rv = 0;}
  else u8_log(LOGERR,cxt,"Unexpected result %q",result);
  kno_decref(result);
  return rv;
}

typedef int (*testp)(lispval);

static U8_MAYBE_UNUSED int check_resultp
(u8_string cxt,lispval result,testp tester)
{
  int rv = 1;
  if (tester(result)) {
    u8_log(LOGNOTICE,cxt,"Got expected result");
    rv = 0;}
  else u8_log(LOGERR,cxt,"Unexpected result %q",result);
  kno_decref(result);
  return rv;
}

static U8_MAYBE_UNUSED int lexenvp(lispval x) { return (KNO_LEXENVP(x)); }

DEFPRIM("modules/testcapi",modules_testcapi,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	"Run various tests of the module C API which are "
	"difficult to do directly from scheme");
static lispval modules_testcapi()
{
  int errors = 0;
  lispval astring = knostring("some string");
  errors += check_null("kno_make_env(#f)",kno_make_env(KNO_FALSE,NULL),
		       freefn(kno_free_lexenv));
  errors += check_null("kno_make_env(string)",kno_make_env(astring,NULL),
		       freefn(kno_free_lexenv));
  errors += check_null("kno_make_export_env(#f)",
		       kno_make_export_env(KNO_FALSE,NULL),
		       freefn(kno_free_lexenv));
  errors += check_null("kno_make_export_env(#f)",
		       kno_make_export_env(astring,NULL),
		       freefn(kno_free_lexenv));
  errors += check_null("kno_new_lexenv()",
		       kno_new_lexenv(KNO_FALSE),
		       freefn(kno_free_lexenv));
  errors += check_neg("kno_finish_module(string)",
		       kno_finish_module(astring));
  errors += check_neg("kno_module_finished(string,flags)",
		      kno_module_finished(astring,0));
  lispval table = kno_make_slotmap(0,0,NULL);
  kno_lexenv tmp_env = kno_new_lexenv(table);
  if (tmp_env == NULL) errors++;
  else {kno_decref((lispval)tmp_env);}
  kno_decref(table);
  kno_decref(astring);
  if (errors == 0)
    return KNO_FALSE;
  else return KNO_INT(errors);
}

DEFPRIM("eval/testcapi",eval_testcapi,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	"Run various tests of the module C API which are "
	"difficult to do directly from scheme");
static lispval eval_testcapi()
{
  int errors = 0;
  {
    kno_lexenv env = kno_working_lexenv();
    lispval invalid_opcode = KNO_OPCODE(2000);
    u8_string unparsed = kno_lisp2string(invalid_opcode);
    if (strstr(unparsed,"invalid")==NULL) {
      u8_log(LOGERR,"FailedCAPI","kno_lisp2string(invalid_opcode): %s:%d",
	     __FILE__,__LINE__);
      errors++;}
    u8_free(unparsed);

    lispval expr = kno_make_list(2,kno_intern("opcode-name"),invalid_opcode);
    lispval result = kno_eval_arg(expr,env);
    if (!(KNO_ABORTP(result))) {
      u8_log(LOGERR,"NoError","Expr %q returned %q",expr,result);
      errors++;}
    lispval eval_choice = KNO_EMPTY_CHOICE;
    KNO_ADD_TO_CHOICE(eval_choice,expr);
    KNO_ADD_TO_CHOICE(eval_choice,kno_intern("opcode-name"));
    eval_choice = kno_simplify_choice(eval_choice);
    if (!(kno_choice_evalp(expr))) {
      u8_log(LOGERR,"FailedCAPI","kno_choice_evalp(expr)=0: %s:%d",
	     __FILE__,__LINE__);
      errors++;}
    if (!(kno_choice_evalp(KNO_INT(8)))) {
      u8_log(LOGERR,"FailedCAPI","kno_choice_evalp(num)=0: %s:%d",
	     __FILE__,__LINE__);
      errors++;}
    if (!(kno_choice_evalp(kno_intern("opcode-name")))) {
      u8_log(LOGERR,"FailedCAPI","kno_choice_evalp(sym)=0: %s:%d",
	     __FILE__,__LINE__);
      errors++;}
    if (!(kno_choice_evalp(eval_choice))) {
      u8_log(LOGERR,"FailedCAPI","kno_choice_evalp(echoice)=0: %s:%d",
	     __FILE__,__LINE__);
      errors++;}
    KNO_ADD_TO_CHOICE(eval_choice,KNO_INT(42));
    if (kno_choice_evalp(eval_choice)) {
      u8_log(LOGERR,"FailedCAPI","kno_choice_evalp(nechoice)=1: %s:%d",
	     __FILE__,__LINE__);
      errors++;}
    KNO_ADD_TO_CHOICE(eval_choice,KNO_INT(42));
    eval_choice = kno_simplify_choice(eval_choice);
    kno_decref(invalid_opcode);
    kno_decref(eval_choice);
    kno_decref(result);}
  if (errors == 0)
    return KNO_FALSE;
  else return KNO_INT(errors);
}

DEFPRIM("API/FORCE-PROMISE",kno_force_promise,MAX_ARGS(1)|MIN_ARGS(1),
	"Run various tests of the module C API which are "
	"difficult to do directly from scheme");


static lispval testcapi_module;

KNO_EXPORT int kno_init_testcapi()
{
  if (testcapi_init) return 0;
  testcapi_module =
    kno_new_cmodule_x("testcapi",0,kno_init_testcapi,__FILE__);

  link_local_cprims();

  u8_register_source_file(_FILEINFO);

  return 1;
}


static void link_local_cprims()
{
  KNO_LINK_PRIM("eval/testcapi",eval_testcapi,0,testcapi_module);
  KNO_LINK_PRIM("modules/testcapi",modules_testcapi,0,testcapi_module);
  KNO_LINK_PRIM("regex/rawop",regex_rawop,4,testcapi_module);
  KNO_LINK_PRIM("regex/testcapi",regex_testcapi,0,testcapi_module);
  KNO_LINK_PRIM("api/force-promise",kno_force_promise,1,testcapi_module);
}

