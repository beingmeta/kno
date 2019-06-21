/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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

#include "libu8/u8logging.h"

KNO_EXPORT int kno_init_testcapi(void) KNO_LIBINIT_FN;

static long long int testcapi_init = 0;

KNO_DCLPRIM("REGEX/TESTCAPI",regex_testcapi,MAX_ARGS(0)|MIN_ARGS(0),
	    "Run various tests of the C API for regexes "
	    "which are difficult to do directly from scheme")
static lispval regex_testcapi()
{
  lispval pattern = knostring("[0123456789]+");
  lispval regex = kno_make_regex(CSTRING(pattern),0);
  lispval bad_pattern = knostring("[0123456789+");
  lispval bad_regex = kno_make_regex(CSTRING(bad_pattern),0);
  u8_string test_string = "There were 112 different people";
  size_t test_len = strlen(test_string);
  ssize_t rv = kno_regex_op(rx_search,regex,test_string,test_len,0);
  /* Try with different flags */
  rv = kno_regex_op(rx_search,regex,test_string,test_len,1);
  rv = kno_regex_op(rx_search,regex,test_string,test_len,2);
  rv = kno_regex_op(rx_search,regex,test_string,test_len,3);
  rv = kno_regex_op(rx_matchstring,regex,test_string,test_len,0);
  rv = kno_regex_op(rx_matchspan,regex,test_string,test_len,0);
  rv = kno_regex_matchlen(regex,test_string,test_len);
  rv = kno_regex_matchlen(regex,test_string+11,test_len-11);
  rv = kno_regex_match(regex,"3333",4);
  if (rv<0) {u8_pop_exception();}
  rv = kno_regex_op(rx_matchlen,bad_regex,test_string,test_len,0);
  if (rv<0) {u8_pop_exception();}
  kno_decref(regex); kno_decref(pattern);
  kno_decref(bad_regex); kno_decref(bad_pattern);
  return VOID;
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
  else u8_log(LOG_ERROR,cxt,"Test failed to generate expected error",result);
  if (fn) fn(result);
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

KNO_DCLPRIM("MODULES/TESTCAPI",modules_testcapi,MAX_ARGS(0)|MIN_ARGS(0),
	    "Run various tests of the module C API which are "
	    "difficult to do directly from scheme")
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
  if (errors == 0)
    return KNO_FALSE;
  else return KNO_INT(errors);
}

KNO_EXPORT int kno_init_testcapi()
{
  if (testcapi_init) return 0;
  lispval testcapi_module = kno_new_cmodule("testcapi",0,kno_init_testcapi);
  /* u8_register_source_file(_FILEINFO); */

  KNO_DECL_PRIM(regex_testcapi,0,testcapi_module);
  KNO_DECL_PRIM(modules_testcapi,0,testcapi_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}
