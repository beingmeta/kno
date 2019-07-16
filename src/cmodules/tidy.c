/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2016 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/cprims.h"

#include "libu8/u8logging.h"

#include "ext/tidy5/tidy.h"
#include "ext/tidy5/tidybuffio.h"
#include "ext/tidy5/tidyenum.h"

KNO_EXPORT int kno_init_tidy(void) KNO_LIBINIT_FN;
u8_condition kno_TidyError=_("Tidy Error");
static long long int tidy_init = 0;

/* Opt setting */

static lispval getoption(lispval opts,u8_string optstring,lispval dflt)
{
  if ((KNO_FALSEP(opts))||(KNO_VOIDP(opts)))
    return kno_incref(dflt);
  else return kno_getopt(opts,kno_intern(optstring),dflt);
}

static U8_MAYBE_UNUSED int tidySetBoolOpt(TidyDoc tdoc,
                                          TidyOptionId optname,
                                          lispval value) {
  if (KNO_FALSEP(value))
    return tidyOptSetBool(tdoc,optname,no);
  else return tidyOptSetBool(tdoc,optname,yes);}
static U8_MAYBE_UNUSED int copyBoolOpt(lispval opts,
                                       TidyDoc tdoc,
                                       TidyOptionId optname,
                                       u8_string optstring,
                                       lispval dflt){
  lispval value = getoption(opts,optstring,dflt); int rc;
  if (KNO_FALSEP(value))
    rc = tidyOptSetBool(tdoc,optname,no);
  else rc = tidyOptSetBool(tdoc,optname,yes);
  kno_decref(value);
  return rc;}

static U8_MAYBE_UNUSED int tidySetIntOpt(TidyDoc tdoc,
                                         TidyOptionId optname,
                                         lispval value){
  if (KNO_INTP(value))
    return tidyOptSetInt(tdoc,optname,KNO_FIX2INT(value));
  else {
    kno_incref(value);
    kno_seterr(kno_TypeError,"tidySetIntOpt","integer",value);
    return -1;}}
static U8_MAYBE_UNUSED int copyIntOpt(lispval opts,TidyDoc tdoc,
                                      TidyOptionId optname,
                                      u8_string optstring,
                                      int dflt){
  int rc = -1;
  lispval value = getoption(opts,optstring,KNO_VOID);
  if (KNO_VOIDP(value))
    rc = tidyOptSetInt(tdoc,optname,dflt);
  else if (KNO_INTP(value))
    rc = tidyOptSetInt(tdoc,optname,KNO_FIX2INT(value));
  else {
    kno_incref(value);
    kno_seterr(kno_TypeError,"tidySetIntOpt","integer",value);
    rc = -1;}
  kno_decref(value);
  return rc;}

static U8_MAYBE_UNUSED int tidySetStringOpt(TidyDoc tdoc,
                                            TidyOptionId optname,
                                            lispval value){
  if (KNO_STRINGP(value))
    return tidyOptSetValue(tdoc,optname,KNO_CSTRING(value));
  else {
    kno_incref(value);
    kno_seterr(kno_TypeError,"tidySetIntOpt","string",value);
    return -1;}}
static U8_MAYBE_UNUSED int copyStringOpt(lispval opts,
                                         TidyDoc tdoc,
                                         TidyOptionId optname,
                                         u8_string optstring,
                                         u8_string dflt) {
  int rc = -1;
  lispval value = getoption(opts,optstring,KNO_VOID);
  if (KNO_VOIDP(value))
    rc = tidyOptSetValue(tdoc,optname,dflt);
  else if (KNO_STRINGP(value))
    rc = tidyOptSetValue(tdoc,optname,KNO_CSTRING(value));
  else {
    kno_incref(value);
    kno_seterr(kno_TypeError,"copyStringOpt(tidy)","string",value);
    return -1;}
  return rc;}

static U8_MAYBE_UNUSED int testopt(lispval opts,lispval sym,int dflt)
{
  lispval v = kno_getopt(opts,sym,KNO_VOID);
  if (KNO_VOIDP(v)) return dflt;
  else if (KNO_FALSEP(v)) return 0;
  else {
    kno_decref(v);
    return 1;}
}

/* The main primitive */

static lispval doctype_symbol, dontfix_symbol, wrap_symbol, xhtml_symbol;

static lispval tidy_prim_helper(lispval string,lispval opts,lispval diag,
                                int do_fixes,int xhtml)
{
  lispval result = KNO_VOID; TidyBuffer outbuf={NULL};
  TidyBuffer errbuf={NULL};
  int rc = -1;
  TidyDoc tdoc = tidyCreate();
  lispval for_real = ((do_fixes)?(KNO_TRUE):(KNO_FALSE));
  tidyBufInit(&outbuf);
  tidyBufInit(&errbuf);
  rc = tidySetErrorBuffer(tdoc,&errbuf);
  if (rc<0) {
    tidyRelease(tdoc);
    return kno_err(kno_TidyError,"tidy_prim/init",NULL,KNO_VOID);}
  if (xhtml) {
    rc = copyBoolOpt(opts,tdoc,TidyXhtmlOut,"XHTMLOUT",KNO_TRUE);
    if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyXmlSpace,"XMLSPACE",KNO_TRUE);}
  else rc = copyBoolOpt(opts,tdoc,TidyHtmlOut,"HTMLOUT",KNO_FALSE);
  if (rc>=0) rc = copyStringOpt(opts,tdoc,TidyCharEncoding,"ENCODING","utf8");
  if (rc>=0) rc = copyStringOpt(opts,tdoc,TidyAltText,"ALTSTRING","utf8");
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyShowWarnings,"WARN",for_real);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyQuiet,"QUIET",KNO_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyMakeBare,"BARE",for_real);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyMakeClean,"CLEAN",for_real);
  if (rc>=0) rc = copyBoolOpt
               (opts,tdoc,TidyDropEmptyParas,"DROPEMPTY",for_real);
  if (rc>=0) rc = copyBoolOpt
               (opts,tdoc,TidyFixComments,"FIXCOMMENTS",KNO_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyXmlDecl,"XMLDECL",KNO_FALSE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyEncloseBodyText,"ENCLOSEBODY",
                              for_real);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyEncloseBlockText,"ENCLOSEBLOCK",
                              KNO_FALSE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyWord2000,"FIXWORD2000",KNO_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyMark,"LEAVEMARK",KNO_FALSE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyJoinClasses,"JOINCLASSES",KNO_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyJoinStyles,"JOINSTYLES",KNO_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyFixUri,"FIXURI",KNO_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyNumEntities,"NUMENTITIES",KNO_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyNCR,"NUMENTITIES",KNO_TRUE);
  if (rc>=0) rc = copyStringOpt(opts,tdoc,TidyCSSPrefix,"CSSPREFIX","tidy-");
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyQuoteAmpersand,"QUOTEAMP",KNO_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyQuoteNbsp,"QUOTENBSP",KNO_FALSE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyIndentAttributes,
                              "INDENTATTRIBS",KNO_TRUE);
  if (rc>=0) rc = copyIntOpt(opts,tdoc,TidyIndentSpaces,"INDENTATION",2);
  if (rc>=0) rc = copyIntOpt(opts,tdoc,TidyTabSize,"TABSIZE",5);
  /*
    if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyMergeDivs,"MERGEDIVS",KNO_FALSE);
    if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyMergeSpans,"MERGESPANS",KNO_FALSE);
  */
  if (rc>=0) {
    lispval wrap = kno_getopt(opts,wrap_symbol,KNO_VOID);
    if (KNO_INTP(wrap))
      rc = tidyOptSetInt(tdoc,TidyWrapLen,KNO_FIX2INT(wrap));
    else if (!((KNO_FALSEP(wrap))||(KNO_VOIDP(wrap))))
      rc = tidyOptSetInt(tdoc,TidyWrapLen,80);
    else {}}
  if (rc>=0) {
    lispval indent = kno_getopt(opts,doctype_symbol,KNO_VOID);
    if (KNO_FALSEP(indent)) {}
    else if (KNO_INTP(indent)) {
      rc = tidyOptSetValue(tdoc,TidyIndentContent,"auto");
      if (rc>=0)
        rc = tidyOptSetInt(tdoc,TidyIndentSpaces,KNO_FIX2INT(indent));}
    else rc = tidyOptSetValue(tdoc,TidyIndentContent,"auto");}
  if (rc>=0) {
    lispval doctype = kno_getopt(opts,doctype_symbol,KNO_VOID);
    if (KNO_VOIDP(doctype))
      tidyOptSetInt(tdoc,TidyDoctypeMode,TidyDoctypeAuto);
    else if (KNO_FALSEP(doctype))
      tidyOptSetInt(tdoc,TidyDoctypeMode,TidyDoctypeAuto);
    else if (!(KNO_STRINGP(doctype))) {
      tidyOptSetInt(tdoc,TidyDoctypeMode,TidyDoctypeUser);
      tidyOptSetValue(tdoc,TidyDoctype,"<!DOCTYPE html>");}
    else {
      tidyOptSetInt(tdoc,TidyDoctypeMode,TidyDoctypeUser);
      tidyOptSetValue(tdoc,TidyDoctype,KNO_CSTRING(doctype));}
    kno_decref(doctype);}
  if (rc<0) result = kno_err(kno_TidyError,"tidy_prim/setopts",errbuf.bp,KNO_VOID);
  else {
    lispval dontfix = kno_getopt(opts,dontfix_symbol,KNO_FALSE);
    rc = tidyParseString(tdoc,KNO_CSTRING(string));
    if (rc<0)
      result = kno_err(kno_TidyError,"tidy_prim/parse",errbuf.bp,KNO_VOID);
    else if (KNO_FALSEP(dontfix))
      rc = tidyCleanAndRepair(tdoc);
    else {}
    if (!(KNO_VOIDP(result))) {}
    else if (rc<0)
      result = kno_err(kno_TidyError,"tidy_prim/clean",errbuf.bp,KNO_VOID);
    else rc = ((tidyOptSetBool(tdoc,TidyForceOutput,yes))?(rc):(-1));
    if (!(KNO_VOIDP(result))) {}
    else if (rc<0)
      result = kno_err(kno_TidyError,"tidy_prim/forceout",errbuf.bp,KNO_VOID);
    else rc = tidySaveBuffer(tdoc,&outbuf);
    if (!(KNO_VOIDP(result))) {}
    else if (rc<0)
      result = kno_err(kno_TidyError,"tidy_prim/output",errbuf.bp,KNO_VOID);
    else result = kno_mkstring(outbuf.bp);
    if ((!((KNO_VOIDP(diag))||(KNO_FALSEP(diag))))&&((rc>0)||(rc<0))) {
      int drc = tidyRunDiagnostics(tdoc);
      if (drc<0) u8_log(LOG_CRIT,"TIDY/diagfail","%s",errbuf.bp);
      else if (KNO_APPLICABLEP(diag)) {
        lispval arg = kno_mkstring(errbuf.bp);
        lispval dresult = kno_apply(diag,1,&arg);
        if (KNO_ABORTP(dresult)) {
          kno_decref(result); result = dresult;}
        else kno_decref(dresult);}
      else u8_log(LOG_WARNING,"TIDY","%s",errbuf.bp);}}
  tidyBufFree(&outbuf);
  tidyBufFree(&errbuf);
  tidyRelease(tdoc);
  return result;
}

DEFPRIM3("TIDY5",tidy_prim,MIN_ARGS(1),
         "Cleans up HTML text in its agument",
         kno_string_type,KNO_VOID,-1,KNO_VOID,
         -1,KNO_VOID);
static lispval tidy_prim(lispval string,lispval opts,lispval diag)
{
  return tidy_prim_helper(string,opts,diag,1,-1);
}

DEFPRIM3("TIDY->INDENT",tidy_indent_prim,MIN_ARGS(1),
         "Cleans up HTML text, indenting the result",
         kno_string_type,KNO_VOID,-1,KNO_VOID,
         -1,KNO_VOID);
static lispval tidy_indent_prim(lispval string,lispval opts,lispval diag)
{
  return tidy_prim_helper(string,opts,diag,0,-1);
}

DEFPRIM3("TIDY->HTML",tidy_html_prim,MIN_ARGS(1),
         "Cleans up HTML text, generates HTML5 (not XHTML)",
         kno_string_type,KNO_VOID,-1,KNO_VOID,
         -1,KNO_VOID);
static lispval tidy_html_prim(lispval string,lispval opts,lispval diag)
{
  return tidy_prim_helper(string,opts,diag,1,0);
}

DEFPRIM3("TIDY->XHTML",tidy_xhtml_prim,MIN_ARGS(1),
         "Cleans up HTML text, generating XHTML",
         kno_string_type,KNO_VOID,-1,KNO_VOID,
         -1,KNO_VOID);
static lispval tidy_xhtml_prim(lispval string,lispval opts,lispval diag)
{
  return tidy_prim_helper(string,opts,diag,1,1);
}

static lispval tidy_module;

KNO_EXPORT int kno_init_tidy()
{
  if (tidy_init) return 0;

  tidy_init = u8_millitime();
  doctype_symbol = kno_intern("doctype");
  dontfix_symbol = kno_intern("dontfix");
  xhtml_symbol = kno_intern("xhtml");
  wrap_symbol = kno_intern("wrap");
  tidy_module = kno_new_cmodule("tidy",0,kno_init_tidy);

  init_local_cprims();

  kno_finish_module(tidy_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

static void init_local_cprims()
{
  KNO_LINK_PRIM("TIDY5",tidy_prim,3,tidy_module);
  KNO_LINK_PRIM("TIDY->XHTML",tidy_xhtml_prim,3,tidy_module);
  KNO_LINK_PRIM("TIDY->INDENT",tidy_indent_prim,3,tidy_module);
  KNO_LINK_PRIM("TIDY->HTML",tidy_html_prim,3,tidy_module);
}
