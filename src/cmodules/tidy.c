/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "libu8/u8logging.h"

#include "ext/tidy5/tidy.h"
#include "ext/tidy5/tidybuffio.h"
#include "ext/tidy5/tidyenum.h"

FD_EXPORT int fd_init_tidy(void) FD_LIBINIT_FN;
u8_condition fd_TidyError=_("Tidy Error");
static long long int tidy_init = 0;

/* Opt setting */

static lispval getoption(lispval opts,u8_string optstring,lispval dflt)
{
  if ((FD_FALSEP(opts))||(FD_VOIDP(opts)))
    return fd_incref(dflt);
  else return fd_getopt(opts,fd_intern(optstring),dflt);
}

static U8_MAYBE_UNUSED int tidySetBoolOpt(TidyDoc tdoc,
                                          TidyOptionId optname,
                                          lispval value) {
  if (FD_FALSEP(value))
    return tidyOptSetBool(tdoc,optname,no);
  else return tidyOptSetBool(tdoc,optname,yes);}
static U8_MAYBE_UNUSED int copyBoolOpt(lispval opts,
                                       TidyDoc tdoc,
                                       TidyOptionId optname,
                                       u8_string optstring,
                                       lispval dflt){
  lispval value = getoption(opts,optstring,dflt); int rc;
  if (FD_FALSEP(value))
    rc = tidyOptSetBool(tdoc,optname,no);
  else rc = tidyOptSetBool(tdoc,optname,yes);
  fd_decref(value);
  return rc;}

static U8_MAYBE_UNUSED int tidySetIntOpt(TidyDoc tdoc,
                                         TidyOptionId optname,
                                         lispval value){
  if (FD_INTP(value))
    return tidyOptSetInt(tdoc,optname,FD_FIX2INT(value));
  else {
    fd_incref(value);
    fd_seterr(fd_TypeError,"tidySetIntOpt","integer",value);
    return -1;}}
static U8_MAYBE_UNUSED int copyIntOpt(lispval opts,TidyDoc tdoc,
                                      TidyOptionId optname,
                                      u8_string optstring,
                                      int dflt){
  int rc = -1;
  lispval value = getoption(opts,optstring,FD_VOID);
  if (FD_VOIDP(value))
    rc = tidyOptSetInt(tdoc,optname,dflt);
  else if (FD_INTP(value))
    rc = tidyOptSetInt(tdoc,optname,FD_FIX2INT(value));
  else {
    fd_incref(value);
    fd_seterr(fd_TypeError,"tidySetIntOpt","integer",value);
    rc = -1;}
  fd_decref(value);
  return rc;}

static U8_MAYBE_UNUSED int tidySetStringOpt(TidyDoc tdoc,
                                            TidyOptionId optname,
                                            lispval value){
  if (FD_STRINGP(value))
    return tidyOptSetValue(tdoc,optname,FD_CSTRING(value));
  else {
    fd_incref(value);
    fd_seterr(fd_TypeError,"tidySetIntOpt","string",value);
    return -1;}}
static U8_MAYBE_UNUSED int copyStringOpt(lispval opts,
                                         TidyDoc tdoc,
                                         TidyOptionId optname,
                                         u8_string optstring,
                                         u8_string dflt) {
  int rc = -1;
  lispval value = getoption(opts,optstring,FD_VOID);
  if (FD_VOIDP(value))
    rc = tidyOptSetValue(tdoc,optname,dflt);
  else if (FD_STRINGP(value))
    rc = tidyOptSetValue(tdoc,optname,FD_CSTRING(value));
  else {
    fd_incref(value);
    fd_seterr(fd_TypeError,"copyStringOpt(tidy)","string",value);
    return -1;}
  return rc;}

static U8_MAYBE_UNUSED int testopt(lispval opts,lispval sym,int dflt)
{
  lispval v = fd_getopt(opts,sym,FD_VOID);
  if (FD_VOIDP(v)) return dflt;
  else if (FD_FALSEP(v)) return 0;
  else {
    fd_decref(v);
    return 1;}
}

/* The main primitive */

static lispval doctype_symbol, dontfix_symbol, wrap_symbol, xhtml_symbol;

static lispval tidy_prim_helper(lispval string,lispval opts,lispval diag,
                               int do_fixes,int xhtml)
{
  lispval result = FD_VOID; TidyBuffer outbuf={NULL};
  TidyBuffer errbuf={NULL};
  int rc = -1;
  TidyDoc tdoc = tidyCreate();
  lispval for_real = ((do_fixes)?(FD_TRUE):(FD_FALSE));
  tidyBufInit(&outbuf);
  tidyBufInit(&errbuf);
  rc = tidySetErrorBuffer(tdoc,&errbuf);
  if (rc<0) {
    tidyRelease(tdoc);
    return fd_err(fd_TidyError,"tidy_prim/init",NULL,FD_VOID);}
  if (xhtml) {
    rc = copyBoolOpt(opts,tdoc,TidyXhtmlOut,"XHTMLOUT",FD_TRUE);
    if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyXmlSpace,"XMLSPACE",FD_TRUE);}
  else rc = copyBoolOpt(opts,tdoc,TidyHtmlOut,"HTMLOUT",FD_FALSE);
  if (rc>=0) rc = copyStringOpt(opts,tdoc,TidyCharEncoding,"ENCODING","utf8");
  if (rc>=0) rc = copyStringOpt(opts,tdoc,TidyAltText,"ALTSTRING","utf8");
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyShowWarnings,"WARN",for_real);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyQuiet,"QUIET",FD_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyMakeBare,"BARE",for_real);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyMakeClean,"CLEAN",for_real);
  if (rc>=0) rc = copyBoolOpt
               (opts,tdoc,TidyDropEmptyParas,"DROPEMPTY",for_real);
  if (rc>=0) rc = copyBoolOpt
               (opts,tdoc,TidyFixComments,"FIXCOMMENTS",FD_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyXmlDecl,"XMLDECL",FD_FALSE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyEncloseBodyText,"ENCLOSEBODY",
                            for_real);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyEncloseBlockText,"ENCLOSEBLOCK",
                            FD_FALSE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyWord2000,"FIXWORD2000",FD_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyMark,"LEAVEMARK",FD_FALSE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyJoinClasses,"JOINCLASSES",FD_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyJoinStyles,"JOINSTYLES",FD_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyFixUri,"FIXURI",FD_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyNumEntities,"NUMENTITIES",FD_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyNCR,"NUMENTITIES",FD_TRUE);
  if (rc>=0) rc = copyStringOpt(opts,tdoc,TidyCSSPrefix,"CSSPREFIX","tidy-");
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyQuoteAmpersand,"QUOTEAMP",FD_TRUE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyQuoteNbsp,"QUOTENBSP",FD_FALSE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyIndentAttributes,
                            "INDENTATTRIBS",FD_TRUE);
  if (rc>=0) rc = copyIntOpt(opts,tdoc,TidyIndentSpaces,"INDENTATION",2);
  if (rc>=0) rc = copyIntOpt(opts,tdoc,TidyTabSize,"TABSIZE",5);
  /*
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyMergeDivs,"MERGEDIVS",FD_FALSE);
  if (rc>=0) rc = copyBoolOpt(opts,tdoc,TidyMergeSpans,"MERGESPANS",FD_FALSE);
  */
  if (rc>=0) {
    lispval wrap = fd_getopt(opts,wrap_symbol,FD_VOID);
    if (FD_INTP(wrap))
      rc = tidyOptSetInt(tdoc,TidyWrapLen,FD_FIX2INT(wrap));
    else if (!((FD_FALSEP(wrap))||(FD_VOIDP(wrap))))
      rc = tidyOptSetInt(tdoc,TidyWrapLen,80);
    else {}}
  if (rc>=0) {
    lispval indent = fd_getopt(opts,doctype_symbol,FD_VOID);
    if (FD_FALSEP(indent)) {}
    else if (FD_INTP(indent)) {
      rc = tidyOptSetValue(tdoc,TidyIndentContent,"auto");
      if (rc>=0)
        rc = tidyOptSetInt(tdoc,TidyIndentSpaces,FD_FIX2INT(indent));}
    else rc = tidyOptSetValue(tdoc,TidyIndentContent,"auto");}
  if (rc>=0) {
    lispval doctype = fd_getopt(opts,doctype_symbol,FD_VOID);
    if (FD_VOIDP(doctype))
      tidyOptSetInt(tdoc,TidyDoctypeMode,TidyDoctypeAuto);
    else if (FD_FALSEP(doctype))
      tidyOptSetInt(tdoc,TidyDoctypeMode,TidyDoctypeAuto);
    else if (!(FD_STRINGP(doctype))) {
      tidyOptSetInt(tdoc,TidyDoctypeMode,TidyDoctypeUser);
      tidyOptSetValue(tdoc,TidyDoctype,"<!DOCTYPE html>");}
    else {
      tidyOptSetInt(tdoc,TidyDoctypeMode,TidyDoctypeUser);
      tidyOptSetValue(tdoc,TidyDoctype,FD_CSTRING(doctype));}
    fd_decref(doctype);}
  if (rc<0) result = fd_err(fd_TidyError,"tidy_prim/setopts",errbuf.bp,FD_VOID);
  else {
    lispval dontfix = fd_getopt(opts,dontfix_symbol,FD_FALSE);
    rc = tidyParseString(tdoc,FD_CSTRING(string));
    if (rc<0)
      result = fd_err(fd_TidyError,"tidy_prim/parse",errbuf.bp,FD_VOID);
    else if (FD_FALSEP(dontfix))
      rc = tidyCleanAndRepair(tdoc);
    else {}
    if (!(FD_VOIDP(result))) {}
    else if (rc<0)
      result = fd_err(fd_TidyError,"tidy_prim/clean",errbuf.bp,FD_VOID);
    else rc = ((tidyOptSetBool(tdoc,TidyForceOutput,yes))?(rc):(-1));
    if (!(FD_VOIDP(result))) {}
    else if (rc<0)
      result = fd_err(fd_TidyError,"tidy_prim/forceout",errbuf.bp,FD_VOID);
    else rc = tidySaveBuffer(tdoc,&outbuf);
    if (!(FD_VOIDP(result))) {}
    else if (rc<0)
      result = fd_err(fd_TidyError,"tidy_prim/output",errbuf.bp,FD_VOID);
    else result = lispval_string(outbuf.bp);
    if ((!((FD_VOIDP(diag))||(FD_FALSEP(diag))))&&((rc>0)||(rc<0))) {
      int drc = tidyRunDiagnostics(tdoc);
      if (drc<0) u8_log(LOG_CRIT,"TIDY/diagfail","%s",errbuf.bp);
      else if (FD_APPLICABLEP(diag)) {
        lispval arg = lispval_string(errbuf.bp);
        lispval dresult = fd_apply(diag,1,&arg);
        if (FD_ABORTP(dresult)) {
          fd_decref(result); result = dresult;}
        else fd_decref(dresult);}
      else u8_log(LOG_WARNING,"TIDY","%s",errbuf.bp);}}
  tidyBufFree(&outbuf);
  tidyBufFree(&errbuf);
  tidyRelease(tdoc);
  return result;
}

static lispval tidy_prim(lispval string,lispval opts,lispval diag)
{
  return tidy_prim_helper(string,opts,diag,1,-1);
}
static lispval tidy_indent_prim(lispval string,lispval opts,lispval diag)
{
  return tidy_prim_helper(string,opts,diag,0,-1);
}
static lispval tidy_html_prim(lispval string,lispval opts,lispval diag)
{
  return tidy_prim_helper(string,opts,diag,1,0);
}
static lispval tidy_xhtml_prim(lispval string,lispval opts,lispval diag)
{
  return tidy_prim_helper(string,opts,diag,1,1);
}


FD_EXPORT int fd_init_tidy()
{
  lispval tidy_module;
  if (tidy_init) return 0;

  tidy_init = u8_millitime();
  doctype_symbol = fd_intern("DOCTYPE");
  dontfix_symbol = fd_intern("DONTFIX");
  xhtml_symbol = fd_intern("XHTML");
  wrap_symbol = fd_intern("WRAP");
  tidy_module = fd_new_cmodule("TIDY",(FD_MODULE_SAFE),fd_init_tidy);

  fd_idefn(tidy_module,
           fd_make_cprim3x("TIDY5",tidy_prim,1,
                           fd_string_type,FD_VOID,-1,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(tidy_module,
           fd_make_cprim3x("TIDY->XHTML",tidy_xhtml_prim,1,
                           fd_string_type,FD_VOID,-1,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(tidy_module,
           fd_make_cprim3x("TIDY->INDENT",tidy_indent_prim,1,
                           fd_string_type,FD_VOID,-1,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(tidy_module,
           fd_make_cprim3x("TIDY->HTML",tidy_html_prim,1,
                           fd_string_type,FD_VOID,-1,FD_VOID,
                           -1,FD_VOID));

  fd_finish_module(tidy_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
