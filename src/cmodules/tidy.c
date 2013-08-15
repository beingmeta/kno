/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "libu8/u8logging.h"

#if HAVE_TIDY_H
#include <tidy.h>
#include <buffio.h>
#include <tidyenum.h>
#elif HAVE_TIDY_TIDY_H
#include <tidy/tidy.h>
#include <tidy/buffio.h>
#include <tidy/tidyenum.h>
#else
#define UNTIDY_HEADERS 1
#endif

#ifndef UNTIDY_HEADERS
FD_EXPORT int fd_init_tidy(void) FD_LIBINIT_FN;
fd_exception fd_TidyError=_("Tidy Error");
static int tidy_init=0;

static fdtype tidy_prim(fdtype string,fdtype opts,fdtype diag)
{
  fdtype result=FD_VOID;
  TidyBuffer outbuf={NULL};
  TidyBuffer errbuf={NULL};
  int rc=-1;
  TidyDoc tdoc=tidyCreate();
  Bool ok=tidyOptSetBool(tdoc,TidyXhtmlOut,yes);
  if (ok) {
    tidyBufInit(&outbuf);
    tidyBufInit(&errbuf);
    rc=tidySetErrorBuffer(tdoc,&errbuf);}
  if (rc<0) {
    tidyRelease(tdoc);
    return fd_err(fd_TidyError,"tidy_prim/init",NULL,FD_VOID);}
  if ((FD_VOIDP(opts))||(FD_FALSEP(opts))) {
    if (rc>=0) rc=tidyOptSetValue(tdoc,TidyCharEncoding,"utf8");
    if (rc>=0) rc=tidyOptSetInt(tdoc,TidyDoctypeMode,TidyDoctypeOmit);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyShowWarnings,no);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyQuiet,yes);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyMakeBare,yes);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyMakeClean,yes);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyDropEmptyParas,yes);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyFixComments,yes);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyXmlSpace,yes);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyEncloseBodyText,yes);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyEncloseBlockText,yes);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyWord2000,yes);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyMark,yes);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyJoinClasses,yes);
    if (rc>=0) rc=tidyOptSetBool(tdoc,TidyNCR,yes);}
  else {}
  if (rc<0) result=fd_err(fd_TidyError,"tidy_prim/setopts",errbuf.bp,FD_VOID);
  else {
    rc=tidyParseString(tdoc,FD_STRDATA(string));
    if (rc<0)
      result=fd_err(fd_TidyError,"tidy_prim/parse",errbuf.bp,FD_VOID);
    else rc=tidyCleanAndRepair(tdoc);
    if (!(FD_VOIDP(result))) {}
    else if (rc<0)
      result=fd_err(fd_TidyError,"tidy_prim/clean",errbuf.bp,FD_VOID);
    else rc=((tidyOptSetBool(tdoc,TidyForceOutput,yes))?(rc):(-1));
    if (!(FD_VOIDP(result))) {}
    else if (rc<0)
      result=fd_err(fd_TidyError,"tidy_prim/forceout",errbuf.bp,FD_VOID);
    else rc=tidySaveBuffer(tdoc,&outbuf);
    if (!(FD_VOIDP(result))) {}
    else if (rc<0)
      result=fd_err(fd_TidyError,"tidy_prim/save",errbuf.bp,FD_VOID);
    else result=fdtype_string(outbuf.bp);
    if ((!((FD_VOIDP(diag))||(FD_FALSEP(diag))))&&((rc>0)||(rc<0))) {
      int drc=tidyRunDiagnostics(tdoc);
      if (drc>=0) u8_log(LOG_WARNING,"TIDY","%s",errbuf.bp);
      else u8_log(LOG_CRIT,"TIDY/diagfail","%s",errbuf.bp);}}
  tidyBufFree(&outbuf);
  tidyBufFree(&errbuf);
  tidyRelease(tdoc);
  return result;
}

FD_EXPORT int fd_init_tidy()
{
  fdtype tidy_module;
  if (tidy_init) return 0;

  tidy_init=1;
  tidy_module=fd_new_module("TIDY",(FD_MODULE_SAFE));

  fd_idefn(tidy_module,
	   fd_make_cprim3x("TIDY->XHTML",tidy_prim,1,
			   fd_string_type,FD_VOID,-1,FD_VOID,
			   -1,FD_VOID));

  fd_finish_module(tidy_module);
  fd_persist_module(tidy_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}
#endif /* ndef UNTIDY_HEADERS */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
