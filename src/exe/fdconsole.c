/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"

#include <libu8/u8.h>
#include <libu8/u8io.h>
#include <libu8/xfiles.h>
#include <libu8/timefns.h>
#include <libu8/pathfns.h>
#include <libu8/filefns.h>

#include <stdlib.h>
#include <stdio.h>
#include <locale.h>
#include <strings.h>
#include <sys/time.h>
#include <time.h>

int skip_whitespace(u8_input in)
{
  int c=u8_getc(in);
  while ((c>=0) && (isspace(c))) c=u8_getc(in);
  if (c==';') {
    while ((c>=0) && (c != '\n')) c=u8_getc(in);
    if (c<0) return c;
    else return skip_whitespace(in);}
  else return c;
}

static u8_input inconsole=NULL;
static u8_output outconsole=NULL;
static u8_output errconsole=NULL;
static fd_lispenv console_env=NULL;

static int debug_maxelts=32, debug_maxchars=80;

static void close_consoles()
{
  if (inconsole) {
    u8_close(inconsole);
    inconsole=NULL;}
  if (outconsole) {
    u8_close(outconsole);
    outconsole=NULL;}
  if (errconsole) {
    u8_close(errconsole);
    errconsole=NULL;}
  if (console_env) {
    fd_recycle_environment(console_env);
    console_env=NULL;}
}

int main(int argc,char **argv)
{
  unsigned char data[1024], *input;
  fd_lispenv env=fd_working_environment();
  fdtype lastval=FD_VOID, that_symbol;
  fdtype showtime_config=fd_config_get("SHOWTIME");
  u8_encoding enc=u8_get_default_encoding();
  u8_input in=(u8_input)u8_open_xinput(0,enc);
  u8_output out=(u8_output)u8_open_xoutput(1,enc);
  u8_output err=(u8_output)u8_open_xoutput(2,enc);
  int i=1, c; u8_string source_file=NULL; double showtime=-1.0;
  /* Initialize these primitives */
#if FD_TESTCONFIG
  u8_init_chardata_c();
  fd_init_fdscheme();
  fd_init_schemeio();
  fd_init_fdweb();
  fd_init_texttools();
#else
  FD_INIT_SCHEME_BUILTINS();
#endif
  fd_register_config("DEBUGMAXCHARS",fd_intconfig_get,fd_intconfig_set,
		     &debug_maxchars);
  fd_register_config("DEBUGMAXELTS",fd_intconfig_get,fd_intconfig_set,
		     &debug_maxelts);
  inconsole=in;
  outconsole=out;
  errconsole=err;
  console_env=env;
  atexit(close_consoles);
  u8_identify_application(argv[0]);
  fd_config_set("OIDDISPLAY",FD_INT2DTYPE(3));
  setlocale(LC_ALL,"");
  that_symbol=fd_intern("THAT");
  while (i<argc)
    if (strchr(argv[i],'=')) 
      fd_config_assignment(argv[i++]);
    else if (source_file) i++;
    else source_file=argv[i++];
  if (source_file) {
    fdtype sourceval=fdtype_string(u8_realpath(source_file,NULL));
    fd_config_set("SOURCE",sourceval); fd_decref(sourceval);
    fd_load_source(source_file,env,NULL);}
  {
    fdtype interpval=fd_init_string(NULL,-1,u8_fromlibc(argv[0]));
    fd_config_set("INTERPRETER",interpval); fd_decref(interpval);}
  if (FD_FIXNUMP(showtime_config))
    showtime=FD_FIX2INT(showtime_config);
  else if (FD_FLONUMP(showtime_config))
    showtime=FD_FLONUM(showtime_config);
  fd_decref(showtime_config);
  u8_printf(out,"Eval: "); u8_flush(out);
  while ((c=skip_whitespace((u8_input)in))>=0) {
    int start_icache, finish_icache;
    int start_ocache, finish_ocache;
    double start_time, finish_time;
    fdtype result, expr;
    start_ocache=fd_cachecount_pools();
    start_icache=fd_cachecount_indices();
    if (c == '=') {
      fdtype sym=fd_parser((u8_input)in,NULL);
      if (FD_SYMBOLP(sym)) {
	fd_bind_value(sym,lastval,env);
	u8_printf(out,_(";; Assigned %s\n"),FD_SYMBOL_NAME(sym));}
      else u8_printf(out,_(";; Bad assignment expression\n"));
      u8_printf(out,"Eval: "); u8_flush(out);
      fd_decref(sym); continue;}
    else u8_ungetc(((u8_input)in),c);
    expr=fd_parser((u8_input)in,NULL);
    if ((FD_EOFP(expr)) || (FD_EOXP(expr))) break;
    if (FD_OIDP(expr)) {
      fdtype v=fd_oid_value(expr);
      if (FD_TABLEP(v)) {
	U8_OUTPUT out; U8_INIT_OUTPUT(&out,4096);
	u8_printf(&out,"%q:\n",expr);
	fd_display_table(&out,v,FD_VOID);
	fputs(out.bytes,stdout); u8_free(out.bytes);}
      else u8_printf(out,"OID value: %q\n",v);
      fd_decref(v);
      u8_printf(out,_("Eval: ")); u8_flush(out);
      continue;}
    start_time=u8_elapsed_time();
    result=fd_eval(expr,env);
    finish_time=u8_elapsed_time();
    fd_decref(expr);
    if (FD_CHECK_PTR(result)==0) {
      fprintf(stderr,";;; The expression returned an invalid pointer!!!!\n");}
    else if (FD_EXCEPTIONP(result)) {
      struct FD_EXCEPTION_OBJECT *e=(struct FD_EXCEPTION_OBJECT *)result;
      int old_maxelts=fd_unparse_maxelts, old_maxchars=fd_unparse_maxchars;
      U8_OUTPUT out; U8_INIT_OUTPUT(&out,512);
      fd_unparse_maxchars=debug_maxchars; fd_unparse_maxelts=debug_maxelts;
      fd_print_error(&out,e);
      fd_print_backtrace(&out,80,e->backtrace);
      fd_print_error(&out,e);
      fd_unparse_maxelts=old_maxelts; fd_unparse_maxchars=old_maxchars;
      fputs(out.bytes,stderr);
      u8_free(out.bytes);}
    else if (FD_TROUBLEP(result)) {
      fd_exception ex; u8_context cxt; u8_string details; fdtype irritant;
      if (fd_poperr(&ex,&cxt,&details,&irritant)) {
	u8_fprintf(stderr,";; (ERROR %m)",ex);
	if (details) u8_fprintf(stderr," %m",details);
	if (cxt) u8_fprintf(stderr," (%s)",cxt);
	u8_fprintf(stderr,"\n");
	if (!(FD_VOIDP(irritant)))
	  u8_fprintf(stderr,";; %q\n",irritant);}
      else u8_fprintf(stderr,";; Unexplained error result %q\n",result);
      if (details) u8_free(details); fd_decref(irritant);}
    else if (FD_VOIDP(result)) {}
    else if ((FD_CHOICEP(result)) || (FD_ACHOICEP(result)))
      if (FD_CHOICE_SIZE(result)<3)
	u8_printf(out,"%q\n",result);
      else {
	int n=FD_CHOICE_SIZE(result);
	u8_printf(out,_("{ ;; %d results\n"),n);
	{FD_DO_CHOICES(elt,result) {
	  u8_printf(out,"  %q\n",elt);}}
	u8_printf(out,_("} ;; %d results\n"),n);}
    else u8_printf(out,"%q\n",result);
    finish_ocache=fd_cachecount_pools();
    finish_icache=fd_cachecount_indices();
    if (((showtime>=0.0) && ((finish_time-start_time)>showtime)) ||
	(finish_ocache!=start_ocache) ||
	(finish_icache!=start_icache))
      u8_printf(out,_(";; Evaluation took %f seconds, %d object loads, %d index loads\n"),
		(finish_time-start_time),
		finish_ocache-start_ocache,
		finish_icache-start_icache);
    fd_clear_errors(1);
    fd_decref(lastval);
    lastval=result;
    if ((FD_CHECK_PTR(lastval)) &&
	(!(FD_ABORTP(lastval))) &&
	(!(FDTYPE_CONSTANTP(lastval))))
      fd_bind_value(that_symbol,lastval,env);
    u8_printf(out,_("Eval: "));
    u8_flush(out);}
  fd_decref(lastval);
  return 0;
}


/* The CVS log for this file
   $Log: fdconsole.c,v $
   Revision 1.33  2006/02/10 14:27:31  haase
   Added CONFIGurable debug unparse length limits

   Revision 1.32  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.31  2006/01/25 02:32:27  haase
   Fixed some end of session leaks in fdconsole

   Revision 1.30  2006/01/21 21:11:26  haase
   Removed some leaks associated with reifying error states as objects

   Revision 1.29  2006/01/16 22:16:31  haase
   Made fdconsole(s) handle achoice return values by listing them

   Revision 1.28  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.27  2005/06/06 01:53:25  haase
   Add builtin inits to fdconsole

   Revision 1.26  2005/06/04 19:50:41  haase
   Don't save errors into THAT

   Revision 1.25  2005/05/30 01:28:28  haase
   Made fdconsole init OIDDISPLAY to 3 and and removed redundant explicit POOLS and INDICES config

   Revision 1.24  2005/05/27 20:50:10  haase
   Made fdconsole bind THAT to immediate non-constants

   Revision 1.23  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.22  2005/04/25 22:02:18  haase
   Made fdconsole report object/index loads

   Revision 1.21  2005/04/24 22:49:21  haase
   Cleaned up executables to use common libraries

   Revision 1.20  2005/04/21 19:07:03  haase
   Reorganized initializations

   Revision 1.19  2005/04/17 17:05:57  haase
   Call fileio init explicitly

   Revision 1.18  2005/04/17 12:39:40  haase
   Fix error reporting in console loop

   Revision 1.17  2005/04/12 00:39:53  haase
   Added that binding and =var functionality to fdconsole

   Revision 1.16  2005/04/11 21:30:14  haase
   Don't try to print invalid pointers in fdconsole

   Revision 1.15  2005/04/08 04:49:17  haase
   Better backtrace printing

   Revision 1.14  2005/04/07 20:03:12  haase
   Better backtrace printing

   Revision 1.13  2005/04/06 15:24:57  haase
   Added application identification and better command-line argument processing, as well as more config var setting

   Revision 1.12  2005/04/04 22:22:27  haase
   Better error reporting from executables

   Revision 1.11  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.10  2005/03/26 18:31:41  haase
   Various configuration fixes

   Revision 1.9  2005/03/26 00:16:13  haase
   Made loading facility be generic and moved the rest of file access into fileio.c

   Revision 1.8  2005/03/06 02:03:06  haase
   Fixed include statements for system header files

   Revision 1.7  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.6  2005/03/05 19:38:39  haase
   Added setlocale call

   Revision 1.5  2005/02/28 03:20:01  haase
   Added optional load file and config processing to fdconsole

   Revision 1.4  2005/02/24 19:12:46  haase
   Fixes to handling index arguments which are strings specifiying index sources

   Revision 1.3  2005/02/16 02:34:26  haase
   Made real fdconsole use u8 xfiles

   Revision 1.17  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
