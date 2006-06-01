/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: fdconsole.c,v 1.54 2006/02/10 14:27:31 haase Exp $";

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <libu8/u8.h>
#include <libu8/timefns.h>
#include <libu8/filefns.h>
#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"

static int debug_maxelts=32, debug_maxchars=80;

FD_EXPORT void fd_list_table(void);

FD_EXPORT void fd_print_backtrace(U8_OUTPUT *out,int,fdtype bt);
FD_EXPORT void fd_print_error(U8_OUTPUT *out,FD_EXCEPTION_OBJECT *e);

int main(int argc,char **argv)
{
  int fd_version=fd_init_fdscheme();
  unsigned char data[1024*16], *input;
  fd_lispenv env=fd_working_environment();
  fdtype lastval=FD_VOID, showtime_config=fd_config_get("SHOWTIME");
  fdtype that_symbol;
  double showtime=-1.0;
  int i=1; u8_string source_file=NULL;
  /* Initialize these primitives */
  fd_init_fdscheme(); fd_init_schemeio();
  fd_init_texttools(); fd_init_fdweb();
  u8_init_chardata_c();
  fd_register_config("DEBUGMAXCHARS",fd_intconfig_get,fd_intconfig_set,
		     &debug_maxchars);
  fd_register_config("DEBUGMAXELTS",fd_intconfig_get,fd_intconfig_set,
		     &debug_maxelts);
  u8_default_appid("fdconsole");
  fd_config_set("OIDDISPLAY",FD_INT2DTYPE(3));
  that_symbol=fd_intern("THAT");
  while (i<argc)
    if (strchr(argv[i],'=')) 
      fd_config_assignment(argv[i++]);
    else if (source_file) i++;
    else source_file=u8_fromlibc(argv[i++]);
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
  fprintf(stdout,_("Eval: ")); fflush(stdout);
  while (input=fgets(data,1024*16,stdin))
    if (input[0]=='=') {
      fdtype sym=fd_parse(input+1);
      if (FD_SYMBOLP(sym)) {
	fd_bind_value(sym,lastval,env);
	fprintf(stdout,_(";; Assigned %s\n"),FD_SYMBOL_NAME(sym));}
      else fprintf(stdout,_(";; Bad assignment expression\n"));
      fprintf(stdout,_("Eval: ")); fflush(stdout);}
    else {
      int start_icache, finish_icache;
      int start_ocache, finish_ocache;
      double start_time, finish_time;
      fdtype expr, result;
      start_ocache=fd_cachecount_pools();
      start_icache=fd_cachecount_indices();
      expr=fd_parse(input);
      if (FD_OIDP(expr)) {
	fdtype v=fd_oid_value(expr);
	if (FD_TABLEP(v)) {
	  U8_OUTPUT out; U8_INIT_OUTPUT(&out,4096);
	  u8_printf(&out,"%q:\n",expr);
	  fd_display_table(&out,v,FD_VOID);
	  fputs(out.bytes,stdout); u8_free(out.bytes);}
	else u8_fprintf(stdout,"OID Value: %q\n",v);
	fd_decref(v);
	fprintf(stdout,_("Eval: "));
	fflush(stdout);
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
	if (details) u8_free(details); fd_incref(irritant);}
      else if (FD_VOIDP(result)) {}
      else if ((FD_CHOICEP(result)) || (FD_ACHOICEP(result)))
	if (FD_CHOICE_SIZE(result)<3)
	  u8_fprintf(stdout,"%q\n",result);
	else {
	  int n=FD_CHOICE_SIZE(result);
	  u8_fprintf(stdout,_("{ ;; %d results\n"),n);
	  {FD_DO_CHOICES(elt,result) {
	    u8_fprintf(stdout,"  %q\n",elt);}}
	  u8_fprintf(stdout,_("} ;; %d results\n"),n);}
      else u8_fprintf(stdout,"%q\n",result);
      finish_ocache=fd_cachecount_pools();
      finish_icache=fd_cachecount_indices();
      if (((showtime>=0.0) && ((finish_time-start_time)>showtime)) ||
	  (finish_ocache!=start_ocache) ||
	  (finish_icache!=start_icache))
	u8_fprintf
	  (stdout,
	   _(";; Evaluation took %f seconds, %d object loads, %d index loads\n"),
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
      fprintf(stdout,_("Eval: "));
      fflush(stdout);}
  fd_recycle_environment(env);
  fd_decref(lastval);
  return 0;
}


/* The CVS log for this file
   $Log: fdconsole.c,v $
   Revision 1.54  2006/02/10 14:27:31  haase
   Added CONFIGurable debug unparse length limits

   Revision 1.53  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.52  2006/01/21 21:11:26  haase
   Removed some leaks associated with reifying error states as objects

   Revision 1.51  2006/01/16 22:16:31  haase
   Made fdconsole(s) handle achoice return values by listing them

   Revision 1.50  2005/12/22 19:15:50  haase
   Added more comprehensive environment recycling

   Revision 1.49  2005/12/20 18:03:58  haase
   Made test executables include all libraries statically

   Revision 1.48  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.47  2005/07/08 20:20:14  haase
   Enlarged read buffer

   Revision 1.46  2005/06/15 13:00:01  haase
   Fixed wraparound error for hashsets

   Revision 1.45  2005/06/04 19:50:41  haase
   Don't save errors into THAT

   Revision 1.44  2005/05/30 01:28:28  haase
   Made fdconsole init OIDDISPLAY to 3 and and removed redundant explicit POOLS and INDICES config

   Revision 1.43  2005/05/27 20:50:10  haase
   Made fdconsole bind THAT to immediate non-constants

   Revision 1.42  2005/05/04 09:11:44  haase
   Moved FD_INIT_SCHEME_BUILTINS into individual executables in order to dance around an OS X dependency problem

   Revision 1.41  2005/05/02 15:03:23  haase
   Reorganized conditional compilation

   Revision 1.40  2005/04/30 12:45:03  haase
   Added CALLTRACK, an internal profiling mechanism

   Revision 1.39  2005/04/25 22:02:18  haase
   Made fdconsole report object/index loads

   Revision 1.38  2005/04/24 22:49:21  haase
   Cleaned up executables to use common libraries

   Revision 1.37  2005/04/24 02:25:25  haase
   Added new error and backtrace functions

   Revision 1.36  2005/04/21 19:08:23  haase
   Add schemeio initialization

   Revision 1.35  2005/04/17 17:05:57  haase
   Call fileio init explicitly

   Revision 1.34  2005/04/17 12:39:40  haase
   Fix error reporting in console loop

   Revision 1.33  2005/04/12 00:39:53  haase
   Added that binding and =var functionality to fdconsole

   Revision 1.32  2005/04/11 21:30:14  haase
   Don't try to print invalid pointers in fdconsole

   Revision 1.31  2005/04/08 04:49:17  haase
   Better backtrace printing

   Revision 1.30  2005/04/07 20:03:12  haase
   Better backtrace printing

   Revision 1.29  2005/04/06 15:24:57  haase
   Added application identification and better command-line argument processing, as well as more config var setting

   Revision 1.28  2005/04/04 22:22:27  haase
   Better error reporting from executables

   Revision 1.27  2005/04/02 16:05:12  haase
   Removed symbol table dumping

   Revision 1.26  2005/04/01 15:13:02  haase
   Various fixes for profiling

   Revision 1.25  2005/03/30 14:48:44  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.24  2005/03/26 00:16:13  haase
   Made loading facility be generic and moved the rest of file access into fileio.c

   Revision 1.23  2005/03/23 21:47:29  haase
   Added return values for use pool/index primitives and added CONFIG interface to pool and index usage

   Revision 1.22  2005/03/06 02:03:06  haase
   Fixed include statements for system header files

   Revision 1.21  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.20  2005/03/05 19:38:39  haase
   Added setlocale call

   Revision 1.19  2005/02/28 03:20:01  haase
   Added optional load file and config processing to fdconsole

   Revision 1.18  2005/02/24 19:12:46  haase
   Fixes to handling index arguments which are strings specifiying index sources

   Revision 1.17  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
