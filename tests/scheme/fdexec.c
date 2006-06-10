/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

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

static void print_backtrace(fdtype bt)
{
  if (FD_PAIRP(bt)) {
    print_backtrace(FD_CDR(bt));
    u8_fprintf(stderr,";;  >> %q\n",FD_CAR(bt));}
}

int main(int argc,char **argv)
{
  int fd_version=fd_init_fdscheme();
  unsigned char data[1024], *input;
  fd_lispenv env=fd_working_environment();
  fdtype main_proc=FD_VOID, result=FD_VOID;
  fdtype *args=u8_malloc(sizeof(fdtype)*argc);
  int i=1, n_args=0;
  u8_string source_file=NULL;
  /* Initialize these primitives */
  fd_init_fdscheme(); fd_init_schemeio();
  fd_init_texttools(); fd_init_fdweb();
  u8_init_chardata_c();
  fd_register_config("DEBUGMAXCHARS",fd_intconfig_get,fd_intconfig_set,
		     &debug_maxchars);
  fd_register_config("DEBUGMAXELTS",fd_intconfig_get,fd_intconfig_set,
		     &debug_maxelts);
  fd_register_source_file(versionid);
  while (i<argc)
    if (strchr(argv[i],'=')) 
      fd_config_assignment(argv[i++]);
    else if (source_file)
      args[n_args++]=fd_parse_arg(argv[i++]);
    else {
      source_file=u8_fromlibc(argv[i++]);
      u8_default_appid(source_file);}
  if (source_file) {
    fdtype interp=fd_init_string(NULL,-1,u8_fromlibc(argv[0]));
    fdtype src=fd_init_string(NULL,-1,u8_realpath(source_file,NULL));
    result=fd_load_source(source_file,env,NULL);
    fd_config_set("INTERPRETER",interp);
    fd_config_set("SOURCE",src);
    fd_decref(src); fd_decref(interp); u8_free(source_file);
    source_file=NULL;}
  else {
    fprintf(stderr,
	    "Usage: fdexec [conf=val]* source_file (arg | [conf=val])*\n");
    return 1;}
  if (!(FD_ABORTP(result))) {
    main_proc=fd_symeval(fd_intern("MAIN"),env);
    if (FD_APPLICABLEP(main_proc)) {
      int ctype=FD_PTR_TYPE(main_proc);
      fd_decref(result);
      result=fd_applyfns[ctype](main_proc,n_args,args);
      fd_decref(main_proc);}}
  fd_recycle_environment(env);
  {int j=0; while (j<n_args) {fd_decref(args[j]); j++;} u8_free(args);}
  if (FD_EXCEPTIONP(result)) {
    struct FD_EXCEPTION_OBJECT *e=(struct FD_EXCEPTION_OBJECT *)result;
    int old_maxelts=fd_unparse_maxelts, old_maxchars=fd_unparse_maxchars;
    fd_unparse_maxchars=debug_maxchars; fd_unparse_maxelts=debug_maxelts;
    u8_fprintf(stderr,";; (ERROR %m)",e->data.cond);
    if (e->data.details) u8_fprintf(stderr," %m",e->data.details);
    if (e->data.cxt) u8_fprintf(stderr," (%s)",e->data.cxt);
    u8_fprintf(stderr,"\n");
    if (!(FD_VOIDP(e->data.irritant)))
      u8_fprintf(stderr,";; %q\n",e->data.irritant);
    print_backtrace(e->backtrace);
    fd_unparse_maxelts=old_maxelts; fd_unparse_maxchars=old_maxchars;
    fd_decref(result);
    return -1;}
  else if (FD_TROUBLEP(result)) {
    fd_exception ex; u8_context cxt; u8_string details; fdtype irritant;
    u8_fprintf(stderr,";; (ERROR %m)",ex);
    if (details) u8_fprintf(stderr," %m",details);
    if (cxt) u8_fprintf(stderr," (%s)",cxt);
    u8_fprintf(stderr,"\n");
    if (!(FD_VOIDP(irritant)))
      u8_fprintf(stderr,";; %q\n",irritant);
    fd_decref(result);
    return -1;}
  fd_decref(result);
  fd_clear_errors(1);
  return 0;
}


/* The CVS log for this file
   $Log: fdexec.c,v $
   Revision 1.24  2006/02/10 14:27:31  haase
   Added CONFIGurable debug unparse length limits

   Revision 1.23  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.22  2006/01/21 20:15:20  haase
   Added ability to have transformation functions in SUBST matcher expressions

   Revision 1.21  2005/12/22 19:15:50  haase
   Added more comprehensive environment recycling

   Revision 1.20  2005/12/20 18:03:58  haase
   Made test executables include all libraries statically

   Revision 1.19  2005/12/19 00:47:43  haase
   Various GC and error reporting fixes

   Revision 1.18  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.17  2005/05/04 09:11:44  haase
   Moved FD_INIT_SCHEME_BUILTINS into individual executables in order to dance around an OS X dependency problem

   Revision 1.16  2005/04/21 19:08:23  haase
   Add schemeio initialization

   Revision 1.15  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.14  2005/04/06 15:16:41  haase
   Added application identification and better command-line argument processing, as well as more config var setting

   Revision 1.13  2005/04/04 22:22:27  haase
   Better error reporting from executables

   Revision 1.12  2005/03/30 14:48:44  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.11  2005/03/26 20:06:52  haase
   Exposed APPLY to scheme and made optional arguments generally available

   Revision 1.10  2005/03/26 00:16:13  haase
   Made loading facility be generic and moved the rest of file access into fileio.c

   Revision 1.9  2005/03/23 21:47:29  haase
   Added return values for use pool/index primitives and added CONFIG interface to pool and index usage

   Revision 1.8  2005/03/06 02:03:06  haase
   Fixed include statements for system header files

   Revision 1.7  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.6  2005/03/05 19:38:39  haase
   Added setlocale call

   Revision 1.5  2005/02/15 23:09:51  haase
   Fixed log entries

   Revision 1.4  2005/02/15 22:56:21  haase
   Fixed bug introduced with index cleanups

   Revision 1.3  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
