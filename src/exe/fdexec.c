/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDEXEC_INCLUDED
static char versionid[] =
  "$Id$";
#endif

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/fddb.h"
#include "fdb/dbfile.h"
#include "fdb/eval.h"
#include "fdb/ports.h"

#include <libu8/libu8.h>
#include <libu8/u8timefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8fileio.h>
#include <libu8/u8stdio.h>
#include <libu8/u8printf.h>

#include <stdlib.h>
#include <stdio.h>
#include <locale.h>
#include <strings.h>
#include <sys/time.h>
#include <time.h>

FD_EXPORT void fd_init_fdweb(void);
FD_EXPORT void fd_init_texttools(void);
FD_EXPORT void fd_init_tagger(void);

#define MAX_CONFIGS 256

static int debug_maxelts=32, debug_maxchars=80;

static char *configs[MAX_CONFIGS], *exe_arg=NULL, *file_arg=NULL;
static int n_configs=0;

static u8_condition FileWait=_("FILEWAIT");

static void identify_application(int argc,char **argv,char *dflt)
{
  int i=1;
  while (i<argc)
    if (strchr(argv[i],'=')) i++;
    else {
      char *copy=u8_strdup(argv[i]);
      char *dot=strchr(copy,'.');
      if (dot) *dot='\0';
      u8_identify_application(copy);
      u8_free(copy);
      return;}
  u8_identify_application(dflt);
}

typedef char *charp;

static fdtype chain_prim(int n,fdtype *args)
{
  if (n_configs>=MAX_CONFIGS)
    return fd_err(_("Too many configs to CHAIN"),"chain_prim",NULL,FD_VOID);
  else {
    int i=0, cargc=0;
    /* This stream will contain the chaining message */
    struct U8_OUTPUT argstring;
    char **cargv=u8_alloc_n(n+n_configs+3,charp); 
    U8_INIT_OUTPUT(&argstring,512);
    cargv[cargc++]=exe_arg;
    cargv[cargc++]=file_arg;
    i=0; while (i<n)
      if (FD_STRINGP(args[i])) {
	u8_printf(&argstring," %s",FD_STRDATA(args[i]));
	cargv[cargc]=u8_tolibc(FD_STRDATA(args[i]));
	i++; cargc++;}
      else {
	u8_string as_string=fd_dtype2string(args[i]);
	char *libc_string=u8_tolibc(as_string);
	u8_printf(&argstring," %s",as_string);
	u8_free(as_string);
	cargv[cargc]=libc_string;
	i++; cargc++;}
    i=0; while (i<n_configs) {
      u8_printf(&argstring," %s",u8_fromlibc(configs[i]));
      cargv[cargc++]=configs[i++];}
    cargv[cargc++]=NULL;
    u8_log(LOG_NOTICE,"CHAIN",">> %s%s",u8_fromlibc(file_arg),argstring.u8_outbuf);
    u8_free(argstring.u8_outbuf);
    fflush(stdout); fflush(stderr);
    fd_close_pools();
    fd_close_indices();
    return execvp(exe_arg,cargv);}
}

static u8_string wait_for_file=NULL;

int main(int argc,char **argv)
{
  unsigned char data[1024], *input;
  u8_string source_file=NULL;
  fd_lispenv env=fd_working_environment();
  fdtype main_proc=FD_VOID, result=FD_VOID;
  fdtype *args=u8_alloc_n(argc,fdtype);
  int i=1, n_args=0, retval=0;
  if (argc<2) {
    fprintf(stderr,"Usage: fdexec filename [config=val]*\n");
    return -1;}
  if (exe_arg==NULL) exe_arg=u8_strdup(argv[0]);
  fd_register_source_file(versionid);
  fd_register_config("DEBUGMAXCHARS",
		     _("Max number of chars in strings output with error reports"),
		     fd_intconfig_get,fd_intconfig_set,
		     &debug_maxchars);
  fd_register_config("DEBUGMAXELTS",
		     _("Max number of elements in vectors/choices/lists output with error reports"),
		     fd_intconfig_get,fd_intconfig_set,
		     &debug_maxelts);
  fd_register_config("FILEWAIT",
		     _("File to wait to exist before starting"),
		     fd_sconfig_get,fd_sconfig_set,
		     &wait_for_file);
  setlocale(LC_ALL,"");
  /* Process command line config arguments */
#ifndef FDEXEC_INCLUDED
  fd_argv_config(argc,argv);
#endif

  /* INITIALIZING MODULES */
  /* Normally, modules have initialization functions called when
     dynamically loaded.  However, if we are statically linked, or we
     don't have the "constructor attributes" use to declare init functions,
     we need to call some initializers explicitly. */

  /* Initialize the libu8 stdio library if it won't happen automatically. */
#if (!(HAVE_CONSTRUCTOR_ATTRIBUTES))
  u8_initialize_u8stdio();
  u8_init_chardata_c();
#endif

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (FD_TESTCONFIG))
  fd_init_fdscheme();
  fd_init_texttools();
  fd_init_tagger();
  fd_init_fdweb();
#else
  FD_INIT_SCHEME_BUILTINS();
#endif

  fd_init_schemeio();
  identify_application(argc,argv,"fdexec");
  if (wait_for_file)
    if (u8_file_existsp(wait_for_file))
      u8_log(LOG_NOTICE,FileWait,"Starting now because '%s' exists",wait_for_file);
    else {
      int n=0;  u8_log(LOG_NOTICE,FileWait,"Waiting for '%s' to exist",wait_for_file);
      while (1) {
	n++; if (n<15) sleep(n); else sleep(15);
	if (u8_file_existsp(wait_for_file)) {
	  u8_log(LOG_NOTICE,FileWait,"[%d] Starting now because '%s' exists",n,wait_for_file);
	  break;}
	else if ((n<15) ? ((n%4)==0) : ((n%20)==0))
	  u8_log(LOG_NOTICE,FileWait,"[%d] Waiting for '%s' to exist",n,wait_for_file);}}
  
  fd_idefn((fdtype)env,fd_make_cprimn("CHAIN",chain_prim,0));
  while (i<argc)
    if (strchr(argv[i],'=')) {
      /* Record but pass command line configs */
      if (n_configs>=MAX_CONFIGS) n_configs++;
      else configs[n_configs++]=u8_strdup(argv[i]);
      i++;}
    else if (source_file)
      args[n_args++]=fd_parse_arg(argv[i++]);
    else {
      file_arg=u8_strdup(argv[i]);
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
      int ctype=FD_PRIM_TYPE(main_proc);
      fd_decref(result);
      result=fd_applyfns[ctype](main_proc,n_args,args);}}
  if (FD_ERRORP(result)) {
    struct FD_EXCEPTION_OBJECT *e=(struct FD_EXCEPTION_OBJECT *)result;
    int old_maxelts=fd_unparse_maxelts, old_maxchars=fd_unparse_maxchars;
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,512);
    fd_unparse_maxchars=debug_maxchars; fd_unparse_maxelts=debug_maxelts;
    fd_print_error(&out,e);
    fd_print_backtrace(&out,80,e->backtrace);
    fd_print_error(&out,e);
    fd_unparse_maxelts=old_maxelts; fd_unparse_maxchars=old_maxchars;
    fputs(out.u8_outbuf,stderr);
    u8_free(out.u8_outbuf);
    retval=-1;}
  else if (FD_TROUBLEP(result)) {
    fd_exception ex; u8_context cxt; u8_string details; fdtype irritant;
    if (fd_poperr(&ex,&cxt,&details,&irritant)) {
      u8_fprintf(stderr,";; (ERROR %m)",ex);
      if (details) u8_fprintf(stderr," %m",details);
      if (cxt) u8_fprintf(stderr," (%s)",cxt);
      u8_fprintf(stderr,"\n");
      if (!(FD_VOIDP(irritant)))
	u8_fprintf(stderr,";; %q\n",irritant);
      if (details) u8_free(details); fd_decref(irritant);}
    else u8_fprintf(stderr,";; Unexplained error result %q\n",result);
    retval=-1;}
  fd_decref(result);
  fd_recycle_environment(env);
  i=0; while (i<n_args) {fd_decref(args[i]); i++;}
  u8_free(args);
  return retval;
}


/* The CVS log for this file
   $Log: fdexec.c,v $
   Revision 1.28  2006/02/10 14:27:31  haase
   Added CONFIGurable debug unparse length limits

   Revision 1.27  2006/01/27 22:07:39  haase
   Streamlined code and fixed some one-time leaks in fdexec

   Revision 1.26  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.25  2006/01/21 21:11:26  haase
   Removed some leaks associated with reifying error states as objects

   Revision 1.24  2005/08/21 02:18:23  haase
   Added usage message for fdexec with no args

   Revision 1.23  2005/08/11 12:47:51  haase
   fdexec uses builtins declaration

   Revision 1.22  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.21  2005/08/10 05:47:43  haase
   Undid previous rename of executables

   Revision 1.1  2005/08/05 10:19:57  haase
   Added fdbservlet and did some executable renames as part of the big FDB switch

   Revision 1.19  2005/06/01 13:07:55  haase
   Fixes for less forgiving compilers

   Revision 1.18  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.17  2005/04/24 22:49:21  haase
   Cleaned up executables to use common libraries

   Revision 1.16  2005/04/21 19:07:03  haase
   Reorganized initializations

   Revision 1.15  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.14  2005/04/06 15:16:41  haase
   Added application identification and better command-line argument processing, as well as more config var setting

   Revision 1.13  2005/04/04 22:22:27  haase
   Better error reporting from executables

   Revision 1.12  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.11  2005/03/26 20:06:52  haase
   Exposed APPLY to scheme and made optional arguments generally available

   Revision 1.10  2005/03/26 18:31:41  haase
   Various configuration fixes

   Revision 1.9  2005/03/26 00:16:13  haase
   Made loading facility be generic and moved the rest of file access into fileio.c

   Revision 1.8  2005/03/06 19:26:44  haase
   Plug some leaks and some failures to return values

   Revision 1.7  2005/03/06 02:03:06  haase
   Fixed include statements for system header files

   Revision 1.6  2005/03/05 19:38:39  haase
   Added setlocale call

   Revision 1.5  2005/03/03 17:58:15  haase
   Moved stdio dependencies out of fddb and reorganized make structure

   Revision 1.4  2005/02/15 23:09:51  haase
   Fixed log entries

   Revision 1.3  2005/02/15 22:56:21  haase
   Fixed bug introduced with index cleanups

   Revision 1.2  2005/02/11 04:51:17  haase
   Added version stuff

*/
