/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2014 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDEXEC_INCLUDED
#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/fddb.h"
#include "framerd/dbfile.h"
#include "framerd/eval.h"
#include "framerd/ports.h"

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

#include "main.h"

#define MAX_CONFIGS 256

#include "main.c"

static int debug_maxelts=32, debug_maxchars=80, quiet_start=0;

static char *configs[MAX_CONFIGS], *exe_arg=NULL, *file_arg=NULL;
static int n_configs=0;

static u8_condition FileWait=_("FILEWAIT");

static void identify_application(int argc,char **argv,char *dflt)
{
  int i=1;
  while (i<argc)
    if (strchr(argv[i],'=')) i++;
    else {
      char *start=strchr(argv[i],'/'), *copy, *dot, *slash;
      if (start==NULL) start=strchr(argv[i],'\\');
      if (start==NULL) start=argv[1];
      if (((*start)=='/') || ((*start)=='\\')) start++;
      if ((*start=='\0') || (argv[i][0]=='/') || (argv[i][0]=='\\'))
	start=argv[1];
      copy=u8_strdup(start); dot=strchr(copy,'.');
      if (dot) *dot='\0';
      slash=strrchr(copy,'/');
      if ((slash)&&(slash[1])) u8_default_appid(slash+1);
      else u8_default_appid(copy);
      u8_free(copy);
      return;}
  u8_default_appid(dflt);
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
    fflush(stdout); fflush(stderr);
    u8_log(LOG_INFO,"CHAIN","Closing pools and indices");
    fd_close_pools();
    fd_close_indices();
    u8_log(LOG_NOTICE,"CHAIN",">> %s %s%s",
	   exe_arg,u8_fromlibc(file_arg),argstring.u8_outbuf);
    u8_free(argstring.u8_outbuf);
    return execvp(exe_arg,cargv);}
}

static u8_string wait_for_file=NULL;

int main(int argc,char **argv)
{
  u8_string source_file=NULL;
  fd_lispenv env=fd_working_environment();
  fdtype main_proc=FD_VOID, result=FD_VOID;
  fdtype *args=u8_alloc_n(argc,fdtype);
  int i=1, n_args=0, retval=0;
  if (argc<2) {
    fprintf(stderr,"Usage: fdexec filename [config=val]*\n");
    return -1;}
  if (exe_arg==NULL) exe_arg=u8_strdup(argv[0]);
  u8_register_source_file(_FILEINFO);
  fd_register_config
    ("DEBUGMAXCHARS",
     _("Max number of chars in strings output in error reports"),
     fd_intconfig_get,fd_intconfig_set,
     &debug_maxchars);
  fd_register_config
    ("DEBUGMAXELTS",
     _("Max number of sequence/choice elements to display in error reports"),
     fd_intconfig_get,fd_intconfig_set,
     &debug_maxelts);
  fd_register_config
    ("FILEWAIT",
     _("File to wait to exist before starting"),
     fd_sconfig_get,fd_sconfig_set,
     &wait_for_file);
  fd_register_config
    ("QUIET",_("Whether to output startup messages"),
     fd_boolconfig_get,fd_boolconfig_set,
     &quiet_start);
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
  identify_application(argc,argv,argv[0]);
  if (!(quiet_start)) fd_boot_message();
  if (wait_for_file) {
    if (u8_file_existsp(wait_for_file))
      u8_log(LOG_NOTICE,FileWait,"Starting now because '%s' exists",
	     wait_for_file);
    else {
      int n=0;
      u8_log(LOG_NOTICE,FileWait,"Waiting for '%s' to exist",
	     wait_for_file);
      while (1) {
	n++; if (n<15) sleep(n); else sleep(15);
	if (u8_file_existsp(wait_for_file)) {
	  u8_log(LOG_NOTICE,FileWait,"[%d] Starting now because '%s' exists",
		 n,wait_for_file);
	  break;}
	else if ((n<15) ? ((n%4)==0) : ((n%20)==0))
	  u8_log(LOG_NOTICE,FileWait,"[%d] Waiting for '%s' to exist",
		 n,wait_for_file);}}}

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
      u8_string arg=u8_fromlibc(argv[i]), basename;
      file_arg=u8_strdup(argv[i]);
      source_file=u8_abspath(arg,NULL);
      basename=u8_basename(source_file,NULL);
      u8_default_appid(basename);
      u8_free(arg); u8_free(basename);
      i++;}
  if (source_file) {
    fdtype interp=fd_lispstring(u8_fromlibc(argv[0]));
    fdtype src=fd_lispstring(u8_realpath(source_file,NULL));
    result=fd_load_source(source_file,env,NULL);
    fd_config_set("INTERPRETER",interp);
    fd_config_set("SOURCE",src);
    fd_decref(src); fd_decref(interp); u8_free(source_file);
    source_file=NULL;}
  else {
    fprintf(stderr,
	    "Usage: fdexec [conf=val]* source_file (arg | [conf=val])*\n");
    return 1;}

  if (!(quiet_start)) {
    double startup_time=u8_elapsed_time()-fd_load_start;
    char *units="s";
    if (startup_time>1) {}
    else if (startup_time>0.001) {
      startup_time=startup_time*1000; units="ms";}
    else {startup_time=startup_time*1000000; units="ms";}
    u8_message("FramerD %s booted in %0.3f%s, %d/%d pools/indices",
	       u8_appid(),startup_time,units,fd_n_pools,
	       fd_n_primary_indices+fd_n_secondary_indices);}

  if (!(FD_ABORTP(result))) {
    main_proc=fd_symeval(fd_intern("MAIN"),env);
    if (FD_APPLICABLEP(main_proc)) {
      int ctype=FD_PRIM_TYPE(main_proc);
      fd_decref(result);
      result=fd_applyfns[ctype](main_proc,n_args,args);
      result=fd_finish_call(result);}}
  if (FD_TROUBLEP(result)) {
    u8_exception e=u8_erreify(), root=e;
    int old_maxelts=fd_unparse_maxelts, old_maxchars=fd_unparse_maxchars;
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,512);
    fd_unparse_maxchars=debug_maxchars; fd_unparse_maxelts=debug_maxelts;
    while (root->u8x_prev) root=root->u8x_prev;
    fd_print_exception(&out,root);
    fd_print_backtrace(&out,e,80);
    fd_unparse_maxelts=old_maxelts; fd_unparse_maxchars=old_maxchars;
    fputs(out.u8_outbuf,stderr);
    u8_free(out.u8_outbuf);
    u8_free_exception(e,1);
    retval=-1;}
  fd_decref(result);
  /* Hollow out the environment, which should let you reclaim it.
     This patches around the classic issue with circular references in
     a reference counting garbage collector.  If the
     working_environment contains procedures which are closed in the
     working environment, it will not be GC'd because of those
     circular pointers. */
  if (FD_HASHTABLEP(env->bindings))
    fd_reset_hashtable((fd_hashtable)(env->bindings),0,1);
  fd_recycle_environment(env);
  i=0; while (i<n_args) {fd_decref(args[i]); i++;}
  u8_free(args);
  fd_decref(main_proc);
  return retval;
}
