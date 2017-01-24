/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDEXEC_INCLUDED
#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif
#endif

#include "framerd/fdsource.h"
#include "framerd/defines.h"
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

#define MAX_CONFIGS 512

#include "main.c"

static int debug_maxelts=32, debug_maxchars=80, quiet_console=0;

static char *configs[MAX_CONFIGS], *exe_arg=NULL, *file_arg=NULL;
static int n_configs=0;

static u8_condition FileWait=_("FILEWAIT");

static void exit_fdexec()
{
  if (!(quiet_console)) fd_status_message();
}

typedef char *charp;

static fdtype chain_prim(int n,fdtype *args)
{
  if (n_configs>=MAX_CONFIGS)
    return fd_err(_("Too many configs to CHAIN"),"chain_prim",NULL,FD_VOID);
  else {
    int i=0, cargc=0, rv=-1;
    /* This stream will contain the chaining message */
    struct U8_OUTPUT argstring;
    char **cargv=u8_alloc_n(n+n_configs+4,charp);
    U8_INIT_STATIC_OUTPUT(argstring,512);
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
    u8_puts(&argstring,"LOGAPPEND=yes ");
    cargv[cargc++]=u8_strdup("LOGAPPEND=yes");
    i=0; while (i<n_configs) {
      char *config=configs[i];
      if (strncmp(config,"LOGAPPEND=",10)) {
        u8_printf(&argstring," %s",u8_fromlibc(configs[i]));
        cargv[cargc++]=configs[i++];}
      else i++;}
    cargv[cargc++]=NULL;
    fflush(stdout); fflush(stderr);
    u8_log(LOG_INFO,"CHAIN","Closing pools and indices");
    fd_close_pools();
    fd_close_indices();
    u8_log(LOG_NOTICE,"CHAIN",">> %s %s%s",
           exe_arg,u8_fromlibc(file_arg),argstring.u8_outbuf);
    u8_free(argstring.u8_outbuf);
    rv=execvp(exe_arg,cargv);
    if (rv<0) {
      u8_graberr(errno,"CHAIN",u8_strdup(file_arg));
      return FD_ERROR_VALUE;}
    else return FD_INT(rv);}
}

static u8_string wait_for_file=NULL;

static void print_args(int argc,char **argv)
{
  int j=0;
  fprintf(stderr,"-- argc=%d pid=%d\n",argc,getpid());
  while (j<argc) {
    fprintf(stderr,"   argv[%d]=%s\n",j,argv[j]);
    j++;}
}

static fdtype *handle_argv(int argc,char **argv,size_t *arglenp,
                           u8_string *exe_namep,
                           u8_string *source_filep,
                           u8_string appid_prefix)
{
  fdtype *args=NULL;
  u8_string tmp_string=NULL, source_file=NULL, exe_name=NULL;
  unsigned int arg_mask=0;
  int i=1;

  if (getenv("FD_SHOWARGV")) print_args(argc,argv);

  exe_arg=argv[0];

  tmp_string=u8_fromlibc(exe_arg);
  exe_name=u8_basename(tmp_string,NULL);
  u8_free(tmp_string); tmp_string=NULL;
  if (exe_namep) *exe_namep=exe_name;

  while (i<argc) {
    if (isconfig(argv[i])) {
      u8_log(LOG_INFO,"FDConfig","    %s",argv[i]);
      if (n_configs >= MAX_CONFIGS) n_configs++;
      else configs[n_configs++] = u8_strdup(argv[i]);
      i++;}
    else if (source_file) i++;
    else {
      arg_mask |= (1<<i);
      file_arg=argv[i++];
      source_file=u8_fromlibc(file_arg);}}

  if (source_file==NULL) {
    int i=0;
    fprintf(stderr,"argc=%d\n",argc);
    while (i<argc) {
      fprintf(stderr,"argv[%d]=%s\n",i,argv[i]);
      i++;}
    fprintf(stderr,"Usage: %s filename [config=val]*\n",exe_name);
    exit(EXIT_FAILURE);}
  else if (source_filep) {
    *source_filep=source_file;}
  else {}

  fd_init_dtypelib();

  args = fd_handle_argv(argc,argv,arg_mask,arglenp);

  if (u8_appid()==NULL) {
    u8_string base;
    if (source_file)
      base=u8_basename(source_file,"*");
    else base=u8_basename(exe_name,NULL);
    if (appid_prefix==NULL)
      u8_default_appid(base);
    else {
      u8_string combined=u8_string_append(appid_prefix,base,NULL);
      u8_default_appid(combined);
      u8_free(combined);}
    u8_free(base);}

  if (source_filep==NULL) u8_free(source_file);
  if (exe_namep==NULL) u8_free(exe_name);

  return args;
}

int do_main(int argc,char **argv,
            u8_string exe_name,u8_string source_file,
            fdtype *args,size_t n_args)
{
  fd_lispenv env=fd_working_environment();
  fdtype main_proc=FD_VOID, result=FD_VOID;
  int retval=0;

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
     &quiet_console);

  setlocale(LC_ALL,"");
  /* Process command line arguments */

  atexit(exit_fdexec);

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
  fd_init_fdweb();
#else
  FD_INIT_SCHEME_BUILTINS();
#endif

  fd_init_schemeio();

  if (!(quiet_console)) fd_boot_message();
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
  
  if (source_file) {
    fdtype src=fd_lispstring(u8_realpath(source_file,NULL));
    result=fd_load_source(source_file,env,NULL);

    fd_set_config("SOURCE",src);
    
    fd_decref(src);
    source_file=NULL;}
  else {
    fprintf(stderr,
            "Usage: fdexec [conf=val]* source_file (arg | [conf=val])*\n");
    return 1;}

  if (!(quiet_console)) {
    double startup_time=u8_elapsed_time()-fd_load_start;
    char *units="s";
    if (startup_time>1) {}
    else if (startup_time>0.001) {
      startup_time=startup_time*1000; units="ms";}
    else {startup_time=startup_time*1000000; units="ms";}
    u8_message("FramerD %s loaded in %0.3f%s, %d/%d pools/indices",
               u8_appid(),startup_time,units,fd_n_pools,
               fd_n_primary_indices+fd_n_secondary_indices);}

  if (!(FD_ABORTP(result))) {
    fdtype main_symbol=fd_intern("MAIN");
    main_proc=fd_symeval(main_symbol,env);
    if (FD_APPLICABLEP(main_proc)) {
      int ctype=FD_PRIM_TYPE(main_proc);
      fd_decref(result);
      result=fd_applyfns[ctype](main_proc,n_args,args);
      result=fd_finish_call(result);}}
  if (FD_TROUBLEP(result)) {
    u8_exception e=u8_erreify(), root=e;
    int old_maxelts=fd_unparse_maxelts, old_maxchars=fd_unparse_maxchars;
    U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,512);
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
  fd_decref(main_proc);
  return retval;
}

#ifndef FDEXEC_INCLUDED
int main(int argc,char **argv)
{
  u8_string source_file=NULL, exe_name=NULL;
  fdtype *args=NULL; size_t n_args=0; 
  int i=0, retval=0;

  fd_main_errno_ptr=&errno;

  args=handle_argv(argc,argv,&n_args,&exe_name,&source_file,NULL);

  retval=do_main(argc,argv,exe_name,source_file,args,n_args);

  i=0; while (i<n_args) {
    fdtype arg=args[i++]; fd_decref(arg);}
  u8_free(args);

  if (exe_name) u8_free(exe_name);
  if (source_file) u8_free(source_file);

  return retval;
}
#endif
  

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
