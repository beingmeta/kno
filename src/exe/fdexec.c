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
#include "framerd/support.h"
#include "framerd/tables.h"
#include "framerd/storage.h"
#include "framerd/drivers.h"
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
#include <math.h>
#include <time.h>

#include "main.h"

#define MAX_CONFIGS 512

#include "main.c"

static u8_string stop_file=NULL;
static u8_string wait_for_file = NULL;

static int debug_maxelts = 32, debug_maxchars = 80;

static char *configs[MAX_CONFIGS], *exe_arg = NULL, *file_arg = NULL;
static int n_configs = 0;

static u8_condition FileWait=_("FILEWAIT");

static void exit_fdexec()
{
  if (!(fd_be_vewy_quiet)) fd_log_status("Exit(fdexec)");
}

typedef char *charp;

static lispval chain_prim(int n,lispval *args)
{
  if (n_configs>=MAX_CONFIGS)
    return fd_err(_("Too many configs to CHAIN"),"chain_prim",NULL,VOID);
  else if ( (stop_file) && (u8_file_existsp(stop_file)) ) {
    u8_log(LOGCRIT,"StopFile",
           "Not chaining because the file '%s' exists",stop_file);
    return FD_FALSE;}
  else {
    int i = 0, cargc = 0, rv = -1;
    /* This stream will contain the chaining message */
    struct U8_OUTPUT argstring;
    char **cargv = u8_alloc_n(n+n_configs+4,charp);
    U8_INIT_STATIC_OUTPUT(argstring,512);
    cargv[cargc++]=exe_arg;
    cargv[cargc++]=file_arg;
    i = 0; while (i<n)
      if (FD_STRINGP(args[i])) {
        u8_printf(&argstring," %s",CSTRING(args[i]));
        cargv[cargc]=u8_tolibc(CSTRING(args[i]));
        i++; cargc++;}
      else {
        u8_string as_string = fd_lisp2string(args[i]);
        char *libc_string = u8_tolibc(as_string);
        u8_printf(&argstring," %s",as_string);
        u8_free(as_string);
        cargv[cargc]=libc_string;
        i++; cargc++;}
    u8_puts(&argstring," LOGAPPEND=yes");
    cargv[cargc++]=u8_strdup("LOGAPPEND=yes");
    i = 0; while (i<n_configs) {
      char *config = configs[i];
      if (strncmp(config,"LOGAPPEND=",10)) {
        u8_printf(&argstring," %s",u8_fromlibc(configs[i]));
        cargv[cargc++]=configs[i++];}
      else i++;}
    cargv[cargc++]=NULL;
    fflush(stdout); fflush(stderr);
    u8_log(LOG_INFO,"CHAIN","Closing pools and indexes");
    fd_close_pools();
    fd_close_indexes();
    u8_log(LOG_NOTICE,"CHAIN",">> %s %s%s",
           exe_arg,u8_fromlibc(file_arg),argstring.u8_outbuf);
    u8_free(argstring.u8_outbuf);
    rv = execvp(exe_arg,cargv);
    if (rv<0) {
      u8_graberr(errno,"CHAIN",u8_strdup(file_arg));
      return FD_ERROR;}
    else return FD_INT(rv);}
}

static void print_args(int argc,char **argv)
{
  int j = 0;
  fprintf(stderr,"-- argc=%d pid=%d\n",argc,getpid());
  while (j<argc) {
    fprintf(stderr,"   argv[%d]=%s\n",j,argv[j]);
    j++;}
}

static lispval *handle_argv(int argc,char **argv,size_t *arglenp,
                            u8_string *exe_namep,
                            u8_string *source_filep,
                            u8_string appid_prefix)
{
  lispval *args = NULL;
  u8_string tmp_string = NULL, source_file = NULL, exe_name = NULL;
  unsigned int arg_mask = 0;
  int i = 1;

  if (getenv("FD_SHOWARGV")) print_args(argc,argv);

  exe_arg = argv[0];

  tmp_string = u8_fromlibc(exe_arg);
  exe_name = u8_basename(tmp_string,NULL);
  u8_free(tmp_string); tmp_string = NULL;
  if (exe_namep) *exe_namep = exe_name;

  while (i<argc) {
    if (isconfig(argv[i])) {
      u8_log(LOG_INFO,"FDConfig","    %s",argv[i]);
      if (n_configs >= MAX_CONFIGS) n_configs++;
      else configs[n_configs++] = u8_strdup(argv[i]);
      i++;}
    else if (source_file) i++;
    else {
      arg_mask  |=  (1<<i);
      file_arg = argv[i++];
      source_file = u8_fromlibc(file_arg);}}

  if (source_file == NULL) {
    int i = 0;
    fprintf(stderr,"argc=%d\n",argc);
    while (i<argc) {
      fprintf(stderr,"argv[%d]=%s\n",i,argv[i]);
      i++;}
    fprintf(stderr,"Usage: %s filename [config = val]*\n",exe_name);
    exit(EXIT_FAILURE);}
  else if (source_filep) {
    *source_filep = source_file;}
  else {}

  fd_init_lisp_types();

  FD_NEW_STACK(((fd_stack)NULL),"startup",argv[0],VOID);
  _stack->stack_label=u8_strdup(u8_appid());
  _stack->stack_free_label=1;

  args = fd_handle_argv(argc,argv,arg_mask,arglenp);

  if (u8_appid() == NULL) {
    u8_string base;
    if (source_file)
      base = u8_basename(source_file,"*");
    else base = u8_basename(exe_name,NULL);
    if (appid_prefix == NULL)
      u8_default_appid(base);
    else {
      u8_string combined = u8_string_append(appid_prefix,base,NULL);
      u8_default_appid(combined);
      u8_free(combined);}
    u8_free(base);}

  if (source_filep == NULL) u8_free(source_file);
  if (exe_namep == NULL) u8_free(exe_name);

  fd_pop_stack(_stack);

  return args;
}

int do_main(int argc,char **argv,
            u8_string exe_name,u8_string source_file,
            lispval *args,size_t n_args)
{
  fd_lexenv env = fd_working_lexenv();
  lispval main_proc = VOID, result = VOID;
  int retval = 0;

  u8_register_source_file(_FILEINFO);

  stop_file=fd_runbase_filename(".stop");

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
    ("STOPFILE",
     _("File to wait to exist before starting"),
     fd_sconfig_get,fd_sconfig_set,
     &stop_file);

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
  fd_init_scheme();
#endif

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (FD_TESTCONFIG))
  fd_init_texttools();
  fd_init_fdweb();
#else
  FD_INIT_SCHEME_BUILTINS();
#endif

  fd_init_schemeio();

  if (!(fd_be_vewy_quiet)) fd_boot_message();
  if ( (stop_file) && (u8_file_existsp(stop_file)) ) {
    u8_log(LOGCRIT,"StopFile",
           "Not starting because the file '%s' exists",stop_file);
    return 1;}
  else if (wait_for_file) {
    if (u8_file_existsp(wait_for_file))
      u8_log(LOG_NOTICE,FileWait,"Starting now because '%s' exists",
             wait_for_file);
    else {
      int n = 0;
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

  fd_idefn((lispval)env,fd_make_cprimn("CHAIN",chain_prim,0));
  
  if (source_file) {
    lispval src = fd_lispstring(u8_realpath(source_file,NULL));
    result = fd_load_source(source_file,env,NULL);

    fd_set_config("SOURCE",src);
    
    fd_decref(src);
    source_file = NULL;}
  else {
    fprintf(stderr,
            "Usage: fdexec [conf = val]* source_file (arg | [conf = val])*\n");
    return 1;}

  if (!(fd_be_vewy_quiet)) {
    double startup_time = u8_elapsed_time()-fd_load_start;
    char *units="s";
    if (startup_time>1) {}
    else if (startup_time>0.001) {
      startup_time = startup_time*1000; units="ms";}
    else {startup_time = startup_time*1000000; units="ms";}
    u8_message("FramerD %s loaded in %0.3f%s, %d/%d pools/indexes",
               u8_appid(),startup_time,units,fd_n_pools,
               fd_n_primary_indexes+fd_n_secondary_indexes);}

  if (!(FD_ABORTP(result))) {
    lispval main_symbol = fd_intern("MAIN");
    main_proc = fd_symeval(main_symbol,env);
    if (FD_APPLICABLEP(main_proc)) {
      fd_decref(result);
      result = fd_apply(main_proc,n_args,args);
      result = fd_finish_call(result);}}
  if (FD_TROUBLEP(result)) {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,2000);
    int old_maxelts = fd_unparse_maxelts, old_maxchars = fd_unparse_maxchars;
    u8_exception e = u8_erreify();

    fd_unparse_maxchars = debug_maxchars; fd_unparse_maxelts = debug_maxelts;
    fd_sum_exception(&out,e);
    fd_unparse_maxelts = old_maxelts; fd_unparse_maxchars = old_maxchars;
    fputs(out.u8_outbuf,stderr); fputc('\n',stderr);

    lispval irritant = (e->u8x_free_xdata == fd_free_exception_xdata ) ?
      ((lispval) (e->u8x_xdata)) : (VOID);
    lispval stacktrace   = (fd_stacktracep(irritant)) ? (irritant) : (VOID);
    if (!(VOIDP(stacktrace))) {
      struct U8_XOUTPUT xout;
      u8_output out=(u8_output)&xout;
      u8_init_xoutput(&xout,2,NULL);
      FD_DOLIST(entry,stacktrace) {
        u8_puts(out,";; ");
        if (STRINGP(entry))
          u8_puts(out,CSTRING(entry));
        else {
          u8_puts(out," ");
          fd_pprint(out,entry,";; ",2,5,100);}
        u8_putc(out,'\n');}
      u8_flush_xoutput(&xout);
      u8_close_xoutput(&xout);}

#if 0
    lispval irritant = (e->u8x_free_xdata == fd_free_exception_xdata ) ?
      ((lispval) (e->u8x_xdata)) : (VOID);
    lispval stacktrace   = (fd_stacktracep(irritant)) ? (irritant) : (VOID);
    double elapsed      = u8_elapsed_time();
    int msecs           = floor(elapsed*1000);
    u8_string dumpfile  = u8_mkstring("_stack%d-%d.dtype",getpid(),msecs);
    fd_write_dtype_to_file(stacktrace,dumpfile);
    out.u8_write=out.u8_outbuf;
    u8_printf(&out,";; Complete stacktrace written to %s\n",dumpfile);
    fputs(out.u8_outbuf,stderr);
    u8_free(dumpfile);
#endif
    u8_free(out.u8_outbuf);
    u8_free_exception(e,1);
    retval = -1;}
  fd_decref(result);
  /* Hollow out the environment, which should let it be reclaimed.
     This patches around some of the circular references that might
     exist because working_lexenv may contain procedures which
     are closed in the working environment, so the working environment
     itself won't be GC'd because of those circular pointers. */
  if (HASHTABLEP(env->env_bindings))
    fd_reset_hashtable((fd_hashtable)(env->env_bindings),0,1);
  fd_recycle_lexenv(env);
  fd_decref(main_proc);
  return retval;
}

#ifndef FDEXEC_INCLUDED
int main(int argc,char **argv)
{
  u8_string source_file = NULL, exe_name = NULL;
  lispval *args = NULL; size_t n_args = 0;
  int i = 0, retval = 0;

  fd_main_errno_ptr = &errno;

  FD_INIT_STACK();

  args = handle_argv(argc,argv,&n_args,&exe_name,&source_file,NULL);

  FD_NEW_STACK(((struct FD_STACK *)NULL),"fdexec",NULL,VOID);
  u8_string appid=u8_appid();
  if (appid==NULL) appid=argv[0];
  _stack->stack_label=u8_strdup(appid);
  _stack->stack_free_label=1;

  retval = do_main(argc,argv,exe_name,source_file,args,n_args);

  fd_pop_stack(_stack);

  i = 0; while (i<n_args) {
    lispval arg = args[i++]; fd_decref(arg);}
  u8_free(args);

  if (exe_name) u8_free(exe_name);
  if (source_file) u8_free(source_file);

  return retval;
}
#endif
  

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
