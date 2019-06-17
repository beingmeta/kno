/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef EMBEDDED_KNO
#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif
#endif

#include "kno/knosource.h"
#include "kno/defines.h"
#include "kno/lisp.h"
#include "kno/support.h"
#include "kno/tables.h"
#include "kno/storage.h"
#include "kno/drivers.h"
#include "kno/eval.h"
#include "kno/ports.h"

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
static int no_stdin = 0;

static int chain_fast_exit=1;

static u8_condition FileWait=_("FILEWAIT");

static void exit_kno()
{
  if (!(kno_be_vewy_quiet)) kno_log_status("Exit(kno)");
}

static lispval load_stdin(kno_lexenv env)
{
  static U8_XINPUT u8stdin;
  u8_string cwd = u8_getcwd();
  u8_init_xinput(&u8stdin,0,NULL);
  lispval v = kno_load_stream((u8_input)&u8stdin,env,cwd);
  u8_close_input((u8_input)&u8stdin);
  u8_free(cwd);
  return v;
}

typedef char *charp;

static lispval main_symbol = KNO_VOID, exec_script = KNO_FALSE;
static lispval real_main = KNO_VOID;

static lispval chain_prim(int n,lispval *args)
{
  if (n_configs>=MAX_CONFIGS)
    return kno_err(_("Too many configs to CHAIN"),"chain_prim",NULL,VOID);
  else if ( (stop_file) && (u8_file_existsp(stop_file)) ) {
    u8_log(LOG_CRIT,"StopFile",
           "Not chaining because the file '%s' exists",stop_file);
    return KNO_FALSE;}
  else {
    int i = 0, cargc = 0, rv = -1;
    /* This stream will contain the chaining message */
    struct U8_OUTPUT argstring;
    char **cargv = u8_alloc_n(n+n_configs+4,charp);
    U8_INIT_STATIC_OUTPUT(argstring,512);
    cargv[cargc++]=exe_arg;
    cargv[cargc++]=file_arg;
    i = 0; while (i<n)
      if (KNO_STRINGP(args[i])) {
        u8_printf(&argstring," %s",CSTRING(args[i]));
        cargv[cargc]=u8_tolibc(CSTRING(args[i]));
        i++; cargc++;}
      else {
        u8_string as_string = kno_lisp2string(args[i]);
        char *libc_string = u8_tolibc(as_string);
        u8_printf(&argstring," %s",as_string);
        u8_free(as_string);
        cargv[cargc]=libc_string;
        i++; cargc++;}
    u8_puts(&argstring," LOGAPPEND=yes");
    cargv[cargc++]=u8_strdup("LOGAPPEND=yes");
    i = 0; while (i<n_configs) {
      char *config = configs[i];
      if ( (strncmp(config,"LOGAPPEND=",10) == 0) ||
           (strncmp(config,"PIDFILE=",10) == 0) )
        i++;
      else {
        u8_printf(&argstring," %s",u8_fromlibc(configs[i]));
        cargv[cargc++]=configs[i++];}}
    cargv[cargc++]=NULL;
    fflush(stdout); fflush(stderr);
    /* TODO: Should we run any exit methods here? */
    u8_log(LOG_INFO,"CHAIN","Closing pools and indexes");
    kno_fast_exit = chain_fast_exit;
    kno_close_pools();
    kno_close_indexes();
    u8_log(LOG_NOTICE,"CHAIN",">> %s %s%s",
           exe_arg,u8_fromlibc(file_arg),argstring.u8_outbuf);
    u8_free(argstring.u8_outbuf);
    u8_log(LOG_NOTICE,"CHAIN","%s",
           "================================================================"
           "================================");
    rv = execvp(exe_arg,cargv);
    if (rv<0) {
      u8_graberr(errno,"CHAIN",u8_strdup(file_arg));
      return KNO_ERROR;}
    else return KNO_INT(rv);}
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
  /* Bit map of args which we handle */
  unsigned char arg_mask[argc];  memset(arg_mask,0,argc);
  int i = 1;

  if (getenv("KNO_SHOWARGV")) print_args(argc,argv);

  exe_arg = argv[0];

  tmp_string = u8_fromlibc(exe_arg);
  exe_name = u8_basename(tmp_string,NULL);
  u8_free(tmp_string); tmp_string = NULL;
  if (exe_namep) *exe_namep = exe_name;

  while (i<argc) {
    if (isconfig(argv[i])) {
      u8_log(LOG_INFO,"Knoconfig","    %s",argv[i]);
      if (n_configs >= MAX_CONFIGS) n_configs++;
      else configs[n_configs++] = u8_strdup(argv[i]);
      i++;}
    else if (source_file) i++;
    else {
      arg_mask[i] = 'X';
      file_arg = argv[i++];
      source_file = u8_fromlibc(file_arg);}}

  if (source_file == NULL) {}
  else if (source_filep) {
    *source_filep = source_file;}
  else {}

  kno_init_lisp_types();

  KNO_NEW_STACK(((kno_stack)NULL),"startup",argv[0],VOID);
  _stack->stack_label=u8_strdup(u8_appid());
  U8_SETBITS(_stack->stack_flags,KNO_STACK_FREE_LABEL);

  args = kno_handle_argv(argc,argv,arg_mask,arglenp);

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

  kno_pop_stack(_stack);

  return args;
}

int do_main(int argc,char **argv,
            u8_string exe_name,u8_string source_file,
            lispval *args,size_t n_args)
{
  kno_lexenv env = kno_working_lexenv();
  lispval main_proc = VOID, result = VOID;
  int retval = 0;

  u8_register_source_file(_FILEINFO);

  stop_file=kno_runbase_filename(".stop");

  kno_set_app_env(env);

  main_symbol = real_main = kno_intern("main");

  kno_register_config
    ("DEBUGMAXCHARS",
     _("Max number of chars in strings output in error reports"),
     kno_intconfig_get,kno_intconfig_set,
     &debug_maxchars);
  kno_register_config
    ("DEBUGMAXELTS",
     _("Max number of sequence/choice elements to display in error reports"),
     kno_intconfig_get,kno_intconfig_set,
     &debug_maxelts);
  kno_register_config
    ("FILEWAIT",
     _("File to wait to exist before starting"),
     kno_sconfig_get,kno_sconfig_set,
     &wait_for_file);
  kno_register_config
    ("STOPFILE",
     _("File to wait to exist before starting"),
     kno_sconfig_get,kno_sconfig_set,
     &stop_file);
  kno_register_config
    ("NOSTDIN",
     _("Don't read source from STDIN when needed"),
     kno_boolconfig_get,kno_boolconfig_set,
     &no_stdin);
  kno_register_config
    ("MAIN",
     _("The name of the (main) routine for this file"),
     kno_lconfig_get,kno_symconfig_set,
     &main_symbol);
  kno_register_config
    ("EXECSCRIPT",
     _("The name of the root file being executed for this application"),
     kno_lconfig_get,NULL,&exec_script);


  setlocale(LC_ALL,"");
  /* Process command line arguments */

  atexit(exit_kno);

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

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (KNO_TESTCONFIG))
  kno_init_scheme();
#endif

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (KNO_TESTCONFIG))
  kno_init_texttools();
  kno_init_webtools();
#else
  KNO_INIT_SCHEME_BUILTINS();
#endif

  kno_init_schemeio();

  if (!(kno_be_vewy_quiet)) kno_boot_message();
  if ( (stop_file) && (u8_file_existsp(stop_file)) ) {
    u8_log(LOG_CRIT,"StopFile",
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

  kno_idefn((lispval)env,kno_make_cprimn("CHAIN",chain_prim,0));

  if (source_file) {
    lispval src = kno_lispstring(u8_realpath(source_file,NULL));
    result = kno_load_source(source_file,env,NULL);

    kno_set_config("SOURCE",src);

    kno_decref(src);}
  else if (no_stdin) {
    int i = 0;
    fprintf(stderr,"argc=%d\n",argc);
    while (i<argc) {
      fprintf(stderr,"argv[%d]=%s\n",i,argv[i]);
      i++;}
    fprintf(stderr,"Usage: %s filename [config = val]*\n",exe_name);
    exit(EXIT_FAILURE);}
  else result = load_stdin(env);

  if (!(kno_be_vewy_quiet)) {
    double startup_time = u8_elapsed_time()-kno_load_start;
    char *units="s";
    if (startup_time>1) {}
    else if (startup_time>0.001) {
      startup_time = startup_time*1000; units="ms";}
    else {startup_time = startup_time*1000000; units="ms";}
    u8_message("Kno %s loaded in %0.3f%s, %d/%d pools/indexes",
               u8_appid(),startup_time,units,kno_n_pools,
               kno_n_primary_indexes+kno_n_secondary_indexes);}

  if (!(KNO_ABORTP(result))) {
    main_proc = kno_symeval(main_symbol,env);
    if ( (KNO_VOIDP(main_proc)) && (main_symbol == real_main) ) {
      /* Nothing to do */
      result = KNO_VOID;}
    else if (KNO_APPLICABLEP(main_proc)) {
      kno_decref(result);
      result = kno_apply(main_proc,n_args,args);
      result = kno_finish_call(result);}
    else {
      u8_log(LOGWARN,"BadMain",
             "The main procedure for %s (%q) isn't applicable",
             ((source_file) ? (source_file) : (U8S("stdin")) ),
             main_proc);}}
  if (source_file) source_file = NULL;
  if (KNO_TROUBLEP(result)) {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,2000);
    int old_maxelts = kno_unparse_maxelts, old_maxchars = kno_unparse_maxchars;
    u8_exception e = u8_erreify();

    kno_unparse_maxchars = debug_maxchars; kno_unparse_maxelts = debug_maxelts;
    kno_output_errstack(&out,e);
    kno_unparse_maxelts = old_maxelts; kno_unparse_maxchars = old_maxchars;
    fputs(out.u8_outbuf,stderr); fputc('\n',stderr);

    /* Write out the exception object somehow */

    u8_free(out.u8_outbuf);
    u8_free_exception(e,1);
    retval = -1;}

  if ( ! kno_fast_exit ) {
    kno_decref(result);
    kno_decref(main_proc);}

  /* Run registered thread cleanup handlers.
     Note that since the main thread wasn't started with a function which
     calls u8_threadexit(), we do it here. */
  u8_threadexit();

  return retval;
}

#ifndef EMBEDDED_KNO
int main(int argc,char **argv)
{
  u8_string source_file = NULL, exe_name = NULL;
  lispval *args = NULL; size_t n_args = 0;
  int i = 0, retval = 0;

  u8_log_show_procinfo=1;

  kno_main_errno_ptr = &errno;

  KNO_INIT_STACK();

#if KNO_TESTCONFIG
  kno_autoload_config("TESTMODS","TESTLOAD","TESTINIT");
#endif

  args = handle_argv(argc,argv,&n_args,&exe_name,&source_file,NULL);

  KNO_NEW_STACK(((struct KNO_STACK *)NULL),"kno",NULL,VOID);
  u8_string appid=u8_appid();
  if (appid==NULL) appid=argv[0];
  _stack->stack_label=u8_strdup(appid);
  U8_SETBITS(_stack->stack_flags,KNO_STACK_FREE_LABEL);

  if (source_file)
    exec_script = kno_lispstring(source_file);

  retval = do_main(argc,argv,exe_name,source_file,args,n_args);

  kno_pop_stack(_stack);

  i = 0; while (i<n_args) {
    lispval arg = args[i++];
    kno_decref(arg);}
  u8_free(args);

  if (exe_name) u8_free(exe_name);
  if (source_file) u8_free(source_file);

  kno_doexit(KNO_FALSE);

  return retval;
}
#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
