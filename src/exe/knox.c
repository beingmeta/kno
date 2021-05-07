/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
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
#include "kno/cprims.h"

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

static u8_string stop_file=NULL, done_file=NULL;
static u8_string wait_for_file = NULL;

static int debug_maxelts = 32, debug_maxchars = 80;

static char *configs[MAX_CONFIGS], *exe_arg = NULL, *source_arg = NULL;
static int n_configs = 0, eval_stdin = 0;

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

static lispval main_symbol = KNO_VOID, exec_source = KNO_FALSE;
static lispval real_main = KNO_VOID;

static lispval chain_prim(int n,lispval *args)
{
  if (n_configs>=MAX_CONFIGS)
    return kno_err(_("Too many configs to CHAIN"),"chain_prim",NULL,VOID);
  else if ( (stop_file) && (u8_file_existsp(stop_file)) ) {
    u8_log(LOG_CRIT,"StopFile",
	   "Not chaining because the file '%s' exists",stop_file);
    return KNO_FALSE;}
  else if ( (done_file) && (u8_file_existsp(done_file)) ) {
    u8_log(LOG_CRIT,"DoneFile",
	   "Not chaining because the file '%s' exists",done_file);
    return KNO_FALSE;}
  else {
    int i = 0, cargc = 0, rv = -1;
    /* This stream will contain the chaining message */
    struct U8_OUTPUT argstring;
    char **cargv = u8_alloc_n(n+n_configs+4,charp);
    U8_INIT_STATIC_OUTPUT(argstring,512);
    cargv[cargc++]=exe_arg;
    cargv[cargc++]=source_arg;
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
	   exe_arg,u8_fromlibc(source_arg),argstring.u8_outbuf);
    u8_free(argstring.u8_outbuf);
    u8_log(LOG_NOTICE,"CHAIN","%s",
	   "================================================================"
	   "================================");
    rv = execvp(exe_arg,cargv);
    if (rv<0) {
      u8_graberr(errno,"CHAIN",u8_strdup(source_arg));
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
			    u8_string *source_specp,
			    u8_string appid_prefix)
{
  lispval *args = NULL;
  u8_string tmp_string = NULL, source_spec = NULL, exe_name = NULL;
  /* Bit map of args which we handle */
  unsigned char arg_mask[argc];	 memset(arg_mask,0,argc);
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
    else if (source_spec) {
      u8_log(LOG_INFO,"ARG","    %s",argv[i]);
      i++;}
    else {
      u8_log(LOG_INFO,"Source","    %s",argv[i]);
      source_arg = argv[i];
      source_spec = u8_fromlibc(source_arg);
      /* The arg_mask is used to tell kno_handle_argv which arguments
	 to leave out of the arguments passed to the script */
      arg_mask[i] = 'X';
      i++;}}

  if (source_spec == NULL) {}
  else if (source_specp) {
    *source_specp = source_spec;}
  else {}

  init_libraries();

  args = kno_handle_argv(argc,argv,arg_mask,arglenp);

  if (u8_appid() == NULL) {
    u8_string base;
    if (source_spec)
      base = u8_basename(source_spec,"*");
    else base = u8_basename(exe_name,NULL);
    if (appid_prefix == NULL)
      u8_default_appid(base);
    else {
      u8_string combined = u8_string_append(appid_prefix,base,NULL);
      u8_default_appid(combined);
      u8_free(combined);}
    u8_free(base);}

  if (source_specp == NULL) u8_free(source_spec);
  if (exe_namep == NULL) u8_free(exe_name);

  return args;
}

static void link_local_cprims()
{
}

static lispval find_exec_module(u8_string string)
{
  lispval modname = kno_getsym(string), result = kno_find_module(modname,0);
  if ( (KNO_VOIDP(result)) && (!(strchr(string,'/'))) ) {
    u8_byte buf[100];
    u8_string newname = u8_bprintf(buf,"actions/%s",string);
    modname = kno_getsym(newname);
    result = kno_find_module(modname,0);}
  return result;
}

int do_main(int argc,char **argv,
	    u8_string exe_name,u8_string source_spec,
	    lispval *args,size_t n_args)
{
  kno_lexenv env = kno_working_lexenv();
  lispval main_proc = VOID, result = VOID;
  int retval = 0, exec_module = 0;
  u8_string source_base = NULL;

  DEFC_PRIMN("CHAIN",chain_prim,KNO_VAR_ARGS|MIN_ARGS(0),
	     "Resets the current process to a fresh instance of knox");
  KNO_LINK_CPRIM("CHAIN",chain_prim,0,(lispval)env);

  link_local_cprims();

  u8_register_source_file(_FILEINFO);

  stop_file=u8_getenv("U8_STOPFILE");
  if (stop_file == NULL)
    stop_file=kno_runbase_filename(".stop");
  done_file=u8_getenv("U8_DONEFILE");
  if (done_file == NULL)
    done_file=kno_runbase_filename(".done");

  kno_set_app_env(env);

  main_symbol = real_main = kno_intern("main");

  u8_string main_ref =
    (source_spec == NULL) ? (NULL) :
    (source_spec[0]==':') ? (strchr(source_spec+1,':')) :
    (strchr(source_spec,':'));
  if (main_ref) {
    main_symbol = kno_getsym(main_ref+1);
    source_base = u8_slice(source_spec,main_ref);}
  else if (source_spec)
    source_base = u8_strdup(source_spec);
  else source_base=NULL;

  if (source_base)
    exec_source = kno_wrapstring(source_base);

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
    ("EVALSTDIN",_("Read additional source from STDIN"),
     kno_boolconfig_get,kno_boolconfig_set,
     &eval_stdin);
  kno_register_config
    ("MAIN",
     _("The name of the (main) routine for this file"),
     kno_lconfig_get,kno_symconfig_set,
     &main_symbol);
  kno_register_config
    ("EXECSOURCE",
     _("The name of the root file being executed for this application"),
     kno_lconfig_get,NULL,&exec_source);

  setlocale(LC_ALL,"");
  /* Process command line arguments */

  atexit(exit_kno);

  /* INITIALIZING MODULES */
  /* Normally, modules have initialization functions called when
     dynamically loaded.  However, if we are statically linked, or we
     don't have the "constructor attributes" use to declare init functions,
     we need to call some initializers explicitly. */

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

  if (source_spec == NULL) {}
  else if (strcmp(source_spec,"-")==0)
    result = load_stdin(env);
  else if (source_spec[0] == ':') { /* Definitely a module reference */
    result = find_exec_module(source_spec+1);
    if (KNO_VOIDP(result))
      result = kno_err("NoExecModule","do_main",source_spec,KNO_VOID);
    else exec_module=1;}
  else if (u8_file_existsp(source_spec) ) {
    lispval src = kno_wrapstring(u8_realpath(source_spec,NULL));
    kno_set_config("SOURCE",src);
    result = kno_load_source(source_spec,env,NULL);
    if (KNO_ABORTED(result)) {
      u8_exception ex = u8_current_exception;
      if (ex == NULL)
	u8_seterr("LoadFailed","knox/main()",u8_strdup(source_spec));}
    kno_decref(src);}
  else {
    result = find_exec_module(source_spec);
    if (KNO_TABLEP(result))
      exec_module=1;
    else {
      kno_decref(result);
      result = kno_err(kno_NoSuchFile,"do_main",source_spec,KNO_VOID);}}

  if (!(kno_be_vewy_quiet)) {
    double startup_time = u8_elapsed_time()-kno_load_start;
    char *units="s";
    if (startup_time>1) {}
    else if (startup_time>0.001) {
      startup_time = startup_time*1000; units="ms";}
    else {startup_time = startup_time*1000000; units="ms";}
    u8_message("Kno %s loaded in %0.3f%s, %d/%d pools/indexes",
	       u8_appid(),startup_time,units,kno_n_pools,
	       kno_n_primary_indexes);}

  if (KNO_ABORTED(result)) {}
  else if (!(SYMBOLP(main_symbol))) {
    kno_decref(result);
    result = kno_err("BadMain","knox.main()",source_spec,main_symbol);
    main_proc = KNO_VOID;}
  else if (exec_module) {
    if (KNO_LEXENVP(result)) {
      kno_lexenv env = (kno_lexenv) result;
      if (KNO_VOIDP(env->env_exports))
	main_proc = kno_get(env->env_bindings,main_symbol,KNO_VOID);
      else if (KNO_TABLEP(env->env_exports))
	main_proc = kno_get(env->env_exports,main_symbol,KNO_VOID);
      else main_proc = kno_get(env->env_bindings,main_symbol,KNO_VOID);}
    else main_proc = kno_get(result,main_symbol,KNO_VOID);
    if (KNO_VOIDP(main_proc)) {
      u8_log(LOG_ERROR,kno_NoHandler,"No handler for %s --- %q in %q",
	     source_spec,main_symbol,result);
      main_proc = kno_err(kno_NoHandler,"knox",source_spec,result);}
    else if (!(KNO_APPLICABLEP(main_proc))) {
      u8_log(LOG_ERROR,kno_BadHandler,"No handler for %s --- %q in %q\n  %q",
	     source_spec,main_symbol,result,main_proc);
      lispval err = kno_err(kno_BadHandler,"knox",source_spec,main_proc);
      kno_decref(main_proc);
      main_proc=err;}}
  else main_proc = kno_symeval(main_symbol,env);

  if (!(KNO_ABORTP(result))) {
    if (KNO_VOIDP(main_proc)) {
      if (source_spec == NULL) {
	int i = 0;
	if (main_symbol == real_main)
	  fprintf(stderr,"Error: %s no filename or (MAIN) specified",
		  exe_name);
	else fprintf(stderr,"Error: %s no filename or main (%s) specified",
		     exe_name,KNO_SYMBOL_NAME(main_symbol));
	while (i<argc) {
	  fprintf(stderr,"argv[%d]=%s\n",i,argv[i]);
	  i++;}
	fprintf(stderr,"Usage: %s [action|execfile] [config=val|arg]*\n",exe_name);
	result = kno_err("No MAIN","main()",NULL,main_symbol);}
      else {
	/* Nothing to do */
	kno_decref(result);
	result = KNO_VOID;}}
    else if (KNO_ABORTED(main_proc))
      result = kno_incref(main_proc); /* Probably not necessary */
    else if (KNO_APPLICABLEP(main_proc)) {
      kno_decref(result);
      result = kno_apply(main_proc,n_args,args);}
    else {
      u8_log(LOGWARN,"BadMain",
	     "The main procedure for %s (%q) isn't applicable",
	     ((source_spec) ? (source_spec) : (U8S("stdin")) ),
	     main_proc);
      result = kno_err("BadMain","main()",KNO_SYMBOL_NAME(main_symbol),
		       main_proc);}}
  if (source_spec) source_spec = NULL;
  if (KNO_TROUBLEP(result)) {
    u8_exception e = u8_erreify();
    if (e == NULL) {
      fputs("\nNull error object!\n",stderr);
      retval = -1;}
    else {
      // lispval handler = kno_symeval(kno_intern("onerror");
      // if (KNO_APPLICABLEP(handler)) {} else NO_ELSE;
      U8_OUTPUT out; U8_INIT_OUTPUT(&out,10000);
      out.u8_streaminfo |= U8_HUMAN_OUTPUT;
      kno_unparse_maxchars = debug_maxchars;
      kno_unparse_maxelts = debug_maxelts;
      kno_output_errstack(&out,e);
      fputs(out.u8_outbuf,stderr);
      fputc('\n',stderr);
      /* Write out the exception object somehow */
      u8_close_output(&out);
      u8_free_exception(e,1);
      retval = -1;}}

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
  u8_string source_spec = NULL, exe_name = NULL;
  lispval *args = NULL; size_t n_args = 0;
  int i = 0, retval = 0;

  u8_log_show_procinfo=1;

  kno_main_errno_ptr = &errno;

  KNO_INIT_STACK_ROOT();

#if KNO_TESTCONFIG
  kno_autoload_config("TESTMODS","TESTLOAD","TESTINIT");
#endif

  args = handle_argv(argc,argv,&n_args,&exe_name,&source_spec,NULL);

  retval = do_main(argc,argv,exe_name,source_spec,args,n_args);

  kno_pop_stack(_stack);

  i = 0; while (i<n_args) {
    lispval arg = args[i++];
    kno_decref(arg);}
  u8_free(args);

  if (exe_name) u8_free(exe_name);
  if (source_spec) u8_free(source_spec);

  /* Call this here, where it might be easier to debug, even
     though it's alos an atexit handler */
  _kno_finish_threads();

 return retval;
}
#endif

