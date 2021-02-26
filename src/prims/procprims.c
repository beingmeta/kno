/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* #define KNO_EVAL_INTERNALS 1 */

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/apply.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/fileprims.h"
#include "kno/procprims.h"
#include "kno/cprims.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include "libu8/u8fileio.h"
#include <libu8/u8netfns.h>
#include <libu8/u8xfiles.h>

#include <stdlib.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#if HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#include <ctype.h>

#if HAVE_SIGNAL_H
#include <signal.h>
#endif

#if (HAVE_SYS_RESOURCE_H)
#include <sys/resource.h>
#elif (HAVE_RESOURCE_H)
#include <resource.h>
#endif

#if HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#if ((HAVE_SYS_VFS_H)&&(HAVE_STATFS))
#include <sys/vfs.h>
#elif ((HAVE_SYS_FSTAT_H)&&(HAVE_STATFS))
#include <sys/statfs.h>

#endif

static u8_condition RedirectFailed = "Redirect failed";

static lispval id_symbol, stdin_symbol, stdout_symbol, stderr_symbol;

static int handle_procopts(lispval opts);

#define KNO_IS_SCHEME 1
#define KNO_DO_FORK 2
#define KNO_DO_WAIT 4
#define KNO_DO_LOOKUP 8

static lispval exec_helper(u8_context caller,
			   int flags,lispval opts,
			   int n,kno_argvec args)
{
  if (!(STRINGP(args[0])))
    return kno_type_error("pathname",caller,args[0]);
  else if ((!(flags&(KNO_DO_LOOKUP|KNO_IS_SCHEME)))&&
	   (!(u8_file_existsp(CSTRING(args[0])))))
    return kno_type_error("real file",caller,args[0]);
  else {
    pid_t pid;
    char **argv;
    u8_byte *arg1 = (u8_byte *)CSTRING(args[0]);
    u8_byte *filename = NULL;
    int i = 1, argc = 0, max_argc = n+2, retval = 0;
    if (strchr(arg1,' ')) {
      const char *scan = arg1; while (scan) {
	const char *brk = strchr(scan,' ');
	if (brk) {max_argc++; scan = brk+1;}
	else break;}}
    else {}
    if ( (n>1) && (SLOTMAPP(args[1])) ) {
      lispval keys = kno_getkeys(args[1]);
      max_argc = max_argc+KNO_CHOICE_SIZE(keys);}
    argv = u8_alloc_n(max_argc,char *);
    if (flags&KNO_IS_SCHEME) {
      argv[argc++]=filename = ((u8_byte *)u8_strdup(KNO_EXEC));
      argv[argc++]=(u8_byte *)u8_tolibc(arg1);}
    else if (strchr(arg1,' ')) {
#ifndef KNO_EXEC_WRAPPER
      u8_free(argv);
      return kno_err(_("No exec wrapper to handle env args"),
		     caller,NULL,args[0]);
#else
      char *argcopy = u8_tolibc(arg1), *start = argcopy;
      char *scan = strchr(start,' ');
      argv[argc++] = filename = (u8_byte *)u8_strdup(KNO_EXEC_WRAPPER);
      while (scan) {
	*scan='\0'; argv[argc++]=start;
	start = scan+1; while (isspace(*start)) start++;
	scan = strchr(start,' ');}
      argv[argc++]=(u8_byte *)u8_tolibc(start);
#endif
    }
    else {
#ifdef KNO_EXEC_WRAPPER
      argv[argc++]=filename = (u8_byte *)u8_strdup(KNO_EXEC_WRAPPER);
#endif
      if (filename)
	argv[argc++]=u8_tolibc(arg1);
      else argv[argc++]=filename = (u8_byte *)u8_tolibc(arg1);}
    if ((n>1)&&(SLOTMAPP(args[1]))) {
      lispval params = args[1];
      lispval keys = kno_getkeys(args[1]);
      DO_CHOICES(key,keys) {
	if ((SYMBOLP(key))||(STRINGP(key))) {
	  lispval value = kno_get(params,key,VOID);
	  if (!(VOIDP(value))) {
	    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
	    u8_string stringval = NULL;
	    if (SYMBOLP(key)) stringval = SYM_NAME(key);
	    else stringval = CSTRING(key);
	    if (stringval) u8_puts(&out,stringval);
	    u8_putc(&out,'=');
	    if (STRINGP(value)) u8_puts(&out,CSTRING(value));
	    else kno_unparse(&out,value);
	    argv[argc++]=out.u8_outbuf;
	    kno_decref(value);}}}
      i++;}
    while (i<n) {
      if (STRINGP(args[i]))
	argv[argc++]=u8_tolibc(CSTRING(args[i++]));
      else {
	u8_string as_string = kno_lisp2string(args[i++]);
	char *as_libc_string = u8_tolibc(as_string);
	argv[argc++]=as_libc_string; u8_free(as_string);}}
    argv[argc++]=NULL;
    if ((flags&KNO_DO_FORK)&&((pid = (fork())))) {
      i = 0; while (i<argc) {
	if (argv[i]) u8_free(argv[i++]); else i++;}
      u8_free(argv);
#if HAVE_WAITPID
      if (flags&KNO_DO_WAIT) {
	unsigned int retval = -1;
	waitpid(pid,&retval,0);
	return KNO_INT(retval);}
#endif
      return KNO_INT(pid);}
    handle_procopts(opts);
    if (flags&KNO_IS_SCHEME)
      retval = execvp(KNO_EXEC,argv);
    else if (flags&KNO_DO_LOOKUP)
      retval = execvp(filename,argv);
    else retval = execvp(filename,argv);
    u8_log(LOG_CRIT,caller,"Fork exec failed (%d/%d:%s) with %s %s (%d)",
	   retval,errno,strerror(errno),
	   filename,argv[0],argc);
    /* We call abort in this case because we've forked but couldn't
       exec and we don't want this Kno executable to exit normally. */
    if (flags&KNO_DO_FORK) {
      u8_graberrno(caller,filename);
      return KNO_ERROR;}
    else {
      kno_clear_errors(1);
      abort();}}
}

DEFC_PRIMN("exec",exec_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "replaces the current application with an "
	   "execution of *command* (a string) to *args* (also "
	   "strings).\n*envmap*, if provided, is a slotmap "
	   "specifying environment variables for execution of "
	   "the command. Environment variables can also be "
	   "explicitly provided in the string *command*.\nThe "
	   "last word of *command* (after any environment "
	   "variables) should be the path of an executable "
	   "program file.")
static lispval exec_prim(int n,kno_argvec args)
{
  return exec_helper("exec_prim",0,n,KNO_FALSE,args);
}

DEFC_PRIMN("exec/cmd",exec_cmd_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "replaces the current application with an "
	   "execution of *command* (a string) to *args* (also "
	   "strings).\n*envmap*, if provided, is a slotmap "
	   "specifying environment variables for execution of "
	   "the command. Environment variables can also be "
	   "explicitly provided in the string *command*.\nThe "
	   "last word of *command* (after any environment "
	   "variables) should be either a command in the "
	   "default search path or the path of an executable "
	   "program file.")
static lispval exec_cmd_prim(int n,kno_argvec args)
{
  return exec_helper("exec_cmd_prim",KNO_DO_LOOKUP,n,KNO_FALSE,args);
}

DEFC_PRIMN("knox",knox_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "replaces the current application with a Kno "
	   "process reading the file *scheme_file* and "
	   "applying the file's `MAIN` definition to the "
	   "results of parsing *args* (also strings).\n"
	   "*envmap*, if provided, specifies CONFIG settings "
	   "for the reading and execution of *scheme-file*. "
	   "Environment variables can also be explicitly "
	   "provided in the string *command*.\n")
static lispval knox_prim(int n,kno_argvec args)
{
  return exec_helper("knox_prim",KNO_IS_SCHEME,n,KNO_FALSE,args);
}

DEFC_PRIMN("fork",fork_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "'forks' a new process executing *command* (a "
	   "string) for *args* (also strings). It returns the "
	   "PID of the new process.\n*envmap*, if provided, is "
	   "a slotmap specifying environment variables for "
	   "execution of the command. Environment variables "
	   "can also be explicitly provided in the string "
	   "*command*.\nThe last word of *command* (after any "
	   "environment variables) should be the path of an "
	   "executable program file.")
static lispval fork_prim(int n,kno_argvec args)
{
  if (n==0) {
    pid_t pid = fork();
    if (pid) {
      long long int_pid = (long long) pid;
      return KNO_INT2LISP(int_pid);}
    else return KNO_FALSE;}
  else return exec_helper("fork_prim",KNO_DO_FORK,n,KNO_FALSE,args);
}

DEFC_PRIMN("fork/cmd",fork_cmd_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "'forks' a new process executing *command* (a "
	   "string) with *args* (also strings). It returns "
	   "the PID of the new process.\n*envmap*, if "
	   "provided, is a slotmap specifying environment "
	   "variables for execution of the command. "
	   "Environment variables can also be explicitly "
	   "provided in the string *command*.\nThe last word "
	   "of *command* (after any environment variables) "
	   "should be either a command in the default search "
	   "path or the path of an executable program file.")
static lispval fork_cmd_prim(int n,kno_argvec args)
{
  return exec_helper("fork_cmd_prim",(KNO_DO_FORK|KNO_DO_LOOKUP),n,KNO_FALSE,args);
}

DEFC_PRIMN("knox/fork",knox_fork_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "'forks' a new Kno process reading the file "
	   "*scheme_file* and applying the file's `MAIN` "
	   "definition to the results of parsing *args* (also "
	   "strings).  It returns the PID of the new process.\n"
	   "*envmap*, if provided, specifies CONFIG settings "
	   "for the reading and execution of *scheme-file*. "
	   "Environment variables can also be explicitly "
	   "provided in the string *command*.\n")
static lispval knox_fork_prim(int n,kno_argvec args)
{
  return exec_helper("knofork_prim",(KNO_IS_SCHEME|KNO_DO_FORK),n,KNO_FALSE,args);
}

DEFC_PRIMN("fork/wait",fork_wait_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "'forks' a new process executing *command* (a "
	   "string) for *args* (also strings). It waits for "
	   "this process to return and returns its exit "
	   "status.\n*envmap*, if provided, is a slotmap "
	   "specifying environment variables for execution of "
	   "the command. Environment variables can also be "
	   "explicitly provided in the string *command*.\nThe "
	   "last word of *command* (after any environment "
	   "variables) should be the path of an executable "
	   "program file.")
static lispval fork_wait_prim(int n,kno_argvec args)
{
  return exec_helper("fork_wait_prim",(KNO_DO_FORK|KNO_DO_WAIT),n,KNO_FALSE,args);
}

DEFC_PRIMN("fork/cmd/wait",fork_cmd_wait_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "'forks' a new process executing *command* (a "
	   "string) with *args* (also strings). It waits for "
	   "this process to return and returns its exit "
	   "status.\n*envmap*, if provided, is a slotmap "
	   "specifying environment variables for execution of "
	   "the command. Environment variables can also be "
	   "explicitly provided in the string *command*.\nThe "
	   "last word of *command* (after any environment "
	   "variables) should be either a command in the "
	   "default search path or the path of an executable "
	   "program file.")
static lispval fork_cmd_wait_prim(int n,kno_argvec args)
{
  return exec_helper("fork_cmd_wait_prim",
		     (KNO_DO_FORK|KNO_DO_LOOKUP|KNO_DO_WAIT),n,KNO_FALSE,args);
}

DEFC_PRIMN("knox/fork/wait",knox_fork_wait_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "'forks' a new Kno process reading the file "
	   "*scheme_file* and applying the file's `MAIN` "
	   "definition to the results of parsing *args* (also "
	   "strings).  It waits for this process to return "
	   "and returns its exit status.\n*envmap*, if "
	   "provided, specifies CONFIG settings for the "
	   "reading and execution of *scheme-file*. "
	   "Environment variables can also be explicitly "
	   "provided in the string *command*.\n")
static lispval knox_fork_wait_prim(int n,kno_argvec args)
{
  return exec_helper("knox_fork_wait_prim",
		     (KNO_IS_SCHEME|KNO_DO_FORK|KNO_DO_WAIT),
		     n,KNO_FALSE,args);
}

/* SUBJOBs */

#define PIPE_FLAGS O_NONBLOCK
#define STDOUT_FILE_MODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)
#define STDERR_FILE_MODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)
#define SUBJOB_EXEC_FLAGS KNO_DO_LOOKUP

#if HAVE_PIPE2
static int setup_pipe(int fds[2])
{
  return pipe2(fds,PIPE_FLAGS);
}
#else
static int setup_pipe(int fds[2])
{
  int rv = pipe(fds);
  return rv;
}
#endif

static int dodup(int from,int to,u8_string stream,u8_string id)
{
  int rv = dup2(from,to);
  if (rv<0) {
    int err = errno; errno=0;
    u8_log(LOGCRIT,RedirectFailed,
	   "Couldn't redirect %s for job %s (%d:%s)",
	   stream,id,err,u8_strerror(err));}
  close(from);
  return rv;
}

static u8_string makeid(int n,kno_argvec args);

DEFC_PRIMN("subjob/open",subjob_open,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	   "'forks' a new process applying *command* to "
	   "*args* and creates a **subjob** object for the "
	   "process.\n*opts* control how the subjob is started "
	   "and how its inputs and outputs are configured.\n"
	   "Some supported options are:\n* **ID** provides a "
	   "descriptive string;\n* **STDIN** is either a file "
	   "to use as the *stdin* to the new process, #f to "
	   "use the *standard input* of the current process, "
	   "or #t to create a stream through which the "
	   "current process can write to the new process.\n* "
	   "**STDOUT** is either a file to use for the "
	   "*stdout* from the new process, #f to share the "
	   "*standard output* of the current process, or #t "
	   "to create a stream from which the current process "
	   "can read the standard output of the new process.\n"
	   "* **STDERR** is either a file to use for the "
	   "*stderr* for the new process, #f to share the "
	   "*stderr* of the current process, or #t to create "
	   "a stream from which the current process can read "
	   "the error output of the new process.\n")
static lispval subjob_open(int n,kno_argvec args)
{
  lispval opts = args[0];
  lispval idstring = kno_getopt(opts,id_symbol,KNO_VOID);
  lispval infile   = kno_getopt(opts,stdin_symbol,KNO_VOID);
  lispval outfile  = kno_getopt(opts,stdout_symbol,KNO_VOID);
  lispval errfile  = kno_getopt(opts,stderr_symbol,KNO_VOID);
  int in_fd[2], out_fd[2], err_fd[2];
  int *in  = (KNO_TRUEP(infile)) ? (in_fd) : (NULL);
  int *out = (KNO_TRUEP(outfile)) ? (out_fd) : (NULL);
  int *err = (KNO_TRUEP(errfile)) ? (err_fd) : (NULL);
  u8_string id = (KNO_STRINGP(idstring)) ?
    (u8_strdup(KNO_CSTRING(idstring))) :
    (makeid(n-1,args+1));
  /* Get pipes */
  if ( (in) && ( (setup_pipe(in)) < 0) ) {
    lispval err = kno_err("PipeFailed","open_subjob",NULL,KNO_VOID);
    return err;}
  else if ( (out) && ( (setup_pipe(out)) < 0) ) {
    lispval err = kno_err("PipeFailed","open_subjob",NULL,KNO_VOID);
    if (in) {close(in[0]); close(in[1]);}
    return err;}
  else if ( (err) && ( (setup_pipe(err)) < 0) ) {
    lispval err = kno_err("PipeFailed","open_subjob",NULL,KNO_VOID);
    if (in) {close(in[0]); close(in[1]);}
    if (out) {close(out[0]); close(out[1]);}
    return err;}
  else {
    pid_t pid = fork();
    if (pid) {
      struct KNO_SUBJOB *subjob = u8_alloc(struct KNO_SUBJOB);
      KNO_INIT_CONS(subjob,kno_subjob_type);
      if (in) close(in[0]);
      if (out) close(out[1]);
      if (err) close(err[1]);
      subjob->subjob_pid = pid;
      subjob->subjob_id = id;

      subjob->subjob_stdin = (in == NULL) ? (kno_incref(infile)) :
	kno_make_port(NULL,(u8_output)u8_open_xoutput(in[1],NULL),
		      u8_mkstring("(in)%s",id));
      if (in) u8_set_blocking(in[1],1);

      subjob->subjob_stdout = (out == NULL) ? (kno_incref(outfile)) :
	kno_make_port((u8_input)u8_open_xinput(out[0],NULL),NULL,
		      u8_mkstring("(out)%s",id));
      if (out) u8_set_blocking(out[0],1);

      subjob->subjob_stderr = (err == NULL) ? (kno_incref(errfile)) :
	kno_make_port((u8_input)u8_open_xinput(err[0],NULL),NULL,
		      u8_mkstring("(err)%s",id));
      if (err) u8_set_blocking(err[0],1);

      kno_decref(infile);
      kno_decref(outfile);
      kno_decref(errfile);
      return (lispval) subjob;}
    else {
      int rv = 0;
      if (in) {
	close(in[1]);
	u8_set_blocking(in[0],1);}
      if (out) {
	close(out[0]);
	u8_set_blocking(out[1],1);}
      if (err) {
	close(err[0]);
	u8_set_blocking(err[1],1);}
      if (in)
	rv = dodup(in[0],STDIN_FILENO,"stdin",id);
      else if (KNO_STRINGP(infile)) {
	int new_stdin = u8_open_fd(KNO_CSTRING(infile),O_RDONLY,0);
	if (new_stdin<0) {
	  u8_exception ex = u8_pop_exception();
	  u8_log(LOGCRIT,RedirectFailed,
		 "Couldn't open stdin %s (%s:%s)",
		 KNO_CSTRING(infile),ex->u8x_cond,ex->u8x_details);
	  u8_free_exception(ex,0);
	  rv=new_stdin;}
	else rv = dodup(in[0],STDIN_FILENO,"stdin",id);}
      else NO_ELSE;
      if (rv<0) {}
      else if (out)
	rv = dodup(out[1],STDOUT_FILENO,"stdout",id);
      else if (KNO_STRINGP(outfile)) {
	int new_stdout = u8_open_fd(KNO_CSTRING(outfile),
				    O_WRONLY|O_CREAT,
				    STDOUT_FILE_MODE);
	if (new_stdout<0) {
	  u8_exception ex = u8_pop_exception();
	  u8_log(LOGCRIT,"Couldn't open stdout %s (%s:%s)",
		 KNO_CSTRING(outfile));
	  u8_free_exception(ex,0);
	  rv=new_stdout;}
	else rv = dodup(new_stdout,STDOUT_FILENO,"stdin",id);}
      else NO_ELSE;
      if (rv<0) {}
      else if (err)
	rv = dodup(err[1],STDERR_FILENO,"stderr",id);
      else if (KNO_STRINGP(errfile)) {
	int new_stderr = u8_open_fd(KNO_CSTRING(errfile),
				    O_WRONLY|O_CREAT,
				    STDERR_FILE_MODE);
	if (new_stderr<0) {
	  u8_exception ex = u8_pop_exception();
	  u8_log(LOGCRIT,"Couldn't open stderr %s (%s:%s)",
		 KNO_CSTRING(errfile),
		 ex->u8x_cond,ex->u8x_details);
	  u8_free_exception(ex,0);
	  rv=new_stderr;}
	else rv = dodup(new_stderr,STDERR_FILENO,"stderr",id);}
      else NO_ELSE;
      if (rv<0) {exit(1);}
      int exec_result = exec_helper("kno_subjob",SUBJOB_EXEC_FLAGS,
				    n-1,opts,args+1);
      if (exec_result < 0) {
	u8_log(LOG_CRIT,"ExecFailed","Couldn't launch subjob %s",id);
	exit(1);}
      /* Never reached */
      return KNO_VOID;}
  }
}

static u8_string makeid(int n,kno_argvec args)
{
  struct U8_OUTPUT idout; U8_INIT_OUTPUT(&idout,64);
  int i = 0; while (i<n) {
    lispval v = args[i];
    if (i>0) u8_putc(&idout,' ');
    if (KNO_STRINGP(v)) {
      u8_string s = KNO_CSTRING(v);
      if (strchr(s,' '))
	kno_unparse(&idout,v);
      else u8_puts(&idout,s);}
    else u8_printf(&idout,"'%q'",v);
    i++;}
  return u8_outstring(&idout);
}

static int unparse_subjob(u8_output out,lispval x)
{
  struct KNO_SUBJOB *sj = (struct KNO_SUBJOB *)x;
  u8_printf(out,"#<SUBJOB/%s>",sj->subjob_id);
  return 1;
}

static void recycle_subjob(struct KNO_RAW_CONS *c)
{
  struct KNO_SUBJOB *sj = (struct KNO_SUBJOB *)c;
  kno_decref(sj->subjob_stdin); sj->subjob_stdin=KNO_VOID;
  kno_decref(sj->subjob_stdout); sj->subjob_stdout=KNO_VOID;
  kno_decref(sj->subjob_stderr); sj->subjob_stderr=KNO_VOID;
  u8_free(sj->subjob_id);
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

DEFC_PRIM("subjob/pid",subjob_pid,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the numeric process ID for the subjob",
	  {"subjob",kno_any_type,KNO_VOID})
static lispval subjob_pid(lispval subjob)
{
  struct KNO_SUBJOB *sj = (kno_subjob) subjob;
  return KNO_INT(sj->subjob_pid);
}

DEFC_PRIM("subjob/stdin",subjob_stdin,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns an output port for sending to the subjob.",
	  {"subjob",kno_any_type,KNO_VOID})
static lispval subjob_stdin(lispval subjob)
{
  struct KNO_SUBJOB *sj = (kno_subjob) subjob;
  return kno_incref(sj->subjob_stdin);
}

DEFC_PRIM("subjob/stdout",subjob_stdout,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns an input port for reading the output of "
	  "subjob.",
	  {"subjob",kno_any_type,KNO_VOID})
static lispval subjob_stdout(lispval subjob)
{
  struct KNO_SUBJOB *sj = (kno_subjob) subjob;
  return kno_incref(sj->subjob_stdout);
}

DEFC_PRIM("subjob/stderr",subjob_stderr,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns an input port for reading the error "
	  "output (stderr)  of subjob.",
	  {"subjob",kno_any_type,KNO_VOID})
static lispval subjob_stderr(lispval subjob)
{
  struct KNO_SUBJOB *sj = (kno_subjob) subjob;
  return kno_incref(sj->subjob_stderr);
}

DEFC_PRIM("subjob/signal",subjob_signal,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "sends the number *signal* to the process "
	  "executing *subjob*.",
	  {"subjob",kno_any_type,KNO_VOID},
	  {"sigval",kno_any_type,KNO_VOID})
static lispval subjob_signal(lispval subjob,lispval sigval)
{
  struct KNO_SUBJOB *sj = (kno_subjob) subjob;
  int sig = KNO_INT(sigval);
  int pid = sj->subjob_pid;
  if ((sig>0)&&(sig<256)) {
    int rv = kill(pid,sig);
    if (rv<0) {
      char buf[128]; sprintf(buf,"pid=%ld;sig=%d",(long int)pid,sig);
      u8_graberrno("subjob_signal",u8_strdup(buf));
      return KNO_ERROR;}
    else return KNO_TRUE;}
  else return kno_type_error("signal","subjob_signal",sigval);
}

/* EXIT functions */

DEFC_PRIM("exit",exit_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "exits the current process with a return code of "
	  "*retval* (defaults to 0)",
	  {"arg",kno_any_type,KNO_VOID})
static lispval exit_prim(lispval arg)
{
  pid_t main_thread = getpid();
  int rv = kill(main_thread,SIGTERM);
  if (rv<0)
    return kno_err("ExitFailed","exit_prim",NULL,VOID);
  else return VOID;
}

DEFC_PRIM("exit/fast",fast_exit_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "exits the current process expeditiously without, "
	  "for example, freeing memory which will just be "
	  "returned to the OS after exit.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval fast_exit_prim(lispval arg)
{
  kno_fast_exit=1;
  pid_t main_thread = getpid();
  int rv = kill(main_thread,SIGTERM);
  if (rv<0)
    return kno_err("ExitFailed","exit_prim",NULL,VOID);
  else return VOID;
}

/* PID functions */

DEFC_PRIM("pid?",ispid_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns #t if it's argument is a current and "
	  "valid process ID.",
	  {"pid_arg",kno_fixnum_type,KNO_VOID})
static lispval ispid_prim(lispval pid_arg)
{
  pid_t pid = FIX2INT(pid_arg);
  int rv = kill(pid,0);
  if (rv<0) {
    errno = 0; return KNO_FALSE;}
  else return KNO_TRUE;
}

DEFC_PRIM("pid/kill!",pid_kill_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "sends *signal* (default is ) to *process*.",
	  {"pid_arg",kno_any_type,KNO_VOID},
	  {"sig_arg",kno_any_type,KNO_VOID})
static lispval pid_kill_prim(lispval pid_arg,lispval sig_arg)
{
  pid_t pid = FIX2INT(pid_arg);
  int sig = FIX2INT(sig_arg);
  if ((sig>0)&&(sig<256)) {
    int rv = kill(pid,sig);
    if (rv<0) {
      char buf[128]; sprintf(buf,"pid=%ld;sig=%d",(long int)pid,sig);
      u8_graberrno("os_kill_prim",u8_strdup(buf));
      return KNO_ERROR;}
    else return KNO_TRUE;}
  else return kno_type_error("signal","pid_kill_prim",sig_arg);
}

/* Generic rlimits */

lispval kno_rlimit_codes = KNO_EMPTY;

DEFC_PRIM("getrlimit",getrlimit_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "gets the *resource* resource limit for the "
	  "current process. If *getmax* is true, gets the "
	  "maximum resource limit.",
	  {"resname",kno_symbol_type,KNO_VOID},
	  {"which",kno_any_type,KNO_VOID})
static lispval getrlimit_prim(lispval resname,lispval which)
{
  struct rlimit lim;
  long long resnum = -1;
  lispval rescode = kno_get(kno_rlimit_codes,resname,KNO_VOID);
  if (KNO_UINTP(rescode))
    resnum = KNO_FIX2INT(rescode);
  else return kno_err(_("BadRLIMITKey"),"setrlimit_prim",NULL,resname);
  int rv = getrlimit(resnum,&lim);
  if (rv < 0) {
    u8_string errstring = u8_strerror(errno); errno=0;
    return kno_err("RLimitFailed","getrlimit_prim",errstring,resname);}
  if (KNO_TRUEP(which)) {
    long long curlim = (long long) lim.rlim_cur;
    long long maxlim = (long long) lim.rlim_max;
    return kno_init_pair(NULL,
			 ( (curlim<0) ? (KNO_FALSE) : (KNO_INT(curlim)) ),
			 ( (maxlim<0) ? (KNO_FALSE) : (KNO_INT(maxlim)) ));}
  else {
    long long curlim = (long long) lim.rlim_cur;
    if (curlim < 0)
      return KNO_FALSE;
    else return KNO_INT(curlim);}
}

DEFC_PRIM("setrlimit!",setrlimit_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "sets the resource limit *resource* (symbol) to "
	  "*value* for the current process. If *setmax* is "
	  "true, sets the maximium resource value if allowed.",
	  {"resname",kno_symbol_type,KNO_VOID},
	  {"limit_val",kno_any_type,KNO_VOID},
	  {"setmax_arg",kno_any_type,KNO_VOID})
static lispval setrlimit_prim(lispval resname,lispval limit_val,
			      lispval setmax_arg)
{

  struct rlimit lim;
  long long resnum = -1;
  long long resval = -1;
  lispval rescode = kno_get(kno_rlimit_codes,resname,KNO_VOID);
  int setmax = (KNO_TRUEP(setmax_arg));
  if (KNO_UINTP(rescode))
    resnum = KNO_FIX2INT(rescode);
  else return kno_err(_("BadRLIMITKey"),"setrlimit_prim",NULL,resname);
  if (KNO_UINTP(limit_val))
    resval = KNO_FIX2INT(limit_val);
  else if ( (KNO_FIXNUMP(limit_val)) || (KNO_FALSEP(limit_val)) )
    resval = RLIM_INFINITY;
  else return kno_err(kno_TypeError,"setrlimit_prim",NULL,limit_val);
  int rv = getrlimit(KNO_FIX2INT(rescode),&lim);
  if (rv < 0) {
    u8_string errstring = u8_strerror(errno); errno=0;
    return kno_err("RLimitFailed","setrlimit_prim/getrlimit",errstring,resname);}
  if (setmax)
    lim.rlim_max = resval;
  else lim.rlim_cur = resval;
  rv = setrlimit(resnum,&lim);
  if (rv < 0) {
    u8_string errstring = u8_strerror(errno); errno=0;
    return kno_err(_("SetRLIMITFailed"),"setrlimit_prim",errstring,resname);}
  return KNO_TRUE;
}

/* RLIMIT tables */

#define DECL_RLIMIT(name)					\
  kno_store(kno_rlimit_codes,kno_intern(# name),KNO_INT(name))
#define DECL_RLIMIT_ALIAS(alias,code)				\
  kno_store(kno_rlimit_codes,kno_intern(alias),KNO_INT(code))

static void init_rlimit_codes()
{
  kno_rlimit_codes = kno_empty_slotmap();
  DECL_RLIMIT(RLIMIT_AS);
  DECL_RLIMIT_ALIAS("VMEM",RLIMIT_AS);
  DECL_RLIMIT(RLIMIT_CORE);
  DECL_RLIMIT_ALIAS("COREFILE",RLIMIT_CORE);
  DECL_RLIMIT(RLIMIT_CPU);
  DECL_RLIMIT_ALIAS("CPU",RLIMIT_CPU);
  DECL_RLIMIT(RLIMIT_DATA);
  DECL_RLIMIT_ALIAS("HEAP",RLIMIT_DATA);
  DECL_RLIMIT(RLIMIT_FSIZE);
  DECL_RLIMIT_ALIAS("FILESIZE",RLIMIT_FSIZE);
#ifdef RLIMIT_LOCKS
  DECL_RLIMIT(RLIMIT_LOCKS);
#endif
#ifdef RLIMIT_MEMLOCK
  DECL_RLIMIT(RLIMIT_MEMLOCK);
#endif
#ifdef RLIMIT_MSGQUEUE
  DECL_RLIMIT(RLIMIT_MSGQUEUE);
#endif
#ifdef RLIMIT_NICE
  DECL_RLIMIT(RLIMIT_NICE);
#endif
  DECL_RLIMIT(RLIMIT_NOFILE);
  DECL_RLIMIT_ALIAS("MAXFILES",RLIMIT_NOFILE);
  DECL_RLIMIT(RLIMIT_NPROC);
  DECL_RLIMIT_ALIAS("MAXPROCS",RLIMIT_NPROC);
  DECL_RLIMIT(RLIMIT_RSS);
  DECL_RLIMIT_ALIAS("RESIDENT",RLIMIT_RSS);
#ifdef RLIMIT_RTPRIO
  DECL_RLIMIT(RLIMIT_RTPRIO);
#endif
#ifdef RLIMIT_RTIME
  DECL_RLIMIT(RLIMIT_RTIME);
#endif
#ifdef RLIMIT_SIGPENDING
  DECL_RLIMIT(RLIMIT_SIGPENDING);
#endif
  DECL_RLIMIT(RLIMIT_STACK);
}

/* Handling procopts */

static lispval nice_symbol = KNO_VOID;

static int handle_procopts(lispval opts)
{
  if ( (KNO_FALSEP(opts)) || (KNO_VOIDP(opts)) ) return 0;
  lispval limit_keys = kno_getkeys(kno_rlimit_codes);
  DO_CHOICES(opt,limit_keys) {
    lispval optval = kno_getopt(opts,opt,KNO_VOID);
    if (KNO_VOIDP(optval)) continue;
    else {
      lispval setval = setrlimit_prim(opt,optval,KNO_FALSE);
      if (KNO_ABORTP(setval)) {
	u8_log(LOGWARN,"RlimitFailed","Couldn't set %q",opt);
	kno_clear_errors(1);}
      else u8_log(LOGNOTICE,"RlimitChanged","Set %q to %q",opt,optval);
      kno_decref(optval);}}
  kno_decref(limit_keys);
  lispval niceval = kno_getopt(opts,nice_symbol,KNO_VOID);
  if (KNO_FIXNUMP(niceval)) {
    int nv = KNO_FIX2INT(niceval); errno = 0;
    int rv = nice(nv);
    if (errno) {
      u8_string errstring = u8_strerror(errno); errno=0;
      u8_log(LOGCRIT,"ReNiceFailed","With %s, setting to %d",
	     errstring,KNO_FIX2INT(niceval));}
    else u8_log(LOGNOTICE,"Renice","To %d for %d",rv,KNO_FIX2INT(niceval));
    return 0;}
  else if (!(KNO_VOIDP(niceval))) {
    u8_log(LOGWARN,"BadNiceValue","handle_procopts %q",niceval);
    return -1;}
  else return 0;
}

/* The nice prim */

DEFC_PRIM("nice",nice_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "Returns or adjusts the priority for the current "
	  "process",
	  {"delta_arg",kno_fixnum_type,KNO_VOID})
static lispval nice_prim(lispval delta_arg)
{
  int delta = (!(KNO_FIXNUMP(delta_arg))) ? (0) : (KNO_FIX2INT(delta_arg));
  errno = 0;
  int rv = nice(delta);
  if (errno) {
    u8_string errstring = u8_strerror(errno); errno=0;
    return kno_err("NiceFailed","nice_prim",errstring,delta);}
  else return KNO_INT(rv);
}

/* The init function */

static int scheme_procprims_initialized = 0;

static lispval procprims_module;

KNO_EXPORT void kno_init_procprims_c()
{
  if (scheme_procprims_initialized) return;
  scheme_procprims_initialized = 1;
  kno_init_scheme();
  procprims_module = kno_new_cmodule
    ("procprims",(KNO_MODULE_DEFAULT),kno_init_procprims_c);
  u8_register_source_file(_FILEINFO);

  init_rlimit_codes();
  link_local_cprims();
#if 0 /* HAVE_WAITPID */
  DECL_PRIM_N(fork_wait_prim,procprims_module);
  DECL_PRIM_N(fork_cmd_wait_prim,procprims_module);
  DECL_PRIM_N(knox_fork_wait_prim,procprims_module);
#endif

  kno_unparsers[kno_subjob_type] = unparse_subjob;
  kno_recyclers[kno_subjob_type] = recycle_subjob;

  id_symbol = kno_intern("id");
  stdin_symbol = kno_intern("stdin");
  stdout_symbol = kno_intern("stdout");
  stderr_symbol = kno_intern("stderr");
  nice_symbol = kno_intern("nice");

  kno_finish_cmodule(procprims_module);
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("nice",nice_prim,1,procprims_module);
  KNO_LINK_CPRIM("setrlimit!",setrlimit_prim,3,procprims_module);
  KNO_LINK_CPRIM("getrlimit",getrlimit_prim,2,procprims_module);
  KNO_LINK_CPRIM("pid/kill!",pid_kill_prim,2,procprims_module);
  KNO_LINK_CPRIM("pid?",ispid_prim,1,procprims_module);
  KNO_LINK_CPRIM("exit/fast",fast_exit_prim,1,procprims_module);
  KNO_LINK_CPRIM("exit",exit_prim,1,procprims_module);
  KNO_LINK_CPRIM("subjob/signal",subjob_signal,2,procprims_module);
  KNO_LINK_CPRIM("subjob/stderr",subjob_stderr,1,procprims_module);
  KNO_LINK_CPRIM("subjob/stdout",subjob_stdout,1,procprims_module);
  KNO_LINK_CPRIM("subjob/stdin",subjob_stdin,1,procprims_module);
  KNO_LINK_CPRIM("subjob/pid",subjob_pid,1,procprims_module);
  KNO_LINK_CPRIMN("subjob/open",subjob_open,procprims_module);
  KNO_LINK_CPRIMN("knox/fork/wait",knox_fork_wait_prim,procprims_module);
  KNO_LINK_CPRIMN("fork/cmd/wait",fork_cmd_wait_prim,procprims_module);
  KNO_LINK_CPRIMN("fork/wait",fork_wait_prim,procprims_module);
  KNO_LINK_CPRIMN("knox/fork",knox_fork_prim,procprims_module);
  KNO_LINK_CPRIMN("fork/cmd",fork_cmd_prim,procprims_module);
  KNO_LINK_CPRIMN("fork",fork_prim,procprims_module);
  KNO_LINK_CPRIMN("knox",knox_prim,procprims_module);
  KNO_LINK_CPRIMN("exec/cmd",exec_cmd_prim,procprims_module);
  KNO_LINK_CPRIMN("exec",exec_prim,procprims_module);
}
