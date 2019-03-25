/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/apply.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/eval.h"
#include "framerd/ports.h"
#include "framerd/fileprims.h"
#include "framerd/procprims.h"

#include "framerd/cprims.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include "libu8/u8fileio.h"
#include <libu8/u8netfns.h>
#include <libu8/u8xfiles.h>

#define fast_eval(x,env) (_fd_fast_eval(x,env,_stack,0))

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

#define FD_IS_SCHEME 1
#define FD_DO_FORK 2
#define FD_DO_WAIT 4
#define FD_DO_LOOKUP 8

static lispval exec_helper(u8_context caller,
                           int flags,lispval opts,
                           int n,lispval *args)
{
  if (!(STRINGP(args[0])))
    return fd_type_error("pathname",caller,args[0]);
  else if ((!(flags&(FD_DO_LOOKUP|FD_IS_SCHEME)))&&
           (!(u8_file_existsp(CSTRING(args[0])))))
    return fd_type_error("real file",caller,args[0]);
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
      lispval keys = fd_getkeys(args[1]);
      max_argc = max_argc+FD_CHOICE_SIZE(keys);}
    argv = u8_alloc_n(max_argc,char *);
    if (flags&FD_IS_SCHEME) {
      argv[argc++]=filename = ((u8_byte *)u8_strdup(FD_EXEC));
      argv[argc++]=(u8_byte *)u8_tolibc(arg1);}
    else if (strchr(arg1,' ')) {
#ifndef FD_EXEC_WRAPPER
      u8_free(argv);
      return fd_err(_("No exec wrapper to handle env args"),
                    caller,NULL,args[0]);
#else
      char *argcopy = u8_tolibc(arg1), *start = argcopy;
      char *scan = strchr(start,' ');
      argv[argc++] = filename = (u8_byte *)u8_strdup(FD_EXEC_WRAPPER);
      while (scan) {
        *scan='\0'; argv[argc++]=start;
        start = scan+1; while (isspace(*start)) start++;
        scan = strchr(start,' ');}
      argv[argc++]=(u8_byte *)u8_tolibc(start);
#endif
    }
    else {
#ifdef FD_EXEC_WRAPPER
      argv[argc++]=filename = (u8_byte *)u8_strdup(FD_EXEC_WRAPPER);
#endif
      if (filename)
        argv[argc++]=u8_tolibc(arg1);
      else argv[argc++]=filename = (u8_byte *)u8_tolibc(arg1);}
    if ((n>1)&&(SLOTMAPP(args[1]))) {
      lispval params = args[1];
      lispval keys = fd_getkeys(args[1]);
      DO_CHOICES(key,keys) {
        if ((SYMBOLP(key))||(STRINGP(key))) {
          lispval value = fd_get(params,key,VOID);
          if (!(VOIDP(value))) {
            struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
            u8_string stringval = NULL;
            if (SYMBOLP(key)) stringval = SYM_NAME(key);
            else stringval = CSTRING(key);
            if (stringval) u8_puts(&out,stringval);
            u8_putc(&out,'=');
            if (STRINGP(value)) u8_puts(&out,CSTRING(value));
            else fd_unparse(&out,value);
            argv[argc++]=out.u8_outbuf;
            fd_decref(value);}}}
        i++;}
    while (i<n) {
      if (STRINGP(args[i]))
        argv[argc++]=u8_tolibc(CSTRING(args[i++]));
      else {
        u8_string as_string = fd_lisp2string(args[i++]);
        char *as_libc_string = u8_tolibc(as_string);
        argv[argc++]=as_libc_string; u8_free(as_string);}}
    argv[argc++]=NULL;
    if ((flags&FD_DO_FORK)&&((pid = (fork())))) {
      i = 0; while (i<argc) {
        if (argv[i]) u8_free(argv[i++]); else i++;}
      u8_free(argv);
#if HAVE_WAITPID
      if (flags&FD_DO_WAIT) {
        unsigned int retval = -1;
        waitpid(pid,&retval,0);
        return FD_INT(retval);}
#endif
      return FD_INT(pid);}
    handle_procopts(opts);
    if (flags&FD_IS_SCHEME)
      retval = execvp(FD_EXEC,argv);
    else if (flags&FD_DO_LOOKUP)
      retval = execvp(filename,argv);
    else retval = execvp(filename,argv);
    u8_log(LOG_CRIT,caller,"Fork exec failed (%d/%d:%s) with %s %s (%d)",
           retval,errno,strerror(errno),
           filename,argv[0],argc);
    /* We call abort in this case because we've forked but couldn't
       exec and we don't want this FramerD executable to exit normally. */
    if (flags&FD_DO_FORK) {
      u8_graberrno(caller,filename);
      return FD_ERROR;}
    else {
      fd_clear_errors(1);
      abort();}}
}

DCLPRIM("EXEC",exec_prim,MIN_ARGS(1)|FD_VAR_ARGS,
        "`(EXEC *command* [*envmap*] [*args*...])` replaces "
        "the current application with an execution of "
        "*command* (a string) to *args* (also strings).\n"
        "*envmap*, if provided, is a slotmap specifying environment "
        "variables for execution of the command. Environment variables "
        "can also be explicitly provided in the string *command*.\n"
        "The last word of *command* (after any environment variables) "
        "should be the path of an executable program file.")
static lispval exec_prim(int n,lispval *args)
{
  return exec_helper("exec_prim",0,n,FD_FALSE,args);
}

DCLPRIM("EXEC/CMD",exec_cmd_prim,MIN_ARGS(1)|FD_VAR_ARGS,
        "`(EXEC/CMD *command* [*envmap*] [*args*...])` replaces "
        "the current application with an execution of "
        "*command* (a string) to *args* (also strings).\n"
        "*envmap*, if provided, is a slotmap specifying environment "
        "variables for execution of the command. Environment variables "
        "can also be explicitly provided in the string *command*.\n"
        "The last word of *command* (after any environment variables) "
        "should be either a command in the default search path or "
        "the path of an executable program file.")
static lispval exec_cmd_prim(int n,lispval *args)
{
  return exec_helper("exec_cmd_prim",FD_DO_LOOKUP,n,FD_FALSE,args);
}

DCLPRIM("FDEXEC",fdexec_prim,MIN_ARGS(1)|FD_VAR_ARGS,
        "`(FDEXEC *scheme_file* [*envmap*] [*args*...])` replaces "
        "the current application with a FramerD process reading "
        "the file *scheme_file* and applying the file's `MAIN` "
        "definition to the results of parsing *args* (also strings).\n"
        "*envmap*, if provided, specifies CONFIG settings for the "
        "reading and execution of *scheme-file*. Environment variables "
        "can also be explicitly provided in the string *command*.\n")
static lispval fdexec_prim(int n,lispval *args)
{
  return exec_helper("fdexec_prim",FD_IS_SCHEME,n,FD_FALSE,args);
}

DCLPRIM("FORK",fork_prim,MIN_ARGS(1)|FD_VAR_ARGS,
        "`(FORK *command* [*envmap*] [*args*...])` 'forks' "
        "a new process executing *command* (a string) for "
        "*args* (also strings). It returns the PID of the new process.\n"
        "*envmap*, if provided, is a slotmap specifying environment "
        "variables for execution of the command. Environment variables "
        "can also be explicitly provided in the string *command*.\n"
        "The last word of *command* (after any environment variables) "
        "should be the path of an executable program file.")
static lispval fork_prim(int n,lispval *args)
{
  if (n==0) {
    pid_t pid = fork();
    if (pid) {
      long long int_pid = (long long) pid;
      return FD_INT2DTYPE(int_pid);}
    else return FD_FALSE;}
  else return exec_helper("fork_prim",FD_DO_FORK,n,FD_FALSE,args);
}

DCLPRIM("FORK/CMD",fork_cmd_prim,MIN_ARGS(1)|FD_VAR_ARGS,
        "`(FOR/CMD *command* [*envmap*] [*args*...])` 'forks' "
        "a new process executing *command* (a string) "
        "with *args* (also strings). It returns the PID of the "
        "new process.\n"
        "*envmap*, if provided, is a slotmap specifying environment "
        "variables for execution of the command. Environment variables "
        "can also be explicitly provided in the string *command*.\n"
        "The last word of *command* (after any environment variables) "
        "should be either a command in the default search path or "
        "the path of an executable program file.")
static lispval fork_cmd_prim(int n,lispval *args)
{
  return exec_helper("fork_cmd_prim",(FD_DO_FORK|FD_DO_LOOKUP),n,FD_FALSE,args);
}

DCLPRIM("FDFORK",fdfork_prim,MIN_ARGS(1)|FD_VAR_ARGS,
        "`(FDFORK *scheme_file* [*envmap*] [*args*...])` 'forks' "
        "a new FramerD process reading the file *scheme_file* and "
        "applying the file's `MAIN` definition to the results of "
        "parsing *args* (also strings).  It returns the PID of the "
        "new process.\n"
        "*envmap*, if provided, specifies CONFIG settings for the "
        "reading and execution of *scheme-file*. Environment variables "
        "can also be explicitly provided in the string *command*.\n")
static lispval fdfork_prim(int n,lispval *args)
{
  return exec_helper("fdfork_prim",(FD_IS_SCHEME|FD_DO_FORK),n,FD_FALSE,args);
}

DCLPRIM("FORK/WAIT",fork_wait_prim,MIN_ARGS(1)|FD_VAR_ARGS,
        "`(FORK *command* [*envmap*] [*args*...])` 'forks' "
        "a new process executing *command* (a string) for "
        "*args* (also strings). It waits for this process to "
        "return and returns its exit status.\n"
        "*envmap*, if provided, is a slotmap specifying environment "
        "variables for execution of the command. Environment variables "
        "can also be explicitly provided in the string *command*.\n"
        "The last word of *command* (after any environment variables) "
        "should be the path of an executable program file.")
static lispval fork_wait_prim(int n,lispval *args)
{
  return exec_helper("fork_wait_prim",(FD_DO_FORK|FD_DO_WAIT),n,FD_FALSE,args);
}

DCLPRIM("FORK/CMD/WAIT",fork_cmd_wait_prim,MIN_ARGS(1)|FD_VAR_ARGS,
        "`(FOR/CMD *command* [*envmap*] [*args*...])` 'forks' "
        "a new process executing *command* (a string) "
        "with *args* (also strings). It waits for this process to "
        "return and returns its exit status.\n"
        "*envmap*, if provided, is a slotmap specifying environment "
        "variables for execution of the command. Environment variables "
        "can also be explicitly provided in the string *command*.\n"
        "The last word of *command* (after any environment variables) "
        "should be either a command in the default search path or "
        "the path of an executable program file.")
static lispval fork_cmd_wait_prim(int n,lispval *args)
{
  return exec_helper("fork_cmd_wait_prim",
                     (FD_DO_FORK|FD_DO_LOOKUP|FD_DO_WAIT),n,FD_FALSE,args);
}

DCLPRIM("FDFORK/WAIT",fdfork_wait_prim,MIN_ARGS(1)|FD_VAR_ARGS,
        "`(FDFORK *scheme_file* [*envmap*] [*args*...])` 'forks' "
        "a new FramerD process reading the file *scheme_file* and "
        "applying the file's `MAIN` definition to the results of "
        "parsing *args* (also strings).  It waits for this process to "
        "return and returns its exit status.\n"
        "*envmap*, if provided, specifies CONFIG settings for the "
        "reading and execution of *scheme-file*. Environment variables "
        "can also be explicitly provided in the string *command*.\n")
static lispval fdfork_wait_prim(int n,lispval *args)
{
  return exec_helper("fdfork_wait_prim",
                     (FD_IS_SCHEME|FD_DO_FORK|FD_DO_WAIT),
                     n,FD_FALSE,args);
}

/* SUBJOBs */

fd_ptr_type fd_subjob_type;

#define PIPE_FLAGS O_NONBLOCK
#define STDOUT_FILE_MODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)
#define STDERR_FILE_MODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)
#define SUBJOB_EXEC_FLAGS FD_DO_LOOKUP

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

static u8_string makeid(int n,lispval *args);

DCLPRIM("SUBJOB/OPEN",subjob_open,MIN_ARGS(1)|FD_VAR_ARGS,
        "`(SUBJOB/OPEN *opts* *command* [*envmap*] [*args*...])` "
        "'forks' a new process applying *command* to *args* and "
        "creates a **subjob** object for the process.\n"
        "*opts* control how the subjob is started and "
        "how its inputs and outputs are configured.\n"
        "Some supported options are:\n"
        "* **ID** provides a descriptive string;\n"
        "* **STDIN** is either a file to use as the *stdin* "
        "to the new process, #f to use the *standard input* of the "
        "current process, or #t to create a stream through which "
        "the current process can write to the new process.\n"
        "* **STDOUT** is either a file to use for the *stdout* "
        "from the new process, #f to share the *standard output* of the "
        "current process, or #t to create a stream from which "
        "the current process can read the standard output of "
        "the new process.\n"
        "* **STDERR** is either a file to use for the *stderr* "
        "for the new process, #f to share the *stderr* of the "
        "current process, or #t to create a stream from which "
        "the current process can read the error output of "
        "the new process.\n")
static lispval subjob_open(int n,lispval *args)
{
  lispval opts = args[0];
  lispval idstring = fd_getopt(opts,id_symbol,FD_VOID);
  lispval infile   = fd_getopt(opts,stdin_symbol,FD_VOID);
  lispval outfile  = fd_getopt(opts,stdout_symbol,FD_VOID);
  lispval errfile  = fd_getopt(opts,stderr_symbol,FD_VOID);
  int in_fd[2], out_fd[2], err_fd[2];
  int *in  = (FD_TRUEP(infile)) ? (in_fd) : (NULL);
  int *out = (FD_TRUEP(outfile)) ? (out_fd) : (NULL);
  int *err = (FD_TRUEP(errfile)) ? (err_fd) : (NULL);
  u8_string id = (FD_STRINGP(idstring)) ?
    (u8_strdup(FD_CSTRING(idstring))) :
    (makeid(n-1,args+1));
  /* Get pipes */
  if ( (in) && ( (setup_pipe(in)) < 0) ) {
    lispval err = fd_err("PipeFailed","open_subjob",NULL,FD_VOID);
    return err;}
  else if ( (out) && ( (setup_pipe(out)) < 0) ) {
    lispval err = fd_err("PipeFailed","open_subjob",NULL,FD_VOID);
    if (in) {close(in[0]); close(in[1]);}
    return err;}
  else if ( (err) && ( (setup_pipe(err)) < 0) ) {
    lispval err = fd_err("PipeFailed","open_subjob",NULL,FD_VOID);
    if (in) {close(in[0]); close(in[1]);}
    if (out) {close(out[0]); close(out[1]);}
    return err;}
  else {
    pid_t pid = fork();
    if (pid) {
      struct FD_SUBJOB *subjob = u8_alloc(struct FD_SUBJOB);
      FD_INIT_CONS(subjob,fd_subjob_type);
      if (in) close(in[0]);
      if (out) close(out[1]);
      if (err) close(err[1]);
      subjob->subjob_pid = pid;
      subjob->subjob_id = id;

      subjob->subjob_stdin = (in == NULL) ? (fd_incref(infile)) :
        fd_make_port(NULL,(u8_output)u8_open_xoutput(in[1],NULL),
                     u8_mkstring("(in)%s",id));
      if (in) u8_set_blocking(in[1],1);

      subjob->subjob_stdout = (out == NULL) ? (fd_incref(outfile)) :
        fd_make_port((u8_input)u8_open_xinput(out[0],NULL),NULL,
                     u8_mkstring("(out)%s",id));
      if (out) u8_set_blocking(out[0],1);

      subjob->subjob_stderr = (err == NULL) ? (fd_incref(errfile)) :
        fd_make_port((u8_input)u8_open_xinput(err[0],NULL),NULL,
                     u8_mkstring("(err)%s",id));
      if (err) u8_set_blocking(err[0],1);

      fd_decref(infile);
      fd_decref(outfile);
      fd_decref(errfile);
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
      else if (FD_STRINGP(infile)) {
        int new_stdin = u8_open_fd(FD_CSTRING(infile),O_RDONLY,0);
        if (new_stdin<0) {
          u8_exception ex = u8_pop_exception();
          u8_log(LOGCRIT,RedirectFailed,
                 "Couldn't open stdin %s (%s:%s)",
                 FD_CSTRING(infile),ex->u8x_cond,ex->u8x_details);
          u8_free_exception(ex,0);
          rv=new_stdin;}
        else rv = dodup(in[0],STDIN_FILENO,"stdin",id);}
      else NO_ELSE;
      if (rv<0) {}
      else if (out)
        rv = dodup(out[1],STDOUT_FILENO,"stdout",id);
      else if (FD_STRINGP(outfile)) {
        int new_stdout = u8_open_fd(FD_CSTRING(outfile),
                                    O_WRONLY|O_CREAT,
                                    STDOUT_FILE_MODE);
        if (new_stdout<0) {
          u8_exception ex = u8_pop_exception();
          u8_log(LOGCRIT,"Couldn't open stdout %s (%s:%s)",
                 FD_CSTRING(outfile));
          u8_free_exception(ex,0);
          rv=new_stdout;}
        else rv = dodup(new_stdout,STDOUT_FILENO,"stdin",id);}
      else NO_ELSE;
      if (rv<0) {}
      else if (err)
        rv = dodup(err[1],STDERR_FILENO,"stderr",id);
      else if (FD_STRINGP(errfile)) {
        int new_stderr = u8_open_fd(FD_CSTRING(errfile),
                                    O_WRONLY|O_CREAT,
                                    STDERR_FILE_MODE);
        if (new_stderr<0) {
          u8_exception ex = u8_pop_exception();
          u8_log(LOGCRIT,"Couldn't open stderr %s (%s:%s)",
                 FD_CSTRING(errfile),
                 ex->u8x_cond,ex->u8x_details);
          u8_free_exception(ex,0);
          rv=new_stderr;}
        else rv = dodup(new_stderr,STDERR_FILENO,"stderr",id);}
      else NO_ELSE;
      if (rv<0) {exit(1);}
      int exec_result = exec_helper("fd_subjob",SUBJOB_EXEC_FLAGS,
                                    n-1,opts,args+1);
      if (exec_result < 0) {
        u8_log(LOG_CRIT,"ExecFailed","Couldn't launch subjob %s",id);
        exit(1);}
      /* Never reached */
      return FD_VOID;}
  }
}

static u8_string makeid(int n,lispval *args)
{
  struct U8_OUTPUT idout; U8_INIT_OUTPUT(&idout,64);
  int i = 0; while (i<n) {
    lispval v = args[i];
    if (i>0) u8_putc(&idout,' ');
    if (FD_STRINGP(v)) {
      u8_string s = FD_CSTRING(v);
      if (strchr(s,' '))
        fd_unparse(&idout,v);
      else u8_puts(&idout,s);}
    else u8_printf(&idout,"'%q'",v);
    i++;}
  return u8_outstring(&idout);
}

static int unparse_subjob(u8_output out,lispval x)
{
  struct FD_SUBJOB *sj = (struct FD_SUBJOB *)x;
  u8_printf(out,"#<SUBJOB/%s>",sj->subjob_id);
  return 1;
}

static void recycle_subjob(struct FD_RAW_CONS *c)
{
  struct FD_SUBJOB *sj = (struct FD_SUBJOB *)c;
  fd_decref(sj->subjob_stdin); sj->subjob_stdin=FD_VOID;
  fd_decref(sj->subjob_stdout); sj->subjob_stdout=FD_VOID;
  fd_decref(sj->subjob_stderr); sj->subjob_stderr=FD_VOID;
  u8_free(sj->subjob_id);
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

DCLPRIM("SUBJOB/PID",subjob_pid,MIN_ARGS(1)|MAX_ARGS(1),
        "Returns the numeric process ID for the subjob")
static lispval subjob_pid(lispval subjob)
{
  struct FD_SUBJOB *sj = (fd_subjob) subjob;
  return FD_INT(sj->subjob_pid);
}

DCLPRIM("SUBJOB/STDIN",subjob_stdin,MIN_ARGS(1)|MAX_ARGS(1),
        "Returns an output port for sending to the subjob.")
static lispval subjob_stdin(lispval subjob)
{
  struct FD_SUBJOB *sj = (fd_subjob) subjob;
  return fd_incref(sj->subjob_stdin);
}

DCLPRIM("SUBJOB/STDOUT",subjob_stdout,MIN_ARGS(1)|MAX_ARGS(1),
        "Returns an input port for reading the output of subjob.")
static lispval subjob_stdout(lispval subjob)
{
  struct FD_SUBJOB *sj = (fd_subjob) subjob;
  return fd_incref(sj->subjob_stdout);
}

DCLPRIM("SUBJOB/STDERR",subjob_stderr,MIN_ARGS(1)|MAX_ARGS(1),
        "Returns an input port for reading the error output (stderr) "
        " of subjob.")
static lispval subjob_stderr(lispval subjob)
{
  struct FD_SUBJOB *sj = (fd_subjob) subjob;
  return fd_incref(sj->subjob_stderr);
}

DCLPRIM("SUBJOB/SIGNAL",subjob_signal,MIN_ARGS(2)|MAX_ARGS(2),
        "`(SUBJOB/SIGNAL *subjob* *signal*)` sends the number "
        "*signal* to the process executing *subjob*.")
static lispval subjob_signal(lispval subjob,lispval sigval)
{
  struct FD_SUBJOB *sj = (fd_subjob) subjob;
  int sig = FD_INT(sigval);
  int pid = sj->subjob_pid;
  if ((sig>0)&&(sig<256)) {
    int rv = kill(pid,sig);
    if (rv<0) {
      char buf[128]; sprintf(buf,"pid=%ld;sig=%d",(long int)pid,sig);
      u8_graberrno("subjob_signal",u8_strdup(buf));
      return FD_ERROR;}
    else return FD_TRUE;}
  else return fd_type_error("signal","subjob_signal",sigval);
}

/* EXIT functions */

DCLPRIM("EXIT",exit_prim,MIN_ARGS(0)|MAX_ARGS(1),
        "`(EXIT [*retval*])` exits the current process with "
        "a return code of *retval* (defaults to 0)")
static lispval exit_prim(lispval arg)
{
  if (FD_INTP(arg))
    exit(FIX2INT(arg));
  else exit(0);
  return VOID;
}

DCLPRIM("EXIT/FAST",fast_exit_prim,MIN_ARGS(0)|MAX_ARGS(1),
        "`(EXIT/FAST [*retval*])` exits the current process "
        "expeditiously without, for example, freeing memory "
        "which will just be returned to the OS after exit.")
static lispval fast_exit_prim(lispval arg)
{
  fd_fast_exit=1;
  if (FD_INTP(arg))
    exit(FIX2INT(arg));
  else exit(0);
  return VOID;
}

/* PID functions */

DCLPRIM1("PID?",ispid_prim,MIN_ARGS(1),
         "Returns #t if it's argument is a current and valid "
         "process ID.",
         fd_fixnum_type,FD_VOID)
static lispval ispid_prim(lispval pid_arg)
{
  pid_t pid = FIX2INT(pid_arg);
  int rv = kill(pid,0);
  if (rv<0) {
    errno = 0; return FD_FALSE;}
  else return FD_TRUE;
}

DCLPRIM2("PID/KILL!",pid_kill_prim,MIN_ARGS(1),
         "`(PID/KILL *process* [*signal*])` "
         "sends *signal* (default is ) to "
         "*process*.",
         -1,FD_VOID,-1,FD_VOID)
static lispval pid_kill_prim(lispval pid_arg,lispval sig_arg)
{
  pid_t pid = FIX2INT(pid_arg);
  int sig = FIX2INT(sig_arg);
  if ((sig>0)&&(sig<256)) {
    int rv = kill(pid,sig);
    if (rv<0) {
      char buf[128]; sprintf(buf,"pid=%ld;sig=%d",(long int)pid,sig);
      u8_graberrno("os_kill_prim",u8_strdup(buf));
      return FD_ERROR;}
    else return FD_TRUE;}
  else return fd_type_error("signal","pid_kill_prim",sig_arg);
}

/* Generic rlimits */

lispval fd_rlimit_codes = FD_EMPTY;

DCLPRIM2("GETRLIMIT",getrlimit_prim,MAX_ARGS(2)|MIN_ARGS(1),
         "`(GETRLIMIT *resource* [*getmax*])` gets the *resource* "
         "resource limit for the current process. If *getmax* is true, "
         "gets the maximum resource limit.",
         fd_symbol_type,FD_VOID,-1,FD_VOID)
static lispval getrlimit_prim(lispval resname,lispval which)
{
  struct rlimit lim;
  long long resnum = -1;
  lispval rescode = fd_get(fd_rlimit_codes,resname,FD_VOID);
  if (FD_UINTP(rescode))
    resnum = FD_FIX2INT(rescode);
  else return fd_err(_("BadRLIMITKey"),"setrlimit_prim",NULL,resname);
  int rv = getrlimit(resnum,&lim);
  if (rv < 0) {
    u8_string errstring = u8_strerror(errno); errno=0;
    return fd_err("RLimitFailed","getrlimit_prim",errstring,resname);}
  if (FD_TRUEP(which)) {
    long long curlim = (long long) lim.rlim_cur;
    long long maxlim = (long long) lim.rlim_max;
    return fd_init_pair(NULL,
                        ( (curlim<0) ? (FD_FALSE) : (FD_INT(curlim)) ),
                        ( (maxlim<0) ? (FD_FALSE) : (FD_INT(maxlim)) ));}
  else {
    long long curlim = (long long) lim.rlim_cur;
    if (curlim < 0)
      return FD_FALSE;
    else return FD_INT(curlim);}
}

DCLPRIM3("SETRLIMIT!",setrlimit_prim,MAX_ARGS(2)|MIN_ARGS(2),
         "`(SETRLIMIT! *resource* *value* [*setmax*])` sets the resource "
         "limit *resource* (symbol) to *value* for the current process. If "
         "*setmax* is true, sets the maximium resource value if allowed.",
         fd_symbol_type,FD_VOID,-1,FD_VOID,-1,FD_VOID)
static lispval setrlimit_prim(lispval resname,lispval limit_val,
                              lispval setmax_arg)
{

  struct rlimit lim;
  long long resnum = -1;
  long long resval = -1;
  lispval rescode = fd_get(fd_rlimit_codes,resname,FD_VOID);
  int setmax = (FD_TRUEP(setmax_arg));
  if (FD_UINTP(rescode))
    resnum = FD_FIX2INT(rescode);
  else return fd_err(_("BadRLIMITKey"),"setrlimit_prim",NULL,resname);
  if (FD_UINTP(limit_val))
    resval = FD_FIX2INT(limit_val);
  else if ( (FD_FIXNUMP(limit_val)) || (FD_FALSEP(limit_val)) )
    resval = RLIM_INFINITY;
  else return fd_err(fd_TypeError,"setrlimit_prim",NULL,limit_val);
  int rv = getrlimit(FD_FIX2INT(rescode),&lim);
  if (rv < 0) {
    u8_string errstring = u8_strerror(errno); errno=0;
    return fd_err("RLimitFailed","setrlimit_prim/getrlimit",errstring,resname);}
  if (setmax)
    lim.rlim_max = resval;
  else lim.rlim_cur = resval;
  rv = setrlimit(resnum,&lim);
  if (rv < 0) {
    u8_string errstring = u8_strerror(errno); errno=0;
    return fd_err(_("SetRLIMITFailed"),"setrlimit_prim",errstring,resname);}
  return FD_TRUE;
}

/* RLIMIT tables */

#define DECL_RLIMIT(name) \
  fd_store(fd_rlimit_codes,fd_intern(# name),FD_INT(name))
#define DECL_RLIMIT_ALIAS(alias,code)                                 \
  fd_store(fd_rlimit_codes,fd_intern(alias),FD_INT(code))

static void init_rlimit_codes()
{
  fd_rlimit_codes = fd_empty_slotmap();
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

static lispval nice_symbol = FD_VOID;

static int handle_procopts(lispval opts)
{
  if ( (FD_FALSEP(opts)) || (FD_VOIDP(opts)) ) return 0;
  lispval limit_keys = fd_getkeys(fd_rlimit_codes);
  DO_CHOICES(opt,limit_keys) {
    lispval optval = fd_getopt(opts,opt,FD_VOID);
    if (FD_VOIDP(optval)) continue;
    else {
      lispval setval = setrlimit_prim(opt,optval,FD_FALSE);
      if (FD_ABORTP(setval)) {
        u8_log(LOGWARN,"RlimitFailed","Couldn't set %q",opt);
        fd_clear_errors(1);}
      else u8_log(LOGNOTICE,"RlimitChanged","Set %q to %q",opt,optval);
      fd_decref(optval);}}
  fd_decref(limit_keys);
  lispval niceval = fd_getopt(opts,nice_symbol,FD_VOID);
  if (FD_FIXNUMP(niceval)) {
    int nv = FD_FIX2INT(niceval); errno = 0;
    int rv = nice(nv);
    if (errno) {
      u8_string errstring = u8_strerror(errno); errno=0;
      u8_log(LOGCRIT,"ReNiceFailed","With %s, setting to %d",
             errstring,FD_FIX2INT(niceval));}
    else u8_log(LOGNOTICE,"Renice","To %d for %d",rv,FD_FIX2INT(niceval));
    return 0;}
  else if (!(FD_VOIDP(niceval))) {
    u8_log(LOGWARN,"BadNiceValue","handle_procopts %q",niceval);
    return -1;}
  else return 0;
 }

/* The nice prim */

DCLPRIM1("NICE",nice_prim,MIN_ARGS(0),
         "Returns or adjusts the priority for the current process",
         fd_fixnum_type,FD_VOID)
static lispval nice_prim(lispval delta_arg)
{
  int delta = (!(FD_FIXNUMP(delta_arg))) ? (0) : (FD_FIX2INT(delta_arg));
  errno = 0;
  int rv = nice(delta);
  if (errno) {
    u8_string errstring = u8_strerror(errno); errno=0;
    return fd_err("NiceFailed","nice_prim",errstring,delta);}
  else return FD_INT(rv);
}

/* The init function */

static int scheme_procprims_initialized = 0;

FD_EXPORT void fd_init_procprims_c()
{
  lispval procprims_module;
  if (scheme_procprims_initialized) return;
  scheme_procprims_initialized = 1;
  fd_init_scheme();
  procprims_module =
    fd_new_cmodule("PROCPRIMS",(FD_MODULE_DEFAULT),fd_init_procprims_c);
  u8_register_source_file(_FILEINFO);

  init_rlimit_codes();

  DECL_PRIM_N(exec_prim,procprims_module);
  DECL_PRIM_N(exec_cmd_prim,procprims_module);
  DECL_PRIM_N(fork_prim,procprims_module);
  DECL_PRIM_N(fork_cmd_prim,procprims_module);

  DECL_PRIM_N(fdexec_prim,procprims_module);
  DECL_PRIM_N(fdfork_prim,procprims_module);

#if HAVE_WAITPID
  DECL_PRIM_N(fork_wait_prim,procprims_module);
  DECL_PRIM_N(fork_cmd_wait_prim,procprims_module);
  DECL_PRIM_N(fdfork_wait_prim,procprims_module);
#endif

  DECL_PRIM(nice_prim,1,procprims_module);

  fd_subjob_type = fd_register_cons_type("subjob");
  fd_unparsers[fd_subjob_type] = unparse_subjob;
  fd_recyclers[fd_subjob_type] = recycle_subjob;

  id_symbol = fd_intern("ID");
  stdin_symbol = fd_intern("STDIN");
  stdout_symbol = fd_intern("STDOUT");
  stderr_symbol = fd_intern("STDERR");
  nice_symbol = fd_intern("NICE");

  DECL_PRIM_N(subjob_open,procprims_module);

  int subjob_prim_types[1] = { fd_subjob_type };
  DECL_PRIM_ARGS(subjob_pid,1,procprims_module,
                 subjob_prim_types,NULL);
  DECL_PRIM_ARGS(subjob_stdin,1,procprims_module,
                 subjob_prim_types,NULL);
  DECL_PRIM_ARGS(subjob_stdout,1,procprims_module,
                 subjob_prim_types,NULL);
  DECL_PRIM_ARGS(subjob_stderr,1,procprims_module,
                 subjob_prim_types,NULL);

  int subjob_signal_types[2] = { fd_subjob_type, fd_fixnum_type };
  DECL_PRIM_ARGS(subjob_signal,2,procprims_module,
                 subjob_signal_types,NULL);

  DECL_PRIM(ispid_prim,1,procprims_module);
  DECL_PRIM(pid_kill_prim,2,procprims_module);
  DECL_PRIM(exit_prim,1,procprims_module);
  DECL_PRIM(fast_exit_prim,1,procprims_module);

  DECL_PRIM(getrlimit_prim,2,fd_scheme_module);
  DECL_PRIM(setrlimit_prim,3,fd_scheme_module);

  fd_finish_module(procprims_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
