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

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
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

#if ((HAVE_SYS_VFS_H)&&(HAVE_STATFS))
#include <sys/vfs.h>
#elif ((HAVE_SYS_FSTAT_H)&&(HAVE_STATFS))
#include <sys/statfs.h>
#endif

static lispval exit_prim(lispval arg)
{
  if (FD_INTP(arg)) exit(FIX2INT(arg));
  else exit(0);
  return VOID;
}

static lispval fast_exit_prim(lispval arg)
{
  fd_fast_exit=1;
  if (FD_INTP(arg))
    exit(FIX2INT(arg));
  else exit(0);
  return VOID;
}

static lispval ispid_prim(lispval pid_arg)
{
  pid_t pid = FIX2INT(pid_arg);
  int rv = kill(pid,0);
  if (rv<0) {
    errno = 0; return FD_FALSE;}
  else return FD_TRUE;
}

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

#define FD_IS_SCHEME 1
#define FD_DO_FORK 2
#define FD_DO_WAIT 4
#define FD_DO_LOOKUP 8

static lispval exec_helper(u8_context caller,int flags,int n,lispval *args)
{
  if (!(STRINGP(args[0])))
    return fd_type_error("pathname",caller,args[0]);
  else if ((!(flags&(FD_DO_LOOKUP|FD_IS_SCHEME)))&&
           (!(u8_file_existsp(CSTRING(args[0])))))
    return fd_type_error("real file",caller,args[0]);
  else {
    char **argv;
    u8_byte *arg1 = (u8_byte *)CSTRING(args[0]);
    u8_byte *filename = NULL;
    int i = 1, argc = 0, max_argc = n+2, retval = 0; pid_t pid;
    if (strchr(arg1,' ')) {
      const char *scan = arg1; while (scan) {
        const char *brk = strchr(scan,' ');
        if (brk) {max_argc++; scan = brk+1;}
        else break;}}
    else {}
    if ((n>1)&&(SLOTMAPP(args[1]))) {
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
      char *argcopy = u8_tolibc(arg1), *start = argcopy, *scan = strchr(start,' ');
      argv[argc++]=filename = (u8_byte *)u8_strdup(FD_EXEC_WRAPPER);
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
      i = 0; while (i<argc) if (argv[i]) u8_free(argv[i++]); else i++;
      u8_free(argv);
#if HAVE_WAITPID
      if (flags&FD_DO_WAIT) {
        unsigned int retval = -1;
        waitpid(pid,&retval,0);
        return FD_INT(retval);}
#endif
      return FD_INT(pid);}
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

static lispval exec_prim(int n,lispval *args)
{
  return exec_helper("exec_prim",0,n,args);
}

static lispval exec_cmd_prim(int n,lispval *args)
{
  return exec_helper("exec_cmd_prim",FD_DO_LOOKUP,n,args);
}

static lispval fdexec_prim(int n,lispval *args)
{
  return exec_helper("fdexec_prim",FD_IS_SCHEME,n,args);
}

static lispval fork_prim(int n,lispval *args)
{
  if (n==0) {
    pid_t pid = fork();
    if (pid) {
      long long int_pid = (long long) pid;
      return FD_INT2DTYPE(int_pid);}
    else return FD_FALSE;}
  else return exec_helper("fork_prim",FD_DO_FORK,n,args);
}

static lispval fork_cmd_prim(int n,lispval *args)
{
  return exec_helper("fork_cmd_prim",(FD_DO_FORK|FD_DO_LOOKUP),n,args);
}

static lispval fork_wait_prim(int n,lispval *args)
{
  return exec_helper("fork_wait_prim",(FD_DO_FORK|FD_DO_WAIT),n,args);
}

static lispval fork_cmd_wait_prim(int n,lispval *args)
{
  return exec_helper("fork_cmd_wait_prim",
                     (FD_DO_FORK|FD_DO_LOOKUP|FD_DO_WAIT),n,args);
}

static lispval fdfork_wait_prim(int n,lispval *args)
{
  return exec_helper("fdfork_wait_prim",
                     (FD_IS_SCHEME|FD_DO_FORK|FD_DO_WAIT),
                     n,args);
}

static lispval fdfork_prim(int n,lispval *args)
{
  return exec_helper("fdfork_prim",(FD_IS_SCHEME|FD_DO_FORK),n,args);
}

/* SUBJOBs */

fd_ptr_type fd_subjob_type;

#define PIPE_FLAGS O_NONBLOCK

#if 0
static lispval open_subjob(int n,lisp *args)
{
  lispval opts = args[0];
  lispval id = fd_getopt(opts,FDSYM_ID,FD_VOID);
  lispval infile = fd_getopt(opts,infile_symbol,FD_VOID);
  lispval outfile = fd_getopt(opts,outfile_symbol,FD_VOID);
  lispval errfile = fd_getopt(opts,errfile_symbol,FD_VOID);
  int in_fd[2], out_fd[2], err_fd[2];
  int *in  = (FD_VOIDP(infile)) ? (in_fd) : (NULL);
  int *out = (FD_VOIDP(outfile)) ? (out_fd) : (NULL);
  int *err = (FD_VOIDP(errfile)) ? (err_fd) : (NULL);
  /* Get pipes */
  if ( (in) && ( (pipe2(in,PIPE_FLAGS)) < 0) ) {
    lispval err = fd_err("PipeFailed","open_subjob",NULL,FD_VOID);
    return err;}
  else if ( (out) && ( (pipe2(out,PIPE_FLAGS)) < 0) ) {
    lispval err = fd_err("PipeFailed","open_subjob",NULL,FD_VOID);
    if (in) {close(in[0]); close(in[1]);}
    return err;}
  else if ( (err) && ( (pipe2(err,PIPE_FLAGS)) < 0) ) {
    lispval err = fd_err("PipeFailed","open_subjob",NULL,FD_VOID);
    if (in) {close(in[0]); close(in[1]);}
    if (out) {close(out[0]); close(out[1]);}
    return err;}
  else {
    pid_t pid = fork();
    if (pid) {
      struct FD_SUBJOB *subjob = u8_alloc(struct FD_SUBJOB);
      u8_string idstring = FD_CSTRING(id);
      FD_INIT_CONS(subjob,fd_subjob_type);
      if (in) close(in[0]);
      if (out) close(out[1]);
      if (err) close(err[1]);
      subjob->subjob_pid = pid;
      subjob->subjob_id = u8_strdup(FD_CSTRING(id));
      subjob->subjob_in = (in == NULL) ? (fd_incref(infile)) :
        make_port(u8_open_xinput(in[0],NULL),NULL,
                  u8_mkstring("(in)%s",idstring));
      subjob->subjob_out = (out == NULL) ? (fd_incref(outfile)) :
        make_port(NULL,u8_open_xoutput(out[1],NULL),
                  u8_mkstring("(out)%s",idstring));
      subjob->subjob_err = (err == NULL) ? (fd_incref(errfile)) :
        make_port(u8_open_xinput(err[1],NULL),NULL,
                  u8_mkstring("(err)%s",idstring));
      fd_decref(infile); fd_decref(outfile); fd_decref(errfile);
      return (lispval) subjob;}
    else {
      int rv = 0;
      if (in) close(in[1]);
      if (out) close(out[0]);
      if (err) close(err[0]);
      if (in) {
        rv = dup2(in[0],STDIN_FILENO);
        if (rv<0) u8_log(LOGCRIT,"Couldn't redirect stdin for job %q",id);}
      else if (FD_STRINGP(infile)) {
        int new_stdin = u8_fopen(FD_CSTRING(infile),"r");
        if (new_stdin<0) {
          u8_exception ex = u8_pop_exception();
          u8_log(LOGCRIT,"Couldn't open stdout %s",FD_CSTRING(infile));
          rv=new_stdin;}
        else {
          rv = dup2(new_stdin,STDIN_FILENO);
          if (rv<0) u8_log(LOGCRIT,"Couldn't redirect stdin for job %q",id);}}
      else NO_ELSE;
      if (rv<0) {}
      else if (out) {
        rv = dup2(out[1],STDOUT_FILENO);
        if (rv<0) u8_log(LOGCRIT,"Couldn't redirect stdout for job %q",id);}
      else if (FD_STRINGP(outfile)) {
        int new_stdout = u8_fopen(FD_CSTRING(infile),"w");
        if (new_stdout<0) {
          u8_exception ex = u8_pop_exception();
          u8_log(LOGCRIT,"Couldn't open stdout %s",FD_CSTRING(outfile));
          rv=new_stdout;}
        else {
          rv = dup2(new_stdout,STDOUT_FILENO);
          if (rv<0) u8_log(LOGCRIT,"Couldn't redirect stdout for job %q",id);}}
      else NO_ELSE;
      if (rv<0) {}
      else if (err) {
        rv = dup2(out[1],STDERR_FILENO);
        if (rv<0) u8_log(LOGCRIT,"Couldn't redirect stderr for job %q",id);}
      else if (FD_STRINGP(errfile)) {
        int new_stderr = u8_fopen(FD_CSTRING(infile),"w");
        if (new_stderr<0) {
          u8_exception ex = u8_pop_exception();
          u8_log(LOGCRIT,"Couldn't open stderr %s",FD_CSTRING(outfile));
          rv=new_stderr;}
        else {
          rv = dup2(new_stderr,STDERR_FILENO);
          if (rv<0) u8_log(LOGCRIT,"Couldn't redirect stderr for job %q",id);}}
      else NO_ELSE;
      if (rv<0) {
        if (in) close(in[0]);
        if (out) close(out[1]);
        if (err) close(err[1]);
        exit(1);}
      int exec_result = exec_helper("fd_subjob",flags,n-1,args+1);
      /* Never reached */
      return FD_VOID;}
  }
}
#endif

static lispval subjob_pid(lispval subjob)
{
  struct FD_SUBJOB *sj = (fd_subjob) subjob;
  return FD_INT(sj->subjob_pid);
}

static lispval subjob_stdin(lispval subjob)
{
  struct FD_SUBJOB *sj = (fd_subjob) subjob;
  return fd_incref(sj->subjob_out);
}

static lispval subjob_stdout(lispval subjob)
{
  struct FD_SUBJOB *sj = (fd_subjob) subjob;
  return fd_incref(sj->subjob_in);
}

static lispval subjob_stderr(lispval subjob)
{
  struct FD_SUBJOB *sj = (fd_subjob) subjob;
  return fd_incref(sj->subjob_err);
}

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

  fd_idefn(procprims_module,fd_make_cprim1("EXIT",exit_prim,0));
  fd_idefn(procprims_module,fd_make_cprim1("EXIT/FAST",fast_exit_prim,0));
  fd_idefn(procprims_module,fd_make_cprimn("EXEC",exec_prim,1));
  fd_idefn(procprims_module,fd_make_cprimn("EXEC/CMD",exec_cmd_prim,1));
  fd_idefn(procprims_module,fd_make_cprimn("FORK",fork_prim,1));
  fd_idefn(procprims_module,fd_make_cprimn("FORK/CMD",fork_cmd_prim,1));
  fd_idefn(procprims_module,fd_make_cprimn("FDEXEC",fdexec_prim,1));
  fd_idefn(procprims_module,fd_make_cprimn("FDFORK",fdfork_prim,1));
#if HAVE_WAITPID
  fd_idefn(procprims_module,fd_make_cprimn("FORK/WAIT",fork_wait_prim,1));
  fd_idefn(procprims_module,fd_make_cprimn("FORK/CMD/WAIT",fork_cmd_wait_prim,1));
  fd_idefn(procprims_module,fd_make_cprimn("FDFORK/WAIT",fdfork_wait_prim,1));
#endif

  fd_idefn(procprims_module,fd_make_cprim1("PID?",ispid_prim,1));
  fd_idefn(procprims_module,fd_make_cprim2("PID/KILL!",pid_kill_prim,2));
  fd_defalias(procprims_module,"PID/KILL","PID/KILL!");

  fd_idefn1(procprims_module,"SUBJOB/PID",subjob_pid,1,
            "Gets the PID for the subjob",
            fd_subjob_type,FD_VOID);
  fd_idefn2(procprims_module,"SUBJOB/SIGNAL",subjob_signal,2,
            "Sends a signal to a subjob",
            fd_subjob_type,FD_VOID,fd_fixnum_type,FD_VOID);

  fd_idefn1(procprims_module,"SUBJOB/STDIN",subjob_stdin,1,
            "Gets the STDIN for the subjob, either a file or an output stream",
            fd_subjob_type,FD_VOID);
  fd_idefn1(procprims_module,"SUBJOB/STDOUT",subjob_stdout,1,
            "Gets the STDOUT for the subjob, either a file or an input stream",
            fd_subjob_type,FD_VOID);
  fd_idefn1(procprims_module,"SUBJOB/STDERR",subjob_stderr,1,
            "Gets the STDOUT for the subjob, either a file or an input stream",
            fd_subjob_type,FD_VOID);

  fd_finish_module(procprims_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
