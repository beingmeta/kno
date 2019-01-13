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
static lispval open_subjob(lispval id)
{
  int in_fd[2], out_fd[2], err_fd[2];
  if ( (pipe2(in_fd,PIPE_FLAGS)) < 0) {
    lispval err = open_error();
    close(in_fd[0]); close(in_fd[1]);
    return err;}
  else if ( (pipe2(out_fd,PIPE_FLAGS)) < 0) {
    lispval err = open_error();
    close(in_fd[0]); close(in_fd[1]);
    close(out_fd[0]); close(out_fd[1]);
    return err;}
  else if ( (pipe2(err_fd,PIPE_FLAGS)) < 0) {
    lispval err = open_error();
    close(in_fd[0]); close(in_fd[1]);
    close(out_fd[0]); close(out_fd[1]);
    close(err_fd[0]); close(err_fd[1]);
    return err;}
  else {
    pid_t pid = fork();
    if (pid) {
      struct FD_SUBJOB *subjob = u8_alloc(struct FD_SUBJOB);
      u8_string idstring = "none";
      FD_INIT_CONS(subjob,fd_subjob_type);
      subjob->subjob_pid = pid;
      subjob->subjob_id = u8_strdup(FD_CSTRING(id));
      subjob->subjob_in =
        make_port(u8_open_xinput(in_fd[0],NULL),NULL,
                  u8_mkstring("(in)%s",idstring));
      subjob->subjob_out =
        make_port(NULL,u8_open_xoutput(out_fd[1],NULL),
                  u8_mkstring("(out)%s",idstring));
      subjob->subjob_err =
        make_port(u8_open_xinput(out_fd[1],NULL),NULL,
                  u8_mkstring("(err)%s",idstring));
      return (lispval) ;}
    else {}
  }
}
#endif

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

  fd_finish_module(procprims_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
