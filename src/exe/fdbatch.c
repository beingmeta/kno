/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2006-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FDEXEC_INCLUDED 1

#define main do_main
#include "fdexec.c"
#undef main

#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#if HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif
#if HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

#define LOGMODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)

static u8_string get_pidfile()
{
  fdtype as_configured=fd_config_get("PIDFILE");
  if (FD_STRINGP(as_configured))
    return u8_strdup(FD_STRDATA(as_configured));
  else return fd_runbase_filename(".pid");
}

static u8_string get_cmdfile()
{
  fdtype as_configured=fd_config_get("CMDFILE");
  if (FD_STRINGP(as_configured))
    return u8_strdup(FD_STRDATA(as_configured));
  else return fd_runbase_filename(".cmd");
}

static u8_string get_logfile()
{
  fdtype as_configured=fd_config_get("LOGFILE");
  if (FD_STRINGP(as_configured))
    return u8_strdup(FD_STRDATA(as_configured));
  else return fd_runbase_filename(".log");
}

static u8_string get_errfile()
{
  fdtype as_configured=fd_config_get("ERRFILE");
  if (FD_STRINGP(as_configured))
    return u8_strdup(FD_STRDATA(as_configured));
  else return fd_runbase_filename(".err");
}

static u8_string get_donefile()
{
  fdtype as_configured=fd_config_get("DONEFILE");
  if (FD_STRINGP(as_configured))
    return u8_strdup(FD_STRDATA(as_configured));
  else return fd_runbase_filename(".done");
}

static u8_string get_diedfile()
{
  fdtype as_configured=fd_config_get("DIEDFILE");
  if (FD_STRINGP(as_configured))
    return u8_strdup(FD_STRDATA(as_configured));
  else return fd_runbase_filename(".died");
}

/* End stuff */

static u8_string pid_file=NULL, died_file=NULL, cmd_file=NULL;

static void fdbatch_atexit()
{
  if ((pid_file) && (u8_file_existsp(pid_file)))
    u8_removefile(pid_file);
  if ((cmd_file) && (u8_file_existsp(cmd_file)))
    u8_removefile(cmd_file);
}

#if 0
static void signal_shutdown(int sig)
{
  if ((pid_file) && (u8_file_existsp(pid_file)))
    u8_removefile(pid_file);
  if (died_file) {
    FILE *f=u8_fopen(died_file,"w");
    if (f) {
      /* Output the current data/time with millisecond precision. */
      u8_fprintf(f,"Process died unexpectedly at %*iMSt with signal %d\n",
                 sig);
      u8_fclose(f);}}

}
#endif

static u8_condition job_stopped="Job stopped";
static u8_condition job_exited="Job exited";
static u8_condition job_terminated="Job terminated";

static void wait_for_the_end(pid_t pid)
{
  int status=0;
  waitpid(pid,&status,0);
  while (WIFSTOPPED(status)) {
    u8_log(LOG_CRIT,job_stopped,"%s <%d> has been stopped with the signal %d",
           u8_appid(),pid,WSTOPSIG(status));
    waitpid(pid,&status,0);}
  if (WIFEXITED(status))
    u8_log(LOG_CRIT,job_exited,"%s <%d> exited with return value %d",
           u8_appid(),pid,WSTOPSIG(status));
  else {
    u8_log(LOG_CRIT,job_terminated,"%s <%d> killed with return value %d",
           u8_appid(),pid,WSTOPSIG(status));
    if ((pid_file) && (u8_file_existsp(pid_file)))
      u8_removefile(pid_file);
    if (died_file) {
      FILE *f=u8_fopen(died_file,"w");
      if (f) {
        u8_fprintf(f,"Process %s <%d> killed at %*iMSt with signal %d\n",
                   u8_appid(),pid,WTERMSIG(status));
        u8_fclose(f);}}}
  exit(0);
}

/* Writing the cmd file */

#define need_escape(s) \
  ((strchr(s,'"'))||(strchr(s,'\\'))|| \
   (strchr(s,' '))||(strchr(s,'\t'))|| \
   (strchr(s,'\n'))||(strchr(s,'\r')))

static void write_cmd_file(int argc,char **argv)
{
  const char *abspath=u8_abspath(cmd_file,NULL);
  int i=0, fd=open(abspath,O_CREAT|O_RDWR|O_TRUNC,
                   S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
  u8_byte buf[512]; struct U8_OUTPUT out;
  U8_INIT_OUTPUT_BUF(&out,512,buf);
  while (i<argc) {
    char *arg=argv[i];
    u8_string argstring=u8_fromlibc(arg);
    if (i>0) u8_putc(&out,' '); i++;
    if (need_escape(argstring)) {
      u8_string scan=argstring; 
      int c=u8_sgetc(&scan); u8_putc(&out,'"');
      while (c>=0) {
        if (c=='\\') {
          u8_putc(&out,'\\'); c=u8_sgetc(&scan);}
        else if ((c==' ')||(c=='\n')||(c=='\t')||(c=='\r')||(c=='"')) {
          u8_putc(&out,'\\');}
        if (c>=0) u8_putc(&out,c);
        c=u8_sgetc(&scan);}
      u8_putc(&out,'"');}
    else u8_puts(&out,argstring);
    if (argstring!=((u8_string)arg)) u8_free(argstring);}
  u8_log(LOG_INFO,"ServletInvocation","%s",out.u8_outbuf);
  if (fd>=0) write(fd,out.u8_outbuf,out.u8_outptr-out.u8_outbuf);
  u8_free(abspath); u8_close_output(&out); close(fd);
}

/* The main event */

static int newlog=0;

int main(int argc,char **argv)
{
  pid_t pid;
  int pid_fd, log_fd=-1, err_fd=-1, chained=0;
  int logopen_flags=O_WRONLY|O_APPEND|O_CREAT;
  u8_string done_file, log_file=NULL, err_file=NULL;
  /* We just initialize this for now. */
  u8_log_show_procinfo=1;
  fd_init_dtypelib();
  fd_register_config("NEWLOG",
                     _("Whether to append to log files"),
                     fd_boolconfig_get,fd_boolconfig_set,
                     &newlog);
  fd_argv_config(argc,argv);
  identify_application(argc,argv,argv[0]);
  if (newlog) logopen_flags=O_WRONLY|O_CREAT|O_TRUNC;
  pid_file=get_pidfile();
  if (u8_file_existsp(pid_file)) {
    FILE *f=u8_fopen(pid_file,"r");
    int ival=-1, retval; pid_t pid=getpid();
    if (f==NULL) retval=-1;
    else retval=fscanf(f,"%d",&ival);
    fclose(f);
    if (retval<0) {
      u8_log(LOG_CRIT,"Launch error","Error reading PID file %s (retval=%d)",
             pid_file,retval);
      exit(1);}
    else if (((pid_t)ival)==pid) {
      u8_log(LOG_NOTICE,"CHAINED",
             "Chained fdbatch invocation, pid=%d",ival);
      chained=1;}
    else {
      u8_log(LOG_CRIT,"Launch scrubbed",
             "PID file %s exists, with pid %d != %d",
             pid_file,ival,pid);
      exit(1);}}

  atexit(exit_fdexec);

  cmd_file=get_cmdfile();
  done_file=get_donefile();
  died_file=get_diedfile();
  /* We only redirect stdio going to ttys. */
  if ((pid_fd=u8_open_fd(pid_file,O_WRONLY|O_CREAT,LOGMODE))<0) {
    u8_log(LOG_CRIT,fd_CantOpenFile,"Couldn't open pid file %s",pid_file);
    exit(-1);}
  /* Remove any pre-existing state files. */
  if (u8_file_existsp(cmd_file)) u8_removefile(cmd_file);
  if (u8_file_existsp(done_file)) u8_removefile(done_file);
  if (u8_file_existsp(died_file)) u8_removefile(died_file);
  /* If either stdout or stderr are interactive, redirect them to files. */
  if (isatty(1)) {
    log_file=get_logfile();
    if ((log_fd=u8_open_fd(log_file,logopen_flags,LOGMODE))<0) {
      u8_log(LOG_CRIT,fd_CantOpenFile,"Couldn't open log file %s",log_file);
      close(pid_fd);
      exit(-1);}}
  if (isatty(2)) {
    err_file=get_errfile();
    if ((err_fd=u8_open_fd(err_file,logopen_flags,LOGMODE))<0) {
      u8_log(LOG_CRIT,fd_CantOpenFile,"Couldn't open err file %s",err_file);
      close(pid_fd);
      if ((log_file)&&(log_fd>=0)) close(log_fd);
      exit(-1);}}
  write_cmd_file(argc,argv);
  /* Now, do the fork. */
  if ((chained==0) && (fork())) exit(0);
  else if ((chained==0) && (pid=fork())) {
    char buf[256]; int retval;
    sprintf(buf,"%d",pid);
    retval=write(pid_fd,buf,strlen(buf));
    if (retval<=0) {
      u8_log(LOG_ERROR,"Aborted","Can't write pid %d to %s",pid_file);
      exit(1);}
    close(pid_fd);
    /* The parent process just reports what it did. */
    u8_log(LOG_NOTICE,"Fork started","Launched process pid=%d",pid);
    if (log_file)
      u8_log(LOG_NOTICE,"Fork started","Process %d stdout >> %s",pid,log_file);
    if (err_file)
      u8_log(LOG_NOTICE,"Fork started","Process %d stderr >> %s",pid,err_file);
    wait_for_the_end(pid);
    if (log_file) close(log_fd);
    if (err_file) close(err_fd);}
  else {
    /* The child process redirects stdio, runs fdexec, and
       removes the pid file and writes the done file when
       when it exits normally. */
    int retval=-1;
    if (log_file) {
      dup2(log_fd,1); u8_free(log_file); close(log_fd);}
    if (err_file) {
      dup2(err_fd,2); u8_free(err_file); close(err_fd);}
    atexit(fdbatch_atexit);
    retval=do_main(argc,argv);
    if (retval>=0) {
      FILE *f=u8_fopen(done_file,"w");
      if (f) {
        /* Output the current data/time with millisecond precision. */
        u8_fprintf(f,"Finished %s at %*iMSt, retval=%d",u8_appid(),retval);
        u8_fclose(f);
        if (died_file) {
          if (u8_file_existsp(died_file))
            u8_removefile(died_file);
          u8_free(died_file);
          died_file=NULL;}}}
    else {
      FILE *f=u8_fopen(died_file,"w");
      if (f) {
        /* Output the current data/time with millisecond precision. */
        u8_fprintf(f,"%s died at %*iMSt, retval=%d",u8_appid(),retval);
        u8_fclose(f);}
      died_file=NULL;}
    exit(retval);
    return retval;}
  exit(0);
  return 0;
}
