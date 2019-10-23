/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2006-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define EMBEDDED_KNO 1

#include "knox.c"

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
  lispval as_configured = kno_config_get("PIDFILE");
  if (STRINGP(as_configured))
    return u8_strdup(CSTRING(as_configured));
  else return kno_runbase_filename(".pid");
}

static u8_string get_cmdfile()
{
  lispval as_configured = kno_config_get("CMDFILE");
  if (STRINGP(as_configured))
    return u8_strdup(CSTRING(as_configured));
  else return kno_runbase_filename(".cmd");
}

static u8_string get_logfile()
{
  lispval as_configured = kno_config_get("LOGFILE");
  if (STRINGP(as_configured))
    return u8_strdup(CSTRING(as_configured));
  else return kno_runbase_filename(".log");
}

static u8_string get_errfile()
{
  lispval as_configured = kno_config_get("ERRFILE");
  if (STRINGP(as_configured))
    return u8_strdup(CSTRING(as_configured));
  else return kno_runbase_filename(".err");
}

static u8_string get_donefile()
{
  lispval as_configured = kno_config_get("DONEFILE");
  if (STRINGP(as_configured))
    return u8_strdup(CSTRING(as_configured));
  else return kno_runbase_filename(".done");
}

static u8_string get_diedfile()
{
  lispval as_configured = kno_config_get("DIEDFILE");
  if (STRINGP(as_configured))
    return u8_strdup(CSTRING(as_configured));
  else return kno_runbase_filename(".died");
}

static u8_string get_stopfile()
{
  lispval as_configured = kno_config_get("STOPFILE");
  if (STRINGP(as_configured))
    return u8_strdup(CSTRING(as_configured));
  else return kno_runbase_filename(".stop");
}

/* End stuff */

static u8_string pid_file = NULL, died_file = NULL, cmd_file = NULL;

static void knotask_atexit()
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
    FILE *f = u8_fopen(died_file,"w");
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
  int status = 0;
  waitpid(pid,&status,0);
  while (WIFSTOPPED(status)) {
    u8_log(LOG_CRIT,job_stopped,"%s <%d> has been stopped with the signal %d",
           u8_appid(),pid,WSTOPSIG(status));
    waitpid(pid,&status,0);}
  if (WIFEXITED(status))
    u8_log(LOG_NOTICE,job_exited,"%s <%d> exited with return value %d",
           u8_appid(),pid,WSTOPSIG(status));
  else {
    u8_log(LOG_CRIT,job_terminated,"%s <%d> killed with return value %d",
           u8_appid(),pid,WSTOPSIG(status));
    if ((pid_file) && (u8_file_existsp(pid_file)))
      u8_removefile(pid_file);
    if (died_file) {
      FILE *f = u8_fopen(died_file,"w");
      if (f) {
        u8_fprintf(f,"Process %s <%d> killed at %*iMSt with signal %d\n",
                   u8_appid(),pid,WTERMSIG(status));
        u8_fclose(f);}}}
  exit(0);
}

/* The main event */

static int keeplog = 0;

int main(int argc,char **argv)
{
  pid_t pid;
  int pid_fd, log_fd = -1, err_fd = -1, chained = 0;
  int logopen_flags = O_WRONLY|O_APPEND|O_CREAT;
  u8_string source_file = NULL, exe_name = NULL;
  u8_string done_file, log_file = NULL, err_file = NULL;
  lispval *args = NULL; size_t n_args;

  u8_log_show_date=1;
  u8_log_show_procinfo=1;
  u8_log_show_elapsed=1;

  kno_main_errno_ptr = &errno;
  KNO_INIT_STACK();

  /* We just initialize this for now. */
  u8_log_show_procinfo = 1;

  args = handle_argv(argc,argv,&n_args,&exe_name,&source_file,"_");

  KNO_NEW_STACK(((struct KNO_STACK *)NULL),"knotask",NULL,VOID);
  _stack->stack_label=u8_strdup(u8_appid());
  U8_SETBITS(_stack->stack_flags,KNO_STACK_FREE_LABEL);

  kno_register_config("LOGAPPEND",
                     _("Whether to extend existing log files or truncate them"),
                     kno_boolconfig_get,kno_boolconfig_set,
                     &keeplog);
  kno_register_config("KEEPLOG",
                     _("Whether to extend existing log files or truncate them"),
                     kno_boolconfig_get,kno_boolconfig_set,
                     &keeplog);

  if (!(keeplog)) logopen_flags = O_WRONLY|O_CREAT|O_TRUNC;

  pid_file = get_pidfile();

  if (u8_file_existsp(pid_file)) {
    FILE *f = u8_fopen(pid_file,"r");
    int ival = -1, retval; pid_t pid = getpid();
    if (f == NULL) retval = -1;
    else retval = fscanf(f,"%d",&ival);
    fclose(f);
    if (retval<0) {
      u8_log(LOG_CRIT,"Launch error","Error reading PID file %s (retval=%d)",
             pid_file,retval);
      exit(1);}
    else if (((pid_t)ival) == pid) {
      u8_log(LOG_NOTICE,"CHAINED",
             "Chained knotask invocation, pid=%d",ival);
      chained = 1;}
    else {
      u8_log(LOG_CRIT,"Launch scrubbed",
             "PID file %s exists, with pid %d != %d",
             pid_file,ival,pid);
      kno_pop_stack(_stack);
      exit(1);}}

  atexit(exit_kno);

  stop_file = get_stopfile();

  cmd_file = get_cmdfile();
  done_file = get_donefile();
  died_file = get_diedfile();

  /* We only redirect stdio going to ttys. */
  if ((pid_fd = u8_open_fd(pid_file,O_WRONLY|O_CREAT,LOGMODE))<0) {
    u8_log(LOG_CRIT,u8_CantOpenFile,"Couldn't open pid file %s",pid_file);
    kno_pop_stack(_stack);
    exit(-1);}

  /* Remove any pre-existing state files. */
  if (u8_file_existsp(cmd_file)) u8_removefile(cmd_file);
  if (u8_file_existsp(done_file)) u8_removefile(done_file);
  if (u8_file_existsp(died_file)) u8_removefile(died_file);

  /* If either stdout or stderr are interactive, redirect them to files. */
  if (isatty(1)) {
    log_file = get_logfile();
    if ((log_fd = u8_open_fd(log_file,logopen_flags,LOGMODE))<0) {
      u8_log(LOG_CRIT,u8_CantOpenFile,"Couldn't open log file %s",log_file);
      close(pid_fd);
      kno_pop_stack(_stack);
      exit(-1);}}
  if (isatty(2)) {
    err_file = get_errfile();
    if ((err_fd = u8_open_fd(err_file,logopen_flags,LOGMODE))<0) {
      u8_log(LOG_CRIT,u8_CantOpenFile,"Couldn't open err file %s",err_file);
      close(pid_fd);
      if ((log_file)&&(log_fd>=0)) close(log_fd);
      kno_pop_stack(_stack);
      exit(-1);}}

  write_cmd_file(cmd_file,"BatchInvocation",argc,argv);

  /* Now, do the fork. */
  if ((chained==0) && (fork())) {
    kno_pop_stack(_stack);
    exit(0);}
  else if ((chained==0) && (pid = fork())) {
    char buf[256]; int retval;
    sprintf(buf,"%d",pid);
    retval = write(pid_fd,buf,strlen(buf));
    if (retval<=0) {
      u8_log(LOG_ERROR,"Aborted","Can't write pid %d to %s",pid_file);
      kno_pop_stack(_stack);
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
    /* The child process redirects stdio, runs kno, and
       removes the pid file and writes the done file when
       when it exits normally. */
    int retval = -1;
    if (log_file) {
      dup2(log_fd,1); u8_free(log_file); close(log_fd);}
    if (err_file) {
      dup2(err_fd,2); u8_free(err_file); close(err_fd);}
    atexit(knotask_atexit);
    retval = do_main(argc,argv,exe_name,source_file,
                   args,n_args);
    if (retval>=0) {
      FILE *f = u8_fopen(done_file,"w");
      if (f) {
        /* Output the current data/time with millisecond precision. */
        u8_fprintf(f,"Finished %s at %*iMSt, retval=%d",u8_appid(),retval);
        u8_fclose(f);
        if (died_file) {
          if (u8_file_existsp(died_file))
            u8_removefile(died_file);
          u8_free(died_file);
          died_file = NULL;}}}
    else {
      FILE *f = u8_fopen(died_file,"w");
      if (f) {
        /* Output the current data/time with millisecond precision. */
        u8_fprintf(f,"%s died at %*iMSt, retval=%d",u8_appid(),retval);
        u8_fclose(f);}
      died_file = NULL;}
    {
      int free_i = 0; while (free_i<n_args) {
        kno_decref(args[free_i]); free_i++;}
      u8_free(args);}
    kno_pop_stack(_stack);
    exit(retval);
    return retval;}
  {
    int free_i = 0; while (free_i<n_args) {
      kno_decref(args[free_i]); free_i++;}
    u8_free(args);}
  kno_pop_stack(_stack);
  kno_doexit(KNO_FALSE);
  exit(0);
  return 0;
}

