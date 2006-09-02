#define main do_main
#include "fdexec.c"
#undef main

#define LOGMODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)

static u8_string get_pidfile(u8_string abspath)
{
  fdtype as_configured=fd_config_get("PIDFILE");
  if (FD_STRINGP(as_configured))
    return u8_strdup(FD_STRDATA(as_configured));
  else return u8_string_append(abspath,".pid",NULL);
}

static u8_string get_logfile(u8_string abspath)
{
  fdtype as_configured=fd_config_get("LOGFILE");
  if (FD_STRINGP(as_configured))
    return u8_strdup(FD_STRDATA(as_configured));
  else {
    u8_string simple=u8_string_append(abspath,".log",NULL);
    if (u8_file_existsp(simple)) {
      u8_free(simple);
      return u8_mkstring("%s_%d.log",abspath,getpid());}
    else return simple;}
}

static u8_string get_errfile(u8_string abspath)
{
  fdtype as_configured=fd_config_get("ERRFILE");
  if (FD_STRINGP(as_configured))
    return u8_strdup(FD_STRDATA(as_configured));
  else {
    u8_string simple=u8_string_append(abspath,".err",NULL);
    if (u8_file_existsp(simple)) {
      u8_free(simple);
      return u8_mkstring("%s_%d.err",abspath,getpid());}
    else return simple;}
}

static u8_string get_donefile(u8_string abspath)
{
  fdtype as_configured=fd_config_get("DONEFILE");
  if (FD_STRINGP(as_configured))
    return u8_strdup(FD_STRDATA(as_configured));
  else {
    u8_string simple=u8_string_append(abspath,".done",NULL);
    if (u8_file_existsp(simple)) {
      u8_free(simple);
      return u8_mkstring("%s_%d.done",abspath,getpid());}
    else return simple;}
}

static u8_string get_diedfile(u8_string abspath)
{
  fdtype as_configured=fd_config_get("DIEDFILE");
  if (FD_STRINGP(as_configured))
    return u8_strdup(FD_STRDATA(as_configured));
  else {
    u8_string simple=u8_string_append(abspath,".died",NULL);
    if (u8_file_existsp(simple)) {
      u8_free(simple);
      return u8_mkstring("%s_%d.died",abspath,getpid());}
    else return simple;}
}

static u8_string pid_file=NULL, died_file=NULL;

static void fdbatch_atexit()
{
  if (pid_file) u8_removefile(pid_file);
  if (died_file) {
    FILE *f=u8_fopen(died_file,"w");
    if (f) {
      /* Output the current data/time with millisecond precision. */
      u8_fprintf(f,"Process died unexpectedly at %*iMSt\n");
      u8_fclose(f);}}
}

static void signal_shutdown(int sig)
{
  if (pid_file) u8_removefile(pid_file);
  if (died_file) {
    FILE *f=u8_fopen(died_file,"w");
    if (f) {
      /* Output the current data/time with millisecond precision. */
      u8_fprintf(f,"Process died unexpectedly at %*iMSt with signal %d\n",
		 sig);
      u8_fclose(f);}}
    
}

int main(int argc,char **argv)
{
  pid_t pid;
  int pid_fd, log_fd, err_fd, chained=0;
  u8_string base=u8_basename(argv[1],".scm"), abspath=u8_abspath(base,NULL);
  u8_string done_file, log_file=NULL, err_file=NULL;
  /* We just initialize this for now. */
  fd_init_dtypelib();
  pid_file=get_pidfile(abspath);
  if (u8_file_existsp(pid_file)) {
    FILE *f=u8_fopen(pid_file,"r");
    int ival=-1, retval; pid_t pid=getpid();
    if (f==NULL) retval=-1;
    else retval=fscanf(f,"%d",&ival);
    fclose(f);
    if (retval<0) {
      u8_warn("Launch error","Error reading PID file %s (retval=%d)",
	      pid_file,retval);
      exit(1);}
    else if (((pid_t)ival)==pid) {
      u8_notify("CHAINED",
		"Chained fdbatch invocation, pid=%d",ival);
      chained=1;}
    else {
      u8_warn("Launch scrubbed",
	      "PID file %s exists, with pid %d != %d",
	      pid_file,ival,pid);
	exit(1);}}

  done_file=get_donefile(abspath);
  died_file=get_diedfile(abspath);
  /* We only redirect stdio going to ttys. */
  if ((pid_fd=u8_open_fd(pid_file,O_WRONLY|O_CREAT,LOGMODE))<0) {
    u8_warn("Couldn't open pid file %s",pid_file);
    exit(-1);}
  /* Remove any pre-existing done file. */
  if (u8_file_existsp(done_file)) u8_removefile(done_file);
  if (u8_file_existsp(died_file)) u8_removefile(died_file);
  /* If either stdout or stderr are interactive, redirect them to files. */
  if (isatty(1)) {
    log_file=get_logfile(abspath);
    if ((log_fd=u8_open_fd(log_file,O_WRONLY|O_APPEND|O_CREAT,LOGMODE))<0) {
      u8_warn("Couldn't open log file %s",log_file);
      close(pid_fd);
    exit(-1);}}
  if (isatty(2)) {
    err_file=get_errfile(abspath);
    if ((err_fd=u8_open_fd(err_file,O_WRONLY|O_APPEND|O_CREAT,LOGMODE))<0) {
      u8_warn("Couldn't open err file %s",err_file);
      if (log_file) close(log_fd); close(pid_fd);
      exit(-1);}}
  /* Now, do the fork. */
  if ((chained==0) && (pid=fork())) {
    char buf[256];
    sprintf(buf,"%d",pid); write(pid_fd,buf,strlen(buf)); close(pid_fd);
    /* The parent process just reports what it did. */
    u8_notify("Fork started","Launched process pid=%d",pid);
    if (log_file)
      u8_notify("Fork started","Process %d stdout >> %s",pid,log_file);
    if (err_file)
      u8_notify("Fork started","Process %d stderr >> %s",pid,err_file);
    exit(0);}
  else {
    /* The child process redirects stdio, runs fdexec, and
       removes the pid file and writes the done file when
       when it exits normally. */
    int retval=-1;
    atexit(fdbatch_atexit);
#ifdef SIGTERM
    signal(SIGTERM,signal_shutdown);
#endif
#ifdef SIGQUIT
    signal(SIGQUIT,signal_shutdown);
#endif
    if (log_file) {dup2(log_fd,1); u8_free(log_file);}
    if (err_file) {dup2(err_fd,2); u8_free(err_file);}
    u8_free(base); u8_free(abspath);
    retval=do_main(argc,argv);
    if (retval>=0) {
      FILE *f=u8_fopen(done_file,"w");
      if (f) {
	/* Output the current data/time with millisecond precision. */
	u8_fprintf(f,"Finished %s at %*iMSt, retval=%d",base,retval);
	u8_fclose(f);
	if (died_file) {
	  u8_removefile(died_file);
	  u8_free(died_file);
	  died_file=NULL;}}}
    else {
      FILE *f=u8_fopen(died_file,"w");
      if (f) {
	/* Output the current data/time with millisecond precision. */
	u8_fprintf(f,"%s died at %*iMSt, retval=%d",base,retval);
	u8_fclose(f);}
      died_file=NULL;}
    return retval;}
}

