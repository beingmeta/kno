#define main do_main
#include "fdexec.c"
#undef main

#define LOGMODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)

int main(int argc,char **argv)
{
  pid_t pid;
  int pid_fd, log_fd, err_fd;
  u8_string base=u8_basename(argv[1],".scm"), abspath=u8_abspath(base,NULL);
  u8_string pid_file=u8_string_append(abspath,".pid",NULL);
  u8_string log_file=NULL, err_file=NULL;
  /* We only redirect stdio going to ttys. */
  if ((pid_fd=u8_open_fd(pid_file,O_WRONLY|O_APPEND|O_CREAT,LOGMODE))<0) {
    u8_warn("Couldn't open pid file %s",pid_file);
    exit(-1);}
  if (isatty(1)) {
    log_file=u8_string_append(abspath,".log",NULL);
    if ((log_fd=u8_open_fd(log_file,O_WRONLY|O_APPEND|O_CREAT,LOGMODE))<0) {
      u8_warn("Couldn't open log file %s",log_file);
      close(pid_fd);
    exit(-1);}}
  if (isatty(2)) {
    err_file=u8_string_append(abspath,".err",NULL);
    if ((err_fd=u8_open_fd(err_file,O_WRONLY|O_APPEND|O_CREAT,LOGMODE))<0) {
      u8_warn("Couldn't open err file %s",err_file);
      if (log_file) close(log_fd); close(pid_fd);
      exit(-1);}}
  if (pid=fork()) {
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
    /* The child process redirects stdio, runs fdexec, and removes the pid file
       when it is done. */
    int retval=-1;
    if (log_file) {dup2(log_fd,1); u8_free(log_file);}
    if (err_file) {dup2(err_fd,2); u8_free(err_file);}
    u8_free(base); u8_free(abspath);
    retval=do_main(argc,argv);
    u8_removefile(pid_file);
    return retval;}
}



