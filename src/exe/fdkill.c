/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include <libu8/libu8.h>
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <signal.h>
#include <stdio.h>

#if HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

int waitfor(pid_t pid,int interval)
{
  time_t done=time(NULL)+interval; int status=-1;
  while (time(NULL)<done) {
    pid_t rv=waitpid(pid,&status,WNOHANG);
    if (rv>0) return (rv==pid);
    sleep(1);}
  return 0;
}

int main(int argc,char *argv[])
{
  pid_t pid; int rv;
  long int_pid=atoi(argv[1]), interval=-1;
  if (int_pid<0) {
    if (u8_file_existsp(argv[1])) {
      u8_string data=u8_filestring(argv[1],NULL);
      int_pid=atoi(data);
      u8_free(data);}}
  if (int_pid<0) {
    fprintf(stderr,"Usage: pid/pidfile [wait]\n");
    exit(1);}
  else pid=(pid_t)int_pid;
  if (argc>2) interval=atoi(argv[2]);
  if ((interval<0)&&(getenv("KNOKILL_WAIT")))
    interval=atoi(getenv("KNOKILL_WAIT"));
  else {}
  if (interval<0) interval=7;
  rv=kill(pid,SIGTERM);
  if (rv) {
    perror("SIGTERM kill() failed");
    exit(1);}
  if (waitfor(pid,interval)) {
    fprintf(stderr,"Process %ld exited normally\n",(long int)pid);
    exit(0);}
  fprintf(stderr,"Process %ld refused to quit, using SIGKILL\n",(long int)pid);
  rv=kill(pid,SIGKILL);
  if (rv) {
    perror("SIGKILL kill() failed");
    exit(1);}
  exit(0);
}

