/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2010 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

static struct timeval start;
static int started=0;

double get_elapsed()
{
  struct timeval now;
  if (started == 0) {
    gettimeofday(&start,NULL);
    started=1;
    return 0;}
  else {
    gettimeofday(&now,NULL);
    return (now.tv_sec-start.tv_sec)+
      (now.tv_usec-start.tv_usec)*0.000001;}
}

static fdtype read_choice(char *file)
{
  fdtype results=FD_EMPTY_CHOICE;
  FILE *f=fopen(file,"r"); char buf[8192];
  while (fgets(buf,8192,f)) {
    fdtype item=fd_parse(buf);
    if (FD_ABORTP(item)) {
      fd_decref(results);
      return item;}
    FD_ADD_TO_CHOICE(results,item);}
  fclose(f);
  return fd_simplify_choice(results);
}

static int write_dtype_to_file(fdtype x,char *file)
{
  FILE *f=fopen(file,"wb"); int retval;
  struct FD_BYTE_OUTPUT out;
  FD_INIT_BYTE_OUTPUT(&out,1024);
  fd_write_dtype(&out,x);
  retval=fwrite(out.start,1,out.ptr-out.start,f);
  u8_free(out.start);
  fclose(f);
}

#define free_var(var) fd_decref(var); var=FD_VOID

int main(int argc,char **argv)
{
  FILE *f;
  int i=2, j=0, write_binary=0;
  fdtype *args=u8_alloc_n(argc-2,fdtype), common;
  double starttime, inputtime, donetime;
  FD_DO_LIBINIT(fd_init_dtypelib);
  starttime=get_elapsed();
  while (i < argc) 
    if (strchr(argv[i],'=')) 
      fd_config_assignment(argv[i++]);
    else {
    fdtype item=read_choice(argv[i]);
    if (FD_ABORTP(item)) {
      if (!(FD_THROWP(item)))
	u8_fprintf(stderr,"Trouble reading %s: %q\n",argv[i],item);
      return -1;}
    u8_fprintf(stderr,"Read %d items from %s\n",FD_CHOICE_SIZE(item),argv[i]);
    args[j++]=item; i++;}
  inputtime=get_elapsed();
  common=fd_intersection(args,j);
  donetime=get_elapsed();
  u8_fprintf(stderr,"CHOICE_SIZE(common)=%d read time=%f; run time=%f\n",
	     FD_CHOICE_SIZE(common),
	     inputtime-starttime,donetime-inputtime);
  write_dtype_to_file(common,"intersection.dtype");
  if ((argv[1][0]=='-') && (argv[1][1]=='b')) write_binary=1;
  else f=fopen(argv[1],"w");
  if (write_binary)
    write_dtype_to_file(common,argv[1]+2);
  else {
    FD_DO_CHOICES(v,common) {
      struct U8_OUTPUT os;
      U8_INIT_OUTPUT(&os,256);
      fd_unparse(&os,v);
      fputs(os.u8_outbuf,f); fputs("\n",f);
      free(os.u8_outbuf);}}
  free_var(common);
  i=0; while (i < j) {fd_decref(args[i]); i++;}
  u8_free(args);
  if (f) fclose(f);
  exit(0);
}
