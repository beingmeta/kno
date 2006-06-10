/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"

#include <stdio.h>
#include <sys/time.h>
#include <time.h>

static struct timeval start;
static started=0;

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
  FILE *f=fopen(file,"r"); char buf[8192]; int i=0;
  while (fgets(buf,8192,f)) {
    fdtype item=fd_parse(buf);
    if ((FD_TROUBLEP(item)) || (FD_EXCEPTIONP(item))) {
      u8_fprintf(stderr,"Error at %s[%d]\n",file,i);
      fd_decref(results); return item;}
    FD_ADD_TO_CHOICE(results,item); i++;}
  fclose(f);
  return fd_simplify_choice(results);
}

static int write_dtype_to_file(fdtype x,char *file)
{
  FILE *f=fopen(file,"wb");
  struct FD_BYTE_OUTPUT out;
  FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
  fd_write_dtype(&out,x);
  fwrite(out.start,out.ptr-out.start,1,f);
  u8_free(out.start);
  fclose(f);
}

int main(int argc,char **argv)
{
  FILE *f; int i=1, k=0; double starttime, inputtime, donetime;
  char *output_arg, *input_arg, *remove_arg;
  FD_DO_LIBINIT(fd_init_dtypelib);
  starttime=get_elapsed();
  while (i<argc)
    if (strchr(argv[i],'='))
      fd_config_assignment(argv[i++]);
    else {
      if (k==0) {
	output_arg=argv[i++]; k++;}
      else if (k==1) {
	input_arg=argv[i++]; k++;}
      else if (k==2) {
	remove_arg=argv[i++]; k++;}
      else i++;}
  {
    fdtype input=read_choice(input_arg);
    fdtype to_remove=read_choice(remove_arg);
    fdtype difference;
    fdtype sdifference;
    if ((FD_TROUBLEP(input)) || (FD_EXCEPTIONP(input))) {
      u8_fprintf(stderr,"Trouble reading %s: %q\n",input_arg,input);
      return -1;}
    if ((FD_TROUBLEP(to_remove)) || (FD_EXCEPTIONP(to_remove))) {
	u8_fprintf(stderr,"Trouble reading %s: %q\n",remove_arg,to_remove);
	return -1;}
    inputtime=get_elapsed();
    difference=fd_difference(input,to_remove);
    write_dtype_to_file(difference,"difference.dtype");
    sdifference=fd_make_simple_choice(difference);
    donetime=get_elapsed();
    fprintf(stderr,"CHOICE_SIZE(difference)=%d, read time=%f, run time=%f\n",
	    fd_choice_size(sdifference),
	    inputtime-starttime,
	    donetime-inputtime);
    f=fopen(output_arg,"w");
    {
      FD_DO_CHOICES(v,difference) {
	struct U8_OUTPUT os;
	U8_INIT_OUTPUT(&os,256);
	fd_unparse(&os,v);
	fputs(os.bytes,f); fputs("\n",f);
	free(os.bytes);}}
  fd_decref(input); fd_decref(to_remove);
  fd_decref(difference); fd_decref(sdifference);
  input=FD_VOID; to_remove=FD_VOID;
  difference=FD_VOID; sdifference=FD_VOID;}
  if (f) fclose(f);
  exit(0);
}


/* The CVS log for this file
   $Log: differencetest.c,v $
   Revision 1.20  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.19  2006/01/07 04:23:29  haase
   Made choicetests include mergesort testing

   Revision 1.18  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.17  2005/05/30 17:48:08  haase
   Fixed some header ordering problems

   Revision 1.16  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.15  2005/02/19 19:09:15  haase
   Report line number in reporting choice reading errors

   Revision 1.14  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.13  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/


