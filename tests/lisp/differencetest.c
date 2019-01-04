/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <stdio.h>
#include <sys/time.h>
#include <time.h>

static struct timeval start;
static int started = 0;

double get_elapsed()
{
  struct timeval now;
  if (started == 0) {
    gettimeofday(&start,NULL);
    started = 1;
    return 0;}
  else {
    gettimeofday(&now,NULL);
    return (now.tv_sec-start.tv_sec)+
      (now.tv_usec-start.tv_usec)*0.000001;}
}

static lispval read_choice(char *file)
{
  lispval results = FD_EMPTY_CHOICE;
  FILE *f = fopen(file,"r"); char buf[8192]; int i = 0;
  while (fgets(buf,8192,f)) {
    lispval item = fd_parse(buf);
    if (FD_ABORTP(item)) {
      if (!(FD_THROWP(item)))
        u8_fprintf(stderr,"Error at %s[%d]\n",file,i);
      fd_decref(results); return item;}
    FD_ADD_TO_CHOICE(results,item); i++;}
  fclose(f);
  return fd_simplify_choice(results);
}

static int write_dtype_to_file(lispval x,char *file)
{
  FILE *f = fopen(file,"wb"); int retval;
  struct FD_OUTBUF out = { 0 };
  FD_INIT_BYTE_OUTPUT(&out,1024);
  fd_write_dtype(&out,x);
  retval = fwrite(out.buffer,out.bufwrite-out.buffer,1,f);
  fd_close_outbuf(&out);
  fclose(f);
  return retval;
}

int main(int argc,char **argv)
{
  FILE *f; int i = 1, k = 0; double starttime, inputtime, donetime;
  char *output_arg = NULL, *input_arg = NULL, *remove_arg = NULL;
  FD_DO_LIBINIT(fd_init_lisp_types);
  starttime = get_elapsed();
  while (i<argc)
    if (strchr(argv[i],'='))
      fd_config_assignment(argv[i++]);
    else {
      if (k==0) {
        output_arg = argv[i++]; k++;}
      else if (k==1) {
        input_arg = argv[i++]; k++;}
      else if (k==2) {
        remove_arg = argv[i++]; k++;}
      else i++;}
  {
    lispval input = read_choice(input_arg);
    lispval to_remove = read_choice(remove_arg);
    lispval difference;
    lispval sdifference;
    if (FD_ABORTP(input)) {
      u8_fprintf(stderr,"Trouble reading %s: %q\n",input_arg,input);
      return -1;}
    if (FD_ABORTP(to_remove)) {
        u8_fprintf(stderr,"Trouble reading %s: %q\n",remove_arg,to_remove);
        return -1;}
    inputtime = get_elapsed();
    difference = fd_difference(input,to_remove);
    write_dtype_to_file(difference,"difference.dtype");
    sdifference = fd_make_simple_choice(difference);
    donetime = get_elapsed();
    fprintf(stderr,"CHOICE_SIZE(difference)=%d, read time=%f, run time=%f\n",
            fd_choice_size(sdifference),
            inputtime-starttime,
            donetime-inputtime);
    f = fopen(output_arg,"w");
    {
      FD_DO_CHOICES(v,difference) {
        struct U8_OUTPUT os;
        U8_INIT_STATIC_OUTPUT(os,256);
        fd_unparse(&os,v);
        fputs(os.u8_outbuf,f); fputs("\n",f);
        free(os.u8_outbuf);}}
  fd_decref(input); fd_decref(to_remove);
  fd_decref(difference); fd_decref(sdifference);
  input = FD_VOID; to_remove = FD_VOID;
  difference = FD_VOID; sdifference = FD_VOID;}
  if (f) fclose(f);
  exit(0);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
