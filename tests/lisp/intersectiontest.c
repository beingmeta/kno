/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/lisp.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <strings.h>
#include <stdlib.h>
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
  lispval results = KNO_EMPTY_CHOICE;
  FILE *f = fopen(file,"r"); char buf[8192];
  while (fgets(buf,8192,f)) {
    lispval item = kno_parse(buf);
    if (KNO_ABORTP(item)) {
      kno_decref(results);
      return item;}
    KNO_ADD_TO_CHOICE(results,item);}
  fclose(f);
  return kno_simplify_choice(results);
}

static int write_dtype_to_file(lispval x,char *file)
{
  FILE *f = fopen(file,"wb"); int retval;
  struct KNO_OUTBUF out = { 0 };
  KNO_INIT_BYTE_OUTPUT(&out,1024);
  kno_write_dtype(&out,x);
  retval = fwrite(out.buffer,1,out.bufwrite-out.buffer,f);
  kno_close_outbuf(&out);
  fclose(f);
  return retval;
}

#define free_var(var) kno_decref(var); var = KNO_VOID

int main(int argc,char **argv)
{
  FILE *f = NULL;
  int i = 2, j = 0, write_binary = 0;
  lispval *args = u8_alloc_n(argc-2,lispval), common;
  double starttime, inputtime, donetime;
  KNO_DO_LIBINIT(kno_init_lisp_types);
  starttime = get_elapsed();
  while (i < argc)
    if (strchr(argv[i],'='))
      kno_config_assignment(argv[i++]);
    else {
      lispval item = read_choice(argv[i]);
      if (KNO_ABORTP(item)) {
        if (!(KNO_THROWP(item)))
          u8_fprintf(stderr,"Trouble reading %s: %q\n",argv[i],item);
        return -1;}
      u8_fprintf(stderr,"Read %d items from %s\n",KNO_CHOICE_SIZE(item),argv[i]);
      args[j++]=item; i++;}
  inputtime = get_elapsed();
  common = kno_intersection(args,j);
  donetime = get_elapsed();
  u8_fprintf(stderr,"CHOICE_SIZE(common)=%d read time=%f; run time=%f\n",
             KNO_CHOICE_SIZE(common),
             inputtime-starttime,donetime-inputtime);
  write_dtype_to_file(common,"intersection.dtype");
  if ((argv[1][0]=='-') && (argv[1][1]=='b')) write_binary = 1;
  else f = fopen(argv[1],"w");
  if (write_binary)
    write_dtype_to_file(common,argv[1]+2);
  else {
    KNO_DO_CHOICES(v,common) {
      struct U8_OUTPUT os;
      U8_INIT_STATIC_OUTPUT(os,256);
      kno_unparse(&os,v);
      fputs(os.u8_outbuf,f); fputs("\n",f);
      free(os.u8_outbuf);}}
  free_var(common);
  i = 0; while (i < j) {kno_decref(args[i]); i++;}
  u8_free(args);
  if (f) fclose(f);
  exit(0);
}

