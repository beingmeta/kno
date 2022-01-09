/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#define KNO_DEBUG_PTRCHECK 2

#include "kno/lisp.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <string.h>
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
  retval = fwrite(out.buffer,out.bufwrite-out.buffer,1,f);
  kno_close_outbuf(&out);
  fclose(f);
  return retval;
}

int main(int argc,char **argv)
{
  FILE *f = NULL;
  char *output_file = NULL;
  lispval combined_inputs = KNO_EMPTY_CHOICE, scombined_inputs;
  lispval *inputv;
  int i = 1, write_binary = 0, n_inputs = 0;
  double starttime, inputtime, donetime;
  KNO_DO_LIBINIT(kno_init_lisp_types);
  inputv = u8_alloc_n(argc,lispval);
  starttime = get_elapsed();
  while (i < argc)
    if (strchr(argv[i],'='))
      kno_config_assignment(argv[i++]);
    else if (output_file == NULL)
      output_file = argv[i++];
    else {
      lispval item = read_choice(argv[i]);
      if (KNO_ABORTP(item)) {
        if (!(KNO_THROWP(item)))
          u8_fprintf(stderr,"Trouble reading %s: %q\n",argv[i],item);
        return -1;}
      u8_fprintf(stderr,"Read %d items from %s\n",
                 KNO_CHOICE_SIZE(item),argv[i]);
      inputv[n_inputs++]=item; i++;}
  i = 0; while (i < n_inputs) {
    lispval item = inputv[i++];
    KNO_ADD_TO_CHOICE(combined_inputs,item);}
  inputtime = get_elapsed();
  scombined_inputs = kno_make_simple_choice(combined_inputs);
  donetime = get_elapsed();
  write_dtype_to_file(combined_inputs,"union.dtype");
  fprintf(stderr,
          "CHOICE_SIZE(combined_inputs)=%d; read time=%f; run time=%f\n",
          kno_choice_size(scombined_inputs),
          inputtime-starttime,donetime-inputtime);
  if ((output_file[0]=='-') && (output_file[1]=='b')) write_binary = 1;
  else f = fopen(output_file,"w");
  if (write_binary)
    write_dtype_to_file(combined_inputs,argv[1]+2);
  else {
    KNO_DO_CHOICES(v,combined_inputs) {
      struct U8_OUTPUT os;
      U8_INIT_STATIC_OUTPUT(os,256);
      kno_unparse(&os,v);
      fputs(os.u8_outbuf,f); fputs("\n",f);
      free(os.u8_outbuf);}}
  kno_decref(combined_inputs);
  kno_decref(scombined_inputs);
  if (f) fclose(f);
  exit(0);
}

