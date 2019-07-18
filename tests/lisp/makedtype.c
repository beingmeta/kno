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

static int write_dtype_to_file(lispval object,FILE *f)
{
  struct KNO_OUTBUF out = { 0 };
  int n;
  KNO_INIT_BYTE_OUTPUT(&out,1024);
  kno_write_dtype(&out,object);
  n = fwrite(out.buffer,1,out.bufwrite-out.buffer,f);
  kno_close_outbuf(&out);
  return n;
}

char *read_text_file(char *filename)
{
  FILE *f = fopen(filename,"r");
  char *buf = u8_malloc(1024), *ptr = buf, *buflim = buf+1024;
  int c = getc(f);
  while (c>=0) {
    if (ptr>=buflim) {
      unsigned int off = ptr-buf, old_size = (buflim-buf);
      buf = u8_realloc(buf,old_size*2);
      ptr = buf+off; buflim = buf+old_size*2;}
    *ptr++=c; c = getc(f);}
  if (ptr>=buflim) {
    unsigned int off = ptr-buf, old_size = (buflim-buf);
    buf = u8_realloc(buf,old_size*2);
    ptr = buf+off; buflim = buf+old_size*2;}
  *ptr='\0';
  fclose(f);
  return buf;
}

int main(int argc,char **argv)
{
  lispval object;
  FILE *f = fopen(argv[1],"wb");
  KNO_DO_LIBINIT(kno_init_lisp_types);
  if ((argv[2][0]=='-') && (argv[2][1]=='f')) {
    unsigned char *buf = read_text_file(argv[2]+2);
    object = kno_parse(buf);
    u8_free(buf);}
  else object = kno_parse(argv[2]);
  write_dtype_to_file(object,f);
  u8_fprintf(stdout,"Dumped the %s %q\n",
             kno_type_names[KNO_LISP_TYPE(object)],object);
  kno_decref(object); object = KNO_VOID;
  fclose(f);
  exit(0);
}

