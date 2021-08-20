/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/lisp.h"

#include <libu8/libu8.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

static void write_dtype_to_file(lispval object,FILE *f)
{
  struct KNO_OUTBUF out = { 0 };
  KNO_INIT_BYTE_OUTPUT(&out,1024);
  kno_write_dtype(&out,object);
  fwrite(out.buffer,1,out.bufwrite-out.buffer,f);
  kno_close_outbuf(&out);
}

int main(int argc,char **argv)
{
  lispval object;
  FILE *f = fopen(argv[1],"wb");
  KNO_DO_LIBINIT(kno_init_lisp_types);
  object = kno_parse(argv[2]);
  write_dtype_to_file(object,f);
  u8_fprintf(stderr,"dumped %q\n",object);
  kno_decref(object);
  exit(0);
}

