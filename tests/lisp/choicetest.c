/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/lisp.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc,char **argv)
{
  struct KNO_OUTBUF out = { 0 };
  FILE *f = fopen(argv[1],"wb");
  lispval value = KNO_EMPTY_CHOICE, svalue, tval; int i = 2, retval;
  KNO_INIT_BYTE_OUTPUT(&out,1024);
  KNO_DO_LIBINIT(kno_init_lisp_types);
  tval = kno_parse(argv[i++]);
  while (i < argc) {
    lispval object = kno_parse(argv[i]);
    KNO_ADD_TO_CHOICE(value,object); i++;}
  svalue = kno_make_simple_choice(value);
  {KNO_DO_CHOICES(x,svalue)
     u8_fprintf(stdout," %q\n",x);}
  if (kno_choice_containsp(tval,svalue))
    u8_fprintf(stdout,"Containment is true\n");
  else u8_fprintf(stdout,"Containment is false\n");
  kno_write_dtype(&out,svalue);
  retval = fwrite(out.buffer,1,out.bufwrite-out.buffer,f);
  if (retval<0) exit(1);
  kno_decref(value); kno_decref(svalue);
  kno_close_outbuf(&out);
  value = KNO_VOID;
  svalue = KNO_VOID;
  exit(0);
}

