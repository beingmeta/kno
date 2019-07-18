/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/numbers.h"
#include "kno/streams.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

static int write_dtype_to_file(lispval object,FILE *f)
{
  struct KNO_OUTBUF out = { 0 };
  int retval;
  KNO_INIT_BYTE_OUTPUT(&out,1024);
  retval = kno_write_dtype(&out,object);
  retval = fwrite(out.buffer,1,out.bufwrite-out.buffer,f);
  kno_close_outbuf(&out);
  return retval;
}

int main(int argc,char **argv)
{
  int lispv = kno_init_lisp_types();
  lispval fix1 = KNO_INT(33994);
  lispval dbl1 = kno_init_flonum(NULL,3.445);
  lispval dbl2 = kno_init_flonum(u8_alloc(struct KNO_FLONUM),-3.9994);
  struct KNO_STRING *strcons = u8_alloc(struct KNO_STRING);
  lispval string1 = kno_make_string(NULL,3,"foo");
  lispval string2 = kno_init_string(strcons,3,u8_strdup("bar"));
  strcons->str_freebytes = 1;
  lispval compound=
    kno_init_compound(NULL,KNOSYM_QUOTE,KNO_COMPOUND_USEREF,1,
                     kno_make_pair(KNO_INT(5),KNO_TRUE));
  lispval vec = kno_make_nvector(3,fix1,dbl1,string1);
  lispval lst = kno_make_list(4,vec,string2,dbl2,compound);
  u8_string as_string = kno_lisp2string(lst);
  lispval tmp = kno_parse(as_string);
  FILE *f = fopen("testcapi.dtype","wb");
  if (lispv<0) {
    u8_log(LOG_WARN,"STARTUP","Couldn't initialize DTypes");
    exit(1);}

  if (write_dtype_to_file(lst,f)<0)
    fprintf(stderr,"write_dtype failed\n");
  if (LISP_EQUAL(tmp,lst)) kno_decref(tmp);
  else fprintf(stderr,"Reparse didn't work\n");
  fclose(f);
  fprintf(stdout,"%s\n",as_string);
  kno_decref(lst);
  u8_free(as_string);
  return 0;
}

