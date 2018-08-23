/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/compounds.h"
#include "framerd/numbers.h"
#include "framerd/streams.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

static int write_dtype_to_file(lispval object,FILE *f)
{
  struct FD_OUTBUF out = { 0 };
  int retval;
  FD_INIT_BYTE_OUTPUT(&out,1024);
  retval = fd_write_dtype(&out,object);
  retval = fwrite(out.buffer,1,out.bufwrite-out.buffer,f);
  fd_close_outbuf(&out);
  return retval;
}

int main(int argc,char **argv)
{
  int lispv = fd_init_lisp_types();
  lispval fix1 = FD_INT(33994);
  lispval dbl1 = fd_init_flonum(NULL,3.445);
  lispval dbl2 = fd_init_flonum(u8_alloc(struct FD_FLONUM),-3.9994);
  lispval string1 = fd_make_string(NULL,3,"foo");
  lispval string2 = fd_init_string(u8_alloc(struct FD_STRING),3,u8_strdup("bar"));
  lispval compound=
    fd_init_compound(NULL,FDSYM_QUOTE,FD_COMPOUND_USEREF,1,
                     fd_make_pair(FD_INT(5),FD_TRUE));
  lispval vec = fd_make_nvector(3,fix1,dbl1,string1);
  lispval lst = fd_make_list(4,vec,string2,dbl2,compound);
  u8_string as_string = fd_lisp2string(lst);
  lispval tmp = fd_parse(as_string);
  FILE *f = fopen("testcapi.dtype","wb");
  if (lispv<0) {
    u8_log(LOG_WARN,"STARTUP","Couldn't initialize DTypes");
    exit(1);}

  if (write_dtype_to_file(lst,f)<0)
    fprintf(stderr,"write_dtype failed\n");
  if (LISP_EQUAL(tmp,lst)) fd_decref(tmp);
  else fprintf(stderr,"Reparse didn't work\n");
  fclose(f);
  fprintf(stdout,"%s\n",as_string);
  fd_decref(lst);
  u8_free(as_string);
  return 0;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
