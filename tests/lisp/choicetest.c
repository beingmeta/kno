/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc,char **argv)
{
  struct FD_OUTBUF out = { 0 };
  FILE *f = fopen(argv[1],"wb");
  lispval value = FD_EMPTY_CHOICE, svalue, tval; int i = 2, retval;
  FD_INIT_BYTE_OUTPUT(&out,1024);
  FD_DO_LIBINIT(fd_init_lisp_types);
  tval = fd_parse(argv[i++]);
  while (i < argc) {
    lispval object = fd_parse(argv[i]);
    FD_ADD_TO_CHOICE(value,object); i++;}
  svalue = fd_make_simple_choice(value);
  {FD_DO_CHOICES(x,svalue)
     u8_fprintf(stdout," %q\n",x);}
  if (fd_choice_containsp(tval,svalue))
    u8_fprintf(stdout,"Containment is true\n");
  else u8_fprintf(stdout,"Containment is false\n");
  fd_write_dtype(&out,svalue);
  retval = fwrite(out.buffer,1,out.bufwrite-out.buffer,f);
  if (retval<0) exit(1);
  fd_decref(value); fd_decref(svalue);
  fd_close_outbuf(&out);
  value = FD_VOID;
  svalue = FD_VOID;
  exit(0);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
