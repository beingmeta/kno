/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2012 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "framerd/dtype.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc,char **argv)
{
  struct FD_BYTE_OUTPUT out; FILE *f=fopen(argv[1],"wb");
  fdtype value=FD_EMPTY_CHOICE, svalue, tval; int i=2, retval;
  FD_INIT_BYTE_OUTPUT(&out,1024);
  FD_DO_LIBINIT(fd_init_dtypelib);
  tval=fd_parse(argv[i++]);
  while (i < argc) {
    fdtype object=fd_parse(argv[i]);
    FD_ADD_TO_CHOICE(value,object); i++;}
  svalue=fd_make_simple_choice(value);
  {FD_DO_CHOICES(x,svalue)
     u8_fprintf(stdout," %q\n",x);}
  if (fd_choice_containsp(tval,svalue))
    u8_fprintf(stdout,"Containment is true\n");
  else u8_fprintf(stdout,"Containment is false\n");
  fd_write_dtype(&out,svalue);
  retval=fwrite(out.start,1,out.ptr-out.start,f);
  fd_decref(value); fd_decref(svalue); u8_free(out.start);
  value=FD_VOID; svalue=FD_VOID;
  exit(0);
}

