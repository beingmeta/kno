/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc,char **argv)
{
  struct FD_BYTE_OUTPUT out; FILE *f=fopen(argv[1],"wb");
  fdtype value=FD_EMPTY_CHOICE, svalue, tval; int i=2;
  FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
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
  fwrite(out.start,1,out.ptr-out.start,f);
  fd_decref(value); fd_decref(svalue); u8_free(out.start);
  value=FD_VOID; svalue=FD_VOID;
  exit(0);
}


/* The CVS log for this file
   $Log: choicetest.c,v $
   Revision 1.14  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.13  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.12  2005/05/30 17:48:08  haase
   Fixed some header ordering problems

   Revision 1.11  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.10  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
