/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"

#include <libu8/u8.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

static void write_dtype_to_file(fdtype object,FILE *f)
{
  struct FD_BYTE_OUTPUT out;
  FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
  fd_write_dtype(&out,object);
  fwrite(out.start,1,out.ptr-out.start,f);
  u8_free(out.start);
}

int main(int argc,char **argv)
{
  fdtype object;
  FILE *f=fopen(argv[1],"wb");
  FD_DO_LIBINIT(fd_init_dtypelib);
  object=fd_parse(argv[2]);
  write_dtype_to_file(object,f);
  u8_fprintf(stderr,"dumped %q\n",object);
  fd_decref(object);
  exit(0);
}


/* The CVS log for this file
   $Log: u8makedtype.c,v $
   Revision 1.11  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.10  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.9  2005/05/30 17:48:09  haase
   Fixed some header ordering problems

   Revision 1.8  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.7  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
