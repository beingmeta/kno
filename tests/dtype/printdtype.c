/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/dtypestream.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc,char **argv)
{
  fdtype object;
  struct FD_DTYPE_STREAM *in; u8_string srep;
  FD_DO_LIBINIT(fd_init_dtypelib);
  in=fd_dtsopen(argv[1],FD_DTSTREAM_READ);
  object=fd_dtsread_dtype(in);
  fd_dtsclose(in,FD_DTSCLOSE_FULL);
  /* For coverage tests */
  srep=fd_dtype2string(object); u8_free(srep);
  /* Print it out */
  u8_fprintf(stdout,"%q\n",object);
  fd_decref(object); object=FD_VOID;
  exit(0);
}


/* The CVS log for this file
   $Log: printdtype.c,v $
   Revision 1.17  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.16  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.15  2005/05/30 17:48:09  haase
   Fixed some header ordering problems

   Revision 1.14  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.13  2005/02/14 01:29:35  haase
   Increased code coverage of tests

   Revision 1.12  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
