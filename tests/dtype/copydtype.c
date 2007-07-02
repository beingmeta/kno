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

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc,char **argv)
{
  fdtype object, copied;
  struct U8_OUTPUT sout;
  struct FD_DTYPE_STREAM *in, *out;
  int bytes=0;
  FD_DO_LIBINIT(fd_init_dtypelib);
  in=fd_dtsopen(argv[1],FD_DTSTREAM_READ);
  out=fd_dtsopen(argv[2],FD_DTSTREAM_CREATE);
  object=fd_dtsread_dtype(in);
  fd_dtsclose(in,FD_DTSCLOSE_FULL);
  copied=fd_copy(object); fd_decref(copied);
  copied=fd_deep_copy(object);
  bytes=fd_dtswrite_dtype(out,copied);
  if (bytes<0)
    u8_fprintf(stdout,"Error writing %q\n",object);
  else u8_fprintf(stdout,"Wrote %d bytes:\n %q\n",bytes,object);
  fd_dtsclose(out,FD_DTSCLOSE_FULL);
  fd_decref(object); object=FD_VOID;
  fd_decref(copied); copied=FD_VOID;
  exit(0);
}


/* The CVS log for this file
   $Log: copydtype.c,v $
   Revision 1.14  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.13  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.12  2005/05/30 17:48:08  haase
   Fixed some header ordering problems

   Revision 1.11  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.10  2005/02/14 01:29:35  haase
   Increased code coverage of tests

   Revision 1.9  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
