/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: tablekeys.c,v 1.13 2006/01/26 14:44:33 haase Exp $";

#include "fdb/dtype.h"
#include "fdb/dtypestream.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

int main(int argc,char **argv)
{
  FILE *f=fopen(argv[1],"r");
  struct FD_DTYPE_STREAM *in, *out;
  fdtype ht, keys;
  FD_DO_LIBINIT(fd_init_dtypelib);
  in=fd_dtsopen(argv[1],FD_DTSTREAM_READ);
  ht=fd_dtsread_dtype(in); fd_dtsclose(in,1);
  keys=fd_hashtable_keys(FD_XHASHTABLE(ht));
  fprintf(stderr,_("Found %d keys\n"),FD_CHOICE_SIZE(keys));
  in=fd_dtsopen(argv[2],FD_DTSTREAM_CREATE);
  fd_dtswrite_dtype(out,keys);
  fd_decref(keys); keys=FD_VOID;
  fd_decref(ht); ht=FD_VOID;
  fd_dtsclose(out,FD_DTSCLOSE_FULL);
}


/* The CVS log for this file
   $Log: tablekeys.c,v $
   Revision 1.13  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.12  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.11  2005/05/30 17:48:09  haase
   Fixed some header ordering problems

   Revision 1.10  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.9  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
