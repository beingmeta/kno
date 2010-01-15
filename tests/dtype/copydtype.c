/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2010 beingmeta, inc.
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

