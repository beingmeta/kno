/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/dtypestream.h"

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
