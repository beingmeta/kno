/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/bytestream.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc,char **argv)
{
  fdtype object;
  struct FD_BYTESTREAM *in; u8_string srep;
  FD_DO_LIBINIT(fd_init_dtypelib);
  in=fd_bytestream_open(argv[1],FD_BYTESTREAM_READ);
  object=fd_bytestream_read_dtype(in);
  fd_bytestream_close(in,FD_BYTESTREAM_CLOSE_FULL);
  /* For coverage tests */
  srep=fd_dtype2string(object); u8_free(srep);
  /* Print it out */
  u8_fprintf(stdout,"%q\n",object);
  fd_decref(object); object=FD_VOID;
  exit(0);
}
