/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2014 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/dtypestream.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

int main(int argc,char **argv)
{
  struct FD_DTYPE_STREAM *in, *out;
  fdtype ht, keys;
  FD_DO_LIBINIT(fd_init_dtypelib);
  in=fd_dtsopen(argv[1],FD_DTSTREAM_READ);
  ht=fd_dtsread_dtype(in); fd_dtsclose(in,1);
  keys=fd_hashtable_keys(FD_XHASHTABLE(ht));
  fprintf(stderr,_("Found %d keys\n"),FD_CHOICE_SIZE(keys));
  out=fd_dtsopen(argv[2],FD_DTSTREAM_CREATE);
  fd_dtswrite_dtype(out,keys);
  fd_decref(keys); keys=FD_VOID;
  fd_decref(ht); ht=FD_VOID;
  fd_dtsclose(out,FD_DTSCLOSE_FULL);
  return 0;
}
