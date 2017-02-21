/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/bytestream.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

int main(int argc,char **argv)
{
  struct FD_BYTESTREAM *in, *out;
  struct FD_BYTE_INBUF *inbuf;
  struct FD_BYTE_OUTBUF *outbuf;
  fdtype ht, keys;
  FD_DO_LIBINIT(fd_init_dtypelib);
  in=fd_bytestream_open(argv[1],FD_BYTESTREAM_READ);
  inbuf=fd_readbuf(in);
  ht=fd_read_dtype(inbuf);
  fd_close_bytestream(in,FD_BYTESTREAM_CLOSE_FULL);
  keys=fd_hashtable_keys(FD_XHASHTABLE(ht));
  fprintf(stderr,_("Found %d keys\n"),FD_CHOICE_SIZE(keys));
  out=fd_bytestream_open(argv[2],FD_BYTESTREAM_CREATE);
  outbuf=fd_writebuf(out);
  fd_write_dtype(outbuf,keys);
  fd_decref(keys); keys=FD_VOID;
  fd_decref(ht); ht=FD_VOID;
  fd_close_bytestream(out,FD_BYTESTREAM_CLOSE_FULL);
  return 0;
}
