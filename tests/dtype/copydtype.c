/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/bytestream.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc,char **argv)
{
  fdtype object, copied;
  struct FD_BYTESTREAM *in, *out;
  int bytes=0;
  FD_DO_LIBINIT(fd_init_dtypelib);
  in=fd_bytestream_open(argv[1],FD_BYTESTREAM_READ);
  out=fd_bytestream_open(argv[2],FD_BYTESTREAM_CREATE);
  object=fd_read_dtype(fd_readbuf(in));
  fd_close_bytestream(in,FD_BYTESTREAM_CLOSE_FULL);
  copied=fd_copy(object); fd_decref(copied);
  copied=fd_deep_copy(object);
  bytes=fd_write_dtype(fd_writebuf(out),copied);
  if (bytes<0)
    u8_fprintf(stdout,"Error writing %q\n",object);
  else u8_fprintf(stdout,"Wrote %d bytes:\n %q\n",bytes,object);
  fd_close_bytestream(out,FD_BYTESTREAM_CLOSE_FULL);
  fd_decref(object); object=FD_VOID;
  fd_decref(copied); copied=FD_VOID;
  exit(0);
}

