/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/streams.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

int main(int argc,char **argv)
{
  struct FD_STREAM *in, *out;
  struct FD_INBUF *inbuf;
  struct FD_OUTBUF *outbuf;
  lispval ht, keys;
  FD_DO_LIBINIT(fd_init_lisp_types);
  in = fd_open_file(argv[1],FD_FILE_READ);
  inbuf = fd_readbuf(in);
  ht = fd_read_dtype(inbuf);
  fd_close_stream(in,FD_STREAM_CLOSE_FULL);
  keys = fd_hashtable_keys(FD_XHASHTABLE(ht));
  fprintf(stderr,_("Found %d keys\n"),FD_CHOICE_SIZE(keys));
  out = fd_open_file(argv[2],FD_FILE_CREATE);
  outbuf = fd_writebuf(out);
  fd_write_dtype(outbuf,keys);
  fd_decref(keys); keys = FD_VOID;
  fd_decref(ht); ht = FD_VOID;
  fd_close_stream(out,FD_STREAM_CLOSE_FULL);
  return 0;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
