/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/streams.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc,char **argv)
{
  lispval object;
  struct FD_STREAM *in; u8_string srep;
  FD_DO_LIBINIT(fd_init_lisp_types);
  in = fd_open_file(argv[1],FD_FILE_READ);
  object = fd_read_dtype(fd_readbuf(in));
  fd_close_stream(in,FD_STREAM_CLOSE_FULL);
  /* For coverage tests */
  srep = fd_lisp2string(object); u8_free(srep);
  /* Print it out */
  u8_fprintf(stdout,"%q\n",object);
  fd_decref(object); object = FD_VOID;
  exit(0);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
