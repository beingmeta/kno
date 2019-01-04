/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/drivers.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>
#include <libu8/u8netfns.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

int main(int argc,char **argv)
{
  u8_socket socket;
  int fd_version = fd_init_drivers(), i = 0;
  struct FD_STREAM ds;
  lispval expr = FD_EMPTY_LIST, result;
  if (fd_version<0) {
    u8_fprintf(stderr,"Couldn't initialize FramerD\n");
    exit(1);}
  socket = u8_connect(argv[1]);
  if (socket<0) {
    u8_fprintf(stderr,"Couldn't open socket to %s\n",argv[1]);
    exit(1);}
  fd_init_stream(&ds,argv[1],socket,FD_STREAM_SOCKET,1024);
  i = argc-1; while (i>1) expr = fd_make_pair(fd_parse(argv[i--]),expr);
  u8_fprintf(stderr,_("Sending: %q\n"),expr);
  fd_write_dtype(fd_writebuf(&ds),expr);
  result = fd_read_dtype(fd_readbuf(&ds));
  u8_fprintf(stderr,_("Result is: %q\n"),result);
  fd_decref(expr); fd_decref(result);
  fd_close_stream(&ds,FD_STREAM_CLOSE_FULL);
  return 0;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
