/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2011 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/dbfile.h"

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
  u8_connection socket;
  int fd_version=fd_init_dbfile(), i=0;
  struct FD_DTYPE_STREAM ds;
  fdtype expr=FD_EMPTY_LIST, result;
  socket=u8_connect(argv[1]);
  if (socket<0) {
    u8_fprintf(stderr,"Couldn't open socket to %s\n",argv[1]);
    exit(1);}
  fd_init_dtype_stream(&ds,socket,1024);
  i=argc-1; while (i>1) expr=fd_make_pair(fd_parse(argv[i--]),expr);
  u8_fprintf(stderr,_("Sending: %q\n"),expr);
  fd_dtswrite_dtype(&ds,expr); 
  result=fd_dtsread_dtype(&ds);
  u8_fprintf(stderr,_("Result is: %q\n"),result);
  fd_decref(expr); fd_decref(result);
  fd_dtsclose(&ds,1);
  return 0;
}
