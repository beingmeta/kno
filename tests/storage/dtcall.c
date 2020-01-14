/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/drivers.h"

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
  int kno_version = kno_init_drivers(), i = 0;
  struct KNO_STREAM ds;
  lispval expr = KNO_EMPTY_LIST, result;
  if (kno_version<0) {
    u8_fprintf(stderr,"Couldn't initialize Kno\n");
    exit(1);}
  socket = u8_connect(argv[1]);
  if (socket<0) {
    u8_fprintf(stderr,"Couldn't open socket to %s\n",argv[1]);
    exit(1);}
  kno_init_stream(&ds,argv[1],socket,KNO_STREAM_SOCKET,1024);
  i = argc-1; while (i>1) expr = kno_make_pair(kno_parse(argv[i--]),expr);
  u8_fprintf(stderr,_("Sending: %q\n"),expr);
  kno_write_dtype(kno_writebuf(&ds),expr);
  result = kno_read_dtype(kno_readbuf(&ds));
  u8_fprintf(stderr,_("Result is: %q\n"),result);
  kno_decref(expr); kno_decref(result);
  kno_close_stream(&ds,KNO_STREAM_CLOSE_FULL);
  return 0;
}

