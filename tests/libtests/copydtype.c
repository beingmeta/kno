/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#include "kno/lisp.h"
#include "kno/streams.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc,char **argv)
{
  lispval object, copied;
  struct KNO_STREAM *in, *out;
  int bytes = 0;
  KNO_DO_LIBINIT(kno_init_lisp_types);
  in = kno_open_file(argv[1],KNO_FILE_READ);
  out = kno_open_file(argv[2],KNO_FILE_CREATE);
  object = kno_read_dtype(kno_readbuf(in));
  kno_close_stream(in,KNO_STREAM_CLOSE_FULL);
  copied = kno_copy(object); kno_decref(copied);
  copied = kno_deep_copy(object);
  bytes = kno_write_dtype(kno_writebuf(out),copied);
  if (bytes<0)
    u8_fprintf(stdout,"Error writing %q\n",object);
  else u8_fprintf(stdout,"Wrote %d bytes:\n %q\n",bytes,object);
  kno_close_stream(out,KNO_STREAM_CLOSE_FULL);
  kno_decref(object); object = KNO_VOID;
  kno_decref(copied); copied = KNO_VOID;
  exit(0);
}

