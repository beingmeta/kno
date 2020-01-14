/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/lisp.h"
#include "kno/streams.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

int main(int argc,char **argv)
{
  struct KNO_STREAM *in, *out;
  struct KNO_INBUF *inbuf;
  struct KNO_OUTBUF *outbuf;
  lispval ht, keys;
  KNO_DO_LIBINIT(kno_init_lisp_types);
  in = kno_open_file(argv[1],KNO_FILE_READ);
  inbuf = kno_readbuf(in);
  ht = kno_read_dtype(inbuf);
  kno_close_stream(in,KNO_STREAM_CLOSE_FULL);
  keys = kno_hashtable_keys(KNO_XHASHTABLE(ht));
  fprintf(stderr,_("Found %d keys\n"),KNO_CHOICE_SIZE(keys));
  out = kno_open_file(argv[2],KNO_FILE_CREATE);
  outbuf = kno_writebuf(out);
  kno_write_dtype(outbuf,keys);
  kno_decref(keys); keys = KNO_VOID;
  kno_decref(ht); ht = KNO_VOID;
  kno_close_stream(out,KNO_STREAM_CLOSE_FULL);
  return 0;
}

