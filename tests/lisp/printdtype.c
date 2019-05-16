/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/dtype.h"
#include "kno/streams.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc,char **argv)
{
  lispval object;
  struct KNO_STREAM *in; u8_string srep;
  KNO_DO_LIBINIT(kno_init_lisp_types);
  in = kno_open_file(argv[1],KNO_FILE_READ);
  object = kno_read_dtype(kno_readbuf(in));
  kno_close_stream(in,KNO_STREAM_CLOSE_FULL);
  /* For coverage tests */
  srep = kno_lisp2string(object); u8_free(srep);
  /* Print it out */
  u8_fprintf(stdout,"%q\n",object);
  kno_decref(object); object = KNO_VOID;
  exit(0);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
