/* -*- Mode: C; character-encoding: utf-8; -*- */

/* mongodb.c
   This implements FramerD bindings to odbc.
   Copyright (C) 2007-2013 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"
#include "framerd/texttools.h"

#include "framerd/extdb.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include <mongo.h>
/* Initialization */

static int mongodb_initialized=0;

FD_EXPORT int fd_init_odbc()
{
  fdtype module;
  if (mongodb_initialized) return 0;
  mongodb_initialized=1;
  fd_init_fdscheme();

  module=fd_new_module("MONGODB",(0));

  fd_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return 1;
}
