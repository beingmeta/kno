/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/pathstore.h"

#include <libu8/libu8.h>

/* Tracking the current source base */

/* Initialization */

static int sys_pathstore_c_init_done = 0;

void kno_init_pathstore_c()
{
  if (sys_pathstore_c_init_done)
    return;
  else sys_pathstore_c_init_done=1;

  u8_register_source_file(_FILEINFO);
}

