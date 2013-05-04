/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* tagger.c
   Copyright (C) 2005-2013 beingmeta, inc.
   This is the initialization file for the tagger module.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"
#include "framerd/texttools.h"
#include "framerd/tagger.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>

static int tagger_init_done=0;

void fd_init_tagger()
{
  fdtype tagger_module=fd_new_module("TAGGER",(FD_MODULE_SAFE));
  
  if (tagger_init_done) return;
  else tagger_init_done=1;
  u8_register_source_file(_FILEINFO);
  
  fd_init_ofsm_c();
  fd_init_tagxtract_c();
  
  fd_finish_module(tagger_module);
  fd_persist_module(tagger_module);

}


