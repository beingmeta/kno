/* -*- Mode: C; -*- */

/* tagger.c
   Copyright (C) 2005-2011 beingmeta, inc.
   This is the initialization file for the tagger module.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/fddb.h"
#include "fdb/eval.h"
#include "fdb/sequences.h"
#include "fdb/texttools.h"
#include "fdb/tagger.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>

static int tagger_init_done=0;

void fd_init_tagger()
{
  fdtype tagger_module=fd_new_module("TAGGER",(FD_MODULE_SAFE));
  
  if (tagger_init_done) return;
  else tagger_init_done=1;
  fd_register_source_file(versionid);
  
  fd_init_ofsm_c();
  fd_init_tagxtract_c();
  
  fd_finish_module(tagger_module);
  fd_persist_module(tagger_module);

}


