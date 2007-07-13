/* C Mode */

/* texttools.c
   This is the core texttools file for the FDB library
   Copyright (C) 2005-2007 beingmeta, inc.
*/

static char versionid[] =
  "$Id: filepools.c 1354 2007-07-09 04:04:49Z haase $";

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


