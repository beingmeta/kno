/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: xmloutput.c 2035 2007-11-05 13:32:17Z haase $";

#define U8_INLINE_IO 1
#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/frames.h"
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/ports.h"
#include "fdb/fdweb.h"
#include "fdb/support.h"

#include "fdb/support.h"

static int fdweb_init_done=0;

FD_EXPORT void fd_init_fdweb()
{
  if (fdweb_init_done) return;
  else {
    fdtype fdweb_module=fd_new_module("FDWEB",FD_MODULE_DEFAULT);
    fdtype safe_fdweb_module=fd_new_module("FDWEB",(FD_MODULE_DEFAULT|FD_MODULE_SAFE));
    fdtype xhtml_module=fd_new_module("XHTML",FD_MODULE_SAFE);
    fdweb_init_done=1;
    fd_init_xmloutput_c();
    fd_init_xmldata_c();
    fd_init_xmlinput_c();
    fd_init_mime_c();
    fd_init_email_c();
    fd_init_xmleval_c();
    fd_init_cgiexec_c();
    fd_init_urifns_c();
#if (FD_WITH_CURL)
    fd_init_curl_c();
#endif
#if (FD_WITH_EXIF)
    fd_init_exif_c();
#endif
    fd_finish_module(safe_fdweb_module);
    fd_finish_module(fdweb_module);
    fd_finish_module(xhtml_module);
    fd_persist_module(safe_fdweb_module);
    fd_persist_module(fdweb_module);
    fd_persist_module(xhtml_module);}

  fd_register_source_file(FDB_FDWEB_H_VERSION);
  fd_register_source_file(versionid);
}
