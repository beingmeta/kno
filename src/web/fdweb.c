/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1
#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/frames.h"
#include "framerd/tables.h"
#include "framerd/eval.h"
#include "framerd/ports.h"
#include "framerd/fdweb.h"
#include "framerd/support.h"

static int fdweb_init_done = 0;

FD_EXPORT void fd_init_fdweb()
{
  if (fdweb_init_done) return;
  else {
    int fdscheme_version = fd_init_scheme();
    lispval fdweb_module = fd_new_cmodule("FDWEB",0,fd_init_fdweb);
    lispval safe_fdweb_module =
      fd_new_cmodule("FDWEB",(FD_MODULE_SAFE),fd_init_fdweb);
    lispval xhtml_module =
      fd_new_cmodule("XHTML",FD_MODULE_SAFE,fd_init_fdweb);
    fdweb_init_done = fdscheme_version;
    fd_init_xmloutput_c();
    fd_init_htmlout_c();
    fd_init_xmldata_c();
    fd_init_xmlinput_c();
    fd_init_mime_c();
    fd_init_email_c();
    fd_init_xmleval_c();
    fd_init_cgiexec_c();
    fd_init_urifns_c();
    fd_init_json_c();
#if (FD_WITH_CURL)
    fd_init_curl_c();
#endif
    fd_finish_module(safe_fdweb_module);
    fd_finish_module(fdweb_module);
    fd_finish_module(xhtml_module);}

  u8_threadcheck();

  u8_register_source_file(FRAMERD_FDWEB_H_INFO);
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
