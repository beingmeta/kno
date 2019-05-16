/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1
#define KNO_PROVIDE_FASTEVAL 1

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/frames.h"
#include "kno/tables.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/fdweb.h"
#include "kno/support.h"

static int fdweb_init_done = 0;

KNO_EXPORT void kno_init_fdweb()
{
  if (fdweb_init_done) return;
  else {
    int fdscheme_version = kno_init_scheme();
    lispval fdweb_module = kno_new_cmodule("FDWEB",0,kno_init_fdweb);
    lispval safe_fdweb_module =
      kno_new_cmodule("FDWEB",(KNO_MODULE_SAFE),kno_init_fdweb);
    lispval xhtml_module =
      kno_new_cmodule("XHTML",KNO_MODULE_SAFE,kno_init_fdweb);
    fdweb_init_done = fdscheme_version;
    kno_init_xmloutput_c();
    kno_init_htmlout_c();
    kno_init_xmldata_c();
    kno_init_xmlinput_c();
    kno_init_mime_c();
    kno_init_email_c();
    kno_init_xmleval_c();
    kno_init_cgiexec_c();
    kno_init_urifns_c();
    kno_init_json_c();
#if (KNO_WITH_CURL)
    kno_init_curl_c();
#endif
    kno_finish_module(safe_fdweb_module);
    kno_finish_module(fdweb_module);
    kno_finish_module(xhtml_module);}

  u8_threadcheck();

  u8_register_source_file(KNO_FDWEB_H_INFO);
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
