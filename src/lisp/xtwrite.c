/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_BUFIO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/xtypes.h"

#include <libu8/u8elapsed.h>

#include <zlib.h>
#include <errno.h>

#ifndef KNO_DEBUG_XTYPEIO
#define KNO_DEBUG_XTYPEIO 0
#endif

/* File initialization */

KNO_EXPORT void kno_init_xtwrite_c()
{
  u8_register_source_file(_FILEINFO);
}
