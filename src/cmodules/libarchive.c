/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"
#include "framerd/support.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>

#include <archive.h>

fd_ptr_type fd_libarchive_type;
FD_EXPORT int fd_init_libarchive(void) FD_LIBINIT_FN;

static long long int libarchive_initialized = 0;

struct FD_ARCHIVE {
  FD_CONS_HEADER;
  u8_string spec;
  struct archive *fd_archive;
  u8_mutex archive_lock;
  lispval archive_opts;} *fd_archive;

#if 0
static lispval open_archive(lispval spec,lispval opts)
{
  struct FD_ARCHIVE *a = u8_alloc(struct FD_ARCHIVE);
  FD_INIT_FRESH_CONS(a,fd_libarchive_type);
  if (FD_STRINGP(spec)) {
    lispval foo;}
  else if (FD_PACKETP(spec)) {}
  else {
    u8_free(a);
    fd_seterr("InvalidArchiveSpec","open_archive",NULL,spec);
    return FD_ERROR;}
  u8_init_mutex(&(a->archive_lock));
  a->archive_opts = opts; fd_incref(opts);
  return (lispval) a;
}
#endif

FD_EXPORT void fd_init_libarchive_c()
{
  if (libarchive_initialized)
    return;
  else libarchive_initialized = u8_millitime();

  fd_libarchive_type = fd_register_cons_type("file archive");

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
