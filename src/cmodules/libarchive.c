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
#include <archive_entry.h>

fd_ptr_type fd_libarchive_type;
FD_EXPORT int fd_init_libarchive(void) FD_LIBINIT_FN;

static long long int libarchive_initialized = 0;

struct FD_ARCHIVE {
  FD_CONS_HEADER;
  u8_string spec;
  struct archive *fd_archive;
  u8_mutex archive_lock;
  lispval archive_opts, archive_source;} *fd_archive;

static lispval open_archive(lispval spec,lispval opts)
{
  int status = -1;
  struct archive *archive = archive_read_new();
  archive_read_support_compression_all(archive);
  archive_read_support_format_all(archive);
  if (FD_STRINGP(spec)) {
    long long bufsz = fd_getfixopt(opts,"BUFSIZE",16000);
    status = archive_read_open_filename(archive,FD_CSTRING(spec),bufsz);}
  else if (FD_PACKETP(spec))
    status = archive_read_open_memory
      (archive,FD_PACKET_DATA(spec),FD_PACKET_LENGTH(spec));
  else {
    archive_read_close(archive);
    fd_seterr("InvalidArchiveSpec","open_archive",NULL,spec);
    return FD_ERROR;}
  if (status < 0) {
    archive_read_close(archive);
    fd_seterr("LibArchiveError","open_archive",NULL,spec);
    return FD_ERROR;}
  else {
    struct FD_ARCHIVE *obj = u8_alloc(struct FD_ARCHIVE);
    FD_INIT_FRESH_CONS(obj,fd_libarchive_type);
    u8_init_mutex(&(obj->archive_lock));
    obj->archive_source = source; fd_incref(source);
    obj->archive_opts = opts; fd_incref(opts);
    obj->fd_archive = archive;
    return (lispval) obj;}
}

static void set_time_prop(lispval tbl,u8_string slotname,time_t t)
{
  if (t >= 0) {
    lispval v = fd_time2timestamp(t);
    fd_store(tbl,fd_intern(slotname),v);
    fd_decref(v);}
}

static void set_string_prop(lispval tbl,u8_string slotname,u8_string s)
{
  if (s) {
    lispval v = fdstring(s);
    fd_store(tbl,fd_intern(slotname),v);
    fd_decref(v);}
}

static void set_int_prop(lispval tbl,u8_string slotname,long long ival)
{
  if (s) {
    lispval v = FD_INT2DTYPE(ival);
    fd_store(tbl,fd_intern(slotname),v);
    fd_decref(v);}
}

static lispval entry_info(struct FD_ARCHIVE *entry)
{
  lispval tbl = fd_make_slotmap(0,7,NULL);
  set_time_prop(tbl,"ATIME",archive_entry_atime(entry));
  set_time_prop(tbl,"CTIME",archive_entry_ctime(entry));
  set_time_prop(tbl,"MTIME",archive_entry_mtime(entry));
  set_string_prop(tbl,"MODE",archive_entry_strmode(entry));
  set_string_prop(tbl,"UID",archive_entry_uname_utf8(entry));
  set_string_prop(tbl,"GID",archive_entry_gname_utf8(entry));
  set_string_prop(tbl,"SYMLINK",archive_entry_symlink_utf8(entry));
  set_string_prop(tbl,"PATH",archive_entry_pathname_utf8(entry));
  set_string_prop(tbl,"PATH",archive_entry_pathname_utf8(entry));
  set_string_prop(tbl,"FLAGS",archive_entry_fflags_text(entry));
  set_int_prop(tbl,"SIZE",archive_entry_pathname_size(entry));
  return tbl;
}

static lispval archive_next(lispval obj)
{
  struct FD_ARCHIVE *archive = (fd_archive) obj;
  struct archive_entry *entry;
  int rv = archive_read_next_header(archive->fd_archive,&entry);
  if (PRED_FALSE(rv != ARCHIVE_OK)) {
    return FD_ERROR;}
  else return entry_info(entry);
}

FD_EXPORT void fd_init_libarchive_c()
{
  if (libarchive_initialized)
    return;
  else libarchive_initialized = u8_millitime();

  fd_libarchive_type = fd_register_cons_type("archive");

  fd_idefn2("OPEN-ARCHIVE",open_archive,1,
            "Opens an archive file",
            -1,FD_VOID,-1,FD_FALSE);
  fd_idefn2("ARCHIVE/NEXT",archive_next,1,
            "Get the next archive entry",
            -1,FD_VOID,-1,FD_FALSE);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
