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

typedef struct FD_ARCHIVE {
  FD_CONS_HEADER;
  u8_string archive_spec;
  struct archive *fd_archive;
  u8_mutex archive_lock;
  lispval archive_opts, archive_source;} *fd_archive;

static lispval open_archive(lispval spec,lispval opts)
{
  int status = -1;
  u8_string use_spec = NULL;
  struct archive *archive = archive_read_new();
  archive_read_support_filter_all(archive);
  archive_read_support_format_all(archive);
  if (FD_STRINGP(spec)) {
    long long bufsz = fd_getfixopt(opts,"BUFSIZE",16000);
    status = archive_read_open_filename(archive,FD_CSTRING(spec),bufsz);
    if (status == ARCHIVE_OK)
      use_spec = u8_strdup(FD_CSTRING(spec));}
  else if (FD_PACKETP(spec)) {
    status = archive_read_open_memory
      (archive,FD_PACKET_DATA(spec),FD_PACKET_LENGTH(spec));
    if (status == ARCHIVE_OK)
      use_spec = u8_mkstring("%lldB@0x%llx",
                             FD_PACKET_LENGTH(spec),
                             (unsigned long long) FD_PACKET_DATA(spec));}
  else {
    archive_read_close(archive);
    fd_seterr("InvalidArchiveSpec","open_archive",NULL,spec);
    return FD_ERROR_VALUE;}
  if (status < 0) {
    archive_read_close(archive);
    fd_seterr("LibArchiveError","open_archive",NULL,spec);
    return FD_ERROR_VALUE;}
  else {
    struct FD_ARCHIVE *obj = u8_alloc(struct FD_ARCHIVE);
    FD_INIT_FRESH_CONS(obj,fd_libarchive_type);
    u8_init_mutex(&(obj->archive_lock));
    obj->archive_source = spec; fd_incref(spec);
    obj->archive_opts = opts; fd_incref(opts);
    obj->archive_spec = use_spec;
    obj->fd_archive = archive;
    return (lispval) obj;}
}

static int unparse_archive(struct U8_OUTPUT *out,lispval x)
{
  struct FD_ARCHIVE *fdarchive = (struct FD_ARCHIVE *)x;
  if (fdarchive->archive_spec)
    u8_printf(out,"#<FileArchive %s #!0x%llx>",
              fdarchive->archive_spec,
              (unsigned long long)fdarchive);
  else u8_printf(out,"#<FileArchive #!0x%llx>",
                 (unsigned long long)fdarchive);
  return 1;
}
static void recycle_archive(struct FD_RAW_CONS *c)
{
  struct FD_ARCHIVE *a = (fd_archive) c;
  archive_read_close(a->fd_archive);
  u8_destroy_mutex(&(a->archive_lock));
  fd_decref(a->archive_source);
  fd_decref(a->archive_opts);
  archive_read_close(a->fd_archive);
  u8_free(a->archive_spec);
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
  lispval v = FD_INT2DTYPE(ival);
  fd_store(tbl,fd_intern(slotname),v);
  fd_decref(v);
}

static lispval entry_info(struct archive_entry *entry)
{
  lispval tbl = fd_make_slotmap(7,0,NULL);
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
  set_int_prop(tbl,"SIZE",archive_entry_size(entry));
  return tbl;
}

static  int archive_seek(struct FD_ARCHIVE *archive,lispval seek,
                         struct archive_entry **entryp)
{
  struct archive_entry *entry;
  int rv = archive_read_next_header(archive->fd_archive,&entry);
  while (rv == ARCHIVE_OK) {
    if ( (FD_VOIDP(seek)) || (FD_FALSEP(seek)) || (FD_DEFAULTP(seek)) ) {
      *entryp = entry;
      return 1;}
    else if (FD_STRINGP(seek)) {
      if (strcmp(FD_CSTRING(seek),archive_entry_pathname_utf8(entry)) == 0) {
        *entryp = entry;
        return 1;}}
    else if (FD_TYPEP(seek,fd_regex_type)) {
      u8_string name = archive_entry_pathname_utf8(entry);
      ssize_t match = fd_regex_op(match,seek,name,strlen(name),0);
      if (match>0) {
        *entryp = entry;
        return 1;}}
    else {
      fd_seterr("BadSeekSpec","archive_seek",archive->archive_spec,seek);
      return -1;}
    rv = archive_read_next_header(archive->fd_archive,&entry);}
  if (rv == ARCHIVE_OK)
    return 0;
  fd_seterr("ArchiveError","archive_find",
            archive_error_string(archive->fd_archive),
            seek);
  return -1;
}

static lispval archive_find(lispval obj,lispval seek)
{
  struct FD_ARCHIVE *archive = (struct FD_ARCHIVE *) obj;
  struct archive_entry *entry;
  if (! ( (FD_VOIDP(seek)) || (FD_FALSEP(seek)) ||
          (FD_STRINGP(seek)) || (FD_TYPEP(seek,fd_regex_type)) ) )
    return fd_err("InvalidArchivePathSpec","archive_find",NULL,seek);
  int rv = archive_seek(archive,seek,&entry);
  if (rv < 0)
    return FD_ERROR_VALUE;
  else if (rv)
    return entry_info(entry);
  else return FD_FALSE;
}

FD_EXPORT int fd_init_libarchive()
{
  lispval module;
  if (libarchive_initialized)
    return libarchive_initialized;
  else libarchive_initialized = u8_millitime();

  module = fd_new_module("LIBARCHIVE",0);

  fd_libarchive_type = fd_register_cons_type("archive");
  fd_unparsers[fd_libarchive_type] = unparse_archive;
  fd_recyclers[fd_libarchive_type] = recycle_archive;

  fd_idefn2(module,"OPEN-ARCHIVE",open_archive,1,
            "Opens an archive file",
            -1,FD_VOID,-1,FD_FALSE);
  fd_idefn2(module,"ARCHIVE/FIND",archive_find,1,
            "Get the next archive entry (possibly matching a string or regex)",
            fd_libarchive_type,FD_VOID,-1,FD_FALSE);

  fd_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return libarchive_initialized;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
